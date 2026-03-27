/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <string.h>
#include <errno.h>
#include <stdio.h>

#include "aeron_cluster_session_manager.h"
#include "aeron_cluster_egress_publisher.h"
#include "aeron_cluster_consensus_publisher.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

#define INITIAL_SESSION_CAPACITY  16
#define INITIAL_PENDING_CAPACITY  8
#define INITIAL_UNCOMMITTED_CAP   32

/* -----------------------------------------------------------------------
 * No-op authenticator: immediately marks every session authenticated.
 * ----------------------------------------------------------------------- */
static void null_auth_on_connect(
    void *clientd, aeron_cluster_cluster_session_t *session, int64_t now_ms)
{
    (void)clientd; (void)now_ms;
    aeron_cluster_cluster_session_authenticate(session);
}

static void null_auth_noop(
    void *clientd, aeron_cluster_cluster_session_t *session, int64_t now_ms)
{
    (void)clientd; (void)session; (void)now_ms;
}

static void null_auth_challenge(
    void *clientd, int64_t session_id,
    const uint8_t *creds, size_t len, int64_t now_ms)
{
    (void)clientd; (void)session_id; (void)creds; (void)len; (void)now_ms;
}

const aeron_cluster_authenticator_t AERON_CLUSTER_AUTHENTICATOR_NULL = {
    .on_connect_request   = null_auth_on_connect,
    .on_connected_session = null_auth_noop,
    .on_challenged_session = null_auth_noop,
    .on_challenge_response = null_auth_challenge,
    .clientd               = NULL,
};

/* -----------------------------------------------------------------------
 * Internal helpers
 * ----------------------------------------------------------------------- */
static int session_list_grow(
    aeron_cluster_cluster_session_t ***list,
    int *count, int *capacity)
{
    if (*count >= *capacity)
    {
        int new_cap = (*capacity) * 2;
        if (aeron_reallocf((void **)list,
            (size_t)new_cap * sizeof(aeron_cluster_cluster_session_t *)) < 0)
        {
            return -1;
        }
        *capacity = new_cap;
    }
    return 0;
}

static int session_list_alloc(
    aeron_cluster_cluster_session_t ***list, int *count, int *capacity, int initial)
{
    *count    = 0;
    *capacity = initial;
    return aeron_alloc((void **)list, (size_t)initial * sizeof(aeron_cluster_cluster_session_t *));
}

/* Remove entry at index from list (swap-remove). Does NOT free the session. */
static void session_list_remove_at(
    aeron_cluster_cluster_session_t **list, int *count, int idx)
{
    list[idx] = list[--(*count)];
}

/* Enqueue into ring-buffer uncommitted_closed. */
static void uncommitted_enqueue(
    aeron_cluster_session_manager_t *m, aeron_cluster_cluster_session_t *s)
{
    /* grow if full (tail+1 wraps to head) */
    int next = (m->uncommitted_closed_tail + 1) % m->uncommitted_closed_capacity;
    if (next == m->uncommitted_closed_head)
    {
        int new_cap = m->uncommitted_closed_capacity * 2;
        aeron_cluster_cluster_session_t **new_buf = NULL;
        if (aeron_alloc((void **)&new_buf,
            (size_t)new_cap * sizeof(aeron_cluster_cluster_session_t *)) < 0) { return; }
        /* copy in order */
        int i = m->uncommitted_closed_head;
        int j = 0;
        while (i != m->uncommitted_closed_tail)
        {
            new_buf[j++] = m->uncommitted_closed[i];
            i = (i + 1) % m->uncommitted_closed_capacity;
        }
        aeron_free(m->uncommitted_closed);
        m->uncommitted_closed          = new_buf;
        m->uncommitted_closed_head     = 0;
        m->uncommitted_closed_tail     = j;
        m->uncommitted_closed_capacity = new_cap;
        next = (m->uncommitted_closed_tail + 1) % m->uncommitted_closed_capacity;
    }
    m->uncommitted_closed[m->uncommitted_closed_tail] = s;
    m->uncommitted_closed_tail = next;
}

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */
int aeron_cluster_session_manager_create(
    aeron_cluster_session_manager_t **manager,
    int64_t initial_session_id,
    aeron_t *aeron)
{
    aeron_cluster_session_manager_t *m = NULL;
    if (aeron_alloc((void **)&m, sizeof(aeron_cluster_session_manager_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate session manager");
        return -1;
    }

    if (session_list_alloc(&m->sessions,        &m->session_count,        &m->session_capacity,        INITIAL_SESSION_CAPACITY)  < 0 ||
        session_list_alloc(&m->pending_user,     &m->pending_user_count,   &m->pending_user_capacity,   INITIAL_PENDING_CAPACITY)  < 0 ||
        session_list_alloc(&m->rejected_user,    &m->rejected_user_count,  &m->rejected_user_capacity,  INITIAL_PENDING_CAPACITY)  < 0 ||
        session_list_alloc(&m->redirect_user,    &m->redirect_user_count,  &m->redirect_user_capacity,  INITIAL_PENDING_CAPACITY)  < 0 ||
        session_list_alloc(&m->pending_backup,   &m->pending_backup_count, &m->pending_backup_capacity, INITIAL_PENDING_CAPACITY)  < 0 ||
        session_list_alloc(&m->rejected_backup,  &m->rejected_backup_count,&m->rejected_backup_capacity,INITIAL_PENDING_CAPACITY)  < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate session lists");
        aeron_free(m);
        return -1;
    }

    m->uncommitted_closed_capacity = INITIAL_UNCOMMITTED_CAP;
    m->uncommitted_closed_head     = 0;
    m->uncommitted_closed_tail     = 0;
    if (aeron_alloc((void **)&m->uncommitted_closed,
        (size_t)INITIAL_UNCOMMITTED_CAP * sizeof(aeron_cluster_cluster_session_t *)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate uncommitted closed queue");
        aeron_free(m);
        return -1;
    }

    m->next_session_id           = initial_session_id;
    m->next_committed_session_id = initial_session_id;
    m->aeron                     = aeron;
    m->log_publisher             = NULL;
    m->egress_pub                = NULL;
    m->recording_log             = NULL;
    m->authenticator             = AERON_CLUSTER_AUTHENTICATOR_NULL;
    m->on_session_opened_hook    = NULL;
    m->on_session_closed_hook    = NULL;
    m->extension_hook_clientd    = NULL;
    m->session_timeout_ns        = INT64_C(5000000000);  /* 5 s default */
    m->max_concurrent_sessions   = INT32_MAX;
    m->service_count             = 1;
    m->cluster_id                = 0;
    m->member_id                 = 0;
    m->commit_position_counter_id = -1;
    m->standby_snapshot_notification_delay_ns = 0;

    m->pending_standby_snapshots = NULL;
    m->pending_standby_count     = 0;
    m->pending_standby_capacity  = 0;

    *manager = m;
    return 0;
}

int aeron_cluster_session_manager_close(aeron_cluster_session_manager_t *manager)
{
    if (NULL == manager) { return 0; }

    for (int i = 0; i < manager->session_count; i++)
        { aeron_cluster_cluster_session_close_and_free(manager->sessions[i]); }
    for (int i = 0; i < manager->pending_user_count; i++)
        { aeron_cluster_cluster_session_close_and_free(manager->pending_user[i]); }
    for (int i = 0; i < manager->rejected_user_count; i++)
        { aeron_cluster_cluster_session_close_and_free(manager->rejected_user[i]); }
    for (int i = 0; i < manager->redirect_user_count; i++)
        { aeron_cluster_cluster_session_close_and_free(manager->redirect_user[i]); }
    for (int i = 0; i < manager->pending_backup_count; i++)
        { aeron_cluster_cluster_session_close_and_free(manager->pending_backup[i]); }
    for (int i = 0; i < manager->rejected_backup_count; i++)
        { aeron_cluster_cluster_session_close_and_free(manager->rejected_backup[i]); }

    /* Uncommitted closed ring: free remaining sessions (not yet swept or restored). */
    int i = manager->uncommitted_closed_head;
    while (i != manager->uncommitted_closed_tail)
    {
        aeron_cluster_cluster_session_close_and_free(manager->uncommitted_closed[i]);
        i = (i + 1) % manager->uncommitted_closed_capacity;
    }

    aeron_free(manager->sessions);
    aeron_free(manager->pending_user);
    aeron_free(manager->rejected_user);
    aeron_free(manager->redirect_user);
    aeron_free(manager->pending_backup);
    aeron_free(manager->rejected_backup);
    aeron_free(manager->uncommitted_closed);
    aeron_free(manager->pending_standby_snapshots);
    aeron_free(manager);
    return 0;
}

/* -----------------------------------------------------------------------
 * Standby snapshot pending notification queue
 * ----------------------------------------------------------------------- */

void aeron_cluster_session_manager_enqueue_standby_snapshot(
    aeron_cluster_session_manager_t *manager,
    int64_t *recording_ids,
    int64_t *leadership_term_ids,
    int64_t *term_base_log_positions,
    int64_t *log_positions,
    int64_t *timestamps,
    int32_t *service_ids,
    int      count)
{
    if (count <= 0) { return; }
    if (count > AERON_STANDBY_SNAPSHOT_MAX_PER_BATCH)
    {
        count = AERON_STANDBY_SNAPSHOT_MAX_PER_BATCH;
    }

    /* Grow the batch array if needed */
    if (manager->pending_standby_count >= manager->pending_standby_capacity)
    {
        int new_cap = (manager->pending_standby_capacity == 0) ? 4
                                                               : manager->pending_standby_capacity * 2;
        if (aeron_reallocf((void **)&manager->pending_standby_snapshots,
            (size_t)new_cap * sizeof(aeron_standby_snapshot_batch_t)) < 0)
        {
            return;  /* out of memory — drop the batch */
        }
        manager->pending_standby_capacity = new_cap;
    }

    aeron_standby_snapshot_batch_t *batch =
        &manager->pending_standby_snapshots[manager->pending_standby_count++];
    batch->count       = count;
    batch->deadline_ns = 0;

    for (int i = 0; i < count; i++)
    {
        batch->entries[i].recording_id           = recording_ids[i];
        batch->entries[i].leadership_term_id     = leadership_term_ids[i];
        batch->entries[i].term_base_log_position = term_base_log_positions[i];
        batch->entries[i].log_position           = log_positions[i];
        batch->entries[i].timestamp              = timestamps[i];
        batch->entries[i].service_id             = service_ids[i];
    }
}

static void append_standby_snapshot_batch(
    aeron_cluster_session_manager_t *manager,
    const aeron_standby_snapshot_batch_t *batch)
{
    if (NULL == manager->recording_log) { return; }

    for (int i = 0; i < batch->count; i++)
    {
        const aeron_standby_snapshot_entry_t *e = &batch->entries[i];
        aeron_cluster_recording_log_append_standby_snapshot(
            manager->recording_log,
            e->recording_id,
            e->leadership_term_id,
            e->term_base_log_position,
            e->log_position,
            e->timestamp,
            e->service_id,
            "");  /* archiveEndpoint not carried in the SBE message we decode */
    }
}

int aeron_cluster_session_manager_process_pending_standby_snapshot_notifications(
    aeron_cluster_session_manager_t *manager,
    int64_t commit_position,
    int64_t now_ns)
{
    int work_count = 0;
    int dst = 0;  /* compaction index */

    for (int src = 0; src < manager->pending_standby_count; src++)
    {
        aeron_standby_snapshot_batch_t *batch = &manager->pending_standby_snapshots[src];

        if (batch->count == 0)
        {
            /* empty batch — drop it */
            continue;
        }

        /* Commit-position gate: first entry's logPosition must be committed */
        const int64_t batch_log_pos = batch->entries[0].log_position;
        if (batch_log_pos > commit_position)
        {
            /* Not yet committed — keep in queue */
            if (dst != src)
            {
                manager->pending_standby_snapshots[dst] = *batch;
            }
            dst++;
            continue;
        }

        /* Apply optional processing delay */
        if (manager->standby_snapshot_notification_delay_ns > 0 && batch->deadline_ns == 0)
        {
            batch->deadline_ns = now_ns + manager->standby_snapshot_notification_delay_ns;
        }

        if (batch->deadline_ns > 0 && now_ns < batch->deadline_ns)
        {
            /* Delay not yet elapsed — keep in queue */
            if (dst != src)
            {
                manager->pending_standby_snapshots[dst] = *batch;
            }
            dst++;
            continue;
        }

        /* Ready to append */
        append_standby_snapshot_batch(manager, batch);
        work_count++;
        /* batch dropped (not copied to dst) */
    }

    manager->pending_standby_count = dst;
    return work_count;
}

/* -----------------------------------------------------------------------
 * Basic operations
 * ----------------------------------------------------------------------- */
aeron_cluster_cluster_session_t *aeron_cluster_session_manager_new_session(
    aeron_cluster_session_manager_t *manager,
    int64_t correlation_id,
    int32_t response_stream_id,
    const char *response_channel,
    const uint8_t *encoded_principal,
    size_t principal_length)
{
    if (session_list_grow(&manager->sessions,
        &manager->session_count, &manager->session_capacity) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to grow sessions array");
        return NULL;
    }

    aeron_cluster_cluster_session_t *session = NULL;
    int64_t session_id = manager->next_session_id++;

    if (aeron_cluster_cluster_session_create(
        &session, session_id, correlation_id,
        response_stream_id, response_channel,
        encoded_principal, principal_length,
        manager->aeron) < 0)
    {
        return NULL;
    }

    manager->sessions[manager->session_count++] = session;
    return session;
}

aeron_cluster_cluster_session_t *aeron_cluster_session_manager_find(
    aeron_cluster_session_manager_t *manager, int64_t session_id)
{
    for (int i = 0; i < manager->session_count; i++)
    {
        if (manager->sessions[i]->id == session_id) { return manager->sessions[i]; }
    }
    return NULL;
}

int aeron_cluster_session_manager_remove(
    aeron_cluster_session_manager_t *manager, int64_t session_id)
{
    for (int i = 0; i < manager->session_count; i++)
    {
        if (manager->sessions[i]->id == session_id)
        {
            aeron_cluster_cluster_session_close_and_free(manager->sessions[i]);
            manager->sessions[i] = manager->sessions[--manager->session_count];
            return 0;
        }
    }
    return -1;
}

void aeron_cluster_session_manager_close_session(
    aeron_cluster_session_manager_t *manager,
    aeron_cluster_cluster_session_t *session)
{
    for (int i = 0; i < manager->session_count; i++)
    {
        if (manager->sessions[i]->id == session->id)
        {
            session_list_remove_at(manager->sessions, &manager->session_count, i);
            break;
        }
    }
    aeron_cluster_cluster_session_close_and_free(session);
}

int aeron_cluster_session_manager_session_count(aeron_cluster_session_manager_t *manager)
{
    return manager->session_count;
}

/* -----------------------------------------------------------------------
 * Connect / close handlers
 * ----------------------------------------------------------------------- */

/* PROTOCOL_MAJOR_VERSION = 0 (from AeronCluster.Configuration.PROTOCOL_MAJOR_VERSION in Java).
 * The CM validates that connecting clients send this major version. */
#define AERON_CLUSTER_PROTOCOL_MAJOR_VERSION 0
#define AERON_CLUSTER_EVENT_CODE_OK       0
#define AERON_CLUSTER_EVENT_CODE_ERROR    1
#define AERON_CLUSTER_EVENT_CODE_REDIRECT 2

void aeron_cluster_session_manager_on_session_connect(
    aeron_cluster_session_manager_t *manager,
    int64_t correlation_id,
    int32_t response_stream_id,
    int32_t version,
    const char *response_channel,
    const uint8_t *encoded_credentials, size_t credentials_len,
    int64_t now_ns,
    bool is_leader,
    const char *ingress_endpoints)
{
    int64_t session_id = is_leader ? manager->next_session_id++ : -1;

    aeron_cluster_cluster_session_t *session = NULL;
    if (aeron_cluster_cluster_session_create(
        &session, session_id, correlation_id,
        response_stream_id, response_channel,
        encoded_credentials, credentials_len,
        manager->aeron) < 0)
    {
        return;
    }

    session->time_of_last_activity_ns = now_ns;

    if (!is_leader)
    {
        aeron_cluster_cluster_session_set_redirect(session, ingress_endpoints);
        if (session_list_grow(&manager->redirect_user,
            &manager->redirect_user_count, &manager->redirect_user_capacity) == 0)
        {
            manager->redirect_user[manager->redirect_user_count++] = session;
        }
        else { aeron_cluster_cluster_session_close_and_free(session); }
        return;
    }

    int major = (version >> 16) & 0xFF;
    if (major != AERON_CLUSTER_PROTOCOL_MAJOR_VERSION)
    {
        char detail[128];
        snprintf(detail, sizeof(detail),
            "Invalid client version %d, cluster is %d",
            major, AERON_CLUSTER_PROTOCOL_MAJOR_VERSION);
        aeron_cluster_cluster_session_reject(session, AERON_CLUSTER_EVENT_CODE_ERROR, detail);
        if (session_list_grow(&manager->rejected_user,
            &manager->rejected_user_count, &manager->rejected_user_capacity) == 0)
        {
            manager->rejected_user[manager->rejected_user_count++] = session;
        }
        else { aeron_cluster_cluster_session_close_and_free(session); }
        return;
    }

    if (manager->pending_user_count + manager->session_count >= manager->max_concurrent_sessions)
    {
        aeron_cluster_cluster_session_reject(session, AERON_CLUSTER_EVENT_CODE_ERROR,
            "Session limit exceeded");
        if (session_list_grow(&manager->rejected_user,
            &manager->rejected_user_count, &manager->rejected_user_capacity) == 0)
        {
            manager->rejected_user[manager->rejected_user_count++] = session;
        }
        else { aeron_cluster_cluster_session_close_and_free(session); }
        return;
    }

    /* Kick off authentication */
    int64_t now_ms = now_ns / 1000000LL;
    manager->authenticator.on_connect_request(manager->authenticator.clientd, session, now_ms);

    if (session_list_grow(&manager->pending_user,
        &manager->pending_user_count, &manager->pending_user_capacity) == 0)
    {
        manager->pending_user[manager->pending_user_count++] = session;
    }
    else { aeron_cluster_cluster_session_close_and_free(session); }
}

void aeron_cluster_session_manager_on_session_close(
    aeron_cluster_session_manager_t *manager,
    int64_t cluster_session_id,
    int64_t leadership_term_id,
    int64_t timestamp_ms)
{
    aeron_cluster_cluster_session_t *session =
        aeron_cluster_session_manager_find(manager, cluster_session_id);
    if (NULL == session) { return; }

    aeron_cluster_cluster_session_closing(session, 1 /* CLIENT_ACTION */);

    /* Extension hook: onSessionClosed */
    if (NULL != manager->on_session_closed_hook)
    {
        manager->on_session_closed_hook(
            manager->extension_hook_clientd, cluster_session_id, session->close_reason);
    }

    /* Append session close to log */
    if (NULL != manager->log_publisher)
    {
        int64_t pos = aeron_cluster_log_publisher_append_session_close(
            manager->log_publisher, cluster_session_id,
            session->close_reason, timestamp_ms);
        if (pos > 0)
        {
            session->closed_log_position = pos;
            /* Remove from active list first, then enqueue — session stays alive in ring */
            for (int i = 0; i < manager->session_count; i++)
            {
                if (manager->sessions[i]->id == cluster_session_id)
                {
                    session_list_remove_at(manager->sessions, &manager->session_count, i);
                    break;
                }
            }
            uncommitted_enqueue(manager, session);
            /* Do NOT free — session lives in uncommitted ring until swept (committed)
             * or restored (on election step-down). Mirrors Java SessionManager. */
        }
    }
}

/* -----------------------------------------------------------------------
 * Log replay
 * ----------------------------------------------------------------------- */
void aeron_cluster_session_manager_on_replay_session_open(
    aeron_cluster_session_manager_t *manager,
    int64_t log_position,
    int64_t correlation_id,
    int64_t cluster_session_id,
    int64_t timestamp,
    int32_t response_stream_id,
    const char *response_channel)
{
    aeron_cluster_cluster_session_t *session = NULL;
    if (aeron_cluster_cluster_session_create(
        &session, cluster_session_id, correlation_id,
        response_stream_id, response_channel,
        NULL, 0, manager->aeron) < 0)
    {
        return;
    }

    aeron_cluster_cluster_session_open(session, log_position);
    session->time_of_last_activity_ns = timestamp;

    if (session_list_grow(&manager->sessions,
        &manager->session_count, &manager->session_capacity) == 0)
    {
        manager->sessions[manager->session_count++] = session;
    }
    else { aeron_cluster_cluster_session_close_and_free(session); return; }

    if (cluster_session_id >= manager->next_session_id)
    {
        manager->next_session_id           = cluster_session_id + 1;
        manager->next_committed_session_id = manager->next_session_id;
    }
}

void aeron_cluster_session_manager_on_replay_session_close(
    aeron_cluster_session_manager_t *manager,
    int64_t cluster_session_id,
    int32_t close_reason)
{
    aeron_cluster_cluster_session_t *session =
        aeron_cluster_session_manager_find(manager, cluster_session_id);
    if (NULL != session)
    {
        aeron_cluster_cluster_session_closing(session, close_reason);
        aeron_cluster_session_manager_close_session(manager, session);
    }
}

/* -----------------------------------------------------------------------
 * Snapshot load / save
 * ----------------------------------------------------------------------- */
void aeron_cluster_session_manager_on_load_cluster_session(
    aeron_cluster_session_manager_t *manager,
    int64_t cluster_session_id,
    int64_t correlation_id,
    int64_t opened_position,
    int64_t time_of_last_activity_ns,
    int32_t close_reason,
    int32_t response_stream_id,
    const char *response_channel)
{
    aeron_cluster_cluster_session_t *session = NULL;
    if (aeron_cluster_cluster_session_create(
        &session, cluster_session_id, correlation_id,
        response_stream_id, response_channel,
        NULL, 0, manager->aeron) < 0)
    {
        return;
    }

    aeron_cluster_cluster_session_open(session, opened_position);
    session->time_of_last_activity_ns = time_of_last_activity_ns;
    if (close_reason != 0)
    {
        aeron_cluster_cluster_session_closing(session, close_reason);
    }

    if (session_list_grow(&manager->sessions,
        &manager->session_count, &manager->session_capacity) == 0)
    {
        manager->sessions[manager->session_count++] = session;
    }
    else { aeron_cluster_cluster_session_close_and_free(session); return; }

    if (cluster_session_id >= manager->next_session_id)
    {
        manager->next_session_id           = cluster_session_id + 1;
        manager->next_committed_session_id = manager->next_session_id;
    }
}

void aeron_cluster_session_manager_load_next_session_id(
    aeron_cluster_session_manager_t *manager, int64_t next_session_id)
{
    manager->next_session_id           = next_session_id;
    manager->next_committed_session_id = next_session_id;
}

void aeron_cluster_session_manager_snapshot_sessions(
    aeron_cluster_session_manager_t *manager,
    aeron_cluster_session_snapshot_fn_t snapshot_fn,
    void *clientd)
{
    for (int i = 0; i < manager->session_count; i++)
    {
        aeron_cluster_cluster_session_t *s = manager->sessions[i];
        if (s->state == AERON_CLUSTER_SESSION_STATE_OPEN ||
            s->state == AERON_CLUSTER_SESSION_STATE_CLOSING)
        {
            snapshot_fn(clientd, s);
        }
    }
}

/* -----------------------------------------------------------------------
 * Periodic processing
 * ----------------------------------------------------------------------- */

/* Process a single pending list (user or backup). */
static int process_one_pending_list(
    aeron_cluster_session_manager_t *manager,
    aeron_cluster_cluster_session_t **list, int *count,
    aeron_cluster_cluster_session_t **rejected_list, int *rejected_count, int *rejected_capacity,
    int64_t now_ns, int64_t now_ms,
    int32_t leader_member_id, int64_t leadership_term_id)
{
    int work = 0;

    for (int i = *count - 1; i >= 0; i--)
    {
        aeron_cluster_cluster_session_t *s = list[i];

        /* Remove invalid sessions immediately */
        if (s->state == AERON_CLUSTER_SESSION_STATE_INVALID)
        {
            session_list_remove_at(list, count, i);
            aeron_cluster_cluster_session_close_and_free(s);
            continue;
        }

        /* Timeout check (skip INIT state) */
        if (s->state != AERON_CLUSTER_SESSION_STATE_INIT &&
            now_ns > (s->time_of_last_activity_ns + manager->session_timeout_ns))
        {
            session_list_remove_at(list, count, i);
            aeron_cluster_cluster_session_close_and_free(s);
            continue;
        }

        /* Drive response publication creation and connection.
         * Runs for ANY state that hasn't yet got a connected publication.
         * Mirrors Java ClusterSession.isResponsePublicationConnected() polling. */
        if (NULL == s->response_publication ||
            !aeron_exclusive_publication_is_connected(s->response_publication))
        {
            if (NULL == s->response_publication)
            {
                if (NULL == s->async_response_pub)
                {
                    /* Start async registration — persist handle across ticks */
                    aeron_async_add_exclusive_publication(
                        &s->async_response_pub, manager->aeron,
                        s->response_channel, s->response_stream_id);
                }
                if (NULL != s->async_response_pub)
                {
                    int rc = aeron_async_add_exclusive_publication_poll(
                        &s->response_publication, s->async_response_pub);
                    if (rc != 0)
                    {
                        s->async_response_pub = NULL;
                    }
                }
            }
            /* If still not connected and not yet AUTHENTICATED, keep state as CONNECTING */
            if (s->state == AERON_CLUSTER_SESSION_STATE_INIT)
            {
                s->state = AERON_CLUSTER_SESSION_STATE_CONNECTING;
            }
        }
        else if (s->state == AERON_CLUSTER_SESSION_STATE_INIT ||
                 s->state == AERON_CLUSTER_SESSION_STATE_CONNECTING)
        {
            /* Publication is ready and connected — drive authenticator */
            s->state = AERON_CLUSTER_SESSION_STATE_CONNECTED;
            manager->authenticator.on_connected_session(
                manager->authenticator.clientd, s, now_ms);
        }

        if (s->state == AERON_CLUSTER_SESSION_STATE_CHALLENGED)
        {
            if (NULL != s->response_publication &&
                aeron_exclusive_publication_is_connected(s->response_publication))
            {
                manager->authenticator.on_challenged_session(
                    manager->authenticator.clientd, s, now_ms);
            }
        }

        if (s->state == AERON_CLUSTER_SESSION_STATE_AUTHENTICATED)
        {
            /* CLIENT sessions need response publication ready before proceeding */
            if (s->action == AERON_CLUSTER_SESSION_ACTION_CLIENT &&
                (NULL == s->response_publication ||
                 !aeron_exclusive_publication_is_connected(s->response_publication)))
            {
                continue;
            }
            if (s->action == AERON_CLUSTER_SESSION_ACTION_CLIENT)
            {
                /* Append session open to log */
                if (NULL != manager->log_publisher)
                {
                    int64_t log_pos = aeron_cluster_log_publisher_append_session_open(
                        manager->log_publisher,
                        s->id, s->correlation_id,
                        now_ms,
                        s->response_stream_id, s->response_channel,
                        s->encoded_principal, s->encoded_principal_length);

                    if (log_pos > 0)
                    {
                        aeron_cluster_cluster_session_open(s, log_pos);

                        if (s->id >= manager->next_committed_session_id)
                        {
                            manager->next_committed_session_id = s->id + 1;
                        }

                        /* Always set has_open_event_pending=true so check_sessions
                         * retries the send each tick until the frame is truly delivered.
                         * Mirrors Java: sendSessionOpenEvent is retried until success. */
                        aeron_cluster_egress_publisher_send_session_event(
                            s->response_publication, s->id, s->correlation_id,
                            leadership_term_id, leader_member_id,
                            AERON_CLUSTER_EVENT_CODE_OK, 0, NULL, 0);
                        s->has_open_event_pending = true;

                        session_list_remove_at(list, count, i);

                        /* Add to active sessions */
                        if (session_list_grow(&manager->sessions,
                            &manager->session_count, &manager->session_capacity) == 0)
                        {
                            manager->sessions[manager->session_count++] = s;
                            if (NULL != manager->on_session_opened_hook)
                            {
                                manager->on_session_opened_hook(
                                    manager->extension_hook_clientd, s->id);
                            }
                        }

                        work++;
                    }
                }
                else
                {
                    /* No log publisher: activate and always set pending for retry */
                    aeron_cluster_cluster_session_open(s, 0);
                    if (NULL != s->response_publication &&
                        aeron_exclusive_publication_is_connected(s->response_publication))
                    {
                        aeron_cluster_egress_publisher_send_session_event(
                            s->response_publication, s->id, s->correlation_id,
                            leadership_term_id, leader_member_id,
                            AERON_CLUSTER_EVENT_CODE_OK, 0, NULL, 0);
                    }
                    s->has_open_event_pending = true;
                    session_list_remove_at(list, count, i);
                    if (session_list_grow(&manager->sessions,
                        &manager->session_count, &manager->session_capacity) == 0)
                    {
                        manager->sessions[manager->session_count++] = s;
                    }
                    work++;
                }
            }
            else if (s->action == AERON_CLUSTER_SESSION_ACTION_BACKUP)
            {
                aeron_cluster_recording_log_entry_t *entry =
                    (NULL != manager->recording_log)
                    ? aeron_cluster_recording_log_find_last_term(manager->recording_log)
                    : NULL;

                if (NULL != entry &&
                    NULL != s->response_publication &&
                    aeron_exclusive_publication_is_connected(s->response_publication))
                {
                    /* Build snapshots array (max service_count + 1 entries, hard cap 33) */
                    aeron_cluster_recording_log_entry_t  rl_snaps[33];
                    aeron_cluster_backup_response_snapshot_t br_snaps[33];
                    int snap_count = 0;

                    aeron_cluster_recording_log_find_snapshots_at_or_before(
                        manager->recording_log,
                        s->request_input,
                        manager->service_count,
                        rl_snaps, &snap_count);

                    for (int k = 0; k < snap_count; k++)
                    {
                        br_snaps[k].recording_id           = rl_snaps[k].recording_id;
                        br_snaps[k].leadership_term_id     = rl_snaps[k].leadership_term_id;
                        br_snaps[k].term_base_log_position = rl_snaps[k].term_base_log_position;
                        br_snaps[k].log_position           = rl_snaps[k].log_position;
                        br_snaps[k].timestamp              = rl_snaps[k].timestamp;
                        br_snaps[k].service_id             = rl_snaps[k].service_id;
                    }

                    if (aeron_cluster_consensus_publisher_backup_response(
                        s->response_publication,
                        s->correlation_id,
                        entry->recording_id,
                        entry->leadership_term_id,
                        entry->term_base_log_position,
                        manager->commit_position_counter_id,
                        leader_member_id,
                        manager->member_id,
                        br_snaps, snap_count,
                        manager->cluster_members_str != NULL ? manager->cluster_members_str : ""))
                    {
                        session_list_remove_at(list, count, i);
                        aeron_cluster_cluster_session_close_and_free(s);
                        work++;
                    }
                }
            }
            else if (s->action == AERON_CLUSTER_SESSION_ACTION_HEARTBEAT)
            {
                if (NULL != s->response_publication &&
                    aeron_cluster_consensus_publisher_heartbeat_response(
                        s->response_publication, s->correlation_id))
                {
                    session_list_remove_at(list, count, i);
                    aeron_cluster_cluster_session_close_and_free(s);
                    work++;
                }
            }
            else if (s->action == AERON_CLUSTER_SESSION_ACTION_STANDBY_SNAPSHOT)
            {
                /* Standby snapshot requests: no interceptor in this C port.
                 * Mirrors Java SessionManager when standbySnapshotInterceptor == null:
                 * close the session and move on. */
                session_list_remove_at(list, count, i);
                aeron_cluster_cluster_session_close_and_free(s);
                work++;
            }
        }
        else if (s->state == AERON_CLUSTER_SESSION_STATE_REJECTED)
        {
            session_list_remove_at(list, count, i);
            if (session_list_grow(
                &rejected_list, rejected_count, rejected_capacity) == 0)
            {
                rejected_list[(*rejected_count)++] = s;
            }
            else { aeron_cluster_cluster_session_close_and_free(s); }
        }
    }

    return work;
}

int aeron_cluster_session_manager_process_pending_sessions(
    aeron_cluster_session_manager_t *manager,
    int64_t now_ns, int64_t now_ms,
    int32_t leader_member_id, int64_t leadership_term_id)
{
    int work = 0;
    work += process_one_pending_list(
        manager,
        manager->pending_user,  &manager->pending_user_count,
        manager->rejected_user, &manager->rejected_user_count, &manager->rejected_user_capacity,
        now_ns, now_ms, leader_member_id, leadership_term_id);
    work += process_one_pending_list(
        manager,
        manager->pending_backup,  &manager->pending_backup_count,
        manager->rejected_backup, &manager->rejected_backup_count, &manager->rejected_backup_capacity,
        now_ns, now_ms, leader_member_id, leadership_term_id);
    return work;
}

int aeron_cluster_session_manager_check_sessions(
    aeron_cluster_session_manager_t *manager,
    int64_t now_ns,
    int64_t leadership_term_id,
    int32_t leader_member_id,
    const char *ingress_endpoints)
{
    int work = 0;

    for (int i = manager->session_count - 1; i >= 0; i--)
    {
        aeron_cluster_cluster_session_t *s = manager->sessions[i];

        if (now_ns > (s->time_of_last_activity_ns + manager->session_timeout_ns))
        {
            int64_t timestamp_ms = now_ns / 1000000LL;

            if (s->state == AERON_CLUSTER_SESSION_STATE_OPEN)
            {
                aeron_cluster_cluster_session_closing(s, 3 /* TIMEOUT */);

                if (NULL != manager->log_publisher)
                {
                    int64_t pos = aeron_cluster_log_publisher_append_session_close(
                        manager->log_publisher, s->id, s->close_reason, timestamp_ms);
                    if (pos > 0)
                    {
                        /* Send CLOSED event via egress pub */
                        if (NULL != manager->egress_pub && NULL != s->response_publication)
                        {
                            aeron_cluster_egress_publisher_send_session_event(
                                s->response_publication, s->id, s->correlation_id,
                                leadership_term_id, leader_member_id,
                                AERON_CLUSTER_EVENT_CODE_ERROR, 0,
                                "TIMEOUT", 7);
                        }
                        s->closed_log_position = pos;
                        session_list_remove_at(manager->sessions, &manager->session_count, i);
                        uncommitted_enqueue(manager, s);
                        /* Do NOT free — lives in uncommitted ring */
                    }
                }
                work++;
            }
            else if (s->state == AERON_CLUSTER_SESSION_STATE_CLOSING)
            {
                if (NULL != manager->log_publisher)
                {
                    int64_t pos = aeron_cluster_log_publisher_append_session_close(
                        manager->log_publisher, s->id, s->close_reason, timestamp_ms);
                    if (pos > 0)
                    {
                        if (NULL != s->response_publication)
                        {
                            aeron_cluster_egress_publisher_send_session_event(
                                s->response_publication, s->id, s->correlation_id,
                                leadership_term_id, leader_member_id,
                                AERON_CLUSTER_EVENT_CODE_ERROR, 0,
                                "CLOSED", 6);
                        }
                        s->closed_log_position = pos;
                        session_list_remove_at(manager->sessions, &manager->session_count, i);
                        uncommitted_enqueue(manager, s);
                        /* Do NOT free — lives in uncommitted ring */
                        work++;
                    }
                }
            }
            else
            {
                session_list_remove_at(manager->sessions, &manager->session_count, i);
                aeron_cluster_cluster_session_close_and_free(s);
                work++;
            }
        }
        else if (s->has_open_event_pending)
        {
            /* Retry sending SESSION_EVENT(OK) every tick until the client connects.
             * The flag stays true — we don't know when the client received it.
             * check_sessions keeps retrying until the session is used (message arrives)
             * or the session times out. Mirrors Java's sendSessionOpenEvent retry loop. */
            if (NULL != s->response_publication &&
                aeron_exclusive_publication_is_connected(s->response_publication))
            {
                aeron_cluster_egress_publisher_send_session_event(
                    s->response_publication, s->id, s->correlation_id,
                    leadership_term_id, leader_member_id,
                    AERON_CLUSTER_EVENT_CODE_OK, 0, NULL, 0);
                /* Note: keep has_open_event_pending=true — client clears it
                 * by sending the first message (session becomes truly OPEN).
                 * Java also retries sendSessionOpenEvent until success confirmed. */
                work++;
            }
        }
        else if (s->has_new_leader_event_pending)
        {
            /* Send new leader event */
            if (NULL != s->response_publication &&
                aeron_exclusive_publication_is_connected(s->response_publication))
            {
                size_t ep_len = ingress_endpoints ? strlen(ingress_endpoints) : 0;
                aeron_cluster_egress_publisher_send_new_leader_event(
                    s->response_publication, s->id,
                    leadership_term_id, leader_member_id,
                    ingress_endpoints, ep_len);
                s->has_new_leader_event_pending = false;
                work++;
            }
        }
    }
    return work;
}

static int send_pending_list(
    aeron_cluster_session_manager_t *manager,
    aeron_cluster_cluster_session_t **list, int *count,
    int64_t leadership_term_id, int32_t leader_member_id, int64_t now_ns)
{
    int work = 0;
    for (int i = *count - 1; i >= 0; i--)
    {
        aeron_cluster_cluster_session_t *s = list[i];

        /* Timeout */
        if (now_ns > (s->time_of_last_activity_ns + manager->session_timeout_ns) ||
            s->state == AERON_CLUSTER_SESSION_STATE_INVALID)
        {
            session_list_remove_at(list, count, i);
            aeron_cluster_cluster_session_close_and_free(s);
            work++;
            continue;
        }

        /* Start publication if not yet */
        if (NULL == s->response_publication)
        {
            aeron_async_add_exclusive_publication_t *async = NULL;
            aeron_async_add_exclusive_publication(
                &async, manager->aeron,
                s->response_channel, s->response_stream_id);
            if (async) { aeron_async_add_exclusive_publication_poll(&s->response_publication, async); }
        }

        if (NULL != s->response_publication &&
            aeron_exclusive_publication_is_connected(s->response_publication))
        {
            size_t detail_len = strlen(s->response_detail);
            bool sent = aeron_cluster_egress_publisher_send_session_event(
                s->response_publication, s->id, s->correlation_id,
                leadership_term_id, leader_member_id,
                s->event_code, 0,
                s->response_detail, detail_len);
            if (sent)
            {
                session_list_remove_at(list, count, i);
                aeron_cluster_cluster_session_close_and_free(s);
                work++;
            }
        }
    }
    return work;
}

int aeron_cluster_session_manager_send_redirects(
    aeron_cluster_session_manager_t *manager,
    int64_t leadership_term_id,
    int32_t leader_member_id,
    int64_t now_ns)
{
    return send_pending_list(manager, manager->redirect_user, &manager->redirect_user_count,
        leadership_term_id, leader_member_id, now_ns);
}

int aeron_cluster_session_manager_send_rejections(
    aeron_cluster_session_manager_t *manager,
    int64_t leadership_term_id,
    int32_t leader_member_id,
    int64_t now_ns)
{
    int work = 0;
    work += send_pending_list(manager, manager->rejected_user, &manager->rejected_user_count,
        leadership_term_id, leader_member_id, now_ns);
    work += send_pending_list(manager, manager->rejected_backup, &manager->rejected_backup_count,
        leadership_term_id, leader_member_id, now_ns);
    return work;
}

int aeron_cluster_session_manager_check_timeouts(
    aeron_cluster_session_manager_t *manager,
    int64_t now_ns,
    int64_t session_timeout_ns,
    aeron_cluster_session_timeout_fn_t on_timeout,
    void *clientd)
{
    int count = 0;
    for (int i = manager->session_count - 1; i >= 0; i--)
    {
        aeron_cluster_cluster_session_t *session = manager->sessions[i];
        if (aeron_cluster_cluster_session_is_timed_out(session, now_ns, session_timeout_ns))
        {
            on_timeout(clientd, session);
            manager->sessions[i] = manager->sessions[--manager->session_count];
            count++;
        }
    }
    return count;
}

/* -----------------------------------------------------------------------
 * State transitions
 * ----------------------------------------------------------------------- */
void aeron_cluster_session_manager_clear_sessions_after(
    aeron_cluster_session_manager_t *manager,
    int64_t log_position,
    int64_t leadership_term_id)
{
    for (int i = manager->session_count - 1; i >= 0; i--)
    {
        aeron_cluster_cluster_session_t *s = manager->sessions[i];
        if (s->opened_log_position > log_position)
        {
            if (NULL != s->response_publication)
            {
                aeron_cluster_egress_publisher_send_session_event(
                    s->response_publication, s->id, s->correlation_id,
                    leadership_term_id, manager->member_id,
                    AERON_CLUSTER_EVENT_CODE_ERROR, 0, "election", 8);
            }
            session_list_remove_at(manager->sessions, &manager->session_count, i);
            aeron_cluster_cluster_session_close_and_free(s);
        }
    }

    /* Also clear pending user sessions */
    for (int i = 0; i < manager->pending_user_count; i++)
    {
        aeron_cluster_cluster_session_t *s = manager->pending_user[i];
        if (NULL != s->response_publication)
        {
            aeron_cluster_egress_publisher_send_session_event(
                s->response_publication, s->id, s->correlation_id,
                leadership_term_id, manager->member_id,
                AERON_CLUSTER_EVENT_CODE_ERROR, 0, "election", 8);
        }
        aeron_cluster_cluster_session_close_and_free(s);
    }
    manager->pending_user_count = 0;
}

void aeron_cluster_session_manager_sweep_uncommitted_sessions(
    aeron_cluster_session_manager_t *manager,
    int64_t commit_position)
{
    while (manager->uncommitted_closed_head != manager->uncommitted_closed_tail)
    {
        aeron_cluster_cluster_session_t *s =
            manager->uncommitted_closed[manager->uncommitted_closed_head];
        if (s->closed_log_position > commit_position) { break; }
        /* Session's close is committed — now safe to free. */
        aeron_cluster_cluster_session_close_and_free(s);
        manager->uncommitted_closed_head =
            (manager->uncommitted_closed_head + 1) % manager->uncommitted_closed_capacity;
    }
}

void aeron_cluster_session_manager_restore_uncommitted_sessions(
    aeron_cluster_session_manager_t *manager,
    int64_t commit_position)
{
    /* Walk the uncommitted ring. Sessions whose close was committed (closed_log_position
     * <= commit_position) are freed. Sessions whose close was NOT committed are put back
     * into the active sessions list and restored to OPEN state.
     * Mirrors Java SessionManager.restoreUncommittedSessions(commitPosition). */
    int i = manager->uncommitted_closed_head;
    while (i != manager->uncommitted_closed_tail)
    {
        aeron_cluster_cluster_session_t *s = manager->uncommitted_closed[i];
        i = (i + 1) % manager->uncommitted_closed_capacity;

        if (s->closed_log_position <= commit_position)
        {
            /* Close was committed — free the session. */
            aeron_cluster_cluster_session_close_and_free(s);
        }
        else
        {
            /* Close was NOT committed — restore session to active. */
            s->state              = AERON_CLUSTER_SESSION_STATE_OPEN;
            s->close_reason       = 0;
            s->closed_log_position = -1;
            if (session_list_grow(&manager->sessions,
                &manager->session_count, &manager->session_capacity) == 0)
            {
                manager->sessions[manager->session_count++] = s;
            }
            else
            {
                aeron_cluster_cluster_session_close_and_free(s);
            }
        }
    }
    /* Ring is fully drained. */
    manager->uncommitted_closed_head = 0;
    manager->uncommitted_closed_tail = 0;
}

void aeron_cluster_session_manager_disconnect_sessions(
    aeron_cluster_session_manager_t *manager)
{
    /* Close the response publication for every active session without freeing the
     * session object. Mirrors Java ConsensusModuleAgent.disconnectSessions() which
     * calls session.closePublication() on each session when stepping down. */
    for (int i = 0; i < manager->session_count; i++)
    {
        aeron_cluster_cluster_session_t *s = manager->sessions[i];
        if (NULL != s->response_publication)
        {
            aeron_exclusive_publication_close(s->response_publication, NULL, NULL);
            s->response_publication = NULL;
        }
    }
}

void aeron_cluster_session_manager_prepare_for_new_term(
    aeron_cluster_session_manager_t *manager,
    bool is_startup,
    int64_t now_ns)
{
    for (int i = 0; i < manager->session_count; i++)
    {
        aeron_cluster_cluster_session_t *s = manager->sessions[i];
        if (s->state == AERON_CLUSTER_SESSION_STATE_OPEN)
        {
            if (is_startup)
            {
                aeron_cluster_cluster_session_closing(s, 3 /* TIMEOUT */);
            }
            else
            {
                s->time_of_last_activity_ns     = now_ns;
                s->has_new_leader_event_pending  = true;
            }
        }
    }
}

void aeron_cluster_session_manager_update_activity(
    aeron_cluster_session_manager_t *manager, int64_t now_ns)
{
    for (int i = 0; i < manager->session_count; i++)
    {
        manager->sessions[i]->time_of_last_activity_ns = now_ns;
    }
}

void aeron_cluster_session_manager_on_challenge_response(
    aeron_cluster_session_manager_t *manager,
    int64_t correlation_id,
    int64_t cluster_session_id,
    const uint8_t *encoded_credentials, size_t len,
    int64_t now_ns)
{
    for (int i = 0; i < manager->pending_user_count; i++)
    {
        aeron_cluster_cluster_session_t *s = manager->pending_user[i];
        if (s->id == cluster_session_id &&
            s->state == AERON_CLUSTER_SESSION_STATE_CHALLENGED)
        {
            s->correlation_id             = correlation_id;
            s->time_of_last_activity_ns   = now_ns;
            int64_t now_ms = now_ns / 1000000LL;
            manager->authenticator.on_challenge_response(
                manager->authenticator.clientd,
                cluster_session_id, encoded_credentials, len, now_ms);
            break;
        }
    }
}

void aeron_cluster_session_manager_on_backup_challenge_response(
    aeron_cluster_session_manager_t *manager,
    int64_t correlation_id,
    int64_t cluster_session_id,
    const uint8_t *encoded_credentials, size_t len,
    int64_t now_ns)
{
    for (int i = 0; i < manager->pending_backup_count; i++)
    {
        aeron_cluster_cluster_session_t *s = manager->pending_backup[i];
        if (s->id == cluster_session_id &&
            s->state == AERON_CLUSTER_SESSION_STATE_CHALLENGED)
        {
            s->correlation_id           = correlation_id;
            s->time_of_last_activity_ns = now_ns;
            int64_t now_ms = now_ns / 1000000LL;
            manager->authenticator.on_challenge_response(
                manager->authenticator.clientd,
                cluster_session_id, encoded_credentials, len, now_ms);
            break;
        }
    }
}
