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

#include <errno.h>
#include <string.h>
#include <inttypes.h>

#include "aeron_clustered_service_agent.h"
#include "concurrent/aeron_thread.h"
#include "aeron_cluster_service_context.h"
#include "aeron_cluster_service_snapshot_taker.h"
#include "aeron_cluster_recovery_state.h"

#include "aeron_archive.h"
#include "aeron_archive_replay_params.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

/* SBE codec for session message header — needed by offerv / try_claim */
#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/sessionMessageHeader.h"

#define SESSION_HEADER_LENGTH 32  /* same as AERON_CLUSTER_SESSION_HEADER_LENGTH */

/* CLUSTER_ACTION_FLAGS_DEFAULT: ordinary snapshot */
#define CLUSTER_ACTION_FLAGS_DEFAULT  0

/* -----------------------------------------------------------------------
 * Internal helpers
 * ----------------------------------------------------------------------- */
static aeron_cluster_client_session_t *find_session(
    aeron_clustered_service_agent_t *agent, int64_t id)
{
    for (size_t i = 0; i < agent->sessions_count; i++)
    {
        if (agent->sessions[i]->cluster_session_id == id)
        {
            return agent->sessions[i];
        }
    }
    return NULL;
}

static int add_session(aeron_clustered_service_agent_t *agent,
                       aeron_cluster_client_session_t *session)
{
    if (agent->sessions_count >= agent->sessions_capacity)
    {
        size_t new_cap = agent->sessions_capacity * 2;
        if (aeron_reallocf((void **)&agent->sessions,
            new_cap * sizeof(aeron_cluster_client_session_t *)) < 0)
        {
            AERON_APPEND_ERR("%s", "unable to grow sessions array");
            return -1;
        }
        agent->sessions_capacity = new_cap;
    }
    agent->sessions[agent->sessions_count++] = session;
    return 0;
}

static void remove_session(aeron_clustered_service_agent_t *agent,
                           aeron_cluster_client_session_t *session)
{
    for (size_t i = 0; i < agent->sessions_count; i++)
    {
        if (agent->sessions[i] == session)
        {
            agent->sessions[i] = agent->sessions[--agent->sessions_count];
            return;
        }
    }
}

/* -----------------------------------------------------------------------
 * Snapshot session restore callback
 * ----------------------------------------------------------------------- */
static void restore_session_from_snapshot(
    void *clientd,
    int64_t cluster_session_id,
    int32_t response_stream_id,
    const char *response_channel,
    const uint8_t *encoded_principal,
    size_t principal_length)
{
    aeron_clustered_service_agent_t *agent = (aeron_clustered_service_agent_t *)clientd;
    aeron_cluster_client_session_t *session = NULL;
    if (aeron_cluster_client_session_create(
        &session,
        cluster_session_id, response_stream_id, response_channel,
        encoded_principal, principal_length,
        agent->aeron) < 0)
    {
        return;
    }
    if (add_session(agent, session) < 0)
    {
        aeron_cluster_client_session_close_and_free(session);
    }
}

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */
int aeron_clustered_service_agent_create(
    aeron_clustered_service_agent_t **agent,
    aeron_cluster_service_context_t *ctx)
{
    aeron_clustered_service_agent_t *_agent = NULL;

    if (aeron_alloc((void **)&_agent, sizeof(aeron_clustered_service_agent_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate clustered service agent");
        return -1;
    }

    _agent->ctx               = ctx;
    _agent->aeron             = ctx->aeron;
    _agent->service_adapter   = NULL;
    _agent->log_adapter       = NULL;
    _agent->member_id         = -1;
    _agent->cluster_time      = 0;
    _agent->log_position      = -1;
    _agent->max_log_position  = -1;
    _agent->leadership_term_id = -1;
    _agent->ack_id            = 0;
    _agent->termination_position = -1;
    _agent->role              = AERON_CLUSTER_ROLE_FOLLOWER;
    _agent->is_service_active = false;
    _agent->commit_position_counter_id = -1;
    _agent->snapshot_log_position      = 0;
    _agent->snapshot_leadership_term_id = 0;
    _agent->mark_file_update_deadline_ns = 0;
    _agent->active_lifecycle_callback = AERON_LIFECYCLE_CALLBACK_NONE;
    _agent->requested_ack_position    = -1;
    _agent->max_snapshot_duration_ns  = 0;

    /* Sessions array */
    _agent->sessions_capacity = AERON_CLUSTER_SESSIONS_INITIAL_CAPACITY;
    _agent->sessions_count    = 0;
    if (aeron_alloc((void **)&_agent->sessions,
        _agent->sessions_capacity * sizeof(aeron_cluster_client_session_t *)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate sessions array");
        aeron_free(_agent);
        return -1;
    }

    _agent->control_subscription = NULL;

    _agent->counters_reader = aeron_counters_reader(ctx->aeron);

    *agent = _agent;
    return 0;
}

int aeron_clustered_service_agent_on_start(aeron_clustered_service_agent_t *agent)
{
    aeron_cluster_service_context_t *ctx = agent->ctx;

    /* Add service publication (service→CM) — spin-poll until ready */
    {
        aeron_async_add_exclusive_publication_t *async_pub = NULL;
        if (aeron_async_add_exclusive_publication(
            &async_pub, agent->aeron, ctx->service_channel, ctx->service_stream_id) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to start add service publication");
            return -1;
        }
        int rc = 0;
        do { rc = aeron_async_add_exclusive_publication_poll(&agent->service_publication, async_pub); }
        while (0 == rc);
        if (rc < 0) { AERON_APPEND_ERR("%s", "failed to add service publication"); return -1; }
    }

    /* Add control subscription (CM→service) — spin-poll until ready */
    {
        aeron_async_add_subscription_t *async_sub = NULL;
        if (aeron_async_add_subscription(
            &async_sub, agent->aeron, ctx->control_channel,
            ctx->consensus_module_stream_id, NULL, NULL, NULL, NULL) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to start add control subscription");
            return -1;
        }
        int rc = 0;
        do { rc = aeron_async_add_subscription_poll(&agent->control_subscription, async_sub); }
        while (0 == rc);
        if (rc < 0) { AERON_APPEND_ERR("%s", "failed to add control subscription"); return -1; }
    }

    aeron_cluster_consensus_module_proxy_init(
        &agent->consensus_module_proxy,
        agent->service_publication,
        AERON_CLUSTER_PROXY_RETRY_ATTEMPTS,
        ctx->service_id);

    if (aeron_cluster_service_adapter_create(
        &agent->service_adapter,
        agent->control_subscription,
        agent) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to create service adapter");
        return -1;
    }

    agent->is_service_active = true;

    /* --- Recovery state lookup --- */
    aeron_counters_reader_t *cr = agent->counters_reader;
    int32_t recovery_counter_id = aeron_cluster_recovery_state_find_counter_id(
        cr, agent->ctx->cluster_id);

    if (recovery_counter_id != AERON_NULL_COUNTER_ID)
    {
        /* Restore position / time / term from the recovery counter */
        agent->log_position      = aeron_cluster_recovery_state_get_log_position(cr, recovery_counter_id);
        agent->cluster_time      = aeron_cluster_recovery_state_get_timestamp(cr, recovery_counter_id);
        agent->leadership_term_id = aeron_cluster_recovery_state_get_leadership_term_id(cr, recovery_counter_id);

        int64_t snapshot_recording_id =
            aeron_cluster_recovery_state_get_snapshot_recording_id(cr, recovery_counter_id, ctx->service_id);

        if (snapshot_recording_id != -1 && NULL != ctx->archive)
        {
            /* Load snapshot via archive replay */
            aeron_subscription_t *snapshot_sub = NULL;
            {
                aeron_async_add_subscription_t *async_sub = NULL;
                if (aeron_async_add_subscription(
                    &async_sub, agent->aeron,
                    ctx->snapshot_channel, ctx->snapshot_stream_id,
                    NULL, NULL, NULL, NULL) < 0)
                {
                    AERON_APPEND_ERR("%s", "failed to add snapshot subscription");
                    return -1;
                }
                int rc = 0;
                do { rc = aeron_async_add_subscription_poll(&snapshot_sub, async_sub); }
                while (0 == rc);
                if (rc < 0)
                {
                    AERON_APPEND_ERR("%s", "failed to poll snapshot subscription");
                    return -1;
                }
            }

            /* Start archive replay */
            int64_t replay_session_id = -1;
            {
                aeron_archive_replay_params_t params;
                aeron_archive_replay_params_init(&params);
                params.position = 0;
                params.length = INT64_MAX;
                params.file_io_max_length = 4096 * 1024;
                if (aeron_archive_start_replay(
                    &replay_session_id, ctx->archive,
                    snapshot_recording_id,
                    ctx->snapshot_channel, ctx->snapshot_stream_id, &params) < 0)
                {
                    AERON_APPEND_ERR("%s", "failed to start snapshot replay");
                    aeron_subscription_close(snapshot_sub, NULL, NULL);
                    return -1;
                }
            }

            /* Spin until the replay image appears */
            int32_t session_id = (int32_t)replay_session_id;
            aeron_image_t *snapshot_image = NULL;
            while (NULL == snapshot_image)
            {
                snapshot_image = aeron_subscription_image_by_session_id(snapshot_sub, session_id);
                aeron_cluster_service_context_invoke_aeron_client(ctx);
            }

            aeron_clustered_service_t *svc = ctx->service;
            aeron_cluster_snapshot_image_t snap_img = { snapshot_image, false };

            /* Restore sessions from snapshot before handing off to user.
             * Java order: BEGIN → sessions → END → user data.
             * After load_sessions, image is positioned at user data. */
            aeron_cluster_service_snapshot_loader_load_sessions(
                &snap_img, restore_session_from_snapshot, agent);
            snap_img.is_done = false;  /* reset so user can poll their own data */

            agent->active_lifecycle_callback = AERON_LIFECYCLE_CALLBACK_ON_START;
            if (NULL != svc->on_start) { svc->on_start(svc->clientd, (aeron_cluster_t *)agent, &snap_img); }
            agent->active_lifecycle_callback = AERON_LIFECYCLE_CALLBACK_NONE;

            aeron_subscription_close(snapshot_sub, NULL, NULL);
        }
        else
        {
            /* No snapshot — fresh start */
            aeron_clustered_service_t *svc = ctx->service;
            agent->active_lifecycle_callback = AERON_LIFECYCLE_CALLBACK_ON_START;
            if (NULL != svc->on_start) { svc->on_start(svc->clientd, (aeron_cluster_t *)agent, NULL); }
            agent->active_lifecycle_callback = AERON_LIFECYCLE_CALLBACK_NONE;
        }
    }
    else
    {
        /* No recovery counter — initial startup */
        agent->active_lifecycle_callback = AERON_LIFECYCLE_CALLBACK_ON_START;
        if (NULL != ctx->service->on_start) { ctx->service->on_start(ctx->service->clientd, (aeron_cluster_t *)agent, NULL); }
        agent->active_lifecycle_callback = AERON_LIFECYCLE_CALLBACK_NONE;
    }

    aeron_cluster_consensus_module_proxy_ack(
        &agent->consensus_module_proxy,
        0,   /* logPosition */
        agent->cluster_time,
        agent->ack_id++,
        -1,  /* relevantId (NULL_VALUE) */
        ctx->service_id);

    return 0;
}

int aeron_clustered_service_agent_do_work(aeron_clustered_service_agent_t *agent,
                                           int64_t now_ns)
{
    int work_count = 0;

    /* 1. Poll CM→service channel */
    if (NULL != agent->service_adapter)
    {
        work_count += aeron_cluster_service_adapter_poll(agent->service_adapter);
    }

    /* 2. Poll the log (bounded by commitPosition) */
    if (NULL != agent->log_adapter)
    {
        int64_t commit_pos = INT64_MAX;  /* default: unbounded */
        if (agent->commit_position_counter_id >= 0)
        {
            int64_t *commit_pos_ptr = aeron_counters_reader_addr(
                agent->counters_reader, agent->commit_position_counter_id);
            commit_pos = (NULL != commit_pos_ptr) ? *commit_pos_ptr : INT64_MAX;
        }

        int polled = aeron_cluster_bounded_log_adapter_poll(
            agent->log_adapter, commit_pos);

        if (polled < 0)
        {
            /* Log image closed — clean up */
            aeron_cluster_bounded_log_adapter_close(agent->log_adapter);
            agent->log_adapter = NULL;
        }
        else
        {
            work_count += polled;
        }
    }

    /* 3. Optional background work (with lifecycle guard) */
    aeron_clustered_service_t *svc = agent->ctx->service;
    if (NULL != svc->do_background_work)
    {
        agent->active_lifecycle_callback = AERON_LIFECYCLE_CALLBACK_DO_BACKGROUND_WORK;
        work_count += svc->do_background_work(svc->clientd, now_ns);
        agent->active_lifecycle_callback = AERON_LIFECYCLE_CALLBACK_NONE;
    }

    /* 4. Deferred ACK position — mirrors Java requestedAckPosition */
    if (agent->requested_ack_position >= 0 &&
        agent->log_position >= agent->requested_ack_position)
    {
        aeron_cluster_consensus_module_proxy_ack(
            &agent->consensus_module_proxy,
            agent->log_position,
            agent->cluster_time,
            agent->ack_id++,
            -1,
            agent->ctx->service_id);
        agent->requested_ack_position = -1;
        work_count++;
    }

    /* 5. Termination position check — call on_terminate, then ACK with retry */
    if (agent->termination_position >= 0 && agent->log_position >= agent->termination_position)
    {
        agent->is_service_active = false;

        /* Invoke on_terminate callback (mirrors Java service.onTerminate()) */
        if (NULL != svc->on_terminate)
        {
            agent->active_lifecycle_callback = AERON_LIFECYCLE_CALLBACK_ON_TERMINATE;
            svc->on_terminate(svc->clientd, (aeron_cluster_t *)agent);
            agent->active_lifecycle_callback = AERON_LIFECYCLE_CALLBACK_NONE;
        }

        /* Send termination ACK with retry (up to 5 attempts, mirrors Java) */
        for (int attempt = 0; attempt < 5; attempt++)
        {
            if (aeron_cluster_consensus_module_proxy_ack(
                &agent->consensus_module_proxy,
                agent->termination_position,
                agent->cluster_time,
                agent->ack_id++,
                -1,
                agent->ctx->service_id))
            {
                break;
            }
        }
        agent->termination_position = -1;
        work_count++;
    }

    /* 5. Mark file activity update (every ~1 second) */
    if (now_ns >= agent->mark_file_update_deadline_ns)
    {
        agent->mark_file_update_deadline_ns = now_ns + INT64_C(1000000000);
        if (NULL != agent->ctx->mark_file_update_fn)
        {
            agent->ctx->mark_file_update_fn(
                agent->ctx->mark_file_update_clientd, now_ns / INT64_C(1000000));
        }
        work_count++;
    }

    return work_count;
}

int aeron_clustered_service_agent_close(aeron_clustered_service_agent_t *agent)
{
    if (NULL != agent)
    {
        /* Invoke on_terminate if service is still active — mirrors Java onClose() */
        if (agent->is_service_active)
        {
            agent->is_service_active = false;
            aeron_clustered_service_t *svc = agent->ctx->service;
            if (NULL != svc && NULL != svc->on_terminate)
            {
                agent->active_lifecycle_callback = AERON_LIFECYCLE_CALLBACK_ON_TERMINATE;
                svc->on_terminate(svc->clientd, (aeron_cluster_t *)agent);
                agent->active_lifecycle_callback = AERON_LIFECYCLE_CALLBACK_NONE;
            }
        }

        for (size_t i = 0; i < agent->sessions_count; i++)
        {
            aeron_cluster_client_session_close_and_free(agent->sessions[i]);
        }
        aeron_free(agent->sessions);

        aeron_cluster_service_adapter_close(agent->service_adapter);
        aeron_cluster_bounded_log_adapter_close(agent->log_adapter);

        if (NULL != agent->control_subscription)
        {
            aeron_subscription_close(agent->control_subscription, NULL, NULL);
        }
        if (NULL != agent->service_publication)
        {
            aeron_exclusive_publication_close(agent->service_publication, NULL, NULL);
        }

        aeron_free(agent);
    }
    return 0;
}

/* -----------------------------------------------------------------------
 * Cluster API
 * ----------------------------------------------------------------------- */
int32_t aeron_cluster_member_id(aeron_cluster_t *c)     { return c->member_id; }
int64_t aeron_cluster_log_position(aeron_cluster_t *c)  { return c->log_position; }
int64_t aeron_cluster_time(aeron_cluster_t *c)           { return c->cluster_time; }
int32_t aeron_cluster_role(aeron_cluster_t *c)           { return c->role; }

aeron_cluster_client_session_t *aeron_cluster_get_client_session(
    aeron_cluster_t *cluster, int64_t id)
{
    return find_session(cluster, id);
}

bool aeron_cluster_close_client_session(aeron_cluster_t *cluster, int64_t id)
{
    aeron_cluster_client_session_t *session = find_session(cluster, id);
    if (NULL == session) { return false; }
    aeron_cluster_client_session_mark_closing(session);
    return aeron_cluster_consensus_module_proxy_close_session(
        &cluster->consensus_module_proxy, id);
}

bool aeron_cluster_schedule_timer(aeron_cluster_t *cluster,
                                   int64_t correlation_id, int64_t deadline)
{
    return aeron_cluster_consensus_module_proxy_schedule_timer(
        &cluster->consensus_module_proxy, correlation_id, deadline);
}

bool aeron_cluster_cancel_timer(aeron_cluster_t *cluster, int64_t correlation_id)
{
    return aeron_cluster_consensus_module_proxy_cancel_timer(
        &cluster->consensus_module_proxy, correlation_id);
}

int64_t aeron_cluster_service_offer(aeron_cluster_t *cluster,
                             int64_t cluster_session_id,
                             const uint8_t *buffer, size_t length)
{
    return aeron_cluster_consensus_module_proxy_offer(
        &cluster->consensus_module_proxy,
        cluster_session_id,
        cluster->leadership_term_id,
        buffer, length);
}

int64_t aeron_cluster_service_offerv(
    aeron_clustered_service_agent_t *agent,
    int64_t cluster_session_id,
    const aeron_iovec_t *iov,
    size_t iovcnt)
{
    aeron_cluster_client_session_t *session = find_session(agent, cluster_session_id);
    if (NULL == session)
    {
        AERON_SET_ERR(EINVAL, "unknown cluster session id: %" PRId64, cluster_session_id);
        return AERON_PUBLICATION_ERROR;
    }

    if (NULL == session->response_publication)
    {
        if (aeron_cluster_client_session_connect(session) < 0)
        {
            return AERON_PUBLICATION_ERROR;
        }
    }

    /* Build the 32-byte session message header */
    uint8_t hdr_buf[SESSION_HEADER_LENGTH];
    struct aeron_cluster_client_messageHeader msg_hdr;
    struct aeron_cluster_client_sessionMessageHeader hdr;

    if (NULL == aeron_cluster_client_sessionMessageHeader_wrap_and_apply_header(
        &hdr, (char *)hdr_buf, 0, sizeof(hdr_buf), &msg_hdr))
    {
        return AERON_PUBLICATION_ERROR;
    }

    aeron_cluster_client_sessionMessageHeader_set_leadershipTermId(&hdr, agent->leadership_term_id);
    aeron_cluster_client_sessionMessageHeader_set_clusterSessionId(&hdr, cluster_session_id);
    aeron_cluster_client_sessionMessageHeader_set_timestamp(&hdr, 0);

    /* Prepend header as first vector, then append user vectors */
    size_t total_iovcnt = 1 + iovcnt;
    aeron_iovec_t vectors[total_iovcnt];
    vectors[0].iov_base = hdr_buf;
    vectors[0].iov_len  = SESSION_HEADER_LENGTH;
    for (size_t i = 0; i < iovcnt; i++)
    {
        vectors[1 + i] = iov[i];
    }

    return aeron_publication_offerv(session->response_publication, vectors, total_iovcnt, NULL, NULL);
}

int64_t aeron_cluster_service_try_claim(
    aeron_clustered_service_agent_t *agent,
    int64_t cluster_session_id,
    size_t length,
    aeron_buffer_claim_t *buffer_claim)
{
    aeron_cluster_client_session_t *session = find_session(agent, cluster_session_id);
    if (NULL == session)
    {
        AERON_SET_ERR(EINVAL, "unknown cluster session id: %" PRId64, cluster_session_id);
        return AERON_PUBLICATION_ERROR;
    }

    if (NULL == session->response_publication)
    {
        if (aeron_cluster_client_session_connect(session) < 0)
        {
            return AERON_PUBLICATION_ERROR;
        }
    }

    /* Claim space for session header + user payload */
    int64_t result = aeron_publication_try_claim(
        session->response_publication,
        SESSION_HEADER_LENGTH + length,
        buffer_claim);

    if (result > 0)
    {
        /* Write the session message header into the claimed buffer */
        struct aeron_cluster_client_messageHeader msg_hdr;
        struct aeron_cluster_client_sessionMessageHeader hdr;

        if (NULL != aeron_cluster_client_sessionMessageHeader_wrap_and_apply_header(
            &hdr, (char *)buffer_claim->data, 0, SESSION_HEADER_LENGTH, &msg_hdr))
        {
            aeron_cluster_client_sessionMessageHeader_set_leadershipTermId(&hdr, agent->leadership_term_id);
            aeron_cluster_client_sessionMessageHeader_set_clusterSessionId(&hdr, cluster_session_id);
            aeron_cluster_client_sessionMessageHeader_set_timestamp(&hdr, 0);
        }

        /* Advance the data pointer past the header so caller writes only payload */
        buffer_claim->data   += SESSION_HEADER_LENGTH;
        buffer_claim->length -= SESSION_HEADER_LENGTH;
    }

    return result;
}

int aeron_cluster_client_sessions(
    aeron_clustered_service_agent_t *agent,
    aeron_cluster_client_session_t ***sessions_out,
    size_t *count_out)
{
    if (NULL == agent)
    {
        AERON_SET_ERR(EINVAL, "%s", "agent is NULL");
        return -1;
    }

    *sessions_out = agent->sessions;
    *count_out    = agent->sessions_count;
    return 0;
}

static const char *ROLE_NAMES[] = { "FOLLOWER", "CANDIDATE", "LEADER" };

const char *aeron_cluster_role_name(aeron_clustered_service_agent_t *agent)
{
    int32_t role = agent->role;
    if (role >= 0 && role <= 2)
    {
        return ROLE_NAMES[role];
    }
    return "UNKNOWN";
}

aeron_t *aeron_cluster_aeron(aeron_clustered_service_agent_t *agent)
{
    return agent->aeron;
}

void aeron_cluster_idle(aeron_clustered_service_agent_t *agent, int work_count)
{
    if (NULL != agent->ctx->idle_strategy_func)
    {
        agent->ctx->idle_strategy_func(agent->ctx->idle_strategy_state, work_count);
    }
}

/* -----------------------------------------------------------------------
 * ServiceAdapter callbacks
 * ----------------------------------------------------------------------- */
void aeron_clustered_service_agent_on_join_log(
    aeron_clustered_service_agent_t *agent,
    int64_t log_position, int64_t max_log_position,
    int32_t member_id, int32_t log_session_id, int32_t log_stream_id,
    bool is_startup, aeron_cluster_role_t role,
    const char *log_channel)
{
    agent->member_id        = member_id;
    agent->log_position     = log_position;
    agent->max_log_position = max_log_position;

    /* Fire on_role_change if role differs (with lifecycle guard) */
    if ((int32_t)role != agent->role)
    {
        agent->role = (int32_t)role;
        aeron_clustered_service_t *svc = agent->ctx->service;
        if (NULL != svc->on_role_change)
        {
            agent->active_lifecycle_callback = AERON_LIFECYCLE_CALLBACK_ON_ROLE_CHANGE;
            svc->on_role_change(svc->clientd, role);
            agent->active_lifecycle_callback = AERON_LIFECYCLE_CALLBACK_NONE;
        }
    }
    else
    {
        agent->role = (int32_t)role;
    }

    /* Subscribe to the log and find the image by session id */
    aeron_subscription_t *log_sub = NULL;
    {
        aeron_async_add_subscription_t *async_sub = NULL;
        if (aeron_async_add_subscription(
            &async_sub, agent->aeron, log_channel, log_stream_id,
            NULL, NULL, NULL, NULL) < 0) { return; }
        int rc = 0;
        do { rc = aeron_async_add_subscription_poll(&log_sub, async_sub); }
        while (0 == rc);
        if (rc < 0) { return; }
    }

    /* Spin until the image for logSessionId appears (with timeout + sleep) */
    aeron_image_t *log_image = NULL;
    for (int spin = 0; spin < 5000 && NULL == log_image; spin++)
    {
        log_image = aeron_subscription_image_by_session_id(log_sub, log_session_id);
        if (NULL == log_image)
        {
            aeron_micro_sleep(1000); /* 1ms — give conductor time to process */
        }
    }
    if (NULL == log_image)
    {
        aeron_subscription_close(log_sub, NULL, NULL);
        return;
    }

    /* Validate image position for non-startup joins.
     * The CM sets up the log channel (archive replay) to start at log_position,
     * so the image must already be positioned there. */
    if (!is_startup)
    {
        int64_t image_pos = aeron_image_position(log_image);
        if (log_position != image_pos)
        {
            AERON_SET_ERR(EINVAL,
                "log position mismatch joining log: expected=%" PRId64 " image=%" PRId64,
                log_position, image_pos);
            return;
        }
    }

    if (NULL != agent->log_adapter)
    {
        aeron_cluster_bounded_log_adapter_close(agent->log_adapter);
        agent->log_adapter = NULL;
    }

    if (aeron_cluster_bounded_log_adapter_create(
        &agent->log_adapter, log_sub, log_image,
        max_log_position, agent) < 0)
    {
        return;
    }

    /* Connect / disconnect session egress publications based on role */
    if (AERON_CLUSTER_ROLE_LEADER == role)
    {
        for (size_t i = 0; i < agent->sessions_count; i++)
        {
            aeron_cluster_client_session_connect(agent->sessions[i]);
        }
    }
    else
    {
        for (size_t i = 0; i < agent->sessions_count; i++)
        {
            aeron_cluster_client_session_disconnect(agent->sessions[i]);
        }
    }

    /* Ack that we have joined the log */
    aeron_cluster_consensus_module_proxy_ack(
        &agent->consensus_module_proxy,
        log_position,
        agent->cluster_time,
        agent->ack_id++,
        -1,
        agent->ctx->service_id);
}

void aeron_clustered_service_agent_on_service_termination_position(
    aeron_clustered_service_agent_t *agent, int64_t log_position)
{
    agent->termination_position = log_position;
}

void aeron_clustered_service_agent_on_request_service_ack(
    aeron_clustered_service_agent_t *agent, int64_t log_position)
{
    /* Defer ACK until log_position is reached — mirrors Java requestedAckPosition.
     * The ACK is sent in do_work when log_position >= requested_ack_position. */
    agent->requested_ack_position = log_position;
}

/* -----------------------------------------------------------------------
 * BoundedLogAdapter callbacks
 * ----------------------------------------------------------------------- */
void aeron_clustered_service_agent_on_session_open(
    aeron_clustered_service_agent_t *agent,
    int64_t cluster_session_id, int64_t correlation_id,
    int64_t log_position, int64_t timestamp,
    int32_t response_stream_id, const char *response_channel,
    const uint8_t *encoded_principal, size_t principal_length)
{
    agent->log_position  = log_position;
    agent->cluster_time  = timestamp;

    aeron_cluster_client_session_t *session = NULL;
    if (aeron_cluster_client_session_create(
        &session,
        cluster_session_id, response_stream_id, response_channel,
        encoded_principal, principal_length,
        agent->aeron) < 0)
    {
        return;
    }

    if (add_session(agent, session) < 0)
    {
        aeron_cluster_client_session_close_and_free(session);
        return;
    }

    aeron_clustered_service_t *svc = agent->ctx->service;
    if (NULL != svc->on_session_open)
    {
        svc->on_session_open(svc->clientd, session, timestamp);
    }
}

void aeron_clustered_service_agent_on_session_close(
    aeron_clustered_service_agent_t *agent,
    int64_t cluster_session_id, int64_t timestamp,
    aeron_cluster_close_reason_t close_reason)
{
    agent->cluster_time = timestamp;

    aeron_cluster_client_session_t *session = find_session(agent, cluster_session_id);
    if (NULL == session) { return; }

    aeron_clustered_service_t *svc = agent->ctx->service;
    if (NULL != svc->on_session_close) { svc->on_session_close(svc->clientd, session, timestamp, close_reason); }

    remove_session(agent, session);
    aeron_cluster_client_session_close_and_free(session);
}

void aeron_clustered_service_agent_on_session_message(
    aeron_clustered_service_agent_t *agent,
    int64_t cluster_session_id, int64_t timestamp,
    const uint8_t *buffer, size_t length)
{
    agent->cluster_time = timestamp;

    aeron_cluster_client_session_t *session = find_session(agent, cluster_session_id);

    aeron_clustered_service_t *svc = agent->ctx->service;
    svc->on_session_message(svc->clientd, session, timestamp, buffer, length);
}

void aeron_clustered_service_agent_on_timer_event(
    aeron_clustered_service_agent_t *agent,
    int64_t correlation_id, int64_t timestamp)
{
    agent->cluster_time = timestamp;

    aeron_clustered_service_t *svc = agent->ctx->service;
    if (NULL != svc->on_timer_event) { svc->on_timer_event(svc->clientd, correlation_id, timestamp); }
}

void aeron_clustered_service_agent_on_service_action(
    aeron_clustered_service_agent_t *agent,
    int64_t log_position, int64_t timestamp,
    int32_t action, int32_t flags)
{
    agent->log_position  = log_position;
    agent->cluster_time  = timestamp;

    /* action == 0 means SNAPSHOT in Java's ClusterAction enum */
    if (0 == action && CLUSTER_ACTION_FLAGS_DEFAULT == flags)
    {
        agent->snapshot_log_position      = log_position;
        agent->snapshot_leadership_term_id = agent->leadership_term_id;

        /* Open snapshot publication (IPC exclusive pub on snapshot channel) */
        aeron_exclusive_publication_t *snapshot_pub = NULL;
        {
            aeron_async_add_exclusive_publication_t *async_pub = NULL;
            if (aeron_async_add_exclusive_publication(
                &async_pub, agent->aeron,
                agent->ctx->snapshot_channel,
                agent->ctx->snapshot_stream_id) < 0) { return; }
            int rc = 0;
            do { rc = aeron_async_add_exclusive_publication_poll(&snapshot_pub, async_pub); }
            while (0 == rc);
            if (rc < 0) { return; }
        }

        /* Write begin marker */
        aeron_cluster_service_snapshot_taker_mark_begin(
            snapshot_pub,
            AERON_CLUSTER_SNAPSHOT_TYPE_ID,
            log_position,
            agent->leadership_term_id,
            0,
            agent->ctx->app_version);

        /* Write all live sessions */
        for (size_t i = 0; i < agent->sessions_count; i++)
        {
            aeron_cluster_service_snapshot_taker_snapshot_session(
                snapshot_pub, agent->sessions[i]);
        }

        /* Write end marker (matches Java: BEGIN → sessions → END → user data) */
        aeron_cluster_service_snapshot_taker_mark_end(
            snapshot_pub,
            AERON_CLUSTER_SNAPSHOT_TYPE_ID,
            log_position,
            agent->leadership_term_id,
            0,
            agent->ctx->app_version);

        /* User writes their own state after END so loader can stop at END */
        aeron_clustered_service_t *svc = agent->ctx->service;
        if (NULL != svc->on_take_snapshot)
        {
            /* Track snapshot duration — mirrors Java SnapshotDurationTracker */
            int64_t snap_start_ns = aeron_nano_clock();
            svc->on_take_snapshot(svc->clientd, snapshot_pub);
            int64_t snap_duration_ns = aeron_nano_clock() - snap_start_ns;
            if (snap_duration_ns > agent->max_snapshot_duration_ns)
            {
                agent->max_snapshot_duration_ns = snap_duration_ns;
            }
        }

        aeron_exclusive_publication_close(snapshot_pub, NULL, NULL);

        /* Ack the snapshot */
        aeron_cluster_consensus_module_proxy_ack(
            &agent->consensus_module_proxy,
            log_position,
            agent->cluster_time,
            agent->ack_id++,
            -1,
            agent->ctx->service_id);
    }
}

void aeron_clustered_service_agent_on_new_leadership_term_event(
    aeron_clustered_service_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position,
    int64_t timestamp, int64_t term_base_log_position,
    int32_t leader_member_id, int32_t log_session_id,
    int32_t app_version)
{
    /* App version validation */
    if (agent->ctx->app_version != 0 && app_version != 0 &&
        agent->ctx->app_version != app_version)
    {
        AERON_SET_ERR(EINVAL,
            "incompatible app version: ctx=%" PRId32 " log=%" PRId32,
            agent->ctx->app_version, app_version);
        /* Notify error handler but continue — do not crash the agent */
        if (NULL != agent->ctx->error_handler)
        {
            agent->ctx->error_handler(agent->ctx->error_handler_clientd,
                EINVAL, aeron_errmsg());
        }
    }

    agent->leadership_term_id = leadership_term_id;
    agent->log_position       = log_position;
    agent->cluster_time       = timestamp;

    aeron_clustered_service_t *svc = agent->ctx->service;
    if (NULL != svc->on_new_leadership_term_event)
    {
        svc->on_new_leadership_term_event(
            svc->clientd,
            leadership_term_id, log_position, timestamp,
            term_base_log_position, leader_member_id,
            log_session_id, app_version);
    }
}

void aeron_clustered_service_agent_on_membership_change(
    aeron_clustered_service_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position,
    int64_t timestamp, int32_t member_id, int32_t change_type)
{
    agent->log_position  = log_position;
    agent->cluster_time  = timestamp;
    /* Role change not triggered by MembershipChangeEvent; role is set in onJoinLog */
}
