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

#include "aeron_clustered_service_agent.h"
#include "aeron_cluster_service_context.h"
#include "aeron_cluster_service_snapshot_taker.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

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
        aeron_cluster_client_session_t **tmp;
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

    /* Notify CM we are ready (no snapshot on first start) */
    ctx->service->on_start(ctx->service->clientd, agent, NULL);

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
        if (agent->commit_position_counter_id >= 0)
        {
            int64_t *commit_pos_ptr = aeron_counters_reader_addr(
                agent->counters_reader, agent->commit_position_counter_id);
            int64_t commit_pos = (NULL != commit_pos_ptr) ? *commit_pos_ptr : -1;

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
    }

    /* 3. Optional background work */
    aeron_clustered_service_t *svc = agent->ctx->service;
    if (NULL != svc->do_background_work)
    {
        work_count += svc->do_background_work(svc->clientd, now_ns);
    }

    return work_count;
}

int aeron_clustered_service_agent_close(aeron_clustered_service_agent_t *agent)
{
    if (NULL != agent)
    {
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

int64_t aeron_cluster_offer(aeron_cluster_t *cluster,
                             int64_t cluster_session_id,
                             const uint8_t *buffer, size_t length)
{
    return aeron_cluster_consensus_module_proxy_offer(
        &cluster->consensus_module_proxy,
        cluster_session_id,
        cluster->leadership_term_id,
        buffer, length);
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
    agent->role             = (int32_t)role;

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

    /* Spin until the image for logSessionId appears */
    aeron_image_t *log_image = NULL;
    while (NULL == log_image)
    {
        log_image = aeron_subscription_image_by_session_id(log_sub, log_session_id);
        aeron_cluster_service_context_invoke_aeron_client(agent->ctx);
    }

    /* Seek to logPosition if this is not a startup from position 0 */
    if (log_position > 0)
    {
        /* In Java this triggers a replay from position; in C we rely on
         * archive replay being configured to start at log_position. */
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
    aeron_cluster_consensus_module_proxy_ack(
        &agent->consensus_module_proxy,
        log_position,
        agent->cluster_time,
        agent->ack_id++,
        -1,
        agent->ctx->service_id);
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
    svc->on_session_open(svc->clientd, session, timestamp);
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
    svc->on_session_close(svc->clientd, session, timestamp, close_reason);

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
    svc->on_timer_event(svc->clientd, correlation_id, timestamp);
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

        /* User writes their own state */
        aeron_clustered_service_t *svc = agent->ctx->service;
        svc->on_take_snapshot(svc->clientd, snapshot_pub);

        /* Write end marker */
        aeron_cluster_service_snapshot_taker_mark_end(
            snapshot_pub,
            AERON_CLUSTER_SNAPSHOT_TYPE_ID,
            log_position,
            agent->leadership_term_id,
            0,
            agent->ctx->app_version);

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
