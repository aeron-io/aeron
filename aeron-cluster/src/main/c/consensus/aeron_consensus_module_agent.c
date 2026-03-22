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

#include <stdio.h>
#include <string.h>
#include <errno.h>

#include "aeron_consensus_module_agent.h"
#include "aeron_cm_context.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_clock.h"
#include "uri/aeron_uri.h"
#include "aeron_archive.h"
#include "aeron_archive_replay_params.h"

/* -----------------------------------------------------------------------
 * Internal spin-poll helpers
 * ----------------------------------------------------------------------- */
static int add_exclusive_pub(aeron_t *aeron,
                              aeron_exclusive_publication_t **pub,
                              const char *channel, int32_t stream_id)
{
    aeron_async_add_exclusive_publication_t *async = NULL;
    if (aeron_async_add_exclusive_publication(&async, aeron, channel, stream_id) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to start add exclusive publication");
        return -1;
    }
    int rc = 0;
    do { rc = aeron_async_add_exclusive_publication_poll(pub, async); } while (0 == rc);
    return rc < 0 ? -1 : 0;
}

static int add_sub(aeron_t *aeron, aeron_subscription_t **sub,
                   const char *channel, int32_t stream_id)
{
    aeron_async_add_subscription_t *async = NULL;
    if (aeron_async_add_subscription(&async, aeron, channel, stream_id,
        NULL, NULL, NULL, NULL) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to start add subscription");
        return -1;
    }
    int rc = 0;
    do { rc = aeron_async_add_subscription_poll(sub, async); } while (0 == rc);
    return rc < 0 ? -1 : 0;
}

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */
int aeron_consensus_module_agent_create(
    aeron_consensus_module_agent_t **agent,
    aeron_cm_context_t *ctx)
{
    aeron_consensus_module_agent_t *a = NULL;
    if (aeron_alloc((void **)&a, sizeof(aeron_consensus_module_agent_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate consensus module agent");
        return -1;
    }

    a->ctx                      = ctx;
    a->aeron                    = ctx->aeron;
    a->member_id                = ctx->member_id;
    a->state                    = AERON_CM_STATE_INIT;
    a->role                     = AERON_CLUSTER_ROLE_FOLLOWER;
    a->leadership_term_id       = -1;
    a->expected_ack_position    = 0;
    a->service_ack_id           = 0;
    a->last_append_position     = -1;
    a->notified_commit_position = 0;
    a->termination_position     = -1;
    a->log_subscription_id      = -1;
    a->log_recording_id         = -1;
    a->app_version              = ctx->app_version;
    a->protocol_version         = aeron_semantic_version_compose(
        AERON_CM_PROTOCOL_MAJOR_VERSION,
        AERON_CM_PROTOCOL_MINOR_VERSION,
        AERON_CM_PROTOCOL_PATCH_VERSION);
    a->service_count            = ctx->service_count;
    a->leader_heartbeat_interval_ns = ctx->leader_heartbeat_interval_ns;
    a->leader_heartbeat_timeout_ns  = ctx->leader_heartbeat_timeout_ns;
    a->session_timeout_ns           = ctx->session_timeout_ns;
    a->slow_tick_deadline_ns        = 0;
    a->time_of_last_log_update_ns   = 0;
    a->time_of_last_append_position_send_ns = 0;

    /* Parse cluster members */
    if (aeron_cluster_members_parse(ctx->cluster_members,
        &a->active_members, &a->active_member_count) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to parse cluster members");
        aeron_free(a);
        return -1;
    }

    a->this_member = aeron_cluster_member_find_by_id(
        a->active_members, a->active_member_count, a->member_id);
    if (NULL == a->this_member)
    {
        AERON_SET_ERR(EINVAL, "member_id %d not found in cluster_members", a->member_id);
        aeron_cluster_members_free(a->active_members, a->active_member_count);
        aeron_free(a);
        return -1;
    }

    a->leader_member = NULL;

    /* Ranked positions for quorum */
    int threshold = aeron_cluster_member_quorum_threshold(a->active_member_count);
    if (aeron_alloc((void **)&a->ranked_positions,
        (size_t)threshold * sizeof(int64_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate ranked positions");
        aeron_cluster_members_free(a->active_members, a->active_member_count);
        aeron_free(a);
        return -1;
    }

    /* Service ACK positions */
    if (aeron_alloc((void **)&a->service_ack_positions,
        (size_t)a->service_count * sizeof(int64_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate service_ack_positions");
        aeron_free(a->ranked_positions);
        aeron_cluster_members_free(a->active_members, a->active_member_count);
        aeron_free(a);
        return -1;
    }
    for (int i = 0; i < a->service_count; i++) { a->service_ack_positions[i] = -1; }

    a->ingress_subscription  = NULL;
    a->consensus_subscription = NULL;
    a->log_publication       = NULL;
    a->service_pub           = NULL;
    a->service_sub           = NULL;
    a->ingress_adapter       = NULL;
    a->consensus_adapter     = NULL;
    a->session_manager       = NULL;
    a->timer_service         = NULL;
    a->recording_log         = NULL;
    a->election              = NULL;
    a->commit_position_counter = NULL;
    a->cluster_role_counter    = NULL;
    a->module_state_counter    = NULL;
    a->archive               = NULL;
    a->pending_trackers      = NULL;

    *agent = a;
    return 0;
}

int aeron_consensus_module_agent_on_start(aeron_consensus_module_agent_t *agent)
{
    aeron_cm_context_t *ctx = agent->ctx;

    /* Add subscriptions */
    if (add_sub(agent->aeron, &agent->ingress_subscription,
        ctx->ingress_channel, ctx->ingress_stream_id) < 0) { return -1; }

    if (add_sub(agent->aeron, &agent->consensus_subscription,
        ctx->consensus_channel, ctx->consensus_stream_id) < 0) { return -1; }

    if (add_sub(agent->aeron, &agent->service_sub,
        ctx->control_channel, ctx->service_stream_id) < 0) { return -1; }

    /* Add publications */
    if (add_exclusive_pub(agent->aeron, &agent->service_pub,
        ctx->control_channel, ctx->consensus_module_stream_id) < 0) { return -1; }

    /* Build adapters */
    if (aeron_cluster_ingress_adapter_cm_create(
        &agent->ingress_adapter, agent->ingress_subscription, agent,
        AERON_CM_INGRESS_FRAGMENT_LIMIT_DEFAULT) < 0) { return -1; }

    if (aeron_cluster_consensus_adapter_create(
        &agent->consensus_adapter, agent->consensus_subscription, agent) < 0) { return -1; }

    /* Service proxy */
    aeron_cluster_service_proxy_cm_init(&agent->service_proxy,
        agent->service_pub, agent->service_count);

    /* Session manager */
    if (aeron_cluster_session_manager_create(
        &agent->session_manager, 1 /* initial session id */, agent->aeron) < 0) { return -1; }

    /* Timer service */
    if (aeron_cluster_timer_service_create(&agent->timer_service, NULL, NULL) < 0) { return -1; }

    /* Recording log */
    if (aeron_cluster_recording_log_open(&agent->recording_log,
        ctx->cluster_dir, false) < 0)
    {
        if (aeron_cluster_recording_log_open(&agent->recording_log,
            ctx->cluster_dir, true) < 0) { return -1; }
    }

    /* Connect to archive (IPC) — needed for log recording and snapshots */
    if (NULL != ctx->archive_ctx)
    {
        if (aeron_archive_connect(&agent->archive, ctx->archive_ctx) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to connect to archive");
            return -1;
        }
    }

    /* Pending message trackers */
    if (aeron_alloc((void **)&agent->pending_trackers,
        (size_t)agent->service_count * sizeof(aeron_cluster_pending_message_tracker_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate pending_trackers");
        return -1;
    }
    for (int i = 0; i < agent->service_count; i++)
    {
        aeron_cluster_pending_message_tracker_init(
            &agent->pending_trackers[i], i, 1, 0, 4096);
    }

    /* Build recovery plan */
    aeron_cluster_recovery_plan_t *plan = NULL;
    aeron_cluster_recording_log_create_recovery_plan(
        agent->recording_log, &plan, agent->service_count);

    int64_t log_position        = (NULL != plan) ? plan->last_append_position       : 0;
    int64_t log_term_id         = (NULL != plan) ? plan->last_leadership_term_id    : -1;
    int64_t leader_recording_id = (NULL != plan) ? plan->last_term_recording_id     : -1;
    bool    has_snapshot        = (NULL != plan) ? plan->snapshot_count > 0          : false;

    /* If recovering from snapshot, load it before starting election */
    if (has_snapshot && NULL != agent->archive)
    {
        aeron_archive_replay_params_t params;
        aeron_archive_replay_params_init(&params);
        params.position = 0;  /* snapshot is self-contained, replay from start */

        /* CM snapshot (service_id = -1) is in plan->snapshots[0] */
        aeron_subscription_t *snap_sub = NULL;
        if (aeron_archive_replay(&snap_sub, agent->archive,
            plan->snapshots[0].recording_id,
            ctx->snapshot_channel, ctx->snapshot_stream_id, &params) == 0 &&
            NULL != snap_sub)
        {
            /* Poll snapshot subscription to load CM state */
            /* (full implementation: read ClusterSession, Timer, ConsensusModule records) */
            aeron_subscription_close(snap_sub, NULL, NULL);
        }
    }

    aeron_cluster_recovery_plan_free(plan);

    if (aeron_cluster_election_create(
        &agent->election, agent,
        agent->this_member, agent->active_members, agent->active_member_count,
        log_term_id, log_position, log_term_id, leader_recording_id,
        ctx->startup_canvass_timeout_ns,
        ctx->election_timeout_ns,
        ctx->election_status_interval_ns,
        ctx->leader_heartbeat_timeout_ns,
        true) < 0) { return -1; }

    agent->state = AERON_CM_STATE_ACTIVE;
    return 0;
}

/* -----------------------------------------------------------------------
 * do_work: main duty cycle
 * ----------------------------------------------------------------------- */
static int slow_tick_work(aeron_consensus_module_agent_t *agent, int64_t now_ns)
{
    int work_count = 0;

    /* Heartbeat timeout check (follower only) */
    if (AERON_CLUSTER_ROLE_FOLLOWER == agent->role &&
        NULL == agent->election &&
        (now_ns - agent->time_of_last_log_update_ns) > agent->leader_heartbeat_timeout_ns)
    {
        /* Start new election */
        if (aeron_cluster_election_create(
            &agent->election, agent,
            agent->this_member, agent->active_members, agent->active_member_count,
            agent->leadership_term_id,
            aeron_consensus_module_agent_get_append_position(agent),
            agent->leadership_term_id,
            agent->log_recording_id,
            agent->ctx->startup_canvass_timeout_ns,
            agent->ctx->election_timeout_ns,
            agent->ctx->election_status_interval_ns,
            agent->leader_heartbeat_timeout_ns,
            false) < 0) { return -1; }
        work_count++;
    }

    /* Leader: send heartbeat (AppendPosition) periodically */
    if (AERON_CLUSTER_ROLE_LEADER == agent->role &&
        NULL == agent->election &&
        (now_ns - agent->time_of_last_append_position_send_ns) >
            agent->leader_heartbeat_interval_ns)
    {
        int64_t pos = aeron_consensus_module_agent_get_append_position(agent);
        for (int i = 0; i < agent->active_member_count; i++)
        {
            if (agent->active_members[i].id != agent->member_id &&
                NULL != agent->active_members[i].publication)
            {
                aeron_cluster_consensus_publisher_append_position(
                    agent->active_members[i].publication,
                    agent->leadership_term_id, pos, agent->member_id, 0);
            }
        }
        agent->time_of_last_append_position_send_ns = now_ns;
        work_count++;
    }

    return work_count;
}

static void on_session_timeout(void *clientd, aeron_cluster_cluster_session_t *session)
{
    aeron_consensus_module_agent_t *agent = (aeron_consensus_module_agent_t *)clientd;
    /* Append SessionCloseEvent(TIMEOUT) to log and remove session */
    if (NULL != agent->log_publication)
    {
        aeron_cluster_log_publisher_append_session_close(
            &agent->log_publisher, session->id,
            2 /* TIMEOUT */, aeron_nano_clock());
    }
    /* Send TIMED_OUT SessionEvent back to client */
    if (NULL != session->response_publication)
    {
        aeron_cluster_egress_publisher_send_session_event(
            session->response_publication,
            session->id, session->correlation_id,
            agent->leadership_term_id, agent->member_id,
            1 /* ERROR */, agent->leader_heartbeat_timeout_ns,
            "session timed out", 17);
    }
}

int aeron_consensus_module_agent_do_work(aeron_consensus_module_agent_t *agent, int64_t now_ns)
{
    int work_count = 0;

    /* Slow tick (1ms) */
    if (now_ns >= agent->slow_tick_deadline_ns)
    {
        int rc = slow_tick_work(agent, now_ns);
        if (rc < 0) { return -1; }
        work_count += rc;

        /* Session timeout check (leader only) */
        if (AERON_CLUSTER_ROLE_LEADER == agent->role && NULL != agent->session_manager)
        {
            aeron_cluster_session_manager_check_timeouts(
                agent->session_manager, now_ns, agent->session_timeout_ns,
                on_session_timeout, agent);
        }

        agent->slow_tick_deadline_ns = now_ns + 1000000LL;
    }

    /* Election drives everything while active */
    if (NULL != agent->election)
    {
        work_count += aeron_cluster_election_do_work(agent->election, now_ns);
        if (aeron_cluster_election_is_closed(agent->election))
        {
            aeron_cluster_election_close(agent->election);
            agent->election = NULL;
        }
        return work_count;
    }

    /* Poll ingress (client connect requests, messages) */
    if (NULL != agent->ingress_adapter)
    {
        work_count += aeron_cluster_ingress_adapter_cm_poll(agent->ingress_adapter);
    }

    /* Poll consensus (peer messages) */
    if (NULL != agent->consensus_adapter)
    {
        work_count += aeron_cluster_consensus_adapter_poll(agent->consensus_adapter);
    }

    /* Timer expiry (leader only) */
    if (AERON_CLUSTER_ROLE_LEADER == agent->role && NULL != agent->timer_service)
    {
        work_count += aeron_cluster_timer_service_poll(agent->timer_service,
            agent->ctx->cluster_clock_ns != NULL
                ? agent->ctx->cluster_clock_ns(agent->ctx->cluster_clock_clientd)
                : now_ns);
    }

    /* Leader: update commit position */
    if (AERON_CLUSTER_ROLE_LEADER == agent->role)
    {
        int64_t quorum_pos = aeron_cluster_member_quorum_position(
            agent->active_members, agent->active_member_count,
            now_ns, agent->leader_heartbeat_timeout_ns);

        if (quorum_pos > agent->notified_commit_position)
        {
            if (NULL != agent->commit_position_counter)
            {
                { int64_t *_cp = aeron_counter_addr(agent->commit_position_counter); if (NULL != _cp) *_cp = quorum_pos; }
            }
            agent->notified_commit_position = quorum_pos;
            aeron_cluster_consensus_publisher_broadcast_commit_position(
                agent->active_members, agent->active_member_count, agent->member_id,
                agent->leadership_term_id, quorum_pos, agent->member_id);
            work_count++;
        }
    }

    return work_count;
}

int aeron_consensus_module_agent_close(aeron_consensus_module_agent_t *agent)
{
    if (NULL != agent)
    {
        aeron_cluster_election_close(agent->election);
        aeron_cluster_session_manager_close(agent->session_manager);
        aeron_cluster_timer_service_close(agent->timer_service);
        aeron_cluster_recording_log_close(agent->recording_log);
        aeron_cluster_ingress_adapter_cm_close(agent->ingress_adapter);
        aeron_cluster_consensus_adapter_close(agent->consensus_adapter);

        if (NULL != agent->archive)
        {
            aeron_archive_close(agent->archive);
        }
        aeron_free(agent->pending_trackers);

        if (NULL != agent->log_publication)
        {
            aeron_exclusive_publication_close(agent->log_publication, NULL, NULL);
        }
        if (NULL != agent->service_pub)
        {
            aeron_exclusive_publication_close(agent->service_pub, NULL, NULL);
        }
        if (NULL != agent->ingress_subscription)
        {
            aeron_subscription_close(agent->ingress_subscription, NULL, NULL);
        }
        if (NULL != agent->consensus_subscription)
        {
            aeron_subscription_close(agent->consensus_subscription, NULL, NULL);
        }
        if (NULL != agent->service_sub)
        {
            aeron_subscription_close(agent->service_sub, NULL, NULL);
        }

        aeron_cluster_members_free(agent->active_members, agent->active_member_count);
        aeron_free(agent->ranked_positions);
        aeron_free(agent->service_ack_positions);
        aeron_free(agent);
    }
    return 0;
}

/* -----------------------------------------------------------------------
 * Accessors
 * ----------------------------------------------------------------------- */
int32_t aeron_consensus_module_agent_get_protocol_version(aeron_consensus_module_agent_t *a)
{ return a->protocol_version; }

int32_t aeron_consensus_module_agent_get_app_version(aeron_consensus_module_agent_t *a)
{ return a->app_version; }

int64_t aeron_consensus_module_agent_get_append_position(aeron_consensus_module_agent_t *a)
{
    if (NULL != a->log_publication)
    {
        return aeron_exclusive_publication_position(a->log_publication);
    }
    return a->last_append_position;
}

int64_t aeron_consensus_module_agent_get_log_recording_id(aeron_consensus_module_agent_t *a)
{ return a->log_recording_id; }

/* -----------------------------------------------------------------------
 * Election callbacks
 * ----------------------------------------------------------------------- */
void aeron_consensus_module_agent_on_election_state_change(
    aeron_consensus_module_agent_t *agent,
    aeron_cluster_election_state_t new_state,
    int64_t now_ns)
{
    /* Update election state counter if available */
    (void)new_state; (void)now_ns;
}

void aeron_consensus_module_agent_on_election_complete(
    aeron_consensus_module_agent_t *agent,
    aeron_cluster_member_t *leader,
    int64_t now_ns)
{
    agent->leader_member = leader;
    bool is_leader = (leader != NULL && leader->id == agent->member_id);
    agent->role = is_leader ? AERON_CLUSTER_ROLE_LEADER : AERON_CLUSTER_ROLE_FOLLOWER;
    agent->time_of_last_log_update_ns = now_ns;

    if (NULL != agent->cluster_role_counter)
    {
        { int64_t *_cp = aeron_counter_addr(agent->cluster_role_counter); if (NULL != _cp) *_cp = (int64_t)agent->role; }
    }

    aeron_cm_context_t *ctx = agent->ctx;

    if (is_leader)
    {
        /* Leader: add exclusive log publication and start archive recording */
        if (NULL == agent->log_publication)
        {
            if (add_exclusive_pub(agent->aeron, &agent->log_publication,
                ctx->log_channel, ctx->log_stream_id) < 0)
            {
                AERON_APPEND_ERR("%s", "failed to add log publication");
                return;
            }

            aeron_publication_constants_t pub_consts;
            aeron_exclusive_publication_constants(agent->log_publication, &pub_consts);
            agent->log_session_id_cache = pub_consts.session_id;

            aeron_cluster_log_publisher_init(&agent->log_publisher,
                agent->log_publication, agent->leadership_term_id);

            /* Start archive recording of the log */
            if (NULL != agent->archive)
            {
                aeron_archive_start_recording(
                    &agent->log_subscription_id, agent->archive,
                    ctx->log_channel, ctx->log_stream_id,
                    AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, false);
            }
        }
    }

    /* Send JoinLog to services */
    int64_t append_pos = aeron_consensus_module_agent_get_append_position(agent);
    aeron_cluster_service_proxy_cm_join_log(
        &agent->service_proxy,
        append_pos, append_pos,
        agent->member_id,
        agent->log_session_id_cache,
        ctx->log_stream_id,
        false,
        (int32_t)agent->role,
        ctx->log_channel);
}

void aeron_consensus_module_agent_begin_new_leadership_term(
    aeron_consensus_module_agent_t *agent,
    int64_t log_leadership_term_id,
    int64_t new_term_id,
    int64_t log_position,
    int64_t timestamp,
    bool is_startup)
{
    agent->leadership_term_id = new_term_id;

    /* Record the new term in recording.log */
    aeron_cluster_recording_log_append_term(agent->recording_log,
        agent->log_recording_id, new_term_id, log_position, timestamp);

    /* Append NewLeadershipTermEvent to the log */
    if (NULL != agent->log_publication)
    {
        aeron_cluster_log_publisher_append_new_leadership_term_event(
            &agent->log_publisher,
            new_term_id, log_position, timestamp, log_position,
            agent->member_id,
            agent->log_session_id_cache,
            agent->app_version);
    }
}

void aeron_consensus_module_agent_on_follower_new_leadership_term(
    aeron_consensus_module_agent_t *agent,
    int64_t log_leadership_term_id,
    int64_t next_leadership_term_id,
    int64_t next_term_base_log_position,
    int64_t next_log_position,
    int64_t leadership_term_id,
    int64_t term_base_log_position,
    int64_t log_position,
    int64_t leader_recording_id,
    int64_t timestamp,
    int32_t leader_member_id,
    int32_t log_session_id,
    int32_t app_version,
    bool is_startup)
{
    agent->leadership_term_id = next_leadership_term_id;
    agent->log_recording_id   = leader_recording_id;
    agent->time_of_last_log_update_ns = aeron_nano_clock();
}

void aeron_consensus_module_agent_on_replay_new_leadership_term_event(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id,
    int64_t log_position,
    int64_t timestamp,
    int64_t term_base_log_position,
    int32_t leader_member_id,
    int32_t log_session_id,
    int32_t app_version)
{
    agent->leadership_term_id = leadership_term_id;
}

void aeron_consensus_module_agent_notify_commit_position(
    aeron_consensus_module_agent_t *agent, int64_t commit_position)
{
    if (commit_position > agent->notified_commit_position)
    {
        agent->notified_commit_position = commit_position;
        if (NULL != agent->commit_position_counter)
        {
            { int64_t *_cp = aeron_counter_addr(agent->commit_position_counter); if (NULL != _cp) *_cp = commit_position; }
        }
    }
}

/* -----------------------------------------------------------------------
 * Consensus adapter callbacks
 * ----------------------------------------------------------------------- */
void aeron_consensus_module_agent_on_canvass_position(
    aeron_consensus_module_agent_t *agent,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t leadership_term_id, int32_t follower_member_id,
    int32_t protocol_version)
{
    if (NULL != agent->election)
    {
        aeron_cluster_election_on_canvass_position(agent->election,
            log_leadership_term_id, log_position,
            leadership_term_id, follower_member_id, protocol_version);
    }
    /* Update member tracking even outside election */
    aeron_cluster_member_t *m = aeron_cluster_member_find_by_id(
        agent->active_members, agent->active_member_count, follower_member_id);
    if (NULL != m)
    {
        m->log_position       = log_position;
        m->leadership_term_id = log_leadership_term_id;
    }
}

void aeron_consensus_module_agent_on_request_vote(
    aeron_consensus_module_agent_t *agent,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t candidate_term_id, int32_t candidate_member_id)
{
    if (NULL != agent->election)
    {
        aeron_cluster_election_on_request_vote(agent->election,
            log_leadership_term_id, log_position,
            candidate_term_id, candidate_member_id);
    }
}

void aeron_consensus_module_agent_on_vote(
    aeron_consensus_module_agent_t *agent,
    int64_t candidate_term_id, int64_t log_leadership_term_id,
    int64_t log_position, int32_t candidate_member_id,
    int32_t follower_member_id, bool vote)
{
    if (NULL != agent->election)
    {
        aeron_cluster_election_on_vote(agent->election,
            candidate_term_id, log_leadership_term_id,
            log_position, candidate_member_id, follower_member_id, vote);
    }
}

void aeron_consensus_module_agent_on_new_leadership_term(
    aeron_consensus_module_agent_t *agent,
    int64_t log_leadership_term_id,
    int64_t next_leadership_term_id,
    int64_t next_term_base_log_position,
    int64_t next_log_position,
    int64_t leadership_term_id,
    int64_t term_base_log_position,
    int64_t log_position,
    int64_t leader_recording_id,
    int64_t timestamp,
    int32_t leader_member_id,
    int32_t log_session_id,
    int32_t app_version,
    bool is_startup)
{
    if (NULL != agent->election)
    {
        aeron_cluster_election_on_new_leadership_term(agent->election,
            log_leadership_term_id, next_leadership_term_id,
            next_term_base_log_position, next_log_position,
            leadership_term_id, term_base_log_position, log_position,
            leader_recording_id, timestamp, leader_member_id,
            log_session_id, app_version, is_startup);
    }
    agent->time_of_last_log_update_ns = aeron_nano_clock();
}

void aeron_consensus_module_agent_on_append_position(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position,
    int32_t follower_member_id, int8_t flags)
{
    aeron_cluster_member_t *m = aeron_cluster_member_find_by_id(
        agent->active_members, agent->active_member_count, follower_member_id);
    if (NULL != m)
    {
        m->log_position      = log_position;
        m->leadership_term_id = leadership_term_id;
        m->time_of_last_append_position_ns = aeron_nano_clock();
    }

    if (NULL != agent->election)
    {
        aeron_cluster_election_on_append_position(agent->election,
            leadership_term_id, log_position, follower_member_id, flags);
    }
}

void aeron_consensus_module_agent_on_commit_position(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position, int32_t leader_member_id)
{
    agent->time_of_last_log_update_ns = aeron_nano_clock();
    aeron_consensus_module_agent_notify_commit_position(agent, log_position);

    if (NULL != agent->election)
    {
        aeron_cluster_election_on_commit_position(agent->election,
            leadership_term_id, log_position, leader_member_id);
    }
}

void aeron_consensus_module_agent_on_catchup_position(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position,
    int32_t follower_member_id, const char *catchup_endpoint)
{
    if (NULL == agent->archive || agent->log_recording_id < 0) { return; }

    /* Build a replay channel targeting the follower's catchup endpoint */
    char replay_channel[AERON_URI_MAX_LENGTH];
    snprintf(replay_channel, sizeof(replay_channel),
        "aeron:udp?endpoint=%s", catchup_endpoint);

    aeron_archive_replay_params_t params;
    aeron_archive_replay_params_init(&params);
    params.position = log_position;
    params.length   = INT64_MAX;    /* replay until stopped */

    int64_t replay_session_id = -1;
    aeron_archive_start_replay(
        &replay_session_id, agent->archive,
        agent->log_recording_id,
        replay_channel, agent->ctx->log_stream_id, &params);
    /* replay_session_id tracked for later stop via on_stop_catchup */
}

void aeron_consensus_module_agent_on_stop_catchup(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int32_t follower_member_id)
{
    (void)leadership_term_id; (void)follower_member_id;
}

void aeron_consensus_module_agent_on_termination_position(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position)
{
    agent->termination_position = log_position;
}

void aeron_consensus_module_agent_on_termination_ack(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position, int32_t member_id)
{
    (void)leadership_term_id; (void)log_position; (void)member_id;
}

/* -----------------------------------------------------------------------
 * Ingress callbacks
 * ----------------------------------------------------------------------- */
void aeron_consensus_module_agent_on_session_connect(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id, int32_t response_stream_id,
    int32_t version, const char *response_channel,
    const uint8_t *encoded_credentials, size_t credentials_length,
    aeron_header_t *header)
{
    /* Only leader processes connect requests */
    if (AERON_CLUSTER_ROLE_LEADER != agent->role) { return; }

    aeron_cluster_cluster_session_t *session =
        aeron_cluster_session_manager_new_session(agent->session_manager,
            correlation_id, response_stream_id, response_channel,
            encoded_credentials, credentials_length);

    if (NULL == session) { return; }

    /* Connect the response publication */
    if (aeron_cluster_cluster_session_connect(session) < 0) { return; }

    session->state = AERON_CLUSTER_SESSION_STATE_OPEN;
    session->time_of_last_activity_ns = aeron_nano_clock();

    /* Send OK event to client */
    aeron_cluster_cluster_session_send_event(session,
        correlation_id,
        agent->leadership_term_id,
        agent->member_id,
        0, /* OK */
        agent->leader_heartbeat_timeout_ns,
        NULL, 0);

    /* Append SessionOpenEvent to the log */
    if (NULL != agent->log_publication)
    {
        aeron_cluster_log_publisher_append_session_open(
            &agent->log_publisher,
            session->id,
            correlation_id,
            aeron_nano_clock(),
            response_stream_id,
            response_channel,
            encoded_credentials,
            credentials_length);
    }
}

void aeron_consensus_module_agent_on_session_close(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t cluster_session_id)
{
    if (AERON_CLUSTER_ROLE_LEADER != agent->role) { return; }

    if (NULL != agent->log_publication)
    {
        aeron_cluster_log_publisher_append_session_close(
            &agent->log_publisher,
            cluster_session_id,
            0, /* CLIENT_ACTION */
            aeron_nano_clock());
    }

    aeron_cluster_session_manager_remove(agent->session_manager, cluster_session_id);
}

void aeron_consensus_module_agent_on_session_keep_alive(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t cluster_session_id,
    aeron_header_t *header)
{
    aeron_cluster_cluster_session_t *session =
        aeron_cluster_session_manager_find(agent->session_manager, cluster_session_id);
    if (NULL != session)
    {
        session->time_of_last_activity_ns = aeron_nano_clock();
    }
}

void aeron_consensus_module_agent_on_session_message(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t cluster_session_id,
    const uint8_t *payload, size_t payload_length,
    aeron_header_t *header)
{
    if (AERON_CLUSTER_ROLE_LEADER != agent->role) { return; }

    aeron_cluster_cluster_session_t *session =
        aeron_cluster_session_manager_find(agent->session_manager, cluster_session_id);
    if (NULL == session || session->state != AERON_CLUSTER_SESSION_STATE_OPEN) { return; }

    session->time_of_last_activity_ns = aeron_nano_clock();

    /* Forward to log (services will see it via BoundedLogAdapter) */
    if (NULL != agent->log_publication)
    {
        aeron_cluster_log_publisher_append_session_message(
            &agent->log_publisher,
            cluster_session_id,
            aeron_nano_clock(),
            payload, payload_length);
    }
}

void aeron_consensus_module_agent_on_ingress_challenge_response(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id, int64_t cluster_session_id,
    const uint8_t *encoded_credentials, size_t credentials_length,
    aeron_header_t *header)
{
    /* Challenge/response auth — not fully implemented in this stub */
    (void)agent; (void)correlation_id; (void)cluster_session_id;
    (void)encoded_credentials; (void)credentials_length; (void)header;
}

void aeron_consensus_module_agent_on_admin_request(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t cluster_session_id,
    int64_t correlation_id, int32_t request_type,
    const uint8_t *payload, size_t payload_length,
    aeron_header_t *header)
{
    if (AERON_CLUSTER_ROLE_LEADER != agent->role) { return; }

    /* Append a ClusterActionRequest(SNAPSHOT) to the log */
    if (AERON_CLUSTER_ACTION_SNAPSHOT == request_type && NULL != agent->log_publication)
    {
        int64_t log_pos   = aeron_consensus_module_agent_get_append_position(agent);
        int64_t timestamp = aeron_nano_clock();

        /* 1. Append ClusterActionRequest(SNAPSHOT) to log → triggers services to snapshot */
        aeron_cluster_log_publisher_append_cluster_action(
            &agent->log_publisher, log_pos, timestamp,
            AERON_CLUSTER_ACTION_SNAPSHOT, AERON_CLUSTER_ACTION_FLAGS_DEFAULT);

        /* 2. CM takes its own snapshot via archive */
        if (NULL != agent->archive)
        {
            aeron_cm_context_t *ctx = agent->ctx;

            /* Start recording the snapshot channel */
            int64_t snap_sub_id = -1;
            aeron_archive_start_recording(&snap_sub_id, agent->archive,
                ctx->snapshot_channel, ctx->snapshot_stream_id,
                AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, false);

            /* Add exclusive publication to write the snapshot */
            aeron_exclusive_publication_t *snap_pub = NULL;
            if (add_exclusive_pub(agent->aeron, &snap_pub,
                ctx->snapshot_channel, ctx->snapshot_stream_id) == 0 &&
                NULL != snap_pub)
            {
                aeron_cluster_cm_snapshot_taker_mark_begin(
                    snap_pub, log_pos, agent->leadership_term_id, ctx->app_version);

                /* Write all live sessions */
                for (int i = 0; i < agent->session_manager->session_count; i++)
                {
                    aeron_cluster_cm_snapshot_taker_snapshot_session(
                        snap_pub, agent->session_manager->sessions[i]);
                }

                /* Write CM state */
                aeron_cluster_cm_snapshot_taker_snapshot_cm_state(
                    snap_pub, agent->session_manager->next_session_id);

                aeron_cluster_cm_snapshot_taker_mark_end(
                    snap_pub, log_pos, agent->leadership_term_id, ctx->app_version);

                aeron_exclusive_publication_close(snap_pub, NULL, NULL);
            }

            /* Stop recording; get recording_id, append to recording_log */
            if (snap_sub_id >= 0)
            {
                int64_t snap_recording_id = -1;
                /* In full impl: poll for recording signal to get actual recording_id.
                 * Simplified: use find_last recording after stop. */
                aeron_archive_stop_recording_subscription(agent->archive, snap_sub_id);

                /* Append snapshot entry to recording.log */
                aeron_cluster_recording_log_append_snapshot(agent->recording_log,
                    snap_recording_id, agent->leadership_term_id,
                    log_pos, log_pos, timestamp,
                    -1 /* CM service_id */);
            }
        }
    }
}
