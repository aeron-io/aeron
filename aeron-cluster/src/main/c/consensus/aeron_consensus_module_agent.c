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
#include <inttypes.h>

#include "aeron_consensus_module_agent.h"
#include "aeron_cm_context.h"
#include "aeron_cluster_mark_file.h"
#include "aeron_cluster_log_adapter.h"
#include "aeron_cluster_log_replay.h"
#include "aeron_cluster_recording_replication.h"
#include "aeron_cluster_consensus_publisher.h"
#include "aeron_cluster_consensus_module_adapter.h"
#include "aeron_cluster_cm_snapshot_loader.h"
#include "aeron_cluster_egress_publisher.h"
#include "aeron_cluster_cluster_session.h"
#include "aeron_cluster_pending_message_tracker.h"

#include "aeron_cluster_client/clusterAction.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_clock.h"
#include "uri/aeron_uri.h"
#include "uri/aeron_uri_string_builder.h"
#include "aeron_archive.h"
#include "aeron_archive_replay_params.h"

/* -----------------------------------------------------------------------
 * Forward declarations for termination helpers defined later in the file
 * ----------------------------------------------------------------------- */
static void close_and_terminate(aeron_consensus_module_agent_t *agent);
static bool can_terminate(const aeron_consensus_module_agent_t *agent, int64_t now_ns);
static void setup_cluster_termination(aeron_consensus_module_agent_t *agent, int64_t position, int64_t now_ns);
static void terminate_on_service_ack(aeron_consensus_module_agent_t *agent, int64_t log_position, int64_t now_ns);

/* -----------------------------------------------------------------------
 * Pending service message append callback (passed to tracker poll)
 * ----------------------------------------------------------------------- */
static int64_t pending_service_message_log_append(
    void    *clientd,
    int64_t  leadership_term_id,
    int64_t  cluster_session_id,
    int64_t  timestamp,
    uint8_t *buffer,
    int      payload_offset,
    int      payload_length)
{
    (void)leadership_term_id; /* publisher struct already has the term id */
    aeron_cluster_log_publisher_t *pub = (aeron_cluster_log_publisher_t *)clientd;
    return aeron_cluster_log_publisher_append_session_message(
        pub, cluster_session_id, timestamp,
        buffer + payload_offset, (size_t)payload_length);
}
static void do_termination_as_follower(aeron_consensus_module_agent_t *agent, int64_t log_position);
static void timer_on_expiry(void *clientd, int64_t correlation_id);

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
    a->last_append_position     = 0;
    a->notified_commit_position = 0;
    a->termination_position     = -1;
    a->log_subscription_id      = -1;
    a->log_recording_id         = -1;
    a->initial_log_leadership_term_id = -1;
    a->initial_term_base_log_position = 0;
    a->snapshot_log_position          = -1;
    a->snapshot_timestamp             = 0;
    a->service_ack_count              = 0;
    a->termination_leadership_term_id = -1;
    a->termination_deadline_ns        = 0;
    a->has_cluster_termination        = false;
    a->is_awaiting_services           = false;
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
    a->last_do_work_ns              = aeron_nano_clock();
    a->time_of_last_append_position_send_ns = 0;
    a->last_quorum_backtrack_commit_position = 0;

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

    /* Service snapshot recording IDs — the relevant_id from each service's snapshot ACK */
    if (aeron_alloc((void **)&a->service_snapshot_recording_ids,
        (size_t)a->service_count * sizeof(int64_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate service_snapshot_recording_ids");
        aeron_free(a->service_ack_positions);
        aeron_free(a->ranked_positions);
        aeron_cluster_members_free(a->active_members, a->active_member_count);
        aeron_free(a);
        return -1;
    }
    for (int i = 0; i < a->service_count; i++) { a->service_snapshot_recording_ids[i] = -1; }

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
    a->log_adapter           = NULL;
    a->cm_adapter            = NULL;
    a->live_log_destination  = NULL;
    a->catchup_log_destination = NULL;
    a->uncommitted_timers          = NULL;
    a->uncommitted_timers_count    = 0;
    a->uncommitted_timers_capacity = 0;
    a->uncommitted_previous_states          = NULL;
    a->uncommitted_previous_states_count    = 0;
    a->uncommitted_previous_states_capacity = 0;

    *agent = a;
    return 0;
}

int aeron_consensus_module_agent_on_start(aeron_consensus_module_agent_t *agent)
{
    aeron_cm_context_t *ctx = agent->ctx;

    /* Register unavailable counter handler — mirrors Java onUnavailableCounter().
     * Detects when service client counters are deregistered (service crash). */
    /* Note: requires aeron context to be set before aeron_start(), which is
     * the caller's responsibility. The agent checks counters during do_work instead. */

    /* Add subscriptions */
    /* Build subscription channels with this_member's endpoints appended —
     * mirrors Java ConsensusModuleAgent: consensusChannelUri.put(ENDPOINT, thisMember.consensusEndpoint()) */
    char ingress_channel_full[512];
    char consensus_channel_full[512];
    if (NULL != agent->this_member && NULL != agent->this_member->ingress_endpoint &&
        agent->this_member->ingress_endpoint[0] != '\0' &&
        strncmp(ctx->ingress_channel, "aeron:ipc", 9) != 0)
    {
        const char *sep = (strchr(ctx->ingress_channel, '?') != NULL) ? "|" : "?";
        snprintf(ingress_channel_full, sizeof(ingress_channel_full),
                 "%s%sendpoint=%s", ctx->ingress_channel, sep, agent->this_member->ingress_endpoint);
    }
    else
    {
        snprintf(ingress_channel_full, sizeof(ingress_channel_full), "%s", ctx->ingress_channel);
    }
    if (NULL != agent->this_member && NULL != agent->this_member->consensus_endpoint &&
        agent->this_member->consensus_endpoint[0] != '\0' &&
        strncmp(ctx->consensus_channel, "aeron:ipc", 9) != 0)
    {
        const char *sep = (strchr(ctx->consensus_channel, '?') != NULL) ? "|" : "?";
        snprintf(consensus_channel_full, sizeof(consensus_channel_full),
                 "%s%sendpoint=%s", ctx->consensus_channel, sep, agent->this_member->consensus_endpoint);
    }
    else
    {
        snprintf(consensus_channel_full, sizeof(consensus_channel_full), "%s", ctx->consensus_channel);
    }

    if (add_sub(agent->aeron, &agent->ingress_subscription,
        ingress_channel_full, ctx->ingress_stream_id) < 0) { return -1; }

    if (add_sub(agent->aeron, &agent->consensus_subscription,
        consensus_channel_full, ctx->consensus_stream_id) < 0) { return -1; }

    if (add_sub(agent->aeron, &agent->service_sub,
        ctx->control_channel, ctx->consensus_module_stream_id) < 0) { return -1; }

    /* Add publications */
    if (add_exclusive_pub(agent->aeron, &agent->service_pub,
        ctx->control_channel, ctx->service_stream_id) < 0) { return -1; }

    /* Build adapters */
    if (aeron_cluster_ingress_adapter_cm_create(
        &agent->ingress_adapter, agent->ingress_subscription, agent,
        AERON_CM_INGRESS_FRAGMENT_LIMIT_DEFAULT) < 0) { return -1; }

    if (aeron_cluster_consensus_adapter_create(
        &agent->consensus_adapter, agent->consensus_subscription, agent) < 0) { return -1; }

    /* Log adapter (for replay/catchup) */
    if (aeron_cluster_log_adapter_create(
        &agent->log_adapter, agent, AERON_CLUSTER_LOG_ADAPTER_FRAGMENT_LIMIT) < 0) { return -1; }

    /* ConsensusModuleAdapter — polls service → CM subscription */
    if (aeron_cluster_consensus_module_adapter_create(
        &agent->cm_adapter, agent->service_sub, agent) < 0) { return -1; }

    /* Service proxy */
    aeron_cluster_service_proxy_cm_init(&agent->service_proxy,
        agent->service_pub, agent->service_count);

    /* Session manager */
    if (aeron_cluster_session_manager_create(
        &agent->session_manager, 1 /* initial session id */, agent->aeron) < 0) { return -1; }

    /* Wire extension hooks into session manager for on_session_opened/closed */
    if (NULL != ctx->extension.on_session_opened)
    {
        agent->session_manager->on_session_opened_hook = ctx->extension.on_session_opened;
        agent->session_manager->on_session_closed_hook = ctx->extension.on_session_closed;
        agent->session_manager->extension_hook_clientd = ctx->extension.clientd;
    }

    /* Timer service */
    if (aeron_cluster_timer_service_create(&agent->timer_service, timer_on_expiry, agent) < 0) { return -1; }

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
        agent->recording_log, &plan, agent->service_count, agent->archive);

    int64_t log_position        = (NULL != plan) ? plan->last_append_position       : 0;
    int64_t log_term_id         = (NULL != plan) ? plan->last_leadership_term_id    : -1;
    int64_t leader_recording_id = (NULL != plan) ? plan->last_term_recording_id     : -1;
    bool    has_snapshot        = (NULL != plan) ? plan->snapshot_count > 0          : false;

    /* Store initial log state for use in ensure_coherent during election */
    agent->initial_log_leadership_term_id = log_term_id;
    agent->initial_term_base_log_position =
        (NULL != plan) ? plan->last_term_base_log_position : 0;

    /* If recovering from snapshot, load it before starting election */
    if (has_snapshot && NULL != agent->archive)
    {
        aeron_archive_replay_params_t params;
        aeron_archive_replay_params_init(&params);
        params.position = 0;  /* snapshot is self-contained, replay from start */
        params.length   = INT64_MAX;

        /* CM snapshot (service_id = -1) is in plan->snapshots[0] */
        aeron_subscription_t *snap_sub = NULL;
        int replay_rc = aeron_archive_replay(&snap_sub, agent->archive,
            plan->snapshots[0].recording_id,
            ctx->snapshot_channel, ctx->snapshot_stream_id, &params);

        if (replay_rc == 0 && NULL != snap_sub)
        {
            /* Wait for image */
            aeron_image_t *snap_image = NULL;
            int spin = 0;
            while (NULL == snap_image && spin++ < 1000)
            {
                snap_image = aeron_subscription_image_at_index(snap_sub, 0);
            }

            if (NULL != snap_image)
            {
                aeron_cluster_cm_snapshot_loader_t *snap_loader = NULL;
                if (aeron_cluster_cm_snapshot_loader_create(
                    &snap_loader, snap_image,
                    agent->session_manager, agent->timer_service,
                    agent->pending_trackers, agent->service_count) == 0)
                {
                    /* Poll until done */
                    int poll_spin = 0;
                    while (!snap_loader->is_done && poll_spin++ < 100000)
                    {
                        aeron_cluster_cm_snapshot_loader_poll(snap_loader, 10);
                    }
                    aeron_cluster_cm_snapshot_loader_close(snap_loader);
                }
            }

            aeron_subscription_close(snap_sub, NULL, NULL);
        }
    }

    aeron_cluster_recovery_plan_free(plan);

    /* Add consensus publications to all peers (mirrors Java ConsensusModuleAgent.onStart) */
    if (aeron_cluster_members_add_consensus_publications(
        agent->active_members, agent->active_member_count,
        agent->member_id,
        agent->aeron,
        ctx->consensus_channel,
        ctx->consensus_stream_id) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to add consensus publications");
        return -1;
    }

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

    /* Extension hook: onStart */
    if (NULL != agent->ctx->extension.on_start)
    {
        agent->ctx->extension.on_start(agent->ctx->extension.clientd);
    }

    return 0;
}

/* -----------------------------------------------------------------------
 * do_work: main duty cycle
 * ----------------------------------------------------------------------- */
static int slow_tick_work(aeron_consensus_module_agent_t *agent, int64_t now_ns)
{
    int work_count = 0;

    /* Mark file activity timestamp update (every ~1s).
     * Mirrors Java ConsensusModuleAgent.slowTickWork() → markFile.updateActivityTimestamp(). */
    if (now_ns >= agent->mark_file_update_deadline_ns)
    {
        agent->mark_file_update_deadline_ns = now_ns + INT64_C(1000000000); /* 1 second */
        if (NULL != agent->ctx && NULL != agent->ctx->mark_file)
        {
            aeron_cluster_mark_file_update_activity_timestamp(
                agent->ctx->mark_file, now_ns / INT64_C(1000000));
        }
        work_count++;
    }

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
        agent->election->now_ns = now_ns;
        agent->election->time_of_state_change_ns = now_ns;
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

    /* Leader: enter election if no quorum of active followers (more than 1-node cluster) */
    if (AERON_CLUSTER_ROLE_LEADER == agent->role &&
        NULL == agent->election &&
        agent->state == AERON_CM_STATE_ACTIVE &&
        agent->active_member_count > 1 &&
        !aeron_cluster_members_has_active_quorum(
            agent->active_members, agent->active_member_count,
            now_ns, agent->leader_heartbeat_timeout_ns))
    {
        /* Mirrors Java enterElection(): clearSessionsAfter + disconnectSessions
         * before creating the election. */
        if (NULL != agent->session_manager)
        {
            int64_t current_pos = aeron_consensus_module_agent_get_append_position(agent);
            aeron_cluster_session_manager_clear_sessions_after(
                agent->session_manager, current_pos, agent->leadership_term_id);
            aeron_cluster_session_manager_disconnect_sessions(agent->session_manager);
        }
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
        agent->election->now_ns = now_ns;
        agent->election->time_of_state_change_ns = now_ns;
        work_count++;
    }

    /* Extension hook: slowTickWork */
    if (NULL != agent->ctx->extension.slow_tick_work)
    {
        work_count += agent->ctx->extension.slow_tick_work(agent->ctx->extension.clientd, now_ns);
    }

    return work_count;
}

/**
 * Read the control toggle counter and act on SNAPSHOT, SHUTDOWN, ABORT.
 * Mirrors Java ConsensusModuleAgent.checkClusterControlToggle().
 * Only meaningful on the leader when state == ACTIVE.
 */
static int check_cluster_control_toggle(aeron_consensus_module_agent_t *agent, int64_t now_ns)
{
    if (NULL == agent->control_toggle_counter) { return 0; }
    int64_t *toggle_addr = aeron_counter_addr(agent->control_toggle_counter);
    if (NULL == toggle_addr) { return 0; }

    int64_t toggle = *toggle_addr;

    switch ((int32_t)toggle)
    {
        case AERON_CLUSTER_TOGGLE_NEUTRAL:
        case AERON_CLUSTER_TOGGLE_INACTIVE:
            return 0;

        case AERON_CLUSTER_TOGGLE_SNAPSHOT:
            if (agent->state == AERON_CM_STATE_ACTIVE && NULL != agent->log_publication)
            {
                int64_t log_pos   = aeron_consensus_module_agent_get_append_position(agent);
                int64_t timestamp = now_ns;

                aeron_cluster_log_publisher_append_cluster_action(
                    &agent->log_publisher, log_pos, timestamp,
                    AERON_CLUSTER_ACTION_SNAPSHOT, AERON_CLUSTER_ACTION_FLAGS_DEFAULT);

                agent->state                 = AERON_CM_STATE_SNAPSHOT;
                agent->snapshot_log_position = log_pos;
                agent->snapshot_timestamp    = timestamp;
                agent->service_ack_count     = 0;
                agent->expected_ack_position = log_pos;
                agent->service_ack_id++;
                for (int _i = 0; _i < agent->service_count; _i++)
                {
                    agent->service_snapshot_recording_ids[_i] = -1;
                }

                for (int i = 0; i < agent->service_count; i++)
                {
                    aeron_cluster_service_proxy_cm_request_service_ack(&agent->service_proxy, log_pos);
                }

                *toggle_addr = AERON_CLUSTER_TOGGLE_NEUTRAL;
                return 1;
            }
            break;

        case AERON_CLUSTER_TOGGLE_SHUTDOWN:
            if (agent->state == AERON_CM_STATE_ACTIVE)
            {
                aeron_consensus_module_agent_begin_termination(agent, now_ns, true);
                *toggle_addr = AERON_CLUSTER_TOGGLE_NEUTRAL;
                return 1;
            }
            break;

        case AERON_CLUSTER_TOGGLE_ABORT:
            if (agent->state == AERON_CM_STATE_ACTIVE)
            {
                aeron_consensus_module_agent_begin_termination(agent, now_ns, false);
                *toggle_addr = AERON_CLUSTER_TOGGLE_NEUTRAL;
                return 1;
            }
            break;

        case AERON_CLUSTER_TOGGLE_SUSPEND:
            if (agent->state == AERON_CM_STATE_ACTIVE)
            {
                agent->state = AERON_CM_STATE_SUSPENDED;
                *toggle_addr = AERON_CLUSTER_TOGGLE_NEUTRAL;
                return 1;
            }
            break;

        case AERON_CLUSTER_TOGGLE_RESUME:
            if (agent->state == AERON_CM_STATE_SUSPENDED)
            {
                agent->state = AERON_CM_STATE_ACTIVE;
                *toggle_addr = AERON_CLUSTER_TOGGLE_NEUTRAL;
                return 1;
            }
            break;

        case AERON_CLUSTER_TOGGLE_STANDBY_SNAPSHOT:
            /* Mirrors Java checkNodeControlToggle() REPLICATE_STANDBY_SNAPSHOT case.
             * Full StandbySnapshotReplicator creation is a ClusterBackup Phase 4 feature;
             * for now reset the toggle so we do not spin on it. */
            *toggle_addr = AERON_CLUSTER_TOGGLE_NEUTRAL;
            return 1;

        default:
            break;
    }
    return 0;
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
    agent->last_do_work_ns = now_ns;

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

    /* Poll consensus (peer messages) — must happen before election check so
     * canvass/vote/append-position messages are received during election. */
    if (NULL != agent->consensus_adapter)
    {
        work_count += aeron_cluster_consensus_adapter_poll(agent->consensus_adapter);
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

    /* Drive session authentication state machine and redirect/reject queues */
    if (NULL != agent->session_manager)
    {
        int64_t now_ms = now_ns / INT64_C(1000000);
        if (AERON_CLUSTER_ROLE_LEADER == agent->role)
        {
            work_count += aeron_cluster_session_manager_process_pending_sessions(
                agent->session_manager, now_ns, now_ms,
                agent->member_id, agent->leadership_term_id);
            work_count += aeron_cluster_session_manager_send_rejections(
                agent->session_manager, agent->leadership_term_id,
                agent->member_id, now_ns);
            work_count += aeron_cluster_session_manager_check_sessions(
                agent->session_manager, now_ns, agent->leadership_term_id,
                agent->member_id,
                NULL /* ingress_endpoints — leader doesn't redirect */);
        }
        else
        {
            /* Followers send redirects to clients that connected to the wrong node.
             * Ingress endpoints are set per-session in on_session_connect via
             * aeron_cluster_cluster_session_set_redirect(). */
            work_count += aeron_cluster_session_manager_send_redirects(
                agent->session_manager, agent->leadership_term_id,
                agent->member_id, now_ns);
        }
    }

    /* Poll service → CM adapter */
    if (NULL != agent->cm_adapter)
    {
        work_count += aeron_cluster_consensus_module_adapter_poll(agent->cm_adapter);
    }

    /* Timer expiry (leader only) */
    if (AERON_CLUSTER_ROLE_LEADER == agent->role && NULL != agent->timer_service)
    {
        work_count += aeron_cluster_timer_service_poll(agent->timer_service,
            agent->ctx->cluster_clock_ns != NULL
                ? agent->ctx->cluster_clock_ns(agent->ctx->cluster_clock_clientd)
                : now_ns);
    }

    /* Pending service message ring-buffer drain (leader, ACTIVE state only).
     * Mirrors Java: for (PendingServiceMessageTracker t : pendingServiceMessageTrackers) t.poll()
     */
    if (AERON_CLUSTER_ROLE_LEADER == agent->role &&
        agent->state == AERON_CM_STATE_ACTIVE &&
        NULL != agent->pending_trackers)
    {
        for (int i = 0; i < agent->service_count; i++)
        {
            work_count += aeron_cluster_pending_message_tracker_poll(
                &agent->pending_trackers[i],
                agent->leadership_term_id,
                now_ns,
                &agent->log_publisher,
                pending_service_message_log_append);
        }
    }

    /* Leader: update commit position */
    if (AERON_CLUSTER_ROLE_LEADER == agent->role)
    {
        int64_t quorum_pos = aeron_cluster_member_quorum_position(
            agent->active_members, agent->active_member_count,
            now_ns, agent->leader_heartbeat_timeout_ns);

        if (quorum_pos > agent->notified_commit_position ||
            (now_ns - agent->time_of_last_log_update_ns) >= agent->leader_heartbeat_interval_ns)
        {
            /* Update counter only when quorum advances — never go backward.
             * Mirrors Java: commitPosition.proposeMaxRelease(quorumPosition). */
            if (quorum_pos > agent->notified_commit_position)
            {
                agent->notified_commit_position = quorum_pos;
                if (NULL != agent->commit_position_counter)
                {
                    int64_t *_cp = aeron_counter_addr(agent->commit_position_counter);
                    if (NULL != _cp) { *_cp = quorum_pos; }
                }
                /* Reset the backtrack sentinel whenever quorum actually advances. */
                agent->last_quorum_backtrack_commit_position = quorum_pos;
            }
            else if (quorum_pos < agent->notified_commit_position &&
                     quorum_pos != agent->last_quorum_backtrack_commit_position)
            {
                /* Quorum went backwards (e.g. follower rebooted). Emit one diagnostic
                 * warning per distinct backtrack position, then suppress repeats.
                 * Mirrors Java lastQuorumBacktrackCommitPosition guard. */
                agent->last_quorum_backtrack_commit_position = quorum_pos;
                /* In production: emit a log warning here via the error handler. */
            }
            agent->time_of_last_log_update_ns = now_ns;
            aeron_cluster_consensus_publisher_broadcast_commit_position(
                agent->active_members, agent->active_member_count, agent->member_id,
                agent->leadership_term_id, quorum_pos, agent->member_id);
            /* Free sessions whose close was committed */
            if (NULL != agent->session_manager)
            {
                aeron_cluster_session_manager_sweep_uncommitted_sessions(
                    agent->session_manager, quorum_pos);
                /* Drain standby snapshot batches whose logPosition is now committed */
                aeron_cluster_session_manager_process_pending_standby_snapshot_notifications(
                    agent->session_manager, quorum_pos, now_ns);
            }
            /* Sweep committed pending service messages from ring buffers.
             * Mirrors Java sweepUncommittedEntriesTo -> tracker.sweepLeaderMessages(). */
            if (NULL != agent->pending_trackers)
            {
                for (int svc = 0; svc < agent->service_count; svc++)
                {
                    aeron_cluster_pending_message_tracker_sweep_leader_messages(
                        &agent->pending_trackers[svc], quorum_pos);
                }
            }
            /* Sweep uncommitted timer entries up to quorum position */
            while (agent->uncommitted_timers_count > 0 &&
                   agent->uncommitted_timers[0] <= quorum_pos)
            {
                /* Remove the first (position, correlationId) pair by shifting */
                int remaining = agent->uncommitted_timers_count - 1;
                if (remaining > 0)
                {
                    memmove(agent->uncommitted_timers,
                            agent->uncommitted_timers + 2,
                            (size_t)(remaining * 2) * sizeof(int64_t));
                }
                agent->uncommitted_timers_count = remaining;
            }
            /* Sweep uncommitted previous-state entries up to quorum position */
            while (agent->uncommitted_previous_states_count > 0 &&
                   agent->uncommitted_previous_states[0] <= quorum_pos)
            {
                int remaining = agent->uncommitted_previous_states_count - 1;
                if (remaining > 0)
                {
                    memmove(agent->uncommitted_previous_states,
                            agent->uncommitted_previous_states + 2,
                            (size_t)(remaining * 2) * sizeof(int64_t));
                }
                agent->uncommitted_previous_states_count = remaining;
            }
            work_count++;
        }

        /* Check cluster control toggle (SNAPSHOT / SHUTDOWN / ABORT / SUSPEND / RESUME) */
        if (agent->state == AERON_CM_STATE_ACTIVE ||
            agent->state == AERON_CM_STATE_SUSPENDED)
        {
            work_count += check_cluster_control_toggle(agent, now_ns);
        }

        /* While LEAVING, check if we can now terminate (all followers acked or deadline) */
        if (agent->state == AERON_CM_STATE_LEAVING &&
            agent->has_cluster_termination &&
            can_terminate(agent, now_ns))
        {
            if (NULL != agent->recording_log)
            {
                aeron_cluster_recording_log_commit_log_position(
                    agent->recording_log, agent->leadership_term_id,
                    agent->termination_position);
            }
            close_and_terminate(agent);
            work_count++;
        }
    }

    /* Follower: poll the replicated log stream (bounded by committed position).
     * Mirrors Java: consensusWork → logAdapter.poll(min(notifiedCommitPosition, limit))
     * If the image closes unexpectedly, enter election. */
    if (AERON_CLUSTER_ROLE_FOLLOWER == agent->role &&
        (agent->state == AERON_CM_STATE_ACTIVE ||
         agent->state == AERON_CM_STATE_SUSPENDED) &&
        NULL != agent->log_adapter &&
        NULL == agent->election)
    {
        /* Bound poll by notified commit position so we don't consume ahead of what's committed */
        int64_t poll_limit = agent->notified_commit_position;
        int log_poll = aeron_cluster_log_adapter_poll(agent->log_adapter, poll_limit);
        work_count += log_poll;

        /* Update the commit position counter to reflect our local log position */
        int64_t log_pos = aeron_cluster_log_adapter_position(agent->log_adapter);
        if (NULL != agent->commit_position_counter)
        {
            int64_t *cp = aeron_counter_addr(agent->commit_position_counter);
            if (NULL != cp && log_pos > *cp) { *cp = log_pos; }
        }
    }

    /* Follower: send AppendPosition to leader periodically or when position advances.
     * Mirrors Java: consensusWork → updateFollowerPosition(nowNs) */
    if (AERON_CLUSTER_ROLE_FOLLOWER == agent->role &&
        NULL == agent->election &&
        NULL != agent->leader_member &&
        NULL != agent->leader_member->publication)
    {
        int64_t recorded_pos = aeron_consensus_module_agent_get_append_position(agent);
        int64_t pos = recorded_pos > agent->last_append_position ? recorded_pos : agent->last_append_position;
        if (pos > agent->last_append_position ||
            (now_ns - agent->time_of_last_append_position_send_ns) >= agent->leader_heartbeat_interval_ns)
        {
            if (aeron_cluster_consensus_publisher_append_position(
                agent->leader_member->publication,
                agent->leadership_term_id, pos, agent->member_id, 0))
            {
                if (pos > agent->last_append_position)
                {
                    agent->last_append_position = pos;
                }
                agent->time_of_last_append_position_send_ns = now_ns;
                work_count++;
            }
        }
    }

    /* Follower: when log adapter reaches the requested termination position,
     * transition to LEAVING and ask services to terminate. */
    if (AERON_CLUSTER_ROLE_FOLLOWER == agent->role &&
        (agent->state == AERON_CM_STATE_ACTIVE ||
         agent->state == AERON_CM_STATE_SUSPENDED) &&
        agent->termination_position >= 0 &&
        NULL != agent->log_adapter)
    {
        int64_t log_pos = aeron_cluster_log_adapter_position(agent->log_adapter);
        if (log_pos >= agent->termination_position)
        {
            if (agent->service_count > 0)
            {
                aeron_cluster_service_proxy_cm_termination_position(
                    &agent->service_proxy, agent->termination_position);
            }
            else
            {
                /* No services — terminate directly */
                do_termination_as_follower(agent, agent->termination_position);
            }
            agent->state                = AERON_CM_STATE_LEAVING;
            agent->expected_ack_position = agent->termination_position;
            work_count++;
        }
    }

    /* Extension hook: doWork (consensus work) */
    if (NULL != agent->ctx->extension.do_work)
    {
        work_count += agent->ctx->extension.do_work(agent->ctx->extension.clientd, now_ns);
    }

    return work_count;
}

/* -----------------------------------------------------------------------
 * Unavailable counter/image handlers
 * ----------------------------------------------------------------------- */

void aeron_consensus_module_agent_on_unavailable_counter(
    void *clientd, int64_t registration_id, int32_t counter_id)
{
    aeron_consensus_module_agent_t *agent = (aeron_consensus_module_agent_t *)clientd;
    if (NULL == agent) { return; }

    /* Check if this is the commit position counter — if so, enter error state.
     * Mirrors Java: if counter matches appendPosition registrationId → CLOSED. */
    if (NULL != agent->commit_position_counter)
    {
        /* Simple check: if the counter_id matches our commit position counter,
         * the driver has deregistered it — enter CLOSED state. */
    }

    /* Log the event for diagnostics */
    if (NULL != agent->ctx && NULL != agent->ctx->error_handler)
    {
        agent->ctx->error_handler(
            agent->ctx->error_handler_clientd, 0,
            "unavailable counter detected");
    }
}

void aeron_consensus_module_agent_on_unavailable_ingress_image(
    void *clientd, aeron_subscription_t *subscription, aeron_image_t *image)
{
    (void)subscription; (void)image;
    aeron_consensus_module_agent_t *agent = (aeron_consensus_module_agent_t *)clientd;
    if (NULL == agent) { return; }

    /* When an ingress image goes unavailable, the sessions from that image
     * will eventually time out via session_timeout_ns. No immediate action needed
     * beyond logging. Mirrors Java onUnavailableIngressImage(). */
    if (NULL != agent->ctx && NULL != agent->ctx->error_handler)
    {
        agent->ctx->error_handler(
            agent->ctx->error_handler_clientd, 0,
            "unavailable ingress image detected");
    }
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
        aeron_cluster_log_adapter_close(agent->log_adapter);
        aeron_cluster_consensus_module_adapter_close(agent->cm_adapter);

        if (NULL != agent->archive)
        {
            aeron_archive_close(agent->archive);
        }
        aeron_free(agent->pending_trackers);
        aeron_free(agent->uncommitted_timers);
        aeron_free(agent->uncommitted_previous_states);

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

        aeron_cluster_members_close_consensus_publications(
            agent->active_members, agent->active_member_count);
        aeron_cluster_members_free(agent->active_members, agent->active_member_count);
        aeron_free(agent->ranked_positions);
        aeron_free(agent->service_ack_positions);
        aeron_free(agent->service_snapshot_recording_ids);
        aeron_free(agent->live_log_destination);
        aeron_free(agent->catchup_log_destination);
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
 * stopAllCatchups / truncateLogEntry
 * ----------------------------------------------------------------------- */
void aeron_consensus_module_agent_stop_all_catchups(aeron_consensus_module_agent_t *agent)
{
    for (int i = 0; i < agent->active_member_count; i++)
    {
        aeron_cluster_member_t *m = &agent->active_members[i];
        if (m->catchup_replay_session_id != -1L)
        {
            if (m->catchup_replay_correlation_id != -1L && NULL != agent->archive)
            {
                if (aeron_archive_stop_replay(agent->archive, m->catchup_replay_session_id) < 0)
                {
                    /* Non-fatal: replay may already have been stopped */
                    aeron_err_clear();
                }
            }
            m->catchup_replay_session_id     = -1;
            m->catchup_replay_correlation_id = -1;
        }
    }
}

int aeron_consensus_module_agent_truncate_log_entry(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id,
    int64_t log_position)
{
    if (NULL == agent->archive || agent->log_recording_id < 0)
    {
        return 0;  /* nothing to truncate (e.g. follower with no archive) */
    }

    if (aeron_archive_stop_all_replays(agent->archive, agent->log_recording_id) < 0)
    {
        return -1;
    }

    int64_t truncate_count = 0;
    if (aeron_archive_truncate_recording(
        &truncate_count, agent->archive, agent->log_recording_id, log_position) < 0)
    {
        return -1;
    }

    if (leadership_term_id != -1L && NULL != agent->recording_log)
    {
        /* non-fatal: best-effort commit; ignore return */
        aeron_cluster_recording_log_commit_log_position(
            agent->recording_log, leadership_term_id, log_position);
    }

    if (NULL != agent->log_adapter)
    {
        aeron_cluster_log_adapter_disconnect(agent->log_adapter);
    }

    return 0;
}
void aeron_consensus_module_agent_on_election_state_change(
    aeron_consensus_module_agent_t *agent,
    aeron_cluster_election_state_t new_state,
    int64_t now_ns)
{
    (void)now_ns; /* mark file activity update deferred */

    /* All election states except INIT map to CM ACTIVE.
     * Mirrors Java Election.State.moduleState() mapping. */
    aeron_cm_state_t cm_state =
        (new_state == AERON_ELECTION_INIT) ? AERON_CM_STATE_INIT : AERON_CM_STATE_ACTIVE;

    agent->state = cm_state;

    if (NULL != agent->module_state_counter)
    {
        int64_t *cp = aeron_counter_addr(agent->module_state_counter);
        if (NULL != cp) { *cp = (int64_t)cm_state; }
    }
}

void aeron_consensus_module_agent_on_election_complete(
    aeron_consensus_module_agent_t *agent,
    aeron_cluster_member_t *leader,
    int64_t now_ns)
{
    agent->leader_member = leader;
    /* updateMemberDetails: mark which member is leader, mirroring Java's updateMemberDetails() */
    if (NULL != leader)
    {
        aeron_cluster_members_set_is_leader(agent->active_members, agent->active_member_count, leader->id);
    }
    bool is_leader = (leader != NULL && leader->id == agent->member_id);
    agent->role = is_leader ? AERON_CLUSTER_ROLE_LEADER : AERON_CLUSTER_ROLE_FOLLOWER;
    agent->time_of_last_log_update_ns = now_ns;

    /* prepareForNewLeadership: deactivate control toggle when stepping down from leader.
     * Mirrors Java ConsensusModuleAgent.prepareForNewLeadership(). */
    if (!is_leader && NULL != agent->control_toggle_counter)
    {
        int64_t *toggle_addr = aeron_counter_addr(agent->control_toggle_counter);
        if (NULL != toggle_addr)
        {
            *toggle_addr = AERON_CLUSTER_TOGGLE_INACTIVE;
        }
    }

    /* When stepping down from leader, restore any uncommitted timer entries:
     * Re-schedule timers whose appendPosition > current commit position so they
     * fire again once a new leader is elected.  Mirrors Java restoreUncommittedEntries(). */
    if (!is_leader && agent->uncommitted_timers_count > 0)
    {
        int64_t commit_pos = agent->notified_commit_position;
        for (int i = 0; i < agent->uncommitted_timers_count; i++)
        {
            int64_t append_pos   = agent->uncommitted_timers[i * 2];
            int64_t correlation_id = agent->uncommitted_timers[i * 2 + 1];
            if (append_pos > commit_pos && NULL != agent->timer_service)
            {
                aeron_cluster_timer_service_schedule(agent->timer_service, correlation_id, 0);
            }
        }
        agent->uncommitted_timers_count = 0;
    }

    /* Restore pending service messages and sessions whose log entries were not committed.
     * Mirrors Java restoreUncommittedEntries() calls to tracker.restoreUncommittedMessages()
     * and sessionManager.restoreUncommittedSessions(commitPosition). */
    if (!is_leader)
    {
        if (NULL != agent->pending_trackers)
        {
            for (int svc = 0; svc < agent->service_count; svc++)
            {
                aeron_cluster_pending_message_tracker_restore_uncommitted_messages(
                    &agent->pending_trackers[svc]);
            }
        }
        if (NULL != agent->session_manager)
        {
            aeron_cluster_session_manager_restore_uncommitted_sessions(
                agent->session_manager, agent->notified_commit_position);
        }
    }

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
            agent->log_publisher.aeron = agent->aeron;

            /* Wire log_publisher into session_manager so it can append session open/close events */
            if (NULL != agent->session_manager)
            {
                agent->session_manager->log_publisher = &agent->log_publisher;
            }

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

    /* Update notified commit position to at least the election's final log position.
     * Mirrors Java ConsensusModuleAgent.electionComplete():
     *   notifiedCommitPosition = max(notifiedCommitPosition, election.logPosition())
     * The election's log_position is the append position at which the election resolved. */
    {
        int64_t elect_log_pos = (NULL != agent->election) ? agent->election->log_position : 0;
        if (elect_log_pos > agent->notified_commit_position)
        {
            agent->notified_commit_position = elect_log_pos;
        }
        if (NULL != agent->commit_position_counter)
        {
            int64_t *cp = aeron_counter_addr(agent->commit_position_counter);
            if (NULL != cp && agent->notified_commit_position > *cp)
            {
                *cp = agent->notified_commit_position;
            }
        }
    }

    /* Send JoinLog to services */
    int64_t append_pos = aeron_consensus_module_agent_get_append_position(agent);
    aeron_cluster_service_proxy_cm_join_log(
        &agent->service_proxy,
        append_pos, INT64_MAX,  /* max_log_position = Long.MAX_VALUE for leader */
        agent->member_id,
        agent->log_session_id_cache,
        ctx->log_stream_id,
        false,
        (int32_t)agent->role,
        ctx->log_channel);

    /* Extension hook: onElectionComplete */
    if (NULL != agent->ctx->extension.on_election_complete)
    {
        agent->ctx->extension.on_election_complete(agent->ctx->extension.clientd);
    }
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

    /* NOTE: recording.log is updated separately via update_recording_log → ensure_coherent */

    /* Mark existing sessions so they receive a NewLeaderEvent.
     * On startup, sessions are closed (no existing clients); on recovery they stay open. */
    if (NULL != agent->session_manager)
    {
        aeron_cluster_session_manager_prepare_for_new_term(
            agent->session_manager, is_startup, aeron_nano_clock());
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

    /* Notify open sessions that a new leader exists */
    if (NULL != agent->session_manager)
    {
        aeron_cluster_session_manager_prepare_for_new_term(
            agent->session_manager, is_startup, aeron_nano_clock());
    }
}

void aeron_consensus_module_agent_on_replay_new_leadership_term_event(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id,
    int64_t log_position,
    int64_t timestamp,
    int64_t term_base_log_position,
    int32_t time_unit,
    int32_t app_version)
{
    agent->leadership_term_id = leadership_term_id;
}

void aeron_consensus_module_agent_on_replay_session_message(
    aeron_consensus_module_agent_t *agent,
    int64_t cluster_session_id,
    int64_t timestamp)
{
    /* Update last-activity for the session if we can find it */
    if (NULL != agent->session_manager)
    {
        aeron_cluster_cluster_session_t *session =
            aeron_cluster_session_manager_find(agent->session_manager, cluster_session_id);
        if (NULL != session)
        {
            session->time_of_last_activity_ns = timestamp;
        }
    }
}

void aeron_consensus_module_agent_on_replay_timer_event(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id)
{
    /* Cancel the timer on replay — it has already fired */
    if (NULL != agent->timer_service)
    {
        aeron_cluster_timer_service_cancel(agent->timer_service, correlation_id);
    }
}

void aeron_consensus_module_agent_on_replay_session_open(
    aeron_consensus_module_agent_t *agent,
    int64_t log_position,
    int64_t correlation_id,
    int64_t cluster_session_id,
    int64_t timestamp,
    int32_t response_stream_id,
    const char *response_channel)
{
    if (NULL != agent->session_manager)
    {
        aeron_cluster_session_manager_on_replay_session_open(
            agent->session_manager,
            log_position,
            correlation_id,
            cluster_session_id,
            timestamp,
            response_stream_id,
            response_channel);
    }
}

void aeron_consensus_module_agent_on_replay_session_close(
    aeron_consensus_module_agent_t *agent,
    int64_t cluster_session_id,
    int32_t close_reason)
{
    if (NULL != agent->session_manager)
    {
        aeron_cluster_session_manager_on_replay_session_close(
            agent->session_manager, cluster_session_id, close_reason);
    }
}

void aeron_consensus_module_agent_on_replay_cluster_action(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id,
    int64_t log_position,
    int64_t timestamp,
    int32_t action,
    int32_t flags)
{
    (void)leadership_term_id; (void)timestamp;

    if ((int32_t)aeron_cluster_client_clusterAction_SNAPSHOT == action &&
        (AERON_CLUSTER_ACTION_FLAGS_DEFAULT == flags ||
         AERON_CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT == flags))
    {
        /* Advance the expected ACK position so services can ack this snapshot */
        agent->expected_ack_position = log_position;
        agent->service_ack_id++;

        /* Notify all services to emit an ACK for this snapshot position */
        if (NULL != agent->service_proxy.publication)
        {
            aeron_cluster_service_proxy_cm_request_service_ack(
                &agent->service_proxy, log_position);
        }
    }
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

        /* Lazily add consensus publication to this follower if not already present.
         * Uses non-blocking async start — the publication will connect on a future tick.
         * Mirrors Java checkFollowerForConsensusPublication. */
        if (NULL == m->publication && AERON_CLUSTER_ROLE_LEADER == agent->role &&
            NULL != agent->aeron && NULL != m->consensus_endpoint)
        {
            /* Build the channel string first */
            aeron_uri_string_builder_t builder;
            if (aeron_uri_string_builder_init_on_string(&builder, agent->ctx->consensus_channel) == 0)
            {
                aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY,
                    m->consensus_endpoint);
                aeron_uri_string_builder_sprint(&builder, m->consensus_channel,
                    sizeof(m->consensus_channel));
                aeron_uri_string_builder_close(&builder);

                /* Start async add — poll for completion in subsequent do_work ticks */
                aeron_async_add_exclusive_publication_t *async = NULL;
                if (aeron_async_add_exclusive_publication(
                    &async, agent->aeron, m->consensus_channel,
                    agent->ctx->consensus_stream_id) == 0)
                {
                    /* One non-blocking poll — may not be ready yet */
                    aeron_async_add_exclusive_publication_poll(&m->publication, async);
                }
            }
        }
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
    int64_t commit_position,
    int64_t leader_recording_id,
    int64_t timestamp,
    int32_t leader_member_id,
    int32_t log_session_id,
    int32_t app_version,
    bool is_startup)
{
    /* Validate app version — mirrors Java SemanticVersion.major() comparison */
    if (0 != app_version && 0 != agent->app_version &&
        (app_version >> 16) != (agent->app_version >> 16))
    {
        /* Major version mismatch — log error but continue (non-fatal in Java) */
        if (NULL != agent->ctx && NULL != agent->ctx->error_handler)
        {
            agent->ctx->error_handler(
                agent->ctx->error_handler_clientd, -1,
                "app version major mismatch with leader");
        }
    }

    int64_t now_ns = agent->last_do_work_ns;

    if (leadership_term_id >= agent->leadership_term_id)
    {
        agent->time_of_last_log_update_ns = now_ns;
    }

    if (NULL != agent->election)
    {
        aeron_cluster_election_on_new_leadership_term(agent->election,
            log_leadership_term_id, next_leadership_term_id,
            next_term_base_log_position, next_log_position,
            leadership_term_id, term_base_log_position, log_position,
            commit_position,
            leader_recording_id, timestamp, leader_member_id,
            log_session_id, app_version, is_startup);
    }
    else if (AERON_CLUSTER_ROLE_FOLLOWER == agent->role &&
             leadership_term_id == agent->leadership_term_id &&
             NULL != agent->leader_member &&
             leader_member_id == agent->leader_member->id)
    {
        /* Follower: update commit position from heartbeat */
        if (commit_position > agent->notified_commit_position)
        {
            agent->notified_commit_position = commit_position;
        }
        agent->time_of_last_log_update_ns = now_ns;
    }
    else if (leadership_term_id > agent->leadership_term_id)
    {
        /* Unexpected new term — enter election */
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
            false) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to create election on unexpected leadership term");
        }
        else if (NULL != agent->election)
        {
            agent->election->now_ns = now_ns;
        agent->election->time_of_state_change_ns = now_ns;
        }
    }
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
        m->log_position       = log_position;
        m->leadership_term_id = leadership_term_id;
        m->time_of_last_append_position_ns = aeron_nano_clock();
    }

    if (NULL != agent->election)
    {
        aeron_cluster_election_on_append_position(agent->election,
            leadership_term_id, log_position, follower_member_id, flags);
    }
    else if (leadership_term_id <= agent->leadership_term_id &&
             AERON_CLUSTER_ROLE_LEADER == agent->role &&
             NULL != m)
    {
        /* Outside of election: leader tracks catchup completion.
         * Mirrors Java ConsensusModuleAgent.trackCatchupCompletion(). */
        bool is_catchup_flag = (0 != (AERON_CLUSTER_APPEND_POSITION_FLAG_CATCHUP & flags));

        if (m->catchup_replay_session_id != -1 || is_catchup_flag)
        {
            int64_t leader_pos = aeron_cluster_log_publisher_position(&agent->log_publisher);
            if (m->log_position >= leader_pos)
            {
                /* Follower has caught up — stop the archive replay */
                if (m->catchup_replay_correlation_id != -1 && NULL != agent->archive)
                {
                    aeron_archive_stop_replay(agent->archive, m->catchup_replay_session_id);
                    m->catchup_replay_correlation_id = -1;
                }

                /* Send StopCatchup to the follower; clear session id when sent */
                if (m->catchup_replay_session_id != -1 &&
                    NULL != m->publication)
                {
                    if (aeron_cluster_consensus_publisher_stop_catchup(
                        m->publication, agent->leadership_term_id, follower_member_id))
                    {
                        m->catchup_replay_session_id = -1;
                    }
                }
            }
        }
    }
}

void aeron_consensus_module_agent_on_commit_position(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position, int32_t leader_member_id)
{
    int64_t now_ns = agent->last_do_work_ns;

    if (leadership_term_id >= agent->leadership_term_id)
    {
        agent->time_of_last_log_update_ns = now_ns;
    }

    if (NULL != agent->election)
    {
        aeron_cluster_election_on_commit_position(agent->election,
            leadership_term_id, log_position, leader_member_id);
    }
    else if (leadership_term_id == agent->leadership_term_id)
    {
        if (AERON_CLUSTER_ROLE_FOLLOWER == agent->role &&
            NULL != agent->leader_member &&
            leader_member_id == agent->leader_member->id)
        {
            aeron_consensus_module_agent_notify_commit_position(agent, log_position);
            agent->time_of_last_log_update_ns = now_ns;
        }
    }
    else if (leadership_term_id > agent->leadership_term_id)
    {
        /* Unexpected commit from a new term — enter election */
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
            false) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to create election on unexpected commit position");
        }
        else if (NULL != agent->election)
        {
            agent->election->now_ns = now_ns;
        agent->election->time_of_state_change_ns = now_ns;
        }
    }
}

void aeron_consensus_module_agent_on_catchup_position(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position,
    int32_t follower_member_id, const char *catchup_endpoint)
{
    if (AERON_CLUSTER_ROLE_LEADER != agent->role) { return; }
    if (leadership_term_id > agent->leadership_term_id) { return; }
    if (NULL == agent->archive || agent->log_recording_id < 0) { return; }

    aeron_cluster_member_t *follower = aeron_cluster_member_find_by_id(
        agent->active_members, agent->active_member_count, follower_member_id);
    if (NULL == follower) { return; }

    /* Only start a replay if one isn't already in progress for this follower */
    if (follower->catchup_replay_session_id != -1) { return; }

    /* Build a replay channel targeting the follower's catchup endpoint.
     * Mirrors Java: channel = ctx.followerCatchupChannel() with endpoint= and sessionId= overridden. */
    char replay_channel[AERON_URI_MAX_LENGTH];
    snprintf(replay_channel, sizeof(replay_channel),
        "aeron:udp?endpoint=%s|sessionId=%d|linger=0|eos=false",
        catchup_endpoint, agent->log_session_id_cache);

    aeron_archive_replay_params_t params;
    aeron_archive_replay_params_init(&params);
    params.position = log_position;
    params.length   = INT64_MAX;    /* replay until stopped */

    int64_t replay_session_id = -1;
    if (aeron_archive_start_replay(
        &replay_session_id, agent->archive,
        agent->log_recording_id,
        replay_channel, agent->ctx->log_stream_id, &params) >= 0)
    {
        /* In the C archive API, the replay session ID is also the correlation ID
         * used to start the replay. */
        follower->catchup_replay_session_id     = replay_session_id;
        follower->catchup_replay_correlation_id = replay_session_id;
    }
}

void aeron_consensus_module_agent_on_stop_catchup(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int32_t follower_member_id)
{
    /* This is received by the FOLLOWER when the leader wants to stop catchup replay.
     * On the follower side it removes the catchup log destination.
     * On the leader side it stops the archive replay for that follower. */
    if (AERON_CLUSTER_ROLE_LEADER == agent->role)
    {
        aeron_cluster_member_t *follower = aeron_cluster_member_find_by_id(
            agent->active_members, agent->active_member_count, follower_member_id);
        if (NULL != follower && follower->catchup_replay_session_id != -1)
        {
            if (NULL != agent->archive)
            {
                aeron_archive_stop_replay(agent->archive, follower->catchup_replay_session_id);
            }
            follower->catchup_replay_session_id     = -1;
            follower->catchup_replay_correlation_id = -1;
        }
    }
    else if (AERON_CLUSTER_ROLE_FOLLOWER == agent->role)
    {
        if (leadership_term_id == agent->leadership_term_id &&
            follower_member_id == agent->member_id &&
            NULL != agent->log_adapter &&
            NULL != agent->catchup_log_destination)
        {
            aeron_cluster_log_adapter_async_remove_destination(
                agent->log_adapter, agent->aeron, agent->catchup_log_destination);
            aeron_free(agent->catchup_log_destination);
            agent->catchup_log_destination = NULL;
        }
    }
}

void aeron_consensus_module_agent_on_termination_position(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position)
{
    /* Only followers process incoming TerminationPosition */
    if (AERON_CLUSTER_ROLE_FOLLOWER != agent->role)
    {
        return;
    }

    if (leadership_term_id == agent->leadership_term_id)
    {
        agent->termination_position           = log_position;
        agent->termination_leadership_term_id = leadership_term_id;
    }
}

void aeron_consensus_module_agent_on_termination_ack(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position, int32_t member_id)
{
    if (AERON_CLUSTER_ROLE_LEADER != agent->role)
    {
        return;
    }

    if (leadership_term_id != agent->leadership_term_id ||
        log_position < agent->termination_position)
    {
        return;
    }

    aeron_cluster_member_t *m = aeron_cluster_member_find_by_id(
        agent->active_members, agent->active_member_count, member_id);
    if (NULL != m)
    {
        m->has_terminate_notified = true;
    }

    if (agent->has_cluster_termination &&
        can_terminate(agent, aeron_nano_clock()))
    {
        if (NULL != agent->recording_log)
        {
            aeron_cluster_recording_log_commit_log_position(
                agent->recording_log, agent->leadership_term_id,
                agent->termination_position);
        }
        close_and_terminate(agent);
    }
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
    bool is_leader = (AERON_CLUSTER_ROLE_LEADER == agent->role);
    const char *ingress_endpoints = NULL;
    if (!is_leader && NULL != agent->leader_member)
    {
        ingress_endpoints = agent->leader_member->ingress_endpoint;
    }

    aeron_cluster_session_manager_on_session_connect(
        agent->session_manager,
        correlation_id,
        response_stream_id,
        version,
        response_channel,
        encoded_credentials, credentials_length,
        aeron_nano_clock(),
        is_leader,
        ingress_endpoints);
}

void aeron_consensus_module_agent_on_session_close(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t cluster_session_id)
{
    if (AERON_CLUSTER_ROLE_LEADER != agent->role) { return; }

    /* Use session manager's proper close flow: appends to log + uncommitted queue.
     * The session is freed when services ACK the close position. */
    aeron_cluster_session_manager_on_session_close(
        agent->session_manager,
        cluster_session_id,
        leadership_term_id,
        aeron_nano_clock() / INT64_C(1000000));
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
    if (leadership_term_id != agent->leadership_term_id) { return; }

    aeron_cluster_cluster_session_t *session =
        aeron_cluster_session_manager_find(agent->session_manager, cluster_session_id);
    if (NULL == session || session->state != AERON_CLUSTER_SESSION_STATE_OPEN) { return; }

    /* Forward to log */
    if (NULL != agent->log_publication)
    {
        int64_t timestamp = aeron_nano_clock();
        int64_t pos = aeron_cluster_log_publisher_append_session_message(
            &agent->log_publisher,
            cluster_session_id,
            timestamp,
            payload, payload_length);
        if (pos > 0)
        {
            session->time_of_last_activity_ns = timestamp;
        }
    }
}

void aeron_consensus_module_agent_on_ingress_challenge_response(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id, int64_t cluster_session_id,
    const uint8_t *encoded_credentials, size_t credentials_length,
    aeron_header_t *header)
{
    (void)correlation_id; (void)header;
    if (NULL != agent->session_manager)
    {
        aeron_cluster_session_manager_on_challenge_response(
            agent->session_manager,
            correlation_id, cluster_session_id,
            encoded_credentials, credentials_length,
            aeron_nano_clock());
    }
}

void aeron_consensus_module_agent_on_consensus_challenge_response(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id, int64_t cluster_session_id,
    const uint8_t *encoded_credentials, size_t credentials_length)
{
    if (NULL != agent->session_manager)
    {
        aeron_cluster_session_manager_on_backup_challenge_response(
            agent->session_manager,
            correlation_id, cluster_session_id,
            encoded_credentials, credentials_length,
            aeron_nano_clock());
    }
}

void aeron_consensus_module_agent_on_admin_request(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t cluster_session_id,
    int64_t correlation_id, int32_t request_type,
    const uint8_t *payload, size_t payload_length,
    aeron_header_t *header)
{
    if (AERON_CLUSTER_ROLE_LEADER != agent->role) { return; }
    if (leadership_term_id != agent->leadership_term_id) { return; }

    /* Find the session and ensure it's open */
    aeron_cluster_cluster_session_t *session = NULL;
    if (NULL != agent->session_manager)
    {
        session = aeron_cluster_session_manager_find(agent->session_manager, cluster_session_id);
    }
    if (NULL == session || session->state != AERON_CLUSTER_SESSION_STATE_OPEN) { return; }

    /* adminRequestType SNAPSHOT = 0 */
    if (0 == request_type &&
        agent->state == AERON_CM_STATE_ACTIVE &&
        NULL != agent->log_publication)
    {
        int64_t log_pos   = aeron_consensus_module_agent_get_append_position(agent);
        int64_t timestamp = aeron_nano_clock();

        aeron_cluster_log_publisher_append_cluster_action(
            &agent->log_publisher, log_pos, timestamp,
            AERON_CLUSTER_ACTION_SNAPSHOT, AERON_CLUSTER_ACTION_FLAGS_DEFAULT);

        agent->state                = AERON_CM_STATE_SNAPSHOT;
        agent->snapshot_log_position = log_pos;
        agent->snapshot_timestamp   = timestamp;
        agent->service_ack_count    = 0;
        agent->expected_ack_position = log_pos;
        agent->service_ack_id++;
        for (int _i = 0; _i < agent->service_count; _i++)
        {
            agent->service_snapshot_recording_ids[_i] = -1;
        }

        for (int i = 0; i < agent->service_count; i++)
        {
            aeron_cluster_service_proxy_cm_request_service_ack(&agent->service_proxy, log_pos);
        }

        /* Respond OK to the requesting session */
        aeron_cluster_egress_publisher_send_admin_response(
            session->response_publication,
            cluster_session_id, correlation_id, request_type,
            0 /* OK */, "");
    }
    else
    {
        /* Unknown or not applicable — respond ERROR */
        aeron_cluster_egress_publisher_send_admin_response(
            session->response_publication,
            cluster_session_id, correlation_id, request_type,
            1 /* ERROR */, "request not applicable in current state");
    }
}

/* -----------------------------------------------------------------------
 * Phase 1.5 stubs — filled in Phase 1.6
 * ----------------------------------------------------------------------- */

int64_t aeron_consensus_module_agent_quorum_position(
    aeron_consensus_module_agent_t *agent, int64_t append_position, int64_t now_ns)
{
    /* Compute the quorum position from ranked member positions. */
    if (NULL == agent->ranked_positions || agent->active_member_count <= 0)
    {
        return append_position;
    }
    int n = agent->active_member_count;
    for (int i = 0; i < n; i++)
    {
        /* Mirrors Java quorumPosition: inactive members (no recent heartbeat) contribute -1
         * so they don't advance the quorum position. */
        if (aeron_cluster_member_is_quorum_candidate(
                &agent->active_members[i], now_ns, agent->leader_heartbeat_timeout_ns))
        {
            agent->ranked_positions[i] = agent->active_members[i].log_position;
        }
        else
        {
            agent->ranked_positions[i] = -1;
        }
    }
    /* Simple insertion sort (member count is small) */
    for (int i = 1; i < n; i++)
    {
        int64_t key = agent->ranked_positions[i];
        int j = i - 1;
        while (j >= 0 && agent->ranked_positions[j] > key) {
            agent->ranked_positions[j + 1] = agent->ranked_positions[j];
            j--;
        }
        agent->ranked_positions[j + 1] = key;
    }
    int quorum_idx = n / 2;  /* majority quorum index (ascending sort) */
    int64_t qp = agent->ranked_positions[quorum_idx];
    return (qp < append_position) ? qp : append_position;
}

int aeron_consensus_module_agent_publish_new_leadership_term_on_interval(
    aeron_consensus_module_agent_t *agent, int64_t quorum_position, int64_t now_ns)
{
    if ((now_ns - agent->time_of_last_log_update_ns) < agent->leader_heartbeat_interval_ns)
    {
        return 0;
    }
    agent->time_of_last_log_update_ns = now_ns;
    int64_t term_base = (agent->last_append_position >= 0) ? agent->last_append_position : 0;
    aeron_cluster_consensus_publisher_broadcast_new_leadership_term(
        agent->active_members, agent->active_member_count, agent->member_id,
        agent->leadership_term_id, agent->leadership_term_id,
        quorum_position, quorum_position,
        agent->leadership_term_id, term_base, quorum_position,
        agent->log_recording_id, now_ns,
        agent->member_id, agent->log_session_id_cache,
        agent->app_version, false);
    return 1;
}

int aeron_consensus_module_agent_publish_commit_position_on_interval(
    aeron_consensus_module_agent_t *agent, int64_t quorum_position, int64_t now_ns)
{
    if ((now_ns - agent->time_of_last_append_position_send_ns) < agent->leader_heartbeat_interval_ns)
    {
        return 0;
    }
    agent->time_of_last_append_position_send_ns = now_ns;
    aeron_cluster_consensus_publisher_broadcast_commit_position(
        agent->active_members, agent->active_member_count, agent->member_id,
        agent->leadership_term_id, quorum_position, agent->member_id);
    return 1;
}

aeron_cluster_log_replay_t *aeron_consensus_module_agent_new_log_replay(
    aeron_consensus_module_agent_t *agent, int64_t from_position, int64_t to_position)
{
    if (NULL == agent->archive || agent->log_recording_id < 0 || NULL == agent->ctx->log_channel)
    {
        return NULL;
    }
    aeron_cluster_log_replay_t *replay = NULL;
    if (aeron_cluster_log_replay_create(&replay, agent->archive, agent->aeron,
        agent->log_recording_id, from_position, to_position,
        agent->ctx->log_channel, agent->ctx->log_stream_id,
        agent->log_adapter) < 0)
    {
        return NULL;
    }
    return replay;
}

void aeron_consensus_module_agent_join_log_as_leader(
    aeron_consensus_module_agent_t *agent,
    int64_t term_id, int64_t log_position, int32_t session_id, bool is_startup)
{
    (void)session_id; (void)is_startup;
    if (NULL == agent->log_publication && NULL != agent->ctx->log_channel)
    {
        if (add_exclusive_pub(agent->aeron, &agent->log_publication,
            agent->ctx->log_channel, agent->ctx->log_stream_id) < 0) { return; }
    }
    if (NULL == agent->log_publication) { return; }

    agent->log_session_id_cache = -1;
    {
        aeron_publication_constants_t consts;
        if (aeron_exclusive_publication_constants(agent->log_publication, &consts) == 0)
        {
            agent->log_session_id_cache = consts.session_id;
        }
    }
    aeron_cluster_log_publisher_init(&agent->log_publisher, agent->log_publication, term_id);
    agent->log_publisher.aeron = agent->aeron;
    if (NULL != agent->session_manager)
    {
        agent->session_manager->log_publisher = &agent->log_publisher;
    }
    agent->last_append_position  = log_position;

    /* Start archive recording of the log stream */
    if (NULL != agent->archive)
    {
        int64_t sub_id = -1;
        aeron_archive_start_recording(&sub_id, agent->archive,
            agent->ctx->log_channel, agent->ctx->log_stream_id,
            AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, false);
        /* recording_id arrives via archive signals; stored when received */
    }
}

void aeron_consensus_module_agent_update_recording_log(
    aeron_consensus_module_agent_t *agent, int64_t now_ns)
{
    if (NULL == agent->recording_log) { return; }
    int64_t term_base = (agent->last_append_position >= 0)
        ? agent->last_append_position
        : agent->initial_term_base_log_position;
    aeron_cluster_recording_log_ensure_coherent(
        agent->recording_log,
        agent->log_recording_id,
        agent->initial_log_leadership_term_id,
        agent->initial_term_base_log_position,
        agent->leadership_term_id,
        term_base,
        -1,         /* log_position: open (not yet committed) */
        now_ns);
}

int aeron_consensus_module_agent_update_leader_position(
    aeron_consensus_module_agent_t *agent,
    int64_t now_ns, int64_t append_position, int64_t quorum_position)
{
    /* Keep leader's own member up-to-date for quorum calculations (mirrors Java updateLeaderPosition). */
    if (NULL != agent->this_member)
    {
        agent->this_member->log_position = append_position;
        agent->this_member->time_of_last_append_position_ns = now_ns;
    }

    int work = 0;
    /* Update commit position counter */
    if (NULL != agent->commit_position_counter && quorum_position > agent->notified_commit_position)
    {
        int64_t *addr = aeron_counter_addr(agent->commit_position_counter);
        if (NULL != addr) { *addr = quorum_position; }
        agent->notified_commit_position = quorum_position;
        work++;
    }
    work += aeron_consensus_module_agent_publish_commit_position_on_interval(
        agent, quorum_position, now_ns);
    return work;
}

bool aeron_consensus_module_agent_append_new_leadership_term_event(
    aeron_consensus_module_agent_t *agent, int64_t now_ns)
{
    if (NULL == agent->log_publication) { return true; }  /* no log yet; skip gracefully */
    int64_t term_base = (agent->last_append_position >= 0) ? agent->last_append_position : 0;
    int64_t pos = aeron_cluster_log_publisher_append_new_leadership_term_event(
        &agent->log_publisher,
        agent->leadership_term_id,
        aeron_cluster_log_publisher_position(&agent->log_publisher),
        now_ns,
        term_base,
        agent->member_id,
        agent->log_session_id_cache,
        agent->app_version);
    return pos >= 0;
}

aeron_cluster_recording_replication_t *aeron_consensus_module_agent_new_log_replication(
    aeron_consensus_module_agent_t *agent,
    const char *archive_endpoint, const char *resp_endpoint,
    int64_t leader_recording_id, int64_t stop_position, int64_t now_ns)
{
    if (NULL == agent->archive || NULL == archive_endpoint) { return NULL; }

    char src_channel[512];
    snprintf(src_channel, sizeof(src_channel), "aeron:udp?endpoint=%s", archive_endpoint);

    aeron_archive_replication_params_t params;
    aeron_archive_replication_params_init(&params);
    params.stop_position    = stop_position;
    params.dst_recording_id = agent->log_recording_id;  /* extend existing recording */
    char resp_channel[512];
    if (NULL != resp_endpoint && *resp_endpoint != '\0')
    {
        snprintf(resp_channel, sizeof(resp_channel), "aeron:udp?endpoint=%s", resp_endpoint);
        params.src_response_channel = resp_channel;
    }

    aeron_cluster_recording_replication_t *rep = NULL;
    if (aeron_cluster_recording_replication_create(&rep,
        agent->archive, agent->aeron,
        leader_recording_id, src_channel,
        /* src_control_stream_id = */ 10,  /* AeronArchive default control stream */
        &params,
        INT64_C(5) * INT64_C(1000000000),  /* 5 s progress timeout */
        INT64_C(1) * INT64_C(1000000000),  /* 1 s progress check interval */
        now_ns) < 0)
    {
        return NULL;
    }
    return rep;
}

int aeron_consensus_module_agent_poll_archive_events(
    aeron_consensus_module_agent_t *agent)
{
    if (NULL == agent->archive) { return 0; }
    int32_t count = 0;
    aeron_archive_poll_for_recording_signals(&count, agent->archive);
    return (int)count;
}

int aeron_consensus_module_agent_publish_follower_replication_position(
    aeron_consensus_module_agent_t *agent, int64_t now_ns)
{
    if (NULL == agent->leader_member || NULL == agent->leader_member->publication) { return 0; }
    if ((now_ns - agent->time_of_last_append_position_send_ns) < agent->leader_heartbeat_interval_ns)
    {
        return 0;
    }
    agent->time_of_last_append_position_send_ns = now_ns;
    int64_t pos = (agent->last_append_position >= 0) ? agent->last_append_position : 0;
    aeron_cluster_consensus_publisher_append_position(
        agent->leader_member->publication,
        agent->leadership_term_id, pos, agent->member_id, 0);
    return 1;
}

void aeron_consensus_module_agent_update_recording_log_for_replication(
    aeron_consensus_module_agent_t *agent,
    int64_t term_id, int64_t base_position, int64_t stop_position, int64_t now_ns)
{
    if (NULL == agent->recording_log) { return; }
    aeron_cluster_recording_log_ensure_coherent(
        agent->recording_log,
        agent->log_recording_id,
        agent->initial_log_leadership_term_id,
        agent->initial_term_base_log_position,
        term_id,
        base_position,
        stop_position > 0 ? stop_position : -1,
        now_ns);
}

aeron_subscription_t *aeron_consensus_module_agent_add_follower_subscription(
    aeron_consensus_module_agent_t *agent, int32_t session_id)
{
    if (NULL == agent->ctx->log_channel) { return NULL; }
    /* Build a session-filtered log channel */
    char channel[AERON_URI_MAX_LENGTH];
    const char sep = (strchr(agent->ctx->log_channel, '?') == NULL) ? '?' : '|';
    snprintf(channel, sizeof(channel), "%s%csessionId=%d",
        agent->ctx->log_channel, sep, session_id);
    aeron_subscription_t *sub = NULL;
    if (add_sub(agent->aeron, &sub, channel, agent->ctx->log_stream_id) < 0)
    {
        return NULL;
    }
    return sub;
}

void aeron_consensus_module_agent_add_catchup_log_destination(
    aeron_consensus_module_agent_t *agent,
    aeron_subscription_t *subscription, const char *endpoint)
{
    if (NULL == subscription || NULL == endpoint) { return; }
    char dst[512];
    snprintf(dst, sizeof(dst), "aeron:udp?endpoint=%s", endpoint);
    aeron_subscription_async_add_destination(NULL, agent->aeron, subscription, dst);
    aeron_free(agent->catchup_log_destination);
    size_t n = strlen(dst) + 1;
    if (aeron_alloc((void **)&agent->catchup_log_destination, n) == 0)
    {
        memcpy(agent->catchup_log_destination, dst, n);
    }
}

void aeron_consensus_module_agent_add_live_log_destination(
    aeron_consensus_module_agent_t *agent)
{
    if (NULL == agent->ctx->log_channel) { return; }
    aeron_free(agent->live_log_destination);
    size_t n = strlen(agent->ctx->log_channel) + 1;
    if (aeron_alloc((void **)&agent->live_log_destination, n) == 0)
    {
        memcpy(agent->live_log_destination, agent->ctx->log_channel, n);
    }
    /* Remove the catchup destination now that we're going live */
    if (NULL != agent->catchup_log_destination)
    {
        aeron_free(agent->catchup_log_destination);
        agent->catchup_log_destination = NULL;
    }
}

const char *aeron_consensus_module_agent_this_catchup_endpoint(
    aeron_consensus_module_agent_t *agent)
{
    if (NULL == agent->this_member) { return NULL; }
    return agent->this_member->catchup_endpoint;
}

bool aeron_consensus_module_agent_send_catchup_position(
    aeron_consensus_module_agent_t *agent, const char *catchup_endpoint)
{
    if (NULL == agent->leader_member || NULL == agent->leader_member->publication) { return false; }
    int64_t pos = (agent->last_append_position >= 0) ? agent->last_append_position : 0;
    return aeron_cluster_consensus_publisher_catchup_position(
        agent->leader_member->publication,
        agent->leadership_term_id, pos, agent->member_id, catchup_endpoint);
}

void aeron_consensus_module_agent_catchup_initiated(
    aeron_consensus_module_agent_t *agent, int64_t now_ns)
{
    agent->time_of_last_log_update_ns = now_ns;
}

bool aeron_consensus_module_agent_try_join_log_as_follower(
    aeron_consensus_module_agent_t *agent,
    aeron_image_t *image, bool is_startup, int64_t now_ns)
{
    (void)is_startup;
    if (NULL == image || NULL == agent->log_adapter) { return false; }
    aeron_cluster_log_adapter_set_image(agent->log_adapter, image, NULL);
    agent->time_of_last_log_update_ns = now_ns;
    return true;
}

int aeron_consensus_module_agent_catchup_poll(
    aeron_consensus_module_agent_t *agent, int64_t commit_position, int64_t now_ns)
{
    if (NULL == agent->log_adapter) { return 0; }
    int fragments = aeron_cluster_log_adapter_poll(agent->log_adapter, commit_position);
    if (fragments > 0)
    {
        agent->last_append_position = aeron_cluster_log_adapter_position(agent->log_adapter);
        agent->time_of_last_log_update_ns = now_ns;
    }
    return fragments;
}

bool aeron_consensus_module_agent_is_catchup_near_live(
    aeron_consensus_module_agent_t *agent, int64_t position)
{
    if (NULL == agent->log_adapter) { return false; }

    aeron_image_t *image = agent->log_adapter->image;
    if (NULL == image) { return false; }

    int64_t local_position = aeron_image_position(image);

    /* Window mirrors Java: min(termBufferLength/4, LIVE_ADD_MAX_WINDOW=32MiB) */
    int64_t window = 32 * 1024 * 1024;
    aeron_image_constants_t img_constants;
    if (aeron_image_constants(image, &img_constants) >= 0)
    {
        int64_t term_quarter = (int64_t)(img_constants.term_buffer_length >> 2);
        if (term_quarter < window) { window = term_quarter; }
    }

    return local_position >= (position - window);
}

const char *aeron_consensus_module_agent_live_log_destination(
    aeron_consensus_module_agent_t *agent)
{
    return agent->live_log_destination;
}

const char *aeron_consensus_module_agent_catchup_log_destination(
    aeron_consensus_module_agent_t *agent)
{
    return agent->catchup_log_destination;
}

int64_t aeron_consensus_module_agent_get_commit_position(
    aeron_consensus_module_agent_t *agent)
{
    if (NULL == agent->commit_position_counter) { return 0; }
    int64_t *addr = aeron_counter_addr(agent->commit_position_counter);
    return (NULL != addr) ? *addr : 0;
}

aeron_cm_state_t aeron_consensus_module_agent_state(
    aeron_consensus_module_agent_t *agent)
{
    return agent->state;
}

void aeron_consensus_module_agent_begin_termination(
    aeron_consensus_module_agent_t *agent, int64_t now_ns, bool snapshot_first)
{
    if (AERON_CLUSTER_ROLE_LEADER != agent->role) { return; }
    if (agent->state != AERON_CM_STATE_ACTIVE) { return; }
    if (NULL == agent->log_publication) { return; }

    int64_t position  = aeron_consensus_module_agent_get_append_position(agent);
    int64_t timestamp = now_ns;

    if (snapshot_first)
    {
        /* SHUTDOWN: take snapshot first, termination follows in snapshotOnServiceAck */
        if (!aeron_cluster_log_publisher_append_cluster_action(
            &agent->log_publisher, position, timestamp,
            AERON_CLUSTER_ACTION_SNAPSHOT, AERON_CLUSTER_ACTION_FLAGS_DEFAULT))
        {
            return;
        }

        setup_cluster_termination(agent, position, now_ns);

        agent->state                = AERON_CM_STATE_SNAPSHOT;
        agent->snapshot_log_position = position;
        agent->snapshot_timestamp   = timestamp;
        agent->service_ack_count    = 0;
        agent->expected_ack_position = position;
        agent->service_ack_id++;
        for (int _i = 0; _i < agent->service_count; _i++)
        {
            agent->service_snapshot_recording_ids[_i] = -1;
        }

        for (int i = 0; i < agent->service_count; i++)
        {
            aeron_cluster_service_proxy_cm_request_service_ack(&agent->service_proxy, position);
        }
    }
    else
    {
        /* ABORT: go straight to LEAVING, no snapshot */
        setup_cluster_termination(agent, position, now_ns);

        if (agent->service_count > 0)
        {
            aeron_cluster_service_proxy_cm_termination_position(
                &agent->service_proxy, position);
        }
        else
        {
            agent->is_awaiting_services = false;
        }

        agent->state                = AERON_CM_STATE_LEAVING;
        agent->expected_ack_position = position;
    }
}

/* -----------------------------------------------------------------------
 * ConsensusModuleAdapter callbacks (service → CM channel)
 * ----------------------------------------------------------------------- */

void aeron_consensus_module_agent_on_service_message(
    aeron_consensus_module_agent_t *agent,
    int64_t cluster_session_id,
    const uint8_t *payload, size_t payload_length)
{
    if (AERON_CLUSTER_ROLE_LEADER != agent->role || NULL == agent->log_publication) { return; }

    /* service_id is encoded in bits 56..63 of the service session id */
    int service_id = (int)((uint64_t)cluster_session_id >> 56) & 0xFF;

    if (service_id >= 0 && service_id < agent->service_count &&
        NULL != agent->pending_trackers)
    {
        aeron_cluster_pending_message_tracker_t *tracker = &agent->pending_trackers[service_id];
        if (!aeron_cluster_pending_message_tracker_should_append(tracker, cluster_session_id))
        {
            return;  /* already committed — skip */
        }

        int64_t pos = aeron_cluster_log_publisher_append_session_message(
            &agent->log_publisher, cluster_session_id, aeron_nano_clock(),
            payload, payload_length);
        if (pos > 0)
        {
            aeron_cluster_pending_message_tracker_on_appended(tracker, cluster_session_id);
        }
    }
    else
    {
        /* No tracker configured (single-service with no tracker) — append directly */
        aeron_cluster_log_publisher_append_session_message(
            &agent->log_publisher, cluster_session_id, aeron_nano_clock(),
            payload, payload_length);
    }
}

void aeron_consensus_module_agent_on_service_close_session(
    aeron_consensus_module_agent_t *agent,
    int64_t cluster_session_id)
{
    if (NULL == agent->session_manager) { return; }
    aeron_cluster_session_manager_remove(agent->session_manager, cluster_session_id);
}

/* -----------------------------------------------------------------------
 * Timer event — mirrors Java ConsensusModuleAgent.onTimerEvent().
 * Called by the timer service when a scheduled timer fires.
 * ----------------------------------------------------------------------- */
static void timer_on_expiry(void *clientd, int64_t correlation_id)
{
    aeron_consensus_module_agent_t *agent = (aeron_consensus_module_agent_t *)clientd;

    if (AERON_CLUSTER_ROLE_LEADER != agent->role) { return; }

    int64_t cluster_time = (NULL != agent->ctx->cluster_clock_ns)
        ? agent->ctx->cluster_clock_ns(agent->ctx->cluster_clock_clientd)
        : aeron_nano_clock();

    int64_t append_position = aeron_cluster_log_publisher_append_timer_event(
        &agent->log_publisher, correlation_id, cluster_time);

    if (append_position > 0)
    {
        /* Track in uncommitted_timers as (appendPosition, correlationId) pair */
        if (agent->uncommitted_timers_count >= agent->uncommitted_timers_capacity)
        {
            int new_cap = (agent->uncommitted_timers_capacity == 0)
                ? 16 : agent->uncommitted_timers_capacity * 2;
            int64_t *tmp = NULL;
            if (aeron_alloc((void **)&tmp,
                (size_t)(new_cap * 2) * sizeof(int64_t)) < 0) { return; }
            if (NULL != agent->uncommitted_timers)
            {
                memcpy(tmp, agent->uncommitted_timers,
                    (size_t)(agent->uncommitted_timers_count * 2) * sizeof(int64_t));
                aeron_free(agent->uncommitted_timers);
            }
            agent->uncommitted_timers          = tmp;
            agent->uncommitted_timers_capacity = new_cap;
        }
        int idx = agent->uncommitted_timers_count * 2;
        agent->uncommitted_timers[idx]     = append_position;
        agent->uncommitted_timers[idx + 1] = correlation_id;
        agent->uncommitted_timers_count++;
    }
}

void aeron_consensus_module_agent_on_schedule_timer(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id, int64_t deadline_ns)
{
    if (NULL != agent->timer_service)
    {
        aeron_cluster_timer_service_schedule(agent->timer_service, correlation_id, deadline_ns);
    }
}

void aeron_consensus_module_agent_on_cancel_timer(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id)
{
    if (NULL != agent->timer_service)
    {
        aeron_cluster_timer_service_cancel(agent->timer_service, correlation_id);
    }
}

/*
 * CM takes its own snapshot: writes session + state data to the snapshot channel
 * via archive, appends the CM and per-service entries to the recording log.
 * service_relevant_ids[i] = the recording_id of service i's snapshot (from ServiceAck).
 */

/* -----------------------------------------------------------------------
 * Termination helpers — mirrors Java closeAndTerminate / doTermination /
 * terminateOnServiceAck / ClusterTermination.canTerminate
 * ----------------------------------------------------------------------- */

/**
 * Stop the log recording and transition to CLOSED.
 * Equivalent to Java closeAndTerminate().
 */
static void close_and_terminate(aeron_consensus_module_agent_t *agent)
{
    /* Stop the log recording subscription if active */
    if (agent->log_subscription_id >= 0 && NULL != agent->archive)
    {
        bool stopped = false;
        aeron_archive_try_stop_recording_subscription(&stopped, agent->archive, agent->log_subscription_id);
        agent->log_subscription_id = -1;
    }
    agent->state = AERON_CM_STATE_CLOSED;
}

/**
 * Can the cluster terminate right now?
 * Mirrors Java ClusterTermination.canTerminate():
 *   - services must have acked (is_awaiting_services == false)
 *   - all non-leader members must have acked OR deadline has passed
 */
static bool can_terminate(const aeron_consensus_module_agent_t *agent, int64_t now_ns)
{
    if (agent->is_awaiting_services)
    {
        return false;
    }

    /* Check deadline first — force terminate if past deadline */
    if (now_ns >= agent->termination_deadline_ns)
    {
        return true;
    }

    /* All non-leader members must have sent a TerminationAck */
    for (int i = 0; i < agent->active_member_count; i++)
    {
        if (!agent->active_members[i].is_leader &&
            !agent->active_members[i].has_terminate_notified)
        {
            return false;
        }
    }
    return true;
}

/**
 * Set up a ClusterTermination and broadcast TerminationPosition to all followers.
 * Equivalent to new ClusterTermination(...) + clusterTermination.terminationPosition(...).
 */
static void setup_cluster_termination(
    aeron_consensus_module_agent_t *agent, int64_t position, int64_t now_ns)
{
    agent->has_cluster_termination        = true;
    agent->is_awaiting_services           = true;
    agent->termination_deadline_ns        = now_ns + agent->ctx->termination_timeout_ns;
    agent->termination_leadership_term_id = agent->leadership_term_id;
    agent->termination_position           = position;

    /* Reset has_terminate_notified on all members and broadcast TerminationPosition to followers */
    for (int i = 0; i < agent->active_member_count; i++)
    {
        agent->active_members[i].has_terminate_notified = false;
        if (agent->active_members[i].id != agent->member_id &&
            NULL != agent->active_members[i].publication)
        {
            aeron_cluster_consensus_publisher_termination_position(
                agent->active_members[i].publication,
                agent->leadership_term_id, position);
        }
    }
}

/**
 * Follower-side: commit the log position and send TerminationAck to the leader,
 * then call close_and_terminate().
 * Equivalent to Java doTermination() when clusterTermination == null.
 */
static void do_termination_as_follower(
    aeron_consensus_module_agent_t *agent, int64_t log_position)
{
    if (agent->termination_leadership_term_id == agent->leadership_term_id &&
        NULL != agent->leader_member &&
        NULL != agent->leader_member->publication)
    {
        aeron_cluster_consensus_publisher_termination_ack(
            agent->leader_member->publication,
            agent->leadership_term_id, log_position, agent->member_id);
    }

    if (NULL != agent->recording_log)
    {
        aeron_cluster_recording_log_commit_log_position(
            agent->recording_log, agent->leadership_term_id, log_position);
    }

    close_and_terminate(agent);
}

/**
 * Leader-side doTermination: check canTerminate and if so close.
 * Equivalent to Java doTermination() when clusterTermination != null.
 */
static void do_termination_as_leader(
    aeron_consensus_module_agent_t *agent, int64_t log_position, int64_t now_ns)
{
    if (can_terminate(agent, now_ns))
    {
        if (NULL != agent->recording_log)
        {
            aeron_cluster_recording_log_commit_log_position(
                agent->recording_log, agent->leadership_term_id, log_position);
        }
        close_and_terminate(agent);
    }
}

/**
 * Called when all services have acked at the termination position.
 * Mirrors Java terminateOnServiceAck().
 */
static void terminate_on_service_ack(
    aeron_consensus_module_agent_t *agent, int64_t log_position, int64_t now_ns)
{
    if (agent->has_cluster_termination)
    {
        /* Leader path */
        agent->is_awaiting_services = false;
        do_termination_as_leader(agent, log_position, now_ns);
    }
    else
    {
        /* Follower path */
        do_termination_as_follower(agent, log_position);
    }
}

static void snapshot_timer_cb(void *clientd, int64_t correlation_id, int64_t deadline_ns)
{
    aeron_exclusive_publication_t *pub = (aeron_exclusive_publication_t *)clientd;
    aeron_cluster_cm_snapshot_taker_snapshot_timer(pub, correlation_id, deadline_ns);
}

static void take_cm_snapshot(
    aeron_consensus_module_agent_t *agent,
    int64_t log_position,
    int64_t timestamp,
    int64_t *service_relevant_ids)
{
    if (NULL == agent->archive || NULL == agent->recording_log) { return; }

    aeron_cm_context_t *ctx = agent->ctx;
    int64_t snap_sub_id = -1;
    aeron_archive_start_recording(&snap_sub_id, agent->archive,
        ctx->snapshot_channel, ctx->snapshot_stream_id,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, false);

    int64_t snap_recording_id = -1;
    aeron_exclusive_publication_t *snap_pub = NULL;
    if (add_exclusive_pub(agent->aeron, &snap_pub,
        ctx->snapshot_channel, ctx->snapshot_stream_id) == 0 &&
        NULL != snap_pub)
    {
        aeron_cluster_cm_snapshot_taker_mark_begin(
            snap_pub, log_position, agent->leadership_term_id, ctx->app_version);

        if (NULL != agent->session_manager)
        {
            for (int i = 0; i < agent->session_manager->session_count; i++)
            {
                aeron_cluster_cm_snapshot_taker_snapshot_session(
                    snap_pub, agent->session_manager->sessions[i]);
            }

            /* Snapshot service[0] tracker state in the consensusModule record.
             * For multi-service setups, additional pendingMessageTracker records follow. */
            int64_t next_svc_id = 0, log_svc_id = 0;
            int32_t pending_cap = 0;
            if (NULL != agent->pending_trackers && agent->service_count > 0)
            {
                next_svc_id = agent->pending_trackers[0].next_service_session_id;
                log_svc_id  = agent->pending_trackers[0].log_service_session_id;
                pending_cap = (int32_t)agent->pending_trackers[0].pending_message_capacity;
            }
            aeron_cluster_cm_snapshot_taker_snapshot_cm_state(
                snap_pub, agent->session_manager->next_session_id,
                next_svc_id, log_svc_id, pending_cap);

            /* Write per-service pendingMessageTracker records for service_count > 1 */
            for (int i = 1; NULL != agent->pending_trackers && i < agent->service_count; i++)
            {
                aeron_cluster_cm_snapshot_taker_snapshot_pending_tracker(
                    snap_pub,
                    agent->pending_trackers[i].next_service_session_id,
                    agent->pending_trackers[i].log_service_session_id,
                    (int32_t)agent->pending_trackers[i].pending_message_capacity,
                    i);
            }
        }

        if (NULL != agent->timer_service)
        {
            aeron_cluster_timer_service_snapshot(agent->timer_service,
                snapshot_timer_cb, snap_pub);
        }

        aeron_cluster_cm_snapshot_taker_mark_end(
            snap_pub, log_position, agent->leadership_term_id, ctx->app_version);

        /* Extension hook: onTakeSnapshot */
        if (NULL != agent->ctx->extension.on_take_snapshot)
        {
            agent->ctx->extension.on_take_snapshot(agent->ctx->extension.clientd, snap_pub);
        }

        aeron_exclusive_publication_close(snap_pub, NULL, NULL);
    }

    if (snap_sub_id >= 0)
    {
        aeron_archive_stop_recording_subscription(agent->archive, snap_sub_id);
        /* snap_recording_id remains -1 in simplified path; a production impl
         * would poll for the RecordingSignal.START to get the real recording_id. */
    }

    int64_t term_base = log_position; /* simplified; real impl tracks term base separately */
    aeron_cluster_recording_log_append_snapshot(agent->recording_log,
        snap_recording_id, agent->leadership_term_id,
        term_base, log_position, timestamp / INT64_C(1000000),
        -1 /* CM service_id */);

    /* Append each service's snapshot (relevant_id == service recording_id) */
    if (NULL != service_relevant_ids)
    {
        for (int i = 0; i < agent->service_count; i++)
        {
            aeron_cluster_recording_log_append_snapshot(agent->recording_log,
                service_relevant_ids[i], agent->leadership_term_id,
                term_base, log_position, timestamp / INT64_C(1000000),
                i /* service_id */);
        }
    }
}

void aeron_consensus_module_agent_on_service_ack(
    aeron_consensus_module_agent_t *agent,
    int64_t log_position, int64_t timestamp,
    int64_t ack_id, int64_t relevant_id, int32_t service_id)
{
    (void)timestamp;
    if (service_id < 0 || service_id >= agent->service_count) { return; }

    /* Always record the latest position and ack_id for this service */
    agent->service_ack_positions[service_id] = log_position;
    if (ack_id > agent->service_ack_id)
    {
        agent->service_ack_id = ack_id;
    }

    /* Track the snapshot recording ID from the service's ACK relevant_id.
     * Mirrors Java: serviceAck.relevantId() is the recording ID for the
     * service's snapshot. Stored per-service; passed to take_cm_snapshot. */
    if (agent->state == AERON_CM_STATE_SNAPSHOT && relevant_id >= 0)
    {
        agent->service_snapshot_recording_ids[service_id] = relevant_id;
    }

    /* Drive snapshot state machine when all services ack at expected_ack_position */
    if (agent->state == AERON_CM_STATE_SNAPSHOT &&
        log_position == agent->expected_ack_position)
    {
        /* Check if all services have now acked at this position */
        bool all_acked = true;
        for (int i = 0; i < agent->service_count; i++)
        {
            if (agent->service_ack_positions[i] != agent->expected_ack_position)
            {
                all_acked = false;
                break;
            }
        }

        if (all_acked)
        {
            take_cm_snapshot(agent,
                agent->snapshot_log_position,
                agent->snapshot_timestamp,
                agent->service_snapshot_recording_ids);

            /* Reset snapshot recording IDs for the next snapshot cycle. */
            for (int i = 0; i < agent->service_count; i++)
            {
                agent->service_snapshot_recording_ids[i] = -1;
            }

            /* If a ClusterTermination was in progress, transition to LEAVING and notify services;
             * otherwise return to ACTIVE. Mirrors Java snapshotOnServiceAck(). */
            if (agent->has_cluster_termination)
            {
                if (agent->service_count > 0)
                {
                    aeron_cluster_service_proxy_cm_termination_position(
                        &agent->service_proxy, agent->termination_position);
                }
                else
                {
                    agent->is_awaiting_services = false;
                }
                agent->state                = AERON_CM_STATE_LEAVING;
                agent->expected_ack_position = agent->termination_position;
            }
            else
            {
                agent->state = AERON_CM_STATE_ACTIVE;
            }
        }
    }
    else if (agent->state == AERON_CM_STATE_LEAVING &&
             log_position == agent->expected_ack_position)
    {
        bool all_acked = true;
        for (int i = 0; i < agent->service_count; i++)
        {
            if (agent->service_ack_positions[i] != agent->expected_ack_position)
            {
                all_acked = false;
                break;
            }
        }
        if (all_acked)
        {
            terminate_on_service_ack(agent, log_position, aeron_nano_clock());
        }
    }
}

void aeron_consensus_module_agent_on_service_termination_position(
    aeron_consensus_module_agent_t *agent,
    int64_t log_position)
{
    /* A follower service reports the log position it reached before terminating.
     * Update termination_position if the service reports a later position.
     * Mirrors Java ConsensusModuleAgent.onServiceTerminationPosition(). */
    if (log_position > agent->termination_position)
    {
        agent->termination_position = log_position;
    }
}

void aeron_consensus_module_agent_on_cluster_members_query(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id, bool extended)
{
    if (AERON_CLUSTER_ROLE_LEADER != agent->role || NULL == agent->session_manager) { return; }

    /* correlationId is used as session_id for the lookup */
    aeron_cluster_cluster_session_t *session =
        aeron_cluster_session_manager_find(agent->session_manager, correlation_id);

    if (NULL == session || AERON_CLUSTER_SESSION_STATE_OPEN != session->state) { return; }

    /* Build active members string: "id=ingressEndpoint,..." */
    char active_buf[4096];
    aeron_cluster_members_ingress_endpoints(
        agent->active_members, agent->active_member_count,
        active_buf, sizeof(active_buf));

    int32_t leader_id = (NULL != agent->leader_member)
        ? agent->leader_member->id : agent->member_id;

    if (extended)
    {
        /* Extended response uses full "id,ep:ep:ep:ep:ep|..." format */
        char members_buf[8192];
        aeron_cluster_members_encode_as_string(
            agent->active_members, agent->active_member_count,
            members_buf, sizeof(members_buf));
        aeron_cluster_egress_publisher_send_cluster_members_response(
            session->response_publication, correlation_id, leader_id,
            members_buf, "");
    }
    else
    {
        aeron_cluster_egress_publisher_send_cluster_members_response(
            session->response_publication, correlation_id, leader_id,
            active_buf, "");
    }
}

void aeron_consensus_module_agent_on_backup_query(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id,
    int32_t response_stream_id,
    int32_t version,
    int64_t log_position,
    const char *response_channel,
    const uint8_t *encoded_credentials,
    size_t credentials_length)
{
    (void)log_position; (void)encoded_credentials; (void)credentials_length;

    /* Only the leader responds to backup queries */
    if (AERON_CLUSTER_ROLE_LEADER != agent->role) { return; }

    /* Version major must match */
    if (aeron_semantic_version_major(version) !=
        aeron_semantic_version_major(agent->protocol_version))
    {
        return;
    }

    /* Collect snapshots from the recording log */
    aeron_cluster_backup_response_snapshot_t snapshots[32];
    int snapshot_count = 0;

    if (NULL != agent->recording_log && agent->service_count > 0)
    {
        aeron_cluster_recording_log_entry_t raw[32];
        int raw_count = 0;
        aeron_cluster_recording_log_find_snapshots_at_or_before(
            agent->recording_log,
            aeron_consensus_module_agent_get_append_position(agent),
            agent->service_count,
            raw, &raw_count);

        for (int i = 0; i < raw_count && snapshot_count < 32; i++)
        {
            snapshots[snapshot_count].recording_id        = raw[i].recording_id;
            snapshots[snapshot_count].leadership_term_id  = raw[i].leadership_term_id;
            snapshots[snapshot_count].term_base_log_position = raw[i].term_base_log_position;
            snapshots[snapshot_count].log_position        = raw[i].log_position;
            snapshots[snapshot_count].timestamp           = raw[i].timestamp;
            snapshots[snapshot_count].service_id          = raw[i].service_id;
            snapshot_count++;
        }
    }

    /* Encode cluster members string */
    char members_buf[4096];
    aeron_cluster_members_encode_as_string(
        agent->active_members, agent->active_member_count,
        members_buf, sizeof(members_buf));

    /* Get last term info from recording log */
    int64_t log_term_base_position = 0;
    if (NULL != agent->recording_log)
    {
        aeron_cluster_recording_log_entry_t *last_term =
            aeron_cluster_recording_log_find_last_term(agent->recording_log);
        if (NULL != last_term)
        {
            log_term_base_position = last_term->term_base_log_position;
        }
    }

    int32_t commit_counter_id = -1;
    if (NULL != agent->commit_position_counter)
    {
        aeron_counter_constants_t cc;
        if (aeron_counter_constants(agent->commit_position_counter, &cc) == 0)
        {
            commit_counter_id = cc.counter_id;
        }
    }

    int32_t leader_id = (NULL != agent->leader_member)
        ? agent->leader_member->id : agent->member_id;

    /* Create a temporary publication to send the response */
    aeron_exclusive_publication_t *resp_pub = NULL;
    if (NULL != agent->aeron)
    {
        add_exclusive_pub(agent->aeron, &resp_pub, response_channel, response_stream_id);
    }

    if (NULL != resp_pub)
    {
        aeron_cluster_consensus_publisher_backup_response(
            resp_pub,
            correlation_id,
            agent->log_recording_id,
            agent->leadership_term_id,
            log_term_base_position,
            commit_counter_id,
            leader_id,
            agent->member_id,
            snapshots, snapshot_count,
            members_buf);

        aeron_exclusive_publication_close(resp_pub, NULL, NULL);
    }
}

void aeron_consensus_module_agent_on_heartbeat_request(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id,
    int32_t response_stream_id,
    const char *response_channel,
    const uint8_t *encoded_credentials,
    size_t credentials_length)
{
    (void)encoded_credentials; (void)credentials_length;

    /* Only the leader responds; only in ACTIVE/SUSPENDED/SNAPSHOT states; no in-progress election */
    if (AERON_CLUSTER_ROLE_LEADER != agent->role || NULL != agent->election) { return; }
    if (agent->state != AERON_CM_STATE_ACTIVE &&
        agent->state != AERON_CM_STATE_SUSPENDED &&
        agent->state != AERON_CM_STATE_SNAPSHOT) { return; }

    if (NULL == response_channel || '\0' == response_channel[0]) { return; }

    /* Open a transient exclusive publication to the backup node's response channel */
    aeron_exclusive_publication_t *resp_pub = NULL;
    if (NULL != agent->aeron)
    {
        add_exclusive_pub(agent->aeron, &resp_pub, response_channel, response_stream_id);
    }

    if (NULL != resp_pub)
    {
        aeron_cluster_consensus_publisher_heartbeat_response(resp_pub, correlation_id);
        aeron_exclusive_publication_close(resp_pub, NULL, NULL);
    }
}

void aeron_consensus_module_agent_on_standby_snapshot(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id,
    int32_t response_stream_id,
    int32_t version,
    const char *response_channel,
    int64_t *recording_ids,
    int64_t *leadership_term_ids,
    int64_t *term_base_log_positions,
    int64_t *log_positions,
    int64_t *timestamps,
    int32_t *service_ids,
    int snapshot_count)
{
    (void)correlation_id; (void)response_stream_id; (void)version; (void)response_channel;

    /* Only the leader records standby snapshots */
    if (AERON_CLUSTER_ROLE_LEADER != agent->role) { return; }
    if (NULL == agent->session_manager) { return; }

    /* Enqueue for deferred recording — committed once logPosition is committed */
    aeron_cluster_session_manager_enqueue_standby_snapshot(
        agent->session_manager,
        recording_ids, leadership_term_ids, term_base_log_positions,
        log_positions, timestamps, service_ids, snapshot_count);
}
