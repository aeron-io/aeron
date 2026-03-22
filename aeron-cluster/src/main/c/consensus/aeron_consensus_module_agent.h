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

#ifndef AERON_CONSENSUS_MODULE_AGENT_H
#define AERON_CONSENSUS_MODULE_AGENT_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#include "aeronc.h"
#include "aeron_consensus_module_configuration.h"
#include "aeron_cluster_member.h"
#include "aeron_cluster_recording_log.h"
#include "aeron_cluster_timer_service.h"
#include "aeron_cluster_session_manager.h"
#include "aeron_cluster_service_proxy_cm.h"
#include "aeron_cluster_consensus_publisher.h"
#include "aeron_cluster_egress_publisher.h"
#include "aeron_cluster_log_publisher.h"
#include "aeron_cluster_consensus_adapter.h"
#include "aeron_cluster_ingress_adapter_cm.h"
#include "aeron_cluster_cm_snapshot_taker.h"
#include "aeron_cluster_election.h"
#include "aeron_cluster_pending_message_tracker.h"
#include "aeron_consensus_module_agent_fwd.h"
#include "aeron_archive.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct aeron_cm_context_stct aeron_cm_context_t;

struct aeron_consensus_module_agent_stct
{
    aeron_cm_context_t              *ctx;
    aeron_t                         *aeron;

    /* Identity */
    int32_t  member_id;
    aeron_cm_state_t  state;
    aeron_cluster_role_t role;

    /* Members */
    aeron_cluster_member_t          *active_members;
    int                              active_member_count;
    aeron_cluster_member_t          *this_member;
    aeron_cluster_member_t          *leader_member;
    int64_t                         *ranked_positions;

    /* Log / term tracking */
    int64_t  leadership_term_id;
    int64_t  expected_ack_position;
    int64_t  service_ack_id;
    int64_t  last_append_position;
    int64_t  notified_commit_position;
    int64_t  termination_position;
    int64_t  log_subscription_id;
    int64_t  log_recording_id;
    int32_t  log_session_id_cache;   /* session_id of the log publication */
    int32_t  app_version;
    int32_t  protocol_version;

    /* Timing */
    int64_t  leader_heartbeat_interval_ns;
    int64_t  leader_heartbeat_timeout_ns;
    int64_t  session_timeout_ns;
    int64_t  time_of_last_log_update_ns;
    int64_t  time_of_last_append_position_send_ns;
    int64_t  slow_tick_deadline_ns;

    /* Subscriptions and publications */
    aeron_subscription_t            *ingress_subscription;
    aeron_subscription_t            *consensus_subscription;
    aeron_exclusive_publication_t   *log_publication;      /* leader only */
    aeron_exclusive_publication_t   *service_pub;          /* CM→service */
    aeron_subscription_t            *service_sub;          /* service→CM */

    /* Components */
    aeron_cluster_ingress_adapter_cm_t  *ingress_adapter;
    aeron_cluster_consensus_adapter_t   *consensus_adapter;
    aeron_cluster_log_publisher_t        log_publisher;
    aeron_cluster_service_proxy_cm_t     service_proxy;
    aeron_cluster_session_manager_t     *session_manager;
    aeron_cluster_timer_service_t       *timer_service;
    aeron_cluster_recording_log_t       *recording_log;
    aeron_cluster_election_t            *election;

    /* Counters */
    aeron_counter_t                 *commit_position_counter;
    aeron_counter_t                 *cluster_role_counter;
    aeron_counter_t                 *module_state_counter;

    /* Service ACK tracking */
    int64_t                         *service_ack_positions;  /* [service_count] */
    int                              service_count;

    /* Archive (IPC) — log recording, snapshot, follower catchup */
    aeron_archive_t                 *archive;

    /* Pending message trackers (one per service) */
    aeron_cluster_pending_message_tracker_t *pending_trackers; /* [service_count] */
};

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */
int aeron_consensus_module_agent_create(
    aeron_consensus_module_agent_t **agent,
    aeron_cm_context_t *ctx);

int aeron_consensus_module_agent_close(aeron_consensus_module_agent_t *agent);
int aeron_consensus_module_agent_on_start(aeron_consensus_module_agent_t *agent);
int aeron_consensus_module_agent_do_work(aeron_consensus_module_agent_t *agent, int64_t now_ns);

/* -----------------------------------------------------------------------
 * Accessor helpers used by election and adapters
 * ----------------------------------------------------------------------- */
int32_t aeron_consensus_module_agent_get_protocol_version(aeron_consensus_module_agent_t *agent);
int32_t aeron_consensus_module_agent_get_app_version(aeron_consensus_module_agent_t *agent);
int64_t aeron_consensus_module_agent_get_append_position(aeron_consensus_module_agent_t *agent);
int64_t aeron_consensus_module_agent_get_log_recording_id(aeron_consensus_module_agent_t *agent);

/* -----------------------------------------------------------------------
 * Election callbacks (called by aeron_cluster_election_t)
 * ----------------------------------------------------------------------- */
void aeron_consensus_module_agent_on_election_state_change(
    aeron_consensus_module_agent_t *agent,
    aeron_cluster_election_state_t new_state,
    int64_t now_ns);

void aeron_consensus_module_agent_on_election_complete(
    aeron_consensus_module_agent_t *agent,
    aeron_cluster_member_t *leader,
    int64_t now_ns);

void aeron_consensus_module_agent_begin_new_leadership_term(
    aeron_consensus_module_agent_t *agent,
    int64_t log_leadership_term_id,
    int64_t new_term_id,
    int64_t log_position,
    int64_t timestamp,
    bool is_startup);

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
    bool is_startup);

void aeron_consensus_module_agent_on_replay_new_leadership_term_event(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id,
    int64_t log_position,
    int64_t timestamp,
    int64_t term_base_log_position,
    int32_t leader_member_id,
    int32_t log_session_id,
    int32_t app_version);

void aeron_consensus_module_agent_notify_commit_position(
    aeron_consensus_module_agent_t *agent, int64_t commit_position);

/* -----------------------------------------------------------------------
 * Consensus message callbacks (from ConsensusAdapter)
 * already declared in aeron_consensus_module_agent_fwd.h — bodies here
 * ----------------------------------------------------------------------- */

/* -----------------------------------------------------------------------
 * Ingress callbacks (from IngressAdapter)
 * already declared in aeron_consensus_module_agent_fwd.h
 * ----------------------------------------------------------------------- */

#ifdef __cplusplus
}
#endif

#endif /* AERON_CONSENSUS_MODULE_AGENT_H */
