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

#ifndef AERON_CONSENSUS_MODULE_AGENT_FWD_H
#define AERON_CONSENSUS_MODULE_AGENT_FWD_H

/* Forward declarations of agent callback functions used by adapters.
 * This avoids a circular include with the full agent header.
 * The actual definitions live in aeron_consensus_module_agent.c. */

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct aeron_consensus_module_agent_stct aeron_consensus_module_agent_t;

/* Consensus adapter callbacks */
void aeron_consensus_module_agent_on_canvass_position(
    aeron_consensus_module_agent_t *agent,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t leadership_term_id, int32_t follower_member_id,
    int32_t protocol_version);

void aeron_consensus_module_agent_on_request_vote(
    aeron_consensus_module_agent_t *agent,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t candidate_term_id, int32_t candidate_member_id);

void aeron_consensus_module_agent_on_vote(
    aeron_consensus_module_agent_t *agent,
    int64_t candidate_term_id, int64_t log_leadership_term_id,
    int64_t log_position, int32_t candidate_member_id,
    int32_t follower_member_id, bool vote);

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
    bool is_startup);

void aeron_consensus_module_agent_on_append_position(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position,
    int32_t follower_member_id, int8_t flags);

void aeron_consensus_module_agent_on_commit_position(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position,
    int32_t leader_member_id);

void aeron_consensus_module_agent_on_catchup_position(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position,
    int32_t follower_member_id, const char *catchup_endpoint);

void aeron_consensus_module_agent_on_stop_catchup(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int32_t follower_member_id);

void aeron_consensus_module_agent_on_termination_position(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position);

void aeron_consensus_module_agent_on_termination_ack(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position, int32_t member_id);

/* Log replay callbacks (dispatched by LogAdapter during log replay) */
void aeron_consensus_module_agent_on_replay_session_message(
    aeron_consensus_module_agent_t *agent,
    int64_t cluster_session_id, int64_t timestamp);

void aeron_consensus_module_agent_on_replay_timer_event(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id);

void aeron_consensus_module_agent_on_replay_session_open(
    aeron_consensus_module_agent_t *agent,
    int64_t log_position,
    int64_t correlation_id, int64_t cluster_session_id, int64_t timestamp,
    int32_t response_stream_id, const char *response_channel);

void aeron_consensus_module_agent_on_replay_session_close(
    aeron_consensus_module_agent_t *agent,
    int64_t cluster_session_id, int32_t close_reason);

void aeron_consensus_module_agent_on_replay_cluster_action(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position,
    int64_t timestamp, int32_t action, int32_t flags);

void aeron_consensus_module_agent_on_replay_new_leadership_term_event(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position,
    int64_t timestamp, int64_t term_base_log_position,
    int32_t time_unit, int32_t app_version);

/* Ingress adapter callbacks */
void aeron_consensus_module_agent_on_session_connect(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id, int32_t response_stream_id,
    int32_t version, const char *response_channel,
    const uint8_t *encoded_credentials, size_t credentials_length,
    aeron_header_t *header);

void aeron_consensus_module_agent_on_session_close(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t cluster_session_id);

void aeron_consensus_module_agent_on_session_keep_alive(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t cluster_session_id,
    aeron_header_t *header);

void aeron_consensus_module_agent_on_session_message(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t cluster_session_id,
    const uint8_t *payload, size_t payload_length,
    aeron_header_t *header);

void aeron_consensus_module_agent_on_ingress_challenge_response(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id, int64_t cluster_session_id,
    const uint8_t *encoded_credentials, size_t credentials_length,
    aeron_header_t *header);

void aeron_consensus_module_agent_on_consensus_challenge_response(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id, int64_t cluster_session_id,
    const uint8_t *encoded_credentials, size_t credentials_length);

void aeron_consensus_module_agent_on_admin_request(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id, int64_t cluster_session_id,
    int64_t correlation_id, int32_t request_type,
    const uint8_t *payload, size_t payload_length,
    aeron_header_t *header);

/* ConsensusModuleAdapter callbacks (service → CM channel) */

/** Service relayed a client session message to the CM. */
void aeron_consensus_module_agent_on_service_message(
    aeron_consensus_module_agent_t *agent,
    int64_t cluster_session_id,
    const uint8_t *payload, size_t payload_length);

/** Service requests that the CM close a cluster session. */
void aeron_consensus_module_agent_on_service_close_session(
    aeron_consensus_module_agent_t *agent,
    int64_t cluster_session_id);

/** Service requests a timer to fire at the given deadline (ns). */
void aeron_consensus_module_agent_on_schedule_timer(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id, int64_t deadline_ns);

/** Service cancels a previously scheduled timer. */
void aeron_consensus_module_agent_on_cancel_timer(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id);

/** Service acknowledges a CM command (snapshot, recovery, replay). */
void aeron_consensus_module_agent_on_service_ack(
    aeron_consensus_module_agent_t *agent,
    int64_t log_position, int64_t timestamp,
    int64_t ack_id, int64_t relevant_id, int32_t service_id);

/**
 * Service reports its own termination position (sent via ServiceTerminationPosition SBE, template 42).
 * Mirrors Java ConsensusModuleAgent.onServiceTerminationPosition(logPosition).
 * Used during follower termination to confirm the service has processed up to the
 * requested position. Updates termination_position if larger.
 */
void aeron_consensus_module_agent_on_service_termination_position(
    aeron_consensus_module_agent_t *agent,
    int64_t log_position);

/** Service requests current cluster members information. */
void aeron_consensus_module_agent_on_cluster_members_query(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id, bool extended);

/**
 * Received a BackupQuery from a backup node (on consensus channel).
 * Only the leader responds.
 */
void aeron_consensus_module_agent_on_backup_query(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id,
    int32_t response_stream_id,
    int32_t version,
    int64_t log_position,
    const char *response_channel,
    const uint8_t *encoded_credentials,
    size_t credentials_length);

/**
 * Received a HeartbeatRequest from a backup/standby node on the consensus channel.
 * The leader sends a HeartbeatResponse to confirm the node is still connected.
 * Mirrors Java ConsensusModuleAgent.onHeartbeatRequest().
 */
void aeron_consensus_module_agent_on_heartbeat_request(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id,
    int32_t response_stream_id,
    const char *response_channel,
    const uint8_t *encoded_credentials,
    size_t credentials_length);

/**
 * Received a StandbySnapshot notification from a backup node.
 * The leader appends each snapshot entry into the RecordingLog.
 */
void aeron_consensus_module_agent_on_standby_snapshot(
    aeron_consensus_module_agent_t *agent,
    int64_t correlation_id,
    int32_t response_stream_id,
    int32_t version,
    const char *response_channel,
    /* each entry in the repeating group snapshots[] */
    int64_t *recording_ids,
    int64_t *leadership_term_ids,
    int64_t *term_base_log_positions,
    int64_t *log_positions,
    int64_t *timestamps,
    int32_t *service_ids,
    int snapshot_count);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CONSENSUS_MODULE_AGENT_FWD_H */
