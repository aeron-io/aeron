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

#ifndef AERON_CLUSTER_ELECTION_H
#define AERON_CLUSTER_ELECTION_H

#include <stdint.h>
#include <stdbool.h>

#include "aeron_consensus_module_configuration.h"
#include "aeron_cluster_member.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct aeron_consensus_module_agent_stct aeron_consensus_module_agent_t;

typedef struct aeron_cluster_election_stct
{
    aeron_cluster_election_state_t  state;
    aeron_consensus_module_agent_t *agent;

    aeron_cluster_member_t         *this_member;
    aeron_cluster_member_t         *members;
    int                             member_count;
    aeron_cluster_member_t         *leader_member;

    /* Term tracking */
    int64_t  log_leadership_term_id;
    int64_t  log_position;
    int64_t  leadership_term_id;
    int64_t  candidate_term_id;
    int64_t  leader_recording_id;
    int64_t  append_position;
    int64_t  notified_commit_position;
    int32_t  log_session_id;

    /* Timing */
    int64_t  time_of_state_change_ns;
    int64_t  time_of_last_update_ns;
    int64_t  nomination_deadline_ns;
    int64_t  startup_canvass_timeout_ns;
    int64_t  election_timeout_ns;
    int64_t  election_status_interval_ns;
    int64_t  leader_heartbeat_timeout_ns;

    bool     is_node_startup;
    bool     is_leader_startup;
    bool     is_extended_canvass;
    bool     is_first_init;
}
aeron_cluster_election_t;

int aeron_cluster_election_create(
    aeron_cluster_election_t **election,
    aeron_consensus_module_agent_t *agent,
    aeron_cluster_member_t *this_member,
    aeron_cluster_member_t *members,
    int member_count,
    int64_t log_leadership_term_id,
    int64_t log_position,
    int64_t leadership_term_id,
    int64_t leader_recording_id,
    int64_t startup_canvass_timeout_ns,
    int64_t election_timeout_ns,
    int64_t election_status_interval_ns,
    int64_t leader_heartbeat_timeout_ns,
    bool is_node_startup);

int aeron_cluster_election_close(aeron_cluster_election_t *election);

/** Drive the election state machine.  Returns work count (> 0 if state advanced). */
int aeron_cluster_election_do_work(aeron_cluster_election_t *election, int64_t now_ns);

/** Called by ConsensuAdapter to deliver incoming messages. */
void aeron_cluster_election_on_canvass_position(aeron_cluster_election_t *election,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t leadership_term_id, int32_t follower_member_id,
    int32_t protocol_version);

void aeron_cluster_election_on_request_vote(aeron_cluster_election_t *election,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t candidate_term_id, int32_t candidate_member_id);

void aeron_cluster_election_on_vote(aeron_cluster_election_t *election,
    int64_t candidate_term_id, int64_t log_leadership_term_id,
    int64_t log_position, int32_t candidate_member_id,
    int32_t follower_member_id, bool vote);

void aeron_cluster_election_on_new_leadership_term(aeron_cluster_election_t *election,
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

void aeron_cluster_election_on_append_position(aeron_cluster_election_t *election,
    int64_t leadership_term_id, int64_t log_position,
    int32_t follower_member_id, int8_t flags);

void aeron_cluster_election_on_commit_position(aeron_cluster_election_t *election,
    int64_t leadership_term_id, int64_t log_position,
    int32_t leader_member_id);

/* Accessors */
aeron_cluster_election_state_t aeron_cluster_election_state(aeron_cluster_election_t *election);
aeron_cluster_member_t *aeron_cluster_election_leader(aeron_cluster_election_t *election);
int64_t aeron_cluster_election_leadership_term_id(aeron_cluster_election_t *election);
int64_t aeron_cluster_election_log_position(aeron_cluster_election_t *election);
int32_t aeron_cluster_election_log_session_id(aeron_cluster_election_t *election);
bool    aeron_cluster_election_is_leader_startup(aeron_cluster_election_t *election);
bool    aeron_cluster_election_is_closed(aeron_cluster_election_t *election);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_ELECTION_H */
