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

#ifndef AERON_CLUSTER_CONSENSUS_PUBLISHER_H
#define AERON_CLUSTER_CONSENSUS_PUBLISHER_H

#include <stdint.h>
#include <stdbool.h>
#include "aeronc.h"
#include "aeron_cluster_member.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define AERON_CLUSTER_CONSENSUS_PUBLISHER_BUFFER_LENGTH (8 * 1024)

/**
 * Encodes and sends inter-node consensus messages to one or all peers.
 * Mirrors Java's ConsensusPublisher.
 */

bool aeron_cluster_consensus_publisher_canvass_position(
    aeron_exclusive_publication_t *pub,
    int64_t log_leadership_term_id,
    int64_t log_position,
    int64_t leadership_term_id,
    int32_t follower_member_id,
    int32_t protocol_version);

bool aeron_cluster_consensus_publisher_request_vote(
    aeron_exclusive_publication_t *pub,
    int64_t log_leadership_term_id,
    int64_t log_position,
    int64_t candidate_term_id,
    int32_t candidate_member_id);

bool aeron_cluster_consensus_publisher_vote(
    aeron_exclusive_publication_t *pub,
    int64_t candidate_term_id,
    int64_t log_leadership_term_id,
    int64_t log_position,
    int32_t candidate_member_id,
    int32_t follower_member_id,
    bool vote);

bool aeron_cluster_consensus_publisher_new_leadership_term(
    aeron_exclusive_publication_t *pub,
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

bool aeron_cluster_consensus_publisher_append_position(
    aeron_exclusive_publication_t *pub,
    int64_t leadership_term_id,
    int64_t log_position,
    int32_t follower_member_id,
    int32_t flags);

bool aeron_cluster_consensus_publisher_commit_position(
    aeron_exclusive_publication_t *pub,
    int64_t leadership_term_id,
    int64_t log_position,
    int32_t leader_member_id);

bool aeron_cluster_consensus_publisher_catchup_position(
    aeron_exclusive_publication_t *pub,
    int64_t leadership_term_id,
    int64_t log_position,
    int32_t follower_member_id,
    const char *catchup_endpoint);

bool aeron_cluster_consensus_publisher_stop_catchup(
    aeron_exclusive_publication_t *pub,
    int64_t leadership_term_id,
    int32_t follower_member_id);

bool aeron_cluster_consensus_publisher_termination_position(
    aeron_exclusive_publication_t *pub,
    int64_t leadership_term_id,
    int64_t log_position);

bool aeron_cluster_consensus_publisher_termination_ack(
    aeron_exclusive_publication_t *pub,
    int64_t leadership_term_id,
    int64_t log_position,
    int32_t member_id);

/**
 * Broadcast a message to all active members (except self).
 * Uses each member's publication field.
 */
void aeron_cluster_consensus_publisher_broadcast_canvass_position(
    aeron_cluster_member_t *members, int count, int32_t self_id,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t leadership_term_id, int32_t follower_member_id,
    int32_t protocol_version);

void aeron_cluster_consensus_publisher_broadcast_request_vote(
    aeron_cluster_member_t *members, int count, int32_t self_id,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t candidate_term_id, int32_t candidate_member_id);

void aeron_cluster_consensus_publisher_broadcast_new_leadership_term(
    aeron_cluster_member_t *members, int count, int32_t self_id,
    int64_t log_leadership_term_id, int64_t next_leadership_term_id,
    int64_t next_term_base_log_position, int64_t next_log_position,
    int64_t leadership_term_id, int64_t term_base_log_position,
    int64_t log_position, int64_t leader_recording_id, int64_t timestamp,
    int32_t leader_member_id, int32_t log_session_id,
    int32_t app_version, bool is_startup);

void aeron_cluster_consensus_publisher_broadcast_commit_position(
    aeron_cluster_member_t *members, int count, int32_t self_id,
    int64_t leadership_term_id, int64_t log_position, int32_t leader_member_id);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_CONSENSUS_PUBLISHER_H */
