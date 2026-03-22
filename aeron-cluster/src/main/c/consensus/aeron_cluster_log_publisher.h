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

#ifndef AERON_CLUSTER_LOG_PUBLISHER_H
#define AERON_CLUSTER_LOG_PUBLISHER_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define AERON_CLUSTER_LOG_PUBLISHER_BUFFER_LENGTH (64 * 1024)

/**
 * Used by the leader to append events to the cluster log.
 * The log is an exclusive publication that is also recorded by AeronArchive.
 */
typedef struct aeron_cluster_log_publisher_stct
{
    aeron_exclusive_publication_t *publication;
    int64_t                        leadership_term_id;
    uint8_t                        buffer[AERON_CLUSTER_LOG_PUBLISHER_BUFFER_LENGTH];
}
aeron_cluster_log_publisher_t;

int aeron_cluster_log_publisher_init(
    aeron_cluster_log_publisher_t *publisher,
    aeron_exclusive_publication_t *publication,
    int64_t leadership_term_id);

int64_t aeron_cluster_log_publisher_position(aeron_cluster_log_publisher_t *publisher);

/**
 * Append a SessionOpenEvent to the log.
 */
int64_t aeron_cluster_log_publisher_append_session_open(
    aeron_cluster_log_publisher_t *publisher,
    int64_t cluster_session_id,
    int64_t correlation_id,
    int64_t timestamp,
    int32_t response_stream_id,
    const char *response_channel,
    const uint8_t *encoded_principal,
    size_t principal_length);

/**
 * Append a SessionCloseEvent to the log.
 */
int64_t aeron_cluster_log_publisher_append_session_close(
    aeron_cluster_log_publisher_t *publisher,
    int64_t cluster_session_id,
    int32_t close_reason,
    int64_t timestamp);

/**
 * Append an app message (SessionMessageHeader + payload) to the log.
 */
int64_t aeron_cluster_log_publisher_append_session_message(
    aeron_cluster_log_publisher_t *publisher,
    int64_t cluster_session_id,
    int64_t timestamp,
    const uint8_t *payload,
    size_t payload_length);

/**
 * Append a TimerEvent to the log.
 */
int64_t aeron_cluster_log_publisher_append_timer_event(
    aeron_cluster_log_publisher_t *publisher,
    int64_t correlation_id,
    int64_t timestamp);

/**
 * Append a ClusterActionRequest (e.g. SNAPSHOT) to the log.
 */
int64_t aeron_cluster_log_publisher_append_cluster_action(
    aeron_cluster_log_publisher_t *publisher,
    int64_t log_position,
    int64_t timestamp,
    int32_t action,
    int32_t flags);

/**
 * Append a NewLeadershipTermEvent to the log.
 */
int64_t aeron_cluster_log_publisher_append_new_leadership_term_event(
    aeron_cluster_log_publisher_t *publisher,
    int64_t leadership_term_id,
    int64_t log_position,
    int64_t timestamp,
    int64_t term_base_log_position,
    int32_t leader_member_id,
    int32_t log_session_id,
    int32_t app_version);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_LOG_PUBLISHER_H */
