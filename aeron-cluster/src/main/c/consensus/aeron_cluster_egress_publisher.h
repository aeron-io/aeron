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

#ifndef AERON_CLUSTER_EGRESS_PUBLISHER_H
#define AERON_CLUSTER_EGRESS_PUBLISHER_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define AERON_CLUSTER_EGRESS_PUBLISHER_BUFFER_LENGTH (8 * 1024)

/**
 * Sends cluster→client egress messages (SessionEvent, NewLeaderEvent, Challenge).
 * Mirrors Java EgressPublisher.
 *
 * All sends go to a single egress publication (leader-side multicast/unicast).
 */

/**
 * Send a SessionEvent to a client (connect response or session event).
 */
bool aeron_cluster_egress_publisher_send_session_event(
    aeron_exclusive_publication_t *pub,
    int64_t cluster_session_id,
    int64_t correlation_id,
    int64_t leadership_term_id,
    int32_t leader_member_id,
    int32_t event_code,        /* 0=OK, 1=ERROR, 2=REDIRECT, 3=AUTH_REJECTED */
    int64_t leader_heartbeat_timeout_ns,
    const char *detail,
    size_t detail_length);

/**
 * Send a NewLeaderEvent to a client (after leader change).
 */
bool aeron_cluster_egress_publisher_send_new_leader_event(
    aeron_exclusive_publication_t *pub,
    int64_t cluster_session_id,
    int64_t leadership_term_id,
    int32_t leader_member_id,
    const char *ingress_endpoints,
    size_t endpoints_length);

/**
 * Send a Challenge to a client.
 */
bool aeron_cluster_egress_publisher_send_challenge(
    aeron_exclusive_publication_t *pub,
    int64_t correlation_id,
    int64_t cluster_session_id,
    const uint8_t *encoded_challenge,
    size_t challenge_length);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_EGRESS_PUBLISHER_H */
