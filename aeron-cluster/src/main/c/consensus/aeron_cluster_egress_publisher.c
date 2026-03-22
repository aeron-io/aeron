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

#include <string.h>
#include "aeron_cluster_egress_publisher.h"

#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/sessionEvent.h"
#include "aeron_cluster_client/newLeaderEvent.h"
#include "aeron_cluster_client/challenge.h"

static uint8_t g_egress_buf[AERON_CLUSTER_EGRESS_PUBLISHER_BUFFER_LENGTH];

static bool egress_offer(aeron_exclusive_publication_t *pub, size_t length)
{
    return aeron_exclusive_publication_offer(pub, g_egress_buf, length, NULL, NULL) > 0;
}

bool aeron_cluster_egress_publisher_send_session_event(
    aeron_exclusive_publication_t *pub,
    int64_t cluster_session_id,
    int64_t correlation_id,
    int64_t leadership_term_id,
    int32_t leader_member_id,
    int32_t event_code,
    int64_t leader_heartbeat_timeout_ns,
    const char *detail,
    size_t detail_length)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_sessionEvent msg;
    if (NULL == aeron_cluster_client_sessionEvent_wrap_and_apply_header(
        &msg, (char *)g_egress_buf, 0, sizeof(g_egress_buf), &hdr)) { return false; }

    aeron_cluster_client_sessionEvent_set_clusterSessionId(&msg, cluster_session_id);
    aeron_cluster_client_sessionEvent_set_correlationId(&msg, correlation_id);
    aeron_cluster_client_sessionEvent_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_sessionEvent_set_leaderMemberId(&msg, leader_member_id);
    aeron_cluster_client_sessionEvent_set_code(&msg, (enum aeron_cluster_client_eventCode)event_code);
    aeron_cluster_client_sessionEvent_set_leaderHeartbeatTimeoutNs(&msg, leader_heartbeat_timeout_ns);

    const char *d = (detail != NULL) ? detail : "";
    uint32_t dlen = (detail != NULL) ? (uint32_t)detail_length : 0;
    aeron_cluster_client_sessionEvent_put_detail(&msg, d, dlen);

    return egress_offer(pub, aeron_cluster_client_sessionEvent_encoded_length(&msg));
}

bool aeron_cluster_egress_publisher_send_new_leader_event(
    aeron_exclusive_publication_t *pub,
    int64_t cluster_session_id,
    int64_t leadership_term_id,
    int32_t leader_member_id,
    const char *ingress_endpoints,
    size_t endpoints_length)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_newLeaderEvent msg;
    if (NULL == aeron_cluster_client_newLeaderEvent_wrap_and_apply_header(
        &msg, (char *)g_egress_buf, 0, sizeof(g_egress_buf), &hdr)) { return false; }

    aeron_cluster_client_newLeaderEvent_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_newLeaderEvent_set_clusterSessionId(&msg, cluster_session_id);
    aeron_cluster_client_newLeaderEvent_set_leaderMemberId(&msg, leader_member_id);

    const char *ep = (ingress_endpoints != NULL) ? ingress_endpoints : "";
    uint32_t eplen = (ingress_endpoints != NULL) ? (uint32_t)endpoints_length : 0;
    aeron_cluster_client_newLeaderEvent_put_ingressEndpoints(&msg, ep, eplen);

    return egress_offer(pub, aeron_cluster_client_newLeaderEvent_encoded_length(&msg));
}

bool aeron_cluster_egress_publisher_send_challenge(
    aeron_exclusive_publication_t *pub,
    int64_t correlation_id,
    int64_t cluster_session_id,
    const uint8_t *encoded_challenge,
    size_t challenge_length)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_challenge msg;
    if (NULL == aeron_cluster_client_challenge_wrap_and_apply_header(
        &msg, (char *)g_egress_buf, 0, sizeof(g_egress_buf), &hdr)) { return false; }

    aeron_cluster_client_challenge_set_correlationId(&msg, correlation_id);
    aeron_cluster_client_challenge_set_clusterSessionId(&msg, cluster_session_id);
    aeron_cluster_client_challenge_put_encodedChallenge(
        &msg, (const char *)encoded_challenge, (uint32_t)challenge_length);

    return egress_offer(pub, aeron_cluster_client_challenge_encoded_length(&msg));
}
