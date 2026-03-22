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
#include "aeron_cluster_log_publisher.h"

#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/sessionOpenEvent.h"
#include "aeron_cluster_client/sessionCloseEvent.h"
#include "aeron_cluster_client/sessionMessageHeader.h"
#include "aeron_cluster_client/timerEvent.h"
#include "aeron_cluster_client/clusterActionRequest.h"
#include "aeron_cluster_client/newLeadershipTermEvent.h"

int aeron_cluster_log_publisher_init(
    aeron_cluster_log_publisher_t *publisher,
    aeron_exclusive_publication_t *publication,
    int64_t leadership_term_id)
{
    publisher->publication       = publication;
    publisher->leadership_term_id = leadership_term_id;
    memset(publisher->buffer, 0, sizeof(publisher->buffer));
    return 0;
}

int64_t aeron_cluster_log_publisher_position(aeron_cluster_log_publisher_t *publisher)
{
    return aeron_exclusive_publication_position(publisher->publication);
}

static int64_t log_offer(aeron_cluster_log_publisher_t *publisher, size_t length)
{
    int64_t result;
    do
    {
        result = aeron_exclusive_publication_offer(
            publisher->publication, publisher->buffer, length, NULL, NULL);
    }
    while (AERON_PUBLICATION_BACK_PRESSURED == result ||
           AERON_PUBLICATION_ADMIN_ACTION  == result);
    return result;
}

int64_t aeron_cluster_log_publisher_append_session_open(
    aeron_cluster_log_publisher_t *publisher,
    int64_t cluster_session_id,
    int64_t correlation_id,
    int64_t timestamp,
    int32_t response_stream_id,
    const char *response_channel,
    const uint8_t *encoded_principal,
    size_t principal_length)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_sessionOpenEvent msg;
    if (NULL == aeron_cluster_client_sessionOpenEvent_wrap_and_apply_header(
        &msg, (char *)publisher->buffer, 0, sizeof(publisher->buffer), &hdr))
    {
        return AERON_PUBLICATION_ERROR;
    }

    aeron_cluster_client_sessionOpenEvent_set_leadershipTermId(&msg, publisher->leadership_term_id);
    aeron_cluster_client_sessionOpenEvent_set_correlationId(&msg, correlation_id);
    aeron_cluster_client_sessionOpenEvent_set_clusterSessionId(&msg, cluster_session_id);
    aeron_cluster_client_sessionOpenEvent_set_timestamp(&msg, timestamp);
    aeron_cluster_client_sessionOpenEvent_set_responseStreamId(&msg, response_stream_id);

    const char *ch = response_channel != NULL ? response_channel : "";
    aeron_cluster_client_sessionOpenEvent_put_responseChannel(&msg, ch, (uint32_t)strlen(ch));

    const char *pr = encoded_principal != NULL ? (const char *)encoded_principal : "";
    aeron_cluster_client_sessionOpenEvent_put_encodedPrincipal(&msg, pr, (uint32_t)principal_length);

    return log_offer(publisher, aeron_cluster_client_sessionOpenEvent_encoded_length(&msg));
}

int64_t aeron_cluster_log_publisher_append_session_close(
    aeron_cluster_log_publisher_t *publisher,
    int64_t cluster_session_id,
    int32_t close_reason,
    int64_t timestamp)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_sessionCloseEvent msg;
    if (NULL == aeron_cluster_client_sessionCloseEvent_wrap_and_apply_header(
        &msg, (char *)publisher->buffer, 0, sizeof(publisher->buffer), &hdr))
    {
        return AERON_PUBLICATION_ERROR;
    }

    aeron_cluster_client_sessionCloseEvent_set_leadershipTermId(&msg, publisher->leadership_term_id);
    aeron_cluster_client_sessionCloseEvent_set_clusterSessionId(&msg, cluster_session_id);
    aeron_cluster_client_sessionCloseEvent_set_timestamp(&msg, timestamp);
    aeron_cluster_client_sessionCloseEvent_set_closeReason(
        &msg, (enum aeron_cluster_client_closeReason)close_reason);

    return log_offer(publisher, aeron_cluster_client_sessionCloseEvent_encoded_length(&msg));
}

int64_t aeron_cluster_log_publisher_append_session_message(
    aeron_cluster_log_publisher_t *publisher,
    int64_t cluster_session_id,
    int64_t timestamp,
    const uint8_t *payload,
    size_t payload_length)
{
    uint8_t hdr_buf[32];
    struct aeron_cluster_client_messageHeader msg_hdr;
    struct aeron_cluster_client_sessionMessageHeader hdr;

    if (NULL == aeron_cluster_client_sessionMessageHeader_wrap_and_apply_header(
        &hdr, (char *)hdr_buf, 0, sizeof(hdr_buf), &msg_hdr))
    {
        return AERON_PUBLICATION_ERROR;
    }

    aeron_cluster_client_sessionMessageHeader_set_leadershipTermId(&hdr, publisher->leadership_term_id);
    aeron_cluster_client_sessionMessageHeader_set_clusterSessionId(&hdr, cluster_session_id);
    aeron_cluster_client_sessionMessageHeader_set_timestamp(&hdr, timestamp);

    aeron_iovec_t vectors[2];
    vectors[0].iov_base = hdr_buf;
    vectors[0].iov_len  = 32;
    vectors[1].iov_base = (uint8_t *)payload;
    vectors[1].iov_len  = payload_length;

    int64_t result;
    do
    {
        result = aeron_exclusive_publication_offerv(
            publisher->publication, vectors, 2, NULL, NULL);
    }
    while (AERON_PUBLICATION_BACK_PRESSURED == result ||
           AERON_PUBLICATION_ADMIN_ACTION  == result);
    return result;
}

int64_t aeron_cluster_log_publisher_append_timer_event(
    aeron_cluster_log_publisher_t *publisher,
    int64_t correlation_id,
    int64_t timestamp)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_timerEvent msg;
    if (NULL == aeron_cluster_client_timerEvent_wrap_and_apply_header(
        &msg, (char *)publisher->buffer, 0, sizeof(publisher->buffer), &hdr))
    {
        return AERON_PUBLICATION_ERROR;
    }

    aeron_cluster_client_timerEvent_set_leadershipTermId(&msg, publisher->leadership_term_id);
    aeron_cluster_client_timerEvent_set_correlationId(&msg, correlation_id);
    aeron_cluster_client_timerEvent_set_timestamp(&msg, timestamp);

    return log_offer(publisher, aeron_cluster_client_timerEvent_encoded_length(&msg));
}

int64_t aeron_cluster_log_publisher_append_cluster_action(
    aeron_cluster_log_publisher_t *publisher,
    int64_t log_position,
    int64_t timestamp,
    int32_t action,
    int32_t flags)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_clusterActionRequest msg;
    if (NULL == aeron_cluster_client_clusterActionRequest_wrap_and_apply_header(
        &msg, (char *)publisher->buffer, 0, sizeof(publisher->buffer), &hdr))
    {
        return AERON_PUBLICATION_ERROR;
    }

    aeron_cluster_client_clusterActionRequest_set_leadershipTermId(&msg, publisher->leadership_term_id);
    aeron_cluster_client_clusterActionRequest_set_logPosition(&msg, log_position);
    aeron_cluster_client_clusterActionRequest_set_timestamp(&msg, timestamp);
    aeron_cluster_client_clusterActionRequest_set_action(
        &msg, (enum aeron_cluster_client_clusterAction)action);
    aeron_cluster_client_clusterActionRequest_set_flags(&msg, flags);

    return log_offer(publisher, aeron_cluster_client_clusterActionRequest_encoded_length(&msg));
}

int64_t aeron_cluster_log_publisher_append_new_leadership_term_event(
    aeron_cluster_log_publisher_t *publisher,
    int64_t leadership_term_id,
    int64_t log_position,
    int64_t timestamp,
    int64_t term_base_log_position,
    int32_t leader_member_id,
    int32_t log_session_id,
    int32_t app_version)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_newLeadershipTermEvent msg;
    if (NULL == aeron_cluster_client_newLeadershipTermEvent_wrap_and_apply_header(
        &msg, (char *)publisher->buffer, 0, sizeof(publisher->buffer), &hdr))
    {
        return AERON_PUBLICATION_ERROR;
    }

    aeron_cluster_client_newLeadershipTermEvent_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_newLeadershipTermEvent_set_logPosition(&msg, log_position);
    aeron_cluster_client_newLeadershipTermEvent_set_timestamp(&msg, timestamp);
    aeron_cluster_client_newLeadershipTermEvent_set_termBaseLogPosition(&msg, term_base_log_position);
    aeron_cluster_client_newLeadershipTermEvent_set_leaderMemberId(&msg, leader_member_id);
    aeron_cluster_client_newLeadershipTermEvent_set_logSessionId(&msg, log_session_id);
    aeron_cluster_client_newLeadershipTermEvent_set_appVersion(&msg, app_version);

    return log_offer(publisher, aeron_cluster_client_newLeadershipTermEvent_encoded_length(&msg));
}
