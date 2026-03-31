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

#include "aeron_cluster_consensus_module_proxy.h"

#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/scheduleTimer.h"
#include "aeron_cluster_client/cancelTimer.h"
#include "aeron_cluster_client/serviceAck.h"
#include "aeron_cluster_client/closeSession.h"
#include "aeron_cluster_client/sessionMessageHeader.h"

#define SESSION_HEADER_LENGTH 32

static int64_t proxy_offer(aeron_cluster_consensus_module_proxy_t *proxy,
                            const uint8_t *buffer, size_t length)
{
    int64_t result = AERON_PUBLICATION_CLOSED;
    for (int i = 0; i < proxy->retry_attempts; i++)
    {
        result = aeron_exclusive_publication_offer(
            proxy->publication, buffer, length, NULL, NULL);
        if (result > 0 || AERON_PUBLICATION_CLOSED == result ||
            AERON_PUBLICATION_MAX_POSITION_EXCEEDED == result)
        {
            break;
        }
    }
    return result;
}

int aeron_cluster_consensus_module_proxy_init(
    aeron_cluster_consensus_module_proxy_t *proxy,
    aeron_exclusive_publication_t *publication,
    int retry_attempts,
    int32_t service_id)
{
    proxy->publication     = publication;
    proxy->retry_attempts  = retry_attempts;
    proxy->service_id      = service_id;
    memset(proxy->buffer, 0, sizeof(proxy->buffer));
    return 0;
}

bool aeron_cluster_consensus_module_proxy_schedule_timer(
    aeron_cluster_consensus_module_proxy_t *proxy,
    int64_t correlation_id,
    int64_t deadline)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_scheduleTimer msg;

    if (NULL == aeron_cluster_client_scheduleTimer_wrap_and_apply_header(
        &msg, (char *)proxy->buffer, 0, sizeof(proxy->buffer), &hdr))
    {
        return false;
    }

    aeron_cluster_client_scheduleTimer_set_correlationId(&msg, correlation_id);
    aeron_cluster_client_scheduleTimer_set_deadline(&msg, deadline);

    return proxy_offer(proxy, proxy->buffer,
        aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_scheduleTimer_encoded_length(&msg)) > 0;
}

bool aeron_cluster_consensus_module_proxy_cancel_timer(
    aeron_cluster_consensus_module_proxy_t *proxy,
    int64_t correlation_id)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_cancelTimer msg;

    if (NULL == aeron_cluster_client_cancelTimer_wrap_and_apply_header(
        &msg, (char *)proxy->buffer, 0, sizeof(proxy->buffer), &hdr))
    {
        return false;
    }

    aeron_cluster_client_cancelTimer_set_correlationId(&msg, correlation_id);

    return proxy_offer(proxy, proxy->buffer,
        aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_cancelTimer_encoded_length(&msg)) > 0;
}

bool aeron_cluster_consensus_module_proxy_ack(
    aeron_cluster_consensus_module_proxy_t *proxy,
    int64_t log_position,
    int64_t timestamp,
    int64_t ack_id,
    int64_t relevant_id,
    int32_t service_id)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_serviceAck msg;

    if (NULL == aeron_cluster_client_serviceAck_wrap_and_apply_header(
        &msg, (char *)proxy->buffer, 0, sizeof(proxy->buffer), &hdr))
    {
        return false;
    }

    aeron_cluster_client_serviceAck_set_logPosition(&msg, log_position);
    aeron_cluster_client_serviceAck_set_timestamp(&msg, timestamp);
    aeron_cluster_client_serviceAck_set_ackId(&msg, ack_id);
    aeron_cluster_client_serviceAck_set_relevantId(&msg, relevant_id);
    aeron_cluster_client_serviceAck_set_serviceId(&msg, service_id);

    return proxy_offer(proxy, proxy->buffer,
        aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_serviceAck_encoded_length(&msg)) > 0;
}

bool aeron_cluster_consensus_module_proxy_close_session(
    aeron_cluster_consensus_module_proxy_t *proxy,
    int64_t cluster_session_id)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_closeSession msg;

    if (NULL == aeron_cluster_client_closeSession_wrap_and_apply_header(
        &msg, (char *)proxy->buffer, 0, sizeof(proxy->buffer), &hdr))
    {
        return false;
    }

    aeron_cluster_client_closeSession_set_clusterSessionId(&msg, cluster_session_id);

    return proxy_offer(proxy, proxy->buffer,
        aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_closeSession_encoded_length(&msg)) > 0;
}

int64_t aeron_cluster_consensus_module_proxy_offer(
    aeron_cluster_consensus_module_proxy_t *proxy,
    int64_t cluster_session_id,
    int64_t leadership_term_id,
    const uint8_t *buffer,
    size_t length)
{
    uint8_t hdr_buf[SESSION_HEADER_LENGTH];
    struct aeron_cluster_client_messageHeader msg_hdr;
    struct aeron_cluster_client_sessionMessageHeader hdr;

    if (NULL == aeron_cluster_client_sessionMessageHeader_wrap_and_apply_header(
        &hdr, (char *)hdr_buf, 0, sizeof(hdr_buf), &msg_hdr))
    {
        return AERON_PUBLICATION_ERROR;
    }

    aeron_cluster_client_sessionMessageHeader_set_leadershipTermId(&hdr, leadership_term_id);
    aeron_cluster_client_sessionMessageHeader_set_clusterSessionId(&hdr, cluster_session_id);
    aeron_cluster_client_sessionMessageHeader_set_timestamp(&hdr, 0);

    aeron_iovec_t vectors[2];
    vectors[0].iov_base = hdr_buf;
    vectors[0].iov_len  = SESSION_HEADER_LENGTH;
    vectors[1].iov_base = (uint8_t *)buffer;
    vectors[1].iov_len  = length;

    int64_t result = AERON_PUBLICATION_CLOSED;
    for (int i = 0; i < proxy->retry_attempts; i++)
    {
        result = aeron_exclusive_publication_offerv(
            proxy->publication, vectors, 2, NULL, NULL);
        if (result > 0 || AERON_PUBLICATION_CLOSED == result ||
            AERON_PUBLICATION_MAX_POSITION_EXCEEDED == result)
        {
            break;
        }
    }
    return result;
}
