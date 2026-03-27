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
#include <errno.h>

#include "aeron_cluster_controlled_egress_adapter.h"
#include "aeron_cluster_configuration.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/sessionMessageHeader.h"
#include "aeron_cluster_client/sessionEvent.h"
#include "aeron_cluster_client/newLeaderEvent.h"
#include "aeron_cluster_client/adminResponse.h"
#include "aeron_cluster_client/adminRequestType.h"
#include "aeron_cluster_client/adminResponseCode.h"
#include "aeron_cluster_client/eventCode.h"

#define EGRESS_SESSION_HDR_LEN  32

static aeron_controlled_fragment_handler_action_t on_controlled_egress_fragment(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    aeron_cluster_controlled_egress_adapter_t *adapter =
        (aeron_cluster_controlled_egress_adapter_t *)clientd;

    if (length < aeron_cluster_client_messageHeader_encoded_length())
    {
        return AERON_ACTION_CONTINUE;
    }

    struct aeron_cluster_client_messageHeader hdr;
    if (NULL == aeron_cluster_client_messageHeader_wrap(
        &hdr, (char *)buffer, 0,
        aeron_cluster_client_messageHeader_sbe_schema_version(), length))
    {
        return AERON_ACTION_CONTINUE;
    }

    const uint64_t hdr_len     = aeron_cluster_client_messageHeader_encoded_length();
    const int32_t  template_id = (int32_t)aeron_cluster_client_messageHeader_templateId(&hdr);
    aeron_cluster_egress_listener_t *l = adapter->listener;

    switch (template_id)
    {
        case AERON_CLUSTER_SESSION_MESSAGE_HEADER_TEMPLATE_ID:
        {
            struct aeron_cluster_client_sessionMessageHeader msg;
            if (NULL == aeron_cluster_client_sessionMessageHeader_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_sessionMessageHeader_sbe_block_length(),
                aeron_cluster_client_sessionMessageHeader_sbe_schema_version(), length))
            {
                break;
            }

            int64_t session_id = aeron_cluster_client_sessionMessageHeader_clusterSessionId(&msg);
            if (session_id != adapter->cluster_session_id) { break; }

            if (NULL != l->on_message && length > EGRESS_SESSION_HDR_LEN)
            {
                l->on_message(l->clientd, session_id,
                    aeron_cluster_client_sessionMessageHeader_leadershipTermId(&msg),
                    aeron_cluster_client_sessionMessageHeader_timestamp(&msg),
                    buffer + EGRESS_SESSION_HDR_LEN, length - EGRESS_SESSION_HDR_LEN,
                    header);
            }
            return AERON_ACTION_BREAK;
        }

        case AERON_CLUSTER_SESSION_EVENT_TEMPLATE_ID:
        {
            struct aeron_cluster_client_sessionEvent msg;
            if (NULL == aeron_cluster_client_sessionEvent_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_sessionEvent_sbe_block_length(),
                aeron_cluster_client_sessionEvent_sbe_schema_version(), length))
            {
                break;
            }

            if (NULL == l->on_session_event) { break; }

            enum aeron_cluster_client_eventCode code = aeron_cluster_client_eventCode_NULL_VALUE;
            aeron_cluster_client_sessionEvent_code(&msg, &code);

            const uint32_t detail_len = aeron_cluster_client_sessionEvent_detail_length(&msg);
            char detail_buf[512];
            uint32_t actual = (uint32_t)aeron_cluster_client_sessionEvent_get_detail(
                &msg, detail_buf, sizeof(detail_buf) - 1);
            detail_buf[actual] = '\0';

            l->on_session_event(l->clientd,
                aeron_cluster_client_sessionEvent_correlationId(&msg),
                aeron_cluster_client_sessionEvent_clusterSessionId(&msg),
                aeron_cluster_client_sessionEvent_leadershipTermId(&msg),
                aeron_cluster_client_sessionEvent_leaderMemberId(&msg),
                (int32_t)code,
                detail_buf, detail_len);
            return AERON_ACTION_BREAK;
        }

        case AERON_CLUSTER_NEW_LEADER_EVENT_TEMPLATE_ID:
        {
            struct aeron_cluster_client_newLeaderEvent msg;
            if (NULL == aeron_cluster_client_newLeaderEvent_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_newLeaderEvent_sbe_block_length(),
                aeron_cluster_client_newLeaderEvent_sbe_schema_version(), length))
            {
                break;
            }

            if (NULL == l->on_new_leader) { break; }

            const uint32_t ep_len = aeron_cluster_client_newLeaderEvent_ingressEndpoints_length(&msg);
            char ep_buf[512];
            uint32_t actual = (uint32_t)aeron_cluster_client_newLeaderEvent_get_ingressEndpoints(
                &msg, ep_buf, sizeof(ep_buf) - 1);
            ep_buf[actual] = '\0';

            l->on_new_leader(l->clientd,
                aeron_cluster_client_newLeaderEvent_clusterSessionId(&msg),
                aeron_cluster_client_newLeaderEvent_leadershipTermId(&msg),
                aeron_cluster_client_newLeaderEvent_leaderMemberId(&msg),
                ep_buf, ep_len);
            return AERON_ACTION_BREAK;
        }

        case AERON_CLUSTER_ADMIN_RESPONSE_TEMPLATE_ID:
        {
            struct aeron_cluster_client_adminResponse msg;
            if (NULL == aeron_cluster_client_adminResponse_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_adminResponse_sbe_block_length(),
                aeron_cluster_client_adminResponse_sbe_schema_version(), length))
            {
                break;
            }

            if (NULL == l->on_admin_response) { break; }

            const uint32_t msg_len = aeron_cluster_client_adminResponse_message_length(&msg);
            char msg_buf[512];
            uint32_t actual_msg = (uint32_t)aeron_cluster_client_adminResponse_get_message(
                &msg, msg_buf, sizeof(msg_buf) - 1);
            msg_buf[actual_msg] = '\0';

            const uint32_t pay_len = aeron_cluster_client_adminResponse_payload_length(&msg);
            const uint8_t *payload_ptr = NULL;
            if (pay_len > 0)
            {
                payload_ptr = (const uint8_t *)aeron_cluster_client_adminResponse_payload(&msg);
            }

            enum aeron_cluster_client_adminRequestType  req_type  = (enum aeron_cluster_client_adminRequestType)0;
            enum aeron_cluster_client_adminResponseCode resp_code = (enum aeron_cluster_client_adminResponseCode)0;
            aeron_cluster_client_adminResponse_requestType(&msg, &req_type);
            aeron_cluster_client_adminResponse_responseCode(&msg, &resp_code);

            l->on_admin_response(l->clientd,
                aeron_cluster_client_adminResponse_clusterSessionId(&msg),
                aeron_cluster_client_adminResponse_correlationId(&msg),
                (int32_t)req_type,
                (int32_t)resp_code,
                msg_buf, msg_len,
                payload_ptr, pay_len);
            return AERON_ACTION_BREAK;
        }

        default:
            break;
    }

    return AERON_ACTION_CONTINUE;
}

int aeron_cluster_controlled_egress_adapter_create(
    aeron_cluster_controlled_egress_adapter_t **adapter,
    aeron_subscription_t *subscription,
    aeron_cluster_egress_listener_t *listener,
    int64_t cluster_session_id)
{
    aeron_cluster_controlled_egress_adapter_t *a = NULL;
    if (aeron_alloc((void **)&a, sizeof(*a)) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to allocate ControlledEgressAdapter");
        return -1;
    }

    a->subscription        = subscription;
    a->listener            = listener;
    a->assembler           = NULL;
    a->cluster_session_id  = cluster_session_id;

    if (aeron_controlled_fragment_assembler_create(
        &a->assembler, on_controlled_egress_fragment, a) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to create ControlledEgressAdapter assembler");
        aeron_free(a);
        return -1;
    }

    *adapter = a;
    return 0;
}

int aeron_cluster_controlled_egress_adapter_poll(aeron_cluster_controlled_egress_adapter_t *adapter)
{
    return aeron_subscription_controlled_poll(
        adapter->subscription,
        aeron_controlled_fragment_assembler_handler,
        adapter->assembler,
        AERON_CLUSTER_CONTROLLED_EGRESS_ADAPTER_FRAGMENT_LIMIT);
}

void aeron_cluster_controlled_egress_adapter_close(aeron_cluster_controlled_egress_adapter_t *adapter)
{
    if (NULL == adapter) { return; }
    if (NULL != adapter->assembler)
    {
        aeron_controlled_fragment_assembler_delete(adapter->assembler);
    }
    aeron_free(adapter);
}
