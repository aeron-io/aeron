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

#include "aeron_cluster_ingress_adapter_cm.h"
#include "aeron_consensus_module_agent_fwd.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/sessionConnectRequest.h"
#include "aeron_cluster_client/sessionCloseRequest.h"
#include "aeron_cluster_client/sessionKeepAlive.h"
#include "aeron_cluster_client/challengeResponse.h"
#include "aeron_cluster_client/adminRequest.h"
#include "aeron_cluster_client/sessionMessageHeader.h"

#define INGRESS_MAX_STR 4096

static aeron_controlled_fragment_handler_action_t on_ingress_fragment(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    aeron_cluster_ingress_adapter_cm_t *adapter = (aeron_cluster_ingress_adapter_cm_t *)clientd;

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

    const uint64_t hdr_len    = aeron_cluster_client_messageHeader_encoded_length();
    const int32_t  template_id = (int32_t)aeron_cluster_client_messageHeader_templateId(&hdr);

    switch (template_id)
    {
        case 1: /* SessionMessageHeader — app message from client */
        {
            struct aeron_cluster_client_sessionMessageHeader msg;
            if (NULL == aeron_cluster_client_sessionMessageHeader_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_sessionMessageHeader_sbe_block_length(),
                aeron_cluster_client_sessionMessageHeader_sbe_schema_version(), length))
            { break; }

            aeron_consensus_module_agent_on_session_message(adapter->agent,
                aeron_cluster_client_sessionMessageHeader_leadershipTermId(&msg),
                aeron_cluster_client_sessionMessageHeader_clusterSessionId(&msg),
                buffer + 32,        /* payload starts after 32-byte session header */
                length  - 32,
                header);
            break;
        }

        case 3: /* SessionConnectRequest */
        {
            struct aeron_cluster_client_sessionConnectRequest msg;
            if (NULL == aeron_cluster_client_sessionConnectRequest_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_sessionConnectRequest_sbe_block_length(),
                aeron_cluster_client_sessionConnectRequest_sbe_schema_version(), length))
            { break; }

            char response_channel[INGRESS_MAX_STR];
            uint32_t ch_len = aeron_cluster_client_sessionConnectRequest_responseChannel_length(&msg);
            if (ch_len >= sizeof(response_channel)) { ch_len = sizeof(response_channel) - 1; }
            aeron_cluster_client_sessionConnectRequest_get_responseChannel(&msg, response_channel, ch_len);
            response_channel[ch_len] = '\0';

            uint32_t cred_len = aeron_cluster_client_sessionConnectRequest_encodedCredentials_length(&msg);
            const char *cred_ptr = aeron_cluster_client_sessionConnectRequest_encodedCredentials(&msg);

            aeron_consensus_module_agent_on_session_connect(adapter->agent,
                aeron_cluster_client_sessionConnectRequest_correlationId(&msg),
                aeron_cluster_client_sessionConnectRequest_responseStreamId(&msg),
                aeron_cluster_client_sessionConnectRequest_version(&msg),
                response_channel,
                (const uint8_t *)cred_ptr, cred_len,
                header);
            break;
        }

        case 4: /* SessionCloseRequest */
        {
            struct aeron_cluster_client_sessionCloseRequest msg;
            if (NULL == aeron_cluster_client_sessionCloseRequest_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_sessionCloseRequest_sbe_block_length(),
                aeron_cluster_client_sessionCloseRequest_sbe_schema_version(), length))
            { break; }

            aeron_consensus_module_agent_on_session_close(adapter->agent,
                aeron_cluster_client_sessionCloseRequest_leadershipTermId(&msg),
                aeron_cluster_client_sessionCloseRequest_clusterSessionId(&msg));
            break;
        }

        case 5: /* SessionKeepAlive */
        {
            struct aeron_cluster_client_sessionKeepAlive msg;
            if (NULL == aeron_cluster_client_sessionKeepAlive_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_sessionKeepAlive_sbe_block_length(),
                aeron_cluster_client_sessionKeepAlive_sbe_schema_version(), length))
            { break; }

            aeron_consensus_module_agent_on_session_keep_alive(adapter->agent,
                aeron_cluster_client_sessionKeepAlive_leadershipTermId(&msg),
                aeron_cluster_client_sessionKeepAlive_clusterSessionId(&msg),
                header);
            break;
        }

        case 8: /* ChallengeResponse */
        {
            struct aeron_cluster_client_challengeResponse msg;
            if (NULL == aeron_cluster_client_challengeResponse_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_challengeResponse_sbe_block_length(),
                aeron_cluster_client_challengeResponse_sbe_schema_version(), length))
            { break; }

            uint32_t cred_len = aeron_cluster_client_challengeResponse_encodedCredentials_length(&msg);
            const char *cred_ptr = aeron_cluster_client_challengeResponse_encodedCredentials(&msg);

            aeron_consensus_module_agent_on_ingress_challenge_response(adapter->agent,
                aeron_cluster_client_challengeResponse_correlationId(&msg),
                aeron_cluster_client_challengeResponse_clusterSessionId(&msg),
                (const uint8_t *)cred_ptr, cred_len,
                header);
            break;
        }

        case 26: /* AdminRequest */
        {
            struct aeron_cluster_client_adminRequest msg;
            if (NULL == aeron_cluster_client_adminRequest_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_adminRequest_sbe_block_length(),
                aeron_cluster_client_adminRequest_sbe_schema_version(), length))
            { break; }

            enum aeron_cluster_client_adminRequestType req_type;
            aeron_cluster_client_adminRequest_requestType(&msg, &req_type);

            uint32_t payload_len = aeron_cluster_client_adminRequest_payload_length(&msg);
            const char *payload_ptr = aeron_cluster_client_adminRequest_payload(&msg);

            aeron_consensus_module_agent_on_admin_request(adapter->agent,
                aeron_cluster_client_adminRequest_leadershipTermId(&msg),
                aeron_cluster_client_adminRequest_clusterSessionId(&msg),
                aeron_cluster_client_adminRequest_correlationId(&msg),
                (int32_t)req_type,
                (const uint8_t *)payload_ptr, payload_len,
                header);
            break;
        }

        default:
            break;
    }

    return AERON_ACTION_CONTINUE;
}

int aeron_cluster_ingress_adapter_cm_create(
    aeron_cluster_ingress_adapter_cm_t **adapter,
    aeron_subscription_t *subscription,
    aeron_consensus_module_agent_t *agent,
    int fragment_limit)
{
    aeron_cluster_ingress_adapter_cm_t *_adapter = NULL;
    if (aeron_alloc((void **)&_adapter, sizeof(aeron_cluster_ingress_adapter_cm_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate ingress adapter");
        return -1;
    }

    _adapter->subscription  = subscription;
    _adapter->agent         = agent;
    _adapter->fragment_limit = fragment_limit;

    if (aeron_controlled_fragment_assembler_create(
        &_adapter->fragment_assembler, on_ingress_fragment, _adapter) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to create assembler for ingress adapter");
        aeron_free(_adapter);
        return -1;
    }

    *adapter = _adapter;
    return 0;
}

int aeron_cluster_ingress_adapter_cm_close(aeron_cluster_ingress_adapter_cm_t *adapter)
{
    if (NULL != adapter)
    {
        aeron_controlled_fragment_assembler_delete(adapter->fragment_assembler);
        aeron_free(adapter);
    }
    return 0;
}

int aeron_cluster_ingress_adapter_cm_poll(aeron_cluster_ingress_adapter_cm_t *adapter)
{
    return aeron_subscription_controlled_poll(
        adapter->subscription,
        aeron_controlled_fragment_assembler_handler,
        adapter->fragment_assembler,
        adapter->fragment_limit);
}
