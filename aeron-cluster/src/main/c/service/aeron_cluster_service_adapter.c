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

#include <errno.h>

#include "aeron_cluster_service_adapter.h"
#include "aeron_clustered_service_agent.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

#include "uri/aeron_uri.h"
#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/joinLog.h"
#include "aeron_cluster_client/serviceTerminationPosition.h"
#include "aeron_cluster_client/requestServiceAck.h"

#define SERVICE_ADAPTER_FRAGMENT_LIMIT 1

static void on_fragment(
    void *clientd,
    const uint8_t *buffer,
    size_t length,
    aeron_header_t *header)
{
    aeron_cluster_service_adapter_t *adapter = (aeron_cluster_service_adapter_t *)clientd;

    if (length < aeron_cluster_client_messageHeader_encoded_length())
    {
        return;
    }

    struct aeron_cluster_client_messageHeader hdr;
    if (NULL == aeron_cluster_client_messageHeader_wrap(
        &hdr, (char *)buffer, 0,
        aeron_cluster_client_messageHeader_sbe_schema_version(), length))
    {
        return;
    }

    const uint64_t hdr_len = aeron_cluster_client_messageHeader_encoded_length();
    const int32_t template_id = (int32_t)aeron_cluster_client_messageHeader_templateId(&hdr);

    switch (template_id)
    {
        case 40: /* JoinLog */
        {
            struct aeron_cluster_client_joinLog msg;
            if (NULL == aeron_cluster_client_joinLog_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_joinLog_sbe_block_length(),
                aeron_cluster_client_joinLog_sbe_schema_version(),
                length))
            {
                return;
            }

            enum aeron_cluster_client_booleanType is_startup_bt;
            bool is_startup = aeron_cluster_client_joinLog_isStartup(&msg, &is_startup_bt) &&
                              is_startup_bt == aeron_cluster_client_booleanType_TRUE;

            char log_channel[AERON_URI_MAX_LENGTH];
            uint32_t ch_len = aeron_cluster_client_joinLog_logChannel_length(&msg);
            if (ch_len >= sizeof(log_channel))
            {
                ch_len = sizeof(log_channel) - 1;
            }
            aeron_cluster_client_joinLog_get_logChannel(&msg, log_channel, ch_len);
            log_channel[ch_len] = '\0';

            aeron_clustered_service_agent_on_join_log(
                adapter->agent,
                aeron_cluster_client_joinLog_logPosition(&msg),
                aeron_cluster_client_joinLog_maxLogPosition(&msg),
                aeron_cluster_client_joinLog_memberId(&msg),
                aeron_cluster_client_joinLog_logSessionId(&msg),
                aeron_cluster_client_joinLog_logStreamId(&msg),
                is_startup,
                (aeron_cluster_role_t)aeron_cluster_client_joinLog_role(&msg),
                log_channel);
            break;
        }

        case 42: /* ServiceTerminationPosition */
        {
            struct aeron_cluster_client_serviceTerminationPosition msg;
            if (NULL == aeron_cluster_client_serviceTerminationPosition_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_serviceTerminationPosition_sbe_block_length(),
                aeron_cluster_client_serviceTerminationPosition_sbe_schema_version(),
                length))
            {
                return;
            }

            aeron_clustered_service_agent_on_service_termination_position(
                adapter->agent,
                aeron_cluster_client_serviceTerminationPosition_logPosition(&msg));
            break;
        }

        case 108: /* requestServiceAck */
        {
            struct aeron_cluster_client_requestServiceAck msg;
            if (NULL == aeron_cluster_client_requestServiceAck_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_requestServiceAck_sbe_block_length(),
                aeron_cluster_client_requestServiceAck_sbe_schema_version(),
                length))
            {
                return;
            }

            aeron_clustered_service_agent_on_request_service_ack(
                adapter->agent,
                aeron_cluster_client_requestServiceAck_logPosition(&msg));
            break;
        }

        default:
            break;
    }
}

int aeron_cluster_service_adapter_create(
    aeron_cluster_service_adapter_t **adapter,
    aeron_subscription_t *subscription,
    aeron_clustered_service_agent_t *agent)
{
    aeron_cluster_service_adapter_t *_adapter = NULL;

    if (aeron_alloc((void **)&_adapter, sizeof(aeron_cluster_service_adapter_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate service adapter");
        return -1;
    }

    _adapter->subscription = subscription;
    _adapter->agent        = agent;

    if (aeron_fragment_assembler_create(&_adapter->fragment_assembler, on_fragment, _adapter) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to create fragment assembler for service adapter");
        aeron_free(_adapter);
        return -1;
    }

    *adapter = _adapter;
    return 0;
}

int aeron_cluster_service_adapter_close(aeron_cluster_service_adapter_t *adapter)
{
    if (NULL != adapter)
    {
        aeron_fragment_assembler_delete(adapter->fragment_assembler);
        aeron_free(adapter);
    }
    return 0;
}

int aeron_cluster_service_adapter_poll(aeron_cluster_service_adapter_t *adapter)
{
    return aeron_subscription_poll(
        adapter->subscription,
        aeron_fragment_assembler_handler,
        adapter->fragment_assembler,
        SERVICE_ADAPTER_FRAGMENT_LIMIT);
}
