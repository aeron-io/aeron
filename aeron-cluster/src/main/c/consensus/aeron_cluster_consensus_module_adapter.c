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

#include "aeron_cluster_consensus_module_adapter.h"
#include "aeron_consensus_module_agent_fwd.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

/* SBE codec headers — generated into CLUSTER_C_CODEC_TARGET_DIR */
#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/sessionMessageHeader.h"
#include "aeron_cluster_client/closeSession.h"
#include "aeron_cluster_client/scheduleTimer.h"
#include "aeron_cluster_client/cancelTimer.h"
#include "aeron_cluster_client/serviceAck.h"
#include "aeron_cluster_client/serviceTerminationPosition.h"
#include "aeron_cluster_client/clusterMembersQuery.h"
#include "aeron_cluster_client/booleanType.h"

/* AeronCluster session header length: 8-byte message header + 24-byte session block = 32 */
#define CMA_SESSION_HEADER_LENGTH  (32)

static aeron_controlled_fragment_handler_action_t on_cm_fragment(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    aeron_cluster_consensus_module_adapter_t *adapter =
        (aeron_cluster_consensus_module_adapter_t *)clientd;

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

    switch (template_id)
    {
        case 1: /* SessionMessageHeader — service relaying a client message */
        {
            struct aeron_cluster_client_sessionMessageHeader msg;
            if (NULL == aeron_cluster_client_sessionMessageHeader_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_sessionMessageHeader_sbe_block_length(),
                aeron_cluster_client_sessionMessageHeader_sbe_schema_version(), length))
            { break; }

            if (length <= CMA_SESSION_HEADER_LENGTH) { break; }

            aeron_consensus_module_agent_on_service_message(adapter->agent,
                aeron_cluster_client_sessionMessageHeader_clusterSessionId(&msg),
                buffer + CMA_SESSION_HEADER_LENGTH,
                length  - CMA_SESSION_HEADER_LENGTH);
            break;
        }

        case 30: /* CloseSession */
        {
            struct aeron_cluster_client_closeSession msg;
            if (NULL == aeron_cluster_client_closeSession_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_closeSession_sbe_block_length(),
                aeron_cluster_client_closeSession_sbe_schema_version(), length))
            { break; }

            aeron_consensus_module_agent_on_service_close_session(adapter->agent,
                aeron_cluster_client_closeSession_clusterSessionId(&msg));
            break;
        }

        case 31: /* ScheduleTimer */
        {
            struct aeron_cluster_client_scheduleTimer msg;
            if (NULL == aeron_cluster_client_scheduleTimer_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_scheduleTimer_sbe_block_length(),
                aeron_cluster_client_scheduleTimer_sbe_schema_version(), length))
            { break; }

            aeron_consensus_module_agent_on_schedule_timer(adapter->agent,
                aeron_cluster_client_scheduleTimer_correlationId(&msg),
                aeron_cluster_client_scheduleTimer_deadline(&msg));
            break;
        }

        case 32: /* CancelTimer */
        {
            struct aeron_cluster_client_cancelTimer msg;
            if (NULL == aeron_cluster_client_cancelTimer_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_cancelTimer_sbe_block_length(),
                aeron_cluster_client_cancelTimer_sbe_schema_version(), length))
            { break; }

            aeron_consensus_module_agent_on_cancel_timer(adapter->agent,
                aeron_cluster_client_cancelTimer_correlationId(&msg));
            break;
        }

        case 33: /* ServiceAck — return BREAK so the agent processes one at a time */
        {
            struct aeron_cluster_client_serviceAck msg;
            if (NULL == aeron_cluster_client_serviceAck_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_serviceAck_sbe_block_length(),
                aeron_cluster_client_serviceAck_sbe_schema_version(), length))
            { break; }

            aeron_consensus_module_agent_on_service_ack(adapter->agent,
                aeron_cluster_client_serviceAck_logPosition(&msg),
                aeron_cluster_client_serviceAck_timestamp(&msg),
                aeron_cluster_client_serviceAck_ackId(&msg),
                aeron_cluster_client_serviceAck_relevantId(&msg),
                aeron_cluster_client_serviceAck_serviceId(&msg));

            return AERON_ACTION_BREAK;
        }

        case 42: /* ServiceTerminationPosition — service reports its termination log position */
        {
            struct aeron_cluster_client_serviceTerminationPosition msg;
            if (NULL == aeron_cluster_client_serviceTerminationPosition_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_serviceTerminationPosition_sbe_block_length(),
                aeron_cluster_client_serviceTerminationPosition_sbe_schema_version(), length))
            { break; }

            aeron_consensus_module_agent_on_service_termination_position(
                adapter->agent,
                aeron_cluster_client_serviceTerminationPosition_logPosition(&msg));
            break;
        }

        case 34: /* ClusterMembersQuery */
        {
            struct aeron_cluster_client_clusterMembersQuery msg;
            if (NULL == aeron_cluster_client_clusterMembersQuery_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_clusterMembersQuery_sbe_block_length(),
                aeron_cluster_client_clusterMembersQuery_sbe_schema_version(), length))
            { break; }

            /* extended field is optional (since schema version 5) */
            bool extended_val = false;
            {
                enum aeron_cluster_client_booleanType bt;
                if (aeron_cluster_client_clusterMembersQuery_extended(&msg, &bt))
                {
                    extended_val = (bt == aeron_cluster_client_booleanType_TRUE);
                }
            }
            aeron_consensus_module_agent_on_cluster_members_query(adapter->agent,
                aeron_cluster_client_clusterMembersQuery_correlationId(&msg),
                extended_val);
            break;
        }

        default:
            break;
    }

    return AERON_ACTION_CONTINUE;
}

int aeron_cluster_consensus_module_adapter_create(
    aeron_cluster_consensus_module_adapter_t **adapter,
    aeron_subscription_t *sub,
    aeron_consensus_module_agent_t *agent)
{
    aeron_cluster_consensus_module_adapter_t *a = NULL;
    if (aeron_alloc((void **)&a, sizeof(*a)) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to allocate ConsensusModuleAdapter");
        return -1;
    }

    a->subscription = sub;
    a->agent        = agent;
    a->assembler    = NULL;

    if (aeron_controlled_fragment_assembler_create(
        &a->assembler, on_cm_fragment, a) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to create fragment assembler for ConsensusModuleAdapter");
        aeron_free(a);
        return -1;
    }

    *adapter = a;
    return 0;
}

int aeron_cluster_consensus_module_adapter_poll(
    aeron_cluster_consensus_module_adapter_t *adapter)
{
    return aeron_subscription_controlled_poll(
        adapter->subscription,
        aeron_controlled_fragment_assembler_handler,
        adapter->assembler,
        AERON_CLUSTER_CONSENSUS_MODULE_ADAPTER_FRAGMENT_LIMIT);
}

void aeron_cluster_consensus_module_adapter_close(
    aeron_cluster_consensus_module_adapter_t *adapter)
{
    if (NULL == adapter) { return; }
    if (NULL != adapter->assembler)
    {
        aeron_controlled_fragment_assembler_delete(adapter->assembler);
    }
    /* subscription is owned by the agent — do not close it here */
    aeron_free(adapter);
}
