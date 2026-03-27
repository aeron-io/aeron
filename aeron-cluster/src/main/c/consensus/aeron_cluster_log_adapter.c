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

#include "aeron_cluster_log_adapter.h"
#include "aeron_consensus_module_agent_fwd.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/sessionMessageHeader.h"
#include "aeron_cluster_client/timerEvent.h"
#include "aeron_cluster_client/sessionOpenEvent.h"
#include "aeron_cluster_client/sessionCloseEvent.h"
#include "aeron_cluster_client/clusterActionRequest.h"
#include "aeron_cluster_client/newLeadershipTermEvent.h"

/* Flags from Java: ConsensusModule.CLUSTER_ACTION_FLAGS_DEFAULT = 0 */
#define CLUSTER_ACTION_FLAGS_DEFAULT (0)

static aeron_controlled_fragment_handler_action_t on_message(
    aeron_cluster_log_adapter_t *adapter,
    const uint8_t *buffer,
    size_t length,
    aeron_header_t *header)
{
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
    const uint16_t schema_id  = aeron_cluster_client_messageHeader_schemaId(&hdr);
    const int32_t  template_id = (int32_t)aeron_cluster_client_messageHeader_templateId(&hdr);
    const uint16_t act_version = aeron_cluster_client_messageHeader_version(&hdr);
    const uint16_t act_blen   = aeron_cluster_client_messageHeader_blockLength(&hdr);

    /* If schema doesn't match, treat as extension message (no-op for now) */
    if (schema_id != aeron_cluster_client_messageHeader_sbe_schema_id())
    {
        return AERON_ACTION_CONTINUE;
    }

    switch (template_id)
    {
        case 1: /* SessionMessageHeader */
        {
            struct aeron_cluster_client_sessionMessageHeader msg;
            if (NULL == aeron_cluster_client_sessionMessageHeader_wrap_for_decode(
                &msg, (char *)buffer, hdr_len, act_blen, act_version, length))
            {
                break;
            }
            aeron_consensus_module_agent_on_replay_session_message(
                adapter->agent,
                aeron_cluster_client_sessionMessageHeader_clusterSessionId(&msg),
                aeron_cluster_client_sessionMessageHeader_timestamp(&msg));
            return AERON_ACTION_CONTINUE;
        }

        case 20: /* TimerEvent */
        {
            struct aeron_cluster_client_timerEvent msg;
            if (NULL == aeron_cluster_client_timerEvent_wrap_for_decode(
                &msg, (char *)buffer, hdr_len, act_blen, act_version, length))
            {
                break;
            }
            aeron_consensus_module_agent_on_replay_timer_event(
                adapter->agent,
                aeron_cluster_client_timerEvent_correlationId(&msg));
            break;
        }

        case 21: /* SessionOpenEvent */
        {
            struct aeron_cluster_client_sessionOpenEvent msg;
            if (NULL == aeron_cluster_client_sessionOpenEvent_wrap_for_decode(
                &msg, (char *)buffer, hdr_len, act_blen, act_version, length))
            {
                break;
            }

            /* responseChannel is a variable-length string at the end */
            uint32_t chan_len = aeron_cluster_client_sessionOpenEvent_responseChannel_length(&msg);
            const char *chan  = aeron_cluster_client_sessionOpenEvent_responseChannel(&msg);

            /* Build a null-terminated copy on the stack */
            char channel[512];
            if (chan_len >= sizeof(channel))
            {
                chan_len = sizeof(channel) - 1;
            }
            memcpy(channel, chan, chan_len);
            channel[chan_len] = '\0';

            int64_t log_position = aeron_image_position(adapter->image);

            aeron_consensus_module_agent_on_replay_session_open(
                adapter->agent,
                log_position,
                aeron_cluster_client_sessionOpenEvent_correlationId(&msg),
                aeron_cluster_client_sessionOpenEvent_clusterSessionId(&msg),
                aeron_cluster_client_sessionOpenEvent_timestamp(&msg),
                aeron_cluster_client_sessionOpenEvent_responseStreamId(&msg),
                channel);
            break;
        }

        case 22: /* SessionCloseEvent */
        {
            struct aeron_cluster_client_sessionCloseEvent msg;
            if (NULL == aeron_cluster_client_sessionCloseEvent_wrap_for_decode(
                &msg, (char *)buffer, hdr_len, act_blen, act_version, length))
            {
                break;
            }

            enum aeron_cluster_client_closeReason close_reason_enum;
            int32_t close_reason = 0;
            if (aeron_cluster_client_sessionCloseEvent_closeReason(&msg, &close_reason_enum))
            {
                close_reason = (int32_t)close_reason_enum;
            }

            aeron_consensus_module_agent_on_replay_session_close(
                adapter->agent,
                aeron_cluster_client_sessionCloseEvent_clusterSessionId(&msg),
                close_reason);
            break;
        }

        case 23: /* ClusterActionRequest */
        {
            struct aeron_cluster_client_clusterActionRequest msg;
            if (NULL == aeron_cluster_client_clusterActionRequest_wrap_for_decode(
                &msg, (char *)buffer, hdr_len, act_blen, act_version, length))
            {
                break;
            }

            int32_t flags = aeron_cluster_client_clusterActionRequest_flags_null_value() !=
                             aeron_cluster_client_clusterActionRequest_flags(&msg)
                           ? aeron_cluster_client_clusterActionRequest_flags(&msg)
                           : CLUSTER_ACTION_FLAGS_DEFAULT;

            enum aeron_cluster_client_clusterAction action_enum;
            int32_t action = 0;
            if (aeron_cluster_client_clusterActionRequest_action(&msg, &action_enum))
            {
                action = (int32_t)action_enum;
            }

            aeron_consensus_module_agent_on_replay_cluster_action(
                adapter->agent,
                aeron_cluster_client_clusterActionRequest_leadershipTermId(&msg),
                aeron_cluster_client_clusterActionRequest_logPosition(&msg),
                aeron_cluster_client_clusterActionRequest_timestamp(&msg),
                action,
                flags);
            /* BREAK action: stop polling this batch after a snapshot boundary */
            return AERON_ACTION_BREAK;
        }

        case 24: /* NewLeadershipTermEvent */
        {
            struct aeron_cluster_client_newLeadershipTermEvent msg;
            if (NULL == aeron_cluster_client_newLeadershipTermEvent_wrap_for_decode(
                &msg, (char *)buffer, hdr_len, act_blen, act_version, length))
            {
                break;
            }

            enum aeron_cluster_client_clusterTimeUnit time_unit_enum;
            int32_t time_unit = 0;
            if (aeron_cluster_client_newLeadershipTermEvent_timeUnit(&msg, &time_unit_enum))
            {
                time_unit = (int32_t)time_unit_enum;
            }

            aeron_consensus_module_agent_on_replay_new_leadership_term_event(
                adapter->agent,
                aeron_cluster_client_newLeadershipTermEvent_leadershipTermId(&msg),
                aeron_cluster_client_newLeadershipTermEvent_logPosition(&msg),
                aeron_cluster_client_newLeadershipTermEvent_timestamp(&msg),
                aeron_cluster_client_newLeadershipTermEvent_termBaseLogPosition(&msg),
                time_unit,
                aeron_cluster_client_newLeadershipTermEvent_appVersion(&msg));
            break;
        }

        default:
            break;
    }

    return AERON_ACTION_CONTINUE;
}

static aeron_controlled_fragment_handler_action_t on_fragment(
    void *clientd,
    const uint8_t *buffer,
    size_t length,
    aeron_header_t *header)
{
    return on_message(
        (aeron_cluster_log_adapter_t *)clientd,
        buffer,
        length,
        header);
}

int aeron_cluster_log_adapter_create(
    aeron_cluster_log_adapter_t **adapter,
    aeron_consensus_module_agent_t *agent,
    int fragment_limit)
{
    aeron_cluster_log_adapter_t *a = NULL;
    if (aeron_alloc((void **)&a, sizeof(aeron_cluster_log_adapter_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate log adapter");
        return -1;
    }

    a->agent          = agent;
    a->image          = NULL;
    a->subscription   = NULL;
    a->log_position   = 0;
    a->fragment_limit = fragment_limit > 0 ? fragment_limit : AERON_CLUSTER_LOG_ADAPTER_FRAGMENT_LIMIT;

    if (aeron_controlled_fragment_assembler_create(&a->fragment_assembler, on_fragment, a) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to create assembler for log adapter");
        aeron_free(a);
        return -1;
    }

    *adapter = a;
    return 0;
}

int aeron_cluster_log_adapter_close(aeron_cluster_log_adapter_t *adapter)
{
    if (NULL != adapter)
    {
        aeron_controlled_fragment_assembler_delete(adapter->fragment_assembler);
        aeron_free(adapter);
    }
    return 0;
}

int aeron_cluster_log_adapter_poll(aeron_cluster_log_adapter_t *adapter, int64_t stop_position)
{
    if (NULL == adapter->image)
    {
        return 0;
    }
    return aeron_image_bounded_controlled_poll(
        adapter->image,
        aeron_controlled_fragment_assembler_handler,
        adapter->fragment_assembler,
        stop_position,
        (size_t)adapter->fragment_limit);
}

int64_t aeron_cluster_log_adapter_position(aeron_cluster_log_adapter_t *adapter)
{
    if (NULL == adapter->image)
    {
        return adapter->log_position;
    }
    return aeron_image_position(adapter->image);
}

bool aeron_cluster_log_adapter_is_image_closed(aeron_cluster_log_adapter_t *adapter)
{
    return NULL == adapter->image || aeron_image_is_closed(adapter->image);
}

bool aeron_cluster_log_adapter_is_end_of_stream(aeron_cluster_log_adapter_t *adapter)
{
    return NULL != adapter->image && aeron_image_is_end_of_stream(adapter->image);
}

void aeron_cluster_log_adapter_set_image(
    aeron_cluster_log_adapter_t *adapter,
    aeron_image_t *image,
    aeron_subscription_t *subscription)
{
    if (NULL != adapter->image)
    {
        adapter->log_position = aeron_image_position(adapter->image);
    }
    adapter->image        = image;
    adapter->subscription = subscription;
}

int64_t aeron_cluster_log_adapter_disconnect(aeron_cluster_log_adapter_t *adapter)
{
    int64_t registration_id = -1;
    if (NULL != adapter->image)
    {
        adapter->log_position = aeron_image_position(adapter->image);
        adapter->image        = NULL;
    }
    if (NULL != adapter->subscription)
    {
        aeron_subscription_constants_t constants;
        if (aeron_subscription_constants(adapter->subscription, &constants) == 0)
        {
            registration_id = constants.registration_id;
        }
        aeron_subscription_close(adapter->subscription, NULL, NULL);
        adapter->subscription = NULL;
    }
    return registration_id;
}

void aeron_cluster_log_adapter_disconnect_max(aeron_cluster_log_adapter_t *adapter, int64_t max_log_position)
{
    aeron_cluster_log_adapter_disconnect(adapter);
    if (adapter->log_position > max_log_position)
    {
        adapter->log_position = max_log_position;
    }
}

void aeron_cluster_log_adapter_async_remove_destination(
    aeron_cluster_log_adapter_t *adapter,
    aeron_t *aeron,
    const char *destination)
{
    if (NULL != adapter->subscription && !aeron_subscription_is_closed(adapter->subscription))
    {
        aeron_async_destination_t *async_dest = NULL;
        aeron_subscription_async_remove_destination(&async_dest, aeron, adapter->subscription, destination);
    }
}
