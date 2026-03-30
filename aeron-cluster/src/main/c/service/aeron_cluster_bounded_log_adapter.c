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

#include "aeron_cluster_bounded_log_adapter.h"
#include "aeron_clustered_service_agent.h"
#include "aeron_cluster_service.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

#include "uri/aeron_uri.h"
#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/sessionMessageHeader.h"
#include "aeron_cluster_client/timerEvent.h"
#include "aeron_cluster_client/sessionOpenEvent.h"
#include "aeron_cluster_client/sessionCloseEvent.h"
#include "aeron_cluster_client/clusterActionRequest.h"
#include "aeron_cluster_client/newLeadershipTermEvent.h"
#include "aeron_cluster_client/membershipChangeEvent.h"

static aeron_controlled_fragment_handler_action_t on_fragment(
    void *clientd,
    const uint8_t *buffer,
    size_t length,
    aeron_header_t *header)
{
    aeron_cluster_bounded_log_adapter_t *adapter = (aeron_cluster_bounded_log_adapter_t *)clientd;

    /* Stop if this fragment would cross the commit boundary */
    int64_t position = aeron_image_position(adapter->image);
    if (position >= adapter->max_log_position)
    {
        return AERON_ACTION_ABORT;
    }

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

    const uint64_t hdr_len  = aeron_cluster_client_messageHeader_encoded_length();
    const int32_t template_id = (int32_t)aeron_cluster_client_messageHeader_templateId(&hdr);

    switch (template_id)
    {
        case 1: /* SessionMessageHeader — app message */
        {
            struct aeron_cluster_client_sessionMessageHeader msg;
            if (NULL == aeron_cluster_client_sessionMessageHeader_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_sessionMessageHeader_sbe_block_length(),
                aeron_cluster_client_sessionMessageHeader_sbe_schema_version(),
                length))
            {
                return AERON_ACTION_CONTINUE;
            }

            aeron_clustered_service_agent_on_session_message(
                adapter->agent,
                aeron_cluster_client_sessionMessageHeader_clusterSessionId(&msg),
                aeron_cluster_client_sessionMessageHeader_timestamp(&msg),
                buffer + AERON_CLUSTER_SESSION_HEADER_LENGTH,
                length - AERON_CLUSTER_SESSION_HEADER_LENGTH);
            return AERON_ACTION_COMMIT;
        }

        case 20: /* TimerEvent */
        {
            struct aeron_cluster_client_timerEvent msg;
            if (NULL == aeron_cluster_client_timerEvent_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_timerEvent_sbe_block_length(),
                aeron_cluster_client_timerEvent_sbe_schema_version(),
                length))
            {
                return AERON_ACTION_CONTINUE;
            }

            aeron_clustered_service_agent_on_timer_event(
                adapter->agent,
                aeron_cluster_client_timerEvent_correlationId(&msg),
                aeron_cluster_client_timerEvent_timestamp(&msg));
            return AERON_ACTION_COMMIT;
        }

        case 21: /* SessionOpenEvent */
        {
            struct aeron_cluster_client_sessionOpenEvent msg;
            if (NULL == aeron_cluster_client_sessionOpenEvent_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_sessionOpenEvent_sbe_block_length(),
                aeron_cluster_client_sessionOpenEvent_sbe_schema_version(),
                length))
            {
                return AERON_ACTION_CONTINUE;
            }

            char response_channel[AERON_URI_MAX_LENGTH];
            uint32_t ch_len = aeron_cluster_client_sessionOpenEvent_responseChannel_length(&msg);
            if (ch_len >= sizeof(response_channel)) { ch_len = sizeof(response_channel) - 1; }
            aeron_cluster_client_sessionOpenEvent_get_responseChannel(&msg, response_channel, ch_len);
            response_channel[ch_len] = '\0';

            uint32_t principal_len = aeron_cluster_client_sessionOpenEvent_encodedPrincipal_length(&msg);
            const char *principal_ptr = aeron_cluster_client_sessionOpenEvent_encodedPrincipal(&msg);

            aeron_clustered_service_agent_on_session_open(
                adapter->agent,
                aeron_cluster_client_sessionOpenEvent_clusterSessionId(&msg),
                aeron_cluster_client_sessionOpenEvent_correlationId(&msg),
                0, /* logPosition not in SessionOpenEvent; derived from header position */
                aeron_cluster_client_sessionOpenEvent_timestamp(&msg),
                aeron_cluster_client_sessionOpenEvent_responseStreamId(&msg),
                response_channel,
                (const uint8_t *)principal_ptr,
                principal_len);
            return AERON_ACTION_COMMIT;
        }

        case 22: /* SessionCloseEvent */
        {
            struct aeron_cluster_client_sessionCloseEvent msg;
            if (NULL == aeron_cluster_client_sessionCloseEvent_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_sessionCloseEvent_sbe_block_length(),
                aeron_cluster_client_sessionCloseEvent_sbe_schema_version(),
                length))
            {
                return AERON_ACTION_CONTINUE;
            }

            enum aeron_cluster_client_closeReason close_reason = 0;
            aeron_cluster_client_sessionCloseEvent_closeReason(&msg, &close_reason);

            aeron_clustered_service_agent_on_session_close(
                adapter->agent,
                aeron_cluster_client_sessionCloseEvent_clusterSessionId(&msg),
                aeron_cluster_client_sessionCloseEvent_timestamp(&msg),
                (aeron_cluster_close_reason_t)close_reason);
            return AERON_ACTION_COMMIT;
        }

        case 23: /* ClusterActionRequest */
        {
            struct aeron_cluster_client_clusterActionRequest msg;
            if (NULL == aeron_cluster_client_clusterActionRequest_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_clusterActionRequest_sbe_block_length(),
                aeron_cluster_client_clusterActionRequest_sbe_schema_version(),
                length))
            {
                return AERON_ACTION_CONTINUE;
            }

            enum aeron_cluster_client_clusterAction action_val = 0;
            aeron_cluster_client_clusterActionRequest_action(&msg, &action_val);

            aeron_clustered_service_agent_on_service_action(
                adapter->agent,
                aeron_cluster_client_clusterActionRequest_logPosition(&msg),
                aeron_cluster_client_clusterActionRequest_timestamp(&msg),
                (int32_t)action_val,
                aeron_cluster_client_clusterActionRequest_flags(&msg));
            return AERON_ACTION_COMMIT;
        }

        case 24: /* NewLeadershipTermEvent */
        {
            struct aeron_cluster_client_newLeadershipTermEvent msg;
            if (NULL == aeron_cluster_client_newLeadershipTermEvent_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_newLeadershipTermEvent_sbe_block_length(),
                aeron_cluster_client_newLeadershipTermEvent_sbe_schema_version(),
                length))
            {
                return AERON_ACTION_CONTINUE;
            }

            aeron_clustered_service_agent_on_new_leadership_term_event(
                adapter->agent,
                aeron_cluster_client_newLeadershipTermEvent_leadershipTermId(&msg),
                aeron_cluster_client_newLeadershipTermEvent_logPosition(&msg),
                aeron_cluster_client_newLeadershipTermEvent_timestamp(&msg),
                aeron_cluster_client_newLeadershipTermEvent_termBaseLogPosition(&msg),
                aeron_cluster_client_newLeadershipTermEvent_leaderMemberId(&msg),
                aeron_cluster_client_newLeadershipTermEvent_logSessionId(&msg),
                aeron_cluster_client_newLeadershipTermEvent_appVersion(&msg));
            return AERON_ACTION_COMMIT;
        }

        case 25: /* MembershipChangeEvent */
        {
            struct aeron_cluster_client_membershipChangeEvent msg;
            if (NULL == aeron_cluster_client_membershipChangeEvent_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_membershipChangeEvent_sbe_block_length(),
                aeron_cluster_client_membershipChangeEvent_sbe_schema_version(),
                length))
            {
                return AERON_ACTION_CONTINUE;
            }

            enum aeron_cluster_client_changeType change_type_val = 0;
            aeron_cluster_client_membershipChangeEvent_changeType(&msg, &change_type_val);

            aeron_clustered_service_agent_on_membership_change(
                adapter->agent,
                aeron_cluster_client_membershipChangeEvent_leadershipTermId(&msg),
                aeron_cluster_client_membershipChangeEvent_logPosition(&msg),
                aeron_cluster_client_membershipChangeEvent_timestamp(&msg),
                aeron_cluster_client_membershipChangeEvent_memberId(&msg),
                (int32_t)change_type_val);
            return AERON_ACTION_COMMIT;
        }

        default:
            break;
    }

    return AERON_ACTION_CONTINUE;
}

int aeron_cluster_bounded_log_adapter_create(
    aeron_cluster_bounded_log_adapter_t **adapter,
    aeron_subscription_t *subscription,
    aeron_image_t *image,
    int64_t max_log_position,
    aeron_clustered_service_agent_t *agent)
{
    aeron_cluster_bounded_log_adapter_t *_adapter = NULL;

    if (aeron_alloc((void **)&_adapter, sizeof(aeron_cluster_bounded_log_adapter_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate bounded log adapter");
        return -1;
    }

    _adapter->subscription     = subscription;
    _adapter->image            = image;
    _adapter->agent            = agent;
    _adapter->max_log_position = max_log_position;

    if (aeron_controlled_fragment_assembler_create(
        &_adapter->fragment_assembler, on_fragment, _adapter) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to create controlled fragment assembler for log adapter");
        aeron_free(_adapter);
        return -1;
    }

    *adapter = _adapter;
    return 0;
}

int aeron_cluster_bounded_log_adapter_close(aeron_cluster_bounded_log_adapter_t *adapter)
{
    if (NULL != adapter)
    {
        aeron_controlled_fragment_assembler_delete(adapter->fragment_assembler);
        aeron_free(adapter);
    }
    return 0;
}

int aeron_cluster_bounded_log_adapter_poll(
    aeron_cluster_bounded_log_adapter_t *adapter,
    int64_t commit_position)
{
    int64_t current_pos = aeron_image_position(adapter->image);
    if (current_pos >= commit_position || current_pos >= adapter->max_log_position)
    {
        return 0;
    }

    return aeron_image_controlled_poll(
        adapter->image,
        aeron_controlled_fragment_assembler_handler,
        adapter->fragment_assembler,
        AERON_CLUSTER_LOG_FRAGMENT_LIMIT);
}

bool aeron_cluster_bounded_log_adapter_is_image_closed(aeron_cluster_bounded_log_adapter_t *adapter)
{
    return aeron_image_is_closed(adapter->image);
}
