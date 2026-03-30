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

#include "aeron_cluster.h"
#include "aeron_cluster_egress_poller.h"
#include "aeron_cluster_context.h"
#include "aeron_cluster_configuration.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

/* Generated C codecs */
#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/sessionMessageHeader.h"
#include "aeron_cluster_client/sessionEvent.h"
#include "aeron_cluster_client/newLeaderEvent.h"
#include "aeron_cluster_client/challenge.h"
#include "aeron_cluster_client/adminResponse.h"

/* -----------------------------------------------------------------------
 * Internal: grow a heap buffer to at least `needed` bytes.
 * ----------------------------------------------------------------------- */
static int egress_poller_ensure_capacity(char **buf, uint32_t *malloced_len, uint32_t needed)
{
    if (needed > *malloced_len)
    {
        if (aeron_reallocf((void **)buf, needed) < 0)
        {
            AERON_APPEND_ERR("%s", "unable to grow egress poller buffer");
            return -1;
        }
        *malloced_len = needed;
    }
    return 0;
}

/* -----------------------------------------------------------------------
 * Fragment handler — controlled so fragment assembler can reassemble.
 * ----------------------------------------------------------------------- */
static aeron_controlled_fragment_handler_action_t on_fragment(
    void *clientd,
    const uint8_t *buffer,
    size_t length,
    aeron_header_t *header)
{
    aeron_cluster_egress_poller_t *poller = (aeron_cluster_egress_poller_t *)clientd;

    if (poller->is_poll_complete)
    {
        return AERON_ACTION_ABORT;
    }

    if (length < aeron_cluster_client_messageHeader_encoded_length())
    {
        return AERON_ACTION_CONTINUE;
    }

    struct aeron_cluster_client_messageHeader hdr;
    if (NULL == aeron_cluster_client_messageHeader_wrap(
        &hdr, (char *)buffer, 0, aeron_cluster_client_messageHeader_sbe_schema_version(), length))
    {
        return AERON_ACTION_CONTINUE;
    }

    const int32_t template_id = (int32_t)aeron_cluster_client_messageHeader_templateId(&hdr);
    poller->template_id = template_id;

    const uint64_t hdr_len = aeron_cluster_client_messageHeader_encoded_length();


    switch (template_id)
    {
        case AERON_CLUSTER_SESSION_MESSAGE_HEADER_TEMPLATE_ID:
        {
            /* App message: skip the session header, deliver payload to client */
            struct aeron_cluster_client_sessionMessageHeader msg;
            if (NULL == aeron_cluster_client_sessionMessageHeader_wrap_for_decode(
                &msg,
                (char *)buffer,
                hdr_len,
                aeron_cluster_client_sessionMessageHeader_sbe_block_length(),
                aeron_cluster_client_sessionMessageHeader_sbe_schema_version(),
                length))
            {
                return AERON_ACTION_CONTINUE;
            }

            poller->leadership_term_id  = aeron_cluster_client_sessionMessageHeader_leadershipTermId(&msg);
            poller->cluster_session_id  = aeron_cluster_client_sessionMessageHeader_clusterSessionId(&msg);

            if (NULL != poller->ctx && NULL != poller->ctx->on_message)
            {
                const size_t payload_offset = AERON_CLUSTER_SESSION_HEADER_LENGTH;
                if (length > payload_offset)
                {
                    poller->ctx->on_message(
                        poller->ctx->on_message_clientd,
                        poller->cluster_session_id,
                        poller->leadership_term_id,
                        aeron_cluster_client_sessionMessageHeader_timestamp(&msg),
                        buffer + payload_offset,
                        length - payload_offset,
                        header);
                }
            }

            poller->is_poll_complete = true;
            return AERON_ACTION_BREAK;
        }

        case AERON_CLUSTER_SESSION_EVENT_TEMPLATE_ID:
        {
            struct aeron_cluster_client_sessionEvent msg;
            if (NULL == aeron_cluster_client_sessionEvent_wrap_for_decode(
                &msg,
                (char *)buffer,
                hdr_len,
                aeron_cluster_client_sessionEvent_sbe_block_length(),
                aeron_cluster_client_sessionEvent_sbe_schema_version(),
                length))
            {
                return AERON_ACTION_CONTINUE;
            }

            poller->cluster_session_id  = aeron_cluster_client_sessionEvent_clusterSessionId(&msg);
            poller->correlation_id      = aeron_cluster_client_sessionEvent_correlationId(&msg);
            poller->leadership_term_id  = aeron_cluster_client_sessionEvent_leadershipTermId(&msg);
            poller->leader_member_id    = aeron_cluster_client_sessionEvent_leaderMemberId(&msg);

            if (aeron_cluster_client_sessionEvent_leaderHeartbeatTimeoutNs_in_acting_version(&msg))
            {
                poller->leader_heartbeat_timeout_ns =
                    aeron_cluster_client_sessionEvent_leaderHeartbeatTimeoutNs(&msg);
            }

            enum aeron_cluster_client_eventCode code;
            if (aeron_cluster_client_sessionEvent_code(&msg, &code))
            {
                poller->event_code = (int32_t)code;
            }

            const uint32_t detail_len = aeron_cluster_client_sessionEvent_detail_length(&msg);
            if (egress_poller_ensure_capacity(&poller->detail, &poller->detail_malloced_len, detail_len + 1) < 0)
            {
                return AERON_ACTION_CONTINUE;
            }
            poller->detail_length = (uint32_t)aeron_cluster_client_sessionEvent_get_detail(
                &msg, poller->detail, detail_len);
            poller->detail[poller->detail_length] = '\0';

            if (NULL != poller->ctx && NULL != poller->ctx->on_session_event)
            {
                poller->ctx->on_session_event(
                    poller->ctx->on_session_event_clientd,
                    poller->cluster_session_id,
                    poller->correlation_id,
                    poller->leadership_term_id,
                    poller->leader_member_id,
                    poller->event_code,
                    poller->detail,
                    poller->detail_length);
            }

            poller->is_poll_complete = true;
            return AERON_ACTION_BREAK;
        }

        case AERON_CLUSTER_NEW_LEADER_EVENT_TEMPLATE_ID:
        {
            struct aeron_cluster_client_newLeaderEvent msg;
            if (NULL == aeron_cluster_client_newLeaderEvent_wrap_for_decode(
                &msg,
                (char *)buffer,
                hdr_len,
                aeron_cluster_client_newLeaderEvent_sbe_block_length(),
                aeron_cluster_client_newLeaderEvent_sbe_schema_version(),
                length))
            {
                return AERON_ACTION_CONTINUE;
            }

            poller->leadership_term_id  = aeron_cluster_client_newLeaderEvent_leadershipTermId(&msg);
            poller->cluster_session_id  = aeron_cluster_client_newLeaderEvent_clusterSessionId(&msg);
            poller->leader_member_id    = aeron_cluster_client_newLeaderEvent_leaderMemberId(&msg);

            const uint32_t ep_len = aeron_cluster_client_newLeaderEvent_ingressEndpoints_length(&msg);
            if (egress_poller_ensure_capacity(&poller->detail, &poller->detail_malloced_len, ep_len + 1) < 0)
            {
                return AERON_ACTION_CONTINUE;
            }
            poller->detail_length = (uint32_t)aeron_cluster_client_newLeaderEvent_get_ingressEndpoints(
                &msg, poller->detail, ep_len);
            poller->detail[poller->detail_length] = '\0';

            if (NULL != poller->ctx && NULL != poller->ctx->on_new_leader_event)
            {
                poller->ctx->on_new_leader_event(
                    poller->ctx->on_new_leader_event_clientd,
                    poller->cluster_session_id,
                    poller->leadership_term_id,
                    poller->leader_member_id,
                    poller->detail,
                    poller->detail_length);
            }

            poller->is_poll_complete = true;
            return AERON_ACTION_BREAK;
        }

        case AERON_CLUSTER_CHALLENGE_TEMPLATE_ID:
        {
            struct aeron_cluster_client_challenge msg;
            if (NULL == aeron_cluster_client_challenge_wrap_for_decode(
                &msg,
                (char *)buffer,
                hdr_len,
                aeron_cluster_client_challenge_sbe_block_length(),
                aeron_cluster_client_challenge_sbe_schema_version(),
                length))
            {
                return AERON_ACTION_CONTINUE;
            }

            poller->cluster_session_id = aeron_cluster_client_challenge_clusterSessionId(&msg);
            poller->correlation_id     = aeron_cluster_client_challenge_correlationId(&msg);

            const uint32_t chal_len = aeron_cluster_client_challenge_encodedChallenge_length(&msg);
            if (egress_poller_ensure_capacity(
                &poller->encoded_challenge_buffer,
                &poller->encoded_challenge_malloced_len,
                chal_len) < 0)
            {
                return AERON_ACTION_CONTINUE;
            }
            poller->encoded_challenge.length = (uint32_t)aeron_cluster_client_challenge_get_encodedChallenge(
                &msg, poller->encoded_challenge_buffer, chal_len);
            poller->encoded_challenge.data   = poller->encoded_challenge_buffer;
            poller->was_challenged           = true;

            poller->is_poll_complete = true;
            return AERON_ACTION_BREAK;
        }

        case AERON_CLUSTER_ADMIN_RESPONSE_TEMPLATE_ID:
        {
            struct aeron_cluster_client_adminResponse msg;
            if (NULL == aeron_cluster_client_adminResponse_wrap_for_decode(
                &msg,
                (char *)buffer,
                hdr_len,
                aeron_cluster_client_adminResponse_sbe_block_length(),
                aeron_cluster_client_adminResponse_sbe_schema_version(),
                length))
            {
                return AERON_ACTION_CONTINUE;
            }

            poller->cluster_session_id = aeron_cluster_client_adminResponse_clusterSessionId(&msg);
            poller->correlation_id     = aeron_cluster_client_adminResponse_correlationId(&msg);

            enum aeron_cluster_client_adminRequestType req_type = 0;
            aeron_cluster_client_adminResponse_requestType(&msg, &req_type);

            enum aeron_cluster_client_adminResponseCode resp_code = 0;
            aeron_cluster_client_adminResponse_responseCode(&msg, &resp_code);

            const uint32_t msg_len = aeron_cluster_client_adminResponse_message_length(&msg);
            if (egress_poller_ensure_capacity(&poller->detail, &poller->detail_malloced_len, msg_len + 1) < 0)
            {
                return AERON_ACTION_CONTINUE;
            }
            poller->detail_length = (uint32_t)aeron_cluster_client_adminResponse_get_message(
                &msg, poller->detail, msg_len);
            poller->detail[poller->detail_length] = '\0';

            if (NULL != poller->ctx && NULL != poller->ctx->on_admin_response)
            {
                const uint32_t payload_len = aeron_cluster_client_adminResponse_payload_length(&msg);
                const char *payload_ptr    = aeron_cluster_client_adminResponse_payload(&msg);

                poller->ctx->on_admin_response(
                    poller->ctx->on_admin_response_clientd,
                    poller->cluster_session_id,
                    poller->correlation_id,
                    (int32_t)req_type,
                    (int32_t)resp_code,
                    poller->detail,
                    poller->detail_length,
                    (const uint8_t *)payload_ptr,
                    payload_len);
            }

            poller->is_poll_complete = true;
            return AERON_ACTION_BREAK;
        }

        default:
            /* Unknown template — skip */
            break;
    }

    return AERON_ACTION_CONTINUE;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */
int aeron_cluster_egress_poller_create(
    aeron_cluster_egress_poller_t **poller,
    aeron_subscription_t *subscription,
    int fragment_limit)
{
    aeron_cluster_egress_poller_t *_poller = NULL;

    if (aeron_alloc((void **)&_poller, sizeof(aeron_cluster_egress_poller_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate aeron_cluster_egress_poller");
        return -1;
    }

    _poller->subscription     = subscription;
    _poller->fragment_limit   = fragment_limit;
    _poller->ctx              = NULL;
    _poller->template_id      = -1;
    _poller->cluster_session_id = -1;
    _poller->leadership_term_id = -1;
    _poller->correlation_id   = -1;
    _poller->leader_member_id = -1;
    _poller->leader_heartbeat_timeout_ns = -1;
    _poller->event_code       = 0;
    _poller->detail           = NULL;
    _poller->detail_malloced_len = 0;
    _poller->detail_length    = 0;
    _poller->encoded_challenge_buffer     = NULL;
    _poller->encoded_challenge_malloced_len = 0;
    _poller->encoded_challenge.data   = NULL;
    _poller->encoded_challenge.length = 0;
    _poller->is_poll_complete = false;
    _poller->was_challenged   = false;

    if (aeron_controlled_fragment_assembler_create(
        &_poller->fragment_assembler, on_fragment, _poller) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to create controlled fragment assembler for egress poller");
        aeron_free(_poller);
        return -1;
    }

    *poller = _poller;
    return 0;
}

int aeron_cluster_egress_poller_close(aeron_cluster_egress_poller_t *poller)
{
    if (NULL != poller)
    {
        aeron_controlled_fragment_assembler_delete(poller->fragment_assembler);
        aeron_free(poller->detail);
        aeron_free(poller->encoded_challenge_buffer);
        aeron_free(poller);
    }

    return 0;
}

void aeron_cluster_egress_poller_set_context(
    aeron_cluster_egress_poller_t *poller,
    aeron_cluster_context_t *ctx)
{
    poller->ctx = ctx;
}

int aeron_cluster_egress_poller_poll(aeron_cluster_egress_poller_t *poller)
{
    poller->is_poll_complete = false;
    poller->was_challenged   = false;
    poller->template_id      = -1;

    int result = aeron_subscription_controlled_poll(
        poller->subscription,
        aeron_controlled_fragment_assembler_handler,
        poller->fragment_assembler,
        poller->fragment_limit);

    return result;
}

aeron_controlled_fragment_handler_action_t aeron_cluster_egress_poller_on_fragment_for_test(
    aeron_cluster_egress_poller_t *poller,
    const uint8_t *buffer,
    size_t length)
{
    return on_fragment(poller, buffer, length, NULL);
}
