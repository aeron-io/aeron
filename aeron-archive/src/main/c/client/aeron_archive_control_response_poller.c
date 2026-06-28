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

#include <stdio.h>

#include "aeron_archive.h"
#include "aeron_archive_control_response_poller.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

#include "c/aeron_archive_client/messageHeader.h"
#include "c/aeron_archive_client/controlResponse.h"
#include "c/aeron_archive_client/challenge.h"
#include "c/aeron_archive_client/recordingSignalEvent.h"

#define AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_ERROR_MESSAGE_INITIAL_LEN 10000
#define AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_ENCODED_CHALLENGE_BUFFER_INITIAL_LEN 10000

void aeron_archive_control_response_poller_reset(aeron_archive_control_response_poller_t *poller);

aeron_controlled_fragment_handler_action_t aeron_archive_control_response_poller_on_fragment(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header);

/* *************** */

int aeron_archive_control_response_poller_create(
    aeron_archive_control_response_poller_t **poller,
    aeron_subscription_t *subscription,
    int fragment_limit)
{
    aeron_archive_control_response_poller_t *_poller = NULL;

    if (aeron_alloc((void **)&_poller, sizeof(aeron_archive_control_response_poller_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_control_response_poller_t");
        return -1;
    }

    _poller->subscription = subscription;
    _poller->fragment_limit = fragment_limit;

    if (aeron_controlled_fragment_assembler_create(
        &_poller->fragment_assembler,
        aeron_archive_control_response_poller_on_fragment,
        _poller) < 0)
    {
        AERON_APPEND_ERR("%s", "aeron_fragment_assembler_create\n");
        return -1;
    }

    _poller->error_message_malloced_len = AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_ERROR_MESSAGE_INITIAL_LEN;
    if (aeron_alloc((void **)&_poller->error_message, _poller->error_message_malloced_len) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    _poller->encoded_challenge_buffer_malloced_len = AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_ENCODED_CHALLENGE_BUFFER_INITIAL_LEN;
    if (aeron_alloc((void **)&_poller->encoded_challenge_buffer, _poller->encoded_challenge_buffer_malloced_len) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    aeron_archive_control_response_poller_reset(_poller);

    *poller = _poller;

    return 0;
}

int aeron_archive_control_response_poller_close(aeron_archive_control_response_poller_t *poller)
{
    aeron_controlled_fragment_assembler_delete(poller->fragment_assembler);
    poller->fragment_assembler = NULL;

    aeron_free(poller->error_message);
    poller->error_message = NULL;
    poller->error_message_malloced_len = 0;

    aeron_free(poller->encoded_challenge_buffer);
    poller->encoded_challenge_buffer = NULL;
    poller->encoded_challenge_buffer_malloced_len = 0;

    aeron_free(poller);

    return 0;
}

int aeron_archive_control_response_poller_poll(aeron_archive_control_response_poller_t *poller)
{
    if (poller->is_poll_complete)
    {
        aeron_archive_control_response_poller_reset(poller);
    }

    int rc = aeron_subscription_controlled_poll(
        poller->subscription,
        aeron_controlled_fragment_assembler_handler,
        poller->fragment_assembler,
        poller->fragment_limit);

    if (rc < 0)
    {
        AERON_APPEND_ERR("%s", "");
    }
    else if (poller->error_on_fragment)
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }

    return rc;
}

/* *************** */

void aeron_archive_control_response_poller_reset(aeron_archive_control_response_poller_t *poller)
{
    poller->error_on_fragment = false;

    poller->control_session_id = AERON_NULL_VALUE;
    poller->correlation_id = AERON_NULL_VALUE;
    poller->relevant_id = AERON_NULL_VALUE;
    poller->recording_id = AERON_NULL_VALUE;
    poller->subscription_id = AERON_NULL_VALUE;
    poller->position = AERON_NULL_VALUE;

    poller->recording_signal_code = INT32_MIN;
    poller->version = 0;

    memset(poller->error_message, 0, poller->error_message_malloced_len);
    memset(poller->encoded_challenge_buffer, 0, poller->encoded_challenge_buffer_malloced_len);

    poller->encoded_challenge.data = NULL;
    poller->encoded_challenge.length = 0;

    poller->code_value = -1;

    poller->is_poll_complete = false;
    poller->is_code_ok = false;
    poller->is_code_error = false;
    poller->is_control_response = false;
    poller->was_challenged = false;
    poller->is_recording_signal = false;
}

aeron_controlled_fragment_handler_action_t aeron_archive_control_response_poller_on_fragment(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    aeron_archive_control_response_poller_t *poller = (aeron_archive_control_response_poller_t *)clientd;

    if (poller->is_poll_complete)
    {
        return AERON_ACTION_ABORT;
    }

    struct aeron_archive_client_messageHeader hdr;

    if (aeron_archive_client_messageHeader_wrap(
        &hdr,
        (char *)buffer,
        0,
        aeron_archive_client_messageHeader_sbe_schema_version(),
        length) == NULL)
    {
        AERON_SET_ERR(errno, "%s", "unable to wrap buffer");
        poller->error_on_fragment = true;
        return AERON_ACTION_BREAK;
    }

    uint16_t schema_id = aeron_archive_client_messageHeader_schemaId(&hdr);

    if (schema_id != aeron_archive_client_messageHeader_sbe_schema_id())
    {
        AERON_SET_ERR(-1, "found schema id: %i that doesn't match expected id: %i", schema_id, aeron_archive_client_messageHeader_sbe_schema_id());
        poller->error_on_fragment = true;
        return AERON_ACTION_BREAK;
    }

    uint16_t template_id = aeron_archive_client_messageHeader_templateId(&hdr);

    switch(template_id)
    {
        case AERON_ARCHIVE_CLIENT_CONTROL_RESPONSE_SBE_TEMPLATE_ID:
        {
            struct aeron_archive_client_controlResponse control_response;

            aeron_archive_client_controlResponse_wrap_for_decode(
                &control_response,
                (char *)buffer,
                aeron_archive_client_messageHeader_encoded_length(),
                aeron_archive_client_controlResponse_sbe_block_length(),
                aeron_archive_client_controlResponse_sbe_schema_version(),
                length);

            poller->control_session_id = aeron_archive_client_controlResponse_controlSessionId(&control_response);
            poller->correlation_id = aeron_archive_client_controlResponse_correlationId(&control_response);
            poller->relevant_id = aeron_archive_client_controlResponse_relevantId(&control_response);
            poller->version = aeron_archive_client_controlResponse_version(&control_response);

            if (!aeron_archive_client_controlResponse_code(
                &control_response,
                (enum aeron_archive_client_controlResponseCode *)&poller->code_value))
            {
                AERON_SET_ERR(-1, "%s", "unable to read control response code");
                poller->error_on_fragment = true;
                return AERON_ACTION_BREAK;
            }

            poller->is_code_error = poller->code_value == aeron_archive_client_controlResponseCode_ERROR;
            poller->is_code_ok = poller->code_value == aeron_archive_client_controlResponseCode_OK;

            uint32_t error_message_len = aeron_archive_client_controlResponse_errorMessage_length(&control_response);
            uint32_t len_with_terminator = error_message_len + 1;
            if (len_with_terminator > poller->error_message_malloced_len)
            {
                if (aeron_reallocf((void **)&poller->error_message, len_with_terminator) < 0)
                {
                    AERON_APPEND_ERR("%s", "unable to reallocate error_message");
                    poller->error_on_fragment = true;
                    return AERON_ACTION_BREAK;
                }
                poller->error_message_malloced_len = len_with_terminator;
            }

            aeron_archive_client_controlResponse_get_errorMessage(
                &control_response,
                poller->error_message,
                error_message_len);
            poller->error_message[error_message_len] = '\0';

            poller->is_control_response = true;
            poller->is_poll_complete = true;

            return AERON_ACTION_BREAK;
        }

        case AERON_ARCHIVE_CLIENT_CHALLENGE_SBE_TEMPLATE_ID:
        {
            struct aeron_archive_client_challenge challenge;

            aeron_archive_client_challenge_wrap_for_decode(
                &challenge,
                (char *)buffer,
                aeron_archive_client_messageHeader_encoded_length(),
                aeron_archive_client_challenge_sbe_block_length(),
                aeron_archive_client_challenge_sbe_schema_version(),
                length);

            poller->control_session_id = aeron_archive_client_challenge_controlSessionId(&challenge);
            poller->correlation_id = aeron_archive_client_challenge_correlationId(&challenge);
            poller->relevant_id = AERON_NULL_VALUE;
            poller->version = aeron_archive_client_challenge_version(&challenge);

            poller->code_value = aeron_archive_client_controlResponseCode_NULL_VALUE;
            poller->is_code_error = false;
            poller->is_code_ok = false;

            uint32_t encoded_challenge_length = aeron_archive_client_challenge_encodedChallenge_length(&challenge);
            uint32_t len_with_terminator = encoded_challenge_length + 1;
            if (len_with_terminator > poller->encoded_challenge_buffer_malloced_len)
            {
                if (aeron_reallocf((void **)&poller->encoded_challenge_buffer, len_with_terminator) < 0)
                {
                    AERON_SET_ERR(ENOMEM, "%s", "unable to reallocate encoded_challenge_buffer");
                    poller->error_on_fragment = true;
                    return AERON_ACTION_BREAK;
                }
                poller->encoded_challenge_buffer_malloced_len = len_with_terminator;
            }

            aeron_archive_client_challenge_get_encodedChallenge(
                &challenge,
                poller->encoded_challenge_buffer,
                encoded_challenge_length);
            poller->encoded_challenge_buffer[encoded_challenge_length] = '\0';

            poller->encoded_challenge.data = poller->encoded_challenge_buffer;
            poller->encoded_challenge.length = encoded_challenge_length;

            poller->is_control_response = false;
            poller->was_challenged = true;
            poller->is_poll_complete = true;

            return AERON_ACTION_BREAK;
        }

        case AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_EVENT_SBE_TEMPLATE_ID:
        {
            struct aeron_archive_client_recordingSignalEvent recording_signal_event;

            aeron_archive_client_recordingSignalEvent_wrap_for_decode(
                &recording_signal_event,
                (char *)buffer,
                aeron_archive_client_messageHeader_encoded_length(),
                aeron_archive_client_recordingSignalEvent_sbe_block_length(),
                aeron_archive_client_recordingSignalEvent_sbe_schema_version(),
                length);

            poller->control_session_id = aeron_archive_client_recordingSignalEvent_controlSessionId(&recording_signal_event);
            poller->correlation_id = aeron_archive_client_recordingSignalEvent_correlationId(&recording_signal_event);
            poller->recording_id = aeron_archive_client_recordingSignalEvent_recordingId(&recording_signal_event);
            poller->subscription_id = aeron_archive_client_recordingSignalEvent_subscriptionId(&recording_signal_event);
            poller->position = aeron_archive_client_recordingSignalEvent_position(&recording_signal_event);

            if (!aeron_archive_client_recordingSignalEvent_signal(
                &recording_signal_event,
                (enum aeron_archive_client_recordingSignal *)&poller->recording_signal_code))
            {
                AERON_SET_ERR(-1, "%s", "unable to read recording signal code");
                poller->error_on_fragment = true;
                return AERON_ACTION_BREAK;
            }

            poller->is_recording_signal = true;
            poller->is_poll_complete = true;

            return AERON_ACTION_BREAK;
        }

        default:
            // do nothing
            break;
    }

    return AERON_ACTION_CONTINUE;
}
