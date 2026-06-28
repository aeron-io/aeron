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
#include <inttypes.h>

#include "aeron_archive.h"
#include "aeron_archive_context.h"
#include "aeron_archive_recording_descriptor_poller.h"
#include "aeron_archive_recording_signal.h"

#include "c/aeron_archive_client/messageHeader.h"
#include "c/aeron_archive_client/controlResponse.h"
#include "c/aeron_archive_client/recordingDescriptor.h"
#include "c/aeron_archive_client/recordingSignalEvent.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

aeron_controlled_fragment_handler_action_t aeron_archive_recording_descriptor_poller_on_fragment(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header);

/* *************** */

int aeron_archive_recording_descriptor_poller_create(
    aeron_archive_recording_descriptor_poller_t **poller,
    aeron_archive_context_t *ctx,
    aeron_subscription_t *subscription,
    int64_t control_session_id,
    int fragment_limit)
{
    aeron_archive_recording_descriptor_poller_t *_poller = NULL;

    if (aeron_alloc((void **)&_poller, sizeof(aeron_archive_recording_descriptor_poller_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_recording_descriptor_poller_t");
        return -1;
    }

    _poller->ctx = ctx;
    _poller->subscription = subscription;
    _poller->control_session_id = control_session_id;

    _poller->fragment_limit = fragment_limit;

    if (aeron_controlled_fragment_assembler_create(
        &_poller->fragment_assembler,
        aeron_archive_recording_descriptor_poller_on_fragment,
        _poller) < 0)
    {
        AERON_APPEND_ERR("%s", "aeron_fragment_assembler_create\n");
        return -1;
    }

    _poller->error_on_fragment = false;
    _poller->is_dispatch_complete = false;

    *poller = _poller;

    return 0;
}

int aeron_archive_recording_descriptor_poller_close(aeron_archive_recording_descriptor_poller_t *poller)
{
    aeron_controlled_fragment_assembler_delete(poller->fragment_assembler);
    poller->fragment_assembler = NULL;

    aeron_free(poller);

    return 0;
}

void aeron_archive_recording_descriptor_poller_reset(
    aeron_archive_recording_descriptor_poller_t *poller,
    int64_t correlation_id,
    int32_t record_count,
    aeron_archive_recording_descriptor_consumer_func_t recording_descriptor_consumer,
    void *recording_descriptor_consumer_clientd)
{
    poller->error_on_fragment = false;

    poller->correlation_id = correlation_id;
    poller->remaining_record_count = record_count;
    poller->recording_descriptor_consumer = recording_descriptor_consumer;
    poller->recording_descriptor_consumer_clientd = recording_descriptor_consumer_clientd;
}

int aeron_archive_recording_descriptor_poller_poll(aeron_archive_recording_descriptor_poller_t *poller)
{
    if (poller->is_dispatch_complete)
    {
        poller->is_dispatch_complete = false;
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

/* ************* */

aeron_controlled_fragment_handler_action_t aeron_archive_recording_descriptor_poller_on_fragment(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    aeron_archive_recording_descriptor_poller_t *poller = (aeron_archive_recording_descriptor_poller_t *)clientd;

    if (poller->is_dispatch_complete)
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

            if (aeron_archive_client_controlResponse_controlSessionId(&control_response) == poller->control_session_id)
            {
                int code;

                if (!aeron_archive_client_controlResponse_code(
                    &control_response,
                    (enum aeron_archive_client_controlResponseCode *)&code))
                {
                    AERON_SET_ERR(-1, "%s", "unable to read control response code");
                    poller->error_on_fragment = true;
                    return AERON_ACTION_BREAK;
                }

                int64_t correlation_id = aeron_archive_client_controlResponse_correlationId(&control_response);

                if (aeron_archive_client_controlResponseCode_RECORDING_UNKNOWN == code &&
                    correlation_id == poller->correlation_id)
                {
                    poller->is_dispatch_complete = true;

                    return AERON_ACTION_BREAK;
                }

                if (aeron_archive_client_controlResponseCode_ERROR == code)
                {
                    if (correlation_id == poller->correlation_id)
                    {
                        struct aeron_archive_client_controlResponse_string_view string_view =
                            aeron_archive_client_controlResponse_get_errorMessage_as_string_view(&control_response);

                        AERON_SET_ERR(
                            (int32_t)aeron_archive_client_controlResponse_relevantId(&control_response),
                            "correlation_id=%" PRIi64 " %.*s",
                            correlation_id,
                            string_view.length,
                            string_view.data);
                        poller->error_on_fragment = true;
                        return AERON_ACTION_BREAK;
                    }
                    else if (NULL != poller->ctx->error_handler)
                    {
                        struct aeron_archive_client_controlResponse_string_view string_view =
                            aeron_archive_client_controlResponse_get_errorMessage_as_string_view(&control_response);

                        aeron_archive_context_invoke_error_handler(
                            poller->ctx,
                            poller->correlation_id,
                            (int32_t)aeron_archive_client_controlResponse_relevantId(&control_response),
                            string_view.data);
                    }
                }
            }

            break;
        }

        case AERON_ARCHIVE_CLIENT_RECORDING_DESCRIPTOR_SBE_TEMPLATE_ID:
        {
            struct aeron_archive_client_recordingDescriptor recording_descriptor;

            aeron_archive_client_recordingDescriptor_wrap_for_decode(
                &recording_descriptor,
                (char *)buffer,
                aeron_archive_client_messageHeader_encoded_length(),
                aeron_archive_client_recordingDescriptor_sbe_block_length(),
                aeron_archive_client_recordingDescriptor_sbe_schema_version(),
                length);

            if (aeron_archive_client_recordingDescriptor_controlSessionId(&recording_descriptor) == poller->control_session_id &&
                aeron_archive_client_recordingDescriptor_correlationId(&recording_descriptor)== poller->correlation_id)
            {
                struct aeron_archive_client_recordingDescriptor_string_view view;

                aeron_archive_recording_descriptor_t descriptor;

                view = aeron_archive_client_recordingDescriptor_get_strippedChannel_as_string_view(&recording_descriptor);
                descriptor.stripped_channel_length = view.length;
                if (aeron_alloc((void **)&descriptor.stripped_channel, descriptor.stripped_channel_length + 1) < 0)
                {
                    AERON_APPEND_ERR("%s", "");
                    poller->error_on_fragment = true;
                    return AERON_ACTION_BREAK;
                }
                memcpy(descriptor.stripped_channel, view.data, descriptor.stripped_channel_length);
                descriptor.stripped_channel[descriptor.stripped_channel_length] = '\0';

                view = aeron_archive_client_recordingDescriptor_get_originalChannel_as_string_view(&recording_descriptor);
                descriptor.original_channel_length = view.length;
                if (aeron_alloc((void **)&descriptor.original_channel, descriptor.original_channel_length + 1) < 0)
                {
                    aeron_free(descriptor.stripped_channel);
                    AERON_APPEND_ERR("%s", "");
                    poller->error_on_fragment = true;
                    return AERON_ACTION_BREAK;
                }
                memcpy(descriptor.original_channel, view.data, descriptor.original_channel_length);
                descriptor.original_channel[descriptor.original_channel_length] = '\0';

                view = aeron_archive_client_recordingDescriptor_get_sourceIdentity_as_string_view(&recording_descriptor);
                descriptor.source_identity_length = view.length;
                if (aeron_alloc((void **)&descriptor.source_identity, descriptor.source_identity_length + 1) < 0)
                {
                    aeron_free(descriptor.stripped_channel);
                    aeron_free(descriptor.original_channel);
                    AERON_APPEND_ERR("%s", "");
                    poller->error_on_fragment = true;
                    return AERON_ACTION_BREAK;
                }
                memcpy(descriptor.source_identity, view.data, descriptor.source_identity_length);
                descriptor.source_identity[descriptor.source_identity_length] = '\0';

                descriptor.control_session_id = poller->control_session_id;
                descriptor.correlation_id = poller->correlation_id;
                descriptor.recording_id = aeron_archive_client_recordingDescriptor_recordingId(&recording_descriptor);
                descriptor.start_timestamp = aeron_archive_client_recordingDescriptor_startTimestamp(&recording_descriptor);
                descriptor.stop_timestamp = aeron_archive_client_recordingDescriptor_stopTimestamp(&recording_descriptor);
                descriptor.start_position = aeron_archive_client_recordingDescriptor_startPosition(&recording_descriptor);
                descriptor.stop_position = aeron_archive_client_recordingDescriptor_stopPosition(&recording_descriptor);
                descriptor.initial_term_id = aeron_archive_client_recordingDescriptor_initialTermId(&recording_descriptor);
                descriptor.segment_file_length = aeron_archive_client_recordingDescriptor_segmentFileLength(&recording_descriptor);
                descriptor.term_buffer_length = aeron_archive_client_recordingDescriptor_termBufferLength(&recording_descriptor);
                descriptor.mtu_length = aeron_archive_client_recordingDescriptor_mtuLength(&recording_descriptor);
                descriptor.session_id = aeron_archive_client_recordingDescriptor_sessionId(&recording_descriptor);
                descriptor.stream_id = aeron_archive_client_recordingDescriptor_streamId(&recording_descriptor);

                poller->recording_descriptor_consumer(
                    &descriptor,
                    poller->recording_descriptor_consumer_clientd);

                aeron_free(descriptor.stripped_channel);
                aeron_free(descriptor.original_channel);
                aeron_free(descriptor.source_identity);

                if (0 == --poller->remaining_record_count)
                {
                    poller->is_dispatch_complete = true;

                    return AERON_ACTION_BREAK;
                }
            }

            break;
        }

        case AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_EVENT_SBE_TEMPLATE_ID:
        {
            struct aeron_archive_client_controlResponse control_response;

            aeron_archive_client_controlResponse_wrap_for_decode(
                &control_response,
                (char *)buffer,
                aeron_archive_client_messageHeader_encoded_length(),
                aeron_archive_client_controlResponse_sbe_block_length(),
                aeron_archive_client_controlResponse_sbe_schema_version(),
                length);

            if (aeron_archive_client_controlResponse_controlSessionId(&control_response) == poller->control_session_id)
            {
                if (aeron_archive_recording_signal_dispatch_buffer(poller->ctx, buffer, length) < 0)
                {
                    AERON_APPEND_ERR("%s", "");
                    poller->error_on_fragment = true;
                    return AERON_ACTION_BREAK;
                }
            }

            break;
        }

        default:
            // do nothing
            break;
    }

    return AERON_ACTION_CONTINUE;
}
