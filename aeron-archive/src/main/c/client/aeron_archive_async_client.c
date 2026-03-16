/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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

#include <c/aeron_archive_client/controlResponse.h>
#include <c/aeron_archive_client/messageHeader.h>
#include <c/aeron_archive_client/recordingDescriptor.h>
#include <errno.h>


// added for debugging
#include <stdio.h>
#include <inttypes.h>

#include "aeron_alloc.h"
#include "aeron_archive_async_client.h"
#include "aeron_archive_async_connect.h"
#include "aeron_archive_client.h"
#include "aeron_archive_context.h"
#include "util/aeron_error.h"

typedef enum aeron_archive_async_client_state_en
{
    AERON_ARCHIVE_ASYNC_CLIENT_CONNECTING,
    AERON_ARCHIVE_ASYNC_CLIENT_CONNECTED,
    AERON_ARCHIVE_ASYNC_CLIENT_DISCONNECTED,
    AERON_ARCHIVE_ASYNC_CLIENT_CLOSED,
}
aeron_archive_async_client_state_t;

static int aeron_archive_async_client_connecting(aeron_archive_async_client_t *client);
static int aeron_archive_async_client_connected(aeron_archive_async_client_t *client);
static int aeron_archive_async_client_disconnected(aeron_archive_async_client_t *client);

struct aeron_archive_async_client_stct
{
    aeron_archive_async_client_state_t state;
    aeron_archive_context_t *context;
    aeron_archive_async_client_listener_t *listener;
    aeron_fragment_assembler_t *fragment_assembler;
    aeron_archive_async_connect_t *async_connect;
    aeron_archive_t *archive;
    char *error_message;
    size_t error_message_capacity;
};

static void poll_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header);

int aeron_archive_async_client_create(
    aeron_archive_async_client_t **client,
    aeron_archive_context_t *context,
    aeron_archive_async_client_listener_t *listener)
{
    if (NULL == client || NULL == context || NULL == listener)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, client: %s, context: %s, listener: %s",
            AERON_NULL_STR(client),
            AERON_NULL_STR(context),
            AERON_NULL_STR(listener));
        return -1;
    }

    aeron_archive_async_client_t *_client = NULL;

    if (aeron_alloc((void **)&_client, sizeof(aeron_archive_async_client_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to allocate aeron_archive_async_client_t");
        return -1;
    }

    if (aeron_fragment_assembler_create(&_client->fragment_assembler, poll_handler, _client) < 0)
    {
        AERON_APPEND_ERR("%s", "aeron_fragment_assembler_create failed");
        goto cleanup;
    }

    _client->error_message_capacity = 16; // TODO what's a good value?
    if (aeron_alloc((void **)&_client->error_message, _client->error_message_capacity) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to allocate error_message");
        goto cleanup;
    }

    _client->state = AERON_ARCHIVE_ASYNC_CLIENT_CONNECTING;
    _client->context = context;
    _client->listener = listener;

    *client = _client;
    return 0;

cleanup:
    aeron_fragment_assembler_delete(_client->fragment_assembler);
    aeron_free(_client);
    return -1;
}

int aeron_archive_async_client_close(aeron_archive_async_client_t *client)
{
    if (NULL != client)
    {
        if (NULL != client->archive)
        {
            aeron_archive_close(client->archive);
        }

        if (NULL != client->async_connect)
        {
            aeron_archive_async_connect_delete(client->async_connect);
        }

        aeron_free(client);
    }

    return 0;
}

int aeron_archive_async_client_poll(aeron_archive_async_client_t *client)
{
    switch (client->state)
    {
        case AERON_ARCHIVE_ASYNC_CLIENT_CONNECTING:
            return aeron_archive_async_client_connecting(client);
        case AERON_ARCHIVE_ASYNC_CLIENT_CONNECTED:
            return aeron_archive_async_client_connected(client);
        case AERON_ARCHIVE_ASYNC_CLIENT_DISCONNECTED:
            return aeron_archive_async_client_disconnected(client);
        default:
            return 0;
    }
}

static int aeron_archive_async_client_connecting(aeron_archive_async_client_t *client)
{
    int work_count = 0;

    static int64_t count = 0;
    static bool connect_failed = false;
    static bool poll_failed = false;

    if (++count == 1)
    {
        printf("async_client_connecting: first call, async_connect=%p, context->aeron=%p\n",
            (void *)client->async_connect,
            (void *)client->context->aeron);
        fflush(stdout);
    }

    if (NULL == client->async_connect)
    {
        bool had_aeron = client->context->aeron != NULL;

        if (aeron_archive_async_connect(&client->async_connect, client->context) < 0)
        {
            if (!connect_failed)
            {
                connect_failed = true;
                printf("async_client_connecting: aeron_archive_async_connect FAILED: %s\n",
                    aeron_errmsg());
                fflush(stdout);
            }
            AERON_APPEND_ERR("%s", "aeron_archive_async_connect failed");
            return -1;
        }

        connect_failed = false;
        printf("async_client_connecting: aeron_archive_async_connect succeeded, async_connect=%p\n",
            (void *)client->async_connect);
        fflush(stdout);

        bool has_aeron = client->context->aeron != NULL;
        if (has_aeron && !had_aeron)
        {
            printf("async_client_connecting: nulling context->aeron\n");
            fflush(stdout);
            client->context->aeron = NULL;
        }

        work_count += 1;
    }

    uint8_t step_before = aeron_archive_async_connect_step(client->async_connect);

    if (aeron_archive_async_connect_poll(&client->archive, client->async_connect) < 0)
    {
        if (!poll_failed)
        {
            poll_failed = true;
            printf("async_client_connecting: aeron_archive_async_connect_poll FAILED: %s\n",
                aeron_errmsg());
            fflush(stdout);
        }
        client->async_connect = NULL;
        return 1;
    }

    poll_failed = false;

    uint8_t step_after = aeron_archive_async_connect_step(client->async_connect);
    if (step_after != step_before)
    {
        printf("async_client_connecting: step %d -> %d\n", step_before, step_after);
        fflush(stdout);
    }
    work_count += (step_after != step_before) ? 1 : 0;

    if (NULL != client->archive)
    {
        printf("async_client_connecting: archive connected!\n");
        fflush(stdout);
        client->async_connect = NULL;
        client->state = AERON_ARCHIVE_ASYNC_CLIENT_CONNECTED;
        client->listener->on_connected(client->listener->clientd);
        return 1;
    }

    return work_count;
}

static int aeron_archive_async_client_connected(aeron_archive_async_client_t *client)
{
    aeron_subscription_t* subscription = client->archive->subscription;

    if (!aeron_subscription_is_connected(subscription))
    {
        client->state = AERON_ARCHIVE_ASYNC_CLIENT_DISCONNECTED;
        return 1;
    }

    return aeron_subscription_poll(subscription, aeron_fragment_assembler_handler, client->fragment_assembler, 10);
}

static int aeron_archive_async_client_disconnected(aeron_archive_async_client_t *client)
{
    aeron_archive_close(client->archive);
    client->archive = NULL;
    client->listener->on_disconnected(client->listener->clientd);
    client->state = AERON_ARCHIVE_ASYNC_CLIENT_CONNECTING;
    return 1;
}

bool aeron_archive_async_client_is_connected(aeron_archive_async_client_t *client)
{
    return client->state == AERON_ARCHIVE_ASYNC_CLIENT_CONNECTED;
}

bool aeron_archive_async_client_try_send_list_recording_request(
    aeron_archive_async_client_t *client,
    int64_t correlation_id,
    int64_t recording_id)
{
    if (client->state == AERON_ARCHIVE_ASYNC_CLIENT_CONNECTED)
    {
        if (aeron_archive_proxy_list_recording(client->archive->archive_proxy, correlation_id, recording_id))
        {
            return true;
        }

        if (ENOTCONN == aeron_errcode())
        {
            client->state = AERON_ARCHIVE_ASYNC_CLIENT_DISCONNECTED;
        }
    }

    return false;
}

bool aeron_archive_async_client_try_send_max_recorded_position_request(
    aeron_archive_async_client_t *client,
    int64_t correlation_id,
    int64_t recording_id)
{
    if (client->state == AERON_ARCHIVE_ASYNC_CLIENT_CONNECTED)
    {
        if (aeron_archive_proxy_get_max_recorded_position(
            client->archive->archive_proxy,
            correlation_id,
            recording_id))
        {
            return true;
        }

        if (ENOTCONN == aeron_errcode())
        {
            client->state = AERON_ARCHIVE_ASYNC_CLIENT_DISCONNECTED;
        }
    }

    return false;
}

bool aeron_archive_async_client_try_send_replay_request(
    aeron_archive_async_client_t *client,
    int64_t correlation_id,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params)
{
    if (client->state == AERON_ARCHIVE_ASYNC_CLIENT_CONNECTED)
    {
        if (aeron_archive_proxy_replay(
            client->archive->archive_proxy,
            correlation_id,
            recording_id,
            replay_channel,
            replay_stream_id,
            params))
        {
            return true;
        }

        if (ENOTCONN == aeron_errcode())
        {
            client->state = AERON_ARCHIVE_ASYNC_CLIENT_DISCONNECTED;
        }
    }

    return false;
}

bool aeron_archive_async_client_try_send_stop_replay_request(
    aeron_archive_async_client_t *client,
    int64_t correlation_id,
    int64_t replay_session_id)
{
    if (client->state == AERON_ARCHIVE_ASYNC_CLIENT_CONNECTED)
    {
        if (aeron_archive_proxy_stop_replay(client->archive->archive_proxy, correlation_id, replay_session_id))
        {
            return true;
        }

        if (ENOTCONN == aeron_errcode())
        {
            client->state = AERON_ARCHIVE_ASYNC_CLIENT_DISCONNECTED;
        }
    }

    return false;
}

static void poll_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    aeron_archive_async_client_t *client = (aeron_archive_async_client_t *)clientd;

    // TODO what to do with errors here?

    struct aeron_archive_client_messageHeader hdr;
    if (aeron_archive_client_messageHeader_wrap(
        &hdr,
        (char *)buffer,
        0,
        aeron_archive_client_messageHeader_sbe_schema_version(),
        length) == NULL)
    {
        // AERON_SET_ERR(errno, "%s", "unable to wrap buffer");
        // poller->error_on_fragment = true;
        return;
    }

    uint16_t block_length = aeron_archive_client_messageHeader_blockLength(&hdr);
    uint16_t template_id = aeron_archive_client_messageHeader_templateId(&hdr);
    uint16_t schema_id = aeron_archive_client_messageHeader_schemaId(&hdr);
    uint16_t version = aeron_archive_client_messageHeader_version(&hdr);

    if (schema_id != aeron_archive_client_messageHeader_sbe_schema_id())
    {
        // AERON_SET_ERR(-1, "found schema id: %i that doesn't match expected id: %i", schema_id, aeron_archive_client_messageHeader_sbe_schema_id());
        // poller->error_on_fragment = true;
        return;
    }

    switch (template_id)
    {
        case AERON_ARCHIVE_CLIENT_CONTROL_RESPONSE_SBE_TEMPLATE_ID:
        {
            struct aeron_archive_client_controlResponse control_response;
            aeron_archive_client_controlResponse_wrap_for_decode(
                &control_response,
                (char *)buffer,
                aeron_archive_client_messageHeader_encoded_length(),
                block_length,
                version,
                length);

            int64_t control_session_id = aeron_archive_client_controlResponse_controlSessionId(&control_response);
            if (control_session_id == client->archive->control_session_id)
            {
                int64_t correlation_id = aeron_archive_client_controlResponse_correlationId(&control_response);
                int64_t relevant_id = aeron_archive_client_controlResponse_relevantId(&control_response);

                int32_t code;
                if (!aeron_archive_client_controlResponse_code(
                    &control_response,
                    (enum aeron_archive_client_controlResponseCode *)&code))
                {
                    // AERON_SET_ERR(-1, "%s", "unable to read control response code");
                    // poller->error_on_fragment = true;
                    return;
                }

                uint32_t error_message_len = aeron_archive_client_controlResponse_errorMessage_length(&control_response);
                uint32_t len_with_terminator = error_message_len + 1;
                if (len_with_terminator > client->error_message_capacity)
                {
                    if (aeron_reallocf((void **)&client->error_message, len_with_terminator) < 0)
                    {
                        // AERON_APPEND_ERR("%s", "failed to reallocate error_message");
                        // poller->error_on_fragment = true;
                        return;
                    }
                    client->error_message_capacity = len_with_terminator;
                }

                aeron_archive_client_controlResponse_get_errorMessage(
                    &control_response,
                    client->error_message,
                    error_message_len);
                client->error_message[error_message_len] = '\0';

                client->listener->on_control_response(
                    client->listener->clientd,
                    correlation_id,
                    relevant_id,
                    code,
                    client->error_message);
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
                block_length,
                version,
                length);

            int64_t control_session_id = aeron_archive_client_recordingDescriptor_controlSessionId(&recording_descriptor);
            if (control_session_id == client->archive->control_session_id)
            {
                struct aeron_archive_client_recordingDescriptor_string_view view;

                aeron_archive_recording_descriptor_t descriptor;

                view = aeron_archive_client_recordingDescriptor_get_strippedChannel_as_string_view(&recording_descriptor);
                descriptor.stripped_channel_length = view.length;
                if (aeron_alloc((void **)&descriptor.stripped_channel, descriptor.stripped_channel_length + 1) < 0)
                {
                    // AERON_APPEND_ERR("%s", "");
                    // poller->error_on_fragment = true;
                    return;
                }
                memcpy(descriptor.stripped_channel, view.data, descriptor.stripped_channel_length);
                descriptor.stripped_channel[descriptor.stripped_channel_length] = '\0';

                view = aeron_archive_client_recordingDescriptor_get_originalChannel_as_string_view(&recording_descriptor);
                descriptor.original_channel_length = view.length;
                if (aeron_alloc((void **)&descriptor.original_channel, descriptor.original_channel_length + 1) < 0)
                {
                    aeron_free(descriptor.stripped_channel);
                    // AERON_APPEND_ERR("%s", "");
                    // poller->error_on_fragment = true;
                    return;
                }
                memcpy(descriptor.original_channel, view.data, descriptor.original_channel_length);
                descriptor.original_channel[descriptor.original_channel_length] = '\0';

                view = aeron_archive_client_recordingDescriptor_get_sourceIdentity_as_string_view(&recording_descriptor);
                descriptor.source_identity_length = view.length;
                if (aeron_alloc((void **)&descriptor.source_identity, descriptor.source_identity_length + 1) < 0)
                {
                    aeron_free(descriptor.stripped_channel);
                    aeron_free(descriptor.original_channel);
                    // AERON_APPEND_ERR("%s", "");
                    // poller->error_on_fragment = true;
                    return;
                }
                memcpy(descriptor.source_identity, view.data, descriptor.source_identity_length);
                descriptor.source_identity[descriptor.source_identity_length] = '\0';

                descriptor.control_session_id = control_session_id;
                descriptor.correlation_id = aeron_archive_client_recordingDescriptor_correlationId(&recording_descriptor);
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

                client->listener->on_recording_descriptor(client->listener->clientd, &descriptor);

                aeron_free(descriptor.stripped_channel);
                aeron_free(descriptor.original_channel);
                aeron_free(descriptor.source_identity);

                break;
            }
        }
    }
}
