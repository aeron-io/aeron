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
#include <stdio.h>

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "aeron_archive_control_session_adapter.h"
#include "aeron_archive_conductor.h"

/*
 * Inline SBE decoding helpers.
 *
 * SBE message header (8 bytes, little-endian):
 *   offset 0: uint16 blockLength
 *   offset 2: uint16 templateId
 *   offset 4: uint16 schemaId
 *   offset 6: uint16 version
 *
 * Variable-length data fields are encoded as:
 *   uint32 length (little-endian) followed by `length` bytes of data.
 */

#define SBE_HEADER_LENGTH (8)

static inline uint16_t sbe_decode_uint16(const uint8_t *buffer, size_t offset)
{
    uint16_t value;
    memcpy(&value, buffer + offset, sizeof(uint16_t));
    return value;
}

static inline int32_t sbe_decode_int32(const uint8_t *buffer, size_t offset)
{
    int32_t value;
    memcpy(&value, buffer + offset, sizeof(int32_t));
    return value;
}

static inline int64_t sbe_decode_int64(const uint8_t *buffer, size_t offset)
{
    int64_t value;
    memcpy(&value, buffer + offset, sizeof(int64_t));
    return value;
}

static inline uint32_t sbe_decode_var_data_length(const uint8_t *buffer, size_t offset)
{
    uint32_t length;
    memcpy(&length, buffer + offset, sizeof(uint32_t));
    return length;
}

static inline const char *sbe_decode_var_data_ptr(const uint8_t *buffer, size_t offset)
{
    return (const char *)(buffer + offset + sizeof(uint32_t));
}

/**
 * Skip one var-data field and return the total bytes consumed (4-byte length header + data).
 */

/* Session map helpers - simple linked list */

static aeron_archive_control_session_info_t *find_session_info(
    aeron_archive_control_session_adapter_t *adapter,
    int64_t control_session_id)
{
    aeron_archive_control_session_info_t *info = adapter->session_map_head;
    while (NULL != info)
    {
        if (info->control_session_id == control_session_id)
        {
            return info;
        }
        info = info->next;
    }
    return NULL;
}

static aeron_archive_control_session_t *get_control_session(
    aeron_archive_control_session_adapter_t *adapter,
    int64_t correlation_id,
    int64_t control_session_id,
    uint16_t template_id)
{
    aeron_archive_control_session_info_t *info = find_session_info(adapter, control_session_id);
    if (NULL != info)
    {
        return info->control_session;
    }
    return NULL;
}

/* Fragment handler */

static void aeron_archive_control_session_adapter_on_fragment(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header);

int aeron_archive_control_session_adapter_create(
    aeron_archive_control_session_adapter_t **adapter,
    aeron_subscription_t *control_subscription,
    aeron_subscription_t *local_control_subscription,
    aeron_archive_conductor_t *conductor)
{
    aeron_archive_control_session_adapter_t *_adapter = NULL;

    if (aeron_alloc((void **)&_adapter, sizeof(aeron_archive_control_session_adapter_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_control_session_adapter_t");
        return -1;
    }

    memset(_adapter, 0, sizeof(aeron_archive_control_session_adapter_t));
    _adapter->control_subscription = control_subscription;
    _adapter->local_control_subscription = local_control_subscription;
    _adapter->conductor = conductor;
    _adapter->session_map_head = NULL;

    if (aeron_fragment_assembler_create(
        &_adapter->fragment_assembler,
        aeron_archive_control_session_adapter_on_fragment,
        _adapter) < 0)
    {
        aeron_free(_adapter);
        AERON_APPEND_ERR("%s", "aeron_fragment_assembler_create");
        return -1;
    }

    *adapter = _adapter;

    return 0;
}

int aeron_archive_control_session_adapter_poll(
    aeron_archive_control_session_adapter_t *adapter)
{
    int fragments_read = 0;

    if (NULL != adapter->control_subscription)
    {
        fragments_read += aeron_subscription_poll(
            adapter->control_subscription,
            aeron_fragment_assembler_handler,
            adapter->fragment_assembler,
            AERON_ARCHIVE_CONTROL_SESSION_ADAPTER_FRAGMENT_LIMIT);
    }

    if (NULL != adapter->local_control_subscription)
    {
        fragments_read += aeron_subscription_poll(
            adapter->local_control_subscription,
            aeron_fragment_assembler_handler,
            adapter->fragment_assembler,
            AERON_ARCHIVE_CONTROL_SESSION_ADAPTER_FRAGMENT_LIMIT);
    }

    return fragments_read;
}

void aeron_archive_control_session_adapter_add_session(
    aeron_archive_control_session_adapter_t *adapter,
    aeron_archive_control_session_t *session,
    aeron_image_t *image)
{
    aeron_archive_control_session_info_t *info = NULL;
    if (aeron_alloc((void **)&info, sizeof(aeron_archive_control_session_info_t)) < 0)
    {
        return;
    }

    info->control_session_id = aeron_archive_control_session_id(session);
    info->control_session = session;
    info->image = image;
    info->next = adapter->session_map_head;
    adapter->session_map_head = info;
}

void aeron_archive_control_session_adapter_remove_session(
    aeron_archive_control_session_adapter_t *adapter,
    int64_t control_session_id,
    bool session_aborted,
    const char *abort_reason)
{
    aeron_archive_control_session_info_t **prev_ptr = &adapter->session_map_head;
    aeron_archive_control_session_info_t *info = adapter->session_map_head;

    while (NULL != info)
    {
        if (info->control_session_id == control_session_id)
        {
            *prev_ptr = info->next;
            aeron_free(info);
            return;
        }
        prev_ptr = &info->next;
        info = info->next;
    }
}

void aeron_archive_control_session_adapter_abort_by_image(
    aeron_archive_control_session_adapter_t *adapter,
    aeron_image_t *image)
{
    aeron_archive_control_session_info_t *info = adapter->session_map_head;
    while (NULL != info)
    {
        if (info->image == image && !aeron_archive_control_session_is_done(info->control_session))
        {
            aeron_archive_control_session_abort(
                info->control_session,
                "control request publication image unavailable");
            break;
        }
        info = info->next;
    }
}

int aeron_archive_control_session_adapter_close(
    aeron_archive_control_session_adapter_t *adapter)
{
    if (NULL == adapter)
    {
        return 0;
    }

    if (NULL != adapter->fragment_assembler)
    {
        aeron_fragment_assembler_delete(adapter->fragment_assembler);
        adapter->fragment_assembler = NULL;
    }

    /* Free the session map entries (but not the sessions themselves - conductor owns those). */
    aeron_archive_control_session_info_t *info = adapter->session_map_head;
    while (NULL != info)
    {
        aeron_archive_control_session_info_t *next = info->next;
        aeron_free(info);
        info = next;
    }

    aeron_free(adapter);

    return 0;
}

/*
 * The main fragment handler. Decodes the SBE message header, then dispatches
 * to the appropriate request handler based on the template id.
 *
 * This mirrors Java's ControlSessionAdapter.onFragment().
 */
static void aeron_archive_control_session_adapter_on_fragment(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    aeron_archive_control_session_adapter_t *adapter =
        (aeron_archive_control_session_adapter_t *)clientd;


    if (length < SBE_HEADER_LENGTH)
    {
        return;
    }

    const uint16_t block_length = sbe_decode_uint16(buffer, 0);
    const uint16_t template_id = sbe_decode_uint16(buffer, 2);
    const uint16_t schema_id = sbe_decode_uint16(buffer, 4);
    const uint16_t version = sbe_decode_uint16(buffer, 6);

    if (schema_id != AERON_ARCHIVE_CONTROL_SCHEMA_ID)
    {
        return;
    }

    const uint8_t *body = buffer + SBE_HEADER_LENGTH;
    const size_t body_length = length - SBE_HEADER_LENGTH;

    (void)block_length;
    (void)version;

    switch (template_id)
    {
        case AERON_ARCHIVE_AUTH_CONNECT_REQUEST_TEMPLATE_ID:
        {
            /*
             * AuthConnectRequest:
             *   correlationId(8) + responseStreamId(4) + version(4) = 16 bytes fixed
             *   var: responseChannel, encodedCredentials, clientInfo
             */
            if (body_length < 16)
            {
                return;
            }

            const int64_t correlation_id = sbe_decode_int64(body, 0);
            const int32_t response_stream_id = sbe_decode_int32(body, 8);
            const int32_t client_version = sbe_decode_int32(body, 12);

            size_t var_offset = block_length;
            const uint32_t response_channel_len = sbe_decode_var_data_length(body, var_offset);
            const char *response_channel_raw = sbe_decode_var_data_ptr(body, var_offset);
            var_offset += sizeof(uint32_t) + response_channel_len;

            /* Decode encodedCredentials */
            const uint32_t credentials_len = sbe_decode_var_data_length(body, var_offset);
            const uint8_t *credentials_data = (const uint8_t *)sbe_decode_var_data_ptr(body, var_offset);
            var_offset += sizeof(uint32_t) + credentials_len;

            /* null-terminate the response channel */
            char response_channel_buf[4096];
            size_t copy_len = response_channel_len < sizeof(response_channel_buf) - 1 ?
                response_channel_len : sizeof(response_channel_buf) - 1;
            memcpy(response_channel_buf, response_channel_raw, copy_len);
            response_channel_buf[copy_len] = '\0';

            /* Get the image from the header context */
            aeron_image_t *image = NULL;
            if (NULL != header)
            {
                aeron_header_values_t header_values;
                if (aeron_header_values(header, &header_values) == 0)
                {
                    /* Image context not directly available from header in C API;
                     * pass NULL for now - used only for diagnostics. */
                }
            }

            aeron_archive_control_session_t *session = aeron_archive_conductor_new_control_session(
                adapter->conductor,
                correlation_id,
                response_stream_id,
                client_version,
                response_channel_buf,
                credentials_data,
                credentials_len,
                adapter,
                image);

            if (NULL != session)
            {
                aeron_archive_control_session_adapter_add_session(adapter, session, image);
            }
            break;
        }

        case AERON_ARCHIVE_CONNECT_REQUEST_TEMPLATE_ID:
        {
            /*
             * ConnectRequest:
             *   correlationId(8) + responseStreamId(4) + version(4) = 16 bytes fixed
             *   var: responseChannel
             */
            if (body_length < 16)
            {
                return;
            }

            const int64_t correlation_id = sbe_decode_int64(body, 0);
            const int32_t response_stream_id = sbe_decode_int32(body, 8);
            const int32_t client_version = sbe_decode_int32(body, 12);

            size_t var_offset = block_length;
            const uint32_t response_channel_len = sbe_decode_var_data_length(body, var_offset);
            const char *response_channel_raw = sbe_decode_var_data_ptr(body, var_offset);

            /* null-terminate the response channel */
            char response_channel_buf[4096];
            size_t copy_len = response_channel_len < sizeof(response_channel_buf) - 1 ?
                response_channel_len : sizeof(response_channel_buf) - 1;
            memcpy(response_channel_buf, response_channel_raw, copy_len);
            response_channel_buf[copy_len] = '\0';

            aeron_image_t *image = NULL;

            aeron_archive_control_session_t *session = aeron_archive_conductor_new_control_session(
                adapter->conductor,
                correlation_id,
                response_stream_id,
                client_version,
                response_channel_buf,
                NULL,
                0,
                adapter,
                image);

            if (NULL != session)
            {
                aeron_archive_control_session_adapter_add_session(adapter, session, image);
            }
            break;
        }

        case AERON_ARCHIVE_CLOSE_SESSION_REQUEST_TEMPLATE_ID:
        {
            /* CloseSessionRequest: controlSessionId(8) = 8 bytes */
            if (body_length < 8)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            aeron_archive_control_session_info_t *info = find_session_info(adapter, control_session_id);
            if (NULL != info)
            {
                aeron_archive_control_session_abort(info->control_session, "session closed");
            }
            break;
        }

        case AERON_ARCHIVE_START_RECORDING_REQUEST_TEMPLATE_ID:
        {
            /*
             * StartRecordingRequest:
             *   controlSessionId(8) + correlationId(8) + streamId(4) + sourceLocation(4) = 24 bytes
             *   var: channel
             */
            if (body_length < 24)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int32_t stream_id = sbe_decode_int32(body, 16);
            const int32_t source_location = sbe_decode_int32(body, 20);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                size_t var_offset = block_length;
                const uint32_t channel_len = sbe_decode_var_data_length(body, var_offset);
                const char *channel = sbe_decode_var_data_ptr(body, var_offset);

                /* Need to null-terminate for C string usage */
                char channel_buf[4096];
                size_t copy_len = channel_len < sizeof(channel_buf) - 1 ? channel_len : sizeof(channel_buf) - 1;
                memcpy(channel_buf, channel, copy_len);
                channel_buf[copy_len] = '\0';

                aeron_archive_control_session_on_start_recording(
                    session, correlation_id, stream_id, source_location, false, channel_buf);
            }
            break;
        }

        case AERON_ARCHIVE_START_RECORDING_REQUEST2_TEMPLATE_ID:
        {
            /*
             * StartRecordingRequest2:
             *   controlSessionId(8) + correlationId(8) + streamId(4) + sourceLocation(4) + autoStop(4) = 28 bytes
             *   var: channel
             */
            if (body_length < 28)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int32_t stream_id = sbe_decode_int32(body, 16);
            const int32_t source_location = sbe_decode_int32(body, 20);
            const int32_t auto_stop_val = sbe_decode_int32(body, 24);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                size_t var_offset = block_length;
                const uint32_t channel_len = sbe_decode_var_data_length(body, var_offset);
                const char *channel = sbe_decode_var_data_ptr(body, var_offset);

                char channel_buf[4096];
                size_t copy_len = channel_len < sizeof(channel_buf) - 1 ? channel_len : sizeof(channel_buf) - 1;
                memcpy(channel_buf, channel, copy_len);
                channel_buf[copy_len] = '\0';

                aeron_archive_control_session_on_start_recording(
                    session, correlation_id, stream_id, source_location, auto_stop_val == 1, channel_buf);
            }
            break;
        }

        case AERON_ARCHIVE_STOP_RECORDING_REQUEST_TEMPLATE_ID:
        {
            /*
             * StopRecordingRequest:
             *   controlSessionId(8) + correlationId(8) + streamId(4) = 20 bytes (block may be padded to 20)
             *   var: channel
             */
            if (body_length < 20)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int32_t stream_id = sbe_decode_int32(body, 16);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                size_t var_offset = block_length;
                const uint32_t channel_len = sbe_decode_var_data_length(body, var_offset);
                const char *channel = sbe_decode_var_data_ptr(body, var_offset);

                char channel_buf[4096];
                size_t copy_len = channel_len < sizeof(channel_buf) - 1 ? channel_len : sizeof(channel_buf) - 1;
                memcpy(channel_buf, channel, copy_len);
                channel_buf[copy_len] = '\0';

                aeron_archive_control_session_on_stop_recording(session, correlation_id, stream_id, channel_buf);
            }
            break;
        }

        case AERON_ARCHIVE_REPLAY_REQUEST_TEMPLATE_ID:
        {
            /*
             * ReplayRequest:
             *   controlSessionId(8) + correlationId(8) + recordingId(8) + position(8) +
             *   length(8) + replayStreamId(4) + fileIoMaxLength(4) + replayToken(8) = 56 bytes
             *   var: replayChannel
             */
            if (body_length < 40) /* minimum without sinceVersion fields */
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t recording_id = sbe_decode_int64(body, 16);
            const int64_t position = sbe_decode_int64(body, 24);
            const int64_t replay_length = sbe_decode_int64(body, 32);
            const int32_t replay_stream_id = sbe_decode_int32(body, 40);
            const int32_t file_io_max_length = (block_length > 44) ?
                sbe_decode_int32(body, 44) : (int32_t)AERON_NULL_VALUE;

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                size_t var_offset = block_length;
                const uint32_t channel_len = sbe_decode_var_data_length(body, var_offset);
                const char *channel = sbe_decode_var_data_ptr(body, var_offset);

                char channel_buf[4096];
                size_t copy_len = channel_len < sizeof(channel_buf) - 1 ? channel_len : sizeof(channel_buf) - 1;
                memcpy(channel_buf, channel, copy_len);
                channel_buf[copy_len] = '\0';

                aeron_archive_control_session_on_start_replay(
                    session, correlation_id, recording_id, position, replay_length,
                    file_io_max_length, replay_stream_id, channel_buf);
            }
            break;
        }

        case AERON_ARCHIVE_STOP_REPLAY_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + replaySessionId(8) = 24 */
            if (body_length < 24)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t replay_session_id = sbe_decode_int64(body, 16);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_stop_replay(session, correlation_id, replay_session_id);
            }
            break;
        }

        case AERON_ARCHIVE_LIST_RECORDINGS_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + fromRecordingId(8) + recordCount(4) = 28 */
            if (body_length < 28)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t from_recording_id = sbe_decode_int64(body, 16);
            const int32_t record_count = sbe_decode_int32(body, 24);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_list_recordings(
                    session, correlation_id, from_recording_id, record_count);
            }
            break;
        }

        case AERON_ARCHIVE_LIST_RECORDINGS_FOR_URI_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + fromRecordingId(8) + recordCount(4) + streamId(4) = 32 */
            if (body_length < 32)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t from_recording_id = sbe_decode_int64(body, 16);
            const int32_t record_count = sbe_decode_int32(body, 24);
            const int32_t stream_id = sbe_decode_int32(body, 28);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                size_t var_offset = block_length;
                const uint32_t channel_len = sbe_decode_var_data_length(body, var_offset);
                const uint8_t *channel_fragment = (const uint8_t *)sbe_decode_var_data_ptr(body, var_offset);

                aeron_archive_control_session_on_list_recordings_for_uri(
                    session, correlation_id, from_recording_id, record_count, stream_id,
                    channel_fragment, channel_len);
            }
            break;
        }

        case AERON_ARCHIVE_LIST_RECORDING_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + recordingId(8) = 24 */
            if (body_length < 24)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t recording_id = sbe_decode_int64(body, 16);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_list_recording(session, correlation_id, recording_id);
            }
            break;
        }

        case AERON_ARCHIVE_EXTEND_RECORDING_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + recordingId(8) + streamId(4) + sourceLocation(4) = 32 */
            if (body_length < 32)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t recording_id = sbe_decode_int64(body, 16);
            const int32_t stream_id = sbe_decode_int32(body, 24);
            const int32_t source_location = sbe_decode_int32(body, 28);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                size_t var_offset = block_length;
                const uint32_t channel_len = sbe_decode_var_data_length(body, var_offset);
                const char *channel = sbe_decode_var_data_ptr(body, var_offset);

                char channel_buf[4096];
                size_t copy_len = channel_len < sizeof(channel_buf) - 1 ? channel_len : sizeof(channel_buf) - 1;
                memcpy(channel_buf, channel, copy_len);
                channel_buf[copy_len] = '\0';

                aeron_archive_control_session_on_extend_recording(
                    session, correlation_id, recording_id, stream_id, source_location, false, channel_buf);
            }
            break;
        }

        case AERON_ARCHIVE_EXTEND_RECORDING_REQUEST2_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + recordingId(8) + streamId(4) + sourceLocation(4) + autoStop(4) = 36 */
            if (body_length < 36)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t recording_id = sbe_decode_int64(body, 16);
            const int32_t stream_id = sbe_decode_int32(body, 24);
            const int32_t source_location = sbe_decode_int32(body, 28);
            const int32_t auto_stop_val = sbe_decode_int32(body, 32);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                size_t var_offset = block_length;
                const uint32_t channel_len = sbe_decode_var_data_length(body, var_offset);
                const char *channel = sbe_decode_var_data_ptr(body, var_offset);

                char channel_buf[4096];
                size_t copy_len = channel_len < sizeof(channel_buf) - 1 ? channel_len : sizeof(channel_buf) - 1;
                memcpy(channel_buf, channel, copy_len);
                channel_buf[copy_len] = '\0';

                aeron_archive_control_session_on_extend_recording(
                    session, correlation_id, recording_id, stream_id, source_location,
                    auto_stop_val == 1, channel_buf);
            }
            break;
        }

        case AERON_ARCHIVE_RECORDING_POSITION_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + recordingId(8) = 24 */
            if (body_length < 24)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t recording_id = sbe_decode_int64(body, 16);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_get_recording_position(session, correlation_id, recording_id);
            }
            break;
        }

        case AERON_ARCHIVE_TRUNCATE_RECORDING_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + recordingId(8) + position(8) = 32 */
            if (body_length < 32)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t recording_id = sbe_decode_int64(body, 16);
            const int64_t position = sbe_decode_int64(body, 24);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_truncate_recording(
                    session, correlation_id, recording_id, position);
            }
            break;
        }

        case AERON_ARCHIVE_STOP_RECORDING_SUBSCRIPTION_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + subscriptionId(8) = 24 */
            if (body_length < 24)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t subscription_id = sbe_decode_int64(body, 16);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_stop_recording_subscription(
                    session, correlation_id, subscription_id);
            }
            break;
        }

        case AERON_ARCHIVE_STOP_POSITION_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + recordingId(8) = 24 */
            if (body_length < 24)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t recording_id = sbe_decode_int64(body, 16);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_get_stop_position(session, correlation_id, recording_id);
            }
            break;
        }

        case AERON_ARCHIVE_FIND_LAST_MATCHING_RECORDING_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + minRecordingId(8) + sessionId(4) + streamId(4) = 32 */
            if (body_length < 32)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t min_recording_id = sbe_decode_int64(body, 16);
            const int32_t session_id = sbe_decode_int32(body, 24);
            const int32_t stream_id = sbe_decode_int32(body, 28);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                size_t var_offset = block_length;
                const uint32_t channel_len = sbe_decode_var_data_length(body, var_offset);
                const uint8_t *channel_fragment = (const uint8_t *)sbe_decode_var_data_ptr(body, var_offset);

                aeron_archive_control_session_on_find_last_matching_recording(
                    session, correlation_id, min_recording_id, session_id, stream_id,
                    channel_fragment, channel_len);
            }
            break;
        }

        case AERON_ARCHIVE_LIST_RECORDING_SUBSCRIPTIONS_REQUEST_TEMPLATE_ID:
        {
            /*
             * controlSessionId(8) + correlationId(8) + pseudoIndex(4) + subscriptionCount(4) +
             * applyStreamId(4) + streamId(4) = 32
             * var: channel
             */
            if (body_length < 32)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int32_t pseudo_index = sbe_decode_int32(body, 16);
            const int32_t subscription_count = sbe_decode_int32(body, 20);
            const int32_t apply_stream_id_val = sbe_decode_int32(body, 24);
            const int32_t stream_id = sbe_decode_int32(body, 28);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                size_t var_offset = block_length;
                const uint32_t channel_len = sbe_decode_var_data_length(body, var_offset);
                const char *channel = sbe_decode_var_data_ptr(body, var_offset);

                char channel_buf[4096];
                size_t copy_len = channel_len < sizeof(channel_buf) - 1 ? channel_len : sizeof(channel_buf) - 1;
                memcpy(channel_buf, channel, copy_len);
                channel_buf[copy_len] = '\0';

                aeron_archive_control_session_on_list_recording_subscriptions(
                    session, correlation_id, pseudo_index, subscription_count,
                    apply_stream_id_val == 1, stream_id, channel_buf);
            }
            break;
        }

        case AERON_ARCHIVE_BOUNDED_REPLAY_REQUEST_TEMPLATE_ID:
        {
            /*
             * BoundedReplayRequest:
             *   controlSessionId(8) + correlationId(8) + recordingId(8) + position(8) +
             *   length(8) + limitCounterId(4) + replayStreamId(4) + fileIoMaxLength(4) + replayToken(8) = 60
             *   var: replayChannel
             */
            if (body_length < 48) /* minimum without sinceVersion fields */
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t recording_id = sbe_decode_int64(body, 16);
            const int64_t position = sbe_decode_int64(body, 24);
            const int64_t replay_length = sbe_decode_int64(body, 32);
            const int32_t limit_counter_id = sbe_decode_int32(body, 40);
            const int32_t replay_stream_id = sbe_decode_int32(body, 44);
            const int32_t file_io_max_length = (block_length > 48) ?
                sbe_decode_int32(body, 48) : (int32_t)AERON_NULL_VALUE;

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                size_t var_offset = block_length;
                const uint32_t channel_len = sbe_decode_var_data_length(body, var_offset);
                const char *channel = sbe_decode_var_data_ptr(body, var_offset);

                char channel_buf[4096];
                size_t copy_len = channel_len < sizeof(channel_buf) - 1 ? channel_len : sizeof(channel_buf) - 1;
                memcpy(channel_buf, channel, copy_len);
                channel_buf[copy_len] = '\0';

                aeron_archive_control_session_on_start_bounded_replay(
                    session, correlation_id, recording_id, position, replay_length,
                    limit_counter_id, file_io_max_length, replay_stream_id, channel_buf);
            }
            break;
        }

        case AERON_ARCHIVE_STOP_ALL_REPLAYS_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + recordingId(8) = 24 */
            if (body_length < 24)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t recording_id = sbe_decode_int64(body, 16);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_stop_all_replays(session, correlation_id, recording_id);
            }
            break;
        }

        case AERON_ARCHIVE_REPLICATE_REQUEST_TEMPLATE_ID:
        {
            /*
             * ReplicateRequest:
             *   controlSessionId(8) + correlationId(8) + srcRecordingId(8) + dstRecordingId(8) +
             *   srcControlStreamId(4) = 36
             *   var: srcControlChannel, liveDestination
             */
            if (body_length < 36)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t src_recording_id = sbe_decode_int64(body, 16);
            const int64_t dst_recording_id = sbe_decode_int64(body, 24);
            const int32_t src_control_stream_id = sbe_decode_int32(body, 32);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                size_t var_offset = block_length;
                const uint32_t src_channel_len = sbe_decode_var_data_length(body, var_offset);
                const char *src_channel = sbe_decode_var_data_ptr(body, var_offset);

                char src_channel_buf[4096];
                size_t copy_len = src_channel_len < sizeof(src_channel_buf) - 1 ?
                    src_channel_len : sizeof(src_channel_buf) - 1;
                memcpy(src_channel_buf, src_channel, copy_len);
                src_channel_buf[copy_len] = '\0';
                var_offset += sizeof(uint32_t) + src_channel_len;

                const uint32_t live_dest_len = sbe_decode_var_data_length(body, var_offset);
                const char *live_dest = sbe_decode_var_data_ptr(body, var_offset);

                char live_dest_buf[4096];
                copy_len = live_dest_len < sizeof(live_dest_buf) - 1 ?
                    live_dest_len : sizeof(live_dest_buf) - 1;
                memcpy(live_dest_buf, live_dest, copy_len);
                live_dest_buf[copy_len] = '\0';

                aeron_archive_control_session_on_replicate(
                    session, correlation_id, src_recording_id, dst_recording_id,
                    AERON_NULL_VALUE, /* stopPosition */
                    AERON_NULL_VALUE, /* channelTagId */
                    AERON_NULL_VALUE, /* subscriptionTagId */
                    src_control_stream_id,
                    (int32_t)AERON_NULL_VALUE, /* fileIoMaxLength */
                    (int32_t)AERON_NULL_VALUE, /* replicationSessionId */
                    src_channel_buf,
                    live_dest_buf,
                    "", /* replicationChannel */
                    NULL, 0, /* encodedCredentials */
                    ""); /* srcResponseChannel */
            }
            break;
        }

        case AERON_ARCHIVE_STOP_REPLICATION_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + replicationId(8) = 24 */
            if (body_length < 24)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t replication_id = sbe_decode_int64(body, 16);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_stop_replication(session, correlation_id, replication_id);
            }
            break;
        }

        case AERON_ARCHIVE_START_POSITION_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + recordingId(8) = 24 */
            if (body_length < 24)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t recording_id = sbe_decode_int64(body, 16);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_get_start_position(session, correlation_id, recording_id);
            }
            break;
        }

        case AERON_ARCHIVE_DETACH_SEGMENTS_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + recordingId(8) + newStartPosition(8) = 32 */
            if (body_length < 32)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t recording_id = sbe_decode_int64(body, 16);
            const int64_t new_start_position = sbe_decode_int64(body, 24);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_detach_segments(
                    session, correlation_id, recording_id, new_start_position);
            }
            break;
        }

        case AERON_ARCHIVE_DELETE_DETACHED_SEGMENTS_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + recordingId(8) = 24 */
            if (body_length < 24)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t recording_id = sbe_decode_int64(body, 16);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_delete_detached_segments(
                    session, correlation_id, recording_id);
            }
            break;
        }

        case AERON_ARCHIVE_PURGE_SEGMENTS_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + recordingId(8) + newStartPosition(8) = 32 */
            if (body_length < 32)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t recording_id = sbe_decode_int64(body, 16);
            const int64_t new_start_position = sbe_decode_int64(body, 24);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_purge_segments(
                    session, correlation_id, recording_id, new_start_position);
            }
            break;
        }

        case AERON_ARCHIVE_ATTACH_SEGMENTS_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + recordingId(8) = 24 */
            if (body_length < 24)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t recording_id = sbe_decode_int64(body, 16);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_attach_segments(session, correlation_id, recording_id);
            }
            break;
        }

        case AERON_ARCHIVE_MIGRATE_SEGMENTS_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + srcRecordingId(8) + dstRecordingId(8) = 32 */
            if (body_length < 32)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t src_recording_id = sbe_decode_int64(body, 16);
            const int64_t dst_recording_id = sbe_decode_int64(body, 24);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_migrate_segments(
                    session, correlation_id, src_recording_id, dst_recording_id);
            }
            break;
        }

        case AERON_ARCHIVE_CHALLENGE_RESPONSE_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) = 16 + var: encodedCredentials */
            if (body_length < 16)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);

            aeron_archive_control_session_info_t *info = find_session_info(adapter, control_session_id);
            if (NULL != info)
            {
                size_t var_offset = block_length;
                const uint32_t credentials_len = sbe_decode_var_data_length(body, var_offset);
                const uint8_t *credentials = (const uint8_t *)sbe_decode_var_data_ptr(body, var_offset);

                aeron_archive_control_session_on_challenge_response(
                    info->control_session, correlation_id, credentials, credentials_len);
            }
            break;
        }

        case AERON_ARCHIVE_KEEP_ALIVE_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) = 16 */
            if (body_length < 16)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_keep_alive(session, correlation_id);
            }
            break;
        }

        case AERON_ARCHIVE_TAGGED_REPLICATE_REQUEST_TEMPLATE_ID:
        {
            /*
             * controlSessionId(8) + correlationId(8) + srcRecordingId(8) + dstRecordingId(8) +
             * channelTagId(8) + subscriptionTagId(8) + srcControlStreamId(4) = 52
             * var: srcControlChannel, liveDestination
             */
            if (body_length < 52)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t src_recording_id = sbe_decode_int64(body, 16);
            const int64_t dst_recording_id = sbe_decode_int64(body, 24);
            const int64_t channel_tag_id = sbe_decode_int64(body, 32);
            const int64_t subscription_tag_id = sbe_decode_int64(body, 40);
            const int32_t src_control_stream_id = sbe_decode_int32(body, 48);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                size_t var_offset = block_length;
                const uint32_t src_channel_len = sbe_decode_var_data_length(body, var_offset);
                const char *src_channel = sbe_decode_var_data_ptr(body, var_offset);

                char src_channel_buf[4096];
                size_t copy_len = src_channel_len < sizeof(src_channel_buf) - 1 ?
                    src_channel_len : sizeof(src_channel_buf) - 1;
                memcpy(src_channel_buf, src_channel, copy_len);
                src_channel_buf[copy_len] = '\0';
                var_offset += sizeof(uint32_t) + src_channel_len;

                const uint32_t live_dest_len = sbe_decode_var_data_length(body, var_offset);
                const char *live_dest = sbe_decode_var_data_ptr(body, var_offset);

                char live_dest_buf[4096];
                copy_len = live_dest_len < sizeof(live_dest_buf) - 1 ?
                    live_dest_len : sizeof(live_dest_buf) - 1;
                memcpy(live_dest_buf, live_dest, copy_len);
                live_dest_buf[copy_len] = '\0';

                aeron_archive_control_session_on_replicate(
                    session, correlation_id, src_recording_id, dst_recording_id,
                    AERON_NULL_VALUE, channel_tag_id, subscription_tag_id,
                    src_control_stream_id,
                    (int32_t)AERON_NULL_VALUE, (int32_t)AERON_NULL_VALUE,
                    src_channel_buf, live_dest_buf, "",
                    NULL, 0, "");
            }
            break;
        }

        case AERON_ARCHIVE_STOP_RECORDING_BY_IDENTITY_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + recordingId(8) = 24 */
            if (body_length < 24)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t recording_id = sbe_decode_int64(body, 16);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_stop_recording_by_identity(
                    session, correlation_id, recording_id);
            }
            break;
        }

        case AERON_ARCHIVE_REPLICATE_REQUEST2_TEMPLATE_ID:
        {
            /*
             * ReplicateRequest2:
             *   controlSessionId(8) + correlationId(8) + srcRecordingId(8) + dstRecordingId(8) +
             *   stopPosition(8) + channelTagId(8) + subscriptionTagId(8) + srcControlStreamId(4) +
             *   fileIoMaxLength(4) + replicationSessionId(4) = 68
             *   var: srcControlChannel, liveDestination, replicationChannel, encodedCredentials, srcResponseChannel
             */
            if (body_length < 60) /* minimum without sinceVersion fields */
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t src_recording_id = sbe_decode_int64(body, 16);
            const int64_t dst_recording_id = sbe_decode_int64(body, 24);
            const int64_t stop_position = sbe_decode_int64(body, 32);
            const int64_t channel_tag_id = sbe_decode_int64(body, 40);
            const int64_t subscription_tag_id = sbe_decode_int64(body, 48);
            const int32_t src_control_stream_id = sbe_decode_int32(body, 56);
            const int32_t file_io_max_length = (block_length > 60) ?
                sbe_decode_int32(body, 60) : (int32_t)AERON_NULL_VALUE;
            const int32_t replication_session_id = (block_length > 64) ?
                sbe_decode_int32(body, 64) : (int32_t)AERON_NULL_VALUE;

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                size_t var_offset = block_length;

                /* srcControlChannel */
                const uint32_t src_channel_len = sbe_decode_var_data_length(body, var_offset);
                const char *src_channel = sbe_decode_var_data_ptr(body, var_offset);
                char src_channel_buf[4096];
                size_t copy_len = src_channel_len < sizeof(src_channel_buf) - 1 ?
                    src_channel_len : sizeof(src_channel_buf) - 1;
                memcpy(src_channel_buf, src_channel, copy_len);
                src_channel_buf[copy_len] = '\0';
                var_offset += sizeof(uint32_t) + src_channel_len;

                /* liveDestination */
                const uint32_t live_dest_len = sbe_decode_var_data_length(body, var_offset);
                const char *live_dest = sbe_decode_var_data_ptr(body, var_offset);
                char live_dest_buf[4096];
                copy_len = live_dest_len < sizeof(live_dest_buf) - 1 ?
                    live_dest_len : sizeof(live_dest_buf) - 1;
                memcpy(live_dest_buf, live_dest, copy_len);
                live_dest_buf[copy_len] = '\0';
                var_offset += sizeof(uint32_t) + live_dest_len;

                /* replicationChannel */
                const uint32_t repl_channel_len = sbe_decode_var_data_length(body, var_offset);
                const char *repl_channel = sbe_decode_var_data_ptr(body, var_offset);
                char repl_channel_buf[4096];
                copy_len = repl_channel_len < sizeof(repl_channel_buf) - 1 ?
                    repl_channel_len : sizeof(repl_channel_buf) - 1;
                memcpy(repl_channel_buf, repl_channel, copy_len);
                repl_channel_buf[copy_len] = '\0';
                var_offset += sizeof(uint32_t) + repl_channel_len;

                /* encodedCredentials (may not be present if version < 8) */
                const uint8_t *encoded_credentials = NULL;
                uint32_t encoded_credentials_len = 0;
                if (var_offset < body_length)
                {
                    encoded_credentials_len = sbe_decode_var_data_length(body, var_offset);
                    encoded_credentials = (const uint8_t *)sbe_decode_var_data_ptr(body, var_offset);
                    var_offset += sizeof(uint32_t) + encoded_credentials_len;
                }

                /* srcResponseChannel (may not be present if version < 10) */
                char src_response_buf[4096];
                src_response_buf[0] = '\0';
                if (var_offset < body_length)
                {
                    const uint32_t src_resp_len = sbe_decode_var_data_length(body, var_offset);
                    const char *src_resp = sbe_decode_var_data_ptr(body, var_offset);
                    copy_len = src_resp_len < sizeof(src_response_buf) - 1 ?
                        src_resp_len : sizeof(src_response_buf) - 1;
                    memcpy(src_response_buf, src_resp, copy_len);
                    src_response_buf[copy_len] = '\0';
                }

                aeron_archive_control_session_on_replicate(
                    session, correlation_id, src_recording_id, dst_recording_id,
                    stop_position, channel_tag_id, subscription_tag_id,
                    src_control_stream_id, file_io_max_length, replication_session_id,
                    src_channel_buf, live_dest_buf, repl_channel_buf,
                    encoded_credentials, encoded_credentials_len,
                    src_response_buf);
            }
            break;
        }

        case AERON_ARCHIVE_PURGE_RECORDING_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + recordingId(8) = 24 */
            if (body_length < 24)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t recording_id = sbe_decode_int64(body, 16);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_purge_recording(session, correlation_id, recording_id);
            }
            break;
        }

        case AERON_ARCHIVE_MAX_RECORDED_POSITION_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + recordingId(8) = 24 */
            if (body_length < 24)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            const int64_t recording_id = sbe_decode_int64(body, 16);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_get_max_recorded_position(
                    session, correlation_id, recording_id);
            }
            break;
        }

        case AERON_ARCHIVE_ARCHIVE_ID_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) = 16 */
            if (body_length < 16)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                aeron_archive_control_session_on_archive_id(session, correlation_id);
            }
            break;
        }

        case AERON_ARCHIVE_REPLAY_TOKEN_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + recordingId(8) = 24 */
            if (body_length < 24)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            /* const int64_t recording_id = sbe_decode_int64(body, 16); */

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                /*
                 * In the full implementation:
                 *   long replayToken = conductor->generateReplayToken(session, recordingId);
                 *   aeron_archive_control_session_send_ok_response(session, correlationId, replayToken);
                 */
                aeron_archive_control_session_send_ok_response(session, correlation_id, 0);
            }
            break;
        }

        case AERON_ARCHIVE_UPDATE_CHANNEL_REQUEST_TEMPLATE_ID:
        {
            /* controlSessionId(8) + correlationId(8) + recordingId(8) = 24 + var: channel */
            if (body_length < 24)
            {
                return;
            }

            const int64_t control_session_id = sbe_decode_int64(body, 0);
            const int64_t correlation_id = sbe_decode_int64(body, 8);
            /* const int64_t recording_id = sbe_decode_int64(body, 16); */

            aeron_archive_control_session_t *session =
                get_control_session(adapter, correlation_id, control_session_id, template_id);

            if (NULL != session)
            {
                /*
                 * In the full implementation:
                 *   conductor->updateChannel(correlationId, recordingId, channel, session);
                 */
            }
            break;
        }

        default:
            break;
    }
}
