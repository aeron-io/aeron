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

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "aeron_archive_recording_events_proxy.h"

/*
 * Inline SBE encoding helpers.
 *
 * The SBE C codecs are generated at build time and may not be present in the source tree.
 * We encode the messages manually using the wire format defined in aeron-archive-codecs.xml.
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

static inline void sbe_encode_header(
    uint8_t *buffer,
    uint16_t block_length,
    uint16_t template_id,
    uint16_t schema_id,
    uint16_t version)
{
    memcpy(buffer + 0, &block_length, sizeof(uint16_t));
    memcpy(buffer + 2, &template_id, sizeof(uint16_t));
    memcpy(buffer + 4, &schema_id, sizeof(uint16_t));
    memcpy(buffer + 6, &version, sizeof(uint16_t));
}

static inline void sbe_encode_int64(uint8_t *buffer, size_t offset, int64_t value)
{
    memcpy(buffer + offset, &value, sizeof(int64_t));
}

static inline void sbe_encode_int32(uint8_t *buffer, size_t offset, int32_t value)
{
    memcpy(buffer + offset, &value, sizeof(int32_t));
}

static inline size_t sbe_encode_var_data(uint8_t *buffer, size_t offset, const char *data, size_t length)
{
    uint32_t len = (uint32_t)length;
    memcpy(buffer + offset, &len, sizeof(uint32_t));
    if (length > 0)
    {
        memcpy(buffer + offset + sizeof(uint32_t), data, length);
    }
    return sizeof(uint32_t) + length;
}

static void check_result(int64_t position, aeron_publication_t *publication)
{
    if (AERON_PUBLICATION_CLOSED == position)
    {
        AERON_SET_ERR(-1, "%s", "recording events publication is closed");
    }
    else if (AERON_PUBLICATION_MAX_POSITION_EXCEEDED == position)
    {
        AERON_SET_ERR(-1, "%s", "recording events publication at max position");
    }
}

int aeron_archive_recording_events_proxy_create(
    aeron_archive_recording_events_proxy_t **proxy,
    aeron_publication_t *publication)
{
    aeron_archive_recording_events_proxy_t *_proxy = NULL;

    if (aeron_alloc((void **)&_proxy, sizeof(aeron_archive_recording_events_proxy_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_recording_events_proxy_t");
        return -1;
    }

    _proxy->publication = publication;
    memset(_proxy->buffer, 0, sizeof(_proxy->buffer));

    *proxy = _proxy;

    return 0;
}

void aeron_archive_recording_events_proxy_started(
    aeron_archive_recording_events_proxy_t *proxy,
    int64_t recording_id,
    int64_t start_position,
    int32_t session_id,
    int32_t stream_id,
    const char *channel,
    const char *source_identity)
{
    if (NULL == proxy || NULL == proxy->publication)
    {
        return;
    }

    uint8_t *buffer = proxy->buffer;
    const size_t body_offset = AERON_ARCHIVE_SBE_MESSAGE_HEADER_LENGTH;

    sbe_encode_header(
        buffer,
        AERON_ARCHIVE_RECORDING_STARTED_BLOCK_LENGTH,
        AERON_ARCHIVE_RECORDING_STARTED_TEMPLATE_ID,
        AERON_ARCHIVE_RECORDING_EVENTS_SCHEMA_ID,
        AERON_ARCHIVE_RECORDING_EVENTS_SCHEMA_VERSION);

    /* Fixed fields */
    sbe_encode_int64(buffer, body_offset + 0, recording_id);
    sbe_encode_int64(buffer, body_offset + 8, start_position);
    sbe_encode_int32(buffer, body_offset + 16, session_id);
    sbe_encode_int32(buffer, body_offset + 20, stream_id);

    /* Variable-length fields */
    size_t var_offset = body_offset + AERON_ARCHIVE_RECORDING_STARTED_BLOCK_LENGTH;
    const size_t channel_len = NULL != channel ? strlen(channel) : 0;
    var_offset += sbe_encode_var_data(buffer, var_offset, channel, channel_len);
    const size_t source_len = NULL != source_identity ? strlen(source_identity) : 0;
    var_offset += sbe_encode_var_data(buffer, var_offset, source_identity, source_len);

    const size_t total_length = var_offset;

    int attempts = AERON_ARCHIVE_RECORDING_EVENTS_PROXY_SEND_ATTEMPTS;
    do
    {
        const int64_t position = aeron_publication_offer(
            proxy->publication, buffer, total_length, NULL, NULL);

        if (position > 0 || AERON_PUBLICATION_NOT_CONNECTED == position)
        {
            break;
        }

        check_result(position, proxy->publication);
    }
    while (--attempts > 0);
}

bool aeron_archive_recording_events_proxy_progress(
    aeron_archive_recording_events_proxy_t *proxy,
    int64_t recording_id,
    int64_t start_position,
    int64_t position)
{
    if (NULL == proxy || NULL == proxy->publication)
    {
        return false;
    }

    const size_t length =
        AERON_ARCHIVE_SBE_MESSAGE_HEADER_LENGTH + AERON_ARCHIVE_RECORDING_PROGRESS_BLOCK_LENGTH;

    aeron_buffer_claim_t buffer_claim;
    const int64_t result = aeron_publication_try_claim(proxy->publication, length, &buffer_claim);

    if (result > 0)
    {
        uint8_t *data = buffer_claim.data;

        sbe_encode_header(
            data,
            AERON_ARCHIVE_RECORDING_PROGRESS_BLOCK_LENGTH,
            AERON_ARCHIVE_RECORDING_PROGRESS_TEMPLATE_ID,
            AERON_ARCHIVE_RECORDING_EVENTS_SCHEMA_ID,
            AERON_ARCHIVE_RECORDING_EVENTS_SCHEMA_VERSION);

        const size_t body_offset = AERON_ARCHIVE_SBE_MESSAGE_HEADER_LENGTH;
        sbe_encode_int64(data, body_offset + 0, recording_id);
        sbe_encode_int64(data, body_offset + 8, start_position);
        sbe_encode_int64(data, body_offset + 16, position);

        aeron_buffer_claim_commit(&buffer_claim);
        return true;
    }
    else
    {
        check_result(result, proxy->publication);
    }

    return false;
}

void aeron_archive_recording_events_proxy_stopped(
    aeron_archive_recording_events_proxy_t *proxy,
    int64_t recording_id,
    int64_t start_position,
    int64_t stop_position)
{
    if (NULL == proxy || NULL == proxy->publication)
    {
        return;
    }

    const size_t length =
        AERON_ARCHIVE_SBE_MESSAGE_HEADER_LENGTH + AERON_ARCHIVE_RECORDING_STOPPED_BLOCK_LENGTH;

    int attempts = AERON_ARCHIVE_RECORDING_EVENTS_PROXY_SEND_ATTEMPTS;
    do
    {
        aeron_buffer_claim_t buffer_claim;
        const int64_t result = aeron_publication_try_claim(proxy->publication, length, &buffer_claim);

        if (result > 0)
        {
            uint8_t *data = buffer_claim.data;

            sbe_encode_header(
                data,
                AERON_ARCHIVE_RECORDING_STOPPED_BLOCK_LENGTH,
                AERON_ARCHIVE_RECORDING_STOPPED_TEMPLATE_ID,
                AERON_ARCHIVE_RECORDING_EVENTS_SCHEMA_ID,
                AERON_ARCHIVE_RECORDING_EVENTS_SCHEMA_VERSION);

            const size_t body_offset = AERON_ARCHIVE_SBE_MESSAGE_HEADER_LENGTH;
            sbe_encode_int64(data, body_offset + 0, recording_id);
            sbe_encode_int64(data, body_offset + 8, start_position);
            sbe_encode_int64(data, body_offset + 16, stop_position);

            aeron_buffer_claim_commit(&buffer_claim);
            break;
        }
        else if (AERON_PUBLICATION_NOT_CONNECTED == result)
        {
            break;
        }

        check_result(result, proxy->publication);
    }
    while (--attempts > 0);
}

int aeron_archive_recording_events_proxy_close(aeron_archive_recording_events_proxy_t *proxy)
{
    if (NULL != proxy)
    {
        if (NULL != proxy->publication)
        {
            aeron_publication_close(proxy->publication, NULL, NULL);
            proxy->publication = NULL;
        }

        aeron_free(proxy);
    }

    return 0;
}
