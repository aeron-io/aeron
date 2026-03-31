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
#include "aeron_archive_control_response_proxy.h"
#include "aeron_archive_catalog.h"

/*
 * Inline SBE encoding helpers.
 *
 * SBE message header (8 bytes, little-endian):
 *   offset 0: uint16 blockLength
 *   offset 2: uint16 templateId
 *   offset 4: uint16 schemaId
 *   offset 6: uint16 version
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
    if (length > 0 && NULL != data)
    {
        memcpy(buffer + offset + sizeof(uint32_t), data, length);
    }
    return sizeof(uint32_t) + length;
}

static void check_result(int64_t position, aeron_exclusive_publication_t *publication)
{
    if (AERON_PUBLICATION_CLOSED == position)
    {
        AERON_SET_ERR(-1, "%s", "control response publication is closed");
    }
    else if (AERON_PUBLICATION_MAX_POSITION_EXCEEDED == position)
    {
        AERON_SET_ERR(-1, "%s", "control response publication at max position");
    }
}

int aeron_archive_control_response_proxy_create(
    aeron_archive_control_response_proxy_t **proxy)
{
    aeron_archive_control_response_proxy_t *_proxy = NULL;

    if (aeron_alloc((void **)&_proxy, sizeof(aeron_archive_control_response_proxy_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_control_response_proxy_t");
        return -1;
    }

    memset(_proxy->buffer, 0, sizeof(_proxy->buffer));

    *proxy = _proxy;

    return 0;
}

int aeron_archive_control_response_proxy_close(
    aeron_archive_control_response_proxy_t *proxy)
{
    if (NULL != proxy)
    {
        aeron_free(proxy);
    }

    return 0;
}

/*
 * Protocol semantic version matching AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION.
 * Major=1, Minor=13, Patch=0 => (1 << 16) | (13 << 8) | 0 = 69376.
 */
#define AERON_ARCHIVE_PROTOCOL_SEMANTIC_VERSION ((1 << 16) | (AERON_ARCHIVE_CONTROL_SCHEMA_VERSION << 8) | 0)

bool aeron_archive_control_response_proxy_send_response(
    aeron_archive_control_response_proxy_t *proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t relevant_id,
    aeron_archive_control_response_code_t code,
    const char *error_message,
    aeron_exclusive_publication_t *publication)
{
    uint8_t *buffer = proxy->buffer;
    const size_t body_offset = AERON_ARCHIVE_SBE_MESSAGE_HEADER_LENGTH;

    sbe_encode_header(
        buffer,
        AERON_ARCHIVE_CONTROL_RESPONSE_BLOCK_LENGTH,
        AERON_ARCHIVE_CONTROL_RESPONSE_TEMPLATE_ID,
        AERON_ARCHIVE_CONTROL_SCHEMA_ID,
        AERON_ARCHIVE_CONTROL_SCHEMA_VERSION);

    sbe_encode_int64(buffer, body_offset + 0, control_session_id);
    sbe_encode_int64(buffer, body_offset + 8, correlation_id);
    sbe_encode_int64(buffer, body_offset + 16, relevant_id);
    sbe_encode_int32(buffer, body_offset + 24, (int32_t)code);
    sbe_encode_int32(buffer, body_offset + 28, (int32_t)AERON_ARCHIVE_PROTOCOL_SEMANTIC_VERSION);

    const size_t error_len = (NULL != error_message) ? strlen(error_message) : 0;
    size_t var_offset = body_offset + AERON_ARCHIVE_CONTROL_RESPONSE_BLOCK_LENGTH;
    var_offset += sbe_encode_var_data(buffer, var_offset, error_message, error_len);

    const size_t total_length = var_offset;

    int attempts = AERON_ARCHIVE_CONTROL_RESPONSE_PROXY_SEND_ATTEMPTS;
    do
    {
        const int64_t position = aeron_exclusive_publication_offer(
            publication, buffer, total_length, NULL, NULL);

        if (position > 0)
        {
            return true;
        }

        if (AERON_PUBLICATION_NOT_CONNECTED == position)
        {
            return false;
        }

        check_result(position, publication);
    }
    while (--attempts > 0);

    return false;
}

bool aeron_archive_control_response_proxy_send_descriptor(
    aeron_archive_control_response_proxy_t *proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    const uint8_t *descriptor_buffer,
    size_t descriptor_length,
    aeron_exclusive_publication_t *publication)
{
    /*
     * The descriptor is stored in catalog format with a RecordingDescriptorHeader followed by
     * the RecordingDescriptor body. We need to wrap it in an SBE message header + controlSessionId +
     * correlationId prefix, then the descriptor content starting from recordingId.
     *
     * Wire format:
     *   [SBE message header (8)]
     *   [controlSessionId (8)]
     *   [correlationId (8)]
     *   [descriptor body from recordingId offset onwards]
     *
     * The descriptor body in the catalog starts at:
     *   RecordingDescriptorHeader (32 bytes) + recordingId offset (16 bytes into the descriptor block)
     */
    const size_t descriptor_header_len = AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_LENGTH;
    const size_t recording_id_offset = AERON_ARCHIVE_RECORDING_DESCRIPTOR_RECORDING_ID_OFFSET;
    const size_t content_offset = descriptor_header_len + recording_id_offset;

    if (descriptor_length <= content_offset)
    {
        return false;
    }

    const size_t content_length = descriptor_length - content_offset;

    uint8_t *buffer = proxy->buffer;
    const size_t body_offset = AERON_ARCHIVE_SBE_MESSAGE_HEADER_LENGTH;

    sbe_encode_header(
        buffer,
        AERON_ARCHIVE_RECORDING_DESCRIPTOR_BLOCK_LENGTH,
        AERON_ARCHIVE_RECORDING_DESCRIPTOR_TEMPLATE_ID,
        AERON_ARCHIVE_CONTROL_SCHEMA_ID,
        AERON_ARCHIVE_CONTROL_SCHEMA_VERSION);

    sbe_encode_int64(buffer, body_offset + 0, control_session_id);
    sbe_encode_int64(buffer, body_offset + 8, correlation_id);

    const size_t prefix_length = body_offset + 16; /* header + controlSessionId + correlationId */

    aeron_iovec_t iov[2];
    iov[0].iov_base = buffer;
    iov[0].iov_len = prefix_length;
    iov[1].iov_base = (uint8_t *)(descriptor_buffer + content_offset);
    iov[1].iov_len = content_length;

    int attempts = AERON_ARCHIVE_CONTROL_RESPONSE_PROXY_SEND_ATTEMPTS;
    do
    {
        const int64_t position = aeron_exclusive_publication_offerv(
            publication, iov, 2, NULL, NULL);

        if (position > 0)
        {
            return true;
        }

        if (AERON_PUBLICATION_NOT_CONNECTED == position)
        {
            return false;
        }

        check_result(position, publication);
    }
    while (--attempts > 0);

    return false;
}

bool aeron_archive_control_response_proxy_send_subscription_descriptor(
    aeron_archive_control_response_proxy_t *proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t subscription_id,
    int32_t stream_id,
    const char *channel,
    aeron_exclusive_publication_t *publication)
{
    uint8_t *buffer = proxy->buffer;
    const size_t body_offset = AERON_ARCHIVE_SBE_MESSAGE_HEADER_LENGTH;

    sbe_encode_header(
        buffer,
        AERON_ARCHIVE_RECORDING_SUBSCRIPTION_DESCRIPTOR_BLOCK_LENGTH,
        AERON_ARCHIVE_RECORDING_SUBSCRIPTION_DESCRIPTOR_TEMPLATE_ID,
        AERON_ARCHIVE_CONTROL_SCHEMA_ID,
        AERON_ARCHIVE_CONTROL_SCHEMA_VERSION);

    sbe_encode_int64(buffer, body_offset + 0, control_session_id);
    sbe_encode_int64(buffer, body_offset + 8, correlation_id);
    sbe_encode_int64(buffer, body_offset + 16, subscription_id);
    sbe_encode_int32(buffer, body_offset + 24, stream_id);

    const size_t channel_len = (NULL != channel) ? strlen(channel) : 0;
    size_t var_offset = body_offset + AERON_ARCHIVE_RECORDING_SUBSCRIPTION_DESCRIPTOR_BLOCK_LENGTH;
    var_offset += sbe_encode_var_data(buffer, var_offset, channel, channel_len);

    const size_t total_length = var_offset;

    int attempts = AERON_ARCHIVE_CONTROL_RESPONSE_PROXY_SEND_ATTEMPTS;
    do
    {
        const int64_t position = aeron_exclusive_publication_offer(
            publication, buffer, total_length, NULL, NULL);

        if (position > 0)
        {
            return true;
        }

        if (AERON_PUBLICATION_NOT_CONNECTED == position)
        {
            return false;
        }

        check_result(position, publication);
    }
    while (--attempts > 0);

    return false;
}

bool aeron_archive_control_response_proxy_send_challenge(
    aeron_archive_control_response_proxy_t *proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    const uint8_t *encoded_challenge,
    size_t challenge_length,
    aeron_exclusive_publication_t *publication)
{
    uint8_t *buffer = proxy->buffer;
    const size_t body_offset = AERON_ARCHIVE_SBE_MESSAGE_HEADER_LENGTH;

    sbe_encode_header(
        buffer,
        AERON_ARCHIVE_CHALLENGE_BLOCK_LENGTH,
        AERON_ARCHIVE_CHALLENGE_TEMPLATE_ID,
        AERON_ARCHIVE_CONTROL_SCHEMA_ID,
        AERON_ARCHIVE_CONTROL_SCHEMA_VERSION);

    sbe_encode_int64(buffer, body_offset + 0, control_session_id);
    sbe_encode_int64(buffer, body_offset + 8, correlation_id);
    sbe_encode_int32(buffer, body_offset + 16, (int32_t)AERON_ARCHIVE_PROTOCOL_SEMANTIC_VERSION);

    size_t var_offset = body_offset + AERON_ARCHIVE_CHALLENGE_BLOCK_LENGTH;
    var_offset += sbe_encode_var_data(buffer, var_offset, (const char *)encoded_challenge, challenge_length);

    const size_t total_length = var_offset;

    int attempts = AERON_ARCHIVE_CONTROL_RESPONSE_PROXY_SEND_ATTEMPTS;
    do
    {
        const int64_t position = aeron_exclusive_publication_offer(
            publication, buffer, total_length, NULL, NULL);

        if (position > 0)
        {
            return true;
        }

        if (AERON_PUBLICATION_NOT_CONNECTED == position)
        {
            return false;
        }

        check_result(position, publication);
    }
    while (--attempts > 0);

    return false;
}

bool aeron_archive_control_response_proxy_send_signal(
    aeron_archive_control_response_proxy_t *proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t subscription_id,
    int64_t position,
    aeron_archive_recording_signal_code_t signal,
    aeron_exclusive_publication_t *publication)
{
    const size_t length =
        AERON_ARCHIVE_SBE_MESSAGE_HEADER_LENGTH + AERON_ARCHIVE_RECORDING_SIGNAL_EVENT_BLOCK_LENGTH;

    int attempts = AERON_ARCHIVE_CONTROL_RESPONSE_PROXY_SEND_ATTEMPTS;
    do
    {
        aeron_buffer_claim_t buffer_claim;
        const int64_t result = aeron_exclusive_publication_try_claim(publication, length, &buffer_claim);

        if (result > 0)
        {
            uint8_t *data = buffer_claim.data;

            sbe_encode_header(
                data,
                AERON_ARCHIVE_RECORDING_SIGNAL_EVENT_BLOCK_LENGTH,
                AERON_ARCHIVE_RECORDING_SIGNAL_EVENT_TEMPLATE_ID,
                AERON_ARCHIVE_CONTROL_SCHEMA_ID,
                AERON_ARCHIVE_CONTROL_SCHEMA_VERSION);

            const size_t body_offset = AERON_ARCHIVE_SBE_MESSAGE_HEADER_LENGTH;
            sbe_encode_int64(data, body_offset + 0, control_session_id);
            sbe_encode_int64(data, body_offset + 8, correlation_id);
            sbe_encode_int64(data, body_offset + 16, recording_id);
            sbe_encode_int64(data, body_offset + 24, subscription_id);
            sbe_encode_int64(data, body_offset + 32, position);
            sbe_encode_int32(data, body_offset + 40, (int32_t)signal);

            aeron_buffer_claim_commit(&buffer_claim);
            return true;
        }
    }
    while (--attempts > 0);

    return false;
}

bool aeron_archive_control_response_proxy_send_ping(
    aeron_archive_control_response_proxy_t *proxy,
    int64_t control_session_id,
    aeron_exclusive_publication_t *publication)
{
    const size_t length = AERON_ARCHIVE_SBE_MESSAGE_HEADER_LENGTH + AERON_ARCHIVE_PING_BLOCK_LENGTH;

    int attempts = AERON_ARCHIVE_CONTROL_RESPONSE_PROXY_SEND_ATTEMPTS;
    do
    {
        aeron_buffer_claim_t buffer_claim;
        const int64_t result = aeron_exclusive_publication_try_claim(publication, length, &buffer_claim);

        if (result > 0)
        {
            uint8_t *data = buffer_claim.data;

            sbe_encode_header(
                data,
                AERON_ARCHIVE_PING_BLOCK_LENGTH,
                AERON_ARCHIVE_PING_TEMPLATE_ID,
                AERON_ARCHIVE_CONTROL_SCHEMA_ID,
                AERON_ARCHIVE_CONTROL_SCHEMA_VERSION);

            const size_t body_offset = AERON_ARCHIVE_SBE_MESSAGE_HEADER_LENGTH;
            sbe_encode_int64(data, body_offset + 0, control_session_id);

            aeron_buffer_claim_commit(&buffer_claim);
            return true;
        }
    }
    while (--attempts > 0);

    return false;
}
