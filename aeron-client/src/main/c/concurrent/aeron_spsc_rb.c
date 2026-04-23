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
#include <inttypes.h>
#include "aeron_spsc_rb.h"
#include "util/aeron_error.h"

int aeron_spsc_rb_init(aeron_spsc_rb_t *ring_buffer, void *buffer, size_t length)
{
    const size_t capacity = length - AERON_RB_TRAILER_LENGTH;
    int result = -1;

    if (AERON_RB_IS_CAPACITY_VALID(capacity, AERON_SPSC_RB_MIN_CAPACITY))
    {
        ring_buffer->buffer = buffer;
        ring_buffer->capacity = capacity;
        ring_buffer->descriptor = (aeron_rb_descriptor_t *)(ring_buffer->buffer + ring_buffer->capacity);
        ring_buffer->max_message_length = AERON_RB_MAX_MESSAGE_LENGTH(ring_buffer->capacity, AERON_SPSC_RB_MIN_CAPACITY);
        result = 0;
    }
    else
    {
        AERON_SET_ERR(EINVAL, "Invalid capacity: %" PRIu64, (uint64_t)capacity);
    }

    return result;
}

aeron_rb_write_result_t aeron_spsc_rb_write(
    aeron_spsc_rb_t *ring_buffer, int32_t msg_type_id, const void *msg, size_t length)
{
    struct iovec vec[1];
    vec[0].iov_len = length;
    vec[0].iov_base = (void *)msg;

    return aeron_spsc_rb_writev(ring_buffer, msg_type_id, vec, 1);
}

inline static int32_t aeron_spsc_rb_claim_capacity(aeron_spsc_rb_t *ring_buffer, const size_t record_length)
{
    const size_t aligned_record_length = AERON_ALIGN(record_length, AERON_RB_ALIGNMENT);
    const size_t required_capacity = aligned_record_length + AERON_RB_RECORD_HEADER_LENGTH;
    const size_t mask = ring_buffer->capacity - 1;

    int64_t head = ring_buffer->descriptor->head_cache_position;
    int64_t tail = ring_buffer->descriptor->tail_position;
    const int32_t available_capacity = (int32_t)ring_buffer->capacity - (int32_t)(tail - head);

    size_t padding = 0;
    size_t record_index = (size_t)tail & mask;
    const size_t to_buffer_end_length = ring_buffer->capacity - record_index;
    aeron_rb_record_descriptor_t *record_header, *next_header = NULL;

    if ((int32_t)required_capacity > available_capacity)
    {
        AERON_GET_ACQUIRE(head, ring_buffer->descriptor->head_position);

        if (required_capacity > (ring_buffer->capacity - (size_t)(tail - head)))
        {
            return AERON_RB_FULL;
        }

        ring_buffer->descriptor->head_cache_position = head;
    }

    int64_t next_tail = tail + (int64_t)aligned_record_length;
    int32_t write_index = (int32_t)record_index;
    if (aligned_record_length == to_buffer_end_length) // message fits within the end of the buffer
    {
        AERON_SET_RELEASE(ring_buffer->descriptor->tail_position, next_tail);
        // pre-zero next message header
        next_header = (aeron_rb_record_descriptor_t *)ring_buffer->buffer;

        next_header->length = 0;
        next_header->msg_type_id = 0;
        return (int32_t)record_index;
    }
    else if (required_capacity > to_buffer_end_length)
    {
        write_index = 0;
        size_t head_index = (size_t)(head & mask);

        if (required_capacity > head_index)
        {
            AERON_GET_ACQUIRE(head, ring_buffer->descriptor->head_position);
            head_index = (size_t)(head & mask);

            if (required_capacity > head_index)
            {
                write_index = AERON_RB_FULL;
                next_tail = tail;
            }

            ring_buffer->descriptor->head_cache_position = head;
        }

        padding = to_buffer_end_length;
        next_tail += (int64_t)padding;
    }

    AERON_SET_RELEASE(ring_buffer->descriptor->tail_position, next_tail);

    if (0 != padding)
    {
        record_header = (aeron_rb_record_descriptor_t *)(ring_buffer->buffer + record_index);
        next_header = (aeron_rb_record_descriptor_t *)ring_buffer->buffer;

        next_header->length = 0;
        next_header->msg_type_id = 0;
        AERON_SET_RELEASE(record_header->length, -(int32_t)padding);
        record_header->msg_type_id = AERON_RB_PADDING_MSG_TYPE_ID;
        AERON_SET_RELEASE(record_header->length, (int32_t)padding);
        record_index = 0;
    }

    if (AERON_RB_FULL != write_index)
    {
        next_header = (aeron_rb_record_descriptor_t *)(ring_buffer->buffer + write_index + aligned_record_length);

        next_header->length = 0;
        next_header->msg_type_id = 0;
    }

    return (int32_t)write_index;
}

aeron_rb_write_result_t aeron_spsc_rb_writev(
    aeron_spsc_rb_t *ring_buffer, int32_t msg_type_id, const struct iovec *iov, int iovcnt)
{
    size_t length = 0;
    for (int i = 0; i < iovcnt; i++)
    {
        length += iov[i].iov_len;
    }

    if (length > ring_buffer->max_message_length || AERON_RB_INVALID_MSG_TYPE_ID(msg_type_id))
    {
        return AERON_RB_ERROR;
    }

    const size_t record_length = length + AERON_RB_RECORD_HEADER_LENGTH;
    const int32_t record_index = aeron_spsc_rb_claim_capacity(ring_buffer, record_length);
    if (-1 != record_index)
    {
        aeron_rb_record_descriptor_t *record_header =
            (aeron_rb_record_descriptor_t *)(ring_buffer->buffer + record_index);
        AERON_SET_RELEASE(record_header->length, -(int32_t)record_length);

        size_t current_vector_offset = 0;
        for (int i = 0; i < iovcnt; i++)
        {
            uint8_t *offset = ring_buffer->buffer + AERON_RB_MESSAGE_OFFSET(record_index) + current_vector_offset;
            memcpy(offset, iov[i].iov_base, iov[i].iov_len);
            current_vector_offset += iov[i].iov_len;
        }

        record_header->msg_type_id = msg_type_id;
        AERON_SET_RELEASE(record_header->length, (int32_t)record_length);

        return AERON_RB_SUCCESS;
    }

    return AERON_RB_FULL;
}

int32_t aeron_spsc_rb_try_claim(aeron_spsc_rb_t *ring_buffer, int32_t msg_type_id, size_t length)
{
    if (length > ring_buffer->max_message_length || AERON_RB_INVALID_MSG_TYPE_ID(msg_type_id))
    {
        return AERON_RB_ERROR;
    }

    const size_t record_length = length + AERON_RB_RECORD_HEADER_LENGTH;
    const int32_t record_index = aeron_spsc_rb_claim_capacity(ring_buffer, record_length);
    if (-1 != record_index)
    {
        aeron_rb_record_descriptor_t *record_header =
            (aeron_rb_record_descriptor_t *)(ring_buffer->buffer + record_index);
        AERON_SET_RELEASE(record_header->length, -(int32_t)record_length);
        record_header->msg_type_id = msg_type_id;

        return AERON_RB_MESSAGE_OFFSET(record_index);
    }

    return AERON_RB_FULL;
}

int aeron_spsc_rb_commit(aeron_spsc_rb_t *ring_buffer, int32_t offset)
{
    const int32_t record_index = offset - (int32_t)AERON_RB_RECORD_HEADER_LENGTH;
    if (record_index < 0 || record_index > (int32_t)(ring_buffer->capacity - AERON_RB_RECORD_HEADER_LENGTH))
    {
        return -1;
    }

    aeron_rb_record_descriptor_t *record_header = (aeron_rb_record_descriptor_t *)(ring_buffer->buffer + record_index);
    const int32_t length = record_header->length;
    if (length < 0)
    {
        AERON_SET_RELEASE(record_header->length, -length);
        return 0;
    }

    return -1;
}

int aeron_spsc_rb_abort(aeron_spsc_rb_t *ring_buffer, int32_t offset)
{
    const int32_t record_index = offset - (int32_t)AERON_RB_RECORD_HEADER_LENGTH;
    if (record_index < 0 || record_index > (int32_t)(ring_buffer->capacity - AERON_RB_RECORD_HEADER_LENGTH))
    {
        return -1;
    }

    aeron_rb_record_descriptor_t *record_header = (aeron_rb_record_descriptor_t *)(ring_buffer->buffer + record_index);
    const int32_t length = record_header->length;
    if (length < 0)
    {
        record_header->msg_type_id = AERON_RB_PADDING_MSG_TYPE_ID;
        AERON_SET_RELEASE(record_header->length, -length);
        return 0;
    }

    return -1;
}

size_t aeron_spsc_rb_read(
    aeron_spsc_rb_t *ring_buffer, aeron_rb_handler_t handler, void *clientd, size_t message_count_limit)
{
    const int64_t head = ring_buffer->descriptor->head_position;
    const size_t head_index = (size_t)(head & (ring_buffer->capacity - 1));
    const size_t contiguous_block_length = ring_buffer->capacity - head_index;
    size_t messages_read = 0;
    size_t bytes_read = 0;

    while ((bytes_read < contiguous_block_length) && (messages_read < message_count_limit))
    {
        const size_t record_index = head_index + bytes_read;
        aeron_rb_record_descriptor_t *header = (aeron_rb_record_descriptor_t *)(ring_buffer->buffer + record_index);
        int32_t record_length;
        AERON_GET_ACQUIRE(record_length, header->length);

        if (record_length <= 0)
        {
            break;
        }

        bytes_read += AERON_ALIGN(record_length, AERON_RB_ALIGNMENT);
        int32_t msg_type_id = header->msg_type_id;

        if (AERON_RB_PADDING_MSG_TYPE_ID == msg_type_id)
        {
            continue;
        }

        ++messages_read;
        handler(
            msg_type_id,
            ring_buffer->buffer + AERON_RB_MESSAGE_OFFSET(record_index),
            record_length - AERON_RB_RECORD_HEADER_LENGTH,
            clientd);
    }

    if (0 != bytes_read)
    {
        AERON_SET_RELEASE(ring_buffer->descriptor->head_position, head + bytes_read);
    }

    return messages_read;
}

size_t aeron_spsc_rb_controlled_read(
    aeron_spsc_rb_t *ring_buffer, aeron_rb_controlled_handler_t handler, void *clientd, size_t message_count_limit)
{
    int64_t head = ring_buffer->descriptor->head_position;
    size_t head_index = (size_t)(head & (ring_buffer->capacity - 1));
    const size_t contiguous_block_length = ring_buffer->capacity - head_index;
    size_t messages_read = 0;
    size_t bytes_read = 0;

    while ((bytes_read < contiguous_block_length) && (messages_read < message_count_limit))
    {
        const size_t record_index = head_index + bytes_read;
        aeron_rb_record_descriptor_t *header = (aeron_rb_record_descriptor_t *)(ring_buffer->buffer + record_index);
        int32_t record_length;
        AERON_GET_ACQUIRE(record_length, header->length);

        if (record_length <= 0)
        {
            break;
        }

        const size_t aligned_length = AERON_ALIGN(record_length, AERON_RB_ALIGNMENT);
        bytes_read += aligned_length;
        int32_t msg_type_id = header->msg_type_id;

        if (AERON_RB_PADDING_MSG_TYPE_ID == msg_type_id)
        {
            continue;
        }

        aeron_rb_read_action_t action = handler(
            msg_type_id,
            ring_buffer->buffer + AERON_RB_MESSAGE_OFFSET(record_index),
            record_length - AERON_RB_RECORD_HEADER_LENGTH,
            clientd);

        if (AERON_RB_ABORT == action)
        {
            bytes_read -= aligned_length;
            break;
        }
        
        ++messages_read;

        if (AERON_RB_BREAK == action)
        {
            break;
        }
        if (AERON_RB_COMMIT == action)
        {
            AERON_SET_RELEASE(ring_buffer->descriptor->head_position, head + bytes_read);
            head_index += bytes_read;
            head += (int64_t)bytes_read;
            bytes_read = 0;
        }
    }

    if (0 != bytes_read)
    {
        AERON_SET_RELEASE(ring_buffer->descriptor->head_position, head + bytes_read);
    }

    return messages_read;
}

int64_t aeron_spsc_rb_next_correlation_id(aeron_spsc_rb_t *ring_buffer)
{
    int64_t result;

    // this is aligned as far as usage goes. And should perform fine.
#if defined(__clang__) && defined(AERON_CPU_ARM)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Watomic-alignment"
#endif
    AERON_GET_AND_ADD_INT64(result, ring_buffer->descriptor->correlation_counter, INT64_C(1));
#if defined(__clang__) && defined(AERON_CPU_ARM)
#pragma clang diagnostic pop
#endif

    return result;
}

void aeron_spsc_rb_consumer_heartbeat_time(aeron_spsc_rb_t *ring_buffer, int64_t time_ms)
{
    AERON_SET_RELEASE(ring_buffer->descriptor->consumer_heartbeat, time_ms);
}

extern int64_t aeron_spsc_rb_consumer_position(aeron_spsc_rb_t *ring_buffer);

extern int64_t aeron_spsc_rb_producer_position(aeron_spsc_rb_t *ring_buffer);

extern int64_t aeron_spsc_rb_size(aeron_spsc_rb_t *ring_buffer);

