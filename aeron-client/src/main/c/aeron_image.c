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

#include "aeron_image.h"
#include "aeron_alloc.h"
#include "aeron_log_buffer.h"
#include "aeron_subscription.h"

_Static_assert(
    sizeof(aeron_header_values_frame_t) == sizeof(aeron_data_header_t),
    "sizeof(aeron_header_values_frame_t) must match sizeof(aeron_data_header_t)");
_Static_assert(
    offsetof(aeron_header_values_frame_t, frame_length) == offsetof(aeron_frame_header_t, frame_length),
    "offsetof(aeron_header_values_frame_t, frame_length) must match offsetof(aeron_frame_header_t, frame_length)");
_Static_assert(
    offsetof(aeron_header_values_frame_t, version) == offsetof(aeron_frame_header_t, version),
    "offsetof(aeron_header_values_frame_t, version) must match offsetof(aeron_frame_header_t, version)");
_Static_assert(
    offsetof(aeron_header_values_frame_t, flags) == offsetof(aeron_frame_header_t, flags),
    "offsetof(aeron_header_values_frame_t, flags) == offsetof(aeron_frame_header_t, flags)");
_Static_assert(
    offsetof(aeron_header_values_frame_t, type) == offsetof(aeron_frame_header_t, type),
    "offsetof(aeron_header_values_frame_t, type) == offsetof(aeron_frame_header_t, type)");
_Static_assert(
    offsetof(aeron_header_values_frame_t, term_offset) == offsetof(aeron_data_header_t, term_offset),
    "offsetof(aeron_header_values_frame_t, term_offset) == offsetof(aeron_data_header_t, term_offset)");
_Static_assert(
    offsetof(aeron_header_values_frame_t, session_id) == offsetof(aeron_data_header_t, session_id),
    "offsetof(aeron_header_values_frame_t, session_id) == offsetof(aeron_data_header_t, session_id)");
_Static_assert(
    offsetof(aeron_header_values_frame_t, stream_id) == offsetof(aeron_data_header_t, stream_id),
    "offsetof(aeron_header_values_frame_t, stream_id) == offsetof(aeron_data_header_t, stream_id)");
_Static_assert(
    offsetof(aeron_header_values_frame_t, term_id) == offsetof(aeron_data_header_t, term_id),
    "offsetof(aeron_header_values_frame_t, term_id) == offsetof(aeron_data_header_t, term_id)");
_Static_assert(
    offsetof(aeron_header_values_frame_t, reserved_value) == offsetof(aeron_data_header_t, reserved_value),
    "offsetof(aeron_header_values_frame_t, reserved_value) == offsetof(aeron_data_header_t, reserved_value)");

int aeron_image_create(
    aeron_image_t **image,
    aeron_subscription_t *subscription,
    aeron_client_conductor_t *conductor,
    aeron_log_buffer_t *log_buffer,
    int32_t subscriber_position_id,
    int64_t *subscriber_position,
    int64_t correlation_id,
    int32_t session_id,
    const char *source_identity,
    size_t source_identity_length)
{
    aeron_image_t *_image;

    *image = NULL;
    if (aeron_alloc((void **)&_image, sizeof(aeron_image_t)) < 0 ||
        aeron_alloc((void **)&_image->source_identity, source_identity_length + 1) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to allocate aeron_image and source_identity");
        return -1;
    }

    _image->command_base.type = AERON_CLIENT_TYPE_IMAGE;

    memcpy(_image->source_identity, source_identity, source_identity_length);
    _image->source_identity[source_identity_length] = '\0';

    _image->subscription = subscription;
    _image->log_buffer = log_buffer;

    _image->subscriber_position = subscriber_position;
    _image->subscriber_position_id = subscriber_position_id;

    _image->conductor = conductor;
    _image->key.correlation_id = correlation_id;
    _image->key.subscription_registration_id = subscription->registration_id;
    _image->session_id = session_id;
    _image->removal_change_number = INT64_MAX;
    _image->final_position = 0;
    _image->join_position = *subscriber_position;
    _image->eos_position = INT64_MAX;
    _image->refcnt = 1;

    _image->metadata =
        (aeron_logbuffer_metadata_t *)log_buffer->mapped_raw_log.log_meta_data.addr;
    int32_t term_length = _image->metadata->term_length;

    _image->term_length_mask = term_length - 1;
    _image->position_bits_to_shift = (size_t)aeron_number_of_trailing_zeroes(term_length);

    _image->is_closed = false;
    _image->is_lingering = false;

    *image = _image;

    return 0;
}

int aeron_image_delete(aeron_image_t *image)
{
    aeron_free((void *)image->source_identity);
    aeron_free(image);

    return 0;
}

void aeron_image_close(aeron_image_t *image)
{
    AERON_GET_ACQUIRE(image->eos_position, image->metadata->end_of_stream_position);
    image->final_position = aeron_counter_get_acquire(image->subscriber_position);
    image->is_eos = image->final_position >= image->eos_position;
    AERON_SET_RELEASE(image->is_closed, true);
}

int aeron_image_constants(aeron_image_t *image, aeron_image_constants_t *constants)
{
    if (NULL == image || NULL == constants)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, image: %s, constants: %s",
            AERON_NULL_STR(image),
            AERON_NULL_STR(constants));
        return -1;
    }

    constants->subscription = image->subscription;
    constants->source_identity = image->source_identity;
    constants->correlation_id = image->key.correlation_id;
    constants->join_position = image->join_position;
    constants->position_bits_to_shift = image->position_bits_to_shift;
    constants->term_buffer_length = (size_t)image->term_length_mask + 1;
    constants->mtu_length = (size_t)image->metadata->mtu_length;
    constants->session_id = image->session_id;
    constants->initial_term_id = image->metadata->initial_term_id;
    constants->subscriber_position_id = image->subscriber_position_id;

    return 0;
}

int64_t aeron_image_position(aeron_image_t *image)
{
    if (NULL == image)
    {
        AERON_SET_ERR(EINVAL, "Parameters must not be null, image: %s", AERON_NULL_STR(image));
        return -1;
    }

    bool is_closed;
    AERON_GET_ACQUIRE(is_closed, image->is_closed);
    if (is_closed)
    {
        return image->final_position;
    }

    return *image->subscriber_position;
}

int aeron_image_set_position(aeron_image_t *image, int64_t position)
{
    if (NULL == image)
    {
        AERON_SET_ERR(EINVAL, "Parameters must not be null, image: %s", AERON_NULL_STR(image));
        return -1;
    }

    bool is_closed;
    AERON_GET_ACQUIRE(is_closed, image->is_closed);
    if (!is_closed)
    {
        if (aeron_image_validate_position(image, position) < 0)
        {
            return -1;
        }

        AERON_SET_RELEASE(*image->subscriber_position, position);
    }

    return 0;
}

bool aeron_image_is_end_of_stream(aeron_image_t *image)
{
    if (NULL == image)
    {
        AERON_SET_ERR(EINVAL, "Parameters must not be null, image: %s", AERON_NULL_STR(image));
        return -1;
    }

    bool is_closed;
    AERON_GET_ACQUIRE(is_closed, image->is_closed);
    if (is_closed)
    {
        return image->is_eos;
    }

    int64_t end_of_stream_position, subscriber_position;
    AERON_GET_ACQUIRE(end_of_stream_position, image->metadata->end_of_stream_position);
    AERON_GET_ACQUIRE(subscriber_position, *image->subscriber_position);

    return subscriber_position >= end_of_stream_position;
}

int64_t aeron_image_end_of_stream_position(aeron_image_t *image)
{
    bool is_closed;
    AERON_GET_ACQUIRE(is_closed, image->is_closed);
    if (is_closed)
    {
        return image->eos_position;
    }

    int64_t end_of_stream_position;
    AERON_GET_ACQUIRE(end_of_stream_position, image->metadata->end_of_stream_position);

    return end_of_stream_position;
}

int aeron_image_active_transport_count(aeron_image_t *image)
{
    if (NULL == image)
    {
        AERON_SET_ERR(EINVAL, "Parameters must not be null, image: %s", AERON_NULL_STR(image));
        return -1;
    }

    bool is_closed;
    AERON_GET_ACQUIRE(is_closed, image->is_closed);
    if (is_closed)
    {
        return 0;
    }

    int32_t active_transport_count;
    AERON_GET_ACQUIRE(active_transport_count, image->metadata->active_transport_count);

    return (int)active_transport_count;
}

bool aeron_image_is_publication_revoked(aeron_image_t *image)
{
    if (NULL == image)
    {
        AERON_SET_ERR(EINVAL, "Parameters must not be null, image: %s", AERON_NULL_STR(image));
        return -1;
    }

    bool is_publication_revoked;

    AERON_GET_ACQUIRE(is_publication_revoked, image->metadata->is_publication_revoked);

    return is_publication_revoked;
}

int aeron_image_poll(aeron_image_t *image, aeron_fragment_handler_t handler, void *clientd, size_t fragment_limit)
{
    if (NULL == image || NULL == handler)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, image: %s, handler: %s",
            AERON_NULL_STR(image),
            AERON_NULL_STR(handler));
        return -1;
    }

    bool is_closed;
    AERON_GET_ACQUIRE(is_closed, image->is_closed);
    if (is_closed)
    {
        return 0;
    }

    size_t fragments_read = 0;
    const int64_t initial_position = *image->subscriber_position;
    const size_t index = aeron_logbuffer_index_by_position(initial_position, image->position_bits_to_shift);
    const uint8_t *term_buffer = image->log_buffer->mapped_raw_log.term_buffers[index].addr;
    const int32_t initial_offset = (int32_t)initial_position & image->term_length_mask;
    const int32_t capacity = (const int32_t)image->log_buffer->mapped_raw_log.term_length;
    int32_t offset = initial_offset;

    while (fragments_read < fragment_limit && offset < capacity)
    {
        AERON_GET_ACQUIRE(is_closed, image->is_closed);
        if (is_closed)
        {
            break;
        }

        aeron_data_header_t *frame = (aeron_data_header_t *)(term_buffer + offset);
        int32_t frame_length, frame_offset;

        AERON_GET_ACQUIRE(frame_length, frame->frame_header.frame_length);

        if (frame_length <= 0)
        {
            break;
        }

        frame_offset = offset;
        offset += AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);

        if (AERON_HDR_TYPE_PAD != frame->frame_header.type)
        {
            aeron_header_t header =
                {
                    frame,
                    image->metadata->initial_term_id,
                    image->position_bits_to_shift,
                    AERON_NULL_VALUE,
                    (void *)image
                };

            ++fragments_read;
            handler(
                clientd,
                term_buffer + frame_offset + AERON_DATA_HEADER_LENGTH,
                frame_length - AERON_DATA_HEADER_LENGTH,
                &header);
        }
    }

    int64_t new_position = initial_position + (offset - initial_offset);
    if (new_position > initial_position)
    {
        aeron_counter_set_release(image->subscriber_position, new_position);
    }

    return (int)fragments_read;
}

int aeron_image_controlled_poll(
    aeron_image_t *image, aeron_controlled_fragment_handler_t handler, void *clientd, size_t fragment_limit)
{
    if (NULL == image || NULL == handler)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, image: %s, handler: %s",
            AERON_NULL_STR(image),
            AERON_NULL_STR(handler));
        return -1;
    }

    bool is_closed;
    AERON_GET_ACQUIRE(is_closed, image->is_closed);
    if (is_closed)
    {
        return 0;
    }

    size_t fragments_read = 0;
    int64_t initial_position = *image->subscriber_position;
    const size_t index = aeron_logbuffer_index_by_position(initial_position, image->position_bits_to_shift);
    const uint8_t *term_buffer = image->log_buffer->mapped_raw_log.term_buffers[index].addr;
    const int32_t capacity = (const int32_t)image->log_buffer->mapped_raw_log.term_length;
    int32_t initial_offset = (int32_t)initial_position & image->term_length_mask;
    int32_t offset = initial_offset;

    while (fragments_read < fragment_limit && offset < capacity)
    {
        AERON_GET_ACQUIRE(is_closed, image->is_closed);
        if (is_closed)
        {
            break;
        }

        aeron_data_header_t *frame = (aeron_data_header_t *)(term_buffer + offset);
        int32_t frame_length, frame_offset, aligned_frame_length;

        AERON_GET_ACQUIRE(frame_length, frame->frame_header.frame_length);

        if (frame_length <= 0)
        {
            break;
        }

        frame_offset = offset;
        aligned_frame_length = AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);
        offset += aligned_frame_length;

        if (AERON_HDR_TYPE_PAD == frame->frame_header.type)
        {
            continue;
        }

        aeron_header_t header =
            {
                frame,
                image->metadata->initial_term_id,
                image->position_bits_to_shift,
                AERON_NULL_VALUE,
                (void *)image
            };

        ++fragments_read;
        aeron_controlled_fragment_handler_action_t action = handler(
            clientd,
            term_buffer + frame_offset + AERON_DATA_HEADER_LENGTH,
            frame_length - AERON_DATA_HEADER_LENGTH,
            &header);

        if (AERON_ACTION_ABORT == action)
        {
            --fragments_read;
            offset -= aligned_frame_length;
            break;
        }

        if (AERON_ACTION_BREAK == action)
        {
            break;
        }

        if (AERON_ACTION_COMMIT == action)
        {
            initial_position += (offset - initial_offset);
            initial_offset = offset;
            aeron_counter_set_release(image->subscriber_position, initial_position);
        }
    }

    int64_t new_position = initial_position + (offset - initial_offset);
    if (new_position > initial_position)
    {
        aeron_counter_set_release(image->subscriber_position, new_position);
    }

    return (int)fragments_read;
}

int aeron_image_bounded_poll(
    aeron_image_t *image,
    aeron_fragment_handler_t handler,
    void *clientd,
    int64_t limit_position,
    size_t fragment_limit)
{
    if (NULL == image || NULL == handler)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, image: %s, handler: %s",
            AERON_NULL_STR(image),
            AERON_NULL_STR(handler));
        return -1;
    }

    bool is_closed;
    AERON_GET_ACQUIRE(is_closed, image->is_closed);
    if (is_closed)
    {
        return 0;
    }

    const int64_t initial_position = *image->subscriber_position;
    if (initial_position >= limit_position)
    {
        return 0;
    }

    size_t fragments_read = 0;
    const size_t index = aeron_logbuffer_index_by_position(initial_position, image->position_bits_to_shift);
    const uint8_t *term_buffer = image->log_buffer->mapped_raw_log.term_buffers[index].addr;
    const int32_t initial_offset = (int32_t)initial_position & image->term_length_mask;
    const int32_t capacity = (const int32_t)image->log_buffer->mapped_raw_log.term_length;
    const int64_t high_limit_offset = limit_position - initial_position + initial_offset;
    const int32_t limit_offset = (int64_t)capacity < high_limit_offset ? capacity : (int32_t)high_limit_offset;
    int32_t offset = initial_offset;

    while (fragments_read < fragment_limit && offset < limit_offset)
    {
        AERON_GET_ACQUIRE(is_closed, image->is_closed);
        if (is_closed)
        {
            break;
        }

        aeron_data_header_t *frame = (aeron_data_header_t *)(term_buffer + offset);
        int32_t frame_length, frame_offset;

        AERON_GET_ACQUIRE(frame_length, frame->frame_header.frame_length);

        if (frame_length <= 0)
        {
            break;
        }

        frame_offset = offset;
        offset += AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);

        if (AERON_HDR_TYPE_PAD != frame->frame_header.type)
        {
            aeron_header_t header =
                {
                    frame,
                    image->metadata->initial_term_id,
                    image->position_bits_to_shift,
                    AERON_NULL_VALUE,
                    (void *)image
                };

            ++fragments_read;
            handler(
                clientd,
                term_buffer + frame_offset + AERON_DATA_HEADER_LENGTH,
                frame_length - AERON_DATA_HEADER_LENGTH,
                &header);
        }
    }

    int64_t new_position = initial_position + (offset - initial_offset);
    if (new_position > initial_position)
    {
        aeron_counter_set_release(image->subscriber_position, new_position);
    }

    return (int)fragments_read;
}

int aeron_image_bounded_controlled_poll(
    aeron_image_t *image,
    aeron_controlled_fragment_handler_t handler,
    void *clientd,
    int64_t limit_position,
    size_t fragment_limit)
{
    if (NULL == image || NULL == handler)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, image: %s, handler: %s",
            AERON_NULL_STR(image),
            AERON_NULL_STR(handler));
        return -1;
    }

    bool is_closed;
    AERON_GET_ACQUIRE(is_closed, image->is_closed);
    if (is_closed)
    {
        return 0;
    }

    int64_t initial_position = *image->subscriber_position;
    if (initial_position >= limit_position)
    {
        return 0;
    }

    size_t fragments_read = 0;
    const size_t index = aeron_logbuffer_index_by_position(initial_position, image->position_bits_to_shift);
    const uint8_t *term_buffer = image->log_buffer->mapped_raw_log.term_buffers[index].addr;
    const int32_t capacity = (const int32_t)image->log_buffer->mapped_raw_log.term_length;
    int32_t initial_offset = (int32_t)initial_position & image->term_length_mask;
    const int64_t high_limit_offset = limit_position - initial_position + initial_offset;
    const int32_t limit_offset = (int64_t)capacity < high_limit_offset ? capacity : (int32_t)high_limit_offset;
    int32_t offset = initial_offset;

    while (fragments_read < fragment_limit && offset < limit_offset)
    {
        AERON_GET_ACQUIRE(is_closed, image->is_closed);
        if (is_closed)
        {
            break;
        }

        aeron_data_header_t *frame = (aeron_data_header_t *)(term_buffer + offset);
        int32_t frame_length, frame_offset, aligned_frame_length;

        AERON_GET_ACQUIRE(frame_length, frame->frame_header.frame_length);

        if (frame_length <= 0)
        {
            break;
        }

        frame_offset = offset;
        aligned_frame_length = AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);
        offset += aligned_frame_length;

        if (AERON_HDR_TYPE_PAD == frame->frame_header.type)
        {
            continue;
        }

        aeron_header_t header =
            {
                frame,
                image->metadata->initial_term_id,
                image->position_bits_to_shift,
                AERON_NULL_VALUE,
                (void *)image
            };

        ++fragments_read;
        aeron_controlled_fragment_handler_action_t action = handler(
            clientd,
            term_buffer + frame_offset + AERON_DATA_HEADER_LENGTH,
            frame_length - AERON_DATA_HEADER_LENGTH,
            &header);

        if (AERON_ACTION_ABORT == action)
        {
            --fragments_read;
            offset -= aligned_frame_length;
            break;
        }

        if (AERON_ACTION_BREAK == action)
        {
            break;
        }

        if (AERON_ACTION_COMMIT == action)
        {
            initial_position += (offset - initial_offset);
            initial_offset = offset;
            aeron_counter_set_release(image->subscriber_position, initial_position);
        }
    }

    int64_t new_position = initial_position + (offset - initial_offset);
    if (new_position > initial_position)
    {
        aeron_counter_set_release(image->subscriber_position, new_position);
    }

    return (int)fragments_read;
}

int64_t aeron_image_controlled_peek(
    aeron_image_t *image,
    int64_t initial_position,
    aeron_controlled_fragment_handler_t handler,
    void *clientd,
    int64_t limit_position)
{
    if (NULL == image || NULL == handler)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, image: %s, handler: %s",
            AERON_NULL_STR(image),
            AERON_NULL_STR(handler));
        return -1;
    }

    bool is_closed;
    AERON_GET_ACQUIRE(is_closed, image->is_closed);
    if (is_closed)
    {
        return initial_position;
    }

    if (aeron_image_validate_position(image, initial_position) < 0)
    {
        return -1;
    }

    if (initial_position >= limit_position)
    {
        return initial_position;
    }

    int64_t position = initial_position, resulting_position = initial_position;
    const size_t index = aeron_logbuffer_index_by_position(initial_position, image->position_bits_to_shift);
    const uint8_t *term_buffer = image->log_buffer->mapped_raw_log.term_buffers[index].addr;
    const int32_t capacity = (const int32_t)image->log_buffer->mapped_raw_log.term_length;
    int32_t initial_offset = (int32_t)initial_position & image->term_length_mask;
    const int64_t high_limit_offset = limit_position - initial_position + initial_offset;
    const int32_t limit_offset = (int64_t)capacity < high_limit_offset ? capacity : (int32_t)high_limit_offset;
    int32_t offset = initial_offset;

    while (offset < limit_offset)
    {
        AERON_GET_ACQUIRE(is_closed, image->is_closed);
        if (is_closed)
        {
            break;
        }

        aeron_data_header_t *frame = (aeron_data_header_t *)(term_buffer + offset);
        int32_t frame_length, frame_offset;

        AERON_GET_ACQUIRE(frame_length, frame->frame_header.frame_length);

        if (frame_length <= 0)
        {
            break;
        }

        frame_offset = offset;
        offset += AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);

        if (AERON_HDR_TYPE_PAD == frame->frame_header.type)
        {
            position += (offset - initial_offset);
            initial_offset = offset;
            resulting_position = position;

            continue;
        }

        aeron_header_t header =
            {
                frame,
                image->metadata->initial_term_id,
                image->position_bits_to_shift,
                AERON_NULL_VALUE,
                (void *)image
            };

        aeron_controlled_fragment_handler_action_t action = handler(
            clientd,
            term_buffer + frame_offset + AERON_DATA_HEADER_LENGTH,
            frame_length - AERON_DATA_HEADER_LENGTH,
            &header);

        if (AERON_ACTION_ABORT == action)
        {
            break;
        }

        position += (offset - initial_offset);
        initial_offset = offset;

        if (frame->frame_header.flags & AERON_DATA_HEADER_END_FLAG)
        {
            resulting_position = position;
        }

        if (AERON_ACTION_BREAK == action)
        {
            break;
        }
    }

    return resulting_position;
}

int aeron_image_block_poll(
    aeron_image_t *image, aeron_block_handler_t handler, void *clientd, size_t block_length_limit)
{
    if (NULL == image || NULL == handler)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, image: %s, handler: %s",
            AERON_NULL_STR(image),
            AERON_NULL_STR(handler));
        return -1;
    }

    bool is_closed;
    AERON_GET_ACQUIRE(is_closed, image->is_closed);
    if (is_closed)
    {
        return 0;
    }

    const int64_t position = *image->subscriber_position;
    const size_t index = aeron_logbuffer_index_by_position(position, image->position_bits_to_shift);
    const uint8_t *term_buffer = image->log_buffer->mapped_raw_log.term_buffers[index].addr;
    const int32_t offset = (int32_t)position & image->term_length_mask;
    const int32_t capacity = (const int32_t)image->log_buffer->mapped_raw_log.term_length;
    const int64_t high_limit_offset = (int64_t)(offset + block_length_limit);
    const int32_t limit_offset = (int64_t)capacity < high_limit_offset ? capacity : (int32_t)high_limit_offset;
    int32_t scan_offset = offset;

    while (scan_offset < limit_offset)
    {
        aeron_data_header_t *frame = (aeron_data_header_t *)(term_buffer + scan_offset);
        int32_t frame_length, aligned_frame_length;

        AERON_GET_ACQUIRE(frame_length, frame->frame_header.frame_length);

        if (frame_length <= 0)
        {
            break;
        }

        aligned_frame_length = AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);

        if (AERON_HDR_TYPE_PAD == frame->frame_header.type)
        {
            if (offset == scan_offset)
            {
                scan_offset += aligned_frame_length;
            }

            break;
        }

        if (scan_offset + aligned_frame_length > limit_offset)
        {
            break;
        }

        scan_offset += aligned_frame_length;
    }

    int32_t resulting_offset = scan_offset;
    int32_t length = resulting_offset - offset;

    if (resulting_offset > offset)
    {
        int32_t term_id = ((aeron_data_header_t *)(term_buffer + offset))->term_id;

        handler(
            clientd,
            term_buffer + offset,
            (size_t)length,
            image->session_id,
            term_id);
    }

    aeron_counter_set_release(image->subscriber_position, position + length);

    return (int)length;
}

bool aeron_image_is_closed(aeron_image_t *image)
{
    bool is_closed = true;

    if (NULL != image)
    {
        AERON_GET_ACQUIRE(is_closed, image->is_closed);
    }

    return is_closed;
}

int aeron_image_reject(aeron_image_t *image, const char *reason)
{
    return aeron_subscription_reject_image(
        image->subscription,
        image->key.correlation_id,
        aeron_image_position(image),
        reason);
}

extern int64_t aeron_image_removal_change_number(aeron_image_t *image);

extern bool aeron_image_is_in_use_by_subscription(aeron_image_t *image, int64_t last_change_number);

extern int aeron_image_validate_position(aeron_image_t *image, int64_t position);

extern int64_t aeron_image_incr_refcnt(aeron_image_t *image);

extern int64_t aeron_image_decr_refcnt(aeron_image_t *image);

extern int64_t aeron_image_refcnt_acquire(aeron_image_t *image);
