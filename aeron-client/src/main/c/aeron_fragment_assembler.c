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

#include "aeron_fragment_assembler.h"
#include "aeron_image.h"
#include "util/aeron_bitutil.h"

#define AERON_BUFFER_BUILDER_MIN_ALLOCATED_CAPACITY (4096)
#define AERON_BUFFER_BUILDER_MAX_CAPACITY (INT32_MAX - 8)

int aeron_buffer_builder_create(aeron_buffer_builder_t **buffer_builder)
{
    aeron_buffer_builder_t *_buffer_builder;
    aeron_data_header_t *_frame;

    if (aeron_alloc((void **)&_buffer_builder, sizeof(aeron_buffer_builder_t)) < 0 ||
        aeron_alloc((void **)&_frame, sizeof(aeron_data_header_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to allocate buffer_builder");
        return -1;
    }

    _buffer_builder->buffer = NULL;
    _buffer_builder->buffer_length = 0;
    _buffer_builder->limit = 0;
    _buffer_builder->next_term_offset = -1;
    _buffer_builder->header.frame = _frame;

    *buffer_builder = _buffer_builder;
    return 0;
}

int aeron_buffer_builder_find_suitable_capacity(size_t current_capacity, size_t required_capacity)
{
    size_t capacity = current_capacity;

    do
    {
        const size_t candidate_capacity = capacity + (capacity >> 1u);
        const size_t new_capacity = candidate_capacity > AERON_BUFFER_BUILDER_MIN_ALLOCATED_CAPACITY ?
            candidate_capacity : AERON_BUFFER_BUILDER_MIN_ALLOCATED_CAPACITY;

        if (new_capacity > AERON_BUFFER_BUILDER_MAX_CAPACITY)
        {
            if (AERON_BUFFER_BUILDER_MAX_CAPACITY == capacity)
            {
                AERON_SET_ERR(EINVAL, "max capacity reached: %" PRId32, AERON_BUFFER_BUILDER_MAX_CAPACITY);
                return -1;
            }

            capacity = AERON_BUFFER_BUILDER_MAX_CAPACITY;
        }
        else
        {
            capacity = new_capacity;
        }
    }
    while (capacity < required_capacity);

    return (int)capacity;
}

int aeron_buffer_builder_ensure_capacity(aeron_buffer_builder_t *buffer_builder, size_t additional_capacity)
{
    size_t required_capacity = buffer_builder->limit + additional_capacity;

    if (required_capacity > buffer_builder->buffer_length)
    {
        int suitable_capacity = aeron_buffer_builder_find_suitable_capacity(
            buffer_builder->buffer_length, required_capacity);

        if (suitable_capacity < 0)
        {
            return -1;
        }

        if (aeron_reallocf((void **)&buffer_builder->buffer, (size_t)suitable_capacity) < 0)
        {
            AERON_APPEND_ERR("%s", "Unable to reallocate buffer_builder->builder");
            return -1;
        }

        buffer_builder->buffer_length = (size_t)suitable_capacity;
    }

    return 0;
}

void aeron_buffer_builder_delete(aeron_buffer_builder_t *buffer_builder)
{
    if (buffer_builder)
    {
        aeron_free(buffer_builder->buffer);
        aeron_free(buffer_builder->header.frame);
        aeron_free(buffer_builder);
    }
}

int aeron_image_fragment_assembler_create(
    aeron_image_fragment_assembler_t **assembler,
    aeron_fragment_handler_t delegate,
    void *delegate_clientd)
{
    aeron_image_fragment_assembler_t *_assembler;

    if (aeron_alloc((void **)&_assembler, sizeof(aeron_image_fragment_assembler_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to allocate assembler");
        return -1;
    }

    if (aeron_buffer_builder_create(&_assembler->buffer_builder) < 0)
    {
        return -1;
    }

    _assembler->delegate = delegate;
    _assembler->delegate_clientd = delegate_clientd;

    *assembler = _assembler;
    return 0;
}

int aeron_image_fragment_assembler_delete(aeron_image_fragment_assembler_t *assembler)
{
    if (assembler)
    {
        aeron_buffer_builder_delete(assembler->buffer_builder);
        aeron_free(assembler);
    }

    return 0;
}

void aeron_image_fragment_assembler_handler(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    aeron_image_fragment_assembler_t *assembler = (aeron_image_fragment_assembler_t *)clientd;
    aeron_buffer_builder_t *buffer_builder = assembler->buffer_builder;
    uint8_t flags = header->frame->frame_header.flags;

    if ((flags & AERON_DATA_HEADER_UNFRAGMENTED) == AERON_DATA_HEADER_UNFRAGMENTED)
    {
        assembler->delegate(assembler->delegate_clientd, buffer, length, header);
    }
    else if ((flags & AERON_DATA_HEADER_BEGIN_FLAG) == AERON_DATA_HEADER_BEGIN_FLAG)
    {
        aeron_buffer_builder_reset(buffer_builder);
        aeron_buffer_builder_capture_header(buffer_builder, header);
        aeron_buffer_builder_append(buffer_builder, buffer, length);
        int32_t next_term_offset = aeron_header_next_term_offset(header);
        aeron_buffer_builder_next_term_offset(buffer_builder, next_term_offset);
    }
    else if (buffer_builder->next_term_offset == header->frame->term_offset)
    {
        aeron_buffer_builder_append(buffer_builder, buffer, length);

        if ((flags & AERON_DATA_HEADER_END_FLAG) == AERON_DATA_HEADER_END_FLAG)
        {
            assembler->delegate(
                assembler->delegate_clientd,
                buffer_builder->buffer,
                buffer_builder->limit,
                aeron_buffer_builder_complete_header(buffer_builder, header));
            aeron_buffer_builder_reset(buffer_builder);
        }
        else
        {
            int32_t next_term_offset = aeron_header_next_term_offset(header);
            aeron_buffer_builder_next_term_offset(buffer_builder, next_term_offset);
        }
    }
    else
    {
        aeron_buffer_builder_reset(buffer_builder);
    }
}

int aeron_image_controlled_fragment_assembler_create(
    aeron_image_controlled_fragment_assembler_t **assembler,
    aeron_controlled_fragment_handler_t delegate,
    void *delegate_clientd)
{
    aeron_image_controlled_fragment_assembler_t *_assembler;

    if (aeron_alloc((void **)&_assembler, sizeof(aeron_image_controlled_fragment_assembler_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to allocate assembler");
        return -1;
    }

    if (aeron_buffer_builder_create(&_assembler->buffer_builder) < 0)
    {
        return -1;
    }

    _assembler->delegate = delegate;
    _assembler->delegate_clientd = delegate_clientd;

    *assembler = _assembler;
    return 0;
}

int aeron_image_controlled_fragment_assembler_delete(aeron_image_controlled_fragment_assembler_t *assembler)
{
    if (assembler)
    {
        aeron_buffer_builder_delete(assembler->buffer_builder);
        aeron_free(assembler);
    }

    return 0;
}

aeron_controlled_fragment_handler_action_t aeron_image_controlled_fragment_assembler_handler(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    aeron_image_controlled_fragment_assembler_t *assembler = (aeron_image_controlled_fragment_assembler_t *)clientd;
    aeron_buffer_builder_t *buffer_builder = assembler->buffer_builder;
    uint8_t flags = header->frame->frame_header.flags;
    aeron_controlled_fragment_handler_action_t action = AERON_ACTION_CONTINUE;

    if ((flags & AERON_DATA_HEADER_UNFRAGMENTED) == AERON_DATA_HEADER_UNFRAGMENTED)
    {
        action = assembler->delegate(assembler->delegate_clientd, buffer, length, header);
    }
    else if ((flags & AERON_DATA_HEADER_BEGIN_FLAG) == AERON_DATA_HEADER_BEGIN_FLAG)
    {
        aeron_buffer_builder_reset(buffer_builder);
        aeron_buffer_builder_capture_header(buffer_builder, header);
        aeron_buffer_builder_append(buffer_builder, buffer, length);
        int32_t next_term_offset = aeron_header_next_term_offset(header);
        aeron_buffer_builder_next_term_offset(buffer_builder, next_term_offset);
    }
    else if (buffer_builder->next_term_offset == header->frame->term_offset)
    {
        size_t limit = buffer_builder->limit;
        aeron_buffer_builder_append(buffer_builder, buffer, length);

        if ((flags & AERON_DATA_HEADER_END_FLAG) == AERON_DATA_HEADER_END_FLAG)
        {
            action = assembler->delegate(
                assembler->delegate_clientd,
                buffer_builder->buffer,
                buffer_builder->limit,
                aeron_buffer_builder_complete_header(buffer_builder, header));

            if (AERON_ACTION_ABORT == action)
            {
                buffer_builder->limit = limit;
            }
            else
            {
                aeron_buffer_builder_reset(buffer_builder);
            }
        }
        else
        {
            int32_t next_term_offset = aeron_header_next_term_offset(header);
            aeron_buffer_builder_next_term_offset(buffer_builder, next_term_offset);
        }
    }
    else
    {
        aeron_buffer_builder_reset(buffer_builder);
    }

    return action;
}

int aeron_fragment_assembler_create(
    aeron_fragment_assembler_t **assembler,
    aeron_fragment_handler_t delegate,
    void *delegate_clientd)
{
    aeron_fragment_assembler_t *_assembler;

    if (aeron_alloc((void **)&_assembler, sizeof(aeron_fragment_assembler_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to allocate assembler");
        return -1;
    }

    if (aeron_int64_to_ptr_hash_map_init(
        &_assembler->builder_by_session_id_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to init builder_by_session_id_map");
        return -1;
    }

    _assembler->delegate = delegate;
    _assembler->delegate_clientd = delegate_clientd;

    *assembler = _assembler;
    return 0;
}

void aeron_fragment_assembler_entry_delete(void *clientd, int64_t key, void *value)
{
    aeron_buffer_builder_t *builder = (aeron_buffer_builder_t *)value;

    aeron_buffer_builder_delete(builder);
}

int aeron_fragment_assembler_delete(aeron_fragment_assembler_t *assembler)
{
    if (assembler)
    {
        aeron_int64_to_ptr_hash_map_for_each(
            &assembler->builder_by_session_id_map, aeron_fragment_assembler_entry_delete, NULL);
        aeron_int64_to_ptr_hash_map_delete(&assembler->builder_by_session_id_map);
        aeron_free(assembler);
    }

    return 0;
}

void aeron_fragment_assembler_handler(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    aeron_fragment_assembler_t *assembler = (aeron_fragment_assembler_t *)clientd;
    uint8_t flags = header->frame->frame_header.flags;

    if ((flags & AERON_DATA_HEADER_UNFRAGMENTED) == AERON_DATA_HEADER_UNFRAGMENTED)
    {
        assembler->delegate(assembler->delegate_clientd, buffer, length, header);
    }
    else
    {
        aeron_buffer_builder_t *buffer_builder = aeron_int64_to_ptr_hash_map_get(
            &assembler->builder_by_session_id_map, header->frame->session_id);
        int32_t next_term_offset = aeron_header_next_term_offset(header);

        if ((flags & AERON_DATA_HEADER_BEGIN_FLAG) == AERON_DATA_HEADER_BEGIN_FLAG)
        {
            if (NULL == buffer_builder)
            {
                if (aeron_buffer_builder_create(&buffer_builder) < 0 ||
                    aeron_int64_to_ptr_hash_map_put(
                        &assembler->builder_by_session_id_map, header->frame->session_id, buffer_builder) < 0)
                {
                    return;
                }
            }

            aeron_buffer_builder_reset(buffer_builder);
            aeron_buffer_builder_capture_header(buffer_builder, header);
            aeron_buffer_builder_append(buffer_builder, buffer, length);
            aeron_buffer_builder_next_term_offset(buffer_builder, next_term_offset);
        }
        else if (NULL != buffer_builder)
        {
            if (buffer_builder->next_term_offset == header->frame->term_offset)
            {
                aeron_buffer_builder_append(buffer_builder, buffer, length);

                if ((flags & AERON_DATA_HEADER_END_FLAG) == AERON_DATA_HEADER_END_FLAG)
                {
                    assembler->delegate(
                        assembler->delegate_clientd,
                        buffer_builder->buffer,
                        buffer_builder->limit,
                        aeron_buffer_builder_complete_header(buffer_builder, header));
                    aeron_buffer_builder_reset(buffer_builder);
                }
                else
                {
                    aeron_buffer_builder_next_term_offset(buffer_builder, next_term_offset);
                }
            }
            else
            {
                aeron_buffer_builder_reset(buffer_builder);
            }
        }
    }
}

int aeron_controlled_fragment_assembler_create(
    aeron_controlled_fragment_assembler_t **assembler,
    aeron_controlled_fragment_handler_t delegate,
    void *delegate_clientd)
{
    aeron_controlled_fragment_assembler_t *_assembler;

    if (aeron_alloc((void **)&_assembler, sizeof(aeron_controlled_fragment_assembler_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to allocate assembler");
        return -1;
    }

    if (aeron_int64_to_ptr_hash_map_init(
        &_assembler->builder_by_session_id_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to init builder_by_session_id_map");
        return -1;
    }

    _assembler->delegate = delegate;
    _assembler->delegate_clientd = delegate_clientd;

    *assembler = _assembler;
    return 0;
}

void aeron_controlled_fragment_assembler_entry_delete(void *clientd, int64_t key, void *value)
{
    aeron_buffer_builder_t *builder = (aeron_buffer_builder_t *)value;

    aeron_buffer_builder_delete(builder);
}

int aeron_controlled_fragment_assembler_delete(aeron_controlled_fragment_assembler_t *assembler)
{
    if (assembler)
    {
        aeron_int64_to_ptr_hash_map_for_each(
            &assembler->builder_by_session_id_map, aeron_controlled_fragment_assembler_entry_delete, NULL);
        aeron_int64_to_ptr_hash_map_delete(&assembler->builder_by_session_id_map);
        aeron_free(assembler);
    }

    return 0;
}

aeron_controlled_fragment_handler_action_t aeron_controlled_fragment_assembler_handler(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    aeron_controlled_fragment_assembler_t *assembler = (aeron_controlled_fragment_assembler_t *)clientd;
    uint8_t flags = header->frame->frame_header.flags;
    aeron_controlled_fragment_handler_action_t action = AERON_ACTION_CONTINUE;

    if ((flags & AERON_DATA_HEADER_UNFRAGMENTED) == AERON_DATA_HEADER_UNFRAGMENTED)
    {
        action = assembler->delegate(assembler->delegate_clientd, buffer, length, header);
    }
    else
    {
        aeron_buffer_builder_t *buffer_builder = aeron_int64_to_ptr_hash_map_get(
            &assembler->builder_by_session_id_map, header->frame->session_id);

        if ((flags & AERON_DATA_HEADER_BEGIN_FLAG) == AERON_DATA_HEADER_BEGIN_FLAG)
        {
            if (NULL == buffer_builder)
            {
                if (aeron_buffer_builder_create(&buffer_builder) < 0 ||
                    aeron_int64_to_ptr_hash_map_put(
                        &assembler->builder_by_session_id_map, header->frame->session_id, buffer_builder) < 0)
                {
                    return AERON_ACTION_ABORT;
                }
            }

            aeron_buffer_builder_reset(buffer_builder);
            aeron_buffer_builder_capture_header(buffer_builder, header);
            aeron_buffer_builder_append(buffer_builder, buffer, length);
            aeron_buffer_builder_next_term_offset(buffer_builder, aeron_header_next_term_offset(header));
        }
        else if (NULL != buffer_builder)
        {
            if (buffer_builder->next_term_offset == header->frame->term_offset)
            {
                size_t limit = buffer_builder->limit;
                aeron_buffer_builder_append(buffer_builder, buffer, length);

                if ((flags & AERON_DATA_HEADER_END_FLAG) == AERON_DATA_HEADER_END_FLAG)
                {
                    action = assembler->delegate(
                        assembler->delegate_clientd,
                        buffer_builder->buffer,
                        buffer_builder->limit,
                        aeron_buffer_builder_complete_header(buffer_builder, header));

                    if (AERON_ACTION_ABORT == action)
                    {
                        buffer_builder->limit = limit;
                    }
                    else
                    {
                        aeron_buffer_builder_reset(buffer_builder);
                    }
                }
                else
                {
                    aeron_buffer_builder_next_term_offset(buffer_builder, aeron_header_next_term_offset(header));
                }
            }
            else
            {
                aeron_buffer_builder_reset(buffer_builder);
            }
        }
    }

    return action;
}

extern void aeron_buffer_builder_reset(aeron_buffer_builder_t *buffer_builder);

extern void aeron_buffer_builder_next_term_offset(aeron_buffer_builder_t *buffer_builder, int32_t next_term_offset);

extern int aeron_buffer_builder_append(
    aeron_buffer_builder_t *buffer_builder, const uint8_t *buffer, size_t length);

extern void aeron_buffer_builder_capture_header(aeron_buffer_builder_t *buffer_builder, aeron_header_t *header);

extern aeron_header_t *aeron_buffer_builder_complete_header(
    aeron_buffer_builder_t *buffer_builder, aeron_header_t *header);
