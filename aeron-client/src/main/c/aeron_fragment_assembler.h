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

#ifndef AERON_C_IMAGE_FRAGMENT_ASSEMBLER_H
#define AERON_C_IMAGE_FRAGMENT_ASSEMBLER_H

#include <string.h>

#include "aeronc.h"
#include "collections/aeron_int64_to_ptr_hash_map.h"
#include "aeron_image.h"

typedef struct aeron_buffer_builder_stct
{
    uint8_t *buffer;
    size_t buffer_length;
    size_t limit;
    int32_t next_term_offset;
    aeron_header_t header;
}
aeron_buffer_builder_t;

typedef struct aeron_image_fragment_assembler_stct
{
    aeron_fragment_handler_t delegate;
    void *delegate_clientd;
    aeron_buffer_builder_t *buffer_builder;
}
aeron_image_fragment_assembler_t;

typedef struct aeron_image_controlled_fragment_assembler_stct
{
    aeron_controlled_fragment_handler_t delegate;
    void *delegate_clientd;
    aeron_buffer_builder_t *buffer_builder;
}
aeron_image_controlled_fragment_assembler_t;

typedef struct aeron_fragment_assembler_stct
{
    aeron_fragment_handler_t delegate;
    void *delegate_clientd;
    aeron_int64_to_ptr_hash_map_t builder_by_session_id_map;
}
aeron_fragment_assembler_t;

typedef struct aeron_controlled_fragment_assembler_stct
{
    aeron_controlled_fragment_handler_t delegate;
    void *delegate_clientd;
    aeron_int64_to_ptr_hash_map_t builder_by_session_id_map;
}
aeron_controlled_fragment_assembler_t;

int aeron_buffer_builder_create(aeron_buffer_builder_t **buffer_builder);
int aeron_buffer_builder_find_suitable_capacity(size_t current_capacity, size_t required_capacity);
int aeron_buffer_builder_ensure_capacity(aeron_buffer_builder_t *buffer_builder, size_t additional_capacity);
void aeron_buffer_builder_delete(aeron_buffer_builder_t *buffer_builder);

inline void aeron_buffer_builder_reset(aeron_buffer_builder_t *buffer_builder)
{
    buffer_builder->limit = 0;
    buffer_builder->next_term_offset = -1;
    buffer_builder->header.fragmented_frame_length = AERON_NULL_VALUE;
    buffer_builder->header.context = NULL;
}

inline void aeron_buffer_builder_next_term_offset(aeron_buffer_builder_t *buffer_builder, int32_t next_term_offset)
{
    buffer_builder->next_term_offset = next_term_offset;
}

inline int aeron_buffer_builder_append(
    aeron_buffer_builder_t *buffer_builder, const uint8_t *buffer, size_t length)
{
    if (aeron_buffer_builder_ensure_capacity(buffer_builder, length) < 0)
    {
        return -1;
    }

    memcpy(buffer_builder->buffer + buffer_builder->limit, buffer, length);
    buffer_builder->limit += length;
    return 0;
}

inline void aeron_buffer_builder_capture_header(aeron_buffer_builder_t *buffer_builder, aeron_header_t *header)
{
    buffer_builder->header.initial_term_id = header->initial_term_id;
    buffer_builder->header.position_bits_to_shift = header->position_bits_to_shift;
    memcpy(buffer_builder->header.frame, header->frame, sizeof(aeron_data_header_t));
}

inline aeron_header_t* aeron_buffer_builder_complete_header(aeron_buffer_builder_t *buffer_builder, aeron_header_t *header)
{
    buffer_builder->header.context = header->context;
    aeron_frame_header_t *frame_header = &buffer_builder->header.frame->frame_header;

    int32_t max_payload_length = frame_header->frame_length - (int32_t)AERON_DATA_HEADER_LENGTH;
    int32_t fragmented_frame_length = (int32_t)aeron_logbuffer_compute_fragmented_length(
        buffer_builder->limit, max_payload_length);
    buffer_builder->header.fragmented_frame_length = fragmented_frame_length;

    frame_header->frame_length = (int32_t)AERON_DATA_HEADER_LENGTH + (int32_t)buffer_builder->limit;
    frame_header->flags |= header->frame->frame_header.flags;

    return &buffer_builder->header;
}

#endif //AERON_C_IMAGE_FRAGMENT_ASSEMBLER_H
