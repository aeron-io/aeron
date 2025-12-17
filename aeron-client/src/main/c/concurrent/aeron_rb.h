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

#ifndef AERON_RB_H
#define AERON_RB_H

#include <string.h>
#include "util/aeron_bitutil.h"
#include "concurrent/aeron_atomic.h"

#pragma pack(push)
#pragma pack(4)
struct aeron_rb_descriptor_stct
{
    uint8_t begin_pad[(2 * AERON_CACHE_LINE_LENGTH)];
    volatile int64_t tail_position;
    uint8_t tail_pad[(2 * AERON_CACHE_LINE_LENGTH) - sizeof(int64_t)];
    volatile int64_t head_cache_position;
    uint8_t head_cache_pad[(2 * AERON_CACHE_LINE_LENGTH) - sizeof(int64_t)];
    volatile int64_t head_position;
    uint8_t head_pad[(2 * AERON_CACHE_LINE_LENGTH) - sizeof(int64_t)];
    volatile int64_t correlation_counter;
    uint8_t correlation_counter_pad[(2 * AERON_CACHE_LINE_LENGTH) - sizeof(int64_t)];
    volatile int64_t consumer_heartbeat;
    uint8_t consumer_heartbeat_pad[(2 * AERON_CACHE_LINE_LENGTH) - sizeof(int64_t)];
};
typedef struct aeron_rb_descriptor_stct aeron_rb_descriptor_t;

struct aeron_rb_record_descriptor_stct
{
    volatile int32_t length;
    int32_t msg_type_id;
};
typedef struct aeron_rb_record_descriptor_stct aeron_rb_record_descriptor_t;
#pragma pack(pop)

#define AERON_RB_TRAILER_LENGTH (sizeof(aeron_rb_descriptor_t))

#define AERON_RB_ALIGNMENT (2 * sizeof(int32_t))

#define AERON_RB_MESSAGE_OFFSET(index) ((index) + sizeof(aeron_rb_record_descriptor_t))
#define AERON_RB_RECORD_HEADER_LENGTH (sizeof(aeron_rb_record_descriptor_t))

#define AERON_RB_MAX_MESSAGE_LENGTH(capacity, min_capacity) (capacity) == (min_capacity) ? 0 : ((capacity) / 8)
#define AERON_RB_INVALID_MSG_TYPE_ID(id) ((id) < 1)
#define AERON_RB_PADDING_MSG_TYPE_ID (-1)

enum aeron_rb_write_result_stct
{
    AERON_RB_SUCCESS = 0,
    AERON_RB_ERROR = -2,
    AERON_RB_FULL = -1
};
typedef enum aeron_rb_write_result_stct aeron_rb_write_result_t;

typedef void (*aeron_rb_handler_t)(int32_t, const void *, size_t, void *);

enum aeron_rb_read_action_stct
{
    AERON_RB_ABORT, AERON_RB_BREAK, AERON_RB_COMMIT, AERON_RB_CONTINUE
};
typedef enum aeron_rb_read_action_stct aeron_rb_read_action_t;

typedef aeron_rb_read_action_t (*aeron_rb_controlled_handler_t)(int32_t, const void *, size_t, void *);

#define AERON_RB_IS_CAPACITY_VALID(capacity, min_capacity) AERON_IS_POWER_OF_TWO(capacity) && (capacity) >= (min_capacity)

#endif //AERON_RB_H
