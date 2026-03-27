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
#include <stdint.h>
#include <errno.h>

#include "aeron_expandable_ring_buffer.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

/* -----------------------------------------------------------------------
 * Internal helpers
 * ----------------------------------------------------------------------- */

static int erb_align(int val, int align)
{
    return (val + align - 1) & ~(align - 1);
}

/* Returns smallest power of 2 >= n; returns 0 for n<=0, -1 on overflow. */
static int erb_next_pow2(int n)
{
    if (n <= 0) { return 0; }
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    int result = n + 1;
    return (result < 0) ? -1 : result;
}

static inline void erb_write_int32(uint8_t *buf, int offset, int32_t val)
{
    memcpy(buf + offset, &val, 4);
}

static inline int32_t erb_read_int32(const uint8_t *buf, int offset)
{
    int32_t val;
    memcpy(&val, buf + offset, 4);
    return val;
}

/* Resize backing store to fit at least new_message_length additional bytes. */
static int erb_resize(aeron_expandable_ring_buffer_t *rb, int new_message_aligned_length)
{
    int current_used  = (int)(rb->tail - rb->head);
    int required      = current_used + new_message_aligned_length;
    int new_capacity  = erb_next_pow2(required);

    if (new_capacity < 0 || new_capacity > rb->max_capacity)
    {
        /* Cannot grow — caller will return false */
        return 0;
    }
    if (new_capacity <= rb->capacity)
    {
        return 0; /* nothing to do */
    }

    uint8_t *new_buf = NULL;
    if (aeron_alloc((void **)&new_buf, (size_t)new_capacity) < 0)
    {
        AERON_APPEND_ERR("%s", "aeron_expandable_ring_buffer resize");
        return -1;
    }
    memset(new_buf, 0, (size_t)new_capacity);

    /* Linearize existing content into new_buf starting at offset 0 */
    if (current_used > 0)
    {
        int head_offset = (int)rb->head & rb->mask;
        int first_len   = rb->capacity - head_offset;
        if (first_len > current_used) { first_len = current_used; }
        memcpy(new_buf, rb->buffer + head_offset, (size_t)first_len);
        if (first_len < current_used)
        {
            memcpy(new_buf + first_len, rb->buffer, (size_t)(current_used - first_len));
        }
    }

    aeron_free(rb->buffer);
    rb->buffer   = new_buf;
    rb->capacity = new_capacity;
    rb->mask     = new_capacity - 1;
    rb->head     = 0;
    rb->tail     = current_used;
    return 0;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int aeron_expandable_ring_buffer_init(
    aeron_expandable_ring_buffer_t *rb,
    int initial_capacity,
    int max_capacity)
{
    if (max_capacity <= 0 || max_capacity > AERON_ERB_MAX_CAPACITY)
    {
        AERON_SET_ERR(EINVAL, "max_capacity %d out of range", max_capacity);
        return -1;
    }
    if (max_capacity != (max_capacity & -max_capacity))
    {
        /* Not a power of 2 — round up */
        max_capacity = erb_next_pow2(max_capacity);
        if (max_capacity < 0 || max_capacity > AERON_ERB_MAX_CAPACITY)
        {
            AERON_SET_ERR(EINVAL, "max_capacity overflow after round-up", 0);
            return -1;
        }
    }

    rb->max_capacity = max_capacity;
    rb->head         = 0;
    rb->tail         = 0;
    rb->buffer       = NULL;
    rb->capacity     = 0;
    rb->mask         = 0;

    if (initial_capacity > 0)
    {
        int cap = erb_next_pow2(initial_capacity);
        if (cap < 0 || cap > max_capacity)
        {
            AERON_SET_ERR(EINVAL, "initial_capacity %d invalid", initial_capacity);
            return -1;
        }
        if (aeron_alloc((void **)&rb->buffer, (size_t)cap) < 0)
        {
            return -1;
        }
        memset(rb->buffer, 0, (size_t)cap);
        rb->capacity = cap;
        rb->mask     = cap - 1;
    }
    return 0;
}

int aeron_expandable_ring_buffer_reset(
    aeron_expandable_ring_buffer_t *rb,
    int required_capacity)
{
    if (required_capacity < 0)
    {
        AERON_SET_ERR(EINVAL, "required_capacity %d < 0", required_capacity);
        return -1;
    }

    int new_cap = (required_capacity == 0) ? 0 : erb_next_pow2(required_capacity);
    if (new_cap < 0)
    {
        AERON_SET_ERR(EINVAL, "required_capacity %d too large", required_capacity);
        return -1;
    }
    if (new_cap > rb->max_capacity)
    {
        AERON_SET_ERR(EINVAL, "required_capacity %d > max_capacity %d",
            required_capacity, rb->max_capacity);
        return -1;
    }

    if (new_cap != rb->capacity)
    {
        aeron_free(rb->buffer);
        rb->buffer   = NULL;
        rb->capacity = 0;
        rb->mask     = 0;

        if (new_cap > 0)
        {
            if (aeron_alloc((void **)&rb->buffer, (size_t)new_cap) < 0)
            {
                return -1;
            }
            memset(rb->buffer, 0, (size_t)new_cap);
            rb->capacity = new_cap;
            rb->mask     = new_cap - 1;
        }
    }

    rb->head = 0;
    rb->tail = 0;
    return 0;
}

void aeron_expandable_ring_buffer_close(aeron_expandable_ring_buffer_t *rb)
{
    if (NULL != rb && NULL != rb->buffer)
    {
        aeron_free(rb->buffer);
        rb->buffer = NULL;
    }
}

bool aeron_expandable_ring_buffer_append(
    aeron_expandable_ring_buffer_t *rb,
    const uint8_t *src,
    int            src_offset,
    int            src_length)
{
    int aligned = erb_align(AERON_ERB_HEADER_LENGTH + src_length, AERON_ERB_ALIGNMENT);
    int used    = (int)(rb->tail - rb->head);
    int avail   = rb->capacity - used;

    if (aligned > avail)
    {
        if (erb_resize(rb, aligned) < 0)
        {
            return false; /* could not resize */
        }
        used  = (int)(rb->tail - rb->head);
        avail = rb->capacity - used;
    }

    if (aligned > avail)
    {
        return false; /* still not enough room (at max_capacity) */
    }

    /* Check if message fits without wrapping to start */
    int tail_offset = (int)rb->tail & rb->mask;
    int head_offset_phys = (int)rb->head & rb->mask;

    /* If tail >= head (not wrapped) and message doesn't fit before end */
    if (tail_offset >= head_offset_phys || head_offset_phys == 0)
    {
        int to_end = rb->capacity - tail_offset;
        if (aligned > to_end)
        {
            /* Need to wrap: insert padding at end if there's enough room at start */
            int space_at_start = head_offset_phys;
            if (aligned <= space_at_start)
            {
                /* Write padding at end */
                erb_write_int32(rb->buffer, tail_offset, to_end);
                erb_write_int32(rb->buffer, tail_offset + 4, AERON_ERB_MSG_TYPE_PADDING);
                rb->tail += to_end;
            }
            else
            {
                /* Must resize */
                if (erb_resize(rb, aligned) < 0) { return false; }
            }
        }
    }

    /* Write the data record */
    int write_offset = (int)rb->tail & rb->mask;
    erb_write_int32(rb->buffer, write_offset,     AERON_ERB_HEADER_LENGTH + src_length);
    erb_write_int32(rb->buffer, write_offset + 4, AERON_ERB_MSG_TYPE_DATA);
    memcpy(rb->buffer + write_offset + AERON_ERB_HEADER_LENGTH,
        src + src_offset, (size_t)src_length);
    rb->tail += aligned;
    return true;
}

int aeron_expandable_ring_buffer_consume(
    aeron_expandable_ring_buffer_t             *rb,
    void                                       *clientd,
    aeron_expandable_ring_buffer_consumer_fn_t  consumer,
    int                                         message_limit)
{
    int64_t position = rb->head;
    int count = 0;

    while (count < message_limit && position < rb->tail)
    {
        int offset       = (int)position & rb->mask;
        int32_t length   = erb_read_int32(rb->buffer, offset);
        int32_t type_id  = erb_read_int32(rb->buffer, offset + 4);
        int aligned      = erb_align(length, AERON_ERB_ALIGNMENT);

        position += aligned;

        if (AERON_ERB_MSG_TYPE_PADDING != type_id)
        {
            int head_off = (int)(position - rb->head);
            int payload_len = length - AERON_ERB_HEADER_LENGTH;
            if (!consumer(clientd, rb->buffer, offset + AERON_ERB_HEADER_LENGTH, payload_len, head_off))
            {
                position -= aligned; /* un-advance; this message stays */
                break;
            }
            ++count;
        }
    }

    int bytes = (int)(position - rb->head);
    rb->head = position;
    return bytes;
}

int aeron_expandable_ring_buffer_for_each(
    aeron_expandable_ring_buffer_t             *rb,
    void                                       *clientd,
    aeron_expandable_ring_buffer_consumer_fn_t  consumer,
    int                                         limit)
{
    int64_t position = rb->head;
    int count = 0;

    while (count < limit && position < rb->tail)
    {
        int offset       = (int)position & rb->mask;
        int32_t length   = erb_read_int32(rb->buffer, offset);
        int32_t type_id  = erb_read_int32(rb->buffer, offset + 4);
        int aligned      = erb_align(length, AERON_ERB_ALIGNMENT);

        position += aligned;

        if (AERON_ERB_MSG_TYPE_PADDING != type_id)
        {
            int head_off = (int)(position - rb->head);
            int payload_len = length - AERON_ERB_HEADER_LENGTH;
            if (!consumer(clientd, rb->buffer, offset + AERON_ERB_HEADER_LENGTH, payload_len, head_off))
            {
                break;
            }
            ++count;
        }
    }

    return (int)(position - rb->head);
}

int aeron_expandable_ring_buffer_for_each_from(
    aeron_expandable_ring_buffer_t             *rb,
    int                                         head_offset,
    void                                       *clientd,
    aeron_expandable_ring_buffer_consumer_fn_t  consumer,
    int                                         limit)
{
    int64_t initial_position = rb->head + head_offset;
    int64_t position         = initial_position;
    int count = 0;

    while (count < limit && position < rb->tail)
    {
        int offset       = (int)position & rb->mask;
        int32_t length   = erb_read_int32(rb->buffer, offset);
        int32_t type_id  = erb_read_int32(rb->buffer, offset + 4);
        int aligned      = erb_align(length, AERON_ERB_ALIGNMENT);

        position += aligned;

        if (AERON_ERB_MSG_TYPE_PADDING != type_id)
        {
            int head_off = (int)(position - rb->head);
            int payload_len = length - AERON_ERB_HEADER_LENGTH;
            if (!consumer(clientd, rb->buffer, offset + AERON_ERB_HEADER_LENGTH, payload_len, head_off))
            {
                break;
            }
            ++count;
        }
    }

    return (int)(position - initial_position);
}
