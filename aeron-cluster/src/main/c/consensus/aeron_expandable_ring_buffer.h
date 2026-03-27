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

#ifndef AERON_EXPANDABLE_RING_BUFFER_H
#define AERON_EXPANDABLE_RING_BUFFER_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C"
{
#endif

/*
 * C port of org.agrona.ExpandableRingBuffer.
 *
 * Ring buffer for variable-length messages that expands up to max_capacity.
 * Messages are stored in FIFO order. The layout of each record is:
 *
 *   [int32 length][int32 type][data...]
 *
 * where `length` is the total record size including the 8-byte header, and
 * `type` is 0 for padding or 1 for data. Records are aligned to 8 bytes.
 * Not thread-safe.
 */

#define AERON_ERB_HEADER_LENGTH    8   /* 4 bytes length + 4 bytes type */
#define AERON_ERB_ALIGNMENT        8   /* all records aligned to 8 bytes */
#define AERON_ERB_MSG_TYPE_PADDING 0
#define AERON_ERB_MSG_TYPE_DATA    1
#define AERON_ERB_MAX_CAPACITY     (1 << 30)  /* 1 GB */

typedef struct aeron_expandable_ring_buffer_stct
{
    uint8_t *buffer;
    int      capacity;      /* current allocated capacity (power of 2) */
    int      mask;          /* capacity - 1 */
    int      max_capacity;  /* upper bound */
    int64_t  head;          /* monotonically increasing logical read position */
    int64_t  tail;          /* monotonically increasing logical write position */
}
aeron_expandable_ring_buffer_t;

/**
 * Callback invoked per message by forEach / consume.
 *
 * @param clientd    opaque pointer passed by the caller
 * @param buffer     pointer to the ring buffer's backing store
 * @param offset     byte offset within buffer at which the message payload begins
 *                   (i.e. past the 8-byte record header)
 * @param length     length of the payload in bytes
 * @param head_offset cumulative bytes from the head of the ring buffer to the
 *                   END of this record (mirrors Java headOffset)
 * @return true to continue iteration; false to stop
 */
typedef bool (*aeron_expandable_ring_buffer_consumer_fn_t)(
    void    *clientd,
    uint8_t *buffer,
    int      offset,
    int      length,
    int      head_offset);

/**
 * Initialise (but not allocate) a ring buffer.
 * Call aeron_expandable_ring_buffer_close() when done.
 *
 * @param initial_capacity 0 for lazy allocation, or positive power-of-2.
 * @param max_capacity     must be a power-of-2 and <= AERON_ERB_MAX_CAPACITY.
 */
int aeron_expandable_ring_buffer_init(
    aeron_expandable_ring_buffer_t *rb,
    int initial_capacity,
    int max_capacity);

/**
 * Reset the ring buffer with a new capacity (resizes backing store if needed).
 * All pending messages are discarded.
 */
int aeron_expandable_ring_buffer_reset(
    aeron_expandable_ring_buffer_t *rb,
    int required_capacity);

/** Release backing memory. */
void aeron_expandable_ring_buffer_close(aeron_expandable_ring_buffer_t *rb);

/**
 * Append a message. The ring buffer will grow if there is not enough room,
 * up to max_capacity. Returns true on success, false if max_capacity reached.
 */
bool aeron_expandable_ring_buffer_append(
    aeron_expandable_ring_buffer_t *rb,
    const uint8_t *src,
    int            src_offset,
    int            src_length);

/**
 * Consume up to message_limit messages, advancing head past consumed ones.
 * The consumer returns false to stop early (message is left unconsumed).
 * @return number of bytes consumed (advanced past).
 */
int aeron_expandable_ring_buffer_consume(
    aeron_expandable_ring_buffer_t                  *rb,
    void                                            *clientd,
    aeron_expandable_ring_buffer_consumer_fn_t       consumer,
    int                                              message_limit);

/**
 * Iterate without consuming (head is not advanced).
 * Mirrors Java ExpandableRingBuffer.forEach(consumer, limit).
 * @return bytes iterated.
 */
int aeron_expandable_ring_buffer_for_each(
    aeron_expandable_ring_buffer_t                  *rb,
    void                                            *clientd,
    aeron_expandable_ring_buffer_consumer_fn_t       consumer,
    int                                              limit);

/**
 * Iterate starting at head_offset bytes from head, without consuming.
 * Mirrors Java ExpandableRingBuffer.forEach(headOffset, consumer, limit).
 * @return bytes iterated from head_offset.
 */
int aeron_expandable_ring_buffer_for_each_from(
    aeron_expandable_ring_buffer_t                  *rb,
    int                                              head_offset,
    void                                            *clientd,
    aeron_expandable_ring_buffer_consumer_fn_t       consumer,
    int                                              limit);

static inline int aeron_expandable_ring_buffer_size(const aeron_expandable_ring_buffer_t *rb)
{
    return (int)(rb->tail - rb->head);
}

static inline bool aeron_expandable_ring_buffer_is_empty(const aeron_expandable_ring_buffer_t *rb)
{
    return rb->head == rb->tail;
}

#ifdef __cplusplus
}
#endif

#endif /* AERON_EXPANDABLE_RING_BUFFER_H */
