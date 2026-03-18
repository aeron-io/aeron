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
#ifndef AERON_SPSC_CONCURRENT_ARRAY_QUEUE_ELEM_H
#define AERON_SPSC_CONCURRENT_ARRAY_QUEUE_ELEM_H

#include <string.h>

#include "util/aeron_bitutil.h"
#include "aeron_atomic.h"
#include "aeron_concurrent_array_queue.h"

typedef struct aeron_spsc_concurrent_array_queue_elem_stct
{

    int8_t padding1[AERON_CACHE_LINE_LENGTH];

    struct
    {
        volatile uint64_t tail;
        uint64_t head_cache;
    } producer;

    int8_t padding2[AERON_CACHE_LINE_LENGTH];

    struct
    {
        volatile uint64_t head;
    } consumer;

    int8_t padding3[AERON_CACHE_LINE_LENGTH];

    size_t capacity;
    size_t mask;
    size_t elem_size;
    uint8_t *buffer;
} aeron_spsc_concurrent_array_queue_elem_t;

int aeron_spsc_concurrent_array_queue_elem_init(aeron_spsc_concurrent_array_queue_elem_t *queue, size_t length, size_t elem_size);

int aeron_spsc_concurrent_array_queue_elem_close(aeron_spsc_concurrent_array_queue_elem_t *queue);

inline aeron_queue_offer_result_t aeron_spsc_concurrent_array_queue_elem_offer(
    aeron_spsc_concurrent_array_queue_elem_t *queue, void *element)
{
    if (NULL == element)
    {
        return AERON_OFFER_ERROR;
    }

    uint64_t current_head = queue->producer.head_cache;
    uint64_t buffer_limit = current_head + queue->capacity;
    uint64_t current_tail = queue->producer.tail;

    if (current_tail >= buffer_limit)
    {
        AERON_GET_ACQUIRE(current_head, queue->consumer.head);
        buffer_limit = current_head + queue->capacity;

        if (current_tail >= buffer_limit)
        {
            return AERON_OFFER_FULL;
        }

        queue->producer.head_cache = current_head;
    }

    const size_t index = (size_t)(current_tail & queue->mask);

    memcpy((void *)(queue->buffer + index * queue->elem_size), element, queue->elem_size);
    AERON_SET_RELEASE(queue->producer.tail, current_tail + 1);

    return AERON_OFFER_SUCCESS;
}

inline bool aeron_spsc_concurrent_array_queue_elem_poll(aeron_spsc_concurrent_array_queue_elem_t *queue, void *dest)
{
    const uint64_t current_head = queue->consumer.head;

    uint64_t current_tail;
    AERON_GET_ACQUIRE(current_tail, queue->producer.tail);

    if (current_tail <= current_head)
    {
        return false;
    }

    const size_t index = (size_t)(current_head & queue->mask);

    memcpy(dest, (void *)(queue->buffer + index * queue->elem_size), queue->elem_size);
    AERON_SET_RELEASE(queue->consumer.head, current_head + 1);

    return true;
}

inline size_t aeron_spsc_concurrent_array_queue_elem_drain(
    aeron_spsc_concurrent_array_queue_elem_t *queue, aeron_queue_drain_func_t func, void *clientd, size_t limit)
{
    const uint64_t current_head = queue->consumer.head;
    uint64_t current_tail;
    AERON_GET_ACQUIRE(current_tail, queue->producer.tail);

    const uint64_t limit_sequence = current_tail - current_head > limit ? current_head + limit : current_tail;

    for (uint64_t next_sequence = current_head; next_sequence < limit_sequence;)
    {
        const size_t index = (size_t)(next_sequence & queue->mask);

        func(clientd, (void *)(queue->buffer + index * queue->elem_size));

        next_sequence++;
        AERON_SET_RELEASE(queue->consumer.head, next_sequence);
    }

    return limit_sequence - current_head;
}

inline size_t aeron_spsc_concurrent_array_queue_elem_drain_all(
    aeron_spsc_concurrent_array_queue_elem_t *queue, aeron_queue_drain_func_t func, void *clientd)
{
    return aeron_spsc_concurrent_array_queue_elem_drain(queue, func, clientd, SIZE_MAX);
}

inline size_t aeron_spsc_concurrent_array_queue_elem_size(aeron_spsc_concurrent_array_queue_elem_t *queue)
{
    uint64_t current_head_before;
    uint64_t current_tail;
    uint64_t current_head_after;

    AERON_GET_ACQUIRE(current_head_after, queue->consumer.head);

    do
    {
        current_head_before = current_head_after;
        AERON_GET_ACQUIRE(current_tail, queue->producer.tail);
        AERON_GET_ACQUIRE(current_head_after, queue->consumer.head);
    } while (current_head_after != current_head_before);

    size_t size = (size_t)(current_tail - current_head_after);
    if ((int64_t)size < 0)
    {
        return 0;
    }
    else if (size > queue->capacity)
    {
        return queue->capacity;
    }

    return size;
}

#endif // AERON_SPSC_CONCURRENT_ARRAY_QUEUE_ELEM_H
