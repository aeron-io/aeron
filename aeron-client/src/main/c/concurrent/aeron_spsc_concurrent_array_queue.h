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
#ifndef AERON_SPSC_CONCURRENT_ARRAY_QUEUE_H
#define AERON_SPSC_CONCURRENT_ARRAY_QUEUE_H

#include "util/aeron_bitutil.h"
#include "aeron_atomic.h"
#include "aeron_concurrent_array_queue.h"

typedef struct aeron_spsc_concurrent_array_queue_stct
{
    int8_t padding[AERON_CACHE_LINE_LENGTH - sizeof(uint64_t)];
    struct
    {
        volatile uint64_t tail;
        uint64_t head_cache;
        int8_t padding[AERON_CACHE_LINE_LENGTH - (2 * sizeof(uint64_t))];
    }
    producer;

    struct
    {
        volatile uint64_t head;
        int8_t padding[AERON_CACHE_LINE_LENGTH - sizeof(uint64_t)];
    }
    consumer;

    size_t capacity;
    size_t mask;
    void * volatile *buffer;
}
aeron_spsc_concurrent_array_queue_t;

int aeron_spsc_concurrent_array_queue_init(aeron_spsc_concurrent_array_queue_t *queue, size_t length);

int aeron_spsc_concurrent_array_queue_close(aeron_spsc_concurrent_array_queue_t *queue);

inline aeron_queue_offer_result_t aeron_spsc_concurrent_array_queue_offer(
    aeron_spsc_concurrent_array_queue_t *queue, void *element)
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

    AERON_SET_RELEASE(queue->buffer[index], element);
    AERON_SET_RELEASE(queue->producer.tail, current_tail + 1);

    return AERON_OFFER_SUCCESS;
}

inline void *aeron_spsc_concurrent_array_queue_poll(aeron_spsc_concurrent_array_queue_t *queue)
{
    const uint64_t current_head = queue->consumer.head;
    const size_t index = (size_t)(current_head & queue->mask);

    volatile void *item;
    AERON_GET_ACQUIRE(item, queue->buffer[index]);

    if (NULL != item)
    {
        AERON_SET_RELEASE(queue->buffer[index], NULL);
        AERON_SET_RELEASE(queue->consumer.head, current_head + 1);
    }

    return (void *)item;
}

inline size_t aeron_spsc_concurrent_array_queue_drain(
    aeron_spsc_concurrent_array_queue_t *queue, aeron_queue_drain_func_t func, void *clientd, size_t limit)
{
    uint64_t current_head = queue->consumer.head;
    uint64_t next_sequence = current_head;
    const uint64_t limit_sequence = next_sequence + limit;

    while (next_sequence < limit_sequence)
    {
        const size_t index = (size_t)(next_sequence & queue->mask);
        volatile void *item;
        AERON_GET_ACQUIRE(item, queue->buffer[index]);

        if (NULL == item)
        {
            break;
        }

        AERON_SET_RELEASE(queue->buffer[index], NULL);
        next_sequence++;
        AERON_SET_RELEASE(queue->consumer.head, next_sequence);
        func(clientd, (void *)item);
    }

    return next_sequence - current_head;
}

inline size_t aeron_spsc_concurrent_array_queue_drain_all(
    aeron_spsc_concurrent_array_queue_t *queue, aeron_queue_drain_func_t func, void *clientd)
{
    uint64_t current_head = queue->consumer.head;
    uint64_t current_tail;
    AERON_GET_ACQUIRE(current_tail, queue->producer.tail);

    return aeron_spsc_concurrent_array_queue_drain(queue, func, clientd, current_tail - current_head);
}

inline size_t aeron_spsc_concurrent_array_queue_size(aeron_spsc_concurrent_array_queue_t *queue)
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
    }
    while (current_head_after != current_head_before);

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

#endif //AERON_SPSC_CONCURRENT_ARRAY_QUEUE_H
