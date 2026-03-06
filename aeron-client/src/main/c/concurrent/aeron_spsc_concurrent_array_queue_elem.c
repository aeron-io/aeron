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

#include "aeron_alloc.h"
#include "concurrent/aeron_spsc_concurrent_array_queue_elem.h"

int aeron_spsc_concurrent_array_queue_elem_init(aeron_spsc_concurrent_array_queue_elem_t *queue, size_t length, size_t elem_size)
{
    length = (size_t)aeron_find_next_power_of_two((int32_t)length);

    if (aeron_alloc((void **)&queue->buffer, length * elem_size) < 0)
    {
        return -1;
    }

    memset((void *)queue->buffer, 0, length * elem_size);

    queue->capacity = length;
    queue->mask = length - 1;
    queue->elem_size = elem_size;
    queue->producer.head_cache = 0;
    AERON_SET_RELEASE(queue->producer.tail, (uint64_t)0);
    AERON_SET_RELEASE(queue->consumer.head, (uint64_t)0);

    return 0;
}

int aeron_spsc_concurrent_array_queue_elem_close(aeron_spsc_concurrent_array_queue_elem_t *queue)
{
    aeron_free((void *)queue->buffer);
    return 0;
}

extern aeron_queue_offer_result_t aeron_spsc_concurrent_array_queue_elem_offer(
    aeron_spsc_concurrent_array_queue_elem_t *queue, void *element);

extern bool aeron_spsc_concurrent_array_queue_elem_poll(aeron_spsc_concurrent_array_queue_elem_t *queue, void *dest);

extern size_t aeron_spsc_concurrent_array_queue_elem_drain(
    aeron_spsc_concurrent_array_queue_elem_t *queue, aeron_queue_drain_func_t func, void *clientd, size_t limit);

extern size_t aeron_spsc_concurrent_array_queue_elem_drain_all(
    aeron_spsc_concurrent_array_queue_elem_t *queue, aeron_queue_drain_func_t func, void *clientd);

extern size_t aeron_spsc_concurrent_array_queue_elem_size(aeron_spsc_concurrent_array_queue_elem_t *queue);
