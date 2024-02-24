/*
 * Copyright 2014-2024 Real Logic Limited.
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
#ifndef AERON_BLOCKING_LINKED_QUEUE_H
#define AERON_BLOCKING_LINKED_QUEUE_H

#include "aeron_spsc_concurrent_linked_queue.h"
#include "aeron_thread.h"

typedef struct aeron_blocking_linked_queue_stct
{
    aeron_spsc_concurrent_linked_queue_t spsc_queue;
    size_t size;
    aeron_mutex_t mutex;
    aeron_cond_t cv;
}
aeron_blocking_linked_queue_t;

int aeron_blocking_linked_queue_init(aeron_blocking_linked_queue_t *queue);

int aeron_blocking_linked_queue_close(aeron_blocking_linked_queue_t *queue);

int aeron_blocking_linked_queue_offer(aeron_blocking_linked_queue_t *queue, void *element);

void *aeron_blocking_linked_queue_poll(aeron_blocking_linked_queue_t *queue);

size_t aeron_blocking_linked_queue_size(aeron_blocking_linked_queue_t *queue);

#endif //AERON_BLOCKING_LINKED_QUEUE_H
