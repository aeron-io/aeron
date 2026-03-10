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
#ifndef AERON_LINKED_QUEUE_H
#define AERON_LINKED_QUEUE_H
#include <stdbool.h>

typedef struct aeron_linked_queue_node_stct aeron_linked_queue_node_t;

typedef struct aeron_linked_queue_stct
{
    aeron_linked_queue_node_t *head;
    aeron_linked_queue_node_t *tail;
}
aeron_linked_queue_t;


int aeron_linked_queue_init(aeron_linked_queue_t *queue);

int aeron_linked_queue_close(aeron_linked_queue_t *queue);

int aeron_linked_queue_offer(aeron_linked_queue_t *queue, void *element);

void *aeron_linked_queue_peek(aeron_linked_queue_t *queue);

void *aeron_linked_queue_poll(aeron_linked_queue_t *queue);

bool aeron_linked_queue_is_empty(aeron_linked_queue_t *queue);

#endif //AERON_LINKED_QUEUE_H
