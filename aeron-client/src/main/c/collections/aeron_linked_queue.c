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
#include "aeron_alloc.h"
#include "collections/aeron_linked_queue.h"
#include "util/aeron_error.h"

struct aeron_linked_queue_node_stct
{
    aeron_linked_queue_node_t *next;
    void *element;
};

int aeron_linked_queue_init(aeron_linked_queue_t *queue)
{
    queue->head = NULL;
    queue->tail = NULL;

    return 0;
}

int aeron_linked_queue_close(aeron_linked_queue_t *queue)
{
    if (aeron_linked_queue_is_empty(queue))
    {
        return 0;
    }

    // the queue is NOT empty
    AERON_SET_ERR(EINVAL, "%s", "queue must be empty to be deleted");
    return -1;
}

int aeron_linked_queue_offer(aeron_linked_queue_t *queue, void *element)
{
    aeron_linked_queue_node_t *node = NULL;
    if (aeron_alloc((void **)&node, sizeof(aeron_linked_queue_node_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }
    node->element = element;
    node->next = NULL;

    if (aeron_linked_queue_is_empty(queue))
    {
        queue->head = node;
        queue->tail = node;
    }
    else
    {
        queue->head->next = node;
        queue->head = node;
    }

    return 0;
}

void *aeron_linked_queue_peek(aeron_linked_queue_t *queue)
{
    return queue->tail == NULL ? NULL : queue->tail->element;
}

void *aeron_linked_queue_poll(aeron_linked_queue_t *queue)
{
    aeron_linked_queue_node_t *next;

    next = queue->tail;

    if (NULL == next)
    {
        return NULL;
    }

    void *element = next->element;

    queue->tail = next->next;

    if (queue->tail == NULL)
    {
        queue->head = NULL;
    }

    aeron_free(next);

    return element;
}

bool aeron_linked_queue_is_empty(aeron_linked_queue_t *queue)
{
    return NULL == queue->head && NULL == queue->tail;
}
