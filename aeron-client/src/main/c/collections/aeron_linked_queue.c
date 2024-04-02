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

#include <errno.h>
#include "aeron_alloc.h"
#include "collections/aeron_linked_queue.h"
#include "util/aeron_error.h"

struct aeron_linked_queue_node_stct
{
    aeron_linked_queue_node_t *next;
    void *element;
};

#define IS_EMPTY(_q) (NULL == (_q)->head && NULL == (_q)->tail)

int aeron_linked_queue_init(aeron_linked_queue_t *queue)
{
    queue->head = NULL;
    queue->tail = NULL;

    return 0;
}

int aeron_linked_queue_close(aeron_linked_queue_t *queue)
{
    if (IS_EMPTY(queue))
    {
        return 0;
    }

    // the queue is NOT empty
    AERON_SET_ERR(EINVAL, "%s", "queue must be empty to be deleted");
    return -1;
}

int aeron_linked_queue_offer(aeron_linked_queue_t *queue, void *element)
{
    return aeron_linked_queue_offer_ex(queue, element, NULL);
}

int aeron_linked_queue_offer_ex(aeron_linked_queue_t *queue, void *element, aeron_linked_queue_node_t *in_node)
{
    aeron_linked_queue_node_t *node = in_node;

    if (NULL == node)
    {
        if (aeron_alloc((void **)&node, sizeof(aeron_linked_queue_node_t)) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
    }
    node->element = element;
    node->next = NULL;

    if (IS_EMPTY(queue))
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
    return aeron_linked_queue_poll_ex(queue, NULL);
}

void *aeron_linked_queue_poll_ex(aeron_linked_queue_t *queue, aeron_linked_queue_node_t **out_nodep)
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

    if (NULL == out_nodep)
    {
        aeron_linked_queue_node_delete(next);
    }
    else
    {
        next->next = NULL;
        next->element = NULL;

        *out_nodep = next;
    }

    return element;
}

int aeron_linked_queue_node_delete(aeron_linked_queue_node_t *node)
{
    if (NULL != node)
    {
        aeron_free(node);
    }

    return 0;
}
