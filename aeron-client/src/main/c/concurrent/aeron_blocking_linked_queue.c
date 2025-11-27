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
#include "concurrent/aeron_blocking_linked_queue.h"
#include "util/aeron_error.h"

#define QUEUE_IS_EMPTY(_q) (NULL == aeron_linked_queue_peek(&(_q)->queue))

int aeron_blocking_linked_queue_init(aeron_blocking_linked_queue_t *queue)
{
    if (aeron_linked_queue_init(&queue->queue) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    aeron_mutex_init(&queue->mutex, NULL);
    aeron_cond_init(&queue->cv, NULL);

    return 0;
}

int aeron_blocking_linked_queue_close(aeron_blocking_linked_queue_t *queue)
{
    aeron_mutex_lock(&queue->mutex);

    if (!QUEUE_IS_EMPTY(queue))
    {
        AERON_SET_ERR(EINVAL, "%s", "queue must be empty to be deleted");
        goto error;
    }

    if (aeron_linked_queue_close(&queue->queue) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    aeron_mutex_unlock(&queue->mutex);
    aeron_mutex_destroy(&queue->mutex);
    aeron_cond_destroy(&queue->cv);

    return 0;
error:
    aeron_mutex_unlock(&queue->mutex);
    return -1;
}

int aeron_blocking_linked_queue_offer(aeron_blocking_linked_queue_t *queue, void *element)
{
    return aeron_blocking_linked_queue_offer_ex(queue, element, NULL);
}

int aeron_blocking_linked_queue_offer_ex(aeron_blocking_linked_queue_t *queue, void *element, aeron_linked_queue_node_t *node)
{
    int rc;

    aeron_mutex_lock(&queue->mutex);

    rc = aeron_linked_queue_offer_ex(&queue->queue, element, node);

    if (rc == 0)
    {
        aeron_cond_signal(&queue->cv);
    }

    aeron_mutex_unlock(&queue->mutex);

    return rc;
}

void *aeron_blocking_linked_queue_retrieve(aeron_blocking_linked_queue_t *queue, bool block, aeron_linked_queue_node_t **out_nodep)
{
    void *element = NULL;

    aeron_mutex_lock(&queue->mutex);

    element = aeron_linked_queue_poll_ex(&queue->queue, out_nodep);

    if (block && NULL == element)
    {
        aeron_cond_wait(&queue->cv, &queue->mutex);

        element = aeron_linked_queue_poll_ex(&queue->queue, out_nodep);
    }

    aeron_mutex_unlock(&queue->mutex);

    return element;
}

void *aeron_blocking_linked_queue_poll(aeron_blocking_linked_queue_t *queue)
{
    return aeron_blocking_linked_queue_retrieve(queue, false, NULL);
}

void *aeron_blocking_linked_queue_poll_ex(aeron_blocking_linked_queue_t *queue, aeron_linked_queue_node_t **out_nodep)
{
    return aeron_blocking_linked_queue_retrieve(queue, false, out_nodep);
}

void *aeron_blocking_linked_queue_take(aeron_blocking_linked_queue_t *queue)
{
    return aeron_blocking_linked_queue_retrieve(queue, true, NULL);
}

void *aeron_blocking_linked_queue_take_ex(aeron_blocking_linked_queue_t *queue, aeron_linked_queue_node_t **out_nodep)
{
    return aeron_blocking_linked_queue_retrieve(queue, true, out_nodep);
}

bool aeron_blocking_linked_queue_is_empty(aeron_blocking_linked_queue_t *queue)
{
    bool is_empty;

    aeron_mutex_lock(&queue->mutex);

    is_empty = QUEUE_IS_EMPTY(queue);

    aeron_mutex_unlock(&queue->mutex);

    return is_empty;
}

void aeron_blocking_linked_queue_unblock(aeron_blocking_linked_queue_t *queue)
{
    aeron_cond_signal(&queue->cv);
}
