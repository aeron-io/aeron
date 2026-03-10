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

static void *aeron_blocking_linked_queue_retrieve(aeron_blocking_linked_queue_t *queue, bool block)
{
    void *element = NULL;

    aeron_mutex_lock(&queue->mutex);

    element = aeron_linked_queue_poll(&queue->queue);

    if (NULL == element && block)
    {
        aeron_cond_wait(&queue->cv, &queue->mutex);

        element = aeron_linked_queue_poll(&queue->queue);
    }

    aeron_mutex_unlock(&queue->mutex);

    return element;
}

int aeron_blocking_linked_queue_init(aeron_blocking_linked_queue_t *queue)
{
    if (aeron_linked_queue_init(&queue->queue) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    aeron_mutex_init(&queue->mutex);
    aeron_cond_init(&queue->cv, NULL);

    return 0;
}

int aeron_blocking_linked_queue_close(aeron_blocking_linked_queue_t *queue)
{
    aeron_mutex_lock(&queue->mutex);

    if (aeron_linked_queue_close(&queue->queue) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    aeron_cond_destroy(&queue->cv);
    aeron_mutex_unlock(&queue->mutex);
    aeron_mutex_destroy(&queue->mutex);

    return 0;
error:
    aeron_mutex_unlock(&queue->mutex);
    return -1;
}

int aeron_blocking_linked_queue_offer(aeron_blocking_linked_queue_t *queue, void *element)
{
    int rc;

    aeron_mutex_lock(&queue->mutex);

    rc = aeron_linked_queue_offer(&queue->queue, element);

    if (rc == 0)
    {
        aeron_cond_signal(&queue->cv);
    }

    aeron_mutex_unlock(&queue->mutex);

    return rc;
}

void *aeron_blocking_linked_queue_poll(aeron_blocking_linked_queue_t *queue)
{
    return aeron_blocking_linked_queue_retrieve(queue, false);
}

void *aeron_blocking_linked_queue_take(aeron_blocking_linked_queue_t *queue)
{
    return aeron_blocking_linked_queue_retrieve(queue, true);
}

bool aeron_blocking_linked_queue_is_empty(aeron_blocking_linked_queue_t *queue)
{
    bool is_empty;

    aeron_mutex_lock(&queue->mutex);

    is_empty = aeron_linked_queue_is_empty(&queue->queue);

    aeron_mutex_unlock(&queue->mutex);

    return is_empty;
}

void aeron_blocking_linked_queue_unblock(aeron_blocking_linked_queue_t *queue)
{
    aeron_cond_signal(&queue->cv);
}
