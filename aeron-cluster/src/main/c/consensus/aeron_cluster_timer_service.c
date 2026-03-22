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

#include <string.h>
#include <errno.h>
#include <limits.h>

#include "aeron_cluster_timer_service.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

/* -----------------------------------------------------------------------
 * Min-heap node
 * ----------------------------------------------------------------------- */
typedef struct
{
    int64_t deadline_ns;
    int64_t correlation_id;
}
timer_node_t;

struct aeron_cluster_timer_service_stct
{
    timer_node_t                    *heap;
    int                              heap_size;
    int                              heap_capacity;
    aeron_cluster_timer_expiry_func_t on_expiry;
    void                            *clientd;
};

#define INITIAL_CAPACITY 64

static int heap_ensure_capacity(aeron_cluster_timer_service_t *svc)
{
    if (svc->heap_size < svc->heap_capacity) { return 0; }

    int new_cap = svc->heap_capacity * 2;
    if (aeron_reallocf((void **)&svc->heap,
        (size_t)new_cap * sizeof(timer_node_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to grow timer heap");
        return -1;
    }
    svc->heap_capacity = new_cap;
    return 0;
}

static void heap_sift_up(timer_node_t *heap, int i)
{
    while (i > 0)
    {
        int parent = (i - 1) / 2;
        if (heap[parent].deadline_ns <= heap[i].deadline_ns) { break; }
        timer_node_t tmp = heap[parent];
        heap[parent] = heap[i];
        heap[i] = tmp;
        i = parent;
    }
}

static void heap_sift_down(timer_node_t *heap, int size, int i)
{
    while (1)
    {
        int smallest = i;
        int left  = 2 * i + 1;
        int right = 2 * i + 2;
        if (left  < size && heap[left].deadline_ns  < heap[smallest].deadline_ns) { smallest = left;  }
        if (right < size && heap[right].deadline_ns < heap[smallest].deadline_ns) { smallest = right; }
        if (smallest == i) { break; }
        timer_node_t tmp = heap[i];
        heap[i] = heap[smallest];
        heap[smallest] = tmp;
        i = smallest;
    }
}

int aeron_cluster_timer_service_create(
    aeron_cluster_timer_service_t **service,
    aeron_cluster_timer_expiry_func_t on_expiry,
    void *clientd)
{
    aeron_cluster_timer_service_t *svc = NULL;
    if (aeron_alloc((void **)&svc, sizeof(aeron_cluster_timer_service_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate timer service");
        return -1;
    }

    if (aeron_alloc((void **)&svc->heap,
        INITIAL_CAPACITY * sizeof(timer_node_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate timer heap");
        aeron_free(svc);
        return -1;
    }

    svc->heap_size     = 0;
    svc->heap_capacity = INITIAL_CAPACITY;
    svc->on_expiry     = on_expiry;
    svc->clientd       = clientd;

    *service = svc;
    return 0;
}

int aeron_cluster_timer_service_close(aeron_cluster_timer_service_t *service)
{
    if (NULL != service)
    {
        aeron_free(service->heap);
        aeron_free(service);
    }
    return 0;
}

int aeron_cluster_timer_service_schedule(
    aeron_cluster_timer_service_t *service,
    int64_t correlation_id,
    int64_t deadline_ns)
{
    /* Update existing timer if found */
    for (int i = 0; i < service->heap_size; i++)
    {
        if (service->heap[i].correlation_id == correlation_id)
        {
            service->heap[i].deadline_ns = deadline_ns;
            heap_sift_up(service->heap, i);
            heap_sift_down(service->heap, service->heap_size, i);
            return 0;
        }
    }

    /* Insert new */
    if (heap_ensure_capacity(service) < 0) { return -1; }

    service->heap[service->heap_size] = (timer_node_t){ deadline_ns, correlation_id };
    heap_sift_up(service->heap, service->heap_size);
    service->heap_size++;
    return 0;
}

bool aeron_cluster_timer_service_cancel(
    aeron_cluster_timer_service_t *service,
    int64_t correlation_id)
{
    for (int i = 0; i < service->heap_size; i++)
    {
        if (service->heap[i].correlation_id == correlation_id)
        {
            service->heap_size--;
            service->heap[i] = service->heap[service->heap_size];
            heap_sift_down(service->heap, service->heap_size, i);
            return true;
        }
    }
    return false;
}

int aeron_cluster_timer_service_poll(
    aeron_cluster_timer_service_t *service,
    int64_t now_ns)
{
    int fired = 0;
    while (service->heap_size > 0 &&
           service->heap[0].deadline_ns <= now_ns)
    {
        int64_t correlation_id = service->heap[0].correlation_id;

        /* Remove top */
        service->heap_size--;
        service->heap[0] = service->heap[service->heap_size];
        heap_sift_down(service->heap, service->heap_size, 0);

        service->on_expiry(service->clientd, correlation_id);
        fired++;
    }
    return fired;
}

int64_t aeron_cluster_timer_service_next_deadline(aeron_cluster_timer_service_t *service)
{
    return (service->heap_size > 0) ? service->heap[0].deadline_ns : INT64_MAX;
}

int aeron_cluster_timer_service_timer_count(aeron_cluster_timer_service_t *service)
{
    return service->heap_size;
}
