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

#ifndef AERON_CLUSTER_TIMER_SERVICE_H
#define AERON_CLUSTER_TIMER_SERVICE_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Callback fired when a timer expires.
 */
typedef void (*aeron_cluster_timer_expiry_func_t)(void *clientd, int64_t correlation_id);

/**
 * Priority-heap timer service.
 * Timers fire when `cluster_time >= deadline`.
 */
typedef struct aeron_cluster_timer_service_stct aeron_cluster_timer_service_t;

int  aeron_cluster_timer_service_create(
    aeron_cluster_timer_service_t **service,
    aeron_cluster_timer_expiry_func_t on_expiry,
    void *clientd);

int  aeron_cluster_timer_service_close(aeron_cluster_timer_service_t *service);

/**
 * Schedule a timer.  If correlationId already exists, the deadline is updated.
 * Returns 0 on success, -1 on error.
 */
int  aeron_cluster_timer_service_schedule(
    aeron_cluster_timer_service_t *service,
    int64_t correlation_id,
    int64_t deadline_ns);

/**
 * Cancel a timer by correlationId.
 * Returns true if found and removed.
 */
bool aeron_cluster_timer_service_cancel(
    aeron_cluster_timer_service_t *service,
    int64_t correlation_id);

/**
 * Poll: fire all timers whose deadline <= now_ns.
 * Returns the number of timers fired.
 */
int  aeron_cluster_timer_service_poll(
    aeron_cluster_timer_service_t *service,
    int64_t now_ns);

/**
 * Return the deadline of the next pending timer, or INT64_MAX if none.
 */
int64_t aeron_cluster_timer_service_next_deadline(
    aeron_cluster_timer_service_t *service);

int aeron_cluster_timer_service_timer_count(aeron_cluster_timer_service_t *service);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_TIMER_SERVICE_H */
