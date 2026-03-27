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

#ifndef AERON_CLUSTER_SERVICE_CONTAINER_H
#define AERON_CLUSTER_SERVICE_CONTAINER_H

#include <stdint.h>
#include <stdbool.h>

#include "aeron_cluster_service_context.h"
#include "aeron_clustered_service_agent.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Container lifecycle wrapper around aeron_clustered_service_agent_t.
 *
 * Mirrors Java ClusteredServiceContainer: holds the context and agent,
 * provides conclude/start/do_work/close. The caller drives the duty cycle
 * by calling aeron_cluster_service_container_do_work() in a loop.
 */
typedef struct aeron_cluster_service_container_stct
{
    aeron_cluster_service_context_t      *ctx;
    aeron_clustered_service_agent_t      *agent;
    bool                                  is_running;
}
aeron_cluster_service_container_t;

/**
 * Validate the context and fill in defaults (control/service channels,
 * stream IDs, cluster dir from env vars, etc.).
 *
 * Must be called after all user-set options and before create().
 *
 * @return 0 on success, -1 on error.
 */
int aeron_cluster_service_container_conclude(aeron_cluster_service_context_t *ctx);

/**
 * Create the container and its agent.  conclude() must have been called first.
 *
 * @param container  out: newly created container.
 * @param ctx        concluded context.
 * @return 0 on success, -1 on error.
 */
int aeron_cluster_service_container_create(
    aeron_cluster_service_container_t **container,
    aeron_cluster_service_context_t *ctx);

/**
 * Start the container (calls agent on_start).
 * Must be called once after create() before do_work().
 *
 * @return 0 on success, -1 on error.
 */
int aeron_cluster_service_container_start(aeron_cluster_service_container_t *container);

/**
 * Execute one duty-cycle iteration.
 *
 * @param container
 * @param now_ns  current time in nanoseconds.
 * @return work count (> 0 if busy, 0 if idle).
 */
int aeron_cluster_service_container_do_work(
    aeron_cluster_service_container_t *container,
    int64_t now_ns);

/** Returns true while the container has not been terminated. */
bool aeron_cluster_service_container_is_running(
    const aeron_cluster_service_container_t *container);

/**
 * Close the container and agent.  The context is NOT closed — caller owns it.
 */
int aeron_cluster_service_container_close(aeron_cluster_service_container_t *container);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_SERVICE_CONTAINER_H */
