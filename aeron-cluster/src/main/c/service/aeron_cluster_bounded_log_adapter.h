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

#ifndef AERON_CLUSTER_BOUNDED_LOG_ADAPTER_H
#define AERON_CLUSTER_BOUNDED_LOG_ADAPTER_H

#include <stdint.h>
#include <stdbool.h>
#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define AERON_CLUSTER_LOG_FRAGMENT_LIMIT 10

typedef struct aeron_clustered_service_agent_stct aeron_clustered_service_agent_t;

typedef struct aeron_cluster_bounded_log_adapter_stct
{
    aeron_subscription_t              *subscription;
    aeron_image_t                     *image;
    aeron_controlled_fragment_assembler_t *fragment_assembler;
    aeron_clustered_service_agent_t   *agent;
    int64_t                            max_log_position;
}
aeron_cluster_bounded_log_adapter_t;

int  aeron_cluster_bounded_log_adapter_create(
    aeron_cluster_bounded_log_adapter_t **adapter,
    aeron_subscription_t *subscription,
    aeron_image_t *image,
    int64_t max_log_position,
    aeron_clustered_service_agent_t *agent);

int  aeron_cluster_bounded_log_adapter_close(aeron_cluster_bounded_log_adapter_t *adapter);

/**
 * Poll the log up to commit_position.
 * Returns fragments polled, 0 at boundary, -1 on error.
 */
int  aeron_cluster_bounded_log_adapter_poll(
    aeron_cluster_bounded_log_adapter_t *adapter,
    int64_t commit_position);

bool aeron_cluster_bounded_log_adapter_is_image_closed(aeron_cluster_bounded_log_adapter_t *adapter);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_BOUNDED_LOG_ADAPTER_H */
