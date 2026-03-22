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

#ifndef AERON_CLUSTER_CONSENSUS_MODULE_PROXY_H
#define AERON_CLUSTER_CONSENSUS_MODULE_PROXY_H

#include <stdint.h>
#include <stdbool.h>

#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define AERON_CLUSTER_PROXY_BUFFER_LENGTH  (8 * 1024)
#define AERON_CLUSTER_PROXY_RETRY_ATTEMPTS 3

typedef struct aeron_cluster_consensus_module_proxy_stct
{
    aeron_exclusive_publication_t *publication;
    int                            retry_attempts;
    int32_t                        service_id;
    uint8_t                        buffer[AERON_CLUSTER_PROXY_BUFFER_LENGTH];
}
aeron_cluster_consensus_module_proxy_t;

int aeron_cluster_consensus_module_proxy_init(
    aeron_cluster_consensus_module_proxy_t *proxy,
    aeron_exclusive_publication_t *publication,
    int retry_attempts,
    int32_t service_id);

bool aeron_cluster_consensus_module_proxy_schedule_timer(
    aeron_cluster_consensus_module_proxy_t *proxy,
    int64_t correlation_id,
    int64_t deadline);

bool aeron_cluster_consensus_module_proxy_cancel_timer(
    aeron_cluster_consensus_module_proxy_t *proxy,
    int64_t correlation_id);

bool aeron_cluster_consensus_module_proxy_ack(
    aeron_cluster_consensus_module_proxy_t *proxy,
    int64_t log_position,
    int64_t timestamp,
    int64_t ack_id,
    int64_t relevant_id,
    int32_t service_id);

bool aeron_cluster_consensus_module_proxy_close_session(
    aeron_cluster_consensus_module_proxy_t *proxy,
    int64_t cluster_session_id);

/**
 * Offer a message to the CM (service→client reply, routed via the log).
 * Prepends SessionMessageHeader automatically.
 */
int64_t aeron_cluster_consensus_module_proxy_offer(
    aeron_cluster_consensus_module_proxy_t *proxy,
    int64_t cluster_session_id,
    int64_t leadership_term_id,
    const uint8_t *buffer,
    size_t length);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_CONSENSUS_MODULE_PROXY_H */
