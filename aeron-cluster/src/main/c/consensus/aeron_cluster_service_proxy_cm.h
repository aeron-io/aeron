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

#ifndef AERON_CLUSTER_SERVICE_PROXY_CM_H
#define AERON_CLUSTER_SERVICE_PROXY_CM_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define AERON_CLUSTER_SERVICE_PROXY_BUFFER_LENGTH (8 * 1024)

/**
 * Sends commands FROM the ConsensusModule TO clustered services.
 * This is the counterpart to ServiceProxy in Java.
 *
 * Messages go on the IPC publication from CM to service
 * (controlChannel, consensusModuleStreamId=104).
 */
typedef struct aeron_cluster_service_proxy_cm_stct
{
    aeron_exclusive_publication_t *publication;
    int                            service_count;
    uint8_t                        buffer[AERON_CLUSTER_SERVICE_PROXY_BUFFER_LENGTH];
}
aeron_cluster_service_proxy_cm_t;

int aeron_cluster_service_proxy_cm_init(
    aeron_cluster_service_proxy_cm_t *proxy,
    aeron_exclusive_publication_t *publication,
    int service_count);

/** Send JoinLog to a specific service (or all services if service_id == -1). */
bool aeron_cluster_service_proxy_cm_join_log(
    aeron_cluster_service_proxy_cm_t *proxy,
    int64_t log_position,
    int64_t max_log_position,
    int32_t member_id,
    int32_t log_session_id,
    int32_t log_stream_id,
    bool is_startup,
    int32_t role,
    const char *log_channel);

/** Send ServiceTerminationPosition. */
bool aeron_cluster_service_proxy_cm_termination_position(
    aeron_cluster_service_proxy_cm_t *proxy,
    int64_t log_position);

/** Send RequestServiceAck. */
bool aeron_cluster_service_proxy_cm_request_service_ack(
    aeron_cluster_service_proxy_cm_t *proxy,
    int64_t log_position);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_SERVICE_PROXY_CM_H */
