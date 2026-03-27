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

#ifndef AERON_CLUSTER_LOG_ADAPTER_H
#define AERON_CLUSTER_LOG_ADAPTER_H

#include <stdint.h>
#include <stdbool.h>
#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define AERON_CLUSTER_LOG_ADAPTER_FRAGMENT_LIMIT 10

typedef struct aeron_consensus_module_agent_stct aeron_consensus_module_agent_t;

/**
 * Adapts a log image during replay.  Mirrors Java's LogAdapter.
 *
 * The adapter polls the image up to `stop_position` and dispatches each
 * decoded SBE message to the corresponding on_replay_* callback on the agent.
 * When the image is NULL, position() returns the last tracked log position.
 */
typedef struct aeron_cluster_log_adapter_stct
{
    aeron_image_t                         *image;
    aeron_subscription_t                  *subscription;  /* subscription owning the image */
    aeron_controlled_fragment_assembler_t *fragment_assembler;
    aeron_consensus_module_agent_t        *agent;
    int64_t                                log_position;  /* cached when image is absent */
    int                                    fragment_limit;
}
aeron_cluster_log_adapter_t;

int aeron_cluster_log_adapter_create(
    aeron_cluster_log_adapter_t **adapter,
    aeron_consensus_module_agent_t *agent,
    int fragment_limit);

int aeron_cluster_log_adapter_close(aeron_cluster_log_adapter_t *adapter);

/**
 * Poll the image up to stop_position (bounded controlled poll).
 * Returns fragments polled, 0 if image is NULL or at boundary, -1 on error.
 */
int aeron_cluster_log_adapter_poll(
    aeron_cluster_log_adapter_t *adapter,
    int64_t stop_position);

/** Current log position: image.position() if image present, else cached value. */
int64_t aeron_cluster_log_adapter_position(aeron_cluster_log_adapter_t *adapter);

bool aeron_cluster_log_adapter_is_image_closed(aeron_cluster_log_adapter_t *adapter);
bool aeron_cluster_log_adapter_is_end_of_stream(aeron_cluster_log_adapter_t *adapter);

/** Attach a new image (from a log replay subscription). */
void aeron_cluster_log_adapter_set_image(
    aeron_cluster_log_adapter_t *adapter,
    aeron_image_t *image,
    aeron_subscription_t *subscription);

/**
 * Disconnect: saves position, closes the underlying subscription, sets image=NULL.
 * Returns the subscription registration ID, or -1 if not connected.
 */
int64_t aeron_cluster_log_adapter_disconnect(aeron_cluster_log_adapter_t *adapter);

/**
 * Disconnect and clamp log_position to max_log_position.
 */
void aeron_cluster_log_adapter_disconnect_max(
    aeron_cluster_log_adapter_t *adapter,
    int64_t max_log_position);

/** Remove a destination from the underlying subscription (for catchup). */
void aeron_cluster_log_adapter_async_remove_destination(
    aeron_cluster_log_adapter_t *adapter,
    aeron_t *aeron,
    const char *destination);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_LOG_ADAPTER_H */
