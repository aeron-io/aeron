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

#ifndef AERON_CLUSTER_SERVICE_SNAPSHOT_LOADER_H
#define AERON_CLUSTER_SERVICE_SNAPSHOT_LOADER_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Thin wrapper around an aeron_image_t for reading a snapshot.
 * Passed to on_start() when recovering.
 */
typedef struct aeron_cluster_snapshot_image_stct
{
    aeron_image_t *image;
    bool           is_done;
}
aeron_cluster_snapshot_image_t;

/**
 * Callback invoked for each ClientSession record found in the snapshot.
 */
typedef void (*aeron_cluster_snapshot_session_loader_func_t)(
    void *clientd,
    int64_t cluster_session_id,
    int32_t response_stream_id,
    const char *response_channel,
    const uint8_t *encoded_principal,
    size_t principal_length);

/**
 * Poll the snapshot image and call on_session for each ClientSession record.
 * Stops at SnapshotMarker(END).
 * Returns 0 on success, -1 on error.
 */
int aeron_cluster_service_snapshot_loader_load_sessions(
    aeron_cluster_snapshot_image_t *snapshot,
    aeron_cluster_snapshot_session_loader_func_t on_session,
    void *clientd);

/** Returns true if the snapshot has been fully consumed. */
bool aeron_cluster_snapshot_image_is_done(aeron_cluster_snapshot_image_t *snapshot);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_SERVICE_SNAPSHOT_LOADER_H */
