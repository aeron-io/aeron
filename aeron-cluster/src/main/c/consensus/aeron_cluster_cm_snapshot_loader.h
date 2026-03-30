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

#ifndef AERON_CLUSTER_CM_SNAPSHOT_LOADER_H
#define AERON_CLUSTER_CM_SNAPSHOT_LOADER_H

#include <stdint.h>
#include <stdbool.h>
#include "aeronc.h"
#include "aeron_cluster_session_manager.h"
#include "aeron_cluster_timer_service.h"
#include "aeron_cluster_pending_message_tracker.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Loads a ConsensusModule snapshot from an Aeron image (fragment-by-fragment).
 * Mirrors Java ConsensusModuleSnapshotLoader.
 *
 * Usage:
 *   1. aeron_cluster_cm_snapshot_loader_create(&loader, session_manager, timer_service)
 *   2. While !loader->is_done: poll loader->image, passing loader_on_fragment as handler
 *   3. aeron_cluster_cm_snapshot_loader_close(loader)
 */
typedef struct aeron_cluster_cm_snapshot_loader_stct
{
    aeron_image_t                    *image;
    aeron_cluster_session_manager_t  *session_manager;
    aeron_cluster_timer_service_t    *timer_service;
    aeron_cluster_pending_message_tracker_t *pending_trackers; /* [pending_tracker_count], may be NULL */
    int                               pending_tracker_count;

    /* Set after successfully reading the consensusModule state record */
    int64_t  next_session_id;
    int64_t  next_service_session_id;
    int64_t  log_service_session_id;
    int32_t  pending_message_capacity;
    int32_t  app_version;
    bool     has_cm_state;

    bool     in_snapshot;  /* true after BEGIN marker, before END */
    bool     is_done;      /* true once END marker processed */
    bool     has_error;
}
aeron_cluster_cm_snapshot_loader_t;

/**
 * Initialize a snapshot loader bound to the given session manager and timer service.
 * Both may be NULL (records are ignored if their target is NULL).
 * pending_trackers is an optional array of service message trackers (may be NULL).
 */
int aeron_cluster_cm_snapshot_loader_create(
    aeron_cluster_cm_snapshot_loader_t **loader,
    aeron_image_t                       *image,
    aeron_cluster_session_manager_t     *session_manager,
    aeron_cluster_timer_service_t       *timer_service,
    aeron_cluster_pending_message_tracker_t *pending_trackers,
    int                                  pending_tracker_count);

/**
 * Poll up to fragment_limit fragments. Returns work count (>0 = progress).
 * Sets loader->is_done when END marker is found.
 * Returns -1 on error.
 */
int aeron_cluster_cm_snapshot_loader_poll(
    aeron_cluster_cm_snapshot_loader_t *loader, int fragment_limit);

void aeron_cluster_cm_snapshot_loader_close(
    aeron_cluster_cm_snapshot_loader_t *loader);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_CM_SNAPSHOT_LOADER_H */
