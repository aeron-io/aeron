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

#ifndef AERON_CLUSTER_CM_SNAPSHOT_TAKER_H
#define AERON_CLUSTER_CM_SNAPSHOT_TAKER_H

#include <stdint.h>
#include <stddef.h>
#include "aeronc.h"
#include "aeron_cluster_cluster_session.h"
#include "aeron_cluster_timer_service.h"

#ifdef __cplusplus
extern "C"
{
#endif

/* Snapshot type ID for CM snapshots */
#define AERON_CM_SNAPSHOT_MARK_BEGIN   0
#define AERON_CM_SNAPSHOT_MARK_END     2

/** Write SnapshotMarker(BEGIN) */
int aeron_cluster_cm_snapshot_taker_mark_begin(
    aeron_exclusive_publication_t *pub,
    int64_t log_position,
    int64_t leadership_term_id,
    int32_t app_version);

/** Write SnapshotMarker(END) */
int aeron_cluster_cm_snapshot_taker_mark_end(
    aeron_exclusive_publication_t *pub,
    int64_t log_position,
    int64_t leadership_term_id,
    int32_t app_version);

/** Write a ClusterSession record */
int aeron_cluster_cm_snapshot_taker_snapshot_session(
    aeron_exclusive_publication_t *pub,
    aeron_cluster_cluster_session_t *session);

/** Write a Timer record */
int aeron_cluster_cm_snapshot_taker_snapshot_timer(
    aeron_exclusive_publication_t *pub,
    int64_t correlation_id,
    int64_t deadline_ns);

/** Write ConsensusModule state */
int aeron_cluster_cm_snapshot_taker_snapshot_cm_state(
    aeron_exclusive_publication_t *pub,
    int64_t next_session_id,
    int64_t next_service_session_id,
    int64_t log_service_session_id,
    int32_t pending_message_capacity);

/** Write a PendingMessageTracker record for a given service */
int aeron_cluster_cm_snapshot_taker_snapshot_pending_tracker(
    aeron_exclusive_publication_t *pub,
    int64_t next_service_session_id,
    int64_t log_service_session_id,
    int32_t pending_message_capacity,
    int32_t service_id);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_CM_SNAPSHOT_TAKER_H */
