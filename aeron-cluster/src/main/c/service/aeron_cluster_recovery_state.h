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

#ifndef AERON_CLUSTER_RECOVERY_STATE_H
#define AERON_CLUSTER_RECOVERY_STATE_H

#include <stdint.h>
#include <stdbool.h>
#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Counter type ID for the cluster recovery state counter.
 * Matches Java AeronCounters.CLUSTER_RECOVERY_STATE_TYPE_ID = 204.
 */
#define AERON_CLUSTER_RECOVERY_STATE_TYPE_ID  (204)

/**
 * Key layout offsets (matching Java RecoveryState exactly).
 */
#define AERON_CLUSTER_RECOVERY_STATE_LEADERSHIP_TERM_ID_OFFSET     (0)
#define AERON_CLUSTER_RECOVERY_STATE_LOG_POSITION_OFFSET           (8)
#define AERON_CLUSTER_RECOVERY_STATE_TIMESTAMP_OFFSET              (16)
#define AERON_CLUSTER_RECOVERY_STATE_CLUSTER_ID_OFFSET             (24)
#define AERON_CLUSTER_RECOVERY_STATE_SERVICE_COUNT_OFFSET          (28)
#define AERON_CLUSTER_RECOVERY_STATE_SNAPSHOT_RECORDING_IDS_OFFSET (32)

/**
 * Allocate a recovery-state counter.
 *
 * @param counter_out        out — newly created counter (caller must close when done).
 * @param aeron              client.
 * @param leadership_term_id term ID at which the snapshot was taken (-1 if no snapshot).
 * @param log_position       log position of the snapshot (0 if no snapshot).
 * @param timestamp          time of the snapshot.
 * @param cluster_id         cluster instance identifier.
 * @param snapshot_recording_ids  array of recording IDs, one per service; index 0 = CM.
 * @param service_count      length of snapshot_recording_ids.
 * @return 0 on success, -1 on error.
 */
int aeron_cluster_recovery_state_allocate(
    aeron_counter_t **counter_out,
    aeron_t *aeron,
    int64_t leadership_term_id,
    int64_t log_position,
    int64_t timestamp,
    int32_t cluster_id,
    const int64_t *snapshot_recording_ids,
    int service_count);

/**
 * Find the counter_id of the active recovery-state counter for the given cluster_id.
 *
 * @return counter_id (>= 0) if found, AERON_NULL_COUNTER_ID (-1) if not found.
 */
int32_t aeron_cluster_recovery_state_find_counter_id(
    aeron_counters_reader_t *reader,
    int32_t cluster_id);

/**
 * Helpers to read fields from the counter key.
 * All return AERON_NULL_VALUE (-1) if the counter is not active or type does not match.
 */
int64_t aeron_cluster_recovery_state_get_leadership_term_id(
    aeron_counters_reader_t *reader, int32_t counter_id);

int64_t aeron_cluster_recovery_state_get_log_position(
    aeron_counters_reader_t *reader, int32_t counter_id);

int64_t aeron_cluster_recovery_state_get_timestamp(
    aeron_counters_reader_t *reader, int32_t counter_id);

int64_t aeron_cluster_recovery_state_get_snapshot_recording_id(
    aeron_counters_reader_t *reader, int32_t counter_id, int32_t service_id);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_RECOVERY_STATE_H */
