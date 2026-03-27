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

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <inttypes.h>

#include "aeron_cluster_recovery_state.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

/* Helper: check that counter is ALLOCATED and has the right type_id. */
static bool is_recovery_counter(aeron_counters_reader_t *reader, int32_t id)
{
    int32_t state = 0, type_id = 0;
    if (aeron_counters_reader_counter_state(reader, id, &state) < 0) { return false; }
    if (state != AERON_COUNTER_RECORD_ALLOCATED) { return false; }
    if (aeron_counters_reader_counter_type_id(reader, id, &type_id) < 0) { return false; }
    return type_id == AERON_CLUSTER_RECOVERY_STATE_TYPE_ID;
}

int aeron_cluster_recovery_state_allocate(
    aeron_counter_t **counter_out,
    aeron_t *aeron,
    int64_t leadership_term_id,
    int64_t log_position,
    int64_t timestamp,
    int32_t cluster_id,
    const int64_t *snapshot_recording_ids,
    int service_count)
{
    /* Build the key buffer */
    const int key_length =
        AERON_CLUSTER_RECOVERY_STATE_SNAPSHOT_RECORDING_IDS_OFFSET + service_count * (int)sizeof(int64_t);

    if (key_length > (int)AERON_COUNTER_MAX_KEY_LENGTH)
    {
        AERON_SET_ERR(EINVAL,
            "recovery state key length %d exceeds max %zu",
            key_length, AERON_COUNTER_MAX_KEY_LENGTH);
        return -1;
    }

    uint8_t key_buf[AERON_COUNTER_MAX_KEY_LENGTH];
    memset(key_buf, 0, (size_t)key_length);

    memcpy(key_buf + AERON_CLUSTER_RECOVERY_STATE_LEADERSHIP_TERM_ID_OFFSET, &leadership_term_id, sizeof(int64_t));
    memcpy(key_buf + AERON_CLUSTER_RECOVERY_STATE_LOG_POSITION_OFFSET,       &log_position,       sizeof(int64_t));
    memcpy(key_buf + AERON_CLUSTER_RECOVERY_STATE_TIMESTAMP_OFFSET,          &timestamp,          sizeof(int64_t));
    memcpy(key_buf + AERON_CLUSTER_RECOVERY_STATE_CLUSTER_ID_OFFSET,         &cluster_id,         sizeof(int32_t));
    memcpy(key_buf + AERON_CLUSTER_RECOVERY_STATE_SERVICE_COUNT_OFFSET,      &service_count,      sizeof(int32_t));
    for (int i = 0; i < service_count; i++)
    {
        memcpy(key_buf + AERON_CLUSTER_RECOVERY_STATE_SNAPSHOT_RECORDING_IDS_OFFSET + i * (int)sizeof(int64_t),
               &snapshot_recording_ids[i], sizeof(int64_t));
    }

    /* Build human-readable label */
    char label[256];
    snprintf(label, sizeof(label),
        "Cluster recovery: leadershipTermId=%" PRId64 " logPosition=%" PRId64 " clusterId=%" PRId32,
        leadership_term_id, log_position, cluster_id);

    aeron_async_add_counter_t *async = NULL;
    if (aeron_async_add_counter(
        &async, aeron,
        AERON_CLUSTER_RECOVERY_STATE_TYPE_ID,
        key_buf, (size_t)key_length,
        label, strlen(label)) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to start add recovery state counter");
        return -1;
    }

    aeron_counter_t *counter = NULL;
    int rc = 0;
    do { rc = aeron_async_add_counter_poll(&counter, async); } while (0 == rc);
    if (rc < 0 || NULL == counter)
    {
        AERON_APPEND_ERR("%s", "failed to add recovery state counter");
        return -1;
    }

    *counter_out = counter;
    return 0;
}

int32_t aeron_cluster_recovery_state_find_counter_id(
    aeron_counters_reader_t *reader,
    int32_t cluster_id)
{
    int32_t max_id = aeron_counters_reader_max_counter_id(reader);
    for (int32_t i = 0; i <= max_id; i++)
    {
        int32_t state = 0;
        if (aeron_counters_reader_counter_state(reader, i, &state) < 0) { continue; }
        if (state == AERON_COUNTER_RECORD_UNUSED) { break; }
        if (state != AERON_COUNTER_RECORD_ALLOCATED) { continue; }

        int32_t type_id = 0;
        if (aeron_counters_reader_counter_type_id(reader, i, &type_id) < 0) { continue; }
        if (type_id != AERON_CLUSTER_RECOVERY_STATE_TYPE_ID) { continue; }

        uint8_t *key = NULL;
        if (aeron_counters_reader_metadata_key(reader, i, &key) < 0 || NULL == key) { continue; }

        int32_t stored_cluster_id = 0;
        memcpy(&stored_cluster_id, key + AERON_CLUSTER_RECOVERY_STATE_CLUSTER_ID_OFFSET, sizeof(int32_t));
        if (stored_cluster_id == cluster_id)
        {
            return i;
        }
    }
    return AERON_NULL_COUNTER_ID;
}

int64_t aeron_cluster_recovery_state_get_leadership_term_id(
    aeron_counters_reader_t *reader, int32_t counter_id)
{
    if (!is_recovery_counter(reader, counter_id)) { return -1; }
    uint8_t *key = NULL;
    if (aeron_counters_reader_metadata_key(reader, counter_id, &key) < 0 || NULL == key) { return -1; }
    int64_t v;
    memcpy(&v, key + AERON_CLUSTER_RECOVERY_STATE_LEADERSHIP_TERM_ID_OFFSET, sizeof(int64_t));
    return v;
}

int64_t aeron_cluster_recovery_state_get_log_position(
    aeron_counters_reader_t *reader, int32_t counter_id)
{
    if (!is_recovery_counter(reader, counter_id)) { return -1; }
    uint8_t *key = NULL;
    if (aeron_counters_reader_metadata_key(reader, counter_id, &key) < 0 || NULL == key) { return -1; }
    int64_t v;
    memcpy(&v, key + AERON_CLUSTER_RECOVERY_STATE_LOG_POSITION_OFFSET, sizeof(int64_t));
    return v;
}

int64_t aeron_cluster_recovery_state_get_timestamp(
    aeron_counters_reader_t *reader, int32_t counter_id)
{
    if (!is_recovery_counter(reader, counter_id)) { return -1; }
    uint8_t *key = NULL;
    if (aeron_counters_reader_metadata_key(reader, counter_id, &key) < 0 || NULL == key) { return -1; }
    int64_t v;
    memcpy(&v, key + AERON_CLUSTER_RECOVERY_STATE_TIMESTAMP_OFFSET, sizeof(int64_t));
    return v;
}

int64_t aeron_cluster_recovery_state_get_snapshot_recording_id(
    aeron_counters_reader_t *reader, int32_t counter_id, int32_t service_id)
{
    if (!is_recovery_counter(reader, counter_id)) { return -1; }
    uint8_t *key = NULL;
    if (aeron_counters_reader_metadata_key(reader, counter_id, &key) < 0 || NULL == key) { return -1; }

    int32_t service_count = 0;
    memcpy(&service_count, key + AERON_CLUSTER_RECOVERY_STATE_SERVICE_COUNT_OFFSET, sizeof(int32_t));
    if (service_id < 0 || service_id >= service_count) { return -1; }

    int64_t v;
    memcpy(&v,
        key + AERON_CLUSTER_RECOVERY_STATE_SNAPSHOT_RECORDING_IDS_OFFSET + service_id * (int)sizeof(int64_t),
        sizeof(int64_t));
    return v;
}
