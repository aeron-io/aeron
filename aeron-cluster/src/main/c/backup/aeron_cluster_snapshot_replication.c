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

#include <stdlib.h>
#include <string.h>

#include "aeron_cluster_snapshot_replication.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

int aeron_cluster_snapshot_replication_create(
    aeron_cluster_snapshot_replication_t **replication,
    aeron_archive_t *archive,
    aeron_t *aeron,
    int32_t src_control_stream_id,
    const char *src_control_channel,
    const char *replication_channel,
    int64_t progress_timeout_ns,
    int64_t progress_interval_ns)
{
    aeron_cluster_snapshot_replication_t *r = NULL;
    if (aeron_alloc((void **)&r, sizeof(*r)) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to allocate SnapshotReplication");
        return -1;
    }
    memset(r, 0, sizeof(*r));

    if (aeron_cluster_multi_recording_replication_create(
        &r->multi,
        archive, aeron,
        src_control_stream_id,
        src_control_channel,
        replication_channel,
        NULL,
        progress_timeout_ns,
        progress_interval_ns) < 0)
    {
        aeron_free(r);
        AERON_APPEND_ERR("%s", "failed to create MultipleRecordingReplication for SnapshotReplication");
        return -1;
    }

    r->snapshots_pending          = NULL;
    r->snapshots_pending_count    = 0;
    r->snapshots_pending_capacity = 0;

    *replication = r;
    return 0;
}

int aeron_cluster_snapshot_replication_add_snapshot(
    aeron_cluster_snapshot_replication_t *replication,
    const aeron_cluster_recording_log_entry_t *snapshot)
{
    /* Grow if needed */
    if (replication->snapshots_pending_count == replication->snapshots_pending_capacity)
    {
        int new_cap = replication->snapshots_pending_capacity == 0 ? 8
                      : replication->snapshots_pending_capacity * 2;
        aeron_cluster_recording_log_entry_t *new_arr = NULL;
        if (aeron_alloc((void **)&new_arr,
            (size_t)new_cap * sizeof(aeron_cluster_recording_log_entry_t)) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to grow snapshots_pending");
            return -1;
        }
        if (replication->snapshots_pending)
        {
            memcpy(new_arr, replication->snapshots_pending,
                (size_t)replication->snapshots_pending_count *
                sizeof(aeron_cluster_recording_log_entry_t));
            aeron_free(replication->snapshots_pending);
        }
        replication->snapshots_pending          = new_arr;
        replication->snapshots_pending_capacity = new_cap;
    }

    replication->snapshots_pending[replication->snapshots_pending_count++] = *snapshot;

    /* NULL_VALUE for dst_recording_id and stop_position — let archive allocate */
    return aeron_cluster_multi_recording_replication_add_recording(
        replication->multi,
        snapshot->recording_id,
        AERON_NULL_VALUE,
        AERON_NULL_VALUE);
}

int aeron_cluster_snapshot_replication_poll(
    aeron_cluster_snapshot_replication_t *replication, int64_t now_ns)
{
    return aeron_cluster_multi_recording_replication_poll(replication->multi, now_ns);
}

void aeron_cluster_snapshot_replication_on_signal(
    aeron_cluster_snapshot_replication_t *replication,
    const aeron_archive_recording_signal_t *signal)
{
    aeron_cluster_multi_recording_replication_on_signal(replication->multi, signal);
}

bool aeron_cluster_snapshot_replication_is_complete(
    const aeron_cluster_snapshot_replication_t *replication)
{
    return aeron_cluster_multi_recording_replication_is_complete(replication->multi);
}

int aeron_cluster_snapshot_replication_snapshots_retrieved(
    const aeron_cluster_snapshot_replication_t *replication,
    aeron_cluster_recording_log_entry_t *out_entries,
    int out_capacity)
{
    int written = 0;
    for (int i = 0; i < replication->snapshots_pending_count && written < out_capacity; i++)
    {
        const aeron_cluster_recording_log_entry_t *pending = &replication->snapshots_pending[i];
        int64_t dst_id = aeron_cluster_multi_recording_replication_completed_dst(
            replication->multi, pending->recording_id);
        if (AERON_NULL_VALUE == dst_id)
        {
            continue;  /* replication not complete for this snapshot — skip */
        }
        out_entries[written]             = *pending;
        out_entries[written].recording_id = dst_id;
        written++;
    }
    return written;
}

const aeron_cluster_recording_log_entry_t *aeron_cluster_snapshot_replication_current_snapshot(
    const aeron_cluster_snapshot_replication_t *replication)
{
    int64_t src_id = aeron_cluster_multi_recording_replication_current_src_recording_id(
        replication->multi);
    if (AERON_NULL_VALUE == src_id)
    {
        return NULL;
    }
    for (int i = 0; i < replication->snapshots_pending_count; i++)
    {
        if (replication->snapshots_pending[i].recording_id == src_id)
        {
            return &replication->snapshots_pending[i];
        }
    }
    return NULL;
}

int64_t aeron_cluster_snapshot_replication_current_src_recording_id(
    const aeron_cluster_snapshot_replication_t *replication)
{
    return aeron_cluster_multi_recording_replication_current_src_recording_id(replication->multi);
}

int64_t aeron_cluster_snapshot_replication_current_replication_id(
    const aeron_cluster_snapshot_replication_t *replication)
{
    return aeron_cluster_multi_recording_replication_current_replication_id(replication->multi);
}

int aeron_cluster_snapshot_replication_close(
    aeron_cluster_snapshot_replication_t *replication)
{
    if (NULL == replication) { return 0; }
    if (NULL != replication->multi)
    {
        aeron_cluster_multi_recording_replication_close(replication->multi);
    }
    if (NULL != replication->snapshots_pending)
    {
        aeron_free(replication->snapshots_pending);
    }
    aeron_free(replication);
    return 0;
}
