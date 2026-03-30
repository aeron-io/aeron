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
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "aeron_cluster_standby_snapshot_replicator.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

static int compare_entries_by_log_position_desc(const void *a, const void *b)
{
    const aeron_cluster_standby_snapshot_entry_t *ea = (const aeron_cluster_standby_snapshot_entry_t *)a;
    const aeron_cluster_standby_snapshot_entry_t *eb = (const aeron_cluster_standby_snapshot_entry_t *)b;
    /* Descending by log_position */
    if (eb->log_position > ea->log_position) { return 1; }
    if (eb->log_position < ea->log_position) { return -1; }
    return strcmp(ea->endpoint, eb->endpoint);
}

int aeron_cluster_standby_snapshot_replicator_create(
    aeron_cluster_standby_snapshot_replicator_t **replicator,
    int32_t member_id,
    aeron_archive_t *archive,
    aeron_t *aeron,
    aeron_cluster_recording_log_t *recording_log,
    int service_count,
    const char *archive_control_channel,
    int32_t archive_control_stream_id,
    const char *replication_channel,
    int file_sync_level)
{
    aeron_cluster_standby_snapshot_replicator_t *r = NULL;
    if (aeron_alloc((void **)&r, sizeof(*r)) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to allocate StandbySnapshotReplicator");
        return -1;
    }
    memset(r, 0, sizeof(*r));

    r->member_id                = member_id;
    r->archive                  = archive;
    r->aeron                    = aeron;
    r->recording_log            = recording_log;
    r->service_count            = service_count;
    r->archive_control_stream_id = archive_control_stream_id;
    r->file_sync_level          = file_sync_level;
    r->current_index            = 0;
    r->is_complete              = false;
    r->endpoint_error_count     = 0;
    r->snapshot_counter         = NULL;
    r->event_listener           = NULL;
    r->event_listener_clientd   = NULL;

    snprintf(r->archive_control_channel, sizeof(r->archive_control_channel), "%s",
        archive_control_channel ? archive_control_channel : "");
    snprintf(r->replication_channel, sizeof(r->replication_channel), "%s",
        replication_channel ? replication_channel : "");

    *replicator = r;
    return 0;
}

/*
 * Build the list of standby snapshot entries to replicate, grouped by endpoint.
 * Uses archive_endpoint stored per-entry in the recording log; falls back to
 * the configured archive_control_channel for entries without an endpoint.
 */
static int build_entries(aeron_cluster_standby_snapshot_replicator_t *r)
{
    if (r->entry_count > 0 || NULL == r->recording_log)
    {
        return 0;
    }

    /* Get the latest complete snapshot set per endpoint (Java-parity logic). */
    aeron_cluster_recording_log_entry_t *raw = NULL;
    int raw_count = 0;
    if (aeron_cluster_recording_log_latest_standby_snapshots(
        r->recording_log, r->service_count, &raw, &raw_count) < 0)
    {
        return -1;
    }

    if (raw_count == 0)
    {
        r->is_complete = true;
        return 0;
    }

    /* Filter out entries already present in the local recording log
     * (i.e., local has an equal or newer snapshot for this service_id).
     * Mirrors Java StandbySnapshotReplicator.filterByExistingRecordingLogEntries(). */
    int kept = 0;
    for (int i = 0; i < raw_count; i++)
    {
        const aeron_cluster_recording_log_entry_t *e = &raw[i];
        const char *ep = (e->archive_endpoint[0] != '\0')
            ? e->archive_endpoint
            : r->archive_control_channel;

        const aeron_cluster_recording_log_entry_t *local =
            aeron_cluster_recording_log_get_latest_snapshot(r->recording_log, e->service_id);
        if (NULL != local && e->log_position <= local->log_position)
        {
            continue; /* already have this or newer locally */
        }

        /* Keep: write back in-place to raw[] */
        raw[kept] = *e;
        /* Override archive_endpoint with fallback if needed */
        if (raw[kept].archive_endpoint[0] == '\0')
        {
            snprintf(raw[kept].archive_endpoint,
                sizeof(raw[kept].archive_endpoint), "%s", ep);
        }
        kept++;
    }

    if (kept == 0)
    {
        aeron_free(raw);
        r->is_complete = true;
        return 0;
    }

    /* Group filtered entries by endpoint. Each group becomes one
     * aeron_cluster_standby_snapshot_entry_t. */
    if (aeron_alloc((void **)&r->entries,
        (size_t)kept * sizeof(aeron_cluster_standby_snapshot_entry_t)) < 0)
    {
        aeron_free(raw);
        AERON_APPEND_ERR("%s", "failed to allocate standby entries");
        return -1;
    }
    r->entry_capacity = kept;
    memset(r->entries, 0, (size_t)kept * sizeof(aeron_cluster_standby_snapshot_entry_t));

    for (int g = 0; g < kept; g++)
    {
        if (aeron_alloc((void **)&r->entries[g].entries,
            (size_t)kept * sizeof(aeron_cluster_recording_log_entry_t)) < 0)
        {
            for (int k = 0; k < g; k++) { aeron_free(r->entries[k].entries); }
            aeron_free(r->entries);
            r->entries = NULL;
            aeron_free(raw);
            AERON_APPEND_ERR("%s", "failed to allocate standby entry list");
            return -1;
        }
        r->entries[g].log_position = INT64_MIN;
    }

    int group_count = 0;
    for (int i = 0; i < kept; i++)
    {
        const aeron_cluster_recording_log_entry_t *e = &raw[i];

        int g = -1;
        for (int k = 0; k < group_count; k++)
        {
            if (strcmp(r->entries[k].endpoint, e->archive_endpoint) == 0) { g = k; break; }
        }
        if (g < 0)
        {
            g = group_count++;
            snprintf(r->entries[g].endpoint, sizeof(r->entries[g].endpoint),
                "%.255s", e->archive_endpoint);
        }

        aeron_cluster_standby_snapshot_entry_t *grp = &r->entries[g];
        grp->entries[grp->entry_count++] = *e;
        if (e->log_position > grp->log_position)
        {
            grp->log_position = e->log_position;
        }
    }

    aeron_free(raw);

    /* Free unused group slots */
    for (int g = group_count; g < kept; g++)
    {
        aeron_free(r->entries[g].entries);
        r->entries[g].entries = NULL;
    }

    /* Sort groups by log_position descending */
    qsort(r->entries, (size_t)group_count, sizeof(*r->entries),
        compare_entries_by_log_position_desc);

    r->entry_count = group_count;
    return 0;
}

int aeron_cluster_standby_snapshot_replicator_poll(
    aeron_cluster_standby_snapshot_replicator_t *replicator, int64_t now_ns)
{
    if (replicator->is_complete) { return 0; }

    /* Lazy-build entries on first poll */
    if (replicator->entry_count == 0 && !replicator->is_complete)
    {
        if (build_entries(replicator) < 0) { return -1; }
        if (replicator->is_complete) { return 1; }
        if (replicator->entry_count == 0) { replicator->is_complete = true; return 1; }
    }

    /* Nothing left to replicate */
    if (replicator->current_index >= replicator->entry_count)
    {
        replicator->is_complete = true;
        return 1;
    }

    aeron_cluster_standby_snapshot_entry_t *current =
        &replicator->entries[replicator->current_index];

    /* Create replication if not yet started */
    if (NULL == replicator->recording_replication)
    {
        const int64_t progress_timeout_ns   = INT64_C(10000000000); /* 10 s */
        const int64_t progress_interval_ns  = INT64_C(1000000000);  /* 1 s */

        if (aeron_cluster_multi_recording_replication_create(
            &replicator->recording_replication,
            replicator->archive,
            replicator->aeron,
            replicator->archive_control_stream_id,
            current->endpoint,
            replicator->replication_channel,
            NULL,
            progress_timeout_ns,
            progress_interval_ns) < 0)
        {
            return -1;
        }

        for (int i = 0; i < current->entry_count; i++)
        {
            aeron_cluster_multi_recording_replication_add_recording(
                replicator->recording_replication,
                current->entries[i].recording_id,
                AERON_NULL_VALUE,
                AERON_NULL_VALUE);
        }
    }

    int work = aeron_cluster_multi_recording_replication_poll(
        replicator->recording_replication, now_ns);

    /* Poll archive for recording signals — mirrors Java archive.pollForRecordingSignals() */
    if (NULL != replicator->archive)
    {
        int32_t sig_count = 0;
        aeron_archive_poll_for_recording_signals(&sig_count, replicator->archive);
    }

    /* Error handling — mirrors Java try-catch around poll + pollForRecordingSignals.
     * On error, store per-endpoint error, close replication, advance to next endpoint. */
    if (work < 0)
    {
        if (replicator->endpoint_error_count < 16)
        {
            snprintf(replicator->endpoint_errors[replicator->endpoint_error_count],
                     sizeof(replicator->endpoint_errors[0]),
                     "%.128s: %.125s", current->endpoint, aeron_errmsg());
            replicator->endpoint_error_count++;
        }
        aeron_cluster_multi_recording_replication_close(replicator->recording_replication);
        replicator->recording_replication = NULL;
        replicator->current_index++;
        if (replicator->current_index >= replicator->entry_count)
        {
            replicator->is_complete = true;
        }
        return 1;
    }

    if (aeron_cluster_multi_recording_replication_is_complete(replicator->recording_replication))
    {
        /* Write replicated entries into recording log */
        for (int i = 0; i < current->entry_count; i++)
        {
            const aeron_cluster_recording_log_entry_t *e = &current->entries[i];
            int64_t dst_id = aeron_cluster_multi_recording_replication_completed_dst(
                replicator->recording_replication, e->recording_id);
            aeron_cluster_recording_log_append_standby_snapshot(
                replicator->recording_log,
                dst_id,
                e->leadership_term_id,
                e->term_base_log_position,
                e->log_position,
                e->timestamp,
                e->service_id,
                current->endpoint);
        }

        /* Force recording log to disk after appending — mirrors Java recordingLog.force(fileSyncLevel) */
        aeron_cluster_recording_log_force(replicator->recording_log);

        /* Increment snapshot counter — mirrors Java snapshotCounter.incrementRelease() */
        if (NULL != replicator->snapshot_counter)
        {
            int64_t *cp = aeron_counter_addr(replicator->snapshot_counter);
            if (NULL != cp) { (*cp)++; }
        }

        /* Fire event listener — mirrors Java logReplicationEnded() */
        if (NULL != replicator->event_listener)
        {
            replicator->event_listener(
                replicator->event_listener_clientd,
                current->endpoint, 0, 0, 0, true);
        }

        aeron_cluster_multi_recording_replication_close(replicator->recording_replication);
        replicator->recording_replication = NULL;
        replicator->current_index++;

        if (replicator->current_index >= replicator->entry_count)
        {
            replicator->is_complete = true;
        }
        work++;
    }

    return work;
}

void aeron_cluster_standby_snapshot_replicator_on_signal(
    aeron_cluster_standby_snapshot_replicator_t *replicator,
    const aeron_archive_recording_signal_t *signal)
{
    if (NULL != replicator->recording_replication)
    {
        aeron_cluster_multi_recording_replication_on_signal(
            replicator->recording_replication, signal);
    }
}

bool aeron_cluster_standby_snapshot_replicator_is_complete(
    const aeron_cluster_standby_snapshot_replicator_t *replicator)
{
    return replicator->is_complete;
}

void aeron_cluster_standby_snapshot_replicator_close(
    aeron_cluster_standby_snapshot_replicator_t *replicator)
{
    if (NULL == replicator) { return; }

    if (NULL != replicator->recording_replication)
    {
        aeron_cluster_multi_recording_replication_close(replicator->recording_replication);
        replicator->recording_replication = NULL;
    }

    for (int i = 0; i < replicator->entry_count; i++)
    {
        if (NULL != replicator->entries[i].entries)
        {
            aeron_free(replicator->entries[i].entries);
        }
    }

    if (NULL != replicator->entries)
    {
        aeron_free(replicator->entries);
    }

    /* Note: archive is owned externally (or by caller) when no separate ctx */
    aeron_free(replicator);
}
