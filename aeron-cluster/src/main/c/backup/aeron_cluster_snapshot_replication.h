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

#ifndef AERON_CLUSTER_SNAPSHOT_REPLICATION_H
#define AERON_CLUSTER_SNAPSHOT_REPLICATION_H

#include <stdint.h>
#include <stdbool.h>
#include "consensus/aeron_cluster_recording_log.h"
#include "consensus/aeron_cluster_multi_recording_replication.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Manages replication of a set of snapshot recordings from the cluster archive
 * to the local backup archive.  Mirrors Java's SnapshotReplication.java.
 *
 * Usage:
 *   create → add_snapshot (×N) → poll / on_signal (until is_complete) → close
 */

#define AERON_CLUSTER_SNAPSHOT_REPLICATION_PROGRESS_TIMEOUT_NS  INT64_C(10000000000)  /* 10 s */
#define AERON_CLUSTER_SNAPSHOT_REPLICATION_PROGRESS_INTERVAL_NS INT64_C(1000000000)   /* 1 s */

typedef struct aeron_cluster_snapshot_replication_stct
{
    aeron_cluster_multi_recording_replication_t *multi;

    /* Parallel array — mirrors the multi's add_recording order */
    aeron_cluster_recording_log_entry_t *snapshots_pending;
    int                                  snapshots_pending_count;
    int                                  snapshots_pending_capacity;
}
aeron_cluster_snapshot_replication_t;

/**
 * Create a SnapshotReplication.
 *
 * @param replication          out: handle.
 * @param archive              local backup archive.
 * @param aeron                aeron client.
 * @param src_control_stream_id  cluster archive control stream.
 * @param src_control_channel  cluster archive control channel.
 * @param replication_channel  channel to use for replicated data.
 * @param progress_timeout_ns   how long before declaring stalled.
 * @param progress_interval_ns  interval between progress checks.
 */
int aeron_cluster_snapshot_replication_create(
    aeron_cluster_snapshot_replication_t **replication,
    aeron_archive_t *archive,
    aeron_t *aeron,
    int32_t src_control_stream_id,
    const char *src_control_channel,
    const char *replication_channel,
    int64_t progress_timeout_ns,
    int64_t progress_interval_ns);

/** Add a snapshot entry to be replicated. */
int aeron_cluster_snapshot_replication_add_snapshot(
    aeron_cluster_snapshot_replication_t *replication,
    const aeron_cluster_recording_log_entry_t *snapshot);

/** Drive progress. Returns work count; -1 on fatal error. */
int aeron_cluster_snapshot_replication_poll(
    aeron_cluster_snapshot_replication_t *replication, int64_t now_ns);

/** Forward a recording signal to the active sub-replication. */
void aeron_cluster_snapshot_replication_on_signal(
    aeron_cluster_snapshot_replication_t *replication,
    const aeron_archive_recording_signal_t *signal);

/** True when all snapshots have been replicated. */
bool aeron_cluster_snapshot_replication_is_complete(
    const aeron_cluster_snapshot_replication_t *replication);

/**
 * Fill @p out_entries with the replicated snapshot entries (dst recording IDs).
 * @p out_count must initially hold the capacity of @p out_entries.
 * Returns number of entries written.
 */
int aeron_cluster_snapshot_replication_snapshots_retrieved(
    const aeron_cluster_snapshot_replication_t *replication,
    aeron_cluster_recording_log_entry_t *out_entries,
    int out_capacity);

/** Src recording ID currently being replicated, or AERON_NULL_VALUE. */
int64_t aeron_cluster_snapshot_replication_current_src_recording_id(
    const aeron_cluster_snapshot_replication_t *replication);

/**
 * Return a pointer to the pending snapshot entry currently being replicated,
 * or NULL if no replication is in progress.
 * Mirrors Java SnapshotReplication.currentSnapshot().
 */
const aeron_cluster_recording_log_entry_t *aeron_cluster_snapshot_replication_current_snapshot(
    const aeron_cluster_snapshot_replication_t *replication);

/** Replication ID of the current active replication, or AERON_NULL_VALUE. */
int64_t aeron_cluster_snapshot_replication_current_replication_id(
    const aeron_cluster_snapshot_replication_t *replication);

/** Close — stops any in-progress replication and frees memory. */
int aeron_cluster_snapshot_replication_close(
    aeron_cluster_snapshot_replication_t *replication);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_SNAPSHOT_REPLICATION_H */
