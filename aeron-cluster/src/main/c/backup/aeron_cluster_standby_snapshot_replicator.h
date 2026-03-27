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

#ifndef AERON_CLUSTER_STANDBY_SNAPSHOT_REPLICATOR_H
#define AERON_CLUSTER_STANDBY_SNAPSHOT_REPLICATOR_H

#include <stdint.h>
#include <stdbool.h>
#include "aeronc.h"
#include "aeron_archive.h"
#include "consensus/aeron_cluster_recording_log.h"
#include "consensus/aeron_cluster_multi_recording_replication.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Replicates standby snapshot recordings for one endpoint at a time.
 * Mirrors Java's StandbySnapshotReplicator.
 *
 * A simplified C port: fetches STANDBY_SNAPSHOT entries from the recording log
 * and drives a MultipleRecordingReplication for the current endpoint.
 */
typedef struct aeron_cluster_standby_snapshot_entry_stct
{
    char    endpoint[256];
    int64_t log_position;
    /* Index range into the recording log entries for this endpoint */
    aeron_cluster_recording_log_entry_t *entries;
    int                                  entry_count;
}
aeron_cluster_standby_snapshot_entry_t;

typedef struct aeron_cluster_standby_snapshot_replicator_stct
{
    int32_t  member_id;
    aeron_archive_t *archive;
    aeron_t         *aeron;

    aeron_cluster_recording_log_t *recording_log;
    int                            service_count;

    char     archive_control_channel[512];
    int32_t  archive_control_stream_id;
    char     replication_channel[512];
    int      file_sync_level;

    /* Ordered list of endpoints to try (sorted by log_position descending) */
    aeron_cluster_standby_snapshot_entry_t *entries;
    int                                     entry_count;
    int                                     entry_capacity;

    int current_index;   /* index into entries[] currently being replicated */

    aeron_cluster_multi_recording_replication_t *recording_replication;
    bool is_complete;

    /* Per-endpoint error tracking — mirrors Java errorsByEndpoint */
    char     endpoint_errors[16][256];  /* up to 16 endpoints × 256 char error message */
    int      endpoint_error_count;

    /* Snapshot counter — incremented when replication for one endpoint completes */
    aeron_counter_t *snapshot_counter;  /* optional, may be NULL */

    /* Event listener for replication completion logging */
    void (*event_listener)(void *clientd, const char *control_uri,
                           int64_t src_recording_id, int64_t dst_recording_id,
                           int64_t position, bool has_synced);
    void  *event_listener_clientd;
}
aeron_cluster_standby_snapshot_replicator_t;

/**
 * Create a StandbySnapshotReplicator.
 *
 * Ownership of @p archive is transferred to the replicator and will be closed
 * on aeron_cluster_standby_snapshot_replicator_close().
 */
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
    int file_sync_level);

/** Drive progress. Returns work count; -1 on error. */
int aeron_cluster_standby_snapshot_replicator_poll(
    aeron_cluster_standby_snapshot_replicator_t *replicator, int64_t now_ns);

/** Forward a recording signal to the active sub-replication. */
void aeron_cluster_standby_snapshot_replicator_on_signal(
    aeron_cluster_standby_snapshot_replicator_t *replicator,
    const aeron_archive_recording_signal_t *signal);

/** True when all needed snapshots have been replicated. */
bool aeron_cluster_standby_snapshot_replicator_is_complete(
    const aeron_cluster_standby_snapshot_replicator_t *replicator);

/** Close and free all resources. */
void aeron_cluster_standby_snapshot_replicator_close(
    aeron_cluster_standby_snapshot_replicator_t *replicator);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_STANDBY_SNAPSHOT_REPLICATOR_H */
