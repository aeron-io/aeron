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

#ifndef AERON_CLUSTER_RECORDING_REPLICATION_H
#define AERON_CLUSTER_RECORDING_REPLICATION_H

#include <stdint.h>
#include <stdbool.h>
#include "aeronc.h"
#include "aeron_archive.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Manages a single recording replication from a source archive to the local archive.
 * Mirrors Java's RecordingReplication.java.
 *
 * Lifecycle:
 *   create → on_signal (via archive signal consumer) + poll (progress) → close
 */
typedef struct aeron_cluster_recording_replication_stct
{
    aeron_archive_t    *archive;
    aeron_t            *aeron;

    int64_t             replication_id;
    int64_t             stop_position;
    int64_t             progress_check_timeout_ns;
    int64_t             progress_check_interval_ns;
    char                src_archive_channel[512];

    int32_t             recording_position_counter_id;
    int64_t             recording_id;
    int64_t             position;

    int64_t             progress_deadline_ns;
    int64_t             progress_check_deadline_ns;

    int32_t             last_recording_signal;   /* aeron_archive_client_recording_signal_t */

    bool                has_replication_ended;
    bool                has_synced;
    bool                has_stopped;
}
aeron_cluster_recording_replication_t;

/**
 * Create and start a recording replication.
 *
 * @param replication      out — newly allocated replication handle
 * @param archive          local archive client
 * @param aeron            aeron client (for counters reader)
 * @param src_recording_id recording ID at the source archive
 * @param src_control_channel  remote control channel of the source archive
 * @param src_control_stream_id remote control stream id of the source archive
 * @param params           replication parameters (stop_position, dst_recording_id, …)
 * @param progress_check_timeout_ns  timeout before declaring stalled
 * @param progress_check_interval_ns interval between position checks
 * @param now_ns           current epoch nanoseconds
 * @return 0 on success, -1 on error
 */
int aeron_cluster_recording_replication_create(
    aeron_cluster_recording_replication_t **replication,
    aeron_archive_t *archive,
    aeron_t *aeron,
    int64_t src_recording_id,
    const char *src_control_channel,
    int32_t src_control_stream_id,
    aeron_archive_replication_params_t *params,
    int64_t progress_check_timeout_ns,
    int64_t progress_check_interval_ns,
    int64_t now_ns);

/**
 * Stop the replication (best-effort) and free the handle.
 * Safe to call even if replication has already ended.
 */
int aeron_cluster_recording_replication_close(aeron_cluster_recording_replication_t *replication);

/**
 * Drive progress: check progress timeout and update position from counter.
 * Returns work count (> 0 if progress made). Returns -1 on fatal error (stalled).
 */
int aeron_cluster_recording_replication_poll(
    aeron_cluster_recording_replication_t *replication, int64_t now_ns);

/**
 * Handle a recording signal from the archive.  Call this from the archive
 * signal consumer whenever a signal arrives.  The signal is accepted only
 * when signal->control_session_id == replication->replication_id.
 */
void aeron_cluster_recording_replication_on_signal(
    aeron_cluster_recording_replication_t *replication,
    const aeron_archive_recording_signal_t *signal);

/* --- Accessors --- */
int64_t aeron_cluster_recording_replication_replication_id(
    const aeron_cluster_recording_replication_t *replication);

int64_t aeron_cluster_recording_replication_position(
    const aeron_cluster_recording_replication_t *replication);

int64_t aeron_cluster_recording_replication_recording_id(
    const aeron_cluster_recording_replication_t *replication);

bool aeron_cluster_recording_replication_has_replication_ended(
    const aeron_cluster_recording_replication_t *replication);

bool aeron_cluster_recording_replication_has_synced(
    const aeron_cluster_recording_replication_t *replication);

bool aeron_cluster_recording_replication_has_stopped(
    const aeron_cluster_recording_replication_t *replication);

const char *aeron_cluster_recording_replication_src_archive_channel(
    const aeron_cluster_recording_replication_t *replication);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_RECORDING_REPLICATION_H */
