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

#ifndef AERON_CLUSTER_MULTI_RECORDING_REPLICATION_H
#define AERON_CLUSTER_MULTI_RECORDING_REPLICATION_H

#include <stdint.h>
#include <stdbool.h>
#include "aeronc.h"
#include "aeron_archive.h"
#include "aeron_cluster_recording_replication.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * (srcRecordingId, dstRecordingId, stopPosition) tuple queued for replication.
 */
typedef struct aeron_cluster_recording_info_stct
{
    int64_t src_recording_id;
    int64_t dst_recording_id;
    int64_t stop_position;
}
aeron_cluster_recording_info_t;

/**
 * Completed entry: srcRecordingId → dstRecordingId.
 */
typedef struct aeron_cluster_recording_completed_stct
{
    int64_t src_recording_id;
    int64_t dst_recording_id;
}
aeron_cluster_recording_completed_t;

/**
 * Optional event listener — notified each time one replication ends.
 */
typedef void (*aeron_cluster_multi_recording_replication_event_func_t)(
    const char *src_archive_channel,
    int64_t src_recording_id,
    int64_t dst_recording_id,
    int64_t position,
    bool has_synced,
    void *clientd);

/**
 * Drives a sequence of RecordingReplications one at a time.
 * Mirrors Java's MultipleRecordingReplication.java.
 *
 * Lifecycle:
 *   create → add_recording (×N) → poll / on_signal (until is_complete) → close
 */
typedef struct aeron_cluster_multi_recording_replication_stct
{
    aeron_archive_t    *archive;
    aeron_t            *aeron;

    int32_t             src_control_stream_id;
    char                src_control_channel[512];
    char                replication_channel[512];
    char                src_response_channel[512];

    int64_t             progress_timeout_ns;
    int64_t             progress_interval_ns;

    /* Pending queue (dynamic array) */
    aeron_cluster_recording_info_t *recordings_pending;
    int                             recordings_pending_capacity;
    int                             recordings_pending_count;
    int                             recording_cursor;

    /* Completed table (dynamic array) */
    aeron_cluster_recording_completed_t *recordings_completed;
    int                                  recordings_completed_capacity;
    int                                  recordings_completed_count;

    /* Active replication (NULL when idle) */
    aeron_cluster_recording_replication_t *recording_replication;

    /* Optional event listener */
    aeron_cluster_multi_recording_replication_event_func_t on_replication_ended;
    void                                                   *event_listener_clientd;
}
aeron_cluster_multi_recording_replication_t;

/**
 * Create a MultipleRecordingReplication.
 * Pass NULL for src_response_channel if not needed.
 */
int aeron_cluster_multi_recording_replication_create(
    aeron_cluster_multi_recording_replication_t **multi,
    aeron_archive_t *archive,
    aeron_t *aeron,
    int32_t src_control_stream_id,
    const char *src_control_channel,
    const char *replication_channel,
    const char *src_response_channel,   /* may be NULL */
    int64_t progress_timeout_ns,
    int64_t progress_interval_ns);

/** Free all resources; stops any in-progress replication. */
int aeron_cluster_multi_recording_replication_close(
    aeron_cluster_multi_recording_replication_t *multi);

/** Enqueue a recording to replicate. */
int aeron_cluster_multi_recording_replication_add_recording(
    aeron_cluster_multi_recording_replication_t *multi,
    int64_t src_recording_id,
    int64_t dst_recording_id,
    int64_t stop_position);

/**
 * Drive progress. Starts replications, advances cursor on sync.
 * Returns work count; -1 on fatal error.
 */
int aeron_cluster_multi_recording_replication_poll(
    aeron_cluster_multi_recording_replication_t *multi, int64_t now_ns);

/**
 * Forward a recording signal to the active RecordingReplication.
 */
void aeron_cluster_multi_recording_replication_on_signal(
    aeron_cluster_multi_recording_replication_t *multi,
    const aeron_archive_recording_signal_t *signal);

/** True when all queued recordings have been replicated. */
bool aeron_cluster_multi_recording_replication_is_complete(
    const aeron_cluster_multi_recording_replication_t *multi);

/**
 * Return the dst_recording_id for a completed src_recording_id,
 * or AERON_NULL_VALUE if not found.
 */
int64_t aeron_cluster_multi_recording_replication_completed_dst(
    const aeron_cluster_multi_recording_replication_t *multi,
    int64_t src_recording_id);

/** Replication ID of the currently active RecordingReplication, or AERON_NULL_VALUE. */
int64_t aeron_cluster_multi_recording_replication_current_replication_id(
    const aeron_cluster_multi_recording_replication_t *multi);

/** Src recording ID being replicated right now, or AERON_NULL_VALUE. */
int64_t aeron_cluster_multi_recording_replication_current_src_recording_id(
    const aeron_cluster_multi_recording_replication_t *multi);

/** Set optional event listener called when each replication ends. */
void aeron_cluster_multi_recording_replication_set_event_listener(
    aeron_cluster_multi_recording_replication_t *multi,
    aeron_cluster_multi_recording_replication_event_func_t on_replication_ended,
    void *clientd);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_MULTI_RECORDING_REPLICATION_H */
