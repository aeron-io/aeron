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

#ifndef AERON_CLUSTER_RECORDING_LOG_H
#define AERON_CLUSTER_RECORDING_LOG_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#include "aeron_consensus_module_configuration.h"

#ifdef __cplusplus
extern "C"
{
#endif

/* -----------------------------------------------------------------------
 * Binary layout of each 4096-byte entry in recording.log
 * (matches Java RecordingLog field offsets exactly)
 * ----------------------------------------------------------------------- */
#define AERON_CLUSTER_RECORDING_LOG_RECORDING_ID_OFFSET         0
#define AERON_CLUSTER_RECORDING_LOG_LEADERSHIP_TERM_ID_OFFSET   8
#define AERON_CLUSTER_RECORDING_LOG_TERM_BASE_LOG_POSITION_OFFSET 16
#define AERON_CLUSTER_RECORDING_LOG_LOG_POSITION_OFFSET         24
#define AERON_CLUSTER_RECORDING_LOG_TIMESTAMP_OFFSET            32
#define AERON_CLUSTER_RECORDING_LOG_SERVICE_ID_OFFSET           40
#define AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_OFFSET           44

typedef struct aeron_cluster_recording_log_entry_stct
{
    int64_t recording_id;
    int64_t leadership_term_id;
    int64_t term_base_log_position;
    int64_t log_position;           /* -1 = still open */
    int64_t timestamp;
    int32_t service_id;             /* -1 for term entries */
    int32_t entry_type;             /* AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_* */
    int     entry_index;            /* physical slot number (0-based) — used by sort comparator */
    bool    is_valid;               /* !( entry_type & INVALID_FLAG ) — cached for convenience */
}
aeron_cluster_recording_log_entry_t;

/* Recovery plan — built from the log on startup */
typedef struct aeron_cluster_recovery_plan_stct
{
    int64_t last_leadership_term_id;
    int64_t last_term_base_log_position;
    int64_t last_append_position;
    int64_t last_term_recording_id;
    int      snapshot_count;
    aeron_cluster_recording_log_entry_t *snapshots;  /* per-service, [0] = CM */
    aeron_cluster_recording_log_entry_t  log;
}
aeron_cluster_recovery_plan_t;

typedef struct aeron_cluster_recording_log_stct
{
    int      fd;                     /* open file descriptor */
    uint8_t *mapped;                 /* mmap'd file contents */
    size_t   mapped_length;
    int      entry_count;            /* physical slot count (includes invalid entries) */

    /**
     * Sorted in-memory view of entries, matching Java RecordingLog.entries().
     * Sorted by: leadershipTermId → termBaseLogPosition → type (TERM < STANDBY < SNAPSHOT)
     *            → logPosition → serviceId DESC.
     * Rebuilt on open() and reload().
     */
    aeron_cluster_recording_log_entry_t *sorted_entries;
    int                                   sorted_count;   /* count of ALL entries (valid + invalid) */
}
aeron_cluster_recording_log_t;

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */
int  aeron_cluster_recording_log_open(
    aeron_cluster_recording_log_t **log,
    const char *cluster_dir,
    bool create_new);

int  aeron_cluster_recording_log_close(aeron_cluster_recording_log_t *log);
int  aeron_cluster_recording_log_force(aeron_cluster_recording_log_t *log);

/**
 * Re-read all entries from disk and rebuild the sorted in-memory view.
 * Equivalent to Java's RecordingLog.reload().
 */
int  aeron_cluster_recording_log_reload(aeron_cluster_recording_log_t *log);

/* -----------------------------------------------------------------------
 * Writes
 * ----------------------------------------------------------------------- */
int  aeron_cluster_recording_log_append_term(
    aeron_cluster_recording_log_t *log,
    int64_t recording_id,
    int64_t leadership_term_id,
    int64_t term_base_log_position,
    int64_t timestamp);

int  aeron_cluster_recording_log_append_snapshot(
    aeron_cluster_recording_log_t *log,
    int64_t recording_id,
    int64_t leadership_term_id,
    int64_t term_base_log_position,
    int64_t log_position,
    int64_t timestamp,
    int32_t service_id);

int  aeron_cluster_recording_log_commit_log_position(
    aeron_cluster_recording_log_t *log,
    int64_t leadership_term_id,
    int64_t log_position);

/**
 * Invalidate the latest complete snapshot pair (CM + all services).
 * Returns 1 if anything was invalidated, 0 if no snapshot found, -1 on error.
 */
int  aeron_cluster_recording_log_invalidate_latest_snapshot(
    aeron_cluster_recording_log_t *log);

/**
 * Invalidate the entry at the given 0-based index.
 * Used in recovery when a mid-log snapshot is replaced.
 */
int  aeron_cluster_recording_log_invalidate_entry_at(
    aeron_cluster_recording_log_t *log, int index);

/* -----------------------------------------------------------------------
 * Reads / queries
 * ----------------------------------------------------------------------- */
/**
 * Get entry at 0-based index in the SORTED view.
 * Equivalent to Java's recordingLog.entries().get(index).
 * Returns NULL if out of range.
 */
aeron_cluster_recording_log_entry_t *aeron_cluster_recording_log_entry_at(
    aeron_cluster_recording_log_t *log, int index);

/** Check whether an entry is valid (INVALID_FLAG not set). */
static inline bool aeron_cluster_recording_log_entry_is_valid(
    const aeron_cluster_recording_log_entry_t *entry)
{
    return (entry->entry_type & AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG) == 0;
}

/** Return recording_id of the last valid TERM entry, or -1. */
int64_t aeron_cluster_recording_log_find_last_term_recording_id(
    aeron_cluster_recording_log_t *log);

/** Return timestamp of the term with given leadership_term_id, or -1. */
int64_t aeron_cluster_recording_log_get_term_timestamp(
    aeron_cluster_recording_log_t *log, int64_t leadership_term_id);

aeron_cluster_recording_log_entry_t *aeron_cluster_recording_log_find_last_term(
    aeron_cluster_recording_log_t *log);

aeron_cluster_recording_log_entry_t *aeron_cluster_recording_log_get_term_entry(
    aeron_cluster_recording_log_t *log, int64_t leadership_term_id);

aeron_cluster_recording_log_entry_t *aeron_cluster_recording_log_get_latest_snapshot(
    aeron_cluster_recording_log_t *log, int32_t service_id);

bool aeron_cluster_recording_log_is_unknown(
    aeron_cluster_recording_log_t *log, int64_t leadership_term_id);

/* -----------------------------------------------------------------------
 * Recovery plan
 * ----------------------------------------------------------------------- */
int aeron_cluster_recording_log_create_recovery_plan(
    aeron_cluster_recording_log_t *log,
    aeron_cluster_recovery_plan_t **plan,
    int service_count);

void aeron_cluster_recovery_plan_free(aeron_cluster_recovery_plan_t *plan);

/* Standby snapshot (ENTRY_TYPE_STANDBY_SNAPSHOT) */
int  aeron_cluster_recording_log_append_standby_snapshot(
    aeron_cluster_recording_log_t *log,
    int64_t recording_id,
    int64_t leadership_term_id,
    int64_t term_base_log_position,
    int64_t log_position,
    int64_t timestamp,
    int32_t service_id,
    const char *archive_endpoint);

/** Remove entry by leadershipTermId+index (marks invalid). */
int  aeron_cluster_recording_log_remove_entry(
    aeron_cluster_recording_log_t *log,
    int64_t leadership_term_id,
    int index);

/** Alias for commit_log_position. */
int  aeron_cluster_recording_log_commit_log_position_by_term(
    aeron_cluster_recording_log_t *log,
    int64_t leadership_term_id,
    int64_t log_position);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_RECORDING_LOG_H */
