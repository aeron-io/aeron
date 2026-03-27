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

#if defined(__linux__)
#define _DEFAULT_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "aeronc.h"
#include "aeron_archive.h"
#include "aeron_cluster_recording_log.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_fileutil.h"

#define ENTRY_STRIDE AERON_CLUSTER_RECORDING_LOG_MAX_ENTRY_LENGTH

static void entry_write(uint8_t *slot, const aeron_cluster_recording_log_entry_t *e)
{
    memset(slot, 0, ENTRY_STRIDE);
    memcpy(slot + AERON_CLUSTER_RECORDING_LOG_RECORDING_ID_OFFSET,          &e->recording_id,          8);
    memcpy(slot + AERON_CLUSTER_RECORDING_LOG_LEADERSHIP_TERM_ID_OFFSET,    &e->leadership_term_id,    8);
    memcpy(slot + AERON_CLUSTER_RECORDING_LOG_TERM_BASE_LOG_POSITION_OFFSET, &e->term_base_log_position, 8);
    memcpy(slot + AERON_CLUSTER_RECORDING_LOG_LOG_POSITION_OFFSET,          &e->log_position,          8);
    memcpy(slot + AERON_CLUSTER_RECORDING_LOG_TIMESTAMP_OFFSET,             &e->timestamp,             8);
    memcpy(slot + AERON_CLUSTER_RECORDING_LOG_SERVICE_ID_OFFSET,            &e->service_id,            4);
    memcpy(slot + AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_OFFSET,            &e->entry_type,            4);
    /* Write archive endpoint as putStringAscii (int32 length + ASCII chars) */
    if (e->archive_endpoint[0] != '\0')
    {
        int32_t ep_len = (int32_t)strnlen(e->archive_endpoint,
            AERON_CLUSTER_RECORDING_LOG_MAX_ENDPOINT_LENGTH - 1);
        memcpy(slot + AERON_CLUSTER_RECORDING_LOG_ENDPOINT_LENGTH_OFFSET, &ep_len, 4);
        memcpy(slot + AERON_CLUSTER_RECORDING_LOG_ENDPOINT_OFFSET, e->archive_endpoint, (size_t)ep_len);
    }
}

static bool is_valid_entry(int32_t entry_type)
{
    return (entry_type & AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG) == 0;
}

static void entry_read(aeron_cluster_recording_log_entry_t *e, const uint8_t *slot)
{
    memcpy(&e->recording_id,           slot + AERON_CLUSTER_RECORDING_LOG_RECORDING_ID_OFFSET,          8);
    memcpy(&e->leadership_term_id,     slot + AERON_CLUSTER_RECORDING_LOG_LEADERSHIP_TERM_ID_OFFSET,    8);
    memcpy(&e->term_base_log_position, slot + AERON_CLUSTER_RECORDING_LOG_TERM_BASE_LOG_POSITION_OFFSET, 8);
    memcpy(&e->log_position,           slot + AERON_CLUSTER_RECORDING_LOG_LOG_POSITION_OFFSET,          8);
    memcpy(&e->timestamp,              slot + AERON_CLUSTER_RECORDING_LOG_TIMESTAMP_OFFSET,             8);
    memcpy(&e->service_id,             slot + AERON_CLUSTER_RECORDING_LOG_SERVICE_ID_OFFSET,            4);
    memcpy(&e->entry_type,             slot + AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_OFFSET,            4);
    e->is_valid    = is_valid_entry(e->entry_type);
    e->entry_index = -1;  /* set by caller */
    /* Read archive endpoint (putStringAscii: int32 length + chars) */
    e->archive_endpoint[0] = '\0';
    int32_t ep_len = 0;
    memcpy(&ep_len, slot + AERON_CLUSTER_RECORDING_LOG_ENDPOINT_LENGTH_OFFSET, 4);
    if (ep_len > 0 && ep_len < AERON_CLUSTER_RECORDING_LOG_MAX_ENDPOINT_LENGTH)
    {
        memcpy(e->archive_endpoint, slot + AERON_CLUSTER_RECORDING_LOG_ENDPOINT_OFFSET, (size_t)ep_len);
        e->archive_endpoint[ep_len] = '\0';
    }
}

/* -----------------------------------------------------------------------
 * Comparator — mirrors Java's ENTRY_COMPARATOR exactly:
 *   1. leadershipTermId ascending
 *   2. termBaseLogPosition ascending
 *   3. type: TERM(0) < STANDBY(2) < SNAPSHOT(1) — SNAPSHOT always last
 *   4. if both TERM: entryIndex (insertion order) ascending
 *   5. if both SNAPSHOT/STANDBY: logPosition ascending, then serviceId DESCENDING
 * ----------------------------------------------------------------------- */
static int entry_comparator(const void *a, const void *b)
{
    const aeron_cluster_recording_log_entry_t *e1 =
        (const aeron_cluster_recording_log_entry_t *)a;
    const aeron_cluster_recording_log_entry_t *e2 =
        (const aeron_cluster_recording_log_entry_t *)b;

    /* 1. leadershipTermId */
    if (e1->leadership_term_id != e2->leadership_term_id)
    {
        return (e1->leadership_term_id < e2->leadership_term_id) ? -1 : 1;
    }

    /* 2. termBaseLogPosition */
    if (e1->term_base_log_position != e2->term_base_log_position)
    {
        return (e1->term_base_log_position < e2->term_base_log_position) ? -1 : 1;
    }

    /* 3. type: if different, SNAPSHOT (1) is always last */
    int t1 = e1->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
    int t2 = e2->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
    if (t1 != t2)
    {
        if (t1 == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT) { return  1; }
        if (t2 == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT) { return -1; }
        return (t1 < t2) ? -1 : 1;
    }

    /* 4. same type */
    if (t1 == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM)
    {
        /* TERM: sort by entryIndex (insertion order) */
        return e1->entry_index - e2->entry_index;
    }

    /* SNAPSHOT / STANDBY: logPosition ascending */
    if (e1->log_position != e2->log_position)
    {
        return (e1->log_position < e2->log_position) ? -1 : 1;
    }

    /* 5. serviceId DESCENDING (Java: Integer.compare(e2.serviceId, e1.serviceId)) */
    if (e2->service_id != e1->service_id)
    {
        return (e2->service_id < e1->service_id) ? -1 : 1;
    }

    return 0;
}

/* Build / rebuild the sorted_entries array from the current mmap contents. */
static int recording_log_track_invalid_snapshot(aeron_cluster_recording_log_t *log, int slot_index)
{
    if (log->invalid_snapshot_count >= log->invalid_snapshot_capacity)
    {
        int new_cap = (log->invalid_snapshot_capacity == 0) ? 8 : log->invalid_snapshot_capacity * 2;
        if (aeron_reallocf((void **)&log->invalid_snapshot_slots,
            (size_t)new_cap * sizeof(int)) < 0)
        {
            return -1;
        }
        log->invalid_snapshot_capacity = new_cap;
    }
    log->invalid_snapshot_slots[log->invalid_snapshot_count++] = slot_index;
    return 0;
}

static int recording_log_sort(aeron_cluster_recording_log_t *log)
{
    if (log->sorted_entries)
    {
        aeron_free(log->sorted_entries);
        log->sorted_entries = NULL;
        log->sorted_count   = 0;
    }

    /* Reset invalid snapshot tracking on every sort (reload rebuilds from scratch). */
    aeron_free(log->invalid_snapshot_slots);
    log->invalid_snapshot_slots    = NULL;
    log->invalid_snapshot_count    = 0;
    log->invalid_snapshot_capacity = 0;

    if (log->entry_count == 0) { return 0; }

    if (aeron_alloc((void **)&log->sorted_entries,
        (size_t)log->entry_count * sizeof(aeron_cluster_recording_log_entry_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate sorted_entries");
        return -1;
    }

    for (int i = 0; i < log->entry_count; i++)
    {
        const uint8_t *slot = log->mapped + (size_t)i * ENTRY_STRIDE;
        entry_read(&log->sorted_entries[i], slot);
        log->sorted_entries[i].entry_index = i;
        log->sorted_entries[i].is_valid =
            is_valid_entry(log->sorted_entries[i].entry_type);

        /* Populate invalid snapshot list from disk */
        const int32_t base_type =
            log->sorted_entries[i].entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
        if (!log->sorted_entries[i].is_valid &&
            (base_type == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT ||
             base_type == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT))
        {
            recording_log_track_invalid_snapshot(log, i);
        }
    }

    qsort(log->sorted_entries, (size_t)log->entry_count,
          sizeof(aeron_cluster_recording_log_entry_t), entry_comparator);

    log->sorted_count = log->entry_count;
    return 0;
}

/* -----------------------------------------------------------------------
 * Grow the mapped file by one entry slot.
 * ----------------------------------------------------------------------- */
static int recording_log_grow(aeron_cluster_recording_log_t *log)
{
    const size_t new_len = (size_t)(log->entry_count + 1) * ENTRY_STRIDE;

    if (ftruncate(log->fd, (off_t)new_len) < 0)
    {
        AERON_SET_ERR(errno, "%s", "ftruncate recording.log");
        return -1;
    }

    if (NULL != log->mapped)
    {
        munmap(log->mapped, log->mapped_length);
    }

    log->mapped = mmap(NULL, new_len, PROT_READ | PROT_WRITE, MAP_SHARED, log->fd, 0);
    if (MAP_FAILED == log->mapped)
    {
        log->mapped = NULL;
        AERON_SET_ERR(errno, "%s", "mmap recording.log");
        return -1;
    }

    log->mapped_length = new_len;
    /* sorted_entries built by recording_log_sort() */
    return 0;
}

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */
int aeron_cluster_recording_log_open(
    aeron_cluster_recording_log_t **log,
    const char *cluster_dir,
    bool create_new)
{
    aeron_cluster_recording_log_t *_log = NULL;
    if (aeron_alloc((void **)&_log, sizeof(aeron_cluster_recording_log_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate recording log");
        return -1;
    }

    char path[4096];
    snprintf(path, sizeof(path), "%s/%s", cluster_dir, AERON_CLUSTER_RECORDING_LOG_FILE_NAME);

    int flags = O_RDWR | (create_new ? O_CREAT | O_TRUNC : 0);
    _log->fd = open(path, flags, 0644);
    if (_log->fd < 0)
    {
        AERON_SET_ERR(errno, "open recording.log: %s", path);
        aeron_free(_log);
        return -1;
    }

    struct stat st;
    if (fstat(_log->fd, &st) < 0)
    {
        AERON_SET_ERR(errno, "%s", "fstat recording.log");
        close(_log->fd);
        aeron_free(_log);
        return -1;
    }

    _log->entry_count  = (int)(st.st_size / ENTRY_STRIDE);
    _log->mapped_length = (size_t)st.st_size;
    _log->mapped       = NULL;
    _log->sorted_entries = NULL;
    _log->sorted_count   = 0;
    _log->invalid_snapshot_slots    = NULL;
    _log->invalid_snapshot_count    = 0;
    _log->invalid_snapshot_capacity = 0;

    if (st.st_size > 0)
    {
        _log->mapped = mmap(NULL, (size_t)st.st_size,
            PROT_READ | PROT_WRITE, MAP_SHARED, _log->fd, 0);
        if (MAP_FAILED == _log->mapped)
        {
            AERON_SET_ERR(errno, "%s", "mmap recording.log (existing)");
            close(_log->fd);
            aeron_free(_log);
            return -1;
        }
        /* sorted_entries built by recording_log_sort() */
    }

    *log = _log;

    /* Build sorted view on open */
    if (recording_log_sort(_log) < 0)
    {
        aeron_cluster_recording_log_close(_log);
        return -1;
    }

    return 0;
}

int aeron_cluster_recording_log_close(aeron_cluster_recording_log_t *log)
{
    if (NULL != log)
    {
        aeron_free(log->sorted_entries);
        aeron_free(log->invalid_snapshot_slots);
        if (NULL != log->mapped)
        {
            munmap(log->mapped, log->mapped_length);
        }
        if (log->fd >= 0)
        {
            close(log->fd);
        }
        aeron_free(log);
    }
    return 0;
}

int aeron_cluster_recording_log_reload(aeron_cluster_recording_log_t *log)
{
    /* Re-read physical entry count from file */
    struct stat st;
    if (fstat(log->fd, &st) < 0) { return -1; }
    log->entry_count = (int)(st.st_size / ENTRY_STRIDE);
    return recording_log_sort(log);
}

int aeron_cluster_recording_log_force(aeron_cluster_recording_log_t *log)
{
    if (NULL != log->mapped)
    {
        msync(log->mapped, log->mapped_length, MS_SYNC);
    }
    return 0;
}

/* -----------------------------------------------------------------------
 * Internal: try to reuse an invalidated snapshot slot.
 * Mirrors Java RecordingLog.restoreInvalidSnapshot().
 * Returns true if a slot was reused (slot written and sorted_entries updated),
 * false if no matching slot was found.
 * ----------------------------------------------------------------------- */
static bool restore_invalid_snapshot(
    aeron_cluster_recording_log_t *log,
    int32_t snapshot_type,
    int64_t recording_id,
    int64_t leadership_term_id,
    int64_t term_base_log_position,
    int64_t log_position,
    int32_t service_id,
    const char *archive_endpoint)
{
    for (int i = log->invalid_snapshot_count - 1; i >= 0; i--)
    {
        int slot_index = log->invalid_snapshot_slots[i];
        const uint8_t *slot = log->mapped + (size_t)slot_index * ENTRY_STRIDE;

        aeron_cluster_recording_log_entry_t candidate;
        entry_read(&candidate, slot);

        /* base_type must match (SNAPSHOT or STANDBY_SNAPSHOT) */
        int32_t base_type = candidate.entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
        if (base_type != snapshot_type) { continue; }

        /* All key fields must match */
        if (candidate.leadership_term_id    != leadership_term_id    ||
            candidate.term_base_log_position != term_base_log_position ||
            candidate.log_position           != log_position           ||
            candidate.service_id             != service_id)
        {
            continue;
        }

        /* Found a reusable slot — write updated entry (same physical slot) */
        aeron_cluster_recording_log_entry_t restored = candidate;
        restored.recording_id = recording_id;
        restored.entry_type   = snapshot_type;  /* clear invalid flag */
        restored.is_valid     = true;
        if (NULL != archive_endpoint && '\0' != archive_endpoint[0])
        {
            snprintf(restored.archive_endpoint, sizeof(restored.archive_endpoint),
                "%s", archive_endpoint);
        }

        uint8_t *writable_slot = log->mapped + (size_t)slot_index * ENTRY_STRIDE;
        entry_write(writable_slot, &restored);
        msync(writable_slot, ENTRY_STRIDE, MS_SYNC);

        /* Remove from invalid list (swap with last) */
        log->invalid_snapshot_slots[i] =
            log->invalid_snapshot_slots[--log->invalid_snapshot_count];

        /* Rebuild sorted view to reflect restored entry */
        recording_log_sort(log);
        return true;
    }
    return false;
}

/* -----------------------------------------------------------------------
 * Internal append
 * ----------------------------------------------------------------------- */
static int recording_log_append(aeron_cluster_recording_log_t *log,
                                const aeron_cluster_recording_log_entry_t *entry)
{
    if (recording_log_grow(log) < 0)
    {
        return -1;
    }

    uint8_t *slot = log->mapped + (size_t)log->entry_count * ENTRY_STRIDE;
    entry_write(slot, entry);
    log->entry_count++;
    msync(slot, ENTRY_STRIDE, MS_SYNC);
    return recording_log_sort(log);  /* rebuild sorted view after append */
}

/* -----------------------------------------------------------------------
 * Writes
 * ----------------------------------------------------------------------- */
int aeron_cluster_recording_log_append_term(
    aeron_cluster_recording_log_t *log,
    int64_t recording_id,
    int64_t leadership_term_id,
    int64_t term_base_log_position,
    int64_t timestamp)
{
    if (leadership_term_id < 0)
    {
        AERON_SET_ERR(EINVAL, "leadership_term_id must be >= 0, got: %lld",
            (long long)leadership_term_id);
        return -1;
    }

    if (recording_id == -1LL)
    {
        AERON_SET_ERR(EINVAL, "invalid recordingId=%lld", (long long)recording_id);
        return -1;
    }

    /* Enforce non-decreasing recording_id across TERM entries */
    int64_t first_rec = aeron_cluster_recording_log_find_last_term_recording_id(log);
    if (first_rec >= 0 && recording_id < first_rec)
    {
        AERON_SET_ERR(EINVAL, "invalid TERM recordingId=%lld, expected recordingId>=%lld",
            (long long)recording_id, (long long)first_rec);
        return -1;
    }

    /* Reject duplicate valid TERM for same leadershipTermId */
    for (int i = 0; i < log->entry_count; i++)
    {
        const uint8_t *slot = log->mapped + (size_t)i * ENTRY_STRIDE;
        int64_t stored_id; int32_t entry_type;
        memcpy(&stored_id,  slot + AERON_CLUSTER_RECORDING_LOG_LEADERSHIP_TERM_ID_OFFSET, 8);
        memcpy(&entry_type, slot + AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_OFFSET, 4);
        if (is_valid_entry(entry_type) &&
            entry_type == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM &&
            stored_id == leadership_term_id)
        {
            AERON_SET_ERR(EINVAL, "duplicate TERM entry for leadershipTermId=%lld",
                (long long)leadership_term_id);
            return -1;
        }
    }

    /* Java: commit previous term's log_position, and find next term to set ours */
    int64_t log_position = -1; /* NULL_POSITION — open */

    if (log->entry_count > 0)
    {
        /* Commit the previous term if it exists */
        aeron_cluster_recording_log_entry_t *prev =
            aeron_cluster_recording_log_get_term_entry(log, leadership_term_id - 1);
        if (NULL != prev)
        {
            aeron_cluster_recording_log_commit_log_position_by_term(
                log, leadership_term_id - 1, term_base_log_position);
        }

        /* If a later term already exists (replay re-ordering), use its base as our close */
        aeron_cluster_recording_log_entry_t *next =
            aeron_cluster_recording_log_get_term_entry(log, leadership_term_id + 1);
        if (NULL != next)
        {
            log_position = next->term_base_log_position;
        }
    }

    aeron_cluster_recording_log_entry_t e = {
        .recording_id          = recording_id,
        .leadership_term_id    = leadership_term_id,
        .term_base_log_position = term_base_log_position,
        .log_position          = log_position,
        .timestamp             = timestamp,
        .service_id            = -1,
        .entry_type            = AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM,
    };
    return recording_log_append(log, &e);
}

int aeron_cluster_recording_log_append_snapshot(
    aeron_cluster_recording_log_t *log,
    int64_t recording_id,
    int64_t leadership_term_id,
    int64_t term_base_log_position,
    int64_t log_position,
    int64_t timestamp,
    int32_t service_id)
{
    if (recording_id == -1LL)
    {
        AERON_SET_ERR(EINVAL, "invalid recordingId=%lld", (long long)recording_id);
        return -1;
    }

    /* Try to reuse an invalidated snapshot slot first (mirrors Java restoreInvalidSnapshot). */
    if (restore_invalid_snapshot(log,
        AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT,
        recording_id, leadership_term_id, term_base_log_position,
        log_position, service_id, NULL))
    {
        return 0;
    }

    aeron_cluster_recording_log_entry_t e = {
        .recording_id          = recording_id,
        .leadership_term_id    = leadership_term_id,
        .term_base_log_position = term_base_log_position,
        .log_position          = log_position,
        .timestamp             = timestamp,
        .service_id            = service_id,
        .entry_type            = AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT,
    };
    return recording_log_append(log, &e);
}

int aeron_cluster_recording_log_commit_log_position(
    aeron_cluster_recording_log_t *log,
    int64_t leadership_term_id,
    int64_t log_position)
{
    for (int i = log->entry_count - 1; i >= 0; i--)
    {
        uint8_t *slot = log->mapped + (size_t)i * ENTRY_STRIDE;
        int64_t stored_term_id;
        int32_t entry_type;
        memcpy(&stored_term_id, slot + AERON_CLUSTER_RECORDING_LOG_LEADERSHIP_TERM_ID_OFFSET, 8);
        memcpy(&entry_type,     slot + AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_OFFSET, 4);

        if (stored_term_id == leadership_term_id &&
            entry_type == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM)
        {
            memcpy(slot + AERON_CLUSTER_RECORDING_LOG_LOG_POSITION_OFFSET, &log_position, 8);
            msync(slot + AERON_CLUSTER_RECORDING_LOG_LOG_POSITION_OFFSET, 8, MS_SYNC);

            /* Keep sorted_entries in sync */
            for (int j = 0; j < log->sorted_count; j++)
            {
                if (log->sorted_entries[j].entry_index == i)
                {
                    log->sorted_entries[j].log_position = log_position;
                    break;
                }
            }
            return 0;
        }
    }
    AERON_SET_ERR(EINVAL, "no term entry for leadershipTermId=%lld", (long long)leadership_term_id);
    return -1;
}

int aeron_cluster_recording_log_invalidate_latest_snapshot(aeron_cluster_recording_log_t *log)
{
    /* Find the log_position of the latest valid snapshot group */
    int64_t latest_log_position = -1;
    for (int i = log->entry_count - 1; i >= 0; i--)
    {
        uint8_t *slot = log->mapped + (size_t)i * ENTRY_STRIDE;
        int32_t entry_type; int64_t log_pos;
        memcpy(&entry_type, slot + AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_OFFSET, 4);
        memcpy(&log_pos,    slot + AERON_CLUSTER_RECORDING_LOG_LOG_POSITION_OFFSET, 8);

        if (is_valid_entry(entry_type) &&
            entry_type == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT)
        {
            latest_log_position = log_pos;
            break;
        }
    }

    if (latest_log_position < 0) { return 0; }  /* nothing to invalidate */

    /* Verify there is a parent TERM entry for this snapshot group */
    bool has_parent_term = false;
    for (int i = 0; i < log->entry_count; i++)
    {
        uint8_t *slot = log->mapped + (size_t)i * ENTRY_STRIDE;
        int32_t entry_type;
        memcpy(&entry_type, slot + AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_OFFSET, 4);
        if (is_valid_entry(entry_type) &&
            entry_type == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM)
        {
            has_parent_term = true;
            break;
        }
    }

    if (!has_parent_term)
    {
        AERON_SET_ERR(EINVAL, "%s", "no matching term for snapshot");
        return -1;
    }

    /* Invalidate ALL valid snapshots at that log_position */
    int count = 0;
    for (int i = 0; i < log->entry_count; i++)
    {
        uint8_t *slot = log->mapped + (size_t)i * ENTRY_STRIDE;
        int32_t entry_type; int64_t log_pos;
        memcpy(&entry_type, slot + AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_OFFSET, 4);
        memcpy(&log_pos,    slot + AERON_CLUSTER_RECORDING_LOG_LOG_POSITION_OFFSET, 8);

        if (is_valid_entry(entry_type) &&
            entry_type == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT &&
            log_pos == latest_log_position)
        {
            int32_t invalid = entry_type | AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
            memcpy(slot + AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_OFFSET, &invalid, 4);
            msync(slot + AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_OFFSET, 4, MS_SYNC);
            recording_log_track_invalid_snapshot(log, i);
            count++;
        }
    }
    if (count > 0) { recording_log_sort(log); }
    return (count > 0) ? 1 : 0;
}

int aeron_cluster_recording_log_invalidate_entry_at(
    aeron_cluster_recording_log_t *log, int index)
{
    if (index < 0 || index >= log->entry_count) { return -1; }
    uint8_t *slot = log->mapped + (size_t)index * ENTRY_STRIDE;
    int32_t entry_type;
    memcpy(&entry_type, slot + AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_OFFSET, 4);
    int32_t invalid = entry_type | AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
    memcpy(slot + AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_OFFSET, &invalid, 4);
    msync(slot + AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_OFFSET, 4, MS_SYNC);

    /* Track as an invalid snapshot slot for potential reuse */
    int32_t base_type = entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
    if (base_type == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT ||
        base_type == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT)
    {
        recording_log_track_invalid_snapshot(log, index);
    }

    return recording_log_sort(log);  /* rebuild sorted view */
}

/* -----------------------------------------------------------------------
 * Queries
 * ----------------------------------------------------------------------- */
aeron_cluster_recording_log_entry_t *aeron_cluster_recording_log_find_last_term(
    aeron_cluster_recording_log_t *log)
{
    /* Scan sorted_entries in reverse — last valid TERM after sort */
    if (NULL == log->sorted_entries) { return NULL; }
    for (int i = log->sorted_count - 1; i >= 0; i--)
    {
        aeron_cluster_recording_log_entry_t *e = &log->sorted_entries[i];
        if (is_valid_entry(e->entry_type) &&
            (e->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG)
                == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM)
        {
            return e;
        }
    }
    return NULL;
}

aeron_cluster_recording_log_entry_t *aeron_cluster_recording_log_get_term_entry(
    aeron_cluster_recording_log_t *log, int64_t leadership_term_id)
{
    if (NULL == log->sorted_entries) { return NULL; }
    for (int i = 0; i < log->sorted_count; i++)
    {
        aeron_cluster_recording_log_entry_t *e = &log->sorted_entries[i];
        int32_t base_type = e->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
        if (is_valid_entry(e->entry_type) &&
            base_type == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM &&
            e->leadership_term_id == leadership_term_id)
        {
            return e;
        }
    }
    return NULL;
}

aeron_cluster_recording_log_entry_t *aeron_cluster_recording_log_get_latest_snapshot(
    aeron_cluster_recording_log_t *log, int32_t service_id)
{
    /* Scan sorted_entries in reverse — latest valid SNAPSHOT for service_id */
    if (NULL == log->sorted_entries) { return NULL; }
    for (int i = log->sorted_count - 1; i >= 0; i--)
    {
        aeron_cluster_recording_log_entry_t *e = &log->sorted_entries[i];
        int32_t base_type = e->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
        if (is_valid_entry(e->entry_type) &&
            base_type == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT &&
            e->service_id == service_id)
        {
            return e;
        }
    }
    return NULL;
}

bool aeron_cluster_recording_log_is_unknown(
    aeron_cluster_recording_log_t *log, int64_t leadership_term_id)
{
    return NULL == aeron_cluster_recording_log_get_term_entry(log, leadership_term_id);
}

aeron_cluster_recording_log_entry_t *aeron_cluster_recording_log_find_term_entry(
    aeron_cluster_recording_log_t *log, int64_t leadership_term_id)
{
    /* Identical to get_term_entry — both return NULL in C (no exception semantics). */
    return aeron_cluster_recording_log_get_term_entry(log, leadership_term_id);
}

/* -----------------------------------------------------------------------
 * Recovery plan
 * ----------------------------------------------------------------------- */
int aeron_cluster_recording_log_create_recovery_plan(
    aeron_cluster_recording_log_t *log,
    aeron_cluster_recovery_plan_t **plan,
    int service_count,
    aeron_archive_t *archive)
{
    aeron_cluster_recovery_plan_t *p = NULL;
    if (aeron_alloc((void **)&p, sizeof(aeron_cluster_recovery_plan_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate recovery plan");
        return -1;
    }

    /* Total snapshot slots: 1 CM + service_count services */
    const int snap_slots = service_count + 1;
    if (aeron_alloc((void **)&p->snapshots,
        (size_t)snap_slots * sizeof(aeron_cluster_recording_log_entry_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate recovery plan snapshots");
        aeron_free(p);
        return -1;
    }
    memset(p->snapshots, 0xFF, (size_t)snap_slots * sizeof(aeron_cluster_recording_log_entry_t));
    p->snapshot_count = 0;

    /* Find the latest snapshot for each service (-1 = CM) */
    for (int svc = -1; svc < service_count; svc++)
    {
        aeron_cluster_recording_log_entry_t *snap =
            aeron_cluster_recording_log_get_latest_snapshot(log, svc);
        if (NULL != snap)
        {
            p->snapshots[p->snapshot_count++] = *snap;
        }
    }

    /* Find the last term */
    aeron_cluster_recording_log_entry_t *last_term =
        aeron_cluster_recording_log_find_last_term(log);

    if (NULL != last_term)
    {
        p->log                      = *last_term;
        p->last_leadership_term_id  = last_term->leadership_term_id;
        p->last_term_base_log_position = last_term->term_base_log_position;
        p->last_term_recording_id   = last_term->recording_id;

        /* Query archive for the actual stop position of the log recording.
         * Java RecordingLog.planRecovery() does: archive.listRecording(entry.recordingId, extent)
         * and uses extent.stopPosition as appendedLogPosition. */
        int64_t stop_position = last_term->log_position;  /* fallback: committed position */
        if (NULL != archive && last_term->recording_id != AERON_NULL_VALUE)
        {
            int64_t queried_stop = 0;
            if (aeron_archive_get_stop_position(&queried_stop, archive, last_term->recording_id) == 0 &&
                queried_stop != AERON_NULL_VALUE && queried_stop > stop_position)
            {
                stop_position = queried_stop;
            }
        }
        p->last_append_position = stop_position;
    }
    else
    {
        memset(&p->log, 0xFF, sizeof(p->log));
        p->last_leadership_term_id  = -1;
        p->last_term_base_log_position = 0;
        p->last_append_position     = 0;
        p->last_term_recording_id   = -1;
    }

    *plan = p;
    return 0;
}

void aeron_cluster_recovery_plan_free(aeron_cluster_recovery_plan_t *plan)
{
    if (NULL != plan)
    {
        aeron_free(plan->snapshots);
        aeron_free(plan);
    }
}

aeron_cluster_recording_log_entry_t *aeron_cluster_recording_log_entry_at(
    aeron_cluster_recording_log_t *log, int index)
{
    if (index < 0 || index >= log->sorted_count || NULL == log->sorted_entries)
    {
        return NULL;
    }
    return &log->sorted_entries[index];
}

int64_t aeron_cluster_recording_log_find_last_term_recording_id(
    aeron_cluster_recording_log_t *log)
{
    aeron_cluster_recording_log_entry_t *e = aeron_cluster_recording_log_find_last_term(log);
    return (e != NULL) ? e->recording_id : -1;
}

int64_t aeron_cluster_recording_log_get_term_timestamp(
    aeron_cluster_recording_log_t *log, int64_t leadership_term_id)
{
    aeron_cluster_recording_log_entry_t *e =
        aeron_cluster_recording_log_get_term_entry(log, leadership_term_id);
    return (e != NULL) ? e->timestamp : -1;
}

int aeron_cluster_recording_log_append_standby_snapshot(
    aeron_cluster_recording_log_t *log,
    int64_t recording_id,
    int64_t leadership_term_id,
    int64_t term_base_log_position,
    int64_t log_position,
    int64_t timestamp,
    int32_t service_id,
    const char *archive_endpoint)
{
    if (recording_id < 0)
    {
        AERON_SET_ERR(EINVAL, "invalid recordingId=%lld", (long long)recording_id);
        return -1;
    }

    /* Try to reuse an invalidated standby snapshot slot first. */
    if (restore_invalid_snapshot(log,
        AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT,
        recording_id, leadership_term_id, term_base_log_position,
        log_position, service_id, archive_endpoint))
    {
        return 0;
    }

    aeron_cluster_recording_log_entry_t e = {
        .recording_id           = recording_id,
        .leadership_term_id     = leadership_term_id,
        .term_base_log_position  = term_base_log_position,
        .log_position            = log_position,
        .timestamp               = timestamp,
        .service_id              = service_id,
        .entry_type              = AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT,
    };
    if (NULL != archive_endpoint && archive_endpoint[0] != '\0')
    {
        snprintf(e.archive_endpoint, sizeof(e.archive_endpoint), "%s", archive_endpoint);
    }
    return recording_log_append(log, &e);
}

int aeron_cluster_recording_log_remove_entry(
    aeron_cluster_recording_log_t *log,
    int64_t leadership_term_id,
    int index)
{
    return aeron_cluster_recording_log_invalidate_entry_at(log, index);
}

int aeron_cluster_recording_log_commit_log_position_by_term(
    aeron_cluster_recording_log_t *log,
    int64_t leadership_term_id,
    int64_t log_position)
{
    return aeron_cluster_recording_log_commit_log_position(log, leadership_term_id, log_position);
}

int aeron_cluster_recording_log_find_snapshots_at_or_before(
    aeron_cluster_recording_log_t *log,
    int64_t log_position,
    int service_count,
    aeron_cluster_recording_log_entry_t *out_snapshots,
    int *out_count)
{
    /* service IDs: -1 (CM), 0 .. service_count-1 */
    const int slots = service_count + 1;
    *out_count = 0;

    if (NULL == log->sorted_entries || slots <= 0)
    {
        return 0;
    }

    for (int svc_idx = 0; svc_idx < slots; svc_idx++)
    {
        int32_t svc_id = (int32_t)(svc_idx - 1);   /* -1, 0, 1, ... */

        /* Best "at or before" — highest log_position <= target */
        aeron_cluster_recording_log_entry_t *at_or_before = NULL;
        /* Fallback "lowest" — lowest log_position regardless */
        aeron_cluster_recording_log_entry_t *lowest = NULL;

        for (int i = 0; i < log->sorted_count; i++)
        {
            aeron_cluster_recording_log_entry_t *e = &log->sorted_entries[i];
            int32_t base_type = e->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;

            if (!is_valid_entry(e->entry_type) ||
                base_type != AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT ||
                e->service_id != svc_id)
            {
                continue;
            }

            /* Track lowest (fallback) */
            if (NULL == lowest || e->log_position < lowest->log_position)
            {
                lowest = e;
            }

            /* Track latest at or before log_position */
            if (e->log_position <= log_position)
            {
                if (NULL == at_or_before || e->log_position > at_or_before->log_position)
                {
                    at_or_before = e;
                }
            }
        }

        aeron_cluster_recording_log_entry_t *chosen = (at_or_before != NULL) ? at_or_before : lowest;
        if (NULL != chosen)
        {
            out_snapshots[(*out_count)++] = *chosen;
        }
    }

    return 0;
}

int aeron_cluster_recording_log_latest_standby_snapshots(
    aeron_cluster_recording_log_t *log,
    int service_count,
    aeron_cluster_recording_log_entry_t **out_snapshots,
    int *out_count)
{
    *out_snapshots = NULL;
    *out_count = 0;

    if (NULL == log->sorted_entries || log->sorted_count == 0)
    {
        return 0;
    }

    /*
     * Java logic: group by endpoint, then for each endpoint find the latest
     * log_position where there are exactly (service_count + 1) entries, and
     * include those entries in the result.
     *
     * We implement this in three passes:
     *  Pass 1: collect distinct (endpoint, log_position) pairs and count entries
     *  Pass 2: for each endpoint, find the latest log_position with a complete set
     *  Pass 3: copy matching entries into the output array
     */

    const int required = service_count + 1; /* CM entry + one per service */

    /* -- Pass 1: enumerate distinct (endpoint, log_position) groups --------- */
    /* Upper bound: total standby snapshot entries */
    int standby_total = 0;
    for (int i = 0; i < log->sorted_count; i++)
    {
        const aeron_cluster_recording_log_entry_t *e = &log->sorted_entries[i];
        int32_t btype = e->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
        if (e->is_valid && btype == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT)
        {
            standby_total++;
        }
    }
    if (standby_total == 0)
    {
        return 0;
    }

    /* Dynamic array of (endpoint, log_position, count) groups */
    typedef struct { char ep[AERON_CLUSTER_RECORDING_LOG_MAX_ENDPOINT_LENGTH]; int64_t pos; int cnt; } ep_pos_t;
    ep_pos_t *groups = NULL;
    int group_count = 0;
    if (aeron_alloc((void **)&groups, (size_t)standby_total * sizeof(ep_pos_t)) < 0)
    {
        return -1;
    }

    for (int i = 0; i < log->sorted_count; i++)
    {
        const aeron_cluster_recording_log_entry_t *e = &log->sorted_entries[i];
        int32_t btype = e->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
        if (!e->is_valid || btype != AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT)
        {
            continue;
        }

        /* Find or create a (endpoint, log_position) group */
        int g = -1;
        for (int k = 0; k < group_count; k++)
        {
            if (groups[k].pos == e->log_position &&
                strcmp(groups[k].ep, e->archive_endpoint) == 0)
            {
                g = k;
                break;
            }
        }
        if (g < 0)
        {
            g = group_count++;
            snprintf(groups[g].ep, sizeof(groups[g].ep), "%s", e->archive_endpoint);
            groups[g].pos = e->log_position;
            groups[g].cnt = 0;
        }
        groups[g].cnt++;
    }

    /* -- Pass 2: for each endpoint find the latest complete log_position ------ */
    /* Collect distinct endpoints */
    char (*eps)[AERON_CLUSTER_RECORDING_LOG_MAX_ENDPOINT_LENGTH] = NULL;
    int ep_count = 0;
    if (aeron_alloc((void **)&eps, (size_t)standby_total *
        AERON_CLUSTER_RECORDING_LOG_MAX_ENDPOINT_LENGTH) < 0)
    {
        aeron_free(groups);
        return -1;
    }
    for (int g = 0; g < group_count; g++)
    {
        bool found = false;
        for (int k = 0; k < ep_count; k++)
        {
            if (strcmp(eps[k], groups[g].ep) == 0) { found = true; break; }
        }
        if (!found)
        {
            snprintf(eps[ep_count++], AERON_CLUSTER_RECORDING_LOG_MAX_ENDPOINT_LENGTH,
                "%s", groups[g].ep);
        }
    }

    /* For each endpoint, pick the latest log_position where cnt == required */
    typedef struct { char ep[AERON_CLUSTER_RECORDING_LOG_MAX_ENDPOINT_LENGTH]; int64_t pos; } best_t;
    best_t *bests = NULL;
    int best_count = 0;
    if (aeron_alloc((void **)&bests, (size_t)ep_count * sizeof(best_t)) < 0)
    {
        aeron_free(groups); aeron_free(eps);
        return -1;
    }

    for (int ei = 0; ei < ep_count; ei++)
    {
        int64_t best_pos = INT64_MIN;
        for (int g = 0; g < group_count; g++)
        {
            if (strcmp(groups[g].ep, eps[ei]) == 0 &&
                groups[g].cnt == required &&
                groups[g].pos > best_pos)
            {
                best_pos = groups[g].pos;
            }
        }
        if (best_pos != INT64_MIN)
        {
            snprintf(bests[best_count].ep,
                sizeof(bests[best_count].ep), "%s", eps[ei]);
            bests[best_count].pos = best_pos;
            best_count++;
        }
    }

    aeron_free(groups);
    aeron_free(eps);

    if (best_count == 0)
    {
        aeron_free(bests);
        return 0;
    }

    /* -- Pass 3: collect entries matching the best (endpoint, log_position) --- */
    int total_entries = best_count * required;
    aeron_cluster_recording_log_entry_t *result = NULL;
    if (aeron_alloc((void **)&result,
        (size_t)total_entries * sizeof(aeron_cluster_recording_log_entry_t)) < 0)
    {
        aeron_free(bests);
        return -1;
    }

    int out_idx = 0;
    for (int bi = 0; bi < best_count; bi++)
    {
        for (int i = 0; i < log->sorted_count; i++)
        {
            const aeron_cluster_recording_log_entry_t *e = &log->sorted_entries[i];
            int32_t btype = e->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
            if (!e->is_valid || btype != AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT)
            {
                continue;
            }
            if (e->log_position == bests[bi].pos &&
                strcmp(e->archive_endpoint, bests[bi].ep) == 0)
            {
                result[out_idx++] = *e;
                if (out_idx - bi * required >= required)
                {
                    break;  /* collected the full set for this endpoint */
                }
            }
        }
    }

    aeron_free(bests);

    *out_snapshots = result;
    *out_count = out_idx;
    return 0;
}

int aeron_cluster_recording_log_ensure_coherent(
    aeron_cluster_recording_log_t *log,
    int64_t recording_id,
    int64_t initial_log_leadership_term_id,
    int64_t initial_term_base_log_position,
    int64_t leadership_term_id,
    int64_t term_base_log_position,
    int64_t log_position,
    int64_t timestamp)
{
    aeron_cluster_recording_log_entry_t *last_term = aeron_cluster_recording_log_find_last_term(log);

    if (NULL == last_term)
    {
        /* Empty log: append placeholder terms from initial to target */
        int64_t start_id = initial_log_leadership_term_id > 0 ? initial_log_leadership_term_id : 0;
        for (int64_t term_id = start_id; term_id < leadership_term_id; term_id++)
        {
            if (aeron_cluster_recording_log_append_term(
                log, recording_id, term_id, initial_term_base_log_position, timestamp) < 0)
            {
                return -1;
            }
        }
        if (aeron_cluster_recording_log_append_term(
            log, recording_id, leadership_term_id, term_base_log_position, timestamp) < 0)
        {
            return -1;
        }
        if (-1 != log_position)
        {
            if (aeron_cluster_recording_log_commit_log_position(
                log, leadership_term_id, log_position) < 0)
            {
                return -1;
            }
        }
    }
    else if (last_term->leadership_term_id < leadership_term_id)
    {
        /* Commit the prior open term if needed */
        if (-1 == last_term->log_position)
        {
            if (-1 == term_base_log_position)
            {
                AERON_SET_ERR(EINVAL,
                    "prior term not committed: leadershipTermId=%lld and term_base_log_position unspecified",
                    (long long)last_term->leadership_term_id);
                return -1;
            }
            if (aeron_cluster_recording_log_commit_log_position(
                log, last_term->leadership_term_id, term_base_log_position) < 0)
            {
                return -1;
            }
            /* Re-fetch: commit updates sorted_entries in-place via entry_index */
            last_term = aeron_cluster_recording_log_find_last_term(log);
        }

        /* Save values before appends that may reallocate sorted_entries */
        int64_t base = last_term->log_position;
        int64_t prev_term_id = last_term->leadership_term_id;

        /* Fill gap terms */
        for (int64_t term_id = prev_term_id + 1; term_id < leadership_term_id; term_id++)
        {
            if (aeron_cluster_recording_log_append_term(
                log, recording_id, term_id, base, timestamp) < 0)
            {
                return -1;
            }
        }

        if (aeron_cluster_recording_log_append_term(
            log, recording_id, leadership_term_id, base, timestamp) < 0)
        {
            return -1;
        }
        if (-1 != log_position)
        {
            if (aeron_cluster_recording_log_commit_log_position(
                log, leadership_term_id, log_position) < 0)
            {
                return -1;
            }
        }
    }
    else
    {
        /* Term already present — optionally commit log_position */
        if (-1 != log_position)
        {
            if (aeron_cluster_recording_log_commit_log_position(
                log, leadership_term_id, log_position) < 0)
            {
                return -1;
            }
        }
    }

    return aeron_cluster_recording_log_force(log);
}
