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
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

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
}

static bool is_valid_entry(int32_t entry_type)
{
    return (entry_type & AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG) == 0;
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
    log->entries = (aeron_cluster_recording_log_entry_t *)log->mapped;
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
    _log->entries      = NULL;

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
        _log->entries = (aeron_cluster_recording_log_entry_t *)_log->mapped;
    }

    *log = _log;
    return 0;
}

int aeron_cluster_recording_log_close(aeron_cluster_recording_log_t *log)
{
    if (NULL != log)
    {
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

int aeron_cluster_recording_log_force(aeron_cluster_recording_log_t *log)
{
    if (NULL != log->mapped)
    {
        msync(log->mapped, log->mapped_length, MS_SYNC);
    }
    return 0;
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
    return 0;
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

    /* Enforce same recording_id across all TERM entries (Java: "invalid TERM recordingId=%d, expected...") */
    int64_t first_rec = aeron_cluster_recording_log_find_last_term_recording_id(log);
    if (first_rec >= 0 && first_rec != recording_id)
    {
        AERON_SET_ERR(EINVAL, "invalid TERM recordingId=%lld, expected recordingId=%lld",
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

    aeron_cluster_recording_log_entry_t e = {
        .recording_id          = recording_id,
        .leadership_term_id    = leadership_term_id,
        .term_base_log_position = term_base_log_position,
        .log_position          = -1,  /* open */
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
            count++;
        }
    }
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
    return 0;
}

/* -----------------------------------------------------------------------
 * Queries
 * ----------------------------------------------------------------------- */
aeron_cluster_recording_log_entry_t *aeron_cluster_recording_log_find_last_term(
    aeron_cluster_recording_log_t *log)
{
    /* Entries are stored sequentially; scan in reverse for last valid TERM */
    static aeron_cluster_recording_log_entry_t result;
    for (int i = log->entry_count - 1; i >= 0; i--)
    {
        const uint8_t *slot = log->mapped + (size_t)i * ENTRY_STRIDE;
        int32_t entry_type;
        memcpy(&entry_type, slot + AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_OFFSET, 4);
        if (entry_type == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM)
        {
            entry_read(&result, slot);
            return &result;
        }
    }
    return NULL;
}

aeron_cluster_recording_log_entry_t *aeron_cluster_recording_log_get_term_entry(
    aeron_cluster_recording_log_t *log, int64_t leadership_term_id)
{
    static aeron_cluster_recording_log_entry_t result;
    for (int i = 0; i < log->entry_count; i++)
    {
        const uint8_t *slot = log->mapped + (size_t)i * ENTRY_STRIDE;
        int64_t stored_id; int32_t entry_type;
        memcpy(&stored_id,  slot + AERON_CLUSTER_RECORDING_LOG_LEADERSHIP_TERM_ID_OFFSET, 8);
        memcpy(&entry_type, slot + AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_OFFSET, 4);
        if (stored_id == leadership_term_id &&
            entry_type == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM)
        {
            entry_read(&result, slot);
            return &result;
        }
    }
    return NULL;
}

aeron_cluster_recording_log_entry_t *aeron_cluster_recording_log_get_latest_snapshot(
    aeron_cluster_recording_log_t *log, int32_t service_id)
{
    static aeron_cluster_recording_log_entry_t result;
    for (int i = log->entry_count - 1; i >= 0; i--)
    {
        const uint8_t *slot = log->mapped + (size_t)i * ENTRY_STRIDE;
        int32_t entry_type; int32_t stored_svc;
        memcpy(&entry_type,  slot + AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_OFFSET, 4);
        memcpy(&stored_svc,  slot + AERON_CLUSTER_RECORDING_LOG_SERVICE_ID_OFFSET, 4);
        if (is_valid_entry(entry_type) &&
            entry_type == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT &&
            stored_svc == service_id)
        {
            entry_read(&result, slot);
            return &result;
        }
    }
    return NULL;
}

bool aeron_cluster_recording_log_is_unknown(
    aeron_cluster_recording_log_t *log, int64_t leadership_term_id)
{
    return NULL == aeron_cluster_recording_log_get_term_entry(log, leadership_term_id);
}

/* -----------------------------------------------------------------------
 * Recovery plan
 * ----------------------------------------------------------------------- */
int aeron_cluster_recording_log_create_recovery_plan(
    aeron_cluster_recording_log_t *log,
    aeron_cluster_recovery_plan_t **plan,
    int service_count)
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
        p->last_append_position     = last_term->log_position;
        p->last_term_recording_id   = last_term->recording_id;
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
    if (index < 0 || index >= log->entry_count) { return NULL; }
    static aeron_cluster_recording_log_entry_t result;
    const uint8_t *slot = log->mapped + (size_t)index * ENTRY_STRIDE;
    entry_read(&result, slot);
    return &result;
}

int64_t aeron_cluster_recording_log_find_last_term_recording_id(
    aeron_cluster_recording_log_t *log)
{
    for (int i = log->entry_count - 1; i >= 0; i--)
    {
        const uint8_t *slot = log->mapped + (size_t)i * ENTRY_STRIDE;
        int32_t entry_type;
        memcpy(&entry_type, slot + AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_OFFSET, 4);
        if (is_valid_entry(entry_type) &&
            entry_type == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM)
        {
            int64_t recording_id;
            memcpy(&recording_id, slot + AERON_CLUSTER_RECORDING_LOG_RECORDING_ID_OFFSET, 8);
            return recording_id;
        }
    }
    return -1;
}

int64_t aeron_cluster_recording_log_get_term_timestamp(
    aeron_cluster_recording_log_t *log, int64_t leadership_term_id)
{
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
            int64_t ts;
            memcpy(&ts, slot + AERON_CLUSTER_RECORDING_LOG_TIMESTAMP_OFFSET, 8);
            return ts;
        }
    }
    return -1;
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
    aeron_cluster_recording_log_entry_t e = {
        .recording_id           = recording_id,
        .leadership_term_id     = leadership_term_id,
        .term_base_log_position  = term_base_log_position,
        .log_position            = log_position,
        .timestamp               = timestamp,
        .service_id              = service_id,
        .entry_type              = AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT,
    };
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
