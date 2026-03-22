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
#include <sys/types.h>
#include <time.h>

#include "aeron_cluster_mark_file.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

/* -----------------------------------------------------------------------
 * Internal field accessors
 * ----------------------------------------------------------------------- */
static int32_t mf_read_version(const uint8_t *mapped)
{
    int32_t v;
    memcpy(&v, mapped + AERON_CLUSTER_MARK_FILE_VERSION_OFFSET, 4);
    return v;
}

static void mf_write_version(uint8_t *mapped, int32_t version)
{
    memcpy(mapped + AERON_CLUSTER_MARK_FILE_VERSION_OFFSET, &version, 4);
    msync(mapped + AERON_CLUSTER_MARK_FILE_VERSION_OFFSET, 4, MS_SYNC);
}

static int64_t mf_read_activity_ms(const uint8_t *mapped)
{
    int64_t ts;
    memcpy(&ts, mapped + AERON_CLUSTER_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET, 8);
    return ts;
}

static void mf_write_activity_ms(uint8_t *mapped, int64_t now_ms)
{
    memcpy(mapped + AERON_CLUSTER_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET, &now_ms, 8);
    msync(mapped + AERON_CLUSTER_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET, 8, MS_SYNC);
}

/* -----------------------------------------------------------------------
 * isActive check
 * ----------------------------------------------------------------------- */
bool aeron_cluster_mark_file_is_active(const char *path, int64_t now_ms, int64_t timeout_ms)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0) { return false; }

    uint8_t buf[AERON_CLUSTER_MARK_FILE_VERSION_OFFSET + 8 +
                AERON_CLUSTER_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET + 8];
    memset(buf, 0, sizeof(buf));

    /* Read just the version and activity timestamp area */
    const size_t needed = AERON_CLUSTER_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET + 8;
    if ((size_t)read(fd, buf, needed) < needed) { close(fd); return false; }
    close(fd);

    int32_t version;
    memcpy(&version, buf + AERON_CLUSTER_MARK_FILE_VERSION_OFFSET, 4);
    if (version <= 0) { return false; }

    int64_t activity_ms;
    memcpy(&activity_ms, buf + AERON_CLUSTER_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET, 8);
    return (now_ms - activity_ms) < timeout_ms;
}

/* -----------------------------------------------------------------------
 * Open / create
 * ----------------------------------------------------------------------- */
int aeron_cluster_mark_file_open(
    aeron_cluster_mark_file_t **mark_file,
    const char *path,
    aeron_cluster_component_type_t component_type,
    int error_buffer_length,
    int64_t now_ms,
    int64_t timeout_ms)
{
    /* Check for active existing instance */
    if (aeron_cluster_mark_file_is_active(path, now_ms, timeout_ms))
    {
        AERON_SET_ERR(EEXIST, "active mark file detected: %s", path);
        return -1;
    }

    const size_t total = (size_t)(AERON_CLUSTER_MARK_FILE_HEADER_LENGTH + error_buffer_length);

    int fd = open(path, O_RDWR | O_CREAT, 0644);
    if (fd < 0)
    {
        AERON_SET_ERR(errno, "open mark file: %s", path);
        return -1;
    }

    if (ftruncate(fd, (off_t)total) < 0)
    {
        AERON_SET_ERR(errno, "ftruncate mark file: %s", path);
        close(fd);
        return -1;
    }

    uint8_t *mapped = (uint8_t *)mmap(NULL, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (MAP_FAILED == mapped)
    {
        AERON_SET_ERR(errno, "mmap mark file: %s", path);
        close(fd);
        return -1;
    }

    memset(mapped, 0, total);

    /* Write component_type and lengths */
    int32_t ct = (int32_t)component_type;
    memcpy(mapped + AERON_CLUSTER_MARK_FILE_COMPONENT_TYPE_OFFSET, &ct, 4);
    int32_t hlen = AERON_CLUSTER_MARK_FILE_HEADER_LENGTH;
    memcpy(mapped + AERON_CLUSTER_MARK_FILE_HEADER_LENGTH_OFFSET, &hlen, 4);
    int32_t elen = error_buffer_length;
    memcpy(mapped + AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_LENGTH_OFFSET, &elen, 4);

    /* candidateTermId = -1 (NULL_VALUE) */
    int64_t null_term = -1LL;
    memcpy(mapped + AERON_CLUSTER_MARK_FILE_CANDIDATE_TERM_ID_OFFSET, &null_term, 8);

    /* pid */
    int64_t pid = (int64_t)getpid();
    memcpy(mapped + AERON_CLUSTER_MARK_FILE_PID_OFFSET, &pid, 8);

    /* startTimestamp */
    memcpy(mapped + AERON_CLUSTER_MARK_FILE_START_TIMESTAMP_OFFSET, &now_ms, 8);

    msync(mapped, total, MS_SYNC);

    aeron_cluster_mark_file_t *mf = NULL;
    if (aeron_alloc((void **)&mf, sizeof(aeron_cluster_mark_file_t)) < 0)
    {
        munmap(mapped, total);
        close(fd);
        return -1;
    }

    mf->fd            = fd;
    mf->mapped        = mapped;
    mf->mapped_length = total;
    snprintf(mf->path, sizeof(mf->path), "%s", path);

    *mark_file = mf;
    return 0;
}

int aeron_cluster_mark_file_close(aeron_cluster_mark_file_t *mark_file)
{
    if (NULL != mark_file)
    {
        if (NULL != mark_file->mapped)
        {
            munmap(mark_file->mapped, mark_file->mapped_length);
        }
        if (mark_file->fd >= 0)
        {
            close(mark_file->fd);
        }
        aeron_free(mark_file);
    }
    return 0;
}

void aeron_cluster_mark_file_signal_ready(aeron_cluster_mark_file_t *mark_file, int64_t now_ms)
{
    mf_write_activity_ms(mark_file->mapped, now_ms);
    mf_write_version(mark_file->mapped, AERON_CLUSTER_MARK_FILE_SEMANTIC_VERSION);
}

void aeron_cluster_mark_file_update_activity_timestamp(
    aeron_cluster_mark_file_t *mark_file, int64_t now_ms)
{
    mf_write_activity_ms(mark_file->mapped, now_ms);
}

int64_t aeron_cluster_mark_file_candidate_term_id(aeron_cluster_mark_file_t *mark_file)
{
    int64_t term_id;
    memcpy(&term_id, mark_file->mapped + AERON_CLUSTER_MARK_FILE_CANDIDATE_TERM_ID_OFFSET, 8);
    return term_id;
}

void aeron_cluster_mark_file_set_candidate_term_id(
    aeron_cluster_mark_file_t *mark_file, int64_t term_id)
{
    memcpy(mark_file->mapped + AERON_CLUSTER_MARK_FILE_CANDIDATE_TERM_ID_OFFSET, &term_id, 8);
    msync(mark_file->mapped + AERON_CLUSTER_MARK_FILE_CANDIDATE_TERM_ID_OFFSET, 8, MS_SYNC);
}

void aeron_cluster_mark_file_filename(char *buf, size_t buf_len)
{
    snprintf(buf, buf_len, "%s", AERON_CLUSTER_MARK_FILE_FILENAME);
}

void aeron_cluster_mark_file_service_filename(char *buf, size_t buf_len, int service_id)
{
    snprintf(buf, buf_len, "%s%d%s",
        AERON_CLUSTER_MARK_FILE_SERVICE_PREFIX, service_id, AERON_CLUSTER_MARK_FILE_EXT);
}
