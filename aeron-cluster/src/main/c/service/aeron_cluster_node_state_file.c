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
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>

#if defined(_MSC_VER)
#include <io.h>
#include <windows.h>
#define open _open
#define close _close
#define ftruncate(fd, size) _chsize_s(fd, size)
#define O_RDWR _O_RDWR
#define O_CREAT _O_CREAT
#define O_TRUNC _O_TRUNC
#else
#include <unistd.h>
#include <sys/mman.h>
#endif

#include "aeron_cluster_node_state_file.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_fileutil.h"
#include "concurrent/aeron_atomic.h"

static void nsf_sync(aeron_cluster_node_state_file_t *nsf, size_t offset, size_t len)
{
    if (nsf->file_sync_level > 0)
    {
        aeron_msync(nsf->mapped + offset, len);
    }
}

static int nsf_create(const char *path, size_t length)
{
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { return -1; }
    if (ftruncate(fd, (off_t)length) < 0)
    {
        close(fd);
        return -1;
    }
    return fd;
}

int aeron_cluster_node_state_file_open(
    aeron_cluster_node_state_file_t **nsf,
    const char *cluster_dir,
    bool create_new,
    int file_sync_level)
{
    aeron_cluster_node_state_file_t *f = NULL;
    if (aeron_alloc((void **)&f, sizeof(*f)) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to allocate node state file");
        return -1;
    }

    snprintf(f->path, sizeof(f->path), "%s/%s", cluster_dir, AERON_CLUSTER_NODE_STATE_FILE_FILENAME);
    f->mapped_length = AERON_CLUSTER_NODE_STATE_FILE_LENGTH;
    f->file_sync_level = file_sync_level;

    struct stat st;
    bool exists = (stat(f->path, &st) == 0);

    if (!exists && !create_new)
    {
        AERON_SET_ERR(ENOENT, "NodeStateFile does not exist and create_new=false: %s", f->path);
        aeron_free(f);
        return -1;
    }

    if (!exists)
    {
        f->fd = nsf_create(f->path, f->mapped_length);
    }
    else
    {
        f->fd = open(f->path, O_RDWR, 0644);
    }

    if (f->fd < 0)
    {
        AERON_SET_ERR(errno, "failed to open node state file: %s", f->path);
        aeron_free(f);
        return -1;
    }

#if defined(_MSC_VER)
    {
        HANDLE hmap = CreateFileMapping((HANDLE)_get_osfhandle(f->fd), NULL, PAGE_READWRITE, 0,
            (DWORD)f->mapped_length, NULL);
        if (NULL == hmap)
        {
            AERON_SET_ERR(GetLastError(), "CreateFileMapping node state file: %s", f->path);
            close(f->fd);
            aeron_free(f);
            return -1;
        }
        f->mapped = (uint8_t *)MapViewOfFile(hmap, FILE_MAP_WRITE, 0, 0, f->mapped_length);
        CloseHandle(hmap);
        if (NULL == f->mapped)
        {
            AERON_SET_ERR(GetLastError(), "MapViewOfFile node state file: %s", f->path);
            close(f->fd);
            aeron_free(f);
            return -1;
        }
    }
#else
    f->mapped = (uint8_t *)mmap(NULL, f->mapped_length, PROT_READ | PROT_WRITE, MAP_SHARED, f->fd, 0);
    if (MAP_FAILED == f->mapped)
    {
        AERON_SET_ERR(errno, "failed to mmap node state file: %s", f->path);
        close(f->fd);
        aeron_free(f);
        return -1;
    }
#endif

    if (!exists)
    {
        /* Initialise: zero entire mapping then write version and sentinel candidateTermId */
        memset(f->mapped, 0, f->mapped_length);
        int32_t version = AERON_CLUSTER_NODE_STATE_FILE_VERSION;
        memcpy(f->mapped + AERON_CLUSTER_NSF_VERSION_OFFSET, &version, sizeof(int32_t));
        int64_t null_val = -1;
        memcpy(f->mapped + AERON_CLUSTER_NSF_CANDIDATE_TERM_OFFSET, &null_val, sizeof(int64_t));
        memcpy(f->mapped + AERON_CLUSTER_NSF_LOG_POSITION_OFFSET,    &null_val, sizeof(int64_t));
        memcpy(f->mapped + AERON_CLUSTER_NSF_TIMESTAMP_OFFSET,       &null_val, sizeof(int64_t));
        nsf_sync(f, 0, 32);
    }
    else
    {
        int32_t version = 0;
        memcpy(&version, f->mapped + AERON_CLUSTER_NSF_VERSION_OFFSET, sizeof(int32_t));
        if (version != AERON_CLUSTER_NODE_STATE_FILE_VERSION)
        {
            AERON_SET_ERR(EINVAL,
                "node state file version mismatch: expected %d got %d in %s",
                AERON_CLUSTER_NODE_STATE_FILE_VERSION, version, f->path);
#if defined(_MSC_VER)
            UnmapViewOfFile(f->mapped);
#else
            munmap(f->mapped, f->mapped_length);
#endif
            close(f->fd);
            aeron_free(f);
            return -1;
        }
    }

    *nsf = f;
    return 0;
}

int aeron_cluster_node_state_file_close(aeron_cluster_node_state_file_t *nsf)
{
    if (NULL == nsf) { return 0; }
    if (NULL != nsf->mapped)
    {
#if defined(_MSC_VER)
        UnmapViewOfFile(nsf->mapped);
#else
        if (MAP_FAILED != (void *)nsf->mapped)
        {
            munmap(nsf->mapped, nsf->mapped_length);
        }
#endif
    }
    if (nsf->fd >= 0) { close(nsf->fd); }
    aeron_free(nsf);
    return 0;
}

void aeron_cluster_node_state_file_update_candidate_term_id(
    aeron_cluster_node_state_file_t *nsf,
    int64_t candidate_term_id,
    int64_t log_position,
    int64_t timestamp_ms)
{
    /* Write supporting fields first, then candidateTermId last as a commit sentinel */
    memcpy(nsf->mapped + AERON_CLUSTER_NSF_LOG_POSITION_OFFSET, &log_position,  sizeof(int64_t));
    memcpy(nsf->mapped + AERON_CLUSTER_NSF_TIMESTAMP_OFFSET,    &timestamp_ms,  sizeof(int64_t));
    aeron_release();
    memcpy(nsf->mapped + AERON_CLUSTER_NSF_CANDIDATE_TERM_OFFSET, &candidate_term_id, sizeof(int64_t));
    nsf_sync(nsf, AERON_CLUSTER_NSF_LOG_POSITION_OFFSET, 24);
}

int64_t aeron_cluster_node_state_file_propose_max_candidate_term_id(
    aeron_cluster_node_state_file_t *nsf,
    int64_t candidate_term_id,
    int64_t log_position,
    int64_t timestamp_ms)
{
    int64_t existing = aeron_cluster_node_state_file_candidate_term_id(nsf);
    if (candidate_term_id > existing)
    {
        aeron_cluster_node_state_file_update_candidate_term_id(nsf, candidate_term_id, log_position, timestamp_ms);
        return candidate_term_id;
    }
    return existing;
}

int64_t aeron_cluster_node_state_file_candidate_term_id(const aeron_cluster_node_state_file_t *nsf)
{
    int64_t v;
    aeron_acquire();
    memcpy(&v, nsf->mapped + AERON_CLUSTER_NSF_CANDIDATE_TERM_OFFSET, sizeof(int64_t));
    return v;
}

int64_t aeron_cluster_node_state_file_log_position(const aeron_cluster_node_state_file_t *nsf)
{
    int64_t v;
    memcpy(&v, nsf->mapped + AERON_CLUSTER_NSF_LOG_POSITION_OFFSET, sizeof(int64_t));
    return v;
}

int64_t aeron_cluster_node_state_file_timestamp(const aeron_cluster_node_state_file_t *nsf)
{
    int64_t v;
    memcpy(&v, nsf->mapped + AERON_CLUSTER_NSF_TIMESTAMP_OFFSET, sizeof(int64_t));
    return v;
}
