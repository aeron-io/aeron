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
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>

#if defined(_MSC_VER)
#include <io.h>
#include <direct.h>
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <process.h>
#define open _open
#define close _close
#define read _read
#define write _write
#define ftruncate(fd, size) _chsize_s(fd, size)
#define O_RDONLY _O_RDONLY
#define O_RDWR _O_RDWR
#define O_CREAT _O_CREAT
#define S_IRUSR _S_IREAD
#define S_IWUSR _S_IWRITE
#else
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#endif

#include "aeron_archive_mark_file.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_fileutil.h"
#include "concurrent/aeron_atomic.h"

/* -----------------------------------------------------------------------
 * Internal field accessors (memcpy + msync for portability)
 * ----------------------------------------------------------------------- */
static void amf_write_version(uint8_t *mapped, int32_t version)
{
    memcpy(mapped + AERON_ARCHIVE_MARK_FILE_VERSION_OFFSET, &version, sizeof(version));
    aeron_msync(mapped + AERON_ARCHIVE_MARK_FILE_VERSION_OFFSET, sizeof(version));
}

static void amf_write_activity_ms(uint8_t *mapped, int64_t now_ms)
{
    memcpy(mapped + AERON_ARCHIVE_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET, &now_ms, sizeof(now_ms));
    aeron_msync(mapped + AERON_ARCHIVE_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET, sizeof(now_ms));
}

static void amf_build_path(char *buf, size_t buf_len, const char *directory)
{
    snprintf(buf, buf_len, "%s/%s", directory, AERON_ARCHIVE_MARK_FILE_FILENAME);
}

/* -----------------------------------------------------------------------
 * Is active check
 * ----------------------------------------------------------------------- */
bool aeron_archive_mark_file_is_active(
    const char *directory, int64_t now_ms, int64_t timeout_ms)
{
    char path[4096];
    amf_build_path(path, sizeof(path), directory);

    int fd = open(path, O_RDONLY);
    if (fd < 0)
    {
        return false;
    }

    uint8_t buf[AERON_ARCHIVE_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET + 8];
    memset(buf, 0, sizeof(buf));

    const size_t needed = AERON_ARCHIVE_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET + 8;
    if ((size_t)read(fd, buf, needed) < needed)
    {
        close(fd);
        return false;
    }
    close(fd);

    int32_t version;
    memcpy(&version, buf + AERON_ARCHIVE_MARK_FILE_VERSION_OFFSET, 4);
    if (version <= 0)
    {
        return false;
    }

    int64_t activity_ms;
    memcpy(&activity_ms, buf + AERON_ARCHIVE_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET, 8);
    return (now_ms - activity_ms) < timeout_ms;
}

/* -----------------------------------------------------------------------
 * Create
 * ----------------------------------------------------------------------- */
int aeron_archive_mark_file_create(
    aeron_archive_mark_file_t **mark_file,
    const char *directory,
    int32_t error_buffer_length,
    int64_t archive_id,
    int32_t control_stream_id,
    int32_t local_control_stream_id,
    int32_t events_stream_id,
    const char *control_channel,
    const char *local_control_channel,
    const char *events_channel,
    const char *aeron_directory,
    int64_t now_ms,
    int64_t timeout_ms)
{
    char path[4096];
    amf_build_path(path, sizeof(path), directory);

    /* Check for an existing active archive */
    if (aeron_archive_mark_file_is_active(directory, now_ms, timeout_ms))
    {
        AERON_SET_ERR(EEXIST, "active archive mark file detected: %s", path);
        return -1;
    }

    if (error_buffer_length < AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT)
    {
        error_buffer_length = AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT;
    }

    const size_t total = (size_t)(AERON_ARCHIVE_MARK_FILE_HEADER_LENGTH + error_buffer_length);

    int fd = open(path, O_RDWR | O_CREAT, 0644);
    if (fd < 0)
    {
        AERON_SET_ERR(errno, "open archive mark file: %s", path);
        return -1;
    }

    if (ftruncate(fd, (off_t)total) < 0)
    {
        AERON_SET_ERR(errno, "ftruncate archive mark file: %s", path);
        close(fd);
        return -1;
    }

#if defined(_MSC_VER)
    HANDLE hmap = CreateFileMapping((HANDLE)_get_osfhandle(fd), NULL, PAGE_READWRITE, 0, (DWORD)total, NULL);
    if (NULL == hmap)
    {
        AERON_SET_ERR(GetLastError(), "CreateFileMapping archive mark file: %s", path);
        close(fd);
        return -1;
    }
    uint8_t *mapped = (uint8_t *)MapViewOfFile(hmap, FILE_MAP_WRITE, 0, 0, total);
    CloseHandle(hmap);
    if (NULL == mapped)
    {
        AERON_SET_ERR(GetLastError(), "MapViewOfFile archive mark file: %s", path);
        close(fd);
        return -1;
    }
#else
    uint8_t *mapped = (uint8_t *)mmap(NULL, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (MAP_FAILED == mapped)
    {
        AERON_SET_ERR(errno, "mmap archive mark file: %s", path);
        close(fd);
        return -1;
    }
#endif

    memset(mapped, 0, total);

    /* Write fixed fields */
#if defined(_MSC_VER)
    int64_t pid = (int64_t)_getpid();
#else
    int64_t pid = (int64_t)getpid();
#endif
    memcpy(mapped + AERON_ARCHIVE_MARK_FILE_PID_OFFSET, &pid, 8);
    memcpy(mapped + AERON_ARCHIVE_MARK_FILE_START_TIMESTAMP_OFFSET, &now_ms, 8);
    memcpy(mapped + AERON_ARCHIVE_MARK_FILE_CONTROL_STREAM_ID_OFFSET, &control_stream_id, 4);
    memcpy(mapped + AERON_ARCHIVE_MARK_FILE_LOCAL_CONTROL_STREAM_ID_OFFSET, &local_control_stream_id, 4);
    memcpy(mapped + AERON_ARCHIVE_MARK_FILE_EVENTS_STREAM_ID_OFFSET, &events_stream_id, 4);

    int32_t hdr_len = AERON_ARCHIVE_MARK_FILE_HEADER_LENGTH;
    memcpy(mapped + AERON_ARCHIVE_MARK_FILE_HEADER_LENGTH_OFFSET, &hdr_len, 4);
    memcpy(mapped + AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_OFFSET, &error_buffer_length, 4);
    memcpy(mapped + AERON_ARCHIVE_MARK_FILE_ARCHIVE_ID_OFFSET, &archive_id, 8);

    /*
     * Variable-length strings are written after the fixed block.
     * Each is a 4-byte length prefix followed by the ASCII bytes.
     * This matches the SBE VarAsciiEncoding pattern used by the Java ArchiveMarkFile.
     */
    size_t offset = 68;
    const char *strings[] = {
        control_channel,
        local_control_channel,
        events_channel,
        aeron_directory
    };
    for (int i = 0; i < 4; i++)
    {
        const char *s = strings[i];
        int32_t slen = (NULL != s) ? (int32_t)strlen(s) : 0;
        if (offset + 4 + (size_t)slen <= AERON_ARCHIVE_MARK_FILE_HEADER_LENGTH)
        {
            memcpy(mapped + offset, &slen, 4);
            offset += 4;
            if (slen > 0)
            {
                memcpy(mapped + offset, s, (size_t)slen);
                offset += (size_t)slen;
            }
        }
    }

    aeron_msync(mapped, total);

    aeron_archive_mark_file_t *mf = NULL;
    if (aeron_alloc((void **)&mf, sizeof(aeron_archive_mark_file_t)) < 0)
    {
#if defined(_MSC_VER)
        UnmapViewOfFile(mapped);
#else
        munmap(mapped, total);
#endif
        close(fd);
        return -1;
    }

    mf->fd = fd;
    mf->mapped = mapped;
    mf->mapped_length = total;
    mf->error_buffer_length = error_buffer_length;
    snprintf(mf->path, sizeof(mf->path), "%s", path);

    *mark_file = mf;
    return 0;
}

/* -----------------------------------------------------------------------
 * Close
 * ----------------------------------------------------------------------- */
int aeron_archive_mark_file_close(aeron_archive_mark_file_t *mark_file)
{
    if (NULL != mark_file)
    {
        if (NULL != mark_file->mapped)
        {
#if defined(_MSC_VER)
            UnmapViewOfFile(mark_file->mapped);
#else
            munmap(mark_file->mapped, mark_file->mapped_length);
#endif
            mark_file->mapped = NULL;
        }
        if (mark_file->fd >= 0)
        {
            close(mark_file->fd);
            mark_file->fd = -1;
        }
        aeron_free(mark_file);
    }
    return 0;
}

/* -----------------------------------------------------------------------
 * Signal ready / terminated
 * ----------------------------------------------------------------------- */
void aeron_archive_mark_file_signal_ready(aeron_archive_mark_file_t *mark_file, int64_t now_ms)
{
    if (NULL != mark_file && NULL != mark_file->mapped)
    {
        amf_write_activity_ms(mark_file->mapped, now_ms);
        amf_write_version(mark_file->mapped, AERON_ARCHIVE_MARK_FILE_SEMANTIC_VERSION);
    }
}

void aeron_archive_mark_file_signal_terminated(aeron_archive_mark_file_t *mark_file)
{
    if (NULL != mark_file && NULL != mark_file->mapped)
    {
        int64_t null_value = -1LL;
        amf_write_activity_ms(mark_file->mapped, null_value);
    }
}

/* -----------------------------------------------------------------------
 * Activity timestamp operations
 * ----------------------------------------------------------------------- */
void aeron_archive_mark_file_update_activity_timestamp(
    aeron_archive_mark_file_t *mark_file, int64_t now_ms)
{
    if (NULL != mark_file && NULL != mark_file->mapped)
    {
        amf_write_activity_ms(mark_file->mapped, now_ms);
    }
}

int64_t aeron_archive_mark_file_activity_timestamp_volatile(
    const aeron_archive_mark_file_t *mark_file)
{
    if (NULL == mark_file || NULL == mark_file->mapped)
    {
        return -1LL;
    }

    int64_t ts;
    AERON_GET_ACQUIRE(ts,
        *(volatile int64_t *)(mark_file->mapped + AERON_ARCHIVE_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET));
    return ts;
}

/* -----------------------------------------------------------------------
 * Accessors
 * ----------------------------------------------------------------------- */
int64_t aeron_archive_mark_file_archive_id(const aeron_archive_mark_file_t *mark_file)
{
    if (NULL == mark_file || NULL == mark_file->mapped)
    {
        return -1LL;
    }
    int64_t id;
    memcpy(&id, mark_file->mapped + AERON_ARCHIVE_MARK_FILE_ARCHIVE_ID_OFFSET, 8);
    return id;
}

uint8_t *aeron_archive_mark_file_error_buffer(aeron_archive_mark_file_t *mark_file)
{
    if (NULL == mark_file || NULL == mark_file->mapped)
    {
        return NULL;
    }
    return mark_file->mapped + AERON_ARCHIVE_MARK_FILE_HEADER_LENGTH;
}

int32_t aeron_archive_mark_file_error_buffer_length(const aeron_archive_mark_file_t *mark_file)
{
    if (NULL == mark_file)
    {
        return 0;
    }
    return mark_file->error_buffer_length;
}
