/*
 * Copyright 2014-2022 Real Logic Limited.
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
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#if defined(__linux__) || defined(AERON_COMPILER_MSVC)
#define AERON_NATIVE_PRETOUCH
#endif

#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "aeron_platform.h"
#include "aeron_error.h"
#include "aeron_fileutil.h"

#ifdef _MSC_VER
#define AERON_FILE_SEP '\\'
#else
#define AERON_FILE_SEP '/'
#endif

#if defined(AERON_COMPILER_MSVC)

#include <windows.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <io.h>
#include <direct.h>

#define PROT_READ  1
#define PROT_WRITE 2
#define MAP_FAILED ((void *)-1)

#define MAP_SHARED 0x01
#define S_IRUSR _S_IREAD
#define S_IWUSR _S_IWRITE
#define S_IRGRP 0
#define S_IWGRP 0
#define S_IROTH 0
#define S_IWOTH 0

static int aeron_mmap(aeron_mapped_file_t *mapping, int fd, bool pre_touch, const char *path)
{
    HANDLE hmap = CreateFileMapping((HANDLE)_get_osfhandle(fd), 0, PAGE_READWRITE, 0, 0, 0);

    if (!hmap)
    {
        AERON_SET_ERR_WIN(GetLastError(), "CreateFileMapping failed for: %s", path);
        return -1;
    }

    mapping->addr = MapViewOfFileEx(hmap, FILE_MAP_WRITE, 0, 0, mapping->length, NULL);

    if (!mapping->addr)
    {
        mapping->addr = MAP_FAILED;
        AERON_SET_ERR_WIN(GetLastError(), "MapViewOfFileEx failed for: %s", path);
    }

    if (!CloseHandle(hmap))
    {
        fprintf(stderr, "unable to close file mapping handle\n");
    }
    close(fd);

    if (pre_touch && MAP_FAILED != mapping->addr)
    {
        WIN32_MEMORY_RANGE_ENTRY entry;
        entry.NumberOfBytes = mapping->length;
        entry.VirtualAddress = mapping->addr;

        if (!PrefetchVirtualMemory(GetCurrentProcess(), 1, &entry, 0))
        {
            AERON_SET_ERR_WIN(GetLastError(), "PrefetchVirtualMemory failed for: %s", path);
            aeron_unmap(mapping);
            mapping->addr = MAP_FAILED;
        }
    }

    return MAP_FAILED == mapping->addr ? -1 : 0;
}

int aeron_unmap(aeron_mapped_file_t *mapped_file)
{
    if (NULL != mapped_file->addr)
    {
        return UnmapViewOfFile(mapped_file->addr) ? 0 : -1;
    }

    return 0;
}

int aeron_mkdir(const char *path, int permission)
{
    return _mkdir(path);
}

int64_t aeron_file_length(const char *path)
{
    WIN32_FILE_ATTRIBUTE_DATA fad;

    if (GetFileAttributesEx(path, GetFileExInfoStandard, &fad) == 0)
    {
        return -1;
    }

    ULARGE_INTEGER file_size;
    file_size.LowPart = fad.nFileSizeLow;
    file_size.HighPart = fad.nFileSizeHigh;

    return (int64_t)file_size.QuadPart;
}

uint64_t aeron_usable_fs_space(const char *path)
{
    ULARGE_INTEGER lpAvailableToCaller, lpTotalNumberOfBytes, lpTotalNumberOfFreeBytes;

    if (!GetDiskFreeSpaceEx(path, &lpAvailableToCaller, &lpTotalNumberOfBytes, &lpTotalNumberOfFreeBytes))
    {
        return 0;
    }

    return (uint64_t)lpAvailableToCaller.QuadPart;
}

int aeron_create_file(const char *path, size_t length)
{
    int fd;
    int error = _sopen_s(&fd, path, _O_RDWR | _O_CREAT | _O_EXCL, _SH_DENYNO, _S_IREAD | _S_IWRITE);

    if (NO_ERROR != error)
    {
        AERON_SET_ERR_WIN(GetLastError(), "Failed to create file: %s", path);
        return -1;
    }

    HANDLE hfile = (HANDLE)_get_osfhandle(fd);
    LARGE_INTEGER file_size;
    file_size.QuadPart = (LONGLONG)length;

    if (!SetFilePointerEx(hfile, file_size, NULL, FILE_BEGIN) ||
        !SetEndOfFile(hfile))
    {
        AERON_SET_ERR_WIN(GetLastError(), "Failed to truncate file: %s", path);
        close(fd);
        if (-1 == remove(path))
        {
            AERON_APPEND_ERR("Failed to remove file: %s", path);
        }
        return -1;
    }

    return fd;
}

int aeron_open_file_rw(const char *path)
{
    int fd;
    int error = _sopen_s(&fd, path, _O_RDWR, _SH_DENYNO, _S_IREAD | _S_IWRITE);

    if (NO_ERROR != error)
    {
        return -1;
    }

    return fd;
}

int aeron_delete_directory(const char *dir)
{
    char dir_buffer[(AERON_MAX_PATH * 2) + 2] = { 0 };

    size_t dir_length = strlen(dir);
    if (dir_length > (AERON_MAX_PATH * 2))
    {
        return -1;
    }

    memcpy(dir_buffer, dir, dir_length);
    dir_buffer[dir_length] = '\0';
    dir_buffer[dir_length + 1] = '\0';

    SHFILEOPSTRUCT file_op =
        {
            NULL,
            FO_DELETE,
            dir_buffer,
            NULL,
            FOF_NOCONFIRMATION |
            FOF_NOERRORUI |
            FOF_SILENT,
            false,
            NULL,
            NULL
        };

    return SHFileOperation(&file_op);
}

int aeron_is_directory(const char *path)
{
    const DWORD attributes = GetFileAttributes(path);
    return INVALID_FILE_ATTRIBUTES != attributes && (attributes & FILE_ATTRIBUTE_DIRECTORY);
}

#else
#include <unistd.h>
#include <sys/mman.h>
#include <sys/statvfs.h>
#include <ftw.h>
#include <stdio.h>
#include <pwd.h>

static int aeron_mmap(aeron_mapped_file_t *mapping, int fd, bool pre_touch, const char *path)
{
    int flags = MAP_SHARED;

#ifdef __linux__
    if (pre_touch)
    {
        flags = flags | MAP_POPULATE;
    }
#else
    (void)pre_touch;
#endif

    mapping->addr = mmap(NULL, mapping->length, PROT_READ | PROT_WRITE, flags, fd, 0);

    if (MAP_FAILED == mapping->addr)
    {
        AERON_SET_ERR(errno, "Failed to mmap file: %s", path);
        close(fd);
        return -1;
    }

#ifdef __linux__
    if (pre_touch)
    {
        static uint8_t single_zero[1] = { 0x0 };
        if (1 != pwrite(fd, single_zero, 1, (off_t)(mapping->length - 1)))
        {
            AERON_SET_ERR(errno, "Failed to pre-touch file: %s", path);
            if (0 != aeron_unmap(mapping))
            {
                AERON_APPEND_ERR("Failed to unmap file: %s", path);
            }
            close(fd);
            if (-1 == remove(path))
            {
                AERON_APPEND_ERR("Failed to remove file: %s", path);
            }
            return -1;
        }
    }
#endif

    close(fd);
    return 0;
}

int aeron_unmap(aeron_mapped_file_t *mapped_file)
{
    if (NULL != mapped_file->addr)
    {
        return munmap(mapped_file->addr, mapped_file->length);
    }

    return 0;
}

static int unlink_func(const char *path, const struct stat *sb, int type_flag, struct FTW *ftw)
{
    if (remove(path) != 0)
    {
        AERON_SET_ERR(errno, "could not remove %s", path);
    }

    return 0;
}

int aeron_delete_directory(const char *dirname)
{
    return nftw(dirname, unlink_func, 64, FTW_DEPTH | FTW_PHYS);
}

int aeron_is_directory(const char *dirname)
{
    struct stat sb;
    return stat(dirname, &sb) == 0 && S_ISDIR(sb.st_mode);
}

int64_t aeron_file_length(const char *path)
{
    struct stat sb;
    return stat(path, &sb) == 0 ? sb.st_size : -1;
}

uint64_t aeron_usable_fs_space(const char *path)
{
    struct statvfs vfs;
    uint64_t result = 0;

    if (statvfs(path, &vfs) == 0)
    {
        result = vfs.f_bsize * vfs.f_bavail;
    }

    return result;
}

int aeron_create_file(const char *path, size_t length)
{
    int fd = open(path, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    if (fd < 0)
    {
        AERON_SET_ERR(errno, "Failed to create file: %s", path);
        return -1;
    }

    if (0 != ftruncate(fd, length))
    {
        AERON_SET_ERR(errno, "Failed to truncate file: %s", path);
        close(fd);
        if (-1 == remove(path))
        {
            AERON_APPEND_ERR("Failed to remove file: %s", path);
        }
        return -1;
    }
    return fd;
}

int aeron_open_file_rw(const char *path)
{
    return open(path, O_RDWR);
}
#endif

#include <inttypes.h>

#define AERON_BLOCK_SIZE (4 * 1024)

#ifndef AERON_NATIVE_PRETOUCH
static void aeron_touch_pages(volatile uint8_t *base, size_t length, size_t page_size)
{
    for (size_t i = 0; i < length; i += page_size)
    {
        volatile uint8_t *first_page_byte = base + i;
        *first_page_byte = 0;
    }
}
#endif

int aeron_map_new_file(aeron_mapped_file_t *mapped_file, const char *path, bool fill_with_zeroes)
{
    int fd = aeron_create_file(path, mapped_file->length);
    if (-1 == fd)
    {
        return -1;
    }

    if (0 != aeron_mmap(mapped_file, fd, fill_with_zeroes, path))
    {
        if (-1 == remove(path))
        {
            AERON_APPEND_ERR("Failed to remove file: %s", path);
        }
        return -1;
    }

#ifndef AERON_NATIVE_PRETOUCH
    if (fill_with_zeroes)
    {
        aeron_touch_pages(mapped_file->addr, mapped_file->length, AERON_BLOCK_SIZE);
    }
#endif

    return 0;
}

int aeron_map_existing_file(aeron_mapped_file_t *mapped_file, const char *path)
{
    int fd = aeron_open_file_rw(path);
    if (fd < 0)
    {
        AERON_SET_ERR(errno, "Failed to open file: %s", path);
        return -1;
    }

    const int64_t file_length = aeron_file_length(path);
    if (-1 == file_length)
    {
        AERON_SET_ERR(errno, "Failed to determine the size of the file: %s", path);
        close(fd);
        return -1;
    }

    mapped_file->length = (size_t)file_length;

    if (0 != aeron_mmap(mapped_file, fd, false, path))
    {
        return -1;
    }

    return 0;
}

uint64_t aeron_usable_fs_space_disabled(const char *path)
{
    return UINT64_MAX;
}

int aeron_ipc_publication_location(char *dst, size_t length, const char *aeron_dir, int64_t correlation_id)
{
    return snprintf(
        dst, length,
        "%s/" AERON_PUBLICATIONS_DIR "/%" PRId64 ".logbuffer",
        aeron_dir, correlation_id);
}

int aeron_network_publication_location(char *dst, size_t length, const char *aeron_dir, int64_t correlation_id)
{
    return snprintf(
        dst, length,
        "%s/" AERON_PUBLICATIONS_DIR "/%" PRId64 ".logbuffer",
        aeron_dir, correlation_id);
}

int aeron_publication_image_location(char *dst, size_t length, const char *aeron_dir, int64_t correlation_id)
{
    return snprintf(
        dst, length,
        "%s/" AERON_IMAGES_DIR "/%" PRId64 ".logbuffer",
        aeron_dir, correlation_id);
}

size_t aeron_temp_filename(char *filename, size_t length)
{
#if !defined(_MSC_VER)
    char rawname[] = "/tmp/aeron-c.XXXXXXX";
    int fd = mkstemp(rawname);
    close(fd);
    unlink(rawname);

    strncpy(filename, rawname, length);

    return strlen(filename);
#else
    char tmpdir[MAX_PATH + 1];
    char tmpfile[MAX_PATH];

    if (GetTempPath(MAX_PATH, &tmpdir[0]) > 0)
    {
        if (GetTempFileName(tmpdir, TEXT("aeron-c"), 101, &tmpfile[0]) != 0)
        {
            strncpy(filename, tmpfile, length);
            return strlen(filename);
        }
    }

    return 0;
#endif
}

int aeron_raw_log_map(
    aeron_mapped_raw_log_t *mapped_raw_log,
    const char *path,
    bool use_sparse_files,
    uint64_t term_length,
    uint64_t page_size)
{
    const uint64_t log_length = aeron_logbuffer_compute_log_length(term_length, page_size);

    int fd = aeron_create_file(path, (size_t)log_length);
    if (-1 == fd)
    {
        return -1;
    }

    mapped_raw_log->mapped_file.length = (size_t)log_length;
    mapped_raw_log->mapped_file.addr = NULL;

    if (0 != aeron_mmap(&mapped_raw_log->mapped_file, fd, !use_sparse_files, path))
    {
        if (-1 == remove(path))
        {
            AERON_APPEND_ERR("Failed to remove raw log, filename: %s", path);
        }
        return -1;
    }

#ifndef AERON_NATIVE_PRETOUCH
    if (!use_sparse_files)
    {
        aeron_touch_pages(mapped_raw_log->mapped_file.addr, (size_t)log_length, (size_t)page_size);
    }
#endif

    for (size_t i = 0; i < AERON_LOGBUFFER_PARTITION_COUNT; i++)
    {
        mapped_raw_log->term_buffers[i].addr = (uint8_t *)mapped_raw_log->mapped_file.addr + (i * term_length);
        mapped_raw_log->term_buffers[i].length = (size_t)term_length;
    }

    mapped_raw_log->log_meta_data.addr =
        (uint8_t *)mapped_raw_log->mapped_file.addr + (log_length - AERON_LOGBUFFER_META_DATA_LENGTH);
    mapped_raw_log->log_meta_data.length = AERON_LOGBUFFER_META_DATA_LENGTH;
    mapped_raw_log->term_length = (size_t)term_length;

    return 0;
}

int aeron_raw_log_map_existing(aeron_mapped_raw_log_t *mapped_raw_log, const char *path, bool pre_touch)
{
    int fd = aeron_open_file_rw(path);
    if (fd < 0)
    {
        AERON_SET_ERR(errno, "Failed to open existing raw log, filename: %s", path);
        return -1;
    }

    const int64_t file_length = aeron_file_length(path);
    if (-1 == file_length)
    {
        AERON_SET_ERR(errno, "Failed to determine the size of the existing raw log, filename: %s", path);
        close(fd);
        return -1;
    }

    mapped_raw_log->mapped_file.length = file_length;
    mapped_raw_log->mapped_file.addr = NULL;

    if (0 != aeron_mmap(&mapped_raw_log->mapped_file, fd, pre_touch, path))
    {
        return -1;
    }

    mapped_raw_log->log_meta_data.addr =
        (uint8_t *)mapped_raw_log->mapped_file.addr +
        (mapped_raw_log->mapped_file.length - AERON_LOGBUFFER_META_DATA_LENGTH);
    mapped_raw_log->log_meta_data.length = AERON_LOGBUFFER_META_DATA_LENGTH;

    aeron_logbuffer_metadata_t *log_meta_data = (aeron_logbuffer_metadata_t *)mapped_raw_log->log_meta_data.addr;
    size_t term_length = (size_t)log_meta_data->term_length;
    size_t page_size = (size_t)log_meta_data->page_size;

    if (aeron_logbuffer_check_term_length(term_length) < 0 || aeron_logbuffer_check_page_size(page_size) < 0)
    {
        AERON_APPEND_ERR("Raw log metadata invalid, unmapping, filename: %s", path);
        aeron_unmap(&mapped_raw_log->mapped_file);
        return -1;
    }

    mapped_raw_log->term_length = term_length;

    for (size_t i = 0; i < AERON_LOGBUFFER_PARTITION_COUNT; i++)
    {
        mapped_raw_log->term_buffers[i].addr = (uint8_t *)mapped_raw_log->mapped_file.addr + (i * term_length);
        mapped_raw_log->term_buffers[i].length = term_length;
    }

#ifndef AERON_NATIVE_PRETOUCH
    if (pre_touch)
    {
        aeron_touch_pages(mapped_raw_log->mapped_file.addr, (size_t)file_length, (size_t)page_size);
    }
#endif

    return 0;
}

int aeron_raw_log_close(aeron_mapped_raw_log_t *mapped_raw_log, const char *filename)
{
    if (!aeron_raw_log_free(mapped_raw_log, filename))
    {
        AERON_SET_ERR(errno, "Failed to close raw log, filename: %s", filename);
        return -1;
    }

    return 0;
}

bool aeron_raw_log_free(aeron_mapped_raw_log_t *mapped_raw_log, const char *filename)
{
    if (NULL != mapped_raw_log->mapped_file.addr)
    {
        if (aeron_unmap(&mapped_raw_log->mapped_file) < 0)
        {
            return false;
        }

        mapped_raw_log->mapped_file.addr = NULL;
    }

    if (NULL != filename && mapped_raw_log->mapped_file.length > 0)
    {
        if (remove(filename) < 0 && aeron_file_length(filename) > 0)
        {
            return false;
        }

        mapped_raw_log->mapped_file.length = 0;
    }

    return true;
}

#if defined(__clang__)
    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wunused-function"
#endif

inline static const char *tmp_dir(void)
{
#if defined(_MSC_VER)
    static char buff[MAX_PATH + 1];

    if (GetTempPath(MAX_PATH, &buff[0]) > 0)
    {
        return buff;
    }

    return NULL;
#else
    const char *dir = "/tmp";
    const char *tmp_dir = getenv("TMPDIR");

    if (NULL != tmp_dir)
    {
        dir = tmp_dir;
    }

    return dir;
#endif
}

inline static bool has_file_separator_at_end(const char *path)
{
#if defined(_MSC_VER)
    const char last = path[strlen(path) - 1];
    return last == '\\' || last == '/';
#else
    return path[strlen(path) - 1] == '/';
#endif
}

#if defined(__clang__)
#pragma clang diagnostic pop
#endif

inline static const char *username(void)
{
#if (_MSC_VER)
    const char *username = getenv("USER");

    if (NULL == username)
    {
        username = getenv("USERNAME");
        if (NULL == username)
        {
             username = "default";
        }
    }

    return username;
#else
    static char static_buffer[16384];
    const char *username = getenv("USER");

    if (NULL == username)
    {
        uid_t uid = getuid(); // using uid instead of euid as that is what the JVM seems to do.
        struct passwd pw, *pw_result = NULL;

        int e = getpwuid_r(uid, &pw, static_buffer, sizeof(static_buffer), &pw_result);
        username = (0 == e && NULL != pw_result && NULL != pw_result->pw_name && '\0' != *(pw_result->pw_name)) ?
            pw_result->pw_name : "default";
    }

    return username;
#endif
}

int aeron_default_path(char *path, size_t path_length)
{
#if defined(__linux__)
    return snprintf(path, path_length, "/dev/shm/aeron-%s", username());
#elif defined(_MSC_VER)
    return snprintf(
        path, path_length, "%s%saeron-%s", tmp_dir(), has_file_separator_at_end(tmp_dir()) ? "" : "\\", username());
#else
    return snprintf(
        path, path_length, "%s%saeron-%s", tmp_dir(), has_file_separator_at_end(tmp_dir()) ? "" : "/", username());
#endif
}

int aeron_file_resolve(const char *parent, const char *child, char *buffer, size_t buffer_len)
{
    int result = snprintf(buffer, buffer_len, "%s%c%s", parent, AERON_FILE_SEP, child);
    buffer[buffer_len - 1] = '\0';

    if (result < 0)
    {
        AERON_SET_ERR(errno, "%s", "Failed to format resolved path");
        return -1;
    }
    else if ((int)buffer_len <= result)
    {
        AERON_SET_ERR(
            EINVAL,
            "Path name was truncated, required: %d, supplied: %d, result: %s",
            result,
            (int)buffer_len,
            buffer);
        return -1;
    }

    return result;
}
