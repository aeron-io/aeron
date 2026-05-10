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

#ifndef AERON_FILEUTIL_H
#define AERON_FILEUTIL_H

#include <stddef.h>
#include <sys/types.h>

#include "util/aeron_platform.h"
#include "concurrent/aeron_logbuffer_descriptor.h"

#define AERON_MAX_FILE_PATH_LENGTH (4096)

typedef struct aeron_mapped_file_stct
{
    void *addr;
    size_t length;
}
aeron_mapped_file_t;

typedef struct aeron_mapped_buffer_stct
{
    uint8_t *addr;
    size_t length;
}
aeron_mapped_buffer_t;

int aeron_is_directory(const char *path);
int aeron_delete_directory(const char *directory);
int aeron_mkdir_recursive(const char *pathname, int permission);

int aeron_map_new_file(aeron_mapped_file_t *mapped_file, const char *path, bool fill_with_zeroes);
int aeron_map_existing_file(aeron_mapped_file_t *mapped_file, const char *path);
int aeron_unmap(aeron_mapped_file_t *mapped_file);

int aeron_msync(void *addr, size_t length);
int aeron_delete_file(const char *path);
const char *aeron_realpath(const char *path, char *resolved_path, size_t resolved_path_len);
const char *aeron_tmpdir(char *path, size_t path_len);
bool aeron_file_exists(const char* path);

#if defined(AERON_COMPILER_GCC)
#include <unistd.h>

#define AERON_FILEUTIL_ERROR_ENOSPC ENOSPC

#define aeron_mkdir mkdir
#elif defined(AERON_COMPILER_MSVC)
#define _CRT_RAND_S
#include <io.h>
#include <direct.h>
#include <process.h>
#include <winsock2.h>
#include <windows.h>

#define S_IRWXU 0
#define S_IRWXG 0
#define S_IRWXO 0

#define AERON_FILEUTIL_ERROR_ENOSPC ERROR_DISK_FULL

int aeron_mkdir(const char *path, int permission);
#endif

typedef uint64_t (*aeron_usable_fs_space_func_t)(const char *path);

int64_t aeron_file_length(const char *path);
uint64_t aeron_usable_fs_space(const char *path);
uint64_t aeron_usable_fs_space_disabled(const char *path);

typedef struct aeron_mapped_raw_log_stct
{
    aeron_mapped_buffer_t term_buffers[AERON_LOGBUFFER_PARTITION_COUNT];
    aeron_mapped_buffer_t log_meta_data;
    aeron_mapped_file_t mapped_file;
    size_t term_length;
}
aeron_mapped_raw_log_t;

#define AERON_PUBLICATIONS_DIR "publications"
#define AERON_IMAGES_DIR "images"

int aeron_ipc_publication_location(char *dst, size_t length, const char *aeron_dir, int64_t correlation_id);

int aeron_network_publication_location(char *dst, size_t length, const char *aeron_dir, int64_t correlation_id);

int aeron_publication_image_location(char *dst, size_t length, const char *aeron_dir, int64_t correlation_id);

size_t aeron_temp_filename(char *filename, size_t length);

typedef int (*aeron_raw_log_map_func_t)(aeron_mapped_raw_log_t *, const char *, bool, uint64_t, uint64_t);
typedef int (*aeron_raw_log_close_func_t)(aeron_mapped_raw_log_t *, const char *filename);
typedef bool (*aeron_raw_log_free_func_t)(aeron_mapped_raw_log_t *, const char *filename);

int aeron_raw_log_map(
    aeron_mapped_raw_log_t *mapped_raw_log,
    const char *path,
    bool use_sparse_files,
    uint64_t term_length,
    uint64_t page_size);

int aeron_raw_log_map_existing(aeron_mapped_raw_log_t *mapped_raw_log, const char *path, bool pre_touch);

int aeron_raw_log_close(aeron_mapped_raw_log_t *mapped_raw_log, const char *filename);

bool aeron_raw_log_free(aeron_mapped_raw_log_t *mapped_raw_log, const char *filename);

int aeron_file_resolve(const char *parent, const char *child, char *buffer, size_t buffer_len);

#endif //AERON_FILEUTIL_H
