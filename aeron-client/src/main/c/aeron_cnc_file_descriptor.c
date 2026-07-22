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
#define _BSD_SOURCE
#define _GNU_SOURCE
#ifdef HAVE_BSDSTDLIB_H
#include <bsd/stdlib.h>
#endif
#endif

#include <stdint.h>
#include <errno.h>

#include "aeron_common.h"
#include "util/aeron_error.h"
#include "aeron_cnc_file_descriptor.h"

int32_t aeron_cnc_version_volatile(aeron_cnc_metadata_t *metadata)
{
    int32_t cnc_version;
    AERON_GET_ACQUIRE(cnc_version, metadata->cnc_version);
    return cnc_version;
}

static bool aeron_cnc_map_file_is_retry_err(int err)
{
#if defined(AERON_COMPILER_MSVC)
    return
        ERROR_FILE_NOT_FOUND == err ||
        ERROR_PATH_NOT_FOUND == err ||
        ERROR_ACCESS_DENIED == err ||
        ERROR_SHARING_VIOLATION == err;
#else
    return
        ENOENT == err ||
        EACCES == err;
#endif
}

aeron_cnc_load_result_t aeron_cnc_map_file_and_load_metadata(
    const char *dir, aeron_mapped_file_t *cnc_mmap, aeron_cnc_metadata_t **metadata, bool read_only)
{
    if (NULL == metadata)
    {
        AERON_SET_ERR(EINVAL, "%s", "CnC metadata pointer must not be NULL");
        return AERON_CNC_LOAD_FAILED;
    }

    char filename[AERON_MAX_PATH];
    if (aeron_cnc_resolve_filename(dir, filename, sizeof(filename)) < 0)
    {
        AERON_APPEND_ERR("Failed to resolve CnC file path: dir=%s, filename=%s", dir, filename);
        return AERON_CNC_LOAD_FAILED;
    }

    if (aeron_file_length(filename) <= (int64_t)AERON_CNC_VERSION_AND_META_DATA_LENGTH)
    {
        return AERON_CNC_LOAD_AWAIT_FILE;
    }

    if (aeron_map_existing_file(cnc_mmap, filename, read_only) < 0)
    {
        if (aeron_cnc_map_file_is_retry_err(aeron_errcode()))
        {
            aeron_err_clear();
            return AERON_CNC_LOAD_AWAIT_FILE;
        }

        AERON_APPEND_ERR("CnC file could not be memory mapped: %s", filename);
        return AERON_CNC_LOAD_FAILED;
    }

    if (cnc_mmap->length <= (int64_t)AERON_CNC_VERSION_AND_META_DATA_LENGTH)
    {
        aeron_unmap(cnc_mmap);
        return AERON_CNC_LOAD_AWAIT_MMAP;
    }

    aeron_cnc_metadata_t *_metadata = (aeron_cnc_metadata_t *)cnc_mmap->addr;
    int32_t cnc_version = aeron_cnc_version_volatile(_metadata);

    if (0 == cnc_version)
    {
        aeron_unmap(cnc_mmap);
        return AERON_CNC_LOAD_AWAIT_VERSION;
    }

    if (aeron_semantic_version_major(AERON_CNC_VERSION) != aeron_semantic_version_major(cnc_version))
    {
        AERON_SET_ERR(
            EINVAL,
            "CnC version not compatible: app=%d.%d.%d file=%d.%d.%d",
            (int)aeron_semantic_version_major(AERON_CNC_VERSION),
            (int)aeron_semantic_version_minor(AERON_CNC_VERSION),
            (int)aeron_semantic_version_patch(AERON_CNC_VERSION),
            (int)aeron_semantic_version_major(cnc_version),
            (int)aeron_semantic_version_minor(cnc_version),
            (int)aeron_semantic_version_patch(cnc_version));
        aeron_unmap(cnc_mmap);

        return AERON_CNC_LOAD_FAILED;
    }

    if (!aeron_cnc_is_file_length_sufficient(cnc_mmap))
    {
        aeron_unmap(cnc_mmap);
        return AERON_CNC_LOAD_AWAIT_CNC_DATA;
    }

    *metadata = _metadata;

    return AERON_CNC_LOAD_SUCCESS;
}

int aeron_cnc_resolve_filename(const char *directory, char *filename_buffer, size_t filename_buffer_length)
{
    return aeron_file_resolve(directory, AERON_CNC_FILE, filename_buffer, filename_buffer_length);
}

extern uint8_t *aeron_cnc_to_driver_buffer(aeron_cnc_metadata_t *metadata);
extern uint8_t *aeron_cnc_to_clients_buffer(aeron_cnc_metadata_t *metadata);
extern uint8_t *aeron_cnc_counters_metadata_buffer(aeron_cnc_metadata_t *metadata);
extern uint8_t *aeron_cnc_counters_values_buffer(aeron_cnc_metadata_t *metadata);
extern uint8_t *aeron_cnc_error_log_buffer(aeron_cnc_metadata_t *metadata);
extern size_t aeron_cnc_computed_length(size_t total_length_of_buffers, size_t alignment);
extern bool aeron_cnc_is_file_length_sufficient(aeron_mapped_file_t *cnc_mmap);
