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

#ifndef AERON_C_CNC_FILE_DESCRIPTOR_H
#define AERON_C_CNC_FILE_DESCRIPTOR_H

#include "aeronc.h"
#include "util/aeron_bitutil.h"
#include "concurrent/aeron_atomic.h"
#include "util/aeron_fileutil.h"

#define AERON_CNC_FILE "cnc.dat"
#define AERON_CNC_VERSION (aeron_semantic_version_compose(0, 2, 0))

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_cnc_metadata_stct
{
    volatile int32_t cnc_version;
    int32_t to_driver_buffer_length;
    int32_t to_clients_buffer_length;
    int32_t counter_metadata_buffer_length;
    int32_t counter_values_buffer_length;
    int32_t error_log_buffer_length;
    int64_t client_liveness_timeout;
    int64_t start_timestamp;
    int64_t pid;
    int32_t file_page_size;
}
aeron_cnc_metadata_t;
#pragma pack(pop)

typedef enum aeron_cnc_load_result_stct
{
    AERON_CNC_LOAD_FAILED = -1,
    AERON_CNC_LOAD_SUCCESS = 0,
    AERON_CNC_LOAD_AWAIT_FILE = 1,
    AERON_CNC_LOAD_AWAIT_MMAP = 2,
    AERON_CNC_LOAD_AWAIT_VERSION = 3,
    AERON_CNC_LOAD_AWAIT_CNC_DATA = 4,
}
aeron_cnc_load_result_t;

#define AERON_CNC_VERSION_AND_META_DATA_LENGTH UINT32_C(AERON_CACHE_LINE_LENGTH * 2)

inline uint8_t *aeron_cnc_to_driver_buffer(aeron_cnc_metadata_t *metadata)
{
    return (uint8_t *)metadata + AERON_CNC_VERSION_AND_META_DATA_LENGTH;
}

inline uint8_t *aeron_cnc_to_clients_buffer(aeron_cnc_metadata_t *metadata)
{
    return (uint8_t *)metadata + AERON_CNC_VERSION_AND_META_DATA_LENGTH + metadata->to_driver_buffer_length;
}

inline uint8_t *aeron_cnc_counters_metadata_buffer(aeron_cnc_metadata_t *metadata)
{
    return (uint8_t *)metadata + AERON_CNC_VERSION_AND_META_DATA_LENGTH +
        metadata->to_driver_buffer_length +
        metadata->to_clients_buffer_length;
}

inline uint8_t *aeron_cnc_counters_values_buffer(aeron_cnc_metadata_t *metadata)
{
    return (uint8_t *)metadata + AERON_CNC_VERSION_AND_META_DATA_LENGTH +
        metadata->to_driver_buffer_length +
        metadata->to_clients_buffer_length +
        metadata->counter_metadata_buffer_length;
}

inline uint8_t *aeron_cnc_error_log_buffer(aeron_cnc_metadata_t *metadata)
{
    return (uint8_t *)metadata + AERON_CNC_VERSION_AND_META_DATA_LENGTH +
        metadata->to_driver_buffer_length +
        metadata->to_clients_buffer_length +
        metadata->counter_metadata_buffer_length +
        metadata->counter_values_buffer_length;
}

inline size_t aeron_cnc_computed_length(size_t total_length_of_buffers, size_t alignment)
{
    return AERON_ALIGN(AERON_CNC_VERSION_AND_META_DATA_LENGTH + total_length_of_buffers, alignment);
}

inline bool aeron_cnc_is_file_length_sufficient(aeron_mapped_file_t *cnc_mmap)
{
    if (cnc_mmap->length < AERON_CNC_VERSION_AND_META_DATA_LENGTH)
    {
        return false;
    }

    aeron_cnc_metadata_t *metadata = (aeron_cnc_metadata_t *)cnc_mmap->addr;
    size_t cnc_length = AERON_CNC_VERSION_AND_META_DATA_LENGTH +
        (size_t)metadata->to_driver_buffer_length +
        (size_t)metadata->to_clients_buffer_length +
        (size_t)metadata->counter_metadata_buffer_length +
        (size_t)metadata->counter_values_buffer_length;

    return cnc_mmap->length >= cnc_length;
}

int32_t aeron_cnc_version_volatile(aeron_cnc_metadata_t *metadata);

aeron_cnc_load_result_t aeron_cnc_map_file_and_load_metadata(
    const char *dir, aeron_mapped_file_t *mapped_file, aeron_cnc_metadata_t **metadata);

int aeron_cnc_resolve_filename(const char *directory, char *filename_buffer, size_t filename_buffer_length);

#endif //AERON_C_CNC_FILE_DESCRIPTOR_H
