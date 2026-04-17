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

#ifndef AERON_COUNTERS_MANAGER_H
#define AERON_COUNTERS_MANAGER_H

#include "aeronc.h"
#include "util/aeron_bitutil.h"
#include "util/aeron_clock.h"
#include "aeron_atomic.h"

#define AERON_COUNTERS_MANAGER_VALUE_LENGTH (sizeof(aeron_counter_value_descriptor_t))
#define AERON_COUNTERS_MANAGER_METADATA_LENGTH (sizeof(aeron_counter_metadata_descriptor_t))

#define AERON_COUNTERS_METADATA_BUFFER_LENGTH(v) \
((v) * (AERON_COUNTERS_MANAGER_METADATA_LENGTH / AERON_COUNTERS_MANAGER_VALUE_LENGTH))

#define AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_INITIALIZING (0)
#define AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ERRORED (-1)
#define AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_NO_ID_ALLOCATED (-1)
#define AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE INT64_C(1)
#define AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_CLOSING INT64_C(2)

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_stream_position_counter_key_layout_stct
{
    int64_t registration_id;
    int32_t session_id;
    int32_t stream_id;
    int32_t channel_length;
    char channel[sizeof(((aeron_counter_metadata_descriptor_t *)0)->key) - (sizeof(int64_t) + 3 * sizeof(int32_t))];
}
aeron_stream_position_counter_key_layout_t;

typedef struct aeron_channel_endpoint_status_key_layout_stct
{
    int32_t channel_length;
    char channel[sizeof(((aeron_counter_metadata_descriptor_t *)0)->key) - sizeof(int32_t)];
}
aeron_channel_endpoint_status_key_layout_t;

typedef struct aeron_heartbeat_timestamp_key_layout_stct
{
    int64_t registration_id;
}
aeron_heartbeat_timestamp_key_layout_t;

typedef struct aeron_local_sockaddr_key_layout_stct
{
    int32_t channel_status_id;
    int32_t local_sockaddr_len;
    char local_sockaddr[sizeof(((aeron_counter_metadata_descriptor_t *)0)->key) - (2 * sizeof(int32_t))];
}
aeron_local_sockaddr_key_layout_t;

#pragma pack(pop)

typedef struct aeron_counters_manager_stct
{
    uint8_t *values;
    uint8_t *metadata;
    size_t values_length;
    size_t metadata_length;

    int32_t max_counter_id;
    int32_t id_high_water_mark;
    int32_t *free_list;
    int32_t free_list_index;
    size_t free_list_length;

    aeron_clock_cache_t *cached_clock;
    int64_t free_to_reuse_timeout_ms;
}
aeron_counters_manager_t;

typedef struct aeron_counters_reader_stct
{
    uint8_t *values;
    uint8_t *metadata;
    size_t values_length;
    size_t metadata_length;
    int32_t max_counter_id;
}
aeron_counters_reader_t;

#define AERON_COUNTERS_MANAGER_IS_BUFFER_LENGTHS_VALID(metadata, values) \
    ((metadata) >= ((values) * (AERON_COUNTERS_MANAGER_METADATA_LENGTH / AERON_COUNTERS_MANAGER_VALUE_LENGTH)))

int aeron_counters_manager_init(
    aeron_counters_manager_t *manager,
    uint8_t *metadata_buffer,
    size_t metadata_length,
    uint8_t *values_buffer,
    size_t values_length,
    aeron_clock_cache_t *cached_clock,
    int64_t free_to_reuse_timeout_ms);

void aeron_counters_manager_close(aeron_counters_manager_t *manager);

int32_t aeron_counters_manager_allocate(
    aeron_counters_manager_t *manager,
    int32_t type_id,
    const uint8_t *key,
    size_t key_length,
    const char *label,
    size_t label_length);

void aeron_counters_manager_counter_registration_id(
    aeron_counters_manager_t *manager, int32_t counter_id, int64_t registration_id);

void aeron_counters_manager_counter_owner_id(
    aeron_counters_manager_t *manager, int32_t counter_id, int64_t owner_id);

void aeron_counters_manager_counter_reference_id(
    aeron_counters_manager_t *manager, int32_t counter_id, int64_t reference_id);

void aeron_counters_manager_update_label(
    aeron_counters_manager_t *manager, int32_t counter_id, size_t label_length, const char *label);

void aeron_counters_manager_append_to_label(
    aeron_counters_manager_t *manager, int32_t counter_id, size_t length, const char *value);

int32_t aeron_counters_manager_next_counter_id(aeron_counters_manager_t *manager);

int aeron_counters_manager_free(aeron_counters_manager_t *manager, int32_t counter_id);

typedef void (*aeron_counters_reader_foreach_metadata_func_t)(
    int32_t id,
    int32_t type_id,
    const uint8_t *key,
    size_t key_length,
    const uint8_t *label,
    size_t label_length,
    void *clientd);

void aeron_counters_reader_foreach_metadata(
    uint8_t *metadata_buffer,
    size_t metadata_length,
    aeron_counters_reader_foreach_metadata_func_t func,
    void *clientd);

inline int64_t *aeron_counters_manager_addr(aeron_counters_manager_t *counters_manager, int32_t counter_id)
{
    return (int64_t *)(counters_manager->values + AERON_COUNTER_OFFSET(counter_id));
}

inline int aeron_counters_reader_init(
    aeron_counters_reader_t *reader,
    uint8_t *metadata_buffer,
    size_t metadata_length,
    uint8_t *values_buffer,
    size_t values_length)
{
    reader->metadata = metadata_buffer;
    reader->metadata_length = metadata_length;
    reader->values = values_buffer;
    reader->values_length = values_length;
    reader->max_counter_id = (int32_t)((values_length / AERON_COUNTERS_MANAGER_VALUE_LENGTH) - 1);

    return 0;
}

inline void aeron_counter_set_release(volatile int64_t *addr, int64_t value)
{
    AERON_SET_RELEASE(*addr, value);
}

inline int64_t aeron_counter_get_plain(volatile int64_t *addr)
{
    return *addr;
}

inline int64_t aeron_counter_get_acquire(volatile int64_t *addr)
{
    int64_t value;
    AERON_GET_ACQUIRE(value, *addr);
    return value;
}

inline int64_t aeron_counter_increment(volatile int64_t *addr)
{
    int64_t result;
    AERON_GET_AND_ADD_INT64(result, *addr, 1);
    return result;
}

inline int64_t aeron_counter_increment_release(volatile int64_t *addr)
{
    int64_t current_value = *addr;
    AERON_SET_RELEASE(*addr, current_value + 1);
    return current_value;
}

inline int64_t aeron_counter_increment_plain(int64_t *addr)
{
    int64_t current_value = *addr;
    *addr = current_value + 1;
    return current_value;
}

inline int64_t aeron_counter_increment_opaque(volatile int64_t *addr)
{
    int64_t current_value = *addr;
    *addr = current_value + 1;
    return current_value;
}

inline int64_t aeron_counter_get_and_add(volatile int64_t *addr, int64_t value)
{
    int64_t result;
    AERON_GET_AND_ADD_INT64(result, *addr, value);
    return result;
}

inline int64_t aeron_counter_get_and_add_release(volatile int64_t *addr, int64_t value)
{
    int64_t current = *addr;
    AERON_SET_RELEASE(*addr, current + value);
    return current;
}

inline int64_t aeron_counter_get_and_add_plain(volatile int64_t *addr, int64_t value)
{
    int64_t current = *addr;
    *addr = current + value;
    return current;
}

inline bool aeron_counter_propose_max_release(volatile int64_t *addr, int64_t proposed_value)
{
    bool updated = false;

    if (*addr < proposed_value)
    {
        AERON_SET_RELEASE(*addr, proposed_value);
        updated = true;
    }

    return updated;
}

inline bool aeron_counter_propose_max_plain(volatile int64_t *addr, int64_t proposed_value)
{
    bool updated = false;

    if (*addr < proposed_value)
    {
        *addr = proposed_value;
        updated = true;
    }

    return updated;
}

#endif //AERON_COUNTERS_MANAGER_H
