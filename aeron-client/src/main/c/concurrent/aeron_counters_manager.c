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

#include <string.h>
#include <math.h>
#include <errno.h>
#include <inttypes.h>

#include "aeron_alloc.h"
#include "concurrent/aeron_counters_manager.h"
#include "util/aeron_error.h"

_Static_assert(
    sizeof(aeron_counters_manager_t) > sizeof(aeron_counters_reader_t),
    "sizeof(aeron_counters_manager_t) must be greater than sizeof(aeron_counters_reader_t)");
_Static_assert(
    offsetof(aeron_counters_manager_t, values) == offsetof(aeron_counters_reader_t, values),
    "offsetof(aeron_counters_manager_t, values) must match offsetof(aeron_counters_reader_t, values)");
_Static_assert(
    offsetof(aeron_counters_manager_t, metadata) == offsetof(aeron_counters_reader_t, metadata),
    "offsetof(aeron_counters_manager_t, metadata) must match offsetof(aeron_counters_reader_t, metadata)");
_Static_assert(
    offsetof(aeron_counters_manager_t, values_length) == offsetof(aeron_counters_reader_t, values_length),
    "offsetof(aeron_counters_manager_t, values_length) must match offsetof(aeron_counters_reader_t, values_length)");
_Static_assert(
    offsetof(aeron_counters_manager_t, metadata_length) == offsetof(aeron_counters_reader_t, metadata_length),
    "offsetof(aeron_counters_manager_t, metadata_length) must match offsetof(aeron_counters_reader_t, metadata_length)");
_Static_assert(
    offsetof(aeron_counters_manager_t, max_counter_id) == offsetof(aeron_counters_reader_t, max_counter_id),
    "offsetof(aeron_counters_manager_t, max_counter_id) must match offsetof(aeron_counters_reader_t, max_counter_id)");

int aeron_counters_manager_init(
    aeron_counters_manager_t *manager,
    uint8_t *metadata_buffer,
    size_t metadata_length,
    uint8_t *values_buffer,
    size_t values_length,
    aeron_clock_cache_t *cached_clock,
    int64_t free_to_reuse_timeout_ms)
{
    int result = -1;

    if (AERON_COUNTERS_MANAGER_IS_BUFFER_LENGTHS_VALID(metadata_length, values_length))
    {
        manager->metadata = metadata_buffer;
        manager->metadata_length = metadata_length;
        manager->values = values_buffer;
        manager->values_length = values_length;
        manager->max_counter_id = (int32_t)((values_length / AERON_COUNTERS_MANAGER_VALUE_LENGTH) - 1);
        manager->id_high_water_mark = -1;
        manager->free_list_index = -1;
        manager->free_list_length = 2;
        manager->cached_clock = cached_clock;
        manager->free_to_reuse_timeout_ms = free_to_reuse_timeout_ms;
        result = aeron_alloc((void **)&manager->free_list, sizeof(int32_t) * manager->free_list_length);
    }
    else
    {
        AERON_SET_ERR(
            EINVAL,
            "Counter buffer lengths invalid, metadata_length: %" PRIu64 ", values_length: %" PRIu64,
            (uint64_t)metadata_length,
            (uint64_t)values_length);
    }

    return result;
}

void aeron_counters_manager_close(aeron_counters_manager_t *manager)
{
    aeron_free(manager->free_list);
}

int32_t aeron_counters_manager_allocate(
    aeron_counters_manager_t *manager,
    int32_t type_id,
    const uint8_t *key,
    size_t key_length,
    const char *label,
    size_t label_length)
{
    const int32_t counter_id = aeron_counters_manager_next_counter_id(manager);
    if (counter_id < 0)
    {
        AERON_APPEND_ERR(
            "Unable to allocate counter: type: %" PRId32 ", label: %.*s",
            type_id,
            (int)label_length,
            label);
        return -1;
    }

    aeron_counter_metadata_descriptor_t *metadata = (aeron_counter_metadata_descriptor_t *)
        (manager->metadata + (counter_id * AERON_COUNTERS_MANAGER_METADATA_LENGTH));

    metadata->type_id = type_id;
    metadata->free_for_reuse_deadline_ms = AERON_COUNTER_NOT_FREE_TO_REUSE;

    if (NULL != key && key_length > 0)
    {
        memcpy(metadata->key, key, (size_t)fmin((double)sizeof(metadata->key), (double)key_length));
    }

    size_t length = (size_t)fmin((double)sizeof(metadata->label), (double)label_length);

    memcpy(metadata->label, label, length);
    metadata->label_length = (int32_t)length;
    AERON_SET_RELEASE(metadata->state, AERON_COUNTER_RECORD_ALLOCATED);

    return counter_id;
}

int aeron_counters_reader_get_buffers(aeron_counters_reader_t *reader, aeron_counters_reader_buffers_t *buffers)
{
    buffers->values = reader->values;
    buffers->metadata = reader->metadata;
    buffers->values_length = reader->values_length;
    buffers->metadata_length = reader->metadata_length;

    return 0;
}

int32_t aeron_counters_reader_max_counter_id(aeron_counters_reader_t *reader)
{
    return reader->max_counter_id;
}

void aeron_counters_manager_counter_registration_id(
    aeron_counters_manager_t *manager, int32_t counter_id, int64_t registration_id)
{
    aeron_counter_value_descriptor_t *value_descriptor = (aeron_counter_value_descriptor_t *)(
        manager->values + AERON_COUNTER_OFFSET(counter_id));

    AERON_SET_RELEASE(value_descriptor->registration_id, registration_id);
}

void aeron_counters_manager_counter_owner_id(
    aeron_counters_manager_t *manager, int32_t counter_id, int64_t owner_id)
{
    aeron_counter_value_descriptor_t *value_descriptor = (aeron_counter_value_descriptor_t *)(
        manager->values + AERON_COUNTER_OFFSET(counter_id));

    value_descriptor->owner_id = owner_id;
}

void aeron_counters_manager_counter_reference_id(
    aeron_counters_manager_t *manager, int32_t counter_id, int64_t reference_id)
{
    aeron_counter_value_descriptor_t *value_descriptor = (aeron_counter_value_descriptor_t *)(
        manager->values + AERON_COUNTER_OFFSET(counter_id));

    value_descriptor->reference_id = reference_id;
}

void aeron_counters_manager_update_label(
    aeron_counters_manager_t *manager, int32_t counter_id, size_t label_length, const char *label)
{
    aeron_counter_metadata_descriptor_t *metadata = (aeron_counter_metadata_descriptor_t *)
        (manager->metadata + (counter_id * AERON_COUNTERS_MANAGER_METADATA_LENGTH));

    size_t length = AERON_MIN(sizeof(metadata->label), label_length);

    memcpy(metadata->label, label, length);
    AERON_SET_RELEASE(metadata->label_length, (int32_t)length);
}

void aeron_counters_manager_append_to_label(
    aeron_counters_manager_t *manager, int32_t counter_id, size_t length, const char *value)
{
    aeron_counter_metadata_descriptor_t *metadata = (aeron_counter_metadata_descriptor_t *)
        (manager->metadata + (counter_id * AERON_COUNTERS_MANAGER_METADATA_LENGTH));

    int32_t label_length;
    AERON_GET_ACQUIRE(label_length, metadata->label_length);

    size_t current_length = (size_t)label_length;
    size_t available_length = sizeof(metadata->label) - current_length;
    size_t copy_length = AERON_MIN(length, available_length);

    memcpy(metadata->label + current_length, value, copy_length);
    AERON_SET_RELEASE(metadata->label_length, (int32_t)(current_length + copy_length));
}

void aeron_counters_manager_remove_free_list_index(aeron_counters_manager_t *manager, int index)
{
    for (int i = index; i < manager->free_list_index; i++)
    {
        manager->free_list[i] = manager->free_list[i + 1];
    }

    manager->free_list_index--;
}

int32_t aeron_counters_manager_next_counter_id(aeron_counters_manager_t *manager)
{
    if (manager->free_list_index > -1)
    {
        int64_t now_ms = aeron_clock_cached_epoch_time(manager->cached_clock);

        for (int i = 0; i <= manager->free_list_index; i++)
        {
            int32_t counter_id = manager->free_list[i];
            aeron_counter_metadata_descriptor_t *metadata = (aeron_counter_metadata_descriptor_t *)
                (manager->metadata + (counter_id * AERON_COUNTERS_MANAGER_METADATA_LENGTH));

            int64_t deadline_ms;
            AERON_GET_ACQUIRE(deadline_ms, metadata->free_for_reuse_deadline_ms);

            if (now_ms >= deadline_ms)
            {
                aeron_counters_manager_remove_free_list_index(manager, i);
                aeron_counter_value_descriptor_t *value = (aeron_counter_value_descriptor_t *)
                    (manager->values + (counter_id * AERON_COUNTERS_MANAGER_VALUE_LENGTH));
                AERON_SET_RELEASE(value->registration_id, AERON_COUNTER_REGISTRATION_ID_DEFAULT);
                value->owner_id = AERON_COUNTER_OWNER_ID_DEFAULT;
                value->reference_id = AERON_COUNTER_REFERENCE_ID_DEFAULT;
                AERON_SET_RELEASE(value->counter_value, INT64_C(0));

                return counter_id;
            }
        }
    }

    if ((manager->id_high_water_mark + 1) > manager->max_counter_id)
    {
        AERON_SET_ERR(ENOMEM, "Max counter id (%" PRId32 ") exceeded", manager->max_counter_id);
        return -1;
    }

    return ++manager->id_high_water_mark;
}

int aeron_counters_manager_free(aeron_counters_manager_t *manager, int32_t counter_id)
{
    if (counter_id < 0 || counter_id > manager->max_counter_id)
    {
        return -1;
    }

    aeron_counter_metadata_descriptor_t *metadata = (aeron_counter_metadata_descriptor_t *)
        (manager->metadata + (counter_id * AERON_COUNTERS_MANAGER_METADATA_LENGTH));

    if (AERON_COUNTER_RECORD_ALLOCATED != metadata->state)
    {
        return -1;
    }

    AERON_SET_RELEASE(metadata->state, AERON_COUNTER_RECORD_RECLAIMED);
    memset(metadata->key, 0, sizeof(metadata->key));
    metadata->free_for_reuse_deadline_ms =
        aeron_clock_cached_epoch_time(manager->cached_clock) + manager->free_to_reuse_timeout_ms;

    if ((manager->free_list_index + 1) >= (int32_t)manager->free_list_length)
    {
        size_t new_length = manager->free_list_length + (manager->free_list_length >> 1u);

        if (aeron_reallocf((void **)&manager->free_list, sizeof(int32_t) * new_length) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        manager->free_list_length = new_length;
    }

    manager->free_list[++manager->free_list_index] = counter_id;

    return 0;
}

void aeron_counters_reader_foreach_metadata(
    uint8_t *metadata_buffer,
    size_t metadata_length,
    aeron_counters_reader_foreach_metadata_func_t func,
    void *clientd)
{
    int32_t id = 0;

    for (size_t i = 0; i < metadata_length; i += AERON_COUNTERS_MANAGER_METADATA_LENGTH)
    {
        aeron_counter_metadata_descriptor_t *record = (aeron_counter_metadata_descriptor_t *)(metadata_buffer + i);
        int32_t record_state;

        AERON_GET_ACQUIRE(record_state, record->state);

        if (AERON_COUNTER_RECORD_ALLOCATED == record_state)
        {
            int32_t label_length;

            AERON_GET_ACQUIRE(label_length, record->label_length);

            func(
                id,
                record->type_id,
                record->key,
                sizeof(record->key),
                record->label,
                (size_t)label_length,
                clientd);
        }
        else if (AERON_COUNTER_RECORD_UNUSED == record_state)
        {
            break;
        }

        id++;
    }
}

void aeron_counters_reader_foreach_counter(
    aeron_counters_reader_t *counters_reader, aeron_counters_reader_foreach_counter_func_t func, void *clientd)
{
    int32_t id = 0;

    for (size_t i = 0; i < counters_reader->metadata_length; i += AERON_COUNTERS_MANAGER_METADATA_LENGTH)
    {
        aeron_counter_metadata_descriptor_t *record = (aeron_counter_metadata_descriptor_t *)(
            counters_reader->metadata + i);
        int32_t record_state;

        AERON_GET_ACQUIRE(record_state, record->state);

        if (AERON_COUNTER_RECORD_ALLOCATED == record_state)
        {
            int64_t *value_addr = (int64_t *)(counters_reader->values + AERON_COUNTER_OFFSET(id));
            int32_t label_length;

            AERON_GET_ACQUIRE(label_length, record->label_length);

            func(
                aeron_counter_get_acquire(value_addr),
                id,
                record->type_id,
                (const uint8_t *)record->key,
                sizeof(record->key),
                (const char *)record->label,
                (size_t)label_length,
                clientd);
        }
        else if (AERON_COUNTER_RECORD_UNUSED == record_state)
        {
            break;
        }

        id++;
    }
}

int32_t aeron_counters_reader_find_by_type_id_and_registration_id(
    aeron_counters_reader_t *counters_reader,
    int32_t type_id,
    int64_t registration_id)
{
    int32_t counter_id = 0;
    for (size_t i = 0; i < counters_reader->metadata_length; i += AERON_COUNTERS_MANAGER_METADATA_LENGTH)
    {
        aeron_counter_metadata_descriptor_t *record = (aeron_counter_metadata_descriptor_t *)(counters_reader->metadata + i);
        int32_t record_state;

        AERON_GET_ACQUIRE(record_state, record->state);

        if (AERON_COUNTER_RECORD_ALLOCATED == record_state)
        {
            if (type_id == record->type_id)
            {
                int64_t counter_registration_id;
                aeron_counter_value_descriptor_t *value_descriptor = (aeron_counter_value_descriptor_t *)(
                    counters_reader->values + AERON_COUNTER_OFFSET(counter_id));

                AERON_GET_ACQUIRE(counter_registration_id, value_descriptor->registration_id);

                if (registration_id == counter_registration_id)
                {
                    return counter_id;
                }
            }
        }
        else if (AERON_COUNTER_RECORD_UNUSED == record_state)
        {
            break;
        }

        counter_id++;
    }

    return AERON_NULL_COUNTER_ID;
}

int64_t *aeron_counters_reader_addr(aeron_counters_reader_t *counters_reader, int32_t counter_id)
{
    if (counter_id < 0)
    {
        return NULL;
    }

    return (int64_t *)(counters_reader->values + AERON_COUNTER_OFFSET(counter_id));
}

int aeron_counters_reader_counter_registration_id(
    aeron_counters_reader_t *counters_reader, int32_t counter_id, int64_t *registration_id)
{
    if (counter_id < 0 || counter_id > counters_reader->max_counter_id)
    {
        return -1;
    }

    aeron_counter_value_descriptor_t *value_descriptor = (aeron_counter_value_descriptor_t *)(
        counters_reader->values + AERON_COUNTER_OFFSET(counter_id));

    AERON_GET_ACQUIRE(*registration_id, value_descriptor->registration_id);

    return 0;
}

int aeron_counters_reader_counter_owner_id(
    aeron_counters_reader_t *counters_reader, int32_t counter_id, int64_t *owner_id)
{
    if (counter_id < 0 || counter_id > counters_reader->max_counter_id)
    {
        return -1;
    }

    aeron_counter_value_descriptor_t *value_descriptor = (aeron_counter_value_descriptor_t *)(
        counters_reader->values + AERON_COUNTER_OFFSET(counter_id));

    *owner_id = value_descriptor->owner_id;

    return 0;
}

int aeron_counters_reader_counter_reference_id(
    aeron_counters_reader_t *counters_reader, int32_t counter_id, int64_t *reference_id)
{
    if (counter_id < 0 || counter_id > counters_reader->max_counter_id)
    {
        return -1;
    }

    aeron_counter_value_descriptor_t *value_descriptor = (aeron_counter_value_descriptor_t *)(
        counters_reader->values + AERON_COUNTER_OFFSET(counter_id));

    *reference_id = value_descriptor->reference_id;

    return 0;
}

int aeron_counters_reader_counter_state(aeron_counters_reader_t *counters_reader, int32_t counter_id, int32_t *state)
{
    if (counter_id < 0 || counter_id > counters_reader->max_counter_id)
    {
        return -1;
    }

    aeron_counter_metadata_descriptor_t *metadata = (aeron_counter_metadata_descriptor_t *)(
        counters_reader->metadata + AERON_COUNTER_METADATA_OFFSET(counter_id));

    AERON_GET_ACQUIRE(*state, metadata->state);

    return 0;
}

int aeron_counters_reader_counter_type_id(
    aeron_counters_reader_t *counters_reader, int32_t counter_id, int32_t *type_id)
{
    if (counter_id < 0 || counter_id > counters_reader->max_counter_id)
    {
        return -1;
    }

    aeron_counter_metadata_descriptor_t *metadata = (aeron_counter_metadata_descriptor_t *)(
        counters_reader->metadata + AERON_COUNTER_METADATA_OFFSET(counter_id));

    *type_id = metadata->type_id;

    return 0;
}

int aeron_counters_reader_metadata_key(
    aeron_counters_reader_t *counters_reader, int32_t counter_id, uint8_t **key_p)
{
    if (NULL == key_p || counter_id < 0 || counter_id > counters_reader->max_counter_id)
    {
        return -1;
    }

    *key_p = counters_reader->metadata + AERON_COUNTER_METADATA_OFFSET(counter_id) + AERON_COUNTER_KEY_OFFSET;

    return 0;
}

int aeron_counters_reader_counter_label(
    aeron_counters_reader_t *counters_reader, int32_t counter_id, char *buffer, size_t buffer_length)
{
    if (counter_id < 0 || counter_id > counters_reader->max_counter_id)
    {
        return -1;
    }

    aeron_counter_metadata_descriptor_t *metadata = (aeron_counter_metadata_descriptor_t *)(
        counters_reader->metadata + AERON_COUNTER_METADATA_OFFSET(counter_id));

    int32_t label_length;
    AERON_GET_ACQUIRE(label_length, metadata->label_length);

    size_t copy_length = (size_t)label_length < buffer_length ? (size_t)label_length : buffer_length;
    memcpy(buffer, metadata->label, copy_length);

    return (int)copy_length;
}

int aeron_counters_reader_free_for_reuse_deadline_ms(
    aeron_counters_reader_t *counters_reader, int32_t counter_id, int64_t *deadline_ms)
{
    if (counter_id < 0 || counter_id > counters_reader->max_counter_id)
    {
        return -1;
    }

    aeron_counter_metadata_descriptor_t *metadata = (aeron_counter_metadata_descriptor_t *)(
        counters_reader->metadata + AERON_COUNTER_METADATA_OFFSET(counter_id));

    *deadline_ms = metadata->free_for_reuse_deadline_ms;

    return 0;
}

extern int aeron_counters_reader_init(
    aeron_counters_reader_t *reader,
    uint8_t *metadata_buffer,
    size_t metadata_length,
    uint8_t *values_buffer,
    size_t values_length);

extern int64_t *aeron_counters_manager_addr(aeron_counters_manager_t *counters_manager, int32_t counter_id);

extern void aeron_counter_set_release(volatile int64_t *addr, int64_t value);

extern int64_t aeron_counter_get_plain(volatile int64_t *addr);
extern int64_t aeron_counter_get_acquire(volatile int64_t *addr);

extern int64_t aeron_counter_increment(volatile int64_t *addr);
extern int64_t aeron_counter_increment_release(volatile int64_t *addr);
extern int64_t aeron_counter_increment_plain(volatile int64_t *addr);

extern int64_t aeron_counter_get_and_add(volatile int64_t *addr, int64_t value);
extern int64_t aeron_counter_get_and_add_release(volatile int64_t *addr, int64_t value);
extern int64_t aeron_counter_get_and_add_plain(volatile int64_t *addr, int64_t value);

extern bool aeron_counter_propose_max_release(volatile int64_t *addr, int64_t proposed_value);
extern bool aeron_counter_propose_max_plain(volatile int64_t *addr, int64_t proposed_value);
