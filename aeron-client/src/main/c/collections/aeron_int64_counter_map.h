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

#ifndef AERON_INT64_COUNTER_MAP_H
#define AERON_INT64_COUNTER_MAP_H

#include <stdbool.h>
#include <errno.h>

#include "util/aeron_error.h"
#include "util/aeron_platform.h"
#include "util/aeron_bitutil.h"
#include "collections/aeron_hashing.h"
#include "collections/aeron_map.h"
#include "aeron_alloc.h"

typedef struct aeron_int64_counter_map_stct
{
    int64_t *entries;
    float load_factor;
    size_t entries_length;
    size_t size;
    size_t resize_threshold;
    int64_t initial_value;
}
aeron_int64_counter_map_t;

inline size_t aeron_int64_counter_map_hash_key(int64_t key, size_t mask)
{
    return aeron_even_hash((uint64_t)key, mask);
}

inline int aeron_int64_counter_map_init(
    aeron_int64_counter_map_t *map, int64_t initial_value, size_t initial_capacity, float load_factor)
{
    size_t capacity = (size_t)aeron_find_next_power_of_two((int32_t)initial_capacity);

    map->load_factor = load_factor;
    map->resize_threshold = (size_t)(load_factor * (float)capacity);
    map->entries = NULL;
    map->entries_length = 2 * capacity;
    map->initial_value = initial_value;
    map->size = 0;

    if (aeron_alloc((void **)&map->entries, (map->entries_length * sizeof(int64_t))) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    for (size_t i = 0, size = map->entries_length; i < size; i++)
    {
        map->entries[i] = map->initial_value;
    }

    return 0;
}

inline void aeron_int64_counter_map_delete(aeron_int64_counter_map_t *map)
{
    if (NULL != map->entries)
    {
        aeron_free(map->entries);
    }
}

inline int aeron_int64_counter_map_rehash(aeron_int64_counter_map_t *map, size_t new_entries_length)
{
    size_t mask = new_entries_length - 1;

    int64_t *tmp_entries;

    if (aeron_alloc((void **)&tmp_entries, (new_entries_length * sizeof(int64_t))) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    for (size_t i = 0, size = new_entries_length; i < size; i++)
    {
        tmp_entries[i] = map->initial_value;
    }

    for (size_t i = 0, size = map->entries_length; i < size; i += 2)
    {
        int64_t value = map->entries[i + 1];

        if (map->initial_value != value)
        {
            int64_t key = map->entries[i];
            size_t new_hash = aeron_int64_counter_map_hash_key(key, mask);

            while (map->initial_value != tmp_entries[new_hash])
            {
                new_hash = (new_hash + 2) & mask;
            }

            tmp_entries[new_hash] = key;
            tmp_entries[new_hash + 1] = value;
        }
    }

    aeron_free(map->entries);

    map->entries = tmp_entries;
    map->entries_length = new_entries_length;
    map->resize_threshold = (size_t)(((float)new_entries_length / 2) * map->load_factor);

    return 0;
}

inline void aeron_int64_counter_map_compact_chain(aeron_int64_counter_map_t *map, size_t delete_index)
{
    size_t mask = map->entries_length - 1;
    size_t index = delete_index;

    while (true)
    {
        index = (index + 2) & mask;
        if (map->initial_value == map->entries[index + 1])
        {
            break;
        }

        size_t hash = aeron_int64_counter_map_hash_key(map->entries[index], mask);

        if ((index < hash && (hash <= delete_index || delete_index <= index)) ||
            (hash <= delete_index && delete_index <= index))
        {
            map->entries[delete_index] = map->entries[index];
            map->entries[delete_index + 1] = map->entries[index + 1];
            map->entries[index + 1] = map->initial_value;

            delete_index = index;
        }
    }
}

inline int64_t aeron_int64_counter_map_remove(aeron_int64_counter_map_t *map, int64_t key)
{
    size_t mask = map->entries_length - 1;
    size_t index = aeron_int64_counter_map_hash_key(key, mask);

    int64_t value;
    while (map->initial_value != (value = map->entries[index + 1]))
    {
        if (key == map->entries[index])
        {
            map->entries[index + 1] = map->initial_value;
            --map->size;

            aeron_int64_counter_map_compact_chain(map, index);
            break;
        }

        index = (index + 2) & mask;
    }

    return value;
}

inline int aeron_int64_counter_map_put(
    aeron_int64_counter_map_t *map, const int64_t key, const int64_t value, int64_t *existing_value)
{
    if (map->initial_value == value)
    {
        AERON_SET_ERR(EINVAL, "%s", "cannot accept initialValue");
        return -1;
    }

    size_t mask = map->entries_length - 1;
    size_t index = aeron_int64_counter_map_hash_key(key, mask);

    int64_t old_value;
    while (map->initial_value != (old_value = map->entries[index + 1]))
    {
        if (key == map->entries[index])
        {
            break;
        }

        index = (index + 2) & mask;
    }

    if (old_value == map->initial_value)
    {
        map->size++;
        map->entries[index] = key;
    }

    map->entries[index + 1] = value;

    if (map->size > map->resize_threshold)
    {
        size_t new_entries_length = map->entries_length << 1;

        if (aeron_int64_counter_map_rehash(map, new_entries_length) < 0)
        {
            return -1;
        }
    }

    if (NULL != existing_value)
    {
        *existing_value = old_value;
    }

    return 0;
}

inline int64_t aeron_int64_counter_map_get(aeron_int64_counter_map_t *map, const int64_t key)
{
    size_t mask = map->entries_length - 1;
    size_t index = aeron_int64_counter_map_hash_key(key, mask);

    int64_t value;
    while (map->initial_value != (value = map->entries[index + 1]))
    {
        if (key == map->entries[index])
        {
            break;
        }

        index = (index + 2) & mask;
    }

    return value;
}

inline int aeron_int64_counter_map_get_and_add(
    aeron_int64_counter_map_t *map, const int64_t key, const int64_t delta, int64_t *value)
{
    size_t mask = map->entries_length - 1;
    size_t index = aeron_int64_counter_map_hash_key(key, mask);

    int64_t old_value;
    while (map->initial_value != (old_value = map->entries[index + 1]))
    {
        if (key == map->entries[index])
        {
            break;
        }

        index = (index + 2) & mask;
    }

    if (delta != 0)
    {
        int64_t new_value = old_value + delta;
        map->entries[index + 1] = new_value;

        if (map->initial_value == old_value)
        {
            map->size++;
            map->entries[index] = key;

            if (map->size > map->resize_threshold)
            {
                size_t new_entries_length = map->entries_length << 1;

                if (aeron_int64_counter_map_rehash(map, new_entries_length) < 0)
                {
                    AERON_APPEND_ERR("%s", "");
                    return -1;
                }
            }
        }
        else if (map->initial_value == new_value)
        {
            map->size--;
            aeron_int64_counter_map_compact_chain(map, index);
        }
    }

    if (NULL != value)
    {
        *value = old_value;
    }

    return 0;
}

inline int aeron_int64_counter_map_add_and_get(
    aeron_int64_counter_map_t *map, const int64_t key, int64_t delta, int64_t *value)
{
    int64_t existing_value = 0;
    int result = aeron_int64_counter_map_get_and_add(map, key, delta, &existing_value);
    if (NULL != value)
    {
        *value = (existing_value + delta);
    }
    return result;
}

inline int aeron_int64_counter_map_inc_and_get(aeron_int64_counter_map_t *map, const int64_t key, int64_t *value)
{
    return aeron_int64_counter_map_add_and_get(map, key, 1, value);
}

inline int aeron_int64_counter_map_dec_and_get(aeron_int64_counter_map_t *map, const int64_t key, int64_t *value)
{
    return aeron_int64_counter_map_add_and_get(map, key, -1, value);
}

inline int aeron_int64_counter_map_get_and_inc(aeron_int64_counter_map_t *map, const int64_t key, int64_t *value)
{
    return aeron_int64_counter_map_get_and_add(map, key, 1, value);
}

inline int aeron_int64_counter_map_get_and_dec(aeron_int64_counter_map_t *map, const int64_t key, int64_t *value)
{
    return aeron_int64_counter_map_get_and_add(map, key, -1, value);
}

typedef void (*aeron_int64_counter_map_for_each_func_t)(void *clientd, int64_t key, int64_t value);
typedef bool (*aeron_int64_counter_map_predicate_func_t)(void *clientd, int64_t key, int64_t value);

inline void aeron_int64_counter_map_for_each(
    aeron_int64_counter_map_t *map, aeron_int64_counter_map_for_each_func_t func, void *clientd)
{
    for (size_t i = 0, size = map->entries_length; i < size; i += 2)
    {
        if (map->initial_value != map->entries[i + 1])
        {
            func(clientd, map->entries[i], map->entries[i + 1]);
        }
    }
}

inline void aeron_int64_counter_map_remove_if(
    aeron_int64_counter_map_t *map, aeron_int64_counter_map_predicate_func_t func, void *clientd)
{
    size_t i = 0;
    size_t remaining = map->size;

    while (i < map->entries_length && remaining > 0)
    {
        bool is_removed = false;
        int64_t key = map->entries[i];
        int64_t value = map->entries[i + 1];
        if (map->initial_value != value)
        {
            if (func(clientd, key, value))
            {
                is_removed = (map->initial_value != aeron_int64_counter_map_remove(map, key));
            }
        }

        if (is_removed)
        {
            --remaining;
        }
        else
        {
            i += 2;
        }
    }
}

#endif
