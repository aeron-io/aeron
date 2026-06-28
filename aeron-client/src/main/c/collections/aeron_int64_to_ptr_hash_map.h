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

#ifndef AERON_INT64_TO_PTR_HASH_MAP_H
#define AERON_INT64_TO_PTR_HASH_MAP_H

#include <errno.h>

#include "util/aeron_platform.h"
#include "util/aeron_bitutil.h"
#include "util/aeron_error.h"
#include "collections/aeron_hashing.h"
#include "collections/aeron_map.h"
#include "aeron_alloc.h"

typedef struct aeron_int64_to_ptr_hash_map_stct
{
    int64_t *keys;
    void **values;
    float load_factor;
    size_t capacity;
    size_t size;
    size_t resize_threshold;
}
aeron_int64_to_ptr_hash_map_t;

inline size_t aeron_int64_to_ptr_hash_map_hash_key(int64_t key, size_t mask)
{
    return aeron_hash((uint64_t)key, mask);
}

inline int aeron_int64_to_ptr_hash_map_init(
    aeron_int64_to_ptr_hash_map_t *map, size_t initial_capacity, float load_factor)
{
    size_t capacity = (size_t)aeron_find_next_power_of_two((int32_t)initial_capacity);

    map->load_factor = load_factor;
    map->resize_threshold = (size_t)(load_factor * (float)capacity);
    map->keys = NULL;
    map->values = NULL;
    map->capacity = capacity;
    map->size = 0;

    if (aeron_alloc((void **)&map->keys, (capacity * sizeof(int64_t))) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_alloc((void **)&map->values, (capacity * sizeof(void *))) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    return 0;
}

inline void aeron_int64_to_ptr_hash_map_delete(aeron_int64_to_ptr_hash_map_t *map)
{
    if (NULL != map->keys)
    {
        aeron_free(map->keys);
    }

    if (NULL != map->values)
    {
        aeron_free(map->values);
    }
}

inline int aeron_int64_to_ptr_hash_map_rehash(aeron_int64_to_ptr_hash_map_t *map, size_t new_capacity)
{
    size_t mask = new_capacity - 1;
    map->resize_threshold = (size_t)((float)new_capacity * map->load_factor);

    int64_t *tmp_keys;
    void **tmp_values;

    if (aeron_alloc((void **)&tmp_keys, new_capacity * sizeof(int64_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_alloc((void **)&tmp_values, new_capacity * sizeof(void *)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        aeron_free(tmp_keys);
        return -1;
    }

    for (size_t i = 0, size = map->capacity; i < size; i++)
    {
        void *value = map->values[i];

        if (NULL != value)
        {
            int64_t key = map->keys[i];
            size_t new_hash = aeron_int64_to_ptr_hash_map_hash_key(key, mask);

            while (NULL != tmp_values[new_hash])
            {
                new_hash = (new_hash + 1) & mask;
            }

            tmp_keys[new_hash] = key;
            tmp_values[new_hash] = value;
        }
    }

    aeron_free(map->keys);
    aeron_free(map->values);

    map->keys = tmp_keys;
    map->values = tmp_values;
    map->capacity = new_capacity;

    return 0;
}

inline int aeron_int64_to_ptr_hash_map_put(aeron_int64_to_ptr_hash_map_t *map, const int64_t key, void *value)
{
    if (NULL == value)
    {
        AERON_SET_ERR(EINVAL, "%s", "value must not be null");
        return -1;
    }

    size_t mask = map->capacity - 1;
    size_t index = aeron_int64_to_ptr_hash_map_hash_key(key, mask);

    void *old_value = NULL;
    while (NULL != map->values[index])
    {
        if (key == map->keys[index])
        {
            old_value = map->values[index];
            break;
        }

        index = (index + 1) & mask;
    }

    if (NULL == old_value)
    {
        ++map->size;
        map->keys[index] = key;
    }

    map->values[index] = value;

    if (map->size > map->resize_threshold)
    {
        size_t new_capacity = map->capacity << 1;

        if (aeron_int64_to_ptr_hash_map_rehash(map, new_capacity) < 0)
        {
            return -1;
        }
    }

    return 0;
}

inline void *aeron_int64_to_ptr_hash_map_get(aeron_int64_to_ptr_hash_map_t *map, const int64_t key)
{
    size_t mask = map->capacity - 1;
    size_t index = aeron_int64_to_ptr_hash_map_hash_key(key, mask);

    void *value;
    while (NULL != (value = map->values[index]))
    {
        if (key == map->keys[index])
        {
            break;
        }

        index = (index + 1) & mask;
    }

    return value;
}

inline void aeron_int64_to_ptr_hash_map_compact_chain(aeron_int64_to_ptr_hash_map_t *map, size_t delete_index)
{
    size_t mask = map->capacity - 1;
    size_t index = delete_index;

    while (true)
    {
        index = (index + 1) & mask;
        if (NULL == map->values[index])
        {
            break;
        }

        size_t hash = aeron_int64_to_ptr_hash_map_hash_key(map->keys[index], mask);

        if ((index < hash && (hash <= delete_index || delete_index <= index)) ||
            (hash <= delete_index && delete_index <= index))
        {
            map->keys[delete_index] = map->keys[index];
            map->values[delete_index] = map->values[index];

            map->values[index] = NULL;
            delete_index = index;
        }
    }
}

inline void *aeron_int64_to_ptr_hash_map_remove(aeron_int64_to_ptr_hash_map_t *map, int64_t key)
{
    size_t mask = map->capacity - 1;
    size_t index = aeron_int64_to_ptr_hash_map_hash_key(key, mask);

    void *value;
    while (NULL != (value = map->values[index]))
    {
        if (key == map->keys[index])
        {
            map->values[index] = NULL;
            --map->size;

            aeron_int64_to_ptr_hash_map_compact_chain(map, index);
            break;
        }

        index = (index + 1) & mask;
    }

    return value;
}

typedef void (*aeron_int64_to_ptr_hash_map_for_each_func_t)(void *clientd, int64_t key, void *value);
typedef bool (*aeron_int64_to_ptr_hash_map_predicate_func_t)(void *clientd, int64_t key, void *value);

inline void aeron_int64_to_ptr_hash_map_for_each(
    aeron_int64_to_ptr_hash_map_t *map, aeron_int64_to_ptr_hash_map_for_each_func_t func, void *clientd)
{
    for (size_t i = 0; i < map->capacity; i++)
    {
        if (NULL != map->values[i])
        {
            func(clientd, map->keys[i], map->values[i]);
        }
    }
}

inline void aeron_int64_to_ptr_hash_map_remove_if(
    aeron_int64_to_ptr_hash_map_t *map, aeron_int64_to_ptr_hash_map_predicate_func_t func, void *clientd)
{
    size_t i = 0;
    size_t remaining = map->size;

    while (i < map->capacity && remaining > 0)
    {
        bool is_removed = false;
        if (NULL != map->values[i])
        {
            if (func(clientd, map->keys[i], map->values[i]))
            {
                is_removed = NULL != aeron_int64_to_ptr_hash_map_remove(map, map->keys[i]);
            }
        }

        if (is_removed)
        {
            --remaining;
        }
        else
        {
            ++i;
        }
    }
}

#endif
