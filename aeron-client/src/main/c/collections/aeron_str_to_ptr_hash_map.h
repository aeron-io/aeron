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

#ifndef AERON_STR_TO_PTR_HASH_MAP_H
#define AERON_STR_TO_PTR_HASH_MAP_H

#include <string.h>
#include <errno.h>

#include "util/aeron_platform.h"
#include "util/aeron_bitutil.h"
#include "util/aeron_strutil.h"
#include "util/aeron_error.h"
#include "collections/aeron_hashing.h"
#include "collections/aeron_map.h"
#include "aeron_alloc.h"

typedef struct aeron_str_to_ptr_hash_map_key_stct
{
    const char *str;
    uint64_t hash_code;
    size_t str_length;
}
aeron_str_to_ptr_hash_map_key_t;

typedef struct aeron_str_to_ptr_hash_map_stct
{
    aeron_str_to_ptr_hash_map_key_t *keys;
    void **values;
    float load_factor;
    size_t capacity;
    size_t size;
    size_t resize_threshold;
}
aeron_str_to_ptr_hash_map_t;

inline size_t aeron_str_to_ptr_hash_map_hash_key(uint64_t key_hash_code, size_t mask)
{
    uint64_t hash = key_hash_code;

    if (mask <= UINT32_MAX)
    {
        hash = (uint32_t)hash ^ (uint32_t)(hash >> 32u);
    }

    return (size_t)(hash & mask);
}

inline bool aeron_str_to_ptr_hash_map_compare(
    aeron_str_to_ptr_hash_map_key_t *key, const char *key_str, size_t key_str_len, uint64_t key_hash_code)
{
    return key->hash_code == key_hash_code &&
        key->str_length == key_str_len &&
        strncmp(key->str, key_str, key_str_len) == 0;
}

inline int aeron_str_to_ptr_hash_map_init(aeron_str_to_ptr_hash_map_t *map, size_t initial_capacity, float load_factor)
{
    size_t capacity = (size_t)aeron_find_next_power_of_two((int32_t)initial_capacity);

    map->load_factor = load_factor;
    map->resize_threshold = (size_t)(load_factor * (float)capacity);
    map->keys = NULL;
    map->values = NULL;
    map->capacity = capacity;
    map->size = 0;

    if (aeron_alloc((void **)&map->keys, (capacity * sizeof(aeron_str_to_ptr_hash_map_key_t))) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate keys");
        return -1;
    }

    if (aeron_alloc((void **)&map->values, (capacity * sizeof(void *))) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate values");
        return -1;
    }

    return 0;
}

inline void aeron_str_to_ptr_hash_map_delete(aeron_str_to_ptr_hash_map_t *map)
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

inline int aeron_str_to_ptr_hash_map_rehash(aeron_str_to_ptr_hash_map_t *map, size_t new_capacity)
{
    size_t mask = new_capacity - 1;
    map->resize_threshold = (size_t)((float)new_capacity * map->load_factor);

    aeron_str_to_ptr_hash_map_key_t *tmp_keys;
    void **tmp_values;

    if (aeron_alloc((void **)&tmp_keys, (new_capacity * sizeof(aeron_str_to_ptr_hash_map_key_t))) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate new keys");
        return -1;
    }

    if (aeron_alloc((void **)&tmp_values, (new_capacity * sizeof(void *))) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate new values");
        aeron_free(tmp_keys);
        return -1;
    }

    for (size_t i = 0, size = map->capacity; i < size; i++)
    {
        void *value = map->values[i];

        if (NULL != value)
        {
            aeron_str_to_ptr_hash_map_key_t *key = &map->keys[i];
            size_t new_hash = aeron_str_to_ptr_hash_map_hash_key(key->hash_code, mask);

            while (NULL != tmp_values[new_hash])
            {
                new_hash = (new_hash + 1) & mask;
            }

            tmp_keys[new_hash].str = key->str;
            tmp_keys[new_hash].str_length = key->str_length;
            tmp_keys[new_hash].hash_code = key->hash_code;
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

inline int aeron_str_to_ptr_hash_map_put(aeron_str_to_ptr_hash_map_t *map, const char *key, size_t key_len, void *value)
{
    if (NULL == value)
    {
        AERON_SET_ERR(EINVAL, "%s", "value is null");
        return -1;
    }

    uint64_t hash_code = aeron_fnv_64a_buf((uint8_t *)key, key_len);
    size_t mask = map->capacity - 1;
    size_t index = aeron_str_to_ptr_hash_map_hash_key(hash_code, mask);

    void *old_value = NULL;
    while (NULL != map->values[index])
    {
        if (aeron_str_to_ptr_hash_map_compare(&map->keys[index], key, key_len, hash_code))
        {
            old_value = map->values[index];
            break;
        }

        index = (index + 1) & mask;
    }

    if (NULL == old_value)
    {
        ++map->size;
        map->keys[index].str = key;
        map->keys[index].hash_code = hash_code;
        map->keys[index].str_length = key_len;
    }

    map->values[index] = value;

    if (map->size > map->resize_threshold)
    {
        size_t new_capacity = map->capacity << 1;

        if (aeron_str_to_ptr_hash_map_rehash(map, new_capacity) < 0)
        {
            return -1;
        }
    }

    return 0;
}

inline void *aeron_str_to_ptr_hash_map_get(aeron_str_to_ptr_hash_map_t *map, const char *key, size_t key_len)
{
    uint64_t hash_code = aeron_fnv_64a_buf((uint8_t *)key, key_len);
    size_t mask = map->capacity - 1;
    size_t index = aeron_str_to_ptr_hash_map_hash_key(hash_code, mask);

    void *value;
    while (NULL != (value = map->values[index]))
    {
        if (aeron_str_to_ptr_hash_map_compare(&map->keys[index], key, key_len, hash_code))
        {
            break;
        }

        index = (index + 1) & mask;
    }

    return value;
}

inline void aeron_str_to_ptr_hash_map_compact_chain(aeron_str_to_ptr_hash_map_t *map, size_t delete_index)
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

        size_t hash = aeron_str_to_ptr_hash_map_hash_key(map->keys[index].hash_code, mask);

        if ((index < hash && (hash <= delete_index || delete_index <= index)) ||
            (hash <= delete_index && delete_index <= index))
        {
            memcpy(&map->keys[delete_index], &map->keys[index], sizeof(aeron_str_to_ptr_hash_map_key_t));
            map->values[delete_index] = map->values[index];

            map->values[index] = NULL;
            delete_index = index;
        }
    }
}

inline void *aeron_str_to_ptr_hash_map_remove(aeron_str_to_ptr_hash_map_t *map, const char *key, size_t key_len)
{
    uint64_t hash_code = aeron_fnv_64a_buf((uint8_t *)key, key_len);
    size_t mask = map->capacity - 1;
    size_t index = aeron_str_to_ptr_hash_map_hash_key(hash_code, mask);

    void *value;
    while (NULL != (value = map->values[index]))
    {
        if (aeron_str_to_ptr_hash_map_compare(&map->keys[index], key, key_len, hash_code))
        {
            map->values[index] = NULL;
            --map->size;

            aeron_str_to_ptr_hash_map_compact_chain(map, index);
            break;
        }

        index = (index + 1) & mask;
    }

    return value;
}

typedef void (*aeron_str_to_ptr_hash_map_for_each_func_t)(void *clientd, const char *key, size_t key_len, void *value);

inline void aeron_str_to_ptr_hash_map_for_each(
    aeron_str_to_ptr_hash_map_t *map, aeron_str_to_ptr_hash_map_for_each_func_t func, void *clientd)
{
    for (size_t i = 0; i < map->capacity; i++)
    {
        if (map->values[i] != NULL)
        {
            func(clientd, map->keys[i].str, map->keys[i].str_length, map->values[i]);
        }
    }
}

#endif //AERON_STR_TO_PTR_HASH_MAP_H
