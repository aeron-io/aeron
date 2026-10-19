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
#endif

#include <string.h>
#include <inttypes.h>

#include "concurrent/aeron_counters_manager.h"
#include "util/aeron_arrayutil.h"
#include "aeron_name_resolver_cache.h"

int aeron_name_resolver_cache_init(aeron_name_resolver_cache_t *cache, int64_t timeout_ms)
{
    memset(cache, 0, sizeof(aeron_name_resolver_cache_t));
    cache->timeout_ms = timeout_ms;
    return 0;
}

int aeron_name_resolver_cache_close(aeron_name_resolver_cache_t *cache)
{
    if (NULL != cache)
    {
        for (size_t i = 0; i < cache->entries.length; i++)
        {
            aeron_free((void *)cache->entries.array[i].name);
        }

        aeron_free((void *)cache->entries.array);
    }

    return 0;
}

int aeron_name_resolver_cache_find_index_by_name_and_type(
    aeron_name_resolver_cache_t *cache, const char *name, size_t name_length, int8_t res_type)
{
    for (size_t i = 0; i < cache->entries.length; i++)
    {
        aeron_name_resolver_cache_entry_t *entry = &cache->entries.array[i];

        if (res_type == entry->cache_addr.res_type &&
            name_length == entry->name_length &&
            0 == strncmp(name, entry->name, name_length))
        {
            return (int)i;
        }
    }
    
    return -1;
}

int aeron_name_resolver_cache_add_or_update(
    aeron_name_resolver_cache_t *cache,
    const char *name,
    size_t name_length,
    aeron_name_resolver_cache_addr_t *cache_addr,
    int64_t time_of_last_activity_ms,
    volatile int64_t *cache_entries_counter)
{
    int index = aeron_name_resolver_cache_find_index_by_name_and_type(cache, name, name_length, cache_addr->res_type);
    aeron_name_resolver_cache_entry_t *entry;
    int num_updated;

    if (index < 0)
    {
        int ensure_capacity_result = 0;
        AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, cache->entries, aeron_name_resolver_cache_entry_t)

        if (ensure_capacity_result < 0)
        {
            AERON_APPEND_ERR(
                "Failed to allocate rows for lookup table (%" PRIu64 ",%" PRIu64 ")",
                (uint64_t)cache->entries.length,
                (uint64_t)cache->entries.capacity);
            return -1;
        }

        entry = &cache->entries.array[cache->entries.length];

        if (aeron_alloc((void **)&entry->name, name_length + 1) < 0) // NULL terminate, just to be safe.
        {
            AERON_APPEND_ERR("%s", "Failed to allocate name for resolver cache");
            return -1;
        }

        strncpy((char *)entry->name, name, name_length);
        entry->name_length = name_length;
        num_updated = 1;

        cache->entries.length++;

        aeron_counter_set_release(cache_entries_counter, (int64_t) cache->entries.length);
    }
    else
    {
        entry = &cache->entries.array[index];
        num_updated = 0;
    }

    memcpy(&entry->cache_addr, cache_addr, sizeof(entry->cache_addr));
    entry->time_of_last_activity_ms = time_of_last_activity_ms;
    entry->deadline_ms = time_of_last_activity_ms + cache->timeout_ms;

    return num_updated;
}

int aeron_name_resolver_cache_lookup_by_name(
    aeron_name_resolver_cache_t *cache,
    const char *name,
    size_t name_length,
    int8_t res_type,
    aeron_name_resolver_cache_entry_t **entry)
{
    int index = aeron_name_resolver_cache_find_index_by_name_and_type(cache, name, name_length, res_type);

    if (0 <= index && NULL != entry)
    {
        *entry = &cache->entries.array[index];
    }

    return index;
}

int aeron_name_resolver_cache_timeout_old_entries(
    aeron_name_resolver_cache_t *cache, int64_t now_ms, volatile int64_t *cache_entries_counter)
{
    int num_removed = 0;
    for (int last_index = (int)cache->entries.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_name_resolver_cache_entry_t *entry = &cache->entries.array[i];

        if (entry->deadline_ms <= now_ms)
        {
            aeron_free((void *)entry->name);
            aeron_array_fast_unordered_remove(
                (uint8_t *)cache->entries.array, sizeof(aeron_name_resolver_cache_entry_t), i, last_index);
            cache->entries.length--;
            last_index--;
            num_removed++;
        }
    }

    if (0 != num_removed)
    {
        aeron_counter_set_release(cache_entries_counter, (int64_t) cache->entries.length);
    }

    return num_removed;
}
