/*
 * Copyright 2014-2020 Real Logic Limited.
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

#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <sys/socket.h>

#include "util/aeron_arrayutil.h"
#include "protocol/aeron_udp_protocol.h"
#include "aeron_name_resolver_driver_cache.h"

int aeron_name_resolver_driver_cache_init(aeron_name_resolver_driver_cache_t *cache)
{
    memset(cache, 0, sizeof(aeron_name_resolver_driver_cache_t));
    return 0;
}

int aeron_name_resolver_driver_cache_close(aeron_name_resolver_driver_cache_t *cache)
{
    if (NULL != cache)
    {
        for (size_t i = 0; i < cache->entries.length; i++)
        {
            aeron_free((void *)cache->entries.array[i].name);
        }
    }

    return 0;
}


int aeron_name_resolver_driver_cache_find_index_by_name_and_type(
    aeron_name_resolver_driver_cache_t *cache,
    const char *name,
    size_t name_length,
    int8_t res_type)
{
    for (size_t i = 0; i < cache->entries.length; i++)
    {
        aeron_name_resolver_driver_cache_entry_t *entry = &cache->entries.array[i];

        if (res_type == entry->res_type &&
            name_length == entry->name_length &&
            0 == strncmp(name, entry->name, name_length))
        {
            return i;
        }
    }
    
    return -1;
}

int aeron_name_resolver_driver_cache_add_or_update(
    aeron_name_resolver_driver_cache_t *cache,
    const char *name,
    size_t name_length,
    int8_t res_type,
    uint8_t *address,
    uint16_t port)
{
    int index = aeron_name_resolver_driver_cache_find_index_by_name_and_type(cache, name, name_length, res_type);
    aeron_name_resolver_driver_cache_entry_t *entry;
    int num_updated;

    if (index < 0)
    {
        int ensure_capacity_result = 0;
        AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, cache->entries, aeron_name_resolver_driver_cache_entry_t)

        if (ensure_capacity_result < 0)
        {
            aeron_set_err_from_last_err_code(
                "Failed to allocate rows for lookup table (%zu,%zu) - %s:%d",
                cache->entries.length, cache->entries.capacity, __FILE__, __LINE__);
            return -1;
        }

        entry = &cache->entries.array[cache->entries.length];

        if (aeron_alloc((void **)&entry->name, name_length + 1) < 0) // NULL terminate, just to be safe.
        {
            aeron_set_err_from_last_err_code("Failed copy name string for cache - %s:%d", __FILE__, __LINE__);
            return -1;
        }
        strncpy((char *)entry->name, name, name_length);
        entry->name_length = name_length;
        entry->res_type = res_type;
        num_updated = 1;

        cache->entries.length++;
    }
    else
    {
        entry = &cache->entries.array[index];
        num_updated = 0;
    }

    entry->port = port;
    size_t address_len = res_type == AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD ? 16 : 4;
    memcpy(entry->address, address, address_len);
    memset(&entry->address[address_len], 0, 16 - address_len);

    return num_updated;
}

int aeron_name_resolver_driver_cache_lookup(
    aeron_name_resolver_driver_cache_t *cache,
    const char *name,
    size_t name_length,
    int8_t res_type,
    aeron_name_resolver_driver_cache_entry_t **entry)
{
    int index = aeron_name_resolver_driver_cache_find_index_by_name_and_type(cache, name, name_length, res_type);

    if (0 <= index)
    {
        *entry = &cache->entries.array[index];
    }

    return index;
}
