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

#include "aeron_local_sockaddr.h"
#include "concurrent/aeron_atomic.h"
#include "concurrent/aeron_counters_manager.h"
#include "aeron_counters.h"
#include "aeron_counters.h"

typedef struct aeron_lock_sockaddr_find_clientd_stct
{
    int32_t channel_status_indicator_id;
    aeron_iovec_t *address_vec;
    size_t address_vec_len;
    size_t current_address;
    aeron_counters_reader_t *reader;
}
aeron_lock_sockaddr_find_clientd_t;

static void aeron_local_sockaddr_find_address_counter_metadata_func(
    int32_t id,
    int32_t type_id,
    const uint8_t *key,
    size_t key_length,
    const uint8_t *label,
    size_t label_length,
    void *clientd)
{
    aeron_lock_sockaddr_find_clientd_t *find_addr = (aeron_lock_sockaddr_find_clientd_t *)clientd;

    if (AERON_COUNTER_LOCAL_SOCKADDR_TYPE_ID != type_id)
    {
        return;
    }

    int32_t status_indicator_id;
    memcpy(&status_indicator_id, key, sizeof(status_indicator_id));

    if (status_indicator_id != find_addr->channel_status_indicator_id)
    {
        return;
    }

    int64_t *status_indicator_addr = aeron_counters_reader_addr(find_addr->reader, status_indicator_id);
    int64_t status;
    AERON_GET_ACQUIRE(status, *(volatile int64_t *)status_indicator_addr);

    if (AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE != status)
    {
        return;
    }

    int32_t addr_len;

    memcpy(&addr_len, key + sizeof(status_indicator_id), sizeof(addr_len));
    char *addr = (char *)(key + sizeof(status_indicator_id) + sizeof(addr_len));

    if (addr_len <= 0)
    {
        return;
    }

    size_t current_address = find_addr->current_address;
    find_addr->current_address++;

    if (find_addr->address_vec_len <= current_address)
    {
        return;
    }

    aeron_iovec_t *iov = &find_addr->address_vec[current_address];
    size_t buffer_len = iov->iov_len;
    size_t addr_len_sz = (size_t)addr_len;

    size_t to_copy = addr_len_sz < (buffer_len - 1) ? addr_len_sz : buffer_len - 1;
    memcpy(iov->iov_base, addr, to_copy);
    iov->iov_base[to_copy] = UINT8_C(0);
}

int aeron_local_sockaddr_find_addrs(
    aeron_counters_reader_t *reader,
    int32_t channel_status_indicator_id,
    aeron_iovec_t *address_vec,
    size_t address_vec_len)
{
    volatile int64_t *status_indicator_addr = aeron_counters_reader_addr(reader, channel_status_indicator_id);

    if (NULL == status_indicator_addr)
    {
        return 0;
    }

    int64_t status;
    AERON_GET_ACQUIRE(status, *status_indicator_addr);

    if (AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE != status)
    {
        return 0;
    }

    aeron_lock_sockaddr_find_clientd_t find_clientd =
        {
            .channel_status_indicator_id = channel_status_indicator_id,
            .address_vec = address_vec,
            .address_vec_len = address_vec_len,
            .current_address = 0,
            .reader = reader
        };

    aeron_counters_reader_foreach_metadata(
        reader->metadata,
        reader->metadata_length,
        aeron_local_sockaddr_find_address_counter_metadata_func,
        &find_clientd);

    return (int)find_clientd.current_address;
}
