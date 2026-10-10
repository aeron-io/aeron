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

#include <cstring>
#include <string>

#include <gtest/gtest.h>

extern "C"
{
#include "aeron_counters.h"
#include "concurrent/aeron_counters_manager.h"
#include "status/aeron_local_sockaddr.h"
}

static const int32_t NUM_COUNTERS = 4;
static const int32_t CHANNEL_STATUS_ID = 0;

class LocalSockaddrTest : public testing::Test
{
protected:
    LocalSockaddrTest()
    {
        aeron_counters_reader_init(&m_reader, m_metadata, sizeof(m_metadata), m_values, sizeof(m_values));
        allocate(CHANNEL_STATUS_ID, AERON_COUNTER_SEND_CHANNEL_STATUS_TYPE_ID, AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE);
    }

    aeron_counter_metadata_descriptor_t *allocate(int32_t counter_id, int32_t type_id, int64_t value)
    {
        aeron_counter_metadata_descriptor_t *metadata =
            reinterpret_cast<aeron_counter_metadata_descriptor_t *>(m_metadata) + counter_id;
        metadata->type_id = type_id;
        metadata->state = AERON_COUNTER_RECORD_ALLOCATED;
        *aeron_counters_reader_addr(&m_reader, counter_id) = value;

        return metadata;
    }

    void allocateLocalSockaddr(int32_t counter_id, const char *address, int64_t status)
    {
        aeron_counter_metadata_descriptor_t *metadata =
            allocate(counter_id, AERON_COUNTER_LOCAL_SOCKADDR_TYPE_ID, status);

        aeron_local_sockaddr_key_layout_t key = {};
        key.channel_status_id = CHANNEL_STATUS_ID;
        key.local_sockaddr_len = (int32_t)strlen(address);
        std::memcpy(key.local_sockaddr, address, strlen(address));
        std::memcpy(metadata->key, &key, sizeof(key));
    }

    alignas(64) uint8_t m_metadata[NUM_COUNTERS * sizeof(aeron_counter_metadata_descriptor_t)] = {};
    alignas(64) uint8_t m_values[NUM_COUNTERS * sizeof(aeron_counter_value_descriptor_t)] = {};
    aeron_counters_reader_t m_reader = {};
};

TEST_F(LocalSockaddrTest, shouldOnlyFindAddressesWhoseOwnCounterIsActive)
{
    allocateLocalSockaddr(1, "127.0.0.1:40123", AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE);
    allocateLocalSockaddr(2, "127.0.0.1:40124", AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_INITIALIZING);
    allocateLocalSockaddr(3, "127.0.0.1:40125", AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_CLOSING);

    uint8_t buffers[3][AERON_CLIENT_MAX_LOCAL_ADDRESS_STR_LEN] = {};
    aeron_iovec_t address_vec[3];
    for (int i = 0; i < 3; i++)
    {
        address_vec[i].iov_base = buffers[i];
        address_vec[i].iov_len = sizeof(buffers[i]);
    }

    ASSERT_EQ(1, aeron_local_sockaddr_find_addrs(&m_reader, CHANNEL_STATUS_ID, address_vec, 3));
    EXPECT_EQ(std::string("127.0.0.1:40123"), std::string(reinterpret_cast<char *>(buffers[0])));
}
