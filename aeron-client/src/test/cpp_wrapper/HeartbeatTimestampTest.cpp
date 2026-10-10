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

#include <gtest/gtest.h>

#include "Aeron.h"
#include "HeartbeatTimestamp.h"

extern "C"
{
#include "concurrent/aeron_counters_manager.h"
}

using namespace aeron;
using namespace aeron::concurrent;

static const std::int32_t NUM_COUNTERS = 4;
static const std::int32_t OTHER_TYPE_ID = 1001;

class HeartbeatTimestampTest : public testing::Test
{
protected:
    // CountersReader captures the buffers on construction, so the C reader must be initialised first
    aeron_counters_reader_t *initReader()
    {
        aeron_counters_reader_init(&m_cReader, m_metadata, sizeof(m_metadata), m_values, sizeof(m_values));
        return &m_cReader;
    }

    void allocate(std::int32_t counterId, std::int32_t typeId, std::int64_t registrationId)
    {
        aeron_counter_metadata_descriptor_t *metadata =
            reinterpret_cast<aeron_counter_metadata_descriptor_t *>(m_metadata) + counterId;
        metadata->type_id = typeId;
        std::memcpy(metadata->key, &registrationId, sizeof(registrationId));
        metadata->state = AERON_COUNTER_RECORD_ALLOCATED;
    }

    alignas(64) std::uint8_t m_metadata[NUM_COUNTERS * sizeof(aeron_counter_metadata_descriptor_t)] = {};
    alignas(64) std::uint8_t m_values[NUM_COUNTERS * sizeof(aeron_counter_value_descriptor_t)] = {};
    aeron_counters_reader_t m_cReader = {};
    CountersReader m_reader = CountersReader(initReader());
};

TEST_F(HeartbeatTimestampTest, shouldFindHeartbeatCounterInEverySlotIncludingTheLast)
{
    ASSERT_EQ(NUM_COUNTERS - 1, m_reader.maxCounterId());

    for (std::int32_t i = 0; i < NUM_COUNTERS; i++)
    {
        allocate(i, OTHER_TYPE_ID, 100 + i);
    }

    for (std::int32_t heartbeatId = 0; heartbeatId < NUM_COUNTERS; heartbeatId++)
    {
        const std::int64_t clientId = 42 + heartbeatId;
        allocate(heartbeatId, HeartbeatTimestamp::CLIENT_HEARTBEAT_TYPE_ID, clientId);

        EXPECT_EQ(heartbeatId, HeartbeatTimestamp::findCounterIdByRegistrationId(
            m_reader, HeartbeatTimestamp::CLIENT_HEARTBEAT_TYPE_ID, clientId)) << "heartbeat counter id " << heartbeatId;
        EXPECT_TRUE(HeartbeatTimestamp::isActive(
            m_reader, heartbeatId, HeartbeatTimestamp::CLIENT_HEARTBEAT_TYPE_ID, clientId));

        allocate(heartbeatId, OTHER_TYPE_ID, 100 + heartbeatId);
    }
}

TEST_F(HeartbeatTimestampTest, shouldRejectCounterIdOutOfRangeInIsActive)
{
    EXPECT_THROW(
        HeartbeatTimestamp::isActive(m_reader, NUM_COUNTERS, HeartbeatTimestamp::CLIENT_HEARTBEAT_TYPE_ID, 42),
        util::IllegalArgumentException);
    EXPECT_THROW(
        HeartbeatTimestamp::isActive(m_reader, -1, HeartbeatTimestamp::CLIENT_HEARTBEAT_TYPE_ID, 42),
        util::IllegalArgumentException);
}
