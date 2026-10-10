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

#include <cstdint>
#include <cstring>

#include <gtest/gtest.h>

extern "C"
{
#include "aeron_counters.h"
#include "concurrent/aeron_counters_manager.h"
#include "client/aeron_archive.h"
}

static const int32_t NUM_COUNTERS = 4;
static const int32_t OTHER_TYPE_ID = 1001;

class AeronArchiveRecordingPosTest : public testing::Test
{
protected:
    AeronArchiveRecordingPosTest()
    {
        aeron_counters_reader_init(&m_reader, m_metadata, sizeof(m_metadata), m_values, sizeof(m_values));
    }

    void allocate(int32_t counter_id, int32_t type_id, int64_t recording_id, int32_t session_id)
    {
        aeron_counter_metadata_descriptor_t *metadata =
            reinterpret_cast<aeron_counter_metadata_descriptor_t *>(m_metadata) + counter_id;
        std::memset(metadata->key, 0, sizeof(metadata->key));
        std::memcpy(metadata->key, &recording_id, sizeof(recording_id));
        std::memcpy(metadata->key + sizeof(recording_id), &session_id, sizeof(session_id));
        metadata->type_id = type_id;
        metadata->state = AERON_COUNTER_RECORD_ALLOCATED;
    }

    alignas(64) uint8_t m_metadata[NUM_COUNTERS * sizeof(aeron_counter_metadata_descriptor_t)] = {};
    alignas(64) uint8_t m_values[NUM_COUNTERS * sizeof(aeron_counter_value_descriptor_t)] = {};
    aeron_counters_reader_t m_reader = {};
};

TEST_F(AeronArchiveRecordingPosTest, shouldFindRecordingPositionCounterInEverySlotIncludingTheLast)
{
    ASSERT_EQ(NUM_COUNTERS - 1, aeron_counters_reader_max_counter_id(&m_reader));

    // the search stops at the first unused record, so allocate every slot
    for (int32_t i = 0; i < NUM_COUNTERS; i++)
    {
        allocate(i, OTHER_TYPE_ID, 100 + i, 200 + i);
    }

    for (int32_t counter_id = 0; counter_id < NUM_COUNTERS; counter_id++)
    {
        const int64_t recording_id = 10 + counter_id;
        const int32_t session_id = 20 + counter_id;
        allocate(counter_id, AERON_COUNTER_ARCHIVE_RECORDING_POSITION_TYPE_ID, recording_id, session_id);

        EXPECT_EQ(counter_id, aeron_archive_recording_pos_find_counter_id_by_recording_id(&m_reader, recording_id))
            << "recording position counter id " << counter_id;
        EXPECT_EQ(counter_id, aeron_archive_recording_pos_find_counter_id_by_session_id(&m_reader, session_id))
            << "recording position counter id " << counter_id;

        allocate(counter_id, OTHER_TYPE_ID, 100 + counter_id, 200 + counter_id);
    }
}
