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
#include <string>

#include <gtest/gtest.h>

extern "C"
{
#include "aeron_counters.h"
#include "concurrent/aeron_counters_manager.h"
#include "client/aeron_archive.h"
}

static const int32_t NUM_COUNTERS = 4;

static constexpr size_t MAX_SOURCE_IDENTITY_LENGTH =
    AERON_COUNTER_MAX_KEY_LENGTH - sizeof(aeron_archive_recording_position_key_layout_t) - sizeof(int64_t);

class AeronArchiveRecordingPosTest : public testing::Test
{
protected:
    AeronArchiveRecordingPosTest()
    {
        if (aeron_counters_reader_init(
            &m_reader, m_metadata, sizeof(m_metadata), m_values, sizeof(m_values)) < 0)
        {
            throw std::runtime_error("aeron_counters_reader_init failed");
        }
        if (aeron_counters_manager_init(
            &m_counters_manager,
            m_metadata,
            sizeof(m_metadata),
            m_values,
            sizeof(m_values),
            &m_cached_clock,
            0) < 0)
        {
            throw std::runtime_error("aeron_counters_manager_init failed");
        }
    }

    int32_t allocateRecPosCounter(
        int64_t recordingId,
        int32_t sessionId,
        int32_t sourceIdentityLength,
        const char *sourceIdentity,
        int64_t archiveId)
    {
        uint8_t keyBuffer[AERON_COUNTER_MAX_KEY_LENGTH];
        memset(keyBuffer, 0, sizeof(keyBuffer));

        auto key = reinterpret_cast<aeron_archive_recording_position_key_layout_t *>(keyBuffer);
        key->recording_id = recordingId;
        key->session_id = sessionId;
        key->source_identity_length = sourceIdentityLength;
        uint8_t *source_identity = reinterpret_cast<uint8_t *>(keyBuffer + sizeof(aeron_archive_recording_position_key_layout_t));
        size_t length = strlen(sourceIdentity);
        memcpy(source_identity, sourceIdentity, length);
        size_t archiveIdOffset = AERON_MIN(
            MAX_SOURCE_IDENTITY_LENGTH, (size_t)sourceIdentityLength >= length ? length : sourceIdentityLength);
        int64_t *keyArchiveId = reinterpret_cast<int64_t *>(source_identity + archiveIdOffset);
        *keyArchiveId = archiveId;

        int32_t counter_id = aeron_counters_manager_allocate(
          &m_counters_manager,
          AERON_COUNTER_ARCHIVE_RECORDING_POSITION_TYPE_ID,
          keyBuffer,
          sizeof(keyBuffer),
          "test",
          sizeof("test"));

        return counter_id;
    }

    alignas(64) uint8_t m_metadata[NUM_COUNTERS * sizeof(aeron_counter_metadata_descriptor_t)] = {};
    alignas(64) uint8_t m_values[NUM_COUNTERS * sizeof(aeron_counter_value_descriptor_t)] = {};
    aeron_clock_cache_t m_cached_clock;
    aeron_counters_reader_t m_reader = {};
    aeron_counters_manager_t m_counters_manager = {};
};

TEST_F(AeronArchiveRecordingPosTest, shouldFindRecordingPositionCounterInEverySlotIncludingTheLast)
{
    ASSERT_EQ(NUM_COUNTERS - 1, aeron_counters_reader_max_counter_id(&m_reader));

    // the search stops at the first unused record, so allocate every slot
    for (int32_t i = 0; i < NUM_COUNTERS; i++)
    {
        ASSERT_GE(
            allocateRecPosCounter(-1, -1, MAX_SOURCE_IDENTITY_LENGTH, "", -1),
            0);
    }

    for (int32_t counter_id = 0; counter_id < NUM_COUNTERS; counter_id++)
    {
        const int64_t recording_id = 10 + counter_id;
        const int32_t session_id = 20 + counter_id;
        const int64_t archive_id = 30 + counter_id;

        auto *metadata =
            reinterpret_cast<aeron_counter_metadata_descriptor_t *>(m_metadata + AERON_COUNTER_METADATA_OFFSET(counter_id));
        auto rec_pos_key = reinterpret_cast<aeron_archive_recording_position_key_layout_t *>(metadata->key);
        rec_pos_key->recording_id = recording_id;
        rec_pos_key->session_id = session_id;
        auto rec_pos_archive_id = reinterpret_cast<int64_t *>(
            metadata->key + sizeof(aeron_archive_recording_position_key_layout_t) + MAX_SOURCE_IDENTITY_LENGTH);
        *rec_pos_archive_id = archive_id;

        EXPECT_EQ(counter_id, aeron_archive_recording_pos_find_counter_id_by_recording_id(&m_reader, recording_id, archive_id))
            << "recording position counter id " << counter_id;
        EXPECT_EQ(counter_id, aeron_archive_recording_pos_find_counter_id_by_session_id(&m_reader, session_id, archive_id))
            << "recording position counter id " << counter_id;
    }
}

TEST_F(AeronArchiveRecordingPosTest, shouldGetSourceIdentity)
{
    auto sourceIdentity = std::string("127.0.0.1");
    int32_t counter_id = allocateRecPosCounter(
        42,
        -19,
        (int32_t)sourceIdentity.length(),
        sourceIdentity.c_str(),
        8);
    ASSERT_GE(counter_id, 0);

    char buffer[AERON_COUNTER_MAX_KEY_LENGTH] = {};
    size_t length = sizeof(buffer);
    ASSERT_EQ(0, aeron_archive_recording_pos_get_source_identity(&m_reader, counter_id, buffer, &length));
    EXPECT_EQ(sourceIdentity, std::string(buffer, length));
}

TEST_F(AeronArchiveRecordingPosTest, shouldRejectNegativeSourceIdentityLength)
{
    int32_t counter_id = allocateRecPosCounter(
        0,
        555,
        -13,
        "",
        42);
    ASSERT_GE(counter_id, 0);

    char buffer[AERON_COUNTER_MAX_KEY_LENGTH];
    std::memset(buffer, 0x5A, sizeof(buffer));
    size_t length = sizeof(buffer);
    EXPECT_EQ(-1, aeron_archive_recording_pos_get_source_identity(&m_reader, 0, buffer, &length));
    EXPECT_EQ(EINVAL, aeron_errcode());

    for (size_t i = 0; i < sizeof(buffer); i++)
    {
        ASSERT_EQ(0x5A, (uint8_t)buffer[i]) << "byte " << i << " was overwritten";
    }
}

TEST_F(AeronArchiveRecordingPosTest, shouldLimitSourceIdentityToTheCounterKey)
{
    auto sourceIdentity = std::string("127.0.0.1");
    int32_t counter_id = allocateRecPosCounter(
        3,
        1,
        1000000,
        sourceIdentity.c_str(),
        15);
    ASSERT_GE(counter_id, 0);

    char buffer[AERON_COUNTER_MAX_KEY_LENGTH] = {};
    size_t length = sizeof(buffer);
    ASSERT_EQ(0, aeron_archive_recording_pos_get_source_identity(&m_reader, 0, buffer, &length));
    EXPECT_EQ(MAX_SOURCE_IDENTITY_LENGTH, length);
}
