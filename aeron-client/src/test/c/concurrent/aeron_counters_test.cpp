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

#include <array>
#include <cstdint>

#include <gtest/gtest.h>

extern "C"
{
#include "concurrent/aeron_counters_manager.h"
}

#define CAPACITY (1024)
#define METADATA_CAPACITY (CAPACITY * 4)
#define MSG_TYPE_ID (101)

typedef std::array<std::uint8_t, CAPACITY> values_buffer_t;
typedef std::array<std::uint8_t, METADATA_CAPACITY> metadata_buffer_t;

static const int REUSE_TIMEOUT = 5000;

class CountersTest : public testing::Test
{
public:

    CountersTest()
    {
        m_valuesBuffer.fill(0);
        m_metadataBuffer.fill(0);

        aeron_counters_manager_init(
            &m_manager,
            m_metadataBuffer.data(),
            m_metadataBuffer.size(),
            m_valuesBuffer.data(),
            m_valuesBuffer.size(),
            &m_cached_clock,
            REUSE_TIMEOUT);

        if (aeron_counters_reader_init(
            &m_reader,
            m_metadataBuffer.data(),
            m_metadataBuffer.size(),
            m_valuesBuffer.data(),
            m_valuesBuffer.size()) < 0)
        {
            std::string message = "aeron_counters_reader_init failed: ";
            message += aeron_errmsg();
            throw std::runtime_error(message);
        }
    }

    ~CountersTest() override
    {
        aeron_counters_manager_close(&m_manager);
    }

protected:
    values_buffer_t m_valuesBuffer = {};
    metadata_buffer_t m_metadataBuffer = {};
    aeron_counters_reader_t m_reader = {};
    aeron_counters_manager_t m_manager = {};
    aeron_clock_cache_t m_cached_clock = {};
};

TEST_F(CountersTest, shouldReadCounterState)
{
    int32_t state;

    EXPECT_EQ(0, aeron_counters_reader_counter_state(&m_reader, 0, &state));
    EXPECT_EQ(AERON_COUNTER_RECORD_UNUSED, state);

    int32_t id = aeron_counters_manager_allocate(&m_manager, 1234, nullptr, 0, nullptr, 0);

    EXPECT_EQ(0, aeron_counters_reader_counter_state(&m_reader, id, &state));
    EXPECT_EQ(AERON_COUNTER_RECORD_ALLOCATED, state);

    aeron_counters_manager_free(&m_manager, id);

    EXPECT_EQ(0, aeron_counters_reader_counter_state(&m_reader, id, &state));
    EXPECT_EQ(AERON_COUNTER_RECORD_RECLAIMED, state);

    EXPECT_EQ(-1, aeron_counters_reader_counter_state(&m_reader, m_reader.max_counter_id + 1, &state));
    EXPECT_EQ(-1, aeron_counters_reader_counter_state(&m_reader, -1, &state));
}

TEST_F(CountersTest, shouldReadCounterTypeId)
{
    int32_t typeId;
    int32_t expectedTypeId = 1234;

    int32_t id = aeron_counters_manager_allocate(&m_manager, expectedTypeId, nullptr, 0, nullptr, 0);

    EXPECT_EQ(0, aeron_counters_reader_counter_type_id(&m_reader, id, &typeId));
    EXPECT_EQ(expectedTypeId, typeId);
}

TEST_F(CountersTest, shouldReadCounterLabel)
{
    const char *label = "label as text";
    char buffer[AERON_COUNTERS_MANAGER_METADATA_LENGTH];
    memset(buffer, 0, AERON_COUNTERS_MANAGER_METADATA_LENGTH);

    EXPECT_EQ(0, aeron_counters_reader_counter_label(&m_reader, 0, buffer, AERON_COUNTERS_MANAGER_METADATA_LENGTH));

    int32_t id = aeron_counters_manager_allocate(&m_manager, 1234, nullptr, 0, label, strlen(label));

    EXPECT_EQ(
        (int32_t)strlen(label),
        aeron_counters_reader_counter_label(&m_reader, id, buffer, AERON_COUNTERS_MANAGER_METADATA_LENGTH));
    EXPECT_STREQ(label, buffer);

    aeron_counters_manager_free(&m_manager, id);

    // We don't reject or change records when freed.
    EXPECT_EQ(13, aeron_counters_reader_counter_label(&m_reader, 0, buffer, AERON_COUNTERS_MANAGER_METADATA_LENGTH));

    EXPECT_EQ(-1, aeron_counters_reader_counter_label(&m_reader, -1, buffer, AERON_COUNTERS_MANAGER_METADATA_LENGTH));
    EXPECT_EQ(-1, aeron_counters_reader_counter_label(
        &m_reader, m_reader.max_counter_id + 1, buffer, AERON_COUNTERS_MANAGER_METADATA_LENGTH));
}

TEST_F(CountersTest, shouldReadTimeForReuseDeadline)
{
    int64_t deadline_ms;
    EXPECT_EQ(0, aeron_counters_reader_free_for_reuse_deadline_ms(&m_reader, 0, &deadline_ms));

    int32_t id = aeron_counters_manager_allocate(&m_manager, 1234, nullptr, 0, nullptr, 0);

    EXPECT_EQ(0, aeron_counters_reader_free_for_reuse_deadline_ms(&m_reader, id, &deadline_ms));
    EXPECT_EQ(AERON_COUNTER_NOT_FREE_TO_REUSE, deadline_ms);

    int64_t now_ms = aeron_epoch_clock();
    aeron_clock_update_cached_time(&m_cached_clock, now_ms, 0);
    aeron_counters_manager_free(&m_manager, id);

    EXPECT_EQ(0, aeron_counters_reader_free_for_reuse_deadline_ms(&m_reader, id, &deadline_ms));
    EXPECT_LE(now_ms + REUSE_TIMEOUT, deadline_ms);

    EXPECT_EQ(
        -1, aeron_counters_reader_free_for_reuse_deadline_ms(&m_reader, m_reader.max_counter_id + 1, &deadline_ms));
    EXPECT_EQ(-1, aeron_counters_reader_free_for_reuse_deadline_ms(&m_reader, -1, &deadline_ms));
}

TEST_F(CountersTest, shouldSetRegistrationId)
{
    int32_t id = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, nullptr, 0);

    int64_t registration_id = 999;
    EXPECT_EQ(0, aeron_counters_reader_counter_registration_id(&m_reader, id, &registration_id));
    EXPECT_EQ(AERON_COUNTER_REGISTRATION_ID_DEFAULT, registration_id);

    int64_t expected_registration_id = 777;
    aeron_counters_manager_counter_registration_id(&m_manager, id, expected_registration_id);
    EXPECT_EQ(0, aeron_counters_reader_counter_registration_id(&m_reader, id, &registration_id));
    EXPECT_EQ(expected_registration_id, registration_id);
}

TEST_F(CountersTest, shouldResetValueAndRegistrationIdIfReused)
{
    int32_t id_one = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, nullptr, 0);

    int64_t registration_id_one = 999;
    EXPECT_EQ(0, aeron_counters_reader_counter_registration_id(&m_reader, id_one, &registration_id_one));
    EXPECT_EQ(AERON_COUNTER_REGISTRATION_ID_DEFAULT, registration_id_one);

    int64_t expected_registration_id_one = 777;
    aeron_counters_manager_counter_registration_id(&m_manager, id_one, expected_registration_id_one);
    EXPECT_EQ(0, aeron_counters_reader_counter_registration_id(&m_reader, id_one, &registration_id_one));
    EXPECT_EQ(expected_registration_id_one, registration_id_one);

    m_manager.free_to_reuse_timeout_ms = 0;
    aeron_counters_manager_free(&m_manager, id_one);

    int32_t id_two = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, nullptr, 0);

    int64_t registration_id_two = 999;
    EXPECT_EQ(0, aeron_counters_reader_counter_registration_id(&m_reader, id_one, &registration_id_two));
    EXPECT_EQ(id_one, id_two);
    EXPECT_EQ(AERON_COUNTER_REGISTRATION_ID_DEFAULT, registration_id_two);

    int64_t expected_registration_id_two = 333;
    aeron_counters_manager_counter_registration_id(&m_manager, id_two, expected_registration_id_two);
    EXPECT_EQ(0, aeron_counters_reader_counter_registration_id(&m_reader, id_two, &registration_id_two));
    EXPECT_EQ(expected_registration_id_two, registration_id_two);
}

TEST_F(CountersTest, shouldSetOwnerId)
{
    int32_t id = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, nullptr, 0);

    int64_t owner_id = 999;
    EXPECT_EQ(0, aeron_counters_reader_counter_owner_id(&m_reader, id, &owner_id));
    EXPECT_EQ(AERON_COUNTER_OWNER_ID_DEFAULT, owner_id);

    int64_t expected_owner_id = 777;
    aeron_counters_manager_counter_owner_id(&m_manager, id, expected_owner_id);
    EXPECT_EQ(0, aeron_counters_reader_counter_owner_id(&m_reader, id, &owner_id));
    EXPECT_EQ(expected_owner_id, owner_id);
}

TEST_F(CountersTest, shouldResetValueAndOwnerIdIfReused)
{
    int32_t id_one = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, nullptr, 0);

    int64_t owner_id_one = 999;
    EXPECT_EQ(0, aeron_counters_reader_counter_owner_id(&m_reader, id_one, &owner_id_one));
    EXPECT_EQ(AERON_COUNTER_REGISTRATION_ID_DEFAULT, owner_id_one);

    int64_t expected_owner_id_one = 777;
    aeron_counters_manager_counter_owner_id(&m_manager, id_one, expected_owner_id_one);
    EXPECT_EQ(0, aeron_counters_reader_counter_owner_id(&m_reader, id_one, &owner_id_one));
    EXPECT_EQ(expected_owner_id_one, owner_id_one);

    m_manager.free_to_reuse_timeout_ms = 0;
    aeron_counters_manager_free(&m_manager, id_one);

    int32_t id_two = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, nullptr, 0);

    int64_t owner_id_two = 999;
    EXPECT_EQ(0, aeron_counters_reader_counter_owner_id(&m_reader, id_one, &owner_id_two));
    EXPECT_EQ(id_one, id_two);
    EXPECT_EQ(AERON_COUNTER_REGISTRATION_ID_DEFAULT, owner_id_two);

    int64_t expected_owner_id_two = 333;
    aeron_counters_manager_counter_owner_id(&m_manager, id_two, expected_owner_id_two);
    EXPECT_EQ(0, aeron_counters_reader_counter_owner_id(&m_reader, id_two, &owner_id_two));
    EXPECT_EQ(expected_owner_id_two, owner_id_two);
}

TEST_F(CountersTest, shouldSetReferenceId)
{
    int32_t id = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, nullptr, 0);

    int64_t reference_id = 999;
    EXPECT_EQ(0, aeron_counters_reader_counter_reference_id(&m_reader, id, &reference_id));
    EXPECT_EQ(AERON_COUNTER_REFERENCE_ID_DEFAULT, reference_id);

    int64_t expected_reference_id = 777;
    aeron_counters_manager_counter_reference_id(&m_manager, id, expected_reference_id);
    EXPECT_EQ(0, aeron_counters_reader_counter_reference_id(&m_reader, id, &reference_id));
    EXPECT_EQ(expected_reference_id, reference_id);
}

TEST_F(CountersTest, shouldResetValueAndReferenceIdIfReused)
{
    int32_t id_one = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, nullptr, 0);

    int64_t reference_id_one = 999;
    EXPECT_EQ(0, aeron_counters_reader_counter_reference_id(&m_reader, id_one, &reference_id_one));
    EXPECT_EQ(AERON_COUNTER_REGISTRATION_ID_DEFAULT, reference_id_one);

    int64_t expected_reference_id_one = 777;
    aeron_counters_manager_counter_reference_id(&m_manager, id_one, expected_reference_id_one);
    EXPECT_EQ(0, aeron_counters_reader_counter_reference_id(&m_reader, id_one, &reference_id_one));
    EXPECT_EQ(expected_reference_id_one, reference_id_one);

    m_manager.free_to_reuse_timeout_ms = 0;
    aeron_counters_manager_free(&m_manager, id_one);

    int32_t id_two = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, nullptr, 0);

    int64_t reference_id_two = 999;
    EXPECT_EQ(0, aeron_counters_reader_counter_reference_id(&m_reader, id_one, &reference_id_two));
    EXPECT_EQ(id_one, id_two);
    EXPECT_EQ(AERON_COUNTER_REGISTRATION_ID_DEFAULT, reference_id_two);

    int64_t expected_reference_id_two = 333;
    aeron_counters_manager_counter_reference_id(&m_manager, id_two, expected_reference_id_two);
    EXPECT_EQ(0, aeron_counters_reader_counter_reference_id(&m_reader, id_two, &reference_id_two));
    EXPECT_EQ(expected_reference_id_two, reference_id_two);
}
