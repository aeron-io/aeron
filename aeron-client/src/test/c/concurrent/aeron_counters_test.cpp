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

#include "gmock/gmock-matchers.h"

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

class CountersTestBase
{
public:

    CountersTestBase()
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

    ~CountersTestBase()
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

class CountersTest : public CountersTestBase, public testing::Test
{
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

class InvalidCounterTest : public CountersTestBase, public testing::TestWithParam<std::int32_t>
{
};

INSTANTIATE_TEST_SUITE_P(
    InvalidCounterTest,
    InvalidCounterTest,
    testing::Values(-1, 1000000));

TEST_P(InvalidCounterTest, shouldRejectCounterThatIsOutOfRangeInReaderAddr)
{
    int32_t counter_id = GetParam();
    EXPECT_EQ(nullptr, aeron_counters_reader_addr(&m_reader, counter_id));
    EXPECT_EQ(EINVAL, aeron_errcode());
    EXPECT_THAT(
        aeron_errmsg(),
        testing::HasSubstr(std::string("counter id is out range [0, 7]: ").append(std::to_string(counter_id))));
}

TEST_P(InvalidCounterTest, shouldRejectCounterThatIsOutOfRangeWhenReadingRegistrationId)
{
    int32_t counter_id = GetParam();
    int64_t registration_id = -1;
    EXPECT_EQ(-1, aeron_counters_reader_counter_registration_id(&m_reader, GetParam(), &registration_id));
    EXPECT_EQ(-1, registration_id);
    EXPECT_EQ(EINVAL, aeron_errcode());
    EXPECT_THAT(
        aeron_errmsg(),
        testing::HasSubstr(std::string("counter id is out range [0, 7]: ").append(std::to_string(counter_id))));
}

TEST_P(InvalidCounterTest, shouldRejectCounterThatIsOutOfRangeWhenReadingOwnerId)
{
    int64_t owner_id = -1;
    EXPECT_EQ(-1, aeron_counters_reader_counter_owner_id(&m_reader, GetParam(), &owner_id));
    EXPECT_EQ(-1, owner_id);
}

TEST_P(InvalidCounterTest, shouldRejectCounterThatIsOutOfRangeWhenReadingReferenceId)
{
    int64_t reference_id = -1;
    EXPECT_EQ(-1, aeron_counters_reader_counter_reference_id(&m_reader, GetParam(), &reference_id));
    EXPECT_EQ(-1, reference_id);
}

TEST_P(InvalidCounterTest, shouldRejectCounterThatIsOutOfRangeWhenReadingState)
{
    int32_t state = -1;
    EXPECT_EQ(-1, aeron_counters_reader_counter_state(&m_reader, GetParam(), &state));
    EXPECT_EQ(-1, state);
}

TEST_P(InvalidCounterTest, shouldRejectCounterThatIsOutOfRangeWhenReadingTypeId)
{
    int32_t typeId = -1;
    EXPECT_EQ(-1, aeron_counters_reader_counter_type_id(&m_reader, GetParam(), &typeId));
    EXPECT_EQ(-1, typeId);
}

TEST_P(InvalidCounterTest, shouldRejectCounterThatIsOutOfRangeWhenReadingMetadataKey)
{
    uint8_t *key = nullptr;
    EXPECT_EQ(-1, aeron_counters_reader_metadata_key(&m_reader, GetParam(), &key));
    EXPECT_EQ(nullptr, key);
}

TEST_P(InvalidCounterTest, shouldRejectCounterThatIsOutOfRangeWhenReadingCounterLabel)
{
    char label[100] = {};
    label[0] = '\0';
    EXPECT_EQ(-1, aeron_counters_reader_counter_label(&m_reader, GetParam(), label, sizeof(label)));
    EXPECT_EQ('\0', label[0]);
}

TEST_P(InvalidCounterTest, shouldRejectCounterThatIsOutOfRangeWhenReadingReuseDeadline)
{
    int64_t deadline_ms = -1;
    EXPECT_EQ(-1, aeron_counters_reader_free_for_reuse_deadline_ms(&m_reader, GetParam(), &deadline_ms));
    EXPECT_EQ(-1, deadline_ms);
}

TEST_P(InvalidCounterTest, shouldRejectCounterThatIsOutOfRangeInManagerAddr)
{
    int32_t counter_id = GetParam();
    EXPECT_EQ(nullptr, aeron_counters_manager_addr(&m_manager, counter_id));
    EXPECT_EQ(EINVAL, aeron_errcode());
    EXPECT_THAT(
        aeron_errmsg(),
        testing::HasSubstr(std::string("counter id is out range [0, 7]: ").append(std::to_string(counter_id))));
}

TEST_P(InvalidCounterTest, shouldRejectCounterThatIsOutOfRangeWhenSettingRegistrationId)
{
    int32_t counter_id = GetParam();
    int64_t registration_id = 5;
    EXPECT_EQ(-1, aeron_counters_manager_counter_registration_id(&m_manager, counter_id, registration_id));
    EXPECT_EQ(nullptr, aeron_counters_manager_addr(&m_manager, counter_id));
    EXPECT_EQ(EINVAL, aeron_errcode());
    EXPECT_THAT(
        aeron_errmsg(),
        testing::HasSubstr(std::string("counter id is out range [0, 7]: ").append(std::to_string(counter_id))));
}

TEST_P(InvalidCounterTest, shouldRejectCounterThatIsOutOfRangeWhenSettingOwnerId)
{
    int32_t counter_id = GetParam();
    int64_t owner_id = 7;
    EXPECT_EQ(-1, aeron_counters_manager_counter_owner_id(&m_manager, counter_id, owner_id));
}

TEST_P(InvalidCounterTest, shouldRejectCounterThatIsOutOfRangeWhenSettingReferenceId)
{
    int32_t counter_id = GetParam();
    int64_t ref_id = 42;
    EXPECT_EQ(-1, aeron_counters_manager_counter_reference_id(&m_manager, counter_id, ref_id));
}

TEST_P(InvalidCounterTest, shouldRejectCounterThatIsOutOfRangeWhenUpdatingLabel)
{
    int32_t counter_id = GetParam();
    const char *label = "label";
    EXPECT_EQ(-1, aeron_counters_manager_update_label(&m_manager, counter_id, strlen(label), label));
}

TEST_P(InvalidCounterTest, shouldRejectCounterThatIsOutOfRangeWhenAppendingToLabel)
{
    int32_t counter_id = GetParam();
    const char *label = "label";
    EXPECT_EQ(-1, aeron_counters_manager_append_to_label(&m_manager, counter_id, strlen(label), label));
}
