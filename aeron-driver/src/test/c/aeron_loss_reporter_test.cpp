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
#include <functional>

#include <gtest/gtest.h>

extern "C"
{
#include "reports/aeron_loss_reporter.h"
}

#define CAPACITY (1024)

#define SESSION_ID (0x0EADBEEF)
#define STREAM_ID (0x12A)

typedef std::array<std::uint8_t, CAPACITY> buffer_t;

class LossReporterTest : public testing::Test
{
public:
    LossReporterTest() :
        m_ptr(m_buffer.data())
    {
        m_buffer.fill(0);
    }

    static void on_loss_entry(
        void *clientd,
        int64_t observation_count,
        int64_t total_bytes_lost,
        int64_t first_observation_timestamp,
        int64_t last_observation_timestamp,
        int32_t session_id,
        int32_t stream_id,
        const char *channel,
        int32_t channel_length,
        const char *source,
        int32_t source_length)
    {
        auto *t = (LossReporterTest *)clientd;

        t->m_on_loss_entry(
            observation_count,
            total_bytes_lost,
            first_observation_timestamp,
            last_observation_timestamp,
            session_id,
            stream_id,
            channel,
            channel_length,
            source,
            source_length);
    }

protected:
    buffer_t m_buffer = {};
    uint8_t *m_ptr = nullptr;
    aeron_loss_reporter_t m_reporter = {};
    std::function<void(int64_t, int64_t, int64_t, int64_t, int32_t, int32_t, const char *, int32_t, const char *, int32_t)>
        m_on_loss_entry;
};

TEST_F(LossReporterTest, shouldCreateEntry)
{
    ASSERT_EQ(aeron_loss_reporter_init(&m_reporter, m_ptr, CAPACITY), 0);

    const int64_t initial_bytes_lost = 32;
    const int64_t timestamp_ms = 7;
    const char *channel = "aeron:udp://stuff";
    const char *source = "127.0.0.1:8888";

    aeron_loss_reporter_entry_offset_t offset = aeron_loss_reporter_create_entry(
        &m_reporter,
        initial_bytes_lost,
        timestamp_ms,
        SESSION_ID,
        STREAM_ID,
        channel,
        strlen(channel),
        source,
        strlen(source));

    EXPECT_EQ(offset, 0);

    auto *entry = (aeron_loss_reporter_entry_t *)m_ptr;
    EXPECT_EQ(entry->observation_count, 1);
    EXPECT_EQ(entry->total_bytes_lost, initial_bytes_lost);
    EXPECT_EQ(entry->first_observation_timestamp, timestamp_ms);
    EXPECT_EQ(entry->last_observation_timestamp, timestamp_ms);
    EXPECT_EQ(entry->session_id, SESSION_ID);
    EXPECT_EQ(entry->stream_id, STREAM_ID);

    const char *channel_ptr = (const char *)(m_ptr + sizeof(aeron_loss_reporter_entry_t));
    EXPECT_EQ(*(int32_t *)(channel_ptr), (int32_t)strlen(channel));
    EXPECT_EQ(std::string(channel_ptr + sizeof(int32_t), strlen(channel)), std::string(channel));

    const char *source_ptr = (const char *)(
        m_ptr +
            sizeof(aeron_loss_reporter_entry_t) +
            AERON_ALIGN((sizeof(int32_t) + strlen(channel)), sizeof(int32_t)));
    EXPECT_EQ(*(int32_t *)(source_ptr), (int32_t)strlen(source));
    EXPECT_EQ(std::string(source_ptr + sizeof(int32_t), strlen(source)), std::string(source));
}

TEST_F(LossReporterTest, shouldUpdateEntry)
{
    ASSERT_EQ(aeron_loss_reporter_init(&m_reporter, m_ptr, CAPACITY), 0);

    const int64_t initial_bytes_lost = 32;
    const int64_t timestamp_ms = 7;
    const char *channel = "aeron:udp://stuff";
    const char *source = "127.0.0.1:8888";

    aeron_loss_reporter_entry_offset_t offset = aeron_loss_reporter_create_entry(
        &m_reporter,
        initial_bytes_lost,
        timestamp_ms,
        SESSION_ID,
        STREAM_ID,
        channel,
        strlen(channel),
        source,
        strlen(source));

    EXPECT_EQ(offset, 0);

    int64_t additional_bytes_lost = 64;
    int64_t latest_timestamp = 10;

    aeron_loss_reporter_record_observation(&m_reporter, offset, additional_bytes_lost, latest_timestamp);

    auto *entry = (aeron_loss_reporter_entry_t *)m_ptr;
    EXPECT_EQ(entry->observation_count, 2);
    EXPECT_EQ(entry->total_bytes_lost, initial_bytes_lost + additional_bytes_lost);
    EXPECT_EQ(entry->last_observation_timestamp, latest_timestamp);
}

TEST_F(LossReporterTest, shouldReadNoEntriesInEmptyReport)
{
    size_t called = 0;
    m_on_loss_entry =
        [&](int64_t observation_count,
            int64_t total_bytes_lost,
            int64_t first_observation_timestamp,
            int64_t last_observation_timestamp,
            int32_t session_id,
            int32_t stream_id,
            const char *channel,
            int32_t channel_length,
            const char *source,
            int32_t source_length)
        {
            called++;
        };

    EXPECT_EQ(aeron_loss_reporter_read(m_ptr, CAPACITY, LossReporterTest::on_loss_entry, this), 0u);
    EXPECT_EQ(called, 0u);
}

TEST_F(LossReporterTest, shouldReadOneEntry)
{
    ASSERT_EQ(aeron_loss_reporter_init(&m_reporter, m_ptr, CAPACITY), 0);

    const int64_t initial_bytes_lost = 32;
    const int64_t timestamp_ms = 7;
    const char *channel = "aeron:udp://stuff";
    const char *source = "127.0.0.1:8888";

    aeron_loss_reporter_entry_offset_t offset = aeron_loss_reporter_create_entry(
        &m_reporter,
        initial_bytes_lost,
        timestamp_ms,
        SESSION_ID,
        STREAM_ID,
        channel,
        strlen(channel),
        source,
        strlen(source));

    EXPECT_EQ(offset, 0);

    size_t called = 0;
    m_on_loss_entry =
        [&](int64_t observation_count,
            int64_t total_bytes_lost,
            int64_t first_observation_timestamp,
            int64_t last_observation_timestamp,
            int32_t session_id,
            int32_t stream_id,
            const char *read_channel,
            int32_t channel_length,
            const char *read_source,
            int32_t source_length)
        {
            EXPECT_EQ(observation_count, 1);
            EXPECT_EQ(total_bytes_lost, initial_bytes_lost);
            EXPECT_EQ(first_observation_timestamp, timestamp_ms);
            EXPECT_EQ(last_observation_timestamp, timestamp_ms);
            EXPECT_EQ(session_id, SESSION_ID);
            EXPECT_EQ(stream_id, STREAM_ID);
            EXPECT_EQ(std::string(read_channel, channel_length), std::string(channel));
            EXPECT_EQ(std::string(read_source, source_length), std::string(source));
            called++;
        };

    EXPECT_EQ(aeron_loss_reporter_read(m_ptr, CAPACITY, LossReporterTest::on_loss_entry, this), 1u);
    EXPECT_EQ(called, 1u);
}

TEST_F(LossReporterTest, shouldReadTwoEntries)
{
    ASSERT_EQ(aeron_loss_reporter_init(&m_reporter, m_ptr, CAPACITY), 0);

    const int64_t initial_bytes_lost_1 = 32;
    const int64_t timestamp_ms_1 = 7;
    const int32_t session_id_1 = SESSION_ID;
    const int32_t stream_id_1 = STREAM_ID;
    const char *channel_1 = "aeron:udp://stuff";
    const char *source_1 = "127.0.0.1:8888";

    const int64_t initial_bytes_lost_2 = 48;
    const int64_t timestamp_ms_2 = 17;
    const int32_t session_id_2 = SESSION_ID + 1;
    const int32_t stream_id_2 = STREAM_ID + 1;
    const char *channel_2 = "aeron:udp://stuff2";
    const char *source_2 = "127.0.0.1:9999";

    aeron_loss_reporter_entry_offset_t offset_1 = aeron_loss_reporter_create_entry(
        &m_reporter,
        initial_bytes_lost_1,
        timestamp_ms_1,
        session_id_1,
        stream_id_1,
        channel_1,
        strlen(channel_1),
        source_1,
        strlen(source_1));

    aeron_loss_reporter_entry_offset_t offset_2 = aeron_loss_reporter_create_entry(
        &m_reporter,
        initial_bytes_lost_2,
        timestamp_ms_2,
        session_id_2,
        stream_id_2,
        channel_2,
        strlen(channel_2),
        source_2,
        strlen(source_2));

    EXPECT_EQ(offset_1, 0);
    EXPECT_GT(offset_2, 0);

    size_t called = 0;
    m_on_loss_entry =
        [&](
            int64_t observation_count,
            int64_t total_bytes_lost,
            int64_t first_observation_timestamp,
            int64_t last_observation_timestamp,
            int32_t session_id,
            int32_t stream_id,
            const char *read_channel,
            int32_t channel_length,
            const char *read_source,
            int32_t source_length)
        {
            called++;

            if (1 == called)
            {
                EXPECT_EQ(observation_count, 1);
                EXPECT_EQ(total_bytes_lost, initial_bytes_lost_1);
                EXPECT_EQ(first_observation_timestamp, timestamp_ms_1);
                EXPECT_EQ(last_observation_timestamp, timestamp_ms_1);
                EXPECT_EQ(session_id, session_id_1);
                EXPECT_EQ(stream_id, stream_id_1);
                EXPECT_EQ(std::string(read_channel, channel_length), std::string(channel_1));
                EXPECT_EQ(std::string(read_source, source_length), std::string(source_1));
            }
            else if (2 == called)
            {
                EXPECT_EQ(observation_count, 1);
                EXPECT_EQ(total_bytes_lost, initial_bytes_lost_2);
                EXPECT_EQ(first_observation_timestamp, timestamp_ms_2);
                EXPECT_EQ(last_observation_timestamp, timestamp_ms_2);
                EXPECT_EQ(session_id, session_id_2);
                EXPECT_EQ(stream_id, stream_id_2);
                EXPECT_EQ(std::string(read_channel, channel_length), std::string(channel_2));
                EXPECT_EQ(std::string(read_source, source_length), std::string(source_2));
            }
        };

    EXPECT_EQ(aeron_loss_reporter_read(m_ptr, CAPACITY, LossReporterTest::on_loss_entry, this), 2u);
    EXPECT_EQ(called, 2u);
}

TEST_F(LossReporterTest, shouldReadEntriesWhenChannelPaddingCrossesEntryAlignment)
{
    ASSERT_EQ(aeron_loss_reporter_init(&m_reporter, m_ptr, CAPACITY), 0);

    // with a 5 byte channel the 4 byte alignment of the channel adds 3 bytes, which takes the first entry from
    // 62 to 65 bytes, past the 64 byte entry alignment
    const char *channels[] = { "aeron", "aeron:udp?endpoint=localhost:20121", "aeron" };
    const char *source = "127.0.0.1";

    for (int i = 0; i < 3; i++)
    {
        ASSERT_GE(aeron_loss_reporter_create_entry(
            &m_reporter,
            100 * (i + 1),
            i,
            SESSION_ID + i,
            STREAM_ID + i,
            channels[i],
            strlen(channels[i]),
            source,
            strlen(source)), 0);
    }

    size_t called = 0;
    m_on_loss_entry =
        [&](
            int64_t observation_count,
            int64_t total_bytes_lost,
            int64_t first_observation_timestamp,
            int64_t last_observation_timestamp,
            int32_t session_id,
            int32_t stream_id,
            const char *read_channel,
            int32_t channel_length,
            const char *read_source,
            int32_t source_length)
        {
            ASSERT_LT(called, 3u);
            EXPECT_EQ(observation_count, 1);
            EXPECT_EQ(total_bytes_lost, 100 * (int64_t)(called + 1));
            EXPECT_EQ(session_id, SESSION_ID + (int32_t)called);
            EXPECT_EQ(stream_id, STREAM_ID + (int32_t)called);
            EXPECT_EQ(std::string(read_channel, (size_t)channel_length), channels[called]);
            EXPECT_EQ(std::string(read_source, (size_t)source_length), source);
            called++;
        };

    EXPECT_EQ(aeron_loss_reporter_read(m_ptr, CAPACITY, LossReporterTest::on_loss_entry, this), 3u);
    EXPECT_EQ(called, 3u);
}
