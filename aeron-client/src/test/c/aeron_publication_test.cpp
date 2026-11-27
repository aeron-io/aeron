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
#include <exception>
#include <string>

#include <gtest/gtest.h>

#include "aeron_client_test_utils.h"

extern "C"
{
#include "aeron_log_buffer.h"
#include "aeron_publication.h"
#include "aeron_counters.h"
}

#define PAGE_SIZE (4 * 1024)
#define MTU_LENGTH (4 * 1024)
#define TERM_LENGTH (1024 * 1024)
#define MAX_MESSAGE_SIZE (TERM_LENGTH >> 3)
#define MAX_PAYLOAD_SIZE (MTU_LENGTH - AERON_DATA_HEADER_LENGTH)

#define PUB_URI "aeron:udp?endpoint=localhost:12345|alias=test"
#define STREAM_ID (101)
#define SESSION_ID (110)
#define REGISTRATION_ID (27)
#define CHANNEL_STATUS_INDICATOR_ID (45)
#define SUBSCRIBER_POSITION_ID (49)

using namespace aeron::test;

class PublicationTest : public testing::Test
{
public:
    aeron_log_buffer_t *createLogBuffer()
    {
        m_filename = tempFileName();
        aeron_log_buffer_t *log_buffer = nullptr;
        createLogFile(m_filename, TERM_LENGTH, INITIAL_TERM_ID);

        if (aeron_log_buffer_create(&log_buffer, m_filename.c_str(), 1, false) < 0)
        {
            throw std::runtime_error("could not create log_buffer: " + std::string(aeron_errmsg()));
        }

        log_buffer->mapped_raw_log.term_length = TERM_LENGTH;
        auto *log_meta_data = (aeron_logbuffer_metadata_t *)log_buffer->mapped_raw_log.log_meta_data.addr;
        log_meta_data->term_length = TERM_LENGTH;
        log_meta_data->mtu_length = MTU_LENGTH;
        log_meta_data->initial_term_id = INITIAL_TERM_ID;
        log_meta_data->page_size = PAGE_SIZE;
        log_meta_data->is_connected = true;

        return log_buffer;
    }

    static aeron_publication_t *createPublication(
        aeron_client_conductor_t *conductor,
        aeron_log_buffer_t *log_buffer,
        int32_t position_limit_counter_id,
        int64_t *position_limit_addr,
        int32_t channel_status_indicator_id,
        int64_t *channel_status_addr)
    {
        aeron_publication_t *publication = nullptr;

        if (aeron_publication_create(
            &publication,
            conductor,
            ::strdup(PUB_URI),
            STREAM_ID,
            SESSION_ID,
            position_limit_counter_id,
            position_limit_addr,
            channel_status_indicator_id,
            channel_status_addr,
            log_buffer,
            REGISTRATION_ID,
            REGISTRATION_ID) < 0)
        {
            throw std::runtime_error("could not create publication: " + std::string(aeron_errmsg()));
        }

        return publication;
    }

    static int64_t packTail(int32_t term_id, int32_t term_offset)
    {
        return ((int64_t)term_id << 32) | term_offset;
    }

    static int64_t reserved_value_supplier(void *clientd, uint8_t *buffer, size_t frame_length)
    {
        return (int64_t)frame_length * 19;
    }

    static int64_t frame_term_offset_reserved_value_supplier(void *clientd, uint8_t *buffer, size_t frame_length)
    {
        return reinterpret_cast<aeron_data_header_t *>(buffer)->term_offset;
    }

    aeron_data_header_t *verifyHeader(
        const aeron_mapped_buffer_t *term_buffer,
        const int32_t term_offset,
        const int32_t expected_frame_length,
        const int32_t expected_term_id,
        const uint8_t expected_flags)
    {
        auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
        EXPECT_EQ(expected_frame_length, header->frame_header.frame_length);
        EXPECT_EQ(AERON_HDR_TYPE_DATA, header->frame_header.type);
        EXPECT_EQ(AERON_FRAME_HEADER_VERSION, header->frame_header.version);
        EXPECT_EQ(expected_flags, header->frame_header.flags);
        EXPECT_EQ(term_offset, header->term_offset);
        EXPECT_EQ(expected_term_id, header->term_id);
        EXPECT_EQ(m_publication->session_id, header->session_id);
        EXPECT_EQ(m_publication->stream_id, header->stream_id);
        return header;
    }

protected:
    aeron_client_conductor_t *m_conductor = nullptr;
    aeron_log_buffer_t *m_log_buffer = nullptr;
    aeron_publication_t *m_publication = nullptr;
    std::string m_filename;

    int64_t *m_position_limit_addr = nullptr;
    int64_t *m_channel_status_addr = nullptr;

    static const size_t NUM_COUNTERS = 4;
    std::array<std::uint8_t, NUM_COUNTERS * AERON_COUNTERS_MANAGER_METADATA_LENGTH> m_counters_metadata = {};
    std::array<std::uint8_t, NUM_COUNTERS * AERON_COUNTERS_MANAGER_VALUE_LENGTH> m_counters_values = {};
    aeron_counters_manager_t m_counters_manager = {};
    aeron_clock_cache_t m_cached_clock = {};

    void SetUp() override
    {
        m_counters_metadata.fill(0);
        m_counters_values.fill(0);
        aeron_counters_manager_init(
            &m_counters_manager,
            m_counters_metadata.data(),
            m_counters_metadata.size(),
            m_counters_values.data(),
            m_counters_values.size(),
            &m_cached_clock,
            0);

        m_conductor = {};
        m_log_buffer = createLogBuffer();

        const int32_t position_limit_counter_id = aeron_counters_manager_allocate(
            &m_counters_manager,
            AERON_COUNTER_PUBLISHER_LIMIT_TYPE_ID,
            nullptr,
            0,
            AERON_COUNTER_PUBLISHER_LIMIT_NAME,
            sizeof(AERON_COUNTER_PUBLISHER_LIMIT_NAME));
        if (position_limit_counter_id < 0)
        {
            throw std::runtime_error("could not create counter: " + std::string(aeron_errmsg()));
        }
        m_position_limit_addr = aeron_counters_manager_addr(&m_counters_manager, position_limit_counter_id);
        aeron_counter_set_release(m_position_limit_addr, INT64_MAX);

        const int32_t channel_status_indicator_id = aeron_counters_manager_allocate(
            &m_counters_manager,
            AERON_COUNTER_SEND_CHANNEL_STATUS_TYPE_ID,
            nullptr,
            0,
            AERON_COUNTER_SEND_CHANNEL_STATUS_NAME,
            sizeof(AERON_COUNTER_SEND_CHANNEL_STATUS_NAME));
        if (channel_status_indicator_id < 0)
        {
            throw std::runtime_error("could not create counter: " + std::string(aeron_errmsg()));
        }
        m_channel_status_addr = aeron_counters_manager_addr(&m_counters_manager, channel_status_indicator_id);

        m_publication = createPublication(
            m_conductor,
            m_log_buffer,
            position_limit_counter_id,
            m_position_limit_addr,
            channel_status_indicator_id,
            m_channel_status_addr);
    }

    void TearDown() override
    {
        if (nullptr != m_publication)
        {
            aeron_publication_delete(m_publication);
        }
        if (!m_filename.empty())
        {
            if (nullptr != m_log_buffer)
            {
                aeron_log_buffer_delete(m_log_buffer);
            }

            ::unlink(m_filename.c_str());
        }
        aeron_counters_manager_close(&m_counters_manager);
    }
};

TEST_F(PublicationTest, offerUnfragmentedMessage)
{
    const char *payload = "Aeron is awesome!";
    const size_t length = strlen(payload);
    const int32_t term_count = 16;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = 4096;
    const int32_t term_id = m_publication->initial_term_id + term_count;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);

    const int64_t position = aeron_publication_offer(
        m_publication,
        reinterpret_cast<const uint8_t *>(payload),
        length,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(16781376, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    const int32_t frame_length = static_cast<int32_t>(length) + static_cast<int32_t>(AERON_DATA_HEADER_LENGTH);
    const auto header = verifyHeader(
        term_buffer,
        term_offset,
        frame_length,
        term_id,
        AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG);
    EXPECT_EQ((int64_t)frame_length * 19, header->reserved_value);
    EXPECT_EQ(0, memcmp(term_buffer->addr + term_offset + AERON_DATA_HEADER_LENGTH, payload, length));
}

TEST_F(PublicationTest, offerFragmentedMessage)
{
    uint8_t msgBuffer[MAX_MESSAGE_SIZE];
    memset(msgBuffer, 'x', sizeof(msgBuffer));
    memset(msgBuffer + MAX_MESSAGE_SIZE / 2, 'a', MAX_MESSAGE_SIZE / 2);
    const size_t length = MAX_MESSAGE_SIZE;
    const int32_t term_count = 5;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = 512;
    const int32_t term_id = m_publication->initial_term_id + term_count;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);

    const int64_t position = aeron_publication_offer(
        m_publication,
        msgBuffer,
        length,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(5375520, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    // first frame
    auto header = verifyHeader(
        term_buffer,
        term_offset,
        static_cast<int32_t>(MTU_LENGTH),
        term_id,
        AERON_DATA_HEADER_BEGIN_FLAG);
    EXPECT_EQ((int64_t)MTU_LENGTH * 19, header->reserved_value);
    EXPECT_EQ(0, memcmp(
        term_buffer->addr + term_offset + AERON_DATA_HEADER_LENGTH, msgBuffer, MTU_LENGTH - AERON_DATA_HEADER_LENGTH));

    // last frame
    const int32_t last_frame_offset = term_offset + (MAX_MESSAGE_SIZE / MAX_PAYLOAD_SIZE) * MTU_LENGTH;
    const size_t last_frame_length = AERON_ALIGN((MAX_MESSAGE_SIZE % MAX_PAYLOAD_SIZE) + AERON_DATA_HEADER_LENGTH,
        AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const size_t last_data_chunk_length = last_frame_length - AERON_DATA_HEADER_LENGTH;
    header = verifyHeader(
        term_buffer,
        last_frame_offset,
        static_cast<int32_t>(last_frame_length),
        term_id,
        AERON_DATA_HEADER_END_FLAG);
    EXPECT_EQ((int64_t)last_frame_length * 19, header->reserved_value);
    EXPECT_EQ(0, memcmp(
        term_buffer->addr + last_frame_offset + AERON_DATA_HEADER_LENGTH,
        msgBuffer + (MAX_MESSAGE_SIZE - last_data_chunk_length), last_data_chunk_length));
}

TEST_F(PublicationTest, offerFragmentedMessageShouldPassEachFrameToReservedValueSupplier)
{
    const int32_t frame_count = 4;
    uint8_t msgBuffer[(MAX_PAYLOAD_SIZE * (frame_count - 1)) + 100];
    memset(msgBuffer, 'x', sizeof(msgBuffer));
    const int32_t term_count = 5;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = 512;
    const int32_t term_id = m_publication->initial_term_id + term_count;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);

    const int64_t position = aeron_publication_offer(
        m_publication,
        msgBuffer,
        sizeof(msgBuffer),
        frame_term_offset_reserved_value_supplier,
        nullptr);

    ASSERT_EQ(5255840, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    for (int32_t i = 0; i < frame_count; i++)
    {
        const int32_t frame_offset = term_offset + (i * MTU_LENGTH);
        const auto *header = reinterpret_cast<const aeron_data_header_t *>(term_buffer->addr + frame_offset);
        EXPECT_EQ(frame_offset, header->term_offset) << "frame " << i;
        EXPECT_EQ(frame_offset, header->reserved_value) << "frame " << i;
    }
}

TEST_F(PublicationTest, vectorOfferUnfragmentedMessage)
{
    const char *payload = "Aeron is awesome squared!";
    const size_t length = strlen(payload);
    aeron_iovec_t iov[2];
    iov[0].iov_base = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(payload));
    iov[0].iov_len = 5;
    iov[1].iov_base = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(payload)) + 5;
    iov[1].iov_len = length - 5;

    const int32_t term_count = 113;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = 3072;
    const int32_t term_id = m_publication->initial_term_id + term_count;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);

    const int64_t position = aeron_publication_offerv(
        m_publication,
        iov,
        2,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(118492224, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    const int32_t frame_length = static_cast<int32_t>(length) + static_cast<int32_t>(AERON_DATA_HEADER_LENGTH);
    const auto header = verifyHeader(
        term_buffer,
        term_offset,
        frame_length,
        term_id,
        AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG);
    EXPECT_EQ((int64_t)frame_length * 19, header->reserved_value);
    EXPECT_EQ(0, memcmp(term_buffer->addr + term_offset + AERON_DATA_HEADER_LENGTH, payload, length));
}

TEST_F(PublicationTest, vectorOfferFragmentedMessage)
{
    uint8_t msgBuffer1[111];
    uint8_t msgBuffer2[222];
    uint8_t msgBuffer3[MAX_MESSAGE_SIZE - sizeof(msgBuffer1) - sizeof(msgBuffer2)];
    memset(msgBuffer1, '1', sizeof(msgBuffer1));
    memset(msgBuffer2, '2', sizeof(msgBuffer2));
    memset(msgBuffer3, 'x', sizeof(msgBuffer3));
    aeron_iovec_t iov[3];
    iov[0].iov_base = msgBuffer1;
    iov[0].iov_len = sizeof(msgBuffer1);
    iov[1].iov_base = msgBuffer2;
    iov[1].iov_len = sizeof(msgBuffer2);
    iov[2].iov_base = msgBuffer3;
    iov[2].iov_len = sizeof(msgBuffer3);

    const int32_t term_count = 591;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = 8192;
    const int32_t term_id = m_publication->initial_term_id + term_count;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);

    const int64_t position = aeron_publication_offerv(
        m_publication,
        iov,
        3,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(619848736, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    // first frame
    auto header = verifyHeader(
        term_buffer,
        term_offset,
        static_cast<int32_t>(MTU_LENGTH),
        term_id,
        AERON_DATA_HEADER_BEGIN_FLAG);
    EXPECT_EQ((int64_t)MTU_LENGTH * 19, header->reserved_value);
    EXPECT_EQ(0, memcmp(
        term_buffer->addr + term_offset + AERON_DATA_HEADER_LENGTH, msgBuffer1, sizeof(msgBuffer1)));
    EXPECT_EQ(0, memcmp(
        term_buffer->addr + term_offset + AERON_DATA_HEADER_LENGTH + sizeof(msgBuffer1),
        msgBuffer2,
        sizeof(msgBuffer2)));
    EXPECT_EQ(0, memcmp(
        term_buffer->addr + term_offset + AERON_DATA_HEADER_LENGTH + sizeof(msgBuffer1) + sizeof(msgBuffer2),
        msgBuffer3,
        MAX_PAYLOAD_SIZE - (sizeof(msgBuffer1) + sizeof(msgBuffer2))));

    // last frame
    const int32_t last_frame_offset = term_offset + (MAX_MESSAGE_SIZE / MAX_PAYLOAD_SIZE) * MTU_LENGTH;
    const size_t last_frame_length = AERON_ALIGN((MAX_MESSAGE_SIZE % MAX_PAYLOAD_SIZE) + AERON_DATA_HEADER_LENGTH,
        AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const size_t last_data_chunk_length = last_frame_length - AERON_DATA_HEADER_LENGTH;
    header = verifyHeader(
        term_buffer,
        last_frame_offset,
        static_cast<int32_t>(last_frame_length),
        term_id,
        AERON_DATA_HEADER_END_FLAG);
    EXPECT_EQ((int64_t)last_frame_length * 19, header->reserved_value);
    EXPECT_EQ(0, memcmp(
        term_buffer->addr + last_frame_offset + AERON_DATA_HEADER_LENGTH,
        msgBuffer3 + (sizeof(msgBuffer3) - last_data_chunk_length), last_data_chunk_length));
}

TEST_F(PublicationTest, vectorOfferFragmentedMessageShouldPassEachFrameToReservedValueSupplier)
{
    const int32_t frame_count = 4;
    uint8_t msgBuffer[(MAX_PAYLOAD_SIZE * (frame_count - 1)) + 100];
    memset(msgBuffer, 'x', sizeof(msgBuffer));
    aeron_iovec_t iov[2];
    iov[0].iov_base = msgBuffer;
    iov[0].iov_len = 333;
    iov[1].iov_base = msgBuffer + 333;
    iov[1].iov_len = sizeof(msgBuffer) - 333;
    const int32_t term_count = 5;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = 512;
    const int32_t term_id = m_publication->initial_term_id + term_count;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);

    const int64_t position = aeron_publication_offerv(
        m_publication,
        iov,
        2,
        frame_term_offset_reserved_value_supplier,
        nullptr);

    ASSERT_EQ(5255840, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    for (int32_t i = 0; i < frame_count; i++)
    {
        const int32_t frame_offset = term_offset + (i * MTU_LENGTH);
        const auto *header = reinterpret_cast<const aeron_data_header_t *>(term_buffer->addr + frame_offset);
        EXPECT_EQ(frame_offset, header->term_offset) << "frame " << i;
        EXPECT_EQ(frame_offset, header->reserved_value) << "frame " << i;
    }
}

TEST_F(PublicationTest, tryClaimMaxPayloadSize)
{
    aeron_buffer_claim_t buffer_claim = {};
    const int32_t term_count = 3;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = 96;
    const int32_t term_id = m_publication->initial_term_id + term_count;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);

    const int64_t position = aeron_publication_try_claim(
        m_publication,
        MAX_PAYLOAD_SIZE,
        &buffer_claim);

    ASSERT_EQ(3149920, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    const auto header = verifyHeader(
        term_buffer,
        term_offset,
        -static_cast<int32_t>(MTU_LENGTH),
        term_id,
        AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG);
    EXPECT_EQ(AERON_DATA_HEADER_DEFAULT_RESERVED_VALUE, header->reserved_value);
    EXPECT_NE(nullptr, buffer_claim.frame_header);
    EXPECT_EQ(buffer_claim.frame_header + AERON_DATA_HEADER_LENGTH, buffer_claim.data);
    EXPECT_EQ(MAX_PAYLOAD_SIZE, buffer_claim.length);
}

TEST_F(PublicationTest, offerErrorIfPublicationIsNull)
{
    const char *payload = "Aeron is awesome!";
    const size_t length = strlen(payload);

    const int64_t position = aeron_publication_offer(
        nullptr,
        reinterpret_cast<const uint8_t *>(payload),
        length,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_ERROR, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[0];
    auto *header = (aeron_data_header_t *)(term_buffer->addr);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, offerErrorIfBufferIsNull)
{
    const int64_t position = aeron_publication_offer(
        m_publication,
        nullptr,
        10,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_ERROR, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[0];
    auto *header = (aeron_data_header_t *)(term_buffer->addr);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, offerClosed)
{
    const char *payload = "Aeron is awesome!";
    const size_t length = strlen(payload);
    const int32_t term_count = 16;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = 4096;
    m_publication->is_closed = true;

    const int64_t position = aeron_publication_offer(
        m_publication,
        reinterpret_cast<const uint8_t *>(payload),
        length,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_CLOSED, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, offerAdminActionIfTermCountDoesNotMatch)
{
    const char *payload = "Aeron is awesome!";
    const size_t length = strlen(payload);
    const int32_t term_offset = 4096;
    m_publication->log_meta_data->active_term_count = 5;

    const int64_t position = aeron_publication_offer(
        m_publication,
        reinterpret_cast<const uint8_t *>(payload),
        length,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_ADMIN_ACTION, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[0];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, offerBackPressureIfPublicationLimitReached)
{
    const char *payload = "Aeron is awesome!";
    const size_t length = strlen(payload);
    const int32_t term_count = 16;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = 4096;
    const int32_t term_id = m_publication->initial_term_id + term_count;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);
    const int64_t limit_position = aeron_logbuffer_compute_position(
        term_id, term_offset, m_publication->position_bits_to_shift, m_publication->initial_term_id);
    aeron_counter_set_release(m_position_limit_addr, limit_position);

    const int64_t position = aeron_publication_offer(
        m_publication,
        reinterpret_cast<const uint8_t *>(payload),
        length,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_BACK_PRESSURED, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, offerNotConnectedIfPublicationLimitReached)
{
    const char *payload = "Aeron is awesome!";
    const size_t length = strlen(payload);
    const int32_t term_count = 3;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = 1024;
    const int32_t term_id = m_publication->initial_term_id + term_count;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);
    m_publication->log_meta_data->is_connected = false;
    const int64_t limit_position = aeron_logbuffer_compute_position(
        term_id, term_offset - 32, m_publication->position_bits_to_shift, m_publication->initial_term_id);
    aeron_counter_set_release(m_position_limit_addr, limit_position);

    const int64_t position = aeron_publication_offer(
        m_publication,
        reinterpret_cast<const uint8_t *>(payload),
        length,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_NOT_CONNECTED, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, offerMaxPositionExceededIfPublicationLimitReached)
{
    const char *payload = "Aeron is awesome!";
    const size_t length = strlen(payload);
    const int32_t term_count = INT32_MAX;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const auto term_offset = (int32_t)(TERM_LENGTH - length - 1);
    const int32_t term_id = INT32_MIN + (INITIAL_TERM_ID - 1);
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);
    aeron_counter_set_release(m_position_limit_addr, 64);

    const int64_t position = aeron_publication_offer(
        m_publication,
        reinterpret_cast<const uint8_t *>(payload),
        length,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_MAX_POSITION_EXCEEDED, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, offerPublicationErrorIfMessageIsLargerThanMaxMessageSize)
{
    const char *payload = "Aeron is awesome!";
    const int32_t term_count = 5;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = (int32_t)TERM_LENGTH;
    const int32_t term_id = term_count + INITIAL_TERM_ID;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);

    const int64_t position = aeron_publication_offer(
        m_publication,
        reinterpret_cast<const uint8_t *>(payload),
        MAX_MESSAGE_SIZE + 1,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_ERROR, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, offerAdminActionAfterRolloingOverToTheNextTerm)
{
    const char *payload = "Aeron is awesome!";
    const size_t length = strlen(payload);
    const int32_t frame_length = AERON_ALIGN(length + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int32_t term_count = 51;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const size_t next_partition_index = aeron_logbuffer_index_by_term_count(term_count + 1);
    const int32_t term_offset = (int32_t)(TERM_LENGTH - AERON_DATA_HEADER_LENGTH - 8);
    const int32_t term_id = term_count + INITIAL_TERM_ID;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);
    m_publication->log_meta_data->term_tail_counters[next_partition_index] =
        packTail(term_id + 1 - AERON_LOGBUFFER_PARTITION_COUNT, 999888);

    const int64_t position = aeron_publication_offer(
        m_publication,
        reinterpret_cast<const uint8_t *>(payload),
        length,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_ADMIN_ACTION, position);
    EXPECT_EQ(
        packTail(term_id, term_offset + frame_length),
        m_publication->log_meta_data->term_tail_counters[partition_index]);
    EXPECT_EQ(
        packTail(term_id + 1, 0),
        m_publication->log_meta_data->term_tail_counters[next_partition_index]);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(AERON_DATA_HEADER_LENGTH + 8, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, offerMaxPositionExceededAfterSuccessfulSpaceClaim)
{
    const char *payload = "Aeron is awesome!";
    const size_t length = strlen(payload);
    const int32_t frame_length = AERON_ALIGN(length + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int32_t term_count = INT32_MAX;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const size_t next_partition_index = (partition_index + 1) % AERON_LOGBUFFER_PARTITION_COUNT;
    const int32_t term_offset = (int32_t)(TERM_LENGTH - AERON_DATA_HEADER_LENGTH - 8);
    const int32_t term_id = INT32_MIN + (INITIAL_TERM_ID - 1);
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);
    m_publication->log_meta_data->term_tail_counters[next_partition_index] =
        packTail(term_id + 1 - AERON_LOGBUFFER_PARTITION_COUNT, 2846386);

    const int64_t position = aeron_publication_offer(
        m_publication,
        reinterpret_cast<const uint8_t *>(payload),
        length,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_MAX_POSITION_EXCEEDED, position);
    EXPECT_EQ(
        packTail(term_id, term_offset + frame_length),
        m_publication->log_meta_data->term_tail_counters[partition_index]);
    EXPECT_EQ(
        packTail(term_id + 1 - AERON_LOGBUFFER_PARTITION_COUNT, 2846386),
        m_publication->log_meta_data->term_tail_counters[next_partition_index]);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(AERON_DATA_HEADER_LENGTH + 8, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, vectorOfferErrorIfPublicationIsNull)
{
    aeron_iovec_t iov[1];

    const int64_t position = aeron_publication_offerv(
        nullptr,
        iov,
        1,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_ERROR, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[0];
    auto *header = (aeron_data_header_t *)(term_buffer->addr);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, vectorOfferErrorIfBufferIsNull)
{
    const int64_t position = aeron_publication_offerv(
        m_publication,
        nullptr,
        10,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_ERROR, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[0];
    auto *header = (aeron_data_header_t *)(term_buffer->addr);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, vectorOfferClosed)
{
    const char *payload = "Aeron is awesome squared!";
    aeron_iovec_t iov[1];
    iov[0].iov_base = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(payload));
    iov[0].iov_len = strlen(payload);
    const int32_t term_count = 16;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = 4096;
    m_publication->is_closed = true;

    const int64_t position = aeron_publication_offerv(
        m_publication,
        iov,
        1,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_CLOSED, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, vectorOfferAdminActionIfTermCountDoesNotMatch)
{
    const char *payload = "Aeron is awesome squared!";
    aeron_iovec_t iov[1];
    iov[0].iov_base = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(payload));
    iov[0].iov_len = strlen(payload);
    const int32_t term_offset = 4096;
    m_publication->log_meta_data->active_term_count = 5;

    const int64_t position = aeron_publication_offerv(
        m_publication,
        iov,
        1,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_ADMIN_ACTION, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[0];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, vectorOfferBackPressureIfPublicationLimitReached)
{
    const char *payload = "Aeron is awesome squared!";
    aeron_iovec_t iov[1];
    iov[0].iov_base = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(payload));
    iov[0].iov_len = strlen(payload);
    const int32_t term_count = 16;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = 4096;
    const int32_t term_id = m_publication->initial_term_id + term_count;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);
    const int64_t limit_position = aeron_logbuffer_compute_position(
        term_id, term_offset, m_publication->position_bits_to_shift, m_publication->initial_term_id);
    aeron_counter_set_release(m_position_limit_addr, limit_position);

    const int64_t position = aeron_publication_offerv(
        m_publication,
        iov,
        1,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_BACK_PRESSURED, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, vectorOfferNotConnectedIfPublicationLimitReached)
{
    const char *payload = "Aeron is awesome squared!";
    aeron_iovec_t iov[1];
    iov[0].iov_base = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(payload));
    iov[0].iov_len = strlen(payload);
    const int32_t term_count = 3;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = 1024;
    const int32_t term_id = m_publication->initial_term_id + term_count;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);
    m_publication->log_meta_data->is_connected = false;
    const int64_t limit_position = aeron_logbuffer_compute_position(
        term_id, term_offset - 32, m_publication->position_bits_to_shift, m_publication->initial_term_id);
    aeron_counter_set_release(m_position_limit_addr, limit_position);

    const int64_t position = aeron_publication_offerv(
        m_publication,
        iov,
        1,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_NOT_CONNECTED, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, vectorOfferMaxPositionExceededIfPublicationLimitReached)
{
    const char *payload = "Test, test, test.";
    aeron_iovec_t iov[1];
    iov[0].iov_base = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(payload));
    iov[0].iov_len = strlen(payload);
    const int32_t term_count = INT32_MAX;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = (int32_t)(TERM_LENGTH - 8);
    const int32_t term_id = INT32_MIN + (INITIAL_TERM_ID - 1);
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);
    aeron_counter_set_release(m_position_limit_addr, 64);

    const int64_t position = aeron_publication_offerv(
        m_publication,
        iov,
        1,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_MAX_POSITION_EXCEEDED, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, vectorOfferPublicationErrorIfMessageIsLargerThanMaxMessageSize)
{
    const char *payload = "Aeron is awesome vector!";
    aeron_iovec_t iov[1];
    iov[0].iov_base = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(payload));
    iov[0].iov_len = MAX_MESSAGE_SIZE + 1;
    const int32_t term_count = 5;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = (int32_t)TERM_LENGTH;
    const int32_t term_id = term_count + INITIAL_TERM_ID;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);

    const int64_t position = aeron_publication_offerv(
        m_publication,
        iov,
        1,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_ERROR, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, vectorOfferAdminActionAfterRolloingOverToTheNextTerm)
{
    const char *payload = "This does not fit into the end of the buffer...";
    const size_t length = strlen(payload);
    aeron_iovec_t iov[1];
    iov[0].iov_base = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(payload));
    iov[0].iov_len = length;
    const int32_t frame_length = AERON_ALIGN(length + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int32_t term_count = 51;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const size_t next_partition_index = aeron_logbuffer_index_by_term_count(term_count + 1);
    const int32_t term_offset = (int32_t)(TERM_LENGTH - AERON_DATA_HEADER_LENGTH - 8);
    const int32_t term_id = term_count + INITIAL_TERM_ID;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);
    m_publication->log_meta_data->term_tail_counters[next_partition_index] =
        packTail(term_id + 1 - AERON_LOGBUFFER_PARTITION_COUNT, 999888);

    const int64_t position = aeron_publication_offerv(
        m_publication,
        iov,
        1,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_ADMIN_ACTION, position);
    EXPECT_EQ(
        packTail(term_id, term_offset + frame_length),
        m_publication->log_meta_data->term_tail_counters[partition_index]);
    EXPECT_EQ(
        packTail(term_id + 1, 0),
        m_publication->log_meta_data->term_tail_counters[next_partition_index]);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(AERON_DATA_HEADER_LENGTH + 8, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, vectorOfferMaxPositionExceededAfterSuccessfulSpaceClaim)
{
    const char *payload = "Aeron is awesome squared!";
    const size_t length = strlen(payload);
    aeron_iovec_t iov[1];
    iov[0].iov_base = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(payload));
    iov[0].iov_len = length;
    const int32_t frame_length = AERON_ALIGN(length + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int32_t term_count = INT32_MAX;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const size_t next_partition_index = (partition_index + 1) % AERON_LOGBUFFER_PARTITION_COUNT;
    const int32_t term_offset = (int32_t)(TERM_LENGTH - AERON_DATA_HEADER_LENGTH - 8);
    const int32_t term_id = INT32_MIN + (INITIAL_TERM_ID - 1);
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);
    m_publication->log_meta_data->term_tail_counters[next_partition_index] =
        packTail(term_id + 1 - AERON_LOGBUFFER_PARTITION_COUNT, 2846386);

    const int64_t position = aeron_publication_offerv(
        m_publication,
        iov,
        1,
        reserved_value_supplier,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_MAX_POSITION_EXCEEDED, position);
    EXPECT_EQ(
        packTail(term_id, term_offset + frame_length),
        m_publication->log_meta_data->term_tail_counters[partition_index]);
    EXPECT_EQ(
        packTail(term_id + 1 - AERON_LOGBUFFER_PARTITION_COUNT, 2846386),
        m_publication->log_meta_data->term_tail_counters[next_partition_index]);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(AERON_DATA_HEADER_LENGTH + 8, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, tryClaimErrorIfPublicationIsNull)
{
    aeron_buffer_claim_t buffer_claim = {};

    const int64_t position = aeron_publication_try_claim(
        nullptr,
        1,
        &buffer_claim);

    ASSERT_EQ(AERON_PUBLICATION_ERROR, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[0];
    auto *header = (aeron_data_header_t *)(term_buffer->addr);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, tryClaimErrorIfBufferIsNull)
{
    const int64_t position = aeron_publication_try_claim(
        m_publication,
        1,
        nullptr);

    ASSERT_EQ(AERON_PUBLICATION_ERROR, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[0];
    auto *header = (aeron_data_header_t *)(term_buffer->addr);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, tryClaimClosed)
{
    aeron_buffer_claim_t buffer_claim = {};
    const size_t length = 5;
    const int32_t term_count = 16;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = 4096;
    m_publication->is_closed = true;

    const int64_t position = aeron_publication_try_claim(
        m_publication,
        length,
        &buffer_claim);

    ASSERT_EQ(AERON_PUBLICATION_CLOSED, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, tryClaimAdminActionIfTermCountDoesNotMatch)
{
    aeron_buffer_claim_t buffer_claim = {};
    const size_t length = 13;
    const int32_t term_offset = 4096;
    m_publication->log_meta_data->active_term_count = 5;

    const int64_t position = aeron_publication_try_claim(
        m_publication,
        length,
        &buffer_claim);

    ASSERT_EQ(AERON_PUBLICATION_ADMIN_ACTION, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[0];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, tryClaimBackPressureIfPublicationLimitReached)
{
    aeron_buffer_claim_t buffer_claim = {};
    const size_t length = 5;
    const int32_t term_count = 16;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = 4096;
    const int32_t term_id = m_publication->initial_term_id + term_count;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);
    const int64_t limit_position = aeron_logbuffer_compute_position(
        term_id, term_offset, m_publication->position_bits_to_shift, m_publication->initial_term_id);
    aeron_counter_set_release(m_position_limit_addr, limit_position);

    const int64_t position = aeron_publication_try_claim(
        m_publication,
        length,
        &buffer_claim);

    ASSERT_EQ(AERON_PUBLICATION_BACK_PRESSURED, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, tryClaimNotConnectedIfPublicationLimitReached)
{
    aeron_buffer_claim_t buffer_claim = {};
    const size_t length = 5;
    const int32_t term_count = 3;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = 1024;
    const int32_t term_id = m_publication->initial_term_id + term_count;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);
    m_publication->log_meta_data->is_connected = false;
    const int64_t limit_position = aeron_logbuffer_compute_position(
        term_id, term_offset - 32, m_publication->position_bits_to_shift, m_publication->initial_term_id);
    aeron_counter_set_release(m_position_limit_addr, limit_position);

    const int64_t position = aeron_publication_try_claim(
        m_publication,
        length,
        &buffer_claim);

    ASSERT_EQ(AERON_PUBLICATION_NOT_CONNECTED, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, tryClaimMaxPositionExceededIfPublicationLimitReached)
{
    aeron_buffer_claim_t buffer_claim = {};
    const size_t length = 5;
    const int32_t term_count = INT32_MAX;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = (int32_t)(TERM_LENGTH - 8);
    const int32_t term_id = INT32_MIN + (INITIAL_TERM_ID - 1);
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);
    aeron_counter_set_release(m_position_limit_addr, 64);

    const int64_t position = aeron_publication_try_claim(
        m_publication,
        length,
        &buffer_claim);

    ASSERT_EQ(AERON_PUBLICATION_MAX_POSITION_EXCEEDED, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, tryClaimPublicationErrorIfMessageIsLargerThanMaxPayloadSize)
{
    aeron_buffer_claim_t buffer_claim = {};
    const int32_t term_count = 5;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const int32_t term_offset = (int32_t)TERM_LENGTH;
    const int32_t term_id = term_count + (int32_t)INITIAL_TERM_ID;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);

    const int64_t position = aeron_publication_try_claim(
        m_publication,
        MAX_PAYLOAD_SIZE + 1,
        &buffer_claim);

    ASSERT_EQ(AERON_PUBLICATION_ERROR, position);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(0, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, tryClaimAdminActionAfterRolloingOverToTheNextTerm)
{
    aeron_buffer_claim_t buffer_claim = {};
    const size_t length = 55;
    const int32_t frame_length = AERON_ALIGN(length + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int32_t term_count = 51;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const size_t next_partition_index = aeron_logbuffer_index_by_term_count(term_count + 1);
    const int32_t term_offset = (int32_t)(TERM_LENGTH - AERON_DATA_HEADER_LENGTH - 8);
    const int32_t term_id = term_count + (int32_t)INITIAL_TERM_ID;
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);
    m_publication->log_meta_data->term_tail_counters[next_partition_index] =
        packTail(term_id + 1 - AERON_LOGBUFFER_PARTITION_COUNT, 999888);

    const int64_t position = aeron_publication_try_claim(
        m_publication,
        length,
        &buffer_claim);

    ASSERT_EQ(AERON_PUBLICATION_ADMIN_ACTION, position);
    EXPECT_EQ(
        packTail(term_id, term_offset + frame_length),
        m_publication->log_meta_data->term_tail_counters[partition_index]);
    EXPECT_EQ(
        packTail(term_id + 1, 0),
        m_publication->log_meta_data->term_tail_counters[next_partition_index]);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(AERON_DATA_HEADER_LENGTH + 8, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}

TEST_F(PublicationTest, tryClaimMaxPositionExceededAfterSuccessfulSpaceClaim)
{
    aeron_buffer_claim_t buffer_claim = {};
    const size_t length = 19;
    const int32_t frame_length = AERON_ALIGN(length + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int32_t term_count = INT32_MAX;
    const size_t partition_index = aeron_logbuffer_index_by_term_count(term_count);
    const size_t next_partition_index = (partition_index + 1) % AERON_LOGBUFFER_PARTITION_COUNT;
    const int32_t term_offset = (int32_t)(TERM_LENGTH - AERON_DATA_HEADER_LENGTH - 8);
    const int32_t term_id = INT32_MIN + (INITIAL_TERM_ID - 1);
    m_publication->log_meta_data->active_term_count = term_count;
    m_publication->log_meta_data->term_tail_counters[partition_index] = packTail(term_id, term_offset);
    m_publication->log_meta_data->term_tail_counters[next_partition_index] =
        packTail(term_id + 1 - AERON_LOGBUFFER_PARTITION_COUNT, 2846386);

    const int64_t position = aeron_publication_try_claim(
        m_publication,
        length,
        &buffer_claim);

    ASSERT_EQ(AERON_PUBLICATION_MAX_POSITION_EXCEEDED, position);
    EXPECT_EQ(
        packTail(term_id, term_offset + frame_length),
        m_publication->log_meta_data->term_tail_counters[partition_index]);
    EXPECT_EQ(
        packTail(term_id + 1 - AERON_LOGBUFFER_PARTITION_COUNT, 2846386),
        m_publication->log_meta_data->term_tail_counters[next_partition_index]);
    const aeron_mapped_buffer_t *term_buffer = &m_publication->log_buffer->mapped_raw_log.term_buffers[partition_index];
    auto *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
    EXPECT_EQ(AERON_DATA_HEADER_LENGTH + 8, header->frame_header.frame_length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, header->frame_header.type);
}
