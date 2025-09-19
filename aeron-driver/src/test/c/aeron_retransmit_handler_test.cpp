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
#include "aeron_retransmit_handler.h"
#include "aeron_flow_control.h"
#include "aeron_counter.h"
#include "concurrent/aeron_logbuffer_descriptor.h"
}

#define TERM_LENGTH (AERON_LOGBUFFER_TERM_MIN_LENGTH)
#define HEADER_LENGTH (AERON_DATA_HEADER_LENGTH)

#define TERM_ID (0x1234)

#define DATA_LENGTH (36)
#define MESSAGE_LENGTH (DATA_LENGTH + HEADER_LENGTH)
#define ALIGNED_FRAME_LENGTH (AERON_ALIGN(MESSAGE_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT))

#define DELAY_TIMEOUT_20MS (20 * 1000 * UINT64_C(1000))
#define LINGER_TIMEOUT_20MS (20 * 1000 * UINT64_C(1000))

#define MTU_LENGTH (1234) // this value is ignored

class RetransmitHandlerTest : public testing::Test
{
public:
    RetransmitHandlerTest()
    {
        m_flow_control.state = nullptr;
        m_flow_control.max_retransmission_length = max_retransmission_length;
    }

    ~RetransmitHandlerTest() override
    {
        aeron_retransmit_handler_close(&m_handler);
    }

    static int on_resend(void *clientd, int32_t term_id, int32_t term_offset, size_t length)
    {
        auto *t = (RetransmitHandlerTest *)clientd;

        return t->m_resend(term_id, term_offset, length);
    }

    static size_t max_retransmission_length(
        void *state,
        size_t term_offset,
        size_t resend_length,
        size_t term_buffer_length,
        size_t mtu_length)
    {
        return aeron_flow_control_calculate_retransmission_length(
            resend_length,
            term_buffer_length,
            term_offset,
            AERON_MULTICAST_FLOW_CONTROL_RETRANSMIT_RECEIVER_WINDOW_MULTIPLE);
    }

protected:
    int64_t m_time = 0;
    int64_t m_invalid_packet_counter = 0;
    int64_t m_retransmit_overflow_counter = 0;
    aeron_retransmit_handler_t m_handler = {};
    aeron_flow_control_strategy_t m_flow_control = {};
    std::function<int(int32_t, int32_t, size_t)> m_resend;
};

TEST_F(RetransmitHandlerTest, shouldImmediateRetransmitOnNak)
{
    ASSERT_EQ(aeron_retransmit_handler_init(
        &m_handler, &m_invalid_packet_counter, 0, LINGER_TIMEOUT_20MS, true, 16, &m_retransmit_overflow_counter), 0);

    const int32_t nak_offset = (ALIGNED_FRAME_LENGTH * 2);
    const size_t nak_length = ALIGNED_FRAME_LENGTH;

    size_t called = 0;
    m_resend =
        [&](int32_t term_id, int32_t term_offset, size_t length)
        {
            EXPECT_EQ(term_id, TERM_ID);
            EXPECT_EQ(term_offset, nak_offset);
            EXPECT_EQ(length, nak_length);
            called++;
            return 0;
        };

    EXPECT_EQ(aeron_retransmit_handler_on_nak(
        &m_handler, TERM_ID, nak_offset, nak_length, TERM_LENGTH, MTU_LENGTH, &m_flow_control, m_time, RetransmitHandlerTest::on_resend, this), 0);
    EXPECT_EQ(called, 1u);
}

TEST_F(RetransmitHandlerTest, shouldNotRetransmitOnNakWhileInLinger)
{
    ASSERT_EQ(aeron_retransmit_handler_init(
        &m_handler, &m_invalid_packet_counter, 0, LINGER_TIMEOUT_20MS, true, 16, &m_retransmit_overflow_counter), 0);

    const int32_t nak_offset = (ALIGNED_FRAME_LENGTH * 2);
    const size_t nak_length = ALIGNED_FRAME_LENGTH;

    size_t called = 0;
    m_resend =
        [&](int32_t term_id, int32_t term_offset, size_t length)
        {
            EXPECT_EQ(term_id, TERM_ID);
            EXPECT_EQ(term_offset, nak_offset);
            EXPECT_EQ(length, nak_length);
            called++;
            return 0;
        };

    EXPECT_EQ(aeron_retransmit_handler_on_nak(
        &m_handler, TERM_ID, nak_offset, nak_length, TERM_LENGTH, MTU_LENGTH, &m_flow_control, m_time, RetransmitHandlerTest::on_resend, this), 0);
    EXPECT_EQ(called, 1u);

    m_time = 10 * 1000 * 1000L;
    EXPECT_EQ(aeron_retransmit_handler_process_timeouts(&m_handler, m_time, RetransmitHandlerTest::on_resend, this), 0);
    EXPECT_EQ(aeron_retransmit_handler_on_nak(
        &m_handler, TERM_ID, nak_offset, nak_length, TERM_LENGTH, MTU_LENGTH, &m_flow_control, m_time, RetransmitHandlerTest::on_resend, this), 0);
    EXPECT_EQ(called, 1u);
}

TEST_F(RetransmitHandlerTest, shouldRetransmitOnNakAfterLinger)
{
    ASSERT_EQ(aeron_retransmit_handler_init(
        &m_handler, &m_invalid_packet_counter, 0, LINGER_TIMEOUT_20MS, true, 16, &m_retransmit_overflow_counter), 0);

    const int32_t nak_offset = (ALIGNED_FRAME_LENGTH * 2);
    const size_t nak_length = ALIGNED_FRAME_LENGTH;

    size_t called = 0;
    m_resend =
        [&](int32_t term_id, int32_t term_offset, size_t length)
        {
            EXPECT_EQ(term_id, TERM_ID);
            EXPECT_EQ(term_offset, nak_offset);
            EXPECT_EQ(length, nak_length);
            called++;
            return 0;
        };

    EXPECT_EQ(aeron_retransmit_handler_on_nak(
        &m_handler, TERM_ID, nak_offset, nak_length, TERM_LENGTH, MTU_LENGTH, &m_flow_control, m_time, RetransmitHandlerTest::on_resend, this), 0);
    EXPECT_EQ(called, 1u);

    m_time = 30 * 1000 * 1000L;
    EXPECT_EQ(aeron_retransmit_handler_process_timeouts(&m_handler, m_time, RetransmitHandlerTest::on_resend, this), 1);
    EXPECT_EQ(aeron_retransmit_handler_on_nak(
        &m_handler, TERM_ID, nak_offset, nak_length, TERM_LENGTH, MTU_LENGTH, &m_flow_control, m_time, RetransmitHandlerTest::on_resend, this), 0);
    EXPECT_EQ(called, 2u);
}

TEST_F(RetransmitHandlerTest, shouldRetransmitOnMultipleNaks)
{
    ASSERT_EQ(aeron_retransmit_handler_init(
        &m_handler, &m_invalid_packet_counter, 0, LINGER_TIMEOUT_20MS, true, 16, &m_retransmit_overflow_counter), 0);

    const int32_t nak_offset_1 = (ALIGNED_FRAME_LENGTH * 2);
    const size_t nak_length_1 = ALIGNED_FRAME_LENGTH;
    const int32_t nak_offset_2 = (ALIGNED_FRAME_LENGTH * 5);
    const size_t nak_length_2 = ALIGNED_FRAME_LENGTH * 2;

    size_t called = 0;
    m_resend =
        [&](int32_t term_id, int32_t term_offset, size_t length)
        {
            called++;

            EXPECT_EQ(term_id, TERM_ID);
            if (1 == called)
            {
                EXPECT_EQ(term_offset, nak_offset_1);
                EXPECT_EQ(length, nak_length_1);
            }
            else if (2 == called)
            {
                EXPECT_EQ(term_offset, nak_offset_2);
                EXPECT_EQ(length, nak_length_2);
            }
            return 0;
        };

    EXPECT_EQ(aeron_retransmit_handler_on_nak(
        &m_handler, TERM_ID, nak_offset_1, nak_length_1, TERM_LENGTH, MTU_LENGTH, &m_flow_control, m_time, RetransmitHandlerTest::on_resend, this), 0);
    EXPECT_EQ(called, 1u);
    EXPECT_EQ(aeron_retransmit_handler_on_nak(
        &m_handler, TERM_ID, nak_offset_2, nak_length_2, TERM_LENGTH, MTU_LENGTH, &m_flow_control, m_time, RetransmitHandlerTest::on_resend, this), 0);
    EXPECT_EQ(called, 2u);
}

TEST_F(RetransmitHandlerTest, errorOnRetransmitOverflow)
{
    ASSERT_EQ(aeron_retransmit_handler_init(
        &m_handler,
        &m_invalid_packet_counter,
        DELAY_TIMEOUT_20MS,
        LINGER_TIMEOUT_20MS,
        true,
        16,
        &m_retransmit_overflow_counter), 0);

    int64_t initial_overflow_value = aeron_counter_get_plain(&m_retransmit_overflow_counter);

    EXPECT_EQ(m_handler.active_retransmit_count, 0);

    int32_t i = 0;
    for (i = 0; i < AERON_RETRANSMIT_HANDLER_MAX_RESEND; i++)
    {
        EXPECT_EQ(aeron_retransmit_handler_on_nak(
            &m_handler, TERM_ID, i, 1, TERM_LENGTH, MTU_LENGTH, &m_flow_control, m_time, RetransmitHandlerTest::on_resend, this), 0);
    }

    EXPECT_EQ(m_handler.active_retransmit_count, AERON_RETRANSMIT_HANDLER_MAX_RESEND);

    // there should be no more available retransmit actions
    EXPECT_EQ(aeron_retransmit_handler_on_nak(
        &m_handler, TERM_ID, i, 1, TERM_LENGTH, MTU_LENGTH, &m_flow_control, m_time, RetransmitHandlerTest::on_resend, this), 0);

    EXPECT_NE(initial_overflow_value, aeron_counter_get_plain(&m_retransmit_overflow_counter));

    // these will all be duplicates of previous NAKs
    for (i = 0; i < AERON_RETRANSMIT_HANDLER_MAX_RESEND; i++)
    {
        EXPECT_EQ(aeron_retransmit_handler_on_nak(
            &m_handler, TERM_ID, i, 1, TERM_LENGTH, MTU_LENGTH, &m_flow_control, m_time, RetransmitHandlerTest::on_resend, this), 0);
    }

    EXPECT_EQ(m_handler.active_retransmit_count, AERON_RETRANSMIT_HANDLER_MAX_RESEND);
}

TEST_F(RetransmitHandlerTest, shouldSkipInvalidNaks)
{
    ASSERT_EQ(aeron_retransmit_handler_init(
        &m_handler,
        &m_invalid_packet_counter,
        DELAY_TIMEOUT_20MS,
        LINGER_TIMEOUT_20MS,
        true,
        16,
        &m_retransmit_overflow_counter), 0);

    EXPECT_EQ(m_handler.active_retransmit_count, 0);

    // invalid `term-offset`
    EXPECT_EQ(aeron_retransmit_handler_on_nak(
            &m_handler, TERM_ID, TERM_LENGTH - 5, 1, TERM_LENGTH, MTU_LENGTH, &m_flow_control, m_time, RetransmitHandlerTest::on_resend, this), 0);
    EXPECT_EQ(1, aeron_counter_get_plain(&m_invalid_packet_counter));

    // negative `term-offset`
    EXPECT_EQ(aeron_retransmit_handler_on_nak(
            &m_handler, TERM_ID, -1024, 1, TERM_LENGTH, MTU_LENGTH, &m_flow_control, m_time, RetransmitHandlerTest::on_resend, this), 0);
    EXPECT_EQ(2, aeron_counter_get_plain(&m_invalid_packet_counter));

    // negative `length`
    EXPECT_EQ(aeron_retransmit_handler_on_nak(
            &m_handler, TERM_ID, 0, -32, TERM_LENGTH, MTU_LENGTH, &m_flow_control, m_time, RetransmitHandlerTest::on_resend, this), 0);
    EXPECT_EQ(3, aeron_counter_get_plain(&m_invalid_packet_counter));

    EXPECT_EQ(m_handler.active_retransmit_count, 0);
}

TEST_F(RetransmitHandlerTest, shouldIgnoreEmptyNak)
{
    ASSERT_EQ(aeron_retransmit_handler_init(
        &m_handler,
        &m_invalid_packet_counter,
        DELAY_TIMEOUT_20MS,
        LINGER_TIMEOUT_20MS,
        true,
        16,
        &m_retransmit_overflow_counter), 0);

    EXPECT_EQ(m_handler.active_retransmit_count, 0);

    // negative `length`
    EXPECT_EQ(aeron_retransmit_handler_on_nak(
            &m_handler, TERM_ID, 0, 0, TERM_LENGTH, MTU_LENGTH, &m_flow_control, m_time, RetransmitHandlerTest::on_resend, this), 0);
    EXPECT_EQ(m_handler.active_retransmit_count, 0);
    EXPECT_EQ(0, aeron_counter_get_plain(&m_invalid_packet_counter));
}
