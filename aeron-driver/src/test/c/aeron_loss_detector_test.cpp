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
#include "aeron_loss_detector.h"
}

#define CAPACITY (AERON_LOGBUFFER_TERM_MIN_LENGTH)
#define POSITION_BITS_TO_SHIFT (aeron_number_of_trailing_zeroes(CAPACITY))
#define MASK (CAPACITY - 1)

#define TERM_ID (0x1234)
#define SESSION_ID (0xDEADBEEF)
#define STREAM_ID (0x12A)

typedef std::array<std::uint8_t, CAPACITY> buffer_t;

class TermGapScannerTest : public testing::Test
{
public:
    TermGapScannerTest() : m_ptr(m_buffer.data())
    {
        m_buffer.fill(0);
    }

    static void on_gap_detected(void *clientd, int32_t term_id, int32_t term_offset, size_t length)
    {
        auto *t = (TermGapScannerTest *)clientd;

        t->m_on_gap_detected(term_id, term_offset, length);
    }

protected:
    buffer_t m_buffer = {};
    uint8_t *m_ptr = nullptr;
    std::function<void(int32_t, int32_t, size_t)> m_on_gap_detected;
};

TEST_F(TermGapScannerTest, shouldReportGapAtBeginningOfBuffer)
{
    const int32_t frame_offset = AERON_ALIGN((AERON_DATA_HEADER_LENGTH * 3), AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int32_t high_water_mark = frame_offset + AERON_DATA_HEADER_LENGTH;
    auto *hdr = (aeron_frame_header_t *)(m_ptr + frame_offset);

    hdr->frame_length = AERON_DATA_HEADER_LENGTH;

    int32_t gap_length;

    ASSERT_EQ(aeron_term_gap_scanner_scan_for_gap(m_ptr, 0, high_water_mark, &gap_length), 0);
    EXPECT_EQ(gap_length, (size_t)frame_offset);
}

TEST_F(TermGapScannerTest, shouldReportSingleGapWhenBufferNotFull)
{
    const int32_t tail = AERON_DATA_HEADER_LENGTH;
    const int32_t high_water_mark = AERON_LOGBUFFER_FRAME_ALIGNMENT * 3;

    aeron_frame_header_t *hdr;

    hdr = (aeron_frame_header_t *)(m_ptr + tail - (AERON_ALIGN(AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT)));
    hdr->frame_length = AERON_DATA_HEADER_LENGTH;

    hdr = (aeron_frame_header_t *)(m_ptr + tail);
    hdr->frame_length = 0;

    hdr = (aeron_frame_header_t *)(m_ptr + high_water_mark - (AERON_ALIGN(AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT)));
    hdr->frame_length = AERON_DATA_HEADER_LENGTH;

    int32_t gap_length;

    ASSERT_EQ(aeron_term_gap_scanner_scan_for_gap(m_ptr, tail, high_water_mark, &gap_length), tail);
    EXPECT_EQ(gap_length, (size_t)AERON_ALIGN(AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT));
}

TEST_F(TermGapScannerTest, shouldReportSingleGapWhenBufferIsFull)
{
    const int32_t tail = CAPACITY - (AERON_DATA_HEADER_LENGTH * 2);
    const int32_t high_water_mark = CAPACITY;

    aeron_frame_header_t *hdr;

    hdr = (aeron_frame_header_t *)(m_ptr + tail - (AERON_ALIGN(AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT)));
    hdr->frame_length = AERON_DATA_HEADER_LENGTH;

    hdr = (aeron_frame_header_t *)(m_ptr + tail);
    hdr->frame_length = 0;

    hdr = (aeron_frame_header_t *)(m_ptr + high_water_mark - (AERON_ALIGN(AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT)));
    hdr->frame_length = AERON_DATA_HEADER_LENGTH;

    int32_t gap_length;

    ASSERT_EQ(aeron_term_gap_scanner_scan_for_gap(m_ptr, tail, high_water_mark, &gap_length), tail);
    EXPECT_EQ(gap_length, (size_t)AERON_ALIGN(AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT));
}

TEST_F(TermGapScannerTest, shouldReportNoGapWhenHwmIsInPadding)
{
    const int32_t padding_length = AERON_DATA_HEADER_LENGTH * 2;
    const int32_t tail = CAPACITY - padding_length;
    const int32_t high_water_mark = CAPACITY - padding_length + AERON_DATA_HEADER_LENGTH;

    aeron_frame_header_t *hdr;

    hdr = (aeron_frame_header_t *)(m_ptr + tail);
    hdr->frame_length = padding_length;

    hdr = (aeron_frame_header_t *)(m_ptr + tail + AERON_DATA_HEADER_LENGTH);
    hdr->frame_length = 0;

    int32_t gap_length;

    ASSERT_EQ(aeron_term_gap_scanner_scan_for_gap(m_ptr, tail, high_water_mark, &gap_length), CAPACITY);
}

#define DATA_LENGTH (36)
#define MESSAGE_LENGTH (DATA_LENGTH + AERON_DATA_HEADER_LENGTH)
#define ALIGNED_FRAME_LENGTH (AERON_ALIGN(MESSAGE_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT))

class LossDetectorTest : public testing::Test
{
public:
    LossDetectorTest() : m_ptr(m_buffer.data()),
                         m_next_ptr(m_next_buffer.data()),
                         m_time(0)
    {
        m_buffer.fill(0);
    }

    static void on_gap_detected(void *clientd, int32_t term_id, int32_t term_offset, size_t length)
    {
        auto *t = (LossDetectorTest *)clientd;

        t->m_on_gap_detected(term_id, term_offset, length);
    }

    static int32_t offset_of_message(int index)
    {
        return index * ALIGNED_FRAME_LENGTH;
    }

    void insert_frame(int32_t offset)
    {
        auto *hdr = (aeron_data_header_t *)(m_ptr + offset);

        hdr->frame_header.frame_length = MESSAGE_LENGTH;
    }

    int feedback_delay_state_init(bool should_immediate_feedback)
    {
        return aeron_feedback_delay_state_init(
            &m_delay_generator_state,
            aeron_loss_detector_nak_unicast_delay_generator,
            should_immediate_feedback ? 0 : 20 * 1000 * 1000LL,
            20 * 1000 * 1000LL,
            1);
    }

protected:
    buffer_t m_buffer = {};
    buffer_t m_next_buffer = {};
    uint8_t *m_ptr = nullptr;
    uint8_t *m_next_ptr = nullptr;
    int64_t m_time = 0;
    aeron_loss_detector_t m_detector = {};
    aeron_feedback_delay_generator_state_t m_delay_generator_state = {};
    std::function<void(int32_t, int32_t, size_t)> m_on_gap_detected;
};

TEST_F(LossDetectorTest, shouldNotSendNakWhenBufferIsEmpty)
{
    const int64_t rebuild_position = 0;
    const int64_t hwm_position = 0;
    bool loss_found;

    ASSERT_EQ(feedback_delay_state_init(true), 0);
    ASSERT_EQ(aeron_loss_detector_init(
                  &m_detector, &m_delay_generator_state, LossDetectorTest::on_gap_detected, this),
              0);

    m_on_gap_detected =
        [](int32_t term_id, int32_t term_offset, size_t length)
    {
        FAIL();
    };

    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              hwm_position);
    EXPECT_FALSE(loss_found);

    m_time = 100 * 1000 * 1000L;
    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              hwm_position);
    EXPECT_FALSE(loss_found);
}

TEST_F(LossDetectorTest, shouldNotNakIfNoMissingData)
{
    const int64_t rebuild_position = 0;
    const int64_t hwm_position = rebuild_position + (ALIGNED_FRAME_LENGTH * 3);
    bool loss_found;

    insert_frame(offset_of_message(0));
    insert_frame(offset_of_message(1));
    insert_frame(offset_of_message(2));

    ASSERT_EQ(feedback_delay_state_init(true), 0);
    ASSERT_EQ(aeron_loss_detector_init(
                  &m_detector, &m_delay_generator_state, LossDetectorTest::on_gap_detected, this),
              0);

    m_on_gap_detected =
        [](int32_t term_id, int32_t term_offset, size_t length)
    {
        FAIL();
    };

    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              hwm_position);
    EXPECT_FALSE(loss_found);

    m_time = 40 * 1000 * 1000L;
    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              hwm_position);
    EXPECT_FALSE(loss_found);
}

TEST_F(LossDetectorTest, shouldNakMissingData)
{
    const int64_t rebuild_position = 0;
    const int64_t hwm_position = rebuild_position + (ALIGNED_FRAME_LENGTH * 3);
    bool loss_found;
    int called = 0;

    insert_frame(offset_of_message(0));
    insert_frame(offset_of_message(2));

    ASSERT_EQ(feedback_delay_state_init(false), 0);
    ASSERT_EQ(aeron_loss_detector_init(
                  &m_detector, &m_delay_generator_state, LossDetectorTest::on_gap_detected, this),
              0);

    m_on_gap_detected =
        [&](int32_t term_id, int32_t term_offset, size_t length)
    {
        EXPECT_EQ(term_id, TERM_ID);
        EXPECT_EQ(term_offset, offset_of_message(1));
        EXPECT_EQ(length, ALIGNED_FRAME_LENGTH);
        called++;
    };

    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(1));
    EXPECT_EQ(called, 0);
    EXPECT_TRUE(loss_found);

    m_time = 40 * 1000 * 1000L;
    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(1));
    EXPECT_EQ(called, 1);
    EXPECT_FALSE(loss_found);
}

TEST_F(LossDetectorTest, shouldRetransmitNakForMissingData)
{
    const int64_t rebuild_position = 0;
    const int64_t hwm_position = rebuild_position + (ALIGNED_FRAME_LENGTH * 3);
    bool loss_found;
    int called = 0;

    insert_frame(offset_of_message(0));
    insert_frame(offset_of_message(2));

    ASSERT_EQ(feedback_delay_state_init(false), 0);
    ASSERT_EQ(aeron_loss_detector_init(
                  &m_detector, &m_delay_generator_state, LossDetectorTest::on_gap_detected, this),
              0);

    m_on_gap_detected =
        [&](int32_t term_id, int32_t term_offset, size_t length)
    {
        EXPECT_EQ(term_id, TERM_ID);
        EXPECT_EQ(term_offset, offset_of_message(1));
        EXPECT_EQ(length, ALIGNED_FRAME_LENGTH);
        called++;
    };

    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(1));
    EXPECT_EQ(called, 0);
    EXPECT_TRUE(loss_found);

    m_time = 30 * 1000 * 1000L;
    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(1));
    EXPECT_EQ(called, 1);
    EXPECT_FALSE(loss_found);

    m_time = 60 * 1000 * 1000L;
    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(1));
    EXPECT_EQ(called, 2);
    EXPECT_FALSE(loss_found);
}

TEST_F(LossDetectorTest, shouldStopNakOnReceivingData)
{
    int64_t rebuild_position = 0;
    const int64_t hwm_position = rebuild_position + (ALIGNED_FRAME_LENGTH * 3);
    bool loss_found;
    int called = 0;

    insert_frame(offset_of_message(0));
    insert_frame(offset_of_message(2));

    ASSERT_EQ(feedback_delay_state_init(false), 0);
    ASSERT_EQ(aeron_loss_detector_init(
                  &m_detector, &m_delay_generator_state, LossDetectorTest::on_gap_detected, this),
              0);

    m_on_gap_detected =
        [&](int32_t term_id, int32_t term_offset, size_t length)
    {
        EXPECT_EQ(term_id, TERM_ID);
        EXPECT_EQ(term_offset, offset_of_message(1));
        EXPECT_EQ(length, ALIGNED_FRAME_LENGTH);
        called++;
    };

    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(1));
    EXPECT_EQ(called, 0);
    EXPECT_TRUE(loss_found);

    m_time = 20 * 1000 * 1000L;
    insert_frame(offset_of_message(1));
    rebuild_position += (3 * ALIGNED_FRAME_LENGTH);

    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              hwm_position);
    EXPECT_EQ(called, 0);
    EXPECT_FALSE(loss_found);

    m_time = 100 * 1000 * 1000L;
    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              hwm_position);
    EXPECT_EQ(called, 0);
    EXPECT_FALSE(loss_found);
}

TEST_F(LossDetectorTest, shouldHandleMoreThan2Gaps)
{
    int64_t rebuild_position = 0;
    const int64_t hwm_position = rebuild_position + (ALIGNED_FRAME_LENGTH * 7);
    bool loss_found;
    int called = 0;

    insert_frame(offset_of_message(0));
    insert_frame(offset_of_message(2));
    insert_frame(offset_of_message(4));
    insert_frame(offset_of_message(6));

    ASSERT_EQ(feedback_delay_state_init(false), 0);
    ASSERT_EQ(aeron_loss_detector_init(
                  &m_detector, &m_delay_generator_state, LossDetectorTest::on_gap_detected, this),
              0);

    m_on_gap_detected =
        [&](int32_t term_id, int32_t term_offset, size_t length)
    {
        EXPECT_EQ(term_id, TERM_ID);
        EXPECT_EQ(length, ALIGNED_FRAME_LENGTH);
        called++;

        if (1 == called)
        {
            EXPECT_EQ(term_offset, offset_of_message(1));
        }
        else if (2 == called)
        {
            EXPECT_EQ(term_offset, offset_of_message(3));
        }
        else if (3 == called)
        {
            EXPECT_EQ(term_offset, offset_of_message(5));
        }
    };

    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(1));
    EXPECT_EQ(called, 0);
    EXPECT_TRUE(loss_found);

    m_time = 40 * 1000 * 1000L;
    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(1));
    EXPECT_EQ(called, 3);
    EXPECT_FALSE(loss_found);

    insert_frame(offset_of_message(1));
    rebuild_position += (3 * ALIGNED_FRAME_LENGTH);

    called = 1;
    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(3));
    EXPECT_EQ(called, 1);
    EXPECT_TRUE(loss_found);

    m_time = 80 * 1000 * 1000L;
    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(3));
    EXPECT_EQ(called, 3);
    EXPECT_FALSE(loss_found);
}

TEST_F(LossDetectorTest, shouldReplaceOldNakWithNewNak)
{
    int64_t rebuild_position = 0;
    int64_t hwm_position = rebuild_position + (ALIGNED_FRAME_LENGTH * 3);
    bool loss_found;
    int called = 0;

    insert_frame(offset_of_message(0));
    insert_frame(offset_of_message(2));

    ASSERT_EQ(feedback_delay_state_init(false), 0);
    ASSERT_EQ(aeron_loss_detector_init(
                  &m_detector, &m_delay_generator_state, LossDetectorTest::on_gap_detected, this),
              0);

    m_on_gap_detected =
        [&](int32_t term_id, int32_t term_offset, size_t length)
    {
        EXPECT_EQ(term_id, TERM_ID);
        EXPECT_EQ(length, ALIGNED_FRAME_LENGTH);
        EXPECT_EQ(term_offset, offset_of_message(3));
        called++;
    };

    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(1));
    EXPECT_EQ(called, 0);
    EXPECT_TRUE(loss_found);

    m_time = 10 * 1000 * 1000L;
    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(1));
    EXPECT_EQ(called, 0);
    EXPECT_FALSE(loss_found);

    insert_frame(offset_of_message(4));
    insert_frame(offset_of_message(1));
    rebuild_position += (3 * ALIGNED_FRAME_LENGTH);
    hwm_position = (ALIGNED_FRAME_LENGTH * 5);

    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(3));
    EXPECT_EQ(called, 0);
    EXPECT_TRUE(loss_found);

    m_time = 100 * 1000 * 1000L;
    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(3));
    EXPECT_EQ(called, 1);
    EXPECT_FALSE(loss_found);
}

TEST_F(LossDetectorTest, shouldHandleImmediateNak)
{
    const int64_t rebuild_position = 0;
    const int64_t hwm_position = rebuild_position + (ALIGNED_FRAME_LENGTH * 3);
    bool loss_found;
    int called = 0;

    insert_frame(offset_of_message(0));
    insert_frame(offset_of_message(2));

    ASSERT_EQ(feedback_delay_state_init(true), 0);
    ASSERT_EQ(aeron_loss_detector_init(
                  &m_detector, &m_delay_generator_state, LossDetectorTest::on_gap_detected, this),
              0);

    m_on_gap_detected =
        [&](int32_t term_id, int32_t term_offset, size_t length)
    {
        EXPECT_EQ(term_id, TERM_ID);
        EXPECT_EQ(term_offset, offset_of_message(1));
        EXPECT_EQ(length, ALIGNED_FRAME_LENGTH);
        called++;
    };

    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(1));
    EXPECT_EQ(called, 1);
    EXPECT_TRUE(loss_found);
}

TEST_F(LossDetectorTest, shouldNotNakImmediatelyByDefault)
{
    const int64_t rebuild_position = 0;
    const int64_t hwm_position = rebuild_position + (ALIGNED_FRAME_LENGTH * 3);
    bool loss_found;
    int called = 0;

    insert_frame(offset_of_message(0));
    insert_frame(offset_of_message(2));

    ASSERT_EQ(feedback_delay_state_init(false), 0);
    ASSERT_EQ(aeron_loss_detector_init(
                  &m_detector, &m_delay_generator_state, LossDetectorTest::on_gap_detected, this),
              0);

    m_on_gap_detected =
        [&](int32_t term_id, int32_t term_offset, size_t length)
    {
        EXPECT_EQ(term_id, TERM_ID);
        EXPECT_EQ(term_offset, offset_of_message(1));
        EXPECT_EQ(length, ALIGNED_FRAME_LENGTH);
        called++;
    };

    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(1));
    EXPECT_EQ(called, 0);
    EXPECT_TRUE(loss_found);
}

TEST_F(LossDetectorTest, shouldOnlySendNaksOnceOnMultipleScans)
{
    const int64_t rebuild_position = 0;
    const int64_t hwm_position = rebuild_position + (ALIGNED_FRAME_LENGTH * 3);
    bool loss_found;
    int called = 0;

    insert_frame(offset_of_message(0));
    insert_frame(offset_of_message(2));

    ASSERT_EQ(feedback_delay_state_init(true), 0);
    ASSERT_EQ(aeron_loss_detector_init(
                  &m_detector, &m_delay_generator_state, LossDetectorTest::on_gap_detected, this),
              0);

    m_on_gap_detected =
        [&](int32_t term_id, int32_t term_offset, size_t length)
    {
        EXPECT_EQ(term_id, TERM_ID);
        EXPECT_EQ(term_offset, offset_of_message(1));
        EXPECT_EQ(length, ALIGNED_FRAME_LENGTH);
        called++;
    };

    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(1));
    EXPECT_EQ(called, 1);
    EXPECT_TRUE(loss_found);
    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(1));
    EXPECT_EQ(called, 1);
    EXPECT_FALSE(loss_found);
}

TEST_F(LossDetectorTest, shouldHandleHwmGreaterThanCompletedBuffer)
{
    int64_t rebuild_position = 0;
    const int64_t hwm_position = rebuild_position + CAPACITY + ALIGNED_FRAME_LENGTH;
    bool loss_found;
    int called = 0;

    insert_frame(offset_of_message(0));
    rebuild_position += ALIGNED_FRAME_LENGTH;

    ASSERT_EQ(feedback_delay_state_init(true), 0);
    ASSERT_EQ(aeron_loss_detector_init(
                  &m_detector, &m_delay_generator_state, LossDetectorTest::on_gap_detected, this),
              0);

    m_on_gap_detected =
        [&](int32_t term_id, int32_t term_offset, size_t length)
    {
        called++;

        if (called == 1)
        {
            EXPECT_EQ(term_id, TERM_ID);
            EXPECT_EQ(term_offset, offset_of_message(1));
            EXPECT_EQ(length, CAPACITY - (size_t)rebuild_position);
        }

        if (called == 2)
        {
            EXPECT_EQ(term_id, TERM_ID + 1);
            EXPECT_EQ(term_offset, 0);
            EXPECT_EQ(length, hwm_position - CAPACITY);
        }
    };

    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(1));
    EXPECT_EQ(called, 2);
    EXPECT_TRUE(loss_found);
}

TEST_F(LossDetectorTest, shouldHandleNonZeroInitialTermOffset)
{
    int64_t rebuild_position = ALIGNED_FRAME_LENGTH * 3;
    const int64_t hwm_position = ALIGNED_FRAME_LENGTH * 5;
    bool loss_found;
    int called = 0;

    insert_frame(offset_of_message(2));
    insert_frame(offset_of_message(4));

    ASSERT_EQ(feedback_delay_state_init(true), 0);
    ASSERT_EQ(aeron_loss_detector_init(
                  &m_detector, &m_delay_generator_state, LossDetectorTest::on_gap_detected, this),
              0);

    m_on_gap_detected =
        [&](int32_t term_id, int32_t term_offset, size_t length)
    {
        EXPECT_EQ(term_id, TERM_ID);
        EXPECT_EQ(term_offset, offset_of_message(3));
        EXPECT_EQ(length, ALIGNED_FRAME_LENGTH);
        called++;
    };

    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, hwm_position, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(3));
    EXPECT_EQ(called, 1);
    EXPECT_TRUE(loss_found);
}

TEST_F(LossDetectorTest, shouldDetectChangesInTheGapLength)
{
    const int64_t rebuild_position = ALIGNED_FRAME_LENGTH * 3;
    bool loss_found;
    int32_t called = 0;

    insert_frame(offset_of_message(2));
    insert_frame(offset_of_message(5));

    ASSERT_EQ(feedback_delay_state_init(true), 0);
    ASSERT_EQ(aeron_loss_detector_init(
                  &m_detector, &m_delay_generator_state, LossDetectorTest::on_gap_detected, this),
              0);

    m_on_gap_detected =
        [&](int32_t term_id, int32_t term_offset, size_t length)
    {
        EXPECT_EQ(term_id, TERM_ID);
        EXPECT_EQ(term_offset, offset_of_message(3));
        EXPECT_EQ(length, 32);
        called++;
    };

    called = 0;
    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, rebuild_position + 32, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(3));
    EXPECT_EQ(called, 1);
    EXPECT_TRUE(loss_found);

    m_on_gap_detected =
        [&](int32_t term_id, int32_t term_offset, size_t length)
    {
        EXPECT_EQ(term_id, TERM_ID);
        EXPECT_EQ(term_offset, offset_of_message(3));
        EXPECT_EQ(length, 64);
        called++;
    };

    called = 0;
    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, rebuild_position + 64, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(3));
    EXPECT_EQ(called, 1);
    EXPECT_TRUE(loss_found);

    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, rebuild_position + 64, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(3));
    EXPECT_EQ(called, 1);
    EXPECT_FALSE(loss_found);

    m_on_gap_detected =
        [&](int32_t term_id, int32_t term_offset, size_t length)
    {
        EXPECT_EQ(term_id, TERM_ID);
        EXPECT_EQ(term_offset, offset_of_message(3));
        EXPECT_EQ(length, 32);
        called++;
    };

    called = 0;
    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, rebuild_position + 32, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(3));
    EXPECT_EQ(called, 0);
    EXPECT_FALSE(loss_found);

    m_on_gap_detected =
        [&](int32_t term_id, int32_t term_offset, size_t length)
    {
        called++;

        if (called == 1)
        {
            EXPECT_EQ(term_id, TERM_ID);
            EXPECT_EQ(term_offset, offset_of_message(3));
            EXPECT_EQ(length, ALIGNED_FRAME_LENGTH * 2);
        }

        if (called == 2)
        {
            EXPECT_EQ(term_id, TERM_ID);
            EXPECT_EQ(term_offset, offset_of_message(6));
            EXPECT_EQ(length, CAPACITY - offset_of_message(6));
        }

        if (called == 3)
        {
            EXPECT_EQ(term_id, TERM_ID + 1);
            EXPECT_EQ(term_offset, 0);
            EXPECT_EQ(length, rebuild_position);
        }
    };

    called = 0;
    ASSERT_EQ(aeron_loss_detector_scan(
                  &m_detector, &loss_found, m_ptr, m_next_ptr, rebuild_position, rebuild_position + CAPACITY, m_time, MASK, POSITION_BITS_TO_SHIFT, TERM_ID),
              offset_of_message(3));
    EXPECT_EQ(called, 3);
    EXPECT_TRUE(loss_found);
}
