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
#include <thread>
#include <atomic>
#include <limits>

#include <gtest/gtest.h>

extern "C"
{
#include "concurrent/aeron_spsc_rb.h"
#include "util/aeron_error.h"
}
#undef max

#define CAPACITY (1024)
#define BUFFER_SZ (CAPACITY + AERON_RB_TRAILER_LENGTH)
#define ODD_BUFFER_SZ ((CAPACITY - 1) + AERON_RB_TRAILER_LENGTH)
#define MSG_TYPE_ID (101)

typedef std::array<std::uint8_t, BUFFER_SZ> buffer_t;
typedef std::array<std::uint8_t, ODD_BUFFER_SZ> odd_sized_buffer_t;

class SpscRbTest : public testing::Test
{
public:

    SpscRbTest()
    {
        m_buffer.fill(0);
        m_srcBuffer.fill(0);
    }

protected:
    buffer_t m_buffer = {};
    buffer_t m_srcBuffer = {};
};

TEST_F(SpscRbTest, shouldCalculateCapacityForBuffer)
{
    aeron_spsc_rb_t rb;

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    EXPECT_EQ(rb.capacity, BUFFER_SZ - AERON_RB_TRAILER_LENGTH);
}

TEST_F(SpscRbTest, shouldErrorForCapacityNotPowerOfTwo)
{
    aeron_spsc_rb_t rb;

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size() - 1), -1);
}

TEST_F(SpscRbTest, shouldErrorForCapacityLessThanTheMinCapacity)
{
    aeron_spsc_rb_t rb;
    const size_t capacity = (AERON_SPSC_RB_MIN_CAPACITY / 2);

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), AERON_RB_TRAILER_LENGTH + capacity), -1);
    ASSERT_EQ(aeron_errcode(), EINVAL);
    const std::string expected_err_msg = "Invalid capacity: " + std::to_string(capacity);
    const std::string actual_err_msg = std::string(aeron_errmsg());
    ASSERT_NE(actual_err_msg.find(expected_err_msg), std::string::npos);
}

TEST_F(SpscRbTest, shouldErrorWhenMaxMessageSizeExceeded)
{
    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);

    EXPECT_EQ(aeron_spsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), rb.max_message_length + 1), AERON_RB_ERROR);
}

TEST_F(SpscRbTest, shouldErrorWhenMinCapacityIsUsedAndMessageSizeIsNotZero)
{
    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), AERON_RB_TRAILER_LENGTH + AERON_SPSC_RB_MIN_CAPACITY), 0);

    EXPECT_EQ(rb.max_message_length, 0);
    EXPECT_EQ(aeron_spsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), 1), AERON_RB_ERROR);
}

TEST_F(SpscRbTest, shouldWriteAnEmptyMessageWhenMinCapacityIsUsed)
{
    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), AERON_RB_TRAILER_LENGTH + AERON_SPSC_RB_MIN_CAPACITY), 0);

    EXPECT_EQ(0, rb.max_message_length);
    EXPECT_EQ(aeron_spsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), 0), AERON_RB_SUCCESS);

    auto *record = (aeron_rb_record_descriptor_t *)(m_buffer.data());

    EXPECT_EQ(record->length, (int32_t)AERON_RB_RECORD_HEADER_LENGTH);
    EXPECT_EQ(record->msg_type_id, (int32_t)MSG_TYPE_ID);
    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)(AERON_RB_ALIGNMENT));
}

TEST_F(SpscRbTest, shouldErrorWhenMessageTypeIsNegative)
{
    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);

    EXPECT_EQ(aeron_spsc_rb_write(&rb, AERON_RB_PADDING_MSG_TYPE_ID, m_srcBuffer.data(), 5), AERON_RB_ERROR);
}

TEST_F(SpscRbTest, shouldErrorWhenMessageTypeIsZero)
{
    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);

    EXPECT_EQ(aeron_spsc_rb_write(&rb, 0, m_srcBuffer.data(), 5), AERON_RB_ERROR);
}

TEST_F(SpscRbTest, shouldWriteToEmptyBuffer)
{
    aeron_spsc_rb_t rb;
    size_t tail = 0;
    size_t tailIndex = 0;
    size_t length = 8;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);

    ASSERT_EQ(aeron_spsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), length), AERON_RB_SUCCESS);

    auto *record = (aeron_rb_record_descriptor_t *)(m_buffer.data() + tailIndex);

    EXPECT_EQ(record->length, (int32_t)recordLength);
    EXPECT_EQ(record->msg_type_id, (int32_t)MSG_TYPE_ID);
    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)(tail + alignedRecordLength));
}

TEST_F(SpscRbTest, shouldWriteVectorToEmptyBuffer)
{
    aeron_spsc_rb_t rb;
    size_t tail = 0;
    size_t tailIndex = 0;

    const int vec_len = 3;
    struct iovec vec[vec_len];
    vec[0].iov_base = m_srcBuffer.data();
    vec[0].iov_len = 8;
    vec[1].iov_base = m_srcBuffer.data() + (vec[0].iov_len);
    vec[1].iov_len = 7;
    vec[2].iov_base = m_srcBuffer.data() + (vec[0].iov_len + vec[1].iov_len);
    vec[2].iov_len = 11;
    size_t length = vec[0].iov_len + vec[1].iov_len + vec[2].iov_len;

    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);

    ASSERT_EQ(aeron_spsc_rb_writev(&rb, MSG_TYPE_ID, vec, vec_len), AERON_RB_SUCCESS);

    auto *record = (aeron_rb_record_descriptor_t *)(m_buffer.data() + tailIndex);

    EXPECT_EQ(record->length, (int32_t)recordLength);
    EXPECT_EQ(record->msg_type_id, (int32_t)MSG_TYPE_ID);
    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)(tail + alignedRecordLength));
}

TEST_F(SpscRbTest, shouldRejectWriteWhenInsufficientSpace)
{
    aeron_spsc_rb_t rb;
    size_t length = 100;
    size_t head = 0;
    size_t tail = head + (CAPACITY - AERON_ALIGN(length - AERON_RB_ALIGNMENT, AERON_RB_ALIGNMENT));

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    ASSERT_EQ(aeron_spsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), length), AERON_RB_FULL);

    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)tail);
}

TEST_F(SpscRbTest, shouldRejectWriteVectorWhenInsufficientSpace)
{
    aeron_spsc_rb_t rb;

    const int vec_len = 3;
    struct iovec vec[vec_len];
    vec[0].iov_base = m_srcBuffer.data();
    vec[0].iov_len = 1;
    vec[1].iov_base = m_srcBuffer.data() + (vec[0].iov_len);
    vec[1].iov_len = 1;
    vec[2].iov_base = m_srcBuffer.data() + (vec[0].iov_len + vec[1].iov_len);
    vec[2].iov_len = 98;
    size_t length = vec[0].iov_len + vec[1].iov_len + vec[2].iov_len;

    size_t head = 0;
    size_t tail = head + (CAPACITY - AERON_ALIGN(length - AERON_RB_ALIGNMENT, AERON_RB_ALIGNMENT));

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    ASSERT_EQ(aeron_spsc_rb_writev(&rb, MSG_TYPE_ID, vec, vec_len), AERON_RB_FULL);

    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)tail);
}

TEST_F(SpscRbTest, shouldRejectWriteWhenBufferFull)
{
    aeron_spsc_rb_t rb;
    size_t length = 8;
    size_t head = 0;
    size_t tail = head + CAPACITY;

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    ASSERT_EQ(aeron_spsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), length), AERON_RB_FULL);

    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)tail);
}

TEST_F(SpscRbTest, shouldInsertPaddingRecordPlusMessageOnBufferWrap)
{
    aeron_spsc_rb_t rb;
    size_t length = 100;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);
    size_t tail = CAPACITY - AERON_RB_ALIGNMENT;
    size_t head = tail - (AERON_RB_ALIGNMENT * 4);

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    ASSERT_EQ(aeron_spsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), length), AERON_RB_SUCCESS);

    auto *record = (aeron_rb_record_descriptor_t *)(rb.buffer + tail);
    EXPECT_EQ(record->msg_type_id, (int32_t)AERON_RB_PADDING_MSG_TYPE_ID);
    EXPECT_EQ(record->length, (int32_t)AERON_RB_ALIGNMENT);

    record = (aeron_rb_record_descriptor_t *)(rb.buffer);
    EXPECT_EQ(record->msg_type_id, (int32_t)MSG_TYPE_ID);
    EXPECT_EQ(record->length, (int32_t)recordLength);
    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)(tail + alignedRecordLength + AERON_RB_ALIGNMENT));
}

TEST_F(SpscRbTest, shouldInsertPaddingRecordPlusMessageOnBufferWrapWithHeadEqualToTail)
{
    aeron_spsc_rb_t rb;
    size_t length = 100;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);
    size_t tail = CAPACITY - AERON_RB_ALIGNMENT;
    size_t head = tail;

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    ASSERT_EQ(aeron_spsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), length), AERON_RB_SUCCESS);

    auto *record = (aeron_rb_record_descriptor_t *)(rb.buffer + tail);
    EXPECT_EQ(record->msg_type_id, (int32_t)AERON_RB_PADDING_MSG_TYPE_ID);
    EXPECT_EQ(record->length, (int32_t)AERON_RB_ALIGNMENT);

    record = (aeron_rb_record_descriptor_t *)(rb.buffer);
    EXPECT_EQ(record->msg_type_id, (int32_t)MSG_TYPE_ID);
    EXPECT_EQ(record->length, (int32_t)recordLength);
    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)(tail + alignedRecordLength + AERON_RB_ALIGNMENT));
}

static void countTimesAsSizeT(int32_t msg_type_id, const void *msg, size_t length, void *clientd)
{
    auto *count = (size_t *)clientd;

    (*count)++; /* unused */
}

TEST_F(SpscRbTest, shouldReadNothingFromEmptyBuffer)
{
    aeron_spsc_rb_t rb;
    size_t tail = 0;
    size_t head = 0;

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    size_t timesCalled = 0;
    const size_t messagesRead = aeron_spsc_rb_read(&rb, countTimesAsSizeT, &timesCalled, 10);

    EXPECT_EQ(messagesRead, (size_t)0);
    EXPECT_EQ(timesCalled, (size_t)0);
}

TEST_F(SpscRbTest, shouldReadSingleMessage)
{
    aeron_spsc_rb_t rb;
    size_t length = 8;
    size_t head = 0;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);
    size_t tail = alignedRecordLength;

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    auto *record = (aeron_rb_record_descriptor_t *)(rb.buffer);
    record->msg_type_id = (int32_t)MSG_TYPE_ID;
    record->length = (int32_t)recordLength;

    size_t timesCalled = 0;
    const size_t messagesRead = aeron_spsc_rb_read(&rb, countTimesAsSizeT, &timesCalled, 10);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
    EXPECT_EQ(rb.descriptor->head_position, (int64_t)(head + alignedRecordLength));
}

TEST_F(SpscRbTest, shouldNotReadSingleMessagePartWayThroughWriting)
{
    aeron_spsc_rb_t rb;
    size_t length = 8;
    size_t head = 0;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);
    size_t endTail = alignedRecordLength;

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)endTail;

    auto *record = (aeron_rb_record_descriptor_t *)(rb.buffer);
    record->msg_type_id = (int32_t)MSG_TYPE_ID;
    record->length = -((int32_t)recordLength);

    size_t timesCalled = 0;
    const size_t messagesRead = aeron_spsc_rb_read(&rb, countTimesAsSizeT, &timesCalled, 10);

    EXPECT_EQ(messagesRead, (size_t)0);
    EXPECT_EQ(timesCalled, (size_t)0);
    EXPECT_EQ(rb.descriptor->head_position, (int64_t)head);
}

TEST_F(SpscRbTest, shouldReadTwoMessages)
{
    aeron_spsc_rb_t rb;
    size_t length = 8;
    size_t head = 0;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);
    size_t tail = alignedRecordLength * 2;

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    aeron_rb_record_descriptor_t *record;

    record = (aeron_rb_record_descriptor_t *)(rb.buffer);
    record->msg_type_id = (int32_t)MSG_TYPE_ID;
    record->length = (int32_t)recordLength;

    record = (aeron_rb_record_descriptor_t *)(rb.buffer + alignedRecordLength);
    record->msg_type_id = (int32_t)MSG_TYPE_ID;
    record->length = (int32_t)recordLength;

    size_t timesCalled = 0;
    const size_t messagesRead = aeron_spsc_rb_read(&rb, countTimesAsSizeT, &timesCalled, 10);

    EXPECT_EQ(messagesRead, (size_t)2);
    EXPECT_EQ(timesCalled, (size_t)2);
    EXPECT_EQ(rb.descriptor->head_position, (int64_t)(head + (alignedRecordLength * 2)));
}

TEST_F(SpscRbTest, shouldLimitReadOfMessages)
{
    aeron_spsc_rb_t rb;
    size_t length = 8;
    size_t head = 0;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);
    size_t tail = alignedRecordLength * 2;

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    aeron_rb_record_descriptor_t *record;

    record = (aeron_rb_record_descriptor_t *)(rb.buffer);
    record->msg_type_id = (int32_t)MSG_TYPE_ID;
    record->length = (int32_t)recordLength;

    record = (aeron_rb_record_descriptor_t *)(rb.buffer + alignedRecordLength);
    record->msg_type_id = (int32_t)MSG_TYPE_ID;
    record->length = (int32_t)recordLength;

    size_t timesCalled = 0;
    const size_t messagesRead = aeron_spsc_rb_read(&rb, countTimesAsSizeT, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
    EXPECT_EQ(rb.descriptor->head_position, (int64_t)(head + alignedRecordLength));
}

TEST_F(SpscRbTest, shouldPutMessageAtTheEndOfTheBufferWithoutPaddingAfterReadUnblocksZeroingOfTheNextHeader)
{
    aeron_spsc_rb_t rb;
    const int32_t msgType = 555;
    const size_t msgLength = (CAPACITY / 8) - AERON_RB_RECORD_HEADER_LENGTH;
    const size_t alignedRecordLength = AERON_ALIGN(CAPACITY / 8, AERON_RB_ALIGNMENT);

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);

    m_srcBuffer.fill(7);
    for (int i = 0; i < 7; i++)
    {
        EXPECT_EQ(aeron_spsc_rb_write(&rb, msgType, m_srcBuffer.data(), msgLength), AERON_RB_SUCCESS);
    }
    m_srcBuffer.fill(5);
    EXPECT_EQ(aeron_spsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), msgLength), AERON_RB_FULL);

    size_t timesCalled = 0;
    size_t messagesRead = aeron_spsc_rb_read(&rb, countTimesAsSizeT, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);

    EXPECT_EQ(aeron_spsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), msgLength), AERON_RB_SUCCESS);

    EXPECT_EQ(rb.descriptor->head_position, (int64_t)alignedRecordLength);
    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)(CAPACITY));

    aeron_rb_record_descriptor_t *record;

    // assert that the next message header was zeroed correctly
    record = (aeron_rb_record_descriptor_t *)(rb.buffer);
    EXPECT_EQ(record->msg_type_id, 0);
    EXPECT_EQ(record->length, 0);

    // second message
    record = (aeron_rb_record_descriptor_t *)(rb.buffer + alignedRecordLength);
    EXPECT_EQ(record->msg_type_id, msgType);
    EXPECT_EQ(record->length, (int32_t)(msgLength + AERON_RB_RECORD_HEADER_LENGTH));

    // last message
    record = (aeron_rb_record_descriptor_t *)(rb.buffer + (CAPACITY - alignedRecordLength));
    EXPECT_EQ(record->msg_type_id, MSG_TYPE_ID);
    EXPECT_EQ(record->length, (int32_t)(msgLength + AERON_RB_RECORD_HEADER_LENGTH));
}

TEST_F(SpscRbTest, tryClaimShouldErrorWhenMessageTypeIsZero)
{
    aeron_spsc_rb_t rb;
    ASSERT_EQ(0, aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()));

    EXPECT_EQ(AERON_RB_ERROR, aeron_spsc_rb_try_claim(&rb, 0, 5));
}

TEST_F(SpscRbTest, tryClaimShouldErrorWhenMessageTypeIsNegative)
{
    aeron_spsc_rb_t rb;
    ASSERT_EQ(0, aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()));

    EXPECT_EQ(AERON_RB_ERROR, aeron_spsc_rb_try_claim(&rb, -3, 5));
}

TEST_F(SpscRbTest, tryClaimShouldErrorWhenLengthExceedMaxMessageSize)
{
    aeron_spsc_rb_t rb;
    ASSERT_EQ(0, aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()));

    EXPECT_EQ(AERON_RB_ERROR, aeron_spsc_rb_try_claim(&rb, 6, rb.max_message_length + 1));
}

TEST_F(SpscRbTest, tryClaimShouldErrorBufferIsFull)
{
    aeron_spsc_rb_t rb;
    size_t length = 8;
    size_t head = 0;
    size_t tail = head + CAPACITY;

    ASSERT_EQ(0, aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()));
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    EXPECT_EQ(AERON_RB_FULL, aeron_spsc_rb_try_claim(&rb, MSG_TYPE_ID, length));
    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)tail);
}

TEST_F(SpscRbTest, tryClaimShouldReturnMessageOffsetUponSuccess)
{
    int msg_type_id = 17;
    size_t length = 100;

    aeron_spsc_rb_t rb;
    ASSERT_EQ(0, aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()));

    EXPECT_EQ((int32_t)AERON_RB_RECORD_HEADER_LENGTH, aeron_spsc_rb_try_claim(&rb, msg_type_id, length));
    auto *record_header = (aeron_rb_record_descriptor_t *)(rb.buffer);
    EXPECT_EQ(msg_type_id, record_header->msg_type_id);
    EXPECT_EQ(-(int32_t)(length + AERON_RB_RECORD_HEADER_LENGTH), record_header->length);
}

TEST_F(SpscRbTest, commitShouldReturnErrorIfOffsetIsNegative)
{
    aeron_spsc_rb_t rb;
    ASSERT_EQ(0, aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()));

    EXPECT_EQ(-1, aeron_spsc_rb_commit(&rb, -2));
}

TEST_F(SpscRbTest, commitShouldReturnErrorIfOffsetIsExceedsBufferCapacity)
{
    aeron_spsc_rb_t rb;
    ASSERT_EQ(0, aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()));

    EXPECT_EQ(-1, aeron_spsc_rb_commit(&rb, (int32_t)(m_buffer.size() + 1)));
}

TEST_F(SpscRbTest, commitShouldReturnErrorIfOffsetIsSmallerThanRecordHeader)
{
    aeron_spsc_rb_t rb;
    ASSERT_EQ(0, aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()));

    EXPECT_EQ(-1, aeron_spsc_rb_commit(&rb, (int32_t)(m_buffer.size() - AERON_RB_RECORD_HEADER_LENGTH + 1)));
}

TEST_F(SpscRbTest, commitShouldReturnZeroUponSuccess)
{
    size_t tail = 200;
    size_t length = 50;

    aeron_spsc_rb_t rb;
    ASSERT_EQ(0, aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()));
    rb.descriptor->tail_position = (int64_t)tail;

    int32_t offset = aeron_spsc_rb_try_claim(&rb, MSG_TYPE_ID, length);
    EXPECT_EQ((int32_t)AERON_RB_MESSAGE_OFFSET(tail), offset);

    EXPECT_EQ(0, aeron_spsc_rb_commit(&rb, offset));
    auto *record_header = (aeron_rb_record_descriptor_t *)(rb.buffer + (offset - AERON_RB_RECORD_HEADER_LENGTH));
    EXPECT_EQ(MSG_TYPE_ID, record_header->msg_type_id);
    EXPECT_EQ((int32_t)(length + AERON_RB_RECORD_HEADER_LENGTH), record_header->length);
}

TEST_F(SpscRbTest, abortShouldReturnErrorIfOffsetIsNegative)
{
    aeron_spsc_rb_t rb;
    ASSERT_EQ(0, aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()));

    EXPECT_EQ(-1, aeron_spsc_rb_abort(&rb, -10));
}

TEST_F(SpscRbTest, abortShouldReturnErrorIfOffsetIsExceedsBufferCapacity)
{
    aeron_spsc_rb_t rb;
    ASSERT_EQ(0, aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()));

    EXPECT_EQ(-1, aeron_spsc_rb_abort(&rb, (int32_t)(m_buffer.size() + 8)));
}

TEST_F(SpscRbTest, abortShouldReturnErrorIfOffsetIsSmallerThanRecordHeader)
{
    aeron_spsc_rb_t rb;
    ASSERT_EQ(0, aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()));

    EXPECT_EQ(-1, aeron_spsc_rb_abort(&rb, (int32_t)(m_buffer.size() - 1)));
}

TEST_F(SpscRbTest, abortShouldReturnZeroUponSuccess)
{
    size_t length = 32;

    aeron_spsc_rb_t rb;
    ASSERT_EQ(0, aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()));

    int32_t offset = aeron_spsc_rb_try_claim(&rb, MSG_TYPE_ID, length);
    EXPECT_EQ((int32_t)AERON_RB_MESSAGE_OFFSET(0), offset);

    EXPECT_EQ(0, aeron_spsc_rb_abort(&rb, offset));
    auto *record_header = (aeron_rb_record_descriptor_t *)(rb.buffer + (offset - AERON_RB_RECORD_HEADER_LENGTH));
    EXPECT_EQ(AERON_RB_PADDING_MSG_TYPE_ID, record_header->msg_type_id);
    EXPECT_EQ((int32_t)(length + AERON_RB_RECORD_HEADER_LENGTH), record_header->length);
}


struct aeron_spsc_rb_control_test_clientd_t
{
    int64_t value;
    aeron_rb_read_action_t action_for_value;
    int result_index;
    int64_t results[10];
};

aeron_rb_read_action_t controlled_read_with_action(int32_t msg_type_id, const void *data, size_t length, void *clientd)
{
     auto *test_clientd = static_cast<aeron_spsc_rb_control_test_clientd_t *>(clientd);
    int64_t value = *(int64_t*)data;
    aeron_rb_read_action_stct action_for_value = value == test_clientd->value ?
        test_clientd->action_for_value : AERON_RB_CONTINUE;

    test_clientd->results[test_clientd->result_index] = value;
    test_clientd->result_index++;

    return action_for_value;
}

TEST_F(SpscRbTest, shouldAbortControlledRead)
{
    aeron_spsc_rb_control_test_clientd_t clientd{ 3, AERON_RB_ABORT, 0, {} };

    AERON_DECL_ALIGNED(buffer_t spsc_buffer, 16) = {};
    spsc_buffer.fill(0);

    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, spsc_buffer.data(), spsc_buffer.size()), 0);

    int64_t data = 1;
    for (int i = 1; i <= 5; i++)
    {
        ASSERT_EQ(AERON_RB_SUCCESS, aeron_spsc_rb_write(&rb, 1, &data, sizeof(data)));
        data++;
    }

    EXPECT_EQ(2, aeron_spsc_rb_controlled_read(&rb, controlled_read_with_action, &clientd, 5));
    EXPECT_EQ(3, clientd.result_index);
    EXPECT_EQ(1, clientd.results[0]);
    EXPECT_EQ(2, clientd.results[1]);
    EXPECT_EQ(3, clientd.results[2]);
    EXPECT_EQ(0, clientd.results[3]);

    clientd.action_for_value = AERON_RB_CONTINUE;

    EXPECT_EQ(3, aeron_spsc_rb_controlled_read(&rb, controlled_read_with_action, &clientd, 5));
    EXPECT_EQ(6, clientd.result_index);
    EXPECT_EQ(1, clientd.results[0]);
    EXPECT_EQ(2, clientd.results[1]);
    EXPECT_EQ(3, clientd.results[2]);
    EXPECT_EQ(3, clientd.results[3]);
    EXPECT_EQ(4, clientd.results[4]);
    EXPECT_EQ(5, clientd.results[5]);
    EXPECT_EQ(0, clientd.results[6]);
}

TEST_F(SpscRbTest, shouldBreakControlledRead)
{
    aeron_spsc_rb_control_test_clientd_t clientd{ 3, AERON_RB_BREAK, 0, {} };

    AERON_DECL_ALIGNED(buffer_t spsc_buffer, 16) = {};
    spsc_buffer.fill(0);

    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, spsc_buffer.data(), spsc_buffer.size()), 0);

    int64_t data = 1;
    for (int i = 1; i <= 5; i++)
    {
        ASSERT_EQ(AERON_RB_SUCCESS, aeron_spsc_rb_write(&rb, 1, &data, sizeof(data)));
        data++;
    }

    EXPECT_EQ(3, aeron_spsc_rb_controlled_read(&rb, controlled_read_with_action, &clientd, 5));
    EXPECT_EQ(3, clientd.result_index);
    EXPECT_EQ(1, clientd.results[0]);
    EXPECT_EQ(2, clientd.results[1]);
    EXPECT_EQ(3, clientd.results[2]);
    EXPECT_EQ(0, clientd.results[3]);

    clientd.action_for_value = AERON_RB_CONTINUE;

    EXPECT_EQ(2, aeron_spsc_rb_controlled_read(&rb, controlled_read_with_action, &clientd, 5));
    EXPECT_EQ(5, clientd.result_index);
    EXPECT_EQ(1, clientd.results[0]);
    EXPECT_EQ(2, clientd.results[1]);
    EXPECT_EQ(3, clientd.results[2]);
    EXPECT_EQ(4, clientd.results[3]);
    EXPECT_EQ(5, clientd.results[4]);
    EXPECT_EQ(0, clientd.results[5]);
}

TEST_F(SpscRbTest, shouldContinueControlledRead)
{
    aeron_spsc_rb_control_test_clientd_t clientd{ 3, AERON_RB_CONTINUE, 0, {} };

    AERON_DECL_ALIGNED(buffer_t spsc_buffer, 16) = {};
    spsc_buffer.fill(0);

    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, spsc_buffer.data(), spsc_buffer.size()), 0);

    int64_t data = 1;
    for (int i = 1; i <= 5; i++)
    {
        ASSERT_EQ(AERON_RB_SUCCESS, aeron_spsc_rb_write(&rb, 1, &data, sizeof(data)));
        data++;
    }

    EXPECT_EQ(5, aeron_spsc_rb_controlled_read(&rb, controlled_read_with_action, &clientd, 5));
    EXPECT_EQ(5, clientd.result_index);
    EXPECT_EQ(1, clientd.results[0]);
    EXPECT_EQ(2, clientd.results[1]);
    EXPECT_EQ(3, clientd.results[2]);
    EXPECT_EQ(4, clientd.results[3]);
    EXPECT_EQ(5, clientd.results[4]);
    EXPECT_EQ(0, clientd.results[5]);
}

aeron_rb_read_action_t controlled_read_with_commit(int32_t msg_type_id, const void *data, size_t length, void *clientd)
{
    auto *rb = static_cast<aeron_spsc_rb_t *>(clientd);
    int64_t value = *(int64_t*)data;

    aeron_rb_read_action_stct action_for_value = value == 3 ? AERON_RB_COMMIT : AERON_RB_CONTINUE;

    if (value <= 3)
    {
        EXPECT_EQ(0, aeron_spsc_rb_consumer_position(rb));
    }
    else
    {
        EXPECT_NE(0, aeron_spsc_rb_consumer_position(rb));
    }

    return action_for_value;
}

TEST_F(SpscRbTest, shouldCommitControlledRead)
{
    AERON_DECL_ALIGNED(buffer_t spsc_buffer, 16) = {};
    spsc_buffer.fill(0);

    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, spsc_buffer.data(), spsc_buffer.size()), 0);

    int64_t data = 1;
    for (int i = 1; i <= 5; i++)
    {
        ASSERT_EQ(AERON_RB_SUCCESS, aeron_spsc_rb_write(&rb, 1, &data, sizeof(data)));
        data++;
    }

    EXPECT_EQ(5, aeron_spsc_rb_controlled_read(&rb, controlled_read_with_commit, &rb, 5));
}

TEST_F(SpscRbTest, shouldGetSize)
{
    const int spsc_padding = 16;
    AERON_DECL_ALIGNED(buffer_t spsc_buffer, 16) = {};
    spsc_buffer.fill(0);

    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, spsc_buffer.data(), spsc_buffer.size()), 0);

    int64_t data = 1;
    size_t total_messages = CAPACITY / (AERON_RB_RECORD_HEADER_LENGTH + sizeof(data));
    ASSERT_EQ(0, aeron_spsc_rb_size(&rb));

    for (size_t i = 0; i < (total_messages / 2); i++)
    {
        ASSERT_EQ(AERON_RB_SUCCESS, aeron_spsc_rb_write(&rb, 1, &data, sizeof(data)));
        data++;
    }

    ASSERT_EQ(CAPACITY / 2, aeron_spsc_rb_size(&rb));

    aeron_rb_write_result_t result;
    do
    {
        result = aeron_spsc_rb_write(&rb, 1, &data, sizeof(data));
    }
    while (AERON_RB_SUCCESS == result);

    ASSERT_EQ(CAPACITY - spsc_padding, aeron_spsc_rb_size(&rb));
}

#define NUM_MESSAGES (10 * 1000 * 1000)
#define NUM_IDS_PER_THREAD (10 * 1000 * 1000)

TEST(SpscRbConcurrentTest, shouldProvideCorrelationIds)
{
    AERON_DECL_ALIGNED(buffer_t buffer, 16) = {};
    buffer.fill(0);

    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, buffer.data(), buffer.size()), 0);

    std::atomic<int> countDown(2);

    std::vector<std::thread> threads;

    for (int i = 0; i < 2; i++)
    {
        threads.push_back(std::thread(
            [&]()
            {
                countDown--;
                while (countDown > 0)
                {
                    std::this_thread::yield();
                }

                for (int m = 0; m < NUM_IDS_PER_THREAD; m++)
                {
                    aeron_spsc_rb_next_correlation_id(&rb);
                }
            }));
    }

    for (std::thread &t: threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }

    ASSERT_EQ(aeron_spsc_rb_next_correlation_id(&rb), NUM_IDS_PER_THREAD * 2);
}

static void spsc_rb_concurrent_handler(int32_t msg_type_id, const void *buffer, size_t length, void *clientd)
{
    auto *counts = (size_t *)clientd;
    const int32_t messageNumber = *((int32_t *)(buffer));

    EXPECT_EQ(length, (size_t)4);
    ASSERT_EQ(msg_type_id, MSG_TYPE_ID);

    EXPECT_EQ((*counts)++, (size_t)messageNumber);
}

TEST(SpscRbConcurrentTest, shouldExchangeMessages)
{
    AERON_DECL_ALIGNED(buffer_t spsc_buffer, 16) = {};
    spsc_buffer.fill(0);

    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, spsc_buffer.data(), spsc_buffer.size()), 0);

    std::atomic<int> countDown(1);

    std::vector<std::thread> threads;
    size_t msgCount = 0;
    size_t counts = 0;

    threads.push_back(std::thread(
        [&]()
        {
            AERON_DECL_ALIGNED(buffer_t buffer, 16);
            buffer.fill(0);

            countDown--;
            while (countDown > 0)
            {
                std::this_thread::yield();
            }

            for (int m = 0; m < NUM_MESSAGES; m++)
            {
                auto *payload = (int32_t *)(buffer.data());
                *payload = m;

                while (AERON_RB_SUCCESS != aeron_spsc_rb_write(&rb, MSG_TYPE_ID, buffer.data(), 4))
                {
                    std::this_thread::yield();
                }
            }
        }));

    while (msgCount < NUM_MESSAGES)
    {
        const size_t readCount = aeron_spsc_rb_read(
            &rb, spsc_rb_concurrent_handler, &counts, std::numeric_limits<size_t>::max());

        if (0 == readCount)
        {
            std::this_thread::yield();
        }

        msgCount += readCount;
    }

    for (std::thread &t: threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }
}

TEST(SpscRbConcurrentTest, shouldExchangeVectorMessages)
{
    AERON_DECL_ALIGNED(buffer_t spsc_buffer, 16) = {};
    spsc_buffer.fill(0);

    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, spsc_buffer.data(), spsc_buffer.size()), 0);

    std::atomic<int> countDown(1);

    std::vector<std::thread> threads;
    size_t msgCount = 0;
    size_t counts = 0;

    threads.push_back(std::thread(
        [&]()
        {
            struct iovec vec[2];
            AERON_DECL_ALIGNED(buffer_t buffer, 16);
            buffer.fill(0);

            countDown--;
            while (countDown > 0)
            {
                std::this_thread::yield();
            }

            for (int m = 0; m < NUM_MESSAGES; m++)
            {
                auto *payload = (int32_t *)(buffer.data());
                *payload = m;

                vec[0].iov_len = 2;
                vec[0].iov_base = payload;
                vec[1].iov_len = 2;
                vec[1].iov_base = ((uint8_t *)payload) + 2;

                while (AERON_RB_SUCCESS != aeron_spsc_rb_writev(&rb, MSG_TYPE_ID, vec, 2))
                {
                    std::this_thread::yield();
                }
            }
        }));

    while (msgCount < NUM_MESSAGES)
    {
        const size_t readCount = aeron_spsc_rb_read(
            &rb, spsc_rb_concurrent_handler, &counts, std::numeric_limits<size_t>::max());

        if (0 == readCount)
        {
            std::this_thread::yield();
        }

        msgCount += readCount;
    }

    for (std::thread &t: threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }
}

TEST(SpscRbConcurrentTest, shouldExchangeMessagesViaTryClaim)
{
    AERON_DECL_ALIGNED(buffer_t spsc_buffer, 16) = {};
    spsc_buffer.fill(0);

    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, spsc_buffer.data(), spsc_buffer.size()), 0);

    std::atomic<int> countDown(1);

    std::vector<std::thread> threads;
    size_t msgCount = 0;
    size_t counts = 0;

    threads.push_back(std::thread(
        [&]()
        {
            countDown--;
            while (countDown > 0)
            {
                std::this_thread::yield();
            }

            for (int m = 0; m < NUM_MESSAGES; m++)
            {
                int32_t offset;
                int32_t length = 4;
                while ((offset = aeron_spsc_rb_try_claim(&rb, MSG_TYPE_ID, length)) < 0)
                {
                    std::this_thread::yield();
                }

                auto *payload = (int32_t *)(rb.buffer + offset);
                *payload = m;

                aeron_spsc_rb_commit(&rb, offset);
            }
        }));

    while (msgCount < NUM_MESSAGES)
    {
        const size_t readCount = aeron_spsc_rb_read(
            &rb, spsc_rb_concurrent_handler, &counts, std::numeric_limits<size_t>::max());

        if (0 == readCount)
        {
            std::this_thread::yield();
        }

        msgCount += readCount;
    }

    for (std::thread &t: threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }
}

TEST_F(SpscRbTest, tryClaimShouldReturnFullWhenNoSpaceAtEndOrStartOfBuffer)
{
    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);

    const size_t length = 100;
    const size_t required_capacity =
        AERON_ALIGN(length + AERON_RB_RECORD_HEADER_LENGTH, AERON_RB_ALIGNMENT) + AERON_RB_RECORD_HEADER_LENGTH;

    const size_t tail = CAPACITY - (AERON_RB_ALIGNMENT * 2);          // 1008
    const size_t head = required_capacity - (AERON_RB_ALIGNMENT * 2); // 104

    // We prepare the queue so that there is insufficient space at the end
    // and beginning of the queue to for claim to succeed.
    rb.descriptor->head_position       = (int64_t)head;
    rb.descriptor->head_cache_position = (int64_t)head;
    rb.descriptor->tail_position       = (int64_t)tail;

    EXPECT_EQ(AERON_RB_FULL, aeron_spsc_rb_try_claim(&rb, MSG_TYPE_ID, length));

    EXPECT_EQ((int64_t)CAPACITY, rb.descriptor->tail_position);
    EXPECT_EQ((int64_t)head,     rb.descriptor->head_position);
    EXPECT_EQ((int64_t)head,     rb.descriptor->head_cache_position);

    // Ensure that the padding was added at the end
    auto *padding = (aeron_rb_record_descriptor_t *)(rb.buffer + tail);
    EXPECT_EQ((int32_t)(CAPACITY - tail),            padding->length);
    EXPECT_EQ((int32_t)AERON_RB_PADDING_MSG_TYPE_ID, padding->msg_type_id);
}

TEST_F(SpscRbTest, writeMustNotOverwriteUnreadMessageWhenNoSpaceAtEndOrStartOfBuffer)
{
    // this test fills up the buffer with small messages and then consume a single small
    // message at beginning  so we end up with a queue that has little space in front
    // and little space at the back. And then we try to place a large message that will
    // not fit into the available space at either side.

    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);

    // fill up the ringbuffer with small messages; there will be a bit of space left at
    // the back
    for (int32_t i = 0; i < 15; i++)
    {
        ASSERT_EQ(AERON_RB_SUCCESS, aeron_spsc_rb_write(&rb, MSG_TYPE_ID + i, m_srcBuffer.data(), 56));
    }

    // read the first messager so we have some space in the front
    size_t count = 0;
    ASSERT_EQ(1u, aeron_spsc_rb_read(&rb, countTimesAsSizeT, &count, 1));

    auto *unread = (aeron_rb_record_descriptor_t *)(rb.buffer + 64);
    const int32_t unread_length      = unread->length;
    const int32_t unread_msg_type_id = unread->msg_type_id;

    // try write a single large message
    EXPECT_EQ(AERON_RB_FULL, aeron_spsc_rb_write(&rb, MSG_TYPE_ID + 99, m_srcBuffer.data(), 128));

    // The 2nd message must be intact and not overwritten by the large message
    EXPECT_EQ(unread_length,      unread->length)      << "unread message length was overwritten";
    EXPECT_EQ(unread_msg_type_id, unread->msg_type_id) << "unread message type was overwritten";
}
