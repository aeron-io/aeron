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

#include <gtest/gtest.h>

#include "concurrent/logbuffer/Header.h"
#include "concurrent/logbuffer/LogBufferDescriptor.h"

extern "C"
{
#include "aeron_image.h"
}

using namespace aeron;
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;

static const std::int32_t STREAM_ID = 10;
static const std::int32_t SESSION_ID = 200;
static const std::int32_t TERM_OFFSET = 1024;
static const std::int32_t TERM_LENGTH = LogBufferDescriptor::TERM_MIN_LENGTH;
static const std::int32_t INITIAL_TERM_ID = -1234;
static const std::int32_t ACTIVE_TERM_ID = INITIAL_TERM_ID + 5;
static const std::int32_t PAYLOAD_LENGTH = 158;
static const std::int64_t RESERVED_VALUE = INT64_C(0x0102030405060708);
static const std::int32_t POSITION_BITS_TO_SHIFT = BitUtil::numberOfTrailingZeroes(TERM_LENGTH);

class HeaderTest : public testing::Test
{
public:
    void SetUp() override
    {
        m_frame.frame_header.frame_length = DataFrameHeader::LENGTH + PAYLOAD_LENGTH;
        m_frame.frame_header.version = DataFrameHeader::CURRENT_VERSION;
        m_frame.frame_header.flags = FrameDescriptor::UNFRAGMENTED;
        m_frame.frame_header.type = DataFrameHeader::HDR_TYPE_DATA;
        m_frame.term_offset = TERM_OFFSET;
        m_frame.session_id = SESSION_ID;
        m_frame.stream_id = STREAM_ID;
        m_frame.term_id = ACTIVE_TERM_ID;
        m_frame.reserved_value = RESERVED_VALUE;

        m_aeronHeader.frame = &m_frame;
        m_aeronHeader.initial_term_id = INITIAL_TERM_ID;
        m_aeronHeader.position_bits_to_shift = static_cast<std::size_t>(POSITION_BITS_TO_SHIFT);
        m_aeronHeader.fragmented_frame_length = AERON_NULL_VALUE;
        m_aeronHeader.context = &m_context;
    }

    void verifyAllValues(const Header &header)
    {
        EXPECT_EQ(INITIAL_TERM_ID, header.initialTermId());
        EXPECT_EQ(DataFrameHeader::LENGTH + PAYLOAD_LENGTH, header.frameLength());
        EXPECT_EQ(SESSION_ID, header.sessionId());
        EXPECT_EQ(STREAM_ID, header.streamId());
        EXPECT_EQ(ACTIVE_TERM_ID, header.termId());
        EXPECT_EQ(TERM_OFFSET, header.termOffset());
        EXPECT_EQ(DataFrameHeader::HDR_TYPE_DATA, header.type());
        EXPECT_EQ(FrameDescriptor::UNFRAGMENTED, header.flags());
        EXPECT_EQ(POSITION_BITS_TO_SHIFT, header.positionBitsToShift());
        EXPECT_EQ(RESERVED_VALUE, header.reservedValue());
    }

    std::int64_t expectedPosition() const
    {
        return LogBufferDescriptor::computePosition(
            ACTIVE_TERM_ID,
            BitUtil::align(TERM_OFFSET + DataFrameHeader::LENGTH + PAYLOAD_LENGTH, FrameDescriptor::FRAME_ALIGNMENT),
            POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID);
    }

protected:
    aeron_data_header_t m_frame = {};
    aeron_header_t m_aeronHeader = {};
    int m_context = 0;
};

TEST_F(HeaderTest, shouldReadAllValuesFromTheFrame)
{
    Header header{ &m_aeronHeader };

    verifyAllValues(header);
    EXPECT_EQ(&m_context, header.context());
    EXPECT_EQ(expectedPosition(), header.position());
    EXPECT_EQ(&m_aeronHeader, header.hdr());
}

TEST_F(HeaderTest, shouldReadTheSameValuesWhicheverFieldIsReadFirst)
{
    Header reservedValueFirst{ &m_aeronHeader };
    EXPECT_EQ(RESERVED_VALUE, reservedValueFirst.reservedValue());
    verifyAllValues(reservedValueFirst);

    Header flagsFirst{ &m_aeronHeader };
    EXPECT_EQ(FrameDescriptor::UNFRAGMENTED, flagsFirst.flags());
    verifyAllValues(flagsFirst);

    Header positionFirst{ &m_aeronHeader };
    EXPECT_EQ(expectedPosition(), positionFirst.position());
    verifyAllValues(positionFirst);
}

TEST_F(HeaderTest, shouldReadConsistentValuesWhenReadRepeatedly)
{
    Header header{ &m_aeronHeader };

    for (int i = 0; i < 3; i++)
    {
        verifyAllValues(header);
    }
}

TEST_F(HeaderTest, shouldReadValuesThroughAConstReference)
{
    Header header{ &m_aeronHeader };
    const Header &constHeader = header;

    verifyAllValues(constHeader);
    EXPECT_EQ(&m_context, constHeader.context());
    EXPECT_EQ(expectedPosition(), constHeader.position());
}

TEST_F(HeaderTest, shouldReadPositionAndContextWithoutReadingTheFrameValues)
{
    Header header{ &m_aeronHeader };

    EXPECT_EQ(&m_context, header.context());
    EXPECT_EQ(expectedPosition(), header.position());

    verifyAllValues(header);
}

TEST_F(HeaderTest, shouldComputePositionForAnAssembledFragmentedMessage)
{
    const std::int32_t fragmentedFrameLength = 3 * TERM_LENGTH / 8;
    m_aeronHeader.fragmented_frame_length = fragmentedFrameLength;

    Header header{ &m_aeronHeader };

    EXPECT_EQ(TERM_OFFSET, header.termOffset());
    EXPECT_EQ(
        LogBufferDescriptor::computePosition(
            ACTIVE_TERM_ID,
            BitUtil::align(TERM_OFFSET + fragmentedFrameLength, FrameDescriptor::FRAME_ALIGNMENT),
            POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID),
        header.position());
}

TEST_F(HeaderTest, shouldRejectANullHeader)
{
    ASSERT_THROW(
        {
            Header header{ nullptr };
            (void)header;
        },
        util::IllegalArgumentException);
}
