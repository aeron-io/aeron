//
// Created by mike on 14/03/24.
//

#ifndef AERON_CONTROLLEDFRAGMENTASSEMBLERTESTFIXTURE_H
#define AERON_CONTROLLEDFRAGMENTASSEMBLERTESTFIXTURE_H

#include <functional>
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "gmock/gmock-matchers.h"
#include "concurrent/logbuffer/LogBufferDescriptor.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/AtomicBuffer.h"
#include "ControlledFragmentAssembler.h"

using namespace aeron;
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;

static const std::int32_t STREAM_ID = 10;
static const std::int32_t SESSION_ID = 200;
static const std::int32_t TERM_OFFSET = 1024;
static const std::int32_t TERM_LENGTH = LogBufferDescriptor::TERM_MIN_LENGTH;
static const std::int32_t INITIAL_TERM_ID = -1234;
static const std::int32_t ACTIVE_TERM_ID = INITIAL_TERM_ID + 5;
static const int POSITION_BITS_TO_SHIFT = BitUtil::numberOfTrailingZeroes(TERM_LENGTH);
static const util::index_t MTU_LENGTH = 128;

typedef std::array<std::uint8_t, TERM_LENGTH> fragment_buffer_t;

class ControlledFragmentAssemblerParameterisedTest : public testing::TestWithParam<std::tuple<
    std::function<void(std::uint8_t *, size_t, Header**)>,
    std::function<void(Header*)>,
    std::function<void(Header&, std::int32_t, std::uint8_t, std::int32_t)>
    >>
{
public:
    void SetUp() override
    {
        std::get<0>(GetParam())(&headerBuffer[0], DataFrameHeader::LENGTH, &m_header);
    }

    void TearDown() override
    {
        std::get<1>(GetParam())(m_header);
    }

    void fillFrame(
        std::int32_t termOffset,
        std::uint8_t flags,
        std::int32_t offset,
        std::int32_t fragmentLength,
        std::uint8_t initialPayloadValue)
    {
        std::get<2>(GetParam())(*m_header, termOffset, flags, fragmentLength);

        std::uint8_t value = initialPayloadValue;
        for (int i = 0; i < fragmentLength; i++)
        {
            m_buffer[i + offset] = value++;
        }
    }

    void verifyPayload(AtomicBuffer &buffer, util::index_t offset, util::index_t length)
    {
        std::uint8_t *ptr = buffer.buffer() + offset;

        for (int i = 0; i < length; i++)
        {
            ASSERT_EQ(*(ptr + i), i % 256);
        }
    }

protected:
    Header *m_header = nullptr;
    fragment_buffer_t m_buffer = {};

private:
    std::uint8_t headerBuffer[DataFrameHeader::LENGTH] = {};
};

TEST_P(ControlledFragmentAssemblerParameterisedTest, shouldPassThroughUnfragmentedMessage)
{
    std::int32_t fragmentLength = 158;
    fillFrame(TERM_OFFSET, FrameDescriptor::UNFRAGMENTED, 0, fragmentLength, 0);
    bool isCalled = false;
    auto handler =
        [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            isCalled = true;
            EXPECT_EQ(offset, 0);
            EXPECT_EQ(length, fragmentLength);
            EXPECT_NE(nullptr, header.context());
            EXPECT_EQ(m_header->context(), header.context());
            EXPECT_EQ(header.positionBitsToShift(), POSITION_BITS_TO_SHIFT);
            EXPECT_EQ(header.initialTermId(), INITIAL_TERM_ID);
            EXPECT_EQ(header.sessionId(), SESSION_ID);
            EXPECT_EQ(header.streamId(), STREAM_ID);
            EXPECT_EQ(header.termId(), ACTIVE_TERM_ID);
            EXPECT_EQ(header.termOffset(), TERM_OFFSET);
            EXPECT_EQ(header.frameLength(), DataFrameHeader::LENGTH + fragmentLength);
            EXPECT_EQ(header.flags(), FrameDescriptor::UNFRAGMENTED);
            EXPECT_EQ(
                header.position(),
                LogBufferDescriptor::computePosition(
                    ACTIVE_TERM_ID,
                    BitUtil::align(header.termOffset() + header.frameLength(), FrameDescriptor::FRAME_ALIGNMENT),
                    POSITION_BITS_TO_SHIFT,
                    INITIAL_TERM_ID));
            verifyPayload(buffer, offset, length);

            return ControlledPollAction::CONTINUE;
        };

    ControlledFragmentAssembler assembler(handler);
    AtomicBuffer buf{m_buffer};
    assembler.handler()(buf, 0, fragmentLength, *m_header);
    EXPECT_TRUE(isCalled);
}

TEST_P(ControlledFragmentAssemblerParameterisedTest, shouldReassembleFromTwoFragments)
{
    util::index_t fragmentLength = MTU_LENGTH - DataFrameHeader::LENGTH;
    bool isCalled = false;
    auto handler =
        [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            isCalled = true;
            EXPECT_EQ(offset, 0);
            EXPECT_EQ(length, fragmentLength * 2);
            EXPECT_NE(nullptr, header.context());
            EXPECT_EQ(m_header->context(), header.context());
            EXPECT_EQ(header.sessionId(), SESSION_ID);
            EXPECT_EQ(header.streamId(), STREAM_ID);
            EXPECT_EQ(header.termId(), ACTIVE_TERM_ID);
            EXPECT_EQ(header.initialTermId(), INITIAL_TERM_ID);
            EXPECT_EQ(header.termOffset(), TERM_OFFSET);
            EXPECT_EQ(header.frameLength(), DataFrameHeader::LENGTH + (2 * fragmentLength));
            EXPECT_EQ(header.flags(), FrameDescriptor::BEGIN_FRAG | FrameDescriptor::END_FRAG);
            EXPECT_EQ(
                header.position(),
                LogBufferDescriptor::computePosition(
                    ACTIVE_TERM_ID,
                    header.termOffset() + LogBufferDescriptor::computeFragmentedFrameLength(2 * fragmentLength, fragmentLength),
                    POSITION_BITS_TO_SHIFT,
                    INITIAL_TERM_ID));
            verifyPayload(buffer, offset, length);

            return ControlledPollAction::CONTINUE;
        };

    ControlledFragmentAssembler assembler(handler);
    AtomicBuffer buf{m_buffer};

    fillFrame(TERM_OFFSET, FrameDescriptor::BEGIN_FRAG, 0, fragmentLength, 0);
    assembler.handler()(buf, 0, fragmentLength, *m_header);
    ASSERT_FALSE(isCalled);

    fillFrame(TERM_OFFSET + MTU_LENGTH, FrameDescriptor::END_FRAG, MTU_LENGTH, fragmentLength, fragmentLength % 256);
    assembler.handler()(buf, MTU_LENGTH, fragmentLength, *m_header);
    ASSERT_TRUE(isCalled);
}

TEST_P(ControlledFragmentAssemblerParameterisedTest, shouldReassembleFromThreeFragments)
{
    util::index_t fragmentLength = MTU_LENGTH - DataFrameHeader::LENGTH;
    bool isCalled = false;
    auto handler =
        [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            isCalled = true;
            EXPECT_EQ(offset, 0);
            EXPECT_EQ(length, fragmentLength * 3);
            EXPECT_EQ(header.positionBitsToShift(), POSITION_BITS_TO_SHIFT);
            EXPECT_EQ(header.initialTermId(), INITIAL_TERM_ID);
            EXPECT_EQ(header.sessionId(), SESSION_ID);
            EXPECT_EQ(header.streamId(), STREAM_ID);
            EXPECT_EQ(header.termId(), ACTIVE_TERM_ID);
            EXPECT_EQ(header.initialTermId(), INITIAL_TERM_ID);
            EXPECT_EQ(header.termOffset(), TERM_OFFSET);
            EXPECT_EQ(header.frameLength(), DataFrameHeader::LENGTH + (3 * fragmentLength));
            EXPECT_EQ(header.flags(), FrameDescriptor::BEGIN_FRAG | FrameDescriptor::END_FRAG);
            EXPECT_EQ(
                header.position(),
                LogBufferDescriptor::computePosition(
                    ACTIVE_TERM_ID,
                    header.termOffset() + LogBufferDescriptor::computeFragmentedFrameLength(3 * fragmentLength, fragmentLength),
                    POSITION_BITS_TO_SHIFT,
                    INITIAL_TERM_ID));
            verifyPayload(buffer, offset, length);

            return ControlledPollAction::CONTINUE;
        };

    ControlledFragmentAssembler assembler(handler);
    AtomicBuffer buf{m_buffer};

    fillFrame(TERM_OFFSET, FrameDescriptor::BEGIN_FRAG, 0, fragmentLength, 0);
    assembler.handler()(buf, 0, fragmentLength, *m_header);
    ASSERT_FALSE(isCalled);

    fillFrame(TERM_OFFSET + MTU_LENGTH, 0, MTU_LENGTH, fragmentLength, fragmentLength % 256);
    assembler.handler()(buf, MTU_LENGTH, fragmentLength, *m_header);
    ASSERT_FALSE(isCalled);

    fillFrame(TERM_OFFSET + (2 * MTU_LENGTH), FrameDescriptor::END_FRAG, MTU_LENGTH * 2, fragmentLength, (fragmentLength * 2) % 256);
    assembler.handler()(buf, (MTU_LENGTH * 2), fragmentLength, *m_header);
    ASSERT_TRUE(isCalled);
}

TEST_P(ControlledFragmentAssemblerParameterisedTest, shouldNotReassembleIfEndFirstFragment)
{
    util::index_t fragmentLength = MTU_LENGTH - DataFrameHeader::LENGTH;
    bool isCalled = false;
    auto handler =
        [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            isCalled = true;
            return ControlledPollAction::CONTINUE;
        };

    ControlledFragmentAssembler assembler(handler);
    AtomicBuffer buf{m_buffer};

    fillFrame(TERM_OFFSET, FrameDescriptor::END_FRAG, MTU_LENGTH, fragmentLength, fragmentLength % 256);
    assembler.handler()(buf, MTU_LENGTH + DataFrameHeader::LENGTH, fragmentLength, *m_header);
    ASSERT_FALSE(isCalled);
}

TEST_P(ControlledFragmentAssemblerParameterisedTest, shouldNotReassembleIfMissingBegin)
{
    util::index_t fragmentLength = MTU_LENGTH - DataFrameHeader::LENGTH;
    bool isCalled = false;
    auto handler =
        [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            isCalled = true;
            return ControlledPollAction::CONTINUE;
        };

    ControlledFragmentAssembler assembler(handler);
    AtomicBuffer buf{m_buffer};

    fillFrame(TERM_OFFSET, 0, MTU_LENGTH, fragmentLength, fragmentLength % 256);
    assembler.handler()(buf, MTU_LENGTH, fragmentLength, *m_header);
    ASSERT_FALSE(isCalled);

    fillFrame(TERM_OFFSET + MTU_LENGTH, FrameDescriptor::END_FRAG, MTU_LENGTH * 2, fragmentLength, (fragmentLength * 2) % 256);
    assembler.handler()(buf, (MTU_LENGTH * 2), fragmentLength, *m_header);
    ASSERT_FALSE(isCalled);
}

TEST_P(ControlledFragmentAssemblerParameterisedTest, shouldReassembleTwoMessagesFromFourFrames)
{
    util::index_t termOffset = 0;
    util::index_t fragmentLength = MTU_LENGTH - DataFrameHeader::LENGTH;
    bool isCalled = false;
    auto handler =
        [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            isCalled = true;
            EXPECT_EQ(offset, 0);
            EXPECT_EQ(length, fragmentLength * 2);
            EXPECT_EQ(header.sessionId(), SESSION_ID);
            EXPECT_EQ(header.streamId(), STREAM_ID);
            EXPECT_EQ(header.termId(), ACTIVE_TERM_ID);
            EXPECT_EQ(header.initialTermId(), INITIAL_TERM_ID);
            EXPECT_EQ(header.frameLength(), DataFrameHeader::LENGTH + (2 * fragmentLength));
            EXPECT_EQ(header.flags(), FrameDescriptor::BEGIN_FRAG | FrameDescriptor::END_FRAG);
            EXPECT_EQ(
                header.position(),
                LogBufferDescriptor::computePosition(
                    ACTIVE_TERM_ID,
                    header.termOffset() + LogBufferDescriptor::computeFragmentedFrameLength(2 * fragmentLength, fragmentLength),
                    POSITION_BITS_TO_SHIFT,
                    INITIAL_TERM_ID));
            return ControlledPollAction::CONTINUE;
        };

    ControlledFragmentAssembler assembler(handler);
    AtomicBuffer buf{m_buffer};

    termOffset = TERM_OFFSET;
    fillFrame(termOffset, FrameDescriptor::BEGIN_FRAG, termOffset, fragmentLength, 0);
    assembler.handler()(buf, 0, fragmentLength, *m_header);
    ASSERT_FALSE(isCalled);

    termOffset += MTU_LENGTH;
    fillFrame(termOffset, FrameDescriptor::END_FRAG, termOffset, fragmentLength, 1);
    assembler.handler()(buf, termOffset, fragmentLength, *m_header);
    ASSERT_TRUE(isCalled);

    isCalled = false;
    termOffset += MTU_LENGTH;
    fillFrame(termOffset, FrameDescriptor::BEGIN_FRAG, termOffset, fragmentLength, 2);
    assembler.handler()(buf, termOffset, fragmentLength, *m_header);
    ASSERT_FALSE(isCalled);

    termOffset += MTU_LENGTH;
    fillFrame(termOffset, FrameDescriptor::END_FRAG, termOffset, fragmentLength, 2);
    assembler.handler()(buf, termOffset, fragmentLength, *m_header);
    ASSERT_TRUE(isCalled);
}

#endif //AERON_CONTROLLEDFRAGMENTASSEMBLERTESTFIXTURE_H

TEST_P(ControlledFragmentAssemblerParameterisedTest, shouldDeleteSessionBuffer)
{
    util::index_t fragmentLength = MTU_LENGTH - DataFrameHeader::LENGTH;
    bool isCalled = false;
    auto handler =
        [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            isCalled = true;
            return ControlledPollAction::CONTINUE;
        };

    ControlledFragmentAssembler assembler(handler);
    AtomicBuffer buf{m_buffer};

    EXPECT_FALSE(assembler.deleteSessionBuffer(SESSION_ID));

    fillFrame(TERM_OFFSET, FrameDescriptor::BEGIN_FRAG, 0, fragmentLength, 0);
    assembler.handler()(buf, 0, fragmentLength, *m_header);

    fillFrame(TERM_OFFSET + MTU_LENGTH, FrameDescriptor::END_FRAG, MTU_LENGTH, fragmentLength, fragmentLength % 256);
    assembler.handler()(buf, MTU_LENGTH, fragmentLength, *m_header);
    ASSERT_TRUE(isCalled);

    EXPECT_TRUE(assembler.deleteSessionBuffer(SESSION_ID));
    EXPECT_FALSE(assembler.deleteSessionBuffer(SESSION_ID));
}
