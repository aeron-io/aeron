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
#include "aeronc.h"
#include "aeron_image.h"
}

#define STREAM_ID (-8)
#define SESSION_ID (43)
#define TERM_LENGTH (AERON_LOGBUFFER_TERM_MIN_LENGTH)
#define INITIAL_TERM_ID (178343)
#define ACTIVE_TERM_ID  (INITIAL_TERM_ID + 5)
#define POSITION_BITS_TO_SHIFT (aeron_number_of_trailing_zeroes(TERM_LENGTH))
#define MTU_LENGTH (2048)

typedef std::array<uint8_t, TERM_LENGTH> fragment_buffer_t;

class ControlledFragmentAssemblerTest : public testing::Test
{
public:
    ControlledFragmentAssemblerTest()
    {
        m_fragment.fill(0);
        m_header.frame = (aeron_data_header_t *)m_fragment.data();
        m_header.context = (void*)"controlled fragment assembler test";

        if (aeron_controlled_fragment_assembler_create(&m_assembler, fragment_handler, this) < 0)
        {
            throw std::runtime_error("could not create aeron_fragment_assembler_create: " + std::string(aeron_errmsg()));
        }
    }

    ~ControlledFragmentAssemblerTest() override
    {
        aeron_controlled_fragment_assembler_delete(m_assembler);
    }

    void fillFrame(uint8_t flags, int32_t offset, size_t length, uint8_t initialPayloadValue)
    {
        fillFrame(
            AERON_FRAME_HEADER_VERSION,
            flags,
            AERON_HDR_TYPE_DATA,
            offset,
            SESSION_ID,
            STREAM_ID,
            ACTIVE_TERM_ID,
            0,
            length,
            initialPayloadValue);
    }

    void fillFrame(
        int8_t version,
        uint8_t flags,
        int16_t type,
        int32_t termOffset,
        int32_t sessionId,
        int32_t streamId,
        int32_t termId,
        int64_t reservedValue,
        size_t length,
        uint8_t initialPayloadValue)
    {
        auto frame = (aeron_data_header_t *)m_fragment.data();

        frame->frame_header.frame_length = (int32_t)(AERON_DATA_HEADER_LENGTH + length);
        frame->frame_header.version = version;
        frame->frame_header.flags = flags;
        frame->frame_header.type = type;
        frame->term_offset = termOffset;
        frame->session_id = sessionId;
        frame->stream_id = streamId;
        frame->term_id = termId;
        frame->reserved_value = reservedValue;

        uint8_t value = initialPayloadValue;
        for (size_t i = 0; i < length; i++)
        {
            m_fragment[i + AERON_DATA_HEADER_LENGTH] = value++;
        }
    }

    static void verifyPayload(const uint8_t *buffer, size_t length)
    {
        for (size_t i = 0; i < length; i++)
        {
            ASSERT_EQ(*(buffer + i), i % 256);
        }
    }

    static aeron_controlled_fragment_handler_action_t fragment_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        auto test = reinterpret_cast<ControlledFragmentAssemblerTest *>(clientd);

        if (test->m_handler)
        {
            return test->m_handler(buffer, length, header);
        }
        return AERON_ACTION_BREAK;
    }

    template<typename F>
    aeron_controlled_fragment_handler_action_t handle_fragment(F &&handler, size_t length)
    {
        m_handler = handler;
        uint8_t *buffer = m_fragment.data() + AERON_DATA_HEADER_LENGTH;
        return ::aeron_controlled_fragment_assembler_handler(m_assembler, buffer, length, &m_header);
    }

protected:
    AERON_DECL_ALIGNED(fragment_buffer_t m_fragment, 16) = {};
    aeron_header_t m_header = {};
    std::function<aeron_controlled_fragment_handler_action_t(const uint8_t *, size_t, aeron_header_t *)> m_handler = nullptr;
    aeron_controlled_fragment_assembler_t *m_assembler = nullptr;
};

TEST_F(ControlledFragmentAssemblerTest, shouldPassThroughUnfragmentedMessage)
{
    size_t fragmentLength = 158;
    fillFrame(AERON_DATA_HEADER_UNFRAGMENTED, 0, fragmentLength, 0);
    bool isCalled = false;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        isCalled = true;
        EXPECT_EQ(length, fragmentLength);
        EXPECT_NE(nullptr, header->context);
        EXPECT_EQ(m_header.context, header->context);
        aeron_header_values_t header_values;
        EXPECT_EQ(0, aeron_header_values(header, &header_values));
        EXPECT_EQ(SESSION_ID, header_values.frame.session_id);
        verifyPayload(buffer, length);
        return AERON_ACTION_CONTINUE;
    };

    EXPECT_EQ(AERON_ACTION_CONTINUE, handle_fragment(handler, fragmentLength));
    EXPECT_TRUE(isCalled);
}

TEST_F(ControlledFragmentAssemblerTest, shouldReassembleFromTwoFragments)
{
    size_t fragmentLength = MTU_LENGTH - AERON_DATA_HEADER_LENGTH;
    int32_t initialTermId = 420;
    size_t positionBitsToShift = 20;
    int8_t version = 42;
    int16_t type = AERON_HDR_TYPE_ERR;
    int32_t sessionId = 18;
    int32_t streamId = 31;
    int32_t termId = initialTermId + 5;
    int64_t reservedValue = -47923742793L;
    int32_t termOffset = 0;

    const int64_t startPosition = ::aeron_logbuffer_compute_position(
        termId, termOffset, positionBitsToShift, initialTermId);

    bool isCalled = false;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        const int64_t expectedPosition =
            startPosition + static_cast<int64_t>(::aeron_logbuffer_compute_fragmented_length(length, fragmentLength));

        isCalled = true;
        EXPECT_EQ(length, fragmentLength * 2);
        EXPECT_NE(nullptr, header->context);
        EXPECT_EQ(m_header.context, header->context);
        aeron_header_values_t header_values;
        EXPECT_EQ(0, aeron_header_values(header, &header_values));
        EXPECT_EQ(initialTermId, header_values.initial_term_id);
        EXPECT_EQ(positionBitsToShift, header_values.position_bits_to_shift);
        EXPECT_EQ(AERON_DATA_HEADER_LENGTH + length, header_values.frame.frame_length);
        EXPECT_EQ((uint8_t)(AERON_DATA_HEADER_END_FLAG | AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_EOS_FLAG), header_values.frame.flags);
        EXPECT_EQ(version, header_values.frame.version);
        EXPECT_EQ(type, header_values.frame.type);
        EXPECT_EQ(0, header_values.frame.term_offset);
        EXPECT_EQ(sessionId, header_values.frame.session_id);
        EXPECT_EQ(streamId, header_values.frame.stream_id);
        EXPECT_EQ(termId, header_values.frame.term_id);
        EXPECT_EQ(reservedValue, header_values.frame.reserved_value);
        EXPECT_EQ(expectedPosition, aeron_header_position(header));
        verifyPayload(buffer, length);
        return AERON_ACTION_CONTINUE;
    };

    m_header.initial_term_id = initialTermId;
    m_header.position_bits_to_shift = positionBitsToShift;
    fillFrame(
        version,
        (uint8_t)(AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_EOS_FLAG),
        type,
        termOffset,
        sessionId,
        streamId,
        termId,
        reservedValue,
        fragmentLength,
        0);
    EXPECT_EQ(AERON_ACTION_CONTINUE, handle_fragment(handler, fragmentLength));
    EXPECT_FALSE(isCalled);

    termOffset += MTU_LENGTH;
    m_header.initial_term_id = -1;
    m_header.position_bits_to_shift = -7;
    fillFrame(
        AERON_FRAME_HEADER_VERSION,
        AERON_DATA_HEADER_END_FLAG,
        AERON_HDR_TYPE_EXT,
        termOffset,
        sessionId,
        -13,
        -190,
        63456385384L,
        fragmentLength,
        fragmentLength % 256);
    EXPECT_EQ(AERON_ACTION_CONTINUE, handle_fragment(handler, fragmentLength));
    EXPECT_TRUE(isCalled);
}

TEST_F(ControlledFragmentAssemblerTest, shouldReassembleFromThreeFragments)
{
    size_t fragmentLength = MTU_LENGTH - AERON_DATA_HEADER_LENGTH;
    size_t lastFragmentLength = 111;
    int32_t initialTermId = 420;
    size_t positionBitsToShift = 20;
    int8_t version = 3;
    int16_t type = AERON_HDR_TYPE_RTTM;
    int32_t streamId = 106;
    int32_t termId = initialTermId + 5;
    int64_t reservedValue = 5734957345793759L;
    int32_t termOffset = 0;

    const int64_t startPosition = ::aeron_logbuffer_compute_position(
        termId, termOffset, positionBitsToShift, initialTermId);

    bool isCalled = false;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        const int64_t expectedPosition =
            startPosition + static_cast<int64_t>(::aeron_logbuffer_compute_fragmented_length(length, fragmentLength));

        isCalled = true;
        EXPECT_EQ(length, fragmentLength * 2 + lastFragmentLength);
        EXPECT_NE(nullptr, header->context);
        EXPECT_EQ(m_header.context, header->context);
        aeron_header_values_t header_values;
        EXPECT_EQ(0, aeron_header_values(header, &header_values));
        EXPECT_EQ(AERON_DATA_HEADER_LENGTH + length, header_values.frame.frame_length);
        EXPECT_EQ(0xFF, header_values.frame.flags);
        EXPECT_EQ(version, header_values.frame.version);
        EXPECT_EQ(type, header_values.frame.type);
        EXPECT_EQ(0, header_values.frame.term_offset);
        EXPECT_EQ(SESSION_ID, header_values.frame.session_id);
        EXPECT_EQ(streamId, header_values.frame.stream_id);
        EXPECT_EQ(termId, header_values.frame.term_id);
        EXPECT_EQ(reservedValue, header_values.frame.reserved_value);
        EXPECT_EQ(expectedPosition, aeron_header_position(header));
        verifyPayload(buffer, length);
        return AERON_ACTION_CONTINUE;
    };

    m_header.initial_term_id = initialTermId;
    m_header.position_bits_to_shift = positionBitsToShift;
    fillFrame(
        version,
        (uint8_t)(AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_EOS_FLAG),
        type,
        termOffset,
        SESSION_ID,
        streamId,
        termId,
        reservedValue,
        fragmentLength,
        0);
    EXPECT_EQ(AERON_ACTION_CONTINUE, handle_fragment(handler, fragmentLength));
    EXPECT_FALSE(isCalled);

    termOffset += MTU_LENGTH;
    fillFrame(0xE, termOffset, fragmentLength, fragmentLength % 256);
    EXPECT_EQ(AERON_ACTION_CONTINUE, handle_fragment(handler, fragmentLength));
    EXPECT_FALSE(isCalled);

    termOffset += MTU_LENGTH;
    fillFrame(0x7F, termOffset, lastFragmentLength, (fragmentLength * 2) % 256);
    handle_fragment(handler, lastFragmentLength);
    EXPECT_TRUE(isCalled);
}

TEST_F(ControlledFragmentAssemblerTest, shouldNotReassembleIfEndFirstFragment)
{
    size_t fragmentLength = MTU_LENGTH - AERON_DATA_HEADER_LENGTH;
    bool isCalled = false;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        isCalled = true;
        return AERON_ACTION_CONTINUE;
    };

    fillFrame(AERON_DATA_HEADER_END_FLAG, 0, fragmentLength, 0);
    EXPECT_EQ(AERON_ACTION_CONTINUE, handle_fragment(handler, fragmentLength));
    EXPECT_FALSE(isCalled);
}

TEST_F(ControlledFragmentAssemblerTest, shouldNotReassembleIfMissingBegin)
{
    size_t fragmentLength = MTU_LENGTH - AERON_DATA_HEADER_LENGTH;
    int32_t termOffset = 0;
    bool isCalled = false;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        isCalled = true;
        return AERON_ACTION_CONTINUE;
    };

    fillFrame(0, termOffset, fragmentLength, 0);
    EXPECT_EQ(AERON_ACTION_CONTINUE, handle_fragment(handler, fragmentLength));
    EXPECT_FALSE(isCalled);

    termOffset += MTU_LENGTH;
    fillFrame(AERON_DATA_HEADER_END_FLAG, termOffset, fragmentLength, fragmentLength % 256);
    EXPECT_EQ(AERON_ACTION_CONTINUE, handle_fragment(handler, fragmentLength));
    EXPECT_FALSE(isCalled);
}

TEST_F(ControlledFragmentAssemblerTest, shouldReassembleTwoMessagesFromFourFrames)
{
    size_t fragmentLength = MTU_LENGTH - AERON_DATA_HEADER_LENGTH;
    int32_t termOffset = 0, messageBeginOffset = 0;
    bool isCalled = false;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        isCalled = true;
        EXPECT_EQ(length, fragmentLength * 2);
        aeron_header_values_t header_values;
        EXPECT_EQ(0, aeron_header_values(header, &header_values));
        EXPECT_EQ(SESSION_ID, header_values.frame.session_id);
        EXPECT_EQ(messageBeginOffset, header_values.frame.term_offset);
        return AERON_ACTION_CONTINUE;
    };

    fillFrame(AERON_DATA_HEADER_BEGIN_FLAG, termOffset, fragmentLength, 0);
    EXPECT_EQ(AERON_ACTION_CONTINUE, handle_fragment(handler, fragmentLength));
    EXPECT_FALSE(isCalled);

    termOffset += MTU_LENGTH;
    fillFrame(AERON_DATA_HEADER_END_FLAG, termOffset, fragmentLength, fragmentLength % 256);
    EXPECT_EQ(AERON_ACTION_CONTINUE, handle_fragment(handler, fragmentLength));
    EXPECT_TRUE(isCalled);

    isCalled = false;
    messageBeginOffset = 2 * MTU_LENGTH;

    termOffset += MTU_LENGTH;
    fillFrame(AERON_DATA_HEADER_BEGIN_FLAG, termOffset, fragmentLength, 0);
    EXPECT_EQ(AERON_ACTION_CONTINUE, handle_fragment(handler, fragmentLength));
    EXPECT_FALSE(isCalled);

    termOffset += MTU_LENGTH;
    fillFrame(AERON_DATA_HEADER_END_FLAG, termOffset, fragmentLength, fragmentLength % 256);
    EXPECT_EQ(AERON_ACTION_CONTINUE, handle_fragment(handler, fragmentLength));
    EXPECT_TRUE(isCalled);

    isCalled = false;
    EXPECT_EQ(AERON_ACTION_CONTINUE, handle_fragment(handler, fragmentLength));
    EXPECT_FALSE(isCalled);
}

TEST_F(ControlledFragmentAssemblerTest, shouldAbortReassembly)
{
    size_t fragmentLength = MTU_LENGTH - AERON_DATA_HEADER_LENGTH;
    int32_t termOffset = 0;
    bool isCalled = false;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        isCalled = true;
        EXPECT_EQ(length, fragmentLength * 2);
        aeron_header_values_t header_values;
        EXPECT_EQ(0, aeron_header_values(header, &header_values));
        EXPECT_EQ(SESSION_ID, header_values.frame.session_id);
        EXPECT_EQ(0, header_values.frame.term_offset);
        return AERON_ACTION_ABORT;
    };

    fillFrame(AERON_DATA_HEADER_BEGIN_FLAG, termOffset, fragmentLength, 0);
    EXPECT_EQ(AERON_ACTION_CONTINUE, handle_fragment(handler, fragmentLength));
    EXPECT_FALSE(isCalled);

    termOffset += MTU_LENGTH;
    fillFrame(AERON_DATA_HEADER_END_FLAG, termOffset, fragmentLength, fragmentLength % 256);
    EXPECT_EQ(AERON_ACTION_ABORT, handle_fragment(handler, fragmentLength));
    EXPECT_TRUE(isCalled);

    isCalled = false;
    EXPECT_EQ(AERON_ACTION_ABORT, handle_fragment(handler, fragmentLength));
    EXPECT_TRUE(isCalled);
}

TEST_F(ControlledFragmentAssemblerTest, testHeaderAfterAbortingFragmentedMessage)
{
    m_header.initial_term_id = INITIAL_TERM_ID;
    m_header.position_bits_to_shift = POSITION_BITS_TO_SHIFT;

    uint64_t callCount = 0;
    size_t fragmentLength = MTU_LENGTH - AERON_DATA_HEADER_LENGTH;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        callCount++;
        EXPECT_EQ(length, fragmentLength * 2);
        aeron_header_values_t header_values;
        EXPECT_EQ(0, aeron_header_values(header, &header_values));
        EXPECT_EQ(ACTIVE_TERM_ID, header_values.frame.term_id);
        EXPECT_EQ(0, header_values.frame.term_offset);
        EXPECT_EQ(INITIAL_TERM_ID, header->initial_term_id);
        EXPECT_EQ(POSITION_BITS_TO_SHIFT, header->position_bits_to_shift);
        EXPECT_EQ(AERON_DATA_HEADER_LENGTH + 2 * fragmentLength, header_values.frame.frame_length);
        EXPECT_EQ(((ACTIVE_TERM_ID - INITIAL_TERM_ID) * TERM_LENGTH) + 2 * MTU_LENGTH, aeron_header_position(header));
        EXPECT_EQ(SESSION_ID, header_values.frame.session_id);
        EXPECT_EQ(AERON_DATA_HEADER_UNFRAGMENTED, header_values.frame.flags & AERON_DATA_HEADER_UNFRAGMENTED);
        return AERON_ACTION_ABORT;
    };

    int32_t termOffset = 0;
    fillFrame(AERON_DATA_HEADER_BEGIN_FLAG, termOffset, fragmentLength, 0);
    EXPECT_EQ(AERON_ACTION_CONTINUE, handle_fragment(handler, fragmentLength));

    termOffset += MTU_LENGTH;
    fillFrame(AERON_DATA_HEADER_END_FLAG, termOffset, fragmentLength, fragmentLength % 256);
    EXPECT_EQ(AERON_ACTION_ABORT, handle_fragment(handler, fragmentLength));
    EXPECT_EQ(AERON_ACTION_ABORT, handle_fragment(handler, fragmentLength));

    EXPECT_EQ(2, callCount);
}
