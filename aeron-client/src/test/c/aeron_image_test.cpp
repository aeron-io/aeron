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

#include <exception>
#include <functional>
#include <string>
#include <limits>

#include <gtest/gtest.h>

#include "aeron_client_test_utils.h"

extern "C"
{
#include "aeron_subscription.h"
#include "aeron_image.h"
}

#define FILE_PAGE_SIZE (4 * 1024)

#define SUB_URI "aeron:udp?endpoint=localhost:24567"
#define STREAM_ID (101)
#define SESSION_ID (110)
#define REGISTRATION_ID (27)
#define SUBSCRIBER_POSITION_ID (49)

#define INITIAL_TERM_ID (1234)

using namespace aeron::test;

class ImageTest : public testing::Test
{
public:
    ImageTest() :
        m_subscription(createSubscription())
    {
    }

    ~ImageTest() override
    {
        if (nullptr != m_subscription)
        {
            aeron_subscription_delete(m_subscription);
        }

        if (!m_filename.empty())
        {
            aeron_log_buffer_delete(m_image->log_buffer);
            aeron_image_delete(m_image);

            ::unlink(m_filename.c_str());
        }
    }

    static aeron_subscription_t *createSubscription()
    {
        aeron_subscription_t *subscription = nullptr;

        if (aeron_subscription_create(
            &subscription,
            nullptr,
            ::strdup(SUB_URI),
            STREAM_ID,
            26,
            0,
            nullptr,
            nullptr,
            nullptr,
            nullptr,
            nullptr) < 0)
        {
            throw std::runtime_error("could not create subscription: " + std::string(aeron_errmsg()));
        }

        return subscription;
    }

    int64_t createImage()
    {
        aeron_image_t *image = nullptr;
        aeron_log_buffer_t *log_buffer = nullptr;
        std::string filename = tempFileName();

        createLogFile(filename);

        if (aeron_log_buffer_create(&log_buffer, filename.c_str(), m_correlationId, false) < 0)
        {
            throw std::runtime_error("could not create log_buffer: " + std::string(aeron_errmsg()));
        }

        if (aeron_image_create(
            &image,
            m_subscription,
            nullptr,
            log_buffer,
            SUBSCRIBER_POSITION_ID,
            &m_sub_pos,
            m_correlationId,
            (int32_t)m_correlationId,
            "none",
            strlen("none")) < 0)
        {
            throw std::runtime_error("could not create image: " + std::string(aeron_errmsg()));
        }

        auto *metadata = (aeron_logbuffer_metadata_t *)log_buffer->mapped_raw_log.log_meta_data.addr;

        m_image = image;
        m_filename = filename;

        m_term_length = metadata->term_length;
        m_initial_term_id = metadata->initial_term_id;
        m_position_bits_to_shift = (size_t)aeron_number_of_trailing_zeroes(metadata->term_length);

        return m_correlationId++;
    }

    void appendMessage(int64_t position, size_t length)
    {
        auto *metadata = (aeron_logbuffer_metadata_t *)m_image->log_buffer->mapped_raw_log.log_meta_data.addr;
        const size_t index = aeron_logbuffer_index_by_position(position, m_position_bits_to_shift);
        aeron_mapped_buffer_t *term_buffer = &m_image->log_buffer->mapped_raw_log.term_buffers[index];
        uint8_t buffer[1024] = { 0 };
        int32_t term_id = aeron_logbuffer_compute_term_id_from_position(
            position, m_position_bits_to_shift, m_initial_term_id);
        int32_t tail_offset = (int32_t)position & (m_term_length - 1);

        metadata->term_tail_counters[index] = static_cast<int64_t>(term_id) << 32 | tail_offset;

        const size_t frame_length = length + AERON_DATA_HEADER_LENGTH;
        const auto aligned_frame_length = (int32_t)AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);

        int32_t resulting_offset = tail_offset + aligned_frame_length;
        metadata->term_tail_counters[index] = static_cast<int64_t>(term_id) << 32 | resulting_offset;

        auto term_length = static_cast<int32_t>(term_buffer->length);
        auto *header = (aeron_data_header_t *)(term_buffer->addr + tail_offset);

        if (resulting_offset > term_length)
        {
            if (tail_offset < term_length)
            {
                const int32_t padding_length = term_length - tail_offset;

                header->frame_header.version = AERON_FRAME_HEADER_VERSION;
                header->frame_header.flags = AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG;
                header->frame_header.type = AERON_HDR_TYPE_PAD;
                header->term_offset = tail_offset;
                header->session_id = SESSION_ID;
                header->stream_id = STREAM_ID;
                header->term_id = term_id;

                AERON_SET_RELEASE(header->frame_header.frame_length, padding_length);
            }
        }
        else
        {
            header->frame_header.version = AERON_FRAME_HEADER_VERSION;
            header->frame_header.flags = AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG;
            header->frame_header.type = AERON_HDR_TYPE_DATA;
            header->term_offset = tail_offset;
            header->session_id = SESSION_ID;
            header->stream_id = STREAM_ID;
            header->term_id = term_id;

            memcpy(term_buffer->addr + tail_offset + AERON_DATA_HEADER_LENGTH, buffer, length);

            AERON_SET_RELEASE(header->frame_header.frame_length, (int32_t)frame_length);
        }
    }

    static void fragment_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        auto image = reinterpret_cast<ImageTest *>(clientd);

        if (image->m_handler)
        {
            image->m_handler(buffer, length, header);
        }
    }

    static aeron_controlled_fragment_handler_action_t controlled_fragment_handler(
        void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        auto image = reinterpret_cast<ImageTest *>(clientd);

        if (image->m_controlled_handler)
        {
            return image->m_controlled_handler(buffer, length, header);
        }

        return AERON_ACTION_CONTINUE;
    }

    template<typename F>
    int imagePoll(F &&handler, size_t fragment_limit)
    {
        m_handler = handler;
        return aeron_image_poll(m_image, fragment_handler, this, fragment_limit);
    }

    template<typename F>
    int imageControlledPoll(F &&handler, size_t fragment_limit)
    {
        m_controlled_handler = handler;
        return aeron_image_controlled_poll(m_image, controlled_fragment_handler, this, fragment_limit);
    }

    template<typename F>
    int imageControlledPeek(F &&handler, int64_t initial_position, int64_t limit_position)
    {
        m_controlled_handler = handler;
        return aeron_image_controlled_peek(m_image, initial_position, controlled_fragment_handler, this, limit_position);
    }

    template<typename F>
    int imageBoundedPoll(F &&handler, int64_t max_position, size_t fragment_limit)
    {
        m_handler = handler;
        return aeron_image_bounded_poll(m_image, fragment_handler, this, max_position, fragment_limit);
    }

    template<typename F>
    int imageBoundedControlledPoll(F &&handler, int64_t max_position, size_t fragment_limit)
    {
        m_controlled_handler = handler;
        return aeron_image_bounded_controlled_poll(
            m_image, controlled_fragment_handler, this, max_position, fragment_limit);
    }

    const uint8_t *termBuffer(size_t index)
    {
        return m_image->log_buffer->mapped_raw_log.term_buffers[index].addr;
    }

protected:
    int64_t m_correlationId = 0;
    int64_t m_sub_pos = 0;

    int32_t m_term_length = 0;
    int32_t m_initial_term_id = 0;

    size_t m_position_bits_to_shift = 0;

    std::function<void(const uint8_t *, size_t, aeron_header_t *)> m_handler = nullptr;
    std::function<aeron_controlled_fragment_handler_action_t(const uint8_t *, size_t, aeron_header_t *)>
        m_controlled_handler = nullptr;

    aeron_subscription_t *m_subscription = nullptr;
    aeron_image_t *m_image = nullptr;
    std::string m_filename;
};

TEST_F(ImageTest, shouldReadFirstMessage)
{
    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    createImage();

    appendMessage(m_sub_pos, messageLength);

    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        EXPECT_EQ(buffer, termBuffer(0) + m_sub_pos + AERON_DATA_HEADER_LENGTH);
        EXPECT_EQ(length, messageLength);
        EXPECT_EQ(header->frame->frame_header.type, AERON_HDR_TYPE_DATA);
    };

    EXPECT_EQ(imagePoll(handler, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_sub_pos, alignedMessageLength);
}

TEST_F(ImageTest, shouldNotReadPastTail)
{
    createImage();

    auto handler = [&](const uint8_t *, size_t length, aeron_header_t *header)
    {
        FAIL() << "should not be called";
    };

    EXPECT_EQ(imagePoll(handler, std::numeric_limits<size_t>::max()), 0);
    EXPECT_EQ(static_cast<size_t>(m_sub_pos), 0u);
}

TEST_F(ImageTest, shouldReadOneLimitedMessage)
{
    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    createImage();

    appendMessage(m_sub_pos, messageLength);
    appendMessage(m_sub_pos + alignedMessageLength, messageLength);

    auto null_handler = [&](const uint8_t *, size_t length, aeron_header_t *header) {};

    EXPECT_EQ(imagePoll(null_handler, 1), 1);
}

TEST_F(ImageTest, shouldReadMultipleMessages)
{
    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    createImage();

    appendMessage(m_sub_pos, messageLength);
    appendMessage(m_sub_pos + alignedMessageLength, messageLength);
    size_t handlerCallCount = 0;

    auto handler = [&](const uint8_t *, size_t length, aeron_header_t *header)
    {
        handlerCallCount++;
    };

    EXPECT_EQ(imagePoll(handler, std::numeric_limits<size_t>::max()), 2);
    EXPECT_EQ(handlerCallCount, 2u);
    EXPECT_EQ(m_sub_pos, alignedMessageLength * 2);
}

TEST_F(ImageTest, shouldStopReadingIfImageIsClosed)
{
    const size_t messageLength = 100;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    createImage();

    appendMessage(m_sub_pos, messageLength);
    appendMessage(m_sub_pos + alignedMessageLength, messageLength);
    size_t handlerCallCount = 0;

    auto handler = [&](const uint8_t *, size_t length, aeron_header_t *header)
    {
        handlerCallCount++;
        aeron_image_close(m_image);
    };

    EXPECT_EQ(imagePoll(handler, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(true, aeron_image_is_closed(m_image));
    EXPECT_EQ(handlerCallCount, 1u);
    // subscriber_position should NOT be advanced because the image was closed
    // during the handler — the !is_closed guard prevents the position write.
    EXPECT_EQ(m_sub_pos, 0);
}

TEST_F(ImageTest, shouldReadLastMessage)
{
    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    createImage();

    m_sub_pos = m_term_length - alignedMessageLength;

    appendMessage(m_sub_pos, messageLength);

    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        EXPECT_EQ(buffer, termBuffer(0) + m_sub_pos + AERON_DATA_HEADER_LENGTH);
        EXPECT_EQ(length, messageLength);
    };

    EXPECT_EQ(imagePoll(handler, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_sub_pos, m_term_length);
}

TEST_F(ImageTest, shouldNotReadLastMessageWhenPadding)
{
    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    createImage();

    m_sub_pos = m_term_length - alignedMessageLength;

    // this will append padding instead of the message as it will trip over the end.
    appendMessage(m_sub_pos, messageLength + AERON_DATA_HEADER_LENGTH);

    auto handler = [&](const uint8_t *, size_t length, aeron_header_t *header)
    {
        FAIL() << "should not be called";
    };

    EXPECT_EQ(imagePoll(handler, std::numeric_limits<size_t>::max()), 0);
    EXPECT_EQ(m_sub_pos, m_term_length);
}

TEST_F(ImageTest, shouldAllowValidPosition)
{
    createImage();

    int64_t expectedPosition = m_term_length - 32;

    m_sub_pos = expectedPosition;
    ASSERT_EQ(aeron_image_position(m_image), expectedPosition);

    ASSERT_EQ(aeron_image_set_position(m_image, m_term_length), 0);
    EXPECT_EQ(aeron_image_position(m_image), m_term_length);
}

TEST_F(ImageTest, shouldNotAdvancePastEndOfTerm)
{
    createImage();

    int64_t expectedPosition = m_term_length - 32;

    m_sub_pos = expectedPosition;
    ASSERT_EQ(aeron_image_position(m_image), expectedPosition);

    ASSERT_EQ(aeron_image_set_position(m_image, m_term_length + 32), -1);
    EXPECT_EQ(aeron_image_position(m_image), expectedPosition);
}

TEST_F(ImageTest, shouldReportCorrectPositionOnReception)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    appendMessage(m_sub_pos, messageLength);

    auto handler = [&](const uint8_t *, size_t length, aeron_header_t *header) {};

    EXPECT_EQ(imagePoll(handler, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_sub_pos, alignedMessageLength);
}

TEST_F(ImageTest, shouldReportCorrectPositionOnReceptionWithNonZeroPositionInInitialTermId)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int64_t initialTermOffset = 5 * alignedMessageLength;
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        INITIAL_TERM_ID, initialTermOffset, m_position_bits_to_shift, INITIAL_TERM_ID);

    m_sub_pos = initialPosition;

    appendMessage(m_sub_pos, messageLength);

    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        EXPECT_EQ(buffer, termBuffer(0) + initialTermOffset + AERON_DATA_HEADER_LENGTH);
        EXPECT_EQ(length, messageLength);
    };

    EXPECT_EQ(imagePoll(handler, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_sub_pos, initialPosition + alignedMessageLength);
}

TEST_F(ImageTest, shouldReportCorrectPositionOnReceptionWithNonZeroPositionInNonInitialTermId)
{
    createImage();

    const int32_t activeTermId = INITIAL_TERM_ID + 1;
    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int64_t initialTermOffset = 5 * alignedMessageLength;
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        activeTermId, initialTermOffset, m_position_bits_to_shift, INITIAL_TERM_ID);

    m_sub_pos = initialPosition;

    appendMessage(m_sub_pos, messageLength);

    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        EXPECT_EQ(buffer, termBuffer(1) + initialTermOffset + AERON_DATA_HEADER_LENGTH);
        EXPECT_EQ(length, messageLength);
    };

    EXPECT_EQ(imagePoll(handler, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_sub_pos, initialPosition + alignedMessageLength);
}

TEST_F(ImageTest, shouldPollNoFragmentsToControlledFragmentHandler)
{
    createImage();

    bool called = false;
    auto handler = [&](const uint8_t *, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        called = true;
        return AERON_ACTION_CONTINUE;
    };

    EXPECT_EQ(imageControlledPoll(handler, std::numeric_limits<size_t>::max()), 0);
    EXPECT_FALSE(called);
    EXPECT_EQ(static_cast<size_t>(m_sub_pos), 0u);
}

TEST_F(ImageTest, shouldPeekNoFragmentsToControlledFragmentHandler)
{
    createImage();

    bool called = false;
    auto handler = [&](const uint8_t *, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        called = true;
        return AERON_ACTION_CONTINUE;
    };

    EXPECT_EQ(imageControlledPeek(handler, 0, INT64_MAX), 0);
    EXPECT_FALSE(called);
    EXPECT_EQ(m_sub_pos, 0);
}

TEST_F(ImageTest, shouldPollOneFragmentToControlledFragmentHandlerOnContinue)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    appendMessage(m_sub_pos, messageLength);

    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        EXPECT_EQ(buffer, termBuffer(0) + m_sub_pos + AERON_DATA_HEADER_LENGTH);
        EXPECT_EQ(length, messageLength);
        EXPECT_EQ(header->frame->frame_header.type, AERON_HDR_TYPE_DATA);
        return AERON_ACTION_CONTINUE;
    };

    EXPECT_EQ(imageControlledPoll(handler, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_sub_pos, alignedMessageLength);
}

TEST_F(ImageTest, shouldPeekOneFragmentToControlledFragmentHandlerOnContinue)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        INITIAL_TERM_ID, 0, m_position_bits_to_shift, INITIAL_TERM_ID);
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    m_sub_pos = initialPosition;

    appendMessage(m_sub_pos, messageLength);

    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        EXPECT_EQ(buffer, termBuffer(0) + m_sub_pos + AERON_DATA_HEADER_LENGTH);
        EXPECT_EQ(length, messageLength);
        EXPECT_EQ(header->frame->frame_header.type, AERON_HDR_TYPE_DATA);
        return AERON_ACTION_CONTINUE;
    };

    EXPECT_EQ(imageControlledPeek(handler, initialPosition, INT64_MAX), initialPosition + alignedMessageLength);
    EXPECT_EQ(m_sub_pos, initialPosition);
}

TEST_F(ImageTest, shouldStopPeekOneFragmentToControlledFragmentHandlerIfImageIsClosed)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        INITIAL_TERM_ID, 0, m_position_bits_to_shift, INITIAL_TERM_ID);
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    m_sub_pos = initialPosition;

    appendMessage(initialPosition, messageLength);
    appendMessage(initialPosition + alignedMessageLength, messageLength);

    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        aeron_image_close(m_image);
        return AERON_ACTION_CONTINUE;
    };

    EXPECT_EQ(imageControlledPeek(handler, initialPosition, INT64_MAX), initialPosition + alignedMessageLength);
    EXPECT_EQ(true, aeron_image_is_closed(m_image));
    EXPECT_EQ(m_sub_pos, initialPosition);
}

TEST_F(ImageTest, shouldNotPollOneFragmentToControlledFragmentHandlerOnAbort)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        INITIAL_TERM_ID, 0, m_position_bits_to_shift, INITIAL_TERM_ID);

    m_sub_pos = initialPosition;

    appendMessage(m_sub_pos, messageLength);

    auto handler = [&](const uint8_t *, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        return AERON_ACTION_ABORT;
    };

    EXPECT_EQ(imageControlledPoll(handler, std::numeric_limits<size_t>::max()), 0);
    EXPECT_EQ(m_sub_pos, initialPosition);
}

TEST_F(ImageTest, shouldPollOneFragmentToControlledFragmentHandlerOnBreak)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    appendMessage(m_sub_pos, messageLength);
    appendMessage(m_sub_pos + alignedMessageLength, messageLength);
    aeron_image_constants_t image_constants;
    aeron_image_constants(m_image, &image_constants);

    EXPECT_EQ(image_constants.subscriber_position_id, SUBSCRIBER_POSITION_ID);

    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        aeron_header_values_t values;
        aeron_header_values(header, &values);

        EXPECT_EQ(buffer, termBuffer(0) + AERON_DATA_HEADER_LENGTH);
        EXPECT_EQ(length, messageLength);
        EXPECT_EQ(header->frame->frame_header.type, AERON_HDR_TYPE_DATA);
        EXPECT_EQ(values.frame.type, AERON_HDR_TYPE_DATA);
        EXPECT_EQ(image_constants.initial_term_id, values.initial_term_id);
        // memcpy to avoid ubsan complaint
        size_t position_bits_to_shift;
        memcpy(&position_bits_to_shift, &values.position_bits_to_shift, sizeof(position_bits_to_shift));
        EXPECT_EQ(image_constants.position_bits_to_shift, position_bits_to_shift);
        EXPECT_EQ(alignedMessageLength, aeron_header_position(header));

        return AERON_ACTION_BREAK;
    };

    EXPECT_EQ(imageControlledPoll(handler, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_sub_pos, alignedMessageLength);
}

TEST_F(ImageTest, shouldPollFragmentsToControlledFragmentHandlerOnCommit)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    size_t fragmentCount = 0;

    appendMessage(m_sub_pos, messageLength);
    appendMessage(m_sub_pos + alignedMessageLength, messageLength);

    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        fragmentCount++;

        if (1 == fragmentCount)
        {
            EXPECT_EQ(m_sub_pos, 0);

            EXPECT_EQ(buffer, termBuffer(0) + AERON_DATA_HEADER_LENGTH);
        }
        else if (2 == fragmentCount)
        {
            // testing current position here after first message commit
            EXPECT_EQ(m_sub_pos, alignedMessageLength);

            EXPECT_EQ(buffer, termBuffer(0) + alignedMessageLength + AERON_DATA_HEADER_LENGTH);
        }

        EXPECT_EQ(length, messageLength);
        return AERON_ACTION_COMMIT;
    };

    EXPECT_EQ(imageControlledPoll(handler, std::numeric_limits<size_t>::max()), 2);
    EXPECT_EQ(m_sub_pos, alignedMessageLength * 2);
}

TEST_F(ImageTest, shouldUpdatePositionToEndOfCommittedFragmentOnCommit)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        INITIAL_TERM_ID, 0, m_position_bits_to_shift, INITIAL_TERM_ID);
    size_t fragmentCount = 0;

    m_sub_pos = initialPosition;

    appendMessage(initialPosition, messageLength);
    appendMessage(initialPosition + alignedMessageLength, messageLength);
    appendMessage(initialPosition + (2 * alignedMessageLength), messageLength);

    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        fragmentCount++;

        EXPECT_EQ(length, messageLength);

        if (1 == fragmentCount)
        {
            EXPECT_EQ(m_sub_pos, initialPosition);

            EXPECT_EQ(buffer, termBuffer(0) + AERON_DATA_HEADER_LENGTH);
            return AERON_ACTION_CONTINUE;
        }
        else if (2 == fragmentCount)
        {
            // testing current position here after first message continue
            EXPECT_EQ(m_sub_pos, initialPosition);

            EXPECT_EQ(buffer, termBuffer(0) + alignedMessageLength + AERON_DATA_HEADER_LENGTH);
            return AERON_ACTION_COMMIT;
        }
        else if (3 == fragmentCount)
        {
            // testing current position here after second message commit
            EXPECT_EQ(m_sub_pos, initialPosition + (2 * alignedMessageLength));

            EXPECT_EQ(buffer, termBuffer(0) + (2 * alignedMessageLength) + AERON_DATA_HEADER_LENGTH);
            return AERON_ACTION_CONTINUE;
        }

        return AERON_ACTION_COMMIT;
    };

    EXPECT_EQ(imageControlledPoll(handler, std::numeric_limits<size_t>::max()), 3);
    EXPECT_EQ(m_sub_pos, initialPosition + (3 * alignedMessageLength));
}

TEST_F(ImageTest, shouldPollFragmentsToControlledFragmentHandlerOnContinue)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        INITIAL_TERM_ID, 0, m_position_bits_to_shift, INITIAL_TERM_ID);
    size_t fragmentCount = 0;

    m_sub_pos = initialPosition;

    appendMessage(initialPosition, messageLength);
    appendMessage(initialPosition + alignedMessageLength, messageLength);

    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        fragmentCount++;

        if (1 == fragmentCount)
        {
            EXPECT_EQ(m_sub_pos, initialPosition);

            EXPECT_EQ(buffer, termBuffer(0) + AERON_DATA_HEADER_LENGTH);
        }
        else if (2 == fragmentCount)
        {
            // testing current position here after first message continue
            EXPECT_EQ(m_sub_pos, initialPosition);

            EXPECT_EQ(buffer, termBuffer(0) + alignedMessageLength + AERON_DATA_HEADER_LENGTH);
        }

        EXPECT_EQ(length, messageLength);
        return AERON_ACTION_CONTINUE;
    };

    EXPECT_EQ(imageControlledPoll(handler, std::numeric_limits<size_t>::max()), 2);
    EXPECT_EQ(m_sub_pos, initialPosition + (2 * alignedMessageLength));
}

TEST_F(ImageTest, shouldStopPollFragmentsToControlledFragmentHandlerIfImageIsClosed)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        INITIAL_TERM_ID, 0, m_position_bits_to_shift, INITIAL_TERM_ID);
    size_t fragmentCount = 0;

    m_sub_pos = initialPosition;

    appendMessage(initialPosition, messageLength);
    appendMessage(initialPosition + alignedMessageLength, messageLength);

    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        fragmentCount++;
        aeron_image_close(m_image);
        return AERON_ACTION_CONTINUE;
    };

    EXPECT_EQ(imageControlledPoll(handler, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(true, aeron_image_is_closed(m_image));
    EXPECT_EQ(1, fragmentCount);
    // subscriber_position should NOT be advanced because the image was closed
    // during the handler
    EXPECT_EQ(m_sub_pos, initialPosition);
}

TEST_F(ImageTest, shouldPollNoFragmentsToBoundedControlledFragmentHandlerWithMaxPositionBeforeInitialPosition)
{
    createImage();

    const size_t messageLength = 120;
    const int32_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        INITIAL_TERM_ID, 0, m_position_bits_to_shift, INITIAL_TERM_ID);
    const int64_t maxPosition = initialPosition - (int64_t)AERON_DATA_HEADER_LENGTH;
    bool called = false;

    m_sub_pos = initialPosition;

    appendMessage(initialPosition, messageLength);
    appendMessage(initialPosition + alignedMessageLength, messageLength);

    auto handler = [&](const uint8_t *, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        called = true;
        return AERON_ACTION_CONTINUE;
    };

    EXPECT_FALSE(called);
    EXPECT_EQ(imageBoundedControlledPoll(handler, maxPosition, std::numeric_limits<size_t>::max()), 0);
    EXPECT_EQ(m_sub_pos, initialPosition);
}

TEST_F(ImageTest, shouldPollFragmentsToBoundedControlledFragmentHandlerWithInitialOffsetNotZero)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        INITIAL_TERM_ID, alignedMessageLength, m_position_bits_to_shift, INITIAL_TERM_ID);
    const int64_t maxPosition = initialPosition + alignedMessageLength;

    m_sub_pos = initialPosition;

    appendMessage(initialPosition, messageLength);
    appendMessage(initialPosition + alignedMessageLength, messageLength);

    auto handler = [&](const uint8_t *, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        return AERON_ACTION_CONTINUE;
    };

    EXPECT_EQ(imageBoundedControlledPoll(handler, maxPosition, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_sub_pos, maxPosition);
}

TEST_F(ImageTest, shouldPollFragmentsToBoundedControlledFragmentHandlerWithMaxPositionBeforeNextMessage)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        INITIAL_TERM_ID, 0, m_position_bits_to_shift, INITIAL_TERM_ID);
    const int64_t maxPosition = initialPosition + alignedMessageLength;

    m_sub_pos = initialPosition;

    appendMessage(initialPosition, messageLength);
    appendMessage(initialPosition + alignedMessageLength, messageLength);

    auto handler = [&](const uint8_t *, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        return AERON_ACTION_CONTINUE;
    };

    EXPECT_EQ(imageBoundedControlledPoll(handler, maxPosition, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_sub_pos, initialPosition + alignedMessageLength);
}

TEST_F(ImageTest, shouldStopPollFragmentsToBoundedControlledFragmentHandlerIfImageIsClosed)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        INITIAL_TERM_ID, alignedMessageLength, m_position_bits_to_shift, INITIAL_TERM_ID);

    m_sub_pos = initialPosition;

    appendMessage(initialPosition, messageLength);
    appendMessage(initialPosition + alignedMessageLength, messageLength);

    auto handler = [&](const uint8_t *, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        aeron_image_close(m_image);
        return AERON_ACTION_CONTINUE;
    };

    EXPECT_EQ(imageBoundedControlledPoll(handler, INT64_MAX, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(true, aeron_image_is_closed(m_image));
    EXPECT_EQ(m_sub_pos, initialPosition);
}

TEST_F(ImageTest, shouldPollFragmentsToBoundedFragmentHandlerWithMaxPositionBeforeNextMessage)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        INITIAL_TERM_ID, 0, m_position_bits_to_shift, INITIAL_TERM_ID);
    const int64_t maxPosition = initialPosition + alignedMessageLength;

    m_sub_pos = initialPosition;

    appendMessage(initialPosition, messageLength);
    appendMessage(initialPosition + alignedMessageLength, messageLength);

    auto null_handler = [&](const uint8_t *, size_t length, aeron_header_t *header) {};

    EXPECT_EQ(imageBoundedPoll(null_handler, maxPosition, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_sub_pos, initialPosition + alignedMessageLength);
}

TEST_F(ImageTest, shouldStopPollFragmentsToBoundedFragmentHandlerIfImageIsClosed)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        INITIAL_TERM_ID, 0, m_position_bits_to_shift, INITIAL_TERM_ID);

    m_sub_pos = initialPosition;

    appendMessage(initialPosition, messageLength);
    appendMessage(initialPosition + alignedMessageLength, messageLength);

    auto null_handler = [&](const uint8_t *, size_t length, aeron_header_t *header)
        {
            aeron_image_close(m_image);
        };

    EXPECT_EQ(imageBoundedPoll(null_handler, INT64_MAX, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(true, aeron_image_is_closed(m_image));
    EXPECT_EQ(m_sub_pos, initialPosition);
}

TEST_F(ImageTest, boundedControlledPollCommitShouldNotAdvancePositionWhenClosedDuringHandler)
{
    const size_t messageLength = 64;
    const int64_t alignedLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    createImage();

    appendMessage(0, messageLength);
    appendMessage(alignedLength, messageLength);

    EXPECT_EQ(0, m_sub_pos);

    int callCount = 0;
    int fragments = imageBoundedControlledPoll(
        [&](const uint8_t *, size_t, aeron_header_t *) -> aeron_controlled_fragment_handler_action_t
        {
            callCount++;
            if (callCount == 1)
            {
                aeron_image_close(m_image);
                // COMMIT should be skipped because is_closed is now true
                return AERON_ACTION_COMMIT;
            }
            return AERON_ACTION_CONTINUE;
        },
        INT64_MAX,
        10);

    EXPECT_EQ(1, fragments);
    EXPECT_EQ(1, callCount);
    EXPECT_EQ(0, m_sub_pos);
    EXPECT_EQ(0, aeron_image_position(m_image));
}

TEST_F(ImageTest, blockPollShouldNotAdvancePositionWhenClosedDuringHandler)
{
    const size_t messageLength = 64;

    createImage();

    appendMessage(0, messageLength);

    EXPECT_EQ(0, m_sub_pos);

    int length = aeron_image_block_poll(
        m_image,
        [](void *clientd, const uint8_t *, size_t, int32_t, int32_t)
        {
            aeron_image_t *img = static_cast<aeron_image_t *>(clientd);
            aeron_image_close(img);
        },
        m_image,
        m_term_length);

    // block_poll scanned and delivered the block, but the position write
    // should be skipped because the image was closed
    EXPECT_GT(length, 0);
    EXPECT_EQ(0, m_sub_pos);
    EXPECT_EQ(0, aeron_image_position(m_image));
}

TEST_F(ImageTest, shouldPollFragmentsToBoundedControlledFragmentHandlerWithMaxPositionAfterEndOfTerm)
{
    createImage();

    const size_t messageLength = 120;
    const int32_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int32_t initialOffset = m_term_length - (2 * alignedMessageLength);
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        INITIAL_TERM_ID, initialOffset, m_position_bits_to_shift, INITIAL_TERM_ID);
    const int64_t maxPosition = initialPosition + m_term_length;

    m_sub_pos = initialPosition;

    appendMessage(initialPosition, messageLength);
    appendMessage(initialPosition + alignedMessageLength, messageLength * 2);  // will insert padding

    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        EXPECT_EQ(buffer, termBuffer(0) + initialOffset + AERON_DATA_HEADER_LENGTH);
        EXPECT_EQ(length, messageLength);
        return AERON_ACTION_CONTINUE;
    };

    EXPECT_EQ(imageBoundedControlledPoll(handler, maxPosition, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_sub_pos, m_term_length);
}

TEST_F(ImageTest, shouldPollFragmentsToBoundedControlledFragmentHandlerWithMaxPositionAboveIntMaxValue)
{
    createImage();

    const size_t messageLength = 120;
    const int32_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int32_t initialOffset = m_term_length - (2 * alignedMessageLength);
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        INITIAL_TERM_ID, initialOffset, m_position_bits_to_shift, INITIAL_TERM_ID);
    const int64_t maxPosition = static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1000L;

    m_sub_pos = initialPosition;

    appendMessage(initialPosition, messageLength);
    appendMessage(initialPosition + alignedMessageLength, messageLength * 2);  // will insert padding

    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        EXPECT_EQ(buffer, termBuffer(0) + initialOffset + AERON_DATA_HEADER_LENGTH);
        EXPECT_EQ(length, messageLength);
        return AERON_ACTION_CONTINUE;
    };

    EXPECT_EQ(imageBoundedControlledPoll(handler, maxPosition, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_sub_pos, m_term_length);
}

TEST_F(ImageTest, shouldPollFragmentsToBoundedFragmentHandlerWithMaxPositionAboveIntMaxValue)
{
    createImage();

    const size_t messageLength = 120;
    const int32_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int32_t initialOffset = m_term_length - (2 * alignedMessageLength);
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        INITIAL_TERM_ID, initialOffset, m_position_bits_to_shift, INITIAL_TERM_ID);
    const int64_t maxPosition = static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1000L;

    m_sub_pos = initialPosition;

    appendMessage(initialPosition, messageLength);
    appendMessage(initialPosition + alignedMessageLength, messageLength * 2);  // will insert padding

    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        EXPECT_EQ(buffer, termBuffer(0) + initialOffset + AERON_DATA_HEADER_LENGTH);
        EXPECT_EQ(length, messageLength);
    };

    EXPECT_EQ(imageBoundedPoll(handler, maxPosition, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_sub_pos, m_term_length);
}

TEST_F(ImageTest, shouldCorrectlyCountReferences)
{
    createImage();

    EXPECT_EQ(1, aeron_image_refcnt_acquire(m_image));
    EXPECT_EQ(2, aeron_image_incr_refcnt(m_image));
    EXPECT_EQ(2, aeron_image_refcnt_acquire(m_image));
    EXPECT_EQ(1, aeron_image_decr_refcnt(m_image));
    EXPECT_EQ(1, aeron_image_refcnt_acquire(m_image));
    EXPECT_EQ(0, aeron_image_decr_refcnt(m_image));
    EXPECT_EQ(0, aeron_image_refcnt_acquire(m_image));
}

TEST_F(ImageTest, shouldReleaseRetainedImageAfterItBecomesUnavailable)
{
    int32_t session_id = (int32_t)createImage();

    EXPECT_EQ(0, aeron_client_conductor_subscription_add_image(m_subscription, m_image));
    int64_t expected_refcnt = aeron_image_refcnt_acquire(m_image);

    aeron_image_t *retained = aeron_subscription_image_by_session_id(m_subscription, session_id);
    EXPECT_EQ(m_image, retained);
    expected_refcnt++;
    EXPECT_EQ(expected_refcnt, aeron_image_refcnt_acquire(m_image));

    EXPECT_EQ(0, aeron_client_conductor_subscription_remove_image(m_subscription, m_image));
    EXPECT_EQ(nullptr, aeron_subscription_image_by_session_id(m_subscription, session_id));

    EXPECT_EQ(0, aeron_subscription_image_release(m_subscription, m_image));
    EXPECT_EQ(expected_refcnt, aeron_image_refcnt_acquire(m_image));

    EXPECT_EQ(0, aeron_image_release(m_image));
    expected_refcnt--;
    EXPECT_EQ(expected_refcnt, aeron_image_refcnt_acquire(m_image));
}
