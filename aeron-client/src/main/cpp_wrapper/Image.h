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

#ifndef AERON_IMAGE_H
#define AERON_IMAGE_H

#include <algorithm>
#include <array>
#include <vector>
#include <atomic>
#include <cassert>
#include <functional>
#include <memory>

#include "concurrent/AtomicBuffer.h"
#include "concurrent/logbuffer/Header.h"

#include "aeronc.h"

namespace aeron
{

using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;

enum class ControlledPollAction : int
{
    /**
     * Abort the current polling operation and do not advance the position for this fragment.
     */
    ABORT = AERON_ACTION_ABORT,

    /**
     * Break from the current polling operation and commit the position as of the end of the current fragment
     * being handled.
     */
    BREAK = AERON_ACTION_BREAK,

    /**
     * Continue processing but commit the position as of the end of the current fragment so that
     * flow control is applied to this point.
     */
    COMMIT = AERON_ACTION_COMMIT,

    /**
     * Continue processing taking the same approach as the in fragment_handler_t.
     */
    CONTINUE = AERON_ACTION_CONTINUE
};

/**
 * Callback for handling fragments of data being read from a log.
 *
 * @param buffer containing the data.
 * @param offset at which the data begins.
 * @param length of the data in bytes.
 * @param header representing the meta data for the data.
 * @return The action to be taken with regard to the stream position after the callback.
 */
typedef std::function<ControlledPollAction(
    concurrent::AtomicBuffer &buffer,
    util::index_t offset,
    util::index_t length,
    concurrent::logbuffer::Header &header)> controlled_poll_fragment_handler_t;


template<typename H>
static void doPoll(void *clientd, const std::uint8_t *buffer, std::size_t length, aeron_header_t *header)
{
    H &handler = *reinterpret_cast<H *>(clientd);
    AtomicBuffer atomicBuffer(const_cast<std::uint8_t *>(buffer), length);
    Header headerWrapper{header};
    handler(atomicBuffer, static_cast<util::index_t>(0), static_cast<util::index_t>(length), headerWrapper);
}

template<typename H>
static aeron_controlled_fragment_handler_action_t doControlledPoll(
    void *clientd, const std::uint8_t *buffer, std::size_t length, aeron_header_t *header)
{
    H &handler = *reinterpret_cast<H *>(clientd);
    AtomicBuffer atomicBuffer(const_cast<std::uint8_t *>(buffer), length);
    Header headerWrapper{header};

    ControlledPollAction action = handler(atomicBuffer, 0, static_cast<std::int32_t>(length), headerWrapper);
    return static_cast<aeron_controlled_fragment_handler_action_t>(action);
}

template<typename H>
static void doBlockPoll(
    void *clientd, const std::uint8_t *buffer, std::size_t length, std::int32_t session_id, std::int32_t term_id)
{
    H &handler = *reinterpret_cast<H *>(clientd);
    AtomicBuffer atomicBuffer(const_cast<std::uint8_t *>(buffer), length);
    handler(atomicBuffer, 0, static_cast<std::int32_t>(length), session_id, term_id);
}

/**
 * Represents a replicated publication {@link Image} from a publisher to a {@link Subscription}.
 * Each {@link Image} identifies a source publisher by session id.
 *
 * Is an overlay on the LogBuffers and Position. So, can be effectively copied and moved.
 */
class Image
{
public:
    typedef Image this_t;
    typedef std::vector<std::shared_ptr<Image>> list_t;
    typedef std::shared_ptr<Image> *array_t;

    Image(aeron_subscription_t *subscription, aeron_image_t *image) :
        m_subscription(subscription), m_image(image), m_sourceIdentity()
    {
        aeron_image_constants(m_image, &m_constants);
        m_sourceIdentity.append(m_constants.source_identity);
        aeron_subscription_image_retain(m_subscription, m_image);
    }

    ~Image()
    {
        aeron_subscription_image_release(m_subscription, m_image);
    }

    /**
     * Get the length in bytes for each term partition in the log buffer.
     *
     * @return the length in bytes for each term partition in the log buffer.
     */
    inline std::int32_t termBufferLength() const
    {
        return static_cast<std::int32_t>(m_constants.term_buffer_length);
    }

    /**
     * Number of bits to right shift a position to get a term count for how far the stream has progressed.
     *
     * @return of bits to right shift a position to get a term count for how far the stream has progressed.
     */
    inline std::int32_t positionBitsToShift() const
    {
        return static_cast<std::int32_t>(m_constants.position_bits_to_shift);
    }

    /**
     * The sessionId for the steam of messages.
     *
     * @return the sessionId for the steam of messages.
     */
    inline std::int32_t sessionId() const
    {
        return m_constants.session_id;
    }

    /**
     * The correlationId for identification of the image with the media driver.
     *
     * @return the correlationId for identification of the image with the media driver.
     */
    inline std::int64_t correlationId() const
    {
        return m_constants.correlation_id;
    }

    /**
     * The registrationId for the Subscription of the Image.
     *
     * @return the registrationId for the Subscription of the Image.
     */
    inline std::int64_t subscriptionRegistrationId() const
    {
        aeron_subscription_constants_t constants;
        aeron_subscription_constants(m_subscription, &constants);
        return constants.registration_id;
    }

    /**
     * The position at which this stream was joined.
     *
     * @return the position at which this stream was joined.
     */
    inline std::int64_t joinPosition() const
    {
        return m_constants.join_position;
    }

    /**
     * The initial term at which the stream started for this session.
     *
     * @return the initial term id.
     */
    inline std::int32_t initialTermId() const
    {
        return m_constants.initial_term_id;
    }

    /**
     * The source identity of the sending publisher as an abstract concept appropriate for the media.
     *
     * @return source identity of the sending publisher as an abstract concept appropriate for the media.
     */
    inline std::string sourceIdentity() const
    {
        return m_constants.source_identity;
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    inline bool isClosed() const
    {
        return aeron_image_is_closed(m_image);
    }

    inline bool isPublicationRevoked() const
    {
        return aeron_image_is_publication_revoked(m_image);
    }

    /**
     * The position this Image has been consumed to by the subscriber.
     *
     * @return the position this Image has been consumed to by the subscriber or CLOSED if closed
     */
    inline std::int64_t position() const
    {
        std::int64_t position = aeron_image_position(m_image);
        if (position < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return position;
    }

    /**
     * Get the counter id used to represent the subscriber position.
     *
     * @return the counter id used to represent the subscriber position.
     */
    inline std::int32_t subscriberPositionId() const
    {
        return m_constants.subscriber_position_id;
    }

    /**
     * Set the subscriber position for this Image to indicate where it has been consumed to.
     *
     * @param newPosition for the consumption point.
     */
    inline void position(std::int64_t newPosition)
    {
        if (aeron_image_set_position(m_image, newPosition) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * Is the current consumed position at the end of the stream?
     *
     * @return true if at the end of the stream or false if not.
     */
    inline bool isEndOfStream() const
    {
        return aeron_image_is_end_of_stream(m_image);
    }

    /**
     * The position the stream reached when EOS was received from the publisher. The position will be
     * INT64_MAX until the stream ends and EOS is set.
     *
     * @return position the stream reached when EOS was received from the publisher.
     */
    inline std::int64_t endOfStreamPosition() const
    {
        return aeron_image_end_of_stream_position(m_image);
    }

    inline void reject(std::string reason)
    {
        if (aeron_image_reject(m_image, reason.c_str()) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * A count of observed active transports within the Image liveness timeout.
     *
     * If the Image is closed, then this is 0. This may also be 0 if no actual datagrams have arrived. IPC
     * Images also will be 0.
     *
     * @return count of active transports - or 0 if Image is closed, no datagrams yet, or IPC.
     */
    inline std::int32_t activeTransportCount() const
    {
        int count = aeron_image_active_transport_count(m_image);
        if (count < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered via the fragment_handler_t up to a limited number of fragments as specified.
     *
     * @param fragmentHandler to which messages are delivered.
     * @param fragmentLimit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     *
     * @see fragment_handler_t
     */
    template<typename F>
    inline int poll(F &&fragmentHandler, int fragmentLimit)
    {
        using handler_type = typename std::remove_reference<F>::type;
        handler_type &handler = fragmentHandler;
        void *handler_ptr = const_cast<void *>(reinterpret_cast<const void *>(&handler));
        int numFragments = aeron_image_poll(
            m_image, doPoll<handler_type>, handler_ptr, static_cast<std::size_t>(fragmentLimit));

        if (numFragments < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return numFragments;
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered via the fragment_handler_t up to a limited number of fragments as specified or the
     * maximum position specified.
     *
     * @param fragmentHandler to which messages are delivered.
     * @param limitPosition   to consume messages up to.
     * @param fragmentLimit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     *
     * @see fragment_handler_t
     */
    template<typename F>
    inline int boundedPoll(F &&fragmentHandler, std::int64_t limitPosition, int fragmentLimit)
    {
        using handler_type = typename std::remove_reference<F>::type;
        handler_type &handler = fragmentHandler;
        void *handler_ptr = const_cast<void *>(reinterpret_cast<const void *>(&handler));

        int numFragments = aeron_image_bounded_poll(
            m_image, doPoll<handler_type>, handler_ptr, limitPosition, static_cast<std::size_t>(fragmentLimit));
        if (numFragments < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return numFragments;
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered to the controlled_poll_fragment_handler_t up to a limited number of fragments as specified.
     *
     * To assemble messages that span multiple fragments then use ControlledFragmentAssembler.
     *
     * @param fragmentHandler to which message fragments are delivered.
     * @param fragmentLimit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     *
     * @see controlled_poll_fragment_handler_t
     */
    template<typename F>
    inline int controlledPoll(F &&fragmentHandler, int fragmentLimit)
    {
        using handler_type = typename std::remove_reference<F>::type;
        handler_type &handler = fragmentHandler;
        void *handler_ptr = const_cast<void *>(reinterpret_cast<const void *>(&handler));

        int numFragments = aeron_image_controlled_poll(
            m_image, doControlledPoll<handler_type>, handler_ptr, static_cast<std::size_t>(fragmentLimit));
        if (numFragments < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return numFragments;
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered to the controlled_poll_fragment_handler_t up to a limited number of fragments as specified
     * or the maximum position specified.
     *
     * To assemble messages that span multiple fragments then use ControlledFragmentAssembler.
     *
     * @param fragmentHandler to which message fragments are delivered.
     * @param limitPosition   to consume messages up to.
     * @param fragmentLimit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     * @see controlled_poll_fragment_handler_t
     */
    template<typename F>
    inline int boundedControlledPoll(F &&fragmentHandler, std::int64_t limitPosition, int fragmentLimit)
    {
        using handler_type = typename std::remove_reference<F>::type;
        handler_type &handler = fragmentHandler;
        void *handler_ptr = const_cast<void *>(reinterpret_cast<const void *>(&handler));

        int numFragments = aeron_image_bounded_controlled_poll(
            m_image,
            doControlledPoll<handler_type>,
            handler_ptr,
            limitPosition,
            static_cast<std::size_t>(fragmentLimit));
        if (numFragments < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return numFragments;
    }

    /**
     * Peek for new messages in a stream by scanning forward from an initial position. If new messages are found then
     * they will be delivered to the controlled_poll_fragment_handler_t up to a limited position.
     * <p>
     * To assemble messages that span multiple fragments then use ControlledFragmentAssembler. Scans must also
     * start at the beginning of a message so that the assembler is reset.
     *
     * @param initialPosition from which to peek forward.
     * @param fragmentHandler to which message fragments are delivered.
     * @param limitPosition   up to which can be scanned.
     * @return the resulting position after the scan terminates which is a complete message.
     * @see controlled_poll_fragment_handler_t
     */
    template<typename F>
    inline std::int64_t controlledPeek(std::int64_t initialPosition, F &&fragmentHandler, std::int64_t limitPosition)
    {
        using handler_type = typename std::remove_reference<F>::type;
        handler_type &handler = fragmentHandler;
        void *handler_ptr = const_cast<void *>(reinterpret_cast<const void *>(&handler));

        std::int64_t bytesPeeked = aeron_image_controlled_peek(
            m_image, initialPosition, doControlledPoll<handler_type>, handler_ptr, limitPosition);

        if (bytesPeeked < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return bytesPeeked;
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered via the block_handler_t up to a limited number of bytes.
     *
     * A scan will terminate if a padding frame is encountered. If first frame in a scan is padding then a block
     * for the padding is notified. If the padding comes after the first frame in a scan then the scan terminates
     * at the offset the padding frame begins. Padding frames are delivered singularly in a block.
     *
     * Padding frames may be for a greater range than the limit offset but only the header needs to be valid so
     * relevant length of the frame is sizeof DataHeaderDefn.
     *
     * @param blockHandler     to which block is delivered.
     * @param blockLengthLimit up to which a block may be in length.
     * @return the number of bytes that have been consumed.
     *
     * @see block_handler_t
     */
    template<typename F>
    inline int blockPoll(F &&blockHandler, int blockLengthLimit)
    {
        using handler_type = typename std::remove_reference<F>::type;
        handler_type &handler = blockHandler;
        void *handler_ptr = const_cast<void *>(reinterpret_cast<const void *>(&handler));

        int numFragments = aeron_image_block_poll(
            m_image, doBlockPoll<handler_type>, handler_ptr, static_cast<std::size_t>(blockLengthLimit));
        if (numFragments < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return numFragments;
    }

private:
    aeron_subscription_t *m_subscription = nullptr;
    aeron_image_t *m_image = nullptr;
    aeron_image_constants_t m_constants = {};
    std::string m_sourceIdentity;
};

}

#endif
