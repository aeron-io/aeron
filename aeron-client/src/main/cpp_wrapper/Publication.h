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

#ifndef AERON_PUBLICATION_H
#define AERON_PUBLICATION_H

#include <array>
#include <atomic>
#include <memory>
#include <string>
#include <vector>
#include <mutex>

#include "concurrent/AtomicBuffer.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "concurrent/status/UnsafeBufferPosition.h"
#include "concurrent/status/StatusIndicatorReader.h"
#include "util/Exceptions.h"

#include "aeronc.h"

namespace aeron
{

using namespace aeron::concurrent::status;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::concurrent;
using namespace aeron::util;

static constexpr std::int64_t NOT_CONNECTED = -1;
static constexpr std::int64_t BACK_PRESSURED = -2;
static constexpr std::int64_t ADMIN_ACTION = -3;
static constexpr std::int64_t PUBLICATION_CLOSED = -4;
static constexpr std::int64_t MAX_POSITION_EXCEEDED = -5;

/**
 * Supplies the reserved value field for a data frame header. The returned value will be set in the header as
 * Little Endian format.
 *
 * This will be called as the last action of encoding a data frame right before the length is set. All other fields
 * in the header plus the body of the frame will have been written at the point of supply.
 *
 * @param termBuffer for the message
 * @param termOffset of the start of the message
 * @param length of the message in bytes
 */
typedef std::function<std::int64_t(
    AtomicBuffer &termBuffer,
    util::index_t termOffset,
    util::index_t length)> on_reserved_value_supplier_t;

static const on_reserved_value_supplier_t DEFAULT_RESERVED_VALUE_SUPPLIER =
    [](AtomicBuffer &, util::index_t, util::index_t) -> std::int64_t
    {
        return 0;
    };

using AsyncDestination = aeron_async_destination_t;

/**
 * @example BasicPublisher.cpp
 */
/**
 * Aeron Publisher API for sending messages to subscribers of a given channel and streamId pair. Publishers
 * are created via an {@link Aeron} object, and messages are sent via an offer method or a claim and commit
 * method combination.
 * <p>
 * The APIs for tryClaim and offer are non-blocking and threadsafe.
 * <p>
 * Note: Publication instances are threadsafe and can be shared between publisher threads.
 * @see Aeron#addPublication
 * @see Aeron#findPublication
 */
class Publication
{
public:

    /// @cond HIDDEN_SYMBOLS
    Publication(aeron_t *aeron, aeron_publication_t *publication) :
        m_aeron(aeron),
        m_publication(publication)
    {
        if (aeron_publication_constants(m_publication, &m_constants) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        m_channel.append(m_constants.channel);
    }
    /// @endcond

    ~Publication()
    {
        aeron_publication_close(m_publication, nullptr, nullptr);
    }

    inline void close()
    {
        if (aeron_publication_close(m_publication, nullptr, nullptr) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        m_publication = nullptr;
    }

    /**
     * Media address for delivery to the channel.
     *
     * @return Media address for delivery to the channel.
     */
    inline const std::string &channel() const
    {
        return m_channel;
    }

    /**
     * Stream identity for scoping within the channel media address.
     *
     * @return Stream identity for scoping within the channel media address.
     */
    inline std::int32_t streamId() const
    {
        return m_constants.stream_id;
    }

    /**
     * Session under which messages are published. Identifies this Publication instance.
     *
     * @return the session id for this publication.
     */
    inline std::int32_t sessionId() const
    {
        return m_constants.session_id;
    }

    /**
     * The initial term id assigned when this Publication was created. This can be used to determine how many
     * terms have passed since creation.
     *
     * @return the initial term id.
     */
    inline std::int32_t initialTermId() const
    {
        return m_constants.initial_term_id;
    }

    /**
     * Get the original registration used to register this Publication with the media driver by the first publisher.
     *
     * @return the original registrationId of the publication.
     */
    inline std::int64_t originalRegistrationId() const
    {
        return m_constants.original_registration_id;
    }

    /**
     * Registration Id returned by Aeron::addPublication when this Publication was added.
     *
     * @return the registrationId of the publication.
     */
    inline std::int64_t registrationId() const
    {
        return m_constants.registration_id;
    }

    /**
     * Is this Publication the original instance added to the driver? If not then it was added after another client
     * has already added the publication.
     *
     * @return true if this instance is the first added otherwise false.
     */
    inline bool isOriginal() const
    {
        return originalRegistrationId() == registrationId();
    }

    /**
     * Maximum message length supported in bytes.
     *
     * @return maximum message length supported in bytes.
     */
    inline util::index_t maxMessageLength() const
    {
        return static_cast<util::index_t>(m_constants.max_message_length);
    }

    /**
     * Maximum length of a message payload that fits within a message fragment.
     *
     * This is the MTU length minus the message fragment header length.
     *
     * @return maximum message fragment payload length.
     */
    inline util::index_t maxPayloadLength() const
    {
        return static_cast<util::index_t>(m_constants.max_payload_length);
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
     * Has this Publication seen an active subscriber recently?
     *
     * @return true if this Publication has seen an active subscriber recently.
     */
    inline bool isConnected() const
    {
        return aeron_publication_is_connected(m_publication);
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    inline bool isClosed() const
    {
        return aeron_publication_is_closed(m_publication);
    }

    /**
     * Get the max possible position the stream can reach given term length.
     *
     * @return the max possible position the stream can reach given term length.
     */
    inline std::int64_t maxPossiblePosition() const
    {
        return m_constants.max_possible_position;
    }

    /**
     * Get the current position to which the publication has advanced for this stream.
     *
     * @return the current position to which the publication has advanced for this stream or {@link CLOSED}.
     */
    inline std::int64_t position() const
    {
        std::int64_t position = aeron_publication_position(m_publication);
        if (AERON_PUBLICATION_ERROR == position)
        {
            if (nullptr == m_publication)
            {
                return AERON_PUBLICATION_CLOSED;
            }

            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return position;
    }

    /**
     * Get the position limit beyond which this {@link Publication} will be back pressured.
     *
     * This should only be used as a guide to determine when back pressure is likely to be applied.
     *
     * @return the position limit beyond which this {@link Publication} will be back pressured.
     */
    inline std::int64_t publicationLimit() const
    {
        std::int64_t limit = aeron_publication_position_limit(m_publication);
        if (AERON_PUBLICATION_ERROR == limit)
        {
            if (nullptr == m_publication)
            {
                return AERON_PUBLICATION_CLOSED;
            }

            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return limit;
    }

    /**
     * Get the counter id used to represent the publication limit.
     *
     * @return the counter id used to represent the publication limit.
     */
    inline std::int32_t publicationLimitId() const
    {
        return m_constants.publication_limit_counter_id;
    }

    /**
     * Available window for offering into a publication before the {@link #positionLimit()} is reached.
     *
     * @return  window for offering into a publication before the {@link #positionLimit()} is reached. If
     * the publication is closed then {@link #CLOSED} will be returned.
     */
    inline std::int64_t availableWindow() const
    {
        const std::int64_t limit = publicationLimit();
        return AERON_PUBLICATION_CLOSED != limit ? limit - position() : AERON_PUBLICATION_CLOSED;
    }

    /**
     * Get the counter id used to represent the channel status.
     *
     * @return the counter id used to represent the channel status.
     */
    inline std::int32_t channelStatusId() const
    {
        return m_constants.channel_status_indicator_id;
    }

    /**
     * Get the status for the channel of this {@link Publication}
     *
     * @return status code for this channel
     */
    std::int64_t channelStatus() const
    {
        return aeron_publication_channel_status(m_publication);
    }

    /**
     * Fetches the local socket addresses for this publication. If the channel is not
     * {@link aeron::concurrent::status::ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE}, then this will return an
     * empty list.
     *
     * The format is as follows:
     * <br>
     * <br>
     * IPv4: <code>ip address:port</code>
     * <br>
     * IPv6: <code>[ip6 address]:port</code>
     * <br>
     * <br>
     * This is to match the formatting used in the Aeron URI
     *
     * @return local socket address for this subscription.
     * @see #channelStatus()
     */
    std::vector<std::string> localSocketAddresses() const
    {
        std::vector<std::string> localAddresses;
        std::uint8_t buffer[AERON_CLIENT_MAX_LOCAL_ADDRESS_STR_LEN];
        // Publications only have a single local address.
        aeron_iovec_t iov;
        iov.iov_base = buffer;
        iov.iov_len = sizeof(buffer);

        if (aeron_publication_local_sockaddrs(m_publication, &iov, 1) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        localAddresses.push_back(std::string(reinterpret_cast<char *>(buffer)));

        return localAddresses;
    }

    /**
     * Non-blocking publish of a buffer containing a message.
     *
     * @param buffer containing message.
     * @param offset offset in the buffer at which the encoded message begins.
     * @param length in bytes of the encoded message.
     * @param reservedValueSupplier for the frame.
     * @return The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     */
    inline std::int64_t offer(
        const concurrent::AtomicBuffer &buffer,
        util::index_t offset,
        util::index_t length,
        const on_reserved_value_supplier_t &reservedValueSupplier)
    {
        std::int64_t position = aeron_publication_offer(
            m_publication,
            buffer.buffer() + offset,
            static_cast<std::size_t>(length),
            reservedValueSupplierCallback,
            (void *)&reservedValueSupplier);
        if (AERON_PUBLICATION_ERROR == position)
        {
            if (nullptr == m_publication)
            {
                return AERON_PUBLICATION_CLOSED;
            }

            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return position;
    }

    /**
     * Non-blocking publish of a buffer containing a message.
     *
     * @param buffer containing message.
     * @param offset offset in the buffer at which the encoded message begins.
     * @param length in bytes of the encoded message.
     * @return The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     */
    inline std::int64_t offer(const concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length)
    {
        return offer(buffer, offset, length, DEFAULT_RESERVED_VALUE_SUPPLIER);
    }

    /**
     * Non-blocking publish of a buffer containing a message.
     *
     * @param buffer containing message.
     * @return The new stream position on success, otherwise {@link BACK_PRESSURED} or {@link NOT_CONNECTED}.
     */
    inline std::int64_t offer(const concurrent::AtomicBuffer &buffer)
    {
        return offer(buffer, 0, buffer.capacity());
    }

    /**
     * Non-blocking publish of buffers containing a message.
     *
     * @param startBuffer containing part of the message.
     * @param lastBuffer after the message.
     * @param reservedValueSupplier for the frame.
     * @return The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     */
    template<class BufferIterator> std::int64_t offer(
        BufferIterator startBuffer,
        BufferIterator lastBuffer,
        const on_reserved_value_supplier_t &reservedValueSupplier = DEFAULT_RESERVED_VALUE_SUPPLIER)
    {
        std::vector<aeron_iovec_t> iov;
        std::size_t length = 0;
        for (BufferIterator it = startBuffer; it != lastBuffer; ++it)
        {
            if (AERON_COND_EXPECT(length + it->capacity() < 0, false))
            {
                throw aeron::util::IllegalStateException(
                    "length overflow: " + std::to_string(length) + " + " + std::to_string(it->capacity()) +
                        " > " + std::to_string(length + it->capacity()),
                    SOURCEINFO);
            }

            aeron_iovec_t buf;
            buf.iov_base = it->buffer();
            buf.iov_len = it->capacity();
            iov.push_back(buf);
        }

        const std::int64_t position = aeron_publication_offerv(
            m_publication, iov.data(), iov.size(), reservedValueSupplierCallback, (void *)&reservedValueSupplier);

        if (AERON_PUBLICATION_ERROR == position)
        {
            if (nullptr == m_publication)
            {
                return AERON_PUBLICATION_CLOSED;
            }

            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return position;
    }

    /**
     * Non-blocking publish of array of buffers containing a message.
     *
     * @param buffers containing parts of the message.
     * @param length of the array of buffers.
     * @param reservedValueSupplier for the frame.
     * @return The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     */
    std::int64_t offer(
        const concurrent::AtomicBuffer *buffers,
        std::size_t length,
        const on_reserved_value_supplier_t &reservedValueSupplier = DEFAULT_RESERVED_VALUE_SUPPLIER)
    {
        return offer(buffers, buffers + length, reservedValueSupplier);
    }

    /**
     * Non-blocking publish of array of buffers containing a message.
     *
     * @param buffers containing parts of the message.
     * @param reservedValueSupplier for the frame.
     * @return The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     */
    template<std::size_t N>
    std::int64_t offer(
        const std::array<concurrent::AtomicBuffer, N> &buffers,
        const on_reserved_value_supplier_t &reservedValueSupplier = DEFAULT_RESERVED_VALUE_SUPPLIER)
    {
        return offer(buffers.begin(), buffers.end(), reservedValueSupplier);
    }

    /**
     * Non-blocking publish of a simple const data buffer and length containing a message
     *
     * @param buffer contain the message.
     * @param length of the message.
     * @param reservedValueSupplier for the frame.
     * @return The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     */
    std::int64_t offer(
        const std::uint8_t *buffer,
        std::size_t length,
        const on_reserved_value_supplier_t &reservedValueSupplier = DEFAULT_RESERVED_VALUE_SUPPLIER)
    {
        std::int64_t position = aeron_publication_offer(
            m_publication,
            buffer,
            static_cast<std::size_t>(length),
            reservedValueSupplierCallback,
            (void *)&reservedValueSupplier);

        if (AERON_PUBLICATION_ERROR == position)
        {
            if (nullptr == m_publication)
            {
                return AERON_PUBLICATION_CLOSED;
            }

            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return position;
    }

    /**
     * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
     * Once the message has been written then {@link BufferClaim#commit()} should be called thus making it available.
     * <p>
     * <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
     *
     * @code
     *     BufferClaim bufferClaim; // Can be stored and reused to avoid allocation
     *
     *     if (publication->tryClaim(messageLength, bufferClaim) > 0)
     *     {
     *         try
     *         {
     *              AtomicBuffer& buffer = bufferClaim.buffer();
     *              const index_t offset = bufferClaim.offset();
     *
     *              // Work with buffer directly or wrap with a flyweight
     *         }
     *         finally
     *         {
     *             bufferClaim.commit();
     *         }
     *     }
     * @endcode
     *
     * @param length      of the range to claim, in bytes..
     * @param bufferClaim to be populate if the claim succeeds.
     * @return The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     * @throws IllegalArgumentException if the length is greater than max payload length within an MTU.
     * @see BufferClaim::commit
     */
    inline std::int64_t tryClaim(util::index_t length, concurrent::logbuffer::BufferClaim &bufferClaim)
    {
        aeron_buffer_claim_t temp_claim;
        const std::int64_t position = aeron_publication_try_claim(
            m_publication, static_cast<std::size_t>(length), &temp_claim);
        if (AERON_PUBLICATION_ERROR == position)
        {
            if (nullptr == m_publication)
            {
                return AERON_PUBLICATION_CLOSED;
            }

            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        // This is slight cheat, we are assuming that the frame_header and data pointers index into the same
        // contiguous block of memory.
        bufferClaim.wrap(temp_claim.frame_header, DataFrameHeader::LENGTH + length);

        return position;
    }

    /**
     * Add a destination manually to a multi-destination-cast Publication.
     *
     * @param endpointChannel for the destination to add
     * @return async object to track the progress of the command
     */
    AsyncDestination *addDestinationAsync(const std::string &endpointChannel)
    {
        AsyncDestination *async = nullptr;
        if (aeron_publication_async_add_destination(&async, m_aeron, m_publication, endpointChannel.c_str()) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return async;
    }

    /**
     * Add a destination manually to a multi-destination-cast Publication.
     *
     * @param endpointChannel for the destination to add
     * @return correlation id for the add command
     */
    std::int64_t addDestination(const std::string &endpointChannel)
    {
        AsyncDestination *async = addDestinationAsync(endpointChannel);
        std::int64_t correlationId = aeron_async_destination_get_registration_id(async);

        std::lock_guard<std::recursive_mutex> lock(m_adminLock);
        m_pendingDestinations[correlationId] = async;

        return correlationId;
    }

    /**
     * Remove a previously added destination manually from a multi-destination-cast Publication.
     *
     * @param endpointChannel for the destination to remove
     * @return async object to track the progress of the command
     */
    AsyncDestination *removeDestinationAsync(const std::string &endpointChannel)
    {
        AsyncDestination *async = nullptr;
        if (aeron_publication_async_remove_destination(&async, m_aeron, m_publication, endpointChannel.c_str()) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return async;
    }

    /**
     * Remove a previously added destination manually from a multi-destination-cast Publication.
     *
     * @param endpointChannel for the destination to remove
     * @return correlation id for the remove command
     */
    std::int64_t removeDestination(const std::string &endpointChannel)
    {
        AsyncDestination *async = removeDestinationAsync(endpointChannel);
        std::int64_t correlationId = aeron_async_destination_get_registration_id(async);

        std::lock_guard<std::recursive_mutex> lock(m_adminLock);
        m_pendingDestinations[correlationId] = async;

        return correlationId;
    }

    /**
     * Remove a previously added destination manually from a multi-destination-cast Publication.
     *
     * @param destinationRegistrationId for the destination to remove
     * @return async object to track the progress of the command
     */
    AsyncDestination *removeDestinationAsync(std::int64_t destinationRegistrationId)
    {
        AsyncDestination *async = nullptr;
        if (aeron_publication_async_remove_destination_by_id(
            &async, m_aeron, m_publication, destinationRegistrationId) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return async;
    }

    /**
     * Remove a previously added destination manually from a multi-destination-cast Publication by destinationRegistrationId.
     *
     * @param destinationRegistrationId for the destination to remove
     * @return correlation id for the remove command
     */
    std::int64_t removeDestination(std::int64_t destinationRegistrationId)
    {
        AsyncDestination *async = removeDestinationAsync(destinationRegistrationId);
        std::int64_t correlationId = aeron_async_destination_get_registration_id(async);

        std::lock_guard<std::recursive_mutex> lock(m_adminLock);
        m_pendingDestinations[correlationId] = async;

        return correlationId;
    }

    /**
     * Retrieve the status of the associated add or remove destination operation with the given correlationId.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the correlationId is unknown, then an exception is thrown.
     * - If the media driver has not answered the add/remove command, then a false is returned.
     * - If the media driver has successfully added or removed the destination then true is returned.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Publication::addDestination
     * @see Publication::removeDestination
     *
     * @param async used to track the progress of the destination command.
     * @return true for added or false if not.
     */
    bool findDestinationResponse(AsyncDestination *async)
    {
        int result = aeron_publication_async_destination_poll(async);
        if (result < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return 0 < result;
    }

    /**
     * Retrieve the status of the associated add or remove destination operation with the given correlationId.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the correlationId is unknown, then an exception is thrown.
     * - If the media driver has not answered the add/remove command, then a false is returned.
     * - If the media driver has successfully added or removed the destination then true is returned.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Publication::addDestination
     * @see Publication::removeDestination
     *
     * @param correlationId of the add/remove command returned by Publication::addDestination
     * or Publication::removeDestination
     * @return true for added or false if not.
     */
    bool findDestinationResponse(std::int64_t correlationId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_adminLock);

        auto search = m_pendingDestinations.find(correlationId);
        if (search == m_pendingDestinations.end())
        {
            throw IllegalArgumentException("Unknown correlation id", SOURCEINFO);
        }

        return findDestinationResponse(search->second);
    }

    /// @cond HIDDEN_SYMBOLS
    aeron_publication_t *publication()
    {
        return m_publication;
    }
    /// @endcond

private:
    aeron_t *m_aeron = nullptr;
    aeron_publication_t *m_publication = nullptr;
    aeron_publication_constants_t m_constants = {};
    std::string m_channel;
    std::unordered_map<std::int64_t, AsyncDestination *> m_pendingDestinations = {};
    std::recursive_mutex m_adminLock = {};

    static std::int64_t reservedValueSupplierCallback(void *clientd, std::uint8_t *buffer, std::size_t frame_length)
    {
        on_reserved_value_supplier_t &supplier = *static_cast<on_reserved_value_supplier_t *>(clientd);
        AtomicBuffer atomicBuffer(buffer, frame_length);
        return supplier(atomicBuffer, 0, static_cast<util::index_t>(frame_length));
    }
};

}

#endif
