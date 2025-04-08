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

#ifndef AERON_SUBSCRIPTION_H
#define AERON_SUBSCRIPTION_H

#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <iterator>
#include <stdexcept>

#include "Image.h"
#include "concurrent/CountersReader.h"
#include "Context.h"
#include "ChannelUri.h"

extern "C"
{
#include "aeron_common.h"
#include "uri/aeron_uri.h"
}

namespace aeron
{

using namespace aeron::concurrent::logbuffer;
using AsyncDestination = aeron_async_destination_t;

class AsyncAddSubscription
{
    friend class Aeron;
private:
    AsyncAddSubscription(
        const on_available_image_t &onAvailableImage,
        const on_unavailable_image_t &onUnavailableImage) :
        m_onAvailableImage(onAvailableImage),
        m_onUnavailableImage(onUnavailableImage)
    {
    }

    aeron_async_add_subscription_t *m_async = nullptr;
    const on_available_image_t m_onAvailableImage;
    const on_unavailable_image_t m_onUnavailableImage;

public:
    void static remove(void *clientd)
    {
        auto *addSubscription = static_cast<AsyncAddSubscription *>(clientd);
        delete addSubscription;
    }
};

/**
 * Aeron Subscriber API for receiving messages from publishers on a given channel and streamId pair.
 * Subscribers are created via an {@link Aeron} object, and received messages are delivered
 * to the {@link fragment_handler_t}.
 * <p>
 * By default fragmented messages are not reassembled before delivery. If an application must
 * receive whole messages, whether or not they were fragmented, then the Subscriber
 * should be created with a {@link FragmentAssembler} or a custom implementation.
 * <p>
 * It is an applications responsibility to {@link #poll} the Subscriber for new messages.
 * <p>
 * Subscriptions are not threadsafe and should not be shared between subscribers.
 *
 * @see FragmentAssembler
 */
class Subscription
{
public:
    /// @cond HIDDEN_SYMBOLS
    Subscription(aeron_t *aeron, aeron_subscription_t *subscription, AsyncAddSubscription *addSubscription) :
        m_aeron(aeron),
        m_subscription(subscription),
        m_addSubscription(addSubscription)
    {
        if (aeron_subscription_constants(m_subscription, &m_constants) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        m_channel.append(m_constants.channel);
    }
    /// @endcond

    ~Subscription()
    {
        aeron_subscription_close(m_subscription, AsyncAddSubscription::remove, m_addSubscription);
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
     * Registration Id returned by Aeron::addSubscription when this Subscription was added.
     *
     * @return the registrationId of the subscription.
     */
    inline std::int64_t registrationId() const
    {
        return m_constants.registration_id;
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
     * Get the status for the channel of this {@link Subscription}
     *
     * @return status code for this channel
     */
    std::int64_t channelStatus() const
    {
        return aeron_subscription_channel_status(m_subscription);
    }

    /**
     * Fetches the local socket addresses for this subscription. If the channel is not
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
        const int initialVectorSize = 16;
        std::uint8_t buffers[initialVectorSize][AERON_CLIENT_MAX_LOCAL_ADDRESS_STR_LEN];
        aeron_iovec_t initialIovecs[initialVectorSize];
        std::unique_ptr<std::uint8_t[]> overflowBuffers;
        std::unique_ptr<aeron_iovec_t[]> overflowIovecs;

        for (size_t i = 0, n = 16; i < n; i++)
        {
            initialIovecs[i].iov_base = buffers[i];
            initialIovecs[i].iov_len = AERON_CLIENT_MAX_LOCAL_ADDRESS_STR_LEN;
        }
        aeron_iovec_t *iovecs = initialIovecs;

        int initialResult = aeron_subscription_local_sockaddrs(m_subscription, initialIovecs, 16);
        if (initialResult < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        int addressCount = initialResult;

        if (initialVectorSize < initialResult)
        {
            const int overflowVectorSize = initialResult;

            overflowBuffers = std::unique_ptr<std::uint8_t[]>(
                new std::uint8_t[overflowVectorSize * AERON_CLIENT_MAX_LOCAL_ADDRESS_STR_LEN]);
            overflowIovecs = std::unique_ptr<aeron_iovec_t[]>(new aeron_iovec_t[overflowVectorSize]);

            for (int i = 0; i < overflowVectorSize; i++)
            {
                overflowIovecs[i].iov_base = &overflowBuffers[i * AERON_CLIENT_MAX_LOCAL_ADDRESS_STR_LEN];
                overflowIovecs[i].iov_len = AERON_CLIENT_MAX_LOCAL_ADDRESS_STR_LEN;
            }

            int overflowResult = aeron_subscription_local_sockaddrs(
                m_subscription, overflowIovecs.get(), static_cast<std::size_t>(overflowVectorSize));
            if (overflowResult < 0)
            {
                AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            addressCount = overflowResult < overflowVectorSize ? overflowResult : overflowVectorSize;
            iovecs = overflowIovecs.get();
        }

        std::vector<std::string> localAddresses;
        for (int i = 0; i < addressCount; i++)
        {
            localAddresses.push_back(std::string(reinterpret_cast<char *>(iovecs[i].iov_base)));
        }

        return localAddresses;
    }

    /**
     * Resolve channel endpoint and replace it with the port from the ephemeral range when 0 was provided. If there are
     * no addresses, or if there is more than one, returned from {@link #localSocketAddresses()} then the original
     * {@link #channel()} is returned.
     * <p>
     * If the channel is not {@link aeron::concurrent::status::ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE}, then an
     * empty string will be returned.
     *
     * @return channel URI string with an endpoint being resolved to the allocated port.
     * @see #channelStatus()
     * @see #localSocketAddresses()
     */
    std::string tryResolveChannelEndpointPort() const
    {
        char uri_buffer[AERON_URI_MAX_LENGTH] = { 0 };

        if (aeron_subscription_try_resolve_channel_endpoint_port(m_subscription, uri_buffer, sizeof(uri_buffer)) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return { uri_buffer };
    }

    /**
     * Find the resolved endpoint for the channel. This may be null of MDS is used and no destination is yet added.
     * The result will similar to taking the first element returned from {@link #localSocketAddresses()}. If more than
     * one destination is added then the first found is returned.
     * <p>
     * If the channel is not {@link aeron::concurrent::status::ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE}, then an
     * empty string will be returned.
     *
     * @return The resolved endpoint or an empty string if not found.
     * @see #channelStatus()
     * @see #localSocketAddresses()
     */
    std::string resolvedEndpoint() const
    {
        char buffer[AERON_CLIENT_MAX_LOCAL_ADDRESS_STR_LEN] = { 0 };

        int result = aeron_subscription_resolved_endpoint(m_subscription, buffer, sizeof(buffer));
        if (result < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return 0 < result ? std::string(buffer) : std::string{};
    }

    AsyncDestination *addDestinationAsync(const std::string &endpointChannel)
    {
        AsyncDestination *async = nullptr;
        if (aeron_subscription_async_add_destination(&async, m_aeron, m_subscription, endpointChannel.c_str()))
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return async;
    }

    /**
     * Add a destination manually to a multi-destination Subscription.
     *
     * @param endpointChannel for the destination to add.
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

    AsyncDestination *removeDestinationAsync(const std::string &endpointChannel)
    {
        AsyncDestination *async = nullptr;
        if (aeron_subscription_async_remove_destination(&async, m_aeron, m_subscription, endpointChannel.c_str()))
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return async;
    }

    /**
     * Remove a previously added destination from a multi-destination Subscription.
     *
     * @param endpointChannel for the destination to remove.
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
     * @see Subscription::addDestination
     * @see Subscription::removeDestination
     *
     * @param correlationId of the add/remove command returned by Subscription::addDestination
     * or Subscription::removeDestination
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

    /**
     * Poll the {@link Image}s under the subscription for available message fragments.
     * <p>
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
     * as a series of fragments ordered withing a session.
     *
     * @param fragmentHandler callback for handling each message fragment as it is read.
     * @param fragmentLimit   number of message fragments to limit for the poll across multiple Image s.
     * @return the number of fragments received
     *
     * @see fragment_handler_t
     */
    template<typename F>
    inline int poll(F &&fragmentHandler, int fragmentLimit)
    {
        using handler_type = typename std::remove_reference<F>::type;
        handler_type &handler = fragmentHandler;
        void *handler_ptr = const_cast<void *>(reinterpret_cast<const void *>(&handler));

        int numFragments = aeron_subscription_poll(
            m_subscription, doPoll<handler_type>, handler_ptr, static_cast<std::size_t>(fragmentLimit));
        if (numFragments < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return numFragments;
    }

    /**
     * Poll in a controlled manner the Image s under the subscription for available message fragments.
     * Control is applied to fragments in the stream. If more fragments can be read on another stream
     * they will even if BREAK or ABORT is returned from the fragment handler.
     * <p>
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
     * as a series of fragments ordered within a session.
     * <p>
     * To assemble messages that span multiple fragments then use controlled_poll_fragment_handler_t.
     *
     * @param fragmentHandler callback for handling each message fragment as it is read.
     * @param fragmentLimit   number of message fragments to limit for the poll operation across multiple Image s.
     * @return the number of fragments received
     * @see controlled_poll_fragment_handler_t
     */
    template<typename F>
    inline int controlledPoll(F &&fragmentHandler, int fragmentLimit)
    {
        using handler_type = typename std::remove_reference<F>::type;
        handler_type &handler = fragmentHandler;
        void *handler_ptr = const_cast<void *>(reinterpret_cast<const void *>(&handler));

        int numFragments = aeron_subscription_controlled_poll(
            m_subscription, doControlledPoll<handler_type>, handler_ptr, static_cast<std::size_t>(fragmentLimit));
        if (numFragments < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return numFragments;
    }

    /**
     * Poll the Image s under the subscription for available message fragments in blocks.
     *
     * @param blockHandler     to receive a block of fragments from each Image.
     * @param blockLengthLimit for each individual block.
     * @return the number of bytes consumed.
     */
    template<typename F>
    inline long blockPoll(F &&blockHandler, int blockLengthLimit)
    {
        using handler_type = typename std::remove_reference<F>::type;
        handler_type &handler = blockHandler;
        void *handler_ptr = const_cast<void *>(reinterpret_cast<const void *>(&handler));

        long numFragments = aeron_subscription_block_poll(
            m_subscription, doBlockPoll<handler_type>, handler_ptr, static_cast<std::size_t>(blockLengthLimit));
        if (numFragments < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return numFragments;
    }

    /**
     * Is the subscription connected by having at least one open image available.
     *
     * @return true if the subscription has more than one open image available.
     */
    inline bool isConnected() const
    {
        return aeron_subscription_is_connected(m_subscription);
    }

    /**
     * Count of images associated with this subscription.
     *
     * @return count of images associated with this subscription.
     */
    inline int imageCount() const
    {
        return aeron_subscription_image_count(m_subscription);
    }

    /**
     * Return the {@link Image} associated with the given sessionId.
     *
     * This method returns a share_ptr to the underlying Image and must be released before the Image may be fully
     * reclaimed.
     *
     * @param sessionId associated with the Image.
     * @return Image associated with the given sessionId or nullptr if no Image exist.
     */
    inline std::shared_ptr<Image> imageBySessionId(std::int32_t sessionId) const
    {
        aeron_image_t *image = aeron_subscription_image_by_session_id(m_subscription, sessionId);
        if (nullptr == image)
        {
            return nullptr;
        }

        return createImage(image);
    }

    /**
     * Return the {@link Image} associated with the given index.
     *
     * This method returns a share_ptr to the underlying Image and must be released before the Image may be fully
     * reclaimed.
     *
     * @param index in the array
     * @return image at given index or exception if out of range.
     */
    inline std::shared_ptr<Image> imageByIndex(std::size_t index) const
    {
        aeron_image_t *image = aeron_subscription_image_at_index(m_subscription, index);
        if (nullptr == image)
        {
            throw std::logic_error("index out of range");
        }

        return createImage(image);
    }

    /**
     * Get a std::vector of active std::shared_ptr of {@link Image}s that match this subscription.
     *
     * This method will create a new std::vector<std::shared_ptr<Image>> populated with the underlying {@link Image}s.
     *
     * @return a std::vector of active std::shared_ptr of {@link Image}s that match this subscription
     */
    inline std::shared_ptr<std::vector<std::shared_ptr<Image>>> copyOfImageList() const
    {
        auto images = std::make_shared<std::vector<std::shared_ptr<Image>>>();
        auto subscriptionAndImages = std::make_pair(m_subscription, images.get());

        aeron_subscription_for_each_image(
            m_subscription, Subscription::copyToVector, static_cast<void *>(&subscriptionAndImages));

        return images;
    }

    /**
     * Iterate over Image list and call passed in function.
     *
     * @return length of Image list
     */
    template<typename F>
    inline int forEachImage(F &&func) const
    {
        auto imageList = copyOfImageList();
        for (auto &image : *imageList)
        {
            func(image);
        }
        
        return static_cast<int>(imageList->size());
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    inline bool isClosed() const
    {
        return aeron_subscription_is_closed(m_subscription);
    }

    /**
     * Get the underlying C Aeron client subscription. Applications should not need to use this method.
     *
     * @return the underlying C Aeron client subscription.
     */
    inline aeron_subscription_t *subscription() const
    {
        return m_subscription;
    }
private:
    aeron_t *m_aeron = nullptr;
    aeron_subscription_t *m_subscription = nullptr;
    AsyncAddSubscription *m_addSubscription = nullptr;
    aeron_subscription_constants_t m_constants = {};
    std::string m_channel;
    std::unordered_map<std::int64_t, AsyncDestination *> m_pendingDestinations = {};
    std::recursive_mutex m_adminLock = {};

    static void copyToVector(aeron_image_t *image, void *clientd)
    {
        auto subscriptionAndImages =
            static_cast<std::pair<aeron_subscription_t *, std::vector<std::shared_ptr<Image>> *> *>(clientd);
        subscriptionAndImages->second->push_back(std::make_shared<Image>(subscriptionAndImages->first, image));
    }

    inline std::shared_ptr<Image> createImage(aeron_image_t *image) const
    {
        std::shared_ptr<Image> result;
        try {
            result = std::make_shared<Image>(m_subscription, image);
        }
        catch (const std::exception& e)
        {
            aeron_subscription_image_release(m_subscription, image);
            throw e;
        }
        aeron_subscription_image_release(m_subscription, image);
        return result;
    }
};

}

#endif
