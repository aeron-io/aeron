/*
 * Copyright 2014-2020 Real Logic Limited.
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
#include <atomic>
#include <memory>
#include <iterator>
#include "Image.h"
#include "util/Export.h"
#include "concurrent/CountersReader.h"
#include "Context.h"

namespace aeron
{

using namespace aeron::concurrent::logbuffer;

class AsyncAddSubscription
{
    friend class Aeron;
private:
    AsyncAddSubscription(
        const on_available_image_t &onAvailableImage,
        const on_unavailable_image_t &onUnavailableImage) :
        m_async(nullptr),
        m_onAvailableImage(onAvailableImage),
        m_onUnavailableImage(onUnavailableImage)
    {}
    aeron_async_add_subscription_t *m_async;
    const on_available_image_t m_onAvailableImage;
    const on_unavailable_image_t m_onUnavailableImage;
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
class CLIENT_EXPORT Subscription
{
public:
    /// @cond HIDDEN_SYMBOLS
    Subscription(
        aeron_subscription_t *subscription,
        AsyncAddSubscription *addSubscription,
        CountersReader& countersReader) :
        m_subscription(subscription),
        m_addSubscription(addSubscription),
        m_countersReader(countersReader),
        m_channel()
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
        delete m_addSubscription;
        if (aeron_subscription_close(m_subscription) < 0)
        {
            // TODO: What should happen here???
        }
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
        // TODO: Float this through the C API.
        return -1;
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
    std::vector<std::string> localSocketAddresses() const;

    /**
     * Add a destination manually to a multi-destination Subscription.
     *
     * @param endpointChannel for the destination to add.
     * @return correlation id for the add command
     */
    std::int64_t addDestination(const std::string &endpointChannel);

    /**
     * Remove a previously added destination from a multi-destination Subscription.
     *
     * @param endpointChannel for the destination to remove.
     * @return correlation id for the remove command
     */
    std::int64_t removeDestination(const std::string &endpointChannel);

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
    bool findDestinationResponse(std::int64_t correlationId);

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
        PollWrapper<F> wrapper(std::forward<F>(fragmentHandler));
        return aeron_subscription_poll(m_subscription, PollWrapper<F>::poll, &wrapper, fragmentLimit);
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
        ControlledPollWrapper<F> wrapper(std::forward<F>(fragmentHandler));
        return aeron_subscription_controlled_poll(
            m_subscription, ControlledPollWrapper<F>::controlledPoll, &wrapper, fragmentLimit);
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
        BlockPollWrapper<F> wrapper(std::forward<F>(blockHandler));
        return aeron_subscription_block_poll(
            m_subscription, BlockPollWrapper<F>::blockPoll, &wrapper, blockLengthLimit);
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
//        aeron_image_t *image = aeron_subscription_image_by_session_id(m_subscription, sessionId);
        throw UnsupportedOperationException("Need to support images", SOURCEINFO);
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
    inline std::shared_ptr<Image> imageByIndex(size_t index) const
    {
        aeron_image_t *image = aeron_subscription_image_at_index(m_subscription, index);
        return std::make_shared<Image>(m_subscription, image);
    }

    /**
     * Get a std::vector of active std::shared_ptr of {@link Image}s that match this subscription.
     *
     * THis method will create a new std::vector<std::shared_ptr<Image>> populated with the underlying {@link Image}s.
     *
     * @return a std::vector of active std::shared_ptr of {@link Image}s that match this subscription
     */
    inline std::shared_ptr<std::vector<std::shared_ptr<Image>>> copyOfImageList() const
    {
        // TODO: need clientd on aeron_subscription_for_each_image
        throw UnsupportedOperationException("Need to support images", SOURCEINFO);
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
        for (auto & image : *imageList)
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

    /// @cond HIDDEN_SYMBOLS
    bool hasImage(std::int64_t correlationId) const
    {
        bool hasImage = false;
        auto imageList = copyOfImageList();
        for (auto & image : *imageList)
        {
            if (image->correlationId() == correlationId)
            {
                hasImage = true;
                break;
            }
        }

        return hasImage;
    }

    Image::array_t addImage(std::shared_ptr<Image> image)
    {
        throw UnsupportedOperationException("Need to support images", SOURCEINFO);
//        return m_imageArray.addElement(std::move(image)).first;
    }

    std::pair<Image::array_t, std::size_t> removeImage(std::int64_t correlationId)
    {
        throw UnsupportedOperationException("Need to support images", SOURCEINFO);
//        auto result = m_imageArray.removeElement(
//            [&](const std::shared_ptr<Image> &image)
//            {
//                if (image->correlationId() == correlationId)
//                {
//                    image->close();
//                    return true;
//                }
//
//                return false;
//            });
//
//        return result;
    }

    std::pair<Image::array_t, std::size_t> closeAndRemoveImages()
    {
        throw UnsupportedOperationException("Need to support images", SOURCEINFO);
//        aeron_subscription_close(m_subscription);
//        if (!m_isClosed.exchange(true))
//        {
//            std::pair<Image::array_t, std::size_t> imageArrayPair = m_imageArray.load();
//            m_imageArray.store(new std::shared_ptr<Image>[0], 0);
//
//            return imageArrayPair;
//        }
//        else
//        {
//            return { nullptr, 0 };
//        }
    }
    /// @endcond

private:
    aeron_subscription_t *m_subscription;
    AsyncAddSubscription *m_addSubscription;
    aeron_subscription_constants_t m_constants;
    CountersReader& m_countersReader;
    std::string m_channel;
};

}

#endif
