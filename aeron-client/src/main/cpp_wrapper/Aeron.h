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

#ifndef INCLUDED_AERON_H
#define INCLUDED_AERON_H

#include <unordered_map>
#include <mutex>

#include "util/Exceptions.h"
#include "concurrent/AgentInvoker.h"
#include "Publication.h"
#include "ExclusivePublication.h"
#include "Subscription.h"
#include "Context.h"
#include "Counter.h"
#include "ClientConductor.h"

#include "aeronc.h"

/// Top namespace for Aeron C++ API
namespace aeron
{

using namespace aeron::util;
using namespace aeron::concurrent;

using AsyncAddPublication = aeron_async_add_publication_t;
using AsyncAddExclusivePublication = aeron_async_add_exclusive_publication_t;
using AsyncAddCounter = aeron_async_add_counter_t;

/**
 * @example BasicPublisher.cpp
 * An example of a basic publishing application
 *
 * @example BasicSubscriber.cpp
 * An example of a basic subscribing application
 */

/**
 * Aeron entry point for communicating to the Media Driver for creating {@link Publication}s and {@link Subscription}s.
 * Use a {@link Context} to configure the Aeron object.
 * <p>
 * A client application requires only one Aeron object per Media Driver.
 */
class Aeron
{
public:
    /**
     * Create an Aeron instance and connect to the media driver.
     * <p>
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     * <p>
     * Note that the ownership of the Context object is passed over to the Aeron instance and the context object is
     * invalid after calling this constructor.
     *
     * @param context for configuration of the client.
     */
    explicit Aeron(Context &context) :
        m_context(context.conclude()),
        m_aeron(Aeron::init_aeron(m_context, &m_aeron_context)),
        m_countersReader(aeron_counters_reader(m_aeron)),
        m_clientConductor(m_aeron),
        m_conductorInvoker(m_clientConductor, m_context.m_exceptionHandler)
    {
        aeron_start(m_aeron);
    }

    explicit Aeron(aeron_t *aeron) :
        m_aeron_context(aeron_context(aeron)),
        m_aeron(aeron),
        m_countersReader(aeron_counters_reader(m_aeron)),
        m_clientConductor(m_aeron),
        m_conductorInvoker(m_clientConductor, m_context.m_exceptionHandler)
    {
    }

    ~Aeron()
    {
        aeron_on_close_client_pair_t closePair = {emptyCallback, nullptr};
        aeron_add_close_handler(m_aeron, &closePair);
        aeron_close(m_aeron);
        aeron_context_close(m_aeron_context);

        for (const std::pair<const std::int64_t, AsyncAddSubscription *>& e : m_pendingSubscriptions)
        {
            delete e.second;
        }

        m_availableCounterHandlers.clear();
        m_unavailableCounterHandlers.clear();
        m_closeClientHandlers.clear();
        m_pendingCounters.clear();
        m_pendingPublications.clear();
        m_pendingExclusivePublications.clear();
        m_pendingSubscriptions.clear();
    }

    /**
     * Indicate if the instance is closed and can not longer be used.
     *
     * @return true is the instance is closed and can no longer be used, otherwise false.
     */
    inline bool isClosed()
    {
        return aeron_is_closed(m_aeron);
    }

    /**
     * Create an Aeron instance and connect to the media driver.
     * <p>
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     *
     * @param context for configuration of the client.
     * @return the new Aeron instance connected to the Media Driver.
     */
    inline static std::shared_ptr<Aeron> connect(Context &context)
    {
        auto aeron = std::make_shared<Aeron>(context);
        aeron->m_self = aeron;
        return aeron;
    }

    /**
     * Create an Aeron instance and connect to the media driver.
     * <p>
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     *
     * @return the new Aeron instance connected to the Media Driver.
     */
    inline static std::shared_ptr<Aeron> connect()
    {
        Context ctx;
        return connect(ctx);
    }

    /**
     * Add a {@link Publication} for publishing messages to subscribers
     *
     * This function returns immediately and does not wait for the response from the media driver. The returned
     * registration id is to be used to determine the status of the command with the media driver.
     *
     * @param channel for sending the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return id to resolve the AsyncAddPublication for the publication
     */
    inline std::int64_t addPublication(const std::string &channel, std::int32_t streamId)
    {
        AsyncAddPublication *addPublication = addPublicationAsync(channel, streamId);
        std::int64_t registrationId = aeron_async_add_publication_get_registration_id(addPublication);

        std::lock_guard<std::recursive_mutex> lock(m_adminLock);
        m_pendingPublications[registrationId] = addPublication;

        return registrationId;
    }

    /**
     * Retrieve the Publication associated with the given registrationId.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registrationId is unknown, then a nullptr is returned.
     * - If the media driver has not answered the add command, then a nullptr is returned.
     * - If the media driver has successfully added the Publication then what is returned is the Publication.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::addPublication
     *
     * @param registrationId of the Publication returned by Aeron::addPublication
     * @return Publication associated with the registrationId
     */
    inline std::shared_ptr<Publication> findPublication(std::int64_t registrationId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_adminLock);

        auto search = m_pendingPublications.find(registrationId);
        if (search == m_pendingPublications.end())
        {
            throw IllegalArgumentException(
                std::string("Unknown registration id: ").append(std::to_string(registrationId)), SOURCEINFO);
        }

        try
        {
            std::shared_ptr<Publication> publication = findPublication(search->second);
            if (nullptr != publication)
            {
                m_pendingPublications.erase(registrationId);
            }
            return publication;
        }
        catch (...)
        {
            m_pendingPublications.erase(registrationId);
            throw;
        }
    }

    /**
     * Add a {@link Publication} for publishing messages to subscribers
     *
     * This function returns immediately and does not wait for the response from the media driver. The returned
     * AsyncAddPublication is to be used to determine the status of the command with the media driver.
     *
     * @param channel for sending the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return AsyncAddPublication for the publication.
     */
    inline AsyncAddPublication *addPublicationAsync(const std::string &channel, std::int32_t streamId)
    {
        aeron_async_add_publication_t *addPublication;
        if (aeron_async_add_publication(&addPublication, m_aeron, channel.c_str(), streamId) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return addPublication;
    }

    /**
     * Retrieve the Publication associated with the given AsyncAddPublication.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registrationId is unknown, then a nullptr is returned.
     * - If the media driver has not answered the add command, then a nullptr is returned.
     * - If the media driver has successfully added the Publication then what is returned is the Publication.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::addPublicationAsync
     *
     * @param addPublication for the Publication returned by Aeron::addPublicationAsync
     * @return Publication associated with the addPublication
     */
    inline std::shared_ptr<Publication> findPublication(AsyncAddPublication *addPublication)
    {
        aeron_publication_t *publication;
        int result = aeron_async_add_publication_poll(&publication, addPublication);
        if (result < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        else if (0 == result)
        {
            return nullptr;
        }
        else
        {
            return std::make_shared<Publication>(m_self.lock(), m_aeron, publication);
        }
    }

    /**
     * Gets the registration id for addition of the publication. Note that using this after a call to poll the
     * succeeds or errors is undefined behaviour. As the AsyncAddPublication may have been freed.
     * <p>
     * <em>Note:<em> The return value cannot be used to call `findPublication` method.
     *
     * @param addPublication used to check for completion.
     * @return registration id for the publication.
     */
    inline std::int64_t addPublicationAsyncGetRegistrationId(AsyncAddPublication *addPublication)
    {
        return aeron_async_add_publication_get_registration_id(addPublication);
    }

    /**
     * Add an {@link ExclusivePublication} for publishing messages to subscribers from a single thread.
     *
     * @param channel  for sending the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return id to resolve AsyncAddExclusivePublication for the publication
     */
    inline std::int64_t addExclusivePublication(const std::string &channel, std::int32_t streamId)
    {
        AsyncAddExclusivePublication *addExclusivePublication = addExclusivePublicationAsync(channel, streamId);
        std::int64_t registrationId = aeron_async_add_exclusive_exclusive_publication_get_registration_id(
            addExclusivePublication);

        std::lock_guard<std::recursive_mutex> lock(m_adminLock);
        m_pendingExclusivePublications[registrationId] = addExclusivePublication;

        return registrationId;
    }

    /**
     * Retrieve the ExclusivePublication associated with the given registrationId.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registrationId is unknown, then a nullptr is returned.
     * - If the media driver has not answered the add command, then a nullptr is returned.
     * - If the media driver has successfully added the ExclusivePublication then what is returned is the
     *  ExclusivePublication.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::addExclusivePublication
     *
     * @param registrationId of the ExclusivePublication returned by Aeron::addExclusivePublication
     * @return ExclusivePublication associated with the registrationId
     */
    inline std::shared_ptr<ExclusivePublication> findExclusivePublication(std::int64_t registrationId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_adminLock);

        auto search = m_pendingExclusivePublications.find(registrationId);
        if (search == m_pendingExclusivePublications.end())
        {
            throw IllegalArgumentException(
                std::string("Unknown registration id: ").append(std::to_string(registrationId)), SOURCEINFO);
        }

        try
        {
            std::shared_ptr<ExclusivePublication> publication = findExclusivePublication(search->second);
            if (nullptr != publication)
            {
                m_pendingExclusivePublications.erase(registrationId);
            }
            return publication;
        }
        catch (...)
        {
            m_pendingExclusivePublications.erase(registrationId);
            throw;
        }
    }

    /**
     * Add a {@link ExclusivePublication} for publishing messages to subscribers
     *
     * This function returns immediately and does not wait for the response from the media driver. The returned
     * AsyncAddExclusivePublication is to be used to determine the status of the command with the media driver.
     *
     * @param channel for sending the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return AsyncAddExclusivePublication for the publication.
     */
    inline AsyncAddExclusivePublication *addExclusivePublicationAsync(const std::string &channel, std::int32_t streamId)
    {
        aeron_async_add_exclusive_publication_t *addPublication;
        if (aeron_async_add_exclusive_publication(&addPublication, m_aeron, channel.c_str(), streamId) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return addPublication;
    }

    /**
     * Retrieve the ExclusivePublication associated with the given AsyncAddPublication.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registrationId is unknown, then a nullptr is returned.
     * - If the media driver has not answered the add command, then a nullptr is returned.
     * - If the media driver has successfully added the ExclusivePublication then what is returned is the
     *  ExclusivePublication.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::addExclusivePublicationAsync
     *
     * @param addPublication for the ExclusivePublication returned by Aeron::addExclusivePublicationAsync
     * @return ExclusivePublication associated with the addPublication
     */
    inline std::shared_ptr<ExclusivePublication> findExclusivePublication(AsyncAddExclusivePublication *addPublication)
    {
        aeron_exclusive_publication_t *publication;
        int result = aeron_async_add_exclusive_publication_poll(&publication, addPublication);
        if (result < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        else if (0 == result)
        {
            return nullptr;
        }
        else
        {
            return std::make_shared<ExclusivePublication>(m_self.lock(), m_aeron, publication);
        }
    }

    /**
     * Gets the registration id for addition of the exclusive publication. Note that using this after a call to poll the
     * succeeds or errors is undefined behaviour. As the AsyncAddExclusivePublication may have been freed.
     * <p>
     * <em>Note:<em> The return value cannot be used to call `findPublication` method.
     *
     * @param addPublication used to check for completion.
     * @return registration id for the exclusive publication.
     */
    inline std::int64_t addExclusivePublicationAsyncGetRegistrationId(AsyncAddExclusivePublication *addPublication)
    {
        return aeron_async_add_exclusive_publication_get_registration_id(addPublication);
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     *
     * This function returns immediately and does not wait for the response from the media driver. The returned
     * registration id is to be used to determine the status of the command with the media driver.
     *
     * @param channel  for receiving the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return registration id for the subscription
     */
    inline std::int64_t addSubscription(const std::string &channel, std::int32_t streamId)
    {
        return addSubscription(
            channel, streamId, m_context.m_onAvailableImageHandler, m_context.m_onUnavailableImageHandler);
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     *
     * This method will override the default handlers from the {@link Context}.
     *
     * @param channel                 for receiving the messages known to the media layer.
     * @param streamId                within the channel scope.
     * @param availableImageHandler   called when {@link Image}s become available for consumption.
     * @param unavailableImageHandler called when {@link Image}s go unavailable for consumption.
     * @return id to resolve the AsyncAddSubscription for the subscription
     */
    std::int64_t addSubscription(
        const std::string &channel,
        std::int32_t streamId,
        const on_available_image_t &onAvailableImageHandler,
        const on_unavailable_image_t &onUnavailableImageHandler)
    {
        AsyncAddSubscription *addSubscription = addSubscriptionAsync(
            channel, streamId, onAvailableImageHandler, onUnavailableImageHandler);
        std::int64_t registrationId = aeron_async_add_subscription_get_registration_id(addSubscription->m_async);

        std::lock_guard<std::recursive_mutex> lock(m_adminLock);
        m_pendingSubscriptions[registrationId] = addSubscription;

        return registrationId;
    }

    /**
     * Retrieve the Subscription associated with the given registrationId.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registrationId is unknown, then a nullptr is returned.
     * - If the media driver has not answered the add command, then a nullptr is returned.
     * - If the media driver has successfully added the Subscription then what is returned is the Subscription.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::addSubscription
     *
     * @param registrationId of the Subscription returned by Aeron::addSubscription
     * @return Subscription associated with the registrationId
     */
    inline std::shared_ptr<Subscription> findSubscription(std::int64_t registrationId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_adminLock);

        auto search = m_pendingSubscriptions.find(registrationId);
        if (search == m_pendingSubscriptions.end())
        {
            throw IllegalArgumentException(
                std::string("Unknown registration id: ").append(std::to_string(registrationId)), SOURCEINFO);
        }

        AsyncAddSubscription* addSubscription = search->second;
        try
        {
            std::shared_ptr<Subscription> subscription = findSubscription(addSubscription);
            if (nullptr != subscription)
            {
                m_pendingSubscriptions.erase(registrationId);
            }
            return subscription;
        }
        catch (...)
        {
            m_pendingSubscriptions.erase(registrationId);
            delete addSubscription;
            throw;
        }
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     *
     * This method will override the default handlers from the {@link Context}.
     *
     * @param channel                 for receiving the messages known to the media layer.
     * @param streamId                within the channel scope.
     * @param availableImageHandler   called when {@link Image}s become available for consumption.
     * @param unavailableImageHandler called when {@link Image}s go unavailable for consumption.
     * @return AsyncAddSubscription object to track the addition of the subscription
     */
    inline AsyncAddSubscription *addSubscriptionAsync(
        const std::string &channel,
        std::int32_t streamId,
        const on_available_image_t &onAvailableImageHandler,
        const on_unavailable_image_t &onUnavailableImageHandler)
    {
        auto *addSubscription = new AsyncAddSubscription(onAvailableImageHandler, onUnavailableImageHandler);
        try
        {
            void *availableClientd =
                const_cast<void *>(reinterpret_cast<const void *>(&addSubscription->m_onAvailableImage));
            void *unavailableClientd =
                const_cast<void *>(reinterpret_cast<const void *>(&addSubscription->m_onUnavailableImage));

            if (aeron_async_add_subscription(
                &addSubscription->m_async,
                m_aeron,
                channel.c_str(),
                streamId,
                onAvailableImageCallback,
                availableClientd,
                onUnavailableImageCallback,
                unavailableClientd) < 0)
            {
                AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            return addSubscription;
        }
        catch (...)
        {
            delete addSubscription;
            throw;
        }
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     *
     * This method will override the default handlers from the {@link Context}.
     *
     * @param channel  for receiving the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return AsyncAddSubscription object to track the addition of the subscription
     */
    inline AsyncAddSubscription *addSubscriptionAsync(const std::string &channel, std::int32_t streamId)
    {
        return addSubscriptionAsync(
            channel, streamId, m_context.m_onAvailableImageHandler, m_context.m_onUnavailableImageHandler);
    }

    /**
     * Retrieve the Subscription associated with the given registrationId.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registrationId is unknown, then a nullptr is returned.
     * - If the media driver has not answered the add command, then a nullptr is returned.
     * - If the media driver has successfully added the Subscription then what is returned is the Subscription.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::addSubscription
     *
     * @param registrationId of the Subscription returned by Aeron::addSubscription
     * @return Subscription associated with the registrationId
     */
    inline std::shared_ptr<Subscription> findSubscription(AsyncAddSubscription *addSubscription)
    {
        aeron_subscription_t *subscription;
        int result = aeron_async_add_subscription_poll(&subscription, addSubscription->m_async);
        if (result < 0)
        {
            addSubscription->m_async = nullptr;
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        else if (0 == result)
        {
            return nullptr;
        }
        else
        {
            addSubscription->m_async = nullptr;
            return std::make_shared<Subscription>(m_self.lock(), m_aeron, subscription, addSubscription);
        }
    }

    /**
     * Gets the registration id for addition of the subscription. Note that using this after a call to poll the
     * succeeds or errors is undefined behaviour. As the AsyncAddSubscription may have been freed.
     * <p>
     * <em>Note:<em> The return value cannot be used to call `findPublication` method.
     *
     * @param addSubscription used to check for completion.
     * @return registration id for the subscription.
     */
    inline std::int64_t addSubscriptionAsyncGetRegistrationId(AsyncAddSubscription *addSubscription)
    {
        return aeron_async_add_subscription_get_registration_id(addSubscription->m_async);
    }

    /**
     * Generate the next correlation id that is unique for the connected Media Driver.
     *
     * This is useful generating correlation identifiers for pairing requests with responses in a clients own
     * application protocol.
     *
     * This method is thread safe and will work across processes that all use the same media driver.
     *
     * @return next correlation id that is unique for the Media Driver.
     */
    inline std::int64_t nextCorrelationId()
    {
        return aeron_next_correlation_id(m_aeron);
    }

    /**
     * Allocate a counter on the media driver and return a {@link Counter} for it.
     *
     * @param typeId      for the counter.
     * @param keyBuffer   containing the optional key for the counter.
     * @param keyLength   of the key in the keyBuffer.
     * @param label       for the counter.
     * @return id to resolve the AsyncAddCounter for the Counter
     */
    std::int64_t addCounter(
        std::int32_t typeId,
        const std::uint8_t *keyBuffer,
        std::size_t keyLength,
        const std::string &label)
    {
        AsyncAddCounter *addCounter = addCounterAsync(typeId, keyBuffer, keyLength, label);
        std::int64_t registrationId = aeron_async_add_counter_get_registration_id(addCounter);

        std::lock_guard<std::recursive_mutex> lock(m_adminLock);

        m_pendingCounters[registrationId] = addCounter;
        return registrationId;
    }

    /**
     * Retrieve the Counter associated with the given registrationId.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registrationId is unknown, then a nullptr is returned.
     * - If the media driver has not answered the add command, then a nullptr is returned.
     * - If the media driver has successfully added the Counter then what is returned is the Counter.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::addCounter
     * @see Aeron::addStaticCounter
     *
     * @param registrationId of the Counter returned by Aeron::addCounter
     * @return Counter associated with the registrationId
     */
    inline std::shared_ptr<Counter> findCounter(std::int64_t registrationId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_adminLock);

        auto search = m_pendingCounters.find(registrationId);
        if (search == m_pendingCounters.end())
        {
            throw IllegalArgumentException(
                std::string("Unknown registration id: ").append(std::to_string(registrationId)), SOURCEINFO);
        }

        try
        {
            std::shared_ptr<Counter> counter = findCounter(search->second);
            if (nullptr != counter)
            {
                m_pendingCounters.erase(registrationId);
            }
            return counter;
        }
        catch (...)
        {
            m_pendingCounters.erase(registrationId);
            throw;
        }
    }

    /**
     * Allocate a counter on the media driver and return a {@link Counter} for it.
     *
     * @param typeId      for the counter.
     * @param keyBuffer   containing the optional key for the counter.
     * @param keyLength   of the key in the keyBuffer.
     * @param label       for the counter.
     * @return AsyncAddCounter to find the counter
     */
    inline AsyncAddCounter *addCounterAsync(
        std::int32_t typeId,
        const std::uint8_t *keyBuffer,
        std::size_t keyLength,
        const std::string &label)
    {
        aeron_async_add_counter_t *addCounter;
        if (aeron_async_add_counter(
            &addCounter, m_aeron, typeId, keyBuffer, keyLength, label.c_str(), label.length()) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return addCounter;
    }

    /**
     * Retrieve the Counter associated with the given registrationId.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registrationId is unknown, then a nullptr is returned.
     * - If the media driver has not answered the add command, then a nullptr is returned.
     * - If the media driver has successfully added the Counter then what is returned is the Counter.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::addCounter
     * @see Aeron::addStaticCounter
     *
     * @param registrationId of the Counter returned by Aeron::addCounter
     * @return Counter associated with the registrationId
     */
    inline std::shared_ptr<Counter> findCounter(AsyncAddCounter *addCounter)
    {
        aeron_counter_t *counter;
        int result = aeron_async_add_counter_poll(&counter, addCounter);
        if (result < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        else if (0 == result)
        {
            return nullptr;
        }
        else
        {
            aeron_counter_constants_t counter_constants = {};
            aeron_counter_constants(counter, &counter_constants);
            return std::make_shared<Counter>(
                m_self.lock(), m_countersReader, counter, counter_constants.registration_id);
        }
    }

    /**
     * Gets the registration id for addition of the counter. Note that using this after a call to poll the
     * succeeds or errors is undefined behaviour. As the AsyncAddCounter may have been freed.
     * <p>
     * <em>Note:<em> The return value cannot be used to call `findPublication` method.
     *
     * @param addCounter used to check for completion.
     * @return registration id for the counter.
     */
    inline std::int64_t addCounterAsyncGetRegistrationId(AsyncAddCounter *addCounter)
    {
        return aeron_async_add_counter_get_registration_id(addCounter);
    }

    /**
     * Allocates or returns an existing static counter instance using specified <code>typeId</code> and
     * <code>registrationId</code> pair. Such counter cannot be deleted and its lifecycle is decoupled from this
     * <code>Aeron</code> instance, i.e. won't be freed when this instance is closed or times out.
     *
     * @param typeId         for the counter.
     * @param keyBuffer      containing the optional key for the counter.
     * @param keyLength      of the key in the keyBuffer.
     * @param label          for the counter.
     * @param registrationId that uniquely identifies the static counter for a given <code>typeId</code>
     * @return correlation id to resolve the AsyncAddCounter for the <code>Counter</code>.
     */
    std::int64_t addStaticCounter(
        std::int32_t typeId,
        const std::uint8_t *keyBuffer,
        std::size_t keyLength,
        const std::string &label,
        std::int64_t registrationId)
    {
        AsyncAddCounter *addCounter = addStaticCounterAsync(typeId, keyBuffer, keyLength, label, registrationId);
        std::int64_t correlationId = aeron_async_add_counter_get_registration_id(addCounter);

        std::lock_guard<std::recursive_mutex> lock(m_adminLock);

        m_pendingCounters[correlationId] = addCounter;
        return correlationId;
    }

    /**
     * Allocates or returns an existing static counter instance using specified <code>typeId</code> and
     * <code>registrationId</code> pair. Such counter cannot be deleted and its lifecycle is decoupled from this
     * <code>Aeron</code> instance, i.e. won't be freed when this instance is closed or times out.
     *
     * @param typeId         for the counter.
     * @param keyBuffer      containing the optional key for the counter.
     * @param keyLength      of the key in the keyBuffer.
     * @param label          for the counter.
     * @param registrationId that uniquely identifies the static counter for a given <code>typeId</code>.
     * @return AsyncAddCounter to find the counter.
     */
    inline AsyncAddCounter *addStaticCounterAsync(
        std::int32_t typeId,
        const std::uint8_t *keyBuffer,
        std::size_t keyLength,
        const std::string &label,
        std::int64_t registrationId)
    {
        aeron_async_add_counter_t *addCounter;
        if (aeron_async_add_static_counter(
            &addCounter, m_aeron, typeId, keyBuffer, keyLength, label.c_str(), label.length(), registrationId) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return addCounter;
    }

    /**
     * Add a handler to the list to be called when a counter becomes available.
     *
     * @param handler to be added to the available counters list.
     * @return registration id to use to remove the handler.
     */
    inline std::int64_t addAvailableCounterHandler(const on_available_counter_t &handler)
    {
        std::lock_guard<std::recursive_mutex> lock(m_adminLock);

        auto handler_ptr = std::make_shared<on_available_counter_t>(handler);

        std::int64_t registrationId = aeron_next_correlation_id(m_aeron);
        m_availableCounterHandlers.emplace_back(registrationId, handler_ptr);
        auto &storedHandler = m_availableCounterHandlers.back();

        aeron_on_available_counter_pair_t counterPair;
        counterPair.handler = onAvailableCounterCallback;
        counterPair.clientd = reinterpret_cast<void *>(storedHandler.second.get());

        if (aeron_add_available_counter_handler(m_aeron, &counterPair) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return registrationId;
    }

    /**
     * Remove a handler from the list to be called when a counter becomes available.
     *
     * @param registrationId id for the handler to be removed from the available counters list.
     */
    inline void removeAvailableCounterHandler(std::int64_t registrationId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_adminLock);

        auto &v = m_availableCounterHandlers;
        auto predicate =
            [registrationId](const std::pair<std::int64_t, std::shared_ptr<on_available_counter_t>> &item)
            {
                return item.first == registrationId;
            };

        auto storedHandler = std::find_if(v.begin(), v.end(), predicate);
        if (storedHandler == v.end())
        {
            return;
        }

        aeron_on_available_counter_pair_t counterPair;
        counterPair.handler = onAvailableCounterCallback;
        counterPair.clientd = reinterpret_cast<void *>(storedHandler->second.get());

        if (aeron_remove_available_counter_handler(m_aeron, &counterPair) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        v.erase(storedHandler);
    }

    /**
     * Add a handler to the list to be called when a counter becomes unavailable.
     *
     * @param handler to be added to the unavailable counters list.
     * @return registration id to use to remove the handler.
     */
    inline std::int64_t addUnavailableCounterHandler(const on_unavailable_counter_t &handler)
    {
        std::lock_guard<std::recursive_mutex> lock(m_adminLock);

        auto handler_ptr = std::make_shared<on_unavailable_counter_t>(handler);

        std::int64_t registrationId = aeron_next_correlation_id(m_aeron);
        m_unavailableCounterHandlers.emplace_back(registrationId, handler_ptr);
        auto &storedHandler = m_unavailableCounterHandlers.back();

        aeron_on_unavailable_counter_pair_t counterPair;
        counterPair.handler = onUnavailableCounterCallback;
        counterPair.clientd = reinterpret_cast<void *>(storedHandler.second.get());

        if (aeron_add_unavailable_counter_handler(m_aeron, &counterPair) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return registrationId;
    }

    /**
     * Remove a handler from the list to be called when a counter becomes unavailable.
     *
     * @param registrationId id for the handler to be removed from the unavailable counter handlers list.
     */
    inline void removeUnavailableCounterHandler(std::int64_t registrationId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_adminLock);

        auto &v = m_unavailableCounterHandlers;
        auto predicate =
            [registrationId](const std::pair<std::int64_t, std::shared_ptr<on_unavailable_counter_t>> &item)
            {
                return item.first == registrationId;
            };

        auto storedHandler = std::find_if(v.begin(), v.end(), predicate);
        if (storedHandler == v.end())
        {
            return;
        }

        aeron_on_unavailable_counter_pair_t counterPair;
        counterPair.handler = onUnavailableCounterCallback;
        counterPair.clientd = reinterpret_cast<void *>(storedHandler->second.get());

        if (aeron_remove_unavailable_counter_handler(m_aeron, &counterPair) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        v.erase(storedHandler);
    }

    /**
     * Add a handler to the list to be called when the client is closed.
     *
     * @param handler to be added to the close client handlers list.
     * @return registration id to use to remove the handler.
     */
    inline std::int64_t addCloseClientHandler(const on_close_client_t &handler)
    {
        std::lock_guard<std::recursive_mutex> lock(m_adminLock);
        
        std::shared_ptr<on_close_client_t> handler_ptr = std::make_shared<on_close_client_t>(handler);

        std::int64_t registrationId = aeron_next_correlation_id(m_aeron);
        m_closeClientHandlers.emplace_back(registrationId, handler_ptr);

        aeron_on_close_client_pair_t counterPair;
        counterPair.handler = onCloseClientCallback;
        counterPair.clientd = reinterpret_cast<void *>(handler_ptr.get());

        if (aeron_add_close_handler(m_aeron, &counterPair) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return registrationId;
    }

    /**
     * Remove a handler from the list to be called when the client is closed.
     *
     * @param registrationId id for the handler to be removed from the close client handlers list.
     */
    inline void removeCloseClientHandler(std::int64_t registrationId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_adminLock);

        auto &v = m_closeClientHandlers;
        auto predicate =
            [registrationId](const std::pair<std::int64_t, std::shared_ptr<on_close_client_t>> &item)
            {
                return item.first == registrationId;
            };

        auto storedHandler = std::find_if(v.begin(), v.end(), predicate);
        if (storedHandler == v.end())
        {
            return;
        }

        aeron_on_close_client_pair_t counterPair;
        counterPair.handler = onCloseClientCallback;
        counterPair.clientd = reinterpret_cast<void *>(storedHandler->second.get());

        if (aeron_remove_close_handler(m_aeron, &counterPair) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        v.erase(storedHandler);
    }

    /**
     * Return the AgentInvoker for the client conductor.
     *
     * @return AgentInvoker for the conductor.
     */
    inline AgentInvoker<ClientConductor> &conductorAgentInvoker()
    {
        if (!usesAgentInvoker())
        {
            throw IllegalStateException("Not configured to use agent invoker", SOURCEINFO);
        }

        m_conductorInvoker.start();
        return m_conductorInvoker;
    }

    /**
     * Return whether the AgentInvoker is used or not.
     *
     * @return true if AgentInvoker used or false if not.
     */
    inline bool usesAgentInvoker() const
    {
        return m_context.m_useConductorAgentInvoker;
    }

    /**
     * Get the CountersReader for the Aeron media driver counters.
     *
     * @return CountersReader for the Aeron media driver in use.
     */
    inline CountersReader &countersReader()
    {
        return m_countersReader;
    }

    /**
     * Get the client identity that has been allocated for communicating with the media driver.
     *
     * @return the client identity that has been allocated for communicating with the media driver.
     */
    inline std::int64_t clientId() const
    {
        return aeron_client_id(m_aeron);
    }

    /**
     * Get the Aeron Context object used in construction of the Aeron instance.
     *
     * @return Context instance in use.
     */
    inline Context &context()
    {
        return m_context;
    }

    inline const Context &context() const
    {
        return m_context;
    }

    /**
     * Get the underlying C Aeron client. Applications should not need to use this method.
     *
     * @return the underlying C Aeron client.
     */
    inline aeron_t *aeron() const
    {
        return m_aeron;
    }

    /**
     * Return the static version and build string for the binary library.
     *
     * @return static version and build string for the binary library.
     */
    static std::string version()
    {
        return { aeron_version_full() };
    }


private:
    Context m_context;
    aeron_context_t *m_aeron_context;
    aeron_t *m_aeron = nullptr;
    CountersReader m_countersReader;
    std::unordered_map<std::int64_t, AsyncAddPublication *> m_pendingPublications;
    std::unordered_map<std::int64_t, AsyncAddExclusivePublication *> m_pendingExclusivePublications;
    std::unordered_map<std::int64_t, AsyncAddSubscription *> m_pendingSubscriptions;
    std::unordered_map<std::int64_t, AsyncAddCounter *> m_pendingCounters;
    std::vector<std::pair<std::int64_t, std::shared_ptr<on_available_counter_t>>> m_availableCounterHandlers;
    std::vector<std::pair<std::int64_t, std::shared_ptr<on_unavailable_counter_t>>> m_unavailableCounterHandlers;
    std::vector<std::pair<std::int64_t, std::shared_ptr<on_close_client_t>>> m_closeClientHandlers;
    std::recursive_mutex m_adminLock;
    ClientConductor m_clientConductor;
    AgentInvoker<ClientConductor> m_conductorInvoker;
    std::weak_ptr<Aeron> m_self; // used to ensure destruction order

    static aeron_t *init_aeron(Context &context, aeron_context_t **aeron_context)
    {
        aeron_context_t *_context;
        aeron_context_init(&_context);

        aeron_t *aeron;
        try
        {
            context.attachCallbacksToContext(_context);
        }
        catch (IllegalArgumentException&)
        {
            aeron_context_close(_context);
            throw;
        }

        if (aeron_init(&aeron, _context) < 0)
        {
            aeron_context_close(_context);
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        *aeron_context = _context;

        return aeron;
    }

    static void onAvailableImageCallback(void *clientd, aeron_subscription_t *subscription, aeron_image_t *image)
    {
        on_available_image_t &callback = *reinterpret_cast<on_available_image_t *>(clientd);
        Image imageWrapper(subscription, image);
        callback(imageWrapper);
    }

    static void onUnavailableImageCallback(void *clientd, aeron_subscription_t *subscription, aeron_image_t *image)
    {
        on_unavailable_image_t &callback = *reinterpret_cast<on_unavailable_image_t *>(clientd);
        Image imageWrapper(subscription, image);
        callback(imageWrapper);
    }

    static void onAvailableCounterCallback(
        void *clientd, aeron_counters_reader_t *counters_reader, std::int64_t registration_id, std::int32_t counter_id)
    {
        CountersReader reader = CountersReader(counters_reader);
        on_available_counter_t &callback = *reinterpret_cast<on_available_counter_t *>(clientd);
        callback(reader, registration_id, counter_id);
    }

    static void onUnavailableCounterCallback(
        void *clientd, aeron_counters_reader_t *counters_reader, std::int64_t registration_id, std::int32_t counter_id)
    {
        CountersReader reader = CountersReader(counters_reader);
        on_unavailable_counter_t &callback = *reinterpret_cast<on_unavailable_counter_t *>(clientd);
        callback(reader, registration_id, counter_id);
    }

    static void onCloseClientCallback(void *clientd)
    {
        on_close_client_t &callback = *reinterpret_cast<on_close_client_t *>(clientd);
        callback();
    }

    static void emptyCallback(void * /* clientd */)
    {
    }
};
}

#endif
