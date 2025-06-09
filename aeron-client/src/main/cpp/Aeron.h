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

#include "ClientConductor.h"
#include "concurrent/SleepingIdleStrategy.h"
#include "concurrent/AgentRunner.h"
#include "concurrent/AgentInvoker.h"
#include "util/Export.h"

#if !defined(AERON_HIDE_DEPRECATION_MESSAGE)
#pragma message ( "The C++ API will be removed in 1.50.0! Please move to using the C++ Wrapper API instead." )
#endif

/// Top namespace for Aeron C++ API
namespace aeron
{

using namespace aeron::util;
using namespace aeron::concurrent;
using namespace aeron::concurrent::broadcast;

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
class CLIENT_EXPORT Aeron
{
public:
    /**
     * Create an Aeron instance and connect to the media driver.
     * <p>
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     *
     * @param context for configuration of the client.
     */
    explicit Aeron(Context &context);

    ~Aeron();

    /**
     * Indicate if the instance is closed and can not longer be used.
     *
     * @return true is the instance is closed and can no longer be used, otherwise false.
     */
    inline bool isClosed()
    {
        return m_conductor.isClosed();
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
        return std::make_shared<Aeron>(context);
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

        return std::make_shared<Aeron>(ctx);
    }

    /**
     * Add a {@link Publication} for publishing messages to subscribers
     *
     * This function returns immediately and does not wait for the response from the media driver. The returned
     * registration id is to be used to determine the status of the command with the media driver.
     *
     * @param channel for sending the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return registration id for the publication
     */
    inline std::int64_t addPublication(const std::string &channel, std::int32_t streamId)
    {
        return m_conductor.addPublication(channel, streamId);
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
        return m_conductor.findPublication(registrationId);
    }

    /**
     * Add an {@link ExclusivePublication} for publishing messages to subscribers from a single thread.
     *
     * @param channel  for sending the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return registration id for the publication
     */
    inline std::int64_t addExclusivePublication(const std::string &channel, std::int32_t streamId)
    {
        return m_conductor.addExclusivePublication(channel, streamId);
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
     * - If the media driver has successfully added the ExclusivePublication then what is returned is the ExclusivePublication.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::addExclusivePublication
     *
     * @param registrationId of the ExclusivePublication returned by Aeron::addExclusivePublication
     * @return ExclusivePublication associated with the registrationId
     */
    inline std::shared_ptr<ExclusivePublication> findExclusivePublication(std::int64_t registrationId)
    {
        return m_conductor.findExclusivePublication(registrationId);
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
        return m_conductor.addSubscription(
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
     * @return registration id for the subscription
     */
    inline std::int64_t addSubscription(
        const std::string &channel,
        std::int32_t streamId,
        const on_available_image_t &onAvailableImageHandler,
        const on_unavailable_image_t &onUnavailableImageHandler)
    {
        return m_conductor.addSubscription(channel, streamId, onAvailableImageHandler, onUnavailableImageHandler);
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
        return m_conductor.findSubscription(registrationId);
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
        m_conductor.ensureOpen();
        return m_toDriverRingBuffer.nextCorrelationId();
    }

    /**
     * Allocate a counter on the media driver and return a {@link Counter} for it.
     *
     * The typeId should be 1000 or greater. Values lower than that are reserved for use by Aeron.
     *
     * @param typeId      for the counter.
     * @param keyBuffer   containing the optional key for the counter.
     * @param keyLength   of the key in the keyBuffer.
     * @param label       for the counter.
     * @return registration id for the Counter
     */
    inline std::int64_t addCounter(
        std::int32_t typeId,
        const std::uint8_t *keyBuffer,
        std::size_t keyLength,
        const std::string &label)
    {
        return m_conductor.addCounter(typeId, keyBuffer, keyLength, label);
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
     *
     * @param registrationId of the Counter returned by Aeron::addCounter
     * @return Counter associated with the registrationId
     */
    inline std::shared_ptr<Counter> findCounter(std::int64_t registrationId)
    {
        return m_conductor.findCounter(registrationId);
    }

    /**
     * Add a handler to the list to be called when a counter becomes available.
     *
     * @param handler to be added to the available counters list.
     * @return registration id to use to remove the handler.
     */
    inline std::int64_t addAvailableCounterHandler(const on_available_counter_t &handler)
    {
        return m_conductor.addAvailableCounterHandler(handler);
    }

    /**
     * Remove a handler from the list to be called when a counter becomes available.
     *
     * @param handler to be removed from the available counter handlers list.
     * @deprecated Use Aeron::removeAvailableCounterHandler(int64_t)
     */
    inline void removeAvailableCounterHandler(const on_available_counter_t &handler)
    {
        m_conductor.removeAvailableCounterHandler(handler);
    }

    /**
     * Remove a handler from the list to be called when a counter becomes available.
     *
     * @param registrationId id for the handler to be removed from the available counters list.
     */
    inline void removeAvailableCounterHandler(std::int64_t registrationId)
    {
        m_conductor.removeAvailableCounterHandler(registrationId);
    }

    /**
     * Add a handler to the list to be called when a counter becomes unavailable.
     *
     * @param handler to be added to the unavailable counters list.
     * @return registration id to use to remove the handler.
     */
    inline std::int64_t addUnavailableCounterHandler(const on_unavailable_counter_t &handler)
    {
        return m_conductor.addUnavailableCounterHandler(handler);
    }

    /**
     * Remove a handler from the list to be called when a counter becomes unavailable.
     *
     * @param handler to be removed from the unavailable counters list.
     * @deprecated Use Aeron::removeUnavailableCounterHandler(int64_t)
     */
    inline void removeUnavailableCounterHandler(const on_unavailable_counter_t &handler)
    {
        m_conductor.removeUnavailableCounterHandler(handler);
    }

    /**
     * Remove a handler from the list to be called when a counter becomes unavailable.
     *
     * @param registrationId id for the handler to be removed from the unavailable counter handlers list.
     */
    inline void removeUnavailableCounterHandler(std::int64_t registrationId)
    {
        m_conductor.removeUnavailableCounterHandler(registrationId);
    }

    /**
     * Add a handler to the list to be called when the client is closed.
     *
     * @param handler to be added to the close client handlers list.
     * @return registration id to use to remove the handler.
     */
    inline std::int64_t addCloseClientHandler(const on_close_client_t &handler)
    {
        return m_conductor.addCloseClientHandler(handler);
    }

    /**
     * Remove a handler from the list to be called when the client is closed.
     *
     * @param handler to be removed from the close client handlers list.
     * @deprecated Use Aeron::removeCloseClientHandler(int64_t)
     */
    inline void removeCloseClientHandler(const on_close_client_t &handler)
    {
        m_conductor.removeCloseClientHandler(handler);
    }

    /**
     * Remove a handler from the list to be called when the client is closed.
     *
     * @param registrationId id for the handler to be removed from the close client handlers list.
     */
    inline void removeCloseClientHandler(std::int64_t registrationId)
    {
        m_conductor.removeCloseClientHandler(registrationId);
    }

    /**
     * Return the AgentInvoker for the client conductor.
     *
     * @return AgentInvoker for the conductor.
     */
    inline AgentInvoker<ClientConductor> &conductorAgentInvoker()
    {
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
        return m_conductor.countersReader();
    }

    /**
     * Get the client identity that has been allocated for communicating with the media driver.
     *
     * @return the client identity that has been allocated for communicating with the media driver.
     */
    inline std::int64_t clientId() const
    {
        return m_driverProxy.clientId();
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
     * Return the static version and build string for the binary library.
     *
     * @return static version and build string for the binary library.
     */
    static std::string version();

private:
    Context m_context;
    MemoryMappedFile::ptr_t m_cncBuffer;

    AtomicBuffer m_toDriverAtomicBuffer;
    AtomicBuffer m_toClientsAtomicBuffer;
    AtomicBuffer m_countersMetadataBuffer;
    AtomicBuffer m_countersValueBuffer;

    ManyToOneRingBuffer m_toDriverRingBuffer;
    DriverProxy m_driverProxy;
    BroadcastReceiver m_toClientsBroadcastReceiver;
    CopyBroadcastReceiver m_toClientsCopyReceiver;

    ClientConductor m_conductor;
    SleepingIdleStrategy m_idleStrategy;
    AgentRunner<ClientConductor, SleepingIdleStrategy> m_conductorRunner;
    AgentInvoker<ClientConductor> m_conductorInvoker;

    static MemoryMappedFile::ptr_t mapCncFile(Context &context);
};

}

#endif
