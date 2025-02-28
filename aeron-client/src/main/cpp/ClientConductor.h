/*
 * Copyright 2014-2024 Real Logic Limited.
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

#ifndef AERON_CLIENT_CONDUCTOR_H
#define AERON_CLIENT_CONDUCTOR_H

#include <chrono>
#include <unordered_map>
#include <vector>
#include <mutex>
#include "util/LangUtil.h"
#include "util/ScopeUtils.h"
#include "Publication.h"
#include "ExclusivePublication.h"
#include "Subscription.h"
#include "Counter.h"
#include "DriverProxy.h"
#include "Context.h"
#include "DriverListenerAdapter.h"
#include "LogBuffers.h"
#include "HeartbeatTimestamp.h"
#include "util/Export.h"
#include "AeronVersion.h"

namespace aeron
{

using namespace aeron::concurrent::logbuffer;
using namespace aeron::concurrent::status;
using namespace aeron::concurrent;
using namespace aeron::util;

typedef std::function<long long()> epoch_clock_t;
typedef std::function<long long()> nano_clock_t;

static const long KEEPALIVE_TIMEOUT_MS = 500;
static const long RESOURCE_TIMEOUT_MS = 1000;
static const std::size_t static_handler_token = 0;

class CLIENT_EXPORT ClientConductor
{
public:

    ClientConductor(
        epoch_clock_t epochClock,
        DriverProxy &driverProxy,
        CopyBroadcastReceiver &broadcastReceiver,
        AtomicBuffer &counterMetadataBuffer,
        AtomicBuffer &counterValuesBuffer,
        const on_new_publication_t &newPublicationHandler,
        const on_new_publication_t &newExclusivePublicationHandler,
        const on_new_subscription_t &newSubscriptionHandler,
        const exception_handler_t &errorHandler,
        const on_available_counter_t &availableCounterHandler,
        const on_unavailable_counter_t &unavailableCounterHandler,
        const on_close_client_t &onCloseClientHandler,
        long driverTimeoutMs,
        long resourceLingerTimeoutMs,
        long long interServiceTimeoutNs,
        bool preTouchMappedMemory,
        std::string clientName) :
        m_driverProxy(driverProxy),
        m_driverListenerAdapter(broadcastReceiver, *this),
        m_countersReader(counterMetadataBuffer, counterValuesBuffer),
        m_counterValuesBuffer(counterValuesBuffer),
        m_onNewPublicationHandler(newPublicationHandler),
        m_onNewExclusivePublicationHandler(newExclusivePublicationHandler),
        m_onNewSubscriptionHandler(newSubscriptionHandler),
        m_errorHandler(errorHandler),
        m_epochClock(std::move(epochClock)),
        m_clientName(clientName),
        m_driverTimeoutMs(driverTimeoutMs),
        m_resourceLingerTimeoutMs(resourceLingerTimeoutMs),
        m_interServiceTimeoutMs(static_cast<long>(interServiceTimeoutNs / 1000000)),
        m_preTouchMappedMemory(preTouchMappedMemory),
        m_timeOfLastDoWorkMs(m_epochClock()),
        m_timeOfLastKeepaliveMs(m_timeOfLastDoWorkMs),
        m_timeOfLastCheckManagedResourcesMs(m_timeOfLastDoWorkMs)
    {
        static_cast<void>(m_padding);

        m_onAvailableCounterHandlers.emplace_back(std::make_pair(static_handler_token, availableCounterHandler));
        m_onUnavailableCounterHandlers.emplace_back(std::make_pair(static_handler_token, unavailableCounterHandler));
        m_onCloseClientHandlers.emplace_back(std::make_pair(static_handler_token, onCloseClientHandler));
    }

    ~ClientConductor();

    void onStart();

    int doWork();

    void onClose();

    std::int64_t addPublication(const std::string &channel, std::int32_t streamId);

    std::shared_ptr<Publication> findPublication(std::int64_t registrationId);

    void releasePublication(std::int64_t registrationId);

    std::int64_t addExclusivePublication(const std::string &channel, std::int32_t streamId);

    std::shared_ptr<ExclusivePublication> findExclusivePublication(std::int64_t registrationId);

    void releaseExclusivePublication(std::int64_t registrationId);

    std::int64_t addSubscription(
        const std::string &channel,
        std::int32_t streamId,
        const on_available_image_t &onAvailableImageHandler,
        const on_unavailable_image_t &onUnavailableImageHandler);

    std::shared_ptr<Subscription> findSubscription(std::int64_t registrationId);

    void releaseSubscription(std::int64_t registrationId, Image::array_t imageArray, std::size_t length);

    std::int64_t addCounter(
        std::int32_t typeId,
        const std::uint8_t *keyBuffer,
        std::size_t keyLength,
        const std::string &label);

    std::shared_ptr<Counter> findCounter(std::int64_t registrationId);

    void releaseCounter(std::int64_t registrationId);

    bool findDestinationResponse(std::int64_t correlationId);

    void onNewPublication(
        std::int64_t registrationId,
        std::int64_t originalRegistrationId,
        std::int32_t streamId,
        std::int32_t sessionId,
        std::int32_t publicationLimitCounterId,
        std::int32_t channelStatusIndicatorId,
        const std::string &logFilename);

    void onNewExclusivePublication(
        std::int64_t registrationId,
        std::int64_t originalRegistrationId,
        std::int32_t streamId,
        std::int32_t sessionId,
        std::int32_t publicationLimitCounterId,
        std::int32_t channelStatusIndicatorId,
        const std::string &logFilename);

    void onSubscriptionReady(std::int64_t registrationId, std::int32_t channelStatusId);

    void onOperationSuccess(std::int64_t correlationId);

    void onChannelEndpointErrorResponse(std::int32_t channelStatusId, const std::string &errorMessage);

    void onErrorResponse(
        std::int64_t offendingCommandCorrelationId, std::int32_t errorCode, const std::string &errorMessage);

    void onAvailableImage(
        std::int64_t correlationId,
        std::int32_t sessionId,
        std::int32_t subscriberPositionId,
        std::int64_t subscriptionRegistrationId,
        const std::string &logFilename,
        const std::string &sourceIdentity);

    void onUnavailableImage(std::int64_t correlationId, std::int64_t subscriptionRegistrationId);

    void onAvailableCounter(std::int64_t registrationId, std::int32_t counterId);

    void onUnavailableCounter(std::int64_t registrationId, std::int32_t counterId);

    void onClientTimeout(std::int64_t clientId);

    void closeAllResources(long long nowMs);

    std::int64_t addDestination(std::int64_t publicationRegistrationId, const std::string &endpointChannel);

    std::int64_t removeDestination(std::int64_t publicationRegistrationId, const std::string &endpointChannel);

    std::int64_t addRcvDestination(std::int64_t subscriptionRegistrationId, const std::string &endpointChannel);

    std::int64_t removeRcvDestination(std::int64_t subscriptionRegistrationId, const std::string &endpointChannel);

    std::int64_t addAvailableCounterHandler(const on_available_counter_t &handler);

    void removeAvailableCounterHandler(const on_available_counter_t &handler);

    void removeAvailableCounterHandler(std::int64_t registrationId);

    std::int64_t addUnavailableCounterHandler(const on_unavailable_counter_t &handler);

    void removeUnavailableCounterHandler(const on_unavailable_counter_t &handler);

    void removeUnavailableCounterHandler(std::int64_t registrationId);

    std::int64_t addCloseClientHandler(const on_close_client_t &handler);

    void removeCloseClientHandler(const on_close_client_t &handler);

    void removeCloseClientHandler(std::int64_t registrationId);

    inline CountersReader &countersReader()
    {
        ensureOpen();
        return m_countersReader;
    }

    inline std::int64_t channelStatus(std::int32_t counterId) const
    {
        switch (counterId)
        {
            case 0:
                return ChannelEndpointStatus::CHANNEL_ENDPOINT_INITIALIZING;

            case ChannelEndpointStatus::NO_ID_ALLOCATED:
                return ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE;

            default:
                return m_countersReader.getCounterValue(counterId);
        }
    }

    inline bool isClosed() const
    {
        return m_isClosed.load(std::memory_order_acquire);
    }

    inline void ensureOpen() const
    {
        if (isClosed())
        {
            throw AeronException("Aeron client conductor is closed", SOURCEINFO);
        }
    }

private:
    enum class RegistrationStatus : std::int8_t
    {
        AWAITING_MEDIA_DRIVER, REGISTERED_MEDIA_DRIVER, ERRORED_MEDIA_DRIVER
    };

    struct PublicationStateDefn
    {
        std::string m_errorMessage;
        std::shared_ptr<LogBuffers> m_buffers;
        std::weak_ptr<Publication> m_publication;
        const std::string m_channel;
        const std::int64_t m_registrationId;
        std::int64_t m_originalRegistrationId = -1;
        const long long m_timeOfRegistrationMs;
        const std::int32_t m_streamId;
        std::int32_t m_sessionId = -1;
        std::int32_t m_publicationLimitCounterId = -1;
        std::int32_t m_channelStatusId = -1;
        std::int32_t m_errorCode = -1;
        RegistrationStatus m_status = RegistrationStatus::AWAITING_MEDIA_DRIVER;

        inline PublicationStateDefn(
            const std::string &channel, std::int64_t registrationId, std::int32_t streamId, long long nowMs) :
            m_channel(channel),
            m_registrationId(registrationId),
            m_timeOfRegistrationMs(nowMs),
            m_streamId(streamId)
        {
        }
    };

    struct ExclusivePublicationStateDefn
    {
        std::string m_errorMessage;
        std::shared_ptr<LogBuffers> m_buffers;
        std::weak_ptr<ExclusivePublication> m_publication;
        const std::string m_channel;
        const std::int64_t m_registrationId;
        const long long m_timeOfRegistrationMs;
        const std::int32_t m_streamId;
        std::int32_t m_sessionId = -1;
        std::int32_t m_publicationLimitCounterId = -1;
        std::int32_t m_channelStatusId = -1;
        std::int32_t m_errorCode = -1;
        RegistrationStatus m_status = RegistrationStatus::AWAITING_MEDIA_DRIVER;

        inline ExclusivePublicationStateDefn(
            const std::string &channel, std::int64_t registrationId, std::int32_t streamId, long long nowMs) :
            m_channel(channel),
            m_registrationId(registrationId),
            m_timeOfRegistrationMs(nowMs),
            m_streamId(streamId)
        {
        }
    };

    struct SubscriptionStateDefn
    {
        std::string m_errorMessage;
        std::shared_ptr<Subscription> m_subscriptionCache;
        std::weak_ptr<Subscription> m_subscription;
        on_available_image_t m_onAvailableImageHandler;
        on_unavailable_image_t m_onUnavailableImageHandler;
        const std::string m_channel;
        const std::int64_t m_registrationId;
        const long long m_timeOfRegistrationMs;
        const std::int32_t m_streamId;
        std::int32_t m_errorCode = -1;
        RegistrationStatus m_status = RegistrationStatus::AWAITING_MEDIA_DRIVER;

        inline SubscriptionStateDefn(
            const std::string &channel,
            std::int64_t registrationId,
            std::int32_t streamId,
            long long nowMs,
            const on_available_image_t &onAvailableImageHandler,
            const on_unavailable_image_t &onUnavailableImageHandler) :
            m_onAvailableImageHandler(onAvailableImageHandler),
            m_onUnavailableImageHandler(onUnavailableImageHandler),
            m_channel(channel),
            m_registrationId(registrationId),
            m_timeOfRegistrationMs(nowMs),
            m_streamId(streamId)
        {
        }
    };

    struct CounterStateDefn
    {
        std::string m_errorMessage;
        std::shared_ptr<Counter> m_counterCache;
        std::weak_ptr<Counter> m_counter;
        const std::int64_t m_registrationId;
        const long long m_timeOfRegistrationMs;
        std::int32_t m_counterId = -1;
        RegistrationStatus m_status = RegistrationStatus::AWAITING_MEDIA_DRIVER;
        std::int32_t m_errorCode = -1;

        inline CounterStateDefn(std::int64_t registrationId, long long nowMs) :
            m_registrationId(registrationId),
            m_timeOfRegistrationMs(nowMs)
        {
        }
    };

    struct ImageListLingerDefn
    {
        Image::array_t m_imageArray;
        long long m_timeOfLastStateChangeMs = LLONG_MAX;

        inline ImageListLingerDefn(long long nowMs, Image::array_t imageArray) :
            m_imageArray(imageArray),
            m_timeOfLastStateChangeMs(nowMs)
        {
        }
    };

    struct LogBuffersDefn
    {
        std::shared_ptr<LogBuffers> m_logBuffers;
        long long m_timeOfLastStateChangeMs;

        inline explicit LogBuffersDefn(std::shared_ptr<LogBuffers> buffers) :
            m_logBuffers(std::move(buffers)),
            m_timeOfLastStateChangeMs(LLONG_MAX)
        {
        }
    };

    struct DestinationStateDefn
    {
        std::string m_errorMessage;
        const std::int64_t m_correlationId;
        const std::int64_t m_registrationId;
        const long long m_timeOfRegistrationMs;
        std::int32_t m_errorCode = -1;
        RegistrationStatus m_status = RegistrationStatus::AWAITING_MEDIA_DRIVER;

        inline DestinationStateDefn(std::int64_t correlationId, std::int64_t registrationId, long long nowMs) :
            m_correlationId(correlationId),
            m_registrationId(registrationId),
            m_timeOfRegistrationMs(nowMs)
        {
        }
    };

    std::unordered_map<std::int64_t, PublicationStateDefn> m_publicationByRegistrationId;
    std::unordered_map<std::int64_t, ExclusivePublicationStateDefn> m_exclusivePublicationByRegistrationId;
    std::unordered_map<std::int64_t, SubscriptionStateDefn> m_subscriptionByRegistrationId;
    std::unordered_map<std::int64_t, CounterStateDefn> m_counterByRegistrationId;
    std::unordered_map<std::int64_t, DestinationStateDefn> m_destinationStateByCorrelationId;

    std::unordered_map<std::int64_t, LogBuffersDefn> m_logBuffersByRegistrationId;
    std::vector<ImageListLingerDefn> m_lingeringImageLists;

    DriverProxy &m_driverProxy;
    DriverListenerAdapter<ClientConductor> m_driverListenerAdapter;

    CountersReader m_countersReader;
    AtomicBuffer &m_counterValuesBuffer;

    on_new_publication_t m_onNewPublicationHandler;
    on_new_publication_t m_onNewExclusivePublicationHandler;
    on_new_subscription_t m_onNewSubscriptionHandler;
    exception_handler_t m_errorHandler;

    std::vector<std::pair<std::int64_t, on_available_counter_t>> m_onAvailableCounterHandlers;
    std::vector<std::pair<std::int64_t, on_unavailable_counter_t>> m_onUnavailableCounterHandlers;
    std::vector<std::pair<std::int64_t, on_close_client_t>> m_onCloseClientHandlers;

    epoch_clock_t m_epochClock;
    std::string m_clientName;
    long m_driverTimeoutMs;
    long m_resourceLingerTimeoutMs;
    long m_interServiceTimeoutMs;
    bool m_preTouchMappedMemory;
    bool m_isInCallback = false;
    std::atomic<bool> m_driverActive = { true };
    std::atomic<bool> m_isClosed = { false };
    std::recursive_mutex m_adminLock;
    std::unique_ptr<AtomicCounter> m_heartbeatTimestamp;

    long long m_timeOfLastDoWorkMs;
    long long m_timeOfLastKeepaliveMs;
    long long m_timeOfLastCheckManagedResourcesMs;

    char m_padding[util::BitUtil::CACHE_LINE_LENGTH] = {};

    inline int onHeartbeatCheckTimeouts()
    {
        const long long nowMs = m_epochClock();
        int result = 0;

        if (nowMs > (m_timeOfLastDoWorkMs + m_interServiceTimeoutMs))
        {
            closeAllResources(nowMs);

            ConductorServiceTimeoutException exception(
                "timeout between service calls over " + std::to_string(m_interServiceTimeoutMs) + " ms", SOURCEINFO);
            m_errorHandler(exception);
        }

        m_timeOfLastDoWorkMs = nowMs;

        if (nowMs > (m_timeOfLastKeepaliveMs + KEEPALIVE_TIMEOUT_MS))
        {
            int64_t lastKeepaliveMs = m_driverProxy.timeOfLastDriverKeepalive();
            if (nowMs > (lastKeepaliveMs + m_driverTimeoutMs))
            {
                m_driverActive = false;
                closeAllResources(nowMs);

                if (NULL_VALUE == lastKeepaliveMs)
                {
                    DriverTimeoutException exception("MediaDriver has been shutdown", SOURCEINFO);
                    m_errorHandler(exception);
                }
                else
                {
                    DriverTimeoutException exception(
                        "MediaDriver keepalive: age=" + std::to_string(nowMs - lastKeepaliveMs) +
                        "ms > timeout=" + std::to_string(m_driverTimeoutMs) + "ms", SOURCEINFO);
                    m_errorHandler(exception);
                }

                return 1;
            }

            if (m_heartbeatTimestamp)
            {
                if (HeartbeatTimestamp::isActive(
                    m_countersReader,
                    m_heartbeatTimestamp->id(),
                    HeartbeatTimestamp::CLIENT_HEARTBEAT_TYPE_ID,
                    m_driverProxy.clientId()))
                {
                    m_heartbeatTimestamp->setOrdered(nowMs);
                }
                else
                {
                    closeAllResources(nowMs);

                    AeronException exception("client heartbeat timestamp not active", SOURCEINFO);
                    m_errorHandler(exception);
                }
            }
            else
            {
                std::int32_t counterId = HeartbeatTimestamp::findCounterIdByRegistrationId(
                    m_countersReader, HeartbeatTimestamp::CLIENT_HEARTBEAT_TYPE_ID, m_driverProxy.clientId());

                if (CountersReader::NULL_COUNTER_ID != counterId)
                {
                    index_t labelLengthOffset =
                        CountersReader::metadataOffset(counterId) + CountersReader::LABEL_LENGTH_OFFSET;
                    int32_t labelLength = m_countersReader.metaDataBuffer().getInt32(labelLengthOffset);
                    std::string extendedInfo = std::string(" name=") + m_clientName + " version=" + AeronVersion::text() +
                        " commit=" + AeronVersion::gitSha();
                    int32_t copyLength = std::min((int32_t)extendedInfo.length(), CountersReader::MAX_LABEL_LENGTH - labelLength);
                    m_countersReader.metaDataBuffer().putStringWithoutLength(
                        labelLengthOffset + (int32_t)sizeof(int32_t) + labelLength, extendedInfo.substr(0, copyLength));
                    m_countersReader.metaDataBuffer().putInt32(
                        labelLengthOffset, labelLength + copyLength);
                    
                    m_heartbeatTimestamp.reset(new AtomicCounter(m_counterValuesBuffer, counterId));
                    m_heartbeatTimestamp->setOrdered(nowMs);
                }
            }

            m_timeOfLastKeepaliveMs = nowMs;
            result = 1;
        }

        if (nowMs > (m_timeOfLastCheckManagedResourcesMs + RESOURCE_TIMEOUT_MS))
        {
            onCheckManagedResources(nowMs);
            m_timeOfLastCheckManagedResourcesMs = nowMs;
            result = 1;
        }

        return result;
    }

    inline void verifyDriverIsActive()
    {
        if (!m_driverActive)
        {
            throw DriverTimeoutException("driver is inactive", SOURCEINFO);
        }
    }

    inline void verifyDriverIsActiveViaErrorHandler()
    {
        if (!m_driverActive)
        {
            DriverTimeoutException exception("driver is inactive", SOURCEINFO);
            m_errorHandler(exception);
        }
    }

    inline void ensureNotReentrant()
    {
        if (m_isInCallback)
        {
            ReentrantException exception("client cannot be invoked within callback", SOURCEINFO);
            m_errorHandler(exception);
        }
    }

    inline std::shared_ptr<LogBuffers> getLogBuffers(
        std::int64_t registrationId, const std::string &logFilename, const std::string &channel)
    {
        auto it = m_logBuffersByRegistrationId.find(registrationId);
        if (it == m_logBuffersByRegistrationId.end())
        {
            auto logBuffers = std::make_shared<LogBuffers>(logFilename.c_str(), m_preTouchMappedMemory);
            m_logBuffersByRegistrationId.insert(std::pair<std::int64_t, LogBuffersDefn>(
                registrationId, LogBuffersDefn(logBuffers)));

            return logBuffers;
        }
        else
        {
            it->second.m_timeOfLastStateChangeMs = LLONG_MAX;
            return it->second.m_logBuffers;
        }
    }

    void onCheckManagedResources(long long nowMs);

    void lingerResource(long long nowMs, Image::array_t imageArray);

    void lingerAllResources(long long nowMs, Image::array_t imageArray);
};

inline long long currentTimeMillis()
{
    using namespace std::chrono;

    system_clock::time_point now = system_clock::now();
    milliseconds ms = duration_cast<milliseconds>(now.time_since_epoch());

    return ms.count();
}

inline long long systemNanoClock()
{
    using namespace std::chrono;

    high_resolution_clock::time_point now = high_resolution_clock::now();
    nanoseconds ns = duration_cast<nanoseconds>(now.time_since_epoch());

    return ns.count();
}

}

#endif
