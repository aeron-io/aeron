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
package io.aeron;

import io.aeron.command.PublicationErrorFrameFlyweight;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.ChannelEndpointException;
import io.aeron.exceptions.ClientTimeoutException;
import io.aeron.exceptions.ConductorServiceTimeoutException;
import io.aeron.exceptions.DriverTimeoutException;
import io.aeron.exceptions.RegistrationException;
import io.aeron.status.ChannelEndpointStatus;
import io.aeron.status.HeartbeatTimestamp;
import io.aeron.status.PublicationErrorFrame;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.SemanticVersion;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.agrona.concurrent.status.UnsafeBufferPosition;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.AeronCounters.DRIVER_SYSTEM_COUNTER_TYPE_ID;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_CONTROL_PROTOCOL_VERSION;
import static io.aeron.AeronCounters.appendToLabel;
import static io.aeron.AeronCounters.formatVersionInfo;
import static io.aeron.ErrorCode.CHANNEL_ENDPOINT_ERROR;
import static io.aeron.status.HeartbeatTimestamp.HEARTBEAT_TYPE_ID;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;
import static org.agrona.concurrent.status.CountersReader.RECORD_ALLOCATED;

/**
 * Client conductor receives responses and notifications from Media Driver and acts on them in addition to forwarding
 * commands from the Client API to the Media Driver conductor.
 */
final class ClientConductor implements Agent
{
    private static final long NO_CORRELATION_ID = NULL_VALUE;
    private static final long EXPLICIT_CLOSE_LINGER_NS = TimeUnit.SECONDS.toNanos(1);
    static final int CONTROL_PROTOCOL_VERSION_WITH_NEXT_AVAILABLE_SESSION_ID_COMMAND = SemanticVersion.compose(1, 0, 0);

    final int controlProtocolVersion;
    private final long idleSleepDurationNs;
    private final long keepAliveIntervalNs;
    private final long driverTimeoutMs;
    private final long driverTimeoutNs;
    private final long interServiceTimeoutNs;
    private long timeOfLastKeepAliveNs;
    private long timeOfLastServiceNs;
    private boolean isClosed;
    private boolean isInCallback;
    private boolean isTerminating;
    private RegistrationException driverException;

    private final Aeron.Context ctx;
    private final Aeron aeron;
    private final Lock clientLock;
    private final EpochClock epochClock;
    private final NanoClock nanoClock;
    private final IdleStrategy awaitingIdleStrategy;
    private final DriverEventsAdapter driverEventsAdapter;
    private final LogBuffersFactory logBuffersFactory;
    private final Long2ObjectHashMap<LogBuffers> logBuffersByIdMap = new Long2ObjectHashMap<>();
    private final ArrayList<LogBuffers> lingeringLogBuffers = new ArrayList<>();
    final Long2ObjectHashMap<Object> resourceByRegIdMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<RegistrationException> asyncExceptionByRegIdMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<String> stashedChannelByRegistrationId = new Long2ObjectHashMap<>();
    final LongHashSet asyncCommandIdSet = new LongHashSet();
    private final AvailableImageHandler defaultAvailableImageHandler;
    private final UnavailableImageHandler defaultUnavailableImageHandler;
    private final Long2ObjectHashMap<AvailableCounterHandler> availableCounterHandlerById = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<UnavailableCounterHandler> unavailableCounterHandlerById =
        new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<Runnable> closeHandlerByIdMap = new Long2ObjectHashMap<>();
    private final DriverProxy driverProxy;
    private final AgentInvoker driverAgentInvoker;
    private final UnsafeBuffer counterValuesBuffer;
    private final CountersReader countersReader;
    private final PublicationErrorFrame publicationErrorFrame = new PublicationErrorFrame();
    private AtomicCounter heartbeatTimestamp;
    private long lastResponseValue;

    ClientConductor(final Aeron.Context ctx, final Aeron aeron)
    {
        this.ctx = ctx;
        this.aeron = aeron;

        clientLock = ctx.clientLock();
        epochClock = ctx.epochClock();
        nanoClock = ctx.nanoClock();
        awaitingIdleStrategy = ctx.awaitingIdleStrategy();
        driverProxy = ctx.driverProxy();
        logBuffersFactory = ctx.logBuffersFactory();
        idleSleepDurationNs = ctx.idleSleepDurationNs();
        keepAliveIntervalNs = ctx.keepAliveIntervalNs();
        driverTimeoutMs = ctx.driverTimeoutMs();
        driverTimeoutNs = MILLISECONDS.toNanos(driverTimeoutMs);
        interServiceTimeoutNs = ctx.interServiceTimeoutNs();
        defaultAvailableImageHandler = ctx.availableImageHandler();
        defaultUnavailableImageHandler = ctx.unavailableImageHandler();
        driverEventsAdapter = new DriverEventsAdapter(ctx.clientId(), ctx.toClientBuffer(), this, asyncCommandIdSet);
        driverAgentInvoker = ctx.driverAgentInvoker();
        counterValuesBuffer = ctx.countersValuesBuffer();
        countersReader = new CountersReader(ctx.countersMetaDataBuffer(), ctx.countersValuesBuffer(), US_ASCII);

        if (null != ctx.availableCounterHandler())
        {
            availableCounterHandlerById.put(aeron.nextCorrelationId(), ctx.availableCounterHandler());
        }

        if (null != ctx.unavailableCounterHandler())
        {
            unavailableCounterHandlerById.put(aeron.nextCorrelationId(), ctx.unavailableCounterHandler());
        }

        if (null != ctx.closeHandler())
        {
            closeHandlerByIdMap.put(aeron.nextCorrelationId(), ctx.closeHandler());
        }

        if (RECORD_ALLOCATED == countersReader.getCounterState(SYSTEM_COUNTER_ID_CONTROL_PROTOCOL_VERSION) &&
            DRIVER_SYSTEM_COUNTER_TYPE_ID ==
            countersReader.getCounterTypeId(SYSTEM_COUNTER_ID_CONTROL_PROTOCOL_VERSION) &&
            SYSTEM_COUNTER_ID_CONTROL_PROTOCOL_VERSION ==
            countersReader.getCounterRegistrationId(SYSTEM_COUNTER_ID_CONTROL_PROTOCOL_VERSION))
        {
            controlProtocolVersion = (int)countersReader.getCounterValue(SYSTEM_COUNTER_ID_CONTROL_PROTOCOL_VERSION);
        }
        else
        {
            controlProtocolVersion = 0;
        }

        final long nowNs = nanoClock.nanoTime();
        timeOfLastKeepAliveNs = nowNs;
        timeOfLastServiceNs = nowNs;
    }

    public void onClose()
    {
        boolean isInterrupted = false;

        aeron.internalClose();

        clientLock.lock();
        try
        {
            final boolean isTerminating = this.isTerminating;
            this.isTerminating = true;
            forceCloseResources();
            notifyCloseHandlers();

            try
            {
                if (isTerminating)
                {
                    Thread.sleep(Aeron.Configuration.IDLE_SLEEP_DEFAULT_MS);
                }

                Thread.sleep(NANOSECONDS.toMillis(ctx.closeLingerDurationNs()));
            }
            catch (final InterruptedException ignore)
            {
                isInterrupted = true;
            }

            for (final LogBuffers lingeringLogBuffer : lingeringLogBuffers)
            {
                CloseHelper.close(ctx.errorHandler(), lingeringLogBuffer);
            }

            driverProxy.clientClose();
            ctx.close();

            ctx.countersMetaDataBuffer().wrap(0, 0);
            ctx.countersValuesBuffer().wrap(0, 0);
        }
        finally
        {
            isClosed = true;
            if (isInterrupted)
            {
                Thread.currentThread().interrupt();
            }

            clientLock.unlock();
        }
    }

    public int doWork()
    {
        int workCount = 0;

        if (clientLock.tryLock())
        {
            try
            {
                if (isTerminating)
                {
                    throw new AgentTerminationException();
                }

                workCount = service(NO_CORRELATION_ID);
            }
            finally
            {
                clientLock.unlock();
            }
        }

        return workCount;
    }

    public String roleName()
    {
        return "aeron-client-conductor";
    }

    boolean isClosed()
    {
        return isClosed;
    }

    boolean isTerminating()
    {
        return isTerminating;
    }

    void onError(final long correlationId, final int codeValue, final ErrorCode errorCode, final String message)
    {
        driverException = new RegistrationException(correlationId, codeValue, errorCode, message);

        final Object resource = resourceByRegIdMap.get(correlationId);
        if (resource instanceof Subscription)
        {
            final Subscription subscription = (Subscription)resource;
            subscription.internalClose(NULL_VALUE);
            resourceByRegIdMap.remove(correlationId);
        }
    }

    void onAsyncError(final long correlationId, final int codeValue, final ErrorCode errorCode, final String message)
    {
        stashedChannelByRegistrationId.remove(correlationId);
        final RegistrationException ex = new RegistrationException(correlationId, codeValue, errorCode, message);
        asyncExceptionByRegIdMap.put(correlationId, ex);
    }

    void onChannelEndpointError(final long correlationId, final String message)
    {
        final int statusIndicatorId = (int)correlationId;

        for (final Object resource : resourceByRegIdMap.values())
        {
            if (resource instanceof Subscription)
            {
                if (((Subscription)resource).channelStatusId() == statusIndicatorId)
                {
                    handleError(new ChannelEndpointException(statusIndicatorId, message));
                }
            }
            else if (resource instanceof Publication)
            {
                if (((Publication)resource).channelStatusId() == statusIndicatorId)
                {
                    handleError(new ChannelEndpointException(statusIndicatorId, message));
                }
            }
        }

        if (asyncCommandIdSet.remove(correlationId))
        {
            stashedChannelByRegistrationId.remove(correlationId);
            handleError(new RegistrationException(
                correlationId, CHANNEL_ENDPOINT_ERROR.value(), CHANNEL_ENDPOINT_ERROR, message));
        }
    }

    void onPublicationError(final PublicationErrorFrameFlyweight errorFrameFlyweight)
    {
        for (final Object resource : resourceByRegIdMap.values())
        {
            if (resource instanceof Publication)
            {
                final Publication publication = (Publication)resource;
                if (publication.originalRegistrationId() == errorFrameFlyweight.registrationId())
                {
                    publicationErrorFrame.set(errorFrameFlyweight);
                    ctx.publicationErrorFrameHandler().onPublicationError(publicationErrorFrame);
                }
            }
        }
    }

    void onNewPublication(
        final long correlationId,
        final long registrationId,
        final int streamId,
        final int sessionId,
        final int publicationLimitId,
        final int statusIndicatorId,
        final String logFileName)
    {
        final String stashedChannel = stashedChannelByRegistrationId.remove(correlationId);
        final ConcurrentPublication publication = new ConcurrentPublication(
            this,
            stashedChannel,
            streamId,
            sessionId,
            new UnsafeBufferPosition(counterValuesBuffer, publicationLimitId),
            statusIndicatorId,
            logBuffers(registrationId, logFileName, stashedChannel),
            registrationId,
            correlationId);

        resourceByRegIdMap.put(correlationId, publication);
    }

    void onNewExclusivePublication(
        final long correlationId,
        final long registrationId,
        final int streamId,
        final int sessionId,
        final int publicationLimitId,
        final int statusIndicatorId,
        final String logFileName)
    {
        if (correlationId != registrationId)
        {
            handleError(new IllegalStateException(
                "correlationId=" + correlationId + " registrationId=" + registrationId));
        }

        final String stashedChannel = stashedChannelByRegistrationId.remove(correlationId);
        final ExclusivePublication publication = new ExclusivePublication(
            this,
            stashedChannel,
            streamId,
            sessionId,
            new UnsafeBufferPosition(counterValuesBuffer, publicationLimitId),
            statusIndicatorId,
            logBuffers(registrationId, logFileName, stashedChannel),
            registrationId,
            correlationId);

        resourceByRegIdMap.put(correlationId, publication);
    }

    void onNewSubscription(final long correlationId, final int statusIndicatorId)
    {
        final Object resource = resourceByRegIdMap.get(correlationId);
        final Subscription subscription;
        if (resource instanceof PendingSubscription)
        {
            subscription = ((PendingSubscription)resource).subscription;
            resourceByRegIdMap.put(correlationId, subscription);
        }
        else
        {
            subscription = (Subscription)resource;
        }

        subscription.channelStatusId(statusIndicatorId);
    }

    void onAvailableImage(
        final long correlationId,
        final int sessionId,
        final long subscriptionRegistrationId,
        final int subscriberPositionId,
        final String logFileName,
        final String sourceIdentity)
    {
        final Subscription subscription = (Subscription)resourceByRegIdMap.get(subscriptionRegistrationId);
        if (null != subscription)
        {
            final Image image = new Image(
                subscription,
                sessionId,
                new UnsafeBufferPosition(counterValuesBuffer, subscriberPositionId),
                logBuffers(correlationId, logFileName, subscription.channel()),
                ctx.subscriberErrorHandler(),
                sourceIdentity,
                correlationId);

            subscription.addImage(image);

            final AvailableImageHandler handler = subscription.availableImageHandler();
            if (null != handler)
            {
                isInCallback = true;
                try
                {
                    handler.onAvailableImage(image);
                }
                catch (final Exception ex)
                {
                    handleError(ex);
                }
                finally
                {
                    isInCallback = false;
                }
            }
        }
    }

    void onUnavailableImage(final long correlationId, final long subscriptionRegistrationId)
    {
        final Subscription subscription = (Subscription)resourceByRegIdMap.get(subscriptionRegistrationId);
        if (null != subscription)
        {
            final Image image = subscription.removeImage(correlationId);
            if (null != image)
            {
                final UnavailableImageHandler handler = subscription.unavailableImageHandler();
                if (null != handler)
                {
                    notifyImageUnavailable(handler, image);
                }
            }
        }
    }

    void onNewCounter(final long correlationId, final int counterId)
    {
        resourceByRegIdMap.put(correlationId, new Counter(correlationId, this, counterValuesBuffer, counterId));
        onAvailableCounter(correlationId, counterId);
    }

    void onAvailableCounter(final long registrationId, final int counterId)
    {
        for (final AvailableCounterHandler handler : availableCounterHandlerById.values())
        {
            notifyCounterAvailable(registrationId, counterId, handler);
        }
    }

    void onUnavailableCounter(final long registrationId, final int counterId)
    {
        notifyUnavailableCounterHandlers(registrationId, counterId);
    }

    void onClientTimeout()
    {
        if (!isClosed)
        {
            terminateConductor();
            handleError(new ClientTimeoutException("client timeout from driver"));
        }
    }

    CountersReader countersReader()
    {
        return countersReader;
    }

    void handleError(final Throwable ex)
    {
        if (!isClosed)
        {
            ctx.errorHandler().onError(ex);
        }
    }

    int nextSessionId(final int streamId)
    {
        if (controlProtocolVersion >= CONTROL_PROTOCOL_VERSION_WITH_NEXT_AVAILABLE_SESSION_ID_COMMAND)
        {
            clientLock.lock();
            try
            {
                ensureActive();
                ensureNotReentrant();

                lastResponseValue = NULL_VALUE;
                final long correlationId = driverProxy.nextAvailableSessionId(streamId);
                awaitResponse(correlationId);

                return (int)lastResponseValue;
            }
            finally
            {
                clientLock.unlock();
            }
        }
        else
        {
            return BitUtil.generateRandomisedId();
        }
    }

    ConcurrentPublication addPublication(final String channel, final int streamId)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long registrationId = driverProxy.addPublication(channel, streamId);
            stashedChannelByRegistrationId.put(registrationId, channel);
            awaitResponse(registrationId);

            return (ConcurrentPublication)resourceByRegIdMap.get(registrationId);
        }
        finally
        {
            clientLock.unlock();
        }
    }

    ExclusivePublication addExclusivePublication(final String channel, final int streamId)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long registrationId = driverProxy.addExclusivePublication(channel, streamId);
            stashedChannelByRegistrationId.put(registrationId, channel);
            awaitResponse(registrationId);

            return (ExclusivePublication)resourceByRegIdMap.get(registrationId);
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long asyncAddPublication(final String channel, final int streamId)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long registrationId = driverProxy.addPublication(channel, streamId);
            stashedChannelByRegistrationId.put(registrationId, channel);
            asyncCommandIdSet.add(registrationId);

            return registrationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long asyncAddExclusivePublication(final String channel, final int streamId)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long registrationId = driverProxy.addExclusivePublication(channel, streamId);
            stashedChannelByRegistrationId.put(registrationId, channel);
            asyncCommandIdSet.add(registrationId);

            return registrationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    ConcurrentPublication getPublication(final long registrationId)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            if (asyncCommandIdSet.contains(registrationId))
            {
                service(NO_CORRELATION_ID);
            }

            return resourceOrThrow(registrationId, ConcurrentPublication.class);
        }
        finally
        {
            clientLock.unlock();
        }
    }

    ExclusivePublication getExclusivePublication(final long registrationId)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            if (asyncCommandIdSet.contains(registrationId))
            {
                service(NO_CORRELATION_ID);
            }

            return resourceOrThrow(registrationId, ExclusivePublication.class);
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void removePublication(final Publication publication)
    {
        clientLock.lock();
        try
        {
            if (isTerminating || isClosed)
            {
                return;
            }

            if (!publication.isClosed())
            {
                ensureNotReentrant();

                publication.internalClose();
                if (publication == resourceByRegIdMap.remove(publication.registrationId()))
                {
                    releaseLogBuffers(
                        publication.logBuffers(), publication.originalRegistrationId(), EXPLICIT_CLOSE_LINGER_NS);
                    asyncCommandIdSet.add(
                        driverProxy.removePublication(publication.registrationId(), publication.revokeOnClose));
                }
            }
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void removePublication(final long publicationRegistrationId)
    {
        clientLock.lock();
        try
        {
            if (NULL_VALUE == publicationRegistrationId || isTerminating || isClosed)
            {
                return;
            }

            ensureNotReentrant();

            final Object resource = resourceByRegIdMap.get(publicationRegistrationId);
            if (null != resource && !(resource instanceof Publication))
            {
                throw new AeronException("registration id is not a Publication: " +
                    resource.getClass().getSimpleName());
            }

            final Publication publication = (Publication)resource;
            boolean revokeOnClose = false;
            if (null != publication)
            {
                resourceByRegIdMap.remove(publicationRegistrationId);
                publication.internalClose();
                releaseLogBuffers(
                    publication.logBuffers(), publication.originalRegistrationId(), EXPLICIT_CLOSE_LINGER_NS);
                revokeOnClose = publication.revokeOnClose;
            }

            if (asyncCommandIdSet.remove(publicationRegistrationId) || null != publication)
            {
                asyncCommandIdSet.add(
                    driverProxy.removePublication(publicationRegistrationId, revokeOnClose));
                stashedChannelByRegistrationId.remove(publicationRegistrationId);
            }
        }
        finally
        {
            clientLock.unlock();
        }
    }

    Subscription addSubscription(final String channel, final int streamId)
    {
        return addSubscription(channel, streamId, defaultAvailableImageHandler, defaultUnavailableImageHandler);
    }

    Subscription addSubscription(
        final String channel,
        final int streamId,
        final AvailableImageHandler availableImageHandler,
        final UnavailableImageHandler unavailableImageHandler)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long correlationId = driverProxy.addSubscription(channel, streamId);
            final Subscription subscription = new Subscription(
                this,
                channel,
                streamId,
                correlationId,
                availableImageHandler,
                unavailableImageHandler);

            resourceByRegIdMap.put(correlationId, subscription);
            awaitResponse(correlationId);

            return subscription;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long asyncAddSubscription(final String channel, final int streamId)
    {
        return asyncAddSubscription(channel, streamId, defaultAvailableImageHandler, defaultUnavailableImageHandler);
    }

    long asyncAddSubscription(
        final String channel,
        final int streamId,
        final AvailableImageHandler availableImageHandler,
        final UnavailableImageHandler unavailableImageHandler)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long registrationId = driverProxy.addSubscription(channel, streamId);
            final PendingSubscription subscription = new PendingSubscription(new Subscription(
                this,
                channel,
                streamId,
                registrationId,
                availableImageHandler,
                unavailableImageHandler));

            resourceByRegIdMap.put(registrationId, subscription);
            asyncCommandIdSet.add(registrationId);

            return registrationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    Subscription getSubscription(final long registrationId)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            if (asyncCommandIdSet.contains(registrationId))
            {
                service(NO_CORRELATION_ID);
            }

            return resourceOrThrow(registrationId, Subscription.class);
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void removeSubscription(final Subscription subscription)
    {
        clientLock.lock();
        try
        {
            if (isTerminating || isClosed)
            {
                return;
            }

            if (!subscription.isClosed())
            {
                ensureNotReentrant();

                subscription.internalClose(EXPLICIT_CLOSE_LINGER_NS);
                final long registrationId = subscription.registrationId();
                if (subscription == resourceByRegIdMap.remove(registrationId))
                {
                    asyncCommandIdSet.add(driverProxy.removeSubscription(registrationId));
                }
            }
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void removeSubscription(final long subscriptionRegistrationId)
    {
        clientLock.lock();
        try
        {
            if (NULL_VALUE == subscriptionRegistrationId || isTerminating || isClosed)
            {
                return;
            }

            ensureNotReentrant();

            final Object resource = resourceByRegIdMap.get(subscriptionRegistrationId);
            if (resource != null && !(resource instanceof PendingSubscription || resource instanceof Subscription))
            {
                throw new AeronException("registration id is not a Subscription: " +
                    resource.getClass().getSimpleName());
            }

            final Subscription subscription = resource instanceof PendingSubscription ?
                ((PendingSubscription)resource).subscription : (Subscription)resource;
            if (null != subscription)
            {
                resourceByRegIdMap.remove(subscriptionRegistrationId);
                subscription.internalClose(EXPLICIT_CLOSE_LINGER_NS);
            }

            if (asyncCommandIdSet.remove(subscriptionRegistrationId) || null != subscription)
            {
                asyncCommandIdSet.add(driverProxy.removeSubscription(subscriptionRegistrationId));
            }
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void addDestination(final long registrationId, final String endpointChannel)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            awaitResponse(driverProxy.addDestination(registrationId, endpointChannel));
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long addDestinationWithId(final long registrationId, final String endpointChannel)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long correlationId = driverProxy.addDestination(registrationId, endpointChannel);
            awaitResponse(correlationId);
            return correlationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void removeDestination(final long registrationId, final String endpointChannel)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            awaitResponse(driverProxy.removeDestination(registrationId, endpointChannel));
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void removeDestination(final long publicationRegistrationId, final long destinationRegistrationId)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            awaitResponse(driverProxy.removeDestination(publicationRegistrationId, destinationRegistrationId));
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void addRcvDestination(final long registrationId, final String endpointChannel)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            awaitResponse(driverProxy.addRcvDestination(registrationId, endpointChannel));
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void removeRcvDestination(final long registrationId, final String endpointChannel)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            awaitResponse(driverProxy.removeRcvDestination(registrationId, endpointChannel));
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long asyncAddDestination(final long registrationId, final String endpointChannel)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long correlationId = driverProxy.addDestination(registrationId, endpointChannel);
            asyncCommandIdSet.add(correlationId);
            return correlationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long asyncRemoveDestination(final long registrationId, final String endpointChannel)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long correlationId = driverProxy.removeDestination(registrationId, endpointChannel);
            asyncCommandIdSet.add(correlationId);
            return correlationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long asyncRemoveDestination(final long registrationId, final long destinationRegistrationId)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long correlationId = driverProxy.removeDestination(registrationId, destinationRegistrationId);
            asyncCommandIdSet.add(correlationId);
            return correlationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long asyncAddRcvDestination(final long registrationId, final String endpointChannel)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long correlationId = driverProxy.addRcvDestination(registrationId, endpointChannel);
            asyncCommandIdSet.add(correlationId);
            return correlationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long asyncRemoveRcvDestination(final long registrationId, final String endpointChannel)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long correlationId = driverProxy.removeRcvDestination(registrationId, endpointChannel);
            asyncCommandIdSet.add(correlationId);
            return correlationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    boolean isCommandActive(final long correlationId)
    {
        clientLock.lock();
        try
        {
            if (isClosed)
            {
                return false;
            }

            ensureActive();

            if (asyncCommandIdSet.contains(correlationId))
            {
                service(NO_CORRELATION_ID);
            }

            return asyncCommandIdSet.contains(correlationId);
        }
        finally
        {
            clientLock.unlock();
        }
    }

    boolean hasActiveCommands()
    {
        clientLock.lock();
        try
        {
            if (isClosed)
            {
                return false;
            }

            ensureActive();

            return !asyncCommandIdSet.isEmpty();
        }
        finally
        {
            clientLock.unlock();
        }
    }

    Counter addCounter(
        final int typeId,
        final DirectBuffer keyBuffer,
        final int keyOffset,
        final int keyLength,
        final DirectBuffer labelBuffer,
        final int labelOffset,
        final int labelLength)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            if (keyLength < 0 || keyLength > CountersManager.MAX_KEY_LENGTH)
            {
                throw new IllegalArgumentException("key length out of bounds: " + keyLength);
            }

            if (labelLength < 0 || labelLength > CountersManager.MAX_LABEL_LENGTH)
            {
                throw new IllegalArgumentException("label length out of bounds: " + labelLength);
            }

            final long registrationId = driverProxy.addCounter(
                typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength);

            awaitResponse(registrationId);

            return (Counter)resourceByRegIdMap.get(registrationId);
        }
        finally
        {
            clientLock.unlock();
        }
    }

    Counter addCounter(final int typeId, final String label)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            if (label.length() > CountersManager.MAX_LABEL_LENGTH)
            {
                throw new IllegalArgumentException("label length exceeds MAX_LABEL_LENGTH: " + label.length());
            }

            final long registrationId = driverProxy.addCounter(typeId, label);
            awaitResponse(registrationId);

            return (Counter)resourceByRegIdMap.get(registrationId);
        }
        finally
        {
            clientLock.unlock();
        }
    }

    Counter addStaticCounter(
        final int typeId,
        final DirectBuffer keyBuffer,
        final int keyOffset,
        final int keyLength,
        final DirectBuffer labelBuffer,
        final int labelOffset,
        final int labelLength,
        final long registrationId)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            if (keyLength < 0 || keyLength > CountersManager.MAX_KEY_LENGTH)
            {
                throw new IllegalArgumentException("key length out of bounds: " + keyLength);
            }

            if (labelLength < 0 || labelLength > CountersManager.MAX_LABEL_LENGTH)
            {
                throw new IllegalArgumentException("label length out of bounds: " + labelLength);
            }

            final long correlationId = driverProxy.addStaticCounter(
                typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength, registrationId);

            awaitResponse(correlationId);

            return (Counter)resourceByRegIdMap.get(correlationId);
        }
        finally
        {
            clientLock.unlock();
        }
    }

    Counter addStaticCounter(final int typeId, final String label, final long registrationId)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            if (label.length() > CountersManager.MAX_LABEL_LENGTH)
            {
                throw new IllegalArgumentException("label length exceeds MAX_LABEL_LENGTH: " + label.length());
            }

            final long correlationId = driverProxy.addStaticCounter(typeId, label, registrationId);
            awaitResponse(correlationId);

            return (Counter)resourceByRegIdMap.get(correlationId);
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long asyncAddCounter(final int typeId, final String label)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            if (label.length() > CountersManager.MAX_LABEL_LENGTH)
            {
                throw new IllegalArgumentException("label length exceeds MAX_LABEL_LENGTH: " + label.length());
            }

            final long registrationId = driverProxy.addCounter(typeId, label);
            asyncCommandIdSet.add(registrationId);
            return registrationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long asyncAddCounter(
        final int typeId,
        final DirectBuffer keyBuffer,
        final int keyOffset,
        final int keyLength,
        final DirectBuffer labelBuffer,
        final int labelOffset,
        final int labelLength)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            if (keyLength < 0 || keyLength > CountersManager.MAX_KEY_LENGTH)
            {
                throw new IllegalArgumentException("key length out of bounds: " + keyLength);
            }

            if (labelLength < 0 || labelLength > CountersManager.MAX_LABEL_LENGTH)
            {
                throw new IllegalArgumentException("label length out of bounds: " + labelLength);
            }

            final long registrationId = driverProxy.addCounter(
                typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength);
            asyncCommandIdSet.add(registrationId);
            return registrationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long asyncAddStaticCounter(final int typeId, final String label, final long registrationId)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            if (label.length() > CountersManager.MAX_LABEL_LENGTH)
            {
                throw new IllegalArgumentException("label length exceeds MAX_LABEL_LENGTH: " + label.length());
            }

            final long correlationId = driverProxy.addStaticCounter(typeId, label, registrationId);
            asyncCommandIdSet.add(correlationId);
            return correlationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long asyncAddStaticCounter(
        final int typeId,
        final DirectBuffer keyBuffer,
        final int keyOffset,
        final int keyLength,
        final DirectBuffer labelBuffer,
        final int labelOffset,
        final int labelLength,
        final long registrationId)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            if (keyLength < 0 || keyLength > CountersManager.MAX_KEY_LENGTH)
            {
                throw new IllegalArgumentException("key length out of bounds: " + keyLength);
            }

            if (labelLength < 0 || labelLength > CountersManager.MAX_LABEL_LENGTH)
            {
                throw new IllegalArgumentException("label length out of bounds: " + labelLength);
            }

            final long correlationId = driverProxy.addStaticCounter(
                typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength, registrationId);
            asyncCommandIdSet.add(correlationId);
            return correlationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void asyncRemoveCounter(final long counterRegistrationId)
    {
        clientLock.lock();
        try
        {
            if (NULL_VALUE == counterRegistrationId || isTerminating || isClosed)
            {
                return;
            }

            ensureNotReentrant();

            final Object resource = resourceByRegIdMap.get(counterRegistrationId);
            if (null != resource && !(resource instanceof Counter))
            {
                throw new AeronException("registration id is not a Counter: " +
                    resource.getClass().getSimpleName());
            }

            final Counter counter = (Counter)resource;
            if (null != counter)
            {
                resourceByRegIdMap.remove(counterRegistrationId);
                counter.internalClose();
            }

            if (asyncCommandIdSet.remove(counterRegistrationId) || null != counter)
            {
                asyncCommandIdSet.add(driverProxy.removeCounter(counterRegistrationId));
            }
        }
        finally
        {
            clientLock.unlock();
        }
    }

    Counter getCounter(final long registrationId)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            if (asyncCommandIdSet.contains(registrationId))
            {
                service(NO_CORRELATION_ID);
            }

            return resourceOrThrow(registrationId, Counter.class);
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long addAvailableCounterHandler(final AvailableCounterHandler handler)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long registrationId = aeron.nextCorrelationId();
            availableCounterHandlerById.put(registrationId, handler);
            return registrationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    boolean removeAvailableCounterHandler(final long registrationId)
    {
        clientLock.lock();
        try
        {
            return availableCounterHandlerById.remove(registrationId) != null;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    boolean removeAvailableCounterHandler(final AvailableCounterHandler handler)
    {
        clientLock.lock();
        try
        {
            if (isTerminating || isClosed)
            {
                return false;
            }

            ensureNotReentrant();

            final Long2ObjectHashMap<AvailableCounterHandler>.ValueIterator iterator =
                availableCounterHandlerById.values().iterator();
            while (iterator.hasNext())
            {
                if (handler == iterator.next())
                {
                    iterator.remove();
                    return true;
                }
            }

            return false;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long addUnavailableCounterHandler(final UnavailableCounterHandler handler)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long registrationId = aeron.nextCorrelationId();
            unavailableCounterHandlerById.put(registrationId, handler);
            return registrationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    boolean removeUnavailableCounterHandler(final long registrationId)
    {
        clientLock.lock();
        try
        {
            return unavailableCounterHandlerById.remove(registrationId) != null;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    boolean removeUnavailableCounterHandler(final UnavailableCounterHandler handler)
    {
        clientLock.lock();
        try
        {
            if (isTerminating || isClosed)
            {
                return false;
            }

            ensureNotReentrant();

            final Long2ObjectHashMap<UnavailableCounterHandler>.ValueIterator iterator =
                unavailableCounterHandlerById.values().iterator();
            while (iterator.hasNext())
            {
                if (handler == iterator.next())
                {
                    iterator.remove();
                    return true;
                }
            }

            return false;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long addCloseHandler(final Runnable handler)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long registrationId = aeron.nextCorrelationId();
            closeHandlerByIdMap.put(registrationId, handler);
            return registrationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    boolean removeCloseHandler(final long registrationId)
    {
        clientLock.lock();
        try
        {
            return closeHandlerByIdMap.remove(registrationId) != null;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    boolean removeCloseHandler(final Runnable handler)
    {
        clientLock.lock();
        try
        {
            if (isTerminating || isClosed)
            {
                return false;
            }

            ensureNotReentrant();

            final Long2ObjectHashMap<Runnable>.ValueIterator iterator = closeHandlerByIdMap.values().iterator();
            while (iterator.hasNext())
            {
                if (handler == iterator.next())
                {
                    iterator.remove();
                    return true;
                }
            }

            return false;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void releaseCounter(final Counter counter)
    {
        clientLock.lock();
        try
        {
            if (isTerminating || isClosed)
            {
                return;
            }

            ensureNotReentrant();

            final long registrationId = counter.registrationId();
            if (counter == resourceByRegIdMap.remove(registrationId))
            {
                asyncCommandIdSet.add(driverProxy.removeCounter(registrationId));
            }
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void releaseLogBuffers(
        final LogBuffers logBuffers, final long registrationId, final long lingerDurationNs)
    {
        if (logBuffers.decRef() == 0)
        {
            lingeringLogBuffers.add(logBuffers);
            logBuffersByIdMap.remove(registrationId);

            final long lingerNs = NULL_VALUE == lingerDurationNs ? ctx.resourceLingerDurationNs() : lingerDurationNs;
            logBuffers.lingerDeadlineNs(nanoClock.nanoTime() + lingerNs);
        }
    }

    DriverEventsAdapter driverListenerAdapter()
    {
        return driverEventsAdapter;
    }

    long channelStatus(final int channelStatusId)
    {
        return switch (channelStatusId)
        {
            case 0 -> ChannelEndpointStatus.INITIALIZING;
            case ChannelEndpointStatus.NO_ID_ALLOCATED -> ChannelEndpointStatus.ACTIVE;
            default -> countersReader.getCounterValue(channelStatusId);
        };
    }

    void closeImages(final Image[] images, final UnavailableImageHandler unavailableImageHandler, final long lingerNs)
    {
        for (final Image image : images)
        {
            image.close();
        }

        for (final Image image : images)
        {
            releaseLogBuffers(image.logBuffers(), image.correlationId(), lingerNs);
        }

        if (null != unavailableImageHandler)
        {
            for (final Image image : images)
            {
                notifyImageUnavailable(unavailableImageHandler, image);
            }
        }
    }

    void onStaticCounter(final long correlationId, final int counterId)
    {
        final CountersReader countersReader = aeron.countersReader();
        resourceByRegIdMap.put(
            correlationId,
            new Counter(countersReader, countersReader.getCounterRegistrationId(counterId), counterId));
    }

    void rejectImage(final long correlationId, final long position, final String reason)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long registrationId = driverProxy.rejectImage(correlationId, position, reason);
            awaitResponse(registrationId);
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void onNextAvailableSessionId(final int nextSessionId)
    {
        lastResponseValue = nextSessionId;
    }

    private void ensureActive()
    {
        if (isClosed)
        {
            throw new AeronException("Aeron client is closed");
        }

        if (isTerminating)
        {
            throw new AeronException("Aeron client is terminating");
        }
    }

    private void ensureNotReentrant()
    {
        if (isInCallback)
        {
            throw new AeronException("reentrant calls not permitted during callbacks");
        }
    }

    private LogBuffers logBuffers(final long registrationId, final String logFileName, final String channel)
    {
        LogBuffers logBuffers = logBuffersByIdMap.get(registrationId);
        if (null == logBuffers)
        {
            try
            {
                logBuffers = logBuffersFactory.map(logFileName);

                if (ctx.preTouchMappedMemory())
                {
                    logBuffers.preTouch();
                }

                logBuffersByIdMap.put(registrationId, logBuffers);
            }
            catch (final Exception ex)
            {
                throw new AeronException("[clientId=" + ctx.clientId() + ", clientName=" + ctx.clientName() +
                    "] Failed to map log buffer with registrationId=" + registrationId + ", channel=" + channel, ex);
            }
        }

        logBuffers.incRef();

        return logBuffers;
    }

    private int service(final long correlationId)
    {
        int workCount = 0;

        try
        {
            workCount += checkTimeouts(nanoClock.nanoTime());
            workCount += driverEventsAdapter.receive(correlationId);
        }
        catch (final AgentTerminationException ex)
        {
            if (isClientApiCall(correlationId))
            {
                terminateConductor();
            }

            throw ex;
        }
        catch (final Exception ex)
        {
            if (driverEventsAdapter.isInvalid())
            {
                terminateConductor();

                if (!isClientApiCall(correlationId))
                {
                    throw new AeronException("Driver events adapter is invalid", ex);
                }
            }

            if (isClientApiCall(correlationId))
            {
                throw ex;
            }

            handleError(ex);
        }

        return workCount;
    }

    private void terminateConductor()
    {
        isTerminating = true;
        forceCloseResources();
    }

    private void awaitResponse(final long correlationId)
    {
        final long nowNs = nanoClock.nanoTime();
        final long deadlineNs = nowNs + driverTimeoutNs;
        checkTimeouts(nowNs);

        awaitingIdleStrategy.reset();
        do
        {
            if (null == driverAgentInvoker)
            {
                awaitingIdleStrategy.idle();
            }
            else
            {
                driverAgentInvoker.invoke();
            }

            service(correlationId);

            if (driverEventsAdapter.receivedCorrelationId() == correlationId)
            {
                stashedChannelByRegistrationId.remove(correlationId);
                final RegistrationException ex = driverException;
                if (null != ex)
                {
                    driverException = null;
                    throw ex;
                }

                return;
            }

            if (Thread.currentThread().isInterrupted())
            {
                terminateConductor();
                throw new AeronException("unexpected interrupt");
            }
        }
        while (deadlineNs - nanoClock.nanoTime() > 0);

        throw new DriverTimeoutException("no response from MediaDriver within " + driverTimeoutNs + "ns");
    }

    private int checkTimeouts(final long nowNs)
    {
        int workCount = 0;

        if ((timeOfLastServiceNs + idleSleepDurationNs) - nowNs < 0)
        {
            checkServiceInterval(nowNs);
            timeOfLastServiceNs = nowNs;

            workCount += checkLiveness(nowNs);
            workCount += checkLingeringResources(nowNs);
        }

        return workCount;
    }

    private void checkServiceInterval(final long nowNs)
    {
        if ((timeOfLastServiceNs + interServiceTimeoutNs) - nowNs < 0)
        {
            terminateConductor();

            throw new ConductorServiceTimeoutException(
                "service interval exceeded: timeout=" + interServiceTimeoutNs +
                "ns, interval=" + (nowNs - timeOfLastServiceNs) + "ns");
        }
    }

    private int checkLiveness(final long nowNs)
    {
        if ((timeOfLastKeepAliveNs + keepAliveIntervalNs) - nowNs < 0)
        {
            final long nowMs = epochClock.time();
            final long lastKeepAliveMs = driverProxy.timeOfLastDriverKeepaliveMs();

            if (nowMs > (lastKeepAliveMs + driverTimeoutMs))
            {
                terminateConductor();

                if (Aeron.NULL_VALUE == lastKeepAliveMs)
                {
                    throw new DriverTimeoutException(
                        "MediaDriver (" + aeron.context().aeronDirectoryName() + ") has been shutdown");
                }

                throw new DriverTimeoutException(
                    "MediaDriver (" + aeron.context().aeronDirectoryName() + ") keepalive: age=" +
                        (nowMs - lastKeepAliveMs) + "ms > timeout=" + driverTimeoutMs + "ms");
            }

            if (null == heartbeatTimestamp)
            {
                final int counterId = HeartbeatTimestamp.findCounterIdByRegistrationId(
                    countersReader, HEARTBEAT_TYPE_ID, ctx.clientId());

                if (NULL_COUNTER_ID != counterId)
                {
                    try
                    {
                        heartbeatTimestamp = new AtomicCounter(counterValuesBuffer, counterId);
                        heartbeatTimestamp.setRelease(nowMs);
                        appendToLabel(
                            countersReader.metaDataBuffer(),
                            counterId,
                            " name=" + ctx.clientName() + " " +
                            formatVersionInfo(AeronVersion.VERSION, AeronVersion.GIT_SHA));
                        timeOfLastKeepAliveNs = nowNs;
                    }
                    catch (final RuntimeException ex)  // a race caused by the driver timing out the client
                    {
                        terminateConductor();
                        throw new AeronException("unexpected close of heartbeat timestamp counter: " + counterId, ex);
                    }
                }
            }
            else
            {
                final int counterId = heartbeatTimestamp.id();
                if (!HeartbeatTimestamp.isActive(countersReader, counterId, HEARTBEAT_TYPE_ID, ctx.clientId()))
                {
                    terminateConductor();
                    throw new AeronException("unexpected close of heartbeat timestamp counter: " + counterId);
                }

                heartbeatTimestamp.setRelease(nowMs);
                timeOfLastKeepAliveNs = nowNs;
            }

            return 1;
        }

        return 0;
    }

    private int checkLingeringResources(final long nowNs)
    {
        int workCount = 0;

        for (int lastIndex = lingeringLogBuffers.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final LogBuffers logBuffers = lingeringLogBuffers.get(i);
            if (logBuffers.lingerDeadlineNs() - nowNs < 0)
            {
                ArrayListUtil.fastUnorderedRemove(lingeringLogBuffers, i, lastIndex--);
                CloseHelper.close(ctx.errorHandler(), logBuffers);

                workCount += 1;
            }
        }

        return workCount;
    }

    private void forceCloseResources()
    {
        for (final Object resource : resourceByRegIdMap.values())
        {
            if (resource instanceof Subscription subscription)
            {
                subscription.internalClose(NULL_VALUE);
            }
            else if (resource instanceof Publication publication)
            {
                publication.internalClose();
                releaseLogBuffers(publication.logBuffers(), publication.originalRegistrationId(), NULL_VALUE);
            }
            else if (resource instanceof Counter counter && this == counter.clientConductor())
            {
                counter.internalClose();
                notifyUnavailableCounterHandlers(counter.registrationId(), counter.id());
            }
        }

        resourceByRegIdMap.clear();
    }

    private void notifyUnavailableCounterHandlers(final long registrationId, final int counterId)
    {
        for (final UnavailableCounterHandler handler : unavailableCounterHandlerById.values())
        {
            isInCallback = true;
            try
            {
                handler.onUnavailableCounter(countersReader, registrationId, counterId);
            }
            catch (final AgentTerminationException ex)
            {
                if (!isTerminating)
                {
                    throw ex;
                }
                handleError(ex);
            }
            catch (final Exception ex)
            {
                handleError(ex);
            }
            finally
            {
                isInCallback = false;
            }
        }
    }

    private void notifyImageUnavailable(final UnavailableImageHandler handler, final Image image)
    {
        isInCallback = true;
        try
        {
            handler.onUnavailableImage(image);
        }
        catch (final AgentTerminationException ex)
        {
            if (!isTerminating)
            {
                throw ex;
            }
            handleError(ex);
        }
        catch (final Exception ex)
        {
            handleError(ex);
        }
        finally
        {
            isInCallback = false;
        }
    }

    private void notifyCounterAvailable(
        final long registrationId, final int counterId, final AvailableCounterHandler handler)
    {
        isInCallback = true;
        try
        {
            handler.onAvailableCounter(countersReader, registrationId, counterId);
        }
        catch (final AgentTerminationException ex)
        {
            throw ex;
        }
        catch (final Exception ex)
        {
            handleError(ex);
        }
        finally
        {
            isInCallback = false;
        }
    }

    private void notifyCloseHandlers()
    {
        for (final Runnable closeHandler : closeHandlerByIdMap.values())
        {
            isInCallback = true;
            try
            {
                closeHandler.run();
            }
            catch (final Exception ex)
            {
                handleError(ex);
            }
            finally
            {
                isInCallback = false;
            }
        }
    }

    private <T> T resourceOrThrow(final long registrationId, final Class<T> resourceClass)
    {
        final Object resource = resourceByRegIdMap.get(registrationId);
        if (resourceClass.isInstance(resource))
        {
            return resourceClass.cast(resource);
        }

        final RegistrationException ex = asyncExceptionByRegIdMap.remove(registrationId);
        if (null != ex)
        {
            throw new RegistrationException(ex);
        }

        return null;
    }

    private static boolean isClientApiCall(final long correlationId)
    {
        return correlationId != NO_CORRELATION_ID;
    }

    static final class PendingSubscription
    {
        final Subscription subscription;

        private PendingSubscription(final Subscription subscription)
        {
            this.subscription = subscription;
        }
    }
}
