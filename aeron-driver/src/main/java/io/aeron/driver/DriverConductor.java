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
package io.aeron.driver;

import io.aeron.ChannelUri;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.exceptions.InvalidChannelException;
import io.aeron.driver.exceptions.UnknownSubscriptionException;
import io.aeron.driver.media.ControlMode;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.ReceiveDestinationTransport;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.status.ClientHeartbeatTimestamp;
import io.aeron.driver.status.PublisherLimit;
import io.aeron.driver.status.PublisherPos;
import io.aeron.driver.status.ReceiveChannelStatus;
import io.aeron.driver.status.ReceiveLocalSocketAddress;
import io.aeron.driver.status.ReceiverHwm;
import io.aeron.driver.status.ReceiverNaksSent;
import io.aeron.driver.status.ReceiverPos;
import io.aeron.driver.status.SendChannelStatus;
import io.aeron.driver.status.SendLocalSocketAddress;
import io.aeron.driver.status.SenderBpe;
import io.aeron.driver.status.SenderLimit;
import io.aeron.driver.status.SenderNaksReceived;
import io.aeron.driver.status.SenderPos;
import io.aeron.driver.status.SubscriberPos;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.driver.status.SystemCounters;
import io.aeron.exceptions.AeronEvent;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.ControlProtocolException;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.ErrorFlyweight;
import io.aeron.protocol.SetupFlyweight;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.collections.ObjectHashSet;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.agrona.concurrent.status.Position;
import org.agrona.concurrent.status.UnsafeBufferPosition;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.ChannelUri.SPY_QUALIFIER;
import static io.aeron.CommonContext.CHANNEL_RECEIVE_TIMESTAMP_OFFSET_PARAM_NAME;
import static io.aeron.CommonContext.CHANNEL_SEND_TIMESTAMP_OFFSET_PARAM_NAME;
import static io.aeron.CommonContext.CONTROL_MODE_RESPONSE;
import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.CommonContext.IPC_MEDIA;
import static io.aeron.CommonContext.InferableBoolean;
import static io.aeron.CommonContext.InferableBoolean.FORCE_TRUE;
import static io.aeron.CommonContext.InferableBoolean.INFER;
import static io.aeron.CommonContext.MDC_CONTROL_MODE_PARAM_NAME;
import static io.aeron.CommonContext.MDC_CONTROL_PARAM_NAME;
import static io.aeron.CommonContext.MEDIA_RCV_TIMESTAMP_OFFSET_PARAM_NAME;
import static io.aeron.CommonContext.MTU_LENGTH_PARAM_NAME;
import static io.aeron.CommonContext.RECEIVER_WINDOW_LENGTH_PARAM_NAME;
import static io.aeron.CommonContext.RESPONSE_CORRELATION_ID_PARAM_NAME;
import static io.aeron.CommonContext.SOCKET_RCVBUF_PARAM_NAME;
import static io.aeron.CommonContext.SOCKET_SNDBUF_PARAM_NAME;
import static io.aeron.ErrorCode.GENERIC_ERROR;
import static io.aeron.ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE;
import static io.aeron.ErrorCode.UNKNOWN_COUNTER;
import static io.aeron.ErrorCode.UNKNOWN_PUBLICATION;
import static io.aeron.driver.PublicationParams.PROTOTYPE_VALUE_CORRELATION_ID;
import static io.aeron.driver.PublicationParams.confirmMatch;
import static io.aeron.driver.PublicationParams.getPublicationParams;
import static io.aeron.driver.PublicationParams.validateMtuForSndbuf;
import static io.aeron.driver.PublicationParams.validateSpiesSimulateConnection;
import static io.aeron.driver.SubscriptionParams.validateInitialWindowForRcvBuf;
import static io.aeron.driver.status.SystemCounterDescriptor.IMAGES_REJECTED;
import static io.aeron.driver.status.SystemCounterDescriptor.INVALID_PACKETS;
import static io.aeron.driver.status.SystemCounterDescriptor.RESOLUTION_CHANGES;
import static io.aeron.driver.status.SystemCounterDescriptor.RETRANSMIT_OVERFLOW;
import static io.aeron.driver.status.SystemCounterDescriptor.UNBLOCKED_COMMANDS;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;
import static io.aeron.logbuffer.LogBufferDescriptor.positionBitsToShift;
import static org.agrona.collections.ArrayListUtil.fastUnorderedRemove;

/**
 * Driver Conductor that takes commands from publishers and subscribers, and orchestrates the media driver.
 */
public final class DriverConductor implements Agent
{
    private static final long CLOCK_UPDATE_INTERNAL_NS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final String[] INVALID_DESTINATION_KEYS = {
        MTU_LENGTH_PARAM_NAME,
        RECEIVER_WINDOW_LENGTH_PARAM_NAME,
        SOCKET_RCVBUF_PARAM_NAME,
        SOCKET_SNDBUF_PARAM_NAME,
        RESPONSE_CORRELATION_ID_PARAM_NAME
    };

    private int nextSessionId = BitUtil.generateRandomisedId();
    private final long timerIntervalNs;
    private final long clientLivenessTimeoutNs;
    private long timeOfLastToDriverPositionChangeNs;
    private long lastCommandConsumerPosition;
    private long timerCheckDeadlineNs;
    private long clockUpdateDeadlineNs;

    private final Context ctx;
    private final ReceiverProxy receiverProxy;
    private final SenderProxy senderProxy;
    private final NativeResourceAgentProxy nativeResourceAgentProxy;
    private final ClientProxy clientProxy;
    private final RingBuffer toDriverCommands;
    private final ClientCommandAdapter clientCommandAdapter;
    private final ManyToOneConcurrentLinkedQueue<Runnable> driverCmdQueue;
    private final Object2ObjectHashMap<String, SendChannelEndpoint> sendChannelEndpointByChannelMap =
        new Object2ObjectHashMap<>();
    private final Object2ObjectHashMap<String, ReceiveChannelEndpoint> receiveChannelEndpointByChannelMap =
        new Object2ObjectHashMap<>();
    private final ArrayList<NetworkPublication> networkPublications = new ArrayList<>();
    private final ArrayList<IpcPublication> ipcPublications = new ArrayList<>();
    private final ArrayList<PublicationImage> publicationImages = new ArrayList<>();
    private final ArrayList<PublicationLink> publicationLinks = new ArrayList<>();
    private final ArrayList<SubscriptionLink> subscriptionLinks = new ArrayList<>();
    private final ArrayList<CounterLink> counterLinks = new ArrayList<>();
    private final ArrayList<AeronClient> clients = new ArrayList<>();
    private final ObjectHashSet<SessionKey> activeSessionSet = new ObjectHashSet<>();
    private final EpochClock epochClock;
    private final NanoClock nanoClock;
    private final CachedEpochClock cachedEpochClock;
    private final CachedNanoClock cachedNanoClock;
    private final CountersManager countersManager;
    private final NetworkPublicationThreadLocals networkPublicationThreadLocals = new NetworkPublicationThreadLocals();
    private final MutableDirectBuffer tempBuffer;
    private final AtomicCounter imagesRejected;
    private final DutyCycleTracker dutyCycleTracker;
    private ClientCommand clientCommand;
    private Command driverCommand;

    DriverConductor(final Context ctx)
    {
        this.ctx = ctx;
        timerIntervalNs = ctx.timerIntervalNs();
        clientLivenessTimeoutNs = ctx.clientLivenessTimeoutNs();
        driverCmdQueue = ctx.driverCommandQueue();
        receiverProxy = ctx.receiverProxy();
        senderProxy = ctx.senderProxy();
        epochClock = ctx.epochClock();
        nanoClock = ctx.nanoClock();
        cachedEpochClock = ctx.cachedEpochClock();
        cachedNanoClock = ctx.cachedNanoClock();
        toDriverCommands = ctx.toDriverCommands();
        clientProxy = ctx.clientProxy();
        tempBuffer = ctx.tempBuffer();
        imagesRejected = ctx.systemCounters().get(IMAGES_REJECTED);
        dutyCycleTracker = ctx.conductorDutyCycleTracker();

        countersManager = ctx.countersManager();

        nativeResourceAgentProxy = ctx.nativeResourceAgentProxy();

        clientCommandAdapter = new ClientCommandAdapter(ctx.countedErrorHandler(), toDriverCommands, clientProxy, this);

        lastCommandConsumerPosition = toDriverCommands.consumerPosition();
    }

    /**
     * {@inheritDoc}
     */
    public void onStart()
    {
        final long nowNs = nanoClock.nanoTime();
        cachedNanoClock.update(nowNs);
        cachedEpochClock.update(epochClock.time());
        dutyCycleTracker.update(nowNs);
        timerCheckDeadlineNs = nowNs + timerIntervalNs;
        clockUpdateDeadlineNs = nowNs + CLOCK_UPDATE_INTERNAL_NS;
        timeOfLastToDriverPositionChangeNs = nowNs;

        final SystemCounters systemCounters = ctx.systemCounters();
        systemCounters.get(RESOLUTION_CHANGES).appendToLabel(": driverName=" + ctx.resolverName());
    }

    /**
     * {@inheritDoc}
     */
    public void onClose()
    {
        CloseHelper.closeAll(receiveChannelEndpointByChannelMap.values());
        CloseHelper.closeAll(sendChannelEndpointByChannelMap.values());
        for (final PublicationImage publicationImage : publicationImages)
        {
            publicationImage.rawLog().free();
        }
        for (final NetworkPublication networkPublication : networkPublications)
        {
            networkPublication.rawLog().free();
        }
        for (final IpcPublication ipcPublication : ipcPublications)
        {
            ipcPublication.rawLog().free();
        }
        toDriverCommands.consumerHeartbeatTime(NULL_VALUE);
        ctx.cncByteBuffer().force();
        ctx.close();
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return "driver-conductor";
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        final long nowNs = nanoClock.nanoTime();
        trackTime(nowNs);

        int workCount = 0;
        workCount += processTimers(nowNs);
        workCount += processClientCommands();
        workCount += drainCommandQueue();
        workCount += trackStreamPositions(workCount, nowNs);

        return workCount;
    }

    boolean notAcceptingClientCommands()
    {
        return senderProxy.isApplyingBackpressure() ||
            receiverProxy.isApplyingBackpressure() ||
            nativeResourceAgentProxy.isApplyingBackpressure();
    }

    @SuppressWarnings("MethodLength")
    void onCreatePublicationImage(
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int activeTermId,
        final int termOffset,
        final int termBufferLength,
        final int senderMtuLength,
        final int transportIndex,
        final short flags,
        final InetSocketAddress controlAddress,
        final InetSocketAddress sourceAddress,
        final ReceiveChannelEndpoint channelEndpoint)
    {
        scheduleDriverCommand(new CreatePublicationImageCommand(
            sessionId,
            streamId,
            initialTermId,
            activeTermId,
            termOffset,
            termBufferLength,
            senderMtuLength,
            transportIndex,
            flags,
            controlAddress,
            sourceAddress,
            channelEndpoint));
    }

    void onPublicationError(
        final long registrationId,
        final long destinationRegistrationId,
        final int sessionId,
        final int streamId,
        final long receiverId,
        final long groupId,
        final InetSocketAddress srcAddress,
        final int errorCode,
        final String errorMessage)
    {
        recordError(new AeronEvent(
            "onPublicationError: " +
            "registrationId=" + registrationId +
            ", destinationRegistrationId=" + destinationRegistrationId +
            ", sessionId=" + sessionId +
            ", streamId=" + streamId +
            ", receiverId=" + receiverId +
            ", groupId=" + groupId +
            ", errorCode=" + errorCode +
            ", errorMessage=" + errorMessage,
            AeronException.Category.WARN));
        clientProxy.onPublicationErrorFrame(
            registrationId,
            destinationRegistrationId,
            sessionId,
            streamId,
            receiverId,
            groupId,
            srcAddress,
            errorCode,
            errorMessage);
    }

    void onReResolveEndpoint(
        final String endpoint, final SendChannelEndpoint channelEndpoint, final InetSocketAddress address)
    {
        if (ChannelEndpointStatus.ACTIVE == channelEndpoint.status())
        {
            scheduleDriverCommand(new ReResolveEndpointAddress(endpoint, channelEndpoint, address));
        }
    }

    void onReResolveControl(
        final String control,
        final UdpChannel udpChannel,
        final ReceiveChannelEndpoint channelEndpoint,
        final InetSocketAddress address)
    {
        if (ChannelEndpointStatus.ACTIVE == channelEndpoint.status())
        {
            scheduleDriverCommand(new ReResolveControlAddress(control, udpChannel, channelEndpoint, address));
        }
    }

    IpcPublication findSharedIpcPublication(final long streamId, final long responseCorrelationId)
    {
        for (final IpcPublication publication : ipcPublications)
        {
            if (publication.streamId() == streamId &&
                !publication.isExclusive() &&
                IpcPublication.State.ACTIVE == publication.state() &&
                publication.responseCorrelationId() == responseCorrelationId)
            {
                return publication;
            }
        }

        return null;
    }

    IpcPublication getIpcPublication(final long registrationId)
    {
        for (final IpcPublication publication : ipcPublications)
        {
            if (publication.registrationId() == registrationId)
            {
                return publication;
            }
        }

        return null;
    }

    NetworkPublication findNetworkPublicationByTag(final long tag)
    {
        for (int i = 0, size = networkPublications.size(); i < size; i++)
        {
            final NetworkPublication publication = networkPublications.get(i);
            final long publicationTag = publication.tag();
            if (publicationTag == tag && publicationTag != ChannelUri.INVALID_TAG)
            {
                return publication;
            }
        }

        return null;
    }

    IpcPublication findIpcPublicationByTag(final long tag)
    {
        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            final IpcPublication publication = ipcPublications.get(i);
            final long publicationTag = publication.tag();
            if (publicationTag == tag && publicationTag != ChannelUri.INVALID_TAG)
            {
                return publication;
            }
        }

        return null;
    }

    void onAddNetworkPublication(
        final String channel,
        final int streamId,
        final long correlationId,
        final long clientId,
        final boolean isExclusive)
    {
        scheduleClientCommand(new AddNetworkPublicationCommand(
            channel, streamId, correlationId, clientId, isExclusive));
    }

    void newPublicationImage(
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int activeTermId,
        final int termOffset,
        final int termBufferLength,
        final int senderMtuLength,
        final int transportIndex,
        final short flags,
        final InetSocketAddress controlAddress,
        final InetSocketAddress sourceAddress,
        final ReceiveChannelEndpoint channelEndpoint,
        final long registrationId,
        final SubscriptionParams subscriptionParams,
        final RawLog rawLog)
    {
        final long joinPosition = computePosition(
            activeTermId, termOffset, LogBufferDescriptor.positionBitsToShift(termBufferLength), initialTermId);
        final ArrayList<SubscriberPosition> subscriberPositions =
            createSubscriberPositions(sessionId, streamId, channelEndpoint, joinPosition);
        if (subscriberPositions.isEmpty())
        {
            nativeResourceAgentProxy.freeLogBuffer(rawLog); // the file will not be used
            return;
        }

        final UdpChannel subscriptionChannel = channelEndpoint.subscriptionUdpChannel();
        final SubscriptionLink subscription = subscriberPositions.get(0).subscription();
        final boolean isMulticastSemantics =
            isMulticastSemantics(subscriptionChannel, subscription.group(), flags);
        final boolean isReliable = subscription.isReliable();

        CongestionControl congestionControl = null;
        UnsafeBufferPosition hwmPos = null, rcvPos = null;
        AtomicCounter rcvNaksSent = null;
        try
        {
            congestionControl = ctx.congestionControlSupplier().newInstance(
                registrationId,
                subscriptionChannel,
                streamId,
                sessionId,
                termBufferLength,
                senderMtuLength,
                controlAddress,
                sourceAddress,
                ctx.receiverCachedNanoClock(),
                ctx,
                countersManager);

            final String uri = subscription.channel();
            final long clientId = subscription.aeronClient().clientId();
            hwmPos = ReceiverHwm.allocate(
                tempBuffer, countersManager, clientId, registrationId, sessionId, streamId, uri);
            rcvPos = ReceiverPos.allocate(
                tempBuffer, countersManager, clientId, registrationId, sessionId, streamId, uri);
            rcvNaksSent = ReceiverNaksSent.allocate(
                tempBuffer, countersManager, clientId, registrationId, sessionId, streamId, uri);

            final String sourceIdentity = Configuration.sourceIdentity(sourceAddress);

            final PublicationImage image = new PublicationImage(
                registrationId,
                ctx,
                channelEndpoint,
                transportIndex,
                controlAddress,
                sessionId,
                streamId,
                initialTermId,
                activeTermId,
                termOffset,
                flags,
                isReliable,
                subscriptionParams.untetheredWindowLimitTimeoutNs,
                subscriptionParams.untetheredLingerTimeoutNs,
                subscriptionParams.untetheredRestingTimeoutNs,
                rawLog,
                resolveDelayGenerator(ctx, subscriptionChannel, isMulticastSemantics, isReliable),
                subscriberPositions,
                hwmPos,
                rcvPos,
                rcvNaksSent,
                sourceIdentity,
                congestionControl);

            channelEndpoint.incRefImages();
            publicationImages.add(image);
            receiverProxy.newPublicationImage(channelEndpoint, image);

            for (final SubscriberPosition position : subscriberPositions)
            {
                position.addLink(image);

                final int positionCounterId = position.positionCounterId();
                countersManager.setCounterReferenceId(positionCounterId, registrationId);

                clientProxy.onAvailableImage(
                    registrationId,
                    streamId,
                    sessionId,
                    position.subscription().registrationId(),
                    positionCounterId,
                    rawLog.fileName(),
                    sourceIdentity);
            }
        }
        catch (final Exception ex)
        {
            recordError(ex);
            subscriberPositions.forEach((subscriberPosition) -> subscriberPosition.position().close());
            CloseHelper.quietCloseAll(congestionControl, hwmPos, rcvPos, rcvNaksSent);
            nativeResourceAgentProxy.freeLogBuffer(rawLog);
        }
    }

    private PublicationImage findPublicationImage(final long correlationId)
    {
        for (final PublicationImage publicationImage : publicationImages)
        {
            if (correlationId == publicationImage.correlationId())
            {
                return publicationImage;
            }
        }

        return null;
    }

    void responseSetup(final long responseCorrelationId, final int responseSessionId)
    {
        for (int i = 0, subscriptionLinksSize = subscriptionLinks.size(); i < subscriptionLinksSize; i++)
        {
            final SubscriptionLink subscriptionLink = subscriptionLinks.get(i);
            if (subscriptionLink.registrationId() == responseCorrelationId &&
                subscriptionLink instanceof final NetworkSubscriptionLink link)
            {
                if (subscriptionLink.hasSessionId())
                {
                    receiverProxy.requestSetup(
                        subscriptionLink.channelEndpoint(), subscriptionLink.streamId(), subscriptionLink.sessionId());
                }
                else
                {
                    link.sessionId(responseSessionId);
                    addNetworkSubscriptionToReceiver(link);
                    link.channelEndpoint().decResponseRefToStream(subscriptionLink.streamId);
                }

                break;
            }
        }
    }

    void responseConnected(final long responseCorrelationId)
    {
        for (final PublicationImage publicationImage : publicationImages)
        {
            if (publicationImage.correlationId() == responseCorrelationId)
            {
                if (publicationImage.hasSendResponseSetup())
                {
                    publicationImage.responseSessionId(null);
                }
            }
        }
    }

    private void validateResponseSubscription(final PublicationParams params)
    {
        if (!params.isResponse && NULL_VALUE != params.responseCorrelationId)
        {
            for (final SubscriptionLink subscriptionLink : subscriptionLinks)
            {
                if (params.responseCorrelationId == subscriptionLink.registrationId())
                {
                    return;
                }
            }

            throw new IllegalArgumentException(
                "unable to find response subscription for response-correlation-id=" + params.responseCorrelationId);
        }
    }

    void cleanupSpies(final NetworkPublication publication)
    {
        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink link = subscriptionLinks.get(i);
            if (link.isLinked(publication))
            {
                notifyUnavailableImageLink(publication.registrationId(), link);
                link.unlink(publication);
            }
        }
    }

    void notifyUnavailableImageLink(final long resourceId, final SubscriptionLink link)
    {
        clientProxy.onUnavailableImage(resourceId, link.registrationId(), link.streamId(), link.channel());
    }

    void notifyAvailableImageLink(
        final long resourceId,
        final int sessionId,
        final SubscriptionLink link,
        final int positionCounterId,
        final long joinPosition,
        final String logFileName,
        final String sourceIdentity)
    {
        countersManager.setCounterValue(positionCounterId, joinPosition);

        final int streamId = link.streamId();
        clientProxy.onAvailableImage(
            resourceId, streamId, sessionId, link.registrationId(), positionCounterId, logFileName, sourceIdentity);
    }

    void cleanupPublication(final NetworkPublication publication)
    {
        senderProxy.removeNetworkPublication(publication);

        final SendChannelEndpoint channelEndpoint = publication.channelEndpoint();
        if (channelEndpoint.shouldBeClosed())
        {
            channelEndpoint.indicateClosing();
            senderProxy.closeSendChannelEndpoint(channelEndpoint);
        }

        final String channel = channelEndpoint.udpChannel().canonicalForm();
        activeSessionSet.remove(new SessionKey(publication.sessionId(), publication.streamId(), channel));
    }

    // TODO rename, as it isn't closed yet
    void sendChannelEndpointClosed(final SendChannelEndpoint channelEndpoint)
    {
        final String channel = channelEndpoint.udpChannel().canonicalForm();
        sendChannelEndpointByChannelMap.remove(channel);
        channelEndpoint.close();
    }

    void cleanupSubscriptionLink(final SubscriptionLink subscription)
    {
        final ReceiveChannelEndpoint channelEndpoint = subscription.channelEndpoint();
        if (null != channelEndpoint)
        {
            if (subscription.hasSessionId())
            {
                if (0 == channelEndpoint.decRefToStreamAndSession(subscription.streamId(), subscription.sessionId()))
                {
                    receiverProxy.removeSubscription(
                        channelEndpoint, subscription.streamId(), subscription.sessionId());
                }
            }
            else if (subscription.isResponse())
            {
                channelEndpoint.decResponseRefToStream(subscription.streamId());
            }
            else
            {
                if (0 == channelEndpoint.decRefToStream(subscription.streamId()))
                {
                    receiverProxy.removeSubscription(channelEndpoint, subscription.streamId());
                }
            }

            tryCloseReceiveChannelEndpoint(channelEndpoint);
        }
    }

    void transitionToLinger(final PublicationImage image)
    {
        boolean rejoin = true;

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink link = subscriptionLinks.get(i);
            if (link.isLinked(image))
            {
                rejoin = link.isRejoin();
                notifyUnavailableImageLink(image.correlationId(), link);
            }
        }

        if (rejoin)
        {
            receiverProxy.removeCoolDown(image.channelEndpoint(), image.sessionId(), image.streamId());
        }
    }

    void transitionToLinger(final IpcPublication publication)
    {
        activeSessionSet.remove(new SessionKey(publication.sessionId(), publication.streamId(), IPC_MEDIA));

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink link = subscriptionLinks.get(i);
            if (link.isLinked(publication))
            {
                notifyUnavailableImageLink(publication.registrationId(), link);
            }
        }
    }

    void cleanupImage(final PublicationImage image)
    {
        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            subscriptionLinks.get(i).unlink(image);
        }
    }

    void cleanupIpcPublication(final IpcPublication publication)
    {
        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            subscriptionLinks.get(i).unlink(publication);
        }
    }

    void unlinkIpcSubscriptions(final IpcPublication publication)
    {
        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink link = subscriptionLinks.get(i);
            if (link.isLinked(publication))
            {
                notifyUnavailableImageLink(publication.registrationId(), link);
                link.unlink(publication);
            }
        }
    }

    void tryCloseReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        if (channelEndpoint.shouldBeClosed())
        {
            channelEndpoint.indicateClosing();
            receiverProxy.closeReceiveChannelEndpoint(channelEndpoint);
        }
    }

    // TODO rename, as it isn't closed yet
    void receiveChannelEndpointClosed(final ReceiveChannelEndpoint channelEndpoint)
    {
        final String channel = channelEndpoint.subscriptionUdpChannel().canonicalForm();
        receiveChannelEndpointByChannelMap.remove(channel);
        channelEndpoint.close();
    }

    void clientTimeout(final long clientId)
    {
        clientProxy.onClientTimeout(clientId);
    }

    void unavailableCounter(final long registrationId, final int counterId)
    {
        clientProxy.onUnavailableCounter(registrationId, counterId);
    }

    void onAddIpcPublication(
        final String channel,
        final int streamId,
        final long correlationId,
        final long clientId,
        final boolean isExclusive)
    {
        scheduleClientCommand(new AddIpcPublicationCommand(channel, streamId, correlationId, clientId, isExclusive));
    }

    void onRemovePublication(final long registrationId, final long correlationId, final boolean revoke)
    {
        PublicationLink publicationLink = null;
        final ArrayList<PublicationLink> publicationLinks = this.publicationLinks;
        for (int i = 0, size = publicationLinks.size(); i < size; i++)
        {
            final PublicationLink publication = publicationLinks.get(i);
            if (registrationId == publication.registrationId())
            {
                publicationLink = publication;
                fastUnorderedRemove(publicationLinks, i);
                break;
            }
        }

        if (null == publicationLink)
        {
            throw new ControlProtocolException(UNKNOWN_PUBLICATION, "unknown publication: " + registrationId);
        }

        if (revoke)
        {
            publicationLink.revoke();
        }
        publicationLink.close();
        clientProxy.operationSucceeded(correlationId);
    }

    void onAddSendDestination(final long registrationId, final String destinationChannel, final long correlationId)
    {
        scheduleClientCommand(new AddSendDestinationCommand(registrationId, destinationChannel, correlationId));
    }

    void onRemoveSendDestination(final long registrationId, final String destinationChannel, final long correlationId)
    {
        scheduleClientCommand(new RemoveSendDestinationCommand(registrationId, destinationChannel, correlationId));
    }

    void onRemoveSendDestination(
        final long publicationRegistrationId, final long destinationRegistrationId, final long correlationId)
    {
        final SendChannelEndpoint sendChannelEndpoint =
            findExistingManualSendChannelEndpoint(publicationRegistrationId);
        senderProxy.removeDestination(sendChannelEndpoint, destinationRegistrationId);
        clientProxy.operationSucceeded(correlationId);
    }

    void onAddNetworkSubscription(
        final String channel, final int streamId, final long registrationId, final long clientId)
    {
        scheduleClientCommand(new AddNetworkSubscriptionCommand(channel, streamId, registrationId, clientId));
    }

    private void addNetworkSubscriptionToReceiver(final NetworkSubscriptionLink subscription)
    {
        final ReceiveChannelEndpoint channelEndpoint = subscription.channelEndpoint();

        if (subscription.hasSessionId())
        {
            if (1 == channelEndpoint.incRefToStreamAndSession(subscription.streamId(), subscription.sessionId()))
            {
                receiverProxy.addSubscription(channelEndpoint, subscription.streamId(), subscription.sessionId());
            }
        }
        else
        {
            if (1 == channelEndpoint.incRefToStream(subscription.streamId()))
            {
                receiverProxy.addSubscription(channelEndpoint, subscription.streamId());
            }
        }
    }

    void onAddIpcSubscription(final String channel, final int streamId, final long registrationId, final long clientId)
    {
        final SubscriptionParams params = SubscriptionParams.getSubscriptionParams(parseUri(channel), ctx, 0);
        final IpcSubscriptionLink subscriptionLink = new IpcSubscriptionLink(
            registrationId, streamId, channel, getOrAddClient(clientId), params);

        subscriptionLinks.add(subscriptionLink);
        clientProxy.onSubscriptionReady(registrationId, ChannelEndpointStatus.NO_ID_ALLOCATED);

        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            final IpcPublication publication = ipcPublications.get(i);
            if (subscriptionLink.matches(publication) && publication.isAcceptingSubscriptions())
            {
                clientProxy.onAvailableImage(
                    publication.registrationId(),
                    streamId,
                    publication.sessionId(),
                    registrationId,
                    linkIpcSubscription(publication, subscriptionLink).id(),
                    publication.rawLog().fileName(),
                    IPC_CHANNEL);
            }
        }
    }

    void onAddSpySubscription(final String channel, final int streamId, final long registrationId, final long clientId)
    {
        scheduleClientCommand(new AddSpySubscriptionCommand(channel, streamId, registrationId, clientId));
    }

    void onRemoveSubscription(final long registrationId, final long correlationId)
    {
        boolean isAnySubscriptionFound = false;
        for (int lastIndex = subscriptionLinks.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final SubscriptionLink subscription = subscriptionLinks.get(i);
            if (subscription.registrationId() == registrationId)
            {
                fastUnorderedRemove(subscriptionLinks, i, lastIndex--);

                subscription.close();
                cleanupSubscriptionLink(subscription);
                isAnySubscriptionFound = true;
            }
        }

        if (!isAnySubscriptionFound)
        {
            throw new UnknownSubscriptionException("unknown subscription: " + registrationId);
        }

        clientProxy.operationSucceeded(correlationId);
    }

    void onClientKeepalive(final long clientId)
    {
        final AeronClient client = findClient(clients, clientId);
        if (null != client)
        {
            client.timeOfLastKeepaliveMs(cachedEpochClock.time());
        }
    }

    void onAddCounter(
        final int typeId,
        final DirectBuffer keyBuffer,
        final int keyOffset,
        final int keyLength,
        final DirectBuffer labelBuffer,
        final int labelOffset,
        final int labelLength,
        final long correlationId,
        final long clientId)
    {
        final AeronClient client = getOrAddClient(clientId);
        final AtomicCounter counter = countersManager.newCounter(
            typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength);

        countersManager.setCounterRegistrationId(counter.id(), correlationId);
        countersManager.setCounterOwnerId(counter.id(), clientId);
        counterLinks.add(new CounterLink(counter, correlationId, client));
        clientProxy.onCounterReady(correlationId, counter.id());
    }

    void onAddStaticCounter(
        final int typeId,
        final DirectBuffer keyBuffer,
        final int keyOffset,
        final int keyLength,
        final DirectBuffer labelBuffer,
        final int labelOffset,
        final int labelLength,
        final long registrationId,
        final long correlationId,
        final long clientId)
    {
        getOrAddClient(clientId);

        final int counterId = countersManager.findByTypeIdAndRegistrationId(typeId, registrationId);
        if (CountersReader.NULL_COUNTER_ID != counterId)
        {
            if (NULL_VALUE != countersManager.getCounterOwnerId(counterId))
            {
                clientProxy.onError(correlationId, GENERIC_ERROR, "cannot add static counter, because a " +
                    "non-static counter exists (counterId=" + counterId + ") for typeId=" + typeId + " and " +
                    "registrationId=" + registrationId);
            }
            else
            {
                clientProxy.onStaticCounter(correlationId, counterId);
            }
        }
        else
        {
            final AtomicCounter counter = countersManager.newCounter(
                typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength);

            countersManager.setCounterRegistrationId(counter.id(), registrationId);
            countersManager.setCounterOwnerId(counter.id(), NULL_VALUE);
            clientProxy.onStaticCounter(correlationId, counter.id());
        }
    }

    void onRemoveCounter(final long registrationId, final long correlationId)
    {
        CounterLink counterLink = null;
        final ArrayList<CounterLink> counterLinks = this.counterLinks;
        for (int i = 0, size = counterLinks.size(); i < size; i++)
        {
            final CounterLink link = counterLinks.get(i);
            if (registrationId == link.registrationId())
            {
                counterLink = link;
                fastUnorderedRemove(counterLinks, i);
                break;
            }
        }

        if (null == counterLink)
        {
            throw new ControlProtocolException(UNKNOWN_COUNTER, "unknown counter: " + registrationId);
        }

        clientProxy.operationSucceeded(correlationId);
        clientProxy.onUnavailableCounter(registrationId, counterLink.counterId());
        counterLink.close();
    }

    void onClientClose(final long clientId)
    {
        final AeronClient client = findClient(clients, clientId);
        if (null != client)
        {
            client.onClosedByCommand();
        }
    }

    void onAddRcvDestination(final long registrationId, final String destinationChannel, final long correlationId)
    {
        if (destinationChannel.startsWith(IPC_CHANNEL))
        {
            onAddRcvIpcDestination(registrationId, destinationChannel, correlationId);
        }
        else if (destinationChannel.startsWith(SPY_QUALIFIER))
        {
            onAddRcvSpyDestination(registrationId, destinationChannel, correlationId);
        }
        else
        {
            onAddRcvNetworkDestination(registrationId, destinationChannel, correlationId);
        }
    }

    void onAddRcvIpcDestination(final long registrationId, final String destinationChannel, final long correlationId)
    {
        final SubscriptionParams params =
            SubscriptionParams.getSubscriptionParams(parseUri(destinationChannel), ctx, 0);
        final SubscriptionLink mdsSubscriptionLink = findMdsSubscriptionLink(subscriptionLinks, registrationId);

        if (null == mdsSubscriptionLink)
        {
            throw new UnknownSubscriptionException("unknown MDS subscription: " + registrationId);
        }

        final IpcSubscriptionLink subscriptionLink = new IpcSubscriptionLink(
            registrationId,
            mdsSubscriptionLink.streamId(),
            destinationChannel,
            mdsSubscriptionLink.aeronClient(),
            params);

        subscriptionLinks.add(subscriptionLink);
        clientProxy.operationSucceeded(correlationId);

        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            final IpcPublication publication = ipcPublications.get(i);
            if (subscriptionLink.matches(publication) && publication.isAcceptingSubscriptions())
            {
                clientProxy.onAvailableImage(
                    publication.registrationId(),
                    mdsSubscriptionLink.streamId(),
                    publication.sessionId(),
                    registrationId,
                    linkIpcSubscription(publication, subscriptionLink).id(),
                    publication.rawLog().fileName(),
                    IPC_CHANNEL);
            }
        }
    }

    void onAddRcvSpyDestination(final long registrationId, final String destinationChannel, final long correlationId)
    {
        scheduleClientCommand(new AddRcvSpyDestinationCommand(registrationId, destinationChannel, correlationId));
    }

    void onAddRcvNetworkDestination(
        final long registrationId, final String destinationChannel, final long correlationId)
    {
        scheduleClientCommand(new AddRcvNetworkDestinationCommand(registrationId, destinationChannel, correlationId));
    }

    void onRemoveRcvDestination(final long registrationId, final String destinationChannel, final long correlationId)
    {
        if (destinationChannel.startsWith(IPC_CHANNEL) || destinationChannel.startsWith(SPY_QUALIFIER))
        {
            onRemoveRcvIpcOrSpyDestination(registrationId, destinationChannel, correlationId);
        }
        else
        {
            onRemoveRcvNetworkDestination(registrationId, destinationChannel, correlationId);
        }
    }

    void onRemoveRcvIpcOrSpyDestination(
        final long registrationId, final String destinationChannel, final long correlationId)
    {
        final SubscriptionLink subscription =
            removeSubscriptionLink(subscriptionLinks, registrationId, destinationChannel);

        if (null == subscription)
        {
            throw new UnknownSubscriptionException("unknown subscription: " + registrationId);
        }

        subscription.close();
        cleanupSubscriptionLink(subscription);
        clientProxy.operationSucceeded(correlationId);
        subscription.notifyUnavailableImages(this);
    }

    void onRemoveRcvNetworkDestination(
        final long registrationId, final String destinationChannel, final long correlationId)
    {
        scheduleClientCommand(new RemoveRcvNetworkDestinationCommand(
            registrationId, destinationChannel, correlationId));
    }

    void closeReceiveDestination(final ReceiveDestinationTransport destinationTransport)
    {
        destinationTransport.close();
    }

    void onTerminateDriver(final DirectBuffer tokenBuffer, final int tokenOffset, final int tokenLength)
    {
        if (ctx.terminationValidator().allowTermination(ctx.aeronDirectory(), tokenBuffer, tokenOffset, tokenLength))
        {
            ctx.terminationHook().run();
        }
    }

    void onRejectImage(
        final long correlationId,
        final long imageCorrelationId,
        final long position,
        final String reason)
    {
        if (reason.length() > ErrorFlyweight.MAX_ERROR_MESSAGE_LENGTH)
        {
            throw new ControlProtocolException(GENERIC_ERROR, "Invalidation reason must be " +
                ErrorFlyweight.MAX_ERROR_MESSAGE_LENGTH + " bytes or less");
        }

        final PublicationImage publicationImage = findPublicationImage(imageCorrelationId);

        if (null == publicationImage)
        {
            final IpcPublication foundPublication = getIpcPublication(imageCorrelationId);

            if (null == foundPublication)
            {
                throw new ControlProtocolException(
                    GENERIC_ERROR, "Unable to resolve image for correlationId=" + imageCorrelationId);
            }

            foundPublication.reject(position, reason, this, cachedNanoClock.nanoTime());
        }
        else
        {
            receiverProxy.rejectImage(imageCorrelationId, position, reason);
        }

        imagesRejected.incrementRelease();

        clientProxy.operationSucceeded(correlationId);
    }

    void onNextAvailableSessionId(final long correlationId, final int streamId)
    {
        outer:
        while (true)
        {
            final int sessionId = advanceSessionId();

            for (final SessionKey key : activeSessionSet)
            {
                if (streamId == key.streamId && sessionId == key.sessionId)
                {
                    continue outer;
                }
            }

            clientProxy.onNextAvailableSessionId(correlationId, sessionId);
            break;
        }
    }

    int nextAvailableSessionId(final int streamId, final String channel)
    {
        final SessionKey sessionKey = new SessionKey(streamId, channel);
        while (true)
        {
            final int sessionId = advanceSessionId();

            sessionKey.sessionId = sessionId;
            if (!activeSessionSet.contains(sessionKey))
            {
                return sessionId;
            }
        }
    }

    private int advanceSessionId()
    {
        int sessionId = nextSessionId++;

        if (ctx.publicationReservedSessionIdLow() <= sessionId &&
            sessionId <= ctx.publicationReservedSessionIdHigh())
        {
            nextSessionId = ctx.publicationReservedSessionIdHigh() + 1;
            sessionId = nextSessionId++;
        }
        return sessionId;
    }

    private void heartbeatAndCheckTimers(final long nowNs)
    {
        final long nowMs = cachedEpochClock.time();
        toDriverCommands.consumerHeartbeatTime(nowMs);

        checkManagedResources(clients, nowNs, nowMs);
        checkManagedResources(publicationLinks, nowNs, nowMs);
        checkManagedResources(networkPublications, nowNs, nowMs);
        checkManagedResources(subscriptionLinks, nowNs, nowMs);
        checkManagedResources(publicationImages, nowNs, nowMs);
        checkManagedResources(ipcPublications, nowNs, nowMs);
        checkManagedResources(counterLinks, nowNs, nowMs);
    }

    private void checkForBlockedToDriverCommands(final long nowNs)
    {
        final long consumerPosition = toDriverCommands.consumerPosition();

        if (consumerPosition == lastCommandConsumerPosition && toDriverCommands.producerPosition() > consumerPosition)
        {
            if ((timeOfLastToDriverPositionChangeNs + clientLivenessTimeoutNs) - nowNs < 0)
            {
                if (toDriverCommands.unblock())
                {
                    ctx.systemCounters().get(UNBLOCKED_COMMANDS).incrementRelease();
                }
            }
        }
        else
        {
            timeOfLastToDriverPositionChangeNs = nowNs;
            lastCommandConsumerPosition = consumerPosition;
        }
    }

    private static ChannelUri parseUri(final String channel)
    {
        try
        {
            return ChannelUri.parse(channel);
        }
        catch (final Exception ex)
        {
            throw new InvalidChannelException(ex);
        }
    }

    private record StreamInterest(boolean sparse, boolean reliable, boolean multicastSemantics)
    {
    }

    private StreamInterest findSubscribers(
        final ReceiveChannelEndpoint channelEndpoint, final int sessionId, final int streamId, final short flags)
    {
        long regId = Long.MAX_VALUE;
        boolean hasSubscribers = false;
        boolean sparse = false, reliable = false, multicastSemantics = false;

        for (final SubscriptionLink subscription : subscriptionLinks)
        {
            if (subscription.matches(channelEndpoint, streamId, sessionId))
            {
                if (!hasSubscribers)
                {
                    reliable = subscription.isReliable();
                    multicastSemantics = isMulticastSemantics(channelEndpoint.udpChannel(), subscription.group, flags);
                }
                hasSubscribers = true;

                if (subscription.registrationId < regId)
                {
                    sparse = subscription.isSparse();
                    regId = subscription.registrationId();
                }
            }
        }

        if (hasSubscribers)
        {
            return new StreamInterest(sparse, reliable, multicastSemantics);
        }

        return null;
    }

    private ArrayList<SubscriberPosition> createSubscriberPositions(
        final int sessionId,
        final int streamId,
        final ReceiveChannelEndpoint channelEndpoint,
        final long joinPosition)
    {
        final ArrayList<SubscriberPosition> subscriberPositions = new ArrayList<>();

        for (final SubscriptionLink subscription : subscriptionLinks)
        {
            if (subscription.matches(channelEndpoint, streamId, sessionId))
            {
                final Position position = SubscriberPos.allocate(
                    tempBuffer,
                    countersManager,
                    subscription.aeronClient().clientId(),
                    subscription.registrationId(),
                    sessionId,
                    streamId,
                    subscription.channel(),
                    joinPosition);

                position.setRelease(joinPosition);
                subscriberPositions.add(new SubscriberPosition(subscription, null, position));
            }
        }

        return subscriberPositions;
    }

    private void scheduleClientCommand(final ClientCommand cmd)
    {
        if (null != clientCommand)
        {
            throw new IllegalStateException("Another client command already in progress");
        }

        clientCommand = cmd;
    }

    private void scheduleDriverCommand(final Command cmd)
    {
        if (null != driverCommand)
        {
            throw new IllegalStateException("Another driver command already in progress");
        }

        driverCommand = cmd;
    }

    private NetworkPublication findPublication(
        final int streamId,
        final SendChannelEndpoint channelEndpoint,
        final long responseCorrelationId)
    {
        for (final NetworkPublication publication : networkPublications)
        {
            if (streamId == publication.streamId() &&
                channelEndpoint == publication.channelEndpoint() &&
                NetworkPublication.State.ACTIVE == publication.state() &&
                !publication.isExclusive() &&
                publication.responseCorrelationId() == responseCorrelationId)
            {
                return publication;
            }
        }

        return null;
    }

    @SuppressWarnings("MethodLength")
    private NetworkPublication newNetworkPublication(
        final long registrationId,
        final long clientId,
        final int streamId,
        final String channel,
        final UdpChannel udpChannel,
        final SendChannelEndpoint channelEndpoint,
        final RawLog rawLog,
        final PublicationParams params,
        final boolean isExclusive)
    {
        UnsafeBufferPosition publisherPos = null;
        UnsafeBufferPosition publisherLmt = null;
        UnsafeBufferPosition senderPos = null;
        UnsafeBufferPosition senderLmt = null;
        AtomicCounter senderBpe = null;
        AtomicCounter senderNaksReceived = null;
        try
        {
            final FlowControl flowControl = udpChannel.isMulticast() || udpChannel.isMultiDestination() ?
                ctx.multicastFlowControlSupplier().newInstance(udpChannel, streamId, registrationId) :
                ctx.unicastFlowControlSupplier().newInstance(udpChannel, streamId, registrationId);
            flowControl.initialize(
                ctx,
                countersManager,
                udpChannel,
                streamId,
                params.sessionId,
                registrationId,
                params.initialTermId,
                params.termLength);

            publisherPos = PublisherPos.allocate(
                tempBuffer,
                countersManager,
                clientId,
                registrationId,
                params.sessionId,
                streamId,
                channel,
                isExclusive);
            publisherLmt = PublisherLimit.allocate(
                tempBuffer, countersManager, clientId, registrationId, params.sessionId, streamId, channel);
            senderPos = SenderPos.allocate(
                tempBuffer, countersManager, clientId, registrationId, params.sessionId, streamId, channel);
            senderLmt = SenderLimit.allocate(
                tempBuffer, countersManager, clientId, registrationId, params.sessionId, streamId, channel);
            senderBpe = SenderBpe.allocate(
                tempBuffer, countersManager, clientId, registrationId, params.sessionId, streamId, channel);
            senderNaksReceived = SenderNaksReceived.allocate(
                tempBuffer, countersManager, clientId, registrationId, params.sessionId, streamId, channel);

            final AtomicCounter retransmitOverflowCounter = ctx.systemCounters().get(RETRANSMIT_OVERFLOW);

            if (params.hasPosition)
            {
                final int bits = LogBufferDescriptor.positionBitsToShift(params.termLength);
                final long position = computePosition(params.termId, params.termOffset, bits, params.initialTermId);
                publisherPos.setRelease(position);
                publisherLmt.setRelease(position);
                senderPos.setRelease(position);
                senderLmt.setRelease(position);
            }

            final RetransmitHandler retransmitHandler = new RetransmitHandler(
                ctx.senderCachedNanoClock(),
                ctx.systemCounters().get(INVALID_PACKETS),
                ctx.retransmitUnicastDelayGenerator(),
                ctx.retransmitUnicastLingerGenerator(),
                udpChannel.hasGroupSemantics(),
                params.maxResend,
                retransmitOverflowCounter);

            final NetworkPublication publication = new NetworkPublication(
                registrationId,
                ctx,
                params,
                channelEndpoint,
                rawLog,
                params.publicationWindowLength,
                publisherPos,
                publisherLmt,
                senderPos,
                senderLmt,
                senderBpe,
                senderNaksReceived,
                params.sessionId,
                streamId,
                params.initialTermId,
                flowControl,
                retransmitHandler,
                networkPublicationThreadLocals,
                isExclusive);

            channelEndpoint.incRef();
            networkPublications.add(publication);
            activeSessionSet.add(new SessionKey(params.sessionId, streamId, udpChannel.canonicalForm()));
            senderProxy.newNetworkPublication(publication);

            return publication;
        }
        catch (final Exception ex)
        {
            CloseHelper.quietCloseAll(
                publisherPos, publisherLmt, senderPos, senderLmt, senderBpe, senderNaksReceived);
            throw ex;
        }
    }

    private SendChannelEndpoint getOrCreateSendChannelEndpoint(
        final PublicationParams params, final UdpChannel udpChannel, final long registrationId)
    {
        SendChannelEndpoint channelEndpoint = findExistingSendChannelEndpoint(udpChannel);
        if (null == channelEndpoint)
        {
            AtomicCounter statusIndicator = null;
            AtomicCounter localSocketAddressIndicator = null;
            try
            {
                statusIndicator = SendChannelStatus.allocate(
                    tempBuffer, countersManager, registrationId, udpChannel.originalUriString());

                channelEndpoint = ctx.sendChannelEndpointSupplier().newInstance(udpChannel, statusIndicator, ctx);

                localSocketAddressIndicator = SendLocalSocketAddress.allocate(
                    tempBuffer, countersManager, registrationId, channelEndpoint.statusIndicatorCounterId());

                channelEndpoint.localSocketAddressIndicator(localSocketAddressIndicator);
                channelEndpoint.allocateDestinationsCounterForMdc(
                    tempBuffer, countersManager, registrationId, udpChannel.originalUriString());

                validateMtuForSndbuf(
                    params, channelEndpoint.socketSndbufLength(), ctx, udpChannel.originalUriString(), null);

                channelEndpoint.openChannel();
                channelEndpoint.indicateActive();

                senderProxy.registerSendChannelEndpoint(channelEndpoint);
                sendChannelEndpointByChannelMap.put(udpChannel.canonicalForm(), channelEndpoint);
            }
            catch (final Exception ex)
            {
                CloseHelper.closeAll(statusIndicator, localSocketAddressIndicator, channelEndpoint);
                throw ex;
            }
        }
        else
        {
            validateUdpChannelAgainstSendChannelEndpoint(params, udpChannel, channelEndpoint);
        }

        return channelEndpoint;
    }

    private void validateUdpChannelAgainstSendChannelEndpoint(
        final PublicationParams params, final UdpChannel udpChannel, final SendChannelEndpoint channelEndpoint)
    {
        validateChannelSendTimestampOffset(udpChannel, channelEndpoint);
        validateMtuForSndbuf(
            params,
            channelEndpoint.socketSndbufLength(),
            ctx,
            udpChannel.originalUriString(),
            channelEndpoint.originalUriString());
        validateChannelBufferLength(
            SOCKET_RCVBUF_PARAM_NAME,
            udpChannel.socketRcvbufLength(),
            channelEndpoint.socketRcvbufLength(),
            udpChannel.originalUriString(),
            channelEndpoint.originalUriString());
        validateChannelBufferLength(
            SOCKET_SNDBUF_PARAM_NAME,
            udpChannel.socketSndbufLength(),
            channelEndpoint.socketSndbufLength(),
            udpChannel.originalUriString(),
            channelEndpoint.originalUriString());
    }

    private static void validateChannelSendTimestampOffset(
        final UdpChannel udpChannel, final SendChannelEndpoint channelEndpoint)
    {
        if (udpChannel.channelSendTimestampOffset() != channelEndpoint.udpChannel().channelSendTimestampOffset())
        {
            throw new InvalidChannelException(
                "option conflicts with existing subscription: " + CHANNEL_SEND_TIMESTAMP_OFFSET_PARAM_NAME + "=" +
                    udpChannel.channelSendTimestampOffset() +
                    " existingChannel=" + channelEndpoint.originalUriString() + " channel=" +
                    udpChannel.originalUriString());
        }
    }

    private static void validateReceiveTimestampOffset(
        final UdpChannel udpChannel, final ReceiveChannelEndpoint channelEndpoint)
    {
        if (udpChannel.channelReceiveTimestampOffset() !=
            channelEndpoint.subscriptionUdpChannel().channelReceiveTimestampOffset())
        {
            throw new InvalidChannelException(
                "option conflicts with existing subscription: " + CHANNEL_RECEIVE_TIMESTAMP_OFFSET_PARAM_NAME + "=" +
                    udpChannel.channelReceiveTimestampOffset() +
                    " existingChannel=" + channelEndpoint.originalUriString() + " channel=" +
                    udpChannel.originalUriString());
        }
    }

    private SendChannelEndpoint findExistingManualSendChannelEndpoint(final long registrationId)
    {
        SendChannelEndpoint sendChannelEndpoint = null;

        for (final NetworkPublication publication : networkPublications)
        {
            if (registrationId == publication.registrationId())
            {
                sendChannelEndpoint = publication.channelEndpoint();
                break;
            }
        }

        if (null == sendChannelEndpoint)
        {
            throw new ControlProtocolException(UNKNOWN_PUBLICATION, "unknown publication: " + registrationId);
        }

        sendChannelEndpoint.validateAllowsManualControl();

        throwResourceTemporaryUnavailableIfEndpointIsClosing(sendChannelEndpoint);

        return sendChannelEndpoint;
    }

    private SendChannelEndpoint findExistingSendChannelEndpoint(final UdpChannel udpChannel)
    {
        if (udpChannel.hasTag())
        {
            for (final SendChannelEndpoint endpoint : sendChannelEndpointByChannelMap.values())
            {
                if (endpoint.matchesTag(udpChannel))
                {
                    return endpoint;
                }
            }

            if (!udpChannel.hasExplicitControl() && !udpChannel.isManualControlMode() &&
                !udpChannel.channelUri().containsKey(ENDPOINT_PARAM_NAME))
            {
                throw new InvalidChannelException(
                    "URI must have explicit control, endpoint, or be manual control-mode when original: channel=" +
                        udpChannel.originalUriString());
            }
        }

        SendChannelEndpoint endpoint = sendChannelEndpointByChannelMap.get(udpChannel.canonicalForm());
        if (null != endpoint && endpoint.udpChannel().hasTag() && udpChannel.hasTag() &&
            endpoint.udpChannel().tag() != udpChannel.tag())
        {
            endpoint = null;
        }

        if (null != endpoint)
        {
            throwResourceTemporaryUnavailableIfEndpointIsClosing(endpoint);
        }

        return endpoint;
    }

    private void checkForClashingSubscription(
        final SubscriptionParams params, final UdpChannel udpChannel, final int streamId)
    {
        final ReceiveChannelEndpoint channelEndpoint = findExistingReceiveChannelEndpoint(udpChannel);
        if (null != channelEndpoint)
        {
            validateUdpChannelAgainstReceiveChannelEndpoint(params, udpChannel, channelEndpoint);

            for (final SubscriptionLink subscription : subscriptionLinks)
            {
                final boolean matchesTag = !udpChannel.hasTag() || channelEndpoint.matchesTag(udpChannel);

                if (matchesTag && subscription.matches(channelEndpoint, streamId, params))
                {
                    if (params.isReliable != subscription.isReliable())
                    {
                        throw new InvalidChannelException(
                            "option conflicts with existing subscription: reliable=" + params.isReliable +
                                " existingChannel=" + subscription.channel() + " channel=" +
                                udpChannel.originalUriString());
                    }

                    if (params.isRejoin != subscription.isRejoin())
                    {
                        throw new InvalidChannelException(
                            "option conflicts with existing subscription: rejoin=" + params.isRejoin +
                                " existingChannel=" + subscription.channel() + " channel=" +
                                udpChannel.originalUriString());
                    }

                    if (params.isResponse != subscription.isResponse())
                    {
                        throw new InvalidChannelException(
                            "option conflicts with existing subscription: isResponse=" + params.isResponse +
                                " existingChannel=" + subscription.channel() + " channel=" +
                                udpChannel.originalUriString());
                    }
                }
            }
        }
    }

    private void linkMatchingImages(final SubscriptionLink subscriptionLink)
    {
        for (int i = 0, size = publicationImages.size(); i < size; i++)
        {
            final PublicationImage image = publicationImages.get(i);
            if (subscriptionLink.matches(image) && image.isAcceptingSubscriptions())
            {
                final long registrationId = subscriptionLink.registrationId();
                final long joinPosition = image.joinPosition();
                final int sessionId = image.sessionId();
                final int streamId = subscriptionLink.streamId();
                final Position position = SubscriberPos.allocate(
                    tempBuffer,
                    countersManager,
                    subscriptionLink.aeronClient().clientId(),
                    registrationId,
                    sessionId,
                    streamId,
                    subscriptionLink.channel(),
                    joinPosition);

                countersManager.setCounterReferenceId(position.id(), image.correlationId());

                position.setRelease(joinPosition);
                subscriptionLink.link(image, position);
                image.addSubscriber(subscriptionLink, position, cachedNanoClock.nanoTime());

                clientProxy.onAvailableImage(
                    image.correlationId(),
                    streamId,
                    sessionId,
                    registrationId,
                    position.id(),
                    image.rawLog().fileName(),
                    image.sourceIdentity());
            }
        }
    }

    void linkIpcSubscriptions(final IpcPublication publication)
    {
        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink subscription = subscriptionLinks.get(i);
            if (subscription.matches(publication) &&
                !subscription.isLinked(publication) &&
                publication.isAcceptingSubscriptions())
            {
                clientProxy.onAvailableImage(
                    publication.registrationId(),
                    publication.streamId(),
                    publication.sessionId(),
                    subscription.registrationId,
                    linkIpcSubscription(publication, subscription).id(),
                    publication.rawLog().fileName(),
                    IPC_CHANNEL);
            }
        }
    }

    private Position linkIpcSubscription(final IpcPublication publication, final SubscriptionLink subscription)
    {
        final long joinPosition = publication.joinPosition();
        final long registrationId = subscription.registrationId();
        final long clientId = subscription.aeronClient().clientId();
        final int sessionId = publication.sessionId();
        final int streamId = subscription.streamId();
        final String channel = subscription.channel();

        final Position position = SubscriberPos.allocate(
            tempBuffer, countersManager, clientId, registrationId, sessionId, streamId, channel, joinPosition);

        countersManager.setCounterReferenceId(position.id(), publication.registrationId());

        position.setRelease(joinPosition);
        subscription.link(publication, position);
        publication.addSubscriber(subscription, position, cachedNanoClock.nanoTime());

        return position;
    }

    private Position linkSpy(final NetworkPublication publication, final SubscriptionLink subscription)
    {
        final long joinPosition = publication.consumerPosition();
        final long registrationId = subscription.registrationId();
        final long clientId = subscription.aeronClient().clientId();
        final int streamId = publication.streamId();
        final int sessionId = publication.sessionId();
        final String channel = subscription.channel();

        final Position position = SubscriberPos.allocate(
            tempBuffer, countersManager, clientId, registrationId, sessionId, streamId, channel, joinPosition);

        countersManager.setCounterReferenceId(position.id(), publication.registrationId());

        position.setRelease(joinPosition);
        subscription.link(publication, position);
        publication.addSubscriber(subscription, position, cachedNanoClock.nanoTime());

        return position;
    }

    private ReceiveChannelEndpoint getOrCreateReceiveChannelEndpoint(
        final SubscriptionParams params, final UdpChannel udpChannel, final long registrationId)
    {
        ReceiveChannelEndpoint channelEndpoint = findExistingReceiveChannelEndpoint(udpChannel);
        if (null == channelEndpoint)
        {
            AtomicCounter channelStatus = null;
            AtomicCounter localSocketAddressIndicator = null;
            try
            {
                final String channel = udpChannel.originalUriString();
                channelStatus = ReceiveChannelStatus.allocate(tempBuffer, countersManager, registrationId, channel);

                final DataPacketDispatcher dispatcher = new DataPacketDispatcher(
                    ctx.driverConductorProxy(), receiverProxy.receiver(), ctx.streamSessionLimit());
                channelEndpoint = ctx.receiveChannelEndpointSupplier().newInstance(
                    udpChannel, dispatcher, channelStatus, ctx);

                if (!udpChannel.isManualControlMode())
                {
                    localSocketAddressIndicator = ReceiveLocalSocketAddress.allocate(
                        tempBuffer, countersManager, registrationId, channelEndpoint.statusIndicatorCounter().id());

                    channelEndpoint.localSocketAddressIndicator(localSocketAddressIndicator);
                }

                validateInitialWindowForRcvBuf(params, channel, channelEndpoint.socketRcvbufLength(), ctx, null);

                channelEndpoint.openChannel();
                channelEndpoint.indicateActive();

                receiverProxy.registerReceiveChannelEndpoint(channelEndpoint);
                receiveChannelEndpointByChannelMap.put(udpChannel.canonicalForm(), channelEndpoint);
            }
            catch (final Exception ex)
            {
                CloseHelper.closeAll(channelStatus, localSocketAddressIndicator, channelEndpoint);
                throw ex;
            }
        }
        else
        {
            validateUdpChannelAgainstReceiveChannelEndpoint(params, udpChannel, channelEndpoint);
        }

        return channelEndpoint;
    }

    private void validateUdpChannelAgainstReceiveChannelEndpoint(
        final SubscriptionParams params, final UdpChannel udpChannel, final ReceiveChannelEndpoint channelEndpoint)
    {
        validateReceiveTimestampOffset(udpChannel, channelEndpoint);
        validateInitialWindowForRcvBuf(
            params,
            udpChannel.originalUriString(),
            channelEndpoint.socketRcvbufLength(),
            ctx,
            channelEndpoint.originalUriString());
        validateChannelBufferLength(
            SOCKET_RCVBUF_PARAM_NAME,
            udpChannel.socketRcvbufLength(),
            channelEndpoint.socketRcvbufLength(),
            udpChannel.originalUriString(),
            channelEndpoint.originalUriString());
        validateChannelBufferLength(
            SOCKET_SNDBUF_PARAM_NAME,
            udpChannel.socketSndbufLength(),
            channelEndpoint.socketSndbufLength(),
            udpChannel.originalUriString(),
            channelEndpoint.originalUriString());
    }

    private ReceiveChannelEndpoint findExistingReceiveChannelEndpoint(final UdpChannel udpChannel)
    {
        if (udpChannel.hasTag())
        {
            for (final ReceiveChannelEndpoint endpoint : receiveChannelEndpointByChannelMap.values())
            {
                if (endpoint.matchesTag(udpChannel))
                {
                    return endpoint;
                }
            }
        }

        ReceiveChannelEndpoint endpoint = receiveChannelEndpointByChannelMap.get(udpChannel.canonicalForm());
        if (null != endpoint && endpoint.hasTag() && udpChannel.hasTag() && endpoint.tag() != udpChannel.tag())
        {
            endpoint = null;
        }

        if (null != endpoint)
        {
            throwResourceTemporaryUnavailableIfEndpointIsClosing(endpoint);
        }

        return endpoint;
    }

    private AeronClient getOrAddClient(final long clientId)
    {
        AeronClient client = findClient(clients, clientId);
        if (null == client)
        {
            final AtomicCounter counter = ClientHeartbeatTimestamp.allocate(tempBuffer, countersManager, clientId);
            final int counterId = counter.id();

            counter.setRelease(cachedEpochClock.time());
            countersManager.setCounterRegistrationId(counterId, clientId);
            countersManager.setCounterOwnerId(counterId, clientId);

            client = new AeronClient(
                clientId,
                clientLivenessTimeoutNs,
                ctx.systemCounters().get(SystemCounterDescriptor.CLIENT_TIMEOUTS),
                counter);
            clients.add(client);

            clientProxy.onCounterReady(clientId, counterId);
        }

        return client;
    }

    private IpcPublication newIpcPublication(
        final long registrationId,
        final long clientId,
        final int streamId,
        final String channel,
        final boolean isExclusive,
        final PublicationParams params,
        final RawLog rawLog)
    {
        UnsafeBufferPosition publisherPosition = null;
        UnsafeBufferPosition publisherLimit = null;
        try
        {
            publisherPosition = PublisherPos.allocate(
                tempBuffer,
                countersManager,
                clientId,
                registrationId,
                params.sessionId,
                streamId,
                channel,
                isExclusive);
            publisherLimit = PublisherLimit.allocate(
                tempBuffer, countersManager, clientId, registrationId, params.sessionId, streamId, channel);

            if (params.hasPosition)
            {
                final int positionBitsToShift = positionBitsToShift(params.termLength);
                final long position = computePosition(
                    params.termId, params.termOffset, positionBitsToShift, params.initialTermId);
                publisherPosition.setRelease(position);
                publisherLimit.setRelease(position);
            }

            final IpcPublication publication = new IpcPublication(
                registrationId,
                channel,
                ctx,
                params.entityTag,
                params.sessionId,
                streamId,
                publisherPosition,
                publisherLimit,
                rawLog,
                isExclusive,
                params);

            findAndUpdateResponseIpcSubscription(params, publication);

            ipcPublications.add(publication);
            activeSessionSet.add(new SessionKey(params.sessionId, streamId, IPC_MEDIA));

            return publication;
        }
        catch (final Exception ex)
        {
            CloseHelper.quietCloseAll(publisherPosition, publisherLimit);
            throw ex;
        }
    }

    private void findAndUpdateResponseIpcSubscription(final PublicationParams params, final IpcPublication publication)
    {
        if (NULL_VALUE != params.responseCorrelationId)
        {
            for (final IpcPublication ipcPublication : ipcPublications)
            {
                if (ipcPublication.registrationId() == params.responseCorrelationId)
                {
                    for (int i = 0, n = subscriptionLinks.size(); i < n; i++)
                    {
                        final SubscriptionLink subscriptionLink = subscriptionLinks.get(i);
                        if (ipcPublication.responseCorrelationId() == subscriptionLink.registrationId &&
                            subscriptionLink instanceof IpcSubscriptionLink)
                        {
                            subscriptionLink.sessionId(publication.sessionId());
                            break;
                        }
                    }

                    break;
                }
            }
        }
    }

    private static AeronClient findClient(final ArrayList<AeronClient> clients, final long clientId)
    {
        AeronClient aeronClient = null;

        for (int i = 0, size = clients.size(); i < size; i++)
        {
            final AeronClient client = clients.get(i);
            if (client.clientId() == clientId)
            {
                aeronClient = client;
                break;
            }
        }

        return aeronClient;
    }

    private static SubscriptionLink findMdsSubscriptionLink(
        final ArrayList<SubscriptionLink> subscriptionLinks, final long registrationId)
    {
        SubscriptionLink subscriptionLink = null;

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink subscription = subscriptionLinks.get(i);
            if (subscription.registrationId() == registrationId && subscription.supportsMds())
            {
                subscriptionLink = subscription;
                break;
            }
        }

        return subscriptionLink;
    }

    private static SubscriptionLink removeSubscriptionLink(
        final ArrayList<SubscriptionLink> subscriptionLinks, final long registrationId, final String channel)
    {
        SubscriptionLink subscriptionLink = null;

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink subscription = subscriptionLinks.get(i);
            if (subscription.registrationId() == registrationId && subscription.channel().equals(channel))
            {
                subscriptionLink = subscription;
                fastUnorderedRemove(subscriptionLinks, i);
                break;
            }
        }

        return subscriptionLink;
    }

    private void checkForSessionClash(
        final int sessionId, final int streamId, final String channel, final String originalChannel)
    {
        if (activeSessionSet.contains(new SessionKey(sessionId, streamId, channel)))
        {
            throw new InvalidChannelException("existing publication has clashing sessionId=" + sessionId +
                " for streamId=" + streamId + " channel=" + originalChannel);
        }
    }

    private <T extends DriverManagedResource> void checkManagedResources(
        final ArrayList<T> list, final long nowNs, final long nowMs)
    {
        for (int lastIndex = list.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final DriverManagedResource resource = list.get(i);

            resource.onTimeEvent(nowNs, nowMs, this);

            if (resource.hasReachedEndOfLife())
            {
                fastUnorderedRemove(list, i, lastIndex--);

                try
                {
                    resource.close();
                }
                catch (final Exception ex)
                {
                    recordError(ex);
                }

                if (resource instanceof IpcPublication ipcPublication)
                {
                    nativeResourceAgentProxy.freeLogBuffer(ipcPublication.rawLog());
                }
                else if (resource instanceof NetworkPublication networkPublication)
                {
                    nativeResourceAgentProxy.freeLogBuffer(networkPublication.rawLog());
                }
                else if (resource instanceof PublicationImage publicationImage)
                {
                    nativeResourceAgentProxy.freeLogBuffer(publicationImage.rawLog());
                }
            }
        }
    }

    private void linkSpies(final ArrayList<SubscriptionLink> links, final NetworkPublication publication)
    {
        for (int i = 0, size = links.size(); i < size; i++)
        {
            final SubscriptionLink subscription = links.get(i);
            if (subscription.matches(publication) && !subscription.isLinked(publication))
            {
                clientProxy.onAvailableImage(
                    publication.registrationId(),
                    publication.streamId(),
                    publication.sessionId(),
                    subscription.registrationId(),
                    linkSpy(publication, subscription).id(),
                    publication.rawLog().fileName(),
                    IPC_CHANNEL);
            }
        }
    }

    private void trackTime(final long nowNs)
    {
        cachedNanoClock.update(nowNs);
        dutyCycleTracker.measureAndUpdate(nowNs);

        if (clockUpdateDeadlineNs - nowNs < 0)
        {
            clockUpdateDeadlineNs = nowNs + CLOCK_UPDATE_INTERNAL_NS;
            cachedEpochClock.update(epochClock.time());
        }
    }

    private int processTimers(final long nowNs)
    {
        int workCount = 0;

        if (timerCheckDeadlineNs - nowNs < 0)
        {
            timerCheckDeadlineNs = nowNs + timerIntervalNs;
            heartbeatAndCheckTimers(nowNs);
            checkForBlockedToDriverCommands(nowNs);
            workCount = 1;
        }

        return workCount;
    }

    private int trackStreamPositions(final int existingWorkCount, final long nowNs)
    {
        int workCount = existingWorkCount;

        final ArrayList<PublicationImage> publicationImages = this.publicationImages;
        for (int i = 0, size = publicationImages.size(); i < size; i++)
        {
            workCount += publicationImages.get(i).trackRebuild(nowNs);
        }

        final ArrayList<NetworkPublication> networkPublications = this.networkPublications;
        for (int i = 0, size = networkPublications.size(); i < size; i++)
        {
            workCount += networkPublications.get(i).updatePublisherPositionAndLimit();
        }

        final ArrayList<IpcPublication> ipcPublications = this.ipcPublications;
        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            workCount += ipcPublications.get(i).updatePublisherPositionAndLimit();
        }

        return workCount;
    }

    private int processClientCommands()
    {
        int workCount = 0;

        if (null == clientCommand)
        {
            workCount += clientCommandAdapter.receive();
        }

        if (null != clientCommand)
        {
            ++workCount;

            try
            {
                if (clientCommand.execute())
                {
                    CloseHelper.quietClose(clientCommand);
                    clientCommand = null;
                }
            }
            catch (final Exception ex)
            {
                clientCommandAdapter.onError(clientCommand.correlationId, ex);
                CloseHelper.quietClose(clientCommand);
                clientCommand = null;
            }
        }

        return workCount;
    }

    private int drainCommandQueue()
    {
        int workCount = 0;

        if (null == driverCommand)
        {
            final Runnable command = driverCmdQueue.poll();
            if (null != command)
            {
                command.run();
                workCount++;
            }
        }
        else
        {
            try
            {
                if (driverCommand.execute())
                {
                    CloseHelper.quietClose(driverCommand);
                    driverCommand = null;
                }
            }
            catch (final Exception ex)
            {
                recordError(ex);
                CloseHelper.quietClose(driverCommand);
                driverCommand = null;
            }
            workCount++;
        }

        return workCount;
    }

    private static void validateChannelBufferLength(
        final String paramName,
        final int newLength,
        final int existingLength,
        final String channel,
        final String existingChannel)
    {
        if (0 != newLength && newLength != existingLength)
        {
            final Object existingValue = 0 == existingLength ? "OS default" : existingLength;
            throw new InvalidChannelException(
                paramName + "=" + newLength + " does not match existing value of " + existingValue +
                    ": existingChannel=" + existingChannel + " channel=" + channel);
        }
    }

    private static void validateEndpointForPublication(final UdpChannel udpChannel)
    {
        if (!udpChannel.isMultiDestination() && udpChannel.hasExplicitEndpoint() &&
            0 == udpChannel.remoteData().getPort())
        {
            throw new IllegalArgumentException(
                ENDPOINT_PARAM_NAME + " has port=0 for publication: channel=" + udpChannel.originalUriString());
        }
    }

    private static void validateControlForPublication(final UdpChannel udpChannel)
    {
        if (udpChannel.isDynamicControlMode() && !udpChannel.hasExplicitControl())
        {
            throw new IllegalArgumentException(
                "'control-mode=dynamic' requires that 'control' parameter is set, channel=" +
                    udpChannel.originalUriString());
        }

        if (udpChannel.hasExplicitControl() && !udpChannel.hasExplicitEndpoint() &&
            ControlMode.NONE == udpChannel.controlMode())
        {
            throw new IllegalArgumentException(
                "'control' parameter requires that either 'endpoint' or 'control-mode' is specified, channel=" +
                    udpChannel.originalUriString());
        }
    }

    private static void validateControlForSubscription(final UdpChannel udpChannel)
    {
        if (udpChannel.hasExplicitControl() &&
            0 == udpChannel.localControl().getPort())
        {
            throw new IllegalArgumentException(MDC_CONTROL_PARAM_NAME + " has port=0 for subscription: channel=" +
                udpChannel.originalUriString());
        }
    }

    private static void validateTimestampConfiguration(final UdpChannel udpChannel)
    {
        if (null != udpChannel.channelUri().get(MEDIA_RCV_TIMESTAMP_OFFSET_PARAM_NAME))
        {
            throw new InvalidChannelException(
                "Media timestamps '" + MEDIA_RCV_TIMESTAMP_OFFSET_PARAM_NAME +
                    "' are not supported in the Java driver: channel=" + udpChannel.originalUriString());
        }
    }

    private static void validateDestinationUri(final ChannelUri uri, final String destinationUri)
    {
        if (SPY_QUALIFIER.equals(uri.prefix()))
        {
            throw new InvalidChannelException("Aeron spies are invalid as send destinations: channel=" +
                destinationUri);
        }

        for (final String invalidKey : INVALID_DESTINATION_KEYS)
        {
            if (uri.containsKey(invalidKey))
            {
                throw new InvalidChannelException(
                    "destinations must not contain the key: " + invalidKey + " channel=" + destinationUri);
            }
        }

        if (Objects.equals(CONTROL_MODE_RESPONSE, uri.get(MDC_CONTROL_MODE_PARAM_NAME)))
        {
            throw new InvalidChannelException("destinations may not specify " +
                MDC_CONTROL_MODE_PARAM_NAME + "=" + CONTROL_MODE_RESPONSE);
        }
    }

    private static void validateSendDestinationUri(final ChannelUri uri, final String destinationUri)
    {
        final String endpoint = uri.get(ENDPOINT_PARAM_NAME);

        if (null != endpoint && endpoint.endsWith(":0"))
        {
            throw new InvalidChannelException(ENDPOINT_PARAM_NAME + " has port=0 for send destination: channel=" +
                destinationUri);
        }
    }

    @SuppressWarnings({ "unused", "UnnecessaryReturnStatement" })
    private static void validateExperimentalFeatures(final boolean enableExperimentalFeatures, final UdpChannel channel)
    {
        if (enableExperimentalFeatures)
        {
            return;
        }

        /*
         * Put experimental feature validation here.
         */
    }

    static FeedbackDelayGenerator resolveDelayGenerator(
        final Context ctx,
        final UdpChannel channel,
        final boolean isMulticastSemantics,
        final boolean isReliable)
    {
        if (!isReliable)
        {
            return StaticDelayGenerator.ZERO_DELAY_GENERATOR;
        }

        if (isMulticastSemantics)
        {
            return ctx.multicastFeedbackDelayGenerator();
        }

        final Long nakDelayNs = channel.nakDelayNs();
        if (null != nakDelayNs)
        {
            final long retryDelayNs =
                Math.max(nakDelayNs, Configuration.NAK_UNICAST_DELAY_MIN_VALUE_NS) * ctx.nakUnicastRetryDelayRatio();
            return new StaticDelayGenerator(nakDelayNs, retryDelayNs);
        }
        else
        {
            return ctx.unicastFeedbackDelayGenerator();
        }
    }

    static boolean isMulticastSemantics(
        final UdpChannel channel, final InferableBoolean receiverGroupConsideration, final short flags)
    {
        final boolean isGroupFromFlag = SetupFlyweight.GROUP_FLAG == (flags & SetupFlyweight.GROUP_FLAG);

        return INFER == receiverGroupConsideration ?
            (channel.isMulticast() || isGroupFromFlag) :
            FORCE_TRUE == receiverGroupConsideration;
    }

    private void recordError(final Exception ex)
    {
        ctx.countedErrorHandler().onError(ex);
    }

    private abstract static class Command implements AutoCloseable
    {
        abstract boolean execute();

        public void close()
        {
        }
    }

    private abstract static class ClientCommand extends Command
    {
        final long correlationId;

        private ClientCommand(final long correlationId)
        {
            this.correlationId = correlationId;
        }
    }

    private static void throwResourceTemporaryUnavailableIfEndpointIsClosing(final SendChannelEndpoint endpoint)
    {
        if (ChannelEndpointStatus.CLOSING == endpoint.status())
        {
            throw new ControlProtocolException(
                RESOURCE_TEMPORARILY_UNAVAILABLE,
                "SendChannelEndpoint found in CLOSING state, please retry");
        }
    }

    private static void throwResourceTemporaryUnavailableIfEndpointIsClosing(final ReceiveChannelEndpoint endpoint)
    {
        if (ChannelEndpointStatus.CLOSING == endpoint.status())
        {
            throw new ControlProtocolException(
                RESOURCE_TEMPORARILY_UNAVAILABLE,
                "ReceiveChannelEndpoint found in CLOSING state, please retry");
        }
    }

    private final class AddNetworkPublicationCommand extends ClientCommand
    {
        private final String channel;
        private final int streamId;
        private final long clientId;
        private final boolean isExclusive;
        private State state = State.INIT;
        private PublicationParams params;
        private UdpChannel udpChannel;
        private SendChannelEndpoint channelEndpoint;
        private PublicationImage responsePublicationImage;
        private NetworkPublication publication;
        private boolean isNewPublication;
        private CommandResult<UdpChannel> parseChannelResult;
        private CommandResult<RawLog> logBufferResult;
        private RawLog rawLog;

        private AddNetworkPublicationCommand(
            final String channel,
            final int streamId,
            final long correlationId,
            final long clientId,
            final boolean isExclusive)
        {
            super(correlationId);
            this.channel = channel;
            this.streamId = streamId;
            this.clientId = clientId;
            this.isExclusive = isExclusive;
        }

        boolean execute()
        {
            if (State.INIT == state)
            {
                parseChannelResult = nativeResourceAgentProxy.parseChannel(channel, false);
                state = State.PARSE_CHANNEL;
            }

            if (State.PARSE_CHANNEL == state)
            {
                parseChannel();
            }

            if (State.VALIDATE == state)
            {
                validate();
            }

            if (State.RESOLVE_PUBLICATION == state)
            {
                resolvePublication();
            }

            if (State.AWAIT_LOG_BUFFER == state)
            {
                awaitLogBuffer();
            }

            if (State.CREATE_PUBLICATION == state)
            {
                createPublication();
            }

            return State.DONE == state;
        }

        public void close()
        {
            if (null != rawLog)
            {
                nativeResourceAgentProxy.freeLogBuffer(rawLog);
            }
        }

        private void parseChannel()
        {
            if (null != (udpChannel = parseChannelResult.get()))
            {
                state = State.VALIDATE;
            }
        }

        private void validate()
        {
            final ChannelUri channelUri = udpChannel.channelUri();
            params = getPublicationParams(channelUri, ctx, DriverConductor.this, streamId, udpChannel.canonicalForm());
            validateExperimentalFeatures(ctx.enableExperimentalFeatures(), udpChannel);
            validateEndpointForPublication(udpChannel);
            validateControlForPublication(udpChannel);
            validateResponseSubscription(params);

            state = State.RESOLVE_PUBLICATION;
        }

        private void resolvePublication()
        {
            channelEndpoint = findExistingSendChannelEndpoint(udpChannel);
            if (null != channelEndpoint)
            {
                validateUdpChannelAgainstSendChannelEndpoint(params, udpChannel, channelEndpoint);

                if (!isExclusive)
                {
                    publication = findPublication(streamId, channelEndpoint, params.responseCorrelationId);
                }
            }

            if (null == publication)
            {
                checkForSessionClash(params.sessionId, streamId, udpChannel.canonicalForm(), channel);

                if (params.isResponse &&
                    PROTOTYPE_VALUE_CORRELATION_ID == params.responseCorrelationId)
                {
                    params.termLength = TERM_MIN_LENGTH;
                }

                final int socketRcvBufLength;
                final int socketSndBufLength;
                if (null != channelEndpoint)
                {
                    socketRcvBufLength = channelEndpoint.socketRcvbufLength();
                    socketSndBufLength = channelEndpoint.socketSndbufLength();
                }
                else
                {
                    socketRcvBufLength = udpChannel.socketRcvbufLength();
                    socketSndBufLength = udpChannel.socketSndbufLength();
                }

                logBufferResult = nativeResourceAgentProxy.newNetworkPublicationLog(
                    isExclusive,
                    streamId,
                    correlationId,
                    socketRcvBufLength,
                    socketSndBufLength,
                    params,
                    udpChannel.hasGroupSemantics());

                state = State.AWAIT_LOG_BUFFER;
            }
            else
            {
                confirmMatch(
                    udpChannel.channelUri(),
                    params,
                    publication.rawLog(),
                    publication.sessionId(),
                    publication.channel(),
                    publication.initialTermId(),
                    publication.startingTermId(),
                    publication.startingTermOffset());

                validateSpiesSimulateConnection(
                    params, publication.spiesSimulateConnection(), channel, publication.channel());

                responsePublicationImage = findResponsePublicationImage();

                linkPublication();
            }
        }

        private void awaitLogBuffer()
        {
            if (null != (rawLog = logBufferResult.get()))
            {
                state = State.CREATE_PUBLICATION;
            }
        }

        private void createPublication()
        {
            responsePublicationImage = findResponsePublicationImage();

            channelEndpoint = getOrCreateSendChannelEndpoint(params, udpChannel, correlationId);

            publication = newNetworkPublication(
                correlationId,
                clientId,
                streamId,
                channel,
                udpChannel,
                channelEndpoint,
                rawLog,
                params,
                isExclusive);

            rawLog = null; // ownership transferred to the NetworkPublication

            isNewPublication = true;
            linkPublication();
        }

        private void linkPublication()
        {
            publicationLinks.add(new PublicationLink(correlationId, getOrAddClient(clientId), publication));

            clientProxy.onPublicationReady(
                correlationId,
                publication.registrationId(),
                streamId,
                publication.sessionId(),
                publication.rawLog().fileName(),
                publication.publisherLimitId(),
                channelEndpoint.statusIndicatorCounterId(),
                isExclusive);

            if (isNewPublication)
            {
                linkSpies(subscriptionLinks, publication);
            }

            if (null != responsePublicationImage)
            {
                responsePublicationImage.responseSessionId(publication.sessionId());
            }

            state = State.DONE;
        }

        private PublicationImage findResponsePublicationImage()
        {
            if (!params.isResponse)
            {
                return null;
            }

            if (NULL_VALUE == params.responseCorrelationId)
            {
                throw new IllegalArgumentException(
                    "control-mode=response was specified, but no response-correlation-id set");
            }

            if (PROTOTYPE_VALUE_CORRELATION_ID == params.responseCorrelationId)
            {
                return null;
            }

            for (final PublicationImage publicationImage : publicationImages)
            {
                if (publicationImage.correlationId() == params.responseCorrelationId)
                {
                    if (publicationImage.hasSendResponseSetup())
                    {
                        return publicationImage;
                    }
                    else
                    {
                        throw new IllegalArgumentException(
                            "image.correlationId=" + params.responseCorrelationId +
                                " did not request a response channel");
                    }
                }
            }

            throw new IllegalArgumentException("image.correlationId=" + params.responseCorrelationId + " not found");
        }

        private enum State
        {
            INIT,
            PARSE_CHANNEL,
            VALIDATE,
            RESOLVE_PUBLICATION,
            AWAIT_LOG_BUFFER,
            CREATE_PUBLICATION,
            DONE
        }
    }

    private final class AddIpcPublicationCommand extends ClientCommand
    {
        private final String channel;
        private final int streamId;
        private final long clientId;
        private final boolean isExclusive;
        private State state = State.INIT;
        private ChannelUri channelUri;
        private PublicationParams params;
        private IpcPublication publication;
        private boolean isNewPublication;
        private CommandResult<RawLog> logBufferResult;
        private RawLog rawLog;

        AddIpcPublicationCommand(
            final String channel,
            final int streamId,
            final long correlationId,
            final long clientId,
            final boolean isExclusive)
        {
            super(correlationId);
            this.channel = channel;
            this.streamId = streamId;
            this.clientId = clientId;
            this.isExclusive = isExclusive;
        }

        boolean execute()
        {
            if (State.INIT == state)
            {
                init();
            }

            if (State.RESOLVE_PUBLICATION == state)
            {
                resolvePublication();
            }

            if (State.AWAIT_LOG_BUFFER == state)
            {
                awaitLogBuffer();
            }

            if (State.CREATE_PUBLICATION == state)
            {
                createPublication();
            }

            return State.DONE == state;
        }

        public void close()
        {
            if (null != rawLog)
            {
                nativeResourceAgentProxy.freeLogBuffer(rawLog);
            }
        }

        private void awaitLogBuffer()
        {
            if (null != (rawLog = logBufferResult.get()))
            {
                state = State.CREATE_PUBLICATION;
            }
        }

        private void init()
        {
            channelUri = parseUri(channel);
            params = getPublicationParams(channelUri, ctx, DriverConductor.this, streamId, IPC_MEDIA);
            state = State.RESOLVE_PUBLICATION;
        }

        private void resolvePublication()
        {
            if (!isExclusive)
            {
                publication = findSharedIpcPublication(streamId, params.responseCorrelationId);
            }

            if (null == publication)
            {
                checkForSessionClash(params.sessionId, streamId, IPC_MEDIA, channel);

                logBufferResult = nativeResourceAgentProxy.newIpcPublicationLog(
                    isExclusive, streamId, correlationId, params);

                state = State.AWAIT_LOG_BUFFER;
            }
            else
            {
                confirmMatch(
                    channelUri,
                    params,
                    publication.rawLog(),
                    publication.sessionId(),
                    publication.channel(),
                    publication.initialTermId(),
                    publication.startingTermId(),
                    publication.startingTermOffset());

                linkPublication();
            }
        }

        private void createPublication()
        {
            publication =
                newIpcPublication(correlationId, clientId, streamId, channel, isExclusive, params, rawLog);
            isNewPublication = true;

            rawLog = null; // ownership transferred to the IpcPublication

            linkPublication();
        }

        private void linkPublication()
        {
            publicationLinks.add(new PublicationLink(correlationId, getOrAddClient(clientId), publication));

            clientProxy.onPublicationReady(
                correlationId,
                publication.registrationId(),
                streamId,
                publication.sessionId(),
                publication.rawLog().fileName(),
                publication.publisherLimitId(),
                ChannelEndpointStatus.NO_ID_ALLOCATED,
                isExclusive);

            if (isNewPublication)
            {
                linkIpcSubscriptions(publication);
            }

            state = State.DONE;
        }

        private enum State
        {
            INIT,
            RESOLVE_PUBLICATION,
            AWAIT_LOG_BUFFER,
            CREATE_PUBLICATION,
            DONE
        }
    }

    private final class AddNetworkSubscriptionCommand extends ClientCommand
    {
        private final String channel;
        private final int streamId;
        private final long registrationId;
        private final long clientId;
        private final CommandResult<UdpChannel> parseChannelResult;
        private State state;
        private SubscriptionParams params;
        private ReceiveChannelEndpoint channelEndpoint;
        private UdpChannel udpChannel;

        private AddNetworkSubscriptionCommand(
            final String channel,
            final int streamId,
            final long registrationId,
            final long clientId)
        {
            super(registrationId);
            this.channel = channel;
            this.streamId = streamId;
            this.registrationId = registrationId;
            this.clientId = clientId;
            parseChannelResult = nativeResourceAgentProxy.parseChannel(channel, false);
            state = State.AWAITING_CHANNEL_PARSE;
        }

        boolean execute()
        {
            if (State.AWAITING_CHANNEL_PARSE == state)
            {
                awaitingChannelParse();
            }

            if (State.VALIDATING == state)
            {
                validating();
            }

            if (State.RESOLVING_ENDPOINT == state)
            {
                resolvingEndpoint();
            }

            if (State.CREATING_LINKS == state)
            {
                creatingLinks();
            }

            return State.DONE == state;
        }

        private void awaitingChannelParse()
        {
            if (null != (udpChannel = parseChannelResult.get()))
            {
                state = State.VALIDATING;
            }
        }

        private void validating()
        {
            validateExperimentalFeatures(ctx.enableExperimentalFeatures(), udpChannel);
            validateControlForSubscription(udpChannel);
            validateTimestampConfiguration(udpChannel);

            params = SubscriptionParams.getSubscriptionParams(udpChannel.channelUri(), ctx, 0);
            checkForClashingSubscription(params, udpChannel, streamId);

            state = State.RESOLVING_ENDPOINT;
        }

        private void resolvingEndpoint()
        {
            channelEndpoint = getOrCreateReceiveChannelEndpoint(params, udpChannel, registrationId);
            state = State.CREATING_LINKS;
        }

        private void creatingLinks()
        {
            final NetworkSubscriptionLink subscription = new NetworkSubscriptionLink(
                registrationId, channelEndpoint, streamId, channel, getOrAddClient(clientId), params);

            subscriptionLinks.add(subscription);

            final ControlMode controlMode = udpChannel.controlMode();
            if (ControlMode.RESPONSE == controlMode)
            {
                channelEndpoint.incResponseRefToStream(subscription.streamId);
            }
            else
            {
                addNetworkSubscriptionToReceiver(subscription);
            }

            clientProxy.onSubscriptionReady(registrationId, channelEndpoint.statusIndicatorCounter().id());
            linkMatchingImages(subscription);

            state = State.DONE;
        }

        private enum State
        {
            AWAITING_CHANNEL_PARSE,
            VALIDATING,
            RESOLVING_ENDPOINT,
            CREATING_LINKS,
            DONE
        }
    }

    private final class AddSpySubscriptionCommand extends ClientCommand
    {
        private final int streamId;
        private final long registrationId;
        private final long clientId;
        private final CommandResult<UdpChannel> parseChannelResult;

        private AddSpySubscriptionCommand(
            final String channel,
            final int streamId,
            final long registrationId,
            final long clientId)
        {
            super(registrationId);
            this.streamId = streamId;
            this.registrationId = registrationId;
            this.clientId = clientId;
            parseChannelResult = nativeResourceAgentProxy.parseChannel(channel, false);
        }

        boolean execute()
        {
            final UdpChannel udpChannel = parseChannelResult.get();
            if (null == udpChannel)
            {
                return false;
            }

            final SubscriptionParams params =
                SubscriptionParams.getSubscriptionParams(udpChannel.channelUri(), ctx, 0);
            final SpySubscriptionLink subscriptionLink = new SpySubscriptionLink(
                registrationId, udpChannel, streamId, getOrAddClient(clientId), params);

            subscriptionLinks.add(subscriptionLink);
            clientProxy.onSubscriptionReady(registrationId, ChannelEndpointStatus.NO_ID_ALLOCATED);

            for (int i = 0, size = networkPublications.size(); i < size; i++)
            {
                final NetworkPublication publication = networkPublications.get(i);
                if (subscriptionLink.matches(publication) && publication.isAcceptingSubscriptions())
                {
                    clientProxy.onAvailableImage(
                        publication.registrationId(),
                        streamId,
                        publication.sessionId(),
                        registrationId,
                        linkSpy(publication, subscriptionLink).id(),
                        publication.rawLog().fileName(),
                        IPC_CHANNEL);
                }
            }

            return true;
        }
    }

    private final class AddRcvSpyDestinationCommand extends ClientCommand
    {
        private final long registrationId;
        private final CommandResult<UdpChannel> parseChannelResult;

        private AddRcvSpyDestinationCommand(
            final long registrationId,
            final String destinationChannel,
            final long correlationId)
        {
            super(correlationId);
            this.registrationId = registrationId;
            parseChannelResult = nativeResourceAgentProxy.parseChannel(destinationChannel, false);
        }

        boolean execute()
        {
            final UdpChannel udpChannel = parseChannelResult.get();
            if (null == udpChannel)
            {
                return false;
            }

            final SubscriptionParams params =
                SubscriptionParams.getSubscriptionParams(udpChannel.channelUri(), ctx, 0);
            final SubscriptionLink mdsSubscriptionLink = findMdsSubscriptionLink(subscriptionLinks, registrationId);

            if (null == mdsSubscriptionLink)
            {
                throw new UnknownSubscriptionException("unknown MDS subscription: " + registrationId);
            }

            final SpySubscriptionLink subscriptionLink = new SpySubscriptionLink(
                registrationId,
                udpChannel,
                mdsSubscriptionLink.streamId(),
                mdsSubscriptionLink.aeronClient(),
                params);

            subscriptionLinks.add(subscriptionLink);
            clientProxy.operationSucceeded(correlationId);

            for (int i = 0, size = networkPublications.size(); i < size; i++)
            {
                final NetworkPublication publication = networkPublications.get(i);
                if (subscriptionLink.matches(publication) && publication.isAcceptingSubscriptions())
                {
                    clientProxy.onAvailableImage(
                        publication.registrationId(),
                        mdsSubscriptionLink.streamId(),
                        publication.sessionId(),
                        registrationId,
                        linkSpy(publication, subscriptionLink).id(),
                        publication.rawLog().fileName(),
                        IPC_CHANNEL);
                }
            }

            return true;
        }
    }

    private final class AddRcvNetworkDestinationCommand extends ClientCommand
    {
        private final long registrationId;
        private final String destinationChannel;
        private State state = State.INIT;
        private ReceiveChannelEndpoint receiveChannelEndpoint;
        private UdpChannel udpChannel;
        private CommandResult<UdpChannel> parseChannelResult;

        private AddRcvNetworkDestinationCommand(
            final long registrationId,
            final String destinationChannel,
            final long correlationId)
        {
            super(correlationId);
            this.registrationId = registrationId;
            this.destinationChannel = destinationChannel;
        }

        boolean execute()
        {
            if (State.INIT == state)
            {
                parseChannelResult = nativeResourceAgentProxy.parseChannel(destinationChannel, true);
                state = State.AWAITING_CHANNEL_PARSE;
            }
            if (State.AWAITING_CHANNEL_PARSE == state)
            {
                if (null != (udpChannel = parseChannelResult.get()))
                {
                    state = State.CREATE_TRANSPORT;
                }
            }
            if (State.CREATE_TRANSPORT == state)
            {
                createTransport();
            }

            return State.DONE == state;
        }

        private void createTransport()
        {
            validateDestinationUri(udpChannel.channelUri(), destinationChannel);

            final SubscriptionLink mdsSubscriptionLink = findMdsSubscriptionLink(subscriptionLinks, registrationId);

            if (null == mdsSubscriptionLink)
            {
                throw new UnknownSubscriptionException("unknown MDS subscription: " + registrationId);
            }

            receiveChannelEndpoint = mdsSubscriptionLink.channelEndpoint();

            AtomicCounter localSocketAddressIndicator = null;
            ReceiveDestinationTransport transport = null;

            try
            {
                localSocketAddressIndicator = ReceiveLocalSocketAddress.allocate(
                    tempBuffer,
                    countersManager,
                    registrationId,
                    receiveChannelEndpoint.statusIndicatorCounter().id());

                transport = new ReceiveDestinationTransport(
                    udpChannel, ctx, localSocketAddressIndicator, receiveChannelEndpoint);

                transport.openChannel(null);
                transport.indicateActive();
            }
            catch (final Exception ex)
            {
                CloseHelper.closeAll(localSocketAddressIndicator, transport);
                throw ex;
            }

            receiverProxy.addDestination(receiveChannelEndpoint, transport);
            clientProxy.operationSucceeded(correlationId);

            state = State.DONE;
        }

        private enum State
        {
            INIT,
            AWAITING_CHANNEL_PARSE,
            CREATE_TRANSPORT,
            DONE
        }
    }

    private final class RemoveRcvNetworkDestinationCommand extends ClientCommand
    {
        private final long registrationId;
        private final String destinationChannel;
        private State state = State.INIT;
        private UdpChannel udpChannel;
        private ReceiveChannelEndpoint receiveChannelEndpoint;
        private CommandResult<UdpChannel> parseChannelResult;

        private RemoveRcvNetworkDestinationCommand(
            final long registrationId,
            final String destinationChannel,
            final long correlationId)
        {
            super(correlationId);
            this.registrationId = registrationId;
            this.destinationChannel = destinationChannel;
        }

        boolean execute()
        {
            if (State.INIT == state)
            {
                init();
            }
            if (State.AWAITING_CHANNEL_PARSE == state)
            {
                awaitingChannelParse();
            }
            if (State.REMOVING == state)
            {
                removing();
            }

            return State.DONE == state;
        }

        private void init()
        {
            for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
            {
                final SubscriptionLink subscriptionLink = subscriptionLinks.get(i);
                if (registrationId == subscriptionLink.registrationId())
                {
                    receiveChannelEndpoint = subscriptionLink.channelEndpoint();
                    break;
                }
            }

            if (null == receiveChannelEndpoint)
            {
                throw new UnknownSubscriptionException("unknown subscription: " + registrationId);
            }

            receiveChannelEndpoint.validateAllowsDestinationControl();

            parseChannelResult = nativeResourceAgentProxy.parseChannel(destinationChannel, true);

            state = State.AWAITING_CHANNEL_PARSE;
        }

        private void awaitingChannelParse()
        {
            if (null != (udpChannel = parseChannelResult.get()))
            {
                state = State.REMOVING;
            }
        }

        private void removing()
        {
            receiverProxy.removeDestination(receiveChannelEndpoint, udpChannel);
            clientProxy.operationSucceeded(correlationId);

            state = State.DONE;
        }

        enum State
        {
            INIT,
            AWAITING_CHANNEL_PARSE,
            REMOVING,
            DONE
        }
    }

    private final class AddSendDestinationCommand extends ClientCommand
    {
        private final long registrationId;
        private final String destinationChannel;
        private State state = State.INIT;
        private ChannelUri channelUri;
        private CommandResult<InetSocketAddress> destinationAddressResult;

        private AddSendDestinationCommand(
            final long registrationId, final String destinationChannel, final long correlationId)
        {
            super(correlationId);
            this.registrationId = registrationId;
            this.destinationChannel = destinationChannel;
        }

        boolean execute()
        {
            if (State.INIT == state)
            {
                init();
            }

            if (State.AWAITING_DESTINATION_ADDRESS == state)
            {
                final InetSocketAddress address = destinationAddressResult.get();
                if (null != address)
                {
                    final SendChannelEndpoint sendChannelEndpoint =
                        findExistingManualSendChannelEndpoint(registrationId);
                    senderProxy.addDestination(sendChannelEndpoint, channelUri, address, correlationId);
                    clientProxy.operationSucceeded(correlationId);
                    state = State.DONE;
                }
            }

            return State.DONE == state;
        }

        private void init()
        {
            channelUri = parseUri(destinationChannel);
            validateDestinationUri(channelUri, destinationChannel);
            validateSendDestinationUri(channelUri, destinationChannel);

            findExistingManualSendChannelEndpoint(registrationId);

            destinationAddressResult = nativeResourceAgentProxy.destinationAddress(channelUri);
            state = State.AWAITING_DESTINATION_ADDRESS;
        }

        enum State
        {
            INIT,
            AWAITING_DESTINATION_ADDRESS,
            DONE
        }
    }

    private final class RemoveSendDestinationCommand extends ClientCommand
    {
        private final long registrationId;
        private final String destinationChannel;
        private State state = State.INIT;
        private ChannelUri channelUri;
        private CommandResult<InetSocketAddress> destinationAddressResult;

        private RemoveSendDestinationCommand(
            final long registrationId, final String destinationChannel, final long correlationId)
        {
            super(correlationId);
            this.registrationId = registrationId;
            this.destinationChannel = destinationChannel;
        }

        boolean execute()
        {
            if (State.INIT == state)
            {
                init();
            }

            if (State.AWAITING_DESTINATION_ADDRESS == state)
            {
                final InetSocketAddress address = destinationAddressResult.get();
                if (null != address)
                {
                    final SendChannelEndpoint sendChannelEndpoint =
                        findExistingManualSendChannelEndpoint(registrationId);
                    senderProxy.removeDestination(sendChannelEndpoint, channelUri, address);
                    clientProxy.operationSucceeded(correlationId);
                    state = State.DONE;
                }
            }

            return State.DONE == state;
        }

        private void init()
        {
            channelUri = parseUri(destinationChannel);
            findExistingManualSendChannelEndpoint(registrationId);

            destinationAddressResult = nativeResourceAgentProxy.destinationAddress(channelUri);
            state = State.AWAITING_DESTINATION_ADDRESS;
        }

        enum State
        {
            INIT,
            AWAITING_DESTINATION_ADDRESS,
            DONE
        }
    }

    private final class CreatePublicationImageCommand extends Command
    {
        private final int sessionId;
        private final int streamId;
        private final int initialTermId;
        private final int activeTermId;
        private final int termOffset;
        private final int termBufferLength;
        private final int senderMtuLength;
        private final int transportIndex;
        private final short flags;
        private final InetSocketAddress controlAddress;
        private final InetSocketAddress sourceAddress;
        private final ReceiveChannelEndpoint channelEndpoint;
        private final SubscriptionParams subscriptionParams;
        private CommandResult<RawLog> imageLogResult;
        private long registrationId;
        private State state;

        CreatePublicationImageCommand(
            final int sessionId,
            final int streamId,
            final int initialTermId,
            final int activeTermId,
            final int termOffset,
            final int termBufferLength,
            final int senderMtuLength,
            final int transportIndex,
            final short flags,
            final InetSocketAddress controlAddress,
            final InetSocketAddress sourceAddress,
            final ReceiveChannelEndpoint channelEndpoint)
        {
            this.sessionId = sessionId;
            this.streamId = streamId;
            this.initialTermId = initialTermId;
            this.activeTermId = activeTermId;
            this.termOffset = termOffset;
            this.termBufferLength = termBufferLength;
            this.senderMtuLength = senderMtuLength;
            this.transportIndex = transportIndex;
            this.flags = flags;
            this.controlAddress = controlAddress;
            this.sourceAddress = sourceAddress;
            this.channelEndpoint = channelEndpoint;

            Configuration.validateMtuLength(senderMtuLength);

            subscriptionParams = SubscriptionParams.getSubscriptionParams(
                channelEndpoint.subscriptionUdpChannel().channelUri(), ctx, termBufferLength);

            Configuration.validateInitialWindowLength(subscriptionParams.receiverWindowLength, senderMtuLength);

            final StreamInterest streamInterest = findSubscribers(channelEndpoint, sessionId, streamId, flags);

            if (null == streamInterest)
            {
                state = State.DONE;
            }
            else
            {
                registrationId = toDriverCommands.nextCorrelationId();

                imageLogResult = nativeResourceAgentProxy.newPublicationImageLog(
                    sessionId,
                    streamId,
                    initialTermId,
                    termOffset,
                    termBufferLength,
                    senderMtuLength,
                    channelEndpoint,
                    registrationId,
                    subscriptionParams,
                    streamInterest.sparse,
                    streamInterest.reliable,
                    streamInterest.multicastSemantics);

                state = State.CREATE_LOG_BUFFER;
            }
        }

        boolean execute()
        {
            if (State.CREATE_LOG_BUFFER == state)
            {
                final RawLog rawLog = imageLogResult.get();
                if (null != rawLog)
                {
                    newPublicationImage(
                        sessionId,
                        streamId,
                        initialTermId,
                        activeTermId,
                        termOffset,
                        termBufferLength,
                        senderMtuLength,
                        transportIndex,
                        flags,
                        controlAddress,
                        sourceAddress,
                        channelEndpoint,
                        registrationId,
                        subscriptionParams,
                        rawLog);

                    state = State.DONE;
                }
            }

            return State.DONE == state;
        }

        private enum State
        {
            CREATE_LOG_BUFFER,
            DONE
        }
    }

    private final class ReResolveControlAddress extends Command
    {
        private final String control;
        private final UdpChannel udpChannel;
        private final ReceiveChannelEndpoint channelEndpoint;
        private final InetSocketAddress address;
        private final CommandResult<InetSocketAddress> resolvedAddressResult;

        ReResolveControlAddress(
            final String control,
            final UdpChannel udpChannel,
            final ReceiveChannelEndpoint channelEndpoint,
            final InetSocketAddress address)
        {
            resolvedAddressResult = nativeResourceAgentProxy.reResolveAddress(control, MDC_CONTROL_PARAM_NAME);
            this.control = control;
            this.udpChannel = udpChannel;
            this.channelEndpoint = channelEndpoint;
            this.address = address;
        }

        boolean execute()
        {
            final InetSocketAddress newAddress = resolvedAddressResult.get();
            if (null != newAddress)
            {
                if (newAddress.isUnresolved())
                {
                    throw new AeronEvent("could not re-resolve: " + MDC_CONTROL_PARAM_NAME + "=" + control);
                }

                if (ChannelEndpointStatus.ACTIVE == channelEndpoint.status())
                {
                    if (!Objects.equals(address, newAddress))
                    {
                        receiverProxy.onResolutionChange(channelEndpoint, udpChannel, newAddress);
                    }
                }
                return true;
            }
            return false;
        }
    }

    private final class ReResolveEndpointAddress extends Command
    {
        private final String endpoint;
        private final SendChannelEndpoint channelEndpoint;
        private final InetSocketAddress address;
        private final CommandResult<InetSocketAddress> resolvedAddressResult;

        ReResolveEndpointAddress(
            final String endpoint, final SendChannelEndpoint channelEndpoint, final InetSocketAddress address)
        {
            this.endpoint = endpoint;
            this.channelEndpoint = channelEndpoint;
            this.address = address;
            resolvedAddressResult = nativeResourceAgentProxy.reResolveAddress(endpoint, ENDPOINT_PARAM_NAME);
        }

        boolean execute()
        {
            final InetSocketAddress newAddress = resolvedAddressResult.get();
            if (null != newAddress)
            {
                if (newAddress.isUnresolved())
                {
                    throw new AeronEvent("could not re-resolve: " + ENDPOINT_PARAM_NAME + "=" + endpoint);
                }

                if (ChannelEndpointStatus.ACTIVE == channelEndpoint.status())
                {
                    if (!Objects.equals(address, newAddress))
                    {
                        senderProxy.onResolutionChange(channelEndpoint, endpoint, newAddress);
                    }
                }
                return true;
            }
            return false;
        }
    }
}
