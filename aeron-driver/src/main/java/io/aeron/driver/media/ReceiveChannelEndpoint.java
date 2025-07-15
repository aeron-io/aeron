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
package io.aeron.driver.media;

import io.aeron.CommonContext;
import io.aeron.ErrorCode;
import io.aeron.driver.DataPacketDispatcher;
import io.aeron.driver.DriverConductorProxy;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.ControlProtocolException;
import io.aeron.protocol.*;
import io.aeron.status.ChannelEndpointStatus;
import io.aeron.status.LocalSocketAddressStatus;
import org.agrona.BitUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Hashing;
import org.agrona.collections.Int2IntCounterMap;
import org.agrona.collections.Long2LongCounterMap;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static io.aeron.driver.status.SystemCounterDescriptor.POSSIBLE_TTL_ASYMMETRY;
import static io.aeron.driver.status.SystemCounterDescriptor.SHORT_SENDS;
import static io.aeron.protocol.StatusMessageFlyweight.SEND_SETUP_FLAG;
import static io.aeron.status.ChannelEndpointStatus.status;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

abstract class ReceiveChannelEndpointLhsPadding extends UdpChannelTransport
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;

    ReceiveChannelEndpointLhsPadding(
        final UdpChannel udpChannel,
        final InetSocketAddress endPointAddress,
        final InetSocketAddress bindAddress,
        final InetSocketAddress connectAddress,
        final MediaDriver.Context context)
    {
        super(udpChannel, endPointAddress, bindAddress, connectAddress, context.receiverPortManager(), context);
    }
}

abstract class ReceiveChannelEndpointHotFields extends ReceiveChannelEndpointLhsPadding
{
    /**
     * Counter for the number of errors frames send back by this channel endpoint.
     */
    protected final AtomicCounter errorFramesSent;
    long timeOfLastActivityNs;

    ReceiveChannelEndpointHotFields(
        final UdpChannel udpChannel,
        final InetSocketAddress endPointAddress,
        final InetSocketAddress bindAddress,
        final InetSocketAddress connectAddress,
        final MediaDriver.Context context)
    {
        super(udpChannel, endPointAddress, bindAddress, connectAddress, context);
        errorFramesSent = context.systemCounters().get(SystemCounterDescriptor.ERROR_FRAMES_SENT);
    }
}

abstract class ReceiveChannelEndpointRhsPadding extends ReceiveChannelEndpointHotFields
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;

    ReceiveChannelEndpointRhsPadding(
        final UdpChannel udpChannel,
        final InetSocketAddress endPointAddress,
        final InetSocketAddress bindAddress,
        final InetSocketAddress connectAddress,
        final MediaDriver.Context context)
    {
        super(udpChannel, endPointAddress, bindAddress, connectAddress, context);
    }
}

/**
 * Aggregator of multiple subscriptions onto a single transport channel for the receiving of data and setup frames
 * plus sending status and NAK frames.
 */
public class ReceiveChannelEndpoint extends ReceiveChannelEndpointRhsPadding
{
    static final long DESTINATION_ADDRESS_TIMEOUT = TimeUnit.SECONDS.toNanos(5);

    private final DataPacketDispatcher dispatcher;
    private final ByteBuffer smBuffer;
    private final StatusMessageFlyweight statusMessageFlyweight;
    private final ByteBuffer nakBuffer;
    private final NakFlyweight nakFlyweight;
    private final ByteBuffer rttMeasurementBuffer;
    private final RttMeasurementFlyweight rttMeasurementFlyweight;
    private final ByteBuffer responseSetupBuffer;
    private final ResponseSetupFlyweight responseSetupHeader;
    private final ByteBuffer errorBuffer;
    private final ErrorFlyweight errorFlyweight;
    private final AtomicCounter shortSends;
    private final AtomicCounter possibleTtlAsymmetry;
    private final AtomicCounter statusIndicator;
    private final Int2IntCounterMap refCountByStreamIdMap = new Int2IntCounterMap(0);
    private final Long2LongCounterMap refCountByStreamIdAndSessionIdMap = new Long2LongCounterMap(0);
    private final Int2IntCounterMap responseRefCountByStreamIdMap = new Int2IntCounterMap(0);
    private final MultiRcvDestination multiRcvDestination;
    private final CachedNanoClock cachedNanoClock;
    private final Long groupTag;
    private final boolean isChannelReceiveTimestampEnabled;
    private final EpochNanoClock channelReceiveTimestampClock;

    private final long receiverId;
    private InetSocketAddress currentControlAddress;
    private AtomicCounter localSocketAddressIndicator;
    private int imageRefCount;

    /**
     * Construct the receiver end for data streams.
     *
     * @param udpChannel      configuration for the media.
     * @param dispatcher      for forwarding packets.
     * @param statusIndicator to indicate the status of the channel endpoint.
     * @param context         for configuration.
     */
    public ReceiveChannelEndpoint(
        final UdpChannel udpChannel,
        final DataPacketDispatcher dispatcher,
        final AtomicCounter statusIndicator,
        final MediaDriver.Context context)
    {
        super(udpChannel, udpChannel.remoteData(), udpChannel.remoteData(), null, context);

        this.dispatcher = dispatcher;
        this.statusIndicator = statusIndicator;

        shortSends = context.systemCounters().get(SHORT_SENDS);
        possibleTtlAsymmetry = context.systemCounters().get(POSSIBLE_TTL_ASYMMETRY);

        final ReceiveChannelEndpointThreadLocals threadLocals = context.receiveChannelEndpointThreadLocals();
        smBuffer = threadLocals.statusMessageBuffer();
        statusMessageFlyweight = threadLocals.statusMessageFlyweight();
        nakBuffer = threadLocals.nakBuffer();
        nakFlyweight = threadLocals.nakFlyweight();
        rttMeasurementBuffer = threadLocals.rttMeasurementBuffer();
        rttMeasurementFlyweight = threadLocals.rttMeasurementFlyweight();
        responseSetupBuffer = threadLocals.responseSetupBuffer();
        responseSetupHeader = threadLocals.responseSetupHeader();
        errorBuffer = threadLocals.errorBuffer();
        errorFlyweight = threadLocals.errorFlyweight();
        cachedNanoClock = context.receiverCachedNanoClock();
        timeOfLastActivityNs = cachedNanoClock.nanoTime();
        receiverId = threadLocals.nextReceiverId();

        this.groupTag = (null == udpChannel.groupTag()) ? context.receiverGroupTag() : udpChannel.groupTag();

        multiRcvDestination = udpChannel.isManualControlMode() ? new MultiRcvDestination() : null;
        currentControlAddress = udpChannel.localControl();

        channelReceiveTimestampClock = context.channelReceiveTimestampClock();
        isChannelReceiveTimestampEnabled = udpChannel.isChannelReceiveTimestampEnabled();
    }

    /**
     * Set a channel binding status counter, if required (not used by control-mode=manual).
     *
     * @param counter to be set.
     */
    public void localSocketAddressIndicator(final AtomicCounter counter)
    {
        if (null != multiRcvDestination)
        {
            throw new IllegalStateException("local socket address indicator not used for MDS");
        }

        localSocketAddressIndicator = counter;
    }

    /**
     * Send contents of {@link java.nio.ByteBuffer} to the remote address.
     *
     * @param buffer        to send containing the payload.
     * @param remoteAddress to send the payload to.
     * @return number of bytes sent.
     */
    public int sendTo(final ByteBuffer buffer, final InetSocketAddress remoteAddress)
    {
        int bytesSent = 0;
        try
        {
            if (null != sendDatagramChannel)
            {
                if (sendDatagramChannel.isOpen())
                {
                    sendHook(buffer, remoteAddress);
                    bytesSent = sendDatagramChannel.send(buffer, remoteAddress);
                }
            }
        }
        catch (final PortUnreachableException ignore)
        {
        }
        catch (final IOException ex)
        {
            onSendError(ex, remoteAddress, errorHandler);
        }

        return bytesSent;
    }

    /**
     * The original URI String used when a subscription was added.
     *
     * @return the original URI String used when a subscription was added.
     */
    public String originalUriString()
    {
        return subscriptionUdpChannel().originalUriString();
    }

    /**
     * Counter which indicates the status of the channel.
     *
     * @return counter which indicates the status of the channel.
     */
    public AtomicCounter statusIndicatorCounter()
    {
        return statusIndicator;
    }

    /**
     * Indicate that the channel as active after successfully opening it.
     */
    public void indicateActive()
    {
        final long currentStatus = statusIndicator.get();
        if (currentStatus != ChannelEndpointStatus.INITIALIZING)
        {
            throw new AeronException(
                "channel cannot be registered unless INITIALIZING: status=" + status(currentStatus));
        }

        if (null == multiRcvDestination)
        {
            final String bindAddressAndPort = bindAddressAndPort();
            statusIndicator.appendToLabel(bindAddressAndPort);
            updateLocalSocketAddress(bindAddressAndPort);
        }

        statusIndicator.setRelease(ChannelEndpointStatus.ACTIVE);
    }

    /**
     * Close the counters used to indicate channel status.
     */
    public void closeIndicators()
    {
        statusIndicator.close();

        if (null != localSocketAddressIndicator)
        {
            localSocketAddressIndicator.close();
        }
    }

    /**
     * Close transports for {@link MultiRcvDestination} if present.
     *
     * @param poller associated with the {@link MultiRcvDestination} if present to be used for selecting without
     *              processing.
     */
    public void closeMultiRcvDestinationTransports(final DataTransportPoller poller)
    {
        if (null != multiRcvDestination)
        {
            multiRcvDestination.closeTransports(this, poller);
        }
    }

    /**
     * Close the {@link MultiRcvDestination} indicators.
     *
     * @param conductorProxy for sending back counters to be closed.
     */
    public void closeMultiRcvDestinationIndicators(final DriverConductorProxy conductorProxy)
    {
        if (null != multiRcvDestination)
        {
            multiRcvDestination.closeIndicators(conductorProxy);
        }
    }

    /**
     * Open the underlying sockets for the channel.
     *
     * @param conductorProxy for notifying potential channel errors.
     */
    public void openChannel(final DriverConductorProxy conductorProxy)
    {
        if (null == multiRcvDestination)
        {
            openDatagramChannel(statusIndicator);
        }
    }

    /**
     * Increment the {@link io.aeron.driver.status.SystemCounterDescriptor#POSSIBLE_TTL_ASYMMETRY} counter.
     */
    public void possibleTtlAsymmetryEncountered()
    {
        possibleTtlAsymmetry.incrementRelease();
    }

    /**
     * Called from the {@link io.aeron.driver.DriverConductor} to
     * increment the reference count for a given stream id.
     *
     * @param streamId to increment the reference for.
     * @return current reference count after the increment.
     */
    public int incRefToStream(final int streamId)
    {
        return refCountByStreamIdMap.incrementAndGet(streamId);
    }

    /**
     * Called from the {@link io.aeron.driver.DriverConductor} to
     * decrement the reference count for a given stream id.
     *
     * @param streamId to decrement the reference for.
     * @return current reference count after the decrement.
     */
    public int decRefToStream(final int streamId)
    {
        final int count = refCountByStreamIdMap.decrementAndGet(streamId);

        if (-1 == count)
        {
            refCountByStreamIdMap.remove(streamId);
            throw new IllegalStateException("unknown stream Id: " + streamId);
        }

        return count;
    }

    /**
     * Called from the {@link io.aeron.driver.DriverConductor} to
     * increment the reference count for a given stream id and session id.
     *
     * @param streamId  to increment the reference for.
     * @param sessionId to increment the reference for.
     * @return current reference count after the increment.
     */
    public long incRefToStreamAndSession(final int streamId, final int sessionId)
    {
        return refCountByStreamIdAndSessionIdMap.incrementAndGet(Hashing.compoundKey(streamId, sessionId));
    }

    /**
     * Called from the {@link io.aeron.driver.DriverConductor} to
     * decrement the reference count for a given stream id and session id.
     *
     * @param streamId  to increment the reference for.
     * @param sessionId to increment the reference for.
     * @return current reference count after the decrement.
     */
    public long decRefToStreamAndSession(final int streamId, final int sessionId)
    {
        final long key = Hashing.compoundKey(streamId, sessionId);
        final long count = refCountByStreamIdAndSessionIdMap.decrementAndGet(key);

        if (-1 == count)
        {
            refCountByStreamIdAndSessionIdMap.remove(key);
            throw new IllegalStateException("unknown stream Id + session Id: " + streamId + " " + sessionId);
        }

        return count;
    }

    /**
     * Called from the {@link io.aeron.driver.DriverConductor} to
     * increment the reference count for a given stream id for a response subscription while it is waiting to be
     * connected.
     *
     * @param streamId to increment the reference for.
     * @return current reference count after the increment.
     */
    public int incResponseRefToStream(final int streamId)
    {
        return responseRefCountByStreamIdMap.incrementAndGet(streamId);
    }


    /**
     * Called from the {@link io.aeron.driver.DriverConductor} to
     * decrement the reference count for a given stream id for a response subscription while it is waiting to be
     * connected.
     *
     * @param streamId to decrement the reference for.
     * @return current reference count after the decrement.
     */
    public int decResponseRefToStream(final int streamId)
    {
        final int count = responseRefCountByStreamIdMap.decrementAndGet(streamId);

        if (-1 == count)
        {
            responseRefCountByStreamIdMap.remove(streamId);
            throw new IllegalStateException("unknown stream Id: " + streamId);
        }

        return count;
    }

    /**
     * Total count of distinct subscriptions to streams.
     *
     * @return total count of distinct subscriptions to streams.
     */
    public int distinctSubscriptionCount()
    {
        return refCountByStreamIdMap.size() + refCountByStreamIdAndSessionIdMap.size();
    }

    /**
     * Called from the {@link io.aeron.driver.DriverConductor} to
     * determine if the channel should be closed for cleanup.
     *
     * @return true if the channel should be closed for cleanup.
     */
    public boolean shouldBeClosed()
    {
        return refCountByStreamIdMap.isEmpty() &&
            refCountByStreamIdAndSessionIdMap.isEmpty() &&
            responseRefCountByStreamIdMap.isEmpty() &&
            !statusIndicator.isClosed() &&
            imageRefCount <= 0;
    }

    /**
     * Does the channel have an explicit control address as used with multi-destination-cast or not?
     *
     * @return does channel have an explicit control address or not?
     */
    public boolean hasExplicitControl()
    {
        return udpChannel.hasExplicitControl();
    }

    /**
     * Does the channel have an explicit control address as used with multi-destination-cast or not?
     *
     * @return does channel have an explicit control address or not?
     */
    public InetSocketAddress explicitControlAddress()
    {
        return udpChannel.hasExplicitControl() ? currentControlAddress : null;
    }

    /**
     * Has the channel got control of destinations for MDS.
     *
     * @return true if the channel got control of destinations for MDS.
     */
    public boolean hasDestinationControl()
    {
        return null != multiRcvDestination;
    }

    /**
     * Validate that the channel allows destination control.
     * <p>
     * If not then a {@link ControlProtocolException} will be thrown.
     */
    public void validateAllowsDestinationControl()
    {
        if (null == multiRcvDestination)
        {
            throw new ControlProtocolException(ErrorCode.INVALID_CHANNEL, "channel does not allow manual control");
        }
    }

    /**
     * Is the primary transport multicast?
     *
     * @return true if the primary transport is multicast.
     */
    public boolean isMulticast()
    {
        return isMulticast(0);
    }

    /**
     * Is a given transport index multicast?
     *
     * @param transportIndex to check for multicast.
     * @return true if the transport index is multicast.
     */
    public boolean isMulticast(final int transportIndex)
    {
        if (null != multiRcvDestination)
        {
            return multiRcvDestination.transport(transportIndex).isMulticast();
        }
        else if (0 == transportIndex)
        {
            return super.isMulticast();
        }

        throw new IllegalStateException("isMulticast for unknown index " + transportIndex);
    }

    /**
     * Return the {@link UdpChannel} for the Subscription channel.
     *
     * @return {@link UdpChannel} for the Subscription channel.
     */
    public UdpChannel subscriptionUdpChannel()
    {
        return super.udpChannel;
    }

    /**
     * Get the {@link UdpChannel} for the primary transport.
     *
     * @return the {@link UdpChannel} for the primary transport.
     */
    public UdpChannel udpChannel()
    {
        return udpChannel(0);
    }

    /**
     * Get the {@link UdpChannel} for the transport index.
     *
     * @param transportIndex to the {@link UdpChannel}.
     * @return the {@link UdpChannel} for the transport index.
     */
    public UdpChannel udpChannel(final int transportIndex)
    {
        if (null != multiRcvDestination && multiRcvDestination.hasDestination(transportIndex))
        {
            return multiRcvDestination.transport(transportIndex).udpChannel();
        }
        else if (0 == transportIndex)
        {
            return super.udpChannel();
        }

        throw new IllegalStateException("udpChannel for unknown index " + transportIndex);
    }

    /**
     * Has the channel been tagged?
     *
     * @return true if the channel has a tag identity.
     */
    public boolean hasTag()
    {
        return super.udpChannel.hasTag();
    }

    /**
     * The tag identity for the channel.
     *
     * @return the tag identity for the channel.
     */
    public long tag()
    {
        return super.udpChannel.tag();
    }

    /**
     * Does the channel have a matching tag?
     *
     * @param udpChannel with tag to match against.
     * @return true if the channel matches on tag identity.
     */
    public boolean matchesTag(final UdpChannel udpChannel)
    {
        return udpChannel.matchesTag(super.udpChannel, currentControlAddress, null);
    }

    /**
     * Get the multicast TTL for the primary transport.
     *
     * @return the multicast TTL for the primary transport.
     */
    public int multicastTtl()
    {
        return multicastTtl(0);
    }

    /**
     * Get the multicast TTL for the transport index.
     *
     * @param transportIndex to get the multicast TTL for.
     * @return the multicast TTL for the transport index.
     */
    public int multicastTtl(final int transportIndex)
    {
        if (null != multiRcvDestination)
        {
            return multiRcvDestination.transport(transportIndex).multicastTtl();
        }
        else if (0 == transportIndex)
        {
            return super.multicastTtl();
        }

        throw new IllegalStateException("multicastTtl for unknown index " + transportIndex);
    }

    /**
     * Add a destination to the channel to receive on.
     *
     * @param transport to add for the destination.
     * @return index for the transport.
     */
    public int addDestination(final ReceiveDestinationTransport transport)
    {
        return multiRcvDestination.addDestination(transport);
    }

    /**
     * Remove transport by index from the channel.
     *
     * @param transportIndex to be removed.
     */
    public void removeDestination(final int transportIndex)
    {
        multiRcvDestination.removeDestination(transportIndex);
    }

    /**
     * Get the transport index for a given channel.
     *
     * @param udpChannel to look up the transport index for.
     * @return the transport index if found otherwise {@link ArrayUtil#UNKNOWN_INDEX}.
     */
    public int destination(final UdpChannel udpChannel)
    {
        return multiRcvDestination.transport(udpChannel);
    }

    /**
     * Get the transport destination for a given index.
     *
     * @param transportIndex to get.
     * @return the transport destination for a given index.
     */
    public ReceiveDestinationTransport destination(final int transportIndex)
    {
        return multiRcvDestination.transport(transportIndex);
    }

    /**
     * Does the channel have a destination for a given index value.
     *
     * @param transportIndex to check if a destination exists for.
     * @return true if the channel has a given transport index.
     */
    public boolean hasDestination(final int transportIndex)
    {
        return null == multiRcvDestination ? (0 == transportIndex) : multiRcvDestination.hasDestination(transportIndex);
    }

    /**
     * Callback to handle a received data packet.
     *
     * @param header         of the data first frame.
     * @param buffer         containing the data packet.
     * @param length         of the data packet.
     * @param srcAddress     from which the data packet was received.
     * @param transportIndex on which the packet was received.
     * @return number of bytes applied as a result of this action.
     */
    public int onDataPacket(
        final DataHeaderFlyweight header,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress,
        final int transportIndex)
    {
        if (isChannelReceiveTimestampEnabled && 0 != (header.flags() & DataHeaderFlyweight.BEGIN_FLAG))
        {
            applyChannelReceiveTimestamp(buffer, length);
        }
        updateTimeOfLastActivityNs(cachedNanoClock.nanoTime(), transportIndex);

        return dispatcher.onDataPacket(this, header, buffer, length, srcAddress, transportIndex);
    }

    /**
     * Callback to handle a received setup frame.
     *
     * @param header         of the setup frame.
     * @param buffer         containing the setup frame.
     * @param length         of the setup frame.
     * @param srcAddress     the message came from.
     * @param transportIndex on which the message was received.
     */
    public void onSetupMessage(
        final SetupFlyweight header,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress,
        final int transportIndex)
    {
        updateTimeOfLastActivityNs(cachedNanoClock.nanoTime(), transportIndex);
        dispatcher.onSetupMessage(this, header, srcAddress, transportIndex);
    }

    /**
     * Callback to handle a received RTT Measurement frame.
     *
     * @param header         of the RTT Measurement frame.
     * @param buffer         containing the RTT Measurement frame.
     * @param length         of the RTT Measurement frame.
     * @param srcAddress     the message came from.
     * @param transportIndex on which the message was received.
     */
    public void onRttMeasurement(
        final RttMeasurementFlyweight header,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress,
        final int transportIndex)
    {
        final long requestedReceiverId = header.receiverId();
        if (requestedReceiverId == receiverId || requestedReceiverId == 0)
        {
            updateTimeOfLastActivityNs(cachedNanoClock.nanoTime(), transportIndex);
            dispatcher.onRttMeasurement(this, header, srcAddress, transportIndex);
        }
    }

    /**
     * Send a Setup Eliciting Status Message to a source.
     *
     * @param transportIndex for the source.
     * @param controlAddress for the source.
     * @param sessionId      for the image.
     * @param streamId       for the image.
     */
    public void sendSetupElicitingStatusMessage(
        final int transportIndex, final InetSocketAddress controlAddress, final int sessionId, final int streamId)
    {
        smBuffer.clear();
        statusMessageFlyweight
            .sessionId(sessionId)
            .streamId(streamId)
            .consumptionTermId(0)
            .consumptionTermOffset(0)
            .receiverWindowLength(0)
            .receiverId(receiverId)
            .groupTag(groupTag)
            .flags(SEND_SETUP_FLAG);
        smBuffer.limit(statusMessageFlyweight.frameLength());

        send(smBuffer, statusMessageFlyweight.frameLength(), transportIndex, controlAddress);
    }

    /**
     * Send RTT Measurement frame to a source.
     *
     * @param transportIndex  for the source.
     * @param controlAddress  for the source.
     * @param sessionId       for the image.
     * @param streamId        for the image.
     * @param echoTimestampNs timestamp to echo in a reply.
     * @param receptionDelta  time in nanoseconds between receiving original request and sending Reply RTT Measurement.
     * @param isReply         true if a reply.
     */
    public void sendRttMeasurement(
        final int transportIndex,
        final InetSocketAddress controlAddress,
        final int sessionId,
        final int streamId,
        final long echoTimestampNs,
        final long receptionDelta,
        final boolean isReply)
    {
        rttMeasurementBuffer.clear();
        rttMeasurementFlyweight
            .sessionId(sessionId)
            .streamId(streamId)
            .receiverId(receiverId)
            .echoTimestampNs(echoTimestampNs)
            .receptionDelta(receptionDelta)
            .flags(isReply ? RttMeasurementFlyweight.REPLY_FLAG : 0);

        send(rttMeasurementBuffer, RttMeasurementFlyweight.HEADER_LENGTH, transportIndex, controlAddress);
    }

    /**
     * Send a Status Message back to a sources.
     *
     * @param controlAddresses  of the sources.
     * @param sessionId         of the image.
     * @param streamId          of the image.
     * @param termId            of the image to indicate position.
     * @param termOffset        of the image to indicate position.
     * @param windowLength      for available buffer from the position.
     * @param flags             for the header.
     */
    public void sendStatusMessage(
        final ImageConnection[] controlAddresses,
        final int sessionId,
        final int streamId,
        final int termId,
        final int termOffset,
        final int windowLength,
        final short flags)
    {
        smBuffer.clear();
        statusMessageFlyweight
            .sessionId(sessionId)
            .streamId(streamId)
            .consumptionTermId(termId)
            .consumptionTermOffset(termOffset)
            .receiverWindowLength(windowLength)
            .receiverId(receiverId)
            .groupTag(groupTag)
            .flags(flags);
        smBuffer.limit(statusMessageFlyweight.frameLength());

        send(smBuffer, statusMessageFlyweight.frameLength(), controlAddresses);
    }

    /**
     * Send a NAK message back to the sources.
     *
     * @param controlAddresses of the sources.
     * @param sessionId        of the image.
     * @param streamId         of the image.
     * @param termId           of the image to indicate position.
     * @param termOffset       of the image to indicate position.
     * @param length           of the range to be re-transmitted.
     */
    public void sendNakMessage(
        final ImageConnection[] controlAddresses,
        final int sessionId,
        final int streamId,
        final int termId,
        final int termOffset,
        final int length)
    {
        nakBuffer.clear();
        nakFlyweight
            .streamId(streamId)
            .sessionId(sessionId)
            .termId(termId)
            .termOffset(termOffset)
            .length(length);

        send(nakBuffer, NakFlyweight.HEADER_LENGTH, controlAddresses);
    }

    /**
     * Send RTT Measurement frame to the sources.
     *
     * @param controlAddresses of the sources.
     * @param sessionId        for the image.
     * @param streamId         for the image.
     * @param echoTimestampNs  timestamp to echo in a reply.
     * @param receptionDelta   time in nanoseconds between receiving original request and sending Reply RTT Measurement.
     * @param isReply          true if a reply.
     */
    public void sendRttMeasurement(
        final ImageConnection[] controlAddresses,
        final int sessionId,
        final int streamId,
        final long echoTimestampNs,
        final long receptionDelta,
        final boolean isReply)
    {
        rttMeasurementBuffer.clear();
        rttMeasurementFlyweight
            .sessionId(sessionId)
            .streamId(streamId)
            .receiverId(receiverId)
            .echoTimestampNs(echoTimestampNs)
            .receptionDelta(receptionDelta)
            .flags(isReply ? RttMeasurementFlyweight.REPLY_FLAG : 0);

        send(rttMeasurementBuffer, RttMeasurementFlyweight.HEADER_LENGTH, controlAddresses);
    }

    /**
     * Send a response setup message.
     *
     * @param controlAddresses  of the sources.
     * @param sessionId         for the image.
     * @param streamId          for the image.
     * @param responseSessionId to be used by the remote subscription to listen for responses.
     */
    public void sendResponseSetup(
        final ImageConnection[] controlAddresses,
        final int sessionId,
        final int streamId,
        final int responseSessionId)
    {
        responseSetupBuffer.clear();
        responseSetupHeader
            .sessionId(sessionId)
            .streamId(streamId)
            .responseSessionId(responseSessionId);

        send(responseSetupBuffer, ResponseSetupFlyweight.HEADER_LENGTH, controlAddresses);
    }

    /**
     * Send an error frame back to the source publications to indicate this image has errored.
     *
     * @param controlAddresses  of the sources.
     * @param sessionId         for the image.
     * @param streamId          for the image.
     * @param errorCode         for the error being sent.
     * @param errorMessage      to be sent back to the publication.
     */
    public void sendErrorFrame(
        final ImageConnection[] controlAddresses,
        final int sessionId,
        final int streamId,
        final int errorCode,
        final String errorMessage)
    {
        errorFramesSent.increment();

        errorBuffer.clear();
        errorFlyweight
            .sessionId(sessionId)
            .streamId(streamId)
            .receiverId(receiverId)
            .groupTag(groupTag)
            .errorCode(errorCode)
            .errorMessage(errorMessage);
        errorBuffer.limit(errorFlyweight.frameLength());

        send(errorBuffer, errorFlyweight.frameLength(), controlAddresses);
    }

    /**
     * Dispatcher for the channel.
     *
     * @return dispatcher for the channel.
     */
    public DataPacketDispatcher dispatcher()
    {
        return dispatcher;
    }

    /**
     * Update the control address for a channel transport when re-resolution occurs.
     *
     * @param transportIndex to update the control address for.
     * @param newAddress     for the control of the transport.
     */
    public void updateControlAddress(final int transportIndex, final InetSocketAddress newAddress)
    {
        if (null != multiRcvDestination)
        {
            multiRcvDestination.updateControlAddress(transportIndex, newAddress);
        }
        else if (udpChannel.hasExplicitControl())
        {
            currentControlAddress = newAddress;
        }
    }

    /**
     * Send a frame to the image connections.
     *
     * @param buffer           containing the frame.
     * @param length           of the frame in the buffer.
     * @param imageConnections to send the frame to.
     */
    protected void send(final ByteBuffer buffer, final int length, final ImageConnection[] imageConnections)
    {
        final int bytesSent = null == multiRcvDestination ?
            sendTo(buffer, imageConnections[0].controlAddress) :
            multiRcvDestination.sendToAll(imageConnections, buffer, length, cachedNanoClock.nanoTime());

        if (length != bytesSent)
        {
            shortSends.increment();
        }
    }

    /**
     * Send a frame to a source.
     *
     * @param buffer         containing the frame.
     * @param length         of the frame in the buffer.
     * @param transportIndex to send the frame on.
     * @param remoteAddress  to send the frame to.
     */
    protected void send(
        final ByteBuffer buffer, final int length, final int transportIndex, final InetSocketAddress remoteAddress)
    {
        final int bytesSent = null == multiRcvDestination ?
            sendTo(buffer, remoteAddress) :
            MultiRcvDestination.sendTo(multiRcvDestination.transport(transportIndex), buffer, remoteAddress);

        if (length != bytesSent)
        {
            shortSends.increment();
        }
    }

    void checkForReResolution(final long nowNs, final DriverConductorProxy conductorProxy)
    {
        if (null != multiRcvDestination)
        {
            multiRcvDestination.checkForReResolution(this, nowNs, conductorProxy);
        }
        else if (udpChannel.hasExplicitControl() && (timeOfLastActivityNs + DESTINATION_ADDRESS_TIMEOUT) - nowNs < 0)
        {
            timeOfLastActivityNs = nowNs;
            conductorProxy.reResolveControl(
                udpChannel.channelUri().get(CommonContext.MDC_CONTROL_PARAM_NAME),
                udpChannel,
                this,
                currentControlAddress);
        }
    }

    /**
     * Called from the {@link io.aeron.driver.DriverConductor} to
     * increment image ref count for this channel endpoint.
     */
    public void incRefImages()
    {
        imageRefCount++;
    }

    /**
     * Called from the {@link io.aeron.driver.DriverConductor} to
     * decrement image ref count for this channel endpoint.
     */
    public void decRefImages()
    {
        --imageRefCount;
    }

    private void updateTimeOfLastActivityNs(final long nowNs, final int transportIndex)
    {
        if (null == multiRcvDestination)
        {
            timeOfLastActivityNs = nowNs;
        }
        else
        {
            multiRcvDestination.transport(transportIndex).timeOfLastActivityNs(nowNs);
        }
    }

    private void updateLocalSocketAddress(final String bindAddressAndPort)
    {
        if (null != localSocketAddressIndicator)
        {
            LocalSocketAddressStatus.updateBindAddress(
                localSocketAddressIndicator, bindAddressAndPort, context.countersMetaDataBuffer());
            localSocketAddressIndicator.setRelease(ChannelEndpointStatus.ACTIVE);
        }
    }

    private void applyChannelReceiveTimestamp(final UnsafeBuffer buffer, final int length)
    {
        if (length > DataHeaderFlyweight.HEADER_LENGTH)
        {
            final int offset = udpChannel.channelReceiveTimestampOffset();

            if (DataHeaderFlyweight.DATA_OFFSET + offset + BitUtil.SIZE_OF_LONG < length)
            {
                buffer.putLong(
                    DataHeaderFlyweight.DATA_OFFSET + offset, channelReceiveTimestampClock.nanoTime(), LITTLE_ENDIAN);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "ReceiveChannelEndpoint{" +
            "groupTag=" + groupTag +
            ", isChannelReceiveTimestampEnabled=" + isChannelReceiveTimestampEnabled +
            ", receiverId=" + receiverId +
            ", currentControlAddress=" + currentControlAddress +
            ", imageRefCount=" + imageRefCount +
            ", udpChannel=" + udpChannel +
            ", connectAddress=" + connectAddress +
            ", isClosed=" + isClosed +
            ", multiRcvDestination=" + multiRcvDestination +
            '}';
    }
}
