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

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.ErrorCode;
import io.aeron.driver.DriverConductorProxy;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.NetworkPublication;
import io.aeron.driver.Sender;
import io.aeron.driver.status.MdcDestinations;
import io.aeron.exceptions.ControlProtocolException;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.ErrorFlyweight;
import io.aeron.protocol.NakFlyweight;
import io.aeron.protocol.ResponseSetupFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import io.aeron.status.ChannelEndpointStatus;
import io.aeron.status.LocalSocketAddressStatus;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static io.aeron.driver.media.SendChannelEndpoint.DESTINATION_TIMEOUT;
import static io.aeron.driver.media.UdpChannelTransport.onSendError;
import static io.aeron.driver.status.SystemCounterDescriptor.*;
import static io.aeron.protocol.StatusMessageFlyweight.SEND_SETUP_FLAG;
import static io.aeron.status.ChannelEndpointStatus.status;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.collections.Hashing.compoundKey;

/**
 * Aggregator of multiple {@link NetworkPublication}s onto a single transport channel for the
 * sending of data and setup frames plus the receiving of status and NAK frames.
 */
public class SendChannelEndpoint extends UdpChannelTransport
{
    static final long DESTINATION_TIMEOUT = TimeUnit.SECONDS.toNanos(5);

    private int refCount = 0;
    private long timeOfLastResolutionNs;
    private final Long2ObjectHashMap<NetworkPublication> publicationBySessionAndStreamId = new Long2ObjectHashMap<>();
    private final MultiSndDestination multiSndDestination;
    private final AtomicCounter statusMessagesReceived;
    private final AtomicCounter nakMessagesReceived;
    private final AtomicCounter statusIndicator;
    private final AtomicCounter errorMessagesReceived;
    private final boolean isChannelSendTimestampEnabled;
    private final EpochNanoClock sendTimestampClock;
    private final UnsafeBuffer bufferForTimestamping = new UnsafeBuffer();
    private AtomicCounter localSocketAddressIndicator;
    private AtomicCounter mdcDestinationsCounter;

    /**
     * Construct the sender end for data streams.
     *
     * @param udpChannel      configuration for the media.
     * @param statusIndicator to indicate the status of the channel endpoint.
     * @param context         for configuration.
     */
    public SendChannelEndpoint(
        final UdpChannel udpChannel, final AtomicCounter statusIndicator, final MediaDriver.Context context)
    {
        super(
            udpChannel,
            udpChannel.remoteControl(),
            udpChannel.localControl(),
            udpChannel.isMultiDestination() || udpChannel.isResponseControlMode() ? null : udpChannel.remoteData(),
            context.senderPortManager(),
            context);

        nakMessagesReceived = context.systemCounters().get(NAK_MESSAGES_RECEIVED);
        statusMessagesReceived = context.systemCounters().get(STATUS_MESSAGES_RECEIVED);
        errorMessagesReceived = context.systemCounters().get(ERROR_FRAMES_RECEIVED);
        this.statusIndicator = statusIndicator;

        MultiSndDestination multiSndDestination = null;
        if (udpChannel.isManualControlMode())
        {
            multiSndDestination = new ManualSndMultiDestination(context.senderCachedNanoClock(), errorHandler);
        }
        else if (udpChannel.isDynamicControlMode())
        {
            multiSndDestination = new DynamicSndMultiDestination(context.senderCachedNanoClock(), errorHandler);
        }

        this.multiSndDestination = multiSndDestination;
        this.isChannelSendTimestampEnabled = udpChannel.isChannelSendTimestampEnabled();
        this.sendTimestampClock = context.channelSendTimestampClock();
    }

    /**
     * Set a channel binding status counter.
     *
     * @param counter to be set.
     */
    public void localSocketAddressIndicator(final AtomicCounter counter)
    {
        localSocketAddressIndicator = counter;
    }

    /**
     * Decrement the reference count to the channel.
     */
    public void decRef()
    {
        --refCount;
    }

    /**
     * Increment the reference count to the channel.
     */
    public void incRef()
    {
        ++refCount;
    }

    /**
     * Open the underlying sockets for the channel.
     *
     * @param conductorProxy for notifying potential channel errors.
     */
    public void openChannel(final DriverConductorProxy conductorProxy)
    {
        if (conductorProxy.notConcurrent())
        {
            openDatagramChannel(statusIndicator);
        }
        else
        {
            try
            {
                openDatagramChannel(statusIndicator);
            }
            catch (final Exception ex)
            {
                conductorProxy.channelEndpointError(statusIndicator.id(), ex);
                throw ex;
            }
        }

        LocalSocketAddressStatus.updateBindAddress(
            requireNonNull(localSocketAddressIndicator, "localSocketAddressIndicator not allocated"),
            bindAddressAndPort(),
            context.countersMetaDataBuffer());
        localSocketAddressIndicator.setRelease(ChannelEndpointStatus.ACTIVE);
    }

    /**
     * The original URI String used when a subscription was added.
     *
     * @return the original URI String used when a subscription was added.
     */
    public String originalUriString()
    {
        return udpChannel().originalUriString();
    }

    /**
     * Counter id of the channel status indicator counter.
     *
     * @return id of the channel status indicator counter.
     */
    public int statusIndicatorCounterId()
    {
        return statusIndicator.id();
    }

    /**
     * Indicate that the channel as active after successfully opening it.
     */
    public void indicateActive()
    {
        final long currentStatus = statusIndicator.get();
        if (currentStatus != ChannelEndpointStatus.INITIALIZING)
        {
            throw new IllegalStateException(
                "channel cannot be registered unless INITIALIZING: status=" + status(currentStatus));
        }

        statusIndicator.appendToLabel(bindAddressAndPort());
        statusIndicator.setRelease(ChannelEndpointStatus.ACTIVE);
    }

    /**
     * Close the counters used to indicate channel status.
     */
    public void closeIndicators()
    {
        CloseHelper.close(statusIndicator);
        CloseHelper.close(localSocketAddressIndicator);
        CloseHelper.close(mdcDestinationsCounter);
    }

    /**
     * Called by to determine if the channel endpoint should be closed.
     *
     * @return true if ready to be closed.
     */
    public boolean shouldBeClosed()
    {
        return 0 == refCount && !statusIndicator.isClosed();
    }

    /**
     * Called from the {@link Sender} to add information to the control packet dispatcher.
     *
     * @param publication to add to the dispatcher
     */
    public void registerForSend(final NetworkPublication publication)
    {
        publicationBySessionAndStreamId.put(compoundKey(publication.sessionId(), publication.streamId()), publication);
    }

    /**
     * Called from the {@link Sender} to remove information from the control packet dispatcher.
     *
     * @param publication to remove
     */
    public void unregisterForSend(final NetworkPublication publication)
    {
        publicationBySessionAndStreamId.remove(compoundKey(publication.sessionId(), publication.streamId()));
    }

    /**
     * Send contents of a {@link ByteBuffer} to connected address.
     * This is used on the sender side for performance over send(ByteBuffer, SocketAddress).
     *
     * @param buffer to send
     * @return number of bytes sent
     */
    public int send(final ByteBuffer buffer)
    {
        int bytesSent = 0;

        if (isChannelSendTimestampEnabled)
        {
            applyChannelSendTimestamp(buffer);
        }

        if (null != sendDatagramChannel)
        {
            final int bytesToSend = buffer.remaining();

            if (null == multiSndDestination)
            {
                try
                {
                    sendHook(buffer, connectAddress);
                    if (sendDatagramChannel.isConnected())
                    {
                        bytesSent = sendDatagramChannel.write(buffer);
                    }
                }
                catch (final PortUnreachableException ignore)
                {
                }
                catch (final IOException ex)
                {
                    onSendError(ex, connectAddress, errorHandler);
                }
            }
            else
            {
                bytesSent = multiSndDestination.send(sendDatagramChannel, buffer, this, bytesToSend);
            }
        }

        return bytesSent;
    }

    /**
     * Send contents of a {@link ByteBuffer} to connected address.
     * This is used on the sender side for performance over send(ByteBuffer, SocketAddress).
     *
     * @param buffer to send
     * @param endpointAddress to send data to.
     * @return number of bytes sent
     */
    public int send(final ByteBuffer buffer, final InetSocketAddress endpointAddress)
    {
        int bytesSent = 0;

        if (isChannelSendTimestampEnabled)
        {
            applyChannelSendTimestamp(buffer);
        }

        if (null != sendDatagramChannel)
        {
            try
            {
                sendHook(buffer, endpointAddress);
                bytesSent = sendDatagramChannel.send(buffer, endpointAddress);
            }
            catch (final PortUnreachableException ignore)
            {
            }
            catch (final IOException ex)
            {
                onSendError(ex, connectAddress, errorHandler);
            }
        }

        return bytesSent;
    }

    /**
     * Check sockets may need to be re-resolved due to no activity.
     *
     * @param nowNs          to test against for activity.
     * @param conductorProxy to notify of any addresses which may need to be re-resolved.
     */
    public void checkForReResolution(final long nowNs, final DriverConductorProxy conductorProxy)
    {
        if (udpChannel.isManualControlMode())
        {
            multiSndDestination.checkForReResolution(this, nowNs, conductorProxy);
        }
        else if (udpChannel.hasExplicitEndpoint() && !udpChannel.isMulticast())
        {
            if (statusMessageTimeout(nowNs) && ((timeOfLastResolutionNs + DESTINATION_TIMEOUT) - nowNs) < 0)
            {
                timeOfLastResolutionNs = nowNs;
                final String endpoint = udpChannel.channelUri().get(CommonContext.ENDPOINT_PARAM_NAME);
                conductorProxy.reResolveEndpoint(endpoint, this, connectAddress);
            }
        }
    }

    /**
     * Callback back handler for received status messages.
     *
     * @param msg            flyweight over the status message.
     * @param buffer         containing the message.
     * @param length         of the message.
     * @param srcAddress     of the message.
     * @param conductorProxy to send messages back to the conductor.
     */
    public void onStatusMessage(
        final StatusMessageFlyweight msg,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress,
        final DriverConductorProxy conductorProxy)
    {
        final int sessionId = msg.sessionId();
        final int streamId = msg.streamId();

        statusMessagesReceived.incrementRelease();

        if (null != multiSndDestination)
        {
            multiSndDestination.onStatusMessage(msg, srcAddress);
        }

        final NetworkPublication publication = publicationBySessionAndStreamId.get(compoundKey(sessionId, streamId));
        if (null != publication)
        {
            if (SEND_SETUP_FLAG == (msg.flags() & SEND_SETUP_FLAG))
            {
                publication.triggerSendSetupFrame(msg, srcAddress);
            }
            else
            {
                publication.onStatusMessage(msg, srcAddress, conductorProxy);
            }
        }
    }


    /**
     * Callback back handler for received error messages.
     *
     * @param msg            flyweight over the status message.
     * @param buffer         containing the message.
     * @param length         of the message.
     * @param srcAddress     of the message.
     * @param conductorProxy to send messages back to the conductor.
     */
    public void onError(
        final ErrorFlyweight msg,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress,
        final DriverConductorProxy conductorProxy)
    {
        final int sessionId = msg.sessionId();
        final int streamId = msg.streamId();

        errorMessagesReceived.incrementRelease();

        final long destinationRegistrationId = (null != multiSndDestination) ?
            multiSndDestination.findRegistrationId(msg, srcAddress) : Aeron.NULL_VALUE;

        final NetworkPublication publication = publicationBySessionAndStreamId.get(compoundKey(sessionId, streamId));
        if (null != publication)
        {
            publication.onError(msg, srcAddress, destinationRegistrationId, conductorProxy);
        }
    }

    /**
     * Callback back handler for received NAK messages.
     *
     * @param msg        flyweight over the NAK message.
     * @param buffer     containing the message.
     * @param length     of the message.
     * @param srcAddress of the message.
     */
    public void onNakMessage(
        final NakFlyweight msg,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress)
    {
        final long key = compoundKey(msg.sessionId(), msg.streamId());
        final NetworkPublication publication = publicationBySessionAndStreamId.get(key);

        if (null != publication)
        {
            publication.onNak(msg.termId(), msg.termOffset(), msg.length());
            nakMessagesReceived.incrementRelease();
        }
    }

    /**
     * Callback back handler for received RTT Measurement messages.
     *
     * @param msg        flyweight over the RTT message.
     * @param buffer     containing the message.
     * @param length     of the message.
     * @param srcAddress of the message.
     */
    public void onRttMeasurement(
        final RttMeasurementFlyweight msg,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress)
    {
        final long key = compoundKey(msg.sessionId(), msg.streamId());
        final NetworkPublication publication = publicationBySessionAndStreamId.get(key);
        if (null != publication)
        {
            publication.onRttMeasurement(msg, srcAddress);
        }
    }


    /**
     * Callback to handle a received Response Setup frame.
     *
     * @param msg            of the Response Setup frame.
     * @param unsafeBuffer   containing the Response Setup frame.
     * @param length         of the Response Setup frame.
     * @param srcAddress     the message came from.
     * @param conductorProxy to send messages back to the conductor.
     */
    public void onResponseSetup(
        final ResponseSetupFlyweight msg,
        final UnsafeBuffer unsafeBuffer,
        final int length,
        final InetSocketAddress srcAddress,
        final DriverConductorProxy conductorProxy)
    {
        final long key = compoundKey(msg.sessionId(), msg.streamId());
        final NetworkPublication publication = publicationBySessionAndStreamId.get(key);

        if (null != publication)
        {
            final long responseCorrelationId = publication.responseCorrelationId();
            if (Aeron.NULL_VALUE != responseCorrelationId)
            {
                conductorProxy.responseSetup(responseCorrelationId, msg.responseSessionId());
            }
        }
    }

    /**
     * Validate that the channel allows manual control for destinations.
     * <p>
     * If not then a {@link ControlProtocolException} will be thrown.
     */
    public void validateAllowsManualControl()
    {
        if (!(multiSndDestination instanceof ManualSndMultiDestination))
        {
            throw new ControlProtocolException(ErrorCode.INVALID_CHANNEL, "channel does not allow manual control");
        }
    }

    /**
     * Add a destination for an MDC channel.
     *
     * @param channelUri for the destination to be added.
     * @param address    of the destination to be added.
     * @param registrationId of the destination.
     */
    public void addDestination(final ChannelUri channelUri, final InetSocketAddress address, final long registrationId)
    {
        multiSndDestination.addDestination(channelUri, address, registrationId);
    }

    /**
     * Remove a destination from an MDC channel.
     *
     * @param channelUri for the destination to be removed.
     * @param address    of the destination to be removed.
     */
    public void removeDestination(final ChannelUri channelUri, final InetSocketAddress address)
    {
        multiSndDestination.removeDestination(channelUri, address);
    }

    /**
     * Remove a destination from an MDC channel.
     *
     * @param destinationRegistrationId the registration id of the destination.
     */
    public void removeDestination(final long destinationRegistrationId)
    {
        multiSndDestination.removeDestination(destinationRegistrationId);
    }

    /**
     * Update the endpoint for the channel on address change.
     *
     * @param endpoint   associated with the address.
     * @param newAddress for the endpoint.
     */
    public void resolutionChange(final String endpoint, final InetSocketAddress newAddress)
    {
        if (null != multiSndDestination)
        {
            multiSndDestination.updateDestination(endpoint, newAddress);
        }
        else
        {
            updateEndpoint(newAddress, statusIndicator);
        }
    }

    /**
     * Allocate a destinations counter if the channel uses multiple destinations.
     *
     * @param tempBuffer        to use for metadata formatting.
     * @param countersManager   for the driver.
     * @param registrationId    of the endpoint.
     * @param originalUriString of the channel.
     */
    public void allocateDestinationsCounterForMdc(
        final MutableDirectBuffer tempBuffer,
        final CountersManager countersManager,
        final long registrationId,
        final String originalUriString)
    {
        if (null != multiSndDestination)
        {
            mdcDestinationsCounter = MdcDestinations.allocate(
                tempBuffer, countersManager, registrationId, originalUriString);
            multiSndDestination.destinationsCounter(mdcDestinationsCounter);
        }
    }

    private boolean statusMessageTimeout(final long nowNs)
    {
        for (final NetworkPublication publication : publicationBySessionAndStreamId.values())
        {
            if (((publication.timeOfLastStatusMessageNs() + DESTINATION_TIMEOUT) - nowNs) >= 0)
            {
                return false;
            }
        }

        return true;
    }

    private void applyChannelSendTimestamp(final ByteBuffer buffer)
    {
        final int length = buffer.remaining();

        if (length >= DataHeaderFlyweight.HEADER_LENGTH)
        {
            bufferForTimestamping.wrap(buffer, buffer.position(), length);

            final int type =
                bufferForTimestamping.getShort(DataHeaderFlyweight.TYPE_FIELD_OFFSET, LITTLE_ENDIAN) & 0xFFFF;
            final int flags = bufferForTimestamping.getByte(DataHeaderFlyweight.FLAGS_FIELD_OFFSET) & 0xFF;

            if (DataHeaderFlyweight.HDR_TYPE_DATA == type &&
                0 != (DataHeaderFlyweight.BEGIN_FLAG & flags) &&
                !DataHeaderFlyweight.isHeartbeat(bufferForTimestamping, length))
            {
                final int offset = udpChannel.channelSendTimestampOffset();

                if (DataHeaderFlyweight.DATA_OFFSET + offset + SIZE_OF_LONG <= length)
                {
                    bufferForTimestamping.putLong(
                        DataHeaderFlyweight.DATA_OFFSET + offset, sendTimestampClock.nanoTime(), LITTLE_ENDIAN);
                }
            }
        }
    }

    /**
     * Does the channel have a matching tag?
     *
     * @param udpChannel with tag to match against.
     * @return true if the channel matches on tag identity.
     */
    public boolean matchesTag(final UdpChannel udpChannel)
    {
        return udpChannel.matchesTag(super.udpChannel, null, connectAddress);
    }
}

abstract class MultiSndDestinationLhsPadding
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
}

abstract class MultiSndDestinationHotFields extends MultiSndDestinationLhsPadding
{
    int roundRobinIndex = 0;
}

abstract class MultiSndDestinationRhsPadding extends MultiSndDestinationHotFields
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;
}

abstract class MultiSndDestination extends MultiSndDestinationRhsPadding
{
    static final Destination[] EMPTY_DESTINATIONS = new Destination[0];

    Destination[] destinations = EMPTY_DESTINATIONS;
    final CachedNanoClock nanoClock;
    final ErrorHandler errorHandler;
    AtomicCounter destinationsCounter = null;

    MultiSndDestination(final CachedNanoClock nanoClock, final ErrorHandler errorHandler)
    {
        this.nanoClock = nanoClock;
        this.errorHandler = errorHandler;
    }

    abstract int send(DatagramChannel channel, ByteBuffer buffer, SendChannelEndpoint channelEndpoint, int bytesToSend);

    abstract void onStatusMessage(StatusMessageFlyweight msg, InetSocketAddress address);

    void addDestination(final ChannelUri channelUri, final InetSocketAddress address, final long registrationId)
    {
    }

    void removeDestination(final ChannelUri channelUri, final InetSocketAddress address)
    {
    }

    void removeDestination(final long destinationRegistrationId)
    {
    }

    void checkForReResolution(
        final SendChannelEndpoint channelEndpoint, final long nowNs, final DriverConductorProxy conductorProxy)
    {
    }

    void updateDestination(final String endpoint, final InetSocketAddress newAddress)
    {
    }

    void destinationsCounter(final AtomicCounter destinationsCounter)
    {
        this.destinationsCounter = destinationsCounter;
    }

    static int send(
        final DatagramChannel datagramChannel,
        final ByteBuffer buffer,
        final SendChannelEndpoint channelEndpoint,
        final int bytesToSend,
        final int position,
        final InetSocketAddress destination,
        final ErrorHandler errorHandler)
    {
        int bytesSent = 0;
        try
        {
            if (destination.isUnresolved())
            {
                bytesSent = bytesToSend;
            }
            else if (datagramChannel.isOpen())
            {
                buffer.position(position);
                channelEndpoint.sendHook(buffer, destination);
                bytesSent = datagramChannel.send(buffer, destination);
            }
        }
        catch (final PortUnreachableException ignore)
        {
        }
        catch (final IOException ex)
        {
            onSendError(ex, destination, errorHandler);
        }

        return bytesSent;
    }

    public long findRegistrationId(final ErrorFlyweight msg, final InetSocketAddress srcAddress)
    {
        return Aeron.NULL_VALUE;
    }
}

class ManualSndMultiDestination extends MultiSndDestination
{
    ManualSndMultiDestination(final CachedNanoClock nanoClock, final ErrorHandler errorHandler)
    {
        super(nanoClock, errorHandler);
    }

    void onStatusMessage(final StatusMessageFlyweight msg, final InetSocketAddress address)
    {
        final long receiverId = msg.receiverId();
        final long nowNs = nanoClock.nanoTime();

        for (final Destination destination : destinations)
        {
            if (destination.isMatch(msg.receiverId(), address))
            {
                if (!destination.isReceiverIdValid)
                {
                    destination.receiverId = receiverId;
                    destination.isReceiverIdValid = true;
                }

                destination.timeOfLastActivityNs = nowNs;
                break;
            }
        }
    }

    int send(
        final DatagramChannel channel,
        final ByteBuffer buffer,
        final SendChannelEndpoint channelEndpoint,
        final int bytesToSend)
    {
        final int position = buffer.position();
        final int length = destinations.length;

        int startingIndex = roundRobinIndex++;
        if (startingIndex >= length)
        {
            roundRobinIndex = startingIndex = 0;
        }

        int result = bytesToSend;
        for (int i = startingIndex; i < length; i++)
        {
            final Destination destination = destinations[i];

            final int bytesSent = send(
                channel, buffer, channelEndpoint, bytesToSend, position, destination.address, errorHandler);
            if (bytesSent < bytesToSend)
            {
                result = bytesSent;
            }
        }

        for (int i = 0; i < startingIndex; i++)
        {
            final Destination destination = destinations[i];

            final int bytesSent = send(
                channel, buffer, channelEndpoint, bytesToSend, position, destination.address, errorHandler);
            if (bytesSent < bytesToSend)
            {
                result = bytesSent;
            }
        }

        return result;
    }

    void addDestination(final ChannelUri channelUri, final InetSocketAddress address, final long registrationId)
    {
        final Destination destination = new Destination(
            nanoClock.nanoTime(), channelUri.get(CommonContext.ENDPOINT_PARAM_NAME), address, registrationId);
        destinations = ArrayUtil.add(destinations, destination);
        destinationsCounter.setRelease(destinations.length);
    }

    void removeDestination(final ChannelUri channelUri, final InetSocketAddress address)
    {
        boolean found = false;
        int index = 0;
        for (final Destination destination : destinations)
        {
            if (destination.address.equals(address))
            {
                found = true;
                break;
            }

            index++;
        }

        if (found)
        {
            if (1 == destinations.length)
            {
                destinations = EMPTY_DESTINATIONS;
            }
            else
            {
                destinations = ArrayUtil.remove(destinations, index);
            }
        }

        destinationsCounter.setRelease(destinations.length);
    }

    void removeDestination(final long destinationRegistrationId)
    {
        boolean found = false;
        int index = 0;
        for (final Destination destination : destinations)
        {
            if (destination.registrationId == destinationRegistrationId)
            {
                found = true;
                break;
            }

            index++;
        }

        if (found)
        {
            if (1 == destinations.length)
            {
                destinations = EMPTY_DESTINATIONS;
            }
            else
            {
                destinations = ArrayUtil.remove(destinations, index);
            }
        }

        destinationsCounter.setRelease(destinations.length);
    }

    void checkForReResolution(
        final SendChannelEndpoint channelEndpoint, final long nowNs, final DriverConductorProxy conductorProxy)
    {
        for (final Destination destination : destinations)
        {
            if ((destination.timeOfLastActivityNs + DESTINATION_TIMEOUT) - nowNs < 0)
            {
                destination.timeOfLastActivityNs = nowNs;
                conductorProxy.reResolveEndpoint(destination.endpoint, channelEndpoint, destination.address);
            }
        }
    }

    void updateDestination(final String endpoint, final InetSocketAddress newAddress)
    {
        for (final Destination destination : destinations)
        {
            if (endpoint.equals(destination.endpoint))
            {
                destination.address = newAddress;
                destination.port = newAddress.getPort();
            }
        }
    }

    public long findRegistrationId(final ErrorFlyweight msg, final InetSocketAddress address)
    {
        for (final Destination destination : destinations)
        {
            if (destination.isMatch(msg.receiverId(), address))
            {
                return destination.registrationId;
            }
        }

        return Aeron.NULL_VALUE;
    }
}

class DynamicSndMultiDestination extends MultiSndDestination
{
    DynamicSndMultiDestination(final CachedNanoClock nanoClock, final ErrorHandler errorHandler)
    {
        super(nanoClock, errorHandler);
    }

    void onStatusMessage(final StatusMessageFlyweight msg, final InetSocketAddress address)
    {
        final long receiverId = msg.receiverId();
        final long nowNs = nanoClock.nanoTime();
        boolean isExisting = false;

        for (final Destination destination : destinations)
        {
            if (receiverId == destination.receiverId && address.getPort() == destination.port)
            {
                destination.timeOfLastActivityNs = nowNs;
                isExisting = true;
                break;
            }
        }

        if (!isExisting)
        {
            add(new Destination(nowNs, receiverId, address));
        }
    }

    int send(
        final DatagramChannel channel,
        final ByteBuffer buffer,
        final SendChannelEndpoint channelEndpoint,
        final int bytesToSend)
    {
        final long nowNs = nanoClock.nanoTime();
        final int position = buffer.position();
        final int length = destinations.length;
        int inactiveDestinationCount = 0;

        int startingIndex = roundRobinIndex++;
        if (startingIndex >= length)
        {
            roundRobinIndex = startingIndex = 0;
        }

        int result = bytesToSend;

        for (int i = startingIndex; i < length; i++)
        {
            final Destination destination = destinations[i];

            if ((destination.timeOfLastActivityNs + DESTINATION_TIMEOUT) - nowNs >= 0)
            {
                final int bytesSent = send(
                    channel, buffer, channelEndpoint, bytesToSend, position, destination.address, errorHandler);
                if (bytesSent < bytesToSend)
                {
                    result = bytesSent;
                }
            }
            else
            {
                inactiveDestinationCount++;
            }
        }

        for (int i = 0; i < startingIndex; i++)
        {
            final Destination destination = destinations[i];

            if ((destination.timeOfLastActivityNs + DESTINATION_TIMEOUT) - nowNs >= 0)
            {
                final int bytesSent = send(
                    channel, buffer, channelEndpoint, bytesToSend, position, destination.address, errorHandler);
                if (bytesSent < bytesToSend)
                {
                    result = bytesSent;
                }
            }
            else
            {
                inactiveDestinationCount++;
            }
        }

        if (inactiveDestinationCount > 0)
        {
            removeInactiveDestinations(nowNs);
        }

        return result;
    }

    private void add(final Destination destination)
    {
        destinations = ArrayUtil.add(destinations, destination);
        destinationsCounter.setRelease(destinations.length);
    }

    private void truncateDestinations(final int removedCount)
    {
        final int length = destinations.length;
        final int newLength = length - removedCount;

        if (0 == newLength)
        {
            destinations = EMPTY_DESTINATIONS;
        }
        else
        {
            destinations = Arrays.copyOf(destinations, newLength);
        }

        destinationsCounter.setRelease(destinations.length);
    }

    private void removeInactiveDestinations(final long nowNs)
    {
        int removedCount = 0;

        for (int lastIndex = destinations.length - 1, i = lastIndex; i >= 0; i--)
        {
            final Destination destination = destinations[i];
            if ((destination.timeOfLastActivityNs + DESTINATION_TIMEOUT) - nowNs < 0)
            {
                if (i != lastIndex)
                {
                    destinations[i] = destinations[lastIndex--];
                }
                removedCount++;
            }
        }

        if (removedCount > 0)
        {
            truncateDestinations(removedCount);
        }
    }
}

abstract class DestinationLhsPadding
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
}

abstract class DestinationHotFields extends DestinationLhsPadding
{
    long timeOfLastActivityNs;
}

abstract class DestinationRhsPadding extends DestinationHotFields
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;
}

final class Destination extends DestinationRhsPadding
{
    long receiverId;
    final long registrationId;
    boolean isReceiverIdValid;
    int port;
    InetSocketAddress address;
    final String endpoint;

    Destination(final long nowNs, final long receiverId, final InetSocketAddress address)
    {
        this.timeOfLastActivityNs = nowNs;
        this.receiverId = receiverId;
        this.isReceiverIdValid = true;
        this.endpoint = null;
        this.address = address;
        this.port = address.getPort();
        this.registrationId = Aeron.NULL_VALUE;
    }

    Destination(final long nowMs, final String endpoint, final InetSocketAddress address, final long registrationId)
    {
        this.timeOfLastActivityNs = nowMs;
        this.receiverId = 0;
        this.isReceiverIdValid = false;
        this.endpoint = endpoint;
        this.address = address;
        this.port = address.getPort();
        this.registrationId = registrationId;
    }

    boolean isMatch(final long receiverId, final InetSocketAddress address)
    {
        return
            (isReceiverIdValid && receiverId == this.receiverId && address.getPort() == this.port) ||
            (!isReceiverIdValid &&
                address.getPort() == this.port && address.getAddress().equals(this.address.getAddress()));
    }
}