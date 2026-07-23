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

import io.aeron.driver.DriverLog;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.exceptions.AeronEvent;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.ErrorFlyweight;
import io.aeron.protocol.NakFlyweight;
import io.aeron.protocol.ResolutionEntryFlyweight;
import io.aeron.protocol.ResponseSetupFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.SetupFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

import static io.aeron.protocol.HeaderFlyweight.CURRENT_VERSION;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_ERR;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_NAK;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_RES;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_RSP_SETUP;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_RTTM;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_SETUP;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_SM;
import static io.aeron.protocol.HeaderFlyweight.MIN_HEADER_LENGTH;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_SNDBUF;

/**
 * Base class for UDP channel transports which is specialised for send or receive endpoints.
 */
public abstract class UdpChannelTransport implements AutoCloseable
{
    /**
     * Context for configuration.
     */
    protected final MediaDriver.Context context;

    /**
     * {@link ErrorHandler} for logging errors and progressing with throwing.
     */
    protected final ErrorHandler errorHandler;

    /**
     * Media configuration for the channel.
     */
    protected final UdpChannel udpChannel;

    /**
     * Channel to be used for sending frames from the perspective of the endpoint.
     */
    protected DatagramChannel sendDatagramChannel;

    /**
     * Channel to be used for receiving frames from the perspective of the endpoint.
     */
    protected DatagramChannel receiveDatagramChannel;

    /**
     * Address to connect to if appropriate for sending.
     */
    protected InetSocketAddress connectAddress;

    private InetSocketAddress bindAddress;
    private final InetSocketAddress endPointAddress;
    final AtomicCounter invalidPackets;
    private final PortManager portManager;

    private int multicastTtl = 0;
    private final int socketSndbufLength;
    private final int socketRcvbufLength;

    /**
     * Construct transport for a given channel.
     *
     * @param udpChannel         configuration for the media.
     * @param endPointAddress    to which data will be sent.
     * @param bindAddress        for listening on.
     * @param connectAddress     for sending data to.
     * @param context            for configuration.
     * @param portManager        for port binding.
     * @param socketRcvbufLength set SO_RCVBUF for socket, 0 for OS default.
     * @param socketSndbufLength set SO_SNDBUF for socket, 0 for OS default.
     */
    protected UdpChannelTransport(
        final UdpChannel udpChannel,
        final InetSocketAddress endPointAddress,
        final InetSocketAddress bindAddress,
        final InetSocketAddress connectAddress,
        final PortManager portManager,
        final MediaDriver.Context context,
        final int socketRcvbufLength,
        final int socketSndbufLength)
    {
        this.context = context;
        this.udpChannel = udpChannel;
        this.errorHandler = context.countedErrorHandler();
        this.portManager = portManager;
        this.endPointAddress = endPointAddress;
        this.bindAddress = bindAddress;
        this.connectAddress = connectAddress;
        this.invalidPackets = context.systemCounters().get(SystemCounterDescriptor.INVALID_PACKETS);
        this.socketRcvbufLength = socketRcvbufLength;
        this.socketSndbufLength = socketSndbufLength;
    }

    /**
     * Construct transport for a given channel.
     *
     * @param udpChannel      configuration for the media.
     * @param endPointAddress to which data will be sent.
     * @param bindAddress     for listening on.
     * @param connectAddress  for sending data to.
     * @param portManager     for port binding.
     * @param context         for configuration.
     */
    protected UdpChannelTransport(
        final UdpChannel udpChannel,
        final InetSocketAddress endPointAddress,
        final InetSocketAddress bindAddress,
        final InetSocketAddress connectAddress,
        final PortManager portManager,
        final MediaDriver.Context context)
    {
        this(
            udpChannel,
            endPointAddress,
            bindAddress,
            connectAddress,
            portManager,
            context,
            udpChannel.socketRcvbufLengthOrDefault(context.socketRcvbufLength()),
            udpChannel.socketSndbufLengthOrDefault(context.socketSndbufLength()));
    }

    /**
     * Throw a {@link AeronException} with a message for a send error.
     *
     * @param bytesToSend expected to be sent to the network.
     * @param ex          experienced.
     * @param destination to which the send operation was addressed.
     * @see #onSendError(IOException, InetSocketAddress, ErrorHandler)
     * @deprecated {@link #onSendError(IOException, InetSocketAddress, ErrorHandler)} is used instead.
     */
    @Deprecated(forRemoval = true, since = "1.46.6")
    public static void sendError(final int bytesToSend, final IOException ex, final InetSocketAddress destination)
    {
        throw new AeronException(
            "failed to send " + bytesToSend + " byte packet to " + destination, ex, AeronException.Category.WARN);
    }

    /**
     * Report an {@link AeronEvent} with a message for a send error.
     *
     * @param ex           experienced.
     * @param destination  to which the send operation was addressed.
     * @param errorHandler to report error to.
     */
    public static void onSendError(
        final IOException ex, final InetSocketAddress destination, final ErrorHandler errorHandler)
    {
        errorHandler.onError(new AeronEvent(
            "failed to send datagram to " + destination + ", cause: " + ex, AeronException.Category.WARN));
    }

    /**
     * Open the underlying channel for reading and writing.
     *
     * @param statusIndicator to set for {@link ChannelEndpointStatus} which could be
     *                        {@link ChannelEndpointStatus#ERRORED}.
     */
    public void openDatagramChannel(final AtomicCounter statusIndicator)
    {
        try
        {
            sendDatagramChannel = DatagramChannel.open(udpChannel.protocolFamily());
            receiveDatagramChannel = sendDatagramChannel;

            if (udpChannel.isMulticast())
            {
                if (null != connectAddress)
                {
                    receiveDatagramChannel = DatagramChannel.open(udpChannel.protocolFamily());
                }

                receiveDatagramChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                receiveDatagramChannel.bind(new InetSocketAddress(endPointAddress.getPort()));
                receiveDatagramChannel.join(endPointAddress.getAddress(), udpChannel.localInterface());
                sendDatagramChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, udpChannel.localInterface());

                if (udpChannel.hasMulticastTtl())
                {
                    sendDatagramChannel.setOption(StandardSocketOptions.IP_MULTICAST_TTL, udpChannel.multicastTtl());
                    multicastTtl = sendDatagramChannel.getOption(StandardSocketOptions.IP_MULTICAST_TTL);
                }
                else if (context.socketMulticastTtl() != 0)
                {
                    sendDatagramChannel.setOption(StandardSocketOptions.IP_MULTICAST_TTL, context.socketMulticastTtl());
                    multicastTtl = sendDatagramChannel.getOption(StandardSocketOptions.IP_MULTICAST_TTL);
                }
            }
            else
            {
                bindAddress = portManager.getManagedPort(udpChannel, bindAddress);
                sendDatagramChannel.bind(bindAddress);
            }

            if (null != connectAddress)
            {
                sendDatagramChannel.connect(connectAddress);
            }

            if (0 != socketSndbufLength())
            {
                sendDatagramChannel.setOption(SO_SNDBUF, socketSndbufLength());
            }

            if (0 != socketRcvbufLength())
            {
                receiveDatagramChannel.setOption(SO_RCVBUF, socketRcvbufLength());
            }

            sendDatagramChannel.configureBlocking(false);
            receiveDatagramChannel.configureBlocking(false);
        }
        catch (final IOException ex)
        {
            if (null != statusIndicator)
            {
                statusIndicator.setRelease(ChannelEndpointStatus.ERRORED);
            }

            CloseHelper.quietClose(sendDatagramChannel);
            if (receiveDatagramChannel != sendDatagramChannel)
            {
                CloseHelper.quietClose(receiveDatagramChannel);
            }

            sendDatagramChannel = null;
            receiveDatagramChannel = null;

            final String message = "channel error - " + ex.getMessage() +
                " (at " + ex.getStackTrace()[0].toString() + "): " + udpChannel.originalUriString();

            throw new AeronException(message, ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.close(errorHandler, sendDatagramChannel);
        if (receiveDatagramChannel != sendDatagramChannel)
        {
            CloseHelper.close(errorHandler, receiveDatagramChannel);
        }
        portManager.freeManagedPort(bindAddress);
    }

    /**
     * Return underlying {@link UdpChannel}.
     *
     * @return underlying channel.
     */
    public UdpChannel udpChannel()
    {
        return udpChannel;
    }

    /**
     * The {@link DatagramChannel} for this transport channel.
     *
     * @return {@link DatagramChannel} for this transport channel.
     */
    public DatagramChannel receiveDatagramChannel()
    {
        return receiveDatagramChannel;
    }

    /**
     * Get the multicast TTL value for sending datagrams on the channel.
     *
     * @return the multicast TTL value for sending datagrams on the channel.
     */
    public int multicastTtl()
    {
        return multicastTtl;
    }

    /**
     * Get the bind address and port in endpoint-style format (ip:port).
     * <p>
     * Must be called after the channel is opened.
     *
     * @return the bind address and port in endpoint-style format (ip:port).
     */
    public String bindAddressAndPort()
    {
        try
        {
            final InetSocketAddress localAddress = (InetSocketAddress)receiveDatagramChannel.getLocalAddress();
            if (null != localAddress)
            {
                return NetworkUtil.formatAddressAndPort(localAddress.getAddress(), localAddress.getPort());
            }
        }
        catch (final IOException ignore)
        {
        }

        return "";
    }

    /**
     * Is transport representing a multicast media?
     *
     * @return true if transport is multicast media, otherwise false.
     */
    public boolean isMulticast()
    {
        return udpChannel.isMulticast();
    }

    /**
     * Is the received frame valid. This method will do some basic checks on the header and can be
     * overridden in a subclass for further validation.
     *
     * @param buffer containing the frame.
     * @param length of the frame.
     * @return true if the frame is believed valid otherwise false.
     */
    public boolean isValidFrame(final UnsafeBuffer buffer, final int length)
    {
        final int frameLength;
        if (length > MIN_HEADER_LENGTH &&
            (frameLength = FrameDescriptor.frameLength(buffer, 0)) >= 0 &&
            CURRENT_VERSION == FrameDescriptor.frameVersion(buffer, 0))
        {
            final int frameType = FrameDescriptor.frameType(buffer, 0);
            if (HDR_TYPE_DATA == frameType || HDR_TYPE_PAD == frameType)
            {
                if (length >= DataHeaderFlyweight.HEADER_LENGTH && FrameDescriptor.isFrameAligned(length))
                {
                    return true;
                }
            }
            else
            {
                final boolean valid = switch (frameType)
                {
                    case HDR_TYPE_NAK -> length >= NakFlyweight.HEADER_LENGTH && frameLength <= length;
                    case HDR_TYPE_SM -> length >= StatusMessageFlyweight.HEADER_LENGTH && frameLength <= length;
                    case HDR_TYPE_ERR -> length >= ErrorFlyweight.HEADER_LENGTH && frameLength <= length;
                    case HDR_TYPE_SETUP -> length >= SetupFlyweight.HEADER_LENGTH && frameLength <= length;
                    case HDR_TYPE_RTTM -> length >= RttMeasurementFlyweight.HEADER_LENGTH && frameLength <= length;
                    case HDR_TYPE_RES -> length >= MIN_HEADER_LENGTH + ResolutionEntryFlyweight.MIN_IPV4_FRAME_LENGTH &&
                        frameLength <= length;
                    case HDR_TYPE_RSP_SETUP -> length >= ResponseSetupFlyweight.HEADER_LENGTH && frameLength <= length;
                    default -> false;
                };
                if (valid)
                {
                    return true;
                }
            }
        }

        invalidPackets.increment();
        return false;
    }

    /**
     * Send packet hook that can be used for logging.
     *
     * @param buffer  containing the packet.
     * @param address to which the packet will be sent.
     */
    public void sendHook(final ByteBuffer buffer, final InetSocketAddress address)
    {
        DriverLog.logFrameOut(buffer, address);
    }

    /**
     * Receive packet hook that can be useful for logging.
     *
     * @param buffer  containing the packet.
     * @param length  length of the packet in bytes.
     * @param address from which the packet came.
     */
    public void receiveHook(final UnsafeBuffer buffer, final int length, final InetSocketAddress address)
    {
        DriverLog.logFrameIn(buffer, 0, length, address);
    }

    /**
     * Useful hook for logging resend calls.
     *
     * @param sessionId  to resend
     * @param streamId   to resend
     * @param termId     to resend
     * @param termOffset to resend
     * @param length     to resend
     */
    public void resendHook(
        final int sessionId, final int streamId, final int termId, final int termOffset, final int length)
    {
        DriverLog.logResend(sessionId, streamId, termId, termOffset, length, udpChannel.originalUriString());
    }

    /**
     * Receive a datagram from the media layer.
     *
     * @param buffer into which the datagram will be received.
     * @return the source address of the datagram if one is available otherwise false.
     */
    public InetSocketAddress receive(final ByteBuffer buffer)
    {
        buffer.clear();

        InetSocketAddress address = null;
        try
        {
            if (receiveDatagramChannel.isOpen())
            {
                address = (InetSocketAddress)receiveDatagramChannel.receive(buffer);
            }
        }
        catch (final PortUnreachableException ignored)
        {
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return address;
    }

    /**
     * Endpoint has moved to a new address. Handle this.
     *
     * @param newAddress      to send data to.
     * @param statusIndicator for the channel
     */
    public void updateEndpoint(final InetSocketAddress newAddress, final AtomicCounter statusIndicator)
    {
        try
        {
            if (null != sendDatagramChannel)
            {
                sendDatagramChannel.disconnect();
                sendDatagramChannel.connect(newAddress);
                connectAddress = newAddress;

                if (null != statusIndicator)
                {
                    statusIndicator.setRelease(ChannelEndpointStatus.ACTIVE);
                }
            }
        }
        catch (final Exception ex)
        {
            if (null != statusIndicator)
            {
                statusIndicator.setRelease(ChannelEndpointStatus.ERRORED);
            }

            final String message = "re-resolve endpoint channel error - " + ex.getMessage() +
                " (at " + ex.getStackTrace()[0].toString() + "): " + udpChannel.originalUriString();

            throw new AeronException(message, ex);
        }
    }

    /**
     * Get the configured OS send socket buffer length (SO_SNDBUF) for the endpoint's socket.
     *
     * @return OS socket send buffer length or 0 if using OS default.
     */
    public int socketSndbufLength()
    {
        return socketSndbufLength;
    }

    /**
     * Get the configured OS receive socket buffer length (SO_RCVBUF) for the endpoint's socket.
     *
     * @return OS socket receive buffer length or 0 if using OS default.
     */
    public int socketRcvbufLength()
    {
        return socketRcvbufLength;
    }
}
