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

import io.aeron.driver.MediaDriver;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.exceptions.AeronEvent;
import io.aeron.exceptions.AeronException;
import io.aeron.protocol.HeaderFlyweight;
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
import java.nio.channels.SelectionKey;

import static io.aeron.logbuffer.FrameDescriptor.frameVersion;
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

    /**
     * To be used when polling the transport.
     */
    protected SelectionKey selectionKey;

    private UdpTransportPoller transportPoller;
    private InetSocketAddress bindAddress;
    private final InetSocketAddress endPointAddress;
    private final AtomicCounter invalidPackets;
    private final PortManager portManager;

    /**
     * Can be used to check if the transport is closed so an operation does not proceed.
     */
    protected boolean isClosed = false;
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
     * Register this transport for reading from a {@link UdpTransportPoller}.
     *
     * @param transportPoller to register for read with.
     */
    public void registerForRead(final UdpTransportPoller transportPoller)
    {
        this.transportPoller = transportPoller;
        selectionKey = transportPoller.registerForRead(this);
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
     * Close transport, canceling any pending read operations and closing channel.
     */
    public void close()
    {
        if (!isClosed)
        {
            isClosed = true;
            if (null != selectionKey)
            {
                selectionKey.cancel();
            }

            if (null != transportPoller)
            {
                transportPoller.cancelRead(this);
                transportPoller.selectNowWithoutProcessing();
            }

            CloseHelper.close(errorHandler, sendDatagramChannel);
            CloseHelper.close(errorHandler, receiveDatagramChannel);

            if (null != transportPoller)
            {
                transportPoller.selectNowWithoutProcessing();
            }

            portManager.freeManagedPort(bindAddress);
        }
    }

    /**
     * Has the channel been closed by calling {@link #close()}.
     *
     * @return true if the channel has been closed.
     */
    public boolean isClosed()
    {
        return isClosed;
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
        if (length >= HeaderFlyweight.MIN_HEADER_LENGTH &&
            frameVersion(buffer, 0) == HeaderFlyweight.CURRENT_VERSION)
        {
            return true;
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
    @SuppressWarnings("unused")
    public void sendHook(final ByteBuffer buffer, final InetSocketAddress address)
    {
    }

    /**
     * Receive packet hook that can be useful for logging.
     *
     * @param buffer  containing the packet.
     * @param length  length of the packet in bytes.
     * @param address from which the packet came.
     */
    @SuppressWarnings("unused")
    public void receiveHook(final UnsafeBuffer buffer, final int length, final InetSocketAddress address)
    {
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
    @SuppressWarnings("unused")
    public void resendHook(
        final int sessionId, final int streamId, final int termId, final int termOffset, final int length)
    {
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
