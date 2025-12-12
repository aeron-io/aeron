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
import io.aeron.driver.MediaDriver;
import io.aeron.driver.NameResolver;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

/**
 * {@link UdpChannelTransport} specialised for name resolution between {@link MediaDriver}s.
 */
public final class UdpNameResolutionTransport extends UdpChannelTransport
{
    /**
     * Handler for processing the received frames.
     */
    @FunctionalInterface
    public interface UdpFrameHandler
    {
        /**
         * Callback for processing the received frames.
         *
         * @param unsafeBuffer containing the received frame.
         * @param length       of the frame in the buffer.
         * @param srcAddress   the frame came from.
         * @param nowMs        current time.
         * @return the number of bytes received.
         */
        int onFrame(UnsafeBuffer unsafeBuffer, int length, InetSocketAddress srcAddress, long nowMs);
    }

    private final UnsafeBuffer unsafeBuffer;
    private final ByteBuffer byteBuffer;

    /**
     * Construct a new channel transport for name resolution.
     *
     * @param udpChannel      associated with the transport.
     * @param resolverAddress to listen on.
     * @param unsafeBuffer    for reading frames.
     * @param context         for configuration.
     */
    public UdpNameResolutionTransport(
        final UdpChannel udpChannel,
        final InetSocketAddress resolverAddress,
        final UnsafeBuffer unsafeBuffer,
        final MediaDriver.Context context)
    {
        super(udpChannel, null, resolverAddress, null, context.receiverPortManager(), context);

        this.unsafeBuffer = unsafeBuffer;
        this.byteBuffer = unsafeBuffer.byteBuffer();
    }

    /**
     * Poll the transport for received frames to be delivered to a {@link UdpFrameHandler}.
     *
     * @param handler for processing the frames.
     * @param nowMs   current time.
     * @return number of bytes received.
     */
    public int poll(final UdpFrameHandler handler, final long nowMs)
    {
        int bytesReceived = 0;
        final InetSocketAddress srcAddress = receive(byteBuffer);

        if (null != srcAddress)
        {
            final int length = byteBuffer.position();

            if (isValidFrame(unsafeBuffer, length))
            {
                receiveHook(unsafeBuffer, length, srcAddress);
                bytesReceived = handler.onFrame(unsafeBuffer, length, srcAddress, nowMs);
            }
        }

        return bytesReceived;
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
        catch (final IOException ex)
        {
            onSendError(ex, remoteAddress, errorHandler);
        }

        return bytesSent;
    }

    /**
     * The {@link InetSocketAddress} which the resolver is bound to for listening to requests.
     *
     * @return the {@link InetSocketAddress} which the resolver is bound to for listening to requests.
     */
    public InetSocketAddress boundAddress()
    {
        try
        {
            return (InetSocketAddress)receiveDatagramChannel.getLocalAddress();
        }
        catch (final IOException ex)
        {
            return null;
        }
    }

    /**
     * Get {@link InetSocketAddress} for an interface of an address and port.
     *
     * @param addressAndPort for the endpoint.
     * @return the {@link InetSocketAddress} if successful or null if not.
     */
    public static InetSocketAddress getInterfaceAddress(final String addressAndPort)
    {
        try
        {
            return InterfaceSearchAddress.parse(addressAndPort).address();
        }
        catch (final UnknownHostException ex)
        {
            LangUtil.rethrowUnchecked(ex);
            return null;
        }
    }

    /**
     * Get the {@link InetSocketAddress} for a host and port endpoint using the default name resolver.
     *
     * @param hostAndPort  to parse.
     * @param nameResolver to resolve a name to an {@link InetAddress}.
     * @return the resolved {@link InetSocketAddress} if successful or null if not.
     */
    public static InetSocketAddress getInetSocketAddress(final String hostAndPort, final NameResolver nameResolver)
    {
        InetSocketAddress address = null;

        try
        {
            address = SocketAddressParser.parse(
                hostAndPort, CommonContext.ENDPOINT_PARAM_NAME, false, nameResolver);

            if (address.isUnresolved())
            {
                throw new UnknownHostException(
                    "unresolved - " + CommonContext.ENDPOINT_PARAM_NAME + "=" + hostAndPort +
                    ", name-resolver=" + nameResolver.getClass().getName());
            }
        }
        catch (final UnknownHostException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return address;
    }
}
