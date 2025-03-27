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
import io.aeron.driver.DriverConductorProxy;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.aeron.driver.media.ReceiveChannelEndpoint.DESTINATION_ADDRESS_TIMEOUT;
import static io.aeron.driver.media.UdpChannelTransport.onSendError;

final class MultiRcvDestination
{
    private static final ReceiveDestinationTransport[] EMPTY_TRANSPORTS = new ReceiveDestinationTransport[0];

    private ReceiveDestinationTransport[] transports = EMPTY_TRANSPORTS;

    private final AtomicCounter receiveChannelShortSends;
    private final AtomicCounter receiveChannelConnectionErrors;

    MultiRcvDestination(
        final AtomicCounter receiveChannelShortSends,
        final AtomicCounter receiveChannelConnectionErrors)
    {
        this.receiveChannelShortSends = receiveChannelShortSends;
        this.receiveChannelConnectionErrors = receiveChannelConnectionErrors;
    }

    void closeTransports(final ReceiveChannelEndpoint endpoint, final DataTransportPoller poller)
    {
        for (final ReceiveDestinationTransport transport : transports)
        {
            if (null != transport)
            {
                poller.cancelRead(endpoint, transport);
                transport.closeTransport();
                poller.selectNowWithoutProcessing();
            }
        }
    }

    void closeIndicators(final DriverConductorProxy conductorProxy)
    {
        for (final ReceiveDestinationTransport transport : transports)
        {
            if (null != transport)
            {
                conductorProxy.closeReceiveDestinationIndicators(transport);
            }
        }
    }

    int addDestination(final ReceiveDestinationTransport transport)
    {
        int index = transports.length;

        for (int i = 0, length = transports.length; i < length; i++)
        {
            if (null == transports[i])
            {
                index = i;
                break;
            }
        }

        transports = ArrayUtil.ensureCapacity(transports, index + 1);
        transports[index] = transport;

        return index;
    }

    void removeDestination(final int transportIndex)
    {
        transports[transportIndex] = null;
    }

    boolean hasDestination(final int transportIndex)
    {
        return transports.length > transportIndex && null != transports[transportIndex];
    }

    ReceiveDestinationTransport transport(final int transportIndex)
    {
        return transports[transportIndex];
    }

    int transport(final UdpChannel udpChannel)
    {
        final ReceiveDestinationTransport[] transports = this.transports;
        int index = ArrayUtil.UNKNOWN_INDEX;

        for (int i = 0, length = transports.length; i < length; i++)
        {
            final ReceiveDestinationTransport transport = transports[i];

            if (null != transport && transport.udpChannel().equals(udpChannel))
            {
                index = i;
                break;
            }
        }

        return index;
    }

    void checkForReResolution(
        final ReceiveChannelEndpoint channelEndpoint, final long nowNs, final DriverConductorProxy conductorProxy)
    {
        for (final ReceiveDestinationTransport transport : transports)
        {
            if (null != transport)
            {
                final UdpChannel udpChannel = transport.udpChannel();

                if (udpChannel.hasExplicitControl() &&
                    (transport.timeOfLastActivityNs() + DESTINATION_ADDRESS_TIMEOUT) < nowNs)
                {
                    transport.timeOfLastActivityNs(nowNs);
                    conductorProxy.reResolveControl(
                        udpChannel.channelUri().get(CommonContext.MDC_CONTROL_PARAM_NAME),
                        udpChannel,
                        channelEndpoint,
                        transport.currentControlAddress());
                }
            }
        }
    }

    void updateControlAddress(final int transportIndex, final InetSocketAddress newAddress)
    {
        if (ArrayUtil.UNKNOWN_INDEX != transportIndex)
        {
            final ReceiveDestinationTransport transport = transports[transportIndex];

            if (null != transport)
            {
                transport.currentControlAddress(newAddress);
            }
        }
    }

    int sendToAll(
        final ImageConnection[] imageConnections, final ByteBuffer buffer, final int bytesToSend, final long nowNs)
    {
        final ReceiveDestinationTransport[] transports = this.transports;
        int minBytesSent = bytesToSend;

        for (int lastIndex = imageConnections.length - 1, i = lastIndex; i >= 0; i--)
        {
            final ImageConnection connection = imageConnections[i];

            if (null != connection)
            {
                final UdpChannelTransport transport = transports[i];
                if (null != transport && ((connection.timeOfLastActivityNs + DESTINATION_ADDRESS_TIMEOUT) - nowNs > 0))
                {
                    buffer.position(0);
                    minBytesSent = Math.min(minBytesSent, sendTo(transport, buffer, connection.controlAddress,
                        receiveChannelShortSends, receiveChannelConnectionErrors));
                }
            }
        }

        return minBytesSent;
    }

    static int sendTo(
        final UdpChannelTransport transport,
        final ByteBuffer buffer,
        final InetSocketAddress remoteAddress,
        final AtomicCounter receiveChannelShortSends,
        final AtomicCounter receiveChannelConnectionErrors)
    {
        int bytesSent = 0;
        try
        {
            if (null != transport && null != transport.sendDatagramChannel && transport.sendDatagramChannel.isOpen())
            {
                final int bytesToSend = buffer.remaining();

                transport.sendHook(buffer, remoteAddress);
                bytesSent = transport.sendDatagramChannel.send(buffer, remoteAddress);

                if (bytesSent < bytesToSend)
                {
                    receiveChannelShortSends.incrementRelease();
                }
            }
        }
        catch (final PortUnreachableException ignore)
        {
            receiveChannelConnectionErrors.incrementRelease();
        }
        catch (final IOException ex)
        {
            onSendError(ex, remoteAddress, transport.errorHandler);
        }

        return bytesSent;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "MultiRcvDestination{" +
            "transports=" + Arrays.toString(transports) +
            '}';
    }
}
