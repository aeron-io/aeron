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

import io.aeron.driver.Configuration;
import io.aeron.driver.DriverConductorProxy;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.SetupFlyweight;
import org.agrona.BufferUtil;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.collections.ArrayListUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.nio.TransportPoller;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.function.Consumer;

import static io.aeron.logbuffer.FrameDescriptor.frameType;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_RTTM;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_SETUP;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;

/**
 * Encapsulates the polling of data {@link UdpChannelTransport}s using whatever means provides the lowest latency.
 */
public final class DataTransportPoller extends UdpTransportPoller
{
    private final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(
        Configuration.MAX_UDP_PAYLOAD_LENGTH, CACHE_LINE_LENGTH);
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
    private final DataHeaderFlyweight dataMessage = new DataHeaderFlyweight(unsafeBuffer);
    private final SetupFlyweight setupMessage = new SetupFlyweight(unsafeBuffer);
    private final RttMeasurementFlyweight rttMeasurement = new RttMeasurementFlyweight(unsafeBuffer);
    private final Consumer<SelectionKey> selectorPoller =
        (selectionKey) -> poll((ChannelAndTransport)selectionKey.attachment());
    private final ArrayList<ChannelAndTransport> channelAndTransports = new ArrayList<>();
    private int totalBytesReceived;

    /**
     * Construct a new {@link TransportPoller} with an {@link ErrorHandler} for logging.
     *
     * @param errorHandler which can be used to log errors and continue.
     */
    public DataTransportPoller(final ErrorHandler errorHandler)
    {
        super(errorHandler);
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        for (final ChannelAndTransport transport : channelAndTransports)
        {
            final ReceiveChannelEndpoint receiveChannelEndpoint = transport.channelEndpoint;
            cancelSingle(transport);
            receiveChannelEndpoint.close();
        }
        super.close();
    }

    /**
     * {@inheritDoc}
     */
    public int pollTransports()
    {
        totalBytesReceived = 0;

        if (channelAndTransports.size() <= ITERATION_THRESHOLD)
        {
            for (final ChannelAndTransport channelAndTransport : channelAndTransports)
            {
                poll(channelAndTransport);
            }
        }
        else
        {
            try
            {
                selector.selectNow(selectorPoller);
            }
            catch (final IOException ex)
            {
                errorHandler.onError(ex);
            }
        }

        return totalBytesReceived;
    }

    /**
     * Register transport for reading with the poller.
     *
     * @param channelEndpoint to which the transport belongs.
     * @param transport       new transport to be registered.
     * @param transportIndex  for the transport in the channel.
     */
    public void registerForRead(
        final ReceiveChannelEndpoint channelEndpoint, final UdpChannelTransport transport, final int transportIndex)
    {
        try
        {
            final ChannelAndTransport channelAndTransport = new ChannelAndTransport(
                channelEndpoint, transport, transportIndex);

            channelAndTransport.selectionKey = transport.receiveDatagramChannel()
                .register(selector, SelectionKey.OP_READ, channelAndTransport);
            channelAndTransports.add(channelAndTransport);
        }
        catch (final ClosedChannelException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    /**
     * Cancel the reading of a given transport.
     *
     * @param channelEndpoint to which the transport belongs.
     * @param transport       transport which was previously registered.
     */
    public void cancelRead(final ReceiveChannelEndpoint channelEndpoint, final UdpChannelTransport transport)
    {
        final ArrayList<ChannelAndTransport> transports = channelAndTransports;
        for (int i = transports.size() - 1; i >= 0; i--)
        {
            final ChannelAndTransport channelAndTransport = transports.get(i);
            if (channelEndpoint == channelAndTransport.channelEndpoint && transport == channelAndTransport.transport)
            {
                cancelSingle(channelAndTransport);
                ArrayListUtil.fastUnorderedRemove(transports, i);
                break;
            }
        }
    }

    /**
     * Cancel all reads of a given channel.
     *
     * @param channelEndpoint to which the transport belongs.
     */
    public void cancelReadForAllTransports(final ReceiveChannelEndpoint channelEndpoint)
    {
        final ArrayList<ChannelAndTransport> transports = channelAndTransports;
        for (int i = transports.size() - 1; i >= 0; i--)
        {
            final ChannelAndTransport transport = transports.get(i);
            if (channelEndpoint == transport.channelEndpoint)
            {
                transport.selectionKey.cancel();
                ArrayListUtil.fastUnorderedRemove(transports, i);
            }
        }

        selectNowWithoutProcessing();
    }

    private void cancelSingle(final ChannelAndTransport transport)
    {
        transport.selectionKey.cancel();
        selectNowWithoutProcessing();
    }

    /**
     * Check if any of the registered channels or transports require re-resolution.
     *
     * @param nowNs          as the current time.
     * @param conductorProxy for sending re-resolution requests.
     */
    public void checkForReResolutions(final long nowNs, final DriverConductorProxy conductorProxy)
    {
        for (final ChannelAndTransport channelAndTransport : channelAndTransports)
        {
            channelAndTransport.channelEndpoint.checkForReResolution(nowNs, conductorProxy);
        }
    }

    private void poll(final ChannelAndTransport channelAndTransport)
    {
        try
        {
            receive(channelAndTransport);
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
        }
    }

    private void receive(final ChannelAndTransport channelAndTransport)
    {
        final InetSocketAddress srcAddress = channelAndTransport.transport.receive(byteBuffer);

        if (null != srcAddress)
        {
            final int length = byteBuffer.position();
            totalBytesReceived += length;
            final ReceiveChannelEndpoint channelEndpoint = channelAndTransport.channelEndpoint;

            if (channelEndpoint.isValidFrame(unsafeBuffer, length))
            {
                channelEndpoint.receiveHook(unsafeBuffer, length, srcAddress);

                final int frameType = frameType(unsafeBuffer, 0);
                if (HDR_TYPE_DATA == frameType || HDR_TYPE_PAD == frameType)
                {
                    channelEndpoint.onDataPacket(
                        dataMessage, unsafeBuffer, length, srcAddress, channelAndTransport.transportIndex);
                }
                else if (HDR_TYPE_SETUP == frameType)
                {
                    channelEndpoint.onSetupMessage(
                        setupMessage, unsafeBuffer, length, srcAddress, channelAndTransport.transportIndex);
                }
                else if (HDR_TYPE_RTTM == frameType)
                {
                    channelEndpoint.onRttMeasurement(
                        rttMeasurement, unsafeBuffer, length, srcAddress, channelAndTransport.transportIndex);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "DataTransportPoller{}";
    }

    static class ChannelAndTransport
    {
        final ReceiveChannelEndpoint channelEndpoint;
        final UdpChannelTransport transport;
        final int transportIndex;
        SelectionKey selectionKey;

        ChannelAndTransport(
            final ReceiveChannelEndpoint channelEndpoint, final UdpChannelTransport transport, final int transportIndex)
        {
            this.channelEndpoint = channelEndpoint;
            this.transport = transport;
            this.transportIndex = transportIndex;
        }
    }
}
