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
import io.aeron.protocol.ErrorFlyweight;
import io.aeron.protocol.NakFlyweight;
import io.aeron.protocol.ResponseSetupFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
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
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_ERR;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_NAK;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_RSP_SETUP;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_RTTM;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_SM;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;

/**
 * Encapsulates the polling of control {@link UdpChannelTransport}s using whatever means provides the lowest latency.
 */
public final class ControlTransportPoller extends UdpTransportPoller
{
    private final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(
        Configuration.MAX_UDP_PAYLOAD_LENGTH, CACHE_LINE_LENGTH);
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
    private final NakFlyweight nakMessage = new NakFlyweight(unsafeBuffer);
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight(unsafeBuffer);
    private final RttMeasurementFlyweight rttMeasurement = new RttMeasurementFlyweight(unsafeBuffer);
    private final ResponseSetupFlyweight responseSetup = new ResponseSetupFlyweight(unsafeBuffer);
    private final ErrorFlyweight error = new ErrorFlyweight(unsafeBuffer);
    private final DriverConductorProxy conductorProxy;
    private final Consumer<SelectionKey> selectorPoller =
        (selectionKey) -> poll((SendChannelEndpoint)selectionKey.attachment());
    private final ArrayList<Transport> transports = new ArrayList<>();
    private int totalBytesReceived;

    /**
     * Construct a new {@link TransportPoller} with an {@link ErrorHandler} for logging.
     *
     * @param errorHandler   which can be used to log errors and continue.
     * @param conductorProxy to send message back to the conductor.
     */
    public ControlTransportPoller(final ErrorHandler errorHandler, final DriverConductorProxy conductorProxy)
    {
        super(errorHandler);
        this.conductorProxy = conductorProxy;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        for (final Transport transport : transports)
        {
            cancelSingleKey(transport.selectionKey);
            transport.sendChannelEndpoint.close();
        }
        super.close();
    }

    /**
     * {@inheritDoc}
     */
    public int pollTransports()
    {
        totalBytesReceived = 0;

        if (transports.size() <= ITERATION_THRESHOLD)
        {
            for (final Transport transport : transports)
            {
                poll(transport.sendChannelEndpoint);
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
     * Register channel for read.
     *
     * @param sendChannelEndpoint to associate with read.
     */
    public void registerForRead(final SendChannelEndpoint sendChannelEndpoint)
    {
        try
        {
            final SelectionKey key = sendChannelEndpoint.receiveDatagramChannel()
                .register(selector, SelectionKey.OP_READ, sendChannelEndpoint);
            transports.add(new Transport(sendChannelEndpoint, key));
        }
        catch (final ClosedChannelException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    /**
     * Cancel a previous read registration.
     *
     * @param sendChannelEndpoint to be canceled and removed.
     */
    public void cancelRead(final SendChannelEndpoint sendChannelEndpoint)
    {
        for (int i = transports.size() - 1; i >= 0; i--)
        {
            final Transport transport = transports.get(i);
            if (sendChannelEndpoint == transport.sendChannelEndpoint)
            {
                cancelSingleKey(transport.selectionKey);
                ArrayListUtil.fastUnorderedRemove(transports, i);
                break;
            }
        }
    }

    /**
     * Check if any of the registered channels require re-resolution.
     *
     * @param nowNs          as the current time.
     * @param conductorProxy for sending re-resolution requests.
     */
    public void checkForReResolutions(final long nowNs, final DriverConductorProxy conductorProxy)
    {
        for (final Transport transport : transports)
        {
            transport.sendChannelEndpoint.checkForReResolution(nowNs, conductorProxy);
        }
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "ControlTransportPoller{}";
    }

    private void poll(final SendChannelEndpoint channelEndpoint)
    {
        try
        {
            receive(channelEndpoint);
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
        }
    }

    private void receive(final SendChannelEndpoint channelEndpoint)
    {
        final InetSocketAddress srcAddress = channelEndpoint.receive(byteBuffer);

        if (null != srcAddress)
        {
            final int length = byteBuffer.position();
            totalBytesReceived += length;
            if (channelEndpoint.isValidFrame(unsafeBuffer, length))
            {
                channelEndpoint.receiveHook(unsafeBuffer, length, srcAddress);

                final int frameType = frameType(unsafeBuffer, 0);
                if (HDR_TYPE_NAK == frameType)
                {
                    channelEndpoint.onNakMessage(nakMessage, unsafeBuffer, length, srcAddress);
                }
                else if (HDR_TYPE_SM == frameType)
                {
                    channelEndpoint.onStatusMessage(
                        statusMessage, unsafeBuffer, length, srcAddress, conductorProxy);
                }
                else if (HDR_TYPE_ERR == frameType)
                {
                    channelEndpoint.onError(
                        error, unsafeBuffer, length, srcAddress, conductorProxy);
                }
                else if (HDR_TYPE_RTTM == frameType)
                {
                    channelEndpoint.onRttMeasurement(rttMeasurement, unsafeBuffer, length, srcAddress);
                }
                else if (HDR_TYPE_RSP_SETUP == frameType)
                {
                    channelEndpoint.onResponseSetup(
                        responseSetup, unsafeBuffer, length, srcAddress, conductorProxy);
                }
            }
        }
    }

    private void cancelSingleKey(final SelectionKey selectionKey)
    {
        selectionKey.cancel();
        selectNowWithoutProcessing();
    }

    private record Transport(SendChannelEndpoint sendChannelEndpoint, SelectionKey selectionKey)
    {
    }
}
