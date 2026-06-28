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

import io.aeron.api.InternalApi;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.ReceiveDestinationTransport;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import java.net.InetSocketAddress;

/**
 * Proxy for sending commands to the {@link DriverConductor}.
 */
@InternalApi
public final class DriverConductorProxy
{
    private DriverConductor driverConductor;
    private final ManyToOneConcurrentLinkedQueue<Runnable> commandQueue;

    DriverConductorProxy(final ManyToOneConcurrentLinkedQueue<Runnable> commandQueue)
    {
        this.commandQueue = commandQueue;
    }

    /**
     * Request the conductor re-resolve an endpoint address.
     *
     * @param endpoint        in string format.
     * @param channelEndpoint that the endpoint belongs to.
     * @param address         of previous resolution.
     */
    public void reResolveEndpoint(
        final String endpoint, final SendChannelEndpoint channelEndpoint, final InetSocketAddress address)
    {
        offer(() -> driverConductor.onReResolveEndpoint(endpoint, channelEndpoint, address));
    }

    /**
     * Re-resolve a control endpoint for a channel.
     *
     * @param endpoint        to be re-resolved.
     * @param udpChannel      which contained the endpoint.
     * @param channelEndpoint to which the endpoint belongs.
     * @param address         of previous resolution.
     */
    public void reResolveControl(
        final String endpoint,
        final UdpChannel udpChannel,
        final ReceiveChannelEndpoint channelEndpoint,
        final InetSocketAddress address)
    {
        offer(() -> driverConductor.onReResolveControl(endpoint, udpChannel, channelEndpoint, address));
    }

    /**
     * Close a receive destination.
     *
     * @param destinationTransport to have its indicators closed.
     */
    public void closeReceiveDestination(final ReceiveDestinationTransport destinationTransport)
    {
        offer(() -> driverConductor.closeReceiveDestination(destinationTransport));
    }

    /**
     * Handle a response setup message from the remote "server".
     *
     * @param responseCorrelationId correlationId of the subscription that will handle responses.
     * @param responseSessionId     sessionId that will be associated with the incoming messages.
     */
    public void responseSetup(final long responseCorrelationId, final int responseSessionId)
    {
        offer(() -> driverConductor.responseSetup(responseCorrelationId, responseSessionId));
    }

    /**
     * Handle notify that a response channel has connected.
     *
     * @param responseCorrelationId correlationId of the subscription that will handle responses.
     */
    public void responseConnected(final long responseCorrelationId)
    {
        offer(() -> driverConductor.responseConnected(responseCorrelationId));
    }

    void receiveChannelEndpointClosed(final ReceiveChannelEndpoint channelEndpoint)
    {
        offer(() -> driverConductor.receiveChannelEndpointClosed(channelEndpoint));
    }

    void sendChannelEndpointClosed(final SendChannelEndpoint channelEndpoint)
    {
        offer(() -> driverConductor.sendChannelEndpointClosed(channelEndpoint));
    }

    void driverConductor(final DriverConductor driverConductor)
    {
        this.driverConductor = driverConductor;
    }

    void createPublicationImage(
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int activeTermId,
        final int termOffset,
        final int termLength,
        final int mtuLength,
        final int transportIndex,
        final short flags,
        final InetSocketAddress controlAddress,
        final InetSocketAddress srcAddress,
        final ReceiveChannelEndpoint channelEndpoint)
    {
        offer(() -> driverConductor.onCreatePublicationImage(
            sessionId,
            streamId,
            initialTermId,
            activeTermId,
            termOffset,
            termLength,
            mtuLength,
            transportIndex,
            flags,
            controlAddress,
            srcAddress,
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
        offer(() -> driverConductor.onPublicationError(
            registrationId,
            destinationRegistrationId,
            sessionId,
            streamId,
            receiverId,
            groupId,
            srcAddress,
            errorCode,
            errorMessage));
    }

    private void offer(final Runnable cmd)
    {
        if (!commandQueue.offer(cmd))
        {
            throw new IllegalStateException("offer failed");
        }
    }
}
