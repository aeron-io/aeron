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

import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.ReceiveDestinationTransport;
import io.aeron.driver.media.UdpChannel;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetSocketAddress;

/**
 * Proxy for offering into the {@link Receiver} Thread's command queue.
 */
final class ReceiverProxy extends CommandProxy
{
    private Receiver receiver;

    ReceiverProxy(final OneToOneConcurrentArrayQueue<Runnable> commandQueue, final AtomicCounter failCount)
    {
        super(commandQueue, failCount);
    }

    void receiver(final Receiver receiver)
    {
        this.receiver = receiver;
    }

    Receiver receiver()
    {
        return receiver;
    }

    void addSubscription(final ReceiveChannelEndpoint mediaEndpoint, final int streamId)
    {
        offer(() -> receiver.onAddSubscription(mediaEndpoint, streamId));
    }

    void addSubscription(final ReceiveChannelEndpoint mediaEndpoint, final int streamId, final int sessionId)
    {
        offer(() -> receiver.onAddSubscription(mediaEndpoint, streamId, sessionId));
    }

    void removeSubscription(final ReceiveChannelEndpoint mediaEndpoint, final int streamId)
    {
        offer(() -> receiver.onRemoveSubscription(mediaEndpoint, streamId));
    }

    void removeSubscription(final ReceiveChannelEndpoint mediaEndpoint, final int streamId, final int sessionId)
    {
        offer(() -> receiver.onRemoveSubscription(mediaEndpoint, streamId, sessionId));
    }

    void newPublicationImage(final ReceiveChannelEndpoint channelEndpoint, final PublicationImage image)
    {
        offer(() -> receiver.onNewPublicationImage(channelEndpoint, image));
    }

    void registerReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        offer(() -> receiver.onRegisterReceiveChannelEndpoint(channelEndpoint));
    }

    void closeReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        offer(() -> receiver.onCloseReceiveChannelEndpoint(channelEndpoint));
    }

    void removeCoolDown(final ReceiveChannelEndpoint channelEndpoint, final int sessionId, final int streamId)
    {
        offer(() -> receiver.onRemoveCoolDown(channelEndpoint, sessionId, streamId));
    }

    void addDestination(final ReceiveChannelEndpoint channelEndpoint, final ReceiveDestinationTransport transport)
    {
        offer(() -> receiver.onAddDestination(channelEndpoint, transport));
    }

    void removeDestination(final ReceiveChannelEndpoint channelEndpoint, final UdpChannel udpChannel)
    {
        offer(() -> receiver.onRemoveDestination(channelEndpoint, udpChannel));
    }

    void onResolutionChange(
        final ReceiveChannelEndpoint channelEndpoint, final UdpChannel udpChannel, final InetSocketAddress newAddress)
    {
        offer(() -> receiver.onResolutionChange(channelEndpoint, udpChannel, newAddress));
    }

    void requestSetup(
        final ReceiveChannelEndpoint channelEndpoint,
        final int streamId,
        final int sessionId)
    {
        offer(() -> receiver.onRequestSetup(channelEndpoint, streamId, sessionId));
    }

    void rejectImage(final long imageCorrelationId, final long position, final String reason)
    {
        offer(() -> receiver.onRejectImage(imageCorrelationId, position, reason));
    }
}
