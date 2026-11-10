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

import java.net.InetSocketAddress;

import static io.aeron.driver.DataPacketDispatcher.SessionState.PENDING_SETUP_FRAME;

final class PendingSetupMessageFromSource
{
    private final int sessionId;
    private final int streamId;
    private final int transportIndex;
    private final boolean periodic;
    private final ReceiveChannelEndpoint channelEndpoint;
    private InetSocketAddress controlAddress;

    private long timeOfStatusMessageNs;

    PendingSetupMessageFromSource(
        final int sessionId,
        final int streamId,
        final int transportIndex,
        final ReceiveChannelEndpoint channelEndpoint,
        final boolean periodic,
        final InetSocketAddress controlAddress)
    {
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.transportIndex = transportIndex;
        this.channelEndpoint = channelEndpoint;
        this.periodic = periodic;
        this.controlAddress = controlAddress;
    }

    int sessionId()
    {
        return sessionId;
    }

    int streamId()
    {
        return streamId;
    }

    int transportIndex()
    {
        return transportIndex;
    }

    ReceiveChannelEndpoint channelEndpoint()
    {
        return channelEndpoint;
    }

    boolean isPeriodic()
    {
        return periodic;
    }

    boolean shouldElicitSetupMessage()
    {
        return channelEndpoint.dispatcher().shouldElicitSetupMessage();
    }

    void controlAddress(final InetSocketAddress newControlAddress)
    {
        this.controlAddress = newControlAddress;
    }

    InetSocketAddress controlAddress()
    {
        return controlAddress;
    }

    long timeOfStatusMessageNs()
    {
        return timeOfStatusMessageNs;
    }

    void timeOfStatusMessageNs(final long nowNs)
    {
        timeOfStatusMessageNs = nowNs;
    }

    void removeFromDataPacketDispatcher()
    {
        channelEndpoint.dispatcher().removeSessionInterestByState(sessionId, streamId, PENDING_SETUP_FRAME);
    }

    @Override
    public String toString()
    {
        return "PendingSetupMessageFromSource{" +
            "sessionId=" + sessionId +
            ", streamId=" + streamId +
            ", transportIndex=" + transportIndex +
            ", periodic=" + periodic +
            ", channelEndpoint=" + channelEndpoint +
            ", controlAddress=" + controlAddress +
            ", timeOfStatusMessageNs=" + timeOfStatusMessageNs +
            '}';
    }
}
