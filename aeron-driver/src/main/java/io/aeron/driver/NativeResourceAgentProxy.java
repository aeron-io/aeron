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

import io.aeron.ChannelUri;
import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetSocketAddress;

final class NativeResourceAgentProxy extends CommandProxy
{
    private NativeResourceAgent nativeResourceAgent;

    NativeResourceAgentProxy(
        final OneToOneConcurrentArrayQueue<Runnable> commandQueue,
        final AtomicCounter failCount)
    {
        super(commandQueue, failCount);
    }

    void nativeResourceAgent(final NativeResourceAgent nativeResourceAgent)
    {
        this.nativeResourceAgent = nativeResourceAgent;
    }

    CommandResult<UdpChannel> parseChannel(final String channel, final boolean isDestination)
    {
        final CommandResult<UdpChannel> result = new CommandResult<>();
        offer(() -> nativeResourceAgent.onParseChannel(result, channel, isDestination));
        return result;
    }

    CommandResult<InetSocketAddress> reResolveAddress(final String endpoint, final String uriParamName)
    {
        final CommandResult<InetSocketAddress> result = new CommandResult<>();
        offer(() -> nativeResourceAgent.onReResolveAddress(result, endpoint, uriParamName));
        return result;
    }

    CommandResult<InetSocketAddress> destinationAddress(final ChannelUri channel)
    {
        final CommandResult<InetSocketAddress> result = new CommandResult<>();
        offer(() -> nativeResourceAgent.onDestinationAddress(result, channel));
        return result;
    }

    void freeLogBuffer(final RawLog rawLog)
    {
        offer(() -> nativeResourceAgent.onFreeLogBuffer(rawLog));
    }

    CommandResult<RawLog> newNetworkPublicationLog(
        final boolean isExclusive,
        final int streamId,
        final long registrationId,
        final int socketRcvBufLength,
        final int socketSndBufLength,
        final PublicationParams params,
        final boolean hasGroupSemantics)
    {
        final CommandResult<RawLog> result = new CommandResult<>();
        offer(() -> nativeResourceAgent.onNewNetworkPublicationLog(
            result,
            isExclusive,
            streamId,
            registrationId,
            socketRcvBufLength,
            socketSndBufLength,
            params,
            hasGroupSemantics));
        return result;
    }

    CommandResult<RawLog> newIpcPublicationLog(
        final boolean isExclusive,
        final int streamId,
        final long registrationId,
        final PublicationParams params)
    {
        final CommandResult<RawLog> result = new CommandResult<>();
        offer(() -> nativeResourceAgent.onNewIpcPublicationLog(result, isExclusive, streamId, registrationId, params));
        return result;
    }

    CommandResult<RawLog> newPublicationImageLog(
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int termOffset,
        final int termBufferLength,
        final int senderMtuLength,
        final ReceiveChannelEndpoint channelEndpoint,
        final long registrationId,
        final SubscriptionParams params,
        final boolean sparse,
        final boolean reliable,
        final boolean groupSemantics)
    {
        final CommandResult<RawLog> result = new CommandResult<>();
        offer(() -> nativeResourceAgent.onNewPublicationImageLog(
            result,
            sessionId,
            streamId,
            initialTermId,
            termOffset,
            termBufferLength,
            senderMtuLength,
            channelEndpoint,
            registrationId,
            params,
            sparse,
            reliable,
            groupSemantics));
        return result;
    }
}
