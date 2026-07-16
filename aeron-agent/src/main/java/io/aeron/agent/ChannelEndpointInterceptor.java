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
package io.aeron.agent;

import io.aeron.driver.media.ImageConnection;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.protocol.NakFlyweight;
import net.bytebuddy.asm.Advice;
import org.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.agent.DriverEventCode.*;
import static io.aeron.agent.DriverEventLogger.LOGGER;

class ChannelEndpointInterceptor
{
    static class SenderProxy
    {
        static class RegisterSendChannelEndpoint
        {
            @Advice.OnMethodEnter
            static void registerSendChannelEndpoint(final SendChannelEndpoint channelEndpoint)
            {
                LOGGER.logString(SEND_CHANNEL_CREATION, channelEndpoint.udpChannel().description());
            }
        }

        static class CloseSendChannelEndpoint
        {
            @Advice.OnMethodEnter
            static void closeSendChannelEndpoint(final SendChannelEndpoint channelEndpoint)
            {
                LOGGER.logString(SEND_CHANNEL_CLOSE, channelEndpoint.udpChannel().description());
            }
        }
    }

    static class ReceiverProxy
    {
        static class RegisterReceiveChannelEndpoint
        {
            @Advice.OnMethodEnter
            static void registerReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
            {
                LOGGER.logString(RECEIVE_CHANNEL_CREATION, channelEndpoint.udpChannel().description());
            }
        }

        static class CloseReceiveChannelEndpoint
        {
            @Advice.OnMethodEnter
            static void closeReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
            {
                LOGGER.logString(RECEIVE_CHANNEL_CLOSE, channelEndpoint.udpChannel().description());
            }
        }
    }

    static class UdpChannelTransport
    {
        static class SendHook
        {
            @Advice.OnMethodEnter
            static void sendHook(final ByteBuffer buffer, final InetSocketAddress address)
            {
                LOGGER.logFrameOut(buffer, address);
            }
        }

        static class ReceiveHook
        {
            @Advice.OnMethodEnter
            static void receiveHook(final UnsafeBuffer buffer, final int length, final InetSocketAddress address)
            {
                LOGGER.logFrameIn(buffer, 0, length, address);
            }
        }

        static class ResendHook
        {
            @Advice.OnMethodEnter
            static void resendHook(
                final int sessionId,
                final int streamId,
                final int termId,
                final int termOffset,
                final int length,
                @Advice.This final Object thisObject)
            {
                final io.aeron.driver.media.UdpChannelTransport transport =
                    (io.aeron.driver.media.UdpChannelTransport)thisObject;
                LOGGER.logResend(
                    sessionId, streamId, termId, termOffset, length, transport.udpChannel().originalUriString());
            }
        }
    }

    static class ReceiveChannelEndpointInterceptor
    {
        static class NakSent
        {
            @Advice.OnMethodEnter
            static void sendNakMessage(
                final ImageConnection[] connections,
                final int sessionId,
                final int streamId,
                final int termId,
                final int termOffset,
                final int length,
                @Advice.This final Object thisObject)
            {
                if (null == connections || null == thisObject)
                {
                    return;
                }

                final ReceiveChannelEndpoint endpoint = (ReceiveChannelEndpoint)thisObject;
                final String channel = endpoint.originalUriString();
                for (final ImageConnection connection : connections)
                {
                    if (null != connection)
                    {
                        LOGGER.logNakMessage(
                            NAK_SENT,
                            connection.controlAddress,
                            sessionId,
                            streamId,
                            termId,
                            termOffset,
                            length,
                            channel);
                    }
                }
            }
        }
    }

    static class SendChannelEndpointInterceptor
    {
        static class NakReceived
        {
            @Advice.OnMethodEnter
            static void onNakMessage(
                final NakFlyweight msg,
                final UnsafeBuffer buffer,
                final int length,
                final InetSocketAddress srcAddress,
                @Advice.This final Object thisObject)
            {
                if (null == thisObject)
                {
                    return;
                }

                final String channel = ((SendChannelEndpoint)thisObject).originalUriString();
                LOGGER.logNakMessage(
                    NAK_RECEIVED,
                    srcAddress,
                    msg.sessionId(),
                    msg.streamId(),
                    msg.termId(),
                    msg.termOffset(),
                    msg.length(),
                    channel);
            }
        }
    }
}
