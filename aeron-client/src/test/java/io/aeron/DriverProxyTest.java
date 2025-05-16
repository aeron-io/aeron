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
package io.aeron;

import io.aeron.command.RemovePublicationFlyweight;
import io.aeron.command.RemoveSubscriptionFlyweight;
import org.junit.jupiter.api.Test;
import io.aeron.command.PublicationMessageFlyweight;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;

import static io.aeron.command.ControlProtocolEvents.*;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DriverProxyTest
{
    public static final String CHANNEL = "aeron:udp?interface=localhost:40123|endpoint=localhost:40124";

    private static final int STREAM_ID = 1001;
    private static final long CORRELATION_ID = 3;
    private static final long CLIENT_ID = 7;
    private final RingBuffer conductorBuffer = new ManyToOneRingBuffer(
        new UnsafeBuffer(ByteBuffer.allocateDirect(TRAILER_LENGTH + 1024)));
    private final DriverProxy conductor = new DriverProxy(conductorBuffer, CLIENT_ID);

    @Test
    void threadSendsAddChannelMessage()
    {
        threadSendsChannelMessage(() -> conductor.addPublication(CHANNEL, STREAM_ID), ADD_PUBLICATION);
    }

    @Test
    void threadSendsRemoveChannelMessage()
    {
        conductor.removePublication(CORRELATION_ID, false);
        assertReadsOneMessage(
            (msgTypeId, buffer, index, length) ->
            {
                final RemovePublicationFlyweight message = new RemovePublicationFlyweight();
                message.wrap(buffer, index);

                assertEquals(REMOVE_PUBLICATION, msgTypeId);
                assertEquals(CORRELATION_ID, message.registrationId());
            }
        );
    }

    @Test
    void threadSendsRemoveSubscriberMessage()
    {
        conductor.removeSubscription(CORRELATION_ID);

        assertReadsOneMessage(
            (msgTypeId, buffer, index, length) ->
            {
                final RemoveSubscriptionFlyweight removeMessage = new RemoveSubscriptionFlyweight();
                removeMessage.wrap(buffer, index);

                assertEquals(REMOVE_SUBSCRIPTION, msgTypeId);
                assertEquals(CORRELATION_ID, removeMessage.registrationId());
            }
        );
    }

    private void threadSendsChannelMessage(final Runnable sendMessage, final int expectedMsgTypeId)
    {
        sendMessage.run();

        assertReadsOneMessage(
            (msgTypeId, buffer, index, length) ->
            {
                final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
                publicationMessage.wrap(buffer, index);

                assertEquals(expectedMsgTypeId, msgTypeId);
                assertEquals(CHANNEL, publicationMessage.channel());
                assertEquals(STREAM_ID, publicationMessage.streamId());
            }
        );
    }

    private void assertReadsOneMessage(final MessageHandler handler)
    {
        final int messageCount = conductorBuffer.read(handler);
        assertEquals(1, messageCount);
    }
}
