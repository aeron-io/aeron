/*
 * Copyright 2014-2026 Real Logic Limited.
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

package io.aeron.driver.logging;

import io.aeron.ErrorCode;
import io.aeron.command.*;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.ByteBuffer;

import static io.aeron.driver.logging.DriverEventCode.*;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.agrona.PrintBufferUtil.prettyHexDump;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DriverAdminCommandBinaryRendererTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[512]);
    private final StringBuilder sb = new StringBuilder();
    private final DriverAdminCommandBinaryRenderer renderer = new DriverAdminCommandBinaryRenderer();

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_ADD_PUBLICATION", "CMD_IN_ADD_EXCLUSIVE_PUBLICATION" })
    void renderCommandPublication(final DriverEventCode eventCode)
    {
        final PublicationMessageFlyweight flyweight = new PublicationMessageFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.channel("pub channel");
        flyweight.streamId(3);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(15);

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "streamId=3 clientId=" + eventCode.id() + " correlationId=15 channel=pub channel",
            sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_ADD_SUBSCRIPTION" })
    void renderCommandSubscription(final DriverEventCode eventCode)
    {
        final SubscriptionMessageFlyweight flyweight = new SubscriptionMessageFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.channel("sub channel");
        flyweight.streamId(31);
        flyweight.registrationCorrelationId(90);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(6);

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "streamId=31 registrationCorrelationId=90 clientId=" + eventCode.id() + " correlationId=6 " +
            "channel=sub channel",
            sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_REMOVE_COUNTER" })
    void renderCommandRemoveCounterEvent(final DriverEventCode eventCode)
    {
        final RemoveCounterFlyweight flyweight = new RemoveCounterFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.registrationId(11);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(16);

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "registrationId=11 clientId=" + eventCode.id() + " correlationId=16",
            sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_REMOVE_PUBLICATION" })
    void renderCommandRemovePublicationEvent(final DriverEventCode eventCode)
    {
        final RemovePublicationFlyweight flyweight = new RemovePublicationFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.registrationId(11);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(16);
        flyweight.revoke(true);

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, RemovePublicationFlyweight.length());

        assertEquals(
            "registrationId=11 clientId=" + eventCode.id() + " correlationId=16 revoke=true",
            sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_REMOVE_PUBLICATION" })
    void renderOldShortCommandRemovePublicationEvent(final DriverEventCode eventCode)
    {
        // A removal command from an old client will be shorter and won't have the flags field.
        // The renderer should notice the shorter length and act appropriately.
        final RemovePublicationFlyweight flyweight = new RemovePublicationFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.registrationId(11);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(16);
        flyweight.revoke(true);

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, 24);

        assertEquals(
            "registrationId=11 clientId=" + eventCode.id() + " correlationId=16",
            sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_REMOVE_SUBSCRIPTION" })
    void renderCommandRemoveSubscriptionEvent(final DriverEventCode eventCode)
    {
        final RemoveSubscriptionFlyweight flyweight = new RemoveSubscriptionFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.registrationId(11);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(16);

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "registrationId=11 clientId=" + eventCode.id() + " correlationId=16",
            sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class,
        names = { "CMD_OUT_PUBLICATION_READY", "CMD_OUT_EXCLUSIVE_PUBLICATION_READY" })
    void renderCommandPublicationReady(final DriverEventCode eventCode)
    {
        final PublicationBuffersReadyFlyweight flyweight = new PublicationBuffersReadyFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.sessionId(eventCode.ordinal());
        flyweight.streamId(-24);
        flyweight.publicationLimitCounterId(1);
        flyweight.channelStatusCounterId(5);
        flyweight.correlationId(8);
        flyweight.registrationId(eventCode.id());
        flyweight.logFileName("log.txt");

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "sessionId=" + eventCode.ordinal() + " streamId=-24 publicationLimitCounterId=1 " +
            "channelStatusCounterId=5 correlationId=8 registrationId=" + eventCode.id() + " logFileName=log.txt",
            sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_AVAILABLE_IMAGE" })
    void renderCommandImageReady(final DriverEventCode eventCode)
    {
        final ImageBuffersReadyFlyweight flyweight = new ImageBuffersReadyFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.sessionId(eventCode.ordinal());
        flyweight.streamId(22);
        flyweight.subscriberPositionId(0);
        flyweight.subscriptionRegistrationId(245);
        flyweight.correlationId(767);
        flyweight.logFileName("log2.txt");
        flyweight.sourceIdentity("source identity");

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "sessionId=" + eventCode.ordinal() + " streamId=22 subscriberPositionId=0 " +
            "subscriptionRegistrationId=245 correlationId=767 sourceIdentity=source identity " +
            "logFileName=log2.txt",
            sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_ON_OPERATION_SUCCESS" })
    void renderCommandOperationSuccess(final DriverEventCode eventCode)
    {
        final OperationSucceededFlyweight flyweight = new OperationSucceededFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.correlationId(eventCode.id());

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("correlationId=" + eventCode.id(), sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_KEEPALIVE_CLIENT", "CMD_IN_CLIENT_CLOSE" })
    void renderCommandCorrelationEvent(final DriverEventCode eventCode)
    {
        final CorrelatedMessageFlyweight flyweight = new CorrelatedMessageFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(2);

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("clientId=" + eventCode.id() + " correlationId=2", sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_ON_UNAVAILABLE_IMAGE" })
    void renderCommandImage(final DriverEventCode eventCode)
    {
        final ImageMessageFlyweight flyweight = new ImageMessageFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.streamId(300);
        flyweight.correlationId(eventCode.id());
        flyweight.subscriptionRegistrationId(-19);
        flyweight.channel("the channel");

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "streamId=300 correlationId=" + eventCode.id() + " subscriptionRegistrationId=-19 " +
            "channel=the channel",
            sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = {
        "CMD_IN_ADD_DESTINATION",
        "CMD_IN_REMOVE_DESTINATION",
        "CMD_IN_ADD_RCV_DESTINATION",
        "CMD_IN_REMOVE_RCV_DESTINATION" })
    void renderCommandDestination(final DriverEventCode eventCode)
    {
        final DestinationMessageFlyweight flyweight = new DestinationMessageFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.channel("dst");
        flyweight.registrationCorrelationId(eventCode.id());
        flyweight.clientId(1010101);
        flyweight.correlationId(404);

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "registrationCorrelationId=" + eventCode.id() + " clientId=1010101 correlationId=404 channel=dst",
            sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_ERROR" })
    void renderCommandError(final DriverEventCode eventCode)
    {
        final ErrorResponseFlyweight flyweight = new ErrorResponseFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.offendingCommandCorrelationId(eventCode.id());
        flyweight.errorCode(ErrorCode.MALFORMED_COMMAND);
        flyweight.errorMessage("Huge stacktrace!");

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "offendingCommandCorrelationId=" + eventCode.id() + " errorCode=" + ErrorCode.MALFORMED_COMMAND +
            " message=Huge stacktrace!",
            sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_ADD_COUNTER" })
    void renderCommandCounter(final DriverEventCode eventCode)
    {
        final CounterMessageFlyweight flyweight = new CounterMessageFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.typeId(3);
        flyweight.keyBuffer(newBuffer(new byte[20]), 0, 10);
        flyweight.labelBuffer(newBuffer(new byte[100]), 26, 13);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(42);

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "typeId=3 keyBufferOffset=" + flyweight.keyBufferOffset() + " keyBufferLength=10 labelBufferOffset=" +
            flyweight.labelBufferOffset() + " labelBufferLength=13 clientId=" + eventCode.id() +
            " correlationId=42",
            sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_ADD_COUNTER" })
    void renderCommandCounterWithRawBytes(final DriverEventCode eventCode)
    {
        final DriverAdminCommandBinaryRenderer rawBytesRenderer = new DriverAdminCommandBinaryRenderer(true);
        final byte[] keyBytes = "0123456789".getBytes(US_ASCII);
        final byte[] labelBytes = "counter label".getBytes(US_ASCII);
        final CounterMessageFlyweight flyweight = new CounterMessageFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.typeId(3);
        flyweight.keyBuffer(newBuffer(keyBytes), 0, keyBytes.length);
        flyweight.labelBuffer(newBuffer(labelBytes), 0, labelBytes.length);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(42);

        rawBytesRenderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        final String expected = String.format(
            "typeId=3 keyBufferOffset=%d keyBufferLength=%d labelBufferOffset=%d labelBufferLength=%d clientId=%d " +
            "correlationId=42 keyBuffer=%n%s" + " label=%n%s",
            flyweight.keyBufferOffset(),
            keyBytes.length,
            flyweight.labelBufferOffset(),
            labelBytes.length,
            eventCode.id(),
            prettyHexDump(newBuffer(keyBytes)),
            prettyHexDump(newBuffer(labelBytes)));

        assertEquals(expected, sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_SUBSCRIPTION_READY" })
    void renderCommandSubscriptionReady(final DriverEventCode eventCode)
    {
        final SubscriptionReadyFlyweight flyweight = new SubscriptionReadyFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.correlationId(42);
        flyweight.channelStatusCounterId(eventCode.id());

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("correlationId=42 channelStatusCounterId=" + eventCode.id(), sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_COUNTER_READY", "CMD_OUT_ON_UNAVAILABLE_COUNTER" })
    void renderCommandCounterUpdate(final DriverEventCode eventCode)
    {
        final CounterUpdateFlyweight flyweight = new CounterUpdateFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.correlationId(eventCode.id());
        flyweight.counterId(18);

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("correlationId=" + eventCode.id() + " counterId=18", sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_ON_CLIENT_TIMEOUT" })
    void renderCommandClientTimeout(final DriverEventCode eventCode)
    {
        final ClientTimeoutFlyweight flyweight = new ClientTimeoutFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.clientId(eventCode.id());

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("clientId=" + eventCode.id(), sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_TERMINATE_DRIVER" })
    void renderCommandTerminateDriver(final DriverEventCode eventCode)
    {
        final TerminateDriverFlyweight flyweight = new TerminateDriverFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.clientId(eventCode.id());
        flyweight.tokenBuffer(newBuffer(new byte[15]), 4, 11);

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("clientId=" + eventCode.id() + " tokenBufferLength=11", sb.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_TERMINATE_DRIVER" })
    void renderCommandTerminateDriverWithRawBytes(final DriverEventCode eventCode)
    {
        final DriverAdminCommandBinaryRenderer rawBytesRenderer = new DriverAdminCommandBinaryRenderer(true);
        final byte[] tokenSource = "xxxxTOKEN-BYTES".getBytes(US_ASCII);
        final byte[] tokenBytes = "TOKEN-BYTES".getBytes(US_ASCII);
        final TerminateDriverFlyweight flyweight = new TerminateDriverFlyweight();
        flyweight.wrap(buffer, 0);
        flyweight.clientId(eventCode.id());
        flyweight.tokenBuffer(newBuffer(tokenSource), 4, tokenBytes.length);

        rawBytesRenderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "clientId=" + eventCode.id() + " tokenBufferLength=" + tokenBytes.length +
            " tokenBuffer=" + prettyHexDump(newBuffer(tokenBytes)),
            sb.toString());
    }

    @Test
    void renderCommandUnknown()
    {
        final DriverEventCode eventCode = SEND_CHANNEL_CREATION;

        renderer.append(sb, eventCode.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("COMMAND_UNKNOWN: " + eventCode, sb.toString());
    }

    private DirectBuffer newBuffer(final byte[] bytes)
    {
        final UnsafeBuffer buf = new UnsafeBuffer(ByteBuffer.allocate(bytes.length));
        buf.putBytes(0, bytes);
        return buf;
    }
}
