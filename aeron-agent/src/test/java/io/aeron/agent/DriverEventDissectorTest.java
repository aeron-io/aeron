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

import io.aeron.ErrorCode;
import io.aeron.command.*;
import io.aeron.protocol.*;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.DriverEventCode.*;
import static io.aeron.agent.DriverEventDissector.*;
import static io.aeron.agent.DriverEventLogger.MAX_CHANNEL_URI_LENGTH;
import static io.aeron.agent.DriverEventLogger.MAX_HOST_NAME_LENGTH;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static io.aeron.protocol.HeaderFlyweight.*;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DriverEventDissectorTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_EVENT_LENGTH]);
    private final StringBuilder builder = new StringBuilder();

    @Test
    void dissectRemovePublicationCleanup()
    {
        final int offset = 12;
        internalEncodeLogHeader(buffer, offset, 22, 88, () -> 2_500_000_000L);
        buffer.putInt(offset + LOG_HEADER_LENGTH, 42, LITTLE_ENDIAN);
        buffer.putInt(offset + LOG_HEADER_LENGTH + SIZE_OF_INT, 11, LITTLE_ENDIAN);
        buffer.putStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2, "channel uri");

        DriverEventDissector.dissectRemovePublicationCleanup(buffer, offset, builder);

        assertEquals("[2.500000000] " + CONTEXT + ": " + REMOVE_PUBLICATION_CLEANUP.name() +
            " [22/88]: sessionId=42 streamId=11 channel=channel uri",
            builder.toString());
    }

    @Test
    void dissectRemoveSubscriptionCleanup()
    {
        final int offset = 16;
        internalEncodeLogHeader(buffer, offset, 100, 100, () -> 100_000_000L);
        buffer.putInt(offset + LOG_HEADER_LENGTH, 33, LITTLE_ENDIAN);
        buffer.putLong(offset + LOG_HEADER_LENGTH + SIZE_OF_INT, 111_111_111_111L, LITTLE_ENDIAN);
        buffer.putStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_INT + SIZE_OF_LONG, "test");

        DriverEventDissector.dissectRemoveSubscriptionCleanup(buffer, offset, builder);

        assertEquals("[0.100000000] " + CONTEXT + ": " + REMOVE_SUBSCRIPTION_CLEANUP.name() +
            " [100/100]: streamId=33 id=111111111111 channel=test",
            builder.toString());
    }

    @Test
    void dissectRemoveImageCleanup()
    {
        final int offset = 32;
        internalEncodeLogHeader(buffer, offset, 66, 99, () -> 12345678900L);
        buffer.putInt(offset + LOG_HEADER_LENGTH, 77, LITTLE_ENDIAN);
        buffer.putInt(offset + LOG_HEADER_LENGTH + SIZE_OF_INT, 55, LITTLE_ENDIAN);
        buffer.putLong(offset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2, 1_000_000L, LITTLE_ENDIAN);
        buffer.putStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2 + SIZE_OF_LONG, "URI");

        DriverEventDissector.dissectRemoveImageCleanup(buffer, offset, builder);

        assertEquals("[12.345678900] " + CONTEXT + ": " + REMOVE_IMAGE_CLEANUP.name() +
            " [66/99]: sessionId=77 streamId=55 id=1000000 channel=URI",
            builder.toString());
    }

    @Test
    void dissectFrameTypePad()
    {
        internalEncodeLogHeader(buffer, 0, 5, 5, () -> 1_000_000_000);
        final int socketAddressOffset = encodeSocketAddress(
            buffer, LOG_HEADER_LENGTH, new InetSocketAddress("localhost", 8080));
        final DataHeaderFlyweight flyweight = new DataHeaderFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH + socketAddressOffset, 300);
        flyweight.headerType(HDR_TYPE_PAD);
        flyweight.flags((short)13);
        flyweight.frameLength(100);
        flyweight.sessionId(42);
        flyweight.streamId(5);
        flyweight.termId(16);
        flyweight.termOffset(1045);

        dissectFrame(FRAME_IN, buffer, 0, builder);

        assertEquals("[1.000000000] " + CONTEXT + ": " + FRAME_IN.name() + " [5/5]: " +
            "address=127.0.0.1:8080 type=PAD flags=00001101 frameLength=100 sessionId=42 streamId=5 termId=16 " +
            "termOffset=1045",
            builder.toString());
    }

    @Test
    void dissectFrameTypeData()
    {
        internalEncodeLogHeader(buffer, 0, 5, 5, () -> 1_000_000_000);
        final int socketAddressOffset = encodeSocketAddress(
            buffer, LOG_HEADER_LENGTH, new InetSocketAddress("localhost", 8888));
        final DataHeaderFlyweight flyweight = new DataHeaderFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH + socketAddressOffset, 300);
        flyweight.headerType(HDR_TYPE_DATA);
        flyweight.flags((short)23);
        flyweight.frameLength(77);
        flyweight.sessionId(12);
        flyweight.streamId(51);
        flyweight.termId(6);
        flyweight.termOffset(444);

        dissectFrame(FRAME_IN, buffer, 0, builder);

        assertEquals("[1.000000000] " + CONTEXT + ": " + FRAME_IN.name() + " [5/5]: " +
            "address=127.0.0.1:8888 type=DATA flags=00010111 frameLength=77 sessionId=12 streamId=51 termId=6 " +
            "termOffset=444",
            builder.toString());
    }

    @Test
    void dissectFrameTypeStatusMessage()
    {
        internalEncodeLogHeader(buffer, 0, 5, 5, () -> 1_000_000_000);
        final int socketAddressOffset = encodeSocketAddress(
            buffer, LOG_HEADER_LENGTH, new InetSocketAddress("localhost", 8888));
        final StatusMessageFlyweight flyweight = new StatusMessageFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH + socketAddressOffset, 300);
        flyweight.headerType(HDR_TYPE_SM);
        flyweight.flags((short)7);
        flyweight.frameLength(121);
        flyweight.sessionId(5);
        flyweight.streamId(8);
        flyweight.consumptionTermId(4);
        flyweight.consumptionTermOffset(18);
        flyweight.receiverWindowLength(2048);
        flyweight.receiverId(11);

        dissectFrame(FRAME_OUT, buffer, 0, builder);

        assertEquals("[1.000000000] " + CONTEXT + ": " + FRAME_OUT.name() + " [5/5]: " +
            "address=127.0.0.1:8888 type=SM flags=00000111 frameLength=121 sessionId=5 streamId=8 termId=4 " +
            "termOffset=18 receiverWindowLength=2048 receiverId=11",
            builder.toString());
    }

    @Test
    void dissectFrameTypeNak()
    {
        internalEncodeLogHeader(buffer, 0, 3, 3, () -> 3_000_000_000L);
        final int socketAddressOffset = encodeSocketAddress(
            buffer, LOG_HEADER_LENGTH, new InetSocketAddress("localhost", 8888));
        final NakFlyweight flyweight = new NakFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH + socketAddressOffset, 300);
        flyweight.headerType(HDR_TYPE_NAK);
        flyweight.flags((short)2);
        flyweight.frameLength(54);
        flyweight.sessionId(5);
        flyweight.streamId(8);
        flyweight.termId(20);
        flyweight.termOffset(0);
        flyweight.length(999999);

        dissectFrame(FRAME_OUT, buffer, 0, builder);

        assertEquals("[3.000000000] " + CONTEXT + ": " + FRAME_OUT.name() +
            " [3/3]: address=127.0.0.1:8888 type=NAK flags=00000010 frameLength=54 sessionId=5 streamId=8 " +
            "termId=20 termOffset=0 length=999999",
            builder.toString());
    }

    @Test
    void dissectFrameTypeSetup()
    {
        internalEncodeLogHeader(buffer, 0, 3, 3, () -> 3_000_000_000L);
        final int socketAddressOffset = encodeSocketAddress(
            buffer, LOG_HEADER_LENGTH, new InetSocketAddress("localhost", 8888));
        final SetupFlyweight flyweight = new SetupFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH + socketAddressOffset, 300);
        flyweight.headerType(HDR_TYPE_SETUP);
        flyweight.flags((short)200);
        flyweight.frameLength(1);
        flyweight.sessionId(15);
        flyweight.streamId(18);
        flyweight.activeTermId(81);
        flyweight.initialTermId(69);
        flyweight.termOffset(10);
        flyweight.termLength(444);
        flyweight.mtuLength(8096);
        flyweight.ttl(20_000);

        dissectFrame(FRAME_IN, buffer, 0, builder);

        assertEquals("[3.000000000] " + CONTEXT + ": " + FRAME_IN.name() +
            " [3/3]: address=127.0.0.1:8888 type=SETUP flags=11001000 frameLength=1 sessionId=15 streamId=18 " +
            "activeTermId=81 initialTermId=69 termOffset=10 termLength=444 mtu=8096 ttl=20000",
            builder.toString());
    }

    @Test
    void dissectFrameTypeRtt()
    {
        internalEncodeLogHeader(buffer, 0, 3, 3, () -> 3_000_000_000L);
        final int socketAddressOffset = encodeSocketAddress(
            buffer, LOG_HEADER_LENGTH, new InetSocketAddress("localhost", 8888));
        final RttMeasurementFlyweight flyweight = new RttMeasurementFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH + socketAddressOffset, 300);
        flyweight.headerType(HDR_TYPE_RTTM);
        flyweight.flags((short)20);
        flyweight.frameLength(100);
        flyweight.sessionId(0);
        flyweight.streamId(1);
        flyweight.echoTimestampNs(123456789);
        flyweight.receptionDelta(354);
        flyweight.receiverId(22);

        dissectFrame(FRAME_OUT, buffer, 0, builder);

        assertEquals("[3.000000000] " + CONTEXT + ": " + FRAME_OUT.name() + " [3/3]: " +
            "address=127.0.0.1:8888 type=RTT flags=00010100 frameLength=100 sessionId=0 streamId=1 " +
            "echoTimestampNs=123456789 receptionDelta=354 receiverId=22",
            builder.toString());
    }

    @Test
    void dissectFrameTypeError()
    {
        internalEncodeLogHeader(buffer, 0, 3, 3, () -> 3_000_000_000L);
        final int socketAddressOffset = encodeSocketAddress(
            buffer, LOG_HEADER_LENGTH, new InetSocketAddress("localhost", 5555));
        final ErrorFlyweight flyweight = new ErrorFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH + socketAddressOffset, 300);
        flyweight.headerType(HDR_TYPE_ERR);
        flyweight.flags((short)ErrorFlyweight.HAS_GROUP_ID_FLAG);
        flyweight.frameLength(876);
        flyweight.sessionId(42);
        flyweight.streamId(999);
        flyweight.receiverId(-4723947284689L);
        flyweight.groupTag(1_000_000_000_000_1L);
        flyweight.errorCode(1959);
        flyweight.errorMessage("test err msg string");

        dissectFrame(FRAME_OUT, buffer, 0, builder);

        assertEquals("[3.000000000] " + CONTEXT + ": " + FRAME_OUT.name() + " [3/3]: " +
            "address=127.0.0.1:5555 type=ERR flags=00001000 frameLength=59 sessionId=42 streamId=999 " +
            "receiverId=-4723947284689 groupTag=10000000000001 errorCode=1959 errorMessage=\"test err msg string\"",
            builder.toString());
    }

    @Test
    void dissectFrameTypeUnknown()
    {
        internalEncodeLogHeader(buffer, 0, 3, 3, () -> 3_000_000_000L);
        final int socketAddressOffset = encodeSocketAddress(
            buffer, LOG_HEADER_LENGTH, new InetSocketAddress("localhost", 8888));
        final DataHeaderFlyweight flyweight = new DataHeaderFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH + socketAddressOffset, 300);
        flyweight.headerType(Integer.MAX_VALUE);

        dissectFrame(CMD_OUT_ON_OPERATION_SUCCESS, buffer, 0, builder);

        assertEquals("[3.000000000] " + CONTEXT + ": " + CMD_OUT_ON_OPERATION_SUCCESS.name() +
            " [3/3]: address=127.0.0.1:8888 type=UNKNOWN(65535)",
            builder.toString());
    }

    @Test
    void dissectString()
    {
        internalEncodeLogHeader(buffer, 0, 1, 1, () -> 1_100_000_000L);
        buffer.putStringAscii(LOG_HEADER_LENGTH, "Hello, World!");

        DriverEventDissector.dissectString(CMD_IN_CLIENT_CLOSE, buffer, 0, builder);

        assertEquals("[1.100000000] " + CONTEXT + ": " + CMD_IN_CLIENT_CLOSE.name() + " [1/1]: Hello, World!",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_ADD_PUBLICATION", "CMD_IN_ADD_EXCLUSIVE_PUBLICATION" })
    void dissectCommandPublication(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, 10, 10, () -> 1_780_000_000L);
        final PublicationMessageFlyweight flyweight = new PublicationMessageFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.channel("pub channel");
        flyweight.streamId(3);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(15);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[1.780000000] " + CONTEXT + ": " + eventCode.name() + " [10/10]: " +
            "streamId=3 clientId=" + eventCode.id() + " correlationId=15 channel=pub channel",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_ADD_SUBSCRIPTION" })
    void dissectCommandSubscription(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 10, () -> 1_780_000_000L);
        final SubscriptionMessageFlyweight flyweight = new SubscriptionMessageFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.channel("sub channel");
        flyweight.streamId(31);
        flyweight.registrationCorrelationId(90);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(6);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[1.780000000] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/10]: " +
            "streamId=31 registrationCorrelationId=90 clientId=" + eventCode.id() + " correlationId=6 " +
            "channel=sub channel",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_REMOVE_COUNTER" })
    void dissectCommandRemoveCounterEvent(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 87, () -> 21_032_000_000L);
        final RemoveCounterFlyweight flyweight = new RemoveCounterFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.registrationId(11);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(16);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[21.032000000] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/87]: " +
            "registrationId=11 clientId=" + eventCode.id() + " correlationId=16",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_REMOVE_PUBLICATION" })
    void dissectCommandRemovePublicationEvent(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, RemovePublicationFlyweight.length(), 87, () -> 21_032_000_000L);
        final RemovePublicationFlyweight flyweight = new RemovePublicationFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.registrationId(11);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(16);
        flyweight.revoke(true);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[21.032000000] " + CONTEXT + ": " + eventCode.name() + " [" +
            RemovePublicationFlyweight.length() + "/87]: " + "registrationId=11 clientId=" +
            eventCode.id() + " correlationId=16 revoke=true",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_REMOVE_PUBLICATION" })
    void dissectOldShortCommandRemovePublicationEvent(final DriverEventCode eventCode)
    {
        // Remove Publication Commands from an old client will be shorter.  The won't have the flags field.
        // The dissector should notice the shorter length and act appropriately.
        internalEncodeLogHeader(buffer, 0, 24, 87, () -> 21_032_000_000L);
        final RemovePublicationFlyweight flyweight = new RemovePublicationFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.registrationId(11);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(16);
        flyweight.revoke(true);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[21.032000000] " + CONTEXT + ": " + eventCode.name() + " [24/87]: " +
            "registrationId=11 clientId=" + eventCode.id() + " correlationId=16",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_REMOVE_SUBSCRIPTION" })
    void dissectCommandRemoveSubscriptionEvent(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 87, () -> 21_032_000_000L);
        final RemoveSubscriptionFlyweight flyweight = new RemoveSubscriptionFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.registrationId(11);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(16);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[21.032000000] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/87]: " +
            "registrationId=11 clientId=" + eventCode.id() + " correlationId=16",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class,
        names = { "CMD_OUT_PUBLICATION_READY", "CMD_OUT_EXCLUSIVE_PUBLICATION_READY" })
    void dissectCommandPublicationReady(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 100, () -> 21_032_000_000L);
        final PublicationBuffersReadyFlyweight flyweight = new PublicationBuffersReadyFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.sessionId(eventCode.ordinal());
        flyweight.streamId(-24);
        flyweight.publicationLimitCounterId(1);
        flyweight.channelStatusCounterId(5);
        flyweight.correlationId(8);
        flyweight.registrationId(eventCode.id());
        flyweight.logFileName("log.txt");

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[21.032000000] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/100]: " +
            "sessionId=" + eventCode.ordinal() + " streamId=-24 publicationLimitCounterId=1 channelStatusCounterId=5 " +
            "correlationId=8 registrationId=" + eventCode.id() + " logFileName=log.txt",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_AVAILABLE_IMAGE" })
    void dissectCommandImageReady(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 100, () -> 21_032_000_000L);
        final ImageBuffersReadyFlyweight flyweight = new ImageBuffersReadyFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.sessionId(eventCode.ordinal());
        flyweight.streamId(22);
        flyweight.subscriberPositionId(0);
        flyweight.subscriptionRegistrationId(245);
        flyweight.correlationId(767);
        flyweight.logFileName("log2.txt");
        flyweight.sourceIdentity("source identity");

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[21.032000000] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/100]: " +
            "sessionId=" + eventCode.ordinal() + " streamId=22 subscriberPositionId=0 subscriptionRegistrationId=245 " +
            "correlationId=767 sourceIdentity=source identity logFileName=log2.txt",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_ON_OPERATION_SUCCESS" })
    void dissectCommandOperationSuccess(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 111, () -> 21_032_000_000L);
        final OperationSucceededFlyweight flyweight = new OperationSucceededFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.correlationId(eventCode.id());

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[21.032000000] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/111]: " +
            "correlationId=" + eventCode.id(),
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_KEEPALIVE_CLIENT", "CMD_IN_CLIENT_CLOSE" })
    void dissectCommandCorrelationEvent(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 100, () -> 21_032_000_000L);
        final CorrelatedMessageFlyweight flyweight = new CorrelatedMessageFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(2);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[21.032000000] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/100]: " +
            "clientId=" + eventCode.id() + " correlationId=2",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_ON_UNAVAILABLE_IMAGE" })
    void dissectCommandImage(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 99, () -> 21_032_000_000L);
        final ImageMessageFlyweight flyweight = new ImageMessageFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.streamId(300);
        flyweight.correlationId(eventCode.id());
        flyweight.subscriptionRegistrationId(-19);
        flyweight.channel("the channel");

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[21.032000000] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/99]: " +
            "streamId=300 correlationId=" + eventCode.id() + " subscriptionRegistrationId=-19 channel=the channel",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = {
        "CMD_IN_ADD_DESTINATION",
        "CMD_IN_REMOVE_DESTINATION",
        "CMD_IN_ADD_RCV_DESTINATION",
        "CMD_IN_REMOVE_RCV_DESTINATION" })
    void dissectCommandDestination(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 77, () -> 21_032_000_000L);
        final DestinationMessageFlyweight flyweight = new DestinationMessageFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.channel("dst");
        flyweight.registrationCorrelationId(eventCode.id());
        flyweight.clientId(1010101);
        flyweight.correlationId(404);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[21.032000000] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/77]: " +
            "registrationCorrelationId=" + eventCode.id() + " clientId=1010101 correlationId=404 channel=dst",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_ERROR" })
    void dissectCommandError(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 100, () -> 1_900_000_000L);
        final ErrorResponseFlyweight flyweight = new ErrorResponseFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.offendingCommandCorrelationId(eventCode.id());
        flyweight.errorCode(ErrorCode.MALFORMED_COMMAND);
        flyweight.errorMessage("Huge stacktrace!");

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[1.900000000] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/100]: " +
            "offendingCommandCorrelationId=" + eventCode.id() + " errorCode=" + ErrorCode.MALFORMED_COMMAND +
            " message=Huge stacktrace!",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_ADD_COUNTER" })
    void dissectCommandCounter(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 100, () -> 1_900_000_000L);
        final CounterMessageFlyweight flyweight = new CounterMessageFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.typeId(3);
        flyweight.keyBuffer(newBuffer(new byte[20]), 0, 10);
        flyweight.labelBuffer(newBuffer(new byte[100]), 26, 13);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(42);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[1.900000000] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/100]: " +
            "typeId=3 keyBufferOffset=" + flyweight.keyBufferOffset() + " keyBufferLength=10 labelBufferOffset=" +
            flyweight.labelBufferOffset() + " labelBufferLength=13 clientId=" + eventCode.id() + " correlationId=42",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_SUBSCRIPTION_READY" })
    void dissectCommandSubscriptionReady(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 100, () -> 1_900_000_000L);
        final SubscriptionReadyFlyweight flyweight = new SubscriptionReadyFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.correlationId(42);
        flyweight.channelStatusCounterId(eventCode.id());

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[1.900000000] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/100]: " +
            "correlationId=42 channelStatusCounterId=" + eventCode.id(),
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_COUNTER_READY", "CMD_OUT_ON_UNAVAILABLE_COUNTER" })
    void dissectCommandCounterUpdate(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 100, () -> 1_900_000_000L);
        final CounterUpdateFlyweight flyweight = new CounterUpdateFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.correlationId(eventCode.id());
        flyweight.counterId(18);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[1.900000000] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/100]: " +
            "correlationId=" + eventCode.id() + " counterId=18",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_ON_CLIENT_TIMEOUT" })
    void dissectCommandClientTimeout(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 100, () -> 1_900_000_000L);
        final ClientTimeoutFlyweight flyweight = new ClientTimeoutFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.clientId(eventCode.id());

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[1.900000000] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/100]: " +
            "clientId=" + eventCode.id(),
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_TERMINATE_DRIVER" })
    void dissectCommandTerminateDriver(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 100, () -> 1_900_000_000L);
        final TerminateDriverFlyweight flyweight = new TerminateDriverFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.clientId(eventCode.id());
        flyweight.tokenBuffer(newBuffer(new byte[15]), 4, 11);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[1.900000000] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/100]: " +
            "clientId=" + eventCode.id() + " tokenBufferLength=11",
            builder.toString());
    }

    @Test
    void dissectCommandUnknown()
    {
        final DriverEventCode eventCode = SEND_CHANNEL_CREATION;
        internalEncodeLogHeader(buffer, 0, 5, 5, () -> 1_000_000_000L);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[1.000000000] " + CONTEXT + ": " + eventCode.name() + " [5/5]: COMMAND_UNKNOWN: " + eventCode,
            builder.toString());
    }

    @Test
    void dissectUntetheredSubscriptionStateChange()
    {
        final int offset = 12;
        internalEncodeLogHeader(buffer, offset, 22, 88, () -> 1_500_000_000L);
        buffer.putLong(offset + LOG_HEADER_LENGTH, 88, LITTLE_ENDIAN);
        buffer.putInt(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG, 123, LITTLE_ENDIAN);
        buffer.putInt(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG + SIZE_OF_INT, 777, LITTLE_ENDIAN);
        buffer.putStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG + 2 * SIZE_OF_INT, "state changed");

        DriverEventDissector.dissectUntetheredSubscriptionStateChange(buffer, offset, builder);

        assertEquals("[1.500000000] " + CONTEXT + ": " + UNTETHERED_SUBSCRIPTION_STATE_CHANGE.name() +
            " [22/88]: subscriptionId=88 streamId=123 sessionId=777 state changed",
            builder.toString());
    }

    @Test
    void dissectAddress()
    {
        final int offset = 24;
        internalEncodeLogHeader(buffer, offset, 17, 27, () -> 2_500_000_000L);
        encodeSocketAddress(
            buffer, LOG_HEADER_LENGTH + offset, new InetSocketAddress("localhost", 4848));

        DriverEventDissector.dissectAddress(NAME_RESOLUTION_NEIGHBOR_ADDED, buffer, offset, builder);

        assertEquals("[2.500000000] " + CONTEXT + ": " + NAME_RESOLUTION_NEIGHBOR_ADDED.name() +
            " [17/27]: 127.0.0.1:4848", builder.toString());
    }

    @Test
    void dissectIpv6Address()
    {
        final int offset = 24;
        internalEncodeLogHeader(buffer, offset, 17, 27, () -> 2_500_000_000L);
        encodeSocketAddress(
            buffer, LOG_HEADER_LENGTH + offset, new InetSocketAddress("2001:0db8:85a3:0000:0000:8a2e:0370:7334", 4848));

        DriverEventDissector.dissectAddress(NAME_RESOLUTION_NEIGHBOR_ADDED, buffer, offset, builder);

        assertEquals("[2.500000000] " + CONTEXT + ": " + NAME_RESOLUTION_NEIGHBOR_ADDED.name() +
            " [17/27]: [2001:db8:85a3:0:0:8a2e:370:7334]:4848", builder.toString());
    }

    @Test
    void dissectFlowControlReceiver()
    {
        final int offset = 24;
        internalEncodeLogHeader(buffer, offset, 42, 48, () -> 2_500_000_000L);
        buffer.putInt(offset + LOG_HEADER_LENGTH, 5, LITTLE_ENDIAN);
        buffer.putLong(offset + LOG_HEADER_LENGTH + SIZE_OF_INT, -45754449919191L, LITTLE_ENDIAN);
        buffer.putInt(offset + LOG_HEADER_LENGTH + SIZE_OF_INT + SIZE_OF_LONG, 11, LITTLE_ENDIAN);
        buffer.putInt(offset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2 + SIZE_OF_LONG, 4, LITTLE_ENDIAN);
        buffer.putStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_INT * 3 + SIZE_OF_LONG, "ABC");

        DriverEventDissector.dissectFlowControlReceiver(FLOW_CONTROL_RECEIVER_ADDED, buffer, offset, builder);

        assertEquals("[2.500000000] " + CONTEXT + ": " + FLOW_CONTROL_RECEIVER_ADDED.name() +
            " [42/48]: receiverCount=5 receiverId=-45754449919191 sessionId=11 streamId=4 channel=ABC",
            builder.toString());
    }

    @Test
    void dissectResolve() throws UnknownHostException
    {
        final String resolver = "testResolver";
        final long durationNs = 32167;
        final String hostname = "localhost";
        final boolean isReResolution = false;
        final InetAddress address = InetAddress.getByName("127.0.0.1");

        final int length = SIZE_OF_BOOLEAN + SIZE_OF_LONG + trailingStringLength(resolver, MAX_HOST_NAME_LENGTH) +
            trailingStringLength(hostname, MAX_HOST_NAME_LENGTH) +
            inetAddressLength(address);

        DriverEventEncoder.encodeResolve(
            buffer, 0, length, length, resolver, durationNs, hostname, isReResolution, address);
        final StringBuilder builder = new StringBuilder();
        DriverEventDissector.dissectResolve(buffer, 0, builder);

        assertThat(builder.toString(), endsWith(
            "DRIVER: NAME_RESOLUTION_RESOLVE [46/46]: " +
            "resolver=testResolver durationNs=32167 name=localhost isReResolution=false address=127.0.0.1"));
    }

    @Test
    void dissectResolveNullAddress()
    {
        final String resolver = "myResolver";
        final long durationNs = -1;
        final String hostname = "some-host";
        final boolean isReResolution = true;
        final InetAddress address = null;

        final int length = SIZE_OF_BOOLEAN + SIZE_OF_LONG + trailingStringLength(resolver, MAX_HOST_NAME_LENGTH) +
            trailingStringLength(hostname, MAX_HOST_NAME_LENGTH) +
            inetAddressLength(address);

        DriverEventEncoder.encodeResolve(
            buffer, 0, length, length, resolver, durationNs, hostname, isReResolution, address);
        final StringBuilder builder = new StringBuilder();
        DriverEventDissector.dissectResolve(buffer, 0, builder);

        assertThat(builder.toString(), endsWith(
            "DRIVER: NAME_RESOLUTION_RESOLVE [40/40]: " +
            "resolver=myResolver durationNs=-1 name=some-host isReResolution=true address=unknown-address"));
    }

    @Test
    void dissectResolveWithReallyLongNames() throws UnknownHostException
    {
        final String longString = "testResolver.this.is.a.really.long.string.to.force.truncation.0000000000000000000" +
            "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000";

        final String expected = "DRIVER: NAME_RESOLUTION_RESOLVE [537/537]: resolver=testResolver." +
            "this.is.a.really.long.string.to.force.truncation.0000000000000000000000000000000000000000000000000000000" +
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
            "00000000000000000000000000000000... " +
            "durationNs=555 " +
            "name=testResolver.this.is.a.really.long.string.to.force.truncation.0000000000000000000000000000000000000" +
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
            "00000000000000000000000000000000000000000000000000... " +
            "isReResolution=false " +
            "address=127.0.0.1";

        final InetAddress address = InetAddress.getByName("127.0.0.1");

        final int length = SIZE_OF_BOOLEAN + SIZE_OF_LONG + trailingStringLength(longString, MAX_HOST_NAME_LENGTH) +
            trailingStringLength(longString, MAX_HOST_NAME_LENGTH) +
            inetAddressLength(address);

        DriverEventEncoder.encodeResolve(buffer, 0, length, length, longString, 555, longString, false, address);
        final StringBuilder builder = new StringBuilder();
        DriverEventDissector.dissectResolve(buffer, 0, builder);

        assertThat(builder.toString(), endsWith(expected));
    }

    @Test
    void dissectLookup()
    {
        final int offset = 48;
        final String resolver = "xyz";
        final long durationNs = 32167;
        final String name = "localhost:7777";
        final boolean isReLookup = false;
        final String resolvedName = "test:1234";

        final int length = SIZE_OF_BOOLEAN + SIZE_OF_LONG +
            trailingStringLength(resolver, MAX_HOST_NAME_LENGTH) +
            trailingStringLength(name, MAX_HOST_NAME_LENGTH) +
            trailingStringLength(resolvedName, MAX_HOST_NAME_LENGTH);

        DriverEventEncoder.encodeLookup(
            buffer, offset, length, length, resolver, durationNs, name, isReLookup, resolvedName);
        final StringBuilder builder = new StringBuilder();
        DriverEventDissector.dissectLookup(buffer, offset, builder);

        assertThat(builder.toString(), endsWith(
            "DRIVER: NAME_RESOLUTION_LOOKUP [47/47]: " +
            "resolver=xyz durationNs=32167 name=localhost:7777 isReLookup=false resolvedName=test:1234"));
    }

    @Test
    void dissectHostName()
    {
        final int offset = 8;
        final long durationNs = 32167;
        final String hostName = "some.funky.host.name";

        final int length = SIZE_OF_LONG + trailingStringLength(hostName, MAX_HOST_NAME_LENGTH);

        DriverEventEncoder.encodeHostName(buffer, offset, length, length, durationNs, hostName);
        final StringBuilder builder = new StringBuilder();
        DriverEventDissector.dissectHostName(buffer, offset, builder);

        assertThat(builder.toString(), endsWith(
            "DRIVER: NAME_RESOLUTION_HOST_NAME [32/32]: durationNs=32167 hostName=some.funky.host.name"));
    }

    @Test
    void dissectPublicationRevoke()
    {
        final int offset = 0;
        final long revokePos = 1234;
        final int sessionId = 4;
        final int streamId = 99;
        final String channel = "excellent.channel";

        final int length = SIZE_OF_LONG + (SIZE_OF_INT * 2) + trailingStringLength(channel, MAX_CHANNEL_URI_LENGTH);

        DriverEventEncoder.encodePublicationRevoke(
            buffer, offset, length, length, revokePos, sessionId, streamId, channel);
        final StringBuilder builder = new StringBuilder();
        DriverEventDissector.dissectPublicationRevoke(buffer, offset, builder);

        assertThat(builder.toString(), endsWith(
            "DRIVER: PUBLICATION_REVOKE [37/37]: revokedPos=1234 sessionId=4 streamId=99 channel=excellent.channel"));
    }

    @Test
    void dissectPublicationImageRevoke()
    {
        final int offset = 0;
        final long revokePos = 1234;
        final int sessionId = 4;
        final int streamId = 99;
        final String channel = "excellent.channel";

        final int length = SIZE_OF_LONG + (SIZE_OF_INT * 2) + trailingStringLength(channel, MAX_CHANNEL_URI_LENGTH);

        DriverEventEncoder.encodePublicationImageRevoke(
            buffer, offset, length, length, revokePos, sessionId, streamId, channel);
        final StringBuilder builder = new StringBuilder();
        DriverEventDissector.dissectPublicationImageRevoke(buffer, offset, builder);

        assertThat(builder.toString(), endsWith(
            "DRIVER: PUBLICATION_IMAGE_REVOKE [37/37]: " +
            "revokedPos=1234 sessionId=4 streamId=99 channel=excellent.channel"));
    }

    private DirectBuffer newBuffer(final byte[] bytes)
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(allocate(bytes.length));
        buffer.putBytes(0, bytes);
        return buffer;
    }
}
