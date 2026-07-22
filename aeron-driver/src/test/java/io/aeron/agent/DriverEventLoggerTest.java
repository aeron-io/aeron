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

import io.aeron.logging.EventConfiguration;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static io.aeron.agent.DriverEventCode.*;
import static io.aeron.agent.DriverEventEncoder.untetheredSubscriptionStateChangeLength;
import static io.aeron.agent.DriverEventLogger.MAX_HOST_NAME_LENGTH;
import static io.aeron.agent.DriverEventLogger.toEventCodeId;
import static io.aeron.logging.CommonEventEncoder.LOG_HEADER_LENGTH;
import static io.aeron.logging.CommonEventEncoder.MAX_CAPTURE_LENGTH;
import static io.aeron.logging.CommonEventEncoder.STATE_SEPARATOR;
import static io.aeron.logging.CommonEventEncoder.captureLength;
import static io.aeron.logging.CommonEventEncoder.inetAddressLength;
import static io.aeron.logging.CommonEventEncoder.socketAddressLength;
import static io.aeron.logging.CommonEventEncoder.trailingStringLength;
import static io.aeron.logging.EventConfiguration.MAX_EVENT_LENGTH;
import static io.aeron.test.agent.AgentTests.verifyLogHeader;
import static io.aeron.test.Tests.generateStringWithSuffix;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.fill;
import static org.agrona.BitUtil.*;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.*;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TAIL_POSITION_OFFSET;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DriverEventLoggerTest
{
    private static final int CAPACITY = 32 * 1024;
    private final UnsafeBuffer logBuffer = new UnsafeBuffer(allocateDirect(CAPACITY + TRAILER_LENGTH));
    private final DriverEventLogger logger = new DriverEventLogger(new ManyToOneRingBuffer(logBuffer));
    private final UnsafeBuffer buffer = new UnsafeBuffer(allocate(MAX_EVENT_LENGTH * 3));

    @AfterEach
    void after()
    {
//        DriverComponentLogger.ENABLED_EVENTS.clear();
        EventConfiguration.eventReader().ringBuffer().unblock();
    }

    @ParameterizedTest
    @EnumSource(DriverEventCode.class)
    void toEventCodeIdComputesEventId(final DriverEventCode eventCode)
    {
        assertEquals(eventCode.id(), toEventCodeId(eventCode));
    }

    @Test
    @Disabled
    void logIsNoOpIfEventIsNotEnabled()
    {
        // TODO: Reconsider the necessity of this test, since it will require disabling FRAME_IN
        //       Explicitly disabling it might break other tests
        buffer.setMemory(20, 100, (byte)5);

        logger.log(FRAME_IN, buffer, 20, 100);

        assertEquals(0, logBuffer.getInt(lengthOffset(0), LITTLE_ENDIAN));
    }

    @Test
    void log()
    {
        final DriverEventCode eventCode = CMD_IN_TERMINATE_DRIVER;
//        DriverComponentLogger.ENABLED_EVENTS.add(eventCode);
        final int recordOffset = align(13, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        final int length = 100;
        final int srcOffset = 20;
        buffer.setMemory(srcOffset, length, (byte)5);

        logger.log(eventCode, buffer, srcOffset, length);

        verifyLogHeader(logBuffer, recordOffset, toEventCodeId(eventCode), length, length);
        for (int i = 0; i < length; i++)
        {
            assertEquals(5, logBuffer.getByte(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + i)));
        }
    }

    @Test
    void logFrameIn()
    {
        final int recordOffset = align(100, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        final int length = 10_000;
        final int captureLength = MAX_CAPTURE_LENGTH;
        final int srcOffset = 4;
        buffer.setMemory(srcOffset, MAX_CAPTURE_LENGTH, (byte)3);
        final int encodedSocketLength = 12;

        logger.logFrameIn(buffer, srcOffset, length, new InetSocketAddress("localhost", 5555));

        verifyLogHeader(logBuffer, recordOffset, toEventCodeId(FRAME_IN), captureLength, length + encodedSocketLength);
        assertEquals(5555, logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(srcOffset,
            logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT), LITTLE_ENDIAN));

        for (int i = 0; i < captureLength - encodedSocketLength; i++)
        {
            assertEquals(3,
                logBuffer.getByte(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + encodedSocketLength + i)));
        }
    }

    @Test
    void logFrameOut()
    {
        final int recordOffset = 24;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        final ByteBuffer byteBuffer = buffer.byteBuffer();
        byteBuffer.position(8);
        final byte[] bytes = new byte[32];
        fill(bytes, (byte)-1);
        byteBuffer.put(bytes);
        byteBuffer.flip().position(10).limit(38);
        final int encodedSocketLength = 12;
        final int length = byteBuffer.remaining() + encodedSocketLength;
        final int arrayCaptureLength = length - encodedSocketLength;

        logger.logFrameOut(byteBuffer, new InetSocketAddress("localhost", 3232));

        verifyLogHeader(logBuffer, recordOffset, toEventCodeId(FRAME_OUT), length, length);
        assertEquals(3232, logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(4,
            logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT), LITTLE_ENDIAN));

        for (int i = 0; i < arrayCaptureLength; i++)
        {
            assertEquals(-1,
                logBuffer.getByte(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + encodedSocketLength + i)));
        }
    }

    @Test
    void logString()
    {
        final int recordOffset = align(100, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        final DriverEventCode eventCode = CMD_IN_ADD_PUBLICATION;
        final String value = "abc";
        final int captureLength = value.length() + SIZE_OF_INT;

        logger.logString(eventCode, value);

        verifyLogHeader(logBuffer, recordOffset, toEventCodeId(eventCode), captureLength, captureLength);
        assertEquals(value,
            logBuffer.getStringAscii(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
    }

    @Test
    void logPublicationRemoval()
    {
        final int recordOffset = align(1111, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        final String uri = "uri";
        final int sessionId = 42;
        final int streamId = 19;
        final int captureLength = uri.length() + SIZE_OF_INT * 3;

        logger.logPublicationRemoval(uri, sessionId, streamId);

        verifyLogHeader(
            logBuffer, recordOffset, toEventCodeId(REMOVE_PUBLICATION_CLEANUP), captureLength, captureLength);
        assertEquals(sessionId, logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(streamId,
            logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT), LITTLE_ENDIAN));
        assertEquals(uri,
            logBuffer.getStringAscii(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2),
            LITTLE_ENDIAN));
    }

    @Test
    void logSubscriptionRemoval()
    {
        final int recordOffset = align(131, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        final String uri = "uri";
        final int streamId = 42;
        final long id = 19;
        final int captureLength = uri.length() + SIZE_OF_INT * 2 + SIZE_OF_LONG;

        logger.logSubscriptionRemoval(uri, streamId, id);

        verifyLogHeader(
            logBuffer, recordOffset, toEventCodeId(REMOVE_SUBSCRIPTION_CLEANUP), captureLength, captureLength);
        assertEquals(streamId, logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(id,
            logBuffer.getLong(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT), LITTLE_ENDIAN));
        assertEquals(uri,
            logBuffer.getStringAscii(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT + SIZE_OF_LONG),
            LITTLE_ENDIAN));
    }

    @Test
    void logImageRemoval()
    {
        final int recordOffset = align(192, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        final String uri = "uri";
        final int sessionId = 8;
        final int streamId = 61;
        final long id = 19;
        final int captureLength = uri.length() + SIZE_OF_INT * 3 + SIZE_OF_LONG;

        logger.logImageRemoval(uri, sessionId, streamId, id);

        verifyLogHeader(
            logBuffer, recordOffset, toEventCodeId(REMOVE_IMAGE_CLEANUP), captureLength, captureLength);
        assertEquals(sessionId, logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(streamId,
            logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT), LITTLE_ENDIAN));
        assertEquals(id,
            logBuffer.getLong(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2), LITTLE_ENDIAN));
        assertEquals(uri, logBuffer.getStringAscii(
            encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2 + SIZE_OF_LONG), LITTLE_ENDIAN));
    }

    @Test
    void logUntetheredSubscriptionStateChange()
    {
        final int recordOffset = align(192, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        final TimeUnit from = TimeUnit.DAYS;
        final TimeUnit to = TimeUnit.NANOSECONDS;
        final long subscriptionId = Long.MIN_VALUE;
        final int streamId = 61;
        final int sessionId = 8;
        final int captureLength = captureLength(untetheredSubscriptionStateChangeLength(from, to));

        logger.logUntetheredSubscriptionStateChange(from, to, subscriptionId, streamId, sessionId);

        verifyLogHeader(
            logBuffer, recordOffset, toEventCodeId(UNTETHERED_SUBSCRIPTION_STATE_CHANGE), captureLength, captureLength);
        assertEquals(subscriptionId,
            logBuffer.getLong(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(streamId,
            logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_LONG), LITTLE_ENDIAN));
        assertEquals(sessionId,
            logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_LONG + SIZE_OF_INT),
            LITTLE_ENDIAN));
        assertEquals(from.name() + STATE_SEPARATOR + to.name(), logBuffer.getStringAscii(
            encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2 + SIZE_OF_LONG), LITTLE_ENDIAN));
    }

    @Test
    void logAddress()
    {
        final int recordOffset = 64;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        final DriverEventCode eventCode = NAME_RESOLUTION_NEIGHBOR_REMOVED;
        final int captureLength = 12;

        logger.logAddress(eventCode, new InetSocketAddress("localhost", 5656));

        verifyLogHeader(logBuffer, recordOffset, toEventCodeId(eventCode), captureLength, captureLength);
        assertEquals(5656, logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(4, logBuffer.getInt(
            encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT), LITTLE_ENDIAN));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void logResolve(final boolean isReResolution) throws UnknownHostException
    {
        final int recordOffset = 64;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);

        final String resolverName = "test";
        final long durationNs = TimeUnit.DAYS.toNanos(1);
        final String hostName = generateStringWithSuffix("very-l", "0", 1000);
        final String expectedHostName = hostName.substring(0, MAX_HOST_NAME_LENGTH - 3) + "...";
        final InetAddress address = InetAddress.getLocalHost();
        final int captureLength = SIZE_OF_BOOLEAN + SIZE_OF_LONG +
            trailingStringLength(resolverName, MAX_HOST_NAME_LENGTH) +
            trailingStringLength(hostName, MAX_HOST_NAME_LENGTH) +
            inetAddressLength(address);

        logger.logResolve(resolverName, durationNs, hostName, isReResolution, address);

        verifyLogHeader(logBuffer, recordOffset, toEventCodeId(NAME_RESOLUTION_RESOLVE), captureLength, captureLength);

        int index = encodedMsgOffset(recordOffset) + LOG_HEADER_LENGTH;

        assertEquals(isReResolution, 1 == logBuffer.getByte(index));
        index += SIZE_OF_BYTE;

        assertEquals(durationNs, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;

        assertEquals(resolverName, logBuffer.getStringAscii(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT + resolverName.length();

        assertEquals(expectedHostName, logBuffer.getStringAscii(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT + expectedHostName.length();

        final byte[] addressBytes = address.getAddress();
        assertEquals(addressBytes.length, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        for (int i = 0; i < addressBytes.length; i++)
        {
            assertEquals(addressBytes[i], logBuffer.getByte(index + i));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void logLookup(final boolean isReLookup)
    {
        final int recordOffset = align(30, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);

        final String resolverName = generateStringWithSuffix("my-resolver", "*", 1000);
        final String expectedResolverName = resolverName.substring(0, MAX_HOST_NAME_LENGTH - 3) + "...";
        final long durationNs = TimeUnit.HOURS.toNanos(3);
        final String name = "abc.example.com:5555";
        final String resolvedName = "corporate.fancy.address.returned:5555";
        final int captureLength = SIZE_OF_BOOLEAN + SIZE_OF_LONG +
            trailingStringLength(resolverName, MAX_HOST_NAME_LENGTH) +
            trailingStringLength(name, MAX_HOST_NAME_LENGTH) +
            trailingStringLength(resolvedName, MAX_HOST_NAME_LENGTH);

        logger.logLookup(resolverName, durationNs, name, isReLookup, resolvedName);

        verifyLogHeader(logBuffer, recordOffset, toEventCodeId(NAME_RESOLUTION_LOOKUP), captureLength, captureLength);

        int index = encodedMsgOffset(recordOffset) + LOG_HEADER_LENGTH;

        assertEquals(isReLookup ? 1 : 0, logBuffer.getByte(index));
        index += SIZE_OF_BOOLEAN;

        assertEquals(durationNs, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;

        assertEquals(expectedResolverName, logBuffer.getStringAscii(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT + expectedResolverName.length();

        assertEquals(name, logBuffer.getStringAscii(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT + name.length();

        assertEquals(resolvedName, logBuffer.getStringAscii(index, LITTLE_ENDIAN));
    }

    @Test
    void logHost()
    {
        final int recordOffset = 64;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);

        final long durationNs = TimeUnit.DAYS.toNanos(1);
        final String hostName = generateStringWithSuffix("host-name", "e", 1000);
        final String expectedHostName = hostName.substring(0, MAX_HOST_NAME_LENGTH - 3) + "...";
        final int captureLength = SIZE_OF_LONG +
            trailingStringLength(hostName, MAX_HOST_NAME_LENGTH);

        logger.logHostName(durationNs, hostName);

        verifyLogHeader(
            logBuffer, recordOffset, toEventCodeId(NAME_RESOLUTION_HOST_NAME), captureLength, captureLength);

        int index = encodedMsgOffset(recordOffset) + LOG_HEADER_LENGTH;

        assertEquals(durationNs, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;

        assertEquals(expectedHostName, logBuffer.getStringAscii(index, LITTLE_ENDIAN));
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "NAK_SENT", "NAK_RECEIVED" })
    void logNakMessage(final DriverEventCode eventCode)
    {
        final InetSocketAddress inetSocketAddress = new InetSocketAddress("192.168.1.1", 10001);

        final int sessionId = 9821374;
        final int streamId = 988234;
        final int termId = 89324;
        final int termOffset = 9862314;
        final int length = 1239;
        final String channel =
            "aeron:udp?endpoint=localhost:10000|term-length=1m|init-term-id=0|term-id=0|term-offset=0";
        final int recordOffset = 64;
        final int captureLength = socketAddressLength(inetSocketAddress) + (6 * SIZE_OF_INT) + channel.length();

        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        logger.logNakMessage(eventCode, inetSocketAddress, sessionId, streamId, termId, termOffset, length, channel);
        verifyLogHeader(
            logBuffer, recordOffset, toEventCodeId(eventCode), captureLength, captureLength);

        final StringBuilder sb = new StringBuilder();
        DriverEventDissector.dissectNak(eventCode, logBuffer, encodedMsgOffset(recordOffset), sb);

        final String expectedMessagePattern =
            "\\[[0-9]+\\.[0-9]+] DRIVER: " + eventCode + " \\[124/124]: address=192.168.1.1:10001 " +
            "sessionId=9821374 streamId=988234 termId=89324 termOffset=9862314 length=1239 channel=" +
            "aeron:udp\\?endpoint=localhost:10000\\|term-length=1m\\|init-term-id=0\\|term-id=0\\|term-offset=0";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logResend()
    {
        final int sessionId = 9821374;
        final int streamId = 988234;
        final int termId = 89324;
        final int termOffset = 9862314;
        final int length = 1239;
        final int recordOffset = 64;
        final String channel =
            "aeron:udp?endpoint=localhost:10000|term-length=1m|init-term-id=0|term-id=0|term-offset=0";
        final int captureLength = 6 * SIZE_OF_INT + channel.length();

        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        logger.logResend(sessionId, streamId, termId, termOffset, length, channel);
        verifyLogHeader(
            logBuffer, recordOffset, toEventCodeId(RESEND), captureLength, captureLength);

        final StringBuilder sb = new StringBuilder();
        DriverEventDissector.dissectResend(logBuffer, encodedMsgOffset(recordOffset), sb);

        final String expectedMessagePattern =
            "\\[[0-9]+\\.[0-9]+] DRIVER: RESEND \\[112/112]: sessionId=9821374 streamId=988234 termId=89324 " +
            "termOffset=9862314 length=1239 " +
            "channel=" +
            "aeron:udp\\?endpoint=localhost:10000\\|term-length=1m\\|init-term-id=0\\|term-id=0\\|term-offset=0";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }
}
