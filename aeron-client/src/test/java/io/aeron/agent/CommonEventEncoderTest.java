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

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.InetSocketAddress;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.asList;
import static java.util.Arrays.fill;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class CommonEventEncoderTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_EVENT_LENGTH * 10]);

    @Test
    void encodeLogHeader()
    {
        final int offset = 12;

        final int encodedLength = internalEncodeLogHeader(buffer, offset, 100, Integer.MAX_VALUE, () -> 555_000L);

        assertEquals(LOG_HEADER_LENGTH, encodedLength);
        assertEquals(100, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(Integer.MAX_VALUE, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertEquals(555_000L, buffer.getLong(offset + SIZE_OF_INT * 2, LITTLE_ENDIAN));
    }

    @Test
    void encodeLogHeaderThrowsIllegalArgumentExceptionIfCaptureLengthIsNegative()
    {
        assertThrows(IllegalArgumentException.class,
            () -> internalEncodeLogHeader(buffer, 0, -1, Integer.MAX_VALUE, () -> 0));
    }

    @Test
    void encodeLogHeaderThrowsIllegalArgumentExceptionIfCaptureLengthIsGreaterThanMaxCaptureSize()
    {
        assertThrows(IllegalArgumentException.class,
            () -> internalEncodeLogHeader(buffer, 0, MAX_CAPTURE_LENGTH + 1, Integer.MAX_VALUE, () -> 0));
    }

    @Test
    void encodeLogHeaderThrowsIllegalArgumentExceptionIfCaptureLengthIsGreaterThanLength()
    {
        assertThrows(IllegalArgumentException.class,
            () -> internalEncodeLogHeader(buffer, 0, 100, 80, () -> 0));
    }

    @Test
    void encodeLogHeaderThrowsIllegalArgumentExceptionIfLengthIsNegative()
    {
        assertThrows(IllegalArgumentException.class,
            () -> internalEncodeLogHeader(buffer, 0, 20, -2, () -> 0));
    }

    @Test
    void encodeSocketAddress()
    {
        final InetSocketAddress socketAddress = new InetSocketAddress(15015);
        final byte[] address = socketAddress.getAddress().getAddress();

        final int encodedLength = CommonEventEncoder.encodeSocketAddress(buffer, 4, socketAddress);

        assertEquals(SIZE_OF_INT * 2 + address.length, encodedLength);
        assertEquals(15015, buffer.getInt(4, LITTLE_ENDIAN));
        assertEquals(address.length, buffer.getInt(4 + SIZE_OF_INT, LITTLE_ENDIAN));
        final byte[] encodedAddress = new byte[address.length];
        buffer.getBytes(4 + SIZE_OF_INT * 2, encodedAddress);
        assertArrayEquals(address, encodedAddress);
    }

    @Test
    void encodeTrailingStringAsAsciiWhenPayloadIsSmallerThanMaxMessageSizeWithoutHeader()
    {
        final int offset = 17;
        final int remainingCapacity = 22;

        final int encodedLength = encodeTrailingString(buffer, offset, remainingCapacity, "ab©d️");

        assertEquals(SIZE_OF_INT + 5, encodedLength);
        assertEquals("ab?d?", buffer.getStringAscii(offset));
    }

    @Test
    void encodeTrailingStringAsAsciiWhenPayloadExceedsMaxMessageSizeWithoutHeader()
    {
        final int offset = 23;
        final int remainingCapacity = 59;
        final char[] chars = new char[100];
        fill(chars, 'x');
        final String value = new String(chars);

        final int encodedLength = encodeTrailingString(buffer, offset, remainingCapacity, value);

        assertEquals(remainingCapacity, encodedLength);
        assertEquals(value.substring(0, remainingCapacity - SIZE_OF_INT - 3) + "...",
            buffer.getStringAscii(offset));
    }

    @Test
    void encodeBufferSmallerThanMaxCaptureSize()
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[256]);
        final int offset = 24;
        final int srcOffset = 20;
        final int length = 128;
        srcBuffer.setMemory(srcOffset, length, (byte)111);

        final int encodedLength = encode(buffer, offset, length, length, srcBuffer, srcOffset);

        assertEquals(LOG_HEADER_LENGTH + length, encodedLength);
        assertEquals(length, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        for (int i = 0; i < length; i++)
        {
            assertEquals(111, buffer.getByte(offset + LOG_HEADER_LENGTH + i));
        }
    }

    @Test
    void encodeBufferBuggerThanMaxCaptureSize()
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[MAX_EVENT_LENGTH * 2]);
        final int offset = 256;
        final int srcOffset = 20;
        final int length = MAX_EVENT_LENGTH + 1000;
        srcBuffer.setMemory(srcOffset, length, (byte)-5);

        final int encodedLength = encode(buffer, offset, MAX_CAPTURE_LENGTH, length, srcBuffer, srcOffset);

        assertEquals(MAX_EVENT_LENGTH, encodedLength);
        assertEquals(MAX_CAPTURE_LENGTH, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        for (int i = 0; i < MAX_CAPTURE_LENGTH; i++)
        {
            assertEquals(-5, buffer.getByte(offset + LOG_HEADER_LENGTH + i));
        }
    }

    @ParameterizedTest
    @MethodSource("captureLengthArgs")
    void testCaptureLength(final int length, final int captureLength)
    {
        assertEquals(captureLength, captureLength(length));
    }

    @ParameterizedTest
    @MethodSource("encodedLengthArgs")
    void testEncodedLength(final int length, final int encodedLength)
    {
        assertEquals(encodedLength, encodedLength(length));
    }

    @Test
    void testStateTransitionStringLength()
    {
        final TimeUnit from = TimeUnit.NANOSECONDS;
        final TimeUnit to = TimeUnit.HOURS;
        assertEquals(from.name().length() + STATE_SEPARATOR.length() + to.name().length() + SIZE_OF_INT,
            stateTransitionStringLength(from, to));
    }

    @Test
    void stateNameReturnsNameOfTheEnumConstant()
    {
        final ChronoUnit state = ChronoUnit.CENTURIES;
        assertEquals(state.name(), enumName(state));
    }

    @Test
    void stateNameReturnsNullIfNull()
    {
        assertEquals("null", enumName(null));
    }

    private static List<Arguments> captureLengthArgs()
    {
        return asList(
            arguments(0, 0),
            arguments(10, 10),
            arguments(MAX_CAPTURE_LENGTH, MAX_CAPTURE_LENGTH),
            arguments(Integer.MAX_VALUE, MAX_CAPTURE_LENGTH));
    }

    private static List<Arguments> encodedLengthArgs()
    {
        return asList(
            arguments(0, LOG_HEADER_LENGTH),
            arguments(2, LOG_HEADER_LENGTH + 2),
            arguments(-LOG_HEADER_LENGTH, 0),
            arguments(Integer.MAX_VALUE, Integer.MIN_VALUE + LOG_HEADER_LENGTH - 1));
    }
}
