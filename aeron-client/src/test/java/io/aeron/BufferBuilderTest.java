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

import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.BufferBuilder.INIT_MIN_CAPACITY;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.logbuffer.LogBufferDescriptor.computeFragmentedFrameLength;
import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.HeaderFlyweight.*;
import static java.lang.Math.min;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

class BufferBuilderTest
{
    private final BufferBuilder bufferBuilder = new BufferBuilder();

    @ParameterizedTest
    @CsvSource({
        "0,0,4096", "4095,1,4096", "4096,7,4096", "100,200,4096", "101,5000,6144", "5000,8080,11250",
        "6666,11111,14998", "0,2147483639,2147483639", "2048,2147483647,2147483639", "5,1000000000000,2147483639" })
    void shouldFindSuitableCapacity(final int capacity, final long requiredCapacity, final int expected)
    {
        assertEquals(expected, BufferBuilder.findSuitableCapacity(capacity, requiredCapacity));
    }

    @Test
    void shouldInitialiseToDefaultValues()
    {
        assertEquals(0, bufferBuilder.capacity());
        assertEquals(0, bufferBuilder.limit());
        assertEquals(Aeron.NULL_VALUE, bufferBuilder.nextTermOffset());
        assertEquals(0, bufferBuilder.buffer().capacity());
        assertNull(bufferBuilder.buffer().byteBuffer());
        assertArrayEquals(ArrayUtil.EMPTY_BYTE_ARRAY, bufferBuilder.buffer().byteArray());
    }

    @Test
    void shouldGrowDirectBuffer()
    {
        final BufferBuilder builder = new BufferBuilder(0, true);
        assertEquals(0, builder.limit());
        assertEquals(0, builder.capacity());
        assertEquals(0, builder.buffer().capacity());

        final int appendedLength = 10;
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[appendedLength]);
        builder.append(srcBuffer, 0, srcBuffer.capacity());

        assertEquals(INIT_MIN_CAPACITY, builder.capacity());
        assertEquals(INIT_MIN_CAPACITY, builder.buffer().capacity());
        assertEquals(appendedLength, builder.limit());
        assertNotNull(builder.buffer().byteBuffer());
        assertTrue(builder.buffer().byteBuffer().isDirect());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldCreateHeaderBuffer(final boolean isDirect)
    {
        final BufferBuilder builder = new BufferBuilder(0, isDirect);
        assertNotNull(builder.buffer());
        assertNotNull(builder.headerBuffer);
        assertNotSame(builder.headerBuffer, builder.buffer());

        assertEquals(0, builder.capacity());
        assertEquals(0, builder.limit());
        assertEquals(0, builder.buffer().capacity());

        assertEquals(HEADER_LENGTH, builder.headerBuffer.capacity());
    }

    @ParameterizedTest
    @CsvSource({"0,0", "10,5", "20,20", "100,99", "111,111"})
    void shouldLimitBufferUpToCapacity(final int capacity, final int limit)
    {
        final BufferBuilder builder = new BufferBuilder(capacity);
        builder.limit(limit);

        assertEquals(capacity, builder.capacity());
        assertEquals(limit, builder.limit());
        assertEquals(capacity, builder.buffer().capacity());
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, Integer.MAX_VALUE, Integer.MAX_VALUE - 7 })
    void shouldThrowIllegalArgumentExceptionIfInitialCapacityIsOutOfRange(final int initialCapacity)
    {
        final IllegalArgumentException exception =
            assertThrowsExactly(IllegalArgumentException.class, () -> new BufferBuilder(initialCapacity));
        assertEquals(
            "initialCapacity outside range 0 - 2147483639: initialCapacity=" + initialCapacity, exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 1, 65, 1000000 })
    void shouldThrowIllegalArgumentExceptionIfLimitIsOutOfRange(final int limit)
    {
        final BufferBuilder builder = new BufferBuilder(0);
        final IllegalArgumentException exception =
            assertThrowsExactly(IllegalArgumentException.class, () -> builder.limit(limit));
        assertEquals("limit outside range: capacity=" + builder.capacity() + " limit=" + limit,
            exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MAX_VALUE, Integer.MAX_VALUE - 23 })
    void shouldThrowIllegalStateExceptionIfDataExceedsMaxCapacity(final int length)
    {
        final UnsafeBuffer src = new UnsafeBuffer(new byte[16]);
        src.putLong(0, Long.MAX_VALUE);
        src.putLong(SIZE_OF_LONG, Long.MIN_VALUE);

        final BufferBuilder builder = new BufferBuilder(64);
        builder.append(src, 0, src.capacity());
        assertEquals(src.capacity(), builder.limit());

        final IllegalStateException exception =
            assertThrowsExactly(IllegalStateException.class, () -> builder.append(src, 0, length));
        assertEquals(
            "insufficient capacity: maxCapacity=2147483639 limit=" + builder.limit() +
            " additionalLength=" + length, exception.getMessage());
    }

    @Test
    void shouldAppendNothingForZeroLength()
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[INIT_MIN_CAPACITY]);

        bufferBuilder.append(srcBuffer, 0, 0);

        assertEquals(0, bufferBuilder.limit());
        assertEquals(0, bufferBuilder.capacity());
    }

    @Test
    void shouldGrowToMultipleOfInitialCapacity()
    {
        final int srcCapacity = INIT_MIN_CAPACITY * 5;
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[srcCapacity]);

        bufferBuilder.append(srcBuffer, 0, srcBuffer.capacity());

        assertEquals(srcCapacity, bufferBuilder.limit());
        assertThat(bufferBuilder.capacity(), greaterThanOrEqualTo(srcCapacity));
    }

    @Test
    void shouldAppendThenReset()
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[INIT_MIN_CAPACITY]);

        bufferBuilder.append(srcBuffer, 0, srcBuffer.capacity());
        bufferBuilder.nextTermOffset(1024);

        assertEquals(srcBuffer.capacity(), bufferBuilder.limit());
        assertEquals(1024, bufferBuilder.nextTermOffset());

        bufferBuilder.reset();

        assertEquals(0, bufferBuilder.limit());
        assertEquals(Aeron.NULL_VALUE, bufferBuilder.nextTermOffset());
    }

    @Test
    void shouldAppendOneBufferWithoutResizing()
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[INIT_MIN_CAPACITY]);
        final byte[] bytes = "Hello World".getBytes(StandardCharsets.UTF_8);
        srcBuffer.putBytes(0, bytes, 0, bytes.length);

        bufferBuilder.append(srcBuffer, 0, bytes.length);

        final byte[] temp = new byte[bytes.length];
        bufferBuilder.buffer().getBytes(0, temp, 0, bytes.length);

        assertEquals(bytes.length, bufferBuilder.limit());
        assertEquals(INIT_MIN_CAPACITY, bufferBuilder.capacity());
        assertArrayEquals(temp, bytes);
    }

    @Test
    void shouldAppendTwoBuffersWithoutResizing()
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[INIT_MIN_CAPACITY]);
        final byte[] bytes = "1111111122222222".getBytes(StandardCharsets.UTF_8);
        srcBuffer.putBytes(0, bytes, 0, bytes.length);

        bufferBuilder.append(srcBuffer, 0, bytes.length / 2);
        bufferBuilder.append(srcBuffer, bytes.length / 2, bytes.length / 2);

        final byte[] temp = new byte[bytes.length];
        bufferBuilder.buffer().getBytes(0, temp, 0, bytes.length);

        assertEquals(bytes.length, bufferBuilder.limit());
        assertEquals(INIT_MIN_CAPACITY, bufferBuilder.capacity());
        assertArrayEquals(temp, bytes);
    }

    @Test
    void shouldFillBufferWithoutResizing()
    {
        final int bufferLength = 128;
        final byte[] buffer = new byte[bufferLength];
        Arrays.fill(buffer, (byte)7);
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(buffer);

        final BufferBuilder bufferBuilder = new BufferBuilder(bufferLength);
        final int initialCapacity = bufferBuilder.capacity();

        bufferBuilder.append(srcBuffer, 0, bufferLength);

        final byte[] temp = new byte[bufferLength];
        bufferBuilder.buffer().getBytes(0, temp, 0, bufferLength);

        assertEquals(bufferLength, bufferBuilder.limit());
        assertEquals(initialCapacity, bufferBuilder.capacity());
        assertArrayEquals(temp, buffer);
    }

    @Test
    void shouldResizeWhenBufferJustDoesNotFit()
    {
        final int bufferLength = 128;
        final byte[] buffer = new byte[bufferLength + 1];
        Arrays.fill(buffer, (byte)7);
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(buffer);

        final BufferBuilder bufferBuilder = new BufferBuilder(bufferLength);

        bufferBuilder.append(srcBuffer, 0, buffer.length);

        final byte[] temp = new byte[buffer.length];
        bufferBuilder.buffer().getBytes(0, temp, 0, buffer.length);

        assertEquals(buffer.length, bufferBuilder.limit());
        assertThat(bufferBuilder.capacity(), greaterThan(bufferLength));
        assertArrayEquals(temp, buffer);
    }

    @Test
    void shouldAppendTwoBuffersAndResize()
    {
        final int bufferLength = 128;
        final byte[] buffer = new byte[bufferLength];
        final int firstLength = buffer.length / 4;
        final int secondLength = buffer.length / 2;
        Arrays.fill(buffer, 0, firstLength + secondLength, (byte)7);
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(buffer);

        final BufferBuilder bufferBuilder = new BufferBuilder(bufferLength / 2);

        bufferBuilder.append(srcBuffer, 0, firstLength);
        bufferBuilder.append(srcBuffer, firstLength, secondLength);

        final byte[] temp = new byte[buffer.length];
        bufferBuilder.buffer().getBytes(0, temp, 0, secondLength + firstLength);

        assertEquals(firstLength + secondLength, bufferBuilder.limit());
        assertThat(bufferBuilder.capacity(), greaterThanOrEqualTo(firstLength + secondLength));
        assertArrayEquals(temp, buffer);
    }

    @Test
    void shouldCompactBufferToLowerLimit()
    {
        final int bufferLength = INIT_MIN_CAPACITY / 2;
        final byte[] buffer = new byte[bufferLength];
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(buffer);

        final BufferBuilder bufferBuilder = new BufferBuilder();

        final int bufferCount = 5;
        for (int i = 0; i < bufferCount; i++)
        {
            bufferBuilder.append(srcBuffer, 0, buffer.length);
        }

        final int expectedLimit = buffer.length * bufferCount;
        assertEquals(expectedLimit, bufferBuilder.limit());
        final int expandedCapacity = bufferBuilder.capacity();
        assertThat(expandedCapacity, greaterThan(expectedLimit));

        bufferBuilder.reset();

        bufferBuilder.append(srcBuffer, 0, buffer.length);
        bufferBuilder.append(srcBuffer, 0, buffer.length);
        bufferBuilder.append(srcBuffer, 0, buffer.length);

        bufferBuilder.compact();

        assertEquals(buffer.length * 3, bufferBuilder.limit());
        assertThat(bufferBuilder.capacity(), lessThan(expandedCapacity));
        assertEquals(bufferBuilder.limit(), bufferBuilder.capacity());
    }

    @Test
    void captureFirstHeaderShouldCopyTheEntireHeader()
    {
        final long reservedValue = Long.MAX_VALUE - 117;
        final int offset = 40;
        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
        headerFlyweight.wrap(new byte[100], offset, 50);
        headerFlyweight.frameLength(512);
        headerFlyweight.version((short)0xA);
        headerFlyweight.flags((short)0b1110_0111);
        headerFlyweight.headerType(HDR_TYPE_RTTM);
        headerFlyweight.termOffset(384);
        headerFlyweight.sessionId(-890);
        headerFlyweight.streamId(555);
        headerFlyweight.termId(42);
        headerFlyweight.reservedValue(reservedValue);

        final Header header = new Header(5, 3);
        header.buffer(new UnsafeBuffer(headerFlyweight.byteArray()));
        header.offset(headerFlyweight.wrapAdjustment());
        final DirectBuffer originalHeaderBuffer = header.buffer();

        assertSame(bufferBuilder, bufferBuilder.captureHeader(header));

        assertEquals(0, bufferBuilder.capacity());
        assertEquals(0, bufferBuilder.limit());
        assertSame(originalHeaderBuffer, header.buffer());
        assertEquals(headerFlyweight.wrapAdjustment(), header.offset());

        headerFlyweight.wrap(bufferBuilder.headerBuffer, 0, HEADER_LENGTH);
        assertEquals(512, bufferBuilder.completeHeader.frameLength());
        assertEquals((short)0xA, headerFlyweight.version());
        assertEquals((byte)0b1110_0111, bufferBuilder.completeHeader.flags());
        assertEquals(HDR_TYPE_RTTM, headerFlyweight.headerType());
        assertEquals(384, bufferBuilder.completeHeader.termOffset());
        assertEquals(-890, bufferBuilder.completeHeader.sessionId());
        assertEquals(555, bufferBuilder.completeHeader.streamId());
        assertEquals(42, bufferBuilder.completeHeader.termId());
        assertEquals(reservedValue, bufferBuilder.completeHeader.reservedValue());
    }

    @Test
    void shouldPrepareCompleteHeader()
    {
        final UnsafeBuffer data = new UnsafeBuffer(new byte[999]);

        final int frameLength = 128;
        final int termOffset = 1024;
        final short version = (short)15;
        final int type = HDR_TYPE_NAK;
        final int sessionId = 87;
        final int streamId = -9;
        final int termId = 10;
        final long reservedValue = 0xCAFE_BABE_DEAD_BEEFL;
        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
        headerFlyweight.wrap(new byte[44], 1, 39);
        headerFlyweight.frameLength(frameLength);
        headerFlyweight.version(version);
        headerFlyweight.flags((short)0b1011_1001);
        headerFlyweight.headerType(type);
        headerFlyweight.termOffset(termOffset);
        headerFlyweight.sessionId(sessionId);
        headerFlyweight.streamId(streamId);
        headerFlyweight.termId(termId);
        headerFlyweight.reservedValue(reservedValue);

        final Header header = new Header(4, 48);
        header.buffer(new UnsafeBuffer(headerFlyweight.byteArray()));
        header.offset(headerFlyweight.wrapAdjustment());

        assertSame(bufferBuilder, bufferBuilder.captureHeader(header));

        bufferBuilder.append(data, 0, frameLength - HEADER_LENGTH);
        bufferBuilder.append(data, 0, frameLength - HEADER_LENGTH);

        headerFlyweight.wrap(new byte[88], 19, 42);
        headerFlyweight.frameLength(frameLength);
        headerFlyweight.version((short)0xC);
        headerFlyweight.flags((short)0b0100_0100);
        headerFlyweight.headerType(HDR_TYPE_ATS_SETUP);
        headerFlyweight.termOffset(48);
        headerFlyweight.sessionId(999);
        headerFlyweight.streamId(4);
        headerFlyweight.termId(39);
        headerFlyweight.reservedValue(Long.MIN_VALUE);
        header.buffer(new UnsafeBuffer(headerFlyweight.byteArray()));
        header.offset(headerFlyweight.wrapAdjustment());
        final DirectBuffer originalHeaderBuffer = header.buffer();

        final Header completeHeader = bufferBuilder.completeHeader(header);
        assertNotSame(header, completeHeader);

        assertEquals(INIT_MIN_CAPACITY, bufferBuilder.capacity());
        assertEquals(2 * (frameLength - HEADER_LENGTH), bufferBuilder.limit());
        assertSame(originalHeaderBuffer, header.buffer());
        assertEquals(headerFlyweight.wrapAdjustment(), header.offset());
        assertNotSame(bufferBuilder.headerBuffer, bufferBuilder.buffer());
        assertSame(bufferBuilder.headerBuffer, completeHeader.buffer());
        assertEquals(0, completeHeader.offset());
        assertEquals(4, completeHeader.initialTermId());
        assertEquals(48, completeHeader.positionBitsToShift());
        assertEquals(HEADER_LENGTH + (2 * (frameLength - HEADER_LENGTH)), completeHeader.frameLength());
        assertEquals((byte)version, completeHeader.buffer().getByte(VERSION_FIELD_OFFSET));
        assertEquals((byte)0xFD, completeHeader.flags());
        assertNotEquals(0, completeHeader.flags() & BEGIN_FRAG_FLAG);
        assertEquals(type, completeHeader.type());
        assertEquals(termOffset, completeHeader.termOffset());
        assertEquals(sessionId, completeHeader.sessionId());
        assertEquals(streamId, completeHeader.streamId());
        assertEquals(termId, completeHeader.termId());
        assertEquals(reservedValue, completeHeader.reservedValue());
    }

    @ParameterizedTest
    @CsvSource({
        "1024, 1408",
        "1024, 128",
        "8192, 1408"
    })
    void shouldCalculatePositionAndFrameLengthWhenReassembling(final int totalPayloadLength, final int mtu)
    {
        final int maxPayloadLength = mtu - HEADER_LENGTH;
        final int termOffset = 1024;
        final int termId = 10;
        final int initialTermId = 4;
        final int positionBitsToShift = 16;

        final int expectedFrameLength = HEADER_LENGTH + totalPayloadLength;
        final int fragmentedFrameLength = computeFragmentedFrameLength(totalPayloadLength, maxPayloadLength);
        final long startPosition = computePosition(termId, termOffset, positionBitsToShift, initialTermId);
        final long expectedPosition = startPosition + fragmentedFrameLength;

        final UnsafeBuffer data = new UnsafeBuffer(new byte[totalPayloadLength]);

        final DataHeaderFlyweight firstHeader = new DataHeaderFlyweight();
        firstHeader.wrap(new byte[32]);
        firstHeader.frameLength(HEADER_LENGTH + maxPayloadLength);
        firstHeader.version((short)15);
        firstHeader.flags((short)0b1011_1001);
        firstHeader.headerType(HDR_TYPE_DATA);
        firstHeader.termOffset(termOffset);
        firstHeader.sessionId(87);
        firstHeader.streamId(-9);
        firstHeader.termId(termId);
        firstHeader.reservedValue(0xCAFE_BABE_DEAD_BEEFL);

        final Header header = new Header(initialTermId, positionBitsToShift);
        header.buffer(new UnsafeBuffer(firstHeader.byteArray()));
        header.offset(firstHeader.wrapAdjustment());

        bufferBuilder.captureHeader(header);

        int lastPayloadLength = 0;
        for (int position = 0; position < data.capacity(); position += maxPayloadLength)
        {
            lastPayloadLength = min(maxPayloadLength, data.capacity() - position);
            bufferBuilder.append(data, position, lastPayloadLength);
        }

        assertNotEquals(0, lastPayloadLength);

        final int valueIsIgnored = Integer.MIN_VALUE;

        final DataHeaderFlyweight lastHeader = new DataHeaderFlyweight();
        lastHeader.wrap(new byte[32]);
        lastHeader.frameLength(HEADER_LENGTH + lastPayloadLength);
        lastHeader.version((short)15);
        lastHeader.flags((short)0b1011_1001);
        lastHeader.headerType(HDR_TYPE_DATA);
        lastHeader.termOffset(valueIsIgnored);
        lastHeader.sessionId(87);
        lastHeader.streamId(-9);
        lastHeader.termId(10);
        lastHeader.reservedValue(0xCAFE_BABE_DEAD_BEEFL);
        header.buffer(new UnsafeBuffer(lastHeader.byteArray()));
        header.offset(lastHeader.wrapAdjustment());

        final Header completeHeader = bufferBuilder.completeHeader(header);

        assertEquals(expectedFrameLength, completeHeader.frameLength());
        assertEquals(fragmentedFrameLength, completeHeader.fragmentedFrameLength());
        assertEquals(expectedPosition, completeHeader.position());
    }

    @Test
    void compactEmptyBufferIsAnOp()
    {
        assertSame(bufferBuilder, bufferBuilder.compact());

        assertEquals(0, bufferBuilder.capacity());
        assertEquals(0, bufferBuilder.limit());
        assertEquals(0, bufferBuilder.buffer().capacity());
    }

    @Test
    void compactIsANoOpIfTheCapacityIsNotDecreasing()
    {
        assertSame(bufferBuilder, bufferBuilder.append(new UnsafeBuffer(new byte[5]), 0, 3));
        assertEquals(3, bufferBuilder.limit());
        assertEquals(INIT_MIN_CAPACITY, bufferBuilder.capacity());
        final MutableDirectBuffer originalBuffer = bufferBuilder.buffer();
        assertEquals(INIT_MIN_CAPACITY, originalBuffer.capacity());

        assertSame(bufferBuilder, bufferBuilder.compact());

        assertEquals(3, bufferBuilder.limit());
        assertEquals(INIT_MIN_CAPACITY, bufferBuilder.capacity());
        assertSame(originalBuffer, bufferBuilder.buffer());
        assertEquals(INIT_MIN_CAPACITY, originalBuffer.capacity());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldNotChangeHeaderBufferUponResize(final boolean isDirect)
    {
        final BufferBuilder builder = new BufferBuilder(0, isDirect);
        final UnsafeBuffer headerBuffer = builder.headerBuffer;
        final ByteBuffer headerByteBuffer = headerBuffer.byteBuffer();
        final byte[] headerByteArray = headerBuffer.byteArray();
        headerBuffer.setMemory(0, headerBuffer.capacity(), (byte)0xFA);

        final UnsafeBuffer src = new UnsafeBuffer(new byte[100]);
        ThreadLocalRandom.current().nextBytes(src.byteArray());
        builder.append(src, 0, src.capacity());

        assertSame(headerBuffer, builder.headerBuffer);
        assertSame(headerByteBuffer, builder.headerBuffer.byteBuffer());
        assertSame(headerByteArray, builder.headerBuffer.byteArray());
        for (int i = 0; i < headerBuffer.capacity(); i++)
        {
            assertEquals((byte)0xFA, headerBuffer.getByte(i));
        }
    }
}
