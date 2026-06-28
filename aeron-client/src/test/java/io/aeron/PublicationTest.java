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

import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.ReadablePosition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.Publication.*;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.align;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PublicationTest
{
    private static final String CHANNEL = "aeron:udp?endpoint=localhost:40124";
    private static final int STREAM_ID_1 = 1002;
    private static final int SESSION_ID_1 = 13;
    private static final int TERM_ID_1 = 1;
    private static final int CORRELATION_ID = 2000;
    private static final int SEND_BUFFER_CAPACITY = 1024;
    private static final int PARTITION_INDEX = 0;
    private static final int MTU_LENGTH = 4096;
    private static final int PAGE_SIZE = 4 * 1024;
    private static final int TERM_LENGTH = TERM_MIN_LENGTH * 4;
    private static final int MAX_PAYLOAD_SIZE = MTU_LENGTH - HEADER_LENGTH;
    private static final int MAX_MESSAGE_SIZE = FrameDescriptor.computeMaxMessageLength(TERM_LENGTH);
    private static final int TOTAL_ALIGNED_MAX_MESSAGE_SIZE = (MAX_MESSAGE_SIZE / MAX_PAYLOAD_SIZE) * MTU_LENGTH +
        align(MAX_MESSAGE_SIZE % MAX_PAYLOAD_SIZE + HEADER_LENGTH, FRAME_ALIGNMENT);
    private static final int POSITION_BITS_TO_SHIFT = LogBufferDescriptor.positionBitsToShift(TERM_LENGTH);
    private static final int DEFAULT_FRAME_TYPE = DataHeaderFlyweight.HDR_TYPE_RTTM;
    private static final int SESSION_ID = 42;
    private static final int STREAM_ID = 111;
    private final UnsafeBuffer logMetaDataBuffer = spy(new UnsafeBuffer(allocate(LOG_META_DATA_LENGTH)));
    private final UnsafeBuffer[] termBuffers = new UnsafeBuffer[PARTITION_COUNT];
    private final ClientConductor conductor = mock(ClientConductor.class);
    private final LogBuffers logBuffers = mock(LogBuffers.class);
    private final ReadablePosition publicationLimit = mock(ReadablePosition.class);
    private ConcurrentPublication publication;

    @BeforeEach
    void setUp()
    {
        when(publicationLimit.getVolatile()).thenReturn(2L * SEND_BUFFER_CAPACITY);
        when(logBuffers.duplicateTermBuffers()).thenReturn(termBuffers);
        when(logBuffers.termLength()).thenReturn(TERM_LENGTH);
        when(logBuffers.metaDataBuffer()).thenReturn(logMetaDataBuffer);

        final UnsafeBuffer defaultHeader = DataHeaderFlyweight.createDefaultHeader(SESSION_ID, STREAM_ID, TERM_ID_1);
        defaultHeader.putShort(DataHeaderFlyweight.TYPE_FIELD_OFFSET, (short)DEFAULT_FRAME_TYPE, LITTLE_ENDIAN);
        storeDefaultFrameHeader(logMetaDataBuffer, defaultHeader);

        initialTermId(logMetaDataBuffer, TERM_ID_1);
        mtuLength(logMetaDataBuffer, MTU_LENGTH);
        termLength(logMetaDataBuffer, TERM_LENGTH);
        pageSize(logMetaDataBuffer, PAGE_SIZE);
        isConnected(logMetaDataBuffer, false);

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termBuffers[i] = new UnsafeBuffer(allocateDirect(TERM_LENGTH));
        }

        publication = new ConcurrentPublication(
            conductor,
            CHANNEL,
            STREAM_ID_1,
            SESSION_ID_1,
            publicationLimit,
            ChannelEndpointStatus.NO_ID_ALLOCATED,
            logBuffers,
            CORRELATION_ID,
            CORRELATION_ID);

        initialiseTailWithTermId(logMetaDataBuffer, PARTITION_INDEX, TERM_ID_1);

        doAnswer(
            (invocation) ->
            {
                publication.internalClose();
                return null;
            }).when(conductor).removePublication(publication);
    }

    @Test
    void shouldEnsureThePublicationIsOpenBeforeReadingPosition()
    {
        publication.close();
        assertEquals(CLOSED, publication.position());

        verify(conductor).removePublication(publication);
    }

    @Test
    void shouldReportThatPublicationHasNotBeenConnectedYet()
    {
        when(publicationLimit.getVolatile()).thenReturn(0L);
        isConnected(logMetaDataBuffer, false);
        assertFalse(publication.isConnected());
    }

    @Test
    void shouldReportThatPublicationHasBeenConnectedYet()
    {
        isConnected(logMetaDataBuffer, true);
        assertTrue(publication.isConnected());
    }

    @Test
    void shouldReportInitialPosition()
    {
        assertEquals(0L, publication.position());
    }

    @Test
    void shouldReportMaxMessageLength()
    {
        assertEquals(FrameDescriptor.computeMaxMessageLength(TERM_LENGTH), publication.maxMessageLength());
    }

    @Test
    void shouldRemovePublicationOnClose()
    {
        publication.close();

        verify(conductor).removePublication(publication);
    }

    @Test
    void shouldReturnErrorMessages()
    {
        assertEquals("NOT_CONNECTED", errorString(-1L));
        assertEquals("BACK_PRESSURED", errorString(-2L));
        assertEquals("ADMIN_ACTION", errorString(-3L));
        assertEquals("CLOSED", errorString(-4L));
        assertEquals("MAX_POSITION_EXCEEDED", errorString(-5L));
        assertEquals("NONE", errorString(0L));
        assertEquals("NONE", errorString(1L));
        assertEquals("UNKNOWN", errorString(-6L));
        assertEquals("UNKNOWN", errorString(Long.MIN_VALUE));
    }

    abstract class BaseTests
    {
        abstract long invoke(int length, ReservedValueSupplier reservedValueSupplier);

        abstract void onError(UnsafeBuffer termBuffer, int termOffset, int length);

        abstract void onSuccess(UnsafeBuffer termBuffer, int termOffset, int length);

        @Test
        void returnsClosedIfPublicationWasClosed()
        {
            final int length = 6;
            final int termOffset = 0;
            publication.close();

            assertEquals(CLOSED, invoke(length, null));

            onError(termBuffers[PARTITION_INDEX], termOffset, length);
            assertFrameType(PARTITION_INDEX, termOffset, FrameDescriptor.PADDING_FRAME_TYPE);
            assertFrameLength(PARTITION_INDEX, termOffset, 0);
        }

        @Test
        void returnsAdminActionIfTermCountAndTermIdDoNotMatch()
        {
            final int termCount = 9;
            final int termOffset = 0;
            final int length = 10;
            assertEquals(indexByTermCount(termCount), PARTITION_INDEX);
            initialiseTailWithTermId(logMetaDataBuffer, PARTITION_INDEX, TERM_ID_1 + 5);

            assertEquals(ADMIN_ACTION, invoke(length, null));

            onError(termBuffers[PARTITION_INDEX], termOffset, length);
            assertFrameType(PARTITION_INDEX, termOffset, FrameDescriptor.PADDING_FRAME_TYPE);
            assertFrameLength(PARTITION_INDEX, termOffset, 0);
        }

        @Test
        void returnsMaxPositionExceededIfPublicationLimitReached()
        {
            final int partitionIndex = 1;
            final int termOffset = TERM_LENGTH - 140;
            final int length = 100;
            when(publicationLimit.getVolatile()).thenReturn(16L);
            rawTail(logMetaDataBuffer, partitionIndex, packTail(Integer.MIN_VALUE, termOffset));
            activeTermCount(logMetaDataBuffer, Integer.MAX_VALUE);
            isConnected(logMetaDataBuffer, true);

            assertEquals(MAX_POSITION_EXCEEDED, invoke(length, null));

            onError(termBuffers[partitionIndex], termOffset, length);
            assertFrameType(partitionIndex, termOffset, FrameDescriptor.PADDING_FRAME_TYPE);
            assertFrameLength(partitionIndex, termOffset, 0);
        }

        @Test
        void returnsBackPressuredIfPublicationLimitReached()
        {
            final int partitionIndex = 1;
            final int termOffset = 64;
            final int length = 11;
            when(publicationLimit.getVolatile()).thenReturn(16L);
            rawTail(logMetaDataBuffer, partitionIndex, packTail(TERM_ID_1 + 1, termOffset));
            activeTermCount(logMetaDataBuffer, 1);
            isConnected(logMetaDataBuffer, true);

            assertEquals(BACK_PRESSURED, invoke(length, (termBuffer, termOffset1, frameLength) -> Long.MAX_VALUE));

            final UnsafeBuffer termBuffer = termBuffers[partitionIndex];
            onError(termBuffer, termOffset, length);
            assertFrameType(partitionIndex, termOffset, FrameDescriptor.PADDING_FRAME_TYPE);
            assertFrameLength(partitionIndex, termOffset, 0);
            assertEquals(0, DataHeaderFlyweight.reservedValue(termBuffer, termOffset));
        }

        @Test
        void returnsNotConnectedIfPublicationLimitReachedAndNotConnected()
        {
            final int partitionIndex = 1;
            final int termOffset = 0;
            final int length = 10;
            when(publicationLimit.getVolatile()).thenReturn(16L);
            initialiseTailWithTermId(logMetaDataBuffer, partitionIndex, TERM_ID_1 + 1);
            activeTermCount(logMetaDataBuffer, 1);

            assertEquals(NOT_CONNECTED, invoke(length, null));

            onError(termBuffers[partitionIndex], termOffset, length);
            assertFrameType(partitionIndex, termOffset, FrameDescriptor.PADDING_FRAME_TYPE);
            assertFrameLength(partitionIndex, termOffset, 0);
        }

        @Test
        void returnsAdminActionIfTermWasRotated()
        {
            final int termOffset = TERM_LENGTH - 64;
            final int length = 60;
            when(publicationLimit.getVolatile()).thenReturn(1L + Integer.MAX_VALUE);
            rawTailVolatile(logMetaDataBuffer, 0, packTail(TERM_ID_1, termOffset));
            rawTailVolatile(logMetaDataBuffer, 1, packTail(TERM_ID_1 + 1 - PARTITION_COUNT, 555));
            rawTailVolatile(logMetaDataBuffer, 2, packTail(STREAM_ID, 777));

            assertEquals(ADMIN_ACTION, invoke(length, null));

            onError(termBuffers[0], termOffset, length);
            assertFrameType(0, termOffset, FrameDescriptor.PADDING_FRAME_TYPE);
            assertFrameLength(0, termOffset, 64);
            assertEquals(packTail(TERM_ID_1, termOffset + 96), rawTail(logMetaDataBuffer, 0));
            assertEquals(packTail(TERM_ID_1 + 1, 0), rawTail(logMetaDataBuffer, 1));
            assertEquals(packTail(STREAM_ID, 777), rawTail(logMetaDataBuffer, 2));
        }

        @Test
        void returnsMaxPositionExceededIfThereIsNotEnoughSpaceLeft()
        {
            final int partitionIndex = 1;
            final int termOffset = TERM_LENGTH - 128;
            final int length = 96;
            when(publicationLimit.getVolatile()).thenReturn(1L + Integer.MAX_VALUE);
            rawTail(logMetaDataBuffer, partitionIndex, packTail(Integer.MIN_VALUE, termOffset));
            rawTail(logMetaDataBuffer, 0, packTail(55, 55));
            rawTail(logMetaDataBuffer, 2, packTail(222, 222));
            activeTermCount(logMetaDataBuffer, Integer.MAX_VALUE);
            isConnected(logMetaDataBuffer, true);

            assertEquals(MAX_POSITION_EXCEEDED, invoke(length, null));

            onError(termBuffers[partitionIndex], termOffset, length);
            assertFrameType(partitionIndex, termOffset, FrameDescriptor.PADDING_FRAME_TYPE);
            assertFrameLength(partitionIndex, termOffset, 0);
            assertEquals(packTail(Integer.MIN_VALUE, termOffset), rawTail(logMetaDataBuffer, partitionIndex));
            assertEquals(packTail(55, 55), rawTail(logMetaDataBuffer, 0));
            assertEquals(packTail(222, 222), rawTail(logMetaDataBuffer, 2));
        }

        void testPositionUponSuccess(
            final int length,
            final int termId,
            final int termCount,
            final long tailAfterUpdate,
            final long expectedPosition)
        {
            when(publicationLimit.getVolatile()).thenReturn(Long.MAX_VALUE);
            isConnected(logMetaDataBuffer, true);
            activeTermCount(logMetaDataBuffer, termCount);
            final int partitionIndex = indexByTermCount(termCount);
            final long rawTail = packTail(termId, 192);
            rawTail(logMetaDataBuffer, partitionIndex, rawTail);
            doAnswer((Answer<Long>)invocation -> tailAfterUpdate)
                .when(logMetaDataBuffer)
                .getAndAddLong(anyInt(), anyLong());

            final long position = invoke(length, ((termBuffer, termOffset, frameLength) -> termOffset ^ frameLength));

            assertEquals(expectedPosition, position);
            final UnsafeBuffer buffer = termBuffers[partitionIndex];
            final int termOffset = termOffset(tailAfterUpdate);
            assertFrameType(partitionIndex, termOffset, DEFAULT_FRAME_TYPE);
            assertEquals(SESSION_ID, FrameDescriptor.frameSessionId(buffer, termOffset));
            assertEquals(STREAM_ID, DataHeaderFlyweight.streamId(buffer, termOffset));
            assertEquals(termId(tailAfterUpdate), DataHeaderFlyweight.termId(buffer, termOffset));
            onSuccess(buffer, termOffset, length);
        }
    }

    @Nested
    class TryClaim extends BaseTests
    {
        private final BufferClaim bufferClaim = new BufferClaim();

        long invoke(final int length, final ReservedValueSupplier reservedValueSupplier)
        {
            return publication.tryClaim(length, bufferClaim);
        }

        void onError(final UnsafeBuffer termBuffer, final int termOffset, final int length)
        {
            final MutableDirectBuffer buffer = bufferClaim.buffer();
            assertEquals(0, buffer.capacity());
        }

        void onSuccess(final UnsafeBuffer termBuffer, final int termOffset, final int length)
        {
            assertEquals(-(length + HEADER_LENGTH), DataHeaderFlyweight.fragmentLength(termBuffer, termOffset));
            assertEquals(0, DataHeaderFlyweight.reservedValue(termBuffer, termOffset));
            assertEquals(length, bufferClaim.length());
        }

        @ParameterizedTest
        @MethodSource("io.aeron.PublicationTest#tryClaimPositions")
        void tryClaimShouldReturnPositionAtWhichTheClaimedSpaceEnds(
            final int length,
            final int termId,
            final int termCount,
            final long tailAfterUpdate,
            final long expectedPosition)
        {
            testPositionUponSuccess(
                length,
                termId,
                termCount,
                tailAfterUpdate,
                expectedPosition);
        }
    }

    abstract class OfferBase extends BaseTests
    {
        abstract UnsafeBuffer buffer(int processedBytes);

        void onError(final UnsafeBuffer termBuffer, final int termOffset, final int length)
        {
            int offset = termOffset + HEADER_LENGTH;
            for (int i = 0, capacity = termBuffer.capacity(); offset < capacity && i < length; i++, offset++)
            {
                assertEquals(0, termBuffer.getByte(offset));
            }
        }

        void onSuccess(final UnsafeBuffer termBuffer, final int termOffset, final int length)
        {
            int index = 0, processedBytes = 0;
            int offset = termOffset;
            UnsafeBuffer dataBuffer = buffer(0);
            while (processedBytes < length)
            {
                final int frameLength = FrameDescriptor.frameLength(termBuffer, offset);
                if (frameLength <= 0)
                {
                    break;
                }

                final int chunkLength = frameLength - HEADER_LENGTH;
                final byte frameFlags = FrameDescriptor.frameFlags(termBuffer, offset);

                if (0 == processedBytes)
                {
                    assertEquals(FrameDescriptor.BEGIN_FRAG_FLAG, (frameFlags & FrameDescriptor.BEGIN_FRAG_FLAG));
                }

                if (length == processedBytes + chunkLength)
                {
                    assertEquals(FrameDescriptor.END_FRAG_FLAG, (frameFlags & FrameDescriptor.END_FRAG_FLAG));
                }

                assertEquals(offset ^ frameLength, DataHeaderFlyweight.reservedValue(termBuffer, offset));
                offset += HEADER_LENGTH;

                for (int i = 0; i < chunkLength; i++)
                {
                    assertEquals(dataBuffer.getByte(index++), termBuffer.getByte(offset++));
                    final UnsafeBuffer nextBuffer = buffer(++processedBytes);
                    if (nextBuffer != dataBuffer)
                    {
                        dataBuffer = nextBuffer;
                        index = 0;
                    }
                }
            }
            assertEquals(length, processedBytes);
        }

        @ParameterizedTest
        @MethodSource("io.aeron.PublicationTest#offerPositions")
        void returnsPositionAfterDataIsCopied(
            final int length,
            final int termId,
            final int termCount,
            final long tailAfterUpdate,
            final long expectedPosition)
        {
            testPositionUponSuccess(
                length,
                termId,
                termCount,
                tailAfterUpdate,
                expectedPosition);
        }
    }

    @Nested
    class Offer extends OfferBase
    {
        private UnsafeBuffer sendBuffer;

        @BeforeEach
        void before()
        {
            final byte[] bytes = new byte[MAX_MESSAGE_SIZE];
            ThreadLocalRandom.current().nextBytes(bytes);
            sendBuffer = new UnsafeBuffer(bytes);
        }

        UnsafeBuffer buffer(final int length)
        {
            return sendBuffer;
        }

        long invoke(final int length, final ReservedValueSupplier reservedValueSupplier)
        {
            return publication.offer(sendBuffer, 0, length, reservedValueSupplier);
        }

        @Test
        void offerWithAnOffset()
        {
            final int partitionIndex = 2;
            final int termId = TERM_ID_1 + 2;
            final int termOffset = 992;
            final int offset = 11;
            final int length = 23;
            when(publicationLimit.getVolatile()).thenReturn(Long.MAX_VALUE);
            isConnected(logMetaDataBuffer, true);
            rawTail(logMetaDataBuffer, partitionIndex, packTail(termId, termOffset));
            activeTermCount(logMetaDataBuffer, 2);

            final long position = publication.offer(
                sendBuffer, offset, length, (termBuffer, frameOffset, frameLength) -> Long.MIN_VALUE);

            assertEquals(525344, position);
            final UnsafeBuffer termBuffer = termBuffers[partitionIndex];
            assertEquals(Long.MIN_VALUE, DataHeaderFlyweight.reservedValue(termBuffer, termOffset));
            for (int i = 0; i < length; i++)
            {
                assertEquals(sendBuffer.getByte(offset + i), termBuffer.getByte(termOffset + HEADER_LENGTH + i));
            }
        }
    }

    @Nested
    class OfferWithTwoBuffers extends OfferBase
    {
        private UnsafeBuffer buffer1;
        private UnsafeBuffer buffer2;

        @BeforeEach
        void before()
        {
        }

        UnsafeBuffer buffer(final int processedBytes)
        {
            return processedBytes < buffer1.capacity() ? buffer1 : buffer2;
        }

        long invoke(final int length, final ReservedValueSupplier reservedValueSupplier)
        {
            final byte[] bytes = new byte[length];
            ThreadLocalRandom.current().nextBytes(bytes);
            final int chunk1Length = (int)(length * 0.25);
            final int chunk2Length = length - chunk1Length;
            buffer1 = new UnsafeBuffer(bytes, 0, chunk1Length);
            buffer2 = new UnsafeBuffer(bytes, chunk1Length, chunk2Length);

            return publication.offer(buffer1, 0, chunk1Length, buffer2, 0, chunk2Length, reservedValueSupplier);
        }

        @Test
        void offerWithOffsets()
        {
            final int partitionIndex = 1;
            final int termId = TERM_ID_1 + 1;
            final int termOffset = 64;
            when(publicationLimit.getVolatile()).thenReturn(Long.MAX_VALUE);
            isConnected(logMetaDataBuffer, true);
            rawTail(logMetaDataBuffer, partitionIndex, packTail(termId, termOffset));
            activeTermCount(logMetaDataBuffer, 1);

            final byte[] bytes = new byte[16];
            ThreadLocalRandom.current().nextBytes(bytes);
            final UnsafeBuffer buffer1 = new UnsafeBuffer(bytes, 0, 8);
            final UnsafeBuffer buffer2 = new UnsafeBuffer(bytes, 8, 8);

            final int lengthOne = 5;
            final int offsetOne = 2;
            final int lengthTwo = 3;
            final int offsetTwo = 4;
            final long position = publication.offer(
                buffer1,
                offsetOne,
                lengthOne,
                buffer2,
                offsetTwo,
                lengthTwo,
                (termBuffer, frameOffset, frameLength) -> -1);

            assertEquals(262272, position);
            final UnsafeBuffer termBuffer = termBuffers[partitionIndex];
            assertEquals(-1, DataHeaderFlyweight.reservedValue(termBuffer, termOffset));
            for (int i = 0; i < lengthOne; i++)
            {
                assertEquals(buffer1.getByte(offsetOne + i), termBuffer.getByte(termOffset + HEADER_LENGTH + i));
            }
            for (int i = 0; i < lengthTwo; i++)
            {
                assertEquals(
                    buffer2.getByte(offsetTwo + i), termBuffer.getByte(termOffset + HEADER_LENGTH + lengthOne + i));
            }
        }
    }

    @Nested
    class VectorOffer extends OfferBase
    {
        private final DirectBufferVector[] vectors = new DirectBufferVector[3];

        UnsafeBuffer buffer(final int processedBytes)
        {
            return (UnsafeBuffer)vectors[0].buffer();
        }

        long invoke(final int length, final ReservedValueSupplier reservedValueSupplier)
        {
            final byte[] bytes = new byte[length];
            ThreadLocalRandom.current().nextBytes(bytes);
            final int numVectors = vectors.length;
            final int chunkSize = length / numVectors;
            final UnsafeBuffer buffer = new UnsafeBuffer(bytes);
            for (int i = 0, offset = 0; i < numVectors; i++)
            {
                final int size = numVectors - 1 == i ? (length - offset) : chunkSize;
                vectors[i] = new DirectBufferVector(buffer, offset, size);
                offset += size;
            }

            return publication.offer(vectors, reservedValueSupplier);
        }
    }

    void assertFrameType(final int partitionIndex, final int termOffset, final int expectedFrameType)
    {
        assertEquals(expectedFrameType, FrameDescriptor.frameType(termBuffers[partitionIndex], termOffset));
    }

    private void assertFrameLength(final int partitionIndex, final int termOffset, final int expectedFrameLength)
    {
        assertEquals(expectedFrameLength, FrameDescriptor.frameLength(termBuffers[partitionIndex], termOffset));
    }

    private static List<Arguments> tryClaimPositions()
    {
        return Arrays.asList(
            Arguments.of(
                100,
                31,
                30,
                packTail(31, 16 * 1024),
                computePosition(31, 16 * 1024 + 160, POSITION_BITS_TO_SHIFT, TERM_ID_1)),
            Arguments.of(
                999,
                7,
                6,
                packTail(212, 8192),
                computePosition(212, 8192 + 1056, POSITION_BITS_TO_SHIFT, TERM_ID_1)));
    }

    private static List<Arguments> offerPositions()
    {
        return Arrays.asList(
            Arguments.of(
                124,
                5,
                4,
                packTail(11, 3072),
                computePosition(11, 3072 + 160, POSITION_BITS_TO_SHIFT, TERM_ID_1)),
            Arguments.of(
                MAX_MESSAGE_SIZE,
                77,
                76,
                packTail(77, 1024),
                computePosition(77, 1024 + TOTAL_ALIGNED_MAX_MESSAGE_SIZE, POSITION_BITS_TO_SHIFT, TERM_ID_1)));
    }
}
