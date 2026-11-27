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
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.ReadablePosition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.Publication.ADMIN_ACTION;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.LogBufferDescriptor.LOG_META_DATA_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.activeTermCount;
import static io.aeron.logbuffer.LogBufferDescriptor.computeFragmentedFrameLength;
import static io.aeron.logbuffer.LogBufferDescriptor.computeTermBeginPosition;
import static io.aeron.logbuffer.LogBufferDescriptor.indexByTermCount;
import static io.aeron.logbuffer.LogBufferDescriptor.initialTermId;
import static io.aeron.logbuffer.LogBufferDescriptor.isConnected;
import static io.aeron.logbuffer.LogBufferDescriptor.mtuLength;
import static io.aeron.logbuffer.LogBufferDescriptor.nextPartitionIndex;
import static io.aeron.logbuffer.LogBufferDescriptor.packTail;
import static io.aeron.logbuffer.LogBufferDescriptor.positionBitsToShift;
import static io.aeron.logbuffer.LogBufferDescriptor.rawTail;
import static io.aeron.logbuffer.LogBufferDescriptor.storeDefaultFrameHeader;
import static io.aeron.protocol.DataHeaderFlyweight.BEGIN_FLAG;
import static io.aeron.protocol.DataHeaderFlyweight.END_FLAG;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.DataHeaderFlyweight.createDefaultHeader;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.align;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ExclusivePublicationUnitTest
{
    private static final int MTU_LENGTH = 1408;
    private static final long REGISTRATION_ID = 8;
    private static final int CHANNEL_STATUS_ID = 33;
    private static final int SESSION_ID = -3435643;
    private static final int STREAM_ID = 111;

    private final DataHeaderFlyweight defaultDataHeader = new DataHeaderFlyweight(createDefaultHeader(0, 0, 0));
    private final UnsafeBuffer dataBuffer = new UnsafeBuffer(new byte[5000]);
    private final UnsafeBuffer metadataBuffer = new UnsafeBuffer(new byte[LOG_META_DATA_LENGTH]);
    private final UnsafeBuffer[] termBuffers = new UnsafeBuffer[]
    {
        new UnsafeBuffer(new byte[TERM_MIN_LENGTH]),
        new UnsafeBuffer(new byte[TERM_MIN_LENGTH]),
        new UnsafeBuffer(new byte[TERM_MIN_LENGTH])
    };
    private final ClientConductor clientConductor = mock(ClientConductor.class);
    private final ReadablePosition positionLimit = mock(ReadablePosition.class);
    private final LogBuffers logBuffers = mock(LogBuffers.class);

    @BeforeEach
    void setUp()
    {
        when(logBuffers.metaDataBuffer()).thenReturn(metadataBuffer);
        when(logBuffers.duplicateTermBuffers()).thenReturn(termBuffers);
        when(logBuffers.termLength()).thenReturn(TERM_MIN_LENGTH);

        for (final UnsafeBuffer termBuffer : termBuffers)
        {
            termBuffer.setMemory(0, termBuffer.capacity(), (byte)0xFF);
        }
    }

    @ParameterizedTest
    @MethodSource("unfragmentedMethods")
    void unfragmentedOfferShouldPreZeroNextFrame(final OfferMethod method)
    {
        dataBuffer.putInt(0, Integer.MAX_VALUE);
        final int termOffset = 0;
        final ExclusivePublication publication = newPublication(0, 0, termOffset);

        assertEquals(64, method.execute(publication, dataBuffer, 0, SIZE_OF_INT));

        final UnsafeBuffer activeTerm = termBuffers[0];
        assertEquals(36, FrameDescriptor.frameLength(activeTerm, termOffset));
        assertEquals(HDR_TYPE_DATA, FrameDescriptor.frameType(activeTerm, termOffset));
        assertEquals(Integer.MAX_VALUE, activeTerm.getInt(32));
        assertEquals(0, FrameDescriptor.frameLength(activeTerm, 64));
    }

    @ParameterizedTest
    @MethodSource("unfragmentedMethods")
    void unfragmentedOfferShouldPreZeroFirstFrameInTheNextBufferOnAdminAction(final OfferMethod method)
    {
        dataBuffer.putInt(0, Integer.MIN_VALUE);
        final int termOffset = TERM_MIN_LENGTH - 32;
        final ExclusivePublication publication = newPublication(17, 4003, termOffset);
        final int activeIndex = indexByTermCount(activeTermCount(metadataBuffer));

        assertEquals(ADMIN_ACTION, method.execute(publication, dataBuffer, 0, SIZE_OF_INT));

        final UnsafeBuffer activeTerm = termBuffers[activeIndex];
        final UnsafeBuffer nextTerm = termBuffers[nextPartitionIndex(activeIndex)];
        assertEquals(32, FrameDescriptor.frameLength(activeTerm, termOffset));
        assertEquals(HDR_TYPE_PAD, FrameDescriptor.frameType(activeTerm, termOffset));
        assertEquals(0, FrameDescriptor.frameLength(nextTerm, 0));
    }

    @ParameterizedTest
    @MethodSource("unfragmentedMethods")
    void unfragmentedOfferShouldPreZeroFirstFrameInTheNextBufferIfCurrentMessageFitsTheBufferEnd(
        final OfferMethod method)
    {
        final int length = 100;
        final int frameLength = DataHeaderFlyweight.HEADER_LENGTH + length;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        dataBuffer.putInt(0, 42);
        final int termOffset = TERM_MIN_LENGTH - alignedFrameLength;
        final ExclusivePublication publication = newPublication(5, 19, termOffset);
        final long position = publication.position();
        final int activeIndex = indexByTermCount(activeTermCount(metadataBuffer));

        assertEquals(position + alignedFrameLength, method.execute(publication, dataBuffer, 0, length));

        final UnsafeBuffer activeTerm = termBuffers[activeIndex];
        final UnsafeBuffer nextTerm = termBuffers[nextPartitionIndex(activeIndex)];
        assertEquals(frameLength, FrameDescriptor.frameLength(activeTerm, termOffset));
        assertEquals(HDR_TYPE_DATA, FrameDescriptor.frameType(activeTerm, termOffset));
        assertEquals(42, activeTerm.getInt(termOffset + HEADER_LENGTH));
        assertEquals(0, FrameDescriptor.frameLength(nextTerm, 0));
    }

    @ParameterizedTest
    @MethodSource("fragmentedMethods")
    void fragmentedOfferShouldPreZeroNextFrame(final OfferMethod method)
    {
        final int length = dataBuffer.capacity();
        ThreadLocalRandom.current().nextBytes(dataBuffer.byteArray());
        final int termOffset = 128;
        final ExclusivePublication publication = newPublication(-3, 32, termOffset);
        final long position = publication.position();
        final int fragmentedLength = computeFragmentedFrameLength(length, publication.maxPayloadLength());
        final int activeIndex = indexByTermCount(activeTermCount(metadataBuffer));

        assertEquals(position + fragmentedLength, method.execute(publication, dataBuffer, 0, length));

        final UnsafeBuffer activeTerm = termBuffers[activeIndex];
        int offset = termOffset;
        while (offset < termOffset + fragmentedLength)
        {
            final int frameLength = FrameDescriptor.frameLength(activeTerm, offset);
            assertThat(fragmentedLength, greaterThan(0));
            assertEquals(HDR_TYPE_DATA, FrameDescriptor.frameType(activeTerm, offset));
            if (offset == termOffset)
            {
                assertEquals(BEGIN_FLAG, 0xFF & FrameDescriptor.frameFlags(activeTerm, offset));
            }
            else if (frameLength > publication.maxPayloadLength())
            {
                assertEquals(0, 0xFF & FrameDescriptor.frameFlags(activeTerm, offset));
            }
            else
            {
                assertEquals(END_FLAG, 0xFF & FrameDescriptor.frameFlags(activeTerm, offset));
                break;
            }
            offset += align(frameLength, FRAME_ALIGNMENT);
        }
        assertEquals(0, FrameDescriptor.frameLength(activeTerm, termOffset + fragmentedLength));
    }

    @ParameterizedTest
    @MethodSource("fragmentedMethods")
    void fragmentedOfferShouldPreZeroFirstFrameInTheNextBufferOnAdminAction(final OfferMethod method)
    {
        final int length = dataBuffer.capacity();
        dataBuffer.putInt(0, Integer.MIN_VALUE);
        final int termOffset = TERM_MIN_LENGTH - 32;
        final ExclusivePublication publication = newPublication(10, 10, termOffset);
        final int activeIndex = indexByTermCount(activeTermCount(metadataBuffer));

        assertEquals(ADMIN_ACTION, method.execute(publication, dataBuffer, 0, length));

        final UnsafeBuffer activeTerm = termBuffers[activeIndex];
        final UnsafeBuffer nextTerm = termBuffers[nextPartitionIndex(activeIndex)];
        assertEquals(32, FrameDescriptor.frameLength(activeTerm, termOffset));
        assertEquals(HDR_TYPE_PAD, FrameDescriptor.frameType(activeTerm, termOffset));
        assertEquals(0, FrameDescriptor.frameLength(nextTerm, 0));
    }

    @ParameterizedTest
    @MethodSource("fragmentedMethods")
    void fragmentedOfferShouldPreZeroFirstFrameInTheNextBufferIfCurrentMessageFitsTheBufferEnd(
        final OfferMethod method)
    {
        final int length = dataBuffer.capacity();
        final int frameLength = computeFragmentedFrameLength(length, MTU_LENGTH - HEADER_LENGTH);
        dataBuffer.putInt(0, 42);
        dataBuffer.putInt(length - SIZE_OF_INT, Integer.MIN_VALUE);
        final int termOffset = TERM_MIN_LENGTH - frameLength;
        final ExclusivePublication publication = newPublication(5, 19, termOffset);
        final long position = publication.position();
        final int activeIndex = indexByTermCount(activeTermCount(metadataBuffer));

        assertEquals(position + frameLength, method.execute(publication, dataBuffer, 0, length));

        final UnsafeBuffer activeTerm = termBuffers[activeIndex];
        final UnsafeBuffer nextTerm = termBuffers[nextPartitionIndex(activeIndex)];
        assertEquals(MTU_LENGTH, FrameDescriptor.frameLength(activeTerm, termOffset));
        assertEquals(HDR_TYPE_DATA, FrameDescriptor.frameType(activeTerm, termOffset));
        assertEquals(42, activeTerm.getInt(termOffset + HEADER_LENGTH));
        final int lastFragmentOffset = termOffset + 3 * MTU_LENGTH;
        final int lastFrameLength = FrameDescriptor.frameLength(activeTerm, lastFragmentOffset);
        assertEquals(
            Integer.MIN_VALUE,
            activeTerm.getInt(lastFragmentOffset + lastFrameLength - SIZE_OF_INT));

        assertEquals(0, FrameDescriptor.frameLength(nextTerm, 0));
    }

    @Test
    void appendPaddingShouldPreZeroNextFrame()
    {
        final int termOffset = 256;
        final ExclusivePublication publication = newPublication(0, 0, termOffset);

        assertEquals(320, publication.appendPadding(16));

        final UnsafeBuffer activeTerm = termBuffers[0];
        assertEquals(48, FrameDescriptor.frameLength(activeTerm, termOffset));
        assertEquals(HDR_TYPE_PAD, FrameDescriptor.frameType(activeTerm, termOffset));
        assertEquals(0, FrameDescriptor.frameLength(activeTerm, 320));
    }

    @Test
    void appendPaddingShouldPreZeroNextFrameAdminAction()
    {
        final int termOffset = TERM_MIN_LENGTH - 128;
        final ExclusivePublication publication = newPublication(11, 49, termOffset);
        final int activeIndex = indexByTermCount(activeTermCount(metadataBuffer));

        assertEquals(ADMIN_ACTION, publication.appendPadding(300));

        final UnsafeBuffer nextTerm = termBuffers[nextPartitionIndex(activeIndex)];
        assertEquals(0, FrameDescriptor.frameLength(nextTerm, 0));
    }

    @Test
    void appendPaddingShouldPreZeroFirstFrameInTheNextTermWhenEndsAtTheTermBoundary()
    {
        final int termOffset = TERM_MIN_LENGTH - 128;
        final ExclusivePublication publication = newPublication(11, 49, termOffset);
        final int activeIndex = indexByTermCount(activeTermCount(metadataBuffer));
        final long position = publication.position();

        assertEquals(position + 128, publication.appendPadding(96));

        final UnsafeBuffer activeTerm = termBuffers[activeIndex];
        final UnsafeBuffer nextTerm = termBuffers[nextPartitionIndex(activeIndex)];
        assertEquals(128, FrameDescriptor.frameLength(activeTerm, termOffset));
        assertEquals(HDR_TYPE_PAD, FrameDescriptor.frameType(activeTerm, termOffset));
        assertEquals(0, FrameDescriptor.frameLength(nextTerm, 0));
    }

    @Test
    void offerBlockShouldPreZeroNextFrameAfterBlockEnds()
    {
        final int termOffset = 256;
        final int length = 4096;
        ThreadLocalRandom.current().nextBytes(dataBuffer.byteArray());
        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
        for (int i = 0; i < 4096; i += 1024)
        {
            headerFlyweight.wrap(dataBuffer, i, 1024);
            headerFlyweight
                .termId(0)
                .sessionId(SESSION_ID)
                .streamId(STREAM_ID)
                .termOffset(termOffset + i)
                .headerType(HDR_TYPE_DATA)
                .frameLength(1024);
        }
        final ExclusivePublication publication = newPublication(0, 0, termOffset);

        assertEquals(termOffset + length, publication.offerBlock(dataBuffer, 0, length));

        final UnsafeBuffer activeTerm = termBuffers[0];
        assertEquals(1024, FrameDescriptor.frameLength(activeTerm, termOffset));
        assertEquals(HDR_TYPE_DATA, FrameDescriptor.frameType(activeTerm, termOffset));
        assertEquals(0, FrameDescriptor.frameLength(activeTerm, termOffset + length));
    }

    @Test
    void offerBlockShouldPreZeroFirstFrameInTheNextTerm()
    {
        final int length = 4096;
        final int termOffset = TERM_MIN_LENGTH - length;
        final int termId = 119;
        ThreadLocalRandom.current().nextBytes(dataBuffer.byteArray());
        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
        for (int i = 0; i < 4096; i += 1024)
        {
            headerFlyweight.wrap(dataBuffer, i, 1024);
            headerFlyweight
                .termId(termId)
                .sessionId(SESSION_ID)
                .streamId(STREAM_ID)
                .termOffset(termOffset + i)
                .headerType(HDR_TYPE_DATA)
                .frameLength(1024);
        }
        final ExclusivePublication publication = newPublication(5, termId, termOffset);
        final long position = publication.position();
        final int activeIndex = indexByTermCount(activeTermCount(metadataBuffer));

        assertEquals(position + length, publication.offerBlock(dataBuffer, 0, length));

        final UnsafeBuffer activeTerm = termBuffers[activeIndex];
        final UnsafeBuffer nextTerm = termBuffers[nextPartitionIndex(activeIndex)];
        assertEquals(1024, FrameDescriptor.frameLength(activeTerm, termOffset));
        assertEquals(HDR_TYPE_DATA, FrameDescriptor.frameType(activeTerm, termOffset));
        assertEquals(0, FrameDescriptor.frameLength(nextTerm, 0));
    }

    private ExclusivePublication newPublication(final int initialTermId, final int termId, final int termOffset)
    {
        initialTermId(metadataBuffer, initialTermId);
        final int termCount = termId - initialTermId;
        activeTermCount(metadataBuffer, termCount);
        rawTail(metadataBuffer, indexByTermCount(termCount), packTail(termId, termOffset));
        mtuLength(metadataBuffer, MTU_LENGTH);
        isConnected(metadataBuffer, true);

        final long limit =
            computeTermBeginPosition(termId, positionBitsToShift(TERM_MIN_LENGTH), initialTermId) + TERM_MIN_LENGTH;
        when(positionLimit.getVolatile()).thenReturn(limit);

        defaultDataHeader
            .sessionId(SESSION_ID)
            .streamId(STREAM_ID)
            .termId(initialTermId)
            .termOffset(0);
        storeDefaultFrameHeader(metadataBuffer, defaultDataHeader);

        return new ExclusivePublication(
            clientConductor,
            "aeron:ipc",
            STREAM_ID,
            SESSION_ID,
            positionLimit,
            CHANNEL_STATUS_ID,
            logBuffers,
            REGISTRATION_ID,
            REGISTRATION_ID);
    }

    private static long offer(
        final ExclusivePublication publication, final DirectBuffer buffer, final int offset, final int length)
    {
        return publication.offer(buffer, offset, length);
    }

    private static long offerReservedValue(
        final ExclusivePublication publication, final DirectBuffer buffer, final int offset, final int length)
    {
        return publication.offer(buffer, offset, length, null);
    }

    private static long offerTwoBuffers(
        final ExclusivePublication publication, final DirectBuffer buffer, final int offset, final int length)
    {
        return publication.offer(buffer, offset, length, buffer, 0, 0, null);
    }

    private static long offerVector(
        final ExclusivePublication publication, final DirectBuffer buffer, final int offset, final int length)
    {
        return publication.offer(new DirectBufferVector[]{ new DirectBufferVector(buffer, offset, length) });
    }

    private static long offerVectorReservedValue(
        final ExclusivePublication publication, final DirectBuffer buffer, final int offset, final int length)
    {
        return publication.offer(new DirectBufferVector[]{ new DirectBufferVector(buffer, offset, length) }, null);
    }

    private static long tryClaim(
        final ExclusivePublication publication, final DirectBuffer buffer, final int offset, final int length)
    {
        final BufferClaim bufferClaim = new BufferClaim();
        final long result = publication.tryClaim(length, bufferClaim);
        if (result > 0)
        {
            bufferClaim.buffer().putBytes(bufferClaim.offset(), buffer, offset, length);
            bufferClaim.commit();
        }
        return result;
    }

    private static List<OfferMethod> unfragmentedMethods()
    {
        return List.of(
            ExclusivePublicationUnitTest::offer,
            ExclusivePublicationUnitTest::offerReservedValue,
            ExclusivePublicationUnitTest::offerTwoBuffers,
            ExclusivePublicationUnitTest::offerVector,
            ExclusivePublicationUnitTest::offerVectorReservedValue,
            ExclusivePublicationUnitTest::tryClaim);
    }

    private static List<OfferMethod> fragmentedMethods()
    {
        return List.of(
            ExclusivePublicationUnitTest::offer,
            ExclusivePublicationUnitTest::offerReservedValue,
            ExclusivePublicationUnitTest::offerTwoBuffers,
            ExclusivePublicationUnitTest::offerVector,
            ExclusivePublicationUnitTest::offerVectorReservedValue);
    }

    @FunctionalInterface
    private interface OfferMethod
    {
        long execute(ExclusivePublication publication, DirectBuffer buffer, int offset, int length);
    }
}
