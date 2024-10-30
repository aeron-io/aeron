/*
 * Copyright 2014-2024 Real Logic Limited.
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

import io.aeron.logbuffer.*;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicLongPosition;
import org.agrona.concurrent.status.Position;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.AdditionalMatchers;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.nio.ByteBuffer.allocateDirect;
import static org.agrona.BitUtil.align;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class ImageTest
{
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int POSITION_BITS_TO_SHIFT = LogBufferDescriptor.positionBitsToShift(TERM_BUFFER_LENGTH);
    private static final byte[] DATA = new byte[36];

    static
    {
        for (int i = 0; i < DATA.length; i++)
        {
            DATA[i] = (byte)i;
        }
    }

    private static final long CORRELATION_ID = 0xC044E1AL;
    private static final int SESSION_ID = 0x5E55101D;
    private static final int STREAM_ID = 0xC400E;
    private static final String SOURCE_IDENTITY = "ipc";
    private static final int INITIAL_TERM_ID = 0xEE81D;
    private static final int MESSAGE_LENGTH = HEADER_LENGTH + DATA.length;
    private static final int ALIGNED_FRAME_LENGTH = align(MESSAGE_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);

    private final UnsafeBuffer rcvBuffer = new UnsafeBuffer(new byte[ALIGNED_FRAME_LENGTH]);
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final FragmentHandler mockFragmentHandler = mock(FragmentHandler.class);
    private final ControlledFragmentHandler mockControlledFragmentHandler = mock(ControlledFragmentHandler.class);
    private final Position position = spy(new AtomicLongPosition());
    private final LogBuffers logBuffers = mock(LogBuffers.class);
    private final ErrorHandler errorHandler = mock(ErrorHandler.class);
    private final Subscription subscription = mock(Subscription.class);

    private final UnsafeBuffer[] termBuffers = new UnsafeBuffer[PARTITION_COUNT];

    @BeforeEach
    void setUp()
    {
        dataHeader.wrap(rcvBuffer);

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termBuffers[i] = new UnsafeBuffer(allocateDirect(TERM_BUFFER_LENGTH));
        }

        final UnsafeBuffer logMetaDataBuffer = new UnsafeBuffer(allocateDirect(LOG_META_DATA_LENGTH));

        when(logBuffers.duplicateTermBuffers()).thenReturn(termBuffers);
        when(logBuffers.termLength()).thenReturn(TERM_BUFFER_LENGTH);
        when(logBuffers.metaDataBuffer()).thenReturn(logMetaDataBuffer);
    }

    @Test
    void shouldHandleClosedImage()
    {
        final Image image = createImage();

        image.close();

        assertTrue(image.isClosed());
        assertThat(image.poll(mockFragmentHandler, Integer.MAX_VALUE), is(0));
        assertThat(image.position(), is(0L));
    }

    @Test
    void shouldAllowValidPosition()
    {
        final Image image = createImage();
        final long expectedPosition = TERM_BUFFER_LENGTH - 32;

        position.setOrdered(expectedPosition);
        assertThat(image.position(), is(expectedPosition));

        image.position(TERM_BUFFER_LENGTH);
        assertThat(image.position(), is((long)TERM_BUFFER_LENGTH));
    }

    @Test
    void shouldNotAdvancePastEndOfTerm()
    {
        final Image image = createImage();
        final long expectedPosition = TERM_BUFFER_LENGTH - 32;

        position.setOrdered(expectedPosition);
        assertThat(image.position(), is(expectedPosition));

        assertThrows(IllegalArgumentException.class, () -> image.position(TERM_BUFFER_LENGTH + 32));
    }

    @Test
    void shouldReportCorrectPositionOnReception()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));

        final int messages = image.poll(mockFragmentHandler, Integer.MAX_VALUE);
        assertThat(messages, is(1));

        verify(mockFragmentHandler).onFragment(
            any(UnsafeBuffer.class),
            eq(HEADER_LENGTH),
            eq(DATA.length),
            any(Header.class));

        final InOrder inOrder = Mockito.inOrder(position);
        inOrder.verify(position).setOrdered(initialPosition);
        inOrder.verify(position).setOrdered(initialPosition + ALIGNED_FRAME_LENGTH);
    }

    @Test
    void shouldReportCorrectPositionOnReceptionWithNonZeroPositionInInitialTermId()
    {
        final int initialMessageIndex = 5;
        final int initialTermOffset = offsetForFrame(initialMessageIndex);
        final long initialPosition = computePosition(
            INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(initialMessageIndex));

        final int messages = image.poll(mockFragmentHandler, Integer.MAX_VALUE);
        assertThat(messages, is(1));

        verify(mockFragmentHandler).onFragment(
            any(UnsafeBuffer.class),
            eq(initialTermOffset + HEADER_LENGTH),
            eq(DATA.length),
            any(Header.class));

        final InOrder inOrder = Mockito.inOrder(position);
        inOrder.verify(position).setOrdered(initialPosition);
        inOrder.verify(position).setOrdered(initialPosition + ALIGNED_FRAME_LENGTH);
    }

    @Test
    void shouldReportCorrectPositionOnReceptionWithNonZeroPositionInNonInitialTermId()
    {
        final int activeTermId = INITIAL_TERM_ID + 1;
        final int initialMessageIndex = 5;
        final int initialTermOffset = offsetForFrame(initialMessageIndex);
        final long initialPosition =
            computePosition(activeTermId, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(activeTermId, offsetForFrame(initialMessageIndex));

        final int messages = image.poll(mockFragmentHandler, Integer.MAX_VALUE);
        assertThat(messages, is(1));

        verify(mockFragmentHandler).onFragment(
            any(UnsafeBuffer.class),
            eq(initialTermOffset + HEADER_LENGTH),
            eq(DATA.length),
            any(Header.class));

        final InOrder inOrder = Mockito.inOrder(position);
        inOrder.verify(position).setOrdered(initialPosition);
        inOrder.verify(position).setOrdered(initialPosition + ALIGNED_FRAME_LENGTH);
    }

    @Test
    void shouldPollNoFragmentsToControlledFragmentHandler()
    {
        final Image image = createImage();
        final int fragmentsRead = image.controlledPoll(mockControlledFragmentHandler, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(0));
        verify(position, never()).setOrdered(anyLong());
        verify(mockControlledFragmentHandler, never()).onFragment(
            any(UnsafeBuffer.class), anyInt(), anyInt(), any(Header.class));
    }

    @Test
    void shouldPollOneFragmentToControlledFragmentHandlerOnContinue()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));

        when(mockControlledFragmentHandler.onFragment(any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class)))
            .thenReturn(Action.CONTINUE);

        final int fragmentsRead = image.controlledPoll(mockControlledFragmentHandler, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(1));

        final InOrder inOrder = Mockito.inOrder(position, mockControlledFragmentHandler);
        inOrder.verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(HEADER_LENGTH), eq(DATA.length), any(Header.class));
        inOrder.verify(position).setOrdered(initialPosition + ALIGNED_FRAME_LENGTH);
    }

    @Test
    void shouldUpdatePositionOnRethrownExceptionInControlledPoll()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));

        when(mockControlledFragmentHandler.onFragment(any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class)))
            .thenThrow(new RuntimeException());

        doThrow(new RuntimeException()).when(errorHandler).onError(any());

        boolean thrown = false;
        try
        {
            image.controlledPoll(mockControlledFragmentHandler, Integer.MAX_VALUE);
        }
        catch (final Exception ignore)
        {
            thrown = true;
        }

        assertTrue(thrown);
        assertThat(image.position(), is(initialPosition + ALIGNED_FRAME_LENGTH));

        verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(HEADER_LENGTH), eq(DATA.length), any(Header.class));
    }

    @Test
    void shouldUpdatePositionOnRethrownExceptionInPoll()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));

        doThrow(new RuntimeException()).when(mockFragmentHandler)
            .onFragment(any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class));

        doThrow(new RuntimeException()).when(errorHandler).onError(any());

        boolean thrown = false;
        try
        {
            image.poll(mockFragmentHandler, Integer.MAX_VALUE);
        }
        catch (final Exception ignore)
        {
            thrown = true;
        }

        assertTrue(thrown);
        assertThat(image.position(), is(initialPosition + ALIGNED_FRAME_LENGTH));

        verify(mockFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(HEADER_LENGTH), eq(DATA.length), any(Header.class));
    }

    @Test
    void shouldNotPollOneFragmentToControlledFragmentHandlerOnAbort()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));

        when(mockControlledFragmentHandler.onFragment(any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class)))
            .thenReturn(Action.ABORT);

        final int fragmentsRead = image.controlledPoll(mockControlledFragmentHandler, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(0));
        assertThat(image.position(), is(initialPosition));

        verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(HEADER_LENGTH), eq(DATA.length), any(Header.class));
    }

    @Test
    void shouldPollOneFragmentToControlledFragmentHandlerOnBreak()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(1));

        when(mockControlledFragmentHandler.onFragment(any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class)))
            .thenReturn(Action.BREAK);

        final int fragmentsRead = image.controlledPoll(mockControlledFragmentHandler, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(1));

        final InOrder inOrder = Mockito.inOrder(position, mockControlledFragmentHandler);
        inOrder.verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(HEADER_LENGTH), eq(DATA.length), any(Header.class));
        inOrder.verify(position).setOrdered(initialPosition + ALIGNED_FRAME_LENGTH);
    }

    @Test
    void shouldPollFragmentsToControlledFragmentHandlerOnCommit()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(1));

        when(mockControlledFragmentHandler.onFragment(any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class)))
            .thenReturn(Action.COMMIT);

        final int fragmentsRead = image.controlledPoll(mockControlledFragmentHandler, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(2));

        final InOrder inOrder = Mockito.inOrder(position, mockControlledFragmentHandler);
        inOrder.verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(HEADER_LENGTH), eq(DATA.length), any(Header.class));
        inOrder.verify(position).setOrdered(initialPosition + ALIGNED_FRAME_LENGTH);

        inOrder.verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(ALIGNED_FRAME_LENGTH + HEADER_LENGTH), eq(DATA.length), any(Header.class));
        inOrder.verify(position).setOrdered(initialPosition + (ALIGNED_FRAME_LENGTH * 2L));
    }

    @Test
    void shouldUpdatePositionToEndOfCommittedFragmentOnCommit()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(1));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(2));

        when(mockControlledFragmentHandler.onFragment(any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class)))
            .thenReturn(Action.CONTINUE, Action.COMMIT, Action.CONTINUE);

        final int fragmentsRead = image.controlledPoll(mockControlledFragmentHandler, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(3));

        final InOrder inOrder = Mockito.inOrder(position, mockControlledFragmentHandler);
        // first fragment, continue
        inOrder.verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(HEADER_LENGTH), eq(DATA.length), any(Header.class));

        // second fragment, commit
        inOrder.verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class),
            eq(ALIGNED_FRAME_LENGTH + HEADER_LENGTH),
            eq(DATA.length),
            any(Header.class));
        inOrder.verify(position).setOrdered(initialPosition + (ALIGNED_FRAME_LENGTH * 2L));

        // third fragment, continue, but position is updated because last
        inOrder.verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class),
            eq(2 * ALIGNED_FRAME_LENGTH + HEADER_LENGTH),
            eq(DATA.length),
            any(Header.class));
        inOrder.verify(position).setOrdered(initialPosition + (ALIGNED_FRAME_LENGTH * 3L));
    }

    @Test
    void shouldPollFragmentsToControlledFragmentHandlerOnContinue()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(1));

        when(mockControlledFragmentHandler.onFragment(any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class)))
            .thenReturn(Action.CONTINUE);

        final int fragmentsRead = image.controlledPoll(mockControlledFragmentHandler, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(2));

        final InOrder inOrder = Mockito.inOrder(position, mockControlledFragmentHandler);
        inOrder.verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(HEADER_LENGTH), eq(DATA.length), any(Header.class));
        inOrder.verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(ALIGNED_FRAME_LENGTH + HEADER_LENGTH), eq(DATA.length), any(Header.class));
        inOrder.verify(position).setOrdered(initialPosition + (ALIGNED_FRAME_LENGTH * 2L));
    }

    @Test
    void shouldPollNoFragmentsToBoundedControlledFragmentHandlerWithMaxPositionBeforeInitialPosition()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        final long maxPosition = initialPosition - HEADER_LENGTH;
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(1));

        when(mockControlledFragmentHandler.onFragment(any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class)))
            .thenReturn(Action.CONTINUE);

        final int fragmentsRead = image.boundedControlledPoll(
            mockControlledFragmentHandler, maxPosition, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(0));

        assertThat(position.get(), is(initialPosition));
        verify(mockControlledFragmentHandler, never()).onFragment(
            any(UnsafeBuffer.class), anyInt(), anyInt(), any(Header.class));
    }

    @Test
    void shouldPollFragmentsToBoundedControlledFragmentHandlerWithInitialOffsetNotZero()
    {
        final long initialPosition = computePosition(
            INITIAL_TERM_ID, offsetForFrame(1), POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        final long maxPosition = initialPosition + ALIGNED_FRAME_LENGTH;
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(1));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(2));

        when(mockControlledFragmentHandler.onFragment(any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class)))
            .thenReturn(Action.CONTINUE);

        final int fragmentsRead = image.boundedControlledPoll(
            mockControlledFragmentHandler, maxPosition, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(1));

        assertThat(position.get(), is(maxPosition));
        verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), anyInt(), anyInt(), any(Header.class));
    }

    @Test
    void shouldPollFragmentsToBoundedControlledFragmentHandlerWithMaxPositionBeforeNextMessage()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        final long maxPosition = initialPosition + ALIGNED_FRAME_LENGTH;
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(1));

        when(mockControlledFragmentHandler.onFragment(any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class)))
            .thenReturn(Action.CONTINUE);

        final int fragmentsRead = image.boundedControlledPoll(
            mockControlledFragmentHandler, maxPosition, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(1));

        final InOrder inOrder = Mockito.inOrder(position, mockControlledFragmentHandler);
        inOrder.verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(HEADER_LENGTH), eq(DATA.length), any(Header.class));
        inOrder.verify(position).setOrdered(initialPosition + ALIGNED_FRAME_LENGTH);
    }

    @Test
    void shouldPollFragmentsToBoundedFragmentHandlerWithMaxPositionBeforeNextMessage()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        final long maxPosition = initialPosition + ALIGNED_FRAME_LENGTH;
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(1));

        final int fragmentsRead = image.boundedPoll(mockFragmentHandler, maxPosition, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(1));

        final InOrder inOrder = Mockito.inOrder(position, mockFragmentHandler);
        inOrder.verify(mockFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(HEADER_LENGTH), eq(DATA.length), any(Header.class));
        inOrder.verify(position).setOrdered(initialPosition + ALIGNED_FRAME_LENGTH);
    }

    @Test
    void shouldPollFragmentsToBoundedControlledFragmentHandlerWithMaxPositionAfterEndOfTerm()
    {
        final int initialOffset = TERM_BUFFER_LENGTH - (ALIGNED_FRAME_LENGTH * 2);
        final long initialPosition = computePosition(
            INITIAL_TERM_ID, initialOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        final long maxPosition = initialPosition + TERM_BUFFER_LENGTH;
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, initialOffset);
        insertPaddingFrame(INITIAL_TERM_ID, initialOffset + ALIGNED_FRAME_LENGTH);

        when(mockControlledFragmentHandler.onFragment(any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class)))
            .thenReturn(Action.CONTINUE);

        final int fragmentsRead = image.boundedControlledPoll(
            mockControlledFragmentHandler, maxPosition, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(1));

        final InOrder inOrder = Mockito.inOrder(position, mockControlledFragmentHandler);
        inOrder.verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(initialOffset + HEADER_LENGTH), eq(DATA.length), any(Header.class));
        inOrder.verify(position).setOrdered(TERM_BUFFER_LENGTH);
    }

    @Test
    void shouldPollFragmentsToBoundedControlledFragmentHandlerWithMaxPositionAboveIntMaxValue()
    {
        final int initialOffset = TERM_BUFFER_LENGTH - (ALIGNED_FRAME_LENGTH * 2);
        final long initialPosition = computePosition(
            INITIAL_TERM_ID, initialOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        final long maxPosition = (long)Integer.MAX_VALUE + 1000;
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, initialOffset);
        insertPaddingFrame(INITIAL_TERM_ID, initialOffset + ALIGNED_FRAME_LENGTH);

        when(mockControlledFragmentHandler.onFragment(any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class)))
            .thenReturn(Action.CONTINUE);

        final int fragmentsRead = image.boundedControlledPoll(
            mockControlledFragmentHandler, maxPosition, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(1));

        final InOrder inOrder = Mockito.inOrder(position, mockControlledFragmentHandler);
        inOrder.verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(initialOffset + HEADER_LENGTH), eq(DATA.length), any(Header.class));
        inOrder.verify(position).setOrdered(TERM_BUFFER_LENGTH);
    }

    @Test
    void shouldPollFragmentsToBoundedFragmentHandlerWithMaxPositionAboveIntMaxValue()
    {
        final int initialOffset = TERM_BUFFER_LENGTH - (ALIGNED_FRAME_LENGTH * 2);
        final long initialPosition = computePosition(
            INITIAL_TERM_ID, initialOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        final long maxPosition = (long)Integer.MAX_VALUE + 1000;
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, initialOffset);
        insertPaddingFrame(INITIAL_TERM_ID, initialOffset + ALIGNED_FRAME_LENGTH);

        final int fragmentsRead = image.boundedPoll(
            mockFragmentHandler, maxPosition, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(1));

        final InOrder inOrder = Mockito.inOrder(position, mockFragmentHandler);
        inOrder.verify(mockFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(initialOffset + HEADER_LENGTH), eq(DATA.length), any(Header.class));
        inOrder.verify(position).setOrdered(TERM_BUFFER_LENGTH);
    }

    @Test
    void shouldRejectFragment()
    {
        final int initialOffset = TERM_BUFFER_LENGTH - (ALIGNED_FRAME_LENGTH * 2);
        final long initialPosition = computePosition(
            INITIAL_TERM_ID, initialOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, initialOffset);
        insertPaddingFrame(INITIAL_TERM_ID, initialOffset + ALIGNED_FRAME_LENGTH);

        assertEquals(initialPosition, image.position());

        final String reason = "this is frame is to be rejected";
        image.reject(reason);

        verify(subscription).rejectImage(image.correlationId(), image.position(), reason);
    }

    @Test
    void shouldExitPollIfImageIsClosed()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(1));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(2));

        final int messages = image.poll((buffer, offset, length, header) -> image.close(), Integer.MAX_VALUE);
        assertThat(messages, is(1));
        assertThat(image.isClosed(), is(true));

        final InOrder inOrder = Mockito.inOrder(position);
        inOrder.verify(position).setOrdered(initialPosition);
        inOrder.verify(position).setOrdered(initialPosition + ALIGNED_FRAME_LENGTH);
    }

    @Test
    void shouldExitBoundedPollIfImageIsClosed()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(1));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(2));

        final int messages = image.boundedPoll(
            (buffer, offset, length, header) -> image.close(), Long.MAX_VALUE, Integer.MAX_VALUE);
        assertThat(messages, is(1));
        assertThat(image.isClosed(), is(true));

        final InOrder inOrder = Mockito.inOrder(position);
        inOrder.verify(position).setOrdered(initialPosition);
        inOrder.verify(position).setOrdered(initialPosition + ALIGNED_FRAME_LENGTH);
    }

    @Test
    void shouldExitControlledPollIfImageIsClosed()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(1));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(2));

        final int messages = image.controlledPoll((buffer, offset, length, header) ->
        {
            image.close();
            return Action.CONTINUE;
        }, Integer.MAX_VALUE);
        assertThat(messages, is(1));
        assertThat(image.isClosed(), is(true));

        final InOrder inOrder = Mockito.inOrder(position);
        inOrder.verify(position).setOrdered(initialPosition);
        inOrder.verify(position).setOrdered(initialPosition + ALIGNED_FRAME_LENGTH);
    }

    @Test
    void shouldExitBoundedControlledPollIfImageIsClosed()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(1));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(2));

        final int messages = image.boundedControlledPoll(
            (buffer, offset, length, header) ->
            {
                image.close();
                return Action.CONTINUE;
            },
            Long.MAX_VALUE,
            Integer.MAX_VALUE);
        assertThat(messages, is(1));
        assertThat(image.isClosed(), is(true));

        final InOrder inOrder = Mockito.inOrder(position);
        inOrder.verify(position).setOrdered(initialPosition);
        inOrder.verify(position).setOrdered(initialPosition + ALIGNED_FRAME_LENGTH);
    }

    @Test
    void shouldExitControlledPeekIfImageIsClosed()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(1));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(2));

        final long resultingPosition = image.controlledPeek(
            initialPosition,
            (buffer, offset, length, header) ->
            {
                image.close();
                return Action.CONTINUE;
            },
            Long.MAX_VALUE);
        assertThat(resultingPosition, is(initialPosition + offsetForFrame(1)));
        assertThat(image.isClosed(), is(true));

        verify(position).setOrdered(initialPosition);
        verify(position, never()).setOrdered(AdditionalMatchers.not(eq(initialPosition)));
    }

    private Image createImage()
    {
        return new Image(subscription, SESSION_ID, position, logBuffers, errorHandler, SOURCE_IDENTITY, CORRELATION_ID);
    }

    private void insertDataFrame(final int activeTermId, final int termOffset)
    {
        dataHeader
            .termId(INITIAL_TERM_ID)
            .streamId(STREAM_ID)
            .sessionId(SESSION_ID)
            .termOffset(termOffset)
            .frameLength(DATA.length + HEADER_LENGTH)
            .headerType(HeaderFlyweight.HDR_TYPE_DATA)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .version(HeaderFlyweight.CURRENT_VERSION);

        rcvBuffer.putBytes(dataHeader.dataOffset(), DATA);

        final int activeIndex = indexByTerm(INITIAL_TERM_ID, activeTermId);
        TermRebuilder.insert(termBuffers[activeIndex], termOffset, rcvBuffer, ALIGNED_FRAME_LENGTH);
    }

    private void insertPaddingFrame(final int activeTermId, final int termOffset)
    {
        dataHeader
            .termId(INITIAL_TERM_ID)
            .streamId(STREAM_ID)
            .sessionId(SESSION_ID)
            .frameLength(TERM_BUFFER_LENGTH - termOffset)
            .headerType(HeaderFlyweight.HDR_TYPE_PAD)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .version(HeaderFlyweight.CURRENT_VERSION);

        final int activeIndex = indexByTerm(INITIAL_TERM_ID, activeTermId);
        TermRebuilder.insert(termBuffers[activeIndex], termOffset, rcvBuffer, TERM_BUFFER_LENGTH - termOffset);
    }

    private static int offsetForFrame(final int index)
    {
        return index * ALIGNED_FRAME_LENGTH;
    }
}
