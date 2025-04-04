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
package io.aeron.driver;

import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.driver.status.SystemCounters;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.BufferUtil;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.AtomicLongPosition;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.Position;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static io.aeron.driver.TermCleaner.TERM_CLEANUP_BLOCK_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NetworkPublicationTest
{
    private static final int REGISTRATION_ID = 42;
    private static final int PUBLICATION_WINDOW_LENGTH = TERM_MIN_LENGTH / 2;
    private static final int SESSION_ID = 8;
    private static final int STREAM_ID = 101;
    private static final int INITIAL_TERM_ID = 99;
    private static final NetworkPublicationThreadLocals NETWORK_PUBLICATION_THREAD_LOCALS =
        new NetworkPublicationThreadLocals();
    private static final UnsafeBuffer[] TERM_BUFFERS = new UnsafeBuffer[PARTITION_COUNT];

    static
    {
        for (int i = 0; i < TERM_BUFFERS.length; i++)
        {
            TERM_BUFFERS[i] = new UnsafeBuffer(BufferUtil.allocateDirectAligned(TERM_MIN_LENGTH, CACHE_LINE_LENGTH));
        }
    }

    private final CountersManager countersManager = new CountersManager(
        new UnsafeBuffer(BufferUtil.allocateDirectAligned(TERM_MIN_LENGTH, CACHE_LINE_LENGTH)),
        new UnsafeBuffer(BufferUtil.allocateDirectAligned(8192, CACHE_LINE_LENGTH)));
    private final MediaDriver.Context ctx = new MediaDriver.Context()
        .senderCachedNanoClock(new CachedNanoClock())
        .systemCounters(new SystemCounters(countersManager));
    private final PublicationParams params = new PublicationParams();
    private final SendChannelEndpoint sendChannelEndpoint = mock(SendChannelEndpoint.class);
    private final RawLog rawLog = mock(RawLog.class);
    private final Position publisherPos = new AtomicLongPosition();
    private final Position publisherLimit = new AtomicLongPosition();
    private final Position senderPosition = new AtomicLongPosition();
    private final Position senderLimit = new AtomicLongPosition();
    private final AtomicCounter senderBpe = countersManager.newCounter("snd-bpe");
    private final FlowControl flowControl = mock(FlowControl.class);
    private final RetransmitHandler retransmitHandler = mock(RetransmitHandler.class);
    private final StatusMessageFlyweight statusMessageFlyweight = mock(StatusMessageFlyweight.class);
    private final InetSocketAddress inetSocketAddress = mock(InetSocketAddress.class);
    private final DriverConductorProxy driverConductorProxy = mock(DriverConductorProxy.class);

    @BeforeEach
    void before()
    {
        final UnsafeBuffer metadataBuffer = new UnsafeBuffer(new byte[LOG_META_DATA_LENGTH]);
        termLength(metadataBuffer, TERM_MIN_LENGTH);
        mtuLength(metadataBuffer, 8192);
        initialTermId(metadataBuffer, INITIAL_TERM_ID);
        initialiseTailWithTermId(metadataBuffer, 0, INITIAL_TERM_ID);
        for (int i = 1; i < PARTITION_COUNT; i++)
        {
            final int expectedTermId = (INITIAL_TERM_ID + i) - PARTITION_COUNT;
            initialiseTailWithTermId(metadataBuffer, i, expectedTermId);
        }
        isConnected(metadataBuffer, true);

        when(rawLog.metaData()).thenReturn(metadataBuffer);
        when(rawLog.termBuffers()).thenReturn(TERM_BUFFERS);
        when(rawLog.sliceTerms()).thenReturn(
            Stream.of(TERM_BUFFERS).map(UnsafeBuffer::byteBuffer).toArray(ByteBuffer[]::new));
        when(rawLog.termLength()).thenReturn(TERM_MIN_LENGTH);

        for (final UnsafeBuffer termBuffer : TERM_BUFFERS)
        {
            termBuffer.setMemory(0, termBuffer.capacity(), (byte)0xFF);
        }

        when(flowControl.hasRequiredReceivers()).thenReturn(true);
    }

    @ParameterizedTest
    @CsvSource({ "0,0", "32, 0", "1024, 0", "4096, 4096", "20480, 20480", "20544, 20480", "13958643584, 13958639616" })
    void shouldAlignInitialCleanPosition(final long initialPosition, final long expectedCleanPosition)
    {
        senderPosition.set(initialPosition);

        final NetworkPublication publication = new NetworkPublication(
            REGISTRATION_ID,
            ctx,
            params,
            sendChannelEndpoint,
            rawLog,
            PUBLICATION_WINDOW_LENGTH,
            publisherPos,
            publisherLimit,
            senderPosition,
            senderLimit,
            senderBpe,
            SESSION_ID,
            STREAM_ID,
            INITIAL_TERM_ID,
            flowControl,
            retransmitHandler,
            NETWORK_PUBLICATION_THREAD_LOCALS,
            true);

        assertEquals(expectedCleanPosition, publication.cleanPosition);
    }

    @SuppressWarnings("MethodLength")
    @ParameterizedTest
    @CsvSource({ "0,0", "512, 0", "8032, 4096", "65440, 61440" })
    void shouldCleanLogBufferOneTermBehindMinSubPosition(final long initialPosition, final long initialCleanPosition)
    {
        publisherPos.set(initialPosition);
        publisherLimit.set(initialPosition);
        senderPosition.set(initialPosition);
        senderLimit.set(initialPosition);

        final NetworkPublication publication = new NetworkPublication(
            REGISTRATION_ID,
            ctx,
            params,
            sendChannelEndpoint,
            rawLog,
            PUBLICATION_WINDOW_LENGTH,
            publisherPos,
            publisherLimit,
            senderPosition,
            senderLimit,
            senderBpe,
            SESSION_ID,
            STREAM_ID,
            INITIAL_TERM_ID,
            flowControl,
            retransmitHandler,
            NETWORK_PUBLICATION_THREAD_LOCALS,
            true);
        assertEquals(initialCleanPosition, publication.cleanPosition);

        // setup initial connection
        publication.onStatusMessage(statusMessageFlyweight, inetSocketAddress, driverConductorProxy);

        // first iteration: only pub-lmt is updated
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialCleanPosition, publication.cleanPosition);

        // consumer position didn't change -> no op
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialCleanPosition, publication.cleanPosition);

        // consumer position moved less than tripGain -> no op
        senderPosition.set(initialPosition + 3072);
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialCleanPosition, publication.cleanPosition);

        // consumer position moved past tripGain -> update limit
        senderPosition.set(initialPosition + TERM_MIN_LENGTH / 8);
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + TERM_MIN_LENGTH / 8 + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialCleanPosition, publication.cleanPosition);

        // consumer position moved more than 1 term but less than cleaning block
        senderPosition.set(initialPosition + TERM_MIN_LENGTH + 64);
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + TERM_MIN_LENGTH + 64 + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialCleanPosition, publication.cleanPosition);

        // consumer position moved more than 1 term and a cleaning block forward
        senderPosition.set(initialPosition + TERM_MIN_LENGTH + PUBLICATION_WINDOW_LENGTH);
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + TERM_MIN_LENGTH + 2 * PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialCleanPosition + TERM_CLEANUP_BLOCK_LENGTH, publication.cleanPosition);

        // cleaning in blocks
        publication.updatePublisherPositionAndLimit();
        publication.updatePublisherPositionAndLimit();
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + TERM_MIN_LENGTH + 2 * PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialCleanPosition + 4 * TERM_CLEANUP_BLOCK_LENGTH, publication.cleanPosition);

        // consumer position moved too far
        senderPosition.set(initialPosition + 2 * TERM_MIN_LENGTH + 3 * PUBLICATION_WINDOW_LENGTH / 4);
        publication.updatePublisherPositionAndLimit();
        assertEquals(
            initialPosition + 2 * TERM_MIN_LENGTH + 3 * PUBLICATION_WINDOW_LENGTH / 4 + PUBLICATION_WINDOW_LENGTH,
            publisherLimit.get());
        assertEquals(initialCleanPosition + 5 * TERM_CLEANUP_BLOCK_LENGTH, publication.cleanPosition);

        // consumer position advanced too far -> no pub-lmt update
        senderPosition.set(initialPosition + 3 * TERM_MIN_LENGTH);
        publication.updatePublisherPositionAndLimit();
        assertEquals(
            initialPosition + 2 * TERM_MIN_LENGTH + 3 * PUBLICATION_WINDOW_LENGTH / 4 + PUBLICATION_WINDOW_LENGTH,
            publisherLimit.get());
        assertEquals(initialCleanPosition + 6 * TERM_CLEANUP_BLOCK_LENGTH, publication.cleanPosition);

        final long newLimit = initialPosition + 3 * TERM_MIN_LENGTH + PUBLICATION_WINDOW_LENGTH;
        final long baseLimitPosition = newLimit - (newLimit & (TERM_MIN_LENGTH - 1));
        while (newLimit != publisherLimit.get())
        {
            publication.updatePublisherPositionAndLimit();
        }
        assertEquals(newLimit, publisherLimit.get());

        final long cleanPositionTermOffset = publication.cleanPosition & (TERM_MIN_LENGTH - 1);
        final long baseCleanPosition = publication.cleanPosition - cleanPositionTermOffset;
        // ensure pub-lmt does not cross into a dirty term
        assertThat(baseLimitPosition, lessThanOrEqualTo(baseCleanPosition + 2 * TERM_MIN_LENGTH));
        assertThat(cleanPositionTermOffset, greaterThan(0L));
    }

    @Test
    void pubLimitShouldNotCrossToThePreviousTermIfAnEntireTermIsDirty()
    {
        final NetworkPublication publication = new NetworkPublication(
            REGISTRATION_ID,
            ctx,
            params,
            sendChannelEndpoint,
            rawLog,
            PUBLICATION_WINDOW_LENGTH,
            publisherPos,
            publisherLimit,
            senderPosition,
            senderLimit,
            senderBpe,
            SESSION_ID,
            STREAM_ID,
            INITIAL_TERM_ID,
            flowControl,
            retransmitHandler,
            NETWORK_PUBLICATION_THREAD_LOCALS,
            true);
        assertEquals(0, publication.cleanPosition);

        // setup initial connection
        publication.onStatusMessage(statusMessageFlyweight, inetSocketAddress, driverConductorProxy);

        // clean the first buffer completely
        senderPosition.set(2 * TERM_MIN_LENGTH);
        while (publication.cleanPosition != TERM_MIN_LENGTH - TERM_CLEANUP_BLOCK_LENGTH)
        {
            publication.updatePublisherPositionAndLimit();
        }
        assertEquals(2 * TERM_MIN_LENGTH + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(TERM_MIN_LENGTH - TERM_CLEANUP_BLOCK_LENGTH, publication.cleanPosition);

        // pub-lmt cannot be changed as clean position is at the start of dirty term
        senderPosition.set(2 * TERM_MIN_LENGTH + PUBLICATION_WINDOW_LENGTH + 1024);
        publication.updatePublisherPositionAndLimit();
        assertEquals(2 * TERM_MIN_LENGTH + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(TERM_MIN_LENGTH, publication.cleanPosition);

        // pub-lmt is allowed to update after clean position moves from the start of a dirty buffer
        publication.updatePublisherPositionAndLimit();
        assertEquals(senderPosition.get() + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(TERM_MIN_LENGTH + TERM_CLEANUP_BLOCK_LENGTH, publication.cleanPosition);
    }
}
