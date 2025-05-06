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
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
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

    @SuppressWarnings("MethodLength")
    @ParameterizedTest
    @ValueSource(longs = { 0L, 512L, 7281770624L })
    void shouldCleanLogBufferOneTermBehindMinSubPosition(final long initialPosition)
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
        assertEquals(initialPosition, publication.cleanPosition);
        final long initialTermBasePosition = initialPosition - (initialPosition & (TERM_MIN_LENGTH - 1));

        // setup initial connection
        publication.onStatusMessage(statusMessageFlyweight, inetSocketAddress, driverConductorProxy);

        // first iteration: only pub-lmt is updated
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialPosition, publication.cleanPosition);

        // consumer position didn't change -> no op
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialPosition, publication.cleanPosition);

        // consumer position moved -> update limit
        senderPosition.set(initialPosition + 3072);
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + 3072 + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialPosition, publication.cleanPosition);

        // consumer position moved -> update limit
        senderPosition.set(initialPosition + TERM_MIN_LENGTH / 8);
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + TERM_MIN_LENGTH / 8 + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialPosition, publication.cleanPosition);

        // consumer position moved by an entire term -> nothing to clean yet
        senderPosition.set(initialPosition + TERM_MIN_LENGTH);
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + TERM_MIN_LENGTH + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialPosition, publication.cleanPosition);

        // consumer position moved again => clean bytes within a first buffer
        senderPosition.set(initialPosition + TERM_MIN_LENGTH + 128);
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + TERM_MIN_LENGTH + 128 + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialPosition + 128, publication.cleanPosition);

        // consumer position moved again => clean the entire first buffer
        senderPosition.set(initialPosition + 2 * TERM_MIN_LENGTH + 192);
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + 2 * TERM_MIN_LENGTH + 192 + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialTermBasePosition + TERM_MIN_LENGTH, publication.cleanPosition);

        // buffer clean trails snd-pos updates by one term
        senderPosition.set(initialPosition + 2 * TERM_MIN_LENGTH + 4096);
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + 2 * TERM_MIN_LENGTH + 4096 + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialPosition + TERM_MIN_LENGTH + 4096, publication.cleanPosition);
    }

    @Test
    void pubLimitShouldNotCrossToThePreviousTermIfAnEntireTermIsDirty()
    {
        final long initialPosition = TERM_MIN_LENGTH - 1440;
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
        assertEquals(initialPosition, publication.cleanPosition);

        // setup initial connection
        publication.onStatusMessage(statusMessageFlyweight, inetSocketAddress, driverConductorProxy);

        // no buffer cleaning at first
        senderPosition.set(initialPosition + TERM_MIN_LENGTH - 128);
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + TERM_MIN_LENGTH - 128 + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialPosition, publication.cleanPosition);

        // pub-lmt cannot be changed as clean position is at the start of a dirty term
        senderPosition.set(initialPosition + 2 * TERM_MIN_LENGTH);
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + TERM_MIN_LENGTH - 128 + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(TERM_MIN_LENGTH, publication.cleanPosition);

        // pub-lmt is allowed to update after clean position moves from the start of a dirty buffer
        publication.updatePublisherPositionAndLimit();
        assertEquals(senderPosition.get() + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialPosition + TERM_MIN_LENGTH, publication.cleanPosition);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, TERM_MIN_LENGTH - PUBLICATION_WINDOW_LENGTH})
    void pubLimitShouldNotCrossToTheDirtyTerm(final long initialPosition)
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
        assertEquals(initialPosition, publication.cleanPosition);

        // setup initial connection
        publication.onStatusMessage(statusMessageFlyweight, inetSocketAddress, driverConductorProxy);

        // initial position
        senderPosition.set(initialPosition + 256);
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + 256 + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialPosition, publication.cleanPosition);

        // new pub-lmt intersects with the clean position
        senderPosition.set(initialPosition + 2 * TERM_MIN_LENGTH + PUBLICATION_WINDOW_LENGTH);
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + 256 + PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(TERM_MIN_LENGTH, publication.cleanPosition);

        // after cleanup pub-lmt can move again
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + 2 * TERM_MIN_LENGTH + 2 * PUBLICATION_WINDOW_LENGTH, publisherLimit.get());
        assertEquals(initialPosition + TERM_MIN_LENGTH + PUBLICATION_WINDOW_LENGTH, publication.cleanPosition);
    }
}
