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

import io.aeron.CommonContext;
import io.aeron.DriverProxy;
import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.buffer.TestLogFactory;
import io.aeron.driver.status.SystemCounters;
import io.aeron.test.Tests;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.AtomicLongPosition;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.Position;
import org.agrona.concurrent.status.UnsafeBufferPosition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.ByteBuffer;

import static io.aeron.driver.TermCleaner.TERM_CLEANUP_BLOCK_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IpcPublicationTest
{
    private static final long CLIENT_ID = 7L;
    private static final int STREAM_ID = 1010;
    private static final int TERM_BUFFER_LENGTH = TERM_MIN_LENGTH;
    private static final int BUFFER_LENGTH = 16 * 1024;

    private Position publisherLimit;
    private IpcPublication ipcPublication;

    private DriverProxy driverProxy;
    private DriverConductor driverConductor;
    private MediaDriver.Context ctx;

    @BeforeEach
    void setUp()
    {
        final RingBuffer toDriverCommands = new ManyToOneRingBuffer(new UnsafeBuffer(
            ByteBuffer.allocateDirect(Configuration.CONDUCTOR_BUFFER_LENGTH_DEFAULT)));

        final CountersManager countersManager = Tests.newCountersManager(BUFFER_LENGTH);
        final SystemCounters systemCounters = new SystemCounters(countersManager);

        final SenderProxy senderProxy = mock(SenderProxy.class);
        final ReceiverProxy receiverProxy = mock(ReceiverProxy.class);

        ctx = new MediaDriver.Context()
            .tempBuffer(new UnsafeBuffer(new byte[METADATA_LENGTH]))
            .ipcTermBufferLength(TERM_BUFFER_LENGTH)
            .toDriverCommands(toDriverCommands)
            .logFactory(new TestLogFactory())
            .clientProxy(mock(ClientProxy.class))
            .senderProxy(senderProxy)
            .receiverProxy(receiverProxy)
            .driverCommandQueue(new ManyToOneConcurrentLinkedQueue<>())
            .epochClock(SystemEpochClock.INSTANCE)
            .cachedEpochClock(new CachedEpochClock())
            .cachedNanoClock(new CachedNanoClock())
            .countersManager(countersManager)
            .systemCounters(systemCounters)
            .nameResolver(DefaultNameResolver.INSTANCE)
            .nanoClock(new CachedNanoClock())
            .threadingMode(ThreadingMode.DEDICATED)
            .conductorDutyCycleTracker(new DutyCycleTracker())
            .nameResolverTimeTracker(new DutyCycleTracker());

        driverProxy = new DriverProxy(toDriverCommands, CLIENT_ID);
        driverConductor = new DriverConductor(ctx);
        driverConductor.onStart();

        driverProxy.addPublication(CommonContext.IPC_CHANNEL, STREAM_ID);
        driverConductor.doWork();

        ipcPublication = driverConductor.getSharedIpcPublication(STREAM_ID);
        publisherLimit = new UnsafeBufferPosition(
            (UnsafeBuffer)countersManager.valuesBuffer(), ipcPublication.publisherLimitId());
    }

    @Test
    void shouldStartWithPublisherLimitSetToZero()
    {
        assertThat(publisherLimit.get(), is(0L));
    }

    @Test
    void shouldKeepPublisherLimitZeroOnNoSubscriptionUpdate()
    {
        ipcPublication.updatePublisherPositionAndLimit();
        assertThat(publisherLimit.get(), is(0L));
    }

    @Test
    void shouldHaveJoiningPositionZeroWhenNoSubscriptions()
    {
        assertThat(ipcPublication.joinPosition(), is(0L));
    }

    @Test
    void shouldIncrementPublisherLimitOnSubscription()
    {
        driverProxy.addSubscription(CommonContext.IPC_CHANNEL, STREAM_ID);
        driverConductor.doWork();

        assertThat(publisherLimit.get(), is(greaterThan(0L)));
    }

    @ParameterizedTest
    @CsvSource({ "0,0", "4096,4096", "135000,131072", "1000000001111,1000000000000" })
    void shouldAlignCleanPositionAtTheStartOfTheBlock(final long startPosition, final long expectedCleanPosition)
    {
        final PublicationParams params = new PublicationParams();
        params.termLength = 128 * 1024;
        final int positionBitsToShift = positionBitsToShift(params.termLength);
        final int initialTermId = 876;
        params.initialTermId = initialTermId;
        params.termId = computeTermIdFromPosition(startPosition, positionBitsToShift, initialTermId);
        params.publicationWindowLength = params.termLength / 2;
        params.mtuLength = 2048;

        final UnsafeBuffer metadataBuffer = new UnsafeBuffer(new byte[LOG_META_DATA_LENGTH]);
        initialTermId(metadataBuffer, initialTermId);
        activeTermCount(metadataBuffer, params.termId - initialTermId);
        rawTailVolatile(
            metadataBuffer,
            indexByTermCount(params.termId - initialTermId),
            packTail(params.termId, (int)(startPosition & (params.termLength - 1))));
        final RawLog rawLog = mock(RawLog.class);
        when(rawLog.metaData()).thenReturn(metadataBuffer);

        final IpcPublication publication = new IpcPublication(
            11111,
            "aeron:ipc?alias=test",
            ctx,
            0,
            42,
            9,
            new AtomicLongPosition(),
            new AtomicLongPosition(),
            rawLog,
            true,
            params);

        assertEquals(expectedCleanPosition, publication.cleanPosition());
    }

    @SuppressWarnings("MethodLength")
    @Test
    void shouldCleanBufferAndUpdatePublisherLimit()
    {
        final int initialTermId = 11;
        final int streamId = -8;
        final int sessionId = 273482763;
        final PublicationParams params = new PublicationParams();
        params.termLength = 64 * 1024;
        params.initialTermId = initialTermId;
        params.termId = initialTermId + 123;
        params.publicationWindowLength = params.termLength / 2;
        params.mtuLength = 8192;

        final UnsafeBuffer metadataBuffer = new UnsafeBuffer(new byte[LOG_META_DATA_LENGTH]);
        initialTermId(metadataBuffer, initialTermId);
        activeTermCount(metadataBuffer, params.termId - initialTermId);
        final int activeTermIndex = indexByTermCount(params.termId - initialTermId);
        rawTailVolatile(metadataBuffer, activeTermIndex, packTail(params.termId, 0));
        final RawLog rawLog = mock(RawLog.class);
        when(rawLog.metaData()).thenReturn(metadataBuffer);
        final UnsafeBuffer[] termBuffers = new UnsafeBuffer[PARTITION_COUNT];
        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termBuffers[i] = new UnsafeBuffer(ByteBuffer.allocateDirect(params.termLength));
            termBuffers[i].setMemory(0, params.termLength, (byte)0xFF);
        }
        when(rawLog.termBuffers()).thenReturn(termBuffers);

        final AtomicLongPosition publisherPos = new AtomicLongPosition();
        final AtomicLongPosition publisherLimit = new AtomicLongPosition();
        final AtomicLongPosition subscriberPosition = new AtomicLongPosition();

        final String channel = "aeron:ipc?alias=test";
        final IpcPublication publication = new IpcPublication(
            11111,
            channel,
            ctx,
            0,
            sessionId,
            streamId,
            publisherPos,
            publisherLimit,
            rawLog,
            true,
            params);

        final long initialPosition =
            computePosition(params.termId, 0, positionBitsToShift(params.termLength), initialTermId);
        assertEquals(initialPosition, publication.cleanPosition());

        final IpcSubscriptionLink subscriptionLink =
            new IpcSubscriptionLink(3, streamId, channel, mock(AeronClient.class), new SubscriptionParams());
        publication.addSubscriber(subscriptionLink, subscriberPosition, 0);

        subscriberPosition.set(0);
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition, publisherPos.get());
        assertEquals(params.publicationWindowLength, publisherLimit.get());
        assertEquals(initialPosition, publication.cleanPosition());

        // sub-pos distance from clean position is too small
        subscriberPosition.set(initialPosition + 128);
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition, publisherPos.get());
        assertEquals(initialPosition + 128 + params.publicationWindowLength, publisherLimit.get());
        assertEquals(initialPosition, publication.cleanPosition());

        // sub-pos change is less than trip gain -> no update to pub-lmt
        subscriberPosition.set(initialPosition + 6600);
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + 128 + params.publicationWindowLength, publisherLimit.get());
        assertEquals(initialPosition + TERM_CLEANUP_BLOCK_LENGTH, publication.cleanPosition());

        // sub-pos change two terms ahead
        subscriberPosition.set(initialPosition + 2L * params.termLength + 64);
        publication.updatePublisherPositionAndLimit();
        assertEquals(
            initialPosition + 2L * params.termLength + 64 + params.publicationWindowLength, publisherLimit.get());
        assertEquals(initialPosition + 2 * TERM_CLEANUP_BLOCK_LENGTH, publication.cleanPosition());

        // pub-lmt update is blocked as it will land in the dirty term
        subscriberPosition.set(initialPosition + 2L * params.termLength + params.termLength - 1024);
        publication.updatePublisherPositionAndLimit();
        assertEquals(
            initialPosition + 2L * params.termLength + 64 + params.publicationWindowLength, publisherLimit.get());
        assertEquals(initialPosition + 3 * TERM_CLEANUP_BLOCK_LENGTH, publication.cleanPosition());

        while (publication.cleanPosition() < initialPosition + params.termLength - TERM_CLEANUP_BLOCK_LENGTH)
        {
            publication.updatePublisherPositionAndLimit();
        }

        // pub-lmt cannot update as the next term is completely dirty
        publication.updatePublisherPositionAndLimit();
        assertEquals(
            initialPosition + 2L * params.termLength + 64 + params.publicationWindowLength, publisherLimit.get());
        assertEquals(initialPosition + params.termLength, publication.cleanPosition());

        // pub-lmt is allowed to update since the beginning of the next term is clean
        publication.updatePublisherPositionAndLimit();
        assertEquals(
            initialPosition + 2L * params.termLength + params.termLength - 1024 + params.publicationWindowLength,
            publisherLimit.get());
        assertEquals(initialPosition + params.termLength + TERM_CLEANUP_BLOCK_LENGTH, publication.cleanPosition());

        publication.removeSubscriber(subscriptionLink, subscriberPosition);
        assertEquals(initialPosition + params.termLength + 2 * TERM_CLEANUP_BLOCK_LENGTH, publication.cleanPosition());

        // pub-lmt is clamped to the latest consumer position
        publisherLimit.set(Long.MAX_VALUE);
        publication.updatePublisherPositionAndLimit();
        assertEquals(initialPosition + 2L * params.termLength + params.termLength - 1024, publisherLimit.get());
        assertEquals(initialPosition + params.termLength + 2 * TERM_CLEANUP_BLOCK_LENGTH, publication.cleanPosition());
    }
}
