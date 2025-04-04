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
import org.agrona.concurrent.*;
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
}
