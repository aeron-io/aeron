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

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.DriverProxy;
import io.aeron.driver.buffer.TestLogFactory;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.Tests;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.Position;
import org.agrona.concurrent.status.UnsafeBufferPosition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static io.aeron.driver.Configuration.CMD_QUEUE_CAPACITY;
import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

class IpcPublicationTest
{
    private static final long CLIENT_ID = 7L;
    private static final int STREAM_ID = 1010;
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int BUFFER_LENGTH = 16 * 1024;

    private Position publisherLimit;
    private IpcPublication ipcPublication;

    private DriverProxy driverProxy;
    private DriverConductor driverConductor;
    private NativeResourceAgent nativeResourceAgent;

    @BeforeEach
    void setUp()
    {
        final RingBuffer toDriverCommands = new ManyToOneRingBuffer(new UnsafeBuffer(
            ByteBuffer.allocateDirect(Configuration.CONDUCTOR_BUFFER_LENGTH_DEFAULT)));

        final CountersManager countersManager = Tests.newCountersManager(BUFFER_LENGTH);
        final SystemCounters systemCounters = new SystemCounters(countersManager);

        final SenderProxy senderProxy = mock(SenderProxy.class);
        final ReceiverProxy receiverProxy = mock(ReceiverProxy.class);
        final ManyToOneConcurrentLinkedQueue<Runnable> driverCommandQueue =
            new ManyToOneConcurrentLinkedQueue<>();
        final DriverConductorProxy driverConductorProxy =
            new DriverConductorProxy(driverCommandQueue);
        final OneToOneConcurrentArrayQueue<Runnable> nativeResourceAgentCommandQueue =
            new OneToOneConcurrentArrayQueue<>(CMD_QUEUE_CAPACITY);
        final NativeResourceAgentProxy nativeResourceAgentProxy = new NativeResourceAgentProxy(
            nativeResourceAgentCommandQueue, mock(AtomicCounter.class));
        final ErrorHandler errorHandler = mock(ErrorHandler.class);

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .tempBuffer(new UnsafeBuffer(new byte[METADATA_LENGTH]))
            .ipcTermBufferLength(TERM_BUFFER_LENGTH)
            .toDriverCommands(toDriverCommands)
            .logFactory(new TestLogFactory())
            .clientProxy(mock(ClientProxy.class))
            .senderProxy(senderProxy)
            .receiverProxy(receiverProxy)
            .driverCommandQueue(driverCommandQueue)
            .driverConductorProxy(driverConductorProxy)
            .errorHandler(errorHandler)
            .countedErrorHandler(mock(CountedErrorHandler.class))
            .nativeResourceAgentCommandQueue(nativeResourceAgentCommandQueue)
            .nativeResourceAgentProxy(nativeResourceAgentProxy)
            .epochClock(SystemEpochClock.INSTANCE)
            .cachedEpochClock(new CachedEpochClock())
            .cachedNanoClock(new CachedNanoClock())
            .countersManager(countersManager)
            .systemCounters(systemCounters)
            .nameResolver(DefaultNameResolver.INSTANCE)
            .nanoClock(new CachedNanoClock())
            .threadingMode(ThreadingMode.DEDICATED)
            .conductorDutyCycleTracker(new DutyCycleTracker())
            .nameResolverTimeTracker(new DutyCycleTracker())
            .resourceFreeLimit(1);

        nativeResourceAgent = new NativeResourceAgent(ctx);
        nativeResourceAgentProxy.nativeResourceAgent(nativeResourceAgent);

        driverProxy = new DriverProxy(toDriverCommands, CLIENT_ID);
        driverConductor = new DriverConductor(ctx);
        driverConductor.onStart();

        driverProxy.addPublication(CommonContext.IPC_CHANNEL, STREAM_ID);
        driverConductor.doWork();
        nativeResourceAgent.doWork();
        driverConductor.doWork();

        ipcPublication = driverConductor.findSharedIpcPublication(STREAM_ID, Aeron.NULL_VALUE);
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
        nativeResourceAgent.doWork();

        assertThat(publisherLimit.get(), is(greaterThan(0L)));
    }
}
