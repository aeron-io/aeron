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

package io.aeron.archive.client;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.RethrowingErrorHandler;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.PersistentSubscriptionException.Reason;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.SystemUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static io.aeron.AeronCounters.FLOW_CONTROL_RECEIVERS_COUNTER_TYPE_ID;
import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.CommonContext.IPC_MEDIA;
import static io.aeron.CommonContext.UDP_CHANNEL;
import static io.aeron.Publication.BACK_PRESSURED;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class PersistentSubscriptionTest
{
    private static final int TERM_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int STREAM_ID = 1000;
    private static final String MDC_CHANNEL = UDP_CHANNEL + "?control=localhost:2000";
    private static final String MDC_PUBLICATION_CHANNEL = UDP_CHANNEL +
        "?control=localhost:2000|control-mode=dynamic|fc=max";

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private final MediaDriver.Context driverCtxTpl = new MediaDriver.Context()
        .termBufferSparseFile(true)
        .threadingMode(ThreadingMode.SHARED)
        .publicationTermBufferLength(TERM_LENGTH)
        .ipcTermBufferLength(TERM_LENGTH)
        .dirDeleteOnShutdown(true)
        .imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(3))
        .spiesSimulateConnection(true);

    private final Aeron.Context aeronCtxTpl = new Aeron.Context()
        .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE);

    private PersistentSubscription.Context persistentSubscriptionCtx;

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private TestMediaDriver driver;
    private File archiveDir;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive aeronArchive;
    private PersistentSubscriptionListenerImpl listener;
    private AeronArchive.Context aeronArchiveContext;
    private BufferingFragmentHandler fragmentHandler;

    @BeforeEach
    void setUp()
    {
        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        final MediaDriver.Context driverCtx = driverCtxTpl.clone()
            .aeronDirectoryName(aeronDirectoryName);

        archiveDir = new File(SystemUtil.tmpDirName(), "archive");

        final Archive.Context archiveCtx = TestContexts.localhostArchive()
            .catalogCapacity(128 * 1024)
            .segmentFileLength(TERM_LENGTH)
            .aeronDirectoryName(aeronDirectoryName)
            .deleteArchiveOnStart(true)
            .archiveDir(archiveDir)
            .threadingMode(ArchiveThreadingMode.SHARED);

        driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());
        archive = Archive.launch(archiveCtx);
        systemTestWatcher.dataCollector().add(archiveCtx.archiveDir());

        aeron = Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeronDirectoryName));

        aeronArchiveContext = TestContexts.localhostAeronArchive().aeron(aeron);
        aeronArchive = AeronArchive.connect(aeronArchiveContext.clone());

        listener = new PersistentSubscriptionListenerImpl();

        persistentSubscriptionCtx = new PersistentSubscription.Context()
            .recordingId(13)
            .startPosition(0)
            .liveChannel(IPC_CHANNEL)
            .liveStreamId(STREAM_ID)
            .listener(listener)
            .aeronArchiveContext(aeronArchiveContext);

        fragmentHandler = new BufferingFragmentHandler();
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(
            this::closeCloseables,
            aeronArchive,
            aeron,
            archive,
            driver,
            () -> IoUtil.delete(archiveDir, true));
    }

    @Test
    @InterruptAfter(10)
    void shouldNotRequireEventListener()
    {
        final PersistentSubscriptionListenerImpl listener = null; // <-- null listener
        persistentSubscriptionCtx.listener(listener);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::hasFailed, () -> persistentSubscription.controlledPoll(null, 1));
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldErrorIfRecordingDoesNotExist()
    {
        final int recordingId = 13; // <-- does not exist
        persistentSubscriptionCtx.recordingId(recordingId);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::hasFailed, () -> persistentSubscription.controlledPoll(null, 1));

            assertEquals(1, listener.errorCount);
            Assertions.assertEquals(
                Reason.RECORDING_NOT_FOUND,
                ((PersistentSubscriptionException)listener.lastException).reason()
            );
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldErrorIfRecordingStreamDoesNotMatchLiveStream()
    {
        final int liveStreamId = 1001; // <-- not the same as the recorded stream.
        final ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(IPC_CHANNEL, STREAM_ID);
        final CountersReader counters = aeron.countersReader();
        final int counterId =
            Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);

        persistentSubscriptionCtx
            .recordingId(recordingId)
            .liveStreamId(liveStreamId);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::hasFailed, () -> persistentSubscription.controlledPoll(null, 1));

            assertEquals(1, listener.errorCount);
            assertEquals(
                Reason.STREAM_ID_MISMATCH,
                ((PersistentSubscriptionException)listener.lastException).reason()
            );
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldErrorIfRecordingPositionIsBeforeStartPosition()
    {
        final String channel = new ChannelUriStringBuilder()
            .media(IPC_MEDIA)
            .initialPosition(1024, 0, TERM_LENGTH) // <-- Recording starts at 1024
            .build();
        final ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(channel, STREAM_ID);
        final CountersReader counters = aeron.countersReader();
        final int counterId =
            Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);

        final int startPosition = 0; // <-- Trying to start from zero

        persistentSubscriptionCtx
            .recordingId(recordingId)
            .startPosition(startPosition);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::hasFailed, () -> persistentSubscription.controlledPoll(null, 1));

            assertEquals(1, listener.errorCount);
            assertEquals(
                Reason.INVALID_START_POSITION,
                ((PersistentSubscriptionException)listener.lastException).reason()
            );
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldErrorIfRecordingPositionIsAfterStopPosition()
    {
        final ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(IPC_CHANNEL, STREAM_ID);
        final CountersReader counters = aeron.countersReader();
        final int counterId =
            Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);

        offerPayloads(List.of(new byte[1024]), publication, counters, counterId);

        aeronArchive.stopRecording(publication);
        final long stopPosition = aeronArchive.getStopPosition(recordingId);
        assertTrue(stopPosition > 0);

        final long startPosition = stopPosition * 2; // <-- after end of recording
        persistentSubscriptionCtx
            .recordingId(recordingId)
            .startPosition(startPosition);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::hasFailed, () -> persistentSubscription.controlledPoll(null, 1));

            assertEquals(1, listener.errorCount);
            assertEquals(
                Reason.INVALID_START_POSITION,
                ((PersistentSubscriptionException)listener.lastException).reason()
            );
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 10 })
    @InterruptAfter(5)
    void shouldReplayExistingRecording(int fragmentLimit)
    {
        final ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(IPC_CHANNEL, STREAM_ID);

        final CountersReader counters = aeron.countersReader();
        final int counterId =
            Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);

        final List<byte[]> payloads = generateRandomPayloads(5);
        offerPayloads(payloads, publication, counters, counterId);

        persistentSubscriptionCtx
            .recordingId(recordingId);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(1),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            assertEquals(1, archive.context().replaySessionCounter().get());
            assertTrue(persistentSubscription.isReplaying());

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(payloads.size()), () ->
                persistentSubscription.controlledPoll(fragmentHandler, fragmentLimit));

            assertPayloads(fragmentHandler.receivedPayloads, payloads);

            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, fragmentLimit));

            assertEquals(payloads.size(), fragmentHandler.receivedPayloads.size());

            // send some more messages
            final List<byte[]> payloads2 = generateRandomPayloads(5);
            offerPayloads(payloads2, publication, counters, counterId);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(payloads.size() + payloads2.size()),
                () ->
                {
                    persistentSubscription.controlledPoll(fragmentHandler, fragmentLimit);

                    // expect remaining messages to be consumed on live channel
                    assertTrue(persistentSubscription.isLive());
                });

            assertTrue(persistentSubscription.isLive());
            assertFalse(persistentSubscription.isReplaying());

            Tests.await(() -> archive.context().replaySessionCounter().get() == 0);
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldTransitionFromReplayToLiveWhileLiveIsAdvancing()
    {
        final ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(IPC_CHANNEL, STREAM_ID);

        final CountersReader counters = aeron.countersReader();
        final int counterId =
            Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);

        final List<byte[]> payloads = generateRandomPayloads(5);
        offerPayloads(payloads, publication, counters, counterId);

        persistentSubscriptionCtx
            .recordingId(recordingId);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(1),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            assertEquals(1, archive.context().replaySessionCounter().get());
            assertTrue(persistentSubscription.isReplaying());

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(payloads.size()), () ->
                persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, payloads);

            // send some more messages
            final List<byte[]> payloads2 = generateRandomPayloads(1);
            offerPayloads(payloads2, publication, counters, counterId);

            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertFalse(persistentSubscription.isReplaying());

            Tests.await(() -> archive.context().replaySessionCounter().get() == 0);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 10 })
    @InterruptAfter(5)
    void shouldCatchupOnReplayBeforeSwitchingToLive(final int fragmentLimit)
    {
        final ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(IPC_CHANNEL, STREAM_ID);

        final CountersReader counters = aeron.countersReader();
        final int counterId =
            Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);

        final List<byte[]> payloads = generateRandomPayloads(5);
        offerPayloads(payloads, publication, counters, counterId);

        persistentSubscriptionCtx
            .recordingId(recordingId);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(1),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            assertEquals(1, archive.context().replaySessionCounter().get());
            assertTrue(persistentSubscription.isReplaying());

            final List<byte[]> payloads2 = generateRandomPayloads(5);
            offerPayloads(payloads2, publication, counters, counterId);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(payloads.size() + payloads2.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, fragmentLimit));

            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, fragmentLimit));

            assertEquals(payloads.size() + payloads2.size(), fragmentHandler.receivedPayloads.size());

            // send some more messages
            final List<byte[]> payloads3 = generateRandomPayloads(5);
            offerPayloads(payloads3, publication, counters, counterId);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(payloads.size() + payloads2.size() + payloads3.size()),
                () ->
                {
                    persistentSubscription.controlledPoll(fragmentHandler, fragmentLimit);

                    // expect remaining messages to be consumed on live channel
                    assertTrue(persistentSubscription.isLive());
                });

            assertTrue(persistentSubscription.isLive());
            assertFalse(persistentSubscription.isReplaying());
            assertTrue(persistentSubscription.joinError() > 0);

            final List<byte[]> allPayloads = new ArrayList<>();
            allPayloads.addAll(payloads);
            allPayloads.addAll(payloads2);
            allPayloads.addAll(payloads3);

            assertPayloads(fragmentHandler.receivedPayloads, allPayloads);

            Tests.await(() -> archive.context().replaySessionCounter().get() == 0);
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldHandleReplayBeingAheadOfLive()
    {
        final String pubChannel = "aeron:udp?control=localhost:2000|control-mode=dynamic|fc=min";
        final String subChannel = "aeron:udp?control=localhost:2000|rcv-wnd=4k";

        final ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(pubChannel, STREAM_ID);

        final CountersReader counters = aeron.countersReader();
        final int counterId =
            Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);

        final int receiversCounterId = counters.findByTypeIdAndRegistrationId(
            FLOW_CONTROL_RECEIVERS_COUNTER_TYPE_ID, publication.registrationId());
        assertNotEquals(NULL_COUNTER_ID, receiversCounterId);

        final String aeronDir2 = CommonContext.generateRandomDirName();
        final MediaDriver.Context driver2Ctx = driverCtxTpl.clone().aeronDirectoryName(aeronDir2);
        addCloseable(TestMediaDriver.launch(driver2Ctx, systemTestWatcher));
        systemTestWatcher.dataCollector().add(driver2Ctx.aeronDirectory());
        final Aeron aeron2 = addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeronDir2)));

        final Subscription subscription = aeron2.addSubscription(subChannel, STREAM_ID);
        Tests.awaitConnected(subscription);

        offerPayloads(generateFixedPayloads(32, 1024 - DataHeaderFlyweight.HEADER_LENGTH), publication, counters, counterId);

        persistentSubscriptionCtx
            .recordingId(recordingId)
            .liveChannel(subChannel);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(32),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            awaitCounterValueEq(counters, receiversCounterId, 2);

            executeUntil(persistentSubscription::isLive,
                () ->
                {
                    persistentSubscription.controlledPoll(fragmentHandler, 10);
                    subscription.poll((b, o, l, h) -> {}, 10);
                });

            assertEquals(-28 * 1024L, persistentSubscription.joinError());
        }
    }

    @Test
    @InterruptAfter(15)
    void shouldDropFromLiveBackToReplay() throws InterruptedException
    {
        final ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(MDC_PUBLICATION_CHANNEL,
            STREAM_ID);

        final CountersReader counters = aeron.countersReader();
        final int counterId =
            Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);

        final List<byte[]> payloads = generateRandomPayloads(5);
        offerPayloads(payloads, publication, counters, counterId);

        persistentSubscriptionCtx
            .recordingId(recordingId)
            .liveChannel(MDC_CHANNEL);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            //            while (listener.onLiveCount == 0) // TODO should get the onLiveCallback
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(payloads.size()), () ->
            {
                persistentSubscription.controlledPoll(fragmentHandler, 10);
                if (fragmentHandler.receivedPayloads.size() == 1)
                {
                    assertEquals(1, archive.context().replaySessionCounter().get());
                    assertTrue(persistentSubscription.isReplaying());
                }
            });

            assertPayloads(fragmentHandler.receivedPayloads, payloads);

            Thread.sleep(100); // TODO

            // send some more messages
            final List<byte[]> payloads2 = generateRandomPayloads(5);
            offerPayloads(payloads2, publication, counters, counterId);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(payloads.size() + payloads2.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );

            assertTrue(persistentSubscription.isLive());

            Tests.await(() -> archive.context().replaySessionCounter().get() == 0);

            final MediaDriver.Context ctx =
                new MediaDriver.Context().aeronDirectoryName(CommonContext.generateRandomDirName());
            try (MediaDriver mediaDriver = MediaDriver.launch(ctx);
                Aeron aeron = Aeron.connect(
                    new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName())))
            {
                final Subscription subscription = aeron.addSubscription(MDC_CHANNEL, STREAM_ID);

                Tests.awaitConnected(subscription);

                for (int i = 0; i < 64; i++)
                {
                    offerPayloads(List.of(new byte[1024]), publication, counters, counterId);

                    while (subscription.poll((buffer1, offset, length, header) ->
                    {
                    }, 1) < 1)
                    {
                        Tests.yield();
                    }
                }

                executeUntil(() -> !persistentSubscription.isLive(), () -> persistentSubscription.controlledPoll(
                    (buffer, offset, length, header) -> ControlledFragmentHandler.Action.CONTINUE, 10));
                assertTrue(persistentSubscription.isReplaying());
            }
        }
    }

    @Test
    @InterruptAfter(60)
    void testLiveJoin() throws Exception
    {
        final String pubChannel = "aeron:udp?term-length=16m|control=localhost:24325|control-mode=dynamic|fc=min";
        final String subChannel = "aeron:udp?control=localhost:24325|group=true";

        final String aeronDir2 = CommonContext.generateRandomDirName();
        final MediaDriver.Context driver2Ctx = driverCtxTpl.clone().aeronDirectoryName(aeronDir2);
        addCloseable(TestMediaDriver.launch(driver2Ctx, systemTestWatcher));
        systemTestWatcher.dataCollector().add(driver2Ctx.aeronDirectory());
        final Aeron aeron2 = addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeronDir2)));

        final ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(pubChannel, STREAM_ID);
        final Subscription controlSubscription = aeron.addSubscription(subChannel, STREAM_ID);
        Tests.awaitConnected(controlSubscription);

        final CountersReader counters = aeron.countersReader();
        final int counterId =
            Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);

        final int maxSeconds = 60;
        final int ratePerSecond = 10_000;
        final long maxProcessingTime = 1_000_000_000 / ratePerSecond / 2;
        final long t0 = System.nanoTime();
        final PerSecondStats publisherMessagesPerSecond = new PerSecondStats(t0, maxSeconds);
        final PerSecondStats publisherBpePerSecond = new PerSecondStats(t0, maxSeconds);
        final PerSecondStats controlMessagesPerSecond = new PerSecondStats(t0, maxSeconds);

        final Thread control = new Thread(
            () ->
            {
                final FragmentAssembler handler = new FragmentAssembler(
                    (buffer, offset, length, header) ->
                    {
                        controlMessagesPerSecond.record(System.nanoTime());
                        simulateWork(maxProcessingTime);
                    });
                while (!Thread.currentThread().isInterrupted())
                {
                    controlSubscription.poll(handler, 10);
                }
            },
            "testLiveJoinControl");
        control.start();
        addCloseable(() -> interruptAndJoin(control));

        final Thread publisher = new Thread(
            () ->
            {
                final ThreadLocalRandom random = ThreadLocalRandom.current();
                final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]); // TODO increase to test fragmentation
                long messageId = 0;
                long nextMessageAt = System.nanoTime() + exponentialArrivalDelay(ratePerSecond);
                while (!Thread.currentThread().isInterrupted())
                {
                    final long now = System.nanoTime();
                    if (now - nextMessageAt >= 0)
                    {
                        final int length = random.nextInt(2 * SIZE_OF_LONG, buffer.capacity() + 1);
                        buffer.putLong(0, messageId);
                        buffer.putLong(length - SIZE_OF_LONG, messageId);
                        final long result = publication.offer(buffer, 0, length);
                        if (result > 0)
                        {
                            messageId++;
                            nextMessageAt = now + exponentialArrivalDelay(ratePerSecond);
                            publisherMessagesPerSecond.record(now);
                        }
                        else if (result == BACK_PRESSURED)
                        {
                            publisherBpePerSecond.record(now);
                        }
                    }
                }
            },
            "testLiveJoinPublisher");
        publisher.start();
        addCloseable(() -> interruptAndJoin(publisher));

        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));

        try (PersistentSubscription persistentSubscription =
            PersistentSubscription.create(new PersistentSubscription.Context()
                .recordingId(recordingId)
                .startPosition(0)
                .liveChannel(subChannel)
                .liveStreamId(STREAM_ID)
                .listener(null)
                .aeronArchiveContext(TestContexts.localhostAeronArchive().aeron(aeron2))))
        {
            final MessageVerifier handler = new MessageVerifier(maxProcessingTime);
            final ControlledFragmentAssembler assembler = new ControlledFragmentAssembler(handler);

            while (!persistentSubscription.isLive())
            {
                if (persistentSubscription.controlledPoll(assembler, 10) == 0)
                {
                    checkForInterrupt("failed to transition to live");
                }
            }

            interruptAndJoin(publisher);
            final long lastPosition = publication.position();

            while (handler.position < lastPosition)
            {
                if (persistentSubscription.controlledPoll(assembler, 10) == 0)
                {
                    checkForInterrupt("failed to drain the stream");
                }
            }

            interruptAndJoin(control);

            final int elapsedSeconds = (int)((System.nanoTime() - t0 + 999_999_999) / 1_000_000_000);

            System.out.println("join error = " + persistentSubscription.joinError());
            System.out.println("expected rate per second = " + ratePerSecond);
            System.out.println("second,published,publisherBpe,control");
            for (int i = 0; i < elapsedSeconds; i++)
            {
                System.out.println(i +
                    "," + publisherMessagesPerSecond.get(i) +
                    "," + publisherBpePerSecond.get(i) +
                    "," + controlMessagesPerSecond.get(i));
            }
        }
    }

    private static void interruptAndJoin(final Thread thread) throws InterruptedException
    {
        thread.interrupt();
        thread.join();
    }

    private static void checkForInterrupt(final String message)
    {
        if (Thread.interrupted())
        {
            fail(message);
        }
    }

    private static void simulateWork(final long maxProcessingTime)
    {
        if (maxProcessingTime > 0)
        {
            LockSupport.parkNanos(ThreadLocalRandom.current().nextLong(maxProcessingTime));
        }
    }

    private static long exponentialArrivalDelay(final long ratePerSecond)
    {
        final double uniform = ThreadLocalRandom.current().nextDouble();
        final double secondFraction = -Math.log(1.0 - uniform) / ratePerSecond;
        return (long)(secondFraction * 1e9);
    }

    private static final class MessageVerifier implements ControlledFragmentHandler
    {
        private final long maxProcessingTime;
        long expectedMessageId;
        long position;

        private MessageVerifier(final long maxProcessingTime)
        {
            this.maxProcessingTime = maxProcessingTime;
        }

        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            if (length < 2 * SIZE_OF_LONG)
            {
                throw new IllegalStateException("length was " + length);
            }
            final long messageId1 = buffer.getLong(offset);
            final long messageId2 = buffer.getLong(offset + length - SIZE_OF_LONG);
            if (messageId1 != messageId2)
            {
                throw new IllegalStateException("message had different ids " + messageId1 + " and " + messageId2);
            }
            if (messageId1 != expectedMessageId)
            {
                throw new IllegalStateException("expected id " + expectedMessageId + ", but got " + messageId1);
            }
            expectedMessageId = messageId1 + 1;
            position = header.position();
            simulateWork(maxProcessingTime);
            return Action.CONTINUE;
        }
    }

    private static final class PerSecondStats
    {
        private final long[] perSecond;
        private final long t0;

        PerSecondStats(final long t0, final int maxSeconds)
        {
            this.t0 = t0;
            this.perSecond = new long[maxSeconds];
        }

        void record(final long ts)
        {
            final int second = (int)((ts - t0) / 1_000_000_000);
            if (second < perSecond.length)
            {
                perSecond[second]++;
            }
        }

        long get(final int second)
        {
            return perSecond[second];
        }
    }

    private <T extends AutoCloseable> T addCloseable(final T closeable)
    {
        closeables.add(closeable);
        return closeable;
    }

    private void closeCloseables() throws Exception
    {
        Exception ex = null;

        for (int i = closeables.size() - 1; i >= 0; i--)
        {
            final AutoCloseable closeable = closeables.get(i);
            try
            {
                closeable.close();
            }
            catch (final Exception e)
            {
                if (ex == null)
                {
                    ex = e;
                }
                else
                {
                    ex.addSuppressed(e);
                }
            }
        }

        if (ex != null)
        {
            throw ex;
        }
    }

    private List<byte[]> generateFixedPayloads(final int count, final int size)
    {
        final byte[] payload = new byte[size];
        final List<byte[]> payloads = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
        {
            payloads.add(payload);
        }
        return payloads;
    }

    private List<byte[]> generateRandomPayloads(final int count)
    {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final List<byte[]> randomPayloads = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
        {
            final int length = random.nextInt(1024); // TODO increase later to add fragmentation
            final byte[] bytes = new byte[length];
            random.nextBytes(bytes);
            randomPayloads.add(bytes);
        }
        return randomPayloads;
    }

    private static void awaitCounterValueEq(
        final CountersReader countersReader,
        final int counterId,
        final long expectedValue)
    {
        while (true)
        {
            final long counterValue = countersReader.getCounterValue(counterId);
            if (counterValue == expectedValue)
            {
                break;
            }
            if (Thread.interrupted())
            {
                fail("timed out waiting for counter " + counterId + " to become equal to " + expectedValue +
                     ", last value was " + counterValue);
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
    }

    private static void executeUntil(final BooleanSupplier predicate, final Runnable runnable)
    {
        Tests.await(() ->
        {
            runnable.run();
            return predicate.getAsBoolean();
        });
    }

    private void offerPayloads(
        final List<byte[]> payloads,
        final Publication publication,
        final CountersReader counters,
        final int counterId)
    {
        final UnsafeBuffer buffer = new UnsafeBuffer();

        long offeredPosition = 0;
        for (final byte[] payload : payloads)
        {
            buffer.wrap(payload);
            while ((offeredPosition = publication.offer(buffer)) < 0)
            {
                Tests.yieldingIdle("failed to offer. offerPosition = " + Publication.errorString(offeredPosition));
            }
        }
        Tests.awaitPosition(counters, counterId, offeredPosition);
    }

    private void assertPayloads(final List<byte[]> receivedPayloads, final List<byte[]> payloads)
    {
        for (int i = 0; i < receivedPayloads.size(); i++)
        {
            assertArrayEquals(payloads.get(i), receivedPayloads.get(i));
        }
    }

    private static final class BufferingFragmentHandler implements ControlledFragmentHandler
    {
        private final List<byte[]> receivedPayloads = new ArrayList<>();

        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            final byte[] bytes = new byte[length];
            buffer.getBytes(offset, bytes);
            receivedPayloads.add(bytes);
            return Action.CONTINUE;
        }

        boolean hasReceivedPayloads(final int numberOfPayloads)
        {
            return receivedPayloads.size() >= numberOfPayloads;
        }
    }

    private static final class PersistentSubscriptionListenerImpl implements PersistentSubscriptionListener
    {
        long onLiveCount;
        int errorCount = 0;
        Exception lastException = null;

        public void onLive()
        {
            onLiveCount++;
        }

        public void onError(final Exception e)
        {
            errorCount++;
            lastException = e;
        }
    }
}
