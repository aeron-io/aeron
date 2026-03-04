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
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
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
import io.aeron.driver.ReceiveChannelEndpointSupplier;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.ext.DebugReceiveChannelEndpoint;
import io.aeron.driver.ext.LossGenerator;
import io.aeron.driver.status.SubscriberPos;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.RandomWatcher;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.StreamIdLossGenerator;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static io.aeron.AeronCounters.FLOW_CONTROL_RECEIVERS_COUNTER_TYPE_ID;
import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.CommonContext.IPC_MEDIA;
import static io.aeron.CommonContext.SESSION_ID_PARAM_NAME;
import static io.aeron.CommonContext.SPY_PREFIX;
import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.archive.client.PersistentSubscription.FROM_LIVE;
import static io.aeron.archive.client.PersistentSubscription.FROM_START;
import static io.aeron.driver.status.StreamCounter.CHANNEL_OFFSET;
import static io.aeron.driver.status.StreamCounter.STREAM_ID_OFFSET;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class PersistentSubscriptionTest
{
    private static final int ONE_KB_MESSAGE_SIZE = 1024 - DataHeaderFlyweight.HEADER_LENGTH;
    private static final int TERM_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int STREAM_ID = 1000;
    private static final String MDC_SUBSCRIPTION_CHANNEL = "aeron:udp?control=localhost:2000";
    private static final String MDC_PUBLICATION_CHANNEL =
        "aeron:udp?control=localhost:2000|control-mode=dynamic|fc=max";
    private static final String UNICAST_CHANNEL = "aeron:udp?endpoint=localhost:2000";
    private static final String MULTICAST_CHANNEL = "aeron:udp?endpoint=224.0.1.1:40456|interface=localhost";

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @RegisterExtension
    final RandomWatcher randomWatcher = new RandomWatcher();

    private final MediaDriver.Context driverCtxTpl = new MediaDriver.Context()
        .termBufferSparseFile(true)
        .threadingMode(ThreadingMode.SHARED)
        .publicationTermBufferLength(TERM_LENGTH)
        .ipcTermBufferLength(TERM_LENGTH)
        .dirDeleteOnShutdown(true)
        .imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(3))
        .untetheredWindowLimitTimeoutNs(TimeUnit.SECONDS.toNanos(2))
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

        final AeronArchive.Context aeronArchiveContext = TestContexts.localhostAeronArchive().aeron(aeron);
        aeronArchive = AeronArchive.connect(aeronArchiveContext.clone());

        listener = new PersistentSubscriptionListenerImpl();

        persistentSubscriptionCtx = new PersistentSubscription.Context()
            .aeron(aeron)
            .recordingId(13)
            .startPosition(0)
            .liveChannel(IPC_CHANNEL)
            .liveStreamId(STREAM_ID)
            .replayChannel("aeron:udp?endpoint=localhost:0")
            .replayStreamId(-5)
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

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
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

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, channel, STREAM_ID);

        final int startPosition = 0; // <-- Trying to start from zero

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
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
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        persistentPublication.persist(List.of(new byte[1024]));

        final long stopPosition = persistentPublication.stop();
        assertTrue(stopPosition > 0);

        final long startPosition = stopPosition * 2; // <-- after end of recording
        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
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
    void shouldNotReplayOldMessagesWhenStartingFromLive()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        final List<byte[]> oldMessages = generateRandomPayloads(5);
        persistentPublication.persist(oldMessages);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(FROM_LIVE);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertEquals(0, fragmentHandler.receivedPayloads.size());

            final List<byte[]> newMessages = generateRandomPayloads(3);
            persistentPublication.persist(newMessages);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(newMessages.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, newMessages);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldReplayFromRecordingStartPositionWhenStartingFromStart()
    {
        final String channel = new ChannelUriStringBuilder()
            .media(IPC_MEDIA)
            .initialPosition(1024, 0, TERM_LENGTH) // <-- Recording starts at 1024
            .build();

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, channel, STREAM_ID);

        final List<byte[]> oldMessages = generateRandomPayloads(5);
        persistentPublication.persist(oldMessages);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(FROM_START);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            final List<byte[]> newMessages = generateRandomPayloads(3);
            persistentPublication.persist(newMessages);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(oldMessages.size() + newMessages.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, oldMessages, newMessages);
        }
    }

    @ParameterizedTest
    @MethodSource("fragmentLimitsAndChannels")
    @InterruptAfter(5)
    void shouldReplayExistingRecordingThenJoinLive(final int fragmentLimit, final String channel)
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, channel, STREAM_ID);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .liveChannel(channel);

        final List<byte[]> payloads = generateRandomPayloads(5);
        persistentPublication.persist(payloads);

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
            persistentPublication.persist(payloads2);

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

    @ParameterizedTest
    @MethodSource("replayChannelsAndStreams")
    @InterruptAfter(5)
    void shouldReplayOverConfiguredChannel(final String replayChannel, final int replayStreamId)
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        final List<byte[]> payloads = generateRandomPayloads(5);
        persistentPublication.persist(payloads);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .replayChannel(replayChannel)
            .replayStreamId(replayStreamId);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(1),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            assertTrue(persistentSubscription.isReplaying());

            final MutableLong replaySubPos = new MutableLong(-1);
            final CountersReader counters = aeron.countersReader();
            counters.forEach((counterId1, typeId, keyBuffer, label) ->
            {
                if (typeId == SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID)
                {
                    final int streamId = keyBuffer.getInt(STREAM_ID_OFFSET);
                    if (streamId == replayStreamId)
                    {
                        assertEquals(replayChannel, removeSessionId(keyBuffer.getStringAscii(CHANNEL_OFFSET)));
                        replaySubPos.set(counters.getCounterValue(counterId1));
                    }
                }
            });
            assertEquals(fragmentHandler.position, replaySubPos.get());

            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, payloads);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 10 })
    @InterruptAfter(5)
    void shouldReplayExistingRecordingThenSpyOnLive(final int fragmentLimit)
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final List<byte[]> payloads = generateFixedPayloads(8, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(payloads);

        persistentSubscriptionCtx
            .liveChannel(SPY_PREFIX + "aeron:udp?control=localhost:2000")
            .recordingId(persistentPublication.recordingId());

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(8),
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
            final List<byte[]> payloads2 = generateFixedPayloads(16, ONE_KB_MESSAGE_SIZE);
            persistentPublication.persist(payloads2);

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
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        final List<byte[]> payloads = generateRandomPayloads(5);
        persistentPublication.persist(payloads);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId());

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
            persistentPublication.persist(payloads2);

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
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        final List<byte[]> payloads = generateRandomPayloads(5);
        persistentPublication.persist(payloads);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId());

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(1),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            assertEquals(1, archive.context().replaySessionCounter().get());
            assertTrue(persistentSubscription.isReplaying());

            final List<byte[]> payloads2 = generateRandomPayloads(25);
            persistentPublication.persist(payloads2);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(payloads.size() + payloads2.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, fragmentLimit));

            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, fragmentLimit));

            assertEquals(payloads.size() + payloads2.size(), fragmentHandler.receivedPayloads.size());

            // send some more messages
            final List<byte[]> payloads3 = generateRandomPayloads(5);
            persistentPublication.persist(payloads3);

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
//            assertThat(persistentSubscription.joinError(), greaterThan(0L)); // TODO this test does not guarantee this.
            assertPayloads(fragmentHandler.receivedPayloads, payloads, payloads2, payloads3);

            Tests.await(() -> archive.context().replaySessionCounter().get() == 0);
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldHandleReplayBeingAheadOfLive()
    {
        final String pubChannel = "aeron:udp?control=localhost:2000|control-mode=dynamic|fc=min";
        final String subChannel = "aeron:udp?control=localhost:2000|rcv-wnd=4k";

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, pubChannel, STREAM_ID);

        final String aeronDir2 = CommonContext.generateRandomDirName();
        final MediaDriver.Context driver2Ctx = driverCtxTpl.clone().aeronDirectoryName(aeronDir2);
        addCloseable(TestMediaDriver.launch(driver2Ctx, systemTestWatcher));
        systemTestWatcher.dataCollector().add(driver2Ctx.aeronDirectory());
        final Aeron aeron2 = addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeronDir2)));

        final Subscription subscription = aeron2.addSubscription(subChannel, STREAM_ID);
        Tests.awaitConnected(subscription);

        persistentPublication.persist(generateFixedPayloads(32, ONE_KB_MESSAGE_SIZE));

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .liveChannel(subChannel);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(32),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            executeUntil(
                () -> persistentPublication.receiverCount() == 2,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );

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
    void shouldDropFromLiveBackToReplayThenJoinLiveAgain()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final List<byte[]> payloads = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(payloads);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(1),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            assertEquals(1, archive.context().replaySessionCounter().get());
            assertTrue(persistentSubscription.isReplaying());

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(payloads.size()), () ->
                persistentSubscription.controlledPoll(fragmentHandler, 10));

            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertEquals(payloads.size(), fragmentHandler.receivedPayloads.size());

            // send some more messages
            final List<byte[]> payloads2 = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
            persistentPublication.persist(payloads2);

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
                final CountingFragmentHandler fastSubscriptionFragmentHandler = new CountingFragmentHandler();
                final Subscription subscription = aeron.addSubscription(MDC_SUBSCRIPTION_CHANNEL, STREAM_ID);

                Tests.awaitConnected(subscription);

                final List<byte[]> payloads3 = generateFixedPayloads(64, ONE_KB_MESSAGE_SIZE);
                persistentPublication.persist(payloads3);

                executeUntil(
                    () -> fastSubscriptionFragmentHandler.hasReceivedPayloads(64),
                    () -> subscription.poll(fastSubscriptionFragmentHandler, 1)
                );

                executeUntil(
                    persistentSubscription::isReplaying,
                    () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
                );
                assertTrue(persistentSubscription.isReplaying());

                final List<byte[]> payloads4 = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
                persistentPublication.persist(payloads4);

                final int expectedMessageCount =
                    payloads.size() + payloads2.size() + payloads3.size() + payloads4.size();

                executeUntil(
                    () -> fragmentHandler.hasReceivedPayloads(expectedMessageCount) && persistentSubscription.isLive(),
                    () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

                assertPayloads(fragmentHandler.receivedPayloads, payloads, payloads2, payloads3, payloads4);
            }
        }
    }

    @Test
    @InterruptAfter(15)
    void shouldStartFromLiveWhenThereIsNoDataToReplay()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertEquals(0, fragmentHandler.receivedPayloads.size());

            // send some messages
            final List<byte[]> payloads = generateRandomPayloads(5);
            persistentPublication.persist(payloads);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(payloads.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );

            Tests.await(() -> archive.context().replaySessionCounter().get() == 0);
        }
    }

    @Test
    @InterruptAfter(15)
    void anUntetheredPersistentSubscriptionCanFallBehindATetheredSubscription()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, UNICAST_CHANNEL, STREAM_ID);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .liveChannel(UNICAST_CHANNEL + "|tether=false"); // <-- persistentSubscription is untethered

        final CountingFragmentHandler fastSubscriptionFragmentHandler = new CountingFragmentHandler();
        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx);
            Subscription subscription = aeron.addSubscription(UNICAST_CHANNEL + "|tether=true", STREAM_ID))
        {
            Tests.awaitConnected(subscription);

            executeUntil(
                persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );

            persistentPublication.persist(generateFixedPayloads(64, ONE_KB_MESSAGE_SIZE));

            executeUntil(
                () -> fastSubscriptionFragmentHandler.hasReceivedPayloads(64),
                () -> subscription.poll(fastSubscriptionFragmentHandler, 10)
            );

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(64),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1)
            );

            assertTrue(persistentSubscription.isReplaying());

            executeUntil(
                persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1)
            );
        }
    }

    @Test
    @InterruptAfter(15)
    void aTetheredPersistentSubscriptionDoesNotFallBehindAnUntetheredSubscription()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, UNICAST_CHANNEL, STREAM_ID);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .liveChannel(UNICAST_CHANNEL + "|tether=true"); // <-- persistentSubscription is tethered

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx);
            Subscription subscription = aeron.addSubscription(UNICAST_CHANNEL + "|tether=false", STREAM_ID))
        {
            Tests.awaitConnected(subscription);

            executeUntil(
                persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );

            persistentPublication.persist(generateFixedPayloads(64, ONE_KB_MESSAGE_SIZE));

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(64),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1)
            );

            assertTrue(persistentSubscription.isLive());

            Tests.await(() -> !subscription.isConnected());
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldAssembleMessages()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        final int sizeRequiringFragmentation = persistentPublication.maxPayloadLength() + 1;
        final byte[] payload0 = new byte[sizeRequiringFragmentation];
        ThreadLocalRandom.current().nextBytes(payload0);
        final byte[] payload1 = new byte[sizeRequiringFragmentation];
        ThreadLocalRandom.current().nextBytes(payload1);

        persistentPublication.persist(List.of(payload0));

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId());

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            persistentPublication.persist(List.of(payload1));

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(2),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            assertPayloads(fragmentHandler.receivedPayloads, List.of(payload0, payload1));
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

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, pubChannel, STREAM_ID);
        final Subscription controlSubscription = aeron.addSubscription(subChannel, STREAM_ID);
        Tests.awaitConnected(controlSubscription);

        persistentSubscriptionCtx
            .aeron(aeron2)
            .recordingId(persistentPublication.recordingId())
            .liveChannel(subChannel)
            .listener(null);

        final int maxSeconds = 60;
        final int ratePerSecond = 10_000;
        final long maxProcessingTime = 1_000_000_000 / ratePerSecond / 8;
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
                final UnsafeBuffer buffer = new UnsafeBuffer(new byte[2048]);
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
                        final long result = persistentPublication.offer(buffer, 0, length);
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

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            final MessageVerifier handler = new MessageVerifier(maxProcessingTime);

            while (!persistentSubscription.isLive())
            {
                if (persistentSubscription.controlledPoll(handler, 10) == 0)
                {
                    checkForInterrupt("failed to transition to live");
                }
            }

            interruptAndJoin(publisher);
            final long lastPosition = persistentPublication.position();

            while (handler.position < lastPosition)
            {
                if (persistentSubscription.controlledPoll(handler, 10) == 0)
                {
                    checkForInterrupt("failed to drain the stream");
                }
            }

            interruptAndJoin(control);

            printResults(t0, persistentSubscription, ratePerSecond, publisherMessagesPerSecond,
                publisherBpePerSecond, controlMessagesPerSecond);
        }
    }

    @Test
    @InterruptAfter(20)
    void shouldRecoverFromReplayChannelNetworkProblems() throws Exception
    {
        shouldRecoverFromNetworkProblems(NetworkFlow.REPLAY);
    }

    @Test
    @InterruptAfter(20)
    void shouldRecoverFromLiveChannelNetworkProblems() throws Exception
    {
        shouldRecoverFromNetworkProblems(NetworkFlow.LIVE);
    }

    private enum NetworkFlow
    {
        REPLAY,
        LIVE,
    }

    private void shouldRecoverFromNetworkProblems(final NetworkFlow victimFlow) throws Exception
    {
        TestMediaDriver.notSupportedOnCMediaDriver("loss generator");

        final String pubChannel = "aeron:udp?term-length=16m|control=localhost:24325|control-mode=dynamic|fc=min";
        final String subChannel = "aeron:udp?control=localhost:24325|group=true";

        final StreamIdLossGenerator lossGenerator = new StreamIdLossGenerator();
        final String aeronDir2 = CommonContext.generateRandomDirName();
        final MediaDriver.Context driver2Ctx = driverCtxTpl.clone().aeronDirectoryName(aeronDir2)
            .receiveChannelEndpointSupplier(receiveChannelEndpointSupplier(lossGenerator));
        addCloseable(TestMediaDriver.launch(driver2Ctx, systemTestWatcher));
        systemTestWatcher.dataCollector().add(driver2Ctx.aeronDirectory());
        final Aeron aeron2 = addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeronDir2)));

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, pubChannel, STREAM_ID);

        persistentSubscriptionCtx
            .aeron(aeron2)
            .recordingId(persistentPublication.recordingId())
            .liveChannel(subChannel);

        final int ratePerSecond = 10_000;
        final long maxProcessingTime = 1_000_000_000 / ratePerSecond / 8;

        final Thread publisher = new Thread(
            () ->
            {
                final ThreadLocalRandom random = ThreadLocalRandom.current();
                final UnsafeBuffer buffer = new UnsafeBuffer(new byte[2048]);
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
                        final long result = persistentPublication.offer(buffer, 0, length);
                        if (result > 0)
                        {
                            messageId++;
                            nextMessageAt = now + exponentialArrivalDelay(ratePerSecond);
                        }
                    }
                }
            },
            "shouldRecoverFromNetworkProblemsPublisher");
        publisher.start();
        addCloseable(() -> interruptAndJoin(publisher));

        final long startTime = System.nanoTime();

        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            final MessageVerifier handler = new MessageVerifier(maxProcessingTime);

            enum LossState
            {
                NOT_STARTED,
                WAITING_TO_START,
                IN_PROGRESS,
                FINISHED,
            }

            LossState lossState = LossState.NOT_STARTED;
            long deadline = 0;

            while (!persistentSubscription.isLive())
            {
                if (persistentSubscription.controlledPoll(handler, 10) == 0)
                {
                    checkForInterrupt("failed to transition to live");
                }

                if (victimFlow == NetworkFlow.REPLAY)
                {
                    if (lossState == LossState.NOT_STARTED && persistentSubscription.isReplaying())
                    {
                        lossState = LossState.WAITING_TO_START;
                        deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
                    }

                    if (lossState == LossState.WAITING_TO_START && System.nanoTime() - deadline >= 0)
                    {
                        lossState = LossState.IN_PROGRESS;
                        deadline = System.nanoTime() +
                                   driver2Ctx.imageLivenessTimeoutNs() + TimeUnit.MILLISECONDS.toNanos(200);
                        lossGenerator.enable(persistentSubscriptionCtx.replayStreamId());
                    }

                    if (lossState == LossState.IN_PROGRESS && System.nanoTime() - deadline >= 0)
                    {
                        lossState = LossState.FINISHED;
                        lossGenerator.disable();
                    }
                }
            }

            if (victimFlow == NetworkFlow.LIVE)
            {
                lossState = LossState.WAITING_TO_START;
                deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);

                while (true)
                {
                    if (persistentSubscription.controlledPoll(handler, 10) == 0)
                    {
                        checkForInterrupt("interrupted while simulating live channel network problems");
                    }

                    if (lossState == LossState.WAITING_TO_START && System.nanoTime() - deadline >= 0)
                    {
                        lossState = LossState.IN_PROGRESS;
                        lossGenerator.enable(persistentSubscriptionCtx.liveStreamId());
                    }

                    if (lossState == LossState.IN_PROGRESS && !persistentSubscription.isLive())
                    {
                        lossState = LossState.FINISHED;
                        lossGenerator.disable();
                    }

                    if (lossState == LossState.FINISHED && persistentSubscription.isLive())
                    {
                        break;
                    }
                }
            }

            interruptAndJoin(publisher);

            final long durationNs = System.nanoTime() - startTime;
            final long minExpectedPosition = TimeUnit.NANOSECONDS.toSeconds(durationNs) * ratePerSecond * 64L;
            final long lastPosition = persistentPublication.position();
            assertThat(lastPosition, greaterThanOrEqualTo(minExpectedPosition));

            while (handler.position < lastPosition)
            {
                if (persistentSubscription.controlledPoll(handler, 10) == 0)
                {
                    checkForInterrupt("failed to drain the stream");
                }
            }
        }
    }

    private static ReceiveChannelEndpointSupplier receiveChannelEndpointSupplier(final LossGenerator lossGenerator)
    {
        return (udpChannel, dispatcher, statusIndicator, context) ->
            new DebugReceiveChannelEndpoint(
                udpChannel, dispatcher, statusIndicator, context, lossGenerator, lossGenerator);
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

    private static void printResults(
        final long t0,
        final PersistentSubscription persistentSubscription,
        final int ratePerSecond,
        final PerSecondStats publisherMessagesPerSecond,
        final PerSecondStats publisherBpePerSecond,
        final PerSecondStats controlMessagesPerSecond)
    {
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
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final List<byte[]> payloads = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
        {
            final byte[] payload = new byte[size];
            random.nextBytes(payload);
            payloads.add(payload);
        }
        return payloads;
    }

    private List<byte[]> generateRandomPayloads(final int count)
    {
        final Random random = randomWatcher.random();
        final List<byte[]> randomPayloads = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
        {
            final int length = random.nextInt(2048);
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

    private void assertPayloads(final List<byte[]> receivedPayloads, final List<byte[]> payloads)
    {
        assertEquals(payloads.size(), receivedPayloads.size());
        for (int i = 0; i < receivedPayloads.size(); i++)
        {
            assertArrayEquals(payloads.get(i), receivedPayloads.get(i));
        }
    }

    @SafeVarargs
    private void assertPayloads(final List<byte[]> receivedPayloads, final List<byte[]>... payloads)
    {
        final List<byte[]> allPayloads = new ArrayList<>();
        for (final List<byte[]> payload : payloads)
        {
            allPayloads.addAll(payload);
        }

        assertPayloads(receivedPayloads, allPayloads);
    }

    private static Stream<Arguments> fragmentLimitsAndChannels()
    {
        return Stream.of(1, 10, Integer.MAX_VALUE)
            .flatMap((fragmentLimit) ->
                Stream.of(IPC_CHANNEL, MULTICAST_CHANNEL)
                    .map((channel) ->
                        Arguments.of(fragmentLimit, channel))
            );
    }

    private static Stream<Arguments> replayChannelsAndStreams()
    {
        return Stream.of(
            arguments("aeron:udp?endpoint=localhost:0", -10),
            arguments("aeron:udp?endpoint=localhost:10001", -11),
            arguments("aeron:ipc", -12)
            // TODO add response channel
        );
    }

    private static String removeSessionId(final String channel)
    {
        final ChannelUri uri = ChannelUri.parse(channel);
        uri.remove(SESSION_ID_PARAM_NAME);
        return uri.toString();
    }

    private static final class BufferingFragmentHandler implements ControlledFragmentHandler
    {
        private final List<byte[]> receivedPayloads = new ArrayList<>();
        private long position;

        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            position = header.position();
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

    private static final class CountingFragmentHandler implements FragmentHandler
    {
        private long receivedFragments = 0;

        public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            receivedFragments++;
        }

        boolean hasReceivedPayloads(final int numberOfPayloads)
        {
            return receivedFragments >= numberOfPayloads;
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

    private static final class PersistentPublication
    {
        private final AeronArchive aeronArchive;
        private final ExclusivePublication publication;
        private final long recordingId;
        private final CountersReader countersReader;
        private final int recordingCounterId;

        static PersistentPublication create(
            final AeronArchive aeronArchive,
            final String channel,
            final int streamId)
        {
            final ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(channel, streamId);
            final CountersReader countersReader = aeronArchive.context().aeron().countersReader();
            final int recordingCounterId =
                Tests.awaitRecordingCounterId(countersReader, publication.sessionId(), aeronArchive.archiveId());
            final long recordingId = RecordingPos.getRecordingId(countersReader, recordingCounterId);

            return new PersistentPublication(
                aeronArchive,
                publication,
                recordingId,
                countersReader,
                recordingCounterId);
        }

        private PersistentPublication(
            final AeronArchive aeronArchive,
            final ExclusivePublication publication,
            final long recordingId,
            final CountersReader countersReader,
            final int recordingCounterId)
        {
            this.aeronArchive = aeronArchive;
            this.publication = publication;
            this.recordingId = recordingId;
            this.countersReader = countersReader;
            this.recordingCounterId = recordingCounterId;
        }

        long offer(final DirectBuffer buffer, final int offset, final int length)
        {
            return publication.offer(buffer, offset, length);
        }

        void persist(final List<byte[]> messages)
        {
            final UnsafeBuffer wrapper = new UnsafeBuffer();

            long position = 0;
            for (final byte[] message : messages)
            {
                wrapper.wrap(message);
                while ((position = publication.offer(wrapper)) < 0)
                {
                    Tests.yieldingIdle("failed to offer due to " + Publication.errorString(position));
                }
            }

            Tests.awaitPosition(countersReader, recordingCounterId, position);
        }

        long stop()
        {
            aeronArchive.stopRecording(publication);
            return aeronArchive.getStopPosition(recordingId);
        }

        long receiverCount()
        {
            final int receiversCounterId = countersReader.findByTypeIdAndRegistrationId(
                FLOW_CONTROL_RECEIVERS_COUNTER_TYPE_ID, publication.registrationId());
            assertNotEquals(NULL_COUNTER_ID, receiversCounterId);
            return countersReader.getCounterValue(receiversCounterId);
        }

        long recordingId()
        {
            return recordingId;
        }

        int maxPayloadLength()
        {
            return publication.maxPayloadLength();
        }

        long position()
        {
            return publication.position();
        }
    }
}
