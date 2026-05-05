/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.RethrowingErrorHandler;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.PersistentSubscriptionException.Reason;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ReceiveChannelEndpointSupplier;
import io.aeron.driver.SendChannelEndpointSupplier;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.ext.DebugReceiveChannelEndpoint;
import io.aeron.driver.ext.DebugSendChannelEndpoint;
import io.aeron.driver.ext.LossGenerator;
import io.aeron.driver.status.SubscriberPos;
import io.aeron.exceptions.TimeoutException;
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
import io.aeron.test.driver.DataInRangeLossGenerator;
import io.aeron.test.driver.FrameDataLossGenerator;
import io.aeron.test.driver.SetupAtPositionLossGenerator;
import io.aeron.test.driver.StreamIdFrameDataLossGenerator;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.aeron.AeronCounters.DRIVER_PUBLISHER_POS_TYPE_ID;
import static io.aeron.AeronCounters.FLOW_CONTROL_RECEIVERS_COUNTER_TYPE_ID;
import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.CommonContext.IPC_MEDIA;
import static io.aeron.CommonContext.REJOIN_PARAM_NAME;
import static io.aeron.CommonContext.SESSION_ID_PARAM_NAME;
import static io.aeron.CommonContext.SPY_PREFIX;
import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.archive.client.PersistentSubscription.FROM_LIVE;
import static io.aeron.archive.client.PersistentSubscription.FROM_START;
import static io.aeron.driver.status.StreamCounter.CHANNEL_OFFSET;
import static io.aeron.driver.status.StreamCounter.STREAM_ID_OFFSET;
import static io.aeron.test.TestContexts.LOCALHOST_CONTROL_REQUEST_CHANNEL;
import static io.aeron.test.TestContexts.LOCALHOST_CONTROL_RESPONSE_CHANNEL;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
abstract class PersistentSubscriptionTest
{
    private static final int ONE_KB_MESSAGE_SIZE = 1024 - DataHeaderFlyweight.HEADER_LENGTH;
    private static final int TERM_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int STREAM_ID = 1000;
    private static final String MDC_SUBSCRIPTION_CHANNEL = "aeron:udp?control=localhost:2000";
    private static final String MDC_PUBLICATION_CHANNEL =
        "aeron:udp?control=localhost:2000|control-mode=dynamic|fc=max";
    private static final String UNICAST_CHANNEL = "aeron:udp?endpoint=localhost:2000";
    private static final String MULTICAST_CHANNEL = "aeron:udp?endpoint=224.20.30.39:40456|interface=localhost";

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
        .imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(2))
        .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
        .untetheredWindowLimitTimeoutNs(TimeUnit.SECONDS.toNanos(1))
        .untetheredLingerTimeoutNs(TimeUnit.SECONDS.toNanos(1))
        .publicationLingerTimeoutNs(TimeUnit.SECONDS.toNanos(1))
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
    private Archive.Context archiveCtxTpl;
    private AeronArchive.Context aeronArchiveCtxTpl;
    private String aeronDirectoryName;

    @BeforeEach
    void setUp()
    {
        aeronDirectoryName = CommonContext.generateRandomDirName();

        final MediaDriver.Context driverCtx = driverCtxTpl.clone()
            .aeronDirectoryName(aeronDirectoryName);

        archiveDir = new File(SystemUtil.tmpDirName(), "archive");

        archiveCtxTpl = TestContexts.localhostArchive()
            .catalogCapacity(128 * 1024)
            .segmentFileLength(TERM_LENGTH)
            .aeronDirectoryName(aeronDirectoryName)
            .deleteArchiveOnStart(true)
            .archiveDir(archiveDir)
            .threadingMode(ArchiveThreadingMode.SHARED);

        driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());
        archive = Archive.launch(archiveCtxTpl.clone());
        systemTestWatcher.dataCollector().add(archiveCtxTpl.archiveDir());

        aeron = Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeronDirectoryName));

        aeronArchiveCtxTpl = TestContexts.localhostAeronArchive().aeron(aeron);
        aeronArchive = AeronArchive.connect(aeronArchiveCtxTpl.clone());

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
            .aeronArchiveContext(aeronArchiveCtxTpl.clone());

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
    @InterruptAfter(15)
    @SuppressWarnings("MethodLength")
    void shouldDropFromLiveBackToReplayThenJoinLiveAgain()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final List<byte[]> firstMessageBatch = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(firstMessageBatch);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            assertEquals(0, listener.liveJoinedCount);
            verify(persistentSubscription);

            // Start consuming messages over replay
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(1),
                () -> poll(persistentSubscription, fragmentHandler, 1));

            assertEquals(1, archive.context().replaySessionCounter().get());
            assertTrue(persistentSubscription.isReplaying());

            // Continue consuming until we switch to live
            executeUntil(persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertEquals(1, listener.liveJoinedCount);
            assertEquals(0, listener.liveLeftCount);

            assertTrue(fragmentHandler.hasReceivedPayloads(firstMessageBatch.size()));
            Tests.await(() -> archive.context().replaySessionCounter().get() == 0);

            // Consume more messages on live
            final List<byte[]> secondMessageBatch = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
            persistentPublication.persist(secondMessageBatch);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(firstMessageBatch.size() + secondMessageBatch.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );

            assertTrue(persistentSubscription.isLive());

            // Publish more messages and consume them from a 'faster' consumer, forcing the Persistent Subscription
            // to fall behind and drop off live.
            final MediaDriver.Context ctx = driverCtxTpl.clone()
                .aeronDirectoryName(CommonContext.generateRandomDirName());
            try (TestMediaDriver mediaDriver = TestMediaDriver.launch(ctx, systemTestWatcher);
                Aeron aeron = Aeron.connect(
                    new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName())))
            {
                final CountingFragmentHandler fastSubscriptionFragmentHandler = new CountingFragmentHandler();
                final Subscription fastConsumer = aeron.addSubscription(MDC_SUBSCRIPTION_CHANNEL, STREAM_ID);

                Tests.awaitConnected(fastConsumer);

                final List<byte[]> thirdMessageBatch = new ArrayList<>();
                for (int i = 0; i < 3; i++)
                {
                    final List<byte[]> batch = generateFixedPayloads(32, ONE_KB_MESSAGE_SIZE);
                    persistentPublication.publish(batch);
                    thirdMessageBatch.addAll(batch);
                    executeUntil(
                        () -> fastSubscriptionFragmentHandler.hasReceivedPayloads(thirdMessageBatch.size()),
                        () -> fastConsumer.poll(fastSubscriptionFragmentHandler, 10)
                    );
                }

                // Verify the Persistent Subscription drops back to replay
                executeUntil(
                    persistentSubscription::isReplaying,
                    () -> poll(persistentSubscription, fragmentHandler, 10),
                    description(persistentSubscription, fragmentHandler, listener)
                );
                assertTrue(persistentSubscription.isReplaying());

                assertEquals(1, listener.liveLeftCount);

                // Consume more messages until the Persistent Subscription rejoins live.
                final List<byte[]> fourthMessageBatch = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
                persistentPublication.persist(fourthMessageBatch);

                executeUntil(
                    () -> fragmentHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount) &&
                        persistentSubscription.isLive(),
                    () -> poll(persistentSubscription, fragmentHandler, 10));

                assertEquals(2, listener.liveJoinedCount);

                assertPayloads(
                    fragmentHandler.receivedPayloads,
                    firstMessageBatch,
                    secondMessageBatch,
                    thirdMessageBatch,
                    fourthMessageBatch
                );
            }

            verify(persistentSubscription);
        }
    }

    @ParameterizedTest
    @MethodSource("replayChannelsAndStreams")
    @InterruptAfter(5)
    void shouldReplayOverConfiguredChannel(
        final String replayChannel,
        final int replayStreamId,
        final String archiveControlRequestChannel,
        final String archiveControlResponseChannel)
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        final List<byte[]> payloads = generateRandomPayloads(5);
        persistentPublication.persist(payloads);

        persistentSubscriptionCtx.aeronArchiveContext()
            .controlRequestChannel(archiveControlRequestChannel)
            .controlResponseChannel(archiveControlResponseChannel);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .replayChannel(replayChannel)
            .replayStreamId(replayStreamId);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(1),
                () -> poll(persistentSubscription, fragmentHandler, 1));

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
                        assertEquals(replayChannel, removeExtraFields(keyBuffer.getStringAscii(CHANNEL_OFFSET)));
                        replaySubPos.set(counters.getCounterValue(counterId1));
                    }
                }
            });
            assertEquals(fragmentHandler.position, replaySubPos.get());

            executeUntil(persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, payloads);
        }
    }

    @ParameterizedTest
    @MethodSource("liveChannels")
    @InterruptAfter(5)
    void shouldConsumeLiveOverConfiguredChannel(
        final int fragmentLimit,
        final String persistentSubscriptionChannel,
        final String publicationChannel)
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, publicationChannel, STREAM_ID);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .liveChannel(persistentSubscriptionChannel);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, fragmentLimit)
            );
            assertEquals(1, listener.liveJoinedCount);

            final List<byte[]> payloads = generateRandomPayloads(5);
            persistentPublication.persist(payloads);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(1),
                () -> poll(persistentSubscription, fragmentHandler, 1));

            assertTrue(persistentSubscription.isLive());
            assertFalse(persistentSubscription.isReplaying());
            Tests.await(() -> archive.context().replaySessionCounter().get() == 0);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(payloads.size()), () ->
                poll(persistentSubscription, fragmentHandler, fragmentLimit));

            assertPayloads(fragmentHandler.receivedPayloads, payloads);
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldAssembleMessages()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        final int sizeRequiringFragmentation = persistentPublication.maxPayloadLength() + 1;
        final List<byte[]> payload0 = generateFixedPayloads(1, sizeRequiringFragmentation);
        final List<byte[]> payload1 = generateFixedPayloads(1, sizeRequiringFragmentation);

        persistentPublication.persist(payload0);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId());

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 1));

            persistentPublication.persist(payload1);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(2),
                () -> poll(persistentSubscription, fragmentHandler, 1));

            assertPayloads(fragmentHandler.receivedPayloads, payload0, payload1);
        }
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, 1024 })
    @InterruptAfter(10)
    void shouldReplayFromRecordingStartPositionWhenStartingFromStart(final long recordingStartPosition)
    {
        final String channel = new ChannelUriStringBuilder()
            .media(IPC_MEDIA)
            .initialPosition(recordingStartPosition, 0, TERM_LENGTH)
            .build();

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, channel, STREAM_ID);

        final List<byte[]> oldMessages = generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(oldMessages);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(FROM_START);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10));

            final List<byte[]> newMessages = generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE);
            persistentPublication.persist(newMessages);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(oldMessages.size() + newMessages.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, oldMessages, newMessages);

            assertEquals(1024 + 1024 + recordingStartPosition, persistentPublication.position());
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldReplayFromSpecificMidRecordingPosition()
    {
        final String channel = new ChannelUriStringBuilder()
            .media(IPC_MEDIA)
            .build();

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, channel, STREAM_ID);

        final List<byte[]> firstMessageBatch = generateFixedPayloads(4, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(firstMessageBatch);
        final List<byte[]> secondMessageBatch = generateFixedPayloads(2, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(secondMessageBatch);


        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(4096);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10));

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(secondMessageBatch.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, secondMessageBatch);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldErrorIfStartPositionIsBeforeRecordingStartPosition()
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
            executeUntil(persistentSubscription::hasFailed, () -> poll(persistentSubscription, null, 1));

            assertEquals(1, listener.errorCount);
            assertEquals(
                Reason.INVALID_START_POSITION,
                ((PersistentSubscriptionException)listener.lastException).reason()
            );
            assertEquals(listener.lastException, persistentSubscription.failureReason());
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldErrorIfStartPositionIsAfterStopPosition()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        persistentPublication.persist(generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE));

        final long stopPosition = persistentPublication.stop();
        assertTrue(stopPosition > 0);

        final long startPosition = stopPosition * 2; // <-- after end of recording
        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(startPosition);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::hasFailed, () -> poll(persistentSubscription, null, 1));

            assertEquals(1, listener.errorCount);
            assertEquals(
                Reason.INVALID_START_POSITION,
                ((PersistentSubscriptionException)listener.lastException).reason()
            );
            assertEquals(listener.lastException, persistentSubscription.failureReason());
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldErrorIfStartPositionIsAfterRecordingLivePosition()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        persistentPublication.persist(generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE));

        final long recordedPosition = persistentPublication.position();
        assertTrue(recordedPosition > 0);

        final long startPosition = recordedPosition * 2; // <-- ahead of latest recorded position
        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(startPosition);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::hasFailed, () -> poll(persistentSubscription, null, 1));

            assertEquals(1, listener.errorCount);
            assertEquals(PersistentSubscriptionException.class, listener.lastException.getClass());
            assertEquals(listener.lastException, persistentSubscription.failureReason());
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldErrorWhenStartPositionDoesNotAlignWithFrame()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);
        persistentPublication.persist(generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE));

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId)
            .startPosition(ONE_KB_MESSAGE_SIZE - 32)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL);

        systemTestWatcher.ignoreErrorsMatching((log) -> log.contains("does not point to a valid frame"));

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                persistentSubscription::hasFailed,
                () -> poll(persistentSubscription, fragmentHandler, 1));
            assertEquals(1, listener.errorCount);
            assertEquals(PersistentSubscriptionException.class, listener.lastException.getClass());
            assertEquals(listener.lastException, persistentSubscription.failureReason());
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
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertEquals(0, fragmentHandler.receivedPayloads.size());

            final List<byte[]> messages = generateRandomPayloads(5);
            persistentPublication.persist(messages);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(messages.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );

            Tests.await(() -> archive.context().replaySessionCounter().get() == 0);

            verify(persistentSubscription);
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
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertEquals(1, listener.liveJoinedCount);

            assertEquals(0, fragmentHandler.receivedPayloads.size());

            final List<byte[]> newMessages = generateRandomPayloads(3);
            persistentPublication.persist(newMessages);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(newMessages.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, newMessages);

            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void canJoinALiveStreamAtTheBeginning()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(FROM_LIVE);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertEquals(1, listener.liveJoinedCount);

            assertEquals(0, fragmentHandler.receivedPayloads.size());

            final List<byte[]> messages = generateRandomPayloads(3);
            persistentPublication.persist(messages);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(messages.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, messages);

            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldRetryAndRecoverWhenLiveIsNotAvailableDuringStartUp()
    {
        // Ensure a recording exists for the stream.
        final PersistentPublication persistentPublication = PersistentPublication.create(
            aeronArchive, IPC_CHANNEL, STREAM_ID
        );
        persistentPublication.persist(generateRandomPayloads(1));
        final long recordingId = persistentPublication.recordingId;

        // Stop the live publication.
        persistentPublication.close();

        persistentSubscriptionCtx
            .startPosition(FROM_LIVE)
            .recordingId(recordingId)
            .aeronArchiveContext().messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            // Start trying to join live while the live publication is stopped.
            executeUntil(
                () -> listener.errorCount > 0,
                () -> poll(persistentSubscription, fragmentHandler, 1)
            );
            assertThat(
                listener.lastException.getMessage(),
                containsString("No image became available on the live subscription")
            );
            assertFalse(persistentSubscription.hasFailed());
            assertNull(persistentSubscription.failureReason());
            assertFalse(persistentSubscription.isLive());

            // Restart the publication and ensure we join live and can consume messages.
            final PersistentPublication resumedPublication = PersistentPublication.resume(
                aeronArchive, IPC_CHANNEL, STREAM_ID, recordingId
            );

            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );

            final List<byte[]> messages = generateRandomPayloads(5);
            resumedPublication.persist(messages);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(messages.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, messages);

            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldRejoinLiveEvenIfNoFragmentsHaveBeenConsumedAfterJoiningFromLive()
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

        final List<byte[]> oldMessages = generateRandomPayloads(randomWatcher.random().nextInt(3));
        persistentPublication.persist(oldMessages);

        persistentSubscriptionCtx
            .aeron(aeron2)
            .recordingId(persistentPublication.recordingId())
            .liveChannel(subChannel)
            .startPosition(FROM_LIVE);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            final Runnable pollSubscription = () -> poll(persistentSubscription, fragmentHandler, 10);

            executeUntil(persistentSubscription::isLive, pollSubscription);

            assertEquals(0, fragmentHandler.receivedPayloads.size());
            assertEquals(1, listener.liveJoinedCount);
            assertEquals(0, listener.liveLeftCount);

            lossGenerator.enable(persistentSubscriptionCtx.liveStreamId());

            executeUntil(() -> !persistentSubscription.isLive(), pollSubscription);

            assertEquals(1, listener.liveJoinedCount);
            assertEquals(1, listener.liveLeftCount);

            lossGenerator.disable();

            executeUntil(persistentSubscription::isLive, pollSubscription);

            assertEquals(0, fragmentHandler.receivedPayloads.size());
            assertEquals(2, listener.liveJoinedCount);
            assertEquals(1, listener.liveLeftCount);

            final List<byte[]> payloads = generateRandomPayloads(3);
            persistentPublication.persist(payloads);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(payloads.size()), pollSubscription);

            assertPayloads(fragmentHandler.receivedPayloads, payloads);

            verify(persistentSubscription);
        }
    }

    @ParameterizedTest
    @ValueSource(longs = { FROM_START, FROM_LIVE })
    @InterruptAfter(10)
    void shouldRetryAndRecoverWhenArchiveIsNotAvailableDuringStartUp(
        final long startPosition, final @TempDir Path tempDir)
    {
        archive.close();
        final File archiveDir = new File(tempDir.toString(), "testLocalArchive");
        final Archive.Context localArchiveCtxTpl = archiveCtxTpl.clone()
            .archiveDir(archiveDir)
            .deleteArchiveOnStart(false);
        final Archive archive = addCloseable(Archive.launch(localArchiveCtxTpl.clone()));

        final AeronArchive aeronArchive = addCloseable(AeronArchive.connect(aeronArchiveCtxTpl.clone()));
        assert aeronArchive != null;

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);
        final List<byte[]> payloads = generateRandomPayloads(1);
        persistentPublication.persist(payloads);

        archive.close();

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(startPosition)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .aeronArchiveContext().messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                () -> listener.errorCount > 1,
                () -> poll(persistentSubscription, fragmentHandler, 1));
            assertEquals(TimeoutException.class, listener.lastException.getClass());
            assertFalse(persistentSubscription.hasFailed());
            assertNull(persistentSubscription.failureReason());
            addCloseable(Archive.launch(localArchiveCtxTpl.clone()));
            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 1));
            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void canStartFromLiveWhenRecordingHasStopped()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        final List<byte[]> firstMessageBatch = generateRandomPayloads(1);
        final List<byte[]> secondMessageBatch = generateRandomPayloads(1);
        persistentPublication.persist(firstMessageBatch);

        final long stopPosition = persistentPublication.stop();
        assertTrue(stopPosition > 0);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(FROM_LIVE);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 1));
            persistentPublication.publish(secondMessageBatch);
            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(secondMessageBatch.size()),
                () -> poll(persistentSubscription, fragmentHandler, 1));
            assertPayloads(fragmentHandler.receivedPayloads, secondMessageBatch);
            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void canStartAtRecordingStopPositionWhenLiveHasNotAdvanced()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        persistentPublication.persist(generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE));

        final long stopPosition = persistentPublication.stop();
        assertTrue(stopPosition > 0);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(stopPosition);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 1));

            final List<byte[]> liveMessages = generateRandomPayloads(3);
            persistentPublication.publish(liveMessages);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(liveMessages.size()),
                () -> poll(persistentSubscription, fragmentHandler, 1));
            assertPayloads(fragmentHandler.receivedPayloads, liveMessages);
            assertEquals(0, listener.errorCount);
            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldRecoverWhenThePersistentPublicationIsRestartedWhileOnLive()
    {
        final PersistentPublication persistentPublication = PersistentPublication.create(
            aeronArchive, IPC_CHANNEL, STREAM_ID
        );
        persistentPublication.persist(generateRandomPayloads(1));
        final long recordingId = persistentPublication.recordingId();

        persistentSubscriptionCtx
            .startPosition(FROM_LIVE)
            .recordingId(recordingId)
            .aeronArchiveContext().messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 1)
            );

            persistentPublication.close();

            executeUntil(
                () -> listener.errorCount > 0,
                () -> poll(persistentSubscription, fragmentHandler, 1)
            );
            assertThat(
                listener.lastException.getMessage(),
                containsString("No image became available on the live subscription")
            );
            assertFalse(persistentSubscription.isLive());

            final PersistentPublication resumedPublication = PersistentPublication.resume(
                aeronArchive, IPC_CHANNEL, STREAM_ID, recordingId
            );

            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );

            final List<byte[]> messages = generateRandomPayloads(5);
            resumedPublication.persist(messages);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(messages.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, messages);

            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldRecoverWhenThePersistentPublicationIsRestartedDuringReplay()
    {
        final PersistentPublication persistentPublication = PersistentPublication.create(
            aeronArchive, IPC_CHANNEL, STREAM_ID
        );
        final List<byte[]> recordedBatch = generateRandomPayloads(1);
        persistentPublication.persist(recordedBatch);
        final long recordingId = persistentPublication.recordingId();

        persistentSubscriptionCtx
            .startPosition(FROM_START)
            .recordingId(recordingId)
            .aeronArchiveContext().messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount()),
                () -> poll(persistentSubscription, fragmentHandler, 1)
            );
            assertTrue(persistentSubscription.isReplaying());

            persistentPublication.close();

            // Wait for the Persistent Subscription to recognise it has reached the end of the
            // recording and attempt to switch to live.
            final Counter persistentSubscriptionState = persistentSubscription.context().stateCounter();
            executeUntil(
                () -> 15 == persistentSubscriptionState.get(), // AWAIT_LIVE
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );

            final PersistentPublication resumedPublication = PersistentPublication.resume(
                aeronArchive, IPC_CHANNEL, STREAM_ID, recordingId
            );

            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );

            final List<byte[]> batchAfterResuming = generateRandomPayloads(5);
            resumedPublication.persist(batchAfterResuming);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(batchAfterResuming.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, recordedBatch, batchAfterResuming);

            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void fallbackFromLiveFailsWhenRecordingStoppedBeforeLivePosition()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        persistentPublication.persist(generateRandomPayloads(1));

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(FROM_LIVE);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 1));

            persistentPublication.stop();

            // These messages advance live past the now-frozen recording stopPosition.
            final List<byte[]> liveOnlyMessages = generateRandomPayloads(3);
            persistentPublication.publish(liveOnlyMessages);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(liveOnlyMessages.size()),
                () -> poll(persistentSubscription, fragmentHandler, 1));

            persistentPublication.closePublicationOnly();

            executeUntil(persistentSubscription::hasFailed, () -> poll(persistentSubscription, fragmentHandler, 1));

            assertEquals(
                Reason.INVALID_START_POSITION,
                ((PersistentSubscriptionException)persistentSubscription.failureReason()).reason()
            );
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldCatchUpWhenStartingAtStopPositionAndRecordingResumes()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        persistentPublication.persist(generateRandomPayloads(1));
        final long stopPosition = persistentPublication.stop();
        assertTrue(stopPosition > 0);
        final long recordingId = persistentPublication.recordingId();
        persistentPublication.closePublicationOnly();

        persistentSubscriptionCtx
            .recordingId(recordingId)
            .startPosition(stopPosition)
            .aeronArchiveContext().messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            // PS shortcuts to ADD_LIVE_SUBSCRIPTION and parks in AWAIT_LIVE with no publisher available;
            // the liveImage deadline breaches as a non-terminal error.
            executeUntil(
                () -> listener.errorCount > 0,
                () -> poll(persistentSubscription, fragmentHandler, 1));
            assertThat(
                listener.lastException.getMessage(),
                containsString("No image became available on the live subscription")
            );
            assertFalse(persistentSubscription.isLive());

            // Resume the publication; this extends the recording so subsequent messages are captured.
            final PersistentPublication resumedPublication =
                PersistentPublication.resume(aeronArchive, IPC_CHANNEL, STREAM_ID, recordingId);
            final List<byte[]> messages = generateRandomPayloads(3);
            resumedPublication.persist(messages);

            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10));

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(messages.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, messages);
            verify(persistentSubscription);
        }
    }

    // Recording is stopped at stop_0, the publication is closed, then the recording is resumed via
    // extendRecording on a fresh MDC publication and more messages are persisted so the recorded
    // end is past stop_0. The subscription then starts with startPosition = stop_0. Because the
    // recording is now active (stopPosition = NULL_VALUE), PS does not shortcut to
    // ADD_LIVE_SUBSCRIPTION — it takes the normal replay path, replays the newly-recorded gap,
    // catches up via ATTEMPT_SWITCH, and joins live.
    @Test
    @InterruptAfter(10)
    void shouldCatchUpWhenStartingAtStopPositionOfExtendedRecording()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        persistentPublication.persist(generateRandomPayloads(1));
        final long stopPosition = persistentPublication.stop();
        assertTrue(stopPosition > 0);
        final long recordingId = persistentPublication.recordingId();
        persistentPublication.closePublicationOnly();

        final PersistentPublication resumedPublication =
            PersistentPublication.resume(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID, recordingId);
        final List<byte[]> catchupMessages = generateRandomPayloads(3);
        resumedPublication.persist(catchupMessages);

        persistentSubscriptionCtx
            .recordingId(recordingId)
            .startPosition(stopPosition)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(catchupMessages.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10));

            final List<byte[]> liveMessages = generateRandomPayloads(2);
            resumedPublication.persist(liveMessages);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(catchupMessages.size() + liveMessages.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, catchupMessages, liveMessages);
            assertEquals(1, listener.liveJoinedCount);
            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldHandOffToLiveWhenReplayCatchesUpAtPublisherJoinPosition()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final List<byte[]> recordedBatch = generateFixedPayloads(3, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(recordedBatch);
        final long stopPosition = persistentPublication.stop();
        assertTrue(stopPosition > 0);
        final long recordingId = persistentPublication.recordingId();
        persistentPublication.closePublicationOnly();
        // Wait for the closed publication's residual state to drain before bringing publisher B up.
        Tests.await(() -> !persistentPublication.publicationCountersExist());

        // Resume publisher B at stopPosition and DO NOT offer anything to it yet.
        // Publisher B's snd_pos stays at stopPosition; the live image, when it attaches, will
        // attach at exactly that position.
        final PersistentPublication resumedPublication =
            PersistentPublication.resume(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID, recordingId);

        persistentSubscriptionCtx
            .recordingId(recordingId)
            .startPosition(FROM_START)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            // Drain the replay; PS sees all recorded bytes up to stopPosition.
            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(recordedBatch.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            // PS reaches LIVE via the replayPosition == livePosition outer branch in attemptSwitch.
            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10));
            assertEquals(1, listener.liveJoinedCount);
            assertEquals(0, listener.liveLeftCount);

            // Now publisher B publishes more — the handoff must not have lost track of position;
            // these messages must arrive via live, in order, with no gap.
            final List<byte[]> liveBatch = generateFixedPayloads(3, ONE_KB_MESSAGE_SIZE);
            resumedPublication.persist(liveBatch);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(recordedBatch.size() + liveBatch.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, recordedBatch, liveBatch);
            verify(persistentSubscription);
        }
    }

    // Deterministically exercises the LIVE -> image-closed -> refresh -> replay path. PS
    // shortcuts to AWAIT_LIVE while the recording is stopped at stop_0, then publisher B is
    // resumed. PS receives publisher B's SETUP normally and either joins live directly at
    // stop_0 (and consumes first_batch via live) or refreshes via AWAIT_LIVE if publisher B's
    // first SETUP carries an advanced offset — both paths land in LIVE with first_batch
    // either delivered or about to be delivered.
    //
    // Once first_batch is delivered AND PS is on its live image, a per-driver loss generator
    // drops every catchup DATA frame on PS's live stream (the recording subscription on the
    // main driver sees no loss). The isLive() wait is required: in path B without it PS
    // stays in REPLAY/ATTEMPT_SWITCH, doesn't poll the live image, doesn't send SMs, and
    // publisher B's flow control stops sending live to PS — catchup arrives only via the
    // REPLAY stream and the loss generator never fires.
    //
    // Heartbeats are always preserved so PS still sees the EOS+REVOKED flag when publisher B
    // is revoked. After revoke, PS's image closes and the bytes the loss generator dropped
    // can never be recovered via NAK retransmits — PS must replay the full catchup from the
    // recording.
    @Test
    @InterruptAfter(10)
    void shouldReplayAndCatchUpWhenExtendedRecordingIsAheadOfLivePosition()
    {
        TestMediaDriver.notSupportedOnCMediaDriver("loss generator");

        final DataInRangeLossGenerator lossGenerator = new DataInRangeLossGenerator();
        final Aeron aeron2 = startSecondAeronWithReceiveLoss(lossGenerator);

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        persistentPublication.persist(generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE));
        final long stopPosition = persistentPublication.stop();
        assertTrue(stopPosition > 0);
        final long recordingId = persistentPublication.recordingId();

        closePublicationAndAwaitDrain(persistentPublication);

        persistentSubscriptionCtx
            .aeron(aeron2)
            .recordingId(recordingId)
            .startPosition(stopPosition)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .aeronArchiveContext().messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            final boolean[] observedReplaying = { false };
            final Runnable pollAndTrack = newReplayObservingPoller(persistentSubscription, observedReplaying);

            awaitFirstAwaitLiveDeadlineBreach(persistentSubscription, pollAndTrack, observedReplaying);

            final PersistentPublication resumedPublication =
                PersistentPublication.resume(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID, recordingId);

            // Publish 1 msg first and wait for PS to receive it. This ensures the live image is
            // attached before the publisher is revoked; without this, the revoke can race ahead
            // of PS's setup-msg handshake and PS gets stuck in AWAIT_LIVE forever (the deadline
            // breach has already fired and won't fire again).
            final List<byte[]> firstBatch = generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE);
            resumedPublication.persist(firstBatch);
            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(firstBatch.size()),
                pollAndTrack);

            // Wait for PS to be on its live image. In path B (advanced-offset SETUP), PS may
            // have delivered first_batch via initial-refresh REPLAY and still be in
            // REPLAY/ATTEMPT_SWITCH. Without polling the live image, PS's last_sm_position
            // doesn't advance and publisher B's flow control stops sending live to PS — so
            // the loss generator never sees catchup frames on PS's live endpoint.
            executeUntil(persistentSubscription::isLive, pollAndTrack);

            // Reset observedReplaying after the first_batch phase. If publisher B's SETUP arrived
            // at an advanced offset, PS already went through REFRESH/REPLAY once to deliver
            // first_batch — we don't want that observation to satisfy the post-revoke assertion
            // below. From here on, only the catchup-refresh path can flip this back to true.
            observedReplaying[0] = false;

            armDataDropFromPosition(lossGenerator, recordingId, stopPosition + 1024L);
            lossGenerator.enable();

            // Publish 40 catchup messages. The recording (on the main driver) sees them all;
            // PS (on driver2) sees none of them on its live image — they're dropped by the
            // loss generator on driver2's receive endpoint.
            final List<byte[]> catchupMessages = generateFixedPayloads(40, ONE_KB_MESSAGE_SIZE);
            resumedPublication.persist(catchupMessages);

            // Revoke ends the live image; after the EOS+REVOKED heartbeat reaches PS (the loss
            // generator explicitly preserves heartbeats) and the image closes, the dropped
            // bytes are gone forever from the live channel — PS must replay them.
            resumedPublication.publication.revoke();

            final List<byte[]> expected = new ArrayList<>(firstBatch);
            expected.addAll(catchupMessages);
            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(expected.size()),
                pollAndTrack);

            lossGenerator.disable();

            assertPayloads(fragmentHandler.receivedPayloads, expected);
            assertTrue(observedReplaying[0],
                "PS did not transition through REPLAY/ATTEMPT_SWITCH after revoke; the " +
                    "catchup-refresh path was not exercised");
            // No additional AWAIT_LIVE entries after the initial one, so no additional breaches.
            assertEquals(1, listener.errorCount);
            // The filter must have actually fired — otherwise the test could pass for the wrong
            // reason (catchup somehow reaching PS without going through the drop range), and a
            // future change that breaks the tuple computation would silently regress. The
            // isLive() wait above is what makes this assertion deterministic: in path B without
            // it, PS stays in REPLAY/ATTEMPT_SWITCH, doesn't poll the live image, doesn't send
            // SMs, and publisher B's flow control stops sending live to PS — catchup arrives
            // only via the REPLAY stream-id (which the filter doesn't target) and framesDropped
            // would stay at 0.
            assertTrue(lossGenerator.framesDropped() > 0,
                "loss generator did not fire on any catchup DATA frame");
            verify(persistentSubscription);
        }
    }

    // Deterministically exercises the AWAIT_LIVE -> live-ahead -> refresh -> replay -> live path.
    // PS shortcuts to AWAIT_LIVE while the recording is stopped at stop_0. When publisher B is
    // resumed, its first SETUP carries the (initial_term_id, term_id_at_stop_0, offset_at_stop_0)
    // tuple — which is also where PS's start_position sits. Without intervention, PS's image is
    // created at stop_0, the AWAIT_LIVE check `live_position > position` is false, and PS takes
    // the direct LIVE path instead of refreshing.
    //
    // To force the refresh path deterministically, PS runs on its own MediaDriver whose receive
    // endpoints carry a SetupAtPositionLossGenerator. When publisher B is resumed, its first
    // SETUP at stop_0 is dropped on PS's endpoint. PS's UDP receiver elicits another SETUP via
    // SM. By the time publisher B's next SETUP fires, snd_pos has advanced past the offers we
    // issued, the SETUP no longer matches the drop filter, and PS's image is created with
    // join_position > stop_0 — triggering the refresh path. The recording subscription on the
    // main driver is unaffected, so the recording stays contiguous.
    @Test
    @InterruptAfter(10)
    void shouldRefreshAndReplayWhenLiveAheadOfStopPositionAfterResume()
    {
        TestMediaDriver.notSupportedOnCMediaDriver("loss generator");

        final SetupAtPositionLossGenerator lossGenerator = new SetupAtPositionLossGenerator();
        final Aeron aeron2 = startSecondAeronWithReceiveLoss(lossGenerator);

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        persistentPublication.persist(generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE));
        final long stopPosition = persistentPublication.stop();
        assertTrue(stopPosition > 0);
        final long recordingId = persistentPublication.recordingId();

        armSetupDropAtPosition(lossGenerator, recordingId, stopPosition);

        closePublicationAndAwaitDrain(persistentPublication);

        persistentSubscriptionCtx
            .aeron(aeron2)
            .recordingId(recordingId)
            .startPosition(stopPosition)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .aeronArchiveContext().messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            final boolean[] observedReplaying = { false };
            final Runnable pollAndTrack = newReplayObservingPoller(persistentSubscription, observedReplaying);

            awaitFirstAwaitLiveDeadlineBreach(persistentSubscription, pollAndTrack, observedReplaying);

            // Arm the SETUP filter just before resuming. Publisher B's first SETUP carries the
            // exact target tuple we configured above; it will be dropped at PS's endpoint and
            // delivered normally to the recording subscription on the main driver (driver1, which
            // has no loss generator).
            lossGenerator.enable();

            final PersistentPublication resumedPublication =
                PersistentPublication.resume(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID, recordingId);

            // Persist a few messages so publisher B's snd_pos advances past stop_0. The next SETUP
            // (gated by the publication SETUP timeout) carries the advanced term_offset, no longer
            // matches the filter, and creates PS's image with join_position > stop_0.
            final List<byte[]> postResumeMessages = generateFixedPayloads(4, ONE_KB_MESSAGE_SIZE);
            resumedPublication.persist(postResumeMessages);

            executeUntil(persistentSubscription::isLive, pollAndTrack);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(postResumeMessages.size()),
                pollAndTrack);
            assertPayloads(fragmentHandler.receivedPayloads, postResumeMessages);

            lossGenerator.disable();

            // Messages offered before PS's live image attached can only have arrived via the
            // refresh -> replay path; mid-stream UDP subscribers don't backfill historical bytes
            // on the live stream.
            assertTrue(observedReplaying[0],
                "PS did not transition through REPLAY/ATTEMPT_SWITCH; refresh path was not exercised");
            assertEquals(1, listener.liveJoinedCount);
            assertEquals(0, listener.liveLeftCount);
            // Still exactly the one initial deadline-breach error — PS refreshed once, went to
            // REPLAY then LIVE, and has stayed there. No additional AWAIT_LIVE entries means no
            // additional breaches.
            assertEquals(1, listener.errorCount);
            // The filter is a safety net for the race where publisher B's first SETUP carries
            // its initial term_offset (causing PS's image to attach at stop_0 and skip refresh).
            // It may or may not fire depending on how fast publisher B's snd_pos advances
            // relative to the first SETUP send. observedReplaying being true is what proves
            // the refresh path was taken — either naturally or because the filter forced it.
            verify(persistentSubscription);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 10 })
    @InterruptAfter(5)
    void shouldReplayExistingRecordingThenJoinLive(final int fragmentLimit)
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        final List<byte[]> replayMessages = generateRandomPayloads(5);
        persistentPublication.persist(replayMessages);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId());

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                persistentSubscription::isReplaying,
                () -> poll(persistentSubscription, fragmentHandler, fragmentLimit)
            );

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount),
                () ->
                {
                    poll(persistentSubscription, fragmentHandler, fragmentLimit);
                    assertTrue(persistentSubscription.isReplaying());
                });

            assertEquals(1, archive.context().replaySessionCounter().get());
            assertEquals(0, listener.liveJoinedCount);
            assertTrue(persistentSubscription.isReplaying());

            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, fragmentLimit));
            assertEquals(1, listener.liveJoinedCount);
            assertEquals(5, fragmentHandler.receivedPayloads.size());

            final List<byte[]> liveMessages = generateRandomPayloads(15);
            persistentPublication.persist(liveMessages);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount),
                () ->
                {
                    poll(persistentSubscription, fragmentHandler, fragmentLimit);

                    // expect remaining messages to be consumed on the live channel
                    assertTrue(persistentSubscription.isLive());
                });

            assertTrue(persistentSubscription.isLive());
            assertFalse(persistentSubscription.isReplaying());
            assertEquals(0, listener.liveLeftCount);
            assertEquals(0, persistentSubscription.joinDifference());
            assertPayloads(fragmentHandler.receivedPayloads, replayMessages, liveMessages);
            verify(persistentSubscription);

            Tests.await(() -> archive.context().replaySessionCounter().get() == 0);
        }
    }

    @Test
    @InterruptAfter(5)
    void canSwitchFromReplayToLiveWhenLivePositionIsAheadOfReplayPosition()
    {
        TestMediaDriver.notSupportedOnCMediaDriver("loss generator");

        final PersistentPublication persistentPublication = PersistentPublication.create(
            aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID
        );

        final StreamIdLossGenerator streamIdFrameDataLossGenerator = new StreamIdLossGenerator();

        final MediaDriver.Context driver2Ctx = driverCtxTpl.clone()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .receiveChannelEndpointSupplier(receiveChannelEndpointSupplier(streamIdFrameDataLossGenerator));

        addCloseable(TestMediaDriver.launch(driver2Ctx, systemTestWatcher));
        final Aeron aeron = addCloseable(
            Aeron.connect(new Aeron.Context().aeronDirectoryName(driver2Ctx.aeronDirectoryName()))
        );
        systemTestWatcher.dataCollector().add(driver2Ctx.aeronDirectory());

        persistentSubscriptionCtx
            .aeron(aeron)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .recordingId(persistentPublication.recordingId());

        final List<byte[]> messagesToConsumeOnReplay = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(messagesToConsumeOnReplay);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            // Start consuming messages over replay.
            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(1),
                () -> poll(persistentSubscription, fragmentHandler, 1)
            );
            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount()),
                () ->
                {
                    poll(persistentSubscription, fragmentHandler, 1);
                    assertTrue(persistentSubscription.isReplaying());
                }
            );

            // Stop the replay from advancing
            streamIdFrameDataLossGenerator.enable(persistentSubscriptionCtx.replayStreamId());

            // Continue sending messages so the live position advances ahead of the replay.
            final List<byte[]> messagesToConsumeAfterAddingLive = generateFixedPayloads(2, ONE_KB_MESSAGE_SIZE);
            persistentPublication.publish(messagesToConsumeAfterAddingLive);

            // Poll the Persistent Subscription until it has added the live channel.
            executeUntil(
                () -> persistentSubscription.joinDifference() != Long.MIN_VALUE,
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );

            // Allow the persistent subscription to continue consuming over replay, and then join live.
            streamIdFrameDataLossGenerator.disable();

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount()) &&
                    persistentSubscription.isLive(),
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );

            // Verify the live position was added ahead of the replay position.
            assertEquals(2048, persistentSubscription.joinDifference());

            final List<byte[]> messagesToConsumeOnLive = generateFixedPayloads(2, ONE_KB_MESSAGE_SIZE);
            persistentPublication.publish(messagesToConsumeOnLive);
            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount()),
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );
            assertTrue(persistentSubscription.isLive());
            assertPayloads(
                fragmentHandler.receivedPayloads,
                messagesToConsumeOnReplay, messagesToConsumeAfterAddingLive, messagesToConsumeOnLive
            );
            verify(persistentSubscription);
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

        final Subscription slowConsumer = aeron2.addSubscription(subChannel, STREAM_ID);
        Tests.awaitConnected(slowConsumer);

        // All 32k will be consumed by the archive and recorded,
        // but only 4k will be sent to the receivers until the subscribers start consuming
        persistentPublication.persist(generateFixedPayloads(32, ONE_KB_MESSAGE_SIZE));

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .liveChannel(subChannel);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            // The persistent subscription can consume all 32k from the archive
            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(32),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertTrue(persistentSubscription.isReplaying());

            executeUntil(
                () -> persistentPublication.receiverCount() == 2,
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );

            // Consuming on the slower consumer allows the sender to send more than the initial 4k
            executeUntil(persistentSubscription::isLive,
                () ->
                {
                    poll(persistentSubscription, fragmentHandler, 10);
                    slowConsumer.poll((b, o, l, h) -> {}, 10);
                });
            // The persistent subscription will add the live channel when live is at 4k, which is
            // 28k behind where it got up to on replay — so joinDifference must be <= 0 (replay
            // is at-or-ahead-of live, never behind). We don't assert the exact -28 * 1024L
            // value because the path through the PS state machine is racy under load:
            //   * If the live image attaches while PS is still in REPLAY, the REPLAY handler
            //     enters ATTEMPT_SWITCH with joinDifference = liveCurrent - replayCurrent
            //     (negative), and ATTEMPT_SWITCH preserves that value through to LIVE.
            //   * If the replay image closes first (e.g. PS already at recorded stop_position
            //     when the live image arrives), PS shortcuts via AWAIT_LIVE → LIVE which sets
            //     joinDifference = 0 before transitioning. Both are valid handling.
            assertTrue(persistentSubscription.joinDifference() <= 0,
                "joinDifference must be <= 0 (replay was ahead of live), got: " +
                    persistentSubscription.joinDifference());

            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(60)
    @SuppressWarnings("methodlength")
    void canJoinLiveWhenLiveAndReplayAreAdvancing() throws Exception
    {
        // This test can also be used to manually observe the impact a Persistent Subscription has on a control
        // subscriber. Run with `-Daeron.test.system.persistentsubscription.printresults=true` to see results.
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
        final int ratePerSecond = 500;
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
                if (poll(persistentSubscription, handler, 10) == 0)
                {
                    checkForInterrupt("failed to transition to live");
                }
            }

            interruptAndJoin(publisher);
            final long lastPosition = persistentPublication.position();

            while (handler.position < lastPosition)
            {
                if (poll(persistentSubscription, handler, 10) == 0)
                {
                    checkForInterrupt("failed to drain the stream");
                }
            }

            interruptAndJoin(control);

            if (Boolean.getBoolean("aeron.test.system.persistentsubscription.printresults"))
            {
                printResults(t0, persistentSubscription, ratePerSecond, publisherMessagesPerSecond,
                    publisherBpePerSecond, controlMessagesPerSecond);
            }

            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void canJoinLiveInTheMiddleOfAFragmentedMessage()
    {
        TestMediaDriver.notSupportedOnCMediaDriver("loss generator");

        final int maxPayloadLength = driver.context().mtuLength() - DataHeaderFlyweight.HEADER_LENGTH;
        final byte[] firstHalfOfMessage = new byte[maxPayloadLength];
        Arrays.fill(firstHalfOfMessage, (byte)1);
        final byte[] secondHalfOfMessage = new byte[maxPayloadLength];
        Arrays.fill(secondHalfOfMessage, (byte)2);
        final byte[] largeMessage = new byte[firstHalfOfMessage.length + secondHalfOfMessage.length];
        System.arraycopy(firstHalfOfMessage, 0, largeMessage, 0, firstHalfOfMessage.length);
        System.arraycopy(secondHalfOfMessage, 0, largeMessage, firstHalfOfMessage.length, secondHalfOfMessage.length);


        final String aeron2Dir = CommonContext.generateRandomDirName();
        final FrameDataLossGenerator frameDataLossGenerator = new FrameDataLossGenerator();

        final MediaDriver.Context driverCtxWithLoss = driverCtxTpl.clone()
            .aeronDirectoryName(aeron2Dir)
            .imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(1))
            .sendChannelEndpointSupplier(sendChannelEndpointSupplier(frameDataLossGenerator));

        addCloseable(TestMediaDriver.launch(driverCtxWithLoss, systemTestWatcher));
        systemTestWatcher.dataCollector().add(driverCtxWithLoss.aeronDirectory());

        final Aeron.Context aeron2Context = aeronCtxTpl.clone().aeronDirectoryName(aeron2Dir);
        final Aeron aeron2 = addCloseable(Aeron.connect(aeron2Context));

        final ExclusivePublication exclusivePublication =
            aeron2.addExclusivePublication(MDC_PUBLICATION_CHANNEL, STREAM_ID);
        aeronArchive.startRecording(MDC_PUBLICATION_CHANNEL, STREAM_ID, SourceLocation.REMOTE);
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, exclusivePublication);

        final AtomicBoolean keepDroppingAfterMatch = new AtomicBoolean(false);

        frameDataLossGenerator.enable(
            (bytes) ->
            {
                final byte[] payload = new byte[bytes.length - DataHeaderFlyweight.HEADER_LENGTH];
                System.arraycopy(bytes, DataHeaderFlyweight.HEADER_LENGTH, payload, 0, payload.length);
                if (Arrays.equals(payload, secondHalfOfMessage))
                {
                    keepDroppingAfterMatch.set(true);
                }
                return keepDroppingAfterMatch.get();
            }
        );
        persistentPublication.publish(List.of(largeMessage));

        persistentSubscriptionCtx
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .liveStreamId(STREAM_ID)
            .recordingId(persistentPublication.recordingId())
            .startPosition(FROM_START);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isReplaying,
                () -> poll(persistentSubscription, fragmentHandler, 1));

            assertEquals(0, fragmentHandler.receivedPayloads.size());

            executeUntil(persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 1));

            assertEquals(0, fragmentHandler.receivedPayloads.size());

            frameDataLossGenerator.disable();

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(1),
                () -> poll(persistentSubscription, fragmentHandler, 1));

            assertPayloads(fragmentHandler.receivedPayloads, List.of(largeMessage));

            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldStartFromStoppedRecordingAndJoinLiveWhenLiveHasNotAdvanced()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final List<byte[]> oldMessages = generateFixedPayloads(8, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(oldMessages);

        aeronArchive.stopRecording(persistentPublication.publication);

        persistentSubscriptionCtx
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .recordingId(persistentPublication.recordingId());

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(oldMessages.size()), () ->
                poll(persistentSubscription, fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, oldMessages);

            executeUntil(persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10));

            final List<byte[]> newMessages = generateFixedPayloads(16, ONE_KB_MESSAGE_SIZE);
            persistentPublication.publish(newMessages);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(oldMessages.size() + newMessages.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertTrue(persistentSubscription.isLive());
            assertFalse(persistentSubscription.isReplaying());

            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldJoinLiveUponReachingEndOfRecordingWhenLiveBecomesAvailable()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final List<byte[]> oldMessages = generateFixedPayloads(8, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(oldMessages);

        persistentPublication.close();
        Tests.await(() -> !persistentPublication.publicationCountersExist());

        final long recordingId = persistentPublication.recordingId();
        persistentSubscriptionCtx
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .recordingId(recordingId)
            .aeronArchiveContext().messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            // Ensure we can still consume from replay
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(oldMessages.size()), () ->
                poll(persistentSubscription, fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, oldMessages);

            executeUntil(
                () -> listener.errorCount > 0,
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );
            assertThat(
                listener.lastException.getMessage(),
                containsString("No image became available on the live subscription")
            );
            assertFalse(persistentSubscription.hasFailed());
            assertNull(persistentSubscription.failureReason());

            // Restart the live publication
            final PersistentPublication resumedPublication = PersistentPublication.resume(
                aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID, recordingId
            );

            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );

            final List<byte[]> newMessages = generateFixedPayloads(16, ONE_KB_MESSAGE_SIZE);
            resumedPublication.persist(newMessages);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(oldMessages.size() + newMessages.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertTrue(persistentSubscription.isLive());
            assertFalse(persistentSubscription.isReplaying());
            assertPayloads(fragmentHandler.receivedPayloads, oldMessages, newMessages);
            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldStartFromStoppedRecordingAndErrorWhenLiveHasAdvanced()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final List<byte[]> recordedMessages = generateFixedPayloads(8, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(recordedMessages);

        aeronArchive.stopRecording(persistentPublication.publication);

        // Add another consumer to allow the live position to advance
        try (Subscription subscriber2 = aeron.addSubscription(MDC_SUBSCRIPTION_CHANNEL, STREAM_ID))
        {
            final List<byte[]> messagesAfterRecording = generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE);
            persistentPublication.publish(messagesAfterRecording);

            final BufferingFragmentHandler subscriber2FragmentHandler = new BufferingFragmentHandler();
            executeUntil(
                () -> subscriber2FragmentHandler.hasReceivedPayloads(messagesAfterRecording.size()),
                () -> subscriber2.controlledPoll(subscriber2FragmentHandler::onFragmentControlled, 10)
            );
        }

        persistentSubscriptionCtx
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .recordingId(persistentPublication.recordingId());

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(recordedMessages.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertTrue(persistentSubscription.isReplaying());

            executeUntil(persistentSubscription::hasFailed,
                () -> poll(persistentSubscription, fragmentHandler, 1));

            assertEquals(recordedMessages.size(), fragmentHandler.receivedPayloads.size());
            assertEquals(1, listener.errorCount);
            assertEquals(PersistentSubscriptionException.class, listener.lastException.getClass());
            assertEquals(
                Reason.INVALID_START_POSITION,
                ((PersistentSubscriptionException)listener.lastException).reason()
            );
            assertEquals(listener.lastException, persistentSubscription.failureReason());
            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldStayOnReplayWhenLiveCannotConnect()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final List<byte[]> messages = generateRandomPayloads(5);
        persistentPublication.persist(messages);

        final String incorrectLiveChannel = "aeron:udp?control=localhost:49582|control-mode=dynamic";

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(FROM_START)
            .liveChannel(incorrectLiveChannel)
            .aeronArchiveContext().messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(5),
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );

            assertTrue(persistentSubscription.isReplaying());
            Tests.await(() -> archive.context().replaySessionCounter().get() == 1);

            executeUntil(
                () -> listener.errorCount > 0,
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );
            assertThat(
                listener.lastException.getMessage(),
                containsString("No image became available on the live subscription")
            );
            assertFalse(persistentSubscription.hasFailed());
            assertNull(persistentSubscription.failureReason());

            assertTrue(persistentSubscription.isReplaying());

            final List<byte[]> moreMessages = generateRandomPayloads(3);
            persistentPublication.persist(moreMessages);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(messages.size() + moreMessages.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );

            assertTrue(persistentSubscription.isReplaying());
            Tests.await(() -> archive.context().replaySessionCounter().get() == 1);

            assertPayloads(fragmentHandler.receivedPayloads, messages, moreMessages);

            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void canFallbackToReplayAfterStartingFromLive()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final List<byte[]> firstMessageBatch = generateRandomPayloads(2);
        persistentPublication.persist(firstMessageBatch);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(FROM_LIVE)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            // Start consuming messages from live
            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );
            assertEquals(1, listener.liveJoinedCount);
            assertEquals(0, archive.context().replaySessionCounter().get());
            assertFalse(persistentSubscription.isReplaying());

            final List<byte[]> secondMessageBatch = generateRandomPayloads(5);
            persistentPublication.persist(secondMessageBatch);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(secondMessageBatch.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );

            // Publish more messages and consume them from a 'faster' consumer, forcing the Persistent Subscription
            // to fall behind and drop off live.
            final MediaDriver.Context ctx = driverCtxTpl.clone()
                .aeronDirectoryName(CommonContext.generateRandomDirName());
            try (TestMediaDriver mediaDriver = TestMediaDriver.launch(ctx, systemTestWatcher);
                Aeron aeron = Aeron.connect(
                    new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName())))
            {
                final CountingFragmentHandler fastSubscriptionFragmentHandler = new CountingFragmentHandler();
                final Subscription fastConsumer = aeron.addSubscription(MDC_SUBSCRIPTION_CHANNEL, STREAM_ID);

                Tests.awaitConnected(fastConsumer);

                final List<byte[]> thirdMessageBatch = new ArrayList<>();
                for (int i = 0; i < 3; i++)
                {
                    final List<byte[]> batch = generateFixedPayloads(32, ONE_KB_MESSAGE_SIZE);
                    persistentPublication.publish(batch);
                    thirdMessageBatch.addAll(batch);
                    executeUntil(
                        () -> fastSubscriptionFragmentHandler.hasReceivedPayloads(thirdMessageBatch.size()),
                        () -> fastConsumer.poll(fastSubscriptionFragmentHandler, 10)
                    );
                }

                // Verify the Persistent Subscription drops back to replay
                executeUntil(
                    persistentSubscription::isReplaying,
                    () -> poll(persistentSubscription, fragmentHandler, 10)
                );
                assertTrue(persistentSubscription.isReplaying());

                assertEquals(1, listener.liveLeftCount);

                // Consume more messages until the Persistent Subscription rejoins live.
                final List<byte[]> fourthMessageBatch = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
                persistentPublication.persist(fourthMessageBatch);

                final int expectedMessageCount = secondMessageBatch.size() + thirdMessageBatch.size() +
                    fourthMessageBatch.size();

                executeUntil(
                    () -> fragmentHandler.hasReceivedPayloads(expectedMessageCount) && persistentSubscription.isLive(),
                    () -> poll(persistentSubscription, fragmentHandler, 10));

                assertEquals(2, listener.liveJoinedCount);

                assertPayloads(
                    fragmentHandler.receivedPayloads,
                    secondMessageBatch,
                    thirdMessageBatch,
                    fourthMessageBatch
                );
            }

            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void untetheredSpyCanFallbackToReplay()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(FROM_LIVE)
            .liveChannel(SPY_PREFIX + MDC_PUBLICATION_CHANNEL + "|tether=false");

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );
            assertEquals(0, listener.liveLeftCount);
            assertEquals(0, fragmentHandler.receivedPayloads.size());

            // Publish more messages and consume them from a 'faster' consumer, forcing the Persistent Subscription
            // to fall behind and drop off live.
            final MediaDriver.Context ctx = driverCtxTpl.clone()
                .aeronDirectoryName(CommonContext.generateRandomDirName());
            try (TestMediaDriver mediaDriver = TestMediaDriver.launch(ctx, systemTestWatcher);
                Aeron aeron = Aeron.connect(
                    new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName())))
            {
                final CountingFragmentHandler fastSubscriptionFragmentHandler = new CountingFragmentHandler();
                final Subscription fastConsumer = aeron.addSubscription(MDC_SUBSCRIPTION_CHANNEL, STREAM_ID);

                Tests.awaitConnected(fastConsumer);

                final List<byte[]> firstMessageBatch = new ArrayList<>();
                for (int i = 0; i < 3; i++)
                {
                    final List<byte[]> batch = generateFixedPayloads(32, ONE_KB_MESSAGE_SIZE);
                    persistentPublication.publish(batch);
                    firstMessageBatch.addAll(batch);
                    executeUntil(
                        () -> fastSubscriptionFragmentHandler.hasReceivedPayloads(firstMessageBatch.size()),
                        () -> fastConsumer.poll(fastSubscriptionFragmentHandler, 10)
                    );
                }
                // Verify the Persistent Subscription drops back to replay
                executeUntil(
                    persistentSubscription::isReplaying,
                    () -> poll(persistentSubscription, fragmentHandler, 10)
                );
                assertEquals(1, listener.liveLeftCount);

                executeUntil(
                    () -> fragmentHandler.hasReceivedPayloads(firstMessageBatch.size()) &&
                          persistentSubscription.isLive(),
                    () -> poll(persistentSubscription, fragmentHandler, 10));

                assertPayloads(
                    fragmentHandler.receivedPayloads,
                    firstMessageBatch
                );
            }
        }
    }

    @Test
    @InterruptAfter(15)
    void cannotFallbackToReplayWhenRecordingHasStoppedAtAnEarlierPosition()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final MediaDriver.Context ctx = driverCtxTpl.clone()
            .aeronDirectoryName(CommonContext.generateRandomDirName());
        final TestMediaDriver mediaDriver = addCloseable(TestMediaDriver.launch(ctx, systemTestWatcher));
        final Aeron aeron = addCloseable(
            Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName())));

        final Subscription fastSubscription = addCloseable(aeron.addSubscription(MDC_SUBSCRIPTION_CHANNEL, STREAM_ID));
        final CountingFragmentHandler countingFragmentHandler = new CountingFragmentHandler();

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .startPosition(FROM_LIVE);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 1));

            // Consume a batch of messages on live
            final List<byte[]> firstMessageBatch = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
            persistentPublication.persist(firstMessageBatch);
            executeUntil(
                () -> countingFragmentHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount()),
                () -> fastSubscription.poll(countingFragmentHandler, 10));

            // Stop the recording
            aeronArchive.stopRecording(persistentPublication.publication);

            // Publish more messages that will not be recorded so the persistent subscription advances past
            // the recording's stop position.
            final List<byte[]> secondMessageBatch = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
            persistentPublication.publish(secondMessageBatch);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(firstMessageBatch.size() + secondMessageBatch.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );

            // Close the publication so the live image ends via EOS rather than relying on a flow-control timeout.
            // The persistent subscription then refreshes the recording descriptor and the validate step rejects
            // the replay because its position is past the recording's stop position.
            persistentPublication.closePublicationOnly();

            executeUntil(
                persistentSubscription::hasFailed,
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );
            assertEquals(PersistentSubscriptionException.class, listener.lastException.getClass());
            assertEquals(listener.lastException, persistentSubscription.failureReason());
            assertEquals(
                Reason.INVALID_START_POSITION,
                ((PersistentSubscriptionException)listener.lastException).reason()
            );

            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void cannotFallbackToReplayWhenRecordingHasBeenRemoved()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final MediaDriver.Context ctx = driverCtxTpl.clone()
            .aeronDirectoryName(CommonContext.generateRandomDirName());
        final TestMediaDriver mediaDriver = addCloseable(TestMediaDriver.launch(ctx, systemTestWatcher));
        final Aeron aeron = addCloseable(
            Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName())));

        final Subscription fastConsumer = addCloseable(aeron.addSubscription(MDC_SUBSCRIPTION_CHANNEL, STREAM_ID));
        final CountingFragmentHandler fastConsumerHandler = new CountingFragmentHandler();

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .startPosition(FROM_LIVE);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 1));

            // Consume a batch of messages on live
            final List<byte[]> firstMessageBatch = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
            persistentPublication.persist(firstMessageBatch);
            executeUntil(
                () -> fastConsumerHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount()),
                () -> fastConsumer.poll(fastConsumerHandler, 10));

            // Remove the recording
            aeronArchive.stopRecording(persistentPublication.publication);
            aeronArchive.purgeRecording(persistentPublication.recordingId);

            // Allow a faster consumer to advance ahead, causing the persistent subscription to drop from live.
            final List<byte[]> secondMessageBatch = new ArrayList<>();
            for (int i = 0; i < 3; i++)
            {
                final List<byte[]> batch = generateFixedPayloads(32, ONE_KB_MESSAGE_SIZE);
                persistentPublication.publish(batch);
                secondMessageBatch.addAll(batch);
                executeUntil(
                    () -> fastConsumerHandler.hasReceivedPayloads(secondMessageBatch.size()),
                    () -> fastConsumer.poll(fastConsumerHandler, 10)
                );
            }

            // Verify we cannot fall back to replay, as the recording no longer exists
            executeUntil(
                persistentSubscription::hasFailed,
                () -> poll(persistentSubscription, fragmentHandler, 10),
                description(persistentSubscription, fragmentHandler, listener)
            );
            assertEquals(PersistentSubscriptionException.class, listener.lastException.getClass());
            assertEquals(listener.lastException, persistentSubscription.failureReason());
            assertThat(
                listener.lastException.getMessage(),
                containsString("unknown recording id:")
            );

            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldReconnectToTheArchiveAfterArchiveRestart(final @TempDir Path tempDir)
    {
        final String aeron2Dir = CommonContext.generateRandomDirName();

        final MediaDriver.Context driver2CtxTpl = driverCtxTpl.clone().aeronDirectoryName(aeron2Dir);
        final TestMediaDriver mediaDriver2 =
            addCloseable(TestMediaDriver.launch(driver2CtxTpl.clone(), systemTestWatcher));
        systemTestWatcher.dataCollector().add(driver2CtxTpl.aeronDirectory());

        final Aeron aeron2 = addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeron2Dir)));

        final String archiveControlRequestChannel = "aeron:udp?endpoint=localhost:8011";
        final File remoteArchiveDir = new File(tempDir.toString(), "remoteArchiveDir");

        final Archive.Context remoteArchiveCtx = archiveCtxTpl.clone()
            .archiveDir(remoteArchiveDir)
            .aeronDirectoryName(aeron2Dir)
            .controlChannel(archiveControlRequestChannel)
            .deleteArchiveOnStart(false);

        final Archive archive = addCloseable(Archive.launch(remoteArchiveCtx.clone()));
        systemTestWatcher.dataCollector().add(remoteArchiveCtx.archiveDir());

        final AeronArchive.Context remoteAeronArchiveCtx = aeronArchiveCtxTpl.clone()
            .controlRequestChannel(archiveControlRequestChannel)
            .aeron(aeron2);

        final AeronArchive remoteArchive = addCloseable(AeronArchive.connect(remoteAeronArchiveCtx.clone()));
        assert remoteArchive != null;

        final ExclusivePublication exclusivePublication =
            addCloseable(aeron.addExclusivePublication(MDC_PUBLICATION_CHANNEL, STREAM_ID));
        remoteArchive.startRecording(MDC_SUBSCRIPTION_CHANNEL, STREAM_ID, SourceLocation.REMOTE);
        Tests.awaitConnected(exclusivePublication);

        final PersistentPublication persistentPublication =
            PersistentPublication.create(remoteArchive, exclusivePublication);

        persistentSubscriptionCtx
            .aeron(aeron)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .recordingId(persistentPublication.recordingId())
            .aeronArchiveContext(remoteAeronArchiveCtx.aeron(aeron))
            .startPosition(FROM_START);

        final List<byte[]> firstMessageBatch = generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE);
        final List<byte[]> secondMessageBatch = generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE);
        final List<byte[]> thirdMessagesBatch = generateFixedPayloads(64, ONE_KB_MESSAGE_SIZE);

        persistentPublication.persist(firstMessageBatch);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 1));

            assertEquals(persistentPublication.publishedMessageCount, fragmentHandler.receivedPayloads.size());

            persistentPublication.persist(secondMessageBatch);
            persistentPublication.persist(thirdMessagesBatch);

            archive.close();
            aeron2.close();
            mediaDriver2.close();

            addCloseable(TestMediaDriver.launch(driver2CtxTpl.clone(), systemTestWatcher));
            addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeron2Dir)));
            addCloseable(Archive.launch(remoteArchiveCtx.clone()));

            executeUntil(
                persistentSubscription::isReplaying,
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );
            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );
            assertPayloads(fragmentHandler.receivedPayloads, firstMessageBatch, secondMessageBatch, thirdMessagesBatch);
            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldRecoverFromArchiveRestartDuringReplay(final @TempDir Path tempDir)
    {
        final String remoteAeronDir = CommonContext.generateRandomDirName();

        final MediaDriver.Context remoteDriverCtxTpl = driverCtxTpl.clone().aeronDirectoryName(remoteAeronDir);
        final TestMediaDriver remoteMediaDriver =
            addCloseable(TestMediaDriver.launch(remoteDriverCtxTpl.clone(), systemTestWatcher));
        systemTestWatcher.dataCollector().add(remoteDriverCtxTpl.aeronDirectory());

        final Aeron remoteAeron = addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(remoteAeronDir)));

        final String archiveControlRequestChannel = "aeron:udp?endpoint=localhost:8011";
        final File remoteArchiveDir = new File(tempDir.toString(), "remoteArchiveDir");

        final Archive.Context remoteArchiveCtx = archiveCtxTpl.clone()
            .archiveDir(remoteArchiveDir)
            .aeronDirectoryName(remoteAeronDir)
            .controlChannel(archiveControlRequestChannel)
            .deleteArchiveOnStart(false);

        final Archive remoteArchive = addCloseable(Archive.launch(remoteArchiveCtx.clone()));
        systemTestWatcher.dataCollector().add(remoteArchiveCtx.archiveDir());

        final AeronArchive.Context remoteAeronArchiveCtx = aeronArchiveCtxTpl.clone()
            .controlRequestChannel(archiveControlRequestChannel)
            .aeron(remoteAeron);

        final AeronArchive remoteAeronArchive = addCloseable(AeronArchive.connect(remoteAeronArchiveCtx.clone()));

        final ExclusivePublication exclusivePublication =
            addCloseable(aeron.addExclusivePublication(MDC_PUBLICATION_CHANNEL, STREAM_ID));
        remoteAeronArchive.startRecording(MDC_SUBSCRIPTION_CHANNEL, STREAM_ID, SourceLocation.REMOTE);
        Tests.awaitConnected(exclusivePublication);

        final PersistentPublication persistentPublication =
            PersistentPublication.create(remoteAeronArchive, exclusivePublication);

        // Publish enough messages so the PS will still be replaying when we kill the archive
        final List<byte[]> messages = generateFixedPayloads(80, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(messages);

        persistentSubscriptionCtx
            .aeron(aeron)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .recordingId(persistentPublication.recordingId())
            .aeronArchiveContext(remoteAeronArchiveCtx.aeron(aeron))
            .startPosition(FROM_START);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            // Wait for a few messages to arrive — PS should still be replaying
            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(5),
                () -> poll(persistentSubscription, fragmentHandler, 1));

            assertTrue(persistentSubscription.isReplaying());

            // Kill archive while PS is still replaying
            remoteArchive.close();
            remoteAeron.close();
            remoteMediaDriver.close();

            // PS should detect disconnection and leave replay without failing
            executeUntil(
                () -> !persistentSubscription.isReplaying(),
                () -> poll(persistentSubscription, fragmentHandler, 1));

            assertFalse(persistentSubscription.hasFailed());

            // Restart archive with existing data
            addCloseable(TestMediaDriver.launch(remoteDriverCtxTpl.clone(), systemTestWatcher));
            addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(remoteAeronDir)));
            addCloseable(Archive.launch(remoteArchiveCtx.clone()));

            // PS should recover, replay remaining messages, and become live
            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, messages);
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldContinueConsumingFromLiveWhileArchiveIsUnavailable()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final List<byte[]> firstMessageBatch = generateRandomPayloads(5);
        final List<byte[]> secondMessageBatch = generateRandomPayloads(5);
        persistentPublication.persist(firstMessageBatch);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount()) &&
                      persistentSubscription.isLive(),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            archive.close();

            persistentPublication.publish(secondMessageBatch);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount()),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, firstMessageBatch, secondMessageBatch);

            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void canFallbackToReplayInTheMiddleOfAFragmentedMessage()
    {
        TestMediaDriver.notSupportedOnCMediaDriver("loss generator");

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final int maxPayloadLength = persistentPublication.maxPayloadLength();
        final byte[] firstHalfOfMessage = new byte[maxPayloadLength];
        Arrays.fill(firstHalfOfMessage, (byte)1);
        final byte[] secondHalfOfMessage = new byte[maxPayloadLength];
        Arrays.fill(secondHalfOfMessage, (byte)2);
        final byte[] largeMessage = new byte[firstHalfOfMessage.length + secondHalfOfMessage.length];
        System.arraycopy(firstHalfOfMessage, 0, largeMessage, 0, firstHalfOfMessage.length);
        System.arraycopy(secondHalfOfMessage, 0, largeMessage, firstHalfOfMessage.length, secondHalfOfMessage.length);

        final StreamIdFrameDataLossGenerator streamIdFrameDataLossGenerator = new StreamIdFrameDataLossGenerator();

        final String aeron2Dir = CommonContext.generateRandomDirName();

        final MediaDriver.Context driverCtxWithLoss = driverCtxTpl.clone()
            .aeronDirectoryName(aeron2Dir)
            .imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(1))
            .receiveChannelEndpointSupplier(receiveChannelEndpointSupplier(streamIdFrameDataLossGenerator));
        addCloseable(TestMediaDriver.launch(driverCtxWithLoss, systemTestWatcher));
        systemTestWatcher.dataCollector().add(driverCtxWithLoss.aeronDirectory());

        final Aeron.Context aeron2Context = aeronCtxTpl.clone()
            .aeronDirectoryName(aeron2Dir);
        final Aeron aeron2 = addCloseable(Aeron.connect(aeron2Context));

        persistentSubscriptionCtx
            .aeron(aeron2)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .liveStreamId(STREAM_ID)
            .recordingId(persistentPublication.recordingId())
            .aeronDirectoryName(aeron2Dir)
            .startPosition(FROM_LIVE);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 1)
            );

            final AtomicBoolean keepDroppingAfterMatch = new AtomicBoolean(false);
            streamIdFrameDataLossGenerator.enable(STREAM_ID,
                (bytes) ->
                {
                    final byte[] payload = new byte[bytes.length - DataHeaderFlyweight.HEADER_LENGTH];
                    System.arraycopy(bytes, DataHeaderFlyweight.HEADER_LENGTH, payload, 0, payload.length);
                    if (Arrays.equals(payload, secondHalfOfMessage))
                    {
                        keepDroppingAfterMatch.set(true);
                    }
                    return keepDroppingAfterMatch.get();
                }
            );
            persistentPublication.persist(List.of(largeMessage));

            executeUntil(
                persistentSubscription::isReplaying,
                () -> poll(persistentSubscription, fragmentHandler, 1)
            );
            assertTrue(fragmentHandler.receivedPayloads.isEmpty());

            streamIdFrameDataLossGenerator.disable();

            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 1)
            );
            assertPayloads(fragmentHandler.receivedPayloads, List.of(largeMessage));

            verify(persistentSubscription);
        }
    }

    // 30% replay loss while PS catches up from archive. NAK/retransmit must deliver all 100.
    @Test
    @InterruptAfter(60)
    void shouldReceiveAllMessagesWithModerateReplayLoss()
    {
        runReplayLossTest(100, 0.3);
    }

    // 80% replay loss — severe stress on NAK/retransmit. Verifies no duplicates, no gaps.
    @Test
    @InterruptAfter(90)
    void shouldReceiveAllMessagesWithHeavyReplayLoss()
    {
        runReplayLossTest(50, 0.8);
    }

    // Persist initial batch, start PS with lossy replay, then publish more WHILE PS is still
    // replaying. PS must catch up through lossy replay, transition to LIVE, receive everything.
    @Test
    @InterruptAfter(60)
    void shouldTransitionToLiveThroughLossyReplayWhilePublishing()
    {
        TestMediaDriver.notSupportedOnCMediaDriver("loss generator");

        final StreamIdFrameDataLossGenerator lossGenerator = new StreamIdFrameDataLossGenerator();
        final String aeronDir2 = CommonContext.generateRandomDirName();
        final MediaDriver.Context driver2Ctx = driverCtxTpl.clone().aeronDirectoryName(aeronDir2)
            .receiveChannelEndpointSupplier(receiveChannelEndpointSupplier(lossGenerator));
        addCloseable(TestMediaDriver.launch(driver2Ctx, systemTestWatcher));
        systemTestWatcher.dataCollector().add(driver2Ctx.aeronDirectory());
        final Aeron aeron2 = addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeronDir2)));

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);
        final List<byte[]> initialMessages = generateFixedPayloads(20, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(initialMessages);

        persistentSubscriptionCtx
            .aeron(aeron2)
            .recordingId(persistentPublication.recordingId())
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            final ThreadLocalRandom random = ThreadLocalRandom.current();
            lossGenerator.enable(persistentSubscriptionCtx.replayStreamId(), bytes -> random.nextDouble() < 0.5);

            // Start PS draining; while it's catching up from archive (with loss), publish more.
            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(initialMessages.size() / 2),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            final List<byte[]> liveMessages = generateFixedPayloads(20, ONE_KB_MESSAGE_SIZE);
            persistentPublication.publish(liveMessages);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(initialMessages.size() + liveMessages.size()) &&
                    persistentSubscription.isLive(),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, initialMessages, liveMessages);
            lossGenerator.disable();
            verify(persistentSubscription);
        }
    }

    private void runReplayLossTest(final int messageCount, final double dropRate)
    {
        TestMediaDriver.notSupportedOnCMediaDriver("loss generator");

        final StreamIdFrameDataLossGenerator lossGenerator = new StreamIdFrameDataLossGenerator();
        final String aeronDir2 = CommonContext.generateRandomDirName();
        final MediaDriver.Context driver2Ctx = driverCtxTpl.clone().aeronDirectoryName(aeronDir2)
            .receiveChannelEndpointSupplier(receiveChannelEndpointSupplier(lossGenerator));
        addCloseable(TestMediaDriver.launch(driver2Ctx, systemTestWatcher));
        systemTestWatcher.dataCollector().add(driver2Ctx.aeronDirectory());
        final Aeron aeron2 = addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeronDir2)));

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);
        final List<byte[]> messages = generateFixedPayloads(messageCount, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(messages);

        persistentSubscriptionCtx
            .aeron(aeron2)
            .recordingId(persistentPublication.recordingId())
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            // Drop the configured fraction of DATA frames on the replay stream-id. SETUPs and
            // other control go through. NAK/retransmit must restore everything.
            final ThreadLocalRandom random = ThreadLocalRandom.current();
            lossGenerator.enable(persistentSubscriptionCtx.replayStreamId(), bytes -> random.nextDouble() < dropRate);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(messages.size()) && persistentSubscription.isLive(),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, messages);
            lossGenerator.disable();
            verify(persistentSubscription);
        }
    }

    // 30% loss on PS's live MDC subscription channel. Initial 5 messages are persisted before
    // PS starts so they replay cleanly; 30 more are offered live with loss. Verifies all 35
    // messages are received in order — proves NAK-driven retransmission across loss.
    @Test
    @InterruptAfter(60)
    void shouldReceiveAllMessagesWithLossOnLiveChannel()
    {
        TestMediaDriver.notSupportedOnCMediaDriver("loss generator");

        final StreamIdFrameDataLossGenerator lossGenerator = new StreamIdFrameDataLossGenerator();
        final String aeronDir2 = CommonContext.generateRandomDirName();
        final MediaDriver.Context driver2Ctx = driverCtxTpl.clone().aeronDirectoryName(aeronDir2)
            .receiveChannelEndpointSupplier(receiveChannelEndpointSupplier(lossGenerator));
        addCloseable(TestMediaDriver.launch(driver2Ctx, systemTestWatcher));
        systemTestWatcher.dataCollector().add(driver2Ctx.aeronDirectory());
        final Aeron aeron2 = addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeronDir2)));

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);
        final List<byte[]> initialMessages = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(initialMessages);

        persistentSubscriptionCtx
            .aeron(aeron2)
            .recordingId(persistentPublication.recordingId())
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            // Enable 30% drop on the live stream-id BEFORE PS is live, so SETUPs go through but
            // post-attach DATA frames are sampled for drop.
            final ThreadLocalRandom random = ThreadLocalRandom.current();
            lossGenerator.enable(STREAM_ID, bytes -> random.nextDouble() < 0.3);

            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10));

            // Use offer (non-blocking) so persist's 30s timeout doesn't trip with backpressure.
            final List<byte[]> liveMessages = generateFixedPayloads(30, ONE_KB_MESSAGE_SIZE);
            persistentPublication.publish(liveMessages);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(initialMessages.size() + liveMessages.size()),
                () -> poll(persistentSubscription, fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, initialMessages, liveMessages);
            lossGenerator.disable();
            verify(persistentSubscription);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { UNICAST_CHANNEL, IPC_CHANNEL })
    @InterruptAfter(15)
    void anUntetheredPersistentSubscriptionCanFallBehindATetheredSubscription(final String channel)
    {
        final ChannelUriStringBuilder channelUriStringBuilder = new ChannelUriStringBuilder(channel);

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, channel, STREAM_ID);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .liveChannel(channelUriStringBuilder.tether(false).build());

        final CountingFragmentHandler fastSubscriptionFragmentHandler = new CountingFragmentHandler();
        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx);
            Subscription fastSubscription = aeron.addSubscription(channelUriStringBuilder.tether(true)
                .build(), STREAM_ID))
        {
            Tests.awaitConnected(fastSubscription);

            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );
            persistentPublication.persist(generateFixedPayloads(32, ONE_KB_MESSAGE_SIZE));
            executeUntil(
                () -> fastSubscriptionFragmentHandler.hasReceivedPayloads(32),
                () -> fastSubscription.poll(fastSubscriptionFragmentHandler, 10)
            );
            persistentPublication.persist(generateFixedPayloads(32, ONE_KB_MESSAGE_SIZE));
            executeUntil(
                () -> fastSubscriptionFragmentHandler.hasReceivedPayloads(64),
                () -> fastSubscription.poll(fastSubscriptionFragmentHandler, 10)
            );

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(64),
                () -> poll(persistentSubscription, fragmentHandler, 10)
            );

            assertTrue(persistentSubscription.isReplaying());

            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 1)
            );
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
            executeUntil(persistentSubscription::hasFailed, () -> poll(persistentSubscription, null, 1));

            assertEquals(1, listener.errorCount);
            assertEquals(
                Reason.RECORDING_NOT_FOUND,
                ((PersistentSubscriptionException)listener.lastException).reason()
            );
            assertEquals(listener.lastException, persistentSubscription.failureReason());
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
            executeUntil(persistentSubscription::hasFailed, () -> poll(persistentSubscription, null, 1));

            assertEquals(1, listener.errorCount);
            assertEquals(
                Reason.STREAM_ID_MISMATCH,
                ((PersistentSubscriptionException)listener.lastException).reason()
            );
            assertEquals(listener.lastException, persistentSubscription.failureReason());
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldFailIfLiveStreamPositionGoesBackwards()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        final List<byte[]> payloads = generateFixedPayloads(2, 32);
        persistentPublication.persist(payloads);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId());

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                persistentSubscription::isLive,
                () -> poll(persistentSubscription, fragmentHandler, 10));

            persistentPublication.close();

            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

            executeUntil(
                persistentSubscription::hasFailed,
                () -> poll(persistentSubscription, fragmentHandler, 10),
                description(persistentSubscription, fragmentHandler, listener));

            assertEquals(
                "ERROR - live stream joined at position 0 which is earlier than last seen position 128",
                persistentSubscription.failureReason().getMessage());
        }
    }

    @Test
    @InterruptAfter(30)
    void shouldRecoverFromReplayChannelNetworkProblems() throws Exception
    {
        shouldRecoverFromNetworkProblems(NetworkFlow.REPLAY);
    }

    @Test
    @InterruptAfter(30)
    void shouldRecoverFromLiveChannelNetworkProblems() throws Exception
    {
        shouldRecoverFromNetworkProblems(NetworkFlow.LIVE);
    }

    @Test
    @InterruptAfter(30)
    void shouldNotUseStaleNextLivePositionAfterRefreshFromAttemptSwitch() throws Exception
    {
        TestMediaDriver.notSupportedOnCMediaDriver("loss generator");

        final String pubChannel =
            "aeron:udp?term-length=16m|control=localhost:24326|control-mode=dynamic|fc=min";
        final String subChannel = "aeron:udp?control=localhost:24326|group=true";

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

        final int ratePerSecond = 500;
        final long maxProcessingTime = 1_000_000_000 / ratePerSecond / 8;

        final AtomicBoolean publisherPaused = new AtomicBoolean(false);
        final Thread publisher = startPausablePublisher(
            persistentPublication, publisherPaused, ratePerSecond, "shouldNotUseStaleNextLivePositionPublisher");

        // Give the publisher a head start so PS will have meaningful replay work to do.
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            final MessageVerifier handler = new MessageVerifier(maxProcessingTime);

            // Step (a): wait until PS has actually entered ATTEMPT_SWITCH 1 — joinDifference
            // is set to (livePosition - replayPosition) at the REPLAY → ATTEMPT_SWITCH
            // transition and starts at Long.MIN_VALUE — then arm replay-stream loss. Polling
            // with fragment_limit=1 stretches replay catchup over many polls so the test thread
            // can observe the ATTEMPT_SWITCH window before catchup completes naturally.
            // Coupling to ATTEMPT_SWITCH (rather than isReplaying() which also matches REPLAY)
            // eliminates the prior race where loss could be armed during REPLAY and stall
            // catchup before the live subscription was even added.
            executeUntil(
                () -> persistentSubscription.joinDifference() > 0 && !persistentSubscription.isLive(),
                () -> poll(persistentSubscription, handler, 1));

            lossGenerator.enable(persistentSubscriptionCtx.replayStreamId());

            // Step (b): wait for the refresh path to engage. resetReplayCatchupState resets
            // joinDifference back to Long.MIN_VALUE, so observing that transition tells us
            // image-liveness has fired on the replay 1 image and PS has started the refresh.
            executeUntil(
                () -> persistentSubscription.joinDifference() == Long.MIN_VALUE,
                () -> poll(persistentSubscription, handler, 10));

            // Step (c): pause the publisher now that refresh has fired. By the time replay 2
            // adds a new live subscription, the publisher's snd_pos will be frozen and the
            // new live image attaches with an empty term buffer — so onLiveCatchupFragment
            // is never called during ATTEMPT_SWITCH 2 and nextLivePosition (without the fix)
            // keeps its stale value from ATTEMPT_SWITCH 1.
            publisherPaused.set(true);

            // Give publisher's most recent offer a moment to be flushed by the sender thread,
            // then disable loss so replay 2's setup can complete.
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
            lossGenerator.disable();

            // Resume the publisher only after PS has either reached LIVE or failed.
            final long settleDeadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!persistentSubscription.isLive() &&
                !persistentSubscription.hasFailed() &&
                System.nanoTime() - settleDeadlineNs < 0)
            {
                poll(persistentSubscription, handler, 10);
            }

            assertFalse(persistentSubscription.hasFailed(),
                "PS failed before reaching LIVE: " + persistentSubscription.failureReason());
            assertTrue(persistentSubscription.isLive(),
                "PS did not reach LIVE within the deadline");

            // Resume the publisher and let PS drain the stream.
            publisherPaused.set(false);

            interruptAndJoin(publisher);

            drainStream(persistentSubscription, handler, persistentPublication.position(),
                TimeUnit.SECONDS.toNanos(10));

            verify(persistentSubscription);
        }
    }

    private enum NetworkFlow
    {
        REPLAY,
        LIVE,
    }

    @SuppressWarnings("methodlength")
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

        final int ratePerSecond = 500;
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
                if (poll(persistentSubscription, handler, 10) == 0)
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

            assertEquals(1, listener.liveJoinedCount);
            assertEquals(0, listener.liveLeftCount);
            verify(persistentSubscription);

            if (victimFlow == NetworkFlow.LIVE)
            {
                lossState = LossState.WAITING_TO_START;
                deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);

                while (true)
                {
                    if (poll(persistentSubscription, handler, 10) == 0)
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
                        assertEquals(1, listener.liveLeftCount);
                        lossState = LossState.FINISHED;
                        lossGenerator.disable();
                    }

                    if (lossState == LossState.FINISHED && persistentSubscription.isLive())
                    {
                        assertEquals(2, listener.liveJoinedCount);
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
                if (poll(persistentSubscription, handler, 10) == 0)
                {
                    checkForInterrupt("failed to drain the stream");
                }
            }

            verify(persistentSubscription);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldCloseArchiveConnectionOnFailureInCaseApplicationKeepsPolling()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(8192);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::hasFailed, () -> poll(persistentSubscription, fragmentHandler, 1));
            executeUntil(
                () -> archive.context().controlSessionsCounter().get() == 1,
                () -> poll(persistentSubscription, fragmentHandler, 1),
                () -> "controlSessionsCounter=" + archive.context().controlSessionsCounter().get());
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldNotRequireEventListener()
    {
        final PersistentSubscriptionListenerImpl listener = null; // <-- null listener
        persistentSubscriptionCtx.listener(listener);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::hasFailed, () -> poll(persistentSubscription, null, 1));
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldCreateOwnAeronInstanceWhenNotSupplied()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);
        persistentPublication.persist(generateRandomPayloads(2));

        final PersistentSubscription.Context persistentSubscriptionCtx = new PersistentSubscription.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .recordingId(persistentPublication.recordingId)
            .startPosition(FROM_START)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .liveStreamId(STREAM_ID)
            .replayChannel("aeron:udp?endpoint=localhost:0")
            .replayStreamId(-5)
            .listener(listener)
            .aeronArchiveContext(aeronArchiveCtxTpl.clone());

        final PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx);
        executeUntil(
            persistentSubscription::isLive,
            () -> poll(persistentSubscription, fragmentHandler, 10));
        final Aeron aeron = persistentSubscriptionCtx.aeron();
        assertNotNull(aeron);
        persistentSubscription.close();
        assertTrue(aeron.isClosed());
    }

    @Test
    @InterruptAfter(5)
    void shouldNotCloseSuppliedAeronInstance()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);
        persistentPublication.persist(generateRandomPayloads(2));

        final PersistentSubscription.Context persistentSubscriptionCtx = this.persistentSubscriptionCtx.clone()
            .aeron(aeron)
            .recordingId(persistentPublication.recordingId)
            .startPosition(FROM_START)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .aeronArchiveContext(aeronArchiveCtxTpl.clone().aeron(aeron));

        final PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx);
        executeUntil(
            persistentSubscription::isLive,
            () -> poll(persistentSubscription, fragmentHandler, 10));
        persistentSubscription.close();
        assertFalse(aeron.isClosed());
    }

    private static ReceiveChannelEndpointSupplier receiveChannelEndpointSupplier(final LossGenerator lossGenerator)
    {
        return (udpChannel, dispatcher, statusIndicator, context) ->
            new DebugReceiveChannelEndpoint(
                udpChannel, dispatcher, statusIndicator, context, lossGenerator, lossGenerator);
    }

    private static SendChannelEndpointSupplier sendChannelEndpointSupplier(final LossGenerator lossGenerator)
    {
        return (udpChannel, statusIndicator, context) ->
            new DebugSendChannelEndpoint(
                udpChannel, statusIndicator, context, lossGenerator, lossGenerator);
    }

    private static void interruptAndJoin(final Thread thread) throws InterruptedException
    {
        thread.interrupt();
        thread.join();
    }

    private Thread startPausablePublisher(
        final PersistentPublication persistentPublication,
        final AtomicBoolean publisherPaused,
        final int ratePerSecond,
        final String threadName)
    {
        final Thread publisher = new Thread(
            () ->
            {
                final ThreadLocalRandom random = ThreadLocalRandom.current();
                final UnsafeBuffer buffer = new UnsafeBuffer(new byte[2048]);
                long messageId = 0;
                long nextMessageAt = System.nanoTime() + exponentialArrivalDelay(ratePerSecond);
                while (!Thread.currentThread().isInterrupted())
                {
                    if (publisherPaused.get())
                    {
                        Thread.yield();
                        continue;
                    }
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
            threadName);
        publisher.start();
        addCloseable(() -> interruptAndJoin(publisher));
        return publisher;
    }

    private void drainStream(
        final PersistentSubscription persistentSubscription,
        final MessageVerifier handler,
        final long lastPosition,
        final long timeoutNs)
    {
        final long deadlineNs = System.nanoTime() + timeoutNs;
        while (handler.position < lastPosition && System.nanoTime() - deadlineNs < 0)
        {
            if (poll(persistentSubscription, handler, 10) == 0)
            {
                Thread.yield();
            }
        }
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

        System.out.println("join difference = " + persistentSubscription.joinDifference());
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
        final Random random = randomWatcher.random();
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

    private Aeron startSecondAeronWithReceiveLoss(final LossGenerator lossGenerator)
    {
        final String aeronDir2 = CommonContext.generateRandomDirName();
        final MediaDriver.Context driver2Ctx = driverCtxTpl.clone().aeronDirectoryName(aeronDir2)
            .receiveChannelEndpointSupplier(receiveChannelEndpointSupplier(lossGenerator));
        addCloseable(TestMediaDriver.launch(driver2Ctx, systemTestWatcher));
        systemTestWatcher.dataCollector().add(driver2Ctx.aeronDirectory());
        return addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeronDir2)));
    }

    // Returns {initialTermId, termBufferLength} from the recording descriptor.
    private int[] readRecordingTermLayout(final long recordingId)
    {
        final int[] result = { 0, 0 };
        aeronArchive.listRecording(recordingId,
            (controlSessionId, correlationId, recordingId1, startTimestamp, stopTimestamp,
                startPosition, stopPos, initialTermId, segmentFileLength, termBufferLength,
                mtuLength, sessionId, streamId1, strippedChannel, originalChannel, sourceIdentity) ->
            {
                result[0] = initialTermId;
                result[1] = termBufferLength;
            });
        return result;
    }

    // Wait for the closed publication's residual state to drain; otherwise PS's live
    // subscription can briefly attach to the ghost image and join LIVE before the actual
    // resumed publisher's image arrives.
    private static void closePublicationAndAwaitDrain(final PersistentPublication publication)
    {
        publication.closePublicationOnly();
        Tests.await(() -> !publication.publicationCountersExist());
    }

    private Runnable newReplayObservingPoller(
        final PersistentSubscription persistentSubscription,
        final boolean[] observedReplaying)
    {
        return () ->
        {
            poll(persistentSubscription, fragmentHandler, 10);
            if (persistentSubscription.isReplaying())
            {
                observedReplaying[0] = true;
            }
        };
    }

    private void awaitFirstAwaitLiveDeadlineBreach(
        final PersistentSubscription persistentSubscription,
        final Runnable pollAndTrack,
        final boolean[] observedReplaying)
    {
        executeUntil(() -> listener.errorCount > 0, pollAndTrack);
        // Exactly one deadline breach — fired once while PS was parked in AWAIT_LIVE.
        assertEquals(1, listener.errorCount);
        assertThat(
            listener.lastException.getMessage(),
            containsString("No image became available on the live subscription"));
        assertFalse(persistentSubscription.isLive());
        assertFalse(observedReplaying[0]);
    }

    // Drop every DATA frame on PS's live stream whose term offset lies past fromPosition,
    // all the way to end-of-term. Stream-id discrimination (STREAM_ID vs the replay
    // stream id) leaves the replay subscription unaffected.
    private void armDataDropFromPosition(
        final DataInRangeLossGenerator lossGenerator,
        final long recordingId,
        final long fromPosition)
    {
        final int[] info = readRecordingTermLayout(recordingId);
        final int initialTermId = info[0];
        final int termBufferLength = info[1];
        final int positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termBufferLength);
        final int targetActiveTermId = LogBufferDescriptor.computeTermIdFromPosition(
            fromPosition, positionBitsToShift, initialTermId);
        final int targetTermOffsetMin = (int)(fromPosition & (termBufferLength - 1L));
        lossGenerator.setTarget(STREAM_ID, targetActiveTermId, targetTermOffsetMin, termBufferLength);
    }

    // Drop SETUP frames whose (term_id, term_offset) tuple matches position. Once the
    // publisher's snd_pos advances past it, subsequent SETUPs no longer match and PS
    // attaches at the advanced position instead.
    private void armSetupDropAtPosition(
        final SetupAtPositionLossGenerator lossGenerator,
        final long recordingId,
        final long position)
    {
        final int[] info = readRecordingTermLayout(recordingId);
        final int initialTermId = info[0];
        final int termBufferLength = info[1];
        final int positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termBufferLength);
        final int targetActiveTermId = LogBufferDescriptor.computeTermIdFromPosition(
            position, positionBitsToShift, initialTermId);
        final int targetTermOffset = (int)(position & (termBufferLength - 1L));
        lossGenerator.setTarget(STREAM_ID, initialTermId, targetActiveTermId, targetTermOffset);
    }

    private static void executeUntil(final BooleanSupplier predicate, final Runnable runnable)
    {
        Tests.await(() ->
        {
            runnable.run();
            return predicate.getAsBoolean();
        });
    }

    private static void executeUntil(
        final BooleanSupplier predicate,
        final Runnable runnable,
        final Supplier<String> errorMessageSupplier)
    {
        while (true)
        {
            runnable.run();

            if (predicate.getAsBoolean())
            {
                break;
            }

            if (Thread.interrupted())
            {
                throw new TimeoutException(errorMessageSupplier.get());
            }

            Thread.yield();
        }
    }

    private static Supplier<String> description(
        final PersistentSubscription persistentSubscription,
        final BufferingFragmentHandler fragmentHandler,
        final PersistentSubscriptionListenerImpl listener)
    {
        return () -> "PersistentSubscription: isLive=" + persistentSubscription.isLive() +
                     " isReplaying=" + persistentSubscription.isReplaying() +
                     " hasFailed=" + persistentSubscription.hasFailed() +
                     " FragmentHandler: count=" + fragmentHandler.receivedPayloads.size() +
                     " position=" + fragmentHandler.position +
                     " Listener: liveJoinedCount=" + listener.liveJoinedCount +
                     " liveLeftCount=" + listener.liveLeftCount +
                     " errorCount=" + listener.errorCount +
                     " lastError=" + (listener.lastException != null ? listener.lastException.getMessage() : "null");
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

    private void verify(final PersistentSubscription persistentSubscription)
    {
        final PersistentSubscription.Context context = persistentSubscription.context();

        assertEquals(persistentSubscription.joinDifference(), context.joinDifferenceCounter().get());

        if (context.listener() == listener)
        {
            assertEquals(listener.liveJoinedCount, context.liveJoinedCounter().get());
            assertEquals(listener.liveLeftCount, context.liveLeftCounter().get());
        }
    }

    private static Stream<Arguments> liveChannels()
    {
        return Stream.of(1, 10, Integer.MAX_VALUE)
            .flatMap((fragmentLimit) -> Stream.of(
                arguments(fragmentLimit, IPC_CHANNEL, IPC_CHANNEL),
                arguments(fragmentLimit, MULTICAST_CHANNEL, MULTICAST_CHANNEL),
                arguments(fragmentLimit, SPY_PREFIX + MDC_PUBLICATION_CHANNEL, MDC_PUBLICATION_CHANNEL)
            ));
    }

    private static Stream<Arguments> replayChannelsAndStreams()
    {
        return Stream.of(
            arguments("aeron:udp?endpoint=localhost:0",
                -10,
                LOCALHOST_CONTROL_REQUEST_CHANNEL,
                LOCALHOST_CONTROL_RESPONSE_CHANNEL
            ),
            arguments(
                "aeron:udp?endpoint=localhost:10001",
                -11,
                LOCALHOST_CONTROL_REQUEST_CHANNEL,
                LOCALHOST_CONTROL_RESPONSE_CHANNEL
            ),
            arguments("aeron:ipc", -12, LOCALHOST_CONTROL_REQUEST_CHANNEL, LOCALHOST_CONTROL_RESPONSE_CHANNEL),
            arguments(
                "aeron:udp?control=localhost:10001|control-mode=response",
                -11,
                LOCALHOST_CONTROL_REQUEST_CHANNEL,
                "aeron:udp?control-mode=response|control=localhost:10002"
            ),
            arguments(
                "aeron:udp?control=localhost:10001|control-mode=response|endpoint=localhost:5006",
                -11,
                LOCALHOST_CONTROL_REQUEST_CHANNEL,
                "aeron:udp?control-mode=response|control=localhost:10002"
            ),
            arguments(
                "aeron:udp?control=localhost:10001|control-mode=response|endpoint=localhost:0",
                -11,
                LOCALHOST_CONTROL_REQUEST_CHANNEL,
                "aeron:udp?control-mode=response|control=localhost:10002"),
            arguments("aeron:ipc?control-mode=response", -11, "aeron:ipc", "aeron:ipc?control-mode=response")
        );
    }

    private static String removeExtraFields(final String channel)
    {
        final ChannelUri uri = ChannelUri.parse(channel);
        uri.remove(SESSION_ID_PARAM_NAME);
        uri.remove(REJOIN_PARAM_NAME);
        return uri.toString();
    }

    private interface FragmentConsumer
    {
        ControlledFragmentHandler.Action consumeFragment(DirectBuffer buffer, int offset, int length, Header header);

        default ControlledFragmentHandler.Action onFragmentControlled(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            return consumeFragment(buffer, offset, length, header);
        }

        default void onFragmentUncontrolled(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            consumeFragment(buffer, offset, length, header);
        }
    }

    private static final class MessageVerifier implements FragmentConsumer
    {
        private final long maxProcessingTime;
        long expectedMessageId;
        long position;

        private MessageVerifier(final long maxProcessingTime)
        {
            this.maxProcessingTime = maxProcessingTime;
        }

        public ControlledFragmentHandler.Action consumeFragment(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
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
            return ControlledFragmentHandler.Action.CONTINUE;
        }
    }

    private static final class BufferingFragmentHandler implements FragmentConsumer
    {
        private final List<byte[]> receivedPayloads = new ArrayList<>();
        private long position;

        public ControlledFragmentHandler.Action consumeFragment(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            position = header.position();
            final byte[] bytes = new byte[length];
            buffer.getBytes(offset, bytes);
            receivedPayloads.add(bytes);
            return ControlledFragmentHandler.Action.CONTINUE;
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
        long liveJoinedCount;
        long liveLeftCount;
        int errorCount;
        Exception lastException;

        public void onLiveJoined()
        {
            liveJoinedCount++;
        }

        public void onLiveLeft()
        {
            liveLeftCount++;
        }

        public void onError(final Exception e)
        {
            errorCount++;
            lastException = e;
        }
    }

    private static final class PersistentPublication implements AutoCloseable
    {
        private final AeronArchive aeronArchive;
        private final ExclusivePublication publication;
        private final long recordingId;
        private final CountersReader countersReader;
        private final int recordingCounterId;
        private int publishedMessageCount = 0;

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

        static PersistentPublication create(
            final AeronArchive aeronArchive,
            final ExclusivePublication publication
        )
        {
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

        static PersistentPublication resume(
            final AeronArchive aeronArchive,
            final String channel,
            final int streamId,
            final long recordingId)
        {
            final ChannelUriStringBuilder channelUriBuilder = new ChannelUriStringBuilder(channel);
            aeronArchive.listRecording(recordingId,
                (controlSessionId,
                    correlationId,
                    recordingId1,
                    startTimestamp,
                    stopTimestamp,
                    startPosition,
                    stopPosition,
                    initialTermId,
                    segmentFileLength,
                    termBufferLength,
                    mtuLength,
                    sessionId,
                    streamId1,
                    strippedChannel,
                    originalChannel,
                    sourceIdentity) ->
                    channelUriBuilder.initialPosition(stopPosition, initialTermId, termBufferLength
                    )
            );

            final Aeron aeron = aeronArchive.context().aeron();
            final String channelUri = channelUriBuilder.build();
            final ExclusivePublication publication = aeron.addExclusivePublication(channelUri, streamId);
            aeronArchive.extendRecording(recordingId, channelUri, streamId, SourceLocation.LOCAL);
            final CountersReader countersReader = aeron.countersReader();

            final int recordingCounterId =
                Tests.awaitRecordingCounterId(countersReader, publication.sessionId(), aeronArchive.archiveId());
            final long recordingIdFromCounter = RecordingPos.getRecordingId(countersReader, recordingCounterId);
            assert recordingId == recordingIdFromCounter;

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
            final long result = publication.offer(buffer, offset, length);
            if (result > 0)
            {
                publishedMessageCount++;
            }
            return result;
        }

        void persist(final List<byte[]> messages)
        {
            if (messages.isEmpty())
            {
                return;
            }
            final long position = publish(messages);
            Tests.awaitPosition(countersReader, recordingCounterId, position);
        }

        long publish(final List<byte[]> messages)
        {
            final UnsafeBuffer wrapper = new UnsafeBuffer();

            long position = publication.position();
            for (final byte[] message : messages)
            {
                wrapper.wrap(message);
                while ((position = publication.offer(wrapper)) < 0)
                {
                    Tests.yieldingIdle("failed to offer due to " + Publication.errorString(position));
                }
            }
            publishedMessageCount += messages.size();
            return position;
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

        int publishedMessageCount()
        {
            return publishedMessageCount;
        }

        boolean publicationCountersExist()
        {
            final int receiversCounterId = countersReader.findByTypeIdAndRegistrationId(
                DRIVER_PUBLISHER_POS_TYPE_ID, publication.registrationId());
            return receiversCounterId != NULL_COUNTER_ID;
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

        public void close()
        {
            aeronArchive.stopRecording(publication);
            CloseHelper.close(publication);
        }

        void closePublicationOnly()
        {
            CloseHelper.close(publication);
        }
    }

    abstract int poll(
        PersistentSubscription persistentSubscription,
        FragmentConsumer fragmentConsumer,
        int fragmentLimit
    );

    static class ControlledPollingPersistentSubscriptionTest extends PersistentSubscriptionTest
    {
        int poll(
            final PersistentSubscription persistentSubscription,
            final FragmentConsumer fragmentConsumer,
            final int fragmentLimit)
        {
            final ControlledFragmentHandler fragmentHandler2 = fragmentConsumer == null ?
                null :
                fragmentConsumer::onFragmentControlled;
            return persistentSubscription.controlledPoll(fragmentHandler2, fragmentLimit);
        }
    }

    static class UncontrolledPollingPersistentSubscriptionTest extends PersistentSubscriptionTest
    {
        int poll(
            final PersistentSubscription persistentSubscription,
            final FragmentConsumer fragmentConsumer,
            final int fragmentLimit)
        {
            final FragmentHandler fragmentHandler = fragmentConsumer == null ?
                null :
                fragmentConsumer::onFragmentUncontrolled;
            return persistentSubscription.poll(fragmentHandler, fragmentLimit);
        }
    }
}
