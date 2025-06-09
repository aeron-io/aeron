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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.client.ReplayParams;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.samples.archive.RecordingDescriptor;
import io.aeron.samples.archive.RecordingDescriptorCollector;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.collections.Hashing;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.LongArrayList;
import org.agrona.collections.LongHashSet;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.ArchiveSystemTests.CATALOG_CAPACITY;
import static io.aeron.archive.ArchiveSystemTests.RecordingResult;
import static io.aeron.archive.ArchiveSystemTests.TERM_LENGTH;
import static io.aeron.archive.ArchiveSystemTests.awaitSignal;
import static io.aeron.archive.ArchiveSystemTests.consume;
import static io.aeron.archive.ArchiveSystemTests.injectRecordingSignalConsumer;
import static io.aeron.archive.ArchiveSystemTests.offer;
import static io.aeron.archive.ArchiveSystemTests.recordData;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class BasicArchiveTest
{
    private static final String RECORDED_CHANNEL_ALIAS = "named-log";
    private static final int RECORDED_STREAM_ID = 33;
    private static final String RECORDED_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:3333")
        .termLength(ArchiveSystemTests.TERM_LENGTH)
        .alias(RECORDED_CHANNEL_ALIAS)
        .build();

    private static final int REPLAY_STREAM_ID = 66;
    private static final String REPLAY_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:6666")
        .build();

    private TestMediaDriver driver;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive aeronArchive;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private File archiveDir;
    private TestRecordingSignalConsumer recordingSignalConsumer;

    @BeforeEach
    public void before()
    {
        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .spiesSimulateConnection(false)
            .dirDeleteOnStart(true);

        archiveDir = new File(SystemUtil.tmpDirName(), "archive");

        final Archive.Context archiveCtx = TestContexts.localhostArchive()
            .catalogCapacity(CATALOG_CAPACITY)
            .segmentFileLength(TERM_LENGTH)
            .aeronDirectoryName(aeronDirectoryName)
            .deleteArchiveOnStart(true)
            .archiveDir(archiveDir)
            .fileSyncLevel(0)
            .maxConcurrentRecordings(50)
            .threadingMode(ArchiveThreadingMode.DEDICATED); // testing concurrent operations

        driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());
        archive = Archive.launch(archiveCtx);
        systemTestWatcher.dataCollector().add(archiveCtx.archiveDir());

        aeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(aeronDirectoryName));

        aeronArchive = AeronArchive.connect(
            TestContexts.localhostAeronArchive()
                .aeron(aeron));

        recordingSignalConsumer = injectRecordingSignalConsumer(aeronArchive);
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeronArchive, aeron, archive, driver);
    }

    @Test
    @InterruptAfter(10)
    void shouldRecordThenReplayThenTruncate()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long stopPosition;

        final long subscriptionId = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);
        final long recordingIdFromCounter;
        final int sessionId;

        try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            sessionId = publication.sessionId();

            final CountersReader counters = aeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(counters, sessionId, aeronArchive.archiveId());
            recordingIdFromCounter = RecordingPos.getRecordingId(counters, counterId);

            assertEquals(CommonContext.IPC_CHANNEL, RecordingPos.getSourceIdentity(counters, counterId));

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            stopPosition = publication.position();
            Tests.awaitPosition(counters, counterId, stopPosition);

            final long joinPosition = subscription.imageBySessionId(sessionId).joinPosition();
            assertEquals(joinPosition, aeronArchive.getStartPosition(recordingIdFromCounter));
            assertEquals(stopPosition, aeronArchive.getRecordingPosition(recordingIdFromCounter));
            assertEquals(NULL_VALUE, aeronArchive.getStopPosition(recordingIdFromCounter));
            assertEquals(stopPosition, aeronArchive.getMaxRecordedPosition(recordingIdFromCounter));
        }

        aeronArchive.stopRecording(subscriptionId);
        Tests.await(() -> stopPosition == aeronArchive.getStopPosition(recordingIdFromCounter));

        final long recordingId = aeronArchive.findLastMatchingRecording(
            0, "alias=" + RECORDED_CHANNEL_ALIAS, RECORDED_STREAM_ID, sessionId);

        assertFalse(aeronArchive.tryStopRecordingByIdentity(recordingId));

        final long position = 0L;
        final long length = stopPosition - position;

        try (Subscription subscription = aeronArchive.replay(
            recordingId, position, length, REPLAY_CHANNEL, REPLAY_STREAM_ID))
        {
            consume(subscription, messageCount, messagePrefix);
            assertEquals(stopPosition, subscription.imageAtIndex(0).position());
        }

        aeronArchive.truncateRecording(recordingId, position);

        final int count = aeronArchive.listRecording(
            recordingId,
            (controlSessionId,
            correlationId,
            recordingId1,
            startTimestamp,
            stopTimestamp,
            startPosition,
            newStopPosition,
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId1,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity) -> assertEquals(startPosition, newStopPosition));

        assertEquals(1, count);
    }

    @Test
    @InterruptAfter(10)
    void jumboRecordingDescriptorEndToEndTest()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long stopPosition;

        final String recordedChannel = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("localhost:3333")
            .termLength(TERM_LENGTH)
            .alias(Tests.generateStringWithSuffix("alias", "X", 2000))
            .build();

        final long subscriptionId = aeronArchive.startRecording(recordedChannel, RECORDED_STREAM_ID, LOCAL);
        final long recordingId;
        final int sessionId;

        try (Subscription subscription = aeron.addSubscription(recordedChannel, RECORDED_STREAM_ID);
            Publication publication = aeron.addPublication(recordedChannel, RECORDED_STREAM_ID))
        {
            sessionId = publication.sessionId();

            final CountersReader counters = aeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(counters, sessionId, aeronArchive.archiveId());
            recordingId = RecordingPos.getRecordingId(counters, counterId);

            assertEquals(CommonContext.IPC_CHANNEL, RecordingPos.getSourceIdentity(counters, counterId));

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            stopPosition = publication.position();
            Tests.awaitPosition(counters, counterId, stopPosition);

            final long joinPosition = subscription.imageBySessionId(sessionId).joinPosition();
            assertEquals(joinPosition, aeronArchive.getStartPosition(recordingId));
            assertEquals(stopPosition, aeronArchive.getRecordingPosition(recordingId));
            assertEquals(NULL_VALUE, aeronArchive.getStopPosition(recordingId));
            assertEquals(stopPosition, aeronArchive.getMaxRecordedPosition(recordingId));
        }

        aeronArchive.stopRecording(subscriptionId);

        final int count = aeronArchive.listRecording(
            recordingId,
            (controlSessionId,
            correlationId,
            recordingId1,
            startTimestamp,
            stopTimestamp,
            startPosition,
            newStopPosition,
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId1,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity) -> assertEquals(recordedChannel, originalChannel));

        assertEquals(1, count);
    }

    @Test
    @InterruptAfter(10)
    void purgeRecording()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long stopPosition;

        final long subscriptionId = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);
        final long recordingId;
        final int sessionId;

        try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            sessionId = publication.sessionId();

            final CountersReader counters = aeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(counters, sessionId, aeronArchive.archiveId());
            recordingId = RecordingPos.getRecordingId(counters, counterId);

            assertEquals(CommonContext.IPC_CHANNEL, RecordingPos.getSourceIdentity(counters, counterId));

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            stopPosition = publication.position();
            Tests.awaitPosition(counters, counterId, stopPosition);

            final long joinPosition = subscription.imageBySessionId(sessionId).joinPosition();
            assertEquals(joinPosition, aeronArchive.getStartPosition(recordingId));
            assertEquals(stopPosition, aeronArchive.getRecordingPosition(recordingId));
            assertEquals(NULL_VALUE, aeronArchive.getStopPosition(recordingId));
            assertEquals(stopPosition, aeronArchive.getMaxRecordedPosition(recordingId));
        }

        aeronArchive.stopRecording(subscriptionId);
        Tests.await(() -> stopPosition == aeronArchive.getStopPosition(recordingId));

        assertEquals(recordingId, aeronArchive.findLastMatchingRecording(
            0, "alias=" + RECORDED_CHANNEL_ALIAS, RECORDED_STREAM_ID, sessionId));

        assertFalse(aeronArchive.tryStopRecordingByIdentity(recordingId));

        final ArrayList<String> segmentFiles = Catalog.listSegmentFiles(archiveDir, recordingId);

        assertNotEquals(0, segmentFiles.size());

        aeronArchive.purgeRecording(recordingId);

        final int count = aeronArchive.listRecording(recordingId, new FailingRecordingDescriptorConsumer());

        assertEquals(0, count);
        Tests.await(() -> Catalog.listSegmentFiles(archiveDir, recordingId).isEmpty());

        for (final String segmentFile : segmentFiles)
        {
            assertFalse(new File(archiveDir, segmentFile).exists());
        }
    }

    @Test
    @InterruptAfter(10)
    void purgeRecordingFailsIfRecordingIsActive()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long stopPosition;

        final long subscriptionId = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);
        try
        {
            final long recordingIdFromCounter;
            final int sessionId;

            try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
                Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
            {
                sessionId = publication.sessionId();

                final CountersReader counters = aeron.countersReader();
                final int counterId = Tests.awaitRecordingCounterId(counters, sessionId, aeronArchive.archiveId());
                recordingIdFromCounter = RecordingPos.getRecordingId(counters, counterId);

                assertEquals(CommonContext.IPC_CHANNEL, RecordingPos.getSourceIdentity(counters, counterId));

                offer(publication, messageCount, messagePrefix);
                consume(subscription, messageCount, messagePrefix);

                stopPosition = publication.position();
                Tests.awaitPosition(counters, counterId, stopPosition);

                final long joinPosition = subscription.imageBySessionId(sessionId).joinPosition();
                assertEquals(joinPosition, aeronArchive.getStartPosition(recordingIdFromCounter));
                assertEquals(stopPosition, aeronArchive.getRecordingPosition(recordingIdFromCounter));
                assertEquals(NULL_VALUE, aeronArchive.getStopPosition(recordingIdFromCounter));
                assertEquals(stopPosition, aeronArchive.getMaxRecordedPosition(recordingIdFromCounter));

                final long recordingId = aeronArchive.findLastMatchingRecording(
                    0, "alias=" + RECORDED_CHANNEL_ALIAS, RECORDED_STREAM_ID, sessionId);
                assertEquals(recordingIdFromCounter, recordingId);

                final ArchiveException exception = assertThrows(
                    ArchiveException.class, () -> aeronArchive.purgeRecording(recordingId));
                assertThat(exception.getMessage(), endsWith("error: cannot purge active recording " + recordingId));

                final ArrayList<String> segmentFiles = Catalog.listSegmentFiles(archiveDir, recordingId);
                assertNotEquals(0, segmentFiles.size());

                for (final String segmentFile : segmentFiles)
                {
                    assertTrue(new File(archiveDir, segmentFile).exists());
                }
            }
        }
        finally
        {
            aeronArchive.stopRecording(subscriptionId);
        }
    }

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("try")
    void purgeRecordingFailsIfThereAreActiveReplays()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long stopPosition;

        final long subscriptionId = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);
        final long recordingId;
        final int sessionId;

        try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            sessionId = publication.sessionId();

            final CountersReader counters = aeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(counters, sessionId, aeronArchive.archiveId());
            recordingId = RecordingPos.getRecordingId(counters, counterId);

            assertEquals(CommonContext.IPC_CHANNEL, RecordingPos.getSourceIdentity(counters, counterId));

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            stopPosition = publication.position();
            Tests.awaitPosition(counters, counterId, stopPosition);

            final long joinPosition = subscription.imageBySessionId(sessionId).joinPosition();
            assertEquals(joinPosition, aeronArchive.getStartPosition(recordingId));
            assertEquals(stopPosition, aeronArchive.getRecordingPosition(recordingId));
            assertEquals(NULL_VALUE, aeronArchive.getStopPosition(recordingId));
            assertEquals(stopPosition, aeronArchive.getMaxRecordedPosition(recordingId));
        }

        aeronArchive.stopRecording(subscriptionId);
        Tests.await(() -> stopPosition == aeronArchive.getStopPosition(recordingId));

        assertEquals(recordingId, aeronArchive.findLastMatchingRecording(
            0, "alias=" + RECORDED_CHANNEL_ALIAS, RECORDED_STREAM_ID, sessionId));

        final long position = 0L;
        final long length = stopPosition - position;

        try (Subscription ignore = aeronArchive.replay(
            recordingId, position, length, REPLAY_CHANNEL, REPLAY_STREAM_ID))
        {
            final ArchiveException exception = assertThrows(
                ArchiveException.class, () -> aeronArchive.purgeRecording(recordingId));
            assertThat(exception.getMessage(),
                endsWith("error: cannot purge recording with active replay " + recordingId));

            final ArrayList<String> segmentFiles = Catalog.listSegmentFiles(archiveDir, recordingId);
            assertNotEquals(0, segmentFiles.size());

            for (final String segmentFile : segmentFiles)
            {
                assertTrue(new File(archiveDir, segmentFile).exists());
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldRecordReplayAndCancelReplayEarly()
    {
        final String messagePrefix = "Message-Prefix-";
        final long stopPosition;
        final int messageCount = 10;
        final long recordingId;

        try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeronArchive.addRecordedPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
            recordingId = RecordingPos.getRecordingId(counters, counterId);

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            stopPosition = publication.position();
            Tests.awaitPosition(counters, counterId, stopPosition);

            assertEquals(stopPosition, aeronArchive.getRecordingPosition(recordingId));

            assertTrue(aeronArchive.tryStopRecordingByIdentity(recordingId));

            while (NULL_POSITION != aeronArchive.getRecordingPosition(recordingId))
            {
                Tests.yield();
            }
        }

        final long position = 0L;
        final long length = stopPosition - position;

        final long replaySessionId = aeronArchive.startReplay(
            recordingId, position, length, REPLAY_CHANNEL, REPLAY_STREAM_ID);

        aeronArchive.stopReplay(replaySessionId);
    }

    @Test
    @InterruptAfter(10)
    void shouldReplayRecordingFromLateJoinPosition()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;

        final long subscriptionId = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);

        try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            final long currentPosition = publication.position();
            Tests.awaitPosition(counters, counterId, currentPosition);

            try (Subscription replaySubscription = aeronArchive.replay(
                recordingId, currentPosition, AeronArchive.NULL_LENGTH, REPLAY_CHANNEL, REPLAY_STREAM_ID))
            {
                offer(publication, messageCount, messagePrefix);
                consume(subscription, messageCount, messagePrefix);
                consume(replaySubscription, messageCount, messagePrefix);

                final long endPosition = publication.position();
                assertEquals(endPosition, replaySubscription.imageAtIndex(0).position());
            }
        }

        aeronArchive.stopRecording(subscriptionId);
    }

    @Test
    @InterruptAfter(10)
    void shouldFindLastMatchingRecordingIdWithFullUri()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;

        final long subscriptionId = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);

        try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            final long currentPosition = publication.position();
            Tests.awaitPosition(counters, counterId, currentPosition);

            final long lastMatchingRecording = aeronArchive.findLastMatchingRecording(
                0, RECORDED_CHANNEL, RECORDED_STREAM_ID, publication.sessionId());

            assertEquals(lastMatchingRecording, recordingId);
        }

        aeronArchive.stopRecording(subscriptionId);
    }

    @Test
    @InterruptAfter(10)
    void shouldReturnNullValueWithFindLastMatchingRecordingIdDoesNotFindTheRecording()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;

        final long subscriptionId = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);

        try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);
            assertNotEquals(RecordingPos.NULL_RECORDING_ID, recordingId);

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            final long currentPosition = publication.position();
            Tests.awaitPosition(counters, counterId, currentPosition);

            final long lastMatchingRecording = aeronArchive.findLastMatchingRecording(
                0, RECORDED_CHANNEL, RECORDED_STREAM_ID, publication.sessionId() + 1);

            assertEquals(Aeron.NULL_VALUE, lastMatchingRecording);
        }

        aeronArchive.stopRecording(subscriptionId);
    }

    @SuppressWarnings("checkstyle:MethodLength")
    @Test
    @SlowTest
    @InterruptAfter(20)
    public void shouldRecordThenBoundReplayWithCounter()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 100;
        final long stopPosition;
        final int timeout = 3_000;

        final long subscriptionId = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);
        final long recordingIdFromCounter;
        final int sessionId;

        try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            sessionId = publication.sessionId();

            final CountersReader counters = aeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(counters, sessionId, aeronArchive.archiveId());
            recordingIdFromCounter = RecordingPos.getRecordingId(counters, counterId);

            assertEquals(CommonContext.IPC_CHANNEL, RecordingPos.getSourceIdentity(counters, counterId));

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            stopPosition = publication.position();
            Tests.awaitPosition(counters, counterId, stopPosition);

            final long joinPosition = subscription.imageBySessionId(sessionId).joinPosition();
            assertEquals(joinPosition, aeronArchive.getStartPosition(recordingIdFromCounter));
            assertEquals(stopPosition, aeronArchive.getRecordingPosition(recordingIdFromCounter));
            assertEquals(NULL_VALUE, aeronArchive.getStopPosition(recordingIdFromCounter));
            assertEquals(stopPosition, aeronArchive.getMaxRecordedPosition(recordingIdFromCounter));
        }

        recordingSignalConsumer.reset();
        aeronArchive.stopRecording(subscriptionId);
        awaitSignal(aeronArchive, recordingSignalConsumer, recordingIdFromCounter, RecordingSignal.STOP);

        final long recordingId = aeronArchive.findLastMatchingRecording(
            0, "alias=" + RECORDED_CHANNEL_ALIAS, RECORDED_STREAM_ID, sessionId);
        assertEquals(recordingIdFromCounter, recordingId);

        final Counter boundingCounter = aeron.addCounter(10000, "bounding counter");

        final RecordingDescriptorCollector recordingDescriptorCollector = new RecordingDescriptorCollector(1);
        assertEquals(1, aeronArchive.listRecording(recordingId, recordingDescriptorCollector.reset()));
        final RecordingDescriptor recordingDescriptor = recordingDescriptorCollector.descriptors().get(0);

        assertNotEquals(NULL_VALUE, recordingDescriptor.stopPosition());
        final long halfLength = (recordingDescriptor.stopPosition() - recordingDescriptor.startPosition()) / 2;
        final long halfPosition = recordingDescriptor.startPosition() + halfLength;
        final long tqPosition = recordingDescriptor.startPosition() + halfLength + (halfLength / 2);

        boundingCounter.setRelease(0);

        final ReplayParams replayParams = new ReplayParams()
            .position(recordingDescriptor.startPosition())
            .length(Long.MAX_VALUE)
            .boundingLimitCounterId(boundingCounter.id());

        final long replaySessionId = aeronArchive.startReplay(
            recordingId, REPLAY_CHANNEL, REPLAY_STREAM_ID, replayParams);

        final String channel = new ChannelUriStringBuilder(REPLAY_CHANNEL).sessionId((int)replaySessionId).build();

        final AtomicReference<Image> replayImage = new AtomicReference<>();
        try (Subscription replaySubscription = aeron.addSubscription(
            channel, REPLAY_STREAM_ID, replayImage::set, image -> {}))
        {
            boundingCounter.setRelease(halfPosition);

            while (null == replayImage.get())
            {
                Tests.yieldingIdle("replay image did not become available");
            }

            final Supplier<String> halfErrorMessage =
                () -> "replayImage.position(" + replayImage.get().position() + ") < halfPosition(" + halfPosition + ")";

            while (replayImage.get().position() < halfPosition)
            {
                if (0 == replaySubscription.poll((buffer, offset, length, header) -> {}, 20))
                {
                    Tests.yieldingIdle(halfErrorMessage);
                }
            }

            final long halfPollDeadline = System.currentTimeMillis() + timeout;
            while (System.currentTimeMillis() < halfPollDeadline)
            {
                replaySubscription.poll((buffer, offset, length, header) -> {}, 20);
                assertThat(replayImage.get().position(), lessThanOrEqualTo(halfPosition));
            }

            boundingCounter.setRelease(tqPosition);

            final Supplier<String> tqErrorMessage =
                () -> "replayImage.position(" + replayImage.get().position() + ") < tqPosition(" + tqPosition + ")";

            while (replayImage.get().position() < tqPosition)
            {
                if (0 == replaySubscription.poll((buffer, offset, length, header) -> {}, 20))
                {
                    Tests.yieldingIdle(tqErrorMessage);
                }
            }

            final long tqPollDeadline = System.currentTimeMillis() + timeout;
            while (System.currentTimeMillis() < tqPollDeadline)
            {
                replaySubscription.poll((buffer, offset, length, header) -> {}, 20);
                assertThat(replayImage.get().position(), lessThanOrEqualTo(tqPosition));
            }
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldErrorReplayFileIoMaxLengthLessThanMtu()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 100;
        final long stopPosition;

        final long subscriptionId = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);
        final long recordingIdFromCounter;
        final int sessionId;

        try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            sessionId = publication.sessionId();

            final CountersReader counters = aeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(counters, sessionId, aeronArchive.archiveId());
            recordingIdFromCounter = RecordingPos.getRecordingId(counters, counterId);

            assertEquals(CommonContext.IPC_CHANNEL, RecordingPos.getSourceIdentity(counters, counterId));

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            stopPosition = publication.position();
            Tests.awaitPosition(counters, counterId, stopPosition);
        }

        aeronArchive.stopRecording(subscriptionId);

        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(1);
        assertEquals(1, aeronArchive.listRecording(recordingIdFromCounter, collector.reset()));

        final int invalidFileIoMaxLength = collector.descriptors().get(0).mtuLength() - 1;

        final long correlationId = aeron.nextCorrelationId();
        assertTrue(aeronArchive.archiveProxy().replay(
            recordingIdFromCounter,
            REPLAY_CHANNEL,
            REPLAY_STREAM_ID,
            new ReplayParams().fileIoMaxLength(invalidFileIoMaxLength),
            correlationId,
            aeronArchive.controlSessionId()));

        String error;
        while (null == (error = aeronArchive.pollForErrorResponse()))
        {
            Tests.yieldingIdle("Error not reported");
        }

        assertThat(error, containsString("mtuLength"));
        assertThat(error, containsString("fileIoMaxLength"));
    }

    @Test
    void shouldNotListRecordingThatWasPurged()
    {
        final RecordingResult recording1 = recordData(aeronArchive);
        final RecordingResult recording2 = recordData(aeronArchive);
        final RecordingResult recording3 = recordData(aeronArchive);

        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(3);
        assertEquals(3, aeronArchive.listRecordings(NULL_VALUE, 100, collector.reset()));
        assertEquals(recording1.recordingId(), collector.descriptors().get(0).recordingId());
        assertEquals(recording2.recordingId(), collector.descriptors().get(1).recordingId());
        assertEquals(recording3.recordingId(), collector.descriptors().get(2).recordingId());

        final RecordingDescriptor descriptor = collector.descriptors().get(0);
        final ChannelUri channelUri = ChannelUri.parse(descriptor.originalChannel());
        channelUri.remove(CommonContext.SESSION_ID_PARAM_NAME);
        final int streamId = descriptor.streamId();
        assertEquals(3, aeronArchive.listRecordingsForUri(
            NULL_VALUE, 100, channelUri.toString(), streamId, collector.reset()));
        assertEquals(recording1.recordingId(), collector.descriptors().get(0).recordingId());
        assertEquals(recording2.recordingId(), collector.descriptors().get(1).recordingId());
        assertEquals(recording3.recordingId(), collector.descriptors().get(2).recordingId());

        assertEquals(1, aeronArchive.listRecording(recording1.recordingId(), collector.reset()));
        assertEquals(recording1.recordingId(), collector.descriptors().get(0).recordingId());

        assertEquals(1, aeronArchive.listRecording(recording2.recordingId(), collector.reset()));
        assertEquals(recording2.recordingId(), collector.descriptors().get(0).recordingId());

        assertEquals(1, aeronArchive.listRecording(recording3.recordingId(), collector.reset()));
        assertEquals(recording3.recordingId(), collector.descriptors().get(0).recordingId());

        assertNotEquals(0, aeronArchive.purgeRecording(recording2.recordingId()));

        assertEquals(1, aeronArchive.listRecording(recording1.recordingId(), collector.reset()));
        assertEquals(recording1.recordingId(), collector.descriptors().get(0).recordingId());

        assertEquals(0, aeronArchive.listRecording(recording2.recordingId(), collector.reset()));
        assertThat(collector.descriptors(), hasSize(0));

        assertEquals(1, aeronArchive.listRecording(recording3.recordingId(), collector.reset()));
        assertEquals(recording3.recordingId(), collector.descriptors().get(0).recordingId());

        assertEquals(2, aeronArchive.listRecordings(NULL_VALUE, 100, collector.reset()));
        assertEquals(recording1.recordingId(), collector.descriptors().get(0).recordingId());
        assertEquals(recording3.recordingId(), collector.descriptors().get(1).recordingId());

        assertEquals(2, aeronArchive.listRecordingsForUri(
            NULL_VALUE, 100, channelUri.toString(), streamId, collector.reset()));
        assertEquals(recording1.recordingId(), collector.descriptors().get(0).recordingId());
        assertEquals(recording3.recordingId(), collector.descriptors().get(1).recordingId());
    }

    @Test
    @InterruptAfter(20)
    void shakeListingAndPurgingRecordings()
    {
        final int recordingCount = 1000;

        final String channel = "aeron:ipc?term-length=64k";
        final Catalog catalog = archive.context().catalog();

        for (int i = 0; i < recordingCount; i++)
        {
            final long startTimestamp = System.currentTimeMillis();
            final long startPosition = (long)Math.pow(2, ThreadLocalRandom.current().nextInt(10, 30));
            catalog.addNewRecording(
                startPosition,
                startPosition + startPosition * 10,
                startTimestamp,
                startTimestamp + i * 1_000,
                ThreadLocalRandom.current().nextInt(),
                1024 * 1024,
                LogBufferDescriptor.TERM_MIN_LENGTH,
                1408,
                ThreadLocalRandom.current().nextInt(),
                10000 + i,
                channel,
                channel,
                channel);
        }

        final LongArrayList existingRecordingIds = new LongArrayList(recordingCount, NULL_VALUE);
        final Long2LongHashMap recordingIdToStreamId =
            new Long2LongHashMap(recordingCount, Hashing.DEFAULT_LOAD_FACTOR, NULL_VALUE);
        int count = aeronArchive.listRecordings(
            0,
            Integer.MAX_VALUE,
            (controlSessionId,
            correlationId,
            recordingId,
            startTimestamp,
            stopTimestamp,
            startPosition,
            newStopPosition,
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity) ->
            {
                existingRecordingIds.addLong(recordingId);
                recordingIdToStreamId.put(recordingId, streamId);
            });

        final Supplier<String> details = () -> existingRecordingIds + " " + recordingIdToStreamId;
        assertEquals(recordingCount, count, details);
        assertEquals(recordingCount, existingRecordingIds.size(), details);
        assertEquals(recordingCount, recordingIdToStreamId.size(), details);

        final FailingRecordingDescriptorConsumer failingRecordingDescriptorConsumer =
            new FailingRecordingDescriptorConsumer();
        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(recordingCount);
        final long seed = ThreadLocalRandom.current().nextLong();
        final Random random = new Random(seed);
        try
        {
            for (int i = 0; i < 20; i++)
            {
                final int victimIndex = random.nextInt(existingRecordingIds.size());
                final long recordingId = existingRecordingIds.removeAt(victimIndex);

                aeronArchive.purgeRecording(recordingId);

                count = aeronArchive.listRecording(recordingId, failingRecordingDescriptorConsumer);
                assertEquals(0, count);

                final int fromIndex = random.nextInt(existingRecordingIds.size());
                final long fromRecordingId = existingRecordingIds.get(fromIndex);
                final int recordCount = random.nextInt(existingRecordingIds.size() + 1) + 1;
                final int expectedCount = Math.min(recordCount, existingRecordingIds.size() - fromIndex);

                count = aeronArchive.listRecordings(fromRecordingId, recordCount, collector.reset());
                assertEquals(expectedCount, count);

                final LongHashSet foundRecordingIds = new LongHashSet();
                for (final RecordingDescriptor descriptor : collector.descriptors())
                {
                    final long recId = descriptor.recordingId();
                    assertEquals(recordingIdToStreamId.get(recId), descriptor.streamId());
                    assertTrue(existingRecordingIds.contains(recId));
                    foundRecordingIds.add(recId);
                }
                assertEquals(expectedCount, foundRecordingIds.size());
            }
        }
        catch (final Exception e)
        {
            fail("seed=" + seed, e);
        }
    }

    @Test
    @InterruptAfter(20)
    void shouldStopReplayWithoutConsumingAnEntireRecording()
    {
        testStopReplay(aeronArchive::stopReplay);
    }

    @Test
    @InterruptAfter(20)
    void shouldStopAllReplaysWithoutConsumingAnEntireRecording()
    {
        testStopReplay((replaySessionId) -> aeronArchive.stopAllReplays(RecordingPos.NULL_RECORDING_ID));
    }

    private void testStopReplay(final LongConsumer stopCommand)
    {
        final long recordingId;
        final long stopPosition;
        try (ExclusivePublication publication =
            aeronArchive.addRecordedExclusivePublication("aeron:ipc?term-length=64K", 555))
        {
            final int counterId = Tests.awaitRecordingCounterId(
                aeron.countersReader(), publication.sessionId(), aeronArchive.archiveId());

            offer(publication, 10000, "test-");

            stopPosition = publication.position();
            Tests.awaitPosition(aeron.countersReader(), counterId, stopPosition);
            recordingId = RecordingPos.getRecordingId(aeron.countersReader(), counterId);
        }

        final String replayChannel = "aeron:udp?endpoint=localhost:17171";
        final int replayStreamId = 777;
        final long replaySessionId =
            aeronArchive.startReplay(recordingId, 0, Long.MAX_VALUE, replayChannel, replayStreamId);
        final AtomicBoolean imageUnavailable = new AtomicBoolean(false);
        final Subscription subscription = aeron.addSubscription(
            ChannelUri.addSessionId(replayChannel, (int)replaySessionId),
            replayStreamId,
            image -> {},
            image -> imageUnavailable.set(true));
        Tests.awaitConnected(subscription);
        final Image image = subscription.imageAtIndex(0);
        final MutableInteger counter = new MutableInteger();
        final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> counter.increment();
        while (counter.get() < 5)
        {
            if (0 == image.poll(fragmentHandler, 1))
            {
                Tests.yield();
            }
        }

        stopCommand.accept(replaySessionId);

        final long startTimeNs = System.nanoTime();
        Tests.await(imageUnavailable::get);
        final long endTimeNs = System.nanoTime();
        assertThat(endTimeNs - startTimeNs, lessThan(driver.context().imageLivenessTimeoutNs()));

        subscription.close();
    }

    private static final class FailingRecordingDescriptorConsumer implements RecordingDescriptorConsumer
    {
        public void onRecordingDescriptor(
            final long controlSessionId,
            final long correlationId,
            final long recordingId,
            final long startTimestamp,
            final long stopTimestamp,
            final long startPosition,
            final long stopPosition,
            final int initialTermId,
            final int segmentFileLength,
            final int termBufferLength,
            final int mtuLength,
            final int sessionId,
            final int streamId,
            final String strippedChannel,
            final String originalChannel,
            final String sourceIdentity)
        {
            fail("unexpected recording " + recordingId);
        }
    }
}
