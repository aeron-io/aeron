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
import io.aeron.Counter;
import io.aeron.ChannelUri;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.checksum.Checksum;
import io.aeron.archive.checksum.Checksums;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ReplayParams;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.archive.Archive.Configuration.CATALOG_FILE_NAME;
import static io.aeron.archive.Archive.Configuration.RECORDING_SEGMENT_SUFFIX;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.ArchiveSystemTests.*;
import static io.aeron.archive.client.AeronArchive.segmentFileBasePosition;
import static java.util.Arrays.asList;
import static java.util.Arrays.copyOfRange;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class ArchiveTruncateRecordingTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();
    private final FragmentAssembler fragmentHandler = new FragmentAssembler((buffer, offset, length, header) -> {});
    private TestMediaDriver driver;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive aeronArchive;
    private TestRecordingSignalConsumer recordingSignalConsumer;

    @BeforeEach
    void before(final @TempDir Path tempDir)
    {
        final Checksum checksum = Checksums.crc32c();

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(YieldingIdleStrategy.INSTANCE)
            .spiesSimulateConnection(true)
            .dirDeleteOnStart(true)
            .aeronDirectoryName(tempDir.resolve("aeron-driver").toString());

        final Archive.Context archiveContext = TestContexts.localhostArchive()
            .catalogCapacity(CATALOG_CAPACITY)
            .deleteArchiveOnStart(true)
            .aeronDirectoryName(driverCtx.aeronDirectoryName())
            .archiveDir(tempDir.resolve("archive-test").toFile())
            .segmentFileLength(LogBufferDescriptor.TERM_MIN_LENGTH * 2)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .recordChecksum(checksum)
            .replayChecksum(checksum)
            .idleStrategySupplier(YieldingIdleStrategy::new);

        driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());

        archive = Archive.launch(archiveContext);
        systemTestWatcher.dataCollector().add(archiveContext.archiveDir());

        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));

        aeronArchive = AeronArchive.connect(TestContexts.localhostAeronArchive().aeron(aeron));
        recordingSignalConsumer = injectRecordingSignalConsumer(aeronArchive);
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeronArchive, aeron, archive, driver);
    }

    @Test
    @InterruptAfter(10)
    void truncateRecordingShouldDeleteAllFilesWhenTruncatingToTheStartOfTheRecording() throws IOException
    {
        final String channel = "aeron:ipc?term-length=64k";
        final int streamId = 3333;
        try (ExclusivePublication publication = aeron.addExclusivePublication(channel, streamId);
            Subscription subscription = aeron.addSubscription(channel, streamId))
        {
            final int messageLength = publication.maxMessageLength();
            final UnsafeBuffer data = new UnsafeBuffer(new byte[messageLength]);
            ThreadLocalRandom.current().nextBytes(data.byteArray());

            sendMessages(publication, subscription, messageLength, data, 99);

            final long subscriptionId = aeronArchive.startRecording(
                ChannelUri.addSessionId(channel, publication.sessionId()), streamId, SourceLocation.LOCAL);

            final CountersReader counters = aeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);
            // The recording joins at the IPC publication's join position, i.e. min(consumerPosition,
            // subscriberPositions). consumerPosition is only advanced on the driver conductor duty cycle, so it can
            // lag the producer position by up to one duty cycle. Capture the actual recording start position rather
            // than assuming it equals publication.position() when startRecording() was called, which is racy (see
            // IpcPublication.joinPosition()).
            final long startPosition = aeronArchive.getStartPosition(recordingId);
            assertTrue(startPosition <= publication.position());
            assertEquals(NULL_VALUE, aeronArchive.getStopPosition(recordingId));

            ThreadLocalRandom.current().nextBytes(data.byteArray());
            sendMessages(publication, subscription, messageLength, data, 42);
            final long stopPosition = publication.position();

            while (stopPosition != aeronArchive.getRecordingPosition(recordingId))
            {
                Tests.yield();
            }

            recordingSignalConsumer.reset();
            aeronArchive.stopRecording(subscriptionId);
            awaitSignal(aeronArchive, recordingSignalConsumer, recordingId, RecordingSignal.STOP);
            assertEquals(startPosition, aeronArchive.getStartPosition(recordingId));
            assertEquals(stopPosition, aeronArchive.getStopPosition(recordingId));

            final Path archiveDir = archive.context().archiveDir().toPath();
            final ArrayList<String> segmentFiles = Catalog.listSegmentFiles(archiveDir.toFile(), recordingId);
            segmentFiles.sort(Comparator.naturalOrder());
            final int termLength = publication.termBufferLength();
            final int segmentFileLength = Math.max(archive.context().segmentFileLength(), termLength);
            final ArrayList<String> expectedSegmentFiles = new ArrayList<>();
            for (long base = segmentFileBasePosition(startPosition, startPosition, termLength, segmentFileLength);
                base < stopPosition;
                base += segmentFileLength)
            {
                expectedSegmentFiles.add(segmentFileName(recordingId, base));
            }
            expectedSegmentFiles.sort(Comparator.naturalOrder());
            assertEquals(expectedSegmentFiles, segmentFiles);

            final ArrayList<Path> deleteList = new ArrayList<>();
            for (final String segmentFileName : segmentFiles)
            {
                deleteList.add(archiveDir.resolve(segmentFileName));
            }
            deleteList.add(Files.createFile(archiveDir.resolve(recordingId + "-0" + RECORDING_SEGMENT_SUFFIX)));
            deleteList.add(Files.createFile(archiveDir.resolve(recordingId + "-1" + RECORDING_SEGMENT_SUFFIX)));
            deleteList.add(Files.createFile(archiveDir.resolve(recordingId + "-1048575" + RECORDING_SEGMENT_SUFFIX)));
            deleteList.add(Files.createFile(archiveDir.resolve(recordingId + "-1048577" + RECORDING_SEGMENT_SUFFIX)));
            deleteList.add(Files.createFile(archiveDir.resolve(recordingId + "-222222222" + RECORDING_SEGMENT_SUFFIX)));
            deleteList.add(Files.createFile(archiveDir.resolve(
                recordingId + "-" + archive.context().segmentFileLength() + RECORDING_SEGMENT_SUFFIX)));

            final Path otherFile = Files.createFile(
                archiveDir.resolve((recordingId + 1) + "-1179648" + RECORDING_SEGMENT_SUFFIX));
            final Path otherFile2 = Files.createFile(archiveDir.resolve("something-else.txt"));

            recordingSignalConsumer.reset();
            aeronArchive.truncateRecording(recordingId, startPosition);
            awaitSignal(aeronArchive, recordingSignalConsumer, recordingId, RecordingSignal.DELETE);

            assertEquals(startPosition, aeronArchive.getStartPosition(recordingId));
            assertEquals(startPosition, aeronArchive.getStopPosition(recordingId));
            assertEquals(Collections.emptyList(), Catalog.listSegmentFiles(archiveDir.toFile(), recordingId));
            for (final Path file : deleteList)
            {
                assertFalse(Files.exists(file));
            }
            assertTrue(Files.exists(otherFile));
            assertTrue(Files.exists(otherFile2));
            assertTrue(Files.exists(archiveDir.resolve(CATALOG_FILE_NAME)));

            verifyRecording(recordingId);
        }
    }

    @Test
    @InterruptAfter(10)
    void truncateRecordingShouldDeleteAllFilesWhenTruncatingToZero() throws IOException
    {
        final String channel = "aeron:ipc?term-length=" + (archive.context().segmentFileLength() * 2);
        final int streamId = 3333;
        try (ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(channel, streamId);
            Subscription subscription = aeron.addSubscription(channel, streamId))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);
            assertEquals(0, aeronArchive.getStartPosition(recordingId));

            final int messageLength = 1600;
            final UnsafeBuffer data = new UnsafeBuffer(new byte[messageLength]);
            ThreadLocalRandom.current().nextBytes(data.byteArray());

            sendMessages(publication, subscription, messageLength, data, 333);
            final long stopPosition = publication.position();

            while (stopPosition != aeronArchive.getRecordingPosition(recordingId))
            {
                Tests.yield();
            }

            recordingSignalConsumer.reset();
            aeronArchive.stopRecording(publication);
            awaitSignal(aeronArchive, recordingSignalConsumer, recordingId, RecordingSignal.STOP);
            assertEquals(0, aeronArchive.getStartPosition(recordingId));
            assertEquals(stopPosition, aeronArchive.getStopPosition(recordingId));

            final Path archiveDir = archive.context().archiveDir().toPath();
            final ArrayList<String> segmentFiles = Catalog.listSegmentFiles(archiveDir.toFile(), recordingId);
            segmentFiles.sort(Comparator.naturalOrder());
            assertEquals(
                asList(recordingId + "-0.rec", recordingId + "-262144.rec", recordingId + "-524288.rec"),
                segmentFiles);

            final ArrayList<Path> deleteList = new ArrayList<>();
            for (final String segmentFileName : segmentFiles)
            {
                final Path file = archiveDir.resolve(segmentFileName);
                final Path renamed = Files.move(file, archiveDir.resolve(segmentFileName + ".del"));
                assertTrue(Files.exists(renamed));
                deleteList.add(renamed);
            }
            deleteList.add(Files.createFile(archiveDir.resolve(recordingId + "-1" + RECORDING_SEGMENT_SUFFIX)));
            deleteList.add(Files.createFile(archiveDir.resolve(
                recordingId + "-2" + RECORDING_SEGMENT_SUFFIX + ".del")));
            deleteList.add(Files.createFile(archiveDir.resolve(recordingId + "-262143" + RECORDING_SEGMENT_SUFFIX)));
            deleteList.add(Files.createFile(archiveDir.resolve(recordingId + "-262145" + RECORDING_SEGMENT_SUFFIX)));
            deleteList.add(Files.createFile(archiveDir.resolve(
                recordingId + "-" + archive.context().segmentFileLength() + RECORDING_SEGMENT_SUFFIX)));

            final Path otherFile = Files.createFile(archiveDir.resolve(
                (recordingId + 1) + "-" + publication.termBufferLength() + RECORDING_SEGMENT_SUFFIX));
            final Path otherFile2 = Files.createFile(archiveDir.resolve("something-else.txt"));
            final Path otherFile3 = Files.createFile(archiveDir.resolve(
                (recordingId + 2) + "-" + archive.context().segmentFileLength() + RECORDING_SEGMENT_SUFFIX + ".del"));

            recordingSignalConsumer.reset();
            aeronArchive.truncateRecording(recordingId, 0);
            awaitSignal(aeronArchive, recordingSignalConsumer, recordingId, RecordingSignal.DELETE);

            assertEquals(0, aeronArchive.getStartPosition(recordingId));
            assertEquals(0, aeronArchive.getStopPosition(recordingId));
            assertEquals(Collections.emptyList(), Catalog.listSegmentFiles(archiveDir.toFile(), recordingId));
            for (final Path file : deleteList)
            {
                assertFalse(Files.exists(file), () -> "not deleted: " + file);
            }
            assertTrue(Files.exists(otherFile));
            assertTrue(Files.exists(otherFile2));
            assertTrue(Files.exists(otherFile3));
            assertTrue(Files.exists(archiveDir.resolve(CATALOG_FILE_NAME)));

            verifyRecording(recordingId);
        }
    }

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("MethodLength")
    void truncateRecordingShouldDeleteSegmentFilesPastPositionAndEraseAlreadyWrittenData() throws IOException
    {
        final String channel = "aeron:ipc?term-length=" + (archive.context().segmentFileLength() * 4);
        final int streamId = 555;
        try (
            ExclusivePublication rec1 = aeronArchive.addRecordedExclusivePublication("aeron:ipc", 2000);
            ExclusivePublication rec2 = aeronArchive.addRecordedExclusivePublication(
                "aeron:udp?endpoint=localhost:5555", 1000);
            ExclusivePublication publication = aeron.addExclusivePublication(channel, streamId);
            Subscription subscription = aeron.addSubscription(channel, streamId))
        {
            final int messageLength = publication.maxMessageLength();
            final UnsafeBuffer data = new UnsafeBuffer(new byte[messageLength]);

            Tests.awaitConnected(rec1);
            Tests.awaitConnected(rec2);
            assertTrue(rec1.offer(data, 0, 10) > 0);
            assertTrue(rec2.offer(data, 0, 99) > 0);
            aeronArchive.stopRecording(rec1);
            aeronArchive.stopRecording(rec2);

            ThreadLocalRandom.current().nextBytes(data.byteArray());
            sendMessages(publication, subscription, messageLength, data, 10);

            aeronArchive.startRecording(
                ChannelUri.addSessionId(channel, publication.sessionId()), streamId, SourceLocation.LOCAL);

            final CountersReader counters = aeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);
            // The recording joins at the IPC publication's join position, i.e. min(consumerPosition,
            // subscriberPositions). consumerPosition is only advanced on the driver conductor duty cycle, so it can
            // lag the producer position by up to one duty cycle relative to the test subscription's last poll.
            // Capture the actual recording start position rather than assuming it equals publication.position() at
            // the time startRecording() was called, which is racy (see IpcPublication.joinPosition()).
            final long startPosition = aeronArchive.getStartPosition(recordingId);
            assertTrue(startPosition <= publication.position());

            ThreadLocalRandom.current().nextBytes(data.byteArray());
            sendMessages(publication, subscription, messageLength, data, 10);
            final long truncatePosition = publication.position();

            ThreadLocalRandom.current().nextBytes(data.byteArray());
            sendMessages(publication, subscription, messageLength, data, 5);
            final long stopPosition = publication.position();

            while (stopPosition != aeronArchive.getRecordingPosition(recordingId))
            {
                Tests.yield();
            }

            recordingSignalConsumer.reset();
            aeronArchive.stopRecording(publication);
            awaitSignal(aeronArchive, recordingSignalConsumer, recordingId, RecordingSignal.STOP);
            assertEquals(startPosition, aeronArchive.getStartPosition(recordingId));
            assertEquals(stopPosition, aeronArchive.getStopPosition(recordingId));

            final Path archiveDir = archive.context().archiveDir().toPath();
            final int termLength = publication.termBufferLength();
            final int segmentFileLength = Math.max(archive.context().segmentFileLength(), termLength);
            final Path startFile = archiveDir.resolve(segmentFileName(recordingId,
                segmentFileBasePosition(startPosition, startPosition, termLength, segmentFileLength)));
            final Path stopFile = archiveDir.resolve(segmentFileName(recordingId,
                segmentFileBasePosition(startPosition, stopPosition, termLength, segmentFileLength)));
            final long truncateFilePosition = segmentFileBasePosition(
                startPosition, truncatePosition, termLength, segmentFileLength);
            final Path truncatedFile = archiveDir.resolve(segmentFileName(recordingId, truncateFilePosition));
            final byte[] startFileData = Files.readAllBytes(startFile);
            final byte[] truncatedFileData = Files.readAllBytes(truncatedFile);

            final ArrayList<String> segmentFiles = Catalog.listSegmentFiles(archiveDir.toFile(), recordingId);
            segmentFiles.sort(Comparator.naturalOrder());
            assertEquals(Arrays.asList(
                truncatedFile.getFileName().toString(),
                stopFile.getFileName().toString(),
                startFile.getFileName().toString()),
                segmentFiles);

            recordingSignalConsumer.reset();
            aeronArchive.truncateRecording(recordingId, truncatePosition);
            awaitSignal(aeronArchive, recordingSignalConsumer, recordingId, RecordingSignal.DELETE);

            assertEquals(startPosition, aeronArchive.getStartPosition(recordingId));
            assertEquals(truncatePosition, aeronArchive.getStopPosition(recordingId));

            // prior to truncate position all files should be unchanged
            assertArrayEquals(startFileData, Files.readAllBytes(startFile));

            // the file with the truncate position must be truncated
            final byte[] afterTruncate = Files.readAllBytes(truncatedFile);
            assertFalse(Arrays.equals(truncatedFileData, afterTruncate));
            final int lengthBeforeTruncationPoint = (int)(truncatePosition - truncateFilePosition);
            assertArrayEquals(
                copyOfRange(truncatedFileData, 0, lengthBeforeTruncationPoint),
                copyOfRange(afterTruncate, 0, lengthBeforeTruncationPoint));
            for (int i = lengthBeforeTruncationPoint; i < segmentFileLength; i++)
            {
                assertEquals(0, afterTruncate[i]);
            }

            // files after truncate position are deleted
            assertFalse(Files.exists(stopFile));

            verifyRecording(recordingId);

            assertTrue(Files.exists(archiveDir.resolve(CATALOG_FILE_NAME)));
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldTruncateRecordingWithConcurrentReplay()
    {
        final ArchiveSystemTests.RecordingResult recordingResult = ArchiveSystemTests.recordData(aeronArchive);

        aeronArchive.startReplay(recordingResult.recordingId(), IPC_CHANNEL, 10001, new ReplayParams());
        aeronArchive.truncateRecording(recordingResult.recordingId(), 0);

        assertEquals(0, aeronArchive.getStopPosition(recordingResult.recordingId()));

        final Counter replaySessionCounter = archive.context().replaySessionCounter();
        while (replaySessionCounter.get() > 0)
        {
            Tests.yield();
        }
    }

    private void sendMessages(
        final ExclusivePublication publication,
        final Subscription subscription,
        final int messageLength,
        final UnsafeBuffer data,
        final int numberOfMessages)
    {
        for (int i = 0; i < numberOfMessages; i++)
        {
            while (publication.offer(data, 0, messageLength) < 0)
            {
                Tests.yield();
            }

            while (0 == subscription.poll(fragmentHandler, Integer.MAX_VALUE))
            {
                Tests.yield();
            }
        }
    }

    private void verifyRecording(final long recordingId)
    {
        final PrintStream out = mock(PrintStream.class);
        assertTrue(ArchiveTool.verifyRecording(
            out,
            archive.context().archiveDir(),
            recordingId,
            EnumSet.allOf(ArchiveTool.VerifyOption.class),
            archive.context().recordChecksum(),
            SystemEpochClock.INSTANCE,
            (file) -> true));

        verify(out).println("(recordingId=" + recordingId + ") OK");
        verifyNoMoreInteractions(out);
    }
}
