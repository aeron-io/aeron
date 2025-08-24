/*
 * Copyright 2014-2023 Real Logic Limited.
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
import io.aeron.CommonContext;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.ReplayParams;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.REPLAY_ALL_AND_FOLLOW;
import static io.aeron.archive.client.AeronArchive.REPLAY_ALL_AND_STOP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith({ InterruptingTestCallback.class, EventLogExtension.class })
public class ArchiveReplayTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher()
        .ignoreErrorsMatching(s -> s.contains("response publication is closed"));

    private TestMediaDriver driver;
    private Archive archive;

    @BeforeEach
    void setUp()
    {
        final int termLength = 64 * 1024;

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .termBufferSparseFile(true)
            .publicationTermBufferLength(termLength)
            .sharedIdleStrategy(YieldingIdleStrategy.INSTANCE)
            .threadingMode(ThreadingMode.SHARED)
            .spiesSimulateConnection(true)
            .dirDeleteOnStart(true);

        final Archive.Context archiveContext = TestContexts.localhostArchive()
            .aeronDirectoryName(driverCtx.aeronDirectoryName())
            .threadingMode(ArchiveThreadingMode.SHARED)
            .controlChannel("aeron:udp?endpoint=localhost:10001")
            .catalogCapacity(ArchiveSystemTests.CATALOG_CAPACITY)
            .fileSyncLevel(0)
            .deleteArchiveOnStart(true)
            .archiveDir(new File(SystemUtil.tmpDirName(), "archive-test"))
            .segmentFileLength(1024 * 1024)
            .idleStrategySupplier(YieldingIdleStrategy::new);

        driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());

        archive = Archive.launch(archiveContext);
        systemTestWatcher.dataCollector().add(archiveContext.archiveDir());
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(archive, driver);
    }

    @Test
    void shouldNotErrorOnReplayThatHasAlreadyStopped()
    {
        try (AeronArchive aeronArchive = AeronArchive.connect(TestContexts.ipcAeronArchive()))
        {
            final ArchiveSystemTests.RecordingResult recordingResult = ArchiveSystemTests.recordData(aeronArchive);

            final Aeron aeron = aeronArchive.context().aeron();
            final int replayStreamId = 10001;

            final long replaySessionId = aeronArchive.startReplay(
                recordingResult.recordingId(), IPC_CHANNEL, replayStreamId, new ReplayParams());

            final String replayChannel = ChannelUri.addSessionId(IPC_CHANNEL, (int)replaySessionId);
            final Subscription replay = aeron.addSubscription(replayChannel, replayStreamId);

            final MutableLong replayPosition = new MutableLong();
            while (replayPosition.get() < recordingResult.position() / 2)
            {
                if (0 == replay.poll((buffer, offset, length, header) -> replayPosition.set(header.position()), 10))
                {
                    aeronArchive.checkForErrorResponse();
                    Tests.yield();
                }
            }

            CloseHelper.quietClose(replay);
            aeronArchive.stopReplay(replaySessionId);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldExitOnEmptyRecording()
    {
        try (AeronArchive aeronArchive = AeronArchive.connect(TestContexts.ipcAeronArchive()))
        {
            final Aeron aeron = aeronArchive.context().aeron();
            final CountersReader countersReader = aeron.countersReader();
            final long recordingId;
            final int recordingCounterId;
            try (Publication publication = aeronArchive.addRecordedPublication("aeron:ipc", 10000))
            {
                recordingCounterId = Tests.awaitRecordingCounterId(
                    countersReader, publication.sessionId(), aeronArchive.archiveId());
                recordingId = RecordingPos.getRecordingId(countersReader, recordingCounterId);
            }

            while (CountersReader.RECORD_ALLOCATED == countersReader.getCounterState(recordingCounterId))
            {
                Tests.yield();
            }

            assertEquals(0, aeronArchive.getStopPosition(recordingId));

            final int replayStreamId = 10001;

            try
            {
                aeronArchive.startReplay(
                    recordingId,
                    CommonContext.IPC_CHANNEL,
                    replayStreamId,
                    new ReplayParams().position(AeronArchive.NULL_POSITION).length(AeronArchive.REPLAY_ALL_AND_STOP));

                fail("Should have thrown exception");
            }
            catch (final ArchiveException ex)
            {
                assertEquals(ArchiveException.EMPTY_RECORDING, ex.errorCode());
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldExitOnNonEmptyLiveRecording()
    {
        try (AeronArchive aeronArchive = AeronArchive.connect(TestContexts.ipcAeronArchive()))
        {
            final Aeron aeron = aeronArchive.context().aeron();
            final long recordingId;
            try (Publication publication = aeronArchive.addRecordedPublication("aeron:ipc", 10000))
            {
                final int recordingCounterId = Tests.awaitRecordingCounterId(
                    aeron.countersReader(), publication.sessionId(), aeronArchive.archiveId());
                recordingId = RecordingPos.getRecordingId(aeron.countersReader(), recordingCounterId);

                writeMessages(publication, "this is a test message", 1);
                awaitRecordingPosition(aeronArchive, recordingId, publication.position());

                final int replayStreamId = 10001;

                final long replaySessionId = aeronArchive.startReplay(
                    recordingId,
                    IPC_CHANNEL,
                    replayStreamId,
                    new ReplayParams().position(NULL_POSITION).length(REPLAY_ALL_AND_STOP));

                final String replayChannel = ChannelUri.addSessionId(IPC_CHANNEL, (int)replaySessionId);
                final AtomicReference<Image> availableImage = new AtomicReference<>();
                final AtomicReference<Image> unavailableImage = new AtomicReference<>();
                final Subscription replay = aeron.addSubscription(
                    replayChannel, replayStreamId, availableImage::set, unavailableImage::set);

                Image image;
                while (null == (image = availableImage.get()))
                {
                    aeronArchive.checkForErrorResponse();
                    Tests.yield();
                }

                final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {};
                while (!image.isEndOfStream())
                {
                    if (0 == image.poll(fragmentHandler, 100))
                    {
                        aeronArchive.checkForErrorResponse();
                        Tests.yield();
                    }
                }

                while (image != unavailableImage.get())
                {
                    Tests.yield();
                }

                CloseHelper.quietClose(replay);
                aeronArchive.stopReplay(replaySessionId);
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldExitOnEmptyLiveRecording()
    {
        try (AeronArchive aeronArchive = AeronArchive.connect(TestContexts.ipcAeronArchive()))
        {
            final Aeron aeron = aeronArchive.context().aeron();
            try (Publication publication = aeronArchive.addRecordedPublication("aeron:ipc", 10000))
            {
                final int recordingCounterId = Tests.awaitRecordingCounterId(
                    aeron.countersReader(), publication.sessionId(), aeronArchive.archiveId());
                final long recordingId = RecordingPos.getRecordingId(aeron.countersReader(), recordingCounterId);

                final int replayStreamId = 10001;

                try
                {
                    aeronArchive.startReplay(
                        recordingId,
                        IPC_CHANNEL,
                        replayStreamId,
                        new ReplayParams().position(NULL_POSITION).length(REPLAY_ALL_AND_STOP));

                    fail("Should have thrown exception");
                }
                catch (final ArchiveException ex)
                {
                    assertEquals(ArchiveException.EMPTY_RECORDING, ex.errorCode());
                }
            }
        }
    }

    @Test
    @InterruptAfter(20)
    void shouldNotExitWhenFollowingAnEmptyLiveRecording()
    {
        try (AeronArchive aeronArchive = AeronArchive.connect(TestContexts.ipcAeronArchive()))
        {
            final Aeron aeron = aeronArchive.context().aeron();
            final long recordingId;
            try (Publication publication = aeronArchive.addRecordedPublication("aeron:ipc", 10000))
            {
                final int recordingCounterId = Tests.awaitRecordingCounterId(
                    aeron.countersReader(), publication.sessionId(), aeronArchive.archiveId());
                recordingId = RecordingPos.getRecordingId(aeron.countersReader(), recordingCounterId);

                final int replayStreamId = 10001;

                final long replaySessionId = aeronArchive.startReplay(
                    recordingId,
                    IPC_CHANNEL,
                    replayStreamId,
                    new ReplayParams().position(NULL_POSITION).length(REPLAY_ALL_AND_FOLLOW));

                final String replayChannel = ChannelUri.addSessionId(IPC_CHANNEL, (int)replaySessionId);
                final AtomicReference<Image> availableImage = new AtomicReference<>();
                final AtomicReference<Image> unavailableImage = new AtomicReference<>();
                final Subscription replay = aeron.addSubscription(
                    replayChannel, replayStreamId, availableImage::set, unavailableImage::set);

                Image image;
                while (null == (image = availableImage.get()))
                {
                    aeronArchive.checkForErrorResponse();
                    Tests.yield();
                }

                final long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                while (System.nanoTime() < deadlineNs)
                {
                    assertFalse(image.isEndOfStream());
                    aeronArchive.checkForErrorResponse();
                    Tests.yield();
                }

                assertNull(unavailableImage.get());

                CloseHelper.quietClose(replay);
                aeronArchive.stopReplay(replaySessionId);
            }
        }
    }

    private static void awaitRecordingPosition(
        final AeronArchive aeronArchive,
        final long recordingId,
        final long position)
    {
        while (aeronArchive.getMaxRecordedPosition(recordingId) < position)
        {
            Tests.yield();
            aeronArchive.checkForErrorResponse();
        }
    }

    private static void writeMessages(final Publication publication, final String thisIsATestMessage, final int count)
    {
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(thisIsATestMessage.getBytes());
        for (int i = 0; i < count; i++)
        {
            while (publication.offer(unsafeBuffer, 0, unsafeBuffer.capacity()) < 0)
            {
                Tests.yield();
            }
        }
    }
}
