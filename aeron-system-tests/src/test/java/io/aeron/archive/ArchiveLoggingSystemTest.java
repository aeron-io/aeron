/*
 * Copyright 2014-2026 Real Logic Limited.
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
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logging.CollectingEventLogReaderAgent;
import io.aeron.logging.EventConfiguration;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.IoUtil;
import org.agrona.SystemUtil;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.ArchiveSystemTests.consume;
import static io.aeron.archive.ArchiveSystemTests.offer;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith(InterruptingTestCallback.class)
class ArchiveLoggingSystemTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private CollectingEventLogReaderAgent reader;

    @BeforeEach
    void before()
    {
        assumeTrue("all".equals(System.getProperty(CommonContext.ARCHIVE_EVENT_LOG)));
        final Object agent = EventConfiguration.eventReader().agent();
        assumeTrue(agent instanceof CollectingEventLogReaderAgent);
        reader = (CollectingEventLogReaderAgent)agent;
    }

    @AfterEach
    void after()
    {
        reader.reset();
    }

    @Test
    @InterruptAfter(3)
    @SuppressWarnings({ "try", "MethodLength" })
    void archiveOperationsWithOversizedVarLengthValuesShouldNotBreakArchiveAndLogging(
        final @TempDir Path tempDir) throws IOException
    {
        final String channelPrefix = "aeron:ipc?alias=";
        final int channelLength = 4000;
        // Truncated channel
        final String alias = "x".repeat(channelLength - channelPrefix.length());
        final String channel = channelPrefix + alias;
        final int streamId = 1000;
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final String aeronDirectoryName = CommonContext.generateRandomDirName();
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true);
        final File archiveDir = new File(SystemUtil.tmpDirName(), "archive-logging");
        final Archive.Context archiveCtx = TestContexts.localhostArchive()
            .aeronDirectoryName(aeronDirectoryName)
            .deleteArchiveOnStart(true)
            .archiveDir(archiveDir)
            .threadingMode(ArchiveThreadingMode.SHARED);

        try
        {
            try (TestMediaDriver driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
                Archive archive = Archive.launch(archiveCtx);
                Aeron aeronClient = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName));
                AeronArchive aeronArchiveClient = AeronArchive.connect(
                    TestContexts.localhostAeronArchive().aeron(aeronClient)))
            {
                systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());
                systemTestWatcher.dataCollector().add(archiveCtx.archiveDir());

                reader.setCollecting(true);

                final long stopPosition;

                final long subscriptionId = aeronArchiveClient.startRecording(channel, streamId, LOCAL);
                final long recordingIdFromCounter;
                final int sessionId;

                try (Subscription subscription = aeronClient.addSubscription(channel, streamId);
                    Publication publication = aeronClient.addPublication(channel, streamId))
                {
                    sessionId = publication.sessionId();

                    final CountersReader counters = aeronClient.countersReader();
                    final int counterId = Tests.awaitRecordingCounterId(
                        counters,
                        sessionId,
                        aeronArchiveClient.archiveId());
                    recordingIdFromCounter = RecordingPos.getRecordingId(counters, counterId);

                    assertEquals(CommonContext.IPC_CHANNEL, RecordingPos.getSourceIdentity(counters, counterId));

                    offer(publication, messageCount, messagePrefix);
                    consume(subscription, messageCount, messagePrefix);

                    stopPosition = publication.position();
                    Tests.awaitPosition(counters, counterId, stopPosition);

                    final long joinPosition = subscription.imageBySessionId(sessionId).joinPosition();
                    assertEquals(joinPosition, aeronArchiveClient.getStartPosition(recordingIdFromCounter));
                    assertEquals(stopPosition, aeronArchiveClient.getRecordingPosition(recordingIdFromCounter));
                    assertEquals(NULL_VALUE, aeronArchiveClient.getStopPosition(recordingIdFromCounter));
                    assertEquals(stopPosition, aeronArchiveClient.getMaxRecordedPosition(recordingIdFromCounter));
                }

                aeronArchiveClient.stopRecording(subscriptionId);
                Tests.await(() -> stopPosition == aeronArchiveClient.getStopPosition(recordingIdFromCounter));

                final long recordingId = aeronArchiveClient.findLastMatchingRecording(
                    0, "alias=" + alias, streamId, sessionId);

                assertFalse(aeronArchiveClient.tryStopRecordingByIdentity(recordingId));

                final long position = 0L;
                final long length = stopPosition - position;

                try (Subscription subscription = aeronArchiveClient.replay(
                    recordingId, position, length, channel, streamId))
                {
                    consume(subscription, messageCount, messagePrefix);
                    assertEquals(stopPosition, subscription.imageAtIndex(0).position());
                }

                aeronArchiveClient.truncateRecording(recordingId, position);

                final int count = aeronArchiveClient.listRecording(
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
                        _streamId,
                        strippedChannel,
                        originalChannel,
                        sourceIdentity) -> assertEquals(startPosition, newStopPosition));
                assertEquals(1, count);
            }

            final Path logFile = tempDir.resolve("archive.log");
            reader.writeToFile(logFile.toString());
            final String content = Files.readString(logFile);
            assertTrue(content.contains("replayChannel=...truncated"));
            assertTrue(content.contains("CMD_IN_START_RECORDING "));
            assertTrue(content.contains("CMD_OUT_RESPONSE"));
            assertTrue(content.contains("CMD_IN_STOP_RECORDING"));
            assertTrue(content.contains("CMD_IN_FIND_LAST_MATCHING_RECORD"));
            assertTrue(content.contains("CMD_IN_REPLAY"));
            assertTrue(content.contains("CMD_IN_CLOSE_SESSION"));
            assertTrue(content.contains("CONTROL_SESSION_STATE_CHANGE"));
            assertTrue(content.contains("RECORDING_SESSION_STATE_CHANGE"));
            assertTrue(content.contains("REPLAY_SESSION_STATE_CHANGE"));
            assertTrue(content.contains("RECORDING_SIGNAL"));
        }
        finally
        {
            IoUtil.delete(archiveDir, false);
        }
    }
}
