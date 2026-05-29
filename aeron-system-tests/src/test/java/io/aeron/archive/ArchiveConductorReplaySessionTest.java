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
import io.aeron.CommonContext;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ReplayParams;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.InterruptAfter;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.REPLAY_ALL_AND_STOP;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class ArchiveConductorReplaySessionTest
{
    private static final int MESSAGE_LENGTH = 4200;
    private static final int RECORD_STREAM_ID = 10000;
    private static final int REPLAY_STREAM_ID = 10001;
    private static final int REPLAY_SESSION_ID = 42;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
    private TestMediaDriver driver;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive aeronArchive;

    @BeforeEach
    void before(final @TempDir Path tempDir)
    {
        driver = TestMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(CommonContext.generateRandomDirName())
                .threadingMode(ThreadingMode.SHARED)
                .ipcTermBufferLength(TERM_MIN_LENGTH)
                .publicationTermBufferLength(TERM_MIN_LENGTH),
            systemTestWatcher);

        systemTestWatcher.dataCollector().add(driver.context().aeronDirectory());

        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));

        archive = Archive.launch(
            new Archive.Context()
                .aeronDirectoryName(driver.aeronDirectoryName())
                .archiveDir(tempDir.resolve("archive").toFile())
                .threadingMode(ArchiveThreadingMode.INVOKER)
                .controlChannel("aeron:udp?endpoint=localhost:0")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .segmentFileLength(TERM_MIN_LENGTH));

        systemTestWatcher.dataCollector().add(archive.context().archiveDir());

        aeronArchive = AeronArchive.connect(new AeronArchive.Context()
            .controlRequestChannel(archive.context().localControlChannel())
            .controlRequestStreamId(archive.context().localControlStreamId())
            .controlResponseChannel("aeron:ipc")
            .controlResponseStreamId(4004)
            .aeronDirectoryName(driver.aeronDirectoryName())
            .agentInvoker(archive.invoker()));

        ThreadLocalRandom.current().nextBytes(buffer.byteArray());

        systemTestWatcher.ignoreErrorsMatching(s -> s.contains("existing publication has clashing sessionId"));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, aeronArchive, archive, driver);
    }

    @Test
    @InterruptAfter(10)
    void shouldSucceedOnReplayRestartWithSameSessionId()
    {
        assertNotNull(archive.invoker());

        final CountersReader countersReader = aeron.countersReader();
        final long recordingId = recordData(countersReader);

        final String replayChannel = ChannelUri.addSessionId("aeron:ipc", REPLAY_SESSION_ID);
        final Counter replaySessionCounter = archive.context().replaySessionCounter();

        final long firstReplayId = aeronArchive.startReplay(
            recordingId, replayChannel, REPLAY_STREAM_ID, new ReplayParams().length(REPLAY_ALL_AND_STOP));

        while (replaySessionCounter.get() < 1)
        {
            archive.invoker().invoke();
            Tests.yield();
        }

        aeronArchive.stopReplay(firstReplayId);

        aeronArchive.startReplay(
            recordingId, replayChannel, REPLAY_STREAM_ID, new ReplayParams().length(REPLAY_ALL_AND_STOP));

        while (replaySessionCounter.get() < 1)
        {
            archive.invoker().invoke();
            Tests.yield();

            final String error = aeronArchive.pollForErrorResponse();
            if (null != error)
            {
                fail("second startReplay with same session-id failed: " + error);
            }
        }
    }

    private long recordData(final CountersReader countersReader)
    {
        final ExclusivePublication publication = aeron.addExclusivePublication("aeron:ipc", RECORD_STREAM_ID);
        final long recordingSubscriptionId = aeronArchive.startRecording(
            ChannelUri.addSessionId("aeron:ipc", publication.sessionId()), RECORD_STREAM_ID, LOCAL, true);

        int recordingCounterId;
        while (NULL_COUNTER_ID == (recordingCounterId = RecordingPos.findCounterIdBySession(
            countersReader, publication.sessionId(), aeronArchive.archiveId())))
        {
            archive.invoker().invoke();
        }

        final long recordingId = RecordingPos.getRecordingId(countersReader, recordingCounterId);

        while (publication.offer(buffer, 0, MESSAGE_LENGTH) < 0)
        {
            archive.invoker().invoke();
        }
        final long position = publication.position();

        while (countersReader.getCounterValue(recordingCounterId) < position)
        {
            archive.invoker().invoke();
        }

        aeronArchive.stopRecording(recordingSubscriptionId);
        while (aeronArchive.getStopPosition(recordingId) == NULL_POSITION)
        {
            archive.invoker().invoke();
        }

        CloseHelper.close(publication);
        return recordingId;
    }
}
