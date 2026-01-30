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
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.InterruptAfter;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.Aeron.Context;
import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.Aeron.connect;
import static io.aeron.archive.ArchiveSystemTests.awaitSignal;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

class ValidationTests
{
    private static final int MESSAGE_LENGTH = 4200;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
    private final CachedEpochClock epochClock = new CachedEpochClock();
    private TestMediaDriver driver;
    private Archive archive;
    private AeronArchive aeronArchive;
    private Aeron aeron;
    private TestRecordingSignalConsumer recordingSignalConsumer;

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

        aeron = connect(new Context().aeronDirectoryName(driver.aeronDirectoryName()));

        epochClock.update(System.currentTimeMillis());
        archive = Archive.launch(
            new Archive.Context()
                .aeronDirectoryName(driver.aeronDirectoryName())
                .archiveDir(tempDir.resolve("archive").toFile())
                .threadingMode(ArchiveThreadingMode.INVOKER)
                .controlChannel("aeron:udp?endpoint=localhost:5050")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .segmentFileLength(TERM_MIN_LENGTH)
                .epochClock(epochClock));

        systemTestWatcher.dataCollector().add(archive.context().archiveDir());

        aeronArchive = AeronArchive.connect(new AeronArchive.Context()
            .controlRequestChannel(archive.context().localControlChannel())
            .controlRequestStreamId(archive.context().localControlStreamId())
            .controlResponseChannel("aeron:ipc")
            .controlResponseStreamId(4004)
            .aeronDirectoryName(driver.aeronDirectoryName())
            .agentInvoker(archive.invoker()));

        recordingSignalConsumer = new TestRecordingSignalConsumer(aeronArchive.controlSessionId());
        aeronArchive.context().recordingSignalConsumer(recordingSignalConsumer);

        ThreadLocalRandom.current().nextBytes(buffer.byteArray());
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, aeronArchive, archive, driver);
    }

    @Test
    @InterruptAfter(10)
    void shouldRejectExtendRecordingRequestIfTruncationIsInProgress()
    {
        assertNotNull(archive.invoker());

        final String channel = "aeron:ipc?ssc=true";
        final int streamId = 888;
        final CountersReader countersReader = aeron.countersReader();

        final ExclusivePublication publication = aeron.addExclusivePublication(channel, streamId);
        final String recordChannel = ChannelUri.addSessionId(channel, publication.sessionId());
        final long recordingSessionId = aeronArchive.startRecording(
            recordChannel, streamId, LOCAL, true);

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
        final long truncatePosition = publication.position();
        final long endPosition = 2L * publication.termBufferLength() + truncatePosition;

        while (publication.position() < endPosition)
        {
            if (publication.offer(buffer, 0, MESSAGE_LENGTH) < 0)
            {
                archive.invoker().invoke();
            }
        }
        final long stopPosition = publication.position();
        assertThat(stopPosition, greaterThanOrEqualTo(endPosition));

        while (countersReader.getCounterValue(recordingCounterId) != stopPosition)
        {
            archive.invoker().invoke();
        }

        aeronArchive.stopRecording(recordingSessionId);
        while (aeronArchive.getStopPosition(recordingId) == NULL_POSITION)
        {
            Tests.sleep(1);
        }

        recordingSignalConsumer.reset();
        assertEquals(2, aeronArchive.truncateRecording(recordingId, truncatePosition));

        final ArchiveException exception = assertThrowsExactly(
            ArchiveException.class,
            () -> aeronArchive.extendRecording(recordingId, recordChannel, streamId, LOCAL, true));
        assertEquals(ArchiveException.GENERIC, exception.errorCode());
        assertThat(
            exception.getMessage(),
            endsWith("cannot extend recording " + recordingId + " due to an outstanding delete operation"));

        awaitSignal(aeronArchive, recordingSignalConsumer, recordingId, RecordingSignal.DELETE);

        // once delete is complete we can extend the recording
        assertNotEquals(NULL_VALUE, aeronArchive.extendRecording(recordingId, recordChannel, streamId, LOCAL, true));
    }

    @Test
    @InterruptAfter(10)
    void shouldRejectExtendRecordingSessionIfTruncateIsInProgress()
    {
        assertNotNull(archive.invoker());

        final String channel = "aeron:ipc?ssc=true";
        final int streamId = 456;
        final CountersReader countersReader = aeron.countersReader();

        final ExclusivePublication publication = aeron.addExclusivePublication(channel, streamId);
        final String recordChannel = ChannelUri.addSessionId(channel, publication.sessionId());
        final long recordingSessionId = aeronArchive.startRecording(
            recordChannel, streamId, LOCAL, true);

        int recordingCounterId;
        while (NULL_COUNTER_ID == (recordingCounterId = RecordingPos.findCounterIdBySession(
            countersReader, publication.sessionId(), aeronArchive.archiveId())))
        {
            epochClock.update(System.currentTimeMillis());
            archive.invoker().invoke();
        }

        final long recordingId = RecordingPos.getRecordingId(countersReader, recordingCounterId);

        while (publication.offer(buffer, 0, MESSAGE_LENGTH) < 0)
        {
            archive.invoker().invoke();
        }
        final long truncatePosition = publication.position();
        final long endPosition = 10L * publication.termBufferLength() + truncatePosition;

        while (publication.position() < endPosition)
        {
            if (publication.offer(buffer, 0, MESSAGE_LENGTH) < 0)
            {
                epochClock.update(System.currentTimeMillis());
                archive.invoker().invoke();
            }
        }
        final long stopPosition = publication.position();
        assertThat(stopPosition, greaterThanOrEqualTo(endPosition));

        while (countersReader.getCounterValue(recordingCounterId) != stopPosition)
        {
            epochClock.update(System.currentTimeMillis());
            archive.invoker().invoke();
        }

        aeronArchive.stopRecording(recordingSessionId);
        while (aeronArchive.getStopPosition(recordingId) == NULL_POSITION)
        {
            Tests.sleep(1);
        }

        assertNotEquals(
            NULL_VALUE, aeronArchive.extendRecording(recordingId, recordChannel, streamId, LOCAL, true));

        assertEquals(10, aeronArchive.truncateRecording(recordingId, truncatePosition));

        epochClock.advance(1);
        archive.invoker().invoke();

        final String error = aeronArchive.pollForErrorResponse();
        assertThat(
            error,
            containsString("cannot extend recording " + recordingId + " due to an outstanding delete operation"));
    }

    @Test
    @InterruptAfter(10)
    void shouldRejectDeleteAttemptIfAnotherOneInProgress()
    {
        assertNotNull(archive.invoker());

        final String channel = "aeron:ipc?ssc=true";
        final int streamId = 123;
        final CountersReader countersReader = aeron.countersReader();

        final ExclusivePublication publication = aeron.addExclusivePublication(channel, streamId);
        final String recordChannel = ChannelUri.addSessionId(channel, publication.sessionId());
        final long recordingSessionId = aeronArchive.startRecording(
            recordChannel, streamId, LOCAL, true);

        int recordingCounterId;
        while (NULL_COUNTER_ID == (recordingCounterId = RecordingPos.findCounterIdBySession(
            countersReader, publication.sessionId(), aeronArchive.archiveId())))
        {
            epochClock.update(System.currentTimeMillis());
            archive.invoker().invoke();
        }

        final long recordingId = RecordingPos.getRecordingId(countersReader, recordingCounterId);

        final long truncatePosition = 0;
        final long endPosition = 3L * publication.termBufferLength() + 1000;

        while (publication.position() < endPosition)
        {
            if (publication.offer(buffer, 0, MESSAGE_LENGTH) < 0)
            {
                epochClock.update(System.currentTimeMillis());
                archive.invoker().invoke();
            }
        }
        final long stopPosition = publication.position();
        assertThat(stopPosition, greaterThanOrEqualTo(endPosition));

        while (countersReader.getCounterValue(recordingCounterId) != stopPosition)
        {
            epochClock.update(System.currentTimeMillis());
            archive.invoker().invoke();
        }

        aeronArchive.stopRecording(recordingSessionId);
        while (aeronArchive.getStopPosition(recordingId) == NULL_POSITION)
        {
            Tests.sleep(1);
        }

        assertEquals(4, aeronArchive.truncateRecording(recordingId, truncatePosition));

        final ArchiveException exception =
            assertThrowsExactly(ArchiveException.class, () -> aeronArchive.purgeRecording(recordingId));
        assertThat(
            exception.getMessage(),
            endsWith("another delete operation in progress for recording id: " + recordingId));
    }
}
