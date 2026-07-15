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

import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.agent.ArchiveEventCode;
import io.aeron.agent.EventConfiguration;
import io.aeron.agent.EventReaderManager;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.Tests;
import io.aeron.test.agent.CountingEventReaderAgent;
import org.agrona.IoUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import static io.aeron.agent.ArchiveEventCode.CMD_IN_AUTH_CONNECT;
import static io.aeron.agent.ArchiveEventCode.CMD_IN_FIND_LAST_MATCHING_RECORD;
import static io.aeron.agent.ArchiveEventCode.CMD_IN_KEEP_ALIVE;
import static io.aeron.agent.ArchiveEventCode.CMD_IN_MAX_RECORDED_POSITION;
import static io.aeron.agent.ArchiveEventCode.CMD_IN_START_RECORDING;
import static io.aeron.agent.ArchiveEventCode.CMD_IN_STOP_RECORDING;
import static io.aeron.agent.ArchiveEventCode.CMD_OUT_RESPONSE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith(InterruptingTestCallback.class)
public class ArchiveLoggingAgentTest
{
    private File testDir;

    @BeforeEach
    void setUp()
    {
        assumeTrue("all".equals(System.getProperty(CommonContext.ARCHIVE_EVENT_LOG)));
        final String readerClass = System.getProperty(EventReaderManager.READER_CLASSNAME);
        assumeTrue("io.aeron.test.agent.CountingEventReaderAgent".equals(readerClass));
    }

    @AfterEach
    void after()
    {
        if (testDir != null && testDir.exists())
        {
            IoUtil.delete(testDir, false);
        }
    }

    @SuppressWarnings("try")
    @Test
    @InterruptAfter(10)
    void logAll()
    {
        testDir = Paths.get(IoUtil.tmpDirName(), "archive-test").toFile();
        if (testDir.exists())
        {
            IoUtil.delete(testDir, false);
        }

        final MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
            .errorHandler(Tests::onError)
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);

        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context()
            .controlRequestChannel("aeron:udp?term-length=64k|endpoint=localhost:8010")
            .controlRequestStreamId(100)
            .controlResponseChannel("aeron:udp?term-length=64k|endpoint=localhost:0")
            .controlResponseStreamId(101);

        final Archive.Context archiveCtx = new Archive.Context()
            .errorHandler(Tests::onError)
            .archiveDir(new File(testDir, "archive"))
            .deleteArchiveOnStart(true)
            .recordingEventsEnabled(false)
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .controlChannel(aeronArchiveContext.controlRequestChannel())
            .controlStreamId(aeronArchiveContext.controlRequestStreamId())
            .localControlStreamId(aeronArchiveContext.controlRequestStreamId())
            .recordingEventsChannel(aeronArchiveContext.recordingEventsChannel())
            .threadingMode(ArchiveThreadingMode.SHARED);

        try (ArchivingMediaDriver ignore1 = ArchivingMediaDriver.launch(mediaDriverCtx, archiveCtx);
            AeronArchive aeronArchive = AeronArchive.connect(aeronArchiveContext))
        {
            final String channel = "aeron:ipc?ssc=true";
            final int streamId = 1000;
            final ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(channel, streamId);

            final UnsafeBuffer msg = new UnsafeBuffer(new byte[256]);
            ThreadLocalRandom.current().nextBytes(msg.byteArray());
            while (publication.offer(msg) < 0)
            {
                Tests.yield();
            }

            final CountersReader counters = aeronArchive.context().aeron().countersReader();
            final int recordingCounterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
            final long recordingId = RecordingPos.getRecordingId(counters, recordingCounterId);
            assertNotEquals(RecordingPos.NULL_RECORDING_ID, recordingId);

            assertEquals(
                recordingId,
                aeronArchive.findLastMatchingRecording(0, "aeron:ipc", streamId, publication.sessionId()));

            Tests.await(() -> publication.position() == aeronArchive.getMaxRecordedPosition(recordingId));

            aeronArchive.stopRecording(publication);

            verifyExpectedEvents(EnumSet.of(
                CMD_OUT_RESPONSE, CMD_IN_AUTH_CONNECT, CMD_IN_KEEP_ALIVE, CMD_IN_START_RECORDING,
                CMD_IN_FIND_LAST_MATCHING_RECORD, CMD_IN_MAX_RECORDED_POSITION, CMD_IN_STOP_RECORDING));
        }
    }

    private static void verifyExpectedEvents(final EnumSet<ArchiveEventCode> expectedEvents)
    {
        final Agent agent = EventConfiguration.eventReader().agent();
        Assertions.assertInstanceOf(CountingEventReaderAgent.class, agent);
        final CountingEventReaderAgent countingAgent = (CountingEventReaderAgent)agent;

        final List<ArchiveEventCode> pendingList = new ArrayList<>(expectedEvents);

        final Supplier<String> errorMessage = () -> "Pending events: " + pendingList;
        while (!pendingList.isEmpty())
        {
            pendingList.removeIf(code -> 0 < countingAgent.countArchiveEvent(code.toEventCodeId()));
            Tests.sleep(1, errorMessage);
        }
    }
}
