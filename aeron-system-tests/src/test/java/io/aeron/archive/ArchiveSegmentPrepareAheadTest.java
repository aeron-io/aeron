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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.RecordingSignal;
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
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static io.aeron.archive.ArchiveSystemTests.CATALOG_CAPACITY;
import static io.aeron.archive.ArchiveSystemTests.awaitSignal;
import static io.aeron.archive.ArchiveSystemTests.consume;
import static io.aeron.archive.ArchiveSystemTests.injectRecordingSignalConsumer;
import static io.aeron.archive.client.AeronArchive.connect;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Exercises the conductor-side segment prepare-ahead path end to end:
 * {@link Archive.Context#segmentPrepareAhead(boolean)} in {@link ArchiveThreadingMode#DEDICATED} so roll overs
 * on the recorder thread consume segment files pre-created on the conductor.
 */
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class ArchiveSegmentPrepareAheadTest
{
    private static final int TERM_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int SEGMENT_LENGTH = TERM_LENGTH * 2;
    private static final int STREAM_ID = 1033;
    private static final int MTU_LENGTH = 1024;

    private static final int REPLAY_STREAM_ID = 66;
    private static final String REPLAY_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:6666")
        .build();

    private final ChannelUriStringBuilder uriBuilder = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:3333")
        .mtu(MTU_LENGTH)
        .termLength(TERM_LENGTH);
    private TestMediaDriver driver;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive aeronArchive;
    private TestRecordingSignalConsumer signalConsumer;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @BeforeEach
    void before(@TempDir final Path tempDir)
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .publicationTermBufferLength(TERM_LENGTH)
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .spiesSimulateConnection(true)
            .dirDeleteOnStart(true);

        final Archive.Context archiveCtx = TestContexts.localhostArchive()
            .catalogCapacity(CATALOG_CAPACITY)
            .segmentFileLength(SEGMENT_LENGTH)
            .segmentPrepareAhead(true)
            .deleteArchiveOnStart(true)
            .archiveDir(tempDir.resolve("archive").toFile())
            .fileSyncLevel(0)
            .threadingMode(ArchiveThreadingMode.DEDICATED);

        driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());
        archive = Archive.launch(archiveCtx);
        systemTestWatcher.dataCollector().add(archiveCtx.archiveDir());

        aeron = Aeron.connect();

        aeronArchive = connect(
            TestContexts.localhostAeronArchive()
                .aeron(aeron));

        signalConsumer = injectRecordingSignalConsumer(aeronArchive);
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeronArchive, aeron, archive, driver);
    }

    @Test
    @InterruptAfter(10)
    void shouldRecordAcrossSegmentBoundariesUsingPreparedSegments()
    {
        final String messagePrefix = "Message-Prefix-";
        final long targetPosition = (SEGMENT_LENGTH * 3L) + 1;

        final long recordingId;
        final long stopPosition;
        final int messageCount;

        try (Publication publication = aeronArchive.addRecordedPublication(uriBuilder.build(), STREAM_ID))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
            recordingId = RecordingPos.getRecordingId(counters, counterId);

            final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
            int i = 0;
            while (publication.position() < targetPosition)
            {
                final int length = buffer.putStringWithoutLengthAscii(0, messagePrefix + i);
                while (publication.offer(buffer, 0, length) <= 0)
                {
                    Tests.yield();
                }
                i++;
            }
            messageCount = i;
            stopPosition = publication.position();
            Tests.awaitPosition(counters, counterId, stopPosition);

            signalConsumer.reset();
            aeronArchive.stopRecording(publication);
            awaitSignal(aeronArchive, signalConsumer, RecordingSignal.STOP);
        }

        assertEquals(stopPosition, aeronArchive.getStopPosition(recordingId));

        // three roll overs, each consuming (or falling back from) a conductor-prepared segment
        final File archiveDir = archive.context().archiveDir();
        final String fileNamePrefix = recordingId + "-";
        assertThat(archiveDir.list((dir, name) -> name.startsWith(fileNamePrefix)), arrayWithSize(4));
        assertArrayEquals(
            new String[0],
            archiveDir.list((dir, name) -> name.endsWith(".prep")),
            "no .prep files should remain");

        // every message must replay intact across the segment boundaries
        try (Subscription subscription = aeronArchive.replay(
            recordingId, 0L, stopPosition, REPLAY_CHANNEL, REPLAY_STREAM_ID))
        {
            consume(subscription, messageCount, messagePrefix);
            assertEquals(stopPosition, subscription.imageAtIndex(0).position());
        }
    }
}
