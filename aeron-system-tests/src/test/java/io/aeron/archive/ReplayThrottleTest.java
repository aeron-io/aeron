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
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ReplayParams;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.SystemUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.nio.ByteBuffer;

import static io.aeron.archive.ArchiveSystemTests.CATALOG_CAPACITY;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the {@link ReplayParams#maxReplayBytesPerSecond(long)} replay throttle end-to-end against a real
 * embedded archive in a single JVM: record a few MB, then replay the recording both unthrottled and with a low
 * bytes/sec cap and assert the capped replay is paced to roughly the configured rate. This observes the
 * {@link ReplaySession} token bucket actually limiting replay throughput (the compile/regression tests never set
 * a non-zero rate).
 */
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class ReplayThrottleTest
{
    private static final int STREAM_ID = 42;
    private static final int TERM_LENGTH = 1024 * 1024;
    private static final String RECORDED_CHANNEL = "aeron:ipc?term-length=" + TERM_LENGTH;

    private static final int REPLAY_STREAM_ID = 66;
    private static final String REPLAY_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:6666")
        .termLength(TERM_LENGTH)
        .build();

    private static final long RECORDING_BYTES = 6L * 1024 * 1024;
    private static final int MESSAGE_LENGTH = 1024;
    private static final long THROTTLE_BYTES_PER_SECOND = 1024L * 1024;

    private TestMediaDriver driver;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive aeronArchive;
    private File archiveDir;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @BeforeEach
    void before()
    {
        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .spiesSimulateConnection(false)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true);

        archiveDir = new File(SystemUtil.tmpDirName(), "archive-replay-throttle");

        final Archive.Context archiveCtx = TestContexts.localhostArchive()
            .catalogCapacity(CATALOG_CAPACITY)
            .segmentFileLength(TERM_LENGTH)
            .aeronDirectoryName(aeronDirectoryName)
            .deleteArchiveOnStart(true)
            .archiveDir(archiveDir)
            .fileSyncLevel(0)
            .threadingMode(ArchiveThreadingMode.DEDICATED);

        driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());
        archive = Archive.launch(archiveCtx);
        systemTestWatcher.dataCollector().add(archiveCtx.archiveDir());

        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName));
        aeronArchive = AeronArchive.connect(TestContexts.localhostAeronArchive().aeron(aeron));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeronArchive, aeron, archive, driver);
        IoUtil.delete(archiveDir, false);
    }

    @Test
    @InterruptAfter(30)
    void shouldThrottleReplayThroughputToConfiguredRate()
    {
        final long recordingId;
        final long stopPosition;

        try (ExclusivePublication publication =
            aeronArchive.addRecordedExclusivePublication(RECORDED_CHANNEL, STREAM_ID))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(
                counters, publication.sessionId(), aeronArchive.archiveId());
            recordingId = RecordingPos.getRecordingId(counters, counterId);

            stopPosition = publishAtLeast(publication, RECORDING_BYTES);
            Tests.awaitPosition(counters, counterId, stopPosition);
        }

        Tests.await(() -> aeronArchive.getStopPosition(recordingId) == stopPosition);

        // unthrottled baseline
        final long unthrottledNs = timeReplay(recordingId, stopPosition, 0);
        // throttled to THROTTLE_BYTES_PER_SECOND
        final long throttledNs = timeReplay(recordingId, stopPosition, THROTTLE_BYTES_PER_SECOND);

        final double throttledSeconds = throttledNs / 1_000_000_000.0;
        final double achievedBytesPerSecond = stopPosition / throttledSeconds;

        // Achieved rate must be bounded near the cap. The per-second token bucket is coarse: a window can
        // overshoot by up to one offerBlock (<= fileIoMaxLength), so allow up to 3x the configured cap.
        assertTrue(
            achievedBytesPerSecond <= THROTTLE_BYTES_PER_SECOND * 3.0,
            () -> "achieved " + (long)achievedBytesPerSecond + " B/s exceeds 3x cap " +
                THROTTLE_BYTES_PER_SECOND + " (throttled=" + throttledSeconds + "s)");

        // Throttled replay must be markedly slower than the unthrottled baseline of the same recording.
        assertTrue(
            throttledNs > unthrottledNs * 2,
            () -> "throttled=" + throttledNs + "ns not > 2x unthrottled=" + unthrottledNs + "ns");
    }

    private static long publishAtLeast(final ExclusivePublication publication, final long targetBytes)
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MESSAGE_LENGTH));

        while (publication.position() < targetBytes)
        {
            if (publication.offer(buffer, 0, MESSAGE_LENGTH) < 0)
            {
                Tests.yield();
            }
        }

        return publication.position();
    }

    private long timeReplay(final long recordingId, final long stopPosition, final long maxReplayBytesPerSecond)
    {
        final ReplayParams replayParams = new ReplayParams()
            .position(0)
            .length(stopPosition)
            .maxReplayBytesPerSecond(maxReplayBytesPerSecond);

        final long replaySessionId = aeronArchive.startReplay(
            recordingId, REPLAY_CHANNEL, REPLAY_STREAM_ID, replayParams);

        final String replayChannel = new ChannelUriStringBuilder(REPLAY_CHANNEL)
            .sessionId((int)replaySessionId)
            .build();

        try (Subscription subscription = aeron.addSubscription(replayChannel, REPLAY_STREAM_ID))
        {
            Tests.awaitConnected(subscription);
            final Image image = subscription.imageAtIndex(0);

            final long startNs = System.nanoTime();
            while (image.position() < stopPosition)
            {
                if (0 == image.poll((buffer, offset, length, header) -> {}, 128))
                {
                    if (image.isClosed())
                    {
                        throw new IllegalStateException("replay image closed before completion at position=" +
                            image.position() + " of " + stopPosition);
                    }
                    Tests.yield();
                }
            }

            return System.nanoTime() - startNs;
        }
    }
}
