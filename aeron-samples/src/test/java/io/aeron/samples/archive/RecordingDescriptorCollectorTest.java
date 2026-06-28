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
package io.aeron.samples.archive;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.test.*;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(InterruptingTestCallback.class)
@SlowTest
@SuppressWarnings("try")
public class RecordingDescriptorCollectorTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(10)
    void shouldCollectPagesOfRecordingDescriptors(@TempDir final Path tempDir)
    {
        try (TestMediaDriver mediaDriver = TestMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(CommonContext.generateRandomDirName()), systemTestWatcher);
            Archive ignore = Archive.launch(TestContexts.localhostArchive()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .archiveDir(tempDir.resolve("archive").toFile())
                .deleteArchiveOnStart(true));
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
            AeronArchive aeronArchive = AeronArchive.connect(TestContexts.localhostAeronArchive()
                .aeron(aeron)
                .ownsAeronClient(false)))
        {
            final int numRecordings = 10;
            createRecordings(aeronArchive, numRecordings);

            final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(3);
            long fromRecordingId = 0;
            int count;
            while (0 < (count = aeronArchive.listRecordings(fromRecordingId, collector.poolSize(), collector.reset())))
            {
                final List<RecordingDescriptor> descriptors = collector.descriptors();
                assertThat(count, lessThanOrEqualTo(collector.poolSize()));
                assertThat(descriptors.size(), lessThanOrEqualTo(collector.poolSize()));

                //noinspection OptionalGetWithoutIsPresent
                final long maxRecordingId = descriptors.stream()
                    .mapToLong(RecordingDescriptor::recordingId)
                    .max()
                    .getAsLong();

                fromRecordingId = maxRecordingId + 1;
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldAllowUserToRetainDescriptorsToPreventReuse(@TempDir final Path tempDir)
    {
        try (TestMediaDriver mediaDriver = TestMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(CommonContext.generateRandomDirName()), systemTestWatcher);
            Archive ignore = Archive.launch(TestContexts.localhostArchive()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .archiveDir(tempDir.resolve("archive").toFile())
                .deleteArchiveOnStart(true));
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
            AeronArchive aeronArchive = AeronArchive.connect(TestContexts.localhostAeronArchive()
                .aeron(aeron)
                .ownsAeronClient(false)))
        {
            createRecordings(aeronArchive, 3);

            final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(1);
            long fromRecordingId = 0;
            int count = aeronArchive.listRecordings(fromRecordingId, collector.poolSize(), collector.reset());
            assertEquals(1, count);

            final RecordingDescriptor desc0 = collector.descriptors().get(0);

            fromRecordingId += count;
            count = aeronArchive.listRecordings(fromRecordingId, collector.poolSize(), collector.reset());
            assertEquals(1, count);

            final RecordingDescriptor desc1 = collector.descriptors().get(0);

            assertEquals(desc0.recordingId(), desc1.recordingId());
            desc1.retain();

            fromRecordingId += count;
            count = aeronArchive.listRecordings(fromRecordingId, collector.poolSize(), collector.reset());
            assertEquals(1, count);

            final RecordingDescriptor desc2 = collector.descriptors().get(0);

            assertNotEquals(desc1.recordingId(), desc2.recordingId());
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldShouldNotReuseDescriptorIfPoolSizeIsZero(@TempDir final Path tempDir)
    {
        try (TestMediaDriver mediaDriver = TestMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(CommonContext.generateRandomDirName()), systemTestWatcher);
            Archive ignore = Archive.launch(TestContexts.localhostArchive()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .archiveDir(tempDir.resolve("archive").toFile())
                .deleteArchiveOnStart(true));
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
            AeronArchive aeronArchive = AeronArchive.connect(TestContexts.localhostAeronArchive()
                .aeron(aeron)
                .ownsAeronClient(false)))
        {
            createRecordings(aeronArchive, 3);

            final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(0);
            long fromRecordingId = 0;
            int count = aeronArchive.listRecordings(fromRecordingId, 1, collector.reset());
            assertEquals(1, count);

            final RecordingDescriptor desc0 = collector.descriptors().get(0);

            fromRecordingId += count;
            count = aeronArchive.listRecordings(fromRecordingId, 1, collector.reset());
            assertEquals(1, count);

            final RecordingDescriptor desc1 = collector.descriptors().get(0);

            assertNotEquals(desc0.recordingId(), desc1.recordingId());
        }
    }

    private void createRecordings(final AeronArchive aeronArchive, final int numRecordings)
    {
        final UnsafeBuffer message = new UnsafeBuffer("this is some data".getBytes());

        for (int i = 0; i < numRecordings; i++)
        {
            try (Publication publication = aeronArchive.addRecordedPublication("aeron:ipc?ssc=true", 10000 + i))
            {
                long expectedPosition;
                while ((expectedPosition = publication.offer(message, 0, message.capacity())) < 0)
                {
                    Tests.yield();
                }

                long recordingId;
                while ((recordingId = aeronArchive.findLastMatchingRecording(
                    0, "aeron:ipc", publication.streamId(), publication.sessionId())) == Aeron.NULL_VALUE)
                {
                    Tests.yield();
                }

                while (expectedPosition < aeronArchive.getRecordingPosition(recordingId))
                {
                    Tests.yield();
                }
            }
        }
    }
}
