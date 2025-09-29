/*
 * Copyright 2014-2024 Real Logic Limited.
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

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.driver.MediaDriver;
import io.aeron.samples.archive.RecordingDescriptor;
import io.aeron.samples.archive.RecordingDescriptorCollector;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;

import static io.aeron.archive.ArchiveSystemTests.recordData;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(InterruptingTestCallback.class)
public class ArchiveListRecordingsTest
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
            .spiesSimulateConnection(true)
            .dirDeleteOnStart(true);

        final Archive.Context archiveContext = TestContexts.localhostArchive()
            .aeronDirectoryName(driverCtx.aeronDirectoryName())
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
        CloseHelper.quietCloseAll(archive);
        CloseHelper.quietCloseAll(driver);
    }

    @Test
    @InterruptAfter(3)
    void shouldFilterByChannelUri()
    {
        try (AeronArchive aeronArchive = AeronArchive.connect(TestContexts.ipcAeronArchive()))
        {
            final ArchiveSystemTests.RecordingResult result1 = recordData(
                aeronArchive, 1, "snapshot-id:10;complete=true;");

            final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(10);

            assertEquals(1, aeronArchive.listRecordingsForUri(
                0, Integer.MAX_VALUE, "alias=snapshot-id:10;", result1.streamId(), collector.reset()));

            assertEquals(0, aeronArchive.listRecordingsForUri(
                0, Integer.MAX_VALUE, "alias=snapshot-id:1;", result1.streamId(), collector.reset()));

            assertEquals(1, aeronArchive.listRecordingsForUri(
                0, Integer.MAX_VALUE, "alias=snapshot-id:1", result1.streamId(), collector.reset()));
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldUpdateChannelForARecording()
    {
        try (AeronArchive aeronArchive = AeronArchive.connect(TestContexts.ipcAeronArchive()))
        {
            final ArchiveSystemTests.RecordingResult result1 = recordData(
                aeronArchive, 1, "snapshot-id-in-progress:10;complete=true;");

            final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(10);

            assertEquals(1, aeronArchive.listRecordingsForUri(
                0, Integer.MAX_VALUE, "snapshot-id-in-progress:", result1.streamId(), collector.reset()));

            assertEquals(1, collector.descriptors().size());
            RecordingDescriptor recordingDescriptor = collector.descriptors().get(0);

            final ChannelUri channel = ChannelUri.parse(recordingDescriptor.originalChannel());
            channel.put(CommonContext.ALIAS_PARAM_NAME, "snapshot-id:10;");

            aeronArchive.updateChannel(recordingDescriptor.recordingId(), channel.toString());

            assertEquals(1, aeronArchive.listRecordingsForUri(
                0, Integer.MAX_VALUE, "snapshot-id:", result1.streamId(), collector.reset()));

            assertEquals(1, collector.descriptors().size());
            recordingDescriptor = collector.descriptors().get(0);

            assertThat(recordingDescriptor.originalChannel(), containsString("snapshot-id:10"));
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldFailToUpdateChannelForARecordingThatDoesntExist()
    {
        try (AeronArchive aeronArchive = AeronArchive.connect(TestContexts.ipcAeronArchive()))
        {
            final ArchiveSystemTests.RecordingResult result1 = recordData(
                aeronArchive, 1, "snapshot-id-in-progress:10;complete=true;");

            final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(10);

            assertEquals(1, aeronArchive.listRecordingsForUri(
                0, Integer.MAX_VALUE, "snapshot-id-in-progress:", result1.streamId(), collector.reset()));

            assertEquals(1, collector.descriptors().size());
            final RecordingDescriptor recordingDescriptor = collector.descriptors().get(0);

            final ChannelUri channel = ChannelUri.parse(recordingDescriptor.originalChannel());
            channel.put(CommonContext.ALIAS_PARAM_NAME, "snapshot-id:10;");

            final long invalidRecordingId = 98273498273498723L;

            final ArchiveException archiveException = assertThrows(
                ArchiveException.class,
                () -> aeronArchive.updateChannel(invalidRecordingId, channel.toString()));
            assertThat(archiveException.getMessage(), containsString("RECORDING_UNKNOWN"));
        }
    }

    @Test
    @InterruptAfter(15)
    @SlowTest
    void shouldFailToUpdateChannelWhileARecordingListingIsRunning()
    {
        try (AeronArchive aeronArchive = AeronArchive.connect(TestContexts.ipcAeronArchive()))
        {
            final ArchiveSystemTests.RecordingResult result1 = recordData(
                aeronArchive, 1, "alias=original");

            final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(10);

            assertEquals(1, aeronArchive.listRecordingsForUri(
                0, Integer.MAX_VALUE, "alias=original", result1.streamId(), collector.reset()));

            assertEquals(1, collector.descriptors().size());
            final RecordingDescriptor recordingDescriptor = collector.descriptors().get(0);

            for (int i = 0; i < 150; i++)
            {
                recordData(aeronArchive, 1, "snapshot-id:" + (i + 1));
            }

            assertTrue(aeronArchive.archiveProxy().listRecordings(
                0,
                Integer.MAX_VALUE,
                aeronArchive.context().aeron().nextCorrelationId(),
                aeronArchive.controlSessionId()));

            final ChannelUri channel = ChannelUri.parse(recordingDescriptor.originalChannel());
            channel.put(CommonContext.ALIAS_PARAM_NAME, "alias=update");

            final ArchiveException archiveException = assertThrows(
                ArchiveException.class,
                () -> aeronArchive.updateChannel(recordingDescriptor.recordingId(), channel.toString()));
            assertThat(archiveException.getMessage(), containsString("active listing already in progress"));
        }
    }
}
