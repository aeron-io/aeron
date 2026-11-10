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
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ReplayMerge;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.samples.archive.RecordingDescriptor;
import io.aeron.samples.archive.RecordingDescriptorCollector;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.SystemUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static io.aeron.CommonContext.MDC_CONTROL_MODE_MANUAL;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.aeron.archive.ArchiveSystemTests.CATALOG_CAPACITY;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class DualReplayMergeTest
{
    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private static final int STREAM_ID = 10000;
    private static final String LIVE_ENDPOINT = "239.192.12.87:20123";
    private static final String LIVE_INTERFACE = "127.0.0.1";
    private static final String REPLAY_ENDPOINT_A = "localhost:0";
    private static final String REPLAY_ENDPOINT_B = "localhost:0";

    private final MediaDriver.Context driverCtx = new MediaDriver.Context();

    private TestMediaDriver driver;
    private Archive archive;

    private void launch()
    {
        driverCtx.dirDeleteOnStart(true);

        driver = TestMediaDriver.launch(driverCtx, watcher);

        final File archiveDir = new File(SystemUtil.tmpDirName(), "archive");
        archive = Archive.launch(
            TestContexts.localhostArchive()
                .catalogCapacity(CATALOG_CAPACITY)
                .aeronDirectoryName(driver.context().aeronDirectoryName())
                .archiveDir(archiveDir)
                .recordingEventsEnabled(false)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(true));

        watcher.dataCollector().add(driver.context().aeronDirectory());
        watcher.dataCollector().add(archive.context().archiveDir());
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.quietCloseAll(archive, driver);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(20)
    @SuppressWarnings("methodlength")
    void shouldMergeTwoIndependentStreams(final boolean concurrent)
    {
        assumeTrue(OS.WINDOWS != OS.current() || TestMediaDriver.shouldRunJavaMediaDriver());

        launch();

        final int sessionIdA = 1111;
        final int sessionIdB = 2222;

        final String publicationChannelA = new ChannelUriStringBuilder()
            .media(UDP_MEDIA)
            .endpoint(LIVE_ENDPOINT)
            .networkInterface(LIVE_INTERFACE)
            .sessionId(sessionIdA)
            .spiesSimulateConnection(true)
            .build();
        final String publicationChannelB = new ChannelUriStringBuilder()
            .media(UDP_MEDIA)
            .endpoint(LIVE_ENDPOINT)
            .networkInterface(LIVE_INTERFACE)
            .sessionId(sessionIdB)
            .spiesSimulateConnection(true)
            .build();
        final String subscriptionChannelA = new ChannelUriStringBuilder()
            .media(UDP_MEDIA)
            .controlMode(MDC_CONTROL_MODE_MANUAL)
            .sessionId(sessionIdA)
            .alias("B")
            .build();
        final String subscriptionChannelB = new ChannelUriStringBuilder()
            .media(UDP_MEDIA)
            .controlMode(MDC_CONTROL_MODE_MANUAL)
            .sessionId(sessionIdB)
            .alias("B")
            .build();
        final String recordingChannel = new ChannelUriStringBuilder()
            .media(UDP_MEDIA)
            .endpoint(LIVE_ENDPOINT)
            .networkInterface(LIVE_INTERFACE)
            .build();
        final String replayChannelA = new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .sessionId(sessionIdA)
            .build();
        final String replayChannelB = new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .sessionId(sessionIdB)
            .build();
        final String liveDestination = new ChannelUriStringBuilder()
            .media(UDP_MEDIA)
            .endpoint(LIVE_ENDPOINT)
            .networkInterface(LIVE_INTERFACE)
            .build();
        final String replayDestinationA = new ChannelUriStringBuilder()
            .media(UDP_MEDIA)
            .endpoint(REPLAY_ENDPOINT_A)
            .build();
        final String replayDestinationB = new ChannelUriStringBuilder()
            .media(UDP_MEDIA)
            .endpoint(REPLAY_ENDPOINT_B)
            .build();

        final AeronArchive.Context aeronArchiveCtxA = new AeronArchive.Context()
            .aeronDirectoryName(driver.context().aeronDirectoryName())
            .controlRequestChannel(archive.context().localControlChannel())
            .controlResponseChannel(archive.context().localControlChannel());
        final AeronArchive.Context aeronArchiveCtxB = new AeronArchive.Context()
            .aeronDirectoryName(driver.context().aeronDirectoryName())
            .controlRequestChannel(archive.context().localControlChannel())
            .controlResponseChannel(archive.context().localControlChannel())
            .controlResponseStreamId(aeronArchiveCtxA.controlRequestStreamId() + 1);

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            AeronArchive aeronArchiveA = AeronArchive.connect(aeronArchiveCtxA);
            AeronArchive aeronArchiveB = AeronArchive.connect(aeronArchiveCtxB);
            Publication pubA = aeron.addExclusivePublication(publicationChannelA, STREAM_ID);
            Publication pubB = aeron.addExclusivePublication(publicationChannelB, STREAM_ID))
        {
            final long subscriptionId = aeronArchiveA.startRecording(recordingChannel, STREAM_ID, SourceLocation.LOCAL);

            final int numMessages = 10;
            sendMessages(pubA, numMessages);
            sendMessages(pubB, numMessages);

            final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(2);

            long recordingIdA = -1;
            long recordingIdB = -1;

            while (2 > aeronArchiveA.listRecordings(0, Integer.MAX_VALUE, collector.reset()))
            {
                Tests.yield();
            }

            for (final RecordingDescriptor descriptor : collector.descriptors())
            {
                if (descriptor.sessionId() == sessionIdA)
                {
                    recordingIdA = descriptor.recordingId();
                }
                else if (descriptor.sessionId() == sessionIdB)
                {
                    recordingIdB = descriptor.recordingId();
                }
            }

            assertNotEquals(-1, recordingIdA);
            assertNotEquals(-1, recordingIdB);

            try (Subscription subA = aeron.addSubscription(subscriptionChannelA, STREAM_ID);
                Subscription subB = aeron.addSubscription(subscriptionChannelB, STREAM_ID))
            {
                final ReplayMerge replayMergeA = new ReplayMerge(
                    subA, aeronArchiveA, replayChannelA, replayDestinationA, liveDestination, recordingIdA, 0);
                final ReplayMerge replayMergeB = new ReplayMerge(
                    subB, aeronArchiveB, replayChannelB, replayDestinationB, liveDestination, recordingIdB, 0);
                final List<InProcessMerge> merges = Arrays.asList(
                    new InProcessMerge("A", replayMergeA), new InProcessMerge("B", replayMergeB));

                if (concurrent)
                {
                    processConcurrently(merges, numMessages);
                }
                else
                {
                    processSequentially(merges, numMessages);
                }
            }

            aeronArchiveA.stopRecording(subscriptionId);
        }
    }

    private static void processConcurrently(final List<InProcessMerge> merges, final int numMessages)
    {
        boolean allComplete;
        do
        {
            allComplete = true;

            for (final InProcessMerge merge : merges)
            {
                try
                {
                    final int workCount = merge.doWork();
                    if (0 == workCount)
                    {
                        Tests.yield();
                    }
                }
                catch (final Exception ex)
                {
                    throw new RuntimeException(
                        "replayMerge=" + merge.replayMerge.subscription().channel() + " merges=" + merges, ex);
                }
            }

            for (final InProcessMerge merge : merges)
            {
                allComplete &= merge.isComplete(numMessages);
            }
        }
        while (!allComplete);
    }

    private static void processSequentially(final List<InProcessMerge> merges, final int numMessages)
    {
        for (final InProcessMerge merge : merges)
        {
            try
            {
                while (!merge.isComplete(numMessages))
                {
                    final int workCount = merge.doWork();
                    if (0 == workCount)
                    {
                        Tests.yield();
                    }
                }
            }
            catch (final Exception ex)
            {
                throw new RuntimeException(
                    "replayMerge=" + merge.replayMerge.subscription().channel() + " merges=" + merges, ex);
            }
        }
    }

    private void sendMessages(final Publication pubA, final int numMessages)
    {
        final DirectBuffer buffer = new UnsafeBuffer("this is a test message".getBytes(US_ASCII));
        for (int i = 0; i < numMessages; i++)
        {
            while (0 > pubA.offer(buffer))
            {
                Tests.yield();
            }
        }
    }

    private static final class InProcessMerge
    {
        private final String name;
        private final ReplayMerge replayMerge;
        private int messageCount = 0;

        private InProcessMerge(final String name, final ReplayMerge replayMerge)
        {
            this.name = name;
            this.replayMerge = replayMerge;
        }

        boolean isComplete(final int totalMessages)
        {
            return replayMerge.isMerged() && totalMessages <= messageCount;
        }

        int doWork()
        {
            final int fragments = replayMerge.poll((buffer, offset, length, header) -> {}, 1);
            messageCount += fragments;
            return fragments;
        }

        @Override
        public String toString()
        {
            return "InProcessMerge{" +
                "name=" + name +
                ", messageCount=" + messageCount +
                '}';
        }
    }
}
