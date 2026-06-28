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
import io.aeron.Counter;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ReplayParams;
import io.aeron.driver.MediaDriver;
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
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;

import static io.aeron.CommonContext.RESPONSE_CORRELATION_ID_PARAM_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class ArchiveResponseClientTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

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
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true);

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
        CloseHelper.closeAll(archive, driver);
    }

    @Test
    @InterruptAfter(10)
    void shouldReplayUsingResponseChannel()
    {
        final AeronArchive.Context aeronArchiveCtx = new AeronArchive.Context()
            .controlRequestChannel(archive.context().controlChannel())
            .controlResponseChannel("aeron:udp?control-mode=response|control=localhost:10002");
        try (AeronArchive aeronArchive = AeronArchive.connect(aeronArchiveCtx))
        {
            final ArchiveSystemTests.RecordingResult recordingResult = ArchiveSystemTests.recordData(aeronArchive);

            final Subscription replay = aeronArchive.replay(
                recordingResult.recordingId(), "aeron:udp?control-mode=response|control=localhost:10002", 10001,
                new ReplayParams());

            final MutableLong replayPosition = new MutableLong();
            while (replayPosition.get() < recordingResult.position())
            {
                if (0 == replay.poll((buffer, offset, length, header) -> replayPosition.set(header.position()), 10))
                {
                    Tests.yield();
                }
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldBoundedReplayUsingResponseChannel()
    {
        final AeronArchive.Context aeronArchiveCtx = new AeronArchive.Context()
            .controlRequestChannel(archive.context().controlChannel())
            .controlResponseChannel("aeron:udp?control-mode=response|control=localhost:10002");
        try (AeronArchive aeronArchive = AeronArchive.connect(aeronArchiveCtx))
        {
            final ArchiveSystemTests.RecordingResult recordingResult = ArchiveSystemTests.recordData(aeronArchive);
            final Counter testBoundedCounter = aeronArchive.context().aeron().addCounter(10001, "test bounded counter");
            testBoundedCounter.set(recordingResult.halfwayPosition());

            final ReplayParams replayParams = new ReplayParams();
            replayParams.boundingLimitCounterId(testBoundedCounter.id());

            final Subscription replay = aeronArchive.replay(
                recordingResult.recordingId(),
                "aeron:udp?control-mode=response|control=localhost:10002",
                10001,
                replayParams);

            final MutableLong replayPosition = new MutableLong();
            while (replayPosition.get() < recordingResult.halfwayPosition())
            {
                if (0 == replay.poll((buffer, offset, length, header) -> replayPosition.set(header.position()), 10))
                {
                    Tests.yield();
                }
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldStartReplayUsingResponseChannel()
    {
        final String responseChannel = "aeron:udp?control-mode=response|control=localhost:10002";
        final int replayStreamId = 10001;

        final AeronArchive.Context aeronArchiveCtx = new AeronArchive.Context()
            .controlRequestChannel(archive.context().controlChannel())
            .controlResponseChannel(responseChannel);
        try (AeronArchive aeronArchive = AeronArchive.connect(aeronArchiveCtx);
            Subscription replay = aeronArchive.context().aeron().addSubscription(responseChannel, replayStreamId))
        {
            final ArchiveSystemTests.RecordingResult recordingResult = ArchiveSystemTests.recordData(aeronArchive);
            final ReplayParams replayParams = new ReplayParams();
            replayParams.subscriptionRegistrationId(replay.registrationId());

            aeronArchive.startReplay(recordingResult.recordingId(), responseChannel, replayStreamId, replayParams);

            final MutableLong replayPosition = new MutableLong();
            while (replayPosition.get() < recordingResult.position())
            {
                if (0 == replay.poll((buffer, offset, length, header) -> replayPosition.set(header.position()), 10))
                {
                    Tests.yield();
                }
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldStartBoundedReplayUsingResponseChannel()
    {
        final String responseChannel = "aeron:udp?control-mode=response|control=localhost:10002";
        final int replayStreamId = 10001;

        final AeronArchive.Context aeronArchiveCtx = new AeronArchive.Context()
            .controlRequestChannel(archive.context().controlChannel())
            .controlResponseChannel(responseChannel);

        try (AeronArchive aeronArchive = AeronArchive.connect(aeronArchiveCtx);
            Subscription replay = aeronArchive.context().aeron().addSubscription(responseChannel, replayStreamId))
        {
            final ArchiveSystemTests.RecordingResult recordingResult = ArchiveSystemTests.recordData(aeronArchive);
            final Counter testBoundedCounter = aeronArchive.context().aeron().addCounter(10001, "test bounded counter");
            testBoundedCounter.set(recordingResult.halfwayPosition());

            final ReplayParams replayParams = new ReplayParams();
            replayParams
                .boundingLimitCounterId(testBoundedCounter.id())
                .subscriptionRegistrationId(replay.registrationId());

            aeronArchive.startReplay(recordingResult.recordingId(), responseChannel, replayStreamId, replayParams);

            final MutableLong replayPosition = new MutableLong();
            while (replayPosition.get() < recordingResult.halfwayPosition())
            {
                if (0 == replay.poll((buffer, offset, length, header) -> replayPosition.set(header.position()), 10))
                {
                    Tests.yield();
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:udp?control-mode=response|control=localhost:10002",
        "aeron:udp?endpoint=localhost:10002"
    })
    @InterruptAfter(10)
    void shouldAsyncConnectUsingResponseChannel(final String responseChannel)
    {
        final AeronArchive.Context aeronArchiveCtx = new AeronArchive.Context()
            .controlRequestChannel(archive.context().controlChannel())
            .controlResponseChannel(responseChannel);
        try (AeronArchive.AsyncConnect asyncConnect = AeronArchive.asyncConnect(aeronArchiveCtx))
        {
            AeronArchive aeronArchive;
            while (null == (aeronArchive = asyncConnect.poll()))
            {
                Tests.yield();
            }

            try
            {
                assertNotEquals(Aeron.NULL_VALUE, aeronArchive.controlSessionId());
                assertEquals(archive.context().archiveId(), aeronArchive.archiveId());
            }
            finally
            {
                CloseHelper.close(aeronArchive);
            }
        }
    }

    @Test
    void shouldConnectUsingLocalIpcResponseChannels()
    {
        final AeronArchive.Context context = new AeronArchive.Context()
            .controlRequestChannel(archive.context().localControlChannel())
            .controlRequestStreamId(archive.context().localControlStreamId())
            .controlResponseChannel("aeron:ipc?control-mode=response");
        try (AeronArchive aeronArchive = AeronArchive.connect(context))
        {
            final ChannelUri requestChannel = ChannelUri.parse(context.controlRequestChannel());
            assertEquals(
                Long.toString(aeronArchive.controlResponsePoller().subscription().registrationId()),
                requestChannel.get(RESPONSE_CORRELATION_ID_PARAM_NAME));
        }
    }
}
