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
package io.aeron.cluster;

import io.aeron.CommonContext;
import io.aeron.Counter;
import io.aeron.agent.ClusterEventCode;
import io.aeron.agent.EventConfiguration;
import io.aeron.agent.EventReaderManager;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.agent.CountingEventReaderAgent;
import io.aeron.test.cluster.ClusterTests;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.Agent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Supplier;

import static io.aeron.agent.ClusterEventCode.APPEND_SESSION_CLOSE;
import static io.aeron.agent.ClusterEventCode.APPEND_SESSION_OPEN;
import static io.aeron.agent.ClusterEventCode.CLUSTER_SESSION_STATE_CHANGE;
import static io.aeron.agent.ClusterEventCode.ELECTION_STATE_CHANGE;
import static io.aeron.agent.ClusterEventCode.NEW_ELECTION;
import static io.aeron.agent.ClusterEventCode.ROLE_CHANGE;
import static io.aeron.agent.ClusterEventCode.STATE_CHANGE;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;

@ExtendWith(InterruptingTestCallback.class)
public class ClusterLoggingAgentTest
{
    private File testDir;
    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;

    @BeforeEach
    void setUp()
    {
        assumeTrue("all".equals(System.getProperty(CommonContext.CLUSTER_EVENT_LOG)));
        final String readerClass = System.getProperty(EventReaderManager.READER_CLASSNAME);
        assumeTrue(CountingEventReaderAgent.class.getName().equals(readerClass));
    }

    @AfterEach
    void after()
    {
        if (null != clusteredMediaDriver)
        {
            CloseHelper.closeAll(clusteredMediaDriver.consensusModule(), container, clusteredMediaDriver);
        }

        if (testDir != null && testDir.exists())
        {
            IoUtil.delete(testDir, false);
        }
    }

    @Test
    @InterruptAfter(20)
    void logAll()
    {
        testDir = new File(IoUtil.tmpDirName(), "cluster-test");
        if (testDir.exists())
        {
            IoUtil.delete(testDir, false);
        }

        final Context mediaDriverCtx = new Context()
            .errorHandler(Tests::onError)
            .dirDeleteOnStart(true)
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .threadingMode(ThreadingMode.SHARED);

        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context()
            .aeronDirectoryName(mediaDriverCtx.aeronDirectoryName())
            .controlRequestChannel("aeron:ipc?term-length=64k")
            .controlRequestStreamId(AeronArchive.Configuration.localControlStreamId())
            .controlResponseChannel("aeron:ipc?term-length=64k")
            .controlResponseStreamId(AeronArchive.Configuration.localControlStreamId() + 1)
            .controlResponseStreamId(101);

        final Archive.Context archiveCtx = TestContexts.localhostArchive()
            .aeronDirectoryName(mediaDriverCtx.aeronDirectoryName())
            .errorHandler(Tests::onError)
            .archiveDir(new File(testDir, "archive"))
            .deleteArchiveOnStart(true)
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED);

        final ConsensusModule.Context consensusModuleCtx = new ConsensusModule.Context()
            .aeronDirectoryName(mediaDriverCtx.aeronDirectoryName())
            .errorHandler(ClusterTests.errorHandler(0))
            .clusterDir(new File(testDir, "consensus-module"))
            .archiveContext(aeronArchiveContext.clone())
            .clusterMemberId(0)
            .clusterMembers("0,localhost:20110,localhost:20220,localhost:20330,localhost:20440,localhost:8010")
            .logChannel("aeron:udp?term-length=256k|control-mode=manual|control=localhost:20550")
            .ingressChannel("aeron:udp?term-length=64k")
            .replicationChannel("aeron:udp?endpoint=localhost:0");

        final ClusteredService clusteredService = mock(ClusteredService.class);
        final ClusteredServiceContainer.Context clusteredServiceCtx = new ClusteredServiceContainer.Context()
            .aeronDirectoryName(mediaDriverCtx.aeronDirectoryName())
            .errorHandler(ClusterTests.errorHandler(0))
            .archiveContext(aeronArchiveContext.clone())
            .clusterDir(new File(testDir, "service"))
            .clusteredService(clusteredService);

        clusteredMediaDriver = ClusteredMediaDriver.launch(mediaDriverCtx, archiveCtx, consensusModuleCtx);
        container = ClusteredServiceContainer.launch(clusteredServiceCtx);

        final Counter state = clusteredMediaDriver.consensusModule().context().electionStateCounter();

        final Supplier<String> message = () -> ElectionState.get(state).toString();
        while (ElectionState.CLOSED != ElectionState.get(state))
        {
            Tests.sleep(1, message);
        }

        final AeronCluster aeronCluster = AeronCluster.connect(new AeronCluster.Context()
            .aeronDirectoryName(mediaDriverCtx.aeronDirectoryName())
            .ingressChannel("aeron:udp")
            .ingressEndpoints("0=localhost:20110")
            .egressChannel("aeron:udp?term-length=256k|endpoint=localhost:0"));

        aeronCluster.close();

        verifyExpectedEvents(EnumSet.of(
            ROLE_CHANGE,
            STATE_CHANGE,
            ELECTION_STATE_CHANGE,
            NEW_ELECTION,
            APPEND_SESSION_OPEN,
            APPEND_SESSION_CLOSE,
            CLUSTER_SESSION_STATE_CHANGE));
    }

    private static void verifyExpectedEvents(final EnumSet<ClusterEventCode> expectedEvents)
    {
        final Agent agent = EventConfiguration.eventReader().agent();
        Assertions.assertInstanceOf(CountingEventReaderAgent.class, agent);
        final CountingEventReaderAgent countingAgent = (CountingEventReaderAgent)agent;

        final List<ClusterEventCode> pendingList = new ArrayList<>(expectedEvents);

        final Supplier<String> errorMessage = () -> "Pending events: " + pendingList;
        while (!pendingList.isEmpty())
        {
            pendingList.removeIf(code -> 0 < countingAgent.countClusterEvent(code.toEventCodeId()));
            Tests.sleep(1, errorMessage);
        }
    }
}
