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
import io.aeron.agent.EventCodeType;
import io.aeron.agent.EventConfiguration;
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
import io.aeron.test.cluster.ClusterTests;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.MessageHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.EnumSet;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

import static io.aeron.agent.ClusterEventCode.APPEND_SESSION_CLOSE;
import static io.aeron.agent.ClusterEventCode.APPEND_SESSION_OPEN;
import static io.aeron.agent.ClusterEventCode.CLUSTER_SESSION_STATE_CHANGE;
import static io.aeron.agent.ClusterEventCode.ELECTION_STATE_CHANGE;
import static io.aeron.agent.ClusterEventCode.NEW_ELECTION;
import static io.aeron.agent.ClusterEventCode.ROLE_CHANGE;
import static io.aeron.agent.ClusterEventCode.STATE_CHANGE;
import static io.aeron.agent.CommonEventEncoder.LOG_HEADER_LENGTH;
import static io.aeron.agent.EventConfiguration.EVENT_READER_FRAME_LIMIT;
import static io.aeron.agent.EventConfiguration.EVENT_RING_BUFFER;
import static java.util.Collections.synchronizedSet;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.mockito.Mockito.mock;

@ExtendWith(InterruptingTestCallback.class)
public class ClusterLoggingAgentTest
{
    private static final Set<ClusterEventCode> WAIT_LIST = synchronizedSet(EnumSet.noneOf(ClusterEventCode.class));
    private static final String READER_CLASSNAME = "aeron.event.log.reader.classname";
    private static final String ENABLED_CLUSTER_EVENT_CODES = "aeron.event.cluster.log";
    private File testDir;
    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(clusteredMediaDriver.consensusModule(), container, clusteredMediaDriver);
//        AgentTests.stopLogging();

        if (testDir != null && testDir.exists())
        {
            IoUtil.delete(testDir, false);
        }
    }

    @Test
    @InterruptAfter(20)
    void logAll()
    {
        testClusterEventsLogging(
            "all",
            EnumSet.of(
                ROLE_CHANGE,
                STATE_CHANGE,
                ELECTION_STATE_CHANGE,
                NEW_ELECTION,
                APPEND_SESSION_OPEN,
                APPEND_SESSION_CLOSE,
                CLUSTER_SESSION_STATE_CHANGE));
    }

    @Test
    @InterruptAfter(20)
    void logRoleChange()
    {
        testClusterEventsLogging(ROLE_CHANGE.name(), EnumSet.of(ROLE_CHANGE));
    }

    @Test
    @InterruptAfter(20)
    void logStateChange()
    {
        testClusterEventsLogging(STATE_CHANGE.name(), EnumSet.of(STATE_CHANGE));
    }

    @Test
    @InterruptAfter(20)
    void logElectionStateChange()
    {
        testClusterEventsLogging(ELECTION_STATE_CHANGE.name(), EnumSet.of(ELECTION_STATE_CHANGE));
    }

    private void testClusterEventsLogging(
        final String enabledEvents, final EnumSet<ClusterEventCode> expectedEvents)
    {
        before(enabledEvents, expectedEvents);

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

        Tests.await(WAIT_LIST::isEmpty);
    }

    private void before(final String enabledEvents, final EnumSet<ClusterEventCode> expectedEvents)
    {
        final Properties configOptions = new Properties();
        configOptions.put(READER_CLASSNAME, StubEventLogReaderAgent.class.getName());
        configOptions.put(ENABLED_CLUSTER_EVENT_CODES, enabledEvents);
        EventConfiguration.restartReader(configOptions);
        WAIT_LIST.clear();
        WAIT_LIST.addAll(expectedEvents);

        testDir = new File(IoUtil.tmpDirName(), "cluster-test");
        if (testDir.exists())
        {
            IoUtil.delete(testDir, false);
        }
    }

    public static final class StubEventLogReaderAgent implements Agent, MessageHandler
    {
        public StubEventLogReaderAgent()
        {
        }

        public String roleName()
        {
            return "event-log-reader";
        }

        public int doWork()
        {
            return EVENT_RING_BUFFER.read(this, EVENT_READER_FRAME_LIMIT);
        }

        public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
        {
            final int eventCodeTypeId = msgTypeId >> 16;
            if (EventCodeType.CLUSTER.getTypeCode() != eventCodeTypeId)
            {
                return;
            }

            final ClusterEventCode eventCode = ClusterEventCode.fromEventCodeId(msgTypeId);
            switch (eventCode)
            {
                case ROLE_CHANGE:
                {
                    final int offset = index + LOG_HEADER_LENGTH + SIZE_OF_INT;
                    final String role = buffer.getStringAscii(offset);
                    if (role.contains("LEADER"))
                    {
                        WAIT_LIST.remove(eventCode);
                    }
                    break;
                }

                case STATE_CHANGE:
                {
                    final int offset = index + LOG_HEADER_LENGTH + SIZE_OF_INT;
                    final String state = buffer.getStringAscii(offset);
                    if (state.contains("ACTIVE"))
                    {
                        WAIT_LIST.remove(eventCode);
                    }
                    break;
                }

                case ELECTION_STATE_CHANGE:
                {
                    final int offset = index + LOG_HEADER_LENGTH + 2 * SIZE_OF_INT + 6 * SIZE_OF_LONG;
                    final String state = buffer.getStringAscii(offset);
                    if (state.contains("CLOSED"))
                    {
                        WAIT_LIST.remove(eventCode);
                    }
                    break;
                }

                case APPEND_SESSION_CLOSE:
                {
                    final int offset = index + LOG_HEADER_LENGTH + 3 * SIZE_OF_LONG + SIZE_OF_INT;
                    final String closeReason = buffer.getStringAscii(offset);
                    if ("CLIENT_ACTION".equals(closeReason))
                    {
                        WAIT_LIST.remove(eventCode);
                    }
                    break;
                }

                case APPEND_SESSION_OPEN:
                {
                    final int offset = index + LOG_HEADER_LENGTH + SIZE_OF_LONG;
                    if (buffer.getLong(offset + SIZE_OF_LONG) > buffer.getLong(offset))
                    {
                        WAIT_LIST.remove(eventCode);
                    }
                    break;
                }

                case CLUSTER_SESSION_STATE_CHANGE:
                {
                    final int offset = index + LOG_HEADER_LENGTH + SIZE_OF_LONG + SIZE_OF_INT;
                    final String action = buffer.getStringAscii(offset);
                    final String stateTransition = buffer.getStringAscii(offset + SIZE_OF_INT + action.length());
                    final String reason = buffer.getStringAscii(
                        offset + SIZE_OF_INT + action.length() + SIZE_OF_INT + stateTransition.length());
                    if ("CLIENT_ACTION".equals(reason))
                    {
                        WAIT_LIST.remove(eventCode);
                    }
                    break;
                }

                default:
                    WAIT_LIST.remove(eventCode);
                    break;
            }
        }
    }
}
