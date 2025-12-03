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

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.IpTables;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.TopologyTest;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.collections.MutableInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@TopologyTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
@EnabledOnOs(OS.LINUX)
class ClusterNetworkPartitionTest
{
    private static final List<String> HOSTNAMES =
        List.of("127.1.0.0", "127.1.1.0", "127.1.2.0", "127.1.3.0", "127.1.4.0");
    private static final int CLUSTER_SIZE = HOSTNAMES.size();
    private static final String CHAIN_NAME = "CLUSTER-TEST";

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionMessageHeaderDecoder sessionHeaderDecoder = new SessionMessageHeaderDecoder();
    private TestCluster cluster;

    @BeforeEach
    void setUp()
    {
        IpTables.setupChain(CHAIN_NAME);
    }

    @AfterEach
    void tearDown()
    {
        IpTables.tearDownChain(CHAIN_NAME);
    }

    @Test
    @InterruptAfter(30)
    void shouldStartClusterThenElectNewLeaderAfterPartition()
    {
        cluster = aCluster()
            .withStaticNodes(CLUSTER_SIZE)
            .withCustomAddresses(HOSTNAMES)
            .withClusterId(7)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();

        cluster.connectClient();
        cluster.sendAndAwaitMessages(100);

        final long initialLeaderLogPosition = firstLeader.appendPosition();

        IpTables.makeSymmetricNetworkPartition(
            CHAIN_NAME,
            List.of(HOSTNAMES.get(firstLeader.memberId())),
            IntStream.range(0, CLUSTER_SIZE)
                .filter((i) -> i != firstLeader.memberId())
                .mapToObj(HOSTNAMES::get)
                .toList());

        cluster.sendMessages(50); // will be sent to the old leader
        Tests.await(() -> firstLeader.appendPosition() > initialLeaderLogPosition);

        final TestNode interimLeader = cluster.awaitLeaderWithoutElectionTerminationCheck(firstLeader.memberId());
        assertNotEquals(firstLeader.memberId(), interimLeader.memberId());

        cluster.awaitNodeState(firstLeader, (n) -> n.electionState() == ElectionState.CANVASS);

        IpTables.flushChain(CHAIN_NAME);

        final TestNode finalLeader = cluster.awaitLeader(); // ensure no more elections
        assertNotEquals(firstLeader.memberId(), finalLeader.memberId());

        cluster.reconnectClient();
        cluster.sendAndAwaitMessages(100, 200);
    }

    @Test
    @InterruptAfter(30)
    void shouldRestartClusterWithMajorityNodesBeingSlow()
    {
        cluster = aCluster()
            .withStaticNodes(CLUSTER_SIZE)
            .withCustomAddresses(HOSTNAMES)
            .withClusterId(7)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        final TestNode fastFollower = followers.get(0);
        final TestNode[] slowFollowers = followers.subList(1, followers.size()).toArray(new TestNode[0]);
        final ClusterMember[] clusterMembers =
            ClusterMember.parse(firstLeader.consensusModule().context().clusterMembers());

        cluster.connectClient();
        final int initialMessageCount = 100;
        cluster.sendAndAwaitMessages(initialMessageCount);

        cluster.takeSnapshot(firstLeader);
        cluster.awaitSnapshotCount(1);

        final int messagesAfterSnapshot = 50;
        final int committedMessageCount = initialMessageCount + messagesAfterSnapshot;
        cluster.sendAndAwaitMessages(messagesAfterSnapshot, committedMessageCount);

        final long commitPositionBeforePartition = firstLeader.commitPosition();

        blockTraffic(
            List.of(clusterMembers[firstLeader.memberId()]),
            Stream.of(slowFollowers)
                .map((node) -> clusterMembers[node.memberId()])
                .toList(),
            ClusterMember::logEndpoint);

        final int messagesReceivedByMinority = 300;
        cluster.sendMessages(messagesReceivedByMinority); // these messages will be only received by 2 out of 5 nodes

        final long leaderAppendPosition =
            awaitLeaderLogRecording(firstLeader, committedMessageCount + messagesReceivedByMinority);

        Tests.await(() -> leaderAppendPosition == fastFollower.appendPosition());

        verifyUncommittedMessagesNotProcessed(commitPositionBeforePartition, committedMessageCount);

        cluster.terminationsExpected(true);
        cluster.stopAllNodes();

        IpTables.flushChain(CHAIN_NAME); // remove network partition

        cluster.restartAllNodes(false);
        cluster.awaitLeader();
        cluster.reconnectClient();

        final int newMessages = 200;
        cluster.sendMessages(newMessages);
        cluster.awaitResponseMessageCount(newMessages);
        cluster.awaitServicesMessageCount(committedMessageCount + messagesReceivedByMinority + newMessages);
    }

    @Test
    @InterruptAfter(30)
    void shouldNotAllowLogReplayBeyondCommitPosition()
    {
        cluster = aCluster()
            .withStaticNodes(CLUSTER_SIZE)
            .withCustomAddresses(HOSTNAMES)
            .withClusterId(7)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        final TestNode fastFollower = followers.get(0);
        final TestNode[] slowFollowers = followers.subList(1, followers.size()).toArray(new TestNode[0]);
        final ClusterMember[] clusterMembers =
            ClusterMember.parse(leader.consensusModule().context().clusterMembers());

        cluster.connectClient();
        final int initialMessageCount = 100;
        cluster.sendAndAwaitMessages(initialMessageCount);

        final long commitPositionBeforePartition = leader.commitPosition();

        // block log traffic from leader to the slow nodes
        blockTraffic(
            List.of(clusterMembers[leader.memberId()]),
            Stream.of(slowFollowers)
                .map((node) -> clusterMembers[node.memberId()])
                .toList(),
            ClusterMember::logEndpoint);

        final int messagesReceivedByMinority = 300;
        cluster.sendMessages(messagesReceivedByMinority); // these messages will be only received by 2 out of 5 nodes

        final long leaderAppendPosition =
            awaitLeaderLogRecording(leader, initialMessageCount + messagesReceivedByMinority);

        Tests.await(() -> leaderAppendPosition == fastFollower.appendPosition());

        // restart follower to force an election, i.e. go through `FOLLOWER_REPLAY`
        fastFollower.isTerminationExpected(true);
        fastFollower.close();
        final TestNode fastFollowerRestarted = cluster.startStaticNode(fastFollower.memberId(), false);
        TestCluster.awaitElectionClosed(fastFollowerRestarted);

        verifyUncommittedMessagesNotProcessed(commitPositionBeforePartition, initialMessageCount);
    }

    private long awaitLeaderLogRecording(final TestNode leader, final int expectedMessageCount)
    {
        final long firstLeaderLogRecordingId =
            RecordingPos.getRecordingId(leader.mediaDriver().counters(), leader.logRecordingCounterId());

        // await leader to record all ingress messages
        try (AeronArchive aeronArchive = AeronArchive.connect(new AeronArchive.Context()
            .clientName("test")
            .aeronDirectoryName(cluster.startClientMediaDriver().aeronDirectoryName())
            .controlRequestChannel(leader.archive().context().controlChannel())
            .controlRequestStreamId(leader.archive().context().controlStreamId())
            .controlResponseChannel("aeron:udp?endpoint=localhost:0")))
        {
            final Aeron aeron = aeronArchive.context().aeron();
            final String replayChannel = "aeron:udp?endpoint=localhost:18181";
            final int replayStreamId = 1111;
            final long replaySubscriptionId = aeronArchive.startReplay(
                firstLeaderLogRecordingId, 0, AeronArchive.REPLAY_ALL_AND_FOLLOW, replayChannel, replayStreamId);
            final int sessionId = (int)replaySubscriptionId;
            final Subscription subscription =
                aeron.addSubscription(ChannelUri.addSessionId(replayChannel, sessionId), replayStreamId);
            Tests.awaitConnected(subscription);

            final Image image = subscription.imageBySessionId(sessionId);
            assertNotNull(image);
            final MutableInteger messageCount = new MutableInteger();
            final FragmentHandler fragmentHandler = (buffer, offset, length, header) ->
            {
                messageHeaderDecoder.wrap(buffer, offset);
                if (MessageHeaderDecoder.SCHEMA_ID == messageHeaderDecoder.schemaId() &&
                    SessionMessageHeaderDecoder.TEMPLATE_ID == messageHeaderDecoder.templateId())
                {
                    sessionHeaderDecoder.wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.blockLength(),
                        messageHeaderDecoder.version());
                    messageCount.increment();
                }
            };

            while (messageCount.get() < expectedMessageCount)
            {
                if (0 == image.poll(fragmentHandler, 100))
                {
                    Tests.yield();
                }
            }

            final long position = image.position();

            subscription.close();
            aeronArchive.stopReplay(replaySubscriptionId);

            return position;
        }
    }

    private static void blockTraffic(
        final List<ClusterMember> from,
        final List<ClusterMember> to,
        final Function<ClusterMember, String> endpointFunction)
    {
        for (final ClusterMember dest : to)
        {
            final String blockedEndpoint = endpointFunction.apply(dest);
            final String blockedPort = blockedEndpoint.substring(blockedEndpoint.indexOf(':') + 1);
            for (final ClusterMember src : from)
            {
                IpTables.dropUdpTrafficBetweenHosts(
                    CHAIN_NAME, HOSTNAMES.get(src.id()), "", HOSTNAMES.get(dest.id()), blockedPort);
            }
        }
    }

    private void verifyUncommittedMessagesNotProcessed(
        final long expectedCommitPosition, final int expectedCommittedMessageCount)
    {
        final TestNode leader = cluster.findLeader();
        assertEquals(expectedCommitPosition, leader.commitPosition(), "[leader] invalid commit position");
        assertEquals(expectedCommittedMessageCount, leader.service().messageCount(), "[leader] invalid message count");
        for (final TestNode follower : cluster.followers())
        {
            assertEquals(expectedCommitPosition, follower.commitPosition(), "[follower] invalid commit position");
            assertEquals(
                expectedCommittedMessageCount, follower.service().messageCount(), "[follower] invalid message count");
        }
    }
}
