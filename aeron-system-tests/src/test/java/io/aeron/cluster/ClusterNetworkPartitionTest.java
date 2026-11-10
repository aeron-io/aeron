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

import io.aeron.cluster.service.Cluster;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.IpTables;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.TopologyTest;
import io.aeron.test.cluster.ClusterTests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.test.cluster.TestCluster.aCluster;
import static io.aeron.test.cluster.TestCluster.awaitElectionClosed;
import static io.aeron.test.cluster.TestCluster.awaitElectionState;
import static io.aeron.test.cluster.TestCluster.awaitLeaderLogRecording;
import static org.agrona.BitUtil.align;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

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
    void shouldRestartClusterWithMajorityOfNodesBeingBehind()
    {
        cluster = aCluster()
            .withStaticNodes(CLUSTER_SIZE)
            .withCustomAddresses(HOSTNAMES)
            .withClusterId(7)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
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

        blockTrafficToSpecificEndpoint(
            List.of(clusterMembers[firstLeader.memberId()]),
            followers.stream()
                .map((node) -> clusterMembers[node.memberId()])
                .toList(),
            ClusterMember::logEndpoint);

        final int messagesReceivedByMinority = 300;
        cluster.sendMessages(messagesReceivedByMinority);

        awaitLeaderLogRecording(cluster, firstLeader, committedMessageCount + messagesReceivedByMinority);

        verifyClusterState(commitPositionBeforePartition, committedMessageCount);

        cluster.terminationsExpected(true);
        cluster.stopAllNodes();

        IpTables.flushChain(CHAIN_NAME); // remove network partition

        cluster.restartAllNodes(false);
        final TestNode newLeader = cluster.awaitLeader();
        assertEquals(firstLeader.memberId(), newLeader.memberId());
        cluster.reconnectClient();

        final int newMessages = 200;
        cluster.sendMessages(newMessages);
        cluster.awaitResponseMessageCount(newMessages);
        cluster.awaitServicesMessageCount(committedMessageCount + messagesReceivedByMinority + newMessages);
    }

    @ParameterizedTest
    @ValueSource(ints = { 128 * 1024, 256 * 1024 })
    @InterruptAfter(30)
    void shouldRecoverClusterWithMajorityOfNodesBeingBehind(final int amountOfLogMajorityShouldBeBehind)
    {
        final long leaderHeartbeatTimeoutNs = TimeUnit.SECONDS.toNanos(10);
        cluster = aCluster()
            .withStaticNodes(CLUSTER_SIZE)
            .withCustomAddresses(HOSTNAMES)
            .withClusterId(7)
            .withLogChannel("aeron:udp?term-length=512k|alias=raft")
            .withLeaderHeartbeatTimeoutNs(leaderHeartbeatTimeoutNs)
            .withStartupCanvassTimeoutNs(leaderHeartbeatTimeoutNs * 2)
            .withElectionTimeoutNs(leaderHeartbeatTimeoutNs / 2)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
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

        blockTrafficToSpecificEndpoint(
            List.of(clusterMembers[firstLeader.memberId()]),
            followers.stream()
                .map((node) -> clusterMembers[node.memberId()])
                .toList(),
            ClusterMember::logEndpoint);

        final int messagesReceivedByMinority = 1 + amountOfLogMajorityShouldBeBehind / align(
            HEADER_LENGTH + SESSION_HEADER_LENGTH + ClusterTests.LARGE_MSG.length(), FRAME_ALIGNMENT);
        cluster.sendLargeMessages(messagesReceivedByMinority);

        awaitLeaderLogRecording(cluster, firstLeader, committedMessageCount + messagesReceivedByMinority);

        verifyClusterState(commitPositionBeforePartition, committedMessageCount);

        IpTables.flushChain(CHAIN_NAME); // remove network partition

        final TestNode newLeader = cluster.awaitLeader();
        assertEquals(firstLeader.memberId(), newLeader.memberId());

        final int newMessages = 200;
        cluster.sendMessages(newMessages);
        cluster.awaitResponseMessageCount(newMessages);
        cluster.awaitServicesMessageCount(committedMessageCount + messagesReceivedByMinority + newMessages);
    }

    @Test
    @InterruptAfter(30)
    void shouldNotAllowLogReplayBeyondCommitPosition()
    {
        final long leaderHeartbeatTimeoutNs = TimeUnit.SECONDS.toNanos(10);
        cluster = aCluster()
            .withStaticNodes(CLUSTER_SIZE)
            .withCustomAddresses(HOSTNAMES)
            .withClusterId(3)
            .withLogChannel("aeron:udp?term-length=512k|alias=raft")
            .withLeaderHeartbeatTimeoutNs(leaderHeartbeatTimeoutNs)
            .withStartupCanvassTimeoutNs(leaderHeartbeatTimeoutNs * 2)
            .withElectionTimeoutNs(leaderHeartbeatTimeoutNs / 2)
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
        blockTrafficToSpecificEndpoint(
            List.of(clusterMembers[leader.memberId()]),
            Stream.of(slowFollowers)
                .map((node) -> clusterMembers[node.memberId()])
                .toList(),
            ClusterMember::logEndpoint);

        final int messagesReceivedByMinority = 300;
        cluster.sendMessages(messagesReceivedByMinority); // these messages will be only received by 2 out of 5 nodes

        final int expectedFinalMessageCount = initialMessageCount + messagesReceivedByMinority;
        final long leaderAppendPosition = awaitLeaderLogRecording(cluster, leader, expectedFinalMessageCount);

        Tests.await(() -> leaderAppendPosition == fastFollower.appendPosition());

        verifyClusterState(commitPositionBeforePartition, initialMessageCount);

        // restart follower to force an election, i.e. to replay its log
        fastFollower.isTerminationExpected(true);
        fastFollower.close();
        final TestNode fastFollowerRestarted = cluster.startStaticNode(fastFollower.memberId(), false);
        awaitElectionState(fastFollowerRestarted, ElectionState.FOLLOWER_REPLAY);
        verifyNodeState(fastFollowerRestarted, commitPositionBeforePartition, initialMessageCount);

        IpTables.flushChain(CHAIN_NAME); // remove network partition
        assertSame(leader, cluster.awaitLeader());
        awaitElectionClosed(fastFollowerRestarted);

        // Once the network partition is removed the majority of nodes will receive the missing data and the commit
        // position will advance. This in turn will unblock `FOLLOWER_REPLAY` progress that is bounded by it.
        verifyClusterState(leaderAppendPosition, expectedFinalMessageCount);
    }

    @Test
    @InterruptAfter(30)
    void shouldNotAllowLogReplayBeyondCommitPositionAfterLeadershipTermChange()
    {
        cluster = aCluster()
            .withStaticNodes(CLUSTER_SIZE)
            .withCustomAddresses(HOSTNAMES)
            .withClusterId(7)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode originalLeader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        final TestNode fastFollower = followers.get(0);
        final TestNode[] slowFollowers = followers.subList(1, followers.size()).toArray(new TestNode[0]);
        final ClusterMember[] clusterMembers =
            ClusterMember.parse(originalLeader.consensusModule().context().clusterMembers());

        cluster.connectClient();
        final int initialMessageCount = 100;
        cluster.sendAndAwaitMessages(initialMessageCount);

        // block log traffic from leader to the slow nodes
        final List<ClusterMember> leaderMember = List.of(clusterMembers[originalLeader.memberId()]);
        final List<ClusterMember> majorityMembers = Stream.of(slowFollowers)
            .map((node) -> clusterMembers[node.memberId()])
            .toList();
        blockTrafficToSpecificEndpoint(leaderMember, majorityMembers, ClusterMember::logEndpoint);

        final int messagesReceivedByMinority = 300;
        cluster.sendMessages(messagesReceivedByMinority); // these messages will be only received by 2 out of 5 nodes

        final long leaderAppendPosition =
            awaitLeaderLogRecording(cluster, originalLeader, initialMessageCount + messagesReceivedByMinority);

        Tests.await(() -> leaderAppendPosition == fastFollower.appendPosition());

        // stop fast follower
        fastFollower.isTerminationExpected(true);
        fastFollower.close();

        // force the majority of nodes to elect a new leader
        blockTrafficToSpecificEndpoint(leaderMember, majorityMembers, ClusterMember::consensusEndpoint);
        final TestNode majorityLeader = cluster.awaitLeaderWithoutElectionTerminationCheck(originalLeader.memberId());
        assertNotEquals(originalLeader.memberId(), majorityLeader.memberId());

        IpTables.flushChain(CHAIN_NAME); // remove network partition

        // wait for old leader to become a follower
        assertSame(majorityLeader, cluster.awaitLeader());
        assertEquals(Cluster.Role.FOLLOWER, originalLeader.role());

        // restart sleeping node in new term
        final TestNode fastFollowerRestarted = cluster.startStaticNode(fastFollower.memberId(), false);
        awaitElectionClosed(fastFollowerRestarted);
        assertEquals(Cluster.Role.FOLLOWER, fastFollowerRestarted.role());

        final long commitPositionInNewTerm = majorityLeader.commitPosition();
        verifyClusterState(commitPositionInNewTerm, initialMessageCount);
    }

    @Test
    @InterruptAfter(30)
    void shouldNotStuckInFollowerCatchup()
    {
        final long leaderHeartbeatTimeoutNs = TimeUnit.SECONDS.toNanos(10);
        final List<String> hosts = HOSTNAMES.subList(0, 3);
        cluster = aCluster()
            .withStaticNodes(hosts.size())
            .withCustomAddresses(hosts)
            .withClusterId(0)
            .withLeaderHeartbeatTimeoutNs(leaderHeartbeatTimeoutNs)
            .withStartupCanvassTimeoutNs(leaderHeartbeatTimeoutNs * 2)
            .withElectionTimeoutNs(leaderHeartbeatTimeoutNs / 2)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        final TestNode follower1 = followers.get(0);
        final TestNode follower2 = followers.get(1);
        final ClusterMember[] clusterMembers =
            ClusterMember.parse(leader.consensusModule().context().clusterMembers());

        cluster.connectClient();
        final int initialMessageCount = 100;
        cluster.sendAndAwaitMessages(initialMessageCount);

        blockTrafficToSpecificEndpoint(
            List.of(clusterMembers[leader.memberId()]),
            List.of(clusterMembers[follower2.memberId()]),
            ClusterMember::logEndpoint);

        // stop a follower for it to fall behind
        follower1.isTerminationExpected(true);
        follower1.close();

        final int numMessagesAfterPartition = 111;
        cluster.sendMessages(numMessagesAfterPartition); // only leader will receive these messages

        final long leaderAppendPosition =
            awaitLeaderLogRecording(cluster, leader, initialMessageCount + numMessagesAfterPartition);

        final TestNode follower1Restarted = cluster.startStaticNode(follower1.memberId(), false);
        awaitElectionState(follower1Restarted, ElectionState.FOLLOWER_CATCHUP_AWAIT);

        follower2.isTerminationExpected(true);
        follower2.close();

        IpTables.flushChain(CHAIN_NAME); // remove network partition

        final TestNode follower2Restarted = cluster.startStaticNode(follower2.memberId(), false);
        awaitElectionState(follower2Restarted, ElectionState.FOLLOWER_CATCHUP_AWAIT);

        // wait for election to be complete
        assertSame(leader, cluster.awaitLeader());
        assertEquals(Cluster.Role.LEADER, leader.role());
        awaitElectionClosed(follower1Restarted);
        awaitElectionClosed(follower2Restarted);

        verifyClusterState(leaderAppendPosition, initialMessageCount + numMessagesAfterPartition);
    }

    private static void blockTrafficToSpecificEndpoint(
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

    private void verifyClusterState(
        final long expectedCommitPosition, final int expectedCommittedMessageCount)
    {
        final TestNode leader = cluster.findLeader();
        verifyNodeState(leader, expectedCommitPosition, expectedCommittedMessageCount);
        for (final TestNode follower : cluster.followers())
        {
            verifyNodeState(follower, expectedCommitPosition, expectedCommittedMessageCount);
        }
    }

    private static void verifyNodeState(
        final TestNode node, final long expectedCommitPosition, final int expectedCommittedMessageCount)
    {
        Tests.await(() -> node.service().messageCount() >= expectedCommittedMessageCount);

        final Supplier<String> errMsg = () ->
            "memberId=" + node.memberId() +
                " role=" + node.role() +
                " electionState=" + node.electionState() +
                " electionCount=" + node.electionCount();
        assertEquals(expectedCommitPosition, node.commitPosition(), errMsg);
        assertEquals(expectedCommittedMessageCount, node.service().messageCount(), errMsg);
    }
}
