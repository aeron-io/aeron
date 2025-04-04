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

import io.aeron.*;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.client.ControlledEgressListener;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.*;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.cluster.service.SnapshotDurationTracker;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.security.AuthenticationException;
import io.aeron.security.Authenticator;
import io.aeron.security.AuthorisationService;
import io.aeron.security.SessionProxy;
import io.aeron.status.HeartbeatTimestamp;
import io.aeron.test.*;
import io.aeron.test.cluster.ClusterTests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Hashing;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.MutableBoolean;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.zip.CRC32;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static io.aeron.cluster.service.Cluster.Role.FOLLOWER;
import static io.aeron.cluster.service.Cluster.Role.LEADER;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.*;
import static io.aeron.status.HeartbeatTimestamp.HEARTBEAT_TYPE_ID;
import static io.aeron.test.SystemTestWatcher.UNKNOWN_HOST_FILTER;
import static io.aeron.test.Tests.awaitAvailableWindow;
import static io.aeron.test.cluster.ClusterTests.*;
import static io.aeron.test.cluster.TestCluster.*;
import static io.aeron.test.cluster.TestNode.atMost;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.concurrent.TimeUnit.*;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.number.OrderingComparison.*;
import static org.junit.jupiter.api.Assertions.*;

@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class ClusterTest
{
    private static final String EMPTY_MSG = "";
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private TestCluster cluster = null;

    @Test
    @InterruptAfter(30)
    void shouldStopFollowerAndRestartFollower()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        assertEquals(1, leader.consensusModule().context().electionCounter().get());
        TestNode follower = cluster.followers().get(0);
        assertEquals(1, follower.consensusModule().context().electionCounter().get());

        awaitElectionClosed(follower);
        cluster.stopNode(follower);

        follower = cluster.startStaticNode(follower.index(), false);

        awaitElectionClosed(follower);
        assertEquals(FOLLOWER, follower.role());
        assertEquals(1 /* new counter */, follower.consensusModule().context().electionCounter().get());
        assertEquals(1, leader.consensusModule().context().electionCounter().get());
    }

    @Test
    @InterruptAfter(40)
    void shouldNotifyClientOfNewLeader()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        assertEquals(1, leader.consensusModule().context().electionCounter().get());
        final long leadershipTermId = leader.consensusModule().context().leadershipTermIdCounter().get();
        assertNotEquals(-1, leadershipTermId);
        final List<TestNode> followers = cluster.followers();
        for (final TestNode follower : followers)
        {
            assertEquals(1, follower.consensusModule().context().electionCounter().get());
            assertEquals(leadershipTermId, follower.consensusModule().context().leadershipTermIdCounter().get());
        }

        cluster.connectClient();
        cluster.awaitActiveSessionCount(1);

        cluster.stopNode(leader);
        cluster.awaitNewLeadershipEvent(1);
        final TestNode leader2 = cluster.awaitLeader();
        final long leadershipTermId2 = leader2.consensusModule().context().leadershipTermIdCounter().get();
        for (final TestNode follower : followers)
        {
            assertEquals(2, follower.consensusModule().context().electionCounter().get());
            assertEquals(leadershipTermId2, follower.consensusModule().context().leadershipTermIdCounter().get());
        }
    }

    @Test
    @InterruptAfter(30)
    void shouldStopLeaderAndFollowersThenRestartAllWithSnapshot()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        cluster.connectClient();

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);

        cluster.stopAllNodes();
        cluster.restartAllNodes(false);
        cluster.awaitLeader();
        assertEquals(2, cluster.followers().size());

        cluster.awaitSnapshotsLoaded();
    }

    @Test
    @InterruptAfter(10)
    void shouldNotSnapshotOnPrimaryClusterWhenStandbySnapshotIsRequested()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        cluster.takeStandbySnapshot(leader);
        cluster.awaitNeutralControlToggle(leader);

        cluster.connectClient();
        cluster.sendAndAwaitMessages(1);
        assertEquals(0, cluster.getSnapshotCount(leader));
    }

    @Test
    @InterruptAfter(30)
    void shouldStopClusteredServicesOnAppropriateMessage()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        TestCluster.awaitElectionClosed(leader);
        final List<TestNode> followers = cluster.followers();
        TestCluster.awaitElectionClosed(followers.get(0));
        TestCluster.awaitElectionClosed(followers.get(1));

        cluster.terminationsExpected(true);
        cluster.connectClient();
        cluster.sendTerminateMessage();
        cluster.awaitNodeTerminations();
    }

    @Test
    @InterruptAfter(30)
    void shouldShutdownClusterAndRestartWithSnapshots()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        TestCluster.awaitElectionClosed(followers.get(0));
        TestCluster.awaitElectionClosed(followers.get(1));

        cluster.node(0).isTerminationExpected(true);
        cluster.node(1).isTerminationExpected(true);
        cluster.node(2).isTerminationExpected(true);

        cluster.shutdownCluster(leader);
        cluster.awaitNodeTerminations();

        assertTrue(cluster.node(0).service().wasSnapshotTaken());
        assertTrue(cluster.node(1).service().wasSnapshotTaken());
        assertTrue(cluster.node(2).service().wasSnapshotTaken());

        cluster.stopAllNodes();
        cluster.restartAllNodes(false);
        final TestNode leader2 = cluster.awaitLeader();
        final long leadershipTermId = leader2.consensusModule().context().leadershipTermIdCounter().get();
        assertEquals(2, cluster.followers().size());
        for (final TestNode follower : cluster.followers())
        {
            assertEquals(leadershipTermId, follower.consensusModule().context().leadershipTermIdCounter().get());
        }

        cluster.awaitSnapshotsLoaded();
    }

    @Test
    @InterruptAfter(30)
    void shouldAbortClusterAndRestart()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        TestCluster.awaitElectionClosed(cluster.node(0));
        TestCluster.awaitElectionClosed(cluster.node(1));
        TestCluster.awaitElectionClosed(cluster.node(2));

        cluster.node(0).isTerminationExpected(true);
        cluster.node(1).isTerminationExpected(true);
        cluster.node(2).isTerminationExpected(true);

        cluster.abortCluster(leader);
        cluster.awaitNodeTerminations();

        assertFalse(cluster.node(0).service().wasSnapshotTaken());
        assertFalse(cluster.node(1).service().wasSnapshotTaken());
        assertFalse(cluster.node(2).service().wasSnapshotTaken());

        cluster.stopAllNodes();
        cluster.restartAllNodes(false);
        cluster.awaitLeader();
        assertEquals(2, cluster.followers().size());

        assertFalse(cluster.node(0).service().wasSnapshotLoaded());
        assertFalse(cluster.node(1).service().wasSnapshotLoaded());
        assertFalse(cluster.node(2).service().wasSnapshotLoaded());
    }

    @Test
    @InterruptAfter(30)
    void shouldAbortClusterOnTerminationTimeout()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();

        assertEquals(2, followers.size());
        final TestNode followerA = followers.get(0);
        final TestNode followerB = followers.get(1);

        leader.isTerminationExpected(true);
        followerA.isTerminationExpected(true);

        cluster.stopNode(followerB);

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        cluster.abortCluster(leader);
        cluster.awaitNodeTermination(leader);
        cluster.awaitNodeTermination(followerA);

        cluster.stopNode(leader);
        cluster.stopNode(followerA);
    }

    @Test
    @InterruptAfter(20)
    void shouldEchoMessages()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        assertNotNull(cluster.asyncConnectClient());

        cluster.sendAndAwaitMessages(10);
        cluster.awaitServiceMessagePredicate(cluster.awaitLeader(), atMost(10));
    }

    @Test
    @InterruptAfter(40)
    void shouldHandleLeaderFailOverWhenNameIsNotResolvable()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster).ignoreErrorsMatching(UNKNOWN_HOST_FILTER);

        final TestNode originalLeader = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendAndAwaitMessages(messageCount);

        cluster.disableNameResolution(originalLeader.hostname());
        cluster.stopNode(originalLeader);

        cluster.awaitNewLeadershipEvent(1);
        cluster.sendAndAwaitMessages(messageCount, 2 * messageCount);
    }

    @Test
    @InterruptAfter(40)
    void shouldHandleClusterStartWhenANameIsNotResolvable()
    {
        final int initiallyUnresolvableNodeId = 1;

        cluster = aCluster().withStaticNodes(3).withInvalidNameResolution(initiallyUnresolvableNodeId).start();
        systemTestWatcher.cluster(cluster).ignoreErrorsMatching(UNKNOWN_HOST_FILTER);

        cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendAndAwaitMessages(messageCount);

        cluster.restoreNameResolution(initiallyUnresolvableNodeId);
        assertNotNull(cluster.startStaticNode(initiallyUnresolvableNodeId, true));

        cluster.awaitServiceMessageCount(cluster.node(initiallyUnresolvableNodeId), messageCount);
    }

    @Test
    @InterruptAfter(30)
    void shouldElectSameLeaderAfterLoosingQuorum()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        TestNode followerOne = cluster.followers().get(0);
        final TestNode followerTwo = cluster.followers().get(1);

        awaitElectionClosed(followerOne);
        awaitElectionClosed(followerTwo);
        cluster.stopNode(followerOne);

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendAndAwaitMessages(messageCount);

        cluster.stopNode(followerTwo);
        cluster.awaitLossOfLeadership(leader.service());

        followerOne = cluster.startStaticNode(followerOne.index(), false);
        cluster.client().sendKeepAlive();
        awaitElectionClosed(followerOne);

        final TestNode newLeader = cluster.awaitLeader();
        cluster.awaitNewLeadershipEvent(1);

        assertEquals(FOLLOWER, followerOne.role());
        assertEquals(leader.index(), newLeader.index());

        cluster.sendAndAwaitMessages(messageCount, messageCount * 2);
    }

    @Test
    @InterruptAfter(10)
    void shouldElectNewLeaderAfterGracefulLeaderClose()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        cluster.connectClient();

        final int messageCount = 10;
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        leader.gracefulClose();

        final TestNode newLeader = cluster.awaitLeader();
        cluster.awaitNewLeadershipEvent(1);
        assertNotEquals(newLeader.index(), leader.index());
    }

    @Test
    @InterruptAfter(10)
    void shouldHandleClusterStartWhereMostNamesBecomeResolvableDuringElection()
    {
        cluster = aCluster().withStaticNodes(3).withInvalidNameResolution(0).withInvalidNameResolution(2).start();
        systemTestWatcher.cluster(cluster).ignoreErrorsMatching(UNKNOWN_HOST_FILTER);

        awaitElectionState(cluster.node(1), ElectionState.CANVASS);

        cluster.restoreNameResolution(0);
        cluster.restoreNameResolution(2);
        assertNotNull(cluster.startStaticNode(0, true));
        assertNotNull(cluster.startStaticNode(2, true));

        cluster.awaitLeader();
        cluster.connectClient();

        cluster.sendAndAwaitMessages(10);
    }

    @Test
    @InterruptAfter(40)
    void shouldEchoMessagesThenContinueOnNewLeader()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode originalLeader = cluster.awaitLeader();

        final int preFailureMessageCount = 10;
        cluster.connectClient();
        cluster.sendAndAwaitMessages(preFailureMessageCount);

        assertEquals(originalLeader.index(), cluster.client().leaderMemberId());

        cluster.stopNode(originalLeader);

        final TestNode newLeader = cluster.awaitLeader(originalLeader.index());
        cluster.awaitNewLeadershipEvent(1);
        assertEquals(newLeader.index(), cluster.client().leaderMemberId());

        final int postFailureMessageCount = 7;
        cluster.sendMessages(postFailureMessageCount);
        cluster.awaitResponseMessageCount(preFailureMessageCount + postFailureMessageCount);

        final TestNode follower = cluster.followers().get(0);

        cluster.awaitServiceMessageCount(newLeader, preFailureMessageCount + postFailureMessageCount);
        cluster.awaitServiceMessageCount(follower, preFailureMessageCount + postFailureMessageCount);
    }

    @Test
    @InterruptAfter(40)
    void shouldStopLeaderAndRestartAsFollower()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode originalLeader = cluster.awaitLeader();

        cluster.stopNode(originalLeader);
        cluster.awaitLeader(originalLeader.index());

        final TestNode follower = cluster.startStaticNode(originalLeader.index(), false);

        awaitElectionClosed(follower);
        assertEquals(FOLLOWER, follower.role());
    }

    @Test
    @InterruptAfter(40)
    void shouldStopLeaderAndRestartAsFollowerWithSendingAfter()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode originalLeader = cluster.awaitLeader();

        cluster.stopNode(originalLeader);
        cluster.awaitLeader(originalLeader.index());

        final TestNode follower = cluster.startStaticNode(originalLeader.index(), false);

        awaitElectionClosed(follower);
        assertEquals(FOLLOWER, follower.role());

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendAndAwaitMessages(messageCount);
    }

    @Test
    @InterruptAfter(60)
    void shouldStopLeaderAndRestartAsFollowerWithSendingAfterThenStopLeader()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode originalLeader = cluster.awaitLeader();

        cluster.stopNode(originalLeader);
        cluster.awaitLeader(originalLeader.index());

        final TestNode follower = cluster.startStaticNode(originalLeader.index(), false);
        awaitElectionClosed(follower);

        assertEquals(FOLLOWER, follower.role());

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        final TestNode leader = cluster.awaitLeader();
        cluster.stopNode(leader);

        cluster.awaitLeader(leader.index());
    }

    @Test
    @InterruptAfter(40)
    void shouldAcceptMessagesAfterSingleNodeCleanRestart()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        TestNode follower = cluster.followers().get(0);

        awaitElectionClosed(follower);
        cluster.stopNode(follower);

        follower = cluster.startStaticNode(follower.index(), true);

        awaitElectionClosed(cluster.node(follower.index()));
        assertEquals(FOLLOWER, follower.role());

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServiceMessageCount(follower, messageCount);
    }

    @Test
    @InterruptAfter(40)
    void shouldReplaySnapshotTakenWhileDown()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        cluster.connectClient();
        final List<TestNode> followers = cluster.followers();
        final TestNode followerA = followers.get(0);
        TestNode followerB = followers.get(1);

        awaitElectionClosed(followerB);
        cluster.stopNode(followerB);

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(leader, 1);
        cluster.awaitSnapshotCount(followerA, 1);

        final int messageCount = 10;
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        followerB = cluster.startStaticNode(followerB.index(), false);

        cluster.awaitSnapshotCount(followerB, 1);
        assertEquals(FOLLOWER, followerB.role());

        cluster.awaitServiceMessageCount(followerB, messageCount);
    }

    @Test
    @InterruptAfter(50)
    void shouldTolerateMultipleLeaderFailures()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();
        cluster.stopNode(firstLeader);

        final TestNode secondLeader = cluster.awaitLeader();

        final long commitPosition = secondLeader.commitPosition();
        final TestNode newFollower = cluster.startStaticNode(firstLeader.index(), false);

        cluster.awaitCommitPosition(newFollower, commitPosition);
        awaitElectionClosed(newFollower);

        cluster.stopNode(secondLeader);
        cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
    }

    @Test
    @InterruptAfter(90)
    void shouldRecoverAfterTwoLeaderNodesFailAndComeBackUpAtSameTime()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();

        final int sufficientMessageCountForReplay = 1_000_000;
        cluster.connectClient();
        cluster.sendAndAwaitMessages(sufficientMessageCountForReplay);
        cluster.closeClient();

        cluster.awaitActiveSessionCount(0);
        cluster.stopNode(firstLeader);

        final TestNode secondLeader = cluster.awaitLeader();
        cluster.stopNode(secondLeader);

        cluster.startStaticNode(firstLeader.index(), false);
        cluster.startStaticNode(secondLeader.index(), false);
        cluster.awaitLeader();

        cluster.connectClient();
        cluster.sendAndAwaitMessages(10, sufficientMessageCountForReplay + 10);
    }

    @Test
    @InterruptAfter(30)
    void shouldAcceptMessagesAfterTwoNodeCleanRestart()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        TestNode followerA = followers.get(0), followerB = followers.get(1);

        awaitElectionClosed(followerA);
        awaitElectionClosed(followerB);

        cluster.stopNode(followerA);
        cluster.stopNode(followerB);

        followerA = cluster.startStaticNode(followerA.index(), true);
        followerB = cluster.startStaticNode(followerB.index(), true);

        awaitElectionClosed(followerA);
        awaitElectionClosed(followerB);

        assertEquals(FOLLOWER, followerA.role());
        assertEquals(FOLLOWER, followerB.role());

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServiceMessageCount(followerA, messageCount);
        cluster.awaitServiceMessageCount(followerB, messageCount);
    }

    @Test
    @InterruptAfter(60)
    void shouldRecoverWithUncommittedMessagesAfterRestartWhenNewCommitPosExceedsPreviousAppendedPos()
    {
        cluster = aCluster().withStaticNodes(5).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        TestNode followerA = followers.get(0);
        TestNode followerB = followers.get(1);
        TestNode followerC = followers.get(2);
        TestNode followerD = followers.get(3);

        cluster.connectClient();

        cluster.stopNode(followerA);
        cluster.stopNode(followerB);
        cluster.stopNode(followerC);

        cluster.sendUnexpectedMessages(10);

        final long commitPosition = leader.commitPosition();
        while (leader.appendPosition() <= commitPosition)
        {
            Tests.yieldingIdle(
                "leader.appendPosition=" + leader.appendPosition() + " leader.commitPosition=" + commitPosition);
        }

        final long targetPosition = leader.appendPosition();

        cluster.stopNode(followerD);
        cluster.stopNode(leader);
        cluster.closeClient();

        followerA = cluster.startStaticNode(followerA.index(), false);
        followerB = cluster.startStaticNode(followerB.index(), false);
        followerC = cluster.startStaticNode(followerC.index(), false);

        cluster.awaitLeader();

        awaitElectionClosed(followerA);
        awaitElectionClosed(followerB);
        awaitElectionClosed(followerC);

        cluster.connectClient();

        final int messageLength = 128;
        int messageCount = 0;
        while (followerA.commitPosition() < targetPosition)
        {
            cluster.pollUntilMessageSent(messageLength);
            messageCount++;
        }

        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServiceMessageCount(followerA, messageCount);
        cluster.awaitServiceMessageCount(followerB, messageCount);
        cluster.awaitServiceMessageCount(followerC, messageCount);

        final TestNode oldLeader = cluster.startStaticNode(leader.index(), false);
        followerD = cluster.startStaticNode(followerD.index(), false);
        cluster.awaitServiceMessageCount(oldLeader, messageCount);
        cluster.awaitServiceMessageCount(followerD, messageCount);
    }

    @Test
    @InterruptAfter(50)
    void shouldRecoverWithUncommittedMessagesAfterRestartWhenNewCommitPosIsLessThanPreviousAppendedPos()
    {
        cluster = aCluster().withStaticNodes(5).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        final TestNode followerA = followers.get(0);
        final TestNode followerB = followers.get(1);
        final TestNode followerC = followers.get(2);
        final TestNode followerD = followers.get(3);

        cluster.connectClient();

        cluster.stopNode(followerA);
        cluster.stopNode(followerB);
        cluster.stopNode(followerC);

        final int messageCount = 10;
        cluster.sendUnexpectedMessages(messageCount);

        final long commitPosition = leader.commitPosition();
        while (leader.appendPosition() <= commitPosition)
        {
            Tests.yield();
        }

        cluster.stopNode(leader);
        cluster.stopNode(followerD);
        cluster.closeClient();

        cluster.startStaticNode(followerA.index(), false);
        cluster.startStaticNode(followerB.index(), false);
        cluster.startStaticNode(followerC.index(), false);
        cluster.awaitLeader();

        final TestNode oldLeader = cluster.startStaticNode(leader.index(), false);
        cluster.startStaticNode(followerD.index(), false);
        awaitElectionClosed(oldLeader);

        cluster.connectClient();
        cluster.sendAndAwaitMessages(messageCount);
    }

    @Test
    @InterruptAfter(40)
    void shouldCallOnRoleChangeOnBecomingLeader()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leaderOne = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        final TestNode followerA = followers.get(0);
        final TestNode followerB = followers.get(1);

        awaitElectionClosed(followerA);
        awaitElectionClosed(followerB);

        assertEquals(LEADER, leaderOne.service().roleChangedTo());
        assertNull(followerA.service().roleChangedTo());
        assertNull(followerB.service().roleChangedTo());

        cluster.stopNode(leaderOne);

        final TestNode leaderTwo = cluster.awaitLeader(leaderOne.index());
        final TestNode follower = cluster.followers().get(0);

        assertEquals(LEADER, leaderTwo.service().roleChangedTo());
        assertNull(follower.service().roleChangedTo());
    }

    @Test
    @InterruptAfter(40)
    void shouldLoseLeadershipWhenNoActiveQuorumOfFollowers()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        final TestNode followerA = followers.get(0);
        final TestNode followerB = followers.get(1);

        assertEquals(LEADER, leader.role());
        assertEquals(LEADER, leader.service().roleChangedTo());

        awaitElectionClosed(followerA);
        awaitElectionClosed(followerB);

        cluster.stopNode(followerA);
        cluster.stopNode(followerB);

        cluster.awaitLossOfLeadership(leader.service());
        assertEquals(FOLLOWER, leader.role());
    }

    @Test
    @InterruptAfter(30)
    void shouldTerminateLeaderWhenServiceStops()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        cluster.connectClient();

        leader.isTerminationExpected(true);
        leader.container().close();

        while (!leader.hasMemberTerminated())
        {
            Tests.sleep(1);
        }

        cluster.awaitNewLeadershipEvent(1);
    }

    @Test
    @InterruptAfter(30)
    void shouldEnterElectionWhenRecordingStopsUnexpectedlyOnLeader()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        cluster.connectClient();
        cluster.sendAndAwaitMessages(1);

        final AeronArchive.Context archiveCtx = new AeronArchive.Context()
            .controlRequestChannel(leader.archive().context().localControlChannel())
            .controlResponseChannel(leader.archive().context().localControlChannel())
            .controlRequestStreamId(leader.archive().context().localControlStreamId())
            .aeronDirectoryName(leader.mediaDriver().aeronDirectoryName());

        try (AeronArchive archive = AeronArchive.connect(archiveCtx))
        {
            final int firstRecordingIdIsTheClusterLog = 0;
            assertTrue(archive.tryStopRecordingByIdentity(firstRecordingIdIsTheClusterLog));
        }

        cluster.awaitNewLeadershipEvent(1);
        cluster.awaitLeaderAndClosedElection();
        cluster.followers(2);
    }

    @Test
    @InterruptAfter(30)
    void shouldCloseClientOnTimeout()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final AeronCluster client = cluster.connectClient();
        final ConsensusModule.Context context = leader.consensusModule().context();
        final Counter timedOutClientCounter = context.timedOutClientCounter();

        assertEquals(0, timedOutClientCounter.get());
        assertFalse(client.isClosed());

        Tests.sleep(NANOSECONDS.toMillis(context.sessionTimeoutNs()));

        cluster.shouldErrorOnClientClose(false);
        while (!client.isClosed())
        {
            Tests.sleep(1);
            client.pollEgress();
        }

        assertEquals(1, timedOutClientCounter.get());
    }

    @Test
    @InterruptAfter(40)
    void shouldRecoverWhileMessagesContinue() throws InterruptedException
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final MutableInteger messageCounter = new MutableInteger();
        cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        final TestNode follower = followers.get(1);

        cluster.connectClient();

        final Thread messageThread = startPublisherThread(cluster, messageCounter);
        final TestNode restartedFollowerB;
        try
        {
            Tests.await(() -> follower.commitPosition() > 0);

            cluster.stopNode(follower);
            final int delaySoClusterAdvancesMs = 2_000;
            Tests.sleep(delaySoClusterAdvancesMs);

            restartedFollowerB = cluster.startStaticNode(follower.index(), false);
            awaitElectionClosed(follower);
            final int delaySoIngressAdvancesAfterCatchupMs = 2_000;
            Tests.sleep(delaySoIngressAdvancesAfterCatchupMs);
        }
        finally
        {
            messageThread.interrupt();
            messageThread.join();
        }

        cluster.awaitResponseMessageCount(messageCounter.get());
        cluster.awaitServiceMessageCount(restartedFollowerB, messageCounter.get());

        cluster.client().close();
        cluster.awaitActiveSessionCount(0);
    }

    @Test
    @InterruptAfter(30)
    void shouldCatchupFromEmptyLog()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        TestNode follower = followers.get(1);

        awaitElectionClosed(follower);
        cluster.stopNode(follower);

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        follower = cluster.startStaticNode(follower.index(), true);
        cluster.awaitServiceMessageCount(follower, messageCount);
    }

    @Test
    @InterruptAfter(30)
    void shouldCatchupFromEmptyLogThenSnapshotAfterShutdownAndFollowerCleanStart()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers(2);
        final TestNode followerA = followers.get(0);
        final TestNode followerB = followers.get(1);

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        leader.isTerminationExpected(true);
        followerA.isTerminationExpected(true);
        followerB.isTerminationExpected(true);

        cluster.shutdownCluster(leader);
        cluster.awaitNodeTerminations();

        assertTrue(cluster.node(0).service().wasSnapshotTaken());
        assertTrue(cluster.node(1).service().wasSnapshotTaken());
        assertTrue(cluster.node(2).service().wasSnapshotTaken());

        cluster.stopAllNodes();

        cluster.startStaticNode(0, false);
        cluster.startStaticNode(1, false);
        cluster.startStaticNode(2, true);

        final TestNode newLeader = cluster.awaitLeader();
        assertNotEquals(2, newLeader.index());

        assertTrue(cluster.node(0).service().wasSnapshotLoaded());
        assertTrue(cluster.node(1).service().wasSnapshotLoaded());
        assertFalse(cluster.node(2).service().wasSnapshotLoaded());

        cluster.awaitServiceMessageCount(cluster.node(2), messageCount);
        cluster.awaitSnapshotCount(cluster.node(2), 1);
        assertTrue(cluster.node(2).service().wasSnapshotTaken());
    }

    @Test
    @InterruptAfter(30)
    void shouldCatchUpTwoFreshNodesAfterRestart()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();

        final int messageCount = 5_000;
        cluster.connectClient();
        final int messageLength = cluster.msgBuffer().putStringWithoutLengthAscii(0, NO_OP_MSG);
        for (int i = 0; i < messageCount; i++)
        {
            cluster.pollUntilMessageSent(messageLength);
        }
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);

        cluster.terminationsExpected(true);
        cluster.abortCluster(leader);
        cluster.awaitNodeTerminations();
        cluster.stopAllNodes();

        final TestNode oldLeader = cluster.startStaticNode(leader.index(), false);
        cluster.startStaticNode(followers.get(0).index(), true);
        cluster.startStaticNode(followers.get(1).index(), true);

        final TestNode newLeader = cluster.awaitLeader();
        assertEquals(newLeader.index(), oldLeader.index());

        cluster.followers(2);
        cluster.awaitServicesMessageCount(messageCount);
    }

    @Test
    @InterruptAfter(30)
    void shouldReplayMultipleSnapshotsWithEmptyFollowerLog()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        int messageCount = 2;
        cluster.connectClient();
        cluster.sendAndAwaitMessages(messageCount);

        cluster.takeSnapshot(leader);
        final int memberCount = 3;
        for (int memberId = 0; memberId < memberCount; memberId++)
        {
            final TestNode node = cluster.node(memberId);
            cluster.awaitSnapshotCount(node, 1);
            assertTrue(node.service().wasSnapshotTaken());
            node.service().resetSnapshotTaken();
        }

        cluster.sendMessages(1);
        messageCount++;
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);

        cluster.terminationsExpected(true);

        cluster.awaitNeutralControlToggle(leader);
        cluster.shutdownCluster(leader);
        cluster.awaitNodeTerminations();

        assertTrue(cluster.node(0).service().wasSnapshotTaken());
        assertTrue(cluster.node(1).service().wasSnapshotTaken());
        assertTrue(cluster.node(2).service().wasSnapshotTaken());

        cluster.stopAllNodes();

        cluster.startStaticNode(0, false);
        cluster.startStaticNode(1, false);
        cluster.startStaticNode(2, true);

        final TestNode newLeader = cluster.awaitLeader();
        assertNotEquals(2, newLeader.index());

        assertTrue(cluster.node(0).service().wasSnapshotLoaded());
        assertTrue(cluster.node(1).service().wasSnapshotLoaded());
        assertFalse(cluster.node(2).service().wasSnapshotLoaded());

        assertEquals(messageCount, cluster.node(0).service().messageCount());
        assertEquals(messageCount, cluster.node(1).service().messageCount());

        Tests.await(() -> cluster.node(2).service().messageCount() >= 3);
        assertEquals(messageCount, cluster.node(2).service().messageCount());

        final int messageCountAfterStart = 4;
        messageCount += messageCountAfterStart;
        cluster.reconnectClient();
        cluster.sendAndAwaitMessages(messageCountAfterStart, messageCount);
    }

    @Test
    @InterruptAfter(40)
    void shouldRecoverQuicklyAfterKillingFollowersThenRestartingOne()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        final TestNode followerOne = followers.get(0);
        final TestNode followerTwo = followers.get(1);

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        cluster.stopNode(followerOne);
        cluster.stopNode(followerTwo);

        while (leader.role() == LEADER)
        {
            cluster.sendMessages(1);
            Tests.sleep(500);
        }

        cluster.startStaticNode(followerTwo.index(), true);
        cluster.awaitLeader();
    }

    @Test
    @InterruptAfter(40)
    void shouldRecoverWhenLeaderHasAppendedMoreThanFollower()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        final TestNode followerOne = followers.get(0);
        final TestNode followerTwo = followers.get(1);

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        cluster.stopNode(followerOne);

        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount * 2);

        cluster.stopNode(followerTwo);
        cluster.stopNode(leader);

        cluster.startStaticNode(leader.index(), false);
        cluster.startStaticNode(followerOne.index(), false);
        cluster.awaitLeader();
    }

    @ParameterizedTest
    @InterruptAfter(40)
    @ValueSource(booleans = { true, false })
    void shouldRecoverWhenFollowerIsMultipleTermsBehind(final boolean useResponseChannels)
    {
        cluster = aCluster().withStaticNodes(3).useResponseChannels(useResponseChannels).start();
        systemTestWatcher.cluster(cluster);

        final TestNode originalLeader = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        cluster.stopNode(originalLeader);
        final TestNode newLeader = cluster.awaitLeader();
        assertNotNull(cluster.reconnectClient());

        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount * 2);

        cluster.stopNode(newLeader);
        cluster.startStaticNode(newLeader.index(), false);
        cluster.awaitLeader();
        cluster.reconnectClient();

        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount * 3);

        cluster.startStaticNode(originalLeader.index(), false);
        final TestNode lateJoiningNode = cluster.node(originalLeader.index());

        while (lateJoiningNode.service().messageCount() < messageCount * 3)
        {
            Tests.yieldingIdle("Waiting for late joining follower to catch up");
        }
    }

    @ParameterizedTest
    @InterruptAfter(60)
    @ValueSource(booleans = { true, false })
    void shouldRecoverWhenFollowerIsMultipleTermsBehindFromEmptyLog(final boolean useResponseChannels)
    {
        cluster = aCluster().withStaticNodes(4).useResponseChannels(useResponseChannels).start();

        systemTestWatcher.cluster(cluster);

        final TestNode originalLeader = cluster.awaitLeader();

        final int messageCount = 100;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        cluster.stopNode(originalLeader);
        final TestNode newLeader = cluster.awaitLeader();
        cluster.reconnectClient();

        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount * 2);

        cluster.stopNode(newLeader);
        cluster.startStaticNode(newLeader.index(), true);
        cluster.awaitLeader();
        cluster.reconnectClient();

        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount * 3);

        cluster.startStaticNode(originalLeader.index(), true);
        final TestNode lateJoiningNode = cluster.node(originalLeader.index());

        cluster.awaitServiceMessageCount(lateJoiningNode, messageCount * 3);
    }

    @Test
    @InterruptAfter(140)
    @Disabled
    void shouldHandleManyLargeMessages()
    {
        cluster = aCluster().withStaticNodes(3).start();

        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        awaitElectionState(cluster.node(0), ElectionState.CLOSED);
        awaitElectionState(cluster.node(1), ElectionState.CLOSED);
        awaitElectionState(cluster.node(2), ElectionState.CLOSED);

        final int largeMessageCount = 256_000;

        cluster.connectClient();
        cluster.sendLargeMessages(largeMessageCount);
        cluster.awaitResponseMessageCount(largeMessageCount);
        cluster.awaitServicesMessageCount(largeMessageCount);
    }

    @Test
    @InterruptAfter(40)
    @Disabled
    void shouldRecoverWhenFollowerWithInitialSnapshotAndArchivePurgeThenIsMultipleTermsBehind()
    {
        cluster = aCluster().withStaticNodes(3).start();

        systemTestWatcher.cluster(cluster);

        final TestNode originalLeader = cluster.awaitLeader();

        final int largeMessageCount = 128_000;
        final int messageCount = 10;

        cluster.connectClient();
        cluster.sendLargeMessages(largeMessageCount);
        cluster.awaitResponseMessageCount(largeMessageCount);
        cluster.awaitServicesMessageCount(largeMessageCount);

        cluster.takeSnapshot(originalLeader);
        cluster.awaitSnapshotCount(1);
        cluster.purgeLogToLastSnapshot();

        cluster.stopNode(originalLeader);
        final TestNode newLeader = cluster.awaitLeader();

        cluster.reconnectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(largeMessageCount + messageCount);

        cluster.stopNode(newLeader);
        cluster.startStaticNode(newLeader.index(), false);
        cluster.awaitLeader();

        cluster.reconnectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(largeMessageCount + (messageCount * 2));

        cluster.startStaticNode(originalLeader.index(), false);
        final TestNode lateJoiningNode = cluster.node(originalLeader.index());

        cluster.awaitServiceMessageCount(lateJoiningNode, largeMessageCount + (messageCount * 2));
    }

    @Test
    @InterruptAfter(40)
    void shouldRecoverWhenFollowerArrivesPartWayThroughTerm()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        final TestNode followerOne = cluster.followers().get(0);

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        cluster.stopNode(followerOne);

        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount * 2);

        cluster.startStaticNode(followerOne.index(), false);

        Tests.await(() -> cluster.node(followerOne.index()).service().messageCount() >= messageCount * 2);
        assertEquals(messageCount * 2, cluster.node(followerOne.index()).service().messageCount());
    }

    @Test
    @InterruptAfter(40)
    void shouldRecoverWhenFollowerArrivePartWayThroughTermAfterMissingElection()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        final TestNode followerOne = followers.get(0);
        final TestNode followerTwo = followers.get(1);

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        cluster.stopNode(followerOne);

        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount * 2);

        cluster.stopNode(followerTwo);
        cluster.stopNode(leader);

        cluster.startStaticNode(leader.index(), false);
        cluster.startStaticNode(followerTwo.index(), false);
        cluster.awaitLeader();
        cluster.reconnectClient();

        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount * 3);

        cluster.startStaticNode(followerOne.index(), false);

        Tests.await(() -> cluster.node(followerOne.index()).service().messageCount() >= messageCount * 3);
        assertEquals(messageCount * 3, cluster.node(followerOne.index()).service().messageCount());
    }

    @Test
    @InterruptAfter(40)
    void shouldRecoverWhenLastSnapshotIsMarkedInvalid()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader0 = cluster.awaitLeader();

        final int messageCount = 3;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitServicesMessageCount(messageCount);

        cluster.takeSnapshot(leader0);
        cluster.awaitSnapshotCount(1);

        cluster.sendMessages(messageCount);
        cluster.awaitServicesMessageCount(messageCount * 2);

        cluster.takeSnapshot(leader0);
        cluster.awaitSnapshotCount(2);

        cluster.stopNode(leader0);
        cluster.awaitLeader(leader0.index());
        cluster.awaitNewLeadershipEvent(1);
        awaitAvailableWindow(cluster.client().ingressPublication());
        assertTrue(cluster.client().sendKeepAlive());
        cluster.startStaticNode(leader0.index(), false);

        cluster.sendAndAwaitMessages(messageCount, messageCount * 3);

        cluster.terminationsExpected(true);
        cluster.stopAllNodes();

        cluster.invalidateLatestSnapshot();

        cluster.restartAllNodes(false);
        cluster.awaitLeader();
        cluster.awaitServicesMessageCount(messageCount * 3);
    }

    @Test
    @InterruptAfter(30)
    void shouldRecoverWhenLastSnapshotForShutdownIsMarkedInvalid()
    {
        cluster = aCluster().withStaticNodes(1).start();
        systemTestWatcher.cluster(cluster);

        TestNode leader = cluster.awaitLeader();

        final int messageCount = 3;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitServicesMessageCount(messageCount);

        cluster.stopNode(leader);
        cluster.startStaticNode(leader.index(), false);
        leader = cluster.awaitLeader();

        cluster.terminationsExpected(true);
        cluster.shutdownCluster(leader);
        cluster.awaitNodeTerminations();
        assertTrue(leader.service().wasSnapshotTaken());
        cluster.stopNode(leader);

        cluster.invalidateLatestSnapshot();

        cluster.restartAllNodes(false);
        leader = cluster.awaitLeader();
        cluster.awaitServicesMessageCount(messageCount);
        assertTrue(leader.service().wasSnapshotTaken());
    }

    @Test
    @InterruptAfter(60)
    void shouldHandleMultipleElections()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader0 = cluster.awaitLeader();

        final int messageCount = 3;
        cluster.connectClient();
        cluster.sendAndAwaitMessages(messageCount);

        cluster.stopNode(leader0);
        final TestNode leader1 = cluster.awaitLeader(leader0.index());
        cluster.awaitNewLeadershipEvent(1);
        awaitAvailableWindow(cluster.client().ingressPublication());
        assertTrue(cluster.client().sendKeepAlive());
        cluster.startStaticNode(leader0.index(), false);
        awaitElectionClosed(cluster.node(leader0.index()));

        cluster.connectClient();
        cluster.sendAndAwaitMessages(messageCount, messageCount * 2);

        cluster.stopNode(leader1);
        cluster.awaitLeader(leader1.index());
        cluster.awaitNewLeadershipEvent(2);
        awaitAvailableWindow(cluster.client().ingressPublication());
        assertTrue(cluster.client().sendKeepAlive());
        cluster.startStaticNode(leader1.index(), false);
        awaitElectionClosed(cluster.node(leader1.index()));

        cluster.connectClient();
        cluster.sendAndAwaitMessages(messageCount, messageCount * 3);
    }

    @Test
    @InterruptAfter(50)
    void shouldRecoverWhenLastSnapshotIsInvalidBetweenTwoElections()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader0 = cluster.awaitLeader();

        final int messageCount = 3;
        cluster.connectClient();
        cluster.sendAndAwaitMessages(messageCount);

        cluster.stopNode(leader0);
        final TestNode leader1 = cluster.awaitLeader(leader0.index());
        cluster.awaitNewLeadershipEvent(1);
        awaitAvailableWindow(cluster.client().ingressPublication());
        assertTrue(cluster.client().sendKeepAlive());
        cluster.startStaticNode(leader0.index(), false);

        cluster.sendAndAwaitMessages(messageCount, messageCount * 2);

        cluster.takeSnapshot(leader1);
        cluster.awaitSnapshotCount(1);

        cluster.stopNode(leader1);
        cluster.awaitLeader(leader1.index());
        cluster.awaitNewLeadershipEvent(2);
        awaitAvailableWindow(cluster.client().ingressPublication());
        assertTrue(cluster.client().sendKeepAlive());
        cluster.startStaticNode(leader1.index(), false);

        cluster.sendAndAwaitMessages(messageCount, messageCount * 3);

        // No snapshot for Term 2

        cluster.terminationsExpected(true);
        cluster.stopAllNodes();

        cluster.invalidateLatestSnapshot();

        cluster.restartAllNodes(false);
        cluster.awaitLeader();
        cluster.awaitServicesMessageCount(messageCount * 3);
    }

    @Test
    @InterruptAfter(50)
    void shouldRecoverWhenLastTwosSnapshotsAreInvalidAfterElection()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader0 = cluster.awaitLeader();

        final int messageCount = 3;
        cluster.connectClient();
        cluster.sendAndAwaitMessages(messageCount);

        cluster.takeSnapshot(leader0);
        cluster.awaitSnapshotCount(1);

        cluster.stopNode(leader0);
        final TestNode leader1 = cluster.awaitLeader(leader0.index());
        cluster.awaitNewLeadershipEvent(1);
        awaitAvailableWindow(cluster.client().ingressPublication());
        assertTrue(cluster.client().sendKeepAlive());
        cluster.startStaticNode(leader0.index(), false);

        cluster.sendAndAwaitMessages(messageCount, messageCount * 2);

        cluster.takeSnapshot(leader1);
        for (int i = 0; i < 3; i++)
        {
            cluster.awaitSnapshotCount(cluster.node(i), leader0.index() == i ? 1 : 2);
        }

        cluster.sendAndAwaitMessages(messageCount, messageCount * 3);

        cluster.takeSnapshot(leader1);
        for (int i = 0; i < 3; i++)
        {
            cluster.awaitSnapshotCount(cluster.node(i), leader0.index() == i ? 2 : 3);
        }

        cluster.sendAndAwaitMessages(messageCount, messageCount * 4);

        cluster.terminationsExpected(true);
        cluster.stopAllNodes();

        cluster.invalidateLatestSnapshot();
        cluster.invalidateLatestSnapshot();

        cluster.restartAllNodes(false);
        cluster.awaitLeader();

        cluster.awaitSnapshotCount(2);

        cluster.awaitServicesMessageCount(messageCount * 4);
    }

    @Test
    @InterruptAfter(30)
    void shouldCatchUpAfterFollowerMissesOneMessage()
    {
        shouldCatchUpAfterFollowerMissesMessage(NO_OP_MSG);
    }

    @Test
    @InterruptAfter(30)
    void shouldCatchUpAfterFollowerMissesTimerRegistration()
    {
        shouldCatchUpAfterFollowerMissesMessage(REGISTER_TIMER_MSG);
    }

    @SuppressWarnings("MethodLength")
    @Test
    @InterruptAfter(30)
    void shouldAllowChangingTermBufferLengthAndMtuAfterRecordingLogIsTruncatedToTheLatestSnapshot()
    {
        final int originalTermLength = 256 * 1024;
        final int originalMtu = 1408;
        final int newTermLength = 2 * 1024 * 1024;
        final int newMtu = 8992;
        final int staticNodeCount = 3;
        final CRC32 crc32 = new CRC32();

        cluster = aCluster().withStaticNodes(staticNodeCount)
            .withLogChannel("aeron:udp?term-length=" + originalTermLength + "|mtu=" + originalMtu)
            .withIngressChannel("aeron:udp?term-length=" + originalTermLength + "|mtu=" + originalMtu)
            .withEgressChannel(
                "aeron:udp?endpoint=localhost:0|term-length=" + originalTermLength + "|mtu=" + originalMtu)
            .withServiceSupplier(
                (i) -> new TestNode.TestService[]{ new TestNode.TestService(), new TestNode.ChecksumService() })
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        for (int i = 0; i < staticNodeCount; i++)
        {
            assertEquals(2, cluster.node(i).services().length);
        }

        cluster.connectClient();
        final int firstBatch = 9;
        int messageLength = computeMaxMessageLength(originalTermLength) - SESSION_HEADER_LENGTH;
        int payloadLength = messageLength - SIZE_OF_INT;
        cluster.msgBuffer().setMemory(0, payloadLength, (byte)'x');
        crc32.reset();
        crc32.update(cluster.msgBuffer().byteArray(), 0, payloadLength);
        int msgChecksum = (int)crc32.getValue();
        cluster.msgBuffer().putInt(payloadLength, msgChecksum, LITTLE_ENDIAN);
        long checksum = 0;
        for (int i = 0; i < firstBatch; i++)
        {
            cluster.pollUntilMessageSent(messageLength);
            checksum = Hashing.hash(checksum ^ msgChecksum);
        }
        cluster.awaitResponseMessageCount(firstBatch);

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);

        cluster.msgBuffer().setMemory(0, payloadLength, (byte)'y');
        crc32.reset();
        crc32.update(cluster.msgBuffer().byteArray(), 0, payloadLength);
        msgChecksum = (int)crc32.getValue();
        cluster.msgBuffer().putInt(payloadLength, msgChecksum, LITTLE_ENDIAN);
        final int secondBatch = 11;
        cluster.reconnectClient();
        for (int i = 0; i < secondBatch; i++)
        {
            try
            {
                cluster.pollUntilMessageSent(messageLength);
            }
            catch (final ClusterException ex)
            {
                throw new RuntimeException("i=" + i, ex);
            }
        }
        cluster.awaitResponseMessageCount(firstBatch + secondBatch);

        cluster.stopAllNodes();

        cluster.seedRecordingsFromLatestSnapshot();

        cluster.logChannel("aeron:udp?term-length=" + newTermLength + "|mtu=" + newMtu);
        cluster.ingressChannel("aeron:udp?term-length=" + newTermLength + "|mtu=" + newMtu);
        cluster.egressChannel("aeron:udp?endpoint=localhost:0|term-length=" + newTermLength + "|mtu=" + newMtu);
        cluster.restartAllNodes(false);
        cluster.awaitLeader();
        assertEquals(2, cluster.followers().size());
        for (int i = 0; i < staticNodeCount; i++)
        {
            assertEquals(2, cluster.node(i).services().length);
        }

        cluster.awaitSnapshotsLoaded();

        cluster.reconnectClient();
        messageLength = computeMaxMessageLength(newTermLength) - SESSION_HEADER_LENGTH;
        payloadLength = messageLength - SIZE_OF_INT;
        cluster.msgBuffer().setMemory(0, payloadLength, (byte)'z');
        crc32.reset();
        crc32.update(cluster.msgBuffer().byteArray(), 0, payloadLength);
        msgChecksum = (int)crc32.getValue();
        cluster.msgBuffer().putInt(payloadLength, msgChecksum, LITTLE_ENDIAN);
        final int thirdBatch = 5;
        for (int i = 0; i < thirdBatch; i++)
        {
            cluster.pollUntilMessageSent(messageLength);
            checksum = Hashing.hash(checksum ^ msgChecksum);
        }
        cluster.awaitResponseMessageCount(firstBatch + secondBatch + thirdBatch);

        final int finalMessageCount = firstBatch + thirdBatch;
        final long finalChecksum = checksum;
        final Predicate<TestNode> finalServiceState =
            (node) ->
            {
                final TestNode.TestService[] services = node.services();
                return finalMessageCount == services[0].messageCount() &&
                    finalChecksum == ((TestNode.ChecksumService)services[1]).checksum();
            };

        for (int i = 0; i < staticNodeCount; i++)
        {
            final TestNode node = cluster.node(i);
            cluster.awaitNodeState(node, finalServiceState);
        }
    }

    @Test
    @InterruptAfter(180)
    void shouldRecoverWhenFollowersIsMultipleTermsBehindFromEmptyLogAndPartialLogWithoutCommittedLogEntry()
    {
        cluster = aCluster().withStaticNodes(5).start(4);

        systemTestWatcher.cluster(cluster);

        final int messageCount = 10;
        final int termCount = 3;
        int totalMessages = 0;

        int partialNode = Aeron.NULL_VALUE;

        for (int i = 0; i < termCount; i++)
        {
            final TestNode oldLeader = cluster.awaitLeader();

            cluster.connectClient();
            cluster.sendMessages(messageCount);
            totalMessages += messageCount;
            cluster.awaitResponseMessageCount(totalMessages);

            if (Aeron.NULL_VALUE == partialNode)
            {
                partialNode = (oldLeader.index() + 1) % 4;
                cluster.stopNode(cluster.node(partialNode));
            }

            cluster.stopNode(oldLeader);
            cluster.startStaticNode(oldLeader.index(), false);
            cluster.awaitLeader();
        }

        final TestNode lateJoiningNode = cluster.startStaticNode(4, true);

        cluster.awaitServiceMessageCount(lateJoiningNode, totalMessages);

        final TestNode node = cluster.startStaticNode(partialNode, false);

        cluster.awaitServiceMessageCount(node, totalMessages);

        cluster.awaitLeader();

        cluster.connectClient();
        cluster.sendMessages(messageCount);
        totalMessages += messageCount;
        cluster.awaitResponseMessageCount(totalMessages);
        cluster.awaitServiceMessageCount(node, totalMessages);

        cluster.assertRecordingLogsEqual();
    }

    @Test
    @InterruptAfter(10)
    void shouldRejectTakeSnapshotRequestWithAnAuthorisationError()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();

        final long requestCorrelationId = System.nanoTime();
        final MutableBoolean hasResponse = injectAdminResponseEgressListener(
            requestCorrelationId,
            AdminRequestType.SNAPSHOT,
            AdminResponseCode.UNAUTHORISED_ACCESS,
            "Execution of the " + AdminRequestType.SNAPSHOT + " request was not authorised");

        final AeronCluster client = cluster.connectClient();
        while (!client.sendAdminRequestToTakeASnapshot(requestCorrelationId))
        {
            Tests.yield();
        }

        while (!hasResponse.get())
        {
            client.pollEgress();
            Tests.yield();
        }

        long time = System.nanoTime();
        final long deadline = time + SECONDS.toNanos(2);
        do
        {
            assertEquals(0, cluster.getSnapshotCount(leader));
            for (final TestNode follower : followers)
            {
                assertEquals(0, cluster.getSnapshotCount(follower));
            }
            Tests.sleep(10);
            time = System.nanoTime();
        }
        while (time < deadline);
    }

    @Test
    @InterruptAfter(10)
    void shouldRejectAnInvalidAdminRequest()
    {
        final AdminRequestType invalidRequestType = AdminRequestType.NULL_VAL;
        final AtomicBoolean isAuthorisedInvoked = new AtomicBoolean();
        cluster = aCluster()
            .withStaticNodes(3)
            .withAuthorisationServiceSupplier(() ->
                (protocolId, actionId, type, encodedPrincipal) ->
                {
                    isAuthorisedInvoked.set(true);
                    assertEquals(MessageHeaderDecoder.SCHEMA_ID, protocolId);
                    assertEquals(AdminRequestEncoder.TEMPLATE_ID, actionId);
                    assertEquals(invalidRequestType, type);
                    return true;
                })
            .start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();

        final long requestCorrelationId = System.nanoTime();
        final MutableBoolean hasResponse = injectAdminResponseEgressListener(
            requestCorrelationId,
            invalidRequestType,
            AdminResponseCode.ERROR,
            "Unknown request type: " + invalidRequestType);

        final AeronCluster client = cluster.connectClient();
        final AdminRequestEncoder adminRequestEncoder = new AdminRequestEncoder()
            .wrapAndApplyHeader(cluster.msgBuffer(), 0, new MessageHeaderEncoder())
            .leadershipTermId(client.leadershipTermId())
            .clusterSessionId(client.clusterSessionId())
            .correlationId(requestCorrelationId)
            .requestType(invalidRequestType);

        final Publication ingressPublication = client.ingressPublication();
        while (ingressPublication.offer(
            adminRequestEncoder.buffer(),
            0,
            MessageHeaderEncoder.ENCODED_LENGTH + adminRequestEncoder.encodedLength()) < 0)
        {
            Tests.yield();
        }

        Tests.await(isAuthorisedInvoked::get);

        while (!hasResponse.get())
        {
            client.pollEgress();
            Tests.yield();
        }
    }

    @Test
    @InterruptAfter(20)
    void shouldTakeASnapshotAfterReceivingAdminRequestOfTypeSnapshot()
    {
        cluster = aCluster()
            .withStaticNodes(3)
            .withAuthorisationServiceSupplier(() -> AuthorisationService.ALLOW_ALL)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final long requestCorrelationId = System.nanoTime();
        final MutableBoolean hasResponse = injectAdminResponseEgressListener(
            requestCorrelationId, AdminRequestType.SNAPSHOT, AdminResponseCode.OK, EMPTY_MSG);

        final AeronCluster client = cluster.connectClient();
        while (!client.sendAdminRequestToTakeASnapshot(requestCorrelationId))
        {
            Tests.yield();
        }

        while (!hasResponse.get())
        {
            client.pollEgress();
            Tests.yield();
        }

        cluster.awaitSnapshotCount(1);
        cluster.awaitNeutralControlToggle(leader);
    }

    @Test
    @InterruptAfter(20)
    @SuppressWarnings("MethodLength")
    void shouldTrackSnapshotDuration()
    {
        final long service1SnapshotDelayMs = 111;
        final long service2SnapshotDelayMs = 222;

        cluster = aCluster()
            .withServiceSupplier(
                (i) -> new TestNode.TestService[]
                    {
                    new TestNode.SleepOnSnapshotTestService()
                        .snapshotDelayMs(service1SnapshotDelayMs).index(i),
                    new TestNode.SleepOnSnapshotTestService()
                        .snapshotDelayMs(service2SnapshotDelayMs).index(i)
                    })
            .withStaticNodes(3)
            .withAuthorisationServiceSupplier(() -> AuthorisationService.ALLOW_ALL)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final SnapshotDurationTracker totalSnapshotDurationTracker = leader.consensusModule().context()
            .totalSnapshotDurationTracker();

        final SnapshotDurationTracker service1SnapshotDurationTracker = leader.container(0).context()
            .snapshotDurationTracker();

        final SnapshotDurationTracker service2SnapshotDurationTracker = leader.container(1).context()
            .snapshotDurationTracker();

        final long requestCorrelationId = System.nanoTime();
        final MutableBoolean hasResponse = injectAdminResponseEgressListener(
            requestCorrelationId, AdminRequestType.SNAPSHOT, AdminResponseCode.OK, EMPTY_MSG);

        final AeronCluster client = cluster.connectClient();

        assertEquals(0, totalSnapshotDurationTracker.snapshotDurationThresholdExceededCount().get());
        assertEquals(0, totalSnapshotDurationTracker.maxSnapshotDuration().get());

        assertEquals(0, service1SnapshotDurationTracker.snapshotDurationThresholdExceededCount().get());
        assertEquals(0, service1SnapshotDurationTracker.maxSnapshotDuration().get());

        assertEquals(0, service2SnapshotDurationTracker.snapshotDurationThresholdExceededCount().get());
        assertEquals(0, service2SnapshotDurationTracker.maxSnapshotDuration().get());

        while (!client.sendAdminRequestToTakeASnapshot(requestCorrelationId))
        {
            Tests.yield();
        }

        while (!hasResponse.get())
        {
            client.pollEgress();
            Tests.yield();
        }

        cluster.awaitSnapshotCount(1);
        cluster.awaitNeutralControlToggle(leader);

        assertEquals(1, totalSnapshotDurationTracker.snapshotDurationThresholdExceededCount().get());
        assertThat(
            totalSnapshotDurationTracker.maxSnapshotDuration().get(),
            greaterThanOrEqualTo(
            percent90(MILLISECONDS.toNanos(Math.max(service1SnapshotDelayMs, service2SnapshotDelayMs)))));

        assertEquals(1, service1SnapshotDurationTracker.snapshotDurationThresholdExceededCount().get());
        assertThat(
            service1SnapshotDurationTracker.maxSnapshotDuration().get(),
            greaterThanOrEqualTo(percent90(MILLISECONDS.toNanos(service1SnapshotDelayMs))));

        assertEquals(1, service2SnapshotDurationTracker.snapshotDurationThresholdExceededCount().get());
        assertThat(
            service2SnapshotDurationTracker.maxSnapshotDuration().get(),
            greaterThanOrEqualTo(percent90(MILLISECONDS.toNanos(service2SnapshotDelayMs))));

        for (final TestNode follower : cluster.followers())
        {
            final SnapshotDurationTracker snapshotDurationTracker = follower.consensusModule().context()
                .totalSnapshotDurationTracker();
            assertEquals(1, snapshotDurationTracker.snapshotDurationThresholdExceededCount().get());
            assertThat(
                snapshotDurationTracker.maxSnapshotDuration().get(),
                greaterThanOrEqualTo(
                percent90(MILLISECONDS.toNanos(Math.max(service1SnapshotDelayMs, service2SnapshotDelayMs)))));

            final SnapshotDurationTracker service1SnapshotTracker = follower.container(0).context()
                .snapshotDurationTracker();

            assertEquals(1, service1SnapshotTracker.snapshotDurationThresholdExceededCount().get());
            assertThat(
                service1SnapshotTracker.maxSnapshotDuration().get(),
                greaterThanOrEqualTo(percent90(MILLISECONDS.toNanos(service1SnapshotDelayMs))));

            final SnapshotDurationTracker service2SnapshotTracker = follower.container(1).context()
                .snapshotDurationTracker();

            assertEquals(1, service2SnapshotTracker.snapshotDurationThresholdExceededCount().get());
            assertThat(
                service2SnapshotTracker.maxSnapshotDuration().get(),
                greaterThanOrEqualTo(percent90(MILLISECONDS.toNanos(service1SnapshotDelayMs))));
        }
    }

    private static long percent90(final long value)
    {
        return 90 * (value / 100);
    }

    @Test
    @InterruptAfter(20)
    void shouldTakeASnapshotAfterReceivingAdminRequestOfTypeSnapshotAndNotifyViaControlledPoll()
    {
        cluster = aCluster()
            .withStaticNodes(3)
            .withAuthorisationServiceSupplier(() ->
                (protocolId, actionId, type, encodedPrincipal) ->
                {
                    assertEquals(MessageHeaderDecoder.SCHEMA_ID, protocolId);
                    assertEquals(AdminRequestEncoder.TEMPLATE_ID, actionId);
                    assertEquals(AdminRequestType.SNAPSHOT, type);
                    return true;
                })
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final long requestCorrelationId = System.nanoTime();
        final MutableBoolean hasResponse = injectAdminRequestControlledEgressListener(requestCorrelationId);

        final AeronCluster client = cluster.connectClient();
        while (!client.sendAdminRequestToTakeASnapshot(requestCorrelationId))
        {
            Tests.yield();
        }

        while (!hasResponse.get())
        {
            client.controlledPollEgress();
            Tests.yield();
        }

        cluster.awaitSnapshotCount(1);
        cluster.awaitNeutralControlToggle(leader);
    }

    @Test
    @InterruptAfter(20)
    void shouldHandleTrimmingClusterFromTheFront()
    {
        cluster = aCluster().withSegmentFileLength(512 * 1024).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leaderNode = cluster.awaitLeader();
        cluster.connectClient();
        cluster.sendLargeMessages(1024);
        cluster.awaitResponseMessageCount(1024);
        cluster.awaitServicesMessageCount(1024);

        cluster.takeSnapshot(leaderNode);
        cluster.awaitSnapshotCount(1);
        cluster.purgeLogToLastSnapshot();

        cluster.terminationsExpected(true);
        cluster.abortCluster(leaderNode);
        cluster.awaitNodeTermination(leaderNode);
        cluster.awaitNodeTermination(cluster.followers().get(0));
        cluster.awaitNodeTermination(cluster.followers().get(1));
        cluster.close();

        cluster.restartAllNodes(false);
        cluster.awaitServicesMessageCount(1024);
    }

    @Test
    @InterruptAfter(20)
    void shouldHandleReusingCorrelationIdsAcrossASnapshot()
    {
        cluster = aCluster().withSegmentFileLength(512 * 1024).start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        cluster.connectClient();
        final int messageLength1 = cluster.msgBuffer().putStringWithoutLengthAscii(0, REGISTER_TIMER_MSG);
        cluster.pollUntilMessageSent(messageLength1);
        cluster.awaitResponseMessageCount(1);

        cluster.awaitTimerEventCount(1);

        final int messageLength2 = cluster.msgBuffer().putStringWithoutLengthAscii(0, REGISTER_TIMER_MSG);
        cluster.pollUntilMessageSent(messageLength2);
        cluster.awaitResponseMessageCount(2);

        cluster.awaitTimerEventCount(1);
    }

    @Test
    @InterruptAfter(20)
    void shouldHandleReplayAfterShutdown()
    {
        cluster = aCluster().withStaticNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        cluster.connectClient();
        cluster.sendAndAwaitMessages(10);

        leader.container().close(); // Will cause shutdown in consensus module
        Tests.sleep(1_000);

        CloseHelper.quietCloseAll(leader.consensusModule(), leader.archive(), leader.mediaDriver());

        cluster.startStaticNode(0, false);
        cluster.awaitLeader();
        cluster.reconnectClient();
        cluster.sendAndAwaitMessages(10);
    }

    @Test
    @InterruptAfter(60)
    void shouldRemainStableWhenThereIsASlowFollower()
    {
        cluster = aCluster().withStaticNodes(3).withLogChannel("aeron:udp?term-length=64k").start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        final TestNode followerToRestart = cluster.followers().get(0);
        final TestNode liveFollower = cluster.followers().get(1);

        awaitElectionClosed(followerToRestart);
        cluster.stopNode(followerToRestart);

        cluster.connectClient();

        final long slowDownDelayMs = 15_000;
        cluster.sendMessageToSlowDownService(liveFollower.index(), MILLISECONDS.toNanos(slowDownDelayMs));
        cluster.sendMessages(1000);

        final TestNode restartedFollower = cluster.startStaticNode(followerToRestart.index(), false);
        awaitElectionClosed(restartedFollower);

        Tests.sleep(slowDownDelayMs);

        awaitElectionClosed(restartedFollower);
    }

    @ParameterizedTest
    @ValueSource(ints = { 20, 100 })
    @InterruptAfter(90)
    @DisabledOnOs(OS.MAC)
    void shouldCatchupFollowerWithSlowService(final int sleepTimeMs)
    {
        final IntFunction<TestNode.TestService[]> serviceSupplier =
            (i) -> new TestNode.TestService[]
            {
                new TestNode.TestService().index(i),
                new TestNode.TestService()
                {
                    public void onSessionMessage(
                        final ClientSession session,
                        final long timestamp,
                        final DirectBuffer buffer,
                        final int offset,
                        final int length,
                        final Header header)
                    {
                        Tests.sleep(sleepTimeMs);
                        messageCount.incrementAndGet();
                    }
                }.index(i)
            };

        cluster = aCluster()
            .withLogChannel("aeron:udp?term-length=1m|alias=log")
            .withStaticNodes(3)
            .withServiceSupplier(serviceSupplier)
            .start();

        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        cluster.connectClient();

        final int firstBatchCount = (int)(SECONDS.toMillis(5) / sleepTimeMs);
        cluster.sendMessages(firstBatchCount);
        cluster.awaitResponseMessageCount(firstBatchCount);
        cluster.awaitServicesMessageCount(firstBatchCount);

        final TestNode followerA = cluster.followers().get(0);
        final TestNode followerB = cluster.followers().get(1);
        cluster.stopNode(followerA);

        final int secondBatchCount = (int)(SECONDS.toMillis(7) / sleepTimeMs);
        cluster.sendMessages(secondBatchCount);
        cluster.awaitResponseMessageCount(firstBatchCount + secondBatchCount);
        cluster.awaitServiceMessageCount(followerB, firstBatchCount + secondBatchCount);

        cluster.startStaticNode(followerA.index(), false);

        final int thirdBatchCount = (int)(SECONDS.toMillis(3) / sleepTimeMs);
        cluster.sendMessages(thirdBatchCount);
        cluster.awaitResponseMessageCount(firstBatchCount + secondBatchCount + thirdBatchCount);
        cluster.awaitServicesMessageCount(firstBatchCount + secondBatchCount + thirdBatchCount);
    }

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("MethodLength")
    void shouldAssembleFragmentedSessionMessages()
    {
        final UnsafeBuffer messages = new UnsafeBuffer(new byte[8192]);
        cluster = aCluster().withServiceSupplier(
            (i) -> new TestNode.TestService[]{ new TestNode.TestService()
            {
                private int messageOffset;

                public void onSessionMessage(
                    final ClientSession session,
                    final long timestamp,
                    final DirectBuffer buffer,
                    final int offset,
                    final int length,
                    final Header header)
                {
                    messages.putBytes(messageOffset, header.buffer(), header.offset(), HEADER_LENGTH);
                    messages.putBytes(
                        messageOffset + HEADER_LENGTH,
                        buffer,
                        offset - SESSION_HEADER_LENGTH,
                        length + SESSION_HEADER_LENGTH);
                    messageOffset += BitUtil.align(length + SESSION_HEADER_LENGTH + HEADER_LENGTH, FRAME_ALIGNMENT);
                    echoMessage(session, buffer, offset, length);
                }
            } }
        ).withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final int logStreamId = leader.consensusModule().context().logStreamId();
        final AeronCluster client = cluster.connectClient();

        final ExpandableArrayBuffer msgBuffer = cluster.msgBuffer();
        final int unfragmentedMessageLength = 63;
        final long unfragmentedReservedValue = 2348723482321L;
        final BufferClaim bufferClaim = new BufferClaim();
        while (client.tryClaim(unfragmentedMessageLength, bufferClaim) < 0)
        {
            Tests.sleep(1);
            ClusterTests.failOnClusterError();
        }
        bufferClaim.buffer().setMemory(
            bufferClaim.offset() + SESSION_HEADER_LENGTH, unfragmentedMessageLength, (byte)0xEA);
        bufferClaim.flags((byte)BEGIN_END_AND_EOS_FLAGS);
        bufferClaim.reservedValue(unfragmentedReservedValue);
        bufferClaim.commit();

        final int messageLength = 5979;
        msgBuffer.setMemory(0, messageLength, (byte)0xBC);
        while (client.offer(msgBuffer, 0, messageLength) < 0)
        {
            Tests.sleep(1);
            ClusterTests.failOnClusterError();
        }

        cluster.awaitResponseMessageCount(2);

        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
        final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        final SessionMessageHeaderDecoder sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
        final Publication ingressPublication = client.ingressPublication();

        headerFlyweight.wrap(messages, 0, HEADER_LENGTH);
        assertEquals(unfragmentedMessageLength + SESSION_HEADER_LENGTH + HEADER_LENGTH, headerFlyweight.frameLength());
        assertEquals(CURRENT_VERSION, headerFlyweight.version());
        assertEquals(UNFRAGMENTED, (byte)headerFlyweight.flags());
        assertEquals(HDR_TYPE_DATA, headerFlyweight.headerType());
        assertEquals(256, headerFlyweight.termOffset());
        assertNotEquals(ingressPublication.sessionId(), headerFlyweight.sessionId());
        assertEquals(logStreamId, headerFlyweight.streamId());
        assertEquals(0, headerFlyweight.termId());
        assertEquals(DEFAULT_RESERVE_VALUE, headerFlyweight.reservedValue()); // assign value is not propagated
        sessionMessageHeaderDecoder.wrapAndApplyHeader(messages, HEADER_LENGTH, messageHeaderDecoder);
        assertEquals(client.leadershipTermId(), sessionMessageHeaderDecoder.leadershipTermId());
        assertEquals(client.clusterSessionId(), sessionMessageHeaderDecoder.clusterSessionId());
        assertNotEquals(0, sessionMessageHeaderDecoder.timestamp());
        for (int i = 0; i < unfragmentedMessageLength; i++)
        {
            assertEquals((byte)0xEA, messages.getByte(SESSION_HEADER_LENGTH + HEADER_LENGTH + i));
        }

        final int offset =
            BitUtil.align(unfragmentedMessageLength + SESSION_HEADER_LENGTH + HEADER_LENGTH, FRAME_ALIGNMENT);
        headerFlyweight.wrap(messages, offset, HEADER_LENGTH);
        assertEquals(HEADER_LENGTH + SESSION_HEADER_LENGTH + messageLength, headerFlyweight.frameLength());
        assertEquals(CURRENT_VERSION, headerFlyweight.version());
        assertEquals(UNFRAGMENTED, (byte)headerFlyweight.flags());
        assertEquals(HDR_TYPE_DATA, headerFlyweight.headerType());
        assertEquals(256 + offset, headerFlyweight.termOffset());
        assertNotEquals(ingressPublication.sessionId(), headerFlyweight.sessionId());
        assertEquals(logStreamId, headerFlyweight.streamId());
        assertEquals(0, headerFlyweight.termId());
        assertEquals(DEFAULT_RESERVE_VALUE, headerFlyweight.reservedValue());
        sessionMessageHeaderDecoder.wrapAndApplyHeader(messages, HEADER_LENGTH, messageHeaderDecoder);
        assertEquals(client.leadershipTermId(), sessionMessageHeaderDecoder.leadershipTermId());
        assertEquals(client.clusterSessionId(), sessionMessageHeaderDecoder.clusterSessionId());
        assertNotEquals(0, sessionMessageHeaderDecoder.timestamp());
        for (int i = 0; i < messageLength; i++)
        {
            assertEquals((byte)0xBC, messages.getByte(HEADER_LENGTH + SESSION_HEADER_LENGTH + offset + i));
        }
    }

    @Test
    @InterruptAfter(90)
    void shouldCatchupAndJoinAsFollowerWhileSendingBigMessages()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        TestNode leader = cluster.awaitLeader();
        final int leaderId = leader.consensusModule().context().clusterMemberId();
        assertEquals(1, leader.consensusModule().context().electionCounter().get());
        final List<TestNode> followers = cluster.followers();
        for (final TestNode follower : followers)
        {
            assertEquals(1, follower.consensusModule().context().electionCounter().get());
        }

        cluster.connectClient();
        int messageCount = 1000;
        cluster.sendLargeMessages(messageCount);

        // Choose the slowest node to stop to ensure that there is quorum in the Cluster after follower is stopped
        TestNode follower1 = followers.get(0);
        TestNode follower2 = followers.get(1);
        if (follower1.appendPosition() < follower2.appendPosition())
        {
            final TestNode tmp = follower2;
            follower2 = follower1;
            follower1 = tmp;
        }
        final int follower1Id = follower1.consensusModule().context().clusterMemberId();
        final int follower2Id = follower2.consensusModule().context().clusterMemberId();
        assertNotEquals(leaderId, follower1Id);
        assertNotEquals(leaderId, follower2Id);
        assertNotEquals(follower1Id, follower2Id);

        cluster.stopNode(follower2);
        long startNs = System.nanoTime();
        long endNs = startNs + SECONDS.toNanos(10);
        final int messageLength = cluster.msgBuffer().putStringWithoutLengthAscii(0, LARGE_MSG);
        while (System.nanoTime() < endNs)
        {
            cluster.pollUntilMessageSent(messageLength);
            messageCount++;
            Tests.sleep(50);
        }

        follower2 = cluster.startStaticNode(follower2.index(), false);
        leader = cluster.awaitLeaderAndClosedElection();
        follower1 = cluster.followers().stream()
            .filter(f -> follower1Id == f.consensusModule().context().clusterMemberId())
            .findFirst()
            .orElse(null);
        assertEquals(leaderId, leader.consensusModule().context().clusterMemberId(), "leader changed");
        assertEquals(follower2Id, follower2.consensusModule().context().clusterMemberId(), "wrong follower restarted");
        assertNotNull(follower1, "first follower not found");

        startNs = System.nanoTime();
        endNs = startNs + SECONDS.toNanos(30);
        while (System.nanoTime() < endNs)
        {
            cluster.pollUntilMessageSent(messageLength);
            messageCount++;
            Tests.sleep(50);
        }

        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);
        assertThat(
            "unexpected election on leader",
            leader.consensusModule().context().electionCounter().get(),
            equalTo(1L /* startup */));
        assertThat(
            "unexpected election on follower 1",
            follower1.consensusModule().context().electionCounter().get(),
            equalTo(1L /* startup */));
        assertThat(
            "election loop detected",
            follower2.consensusModule().context().electionCounter().get(),
            equalTo(1L /* node restarted */));
    }

    @Test
    @InterruptAfter(10)
    void shouldSetClientName()
    {
        cluster = aCluster().withStaticNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final ConsensusModule.Context leaderContext = leader.consensusModule().context();
        try (Aeron aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(leaderContext.aeronDirectoryName())))
        {
            verifyClientName(aeron, leaderContext.aeron().clientId(), leaderContext.agentRoleName());

            final ClusteredServiceContainer.Context containerContext = leader.container().context();
            verifyClientName(aeron, containerContext.aeron().clientId(), containerContext.serviceName());
        }
    }

    @Test
    @SuppressWarnings("MethodLength")
    @InterruptAfter(30)
    void twoClustersCanShareArchiveAndMediaDriver(@TempDir final Path tmpDir)
    {
        final ConsensusModule.Context cmContext1 = new ConsensusModule.Context();
        final ClusteredServiceContainer.Context cscContext1 = new ClusteredServiceContainer.Context();
        final ConsensusModule.Context cmContext2 = new ConsensusModule.Context();
        final ClusteredServiceContainer.Context cscContext2 = new ClusteredServiceContainer.Context();
        final MutableInteger leadershipCounter1 = new MutableInteger();
        final MutableInteger leadershipCounter2 = new MutableInteger();
        try (TestMediaDriver mediaDriver = TestMediaDriver.launch(new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .aeronDirectoryName(tmpDir.resolve("aeron").toString()),
            systemTestWatcher);
            Archive archive = Archive.launch(new Archive.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .archiveDir(tmpDir.resolve("archive").toFile())
                .threadingMode(ArchiveThreadingMode.SHARED)
                .recordingEventsEnabled(false)
                .controlChannel("aeron:udp?endpoint=localhost:8888|term-length=64k")
                .replicationChannel("aeron:udp?endpoint=localhost:0"));
            ConsensusModule consensusModule1 = ConsensusModule.launch(cmContext1
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .clusterId(5)
                .serviceCount(1)
                .clusterDir(tmpDir.resolve("cluster-" + cmContext1.clusterId()).toFile())
                .ingressChannel("aeron:udp?term-length=128k|alias=ingress-cluster-" + cmContext1.clusterId())
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .clusterMembers("0,localhost:8811,localhost:8822,localhost:8833,localhost:0,localhost:8888")
                .consensusModuleStreamId(cmContext1.consensusModuleStreamId() + 100)
                .serviceStreamId(cmContext1.serviceStreamId() + 100)
                .snapshotStreamId(cmContext1.snapshotStreamId() + 100)
                .replayStreamId(cmContext1.replayStreamId() + 100));
            ClusteredServiceContainer clusteredServiceContainer1 = ClusteredServiceContainer.launch(cscContext1
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .clusterDir(consensusModule1.context().clusterDir())
                .clusterId(consensusModule1.context().clusterId())
                .serviceStreamId(cmContext1.serviceStreamId())
                .consensusModuleStreamId(cmContext1.consensusModuleStreamId())
                .snapshotStreamId(cscContext1.snapshotStreamId() + 100)
                .replayStreamId(cmContext1.replayStreamId())
                .serviceId(0)
                .serviceName("test1")
                .clusteredService(new TestNode.TestService()
                {
                    public void onNewLeadershipTermEvent(
                        final long leadershipTermId,
                        final long logPosition,
                        final long timestamp,
                        final long termBaseLogPosition,
                        final int leaderMemberId,
                        final int logSessionId,
                        final TimeUnit timeUnit,
                        final int appVersion)
                    {
                        leadershipCounter1.increment();
                    }
                }));
            ConsensusModule consensusModule2 = ConsensusModule.launch(cmContext2
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .clusterId(7)
                .serviceCount(1)
                .clusterDir(tmpDir.resolve("cluster-" + cmContext2.clusterId()).toFile())
                .ingressChannel("aeron:udp?term-length=128k|alias=ingress-cluster-" + cmContext2.clusterId())
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .clusterMembers("0,localhost:9911,localhost:9922,localhost:9933,localhost:0,localhost:8888")
                .consensusModuleStreamId(cmContext2.consensusModuleStreamId() + 200)
                .serviceStreamId(cmContext2.serviceStreamId() + 200)
                .snapshotStreamId(cmContext2.snapshotStreamId() + 200)
                .replayStreamId(cmContext2.replayStreamId() + 200));
            ClusteredServiceContainer clusteredServiceContainer2 = ClusteredServiceContainer.launch(cscContext2
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .clusterDir(consensusModule2.context().clusterDir())
                .clusterId(consensusModule2.context().clusterId())
                .serviceStreamId(cmContext2.serviceStreamId())
                .consensusModuleStreamId(cmContext2.consensusModuleStreamId())
                .snapshotStreamId(cscContext2.snapshotStreamId() + 100)
                .replayStreamId(cmContext2.replayStreamId())
                .serviceId(0)
                .serviceName("test2")
                .clusteredService(new TestNode.TestService()
                {
                    public void onNewLeadershipTermEvent(
                        final long leadershipTermId,
                        final long logPosition,
                        final long timestamp,
                        final long termBaseLogPosition,
                        final int leaderMemberId,
                        final int logSessionId,
                        final TimeUnit timeUnit,
                        final int appVersion)
                    {
                        leadershipCounter2.increment();
                    }
                })))
        {
            Tests.await(() ->
                ElectionState.CLOSED == ElectionState.get(consensusModule1.context().electionStateCounter()));
            Tests.await(() ->
                ElectionState.CLOSED == ElectionState.get(consensusModule2.context().electionStateCounter()));

            assertEquals(1L, consensusModule1.context().electionCounter().get());
            assertEquals(1L, consensusModule2.context().electionCounter().get());
            Tests.await(() -> 1 == leadershipCounter1.get() && 1 == leadershipCounter2.get());

            try (AeronArchive aeronArchive = AeronArchive.connect(new AeronArchive.Context()
                .aeronDirectoryName(archive.context().aeronDirectoryName())
                .controlRequestChannel(archive.context().controlChannel())
                .controlResponseChannel("aeron:udp?endpoint=localhost:0")))
            {
                final IntHashSet logSessions = new IntHashSet();
                assertEquals(2, aeronArchive.listRecordings(
                    0,
                    Integer.MAX_VALUE,
                    (controlSessionId,
                    correlationId,
                    recordingId,
                    startTimestamp,
                    stopTimestamp,
                    startPosition,
                    stopPosition,
                    initialTermId,
                    segmentFileLength,
                    termBufferLength,
                    mtuLength,
                    sessionId,
                    streamId,
                    strippedChannel,
                    originalChannel,
                    sourceIdentity) ->
                    {
                        assertThat(originalChannel, CoreMatchers.containsString("alias=log"));
                        logSessions.add(sessionId);
                    }),
                    "wrong number of recordings");
                assertEquals(2, logSessions.size());
            }

            try (AeronCluster client = AeronCluster.connect(new AeronCluster.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .ingressChannel("aeron:udp?term-length=128k")
                .ingressEndpoints("0=localhost:8811")
                .egressChannel("aeron:udp?endpoint=localhost:0")))
            {
                assertEquals(1, client.clusterSessionId());
            }

            final MutableLong client1SessionId = new MutableLong();
            final MutableLong client2SessionId = new MutableLong();
            final MutableInteger clientResponsesCount1 = new MutableInteger();
            final MutableInteger clientResponsesCount2 = new MutableInteger();
            try (AeronCluster client1 = AeronCluster.connect(new AeronCluster.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .ingressChannel("aeron:udp?term-length=128k")
                .ingressEndpoints("0=localhost:8811")
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .egressListener((clusterSessionId, timestamp, buffer, offset, length, header) ->
                {
                    assertEquals(client1SessionId.get(), clusterSessionId);
                    clientResponsesCount1.getAndIncrement();
                }));

                AeronCluster client2 = AeronCluster.connect(new AeronCluster.Context()
                    .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                    .ingressChannel("aeron:udp?term-length=128k")
                    .ingressEndpoints("0=localhost:9911")
                    .egressChannel("aeron:udp?endpoint=localhost:0")
                    .egressListener((clusterSessionId, timestamp, buffer, offset, length, header) ->
                    {
                        assertEquals(client2SessionId.get(), clusterSessionId);
                        clientResponsesCount2.getAndIncrement();
                    })))
            {
                assertNotEquals(client1.clusterSessionId(), client2.clusterSessionId());
                client1SessionId.set(client1.clusterSessionId());
                client2SessionId.set(client2.clusterSessionId());

                final UnsafeBuffer msgBuf = new UnsafeBuffer(new byte[32]);
                ThreadLocalRandom.current().nextBytes(msgBuf.byteArray());

                for (int i = 0; i < 3; i++)
                {
                    while (client1.offer(msgBuf, 0, 16) < 0)
                    {
                        client1.pollEgress();
                    }
                }

                ThreadLocalRandom.current().nextBytes(msgBuf.byteArray());
                for (int i = 0; i < 5; i++)
                {
                    while (client2.offer(msgBuf, 0, msgBuf.capacity()) < 0)
                    {
                        client2.pollEgress();
                    }
                }

                Tests.await(() ->
                {
                    client2.pollEgress();
                    final TestNode.TestService service =
                        (TestNode.TestService)clusteredServiceContainer2.context().clusteredService();
                    return 5 == service.messageCount();
                });

                Tests.await(() ->
                {
                    client1.pollEgress();
                    final TestNode.TestService service =
                        (TestNode.TestService)clusteredServiceContainer1.context().clusteredService();
                    return 3 == service.messageCount();
                });

                assertEquals(3,
                    ((TestNode.TestService)clusteredServiceContainer1.context().clusteredService()).messageCount());
                assertEquals(5,
                    ((TestNode.TestService)clusteredServiceContainer2.context().clusteredService()).messageCount());
            }
        }
    }


    @Test
    void allNodesShouldHaveIdenticalSnapshots()
    {
        final MutableInteger sessionCounter = new MutableInteger(0);

        cluster = aCluster(true)
                .withStaticNodes(3)
                .withAuthenticationSupplier(() -> new Authenticator()
                {
                    @Override
                    public void onConnectRequest(
                        final long sessionId, final byte[] encodedCredentials, final long nowMs)
                    {
                        sessionCounter.increment();
                    }

                    @Override
                    public void onChallengeResponse(
                        final long sessionId, final byte[] encodedCredentials, final long nowMs)
                    {
                    }

                    @Override
                    public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs)
                    {
                        if (sessionCounter.get() > 0)
                        {
                            sessionProxy.reject();
                        }
                    }

                    @Override
                    public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs)
                    {
                    }
                })
                .start();

//        try (Subscription subscription = aeron.addSubscription(replayChannel, streamId)) {
//            final Image image = awaitImage(sessionId, subscription);
//        }
//        final ConsensusModuleSnapshotAdapter adapter =
//              new ConsensusModuleSnapshotAdapter(cluster.node(0).consensusModule(),
//              new ConsensusModuleSnapshotListener() {
//            @Override
//            public void onLoadBeginSnapshot(int appVersion, TimeUnit timeUnit,
//              DirectBuffer buffer, int offset, int length) {
//            }
//
//            @Override
//            public void onLoadConsensusModuleState(long nextSessionId, long nextServiceSessionId,
//            long logServiceSessionId, int pendingMessageCapacity, DirectBuffer buffer,
//            int offset, int length) {
//            }
//
//            @Override
//            public void onLoadPendingMessage(long clusterSessionId, DirectBuffer buffer,
//            int offset, int length) {
//            }
//
//            @Override
//            public void onLoadClusterSession(long clusterSessionId, long correlationId, long openedLogPosition,
//              long timeOfLastActivity, CloseReason closeReason, int responseStreamId, String responseChannel,
//              DirectBuffer buffer, int offset, int length) {
//            }
//
//            @Override
//            public void onLoadTimer(long correlationId, long deadline, DirectBuffer buffer,
//            int offset, int length) {
//
//            }
//
//            @Override
//            public void onLoadPendingMessageTracker(long nextServiceSessionId, long logServiceSessionId,
//              int pendingMessageCapacity, int serviceId, DirectBuffer buffer, int offset, int length) {
//            }
//
//            @Override
//            public void onLoadEndSnapshot(DirectBuffer buffer, int offset, int length) {
//            }
//        });
        systemTestWatcher.cluster(cluster);
        final TestNode leader = cluster.awaitLeader();
        // dont do this.  do AeronCluster.connect()
        assertThrowsExactly(AuthenticationException.class, () -> cluster.connectClient());
        //AeronCluster myCluster = AeronCluster.connect();
        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);

        final List<byte[]> snapshots = IntStream.range(0, 3)
            .mapToObj(nodeIdx -> readLatestSnapshot(cluster.node(nodeIdx)))
            .toList();
        for (int nodeIdx = 1; nodeIdx < 3; nodeIdx++)
        {
            assertArrayEquals(snapshots.get(nodeIdx), snapshots.get(0));
        }
    }

    private byte[] readLatestSnapshot(final TestNode node)
    {
        final long recordingId;
        try (RecordingLog recordingLog = new RecordingLog(node.consensusModule().context().clusterDir(), false))
        {
            final RecordingLog.Entry snapshot = recordingLog.getLatestSnapshot(-1);
            assertNotNull(snapshot);

            recordingId = snapshot.recordingId;
        }

        final AeronArchive.Context archiveCtx = new AeronArchive.Context()
            .controlRequestChannel(node.archive().context().localControlChannel())
            .controlResponseChannel(node.archive().context().localControlChannel())
            .controlRequestStreamId(node.archive().context().localControlStreamId())
            .aeronDirectoryName(node.mediaDriver().aeronDirectoryName());

        try (
            AeronArchive archive = AeronArchive.connect(archiveCtx);
            Subscription subscription = archive.replay(recordingId, NULL_POSITION, Long.MAX_VALUE, "aeron:ipc", 12345)
        )
        {
            final ExpandableArrayBuffer destBuffer = new ExpandableArrayBuffer();
            final MutableInteger destOffset = new MutableInteger();
            final IdleStrategy idleStrategy = new BackoffIdleStrategy();

            while (subscription.imageCount() == 0)
            {
                if (Thread.interrupted())
                {
                    fail("interrupted");
                }

                idleStrategy.idle();
            }
            idleStrategy.reset();

            final Image image = subscription.imageAtIndex(0);
            while (!image.isClosed())
            {
                if (Thread.interrupted())
                {
                    fail("interrupted");
                }

                final long work = image.poll((srcBuffer, offset, length, header) ->
                {
                    destBuffer.putBytes(destOffset.getAndAdd(length), srcBuffer, offset, length);
                }, Integer.MAX_VALUE);
                idleStrategy.idle((int)work);
            }

            return Arrays.copyOf(destBuffer.byteArray(), destOffset.get());
        }
    }

    private static void verifyClientName(final Aeron aeron, final long targetClientId, final String expectedClientName)
    {
        assertNotEquals(aeron.clientId(), targetClientId);
        final CountersReader countersReader = aeron.countersReader();
        int counterId = NULL_COUNTER_ID;
        String counterLabel = null;
        while (true)
        {
            if (NULL_COUNTER_ID == counterId)
            {
                counterId = HeartbeatTimestamp.findCounterIdByRegistrationId(
                    countersReader, HEARTBEAT_TYPE_ID, targetClientId);
            }
            else if (null == counterLabel || !counterLabel.contains("name="))
            {
                counterLabel = countersReader.getCounterLabel(counterId);
            }
            else
            {
                assertThat(counterLabel, containsString(expectedClientName));
                break;
            }
            Tests.checkInterruptStatus();
        }
    }

    private void shouldCatchUpAfterFollowerMissesMessage(final String message)
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        TestNode follower = cluster.followers().get(0);

        cluster.stopNode(follower);

        cluster.connectClient();
        final int messageLength = cluster.msgBuffer().putStringWithoutLengthAscii(0, message);
        cluster.pollUntilMessageSent(messageLength);
        cluster.awaitResponseMessageCount(1);

        follower = cluster.startStaticNode(follower.index(), false);

        awaitElectionClosed(follower);
        assertEquals(FOLLOWER, follower.role());
    }

    private MutableBoolean injectAdminResponseEgressListener(
        final long expectedCorrelationId,
        final AdminRequestType expectedRequestType,
        final AdminResponseCode expectedResponseCode,
        final String expectedMessage)
    {
        final MutableBoolean hasResponse = new MutableBoolean();

        cluster.egressListener(
            new EgressListener()
            {
                public void onMessage(
                    final long clusterSessionId,
                    final long timestamp,
                    final DirectBuffer buffer,
                    final int offset,
                    final int length,
                    final Header header)
                {
                }

                public void onAdminResponse(
                    final long clusterSessionId,
                    final long correlationId,
                    final AdminRequestType requestType,
                    final AdminResponseCode responseCode,
                    final String message,
                    final DirectBuffer payload,
                    final int payloadOffset,
                    final int payloadLength)
                {
                    hasResponse.set(true);
                    assertEquals(expectedCorrelationId, correlationId);
                    assertEquals(expectedRequestType, requestType);
                    assertEquals(expectedResponseCode, responseCode);
                    assertEquals(expectedMessage, message);
                    assertNotNull(payload);
                    final int minPayloadOffset =
                        MessageHeaderEncoder.ENCODED_LENGTH +
                        AdminResponseEncoder.BLOCK_LENGTH +
                        AdminResponseEncoder.messageHeaderLength() +
                        message.length() +
                        AdminResponseEncoder.payloadHeaderLength();
                    assertTrue(payloadOffset > minPayloadOffset);
                    assertEquals(0, payloadLength);
                }
            });

        return hasResponse;
    }

    private MutableBoolean injectAdminRequestControlledEgressListener(final long expectedCorrelationId)
    {
        final MutableBoolean hasResponse = new MutableBoolean();

        cluster.controlledEgressListener(
            new ControlledEgressListener()
            {
                public ControlledFragmentHandler.Action onMessage(
                    final long clusterSessionId,
                    final long timestamp,
                    final DirectBuffer buffer,
                    final int offset,
                    final int length,
                    final Header header)
                {
                    return ControlledFragmentHandler.Action.ABORT;
                }

                public void onAdminResponse(
                    final long clusterSessionId,
                    final long correlationId,
                    final AdminRequestType requestType,
                    final AdminResponseCode responseCode,
                    final String message,
                    final DirectBuffer payload,
                    final int payloadOffset,
                    final int payloadLength)
                {
                    hasResponse.set(true);
                    assertEquals(expectedCorrelationId, correlationId);
                    assertEquals(AdminRequestType.SNAPSHOT, requestType);
                    assertEquals(AdminResponseCode.OK, responseCode);
                    assertEquals(EMPTY_MSG, message);
                    assertNotNull(payload);
                    final int minPayloadOffset =
                        MessageHeaderEncoder.ENCODED_LENGTH +
                        AdminResponseEncoder.BLOCK_LENGTH +
                        AdminResponseEncoder.messageHeaderLength() +
                        message.length() +
                        AdminResponseEncoder.payloadHeaderLength();
                    assertTrue(payloadOffset > minPayloadOffset);
                    assertEquals(0, payloadLength);
                }
            });

        return hasResponse;
    }
}
