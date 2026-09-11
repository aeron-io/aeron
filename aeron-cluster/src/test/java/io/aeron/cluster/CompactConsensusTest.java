/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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

import io.aeron.cluster.codecs.*;
import static org.junit.jupiter.api.Assertions.*;

import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.UnavailableImageHandler;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.AppendPositionDecoder;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.driver.DutyCycleTracker;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.security.AuthorisationService;
import io.aeron.security.DefaultAuthenticatorSupplier;
import io.aeron.status.ReadableCounter;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestClusterClock;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

import static java.lang.Boolean.TRUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CompactConsensusTest
{

    private final EgressPublisher mockEgressPublisher = mock(EgressPublisher.class);
    private final LogPublisher mockLogPublisher = mock(LogPublisher.class);
    private final Aeron mockAeron = mock(Aeron.class);
    private final ConcurrentPublication mockResponsePublication = mock(ConcurrentPublication.class);
    private final ExclusivePublication mockExclusivePublication = mock(ExclusivePublication.class);
    private final Counter mockTimedOutClientCounter = mock(Counter.class);
    private final LongConsumer mockTimeConsumer = mock(LongConsumer.class);

    private final ConsensusModule.Context ctx = TestContexts.localhostConsensusModule()
        .errorHandler(Tests::onError)
        .errorCounter(mock(AtomicCounter.class))
        .countedErrorHandler(mock(CountedErrorHandler.class))
        .moduleStateCounter(mock(Counter.class))
        .commitPositionCounter(mock(Counter.class))
        .controlToggleCounter(mock(Counter.class))
        .nodeControlToggleCounter(mock(Counter.class))
        .clusterNodeRoleCounter(mock(Counter.class))
        .electionCounter(mock(Counter.class))
        .leadershipTermIdCounter(mock(Counter.class))
        .timedOutClientCounter(mockTimedOutClientCounter)
        .clusterTimeConsumerSupplier((ctx) -> mockTimeConsumer)
        .idleStrategySupplier(NoOpIdleStrategy::new)
        .timerServiceSupplier((clusterClock, timerHandler) -> mock(TimerService.class))
        .aeron(mockAeron)
        .clusterMemberId(0)
        .authenticatorSupplier(new DefaultAuthenticatorSupplier())
        .authorisationServiceSupplier(() -> AuthorisationService.DENY_ALL)
        .clusterMarkFile(mock(ClusterMarkFile.class))
        .archiveContext(new AeronArchive.Context())
        .logPublisher(mockLogPublisher)
        .egressPublisher(mockEgressPublisher)
        .dutyCycleTracker(new DutyCycleTracker());

    @BeforeEach
    void before()
    {
        when(mockAeron.conductorAgentInvoker()).thenReturn(mock(AgentInvoker.class));
        when(mockEgressPublisher.sendEvent(any(), anyLong(), anyInt(), any(), any())).thenReturn(TRUE);
        when(mockLogPublisher.appendSessionClose(anyInt(), any(), anyLong(), anyLong(), any())).thenReturn(TRUE);
        when(mockLogPublisher.appendSessionOpen(any(), anyLong(), anyLong())).thenReturn(128L);
        when(mockLogPublisher.appendClusterAction(anyLong(), anyLong(), any(ClusterAction.class), anyInt()))
            .thenReturn(TRUE);
        when(mockAeron.addPublication(anyString(), anyInt())).thenReturn(mockResponsePublication);
        when(mockAeron.getPublication(anyLong())).thenReturn(mockResponsePublication);
        when(mockAeron.addExclusivePublication(anyString(), anyInt())).thenReturn(mockExclusivePublication);
        when(mockAeron.addSubscription(anyString(), anyInt())).thenReturn(mock(Subscription.class));
        when(mockAeron.addSubscription(anyString(), anyInt(), eq(null), any(UnavailableImageHandler.class)))
            .thenReturn(mock(Subscription.class));
        when(mockResponsePublication.isConnected()).thenReturn(TRUE);
        when(mockResponsePublication.availableWindow()).thenReturn(Long.MAX_VALUE);
    }

    @Test
    void shouldConfirmThroughRealCodecsOnlyAfterBroadcastAndEcho()
    {
        final Pair pair = new Pair();
        final long token = pair.leader.agent.triggerQuorumConfirmation();
        assertFalse(pair.leader.agent.isLeadershipConfirmedSince(token));
        pair.broadcast();
        assertFalse(pair.leader.agent.isLeadershipConfirmedSince(token));
        pair.echo();
        assertTrue(pair.leader.agent.isLeadershipConfirmedSince(token));
        assertEquals(24, CompactCommitPositionEncoder.BLOCK_LENGTH);
        assertEquals(20, CompactLeadershipConfirmAckEncoder.BLOCK_LENGTH);
        assertEquals(8, CompactLeadershipConfirmAckEncoder.confirmationCounterEncodingOffset());
        assertEquals(24, CommitPositionEncoder.BLOCK_LENGTH);
        assertEquals(16, LeadershipConfirmAckEncoder.BLOCK_LENGTH);
        final long next = pair.leader.agent.triggerQuorumConfirmation();
        assertFalse(pair.leader.agent.isLeadershipConfirmedSince(next));
    }

    @Test
    void shouldKeepOldEchoesStaleAcrossHalfRangeAndMultipleFullWraps()
    {
        final Pair pair = new Pair();
        for (final long value : new long[]{ (1L << 31) + 2, (1L << 34) + 2 })
        {
            final CompactConfirmation state = Tests.getField(pair.leader.agent, "compactConfirmation");
            Tests.setField(state, "round", value);
            final long token = pair.leader.agent.triggerQuorumConfirmation();
            assertFalse(pair.leader.agent.isLeadershipConfirmedSince(token));
            pair.leader.agent.onCompactLeadershipConfirmAck(42, 1, 1, pair.toLeader.image);
            assertFalse(pair.leader.agent.isLeadershipConfirmedSince(token));
            pair.broadcast();
            pair.echo();
            assertTrue(pair.leader.agent.isLeadershipConfirmedSince(token));
        }
    }

    @Test
    void shouldRejectUnboundMismatchedClosedAndWrongTermAcknowledgements()
    {
        final Pair pair = new Pair();
        final long token = pair.leader.agent.triggerQuorumConfirmation();
        pair.broadcast();
        pair.leader.agent.onCompactLeadershipConfirmAck(42, 1, token + 1, mock(Image.class));
        pair.leader.agent.onCompactLeadershipConfirmAck(42, 2, token + 1, pair.toLeader.image);
        pair.leader.agent.onCompactLeadershipConfirmAck(41, 1, token + 1, pair.toLeader.image);
        pair.leader.agent.onCompactLeadershipConfirmAck(42 + (1L << 32), 1, token + 1, pair.toLeader.image);
        pair.leader.agent.onCompactLeadershipConfirmAck(42, 1, token + 2, pair.toLeader.image);
        final UnsafeBuffer legacyAck = new UnsafeBuffer(new byte[64]);
        new LeadershipConfirmAckEncoder().wrapAndApplyHeader(legacyAck, 0, new MessageHeaderEncoder())
            .leadershipTermId(42).followerMemberId(1).confirmationCounter((int)token + 1);
        pair.toLeader.adapter.onFragment(legacyAck, 0,
            MessageHeaderEncoder.ENCODED_LENGTH + LeadershipConfirmAckEncoder.BLOCK_LENGTH,
            new Header(0, 0, pair.toLeader.image));
        assertFalse(pair.leader.agent.isLeadershipConfirmedSince(token));
        when(pair.toLeader.image.isClosed()).thenReturn(true);
        pair.leader.agent.onCompactLeadershipConfirmAck(42, 1, token + 1, pair.toLeader.image);
        assertFalse(pair.leader.agent.isLeadershipConfirmedSince(token));
        when(pair.toLeader.image.isClosed()).thenReturn(false);
        pair.echo();
        assertTrue(pair.leader.agent.isLeadershipConfirmedSince(token));
    }

    @Test
    void shouldRejectUnboundCommitAndPreventImageIdentityReassignment()
    {
        final Pair pair = new Pair();
        final CompactConfirmation state = Tests.getField(pair.follower.agent, "compactConfirmation");
        final long before = state.followerRound();
        final Image unbound = mock(Image.class);
        pair.follower.agent.onCompactCommitPosition(42, 200, 100, unbound);
        assertEquals(before, state.followerRound());
        pair.follower.agent.onConsensusConnection(2, pair.toFollower.image);
        pair.follower.agent.onCompactCommitPosition(41, 200, 100, pair.toFollower.image);
        assertEquals(before, state.followerRound());
        pair.follower.agent.onCompactCommitPosition(42, 200, 100, pair.toFollower.image);
        assertEquals(100, state.followerRound());
    }

    @Test
    void shouldPreserveElectionCommitPositionHandling()
    {
        final Pair pair = new Pair();
        final Election election = mock(Election.class);
        Tests.setField(pair.follower.agent, "election", election);
        pair.follower.agent.onCompactCommitPosition(43, 200, 100, pair.toFollower.image);
        verify(election).onCommitPosition(43, 200, 0);
        final CompactConfirmation state = Tests.getField(pair.follower.agent, "compactConfirmation");
        assertNotEquals(100, state.followerRound());
    }

    @Test
    void shouldKeepReplicationButNotCreditLegacyPeers()
    {
        final Pair pair = new Pair();
        pair.leader.agent.onConsensusPeerImage(1, mock(Image.class));
        pair.toFollower.messages.clear();
        pair.toFollower.delivered = 0;
        final long token = pair.leader.agent.triggerQuorumConfirmation();
        pair.broadcast();
        assertEquals(ConsensusConnectionDecoder.TEMPLATE_ID, pair.toFollower.template(0));
        assertEquals(CommitPositionDecoder.TEMPLATE_ID, pair.toFollower.template(1));
        assertFalse(pair.leader.agent.isLeadershipConfirmedSince(token));
        pair.leader.agent.onConsensusConnection(1, pair.toLeader.image);
        pair.broadcast();
        pair.echo();
        assertTrue(pair.leader.agent.isLeadershipConfirmedSince(token));
    }

    @Test
    void shouldNotAcceptEchoForBackpressuredBroadcast()
    {
        final Pair pair = new Pair();
        final long token = pair.leader.agent.triggerQuorumConfirmation();
        pair.toFollower.credit = 0;
        pair.broadcast();
        pair.leader.agent.onCompactLeadershipConfirmAck(42, 1, token + 1, pair.toLeader.image);
        assertFalse(pair.leader.agent.isLeadershipConfirmedSince(token));
        pair.toFollower.credit = 100;
        pair.broadcast();
        pair.echo();
        assertTrue(pair.leader.agent.isLeadershipConfirmedSince(token));
    }

    @Test
    void shouldShareSingleFrameCreditBetweenPersistentAppendAndCompactAck()
    {
        final Pair pair = new Pair();
        pair.toLeader.messages.clear();
        pair.toLeader.delivered = 0;
        for (int cycle = 1; cycle <= 32; cycle++)
        {
            pair.follower.recordedPosition = cycle * 64L;
            pair.follower.agent.onCompactCommitPosition(42, cycle * 64L, 100 + cycle, pair.toFollower.image);
            pair.toLeader.credit = 1;
            pair.follower.agent.updateFollowerPosition(cycle + 1000);
            assertEquals(0 == cycle % 2 ? CompactLeadershipConfirmAckDecoder.TEMPLATE_ID :
                AppendPositionDecoder.TEMPLATE_ID, pair.toLeader.template(cycle - 1));
        }
        assertEquals(32, pair.toLeader.messages.size());
    }

    @Test
    void shouldDiscardOldTokensAndRejectConfirmationDuringElection()
    {
        final Pair pair = new Pair();
        final long token = pair.leader.agent.triggerQuorumConfirmation();
        pair.broadcast();
        pair.echo();
        assertTrue(pair.leader.agent.isLeadershipConfirmedSince(token));
        completeElection(pair.leader.agent, 42 + (1L << 32), pair.leader.members[0], 100);
        assertFalse(pair.leader.agent.isLeadershipConfirmedSince(token));
        Tests.setField(pair.leader.agent, "election", mock(Election.class));
        assertEquals(Aeron.NULL_VALUE, pair.leader.agent.triggerQuorumConfirmation());
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 2, 3 })
    void shouldRecoverImageOnlyReconnectWithoutElection(final int directions)
    {
        final Pair pair = new Pair();
        if (0 != (directions & 1))
        {
            pair.toFollower.replaceImage();
        }
        if (0 != (directions & 2))
        {
            pair.toLeader.replaceImage();
        }
        final long token = pair.leader.agent.triggerQuorumConfirmation();
        for (int cycle = 0; cycle < 12; cycle++)
        {
            pair.broadcast();
            pair.follower.recordedPosition += 64;
            pair.echo();
        }
        assertTrue(pair.leader.agent.isLeadershipConfirmedSince(token));
        assertEquals(42L, Tests.<Long>getField(pair.leader.agent, "leadershipTermId"));
        assertSame(pair.toFollower.publication, pair.leader.members[1].publication());
        assertSame(pair.toLeader.publication, pair.follower.members[0].publication());

        // Recovery must settle: healthy reads still take one broadcast/echo and produce no announcements.
        final int commitsBefore = pair.toFollower.messages.size();
        final int acksBefore = pair.toLeader.messages.size();
        for (int read = 0; read < 16; read++)
        {
            final long fresh = pair.leader.agent.triggerQuorumConfirmation();
            pair.broadcast();
            assertFalse(pair.leader.agent.isLeadershipConfirmedSince(fresh));
            pair.echo();
            assertTrue(pair.leader.agent.isLeadershipConfirmedSince(fresh));
        }
        assertEquals(commitsBefore + 16, pair.toFollower.messages.size());
        assertEquals(acksBefore + 16, pair.toLeader.messages.size());
        for (int i = commitsBefore; i < pair.toFollower.messages.size(); i++)
        {
            assertEquals(CompactCommitPositionDecoder.TEMPLATE_ID, pair.toFollower.template(i));
        }
        for (int i = acksBefore; i < pair.toLeader.messages.size(); i++)
        {
            assertEquals(CompactLeadershipConfirmAckDecoder.TEMPLATE_ID, pair.toLeader.template(i));
        }
    }

    @Test
    void shouldRetryImageRecoveryAfterBackpressureWithoutLogProgress()
    {
        final Pair pair = new Pair();
        final long token = pair.leader.agent.triggerQuorumConfirmation();
        pair.toFollower.replaceImage();
        pair.broadcast();
        assertTrue(Tests.<Boolean>getField(pair.follower.agent, "leaderAnnouncementRequested"));
        pair.toLeader.credit = 0;
        pair.echo();
        assertFalse(pair.leader.agent.isLeadershipConfirmedSince(token));
        pair.toLeader.credit = 100;
        pair.echo();
        final UnsafeBuffer request = pair.toLeader.messages.get(pair.toLeader.messages.size() - 1);
        assertEquals(ConsensusModuleAgent.APPEND_POSITION_FLAG_REQUEST_ANNOUNCEMENT,
            new AppendPositionDecoder().wrap(request,
                DataHeaderFlyweight.HEADER_LENGTH + MessageHeaderDecoder.ENCODED_LENGTH,
                AppendPositionDecoder.BLOCK_LENGTH, AppendPositionDecoder.SCHEMA_VERSION).flags());

        // The request makes the leader's heartbeat immediately due, even with no new log position.
        pair.toFollower.credit = 0;
        assertEquals(1, pair.leader.agent.updateLeaderPosition(1_000, 0, 0));
        pair.toFollower.credit = 100;
        final long retryTime = TimeUnit.SECONDS.toNanos(1);
        pair.follower.agent.updateFollowerPosition(retryTime);
        pair.toLeader.deliver();
        assertEquals(1, pair.leader.agent.updateLeaderPosition(retryTime, 0, 0));
        pair.toFollower.deliver();
        pair.echo();
        assertTrue(pair.leader.agent.isLeadershipConfirmedSince(token));
        assertFalse(Tests.<Boolean>getField(pair.follower.agent, "leaderAnnouncementRequested"));
    }

    @Test
    void shouldRecoverWhenBothImagesChangeAndTheRecoveryAnnouncementIsLost()
    {
        final Pair pair = new Pair();
        pair.toLeader.replaceImage();
        pair.follower.recordedPosition += 64;
        pair.echo();
        pair.leader.agent.publishCommitPosition(100, 42);
        // Drop the offered announcement by replacing the receiving Image before delivery.
        pair.toFollower.replaceImage();
        final long token = pair.leader.agent.triggerQuorumConfirmation();
        for (int cycle = 0; cycle < 12; cycle++)
        {
            pair.broadcast();
            pair.follower.recordedPosition += 64;
            pair.echo();
        }
        assertTrue(pair.leader.agent.isLeadershipConfirmedSince(token));
    }

    @Test
    void shouldClearRecoveryRequestsOnElectionAndIgnoreInvalidCompactTraffic()
    {
        final Pair pair = new Pair();
        final Image unknown = mock(Image.class);
        pair.follower.agent.onCompactCommitPosition(41, 0, 100, unknown);
        pair.follower.agent.onCompactCommitPosition(42, 0, -1, unknown);
        when(unknown.isClosed()).thenReturn(true);
        pair.follower.agent.onCompactCommitPosition(42, 0, 100, unknown);
        assertFalse(Tests.<Boolean>getField(pair.follower.agent, "leaderAnnouncementRequested"));
        when(unknown.isClosed()).thenReturn(false);
        pair.follower.agent.onCompactCommitPosition(42, 0, 100, unknown);
        assertTrue(Tests.<Boolean>getField(pair.follower.agent, "leaderAnnouncementRequested"));
        completeElection(pair.follower.agent, 43, pair.follower.members[0], 0);
        assertFalse(Tests.<Boolean>getField(pair.follower.agent, "leaderAnnouncementRequested"));
    }

    @Test
    void shouldRecoverWhenTheInitialLeaderAnnouncementsAreLost()
    {
        final Pair pair = new Pair(false);
        pair.echo();
        pair.leader.agent.publishCommitPosition(100, 42);
        pair.toLeader.replaceImage();
        pair.follower.recordedPosition += 64;
        pair.echo();
        pair.leader.agent.publishCommitPosition(100, 42);
        // The follower has never bound any leader Image; lose both queued announcements.
        pair.toFollower.replaceImage();
        assertNull(Tests.getField(pair.follower.members[0].compactConfirmation, "image"));
        final long token = pair.leader.agent.triggerQuorumConfirmation();
        for (int cycle = 0; cycle < 12; cycle++)
        {
            pair.broadcast();
            pair.follower.recordedPosition += 64;
            pair.echo();
        }
        assertTrue(pair.leader.agent.isLeadershipConfirmedSince(token));
    }

    @Test
    void shouldRequestBindingForLivePeerImagesObservedBeforeTheHandshake()
    {
        final Pair pair = new Pair(false);
        // Append-position traffic identifies a peer Image but does not bind it for compact commits.
        pair.leader.agent.onConsensusPeerImage(1, pair.toLeader.image);
        pair.follower.agent.onConsensusPeerImage(0, pair.toFollower.image);
        pair.leader.members[1].compactConfirmation.announced(pair.toFollower.publication);
        pair.follower.members[0].compactConfirmation.announced(pair.toLeader.publication);
        final long token = pair.leader.agent.triggerQuorumConfirmation();
        for (int cycle = 0; cycle < 12; cycle++)
        {
            pair.broadcast();
            pair.follower.recordedPosition += 64;
            pair.echo();
        }
        assertTrue(pair.leader.agent.isLeadershipConfirmedSince(token));
    }

    private final class Pair
    {
        private final Node leader = new Node(0, Cluster.Role.LEADER);
        private final Node follower = new Node(1, Cluster.Role.FOLLOWER);
        private final Link toFollower = new Link(follower);
        private final Link toLeader = new Link(leader);
        private long position = 64;

        private Pair()
        {
            this(true);
        }

        private Pair(final boolean connect)
        {
            leader.members[1].publication(toFollower.publication);
            follower.members[0].publication(toLeader.publication);
            if (connect)
            {
                echo();
                broadcast();
                echo();
                broadcast();
                echo();
            }
        }

        private void broadcast()
        {
            position += 64;
            leader.agent.updateLeaderPosition(position, position, position);
            toFollower.deliver();
        }

        private void echo()
        {
            follower.agent.updateFollowerPosition(++follower.nowNs);
            toLeader.deliver();
        }
    }

    private final class Node
    {
        private final ConsensusModuleAgent agent;
        private final ClusterMember[] members;
        private long recordedPosition;
        private long nowNs;

        private Node(final int id, final Cluster.Role role)
        {
            final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
            final ConsensusModule.Context context = ctx.clone().clusterMemberId(id)
                .clusterMembers(id + ",localhost:20000,localhost:20001,localhost:20002,localhost:0,localhost:8010")
                .epochClock(clock.asEpochClock()).clusterClock(clock)
                .recordingLog(mock(RecordingLog.class)).ingressChannel("aeron:udp");
            agent = new ConsensusModuleAgent(context);
            agent.state(ConsensusModule.State.ACTIVE, "");
            agent.role(role);
            members = setActiveMembers(agent, 0, 1, 2);
            Tests.setField(agent, "consensusPublisher", new ConsensusPublisher());
            final ReadableCounter appendPosition = mock(ReadableCounter.class);
            when(appendPosition.get()).thenAnswer(invocation -> recordedPosition);
            Tests.setField(agent, "appendPosition", appendPosition);
            completeElection(agent, 42, members[0], 0);
        }
    }

    private static final class Link
    {
        private Image image = mock(Image.class);
        private final ExclusivePublication publication = mock(ExclusivePublication.class);
        private final List<UnsafeBuffer> messages = new ArrayList<>();
        private final ConsensusAdapter adapter;
        private int credit = 1000;
        private int delivered;

        private Link(final Node target)
        {
            adapter = new ConsensusAdapter(mock(Subscription.class), target.agent);
            when(publication.tryClaim(anyInt(), any(BufferClaim.class))).thenAnswer(invocation ->
            {
                if (0 == credit)
                {
                    return Publication.BACK_PRESSURED;
                }
                --credit;
                final int length = invocation.getArgument(0);
                assertTrue(DataHeaderFlyweight.HEADER_LENGTH + length <= 64);
                final UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);
                final BufferClaim claim = invocation.getArgument(1);
                claim.wrap(buffer, 0, DataHeaderFlyweight.HEADER_LENGTH + length);
                messages.add(buffer);
                return messages.size() * 64L;
            });
        }

        private void replaceImage()
        {
            when(image.isClosed()).thenReturn(true);
            image = mock(Image.class);
            delivered = messages.size();
        }

        private int template(final int index)
        {
            return new MessageHeaderDecoder().wrap(messages.get(index), DataHeaderFlyweight.HEADER_LENGTH).templateId();
        }

        private void deliver()
        {
            while (delivered < messages.size())
            {
                final UnsafeBuffer buffer = messages.get(delivered++);
                final MessageHeaderDecoder header =
                    new MessageHeaderDecoder().wrap(buffer, DataHeaderFlyweight.HEADER_LENGTH);
                adapter.onFragment(buffer, DataHeaderFlyweight.HEADER_LENGTH,
                    MessageHeaderEncoder.ENCODED_LENGTH + header.blockLength(), new Header(0, 0, image));
            }
        }
    }

    private static ClusterMember[] setActiveMembers(final ConsensusModuleAgent agent, final int... ids)
    {
        final ClusterMember[] members = new ClusterMember[ids.length];
        for (int i = 0; i < ids.length; i++)
        {
            members[i] = new ClusterMember(ids[i], "", "", "", "", "", "");
        }

        final org.agrona.collections.Int2ObjectHashMap<ClusterMember> map =
            new org.agrona.collections.Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(members, map);
        Tests.setField(agent, "activeMembers", members);
        Tests.setField(agent, "clusterMemberByIdMap", map);

        return members;
    }

    private static void completeElection(
        final ConsensusModuleAgent agent, final long term, final ClusterMember leader, final long nowNs)
    {
        final Election election = mock(Election.class);
        when(election.leadershipTermId()).thenReturn(term);
        when(election.leader()).thenReturn(leader);
        Tests.setField(agent, "election", election);
        agent.electionComplete(nowNs);
    }

}
