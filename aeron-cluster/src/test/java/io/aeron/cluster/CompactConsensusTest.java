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

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.cluster.codecs.AppendPositionDecoder;
import io.aeron.cluster.codecs.CommitPositionDecoder;
import io.aeron.cluster.codecs.CommitPositionEncoder;
import io.aeron.cluster.codecs.CompactCommitPositionDecoder;
import io.aeron.cluster.codecs.CompactCommitPositionEncoder;
import io.aeron.cluster.codecs.CompactLeadershipConfirmAckDecoder;
import io.aeron.cluster.codecs.CompactLeadershipConfirmAckEncoder;
import io.aeron.cluster.codecs.ConsensusConnectionDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.Tests;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.TimeUnit;

import static io.aeron.cluster.CompactConsensusTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CompactConsensusTest
{
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
        assertEquals(20, CommitPositionEncoder.BLOCK_LENGTH);
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
        pair.leader.agent.onCompactLeadershipConfirmAck(42, token + 1, 1, mock(Image.class));
        pair.leader.agent.onCompactLeadershipConfirmAck(42, token + 1, 2, pair.toLeader.image);
        pair.leader.agent.onCompactLeadershipConfirmAck(41, token + 1, 1, pair.toLeader.image);
        pair.leader.agent.onCompactLeadershipConfirmAck(42 + (1L << 32), token + 1, 1, pair.toLeader.image);
        pair.leader.agent.onCompactLeadershipConfirmAck(42, token + 2, 1, pair.toLeader.image);
        assertFalse(pair.leader.agent.isLeadershipConfirmedSince(token));
        when(pair.toLeader.image.isClosed()).thenReturn(true);
        pair.leader.agent.onCompactLeadershipConfirmAck(42, token + 1, 1, pair.toLeader.image);
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
    void shouldKeepReplicationButNotCreditPeersWithoutCompactSupport()
    {
        final Pair pair = new Pair();
        pair.leader.agent.onAppendPosition(42, 0, 1, (short)0, mock(Image.class));
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
        pair.leader.agent.onCompactLeadershipConfirmAck(42, token + 1, 1, pair.toLeader.image);
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
        pair.leader.agent.onAppendPosition(42, 0, 1, (short)0, pair.toLeader.image);
        pair.follower.agent.onAppendPosition(42, 0, 0, (short)0, pair.toFollower.image);
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

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldResendAnOfferedAckLostDuringImageRecovery(final boolean replaceReverseImage)
    {
        final Pair pair = new Pair();
        final long token = pair.leader.agent.triggerQuorumConfirmation();
        pair.broadcast();
        pair.follower.agent.updateFollowerPosition(++pair.follower.nowNs);
        assertEquals(CompactLeadershipConfirmAckDecoder.TEMPLATE_ID,
            pair.toLeader.template(pair.toLeader.messages.size() - 1));

        // No new read request is allowed to heal the original read, even after repeated loss.
        for (int loss = 0; loss < 2; loss++)
        {
            pair.toLeader.replaceImage();
            if (replaceReverseImage)
            {
                pair.toFollower.replaceImage();
            }
            for (int cycle = 0; cycle < 12; cycle++)
            {
                pair.broadcast();
                pair.follower.nowNs += pair.follower.context.leaderHeartbeatIntervalNs();
                pair.follower.agent.updateFollowerPosition(pair.follower.nowNs);
                if (loss == 0 && pair.toLeader.template(pair.toLeader.messages.size() - 1) ==
                    CompactLeadershipConfirmAckDecoder.TEMPLATE_ID)
                {
                    break; // Lose the first retransmission too.
                }
                pair.toLeader.deliver();
            }
        }
        pair.toLeader.deliver();
        assertTrue(pair.leader.agent.isLeadershipConfirmedSince(token));
        assertEquals(token + 1, Tests.<CompactConfirmation>getField(pair.leader.agent, "compactConfirmation").round());
        assertEquals(42L, Tests.<Long>getField(pair.leader.agent, "leadershipTermId"));
        assertEquals(0, pair.leader.recordedPosition);
    }

    @Test
    void shouldRetryUnsentRoundBeforeHeartbeatWithoutNewReadsOrWrites()
    {
        final Pair pair = new Pair();
        final long token = pair.leader.agent.triggerQuorumConfirmation();
        final long nowNs = pair.leader.nowNs + 1;
        final int messages = pair.toFollower.messages.size();
        pair.toFollower.credit = 0;
        assertEquals(1, pair.tickLeader(nowNs));
        assertEquals(messages, pair.toFollower.messages.size());
        pair.toFollower.credit = 1;
        assertEquals(1, pair.tickLeader(nowNs + 1));
        pair.toFollower.deliver();
        pair.echo();
        assertTrue(pair.leader.agent.isLeadershipConfirmedSince(token));
        assertEquals(0, pair.tickLeader(nowNs + 2));
        assertEquals(messages + 1, pair.toFollower.messages.size());
        assertEquals(0, pair.leader.committedPosition);
    }

    @Test
    void shouldRetryAnnouncementAndRoundWithoutSpinningOnUnsupportedPeers()
    {
        final Pair pair = new Pair();
        final long token = pair.leader.agent.triggerQuorumConfirmation();
        pair.leader.members[1].compactConfirmation.requestAnnouncement();
        pair.toFollower.credit = 0;
        final long nowNs = pair.leader.nowNs + 1;
        pair.tickLeader(nowNs);
        pair.toFollower.credit = 1;
        assertEquals(1, pair.tickLeader(nowNs + 1)); // Hello consumes the available frame.
        pair.toFollower.deliver();
        pair.toFollower.credit = 1;
        assertEquals(1, pair.tickLeader(nowNs + 2));
        pair.toFollower.deliver();
        pair.echo();
        assertTrue(pair.leader.agent.isLeadershipConfirmedSince(token));
        assertEquals(0, pair.tickLeader(nowNs + 3));
    }

    @Test
    void shouldRequireFreshQuorumConfirmationAfterNewRequestsAndElections()
    {
        final Pair pair = new Pair();
        final long token = pair.leader.agent.triggerQuorumConfirmation();
        pair.broadcast();
        assertFalse(pair.leader.agent.isLeadershipConfirmedSince(token));
        pair.echo();
        assertTrue(pair.leader.agent.isLeadershipConfirmedSince(token));
        final long next = pair.leader.agent.triggerQuorumConfirmation();
        assertFalse(pair.leader.agent.isLeadershipConfirmedSince(next));
        pair.broadcast();
        pair.echo();
        assertTrue(pair.leader.agent.isLeadershipConfirmedSince(next));
        Tests.setField(pair.leader.agent, "election", mock(Election.class));
        ClusterMember.reset(pair.leader.members);
        assertFalse(pair.leader.agent.isLeadershipConfirmedSince(token));
        completeElection(pair.leader.agent, 43, pair.leader.members[0], pair.leader.nowNs);
        assertFalse(pair.leader.agent.isLeadershipConfirmedSince(token));
        assertFalse(pair.leader.agent.isLeadershipConfirmedSince(pair.leader.agent.triggerQuorumConfirmation()));
    }

    @Test
    void shouldCountRecoveryAnnouncementWorkWithoutAnAckOrAppendToSend()
    {
        final Pair pair = new Pair();
        pair.follower.members[0].compactConfirmation.requestAnnouncement();
        assertEquals(1, pair.follower.agent.updateFollowerPosition(++pair.follower.nowNs));
        assertEquals(ConsensusConnectionDecoder.TEMPLATE_ID,
            pair.toLeader.template(pair.toLeader.messages.size() - 1));
    }
}
