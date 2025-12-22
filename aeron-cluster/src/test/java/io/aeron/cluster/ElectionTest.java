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
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.exceptions.TimeoutException;
import io.aeron.test.cluster.TestClusterClock;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.CountedErrorHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Random;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.ConsensusModuleAgent.APPEND_POSITION_FLAG_NONE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class ElectionTest
{
    private static final long RECORDING_ID = 600L;
    private static final int LOG_SESSION_ID = 777;
    private static final int VERSION = ConsensusModule.Configuration.PROTOCOL_SEMANTIC_VERSION;
    private final Aeron aeron = mock(Aeron.class);
    private final Counter electionStateCounter = mock(Counter.class);
    private final Counter electionCounter = mock(Counter.class);
    private final Counter commitPositionCounter = mock(Counter.class);
    private final Subscription subscription = mock(Subscription.class);
    private final Image logImage = mock(Image.class);
    private final RecordingLog recordingLog = mock(RecordingLog.class);
    private final ClusterMarkFile clusterMarkFile = mock(ClusterMarkFile.class);
    private final NodeStateFile nodeStateFile = mock(NodeStateFile.class);
    private final NodeStateFile.CandidateTerm candidateTerm = mock(NodeStateFile.CandidateTerm.class);
    private final ConsensusPublisher consensusPublisher = mock(ConsensusPublisher.class);
    private final ConsensusModuleAgent consensusModuleAgent = mock(ConsensusModuleAgent.class);
    private final CountedErrorHandler countedErrorHandler = mock(CountedErrorHandler.class);
    private final TestClusterClock clock = new TestClusterClock(NANOSECONDS);
    private final MutableLong markFileCandidateTermId = new MutableLong(-1);

    private final ConsensusModule.Context ctx = new ConsensusModule.Context()
        .aeron(aeron)
        .recordingLog(recordingLog)
        .clusterClock(clock)
        .epochClock(clock.asEpochClock())
        .random(new Random())
        .electionStateCounter(electionStateCounter)
        .electionCounter(electionCounter)
        .commitPositionCounter(commitPositionCounter)
        .clusterMarkFile(clusterMarkFile)
        .nodeStateFile(nodeStateFile)
        .countedErrorHandler(countedErrorHandler);

    @BeforeEach
    void before()
    {
        when(aeron.addCounter(anyInt(), anyString())).thenReturn(electionStateCounter);
        when(aeron.addSubscription(anyString(), anyInt())).thenReturn(subscription);
        when(consensusModuleAgent.logRecordingId()).thenReturn(RECORDING_ID);
        when(consensusModuleAgent.addLogPublication(anyLong())).thenReturn(LOG_SESSION_ID);
        when(subscription.imageBySessionId(anyInt())).thenReturn(logImage);
        when(nodeStateFile.candidateTerm()).thenReturn(candidateTerm);

        when(candidateTerm.candidateTermId()).thenAnswer((invocation) -> markFileCandidateTermId.get());
        when(nodeStateFile.proposeMaxCandidateTermId(anyLong(), anyLong(), anyLong())).thenAnswer(
            (invocation) ->
            {
                final long candidateTermId = invocation.getArgument(0);
                final long existingCandidateTermId = markFileCandidateTermId.get();

                if (candidateTermId > existingCandidateTermId)
                {
                    markFileCandidateTermId.set(candidateTermId);
                    return candidateTermId;
                }

                return existingCandidateTermId;
            });
    }

    @Test
    void shouldElectSingleNodeClusterLeader()
    {
        final long leadershipTermId = NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = ClusterMember.parse(
            "0,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint");

        final ClusterMember thisMember = clusterMembers[0];
        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, thisMember);

        final long newLeadershipTermId = leadershipTermId + 1;
        when(recordingLog.isUnknown(newLeadershipTermId)).thenReturn(Boolean.TRUE);
        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        election.doWork(clock.nanoTime());
        election.doWork(clock.nanoTime());

        verify(consensusModuleAgent).joinLogAsLeader(eq(newLeadershipTermId), eq(logPosition), anyInt(), eq(true));
        verify(electionStateCounter).setRelease(ElectionState.LEADER_READY.code());
        verify(electionCounter).incrementRelease();
        verify(recordingLog).ensureCoherent(
            RECORDING_ID,
            NULL_VALUE,
            logPosition,
            newLeadershipTermId,
            logPosition,
            NULL_VALUE,
            clock.nanoTime(),
            clock.nanoTime(),
            ctx.fileSyncLevel());
    }

    @Test
    @SuppressWarnings("MethodLength")
    void shouldElectAppointedLeader()
    {
        final long leadershipTermId = NULL_VALUE;
        final long logPosition = 0;
        final int appVersion = -98;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[0];
        when(consensusModuleAgent.logRecordingId()).thenReturn(RECORDING_ID);

        ctx.appointedLeaderId(candidateMember.id()).appVersion(appVersion);

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember);

        final long candidateTermId = leadershipTermId + 1;
        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 1, VERSION);
        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 2, VERSION);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.NOMINATE.code());

        clock.increment(ctx.electionTimeoutNs() >> 1);
        election.doWork(clock.nanoTime());
        election.doWork(clock.nanoTime());
        verify(consensusPublisher).requestVote(
            clusterMembers[1].publication(),
            leadershipTermId,
            logPosition,
            candidateTermId,
            candidateMember.id());
        verify(consensusPublisher).requestVote(
            clusterMembers[2].publication(),
            leadershipTermId,
            logPosition,
            candidateTermId,
            candidateMember.id());
        verify(electionStateCounter).setRelease(ElectionState.CANDIDATE_BALLOT.code());
        verify(consensusModuleAgent).role(Cluster.Role.CANDIDATE);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[1].id(), true);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[2].id(), true);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.LEADER_LOG_REPLICATION.code());

        election.doWork(clock.nanoTime());

        verify(consensusPublisher).newLeadershipTerm(
            clusterMembers[1].publication(),
            leadershipTermId,
            0,
            0,
            NULL_POSITION,
            candidateTermId,
            logPosition,
            logPosition,
            RECORDING_ID,
            clock.nanoTime(),
            candidateMember.id(),
            LOG_SESSION_ID,
            appVersion,
            election.isLeaderStartup());

        verify(consensusPublisher).newLeadershipTerm(
            clusterMembers[2].publication(),
            leadershipTermId,
            0,
            0,
            NULL_POSITION,
            candidateTermId,
            logPosition,
            logPosition,
            RECORDING_ID,
            clock.nanoTime(),
            candidateMember.id(),
            LOG_SESSION_ID,
            appVersion,
            election.isLeaderStartup());

        when(recordingLog.isUnknown(candidateTermId)).thenReturn(Boolean.TRUE);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.LEADER_REPLAY.code());

        election.doWork(clock.nanoTime());

        verify(consensusModuleAgent).joinLogAsLeader(eq(candidateTermId), eq(logPosition), anyInt(), eq(true));
        verify(electionStateCounter).setRelease(ElectionState.LEADER_READY.code());
        verify(recordingLog).ensureCoherent(
            RECORDING_ID,
            NULL_VALUE,
            logPosition,
            candidateTermId,
            logPosition,
            NULL_VALUE,
            clock.nanoTime(),
            clock.nanoTime(),
            ctx.fileSyncLevel());

        assertEquals(NULL_POSITION, clusterMembers[1].logPosition());
        assertEquals(NULL_POSITION, clusterMembers[2].logPosition());
        assertEquals(candidateTermId, election.leadershipTermId());

        clock.increment(1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.LEADER_READY.code());

        when(consensusModuleAgent.appendNewLeadershipTermEvent(anyLong())).thenReturn(true);

        clock.increment(1);
        election.onAppendPosition(candidateTermId, logPosition, clusterMembers[1].id(), APPEND_POSITION_FLAG_NONE);
        election.onAppendPosition(candidateTermId, logPosition, clusterMembers[2].id(), APPEND_POSITION_FLAG_NONE);
        election.doWork(clock.nanoTime());
        final InOrder inOrder = inOrder(consensusModuleAgent, electionStateCounter);
        inOrder.verify(consensusModuleAgent).electionComplete(anyLong());
        inOrder.verify(electionStateCounter).setRelease(ElectionState.CLOSED.code());
    }

    @Test
    void shouldVoteForAppointedLeader()
    {
        final long leadershipTermId = NULL_VALUE;
        final long logPosition = 0;
        final int candidateId = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];
        final long leaderRecordingId = 983724;

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        long nowNs = 0;
        election.doWork(++nowNs);
        verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        final long candidateTermId = leadershipTermId + 1;
        election.onRequestVote(leadershipTermId, logPosition, candidateTermId, candidateId, VERSION);
        verify(consensusPublisher).placeVote(
            clusterMembers[candidateId].publication(),
            candidateTermId,
            leadershipTermId,
            logPosition,
            candidateId,
            followerMember.id(),
            true);
        election.doWork(++nowNs);
        clock.increment(1);
        verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_BALLOT.code());

        final int logSessionId = -7;
        election.onNewLeadershipTerm(
            leadershipTermId,
            NULL_VALUE,
            NULL_POSITION,
            NULL_POSITION,
            candidateTermId,
            logPosition,
            logPosition,
            leaderRecordingId,
            clock.nanoTime(),
            candidateId,
            logSessionId, false);

        verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_REPLAY.code());

        election.doWork(++nowNs);
        verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_LOG_INIT.code());

        election.doWork(++nowNs);
        verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_LOG_AWAIT.code());

        when(consensusModuleAgent.tryJoinLogAsFollower(any(), anyBoolean(), anyLong())).thenReturn(true);
        election.doWork(++nowNs);
        verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_READY.code());

        when(consensusPublisher.appendPosition(
            any(), anyLong(), anyLong(), anyInt(), anyShort())).thenReturn(Boolean.TRUE);
        when(consensusModuleAgent.appendNewLeadershipTermEvent(anyLong())).thenReturn(true);

        election.doWork(++nowNs);
        final InOrder inOrder = inOrder(consensusPublisher, consensusModuleAgent, electionStateCounter);
        inOrder.verify(consensusPublisher).appendPosition(
            clusterMembers[candidateId].publication(),
            candidateTermId,
            logPosition,
            followerMember.id(),
            APPEND_POSITION_FLAG_NONE);
        inOrder.verify(consensusModuleAgent).electionComplete(anyLong());
        inOrder.verify(electionStateCounter).setRelease(ElectionState.CLOSED.code());
    }

    @Test
    void shouldCanvassMembersInSuccessfulLeadershipBid()
    {
        final long logPosition = 0;
        final long leadershipTermId = NULL_VALUE;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        election.doWork(0);

        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());
        verify(consensusPublisher).canvassPosition(
            clusterMembers[0].publication(), leadershipTermId, logPosition, leadershipTermId, followerMember.id());
        verify(consensusPublisher).canvassPosition(
            clusterMembers[2].publication(), leadershipTermId, logPosition, leadershipTermId, followerMember.id());

        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 0, VERSION);
        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 2, VERSION);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.NOMINATE.code());
    }

    @Test
    void shouldVoteForCandidateDuringNomination()
    {
        final long logPosition = 0;
        final long leadershipTermId = NULL_VALUE;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        clock.increment(1);
        election.doWork(clock.nanoTime());

        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 0, VERSION);
        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 2, VERSION);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.NOMINATE.code());

        clock.increment(1);
        final long candidateTermId = leadershipTermId + 1;
        election.onRequestVote(leadershipTermId, logPosition, candidateTermId, 0, VERSION);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_BALLOT.code());
    }

    @Test
    void shouldTimeoutCanvassWithMajority()
    {
        final long leadershipTermId = NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        election.onAppendPosition(leadershipTermId, logPosition, 0, APPEND_POSITION_FLAG_NONE);

        clock.increment(1);
        election.doWork(clock.nanoTime());

        clock.increment(ctx.startupCanvassTimeoutNs());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.NOMINATE.code());
    }

    @Test
    void shouldWinCandidateBallotWithMajority()
    {
        final long leadershipTermId = NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(false, leadershipTermId, logPosition, clusterMembers, candidateMember);

        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 0, VERSION);
        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 2, VERSION);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.NOMINATE.code());

        clock.increment(ctx.electionTimeoutNs() >> 1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.CANDIDATE_BALLOT.code());

        clock.increment(ctx.electionTimeoutNs());
        final long candidateTermId = leadershipTermId + 1;
        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[2].id(), true);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.LEADER_LOG_REPLICATION.code());
    }

    @ParameterizedTest
    @CsvSource(value = {"true,true", "true,false", "false,false", "false,true"})
    void shouldBaseStartupValueOnLeader(final boolean isLeaderStart, final boolean isNodeStart)
    {
        final long leadershipTermId = 0;
        final long logPosition = 0;
        final long leaderRecordingId = 367234;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];
        when(consensusModuleAgent.tryJoinLogAsFollower(any(), anyBoolean(), anyLong())).thenReturn(true);

        final Election election = newElection(
            isNodeStart, leadershipTermId, logPosition, clusterMembers, followerMember);

        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        clock.increment(1);
        final int leaderMemberId = clusterMembers[0].id();
        election.onNewLeadershipTerm(
            leadershipTermId,
            NULL_VALUE,
            NULL_POSITION,
            NULL_POSITION,
            leadershipTermId,
            logPosition,
            logPosition,
            leaderRecordingId,
            clock.nanoTime(),
            leaderMemberId,
            0,
            isLeaderStart);
        election.doWork(clock.nanoTime());

        election.doWork(clock.increment(1));

        final long timeAtFollowerLogAwaitNs = clock.increment(1);
        election.doWork(timeAtFollowerLogAwaitNs);

        election.doWork(clock.increment(1));

        verify(consensusModuleAgent).tryJoinLogAsFollower(logImage, isLeaderStart, timeAtFollowerLogAwaitNs);
    }

    @Test
    void shouldElectCandidateWithFullVote()
    {
        final long leadershipTermId = NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember);

        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 0, VERSION);
        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 2, VERSION);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.NOMINATE.code());

        clock.increment(ctx.electionTimeoutNs() >> 1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.CANDIDATE_BALLOT.code());

        clock.increment(1);
        final long candidateTermId = leadershipTermId + 1;
        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[0].id(), true);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[2].id(), true);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.LEADER_LOG_REPLICATION.code());
    }

    @Test
    void shouldTimeoutCandidateBallotWithoutMajority()
    {
        final long leadershipTermId = NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember);

        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        final InOrder inOrder = Mockito.inOrder(electionStateCounter);
        inOrder.verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 0, VERSION);
        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 2, VERSION);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setRelease(ElectionState.NOMINATE.code());

        clock.increment(ctx.electionTimeoutNs() >> 1);
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setRelease(ElectionState.CANDIDATE_BALLOT.code());

        clock.increment(ctx.electionTimeoutNs());
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());
        assertEquals(leadershipTermId, election.leadershipTermId());
    }

    @Test
    void shouldTimeoutFailedCandidateBallotOnSplitVoteThenSucceedOnRetry()
    {
        final long leadershipTermId = NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        final InOrder inOrder = Mockito.inOrder(electionStateCounter);
        inOrder.verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 0, VERSION);

        clock.increment(ctx.startupCanvassTimeoutNs());
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setRelease(ElectionState.NOMINATE.code());

        clock.increment(ctx.electionTimeoutNs() >> 1);
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setRelease(ElectionState.CANDIDATE_BALLOT.code());

        clock.increment(1);
        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(
            leadershipTermId + 1, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[2].id(), false);
        election.doWork(clock.nanoTime());

        clock.increment(ctx.electionTimeoutNs());
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 0, VERSION);

        clock.increment(ctx.leaderHeartbeatTimeoutNs());
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setRelease(ElectionState.NOMINATE.code());

        clock.increment(ctx.electionTimeoutNs());
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setRelease(ElectionState.CANDIDATE_BALLOT.code());

        final long candidateTermId = leadershipTermId + 2;
        election.onVote(
            candidateTermId, leadershipTermId + 1, logPosition, candidateMember.id(), clusterMembers[2].id(), true);

        clock.increment(ctx.electionTimeoutNs());
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setRelease(ElectionState.LEADER_LOG_REPLICATION.code());
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setRelease(ElectionState.LEADER_REPLAY.code());

        clock.increment(1);
        election.doWork(clock.nanoTime());
        election.doWork(clock.nanoTime());
        assertEquals(candidateTermId, election.leadershipTermId());
        inOrder.verify(electionStateCounter).setRelease(ElectionState.LEADER_READY.code());
    }

    @Test
    void shouldTimeoutFollowerBallotWithoutLeaderEmerging()
    {
        final long leadershipTermId = NULL_VALUE;
        final long logPosition = 0L;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        final InOrder inOrder = Mockito.inOrder(electionStateCounter);
        inOrder.verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        final long candidateTermId = leadershipTermId + 1;
        election.onRequestVote(leadershipTermId, logPosition, candidateTermId, 0, VERSION);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_BALLOT.code());

        clock.increment(ctx.electionTimeoutNs());
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());
        assertEquals(leadershipTermId, election.leadershipTermId());
    }

    @Test
    void shouldBecomeFollowerIfEnteringNewElection()
    {
        final long leadershipTermId = 1;
        final long logPosition = 120;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[0];

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.LEADER);
        final Election election = newElection(false, leadershipTermId, logPosition, clusterMembers, thisMember);

        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());
        verify(consensusModuleAgent).prepareForNewLeadership(logPosition, clock.nanoTime());
        verify(consensusModuleAgent, atLeastOnce()).role(Cluster.Role.FOLLOWER);
    }

    @Test
    @SuppressWarnings("MethodLength")
    void followerShouldReplicateLogBeforeReplayDuringElection()
    {
        final long term0Id = 0;
        final long term1Id = 1;
        final long term1BaseLogPosition = 60;

        final long term2Id = 2;
        final long term2BaseLogPosition = 120;

        final long localRecordingId = 2390485;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[1];
        final ClusterMember liveLeader = clusterMembers[0];
        final int leaderId = liveLeader.id();
        final RecordingReplication logReplication = mock(RecordingReplication.class);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.FOLLOWER);
        when(consensusModuleAgent.prepareForNewLeadership(anyLong(), anyLong())).thenReturn(term1BaseLogPosition);

        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        final Election followerElection = new Election(
            true,
            NULL_VALUE,
            term0Id,
            term1BaseLogPosition,
            term1BaseLogPosition,
            term1BaseLogPosition,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);

        long t1 = System.nanoTime();
        followerElection.doWork(++t1);
        verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());
        reset(consensusPublisher);

        followerElection.onRequestVote(term1Id, term2BaseLogPosition, term2Id, leaderId, VERSION);
        verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_BALLOT.code());

        followerElection.onNewLeadershipTerm(
            term1Id,
            term2Id,
            term2BaseLogPosition,
            term2BaseLogPosition,
            term2Id,
            term2BaseLogPosition,
            term2BaseLogPosition,
            RECORDING_ID,
            t1,
            leaderId,
            0,
            true);

        verify(electionStateCounter, times(2)).setRelease(ElectionState.CANVASS.code());

        followerElection.doWork(++t1);
        verify(consensusPublisher).canvassPosition(
            liveLeader.publication(),
            term0Id,
            term1BaseLogPosition,
            term0Id,
            thisMember.id());

        followerElection.onNewLeadershipTerm(
            term0Id,
            term1Id,
            term1BaseLogPosition,
            term2BaseLogPosition,
            term2Id,
            term2BaseLogPosition,
            term2BaseLogPosition,
            RECORDING_ID,
            t1,
            leaderId,
            0,
            true);

        when(consensusModuleAgent.newLogReplication(any(), any(), anyLong(), anyLong(), anyLong()))
            .thenReturn(logReplication);
        followerElection.doWork(++t1);

        verify(consensusModuleAgent, times(1)).newLogReplication(
            liveLeader.archiveEndpoint(), liveLeader.archiveResponseEndpoint(), RECORDING_ID, term2BaseLogPosition, t1);

        when(logReplication.hasReplicationEnded()).thenReturn(false);
        followerElection.doWork(++t1);
        followerElection.doWork(++t1);
        followerElection.doWork(++t1);
        followerElection.doWork(++t1);

        verify(consensusModuleAgent, times(4)).pollArchiveEvents();

        when(logReplication.hasReplicationEnded()).thenReturn(true);
        when(logReplication.hasStopped()).thenReturn(true);
        when(logReplication.position()).thenReturn(term2BaseLogPosition);
        when(logReplication.recordingId()).thenReturn(localRecordingId);
        when(consensusPublisher.appendPosition(
            liveLeader.publication(), term2Id, term2BaseLogPosition, thisMember.id(), APPEND_POSITION_FLAG_NONE))
            .thenReturn(true);
        t1 += ctx.leaderHeartbeatIntervalNs();

        verify(electionStateCounter, times(2)).setRelease(ElectionState.CANVASS.code());
        followerElection.doWork(clock.nanoTime());

        verify(consensusPublisher).appendPosition(
            liveLeader.publication(), term2Id, term2BaseLogPosition, thisMember.id(), APPEND_POSITION_FLAG_NONE);
        verify(electionStateCounter, times(2)).setRelease(ElectionState.CANVASS.code());

        followerElection.onCommitPosition(term2Id, term2BaseLogPosition, leaderId);
        followerElection.doWork(++t1);
        verify(electionStateCounter, times(3)).setRelease(ElectionState.CANVASS.code());
    }

    @Test
    void followerShouldTimeoutLeaderIfReplicateLogPositionIsNotCommittedByLeader()
    {
        final long term1Id = 1;
        final long term2Id = 2;
        final long term1BaseLogPosition = 60;
        final long term2BaseLogPosition = 120;
        final long localRecordingId = 2390485;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[1];
        final ClusterMember liveLeader = clusterMembers[0];
        final int leaderId = liveLeader.id();
        final RecordingReplication logReplication = mock(RecordingReplication.class);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.FOLLOWER);
        when(consensusModuleAgent.prepareForNewLeadership(anyLong(), anyLong())).thenReturn(term1BaseLogPosition);

        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        final Election election = new Election(
            true,
            NULL_VALUE,
            term1Id,
            term1BaseLogPosition,
            term1BaseLogPosition,
            term1BaseLogPosition,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);

        election.doWork(clock.increment(1));
        verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        election.onRequestVote(term1Id, term2BaseLogPosition, term2Id, leaderId, VERSION);
        verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_BALLOT.code());

        election.onNewLeadershipTerm(
            term1Id,
            term2Id,
            term2BaseLogPosition,
            term2BaseLogPosition,
            term2Id,
            term2BaseLogPosition,
            term2BaseLogPosition,
            RECORDING_ID,
            clock.nanoTime(),
            leaderId,
            0,
            true);

        verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_LOG_REPLICATION.code());

        when(consensusModuleAgent.newLogReplication(any(), any(), anyLong(), anyLong(), anyLong()))
            .thenReturn(logReplication);
        election.doWork(clock.increment(1));

        verify(consensusModuleAgent, times(1)).newLogReplication(
            liveLeader.archiveEndpoint(),
            liveLeader.archiveResponseEndpoint(),
            RECORDING_ID,
            term2BaseLogPosition,
            clock.nanoTime());

        when(logReplication.hasReplicationEnded()).thenReturn(true);
        when(logReplication.hasStopped()).thenReturn(true);
        when(logReplication.position()).thenReturn(term2BaseLogPosition);
        when(logReplication.recordingId()).thenReturn(localRecordingId);
        when(consensusPublisher.appendPosition(
            liveLeader.publication(), term1Id, term2BaseLogPosition, thisMember.id(), APPEND_POSITION_FLAG_NONE))
            .thenReturn(true);
        clock.increment(ctx.leaderHeartbeatIntervalNs());
        election.doWork(clock.nanoTime());

        verify(consensusModuleAgent, atLeastOnce()).pollArchiveEvents();
        verify(consensusPublisher).appendPosition(
            liveLeader.publication(), term2Id, term2BaseLogPosition, thisMember.id(), APPEND_POSITION_FLAG_NONE);
        verify(electionStateCounter, never()).setRelease(ElectionState.FOLLOWER_REPLAY.code());
        reset(countedErrorHandler);

        clock.increment(ctx.leaderHeartbeatTimeoutNs());
        assertThrows(TimeoutException.class, () -> election.doWork(clock.nanoTime()));
    }

    @Test
    void followerShouldProgressThroughFailedElectionsTermsImmediatelyPriorToCurrent()
    {
        final long term1Id = 1;
        final long term10Id = 10;
        final long term1BaseLogPosition = 60;
        final long term10BaseLogPosition = 120;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[1];
        final ClusterMember liveLeader = clusterMembers[0];
        final int leaderId = liveLeader.id();
        final RecordingReplication logReplication = mock(RecordingReplication.class);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.FOLLOWER);
        when(consensusModuleAgent.prepareForNewLeadership(anyLong(), anyLong())).thenReturn(term1BaseLogPosition);

        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        final Election election = new Election(
            true,
            NULL_VALUE,
            term1Id,
            term1BaseLogPosition,
            term1BaseLogPosition,
            term1BaseLogPosition,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);

        long t1 = 0;
        election.doWork(++t1);
        verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        election.onRequestVote(term1Id, term10BaseLogPosition, term10Id, leaderId, VERSION);
        verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_BALLOT.code());

        election.onNewLeadershipTerm(
            term1Id,
            term10Id,
            term10BaseLogPosition,
            term10BaseLogPosition,
            term10Id,
            term10BaseLogPosition,
            term10BaseLogPosition,
            RECORDING_ID,
            t1,
            leaderId,
            0,
            true);

        verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_LOG_REPLICATION.code());

        when(consensusModuleAgent.newLogReplication(any(), any(), anyLong(), anyLong(), anyLong()))
            .thenReturn(logReplication);
        election.doWork(++t1);

        verify(consensusModuleAgent, times(1)).newLogReplication(
            liveLeader.archiveEndpoint(),
            liveLeader.archiveResponseEndpoint(),
            RECORDING_ID,
            term10BaseLogPosition,
            t1);
    }

    @Test
    @SuppressWarnings("MethodLength")
    void followerShouldProgressThroughInterimElectionsTerms()
    {
        final long term9Id = 9;
        final long term10Id = 10;
        final long term9BaseLogPosition = 60;
        final long term10BaseLogPosition = 120;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[1];
        final ClusterMember liveLeader = clusterMembers[0];
        final int leaderId = liveLeader.id();
        final RecordingReplication logReplication = mock(RecordingReplication.class);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.FOLLOWER);
        when(consensusModuleAgent.prepareForNewLeadership(anyLong(), anyLong())).thenReturn(0L);

        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        final Election election = new Election(
            true,
            NULL_VALUE,
            0,
            0,
            0,
            0,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);

        long t1 = System.nanoTime();
        election.doWork(++t1);
        verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        election.onRequestVote(term9Id, term10BaseLogPosition, term10Id, leaderId, VERSION);
        verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_BALLOT.code());
        reset(consensusPublisher);
        reset(electionStateCounter);

        election.onNewLeadershipTerm(
            term9Id,
            term10Id,
            term10BaseLogPosition,
            term10BaseLogPosition,
            term10Id,
            term10BaseLogPosition,
            term10BaseLogPosition,
            RECORDING_ID,
            t1,
            leaderId,
            0,
            true);

        verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        election.doWork(++t1);
        verify(consensusPublisher).canvassPosition(liveLeader.publication(), 0L, 0L, 0L, thisMember.id());

        for (int i = 0; i < term9Id - 1; i++)
        {
            reset(consensusPublisher);
            reset(electionStateCounter);

            election.onNewLeadershipTerm(
                i,
                i + 1,
                0,
                0,
                term10Id,
                term10BaseLogPosition,
                term10BaseLogPosition,
                RECORDING_ID,
                t1,
                leaderId,
                0,
                true);

            election.doWork(++t1);
            verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_LOG_REPLICATION.code());

            election.doWork(++t1);
            verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

            election.doWork(++t1);
            verify(consensusPublisher).canvassPosition(liveLeader.publication(), i + 1, 0L, term10Id, thisMember.id());
        }
        reset(consensusPublisher);
        reset(electionStateCounter);

        election.onNewLeadershipTerm(
            term9Id - 1,
            term9Id,
            term9BaseLogPosition,
            term10BaseLogPosition,
            term10Id,
            term10BaseLogPosition,
            term10BaseLogPosition,
            RECORDING_ID,
            t1,
            leaderId,
            0,
            true);

        election.doWork(++t1);
        verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_LOG_REPLICATION.code());

        when(consensusModuleAgent.newLogReplication(any(), any(), anyLong(), anyLong(), anyLong()))
            .thenReturn(logReplication);
        election.doWork(++t1);

        verify(consensusModuleAgent).newLogReplication(
            liveLeader.archiveEndpoint(), liveLeader.archiveResponseEndpoint(), RECORDING_ID, term9BaseLogPosition, t1);
    }

    @Test
    void followerShouldReplayAndCatchupWhenLateJoiningClusterInSameTerm()
    {
        final long leadershipTermId = 1;
        final long leaderLogPosition = 120;
        final long followerLogPosition = 60;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[1];
        final ClusterMember liveLeader = clusterMembers[0];
        final int leaderId = liveLeader.id();
        final LogReplay logReplay = mock(LogReplay.class);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.FOLLOWER);
        when(consensusModuleAgent.prepareForNewLeadership(anyLong(), anyLong())).thenReturn(followerLogPosition);

        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        final Election election = new Election(
            true,
            NULL_VALUE,
            1,
            followerLogPosition,
            followerLogPosition,
            followerLogPosition,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);

        long t1 = 0;
        election.doWork(++t1);
        verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        election.onNewLeadershipTerm(
            leadershipTermId,
            NULL_VALUE,
            NULL_POSITION,
            NULL_POSITION,
            leadershipTermId,
            followerLogPosition,
            leaderLogPosition,
            RECORDING_ID,
            t1,
            leaderId,
            LOG_SESSION_ID,
            true);
        verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_REPLAY.code());

        when(consensusModuleAgent.newLogReplay(anyLong(), anyLong())).thenReturn(logReplay);
        when(logReplay.isDone()).thenReturn(true);
        election.doWork(++t1);
        election.doWork(++t1);

        verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_CATCHUP_INIT.code());
    }

    @Test
    void followerShouldReplicateReplayAndCatchupWhenLateJoiningClusterInLaterTerm()
    {
        final long leadershipTermId = 1;
        final long leaderLogPosition = 120;
        final long termBaseLogPosition = 60;
        final long localRecordingId = 2390485;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[1];
        final ClusterMember liveLeader = clusterMembers[0];
        final int leaderId = liveLeader.id();
        final RecordingReplication logReplication = mock(RecordingReplication.class);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.FOLLOWER);

        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        final Election election = new Election(
            true,
            NULL_VALUE,
            0,
            0,
            0,
            0,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);

        long t1 = 0;
        election.doWork(++t1);
        verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        election.onNewLeadershipTerm(
            0,
            1,
            termBaseLogPosition,
            leaderLogPosition,
            leadershipTermId,
            termBaseLogPosition,
            leaderLogPosition,
            RECORDING_ID,
            t1,
            leaderId,
            0,
            false);
        verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_LOG_REPLICATION.code());

        when(consensusModuleAgent.newLogReplication(any(), any(), anyLong(), anyLong(), anyLong()))
            .thenReturn(logReplication);
        election.doWork(++t1);

        verify(consensusModuleAgent, times(1)).newLogReplication(
            liveLeader.archiveEndpoint(), liveLeader.archiveResponseEndpoint(), RECORDING_ID, termBaseLogPosition, t1);

        when(logReplication.hasReplicationEnded()).thenReturn(true);
        when(logReplication.hasStopped()).thenReturn(true);
        when(logReplication.position()).thenReturn(termBaseLogPosition);
        when(logReplication.recordingId()).thenReturn(localRecordingId);
        when(consensusPublisher.appendPosition(any(), anyLong(), anyLong(), anyInt(), anyShort())).thenReturn(true);
        t1 += ctx.leaderHeartbeatIntervalNs();
        election.doWork(++t1);

        verify(consensusPublisher).appendPosition(
            liveLeader.publication(),
            leadershipTermId,
            termBaseLogPosition,
            thisMember.id(),
            APPEND_POSITION_FLAG_NONE);

        election.onCommitPosition(leadershipTermId, leaderLogPosition, leaderId);
        election.doWork(++t1);
        verify(electionStateCounter, times(2)).setRelease(ElectionState.CANVASS.code());
    }

    @Test
    void followerShouldUseInitialLeadershipTermIdAndInitialTermBaseLogPositionWhenRecordingLogIsEmpty()
    {
        final long initialLeadershipTermId = 2;
        final long initialTermBaseLogPosition = 120;
        final long snapshotLogPosition = 150;
        final long leaderLeadershipTermId = 3;
        final long leaderTermBaseLogPosition = 500;
        final long leaderLogPosition = 600;
        final long localRecordingId = 2390485;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[1];
        final ClusterMember liveLeader = clusterMembers[0];
        final int leaderId = liveLeader.id();
        final RecordingReplication logReplication = mock(RecordingReplication.class);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.FOLLOWER);

        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        final Election election = new Election(
            false,
            NULL_VALUE,
            initialLeadershipTermId,
            initialTermBaseLogPosition,
            snapshotLogPosition,
            snapshotLogPosition,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);

        long t1 = 0;
        election.doWork(++t1);
        verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        when(logReplication.hasReplicationEnded()).thenReturn(true);
        when(logReplication.hasStopped()).thenReturn(true);
        when(logReplication.recordingId()).thenReturn(localRecordingId);
        when(consensusPublisher.appendPosition(any(), anyLong(), anyLong(), anyInt(), anyShort())).thenReturn(true);

        election.onNewLeadershipTerm(
            initialLeadershipTermId,
            leaderLeadershipTermId,
            leaderTermBaseLogPosition,
            leaderLogPosition,
            leaderLeadershipTermId,
            leaderTermBaseLogPosition,
            leaderLogPosition,
            RECORDING_ID,
            t1,
            leaderId,
            0,
            false);

        doReturn(logReplication).when(consensusModuleAgent)
            .newLogReplication(any(), any(), anyLong(), anyLong(), anyLong());
        doReturn(leaderTermBaseLogPosition).when(logReplication).position();

        election.doWork(++t1);
        election.onCommitPosition(leaderLeadershipTermId, leaderTermBaseLogPosition, leaderId);
        election.doWork(++t1);

        verify(recordingLog).ensureCoherent(
            RECORDING_ID,
            initialLeadershipTermId,
            initialTermBaseLogPosition,
            initialLeadershipTermId,
            initialTermBaseLogPosition,
            leaderTermBaseLogPosition,
            t1,
            t1,
            ctx.fileSyncLevel());
    }

    @Test
    void followerShouldReplicateAndSendAppendPositionWhenLogReplicationDone()
    {
        final long leadershipTermId = 1;
        final long leaderLogPosition = 120;
        final long termBaseLogPosition = 60;
        final long nextTermBaseLogPosition = 60;
        final long localRecordingId = 2390485;
        final MutableLong replicationPosition = new MutableLong(0);
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[1];
        final ClusterMember liveLeader = clusterMembers[0];
        final int leaderId = liveLeader.id();
        final RecordingReplication logReplication = mock(RecordingReplication.class);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.FOLLOWER);

        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        final Election election = new Election(
            true,
            NULL_VALUE,
            0,
            0,
            0,
            0,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);

        long t1 = 0;
        election.doWork(++t1);
        verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        election.onNewLeadershipTerm(
            0,
            1,
            nextTermBaseLogPosition,
            NULL_POSITION,
            leadershipTermId,
            termBaseLogPosition,
            leaderLogPosition,
            RECORDING_ID,
            t1,
            leaderId,
            0,
            false);
        verify(electionStateCounter).setRelease(ElectionState.FOLLOWER_LOG_REPLICATION.code());

        when(consensusModuleAgent.newLogReplication(any(), any(), anyLong(), anyLong(), anyLong()))
            .thenReturn(logReplication);
        election.doWork(++t1);

        verify(consensusModuleAgent, times(1)).newLogReplication(
            liveLeader.archiveEndpoint(), liveLeader.archiveResponseEndpoint(), RECORDING_ID, termBaseLogPosition, t1);

        when(logReplication.recordingId()).thenReturn(localRecordingId);

        when(logReplication.position()).then((invocation) -> replicationPosition.get());
        // When replication is done we move the replication position.
        when(logReplication.poll(anyLong())).thenAnswer(
            (invocation) ->
            {
                replicationPosition.set(nextTermBaseLogPosition);
                return 1;
            });
        when(logReplication.hasReplicationEnded()).thenReturn(true);
        when(logReplication.hasStopped()).thenReturn(true);

        when(consensusPublisher.appendPosition(any(), anyLong(), anyLong(), anyInt(), anyShort()))
            .thenReturn(true);

        t1 += ctx.leaderHeartbeatIntervalNs();
        election.doWork(++t1);

        verify(consensusPublisher).appendPosition(
            liveLeader.publication(),
            leadershipTermId,
            nextTermBaseLogPosition, // Expect to be the position after the replication has completed.
            thisMember.id(),
            APPEND_POSITION_FLAG_NONE);

        election.onCommitPosition(leadershipTermId, leaderLogPosition, leaderId);
        election.doWork(++t1);
        verify(electionStateCounter, times(2)).setRelease(ElectionState.CANVASS.code());
    }

    @Test
    void leaderShouldMoveToLogReplicationThenWaitForCommitPosition()
    {
        final long leadershipTermId = 1;
        final long candidateTermId = leadershipTermId + 1;
        final long leaderLogPosition = 120;
        final long followerLogPosition = 60;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[0];
        final ClusterMember liveFollower = clusterMembers[1];
        final int leaderId = thisMember.id();
        final int followerId = liveFollower.id();
        final int appVersion = 888;
        ctx.appVersion(appVersion);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.LEADER);
        when(consensusModuleAgent.logRecordingId()).thenReturn(RECORDING_ID);

        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        final Election election = new Election(
            true,
            NULL_VALUE,
            leadershipTermId,
            leaderLogPosition,
            leaderLogPosition,
            leaderLogPosition,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);

        election.doWork(0);

        final InOrder inOrder = inOrder(electionStateCounter);
        inOrder.verify(electionStateCounter).setRelease(ElectionState.INIT.code());
        inOrder.verify(electionStateCounter).setRelease(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, leaderLogPosition, leadershipTermId, leaderId, VERSION);
        election.onCanvassPosition(leadershipTermId, followerLogPosition, leadershipTermId, followerId, VERSION);

        clock.increment(2 * ctx.startupCanvassTimeoutNs());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.NOMINATE.code());

        clock.increment(2 * ctx.electionTimeoutNs());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.CANDIDATE_BALLOT.code());

        election.onVote(candidateTermId, leadershipTermId, 120, leaderId, leaderId, true);
        election.onVote(candidateTermId, leadershipTermId, 120, leaderId, followerId, true);

        clock.increment(2 * ctx.electionTimeoutNs());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setRelease(ElectionState.LEADER_LOG_REPLICATION.code());

        // Until the quorum position moves to the leader's append position
        // we stay in the same state and emit new leadership terms.
        when(consensusModuleAgent.quorumPosition()).thenReturn(followerLogPosition);
        election.doWork(clock.increment(1));
        verifyNoMoreInteractions(electionStateCounter);
        verify(consensusPublisher).newLeadershipTerm(
            clusterMembers[1].publication(),
            leadershipTermId,
            candidateTermId,
            leaderLogPosition,
            NULL_POSITION,
            candidateTermId,
            leaderLogPosition,
            leaderLogPosition,
            RECORDING_ID,
            clock.nanoTime(),
            leaderId,
            LOG_SESSION_ID,
            appVersion,
            election.isLeaderStartup());

        verify(consensusPublisher).newLeadershipTerm(
            clusterMembers[2].publication(),
            leadershipTermId,
            candidateTermId,
            leaderLogPosition,
            NULL_POSITION,
            candidateTermId,
            leaderLogPosition,
            leaderLogPosition,
            RECORDING_ID,
            clock.nanoTime(),
            leaderId,
            LOG_SESSION_ID,
            appVersion,
            election.isLeaderStartup());

        // Begin replay once a quorum of followers has caught up.
        when(consensusModuleAgent.quorumPosition()).thenReturn(leaderLogPosition);
        election.doWork(clock.increment(1));
        verify(electionStateCounter).setRelease(ElectionState.LEADER_REPLAY.code());
    }

    @Test
    @Disabled
    void shouldThrowNonZeroLogPositionAndNullRecordingIdSpecified()
    {
        Election.ensureRecordingLogCoherent(ctx, NULL_POSITION, 0, 0, 0, 0, 0, 1);
        Election.ensureRecordingLogCoherent(ctx, NULL_POSITION, 0, 0, 0, 0, 1000, 1);

        verifyNoInteractions(recordingLog);
    }

    private Election newElection(
        final boolean isStartup,
        final long logLeadershipTermId,
        final long logPosition,
        final ClusterMember[] clusterMembers,
        final ClusterMember thisMember)
    {
        final Int2ObjectHashMap<ClusterMember> idToClusterMemberMap = new Int2ObjectHashMap<>();

        ClusterMember.addClusterMemberIds(clusterMembers, idToClusterMemberMap);

        return new Election(
            isStartup,
            NULL_VALUE,
            logLeadershipTermId,
            logPosition,
            logPosition,
            logPosition,
            clusterMembers,
            idToClusterMemberMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);
    }

    private Election newElection(
        final long logLeadershipTermId,
        final long logPosition,
        final ClusterMember[] clusterMembers,
        final ClusterMember thisMember)
    {
        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();

        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        return new Election(
            true,
            NULL_VALUE,
            logLeadershipTermId,
            logPosition,
            logPosition,
            logPosition,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);
    }

    private static ClusterMember[] prepareClusterMembers()
    {
        final ClusterMember[] clusterMembers = ClusterMember.parse(
            "0,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|" +
            "1,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|" +
            "2,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|");

        clusterMembers[0].publication(mock(ExclusivePublication.class));
        clusterMembers[1].publication(mock(ExclusivePublication.class));
        clusterMembers[2].publication(mock(ExclusivePublication.class));

        return clusterMembers;
    }
}
