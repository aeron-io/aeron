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
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.service.Cluster;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.TimeoutException;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.AgentTerminationException;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.*;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.ClusterMember.compareLog;
import static io.aeron.cluster.ConsensusModuleAgent.APPEND_POSITION_FLAG_NONE;
import static io.aeron.cluster.ElectionState.*;
import static java.lang.Math.max;

/**
 * Election process to determine a new cluster leader and catch up followers.
 */
class Election
{
    private final boolean isNodeStartup;
    private final long initialLogLeadershipTermId;
    private final long initialTermBaseLogPosition;
    private boolean isFirstInit = true;
    private boolean isLeaderStartup;
    private boolean isExtendedCanvass;
    private int logSessionId = NULL_SESSION_ID;
    private long timeOfLastStateChangeNs;
    private long timeOfLastUpdateNs;
    private long timeOfLastCommitPositionUpdateNs;
    private final long initialTimeOfLastUpdateNs;
    private long nominationDeadlineNs;
    private long logPosition;
    private long appendPosition;
    private long catchupJoinPosition = NULL_POSITION;
    private long catchupCommitPosition = 0;
    private long replicationLeadershipTermId = NULL_VALUE;
    private long replicationStopPosition = NULL_POSITION;
    private long leaderRecordingId = NULL_VALUE;
    private long leadershipTermId;
    private long logLeadershipTermId;
    private long candidateTermId;
    private ClusterMember leaderMember = null;
    private ElectionState state = INIT;
    private Subscription logSubscription = null;
    private LogReplay logReplay = null;
    private final ClusterMember[] clusterMembers;
    private final ClusterMember thisMember;
    private final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap;
    private final ConsensusPublisher consensusPublisher;
    private final ConsensusModule.Context ctx;
    private final ConsensusModuleAgent consensusModuleAgent;
    private RecordingReplication logReplication = null;
    private long replicationCommitPosition = 0;
    private long replicationDeadlineNs = 0;
    private long replicationTermBaseLogPosition;
    private long lastPublishedCommitPosition;
    private int gracefulClosedLeaderId;

    Election(
        final boolean isNodeStartup,
        final int gracefulClosedLeaderId,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long appendPosition,
        final ClusterMember[] clusterMembers,
        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap,
        final ClusterMember thisMember,
        final ConsensusPublisher consensusPublisher,
        final ConsensusModule.Context ctx,
        final ConsensusModuleAgent consensusModuleAgent)
    {
        this.isNodeStartup = isNodeStartup;
        this.isExtendedCanvass = isNodeStartup;
        this.gracefulClosedLeaderId = gracefulClosedLeaderId;
        this.logPosition = logPosition;
        this.appendPosition = appendPosition;
        this.logLeadershipTermId = leadershipTermId;
        this.initialLogLeadershipTermId = leadershipTermId;
        this.initialTermBaseLogPosition = termBaseLogPosition;
        this.leadershipTermId = leadershipTermId;
        this.candidateTermId = leadershipTermId;
        this.clusterMembers = clusterMembers;
        this.clusterMemberByIdMap = clusterMemberByIdMap;
        this.thisMember = thisMember;
        this.consensusPublisher = consensusPublisher;
        this.ctx = ctx;
        this.consensusModuleAgent = consensusModuleAgent;

        final long nowNs = ctx.clusterClock().timeNanos();
        this.initialTimeOfLastUpdateNs = nowNs - TimeUnit.DAYS.toNanos(1);
        this.timeOfLastUpdateNs = initialTimeOfLastUpdateNs;
        this.timeOfLastCommitPositionUpdateNs = initialTimeOfLastUpdateNs;

        Objects.requireNonNull(thisMember);
        ctx.electionStateCounter().setRelease(INIT.code());
        ctx.electionCounter().incrementRelease();

        if (clusterMembers.length == 1 && thisMember.id() == clusterMembers[0].id())
        {
            candidateTermId = max(leadershipTermId + 1, ctx.nodeStateFile().candidateTerm().candidateTermId() + 1);
            this.leadershipTermId = candidateTermId;
            leaderMember = thisMember;
            ctx.nodeStateFile().updateCandidateTermId(candidateTermId, logPosition, ctx.epochClock().time());
            state(LEADER_LOG_REPLICATION, nowNs, "");
        }
    }

    ClusterMember leader()
    {
        return leaderMember;
    }

    int logSessionId()
    {
        return logSessionId;
    }

    long leadershipTermId()
    {
        return leadershipTermId;
    }

    long logPosition()
    {
        return logPosition;
    }

    boolean isLeaderStartup()
    {
        return isLeaderStartup;
    }

    int thisMemberId()
    {
        return thisMember.id();
    }

    int doWork(final long nowNs)
    {
        int workCount = 0;

        switch (state)
        {
            case INIT:
                workCount += init(nowNs);
                break;

            case CANVASS:
                workCount += canvass(nowNs);
                break;

            case NOMINATE:
                workCount += nominate(nowNs);
                break;

            case CANDIDATE_BALLOT:
                workCount += candidateBallot(nowNs);
                break;

            case FOLLOWER_BALLOT:
                workCount += followerBallot(nowNs);
                break;

            case LEADER_LOG_REPLICATION:
                workCount += leaderLogReplication(nowNs);
                break;

            case LEADER_REPLAY:
                workCount += leaderReplay(nowNs);
                break;

            case LEADER_INIT:
                workCount += leaderInit(nowNs);
                break;

            case LEADER_READY:
                workCount += leaderReady(nowNs);
                break;

            case FOLLOWER_LOG_REPLICATION:
                workCount += followerLogReplication(nowNs);
                break;

            case FOLLOWER_REPLAY:
                workCount += followerReplay(nowNs);
                break;

            case FOLLOWER_CATCHUP_INIT:
                workCount += followerCatchupInit(nowNs);
                break;

            case FOLLOWER_CATCHUP_AWAIT:
                workCount += followerCatchupAwait(nowNs);
                break;

            case FOLLOWER_CATCHUP:
                workCount += followerCatchup(nowNs);
                break;

            case FOLLOWER_LOG_INIT:
                workCount += followerLogInit(nowNs);
                break;

            case FOLLOWER_LOG_AWAIT:
                workCount += followerLogAwait(nowNs);
                break;

            case FOLLOWER_READY:
                workCount += followerReady(nowNs);
                break;

            case CLOSED:
                break;
        }

        return workCount;
    }

    void handleError(final long nowNs, final Throwable ex)
    {
        ctx.countedErrorHandler().onError(ex);
        logPosition = ctx.commitPositionCounter().getPlain();
        state(INIT, nowNs, ex.getMessage());

        if (ex instanceof AgentTerminationException || ex instanceof InterruptedException)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    void onRecordingSignal(
        final long correlationId, final long recordingId, final long position, final RecordingSignal signal)
    {
        if (INIT == state)
        {
            return;
        }

        if (null != logReplication)
        {
            logReplication.onSignal(correlationId, recordingId, position, signal);
            consensusModuleAgent.logRecordingId(logReplication.recordingId());
        }
    }

    void onCanvassPosition(
        final long logLeadershipTermId,
        final long logPosition,
        final long leadershipTermId,
        final int followerMemberId,
        final int protocolVersion)
    {
        if (INIT == state)
        {
            return;
        }

        if (followerMemberId == gracefulClosedLeaderId)
        {
            gracefulClosedLeaderId = NULL_VALUE;
        }

        final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
        if (null != follower && thisMember.id() != followerMemberId)
        {
            follower
                .leadershipTermId(logLeadershipTermId)
                .logPosition(logPosition);

            if (logLeadershipTermId < this.leadershipTermId)
            {
                if (Cluster.Role.LEADER == consensusModuleAgent.role())
                {
                    publishNewLeadershipTerm(follower, logLeadershipTermId, ctx.clusterClock().time());
                }
            }
            else if (logLeadershipTermId > this.leadershipTermId)
            {
                switch (state)
                {
                    case LEADER_LOG_REPLICATION:
                    case LEADER_READY:
                        throw new ClusterEvent("potential new election in progress");

                    default:
                        break;
                }
            }
        }
    }

    void onRequestVote(
        final long logLeadershipTermId,
        final long logPosition,
        final long candidateTermId,
        final int candidateId,
        final int protocolVersion)
    {
        if (INIT == state)
        {
            return;
        }

        if (candidateId == thisMember.id())
        {
            return;
        }

        if (candidateTermId <= this.candidateTermId)
        {
            placeVote(candidateTermId, candidateId, false);
        }
        else if (compareLog(this.logLeadershipTermId, appendPosition, logLeadershipTermId, logPosition) > 0)
        {
            this.candidateTermId = ctx.nodeStateFile().proposeMaxCandidateTermId(
                candidateTermId, logPosition, ctx.epochClock().time());

            placeVote(candidateTermId, candidateId, false);

            final ClusterMember candidateMember = clusterMemberByIdMap.get(candidateId);
            if (null != candidateMember && Cluster.Role.LEADER == consensusModuleAgent.role())
            {
                publishNewLeadershipTerm(candidateMember, logLeadershipTermId, ctx.clusterClock().time());
            }
        }
        else if (CANVASS == state || NOMINATE == state || CANDIDATE_BALLOT == state || FOLLOWER_BALLOT == state)
        {
            this.candidateTermId = ctx.nodeStateFile().proposeMaxCandidateTermId(
                candidateTermId, logPosition, ctx.epochClock().time());
            placeVote(candidateTermId, candidateId, true);
            state(FOLLOWER_BALLOT, ctx.clusterClock().timeNanos(), "");
        }
    }

    void onVote(
        final long candidateTermId,
        final long logLeadershipTermId,
        final long logPosition,
        final int candidateMemberId,
        final int followerMemberId,
        final boolean vote)
    {
        if (INIT == state)
        {
            return;
        }

        if (CANDIDATE_BALLOT == state &&
            candidateTermId == this.candidateTermId &&
            candidateMemberId == thisMember.id())
        {
            final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
            if (null != follower)
            {
                follower
                    .candidateTermId(candidateTermId)
                    .leadershipTermId(logLeadershipTermId)
                    .logPosition(logPosition)
                    .vote(vote ? Boolean.TRUE : Boolean.FALSE);
            }
        }
    }

    void onNewLeadershipTerm(
        final long logLeadershipTermId,
        final long nextLeadershipTermId,
        final long nextTermBaseLogPosition,
        final long nextLogPosition,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long leaderRecordingId,
        final long timestamp,
        final int leaderMemberId,
        final int logSessionId,
        final boolean isStartup)
    {
        if (INIT == state)
        {
            return;
        }

        final ClusterMember leader = clusterMemberByIdMap.get(leaderMemberId);
        if (null == leader || (leaderMemberId == thisMember.id() && leadershipTermId == this.leadershipTermId))
        {
            return;
        }

        if (leaderMemberId == gracefulClosedLeaderId)
        {
            gracefulClosedLeaderId = NULL_VALUE;
        }

        if (((FOLLOWER_BALLOT == state || CANDIDATE_BALLOT == state) && leadershipTermId == candidateTermId) ||
            CANVASS == state)
        {
            if (logLeadershipTermId == this.logLeadershipTermId)
            {
                if (NULL_POSITION != nextTermBaseLogPosition && nextTermBaseLogPosition < appendPosition)
                {
                    onTruncateLogEntry(
                        thisMember.id(),
                        state,
                        logLeadershipTermId,
                        this.leadershipTermId,
                        candidateTermId,
                        ctx.commitPositionCounter().getPlain(),
                        this.logPosition,
                        appendPosition,
                        logPosition,
                        nextTermBaseLogPosition);
                }

                this.leaderMember = leader;
                this.isLeaderStartup = isStartup;
                this.leadershipTermId = leadershipTermId;
                this.candidateTermId = max(leadershipTermId, candidateTermId);
                this.logSessionId = logSessionId;
                this.leaderRecordingId = leaderRecordingId;
                this.catchupJoinPosition = appendPosition < logPosition ? logPosition : NULL_POSITION;

                if (this.appendPosition < termBaseLogPosition)
                {
                    if (NULL_VALUE != nextLeadershipTermId)
                    {
                        if (appendPosition < nextTermBaseLogPosition)
                        {
                            replicationLeadershipTermId = logLeadershipTermId;
                            replicationStopPosition = nextTermBaseLogPosition;
                            // Here we should have an open, but uncommitted term so the base position
                            // is already known. We could look it up from the recording log only to write
                            // it back again...
                            replicationTermBaseLogPosition = NULL_VALUE;
                            state(FOLLOWER_LOG_REPLICATION, ctx.clusterClock().timeNanos(), "");
                        }
                        else if (appendPosition == nextTermBaseLogPosition)
                        {
                            if (NULL_POSITION != nextLogPosition)
                            {
                                replicationLeadershipTermId = nextLeadershipTermId;
                                replicationStopPosition = nextLogPosition;
                                replicationTermBaseLogPosition = nextTermBaseLogPosition;
                                state(FOLLOWER_LOG_REPLICATION, ctx.clusterClock().timeNanos(), "");
                            }
                        }
                    }
                    else
                    {
                        throw new ClusterException(
                            "invalid newLeadershipTerm - this.appendPosition=" + appendPosition +
                            " < termBaseLogPosition=" + termBaseLogPosition +
                            " and nextLeadershipTermId=" + nextLeadershipTermId +
                            ", logLeadershipTermId=" + logLeadershipTermId +
                            ", nextTermBaseLogPosition=" + nextTermBaseLogPosition +
                            ", nextLogPosition=" + nextLogPosition + ", leadershipTermId=" + leadershipTermId +
                            ", termBaseLogPosition=" + termBaseLogPosition + ", logPosition=" + logPosition +
                            ", leaderRecordingId=" + leaderRecordingId + ", leaderMemberId=" + leaderMemberId +
                            ", logSessionId=" + logSessionId + ", isStartup=" + isStartup);
                    }
                }
                else
                {
                    state(FOLLOWER_REPLAY, ctx.clusterClock().timeNanos(), "");
                }
            }
            else
            {
                state(CANVASS, ctx.clusterClock().timeNanos(), "");
            }
        }

        if (state == FOLLOWER_LOG_REPLICATION && leaderMemberId == this.leaderMember.id())
        {
            replicationDeadlineNs = ctx.clusterClock().timeNanos() + ctx.leaderHeartbeatTimeoutNs();
        }
    }

    void onAppendPosition(
        final long leadershipTermId,
        final long logPosition,
        final int followerMemberId,
        final short flags)
    {
        if (INIT == state)
        {
            return;
        }

        if (leadershipTermId <= this.leadershipTermId)
        {
            final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
            if (null != follower)
            {
                follower
                    .leadershipTermId(leadershipTermId)
                    .logPosition(logPosition)
                    .timeOfLastAppendPositionNs(ctx.clusterClock().timeNanos());

                consensusModuleAgent.trackCatchupCompletion(follower, leadershipTermId, flags);
            }
        }
    }

    void onCommitPosition(final long leadershipTermId, final long logPosition, final int leaderMemberId)
    {
        if (INIT == state)
        {
            return;
        }

        if (leadershipTermId == this.leadershipTermId &&
            NULL_POSITION != catchupJoinPosition &&
            FOLLOWER_CATCHUP == state &&
            leaderMemberId == leaderMember.id())
        {
            catchupCommitPosition = max(catchupCommitPosition, logPosition);
        }
        else if (FOLLOWER_LOG_REPLICATION == state && leaderMemberId == leaderMember.id())
        {
            replicationCommitPosition = max(replicationCommitPosition, logPosition);
            replicationDeadlineNs = ctx.clusterClock().timeNanos() + ctx.leaderHeartbeatTimeoutNs();
        }
        else if (leadershipTermId > this.leadershipTermId && LEADER_READY == state)
        {
            throw new ClusterEvent("new leader detected due to commit position - " +
                " memberId=" + thisMemberId() +
                " this.leadershipTermId=" + this.leadershipTermId +
                " this.leaderMemberId=" + thisMemberId() +
                " this.logPosition=" + this.logPosition +
                " newLeadershipTermId=" + leadershipTermId +
                " newLeaderMemberId=" + leaderMemberId +
                " newLogPosition=" + logPosition +
                ")");
        }
    }

    void onReplayNewLeadershipTermEvent(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition)
    {
        if (INIT == state)
        {
            return;
        }

        if (FOLLOWER_CATCHUP == state || FOLLOWER_REPLAY == state)
        {
            final long nowNs = ctx.clusterClock().convertToNanos(timestamp);
            ensureRecordingLogCoherent(leadershipTermId, termBaseLogPosition, NULL_VALUE, nowNs);
            this.logPosition = logPosition;
            this.logLeadershipTermId = leadershipTermId;
        }
    }

    void onTruncateLogEntry(
        final int memberId,
        final ElectionState state,
        final long logLeadershipTermId,
        final long leadershipTermId,
        final long candidateTermId,
        final long commitPosition,
        final long logPosition,
        final long appendPosition,
        final long oldPosition,
        final long newPosition)
    {
        consensusModuleAgent.truncateLogEntry(logLeadershipTermId, newPosition);
        throw new ClusterEvent("Truncating Cluster Log - memberId=" + memberId +
            " state=" + state +
            " this.logLeadershipTermId=" + logLeadershipTermId +
            " this.leadershipTermId=" + leadershipTermId +
            " this.candidateTermId=" + candidateTermId +
            " this.commitPosition=" + commitPosition +
            " this.logPosition=" + logPosition +
            " this.appendPosition=" + appendPosition +
            " oldPosition=" + oldPosition +
            " newPosition=" + newPosition);
    }

    private int init(final long nowNs)
    {
        if (isFirstInit)
        {
            isFirstInit = false;
            if (!isNodeStartup)
            {
                prepareForNewLeadership(nowNs);
            }
        }
        else
        {
            stopLogReplication();
            stopCatchup();

            prepareForNewLeadership(nowNs);
            logSessionId = NULL_SESSION_ID;
            stopReplay();

            if (null != logSubscription)
            {
                CloseHelper.close(logSubscription);
                consensusModuleAgent.awaitLocalSocketsClosed(logSubscription.registrationId());
                logSubscription = null;
            }
        }

        candidateTermId = max(ctx.nodeStateFile().candidateTerm().candidateTermId(), leadershipTermId);

        if (clusterMembers.length == 1 && thisMember.id() == clusterMembers[0].id())
        {
            state(LEADER_LOG_REPLICATION, nowNs, "");
        }
        else
        {
            state(CANVASS, nowNs, "");
        }

        return 1;
    }

    private int canvass(final long nowNs)
    {
        int workCount = 0;
        final long deadlineNs = isExtendedCanvass ?
            timeOfLastStateChangeNs + ctx.startupCanvassTimeoutNs() :
            consensusModuleAgent.timeOfLastLeaderUpdateNs() + ctx.leaderHeartbeatTimeoutNs();

        if (hasUpdateIntervalExpired(nowNs, ctx.electionStatusIntervalNs()))
        {
            timeOfLastUpdateNs = nowNs;
            publishCanvassPosition();

            workCount++;
        }

        if (ctx.appointedLeaderId() != NULL_VALUE && ctx.appointedLeaderId() != thisMember.id())
        {
            return workCount;
        }

        if (ClusterMember.isUnanimousCandidate(clusterMembers, thisMember, gracefulClosedLeaderId) ||
            (nowNs >= deadlineNs && ClusterMember.isQuorumCandidate(clusterMembers, thisMember)))
        {
            final long delayNs = (long)(ctx.random().nextDouble() * (ctx.electionTimeoutNs() >> 1));
            nominationDeadlineNs = nowNs + delayNs;
            state(NOMINATE, nowNs, "");
            workCount++;
        }

        return workCount;
    }

    private int nominate(final long nowNs)
    {
        if (nowNs >= nominationDeadlineNs)
        {
            candidateTermId = ctx.nodeStateFile().proposeMaxCandidateTermId(
                candidateTermId + 1, logPosition, ctx.epochClock().time());
            ClusterMember.becomeCandidate(clusterMembers, candidateTermId, thisMember.id());
            state(CANDIDATE_BALLOT, nowNs, "");

            return 1;
        }
        else if (hasUpdateIntervalExpired(nowNs, ctx.electionStatusIntervalNs()))
        {
            timeOfLastUpdateNs = nowNs;
            publishCanvassPosition();

            return 1;
        }

        return 0;
    }

    private int candidateBallot(final long nowNs)
    {
        int workCount = 0;

        if (ClusterMember.isUnanimousLeader(clusterMembers, candidateTermId, gracefulClosedLeaderId))
        {
            leaderMember = thisMember;
            leadershipTermId = candidateTermId;
            state(LEADER_LOG_REPLICATION, nowNs, "");
            workCount++;
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.electionTimeoutNs()))
        {
            if (ClusterMember.isQuorumLeader(clusterMembers, candidateTermId))
            {
                leaderMember = thisMember;
                leadershipTermId = candidateTermId;
                state(LEADER_LOG_REPLICATION, nowNs, "");
            }
            else
            {
                state(CANVASS, nowNs, "");
            }

            workCount++;
        }
        else
        {
            for (final ClusterMember member : clusterMembers)
            {
                if (!member.isBallotSent())
                {
                    workCount++;
                    member.isBallotSent(consensusPublisher.requestVote(
                        member.publication(), logLeadershipTermId, appendPosition, candidateTermId, thisMember.id()));
                }
            }
        }

        return workCount;
    }

    private int followerBallot(final long nowNs)
    {
        int workCount = 0;

        if (nowNs >= (timeOfLastStateChangeNs + ctx.electionTimeoutNs()))
        {
            state(CANVASS, nowNs, "");
            workCount++;
        }

        return workCount;
    }

    private int leaderLogReplication(final long nowNs)
    {
        int workCount = 0;

        thisMember.logPosition(appendPosition).timeOfLastAppendPositionNs(nowNs);
        final long quorumPosition = consensusModuleAgent.quorumPosition();

        workCount += publishNewLeadershipTermOnInterval(nowNs);
        workCount += publishCommitPositionOnInterval(quorumPosition, nowNs);

        if (quorumPosition >= appendPosition)
        {
            workCount++;
            state(LEADER_REPLAY, nowNs, "");
        }

        return workCount;
    }

    private int leaderReplay(final long nowNs)
    {
        int workCount = 0;

        if (null == logReplay)
        {
            if (logPosition < appendPosition)
            {
                logReplay = consensusModuleAgent.newLogReplay(logPosition, appendPosition);
            }
            else
            {
                state(LEADER_INIT, nowNs, "");
            }

            workCount++;
            isLeaderStartup = isNodeStartup;
            ClusterMember.resetLogPositions(clusterMembers, NULL_POSITION);
            thisMember.leadershipTermId(leadershipTermId).logPosition(appendPosition);
        }
        else
        {
            workCount += logReplay.doWork();
            if (logReplay.isDone())
            {
                stopReplay();
                logPosition = appendPosition;
                state(LEADER_INIT, nowNs, "");
            }
        }

        workCount += publishNewLeadershipTermOnInterval(nowNs);
        workCount += publishCommitPositionOnInterval(consensusModuleAgent.quorumPosition(), nowNs);

        return workCount;
    }

    private int leaderInit(final long nowNs)
    {
        consensusModuleAgent.joinLogAsLeader(leadershipTermId, logPosition, logSessionId, isLeaderStartup);
        updateRecordingLog(nowNs);
        state(LEADER_READY, nowNs, "");

        return 1;
    }

    private int leaderReady(final long nowNs)
    {
        int workCount = consensusModuleAgent.updateLeaderPosition(nowNs, appendPosition);
        workCount += publishNewLeadershipTermOnInterval(nowNs);

        if (ClusterMember.hasVotersAtPosition(clusterMembers, logPosition, leadershipTermId) ||
            (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()) &&
            ClusterMember.hasQuorumAtPosition(clusterMembers, logPosition, leadershipTermId)))
        {
            if (consensusModuleAgent.appendNewLeadershipTermEvent(nowNs))
            {
                consensusModuleAgent.electionComplete(nowNs);
                state(CLOSED, nowNs, "");
                workCount++;
            }
        }

        return workCount;
    }

    private int followerLogReplication(final long nowNs)
    {
        int workCount = 0;

        if (null == logReplication)
        {
            if (appendPosition < replicationStopPosition)
            {
                logReplication = consensusModuleAgent.newLogReplication(
                    leaderMember.archiveEndpoint(),
                    leaderMember.archiveResponseEndpoint(),
                    leaderRecordingId,
                    replicationStopPosition,
                    nowNs);
                replicationDeadlineNs = nowNs + ctx.leaderHeartbeatTimeoutNs();
                workCount++;
            }
            else
            {
                updateRecordingLogForReplication(
                    replicationLeadershipTermId, replicationTermBaseLogPosition, replicationStopPosition, nowNs);
                state(CANVASS, nowNs, "");
            }
        }
        else
        {
            workCount += consensusModuleAgent.pollArchiveEvents();
            logReplication.poll(nowNs);
            final boolean replicationDone = logReplication.hasReplicationEnded() && logReplication.hasStopped();
            // Log replication runs concurrently, calling this after the check for completion ensures that the
            // last position at the end of the leadership is published as an appendPosition event.
            workCount += publishFollowerReplicationPosition(nowNs);

            if (replicationDone)
            {
                if (replicationCommitPosition >= appendPosition)
                {
                    ConsensusModuleAgent.logReplicationEnded(
                        thisMember.id(),
                        "ELECTION",
                        logReplication.srcArchiveChannel(),
                        logReplication.recordingId(),
                        leaderRecordingId,
                        logReplication.position(),
                        logReplication.hasSynced());

                    appendPosition = logReplication.position();
                    stopLogReplication();
                    updateRecordingLogForReplication(
                        replicationLeadershipTermId, replicationTermBaseLogPosition, replicationStopPosition, nowNs);
                    state(CANVASS, nowNs, "");
                    workCount++;
                }
                else if (nowNs >= replicationDeadlineNs)
                {
                    throw new TimeoutException("timeout awaiting commit position", AeronException.Category.WARN);
                }
            }
        }

        return workCount;
    }

    private int followerReplay(final long nowNs)
    {
        int workCount = 0;

        if (null == logReplay)
        {
            workCount++;
            if (logPosition < appendPosition)
            {
                logReplay = consensusModuleAgent.newLogReplay(logPosition, appendPosition);
            }
            else
            {
                state(
                    NULL_POSITION != catchupJoinPosition ? FOLLOWER_CATCHUP_INIT : FOLLOWER_LOG_INIT,
                    nowNs,
                    "skip log replay");
            }
        }
        else
        {
            workCount += logReplay.doWork();
            if (logReplay.isDone())
            {
                stopReplay();
                logPosition = appendPosition;
                state(
                    NULL_POSITION != catchupJoinPosition ? FOLLOWER_CATCHUP_INIT : FOLLOWER_LOG_INIT,
                    nowNs,
                    "log replay done");
            }
        }

        return workCount;
    }

    private int followerCatchupInit(final long nowNs)
    {
        if (null == logSubscription)
        {
            logSubscription = addFollowerSubscription();
            addCatchupLogDestination();
        }

        String catchupEndpoint = null;
        final String endpoint = thisMember.catchupEndpoint();
        if (endpoint.endsWith(":0"))
        {
            final String resolvedEndpoint = logSubscription.resolvedEndpoint();
            if (null != resolvedEndpoint)
            {
                final int i = resolvedEndpoint.lastIndexOf(':');
                catchupEndpoint = endpoint.substring(0, endpoint.length() - 2) + resolvedEndpoint.substring(i);
            }
        }
        else
        {
            catchupEndpoint = endpoint;
        }

        if (null != catchupEndpoint && sendCatchupPosition(catchupEndpoint))
        {
            timeOfLastUpdateNs = nowNs;
            consensusModuleAgent.catchupInitiated(nowNs);
            state(FOLLOWER_CATCHUP_AWAIT, nowNs, "");
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
        {
            throw new TimeoutException("failed to send catchup position", AeronException.Category.WARN);
        }

        return 1;
    }

    private int followerCatchupAwait(final long nowNs)
    {
        int workCount = 0;

        final Image image = logSubscription.imageBySessionId(logSessionId);
        if (null != image)
        {
            verifyLogJoinPosition("followerCatchupAwait", image.joinPosition());
            if (consensusModuleAgent.tryJoinLogAsFollower(image, isLeaderStartup, nowNs))
            {
                state(FOLLOWER_CATCHUP, nowNs, "");
                workCount++;
            }
            else if (ChannelEndpointStatus.ERRORED == logSubscription.channelStatus())
            {
                final String message = "failed to add catchup log as follower - " + logSubscription.channel();
                throw new ClusterException(message, AeronException.Category.WARN);
            }
            else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
            {
                throw new TimeoutException("failed to join catchup log as follower", AeronException.Category.WARN);
            }
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
        {
            throw new TimeoutException("failed to join catchup log", AeronException.Category.WARN);
        }

        return workCount;
    }

    private int followerCatchup(final long nowNs)
    {
        int workCount = consensusModuleAgent.catchupPoll(catchupCommitPosition, nowNs);

        if (null == consensusModuleAgent.liveLogDestination() &&
            consensusModuleAgent.isCatchupNearLive(max(catchupJoinPosition, catchupCommitPosition)))
        {
            addLiveLogDestination();
            workCount++;
        }

        final long position = ctx.commitPositionCounter().getPlain();
        if (position >= catchupJoinPosition &&
            position >= catchupCommitPosition &&
            null == consensusModuleAgent.catchupLogDestination() &&
            ConsensusModule.State.SNAPSHOT != consensusModuleAgent.state())
        {
            appendPosition = position;
            logPosition = position;
            state(FOLLOWER_LOG_INIT, nowNs, "");
            workCount++;
        }

        return workCount;
    }

    private int followerLogInit(final long nowNs)
    {
        if (null == logSubscription)
        {
            if (NULL_SESSION_ID != logSessionId)
            {
                logSubscription = addFollowerSubscription();
                addLiveLogDestination();
                state(FOLLOWER_LOG_AWAIT, nowNs, "");
            }
        }
        else
        {
            state(FOLLOWER_READY, nowNs, "");
        }

        return 1;
    }

    private int followerLogAwait(final long nowNs)
    {
        int workCount = 0;

        final Image image = logSubscription.imageBySessionId(logSessionId);
        if (null != image)
        {
            verifyLogJoinPosition("followerLogAwait", image.joinPosition());
            if (consensusModuleAgent.tryJoinLogAsFollower(image, isLeaderStartup, nowNs))
            {
                appendPosition = image.joinPosition();
                logPosition = image.joinPosition();
                updateRecordingLog(nowNs);
                state(FOLLOWER_READY, nowNs, "");
                workCount++;
            }
            else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
            {
                throw new TimeoutException("failed to join live log as follower", AeronException.Category.WARN);
            }
        }
        else if (ChannelEndpointStatus.ERRORED == logSubscription.channelStatus())
        {
            final String message = "failed to add live log as follower - " + logSubscription.channel();
            throw new ClusterException(message, AeronException.Category.WARN);
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
        {
            throw new TimeoutException("failed to join live log", AeronException.Category.WARN);
        }

        return workCount;
    }

    private int followerReady(final long nowNs)
    {
        if (consensusPublisher.appendPosition(
            leaderMember.publication(), leadershipTermId, logPosition, thisMember.id(), APPEND_POSITION_FLAG_NONE))
        {
            consensusModuleAgent.electionComplete(nowNs);
            state(CLOSED, nowNs, "");
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
        {
            throw new TimeoutException("ready follower failed to notify leader", AeronException.Category.WARN);
        }

        return 1;
    }

    private void placeVote(final long candidateTermId, final int candidateId, final boolean vote)
    {
        final ClusterMember candidate = clusterMemberByIdMap.get(candidateId);
        if (null != candidate)
        {
            consensusPublisher.placeVote(
                candidate.publication(),
                candidateTermId,
                logLeadershipTermId,
                appendPosition,
                candidateId,
                thisMember.id(),
                vote);
        }
    }

    private int publishNewLeadershipTermOnInterval(final long nowNs)
    {
        int workCount = 0;

        if (hasUpdateIntervalExpired(nowNs, ctx.leaderHeartbeatIntervalNs()))
        {
            timeOfLastUpdateNs = nowNs;
            publishNewLeadershipTerm(ctx.clusterClock().timeUnit().convert(nowNs, TimeUnit.NANOSECONDS));
            workCount++;
        }

        return workCount;
    }

    private int publishCommitPositionOnInterval(final long quorumPosition, final long nowNs)
    {
        int workCount = 0;

        if (lastPublishedCommitPosition < quorumPosition ||
            (lastPublishedCommitPosition == quorumPosition &&
            hasIntervalExpired(nowNs, timeOfLastCommitPositionUpdateNs, ctx.leaderHeartbeatIntervalNs())))
        {
            timeOfLastCommitPositionUpdateNs = nowNs;
            lastPublishedCommitPosition = quorumPosition;
            consensusModuleAgent.publishCommitPosition(quorumPosition);
            workCount++;
        }

        return workCount;
    }

    private void publishCanvassPosition()
    {
        for (final ClusterMember member : clusterMembers)
        {
            if (member.id() != thisMember.id())
            {
                if (null == member.publication())
                {
                    ClusterMember.tryAddPublication(
                        member,
                        ctx.consensusStreamId(),
                        ctx.aeron(),
                        ctx.countedErrorHandler());
                }

                consensusPublisher.canvassPosition(
                    member.publication(), logLeadershipTermId, appendPosition, leadershipTermId, thisMember.id());
            }
        }
    }

    private void publishNewLeadershipTerm(final long timestamp)
    {
        for (final ClusterMember member : clusterMembers)
        {
            publishNewLeadershipTerm(member, logLeadershipTermId, timestamp);
        }
    }

    private void publishNewLeadershipTerm(
        final ClusterMember member, final long logLeadershipTermId, final long timestamp)
    {
        if (member.id() != thisMember.id() && NULL_SESSION_ID != logSessionId)
        {
            final RecordingLog.Entry logNextTermEntry =
                ctx.recordingLog().findTermEntry(logLeadershipTermId + 1);

            final long nextLeadershipTermId = null != logNextTermEntry ?
                logNextTermEntry.leadershipTermId : leadershipTermId;
            final long nextTermBaseLogPosition = null != logNextTermEntry ?
                logNextTermEntry.termBaseLogPosition : appendPosition;

            final long nextLogPosition = null != logNextTermEntry ?
                (NULL_POSITION != logNextTermEntry.logPosition ? logNextTermEntry.logPosition : appendPosition) :
                NULL_POSITION;

            consensusPublisher.newLeadershipTerm(
                member.publication(),
                logLeadershipTermId,
                nextLeadershipTermId,
                nextTermBaseLogPosition,
                nextLogPosition,
                leadershipTermId,
                appendPosition,
                appendPosition,
                consensusModuleAgent.logRecordingId(),
                timestamp,
                thisMember.id(),
                logSessionId,
                ctx.appVersion(),
                isLeaderStartup);
        }
    }

    private int publishFollowerReplicationPosition(final long nowNs)
    {
        final long position = logReplication.position();
        if (position > appendPosition ||
            (position == appendPosition && hasUpdateIntervalExpired(nowNs, ctx.leaderHeartbeatIntervalNs())))
        {
            if (consensusPublisher.appendPosition(
                leaderMember.publication(), leadershipTermId, position, thisMember.id(), APPEND_POSITION_FLAG_NONE))
            {
                appendPosition = position;
                timeOfLastUpdateNs = nowNs;
                return 1;
            }
        }

        return 0;
    }

    private boolean sendCatchupPosition(final String catchupEndpoint)
    {
        return consensusPublisher.catchupPosition(
            leaderMember.publication(), leadershipTermId, logPosition, thisMember.id(), catchupEndpoint);
    }

    private void addCatchupLogDestination()
    {
        final String destination = ChannelUri.createDestinationUri(ctx.logChannel(), thisMember.catchupEndpoint());
        logSubscription.addDestination(destination);
        consensusModuleAgent.catchupLogDestination(destination);
    }

    private void addLiveLogDestination()
    {
        final String destination;
        if (ctx.isLogMdc())
        {
            destination = ChannelUri.createDestinationUri(ctx.logChannel(), thisMember.logEndpoint());
        }
        else
        {
            destination = ctx.logChannel();
        }
        logSubscription.addDestination(destination);
        consensusModuleAgent.liveLogDestination(destination);
    }

    private Subscription addFollowerSubscription()
    {
        final Aeron aeron = ctx.aeron();
        final ChannelUri logChannelUri = ChannelUri.parse(ctx.logChannel());
        final String channel = new ChannelUriStringBuilder()
            .media(UDP_MEDIA)
            .tags(aeron.nextCorrelationId() + "," + aeron.nextCorrelationId())
            .controlMode(MDC_CONTROL_MODE_MANUAL)
            .sessionId(logSessionId)
            .group(Boolean.TRUE)
            .rejoin(Boolean.FALSE)
            .socketRcvbufLength(logChannelUri)
            .receiverWindowLength(logChannelUri)
            .alias("log-cm")
            .build();

        return aeron.addSubscription(channel, ctx.logStreamId());
    }

    private void state(final ElectionState newState, final long nowNs, final String reason)
    {
        if (newState != state)
        {
            if (CANVASS == state)
            {
                isExtendedCanvass = false;
            }

            switch (newState)
            {
                case CANVASS:
                    resetMembers();
                    consensusModuleAgent.role(Cluster.Role.FOLLOWER);
                    break;

                case CANDIDATE_BALLOT:
                    consensusModuleAgent.role(Cluster.Role.CANDIDATE);
                    break;

                case LEADER_LOG_REPLICATION:
                    consensusModuleAgent.role(Cluster.Role.LEADER);
                    logSessionId = consensusModuleAgent.addLogPublication(appendPosition);
                    break;

                case FOLLOWER_LOG_REPLICATION:
                case FOLLOWER_REPLAY:
                    consensusModuleAgent.role(Cluster.Role.FOLLOWER);
                    break;

                default:
                    break;
            }

            logStateChange(
                thisMember.id(),
                state,
                newState,
                null != leaderMember ? leaderMember.id() : NULL_VALUE,
                candidateTermId,
                leadershipTermId,
                logPosition,
                logLeadershipTermId,
                appendPosition,
                catchupJoinPosition,
                reason);

            state = newState;
            ctx.electionStateCounter().setRelease(newState.code());
            timeOfLastStateChangeNs = nowNs;
            timeOfLastUpdateNs = initialTimeOfLastUpdateNs;
            timeOfLastCommitPositionUpdateNs = initialTimeOfLastUpdateNs;
        }
    }

    private void stopCatchup()
    {
        consensusModuleAgent.stopAllCatchups();
        catchupJoinPosition = NULL_POSITION;
        catchupCommitPosition = 0;
    }

    private void resetMembers()
    {
        ClusterMember.reset(clusterMembers);
        thisMember.leadershipTermId(leadershipTermId).logPosition(appendPosition);
        leaderMember = null;
    }

    private void stopReplay()
    {
        if (null != logReplay)
        {
            logReplay.close();
            logReplay = null;
        }
    }

    private void stopLogReplication()
    {
        if (null != logReplication)
        {
            logReplication.close();
            logReplication = null;
        }
        replicationCommitPosition = 0;
        replicationDeadlineNs = 0;
        lastPublishedCommitPosition = 0;
    }

    private void ensureRecordingLogCoherent(
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long nowNs)
    {
        ensureRecordingLogCoherent(
            ctx,
            consensusModuleAgent.logRecordingId(),
            initialLogLeadershipTermId,
            initialTermBaseLogPosition,
            leadershipTermId,
            termBaseLogPosition,
            logPosition,
            nowNs);
    }

    static void ensureRecordingLogCoherent(
        final ConsensusModule.Context ctx,
        final long recordingId,
        final long initialLogLeadershipTermId,
        final long initialTermBaseLogPosition,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long nowNs)
    {
        if (NULL_VALUE == recordingId)
        {
            // This can happen during a log replication if the initial appendPosition != 0 and
            // nextTermLogPosition == appendPosition.
            return;
        }

        final long timestamp = ctx.clusterClock().timeUnit().convert(nowNs, TimeUnit.NANOSECONDS);
        final RecordingLog recordingLog = ctx.recordingLog();

        recordingLog.ensureCoherent(
            recordingId,
            initialLogLeadershipTermId,
            initialTermBaseLogPosition,
            leadershipTermId,
            NULL_VALUE != termBaseLogPosition ? termBaseLogPosition : initialTermBaseLogPosition,
            logPosition,
            nowNs,
            timestamp,
            ctx.fileSyncLevel());
    }

    private void updateRecordingLog(final long nowNs)
    {
        ensureRecordingLogCoherent(leadershipTermId, logPosition, NULL_VALUE, nowNs);
        logLeadershipTermId = leadershipTermId;
    }

    private void updateRecordingLogForReplication(
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long nowNs)
    {
        ensureRecordingLogCoherent(leadershipTermId, termBaseLogPosition, logPosition, nowNs);
        logLeadershipTermId = leadershipTermId;
    }

    private void verifyLogJoinPosition(final String state, final long joinPosition)
    {
        if (joinPosition != logPosition)
        {
            final String inequality = joinPosition < logPosition ? " less " : " greater ";
            throw new ClusterEvent(
                state + " - joinPosition=" + joinPosition + inequality + "than logPosition=" + logPosition);
        }
    }

    private boolean hasUpdateIntervalExpired(final long nowNs, final long intervalNs)
    {
        return hasIntervalExpired(nowNs, timeOfLastUpdateNs, intervalNs);
    }

    private boolean hasIntervalExpired(
        final long nowNs, final long previousTimestampForIntervalNs, final long intervalNs)
    {
        return (nowNs - previousTimestampForIntervalNs) >= intervalNs;
    }

    private void logStateChange(
        final int memberId,
        final ElectionState oldState,
        final ElectionState newState,
        final int leaderId,
        final long candidateTermId,
        final long leadershipTermId,
        final long logPosition,
        final long logLeadershipTermId,
        final long appendPosition,
        final long catchupPosition,
        final String reason)
    {
        /*
        System.out.println("Election: memberId=" + memberId + " " + oldState + " -> " + newState +
            " leaderId=" + leaderId +
            " candidateTermId=" + candidateTermId +
            " leadershipTermId=" + leadershipTermId +
            " logPosition=" + logPosition +
            " logLeadershipTermId=" + logLeadershipTermId +
            " appendPosition=" + appendPosition +
            " catchupPosition=" + catchupPosition +
            " reason=" + reason);
         */
    }

    private void prepareForNewLeadership(final long nowNs)
    {
        final long lastAppendPosition = consensusModuleAgent.prepareForNewLeadership(logPosition, nowNs);
        if (NULL_POSITION != lastAppendPosition)
        {
            appendPosition = lastAppendPosition;
        }
    }

    public String toString()
    {
        return "Election{" +
            "isNodeStartup=" + isNodeStartup +
            ", isLeaderStartup=" + isLeaderStartup +
            ", isExtendedCanvass=" + isExtendedCanvass +
            ", logSessionId=" + logSessionId +
            ", timeOfLastStateChangeNs=" + timeOfLastStateChangeNs +
            ", timeOfLastUpdateNs=" + timeOfLastUpdateNs +
            ", nominationDeadlineNs=" + nominationDeadlineNs +
            ", logPosition=" + logPosition +
            ", appendPosition=" + appendPosition +
            ", catchupJoinPosition=" + catchupJoinPosition +
            ", catchupCommitPosition=" + catchupCommitPosition +
            ", replicationStopPosition=" + replicationStopPosition +
            ", leaderRecordingId=" + leaderRecordingId +
            ", leadershipTermId=" + leadershipTermId +
            ", logLeadershipTermId=" + logLeadershipTermId +
            ", candidateTermId=" + candidateTermId +
            ", leaderMember=" + leaderMember +
            ", state=" + state +
            ", logSubscription=" + logSubscription +
            ", logReplay=" + logReplay +
            ", clusterMembers=" + Arrays.toString(clusterMembers) +
            ", thisMember=" + thisMember +
            ", clusterMemberByIdMap=" + clusterMemberByIdMap +
            ", logReplication=" + logReplication +
            ", ctx=" + ctx +
            '}';
    }
}
