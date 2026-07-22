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
package io.aeron.cluster.logging;

import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.eventlog.GeneratedLogger;
import io.aeron.eventlog.LoggerMethod;
import io.aeron.logging.EventConfiguration;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.util.concurrent.TimeUnit;

import static org.agrona.BitUtil.SIZE_OF_BYTE;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Event logger interface used by interceptors for recording cluster events into a {@link RingBuffer} for a
 * {@link ConsensusModule} events via a Java Agent. The implementation is generated at compile time from the
 * {@link LoggerMethod}-annotated methods below.
 */
@GeneratedLogger(
    encoder = "io.aeron.cluster.logging.ClusterEventEncoder",
    eventCodeType = "io.aeron.cluster.logging.ClusterEventCode")
public interface ClusterEventLogger
{
    /**
     * Logger for writing into the {@link ManyToOneRingBuffer} held by {@link EventConfiguration#eventReader}.
     */
    ClusterEventLogger LOGGER = new ClusterEventLoggerImpl(EventConfiguration.eventReader().ringBuffer());
    /**
     * CBOR-specific logger.
     */
    ClusterEventLogger CBOR_LOGGER = new CborClusterEventLogger(EventConfiguration.eventReader().ringBuffer());

    /**
     * Log a new leadership term event.
     *
     * @param memberId                of the current cluster node.
     * @param logLeadershipTermId     term for which log entries are present.
     * @param nextLeadershipTermId    next term relative to the logLeadershipTermId
     * @param nextTermBaseLogPosition base log position for the next term.
     * @param nextLogPosition         committed log position for next term.
     * @param leadershipTermId        new leadership term id.
     * @param termBaseLogPosition     position the log reached at base of new term.
     * @param logPosition             position the log reached for the new term (i.e. appendPosition of the leader node).
     * @param commitPosition          of the Cluster, i.e. quorum log position.
     * @param leaderRecordingId       of the log in the leader archive.
     * @param timestamp               of the new term.
     * @param leaderId                member id for the new leader.
     * @param logSessionId            session id of the log extension.
     * @param appVersion              associated with the recorded state.
     * @param isStartup               is the leader starting up fresh.
     */
    @LoggerMethod(eventCode = "NEW_LEADERSHIP_TERM", lengthMethod = "newLeaderShipTermLength")
    default void logOnNewLeadershipTerm(
        final int memberId,
        final long logLeadershipTermId,
        final long nextLeadershipTermId,
        final long nextTermBaseLogPosition,
        final long nextLogPosition,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long commitPosition,
        final long leaderRecordingId,
        final long timestamp,
        final int leaderId,
        final int logSessionId,
        final int appVersion,
        final boolean isStartup)
    {
    }

    /**
     * Log a state change event for a cluster node.
     *
     * @param <E>       type representing the state change.
     * @param eventCode for the type of state change.
     * @param memberId  of the current cluster node.
     * @param oldState  before the change.
     * @param newState  after the change.
     * @param reason for state to change.
     */
    @LoggerMethod(lengthMethod = "stateChangeLength", lengthArgs = { "oldState", "newState", "reason" },
        encodeArgs = { "memberId", "oldState", "newState", "reason" })
    default <E extends Enum<E>> void logStateChange(
        final ClusterEventCode eventCode, final int memberId, final E oldState, final E newState, final String reason)
    {
    }

    /**
     * Log an election state change event for a cluster node.
     *
     * @param <E>                 type representing the state change.
     * @param memberId            on which the change has taken place.
     * @param oldState            before the change.
     * @param newState            after the change.
     * @param leaderId            of the cluster.
     * @param candidateTermId     of the node.
     * @param leadershipTermId    of the node.
     * @param logPosition         of the node.
     * @param logLeadershipTermId of the node.
     * @param appendPosition      of the node.
     * @param catchupPosition     of the node.
     * @param reason              for the state transition to occur.
     */
    @LoggerMethod(eventCode = "ELECTION_STATE_CHANGE", lengthMethod = "electionStateChangeLength",
        lengthArgs = { "oldState", "newState", "reason" })
    default <E extends Enum<E>> void logElectionStateChange(
        final int memberId,
        final E oldState,
        final E newState,
        final int leaderId,
        final long candidateTermId,
        final long leadershipTermId,
        final long logPosition,
        final long logLeadershipTermId,
        final long appendPosition,
        final long catchupPosition,
        final String reason)
    {
    }

    /**
     * Log a canvass position event received by the cluster node.
     *
     * @param memberId            member who sent the event.
     * @param logLeadershipTermId leadershipTermId reached by the member for it recorded log.
     * @param logPosition         position the member has durably recorded.
     * @param leadershipTermId    the most current leadershipTermId a member has seen.
     * @param followerMemberId    follower node id.
     * @param protocolVersion     of the consensus module.
     */
    @LoggerMethod(eventCode = "CANVASS_POSITION", lengthMethod = "canvassPositionLength")
    default void logOnCanvassPosition(
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition,
        final long leadershipTermId,
        final int followerMemberId,
        final int protocolVersion)
    {
    }

    /**
     * Log a request to vote from a cluster candidate for new leadership.
     *
     * @param memberId            of the current cluster node.
     * @param logLeadershipTermId leadershipTermId processes from the log by the candidate.
     * @param logPosition         position reached in the log for the latest leadership term.
     * @param candidateTermId     the term id as the candidate sees it for the election.
     * @param candidateId         id of the candidate node.
     * @param protocolVersion     from the request.
     */
    @LoggerMethod(eventCode = "REQUEST_VOTE", fixedLength = 3 * SIZE_OF_LONG + 3 * SIZE_OF_INT)
    default void logOnRequestVote(
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition,
        final long candidateTermId,
        final int candidateId,
        final int protocolVersion)
    {
    }

    /**
     * Log a vote response from a cluster candidate for new leadership.
     *
     * @param memberId            of the current cluster node.
     * @param logLeadershipTermId leadershipTermId processes from the log by the candidate.
     * @param logPosition         position reached in the log for the latest leadership term.
     * @param candidateTermId     the term id as the candidate sees it for the election.
     * @param candidateId         id of the candidate node.
     * @param voterId             id of the follower node that voted.
     * @param vote                expressed by the follower node.
     */
    @LoggerMethod(eventCode = "VOTE", fixedLength = 3 * SIZE_OF_LONG + 3 * SIZE_OF_INT + SIZE_OF_BYTE,
        encodeArgs = {
            "memberId", "candidateTermId", "logLeadershipTermId", "logPosition", "candidateId", "voterId", "vote" })
    default void logOnVote(
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition,
        final long candidateTermId,
        final int candidateId,
        final int voterId,
        final boolean vote)
    {
    }

    /**
     * Log the catchup position message.
     *
     * @param memberId         of the current cluster node.
     * @param leadershipTermId leadership term to catch up on
     * @param logPosition      position to catchup from
     * @param followerMemberId the id of the follower that is catching up
     * @param catchupEndpoint  the endpoint to send catchup messages
     */
    @LoggerMethod(eventCode = "CATCHUP_POSITION", lengthMethod = "catchupPositionLength",
        lengthArgs = { "catchupEndpoint" })
    default void logOnCatchupPosition(
        final int memberId,
        final long leadershipTermId,
        final long logPosition,
        final int followerMemberId,
        final String catchupEndpoint)
    {
    }

    /**
     * Log the stop catchup message.
     *
     * @param memberId         of the current cluster node.
     * @param leadershipTermId current leadershipTermId.
     * @param followerMemberId id of follower currently catching up.
     */
    @LoggerMethod(eventCode = "STOP_CATCHUP", fixedLength = SIZE_OF_LONG + 2 * SIZE_OF_INT, skipCaptureLength = true)
    default void logOnStopCatchup(final int memberId, final long leadershipTermId, final int followerMemberId)
    {
    }

    /**
     * Log an event when a log entry is being truncated.
     *
     * @param <E>                 type of the enum.
     * @param memberId            the node which truncates its log entry.
     * @param state               of the election.
     * @param logLeadershipTermId the election is in.
     * @param leadershipTermId    the election is in.
     * @param candidateTermId     the election is in.
     * @param commitPosition      when the truncation happens.
     * @param logPosition         of the election.
     * @param appendPosition      of the election.
     * @param oldPosition         truncated from.
     * @param newPosition         truncated to.
     */
    @LoggerMethod(eventCode = "TRUNCATE_LOG_ENTRY", lengthMethod = "truncateLogEntryLength", lengthArgs = { "state" },
        encodeMethod = "encodeTruncateLogEntry")
    default <E extends Enum<E>> void logOnTruncateLogEntry(
        final int memberId,
        final E state,
        final long logLeadershipTermId,
        final long leadershipTermId,
        final long candidateTermId,
        final long commitPosition,
        final long logPosition,
        final long appendPosition,
        final long oldPosition,
        final long newPosition)
    {
    }

    /**
     * Log the replay of the leadership term id.
     *
     * @param memberId            current memberId.
     * @param isInElection        an election is currently in process.
     * @param leadershipTermId    the logged leadership term id.
     * @param logPosition         current position in the log.
     * @param timestamp           logged timestamp.
     * @param termBaseLogPosition initial position for this term.
     * @param timeUnit            cluster time unit.
     * @param appVersion          version of the application.
     */
    @LoggerMethod(eventCode = "REPLAY_NEW_LEADERSHIP_TERM", lengthMethod = "replayNewLeadershipTermEventLength",
        lengthArgs = { "timeUnit" })
    default void logOnReplayNewLeadershipTermEvent(
        final int memberId,
        final boolean isInElection,
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final TimeUnit timeUnit,
        final int appVersion)
    {
    }

    /**
     * The Append position received by the leader from a follower.
     *
     * @param memberId         of the current cluster node.
     * @param leadershipTermId the current leadership term id.
     * @param logPosition      the current position in the log.
     * @param followerMemberId follower member sending the Append position.
     * @param flags            applied to append position by follower.
     */
    @LoggerMethod(eventCode = "APPEND_POSITION",
        fixedLength = 2 * SIZE_OF_LONG + 2 * SIZE_OF_INT + SIZE_OF_BYTE, skipCaptureLength = true)
    default void logOnAppendPosition(
        final int memberId,
        final long leadershipTermId,
        final long logPosition,
        final int followerMemberId,
        final short flags)
    {
    }

    /**
     * The commit position received by the follower form the leader.
     *
     * @param memberId         of the node receiving commit position message.
     * @param leadershipTermId the current leadership term id.
     * @param logPosition      the current position in the log.
     * @param leaderId         leader member sending the commit position.
     */
    @LoggerMethod(eventCode = "COMMIT_POSITION", fixedLength = 2 * SIZE_OF_LONG + 2 * SIZE_OF_INT,
        skipCaptureLength = true)
    default void logOnCommitPosition(
        final int memberId, final long leadershipTermId, final long logPosition, final int leaderId)
    {
    }

    /**
     * Log the appending of a session close event to the log.
     *
     * @param memberId         member (leader) publishing the event.
     * @param sessionId        session id of the session be closed.
     * @param closeReason      reason to close the session.
     * @param leadershipTermId current leadership term id.
     * @param timestamp        the current timestamp.
     * @param timeUnit         units for the timestamp.
     */
    @LoggerMethod(eventCode = "APPEND_SESSION_CLOSE", lengthMethod = "appendSessionCloseLength",
        lengthArgs = { "closeReason", "timeUnit" })
    default void logAppendSessionClose(
        final int memberId,
        final long sessionId,
        final CloseReason closeReason,
        final long leadershipTermId,
        final long timestamp,
        final TimeUnit timeUnit)
    {
    }

    /**
     * Log the appending of a session open event to the log.
     *
     * @param memberId         member (leader) publishing the event.
     * @param sessionId        session id of the session be closed.
     * @param leadershipTermId current leadership term id.
     * @param logPosition      when session was opened.
     * @param timestamp        the current timestamp.
     * @param timeUnit         units for the timestamp.
     */
    @LoggerMethod(eventCode = "APPEND_SESSION_OPEN", lengthMethod = "appendSessionOpenLength",
        lengthArgs = { "timeUnit" })
    default void logAppendSessionOpen(
        final int memberId,
        final long sessionId,
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final TimeUnit timeUnit)
    {
    }

    /**
     * Log the receiving of a termination position event.
     *
     * @param memberId            that received the termination position.
     * @param logLeadershipTermId leadership term for the supplied position.
     * @param logPosition         position to terminate at.
     */
    @LoggerMethod(eventCode = "TERMINATION_POSITION", lengthMethod = "terminationPositionLength")
    default void logTerminationPosition(final int memberId, final long logLeadershipTermId, final long logPosition)
    {
    }

    /**
     * Log the receiving of an acknowledgement to a termination position event.
     *
     * @param memberId            that received the termination ack.
     * @param logLeadershipTermId leadership term for the supplied position.
     * @param logPosition         position to terminate at.
     * @param senderMemberId      member sending the ack.
     */
    @LoggerMethod(eventCode = "TERMINATION_ACK", lengthMethod = "terminationAckLength")
    default void logTerminationAck(
        final int memberId, final long logLeadershipTermId, final long logPosition, final int senderMemberId)
    {
    }

    /**
     * Log an ack received from a cluster service.
     *
     * @param memberId    memberId receiving the ack.
     * @param logPosition position in the log when the ack was sent.
     * @param timestamp   timestamp when the ack was sent.
     * @param timeUnit    time unit used for the timestamp.
     * @param ackId       id of the ack.
     * @param relevantId  associated id used in the ack, e.g. recordingId for snapshot acks.
     * @param serviceId   the id of the service that sent the ack.
     */
    @LoggerMethod(eventCode = "SERVICE_ACK", lengthMethod = "serviceAckLength", lengthArgs = { "timeUnit" })
    default void logServiceAck(
        final int memberId,
        final long logPosition,
        final long timestamp,
        final TimeUnit timeUnit,
        final long ackId,
        final long relevantId,
        final int serviceId)
    {
    }

    /**
     * Log a replication end event.
     *
     * @param memberId       memberId running the replication.
     * @param purpose        the reason for the replication.
     * @param channelOrNull  the channel used to connect to the source archive.
     * @param srcRecordingId source recording id.
     * @param dstRecordingId destination recording id.
     * @param position       the position where the recording ended.
     * @param hasSynced      was the sync event been received for the replication.
     */
    @LoggerMethod(eventCode = "REPLICATION_ENDED", lengthMethod = "replicationEndedLength",
        lengthArgs = { "purpose", "channelOrNull" })
    default void logReplicationEnded(
        final int memberId,
        final String purpose,
        final String channelOrNull,
        final long srcRecordingId,
        final long dstRecordingId,
        final long position,
        final boolean hasSynced)
    {
    }

    /**
     * Log a standby snapshot notification.
     *
     * @param memberId            memberId receiving the notification.
     * @param recordingId         the recording id of the standby snapshot in the remote archive.
     * @param leadershipTermId    the leadershipTermId of the standby snapshot.
     * @param termBaseLogPosition the termBaseLogPosition of the standby snapshot.
     * @param logPosition         the position of the standby snapshot when it is taken.
     * @param timestamp           the cluster timestamp when the snapshot is taken.
     * @param timeUnit            the cluster time unit.
     * @param serviceId           the serviceId for the snapshot.
     * @param archiveEndpoint     the endpoint holding the standby snapshot.
     */
    @LoggerMethod(eventCode = "STANDBY_SNAPSHOT_NOTIFICATION", lengthMethod = "standbySnapshotNotificationLength",
        lengthArgs = { "timeUnit", "archiveEndpoint" })
    default void logStandbySnapshotNotification(
        final int memberId,
        final long recordingId,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long timestamp,
        final TimeUnit timeUnit,
        final int serviceId,
        final String archiveEndpoint)
    {
    }

    /**
     * Log the start of the new election.
     *
     * @param memberId         memberId which start the election.
     * @param leadershipTermId of the member.
     * @param logPosition      the log position.
     * @param appendPosition   the append position.
     * @param reason           for election to be started.
     */
    @LoggerMethod(eventCode = "NEW_ELECTION", lengthMethod = "newElectionLength", lengthArgs = { "reason" })
    default void logNewElection(
        final int memberId,
        final long leadershipTermId,
        final long logPosition,
        final long appendPosition,
        final String reason)
    {
    }

    /**
     * Log a state change event for a cluster session.
     *
     * @param <A>       type representing the action.
     * @param <S>       type representing the state change.
     * @param memberId  of the current cluster node.
     * @param sessionId of the session.
     * @param action    action.
     * @param oldState  before the change.
     * @param newState  after the change.
     * @param reason    for the change.
     */
    @LoggerMethod(eventCode = "CLUSTER_SESSION_STATE_CHANGE", lengthMethod = "clusterSessionStateChangeLength",
        lengthArgs = { "action", "oldState", "newState", "reason" })
    default <A extends Enum<A>, S extends Enum<S>> void logClusterSessionStateChange(
        final int memberId,
        final long sessionId,
        final A action,
        final S oldState,
        final S newState,
        final String reason)
    {
    }

    /**
     * Log a snapshot entry invalidation.
     *
     * @param memberId      on which the snapshot was invalidated.
     * @param entryIndex    within the recording long that was invalidated.
     * @param recordingId   of the entry.
     * @param logPosition   of the snapshot.
     * @param serviceId     that took the snapshot.
     */
    @LoggerMethod(eventCode = "SNAPSHOT_ENTRY_INVALIDATION", lengthMethod = "snapshotEntryInvalidationLength")
    default void logSnapshotEntryInvalidation(
        final int memberId,
        final int entryIndex,
        final long recordingId,
        final long logPosition,
        final int serviceId)
    {
    }
}
