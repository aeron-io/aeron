/*
 * Copyright 2014-2026 Real Logic Limited.
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

import io.aeron.Aeron;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.logging.EventConfiguration;
import org.agrona.collections.Object2ObjectHashMap;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Logging entry points for the cluster.
 */
public final class ClusterTracing
{
    private static final Object2ObjectHashMap<String, EnumSet<ClusterEventCode>> SPECIAL_EVENTS =
        new Object2ObjectHashMap<>();

    private static final Set<ClusterEventCode> ENABLED_EVENT_CODES;

    static
    {
        SPECIAL_EVENTS.put("all", EnumSet.allOf(ClusterEventCode.class));

        final String enabledEventCodes = System.getProperty("aeron.event.cluster.log");
        final String disabledEventCodes = System.getProperty("aeron.event.cluster.log.disable");

        final EnumSet<ClusterEventCode> disabledEventCodeSet = EventConfiguration.parseEventCodes(
            ClusterEventCode.class,
            disabledEventCodes,
            SPECIAL_EVENTS,
            ClusterEventCode::get,
            ClusterEventCode::valueOf);

        final EnumSet<ClusterEventCode> enabledEventCodeSet = EventConfiguration.parseEventCodes(
            ClusterEventCode.class,
            enabledEventCodes,
            SPECIAL_EVENTS,
            ClusterEventCode::get,
            ClusterEventCode::valueOf);

        enabledEventCodeSet.removeAll(disabledEventCodeSet);

        ENABLED_EVENT_CODES = Collections.unmodifiableSet(enabledEventCodeSet);
    }

    static final boolean LOG_ELECTION_STATE_CHANGE_ENABLED = isEnabled(ClusterEventCode.ELECTION_STATE_CHANGE);
    static final boolean LOG_NEW_LEADERSHIP_TERM_ENABLED = isEnabled(ClusterEventCode.NEW_LEADERSHIP_TERM);
    static final boolean LOG_STATE_CHANGE_ENABLED = isEnabled(ClusterEventCode.STATE_CHANGE);
    static final boolean LOG_ROLE_CHANGE_ENABLED = isEnabled(ClusterEventCode.ROLE_CHANGE);
    static final boolean LOG_CANVASS_POSITION_ENABLED = isEnabled(ClusterEventCode.CANVASS_POSITION);
    static final boolean LOG_REQUEST_VOTE_ENABLED = isEnabled(ClusterEventCode.REQUEST_VOTE);
    static final boolean LOG_CATCHUP_POSITION_ENABLED = isEnabled(ClusterEventCode.CATCHUP_POSITION);
    static final boolean LOG_STOP_CATCHUP_ENABLED = isEnabled(ClusterEventCode.STOP_CATCHUP);
    static final boolean LOG_TRUNCATE_LOG_ENTRY_ENABLED = isEnabled(ClusterEventCode.TRUNCATE_LOG_ENTRY);
    static final boolean LOG_REPLAY_NEW_LEADERSHIP_TERM_ENABLED =
        isEnabled(ClusterEventCode.REPLAY_NEW_LEADERSHIP_TERM);
    static final boolean LOG_APPEND_POSITION_ENABLED = isEnabled(ClusterEventCode.APPEND_POSITION);
    static final boolean LOG_COMMIT_POSITION_ENABLED = isEnabled(ClusterEventCode.COMMIT_POSITION);
    static final boolean LOG_APPEND_SESSION_CLOSE_ENABLED = isEnabled(ClusterEventCode.APPEND_SESSION_CLOSE);
    static final boolean LOG_CLUSTER_BACKUP_STATE_CHANGE_ENABLED =
        isEnabled(ClusterEventCode.CLUSTER_BACKUP_STATE_CHANGE);
    static final boolean LOG_TERMINATION_POSITION_ENABLED = isEnabled(ClusterEventCode.TERMINATION_POSITION);
    static final boolean LOG_TERMINATION_ACK_ENABLED = isEnabled(ClusterEventCode.TERMINATION_ACK);
    static final boolean LOG_SERVICE_ACK_ENABLED = isEnabled(ClusterEventCode.SERVICE_ACK);
    static final boolean LOG_REPLICATION_ENDED_ENABLED = isEnabled(ClusterEventCode.REPLICATION_ENDED);
    static final boolean LOG_STANDBY_SNAPSHOT_NOTIFICATION_ENABLED =
        isEnabled(ClusterEventCode.STANDBY_SNAPSHOT_NOTIFICATION);
    static final boolean LOG_NEW_ELECTION_ENABLED = isEnabled(ClusterEventCode.NEW_ELECTION);
    static final boolean LOG_APPEND_SESSION_OPEN_ENABLED = isEnabled(ClusterEventCode.APPEND_SESSION_OPEN);
    static final boolean LOG_CLUSTER_SESSION_STATE_CHANGE_ENABLED =
        isEnabled(ClusterEventCode.CLUSTER_SESSION_STATE_CHANGE);
    static final boolean LOG_VOTE_ENABLED = isEnabled(ClusterEventCode.VOTE);
    static final boolean LOG_SNAPSHOT_ENTRY_INVALIDATION_ENABLED =
        isEnabled(ClusterEventCode.SNAPSHOT_ENTRY_INVALIDATION);
    static final boolean LOG_START = !ENABLED_EVENT_CODES.isEmpty();

    private ClusterTracing()
    {
    }

    /**
     * Determine if a given event code is configured/enabled for logging.
     *
     * @param clusterEventCode to check for enablement.
     * @return <code>true</code> if enabled, <code>false</code> otherwise.
     */
    private static boolean isEnabled(final ClusterEventCode clusterEventCode)
    {
        return ENABLED_EVENT_CODES.contains(clusterEventCode);
    }

    /**
     * Log an election state change event for a cluster node if enabled.
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
    public static <E extends Enum<E>> void traceElectionStateChange(
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
        if (!LOG_ELECTION_STATE_CHANGE_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logElectionStateChange(
            memberId,
            oldState,
            newState,
            leaderId,
            candidateTermId,
            leadershipTermId,
            logPosition,
            logLeadershipTermId,
            appendPosition,
            catchupPosition,
            reason);
    }

    /**
     * Log a new leadership term event if enabled.
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
    public static void traceOnNewLeadershipTerm(
        final int memberId,
        final long logLeadershipTermId,
        final long nextLeadershipTermId,
        final long nextTermBaseLogPosition,
        final long nextLogPosition,
        final long leadershipTermId,
        final long termBaseLogPosition, //----
        // termination
        final long logPosition,
        final long commitPosition,
        final long leaderRecordingId,
        final long timestamp,
        final int leaderId,
        final int logSessionId,
        final int appVersion,
        final boolean isStartup)
    {
        if (!LOG_NEW_LEADERSHIP_TERM_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logOnNewLeadershipTerm(
            memberId,
            logLeadershipTermId,
            nextLeadershipTermId,
            nextTermBaseLogPosition,
            nextLogPosition,
            leadershipTermId,
            termBaseLogPosition,
            logPosition,
            commitPosition,
            leaderRecordingId,
            timestamp,
            leaderId,
            logSessionId,
            appVersion,
            isStartup);
    }

    /**
     * Log a state change if enabled.
     *
     * @param memberId of the cluster node.
     * @param oldState state prior to the change.
     * @param newState state after the change.
     * @param reason   for the state change.
     * @param <E>      type of state.
     */
    public static <E extends Enum<E>> void traceStateChange(
        final int memberId, final E oldState, final E newState, final String reason)
    {
        if (!LOG_STATE_CHANGE_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logStateChange(memberId, oldState, newState, reason);
    }

    /**
     * Log a role change if enabled.
     *
     * @param memberId  of the cluster node.
     * @param oldRole   role prior to the change.
     * @param newRole   role after the change.
     * @param <E>       type of the role.
     */
    public static <E extends Enum<E>> void traceRoleChange(final int memberId, final E oldRole, final E newRole)
    {
        if (!LOG_ROLE_CHANGE_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logRoleChange(memberId, oldRole, newRole, "");
    }

    /**
     * Log a canvass position event received by the cluster node if enabled.
     *
     * @param memberId            member who sent the event.
     * @param logLeadershipTermId leadershipTermId reached by the member for it recorded log.
     * @param logPosition         position the member has durably recorded.
     * @param leadershipTermId    the most current leadershipTermId a member has seen.
     * @param followerMemberId    follower node id.
     * @param protocolVersion     of the consensus module.
     */
    public static void traceOnCanvassPosition(
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition,
        final long leadershipTermId,
        final int followerMemberId,
        final int protocolVersion)
    {
        if (!LOG_CANVASS_POSITION_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logOnCanvassPosition(
            memberId, logLeadershipTermId, logPosition, leadershipTermId, followerMemberId, protocolVersion);
    }

    /**
     * Log a request to vote from a cluster candidate for new leadership if enabled.
     *
     * @param memberId            of the current cluster node.
     * @param logLeadershipTermId leadershipTermId processes from the log by the candidate.
     * @param logPosition         position reached in the log for the latest leadership term.
     * @param candidateTermId     the term id as the candidate sees it for the election.
     * @param candidateId         id of the candidate node.
     * @param protocolVersion     from the request.
     */
    public static void traceOnRequestVote(
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition,
        final long candidateTermId,
        final int candidateId,
        final int protocolVersion)
    {
        if (!LOG_REQUEST_VOTE_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logOnRequestVote(
            memberId, logLeadershipTermId, logPosition, candidateTermId, candidateId, protocolVersion);
    }

    /**
     * Log a vote response from a cluster candidate for new leadership if enabled.
     *
     * @param memberId            of the current cluster node.
     * @param logLeadershipTermId leadershipTermId processes from the log by the candidate.
     * @param logPosition         position reached in the log for the latest leadership term.
     * @param candidateTermId     the term id as the candidate sees it for the election.
     * @param candidateId         id of the candidate node.
     * @param voterId             id of the follower node that voted.
     * @param vote                expressed by the follower node.
     */
    public static void traceOnVote(
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition,
        final long candidateTermId,
        final int candidateId,
        final int voterId,
        final boolean vote)
    {
        if (!LOG_VOTE_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logOnVote(
            memberId, logLeadershipTermId, logPosition, candidateTermId, candidateId, voterId, vote);
    }

    /**
     * Log the catchup position message if enabled.
     *
     * @param memberId         of the current cluster node.
     * @param leadershipTermId leadership term to catch up on
     * @param logPosition      position to catchup from
     * @param followerMemberId the id of the follower that is catching up
     * @param catchupEndpoint  the endpoint to send catchup messages
     */
    public static void traceOnCatchupPosition(
        final int memberId,
        final long leadershipTermId,
        final long logPosition,
        final int followerMemberId,
        final String catchupEndpoint)
    {
        if (!LOG_CATCHUP_POSITION_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logOnCatchupPosition(
            memberId, leadershipTermId, logPosition, followerMemberId, catchupEndpoint);
    }

    /**
     * Log the stop catchup message if enabled.
     *
     * @param memberId         of the current cluster node.
     * @param leadershipTermId current leadershipTermId.
     * @param followerMemberId id of follower currently catching up.
     */
    public static void traceOnStopCatchup(final int memberId, final long leadershipTermId, final int followerMemberId)
    {
        if (!LOG_STOP_CATCHUP_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logOnStopCatchup(memberId, leadershipTermId, followerMemberId);
    }

    /**
     * Log an event when a log entry is being truncated if enabled.
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
    public static <E extends Enum<E>> void traceOnTruncateLogEntry(
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
        if (!LOG_TRUNCATE_LOG_ENTRY_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logOnTruncateLogEntry(
            memberId,
            state,
            logLeadershipTermId,
            leadershipTermId,
            candidateTermId,
            commitPosition,
            logPosition,
            appendPosition,
            oldPosition,
            newPosition);
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
    public static void traceOnReplayNewLeadershipTermEvent(
        final int memberId,
        final boolean isInElection,
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        if (!LOG_REPLAY_NEW_LEADERSHIP_TERM_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logOnReplayNewLeadershipTermEvent(
            memberId,
            isInElection,
            leadershipTermId,
            logPosition,
            timestamp,
            termBaseLogPosition,
            timeUnit,
            appVersion);
    }

    /**
     * The Append position received by the leader from a follower if enabled.
     *
     * @param memberId         of the current cluster node.
     * @param leadershipTermId the current leadership term id.
     * @param logPosition      the current position in the log.
     * @param followerMemberId follower member sending the Append position.
     * @param flags            applied to append position by follower.
     */
    public static void traceOnAppendPosition(
        final int memberId,
        final long leadershipTermId,
        final long logPosition,
        final int followerMemberId,
        final short flags)
    {
        if (!LOG_APPEND_POSITION_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logOnAppendPosition(
            memberId, leadershipTermId, logPosition, followerMemberId, flags);
    }

    /**
     * The commit position received by the follower form the leader if enabled.
     *
     * @param memberId         of the node receiving commit position message.
     * @param leadershipTermId the current leadership term id.
     * @param logPosition      the current position in the log.
     * @param leaderMemberId   leader member sending the commit position.
     */
    public static void traceOnCommitPosition(
        final int memberId, final long leadershipTermId, final long logPosition, final int leaderMemberId)
    {
        if (!LOG_COMMIT_POSITION_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logOnCommitPosition(memberId, leadershipTermId, logPosition, leaderMemberId);
    }

    /**
     * Log the appending of a session close event to the log if enabled.
     *
     * @param memberId         member (leader) publishing the event.
     * @param sessionId        session id of the session be closed.
     * @param closeReason      reason to close the session.
     * @param leadershipTermId current leadership term id.
     * @param timestamp        the current timestamp.
     * @param timeUnit         units for the timestamp.
     */
    public static void traceAppendSessionClose(
        final int memberId,
        final long sessionId,
        final CloseReason closeReason,
        final long leadershipTermId,
        final long timestamp,
        final TimeUnit timeUnit)
    {
        if (!LOG_APPEND_SESSION_CLOSE_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logAppendSessionClose(
            memberId, sessionId, closeReason, leadershipTermId, timestamp, timeUnit);
    }

    /**
     * Log the appending of a session open event to the log if enabled.
     *
     * @param memberId         member (leader) publishing the event.
     * @param sessionId        session id of the session be closed.
     * @param leadershipTermId current leadership term id.
     * @param logPosition      when session was opened.
     * @param timestamp        the current timestamp.
     * @param timeUnit         units for the timestamp.
     */
    public static void traceAppendSessionOpen(
        final int memberId,
        final long sessionId,
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final TimeUnit timeUnit)
    {
        if (!LOG_APPEND_SESSION_OPEN_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logAppendSessionOpen(
            memberId, sessionId, leadershipTermId, logPosition, timestamp, timeUnit);
    }

    /**
     * Log a state change event for a cluster backup node if enabled.
     *
     * @param <E>       type representing the state change.
     * @param oldState  before the change.
     * @param newState  after the change.
     */
    public static <E extends Enum<E>> void traceClusterBackupStateChange(
        final E oldState,
        final E newState)
    {
        if (!LOG_CLUSTER_BACKUP_STATE_CHANGE_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logClusterBackupStateChange(Aeron.NULL_VALUE, oldState, newState, "");
    }

    /**
     * Log the receiving of a termination position event if enabled.
     *
     * @param memberId            that received the termination position.
     * @param leadershipTermId    leadership term for the supplied position.
     * @param logPosition         position to terminate at.
     */
    public static void traceTerminationPosition(final int memberId, final long leadershipTermId, final long logPosition)
    {
        if (!LOG_TERMINATION_POSITION_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logTerminationPosition(memberId, leadershipTermId, logPosition);
    }

    /**
     * Log the receiving of an acknowledgement to a termination position event if enabled.
     *
     * @param memberId            that received the termination ack.
     * @param leadershipTermId    leadership term for the supplied position.
     * @param logPosition         position to terminate at.
     * @param senderMemberId      member sending the ack.
     */
    public static void traceTerminationAck(
        final int memberId, final long leadershipTermId, final long logPosition, final int senderMemberId)
    {
        if (!LOG_TERMINATION_ACK_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logTerminationAck(memberId, leadershipTermId, logPosition, senderMemberId);
    }

    /**
     * Log an ack received from a cluster service if enabled.
     *
     * @param memberId    memberId receiving the ack.
     * @param logPosition position in the log when the ack was sent.
     * @param timestamp   timestamp when the ack was sent.
     * @param timeUnit    time unit used for the timestamp.
     * @param ackId       id of the ack.
     * @param relevantId  associated id used in the ack, e.g. recordingId for snapshot acks.
     * @param serviceId   the id of the service that sent the ack.
     */
    public static void traceServiceAck(
        final int memberId,
        final long logPosition,
        final long timestamp,
        final TimeUnit timeUnit,
        final long ackId,
        final long relevantId,
        final int serviceId)
    {
        if (!LOG_SERVICE_ACK_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logServiceAck(
            memberId, logPosition, timestamp, timeUnit, ackId, relevantId, serviceId);
    }

    /**
     * Log a replication end event if enabled.
     *
     * @param memberId       memberId running the replication.
     * @param purpose        the reason for the replication.
     * @param channel        the channel used to connect to the source archive.
     * @param srcRecordingId source recording id.
     * @param dstRecordingId destination recording id.
     * @param position       the position where the recording ended.
     * @param hasSynced      was the sync event been received for the replication.
     */
    public static void traceReplicationEnded(
        final int memberId,
        final String purpose,
        final String channel,
        final long srcRecordingId,
        final long dstRecordingId,
        final long position,
        final boolean hasSynced)
    {
        if (!LOG_REPLICATION_ENDED_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logReplicationEnded(
            memberId, purpose, channel, srcRecordingId, dstRecordingId, position, hasSynced);
    }

    /**
     * Log a standby snapshot notification if enabled.
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
    public static void traceStandbySnapshotNotification(
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
        if (!LOG_STANDBY_SNAPSHOT_NOTIFICATION_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logStandbySnapshotNotification(
            memberId,
            recordingId,
            leadershipTermId,
            termBaseLogPosition,
            logPosition,
            timestamp,
            timeUnit,
            serviceId,
            archiveEndpoint);
    }

    /**
     * Log the start of the new election if enabled.
     *
     * @param memberId         memberId which start the election.
     * @param leadershipTermId of the member.
     * @param logPosition      the log position.
     * @param appendPosition   the append position.
     * @param reason           for election to be started.
     */
    public static void traceNewElection(
        final int memberId,
        final long leadershipTermId,
        final long logPosition,
        final long appendPosition,
        final String reason)
    {
        if (!LOG_NEW_ELECTION_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logNewElection(memberId, leadershipTermId, logPosition, appendPosition, reason);
    }

    /**
     * Log a state change event for a cluster session if enabled.
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
    public static <A extends Enum<A>, S extends Enum<S>> void traceClusterSessionStateChange(
        final int memberId,
        final long sessionId,
        final A action,
        final S oldState,
        final S newState,
        final String reason)
    {
        if (!LOG_CLUSTER_SESSION_STATE_CHANGE_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logClusterSessionStateChange(
            memberId, sessionId, action, oldState, newState, reason);
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
    public static void traceSnapshotEntryInvalidation(
        final int memberId,
        final int entryIndex,
        final long recordingId,
        final long logPosition,
        final int serviceId)
    {
        if (!LOG_SNAPSHOT_ENTRY_INVALIDATION_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logSnapshotEntryInvalidation(
            memberId, entryIndex, recordingId, logPosition, serviceId);
    }

    /**
     * Log cluster start.
     *
     * @param version   of the cluster.
     */
    public static void traceStart(final String version)
    {
        if (!LOG_START)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logStart(version);
    }
}
