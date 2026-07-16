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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.agent.ClusterEventCode;
import io.aeron.agent.ClusterEventLogger;
import io.aeron.cluster.codecs.CloseReason;

import java.util.concurrent.TimeUnit;

import static io.aeron.agent.ClusterModuleLogger.isEnabled;

class ClusterLog
{
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

    static <E extends Enum<E>> void logElectionStateChange(
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

    static void logOnNewLeadershipTerm(
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

    static <E extends Enum<E>> void logStateChange(
        final int memberId, final E oldState, final E newState, final String reason)
    {
        if (!LOG_STATE_CHANGE_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logStateChange(ClusterEventCode.STATE_CHANGE, memberId, oldState, newState, reason);
    }

    static <E extends Enum<E>> void logRoleChange(final int memberId, final E oldRole, final E newRole)
    {
        if (!LOG_ROLE_CHANGE_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logStateChange(ClusterEventCode.ROLE_CHANGE, memberId, oldRole, newRole, "");
    }

    static void logOnCanvassPosition(
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

    static void logOnRequestVote(
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

    static void logOnVote(
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

    static void logOnCatchupPosition(
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

    static void logOnStopCatchup(final int memberId, final long leadershipTermId, final int followerMemberId)
    {
        if (!LOG_STOP_CATCHUP_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logOnStopCatchup(memberId, leadershipTermId, followerMemberId);
    }

    static <E extends Enum<E>> void logOnTruncateLogEntry(
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

    static void logOnReplayNewLeadershipTermEvent(
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

    static void logOnAppendPosition(
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

    static void logOnCommitPosition(
        final int memberId, final long leadershipTermId, final long logPosition, final int leaderMemberId)
    {
        if (!LOG_COMMIT_POSITION_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logOnCommitPosition(memberId, leadershipTermId, logPosition, leaderMemberId);
    }

    static void logAppendSessionClose(
        final int memberId,
        final long id,
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
            memberId, id, closeReason, leadershipTermId, timestamp, timeUnit);
    }

    static void logAppendSessionOpen(
        final int memberId,
        final long id,
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
            memberId, id, leadershipTermId, logPosition, timestamp, timeUnit);
    }

    static <E extends Enum<E>> void logClusterBackupStateChange(final E oldState, final E newState, final long nowMs)
    {
        if (!LOG_CLUSTER_BACKUP_STATE_CHANGE_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logStateChange(
            ClusterEventCode.CLUSTER_BACKUP_STATE_CHANGE, Aeron.NULL_VALUE, oldState, newState, "");
    }

    static void logTerminationPosition(final int memberId, final long leadershipTermId, final long logPosition)
    {
        if (!LOG_TERMINATION_POSITION_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logTerminationPosition(memberId, leadershipTermId, logPosition);
    }

    static void logTerminationAck(
        final int memberId, final long leadershipTermId, final long logPosition, final int senderMemberId)
    {
        if (!LOG_TERMINATION_ACK_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logTerminationAck(memberId, leadershipTermId, logPosition, senderMemberId);
    }

    static void logServiceAck(
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

    static void logReplicationEnded(
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

    static void logStandbySnapshotNotification(
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

    static void logNewElection(
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

    static <A extends Enum<A>, S extends Enum<S>> void logClusterSessionStateChange(
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

    static void logSnapshotEntryInvalidation(
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
}
