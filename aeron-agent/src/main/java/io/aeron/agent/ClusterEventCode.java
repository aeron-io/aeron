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
package io.aeron.agent;

import org.agrona.MutableDirectBuffer;

import java.util.Arrays;

import static io.aeron.agent.ClusterEventDissector.dissectElectionStateChange;
import static io.aeron.agent.ClusterEventDissector.dissectNewLeadershipTerm;

/**
 * Events that can be enabled for logging in the cluster module.
 */
public enum ClusterEventCode implements EventCode
{
    /**
     * State change events within a cluster election.
     */
    ELECTION_STATE_CHANGE(1,
        (eventCode, buffer, offset, builder) -> dissectElectionStateChange(buffer, offset, builder)),

    /**
     * A new term of leadership is to begin for an elected cluster member.
     */
    NEW_LEADERSHIP_TERM(2, (eventCode, buffer, offset, builder) -> dissectNewLeadershipTerm(buffer, offset, builder)),

    /**
     * State change in the cluster node consensus module.
     */
    STATE_CHANGE(3, ClusterEventDissector::dissectStateChange),

    /**
     * Role change for the cluster member.
     */
    ROLE_CHANGE(4, ClusterEventDissector::dissectStateChange),

    /**
     * A Canvass position event to notify the state of a member's log before nomination.
     */
    CANVASS_POSITION(5, ClusterEventDissector::dissectCanvassPosition),

    /**
     * A vote request for new leadership.
     */
    REQUEST_VOTE(6, ClusterEventDissector::dissectRequestVote),

    /**
     * Notification of a follower's catchup position.
     */
    CATCHUP_POSITION(7, ClusterEventDissector::dissectCatchupPosition),

    /**
     * A request to stop follower catchup.
     */
    STOP_CATCHUP(8, ClusterEventDissector::dissectStopCatchup),

    /**
     * Event when a RecordingLog entry is being truncated.
     */
    TRUNCATE_LOG_ENTRY(9, ClusterEventDissector::dissectTruncateLogEntry),

    /**
     * Event when a new leadership term is replayed.
     */
    REPLAY_NEW_LEADERSHIP_TERM(10, ClusterEventDissector::dissectReplayNewLeadershipTerm),

    /**
     * Event when an append position is received.
     */
    APPEND_POSITION(11, ClusterEventDissector::dissectAppendPosition),

    /**
     * Event when a commit position is received.
     */
    COMMIT_POSITION(12, ClusterEventDissector::dissectCommitPosition),

    /**
     * Event when a session is closed.
     */
    APPEND_SESSION_CLOSE(14, ClusterEventDissector::dissectAppendSessionClose),

    /**
     * Event when the DynamicJoin changes state (Unused).
     */
    DYNAMIC_JOIN_STATE_CHANGE_UNUSED(15, ClusterEventDissector::dissectNoOp),

    /**
     * Event when the ClusterBackup changes state.
     */
    CLUSTER_BACKUP_STATE_CHANGE(16, ClusterEventDissector::dissectStateChange),

    /**
     * Event when a node is instructed to terminate.
     */
    TERMINATION_POSITION(17, ClusterEventDissector::dissectTerminationPosition),

    /**
     * Event when a node acks the termination request.
     */
    TERMINATION_ACK(18, ClusterEventDissector::dissectTerminationAck),

    /**
     * Event when a nodes consensus module receives an ack from a service.
     */
    SERVICE_ACK(19, ClusterEventDissector::dissectServiceAck),

    /**
     * Event when a replication has ended.
     */
    REPLICATION_ENDED(20, ClusterEventDissector::dissectReplicationEnded),

    /**
     * Event when a standby snapshot notification has been received by a consensus module.
     */
    STANDBY_SNAPSHOT_NOTIFICATION(21, ClusterEventDissector::dissectStandbySnapshotNotification),

    /**
     * Event when a new Election is started.
     *
     * @since 1.44.0
     */
    NEW_ELECTION(22, ClusterEventDissector::dissectNewElection),

    /**
     * Event when a session is opened.
     *
     * @since 1.49.0
     */
    APPEND_SESSION_OPEN(23, ClusterEventDissector::dissectAppendSessionOpen),

    /**
     * Event for {@code ClusterSession} state changes.
     *
     * @since 1.49.0
     */
    CLUSTER_SESSION_STATE_CHANGE(24, ClusterEventDissector::dissectClusterSessionStateChange),

    /**
     * An actual vote for new leadership, i.e. response to the {@link #REQUEST_VOTE}.
     *
     * @since 1.50.0
     */
    VOTE(25, ClusterEventDissector::dissectVote),

    /**
     * Invalidation of an entry in the recording log for snapshot. Occurs when the consensus module runs the recording
     * log validator and finds a snapshot that has been removed from the archive.
     */
    SNAPSHOT_ENTRY_INVALIDATION(26, ClusterEventDissector::dissectSnapshotEntryInvalidation);

    static final int EVENT_CODE_TYPE = EventCodeType.CLUSTER.getTypeCode();
    private static final ClusterEventCode[] EVENT_CODE_BY_ID;

    private final int id;
    private final DissectFunction<ClusterEventCode> dissector;

    static
    {
        final ClusterEventCode[] codes = ClusterEventCode.values();
        final int maxId = Arrays.stream(codes).mapToInt(ClusterEventCode::id).max().orElse(0);
        EVENT_CODE_BY_ID = new ClusterEventCode[maxId + 1];

        for (final ClusterEventCode code : codes)
        {
            final int id = code.id();
            if (null != EVENT_CODE_BY_ID[id])
            {
                throw new IllegalArgumentException("id already in use: " + id);
            }

            EVENT_CODE_BY_ID[id] = code;
        }
    }

    ClusterEventCode(final int id, final DissectFunction<ClusterEventCode> dissector)
    {
        this.id = id;
        this.dissector = dissector;
    }

    static ClusterEventCode get(final int id)
    {
        if (id < 0 || id >= EVENT_CODE_BY_ID.length)
        {
            throw new IllegalArgumentException("no ClusterEventCode for id: " + id);
        }

        final ClusterEventCode code = EVENT_CODE_BY_ID[id];
        if (null == code)
        {
            throw new IllegalArgumentException("no ClusterEventCode for id: " + id);
        }

        return code;
    }

    /**
     * {@inheritDoc}
     */
    public int id()
    {
        return id;
    }

    /**
     * Get {@link ClusterEventCode#id()} from {@link #id()}.
     *
     * @return get {@link ClusterEventCode#id()} from {@link #id()}.
     */
    public int toEventCodeId()
    {
        return EVENT_CODE_TYPE << 16 | (id & 0xFFFF);
    }

    /**
     * Get {@link ClusterEventCode} from its event code id.
     *
     * @param eventCodeId to convert.
     * @return {@link ClusterEventCode} from its event code id.
     */
    public static ClusterEventCode fromEventCodeId(final int eventCodeId)
    {
        return get(eventCodeId - (EVENT_CODE_TYPE << 16));
    }

    /**
     * Decode an event serialised in a buffer to a provided {@link StringBuilder}.
     *
     * @param buffer  containing the encoded event.
     * @param offset  offset at which the event begins.
     * @param builder to write the decoded event to.
     */
    public void decode(final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        dissector.dissect(this, buffer, offset, builder);
    }
}
