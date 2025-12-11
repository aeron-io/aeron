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

import io.aeron.protocol.HeaderFlyweight;
import org.agrona.MutableDirectBuffer;
import org.agrona.SemanticVersion;

import static io.aeron.agent.ClusterEventCode.ELECTION_STATE_CHANGE;
import static io.aeron.agent.ClusterEventCode.NEW_LEADERSHIP_TERM;
import static io.aeron.agent.CommonEventDissector.dissectLogHeader;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.*;

final class ClusterEventDissector
{
    static final String CONTEXT = "CLUSTER";

    private ClusterEventDissector()
    {
    }

    static void dissectNewLeadershipTerm(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, NEW_LEADERSHIP_TERM, buffer, absoluteOffset, builder);

        final long logLeadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long nextLeadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long nextTermBaseLogPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long nextLogPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long termBaseLogPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long commitPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long leaderRecordingId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long timestamp = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        final int leaderId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        final int logSessionId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        final int appVersion = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        final boolean isStartup = 1 == buffer.getByte(absoluteOffset);

        builder.append(": memberId=").append(memberId)
            .append(" logLeadershipTermId=").append(logLeadershipTermId)
            .append(" nextLeadershipTermId=").append(nextLeadershipTermId)
            .append(" nextTermBaseLogPosition=").append(nextTermBaseLogPosition)
            .append(" nextLogPosition=").append(nextLogPosition)
            .append(" leadershipTermId=").append(leadershipTermId)
            .append(" termBaseLogPosition=").append(termBaseLogPosition)
            .append(" logPosition=").append(logPosition)
            .append(" commitPosition=").append(commitPosition)
            .append(" leaderRecordingId=").append(leaderRecordingId)
            .append(" timestamp=").append(timestamp)
            .append(" leaderId=").append(leaderId)
            .append(" logSessionId=").append(logSessionId)
            .append(" appVersion=");
        appendSemanticVersion(appVersion, builder);
        builder.append(" isStartup=").append(isStartup);
    }

    private static void appendSemanticVersion(final int version, final StringBuilder builder)
    {
        builder.append(SemanticVersion.major(version))
            .append('.')
            .append(SemanticVersion.minor(version))
            .append('.')
            .append(SemanticVersion.patch(version));
    }

    static void dissectStateChange(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId);
        builder.append(' ');
        buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
    }

    static void dissectNoOp(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
    }

    static void dissectElectionStateChange(
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, ELECTION_STATE_CHANGE, buffer, absoluteOffset, builder);

        final long candidateTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long logLeadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long appendPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long catchupPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final int leaderId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId).append(' ');
        absoluteOffset += SIZE_OF_INT + buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);

        builder.append(" leaderId=").append(leaderId);
        builder.append(" candidateTermId=").append(candidateTermId);
        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" logLeadershipTermId=").append(logLeadershipTermId);
        builder.append(" appendPosition=").append(appendPosition);
        builder.append(" catchupPosition=").append(catchupPosition);
        builder.append(" reason=");
        buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
    }

    static void dissectCanvassPosition(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long logLeadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int followerMemberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final int protocolVersion = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);

        builder.append(": memberId=").append(memberId);
        builder.append(" logLeadershipTermId=").append(logLeadershipTermId);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" followerMemberId=").append(followerMemberId);
        builder.append(" protocolVersion=");
        appendSemanticVersion(protocolVersion, builder);
    }

    static void dissectRequestVote(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long logLeadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long candidateTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int candidateId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final int protocolVersion = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);

        builder.append(": memberId=").append(memberId);
        builder.append(" logLeadershipTermId=").append(logLeadershipTermId);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" candidateTermId=").append(candidateTermId);
        builder.append(" candidateId=").append(candidateId);
        builder.append(" protocolVersion=");
        appendSemanticVersion(protocolVersion, builder);
    }

    static void dissectCatchupPosition(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int followerMemberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final int catchupEndpointLength = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId);
        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" followerMemberId=").append(followerMemberId);
        builder.append(" catchupEndpoint=");
        buffer.getStringWithoutLengthAscii(absoluteOffset, catchupEndpointLength, builder);
    }

    static void dissectStopCatchup(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int followerMemberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);

        builder.append(": memberId=").append(memberId);
        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" followerMemberId=").append(followerMemberId);
    }

    static void dissectTruncateLogEntry(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long logLeadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long candidateTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long commitPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long appendPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long oldPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long newPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId);
        builder.append(" state=");
        buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
        builder.append(" logLeadershipTermId=").append(logLeadershipTermId);
        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" candidateTermId=").append(candidateTermId);
        builder.append(" commitPosition=").append(commitPosition);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" appendPosition=").append(appendPosition);
        builder.append(" oldPosition=").append(oldPosition);
        builder.append(" newPosition=").append(newPosition);
    }

    static void dissectReplayNewLeadershipTerm(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long timestamp = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long termBaseLogPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        final int appVersion = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        final boolean isInElection = 0 != buffer.getByte(absoluteOffset);
        absoluteOffset += SIZE_OF_BYTE;

        final int timeUnitLength = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId);
        builder.append(" isInElection=").append(isInElection);
        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" termBaseLogPosition=").append(termBaseLogPosition);
        builder.append(" appVersion=").append(appVersion);
        builder.append(" timestamp=").append(timestamp);
        builder.append(" timeUnit=");

        buffer.getStringWithoutLengthAscii(absoluteOffset, timeUnitLength, builder);
    }

    static void dissectAppendPosition(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int followerMemberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final short flags = (short)(buffer.getByte(absoluteOffset) & 0xFF);

        builder.append(": memberId=").append(memberId);
        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" followerMemberId=").append(followerMemberId);
        builder.append(" flags=0b");
        HeaderFlyweight.appendFlagsAsChars(flags, builder);
    }

    static void dissectCommitPosition(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int leaderId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);

        builder.append(": memberId=").append(memberId);
        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" leaderId=").append(leaderId);
    }

    static void dissectAppendSessionClose(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long sessionId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long timestamp = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId);
        builder.append(" sessionId=").append(sessionId);
        builder.append(" closeReason=");
        absoluteOffset += SIZE_OF_INT + buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);

        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" timestamp=").append(timestamp);
        builder.append(" timeUnit=");
        buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
    }

    static void dissectAppendSessionOpen(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long sessionId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long timestamp = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId);
        builder.append(" sessionId=").append(sessionId);

        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" timestamp=").append(timestamp);
        builder.append(" timeUnit=");
        buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
    }

    static void dissectTerminationPosition(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long logLeadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long position = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId);
        builder.append(" logLeadershipTermId=").append(logLeadershipTermId);
        builder.append(" logPosition=").append(position);
    }

    static void dissectTerminationAck(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long logLeadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long position = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final int senderMemberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId);
        builder.append(" logLeadershipTermId=").append(logLeadershipTermId);
        builder.append(" logPosition=").append(position);
        builder.append(" senderMemberId=").append(senderMemberId);

    }

    static void dissectServiceAck(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long timestamp = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long ackId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long relevantId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final long serviceId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" timestamp=").append(timestamp);
        builder.append(" timeUnit=");
        buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
        builder.append(" ackId=").append(ackId);
        builder.append(" relevantId=").append(relevantId);
        builder.append(" serviceId=").append(serviceId);
    }

    static void dissectReplicationEnded(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long srcRecordingId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long dstRecordingId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long position = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final boolean hasSynced = 1 == buffer.getByte(absoluteOffset);
        absoluteOffset += SIZE_OF_BYTE;

        builder.append(": memberId=").append(memberId);
        builder.append(" purpose=");
        absoluteOffset += buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(" channel=");
        buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);

        builder.append(" srcRecordingId=").append(srcRecordingId);
        builder.append(" dstRecordingId=").append(dstRecordingId);
        builder.append(" position=").append(position);
        builder.append(" hasSynced=").append(hasSynced);
    }

    static void dissectStandbySnapshotNotification(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long recordingId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long termBaseLeadershipPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long timestamp = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final int serviceId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId);
        builder.append(" recordingId=").append(recordingId);
        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" termBaseLeadershipPosition=").append(termBaseLeadershipPosition);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" timestamp=").append(timestamp);
        builder.append(" timeUnit=");
        absoluteOffset += buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        builder.append(" serviceId=").append(serviceId);
        builder.append(" archiveEndpoint=");
        absoluteOffset += buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
    }

    static void dissectNewElection(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long appendPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId);
        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" appendPosition=").append(appendPosition);
        builder.append(" reason=");
        buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
    }

    static void dissectClusterSessionStateChange(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long sessionId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId);
        builder.append(" sessionId=").append(sessionId);

        builder.append(" action=");
        absoluteOffset += buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(" ");
        absoluteOffset += buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(" reason=\"");
        buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
        builder.append("\"");
    }
}
