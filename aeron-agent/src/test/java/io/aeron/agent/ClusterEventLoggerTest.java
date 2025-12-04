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

import io.aeron.cluster.codecs.CloseReason;
import io.aeron.test.Tests;
import org.agrona.SemanticVersion;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static io.aeron.agent.AgentTests.verifyLogHeader;
import static io.aeron.agent.ClusterEventCode.APPEND_POSITION;
import static io.aeron.agent.ClusterEventCode.APPEND_SESSION_CLOSE;
import static io.aeron.agent.ClusterEventCode.APPEND_SESSION_OPEN;
import static io.aeron.agent.ClusterEventCode.CANVASS_POSITION;
import static io.aeron.agent.ClusterEventCode.CATCHUP_POSITION;
import static io.aeron.agent.ClusterEventCode.CLUSTER_SESSION_STATE_CHANGE;
import static io.aeron.agent.ClusterEventCode.COMMIT_POSITION;
import static io.aeron.agent.ClusterEventCode.ELECTION_STATE_CHANGE;
import static io.aeron.agent.ClusterEventCode.NEW_ELECTION;
import static io.aeron.agent.ClusterEventCode.NEW_LEADERSHIP_TERM;
import static io.aeron.agent.ClusterEventCode.REPLAY_NEW_LEADERSHIP_TERM;
import static io.aeron.agent.ClusterEventCode.REPLICATION_ENDED;
import static io.aeron.agent.ClusterEventCode.REQUEST_VOTE;
import static io.aeron.agent.ClusterEventCode.SERVICE_ACK;
import static io.aeron.agent.ClusterEventCode.STANDBY_SNAPSHOT_NOTIFICATION;
import static io.aeron.agent.ClusterEventCode.STATE_CHANGE;
import static io.aeron.agent.ClusterEventCode.STOP_CATCHUP;
import static io.aeron.agent.ClusterEventCode.TERMINATION_ACK;
import static io.aeron.agent.ClusterEventCode.TERMINATION_POSITION;
import static io.aeron.agent.ClusterEventCode.TRUNCATE_LOG_ENTRY;
import static io.aeron.agent.ClusterEventEncoder.MAX_REASON_LENGTH;
import static io.aeron.agent.ClusterEventEncoder.canvassPositionLength;
import static io.aeron.agent.ClusterEventEncoder.clusterSessionStateChangeLength;
import static io.aeron.agent.ClusterEventEncoder.electionStateChangeLength;
import static io.aeron.agent.ClusterEventEncoder.newElectionLength;
import static io.aeron.agent.ClusterEventEncoder.newLeaderShipTermLength;
import static io.aeron.agent.ClusterEventEncoder.replayNewLeadershipTermEventLength;
import static io.aeron.agent.ClusterEventEncoder.replicationEndedLength;
import static io.aeron.agent.ClusterEventEncoder.serviceAckLength;
import static io.aeron.agent.ClusterEventEncoder.standbySnapshotNotificationLength;
import static io.aeron.agent.ClusterEventEncoder.terminationAckLength;
import static io.aeron.agent.ClusterEventEncoder.terminationPositionLength;
import static io.aeron.agent.CommonEventEncoder.LOG_HEADER_LENGTH;
import static io.aeron.agent.CommonEventEncoder.STATE_SEPARATOR;
import static io.aeron.agent.CommonEventEncoder.enumName;
import static io.aeron.agent.EventConfiguration.BUFFER_LENGTH_DEFAULT;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BitUtil.SIZE_OF_BYTE;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.BitUtil.align;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.ALIGNMENT;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.encodedMsgOffset;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TAIL_POSITION_OFFSET;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ClusterEventLoggerTest
{
    private static final int CAPACITY = align(BUFFER_LENGTH_DEFAULT, CACHE_LINE_LENGTH);
    private final UnsafeBuffer logBuffer = new UnsafeBuffer(
        ByteBuffer.allocateDirect(BUFFER_LENGTH_DEFAULT + TRAILER_LENGTH));
    private final ClusterEventLogger logger = new ClusterEventLogger(new ManyToOneRingBuffer(logBuffer));

    @Test
    void logOnNewLeadershipTerm()
    {
        final int offset = align(22, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final long logLeadershipTermId = 434;
        final long nextLeadershipTermId = 2561;
        final long nextTermBaseLogPosition = 2562;
        final long nextLogPosition = 2563;
        final long leadershipTermId = -500;
        final long logPosition = 432;
        final long commitPosition = 290;
        final long timestamp = 2;
        final int memberId = 19;
        final int leaderId = -1;
        final int logSessionId = 3;
        final int appVersion = SemanticVersion.compose(0, 3, 9);
        final int captureLength = newLeaderShipTermLength();
        final boolean isStartup = true;
        final long termBaseLogPosition = 982734;
        final long leaderRecordingId = 76434;

        logger.logOnNewLeadershipTerm(
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

        verifyLogHeader(logBuffer, offset, NEW_LEADERSHIP_TERM.toEventCodeId(), captureLength, captureLength);
        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(logLeadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(nextLeadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(nextTermBaseLogPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(nextLogPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(termBaseLogPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(commitPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(leaderRecordingId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(timestamp, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(leaderId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(logSessionId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(appVersion, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;

        assertEquals(isStartup, 1 == logBuffer.getByte(index));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectNewLeadershipTerm(logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: NEW_LEADERSHIP_TERM " +
            "\\[97/97]: memberId=19 logLeadershipTermId=434 nextLeadershipTermId=2561 " +
            "nextTermBaseLogPosition=2562 nextLogPosition=2563 leadershipTermId=-500 termBaseLogPosition=982734 " +
            "logPosition=432 commitPosition=290 leaderRecordingId=76434 timestamp=2 leaderId=-1 logSessionId=3 " +
            "appVersion=0.3.9 isStartup=true";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logStateChange()
    {
        final int offset = ALIGNMENT * 11;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final TimeUnit from = MINUTES;
        final TimeUnit to = SECONDS;
        final int memberId = 42;
        final String payload = from.name() + STATE_SEPARATOR + to.name();
        final int captureLength = SIZE_OF_INT * 2 + payload.length();

        logger.logStateChange(STATE_CHANGE, memberId, from, to);

        verifyLogHeader(logBuffer, offset, STATE_CHANGE.toEventCodeId(), captureLength, captureLength);
        final int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        assertEquals(payload, logBuffer.getStringAscii(index + SIZE_OF_INT));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectStateChange(
            ClusterEventCode.STATE_CHANGE, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: STATE_CHANGE " +
            "\\[26/26]: memberId=42 MINUTES -> SECONDS";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logElectionStateChange()
    {
        final int offset = ALIGNMENT * 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final ChronoUnit from = ChronoUnit.ERAS;
        final ChronoUnit to = null;
        final int memberId = 18;
        final int leaderId = -1;
        final long candidateTermId = 29L;
        final long leadershipTermId = 0L;
        final long logPosition = 100L;
        final long logLeadershipTermId = -9L;
        final long appendPosition = 16 * 1024L;
        final long catchupPosition = 8192L;
        final String reason = Tests.generateStringWithSuffix("reason-", "x", MAX_REASON_LENGTH * 5);
        final int length = electionStateChangeLength(from, to, reason);

        logger.logElectionStateChange(
            memberId,
            from,
            to,
            leaderId,
            candidateTermId,
            leadershipTermId,
            logPosition,
            logLeadershipTermId,
            appendPosition,
            catchupPosition,
            reason);

        verifyLogHeader(logBuffer, offset, ELECTION_STATE_CHANGE.toEventCodeId(), length, length);
        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(candidateTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logLeadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(appendPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(catchupPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(leaderId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        final String encodedStateTransition = from.name() + STATE_SEPARATOR + "null";
        assertEquals(encodedStateTransition, logBuffer.getStringAscii(index));
        index += SIZE_OF_INT + encodedStateTransition.length();
        final String trailingReason = reason.substring(0, MAX_REASON_LENGTH - 3) + "...";
        assertEquals(trailingReason, logBuffer.getStringAscii(index));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectElectionStateChange(logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: ELECTION_STATE_CHANGE " +
            "\\[376/376]: memberId=18 ERAS -> null leaderId=-1 candidateTermId=29 leadershipTermId=0 " +
            "logPosition=100 logLeadershipTermId=-9 appendPosition=16384 catchupPosition=8192 reason=" + trailingReason;

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logCatchupPosition()
    {
        final int offset = ALIGNMENT * 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final long leadershipTermId = 1233L;
        final long logPosition = 100L;
        final int followerMemberId = 18;
        final int memberId = -901;
        final String catchupEndpoint = "aeron:udp?endpoint=localhost:9090";

        logger.logOnCatchupPosition(memberId, leadershipTermId, logPosition, followerMemberId, catchupEndpoint);

        final int length = 2 * SIZE_OF_LONG + 2 * SIZE_OF_INT + SIZE_OF_INT + catchupEndpoint.length();
        verifyLogHeader(logBuffer, offset, CATCHUP_POSITION.toEventCodeId(), length, length);
        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(followerMemberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        final int catchupEndpointLength = logBuffer.getInt(index, LITTLE_ENDIAN);
        index += SIZE_OF_INT;
        assertEquals(catchupEndpoint, logBuffer.getStringWithoutLengthAscii(index, catchupEndpointLength));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectCatchupPosition(CATCHUP_POSITION, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: CATCHUP_POSITION \\[61/61]: " +
            "memberId=-901 leadershipTermId=1233 logPosition=100 followerMemberId=18 " +
            "catchupEndpoint=aeron:udp\\?endpoint=localhost:9090";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logCatchupPositionLongCatchupEndpoint()
    {
        final int offset = ALIGNMENT * 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final long leadershipTermId = 1233L;
        final long logPosition = 100L;
        final int followerMemberId = -7;
        final int memberId = 44;

        final byte[] alias = new byte[8192];
        Arrays.fill(alias, (byte)'x');

        final String catchupEndpoint = "aeron:udp?endpoint=localhost:9090|alias=" + new String(
            alias,
            StandardCharsets.US_ASCII);

        logger.logOnCatchupPosition(memberId, leadershipTermId, logPosition, followerMemberId, catchupEndpoint);

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectCatchupPosition(CATCHUP_POSITION, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]*\\.[0-9]*] CLUSTER: CATCHUP_POSITION \\[[0-9]*/8260]: " +
            "memberId=44 leadershipTermId=1233 logPosition=100 followerMemberId=-7 " +
            "catchupEndpoint=aeron:udp\\?endpoint=localhost:9090\\|alias=(x)*\\.\\.\\.";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logStopCatchup()
    {
        final int offset = ALIGNMENT * 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final long leadershipTermId = 1233L;
        final int followerMemberId = 7;
        final int memberId = 2;

        logger.logOnStopCatchup(memberId, leadershipTermId, followerMemberId);

        final int length = SIZE_OF_LONG + 2 * SIZE_OF_INT;
        verifyLogHeader(logBuffer, offset, STOP_CATCHUP.toEventCodeId(), length, length);
        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(followerMemberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectStopCatchup(STOP_CATCHUP, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: STOP_CATCHUP \\[16/16]: " +
            "memberId=2 leadershipTermId=1233 followerMemberId=7";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logTruncateLogEntry()
    {
        final int offset = align(22, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final ChronoUnit state = ChronoUnit.FOREVER;
        final int memberId = 8;
        final long logLeadershipTermId = 777L;
        final long leadershipTermId = 1233L;
        final long candidateTermId = 42L;
        final int commitPosition = 1000;
        final long logPosition = 33L;
        final long appendPosition = 555L;
        final long oldPosition = 98L;
        final long newPosition = 24L;

        logger.logOnTruncateLogEntry(
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

        final int length = SIZE_OF_INT + state.name().length() + SIZE_OF_INT + 8 * SIZE_OF_LONG;
        verifyLogHeader(logBuffer, offset, TRUNCATE_LOG_ENTRY.toEventCodeId(), length, length);

        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(logLeadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(candidateTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(commitPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(appendPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(oldPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(newPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(enumName(state), logBuffer.getStringAscii(index, LITTLE_ENDIAN));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectTruncateLogEntry(TRUNCATE_LOG_ENTRY, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: TRUNCATE_LOG_ENTRY \\[79/79]: " +
            "memberId=8 state=FOREVER logLeadershipTermId=777 leadershipTermId=1233 candidateTermId=42 " +
            "commitPosition=1000 logPosition=33 appendPosition=555 oldPosition=98 newPosition=24";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logReplayNewLeadershipTerm()
    {
        final int offset = ALIGNMENT * 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final int memberId = 982374;
        final boolean isInElection = true;
        final long leadershipTermId = 1233L;
        final long logPosition = 988723465L;
        final long timestamp = 890723452345L;
        final long termBaseLogPosition = logPosition - 32;
        final TimeUnit timeUnit = NANOSECONDS;
        final int appVersion = 13;

        logger.logOnReplayNewLeadershipTermEvent(
            memberId,
            isInElection,
            leadershipTermId,
            logPosition,
            timestamp,
            termBaseLogPosition,
            TimeUnit.NANOSECONDS,
            appVersion);

        final int length = replayNewLeadershipTermEventLength(timeUnit);
        verifyLogHeader(logBuffer, offset, REPLAY_NEW_LEADERSHIP_TERM.toEventCodeId(), length, length);
        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;

        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(timestamp, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(termBaseLogPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(appVersion, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(isInElection, 0 != logBuffer.getByte(index));
        index += SIZE_OF_BYTE;
        assertEquals(timeUnit.name(), logBuffer.getStringAscii(index, LITTLE_ENDIAN));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectReplayNewLeadershipTerm(
            REPLAY_NEW_LEADERSHIP_TERM, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: REPLAY_NEW_LEADERSHIP_TERM " +
            "\\[56/56]: memberId=982374 isInElection=true leadershipTermId=1233 logPosition=988723465 " +
            "termBaseLogPosition=988723433 appVersion=13 timestamp=890723452345 timeUnit=NANOSECONDS";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logOnAppendPosition()
    {
        final int offset = ALIGNMENT * 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final long leadershipTermId = 1233L;
        final long logPosition = 988723465L;
        final int followerMemberId = 982374;
        final int memberId = 61;
        final byte flags = 1;

        logger.logOnAppendPosition(memberId, leadershipTermId, logPosition, followerMemberId, flags);

        final int length = 2 * SIZE_OF_LONG + 2 * SIZE_OF_INT + SIZE_OF_BYTE;
        verifyLogHeader(logBuffer, offset, APPEND_POSITION.toEventCodeId(), length, length);
        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;

        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(followerMemberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(flags, logBuffer.getByte(index));
        index += SIZE_OF_BYTE;

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectAppendPosition(APPEND_POSITION, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: APPEND_POSITION " +
            "\\[25/25]: memberId=61 leadershipTermId=1233 logPosition=988723465 followerMemberId=982374 " +
            "flags=0b00000001";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logOnCommitPosition()
    {
        final int offset = ALIGNMENT * 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final long leadershipTermId = 1233L;
        final long logPosition = 988723465L;
        final int leaderId = 982374;
        final int memberId = 2;

        logger.logOnCommitPosition(memberId, leadershipTermId, logPosition, leaderId);

        final int length = 2 * SIZE_OF_LONG + 2 * SIZE_OF_INT;
        verifyLogHeader(logBuffer, offset, COMMIT_POSITION.toEventCodeId(), length, length);
        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;

        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(leaderId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectCommitPosition(COMMIT_POSITION, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: COMMIT_POSITION " +
            "\\[24/24]: memberId=2 leadershipTermId=1233 logPosition=988723465 leaderId=982374";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logAppendSessionClose()
    {
        final int offset = ALIGNMENT + 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);

        final int memberId = 829374;
        final long sessionId = 289374L;
        final CloseReason closeReason = CloseReason.TIMEOUT;
        final long leadershipTermId = 2039842L;
        final long timestamp = 29384;
        final TimeUnit timeUnit = MILLISECONDS;

        logger.logAppendSessionClose(memberId, sessionId, closeReason, leadershipTermId, timestamp, timeUnit);

        final int length = 3 * SIZE_OF_LONG + SIZE_OF_INT + (SIZE_OF_INT + closeReason.name().length()) +
            (SIZE_OF_INT + timeUnit.name().length());

        verifyLogHeader(logBuffer, offset, APPEND_SESSION_CLOSE.toEventCodeId(), length, length);
        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;

        assertEquals(sessionId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(timestamp, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(closeReason.name(), logBuffer.getStringAscii(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT + closeReason.name().length();
        assertEquals(timeUnit.name(), logBuffer.getStringAscii(index, LITTLE_ENDIAN));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectAppendSessionClose(
            APPEND_SESSION_CLOSE, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: APPEND_SESSION_CLOSE " +
            "\\[55/55]: memberId=829374 sessionId=289374 closeReason=TIMEOUT leadershipTermId=2039842 " +
            "timestamp=29384 timeUnit=MILLISECONDS";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logAppendSessionOpen()
    {
        final int offset = ALIGNMENT + 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);

        final int memberId = 829374;
        final long sessionId = 289374L;
        final long leadershipTermId = 2039842L;
        final long logPosition = 4L * 1024 * 1024 * 1024;
        final long timestamp = 29384;
        final TimeUnit timeUnit = MILLISECONDS;

        logger.logAppendSessionOpen(memberId, sessionId, leadershipTermId, logPosition, timestamp, timeUnit);

        final int length = 4 * SIZE_OF_LONG + SIZE_OF_INT + (SIZE_OF_INT + timeUnit.name().length());

        verifyLogHeader(logBuffer, offset, APPEND_SESSION_OPEN.toEventCodeId(), length, length);
        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;

        assertEquals(sessionId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(timestamp, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(timeUnit.name(), logBuffer.getStringAscii(index, LITTLE_ENDIAN));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectAppendSessionOpen(
            APPEND_SESSION_OPEN, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = """
            \\[[0-9]+\\.[0-9]+] CLUSTER: APPEND_SESSION_OPEN \
            \\[52/52]: memberId=829374 sessionId=289374 leadershipTermId=2039842 logPosition=4294967296 \
            timestamp=29384 timeUnit=MILLISECONDS""";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logOnRequestVote()
    {
        final long logLeadershipTermId = 12;
        final long logPosition = 4723489263846823L;
        final long candidateTermId = -19;
        final int candidateId = 89;
        final int protocolVersion = SemanticVersion.compose(2, 5, 17);
        final int memberId = 3;
        final int offset = 8;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);

        logger.logOnRequestVote(
            memberId, logLeadershipTermId, logPosition, candidateTermId, candidateId, protocolVersion);

        verifyLogHeader(logBuffer, offset, REQUEST_VOTE.toEventCodeId(), 36, 36);
        final int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(logLeadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        assertEquals(logPosition, logBuffer.getLong(index + SIZE_OF_LONG, LITTLE_ENDIAN));
        assertEquals(candidateTermId, logBuffer.getLong(index + 2 * SIZE_OF_LONG, LITTLE_ENDIAN));
        assertEquals(candidateId, logBuffer.getInt(index + 3 * SIZE_OF_LONG, LITTLE_ENDIAN));
        assertEquals(protocolVersion, logBuffer.getInt(index + 3 * SIZE_OF_LONG + SIZE_OF_INT, LITTLE_ENDIAN));
        assertEquals(memberId, logBuffer.getInt(index + 3 * SIZE_OF_LONG + 2 * SIZE_OF_INT, LITTLE_ENDIAN));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectRequestVote(
            REQUEST_VOTE, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: REQUEST_VOTE " +
            "\\[36/36]: memberId=3 logLeadershipTermId=12 logPosition=4723489263846823 candidateTermId=-19 " +
            "candidateId=89 protocolVersion=2.5.17";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logOnCanvassPosition()
    {
        final long logLeadershipTermId = 96;
        final long logPosition = 128L;
        final long leadershipTermId = 54;
        final int followerMemberId = 15;
        final int protocolVersion = SemanticVersion.compose(1, 9, 9);
        final int memberId = 222;
        final int offset = 64;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);

        logger.logOnCanvassPosition(
            memberId, logLeadershipTermId, logPosition, leadershipTermId, followerMemberId, protocolVersion);

        verifyLogHeader(
            logBuffer, offset, CANVASS_POSITION.toEventCodeId(), canvassPositionLength(), canvassPositionLength());
        final int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(logLeadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        assertEquals(logPosition, logBuffer.getLong(index + SIZE_OF_LONG, LITTLE_ENDIAN));
        assertEquals(leadershipTermId, logBuffer.getLong(index + 2 * SIZE_OF_LONG, LITTLE_ENDIAN));
        assertEquals(followerMemberId, logBuffer.getInt(index + 3 * SIZE_OF_LONG, LITTLE_ENDIAN));
        assertEquals(protocolVersion, logBuffer.getInt(index + 3 * SIZE_OF_LONG + SIZE_OF_INT, LITTLE_ENDIAN));
        assertEquals(memberId, logBuffer.getInt(index + 3 * SIZE_OF_LONG + 2 * SIZE_OF_INT, LITTLE_ENDIAN));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectCanvassPosition(
            CANVASS_POSITION, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: CANVASS_POSITION " +
            "\\[36/36]: memberId=222 logLeadershipTermId=96 logPosition=128 leadershipTermId=54 followerMemberId=15 " +
            "protocolVersion=1.9.9";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logTerminationPosition()
    {
        final long logLeadershipTermId = 96;
        final long logPosition = 128L;
        final int memberId = 222;
        final int offset = 64;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);

        logger.logTerminationPosition(memberId, logLeadershipTermId, logPosition);

        verifyLogHeader(
            logBuffer,
            offset,
            TERMINATION_POSITION.toEventCodeId(),
            terminationPositionLength(),
            terminationPositionLength());

        final int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(logLeadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        assertEquals(logPosition, logBuffer.getLong(index + SIZE_OF_LONG, LITTLE_ENDIAN));
        assertEquals(memberId, logBuffer.getInt(index + 2 * SIZE_OF_LONG, LITTLE_ENDIAN));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectTerminationPosition(
            TERMINATION_POSITION, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: TERMINATION_POSITION " +
            "\\[20/20]: memberId=222 logLeadershipTermId=96 logPosition=128";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logTerminationAck()
    {
        final long logLeadershipTermId = 96;
        final long logPosition = 128L;
        final int memberId = 222;
        final int senderMemberId = 982374;
        final int offset = 64;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);

        logger.logTerminationAck(memberId, logLeadershipTermId, logPosition, senderMemberId);

        verifyLogHeader(
            logBuffer,
            offset,
            TERMINATION_ACK.toEventCodeId(),
            terminationAckLength(),
            terminationAckLength());

        final int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(logLeadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        assertEquals(logPosition, logBuffer.getLong(index + SIZE_OF_LONG, LITTLE_ENDIAN));
        assertEquals(memberId, logBuffer.getInt(index + 2 * SIZE_OF_LONG, LITTLE_ENDIAN));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectTerminationAck(
            TERMINATION_ACK, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: TERMINATION_ACK " +
            "\\[24/24]: memberId=222 logLeadershipTermId=96 logPosition=128 senderMemberId=982374";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logServiceAck()
    {
        final int memberId = 222;
        final long logPosition = 128L;
        final long timestamp = 98273423L;
        final long ackId = 98234L;
        final long relevantId = 8998L;
        final int serviceId = 982374;
        final int offset = 64;
        final TimeUnit timeUnit = MILLISECONDS;

        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        logger.logServiceAck(memberId, logPosition, timestamp, timeUnit, ackId, relevantId, serviceId);

        verifyLogHeader(
            logBuffer,
            offset,
            SERVICE_ACK.toEventCodeId(),
            serviceAckLength(timeUnit),
            serviceAckLength(timeUnit));

        final int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(logPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        assertEquals(timestamp, logBuffer.getLong(index + SIZE_OF_LONG, LITTLE_ENDIAN));
        assertEquals(ackId, logBuffer.getLong(index + (2 * SIZE_OF_LONG), LITTLE_ENDIAN));
        assertEquals(relevantId, logBuffer.getLong(index + (3 * SIZE_OF_LONG), LITTLE_ENDIAN));
        assertEquals(memberId, logBuffer.getInt(index + (4 * SIZE_OF_LONG), LITTLE_ENDIAN));
        assertEquals(serviceId, logBuffer.getInt(index + SIZE_OF_INT + (4 * SIZE_OF_LONG), LITTLE_ENDIAN));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectServiceAck(
            SERVICE_ACK, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: SERVICE_ACK " +
            "\\[56/56]: memberId=222 logPosition=128 timestamp=98273423 timeUnit=MILLISECONDS " +
            "ackId=98234 relevantId=8998 serviceId=982374";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logReplicationEnded()
    {
        final int memberId = 222;
        final String purpose = "STANDBY_SNAPSHOT";
        final String channel = "aeron:udp?endpoint=localhost:9090";
        final long srcRecordingId = 234;
        final long dstRecordingId = 8435;
        final long position = 982342;
        final boolean hasSynced = true;
        final int offset = 64;

        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        logger.logReplicationEnded(
            memberId, purpose, channel, srcRecordingId, dstRecordingId, position, hasSynced);

        verifyLogHeader(
            logBuffer,
            offset,
            REPLICATION_ENDED.toEventCodeId(),
            replicationEndedLength(purpose, channel),
            replicationEndedLength(purpose, channel));

        final int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(srcRecordingId, logBuffer.getLong(index, LITTLE_ENDIAN));
        assertEquals(dstRecordingId, logBuffer.getLong(index + SIZE_OF_LONG, LITTLE_ENDIAN));
        assertEquals(position, logBuffer.getLong(index + (2 * SIZE_OF_LONG), LITTLE_ENDIAN));
        assertEquals(memberId, logBuffer.getInt(index + (3 * SIZE_OF_LONG), LITTLE_ENDIAN));
        assertEquals(1, logBuffer.getByte(index + (3 * SIZE_OF_LONG) + (SIZE_OF_INT)));
        final int purposeIndex = index + (3 * SIZE_OF_LONG) + (SIZE_OF_INT) + (SIZE_OF_BYTE);
        assertEquals(purpose, logBuffer.getStringAscii(purposeIndex));
        final int channelIndex = purposeIndex + SIZE_OF_INT + purpose.length();
        assertEquals(channel, logBuffer.getStringAscii(channelIndex, LITTLE_ENDIAN));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectReplicationEnded(
            REPLICATION_ENDED, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern =
            "\\[[0-9]+\\.[0-9]+] CLUSTER: REPLICATION_ENDED \\[86/86]: memberId=222 " +
            "purpose=STANDBY_SNAPSHOT channel=aeron:udp\\?endpoint=localhost:9090 srcRecordingId=234 " +
            "dstRecordingId=8435 position=982342 hasSynced=true";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logStandbySnapshotNotification()
    {
        final int memberId = 222;
        final long recordingId = 9823674L;
        final long leadershipTermId = 23478L;
        final long termBaseLogPosition = 823423L;
        final long logPosition = 9827342L;
        final long timestamp = 98273423434L;
        final int serviceId = 1;
        final String archiveEndpoint = "localhost:9090";
        final int offset = 64;
        final TimeUnit timeUnit = MILLISECONDS;

        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        logger.logStandbySnapshotNotification(
            memberId,
            recordingId,
            leadershipTermId,
            termBaseLogPosition,
            logPosition,
            timestamp,
            timeUnit,
            serviceId,
            archiveEndpoint);

        verifyLogHeader(
            logBuffer,
            offset,
            STANDBY_SNAPSHOT_NOTIFICATION.toEventCodeId(),
            standbySnapshotNotificationLength(timeUnit, archiveEndpoint),
            standbySnapshotNotificationLength(timeUnit, archiveEndpoint));

        final int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(recordingId, logBuffer.getLong(index, LITTLE_ENDIAN));
        assertEquals(leadershipTermId, logBuffer.getLong(index + SIZE_OF_LONG, LITTLE_ENDIAN));
        assertEquals(termBaseLogPosition, logBuffer.getLong(index + (2 * SIZE_OF_LONG), LITTLE_ENDIAN));
        assertEquals(logPosition, logBuffer.getLong(index + (3 * SIZE_OF_LONG), LITTLE_ENDIAN));
        assertEquals(timestamp, logBuffer.getLong(index + (4 * SIZE_OF_LONG), LITTLE_ENDIAN));
        assertEquals(memberId, logBuffer.getInt(index + (5 * SIZE_OF_LONG), LITTLE_ENDIAN));
        assertEquals(serviceId, logBuffer.getInt(index + (5 * SIZE_OF_LONG) + (SIZE_OF_INT), LITTLE_ENDIAN));

        final int timeUnitIndex = index + (5 * SIZE_OF_LONG) + (2 * SIZE_OF_INT);
        assertEquals(timeUnit.name(), logBuffer.getStringAscii(timeUnitIndex));
        final int archiveEndpointIndex = timeUnitIndex + SIZE_OF_INT + timeUnit.name().length();
        assertEquals(archiveEndpoint, logBuffer.getStringAscii(archiveEndpointIndex, LITTLE_ENDIAN));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectStandbySnapshotNotification(
            STANDBY_SNAPSHOT_NOTIFICATION, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern =
            "\\[[0-9]+\\.[0-9]+] CLUSTER: STANDBY_SNAPSHOT_NOTIFICATION \\[90/90]: memberId=222 " +
            "recordingId=9823674 leadershipTermId=23478 termBaseLeadershipPosition=823423 logPosition=9827342 " +
            "timestamp=98273423434 timeUnit=MILLISECONDS serviceId=1 archiveEndpoint=localhost:9090";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logNewElection()
    {
        final int memberId = 42;
        final long leadershipTermId = 8L;
        final long logPosition = 9827342L;
        final long appendPosition = 342384382L;
        final String reason = "why an election was started";
        final int offset = 16;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);

        final int encodedLength = newElectionLength(reason);

        logger.logNewElection(memberId, leadershipTermId, logPosition, appendPosition, reason);

        verifyLogHeader(
            logBuffer,
            offset,
            NEW_ELECTION.toEventCodeId(),
            encodedLength,
            encodedLength);

        final int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        assertEquals(logPosition, logBuffer.getLong(index + SIZE_OF_LONG, LITTLE_ENDIAN));
        assertEquals(appendPosition, logBuffer.getLong(index + 2 * SIZE_OF_LONG, LITTLE_ENDIAN));
        assertEquals(memberId, logBuffer.getInt(index + (3 * SIZE_OF_LONG), LITTLE_ENDIAN));
        assertEquals(reason, logBuffer.getStringAscii(index + (3 * SIZE_OF_LONG) + SIZE_OF_INT, LITTLE_ENDIAN));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectNewElection(NEW_ELECTION, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern =
            "\\[[0-9]+\\.[0-9]+] CLUSTER: NEW_ELECTION \\[59/59]: memberId=42 " +
            "leadershipTermId=8 logPosition=9827342 appendPosition=342384382 reason=why an election was started";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logClusterSessionStateChange()
    {
        final int memberId = 7;
        final long sessionId = -47238947;
        final ChronoUnit action = ChronoUnit.WEEKS;
        final TimeUnit from = TimeUnit.NANOSECONDS;
        final TimeUnit to = TimeUnit.HOURS;
        final String reason = "state changed somehow";
        final int offset = 16;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);

        final int encodedLength = clusterSessionStateChangeLength(action, from, to, reason);

        logger.logClusterSessionStateChange(
            memberId, sessionId, action, from, to, reason);

        verifyLogHeader(
            logBuffer,
            offset,
            CLUSTER_SESSION_STATE_CHANGE.toEventCodeId(),
            encodedLength,
            encodedLength);

        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(sessionId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(action.name(), logBuffer.getStringAscii(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT + action.name().length();
        final String expectedStateTransition = from + " -> " + to;
        assertEquals(expectedStateTransition, logBuffer.getStringAscii(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT + expectedStateTransition.length();
        assertEquals(reason, logBuffer.getStringAscii(index, LITTLE_ENDIAN));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectClusterSessionStateChange(
            CLUSTER_SESSION_STATE_CHANGE, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern =
            """
                \\[[0-9]+\\.[0-9]+] CLUSTER: CLUSTER_SESSION_STATE_CHANGE \\[74/74]: memberId=7 \
                sessionId=-47238947 action=WEEKS NANOSECONDS -> HOURS reason=\"state changed somehow\"""";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }
}
