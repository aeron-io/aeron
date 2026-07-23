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

import io.aeron.cluster.ConsensusModule;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.aeron.agent.ClusterEventEncoder.electionStateChangeLength;
import static io.aeron.agent.ClusterEventEncoder.encodeElectionStateChange;
import static io.aeron.agent.ClusterEventEncoder.encodeOnNewLeadershipTerm;
import static io.aeron.agent.ClusterEventEncoder.encodeStateChange;
import static io.aeron.agent.ClusterEventEncoder.encodeTruncateLogEntry;
import static io.aeron.agent.ClusterEventEncoder.newLeaderShipTermLength;
import static io.aeron.agent.ClusterEventEncoder.stateChangeLength;
import static io.aeron.agent.CommonEventEncoder.LOG_HEADER_LENGTH;
import static io.aeron.agent.CommonEventEncoder.STATE_SEPARATOR;
import static io.aeron.agent.CommonEventEncoder.captureLength;
import static io.aeron.agent.CommonEventEncoder.encodedLength;
import static io.aeron.agent.CommonEventEncoder.enumName;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_BYTE;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ClusterEventEncoderTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_EVENT_LENGTH]);

    @Test
    void testEncodeStateChange()
    {
        final int offset = 24;
        final ConsensusModule.State from = ConsensusModule.State.ACTIVE;
        final ConsensusModule.State to = ConsensusModule.State.CLOSED;
        final int memberId = 42;
        final String payload = from.name() + STATE_SEPARATOR + to.name();
        final String reason = "test it here";
        final int length = stateChangeLength(from, to, reason);
        final int captureLength = captureLength(length);

        final int encodedLength = encodeStateChange(buffer, offset, captureLength, length, memberId, from, to, reason);
        assertEquals(encodedLength(captureLength), encodedLength);

        int index = offset;
        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertNotEquals(0, buffer.getLong(offset + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(memberId, buffer.getInt(offset + LOG_HEADER_LENGTH));
        index += SIZE_OF_INT;
        assertEquals(payload, buffer.getStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_INT));
    }

    @Test
    @SuppressWarnings("MethodLength")
    void testEncodeNewLeadershipTerm()
    {
        final int offset = 200;
        final int captureLength = 18;
        final int length = 54;
        final long logLeadershipTermId = 111;
        final long nextLeadershipTermId = 2561;
        final long nextTermBaseLogPosition = 2562;
        final long nextLogPosition = 2563;
        final long leadershipTermId = 222;
        final long logPosition = 1024;
        final long commitPosition = 512;
        final long timestamp = 32423436;
        final int memberId = 5;
        final int leaderId = 42;
        final int logSessionId = 18;
        final int appVersion = 777;
        final long termBaseLogPosition = 23874;
        final long leaderRecordingId = 9;
        final boolean isStartup = true;

        final int encodedLength = encodeOnNewLeadershipTerm(
            buffer,
            offset,
            captureLength,
            length,
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

        assertEquals(encodedLength(newLeaderShipTermLength()), encodedLength);

        int index = offset;
        assertEquals(captureLength, buffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(length, buffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertNotEquals(0, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logLeadershipTermId, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(nextLeadershipTermId, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(nextTermBaseLogPosition, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(nextLogPosition, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(leadershipTermId, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(termBaseLogPosition, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logPosition, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(commitPosition, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(leaderRecordingId, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(timestamp, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(memberId, buffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(leaderId, buffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(logSessionId, buffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(appVersion, buffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;

        assertEquals(isStartup, 1 == buffer.getInt(index, LITTLE_ENDIAN));
    }

    @Test
    void testNewLeaderShipTermLength()
    {
        assertEquals(SIZE_OF_LONG * 10 + SIZE_OF_INT * 4 + SIZE_OF_BYTE, newLeaderShipTermLength());
    }

    @Test
    void testStateChangeLength()
    {
        final ChronoUnit from = ChronoUnit.CENTURIES;
        final ChronoUnit to = ChronoUnit.HALF_DAYS;
        final String payload = from.name() + STATE_SEPARATOR + to.name();
        final String data = "data ...";

        assertEquals(
            payload.length() + (SIZE_OF_INT * 2) + SIZE_OF_INT + data.length(),
            stateChangeLength(from, to, data));
    }

    @Test
    void testElectionStateChangeLength()
    {
        final TimeUnit from = TimeUnit.DAYS;
        final TimeUnit to = TimeUnit.MICROSECONDS;
        final String payload = from.name() + STATE_SEPARATOR + to.name();
        final String reason = "this state transition occured in a test";

        assertEquals((2 * SIZE_OF_INT) + (6 * SIZE_OF_LONG) + SIZE_OF_INT + payload.length() +
            SIZE_OF_INT + reason.length(),
            electionStateChangeLength(from, to, reason));
    }

    @Test
    void testEncodeElectionStateChange()
    {
        final int offset = 8;
        final ChronoUnit to = ChronoUnit.MILLENNIA;
        final int memberId = 278;
        final int leaderId = -100;
        final long candidateTermId = 777L;
        final long leadershipTermId = 42L;
        final long logPosition = 128L;
        final long logLeadershipTermId = 1L;
        final long appendPosition = 998L;
        final long catchupPosition = 200L;
        final String reason = "test";
        final int captureLength = captureLength(electionStateChangeLength(null, to, reason));
        final int length = encodedLength(captureLength);

        final int encodedLength = encodeElectionStateChange(
            buffer,
            offset,
            captureLength,
            length,
            memberId,
            null,
            to,
            leaderId,
            candidateTermId,
            leadershipTermId,
            logPosition,
            logLeadershipTermId,
            appendPosition,
            catchupPosition,
            reason);

        assertEquals(length, encodedLength);

        int index = offset;
        assertEquals(captureLength, buffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(length, buffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertNotEquals(0, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(candidateTermId, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(leadershipTermId, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logPosition, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logLeadershipTermId, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(appendPosition, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(catchupPosition, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(memberId, buffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(leaderId, buffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals("null" + STATE_SEPARATOR + to.name(), buffer.getStringAscii(index));
    }

    @ParameterizedTest
    @MethodSource("logEntryStates")
    void testEncodeTruncateLogEntry(final TimeUnit state)
    {
        final int offset = 8;
        final int length = 300;
        final int captureLength = 128;
        final int memberId = -18;
        final long logLeadershipTermId = 42;
        final long leadershipTermId = -408324982349823L;
        final long candidateTermId = 233333L;
        final long commitPosition = 192L;
        final long logPosition = 100L;
        final long appendPosition = 120L;
        final long oldPosition = 200L;
        final long newPosition = 88L;

        final int encodedLength = encodeTruncateLogEntry(
            buffer,
            offset,
            length,
            captureLength,
            memberId, state,
            logLeadershipTermId,
            leadershipTermId,
            candidateTermId,
            commitPosition,
            logPosition,
            appendPosition,
            oldPosition,
            newPosition);

        assertEquals(
            encodedLength(SIZE_OF_INT + 8 * SIZE_OF_LONG + SIZE_OF_INT + enumName(state).length()),
            encodedLength);
        int index = offset;
        assertEquals(captureLength, buffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(length, buffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertNotEquals(0, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logLeadershipTermId, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(leadershipTermId, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(candidateTermId, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(commitPosition, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logPosition, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(appendPosition, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(oldPosition, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(newPosition, buffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(memberId, buffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(enumName(state), buffer.getStringAscii(index, LITTLE_ENDIAN));
    }

    private static List<TimeUnit> logEntryStates()
    {
        return Arrays.asList(
            TimeUnit.MICROSECONDS,
            null);
    }
}
