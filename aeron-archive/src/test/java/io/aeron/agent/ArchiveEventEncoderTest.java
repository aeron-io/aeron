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

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static io.aeron.agent.ArchiveEventEncoder.*;
import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ArchiveEventEncoderTest
{
    enum State
    {
        ALPHA, BETA
    }

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_EVENT_LENGTH]);

    @Test
    void testEncodeControlSessionStateChange()
    {
        final int offset = 24;
        final TimeUnit from = DAYS;
        final TimeUnit to = MILLISECONDS;
        final long sessionId = Long.MAX_VALUE;
        final String payload = from.name() + STATE_SEPARATOR + to.name();
        final String reason = "test reason for now";
        final int length = payload.length() + SIZE_OF_LONG + SIZE_OF_INT + SIZE_OF_INT + reason.length();
        final int captureLength = captureLength(length);

        final int encodedLength = encodeControlSessionStateChange(
            buffer, offset, captureLength, length, from, to, sessionId, reason);

        assertEquals(encodedLength(sessionStateChangeLength(from, to, reason)), encodedLength);
        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(sessionId, buffer.getLong(offset + LOG_HEADER_LENGTH));
        assertEquals(payload, buffer.getStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG));
    }

    @Test
    void testSessionStateChangeLength()
    {
        final ChronoUnit from = ChronoUnit.ERAS;
        final ChronoUnit to = ChronoUnit.MILLENNIA;
        final String payload = from.name() + STATE_SEPARATOR + to.name();
        final String reason = "hfskhflkdhfldshlfkhllkshflkhsldfhaslkfhsaklhflksahdflsahlhalks";

        assertEquals(
            payload.length() + SIZE_OF_LONG + SIZE_OF_INT * 2 + reason.length(),
            sessionStateChangeLength(from, to, reason));
    }

    @Test
    void testEncodeReplaySessionError()
    {
        final int offset = 24;
        final long sessionId = Long.MAX_VALUE;
        final long recordingId = 56;
        final String errorMessage = "funny";
        final int length = errorMessage.length() + SIZE_OF_LONG * 2 + SIZE_OF_INT;
        final int captureLength = captureLength(length);

        encodeReplaySessionError(buffer, offset, captureLength, length, sessionId, recordingId, errorMessage);

        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(sessionId, buffer.getLong(offset + LOG_HEADER_LENGTH));
        assertEquals(recordingId, buffer.getLong(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG));
        assertEquals(errorMessage, buffer.getStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG * 2));
    }

    @Test
    void testEncodeCatalogResize()
    {
        final int offset = 24;
        final int length = SIZE_OF_LONG * 2 + SIZE_OF_INT * 2;
        final int captureLength = captureLength(length);
        final long catalogLength = 128;
        final long newCatalogLength = 1024;

        encodeCatalogResize(buffer, offset, captureLength, length, catalogLength, newCatalogLength);

        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(catalogLength, buffer.getLong(offset + LOG_HEADER_LENGTH));
        assertEquals(newCatalogLength, buffer.getLong(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG));
    }

    @Test
    void testEncodeReplaySessionStateChange()
    {
        int offset = 24;
        final int length = replaySessionStateChangeLength(State.ALPHA, State.BETA, "reason");
        final int captureLength = captureLength(length);

        encodeReplaySessionStateChange(buffer, offset, captureLength, length,
            State.ALPHA, State.BETA, 1, 2, 3, "reason");

        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + 2 * SIZE_OF_INT, LITTLE_ENDIAN));
        offset += LOG_HEADER_LENGTH;

        assertEquals(1, buffer.getLong(offset, LITTLE_ENDIAN));
        offset += SIZE_OF_LONG;
        assertEquals(2, buffer.getLong(offset, LITTLE_ENDIAN));
        offset += SIZE_OF_LONG;
        assertEquals(3, buffer.getLong(offset, LITTLE_ENDIAN));
        offset += SIZE_OF_LONG;
        assertEquals("ALPHA -> BETA", buffer.getStringAscii(offset));
        offset += SIZE_OF_INT + "ALPHA -> BETA".length();
        assertEquals("reason", buffer.getStringAscii(offset));
    }

    @Test
    void testEncodeRecordingSessionStateChange()
    {
        int offset = 24;
        final int length = recordingSessionStateChangeLength(State.ALPHA, State.BETA, "reason");
        final int captureLength = captureLength(length);

        encodeRecordingSessionStateChange(buffer, offset, captureLength, length,
            State.ALPHA, State.BETA, 1, 2, "reason");

        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + 2 * SIZE_OF_INT, LITTLE_ENDIAN));
        offset += LOG_HEADER_LENGTH;

        assertEquals(1, buffer.getLong(offset, LITTLE_ENDIAN));
        offset += SIZE_OF_LONG;
        assertEquals(2, buffer.getLong(offset, LITTLE_ENDIAN));
        offset += SIZE_OF_LONG;
        assertEquals("ALPHA -> BETA", buffer.getStringAscii(offset));
        offset += SIZE_OF_INT + "ALPHA -> BETA".length();
        assertEquals("reason", buffer.getStringAscii(offset));
    }

    @Test
    void testEncodeReplicationSessionStateChange()
    {
        int offset = 24;
        final int length = replicationSessionStateChangeLength(State.ALPHA, State.BETA, "reason");
        final int captureLength = captureLength(length);

        encodeReplicationSessionStateChange(buffer, offset, captureLength, length,
            State.ALPHA, State.BETA, 1, 2, 3, 4, "reason");

        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + 2 * SIZE_OF_INT, LITTLE_ENDIAN));
        offset += LOG_HEADER_LENGTH;

        assertEquals(1, buffer.getLong(offset, LITTLE_ENDIAN));
        offset += SIZE_OF_LONG;
        assertEquals(2, buffer.getLong(offset, LITTLE_ENDIAN));
        offset += SIZE_OF_LONG;
        assertEquals(3, buffer.getLong(offset, LITTLE_ENDIAN));
        offset += SIZE_OF_LONG;
        assertEquals(4, buffer.getLong(offset, LITTLE_ENDIAN));
        offset += SIZE_OF_LONG;
        assertEquals("ALPHA -> BETA", buffer.getStringAscii(offset));
        offset += SIZE_OF_INT + "ALPHA -> BETA".length();
        assertEquals("reason", buffer.getStringAscii(offset));
    }

    @Test
    void testEncodePersistentSubscriptionStateChange()
    {
        final int offset = 24;
        final long recordingId = 16;
        final String replayChannel = "aeron:udp?endpoint=localhost:9010";
        final int replayStreamId = 10;
        final String liveChannel = "aeron:udp?endpoint=localhost:10010";
        final int liveStreamId = 11;
        final TimeUnit to = DAYS;
        final TimeUnit from = MILLISECONDS;
        final String payload = from.name() + STATE_SEPARATOR + to.name();

        final int length = SIZE_OF_LONG + replayChannel.length() + SIZE_OF_INT * 2 +
            liveChannel.length() + SIZE_OF_INT * 2 + payload.length() + SIZE_OF_INT;
        final int captureLength = captureLength(length);

        final int encodedLength = encodePersistentSubscriptionStateChange(
            buffer, offset, captureLength, length, from, to, recordingId, replayChannel, replayStreamId, liveChannel,
            liveStreamId
        );

        int absoluteOffset = offset + LOG_HEADER_LENGTH;

        assertEquals(
            encodedLength(persistentSubscriptionStateChangeLength(from, to, replayChannel, liveChannel)),
            encodedLength
        );
        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));

        assertEquals(recordingId, buffer.getLong(absoluteOffset));
        absoluteOffset += SIZE_OF_LONG;
        assertEquals(replayChannel, buffer.getStringAscii(absoluteOffset));
        absoluteOffset += replayChannel.length() + SIZE_OF_INT;
        assertEquals(replayStreamId, buffer.getInt(absoluteOffset));
        absoluteOffset += SIZE_OF_INT;
        assertEquals(liveChannel, buffer.getStringAscii(absoluteOffset));
        absoluteOffset += liveChannel.length() + SIZE_OF_INT;
        assertEquals(liveStreamId, buffer.getInt(absoluteOffset));
        absoluteOffset += SIZE_OF_INT;
        assertEquals(payload, buffer.getStringAscii(absoluteOffset));
    }

    @Test
    void testEncodePersistentSubscriptionJoinedLive()
    {
        final int offset = 24;
        final int liveSessionId = 21;
        final long joinPosition = 128;
        final long recordingId = 16;
        final String replayChannel = "aeron:udp?endpoint=localhost:9010";
        final int replayStreamId = 10;
        final String liveChannel = "aeron:udp?endpoint=localhost:10010";
        final int liveStreamId = 11;

        final int length = SIZE_OF_LONG + replayChannel.length() + SIZE_OF_INT * 2 +
            liveChannel.length() + SIZE_OF_INT * 3 + SIZE_OF_LONG;
        final int captureLength = captureLength(length);

        final int encodedLength = encodePersistentSubscriptionJoinedLive(
            buffer, offset, captureLength, length, recordingId, replayChannel, replayStreamId, liveChannel,
            liveStreamId, liveSessionId, joinPosition
        );

        int absoluteOffset = offset + LOG_HEADER_LENGTH;

        assertEquals(encodedLength(persistentSubscriptionJoinedLiveLength(replayChannel, liveChannel)), encodedLength);
        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));

        assertEquals(recordingId, buffer.getLong(absoluteOffset));
        absoluteOffset += SIZE_OF_LONG;
        assertEquals(replayChannel, buffer.getStringAscii(absoluteOffset));
        absoluteOffset += replayChannel.length() + SIZE_OF_INT;
        assertEquals(replayStreamId, buffer.getInt(absoluteOffset));
        absoluteOffset += SIZE_OF_INT;
        assertEquals(liveChannel, buffer.getStringAscii(absoluteOffset));
        absoluteOffset += liveChannel.length() + SIZE_OF_INT;
        assertEquals(liveStreamId, buffer.getInt(absoluteOffset));
        absoluteOffset += SIZE_OF_INT;
        assertEquals(liveSessionId, buffer.getInt(absoluteOffset));
        absoluteOffset += SIZE_OF_INT;
        assertEquals(joinPosition, buffer.getLong(absoluteOffset));
    }

    @Test
    void testEncodePersistentSubscriptionLeftLive()
    {
        final int offset = 24;
        final long recordingId = 16;
        final String replayChannel = "aeron:udp?endpoint=localhost:9010";
        final int replayStreamId = 10;
        final String liveChannel = "aeron:udp?endpoint=localhost:10010";
        final int liveStreamId = 11;
        final long livePosition = 256;

        final int length = SIZE_OF_LONG + replayChannel.length() + SIZE_OF_INT * 2 +
            liveChannel.length() + SIZE_OF_INT * 2 + SIZE_OF_LONG;
        final int captureLength = captureLength(length);

        final int encodedLength = encodePersistentSubscriptionLeftLive(
            buffer, offset, captureLength, length, recordingId, replayChannel, replayStreamId, liveChannel,
            liveStreamId, livePosition
        );

        int absoluteOffset = offset + LOG_HEADER_LENGTH;

        assertEquals(
            encodedLength(persistentSubscriptionLeftLiveLength(replayChannel, liveChannel)), encodedLength);
        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));

        assertEquals(recordingId, buffer.getLong(absoluteOffset));
        absoluteOffset += SIZE_OF_LONG;
        assertEquals(replayChannel, buffer.getStringAscii(absoluteOffset));
        absoluteOffset += replayChannel.length() + SIZE_OF_INT;
        assertEquals(replayStreamId, buffer.getInt(absoluteOffset));
        absoluteOffset += SIZE_OF_INT;
        assertEquals(liveChannel, buffer.getStringAscii(absoluteOffset));
        absoluteOffset += liveChannel.length() + SIZE_OF_INT;
        assertEquals(liveStreamId, buffer.getInt(absoluteOffset));
        absoluteOffset += SIZE_OF_INT;
        assertEquals(livePosition, buffer.getLong(absoluteOffset));
    }
}
