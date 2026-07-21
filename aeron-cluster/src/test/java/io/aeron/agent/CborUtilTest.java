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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.cbor.CBORFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class CborUtilTest
{
    @ParameterizedTest
    @ValueSource(longs = {
        2, 25, 0x7F, 0x100,
        0x1234, 0x7FFF, 0x10000,
        0x12345678, 0x7FFFFFFF, 0x80000000,
        0x123456789ABCDEF0L, 0xFFFFFFFFFFFFFFFFL,
        -2, -25, -0xFF,
        -0x1234, -0xFFFF,
        -0x12345678, -0x7FFFFFFE })
    void shouldEncodeNumberMessage(final long memberId)
    {
        final int offset = 0;
        final int length = 1024;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);

        final long timestamp = 12643263L;

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);

        CborUtil.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, timestamp);
        CborUtil.encode(encodingState, "memberId", memberId);
        CborUtil.encodeFooter(encodingState);

        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());

        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Map<String, Object> stringObjectMap = cborObjectMapper.readValue(
            data,
            new TypeReference<>()
            {
            });

        assertEquals(memberId, ((Number)stringObjectMap.get("memberId")).longValue());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldEncodeBooleans(final boolean val)
    {
        final int offset = 0;
        final int length = 1024;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);

        final long timestamp = 12643263L;

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);

        CborUtil.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, timestamp);
        CborUtil.encode(encodingState, "boolean", val);
        CborUtil.encodeFooter(encodingState);

        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());

        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Map<String, Object> stringObjectMap = cborObjectMapper.readValue(
            data,
            new TypeReference<>()
            {
            });

        assertEquals(val, stringObjectMap.get("boolean"));
    }

    static Stream<Arguments> generateStringsAndNull()
    {
        return Stream.of(
            Arguments.of("A".repeat(10)),
            Arguments.of("A".repeat(1000)),
            Arguments.of("A".repeat(100_000)),
            Arguments.of((String)null)
        );
    }

    @ParameterizedTest
    @MethodSource("generateStringsAndNull")
    void shouldEncodeCharSequenceMessageOrNull(final String reason)
    {
        final int offset = 0;
        final int length = 200_000;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);

        final long timestamp = 12643263L;

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);

        CborUtil.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, timestamp);
        CborUtil.encode(encodingState, "reason", reason);
        CborUtil.encodeFooter(encodingState);

        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());

        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Map<String, Object> stringObjectMap = cborObjectMapper.readValue(
            data,
            new TypeReference<>()
            {
            });

        assertEquals(reason, stringObjectMap.get("reason"));
    }

    @ParameterizedTest
    @CsvSource({
        "10,1",
        "0x1F,2",
        "0x1234,3",
        "0x12345678,5",
        "0x123456781234567, 9"
    })
    void shouldGetCorrectLengthForNumber(final long number, final int expectedLength)
    {
        assertEquals(expectedLength, CborUtil.lengthNumber(number));
    }

    static Stream<Arguments> generateBigStringsForLengthCalculation()
    {
        return Stream.of(
            Arguments.of("A".repeat(10), 11),
            Arguments.of("A".repeat(100), 102),
            Arguments.of("A".repeat(1000), 1003),
            Arguments.of("A".repeat(100_000), 100_005),
            Arguments.of(null, 1)
        );
    }

    @ParameterizedTest
    @MethodSource("generateBigStringsForLengthCalculation")
    void shouldGetCorrectLengthForCharSequence(final CharSequence text, final int expectedLength)
    {
        assertEquals(expectedLength, CborUtil.lengthString(text));
    }

    @Test
    void shouldEncodeTruncateMessageThatIsTooLong()
    {
        final int offset = 0;
        final int length = 100;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);
        CborUtil.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, 12643263L);
        CborUtil.encode(encodingState, "key1", 1_000_000_000L);
        CborUtil.encode(encodingState, "key2", TimeUnit.DAYS.name());
        CborUtil.encode(encodingState, "key3", "S".repeat(1_000_000));

        CborUtil.encodeFooter(encodingState);
        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());

        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Map<String, Object> stringObjectMap = cborObjectMapper.readValue(
            data,
            new TypeReference<>()
            {
            });

        assertEquals(Set.of("key1", "key2", "key3"), stringObjectMap.keySet());
        assertEquals(1_000_000_000L, ((Number)stringObjectMap.get("key1")).longValue());
        assertEquals(TimeUnit.DAYS.name(), stringObjectMap.get("key2"));

        final String truncatedKey3 = (String)stringObjectMap.get("key3");
        assertTrue(truncatedKey3.endsWith("..."));
        assertTrue(truncatedKey3.length() < 1_000_000);
        assertTrue("S".repeat(1_000_000).startsWith(truncatedKey3.substring(0, truncatedKey3.length() - 3)));
    }

    @Test
    void shouldEncodeTruncatedMultikeyMessageTooLongForFooter()
    {
        final int offset = 0;
        // key1 and key3 fit fully; the remaining space is only enough for key2's
        // key plus a handful of characters, forcing its huge value to truncate,
        // while the footer still has room to be written.
        final int length = 40;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);
        CborUtil.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, 12643263L);
        CborUtil.encode(encodingState, "key1", 1_000_000_000L);
        CborUtil.encode(encodingState, "key3", TimeUnit.DAYS.name());
        CborUtil.encode(encodingState, "key2", "S".repeat(1_000_000));
        CborUtil.encodeFooter(encodingState);
        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());

        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Map<String, Object> stringObjectMap = cborObjectMapper.readValue(
            data,
            new TypeReference<>()
            {
            });

        assertEquals(Set.of("key1", "key2", "key3"), stringObjectMap.keySet());
        assertEquals(1_000_000_000L, ((Number)stringObjectMap.get("key1")).longValue());
        assertEquals(TimeUnit.DAYS.name(), stringObjectMap.get("key3"));

        final String truncatedKey2 = (String)stringObjectMap.get("key2");
        assertTrue(truncatedKey2.endsWith("..."));
        assertTrue(truncatedKey2.length() < 1_000_000);
        assertTrue(
            "S".repeat(1_000_000).startsWith(
                truncatedKey2.substring(
                    0,
                    truncatedKey2.length() - 3)));
    }

    @Test
    void shouldEncodeTruncatedExtensiveMultikeyMessageThatIsTooLong()
    {
        final int offset = 0;

        // A relatively large buffer that comfortably fits every key except the oversized
        // "candidateTermIdentifierValue" string value, which alone needs ~5KB and gets
        // truncated. It is encoded last to keep buffer-size reasoning simple.
        final int length = 512;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);

        final String reason = "R".repeat(5_000);

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);
        CborUtil.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, 12643263L);
        CborUtil.encode(encodingState, "veryLongMemberIdentifierKey", Long.MAX_VALUE);
        CborUtil.encode(encodingState, "leadershipTermTimestampNanos", -123_456_789_012_345L);
        CborUtil.encode(encodingState, "logPositionSnapshotState", TimeUnit.NANOSECONDS.name());
        CborUtil.encode(encodingState, "appendPositionCatchupTarget", 42L);
        CborUtil.encode(encodingState, "negativeCatchupOffsetValue", -0x12345678L);
        CborUtil.encode(encodingState, "reason", reason);
        CborUtil.encodeFooter(encodingState);
        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());

        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Map<String, Object> stringObjectMap = cborObjectMapper.readValue(
            data,
            new TypeReference<>()
            {
            });

        assertEquals(Set.of(
            "veryLongMemberIdentifierKey",
            "leadershipTermTimestampNanos",
            "logPositionSnapshotState",
            "appendPositionCatchupTarget",
            "negativeCatchupOffsetValue",
            "reason"), stringObjectMap.keySet());
        assertEquals(Long.MAX_VALUE, ((Number)stringObjectMap.get("veryLongMemberIdentifierKey")).longValue());
        assertEquals(
            -123_456_789_012_345L, ((Number)stringObjectMap.get("leadershipTermTimestampNanos")).longValue());
        assertEquals(TimeUnit.NANOSECONDS.name(), stringObjectMap.get("logPositionSnapshotState"));
        assertEquals(42L, ((Number)stringObjectMap.get("appendPositionCatchupTarget")).longValue());
        assertEquals(-0x12345678L, ((Number)stringObjectMap.get("negativeCatchupOffsetValue")).longValue());

        final String truncatedValue = (String)stringObjectMap.get("reason");
        assertTrue(truncatedValue.endsWith("..."));
        assertTrue(truncatedValue.length() < 5_000);
        assertTrue(reason.startsWith(truncatedValue.substring(0, truncatedValue.length() - 3)));
    }

    @Test
    void shouldDropNonTruncatableLongKeyInMultiKeyMessage()
    {
        final int offset = 0;

        // A relatively large buffer that comfortably fits every key except the oversized
        // "candidateTermIdentifierValue" string value, which alone needs ~5KB and gets
        // truncated. It is encoded last to keep buffer-size reasoning simple.
        final int length = 512;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);

        final String reason = "R".repeat(5_000);

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);
        CborUtil.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, 12643263L);
        CborUtil.encode(encodingState, "veryLongMemberIdentifierKey", Long.MAX_VALUE);
        CborUtil.encode(encodingState, "leadershipTermTimestampNanos", -123_456_789_012_345L);
        CborUtil.encode(encodingState, "logPositionSnapshotState", TimeUnit.NANOSECONDS.name());
        CborUtil.encode(encodingState, "appendPositionCatchupTarget", 42L);
        CborUtil.encode(encodingState, "negativeCatchupOffsetValue", -0x12345678L);
        // Explicitly canTruncate = false to test that the key is dropped
        CborUtil.encode(encodingState, "reason", reason, false);
        CborUtil.encodeFooter(encodingState);
        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());

        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Map<String, Object> stringObjectMap = cborObjectMapper.readValue(
            data,
            new TypeReference<>()
            {
            });

        // Should have dropped reason
        assertEquals(Set.of(
            "veryLongMemberIdentifierKey",
            "leadershipTermTimestampNanos",
            "logPositionSnapshotState",
            "appendPositionCatchupTarget",
            "negativeCatchupOffsetValue"
        ), stringObjectMap.keySet());
        assertEquals(Long.MAX_VALUE, ((Number)stringObjectMap.get("veryLongMemberIdentifierKey")).longValue());
        assertEquals(
            -123_456_789_012_345L, ((Number)stringObjectMap.get("leadershipTermTimestampNanos")).longValue());
        assertEquals(TimeUnit.NANOSECONDS.name(), stringObjectMap.get("logPositionSnapshotState"));
        assertEquals(42L, ((Number)stringObjectMap.get("appendPositionCatchupTarget")).longValue());
        assertEquals(-0x12345678L, ((Number)stringObjectMap.get("negativeCatchupOffsetValue")).longValue());

    }
}