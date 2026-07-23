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

package io.aeron.logging;

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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.aeron.logging.CborUtils.NO_TAG;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class CborEncodeTest
{
    private static final TestEventCode TEST_EVENT_CODE = new TestEventCode(2, 1);

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

        CborEncode.encodeHeader(encodingState, TEST_EVENT_CODE, timestamp);
        CborEncode.encode(encodingState, "memberId", memberId);
        CborEncode.encodeFooter(encodingState);

        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());

        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Object[] entries = cborObjectMapper.readValue(data, new TypeReference<>()
        {
        });

        final long actualTimestamp = ((Number)entries[0]).longValue();
        final int actualEventCode = ((Number)entries[1]).intValue();
        @SuppressWarnings("unchecked") final Map<String, Object> stringObjectMap = (Map<String, Object>)entries[2];

        assertEquals(timestamp, actualTimestamp);
        assertEquals(TEST_EVENT_CODE.toEventCodeId(), actualEventCode);
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

        CborEncode.encodeHeader(encodingState, TEST_EVENT_CODE, timestamp);
        CborEncode.encode(encodingState, "boolean", val);
        CborEncode.encodeFooter(encodingState);

        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());

        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Object[] entries = cborObjectMapper.readValue(data, new TypeReference<>()
        {
        });

        @SuppressWarnings("unchecked") final Map<String, Object> stringObjectMap = (Map<String, Object>)entries[2];

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

        CborEncode.encodeHeader(encodingState, TEST_EVENT_CODE, timestamp);
        CborEncode.encode(encodingState, "reason", reason);
        CborEncode.encodeFooter(encodingState);

        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());

        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Object[] entries = cborObjectMapper.readValue(data, new TypeReference<>()
        {
        });

        @SuppressWarnings("unchecked") final Map<String, Object> stringObjectMap = (Map<String, Object>)entries[2];

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
        assertEquals(expectedLength, CborEncode.lengthNumber(number));
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
        assertEquals(expectedLength, CborEncode.lengthString(text));
    }

    @Test
    void shouldEncodeTruncateMessageThatIsTooLong()
    {
        final int offset = 0;
        final int length = 100;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);
        CborEncode.encodeHeader(encodingState, TEST_EVENT_CODE, 12643263L);
        CborEncode.encode(encodingState, "key1", 1_000_000_000L);
        CborEncode.encode(encodingState, "key2", TimeUnit.DAYS.name());
        CborEncode.encode(encodingState, "key3", "S".repeat(1_000_000));

        CborEncode.encodeFooter(encodingState);
        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());

        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Object[] entries = cborObjectMapper.readValue(data, new TypeReference<>()
        {
        });

        @SuppressWarnings("unchecked") final Map<String, Object> stringObjectMap = (Map<String, Object>)entries[2];

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
        final long timestamp = 12643263L;
        final int length = 39 + CborEncode.headerLength(TEST_EVENT_CODE, timestamp);
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);
        CborEncode.encodeHeader(encodingState, TEST_EVENT_CODE, timestamp);
        CborEncode.encode(encodingState, "key1", 1_000_000_000L);
        CborEncode.encode(encodingState, "key3", TimeUnit.DAYS.name());
        CborEncode.encode(encodingState, "key2", "S".repeat(1_000_000));
        CborEncode.encodeFooter(encodingState);
        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());

        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Object[] entries = cborObjectMapper.readValue(data, new TypeReference<>()
        {
        });

        @SuppressWarnings("unchecked") final Map<String, Object> stringObjectMap = (Map<String, Object>)entries[2];

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
        CborEncode.encodeHeader(encodingState, TEST_EVENT_CODE, 12643263L);
        CborEncode.encode(encodingState, "veryLongMemberIdentifierKey", Long.MAX_VALUE);
        CborEncode.encode(encodingState, "leadershipTermTimestampNanos", -123_456_789_012_345L);
        CborEncode.encode(encodingState, "logPositionSnapshotState", TimeUnit.NANOSECONDS.name());
        CborEncode.encode(encodingState, "appendPositionCatchupTarget", 42L);
        CborEncode.encode(encodingState, "negativeCatchupOffsetValue", -0x12345678L);
        CborEncode.encode(encodingState, "reason", reason);
        CborEncode.encodeFooter(encodingState);
        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());

        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Object[] entries = cborObjectMapper.readValue(data, new TypeReference<>()
        {
        });

        @SuppressWarnings("unchecked") final Map<String, Object> stringObjectMap = (Map<String, Object>)entries[2];

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
        CborEncode.encodeHeader(encodingState, TEST_EVENT_CODE, 12643263L);
        CborEncode.encode(encodingState, "veryLongMemberIdentifierKey", Long.MAX_VALUE);
        CborEncode.encode(encodingState, "leadershipTermTimestampNanos", -123_456_789_012_345L);
        CborEncode.encode(encodingState, "logPositionSnapshotState", TimeUnit.NANOSECONDS.name());
        CborEncode.encode(encodingState, "appendPositionCatchupTarget", 42L);
        CborEncode.encode(encodingState, "negativeCatchupOffsetValue", -0x12345678L);
        // Explicitly canTruncate = false to test that the key is dropped
        CborEncode.encode(encodingState, "reason", reason, false);
        CborEncode.encodeFooter(encodingState);
        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());

        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Object[] entries = cborObjectMapper.readValue(data, new TypeReference<>()
        {
        });

        @SuppressWarnings("unchecked") final Map<String, Object> stringObjectMap = (Map<String, Object>)entries[2];

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

    @Test
    void shouldEncodeEmptyString()
    {
        final int offset = 0;
        final int length = 1024;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);

        CborEncode.encodeHeader(encodingState, TEST_EVENT_CODE, 12643263L);
        CborEncode.encode(encodingState, "reason", "");
        CborEncode.encodeFooter(encodingState);

        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());

        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Object[] entries = cborObjectMapper.readValue(data, new TypeReference<>()
        {
        });

        @SuppressWarnings("unchecked") final Map<String, Object> stringObjectMap = (Map<String, Object>)entries[2];

        assertEquals("", stringObjectMap.get("reason"));
    }

    @Test
    void shouldSilentlyDropEntryWhenRemainingSpaceCannotFitLengthByte()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, 0, 7);
        CborEncode.encodeHeader(encodingState, TEST_EVENT_CODE, 12643263L);
        CborEncode.encode(encodingState, "a", "S".repeat(100_000));
        CborEncode.encodeFooter(encodingState);

        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());
        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Object[] entries = cborObjectMapper.readValue(data, new TypeReference<>()
        {
        });

        @SuppressWarnings("unchecked") final Map<String, Object> stringObjectMap = (Map<String, Object>)entries[2];

        assertFalse(stringObjectMap.containsKey("a"));
    }

    @Test
    void shouldDropBooleanFieldWhenItCannotFit()
    {
        final long timestamp = 12643263L;
        final int length = 3 + CborEncode.headerLength(TEST_EVENT_CODE, timestamp);
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, 0, length);
        CborEncode.encodeHeader(encodingState, TEST_EVENT_CODE, timestamp);
        CborEncode.encode(encodingState, "k", true);

        // Footer should be written properly since k should be dropped completely
        CborEncode.encodeFooter(encodingState);
        assertTrue(encodingState.isReachedLimit());
        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());
        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Object[] entries = cborObjectMapper.readValue(data, new TypeReference<>()
        {
        });

        @SuppressWarnings("unchecked") final Map<String, Object> stringObjectMap = (Map<String, Object>)entries[2];

        assertFalse(stringObjectMap.containsKey("k"));
    }

    @Test
    void shouldEncodeMultikeyMessageWithTags()
    {
        final int offset = 0;
        final int length = 1000;
        final long lowerBitTag = 1 << 5;
        final long higherBitTag = 1 << 20;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
        final LoggerEventCallback loggerEventCallback = mock(LoggerEventCallback.class);

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);
        final long timestamp = 12643263L;
        CborEncode.encodeHeader(encodingState, TEST_EVENT_CODE, timestamp);
        CborEncode.encodeTag(encodingState, lowerBitTag);
        CborEncode.encode(encodingState, "key1", 1_000_000_000L);
        CborEncode.encodeTag(encodingState, higherBitTag);
        CborEncode.encode(encodingState, "key2", "S".repeat(50));
        CborEncode.encode(encodingState, "key3", TimeUnit.DAYS.name());
        CborEncode.encodeFooter(encodingState);

        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());
        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Object[] entries = cborObjectMapper.readValue(data, new TypeReference<>()
        {
        });

        System.out.println(Arrays.toString(entries));
    }
}