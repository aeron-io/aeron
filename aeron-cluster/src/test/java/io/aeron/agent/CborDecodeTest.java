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
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.cbor.CBORFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class CborDecodeTest
{
    static class ProxyLoggerEventCallback implements LoggerEventCallback
    {
        private final LoggerEventCallback delegate;

        ProxyLoggerEventCallback(final LoggerEventCallback delegate)
        {
            this.delegate = delegate;
        }

        public void onHeader(final int eventType, final int eventCode, final long timestamp)
        {
            this.delegate.onHeader(eventType, eventCode, timestamp);
        }

        public void onValue(final CharSequence name, final CharSequence value)
        {
            this.delegate.onValue(name.toString(), value.toString());
        }

        public void onValue(final CharSequence name, final long value)
        {
            this.delegate.onValue(name.toString(), value);
        }

        public void onFooter(final boolean truncated)
        {
            this.delegate.onFooter(truncated);
        }
    }
    @ParameterizedTest
    @ValueSource(longs = {
        2, 25, 0x7F, 0x100,
        0x1234, 0x7FFF, 0x10000,
        0x12345678, 0x7FFFFFFF, 0x80000000,
        0x123456789ABCDEF0L, 0xFFFFFFFFFFFFFFFFL,
        -2, -25, -0xFF,
        -0x1234, -0xFFFF,
        -0x12345678, -0x7FFFFFFE })
    void shouldDecodeNumberMessage(final long memberId)
    {
        final int offset = 0;
        final int length = 1024;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
        final LoggerEventCallback loggerEventCallback = mock(LoggerEventCallback.class);
        final long timestamp = 12643263L;

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);

        CborUtil.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, timestamp);
        CborUtil.encode(encodingState, "memberId", memberId);
        CborUtil.encodeFooter(encodingState);

        CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(loggerEventCallback)));
        cborDecode.onMessage(
            ClusterEventCode.ELECTION_STATE_CHANGE.toEventCodeId(),
            encodingState.buffer(),0, encodingState.offset());

        verify(loggerEventCallback).onHeader(
            EventCodeType.CLUSTER.getTypeCode(),
            ClusterEventCode.ELECTION_STATE_CHANGE.toEventCodeId(),
            0L);

        verify(loggerEventCallback).onValue("memberId", memberId);
        verify(loggerEventCallback).onFooter(false);
    }

    static Stream<Arguments> generateBigStrings()
    {
        return Stream.of(
            Arguments.of("A".repeat(10)),
            Arguments.of("A".repeat(1000)),
            Arguments.of("A".repeat(100_000))
        );
    }

    @ParameterizedTest
    @MethodSource("generateBigStrings")
    void shouldDecodeCharSequences(final String reason)
    {
        final int offset = 0;
        final int length = 200_000;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
        final LoggerEventCallback loggerEventCallback = mock(LoggerEventCallback.class);
        final long timestamp = 12643263L;

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);

        CborUtil.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, timestamp);
        CborUtil.encode(encodingState, "reason", reason);
        CborUtil.encodeFooter(encodingState);

        CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(loggerEventCallback)));
        cborDecode.onMessage(
            ClusterEventCode.ELECTION_STATE_CHANGE.toEventCodeId(),
            encodingState.buffer(),0, encodingState.offset());

        verify(loggerEventCallback).onHeader(
            EventCodeType.CLUSTER.getTypeCode(),
            ClusterEventCode.ELECTION_STATE_CHANGE.toEventCodeId(),
            0L);

        verify(loggerEventCallback).onValue("reason", reason);
        verify(loggerEventCallback).onFooter(false);
    }

    @Test
    void shouldDecodeMultikeyMessage()
    {
        final int offset = 0;
        final int length = 1000;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
        final LoggerEventCallback loggerEventCallback = mock(LoggerEventCallback.class);

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);
        CborUtil.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, 12643263L);
        CborUtil.encode(encodingState, "key1", 1_000_000_000L);
        CborUtil.encode(encodingState, "key2", "S".repeat(50));
        CborUtil.encode(encodingState, "key3", TimeUnit.DAYS.name());
        CborUtil.encodeFooter(encodingState);

        CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(loggerEventCallback)));
        cborDecode.onMessage(
            ClusterEventCode.ELECTION_STATE_CHANGE.toEventCodeId(),
            encodingState.buffer(), 0, encodingState.offset());

        verify(loggerEventCallback).onHeader(
            EventCodeType.CLUSTER.getTypeCode(),
            ClusterEventCode.ELECTION_STATE_CHANGE.toEventCodeId(),
            0L);

        verify(loggerEventCallback).onValue("key1", 1_000_000_000L);
        verify(loggerEventCallback).onValue("key2", "S".repeat(50));
        verify(loggerEventCallback).onValue("key3", TimeUnit.DAYS.name());
        verify(loggerEventCallback).onFooter(false);
    }

    @Test
    void shouldDecodeExtensiveMultikeyMessage()
    {
        final int offset = 0;
        // Generously sized so that every key below, including the ~1000-char string, encodes fully.
        final int length = 8192;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
        final LoggerEventCallback loggerEventCallback = mock(LoggerEventCallback.class);

        final long negativeCatchupOffsetValue = -0x12345678L;
        final long leadershipTermTimestampNanos = -123_456_789_012_345L;
        final long appendPositionCatchupTarget = 42L;
        final String candidateTermIdentifierValue = "R".repeat(1000);
        final String shortStringBoundary = "T".repeat(23);
        final String oneByteLengthBoundary = "T".repeat(24);
        final String twoByteLengthBoundary = "T".repeat(256);

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);
        CborUtil.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, 12643263L);
        CborUtil.encode(encodingState, "veryLongMemberIdentifierKey", Long.MAX_VALUE);
        CborUtil.encode(encodingState, "candidateTermIdentifierValue", candidateTermIdentifierValue);
        CborUtil.encode(encodingState, "leadershipTermTimestampNanos", leadershipTermTimestampNanos);
        CborUtil.encode(encodingState, "logPositionSnapshotState", TimeUnit.DAYS.name());
        CborUtil.encode(encodingState, "appendPositionCatchupTarget", appendPositionCatchupTarget);
        CborUtil.encode(encodingState, "negativeCatchupOffsetValue", negativeCatchupOffsetValue);
        CborUtil.encode(encodingState, "smallPositiveBoundary", 0x7FL);
        CborUtil.encode(encodingState, "oneByteBoundary", 0x100L);
        CborUtil.encode(encodingState, "twoBytePositiveBoundary", 0x7FFFL);
        CborUtil.encode(encodingState, "twoByteBoundary", 0x10000L);
        CborUtil.encode(encodingState, "fourBytePositiveBoundary", 0x7FFFFFFFL);
        CborUtil.encode(encodingState, "fourByteBoundary", 0x80000000L);
        CborUtil.encode(encodingState, "smallNegativeBoundary", -2L);
        CborUtil.encode(encodingState, "twoByteNegativeBoundary", -0xFFFFL);
        CborUtil.encode(encodingState, "shortStringBoundary", shortStringBoundary);
        CborUtil.encode(encodingState, "oneByteLengthBoundary", oneByteLengthBoundary);
        CborUtil.encode(encodingState, "twoByteLengthBoundary", twoByteLengthBoundary);
        CborUtil.encode(encodingState, "replicationUnit", TimeUnit.NANOSECONDS.name());
        CborUtil.encodeFooter(encodingState);

        assertFalse(encodingState.isReachedLimit());

        CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(loggerEventCallback)));
        cborDecode.onMessage(
            ClusterEventCode.ELECTION_STATE_CHANGE.toEventCodeId(),
            encodingState.buffer(), 0, encodingState.offset());

        verify(loggerEventCallback).onHeader(
            EventCodeType.CLUSTER.getTypeCode(),
            ClusterEventCode.ELECTION_STATE_CHANGE.toEventCodeId(),
            0L);

        verify(loggerEventCallback).onValue("veryLongMemberIdentifierKey", Long.MAX_VALUE);
        verify(loggerEventCallback).onValue("candidateTermIdentifierValue", candidateTermIdentifierValue);
        verify(loggerEventCallback).onValue("leadershipTermTimestampNanos", leadershipTermTimestampNanos);
        verify(loggerEventCallback).onValue("logPositionSnapshotState", TimeUnit.DAYS.name());
        verify(loggerEventCallback).onValue("appendPositionCatchupTarget", appendPositionCatchupTarget);
        verify(loggerEventCallback).onValue("negativeCatchupOffsetValue", negativeCatchupOffsetValue);
        verify(loggerEventCallback).onValue("smallPositiveBoundary", 0x7FL);
        verify(loggerEventCallback).onValue("oneByteBoundary", 0x100L);
        verify(loggerEventCallback).onValue("twoBytePositiveBoundary", 0x7FFFL);
        verify(loggerEventCallback).onValue("twoByteBoundary", 0x10000L);
        verify(loggerEventCallback).onValue("fourBytePositiveBoundary", 0x7FFFFFFFL);
        verify(loggerEventCallback).onValue("fourByteBoundary", 0x80000000L);
        verify(loggerEventCallback).onValue("smallNegativeBoundary", -2L);
        verify(loggerEventCallback).onValue("twoByteNegativeBoundary", -0xFFFFL);
        verify(loggerEventCallback).onValue("shortStringBoundary", shortStringBoundary);
        verify(loggerEventCallback).onValue("oneByteLengthBoundary", oneByteLengthBoundary);
        verify(loggerEventCallback).onValue("twoByteLengthBoundary", twoByteLengthBoundary);
        verify(loggerEventCallback).onValue("replicationUnit", TimeUnit.NANOSECONDS.name());
        verify(loggerEventCallback).onFooter(false);
    }
//    @Test
//    void shouldPartiallyEncodeMultikeyMessageThatIsTooLong()
//    {
//        final int offset = 0;
//        // Based on by-hand calculation, around 22 bytes are needed for key1 and key3 preservation
//        final int length = 26;
//        final Set<String> expectedMessageKeySet = Set.of("key1", "key3");
//        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
//
//        final EncodingState encodingState = new EncodingState();
//        encodingState.reset(buffer, offset, length);
//        CborUtil.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, 12643263L);
//        CborUtil.encode(encodingState, "key1", 1_000_000_000L);
//        CborUtil.encode(encodingState, "key2", "S".repeat(1_000_000));
//        CborUtil.encode(encodingState, "key3", TimeUnit.DAYS.name());
//        CborUtil.encodeFooter(encodingState);
//        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());
//
//        final byte[] data = new byte[encodingState.offset()];
//        encodingState.buffer().getBytes(0, data);
//
//        final Map<String, Object> stringObjectMap = cborObjectMapper.readValue(
//            data,
//            new TypeReference<>()
//            {
//            });
//        assertEquals(expectedMessageKeySet, stringObjectMap.keySet());
//    }
//
//    @Test
//    void shouldPartiallyEncodeMultikeyMessageThatIsTooLongForFooter()
//    {
//        final int offset = 0;
//        // key3 should be dropped here, because the footer cannot fit.
//        // This may break if the footer schema is changed.
//        final int length = 21;
//        final Set<String> expectedMessageKeySet = Set.of("key1");
//        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
//
//        final EncodingState encodingState = new EncodingState();
//        encodingState.reset(buffer, offset, length);
//        CborUtil.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, 12643263L);
//        CborUtil.encode(encodingState, "key1", 1_000_000_000L);
//        CborUtil.encode(encodingState, "key2", "S".repeat(1_000_000));
//        CborUtil.encode(encodingState, "key3", TimeUnit.DAYS.name());
//        CborUtil.encodeFooter(encodingState);
//        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());
//
//        final byte[] data = new byte[encodingState.offset()];
//        encodingState.buffer().getBytes(0, data);
//
//        final Map<String, Object> stringObjectMap = cborObjectMapper.readValue(
//            data,
//            new TypeReference<>()
//            {
//            });
//        assertEquals(expectedMessageKeySet, stringObjectMap.keySet());
//    }
//
//    @Test
//    void shouldPartiallyEncodeExtensiveMultikeyMessageThatIsTooLong()
//    {
//        final int offset = 0;
//        // A relatively large buffer that comfortably fits every key except the oversized
//        // "candidateTermIdentifierValue" string value, which alone needs ~5KB and is dropped
//        // independently of the other keys, which still get encoded after it.
//        final int length = 512;
//        final Set<String> expectedMessageKeySet = Set.of(
//            "veryLongMemberIdentifierKey",
//            "leadershipTermTimestampNanos",
//            "logPositionSnapshotState",
//            "appendPositionCatchupTarget",
//            "negativeCatchupOffsetValue");
//        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
//
//        final EncodingState encodingState = new EncodingState();
//        encodingState.reset(buffer, offset, length);
//        CborUtil.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, 12643263L);
//        CborUtil.encode(encodingState, "veryLongMemberIdentifierKey", Long.MAX_VALUE);
//        CborUtil.encode(encodingState, "candidateTermIdentifierValue", "R".repeat(5_000));
//        CborUtil.encode(encodingState, "leadershipTermTimestampNanos", -123_456_789_012_345L);
//        CborUtil.encode(encodingState, "logPositionSnapshotState", TimeUnit.NANOSECONDS.name());
//        CborUtil.encode(encodingState, "appendPositionCatchupTarget", 42L);
//        CborUtil.encode(encodingState, "negativeCatchupOffsetValue", -0x12345678L);
//        CborUtil.encodeFooter(encodingState);
//        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());
//
//        final byte[] data = new byte[encodingState.offset()];
//        encodingState.buffer().getBytes(0, data);
//
//        final Map<String, Object> stringObjectMap = cborObjectMapper.readValue(
//            data,
//            new TypeReference<>()
//            {
//            });
//        assertEquals(expectedMessageKeySet, stringObjectMap.keySet());
//    }
}