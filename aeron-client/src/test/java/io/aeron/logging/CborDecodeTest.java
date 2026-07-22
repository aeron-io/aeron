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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

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
            if (value == null)
            {
                this.delegate.onValue(name.toString(), null);
                return;
            }

            this.delegate.onValue(name.toString(), value.toString());
        }

        public void onValue(final CharSequence name, final long value)
        {
            this.delegate.onValue(name.toString(), value);
        }

        public void onValue(final CharSequence name, final boolean value)
        {
            this.delegate.onValue(name.toString(), value);
        }

        public void onFooter(final boolean truncated)
        {
            this.delegate.onFooter(truncated);
        }
    }

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
    void shouldDecodeNumberMessage(final long memberId)
    {
        final int offset = 0;
        final int length = 1024;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
        final LoggerEventCallback loggerEventCallback = mock(LoggerEventCallback.class);
        final long timestamp = 12643263L;

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);

        CborEncode.encodeHeader(encodingState, TEST_EVENT_CODE, timestamp);
        CborEncode.encode(encodingState, "memberId", memberId);
        CborEncode.encodeFooter(encodingState);

        final CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(loggerEventCallback)));
        cborDecode.onMessage(
            TEST_EVENT_CODE.toEventCodeId(),
            encodingState.buffer(), 0, encodingState.offset());

        verify(loggerEventCallback).onHeader(
            TEST_EVENT_CODE.eventCode(),
            TEST_EVENT_CODE.id(),
            timestamp);

        verify(loggerEventCallback).onValue("memberId", memberId);
        verify(loggerEventCallback).onFooter(false);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldDecodeBooleanMessage(final boolean booleanValue)
    {
        final int offset = 0;
        final int length = 1024;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
        final LoggerEventCallback loggerEventCallback = mock(LoggerEventCallback.class);
        final long timestamp = 12643263L;

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);

        CborEncode.encodeHeader(encodingState, TEST_EVENT_CODE, timestamp);
        CborEncode.encode(encodingState, "booleanValue", booleanValue);
        CborEncode.encodeFooter(encodingState);

        final CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(loggerEventCallback)));
        cborDecode.onMessage(
            TEST_EVENT_CODE.toEventCodeId(),
            encodingState.buffer(), 0, encodingState.offset());

        verify(loggerEventCallback).onHeader(
            TEST_EVENT_CODE.eventCode(),
            TEST_EVENT_CODE.id(),
            timestamp);

        verify(loggerEventCallback).onValue("booleanValue", booleanValue);
        verify(loggerEventCallback).onFooter(false);
    }

    static Stream<Arguments> generateStringsAndNull()
    {
        return Stream.of(
            Arguments.of("A".repeat(10)),
            Arguments.of("A".repeat(1000)),
            Arguments.of("A".repeat(100_000)),
            Arguments.of((CharSequence)null)
        );
    }

    @ParameterizedTest
    @MethodSource("generateStringsAndNull")
    void shouldDecodeCharSequencesAndNull(final String reason)
    {
        final int offset = 0;
        final int length = 200_000;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
        final LoggerEventCallback loggerEventCallback = mock(LoggerEventCallback.class);
        final long timestamp = 12643263L;

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);

        CborEncode.encodeHeader(encodingState, TEST_EVENT_CODE, timestamp);
        CborEncode.encode(encodingState, "reason", reason);
        CborEncode.encodeFooter(encodingState);

        final CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(loggerEventCallback)));
        cborDecode.onMessage(
            TEST_EVENT_CODE.toEventCodeId(),
            encodingState.buffer(), 0, encodingState.offset());

        verify(loggerEventCallback).onHeader(
            TEST_EVENT_CODE.eventCode(),
            TEST_EVENT_CODE.id(),
            timestamp);

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
        final long timestamp = 12643263L;
        CborEncode.encodeHeader(encodingState, TEST_EVENT_CODE, timestamp);
        CborEncode.encode(encodingState, "key1", 1_000_000_000L);
        CborEncode.encode(encodingState, "key2", "S".repeat(50));
        CborEncode.encode(encodingState, "key3", TimeUnit.DAYS.name());
        CborEncode.encodeFooter(encodingState);

        final CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(loggerEventCallback)));
        cborDecode.onMessage(
            TEST_EVENT_CODE.toEventCodeId(),
            encodingState.buffer(), 0, encodingState.offset());

        verify(loggerEventCallback).onHeader(
            TEST_EVENT_CODE.eventCode(),
            TEST_EVENT_CODE.id(),
            timestamp);

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
        final long timestamp = 12643263L;
        CborEncode.encodeHeader(encodingState, TEST_EVENT_CODE, timestamp);
        CborEncode.encode(encodingState, "veryLongMemberIdentifierKey", Long.MAX_VALUE);
        CborEncode.encode(encodingState, "candidateTermIdentifierValue", candidateTermIdentifierValue);
        CborEncode.encode(encodingState, "leadershipTermTimestampNanos", leadershipTermTimestampNanos);
        CborEncode.encode(encodingState, "logPositionSnapshotState", TimeUnit.DAYS.name());
        CborEncode.encode(encodingState, "appendPositionCatchupTarget", appendPositionCatchupTarget);
        CborEncode.encode(encodingState, "negativeCatchupOffsetValue", negativeCatchupOffsetValue);
        CborEncode.encode(encodingState, "smallPositiveBoundary", 0x7FL);
        CborEncode.encode(encodingState, "oneByteBoundary", 0x100L);
        CborEncode.encode(encodingState, "twoBytePositiveBoundary", 0x7FFFL);
        CborEncode.encode(encodingState, "twoByteBoundary", 0x10000L);
        CborEncode.encode(encodingState, "fourBytePositiveBoundary", 0x7FFFFFFFL);
        CborEncode.encode(encodingState, "fourByteBoundary", 0x80000000L);
        CborEncode.encode(encodingState, "smallNegativeBoundary", -2L);
        CborEncode.encode(encodingState, "twoByteNegativeBoundary", -0xFFFFL);
        CborEncode.encode(encodingState, "shortStringBoundary", shortStringBoundary);
        CborEncode.encode(encodingState, "oneByteLengthBoundary", oneByteLengthBoundary);
        CborEncode.encode(encodingState, "twoByteLengthBoundary", twoByteLengthBoundary);
        CborEncode.encode(encodingState, "replicationUnit", TimeUnit.NANOSECONDS.name());
        CborEncode.encodeFooter(encodingState);

        Assertions.assertFalse(encodingState.isReachedLimit());

        final CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(loggerEventCallback)));
        cborDecode.onMessage(
            TEST_EVENT_CODE.toEventCodeId(),
            encodingState.buffer(), 0, encodingState.offset());

        verify(loggerEventCallback).onHeader(
            TEST_EVENT_CODE.eventCode(),
            TEST_EVENT_CODE.id(),
            timestamp);

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

    @ParameterizedTest
    @ValueSource(ints = {0, 2, 6, 9, 12, 14, 18, 27, 35, 60, 78, 82, 87, 89})
    void shouldReceiveInvalidMessageIfEndOfBufferIsReachedBeforeTermination(final int cutoffPoint)
    {
        final int offset = 0;
        final int length = 1000;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
        final LoggerEventCallback loggerEventCallback = mock(LoggerEventCallback.class);

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);
        CborEncode.encodeHeader(encodingState, TEST_EVENT_CODE, 12643263L);
        CborEncode.encode(encodingState, "key1", 1_000_000_000L);
        CborEncode.encode(encodingState, "key2", "S".repeat(50));
        CborEncode.encode(encodingState, "key3", TimeUnit.DAYS.name());
        CborEncode.encodeFooter(encodingState);

        final CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(loggerEventCallback)));
        final byte[] partialBytes = new byte[cutoffPoint];
        buffer.getBytes(0, partialBytes, 0, cutoffPoint);
        final UnsafeBuffer partialBuffer = new UnsafeBuffer(partialBytes);

        // TODO: Might change this whole assertion block to something less implementation-specific
        final RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () ->
            cborDecode.onMessage(
                TEST_EVENT_CODE.toEventCodeId(),
                partialBuffer,
                0,
                cutoffPoint
            )
        );
        final Throwable cause = exception.getCause();
        Assertions.assertInstanceOf(InvalidMessage.class, cause);
        Assertions.assertEquals("ERROR - Terminated prematurely", cause.getMessage());
    }

    @Test
    @Disabled
    void shouldTriggerTruncatedFooterFlagWhenMessageWasTruncatedDuringEncode()
    {
        final int offset = 0;
        final int length = 40;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
        final LoggerEventCallback loggerEventCallback = mock(LoggerEventCallback.class);

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);
        CborEncode.encodeHeader(encodingState, TEST_EVENT_CODE, 12643263L);
        CborEncode.encode(encodingState, "key1", 1_000_000_000L);
        CborEncode.encode(encodingState, "key3", TimeUnit.DAYS.name());
        CborEncode.encode(encodingState, "key2", "S".repeat(1_000_000));
        CborEncode.encodeFooter(encodingState);

        final CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(loggerEventCallback)));
        cborDecode.onMessage(
            TEST_EVENT_CODE.toEventCodeId(),
            encodingState.buffer(), 0, encodingState.offset());

        verify(loggerEventCallback).onFooter(true);
    }
}