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
package io.aeron.cluster.logging;

import io.aeron.cluster.ElectionState;
import io.aeron.logging.CborDecode;
import io.aeron.logging.EventCodeType;
import io.aeron.logging.LoggerEventCallback;
import io.aeron.test.Tests;
import io.aeron.test.logging.ProxyLoggerEventCallback;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.stream.Stream;

import static io.aeron.logging.CborUtils.ENUM_TAG;
import static io.aeron.logging.CborUtils.NO_TAG;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class CborClusterEventCodecTest
{
    private final ManyToOneRingBuffer ringBuffer = new ManyToOneRingBuffer(
        new UnsafeBuffer(BufferUtil.allocateDirectAligned(64 * 1024 + TRAILER_LENGTH, CACHE_LINE_LENGTH)));

    @Test
    void encodeDecodeLogElectionStateChange()
    {
        final LoggerEventCallback mockLoggingCallback = mock(LoggerEventCallback.class);
        final CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(mockLoggingCallback)));
        final CborClusterEventLogger cborClusterEventLogger = new CborClusterEventLogger(ringBuffer);


        cborClusterEventLogger.logElectionStateChange(
            12,
            ElectionState.CANVASS,
            ElectionState.CLOSED,
            2342,
            23434,
            62354,
            2789345,
            87345,
            345345,
            2345,
            "invalid");

        while (0 == ringBuffer.read(cborDecode))
        {
            Tests.yield();
        }

        verify(mockLoggingCallback).onHeader(
            eq(EventCodeType.CLUSTER.getTypeCode()),
            eq(ClusterEventCode.ELECTION_STATE_CHANGE.id()),
            eq(ClusterEventCode.ELECTION_STATE_CHANGE.name()),
            anyLong());
        verify(mockLoggingCallback).onValue("memberId", NO_TAG, 12L);
        verify(mockLoggingCallback).onValue("oldState", ENUM_TAG, "CANVASS");
        verify(mockLoggingCallback).onValue("newState", ENUM_TAG, "CLOSED");
        verify(mockLoggingCallback).onValue("candidateTermId", NO_TAG, 23434L);
        verify(mockLoggingCallback).onValue("leadershipTermId", NO_TAG, 62354L);
        verify(mockLoggingCallback).onValue("logPosition", NO_TAG, 2789345L);
        verify(mockLoggingCallback).onValue("logLeadershipTermId", NO_TAG, 87345L);
        verify(mockLoggingCallback).onValue("appendPosition", NO_TAG, 345345L);
        verify(mockLoggingCallback).onValue("catchupPosition", NO_TAG, 2345L);
        verify(mockLoggingCallback).onValue("reason", NO_TAG, "invalid");
        verify(mockLoggingCallback).onFooter(false);
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
    void encodeDecodeLogElectionStateChangeWithBoundaryLongValues(final long value)
    {
        final LoggerEventCallback mockLoggingCallback = mock(LoggerEventCallback.class);
        final CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(mockLoggingCallback)));
        final CborClusterEventLogger cborClusterEventLogger = new CborClusterEventLogger(ringBuffer);

        cborClusterEventLogger.logElectionStateChange(
            12,
            ElectionState.CANVASS,
            ElectionState.CLOSED,
            2342,
            value,
            value,
            value,
            value,
            value,
            value,
            "invalid");

        while (0 == ringBuffer.read(cborDecode))
        {
            Tests.yield();
        }

        verify(mockLoggingCallback).onValue("candidateTermId", NO_TAG, value);
        verify(mockLoggingCallback).onValue("leadershipTermId", NO_TAG, value);
        verify(mockLoggingCallback).onValue("logPosition", NO_TAG, value);
        verify(mockLoggingCallback).onValue("logLeadershipTermId", NO_TAG, value);
        verify(mockLoggingCallback).onValue("appendPosition", NO_TAG, value);
        verify(mockLoggingCallback).onValue("catchupPosition", NO_TAG, value);
        verify(mockLoggingCallback).onFooter(false);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, -1, 1, 0x7F, 0x100, 0x10000, Integer.MAX_VALUE, Integer.MIN_VALUE, -0x7FFFFFFE })
    void encodeDecodeLogElectionStateChangeWithBoundaryIntValues(final int value)
    {
        final LoggerEventCallback mockLoggingCallback = mock(LoggerEventCallback.class);
        final CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(mockLoggingCallback)));
        final CborClusterEventLogger cborClusterEventLogger = new CborClusterEventLogger(ringBuffer);

        cborClusterEventLogger.logElectionStateChange(
            value,
            ElectionState.CANVASS,
            ElectionState.CLOSED,
            value,
            23434,
            62354,
            2789345,
            87345,
            345345,
            2345,
            "invalid");

        while (0 == ringBuffer.read(cborDecode))
        {
            Tests.yield();
        }

        verify(mockLoggingCallback).onValue("memberId", NO_TAG, (long)value);
        verify(mockLoggingCallback).onValue("leaderId", NO_TAG, (long)value);
        verify(mockLoggingCallback).onFooter(false);
    }

    static Stream<Arguments> generateReasonValues()
    {
        return Stream.of(
            Arguments.of("A".repeat(10)),
            Arguments.of("T".repeat(23)),
            Arguments.of("T".repeat(24)),
            Arguments.of("T".repeat(255)),
            Arguments.of("T".repeat(256)),
            Arguments.of("A".repeat(1000)),
            Arguments.of((String)null)
        );
    }

    @ParameterizedTest
    @MethodSource("generateReasonValues")
    void encodeDecodeLogElectionStateChangeWithVariousReasonLengths(final String reason)
    {
        final LoggerEventCallback mockLoggingCallback = mock(LoggerEventCallback.class);
        final CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(mockLoggingCallback)));
        final CborClusterEventLogger cborClusterEventLogger = new CborClusterEventLogger(ringBuffer);

        cborClusterEventLogger.logElectionStateChange(
            12,
            ElectionState.CANVASS,
            ElectionState.CLOSED,
            2342,
            23434,
            62354,
            2789345,
            87345,
            345345,
            2345,
            reason);

        while (0 == ringBuffer.read(cborDecode))
        {
            Tests.yield();
        }

        verify(mockLoggingCallback).onValue("reason", NO_TAG, reason);
        verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void encodeDecodeLogElectionStateChangeTruncatesReasonThatIsTooLong()
    {
        final LoggerEventCallback mockLoggingCallback = mock(LoggerEventCallback.class);
        final CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(mockLoggingCallback)));
        final CborClusterEventLogger cborClusterEventLogger = new CborClusterEventLogger(ringBuffer);

        final String reason = "R".repeat(10_000);

        cborClusterEventLogger.logElectionStateChange(
            12,
            ElectionState.CANVASS,
            ElectionState.CLOSED,
            2342,
            23434,
            62354,
            2789345,
            87345,
            345345,
            2345,
            reason);

        while (0 == ringBuffer.read(cborDecode))
        {
            Tests.yield();
        }

        verify(mockLoggingCallback).onValue("memberId", NO_TAG, 12L);
        verify(mockLoggingCallback).onValue("oldState", ENUM_TAG, "CANVASS");
        verify(mockLoggingCallback).onValue("newState", ENUM_TAG, "CLOSED");
        verify(mockLoggingCallback).onValue("leaderId", NO_TAG, 2342L);
        verify(mockLoggingCallback).onValue("candidateTermId", NO_TAG, 23434L);
        verify(mockLoggingCallback).onValue("leadershipTermId", NO_TAG, 62354L);
        verify(mockLoggingCallback).onValue("logPosition", NO_TAG, 2789345L);
        verify(mockLoggingCallback).onValue("logLeadershipTermId", NO_TAG, 87345L);
        verify(mockLoggingCallback).onValue("appendPosition", NO_TAG, 345345L);
        verify(mockLoggingCallback).onValue("catchupPosition", NO_TAG, 2345L);

        final ArgumentCaptor<String> reasonCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockLoggingCallback).onValue(eq("reason"), eq(NO_TAG), reasonCaptor.capture());
        final String truncatedReason = reasonCaptor.getValue();
        assertTrue(truncatedReason.endsWith("..."));
        assertTrue(truncatedReason.length() < reason.length());
        assertTrue(reason.startsWith(truncatedReason.substring(0, truncatedReason.length() - 3)));

        verify(mockLoggingCallback).onFooter(true);
    }

    @Test
    void logElectionStateChangeThrowsWhenMessageExceedsRingBufferCapacity()
    {
        final int dataCapacity = 64;
        final ManyToOneRingBuffer smallRingBuffer = new ManyToOneRingBuffer(
            new UnsafeBuffer(BufferUtil.allocateDirectAligned(dataCapacity + TRAILER_LENGTH, CACHE_LINE_LENGTH)));
        final CborClusterEventLogger cborClusterEventLogger = new CborClusterEventLogger(smallRingBuffer);

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            cborClusterEventLogger.logElectionStateChange(
                12,
                ElectionState.CANVASS,
                ElectionState.CLOSED,
                2342,
                23434,
                62354,
                2789345,
                87345,
                345345,
                2345,
                "invalid"));

        assertTrue(exception.getMessage().contains("exceeds maxMsgLength=8"));
    }
}