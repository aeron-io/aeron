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

import java.util.List;

import static io.aeron.logging.CborUtils.ENUM_TAG;
import static io.aeron.logging.CborUtils.NO_TAG;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
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
            anyLong()
        );
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
}