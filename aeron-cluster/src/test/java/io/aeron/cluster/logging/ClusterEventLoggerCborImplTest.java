/*
 * Copyright 2014-2026 Real Logic Limited.
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
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.logging.CborDecode;
import io.aeron.logging.EventCodeType;
import io.aeron.logging.LoggerEventCallback;
import io.aeron.test.Tests;
import io.aeron.test.logging.ProxyLoggerEventCallback;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.aeron.logging.CborUtils.ENUM_TAG;
import static io.aeron.logging.CborUtils.NO_TAG;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ClusterEventLoggerCborImplTest
{
    private final ManyToOneRingBuffer ringBuffer = new ManyToOneRingBuffer(
        new UnsafeBuffer(BufferUtil.allocateDirectAligned(64 * 1024 + TRAILER_LENGTH, CACHE_LINE_LENGTH)));

    private final LoggerEventCallback mockLoggingCallback = mock(LoggerEventCallback.class);
    private final CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(mockLoggingCallback)));
    private final ClusterTracer logger = new CborClusterTracer(ringBuffer);

    private void drain()
    {
        while (0 == ringBuffer.read(cborDecode))
        {
            Tests.yield();
        }
    }

    @Test
    void logElectionStateChangeMatchesTheHandWrittenTemplateShape()
    {
        logger.logElectionStateChange(
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

        drain();

        final InOrder inOrder = Mockito.inOrder(mockLoggingCallback);
        inOrder.verify(mockLoggingCallback).onHeader(
            eq(EventCodeType.CLUSTER.getTypeCode()),
            eq(ClusterEventCode.ELECTION_STATE_CHANGE.id()),
            eq(ClusterEventCode.ELECTION_STATE_CHANGE.name()),
            anyLong());
        inOrder.verify(mockLoggingCallback).onValue("memberId", NO_TAG, 12L);
        inOrder.verify(mockLoggingCallback).onValue("oldState", ENUM_TAG, "CANVASS");
        inOrder.verify(mockLoggingCallback).onValue("newState", ENUM_TAG, "CLOSED");
        inOrder.verify(mockLoggingCallback).onValue("leaderId", NO_TAG, 2342L);
        inOrder.verify(mockLoggingCallback).onValue("candidateTermId", NO_TAG, 23434L);
        inOrder.verify(mockLoggingCallback).onValue("leadershipTermId", NO_TAG, 62354L);
        inOrder.verify(mockLoggingCallback).onValue("logPosition", NO_TAG, 2789345L);
        inOrder.verify(mockLoggingCallback).onValue("logLeadershipTermId", NO_TAG, 87345L);
        inOrder.verify(mockLoggingCallback).onValue("appendPosition", NO_TAG, 345345L);
        inOrder.verify(mockLoggingCallback).onValue("catchupPosition", NO_TAG, 2345L);
        inOrder.verify(mockLoggingCallback).onValue("reason", NO_TAG, "invalid");
        inOrder.verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logElectionStateChangeTruncatesReasonThatIsTooLong()
    {
        final String reason = "R".repeat(10_000);

        logger.logElectionStateChange(
            12, ElectionState.CANVASS, ElectionState.CLOSED, 2342, 23434, 62354, 2789345, 87345, 345345, 2345,
            reason);

        drain();

        final ArgumentCaptor<String> reasonCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockLoggingCallback).onValue(eq("reason"), eq(NO_TAG), reasonCaptor.capture());
        final String truncatedReason = reasonCaptor.getValue();
        assertTrue(truncatedReason.endsWith("..."));
        assertTrue(truncatedReason.length() < reason.length());
        verify(mockLoggingCallback).onFooter(true);
    }

    @Test
    void logAppendSessionCloseEncodesCloseReasonAndTimeUnitAsEnumTag()
    {
        logger.logAppendSessionClose(7, 555L, CloseReason.CLIENT_ACTION, 99L, 123456789L, TimeUnit.MICROSECONDS);

        drain();

        final InOrder inOrder = Mockito.inOrder(mockLoggingCallback);
        inOrder.verify(mockLoggingCallback).onHeader(
            eq(EventCodeType.CLUSTER.getTypeCode()),
            eq(ClusterEventCode.APPEND_SESSION_CLOSE.id()),
            eq(ClusterEventCode.APPEND_SESSION_CLOSE.name()),
            anyLong());
        inOrder.verify(mockLoggingCallback).onValue("memberId", NO_TAG, 7L);
        inOrder.verify(mockLoggingCallback).onValue("sessionId", NO_TAG, 555L);
        inOrder.verify(mockLoggingCallback).onValue("closeReason", ENUM_TAG, "CLIENT_ACTION");
        inOrder.verify(mockLoggingCallback).onValue("leadershipTermId", NO_TAG, 99L);
        inOrder.verify(mockLoggingCallback).onValue("timestamp", NO_TAG, 123456789L);
        inOrder.verify(mockLoggingCallback).onValue("timeUnit", ENUM_TAG, "MICROSECONDS");
        inOrder.verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logOnVoteEncodesBooleanFieldWithNoTagInDeclaredParameterOrder()
    {
        logger.logOnVote(1, 10L, 20L, 30L, 2, 3, true);

        drain();

        final InOrder inOrder = Mockito.inOrder(mockLoggingCallback);
        inOrder.verify(mockLoggingCallback).onHeader(
            eq(EventCodeType.CLUSTER.getTypeCode()),
            eq(ClusterEventCode.VOTE.id()),
            eq(ClusterEventCode.VOTE.name()),
            anyLong());
        inOrder.verify(mockLoggingCallback).onValue("memberId", NO_TAG, 1L);
        inOrder.verify(mockLoggingCallback).onValue("logLeadershipTermId", NO_TAG, 10L);
        inOrder.verify(mockLoggingCallback).onValue("logPosition", NO_TAG, 20L);
        inOrder.verify(mockLoggingCallback).onValue("candidateTermId", NO_TAG, 30L);
        inOrder.verify(mockLoggingCallback).onValue("candidateId", NO_TAG, 2L);
        inOrder.verify(mockLoggingCallback).onValue("voterId", NO_TAG, 3L);
        inOrder.verify(mockLoggingCallback).onValue("vote", NO_TAG, true);
        inOrder.verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logStateChangeUsesTheDynamicallyPassedEventCodeForTheHeader()
    {
        logger.logStateChange(4, ElectionState.NOMINATE, ElectionState.LEADER_READY, "because");

        drain();

        final InOrder inOrder = Mockito.inOrder(mockLoggingCallback);
        inOrder.verify(mockLoggingCallback).onHeader(
            eq(EventCodeType.CLUSTER.getTypeCode()),
            eq(ClusterEventCode.STATE_CHANGE.id()),
            eq(ClusterEventCode.STATE_CHANGE.name()),
            anyLong());
        inOrder.verify(mockLoggingCallback).onValue("memberId", NO_TAG, 4L);
        inOrder.verify(mockLoggingCallback).onValue("oldState", ENUM_TAG, "NOMINATE");
        inOrder.verify(mockLoggingCallback).onValue("newState", ENUM_TAG, "LEADER_READY");
        inOrder.verify(mockLoggingCallback).onValue("reason", NO_TAG, "because");
        inOrder.verify(mockLoggingCallback).onFooter(false);
    }
}
