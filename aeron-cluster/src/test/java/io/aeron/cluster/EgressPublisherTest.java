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
package io.aeron.cluster;

import io.aeron.Publication;
import io.aeron.cluster.codecs.AdminRequestType;
import io.aeron.cluster.codecs.AdminResponseCode;
import io.aeron.cluster.codecs.AdminResponseDecoder;
import io.aeron.cluster.codecs.ChallengeDecoder;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.NewLeaderEventDecoder;
import io.aeron.cluster.codecs.SessionEventDecoder;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.test.Tests;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.aeron.Publication.ADMIN_ACTION;
import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.Publication.NOT_CONNECTED;
import static io.aeron.cluster.client.AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EgressPublisherTest
{
    private static final long LEADER_HEARTBEAT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);
    private final EgressPublisher egressPublisher = new EgressPublisher(LEADER_HEARTBEAT_TIMEOUT_NS);
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ClusterSession session = mock(ClusterSession.class);
    private final Publication responsePublication = mock(Publication.class);

    @Test
    void sendEventShouldUseTryClaimIfEventIsSmall()
    {
        final long leadershipTermId = 5;
        final long clusterSessionId = -473924;
        final long correlationId = 147239476238948923L;
        final int leaderMemberId = 0;
        final EventCode eventCode = EventCode.AUTHENTICATION_REJECTED;
        final String detail = "test";
        when(responsePublication.maxPayloadLength()).thenReturn(2000);
        when(session.responsePublication()).thenReturn(responsePublication);
        when(session.tryClaim(anyInt(), any()))
            .thenReturn(BACK_PRESSURED)
            .thenAnswer((invocation) ->
            {
                final int totalLength = (int)invocation.getArgument(0) + HEADER_LENGTH;
                final BufferClaim bufferClaim = invocation.getArgument(1);
                bufferClaim.wrap(buffer, 0, totalLength);
                return 1L;
            });
        when(session.id()).thenReturn(clusterSessionId);
        when(session.correlationId()).thenReturn(correlationId);

        assertTrue(egressPublisher.sendEvent(session, leadershipTermId, leaderMemberId, eventCode, detail));

        verify(session).responsePublication();
        verify(responsePublication).maxPayloadLength();
        verify(session, times(2)).tryClaim(anyInt(), any());

        verifySessionEvent(
            buffer,
            HEADER_LENGTH,
            clusterSessionId,
            correlationId,
            leadershipTermId,
            leaderMemberId,
            eventCode,
            detail);
    }

    @Test
    void sendEventShouldUseOfferWhenEventIsTooBig()
    {
        final long leadershipTermId = 42;
        final long clusterSessionId = 11;
        final long correlationId = -100;
        final int leaderMemberId = 4;
        final EventCode eventCode = EventCode.ERROR;
        final String detail = Tests.generateStringWithSuffix("error", "x", 1000);
        when(responsePublication.maxPayloadLength()).thenReturn(100);
        when(session.responsePublication()).thenReturn(responsePublication);
        when(session.offer(any(), anyInt(), anyInt()))
            .thenReturn(BACK_PRESSURED, ADMIN_ACTION, 5L);
        when(session.id()).thenReturn(clusterSessionId);
        when(session.correlationId()).thenReturn(correlationId);

        assertTrue(egressPublisher.sendEvent(session, leadershipTermId, leaderMemberId, eventCode, detail));

        verify(session).responsePublication();
        verify(responsePublication).maxPayloadLength();
        final ArgumentCaptor<MutableDirectBuffer> captor = ArgumentCaptor.forClass(MutableDirectBuffer.class);
        verify(session, times(3)).offer(captor.capture(), anyInt(), anyInt());

        final MutableDirectBuffer srcBuffer = captor.getValue();
        assertInstanceOf(ExpandableArrayBuffer.class, srcBuffer);
        verifySessionEvent(
            srcBuffer, 0, clusterSessionId, correlationId, leadershipTermId, leaderMemberId, eventCode, detail);
    }

    @Test
    void shouldSendChallengeResponseUsingTryClaimIfSmall()
    {
        final long id = 15L;
        final long correlationId = -1000000L;
        final byte[] encodedChallenge = { 0x1, 0x2 };
        when(responsePublication.maxPayloadLength()).thenReturn(500);
        when(session.responsePublication()).thenReturn(responsePublication);
        when(session.id()).thenReturn(id);
        when(session.correlationId()).thenReturn(correlationId);
        when(session.tryClaim(anyInt(), any()))
            .thenReturn(BACK_PRESSURED, NOT_CONNECTED)
            .thenAnswer((invocation) ->
            {
                final int totalLength = (int)invocation.getArgument(0) + HEADER_LENGTH;
                final BufferClaim bufferClaim = invocation.getArgument(1);
                bufferClaim.wrap(buffer, 0, totalLength);
                return 1L;
            });

        assertTrue(egressPublisher.sendChallenge(session, encodedChallenge));

        verify(session, times(3)).tryClaim(anyInt(), any());
        verify(session).responsePublication();
        verify(responsePublication).maxPayloadLength();

        verifyChallenge(buffer, HEADER_LENGTH, correlationId, id, encodedChallenge);
    }

    @Test
    void shouldSendChallengeResponseUsingOfferIfLarge()
    {
        final long id = 1;
        final long correlationId = 4444;
        final byte[] encodedChallenge = new byte[100];
        ThreadLocalRandom.current().nextBytes(encodedChallenge);
        when(responsePublication.maxPayloadLength()).thenReturn(encodedChallenge.length);
        when(session.responsePublication()).thenReturn(responsePublication);
        when(session.id()).thenReturn(id);
        when(session.correlationId()).thenReturn(correlationId);
        when(session.offer(any(), anyInt(), anyInt())).thenReturn(BACK_PRESSURED, 1111L);

        assertTrue(egressPublisher.sendChallenge(session, encodedChallenge));

        final ArgumentCaptor<MutableDirectBuffer> captor = ArgumentCaptor.forClass(MutableDirectBuffer.class);
        verify(session, times(2)).offer(captor.capture(), anyInt(), anyInt());
        verify(session).responsePublication();
        verify(responsePublication).maxPayloadLength();

        final MutableDirectBuffer srcBuffer = captor.getValue();
        assertInstanceOf(ExpandableArrayBuffer.class, srcBuffer);
        verifyChallenge(srcBuffer, 0, correlationId, id, encodedChallenge);
    }

    @Test
    void shouldSendNewLeaderEventUsingTryClaimIfSmall()
    {
        final long id = 442394;
        final long leadershipTermId = 42;
        final int leaderMemberId = 555;
        final String ingressEndpoints = "host0:7777,host1:7777,host2:7777";
        when(responsePublication.maxPayloadLength()).thenReturn(500);
        when(session.responsePublication()).thenReturn(responsePublication);
        when(session.id()).thenReturn(id);
        when(session.tryClaim(anyInt(), any()))
            .thenReturn(NOT_CONNECTED)
            .thenAnswer((invocation) ->
            {
                final int totalLength = (int)invocation.getArgument(0) + HEADER_LENGTH;
                final BufferClaim bufferClaim = invocation.getArgument(1);
                bufferClaim.wrap(buffer, 0, totalLength);
                return 1L;
            });

        assertTrue(egressPublisher.newLeader(session, leadershipTermId, leaderMemberId, ingressEndpoints));

        verify(session, times(2)).tryClaim(anyInt(), any());
        verify(session).responsePublication();
        verify(responsePublication).maxPayloadLength();

        verifyNewLeader(buffer, HEADER_LENGTH, leadershipTermId, id, leaderMemberId, ingressEndpoints);
    }

    @Test
    void shouldSendNewLeaderEventUsingOfferIfTooBig()
    {
        final long id = 0;
        final long leadershipTermId = 1;
        final int leaderMemberId = 3;
        final String ingressEndpoints = Tests.generateStringWithSuffix("test", "x", 1000);
        when(responsePublication.maxPayloadLength()).thenReturn(500);
        when(session.responsePublication()).thenReturn(responsePublication);
        when(session.id()).thenReturn(id);
        when(session.offer(any(), anyInt(), anyInt())).thenReturn(NOT_CONNECTED, ADMIN_ACTION, 10L);

        assertTrue(egressPublisher.newLeader(session, leadershipTermId, leaderMemberId, ingressEndpoints));

        final ArgumentCaptor<MutableDirectBuffer> captor = ArgumentCaptor.forClass(MutableDirectBuffer.class);
        verify(session, times(3)).offer(captor.capture(), anyInt(), anyInt());
        verify(session).responsePublication();
        verify(responsePublication).maxPayloadLength();

        final MutableDirectBuffer srcBuffer = captor.getValue();
        assertInstanceOf(ExpandableArrayBuffer.class, srcBuffer);
        verifyNewLeader(srcBuffer, 0, leadershipTermId, id, leaderMemberId, ingressEndpoints);
    }

    @Test
    void shouldSendAdminResponseUsingTryClaimIfSmall()
    {
        final long id = 111111;
        final long correlationId = 10;
        final AdminRequestType adminRequestType = AdminRequestType.NULL_VAL;
        final AdminResponseCode adminResponseCode = AdminResponseCode.ERROR;
        final String message = "error message";
        when(responsePublication.maxPayloadLength()).thenReturn(500);
        when(session.responsePublication()).thenReturn(responsePublication);
        when(session.id()).thenReturn(id);
        when(session.tryClaim(anyInt(), any()))
            .thenReturn(BACK_PRESSURED, NOT_CONNECTED)
            .thenAnswer((invocation) ->
            {
                final int totalLength = (int)invocation.getArgument(0) + HEADER_LENGTH;
                final BufferClaim bufferClaim = invocation.getArgument(1);
                bufferClaim.wrap(buffer, 0, totalLength);
                return 1L;
            });


        assertTrue(egressPublisher.sendAdminResponse(
            session, correlationId, adminRequestType, adminResponseCode, message));

        verify(session, times(3)).tryClaim(anyInt(), any());
        verify(session).responsePublication();
        verify(responsePublication).maxPayloadLength();

        verifAdminResponse(buffer, HEADER_LENGTH, id, correlationId, adminRequestType, adminResponseCode, message);
    }

    @Test
    void shouldSendAdminResponseUsingOfferIfTooBig()
    {
        final long id = -3463284;
        final long correlationId = 42;
        final AdminRequestType adminRequestType = AdminRequestType.SNAPSHOT;
        final AdminResponseCode adminResponseCode = AdminResponseCode.UNAUTHORISED_ACCESS;
        final String message = Tests.generateStringWithSuffix("looon", "g", 200);
        when(responsePublication.maxPayloadLength()).thenReturn(50);
        when(session.responsePublication()).thenReturn(responsePublication);
        when(session.id()).thenReturn(id);
        when(session.correlationId()).thenReturn(correlationId);
        when(session.offer(any(), anyInt(), anyInt())).thenReturn(ADMIN_ACTION, 10L);

        assertTrue(egressPublisher.sendAdminResponse(
            session, correlationId, adminRequestType, adminResponseCode, message));

        final ArgumentCaptor<MutableDirectBuffer> captor = ArgumentCaptor.forClass(MutableDirectBuffer.class);
        verify(session, times(2)).offer(captor.capture(), anyInt(), anyInt());
        verify(session).responsePublication();
        verify(responsePublication).maxPayloadLength();

        final MutableDirectBuffer srcBuffer = captor.getValue();
        assertInstanceOf(ExpandableArrayBuffer.class, srcBuffer);
        verifAdminResponse(srcBuffer, 0, id, correlationId, adminRequestType, adminResponseCode, message);
    }

    private void verifySessionEvent(
        final MutableDirectBuffer buffer,
        final int offset,
        final long clusterSessionId,
        final long correlationId,
        final long leadershipTermId,
        final int leaderMemberId,
        final EventCode eventCode,
        final String detail)
    {
        final SessionEventDecoder sessionEventDecoder = new SessionEventDecoder()
            .wrapAndApplyHeader(buffer, offset, messageHeaderDecoder);
        assertEquals(clusterSessionId, sessionEventDecoder.clusterSessionId());
        assertEquals(correlationId, sessionEventDecoder.correlationId());
        assertEquals(leadershipTermId, sessionEventDecoder.leadershipTermId());
        assertEquals(leaderMemberId, sessionEventDecoder.leaderMemberId());
        assertEquals(eventCode, sessionEventDecoder.code());
        assertEquals(PROTOCOL_SEMANTIC_VERSION, sessionEventDecoder.version());
        assertEquals(LEADER_HEARTBEAT_TIMEOUT_NS, sessionEventDecoder.leaderHeartbeatTimeoutNs());
        assertEquals(detail, sessionEventDecoder.detail());
    }

    private void verifyChallenge(
        final MutableDirectBuffer buffer,
        final int offset,
        final long correlationId,
        final long id,
        final byte[] encodedChallenge)
    {
        final ChallengeDecoder challengeDecoder = new ChallengeDecoder()
            .wrapAndApplyHeader(buffer, offset, messageHeaderDecoder);
        assertEquals(correlationId, challengeDecoder.correlationId());
        assertEquals(id, challengeDecoder.clusterSessionId());
        final byte[] result = new byte[challengeDecoder.encodedChallengeLength()];
        challengeDecoder.getEncodedChallenge(result, 0, result.length);
        assertArrayEquals(encodedChallenge, result);
    }

    private void verifyNewLeader(
        final MutableDirectBuffer buffer,
        final int offset,
        final long leadershipTermId,
        final long id,
        final int leaderMemberId,
        final String ingressEndpoints)
    {
        final NewLeaderEventDecoder decoder = new NewLeaderEventDecoder()
            .wrapAndApplyHeader(buffer, offset, messageHeaderDecoder);
        assertEquals(leadershipTermId, decoder.leadershipTermId());
        assertEquals(id, decoder.clusterSessionId());
        assertEquals(leaderMemberId, decoder.leaderMemberId());
        assertEquals(ingressEndpoints, decoder.ingressEndpoints());
    }

    private void verifAdminResponse(
        final MutableDirectBuffer srcBuffer,
        final int offset,
        final long id,
        final long correlationId,
        final AdminRequestType adminRequestType,
        final AdminResponseCode adminResponseCode,
        final String message)
    {
        final AdminResponseDecoder decoder = new AdminResponseDecoder()
            .wrapAndApplyHeader(srcBuffer, offset, messageHeaderDecoder);
        assertEquals(id, decoder.clusterSessionId());
        assertEquals(correlationId, decoder.correlationId());
        assertEquals(adminRequestType, decoder.requestType());
        assertEquals(adminResponseCode, decoder.responseCode());
        assertEquals(message, decoder.message());
        assertEquals(0, decoder.payloadLength());
    }
}
