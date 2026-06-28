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
package io.aeron.cluster.client;

import io.aeron.Subscription;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.Header;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class EgressAdapterTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[512]);
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final SessionMessageHeaderEncoder sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();
    private final SessionEventEncoder sessionEventEncoder = new SessionEventEncoder();
    private final NewLeaderEventEncoder newLeaderEventEncoder = new NewLeaderEventEncoder();
    private final AdminResponseEncoder adminResponseEncoder = new AdminResponseEncoder();

    @Test
    void onFragmentShouldDelegateToEgressListenerOnUnknownSchemaId()
    {
        final int schemaId = 17;
        final int templateId = 19;
        messageHeaderEncoder
            .wrap(buffer, 0)
            .schemaId(schemaId)
            .templateId(templateId);

        final EgressListenerExtension listenerExtension = mock(EgressListenerExtension.class);
        final Header header = new Header(0, 0);
        final EgressAdapter adapter = new EgressAdapter(
            mock(EgressListener.class), listenerExtension, 0, mock(Subscription.class), 3);

        adapter.onFragment(buffer, 0, MessageHeaderDecoder.ENCODED_LENGTH * 2, header);

        verify(listenerExtension).onExtensionMessage(
            anyInt(),
            eq(templateId),
            eq(schemaId),
            eq(0),
            eq(buffer),
            eq(MessageHeaderDecoder.ENCODED_LENGTH),
            eq(MessageHeaderDecoder.ENCODED_LENGTH));
        verifyNoMoreInteractions(listenerExtension);
    }

    @Test
    void defaultEgressListenerBehaviourShouldThrowClusterExceptionOnUnknownSchemaId()
    {
        final EgressListener listener = (clusterSessionId, timestamp, buffer, offset, length, header) ->
        {
        };
        final EgressAdapter adapter =
            new EgressAdapter(listener, 42, mock(Subscription.class), 5);
        final ClusterException exception = assertThrows(ClusterException.class,
            () -> adapter.onFragment(buffer, 0, 64, new Header(0, 0)));
        assertEquals(
            "ERROR - expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=0",
            exception.getMessage());

    }

    @Test
    void onFragmentShouldInvokeOnMessageCallbackIfSessionIdMatches()
    {
        final int offset = 4;
        final long sessionId = 2973438724L;
        final long timestamp = -46328746238764832L;
        sessionMessageHeaderEncoder
            .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
            .clusterSessionId(sessionId)
            .timestamp(timestamp);

        final EgressListener egressListener = mock(EgressListener.class);
        final Header header = new Header(0, 0);
        final EgressAdapter adapter = new EgressAdapter(egressListener, sessionId, mock(Subscription.class), 3);

        adapter.onFragment(buffer, offset, sessionMessageHeaderEncoder.encodedLength(), header);

        verify(egressListener).onMessage(
            sessionId,
            timestamp,
            buffer,
            offset + SESSION_HEADER_LENGTH,
            sessionMessageHeaderEncoder.encodedLength() - SESSION_HEADER_LENGTH, header);
        verifyNoMoreInteractions(egressListener);
    }

    @Test
    void onFragmentIsANoOpIfSessionIdDoesNotMatchOnSessionMessage()
    {
        final int offset = 18;
        final long sessionId = 21;
        final long timestamp = 1000;
        sessionMessageHeaderEncoder
            .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
            .clusterSessionId(sessionId)
            .timestamp(timestamp);

        final EgressListener egressListener = mock(EgressListener.class);
        final Header header = new Header(0, 0);
        final EgressAdapter adapter = new EgressAdapter(egressListener, -19, mock(Subscription.class), 3);

        adapter.onFragment(buffer, offset, sessionMessageHeaderEncoder.encodedLength(), header);

        verifyNoInteractions(egressListener);
    }

    @Test
    void onFragmentShouldInvokeOnSessionEventCallbackIfSessionIdMatches()
    {
        final int offset = 8;
        final long clusterSessionId = 42;
        final long correlationId = 777;
        final long leadershipTermId = 6;
        final int leaderMemberId = 3;
        final EventCode eventCode = EventCode.REDIRECT;
        final int version = 18;
        final String eventDetail = "Event details";
        sessionEventEncoder
            .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
            .clusterSessionId(clusterSessionId)
            .correlationId(correlationId)
            .leadershipTermId(leadershipTermId)
            .leaderMemberId(leaderMemberId)
            .code(eventCode)
            .version(version)
            .detail(eventDetail);

        final EgressListener egressListener = mock(EgressListener.class);
        final Header header = new Header(1, 3);
        final EgressAdapter adapter = new EgressAdapter(egressListener, clusterSessionId, mock(Subscription.class), 10);

        adapter.onFragment(buffer, offset, sessionEventEncoder.encodedLength(), header);

        verify(egressListener).onSessionEvent(
            correlationId, clusterSessionId, leadershipTermId, leaderMemberId, eventCode, eventDetail);
        verifyNoMoreInteractions(egressListener);
    }

    @Test
    void onFragmentIsANoOpIfSessionIdDoesNotMatchOnSessionEvent()
    {
        final int offset = 8;
        final long clusterSessionId = 42;
        final long correlationId = 777;
        final long leadershipTermId = 6;
        final int leaderMemberId = 3;
        final EventCode eventCode = EventCode.REDIRECT;
        final int version = 18;
        final String eventDetail = "Event details";
        sessionEventEncoder
            .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
            .clusterSessionId(clusterSessionId)
            .correlationId(correlationId)
            .leadershipTermId(leadershipTermId)
            .leaderMemberId(leaderMemberId)
            .code(eventCode)
            .version(version)
            .detail(eventDetail);

        final EgressListener egressListener = mock(EgressListener.class);
        final Header header = new Header(0, 0);
        final EgressAdapter adapter = new EgressAdapter(
            egressListener, clusterSessionId + 1, mock(Subscription.class), 3);

        adapter.onFragment(buffer, offset, sessionEventEncoder.encodedLength(), header);

        verifyNoInteractions(egressListener);
    }

    @Test
    void onFragmentShouldInvokeOnNewLeaderCallbackIfSessionIdMatches()
    {
        final int offset = 0;
        final long clusterSessionId = 0;
        final long leadershipTermId = 6;
        final int leaderMemberId = 9999;
        final String ingressEndpoints = "ingress endpoints ...";
        newLeaderEventEncoder
            .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
            .leadershipTermId(leadershipTermId)
            .clusterSessionId(clusterSessionId)
            .leaderMemberId(leaderMemberId)
            .ingressEndpoints(ingressEndpoints);

        final EgressListener egressListener = mock(EgressListener.class);
        final Header header = new Header(1, 3);
        final EgressAdapter adapter = new EgressAdapter(egressListener, clusterSessionId, mock(Subscription.class), 10);

        adapter.onFragment(buffer, offset, newLeaderEventEncoder.encodedLength(), header);

        verify(egressListener).onNewLeader(clusterSessionId, leadershipTermId, leaderMemberId, ingressEndpoints);
        verifyNoMoreInteractions(egressListener);
    }

    @Test
    void onFragmentIsANoOpIfSessionIdDoesNotMatchOnNewLeader()
    {
        final int offset = 0;
        final long clusterSessionId = -100;
        final long leadershipTermId = 6;
        final int leaderMemberId = 9999;
        final String ingressEndpoints = "ingress endpoints ...";
        newLeaderEventEncoder
            .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
            .leadershipTermId(leadershipTermId)
            .clusterSessionId(clusterSessionId)
            .leaderMemberId(leaderMemberId)
            .ingressEndpoints(ingressEndpoints);

        final EgressListener egressListener = mock(EgressListener.class);
        final Header header = new Header(1, 3);
        final EgressAdapter adapter = new EgressAdapter(egressListener, 0, mock(Subscription.class), 10);

        adapter.onFragment(buffer, offset, newLeaderEventEncoder.encodedLength(), header);

        verifyNoInteractions(egressListener);
    }

    @Test
    void onFragmentShouldInvokeOnAdminResponseCallbackIfSessionIdMatches()
    {
        final int offset = 24;
        final long clusterSessionId = 18;
        final long correlationId = 3274239749237498239L;
        final AdminRequestType type = AdminRequestType.SNAPSHOT;
        final AdminResponseCode responseCode = AdminResponseCode.UNAUTHORISED_ACCESS;
        final String message = "Unauthorised access detected!";
        final byte[] payload = new byte[]{ 0x1, 0x2, 0x3 };
        adminResponseEncoder
            .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
            .clusterSessionId(clusterSessionId)
            .correlationId(correlationId)
            .requestType(type)
            .responseCode(responseCode)
            .message(message);
        adminResponseEncoder.putPayload(payload, 0, payload.length);

        final EgressListener egressListener = mock(EgressListener.class);
        final Header header = new Header(1, 3);
        final EgressAdapter adapter = new EgressAdapter(egressListener, clusterSessionId, mock(Subscription.class), 10);

        adapter.onFragment(buffer, offset, adminResponseEncoder.encodedLength(), header);

        verify(egressListener).onAdminResponse(
            clusterSessionId,
            correlationId,
            type,
            responseCode,
            message,
            buffer,
            offset + MessageHeaderEncoder.ENCODED_LENGTH + adminResponseEncoder.encodedLength() - payload.length,
            payload.length);
        verifyNoMoreInteractions(egressListener);
    }

    @Test
    void onFragmentIsANoOpIfSessionIdDoesNotMatchOnAdminResponse()
    {
        final int offset = 24;
        final long clusterSessionId = 18;
        final long correlationId = 3274239749237498239L;
        final AdminRequestType type = AdminRequestType.SNAPSHOT;
        final AdminResponseCode responseCode = AdminResponseCode.OK;
        final String message = "Unauthorised access detected!";
        final byte[] payload = new byte[]{ 0x1, 0x2, 0x3 };
        adminResponseEncoder
            .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
            .clusterSessionId(clusterSessionId)
            .correlationId(correlationId)
            .requestType(type)
            .responseCode(responseCode)
            .message(message);
        adminResponseEncoder.putPayload(payload, 0, payload.length);

        final EgressListener egressListener = mock(EgressListener.class);
        final Header header = new Header(1, 3);
        final EgressAdapter adapter = new EgressAdapter(
            egressListener, -clusterSessionId, mock(Subscription.class), 10);

        adapter.onFragment(buffer, offset, adminResponseEncoder.encodedLength(), header);

        verifyNoInteractions(egressListener);
    }
}
