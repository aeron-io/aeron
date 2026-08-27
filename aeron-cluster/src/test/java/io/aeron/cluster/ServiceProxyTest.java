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
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.cluster.codecs.ClusterMembersExtendedResponseDecoder;
import io.aeron.cluster.codecs.ClusterMembersResponseDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static io.aeron.Publication.ADMIN_ACTION;
import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.cluster.ServiceProxy.SEND_ATTEMPTS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ServiceProxyTest
{
    private final Publication publication = mock(Publication.class);
    private final ErrorHandler errorHandler = mock(ErrorHandler.class);
    private final ServiceProxy serviceProxy = new ServiceProxy(publication, errorHandler);

    @Test
    void shouldLogWarningIfClusterMemberResponseFails()
    {
        when(publication.tryClaim(anyInt(), any())).thenReturn(BACK_PRESSURED);

        serviceProxy.clusterMembersResponse(
            42,
            5,
            "0,localhost:5555|1,localhost:6666|2,localhost:7777"
        );

        final ArgumentCaptor<ClusterEvent> captor = ArgumentCaptor.forClass(ClusterEvent.class);
        verify(publication, times(SEND_ATTEMPTS)).tryClaim(anyInt(), any());
        verify(errorHandler).onError(captor.capture());
        final ClusterEvent event = captor.getValue();
        assertEquals(
            "WARN - failed to send cluster members response: result=BACK_PRESSURED", event.getMessage());
    }

    @Test
    void shouldSendClusterMemberResponse()
    {
        when(publication.tryClaim(anyInt(), any()))
            .thenReturn(ADMIN_ACTION)
            .thenAnswer((invocation) ->
            {
                final int length = invocation.getArgument(0);
                final BufferClaim bufferClaim = invocation.getArgument(1);
                final int totalLength = DataHeaderFlyweight.HEADER_LENGTH + length;
                bufferClaim.wrap(new UnsafeBuffer(new byte[totalLength]), 0, totalLength);
                return (long)length;
            });

        final long correlationId = 473749239847L;
        final int leaderMemberId = 1;
        final String activeMembers = "0,localhost:5555|1,localhost:6666|2,localhost:7777";
        serviceProxy.clusterMembersResponse(
            correlationId,
            leaderMemberId,
            activeMembers
        );

        final ArgumentCaptor<Integer> lengthCaptor = ArgumentCaptor.forClass(int.class);
        final ArgumentCaptor<BufferClaim> bufferCaptor = ArgumentCaptor.forClass(BufferClaim.class);
        verify(publication, times(2)).tryClaim(lengthCaptor.capture(), bufferCaptor.capture());
        verifyNoInteractions(errorHandler);

        final BufferClaim bufferClaim = bufferCaptor.getValue();
        final ClusterMembersResponseDecoder decoder = new ClusterMembersResponseDecoder();
        decoder.wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), new MessageHeaderDecoder());
        assertEquals(correlationId, decoder.correlationId());
        assertEquals(leaderMemberId, decoder.leaderMemberId());
        assertEquals(activeMembers, decoder.activeMembers());
        assertEquals("", decoder.passiveFollowers());
    }

    @Test
    void shouldLogWarningIfClusterMemberExtendedResponseFails()
    {
        when(publication.offer(any(), anyInt(), anyInt(), eq(null))).thenReturn(BACK_PRESSURED);

        serviceProxy.clusterMembersExtendedResponse(
            42,
            1000000,
            5,
            3,
            new ClusterMember[]{
                new ClusterMember(3, "ingress", "consensus", "log", "catchup", "archive", "endpoints") }
        );

        final ArgumentCaptor<ClusterEvent> captor = ArgumentCaptor.forClass(ClusterEvent.class);
        verify(publication, times(SEND_ATTEMPTS)).offer(any(), anyInt(), anyInt(), eq(null));
        verify(errorHandler).onError(captor.capture());
        final ClusterEvent event = captor.getValue();
        assertEquals(
            "WARN - failed to send cluster members extended response: result=BACK_PRESSURED", event.getMessage());
    }


    @Test
    void shouldSendClusterMemberExtendedResponse()
    {
        when(publication.offer(any(), anyInt(), anyInt(), eq(null)))
            .thenReturn(ADMIN_ACTION, 123L);

        final long correlationId = -2423L;
        final long currentTimeNs = 1000000;
        final int leaderMemberId = 1;
        final int memberId = 3;
        final ClusterMember[] activeMembers = {
            new ClusterMember(3, "ingress", "consensus", "log", "catchup", "archive", "endpoints") };
        serviceProxy.clusterMembersExtendedResponse(
            correlationId,
            currentTimeNs,
            leaderMemberId,
            memberId,
            activeMembers
        );

        final ArgumentCaptor<DirectBuffer> captor = ArgumentCaptor.forClass(DirectBuffer.class);
        verify(publication, times(2)).offer(captor.capture(), anyInt(), anyInt(), eq(null));
        verifyNoInteractions(errorHandler);

        final DirectBuffer buffer = captor.getValue();
        final ClusterMembersExtendedResponseDecoder decoder = new ClusterMembersExtendedResponseDecoder();
        decoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());
        assertEquals(correlationId, decoder.correlationId());
        assertEquals(currentTimeNs, decoder.currentTimeNs());
        assertEquals(leaderMemberId, decoder.leaderMemberId());
        assertEquals(memberId, decoder.memberId());
        final ClusterMembersExtendedResponseDecoder.ActiveMembersDecoder activeMembersDecoder = decoder.activeMembers();
        assertEquals(1, activeMembersDecoder.count());
        while (activeMembersDecoder.hasNext())
        {
            activeMembersDecoder.next();
            activeMembersDecoder.sbeSkip();
        }
        assertEquals(0, decoder.passiveMembers().count());
    }
}
