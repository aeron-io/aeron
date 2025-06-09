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

import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionEventEncoder;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.security.NullCredentialsSupplier;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

import java.util.concurrent.TimeUnit;

import static io.aeron.cluster.client.AeronCluster.AsyncConnect.State.*;
import static io.aeron.protocol.DataHeaderFlyweight.BEGIN_AND_END_FLAGS;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AeronClusterAsyncConnectTest
{
    private final Aeron aeron = mock(Aeron.class);
    private final Aeron.Context aeronContext = new Aeron.Context().nanoClock(SystemNanoClock.INSTANCE);
    private final AeronCluster.Context context = spy(new AeronCluster.Context()
        .aeron(aeron)
        .ownsAeronClient(false)
        .egressChannel("aeron:udp?endpoint=localhost:0")
        .egressStreamId(42)
        .ingressChannel("aeron:udp?endpoint=replace-me:5555")
        .ingressStreamId(-19)
        .credentialsSupplier(new NullCredentialsSupplier()))
        .idleStrategy(NoOpIdleStrategy.INSTANCE);

    @BeforeEach
    void before()
    {
        when(aeron.context()).thenReturn(aeronContext);
    }

    @Test
    public void initialState()
    {
        final AeronCluster.AsyncConnect asyncConnect = new AeronCluster.AsyncConnect(context, 1);
        assertEquals(CREATE_EGRESS_SUBSCRIPTION, asyncConnect.state());
        assertEquals(CREATE_EGRESS_SUBSCRIPTION.step, asyncConnect.step());
    }

    @Test
    public void shouldCloseAsyncSubscription()
    {
        final long subscriptionId = 999;
        when(aeron.asyncAddSubscription(context.egressChannel(), context.egressStreamId())).thenReturn(subscriptionId);

        final AeronCluster.AsyncConnect asyncConnect = new AeronCluster.AsyncConnect(
            context, aeronContext.nanoClock().nanoTime() + TimeUnit.HOURS.toNanos(1));

        assertNull(asyncConnect.poll());
        assertEquals(CREATE_EGRESS_SUBSCRIPTION, asyncConnect.state());

        asyncConnect.close();
        final InOrder inOrder = inOrder(aeron, context);
        inOrder.verify(aeron).context();
        inOrder.verify(aeron).asyncAddSubscription(context.egressChannel(), context.egressStreamId());
        inOrder.verify(aeron).getSubscription(subscriptionId);
        inOrder.verify(aeron).asyncRemoveSubscription(subscriptionId);
        inOrder.verify(context).close();
    }

    @Test
    public void shouldCloseEgressSubscription()
    {
        final long subscriptionId = -4343;
        when(aeron.asyncAddSubscription(context.egressChannel(), context.egressStreamId())).thenReturn(subscriptionId);
        final Subscription subscription = mock(Subscription.class);
        when(aeron.getSubscription(subscriptionId)).thenReturn(subscription);

        final AeronCluster.AsyncConnect asyncConnect = new AeronCluster.AsyncConnect(
            context, aeronContext.nanoClock().nanoTime() + TimeUnit.HOURS.toNanos(1));

        assertNull(asyncConnect.poll());
        assertEquals(CREATE_INGRESS_PUBLICATIONS, asyncConnect.state());

        asyncConnect.close();
        verify(subscription, only()).close();
        verify(context).close();
        verify(aeron, never()).asyncRemoveSubscription(subscriptionId);
    }

    @Test
    public void shouldCloseAsyncPublication()
    {
        final long subscriptionId = 87;
        when(aeron.asyncAddSubscription(context.egressChannel(), context.egressStreamId())).thenReturn(subscriptionId);
        final Subscription subscription = mock(Subscription.class);
        when(aeron.getSubscription(subscriptionId)).thenReturn(subscription);

        context.isIngressExclusive(true);
        final long publicationId = Long.MAX_VALUE;
        when(aeron.asyncAddExclusivePublication(context.ingressChannel(), context.ingressStreamId()))
            .thenReturn(publicationId);

        final AeronCluster.AsyncConnect asyncConnect = new AeronCluster.AsyncConnect(
            context, aeronContext.nanoClock().nanoTime() + TimeUnit.HOURS.toNanos(1));

        assertNull(asyncConnect.poll());
        assertEquals(CREATE_INGRESS_PUBLICATIONS, asyncConnect.state());

        assertNull(asyncConnect.poll());
        assertEquals(CREATE_INGRESS_PUBLICATIONS, asyncConnect.state());

        asyncConnect.close();
        final InOrder inOrder = inOrder(aeron, subscription, context);
        inOrder.verify(aeron).context();
        inOrder.verify(aeron).asyncAddSubscription(context.egressChannel(), context.egressStreamId());
        inOrder.verify(aeron).getSubscription(subscriptionId);
        inOrder.verify(aeron).asyncAddExclusivePublication(context.ingressChannel(), context.ingressStreamId());
        inOrder.verify(aeron).getExclusivePublication(publicationId);
        inOrder.verify(aeron).asyncRemovePublication(publicationId);
        inOrder.verify(subscription).close();
        inOrder.verify(context).close();
    }

    @Test
    public void shouldCloseIngressPublication()
    {
        final long subscriptionId = 42;
        when(aeron.asyncAddSubscription(context.egressChannel(), context.egressStreamId())).thenReturn(subscriptionId);
        final Subscription subscription = mock(Subscription.class);
        when(aeron.getSubscription(subscriptionId)).thenReturn(subscription);

        context.isIngressExclusive(false);
        final long publicationId = -6342756432L;
        when(aeron.asyncAddPublication(context.ingressChannel(), context.ingressStreamId())).thenReturn(publicationId);
        final ConcurrentPublication publication = mock(ConcurrentPublication.class);
        when(aeron.getPublication(publicationId)).thenReturn(publication);

        final AeronCluster.AsyncConnect asyncConnect = new AeronCluster.AsyncConnect(
            context, aeronContext.nanoClock().nanoTime() + TimeUnit.HOURS.toNanos(1));

        assertNull(asyncConnect.poll());
        assertEquals(CREATE_INGRESS_PUBLICATIONS, asyncConnect.state());

        assertNull(asyncConnect.poll());
        assertEquals(AWAIT_PUBLICATION_CONNECTED, asyncConnect.state());

        asyncConnect.close();
        verify(publication, only()).close();
        verify(subscription, only()).close();
        verify(context).close();
        verify(aeron, never()).asyncRemovePublication(publicationId);
        verify(aeron, never()).asyncRemoveSubscription(subscriptionId);
    }

    @Test
    public void shouldCloseIngressPublicationsOnMembers()
    {
        final long subscriptionId = 42;
        when(aeron.asyncAddSubscription(context.egressChannel(), context.egressStreamId())).thenReturn(subscriptionId);
        final Subscription subscription = mock(Subscription.class);
        when(aeron.getSubscription(subscriptionId)).thenReturn(subscription);

        final int insgressStreamId = 878;
        context
            .isIngressExclusive(true)
            .ingressEndpoints("0=localhost:20000,1=localhost:20001,2=localhost:20002")
            .ingressStreamId(insgressStreamId);
        final long publicationId1 = -6342756432L;
        final ExclusivePublication publication1 = mock(ExclusivePublication.class);
        when(aeron.asyncAddExclusivePublication("aeron:udp?endpoint=localhost:20000", insgressStreamId))
            .thenReturn(publicationId1);
        when(aeron.getExclusivePublication(publicationId1)).thenReturn(null, publication1);
        final long publicationId2 = Aeron.NULL_VALUE;
        when(aeron.asyncAddExclusivePublication("aeron:udp?endpoint=localhost:20001", insgressStreamId))
            .thenReturn(publicationId2);
        final long publicationId3 = 573495;
        when(aeron.asyncAddExclusivePublication("aeron:udp?endpoint=localhost:20002", insgressStreamId))
            .thenReturn(publicationId3);

        final AeronCluster.AsyncConnect asyncConnect = new AeronCluster.AsyncConnect(
            context, aeronContext.nanoClock().nanoTime() + TimeUnit.HOURS.toNanos(1));

        assertNull(asyncConnect.poll());
        assertEquals(CREATE_INGRESS_PUBLICATIONS, asyncConnect.state());

        final int iterations = 10;
        for (int i = 0; i < iterations; i++)
        {
            assertNull(asyncConnect.poll());
            assertEquals(CREATE_INGRESS_PUBLICATIONS, asyncConnect.state());
        }

        verify(aeron, atMostOnce())
            .asyncAddExclusivePublication("aeron:udp?endpoint=localhost:20000", insgressStreamId);
        verify(aeron, times(iterations))
            .asyncAddExclusivePublication("aeron:udp?endpoint=localhost:20001", insgressStreamId);
        verify(aeron, atMostOnce())
            .asyncAddExclusivePublication("aeron:udp?endpoint=localhost:20002", insgressStreamId);
        verify(aeron, times(2)).getExclusivePublication(publicationId1);
        verify(aeron, times(iterations)).getExclusivePublication(publicationId2);
        verify(aeron, times(iterations)).getExclusivePublication(publicationId3);

        asyncConnect.close();

        verify(subscription, only()).close();
        verify(publication1, only()).close();
        verify(context).close();
        verify(aeron, atMostOnce()).asyncRemovePublication(publicationId3);
        verify(aeron, never()).asyncRemoveSubscription(subscriptionId);
        verify(aeron, never()).asyncRemovePublication(publicationId1);
        verify(aeron, never()).asyncRemovePublication(publicationId2);
    }

    @Test
    public void shouldConnectViaIngressChannel()
    {
        final long subscriptionId = 42;
        final MutableLong correlationId = new MutableLong();
        when(aeron.nextCorrelationId()).thenAnswer((invocation) -> correlationId.incrementAndGet());
        when(aeron.asyncAddSubscription(context.egressChannel(), context.egressStreamId())).thenReturn(subscriptionId);
        final Subscription subscription = mock(Subscription.class);
        when(aeron.getSubscription(subscriptionId)).thenReturn(subscription);

        context.isIngressExclusive(false);
        final long publicationId = -19L;
        when(aeron.asyncAddPublication(context.ingressChannel(), context.ingressStreamId())).thenReturn(publicationId);
        final ConcurrentPublication publication = mock(ConcurrentPublication.class);
        when(aeron.getPublication(publicationId)).thenReturn(publication);

        final AeronCluster.AsyncConnect asyncConnect = new AeronCluster.AsyncConnect(
            context, aeronContext.nanoClock().nanoTime() + TimeUnit.HOURS.toNanos(1));

        assertNull(asyncConnect.poll());
        assertEquals(CREATE_INGRESS_PUBLICATIONS, asyncConnect.state());

        assertNull(asyncConnect.poll());
        assertEquals(AWAIT_PUBLICATION_CONNECTED, asyncConnect.state());

        final String responseChannel = "aeron:udp?endpoint=localhost:8888";
        when(subscription.tryResolveChannelEndpointPort()).thenReturn(responseChannel);
        when(publication.isConnected()).thenReturn(true);

        assertNull(asyncConnect.poll());
        assertEquals(SEND_MESSAGE, asyncConnect.state());
        final long sendMessageCorrelationId = correlationId.get();

        when(publication.offer(any(DirectBuffer.class), eq(0), anyInt())).thenReturn(8L);

        assertNull(asyncConnect.poll());
        assertEquals(POLL_RESPONSE, asyncConnect.state());

        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final SessionEventEncoder sessionEventEncoder = new SessionEventEncoder();
        final long clusterSessionId = 888;
        final long leadershipTermId = 5;
        final int leaderMemberId = 2;
        sessionEventEncoder
            .wrapAndApplyHeader(buffer, HEADER_LENGTH, headerEncoder)
            .clusterSessionId(clusterSessionId)
            .correlationId(sendMessageCorrelationId)
            .leadershipTermId(leadershipTermId)
            .leaderMemberId(leaderMemberId)
            .code(EventCode.OK)
            .version(ConsensusModule.Configuration.PROTOCOL_SEMANTIC_VERSION)
            .leaderHeartbeatTimeoutNs(SessionEventEncoder.leaderHeartbeatTimeoutNsNullValue())
            .detail("you are now connected");
        final Image egressImage = mock(Image.class);
        final Header header = new Header(1, 16, egressImage);
        header.buffer(buffer).offset(0);
        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
        headerFlyweight.wrap(buffer, 0, HEADER_LENGTH);
        headerFlyweight.flags(BEGIN_AND_END_FLAGS);
        when(subscription.controlledPoll(any(ControlledFragmentHandler.class), anyInt()))
            .thenAnswer((Answer<Integer>)invocation ->
            {
                final ControlledFragmentAssembler assembler = invocation.getArgument(0);
                assembler.onFragment(buffer, HEADER_LENGTH, sessionEventEncoder.encodedLength(), header);
                return 1;
            });

        assertNull(asyncConnect.poll());
        assertEquals(CONCLUDE_CONNECT, asyncConnect.state());

        final AeronCluster aeronCluster = asyncConnect.poll();
        assertNotNull(aeronCluster);
        assertSame(publication, aeronCluster.ingressPublication());
        assertSame(subscription, aeronCluster.egressSubscription());
        assertEquals(leadershipTermId, aeronCluster.leadershipTermId());
        assertEquals(leaderMemberId, aeronCluster.leaderMemberId());
        assertEquals(clusterSessionId, aeronCluster.clusterSessionId());
        assertEquals(
            2 * ConsensusModule.Configuration.LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS,
            context.newLeaderTimeoutNs());

        asyncConnect.close();
        verify(publication, never()).close();
        verify(subscription, never()).close();

        when(publication.tryClaim(anyInt(), any(BufferClaim.class))).thenAnswer((invocation) ->
        {
            final int length = invocation.getArgument(0);
            final BufferClaim bufferClaim = invocation.getArgument(1);
            bufferClaim.wrap(buffer, 0, BitUtil.align(HEADER_LENGTH + length, HEADER_LENGTH));
            return 42L;
        });
        aeronCluster.close();
        assertTrue(aeronCluster.isClosed());
        final InOrder inOrder = inOrder(context, subscription, publication);
        inOrder.verify(publication).tryClaim(anyInt(), any(BufferClaim.class));
        inOrder.verify(subscription).close();
        inOrder.verify(publication).close();
        inOrder.verify(context).close();
        inOrder.verifyNoMoreInteractions();
    }

}
