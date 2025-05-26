/*
 * Copyright 2025 Adaptive Financial Consulting Limited.
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
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.NewLeaderEventEncoder;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.agrona.BitUtil.align;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AeronClusterTest
{
    private static final String INGRESS_ENDPOINTS = "foo:1000,bar:1000,baz:1000";
    private static final int CLUSTER_SESSION_ID = 123;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
    private final UnsafeBuffer appMessage = new UnsafeBuffer(new byte[8]);
    private final EgressListener egressListener = mock(EgressListener.class);
    private final Aeron aeron = mock(Aeron.class);
    private final Aeron.Context aeronContext = new Aeron.Context().nanoClock(this::nanoTime);
    private final AeronCluster.Context context = spy(new AeronCluster.Context()
        .aeron(aeron)
        .ownsAeronClient(false)
        .egressChannel("aeron:udp?endpoint=localhost:0")
        .ingressChannel("aeron:udp")
        .idleStrategy(NoOpIdleStrategy.INSTANCE)
        .egressListener(egressListener))
        .newLeaderTimeoutNs(TimeUnit.SECONDS.toNanos(1));
    private final ExclusivePublication ingressPublication = mock(ExclusivePublication.class);
    private final Subscription egressSubscription = mock(Subscription.class);
    private final Image egressImage = mock(Image.class);
    private AeronCluster aeronCluster;
    private long nanoTime;
    private int leadershipTermId = 2;
    private int leaderMemberId = 1;
    private boolean newLeaderEventPending;

    @BeforeEach
    void setUp()
    {
        context.conclude();

        when(aeron.context()).thenReturn(aeronContext);
        when(aeron.addExclusivePublication(context.ingressChannel(), context.ingressStreamId()))
            .thenReturn(ingressPublication);

        when(egressSubscription.poll(any(FragmentHandler.class), anyInt())).thenAnswer(invocation ->
        {
            if (newLeaderEventPending)
            {
                newLeaderEventPending = false;

                final int offset = DataHeaderFlyweight.HEADER_LENGTH;
                FrameDescriptor.frameFlags(buffer, 0, FrameDescriptor.UNFRAGMENTED);

                final NewLeaderEventEncoder newLeaderEventEncoder = new NewLeaderEventEncoder();
                newLeaderEventEncoder.wrapAndApplyHeader(buffer, offset, new MessageHeaderEncoder());
                newLeaderEventEncoder.clusterSessionId(CLUSTER_SESSION_ID);
                newLeaderEventEncoder.leadershipTermId(++leadershipTermId);
                newLeaderEventEncoder.leaderMemberId(++leaderMemberId);
                newLeaderEventEncoder.ingressEndpoints(INGRESS_ENDPOINTS);

                final int length = MessageHeaderEncoder.ENCODED_LENGTH + newLeaderEventEncoder.encodedLength();

                final Header header = new Header(0, 0, egressImage);
                header.buffer(buffer);

                final FragmentHandler handler = invocation.getArgument(0, FragmentHandler.class);
                handler.onFragment(buffer, offset, length, header);

                return 1;
            }

            return 0;
        });

        aeronCluster = new AeronCluster(
            context,
            new MessageHeaderEncoder(),
            ingressPublication,
            egressSubscription,
            egressImage,
            new Int2ObjectHashMap<>(),
            CLUSTER_SESSION_ID,
            leadershipTermId,
            leaderMemberId);
    }

    static Stream<Arguments> shouldStayConnectedAfterSuccessfulFailover()
    {
        return Stream.of(
            arguments(false, false),
            arguments(false, true),
            arguments(true, false),
            arguments(true, true));
    }

    @ParameterizedTest
    @MethodSource
    void shouldStayConnectedAfterSuccessfulFailover(final boolean withIngressDisconnect, final boolean withAppMessages)
    {
        final long initialResult = withIngressDisconnect ? Publication.NOT_CONNECTED : 128;
        makeIngressPublicationReturn(initialResult);
        if (withAppMessages)
        {
            assertEquals(initialResult, aeronCluster.offer(appMessage, 0, 8));
        }
        else
        {
            assertEquals(!withIngressDisconnect, aeronCluster.sendKeepAlive());
        }

        nanoTime += context.newLeaderTimeoutNs() - 1;

        makeEgressSubscriptionDeliverNewLeaderEvent();
        assertEquals(1, aeronCluster.pollEgress());
        verify(egressListener).onNewLeader(CLUSTER_SESSION_ID, leadershipTermId, leaderMemberId, INGRESS_ENDPOINTS);
        assertEquals(0, aeronCluster.pollEgress());

        nanoTime += context.messageTimeoutNs() - 1;

        makeIngressPublicationReturn(256);
        if (withAppMessages)
        {
            assertEquals(256, aeronCluster.offer(appMessage, 0, 8));
        }
        else
        {
            assertTrue(aeronCluster.sendKeepAlive());
        }

        nanoTime += 1;

        assertEquals(0, aeronCluster.pollEgress());
        assertFalse(aeronCluster.isClosed());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldCloseItselfWhenDisconnectedForLongerThanNewLeaderTimeout(final boolean withAppMessages)
    {
        makeIngressPublicationReturn(Publication.NOT_CONNECTED);
        if (withAppMessages)
        {
            assertEquals(Publication.NOT_CONNECTED, aeronCluster.offer(appMessage, 0, 8));
        }
        else
        {
            assertFalse(aeronCluster.sendKeepAlive());
        }

        nanoTime += context.newLeaderTimeoutNs() - 1;

        assertEquals(0, aeronCluster.pollEgress());
        assertFalse(aeronCluster.isClosed());

        nanoTime += 1;

        assertEquals(1, aeronCluster.pollEgress());
        assertTrue(aeronCluster.isClosed());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldCloseItselfWhenUnableToSendMessageForLongerThanNewLeaderConnectionTimeout(final boolean withAppMessages)
    {
        makeIngressPublicationReturn(Publication.NOT_CONNECTED);
        if (withAppMessages)
        {
            assertEquals(Publication.NOT_CONNECTED, aeronCluster.offer(appMessage, 0, 8));
        }
        else
        {
            assertFalse(aeronCluster.sendKeepAlive());
        }

        nanoTime += context.newLeaderTimeoutNs() / 2;

        makeEgressSubscriptionDeliverNewLeaderEvent();

        assertEquals(1, aeronCluster.pollEgress());
        assertFalse(aeronCluster.isClosed());

        nanoTime += context.messageTimeoutNs() - 1;
        if (withAppMessages)
        {
            assertEquals(Publication.NOT_CONNECTED, aeronCluster.offer(appMessage, 0, 8));
        }
        else
        {
            assertFalse(aeronCluster.sendKeepAlive());
        }

        nanoTime += 1;

        assertEquals(1, aeronCluster.pollEgress());
        assertTrue(aeronCluster.isClosed());
    }

    @Test
    void shouldCloseIngressPublicationWhenEgressImageCloses()
    {
        // in CONNECTED state
        when(egressImage.isClosed()).thenReturn(true);
        assertEquals(1, aeronCluster.pollEgress());
        verify(ingressPublication).close();

        when(egressImage.isClosed()).thenReturn(false);
        makeEgressSubscriptionDeliverNewLeaderEvent();
        assertEquals(1, aeronCluster.pollEgress());
        verify(ingressPublication, times(2)).close();

        // and in AWAIT_NEW_LEADER_CONNECTION state too
        when(egressImage.isClosed()).thenReturn(true);
        assertEquals(1, aeronCluster.pollEgress());
        verify(ingressPublication, times(3)).close();
    }

    @Test
    void shouldCloseItselfAfterReachingMaxPositionOnTheIngressPublication()
    {
        makeIngressPublicationReturn(Publication.MAX_POSITION_EXCEEDED);
        assertEquals(Publication.MAX_POSITION_EXCEEDED, aeronCluster.offer(appMessage, 0, 8));
        verify(ingressPublication).close();
        assertEquals(1, aeronCluster.pollStateChanges());
        assertTrue(aeronCluster.isClosed());
    }

    private void makeIngressPublicationReturn(final long result)
    {
        if (result > 0)
        {
            when(ingressPublication.tryClaim(anyInt(), any(BufferClaim.class))).thenAnswer(invocation ->
            {
                int length = invocation.getArgument(0, Integer.class);
                length = align(DataHeaderFlyweight.HEADER_LENGTH + length, FrameDescriptor.FRAME_ALIGNMENT);
                final BufferClaim bufferClaim = invocation.getArgument(1, BufferClaim.class);
                bufferClaim.wrap(buffer, 0, length);
                return result;
            });
        }
        else
        {
            when(ingressPublication.tryClaim(anyInt(), any(BufferClaim.class))).thenReturn(result);
        }

        when(ingressPublication.offer(
            any(DirectBuffer.class), anyInt(), anyInt(),
            any(DirectBuffer.class), anyInt(), anyInt(),
            isNull())).thenReturn(result);
    }

    private void makeEgressSubscriptionDeliverNewLeaderEvent()
    {
        newLeaderEventPending = true;
    }

    private long nanoTime()
    {
        return nanoTime;
    }
}
