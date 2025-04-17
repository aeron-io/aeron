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
package io.aeron.cluster;

import io.aeron.ChannelUri;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.driver.SendChannelEndpointSupplier;
import io.aeron.driver.ext.DebugSendChannelEndpoint;
import io.aeron.driver.ext.LossGenerator;
import io.aeron.logbuffer.Header;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;

import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class ClusterSessionReliabilityTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(10)
    void sessionShouldGetClosedWhenIngressImageGoesUnavailableToPreventSilentMessageLoss()
    {
        sessionShouldGetClosedWhenIngressImageGoesUnavailableToPreventSilentMessageLoss(false, false);
    }

    @Test
    @InterruptAfter(10)
    void sessionShouldGetClosedWhenMulticastIngressImageGoesUnavailableToPreventSilentMessageLoss()
    {
        sessionShouldGetClosedWhenIngressImageGoesUnavailableToPreventSilentMessageLoss(true, false);
    }

    @Test
    @InterruptAfter(20)
    void sessionShouldGetClosedWhenIngressImageGoesUnavailableAfterFailoverToPreventSilentMessageLoss()
    {
        sessionShouldGetClosedWhenIngressImageGoesUnavailableToPreventSilentMessageLoss(false, true);
    }

    @Test
    @InterruptAfter(20)
    void sessionShouldGetClosedWhenMulticastIngressImageGoesUnavailableAfterFailoverToPreventSilentMessageLoss()
    {
        sessionShouldGetClosedWhenIngressImageGoesUnavailableToPreventSilentMessageLoss(true, true);
    }

    private void sessionShouldGetClosedWhenIngressImageGoesUnavailableToPreventSilentMessageLoss(
        final boolean multicastIngress,
        final boolean withFailover)
    {
        TestMediaDriver.notSupportedOnCMediaDriver("uses custom channel endpoint suppliers for simulating loss");

        final AtomicLong outOfSequenceCount = new AtomicLong();
        final IntFunction<TestNode.TestService[]> serviceSupplier =
            (index) -> new TestNode.TestService[]
            {
                new SequenceCheckingService(index, outOfSequenceCount)
            };
        String ingressChannel = "aeron:udp?term-length=128k|alias=ingress";
        if (multicastIngress)
        {
            ingressChannel += "|endpoint=239.192.11.87:20123|interface=127.0.0.1";
        }
        final long imageLivenessTimeoutNs = TimeUnit.MILLISECONDS.toNanos(1000);
        final long sessionTimeoutNs = TimeUnit.MILLISECONDS.toNanos(4000);
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withServiceSupplier(serviceSupplier)
            .withIngressChannel(ingressChannel)
            .withImageLivenessTimeoutNs(imageLivenessTimeoutNs)
            .withSessionTimeoutNs(sessionTimeoutNs)
            .start();
        systemTestWatcher.cluster(cluster);

        TestNode leader = cluster.awaitLeader();

        final PortLossGenerator clientSendLossGenerator = new PortLossGenerator();
        cluster.clientSendChannelEndpointSupplier(sendChannelEndpointSupplier(clientSendLossGenerator));
        final AeronCluster client = cluster.connectClient();

        if (withFailover)
        {
            leader.gracefulClose();

            cluster.awaitNewLeadershipEvent(1);

            final int previousLeaderIndex = leader.index();
            leader = cluster.awaitLeader();
            assertNotEquals(previousLeaderIndex, leader.index());

            while (!client.sendKeepAlive())
            {
                Tests.yieldingIdle("failed to send keep-alive");
            }
        }

        final SequenceCheckingService leaderService = (SequenceCheckingService)leader.service();

        cluster.shouldErrorOnClientClose(false);

        final MutableDirectBuffer buffer = new UnsafeBuffer(new byte[SIZE_OF_LONG]);
        long sequence = 0;
        boolean lossRequested = false;
        long now = System.nanoTime();
        long nextMessageAt = now;
        final long deadline = now + sessionTimeoutNs + TimeUnit.SECONDS.toNanos(1);
        while (true)
        {
            now = System.nanoTime();

            if (now - deadline >= 0)
            {
                break;
            }

            if (now - nextMessageAt >= 0)
            {
                buffer.putLong(0, sequence);
                if (client.offer(buffer, 0, buffer.capacity()) > 0)
                {
                    sequence++;
                    nextMessageAt += TimeUnit.MILLISECONDS.toNanos(10);
                }
            }

            client.pollEgress();

            if (client.isClosed())
            {
                break;
            }

            if (!lossRequested && leaderService.messageCount() >= 5)
            {
                clientSendLossGenerator.startDropping(
                    endpointPort(client.ingressPublication().channel()),
                    imageLivenessTimeoutNs + TimeUnit.MILLISECONDS.toNanos(200));
                lossRequested = true;
            }

            Tests.sleep(1);
        }

        assertTrue(lossRequested);
        assertEquals(0, outOfSequenceCount.get());
        assertTrue(client.isClosed());
    }

    private int endpointPort(final String channel)
    {
        final ChannelUri uri = ChannelUri.parse(channel);
        final String endpoint = uri.get(ENDPOINT_PARAM_NAME);
        return Integer.parseInt(endpoint.substring(endpoint.indexOf(':') + 1));
    }

    private static SendChannelEndpointSupplier sendChannelEndpointSupplier(final LossGenerator lossGenerator)
    {
        return (udpChannel, statusIndicator, context) ->
        new DebugSendChannelEndpoint(udpChannel, statusIndicator, context, lossGenerator, lossGenerator);
    }

    private static class SequenceCheckingService extends TestNode.TestService
    {
        private final Long2LongHashMap nextSequenceBySessionId = new Long2LongHashMap(-1);
        private final AtomicLong outOfSequenceCount;

        SequenceCheckingService(final int index, final AtomicLong outOfSequenceCount)
        {
            this.outOfSequenceCount = outOfSequenceCount;
            index(index);
        }

        public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            final long sessionId = session.id();
            final long sequence = buffer.getLong(offset);
            final long expectedSequence = nextSequenceBySessionId.get(sessionId);
            if (sequence != expectedSequence)
            {
                System.out.println("expected sequence " + expectedSequence + ", but got " + sequence);
                outOfSequenceCount.incrementAndGet();
            }
            nextSequenceBySessionId.put(sessionId, sequence + 1);
            messageCount.incrementAndGet();
        }

        public void onSessionOpen(final ClientSession session, final long timestamp)
        {
            super.onSessionOpen(session, timestamp);
            nextSequenceBySessionId.put(session.id(), 0);
        }

        public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
        {
            super.onSessionClose(session, timestamp, closeReason);
            nextSequenceBySessionId.remove(session.id());
        }
    }

    private static final class PortLossGenerator implements LossGenerator
    {
        private int port;
        private long dropUntil;
        private volatile boolean drop;

        public void startDropping(final int port, final long durationNs)
        {
            this.port = port;
            dropUntil = System.nanoTime() + durationNs;
            drop = true;
        }

        public boolean shouldDropFrame(final InetSocketAddress address, final UnsafeBuffer buffer, final int length)
        {
            if (drop && port == address.getPort())
            {
                if (System.nanoTime() - dropUntil < 0)
                {
                    return true;
                }
                else
                {
                    drop = false;
                }
            }

            return false;
        }
    }
}
