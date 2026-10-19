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

import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.Header;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.cluster.ClusterTests;
import io.aeron.test.cluster.StubClusteredService;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.atomic.AtomicLong;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.cluster.ClusterTestConstants.CLUSTER_MEMBERS;
import static io.aeron.cluster.ClusterTestConstants.INGRESS_ENDPOINTS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(InterruptingTestCallback.class)
class ClusterNodeTest
{
    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;
    private AeronCluster aeronCluster;

    @BeforeEach
    void before()
    {
        clusteredMediaDriver = ClusteredMediaDriver.launch(
            new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .errorHandler(ClusterTests.errorHandler(0))
                .dirDeleteOnStart(true),
            TestContexts.localhostArchive()
                .catalogCapacity(ClusterTestConstants.CATALOG_CAPACITY)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .recordingEventsEnabled(false)
                .deleteArchiveOnStart(true),
            new ConsensusModule.Context()
                .errorHandler(ClusterTests.errorHandler(0))
                .terminationHook(ClusterTests.NOOP_TERMINATION_HOOK)
                .clusterMembers(CLUSTER_MEMBERS)
                .ingressChannel("aeron:udp")
                .logChannel("aeron:ipc")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .deleteDirOnStart(true));
    }

    @AfterEach
    void after()
    {
        final ConsensusModule consensusModule = null == clusteredMediaDriver ?
            null : clusteredMediaDriver.consensusModule();

        CloseHelper.closeAll(aeronCluster, consensusModule, container, clusteredMediaDriver);

        if (null != clusteredMediaDriver)
        {
            clusteredMediaDriver.consensusModule().context().deleteDirectory();
            clusteredMediaDriver.archive().context().deleteDirectory();
            clusteredMediaDriver.mediaDriver().context().deleteDirectory();
            container.context().deleteDirectory();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldConnectAndSendKeepAlive()
    {
        container = launchEchoService();
        aeronCluster = connectToCluster(null);

        assertTrue(aeronCluster.sendKeepAlive());

        ClusterTests.failOnClusterError();
    }

    @Test
    @InterruptAfter(10)
    void shouldEchoMessageViaServiceUsingDirectOffer()
    {
        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        final String msg = "Hello World!";
        msgBuffer.putStringWithoutLengthAscii(0, msg);

        final MutableInteger messageCount = new MutableInteger();

        final EgressListener listener =
            (clusterSessionId, timestamp, buffer, offset, length, header) ->
            {
                assertEquals(msg, buffer.getStringWithoutLengthAscii(offset, length));
                messageCount.value += 1;
            };

        container = launchEchoService();
        aeronCluster = connectToCluster(listener);

        offerMessage(msgBuffer, msg);
        awaitResponse(messageCount);

        ClusterTests.failOnClusterError();
    }

    @Test
    @InterruptAfter(10)
    void shouldEchoMessageViaServiceUsingTryClaim()
    {
        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        final String msg = "Hello World!";
        msgBuffer.putStringWithoutLengthAscii(0, msg);

        final MutableInteger messageCount = new MutableInteger();

        final EgressListener listener =
            (clusterSessionId, timestamp, buffer, offset, length, header) ->
            {
                assertEquals(msg, buffer.getStringWithoutLengthAscii(offset, length));
                messageCount.value += 1;
            };

        container = launchEchoService();
        aeronCluster = connectToCluster(listener);

        final BufferClaim bufferClaim = new BufferClaim();
        long publicationResult;
        do
        {
            publicationResult = aeronCluster.tryClaim(msg.length(), bufferClaim);
            if (publicationResult > 0)
            {
                final int offset = bufferClaim.offset() + AeronCluster.SESSION_HEADER_LENGTH;
                bufferClaim.buffer().putBytes(offset, msgBuffer, 0, msg.length());
                bufferClaim.commit();
            }
            else
            {
                Tests.yield();
            }
        }
        while (publicationResult < 0);

        offerMessage(msgBuffer, msg);
        awaitResponse(messageCount);

        ClusterTests.failOnClusterError();
    }

    @Test
    @InterruptAfter(10)
    void shouldScheduleEventInService()
    {
        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        final String msg = "Hello World!";
        msgBuffer.putStringWithoutLengthAscii(0, msg);

        final MutableInteger messageCount = new MutableInteger();

        final EgressListener listener =
            (clusterSessionId, timestamp, buffer, offset, length, header) ->
            {
                final String expected = msg + "-scheduled";
                assertEquals(expected, buffer.getStringWithoutLengthAscii(offset, length));
                messageCount.value += 1;
            };

        container = launchTimedService();
        aeronCluster = connectToCluster(listener);

        offerMessage(msgBuffer, msg);
        awaitResponse(messageCount);

        ClusterTests.failOnClusterError();
    }

    @Test
    @InterruptAfter(10)
    void shouldSendResponseAfterServiceMessage()
    {
        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        final String msg = "Hello World!";
        msgBuffer.putStringWithoutLengthAscii(0, msg);

        final MutableInteger messageCount = new MutableInteger();

        final EgressListener listener =
            (clusterSessionId, timestamp, buffer, offset, length, header) ->
            {
                assertEquals(msg, buffer.getStringWithoutLengthAscii(offset, length));
                messageCount.value += 1;
            };

        container = launchServiceMessageIngressService();
        aeronCluster = connectToCluster(listener);

        offerMessage(msgBuffer, msg);
        awaitResponse(messageCount);

        ClusterTests.failOnClusterError();
    }

    @Test
    @InterruptAfter(10)
    void shouldTerminateServiceWhenServiceClientTimesOut()
    {
        final AtomicLong clockOffsetMs = new AtomicLong();
        final ClusteredServiceContainer.Context containerCtx = new ClusteredServiceContainer.Context()
            .clusteredService(new StubClusteredService())
            .epochClock(() -> System.currentTimeMillis() + clockOffsetMs.get());

        container = ClusteredServiceContainer.launch(containerCtx);
        aeronCluster = connectToCluster(null);

        // step the service's clock past the driver timeout, as after a process stall
        clockOffsetMs.set(containerCtx.aeron().context().driverTimeoutMs() + 1);

        // the service marks itself terminated once it has closed
        Tests.await(() -> NULL_VALUE == containerCtx.clusterMarkFile().activityTimestampVolatile());
    }

    private void offerMessage(final ExpandableArrayBuffer msgBuffer, final String msg)
    {
        while (aeronCluster.offer(msgBuffer, 0, msg.length()) < 0)
        {
            Tests.yield();
        }
    }

    private void awaitResponse(final MutableInteger messageCount)
    {
        while (messageCount.get() == 0)
        {
            if (aeronCluster.pollEgress() <= 0)
            {
                Tests.yield();
            }
        }
    }

    private ClusteredServiceContainer launchEchoService()
    {
        final ClusteredService clusteredService = new StubClusteredService()
        {
            public void onSessionMessage(
                final ClientSession session,
                final long timestamp,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header)
            {
                echoMessage(session, buffer, offset, length);
            }
        };

        return ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(clusteredService)
                .errorHandler(Tests::onError));
    }

    private ClusteredServiceContainer launchTimedService()
    {
        final ClusteredService clusteredService = new StubClusteredService()
        {
            private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
            private long clusterSessionId;
            private int nextCorrelationId;
            private String msg;

            public void onSessionMessage(
                final ClientSession session,
                final long timestamp,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header)
            {
                clusterSessionId = session.id();
                msg = buffer.getStringWithoutLengthAscii(offset, length);
                final long correlationId = serviceCorrelationId(nextCorrelationId++);

                idleStrategy.reset();
                while (!cluster.scheduleTimer(correlationId, timestamp + 100))
                {
                    idleStrategy.idle();
                }
            }

            public void onTimerEvent(final long correlationId, final long timestamp)
            {
                final String responseMsg = msg + "-scheduled";
                buffer.putStringWithoutLengthAscii(0, responseMsg);
                final ClientSession clientSession = cluster.getClientSession(clusterSessionId);

                echoMessage(clientSession, buffer, 0, responseMsg.length());
            }
        };

        return ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(clusteredService)
                .terminationHook(ClusterTests.NOOP_TERMINATION_HOOK)
                .errorHandler(ClusterTests.errorHandler(0)));
    }

    private ClusteredServiceContainer launchServiceMessageIngressService()
    {
        final ClusteredService clusteredService = new StubClusteredService()
        {
            public void onSessionMessage(
                final ClientSession session,
                final long timestamp,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                @SuppressWarnings("unused") final Header header)
            {
                if (null != session)
                {
                    idleStrategy.reset();
                    while (cluster.offer(buffer, offset, length) < 0)
                    {
                        idleStrategy.idle();
                    }
                }
                else
                {
                    cluster.forEachClientSession((clientSession) -> echoMessage(clientSession, buffer, offset, length));
                }
            }
        };

        return ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(clusteredService)
                .terminationHook(ClusterTests.NOOP_TERMINATION_HOOK)
                .errorHandler(ClusterTests.errorHandler(0)));
    }

    private AeronCluster connectToCluster(final EgressListener egressListener)
    {
        return AeronCluster.connect(
            new AeronCluster.Context()
                .egressListener(egressListener)
                .ingressChannel("aeron:udp")
                .ingressEndpoints(INGRESS_ENDPOINTS)
                .egressChannel("aeron:udp?endpoint=localhost:0"));
    }
}
