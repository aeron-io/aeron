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

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ControlledEgressListener;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.NewLeadershipTermEventEncoder;
import io.aeron.cluster.codecs.SessionCloseEventEncoder;
import io.aeron.cluster.codecs.SessionOpenEventEncoder;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.status.StreamCounter;
import io.aeron.driver.status.SubscriberPos;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.StubClusteredService;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.computeFragmentedFrameLength;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class})
class ClusterReplayFragmentedTest
{
    private static final int NODE_COUNT = 3;
    private static final String MEMBERS =
        "0,localhost:10002,localhost:10003,localhost:10004,localhost:10005,localhost:10001|" +
            "1,localhost:10102,localhost:10103,localhost:10104,localhost:10105,localhost:10101|" +
            "2,localhost:10202,localhost:10203,localhost:10204,localhost:10205,localhost:10201";
    private static final Map<Integer, String> MEMBER_ARCHIVE_PORT = Map.of(0, "10001", 1, "10101", 2, "10201");
    private static final String MEMBER_INGRESS_ENDPOINTS = "0=localhost:10002|1=localhost:10102|2=localhost:10202";

    private static final int EIGHT_MEGABYTES = 8 * 1024 * 1024;
    private static final int FRAME_LENGTH = Configuration.mtuLength() - DataHeaderFlyweight.HEADER_LENGTH;
    static final int EIGHT_MEGABYTES_LENGTH = computeFragmentedFrameLength(EIGHT_MEGABYTES, FRAME_LENGTH);
    static final int SESSION_CLOSE_LENGTH = computeFragmentedFrameLength(
        MessageHeaderEncoder.ENCODED_LENGTH + SessionCloseEventEncoder.BLOCK_LENGTH, FRAME_LENGTH);
    static final int SESSION_OPEN_BLOCK_LENGTH = computeFragmentedFrameLength(
        MessageHeaderEncoder.ENCODED_LENGTH + SessionOpenEventEncoder.BLOCK_LENGTH, FRAME_LENGTH);
    static final int NEW_LEADERSHIP_TERM_LENGTH = computeFragmentedFrameLength(
        MessageHeaderEncoder.ENCODED_LENGTH + NewLeadershipTermEventEncoder.BLOCK_LENGTH, FRAME_LENGTH);

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    @TempDir
    public Path tempDir;

    @Test
    @SlowTest
    @InterruptAfter(20)
    public void shouldElectionBetweenFragmentedServiceMessageAvoidDuplicateServiceMessage()
    {
        final AtomicBoolean waitingToOfferFragmentedMessage = new AtomicBoolean(true);

        try (TestCluster cluster = new TestCluster(tempDir))
        {
            cluster.withServiceSupplier(() -> new OfferMessageOnSessionCloseService(waitingToOfferFragmentedMessage));
            cluster.node(0).consensusModuleContext().appointedLeaderId(0);
            cluster.node(1).consensusModuleContext().appointedLeaderId(0);
            cluster.node(2).consensusModuleContext().appointedLeaderId(0);
            cluster.launch();
            cluster.awaitStarted();

            final TestNode leader = cluster.leader();
            final TestNode follower1 = cluster.node(1);
            final TestNode follower2 = cluster.node(2);
            final Counter leaderElectionStateCounter = leader.consensusModuleContext().electionStateCounter();
            final long initialLeadershipTermId = leader.consensusModuleContext().leadershipTermIdCounter().get();
            assertTrue(leader.isLeader());

            final long expectedPositionLowerBound =
                NEW_LEADERSHIP_TERM_LENGTH + SESSION_OPEN_BLOCK_LENGTH + SESSION_CLOSE_LENGTH;
            cluster.connectAndCloseClient();
            Tests.await(() ->
            {
                cluster.poll();
                return leader.publicationPosition() > expectedPositionLowerBound &&
                    leader.publicationPosition() == leader.commitPosition() &&
                    leader.commitPosition() == follower1.commitPosition() &&
                    leader.commitPosition() == follower2.commitPosition();
            });

            final long commitPositionBeforeFragmentedMessage = leader.commitPosition();
            waitingToOfferFragmentedMessage.set(false);
            Tests.await(() ->
            {
                cluster.poll();
                return leader.commitPosition() > commitPositionBeforeFragmentedMessage &&
                    leader.commitPosition() < (commitPositionBeforeFragmentedMessage + EIGHT_MEGABYTES);
            });

            final long expectedAppendPosition = commitPositionBeforeFragmentedMessage + EIGHT_MEGABYTES_LENGTH;
            Tests.await(() -> leader.appendPosition() == expectedAppendPosition);

            Tests.sleep(TimeUnit.NANOSECONDS.toMillis(leader.consensusModuleContext().leaderHeartbeatTimeoutNs() * 2));
            leader.poll();
            assertNotEquals(ElectionState.CLOSED, ElectionState.get(leaderElectionStateCounter));

            cluster.awaitStarted();
            assertTrue(leader.isLeader());
            assertTrue(initialLeadershipTermId < leader.consensusModuleContext().leadershipTermIdCounter().get());

            Tests.await(() ->
            {
                cluster.poll();
                return leader.publicationPosition() == leader.commitPosition() &&
                    leader.commitPosition() == follower1.commitPosition() &&
                    leader.commitPosition() == follower2.commitPosition() &&
                    leader.commitPosition() == leader.servicePosition() &&
                    follower1.commitPosition() == follower1.servicePosition() &&
                    follower2.commitPosition() == follower2.servicePosition();
            });

            for (int i = 0; i < NODE_COUNT; ++i)
            {
                final OfferMessageOnSessionCloseService service =
                    (OfferMessageOnSessionCloseService)cluster.node(0).clusteredService;
                assertEquals(1, service.offeredMessages(), "Member " + i + " has incorrect offered messages");
                assertEquals(1, service.receivedMessages(), "Member " + i + " has incorrect received messages");
            }
        }
    }

    private final class TestCluster implements AutoCloseable
    {
        TestNode[] nodes = new TestNode[NODE_COUNT];
        Supplier<ClusteredService> clusteredServiceSupplier = () -> null;

        private TestCluster(final Path tempDir)
        {
            for (int i = 0; i < NODE_COUNT; ++i)
            {
                nodes[i] = new TestNode(i, tempDir.resolve("node-" + i));
            }
        }

        private void withServiceSupplier(final Supplier<ClusteredService> clusteredServiceSupplier)
        {
            this.clusteredServiceSupplier = clusteredServiceSupplier;
        }

        private void launch()
        {
            for (final TestNode node : nodes)
            {
                node.withService(clusteredServiceSupplier.get());
                node.launch(testWatcher);
            }
        }

        private TestNode node(final int i)
        {
            return nodes[i];
        }

        private void poll()
        {
            poll(Aeron.NULL_VALUE);
        }

        private void poll(final int except)
        {
            for (int i = 0; i < nodes.length; ++i)
            {
                if (i != except)
                {
                    nodes[i].poll();
                }
            }
        }

        private void awaitStarted()
        {
            awaitStarted(Aeron.NULL_VALUE);
        }

        private void awaitStarted(final int except)
        {
            Tests.await(() ->
            {
                boolean started = true;
                for (int i = 0; i < nodes.length; ++i)
                {
                    if (i != except)
                    {
                        nodes[i].poll();
                        started &= nodes[i].started();
                    }
                }
                return started;
            });
        }

        private TestNode leader()
        {
            return leader(Aeron.NULL_VALUE);
        }

        private TestNode leader(final int except)
        {
            for (int i = 0; i < nodes.length; ++i)
            {
                final TestNode node = nodes[i];
                if (i != except && node.isLeader())
                {
                    return node;
                }
            }

            return null;
        }

        @SuppressWarnings("try")
        private void connectAndCloseClient()
        {
            final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
                .aeronDirectoryName(tempDir.resolve("client-driver").toAbsolutePath().toString())
                .dirDeleteOnStart(true)
                .termBufferSparseFile(true)
                .publicationTermBufferLength(64 * 1024)
                .ipcPublicationTermWindowLength(64 * 1024)
                .threadingMode(ThreadingMode.SHARED);

            final ControlledEgressListener noOpControlledEgressListener =
                (sessionId, timestamp, buffer, offset, length, header) -> ControlledFragmentHandler.Action.CONTINUE;

            final AeronCluster.Context aeronClusterContext = new AeronCluster.Context()
                .aeronDirectoryName(mediaDriverContext.aeronDirectoryName())
                .ingressEndpoints(MEMBER_INGRESS_ENDPOINTS)
                .ingressChannel("aeron:udp")
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .controlledEgressListener(noOpControlledEgressListener);

            try (TestMediaDriver clientMediaDriver = TestMediaDriver.launch(mediaDriverContext, testWatcher);
                AeronCluster.AsyncConnect asyncConnect = AeronCluster.asyncConnect(aeronClusterContext))
            {
                final MutableReference<AeronCluster> aeronClusterRef = new MutableReference<>();
                Tests.await(() ->
                {
                    poll();
                    aeronClusterRef.set(asyncConnect.poll());
                    return null != aeronClusterRef.get();
                });
                CloseHelper.close(aeronClusterRef.get());
            }
        }

        @Override
        public void close()
        {
            CloseHelper.quietCloseAll(nodes);
        }
    }

    private static final class TestNode implements AutoCloseable
    {
        private final MediaDriver.Context mediaDriverContext;
        private final Archive.Context archiveContext;
        private final ConsensusModule.Context consensusModuleContext;
        private final ClusteredServiceContainer.Context clusteredServiceContext;

        private ClusteredService clusteredService;

        private TestMediaDriver mediaDriver;
        private Archive archive;
        private ConsensusModule consensusModule;
        private ClusteredServiceContainer serviceContainer;

        private int recordingPositionCounterId = Aeron.NULL_VALUE;
        private int serviceSubscriberPositionCounterId = Aeron.NULL_VALUE;

        private TestNode(final int clusterMemberId, final Path directory)
        {
            mediaDriverContext = new MediaDriver.Context()
                .aeronDirectoryName(directory.resolve("driver").toAbsolutePath().toString())
                .dirDeleteOnStart(true)
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED);

            archiveContext = new Archive.Context()
                .aeronDirectoryName(mediaDriverContext.aeronDirectoryName())
                .archiveDir(directory.resolve("archive").toFile())
                .controlChannel("aeron:udp?endpoint=localhost:" + MEMBER_ARCHIVE_PORT.get(clusterMemberId))
                .recordingEventsEnabled(false)
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .threadingMode(ArchiveThreadingMode.SHARED);

            clusteredServiceContext = new ClusteredServiceContainer.Context()
                .controlChannel("aeron:ipc")
                .clusterDir(directory.resolve("cluster").toFile())
                .aeronDirectoryName(mediaDriverContext.aeronDirectoryName());

            consensusModuleContext = new ConsensusModule.Context()
                .aeronDirectoryName(mediaDriverContext.aeronDirectoryName())
                .clusterDir(directory.resolve("cluster").toFile())
                .clusterMemberId(clusterMemberId)
                .clusterMembers(MEMBERS)
                .ingressChannel("aeron:udp")
                .egressChannel("aeron:udp")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .serviceCount(1)
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .useAgentInvoker(true);
        }

        private void withService(final ClusteredService clusteredService)
        {
            this.clusteredService = clusteredService;
        }

        private void launch(final SystemTestWatcher testWatcher)
        {
            mediaDriver = TestMediaDriver.launch(mediaDriverContext, testWatcher);
            archive = Archive.launch(archiveContext);
            clusteredServiceContext.clusteredService(clusteredService);
            serviceContainer = ClusteredServiceContainer.launch(clusteredServiceContext);
            consensusModule = ConsensusModule.launch(consensusModuleContext);
        }

        private MediaDriver.Context mediaDriverContext()
        {
            return mediaDriverContext;
        }

        private Archive.Context archiveContext()
        {
            return archiveContext;
        }

        private ConsensusModule.Context consensusModuleContext()
        {
            return consensusModuleContext;
        }

        private ClusteredServiceContainer.Context clusteredServiceContext()
        {
            return clusteredServiceContext;
        }

        private boolean started()
        {
            final ConsensusModule.Context context = consensusModule.context();
            return context.electionStateCounter().get() == ElectionState.CLOSED.code() &&
                (context.clusterNodeRoleCounter().get() == Cluster.Role.LEADER.code() ||
                    context.clusterNodeRoleCounter().get() == Cluster.Role.FOLLOWER.code());
        }

        private boolean isLeader()
        {
            return Cluster.Role.LEADER.code() == consensusModule.context().clusterNodeRoleCounter().get();
        }

        private void poll()
        {
            consensusModule.conductorAgentInvoker().invoke();
        }

        public long publicationPosition()
        {
            return consensusModule.context().logPublisher().position();
        }

        public long appendPosition()
        {
            final Aeron aeron = consensusModule.context().aeron();
            final CountersReader countersReader = aeron.countersReader();

            if (Aeron.NULL_VALUE == recordingPositionCounterId)
            {
                countersReader.forEach((counterId, typeId, keyBuffer, label) ->
                {
                    if (RecordingPos.RECORDING_POSITION_TYPE_ID == typeId &&
                        label.contains("alias=log"))
                    {
                        recordingPositionCounterId = counterId;
                    }
                });
            }

            assertNotEquals(Aeron.NULL_VALUE, recordingPositionCounterId);
            return countersReader.getCounterValue(recordingPositionCounterId);
        }

        public long commitPosition()
        {
            return consensusModule.context().commitPositionCounter().get();
        }

        public long servicePosition()
        {
            final Aeron aeron = serviceContainer.context().aeron();
            final CountersReader countersReader = aeron.countersReader();

            if (Aeron.NULL_VALUE == serviceSubscriberPositionCounterId)
            {
                final long aeronClientId = aeron.clientId();
                countersReader.forEach((counterId, typeId, keyBuffer, label) ->
                {
                    final int streamId = keyBuffer.getInt(StreamCounter.STREAM_ID_OFFSET);
                    if (SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID == typeId &&
                        consensusModule.context().logStreamId() == streamId)
                    {
                        if (countersReader.getCounterOwnerId(counterId) == aeronClientId)
                        {
                            serviceSubscriberPositionCounterId = counterId;
                        }
                    }
                });
            }

            assertNotEquals(Aeron.NULL_VALUE, serviceSubscriberPositionCounterId);
            return countersReader.getCounterValue(serviceSubscriberPositionCounterId);
        }

        @Override
        public void close()
        {
            CloseHelper.closeAll(serviceContainer, consensusModule, archive, mediaDriver);
        }
    }

    static class OfferMessageOnSessionCloseService extends StubClusteredService
    {

        private static final UnsafeBuffer EIGHT_MEGABYTE_BUFFER =
            new UnsafeBuffer(new byte[EIGHT_MEGABYTES - SESSION_HEADER_LENGTH]);

        static
        {
            EIGHT_MEGABYTE_BUFFER.setMemory(0, EIGHT_MEGABYTE_BUFFER.capacity(), (byte)'x');
        }

        private final AtomicBoolean waiting;
        private final AtomicInteger offeredMessages = new AtomicInteger(0);
        private final AtomicInteger receivedMessages = new AtomicInteger(0);

        OfferMessageOnSessionCloseService(final AtomicBoolean waiting)
        {
            this.waiting = waiting;
        }

        int offeredMessages()
        {
            return offeredMessages.get();
        }

        int receivedMessages()
        {
            return receivedMessages.get();
        }

        public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
        {
            cluster.idleStrategy().reset();
            while (waiting.get())
            {
                cluster.idleStrategy().idle();
            }

            while (0 > cluster.offer(EIGHT_MEGABYTE_BUFFER, 0, EIGHT_MEGABYTE_BUFFER.capacity()))
            {
                cluster.idleStrategy().idle();
            }

            offeredMessages.incrementAndGet();
        }

        public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            receivedMessages.incrementAndGet();
        }
    }
}
