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
import io.aeron.test.Tests;
import io.aeron.test.cluster.StubClusteredService;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.cluster.UnexpectedElectionTest.ClusterClient.NODE_0_INGRESS;
import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.computeFragmentedFrameLength;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({EventLogExtension.class, InterruptingTestCallback.class})
public class UnexpectedElectionTest
{
    private static final int EIGHT_MEGABYTES = 8 * 1024 * 1024;
    private static final int FRAME_LENGTH = Configuration.mtuLength() - DataHeaderFlyweight.HEADER_LENGTH;
    private static final int NEW_LEADERSHIP_TERM_LENGTH = computeFragmentedFrameLength(
        MessageHeaderEncoder.ENCODED_LENGTH + NewLeadershipTermEventEncoder.BLOCK_LENGTH, FRAME_LENGTH);
    private static final int SESSION_OPEN_LENGTH_BLOCK_LENGTH = computeFragmentedFrameLength(
        MessageHeaderEncoder.ENCODED_LENGTH + SessionOpenEventEncoder.BLOCK_LENGTH, FRAME_LENGTH);
    private static final int SESSION_CLOSE_LENGTH = computeFragmentedFrameLength(
        MessageHeaderEncoder.ENCODED_LENGTH + SessionCloseEventEncoder.BLOCK_LENGTH, FRAME_LENGTH);
    private static final int EIGHT_MEGABYTES_LENGTH = computeFragmentedFrameLength(EIGHT_MEGABYTES, FRAME_LENGTH);

    @TempDir
    public Path nodeDir0;
    @TempDir
    public Path nodeDir1;
    @TempDir
    public Path nodeDir2;
    @TempDir
    public Path clientDir;

    @SuppressWarnings("MethodLength")
    @Test
    @InterruptAfter(10)
    public void shouldElectionBetweenFragmentedServiceMessageAvoidDuplicateServiceMessage()
    {
        final AtomicBoolean waitingToOfferFragmentedMessage = new AtomicBoolean(true);
        try (ClusterNode node0 = new ClusterNode(0, 0, nodeDir0, waitingToOfferFragmentedMessage);
             ClusterNode node1 = new ClusterNode(1, 0, nodeDir1, waitingToOfferFragmentedMessage);
             ClusterNode node2 = new ClusterNode(2, 0, nodeDir2, waitingToOfferFragmentedMessage))
        {
            node0.consensusModuleContext.leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1));
            node1.consensusModuleContext.leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1));
            node2.consensusModuleContext.leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1));

            node0.launch();
            node1.launch();
            node2.launch();

            Tests.await(() ->
            {
                node0.poll();
                node1.poll();
                node2.poll();
                return node0.started() && node1.started() && node2.started();
            });
            assertTrue(node0.isLeader());
            final long initialLeadershipTermId = node0.leadershipTermId();

            try (ClusterClient client0 = new ClusterClient(NODE_0_INGRESS, clientDir))
            {
                Tests.await(() ->
                {
                    node0.poll(); node1.poll(); node2.poll();
                    return client0.connect();
                });
            }

            final long expectedPositionLowerBound =
                NEW_LEADERSHIP_TERM_LENGTH + SESSION_OPEN_LENGTH_BLOCK_LENGTH + SESSION_CLOSE_LENGTH;
            Tests.await(() ->
            {
                node0.poll();
                node1.poll();
                node2.poll();
                return node0.publicationPosition() > expectedPositionLowerBound &&
                    node0.publicationPosition() == node0.commitPosition() &&
                    node0.commitPosition() == node1.commitPosition() &&
                    node0.commitPosition() == node2.commitPosition();
            });

            final long commitPositionBeforeFragmentedMessage = node0.commitPosition();
            waitingToOfferFragmentedMessage.set(false);
            Tests.await(() ->
            {
                node0.poll();
                node1.poll();
                node2.poll();
                return node0.commitPosition() > commitPositionBeforeFragmentedMessage &&
                    node0.commitPosition() < (commitPositionBeforeFragmentedMessage + EIGHT_MEGABYTES);
            });

            final long expectedAppendPosition = commitPositionBeforeFragmentedMessage + EIGHT_MEGABYTES_LENGTH;
            Tests.await(() -> node0.appendPosition() == expectedAppendPosition);

            Tests.sleep(TimeUnit.NANOSECONDS.toMillis(node0.consensusModule.context().leaderHeartbeatTimeoutNs()) + 1);
            assertTrue(node0.consensusModulePosition() < node0.commitPosition());

            Tests.await(() ->
            {
                node0.poll();
                node1.poll();
                node2.poll();
                return node0.electionStarted();
            });
            assertEquals(node0.commitPosition(), node0.consensusModulePosition());

            Tests.await(() ->
            {
                node0.poll();
                node1.poll();
                node2.poll();
                return node0.started() && node1.started() && node2.started();
            });
            assertTrue(node0.isLeader());
            assertTrue(initialLeadershipTermId < node0.leadershipTermId());

            Tests.await(() ->
            {
                node0.poll();
                node1.poll();
                node2.poll();
                return node0.publicationPosition() == node0.commitPosition() &&
                    node0.commitPosition() == node1.commitPosition() &&
                    node0.commitPosition() == node2.commitPosition() &&
                    node0.commitPosition() == node0.servicePosition() &&
                    node1.commitPosition() == node1.servicePosition() &&
                    node2.commitPosition() == node2.servicePosition();
            });

            assertEquals(1, node0.offeredServiceMessages());
            assertEquals(1, node1.offeredServiceMessages());
            assertEquals(1, node2.offeredServiceMessages());

            assertEquals(1, node0.receivedServiceMessages());
            assertEquals(1, node1.receivedServiceMessages());
            assertEquals(1, node2.receivedServiceMessages());
        }
    }

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("methodlength")
    public void shouldHandleSnapshotWithFragmentedMessageInLog()
    {
        final AtomicBoolean waitingToOfferFragmentedMessage = new AtomicBoolean(true);
        try (ClusterNode node0 = new ClusterNode(0, 0, nodeDir0, waitingToOfferFragmentedMessage);
            ClusterNode node1 = new ClusterNode(1, 0, nodeDir1, waitingToOfferFragmentedMessage);
            ClusterNode node2 = new ClusterNode(2, 0, nodeDir2, waitingToOfferFragmentedMessage))
        {
            node0.consensusModuleContext.logFragmentLimit(1);
            node1.consensusModuleContext.logFragmentLimit(1000);
            node2.consensusModuleContext.logFragmentLimit(1000);
            node0.clusteredServiceContext.logFragmentLimit(1000);
            node1.clusteredServiceContext.logFragmentLimit(1000);
            node2.clusteredServiceContext.logFragmentLimit(1000);

            node0.launch();
            node1.launch();
            node2.launch();

            Tests.await(() ->
            {
                node0.poll();
                node1.poll();
                node2.poll();
                return node0.started() && node1.started() && node2.started();
            });
            assertTrue(node0.isLeader());

            try (ClusterClient client0 = new ClusterClient(NODE_0_INGRESS, clientDir))
            {
                Tests.await(() ->
                {
                    node0.poll();
                    node1.poll();
                    node2.poll();
                    return client0.connect();
                });
            }

            final long expectedPositionLowerBound =
                NEW_LEADERSHIP_TERM_LENGTH + SESSION_OPEN_LENGTH_BLOCK_LENGTH + SESSION_CLOSE_LENGTH;
            Tests.await(() ->
            {
                node0.poll();
                node1.poll();
                node2.poll();
                return node0.publicationPosition() > expectedPositionLowerBound &&
                    node0.publicationPosition() == node0.commitPosition() &&
                    node0.commitPosition() == node1.commitPosition() &&
                    node0.commitPosition() == node2.commitPosition();
            });

            final long commitPositionBeforeFragmentedMessage = node0.commitPosition();
            waitingToOfferFragmentedMessage.set(false);
            Tests.await(() ->
            {
                node0.poll();
                node1.poll();
                node2.poll();
                return
                    commitPositionBeforeFragmentedMessage < node0.commitPosition() &&
                    node0.commitPosition() < (commitPositionBeforeFragmentedMessage + EIGHT_MEGABYTES);
            });

            final long expectedAppendPosition = commitPositionBeforeFragmentedMessage + EIGHT_MEGABYTES_LENGTH;
            Tests.await(() -> node0.appendPosition() == expectedAppendPosition);

            final Counter controlToggle = node0.consensusModule.context().controlToggleCounter();
            controlToggle.setRelease(ClusterControl.ToggleState.SNAPSHOT.code());

            Tests.await(() ->
            {
                node0.poll();
                node1.poll();
                node2.poll();
                return ConsensusModule.State.SNAPSHOT == node0.clusterState();
            });

            Tests.await(() ->
            {
                node0.poll();
                node1.poll();
                node2.poll();
                return ConsensusModule.State.ACTIVE == node0.clusterState();
            });
            assertEquals(1L, node0.consensusModule.context().snapshotCounter().get());
            assertTrue(expectedAppendPosition < node0.servicePosition());
            assertTrue(node0.consensusModulePosition() < expectedAppendPosition);

            Tests.await(() ->
            {
                node0.poll();
                node1.poll();
                node2.poll();
                return expectedAppendPosition < node0.consensusModulePosition() &&
                    node0.publicationPosition() == node0.commitPosition() &&
                    node0.commitPosition() == node1.commitPosition() &&
                    node0.commitPosition() == node2.commitPosition() &&
                    node0.commitPosition() == node0.servicePosition() &&
                    node1.commitPosition() == node1.servicePosition() &&
                    node2.commitPosition() == node2.servicePosition();
            });

            assertEquals(1, node0.offeredServiceMessages());
            assertEquals(1, node1.offeredServiceMessages());
            assertEquals(1, node2.offeredServiceMessages());

            assertEquals(1, node0.receivedServiceMessages());
            assertEquals(1, node1.receivedServiceMessages());
            assertEquals(1, node2.receivedServiceMessages());
        }
    }

    static class ClusterNode implements AutoCloseable
    {
        private static final String MEMBERS =
            "0,localhost:10002,localhost:10003,localhost:10004,localhost:10005,localhost:10001|" +
            "1,localhost:10102,localhost:10103,localhost:10104,localhost:10105,localhost:10101|" +
            "2,localhost:10202,localhost:10203,localhost:10204,localhost:10205,localhost:10201";
        private static final Map<Integer, String> MEMBER_ARCHIVE_PORT = Map.of(0, "10001", 1, "10101", 2, "10201");

        private final MediaDriver.Context mediaDriverContext;
        private final Archive.Context archiveContext;
        private final ConsensusModule.Context consensusModuleContext;
        private final ClusteredServiceContainer.Context clusteredServiceContext;
        private final TestClusteredService clusteredService;

        private MediaDriver mediaDriver;
        private Archive archive;
        private ConsensusModule consensusModule;
        private ClusteredServiceContainer serviceContainer;

        private int recordingPositionCounterId = Aeron.NULL_VALUE;
        private int serviceSubscriberPositionCounterId = Aeron.NULL_VALUE;
        private int consensusModuleSubscriberPositionCounterId = Aeron.NULL_VALUE;

        ClusterNode(final int clusterMemberId,
            final int appointedLeader,
            final Path directory,
            final AtomicBoolean waiting)
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

            clusteredService = new TestClusteredService(waiting);
            clusteredServiceContext = new ClusteredServiceContainer.Context()
                .controlChannel("aeron:ipc")
                .clusterDir(directory.resolve("cluster").toFile())
                .aeronDirectoryName(mediaDriverContext.aeronDirectoryName())
                .clusteredService(clusteredService);

            consensusModuleContext = new ConsensusModule.Context()
                .appointedLeaderId(appointedLeader)
                .aeronDirectoryName(mediaDriverContext.aeronDirectoryName())
                .clusterDir(directory.resolve("cluster").toFile())
                .clusterMemberId(clusterMemberId)
                .clusterMembers(MEMBERS)
                .ingressChannel("aeron:udp")
                .egressChannel("aeron:udp")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .serviceCount(1)
                .useAgentInvoker(true);
        }

        public void launch()
        {
            mediaDriver = MediaDriver.launchEmbedded(mediaDriverContext);
            archive = Archive.launch(archiveContext);
            serviceContainer = ClusteredServiceContainer.launch(clusteredServiceContext);
            consensusModule = ConsensusModule.launch(consensusModuleContext);
        }

        public boolean started()
        {
            final ConsensusModule.Context context = consensusModule.context();
            return context.moduleStateCounter().get() == ConsensusModule.State.ACTIVE.code() &&
                context.electionStateCounter().get() == ElectionState.CLOSED.code() &&
                (context.clusterNodeRoleCounter().get() == Cluster.Role.LEADER.code() ||
                context.clusterNodeRoleCounter().get() == Cluster.Role.FOLLOWER.code());
        }

        public boolean isLeader()
        {
            return Cluster.Role.LEADER.code() == consensusModule.context().clusterNodeRoleCounter().get();
        }

        public void poll()
        {
            consensusModule.conductorAgentInvoker().invoke();
        }

        public long publicationPosition()
        {
            return consensusModule.context().logPublisher().position();
        }

        public long commitPosition()
        {
            return consensusModule.context().commitPositionCounter().get();
        }

        public long leadershipTermId()
        {
            return consensusModule.context().leadershipTermIdCounter().get();
        }

        public ConsensusModule.State clusterState()
        {
            return ConsensusModule.State.get(consensusModule.context().moduleStateCounter());
        }

        public long appendPosition()
        {
            final Aeron aeron = serviceContainer.context().aeron();
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

        public long consensusModulePosition()
        {
            final Aeron aeron = consensusModule.context().aeron();
            final CountersReader countersReader = aeron.countersReader();

            if (Aeron.NULL_VALUE == consensusModuleSubscriberPositionCounterId)
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
                            consensusModuleSubscriberPositionCounterId = counterId;
                        }
                    }
                });
            }

            assertNotEquals(Aeron.NULL_VALUE, consensusModuleSubscriberPositionCounterId);
            return countersReader.getCounterValue(consensusModuleSubscriberPositionCounterId);
        }

        public int offeredServiceMessages()
        {
            return clusteredService.offeredMessages();
        }

        public int receivedServiceMessages()
        {
            return clusteredService.receivedMessages();
        }

        public boolean electionStarted()
        {
            return ElectionState.CLOSED != ElectionState.get(consensusModule.context().electionStateCounter());
        }

        @Override
        public void close()
        {
            CloseHelper.closeAll(serviceContainer, consensusModule, archive, mediaDriver);
        }

        static class TestClusteredService extends StubClusteredService
        {

            private static final UnsafeBuffer EIGHT_MEGABYTE_BUFFER =
                new UnsafeBuffer(new byte[EIGHT_MEGABYTES - SESSION_HEADER_LENGTH]);

            static
            {
                EIGHT_MEGABYTE_BUFFER.setMemory(0, EIGHT_MEGABYTE_BUFFER.capacity(), (byte)'x');
            }

            private final AtomicBoolean waiting;
            int offeredMessages;
            int receivedMessages;

            TestClusteredService(final AtomicBoolean waiting)
            {
                this.waiting = waiting;
            }

            int offeredMessages()
            {
                return offeredMessages;
            }

            int receivedMessages()
            {
                return receivedMessages;
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

                ++offeredMessages;
            }

            public void onSessionMessage(
                final ClientSession session,
                final long timestamp,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header)
            {
                ++receivedMessages;
            }
        }
    }

    static class ClusterClient implements ControlledEgressListener, AutoCloseable
    {
        static final String NODE_0_INGRESS = "0=localhost:10002";

        private MediaDriver mediaDriver;
        private AeronCluster.AsyncConnect asyncConnect;
        private AeronCluster aeronCluster;

        ClusterClient(final String ingressEndpoints, final Path directory)
        {
            final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
                .aeronDirectoryName(directory.resolve("driver").toAbsolutePath().toString())
                .dirDeleteOnStart(true)
                .termBufferSparseFile(true)
                .publicationTermBufferLength(64 * 1024)
                .ipcPublicationTermWindowLength(64 * 1024)
                .threadingMode(ThreadingMode.SHARED);
            final AeronCluster.Context aeronClusterContext = new AeronCluster.Context()
                .aeronDirectoryName(mediaDriverContext.aeronDirectoryName())
                .ingressEndpoints(ingressEndpoints)
                .ingressChannel("aeron:udp")
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .controlledEgressListener(this);

            mediaDriver = MediaDriver.launch(mediaDriverContext);
            asyncConnect = AeronCluster.asyncConnect(aeronClusterContext);
        }

        @Override
        public ControlledFragmentHandler.Action onMessage(
                final long clusterSessionId,
                final long timestamp,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header)
        {
            return ControlledFragmentHandler.Action.CONTINUE;
        }

        public boolean connect()
        {
            aeronCluster = asyncConnect.poll();
            return aeronCluster != null;
        }

        @Override
        public void close()
        {
            CloseHelper.closeAll(aeronCluster, asyncConnect, mediaDriver);
        }
    }
}
