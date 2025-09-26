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
import io.aeron.test.cluster.StubClusteredService;
import io.aeron.test.cluster.TestNode;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.computeFragmentedFrameLength;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ClusterExtensionTestUtil
{

    static final int EIGHT_MEGABYTES = 8 * 1024 * 1024;
    private static final int FRAME_LENGTH = Configuration.mtuLength() - DataHeaderFlyweight.HEADER_LENGTH;
    static final int EIGHT_MEGABYTES_LENGTH = computeFragmentedFrameLength(EIGHT_MEGABYTES, FRAME_LENGTH);
    static final int SESSION_CLOSE_LENGTH = computeFragmentedFrameLength(
        MessageHeaderEncoder.ENCODED_LENGTH + SessionCloseEventEncoder.BLOCK_LENGTH, FRAME_LENGTH);
    static final int SESSION_OPEN_LENGTH_BLOCK_LENGTH = computeFragmentedFrameLength(
            MessageHeaderEncoder.ENCODED_LENGTH + SessionOpenEventEncoder.BLOCK_LENGTH, FRAME_LENGTH);
    static final int NEW_LEADERSHIP_TERM_LENGTH = computeFragmentedFrameLength(
                MessageHeaderEncoder.ENCODED_LENGTH + NewLeadershipTermEventEncoder.BLOCK_LENGTH, FRAME_LENGTH);

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

        ClusterNode(final int clusterMemberId, final Path directory, final AtomicBoolean waiting)
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
            if (null == consensusModuleContext.consensusModuleExtension())
            {
                serviceContainer = ClusteredServiceContainer.launch(clusteredServiceContext);
            }
            consensusModule = ConsensusModule.launch(consensusModuleContext);
        }

        public ConsensusModule.Context consensusModuleContext()
        {
            return consensusModuleContext;
        }

        public ClusteredServiceContainer.Context clusteredServiceContext()
        {
            return clusteredServiceContext;
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

        public long extensionIngressCount()
        {
            final TestNode.TestConsensusModuleExtension testConsensusModuleExtension =
                (TestNode.TestConsensusModuleExtension)consensusModuleContext.consensusModuleExtension();
            return testConsensusModuleExtension.ingressMessageCount().get();
        }

        public List<TestNode.TestExtensionSnapshot> extensionSnapshots()
        {
            final TestNode.TestConsensusModuleExtension testConsensusModuleExtension =
                (TestNode.TestConsensusModuleExtension)consensusModuleContext.consensusModuleExtension();
            return testConsensusModuleExtension.snapshots();
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

        private MediaDriver.Context mediaDriverContext;
        private AeronCluster.Context aeronClusterContext;

        private MediaDriver mediaDriver;
        private AeronCluster.AsyncConnect asyncConnect;
        private AeronCluster aeronCluster;

        ClusterClient(final String ingressEndpoints, final Path directory)
        {
            mediaDriverContext = new MediaDriver.Context()
                .aeronDirectoryName(directory.resolve("driver").toAbsolutePath().toString())
                .dirDeleteOnStart(true)
                .termBufferSparseFile(true)
                .publicationTermBufferLength(64 * 1024)
                .ipcPublicationTermWindowLength(64 * 1024)
                .threadingMode(ThreadingMode.SHARED);
            aeronClusterContext = new AeronCluster.Context()
                .aeronDirectoryName(mediaDriverContext.aeronDirectoryName())
                .ingressEndpoints(ingressEndpoints)
                .ingressChannel("aeron:udp")
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .controlledEgressListener(this);
        }

        public MediaDriver.Context mediaDriverContext()
        {
            return mediaDriverContext;
        }

        public void launch()
        {
            mediaDriver = MediaDriver.launch(mediaDriverContext);
            asyncConnect = AeronCluster.asyncConnect(aeronClusterContext);
        }

        public AeronCluster aeronCluster()
        {
            return aeronCluster;
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
