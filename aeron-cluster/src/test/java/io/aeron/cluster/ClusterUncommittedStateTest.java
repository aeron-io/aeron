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
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ControlledEgressListener;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.cluster.codecs.SessionMessageHeaderEncoder;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
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
import io.aeron.test.cluster.TestNode.TestConsensusModuleExtension;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.LongArrayQueue;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class})
public class ClusterUncommittedStateTest
{
    private static final int NODE_COUNT = 3;
    private static final String MEMBERS =
        "0,localhost:10002,localhost:10003,localhost:10004,localhost:10005,localhost:10001|" +
        "1,localhost:10102,localhost:10103,localhost:10104,localhost:10105,localhost:10101|" +
        "2,localhost:10202,localhost:10203,localhost:10204,localhost:10205,localhost:10201";
    private static final Map<Integer, String> MEMBER_ARCHIVE_PORT = Map.of(0, "10001", 1, "10101", 2, "10201");
    private static final String MEMBER_INGRESS_ENDPOINTS = "0=localhost:10002|1=localhost:10102|2=localhost:10202";
    private static final long SLOW_TICK_INTERVAL_MS = 20;
    private static final long CLIENT_MESSAGE_COUNT = 32;
    private static final int CLIENT_MESSAGE_SIZE = 1024;
    private static final int EXTENSION_SCHEMA = 7777;

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    @TempDir
    public Path tempDir;

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @SlowTest
    @InterruptAfter(20)
    public void shouldRollbackUncommittedSuspendControlToggle(final boolean hasServices)
    {
        try (TestCluster testCluster = new TestCluster(tempDir))
        {
            if (hasServices)
            {
                testCluster.withServiceSupplier(StubClusteredService::new);
            }
            else
            {
                testCluster.withExtensionSupplier(SnapshottingCounterExtension::new);
            }
            testCluster.launch();
            testCluster.awaitStarted();

            final TestNode leader = testCluster.leader();
            final Counter leaderToggle = leader.consensusModuleContext().controlToggleCounter();
            final Counter leaderStateCounter = leader.consensusModuleContext().moduleStateCounter();

            testCluster.generateUncommittedDataAgainstLeader(leader);

            leaderToggle.setRelease(ClusterControl.ToggleState.SUSPEND.code());
            Tests.sleep(SLOW_TICK_INTERVAL_MS);
            leader.poll();
            assertEquals(ConsensusModule.State.SUSPENDED, ConsensusModule.State.get(leaderStateCounter));

            testCluster.generateNewElectionForLeader(leader);
            assertEquals(ConsensusModule.State.ACTIVE, ConsensusModule.State.get(leaderStateCounter));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @SlowTest
    @InterruptAfter(20)
    public void shouldRollbackUncommittedResumeControlToggle(final boolean hasServices)
    {
        try (TestCluster testCluster = new TestCluster(tempDir))
        {
            if (hasServices)
            {
                testCluster.withServiceSupplier(StubClusteredService::new);
            }
            else
            {
                testCluster.withExtensionSupplier(SnapshottingCounterExtension::new);
            }
            testCluster.launch();
            testCluster.awaitStarted();

            final TestNode leader = testCluster.leader();
            final Counter leaderControlToggle = leader.consensusModuleContext().controlToggleCounter();
            final Counter leaderStateCounter = leader.consensusModuleContext().moduleStateCounter();
            final Counter node0StateCounter = testCluster.node(0).consensusModuleContext().moduleStateCounter();
            final Counter node1StateCounter = testCluster.node(1).consensusModuleContext().moduleStateCounter();
            final Counter node2StateCounter = testCluster.node(2).consensusModuleContext().moduleStateCounter();

            leaderControlToggle.setRelease(ClusterControl.ToggleState.SUSPEND.code());
            Tests.sleep(SLOW_TICK_INTERVAL_MS);
            leader.poll();
            assertEquals(ConsensusModule.State.SUSPENDED, ConsensusModule.State.get(leaderStateCounter));

            Tests.await(() ->
            {
                testCluster.poll();
                return ConsensusModule.State.SUSPENDED == ConsensusModule.State.get(node0StateCounter) &&
                    ConsensusModule.State.SUSPENDED == ConsensusModule.State.get(node1StateCounter) &&
                    ConsensusModule.State.SUSPENDED == ConsensusModule.State.get(node2StateCounter);
            });

            leaderControlToggle.setRelease(ClusterControl.ToggleState.RESUME.code());
            Tests.sleep(SLOW_TICK_INTERVAL_MS);
            leader.poll();
            assertEquals(ConsensusModule.State.ACTIVE, ConsensusModule.State.get(leaderStateCounter));

            testCluster.generateNewElectionForLeader(leader);
            assertEquals(ConsensusModule.State.SUSPENDED, ConsensusModule.State.get(leaderStateCounter));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @SlowTest
    @InterruptAfter(20)
    public void shouldRollbackUncommittedSnapshotToggle(final boolean hasServices)
    {
        try (TestCluster testCluster = new TestCluster(tempDir))
        {
            if (hasServices)
            {
                testCluster.withServiceSupplier(StubClusteredService::new);
            }
            else
            {
                testCluster.withExtensionSupplier(SnapshottingCounterExtension::new);
            }
            testCluster.launch();
            testCluster.awaitStarted();

            final TestNode leader = testCluster.leader();
            final Counter leaderControlToggle = leader.consensusModuleContext().controlToggleCounter();
            final Counter leaderStateCounter = leader.consensusModuleContext().moduleStateCounter();
            final Counter leaderSnapshotCounter = leader.consensusModuleContext().snapshotCounter();

            testCluster.generateUncommittedDataAgainstLeader(leader);

            leaderControlToggle.setRelease(ClusterControl.ToggleState.SNAPSHOT.code());
            Tests.sleep(SLOW_TICK_INTERVAL_MS);
            leader.poll();
            assertEquals(ConsensusModule.State.SNAPSHOT, ConsensusModule.State.get(leaderStateCounter));

            testCluster.generateNewElectionForLeader(leader);
            assertEquals(ConsensusModule.State.ACTIVE, ConsensusModule.State.get(leaderStateCounter));
            assertEquals(0, leaderSnapshotCounter.get());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @SlowTest
    @InterruptAfter(20)
    public void shouldRollbackMultipleUncommittedControlToggles(final boolean hasServices)
    {
        try (TestCluster testCluster = new TestCluster(tempDir))
        {
            if (hasServices)
            {
                testCluster.withServiceSupplier(StubClusteredService::new);
            }
            else
            {
                testCluster.withExtensionSupplier(TestConsensusModuleExtension::new);
            }
            testCluster.launch();
            testCluster.awaitStarted();

            final TestNode leader = testCluster.leader();
            final Counter leaderControlToggle = leader.consensusModuleContext().controlToggleCounter();
            final Counter leaderStateCounter = leader.consensusModuleContext().moduleStateCounter();
            final Counter leaderSnapshotCounter = leader.consensusModuleContext().snapshotCounter();
            final Counter node0StateCounter = testCluster.node(0).consensusModuleContext().moduleStateCounter();
            final Counter node1StateCounter = testCluster.node(1).consensusModuleContext().moduleStateCounter();
            final Counter node2StateCounter = testCluster.node(2).consensusModuleContext().moduleStateCounter();

            leaderControlToggle.setRelease(ClusterControl.ToggleState.SUSPEND.code());
            Tests.sleep(SLOW_TICK_INTERVAL_MS);
            leader.poll();
            assertEquals(ConsensusModule.State.SUSPENDED, ConsensusModule.State.get(leaderStateCounter));

            Tests.await(() ->
            {
                testCluster.poll();
                return ConsensusModule.State.SUSPENDED == ConsensusModule.State.get(node0StateCounter) &&
                    ConsensusModule.State.SUSPENDED == ConsensusModule.State.get(node1StateCounter) &&
                    ConsensusModule.State.SUSPENDED == ConsensusModule.State.get(node2StateCounter);
            });

            leaderControlToggle.setRelease(ClusterControl.ToggleState.RESUME.code());
            Tests.sleep(SLOW_TICK_INTERVAL_MS);
            leader.poll();
            assertEquals(ConsensusModule.State.ACTIVE, ConsensusModule.State.get(leaderStateCounter));

            leaderControlToggle.setRelease(ClusterControl.ToggleState.SUSPEND.code());
            Tests.sleep(SLOW_TICK_INTERVAL_MS);
            leader.poll();
            assertEquals(ConsensusModule.State.SUSPENDED, ConsensusModule.State.get(leaderStateCounter));

            leaderControlToggle.setRelease(ClusterControl.ToggleState.RESUME.code());
            Tests.sleep(SLOW_TICK_INTERVAL_MS);
            leader.poll();
            assertEquals(ConsensusModule.State.ACTIVE, ConsensusModule.State.get(leaderStateCounter));

            leaderControlToggle.setRelease(ClusterControl.ToggleState.SNAPSHOT.code());
            Tests.sleep(SLOW_TICK_INTERVAL_MS);
            leader.poll();
            assertEquals(ConsensusModule.State.SNAPSHOT, ConsensusModule.State.get(leaderStateCounter));

            testCluster.generateNewElectionForLeader(leader);
            assertEquals(ConsensusModule.State.SUSPENDED, ConsensusModule.State.get(leaderStateCounter));
            assertEquals(0, leaderSnapshotCounter.get());
        }
    }

    @Test
    @SlowTest
    @InterruptAfter(20)
    public void shouldSnapshotWithNoServices()
    {
        try (TestCluster testCluster = new TestCluster(tempDir))
        {
            for (int i = 0; i < NODE_COUNT; ++i)
            {
                testCluster.node(i).consensusModuleContext().logFragmentLimit(1);
            }
            testCluster.withExtensionSupplier(SnapshottingCounterExtension::new);
            testCluster.launch();
            testCluster.awaitStarted();

            final TestNode leader = testCluster.leader();
            final Counter leaderControlToggle = leader.consensusModuleContext().controlToggleCounter();
            final Counter leaderStateCounter = leader.consensusModuleContext().moduleStateCounter();
            final Counter node0SnapshotCounter = testCluster.node(0).consensusModuleContext().snapshotCounter();
            final Counter node1SnapshotCounter = testCluster.node(1).consensusModuleContext().snapshotCounter();
            final Counter node2SnapshotCounter = testCluster.node(2).consensusModuleContext().snapshotCounter();

            testCluster.generateUncommittedDataAgainstLeader(leader);

            leaderControlToggle.setRelease(ClusterControl.ToggleState.SNAPSHOT.code());
            Tests.sleep(SLOW_TICK_INTERVAL_MS);
            leader.poll();
            assertEquals(ConsensusModule.State.SNAPSHOT, ConsensusModule.State.get(leaderStateCounter));

            Tests.await(() ->
            {
                testCluster.poll();
                return 1L == node0SnapshotCounter.get() &&
                    1L == node1SnapshotCounter.get() &&
                    1L == node2SnapshotCounter.get();
            });

            final SnapshottingCounterExtension node0Extension =
                (SnapshottingCounterExtension)testCluster.node(0).consensusModuleContext().consensusModuleExtension();
            final SnapshottingCounterExtension node1Extension =
                (SnapshottingCounterExtension)testCluster.node(1).consensusModuleContext().consensusModuleExtension();
            final SnapshottingCounterExtension node2Extension =
                (SnapshottingCounterExtension)testCluster.node(2).consensusModuleContext().consensusModuleExtension();
            final List<Long> node0Snapshots = node0Extension.counterSnapshots();
            final List<Long> node1Snapshots = node1Extension.counterSnapshots();
            final List<Long> node2Snapshots = node2Extension.counterSnapshots();

            assertEquals(1, node0Snapshots.size());
            assertEquals(1, node1Snapshots.size());
            assertEquals(1, node2Snapshots.size());
            assertEquals(CLIENT_MESSAGE_COUNT, node0Snapshots.get(0));
            assertEquals(CLIENT_MESSAGE_COUNT, node1Snapshots.get(0));
            assertEquals(CLIENT_MESSAGE_COUNT, node2Snapshots.get(0));
        }
    }

    private final class TestCluster implements AutoCloseable
    {
        TestNode[] nodes = new TestNode[NODE_COUNT];
        Supplier<ClusteredService> clusteredServiceSupplier = () -> null;
        Supplier<ConsensusModuleExtension> consensusModuleExtensionSupplier = () -> null;

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

        private void withExtensionSupplier(final Supplier<ConsensusModuleExtension> consensusModuleExtensionSupplier)
        {
            this.consensusModuleExtensionSupplier = consensusModuleExtensionSupplier;
        }

        private void launch()
        {
            for (final TestNode node : nodes)
            {
                final ClusteredService clusteredService = clusteredServiceSupplier.get();
                if (null != clusteredService)
                {
                    node.withService(clusteredService);
                }

                final ConsensusModuleExtension consensusModuleExtension = consensusModuleExtensionSupplier.get();
                if (null != consensusModuleExtension)
                {
                    node.withExtension(consensusModuleExtension);
                }

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
        private void generateUncommittedDataAgainstLeader(final TestNode leader)
        {
            assertTrue(leader.isLeader());

            final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(tempDir.resolve("client").toAbsolutePath().toString())
                .dirDeleteOnStart(true)
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED);

            final ControlledEgressListener stubEgressListener =
                (clusterSessionId, timestamp, buffer, offset, length, header) ->
                    ControlledFragmentHandler.Action.CONTINUE;

            final AeronCluster.Context aeronClusterContext = new AeronCluster.Context()
                .aeronDirectoryName(context.aeronDirectoryName())
                .ingressEndpoints(MEMBER_INGRESS_ENDPOINTS)
                .ingressChannel("aeron:udp")
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .controlledEgressListener(stubEgressListener);

            try (TestMediaDriver clientMediaDriver = TestMediaDriver.launch(context, null);
                AeronCluster.AsyncConnect asyncConnect = AeronCluster.asyncConnect(aeronClusterContext))
            {
                final MutableReference<AeronCluster> aeronClusterRef = new MutableReference<>();
                Tests.await(() ->
                {
                    poll(Aeron.NULL_VALUE);
                    aeronClusterRef.set(asyncConnect.poll());
                    return null != aeronClusterRef.get();
                });

                try (AeronCluster aeronCluster = aeronClusterRef.get())
                {
                    final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(new byte[CLIENT_MESSAGE_SIZE]);
                    final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
                    final SessionMessageHeaderEncoder sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();
                    final int data = MessageHeaderEncoder.ENCODED_LENGTH + SessionMessageHeaderEncoder.BLOCK_LENGTH;

                    sessionMessageHeaderEncoder.wrapAndApplyHeader(unsafeBuffer, 0, messageHeaderEncoder)
                        .leadershipTermId(aeronCluster.leadershipTermId())
                        .clusterSessionId(aeronCluster.clusterSessionId());

                    if (0 == leader.consensusModuleContext().serviceCount())
                    {
                        messageHeaderEncoder.schemaId(EXTENSION_SCHEMA);
                    }

                    for (int i = 1; i <= CLIENT_MESSAGE_COUNT; ++i)
                    {
                        unsafeBuffer.putLong(data, i);

                        while (aeronCluster.ingressPublication().offer(unsafeBuffer, 0, unsafeBuffer.capacity()) < 0)
                        {
                            leader.poll();
                            Tests.yield();
                        }
                    }
                }

                Tests.await(() ->
                {
                    leader.poll();
                    return CLIENT_MESSAGE_COUNT * (CLIENT_MESSAGE_SIZE + DataHeaderFlyweight.HEADER_LENGTH) <
                        leader.consensusModuleContext().logPublisher().position();
                });

                final long logPublisherPos = leader.consensusModuleContext().logPublisher().position();
                final long commitPos = leader.consensusModuleContext().commitPositionCounter().get();
                assertTrue(commitPos < logPublisherPos,
                    "Commit position " + commitPos + " not less than log publisher position " + logPublisherPos);
            }
        }

        private void generateNewElectionForLeader(final TestNode leader)
        {
            assertTrue(leader.isLeader());

            Tests.sleep(TimeUnit.NANOSECONDS.toMillis(leader.consensusModuleContext().leaderHeartbeatTimeoutNs()));
            Tests.await(() ->
            {
                poll(leader.consensusModuleContext().clusterMemberId());
                return null != leader(leader.consensusModuleContext().clusterMemberId());
            });
            awaitStarted(leader.consensusModuleContext().clusterMemberId());

            final Counter leaderElectionCounter = leader.consensusModuleContext().electionCounter();
            final long electionCount = leaderElectionCounter.get();
            Tests.await(() ->
            {
                poll();
                return electionCount < leaderElectionCounter.get();
            });
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
        private ConsensusModuleExtension consensusModuleExtension;

        private TestMediaDriver mediaDriver;
        private Archive archive;
        private ConsensusModule consensusModule;
        private ClusteredServiceContainer serviceContainer;

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

        private void withExtension(final ConsensusModuleExtension consensusModuleExtension)
        {
            this.consensusModuleExtension = consensusModuleExtension;
        }

        private void launch(final SystemTestWatcher testWatcher)
        {
            mediaDriver = TestMediaDriver.launch(mediaDriverContext, testWatcher);
            archive = Archive.launch(archiveContext);
            if (null != clusteredService)
            {
                clusteredServiceContext.clusteredService(clusteredService);
                serviceContainer = ClusteredServiceContainer.launch(clusteredServiceContext);
            }
            if (null != consensusModuleExtension)
            {
                consensusModuleContext.consensusModuleExtension(consensusModuleExtension);
                consensusModuleContext.serviceCount(0);
            }
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

        @Override
        public void close()
        {
            CloseHelper.closeAll(serviceContainer, consensusModule, archive, mediaDriver);
        }
    }

    private static final class SnapshottingCounterExtension extends TestConsensusModuleExtension
    {
        private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private final SessionMessageHeaderDecoder sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();

        private Counter commitPosition = null;
        private ExclusivePublication logPublication = null;

        private final LongArrayQueue uncommittedCounters = new LongArrayQueue(Long.MAX_VALUE);
        private long committedCounter = 0;
        private final List<Long> counterSnapshots = new ArrayList<>();


        public void onStart(final ConsensusModuleControl consensusModuleControl, final Image snapshotImage)
        {
            assertNull(snapshotImage);
            commitPosition = consensusModuleControl.context().commitPositionCounter();
        }

        public void onPrepareForNewLeadership()
        {
            logPublication = null;
            drainUncommitted();
            uncommittedCounters.clear();
        }

        public void onElectionComplete(final ConsensusControlState consensusControlState)
        {
            logPublication = consensusControlState.logPublication();
        }

        public ControlledFragmentHandler.Action onIngressExtensionMessage(
            final int actingBlockLength,
            final int templateId,
            final int schemaId,
            final int actingVersion,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            sessionMessageHeaderDecoder.wrapAndApplyHeader(buffer, offset, messageHeaderDecoder);
            assertEquals(EXTENSION_SCHEMA, messageHeaderDecoder.schemaId());

            if (logPublication.offer(buffer, offset, length) < 0)
            {
                return ControlledFragmentHandler.Action.ABORT;
            }

            assertTrue(uncommittedCounters.offerLong(logPublication.position()));
            assertTrue(uncommittedCounters.offerLong(buffer.getLong(sessionMessageHeaderDecoder.limit())));

            return ControlledFragmentHandler.Action.CONTINUE;
        }

        public ControlledFragmentHandler.Action onLogExtensionMessage(
            final int actingBlockLength,
            final int templateId,
            final int schemaId,
            final int actingVersion,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            assertEquals(EXTENSION_SCHEMA, schemaId);

            sessionMessageHeaderDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
            committedCounter = buffer.getLong(sessionMessageHeaderDecoder.limit());

            return ControlledFragmentHandler.Action.CONTINUE;
        }

        public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
        {
            drainUncommitted();
            counterSnapshots.add(committedCounter);
        }

        private List<Long> counterSnapshots()
        {
            return counterSnapshots;
        }

        private void drainUncommitted()
        {
            while (uncommittedCounters.peekLong() <= commitPosition.get())
            {
                uncommittedCounters.pollLong();
                committedCounter = uncommittedCounters.pollLong();
            }
        }
    }
}
