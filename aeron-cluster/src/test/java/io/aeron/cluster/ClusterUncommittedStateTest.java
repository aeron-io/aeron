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
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ControlledEgressListener;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.StubClusteredService;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    @TempDir
    public Path tempDir;

    @Test
    @SlowTest
    @InterruptAfter(20)
    public void shouldRollbackUncommittedSuspendControlToggle()
    {
        try (TestCluster testCluster = new TestCluster(tempDir))
        {
            testCluster.withServiceSupplier(StubClusteredService::new);
            testCluster.launch();
            testCluster.awaitStarted();
            testCluster.generateUncommittedData();

            final TestNode leader = testCluster.leader();
            final Counter leaderToggle = leader.consensusModuleContext().controlToggleCounter();
            final Counter leaderStateCounter = leader.consensusModuleContext().moduleStateCounter();
            final Counter leaderElectionCounter = leader.consensusModuleContext().electionCounter();

            leaderToggle.setRelease(ClusterControl.ToggleState.SUSPEND.code());
            Tests.sleep(SLOW_TICK_INTERVAL_MS);
            leader.poll();
            assertEquals(ConsensusModule.State.SUSPENDED, ConsensusModule.State.get(leaderStateCounter));

            Tests.sleep(TimeUnit.NANOSECONDS.toMillis(leader.consensusModuleContext().leaderHeartbeatTimeoutNs()));
            Tests.await(() ->
            {
                testCluster.poll(leader.consensusModuleContext().clusterMemberId());
                return null != testCluster.leader(leader.consensusModuleContext().clusterMemberId());
            });
            testCluster.awaitStarted(leader.consensusModuleContext().clusterMemberId());

            final long electionCount = leaderElectionCounter.get();
            Tests.await(() ->
            {
                testCluster.poll();
                return electionCount < leaderElectionCounter.get();
            });
            assertEquals(ConsensusModule.State.ACTIVE, ConsensusModule.State.get(leaderStateCounter));
        }
    }

    @Test
    @SlowTest
    @InterruptAfter(20)
    public void shouldRollbackUncommittedResumeControlToggle()
    {
        try (TestCluster testCluster = new TestCluster(tempDir))
        {
            testCluster.withServiceSupplier(StubClusteredService::new);
            testCluster.launch();
            testCluster.awaitStarted();

            final TestNode leader = testCluster.leader();
            final Counter leaderControlToggle = leader.consensusModuleContext().controlToggleCounter();
            final Counter leaderElectionCounter = leader.consensusModuleContext().electionCounter();
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

            Tests.sleep(TimeUnit.NANOSECONDS.toMillis(leader.consensusModuleContext().leaderHeartbeatTimeoutNs()));
            Tests.await(() ->
            {
                testCluster.poll(leader.consensusModuleContext().clusterMemberId());
                return null != testCluster.leader(leader.consensusModuleContext().clusterMemberId());
            });
            testCluster.awaitStarted(leader.consensusModuleContext().clusterMemberId());

            final long electionCount = leaderElectionCounter.get();
            Tests.await(() ->
            {
                testCluster.poll();
                return electionCount < leaderElectionCounter.get();
            });
            assertEquals(ConsensusModule.State.SUSPENDED, ConsensusModule.State.get(leaderStateCounter));
        }
    }

    @Test
    @SlowTest
    @InterruptAfter(20)
    public void shouldRollbackUncommittedSnapshotToggle()
    {
        try (TestCluster testCluster = new TestCluster(tempDir))
        {
            testCluster.withServiceSupplier(StubClusteredService::new);
            testCluster.launch();
            testCluster.awaitStarted();
            testCluster.generateUncommittedData();

            final TestNode leader = testCluster.leader();
            final Counter leaderControlToggle = leader.consensusModuleContext().controlToggleCounter();
            final Counter leaderElectionCounter = leader.consensusModuleContext().electionCounter();
            final Counter leaderStateCounter = leader.consensusModuleContext().moduleStateCounter();

            leaderControlToggle.setRelease(ClusterControl.ToggleState.SNAPSHOT.code());
            Tests.sleep(SLOW_TICK_INTERVAL_MS);
            leader.poll();
            assertEquals(ConsensusModule.State.SNAPSHOT, ConsensusModule.State.get(leaderStateCounter));

            Tests.sleep(TimeUnit.NANOSECONDS.toMillis(leader.consensusModuleContext().leaderHeartbeatTimeoutNs()));
            Tests.await(() ->
            {
                testCluster.poll(leader.consensusModuleContext().clusterMemberId());
                return null != testCluster.leader(leader.consensusModuleContext().clusterMemberId());
            });
            testCluster.awaitStarted(leader.consensusModuleContext().clusterMemberId());

            final long electionCount = leaderElectionCounter.get();
            Tests.await(() ->
            {
                testCluster.poll();
                return electionCount < leaderElectionCounter.get();
            });
            assertEquals(ConsensusModule.State.ACTIVE, ConsensusModule.State.get(leaderStateCounter));
        }
    }

    @Test
    @SlowTest
    @InterruptAfter(20)
    public void shouldRollbackMultipleUncommittedControlToggles()
    {
        try (TestCluster testCluster = new TestCluster(tempDir))
        {
            testCluster.withServiceSupplier(StubClusteredService::new);
            testCluster.launch();
            testCluster.awaitStarted();

            final TestNode leader = testCluster.leader();
            final Counter leaderControlToggle = leader.consensusModuleContext().controlToggleCounter();
            final Counter leaderElectionCounter = leader.consensusModuleContext().electionCounter();
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

            Tests.sleep(TimeUnit.NANOSECONDS.toMillis(leader.consensusModuleContext().leaderHeartbeatTimeoutNs()));
            Tests.await(() ->
            {
                testCluster.poll(leader.consensusModuleContext().clusterMemberId());
                return null != testCluster.leader(leader.consensusModuleContext().clusterMemberId());
            });
            testCluster.awaitStarted(leader.consensusModuleContext().clusterMemberId());

            final long electionCount = leaderElectionCounter.get();
            Tests.await(() ->
            {
                testCluster.poll();
                return electionCount < leaderElectionCounter.get();
            });
            assertEquals(ConsensusModule.State.SUSPENDED, ConsensusModule.State.get(leaderStateCounter));
        }
    }

    class TestCluster implements AutoCloseable
    {
        TestNode[] nodes = new TestNode[NODE_COUNT];
        Supplier<ClusteredService> clusteredServiceSupplier = () -> null;

        TestCluster(final Path tempDir)
        {
            for (int i = 0; i < NODE_COUNT; ++i)
            {
                nodes[i] = new TestNode(i, tempDir.resolve("node-" + i));
            }
        }

        void withServiceSupplier(final Supplier<ClusteredService> clusteredServiceSupplier)
        {
            this.clusteredServiceSupplier = clusteredServiceSupplier;
        }

        void launch()
        {
            for (final TestNode node : nodes)
            {
                node.withService(clusteredServiceSupplier.get());
                node.launch(testWatcher);
            }
        }

        TestNode node(final int i)
        {
            return nodes[i];
        }

        void poll()
        {
            poll(Aeron.NULL_VALUE);
        }

        void poll(final int except)
        {
            for (int i = 0; i < nodes.length; ++i)
            {
                if (i != except)
                {
                    nodes[i].poll();
                }
            }
        }

        void awaitStarted()
        {
            awaitStarted(Aeron.NULL_VALUE);
        }

        void awaitStarted(final int except)
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

        TestNode leader()
        {
            return leader(Aeron.NULL_VALUE);
        }

        TestNode leader(final int except)
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
        void generateUncommittedData()
        {
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

                final TestNode leaderNode = leader();
                try (AeronCluster aeronCluster = aeronClusterRef.get())
                {
                    final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(new byte[CLIENT_MESSAGE_SIZE]);
                    for (int i = 0; i < CLIENT_MESSAGE_COUNT; ++i)
                    {
                        while (aeronCluster.offer(unsafeBuffer, 0, unsafeBuffer.capacity()) < 0)
                        {
                            leaderNode.poll();
                            Tests.yield();
                        }
                    }
                }

                Tests.await(() ->
                {
                    leaderNode.poll();
                    return CLIENT_MESSAGE_COUNT * CLIENT_MESSAGE_SIZE <
                        leaderNode.consensusModuleContext().logPublisher().position();
                });

                final long logPublisherPos = leaderNode.consensusModuleContext().logPublisher().position();
                final long commitPos = leaderNode.consensusModuleContext().commitPositionCounter().get();
                assertTrue(commitPos < logPublisherPos,
                    "Commit position " + commitPos + " not less than log publisher position " + logPublisherPos);
            }
        }

        @Override
        public void close()
        {
            CloseHelper.quietCloseAll(nodes);
        }
    }

    static class TestNode implements AutoCloseable
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

        TestNode(final int clusterMemberId, final Path directory)
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

        public void withService(final ClusteredService clusteredService)
        {
            this.clusteredService = clusteredService;
        }

        public void launch(final SystemTestWatcher testWatcher)
        {
            mediaDriver = TestMediaDriver.launch(mediaDriverContext, testWatcher);
            archive = Archive.launch(archiveContext);
            clusteredServiceContext.clusteredService(clusteredService);
            serviceContainer = ClusteredServiceContainer.launch(clusteredServiceContext);
            consensusModule = ConsensusModule.launch(consensusModuleContext);
        }

        public MediaDriver.Context mediaDriverContext()
        {
            return mediaDriverContext;
        }

        public Archive.Context archiveContext()
        {
            return archiveContext;
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
            return context.electionStateCounter().get() == ElectionState.CLOSED.code() &&
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

        @Override
        public void close()
        {
            CloseHelper.closeAll(serviceContainer, consensusModule, archive, mediaDriver);
        }
    }
}
