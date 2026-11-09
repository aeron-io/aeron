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

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.TestContexts;
import io.aeron.test.cluster.ClusterTests;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@ExtendWith(InterruptingTestCallback.class)
class ClusterWithNoServicesTest
{
    enum LatchTrigger
    {
        SESSION_OPENED, SNAPSHOT_TAKEN, CLOSED
    }

    private ClusteredMediaDriver clusteredMediaDriver;
    private AeronCluster aeronCluster;

    @AfterEach
    void after()
    {
        final ConsensusModule consensusModule = null == clusteredMediaDriver ?
            null : clusteredMediaDriver.consensusModule();

        CloseHelper.closeAll(aeronCluster, consensusModule, clusteredMediaDriver);

        if (null != clusteredMediaDriver)
        {
            clusteredMediaDriver.consensusModule().context().deleteDirectory();
            clusteredMediaDriver.archive().context().deleteDirectory();
            clusteredMediaDriver.mediaDriver().context().deleteDirectory();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldConnectAndSendKeepAliveWithExtensionLoaded() throws InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch(1);
        final ConsensusModuleExtension consensusModuleExtension = spy(
            new TestConsensusModuleExtension(latch, LatchTrigger.SESSION_OPENED));

        clusteredMediaDriver = launchCluster(consensusModuleExtension);
        aeronCluster = connectClient();

        assertTrue(aeronCluster.sendKeepAlive());
        latch.await();

        final InOrder inOrder = inOrder(consensusModuleExtension);
        inOrder.verify(consensusModuleExtension).onStart(any(ConsensusModuleControl.class), isNull());
        inOrder.verify(consensusModuleExtension).onElectionComplete(any(ConsensusControlState.class));
        inOrder.verify(consensusModuleExtension, atLeastOnce()).doWork(anyLong());

        verify(consensusModuleExtension).onSessionOpened(anyLong());

        ClusterTests.failOnClusterError();
    }

    @Test
    @InterruptAfter(10)
    void shouldSnapshotExtensionState() throws InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch(1);

        final TestConsensusModuleExtension moduleExtension =
            new TestConsensusModuleExtension(latch, LatchTrigger.SNAPSHOT_TAKEN);
        clusteredMediaDriver = launchCluster(moduleExtension);
        aeronCluster = connectClient();

        final AtomicCounter controlToggle = getClusterControlToggle();
        assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));

        latch.await();
        assertEquals(1, moduleExtension.snapshotCount.get());

        ClusterTests.failOnClusterError();
    }

    @Test
    @InterruptAfter(10)
    void shouldShutdownWithExtension() throws InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch(1);

        final TestConsensusModuleExtension moduleExtension =
            new TestConsensusModuleExtension(latch, LatchTrigger.CLOSED);
        clusteredMediaDriver = launchCluster(moduleExtension);
        aeronCluster = connectClient();

        final AtomicCounter controlToggle = getClusterControlToggle();
        assertTrue(ClusterControl.ToggleState.SHUTDOWN.toggle(controlToggle));

        latch.await();
        assertEquals(1, moduleExtension.snapshotCount.get());

        ClusterTests.failOnClusterError();
    }

    @Test
    @InterruptAfter(10)
    void shouldAbortWithExtension() throws InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch(1);

        final TestConsensusModuleExtension moduleExtension =
            new TestConsensusModuleExtension(latch, LatchTrigger.CLOSED);
        clusteredMediaDriver = launchCluster(moduleExtension);
        aeronCluster = connectClient();

        final AtomicCounter controlToggle = getClusterControlToggle();
        assertTrue(ClusterControl.ToggleState.ABORT.toggle(controlToggle));

        latch.await();
        assertEquals(0, moduleExtension.snapshotCount.get());

        ClusterTests.failOnClusterError();
    }

    private ClusteredMediaDriver launchCluster(final ConsensusModuleExtension consensusModuleExtension)
    {
        return ClusteredMediaDriver.launch(
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
                .clusterMembers(ClusterTestConstants.CLUSTER_MEMBERS)
                .ingressChannel("aeron:udp")
                .serviceCount(0)
                .consensusModuleExtension(consensusModuleExtension)
                .logChannel("aeron:ipc")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .deleteDirOnStart(true));
    }

    private static AeronCluster connectClient()
    {
        return AeronCluster.connect(
            new AeronCluster.Context()
                .ingressChannel("aeron:udp")
                .ingressEndpoints(ClusterTestConstants.INGRESS_ENDPOINTS)
                .egressChannel("aeron:udp?endpoint=localhost:0"));
    }

    public AtomicCounter getClusterControlToggle()
    {
        final CountersReader counters = clusteredMediaDriver.mediaDriver().context().countersManager();
        final int clusterId = clusteredMediaDriver.consensusModule().context().clusterId();
        final AtomicCounter controlToggle = ClusterControl.findControlToggle(counters, clusterId);
        assertNotNull(controlToggle);

        return controlToggle;
    }

    static final class TestConsensusModuleExtension implements ConsensusModuleExtension
    {
        private final CountDownLatch latch;
        private final LatchTrigger latchTrigger;
        final AtomicInteger snapshotCount = new AtomicInteger();

        TestConsensusModuleExtension(final CountDownLatch latch, final LatchTrigger latchTrigger)
        {
            this.latch = latch;
            this.latchTrigger = latchTrigger;
        }

        public int supportedSchemaId()
        {
            return 0;
        }

        @Override
        public void onStart(final ConsensusModuleControl consensusModuleControl, final Image snapshotImage)
        {
        }

        @Override
        public int doWork(final long nowNs)
        {
            return 0;
        }

        @Override
        public int slowTickWork(final long nowNs)
        {
            return 0;
        }

        @Override
        public int consensusWork(final long nowNs)
        {
            return 0;
        }

        @Override
        public void onElectionComplete(final ConsensusControlState consensusControlState)
        {
        }

        @Override
        public void onNewLeadershipTerm(final ConsensusControlState consensusControlState)
        {
        }

        @Override
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
            return ControlledFragmentHandler.Action.CONTINUE;
        }

        @Override
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
            return ControlledFragmentHandler.Action.CONTINUE;
        }

        @Override
        public void close()
        {
            if (LatchTrigger.CLOSED == latchTrigger)
            {
                latch.countDown();
            }
        }

        @Override
        public void onSessionOpened(final long clusterSessionId)
        {
            if (LatchTrigger.SESSION_OPENED == latchTrigger)
            {
                latch.countDown();
            }
        }

        @Override
        public void onSessionClosed(final long clusterSessionId, final CloseReason closeReason)
        {
        }

        @Override
        public void onPrepareForNewLeadership()
        {
        }

        @Override
        public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
        {
            snapshotCount.getAndIncrement();
            if (LatchTrigger.SNAPSHOT_TAKEN == latchTrigger)
            {
                latch.countDown();
            }
        }
    }
}
