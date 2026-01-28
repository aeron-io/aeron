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

import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.driver.DataPacketDispatcher;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ReceiveChannelEndpointSupplier;
import io.aeron.driver.SendChannelEndpointSupplier;
import io.aeron.driver.ext.DebugReceiveChannelEndpoint;
import io.aeron.driver.ext.DebugSendChannelEndpoint;
import io.aeron.driver.ext.LossGenerator;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.collections.LongArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static io.aeron.test.driver.TestMediaDriver.shouldRunJavaMediaDriver;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class ClusterUncommittedStateTest
{
    private static final int NODE_COUNT = 3;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();
    private TestCluster cluster;
    private final ToggledLossControl[] toggledLossControls = new ToggledLossControl[NODE_COUNT];

    @BeforeEach
    void setUp()
    {
        for (int i = 0; i < NODE_COUNT; ++i)
        {
            toggledLossControls[i] = new ToggledLossControl();
        }
    }

    @ParameterizedTest(name = "ShouldRollbackUncommittedSuspendControlToggle hasService={0}")
    @ValueSource(booleans = {true, false})
    @SlowTest
    @InterruptAfter(20)
    void shouldRollbackUncommittedSuspendControlToggle(final boolean hasService)
    {
        assumeTrue(shouldRunJavaMediaDriver());

        final TestCluster.Builder clusterBuilder = aCluster()
            .withStaticNodes(NODE_COUNT)
            .withReceiveChannelEndpointSupplier((memberId) -> toggledLossControls[memberId])
            .withSendChannelEndpointSupplier((memberId) -> toggledLossControls[memberId]);
        if (!hasService)
        {
            clusterBuilder
                .withExtensionSuppler(TestNode.TestConsensusModuleExtension::new)
                .withServiceSupplier(value -> new TestNode.TestService[0]);
        }
        cluster = clusterBuilder.start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();
        final ToggledLossControl leaderLossControl = toggledLossControls[firstLeader.memberId()];

        leaderLossControl.toggleLoss(true);
        Tests.await(() -> 0 < leaderLossControl.droppedOutboundFrames.get() &&
            0 < leaderLossControl.droppedInboundFrames.get());

        cluster.suspendCluster(firstLeader);
        Tests.await(() -> ConsensusModule.State.SUSPENDED == firstLeader.moduleState());

        Tests.await(() -> null != cluster.findLeader(firstLeader.memberId()));

        leaderLossControl.toggleLoss(false);
        Tests.await(() -> ConsensusModule.State.ACTIVE == firstLeader.moduleState());
    }

    @ParameterizedTest(name = "ShouldRollbackUncommittedResumeControlToggle hasService={0}")
    @ValueSource(booleans = {true, false})
    @SlowTest
    @InterruptAfter(20)
    void shouldRollbackUncommittedResumeControlToggle(final boolean hasService)
    {
        assumeTrue(shouldRunJavaMediaDriver());

        final TestCluster.Builder clusterBuilder = aCluster()
            .withStaticNodes(NODE_COUNT)
            .withReceiveChannelEndpointSupplier((memberId) -> toggledLossControls[memberId])
            .withSendChannelEndpointSupplier((memberId) -> toggledLossControls[memberId]);
        if (!hasService)
        {
            clusterBuilder
                .withExtensionSuppler(TestNode.TestConsensusModuleExtension::new)
                .withServiceSupplier(value -> new TestNode.TestService[0]);
        }
        cluster = clusterBuilder.start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();
        final ToggledLossControl leaderLossControl = toggledLossControls[firstLeader.memberId()];

        cluster.suspendCluster(firstLeader);
        Tests.await(() ->
        {
            for (int i = 0; i < cluster.memberCount(); ++i)
            {
                if (ConsensusModule.State.SUSPENDED != cluster.node(i).moduleState())
                {
                    return false;
                }
            }
            return true;
        });

        leaderLossControl.toggleLoss(true);
        Tests.await(() -> 0 < leaderLossControl.droppedOutboundFrames.get() &&
            0 < leaderLossControl.droppedInboundFrames.get());

        cluster.resumeCluster(firstLeader);
        Tests.await(() -> ConsensusModule.State.ACTIVE == firstLeader.moduleState());

        Tests.await(() -> null != cluster.findLeader(firstLeader.memberId()));

        leaderLossControl.toggleLoss(false);
        Tests.await(() -> ConsensusModule.State.SUSPENDED == firstLeader.moduleState());
    }

    @ParameterizedTest(name = "ShouldRollbackUncommittedSnapshotToggle hasService={0}")
    @ValueSource(booleans = {true, false})
    @SlowTest
    @InterruptAfter(20)
    void shouldRollbackUncommittedSnapshotToggle(final boolean hasService)
    {
        assumeTrue(shouldRunJavaMediaDriver());

        final TestCluster.Builder clusterBuilder = aCluster()
            .withStaticNodes(NODE_COUNT)
            .withReceiveChannelEndpointSupplier((memberId) -> toggledLossControls[memberId])
            .withSendChannelEndpointSupplier((memberId) -> toggledLossControls[memberId]);
        if (!hasService)
        {
            clusterBuilder
                .withExtensionSuppler(TestNode.TestConsensusModuleExtension::new)
                .withServiceSupplier(value -> new TestNode.TestService[0]);
        }
        cluster = clusterBuilder.start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();
        final ToggledLossControl leaderLossControl = toggledLossControls[firstLeader.memberId()];

        cluster.suspendCluster(firstLeader);
        Tests.await(() ->
        {
            for (int i = 0; i < cluster.memberCount(); ++i)
            {
                if (ConsensusModule.State.SUSPENDED != cluster.node(i).moduleState())
                {
                    return false;
                }
            }
            return true;
        });

        leaderLossControl.toggleLoss(true);
        Tests.await(() -> 0 < leaderLossControl.droppedOutboundFrames.get() &&
            0 < leaderLossControl.droppedInboundFrames.get());

        cluster.resumeCluster(firstLeader);
        Tests.await(() -> ConsensusModule.State.ACTIVE == firstLeader.moduleState());
        Tests.await(() -> ClusterControl.ToggleState.NEUTRAL ==
            ClusterControl.ToggleState.get(firstLeader.consensusModule().context().controlToggleCounter()));

        cluster.suspendCluster(firstLeader);
        Tests.await(() -> ConsensusModule.State.SUSPENDED == firstLeader.moduleState());

        cluster.resumeCluster(firstLeader);
        Tests.await(() -> ConsensusModule.State.ACTIVE == firstLeader.moduleState());
        Tests.await(() -> ClusterControl.ToggleState.NEUTRAL ==
            ClusterControl.ToggleState.get(firstLeader.consensusModule().context().controlToggleCounter()));

        cluster.takeSnapshot(firstLeader);
        Tests.await(() -> ConsensusModule.State.SNAPSHOT == firstLeader.moduleState());

        Tests.await(() -> null != cluster.findLeader(firstLeader.memberId()));

        leaderLossControl.toggleLoss(false);
        Tests.await(() -> ConsensusModule.State.SUSPENDED == firstLeader.moduleState());
    }

    @Test
    @SlowTest
    @InterruptAfter(20)
    void shouldSnapshotWithNoServicesWithUncommittedData()
    {
        assumeTrue(shouldRunJavaMediaDriver());

        cluster = aCluster()
            .withStaticNodes(NODE_COUNT)
            .withReceiveChannelEndpointSupplier((index) -> toggledLossControls[index])
            .withSendChannelEndpointSupplier((index) -> toggledLossControls[index])
            .withExtensionSuppler(TestCounterExtension::new)
            .withServiceSupplier(value -> new TestNode.TestService[0])
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();
        final ToggledLossControl leaderLossControl = toggledLossControls[firstLeader.memberId()];

        leaderLossControl.toggleLoss(true);
        Tests.await(() -> 0 < leaderLossControl.droppedOutboundFrames.get() &&
            0 < leaderLossControl.droppedInboundFrames.get());

        cluster.connectIpcClient(new AeronCluster.Context(), firstLeader.mediaDriver().aeronDirectoryName());
        cluster.sendExtensionMessages(32);
        final long messageLength = BitUtil.align(
            DataHeaderFlyweight.HEADER_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH + BitUtil.SIZE_OF_INT,
            FrameDescriptor.FRAME_ALIGNMENT);
        Tests.await(() -> firstLeader.appendPosition() > 32L * messageLength);

        cluster.takeSnapshot(firstLeader);
        Tests.await(() -> ConsensusModule.State.SNAPSHOT == firstLeader.moduleState());

        leaderLossControl.toggleLoss(false);
        cluster.awaitSnapshotCount(1);

        final TestCounterExtension node0Extension =
            (TestCounterExtension)cluster.node(0).consensusModule().context().consensusModuleExtension();
        final TestCounterExtension node1Extension =
            (TestCounterExtension)cluster.node(1).consensusModule().context().consensusModuleExtension();
        final TestCounterExtension node2Extension =
            (TestCounterExtension)cluster.node(2).consensusModule().context().consensusModuleExtension();
        final List<Integer> node0Snapshots = node0Extension.counterSnapshots();
        final List<Integer> node1Snapshots = node1Extension.counterSnapshots();
        final List<Integer> node2Snapshots = node2Extension.counterSnapshots();

        assertEquals(1, node0Snapshots.size());
        assertEquals(1, node1Snapshots.size());
        assertEquals(1, node2Snapshots.size());
        assertEquals(31, node0Snapshots.get(0));
        assertEquals(31, node1Snapshots.get(0));
        assertEquals(31, node2Snapshots.get(0));
    }

    private static final class TestCounterExtension extends TestNode.TestConsensusModuleExtension
    {
        private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();

        private Counter commitPosition = null;
        private ExclusivePublication logPublication = null;
        private final LongArrayQueue uncommittedCounters = new LongArrayQueue(Long.MAX_VALUE);
        private int committedCounter = 0;
        private final List<Integer> counterSnapshots = new ArrayList<>();

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
            messageHeaderDecoder.wrap(buffer, offset);
            assertEquals(TestCluster.EXTENSION_SCHEMA_ID, messageHeaderDecoder.schemaId());

            if (logPublication.offer(buffer, offset, length) < 0)
            {
                return ControlledFragmentHandler.Action.ABORT;
            }

            assertTrue(uncommittedCounters.offerLong(logPublication.position()));
            assertTrue(uncommittedCounters.offerLong(buffer.getLong(offset + MessageHeaderDecoder.ENCODED_LENGTH)));

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
            assertEquals(TestCluster.EXTENSION_SCHEMA_ID, schemaId);
            committedCounter = buffer.getInt(offset);

            return ControlledFragmentHandler.Action.CONTINUE;
        }

        public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
        {
            drainUncommitted();
            counterSnapshots.add(committedCounter);
        }

        private List<Integer> counterSnapshots()
        {
            return counterSnapshots;
        }

        private void drainUncommitted()
        {
            while (uncommittedCounters.peekLong() <= commitPosition.get())
            {
                uncommittedCounters.pollLong();
                committedCounter = (int)uncommittedCounters.pollLong();
            }
        }
    }

    private static final class ToggledLossControl implements ReceiveChannelEndpointSupplier, SendChannelEndpointSupplier
    {
        final AtomicBoolean shouldDropOutboundFrames = new AtomicBoolean(false);
        final AtomicInteger droppedOutboundFrames = new AtomicInteger(0);
        final ToggledLossGenerator outboundLossGenerator = new ToggledLossGenerator(
            shouldDropOutboundFrames, droppedOutboundFrames);

        final AtomicBoolean shouldDropInboundFrames = new AtomicBoolean(false);
        final AtomicInteger droppedInboundFrames = new AtomicInteger(0);
        final ToggledLossGenerator inboundLossGenerator = new ToggledLossGenerator(
            shouldDropInboundFrames, droppedInboundFrames);

        public ReceiveChannelEndpoint newInstance(
            final UdpChannel udpChannel,
            final DataPacketDispatcher dispatcher,
            final AtomicCounter statusIndicator,
            final MediaDriver.Context context)
        {
            return new DebugReceiveChannelEndpoint(
                udpChannel, dispatcher, statusIndicator, context, inboundLossGenerator, inboundLossGenerator);
        }

        public SendChannelEndpoint newInstance(
            final UdpChannel udpChannel,
            final AtomicCounter statusIndicator,
            final MediaDriver.Context context)
        {
            return new DebugSendChannelEndpoint(
                udpChannel, statusIndicator, context, outboundLossGenerator, outboundLossGenerator);
        }

        void toggleLoss(final boolean loss)
        {
            shouldDropOutboundFrames.set(loss);
            shouldDropInboundFrames.set(loss);
        }

        private static final class ToggledLossGenerator implements LossGenerator
        {
            private final AtomicBoolean shouldDropFrame;
            private final AtomicInteger droppedFrames;

            private ToggledLossGenerator(final AtomicBoolean shouldDropFrame, final AtomicInteger droppedFrames)
            {
                this.shouldDropFrame = shouldDropFrame;
                this.droppedFrames = droppedFrames;
            }

            public boolean shouldDropFrame(final InetSocketAddress address, final UnsafeBuffer buffer, final int length)
            {
                droppedFrames.incrementAndGet();
                return shouldDropFrame.get();
            }

            public boolean shouldDropFrame(
                final InetSocketAddress address,
                final UnsafeBuffer buffer,
                final int streamId,
                final int sessionId,
                final int termId,
                final int termOffset,
                final int length)
            {
                droppedFrames.incrementAndGet();
                return shouldDropFrame.get();
            }
        }
    }
}