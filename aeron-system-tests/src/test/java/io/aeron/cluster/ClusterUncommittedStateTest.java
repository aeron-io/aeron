/*
 * Copyright 2014-2024 Real Logic Limited.
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
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.IpTables;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.collections.LongArrayQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
@EnabledOnOs(OS.LINUX)
public class ClusterUncommittedStateTest
{
    private static final List<String> HOSTNAMES = List.of("127.2.0.0", "127.2.1.0", "127.2.2.0");
    private static final String CHAIN_NAME = "CLUSTER-TEST";

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();
    private TestCluster cluster;

    @BeforeEach
    void setUp()
    {
        IpTables.setupChain(CHAIN_NAME);
    }

    @AfterEach
    void tearDown()
    {
        IpTables.tearDownChain(CHAIN_NAME);
    }

    @Test
    @SlowTest
    @InterruptAfter(20)
    void shouldSnapshotWithNoServicesWithUncommittedData()
    {
        cluster = aCluster()
            .withStaticNodes(HOSTNAMES.size())
            .withCustomAddresses(HOSTNAMES)
            .withExtensionSuppler(TestCounterExtension::new)
            .withServiceSupplier(value -> new TestNode.TestService[0])
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();
        final List<String> leaderHostname = List.of(HOSTNAMES.get(firstLeader.memberId()));
        final List<String> followerHostnames = new ArrayList<>(HOSTNAMES);
        followerHostnames.remove(firstLeader.memberId());

        IpTables.makeSymmetricNetworkPartition(CHAIN_NAME, leaderHostname, followerHostnames);

        cluster.connectClient();
        cluster.sendExtensionMessages(32);
        final long messageLength = BitUtil.align(
            DataHeaderFlyweight.HEADER_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH + BitUtil.SIZE_OF_INT,
            FrameDescriptor.FRAME_ALIGNMENT);
        Tests.await(() -> firstLeader.appendPosition() > 32L * messageLength);

        cluster.takeSnapshot(firstLeader);
        Tests.await(() -> ConsensusModule.State.SNAPSHOT == firstLeader.moduleState());

        IpTables.flushChain(CHAIN_NAME);
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

}