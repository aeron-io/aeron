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
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.Tests;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.cluster.ClusterExtensionTestUtil.ClusterClient.NODE_0_INGRESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({EventLogExtension.class, InterruptingTestCallback.class})
public class UnexpectedElectionTest
{

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
        try (
            ClusterExtensionTestUtil.ClusterNode node0 = new ClusterExtensionTestUtil.ClusterNode(
                0, 0, nodeDir0, waitingToOfferFragmentedMessage);
            ClusterExtensionTestUtil.ClusterNode node1 = new ClusterExtensionTestUtil.ClusterNode(
                1, 0, nodeDir1, waitingToOfferFragmentedMessage);
            ClusterExtensionTestUtil.ClusterNode node2 = new ClusterExtensionTestUtil.ClusterNode(
                2, 0, nodeDir2, waitingToOfferFragmentedMessage))
        {
            node0.consensusModuleContext().leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1));
            node1.consensusModuleContext().leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1));
            node2.consensusModuleContext().leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1));

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

            try (ClusterExtensionTestUtil.ClusterClient client0 = new ClusterExtensionTestUtil.ClusterClient(
                NODE_0_INGRESS, clientDir))
            {
                client0.launch();

                Tests.await(() ->
                {
                    node0.poll(); node1.poll(); node2.poll();
                    return client0.connect();
                });
            }

            final long expectedPositionLowerBound = ClusterExtensionTestUtil.NEW_LEADERSHIP_TERM_LENGTH +
                ClusterExtensionTestUtil.SESSION_OPEN_LENGTH_BLOCK_LENGTH +
                ClusterExtensionTestUtil.SESSION_CLOSE_LENGTH;
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
                    node0.commitPosition() <
                    (commitPositionBeforeFragmentedMessage + ClusterExtensionTestUtil.EIGHT_MEGABYTES);
            });

            final long expectedAppendPosition = commitPositionBeforeFragmentedMessage +
                ClusterExtensionTestUtil.EIGHT_MEGABYTES_LENGTH;
            Tests.await(() -> node0.appendPosition() == expectedAppendPosition);

            Tests.sleep(TimeUnit.NANOSECONDS.toMillis(node0.consensusModuleContext().leaderHeartbeatTimeoutNs()) + 1);
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
        try (
            ClusterExtensionTestUtil.ClusterNode node0 = new ClusterExtensionTestUtil.ClusterNode(
                0, 0, nodeDir0, waitingToOfferFragmentedMessage);
            ClusterExtensionTestUtil.ClusterNode node1 = new ClusterExtensionTestUtil.ClusterNode(
                1, 0, nodeDir1, waitingToOfferFragmentedMessage);
            ClusterExtensionTestUtil.ClusterNode node2 = new ClusterExtensionTestUtil.ClusterNode(
                2, 0, nodeDir2, waitingToOfferFragmentedMessage))
        {
            node0.consensusModuleContext().logFragmentLimit(1);
            node1.consensusModuleContext().logFragmentLimit(1000);
            node2.consensusModuleContext().logFragmentLimit(1000);
            node0.clusteredServiceContext().logFragmentLimit(1000);
            node1.clusteredServiceContext().logFragmentLimit(1000);
            node2.clusteredServiceContext().logFragmentLimit(1000);

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

            try (ClusterExtensionTestUtil.ClusterClient client0 = new ClusterExtensionTestUtil.ClusterClient(
                NODE_0_INGRESS, clientDir))
            {
                client0.launch();

                Tests.await(() ->
                {
                    node0.poll();
                    node1.poll();
                    node2.poll();
                    return client0.connect();
                });
            }

            final long expectedPositionLowerBound = ClusterExtensionTestUtil.NEW_LEADERSHIP_TERM_LENGTH +
                ClusterExtensionTestUtil.SESSION_OPEN_LENGTH_BLOCK_LENGTH +
                ClusterExtensionTestUtil.SESSION_CLOSE_LENGTH;
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
                    node0.commitPosition() <
                        (commitPositionBeforeFragmentedMessage + ClusterExtensionTestUtil.EIGHT_MEGABYTES);
            });

            final long expectedAppendPosition = commitPositionBeforeFragmentedMessage +
                ClusterExtensionTestUtil.EIGHT_MEGABYTES_LENGTH;
            Tests.await(() -> node0.appendPosition() == expectedAppendPosition);

            final Counter controlToggle = node0.consensusModuleContext().controlToggleCounter();
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
            assertEquals(1L, node0.consensusModuleContext().snapshotCounter().get());
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

}
