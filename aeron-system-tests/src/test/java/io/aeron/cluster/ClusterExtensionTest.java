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
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static io.aeron.cluster.ClusterExtensionTestUtil.ClusterClient.NODE_0_INGRESS;
import static io.aeron.cluster.ClusterExtensionTestUtil.EIGHT_MEGABYTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(InterruptingTestCallback.class)
public class ClusterExtensionTest
{

    @TempDir
    public Path nodeDir0;
    @TempDir
    public Path nodeDir1;
    @TempDir
    public Path nodeDir2;
    @TempDir
    public Path clientDir;

    @Test
    @InterruptAfter(5)
    @SuppressWarnings("methodLength")
    public void shouldSnapshotWhenLogAdapterIsBehind()
    {
        try (
            ClusterExtensionTestUtil.ClusterNode node0 = new ClusterExtensionTestUtil.ClusterNode(
                0, 0, nodeDir0, null);
            ClusterExtensionTestUtil.ClusterNode node1 = new ClusterExtensionTestUtil.ClusterNode(
                1, 0, nodeDir1, null);
            ClusterExtensionTestUtil.ClusterNode node2 = new ClusterExtensionTestUtil.ClusterNode(
                2, 0, nodeDir2, null))
        {
            node0.consensusModuleContext()
                .serviceCount(0)
                .consensusModuleExtension(new TestNode.TestConsensusModuleExtension())
                .logFragmentLimit(1);
            node1.consensusModuleContext()
                .serviceCount(0)
                .consensusModuleExtension(new TestNode.TestConsensusModuleExtension())
                .logFragmentLimit(1000);
            node2.consensusModuleContext()
                .serviceCount(0)
                .consensusModuleExtension(new TestNode.TestConsensusModuleExtension())
                .logFragmentLimit(1000);

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
                client0.mediaDriverContext().publicationTermBufferLength(64 * 1024 * 1024);
                client0.launch();

                Tests.await(() ->
                {
                    node0.poll();
                    node1.poll();
                    node2.poll();
                    return client0.connect();
                });

                final UnsafeBuffer buffer = new UnsafeBuffer(new byte[EIGHT_MEGABYTES]);
                final MessageHeaderEncoder encoder = new MessageHeaderEncoder();
                encoder.wrap(buffer, 0);
                encoder.blockLength(64);
                encoder.templateId(TestCluster.EXTENSION_TEMPLATE_ID);
                encoder.schemaId(TestCluster.EXTENSION_SCHEMA_ID);
                encoder.version(TestCluster.EXTENSION_VERSION);

                Tests.await(() ->
                {
                    node0.poll();
                    node1.poll();
                    node2.poll();
                    return client0.aeronCluster().ingressPublication().offer(buffer, 0, buffer.capacity()) > 0;
                });

                Tests.await(() ->
                {
                    node0.poll();
                    node1.poll();
                    node2.poll();
                    return node0.extensionIngressCount() == 1;
                });
            }

            Tests.await(() ->
            {
                node0.poll();
                node1.poll();
                node2.poll();
                return node0.consensusModulePosition() < node0.commitPosition() + (64 * 1024) &&
                    node0.commitPosition() < node0.appendPosition();
            });

            final Counter controlToggle = node0.consensusModuleContext().controlToggleCounter();
            controlToggle.setRelease(ClusterControl.ToggleState.SNAPSHOT.code());

            Tests.await(() ->
            {
                node0.poll();
                node1.poll();
                node2.poll();
                return 1 == node0.extensionSnapshots().size() &&
                    1 == node1.extensionSnapshots().size() &&
                    1 == node2.extensionSnapshots().size();
            });

            final TestNode.TestExtensionSnapshot node0Snapshot = node0.extensionSnapshots().get(0);
            final TestNode.TestExtensionSnapshot node1Snapshot = node1.extensionSnapshots().get(0);
            final TestNode.TestExtensionSnapshot node2Snapshot = node2.extensionSnapshots().get(0);

            assertEquals(1, node1Snapshot.logMessageCount());
            assertEquals(1, node2Snapshot.logMessageCount());
            assertEquals(1, node0Snapshot.logMessageCount());
        }
    }
}
