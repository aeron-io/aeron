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

import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.NameResolverAgent;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.aeron.test.DataCollector;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.cluster.StubClusteredService;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.driver.RedirectingNameResolver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class MultiModuleSharedDriverTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(20)
    @SuppressWarnings({ "try", "methodlength" })
    void shouldSupportTwoSingleNodeClusters()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .nameResolver(new RedirectingNameResolver(TestCluster.DEFAULT_NODE_MAPPINGS))
            .dirDeleteOnShutdown(false)
            .dirDeleteOnStart(true);

        final Archive.Context archiveCtx = TestContexts.localhostArchive()
            .threadingMode(ArchiveThreadingMode.SHARED)
            .archiveDir(new File(SystemUtil.tmpDirName(), "archive"))
            .recordingEventsEnabled(false)
            .deleteArchiveOnStart(true);

        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(driverCtx, archiveCtx))
        {
            final ConsensusModule.Context moduleCtx0 = TestContexts.localhostConsensusModule()
                .clusterId(0)
                .deleteDirOnStart(true)
                .clusterDir(new File(SystemUtil.tmpDirName(), "cluster-0-0"))
                .logChannel("aeron:ipc?term-length=64k")
                .logStreamId(100)
                .serviceStreamId(104)
                .consensusModuleStreamId(105)
                .ingressChannel("aeron:udp?endpoint=localhost:9020")
                .replicationChannel("aeron:udp?endpoint=localhost:0");

            final ClusteredServiceContainer.Context containerCtx0 = new ClusteredServiceContainer.Context()
                .clusterId(moduleCtx0.clusterId())
                .clusteredService(new EchoService())
                .clusterDir(moduleCtx0.clusterDir())
                .serviceStreamId(moduleCtx0.serviceStreamId())
                .consensusModuleStreamId(moduleCtx0.consensusModuleStreamId());

            final ConsensusModule.Context moduleCtx1 = TestContexts.localhostConsensusModule()
                .clusterId(1)
                .deleteDirOnStart(true)
                .clusterDir(new File(SystemUtil.tmpDirName(), "cluster-0-1"))
                .logChannel("aeron:ipc?term-length=64k")
                .logStreamId(200)
                .serviceStreamId(204)
                .consensusModuleStreamId(205)
                .ingressChannel("aeron:udp?endpoint=localhost:9021")
                .replicationChannel("aeron:udp?endpoint=localhost:0");

            final ClusteredServiceContainer.Context containerCtx1 = new ClusteredServiceContainer.Context()
                .clusteredService(new EchoService())
                .clusterDir(moduleCtx1.clusterDir())
                .serviceStreamId(moduleCtx1.serviceStreamId())
                .consensusModuleStreamId(moduleCtx1.consensusModuleStreamId())
                .clusterId(moduleCtx1.clusterId());

            ConsensusModule consensusModule0 = null;
            ClusteredServiceContainer container0 = null;
            ConsensusModule consensusModule1 = null;
            ClusteredServiceContainer container1 = null;
            AeronCluster client0 = null;
            AeronCluster client1 = null;

            try
            {
                consensusModule0 = ConsensusModule.launch(moduleCtx0);
                consensusModule1 = ConsensusModule.launch(moduleCtx1);

                container0 = ClusteredServiceContainer.launch(containerCtx0);
                container1 = ClusteredServiceContainer.launch(containerCtx1);

                final MutableReference<String> egress = new MutableReference<>();
                final EgressListener egressListener = (clusterSessionId, timestamp, buffer, offset, length, header) ->
                    egress.set(buffer.getStringWithoutLengthAscii(offset, length));

                client0 = AeronCluster.connect(new AeronCluster.Context()
                    .egressListener(egressListener)
                    .ingressChannel(moduleCtx0.ingressChannel())
                    .egressChannel("aeron:udp?endpoint=localhost:0"));

                client1 = AeronCluster.connect(new AeronCluster.Context()
                    .egressListener(egressListener)
                    .ingressChannel(moduleCtx1.ingressChannel())
                    .egressChannel("aeron:udp?endpoint=localhost:0"));

                echoMessage(client0, "Message 0", egress);
                echoMessage(client1, "Message 1", egress);
            }
            finally
            {
                systemTestWatcher.dataCollector().add(moduleCtx0.clusterDir());
                systemTestWatcher.dataCollector().add(moduleCtx1.clusterDir());
                CloseHelper.closeAll(client0, client1, consensusModule0, consensusModule1, container0, container1);
            }
        }
        finally
        {
            systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());
            systemTestWatcher.dataCollector().add(archiveCtx.archiveDir());
        }
    }

    @Test
    @InterruptAfter(30)
    void shouldSupportTwoMultiNodeClusters()
    {
        try (MultiClusterNode node0 = new MultiClusterNode(0, systemTestWatcher.dataCollector());
            MultiClusterNode node1 = new MultiClusterNode(1, systemTestWatcher.dataCollector()))
        {
            final MutableReference<String> egress = new MutableReference<>();
            final EgressListener egressListener = (clusterSessionId, timestamp, buffer, offset, length, header) ->
                egress.set(buffer.getStringWithoutLengthAscii(offset, length));

            try (
                AeronCluster client0 = AeronCluster.connect(new AeronCluster.Context()
                    .aeronDirectoryName(node0.archivingMediaDriver.mediaDriver().aeronDirectoryName())
                    .egressListener(egressListener)
                    .ingressChannel("aeron:udp?term-length=64k")
                    .ingressEndpoints(TestCluster.ingressEndpoints(
                        node0.consensusModule0.context().clusterId(), 0, 2, 2))
                    .egressChannel("aeron:udp?endpoint=localhost:0"));
                AeronCluster client1 = AeronCluster.connect(new AeronCluster.Context()
                    .aeronDirectoryName(node1.archivingMediaDriver.mediaDriver().aeronDirectoryName())
                    .egressListener(egressListener)
                    .ingressChannel("aeron:udp?term-length=64k")
                    .ingressEndpoints(TestCluster.ingressEndpoints(
                        node1.consensusModule1.context().clusterId(), 0, 2, 2))
                    .egressChannel("aeron:udp?endpoint=localhost:0")))
            {
                echoMessage(client0, "Message 0", egress);
                echoMessage(client1, "Message 1", egress);
            }
        }
    }

    private void echoMessage(final AeronCluster client, final String message, final MutableReference<String> egress)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final int messageLength = buffer.putStringWithoutLengthAscii(0, message);

        while (client.offer(buffer, 0, messageLength) < 0)
        {
            Tests.yield();
        }

        egress.set(null);
        while (null == egress.get())
        {
            Tests.yield();
            client.pollEgress();
        }

        assertEquals(message, egress.get());
    }

    static class EchoService extends StubClusteredService
    {
        public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            echoMessage(session, buffer, offset, length);
        }
    }

    static class MultiClusterNode implements AutoCloseable
    {
        final int nodeId;
        final DataCollector dataCollector;
        final ArchivingMediaDriver archivingMediaDriver;
        final NameResolverAgent nameResolver;

        final ConsensusModule consensusModule0;
        final ClusteredServiceContainer container0;
        AeronCluster client0;

        final ConsensusModule consensusModule1;
        final ClusteredServiceContainer container1;
        AeronCluster client1;

        MultiClusterNode(final int nodeId, final DataCollector dataCollector)
        {
            this.nodeId = nodeId;
            this.dataCollector = dataCollector;
            nameResolver = new RedirectingNameResolver(TestCluster.DEFAULT_NODE_MAPPINGS);

            final MediaDriver.Context driverCtx = new MediaDriver.Context()
                .aeronDirectoryName(CommonContext.generateRandomDirName())
                .threadingMode(ThreadingMode.SHARED)
                .nameResolver(nameResolver)
                .dirDeleteOnStart(true);

            final Archive.Context archiveCtx = TestContexts.localhostArchive()
                .threadingMode(ArchiveThreadingMode.SHARED)
                .archiveDir(new File(SystemUtil.tmpDirName(), "archive-" + nodeId))
                .controlChannel("aeron:udp?endpoint=localhost:801" + nodeId)
                .recordingEventsEnabled(false)
                .deleteArchiveOnStart(true);

            archivingMediaDriver = ArchivingMediaDriver.launch(driverCtx, archiveCtx);
            dataCollector.add(archivingMediaDriver.archive().context().archiveDir());
            dataCollector.add(archivingMediaDriver.mediaDriver().context().aeronDirectory());

            consensusModule0 = consensusModule(0, driverCtx.aeronDirectoryName());
            dataCollector.add(consensusModule0.context().clusterDir());
            container0 = container(consensusModule0.context());

            consensusModule1 = consensusModule(1, driverCtx.aeronDirectoryName());
            dataCollector.add(consensusModule1.context().clusterDir());
            container1 = container(consensusModule1.context());
        }

        public void close()
        {
            CloseHelper.closeAll(
                client0,
                consensusModule0,
                container0,
                client1,
                consensusModule1,
                container1,
                archivingMediaDriver);
        }

        ConsensusModule consensusModule(final int clusterId, final String aeronDirectoryName)
        {
            final int nodeOffset = (clusterId * 100) + (nodeId * 10);
            final String ingressChannelWithInvalidEndpointFormatToBeRemovedByNameResolver =
                "aeron:udp?term-length=64k|endpoint=node" + nodeId + ":2" + clusterId + "11" + nodeId;
            final ConsensusModule.Context ctx = new ConsensusModule.Context()
                .clusterMemberId(nodeId)
                .clusterId(clusterId)
                .deleteDirOnStart(true)
                .aeronDirectoryName(aeronDirectoryName)
                .clusterDir(new File(SystemUtil.tmpDirName(), "cluster-" + nodeId + "-" + clusterId))
                .clusterMembers(TestCluster.clusterMembers(clusterId, 0, 2, 2, false))
                .logChannel("aeron:udp?term-length=64k")
                .serviceStreamId(104 + nodeOffset)
                .consensusModuleStreamId(105 + nodeOffset)
                .ingressChannel(ingressChannelWithInvalidEndpointFormatToBeRemovedByNameResolver)
                .replicationChannel("aeron:udp?endpoint=localhost:0");

            return ConsensusModule.launch(ctx);
        }

        ClusteredServiceContainer container(final ConsensusModule.Context moduleCtx)
        {
            final ClusteredServiceContainer.Context ctx = new ClusteredServiceContainer.Context()
                .clusterId(moduleCtx.clusterId())
                .clusteredService(new EchoService())
                .aeronDirectoryName(moduleCtx.aeronDirectoryName())
                .clusterDir(moduleCtx.clusterDir())
                .serviceStreamId(moduleCtx.serviceStreamId())
                .consensusModuleStreamId(moduleCtx.consensusModuleStreamId());

            return ClusteredServiceContainer.launch(ctx);
        }
    }
}
