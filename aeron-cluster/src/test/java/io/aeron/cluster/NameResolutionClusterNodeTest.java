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
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.exceptions.InvalidChannelException;
import io.aeron.logbuffer.Header;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.cluster.ClusterTests;
import io.aeron.test.cluster.StubClusteredService;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CopyOnWriteArrayList;

import static io.aeron.cluster.ClusterTestConstants.CLUSTER_MEMBERS;
import static io.aeron.cluster.ClusterTestConstants.INGRESS_ENDPOINTS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(InterruptingTestCallback.class)
class NameResolutionClusterNodeTest
{
    private MediaDriver mediaDriver;
    private Archive archive;
    private ConsensusModule consensusModule;
    private ClusteredServiceContainer container;
    private AeronCluster aeronCluster;

    private final CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();

    @BeforeEach
    void before()
    {
        mediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .errorHandler(errors::add)
            .dirDeleteOnStart(true));

        archive = Archive.launch(TestContexts.localhostArchive()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .catalogCapacity(ClusterTestConstants.CATALOG_CAPACITY)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .recordingEventsEnabled(false)
            .deleteArchiveOnStart(true));

        consensusModule = ConsensusModule.launch(new ConsensusModule.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .errorHandler(ClusterTests.errorHandler(0))
            .terminationHook(ClusterTests.NOOP_TERMINATION_HOOK)
            .logChannel("aeron:ipc")
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .ingressChannel("aeron:udp")
            .clusterMembers(CLUSTER_MEMBERS)
            .deleteDirOnStart(true));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeronCluster, consensusModule, container, archive, mediaDriver);

        if (null != container)
        {
            container.context().deleteDirectory();
        }

        if (null != consensusModule)
        {
            consensusModule.context().deleteDirectory();
        }

        if (null != archive)
        {
            archive.context().deleteDirectory();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldConnectAndSendKeepAliveWithBadName()
    {
        container = launchEchoService();
        aeronCluster = connectToCluster();

        assertTrue(aeronCluster.sendKeepAlive());

        assertEquals(1, errors.size(), errors::toString);
        final Throwable exception = errors.get(0);

        assertInstanceOf(InvalidChannelException.class, exception);
        assertThat(exception.getMessage(), containsString("badname"));

        ClusterTests.failOnClusterError();
    }

    private ClusteredServiceContainer launchEchoService()
    {
        final ClusteredService clusteredService = new StubClusteredService()
        {
            @Override
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
        };

        return ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .clusteredService(clusteredService)
                .errorHandler(Tests::onError));
    }

    private AeronCluster connectToCluster()
    {
        final ErrorHandler errorHandler =
            (t) ->
            {
                System.err.println("** MY HANDLER **");
                t.printStackTrace();
            };

        return AeronCluster.connect(
            new AeronCluster.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .errorHandler(errorHandler)
                .ingressChannel("aeron:udp")
                .ingressEndpoints(INGRESS_ENDPOINTS + ",1=badname:9011")
                .egressChannel("aeron:udp?endpoint=localhost:0"));
    }
}
