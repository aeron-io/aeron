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

import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.service.Cluster;
import io.aeron.driver.MediaDriver;
import io.aeron.samples.archive.RecordingDescriptor;
import io.aeron.samples.archive.RecordingDescriptorCollector;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.concurrent.SystemEpochClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.Configuration.PRE_TOUCH_MAPPED_MEMORY_PROP_NAME;
import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class SingleNodeTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(20)
    void shouldStartCluster()
    {
        final TestCluster cluster = aCluster().withStaticNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        assertEquals(0, leader.index());
        assertEquals(Cluster.Role.LEADER, leader.role());
    }

    @Test
    @InterruptAfter(20)
    void shouldNotConsiderItselfInactiveAndEnterAnElection()
    {
        final OffsetMillisecondClusterClock clusterClock = new OffsetMillisecondClusterClock(SystemEpochClock.INSTANCE);
        final TestCluster cluster = aCluster()
            .withClusterClock(clusterClock)
            .withStaticNodes(1)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        assertEquals(0, leader.index());
        assertEquals(Cluster.Role.LEADER, leader.role());

        final long heartbeatTimeOutNs = leader.consensusModule().context().leaderHeartbeatTimeoutNs();
        clusterClock.addOffset(TimeUnit.NANOSECONDS.toMillis(heartbeatTimeOutNs) * 2);

        final ClusterMembership clusterMembership = leader.clusterMembership();
        assertEquals(1, clusterMembership.activeMembers.size());
        assertEquals(0, clusterMembership.activeMembers.get(0).leadershipTermId());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    @InterruptAfter(20)
    void shouldSendMessagesToCluster(final boolean preTouch)
    {
        System.setProperty(PRE_TOUCH_MAPPED_MEMORY_PROP_NAME, Boolean.toString(preTouch));
        try
        {
            final TestCluster cluster = aCluster().withStaticNodes(1).start();
            systemTestWatcher.cluster(cluster);

            final TestNode leader = cluster.awaitLeader();

            assertEquals(0, leader.index());
            assertEquals(Cluster.Role.LEADER, leader.role());

            cluster.connectClient();
            cluster.sendMessages(10);
            cluster.awaitResponseMessageCount(10);
            cluster.awaitServiceMessageCount(leader, 10);
        }
        finally
        {
            System.clearProperty(PRE_TOUCH_MAPPED_MEMORY_PROP_NAME);
        }
    }

    @Test
    @InterruptAfter(20)
    void shouldReplayLog()
    {
        final TestCluster cluster = aCluster().withStaticNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServiceMessageCount(leader, messageCount);

        cluster.stopNode(leader);

        cluster.startStaticNode(0, false);
        final TestNode newLeader = cluster.awaitLeader();
        cluster.awaitServiceMessageCount(newLeader, messageCount);
    }

    @Test
    @InterruptAfter(20)
    void shouldReplayLogWithPaddingAtEndOfRecording()
    {
        final TestCluster cluster = aCluster().withStaticNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int largeMessageCount = 481;
        cluster.connectClient();
        cluster.sendLargeMessages(largeMessageCount);
        cluster.awaitResponseMessageCount(largeMessageCount);
        cluster.awaitServiceMessageCount(leader, largeMessageCount);
        final int smallMessageCount = 8;
        cluster.sendMessages(smallMessageCount);
        cluster.awaitResponseMessageCount(largeMessageCount + smallMessageCount);
        cluster.awaitServiceMessageCount(leader, largeMessageCount + smallMessageCount);

        final String aeronDirectoryName = leader.mediaDriver().context().aeronDirectoryName();
        final File archiveDir = leader.archive().context().archiveDir();

        cluster.stopClient();
        cluster.stopAllNodes();

        truncateRecordingToTermLength(aeronDirectoryName, archiveDir);

        cluster.startStaticNode(0, false);
        final TestNode newLeader = cluster.awaitLeader();
        cluster.awaitServiceMessageCount(newLeader, largeMessageCount);
    }

    private void truncateRecordingToTermLength(final String aeronDirectoryName, final File archiveDir)
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirectoryName);
        final Archive.Context archiveCtx = TestContexts.localhostArchive()
            .archiveDir(archiveDir);
        final AeronArchive.Context aeronArchiveCtx = TestContexts.localhostAeronArchive()
            .aeronDirectoryName(aeronDirectoryName);

        try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(driverCtx, archiveCtx);
            AeronArchive archive = AeronArchive.connect(aeronArchiveCtx))
        {
            assertNotNull(driver);
            final RecordingDescriptorCollector recordingDescriptorCollector = new RecordingDescriptorCollector(1);
            archive.listRecordings(0, Integer.MAX_VALUE, recordingDescriptorCollector.reset());
            final List<RecordingDescriptor> descriptors = recordingDescriptorCollector.descriptors();
            assertEquals(1, descriptors.size());
            final RecordingDescriptor recordingDescriptor = descriptors.get(0);
            assertEquals(512 * 1024, recordingDescriptor.termBufferLength());
            assertThat(recordingDescriptor.stopPosition(), greaterThan((long)recordingDescriptor.termBufferLength()));
            archive.truncateRecording(recordingDescriptor.recordingId(), recordingDescriptor.termBufferLength());
        }
    }
}
