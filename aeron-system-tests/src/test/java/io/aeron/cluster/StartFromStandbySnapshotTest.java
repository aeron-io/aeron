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

import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.IoUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SlowTest
@ExtendWith({ InterruptingTestCallback.class, EventLogExtension.class })
class StartFromStandbySnapshotTest
{
    private static final int MESSAGE_COUNT = 10;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private TestCluster cluster;

    @Test
    @InterruptAfter(40)
    void shouldSnapshotAndRestartFollowerStartedWithOnlyAStandbySnapshotFromAPeer()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        cluster.connectClient();
        cluster.sendMessages(MESSAGE_COUNT);
        cluster.awaitResponseMessageCount(MESSAGE_COUNT);

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);
        cluster.awaitNeutralControlToggle(leader);

        cluster.sendMessages(MESSAGE_COUNT);
        cluster.awaitResponseMessageCount(MESSAGE_COUNT * 2);

        final TestNode peer = cluster.followers().get(0);
        final TestNode replaced = cluster.followers().get(1);
        final int replacedIndex = replaced.index();
        final File clusterDir = replaced.consensusModule().context().clusterDir();
        final File archiveDir = replaced.archive().context().archiveDir();
        final String peerArchiveEndpoint = ClusterMember.findMember(
            ClusterMember.parse(peer.consensusModule().context().clusterMembers()), peer.index()).archiveEndpoint();
        final long peerSnapshotLogPosition = latestSnapshotLogPosition(peer.consensusModule().context().clusterDir());

        cluster.stopNode(replaced);
        IoUtil.delete(clusterDir, false);
        IoUtil.delete(archiveDir, false);
        IoUtil.ensureDirectoryExists(clusterDir, "cluster");
        appendStandbySnapshots(peer.consensusModule().context().clusterDir(), clusterDir, peerArchiveEndpoint);

        final TestNode replacement = cluster.startStaticNode(replacedIndex, false);
        cluster.awaitServiceMessageCount(replacement, MESSAGE_COUNT * 2);
        assertEquals(peerSnapshotLogPosition, latestSnapshotLogPosition(clusterDir));

        final long logRecordingId = lastTermRecordingId(clusterDir);
        assertNotEquals(NULL_VALUE, logRecordingId);

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(2);
        cluster.awaitNeutralControlToggle(leader);

        cluster.stopNode(replacement);
        final TestNode restarted = cluster.startStaticNode(replacedIndex, false);

        cluster.sendMessages(MESSAGE_COUNT);
        cluster.awaitResponseMessageCount(MESSAGE_COUNT * 3);
        cluster.awaitServiceMessageCount(restarted, MESSAGE_COUNT * 3);

        assertEquals(logRecordingId, lastTermRecordingId(clusterDir));
    }

    private static void appendStandbySnapshots(
        final File peerClusterDir, final File clusterDir, final String peerArchiveEndpoint)
    {
        try (RecordingLog peerRecordingLog = new RecordingLog(peerClusterDir, false);
            RecordingLog recordingLog = new RecordingLog(clusterDir, true))
        {
            appendStandbySnapshot(peerRecordingLog, recordingLog, 0, peerArchiveEndpoint);
            appendStandbySnapshot(
                peerRecordingLog, recordingLog, ConsensusModule.Configuration.SERVICE_ID, peerArchiveEndpoint);
        }
    }

    private static void appendStandbySnapshot(
        final RecordingLog peerRecordingLog,
        final RecordingLog recordingLog,
        final int serviceId,
        final String peerArchiveEndpoint)
    {
        final RecordingLog.Entry snapshot = peerRecordingLog.getLatestSnapshot(serviceId);
        assertNotNull(snapshot);

        recordingLog.appendStandbySnapshot(
            snapshot.recordingId,
            snapshot.leadershipTermId,
            snapshot.termBaseLogPosition,
            snapshot.logPosition,
            snapshot.timestamp,
            snapshot.serviceId,
            peerArchiveEndpoint);
    }

    private static long latestSnapshotLogPosition(final File clusterDir)
    {
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false))
        {
            final RecordingLog.Entry snapshot =
                recordingLog.getLatestSnapshot(ConsensusModule.Configuration.SERVICE_ID);
            assertNotNull(snapshot);

            return snapshot.logPosition;
        }
    }

    private static long lastTermRecordingId(final File clusterDir)
    {
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false))
        {
            return recordingLog.findLastTermRecordingId();
        }
    }
}
