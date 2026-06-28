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
import io.aeron.cluster.codecs.mark.ClusterComponentType;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.CapturingPrintStream;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.concurrent.SystemEpochClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.matchesRegex;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class ClusterToolTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(30)
    void shouldHandleSnapshotOnLeaderOnly()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        cluster.connectClient();
        final long initialSnapshotCount = leader.consensusModule().context().snapshotCounter().get();
        final CapturingPrintStream capturingPrintStream = new CapturingPrintStream();

        assertTrue(ClusterTool.snapshot(
            leader.consensusModule().context().clusterDir(),
            capturingPrintStream.resetAndGetPrintStream()));

        assertThat(
            capturingPrintStream.flushAndGetContent(),
            containsString("SNAPSHOT applied successfully"));

        final long expectedSnapshotCount = initialSnapshotCount + 1;
        cluster.awaitSnapshotCount(expectedSnapshotCount);

        for (final TestNode follower : cluster.followers())
        {
            assertFalse(ClusterTool.snapshot(
                follower.consensusModule().context().clusterDir(),
                capturingPrintStream.resetAndGetPrintStream()));

            assertThat(
                capturingPrintStream.flushAndGetContent(),
                containsString("Current node is not the leader"));
        }
    }

    @Test
    @InterruptAfter(30)
    void shouldDescribeLatestConsensusModuleSnapshot()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final CapturingPrintStream capturingPrintStream = new CapturingPrintStream();

        assertTrue(ClusterTool.snapshot(
            leader.consensusModule().context().clusterDir(),
            capturingPrintStream.resetAndGetPrintStream()));

        assertTrue(ClusterTool.describeLatestConsensusModuleSnapshot(
            capturingPrintStream.resetAndGetPrintStream(),
            leader.consensusModule().context().clusterDir()));

        assertThat(
            capturingPrintStream.flushAndGetContent(),
            containsString("Snapshot: appVersion=1 timeUnit=MILLISECONDS"));
    }

    @Test
    @InterruptAfter(30)
    void shouldNotSnapshotWhenSuspendedOnly()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final long initialSnapshotCount = leader.consensusModule().context().snapshotCounter().get();
        final CapturingPrintStream capturingPrintStream = new CapturingPrintStream();

        assertTrue(ClusterTool.suspend(
            leader.consensusModule().context().clusterDir(),
            capturingPrintStream.resetAndGetPrintStream()));

        assertThat(
            capturingPrintStream.flushAndGetContent(),
            containsString("SUSPEND applied successfully"));

        assertFalse(ClusterTool.snapshot(
            leader.consensusModule().context().clusterDir(),
            capturingPrintStream.resetAndGetPrintStream()));

        final String expectedMessage =
            "Unable to SNAPSHOT as the state of the consensus module is SUSPENDED, but needs to be ACTIVE";
        assertThat(capturingPrintStream.flushAndGetContent(), containsString(expectedMessage));

        assertEquals(initialSnapshotCount, leader.consensusModule().context().snapshotCounter().get());
    }

    @Test
    @InterruptAfter(30)
    void shouldSuspendAndResume()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final CapturingPrintStream capturingPrintStream = new CapturingPrintStream();

        assertTrue(ClusterTool.suspend(
            leader.consensusModule().context().clusterDir(),
            capturingPrintStream.resetAndGetPrintStream()));

        assertThat(
            capturingPrintStream.flushAndGetContent(),
            containsString("SUSPEND applied successfully"));

        assertTrue(ClusterTool.resume(
            leader.consensusModule().context().clusterDir(),
            capturingPrintStream.resetAndGetPrintStream()));

        assertThat(
            capturingPrintStream.flushAndGetContent(),
            containsString("RESUME applied successfully"));
    }

    @Test
    @InterruptAfter(30)
    void shouldFailIfMarkFileUnavailable(final @TempDir Path emptyClusterDir)
    {
        final CapturingPrintStream capturingPrintStream = new CapturingPrintStream();

        assertFalse(ClusterTool.snapshot(emptyClusterDir.toFile(), capturingPrintStream.resetAndGetPrintStream()));
        assertThat(
            capturingPrintStream.flushAndGetContent(),
            containsString("cluster-mark.dat does not exist"));
    }

    @Test
    @InterruptAfter(30)
    void shouldBeAbleToAccessClusterMarkFilesInANonDefaultLocation(final @TempDir File markFileDir)
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).markFileBaseDir(markFileDir).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        cluster.connectClient();
        cluster.sendErrorGeneratingMessages(1);
        cluster.awaitResponseMessageCount(1);

        final CapturingPrintStream stream = new CapturingPrintStream();
        ClusterTool.errors(
            stream.resetAndGetPrintStream(),
            cluster.node(leader.index()).consensusModule().context().clusterDir());

        final String errorContent = stream.flushAndGetContent();
        assertThat(errorContent, containsString("This message will cause an error"));
        assertThat(errorContent, containsString("mark file exists"));
        final String path = markFileDir.getName();
        final Pattern serviceMarkFileName = Pattern.compile(
            ".*mark file exists:.*" + path + ".*cluster-mark-service-0.dat.*", Pattern.DOTALL);
        assertThat("Tool output: " + errorContent, errorContent, matchesRegex(serviceMarkFileName));
    }

    @Test
    @InterruptAfter(30)
    void sortRecordingLogIsANoOpIfRecordLogIsEmpty(final @TempDir Path emptyClusterDir) throws IOException
    {
        final File clusterDir = emptyClusterDir.toFile();
        final RecordingLog recordingLog = new RecordingLog(clusterDir, true);
        recordingLog.close();

        final Path logFile = emptyClusterDir.resolve(RecordingLog.RECORDING_LOG_FILE_NAME);

        final boolean result = ClusterTool.sortRecordingLog(clusterDir);

        assertFalse(result);
        assertArrayEquals(new byte[0], Files.readAllBytes(logFile));
    }

    @Test
    @InterruptAfter(30)
    void sortRecordingLogIsANoOpIfRecordDoesNotExist(final @TempDir Path emptyClusterDir)
    {
        final File clusterDir = emptyClusterDir.toFile();
        final Path logFile = emptyClusterDir.resolve(RecordingLog.RECORDING_LOG_FILE_NAME);

        final boolean result = ClusterTool.sortRecordingLog(clusterDir);

        assertFalse(result);
        assertFalse(Files.exists(logFile));
    }

    @Test
    @InterruptAfter(30)
    void sortRecordingLogIsANoOpIfRecordLogIsAlreadySorted(final @TempDir Path emptyClusterDir) throws IOException
    {
        final File clusterDir = emptyClusterDir.toFile();
        final Path logFile = emptyClusterDir.resolve(RecordingLog.RECORDING_LOG_FILE_NAME);
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true))
        {
            recordingLog.appendTerm(21, 0, 100, 100);
            recordingLog.appendSnapshot(0, 0, 100, 0, 200, 0);
            recordingLog.appendTerm(21, 1, 1024, 200);
        }

        final byte[] originalBytes = Files.readAllBytes(logFile);
        assertNotEquals(0, originalBytes.length);

        final boolean result = ClusterTool.sortRecordingLog(clusterDir);

        assertFalse(result);
        assertArrayEquals(originalBytes, Files.readAllBytes(logFile));
    }

    @Test
    @InterruptAfter(30)
    void sortRecordingLogShouldRearrangeDataOnDisc(final @TempDir Path emptyClusterDir) throws IOException
    {
        final File clusterDir = emptyClusterDir.toFile();
        final Path logFile = emptyClusterDir.resolve(RecordingLog.RECORDING_LOG_FILE_NAME);
        final List<RecordingLog.Entry> sortedEntries = new ArrayList<>();
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true))
        {
            recordingLog.appendTerm(21, 2, 100, 100);
            recordingLog.appendSnapshot(1, 2, 50, 60, 42, 89);
            recordingLog.appendTerm(21, 1, 1024, 200);
            recordingLog.appendSnapshot(0, 0, 0, 0, 200, 0);

            final List<RecordingLog.Entry> entries = recordingLog.entries();
            for (int i = 0, size = entries.size(); i < size; i++)
            {
                final RecordingLog.Entry entry = entries.get(i);
                assertNotEquals(i, entry.entryIndex);
                sortedEntries.add(new RecordingLog.Entry(
                    entry.recordingId,
                    entry.leadershipTermId,
                    entry.termBaseLogPosition,
                    entry.logPosition,
                    entry.timestamp,
                    entry.serviceId,
                    entry.type,
                    null,
                    entry.isValid,
                    i));
            }
        }

        final byte[] originalBytes = Files.readAllBytes(logFile);
        assertNotEquals(0, originalBytes.length);

        final boolean result = ClusterTool.sortRecordingLog(clusterDir);

        assertTrue(result);
        assertFalse(Arrays.equals(originalBytes, Files.readAllBytes(logFile)));
        assertArrayEquals(new String[]{ RecordingLog.RECORDING_LOG_FILE_NAME }, clusterDir.list());
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true))
        {
            final List<RecordingLog.Entry> entries = recordingLog.entries();
            assertEquals(sortedEntries, entries);
        }
    }

    @Test
    @InterruptAfter(30)
    void seedRecordingLogFromSnapshotShouldDeleteOriginalRecordingLogFileIfThereAreNoValidSnapshots(
        final @TempDir Path emptyClusterDir) throws IOException
    {
        final File clusterDir = emptyClusterDir.toFile();
        final Path logFile = emptyClusterDir.resolve(RecordingLog.RECORDING_LOG_FILE_NAME);
        final Path backupLogFile = emptyClusterDir.resolve(RecordingLog.RECORDING_LOG_FILE_NAME + ".bak");
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true))
        {
            recordingLog.appendTerm(1, 1, 0, 100);
            recordingLog.appendSnapshot(1, 1, 1000, 256, 300, 0);
            recordingLog.appendSnapshot(1, 1, 1000, 256, 300, ConsensusModule.Configuration.SERVICE_ID);
            recordingLog.appendTerm(1, 2, 2000, 400);
            recordingLog.appendSnapshot(2, 5, 56, 111, 500, 5);

            assertTrue(recordingLog.invalidateLatestSnapshot());
        }

        assertTrue(Files.exists(logFile));
        assertFalse(Files.exists(backupLogFile));
        final byte[] logContents = Files.readAllBytes(logFile);

        ClusterTool.seedRecordingLogFromSnapshot(clusterDir);

        assertFalse(Files.exists(logFile));
        assertTrue(Files.exists(backupLogFile));
        assertArrayEquals(logContents, Files.readAllBytes(backupLogFile));
    }

    @Test
    @InterruptAfter(30)
    void seedRecordingLogFromSnapshotShouldCreateANewRecordingLogFromALatestValidSnapshot(
        final @TempDir Path emptyClusterDir) throws IOException
    {
        testSeedRecordingLogFromSnapshot(emptyClusterDir, ClusterTool::seedRecordingLogFromSnapshot);
    }

    @Test
    @InterruptAfter(30)
    void seedRecordingLogFromSnapshotShouldCreateANewRecordingLogFromALatestValidSnapshotCommandLine(
        final @TempDir Path emptyClusterDir) throws IOException
    {
        testSeedRecordingLogFromSnapshot(
            emptyClusterDir,
            clusterDir -> ClusterTool.main(new String[]{ clusterDir.toString(), "seed-recording-log-from-snapshot" }));
    }

    @Test
    @InterruptAfter(30)
    void shouldCheckForLeaderInAnyStateAfterElectionWasClosed()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);
        final PrintStream out = mock(PrintStream.class);

        final TestNode leader = cluster.awaitLeader();
        assertEquals(0, ClusterTool.isLeader(out, leader.consensusModule().context().clusterDir()));
        for (final TestNode follower : cluster.followers())
        {
            assertEquals(1, ClusterTool.isLeader(out, follower.consensusModule().context().clusterDir()));
        }

        assertTrue(ClusterTool.suspend(leader.consensusModule().context().clusterDir(), out));
        assertEquals(0, ClusterTool.isLeader(out, leader.consensusModule().context().clusterDir()));
        for (final TestNode follower : cluster.followers())
        {
            assertEquals(1, ClusterTool.isLeader(out, follower.consensusModule().context().clusterDir()));
        }
    }

    @Test
    void listMembersShouldReturnFalseIfNoMarkFileExists(@TempDir final File emptyClusterDir)
    {
        final ClusterMembership clusterMembership = new ClusterMembership();
        assertFalse(ClusterTool.listMembers(clusterMembership, emptyClusterDir, 0));
    }

    @Test
    void listMembersShouldReturnFalseIfQueryTimesOut(@TempDir final Path clusterDir)
    {
        final SystemEpochClock clock = SystemEpochClock.INSTANCE;
        try (TestMediaDriver driver = TestMediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName()), systemTestWatcher);
            ClusterMarkFile clusterMarkFile = new ClusterMarkFile(
                clusterDir.resolve(ClusterMarkFile.FILENAME).toFile(),
                ClusterComponentType.BACKUP,
                1024 * 1024,
                clock,
                1000,
                LogBufferDescriptor.PAGE_MIN_SIZE))
        {
            clusterMarkFile.encoder()
                .activityTimestamp(clock.time())
                .consensusModuleStreamId(111)
                .serviceStreamId(222)
                .aeronDirectory(driver.aeronDirectoryName())
                .controlChannel("aeron:udp?endpoint=localhost:7799");
            clusterMarkFile.signalReady(clock.time());

            final ClusterMembership clusterMembership = new ClusterMembership();
            assertFalse(ClusterTool.listMembers(clusterMembership, clusterMarkFile.parentDirectory(), 1));
        }
    }

    private void testSeedRecordingLogFromSnapshot(final Path emptyClusterDir, final Consumer<File> truncateAction)
        throws IOException
    {
        final Path logFile = emptyClusterDir.resolve(RecordingLog.RECORDING_LOG_FILE_NAME);
        final Path backupLogFile = emptyClusterDir.resolve(RecordingLog.RECORDING_LOG_FILE_NAME + ".bak");
        Files.write(backupLogFile, new byte[]{ 0x1, -128, 0, 1, -1, 127 }, CREATE_NEW, WRITE);
        Files.setLastModifiedTime(backupLogFile, FileTime.fromMillis(0));

        final File clusterDir = emptyClusterDir.toFile();
        final List<RecordingLog.Entry> truncatedEntries = new ArrayList<>();
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true))
        {
            recordingLog.appendTerm(1, 0, 0, 0);
            recordingLog.appendSnapshot(1, 3, 4000, 4000, 600, 2);
            recordingLog.appendTerm(1, 3, 3000, 500);
            recordingLog.appendSnapshot(1, 2, 2900, 2200, 400, 2);
            recordingLog.appendSnapshot(1, 2, 2900, 2200, 400, 1);
            recordingLog.appendSnapshot(1, 2, 2900, 2200, 400, 0);
            recordingLog.appendSnapshot(1, 2, 2900, 2200, 400, ConsensusModule.Configuration.SERVICE_ID);
            recordingLog.appendTerm(1, 2, 2000, 300);
            recordingLog.appendSnapshot(1, 1, 1800, 1000, 200, ConsensusModule.Configuration.SERVICE_ID);
            recordingLog.appendSnapshot(1, 1, 1800, 1000, 200, 0);
            recordingLog.appendSnapshot(1, 1, 1800, 1000, 200, 1);
            recordingLog.appendTerm(1, 1, 1000, 100);

            assertTrue(recordingLog.invalidateLatestSnapshot());

            final List<RecordingLog.Entry> entries = recordingLog.entries();
            for (int i = 2; i < 5; i++)
            {
                final RecordingLog.Entry entry = entries.get(i);
                truncatedEntries.add(new RecordingLog.Entry(
                    entry.recordingId,
                    entry.leadershipTermId,
                    0,
                    0,
                    entry.timestamp,
                    entry.serviceId,
                    entry.type,
                    null,
                    entry.isValid,
                    i - 2));
            }
        }

        final byte[] logContents = Files.readAllBytes(logFile);
        final FileTime logLastModifiedTime = Files.getLastModifiedTime(logFile);

        truncateAction.accept(clusterDir);

        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true))
        {
            assertEquals(truncatedEntries, recordingLog.entries());
        }

        assertArrayEquals(logContents, Files.readAllBytes(backupLogFile));
        // compare up to millis, because upon copy file timestamp seems to be truncated
        // e.g. expected: <2021-09-27T09:49:22.756944756Z> but was: <2021-09-27T09:49:22.756944Z>
        assertEquals(logLastModifiedTime.toMillis(), Files.getLastModifiedTime(backupLogFile).toMillis());
    }
}
