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

import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.ConsensusModuleDecoder;
import io.aeron.cluster.codecs.ConsensusModuleEncoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.PendingMessageTrackerDecoder;
import io.aeron.cluster.codecs.PendingMessageTrackerEncoder;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.cluster.codecs.SessionMessageHeaderEncoder;
import io.aeron.samples.archive.RecordingReplicator;
import io.aeron.samples.archive.RecordingSignalCapture;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.LongArrayList;
import org.agrona.collections.MutableBoolean;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.CommonContext.NULL_SESSION_ID;
import static io.aeron.cluster.ConsensusModuleSnapshotPendingServiceMessagesPatch.replayLocalSnapshotRecording;
import static io.aeron.cluster.PendingServiceMessageTracker.*;
import static io.aeron.test.cluster.TestCluster.aCluster;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class ConsensusModuleSnapshotPendingServiceMessagesPatchTest
{
    private static final int NUM_MESSAGES = 1987;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @AfterEach
    void tearDown()
    {
        TestNode.MessageTrackingService.delaySessionMessageProcessing(false);
    }

    @Test
    void executeThrowsNullPointerExceptionIfClusterDirIsNull()
    {
        //noinspection DataFlowIssue
        assertThrowsExactly(
            NullPointerException.class,
            () -> new ConsensusModuleSnapshotPendingServiceMessagesPatch().execute(null));
    }

    @Test
    void executeThrowsIllegalArgumentExceptionIfClusterDirDoesNotExist()
    {
        final File clusterDir = new File("non-existing-file-blah-blah");

        final IllegalArgumentException exception = assertThrowsExactly(
            IllegalArgumentException.class,
            () -> new ConsensusModuleSnapshotPendingServiceMessagesPatch().execute(clusterDir));
        assertEquals("invalid cluster directory: " + clusterDir.getAbsolutePath(), exception.getMessage());
    }

    @Test
    void executeThrowsIllegalArgumentExceptionIfClusterDirIsNotADirectory(
        final @TempDir File tempDir) throws IOException
    {
        final File clusterDir = new File(tempDir, "file.txt");
        assertTrue(clusterDir.createNewFile());

        final IllegalArgumentException exception = assertThrowsExactly(
            IllegalArgumentException.class,
            () -> new ConsensusModuleSnapshotPendingServiceMessagesPatch().execute(clusterDir));
        assertEquals("invalid cluster directory: " + clusterDir.getAbsolutePath(), exception.getMessage());
    }

    @Test
    void executeThrowsClusterExceptionIfClusterDirDoesNotContainARecordingLog(final @TempDir File tempDir)
    {
        final File clusterDir = new File(tempDir, "cluster-dir");
        assertTrue(clusterDir.mkdir());

        final ClusterException exception = assertThrowsExactly(
            ClusterException.class,
            () -> new ConsensusModuleSnapshotPendingServiceMessagesPatch().execute(clusterDir));
        assertInstanceOf(IOException.class, exception.getCause());
    }

    @Test
    @SlowTest
    @InterruptAfter(90)
    void executeIsANoOpIfTheSnapshotIsValid()
    {
        final IntFunction<TestNode.TestService[]> servicesSupplier =
            (i) -> new TestNode.TestService[]
            {
                new TestNode.MessageTrackingService(1, i),
                new TestNode.MessageTrackingService(2, i)
            };
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withTimerServiceSupplier(new PriorityHeapTimerServiceSupplier())
            .withServiceSupplier(servicesSupplier)
            .start();
        systemTestWatcher.cluster(cluster);
        final int serviceCount = cluster.node(0).services().length;

        final TestNode leader = cluster.awaitLeader();
        cluster.connectClient();

        TestNode.MessageTrackingService.delaySessionMessageProcessing(true);
        int messageCount = 0;
        final ExpandableArrayBuffer msgBuffer = cluster.msgBuffer();
        for (int i = 0; i < NUM_MESSAGES; i++)
        {
            msgBuffer.putInt(0, ++messageCount, LITTLE_ENDIAN);
            cluster.pollUntilMessageSent(SIZE_OF_INT);
        }

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);
        TestNode.MessageTrackingService.delaySessionMessageProcessing(false);

        cluster.awaitResponseMessageCount(messageCount * serviceCount);
        awaitServiceMessages(cluster, serviceCount, messageCount);
        stopConsensusModulesAndServices(cluster);

        final File leaderClusterDir = leader.consensusModule().context().clusterDir();
        final RecordingLog.Entry leaderSnapshot = ClusterTool.findLatestValidSnapshot(leaderClusterDir);
        assertNotNull(leaderSnapshot);

        final MutableLong mutableNextSessionId = new MutableLong(NULL_SESSION_ID);
        final MutableLong mutableNextServiceSessionId = new MutableLong(NULL_SESSION_ID);
        final MutableLong mutableLogServiceSessionId = new MutableLong(NULL_SESSION_ID);
        final LongArrayList pendingMessageClusterSessionIds = new LongArrayList();
        final ConsensusModuleSnapshotListener stateReader = new NoOpConsensusModuleSnapshotListener()
        {
            public void onLoadConsensusModuleState(
                final long nextSessionId,
                final long nextServiceSessionId,
                final long logServiceSessionId,
                final int pendingMessageCapacity,
                final DirectBuffer buffer,
                final int offset,
                final int length)
            {
                mutableNextSessionId.set(nextSessionId);
                mutableNextServiceSessionId.set(nextServiceSessionId);
                mutableLogServiceSessionId.set(logServiceSessionId);
            }

            public void onLoadPendingMessage(
                final long clusterSessionId, final DirectBuffer buffer, final int offset, final int length)
            {
                pendingMessageClusterSessionIds.add(clusterSessionId);
            }
        };

        readSnapshotRecording(leader, leaderSnapshot.recordingId, stateReader);

        final long beforeNextSessionId = mutableNextSessionId.get();
        final long beforeNextServiceSessionId = mutableNextServiceSessionId.get();
        final long beforeLogServiceSessionId = mutableLogServiceSessionId.get();
        final long[] beforeClusterSessionIds = pendingMessageClusterSessionIds.toLongArray();
        assertNotEquals(NULL_SESSION_ID, beforeNextSessionId);
        assertNotEquals(NULL_SESSION_ID, beforeNextServiceSessionId);
        assertNotEquals(NULL_SESSION_ID, beforeLogServiceSessionId);
        assertNotEquals(beforeNextSessionId, beforeNextServiceSessionId);
        assertNotEquals(beforeNextSessionId, beforeLogServiceSessionId);
        assertNotEquals(beforeNextServiceSessionId, beforeLogServiceSessionId);
        assertNotEquals(0, beforeClusterSessionIds.length);

        final ConsensusModuleSnapshotPendingServiceMessagesPatch snapshotPatch =
            new ConsensusModuleSnapshotPendingServiceMessagesPatch(
            leader.archive().context().localControlChannel(),
            leader.archive().context().localControlStreamId());
        assertFalse(snapshotPatch.execute(leaderClusterDir));

        mutableNextSessionId.set(NULL_SESSION_ID);
        mutableNextServiceSessionId.set(NULL_SESSION_ID);
        mutableLogServiceSessionId.set(NULL_SESSION_ID);

        pendingMessageClusterSessionIds.clear();
        readSnapshotRecording(leader, leaderSnapshot.recordingId, stateReader);

        assertEquals(beforeNextSessionId, mutableNextSessionId.get());
        assertEquals(beforeNextServiceSessionId, mutableNextServiceSessionId.get());
        assertEquals(beforeLogServiceSessionId, mutableLogServiceSessionId.get());
        assertArrayEquals(beforeClusterSessionIds, pendingMessageClusterSessionIds.toLongArray());
    }

    @ParameterizedTest
    @CsvSource({
        "-9193372036854775999, -9223372036854775808, -9223372036854775808, -9200000000000000000, LogServiceSessionId",
        "1000000, 1000, 500000, 1000000, LogServiceSessionId",
        "1000000, $compute, 1000001, 1000999, LogServiceSessionId",
        "5000, 200000, 100000, 150000, NextServiceSessionId",
        "$compute, 200000, 1, 199999, NextServiceSessionId",
        "7777, 9999, 1000000000000000, 1223372036854775, MaxClusterSessionId" })
    @SlowTest
    @InterruptAfter(90)
    @SuppressWarnings("MethodLength")
    void executeShouldPatchTheStateOfTheLeaderSnapshot(
        final String baseLogServiceSessionId,
        final String baseNextServiceSessionId,
        final long clusterSessionIdLowerBound,
        final long clusterSessionIdUpperBound,
        final String mode)
    {
        final IntFunction<TestNode.TestService[]> servicesSupplier =
            (i) -> new TestNode.TestService[]
            {
                new TestNode.MessageTrackingService(1, i),
                new TestNode.MessageTrackingService(2, i)
            };
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withTimerServiceSupplier(new PriorityHeapTimerServiceSupplier())
            .withServiceSupplier(servicesSupplier)
            .start();
        systemTestWatcher.cluster(cluster);
        final int serviceCount = cluster.node(0).services().length;

        final TestNode leader = cluster.awaitLeader();
        cluster.connectClient();

        TestNode.MessageTrackingService.delaySessionMessageProcessing(true);
        int messageCount = 0;
        final ExpandableArrayBuffer msgBuffer = cluster.msgBuffer();
        for (int i = 0; i < NUM_MESSAGES; i++)
        {
            msgBuffer.putInt(0, ++messageCount, LITTLE_ENDIAN);
            cluster.pollUntilMessageSent(SIZE_OF_INT);
        }

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);
        TestNode.MessageTrackingService.delaySessionMessageProcessing(false);

        cluster.awaitResponseMessageCount(messageCount * serviceCount);
        awaitServiceMessages(cluster, serviceCount, messageCount);
        stopConsensusModulesAndServices(cluster);

        final File leaderClusterDir = leader.consensusModule().context().clusterDir();
        final RecordingLog.Entry leaderSnapshot = ClusterTool.findLatestValidSnapshot(leaderClusterDir);
        assertNotNull(leaderSnapshot);

        final MutableLong mutableNextSessionId = new MutableLong();
        final MutableLong mutableNextServiceSessionId = new MutableLong();
        final MutableLong mutableLogServiceSessionId = new MutableLong();
        final MutableInteger consensusModuleStateOffset = new MutableInteger();
        final IntArrayList pendingMessageTrackerOffsets = new IntArrayList();
        final IntArrayList[] pendingMessageOffsets = new IntArrayList[serviceCount];
        for (int i = 0; i < serviceCount; i++)
        {
            pendingMessageOffsets[i] = new IntArrayList();
        }
        readSnapshotRecording(
            leader,
            leaderSnapshot.recordingId,
            new NoOpConsensusModuleSnapshotListener()
            {
                public void onLoadConsensusModuleState(
                    final long nextSessionId,
                    final long nextServiceSessionId,
                    final long logServiceSessionId,
                    final int pendingMessageCapacity,
                    final DirectBuffer buffer,
                    final int offset,
                    final int length)
                {
                    mutableNextSessionId.set(nextSessionId);
                    mutableNextServiceSessionId.set(nextServiceSessionId);
                    mutableLogServiceSessionId.set(logServiceSessionId);
                    consensusModuleStateOffset.set(offset);
                    assertEquals(MessageHeaderDecoder.ENCODED_LENGTH + ConsensusModuleDecoder.BLOCK_LENGTH, length);
                }

                public void onLoadPendingMessageTracker(
                    final long nextServiceSessionId,
                    final long logServiceSessionId,
                    final int pendingMessageCapacity,
                    final int serviceId,
                    final DirectBuffer buffer,
                    final int offset,
                    final int length)
                {
                    pendingMessageTrackerOffsets.add(offset);
                    assertEquals(
                        MessageHeaderDecoder.ENCODED_LENGTH + PendingMessageTrackerDecoder.BLOCK_LENGTH,
                        length);
                }

                public void onLoadPendingMessage(
                    final long clusterSessionId, final DirectBuffer buffer, final int offset, final int length)
                {
                    final int serviceId = serviceIdFromLogMessage(clusterSessionId);
                    pendingMessageOffsets[serviceId].addInt(offset);
                    assertEquals(
                        MessageHeaderDecoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.BLOCK_LENGTH + SIZE_OF_INT,
                        length);
                }
            });
        assertNotEquals(0, consensusModuleStateOffset.get());
        assertEquals(serviceCount, pendingMessageTrackerOffsets.size());
        for (int i = 0; i < serviceCount; i++)
        {
            assertNotEquals(0, pendingMessageOffsets[i].size());
        }

        final long expectedNextSessionId = mutableNextSessionId.get();
        assertNotEquals(Long.toString(mutableLogServiceSessionId.get()), baseLogServiceSessionId);
        assertNotEquals(Long.toString(mutableNextServiceSessionId.get()), baseNextServiceSessionId);

        modifySnapshot(
            leader,
            leaderSnapshot,
            consensusModuleStateOffset,
            pendingMessageTrackerOffsets,
            pendingMessageOffsets,
            baseLogServiceSessionId,
            baseNextServiceSessionId,
            clusterSessionIdLowerBound,
            clusterSessionIdUpperBound);

        final ConsensusModuleSnapshotPendingServiceMessagesPatch snapshotPatch =
            new ConsensusModuleSnapshotPendingServiceMessagesPatch(
            leader.archive().context().localControlChannel(),
            leader.archive().context().localControlStreamId());
        assertTrue(snapshotPatch.execute(leaderClusterDir));

        final MutableBoolean hasLoadedConsensusModuleState = new MutableBoolean();
        final MutableInteger[] onLoadPendingMessageCounters = new MutableInteger[serviceCount];
        for (int i = 0; i < serviceCount; i++)
        {
            onLoadPendingMessageCounters[i] = new MutableInteger();
        }
        readSnapshotRecording(
            leader,
            leaderSnapshot.recordingId,
            new NoOpConsensusModuleSnapshotListener()
            {
                final long[] nextClusterSessionIds = new long[serviceCount];

                public void onLoadConsensusModuleState(
                    final long nextSessionId,
                    final long nextServiceSessionId,
                    final long logServiceSessionId,
                    final int pendingMessageCapacity,
                    final DirectBuffer buffer,
                    final int offset,
                    final int length)
                {
                    assertEquals(expectedNextSessionId, nextSessionId);
                    final int numPendingMessages = pendingMessageOffsets[0].size();

                    switch (mode)
                    {
                        case "LogServiceSessionId":
                        {
                            assertEquals(Long.parseLong(baseLogServiceSessionId), logServiceSessionId);
                            assertEquals(logServiceSessionId + 1 + numPendingMessages, nextServiceSessionId);
                            break;
                        }

                        case "NextServiceSessionId":
                        {
                            assertEquals(Long.parseLong(baseNextServiceSessionId), nextServiceSessionId);
                            assertEquals(nextServiceSessionId - 1 - numPendingMessages, logServiceSessionId);
                            break;
                        }

                        case "MaxClusterSessionId":
                        {
                            assertEquals(clusterSessionIdUpperBound - numPendingMessages, logServiceSessionId);
                            assertEquals(clusterSessionIdUpperBound + 1, nextServiceSessionId);
                            break;
                        }

                        default:
                        {
                            fail("unknown mode: " + mode);
                        }
                    }

                    hasLoadedConsensusModuleState.set(true);
                }

                public void onLoadPendingMessageTracker(
                    final long nextServiceSessionId,
                    final long logServiceSessionId,
                    final int pendingMessageCapacity,
                    final int serviceId,
                    final DirectBuffer buffer,
                    final int offset,
                    final int length)
                {
                    nextClusterSessionIds[serviceId] = logServiceSessionId + 1;
                }

                public void onLoadPendingMessage(
                    final long clusterSessionId, final DirectBuffer buffer, final int offset, final int length)
                {
                    final int serviceId = serviceIdFromLogMessage(clusterSessionId);
                    onLoadPendingMessageCounters[serviceId].increment();
                    assertEquals(
                        nextClusterSessionIds[serviceId]++, clusterSessionId, "Invalid pending message header!");
                }
            });
        assertTrue(hasLoadedConsensusModuleState.get());
        for (int i = 0; i < serviceCount; i++)
        {
            assertEquals(pendingMessageOffsets[i].size(), onLoadPendingMessageCounters[i].get());
        }

        for (int i = 0; i < 3; i++)
        {
            if (i != leader.index())
            {
                replicatePatchedSnapshotToFollower(leader, leaderSnapshot.recordingId, cluster.node(i));
            }
        }

        cluster.stopAllNodes();
        cluster.restartAllNodes(false);
        cluster.awaitLeader();
        cluster.connectClient();

        for (int i = 0; i < 10; i++)
        {
            msgBuffer.putInt(0, ++messageCount, LITTLE_ENDIAN);
            cluster.pollUntilMessageSent(SIZE_OF_INT);
        }
        cluster.awaitResponseMessageCount(messageCount * serviceCount);
        awaitServiceMessages(cluster, serviceCount, messageCount);
    }

    private static void awaitServiceMessages(final TestCluster cluster, final int serviceCount, final int messageCount)
    {
        for (int i = 0; i < 3; i++)
        {
            final TestNode node = cluster.node(i);
            final TestNode.TestService[] services = node.services();
            for (final TestNode.TestService service : services)
            {
                // 1 client message + 3 service messages x number of services
                cluster.awaitServiceMessageCount(node, service, messageCount + (messageCount * 3 * serviceCount));
                // 2 timers x number of services
                cluster.awaitTimerEventCount(node, service, messageCount * 2 * serviceCount);
            }
        }
    }

    private static void stopConsensusModulesAndServices(final TestCluster cluster)
    {
        final TestNode leader = cluster.findLeader();
        for (int i = 0; i < 3; i++)
        {
            final TestNode node = cluster.node(i);
            if (null != node && node != leader)
            {
                node.isTerminationExpected(true);
                node.stopServiceContainers();
                node.consensusModule().close();
            }
        }

        leader.isTerminationExpected(true);
        leader.stopServiceContainers();
        leader.consensusModule().close();
    }

    private static void modifySnapshot(
        final TestNode leader,
        final RecordingLog.Entry leaderSnapshot,
        final MutableInteger consensusModuleStateOffset,
        final IntArrayList pendingMessageTrackerOffsets,
        final IntArrayList[] pendingMessageOffsets,
        final String baseLogServiceSessionId,
        final String baseNextServiceSessionId,
        final long clusterSessionIdLowerBound,
        final long clusterSessionIdUpperBound)
    {
        final int numberOfPendingMessages = pendingMessageOffsets[0].size();
        final long logServiceSessionId;
        if ("$compute".equals(baseLogServiceSessionId))
        {
            logServiceSessionId = Long.parseLong(baseNextServiceSessionId) - 1 - numberOfPendingMessages;
        }
        else
        {
            logServiceSessionId = Long.parseLong(baseLogServiceSessionId);
        }

        final long nextServiceSessionId;
        if ("$compute".equals(baseNextServiceSessionId))
        {
            nextServiceSessionId = Long.parseLong(baseLogServiceSessionId) + 1 + numberOfPendingMessages;
        }
        else
        {
            nextServiceSessionId = Long.parseLong(baseNextServiceSessionId);
        }

        final ArrayList<File> segmentFiles = listSegmentFiles(leader, leaderSnapshot.recordingId);
        assertEquals(1, segmentFiles.size());
        final MappedByteBuffer mappedByteBuffer = IoUtil.mapExistingFile(segmentFiles.get(0), "snapshot file");
        try
        {
            final UnsafeBuffer snapshotBuffer = new UnsafeBuffer(mappedByteBuffer);
            final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
            final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();

            // Set the ConsensusModuleState values
            final ConsensusModuleEncoder consensusModuleEncoder = new ConsensusModuleEncoder();
            consensusModuleEncoder
                .wrapAndApplyHeader(snapshotBuffer, consensusModuleStateOffset.get(), messageHeaderEncoder)
                .logServiceSessionId(logServiceSessionId)
                .nextServiceSessionId(nextServiceSessionId);

            final PendingMessageTrackerEncoder pendingMessageTrackerEncoder = new PendingMessageTrackerEncoder();
            final int serviceCount = pendingMessageTrackerOffsets.size();
            for (int i = 0; i < serviceCount; i++)
            {
                final int offset = pendingMessageTrackerOffsets.get(i);
                pendingMessageTrackerEncoder
                    .wrapAndApplyHeader(snapshotBuffer, offset, messageHeaderEncoder)
                    .logServiceSessionId(serviceSessionId(i, logServiceSessionId))
                    .nextServiceSessionId(serviceSessionId(i, nextServiceSessionId));
            }

            // Now randomize clusterSessionId of every pending service message
            final SessionMessageHeaderEncoder sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();
            final SessionMessageHeaderDecoder sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
            final MutableInteger count = new MutableInteger();
            for (int i = 0; i < serviceCount; i++)
            {
                final IntArrayList offsets = pendingMessageOffsets[i];
                offsets.forEachInt(
                    (offset) ->
                    {
                        final long clusterSessionId = switch (count.getAndIncrement())
                        {
                            case 0 -> clusterSessionIdLowerBound;
                            case 1 -> clusterSessionIdUpperBound;
                            default -> ThreadLocalRandom.current().nextLong(
                            clusterSessionIdLowerBound + 1, clusterSessionIdUpperBound);
                        };

                        final long originalClusterSessionId = sessionMessageHeaderDecoder
                            .wrapAndApplyHeader(snapshotBuffer, offset, messageHeaderDecoder)
                            .clusterSessionId();
                        final int serviceId = serviceIdFromLogMessage(originalClusterSessionId);
                        final long newClusterSessionId = serviceSessionId(serviceId, clusterSessionId);

                        sessionMessageHeaderEncoder
                            .wrapAndApplyHeader(snapshotBuffer, offset, messageHeaderEncoder)
                            .clusterSessionId(newClusterSessionId);
                    });
            }
        }
        finally
        {
            IoUtil.unmap(mappedByteBuffer);
        }
    }

    private static ArrayList<File> listSegmentFiles(final TestNode node, final long recordingId)
    {
        final File[] files = node.archive().context().archiveDir().listFiles();
        assertNotNull(files);
        final String segmentFileNamePrefix = recordingId + "-";
        final ArrayList<File> segmentFiles = new ArrayList<>();
        for (final File file : files)
        {
            if (null != file)
            {
                final String fileName = file.getName();
                if (fileName.startsWith(segmentFileNamePrefix) && fileName.endsWith(".rec"))
                {
                    segmentFiles.add(file);
                }
            }
        }

        return segmentFiles;
    }

    private static void readSnapshotRecording(
        final TestNode node,
        final long recordingId,
        final ConsensusModuleSnapshotListener snapshotListener)
    {
        try (
            Aeron aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(node.mediaDriver().aeronDirectoryName()));
            AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                .controlRequestChannel(node.archive().context().localControlChannel())
                .controlRequestStreamId(node.archive().context().localControlStreamId())
                .controlResponseChannel(IPC_CHANNEL)
                .aeron(aeron)))
        {
            replayLocalSnapshotRecording(
                aeron,
                archive,
                recordingId,
                snapshotListener);
        }
    }

    private static void replicatePatchedSnapshotToFollower(
        final TestNode leader, final long snapshotRecordingId, final TestNode follower)
    {
        final File clusterDir = follower.consensusModule().context().clusterDir();
        final RecordingLog.Entry followerSnapshot = ClusterTool.findLatestValidSnapshot(clusterDir);
        assertNotNull(followerSnapshot, "follower without a snapshot node");

        final String aeronDirectoryName = follower.mediaDriver().aeronDirectoryName();
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName));
            AeronArchive aeronArchive = AeronArchive.connect(new AeronArchive.Context()
                .controlRequestChannel(follower.archive().context().localControlChannel())
                .controlRequestStreamId(follower.archive().context().localControlStreamId())
                .controlResponseChannel(IPC_CHANNEL)
                .aeron(aeron)
                .recordingSignalConsumer(new RecordingSignalCapture())))
        {
            final Archive.Context leaderArchiveContext = leader.archive().context();
            final RecordingReplicator recordingReplicator = new RecordingReplicator(
                aeronArchive,
                snapshotRecordingId,
                followerSnapshot.recordingId,
                leaderArchiveContext.controlChannel(),
                leaderArchiveContext.controlStreamId(),
                null);

            assertEquals(followerSnapshot.recordingId, recordingReplicator.replicate());
        }
    }

    private static class NoOpConsensusModuleSnapshotListener implements ConsensusModuleSnapshotListener
    {
        public void onLoadBeginSnapshot(
            final int appVersion,
            final TimeUnit timeUnit,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        public void onLoadConsensusModuleState(
            final long nextSessionId,
            final long nextServiceSessionId,
            final long logServiceSessionId,
            final int pendingMessageCapacity,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        public void onLoadPendingMessage(
            final long clusterSessionId, final DirectBuffer buffer, final int offset, final int length)
        {
        }

        public void onLoadClusterSession(
            final long clusterSessionId,
            final long correlationId,
            final long openedLogPosition,
            final long timeOfLastActivity,
            final CloseReason closeReason,
            final int responseStreamId,
            final String responseChannel,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        public void onLoadTimer(
            final long correlationId,
            final long deadline,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        public void onLoadPendingMessageTracker(
            final long nextServiceSessionId,
            final long logServiceSessionId,
            final int pendingMessageCapacity,
            final int serviceId,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        public void onLoadEndSnapshot(final DirectBuffer buffer, final int offset, final int length)
        {
        }
    }
}
