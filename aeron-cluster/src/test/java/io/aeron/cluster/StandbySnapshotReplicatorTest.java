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
import io.aeron.Counter;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.cluster.client.ClusterException;
import org.agrona.collections.Long2LongHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.status.RecordingPos.NULL_RECORDING_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.SERVICE_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class StandbySnapshotReplicatorTest
{
    private final String endpoint0 = "host0:10001";
    private final String endpoint1 = "host1:10101";
    private final String archiveControlChannel = "aeron:udp?endpoint=invalid:6666";
    private final int archiveControlStreamId = 98734;
    private final String replicationChannel = "aeron:udp?endpoint=host0:0";
    private final AeronArchive.Context ctx = new AeronArchive.Context();
    private final int memberId = 12;
    private final int fileSyncLevel = 1;

    private final AeronArchive mockArchive = mock(AeronArchive.class);
    private final MultipleRecordingReplication mockMultipleRecordingReplication0 = mock(
        MultipleRecordingReplication.class, "host0");
    private final MultipleRecordingReplication mockMultipleRecordingReplication1 = mock(
        MultipleRecordingReplication.class, "host1");
    private final Counter snapshotCounter = mock(Counter.class, "snapshotCounter");

    @BeforeEach
    void setUp()
    {
        when(mockArchive.context()).thenReturn(ctx);
    }

    @TempDir
    private File clusterDir;

    @Test
    void shouldReplicateStandbySnapshots()
    {
        final long logPositionOldest = 1000L;
        final long logPositionNewest = 3000L;
        final long progressTimeoutNs = TimeUnit.SECONDS.toNanos(20);
        final long intervalTimeoutNs = TimeUnit.SECONDS.toNanos(2);
        final int serviceCount = 1;
        final Long2LongHashMap dstRecordingIds = new Long2LongHashMap(Aeron.NULL_VALUE);
        dstRecordingIds.put(1, 11);
        dstRecordingIds.put(2, 12);

        when(mockMultipleRecordingReplication0.completedDstRecordingId(anyLong())).thenAnswer(
            (invocation) -> dstRecordingIds.get(invocation.<Long>getArgument(0)));

        try (RecordingLog recordingLog = spy(new RecordingLog(clusterDir, true)))
        {
            recordingLog.appendSnapshot(1, 0, 0, logPositionOldest, 1_000_000_000L, SERVICE_ID);
            recordingLog.appendSnapshot(2, 0, 0, logPositionOldest, 1_000_000_000L, 0);

            recordingLog.appendStandbySnapshot(1, 0, 0, logPositionNewest, 1_000_000_000L, SERVICE_ID, endpoint0);
            recordingLog.appendStandbySnapshot(2, 0, 0, logPositionNewest, 1_000_000_000L, 0, endpoint0);

            recordingLog.appendStandbySnapshot(1, 0, 0, 2000L, 1_000_000_000L, SERVICE_ID, endpoint1);
            recordingLog.appendStandbySnapshot(2, 0, 0, 2000L, 1_000_000_000L, 0, endpoint1);

            final long nowNs = 2_000_000_000L;

            try (MockedStatic<MultipleRecordingReplication> staticMockReplication = mockStatic(
                MultipleRecordingReplication.class);
                MockedStatic<AeronArchive> staticMockArchive = mockStatic(AeronArchive.class))
            {
                staticMockReplication
                    .when(() -> MultipleRecordingReplication.newInstance(
                        any(), anyInt(), any(), any(), anyLong(), anyLong()))
                    .thenReturn(mockMultipleRecordingReplication0);
                staticMockArchive.when(() -> AeronArchive.connect(any())).thenReturn(mockArchive);

                final StandbySnapshotReplicator standbySnapshotReplicator = StandbySnapshotReplicator.newInstance(
                    memberId,
                    ctx,
                    recordingLog,
                    serviceCount,
                    archiveControlChannel,
                    archiveControlStreamId,
                    replicationChannel,
                    fileSyncLevel,
                    snapshotCounter);

                when(mockMultipleRecordingReplication0.isComplete()).thenReturn(true);

                assertNotEquals(0, standbySnapshotReplicator.poll(nowNs));
                assertTrue(standbySnapshotReplicator.isComplete());

                verify(mockArchive).pollForRecordingSignals();

                staticMockReplication.verify(() -> MultipleRecordingReplication.newInstance(
                    eq(mockArchive),
                    eq(archiveControlStreamId),
                    contains(endpoint0),
                    eq(replicationChannel),
                    eq(progressTimeoutNs),
                    eq(intervalTimeoutNs)));

                verify(mockMultipleRecordingReplication0).addRecording(1L, NULL_RECORDING_ID, NULL_POSITION);
                verify(mockMultipleRecordingReplication0).addRecording(2L, NULL_RECORDING_ID, NULL_POSITION);
                verify(mockMultipleRecordingReplication0).poll(nowNs);
                verify(recordingLog).force(fileSyncLevel);
                verify(snapshotCounter).incrementRelease();
            }
        }

        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true))
        {
            for (int serviceId = -1; serviceId < serviceCount; serviceId++)
            {
                final RecordingLog.Entry latestSnapshot = recordingLog.getLatestSnapshot(serviceId);

                assertNotNull(latestSnapshot);
                assertEquals(RecordingLog.ENTRY_TYPE_SNAPSHOT, latestSnapshot.type);
                assertEquals(logPositionNewest, latestSnapshot.logPosition);
                assertEquals(12 + serviceId, latestSnapshot.recordingId);
            }
        }
    }

    @Test
    void shouldPassSignalsToRecordingReplication()
    {
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true);
            MockedStatic<MultipleRecordingReplication> staticMockReplication = mockStatic(
                MultipleRecordingReplication.class);
            MockedStatic<AeronArchive> staticMockArchive = mockStatic(AeronArchive.class))
        {
            recordingLog.appendStandbySnapshot(1, 0, 0, 1000, 1_000_000_000L, SERVICE_ID, endpoint0);
            recordingLog.appendStandbySnapshot(2, 0, 0, 1000, 1_000_000_000L, 0, endpoint0);

            staticMockReplication
                .when(() -> MultipleRecordingReplication.newInstance(
                    any(), anyInt(), any(), any(), anyLong(), anyLong()))
                .thenReturn(mockMultipleRecordingReplication0);
            staticMockArchive.when(() -> AeronArchive.connect(any())).thenReturn(mockArchive);

            final StandbySnapshotReplicator standbySnapshotReplicator = StandbySnapshotReplicator.newInstance(
                memberId,
                ctx,
                recordingLog,
                1,
                archiveControlChannel,
                archiveControlStreamId,
                replicationChannel,
                fileSyncLevel,
                snapshotCounter);

            standbySnapshotReplicator.poll(0);
            verify(mockArchive).pollForRecordingSignals();
            standbySnapshotReplicator.onSignal(2, 11, 23, 29, 37, RecordingSignal.START);

            verify(mockMultipleRecordingReplication0).onSignal(11, 23, 37, RecordingSignal.START);
            verifyNoInteractions(snapshotCounter);
        }
    }

    @Test
    void shouldHandleNoStandbySnapshots()
    {
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true);
            MockedStatic<MultipleRecordingReplication> staticMockReplication = mockStatic(
                MultipleRecordingReplication.class);
            MockedStatic<AeronArchive> staticMockArchive = mockStatic(AeronArchive.class))
        {
            staticMockReplication
                .when(() -> MultipleRecordingReplication.newInstance(
                    any(), anyInt(), any(), any(), anyLong(), anyLong()))
                .thenReturn(mockMultipleRecordingReplication0);
            staticMockArchive.when(() -> AeronArchive.connect(any())).thenReturn(mockArchive);

            final StandbySnapshotReplicator standbySnapshotReplicator = StandbySnapshotReplicator.newInstance(
                memberId,
                ctx,
                recordingLog,
                1,
                archiveControlChannel,
                archiveControlStreamId,
                replicationChannel,
                fileSyncLevel, snapshotCounter);

            standbySnapshotReplicator.poll(0);
            assertTrue(standbySnapshotReplicator.isComplete());
            verifyNoInteractions(snapshotCounter);
        }
    }

    @Test
    void shouldSwitchEndpointsOnMultipleReplicationException()
    {
        final long standbySnapshotLogPosition = 10_000L;
        final long localSnapshotLogPosition = 2_000L;
        final long nowNs = 1_000_000_000L;

        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true))
        {
            recordingLog.appendSnapshot(1, 0, 0, localSnapshotLogPosition, 1_000_000_000L, SERVICE_ID);
            recordingLog.appendSnapshot(2, 0, 0, localSnapshotLogPosition, 1_000_000_000L, 0);

            recordingLog.appendStandbySnapshot(
                1, 0, 0, standbySnapshotLogPosition, 1_000_000_000L, SERVICE_ID, endpoint0);
            recordingLog.appendStandbySnapshot(2, 0, 0, standbySnapshotLogPosition, 1_000_000_000L, 0, endpoint0);

            recordingLog.appendStandbySnapshot(
                1, 0, 0, standbySnapshotLogPosition, 1_000_000_000L, SERVICE_ID, endpoint1);
            recordingLog.appendStandbySnapshot(2, 0, 0, standbySnapshotLogPosition, 1_000_000_000L, 0, endpoint1);
        }

        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true);
            MockedStatic<MultipleRecordingReplication> staticMockReplication = mockStatic(
                MultipleRecordingReplication.class);
            MockedStatic<AeronArchive> staticMockArchive = mockStatic(AeronArchive.class))
        {
            staticMockReplication
                .when(() -> MultipleRecordingReplication.newInstance(
                    any(), anyInt(), contains("host0"), any(), anyLong(), anyLong()))
                .thenReturn(mockMultipleRecordingReplication0);

            staticMockReplication
                .when(() -> MultipleRecordingReplication.newInstance(
                    any(), anyInt(), contains("host1"), any(), anyLong(), anyLong()))
                .thenReturn(mockMultipleRecordingReplication1);

            staticMockArchive.when(() -> AeronArchive.connect(any())).thenReturn(mockArchive);

            final StandbySnapshotReplicator standbySnapshotReplicator = StandbySnapshotReplicator.newInstance(
                memberId,
                ctx,
                recordingLog,
                1,
                archiveControlChannel,
                archiveControlStreamId,
                replicationChannel,
                fileSyncLevel,
                snapshotCounter);

            when(mockMultipleRecordingReplication0.poll(anyLong())).thenThrow(new ClusterException("fail"));
            when(mockMultipleRecordingReplication1.isComplete()).thenReturn(true);

            standbySnapshotReplicator.poll(nowNs);
            standbySnapshotReplicator.poll(nowNs);

            assertTrue(standbySnapshotReplicator.isComplete());

            verify(mockMultipleRecordingReplication0).poll(anyLong());
            verify(mockMultipleRecordingReplication1).poll(anyLong());
            verify(snapshotCounter).incrementRelease();
        }
    }

    @Test
    void shouldSwitchEndpointsOnArchivePollForSignalsException()
    {
        final long standbySnapshotLogPosition = 10_000L;
        final long localSnapshotLogPosition = 2_000L;
        final long nowNs = 1_000_000_000L;

        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true))
        {
            recordingLog.appendSnapshot(1, 0, 0, localSnapshotLogPosition, 1_000_000_000L, SERVICE_ID);
            recordingLog.appendSnapshot(2, 0, 0, localSnapshotLogPosition, 1_000_000_000L, 0);

            recordingLog.appendStandbySnapshot(
                1, 0, 0, standbySnapshotLogPosition, 1_000_000_000L, SERVICE_ID, endpoint0);
            recordingLog.appendStandbySnapshot(2, 0, 0, standbySnapshotLogPosition, 1_000_000_000L, 0, endpoint0);

            recordingLog.appendStandbySnapshot(
                1, 0, 0, standbySnapshotLogPosition, 1_000_000_000L, SERVICE_ID, endpoint1);
            recordingLog.appendStandbySnapshot(2, 0, 0, standbySnapshotLogPosition, 1_000_000_000L, 0, endpoint1);
        }

        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true);
            MockedStatic<MultipleRecordingReplication> staticMockReplication = mockStatic(
                MultipleRecordingReplication.class);
            MockedStatic<AeronArchive> staticMockArchive = mockStatic(AeronArchive.class))
        {
            staticMockReplication
                .when(() -> MultipleRecordingReplication.newInstance(
                    any(), anyInt(), contains("host0"), any(), anyLong(), anyLong()))
                .thenReturn(mockMultipleRecordingReplication0);

            staticMockReplication
                .when(() -> MultipleRecordingReplication.newInstance(
                    any(), anyInt(), contains("host1"), any(), anyLong(), anyLong()))
                .thenReturn(mockMultipleRecordingReplication1);

            staticMockArchive.when(() -> AeronArchive.connect(any())).thenReturn(mockArchive);
            when(mockArchive.pollForRecordingSignals()).thenThrow(new ArchiveException("fail")).thenReturn(1);

            final StandbySnapshotReplicator standbySnapshotReplicator = StandbySnapshotReplicator.newInstance(
                memberId,
                ctx,
                recordingLog,
                1,
                archiveControlChannel,
                archiveControlStreamId,
                replicationChannel,
                fileSyncLevel,
                snapshotCounter);

            when(mockMultipleRecordingReplication1.isComplete()).thenReturn(true);

            standbySnapshotReplicator.poll(nowNs);
            standbySnapshotReplicator.poll(nowNs);

            assertTrue(standbySnapshotReplicator.isComplete());

            verify(mockMultipleRecordingReplication0).poll(anyLong());
            verify(mockMultipleRecordingReplication1).poll(anyLong());
            verify(snapshotCounter).incrementRelease();
        }
    }

    @Test
    void shouldThrowExceptionIfUnableToReplicateAnySnapshotsDueToClusterExceptions()
    {
        final long standbySnapshotLogPosition = 10_000L;
        final long localSnapshotLogPosition = 2_000L;
        final long nowNs = 1_000_000_000L;

        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true))
        {
            recordingLog.appendSnapshot(1, 0, 0, localSnapshotLogPosition, 1_000_000_000L, SERVICE_ID);
            recordingLog.appendSnapshot(2, 0, 0, localSnapshotLogPosition, 1_000_000_000L, 0);

            recordingLog.appendStandbySnapshot(
                1, 0, 0, standbySnapshotLogPosition, 1_000_000_000L, SERVICE_ID, endpoint0);
            recordingLog.appendStandbySnapshot(2, 0, 0, standbySnapshotLogPosition, 1_000_000_000L, 0, endpoint0);

            recordingLog.appendStandbySnapshot(
                1, 0, 0, standbySnapshotLogPosition, 1_000_000_000L, SERVICE_ID, endpoint1);
            recordingLog.appendStandbySnapshot(2, 0, 0, standbySnapshotLogPosition, 1_000_000_000L, 0, endpoint1);
        }

        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true);
            MockedStatic<MultipleRecordingReplication> staticMockReplication = mockStatic(
                MultipleRecordingReplication.class);
            MockedStatic<AeronArchive> staticMockArchive = mockStatic(AeronArchive.class))
        {
            staticMockReplication
                .when(() -> MultipleRecordingReplication.newInstance(
                    any(), anyInt(), contains("host0"), any(), anyLong(), anyLong()))
                .thenReturn(mockMultipleRecordingReplication0);

            staticMockReplication
                .when(() -> MultipleRecordingReplication.newInstance(
                    any(), anyInt(), contains("host1"), any(), anyLong(), anyLong()))
                .thenReturn(mockMultipleRecordingReplication1);

            staticMockArchive.when(() -> AeronArchive.connect(any())).thenReturn(mockArchive);

            when(mockMultipleRecordingReplication0.poll(anyLong())).thenThrow(new ClusterException("fail"));
            when(mockMultipleRecordingReplication1.poll(anyLong())).thenThrow(new ClusterException("fail"));

            final StandbySnapshotReplicator standbySnapshotReplicator = StandbySnapshotReplicator.newInstance(
                memberId,
                ctx,
                recordingLog,
                1,
                archiveControlChannel,
                archiveControlStreamId,
                replicationChannel,
                fileSyncLevel,
                snapshotCounter);

            standbySnapshotReplicator.poll(nowNs);
            standbySnapshotReplicator.poll(nowNs);
            assertThrows(ClusterException.class, () -> standbySnapshotReplicator.poll(nowNs));

            verify(mockMultipleRecordingReplication0).poll(anyLong());
            verify(mockMultipleRecordingReplication1).poll(anyLong());
            verifyNoInteractions(snapshotCounter);
        }
    }

    @Test
    void shouldThrowExceptionIfUnableToReplicateAnySnapshotsDueToArchiveExceptions()
    {
        final long standbySnapshotLogPosition = 10_000L;
        final long localSnapshotLogPosition = 2_000L;
        final long nowNs = 1_000_000_000L;

        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true))
        {
            recordingLog.appendSnapshot(1, 0, 0, localSnapshotLogPosition, 1_000_000_000L, SERVICE_ID);
            recordingLog.appendSnapshot(2, 0, 0, localSnapshotLogPosition, 1_000_000_000L, 0);

            recordingLog.appendStandbySnapshot(
                1, 0, 0, standbySnapshotLogPosition, 1_000_000_000L, SERVICE_ID, endpoint0);
            recordingLog.appendStandbySnapshot(2, 0, 0, standbySnapshotLogPosition, 1_000_000_000L, 0, endpoint0);

            recordingLog.appendStandbySnapshot(
                1, 0, 0, standbySnapshotLogPosition, 1_000_000_000L, SERVICE_ID, endpoint1);
            recordingLog.appendStandbySnapshot(2, 0, 0, standbySnapshotLogPosition, 1_000_000_000L, 0, endpoint1);
        }

        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true);
            MockedStatic<MultipleRecordingReplication> staticMockReplication = mockStatic(
                MultipleRecordingReplication.class);
            MockedStatic<AeronArchive> staticMockArchive = mockStatic(AeronArchive.class))
        {
            staticMockReplication
                .when(() -> MultipleRecordingReplication.newInstance(
                    any(), anyInt(), contains("host0"), any(), anyLong(), anyLong()))
                .thenReturn(mockMultipleRecordingReplication0);

            staticMockReplication
                .when(() -> MultipleRecordingReplication.newInstance(
                    any(), anyInt(), contains("host1"), any(), anyLong(), anyLong()))
                .thenReturn(mockMultipleRecordingReplication1);

            staticMockArchive.when(() -> AeronArchive.connect(any())).thenReturn(mockArchive);
            when(mockArchive.pollForRecordingSignals()).thenThrow(new ArchiveException("fail"));

            final StandbySnapshotReplicator standbySnapshotReplicator = StandbySnapshotReplicator.newInstance(
                memberId,
                ctx,
                recordingLog,
                1,
                archiveControlChannel,
                archiveControlStreamId,
                replicationChannel,
                fileSyncLevel,
                snapshotCounter);

            standbySnapshotReplicator.poll(nowNs);
            standbySnapshotReplicator.poll(nowNs);
            assertThrows(ClusterException.class, () -> standbySnapshotReplicator.poll(nowNs));

            verify(mockMultipleRecordingReplication0).poll(anyLong());
            verify(mockMultipleRecordingReplication1).poll(anyLong());
            verifyNoInteractions(snapshotCounter);
        }
    }

    @Test
    void shouldNotRetrieveSnapshotsIfRecordingLogAlreadyHasUpToDateCopies()
    {
        final long logPosition = 1001234;
        final long nowNs = 1_000_000_000L;

        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true))
        {
            recordingLog.appendSnapshot(1, 0, 0, logPosition, 1_000_000_000L, SERVICE_ID);
            recordingLog.appendSnapshot(2, 0, 0, logPosition, 1_000_000_000L, 0);

            recordingLog.appendStandbySnapshot(1, 0, 0, logPosition, 1_000_000_000L, SERVICE_ID, endpoint0);
            recordingLog.appendStandbySnapshot(2, 0, 0, logPosition, 1_000_000_000L, 0, endpoint0);
        }

        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true);
            MockedStatic<MultipleRecordingReplication> staticMockReplication = mockStatic(
                MultipleRecordingReplication.class);
            MockedStatic<AeronArchive> staticMockArchive = mockStatic(AeronArchive.class))
        {
            staticMockReplication
                .when(() -> MultipleRecordingReplication.newInstance(
                    any(), anyInt(), any(), any(), anyLong(), anyLong()))
                .thenReturn(mockMultipleRecordingReplication0);

            staticMockArchive.when(() -> AeronArchive.connect(any())).thenReturn(mockArchive);

            final StandbySnapshotReplicator standbySnapshotReplicator = StandbySnapshotReplicator.newInstance(
                memberId,
                ctx,
                recordingLog,
                1,
                archiveControlChannel,
                archiveControlStreamId,
                replicationChannel,
                fileSyncLevel,
                snapshotCounter);

            standbySnapshotReplicator.poll(nowNs);
            standbySnapshotReplicator.poll(nowNs);

            verify(mockMultipleRecordingReplication0, never()).poll(anyLong());
            verifyNoInteractions(snapshotCounter);
        }
    }
}