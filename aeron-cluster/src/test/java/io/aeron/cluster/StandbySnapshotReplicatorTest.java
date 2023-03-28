/*
 * Copyright 2014-2023 Real Logic Limited.
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
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.RecordingSignal;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StandbySnapshotReplicatorTest
{
    private final String endpoint0 = "host0:10001";
    private final String endpoint1 = "host1:10101";
    private final String archiveControlChannel = "aeron:udp?endpoint=invalid:6666";
    private final int archiveControlStreamId = 98734;
    private final String replicationChannel = "aeron:udp?endpoint=host0:0";
    private final AeronArchive.Context ctx = new AeronArchive.Context();

    private final AeronArchive mockArchive = mock(AeronArchive.class);
    private final MultipleRecordingReplication mockMultipleRecordingReplication = mock(
        MultipleRecordingReplication.class);

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
        final long progressTimeoutNs = TimeUnit.SECONDS.toNanos(10);
        final long intervalTimeoutNs = TimeUnit.SECONDS.toNanos(1);
        final int serviceCount = 1;
        final Long2LongHashMap dstRecordingIds = new Long2LongHashMap(Aeron.NULL_VALUE);
        dstRecordingIds.put(1, 11);
        dstRecordingIds.put(2, 12);

        when(mockMultipleRecordingReplication.completedDstRecordingId(anyLong())).thenAnswer(
            (invocation) -> dstRecordingIds.get(invocation.<Long>getArgument(0)));

        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true))
        {
            recordingLog.appendSnapshot(1, 0, 0, logPositionOldest, 1_000_000_000L, SERVICE_ID);
            recordingLog.appendSnapshot(2, 0, 0, logPositionOldest, 1_000_000_000L, 0);

            recordingLog.appendRemoteSnapshot(1, 0, 0, logPositionNewest, 1_000_000_000L, SERVICE_ID, endpoint0);
            recordingLog.appendRemoteSnapshot(2, 0, 0, logPositionNewest, 1_000_000_000L, 0, endpoint0);

            recordingLog.appendRemoteSnapshot(1, 0, 0, 2000L, 1_000_000_000L, SERVICE_ID, endpoint1);
            recordingLog.appendRemoteSnapshot(2, 0, 0, 2000L, 1_000_000_000L, 0, endpoint1);

            final long nowNs = 2_000_000_000L;

            try (MockedStatic<MultipleRecordingReplication> staticMockReplication = mockStatic(
                MultipleRecordingReplication.class);
                MockedStatic<AeronArchive> staticMockArchive = mockStatic(AeronArchive.class))
            {
                staticMockReplication
                    .when(() -> MultipleRecordingReplication.newInstance(
                        any(), anyInt(), any(), any(), anyLong(), anyLong()))
                    .thenReturn(mockMultipleRecordingReplication);
                staticMockArchive.when(() -> AeronArchive.connect(any())).thenReturn(mockArchive);

                final StandbySnapshotReplicator standbySnapshotReplicator = StandbySnapshotReplicator.newInstance(
                    ctx,
                    recordingLog,
                    serviceCount,
                    archiveControlChannel,
                    archiveControlStreamId,
                    replicationChannel);

                when(mockMultipleRecordingReplication.isComplete()).thenReturn(true);

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

                verify(mockMultipleRecordingReplication).addRecording(1L, NULL_RECORDING_ID, NULL_POSITION);
                verify(mockMultipleRecordingReplication).addRecording(2L, NULL_RECORDING_ID, NULL_POSITION);
                verify(mockMultipleRecordingReplication).poll(nowNs);
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
            recordingLog.appendRemoteSnapshot(1, 0, 0, 1000, 1_000_000_000L, SERVICE_ID, endpoint0);
            recordingLog.appendRemoteSnapshot(2, 0, 0, 1000, 1_000_000_000L, 0, endpoint0);

            staticMockReplication
                .when(() -> MultipleRecordingReplication.newInstance(
                    any(), anyInt(), any(), any(), anyLong(), anyLong()))
                .thenReturn(mockMultipleRecordingReplication);
            staticMockArchive.when(() -> AeronArchive.connect(any())).thenReturn(mockArchive);

            final StandbySnapshotReplicator standbySnapshotReplicator = StandbySnapshotReplicator.newInstance(
                ctx,
                recordingLog,
                1,
                archiveControlChannel,
                archiveControlStreamId,
                replicationChannel);

            standbySnapshotReplicator.poll(0);
            verify(mockArchive).pollForRecordingSignals();
            standbySnapshotReplicator.onSignal(2, 11, 23, 29, 37, RecordingSignal.START);

            verify(mockMultipleRecordingReplication).onSignal(11, 23, 37, RecordingSignal.START);
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
                .thenReturn(mockMultipleRecordingReplication);
            staticMockArchive.when(() -> AeronArchive.connect(any())).thenReturn(mockArchive);

            final StandbySnapshotReplicator standbySnapshotReplicator = StandbySnapshotReplicator.newInstance(
                ctx,
                recordingLog,
                1,
                archiveControlChannel,
                archiveControlStreamId,
                replicationChannel);

            standbySnapshotReplicator.poll(0);
            assertTrue(standbySnapshotReplicator.isComplete());
        }
    }
}