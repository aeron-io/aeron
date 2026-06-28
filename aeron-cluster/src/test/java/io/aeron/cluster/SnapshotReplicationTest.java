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
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ReplicationParams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static io.aeron.archive.codecs.RecordingSignal.REPLICATE_END;
import static io.aeron.archive.codecs.RecordingSignal.SYNC;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SnapshotReplicationTest
{
    private final AeronArchive archive = mock(AeronArchive.class);
    private final String srcChannel = "aeron:udp?endpoint=coming_from:8888";
    private final String replicationChannel = "aeron:udp?endpoint=going_to:8888";
    private final int srcStreamId = 892374;
    private final long nowNs = 1_000_000_000;
    private final long correlationId = 987237452342L;

    @BeforeEach
    void setUp()
    {
        final AeronArchive.Context mockContext = mock(AeronArchive.Context.class);
        final Aeron mockAeron = mock(Aeron.class);
        when(archive.context()).thenReturn(mockContext);
        when(mockContext.aeron()).thenReturn(mockAeron);
        when(mockAeron.nextCorrelationId()).thenReturn(correlationId);
    }

    @Test
    void shouldReplicateTwoSnapshots()
    {
        final long replicationId0 = 89374;
        final long newRecordingId0 = 9823754293L;
        final long replicationId1 = 89375;
        final long newRecordingId1 = 7635445643L;

        final List<RecordingLog.Snapshot> snapshots = Arrays.asList(
            new RecordingLog.Snapshot(2, 3, 5, 7, 11, 0),
            new RecordingLog.Snapshot(17, 3, 5, 7, 31, -1));

        when(archive.replicate(eq(snapshots.get(0).recordingId()), anyInt(), any(), any()))
            .thenReturn(replicationId0);
        when(archive.replicate(eq(snapshots.get(1).recordingId()), anyInt(), any(), any()))
            .thenReturn(replicationId1);

        final SnapshotReplication snapshotReplication = new SnapshotReplication(
            archive, srcStreamId, srcChannel, replicationChannel);

        snapshotReplication.addSnapshot(snapshots.get(0));
        snapshotReplication.addSnapshot(snapshots.get(1));

        assertEquals(1, snapshotReplication.poll(nowNs));

        final ReplicationParams replicationParams = new ReplicationParams()
            .replicationChannel(replicationChannel)
            .replicationSessionId((int)correlationId);

        verify(archive).replicate(
            snapshots.get(0).recordingId(),
            srcStreamId,
            srcChannel,
            replicationParams);
        assertEquals(replicationId0, snapshotReplication.currentReplicationId());
        assertEquals(snapshots.get(0).recordingId(), snapshotReplication.currentSnapshot().recordingId());

        ignoreArchiveContextLookup();

        snapshotReplication.poll(nowNs);
        verifyNoMoreInteractions(archive);

        snapshotReplication.onSignal(replicationId0, newRecordingId0, 23423, SYNC);
        snapshotReplication.poll(nowNs);
        verifyNoMoreInteractions(archive);

        snapshotReplication.onSignal(replicationId0, newRecordingId0, 23423, REPLICATE_END);
        snapshotReplication.poll(nowNs);
        verifyNoMoreInteractions(archive);

        snapshotReplication.poll(nowNs);

        verify(archive).replicate(
            snapshots.get(1).recordingId(),
            srcStreamId,
            srcChannel,
            replicationParams);
        assertEquals(replicationId1, snapshotReplication.currentReplicationId());
        assertEquals(snapshots.get(1).recordingId(), snapshotReplication.currentSnapshot().recordingId());

        ignoreArchiveContextLookup();

        snapshotReplication.poll(nowNs);
        verifyNoMoreInteractions(archive);

        snapshotReplication.onSignal(replicationId1, newRecordingId1, 23423, SYNC);
        snapshotReplication.poll(nowNs);
        verifyNoMoreInteractions(archive);

        snapshotReplication.onSignal(replicationId1, newRecordingId1, 23423, REPLICATE_END);
        snapshotReplication.poll(nowNs);
        verifyNoMoreInteractions(archive);

        assertTrue(snapshotReplication.isComplete());
        assertEquals(0, snapshotReplication.snapshotsRetrieved().get(0).serviceId());
        assertEquals(newRecordingId0, snapshotReplication.snapshotsRetrieved().get(0).recordingId());
        assertEquals(-1, snapshotReplication.snapshotsRetrieved().get(1).serviceId());
        assertEquals(newRecordingId1, snapshotReplication.snapshotsRetrieved().get(1).recordingId());

        snapshotReplication.poll(nowNs);
        verifyNoMoreInteractions(archive);
    }

    @Test
    void shouldNotBeCompleteIfNotSynced()
    {
        final long replicationId0 = 89374;
        final long replicationId1 = 89375;
        final List<RecordingLog.Snapshot> snapshots = Arrays.asList(
            new RecordingLog.Snapshot(2, 3, 5, 7, 11, 13),
            new RecordingLog.Snapshot(17, 19, 23, 29, 31, 37));

        when(archive.replicate(eq(snapshots.get(0).recordingId()), anyLong(), anyLong(), anyInt(), any(), any(), any()))
            .thenReturn(replicationId0);
        when(archive.replicate(eq(snapshots.get(1).recordingId()), anyLong(), anyLong(), anyInt(), any(), any(), any()))
            .thenReturn(replicationId1);

        final SnapshotReplication snapshotReplication = new SnapshotReplication(
            archive, srcStreamId, srcChannel, replicationChannel);

        snapshotReplication.addSnapshot(snapshots.get(0));
        snapshotReplication.addSnapshot(snapshots.get(1));

        snapshotReplication.poll(nowNs);
        verify(archive).replicate(eq(snapshots.get(0).recordingId()), anyInt(), any(), any());

        snapshotReplication.poll(nowNs);
        snapshotReplication.onSignal(replicationId0, snapshots.get(0).recordingId(), 23423, REPLICATE_END);
        assertFalse(snapshotReplication.isComplete());
    }

    @Test
    void closeWillCloseUnderlyingSnapshotReplication()
    {
        final List<RecordingLog.Snapshot> snapshots = Arrays.asList(
            new RecordingLog.Snapshot(2, 3, 5, 7, 11, 0),
            new RecordingLog.Snapshot(17, 3, 5, 7, 31, -1));

        when(archive.replicate(eq(snapshots.get(0).recordingId()), anyLong(), anyLong(), anyInt(), any(), any(), any()))
            .thenReturn(1L);
        when(archive.replicate(eq(snapshots.get(1).recordingId()), anyLong(), anyLong(), anyInt(), any(), any(), any()))
            .thenReturn(2L);

        final SnapshotReplication snapshotReplication = new SnapshotReplication(
            archive, srcStreamId, srcChannel, replicationChannel);
        snapshots.forEach(snapshotReplication::addSnapshot);

        snapshotReplication.poll(nowNs);

        verify(archive).replicate(anyLong(), anyInt(), any(), any());
        ignoreArchiveContextLookup();

        snapshotReplication.close();
        verify(archive).tryStopReplication(anyLong());

        snapshotReplication.close();
        verifyNoMoreInteractions(archive);
    }

    private void ignoreArchiveContextLookup()
    {
        verify(archive, atLeast(0)).context();
    }
}