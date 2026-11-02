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
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ControlResponsePoller;
import io.aeron.archive.client.ReplicationParams;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.ClusterException;
import io.aeron.exceptions.AeronException;
import io.aeron.test.Tests;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.concurrent.TimeUnit;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static org.agrona.concurrent.status.CountersReader.COUNTER_LENGTH;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RecordingReplicationTest
{
    private static final long SRC_RECORDING_ID = 1;
    private static final long DST_RECORDING_ID = 2;
    private static final long REPLICATION_ID = 4;

    private static final String ENDPOINT = "localhost:20123";
    private static final int SRC_STREAM_ID = 982734;
    private static final String REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final long PROGRESS_CHECK_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
    private static final long PROGRESS_CHECK_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);

    private final CountersReader countersReader = mock(CountersReader.class);
    private final AeronArchive aeronArchive = mock(AeronArchive.class);
    private final ControlResponsePoller controlResponsePoller = mock(ControlResponsePoller.class);
    private final Subscription subscription = mock(Subscription.class);

    @BeforeEach
    void setUp()
    {
        final AeronArchive.Context ctx = mock(AeronArchive.Context.class);
        final Aeron aeron = mock(Aeron.class);
        when(aeronArchive.context()).thenReturn(ctx);
        when(ctx.aeron()).thenReturn(aeron);
        when(aeron.countersReader()).thenReturn(countersReader);

        when(aeronArchive.controlResponsePoller()).thenReturn(controlResponsePoller);
        when(controlResponsePoller.subscription()).thenReturn(subscription);
        when(aeronArchive.replicate(anyLong(), anyInt(), any(), any())).thenReturn(REPLICATION_ID);
    }

    @Test
    void shouldIndicateAppropriateStatesAsSignalsAreReceived()
    {
        final long stopPosition = 982734;
        final long nowNs = 0;

        final ReplicationParams replicationParams = new ReplicationParams()
            .dstRecordingId(DST_RECORDING_ID)
            .stopPosition(stopPosition)
            .replicationChannel(REPLICATION_CHANNEL)
            .replicationSessionId(Aeron.NULL_VALUE);

        final RecordingReplication logReplication = new RecordingReplication(
            aeronArchive,
            SRC_RECORDING_ID,
            ENDPOINT,
            SRC_STREAM_ID,
            replicationParams,
            PROGRESS_CHECK_TIMEOUT_NS,
            PROGRESS_CHECK_INTERVAL_NS,
            nowNs);

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, NULL_POSITION, RecordingSignal.REPLICATE_END);
        assertTrue(logReplication.hasReplicationEnded());
        assertFalse(logReplication.hasSynced());
        assertFalse(logReplication.hasStopped());

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, NULL_POSITION, RecordingSignal.STOP);
        assertTrue(logReplication.hasReplicationEnded());
        assertFalse(logReplication.hasSynced());
        assertTrue(logReplication.hasStopped());

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, NULL_POSITION, RecordingSignal.SYNC);
        assertTrue(logReplication.hasReplicationEnded());
        assertTrue(logReplication.hasSynced());
        assertTrue(logReplication.hasStopped());
    }

    @ParameterizedTest
    @EnumSource(value = RecordingSignal.class, names = { "START", "SYNC", "EXTEND", "REPLICATE", "MERGE" })
    void shouldStopReplicationIfNotAlreadyStopped(final RecordingSignal recordingSignal)
    {
        final long stopPosition = 982734;
        final long nowNs = 0;

        final ReplicationParams replicationParams = new ReplicationParams()
            .dstRecordingId(DST_RECORDING_ID)
            .stopPosition(stopPosition)
            .replicationChannel(REPLICATION_CHANNEL)
            .replicationSessionId(Aeron.NULL_VALUE);

        final RecordingReplication logReplication = new RecordingReplication(
            aeronArchive,
            SRC_RECORDING_ID,
            ENDPOINT,
            SRC_STREAM_ID,
            replicationParams,
            PROGRESS_CHECK_TIMEOUT_NS,
            PROGRESS_CHECK_INTERVAL_NS,
            nowNs);

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, stopPosition, recordingSignal);
        logReplication.poll(nowNs);
        assertFalse(logReplication.hasReplicationEnded());

        logReplication.close();
        verify(aeronArchive).tryStopReplication(REPLICATION_ID);
    }

    @Test
    void shouldNotStopReplicationIfStopSignalled()
    {
        final long stopPosition = 982734;
        final long nowNs = 0;

        final ReplicationParams replicationParams = new ReplicationParams()
            .dstRecordingId(DST_RECORDING_ID)
            .stopPosition(stopPosition)
            .replicationChannel(REPLICATION_CHANNEL)
            .replicationSessionId(Aeron.NULL_VALUE);

        final RecordingReplication logReplication = new RecordingReplication(
            aeronArchive,
            SRC_RECORDING_ID,
            ENDPOINT,
            SRC_STREAM_ID,
            replicationParams,
            PROGRESS_CHECK_TIMEOUT_NS,
            PROGRESS_CHECK_INTERVAL_NS,
            nowNs);

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, stopPosition, RecordingSignal.STOP);

        logReplication.close();
        verify(aeronArchive, never()).stopReplication(anyLong());
    }

    @Test
    void shouldFailIfRecordingLogIsDeletedDuringReplication()
    {
        final RecordingSignal recordingSignal = RecordingSignal.DELETE;
        final long stopPosition = 982734;
        final long nowNs = 0;

        final ReplicationParams replicationParams = new ReplicationParams()
            .dstRecordingId(DST_RECORDING_ID)
            .stopPosition(stopPosition)
            .replicationChannel(REPLICATION_CHANNEL)
            .replicationSessionId(Aeron.NULL_VALUE);

        final RecordingReplication logReplication = new RecordingReplication(
            aeronArchive,
            SRC_RECORDING_ID,
            ENDPOINT,
            SRC_STREAM_ID,
            replicationParams,
            PROGRESS_CHECK_TIMEOUT_NS,
            PROGRESS_CHECK_INTERVAL_NS,
            nowNs);

        assertThrows(
            ClusterException.class,
            () -> logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, stopPosition - 1, recordingSignal));
    }

    @Test
    void shouldPollForProgressAndFailIfNotProgressing()
    {
        final long stopPosition = 982734;
        final long t0 = 20L;

        final ReplicationParams replicationParams = new ReplicationParams()
            .dstRecordingId(DST_RECORDING_ID)
            .stopPosition(stopPosition)
            .replicationChannel(REPLICATION_CHANNEL)
            .replicationSessionId(Aeron.NULL_VALUE);

        final RecordingReplication logReplication = new RecordingReplication(
            aeronArchive,
            SRC_RECORDING_ID,
            ENDPOINT,
            SRC_STREAM_ID,
            replicationParams,
            PROGRESS_CHECK_TIMEOUT_NS,
            PROGRESS_CHECK_INTERVAL_NS,
            t0);

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, 0, RecordingSignal.EXTEND);
        logReplication.poll(t0);
        logReplication.poll(t0 + (PROGRESS_CHECK_INTERVAL_NS - 1));
        logReplication.poll(t0 + PROGRESS_CHECK_INTERVAL_NS);

        assertThrows(
            ClusterException.class,
            () -> logReplication.poll(t0 + PROGRESS_CHECK_INTERVAL_NS + PROGRESS_CHECK_TIMEOUT_NS));
    }

    @ParameterizedTest
    @CsvSource({
        "SYNC, 64, ERROR, ERROR - log replication failed to stop",
        "EXTEND, 32, WARN, WARN - log replication has not progressed"
    })
    void shouldRefreshProgressDeadlineForSignalPositionAdvance(
        final RecordingSignal signal,
        final long signalPosition,
        final AeronException.Category category,
        final String message)
    {
        final long t0 = 20L;
        final long stopPosition = 64;
        final long oldDeadlineNs = t0 + PROGRESS_CHECK_TIMEOUT_NS;
        final RecordingReplication logReplication = newRecordingReplication(stopPosition, t0);

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, 0, RecordingSignal.REPLICATE);
        logReplication.poll(t0);
        logReplication.poll(oldDeadlineNs - 1);

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, signalPosition, signal);
        assertDoesNotThrow(() -> logReplication.poll(oldDeadlineNs));
        assertFalse(logReplication.hasReplicationEnded());
        verify(aeronArchive, never()).tryStopReplication(anyLong());

        final long refreshedDeadlineNs = oldDeadlineNs + PROGRESS_CHECK_TIMEOUT_NS;
        assertDoesNotThrow(() -> logReplication.poll(refreshedDeadlineNs - 1));
        assertEquals(signalPosition, logReplication.position());

        final ClusterException exception = assertThrows(
            ClusterException.class, () -> logReplication.poll(refreshedDeadlineNs));
        assertEquals(message, exception.getMessage());
        assertEquals(category, exception.category());
        verify(aeronArchive).tryStopReplication(REPLICATION_ID);
    }

    @Test
    void shouldEndWhenReplicateEndFollowsSyncSignalledJustBeforeProgressDeadline()
    {
        final long t0 = 20L;
        final long stopPosition = 64;
        final long oldDeadlineNs = t0 + PROGRESS_CHECK_TIMEOUT_NS;
        final RecordingReplication logReplication = newRecordingReplication(stopPosition, t0);

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, 32, RecordingSignal.REPLICATE);
        logReplication.poll(t0);
        logReplication.poll(oldDeadlineNs - 1);

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, stopPosition, RecordingSignal.SYNC);
        assertDoesNotThrow(() -> logReplication.poll(oldDeadlineNs));

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, NULL_POSITION, RecordingSignal.REPLICATE_END);
        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, stopPosition, RecordingSignal.STOP);
        assertEquals(0, logReplication.poll(oldDeadlineNs + PROGRESS_CHECK_TIMEOUT_NS));
        assertTrue(logReplication.hasSynced());
        assertTrue(logReplication.hasReplicationEnded());
        assertTrue(logReplication.hasStopped());
        assertEquals(stopPosition, logReplication.position());

        logReplication.close();
        verify(aeronArchive, never()).tryStopReplication(anyLong());
    }

    @Test
    void shouldNotRefreshProgressDeadlineForSignalsWhichDoNotAdvancePosition()
    {
        final long t0 = 20L;
        final long stopPosition = 64;
        final long oldDeadlineNs = t0 + PROGRESS_CHECK_TIMEOUT_NS;
        final RecordingReplication logReplication = newRecordingReplication(stopPosition, t0);

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, 32, RecordingSignal.REPLICATE);
        logReplication.poll(t0);
        logReplication.poll(oldDeadlineNs - 1);

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, NULL_POSITION, RecordingSignal.SYNC);
        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, 32, RecordingSignal.SYNC);
        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, 32, RecordingSignal.EXTEND);
        logReplication.onSignal(REPLICATION_ID + 1, DST_RECORDING_ID, stopPosition, RecordingSignal.SYNC);

        final ClusterException exception = assertThrows(
            ClusterException.class, () -> logReplication.poll(oldDeadlineNs));
        assertEquals("WARN - log replication has not progressed", exception.getMessage());
        assertTrue(logReplication.hasSynced());
        verify(aeronArchive).tryStopReplication(REPLICATION_ID);
    }

    @Test
    void shouldNotRefreshProgressDeadlineForSignalPositionBelowCurrentPosition()
    {
        final long t0 = 20L;
        final long stopPosition = 64;
        final long oldDeadlineNs = t0 + PROGRESS_CHECK_TIMEOUT_NS;
        final RecordingReplication logReplication = newRecordingReplication(stopPosition, t0);

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, 32, RecordingSignal.REPLICATE);
        logReplication.poll(t0);

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, 16, RecordingSignal.REPLICATE);
        assertDoesNotThrow(() -> logReplication.poll(oldDeadlineNs - 1));
        assertEquals(16, logReplication.position());

        final ClusterException exception = assertThrows(
            ClusterException.class, () -> logReplication.poll(oldDeadlineNs));
        assertEquals("WARN - log replication has not progressed", exception.getMessage());
        assertEquals(AeronException.Category.WARN, exception.category());
        verify(aeronArchive).tryStopReplication(REPLICATION_ID);
    }

    @Test
    void shouldRefreshProgressDeadlineWhenSyncSignalPositionMatchesRecordingCounter()
    {
        final long archiveId = 19;
        final long t0 = 20L;
        final long stopPosition = 64;
        final CountersManager countersManager = Tests.newCountersManager(2 * COUNTER_LENGTH);
        final Aeron aeron = aeronArchive.context().aeron();
        when(aeron.countersReader()).thenReturn(countersManager);
        when(aeron.addCounter(
            anyInt(), any(DirectBuffer.class), anyInt(), anyInt(), any(DirectBuffer.class), anyInt(), anyInt()))
            .thenAnswer(Tests.addCounterAnswer(countersManager, () -> 1L));
        when(aeronArchive.archiveId()).thenReturn(archiveId);
        final Counter recordingPosition = RecordingPos.allocate(
            aeron, new UnsafeBuffer(new byte[1024]), archiveId, DST_RECORDING_ID, 5, 6, REPLICATION_CHANNEL, ENDPOINT);

        final RecordingReplication logReplication = newRecordingReplication(stopPosition, t0);
        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, 0, RecordingSignal.EXTEND);
        logReplication.poll(t0);

        recordingPosition.set(32);
        logReplication.poll(t0 + PROGRESS_CHECK_INTERVAL_NS);
        assertEquals(32, logReplication.position());
        final long counterDeadlineNs = t0 + PROGRESS_CHECK_INTERVAL_NS + PROGRESS_CHECK_TIMEOUT_NS;

        recordingPosition.set(stopPosition);
        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, stopPosition, RecordingSignal.SYNC);
        logReplication.poll(t0 + (2 * PROGRESS_CHECK_INTERVAL_NS));
        assertDoesNotThrow(() -> logReplication.poll(counterDeadlineNs));
        assertEquals(stopPosition, logReplication.position());

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, NULL_POSITION, RecordingSignal.REPLICATE_END);
        assertEquals(0, logReplication.poll(counterDeadlineNs + PROGRESS_CHECK_TIMEOUT_NS));
        assertTrue(logReplication.hasReplicationEnded());
        verify(aeronArchive, never()).tryStopReplication(anyLong());
    }

    private RecordingReplication newRecordingReplication(final long stopPosition, final long nowNs)
    {
        final ReplicationParams replicationParams = new ReplicationParams()
            .dstRecordingId(DST_RECORDING_ID)
            .stopPosition(stopPosition)
            .replicationChannel(REPLICATION_CHANNEL)
            .replicationSessionId(Aeron.NULL_VALUE);

        return new RecordingReplication(
            aeronArchive,
            SRC_RECORDING_ID,
            ENDPOINT,
            SRC_STREAM_ID,
            replicationParams,
            PROGRESS_CHECK_TIMEOUT_NS,
            PROGRESS_CHECK_INTERVAL_NS,
            nowNs);
    }
}
