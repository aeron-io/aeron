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
import io.aeron.archive.codecs.RecordingSignal;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MultipleRecordingReplicationTest
{
    private static final long SRC_RECORDING_ID = 1;
    private static final long DST_RECORDING_ID = 2;
    private static final long REPLICATION_ID = 4;

    private static final String ENDPOINT = "localhost:20123";
    private static final int SRC_STREAM_ID = 982734;
    private static final String REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final long PROGRESS_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
    private static final long PROGRESS_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);

    private final AeronArchive aeronArchive = mock(AeronArchive.class);

    @BeforeEach
    void setUp()
    {
        final AeronArchive.Context ctx = mock(AeronArchive.Context.class);
        final Aeron aeron = mock(Aeron.class);
        when(aeronArchive.context()).thenReturn(ctx);
        when(ctx.aeron()).thenReturn(aeron);
        when(aeron.countersReader()).thenReturn(mock(CountersReader.class));
        when(aeronArchive.replicate(anyLong(), anyInt(), any(), any())).thenReturn(REPLICATION_ID);
    }

    @Test
    void shouldCompleteOnReplicationEndWithoutWaitingForStop()
    {
        final long t0 = 20L;
        final MultipleRecordingReplication replication = MultipleRecordingReplication.newInstance(
            aeronArchive, SRC_STREAM_ID, ENDPOINT, REPLICATION_CHANNEL, PROGRESS_TIMEOUT_NS, PROGRESS_INTERVAL_NS);
        replication.addRecording(SRC_RECORDING_ID, NULL_VALUE, NULL_POSITION);

        replication.poll(t0);
        assertEquals(REPLICATION_ID, replication.currentReplicationId());

        replication.onSignal(REPLICATION_ID, DST_RECORDING_ID, 0, RecordingSignal.EXTEND);
        replication.onSignal(REPLICATION_ID, DST_RECORDING_ID, 1024, RecordingSignal.SYNC);
        replication.onSignal(REPLICATION_ID, DST_RECORDING_ID, NULL_POSITION, RecordingSignal.REPLICATE_END);

        assertEquals(1, replication.poll(t0 + PROGRESS_TIMEOUT_NS));
        assertTrue(replication.isComplete());
        assertEquals(DST_RECORDING_ID, replication.completedDstRecordingId(SRC_RECORDING_ID));
        verify(aeronArchive, never()).tryStopReplication(anyLong());
    }
}
