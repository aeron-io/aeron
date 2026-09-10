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
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.SystemEpochClock;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.Arrays;
import java.util.List;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.ClusterBackup.Configuration.ReplayStart;
import static io.aeron.cluster.ClusterBackupAgent.replayStartPosition;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClusterBackupAgentTest
{
    final AeronArchive mockAeronArchive = mock(AeronArchive.class);

    @Test
    void shouldReturnReplayStartPositionIfAlreadyExisting()
    {
        final long expectedStartPosition = 892374;
        final long recordingId = 234;
        final RecordingLog.Entry lastTerm = new RecordingLog.Entry(
            recordingId, 0, 0, expectedStartPosition, 0, 0, 0, null, true, 0);

        when(mockAeronArchive.getStopPosition(anyLong())).thenReturn(expectedStartPosition);

        final long replayStartPosition = replayStartPosition(
            lastTerm, emptyList(), ReplayStart.BEGINNING, mockAeronArchive);
        assertEquals(expectedStartPosition, replayStartPosition);
    }

    @Test
    void shouldReturnNullPositionIfLastTermIsNullAndSnapshotsIsEmpty()
    {
        assertEquals(NULL_POSITION, replayStartPosition(null, emptyList(), ReplayStart.BEGINNING, mockAeronArchive));
    }

    @Test
    void shouldLargestPositionLessThanOrEqualToInitialReplayPosition()
    {
        final List<RecordingLog.Snapshot> snapshots = Arrays.asList(
            new RecordingLog.Snapshot(1, 0, 0, 1000, 0, ConsensusModule.Configuration.SERVICE_ID),
            new RecordingLog.Snapshot(1, 0, 0, 2000, 0, ConsensusModule.Configuration.SERVICE_ID),
            new RecordingLog.Snapshot(1, 0, 0, 3000, 0, ConsensusModule.Configuration.SERVICE_ID),
            new RecordingLog.Snapshot(1, 0, 0, 4000, 0, ConsensusModule.Configuration.SERVICE_ID),
            new RecordingLog.Snapshot(1, 0, 0, 5000, 0, ConsensusModule.Configuration.SERVICE_ID),
            new RecordingLog.Snapshot(1, 0, 0, 6000, 0, ConsensusModule.Configuration.SERVICE_ID));

        assertEquals(NULL_POSITION, replayStartPosition(null, snapshots, ReplayStart.BEGINNING, mockAeronArchive));
        assertEquals(
            6000, replayStartPosition(null, snapshots, ReplayStart.LATEST_SNAPSHOT, mockAeronArchive));
    }

    @Test
    void shouldRunTerminationHooksOnTermination()
    {
        final Aeron aeron = mock(Aeron.class);
        final AgentInvoker clientInvoker = mock(AgentInvoker.class);
        when(aeron.conductorAgentInvoker()).thenReturn(clientInvoker);
        when(aeron.isClosed()).thenReturn(true);

        final Runnable terminationHook = mock(Runnable.class);
        final ExtendedTerminationHook extendedTerminationHook = mock(ExtendedTerminationHook.class);

        final ClusterBackup.Context ctx = new ClusterBackup.Context()
            .aeron(aeron)
            .epochClock(SystemEpochClock.INSTANCE)
            .terminationHook(terminationHook)
            .extendedTerminationHook(extendedTerminationHook);

        final ClusterBackupAgent agent = new ClusterBackupAgent(ctx);

        final AgentTerminationException ex = assertThrows(AgentTerminationException.class, agent::doWork);

        final InOrder inOrder = inOrder(terminationHook, extendedTerminationHook);
        inOrder.verify(extendedTerminationHook).run(ex);
        inOrder.verify(terminationHook).run();
    }
}
