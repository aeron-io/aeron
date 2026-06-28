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
import io.aeron.ConcurrentPublication;
import io.aeron.Counter;
import io.aeron.Image;
import io.aeron.logbuffer.Header;
import io.aeron.security.AuthorisationService;
import io.aeron.security.DefaultAuthenticatorSupplier;
import io.aeron.test.cluster.TestClusterClock;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class SessionManagerTest
{
    final RecordingLog mockRecordingLog = mock(RecordingLog.class);
    final Header mockHeader = mock(Header.class);
    final Image mockImage = mock(Image.class);
    final Aeron mockAeron = mock(Aeron.class);
    final ConcurrentPublication mockPublication = mock(ConcurrentPublication.class);

    final TestClusterClock clock = new TestClusterClock(TimeUnit.NANOSECONDS);
    SessionManager sessionManager;

    @BeforeEach
    void setup()
    {
        when(mockHeader.context()).thenReturn(mockImage);
        when(mockImage.sourceIdentity()).thenReturn("localhost:1234");
        when(mockAeron.getPublication(anyLong())).thenReturn(mockPublication);
        when(mockPublication.isConnected()).thenReturn(true);
    }

    void setupSessionManager(final long standbySnapshotNotificationProcessingDelayNs)
    {
        sessionManager = new SessionManager(
            new ClusterMember[0],
            0,
            clock,
            mock(EgressPublisher.class),
            mockAeron,
            0,
            mock(CountedErrorHandler.class),
            mock(Counter.class),
            mock(DistinctErrorLog.class),
            true,
            mock(Counter.class),
            DefaultAuthenticatorSupplier.DEFAULT_AUTHENTICATOR,
            AuthorisationService.ALLOW_ALL,
            mockRecordingLog,
            mock(LogPublisher.class),
            mock(ConsensusPublisher.class),
            mock(ConsensusModuleExtension.class),
            0,
            0,
            0,
            0,
            standbySnapshotNotificationProcessingDelayNs);
    }


    @Test
    void shouldProcessPendingStandbySnapshotNotificationsAfterReachingCommitPosition()
    {
        setupSessionManager(0);

        final List<StandbySnapshotEntry> standby1SnapshotEntries = new ArrayList<>();
        standby1SnapshotEntries.add(new StandbySnapshotEntry(0, 0, 0, 100, 0, -1, "localhost:1234"));
        standby1SnapshotEntries.add(new StandbySnapshotEntry(1, 0, 0, 100, 0, 0, "localhost:1234"));

        sessionManager.onStandbySnapshot(
            0,
            0,
            standby1SnapshotEntries,
            0,
            null,
            new byte[0],
            mockHeader);

        final List<StandbySnapshotEntry> standby2SnapshotEntries = new ArrayList<>();
        standby2SnapshotEntries.add(new StandbySnapshotEntry(2, 0, 0, 50, 0, -1, "localhost:1234"));
        standby2SnapshotEntries.add(new StandbySnapshotEntry(3, 0, 0, 50, 0, 0, "localhost:1234"));

        sessionManager.onStandbySnapshot(
            0,
            0,
            standby2SnapshotEntries,
            0,
            null,
            new byte[0],
            mockHeader);

        sessionManager.processPendingBackupSessions(clock.time(), 0, 0);

        sessionManager.processPendingStandbySnapshotNotifications(49, clock.time());

        verify(mockRecordingLog, never())
            .appendStandbySnapshot(anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyInt(), anyString());

        sessionManager.processPendingStandbySnapshotNotifications(50, clock.time());

        for (final StandbySnapshotEntry entry : standby2SnapshotEntries)
        {
            verify(mockRecordingLog).appendStandbySnapshot(
                eq(entry.recordingId()),
                eq(entry.leadershipTermId()),
                eq(entry.termBaseLogPosition()),
                eq(entry.logPosition()),
                eq(entry.timestamp()),
                eq(entry.serviceId()),
                eq(entry.archiveEndpoint()));
        }

        verifyNoMoreInteractions(mockRecordingLog);

        sessionManager.processPendingStandbySnapshotNotifications(100, clock.time());

        for (final StandbySnapshotEntry entry : standby1SnapshotEntries)
        {
            verify(mockRecordingLog).appendStandbySnapshot(
                eq(entry.recordingId()),
                eq(entry.leadershipTermId()),
                eq(entry.termBaseLogPosition()),
                eq(entry.logPosition()),
                eq(entry.timestamp()),
                eq(entry.serviceId()),
                eq(entry.archiveEndpoint()));
        }

        sessionManager.processPendingStandbySnapshotNotifications(100, clock.time());

        verifyNoMoreInteractions(mockRecordingLog);
    }

    @Test
    void shouldProcessPendingStandbySnapshotNotificationsAfterProcessingDelay()
    {
        clock.update(0, TimeUnit.NANOSECONDS);

        final long processingDelay = 100;

        setupSessionManager(processingDelay);

        final List<StandbySnapshotEntry> standbySnapshotEntries = new ArrayList<>();
        standbySnapshotEntries.add(new StandbySnapshotEntry(0, 0, 0, 100, 0, -1, "localhost:1234"));
        standbySnapshotEntries.add(new StandbySnapshotEntry(1, 0, 0, 100, 0, 0, "localhost:1234"));

        sessionManager.onStandbySnapshot(
            0,
            0,
            standbySnapshotEntries,
            0,
            null,
            new byte[0],
            mockHeader);

        sessionManager.processPendingBackupSessions(clock.time(), 0, 0);

        sessionManager.processPendingStandbySnapshotNotifications(100, clock.time());

        verify(mockRecordingLog, never())
            .appendStandbySnapshot(anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyInt(), anyString());

        clock.increment(processingDelay);

        sessionManager.processPendingStandbySnapshotNotifications(100, clock.time());

        for (final StandbySnapshotEntry entry : standbySnapshotEntries)
        {
            verify(mockRecordingLog).appendStandbySnapshot(
                eq(entry.recordingId()),
                eq(entry.leadershipTermId()),
                eq(entry.termBaseLogPosition()),
                eq(entry.logPosition()),
                eq(entry.timestamp()),
                eq(entry.serviceId()),
                eq(entry.archiveEndpoint()));
        }
    }
}