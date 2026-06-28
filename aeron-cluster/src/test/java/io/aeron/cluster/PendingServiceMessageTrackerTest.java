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

import io.aeron.Counter;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestClusterClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PendingServiceMessageTrackerTest
{
    private final CountersManager countersManager = Tests.newCountersManager(16 * 1024);
    private final int counterId = countersManager.allocate("test");
    private final Counter counter = new Counter(countersManager, counterId);
    private final LogPublisher logPublisher = mock(LogPublisher.class);
    private final int clusterSessionIdOffset =
        MessageHeaderEncoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.clusterSessionIdEncodingOffset();
    private final TestClusterClock clusterClock = new TestClusterClock(TimeUnit.MILLISECONDS);

    @BeforeEach
    void setUp()
    {
        when(logPublisher.appendMessage(anyLong(), anyLong(), anyLong(), any(), anyInt(), anyInt()))
            .thenReturn(64L);
    }

    @Test
    void snapshotEmpty()
    {
        final PendingServiceMessageTracker tracker = new PendingServiceMessageTracker(
            0, counter, logPublisher, clusterClock);

        final Snapshot snapshot = takeSnapshot(tracker);

        final PendingServiceMessageTracker trackerLoaded = new PendingServiceMessageTracker(
            0, counter, logPublisher, clusterClock);

        loadSnapshot(trackerLoaded, snapshot);
        trackerLoaded.verify();
    }

    @Test
    void snapshotAfterEnqueueBeforePollAndSweep()
    {
        final PendingServiceMessageTracker tracker = new PendingServiceMessageTracker(
            0, counter, logPublisher, clusterClock);

        final UnsafeBuffer message = new UnsafeBuffer(new byte[64]);

        tracker.enqueueMessage(
            message,
            AeronCluster.SESSION_HEADER_LENGTH,
            message.capacity() - AeronCluster.SESSION_HEADER_LENGTH);

        final Snapshot snapshot = takeSnapshot(tracker);

        final PendingServiceMessageTracker trackerLoaded = new PendingServiceMessageTracker(
            0, counter, logPublisher, clusterClock);

        loadSnapshot(trackerLoaded, snapshot);
        trackerLoaded.verify();
    }

    @Test
    void snapshotAfterEnqueueAndPollBeforeSweep()
    {
        final PendingServiceMessageTracker tracker = new PendingServiceMessageTracker(
            0, counter, logPublisher, clusterClock);

        final UnsafeBuffer message = new UnsafeBuffer(new byte[64]);

        tracker.enqueueMessage(
            message,
            AeronCluster.SESSION_HEADER_LENGTH,
            message.capacity() - AeronCluster.SESSION_HEADER_LENGTH);

        tracker.poll();

        final Snapshot snapshot = takeSnapshot(tracker);

        final PendingServiceMessageTracker trackerLoaded = new PendingServiceMessageTracker(
            0, counter, logPublisher, clusterClock);

        loadSnapshot(trackerLoaded, snapshot);
        trackerLoaded.verify();
    }

    @Test
    void snapshotAfterEnqueuePollAndSweepForLeader()
    {
        final PendingServiceMessageTracker tracker = new PendingServiceMessageTracker(
            0, counter, logPublisher, clusterClock);

        final UnsafeBuffer message = new UnsafeBuffer(new byte[64]);

        tracker.enqueueMessage(
            message,
            AeronCluster.SESSION_HEADER_LENGTH,
            message.capacity() - AeronCluster.SESSION_HEADER_LENGTH);

        tracker.poll();
        tracker.sweepLeaderMessages();

        final Snapshot snapshot = takeSnapshot(tracker);

        final PendingServiceMessageTracker trackerLoaded = new PendingServiceMessageTracker(
            0, counter, logPublisher, clusterClock);

        loadSnapshot(trackerLoaded, snapshot);
        trackerLoaded.verify();
    }


    @Test
    void snapshotAfterEnqueuePollAndSweepForFollower()
    {
        final PendingServiceMessageTracker tracker = new PendingServiceMessageTracker(
            0, counter, logPublisher, clusterClock);

        final UnsafeBuffer message = new UnsafeBuffer(new byte[64]);

        tracker.enqueueMessage(
            message,
            AeronCluster.SESSION_HEADER_LENGTH,
            message.capacity() - AeronCluster.SESSION_HEADER_LENGTH);
        final long clusterSessionId = message.getLong(clusterSessionIdOffset);

        tracker.poll();
        tracker.sweepFollowerMessages(clusterSessionId);

        final Snapshot snapshot = takeSnapshot(tracker);

        final PendingServiceMessageTracker trackerLoaded = new PendingServiceMessageTracker(
            0, counter, logPublisher, clusterClock);

        loadSnapshot(trackerLoaded, snapshot);
        trackerLoaded.verify();
    }

    @Test
    void loadInvalid()
    {
        final CountersManager countersManager = Tests.newCountersManager(16 * 1024);
        final int counterId = countersManager.allocate("test");
        final Counter counter = new Counter(countersManager, counterId);
        final LogPublisher logPublisher = mock(LogPublisher.class);

        final TestClusterClock clusterClock = new TestClusterClock(TimeUnit.MILLISECONDS);

        final PendingServiceMessageTracker tracker = new PendingServiceMessageTracker(
            0, counter, logPublisher, clusterClock);

        tracker.loadState(-9223372036854774166L, -9223372036854774166L, 0);
        assertThrows(ClusterException.class, tracker::verify);
    }

    @Test
    void loadValid()
    {
        final CountersManager countersManager = Tests.newCountersManager(16 * 1024);
        final int counterId = countersManager.allocate("test");
        final Counter counter = new Counter(countersManager, counterId);
        final LogPublisher logPublisher = mock(LogPublisher.class);

        final TestClusterClock clusterClock = new TestClusterClock(TimeUnit.MILLISECONDS);

        final PendingServiceMessageTracker tracker = new PendingServiceMessageTracker(
            0, counter, logPublisher, clusterClock);

        tracker.loadState(-9223372036854774165L, -9223372036854774166L, 0);
        tracker.verify();
    }

    private static Snapshot takeSnapshot(final PendingServiceMessageTracker tracker)
    {
        final long nextServiceSessionId = tracker.nextServiceSessionId();
        final long logServiceSessionId = tracker.logServiceSessionId();
        final int size = tracker.size();
        final ArrayList<UnsafeBuffer> messages = new ArrayList<>();
        tracker.pendingMessages().forEach(
            (buffer, offset, length, headOffset) ->
            {
                final UnsafeBuffer msg = new UnsafeBuffer(new byte[length]);
                msg.putBytes(0, buffer, offset, length);
                messages.add(msg);

                return true;
            },
            10);

        return new Snapshot(nextServiceSessionId, logServiceSessionId, size, messages);
    }

    private static void loadSnapshot(final PendingServiceMessageTracker tracker, final Snapshot snapshot)
    {
        tracker.loadState(snapshot.nextServiceSessionId, snapshot.logServiceSessionId, snapshot.size);
        snapshot.messages.forEach((msg) -> tracker.appendMessage(msg, 0, msg.capacity()));
    }

    private static final class Snapshot
    {
        private final long nextServiceSessionId;
        private final long logServiceSessionId;
        private final int size;
        private final ArrayList<UnsafeBuffer> messages;

        Snapshot(
            final long nextServiceSessionId,
            final long logServiceSessionId,
            final int size,
            final ArrayList<UnsafeBuffer> messages)
        {

            this.nextServiceSessionId = nextServiceSessionId;
            this.logServiceSessionId = logServiceSessionId;
            this.size = size;
            this.messages = messages;
        }
    }
}