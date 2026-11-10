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

import io.aeron.cluster.codecs.CloseReason;
import org.agrona.ExpandableRingBuffer;

import java.util.List;

import static java.util.Collections.unmodifiableList;

class ConsensusModuleStateExport
{
    final long logRecordingId;
    final long leadershipTermId;
    final long expectedAckPosition;
    final long nextSessionId;
    final long serviceAckId;
    final List<TimerStateExport> timers;
    final List<ClusterSessionStateExport> sessions;
    final List<PendingServiceMessageTrackerStateExport> pendingMessageTrackers;

    ConsensusModuleStateExport(
        final long logRecordingId,
        final long leadershipTermId,
        final long expectedAckPosition,
        final long nextSessionId,
        final long serviceAckId,
        final List<TimerStateExport> timers,
        final List<ClusterSessionStateExport> sessions,
        final List<PendingServiceMessageTrackerStateExport> pendingMessageTrackers)
    {
        this.logRecordingId = logRecordingId;
        this.leadershipTermId = leadershipTermId;
        this.expectedAckPosition = expectedAckPosition;
        this.nextSessionId = nextSessionId;
        this.serviceAckId = serviceAckId;
        this.timers = unmodifiableList(timers);
        this.sessions = unmodifiableList(sessions);
        this.pendingMessageTrackers = unmodifiableList(pendingMessageTrackers);
    }

    @Override
    public String toString()
    {
        return "ConsensusModuleStateExport{" +
            "logRecordingId=" + logRecordingId +
            ", leadershipTermId=" + leadershipTermId +
            ", expectedAckPosition=" + expectedAckPosition +
            ", nextSessionId=" + nextSessionId +
            ", serviceAckId=" + serviceAckId +
            ", timers=" + timers +
            ", sessions=" + sessions +
            ", pendingMessageTrackers=" + pendingMessageTrackers +
            '}';
    }

    static class TimerStateExport
    {
        final long correlationId;
        final long deadline;

        TimerStateExport(final long correlationId, final long deadline)
        {
            this.correlationId = correlationId;
            this.deadline = deadline;
        }
    }

    static class ClusterSessionStateExport
    {
        final long id;
        final long correlationId;
        final long openedLogPosition;
        final long timeOfLastActivityNs;
        final int responseStreamId;
        final String responseChannel;
        final CloseReason closeReason;

        ClusterSessionStateExport(
            final long id,
            final long correlationId,
            final long openedLogPosition,
            final long timeOfLastActivityNs,
            final int responseStreamId,
            final String responseChannel,
            final CloseReason closeReason)
        {
            this.id = id;
            this.correlationId = correlationId;
            this.openedLogPosition = openedLogPosition;
            this.timeOfLastActivityNs = timeOfLastActivityNs;
            this.responseStreamId = responseStreamId;
            this.responseChannel = responseChannel;
            this.closeReason = closeReason;
        }
    }

    static class PendingServiceMessageTrackerStateExport
    {
        final long nextServiceSessionId;
        final long logServiceSessionId;
        final int capacity;
        final int serviceId;
        final ExpandableRingBuffer pendingMessages;

        PendingServiceMessageTrackerStateExport(
            final long nextServiceSessionId,
            final long logServiceSessionId,
            final int capacity,
            final int serviceId,
            final ExpandableRingBuffer pendingMessages)
        {
            this.nextServiceSessionId = nextServiceSessionId;
            this.logServiceSessionId = logServiceSessionId;
            this.capacity = capacity;
            this.serviceId = serviceId;
            this.pendingMessages = pendingMessages;
        }
    }
}
