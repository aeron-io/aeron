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

import io.aeron.ChannelUri;
import io.aeron.Counter;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.cluster.client.ClusterException;
import io.aeron.exceptions.AeronException;
import org.agrona.CloseHelper;
import org.agrona.collections.Object2ObjectHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.status.RecordingPos.NULL_RECORDING_ID;

class StandbySnapshotReplicator implements AutoCloseable
{
    private final int memberId;
    private final AeronArchive archive;
    private final RecordingLog recordingLog;
    private final int serviceCount;
    private final String archiveControlChannel;
    private final int archiveControlStreamId;
    private final String replicationChannel;
    private final int fileSyncLevel;
    private final Counter snapshotCounter;
    private final Object2ObjectHashMap<String, String> errorsByEndpoint = new Object2ObjectHashMap<>();
    private MultipleRecordingReplication recordingReplication;
    private ArrayList<SnapshotReplicationEntry> snapshotsToReplicate;
    private SnapshotReplicationEntry currentSnapshotToReplicate;
    private boolean isComplete = false;

    StandbySnapshotReplicator(
        final int memberId,
        final AeronArchive archive,
        final RecordingLog recordingLog,
        final int serviceCount,
        final String archiveControlChannel,
        final int archiveControlStreamId,
        final String replicationChannel,
        final int fileSyncLevel,
        final Counter snapshotCounter)
    {
        this.memberId = memberId;
        this.archive = archive;
        this.recordingLog = recordingLog;
        this.serviceCount = serviceCount;
        this.archiveControlChannel = archiveControlChannel;
        this.archiveControlStreamId = archiveControlStreamId;
        this.replicationChannel = replicationChannel;
        this.fileSyncLevel = fileSyncLevel;
        this.snapshotCounter = snapshotCounter;
    }

    static StandbySnapshotReplicator newInstance(
        final int memberId,
        final AeronArchive.Context archiveCtx,
        final RecordingLog recordingLog,
        final int serviceCount,
        final String archiveControlChannel,
        final int archiveControlStreamId,
        final String replicationChannel,
        final int fileSyncLevel, final Counter snapshotCounter)
    {
        final AeronArchive archive = AeronArchive.connect(archiveCtx.clone().errorHandler(null));
        final StandbySnapshotReplicator standbySnapshotReplicator = new StandbySnapshotReplicator(
            memberId,
            archive,
            recordingLog,
            serviceCount,
            archiveControlChannel,
            archiveControlStreamId,
            replicationChannel,
            fileSyncLevel,
            snapshotCounter);
        archive.context().recordingSignalConsumer(standbySnapshotReplicator::onSignal);
        return standbySnapshotReplicator;
    }

    int poll(final long nowNs)
    {
        int workCount = 0;

        if (null == recordingReplication)
        {
            workCount++;

            if (null == snapshotsToReplicate)
            {
                snapshotsToReplicate = computeSnapshotsToReplicate();

                if (null == snapshotsToReplicate)
                {
                    isComplete = true;
                    return workCount;
                }
            }

            if (snapshotsToReplicate.isEmpty())
            {
                throw new ClusterException(
                    "failed to replicate any standby snapshots, errors: " + errorsByEndpoint,
                    AeronException.Category.WARN);
            }

            currentSnapshotToReplicate = snapshotsToReplicate.remove(0);
            final String srcChannel = ChannelUri.createDestinationUri(
                archiveControlChannel, currentSnapshotToReplicate.endpoint);

            final long progressTimeoutNs = archive.context().messageTimeoutNs() * 2;
            final long progressIntervalNs = progressTimeoutNs / 10;

            recordingReplication = MultipleRecordingReplication.newInstance(
                archive,
                archiveControlStreamId,
                srcChannel,
                replicationChannel,
                progressTimeoutNs,
                progressIntervalNs);
            recordingReplication.setEventListener(this::logReplicationEnded);

            for (int i = 0, n = currentSnapshotToReplicate.recordingLogEntries.size(); i < n; i++)
            {
                final RecordingLog.Entry entry = currentSnapshotToReplicate.recordingLogEntries.get(i);
                recordingReplication.addRecording(entry.recordingId, NULL_RECORDING_ID, NULL_POSITION);
            }

            workCount++;
        }

        try
        {
            workCount += recordingReplication.poll(nowNs);
            archive.pollForRecordingSignals();
        }
        catch (final ArchiveException | ClusterException ex)
        {
            errorsByEndpoint.put(currentSnapshotToReplicate.endpoint, ex.getMessage());
            CloseHelper.quietClose(recordingReplication);
            recordingReplication = null;
        }

        if (null != recordingReplication && recordingReplication.isComplete())
        {
            for (int i = 0, n = currentSnapshotToReplicate.recordingLogEntries.size(); i < n; i++)
            {
                final RecordingLog.Entry entry = currentSnapshotToReplicate.recordingLogEntries.get(i);
                final long dstRecordingId = recordingReplication.completedDstRecordingId(entry.recordingId);
                recordingLog.appendSnapshot(
                    dstRecordingId,
                    entry.leadershipTermId,
                    entry.termBaseLogPosition,
                    entry.logPosition,
                    entry.timestamp,
                    entry.serviceId);
            }
            recordingLog.force(fileSyncLevel);

            snapshotCounter.incrementRelease();

            CloseHelper.quietClose(recordingReplication);
            recordingReplication = null;
            isComplete = true;
        }

        return workCount;
    }

    private ArrayList<SnapshotReplicationEntry> computeSnapshotsToReplicate()
    {
        final Map<String, List<RecordingLog.Entry>> snapshotsByEndpoint = filterByExistingRecordingLogEntries(
            recordingLog.latestStandbySnapshots(serviceCount));

        final ArrayList<SnapshotReplicationEntry> orderedSnapshotToReplicate;
        if (snapshotsByEndpoint.isEmpty())
        {
            orderedSnapshotToReplicate = null;
        }
        else
        {
            orderedSnapshotToReplicate = new ArrayList<>();

            snapshotsByEndpoint.forEach(
                (k, v) ->
                {
                    final long logPosition = v.get(0).logPosition;
                    orderedSnapshotToReplicate.add(new SnapshotReplicationEntry(k, logPosition, v));
                });

            orderedSnapshotToReplicate.sort(StandbySnapshotReplicator::compareTo);
        }

        return orderedSnapshotToReplicate;
    }

    private static int compareTo(final SnapshotReplicationEntry a, final SnapshotReplicationEntry b)
    {
        final int descendingOrderCompare = -Long.compare(a.logPosition, b.logPosition);
        if (0 != descendingOrderCompare)
        {
            return descendingOrderCompare;
        }

        return a.endpoint.compareTo(b.endpoint);
    }

    boolean isComplete()
    {
        return isComplete;
    }

    void onSignal(
        final long controlSessionId,
        final long correlationId,
        final long recordingId,
        final long subscriptionId,
        final long position,
        final RecordingSignal signal)
    {
        if (null != recordingReplication)
        {
            recordingReplication.onSignal(correlationId, recordingId, position, signal);
        }
    }

    public void close()
    {
        CloseHelper.quietClose(archive);
    }

    private static final class SnapshotReplicationEntry
    {
        private final String endpoint;
        private final long logPosition;
        private final List<RecordingLog.Entry> recordingLogEntries = new ArrayList<>();

        private SnapshotReplicationEntry(
            final String endpoint,
            final long logPosition,
            final List<RecordingLog.Entry> entries)
        {
            this.endpoint = endpoint;
            this.logPosition = logPosition;
            this.recordingLogEntries.addAll(entries);
        }
    }

    private void logReplicationEnded(
        final String controlUri,
        final long srcRecordingId,
        final long dstRecordingId,
        final long position,
        final boolean hasSynced)
    {
        ConsensusModuleAgent.logReplicationEnded(
            memberId, "STANDBY_SNAPSHOT", controlUri, srcRecordingId, dstRecordingId, position, hasSynced);
    }

    private Map<String, List<RecordingLog.Entry>> filterByExistingRecordingLogEntries(
        final Map<String, List<RecordingLog.Entry>> standbySnapshotsByEndpoint)
    {
        final Map<String, List<RecordingLog.Entry>> filteredSnapshotsByEndpoint = new Object2ObjectHashMap<>();

        for (final Map.Entry<String, List<RecordingLog.Entry>> entry : standbySnapshotsByEndpoint.entrySet())
        {
            for (int i = entry.getValue().size(); --i > -1;)
            {
                final RecordingLog.Entry standbySnapshotEntry = entry.getValue().get(i);
                final RecordingLog.Entry snapshotEntry = recordingLog.getLatestSnapshot(standbySnapshotEntry.serviceId);
                if (null != snapshotEntry && standbySnapshotEntry.logPosition <= snapshotEntry.logPosition)
                {
                    entry.getValue().remove(i);
                }
            }

            if (!entry.getValue().isEmpty())
            {
                filteredSnapshotsByEndpoint.put(entry.getKey(), entry.getValue());
            }
        }

        return filteredSnapshotsByEndpoint;
    }
}
