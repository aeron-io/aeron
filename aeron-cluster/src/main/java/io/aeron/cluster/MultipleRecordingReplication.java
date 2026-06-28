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
import io.aeron.archive.codecs.RecordingSignal;
import org.agrona.CloseHelper;
import org.agrona.collections.Long2LongHashMap;

import java.util.ArrayList;

final class MultipleRecordingReplication implements AutoCloseable
{
    private final AeronArchive archive;
    private final int srcControlStreamId;
    private final String srcControlChannel;
    private final String replicationChannel;
    private final String srcResponseChannel;
    private final ArrayList<RecordingInfo> recordingsPending = new ArrayList<>();
    private final Long2LongHashMap recordingsCompleted = new Long2LongHashMap(Aeron.NULL_VALUE);
    private final long progressTimeoutNs;
    private final long progressIntervalNs;
    private int recordingCursor = 0;
    private RecordingReplication recordingReplication = null;
    private EventListener eventListener = null;

    private MultipleRecordingReplication(
        final AeronArchive archive,
        final int srcControlStreamId,
        final String srcControlChannel,
        final String replicationChannel,
        final String srcResponseChannel,
        final long replicationProgressTimeoutNs,
        final long replicationProgressIntervalNs)
    {
        this.archive = archive;
        this.srcControlStreamId = srcControlStreamId;
        this.srcControlChannel = srcControlChannel;
        this.replicationChannel = replicationChannel;
        this.srcResponseChannel = srcResponseChannel;
        this.progressTimeoutNs = replicationProgressTimeoutNs;
        this.progressIntervalNs = replicationProgressIntervalNs;
    }

    static MultipleRecordingReplication newInstance(
        final AeronArchive archive,
        final int srcControlStreamId,
        final String srcControlChannel,
        final String replicationChannel,
        final long replicationProgressTimeoutNs,
        final long replicationProgressIntervalNs)
    {
        return new MultipleRecordingReplication(
            archive,
            srcControlStreamId,
            srcControlChannel,
            replicationChannel,
            null,
            replicationProgressTimeoutNs,
            replicationProgressIntervalNs);
    }

    static MultipleRecordingReplication newInstance(
        final AeronArchive archive,
        final int srcControlStreamId,
        final String srcControlChannel,
        final String replicationChannel,
        final String srcResponseChannel,
        final long replicationProgressTimeoutNs,
        final long replicationProgressIntervalNs)
    {
        return new MultipleRecordingReplication(
            archive,
            srcControlStreamId,
            srcControlChannel,
            replicationChannel,
            srcResponseChannel,
            replicationProgressTimeoutNs,
            replicationProgressIntervalNs);
    }

    void addRecording(final long srcRecordingId, final long dstRecordingId, final long stopPosition)
    {
        recordingsPending.add(new RecordingInfo(srcRecordingId, dstRecordingId, stopPosition));
    }

    int poll(final long nowNs)
    {
        if (isComplete())
        {
            return 0;
        }

        int workDone = 0;

        if (null == recordingReplication)
        {
            replicateCurrentSnapshot(nowNs);
            workDone++;
        }
        else
        {
            recordingReplication.poll(nowNs);
            if (recordingReplication.hasReplicationEnded())
            {
                final RecordingInfo pending = recordingsPending.get(recordingCursor);

                onReplicationEnded(
                    srcControlChannel,
                    pending.srcRecordingId,
                    recordingReplication.recordingId(),
                    recordingReplication.position(),
                    recordingReplication.hasSynced());

                if (recordingReplication.hasSynced())
                {
                    recordingsCompleted.put(pending.srcRecordingId, recordingReplication.recordingId());
                    recordingCursor++;

                    final RecordingReplication replication = recordingReplication;
                    recordingReplication = null;
                    CloseHelper.close(replication);
                }
                else
                {
                    final RecordingReplication replication = recordingReplication;
                    recordingReplication = null;
                    CloseHelper.close(replication);

                    replicateCurrentSnapshot(nowNs);
                }

                workDone++;
            }
        }

        return workDone;
    }

    long completedDstRecordingId(final long srcRecordingId)
    {
        return recordingsCompleted.get(srcRecordingId);
    }

    void onSignal(final long correlationId, final long recordingId, final long position, final RecordingSignal signal)
    {
        if (null != recordingReplication)
        {
            recordingReplication.onSignal(correlationId, recordingId, position, signal);
        }
    }

    long currentReplicationId()
    {
        return null != recordingReplication ? recordingReplication.replicationId() : Aeron.NULL_VALUE;
    }

    long currentSrcRecordingId()
    {
        if (0 <= recordingCursor && recordingCursor < recordingsPending.size())
        {
            final RecordingInfo recordingInfo = recordingsPending.get(recordingCursor);
            return recordingInfo.srcRecordingId;
        }

        return Aeron.NULL_VALUE;
    }

    boolean isComplete()
    {
        return recordingCursor >= recordingsPending.size();
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.close(recordingReplication);
        recordingReplication = null;
    }

    private void replicateCurrentSnapshot(final long nowNs)
    {
        final RecordingInfo recordingInfo = recordingsPending.get(recordingCursor);
        final ReplicationParams replicationParams = new ReplicationParams()
            .dstRecordingId(recordingInfo.dstRecordingId)
            .stopPosition(recordingInfo.stopPosition)
            .replicationChannel(replicationChannel)
            .srcResponseChannel(srcResponseChannel)
            .replicationSessionId((int)archive.context().aeron().nextCorrelationId());

        recordingReplication = new RecordingReplication(
            archive,
            recordingInfo.srcRecordingId,
            srcControlChannel,
            srcControlStreamId,
            replicationParams,
            progressTimeoutNs,
            progressIntervalNs,
            nowNs);
    }

    static final class RecordingInfo
    {
        private final long srcRecordingId;
        private final long dstRecordingId;
        private final long stopPosition;

        private RecordingInfo(final long srcRecordingId, final long dstRecordingId, final long stopPosition)
        {
            this.srcRecordingId = srcRecordingId;
            this.dstRecordingId = dstRecordingId;
            this.stopPosition = stopPosition;
        }
    }

    private void onReplicationEnded(
        final String srcArchiveControlChannel,
        final long srcRecordingId,
        final long dstRecordingId,
        final long position,
        final boolean hasSynced)
    {
        if (null != eventListener)
        {
            eventListener.onReplicationEnded(
                srcArchiveControlChannel, srcRecordingId, dstRecordingId, position, hasSynced);
        }
    }

    void setEventListener(final EventListener eventListener)
    {
        this.eventListener = eventListener;
    }

    interface EventListener
    {
        void onReplicationEnded(
            String controlUri,
            long srcRecordingId,
            long dstRecordingId,
            long position,
            boolean hasSynced);
    }
}
