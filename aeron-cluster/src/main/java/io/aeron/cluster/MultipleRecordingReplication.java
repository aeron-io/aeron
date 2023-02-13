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
import org.agrona.CloseHelper;
import org.agrona.collections.Long2LongHashMap;

import java.util.ArrayList;

class MultipleRecordingReplication implements AutoCloseable
{
    private final AeronArchive archive;
    private final int srcControlStreamId;
    private final String srcControlChannel;
    private final String replicationChannel;
    private final ArrayList<RecordingInfo> recordingsPending = new ArrayList<>();
    private final Long2LongHashMap recordingsCompleted = new Long2LongHashMap(Aeron.NULL_VALUE);
    private final long progressTimeoutNs;
    private final long progressIntervalNs;
    private int recordingCursor = 0;
    private RecordingReplication recordingReplication = null;

    MultipleRecordingReplication(
        final AeronArchive archive,
        final int srcControlStreamId,
        final String srcControlChannel,
        final String replicationChannel,
        final long replicationProgressTimeoutNs,
        final long replicationProgressIntervalNs)
    {
        this.archive = archive;
        this.srcControlStreamId = srcControlStreamId;
        this.srcControlChannel = srcControlChannel;
        this.replicationChannel = replicationChannel;
        this.progressTimeoutNs = replicationProgressTimeoutNs;
        this.progressIntervalNs = replicationProgressIntervalNs;
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
                if (recordingReplication.hasSynced())
                {
                    final RecordingInfo pending = recordingsPending.get(recordingCursor);
                    recordingsCompleted.put(pending.srcRecordingId, recordingReplication.recordingId());
                    recordingCursor++;

                    CloseHelper.close(recordingReplication);
                    recordingReplication = null;
                }
                else
                {
                    CloseHelper.close(recordingReplication);
                    recordingReplication = null;

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
        recordingReplication = new RecordingReplication(
            archive,
            recordingInfo.srcRecordingId,
            recordingInfo.dstRecordingId,
            recordingInfo.stopPosition,
            srcControlChannel,
            srcControlStreamId,
            replicationChannel,
            progressTimeoutNs,
            progressIntervalNs,
            nowNs);
    }

    private static final class RecordingInfo
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
}
