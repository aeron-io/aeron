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

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ReplicationParams;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.ClusterException;
import io.aeron.exceptions.AeronException;
import org.agrona.concurrent.status.CountersReader;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

final class RecordingReplication implements AutoCloseable
{
    private final long replicationId;
    private final long stopPosition;
    private final long progressCheckTimeoutNs;
    private final long progressCheckIntervalNs;
    private final String srcArchiveChannel;

    private int recordingPositionCounterId = NULL_COUNTER_ID;
    private long recordingId = NULL_VALUE;
    private long position = NULL_POSITION;

    private long progressDeadlineNs;
    private long progressCheckDeadlineNs;
    private final AeronArchive archive;
    private RecordingSignal lastRecordingSignal = RecordingSignal.NULL_VAL;

    private boolean hasReplicationEnded = false;
    private boolean hasSynced = false;
    private boolean hasStopped = false;

    RecordingReplication(
        final AeronArchive archive,
        final long srcRecordingId,
        final String srcArchiveChannel,
        final int srcControlStreamId,
        final ReplicationParams replicationParams,
        final long progressCheckTimeoutNs,
        final long progressCheckIntervalNs,
        final long nowNs)
    {
        this.archive = archive;
        this.stopPosition = replicationParams.stopPosition();
        this.progressCheckTimeoutNs = progressCheckTimeoutNs;
        this.progressCheckIntervalNs = progressCheckIntervalNs;
        this.progressDeadlineNs = nowNs + progressCheckTimeoutNs;
        this.progressCheckDeadlineNs = nowNs + progressCheckIntervalNs;
        this.srcArchiveChannel = srcArchiveChannel;

        replicationId = archive.replicate(
            srcRecordingId,
            srcControlStreamId,
            srcArchiveChannel,
            replicationParams);
    }

    int poll(final long nowNs)
    {
        int workCount = 0;

        if (hasReplicationEnded)
        {
            return workCount;
        }

        try
        {
            if (nowNs >= progressCheckDeadlineNs)
            {
                progressCheckDeadlineNs = nowNs + progressCheckIntervalNs;
                if (pollDstRecordingPosition())
                {
                    progressDeadlineNs = nowNs + progressCheckTimeoutNs;
                }
                workCount++;
            }

            if (nowNs >= progressDeadlineNs)
            {
                if (NULL_POSITION == stopPosition || position < stopPosition)
                {
                    throw new ClusterException("log replication has not progressed", AeronException.Category.WARN);
                }
                else
                {
                    throw new ClusterException("log replication failed to stop");
                }
            }

            return workCount;
        }
        catch (final ClusterException ex)
        {
            try
            {
                close();
            }
            catch (final ClusterException ex1)
            {
                ex.addSuppressed(ex1);
            }

            throw ex;
        }
    }

    long replicationId()
    {
        return replicationId;
    }

    long position()
    {
        return position;
    }

    long recordingId()
    {
        return recordingId;
    }

    boolean hasReplicationEnded()
    {
        return hasReplicationEnded;
    }

    boolean hasSynced()
    {
        return hasSynced;
    }

    boolean hasStopped()
    {
        return hasStopped;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        if (!hasReplicationEnded)
        {
            try
            {
                hasReplicationEnded = true;
                archive.tryStopReplication(replicationId);
            }
            catch (final Exception ex)
            {
                throw new ClusterException("failed to stop log replication", ex, AeronException.Category.WARN);
            }
        }
    }

    void onSignal(final long correlationId, final long recordingId, final long position, final RecordingSignal signal)
    {
        if (correlationId == replicationId)
        {
            if (RecordingSignal.EXTEND == signal)
            {
                final CountersReader counters = archive.context().aeron().countersReader();
                recordingPositionCounterId =
                    RecordingPos.findCounterIdByRecording(counters, recordingId, archive.archiveId());
            }
            else if (RecordingSignal.SYNC == signal)
            {
                hasSynced = true;
            }
            else if (RecordingSignal.REPLICATE_END == signal)
            {
                hasReplicationEnded = true;
            }
            else if (RecordingSignal.STOP == signal)
            {
                if (NULL_POSITION != position)
                {
                    this.position = position;
                }
                hasStopped = true;
            }
            else if (RecordingSignal.DELETE == signal)
            {
                throw new ClusterException("recording was deleted during replication: " + this);
            }

            this.lastRecordingSignal = signal;

            if (NULL_VALUE != recordingId)
            {
                this.recordingId = recordingId;
            }

            if (NULL_POSITION != position)
            {
                this.position = position;
            }
        }
    }

    private boolean pollDstRecordingPosition()
    {
        if (NULL_COUNTER_ID != recordingPositionCounterId)
        {
            final CountersReader counters = archive.context().aeron().countersReader();
            final long recordingPosition = counters.getCounterValue(recordingPositionCounterId);

            if (RecordingPos.isActive(counters, recordingPositionCounterId, recordingId) &&
                recordingPosition > position)
            {
                position = recordingPosition;
                return true;
            }
        }

        return false;
    }

    String srcArchiveChannel()
    {
        return srcArchiveChannel;
    }

    public String toString()
    {
        return "RecordingReplication{" +
            "replicationId=" + replicationId +
            ", stopPosition=" + stopPosition +
            ", progressCheckTimeoutNs=" + progressCheckTimeoutNs +
            ", progressCheckIntervalNs=" + progressCheckIntervalNs +
            ", recordingPositionCounterId=" + recordingPositionCounterId +
            ", recordingId=" + recordingId +
            ", position=" + position +
            ", progressDeadlineNs=" + progressDeadlineNs +
            ", progressCheckDeadlineNs=" + progressCheckDeadlineNs +
            ", lastRecordingSignal=" + lastRecordingSignal +
            ", hasReplicationEnded=" + hasReplicationEnded +
            ", hasSynced=" + hasSynced +
            ", hasStopped=" + hasStopped +
            '}';
    }
}
