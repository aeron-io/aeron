/*
 * Copyright 2014-2024 Real Logic Limited.
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

import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SnapshotMarkerEncoder;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;

import java.util.concurrent.TimeUnit;

import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.SNAPSHOT_TYPE_ID;

/**
 * Copies the relevant bits of a consensus module snapshot into a regular service snapshot.
 * Copies the snapshot marker begin and end, as well as the sessions.
 * Modifies the snapshot marker type id.
 */
class ConsensusModuleSnapshotCopier implements ConsensusModuleSnapshotListener
{
    private final BufferClaim bufferClaim = new BufferClaim();
    private final SnapshotMarkerEncoder encoder = new SnapshotMarkerEncoder();
    private final ExclusivePublication publication;

    ConsensusModuleSnapshotCopier(final ExclusivePublication publication)
    {
        this.publication = publication;
    }

    public void onLoadBeginSnapshot(
        final int appVersion, final TimeUnit timeUnit, final DirectBuffer buffer, final int offset, final int length)
    {
        onSnapshotMarker(buffer, offset, length);
    }

    public void onLoadEndSnapshot(final DirectBuffer buffer, final int offset, final int length)
    {
        onSnapshotMarker(buffer, offset, length);
    }

    public void onLoadConsensusModuleState(
        final long nextSessionId,
        final long nextServiceSessionId,
        final long logServiceSessionId,
        final int pendingMessageCapacity,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
    }

    public void onLoadPendingMessage(
        final long clusterSessionId, final DirectBuffer buffer, final int offset, final int length)
    {
    }

    public void onLoadClusterSession(
        final long clusterSessionId,
        final long correlationId,
        final long openedLogPosition,
        final long timeOfLastActivity,
        final CloseReason closeReason,
        final int responseStreamId,
        final String responseChannel,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        claimAndPut(buffer, offset, length);
        bufferClaim.commit();
    }

    public void onLoadTimer(
        final long correlationId, final long deadline, final DirectBuffer buffer, final int offset, final int length)
    {
    }

    public void onLoadPendingMessageTracker(
        final long nextServiceSessionId,
        final long logServiceSessionId,
        final int pendingMessageCapacity,
        final int serviceId,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
    }

    private void onSnapshotMarker(final DirectBuffer buffer, final int offset, final int length)
    {
        claimAndPut(buffer, offset, length);

        // overwrite the marker's type id before committing
        encoder.wrap(bufferClaim.buffer(), bufferClaim.offset() + MessageHeaderEncoder.ENCODED_LENGTH);
        encoder.typeId(SNAPSHOT_TYPE_ID);

        bufferClaim.commit();
    }

    private void claimAndPut(final DirectBuffer buffer, final int offset, final int length)
    {
        while (true)
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                bufferClaim.putBytes(buffer, offset, length);
                break;
            }

            if (Publication.NOT_CONNECTED == result)
            {
                throw new ClusterException("publication is not connected");
            }

            if (Publication.CLOSED == result)
            {
                throw new ClusterException("publication is closed");
            }

            if (Publication.MAX_POSITION_EXCEEDED == result)
            {
                throw new ClusterException(
                    "publication at max position: term-length=" + publication.termBufferLength());
            }

            Thread.yield();
        }
    }
}
