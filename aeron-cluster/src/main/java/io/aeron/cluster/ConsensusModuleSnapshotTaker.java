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
import io.aeron.ExclusivePublication;
import io.aeron.cluster.codecs.*;
import io.aeron.cluster.service.SnapshotTaker;
import io.aeron.exceptions.AeronEvent;
import io.aeron.exceptions.AeronException;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.ExpandableRingBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.IdleStrategy;

class ConsensusModuleSnapshotTaker
    extends SnapshotTaker
    implements ExpandableRingBuffer.MessageConsumer, TimerService.TimerSnapshotTaker
{
    private static final int ENCODED_TIMER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH + TimerEncoder.BLOCK_LENGTH;

    private final ExpandableArrayBuffer offerBuffer = new ExpandableArrayBuffer(1024);
    private final ClusterSessionEncoder clusterSessionEncoder = new ClusterSessionEncoder();
    private final TimerEncoder timerEncoder = new TimerEncoder();
    private final ConsensusModuleEncoder consensusModuleEncoder = new ConsensusModuleEncoder();

    private final PendingMessageTrackerEncoder pendingMessageTrackerEncoder = new PendingMessageTrackerEncoder();

    ConsensusModuleSnapshotTaker(
        final ExclusivePublication publication, final IdleStrategy idleStrategy, final AgentInvoker aeronClientInvoker)
    {
        super(publication, idleStrategy, aeronClientInvoker);
    }

    @Override
    public boolean onMessage(final MutableDirectBuffer buffer, final int offset, final int length, final int headOffset)
    {
        offer(buffer, offset, length);
        return true;
    }

    void snapshotConsensusModuleState(
        final long nextSessionId,
        final long nextServiceSessionId,
        final long logServiceSessionId,
        final int pendingMessageCapacity)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ConsensusModuleEncoder.BLOCK_LENGTH;

        idleStrategy.reset();
        while (true)
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                consensusModuleEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .nextSessionId(nextSessionId)
                    .nextServiceSessionId(nextServiceSessionId)
                    .logServiceSessionId(logServiceSessionId)
                    .pendingMessageCapacity(pendingMessageCapacity);

                bufferClaim.commit();
                break;
            }

            checkResultAndIdle(result);
        }
    }

    void snapshotSession(final ClusterSession session)
    {
        final String responseChannel = session.responseChannel();
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ClusterSessionEncoder.BLOCK_LENGTH +
            ClusterSessionEncoder.responseChannelHeaderLength() + responseChannel.length();

        if (length <= publication.maxPayloadLength())
        {
            idleStrategy.reset();
            while (true)
            {
                final long result = publication.tryClaim(length, bufferClaim);
                if (result > 0)
                {
                    encodeSession(session, responseChannel, bufferClaim.buffer(), bufferClaim.offset());
                    bufferClaim.commit();
                    break;
                }

                checkResultAndIdle(result);
            }
        }
        else
        {
            final int offset = 0;
            encodeSession(session, responseChannel, offerBuffer, offset);
            offer(offerBuffer, offset, length);
        }
    }

    @Override
    public void snapshotTimer(final long correlationId, final long deadline)
    {
        idleStrategy.reset();
        while (true)
        {
            final long result = publication.tryClaim(ENCODED_TIMER_LENGTH, bufferClaim);
            if (result > 0)
            {
                timerEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .correlationId(correlationId)
                    .deadline(deadline);
                bufferClaim.commit();
                break;
            }

            checkResultAndIdle(result);
        }
    }

    void snapshot(final PendingServiceMessageTracker tracker, final ErrorHandler errorHandler)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + PendingMessageTrackerEncoder.BLOCK_LENGTH;
        final long nextServiceSessionId = correctNextServiceSessionId(tracker, errorHandler);

        idleStrategy.reset();
        while (true)
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                pendingMessageTrackerEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .nextServiceSessionId(nextServiceSessionId)
                    .logServiceSessionId(tracker.logServiceSessionId())
                    .pendingMessageCapacity(tracker.pendingMessages().size())
                    .serviceId(tracker.serviceId());
                bufferClaim.commit();
                break;
            }

            checkResultAndIdle(result);
        }

        tracker.pendingMessages().forEach(this, Integer.MAX_VALUE);
    }

    private void encodeSession(
        final ClusterSession session, final String responseChannel, final MutableDirectBuffer buffer, final int offset)
    {
        clusterSessionEncoder
            .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
            .clusterSessionId(session.id())
            .correlationId(session.correlationId())
            .openedLogPosition(session.openedLogPosition())
            .timeOfLastActivity(Aeron.NULL_VALUE)
            .closeReason(session.closeReason())
            .responseStreamId(session.responseStreamId())
            .responseChannel(responseChannel);
    }

    private static long correctNextServiceSessionId(
        final PendingServiceMessageTracker tracker,
        final ErrorHandler errorHandler)
    {
        final long nextServiceSessionId = tracker.pendingMessages().isEmpty() ?
            tracker.logServiceSessionId() + 1 : tracker.nextServiceSessionId();
        final long missedServiceMessageCount = nextServiceSessionId - tracker.nextServiceSessionId();

        if (0 < missedServiceMessageCount)
        {
            final String message = "Follower has missed " + missedServiceMessageCount +
                " service message(s).  Please check service (id=" + tracker.serviceId() +
                ") determinism around the use of Cluster::offer";
            errorHandler.onError(new AeronEvent(message, AeronException.Category.ERROR));
        }

        return nextServiceSessionId;
    }
}
