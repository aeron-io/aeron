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

import io.aeron.cluster.client.ClusterEvent;
import io.aeron.cluster.client.ClusterException;
import org.agrona.concurrent.errors.DistinctErrorLog;

import java.util.ArrayDeque;

import static io.aeron.Aeron.NULL_VALUE;

/**
 * State holder for ACKs from each of the {@link io.aeron.cluster.service.ClusteredService}s.
 */
final class ServiceAck
{
    static final ServiceAck[] EMPTY_SERVICE_ACKS = new ServiceAck[0];

    private final long ackId;
    private final long logPosition;
    private final long relevantId;

    ServiceAck(final long ackId, final long logPosition, final long relevantId)
    {
        this.logPosition = logPosition;
        this.ackId = ackId;
        this.relevantId = relevantId;
    }

    long ackId()
    {
        return ackId;
    }

    long logPosition()
    {
        return logPosition;
    }

    long relevantId()
    {
        return relevantId;
    }

    static boolean hasReached(final long logPosition, final long ackId, final ArrayDeque<ServiceAck>[] queues)
    {
        for (int serviceId = 0, serviceCount = queues.length; serviceId < serviceCount; serviceId++)
        {
            final ServiceAck serviceAck = queues[serviceId].peek();
            if (null == serviceAck)
            {
                return false;
            }

            if (serviceAck.ackId != ackId || serviceAck.logPosition != logPosition)
            {
                throw new ClusterException(
                    "ack out of sequence: expected [ackId=" + ackId + ", logPosition=" + logPosition + "] vs " +
                    "received [ackId=" + serviceAck.ackId + ", logPosition=" + serviceAck.logPosition +
                    ", relevantId=" + serviceAck.relevantId + ", serviceId=" + serviceId + "]");
            }
        }

        return true;
    }

    static void removeHead(final ArrayDeque<ServiceAck>[] queues)
    {
        for (final ArrayDeque<ServiceAck> queue : queues)
        {
            queue.pollFirst();
        }
    }

    static ServiceAck[] pollServiceAcks(final ArrayDeque<ServiceAck>[] serviceAckQueues)
    {
        final ServiceAck[] serviceAcks = new ServiceAck[serviceAckQueues.length];
        for (int id = 0, length = serviceAckQueues.length; id < length; id++)
        {
            final ServiceAck serviceAck = serviceAckQueues[id].pollFirst();
            if (null == serviceAck)
            {
                throw new ClusterException("invalid ack for serviceId=" + id);
            }

            serviceAcks[id] = serviceAck;
        }

        return serviceAcks;
    }

    static ServiceAck[] pollServiceAcks(
        final long logPosition,
        final int serviceId,
        final ArrayDeque<ServiceAck>[] serviceAckQueues)
    {
        final ServiceAck[] serviceAcks = new ServiceAck[serviceAckQueues.length];
        for (int id = 0, length = serviceAckQueues.length; id < length; id++)
        {
            final ServiceAck serviceAck = serviceAckQueues[id].pollFirst();
            if (null == serviceAck || serviceAck.logPosition() != logPosition)
            {
                throw new ClusterException(
                    "invalid ack for serviceId=" + serviceId + " logPosition=" + logPosition + " " + serviceAck);
            }

            serviceAcks[id] = serviceAck;
        }

        return serviceAcks;
    }

    static boolean areAllRelevantIdsNonNull(
        final String message,
        final ServiceAck[] serviceAcks,
        final DistinctErrorLog distinctErrorLog)
    {
        boolean isSetComplete = true;

        for (int serviceId = serviceAcks.length - 1; serviceId >= 0; serviceId--)
        {
            if (NULL_VALUE == serviceAcks[serviceId].relevantId())
            {
                distinctErrorLog.record(new ClusterEvent("service=" + serviceId + " " + message));
                isSetComplete = false;
            }
        }

        return isSetComplete;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    static ArrayDeque<ServiceAck>[] newArrayOfQueues(final int serviceCount)
    {
        final ArrayDeque<ServiceAck>[] queues = new ArrayDeque[serviceCount];

        for (int serviceId = 0; serviceId < serviceCount; serviceId++)
        {
            queues[serviceId] = new ArrayDeque<>();
        }

        return queues;
    }

    public String toString()
    {
        return "ServiceAck{" +
            "ackId=" + ackId +
            ", logPosition=" + logPosition +
            ", relevantId=" + relevantId +
            '}';
    }
}
