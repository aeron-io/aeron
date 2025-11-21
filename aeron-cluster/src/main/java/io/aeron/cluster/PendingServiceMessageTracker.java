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
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.cluster.codecs.SessionMessageHeaderEncoder;
import io.aeron.cluster.service.ClusterClock;
import io.aeron.exceptions.AeronException;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableRingBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableInteger;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;

final class PendingServiceMessageTracker
{
    private static final int SERVICE_MESSAGE_LIMIT = 20;

    private final int serviceId;
    private int pendingMessageHeadOffset = 0;
    private int uncommittedMessages = 0;
    private long nextServiceSessionId;
    private long logServiceSessionId;
    private long leadershipTermId = NULL_VALUE;

    private final Counter commitPosition;
    private final LogPublisher logPublisher;
    private final ClusterClock clusterClock;
    private final ExpandableRingBuffer pendingMessages = new ExpandableRingBuffer();
    private final ExpandableRingBuffer.MessageConsumer messageAppender = this::messageAppender;
    private final ExpandableRingBuffer.MessageConsumer leaderMessageSweeper = this::leaderMessageSweeper;
    private final ExpandableRingBuffer.MessageConsumer followerMessageSweeper = this::followerMessageSweeper;

    PendingServiceMessageTracker(
        final int serviceId,
        final Counter commitPosition,
        final LogPublisher logPublisher,
        final ClusterClock clusterClock)
    {
        this.serviceId = serviceId;
        this.commitPosition = commitPosition;
        this.logPublisher = logPublisher;
        this.clusterClock = clusterClock;

        logServiceSessionId = serviceSessionId(serviceId, Long.MIN_VALUE);
        nextServiceSessionId = logServiceSessionId + 1;
    }

    void leadershipTermId(final long leadershipTermId)
    {
        this.leadershipTermId = leadershipTermId;
    }

    int serviceId()
    {
        return serviceId;
    }

    long nextServiceSessionId()
    {
        return nextServiceSessionId;
    }

    long logServiceSessionId()
    {
        return logServiceSessionId;
    }

    void enqueueMessage(final MutableDirectBuffer buffer, final int offset, final int length)
    {
        final long clusterSessionId = nextServiceSessionId++;
        if (clusterSessionId > logServiceSessionId)
        {
            final int headerOffset = offset - SessionMessageHeaderDecoder.BLOCK_LENGTH;
            final int clusterSessionIdOffset =
                headerOffset + SessionMessageHeaderDecoder.clusterSessionIdEncodingOffset();
            final int timestampOffset = headerOffset + SessionMessageHeaderDecoder.timestampEncodingOffset();

            buffer.putLong(clusterSessionIdOffset, clusterSessionId, SessionMessageHeaderDecoder.BYTE_ORDER);
            buffer.putLong(timestampOffset, Long.MAX_VALUE, SessionMessageHeaderDecoder.BYTE_ORDER);
            if (!pendingMessages.append(buffer, offset - SESSION_HEADER_LENGTH, length + SESSION_HEADER_LENGTH))
            {
                throw new ClusterException("pending service message buffer at capacity=" + pendingMessages.size() +
                    " for serviceId=" + serviceId);
            }
        }
    }

    void sweepFollowerMessages(final long clusterSessionId)
    {
        logServiceSessionId = clusterSessionId;
        pendingMessages.consume(followerMessageSweeper, Integer.MAX_VALUE);
    }

    void sweepLeaderMessages()
    {
        if (uncommittedMessages > 0)
        {
            pendingMessageHeadOffset -= pendingMessages.consume(leaderMessageSweeper, Integer.MAX_VALUE);
            pendingMessageHeadOffset = Math.max(pendingMessageHeadOffset, 0);
        }
    }

    void restoreUncommittedMessages()
    {
        if (uncommittedMessages > 0)
        {
            pendingMessages.consume(leaderMessageSweeper, Integer.MAX_VALUE);
            pendingMessages.forEach(PendingServiceMessageTracker::messageReset, Integer.MAX_VALUE);
            uncommittedMessages = 0;
            pendingMessageHeadOffset = 0;
        }
    }

    void appendMessage(final DirectBuffer buffer, final int offset, final int length)
    {
        pendingMessages.append(buffer, offset, length);
    }

    void loadState(final long nextServiceSessionId, final long logServiceSessionId, final int pendingMessageCapacity)
    {
        this.nextServiceSessionId = nextServiceSessionId;
        this.logServiceSessionId = logServiceSessionId;
        pendingMessages.reset(pendingMessageCapacity);
    }

    int poll()
    {
        return pendingMessages.forEach(pendingMessageHeadOffset, messageAppender, SERVICE_MESSAGE_LIMIT);
    }

    int size()
    {
        return pendingMessages.size();
    }

    void verify()
    {
        final MutableInteger messageCount = new MutableInteger();
        final ExpandableRingBuffer.MessageConsumer messageConsumer =
            (buffer, offset, length, headOffset) ->
            {
                messageCount.increment();

                final int headerOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
                final int clusterSessionIdOffset =
                    headerOffset + SessionMessageHeaderDecoder.clusterSessionIdEncodingOffset();

                final long clusterSessionId = buffer.getLong(
                    clusterSessionIdOffset, SessionMessageHeaderDecoder.BYTE_ORDER);

                if (clusterSessionId != (logServiceSessionId + messageCount.get()))
                {
                    throw new ClusterException("snapshot has incorrect pending message:" +
                        " serviceId=" + serviceId +
                        " nextServiceSessionId=" + nextServiceSessionId +
                        " logServiceSessionId=" + logServiceSessionId +
                        " clusterSessionId=" + clusterSessionId +
                        " pendingMessageIndex=" + messageCount.get(),
                        AeronException.Category.FATAL);
                }

                return true;
            };

        pendingMessages.forEach(messageConsumer, Integer.MAX_VALUE);

        if (nextServiceSessionId != (logServiceSessionId + messageCount.get() + 1))
        {
            throw new ClusterException("snapshot has incorrect pending message state:" +
                " serviceId=" + serviceId +
                " nextServiceSessionId=" + nextServiceSessionId +
                " logServiceSessionId=" + logServiceSessionId +
                " pendingMessageCount=" + messageCount.get(),
                AeronException.Category.FATAL);
        }
    }

    void reset()
    {
        pendingMessages.forEach(PendingServiceMessageTracker::messageReset, Integer.MAX_VALUE);
    }

    ExpandableRingBuffer pendingMessages()
    {
        return pendingMessages;
    }

    private boolean messageAppender(
        final MutableDirectBuffer buffer, final int offset, final int length, final int headOffset)
    {
        final int headerOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        final int clusterSessionIdOffset = headerOffset + SessionMessageHeaderDecoder.clusterSessionIdEncodingOffset();
        final int timestampOffset = headerOffset + SessionMessageHeaderDecoder.timestampEncodingOffset();
        final long clusterSessionId = buffer.getLong(clusterSessionIdOffset, SessionMessageHeaderDecoder.BYTE_ORDER);

        final long appendPosition = logPublisher.appendMessage(
            leadershipTermId,
            clusterSessionId,
            clusterClock.time(),
            buffer,
            offset + SESSION_HEADER_LENGTH,
            length - SESSION_HEADER_LENGTH);

        if (appendPosition > 0)
        {
            ++uncommittedMessages;
            pendingMessageHeadOffset = headOffset;
            buffer.putLong(timestampOffset, appendPosition, SessionMessageHeaderEncoder.BYTE_ORDER);

            return true;
        }

        return false;
    }

    private static boolean messageReset(
        final MutableDirectBuffer buffer, final int offset, final int length, final int headOffset)
    {
        final int timestampOffset = offset +
            MessageHeaderDecoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.timestampEncodingOffset();
        final long appendPosition = buffer.getLong(timestampOffset, SessionMessageHeaderDecoder.BYTE_ORDER);

        if (appendPosition < Long.MAX_VALUE)
        {
            buffer.putLong(timestampOffset, Long.MAX_VALUE, SessionMessageHeaderEncoder.BYTE_ORDER);
            return true;
        }

        return false;
    }

    private boolean leaderMessageSweeper(
        final MutableDirectBuffer buffer, final int offset, final int length, final int headOffset)
    {
        final int headerOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        final int clusterSessionIdOffset = headerOffset + SessionMessageHeaderDecoder.clusterSessionIdEncodingOffset();
        final int timestampOffset = headerOffset + SessionMessageHeaderDecoder.timestampEncodingOffset();
        final long appendPosition = buffer.getLong(timestampOffset, SessionMessageHeaderDecoder.BYTE_ORDER);

        if (commitPosition.getPlain() >= appendPosition)
        {
            logServiceSessionId = buffer.getLong(clusterSessionIdOffset, SessionMessageHeaderDecoder.BYTE_ORDER);
            --uncommittedMessages;

            return true;
        }

        return false;
    }

    private boolean followerMessageSweeper(
        final MutableDirectBuffer buffer, final int offset, final int length, final int headOffset)
    {
        final int clusterSessionIdOffset = offset +
            MessageHeaderDecoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.clusterSessionIdEncodingOffset();

        return buffer.getLong(clusterSessionIdOffset, SessionMessageHeaderDecoder.BYTE_ORDER) <= logServiceSessionId;
    }

    static int serviceIdFromLogMessage(final long clusterSessionId)
    {
        return ((int)(clusterSessionId >>> 56)) & 0x7F;
    }

    /**
     * Services use different approach for communicating the serviceId, this method extracts the serviceId from a
     * cluster session id sent via an inter-service message.
     *
     * @param clusterSessionId passed in on an inter-service message.
     * @return the associated serviceId.
     */
    static int serviceIdFromServiceMessage(final long clusterSessionId)
    {
        return (int)clusterSessionId;
    }

    static long serviceSessionId(final int serviceId, final long sessionId)
    {
        return ((long)serviceId << 56) | sessionId;
    }
}
