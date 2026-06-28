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

import io.aeron.Publication;
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.cluster.service.Cluster;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.*;

final class ServiceProxy implements AutoCloseable
{
    private static final int SEND_ATTEMPTS = 5;

    private final BufferClaim bufferClaim = new BufferClaim();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final JoinLogEncoder joinLogEncoder = new JoinLogEncoder();
    private final ClusterMembersResponseEncoder clusterMembersResponseEncoder = new ClusterMembersResponseEncoder();
    private final ServiceTerminationPositionEncoder serviceTerminationPositionEncoder =
        new ServiceTerminationPositionEncoder();
    private final ClusterMembersExtendedResponseEncoder clusterMembersExtendedResponseEncoder =
        new ClusterMembersExtendedResponseEncoder();
    private final RequestServiceAckEncoder requestServiceAckEncoder = new RequestServiceAckEncoder();
    private final ExpandableArrayBuffer expandableArrayBuffer = new ExpandableArrayBuffer();
    private final Publication publication;

    ServiceProxy(final Publication publication)
    {
        this.publication = publication;
    }

    public void close()
    {
        CloseHelper.close(publication);
    }

    void joinLog(
        final long logPosition,
        final long maxLogPosition,
        final int memberId,
        final int logSessionId,
        final int logStreamId,
        final boolean isStartup,
        final Cluster.Role role,
        final String channel,
        final boolean isStandby)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + JoinLogEncoder.BLOCK_LENGTH +
            JoinLogEncoder.logChannelHeaderLength() + channel.length();
        long position;

        int attempts = SEND_ATTEMPTS;
        do
        {
            position = publication.tryClaim(length, bufferClaim);
            if (position > 0)
            {
                joinLogEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .logPosition(logPosition)
                    .maxLogPosition(maxLogPosition)
                    .memberId(memberId)
                    .logSessionId(logSessionId)
                    .logStreamId(logStreamId)
                    .isStartup(isStartup ? BooleanType.TRUE : BooleanType.FALSE)
                    .role(role.code())
                    .isStandby(isStandby ? BooleanType.TRUE : BooleanType.FALSE)
                    .logChannel(channel);

                bufferClaim.commit();

                return;
            }

            checkResult(position, publication);
            if (Publication.BACK_PRESSURED == position)
            {
                Thread.yield();
            }
        }
        while (--attempts > 0);

        throw new ClusterException("failed to send join log request: " + Publication.errorString(position));
    }

    void clusterMembersResponse(
        final long correlationId, final int leaderMemberId, final String activeMembers)
    {
        final String passiveFollowers = "";
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ClusterMembersResponseEncoder.BLOCK_LENGTH +
            ClusterMembersResponseEncoder.activeMembersHeaderLength() + activeMembers.length() +
            ClusterMembersResponseEncoder.passiveFollowersHeaderLength() + passiveFollowers.length();

        long result;
        int attempts = SEND_ATTEMPTS;
        do
        {
            result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                clusterMembersResponseEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .correlationId(correlationId)
                    .leaderMemberId(leaderMemberId)
                    .activeMembers(activeMembers)
                    .passiveFollowers(passiveFollowers);

                bufferClaim.commit();

                return;
            }

            if (Publication.BACK_PRESSURED == result)
            {
                Thread.yield();
            }
        }
        while (--attempts > 0);

        throw new ClusterException("failed to send cluster members response: result=" + result);
    }

    void clusterMembersExtendedResponse(
        final long correlationId,
        final long currentTimeNs,
        final int leaderMemberId,
        final int memberId,
        final ClusterMember[] activeMembers)
    {
        clusterMembersExtendedResponseEncoder
            .wrapAndApplyHeader(expandableArrayBuffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .currentTimeNs(currentTimeNs)
            .leaderMemberId(leaderMemberId)
            .memberId(memberId);

        final ClusterMembersExtendedResponseEncoder.ActiveMembersEncoder activeMembersEncoder =
            clusterMembersExtendedResponseEncoder.activeMembersCount(activeMembers.length);
        for (final ClusterMember member : activeMembers)
        {
            activeMembersEncoder.next()
                .leadershipTermId(member.leadershipTermId())
                .logPosition(member.logPosition())
                .timeOfLastAppendNs(member.timeOfLastAppendPositionNs())
                .memberId(member.id())
                .ingressEndpoint(member.ingressEndpoint())
                .consensusEndpoint(member.consensusEndpoint())
                .logEndpoint(member.logEndpoint())
                .catchupEndpoint(member.catchupEndpoint())
                .archiveEndpoint(member.archiveEndpoint());
        }

        clusterMembersExtendedResponseEncoder.passiveMembersCount(0);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + clusterMembersExtendedResponseEncoder.encodedLength();

        long result;
        int attempts = SEND_ATTEMPTS;
        do
        {
            result = publication.offer(expandableArrayBuffer, 0, length, null);
            if (result > 0)
            {
                return;
            }

            if (Publication.BACK_PRESSURED == result)
            {
                Thread.yield();
            }
        }
        while (--attempts > 0);

        throw new ClusterException("failed to send cluster members extended response: result=" + result);
    }

    void terminationPosition(final long logPosition, final ErrorHandler errorHandler)
    {
        if (!publication.isClosed())
        {
            final int length = MessageHeaderDecoder.ENCODED_LENGTH + ServiceTerminationPositionEncoder.BLOCK_LENGTH;
            long result;

            int attempts = SEND_ATTEMPTS;
            do
            {
                result = publication.tryClaim(length, bufferClaim);
                if (result > 0)
                {
                    serviceTerminationPositionEncoder
                        .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                        .logPosition(logPosition);

                    bufferClaim.commit();

                    return;
                }

                if (Publication.BACK_PRESSURED == result)
                {
                    Thread.yield();
                }
            }
            while (--attempts > 0);

            errorHandler.onError(new ClusterEvent(
                "failed to send service termination position: result=" + Publication.errorString(result)));
        }
    }

    void requestServiceAck(final long logPosition)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + RequestServiceAckEncoder.BLOCK_LENGTH;

        long result;
        int attempts = SEND_ATTEMPTS;
        do
        {
            result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                requestServiceAckEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .logPosition(logPosition);

                bufferClaim.commit();

                return;
            }

            if (Publication.BACK_PRESSURED == result)
            {
                Thread.yield();
            }
        }
        while (--attempts > 0);

        throw new ClusterException("failed to send request for service ack: result=" + Publication.errorString(result));
    }

    private static void checkResult(final long position, final Publication publication)
    {
        if (Publication.NOT_CONNECTED == position)
        {
            throw new ClusterException("publication is not connected");
        }

        if (Publication.CLOSED == position)
        {
            throw new ClusterException("publication is closed");
        }

        if (Publication.MAX_POSITION_EXCEEDED == position)
        {
            throw new ClusterException("publication at max position: term-length=" + publication.termBufferLength());
        }
    }
}
