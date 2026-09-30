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
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.AdminRequestType;
import io.aeron.cluster.codecs.AdminResponseCode;
import io.aeron.cluster.codecs.AdminResponseEncoder;
import io.aeron.cluster.codecs.ChallengeEncoder;
import io.aeron.cluster.codecs.ChallengeResponseEncoder;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.NewLeaderEventEncoder;
import io.aeron.cluster.codecs.SessionEventEncoder;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayUtil;

import static io.aeron.cluster.ClusterSession.MAX_ENCODED_MEMBERSHIP_QUERY_LENGTH;

class EgressPublisher
{
    private static final int SEND_ATTEMPTS = 3;

    private final BufferClaim bufferClaim = new BufferClaim();
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(MAX_ENCODED_MEMBERSHIP_QUERY_LENGTH);
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final SessionEventEncoder sessionEventEncoder = new SessionEventEncoder();
    private final ChallengeEncoder challengeEncoder = new ChallengeEncoder();
    private final NewLeaderEventEncoder newLeaderEventEncoder = new NewLeaderEventEncoder();
    private final AdminResponseEncoder adminResponseEncoder = new AdminResponseEncoder();
    private final long leaderHeartbeatTimeoutNs;

    EgressPublisher(final long leaderHeartbeatTimeoutNs)
    {
        this.leaderHeartbeatTimeoutNs = leaderHeartbeatTimeoutNs;
    }

    boolean sendEvent(
        final ClusterSession session,
        final long leadershipTermId,
        final int leaderMemberId,
        final EventCode code,
        final String detail)
    {
        final Publication responsePublication = session.responsePublication();
        if (null == responsePublication)
        {
            return false;
        }

        final int length = MessageHeaderEncoder.ENCODED_LENGTH +
            SessionEventEncoder.BLOCK_LENGTH +
            SessionEventEncoder.detailHeaderLength() +
            detail.length();

        if (length <= responsePublication.maxPayloadLength())
        {
            int attempts = SEND_ATTEMPTS;
            do
            {
                final long position = session.tryClaim(length, bufferClaim);
                if (position > 0)
                {
                    encodeSessionEvent(
                        bufferClaim.buffer(),
                        bufferClaim.offset(),
                        session,
                        leadershipTermId,
                        leaderMemberId,
                        code,
                        detail);

                    bufferClaim.commit();
                    return true;
                }
            }
            while (--attempts > 0);

            return false;
        }
        else
        {
            encodeSessionEvent(
                buffer,
                0,
                session,
                leadershipTermId,
                leaderMemberId,
                code,
                detail);
            return offerMessage(session, length);
        }
    }

    boolean sendChallenge(final ClusterSession session, final byte[] encodedChallenge)
    {
        final Publication responsePublication = session.responsePublication();
        if (null == responsePublication)
        {
            return false;
        }

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ChallengeResponseEncoder.BLOCK_LENGTH +
            ChallengeResponseEncoder.encodedCredentialsHeaderLength() + encodedChallenge.length;

        if (length <= responsePublication.maxPayloadLength())
        {
            int attempts = SEND_ATTEMPTS;
            do
            {
                final long position = session.tryClaim(length, bufferClaim);
                if (position > 0)
                {
                    encodeChallenge(bufferClaim.buffer(), bufferClaim.offset(), session, encodedChallenge);
                    bufferClaim.commit();
                    return true;
                }
            }
            while (--attempts > 0);

            return false;
        }
        else
        {
            encodeChallenge(buffer, 0, session, encodedChallenge);
            return offerMessage(session, length);
        }
    }

    boolean newLeader(
        final ClusterSession session,
        final long leadershipTermId,
        final int leaderMemberId,
        final String ingressEndpoints)
    {
        final Publication responsePublication = session.responsePublication();
        if (null == responsePublication)
        {
            return false;
        }

        final int length = MessageHeaderEncoder.ENCODED_LENGTH +
            NewLeaderEventEncoder.BLOCK_LENGTH +
            NewLeaderEventEncoder.ingressEndpointsHeaderLength() +
            ingressEndpoints.length();

        if (length <= responsePublication.maxPayloadLength())
        {
            int attempts = SEND_ATTEMPTS;
            do
            {
                final long position = session.tryClaim(length, bufferClaim);
                if (position > 0)
                {
                    encodeNewLeader(
                        bufferClaim.buffer(),
                        bufferClaim.offset(),
                        session,
                        leadershipTermId,
                        leaderMemberId,
                        ingressEndpoints);

                    bufferClaim.commit();

                    return true;
                }
            }
            while (--attempts > 0);

            return false;
        }
        else
        {
            encodeNewLeader(buffer, 0, session, leadershipTermId, leaderMemberId, ingressEndpoints);
            return offerMessage(session, length);
        }
    }

    boolean sendAdminResponse(
        final ClusterSession session,
        final long correlationId,
        final AdminRequestType adminRequestType,
        final AdminResponseCode responseCode,
        final String message)
    {
        final Publication responsePublication = session.responsePublication();
        if (null == responsePublication)
        {
            return false;
        }

        final int length = MessageHeaderEncoder.ENCODED_LENGTH +
            AdminResponseEncoder.BLOCK_LENGTH +
            AdminResponseEncoder.messageHeaderLength() +
            message.length() +
            AdminResponseEncoder.payloadHeaderLength();

        if (length <= responsePublication.maxPayloadLength())
        {
            int attempts = SEND_ATTEMPTS;
            do
            {
                final long position = session.tryClaim(length, bufferClaim);
                if (position > 0)
                {
                    encodeAdminResponse(
                        bufferClaim.buffer(),
                        bufferClaim.offset(),
                        session,
                        correlationId,
                        adminRequestType,
                        responseCode,
                        message);

                    bufferClaim.commit();

                    return true;
                }
            }
            while (--attempts > 0);

            return false;
        }
        else
        {
            encodeAdminResponse(buffer, 0, session, correlationId, adminRequestType, responseCode, message);
            return offerMessage(session, length);
        }
    }

    private void encodeSessionEvent(
        final MutableDirectBuffer dstBuffer,
        final int dstOffset,
        final ClusterSession session,
        final long leadershipTermId,
        final int leaderMemberId,
        final EventCode code,
        final String detail)
    {
        sessionEventEncoder
            .wrapAndApplyHeader(dstBuffer, dstOffset, messageHeaderEncoder)
            .clusterSessionId(session.id())
            .correlationId(session.correlationId())
            .leadershipTermId(leadershipTermId)
            .leaderMemberId(leaderMemberId)
            .code(code)
            .version(AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION)
            .leaderHeartbeatTimeoutNs(leaderHeartbeatTimeoutNs)
            .detail(detail);
    }

    private void encodeChallenge(
        final MutableDirectBuffer buffer,
        final int offset,
        final ClusterSession session,
        final byte[] encodedChallenge)
    {
        challengeEncoder
            .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
            .clusterSessionId(session.id())
            .correlationId(session.correlationId())
            .putEncodedChallenge(encodedChallenge, 0, encodedChallenge.length);
    }

    private void encodeNewLeader(
        final MutableDirectBuffer buffer,
        final int offset,
        final ClusterSession session,
        final long leadershipTermId,
        final int leaderMemberId,
        final String ingressEndpoints)
    {
        newLeaderEventEncoder
            .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
            .clusterSessionId(session.id())
            .leadershipTermId(leadershipTermId)
            .leaderMemberId(leaderMemberId)
            .ingressEndpoints(ingressEndpoints);
    }

    private void encodeAdminResponse(
        final MutableDirectBuffer buffer,
        final int offset,
        final ClusterSession session,
        final long correlationId,
        final AdminRequestType adminRequestType,
        final AdminResponseCode responseCode,
        final String message)
    {
        adminResponseEncoder
            .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
            .clusterSessionId(session.id())
            .correlationId(correlationId)
            .requestType(adminRequestType)
            .responseCode(responseCode)
            .message(message)
            .putPayload(ArrayUtil.EMPTY_BYTE_ARRAY, 0, 0);
    }

    private boolean offerMessage(final ClusterSession session, final int length)
    {
        int attempts = SEND_ATTEMPTS;
        do
        {
            final long position = session.offer(buffer, 0, length);
            if (position > 0)
            {
                return true;
            }
        }
        while (--attempts > 0);

        return false;
    }
}
