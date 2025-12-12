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
package io.aeron.cluster.client;

import io.aeron.Aeron;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.collections.ArrayUtil;

/**
 * Poller for the egress from a cluster to capture administration message details.
 */
public final class EgressPoller implements ControlledFragmentHandler
{
    private final int fragmentLimit;
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionEventDecoder sessionEventDecoder = new SessionEventDecoder();
    private final ChallengeDecoder challengeDecoder = new ChallengeDecoder();
    private final NewLeaderEventDecoder newLeaderEventDecoder = new NewLeaderEventDecoder();
    private final SessionMessageHeaderDecoder sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
    private final ControlledFragmentAssembler fragmentAssembler = new ControlledFragmentAssembler(this);
    private final Subscription subscription;

    private Image egressImage;
    private long clusterSessionId = Aeron.NULL_VALUE;
    private long correlationId = Aeron.NULL_VALUE;
    private long leadershipTermId = Aeron.NULL_VALUE;
    private int leaderMemberId = Aeron.NULL_VALUE;
    private int templateId = Aeron.NULL_VALUE;
    private int version = 0;
    private long leaderHeartbeatTimeoutNs = Aeron.NULL_VALUE;
    private boolean isPollComplete = false;
    private EventCode eventCode;
    private String detail = "";
    private byte[] encodedChallenge;

    /**
     * Construct a poller on the egress subscription.
     *
     * @param subscription  for egress from the cluster.
     * @param fragmentLimit for each poll operation.
     */
    public EgressPoller(final Subscription subscription, final int fragmentLimit)
    {
        this.subscription = subscription;
        this.fragmentLimit = fragmentLimit;
    }

    /**
     * Get the {@link Subscription} used for polling events.
     *
     * @return the {@link Subscription} used for polling events.
     */
    public Subscription subscription()
    {
        return subscription;
    }

    /**
     * {@link Image} for the egress response from the cluster which can be used for connection tracking.
     *
     * @return {@link Image} for the egress response from the cluster which can be used for connection tracking.
     */
    public Image egressImage()
    {
        return egressImage;
    }

    /**
     * Get the template id of the last received event.
     *
     * @return the template id of the last received event.
     */
    public int templateId()
    {
        return templateId;
    }

    /**
     * Cluster session id of the last polled event or {@link Aeron#NULL_VALUE} if poll returned nothing.
     *
     * @return cluster session id of the last polled event or {@link Aeron#NULL_VALUE} if not returned.
     */
    public long clusterSessionId()
    {
        return clusterSessionId;
    }

    /**
     * Correlation id of the last polled event or {@link Aeron#NULL_VALUE} if poll returned nothing.
     *
     * @return correlation id of the last polled event or {@link Aeron#NULL_VALUE} if not returned.
     */
    public long correlationId()
    {
        return correlationId;
    }

    /**
     * Leadership term id of the last polled event or {@link Aeron#NULL_VALUE} if poll returned nothing.
     *
     * @return leadership term id of the last polled event or {@link Aeron#NULL_VALUE} if not returned.
     */
    public long leadershipTermId()
    {
        return leadershipTermId;
    }

    /**
     * Leader cluster member id of the last polled event or {@link Aeron#NULL_VALUE} if poll returned nothing.
     *
     * @return leader cluster member id of the last polled event or {@link Aeron#NULL_VALUE} if poll returned nothing.
     */
    public int leaderMemberId()
    {
        return leaderMemberId;
    }

    /**
     * Get the event code returned from the last session event.
     *
     * @return the event code returned from the last session event.
     */
    public EventCode eventCode()
    {
        return eventCode;
    }

    /**
     * Version response from the server in semantic version form.
     *
     * @return response from the server in semantic version form.
     */
    public int version()
    {
        return version;
    }

    /**
     * Leader heartbeat timeout of the last polled event or {@link Aeron#NULL_VALUE} if not available.
     *
     * @return leader heartbeat timeout of the last polled event or {@link Aeron#NULL_VALUE} if not available.
     */
    public long leaderHeartbeatTimeoutNs()
    {
        return leaderHeartbeatTimeoutNs;
    }

    /**
     * Get the detail returned from the last session event.
     *
     * @return the detail returned from the last session event.
     */
    public String detail()
    {
        return detail;
    }

    /**
     * Get the encoded challenge in the last challenge.
     *
     * @return the encoded challenge in the last challenge or null if last message was not a challenge.
     */
    public byte[] encodedChallenge()
    {
        return encodedChallenge;
    }

    /**
     * Has the last polling action received a complete event?
     *
     * @return true if the last polling action received a complete event.
     */
    public boolean isPollComplete()
    {
        return isPollComplete;
    }

    /**
     * Was last message a challenge or not?
     *
     * @return true if last message was a challenge or false if not.
     */
    public boolean isChallenged()
    {
        return ChallengeDecoder.TEMPLATE_ID == templateId;
    }

    /**
     * Reset last captured value and poll the egress subscription for output.
     *
     * @return number of fragments consumed.
     */
    public int poll()
    {
        if (isPollComplete)
        {
            isPollComplete = false;
            clusterSessionId = Aeron.NULL_VALUE;
            correlationId = Aeron.NULL_VALUE;
            leadershipTermId = Aeron.NULL_VALUE;
            leaderMemberId = Aeron.NULL_VALUE;
            templateId = Aeron.NULL_VALUE;
            version = 0;
            leaderHeartbeatTimeoutNs = Aeron.NULL_VALUE;
            eventCode = null;
            detail = "";
            encodedChallenge = null;
        }

        return subscription.controlledPoll(fragmentAssembler, fragmentLimit);
    }

    /**
     * {@inheritDoc}
     */
    public ControlledFragmentAssembler.Action onFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        if (isPollComplete)
        {
            return Action.ABORT;
        }

        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            return Action.CONTINUE; // skip unknown schemas
        }

        templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case SessionMessageHeaderDecoder.TEMPLATE_ID:
                sessionMessageHeaderDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                leadershipTermId = sessionMessageHeaderDecoder.leadershipTermId();
                clusterSessionId = sessionMessageHeaderDecoder.clusterSessionId();
                isPollComplete = true;
                return Action.BREAK;

            case SessionEventDecoder.TEMPLATE_ID:
                sessionEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                clusterSessionId = sessionEventDecoder.clusterSessionId();
                correlationId = sessionEventDecoder.correlationId();
                leadershipTermId = sessionEventDecoder.leadershipTermId();
                leaderMemberId = sessionEventDecoder.leaderMemberId();
                eventCode = sessionEventDecoder.code();
                version = sessionEventDecoder.version();
                leaderHeartbeatTimeoutNs = leaderHeartbeatTimeoutNs(sessionEventDecoder);
                detail = sessionEventDecoder.detail();
                isPollComplete = true;
                egressImage = (Image)header.context();
                return Action.BREAK;

            case NewLeaderEventDecoder.TEMPLATE_ID:
                newLeaderEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                clusterSessionId = newLeaderEventDecoder.clusterSessionId();
                leadershipTermId = newLeaderEventDecoder.leadershipTermId();
                leaderMemberId = newLeaderEventDecoder.leaderMemberId();
                detail = newLeaderEventDecoder.ingressEndpoints();
                isPollComplete = true;
                egressImage = (Image)header.context();
                return Action.BREAK;

            case ChallengeDecoder.TEMPLATE_ID:
                challengeDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final int encodedChallengeLength = challengeDecoder.encodedChallengeLength();
                encodedChallenge =
                    0 == encodedChallengeLength ? ArrayUtil.EMPTY_BYTE_ARRAY : new byte[encodedChallengeLength];
                challengeDecoder.getEncodedChallenge(encodedChallenge, 0, encodedChallengeLength);

                clusterSessionId = challengeDecoder.clusterSessionId();
                correlationId = challengeDecoder.correlationId();
                isPollComplete = true;
                return Action.BREAK;
        }

        return Action.CONTINUE;
    }

    private static long leaderHeartbeatTimeoutNs(final SessionEventDecoder sessionEventDecoder)
    {
        final long leaderHeartbeatTimeoutNs = sessionEventDecoder.leaderHeartbeatTimeoutNs();

        if (leaderHeartbeatTimeoutNs == SessionEventDecoder.leaderHeartbeatTimeoutNsNullValue())
        {
            return Aeron.NULL_VALUE;
        }

        return leaderHeartbeatTimeoutNs;
    }
}
