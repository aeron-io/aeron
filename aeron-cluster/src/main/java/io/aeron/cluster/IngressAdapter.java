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

import io.aeron.ControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.collections.ArrayUtil;

class IngressAdapter implements AutoCloseable
{
    private final int fragmentPollLimit;
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionConnectRequestDecoder connectRequestDecoder = new SessionConnectRequestDecoder();
    private final SessionCloseRequestDecoder closeRequestDecoder = new SessionCloseRequestDecoder();
    private final SessionMessageHeaderDecoder sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
    private final SessionKeepAliveDecoder sessionKeepAliveDecoder = new SessionKeepAliveDecoder();
    private final ChallengeResponseDecoder challengeResponseDecoder = new ChallengeResponseDecoder();
    private final AdminRequestDecoder adminRequestDecoder = new AdminRequestDecoder();
    private final ControlledFragmentAssembler udpFragmentAssembler = new ControlledFragmentAssembler(this::onMessage);
    private final ControlledFragmentAssembler ipcFragmentAssembler = new ControlledFragmentAssembler(this::onMessage);
    private final ConsensusModuleAgent consensusModuleAgent;
    private Subscription subscription;
    private Subscription ipcSubscription;

    IngressAdapter(final int fragmentPollLimit, final ConsensusModuleAgent consensusModuleAgent)
    {
        this.fragmentPollLimit = fragmentPollLimit;
        this.consensusModuleAgent = consensusModuleAgent;
    }

    public void close()
    {
        final Subscription subscription = this.subscription;
        final Subscription ipcSubscription = this.ipcSubscription;

        this.subscription = null;
        this.ipcSubscription = null;

        if (null != subscription)
        {
            subscription.close();
        }

        if (null != ipcSubscription)
        {
            ipcSubscription.close();
        }

        udpFragmentAssembler.clear();
        ipcFragmentAssembler.clear();
    }

    @SuppressWarnings("MethodLength")
    public ControlledFragmentHandler.Action onMessage(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        final int templateId = messageHeaderDecoder.templateId();
        final int actingVersion = messageHeaderDecoder.version();
        final int actingBlockLength = messageHeaderDecoder.blockLength();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            return consensusModuleAgent.onExtensionMessage(
                actingBlockLength, templateId, schemaId, actingVersion, buffer, offset, length, header);
        }

        if (templateId == SessionMessageHeaderDecoder.TEMPLATE_ID)
        {
            sessionMessageHeaderDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                actingVersion);

            return consensusModuleAgent.onIngressMessage(
                sessionMessageHeaderDecoder.leadershipTermId(),
                sessionMessageHeaderDecoder.clusterSessionId(),
                buffer,
                offset + AeronCluster.SESSION_HEADER_LENGTH,
                length - AeronCluster.SESSION_HEADER_LENGTH);
        }

        switch (templateId)
        {
            case SessionConnectRequestDecoder.TEMPLATE_ID:
            {
                connectRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    actingVersion);

                final String responseChannel = connectRequestDecoder.responseChannel();
                final int credentialsLength = connectRequestDecoder.encodedCredentialsLength();
                byte[] credentials = ArrayUtil.EMPTY_BYTE_ARRAY;
                if (credentialsLength > 0)
                {
                    credentials = new byte[credentialsLength];
                    connectRequestDecoder.getEncodedCredentials(credentials, 0, credentialsLength);
                }

                consensusModuleAgent.onSessionConnect(
                    connectRequestDecoder.correlationId(),
                    connectRequestDecoder.responseStreamId(),
                    connectRequestDecoder.version(),
                    responseChannel,
                    credentials);
                break;
            }

            case SessionCloseRequestDecoder.TEMPLATE_ID:
            {
                closeRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    actingVersion);

                consensusModuleAgent.onSessionClose(
                    closeRequestDecoder.leadershipTermId(),
                    closeRequestDecoder.clusterSessionId());
                break;
            }

            case SessionKeepAliveDecoder.TEMPLATE_ID:
            {
                sessionKeepAliveDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    actingVersion);

                consensusModuleAgent.onSessionKeepAlive(
                    sessionKeepAliveDecoder.leadershipTermId(),
                    sessionKeepAliveDecoder.clusterSessionId());
                break;
            }

            case ChallengeResponseDecoder.TEMPLATE_ID:
            {
                challengeResponseDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    actingVersion);

                final byte[] credentials = new byte[challengeResponseDecoder.encodedCredentialsLength()];
                challengeResponseDecoder.getEncodedCredentials(credentials, 0, credentials.length);

                consensusModuleAgent.onIngressChallengeResponse(
                    challengeResponseDecoder.correlationId(),
                    challengeResponseDecoder.clusterSessionId(),
                    credentials);
                break;
            }

            case AdminRequestDecoder.TEMPLATE_ID:
            {
                adminRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    actingVersion);

                final int payloadOffset = adminRequestDecoder.offset() +
                    adminRequestDecoder.encodedLength() +
                    AdminRequestDecoder.payloadHeaderLength();
                consensusModuleAgent.onAdminRequest(
                    adminRequestDecoder.leadershipTermId(),
                    adminRequestDecoder.clusterSessionId(),
                    adminRequestDecoder.correlationId(),
                    adminRequestDecoder.requestType(),
                    buffer,
                    payloadOffset,
                    adminRequestDecoder.payloadLength());
                break;
            }
        }

        return ControlledFragmentHandler.Action.CONTINUE;
    }

    void connect(final Subscription subscription, final Subscription ipcSubscription)
    {
        this.subscription = subscription;
        this.ipcSubscription = ipcSubscription;
    }

    int poll()
    {
        int fragmentsRead = 0;

        if (null != subscription)
        {
            fragmentsRead += subscription.controlledPoll(udpFragmentAssembler, fragmentPollLimit);
        }

        if (null != ipcSubscription)
        {
            fragmentsRead += ipcSubscription.controlledPoll(ipcFragmentAssembler, fragmentPollLimit);
        }

        return fragmentsRead;
    }

    void freeSessionBuffer(final int imageSessionId, final boolean isIpc)
    {
        if (isIpc)
        {
            ipcFragmentAssembler.freeSessionBuffer(imageSessionId);
        }
        else
        {
            udpFragmentAssembler.freeSessionBuffer(imageSessionId);
        }
    }
}
