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
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

final class ConsensusModuleAdapter implements AutoCloseable
{
    private static final int FRAGMENT_LIMIT = 25;

    private final Subscription subscription;
    private final ConsensusModuleAgent consensusModuleAgent;
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionMessageHeaderDecoder sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
    private final ScheduleTimerDecoder scheduleTimerDecoder = new ScheduleTimerDecoder();
    private final CancelTimerDecoder cancelTimerDecoder = new CancelTimerDecoder();
    private final ServiceAckDecoder serviceAckDecoder = new ServiceAckDecoder();
    private final CloseSessionDecoder closeSessionDecoder = new CloseSessionDecoder();
    private final ClusterMembersQueryDecoder clusterMembersQueryDecoder = new ClusterMembersQueryDecoder();
    private final ControlledFragmentAssembler fragmentAssembler = new ControlledFragmentAssembler(this::onFragment);

    ConsensusModuleAdapter(final Subscription subscription, final ConsensusModuleAgent consensusModuleAgent)
    {
        this.subscription = subscription;
        this.consensusModuleAgent = consensusModuleAgent;
    }

    @Override
    public void close()
    {
        subscription.close();
    }

    int poll()
    {
        return subscription.controlledPoll(fragmentAssembler, FRAGMENT_LIMIT);
    }

    @SuppressWarnings("MethodLength")
    private ControlledFragmentHandler.Action onFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        ControlledFragmentHandler.Action action = ControlledFragmentHandler.Action.CONTINUE;

        switch (messageHeaderDecoder.templateId())
        {
            case SessionMessageHeaderDecoder.TEMPLATE_ID:
                sessionMessageHeaderDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onServiceMessage(
                    sessionMessageHeaderDecoder.clusterSessionId(),
                    buffer,
                    offset + AeronCluster.SESSION_HEADER_LENGTH,
                    length - AeronCluster.SESSION_HEADER_LENGTH);
                break;

            case CloseSessionDecoder.TEMPLATE_ID:
                closeSessionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onServiceCloseSession(closeSessionDecoder.clusterSessionId());
                break;

            case ScheduleTimerDecoder.TEMPLATE_ID:
                scheduleTimerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onScheduleTimer(
                    scheduleTimerDecoder.correlationId(),
                    scheduleTimerDecoder.deadline());
                break;

            case CancelTimerDecoder.TEMPLATE_ID:
                cancelTimerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onCancelTimer(cancelTimerDecoder.correlationId());
                break;

            case ServiceAckDecoder.TEMPLATE_ID:
                serviceAckDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onServiceAck(
                    serviceAckDecoder.logPosition(),
                    serviceAckDecoder.timestamp(),
                    serviceAckDecoder.ackId(),
                    serviceAckDecoder.relevantId(),
                    serviceAckDecoder.serviceId());

                action = ControlledFragmentHandler.Action.BREAK;
                break;

            case ClusterMembersQueryDecoder.TEMPLATE_ID:
                clusterMembersQueryDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onClusterMembersQuery(
                    clusterMembersQueryDecoder.correlationId(),
                    BooleanType.TRUE == clusterMembersQueryDecoder.extended());
                break;
        }

        return action;
    }
}
