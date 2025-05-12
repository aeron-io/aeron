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
package io.aeron.driver;

import io.aeron.ErrorCode;
import io.aeron.command.*;
import io.aeron.exceptions.ControlProtocolException;
import io.aeron.exceptions.StorageSpaceException;
import org.agrona.ErrorHandler;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.ControlledMessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import static io.aeron.ChannelUri.SPY_QUALIFIER;
import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.ErrorCode.GENERIC_ERROR;
import static io.aeron.ErrorCode.STORAGE_SPACE;
import static io.aeron.command.ControlProtocolEvents.*;

/**
 * Receives commands from Aeron clients and dispatches them to the {@link DriverConductor} for processing.
 */
final class ClientCommandAdapter implements ControlledMessageHandler
{
    private final PublicationMessageFlyweight publicationMsgFlyweight = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMsgFlyweight = new SubscriptionMessageFlyweight();
    private final CorrelatedMessageFlyweight correlatedMsgFlyweight = new CorrelatedMessageFlyweight();
    private final RemoveMessageFlyweight removeMsgFlyweight = new RemoveMessageFlyweight();
    private final DestinationMessageFlyweight destinationMsgFlyweight = new DestinationMessageFlyweight();
    private final CounterMessageFlyweight counterMsgFlyweight = new CounterMessageFlyweight();
    private final StaticCounterMessageFlyweight staticCounterMessageFlyweight = new StaticCounterMessageFlyweight();
    private final TerminateDriverFlyweight terminateDriverFlyweight = new TerminateDriverFlyweight();
    private final RejectImageFlyweight rejectImageFlyweight = new RejectImageFlyweight();
    private final DestinationByIdMessageFlyweight destinationByIdMessageFlyweight =
        new DestinationByIdMessageFlyweight();
    private final DriverConductor conductor;
    private final RingBuffer toDriverCommands;
    private final ClientProxy clientProxy;
    private final AtomicCounter errors;
    private final ErrorHandler errorHandler;

    ClientCommandAdapter(
        final AtomicCounter errors,
        final ErrorHandler errorHandler,
        final RingBuffer toDriverCommands,
        final ClientProxy clientProxy,
        final DriverConductor driverConductor)
    {
        this.errors = errors;
        this.errorHandler = errorHandler;
        this.toDriverCommands = toDriverCommands;
        this.clientProxy = clientProxy;
        this.conductor = driverConductor;
    }

    int receive()
    {
        return toDriverCommands.controlledRead(this, Configuration.COMMAND_DRAIN_LIMIT);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("MethodLength")
    public ControlledMessageHandler.Action onMessage(
        final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
    {
        long correlationId = 0;

        if (conductor.notAcceptingClientCommands())
        {
            return Action.ABORT;
        }

        try
        {
            switch (msgTypeId)
            {
                case ADD_PUBLICATION:
                {
                    publicationMsgFlyweight.wrap(buffer, index);
                    publicationMsgFlyweight.validateLength(msgTypeId, length);

                    correlationId = publicationMsgFlyweight.correlationId();
                    addPublication(correlationId, false);
                    break;
                }

                case REMOVE_PUBLICATION:
                {
                    removeMsgFlyweight.wrap(buffer, index);
                    removeMsgFlyweight.validateLength(msgTypeId, length);

                    correlationId = removeMsgFlyweight.correlationId();
                    conductor.onRemovePublication(removeMsgFlyweight.registrationId(), correlationId);
                    break;
                }

                case ADD_EXCLUSIVE_PUBLICATION:
                {
                    publicationMsgFlyweight.wrap(buffer, index);
                    publicationMsgFlyweight.validateLength(msgTypeId, length);

                    correlationId = publicationMsgFlyweight.correlationId();
                    addPublication(correlationId, true);
                    break;
                }

                case ADD_SUBSCRIPTION:
                {
                    subscriptionMsgFlyweight.wrap(buffer, index);
                    subscriptionMsgFlyweight.validateLength(msgTypeId, length);

                    correlationId = subscriptionMsgFlyweight.correlationId();
                    final long clientId = subscriptionMsgFlyweight.clientId();
                    final int streamId = subscriptionMsgFlyweight.streamId();
                    final String channel = subscriptionMsgFlyweight.channel();

                    if (channel.startsWith(IPC_CHANNEL))
                    {
                        conductor.onAddIpcSubscription(channel, streamId, correlationId, clientId);
                    }
                    else if (channel.startsWith(SPY_QUALIFIER))
                    {
                        conductor.onAddSpySubscription(channel, streamId, correlationId, clientId);
                    }
                    else
                    {
                        conductor.onAddNetworkSubscription(channel, streamId, correlationId, clientId);
                    }
                    break;
                }

                case REMOVE_SUBSCRIPTION:
                {
                    removeMsgFlyweight.wrap(buffer, index);
                    removeMsgFlyweight.validateLength(msgTypeId, length);

                    correlationId = removeMsgFlyweight.correlationId();
                    conductor.onRemoveSubscription(removeMsgFlyweight.registrationId(), correlationId);
                    break;
                }

                case ADD_DESTINATION:
                {
                    destinationMsgFlyweight.wrap(buffer, index);
                    destinationMsgFlyweight.validateLength(msgTypeId, length);

                    correlationId = destinationMsgFlyweight.correlationId();
                    final long channelRegistrationId = destinationMsgFlyweight.registrationCorrelationId();
                    final String channel = destinationMsgFlyweight.channel();

                    conductor.onAddSendDestination(channelRegistrationId, channel, correlationId);
                    break;
                }

                case REMOVE_DESTINATION:
                {
                    destinationMsgFlyweight.wrap(buffer, index);
                    destinationMsgFlyweight.validateLength(msgTypeId, length);

                    correlationId = destinationMsgFlyweight.correlationId();
                    final long channelRegistrationId = destinationMsgFlyweight.registrationCorrelationId();
                    final String channel = destinationMsgFlyweight.channel();

                    conductor.onRemoveSendDestination(channelRegistrationId, channel, correlationId);
                    break;
                }

                case CLIENT_KEEPALIVE:
                {
                    correlatedMsgFlyweight.wrap(buffer, index);
                    correlatedMsgFlyweight.validateLength(msgTypeId, length);

                    conductor.onClientKeepalive(correlatedMsgFlyweight.clientId());
                    break;
                }

                case ADD_COUNTER:
                {
                    counterMsgFlyweight.wrap(buffer, index);
                    counterMsgFlyweight.validateLength(msgTypeId, length);

                    correlationId = counterMsgFlyweight.correlationId();
                    final long clientId = counterMsgFlyweight.clientId();
                    conductor.onAddCounter(
                        counterMsgFlyweight.typeId(),
                        buffer,
                        index + counterMsgFlyweight.keyBufferOffset(),
                        counterMsgFlyweight.keyBufferLength(),
                        buffer,
                        index + counterMsgFlyweight.labelBufferOffset(),
                        counterMsgFlyweight.labelBufferLength(),
                        correlationId,
                        clientId);
                    break;
                }

                case REMOVE_COUNTER:
                {
                    removeMsgFlyweight.wrap(buffer, index);
                    removeMsgFlyweight.validateLength(msgTypeId, length);

                    correlationId = removeMsgFlyweight.correlationId();
                    conductor.onRemoveCounter(removeMsgFlyweight.registrationId(), correlationId);
                    break;
                }

                case CLIENT_CLOSE:
                {
                    correlatedMsgFlyweight.wrap(buffer, index);
                    correlatedMsgFlyweight.validateLength(msgTypeId, length);

                    conductor.onClientClose(correlatedMsgFlyweight.clientId());
                    break;
                }

                case ADD_RCV_DESTINATION:
                {
                    destinationMsgFlyweight.wrap(buffer, index);
                    destinationMsgFlyweight.validateLength(msgTypeId, length);

                    correlationId = destinationMsgFlyweight.correlationId();
                    final long channelRegistrationId = destinationMsgFlyweight.registrationCorrelationId();
                    final String channel = destinationMsgFlyweight.channel();

                    conductor.onAddRcvDestination(channelRegistrationId, channel, correlationId);
                    break;
                }

                case REMOVE_RCV_DESTINATION:
                {
                    destinationMsgFlyweight.wrap(buffer, index);
                    destinationMsgFlyweight.validateLength(msgTypeId, length);

                    correlationId = destinationMsgFlyweight.correlationId();
                    final long channelRegistrationId = destinationMsgFlyweight.registrationCorrelationId();
                    final String channel = destinationMsgFlyweight.channel();

                    conductor.onRemoveRcvDestination(channelRegistrationId, channel, correlationId);
                    break;
                }

                case TERMINATE_DRIVER:
                {
                    terminateDriverFlyweight.wrap(buffer, index);
                    terminateDriverFlyweight.validateLength(msgTypeId, length);

                    conductor.onTerminateDriver(
                        buffer,
                        terminateDriverFlyweight.tokenBufferOffset(),
                        terminateDriverFlyweight.tokenBufferLength());
                    break;
                }

                case ADD_STATIC_COUNTER:
                {
                    staticCounterMessageFlyweight.wrap(buffer, index);
                    staticCounterMessageFlyweight.validateLength(msgTypeId, length);

                    correlationId = staticCounterMessageFlyweight.correlationId();
                    final long clientId = staticCounterMessageFlyweight.clientId();
                    conductor.onAddStaticCounter(
                        staticCounterMessageFlyweight.typeId(),
                        buffer,
                        index + staticCounterMessageFlyweight.keyBufferOffset(),
                        staticCounterMessageFlyweight.keyBufferLength(),
                        buffer,
                        index + staticCounterMessageFlyweight.labelBufferOffset(),
                        staticCounterMessageFlyweight.labelBufferLength(),
                        staticCounterMessageFlyweight.registrationId(),
                        correlationId,
                        clientId);
                    break;
                }

                case REJECT_IMAGE:
                {
                    rejectImageFlyweight.wrap(buffer, index);
                    rejectImageFlyweight.validateLength(msgTypeId, length);
                    correlationId = rejectImageFlyweight.correlationId();

                    conductor.onRejectImage(
                        rejectImageFlyweight.correlationId(),
                        rejectImageFlyweight.imageCorrelationId(),
                        rejectImageFlyweight.position(),
                        rejectImageFlyweight.reason());
                    break;
                }

                case REMOVE_DESTINATION_BY_ID:
                {
                    destinationByIdMessageFlyweight.wrap(buffer, index);
                    destinationByIdMessageFlyweight.validateLength(msgTypeId, length);

                    correlationId = destinationByIdMessageFlyweight.correlationId();
                    conductor.onRemoveSendDestination(
                        destinationByIdMessageFlyweight.resourceRegistrationId(),
                        destinationByIdMessageFlyweight.destinationRegistrationId(),
                        destinationByIdMessageFlyweight.correlationId());

                    break;
                }

                default:
                {
                    final ControlProtocolException ex = new ControlProtocolException(
                        ErrorCode.UNKNOWN_COMMAND_TYPE_ID, "command typeId=" + msgTypeId);

                    onError(correlationId, ex);
                }
            }
        }
        catch (final Exception ex)
        {
            onError(correlationId, ex);
        }

        return Action.CONTINUE;
    }

    private void addPublication(final long correlationId, final boolean isExclusive)
    {
        final long clientId = publicationMsgFlyweight.clientId();
        final int streamId = publicationMsgFlyweight.streamId();
        final String channel = publicationMsgFlyweight.channel();

        if (channel.startsWith(IPC_CHANNEL))
        {
            conductor.onAddIpcPublication(channel, streamId, correlationId, clientId, isExclusive);
        }
        else
        {
            conductor.onAddNetworkPublication(channel, streamId, correlationId, clientId, isExclusive);
        }
    }

    void onError(final long correlationId, final Exception error)
    {
        if (!errors.isClosed())
        {
            errors.increment();
        }

        errorHandler.onError(error);

        if (error instanceof ControlProtocolException)
        {
            clientProxy.onError(correlationId, ((ControlProtocolException)error).errorCode(), error.getMessage());
        }
        else if (error instanceof StorageSpaceException || StorageSpaceException.isStorageSpaceError(error))
        {
            clientProxy.onError(correlationId, STORAGE_SPACE, error.getMessage());
        }
        else
        {
            final String errorMessage = error.getClass().getName() + " : " + error.getMessage();
            clientProxy.onError(correlationId, GENERIC_ERROR, errorMessage);
        }
    }
}
