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
package io.aeron;

import io.aeron.command.*;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

import static io.aeron.ErrorCode.CHANNEL_ENDPOINT_ERROR;
import static io.aeron.command.ControlProtocolEvents.*;

/**
 * Analogue of {@link DriverProxy} on the client side for dispatching driver events to the client conductor.
 */
class DriverEventsAdapter implements MessageHandler
{
    private final ErrorResponseFlyweight errorResponse = new ErrorResponseFlyweight();
    private final PublicationErrorFrameFlyweight publicationErrorFrame = new PublicationErrorFrameFlyweight();
    private final PublicationBuffersReadyFlyweight publicationReady = new PublicationBuffersReadyFlyweight();
    private final SubscriptionReadyFlyweight subscriptionReady = new SubscriptionReadyFlyweight();
    private final ImageBuffersReadyFlyweight imageReady = new ImageBuffersReadyFlyweight();
    private final OperationSucceededFlyweight operationSucceeded = new OperationSucceededFlyweight();
    private final ImageMessageFlyweight imageMessage = new ImageMessageFlyweight();
    private final CounterUpdateFlyweight counterUpdate = new CounterUpdateFlyweight();
    private final ClientTimeoutFlyweight clientTimeout = new ClientTimeoutFlyweight();
    private final StaticCounterFlyweight staticCounter = new StaticCounterFlyweight();
    private final CopyBroadcastReceiver receiver;
    private final ClientConductor conductor;
    private final LongHashSet asyncCommandIdSet;
    private final long clientId;

    private long activeCorrelationId;
    private long receivedCorrelationId;
    private boolean isInvalid;

    DriverEventsAdapter(
        final long clientId,
        final CopyBroadcastReceiver receiver,
        final ClientConductor conductor,
        final LongHashSet asyncCommandIdSet)
    {
        this.clientId = clientId;
        this.receiver = receiver;
        this.conductor = conductor;
        this.asyncCommandIdSet = asyncCommandIdSet;
    }

    int receive(final long activeCorrelationId)
    {
        this.activeCorrelationId = activeCorrelationId;
        this.receivedCorrelationId = Aeron.NULL_VALUE;

        try
        {
            return receiver.receive(this);
        }
        catch (final IllegalStateException ex)
        {
            isInvalid = true;
            throw ex;
        }
    }

    long receivedCorrelationId()
    {
        return receivedCorrelationId;
    }

    boolean isInvalid()
    {
        return isInvalid;
    }

    long clientId()
    {
        return clientId;
    }

    @SuppressWarnings("MethodLength")
    public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
    {
        switch (msgTypeId)
        {
            case ON_ERROR:
            {
                errorResponse.wrap(buffer, index);

                final long correlationId = errorResponse.offendingCommandCorrelationId();
                final int errorCodeValue = errorResponse.errorCodeValue();
                final ErrorCode errorCode = ErrorCode.get(errorCodeValue);
                boolean notProcessed = true;

                if (CHANNEL_ENDPOINT_ERROR == errorCode)
                {
                    notProcessed = false;
                    conductor.onChannelEndpointError(correlationId, errorResponse.errorMessage());
                }
                else if (correlationId == activeCorrelationId)
                {
                    notProcessed = false;
                    receivedCorrelationId = correlationId;
                    conductor.onError(correlationId, errorCodeValue, errorCode, errorResponse.errorMessage());
                }

                if (asyncCommandIdSet.remove(correlationId) && notProcessed)
                {
                    conductor.onAsyncError(correlationId, errorCodeValue, errorCode, errorResponse.errorMessage());
                }
                break;
            }

            case ON_AVAILABLE_IMAGE:
            {
                imageReady.wrap(buffer, index);

                conductor.onAvailableImage(
                    imageReady.correlationId(),
                    imageReady.sessionId(),
                    imageReady.subscriptionRegistrationId(),
                    imageReady.subscriberPositionId(),
                    imageReady.logFileName(),
                    imageReady.sourceIdentity());
                break;
            }

            case ON_PUBLICATION_READY:
            {
                publicationReady.wrap(buffer, index);

                final long correlationId = publicationReady.correlationId();
                if (correlationId == activeCorrelationId || asyncCommandIdSet.remove(correlationId))
                {
                    receivedCorrelationId = correlationId;
                    conductor.onNewPublication(
                        correlationId,
                        publicationReady.registrationId(),
                        publicationReady.streamId(),
                        publicationReady.sessionId(),
                        publicationReady.publicationLimitCounterId(),
                        publicationReady.channelStatusCounterId(),
                        publicationReady.logFileName());
                }
                break;
            }

            case ON_SUBSCRIPTION_READY:
            {
                subscriptionReady.wrap(buffer, index);

                final long correlationId = subscriptionReady.correlationId();
                if (correlationId == activeCorrelationId || asyncCommandIdSet.remove(correlationId))
                {
                    receivedCorrelationId = correlationId;
                    conductor.onNewSubscription(correlationId, subscriptionReady.channelStatusCounterId());
                }
                break;
            }

            case ON_OPERATION_SUCCESS:
            {
                operationSucceeded.wrap(buffer, index);

                final long correlationId = operationSucceeded.correlationId();
                asyncCommandIdSet.remove(correlationId);
                if (correlationId == activeCorrelationId)
                {
                    receivedCorrelationId = correlationId;
                }
                break;
            }

            case ON_UNAVAILABLE_IMAGE:
            {
                imageMessage.wrap(buffer, index);

                conductor.onUnavailableImage(
                    imageMessage.correlationId(),
                    imageMessage.subscriptionRegistrationId());
                break;
            }

            case ON_EXCLUSIVE_PUBLICATION_READY:
            {
                publicationReady.wrap(buffer, index);

                final long correlationId = publicationReady.correlationId();
                if (correlationId == activeCorrelationId || asyncCommandIdSet.remove(correlationId))
                {
                    receivedCorrelationId = correlationId;
                    conductor.onNewExclusivePublication(
                        correlationId,
                        publicationReady.registrationId(),
                        publicationReady.streamId(),
                        publicationReady.sessionId(),
                        publicationReady.publicationLimitCounterId(),
                        publicationReady.channelStatusCounterId(),
                        publicationReady.logFileName());
                }
                break;
            }

            case ON_COUNTER_READY:
            {
                counterUpdate.wrap(buffer, index);

                final int counterId = counterUpdate.counterId();
                final long correlationId = counterUpdate.correlationId();
                if (correlationId == activeCorrelationId)
                {
                    receivedCorrelationId = correlationId;
                    conductor.onNewCounter(correlationId, counterId);
                }
                else
                {
                    conductor.onAvailableCounter(correlationId, counterId);
                }
                break;
            }

            case ON_UNAVAILABLE_COUNTER:
            {
                counterUpdate.wrap(buffer, index);

                conductor.onUnavailableCounter(counterUpdate.correlationId(), counterUpdate.counterId());
                break;
            }

            case ON_CLIENT_TIMEOUT:
            {
                clientTimeout.wrap(buffer, index);

                if (clientTimeout.clientId() == clientId)
                {
                    conductor.onClientTimeout();
                }
                break;
            }

            case ON_STATIC_COUNTER:
            {
                staticCounter.wrap(buffer, index);

                final long correlationId = staticCounter.correlationId();
                if (correlationId == activeCorrelationId)
                {
                    final int counterId = staticCounter.counterId();
                    receivedCorrelationId = correlationId;
                    conductor.onStaticCounter(correlationId, counterId);
                }
                break;
            }

            case ON_PUBLICATION_ERROR:
            {
                publicationErrorFrame.wrap(buffer, index);

                conductor.onPublicationError(publicationErrorFrame);
                break;
            }
        }
    }
}
