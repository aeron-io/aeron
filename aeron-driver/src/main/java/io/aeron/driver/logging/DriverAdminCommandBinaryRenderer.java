/*
 * Copyright 2014-2026 Real Logic Limited.
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
package io.aeron.driver.logging;

import io.aeron.command.*;
import io.aeron.logging.BinaryRenderer;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Binary renderer for the Driver admin commands.
 */
public class DriverAdminCommandBinaryRenderer implements BinaryRenderer
{
    private static final PublicationMessageFlyweight PUB_MSG = new PublicationMessageFlyweight();
    private static final SubscriptionMessageFlyweight SUB_MSG = new SubscriptionMessageFlyweight();
    private static final PublicationBuffersReadyFlyweight PUB_READY = new PublicationBuffersReadyFlyweight();
    private static final ImageBuffersReadyFlyweight IMAGE_READY = new ImageBuffersReadyFlyweight();
    private static final CorrelatedMessageFlyweight CORRELATED_MSG = new CorrelatedMessageFlyweight();
    private static final ImageMessageFlyweight IMAGE_MSG = new ImageMessageFlyweight();
    private static final RemoveCounterFlyweight REMOVE_COUNTER = new RemoveCounterFlyweight();
    private static final RemovePublicationFlyweight REMOVE_PUBLICATION = new RemovePublicationFlyweight();
    private static final RemoveSubscriptionFlyweight REMOVE_SUBSCRIPTION = new RemoveSubscriptionFlyweight();
    private static final DestinationMessageFlyweight DESTINATION_MSG = new DestinationMessageFlyweight();
    private static final ErrorResponseFlyweight ERROR_MSG = new ErrorResponseFlyweight();
    private static final CounterMessageFlyweight COUNTER_MSG = new CounterMessageFlyweight();
    private static final CounterUpdateFlyweight COUNTER_UPDATE = new CounterUpdateFlyweight();
    private static final OperationSucceededFlyweight OPERATION_SUCCEEDED = new OperationSucceededFlyweight();
    private static final SubscriptionReadyFlyweight SUBSCRIPTION_READY = new SubscriptionReadyFlyweight();
    private static final ClientTimeoutFlyweight CLIENT_TIMEOUT = new ClientTimeoutFlyweight();
    private static final TerminateDriverFlyweight TERMINATE_DRIVER = new TerminateDriverFlyweight();
    private static final DestinationByIdMessageFlyweight DESTINATION_BY_ID = new DestinationByIdMessageFlyweight();
    private static final RejectImageFlyweight REJECT_IMAGE = new RejectImageFlyweight();

    private static final int[] MSG_TYPE_ID = {
        DriverEventCode.CMD_IN_ADD_PUBLICATION.toEventCodeId(),
        DriverEventCode.CMD_IN_ADD_EXCLUSIVE_PUBLICATION.toEventCodeId(),
        DriverEventCode.CMD_IN_ADD_SUBSCRIPTION.toEventCodeId(),
        DriverEventCode.CMD_IN_REMOVE_PUBLICATION.toEventCodeId(),
        DriverEventCode.CMD_IN_REMOVE_SUBSCRIPTION.toEventCodeId(),
        DriverEventCode.CMD_IN_REMOVE_COUNTER.toEventCodeId(),
        DriverEventCode.CMD_OUT_PUBLICATION_READY.toEventCodeId(),
        DriverEventCode.CMD_OUT_EXCLUSIVE_PUBLICATION_READY.toEventCodeId(),
        DriverEventCode.CMD_OUT_AVAILABLE_IMAGE.toEventCodeId(),
        DriverEventCode.CMD_OUT_ON_OPERATION_SUCCESS.toEventCodeId(),
        DriverEventCode.CMD_IN_KEEPALIVE_CLIENT.toEventCodeId(),
        DriverEventCode.CMD_IN_CLIENT_CLOSE.toEventCodeId(),
        DriverEventCode.CMD_OUT_ON_UNAVAILABLE_IMAGE.toEventCodeId(),
        DriverEventCode.CMD_IN_ADD_DESTINATION.toEventCodeId(),
        DriverEventCode.CMD_IN_REMOVE_DESTINATION.toEventCodeId(),
        DriverEventCode.CMD_IN_ADD_RCV_DESTINATION.toEventCodeId(),
        DriverEventCode.CMD_IN_REMOVE_RCV_DESTINATION.toEventCodeId(),
        DriverEventCode.CMD_OUT_ERROR.toEventCodeId(),
        DriverEventCode.CMD_IN_ADD_COUNTER.toEventCodeId(),
        DriverEventCode.CMD_OUT_SUBSCRIPTION_READY.toEventCodeId(),
        DriverEventCode.CMD_OUT_COUNTER_READY.toEventCodeId(),
        DriverEventCode.CMD_OUT_ON_UNAVAILABLE_COUNTER.toEventCodeId(),
        DriverEventCode.CMD_OUT_ON_CLIENT_TIMEOUT.toEventCodeId(),
        DriverEventCode.CMD_IN_TERMINATE_DRIVER.toEventCodeId(),
        DriverEventCode.CMD_IN_REMOVE_DESTINATION_BY_ID.toEventCodeId(),
        DriverEventCode.CMD_IN_REJECT_IMAGE.toEventCodeId()
    };

    /**
     * Default constructor.
     */
    public DriverAdminCommandBinaryRenderer()
    {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int[] supportingMsgTypeIds()
    {
        return MSG_TYPE_ID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("MethodLength")
    public void append(
        final StringBuilder sb,
        final int msgTypeId,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        final MutableDirectBuffer mutableBuffer = (MutableDirectBuffer)buffer;
        final DriverEventCode code = DriverEventCode.fromEventCodeId(msgTypeId);

        switch (code)
        {
            case CMD_IN_ADD_PUBLICATION, CMD_IN_ADD_EXCLUSIVE_PUBLICATION ->
            {
                PUB_MSG.wrap(mutableBuffer, offset);
                renderPublication(sb);
            }
            case CMD_IN_ADD_SUBSCRIPTION ->
            {
                SUB_MSG.wrap(mutableBuffer, offset);
                renderSubscription(sb);
            }
            case CMD_IN_REMOVE_PUBLICATION ->
            {
                REMOVE_PUBLICATION.wrap(mutableBuffer, offset);
                renderRemovePublicationEvent(sb, length);
            }
            case CMD_IN_REMOVE_SUBSCRIPTION ->
            {
                REMOVE_SUBSCRIPTION.wrap(mutableBuffer, offset);
                renderRemoveSubscriptionEvent(sb);
            }
            case CMD_IN_REMOVE_COUNTER ->
            {
                REMOVE_COUNTER.wrap(mutableBuffer, offset);
                renderRemoveCounterEvent(sb);
            }
            case CMD_OUT_PUBLICATION_READY, CMD_OUT_EXCLUSIVE_PUBLICATION_READY ->
            {
                PUB_READY.wrap(mutableBuffer, offset);
                renderPublicationReady(sb);
            }
            case CMD_OUT_AVAILABLE_IMAGE ->
            {
                IMAGE_READY.wrap(mutableBuffer, offset);
                renderImageReady(sb);
            }
            case CMD_OUT_ON_OPERATION_SUCCESS ->
            {
                OPERATION_SUCCEEDED.wrap(mutableBuffer, offset);
                renderOperationSuccess(sb);
            }
            case CMD_IN_KEEPALIVE_CLIENT, CMD_IN_CLIENT_CLOSE ->
            {
                CORRELATED_MSG.wrap(mutableBuffer, offset);
                renderCorrelationEvent(sb);
            }
            case CMD_OUT_ON_UNAVAILABLE_IMAGE ->
            {
                IMAGE_MSG.wrap(mutableBuffer, offset);
                renderImage(sb);
            }
            case CMD_IN_ADD_DESTINATION, CMD_IN_REMOVE_DESTINATION, CMD_IN_ADD_RCV_DESTINATION,
                 CMD_IN_REMOVE_RCV_DESTINATION ->
            {
                DESTINATION_MSG.wrap(mutableBuffer, offset);
                renderDestination(sb);
            }
            case CMD_OUT_ERROR ->
            {
                ERROR_MSG.wrap(mutableBuffer, offset);
                renderError(sb);
            }
            case CMD_IN_ADD_COUNTER ->
            {
                COUNTER_MSG.wrap(mutableBuffer, offset);
                renderCounter(sb);
            }
            case CMD_OUT_SUBSCRIPTION_READY ->
            {
                SUBSCRIPTION_READY.wrap(mutableBuffer, offset);
                renderSubscriptionReady(sb);
            }
            case CMD_OUT_COUNTER_READY, CMD_OUT_ON_UNAVAILABLE_COUNTER ->
            {
                COUNTER_UPDATE.wrap(mutableBuffer, offset);
                renderCounterUpdate(sb);
            }
            case CMD_OUT_ON_CLIENT_TIMEOUT ->
            {
                CLIENT_TIMEOUT.wrap(mutableBuffer, offset);
                renderClientTimeout(sb);
            }
            case CMD_IN_TERMINATE_DRIVER ->
            {
                TERMINATE_DRIVER.wrap(mutableBuffer, offset);
                renderTerminateDriver(sb);
            }
            case CMD_IN_REMOVE_DESTINATION_BY_ID ->
            {
                DESTINATION_BY_ID.wrap(mutableBuffer, offset);
                renderDestinationById(sb);
            }
            case CMD_IN_REJECT_IMAGE ->
            {
                REJECT_IMAGE.wrap(mutableBuffer, offset);
                renderRejectImage(sb);
            }
            default -> sb.append("COMMAND_UNKNOWN: ").append(code);
        }
    }

    private static void renderPublication(final StringBuilder sb)
    {
        sb
            .append("streamId=").append(PUB_MSG.streamId())
            .append(" clientId=").append(PUB_MSG.clientId())
            .append(" correlationId=").append(PUB_MSG.correlationId())
            .append(" channel=");

        PUB_MSG.appendChannel(sb);
    }

    private static void renderSubscription(final StringBuilder sb)
    {
        sb
            .append("streamId=").append(SUB_MSG.streamId())
            .append(" registrationCorrelationId=").append(SUB_MSG.registrationCorrelationId())
            .append(" clientId=").append(SUB_MSG.clientId())
            .append(" correlationId=").append(SUB_MSG.correlationId())
            .append(" channel=");

        SUB_MSG.appendChannel(sb);
    }

    private static void renderPublicationReady(final StringBuilder sb)
    {
        sb
            .append("sessionId=").append(PUB_READY.sessionId())
            .append(" streamId=").append(PUB_READY.streamId())
            .append(" publicationLimitCounterId=").append(PUB_READY.publicationLimitCounterId())
            .append(" channelStatusCounterId=").append(PUB_READY.channelStatusCounterId())
            .append(" correlationId=").append(PUB_READY.correlationId())
            .append(" registrationId=").append(PUB_READY.registrationId())
            .append(" logFileName=");

        PUB_READY.appendLogFileName(sb);
    }

    private static void renderImageReady(final StringBuilder sb)
    {
        sb
            .append("sessionId=").append(IMAGE_READY.sessionId())
            .append(" streamId=").append(IMAGE_READY.streamId())
            .append(" subscriberPositionId=").append(IMAGE_READY.subscriberPositionId())
            .append(" subscriptionRegistrationId=").append(IMAGE_READY.subscriptionRegistrationId())
            .append(" correlationId=").append(IMAGE_READY.correlationId());

        sb.append(" sourceIdentity=");
        IMAGE_READY.appendSourceIdentity(sb);
        sb.append(" logFileName=");
        IMAGE_READY.appendLogFileName(sb);
    }

    private static void renderCorrelationEvent(final StringBuilder sb)
    {
        sb
            .append("clientId=").append(CORRELATED_MSG.clientId())
            .append(" correlationId=").append(CORRELATED_MSG.correlationId());
    }

    private static void renderImage(final StringBuilder sb)
    {
        sb
            .append("streamId=").append(IMAGE_MSG.streamId())
            .append(" correlationId=").append(IMAGE_MSG.correlationId())
            .append(" subscriptionRegistrationId=")
            .append(IMAGE_MSG.subscriptionRegistrationId())
            .append(" channel=");

        IMAGE_MSG.appendChannel(sb);
    }

    private static void renderRemoveCounterEvent(final StringBuilder sb)
    {
        sb
            .append("registrationId=").append(REMOVE_COUNTER.registrationId())
            .append(" clientId=").append(REMOVE_COUNTER.clientId())
            .append(" correlationId=").append(REMOVE_COUNTER.correlationId());
    }

    private static void renderRemovePublicationEvent(final StringBuilder sb, final int length)
    {
        sb
            .append("registrationId=").append(REMOVE_PUBLICATION.registrationId())
            .append(" clientId=").append(REMOVE_PUBLICATION.clientId())
            .append(" correlationId=").append(REMOVE_PUBLICATION.correlationId());

        if (REMOVE_PUBLICATION.flagsFieldIsValid(length))
        {
            sb.append(" revoke=").append(REMOVE_PUBLICATION.revoke());
        }
    }

    private static void renderRemoveSubscriptionEvent(final StringBuilder sb)
    {
        sb
            .append("registrationId=").append(REMOVE_SUBSCRIPTION.registrationId())
            .append(" clientId=").append(REMOVE_SUBSCRIPTION.clientId())
            .append(" correlationId=").append(REMOVE_SUBSCRIPTION.correlationId());
    }

    private static void renderDestination(final StringBuilder sb)
    {
        sb
            .append("registrationCorrelationId=").append(DESTINATION_MSG.registrationCorrelationId())
            .append(" clientId=").append(DESTINATION_MSG.clientId())
            .append(" correlationId=").append(DESTINATION_MSG.correlationId())
            .append(" channel=");

        DESTINATION_MSG.appendChannel(sb);
    }

    private static void renderError(final StringBuilder sb)
    {
        sb
            .append("offendingCommandCorrelationId=").append(ERROR_MSG.offendingCommandCorrelationId())
            .append(" errorCode=").append(ERROR_MSG.errorCode())
            .append(" message=");

        ERROR_MSG.appendMessage(sb);
    }

    private static void renderCounter(final StringBuilder sb)
    {
        sb
            .append("typeId=").append(COUNTER_MSG.typeId())
            .append(" keyBufferOffset=").append(COUNTER_MSG.keyBufferOffset())
            .append(" keyBufferLength=").append(COUNTER_MSG.keyBufferLength())
            .append(" labelBufferOffset=").append(COUNTER_MSG.labelBufferOffset())
            .append(" labelBufferLength=").append(COUNTER_MSG.labelBufferLength())
            .append(" clientId=").append(COUNTER_MSG.clientId())
            .append(" correlationId=").append(COUNTER_MSG.correlationId());
    }

    private static void renderCounterUpdate(final StringBuilder sb)
    {
        sb
            .append("correlationId=").append(COUNTER_UPDATE.correlationId())
            .append(" counterId=").append(COUNTER_UPDATE.counterId());
    }

    private static void renderOperationSuccess(final StringBuilder sb)
    {
        sb.append("correlationId=").append(OPERATION_SUCCEEDED.correlationId());
    }

    private static void renderSubscriptionReady(final StringBuilder sb)
    {
        sb
            .append("correlationId=").append(SUBSCRIPTION_READY.correlationId())
            .append(" channelStatusCounterId=").append(SUBSCRIPTION_READY.channelStatusCounterId());
    }

    private static void renderClientTimeout(final StringBuilder sb)
    {
        sb.append("clientId=").append(CLIENT_TIMEOUT.clientId());
    }

    private static void renderTerminateDriver(final StringBuilder sb)
    {
        sb
            .append("clientId=").append(TERMINATE_DRIVER.clientId())
            .append(" tokenBufferLength=").append(TERMINATE_DRIVER.tokenBufferLength());
    }

    private static void renderDestinationById(final StringBuilder sb)
    {
        sb
            .append("resourceRegistrationId=").append(DESTINATION_BY_ID.resourceRegistrationId())
            .append(" destinationRegistrationId=").append(DESTINATION_BY_ID.destinationRegistrationId());
    }

    private static void renderRejectImage(final StringBuilder sb)
    {
        sb
            .append("clientId=").append(REJECT_IMAGE.clientId())
            .append(" correlationId=").append(REJECT_IMAGE.correlationId())
            .append(" imageCorrelationId=").append(REJECT_IMAGE.imageCorrelationId())
            .append(" position=").append(REJECT_IMAGE.position())
            .append(" reason=").append(REJECT_IMAGE.reason());
    }
}
