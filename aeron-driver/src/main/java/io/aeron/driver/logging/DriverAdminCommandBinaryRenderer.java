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

import static org.agrona.PrintBufferUtil.appendPrettyHexDump;

/**
 * Binary renderer for the Driver admin commands.
 */
public class DriverAdminCommandBinaryRenderer implements BinaryRenderer
{
    private final PublicationMessageFlyweight pubMsg = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subMsg = new SubscriptionMessageFlyweight();
    private final PublicationBuffersReadyFlyweight pubReady = new PublicationBuffersReadyFlyweight();
    private final ImageBuffersReadyFlyweight imageReady = new ImageBuffersReadyFlyweight();
    private final CorrelatedMessageFlyweight correlatedMsg = new CorrelatedMessageFlyweight();
    private final ImageMessageFlyweight imageMsg = new ImageMessageFlyweight();
    private final RemoveCounterFlyweight removeCounter = new RemoveCounterFlyweight();
    private final RemovePublicationFlyweight removePublication = new RemovePublicationFlyweight();
    private final RemoveSubscriptionFlyweight removeSubscription = new RemoveSubscriptionFlyweight();
    private final DestinationMessageFlyweight destinationMsg = new DestinationMessageFlyweight();
    private final ErrorResponseFlyweight errorMsg = new ErrorResponseFlyweight();
    private final CounterMessageFlyweight counterMsg = new CounterMessageFlyweight();
    private final CounterUpdateFlyweight counterUpdate = new CounterUpdateFlyweight();
    private final OperationSucceededFlyweight operationSucceeded = new OperationSucceededFlyweight();
    private final SubscriptionReadyFlyweight subscriptionReady = new SubscriptionReadyFlyweight();
    private final ClientTimeoutFlyweight clientTimeout = new ClientTimeoutFlyweight();
    private final TerminateDriverFlyweight terminateDriver = new TerminateDriverFlyweight();
    private final DestinationByIdMessageFlyweight destinationById = new DestinationByIdMessageFlyweight();
    private final RejectImageFlyweight rejectImage = new RejectImageFlyweight();

    private final boolean renderExtraBytes;

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

    private final String newLine = String.format("%n");

    /**
     * Default constructor.
     */
    public DriverAdminCommandBinaryRenderer()
    {
        this(RENDER_DATA_CONTENT);
    }

    /**
     * Constructor allowing explicit control of raw byte rendering.
     *
     * @param renderExtraBytes whether to render raw bytes of otherwise unlogged fields as a pretty hex/ASCII dump.
     */
    public DriverAdminCommandBinaryRenderer(final boolean renderExtraBytes)
    {
        this.renderExtraBytes = renderExtraBytes;
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
            case CMD_IN_ADD_PUBLICATION:
            case CMD_IN_ADD_EXCLUSIVE_PUBLICATION:
                pubMsg.wrap(mutableBuffer, offset);
                renderPublication(sb);
                break;
            case CMD_IN_ADD_SUBSCRIPTION:
                subMsg.wrap(mutableBuffer, offset);
                renderSubscription(sb);
                break;
            case CMD_IN_REMOVE_PUBLICATION:
                removePublication.wrap(mutableBuffer, offset);
                renderRemovePublicationEvent(sb, length);
                break;
            case CMD_IN_REMOVE_SUBSCRIPTION:
                removeSubscription.wrap(mutableBuffer, offset);
                renderRemoveSubscriptionEvent(sb);
                break;
            case CMD_IN_REMOVE_COUNTER:
                removeCounter.wrap(mutableBuffer, offset);
                renderRemoveCounterEvent(sb);
                break;
            case CMD_OUT_PUBLICATION_READY:
            case CMD_OUT_EXCLUSIVE_PUBLICATION_READY:
                pubReady.wrap(mutableBuffer, offset);
                renderPublicationReady(sb);
                break;
            case CMD_OUT_AVAILABLE_IMAGE:
                imageReady.wrap(mutableBuffer, offset);
                renderImageReady(sb);
                break;
            case CMD_OUT_ON_OPERATION_SUCCESS:
                operationSucceeded.wrap(mutableBuffer, offset);
                renderOperationSuccess(sb);
                break;
            case CMD_IN_KEEPALIVE_CLIENT:
            case CMD_IN_CLIENT_CLOSE:
                correlatedMsg.wrap(mutableBuffer, offset);
                renderCorrelationEvent(sb);
                break;
            case CMD_OUT_ON_UNAVAILABLE_IMAGE:
                imageMsg.wrap(mutableBuffer, offset);
                renderImage(sb);
                break;
            case CMD_IN_ADD_DESTINATION:
            case CMD_IN_REMOVE_DESTINATION:
            case CMD_IN_ADD_RCV_DESTINATION:
            case CMD_IN_REMOVE_RCV_DESTINATION:
                destinationMsg.wrap(mutableBuffer, offset);
                renderDestination(sb);
                break;
            case CMD_OUT_ERROR:
                errorMsg.wrap(mutableBuffer, offset);
                renderError(sb);
                break;
            case CMD_IN_ADD_COUNTER:
                counterMsg.wrap(mutableBuffer, offset);
                renderCounter(sb, buffer, offset);
                break;
            case CMD_OUT_SUBSCRIPTION_READY:
                subscriptionReady.wrap(mutableBuffer, offset);
                renderSubscriptionReady(sb);
                break;
            case CMD_OUT_COUNTER_READY:
            case CMD_OUT_ON_UNAVAILABLE_COUNTER:
                counterUpdate.wrap(mutableBuffer, offset);
                renderCounterUpdate(sb);
                break;
            case CMD_OUT_ON_CLIENT_TIMEOUT:
                clientTimeout.wrap(mutableBuffer, offset);
                renderClientTimeout(sb);
                break;
            case CMD_IN_TERMINATE_DRIVER:
                terminateDriver.wrap(mutableBuffer, offset);
                renderTerminateDriver(sb, buffer, offset);
                break;
            case CMD_IN_REMOVE_DESTINATION_BY_ID:
                destinationById.wrap(mutableBuffer, offset);
                renderDestinationById(sb);
                break;
            case CMD_IN_REJECT_IMAGE:
                rejectImage.wrap(mutableBuffer, offset);
                renderRejectImage(sb);
                break;
            default:
                sb.append("COMMAND_UNKNOWN: ").append(code);
                break;
        }
    }

    private void renderPublication(final StringBuilder sb)
    {
        sb
            .append("streamId=").append(pubMsg.streamId())
            .append(" clientId=").append(pubMsg.clientId())
            .append(" correlationId=").append(pubMsg.correlationId())
            .append(" channel=");

        pubMsg.appendChannel(sb);
    }

    private void renderSubscription(final StringBuilder sb)
    {
        sb
            .append("streamId=").append(subMsg.streamId())
            .append(" registrationCorrelationId=").append(subMsg.registrationCorrelationId())
            .append(" clientId=").append(subMsg.clientId())
            .append(" correlationId=").append(subMsg.correlationId())
            .append(" channel=");

        subMsg.appendChannel(sb);
    }

    private void renderPublicationReady(final StringBuilder sb)
    {
        sb
            .append("sessionId=").append(pubReady.sessionId())
            .append(" streamId=").append(pubReady.streamId())
            .append(" publicationLimitCounterId=").append(pubReady.publicationLimitCounterId())
            .append(" channelStatusCounterId=").append(pubReady.channelStatusCounterId())
            .append(" correlationId=").append(pubReady.correlationId())
            .append(" registrationId=").append(pubReady.registrationId())
            .append(" logFileName=");

        pubReady.appendLogFileName(sb);
    }

    private void renderImageReady(final StringBuilder sb)
    {
        sb
            .append("sessionId=").append(imageReady.sessionId())
            .append(" streamId=").append(imageReady.streamId())
            .append(" subscriberPositionId=").append(imageReady.subscriberPositionId())
            .append(" subscriptionRegistrationId=").append(imageReady.subscriptionRegistrationId())
            .append(" correlationId=").append(imageReady.correlationId());

        sb.append(" sourceIdentity=");
        imageReady.appendSourceIdentity(sb);
        sb.append(" logFileName=");
        imageReady.appendLogFileName(sb);
    }

    private void renderCorrelationEvent(final StringBuilder sb)
    {
        sb
            .append("clientId=").append(correlatedMsg.clientId())
            .append(" correlationId=").append(correlatedMsg.correlationId());
    }

    private void renderImage(final StringBuilder sb)
    {
        sb
            .append("streamId=").append(imageMsg.streamId())
            .append(" correlationId=").append(imageMsg.correlationId())
            .append(" subscriptionRegistrationId=")
            .append(imageMsg.subscriptionRegistrationId())
            .append(" channel=");

        imageMsg.appendChannel(sb);
    }

    private void renderRemoveCounterEvent(final StringBuilder sb)
    {
        sb
            .append("registrationId=").append(removeCounter.registrationId())
            .append(" clientId=").append(removeCounter.clientId())
            .append(" correlationId=").append(removeCounter.correlationId());
    }

    private void renderRemovePublicationEvent(final StringBuilder sb, final int length)
    {
        sb
            .append("registrationId=").append(removePublication.registrationId())
            .append(" clientId=").append(removePublication.clientId())
            .append(" correlationId=").append(removePublication.correlationId());

        if (removePublication.flagsFieldIsValid(length))
        {
            sb.append(" revoke=").append(removePublication.revoke());
        }
    }

    private void renderRemoveSubscriptionEvent(final StringBuilder sb)
    {
        sb
            .append("registrationId=").append(removeSubscription.registrationId())
            .append(" clientId=").append(removeSubscription.clientId())
            .append(" correlationId=").append(removeSubscription.correlationId());
    }

    private void renderDestination(final StringBuilder sb)
    {
        sb
            .append("registrationCorrelationId=").append(destinationMsg.registrationCorrelationId())
            .append(" clientId=").append(destinationMsg.clientId())
            .append(" correlationId=").append(destinationMsg.correlationId())
            .append(" channel=");

        destinationMsg.appendChannel(sb);
    }

    private void renderError(final StringBuilder sb)
    {
        sb
            .append("offendingCommandCorrelationId=").append(errorMsg.offendingCommandCorrelationId())
            .append(" errorCode=").append(errorMsg.errorCode())
            .append(" message=");

        errorMsg.appendMessage(sb);
    }

    private void renderCounter(final StringBuilder sb, final DirectBuffer buffer, final int offset)
    {
        sb
            .append("typeId=").append(counterMsg.typeId())
            .append(" keyBufferOffset=").append(counterMsg.keyBufferOffset())
            .append(" keyBufferLength=").append(counterMsg.keyBufferLength())
            .append(" labelBufferOffset=").append(counterMsg.labelBufferOffset())
            .append(" labelBufferLength=").append(counterMsg.labelBufferLength())
            .append(" clientId=").append(counterMsg.clientId())
            .append(" correlationId=").append(counterMsg.correlationId());

        if (renderExtraBytes)
        {
            final int keyBufferLength = counterMsg.keyBufferLength();
            if (keyBufferLength > 0)
            {
                sb.append(" keyBuffer=").append(newLine);
                appendPrettyHexDump(sb, buffer, offset + counterMsg.keyBufferOffset(), keyBufferLength);
            }

            final int labelBufferLength = counterMsg.labelBufferLength();
            if (labelBufferLength > 0)
            {
                sb.append(" label=").append(newLine);
                appendPrettyHexDump(sb, buffer, offset + counterMsg.labelBufferOffset(), labelBufferLength);
            }
        }
    }

    private void renderCounterUpdate(final StringBuilder sb)
    {
        sb
            .append("correlationId=").append(counterUpdate.correlationId())
            .append(" counterId=").append(counterUpdate.counterId());
    }

    private void renderOperationSuccess(final StringBuilder sb)
    {
        sb.append("correlationId=").append(operationSucceeded.correlationId());
    }

    private void renderSubscriptionReady(final StringBuilder sb)
    {
        sb
            .append("correlationId=").append(subscriptionReady.correlationId())
            .append(" channelStatusCounterId=").append(subscriptionReady.channelStatusCounterId());
    }

    private void renderClientTimeout(final StringBuilder sb)
    {
        sb.append("clientId=").append(clientTimeout.clientId());
    }

    private void renderTerminateDriver(final StringBuilder sb, final DirectBuffer buffer, final int offset)
    {
        sb
            .append("clientId=").append(terminateDriver.clientId())
            .append(" tokenBufferLength=").append(terminateDriver.tokenBufferLength());

        if (renderExtraBytes)
        {
            final int tokenBufferLength = terminateDriver.tokenBufferLength();
            if (tokenBufferLength > 0)
            {
                sb.append(" tokenBuffer=");
                appendPrettyHexDump(sb, buffer, offset + terminateDriver.tokenBufferOffset(), tokenBufferLength);
            }
        }
    }

    private void renderDestinationById(final StringBuilder sb)
    {
        sb
            .append("resourceRegistrationId=").append(destinationById.resourceRegistrationId())
            .append(" destinationRegistrationId=").append(destinationById.destinationRegistrationId());
    }

    private void renderRejectImage(final StringBuilder sb)
    {
        sb
            .append("clientId=").append(rejectImage.clientId())
            .append(" correlationId=").append(rejectImage.correlationId())
            .append(" imageCorrelationId=").append(rejectImage.imageCorrelationId())
            .append(" position=").append(rejectImage.position())
            .append(" reason=").append(rejectImage.reason());
    }
}
