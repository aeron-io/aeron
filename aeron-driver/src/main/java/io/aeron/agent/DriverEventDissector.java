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
package io.aeron.agent;

import io.aeron.command.*;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.*;
import org.agrona.MutableDirectBuffer;

import static io.aeron.agent.CommonEventDissector.*;
import static io.aeron.agent.DriverEventCode.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.*;

/**
 * Dissect encoded log events and append them to a provided {@link StringBuilder}.
 * <p>
 * <b>Note:</b>The event consumer of the log should be single threaded.
 */
final class DriverEventDissector
{
    private static final DataHeaderFlyweight DATA_HEADER = new DataHeaderFlyweight();
    private static final NakFlyweight NAK_HEADER = new NakFlyweight();
    private static final StatusMessageFlyweight SM_HEADER = new StatusMessageFlyweight();
    private static final ErrorFlyweight ERROR_HEADER = new ErrorFlyweight();
    private static final SetupFlyweight SETUP_HEADER = new SetupFlyweight();
    private static final RttMeasurementFlyweight RTT_MEASUREMENT = new RttMeasurementFlyweight();
    private static final HeaderFlyweight HEADER = new HeaderFlyweight();
    private static final ResolutionEntryFlyweight RESOLUTION = new ResolutionEntryFlyweight();
    private static final ResponseSetupFlyweight RSP_SETUP = new ResponseSetupFlyweight();
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

    static final String CONTEXT = "DRIVER";

    private DriverEventDissector()
    {
    }

    static void dissectFrame(
        final DriverEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int encodedLength = dissectLogHeader(CONTEXT, eventCode, buffer, offset, builder);

        builder.append(": address=");

        encodedLength += dissectSocketAddress(buffer, offset + encodedLength, builder);

        builder.append(" ");

        final int frameOffset = offset + encodedLength;
        final int frameType = frameType(buffer, frameOffset);
        switch (frameType)
        {
            case HeaderFlyweight.HDR_TYPE_PAD:
            case HeaderFlyweight.HDR_TYPE_DATA:
                DATA_HEADER.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                dissectDataFrame(builder);
                break;

            case HeaderFlyweight.HDR_TYPE_NAK:
                NAK_HEADER.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                dissectNakFrame(builder);
                break;

            case HeaderFlyweight.HDR_TYPE_SM:
                SM_HEADER.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                dissectStatusFrame(builder);
                break;

            case HeaderFlyweight.HDR_TYPE_ERR:
                ERROR_HEADER.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                dissectErrorFrame(builder);
                break;

            case HeaderFlyweight.HDR_TYPE_SETUP:
                SETUP_HEADER.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                dissectSetupFrame(builder);
                break;

            case HeaderFlyweight.HDR_TYPE_RTTM:
                RTT_MEASUREMENT.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                dissectRttFrame(builder);
                break;

            case HeaderFlyweight.HDR_TYPE_RES:
                dissectResFrame(buffer, frameOffset, builder);
                break;

            case HeaderFlyweight.HDR_TYPE_RSP_SETUP:
                RSP_SETUP.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                dissectRspSetupFrame(builder);
                break;

            default:
                builder.append("type=UNKNOWN(").append(frameType).append(")");
                break;
        }
    }

    @SuppressWarnings("MethodLength")
    static void dissectCommand(
        final DriverEventCode code, final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        final int encodedLength = dissectLogHeader(CONTEXT, code, buffer, offset, builder);
        builder.append(": ");

        switch (code)
        {
            case CMD_IN_ADD_PUBLICATION:
            case CMD_IN_ADD_EXCLUSIVE_PUBLICATION:
                PUB_MSG.wrap(buffer, offset + encodedLength);
                dissectPublication(builder);
                break;

            case CMD_IN_ADD_SUBSCRIPTION:
                SUB_MSG.wrap(buffer, offset + encodedLength);
                dissectSubscription(builder);
                break;

            case CMD_IN_REMOVE_PUBLICATION:
                REMOVE_PUBLICATION.wrap(buffer, offset + encodedLength);
                final int captureLength = buffer.getInt(offset, LITTLE_ENDIAN);
                dissectRemovePublicationEvent(builder, captureLength);
                break;

            case CMD_IN_REMOVE_SUBSCRIPTION:
                REMOVE_SUBSCRIPTION.wrap(buffer, offset + encodedLength);
                dissectRemoveSubscriptionEvent(builder);
                break;

            case CMD_IN_REMOVE_COUNTER:
                REMOVE_COUNTER.wrap(buffer, offset + encodedLength);
                dissectRemoveCounterEvent(builder);
                break;

            case CMD_OUT_PUBLICATION_READY:
            case CMD_OUT_EXCLUSIVE_PUBLICATION_READY:
                PUB_READY.wrap(buffer, offset + encodedLength);
                dissectPublicationReady(builder);
                break;

            case CMD_OUT_AVAILABLE_IMAGE:
                IMAGE_READY.wrap(buffer, offset + encodedLength);
                dissectImageReady(builder);
                break;

            case CMD_OUT_ON_OPERATION_SUCCESS:
                OPERATION_SUCCEEDED.wrap(buffer, offset + encodedLength);
                dissectOperationSuccess(builder);
                break;

            case CMD_IN_KEEPALIVE_CLIENT:
            case CMD_IN_CLIENT_CLOSE:
                CORRELATED_MSG.wrap(buffer, offset + encodedLength);
                dissectCorrelationEvent(builder);
                break;

            case CMD_OUT_ON_UNAVAILABLE_IMAGE:
                IMAGE_MSG.wrap(buffer, offset + encodedLength);
                dissectImage(builder);
                break;

            case CMD_IN_ADD_DESTINATION:
            case CMD_IN_REMOVE_DESTINATION:
            case CMD_IN_ADD_RCV_DESTINATION:
            case CMD_IN_REMOVE_RCV_DESTINATION:
                DESTINATION_MSG.wrap(buffer, offset + encodedLength);
                dissectDestination(builder);
                break;

            case CMD_OUT_ERROR:
                ERROR_MSG.wrap(buffer, offset + encodedLength);
                dissectError(builder);
                break;

            case CMD_IN_ADD_COUNTER:
                COUNTER_MSG.wrap(buffer, offset + encodedLength);
                dissectCounter(builder);
                break;

            case CMD_OUT_SUBSCRIPTION_READY:
                SUBSCRIPTION_READY.wrap(buffer, offset + encodedLength);
                dissectSubscriptionReady(builder);
                break;

            case CMD_OUT_COUNTER_READY:
            case CMD_OUT_ON_UNAVAILABLE_COUNTER:
                COUNTER_UPDATE.wrap(buffer, offset + encodedLength);
                dissectCounterUpdate(builder);
                break;

            case CMD_OUT_ON_CLIENT_TIMEOUT:
                CLIENT_TIMEOUT.wrap(buffer, offset + encodedLength);
                dissectClientTimeout(builder);
                break;

            case CMD_IN_TERMINATE_DRIVER:
                TERMINATE_DRIVER.wrap(buffer, offset + encodedLength);
                dissectTerminateDriver(builder);
                break;

            case CMD_IN_REMOVE_DESTINATION_BY_ID:
                DESTINATION_BY_ID.wrap(buffer, offset + encodedLength);
                dissectDestinationById(builder);
                break;

            case CMD_IN_REJECT_IMAGE:
                REJECT_IMAGE.wrap(buffer, offset + encodedLength);
                dissectRejectImage(builder);
                break;

            default:
                builder.append("COMMAND_UNKNOWN: ").append(code);
                break;
        }
    }

    static void dissectString(
        final DriverEventCode code, final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        final int encodedLength = dissectLogHeader(CONTEXT, code, buffer, offset, builder);
        builder.append(": ").append(buffer.getStringAscii(offset + encodedLength, LITTLE_ENDIAN));
    }

    static void dissectRemovePublicationCleanup(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, REMOVE_PUBLICATION_CLEANUP, buffer, absoluteOffset, builder);

        builder.append(": sessionId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;

        builder.append(" streamId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;

        builder.append(" channel=");
        buffer.getStringAscii(absoluteOffset, builder);
    }

    static void dissectRemoveSubscriptionCleanup(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, REMOVE_SUBSCRIPTION_CLEANUP, buffer, absoluteOffset, builder);

        builder.append(": streamId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;

        builder.append(" id=").append(buffer.getLong(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_LONG;

        builder.append(" channel=");
        buffer.getStringAscii(absoluteOffset, builder);
    }

    static void dissectRemoveImageCleanup(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, REMOVE_IMAGE_CLEANUP, buffer, absoluteOffset, builder);

        builder.append(": sessionId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;

        builder.append(" streamId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;

        builder.append(" id=").append(buffer.getLong(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_LONG;

        builder.append(" channel=");
        buffer.getStringAscii(absoluteOffset, builder);
    }

    static void dissectUntetheredSubscriptionStateChange(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(
            CONTEXT, UNTETHERED_SUBSCRIPTION_STATE_CHANGE, buffer, absoluteOffset, builder);

        builder.append(": subscriptionId=").append(buffer.getLong(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_LONG;

        builder.append(" streamId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;

        builder.append(" sessionId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;

        builder.append(" ");
        buffer.getStringAscii(absoluteOffset, builder);
    }

    static void dissectAddress(
        final DriverEventCode code, final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, code, buffer, absoluteOffset, builder);

        builder.append(": ");
        dissectSocketAddress(buffer, absoluteOffset, builder);
    }

    static void dissectFlowControlReceiver(
        final DriverEventCode code, final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, code, buffer, absoluteOffset, builder);

        builder.append(": receiverCount=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;

        builder.append(" receiverId=").append(buffer.getLong(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_LONG;

        builder.append(" sessionId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;

        builder.append(" streamId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;

        builder.append(" channel=");
        buffer.getStringAscii(absoluteOffset, builder);
    }

    static void dissectResolve(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, NAME_RESOLUTION_RESOLVE, buffer, absoluteOffset, builder);

        final boolean isReResolution = 1 == buffer.getByte(absoluteOffset);
        absoluteOffset += SIZE_OF_BYTE;

        final long durationNs = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        builder.append(": resolver=");
        absoluteOffset += buffer.getStringAscii(absoluteOffset, builder);
        absoluteOffset += SIZE_OF_INT;

        builder.append(" durationNs=").append(durationNs);

        builder.append(" name=");
        absoluteOffset += buffer.getStringAscii(absoluteOffset, builder);
        absoluteOffset += SIZE_OF_INT;

        builder.append(" isReResolution=").append(isReResolution);

        builder.append(" address=");
        dissectInetAddress(buffer, absoluteOffset, builder);
    }

    static void dissectLookup(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, NAME_RESOLUTION_LOOKUP, buffer, absoluteOffset, builder);

        final boolean isReLookup = 1 == buffer.getByte(absoluteOffset);
        absoluteOffset += SIZE_OF_BYTE;

        final long durationNs = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        builder.append(": resolver=");
        absoluteOffset += buffer.getStringAscii(absoluteOffset, builder);
        absoluteOffset += SIZE_OF_INT;

        builder.append(" durationNs=").append(durationNs);

        builder.append(" name=");
        absoluteOffset += buffer.getStringAscii(absoluteOffset, builder);
        absoluteOffset += SIZE_OF_INT;

        builder.append(" isReLookup=").append(isReLookup);

        builder.append(" resolvedName=");
        buffer.getStringAscii(absoluteOffset, builder);
    }

    static void dissectHostName(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, NAME_RESOLUTION_HOST_NAME, buffer, absoluteOffset, builder);

        builder.append(": durationNs=").append(buffer.getLong(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_LONG;

        builder.append(" hostName=");
        buffer.getStringAscii(absoluteOffset, builder);
    }

    static void dissectNak(
        final DriverEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);
        builder.append(": address=");
        final int encodedSocketLength = dissectSocketAddress(buffer, absoluteOffset, builder);
        absoluteOffset += encodedSocketLength;
        builder.append(" sessionId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;
        builder.append(" streamId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;
        builder.append(" termId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;
        builder.append(" termOffset=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;
        builder.append(" length=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;
        builder.append(" channel=");
        buffer.getStringAscii(absoluteOffset, builder);
    }

    static void dissectResend(final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, RESEND, buffer, absoluteOffset, builder);
        builder.append(": sessionId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;
        builder.append(" streamId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;
        builder.append(" termId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;
        builder.append(" termOffset=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;
        builder.append(" length=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;
        builder.append(" channel=");
        buffer.getStringAscii(absoluteOffset, builder);
    }

    static void dissectPublicationRevoke(
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, PUBLICATION_REVOKE, buffer, absoluteOffset, builder);
        builder.append(": revokedPos=").append(buffer.getLong(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_LONG;
        builder.append(" sessionId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;
        builder.append(" streamId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;
        builder.append(" channel=");
        buffer.getStringAscii(absoluteOffset, builder);
    }

    static void dissectPublicationImageRevoke(
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, PUBLICATION_IMAGE_REVOKE, buffer, absoluteOffset, builder);
        builder.append(": revokedPos=").append(buffer.getLong(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_LONG;
        builder.append(" sessionId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;
        builder.append(" streamId=").append(buffer.getInt(absoluteOffset, LITTLE_ENDIAN));
        absoluteOffset += SIZE_OF_INT;
        builder.append(" channel=");
        buffer.getStringAscii(absoluteOffset, builder);
    }

    static int frameType(final MutableDirectBuffer buffer, final int termOffset)
    {
        return buffer.getShort(FrameDescriptor.typeOffset(termOffset), LITTLE_ENDIAN) & 0xFFFF;
    }

    private static void dissectDataFrame(final StringBuilder builder)
    {
        builder
            .append("type=")
            .append(DATA_HEADER.headerType() == HeaderFlyweight.HDR_TYPE_PAD ? "PAD" : "DATA")
            .append(" flags=");

        HeaderFlyweight.appendFlagsAsChars(DATA_HEADER.flags(), builder);

        builder
            .append(" frameLength=")
            .append(DATA_HEADER.frameLength())
            .append(" sessionId=")
            .append(DATA_HEADER.sessionId())
            .append(" streamId=")
            .append(DATA_HEADER.streamId())
            .append(" termId=")
            .append(DATA_HEADER.termId())
            .append(" termOffset=")
            .append(DATA_HEADER.termOffset());
    }

    private static void dissectStatusFrame(final StringBuilder builder)
    {
        builder.append("type=SM flags=");
        HeaderFlyweight.appendFlagsAsChars(SM_HEADER.flags(), builder);

        builder
            .append(" frameLength=")
            .append(SM_HEADER.frameLength())
            .append(" sessionId=")
            .append(SM_HEADER.sessionId())
            .append(" streamId=")
            .append(SM_HEADER.streamId())
            .append(" termId=")
            .append(SM_HEADER.consumptionTermId())
            .append(" termOffset=")
            .append(SM_HEADER.consumptionTermOffset())
            .append(" receiverWindowLength=")
            .append(SM_HEADER.receiverWindowLength())
            .append(" receiverId=")
            .append(SM_HEADER.receiverId());
    }

    private static void dissectNakFrame(final StringBuilder builder)
    {
        builder.append("type=NAK flags=");
        HeaderFlyweight.appendFlagsAsChars(NAK_HEADER.flags(), builder);

        builder
            .append(" frameLength=")
            .append(NAK_HEADER.frameLength())
            .append(" sessionId=")
            .append(NAK_HEADER.sessionId())
            .append(" streamId=")
            .append(NAK_HEADER.streamId())
            .append(" termId=")
            .append(NAK_HEADER.termId())
            .append(" termOffset=")
            .append(NAK_HEADER.termOffset())
            .append(" length=")
            .append(NAK_HEADER.length());
    }

    private static void dissectErrorFrame(final StringBuilder builder)
    {
        builder.append("type=ERR flags=");
        HeaderFlyweight.appendFlagsAsChars(ERROR_HEADER.flags(), builder);

        builder
            .append(" frameLength=")
            .append(ERROR_HEADER.frameLength())
            .append(" sessionId=")
            .append(ERROR_HEADER.sessionId())
            .append(" streamId=")
            .append(ERROR_HEADER.streamId())
            .append(" receiverId=")
            .append(ERROR_HEADER.receiverId())
            .append(" groupTag=")
            .append(ERROR_HEADER.groupTag())
            .append(" errorCode=")
            .append(ERROR_HEADER.errorCode())
            .append(" errorMessage=\"")
            .append(ERROR_HEADER.errorMessage())
            .append('"');
    }

    private static void dissectSetupFrame(final StringBuilder builder)
    {
        builder.append("type=SETUP flags=");
        HeaderFlyweight.appendFlagsAsChars(SETUP_HEADER.flags(), builder);

        builder
            .append(" frameLength=")
            .append(SETUP_HEADER.frameLength())
            .append(" sessionId=")
            .append(SETUP_HEADER.sessionId())
            .append(" streamId=")
            .append(SETUP_HEADER.streamId())
            .append(" activeTermId=")
            .append(SETUP_HEADER.activeTermId())
            .append(" initialTermId=")
            .append(SETUP_HEADER.initialTermId())
            .append(" termOffset=")
            .append(SETUP_HEADER.termOffset())
            .append(" termLength=")
            .append(SETUP_HEADER.termLength())
            .append(" mtu=")
            .append(SETUP_HEADER.mtuLength())
            .append(" ttl=")
            .append(SETUP_HEADER.ttl());
    }

    private static void dissectRttFrame(final StringBuilder builder)
    {
        builder.append("type=RTT flags=");
        HeaderFlyweight.appendFlagsAsChars(RTT_MEASUREMENT.flags(), builder);

        builder
            .append(" frameLength=")
            .append(RTT_MEASUREMENT.frameLength())
            .append(" sessionId=")
            .append(RTT_MEASUREMENT.sessionId())
            .append(" streamId=")
            .append(RTT_MEASUREMENT.streamId())
            .append(" echoTimestampNs=")
            .append(RTT_MEASUREMENT.echoTimestampNs())
            .append(" receptionDelta=")
            .append(RTT_MEASUREMENT.receptionDelta())
            .append(" receiverId=")
            .append(RTT_MEASUREMENT.receiverId());
    }

    private static void dissectResFrame(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int currentOffset = offset;

        HEADER.wrap(buffer, offset, buffer.capacity() - offset);
        final int length = offset + Math.min(HEADER.frameLength(), CommonEventEncoder.MAX_CAPTURE_LENGTH);
        currentOffset += HeaderFlyweight.MIN_HEADER_LENGTH;

        builder.append("type=RES flags=");
        HeaderFlyweight.appendFlagsAsChars(HEADER.flags(), builder);

        builder
            .append(" frameLength=")
            .append(HEADER.frameLength());

        while (length > currentOffset)
        {
            RESOLUTION.wrap(buffer, currentOffset, buffer.capacity() - currentOffset);

            if ((length - offset) < RESOLUTION.entryLength())
            {
                builder.append(" ... ").append(length - offset).append(" bytes left");
                break;
            }

            dissectResEntry(builder);

            currentOffset += RESOLUTION.entryLength();
        }
    }

    private static void dissectRspSetupFrame(final StringBuilder builder)
    {
        builder.append("type=RSP_SETUP flags=");
        HeaderFlyweight.appendFlagsAsChars(RSP_SETUP.flags(), builder);

        builder
            .append(" frameLength=")
            .append(RSP_SETUP.frameLength())
            .append(" sessionId=")
            .append(RSP_SETUP.sessionId())
            .append(" streamId=")
            .append(RSP_SETUP.streamId())
            .append(" responseSessionId=")
            .append(RSP_SETUP.responseSessionId());
    }

    private static void dissectResEntry(final StringBuilder builder)
    {
        builder
            .append(" [resType=")
            .append(RESOLUTION.resType())
            .append(" flags=");

        HeaderFlyweight.appendFlagsAsChars(RESOLUTION.flags(), builder);

        builder
            .append(" port=")
            .append(RESOLUTION.udpPort())
            .append(" ageInMs=")
            .append(RESOLUTION.ageInMs());

        builder.append(" address=");
        RESOLUTION.appendAddress(builder);

        builder.append(" name=");
        RESOLUTION.appendName(builder);
        builder.append(']');
    }

    private static void dissectPublication(final StringBuilder builder)
    {
        builder
            .append("streamId=").append(PUB_MSG.streamId())
            .append(" clientId=").append(PUB_MSG.clientId())
            .append(" correlationId=").append(PUB_MSG.correlationId())
            .append(" channel=");

        PUB_MSG.appendChannel(builder);
    }

    private static void dissectSubscription(final StringBuilder builder)
    {
        builder
            .append("streamId=").append(SUB_MSG.streamId())
            .append(" registrationCorrelationId=").append(SUB_MSG.registrationCorrelationId())
            .append(" clientId=").append(SUB_MSG.clientId())
            .append(" correlationId=").append(SUB_MSG.correlationId())
            .append(" channel=");

        SUB_MSG.appendChannel(builder);
    }

    private static void dissectPublicationReady(final StringBuilder builder)
    {
        builder
            .append("sessionId=").append(PUB_READY.sessionId())
            .append(" streamId=").append(PUB_READY.streamId())
            .append(" publicationLimitCounterId=").append(PUB_READY.publicationLimitCounterId())
            .append(" channelStatusCounterId=").append(PUB_READY.channelStatusCounterId())
            .append(" correlationId=").append(PUB_READY.correlationId())
            .append(" registrationId=").append(PUB_READY.registrationId())
            .append(" logFileName=");

        PUB_READY.appendLogFileName(builder);
    }

    private static void dissectImageReady(final StringBuilder builder)
    {
        builder
            .append("sessionId=").append(IMAGE_READY.sessionId())
            .append(" streamId=").append(IMAGE_READY.streamId())
            .append(" subscriberPositionId=").append(IMAGE_READY.subscriberPositionId())
            .append(" subscriptionRegistrationId=").append(IMAGE_READY.subscriptionRegistrationId())
            .append(" correlationId=").append(IMAGE_READY.correlationId());

        builder.append(" sourceIdentity=");
        IMAGE_READY.appendSourceIdentity(builder);
        builder.append(" logFileName=");
        IMAGE_READY.appendLogFileName(builder);
    }

    private static void dissectCorrelationEvent(final StringBuilder builder)
    {
        builder
            .append("clientId=").append(CORRELATED_MSG.clientId())
            .append(" correlationId=").append(CORRELATED_MSG.correlationId());
    }

    private static void dissectImage(final StringBuilder builder)
    {
        builder
            .append("streamId=").append(IMAGE_MSG.streamId())
            .append(" correlationId=").append(IMAGE_MSG.correlationId())
            .append(" subscriptionRegistrationId=")
            .append(IMAGE_MSG.subscriptionRegistrationId())
            .append(" channel=");

        IMAGE_MSG.appendChannel(builder);
    }

    private static void dissectRemoveCounterEvent(final StringBuilder builder)
    {
        builder
            .append("registrationId=").append(REMOVE_COUNTER.registrationId())
            .append(" clientId=").append(REMOVE_COUNTER.clientId())
            .append(" correlationId=").append(REMOVE_COUNTER.correlationId());
    }

    private static void dissectRemovePublicationEvent(final StringBuilder builder, final int captureLength)
    {
        builder
            .append("registrationId=").append(REMOVE_PUBLICATION.registrationId())
            .append(" clientId=").append(REMOVE_PUBLICATION.clientId())
            .append(" correlationId=").append(REMOVE_PUBLICATION.correlationId());

        if (REMOVE_PUBLICATION.flagsFieldIsValid(captureLength))
        {
            builder.append(" revoke=").append(REMOVE_PUBLICATION.revoke());
        }
    }

    private static void dissectRemoveSubscriptionEvent(final StringBuilder builder)
    {
        builder
            .append("registrationId=").append(REMOVE_SUBSCRIPTION.registrationId())
            .append(" clientId=").append(REMOVE_SUBSCRIPTION.clientId())
            .append(" correlationId=").append(REMOVE_SUBSCRIPTION.correlationId());
    }

    private static void dissectDestination(final StringBuilder builder)
    {
        builder
            .append("registrationCorrelationId=").append(DESTINATION_MSG.registrationCorrelationId())
            .append(" clientId=").append(DESTINATION_MSG.clientId())
            .append(" correlationId=").append(DESTINATION_MSG.correlationId())
            .append(" channel=");

        DESTINATION_MSG.appendChannel(builder);
    }

    private static void dissectError(final StringBuilder builder)
    {
        builder
            .append("offendingCommandCorrelationId=").append(ERROR_MSG.offendingCommandCorrelationId())
            .append(" errorCode=").append(ERROR_MSG.errorCode())
            .append(" message=");

        ERROR_MSG.appendMessage(builder);
    }

    private static void dissectCounter(final StringBuilder builder)
    {
        builder
            .append("typeId=").append(COUNTER_MSG.typeId())
            .append(" keyBufferOffset=").append(COUNTER_MSG.keyBufferOffset())
            .append(" keyBufferLength=").append(COUNTER_MSG.keyBufferLength())
            .append(" labelBufferOffset=").append(COUNTER_MSG.labelBufferOffset())
            .append(" labelBufferLength=").append(COUNTER_MSG.labelBufferLength())
            .append(" clientId=").append(COUNTER_MSG.clientId())
            .append(" correlationId=").append(COUNTER_MSG.correlationId());
    }

    private static void dissectCounterUpdate(final StringBuilder builder)
    {
        builder
            .append("correlationId=").append(COUNTER_UPDATE.correlationId())
            .append(" counterId=").append(COUNTER_UPDATE.counterId());
    }

    private static void dissectOperationSuccess(final StringBuilder builder)
    {
        builder.append("correlationId=").append(OPERATION_SUCCEEDED.correlationId());
    }

    private static void dissectSubscriptionReady(final StringBuilder builder)
    {
        builder
            .append("correlationId=").append(SUBSCRIPTION_READY.correlationId())
            .append(" channelStatusCounterId=").append(SUBSCRIPTION_READY.channelStatusCounterId());
    }

    private static void dissectClientTimeout(final StringBuilder builder)
    {
        builder.append("clientId=").append(CLIENT_TIMEOUT.clientId());
    }

    private static void dissectTerminateDriver(final StringBuilder builder)
    {
        builder
            .append("clientId=").append(TERMINATE_DRIVER.clientId())
            .append(" tokenBufferLength=").append(TERMINATE_DRIVER.tokenBufferLength());
    }

    private static void dissectDestinationById(final StringBuilder builder)
    {
        builder
            .append("resourceRegistrationId=").append(DESTINATION_BY_ID.resourceRegistrationId())
            .append(" destinationRegistrationId=").append(DESTINATION_BY_ID.destinationRegistrationId());
    }

    private static void dissectRejectImage(final StringBuilder builder)
    {
        builder
            .append("clientId=").append(REJECT_IMAGE.clientId())
            .append(" correlationId=").append(REJECT_IMAGE.correlationId())
            .append(" imageCorrelationId=").append(REJECT_IMAGE.imageCorrelationId())
            .append(" position=").append(REJECT_IMAGE.position())
            .append(" reason=").append(REJECT_IMAGE.reason());
    }
}
