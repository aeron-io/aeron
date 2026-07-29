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

import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logging.BinaryRenderer;
import io.aeron.logging.CommonEventEncoder;
import io.aeron.protocol.*;
import org.agrona.DirectBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.util.Arrays;

/**
 * Binary renderer for the Aeron network protocol messages.
 */
public class DriverProtocolBinaryRenderer implements BinaryRenderer
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

    private static final int[] MSG_TYPE_ID = {
        DriverEventCode.FRAME_IN.toEventCodeId(),
        DriverEventCode.FRAME_OUT.toEventCodeId()
    };

    /**
     * Default constructor.
     */
    public DriverProtocolBinaryRenderer()
    {
    }

    /**
     * {@inheritDoc}
     */
    public int[] supportingMsgTypeIds()
    {
        return Arrays.copyOf(MSG_TYPE_ID, MSG_TYPE_ID.length);
    }

    /**
     * {@inheritDoc}
     */
    public void append(
        final StringBuilder sb,
        final int msgTypeId,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        final int frameType = frameType(buffer, offset);
        switch (frameType)
        {
            case HeaderFlyweight.HDR_TYPE_PAD:
            case HeaderFlyweight.HDR_TYPE_DATA:
                DATA_HEADER.wrap(buffer, offset, buffer.capacity() - offset);
                renderDataFrame(sb);
                break;

            case HeaderFlyweight.HDR_TYPE_NAK:
                NAK_HEADER.wrap(buffer, offset, buffer.capacity() - offset);
                renderNakFrame(sb);
                break;

            case HeaderFlyweight.HDR_TYPE_SM:
                SM_HEADER.wrap(buffer, offset, buffer.capacity() - offset);
                renderStatusFrame(sb);
                break;

            case HeaderFlyweight.HDR_TYPE_ERR:
                ERROR_HEADER.wrap(buffer, offset, buffer.capacity() - offset);
                renderErrorFrame(sb);
                break;

            case HeaderFlyweight.HDR_TYPE_SETUP:
                SETUP_HEADER.wrap(buffer, offset, buffer.capacity() - offset);
                renderSetupFrame(sb);
                break;

            case HeaderFlyweight.HDR_TYPE_RTTM:
                RTT_MEASUREMENT.wrap(buffer, offset, buffer.capacity() - offset);
                renderRttFrame(sb);
                break;

            case HeaderFlyweight.HDR_TYPE_RES:
                renderResFrame(buffer, offset, sb);
                break;

            case HeaderFlyweight.HDR_TYPE_RSP_SETUP:
                RSP_SETUP.wrap(buffer, offset, buffer.capacity() - offset);
                renderRspSetupFrame(sb);
                break;

            default:
                sb.append("type=UNKNOWN(").append(frameType).append(")");
                break;
        }
    }

    private static int frameType(final DirectBuffer buffer, final int offset)
    {
        return buffer.getShort(FrameDescriptor.typeOffset(offset), LITTLE_ENDIAN) & 0xFFFF;
    }

    private static void renderDataFrame(final StringBuilder sb)
    {
        sb
            .append("type=")
            .append(DATA_HEADER.headerType() == HeaderFlyweight.HDR_TYPE_PAD ? "PAD" : "DATA")
            .append(" flags=");

        HeaderFlyweight.appendFlagsAsChars(DATA_HEADER.flags(), sb);

        sb
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

    private static void renderStatusFrame(final StringBuilder sb)
    {
        sb.append("type=SM flags=");
        HeaderFlyweight.appendFlagsAsChars(SM_HEADER.flags(), sb);

        sb
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

    private static void renderNakFrame(final StringBuilder sb)
    {
        sb.append("type=NAK flags=");
        HeaderFlyweight.appendFlagsAsChars(NAK_HEADER.flags(), sb);

        sb
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

    private static void renderErrorFrame(final StringBuilder sb)
    {
        sb.append("type=ERR flags=");
        HeaderFlyweight.appendFlagsAsChars(ERROR_HEADER.flags(), sb);

        sb
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

    private static void renderSetupFrame(final StringBuilder sb)
    {
        sb.append("type=SETUP flags=");
        HeaderFlyweight.appendFlagsAsChars(SETUP_HEADER.flags(), sb);

        sb
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

    private static void renderRttFrame(final StringBuilder sb)
    {
        sb.append("type=RTT flags=");
        HeaderFlyweight.appendFlagsAsChars(RTT_MEASUREMENT.flags(), sb);

        sb
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

    private static void renderResFrame(final DirectBuffer buffer, final int offset, final StringBuilder sb)
    {
        int currentOffset = offset;

        HEADER.wrap(buffer, offset, buffer.capacity() - offset);
        final int length = offset + Math.min(HEADER.frameLength(), CommonEventEncoder.MAX_CAPTURE_LENGTH);
        currentOffset += HeaderFlyweight.MIN_HEADER_LENGTH;

        sb.append("type=RES flags=");
        HeaderFlyweight.appendFlagsAsChars(HEADER.flags(), sb);

        sb
            .append(" frameLength=")
            .append(HEADER.frameLength());

        while (length > currentOffset)
        {
            RESOLUTION.wrap(buffer, currentOffset, buffer.capacity() - currentOffset);

            if ((length - offset) < RESOLUTION.entryLength())
            {
                sb.append(" ... ").append(length - offset).append(" bytes left");
                break;
            }

            renderResEntry(sb);

            currentOffset += RESOLUTION.entryLength();
        }
    }

    private static void renderRspSetupFrame(final StringBuilder sb)
    {
        sb.append("type=RSP_SETUP flags=");
        HeaderFlyweight.appendFlagsAsChars(RSP_SETUP.flags(), sb);

        sb
            .append(" frameLength=")
            .append(RSP_SETUP.frameLength())
            .append(" sessionId=")
            .append(RSP_SETUP.sessionId())
            .append(" streamId=")
            .append(RSP_SETUP.streamId())
            .append(" responseSessionId=")
            .append(RSP_SETUP.responseSessionId());
    }

    private static void renderResEntry(final StringBuilder sb)
    {
        sb
            .append(" [resType=")
            .append(RESOLUTION.resType())
            .append(" flags=");

        HeaderFlyweight.appendFlagsAsChars(RESOLUTION.flags(), sb);

        sb
            .append(" port=")
            .append(RESOLUTION.udpPort())
            .append(" ageInMs=")
            .append(RESOLUTION.ageInMs());

        sb.append(" address=");
        RESOLUTION.appendAddress(sb);

        sb.append(" name=");
        RESOLUTION.appendName(sb);
        sb.append(']');
    }
}
