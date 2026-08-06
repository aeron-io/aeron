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
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.ErrorFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.NakFlyweight;
import io.aeron.protocol.ResolutionEntryFlyweight;
import io.aeron.protocol.ResponseSetupFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.SetupFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.DirectBuffer;

import java.util.Arrays;

import static io.aeron.logging.BinaryRenderer.renderTruncated;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.PrintBufferUtil.appendPrettyHexDump;

/**
 * Binary renderer for the Aeron network protocol messages.
 */
public class DriverProtocolBinaryRenderer implements BinaryRenderer
{
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final NakFlyweight nakHeader = new NakFlyweight();
    private final StatusMessageFlyweight smHeader = new StatusMessageFlyweight();
    private final ErrorFlyweight errorHeader = new ErrorFlyweight();
    private final SetupFlyweight setupHeader = new SetupFlyweight();
    private final RttMeasurementFlyweight rttMeasurement = new RttMeasurementFlyweight();
    private final HeaderFlyweight header = new HeaderFlyweight();
    private final ResolutionEntryFlyweight resolution = new ResolutionEntryFlyweight();
    private final ResponseSetupFlyweight rspSetup = new ResponseSetupFlyweight();

    private final boolean renderExtraBytes;

    private static final int[] MSG_TYPE_ID = {
        DriverEventCode.FRAME_IN.toEventCodeId(),
        DriverEventCode.FRAME_OUT.toEventCodeId()
    };

    private final String newLine = String.format("%n");

    /**
     * Default constructor.
     */
    public DriverProtocolBinaryRenderer()
    {
        this(RENDER_DATA_CONTENT);
    }

    /**
     * Constructor allowing explicit control of raw byte rendering.
     *
     * @param renderExtraBytes whether to render raw bytes of otherwise unlogged fields as a pretty hex/ASCII dump.
     */
    public DriverProtocolBinaryRenderer(final boolean renderExtraBytes)
    {
        this.renderExtraBytes = renderExtraBytes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int[] supportingMsgTypeIds()
    {
        return Arrays.copyOf(MSG_TYPE_ID, MSG_TYPE_ID.length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void append(
        final StringBuilder sb,
        final int msgTypeId,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        if (length < HeaderFlyweight.MIN_HEADER_LENGTH)
        {
            renderTruncated(sb);
            return;
        }

        final int frameType = frameType(buffer, offset);
        switch (frameType)
        {
            case HeaderFlyweight.HDR_TYPE_PAD:
            case HeaderFlyweight.HDR_TYPE_DATA:
                if (length < DataHeaderFlyweight.HEADER_LENGTH)
                {
                    renderTruncated(sb);
                    break;
                }

                dataHeader.wrap(buffer, offset, buffer.capacity() - offset);
                renderDataFrame(sb, buffer, offset);
                break;

            case HeaderFlyweight.HDR_TYPE_NAK:
                if (length < NakFlyweight.HEADER_LENGTH)
                {
                    renderTruncated(sb);
                    break;
                }

                nakHeader.wrap(buffer, offset, buffer.capacity() - offset);
                renderNakFrame(sb);
                break;

            case HeaderFlyweight.HDR_TYPE_SM:
                if (length < StatusMessageFlyweight.HEADER_LENGTH)
                {
                    renderTruncated(sb);
                    break;
                }

                smHeader.wrap(buffer, offset, buffer.capacity() - offset);
                renderStatusFrame(sb);
                break;

            case HeaderFlyweight.HDR_TYPE_ERR:
                if (length < ErrorFlyweight.HEADER_LENGTH)
                {
                    renderTruncated(sb);
                    break;
                }

                errorHeader.wrap(buffer, offset, buffer.capacity() - offset);
                renderErrorFrame(sb);
                break;

            case HeaderFlyweight.HDR_TYPE_SETUP:
                if (length < SetupFlyweight.HEADER_LENGTH)
                {
                    renderTruncated(sb);
                    break;
                }

                setupHeader.wrap(buffer, offset, buffer.capacity() - offset);
                renderSetupFrame(sb);
                break;

            case HeaderFlyweight.HDR_TYPE_RTTM:
                if (length < RttMeasurementFlyweight.HEADER_LENGTH)
                {
                    renderTruncated(sb);
                    break;
                }

                rttMeasurement.wrap(buffer, offset, buffer.capacity() - offset);
                renderRttFrame(sb);
                break;

            case HeaderFlyweight.HDR_TYPE_RES:
                renderResFrame(buffer, offset, length, sb);
                break;

            case HeaderFlyweight.HDR_TYPE_RSP_SETUP:
                if (length < ResponseSetupFlyweight.HEADER_LENGTH)
                {
                    renderTruncated(sb);
                    break;
                }

                rspSetup.wrap(buffer, offset, buffer.capacity() - offset);
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

    private void renderDataFrame(final StringBuilder sb, final DirectBuffer buffer, final int offset)
    {
        sb
            .append("type=")
            .append(dataHeader.headerType() == HeaderFlyweight.HDR_TYPE_PAD ? "PAD" : "DATA")
            .append(" flags=");

        HeaderFlyweight.appendFlagsAsChars(dataHeader.flags(), sb);

        sb
            .append(" frameLength=")
            .append(dataHeader.frameLength())
            .append(" sessionId=")
            .append(dataHeader.sessionId())
            .append(" streamId=")
            .append(dataHeader.streamId())
            .append(" termId=")
            .append(dataHeader.termId())
            .append(" termOffset=")
            .append(dataHeader.termOffset());

        if (renderExtraBytes)
        {
            final int payloadOffset = offset + DataHeaderFlyweight.HEADER_LENGTH;
            final int payloadEnd = offset + dataHeader.frameLength();
            final int payloadLength = payloadEnd - payloadOffset;

            if (payloadLength > 0)
            {
                sb.append(" payload=").append(newLine);
                appendPrettyHexDump(sb, buffer, payloadOffset, payloadLength);
            }
        }
    }

    private void renderStatusFrame(final StringBuilder sb)
    {
        sb.append("type=SM flags=");
        HeaderFlyweight.appendFlagsAsChars(smHeader.flags(), sb);

        sb
            .append(" frameLength=")
            .append(smHeader.frameLength())
            .append(" sessionId=")
            .append(smHeader.sessionId())
            .append(" streamId=")
            .append(smHeader.streamId())
            .append(" termId=")
            .append(smHeader.consumptionTermId())
            .append(" termOffset=")
            .append(smHeader.consumptionTermOffset())
            .append(" receiverWindowLength=")
            .append(smHeader.receiverWindowLength())
            .append(" receiverId=")
            .append(smHeader.receiverId());
    }

    private void renderNakFrame(final StringBuilder sb)
    {
        sb.append("type=NAK flags=");
        HeaderFlyweight.appendFlagsAsChars(nakHeader.flags(), sb);

        sb
            .append(" frameLength=")
            .append(nakHeader.frameLength())
            .append(" sessionId=")
            .append(nakHeader.sessionId())
            .append(" streamId=")
            .append(nakHeader.streamId())
            .append(" termId=")
            .append(nakHeader.termId())
            .append(" termOffset=")
            .append(nakHeader.termOffset())
            .append(" length=")
            .append(nakHeader.length());
    }

    private void renderErrorFrame(final StringBuilder sb)
    {
        sb.append("type=ERR flags=");
        HeaderFlyweight.appendFlagsAsChars(errorHeader.flags(), sb);

        sb
            .append(" frameLength=")
            .append(errorHeader.frameLength())
            .append(" sessionId=")
            .append(errorHeader.sessionId())
            .append(" streamId=")
            .append(errorHeader.streamId())
            .append(" receiverId=")
            .append(errorHeader.receiverId())
            .append(" groupTag=")
            .append(errorHeader.groupTag())
            .append(" errorCode=")
            .append(errorHeader.errorCode())
            .append(" errorMessage=\"")
            .append(errorHeader.errorMessage())
            .append('"');
    }

    private void renderSetupFrame(final StringBuilder sb)
    {
        sb.append("type=SETUP flags=");
        HeaderFlyweight.appendFlagsAsChars(setupHeader.flags(), sb);

        sb
            .append(" frameLength=")
            .append(setupHeader.frameLength())
            .append(" sessionId=")
            .append(setupHeader.sessionId())
            .append(" streamId=")
            .append(setupHeader.streamId())
            .append(" activeTermId=")
            .append(setupHeader.activeTermId())
            .append(" initialTermId=")
            .append(setupHeader.initialTermId())
            .append(" termOffset=")
            .append(setupHeader.termOffset())
            .append(" termLength=")
            .append(setupHeader.termLength())
            .append(" mtu=")
            .append(setupHeader.mtuLength())
            .append(" ttl=")
            .append(setupHeader.ttl());
    }

    private void renderRttFrame(final StringBuilder sb)
    {
        sb.append("type=RTT flags=");
        HeaderFlyweight.appendFlagsAsChars(rttMeasurement.flags(), sb);

        sb
            .append(" frameLength=")
            .append(rttMeasurement.frameLength())
            .append(" sessionId=")
            .append(rttMeasurement.sessionId())
            .append(" streamId=")
            .append(rttMeasurement.streamId())
            .append(" echoTimestampNs=")
            .append(rttMeasurement.echoTimestampNs())
            .append(" receptionDelta=")
            .append(rttMeasurement.receptionDelta())
            .append(" receiverId=")
            .append(rttMeasurement.receiverId());
    }

    private void renderResFrame(
        final DirectBuffer buffer, final int offset, final int length, final StringBuilder sb)
    {
        int currentOffset = offset;

        header.wrap(buffer, offset, buffer.capacity() - offset);
        final int availableEnd = offset + length;
        final int declaredEnd = offset + Math.min(header.frameLength(), CommonEventEncoder.MAX_CAPTURE_LENGTH);
        final int entriesEnd = Math.min(declaredEnd, availableEnd);
        currentOffset += HeaderFlyweight.MIN_HEADER_LENGTH;

        sb.append("type=RES flags=");
        HeaderFlyweight.appendFlagsAsChars(header.flags(), sb);

        sb
            .append(" frameLength=")
            .append(header.frameLength());

        while (entriesEnd > currentOffset)
        {
            final int remaining = availableEnd - currentOffset;
            if (remaining < ResolutionEntryFlyweight.MIN_IPV4_FRAME_LENGTH)
            {
                sb.append(" ... ").append(remaining).append(" bytes left");
                break;
            }

            resolution.wrap(buffer, currentOffset, buffer.capacity() - currentOffset);

            final byte resType = resolution.resType();
            if (ResolutionEntryFlyweight.RES_TYPE_NAME_TO_IP4_MD != resType &&
                ResolutionEntryFlyweight.RES_TYPE_NAME_TO_IP6_MD != resType)
            {
                sb.append(" ... invalid resType=").append(resType);
                break;
            }

            final int minFrameLength = ResolutionEntryFlyweight.RES_TYPE_NAME_TO_IP4_MD == resType ?
                ResolutionEntryFlyweight.MIN_IPV4_FRAME_LENGTH : ResolutionEntryFlyweight.MIN_IPV6_FRAME_LENGTH;
            if (remaining < minFrameLength)
            {
                sb.append(" ... ").append(remaining).append(" bytes left");
                break;
            }

            if ((entriesEnd - offset) < resolution.entryLength())
            {
                sb.append(" ... ").append(entriesEnd - offset).append(" bytes left");
                break;
            }

            renderResEntry(sb);

            currentOffset += resolution.entryLength();
        }
    }

    private void renderRspSetupFrame(final StringBuilder sb)
    {
        sb.append("type=RSP_SETUP flags=");
        HeaderFlyweight.appendFlagsAsChars(rspSetup.flags(), sb);

        sb
            .append(" frameLength=")
            .append(rspSetup.frameLength())
            .append(" sessionId=")
            .append(rspSetup.sessionId())
            .append(" streamId=")
            .append(rspSetup.streamId())
            .append(" responseSessionId=")
            .append(rspSetup.responseSessionId());
    }

    private void renderResEntry(final StringBuilder sb)
    {
        sb
            .append(" [resType=")
            .append(resolution.resType())
            .append(" flags=");

        HeaderFlyweight.appendFlagsAsChars(resolution.flags(), sb);

        sb
            .append(" port=")
            .append(resolution.udpPort())
            .append(" ageInMs=")
            .append(resolution.ageInMs());

        sb.append(" address=");
        resolution.appendAddress(sb);

        sb.append(" name=");
        resolution.appendName(sb);
        sb.append(']');
    }
}
