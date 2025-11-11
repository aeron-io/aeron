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
package io.aeron.driver.media;

import io.aeron.protocol.ErrorFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.NakFlyweight;
import io.aeron.protocol.ResponseSetupFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;

import java.nio.ByteBuffer;
import java.util.UUID;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Thread local variables that will only be accessed in the context of the Receiver agent thread from within a
 * {@link ReceiveChannelEndpoint} subclass.
 */
public final class ReceiveChannelEndpointThreadLocals
{
    private final ByteBuffer smBuffer;
    private final StatusMessageFlyweight statusMessageFlyweight;
    private final ByteBuffer nakBuffer;
    private final NakFlyweight nakFlyweight;
    private final ByteBuffer rttMeasurementBuffer;
    private final RttMeasurementFlyweight rttMeasurementFlyweight;
    private final ByteBuffer responseSetupBuffer;
    private final ResponseSetupFlyweight responseSetupHeader;
    private final ByteBuffer errorBuffer;
    private final ErrorFlyweight errorFlyweight;
    private long nextReceiverId;

    /**
     * Construct a set of local state to be used by the receiver thread.
     */
    public ReceiveChannelEndpointThreadLocals()
    {
        final int smLength = StatusMessageFlyweight.HEADER_LENGTH + SIZE_OF_LONG;
        final int bufferLength =
            BitUtil.align(smLength, CACHE_LINE_LENGTH) +
            BitUtil.align(NakFlyweight.HEADER_LENGTH, CACHE_LINE_LENGTH) +
            BitUtil.align(RttMeasurementFlyweight.HEADER_LENGTH, CACHE_LINE_LENGTH) +
            BitUtil.align(ResponseSetupFlyweight.HEADER_LENGTH, CACHE_LINE_LENGTH) +
            BitUtil.align(ErrorFlyweight.MAX_ERROR_FRAME_LENGTH, CACHE_LINE_LENGTH);

        final UUID uuid = UUID.randomUUID();
        nextReceiverId = uuid.getMostSignificantBits() ^ uuid.getLeastSignificantBits();

        final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(bufferLength, CACHE_LINE_LENGTH);

        byteBuffer.limit(smLength);
        smBuffer = byteBuffer.slice();
        statusMessageFlyweight = new StatusMessageFlyweight(smBuffer);

        final int nakMessageOffset = BitUtil.align(smLength, FRAME_ALIGNMENT);
        byteBuffer.limit(nakMessageOffset + NakFlyweight.HEADER_LENGTH).position(nakMessageOffset);
        nakBuffer = byteBuffer.slice();
        nakFlyweight = new NakFlyweight(nakBuffer);

        final int rttMeasurementOffset = nakMessageOffset + BitUtil.align(NakFlyweight.HEADER_LENGTH, FRAME_ALIGNMENT);
        byteBuffer.limit(rttMeasurementOffset + RttMeasurementFlyweight.HEADER_LENGTH).position(rttMeasurementOffset);
        rttMeasurementBuffer = byteBuffer.slice();
        rttMeasurementFlyweight = new RttMeasurementFlyweight(rttMeasurementBuffer);

        final int responseSetupOffset = rttMeasurementOffset + BitUtil.align(
            RttMeasurementFlyweight.HEADER_LENGTH, CACHE_LINE_LENGTH);
        byteBuffer.limit(responseSetupOffset + ResponseSetupFlyweight.HEADER_LENGTH).position(responseSetupOffset);
        responseSetupBuffer = byteBuffer.slice();
        responseSetupHeader = new ResponseSetupFlyweight(responseSetupBuffer);

        final int errorOffset = responseSetupOffset + BitUtil.align(
            ResponseSetupFlyweight.HEADER_LENGTH, FRAME_ALIGNMENT);
        byteBuffer.limit(errorOffset + ErrorFlyweight.MAX_ERROR_FRAME_LENGTH).position(errorOffset);
        errorBuffer = byteBuffer.slice();
        errorFlyweight = new ErrorFlyweight(errorBuffer);

        statusMessageFlyweight
            .version(HeaderFlyweight.CURRENT_VERSION)
            .headerType(HeaderFlyweight.HDR_TYPE_SM)
            .frameLength(StatusMessageFlyweight.HEADER_LENGTH);

        nakFlyweight
            .version(HeaderFlyweight.CURRENT_VERSION)
            .headerType(HeaderFlyweight.HDR_TYPE_NAK)
            .frameLength(NakFlyweight.HEADER_LENGTH);

        rttMeasurementFlyweight
            .version(HeaderFlyweight.CURRENT_VERSION)
            .headerType(HeaderFlyweight.HDR_TYPE_RTTM)
            .frameLength(RttMeasurementFlyweight.HEADER_LENGTH);

        responseSetupHeader
            .version(HeaderFlyweight.CURRENT_VERSION)
            .headerType(HeaderFlyweight.HDR_TYPE_RSP_SETUP)
            .frameLength(ResponseSetupFlyweight.HEADER_LENGTH);

        errorFlyweight
            .version(HeaderFlyweight.CURRENT_VERSION)
            .headerType(HeaderFlyweight.HDR_TYPE_ERR)
            .frameLength(ErrorFlyweight.HEADER_LENGTH);
    }

    /**
     * Buffer for writing status messages to send.
     *
     * @return buffer for writing status messages to send.
     */
    public ByteBuffer statusMessageBuffer()
    {
        return smBuffer;
    }

    /**
     * Flyweight over the {@link #statusMessageBuffer()}.
     *
     * @return flyweight over the {@link #statusMessageBuffer()}.
     */
    public StatusMessageFlyweight statusMessageFlyweight()
    {
        return statusMessageFlyweight;
    }

    /**
     * Buffer for writing NAK messages to send.
     *
     * @return buffer for writing NAK messages to send.
     */
    public ByteBuffer nakBuffer()
    {
        return nakBuffer;
    }

    /**
     * Flyweight over the {@link #nakBuffer()}.
     *
     * @return flyweight over the {@link #nakBuffer()}.
     */
    public NakFlyweight nakFlyweight()
    {
        return nakFlyweight;
    }

    /**
     * Buffer for writing RTT measurement messages to send.
     *
     * @return buffer for writing RTT measurement messages to send.
     */
    public ByteBuffer rttMeasurementBuffer()
    {
        return rttMeasurementBuffer;
    }

    /**
     * Flyweight over the {@link #rttMeasurementBuffer()}.
     *
     * @return flyweight over the {@link #rttMeasurementBuffer()}.
     */
    public RttMeasurementFlyweight rttMeasurementFlyweight()
    {
        return rttMeasurementFlyweight;
    }

    /**
     * Buffer for writing Response Setup messages to send.
     *
     * @return buffer for writing Response Setup messages to send.
     */
    public ByteBuffer responseSetupBuffer()
    {
        return responseSetupBuffer;
    }

    /**
     * Flyweight over the {@link #responseSetupBuffer()}.
     *
     * @return flyweight over the {@link #responseSetupBuffer()}.
     */
    public ResponseSetupFlyweight responseSetupHeader()
    {
        return responseSetupHeader;
    }

    /**
     * Buffer for writing the Error messages to send.
     *
     * @return buffer for writing the error messages to send.
     */
    public ByteBuffer errorBuffer()
    {
        return errorBuffer;
    }

    /**
     * Flyweight over the {@link #errorBuffer()}.
     *
     * @return flyweight over the {@link #errorBuffer()}.
     */
    public ErrorFlyweight errorFlyweight()
    {
        return errorFlyweight;
    }

    /**
     * Get the next receiver id to be used for a receiver channel identity.
     *
     * @return the next receiver id to be used for a receiver channel identity.
     */
    public long nextReceiverId()
    {
        return nextReceiverId++;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "ReceiveChannelEndpointThreadLocals{" +
            "nextReceiverId=" + nextReceiverId +
            '}';
    }
}
