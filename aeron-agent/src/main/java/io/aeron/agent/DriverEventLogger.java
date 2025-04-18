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

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.agent.CommonEventEncoder.encode;
import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.DriverEventCode.*;
import static io.aeron.agent.DriverEventEncoder.encode;
import static io.aeron.agent.DriverEventEncoder.*;
import static io.aeron.agent.EventConfiguration.EVENT_RING_BUFFER;
import static org.agrona.BitUtil.*;

/**
 * Event logger interface used by interceptors for recording into a {@link RingBuffer} for a
 * {@link io.aeron.driver.MediaDriver} via a Java Agent.
 */
public final class DriverEventLogger
{
    /**
     * Logger for writing into the {@link EventConfiguration#EVENT_RING_BUFFER}.
     */
    public static final DriverEventLogger LOGGER = new DriverEventLogger(EVENT_RING_BUFFER);

    /**
     * Maximum length of a host name.
     */
    public static final int MAX_HOST_NAME_LENGTH = 256;

    /**
     * Maximum length of a Channel URI.
     */
    public static final int MAX_CHANNEL_URI_LENGTH = 4096;

    private final ManyToOneRingBuffer ringBuffer;

    DriverEventLogger(final ManyToOneRingBuffer ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    /**
     * Log an event for the driver.
     *
     * @param code   for the type of event.
     * @param buffer containing the encoded event.
     * @param offset in the buffer at which the event begins.
     * @param length of the encoded event.
     */
    public void log(final DriverEventCode code, final DirectBuffer buffer, final int offset, final int length)
    {
        if (DriverComponentLogger.ENABLED_EVENTS.contains(code))
        {
            final int captureLength = captureLength(length);
            final int encodedLength = encodedLength(captureLength);

            final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
            final int index = ringBuffer.tryClaim(toEventCodeId(code), encodedLength);
            if (index > 0)
            {
                try
                {
                    encode((UnsafeBuffer)ringBuffer.buffer(), index, captureLength, length, buffer, offset);
                }
                finally
                {
                    ringBuffer.commit(index);
                }
            }
        }
    }

    /**
     * Log a frame coming in from the media.
     *
     * @param buffer      containing the frame.
     * @param offset      in the buffer at which the frame begins.
     * @param frameLength of the frame.
     * @param dstAddress  for the frame.
     */
    public void logFrameIn(
        final DirectBuffer buffer, final int offset, final int frameLength, final InetSocketAddress dstAddress)
    {
        final int length = frameLength + socketAddressLength(dstAddress);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(FRAME_IN), encodedLength);
        if (index > 0)
        {
            try
            {
                encode((UnsafeBuffer)ringBuffer.buffer(), index, captureLength, length, buffer, offset, dstAddress);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log a frame being sent out from the driver to the media.
     *
     * @param buffer     containing the frame.
     * @param dstAddress for the frame.
     */
    public void logFrameOut(final ByteBuffer buffer, final InetSocketAddress dstAddress)
    {
        final int length = buffer.remaining() + socketAddressLength(dstAddress);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(FRAME_OUT), encodedLength);
        if (index > 0)
        {
            try
            {
                encode(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    buffer,
                    buffer.position(),
                    dstAddress);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log the removal of a publication.
     *
     * @param channel   for the channel.
     * @param sessionId for the publication.
     * @param streamId  within the channel.
     */
    public void logPublicationRemoval(final String channel, final int sessionId, final int streamId)
    {
        final int length = SIZE_OF_INT * 3 + channel.length();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(REMOVE_PUBLICATION_CLEANUP), encodedLength);
        if (index > 0)
        {
            try
            {
                final UnsafeBuffer buffer = (UnsafeBuffer)ringBuffer.buffer();
                encodePublicationRemoval(buffer, index, captureLength, length, channel, sessionId, streamId);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log the removal of a subscription.
     *
     * @param channel        for the channel.
     * @param streamId       within the channel.
     * @param subscriptionId for the subscription.
     */
    public void logSubscriptionRemoval(final String channel, final int streamId, final long subscriptionId)
    {
        final int length = SIZE_OF_INT * 2 + SIZE_OF_LONG + channel.length();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(REMOVE_SUBSCRIPTION_CLEANUP), encodedLength);
        if (index > 0)
        {
            try
            {
                final UnsafeBuffer buffer = (UnsafeBuffer)ringBuffer.buffer();
                encodeSubscriptionRemoval(buffer, index, captureLength, length, channel, streamId, subscriptionId);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log the removal of an image from the driver.
     *
     * @param channel       for the channel.
     * @param sessionId     for the image.
     * @param streamId      for the image.
     * @param correlationId for the image.
     */
    public void logImageRemoval(final String channel, final int sessionId, final int streamId, final long correlationId)
    {
        final int length = SIZE_OF_INT * 3 + SIZE_OF_LONG + channel.length();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(REMOVE_IMAGE_CLEANUP), encodedLength);
        if (index > 0)
        {
            try
            {
                final UnsafeBuffer buffer = (UnsafeBuffer)ringBuffer.buffer();
                encodeImageRemoval(buffer, index, captureLength, length, channel, sessionId, streamId, correlationId);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log a generic string associated with an event.
     *
     * @param code  for the event type.
     * @param value of the string to be logged.
     */
    public void logString(final DriverEventCode code, final String value)
    {
        final int length = value.length() + SIZE_OF_INT;
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(code), encodedLength);
        if (index > 0)
        {
            try
            {
                encode((UnsafeBuffer)ringBuffer.buffer(), index, captureLength, length, value);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log an untethered subscription state change.
     *
     * @param oldState       before the change.
     * @param newState       after the change.
     * @param subscriptionId to which the change applies.
     * @param streamId       of the image.
     * @param sessionId      of the image.
     * @param <E>            type of the event.
     */
    public <E extends Enum<E>> void logUntetheredSubscriptionStateChange(
        final E oldState, final E newState, final long subscriptionId, final int streamId, final int sessionId)
    {
        final int length = untetheredSubscriptionStateChangeLength(oldState, newState);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(UNTETHERED_SUBSCRIPTION_STATE_CHANGE), encodedLength);

        if (index > 0)
        {
            try
            {
                encodeUntetheredSubscriptionStateChange(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    oldState,
                    newState,
                    subscriptionId,
                    streamId,
                    sessionId);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log an address with associated event.
     *
     * @param code    representing the event type.
     * @param address to be logged.
     */
    public void logAddress(final DriverEventCode code, final InetSocketAddress address)
    {
        final int length = socketAddressLength(address);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(code), encodedLength);
        if (index > 0)
        {
            try
            {
                encode((UnsafeBuffer)ringBuffer.buffer(), index, captureLength, length, address);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log a resolution for a resolver and the associated result.
     *
     * @param resolverName   simple class name of the resolver.
     * @param durationNs     of the call in nanoseconds.
     * @param name           host name being resolved.
     * @param isReResolution {@code true} if this is a re-resolution or {@code false} if initial resolution.
     * @param address        address that was resolved to, can be {@code null}.
     */
    public void logResolve(
        final String resolverName,
        final long durationNs,
        final String name,
        final boolean isReResolution,
        final InetAddress address)
    {
        final int length = SIZE_OF_BOOLEAN + SIZE_OF_LONG +
            trailingStringLength(resolverName, MAX_HOST_NAME_LENGTH) +
            trailingStringLength(name, MAX_HOST_NAME_LENGTH) +
            inetAddressLength(address);

        final int encodedLength = encodedLength(length);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(NAME_RESOLUTION_RESOLVE), encodedLength);
        if (index > 0)
        {
            try
            {
                encodeResolve(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    length,
                    length,
                    resolverName,
                    durationNs,
                    name,
                    isReResolution,
                    address);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log a resolution for a resolver and the associated result.
     *
     * @param resolverName simple class name of the resolver
     * @param durationNs   of the call in nanoseconds.
     * @param name         host name being resolved.
     * @param isReLookup   address that was resolved to, can be null.
     * @param resolvedName address that was resolved to, can be null.
     */
    public void logLookup(
        final String resolverName,
        final long durationNs,
        final String name,
        final boolean isReLookup,
        final String resolvedName)
    {
        final int length = SIZE_OF_LONG + trailingStringLength(resolverName, MAX_HOST_NAME_LENGTH) +
            trailingStringLength(name, MAX_HOST_NAME_LENGTH) + SIZE_OF_BOOLEAN +
            trailingStringLength(resolvedName, MAX_HOST_NAME_LENGTH);

        final int encodedLength = encodedLength(length);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(NAME_RESOLUTION_LOOKUP), encodedLength);
        if (index > 0)
        {
            try
            {
                encodeLookup(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    length,
                    length,
                    resolverName,
                    durationNs,
                    name,
                    isReLookup,
                    resolvedName);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log a host name resolution duration.
     *
     * @param durationNs of the call in nanoseconds.
     * @param hostName   host name being resolved.
     */
    public void logHostName(final long durationNs, final String hostName)
    {
        final int length = SIZE_OF_LONG + trailingStringLength(hostName, MAX_HOST_NAME_LENGTH);

        final int encodedLength = encodedLength(length);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(NAME_RESOLUTION_HOST_NAME), encodedLength);
        if (index > 0)
        {
            try
            {
                encodeHostName(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    length,
                    length,
                    durationNs,
                    hostName);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log the information about receiver for the corresponding flow control event.
     *
     * @param code          flow control event type.
     * @param receiverId    of the receiver.
     * @param sessionId     of the image.
     * @param streamId      of the image.
     * @param channel       uri of the channel.
     * @param receiverCount number of the receivers after the event.
     */
    public void logFlowControlReceiver(
        final DriverEventCode code,
        final long receiverId,
        final int sessionId,
        final int streamId,
        final String channel,
        final int receiverCount)
    {
        final int length = SIZE_OF_INT * 4 + SIZE_OF_LONG + channel.length();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(code), encodedLength);
        if (index > 0)
        {
            try
            {
                encodeFlowControlReceiver(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    receiverId,
                    sessionId,
                    streamId,
                    channel,
                    receiverCount);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Logs a NAK message sent by the receiver for a single control address or received by the sender.
     *
     * @param eventCode  to log Nak by.
     * @param address    Nak UDP destination/source.
     * @param sessionId  of the Nak.
     * @param streamId   of the Nak.
     * @param termId     of the Nak.
     * @param termOffset of the Nak.
     * @param nakLength  of the Nak.
     * @param channel    of the Nak.
     */
    public void logNakMessage(
        final DriverEventCode eventCode,
        final InetSocketAddress address,
        final int sessionId,
        final int streamId,
        final int termId,
        final int termOffset,
        final int nakLength,
        final String channel)
    {
        final int length = socketAddressLength(address) + (SIZE_OF_INT * 6) + channel.length();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(eventCode), encodedLength);
        if (index > 0)
        {
            try
            {
                encodeNakMessage(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    address,
                    sessionId,
                    streamId,
                    termId,
                    termOffset,
                    nakLength,
                    channel);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Logs a nak message sent by the receiver for a single control address.
     *
     * @param sessionId    of the Resend.
     * @param streamId     of the Resend.
     * @param termId       of the Resend.
     * @param termOffset   of the Resend.
     * @param resendLength of the Resend.
     * @param channel      of the Resend.
     */
    public void logResend(
        final int sessionId,
        final int streamId,
        final int termId,
        final int termOffset,
        final int resendLength,
        final String channel)
    {
        final int length = (SIZE_OF_INT * 6) + channel.length();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(RESEND), encodedLength);
        if (index > 0)
        {
            try
            {
                encodeResend(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    sessionId,
                    streamId,
                    termId,
                    termOffset,
                    resendLength,
                    channel);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Logs a publication being revoked.
     *
     * @param revokedPos of the PublicationRevoke
     * @param sessionId  of the PublicationRevoke
     * @param streamId   of the PublicationRevoke
     * @param channel    of the PublicationRevoke
     */
    public void logPublicationRevoke(
        final long revokedPos,
        final int sessionId,
        final int streamId,
        final String channel)
    {
        final int length = SIZE_OF_LONG + (SIZE_OF_INT * 3) + channel.length();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(PUBLICATION_REVOKE), encodedLength);
        if (index > 0)
        {
            try
            {
                encodePublicationRevoke(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    revokedPos,
                    sessionId,
                    streamId,
                    channel);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Logs a publication image being revoked.
     *
     * @param revokedPos of the PublicationImageRevoke
     * @param sessionId  of the PublicationImageRevoke
     * @param streamId   of the PublicationImageRevoke
     * @param channel    of the PublicationImageRevoke
     */
    public void logPublicationImageRevoke(
        final long revokedPos,
        final int sessionId,
        final int streamId,
        final String channel)
    {
        final int length = SIZE_OF_LONG + (SIZE_OF_INT * 3) + channel.length();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(PUBLICATION_IMAGE_REVOKE), encodedLength);
        if (index > 0)
        {
            try
            {
                encodePublicationImageRevoke(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    revokedPos,
                    sessionId,
                    streamId,
                    channel);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    static int toEventCodeId(final DriverEventCode code)
    {
        return EVENT_CODE_TYPE << 16 | (code.id() & 0xFFFF);
    }
}
