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
package io.aeron.driver.logging;

import io.aeron.eventlog.GeneratedLogger;
import io.aeron.eventlog.LoggerMethod;
import io.aeron.logging.EventConfiguration;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.driver.logging.DriverEventCode.EVENT_CODE_TYPE;

/**
 * Event logger interface used by interceptors for recording into a {@link RingBuffer} for a
 * {@link io.aeron.driver.MediaDriver} via a Java Agent. The implementation is generated at compile time from the
 * {@link LoggerMethod}-annotated methods below.
 */
@GeneratedLogger(
    encoder = "io.aeron.driver.logging.DriverEventEncoder",
    eventCodeType = "io.aeron.driver.logging.DriverEventCode")
public interface DriverEventLogger
{
    /**
     * Logger for writing into the {@link ManyToOneRingBuffer} held by {@link EventConfiguration#eventReader}.
     */
    DriverEventLogger LOGGER = new DriverEventLoggerImpl(EventConfiguration.eventReader().ringBuffer());

    /**
     * Maximum length of a host name.
     */
    int MAX_HOST_NAME_LENGTH = 256;

    /**
     * Maximum length of a Channel URI.
     */
    int MAX_CHANNEL_URI_LENGTH = 4096;

    /**
     * Log an event for the driver.
     *
     * @param code          for the type of event.
     * @param buffer        containing the encoded event.
     * @param offset        in the buffer at which the event begins.
     * @param messageLength of the encoded event.
     */
    @LoggerMethod(lengthMethod = "identityLength", lengthArgs = { "messageLength" }, encodeMethod = "encode",
        encodeArgs = { "buffer", "offset" })
    default void log(
        final DriverEventCode code, final DirectBuffer buffer, final int offset, final int messageLength)
    {
    }

    /**
     * Log a frame coming in from the media.
     *
     * @param buffer      containing the frame.
     * @param offset      in the buffer at which the frame begins.
     * @param frameLength of the frame.
     * @param dstAddress  for the frame.
     */
    @LoggerMethod(eventCode = "FRAME_IN", lengthMethod = "frameInLength", lengthArgs = { "frameLength", "dstAddress" },
        encodeMethod = "encode", encodeArgs = { "buffer", "offset", "dstAddress" })
    default void logFrameIn(
        final DirectBuffer buffer, final int offset, final int frameLength, final InetSocketAddress dstAddress)
    {
    }

    /**
     * Log a frame being sent out from the driver to the media.
     *
     * @param buffer     containing the frame.
     * @param dstAddress for the frame.
     */
    @LoggerMethod(eventCode = "FRAME_OUT", lengthMethod = "frameOutLength", lengthArgs = { "buffer", "dstAddress" })
    default void logFrameOut(final ByteBuffer buffer, final InetSocketAddress dstAddress)
    {
    }

    /**
     * Log the removal of a publication.
     *
     * @param channel   for the channel.
     * @param sessionId for the publication.
     * @param streamId  within the channel.
     */
    @LoggerMethod(eventCode = "REMOVE_PUBLICATION_CLEANUP", lengthMethod = "publicationRemovalLength",
        lengthArgs = { "channel" })
    default void logPublicationRemoval(final String channel, final int sessionId, final int streamId)
    {
    }

    /**
     * Log the removal of a subscription.
     *
     * @param channel        for the channel.
     * @param streamId       within the channel.
     * @param subscriptionId for the subscription.
     */
    @LoggerMethod(eventCode = "REMOVE_SUBSCRIPTION_CLEANUP", lengthMethod = "subscriptionRemovalLength",
        lengthArgs = { "channel" })
    default void logSubscriptionRemoval(final String channel, final int streamId, final long subscriptionId)
    {
    }

    /**
     * Log the removal of an image from the driver.
     *
     * @param channel       for the channel.
     * @param sessionId     for the image.
     * @param streamId      for the image.
     * @param correlationId for the image.
     */
    @LoggerMethod(eventCode = "REMOVE_IMAGE_CLEANUP", lengthMethod = "imageRemovalLength", lengthArgs = { "channel" })
    default void logImageRemoval(

        final String channel,
        final int sessionId,
        final int streamId,
        final long correlationId)
    {
    }

    /**
     * Log a generic string associated with an event.
     *
     * @param code  for the event type.
     * @param value of the string to be logged.
     */
    @LoggerMethod(lengthMethod = "stringValueLength", lengthArgs = { "value" }, encodeMethod = "encode",
        encodeArgs = { "value" })
    default void logString(final DriverEventCode code, final String value)
    {
    }

    /**
     * Log an untethered subscription state change.
     *
     * @param <E>            type of the event.
     * @param oldState       before the change.
     * @param newState       after the change.
     * @param subscriptionId to which the change applies.
     * @param streamId       of the image.
     * @param sessionId      of the image.
     */
    @LoggerMethod(eventCode = "UNTETHERED_SUBSCRIPTION_STATE_CHANGE",
        lengthMethod = "untetheredSubscriptionStateChangeLength", lengthArgs = { "oldState", "newState" })
    default <E extends Enum<E>> void logUntetheredSubscriptionStateChange(
        final E oldState, final E newState, final long subscriptionId, final int streamId, final int sessionId)
    {
    }

    /**
     * Log an address with associated event.
     *
     * @param code    representing the event type.
     * @param address to be logged.
     */
    @LoggerMethod(lengthMethod = "addressLength", lengthArgs = { "address" }, encodeMethod = "encode",
        encodeArgs = { "address" })
    default void logAddress(final DriverEventCode code, final InetSocketAddress address)
    {
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
    @LoggerMethod(eventCode = "NAME_RESOLUTION_RESOLVE", lengthMethod = "resolveLength",
        lengthArgs = { "resolverName", "name", "address" }, skipCaptureLength = true)
    default void logResolve(
        final String resolverName,
        final long durationNs,
        final String name,
        final boolean isReResolution,
        final InetAddress address)
    {
    }

    /**
     * Log a resolution for a resolver and the associated result.
     *
     * @param resolverName       simple class name of the resolver
     * @param durationNs         of the call in nanoseconds.
     * @param name               host name being resolved.
     * @param isReLookup         address that was resolved to, can be null.
     * @param resolvedNameOrNull address that was resolved to, can be null.
     */
    @LoggerMethod(eventCode = "NAME_RESOLUTION_LOOKUP", lengthMethod = "lookupLength",
        lengthArgs = { "resolverName", "name", "resolvedNameOrNull" }, skipCaptureLength = true)
    default void logLookup(
        final String resolverName,
        final long durationNs,
        final String name,
        final boolean isReLookup,
        final String resolvedNameOrNull)
    {
    }

    /**
     * Log a host name resolution duration.
     *
     * @param durationNs of the call in nanoseconds.
     * @param hostName   host name being resolved.
     */
    @LoggerMethod(eventCode = "NAME_RESOLUTION_HOST_NAME", lengthMethod = "hostNameLength",
        lengthArgs = { "hostName" }, skipCaptureLength = true)
    default void logHostName(final long durationNs, final String hostName)
    {
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
    @LoggerMethod(lengthMethod = "flowControlReceiverLength", lengthArgs = { "channel" },
        encodeArgs = { "receiverId", "sessionId", "streamId", "channel", "receiverCount" })
    default void logFlowControlReceiver(
        final DriverEventCode code,
        final long receiverId,
        final int sessionId,
        final int streamId,
        final String channel,
        final int receiverCount)
    {
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
    @LoggerMethod(lengthMethod = "nakMessageLength", lengthArgs = { "address", "channel" },
        encodeArgs = { "address", "sessionId", "streamId", "termId", "termOffset", "nakLength", "channel" })
    default void logNakMessage(
        final DriverEventCode eventCode,
        final InetSocketAddress address,
        final int sessionId,
        final int streamId,
        final int termId,
        final int termOffset,
        final int nakLength,
        final String channel)
    {
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
    @LoggerMethod(eventCode = "RESEND", lengthMethod = "resendMessageLength", lengthArgs = { "channel" })
    default void logResend(
        final int sessionId,
        final int streamId,
        final int termId,
        final int termOffset,
        final int resendLength,
        final String channel)
    {
    }

    /**
     * Logs a publication being revoked.
     *
     * @param revokedPos of the PublicationRevoke
     * @param sessionId  of the PublicationRevoke
     * @param streamId   of the PublicationRevoke
     * @param channel    of the PublicationRevoke
     */
    @LoggerMethod(eventCode = "PUBLICATION_REVOKE", lengthMethod = "publicationRevokeLength",
        lengthArgs = { "channel" })
    default void logPublicationRevoke(
        final long revokedPos,
        final int sessionId,
        final int streamId,
        final String channel)
    {
    }

    /**
     * Logs a publication image being revoked.
     *
     * @param revokedPos of the PublicationImageRevoke
     * @param sessionId  of the PublicationImageRevoke
     * @param streamId   of the PublicationImageRevoke
     * @param channel    of the PublicationImageRevoke
     */
    @LoggerMethod(eventCode = "PUBLICATION_IMAGE_REVOKE", lengthMethod = "publicationImageRevokeLength",
        lengthArgs = { "channel" })
    default void logPublicationImageRevoke(
        final long revokedPos,
        final int sessionId,
        final int streamId,
        final String channel)
    {
    }

    /**
     * Compute the full event code id for a {@link DriverEventCode}.
     *
     * @param code to convert.
     * @return the full event code id.
     */
    static int toEventCodeId(final DriverEventCode code)
    {
        return EVENT_CODE_TYPE << 16 | (code.id() & 0xFFFF);
    }
}
