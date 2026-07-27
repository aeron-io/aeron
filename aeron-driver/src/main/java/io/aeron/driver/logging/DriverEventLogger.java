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

import io.aeron.eventlog.AllowTruncate;
import io.aeron.eventlog.GeneratedLogger;
import io.aeron.eventlog.LoggerMethod;
import io.aeron.eventlog.Tag;
import io.aeron.logging.EventConfiguration;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.driver.logging.DriverEventCode.EVENT_CODE_TYPE;
import static io.aeron.logging.CborUtils.AERON_DRIVER_ADMIN_TAG;
import static io.aeron.logging.CborUtils.AERON_PROTOCOL_TAG;

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
    DriverEventLogger LOGGER = new DriverEventLoggerCborImpl(EventConfiguration.eventReader().ringBuffer());

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
    @LoggerMethod(bufferView = { "buffer", "offset", "messageLength" })
    default void log(
        final DriverEventCode code,
        @Tag(AERON_DRIVER_ADMIN_TAG) final DirectBuffer buffer,
        final int offset,
        final int messageLength)
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
    @LoggerMethod(eventCode = "FRAME_IN", bufferView = { "buffer", "offset", "frameLength" })
    default void logFrameIn(
        @Tag(AERON_PROTOCOL_TAG) @AllowTruncate final DirectBuffer buffer,
        final int offset,
        final int frameLength,
        final InetSocketAddress dstAddress)
    {
    }

    /**
     * Log a frame being sent out from the driver to the media.
     *
     * @param buffer     containing the frame.
     * @param dstAddress for the frame.
     */
    @LoggerMethod(eventCode = "FRAME_OUT")
    default void logFrameOut(
        @Tag(AERON_PROTOCOL_TAG) @AllowTruncate final ByteBuffer buffer,
        final InetSocketAddress dstAddress)
    {
    }

    /**
     * Log the removal of a publication.
     *
     * @param channel   for the channel.
     * @param sessionId for the publication.
     * @param streamId  within the channel.
     */
    @LoggerMethod(eventCode = "REMOVE_PUBLICATION_CLEANUP")
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
    @LoggerMethod(eventCode = "REMOVE_SUBSCRIPTION_CLEANUP")
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
    @LoggerMethod(eventCode = "REMOVE_IMAGE_CLEANUP")
    default void logImageRemoval(

        final String channel,
        final int sessionId,
        final int streamId,
        final long correlationId)
    {
    }

    /**
     * Log the creation of a send channel endpoint.
     *
     * @param value description of the channel.
     */
    @LoggerMethod(eventCode = "SEND_CHANNEL_CREATION")
    default void logSendChannelCreation(final String value)
    {
    }

    /**
     * Log the closing of a send channel endpoint.
     *
     * @param value description of the channel.
     */
    @LoggerMethod(eventCode = "SEND_CHANNEL_CLOSE")
    default void logSendChannelClose(final String value)
    {
    }

    /**
     * Log the creation of a receive channel endpoint.
     *
     * @param value description of the channel.
     */
    @LoggerMethod(eventCode = "RECEIVE_CHANNEL_CREATION")
    default void logReceiveChannelCreation(final String value)
    {
    }

    /**
     * Log the closing of a receive channel endpoint.
     *
     * @param value description of the channel.
     */
    @LoggerMethod(eventCode = "RECEIVE_CHANNEL_CLOSE")
    default void logReceiveChannelClose(final String value)
    {
    }

    /**
     * Log a simple text input.
     *
     * @param code  for the type of event.
     * @param value of the string to be logged.
     */
    @LoggerMethod
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
    @LoggerMethod(eventCode = "UNTETHERED_SUBSCRIPTION_STATE_CHANGE")
    default <E extends Enum<E>> void logUntetheredSubscriptionStateChange(
        final E oldState, final E newState, final long subscriptionId, final int streamId, final int sessionId)
    {
    }

    /**
     * Log a neighbor being added for name resolution.
     *
     * @param address of the neighbor.
     */
    @LoggerMethod(eventCode = "NAME_RESOLUTION_NEIGHBOR_ADDED")
    default void logNeighborAdded(final InetSocketAddress address)
    {
    }

    /**
     * Log a neighbor being removed for name resolution.
     *
     * @param address of the neighbor.
     */
    @LoggerMethod(eventCode = "NAME_RESOLUTION_NEIGHBOR_REMOVED")
    default void logNeighborRemoved(final InetSocketAddress address)
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
    @LoggerMethod(eventCode = "NAME_RESOLUTION_RESOLVE")
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
    @LoggerMethod(eventCode = "NAME_RESOLUTION_LOOKUP")
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
    @LoggerMethod(eventCode = "NAME_RESOLUTION_HOST_NAME")
    default void logHostName(final long durationNs, final String hostName)
    {
    }

    /**
     * Log a receiver being added to a flow control strategy.
     *
     * @param receiverId    of the receiver.
     * @param sessionId     of the image.
     * @param streamId      of the image.
     * @param channel       uri of the channel.
     * @param receiverCount number of the receivers after the event.
     */
    @LoggerMethod(eventCode = "FLOW_CONTROL_RECEIVER_ADDED")
    default void logFlowControlReceiverAdded(
        final long receiverId,
        final int sessionId,
        final int streamId,
        final String channel,
        final int receiverCount)
    {
    }

    /**
     * Log a receiver being removed from a flow control strategy.
     *
     * @param receiverId    of the receiver.
     * @param sessionId     of the image.
     * @param streamId      of the image.
     * @param channel       uri of the channel.
     * @param receiverCount number of the receivers after the event.
     */
    @LoggerMethod(eventCode = "FLOW_CONTROL_RECEIVER_REMOVED")
    default void logFlowControlReceiverRemoved(
        final long receiverId,
        final int sessionId,
        final int streamId,
        final String channel,
        final int receiverCount)
    {
    }

    /**
     * Logs a NAK message sent by the receiver for a single control address.
     *
     * @param address    Nak UDP destination.
     * @param sessionId  of the Nak.
     * @param streamId   of the Nak.
     * @param termId     of the Nak.
     * @param termOffset of the Nak.
     * @param nakLength  of the Nak.
     * @param channel    of the Nak.
     */
    @LoggerMethod(eventCode = "NAK_SENT")
    default void logNakSent(
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
     * Logs a NAK message received by the sender.
     *
     * @param address    Nak UDP source.
     * @param sessionId  of the Nak.
     * @param streamId   of the Nak.
     * @param termId     of the Nak.
     * @param termOffset of the Nak.
     * @param nakLength  of the Nak.
     * @param channel    of the Nak.
     */
    @LoggerMethod(eventCode = "NAK_RECEIVED")
    default void logNakReceived(
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
    @LoggerMethod(eventCode = "RESEND")
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
    @LoggerMethod(eventCode = "PUBLICATION_REVOKE")
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
    @LoggerMethod(eventCode = "PUBLICATION_IMAGE_REVOKE")
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
