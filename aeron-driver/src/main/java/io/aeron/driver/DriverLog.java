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
package io.aeron.driver;

import io.aeron.agent.DriverEventCode;
import io.aeron.agent.DriverEventLogger;
import io.aeron.driver.media.ImageConnection;
import org.agrona.DirectBuffer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.agent.DriverEventCode.*;
import static io.aeron.agent.DriverModuleLogger.isEnabled;
import static io.aeron.command.ControlProtocolEvents.*;

/**
 * Direct-call entry points for logging {@link io.aeron.driver.MediaDriver} events, replacing the previous
 * ByteBuddy-based instrumentation.
 */
public final class DriverLog
{
    private static final boolean LOG_FRAME_IN_ENABLED = isEnabled(FRAME_IN);
    private static final boolean LOG_FRAME_OUT_ENABLED = isEnabled(FRAME_OUT);
    private static final boolean LOG_REMOVE_PUBLICATION_CLEANUP_ENABLED = isEnabled(REMOVE_PUBLICATION_CLEANUP);
    private static final boolean LOG_REMOVE_SUBSCRIPTION_CLEANUP_ENABLED = isEnabled(REMOVE_SUBSCRIPTION_CLEANUP);
    private static final boolean LOG_REMOVE_IMAGE_CLEANUP_ENABLED = isEnabled(REMOVE_IMAGE_CLEANUP);
    private static final boolean LOG_SEND_CHANNEL_CREATION_ENABLED = isEnabled(SEND_CHANNEL_CREATION);
    private static final boolean LOG_SEND_CHANNEL_CLOSE_ENABLED = isEnabled(SEND_CHANNEL_CLOSE);
    private static final boolean LOG_RECEIVE_CHANNEL_CREATION_ENABLED = isEnabled(RECEIVE_CHANNEL_CREATION);
    private static final boolean LOG_RECEIVE_CHANNEL_CLOSE_ENABLED = isEnabled(RECEIVE_CHANNEL_CLOSE);
    private static final boolean LOG_UNTETHERED_SUBSCRIPTION_STATE_CHANGE_ENABLED =
        isEnabled(UNTETHERED_SUBSCRIPTION_STATE_CHANGE);
    private static final boolean LOG_NAME_RESOLUTION_NEIGHBOR_ADDED_ENABLED =
        isEnabled(NAME_RESOLUTION_NEIGHBOR_ADDED);
    private static final boolean LOG_NAME_RESOLUTION_NEIGHBOR_REMOVED_ENABLED =
        isEnabled(NAME_RESOLUTION_NEIGHBOR_REMOVED);
    private static final boolean LOG_NAME_RESOLUTION_RESOLVE_ENABLED = isEnabled(NAME_RESOLUTION_RESOLVE);
    private static final boolean LOG_NAME_RESOLUTION_LOOKUP_ENABLED = isEnabled(NAME_RESOLUTION_LOOKUP);
    private static final boolean LOG_NAME_RESOLUTION_HOST_NAME_ENABLED = isEnabled(NAME_RESOLUTION_HOST_NAME);
    private static final boolean LOG_FLOW_CONTROL_RECEIVER_ADDED_ENABLED = isEnabled(FLOW_CONTROL_RECEIVER_ADDED);
    private static final boolean LOG_FLOW_CONTROL_RECEIVER_REMOVED_ENABLED =
        isEnabled(FLOW_CONTROL_RECEIVER_REMOVED);
    private static final boolean LOG_NAK_SENT_ENABLED = isEnabled(NAK_SENT);
    private static final boolean LOG_NAK_RECEIVED_ENABLED = isEnabled(NAK_RECEIVED);
    private static final boolean LOG_RESEND_ENABLED = isEnabled(RESEND);
    private static final boolean LOG_PUBLICATION_REVOKE_ENABLED = isEnabled(PUBLICATION_REVOKE);
    private static final boolean LOG_PUBLICATION_IMAGE_REVOKE_ENABLED = isEnabled(PUBLICATION_IMAGE_REVOKE);
    private static final boolean LOG_TEXT_DATA_ENABLED = isEnabled(TEXT_DATA);

    private DriverLog()
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
    public static void logFrameIn(
        final DirectBuffer buffer, final int offset, final int frameLength, final InetSocketAddress dstAddress)
    {
        if (!LOG_FRAME_IN_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logFrameIn(buffer, offset, frameLength, dstAddress);
    }

    /**
     * Log a frame being sent out from the driver to the media.
     *
     * @param buffer     containing the frame.
     * @param dstAddress for the frame.
     */
    public static void logFrameOut(final ByteBuffer buffer, final InetSocketAddress dstAddress)
    {
        if (!LOG_FRAME_OUT_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logFrameOut(buffer, dstAddress);
    }

    /**
     * Log the removal of a publication.
     *
     * @param channel   for the channel.
     * @param sessionId for the publication.
     * @param streamId  within the channel.
     */
    public static void logPublicationRemoval(final String channel, final int sessionId, final int streamId)
    {
        if (!LOG_REMOVE_PUBLICATION_CLEANUP_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logPublicationRemoval(channel, sessionId, streamId);
    }

    /**
     * Log the removal of a subscription.
     *
     * @param channel        for the channel.
     * @param streamId       within the channel.
     * @param subscriptionId for the subscription.
     */
    public static void logSubscriptionRemoval(final String channel, final int streamId, final long subscriptionId)
    {
        if (!LOG_REMOVE_SUBSCRIPTION_CLEANUP_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logSubscriptionRemoval(channel, streamId, subscriptionId);
    }

    /**
     * Log the removal of an image from the driver.
     *
     * @param channel       for the channel.
     * @param sessionId     for the image.
     * @param streamId      for the image.
     * @param correlationId for the image.
     */
    public static void logImageRemoval(
        final String channel, final int sessionId, final int streamId, final long correlationId)
    {
        if (!LOG_REMOVE_IMAGE_CLEANUP_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logImageRemoval(channel, sessionId, streamId, correlationId);
    }

    /**
     * Log the creation of a send channel endpoint.
     *
     * @param description of the channel.
     */
    public static void logSendChannelCreation(final String description)
    {
        if (!LOG_SEND_CHANNEL_CREATION_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logString(SEND_CHANNEL_CREATION, description);
    }

    /**
     * Log the closing of a send channel endpoint.
     *
     * @param description of the channel.
     */
    public static void logSendChannelClose(final String description)
    {
        if (!LOG_SEND_CHANNEL_CLOSE_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logString(SEND_CHANNEL_CLOSE, description);
    }

    /**
     * Log the creation of a receive channel endpoint.
     *
     * @param description of the channel.
     */
    public static void logReceiveChannelCreation(final String description)
    {
        if (!LOG_RECEIVE_CHANNEL_CREATION_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logString(RECEIVE_CHANNEL_CREATION, description);
    }

    /**
     * Log the closing of a receive channel endpoint.
     *
     * @param description of the channel.
     */
    public static void logReceiveChannelClose(final String description)
    {
        if (!LOG_RECEIVE_CHANNEL_CLOSE_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logString(RECEIVE_CHANNEL_CLOSE, description);
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
    public static <E extends Enum<E>> void logUntetheredSubscriptionStateChange(
        final E oldState, final E newState, final long subscriptionId, final int streamId, final int sessionId)
    {
        if (!LOG_UNTETHERED_SUBSCRIPTION_STATE_CHANGE_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logUntetheredSubscriptionStateChange(
            oldState, newState, subscriptionId, streamId, sessionId);
    }

    /**
     * Log a neighbor being added for name resolution.
     *
     * @param address of the neighbor.
     */
    public static void logNeighborAdded(final InetSocketAddress address)
    {
        if (!LOG_NAME_RESOLUTION_NEIGHBOR_ADDED_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logAddress(NAME_RESOLUTION_NEIGHBOR_ADDED, address);
    }

    /**
     * Log a neighbor being removed for name resolution.
     *
     * @param address of the neighbor.
     */
    public static void logNeighborRemoved(final InetSocketAddress address)
    {
        if (!LOG_NAME_RESOLUTION_NEIGHBOR_REMOVED_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logAddress(NAME_RESOLUTION_NEIGHBOR_REMOVED, address);
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
    public static void logResolve(
        final String resolverName,
        final long durationNs,
        final String name,
        final boolean isReResolution,
        final InetAddress address)
    {
        if (!LOG_NAME_RESOLUTION_RESOLVE_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logResolve(resolverName, durationNs, name, isReResolution, address);
    }

    /**
     * Log a lookup for a resolver and the associated result.
     *
     * @param resolverName simple class name of the resolver.
     * @param durationNs   of the call in nanoseconds.
     * @param name         host name being resolved.
     * @param isReLookup   {@code true} if this is a re-lookup.
     * @param resolvedName address that was resolved to, can be {@code null}.
     */
    public static void logLookup(
        final String resolverName,
        final long durationNs,
        final String name,
        final boolean isReLookup,
        final String resolvedName)
    {
        if (!LOG_NAME_RESOLUTION_LOOKUP_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logLookup(resolverName, durationNs, name, isReLookup, resolvedName);
    }

    /**
     * Log a host name resolution duration.
     *
     * @param durationNs of the call in nanoseconds.
     * @param hostName   host name being resolved.
     */
    public static void logHostName(final long durationNs, final String hostName)
    {
        if (!LOG_NAME_RESOLUTION_HOST_NAME_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logHostName(durationNs, hostName);
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
    public static void logFlowControlReceiverAdded(
        final long receiverId,
        final int sessionId,
        final int streamId,
        final String channel,
        final int receiverCount)
    {
        if (!LOG_FLOW_CONTROL_RECEIVER_ADDED_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logFlowControlReceiver(
            FLOW_CONTROL_RECEIVER_ADDED, receiverId, sessionId, streamId, channel, receiverCount);
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
    public static void logFlowControlReceiverRemoved(
        final long receiverId,
        final int sessionId,
        final int streamId,
        final String channel,
        final int receiverCount)
    {
        if (!LOG_FLOW_CONTROL_RECEIVER_REMOVED_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logFlowControlReceiver(
            FLOW_CONTROL_RECEIVER_REMOVED, receiverId, sessionId, streamId, channel, receiverCount);
    }

    /**
     * Log a NAK message sent by the receiver for a single control address.
     *
     * @param address    NAK UDP destination.
     * @param sessionId  of the NAK.
     * @param streamId   of the NAK.
     * @param termId     of the NAK.
     * @param termOffset of the NAK.
     * @param nakLength  of the NAK.
     * @param channel    of the NAK.
     */
    private static void logNakSent(
        final InetSocketAddress address,
        final int sessionId,
        final int streamId,
        final int termId,
        final int termOffset,
        final int nakLength,
        final String channel)
    {
        DriverEventLogger.LOGGER.logNakMessage(
            NAK_SENT, address, sessionId, streamId, termId, termOffset, nakLength, channel);
    }

    /**
     * Log all the naks send for the connections for a single image.
     *
     * @param controlAddresses  NAK UDP destinations.
     * @param sessionId         of the NAK.
     * @param streamId          of the NAK.
     * @param termId            of the NAK.
     * @param termOffset        of the NAK.
     * @param nakLength         of the NAK.
     * @param channel           of the NAK.
     */
    public static void logNaksSent(
        final ImageConnection[] controlAddresses,
        final int sessionId,
        final int streamId,
        final int termId,
        final int termOffset,
        final int nakLength,
        final String channel)
    {
        if (!LOG_NAK_SENT_ENABLED)
        {
            return;
        }

        for (final ImageConnection connection : controlAddresses)
        {
            if (null != connection)
            {
                logNakSent(
                    connection.controlAddress, sessionId, streamId, termId, termOffset, nakLength, channel);
            }
        }
    }

    /**
     * Log a NAK message received by the sender.
     *
     * @param address    NAK UDP source.
     * @param sessionId  of the NAK.
     * @param streamId   of the NAK.
     * @param termId     of the NAK.
     * @param termOffset of the NAK.
     * @param nakLength  of the NAK.
     * @param channel    of the NAK.
     */
    public static void logNakReceived(
        final InetSocketAddress address,
        final int sessionId,
        final int streamId,
        final int termId,
        final int termOffset,
        final int nakLength,
        final String channel)
    {
        if (!LOG_NAK_RECEIVED_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logNakMessage(
            NAK_RECEIVED, address, sessionId, streamId, termId, termOffset, nakLength, channel);
    }

    /**
     * Logs a resend of a range of a term buffer.
     *
     * @param sessionId    of the resend.
     * @param streamId     of the resend.
     * @param termId       of the resend.
     * @param termOffset   of the resend.
     * @param resendLength of the resend.
     * @param channel      of the resend.
     */
    public static void logResend(
        final int sessionId,
        final int streamId,
        final int termId,
        final int termOffset,
        final int resendLength,
        final String channel)
    {
        if (!LOG_RESEND_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logResend(sessionId, streamId, termId, termOffset, resendLength, channel);
    }

    /**
     * Logs a publication being revoked.
     *
     * @param revokedPos of the publication revoke.
     * @param sessionId  of the publication revoke.
     * @param streamId   of the publication revoke.
     * @param channel    of the publication revoke.
     */
    public static void logPublicationRevoke(
        final long revokedPos, final int sessionId, final int streamId, final String channel)
    {
        if (!LOG_PUBLICATION_REVOKE_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logPublicationRevoke(revokedPos, sessionId, streamId, channel);
    }

    /**
     * Logs a publication image being revoked.
     *
     * @param revokedPos of the publication image revoke.
     * @param sessionId  of the publication image revoke.
     * @param streamId   of the publication image revoke.
     * @param channel    of the publication image revoke.
     */
    public static void logPublicationImageRevoke(
        final long revokedPos, final int sessionId, final int streamId, final String channel)
    {
        if (!LOG_PUBLICATION_IMAGE_REVOKE_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logPublicationImageRevoke(revokedPos, sessionId, streamId, channel);
    }

    /**
     * Log a client command/response message keyed by its protocol message type id.
     *
     * @param msgTypeId of the message, from {@link io.aeron.command.ControlProtocolEvents}.
     * @param buffer    containing the encoded message.
     * @param index     at which the message begins.
     * @param length    of the encoded message.
     */
    public static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
    {
        final DriverEventCode code = cmdEventCode(msgTypeId);
        if (null != code)
        {
            DriverEventLogger.LOGGER.log(code, buffer, index, length);
        }
    }

    /**
     * Log a simple text input.
     *
     * @param text to be logged.
     */
    public static void logText(final String text)
    {
        if (!LOG_TEXT_DATA_ENABLED)
        {
            return;
        }

        DriverEventLogger.LOGGER.logString(TEXT_DATA, text);
    }

    private static DriverEventCode cmdEventCode(final int msgTypeId)
    {
        switch (msgTypeId)
        {
            case ADD_PUBLICATION:
                return CMD_IN_ADD_PUBLICATION;
            case REMOVE_PUBLICATION:
                return CMD_IN_REMOVE_PUBLICATION;
            case ADD_EXCLUSIVE_PUBLICATION:
                return CMD_IN_ADD_EXCLUSIVE_PUBLICATION;
            case ADD_SUBSCRIPTION:
                return CMD_IN_ADD_SUBSCRIPTION;
            case REMOVE_SUBSCRIPTION:
                return CMD_IN_REMOVE_SUBSCRIPTION;
            case CLIENT_KEEPALIVE:
                return CMD_IN_KEEPALIVE_CLIENT;
            case ADD_DESTINATION:
                return CMD_IN_ADD_DESTINATION;
            case REMOVE_DESTINATION:
                return CMD_IN_REMOVE_DESTINATION;
            case ON_AVAILABLE_IMAGE:
                return CMD_OUT_AVAILABLE_IMAGE;
            case ON_ERROR:
                return CMD_OUT_ERROR;
            case ON_OPERATION_SUCCESS:
                return CMD_OUT_ON_OPERATION_SUCCESS;
            case ON_PUBLICATION_READY:
                return CMD_OUT_PUBLICATION_READY;
            case ON_UNAVAILABLE_IMAGE:
                return CMD_OUT_ON_UNAVAILABLE_IMAGE;
            case ON_EXCLUSIVE_PUBLICATION_READY:
                return CMD_OUT_EXCLUSIVE_PUBLICATION_READY;
            case ON_SUBSCRIPTION_READY:
                return CMD_OUT_SUBSCRIPTION_READY;
            case ON_COUNTER_READY:
                return CMD_OUT_COUNTER_READY;
            case ON_UNAVAILABLE_COUNTER:
                return CMD_OUT_ON_UNAVAILABLE_COUNTER;
            case ADD_COUNTER:
                return CMD_IN_ADD_COUNTER;
            case REMOVE_COUNTER:
                return CMD_IN_REMOVE_COUNTER;
            case CLIENT_CLOSE:
                return CMD_IN_CLIENT_CLOSE;
            case ADD_RCV_DESTINATION:
                return CMD_IN_ADD_RCV_DESTINATION;
            case REMOVE_RCV_DESTINATION:
                return CMD_IN_REMOVE_RCV_DESTINATION;
            case ON_CLIENT_TIMEOUT:
                return CMD_OUT_ON_CLIENT_TIMEOUT;
            case TERMINATE_DRIVER:
                return CMD_IN_TERMINATE_DRIVER;
            case REMOVE_DESTINATION_BY_ID:
                return CMD_IN_REMOVE_DESTINATION_BY_ID;
            case REJECT_IMAGE:
                return CMD_IN_REJECT_IMAGE;
            default:
                return null;
        }
    }
}
