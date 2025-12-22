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

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.driver.DefaultNameResolver;
import io.aeron.driver.NameResolver;
import io.aeron.driver.exceptions.InvalidChannelException;
import org.agrona.BitUtil;
import org.agrona.SystemUtil;

import java.net.*;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static io.aeron.CommonContext.*;
import static io.aeron.driver.media.NetworkUtil.*;
import static java.net.InetAddress.getByAddress;

/**
 * The media configuration for Aeron UDP channels as an instantiation of the socket addresses for a {@link ChannelUri}.
 *
 * @see ChannelUri
 * @see io.aeron.ChannelUriStringBuilder
 */
public final class UdpChannel
{
    /**
     * The offset from the beginning of a payload where the reserved value begins.
     */
    public static final int RESERVED_VALUE_MESSAGE_OFFSET = -8;

    private static final AtomicInteger UNIQUE_CANONICAL_FORM_VALUE = new AtomicInteger();
    private static final InetSocketAddress ANY_IPV4 = new InetSocketAddress("0.0.0.0", 0);
    private static final InetSocketAddress ANY_IPV6 = new InetSocketAddress("::", 0);

    private final ControlMode controlMode;
    private final boolean hasExplicitControl;
    private final boolean hasExplicitEndpoint;
    private final boolean isMulticast;
    private final boolean hasMulticastTtl;
    private final boolean hasTag;
    private final int multicastTtl;
    private final int socketRcvbufLength;
    private final int socketSndbufLength;
    private final int receiverWindowLength;
    private final long tag;
    private final InetSocketAddress remoteData;
    private final InetSocketAddress localData;
    private final InetSocketAddress remoteControl;
    private final InetSocketAddress localControl;
    private final String uriStr;
    private final String canonicalForm;
    private final NetworkInterface localInterface;
    private final ProtocolFamily protocolFamily;
    private final ChannelUri channelUri;
    private final int channelReceiveTimestampOffset;
    private final int channelSendTimestampOffset;
    private final Long groupTag;
    private final Long nakDelayNs;

    private UdpChannel(final Context context)
    {
        controlMode = context.controlMode;
        hasExplicitEndpoint = context.hasExplicitEndpoint;
        hasExplicitControl = context.hasExplicitControl;
        isMulticast = context.isMulticast;
        hasTag = context.hasTagId;
        tag = context.tagId;
        hasMulticastTtl = context.hasMulticastTtl;
        multicastTtl = context.multicastTtl;
        remoteData = context.remoteData;
        localData = context.localData;
        remoteControl = context.remoteControl;
        localControl = context.localControl;
        uriStr = context.uriStr;
        canonicalForm = context.canonicalForm;
        localInterface = context.localInterface;
        protocolFamily = context.protocolFamily;
        channelUri = context.channelUri;
        socketRcvbufLength = context.socketRcvbufLength;
        socketSndbufLength = context.socketSndbufLength;
        receiverWindowLength = context.receiverWindowLength;
        channelReceiveTimestampOffset = context.channelReceiveTimestampOffset;
        channelSendTimestampOffset = context.channelSendTimestampOffset;
        groupTag = context.groupTag;
        nakDelayNs = context.nakDelayNs;
    }

    /**
     * Parse channel URI and create a {@link UdpChannel} using the default name resolver.
     *
     * @param channelUriString to parse.
     * @return a new {@link UdpChannel} as the result of parsing.
     * @throws InvalidChannelException if an error occurs.
     */
    public static UdpChannel parse(final String channelUriString)
    {
        return parse(channelUriString, DefaultNameResolver.INSTANCE, false);
    }

    /**
     * Parse channel URI and create a {@link UdpChannel}.
     *
     * @param channelUriString to parse.
     * @param nameResolver     to use for resolving names.
     * @return a new {@link UdpChannel} as the result of parsing.
     * @throws InvalidChannelException if an error occurs.
     */
    public static UdpChannel parse(final String channelUriString, final NameResolver nameResolver)
    {
        return parse(channelUriString, nameResolver, false);
    }

    /**
     * Parse channel URI and create a {@link UdpChannel}.
     *
     * @param channelUriString to parse.
     * @param nameResolver     to use for resolving names.
     * @param isDestination    to identify if it is a destination within a channel.
     * @return a new {@link UdpChannel} as the result of parsing.
     * @throws InvalidChannelException if an error occurs.
     */
    @SuppressWarnings("MethodLength")
    public static UdpChannel parse(
        final String channelUriString, final NameResolver nameResolver, final boolean isDestination)
    {
        try
        {
            final ChannelUri channelUri = ChannelUri.parse(channelUriString);
            validateConfiguration(channelUri);

            InetSocketAddress endpointAddress = getEndpointAddress(channelUri, nameResolver);
            final InetSocketAddress controlAddress = getExplicitControlAddress(channelUri, nameResolver);

            final String tagIdStr = channelUri.channelTag();
            final ControlMode controlMode = parseControlMode(channelUri);

            final int socketRcvbufLength = parseBufferLength(channelUri, SOCKET_RCVBUF_PARAM_NAME);
            final int socketSndbufLength = parseBufferLength(channelUri, SOCKET_SNDBUF_PARAM_NAME);
            final int receiverWindowLength = parseBufferLength(
                channelUri, RECEIVER_WINDOW_LENGTH_PARAM_NAME);

            final boolean requiresAdditionalSuffix = !isDestination &&
                (null == endpointAddress && null == controlAddress ||
                (null != endpointAddress && endpointAddress.getPort() == 0) ||
                (null != controlAddress && controlAddress.getPort() == 0));

            final boolean hasNoDistinguishingCharacteristic =
                null == endpointAddress && null == controlAddress && null == tagIdStr;

            if (ControlMode.DYNAMIC == controlMode && null == controlAddress)
            {
                throw new IllegalArgumentException(
                    "explicit control expected with dynamic control mode: " + channelUriString);
            }

            if (hasNoDistinguishingCharacteristic && ControlMode.MANUAL != controlMode &&
                ControlMode.RESPONSE != controlMode)
            {
                throw new IllegalArgumentException(
                    "URIs for UDP must specify an endpoint, control, tags, or control-mode=manual/response: " +
                    channelUriString);
            }

            if (null != endpointAddress && endpointAddress.isUnresolved())
            {
                throw new UnknownHostException("could not resolve endpoint address: " + endpointAddress);
            }

            if (null != controlAddress && controlAddress.isUnresolved())
            {
                throw new UnknownHostException("could not resolve control address: " + controlAddress);
            }

            boolean hasExplicitEndpoint = true;
            if (null == endpointAddress)
            {
                hasExplicitEndpoint = false;
                endpointAddress = null != controlAddress && controlAddress.getAddress() instanceof Inet6Address ?
                    ANY_IPV6 : ANY_IPV4;
            }

            final ProtocolFamily protocolFamily = getProtocolFamily(endpointAddress.getAddress());

            final Context context = new Context()
                .protocolFamily(protocolFamily)
                .isMulticast(false)
                .hasExplicitControl(false)
                .hasMulticastTtl(false)
                .hasTagId(false)
                .uriStr(channelUriString)
                .channelUri(channelUri)
                .controlMode(controlMode)
                .hasExplicitEndpoint(hasExplicitEndpoint)
                .hasNoDistinguishingCharacteristic(hasNoDistinguishingCharacteristic)
                .socketRcvbufLength(socketRcvbufLength)
                .socketSndbufLength(socketSndbufLength)
                .receiverWindowLength(receiverWindowLength)
                .nakDelayNs(parseOptionalDurationNs(channelUri, NAK_DELAY_PARAM_NAME));

            if (null != tagIdStr)
            {
                context.hasTagId(true).tagId(Long.parseLong(tagIdStr));
            }

            if (endpointAddress.getAddress().isMulticastAddress())
            {
                final UnresolvedInterface unresolvedInterface = getInterface(channelUri);
                final ResolvedInterface resolvedInterface = unresolvedInterface.resolve(true, protocolFamily);
                final NetworkInterface localInterface = resolvedInterface.localInterface();
                final InetSocketAddress resolvedAddress = resolvedInterface.address();

                context
                    .isMulticast(true)
                    .remoteControlAddress(getMulticastControlAddress(endpointAddress))
                    .remoteDataAddress(endpointAddress)
                    .localControlAddress(resolvedAddress)
                    .localDataAddress(resolvedAddress)
                    .localInterface(localInterface)
                    .canonicalForm(canonicalise(null, resolvedAddress, null, endpointAddress));

                final String ttlValue = channelUri.get(TTL_PARAM_NAME);
                if (null != ttlValue)
                {
                    context.hasMulticastTtl(true).multicastTtl(Integer.parseInt(ttlValue));
                }
            }
            else if (null != controlAddress)
            {
                final String controlVal = channelUri.get(MDC_CONTROL_PARAM_NAME);
                final String endpointVal = channelUri.get(ENDPOINT_PARAM_NAME);

                String suffix = "";
                if (requiresAdditionalSuffix)
                {
                    suffix = null != tagIdStr ? "#" + tagIdStr : "-" + UNIQUE_CANONICAL_FORM_VALUE.getAndAdd(1);
                }

                final String canonicalForm = canonicalise(
                    controlVal, controlAddress, endpointVal, endpointAddress) + suffix;

                context
                    .hasExplicitControl(true)
                    .remoteControlAddress(endpointAddress)
                    .remoteDataAddress(endpointAddress)
                    .localControlAddress(controlAddress)
                    .localDataAddress(controlAddress)
                    .canonicalForm(canonicalForm);
            }
            else
            {
                final UnresolvedInterface unresolvedInterface = getInterface(channelUri);
                final ResolvedInterface resolvedInterface = unresolvedInterface.resolve(false, protocolFamily);
                final InetSocketAddress localAddress = resolvedInterface.address();

                final String endpointVal = channelUri.get(ENDPOINT_PARAM_NAME);
                String suffix = "";
                if (requiresAdditionalSuffix)
                {
                    suffix = (null != tagIdStr) ? "#" + tagIdStr : ("-" + UNIQUE_CANONICAL_FORM_VALUE.getAndAdd(1));
                }

                context
                    .remoteControlAddress(endpointAddress)
                    .remoteDataAddress(endpointAddress)
                    .localControlAddress(localAddress)
                    .localDataAddress(localAddress)
                    .canonicalForm(canonicalise(null, localAddress, endpointVal, endpointAddress) + suffix);
            }

            context.channelReceiveTimestampOffset(
                parseTimestampOffset(channelUri, CHANNEL_RECEIVE_TIMESTAMP_OFFSET_PARAM_NAME));
            context.channelSendTimestampOffset(
                parseTimestampOffset(channelUri, CHANNEL_SEND_TIMESTAMP_OFFSET_PARAM_NAME));

            final Long groupTag = parseOptionalLong(channelUri, GROUP_TAG_PARAM_NAME);
            context.groupTag(groupTag);

            return new UdpChannel(context);
        }
        catch (final Exception ex)
        {
            throw new InvalidChannelException(ex);
        }
    }

    private static int parseTimestampOffset(final ChannelUri channelUri, final String timestampOffsetParamName)
    {
        final String offsetStr = channelUri.get(timestampOffsetParamName);
        if (null == offsetStr)
        {
            return Aeron.NULL_VALUE;
        }
        else if (RESERVED_OFFSET.equals(offsetStr))
        {
            return RESERVED_VALUE_MESSAGE_OFFSET;
        }
        else
        {
            try
            {
                return Integer.parseInt(offsetStr);
            }
            catch (final NumberFormatException ex)
            {
                throw new IllegalArgumentException(
                    "timestamp offset must be a valid integer or the 'reserved' keyword", ex);
            }
        }
    }

    private static Long parseOptionalLong(final ChannelUri channelUri, final String paramName)
    {
        final String longAsString = channelUri.get(paramName);
        if (null == longAsString)
        {
            return null;
        }

        try
        {
            return Long.valueOf(longAsString);
        }
        catch (final NumberFormatException ex)
        {
            throw new IllegalArgumentException("'" + paramName + "' does not contain a valid long value", ex);
        }
    }

    /**
     * Parse a buffer length for a given URI paramName with a format specified by
     * {@link SystemUtil#parseSize(String, String)}, clamping the range to 0 &lt;= x &lt;= Integer.MAX_VALUE.
     *
     * @param channelUri to get the value from.
     * @param paramName  key for the parameter.
     * @return value as an integer.
     * @see SystemUtil#parseSize(String, String)
     */
    public static int parseBufferLength(final ChannelUri channelUri, final String paramName)
    {
        int socketBufferLength = 0;

        final String paramValue = channelUri.get(paramName);
        if (null != paramValue)
        {
            final long size = SystemUtil.parseSize(paramName, paramValue);
            if (size < 0 || size > Integer.MAX_VALUE)
            {
                throw new IllegalArgumentException("Invalid " + paramName + " length: " + size);
            }
            socketBufferLength = (int)size;
        }

        return socketBufferLength;
    }

    /**
     * Parse the control mode from the channel URI. If the value is null or unknown then {@link ControlMode#NONE} will
     * be used.
     *
     * @param channelUri to parse the control mode from.
     * @return an enum value representing the control mode.
     */
    public static ControlMode parseControlMode(final ChannelUri channelUri)
    {
        final String paramValue = channelUri.get(MDC_CONTROL_MODE_PARAM_NAME);
        if (null == paramValue)
        {
            return ControlMode.NONE;
        }

        switch (paramValue)
        {
            case MDC_CONTROL_MODE_DYNAMIC:
                return ControlMode.DYNAMIC;
            case MDC_CONTROL_MODE_MANUAL:
                return ControlMode.MANUAL;
            case CONTROL_MODE_RESPONSE:
                return ControlMode.RESPONSE;
            default:
                return ControlMode.NONE;
        }
    }

    /**
     * Parses out a duration from a channel URI and caters for unit suffix information.
     *
     * @param channelUri    to read the value from
     * @param paramName     specific field to access in the URI
     * @return              duration in nanoseconds, null if not present
     */
    public static Long parseOptionalDurationNs(final ChannelUri channelUri, final String paramName)
    {
        final String valueStr = channelUri.get(paramName);
        if (null == valueStr)
        {
            return null;
        }

        return SystemUtil.parseDuration(paramName, valueStr);
    }

    /**
     * Return a string which is a canonical form of the channel suitable for use as a file or directory
     * name and also as a method of hashing, etc.
     * <p>
     * The general format is:
     * UDP-interface:localPort-remoteAddress:remotePort
     *
     * @param localParamValue  interface or MDC control param value or null for not set.
     * @param localData        address/interface for the channel.
     * @param remoteParamValue endpoint param value or null if not set.
     * @param remoteData       address for the channel.
     * @return canonical representation as a string.
     */
    public static String canonicalise(
        final String localParamValue,
        final InetSocketAddress localData,
        final String remoteParamValue,
        final InetSocketAddress remoteData)
    {
        final StringBuilder builder = new StringBuilder(48);

        builder.append("UDP-");

        if (null == localParamValue)
        {
            builder.append(localData.getHostString())
                .append(':')
                .append(localData.getPort());
        }
        else
        {
            builder.append(localParamValue);
        }

        builder.append('-');

        if (null == remoteParamValue)
        {
            builder.append(remoteData.getHostString())
                .append(':')
                .append(remoteData.getPort());
        }
        else
        {
            builder.append(remoteParamValue);
        }

        return builder.toString();
    }

    /**
     * Remote data address and port.
     *
     * @return remote data address and port.
     */
    public InetSocketAddress remoteData()
    {
        return remoteData;
    }

    /**
     * Local data address and port.
     *
     * @return local data address port.
     */
    public InetSocketAddress localData()
    {
        return localData;
    }

    /**
     * Remote control address information.
     *
     * @return remote control address information.
     */
    public InetSocketAddress remoteControl()
    {
        return remoteControl;
    }

    /**
     * Local control address and port.
     *
     * @return local control address and port.
     */
    public InetSocketAddress localControl()
    {
        return localControl;
    }

    /**
     * Get the {@link ChannelUri} for this channel.
     *
     * @return the {@link ChannelUri} for this channel.
     */
    public ChannelUri channelUri()
    {
        return channelUri;
    }

    /**
     * Has this channel got a multicast TTL value set so that {@link #multicastTtl()} is valid.
     *
     * @return true if this channel is a multicast TTL set otherwise false.
     */
    public boolean hasMulticastTtl()
    {
        return hasMulticastTtl;
    }

    /**
     * Multicast TTL value.
     *
     * @return multicast TTL value.
     */
    public int multicastTtl()
    {
        return multicastTtl;
    }

    /**
     * The canonical form for the channel.
     *
     * @return canonical form for channel.
     * @see UdpChannel#canonicalise
     */
    public String canonicalForm()
    {
        return canonicalForm;
    }

    /**
     * The {@link #canonicalForm()} for the channel.
     *
     * @return the {@link #canonicalForm()} for the channel.
     */
    public String toString()
    {
        return canonicalForm;
    }

    /**
     * Is the channel UDP multicast.
     *
     * @return true if the channel is UDP multicast.
     */
    public boolean isMulticast()
    {
        return isMulticast;
    }

    /**
     * Local interface to be used by the channel.
     *
     * @return {@link NetworkInterface} for the local interface used by the channel.
     */
    public NetworkInterface localInterface()
    {
        return localInterface;
    }

    /**
     * Original URI of the channel URI.
     *
     * @return the original uri string from the client.
     */
    public String originalUriString()
    {
        return uriStr;
    }

    /**
     * Get the {@link ProtocolFamily} for this channel.
     *
     * @return the {@link ProtocolFamily} for this channel.
     */
    public ProtocolFamily protocolFamily()
    {
        return protocolFamily;
    }

    /**
     * Get the tag value on the channel which is only valid if {@link #hasTag()} is true.
     *
     * @return the tag value on the channel.
     */
    public long tag()
    {
        return tag;
    }

    /**
     * The channel's control mode.
     *
     * @return the control mode for the channel.
     */
    public ControlMode controlMode()
    {
        return controlMode;
    }


    /**
     * Does the channel have manual control mode specified.
     *
     * @return does channel have manual control mode specified.
     */
    public boolean isManualControlMode()
    {
        return ControlMode.MANUAL == controlMode;
    }

    /**
     * Does the channel have dynamic control mode specified.
     *
     * @return does channel have dynamic control mode specified.
     */
    public boolean isDynamicControlMode()
    {
        return ControlMode.DYNAMIC == controlMode;
    }

    /**
     * Does the channel have response control mode specified.
     *
     * @return does the channel have response control mode specified.
     */
    public boolean isResponseControlMode()
    {
        return ControlMode.RESPONSE == controlMode;
    }

    /**
     * Does the channel have an explicit endpoint address?
     *
     * @return does channel have an explicit endpoint address or not?
     */
    public boolean hasExplicitEndpoint()
    {
        return hasExplicitEndpoint;
    }

    /**
     * Does the channel have an explicit control address as used with multi-destination-cast or not?
     *
     * @return does channel have an explicit control address or not?
     */
    public boolean hasExplicitControl()
    {
        return hasExplicitControl;
    }

    /**
     * Has the URI a tag to indicate entity relationships and if {@link #tag()} is valid.
     *
     * @return true if the channel has a tag.
     */
    public boolean hasTag()
    {
        return hasTag;
    }

    /**
     * Is the channel configured as multi-destination.
     *
     * @return true if the channel configured as multi-destination.
     */
    public boolean isMultiDestination()
    {
        return controlMode.isMultiDestination();
    }

    /**
     * Does the channel have group semantics (multicast or MDC).
     *
     * @return true if the channel has group semantics.
     */
    public boolean hasGroupSemantics()
    {
        return isMulticast || isMultiDestination();
    }

    /**
     * Get the socket receive buffer length.
     *
     * @return socket receive buffer length or 0 if not specified.
     */
    public int socketRcvbufLength()
    {
        return socketRcvbufLength;
    }

    /**
     * Get the socket receive buffer length.
     *
     * @param defaultValue to be used if the UdpChannel's value is 0 (unspecified).
     * @return socket receive buffer length or 0 if not specified.
     */
    public int socketRcvbufLengthOrDefault(final int defaultValue)
    {
        return 0 != socketRcvbufLength ? socketRcvbufLength : defaultValue;
    }

    /**
     * Get the socket send buffer length.
     *
     * @return socket send buffer length or 0 if not specified.
     */
    public int socketSndbufLength()
    {
        return socketSndbufLength;
    }

    /**
     * Get the socket send buffer length.
     *
     * @param defaultValue to be used if the UdpChannel's value is 0 (unspecified).
     * @return socket send buffer length or defaultValue if not specified.
     */
    public int socketSndbufLengthOrDefault(final int defaultValue)
    {
        return 0 != socketSndbufLength ? socketSndbufLength : defaultValue;
    }

    /**
     * Get the receiver window length used as the initial window length for congestion control.
     *
     * @return receiver window length or 0 if not specified.
     */
    public int receiverWindowLength()
    {
        return receiverWindowLength;
    }

    /**
     * Get the receiver window length used as the initial window length for congestion control.
     *
     * @param defaultValue to be used if the UdpChannel's value is 0 (unspecified).
     * @return receiver window length or defaultValue if not specified.
     */
    public int receiverWindowLengthOrDefault(final int defaultValue)
    {
        return 0 != receiverWindowLength() ? receiverWindowLength() : defaultValue;
    }

    /**
     * The length of the initial nak delay to be used by the LossDetector on this channel.
     *
     * @return delay in nanoseconds or null if the value is not set.
     */
    public Long nakDelayNs()
    {
        return nakDelayNs;
    }

    /**
     * Does this channel have a tag match to another channel having INADDR_ANY endpoints.
     *
     * @param udpChannel to match against.
     * @param localAddress local address override to use for this channel.
     * @param remoteAddress remote address override to use for this channel.
     * @return true if there is a match otherwise false.
     */
    public boolean matchesTag(
        final UdpChannel udpChannel,
        final InetSocketAddress localAddress,
        final InetSocketAddress remoteAddress)
    {
        if (!hasTag || !udpChannel.hasTag() || tag != udpChannel.tag())
        {
            return false;
        }

        if (!hasMatchingControlMode(udpChannel))
        {
            throw new IllegalArgumentException(
                "matching tag=" + tag + " has mismatched control-mode: " + uriStr + " <> " + udpChannel.uriStr);
        }

        if (!hasMatchingAddress(udpChannel, localAddress, remoteAddress))
        {
            throw new IllegalArgumentException(
                "matching tag=" + tag + " has mismatched endpoint or control: " + uriStr + " <> " + udpChannel.uriStr);
        }

        return true;
    }

    private boolean isWildcard()
    {
        return remoteData.getAddress().isAnyLocalAddress() &&
            remoteData.getPort() == 0 &&
            localData.getAddress().isAnyLocalAddress() &&
            localData.getPort() == 0;
    }

    private boolean hasMatchingControlMode(final UdpChannel udpChannel)
    {
        return controlMode() == ControlMode.NONE || controlMode() == udpChannel.controlMode();
    }

    private boolean hasMatchingAddress(
        final UdpChannel udpChannel,
        final InetSocketAddress localAddress,
        final InetSocketAddress remoteAddress)
    {
        final InetSocketAddress otherLocalData = localAddress != null ? localAddress : udpChannel.localData();
        final InetSocketAddress otherRemoteData = remoteAddress != null ? remoteAddress : udpChannel.remoteData();

        return isWildcard() || remoteData().getAddress().equals(otherRemoteData.getAddress()) &&
            remoteData().getPort() == otherRemoteData.getPort() &&
            localData().getAddress().equals(otherLocalData.getAddress()) &&
            localData().getPort() == otherLocalData.getPort();
    }

    /**
     * Used for debugging to get a human-readable description of the channel.
     *
     * @return a human-readable description of the channel.
     */
    public String description()
    {
        return "localData: " + formatAddressAndPort(localData.getAddress(), localData.getPort()) +
            ", remoteData: " + formatAddressAndPort(remoteData.getAddress(), remoteData.getPort()) +
            ", ttl: " + multicastTtl;
    }

    /**
     * Channels are considered equal if the {@link #canonicalForm()} is equal.
     *
     * @param o object to be compared with.
     * @return true if the {@link #canonicalForm()} is equal, otherwise false.
     */
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final UdpChannel that = (UdpChannel)o;

        return Objects.equals(canonicalForm, that.canonicalForm);
    }

    /**
     * The hash code for the {@link #canonicalForm()}.
     *
     * @return the hash code for the {@link #canonicalForm()}.
     */
    public int hashCode()
    {
        return canonicalForm != null ? canonicalForm.hashCode() : 0;
    }

    /**
     * Get the endpoint destination address from the URI.
     *
     * @param uri          to check.
     * @param nameResolver to use for resolution.
     * @return endpoint address for URI.
     */
    public static InetSocketAddress destinationAddress(final ChannelUri uri, final NameResolver nameResolver)
    {
        try
        {
            validateConfiguration(uri);

            final String endpointValue = uri.get(ENDPOINT_PARAM_NAME);
            return SocketAddressParser.parse(endpointValue, ENDPOINT_PARAM_NAME, false, nameResolver);
        }
        catch (final Exception ex)
        {
            throw new InvalidChannelException(ex);
        }
    }

    /**
     * Check if the address pointed to by the endpoint is multicast.
     *
     * @param uri to check.
     * @return {@code true} if the destination uses multicast address.
     * @since 1.44.0
     */
    public static boolean isMulticastDestinationAddress(final ChannelUri uri)
    {
        try
        {
            validateConfiguration(uri);

            final String endpointValue = uri.get(ENDPOINT_PARAM_NAME);
            return isMulticastEndpoint(endpointValue);
        }
        catch (final Exception ex)
        {
            throw new InvalidChannelException(ex);
        }
    }

    /**
     * Check if the endpoint is multicast.
     *
     * @param endpoint to check.
     * @return {@code true} if the destination uses multicast address.
     * @since 1.47.0
     */
    public static boolean isMulticastEndpoint(final String endpoint)
    {
        return SocketAddressParser.isMulticastAddress(endpoint);
    }

    /**
     * Resolve and endpoint into a {@link InetSocketAddress}.
     *
     * @param endpoint       to resolve.
     * @param uriParamName   for the resolution.
     * @param isReResolution for the resolution.
     * @param nameResolver   to be used for hostname.
     * @return address for endpoint.
     */
    public static InetSocketAddress resolve(
        final String endpoint, final String uriParamName, final boolean isReResolution, final NameResolver nameResolver)
    {
        return SocketAddressParser.parse(endpoint, uriParamName, isReResolution, nameResolver);
    }

    /**
     * Offset to store the channel receive timestamp in a user message.
     *
     * @return offset of channel receive timestamps.
     */
    public int channelReceiveTimestampOffset()
    {
        return channelReceiveTimestampOffset;
    }

    /**
     * Check if channel receive timestamps should be recorded.
     *
     * @return true if channel receive timestamps should be collected false otherwise.
     */
    public boolean isChannelReceiveTimestampEnabled()
    {
        return RESERVED_VALUE_MESSAGE_OFFSET == channelReceiveTimestampOffset || 0 <= channelReceiveTimestampOffset;
    }

    /**
     * Check if channel send timestamps should be recorded.
     *
     * @return true if channel send timestamps should be collected false otherwise.
     */
    public boolean isChannelSendTimestampEnabled()
    {
        return RESERVED_VALUE_MESSAGE_OFFSET == channelSendTimestampOffset || 0 <= channelSendTimestampOffset;
    }

    /**
     * Offset to store the channel send timestamp in a user message.
     *
     * @return offset of channel send timestamps.
     */
    public int channelSendTimestampOffset()
    {
        return channelSendTimestampOffset;
    }

    /**
     * Group tag specified for this channel or null if not specified.
     *
     * @return group tag for the channel
     */
    public Long groupTag()
    {
        return groupTag;
    }


    private static InetSocketAddress getMulticastControlAddress(final InetSocketAddress endpointAddress)
        throws UnknownHostException
    {
        final byte[] addressAsBytes = endpointAddress.getAddress().getAddress();
        validateDataAddress(addressAsBytes);

        addressAsBytes[addressAsBytes.length - 1]++;
        return new InetSocketAddress(getByAddress(addressAsBytes), endpointAddress.getPort());
    }

    private static UnresolvedInterface getInterface(final ChannelUri uri) throws UnknownHostException
    {
        final String interfaceValue = uri.get(INTERFACE_PARAM_NAME);
        if (null != interfaceValue)
        {
            return UnresolvedInterface.parse(interfaceValue);
        }

        return InterfaceSearchAddress.wildcard();
    }

    private static InetSocketAddress getEndpointAddress(final ChannelUri uri, final NameResolver nameResolver)
        throws UnknownHostException
    {
        InetSocketAddress address = null;
        final String endpointValue = uri.get(ENDPOINT_PARAM_NAME);
        if (null != endpointValue)
        {
            address = SocketAddressParser.parse(
                endpointValue, ENDPOINT_PARAM_NAME, false, nameResolver);

            if (address.isUnresolved())
            {
                throw new UnknownHostException(
                    "unresolved - " + ENDPOINT_PARAM_NAME + "=" + endpointValue +
                    ", name-resolver=" + nameResolver.name());
            }
        }

        return address;
    }

    private static InetSocketAddress getExplicitControlAddress(final ChannelUri uri, final NameResolver nameResolver)
        throws UnknownHostException
    {
        InetSocketAddress address = null;
        final String controlValue = uri.get(MDC_CONTROL_PARAM_NAME);
        if (null != controlValue)
        {
            address = SocketAddressParser.parse(
                controlValue, MDC_CONTROL_PARAM_NAME, false, nameResolver);

            if (address.isUnresolved())
            {
                throw new UnknownHostException(
                    "unresolved - " + MDC_CONTROL_PARAM_NAME + "=" + controlValue +
                    ", name-resolver=" + nameResolver.getClass().getName());
            }
        }

        return address;
    }

    private static void validateDataAddress(final byte[] addressAsBytes)
    {
        if (BitUtil.isEven(addressAsBytes[addressAsBytes.length - 1]))
        {
            throw new IllegalArgumentException("multicast data address must be odd");
        }
    }

    private static void validateConfiguration(final ChannelUri uri)
    {
        validateMedia(uri);
    }

    private static void validateMedia(final ChannelUri uri)
    {
        if (!uri.isUdp())
        {
            throw new IllegalArgumentException("UdpChannel only supports UDP media: " + uri);
        }
    }

    static final class Context
    {
        ControlMode controlMode = ControlMode.NONE;
        boolean hasExplicitEndpoint = false;
        boolean hasExplicitControl = false;
        boolean isMulticast = false;
        boolean hasMulticastTtl = false;
        boolean hasTagId = false;
        boolean hasNoDistinguishingCharacteristic = false;
        int socketRcvbufLength = 0;
        int socketSndbufLength = 0;
        int receiverWindowLength = 0;
        int multicastTtl;
        long tagId;
        InetSocketAddress remoteData;
        InetSocketAddress localData;
        InetSocketAddress remoteControl;
        InetSocketAddress localControl;
        String uriStr;
        String canonicalForm;
        NetworkInterface localInterface;
        ProtocolFamily protocolFamily;
        ChannelUri channelUri;
        int channelReceiveTimestampOffset;
        int channelSendTimestampOffset;
        Long groupTag = null;
        Long nakDelayNs = null;

        Context uriStr(final String uri)
        {
            uriStr = uri;
            return this;
        }

        Context remoteDataAddress(final InetSocketAddress remoteData)
        {
            this.remoteData = remoteData;
            return this;
        }

        Context localDataAddress(final InetSocketAddress localData)
        {
            this.localData = localData;
            return this;
        }

        Context remoteControlAddress(final InetSocketAddress remoteControl)
        {
            this.remoteControl = remoteControl;
            return this;
        }

        Context localControlAddress(final InetSocketAddress localControl)
        {
            this.localControl = localControl;
            return this;
        }

        @SuppressWarnings("UnusedReturnValue")
        Context canonicalForm(final String canonicalForm)
        {
            this.canonicalForm = canonicalForm;
            return this;
        }

        Context localInterface(final NetworkInterface networkInterface)
        {
            this.localInterface = networkInterface;
            return this;
        }

        Context protocolFamily(final ProtocolFamily protocolFamily)
        {
            this.protocolFamily = protocolFamily;
            return this;
        }

        Context hasMulticastTtl(final boolean hasMulticastTtl)
        {
            this.hasMulticastTtl = hasMulticastTtl;
            return this;
        }

        @SuppressWarnings("UnusedReturnValue")
        Context multicastTtl(final int multicastTtl)
        {
            this.multicastTtl = multicastTtl;
            return this;
        }

        @SuppressWarnings("UnusedReturnValue")
        Context tagId(final long tagId)
        {
            this.tagId = tagId;
            return this;
        }

        Context channelUri(final ChannelUri channelUri)
        {
            this.channelUri = channelUri;
            return this;
        }

        Context controlMode(final ControlMode controlMode)
        {
            this.controlMode = controlMode;
            return this;
        }

        Context hasExplicitEndpoint(final boolean hasExplicitEndpoint)
        {
            this.hasExplicitEndpoint = hasExplicitEndpoint;
            return this;
        }

        Context hasExplicitControl(final boolean hasExplicitControl)
        {
            this.hasExplicitControl = hasExplicitControl;
            return this;
        }

        Context isMulticast(final boolean isMulticast)
        {
            this.isMulticast = isMulticast;
            return this;
        }

        Context hasTagId(final boolean hasTagId)
        {
            this.hasTagId = hasTagId;
            return this;
        }

        Context hasNoDistinguishingCharacteristic(final boolean hasNoDistinguishingCharacteristic)
        {
            this.hasNoDistinguishingCharacteristic = hasNoDistinguishingCharacteristic;
            return this;
        }

        Context socketRcvbufLength(final int socketRcvbufLength)
        {
            this.socketRcvbufLength = socketRcvbufLength;
            return this;
        }

        Context socketSndbufLength(final int socketSndbufLength)
        {
            this.socketSndbufLength = socketSndbufLength;
            return this;
        }

        Context receiverWindowLength(final int receiverWindowLength)
        {
            this.receiverWindowLength = receiverWindowLength;
            return this;
        }

        @SuppressWarnings("UnusedReturnValue")
        Context channelReceiveTimestampOffset(final int timestampOffset)
        {
            this.channelReceiveTimestampOffset = timestampOffset;
            return this;
        }

        @SuppressWarnings("UnusedReturnValue")
        Context channelSendTimestampOffset(final int timestampOffset)
        {
            this.channelSendTimestampOffset = timestampOffset;
            return this;
        }

        Context nakDelayNs(final Long nakDelayNs)
        {
            this.nakDelayNs = nakDelayNs;
            return this;
        }

        public void groupTag(final Long groupTag)
        {
            this.groupTag = groupTag;
        }
    }
}
