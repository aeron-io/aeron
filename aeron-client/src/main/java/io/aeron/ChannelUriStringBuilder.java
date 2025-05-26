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
package io.aeron;

import io.aeron.logbuffer.LogBufferDescriptor;

import static io.aeron.ChannelUri.SPY_QUALIFIER;
import static io.aeron.CommonContext.*;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MAX_LENGTH;
import static org.agrona.SystemUtil.*;

/**
 * Typesafe means of building a channel URI associated with a {@link Publication} or {@link Subscription}.
 *
 * @see Aeron#addPublication(String, int)
 * @see Aeron#addSubscription(String, int)
 * @see ChannelUri
 */
public final class ChannelUriStringBuilder
{
    /**
     * Can be used when the likes of session-id wants to reference another entity such as a tagged publication.
     * <p>
     * For example {@code session-id=tag:777} where the publication uses {@code tags=777}.
     */
    public static final String TAG_PREFIX = "tag:";

    private final StringBuilder sb = new StringBuilder(64);

    private String prefix;
    private String media;
    private String endpoint;
    private String networkInterface;
    private String controlEndpoint;
    private String controlMode;
    private String tags;
    private String alias;
    private String cc;
    private String fc;
    private String mediaReceiveTimestampOffset;
    private String channelReceiveTimestampOffset;
    private String channelSendTimestampOffset;
    private String responseEndpoint;
    private Boolean reliable;
    private Boolean sparse;
    private Boolean eos;
    private Boolean tether;
    private Boolean group;
    private Boolean rejoin;
    private Boolean ssc;
    private Integer ttl;
    private Integer mtu;
    private Integer termLength;
    private Integer initialTermId;
    private Integer termId;
    private Integer termOffset;
    private Integer socketSndbufLength;
    private Integer socketRcvbufLength;
    private Integer receiverWindowLength;
    private Integer maxResend;
    private Integer streamId;
    private Integer publicationWindowLength;
    private Long sessionId;
    private Long groupTag;
    private Long linger;
    private Long responseCorrelationId;
    private Long nakDelay;
    private Long untetheredWindowLimitTimeoutNs;
    private Long untetheredLingerTimeoutNs;
    private Long untetheredRestingTimeoutNs;
    private boolean isSessionIdTagged;

    /**
     * Default constructor.
     */
    public ChannelUriStringBuilder()
    {
    }

    /**
     * Constructs the ChannelUriStringBuilder with the initial values derived from the supplied URI. Will parse the
     * incoming URI during this process, so could through an exception at this point of the URI is badly formed.
     *
     * @param initialUri initial values for the builder.
     */
    public ChannelUriStringBuilder(final String initialUri)
    {
        this(ChannelUri.parse(initialUri));
    }

    /**
     * Constructs the ChannelUriStringBuilder with the initial values derived from the supplied ChannelUri.
     *
     * @param channelUri initial values for the builder.
     */
    public ChannelUriStringBuilder(final ChannelUri channelUri)
    {
        isSessionIdTagged = false;

        prefix(channelUri);
        media(channelUri);
        endpoint(channelUri);
        networkInterface(channelUri);
        controlEndpoint(channelUri);
        controlMode(channelUri);
        tags(channelUri);
        alias(channelUri);
        congestionControl(channelUri);
        flowControl(channelUri);
        reliable(channelUri);
        ttl(channelUri);
        mtu(channelUri);
        termLength(channelUri);
        initialTermId(channelUri);
        termId(channelUri);
        termOffset(channelUri);
        sessionId(channelUri);
        group(channelUri);
        linger(channelUri);
        sparse(channelUri);
        eos(channelUri);
        tether(channelUri);
        groupTag(channelUri);
        rejoin(channelUri);
        spiesSimulateConnection(channelUri);
        socketRcvbufLength(channelUri);
        socketSndbufLength(channelUri);
        receiverWindowLength(channelUri);
        mediaReceiveTimestampOffset(channelUri);
        channelReceiveTimestampOffset(channelUri);
        channelSendTimestampOffset(channelUri);
        responseEndpoint(channelUri);
        responseCorrelationId(channelUri);
        nakDelay(channelUri);
        untetheredWindowLimitTimeout(channelUri);
        untetheredLingerTimeout(channelUri);
        untetheredRestingTimeout(channelUri);
        maxResend(channelUri);
        streamId(channelUri);
        publicationWindowLength(channelUri);
    }

    /**
     * Clear out all the values thus setting back to the initial state.
     *
     * @return this for a fluent API.
     */
    public ChannelUriStringBuilder clear()
    {
        prefix = null;
        media = null;
        endpoint = null;
        networkInterface = null;
        controlEndpoint = null;
        controlMode = null;
        tags = null;
        alias = null;
        cc = null;
        fc = null;
        reliable = null;
        ttl = null;
        mtu = null;
        termLength = null;
        initialTermId = null;
        termId = null;
        termOffset = null;
        sessionId = null;
        groupTag = null;
        linger = null;
        sparse = null;
        eos = null;
        tether = null;
        group = null;
        rejoin = null;
        isSessionIdTagged = false;
        socketRcvbufLength = null;
        socketSndbufLength = null;
        receiverWindowLength = null;
        mediaReceiveTimestampOffset = null;
        channelReceiveTimestampOffset = null;
        channelSendTimestampOffset = null;
        responseEndpoint = null;
        responseCorrelationId = null;
        maxResend = null;
        streamId = null;
        publicationWindowLength = null;

        return this;
    }

    /**
     * Validates that the collection of set parameters are valid together.
     *
     * @return this for a fluent API.
     * @throws IllegalArgumentException if the combination of params is invalid.
     */
    public ChannelUriStringBuilder validate()
    {
        if (null == media)
        {
            throw new IllegalArgumentException("media type is mandatory");
        }

        if (CommonContext.UDP_MEDIA.equals(media) && (null == endpoint && null == controlEndpoint))
        {
            throw new IllegalArgumentException("either 'endpoint' or 'control' must be specified for UDP.");
        }

        final boolean anyNonNull = null != initialTermId || null != termId || null != termOffset;
        final boolean anyNull = null == initialTermId || null == termId || null == termOffset;
        if (anyNonNull)
        {
            if (anyNull)
            {
                throw new IllegalArgumentException(
                    "either all or none of the parameters ['initialTermId', 'termId', 'termOffset'] must be provided");
            }

            if (termId - initialTermId < 0)
            {
                throw new IllegalArgumentException(
                    "difference greater than 2^31 - 1: termId=" + termId + " - initialTermId=" + initialTermId);
            }

            if (null != termLength && termOffset > termLength)
            {
                throw new IllegalArgumentException("termOffset=" + termOffset + " > termLength=" + termLength);
            }
        }

        return this;
    }

    /**
     * Set the prefix for taking an additional action such as spying on an outgoing publication with "aeron-spy".
     *
     * @param prefix to be applied to the URI before the scheme.
     * @return this for a fluent API.
     * @see ChannelUri#SPY_QUALIFIER
     */
    public ChannelUriStringBuilder prefix(final String prefix)
    {
        if (null != prefix && !prefix.isEmpty() && !prefix.equals(SPY_QUALIFIER))
        {
            throw new IllegalArgumentException("invalid prefix: " + prefix);
        }

        this.prefix = prefix;
        return this;
    }

    /**
     * Set the prefix value to be what is in the {@link ChannelUri}.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see ChannelUri#SPY_QUALIFIER
     */
    public ChannelUriStringBuilder prefix(final ChannelUri channelUri)
    {
        return prefix(channelUri.prefix());
    }

    /**
     * Get the prefix for the additional action to be taken on the request.
     *
     * @return the prefix for the additional action to be taken on the request.
     */
    public String prefix()
    {
        return prefix;
    }

    /**
     * Set the media for this channel. Valid values are "udp" and "ipc".
     *
     * @param media for this channel.
     * @return this for a fluent API.
     */
    public ChannelUriStringBuilder media(final String media)
    {
        switch (media)
        {
            case CommonContext.UDP_MEDIA:
            case CommonContext.IPC_MEDIA:
                break;

            default:
                throw new IllegalArgumentException("invalid media: " + media);
        }

        this.media = media;
        return this;
    }

    /**
     * Set the endpoint value to be what is in the {@link ChannelUri}.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     */
    public ChannelUriStringBuilder media(final ChannelUri channelUri)
    {
        return media(channelUri.media());
    }

    /**
     * The media over which the channel transmits.
     *
     * @return the media over which the channel transmits.
     */
    public String media()
    {
        return media;
    }

    /**
     * Set the endpoint address:port pairing for the channel. This is the address the publication sends to and the
     * address the subscription receives from.
     *
     * @param endpoint address and port for the channel.
     * @return this for a fluent API.
     * @see CommonContext#ENDPOINT_PARAM_NAME
     */
    public ChannelUriStringBuilder endpoint(final String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    /**
     * Set the endpoint value to be what is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#ENDPOINT_PARAM_NAME
     */
    public ChannelUriStringBuilder endpoint(final ChannelUri channelUri)
    {
        return endpoint(channelUri.get(ENDPOINT_PARAM_NAME));
    }

    /**
     * Get the endpoint address:port pairing for the channel.
     *
     * @return the endpoint address:port pairing for the channel.
     * @see CommonContext#ENDPOINT_PARAM_NAME
     */
    public String endpoint()
    {
        return endpoint;
    }

    /**
     * Set the address of the local interface in the form host:[port]/[subnet mask] for routing traffic.
     *
     * @param networkInterface for routing traffic.
     * @return this for a fluent API.
     * @see CommonContext#INTERFACE_PARAM_NAME
     */
    public ChannelUriStringBuilder networkInterface(final String networkInterface)
    {
        this.networkInterface = networkInterface;
        return this;
    }

    /**
     * Set the network interface value to be what is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#INTERFACE_PARAM_NAME
     */
    public ChannelUriStringBuilder networkInterface(final ChannelUri channelUri)
    {
        return networkInterface(channelUri.get(INTERFACE_PARAM_NAME));
    }

    /**
     * Get the address of the local interface in the form host:[port]/[subnet mask] for routing traffic.
     *
     * @return the address of the local interface in the form host:[port]/[subnet mask] for routing traffic.
     * @see CommonContext#INTERFACE_PARAM_NAME
     */
    public String networkInterface()
    {
        return networkInterface;
    }

    /**
     * Set the control address:port pair for dynamically joining a multi-destination-cast publication.
     *
     * @param controlEndpoint for joining MDC control socket.
     * @return this for a fluent API.
     * @see CommonContext#MDC_CONTROL_PARAM_NAME
     */
    public ChannelUriStringBuilder controlEndpoint(final String controlEndpoint)
    {
        this.controlEndpoint = controlEndpoint;
        return this;
    }

    /**
     * Set the control endpoint value to be what is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#MDC_CONTROL_PARAM_NAME
     */
    public ChannelUriStringBuilder controlEndpoint(final ChannelUri channelUri)
    {
        return controlEndpoint(channelUri.get(MDC_CONTROL_PARAM_NAME));
    }

    /**
     * Get the control address:port pair for dynamically joining a multi-destination-cast publication.
     *
     * @return the control address:port pair for dynamically joining a multi-destination-cast publication.
     * @see CommonContext#MDC_CONTROL_PARAM_NAME
     */
    public String controlEndpoint()
    {
        return controlEndpoint;
    }

    /**
     * Set the control mode for multi-destination-cast. Set to "manual" for allowing control from the publication API.
     *
     * @param controlMode for taking control of MDC.
     * @return this for a fluent API.
     * @see Publication#addDestination(String)
     * @see Publication#removeDestination(String)
     * @see CommonContext#MDC_CONTROL_MODE_PARAM_NAME
     * @see CommonContext#MDC_CONTROL_MODE_MANUAL
     * @see CommonContext#MDC_CONTROL_MODE_DYNAMIC
     * @see CommonContext#CONTROL_MODE_RESPONSE
     */
    public ChannelUriStringBuilder controlMode(final String controlMode)
    {
        if (null != controlMode &&
            !controlMode.equals(MDC_CONTROL_MODE_MANUAL) &&
            !controlMode.equals(MDC_CONTROL_MODE_DYNAMIC) &&
            !controlMode.equals(CONTROL_MODE_RESPONSE))
        {
            throw new IllegalArgumentException("invalid control mode: " + controlMode);
        }

        this.controlMode = controlMode;
        return this;
    }

    /**
     * Set the control mode to be what is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#MDC_CONTROL_MODE_PARAM_NAME
     */
    public ChannelUriStringBuilder controlMode(final ChannelUri channelUri)
    {
        return controlMode(channelUri.get(MDC_CONTROL_MODE_PARAM_NAME));
    }

    /**
     * Get the control mode for multi-destination-cast.
     *
     * @return the control mode for multi-destination-cast.
     * @see CommonContext#MDC_CONTROL_MODE_PARAM_NAME
     * @see CommonContext#MDC_CONTROL_MODE_MANUAL
     * @see CommonContext#MDC_CONTROL_MODE_DYNAMIC
     * @see CommonContext#CONTROL_MODE_RESPONSE
     */
    public String controlMode()
    {
        return controlMode;
    }

    /**
     * Set the subscription semantics for if loss is acceptable, or not, for a reliable message delivery.
     *
     * @param isReliable false if loss can be gap-filled.
     * @return this for a fluent API.
     * @see CommonContext#RELIABLE_STREAM_PARAM_NAME
     */
    public ChannelUriStringBuilder reliable(final Boolean isReliable)
    {
        this.reliable = isReliable;
        return this;
    }

    /**
     * Set the reliable value to be what is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#RELIABLE_STREAM_PARAM_NAME
     */
    public ChannelUriStringBuilder reliable(final ChannelUri channelUri)
    {
        final String reliableValue = channelUri.get(RELIABLE_STREAM_PARAM_NAME);
        if (null == reliableValue)
        {
            reliable = null;
            return this;
        }
        else
        {
            return reliable(Boolean.valueOf(reliableValue));
        }
    }

    /**
     * Get the subscription semantics for if loss is acceptable, or not, for a reliable message delivery.
     *
     * @return the subscription semantics for if loss is acceptable, or not, for a reliable message delivery.
     * @see CommonContext#RELIABLE_STREAM_PARAM_NAME
     */
    public Boolean reliable()
    {
        return reliable;
    }

    /**
     * Set the Time To Live (TTL) for a multicast datagram. Valid values are 0-255 for the number of hops the datagram
     * can progress along.
     *
     * @param ttl value for a multicast datagram.
     * @return this for a fluent API.
     * @see CommonContext#TTL_PARAM_NAME
     */
    public ChannelUriStringBuilder ttl(final Integer ttl)
    {
        if (null != ttl && (ttl < 0 || ttl > 255))
        {
            throw new IllegalArgumentException("TTL not in range 0-255: " + ttl);
        }

        this.ttl = ttl;
        return this;
    }

    /**
     * Set the ttl value to be what is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#TTL_PARAM_NAME
     */
    public ChannelUriStringBuilder ttl(final ChannelUri channelUri)
    {
        final String ttlValue = channelUri.get(TTL_PARAM_NAME);
        if (null == ttlValue)
        {
            ttl = null;
            return this;
        }
        else
        {
            try
            {
                return ttl(Integer.valueOf(ttlValue));
            }
            catch (final NumberFormatException ex)
            {
                throw new IllegalArgumentException("'ttl' must be a value integer", ex);
            }
        }
    }

    /**
     * Get the Time To Live (TTL) for a multicast datagram.
     *
     * @return the Time To Live (TTL) for a multicast datagram.
     * @see CommonContext#TTL_PARAM_NAME
     */
    public Integer ttl()
    {
        return ttl;
    }

    /**
     * Set the maximum transmission unit (MTU) including Aeron header for a datagram payload. If this is greater
     * than the network MTU for UDP then the packet will be fragmented and can amplify the impact of loss.
     *
     * @param mtu the maximum transmission unit including Aeron header for a datagram payload.
     * @return this for a fluent API.
     * @see CommonContext#MTU_LENGTH_PARAM_NAME
     */
    public ChannelUriStringBuilder mtu(final Integer mtu)
    {
        if (null != mtu)
        {
            if (mtu < 32 || mtu > 65504)
            {
                throw new IllegalArgumentException("MTU not in range 32-65504: " + mtu);
            }

            if ((mtu & (FRAME_ALIGNMENT - 1)) != 0)
            {
                throw new IllegalArgumentException("MTU not a multiple of FRAME_ALIGNMENT: mtu=" + mtu);
            }
        }

        this.mtu = mtu;
        return this;
    }

    /**
     * Set the mtu value to be what is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#MTU_LENGTH_PARAM_NAME
     */
    public ChannelUriStringBuilder mtu(final ChannelUri channelUri)
    {
        final String mtuValue = channelUri.get(MTU_LENGTH_PARAM_NAME);
        if (null == mtuValue)
        {
            mtu = null;
            return this;
        }
        else
        {
            final long value = parseSize(MTU_LENGTH_PARAM_NAME, mtuValue);
            if (value > Integer.MAX_VALUE)
            {
                throw new IllegalArgumentException(MTU_LENGTH_PARAM_NAME + " " + value + " > " + Integer.MAX_VALUE);
            }

            return mtu((int)value);
        }
    }

    /**
     * Get the maximum transmission unit (MTU) including Aeron header for a datagram payload. If this is greater
     * than the network MTU for UDP then the packet will be fragmented and can amplify the impact of loss.
     *
     * @return the maximum transmission unit (MTU) including Aeron header for a datagram payload.
     * @see CommonContext#MTU_LENGTH_PARAM_NAME
     */
    public Integer mtu()
    {
        return mtu;
    }

    /**
     * Set the length of buffer used for each term of the log. Valid values are powers of 2 in the 64K - 1G range.
     *
     * @param termLength of the buffer used for each term of the log.
     * @return this for a fluent API.
     * @see CommonContext#TERM_LENGTH_PARAM_NAME
     */
    public ChannelUriStringBuilder termLength(final Integer termLength)
    {
        if (null != termLength)
        {
            LogBufferDescriptor.checkTermLength(termLength);
        }

        this.termLength = termLength;
        return this;
    }

    /**
     * Set the termLength value to be what is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#TERM_LENGTH_PARAM_NAME
     */
    public ChannelUriStringBuilder termLength(final ChannelUri channelUri)
    {
        final String termLengthValue = channelUri.get(TERM_LENGTH_PARAM_NAME);
        if (null == termLengthValue)
        {
            termLength = null;
            return this;
        }
        else
        {
            final long value = parseSize(TERM_LENGTH_PARAM_NAME, termLengthValue);
            if (value > Integer.MAX_VALUE)
            {
                throw new IllegalArgumentException(
                    "term length more than max length of " + TERM_MAX_LENGTH + ": value=" + value);
            }

            return termLength((int)value);
        }
    }

    /**
     * Get the length of buffer used for each term of the log.
     *
     * @return the length of buffer used for each term of the log.
     * @see CommonContext#TERM_LENGTH_PARAM_NAME
     */
    public Integer termLength()
    {
        return termLength;
    }

    /**
     * Set the initial term id at which a publication will start.
     *
     * @param initialTermId the initial term id at which a publication will start.
     * @return this for a fluent API.
     * @see CommonContext#INITIAL_TERM_ID_PARAM_NAME
     */
    public ChannelUriStringBuilder initialTermId(final Integer initialTermId)
    {
        this.initialTermId = initialTermId;
        return this;
    }

    /**
     * Set the initialTermId value to be what is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#INITIAL_TERM_ID_PARAM_NAME
     */
    public ChannelUriStringBuilder initialTermId(final ChannelUri channelUri)
    {
        final String initialTermIdValue = channelUri.get(INITIAL_TERM_ID_PARAM_NAME);
        if (null == initialTermIdValue)
        {
            initialTermId = null;
            return this;
        }
        else
        {
            try
            {
                return initialTermId(Integer.valueOf(initialTermIdValue));
            }
            catch (final NumberFormatException ex)
            {
                throw new IllegalArgumentException("'initial-term-id' must be a valid integer", ex);
            }
        }
    }

    /**
     * the initial term id at which a publication will start.
     *
     * @return the initial term id at which a publication will start.
     * @see CommonContext#INITIAL_TERM_ID_PARAM_NAME
     */
    public Integer initialTermId()
    {
        return initialTermId;
    }

    /**
     * Set the current term id at which a publication will start. This when combined with the initial term can
     * establish a starting position.
     *
     * @param termId at which a publication will start.
     * @return this for a fluent API.
     * @see CommonContext#TERM_ID_PARAM_NAME
     */
    public ChannelUriStringBuilder termId(final Integer termId)
    {
        this.termId = termId;
        return this;
    }

    /**
     * Set the termId value to be what is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#TERM_ID_PARAM_NAME
     */
    public ChannelUriStringBuilder termId(final ChannelUri channelUri)
    {
        final String termIdValue = channelUri.get(TERM_ID_PARAM_NAME);
        if (null == termIdValue)
        {
            termId = null;
            return this;
        }
        else
        {
            try
            {
                return termId(Integer.valueOf(termIdValue));
            }
            catch (final NumberFormatException ex)
            {
                throw new IllegalArgumentException("'term-id' must be a valid integer", ex);
            }
        }
    }

    /**
     * Get the current term id at which a publication will start.
     *
     * @return the current term id at which a publication will start.
     * @see CommonContext#TERM_ID_PARAM_NAME
     */
    public Integer termId()
    {
        return termId;
    }

    /**
     * Set the offset within a term at which a publication will start. This when combined with the term id can establish
     * a starting position.
     *
     * @param termOffset within a term at which a publication will start.
     * @return this for a fluent API.
     * @see CommonContext#TERM_OFFSET_PARAM_NAME
     */
    public ChannelUriStringBuilder termOffset(final Integer termOffset)
    {
        if (null != termOffset)
        {
            if ((termOffset < 0 || termOffset > TERM_MAX_LENGTH))
            {
                throw new IllegalArgumentException("term offset not in range 0-1g: " + termOffset);
            }

            if (0 != (termOffset & (FRAME_ALIGNMENT - 1)))
            {
                throw new IllegalArgumentException("term offset not multiple of FRAME_ALIGNMENT: " + termOffset);
            }
        }

        this.termOffset = termOffset;
        return this;
    }

    /**
     * Set the termOffset value to be what is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#TERM_OFFSET_PARAM_NAME
     */
    public ChannelUriStringBuilder termOffset(final ChannelUri channelUri)
    {
        final String termOffsetValue = channelUri.get(TERM_OFFSET_PARAM_NAME);
        if (null == termOffsetValue)
        {
            termOffset = null;
            return this;
        }
        else
        {
            try
            {
                return termOffset(Integer.valueOf(termOffsetValue));
            }
            catch (final NumberFormatException ex)
            {
                throw new IllegalArgumentException("'term-offset' must be a valid integer", ex);
            }
        }
    }

    /**
     * Get the offset within a term at which a publication will start.
     *
     * @return the offset within a term at which a publication will start.
     * @see CommonContext#TERM_OFFSET_PARAM_NAME
     */
    public Integer termOffset()
    {
        return termOffset;
    }

    /**
     * Set the session id for a publication or restricted subscription.
     *
     * @param sessionId for the publication or a restricted subscription.
     * @return this for a fluent API.
     * @see CommonContext#SESSION_ID_PARAM_NAME
     */
    public ChannelUriStringBuilder sessionId(final Integer sessionId)
    {
        this.sessionId = null != sessionId ? sessionId.longValue() : null;
        return this;
    }

    /**
     * Set the session id for a publication or restricted subscription from a formatted string.  Supports a format of
     * either a string encoded signed 32-bit number or 'tag:' followed by a signed 64 bit value.
     *
     * @param sessionIdStr for the publication or a restricted subscription.
     * @return this for a fluent API.
     * @see CommonContext#SESSION_ID_PARAM_NAME
     */
    public ChannelUriStringBuilder sessionId(final String sessionIdStr)
    {
        if (null != sessionIdStr)
        {
            if (ChannelUri.isTagged(sessionIdStr))
            {
                taggedSessionId(ChannelUri.getTag(sessionIdStr));
            }
            else
            {
                isSessionIdTagged(false);
                try
                {
                    sessionId(Integer.valueOf(sessionIdStr));
                }
                catch (final NumberFormatException ex)
                {
                    throw new IllegalArgumentException("'session-id' must be a valid integer", ex);
                }
            }
        }
        else
        {
            sessionId((Integer)null);
        }

        return this;
    }

    /**
     * Set the session id for a publication or restricted subscription as a tag referenced value.
     *
     * @param sessionId for the publication or a restricted subscription.
     * @return this for a fluent API.
     * @see CommonContext#SESSION_ID_PARAM_NAME
     */
    public ChannelUriStringBuilder taggedSessionId(final Long sessionId)
    {
        isSessionIdTagged(true);
        this.sessionId = sessionId;
        return this;
    }

    /**
     * Set the sessionId value to be what is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#SESSION_ID_PARAM_NAME
     */
    public ChannelUriStringBuilder sessionId(final ChannelUri channelUri)
    {
        return sessionId(channelUri.get(SESSION_ID_PARAM_NAME));
    }

    /**
     * Get the session id for a publication or restricted subscription.
     *
     * @return the session id for a publication or restricted subscription.
     * @see CommonContext#SESSION_ID_PARAM_NAME
     * @deprecated this method will not correctly handle tagged sessionId values that are outside the range of
     * a signed 32-bit number.  If this is called and a tagged value outside this range is currently held in this
     * object, then the result will be the same as {@link Long#intValue()}.
     */
    @Deprecated
    public Integer sessionId()
    {
        return null != sessionId ? sessionId.intValue() : null;
    }

    /**
     * Set the time a network publication will linger in nanoseconds after being drained. This time is so that tail
     * loss can be recovered.
     *
     * @param lingerNs time for the publication after it is drained.
     * @return this for a fluent API.
     * @see CommonContext#LINGER_PARAM_NAME
     */
    public ChannelUriStringBuilder linger(final Long lingerNs)
    {
        if (null != lingerNs && lingerNs < 0)
        {
            throw new IllegalArgumentException("linger value cannot be negative: " + lingerNs);
        }

        this.linger = lingerNs;
        return this;
    }

    /**
     * Set the linger value to be what is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#LINGER_PARAM_NAME
     */
    public ChannelUriStringBuilder linger(final ChannelUri channelUri)
    {
        final String lingerValue = channelUri.get(LINGER_PARAM_NAME);
        if (null == lingerValue)
        {
            linger = null;
            return this;
        }
        else
        {
            return linger(parseDuration(LINGER_PARAM_NAME, lingerValue));
        }
    }

    /**
     * Get the time a network publication will linger in nanoseconds after being drained. This time is so that tail
     * loss can be recovered.
     *
     * @return the  time in nanoseconds a publication will linger after being drained.
     * @see CommonContext#LINGER_PARAM_NAME
     */
    public Long linger()
    {
        return linger;
    }

    /**
     * Set to indicate if a term log buffer should be sparse on disk or not. Sparse saves space at the potential
     * expense of latency.
     *
     * @param isSparse true if the term buffer log is sparse on disk.
     * @return this for a fluent API.
     * @see CommonContext#SPARSE_PARAM_NAME
     */
    public ChannelUriStringBuilder sparse(final Boolean isSparse)
    {
        this.sparse = isSparse;
        return this;
    }

    /**
     * Set the sparse value to be what is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#SPARSE_PARAM_NAME
     */
    public ChannelUriStringBuilder sparse(final ChannelUri channelUri)
    {
        final String sparseValue = channelUri.get(SPARSE_PARAM_NAME);
        if (null == sparseValue)
        {
            sparse = null;
            return this;
        }
        else
        {
            return sparse(Boolean.valueOf(sparseValue));
        }
    }

    /**
     * Should term log buffer be sparse on disk or not. Sparse saves space at the potential expense of latency.
     *
     * @return true if the term buffer log is sparse on disk.
     * @see CommonContext#SPARSE_PARAM_NAME
     */
    public Boolean sparse()
    {
        return sparse;
    }

    /**
     * Set to indicate if an EOS should be sent on the media or not.
     *
     * @param eos true if the EOS should be sent.
     * @return this for a fluent API.
     * @see CommonContext#EOS_PARAM_NAME
     */
    public ChannelUriStringBuilder eos(final Boolean eos)
    {
        this.eos = eos;
        return this;
    }

    /**
     * Set the eos value to be what is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#EOS_PARAM_NAME
     */
    public ChannelUriStringBuilder eos(final ChannelUri channelUri)
    {
        final String eosValue = channelUri.get(EOS_PARAM_NAME);
        if (null == eosValue)
        {
            eos = null;
            return this;
        }
        else
        {
            return eos(Boolean.valueOf(eosValue));
        }
    }

    /**
     * Should an EOS flag be sent on the media or not.
     *
     * @return true if the EOS param should be set.
     * @see CommonContext#EOS_PARAM_NAME
     */
    public Boolean eos()
    {
        return eos;
    }

    /**
     * Should the subscription channel be tethered or not for local flow control.
     *
     * @param tether value to be set for the tether param.
     * @return this for a fluent API.
     * @see CommonContext#TETHER_PARAM_NAME
     */
    public ChannelUriStringBuilder tether(final Boolean tether)
    {
        this.tether = tether;
        return this;
    }

    /**
     * Set the tether value to be what is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#TETHER_PARAM_NAME
     */
    public ChannelUriStringBuilder tether(final ChannelUri channelUri)
    {
        final String tetherValue = channelUri.get(TETHER_PARAM_NAME);
        if (null == tetherValue)
        {
            tether = null;
            return this;
        }
        else
        {
            return tether(Boolean.valueOf(tetherValue));
        }
    }

    /**
     * Should the subscription channel be tethered or not for local flow control.
     *
     * @return value of the tether param.
     * @see CommonContext#TETHER_PARAM_NAME
     */
    public Boolean tether()
    {
        return tether;
    }

    /**
     * Is the receiver likely to be part of a group. This informs behaviour such as loss handling.
     *
     * @param group value to be set for the group param.
     * @return this for a fluent API.
     * @see CommonContext#GROUP_PARAM_NAME
     * @see #controlMode()
     * @see #controlEndpoint()
     */
    public ChannelUriStringBuilder group(final Boolean group)
    {
        this.group = group;
        return this;
    }

    /**
     * Set the group value to be what is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#GROUP_PARAM_NAME
     */
    public ChannelUriStringBuilder group(final ChannelUri channelUri)
    {
        final String groupValue = channelUri.get(GROUP_PARAM_NAME);
        if (null == groupValue)
        {
            group = null;
            return this;
        }
        else
        {
            return group(Boolean.valueOf(groupValue));
        }
    }

    /**
     * Is the receiver likely to be part of a group. This informs behaviour such as loss handling.
     *
     * @return value of the group param.
     * @see CommonContext#GROUP_PARAM_NAME
     * @see #controlMode()
     * @see #controlEndpoint()
     */
    public Boolean group()
    {
        return group;
    }

    /**
     * Set the tags for a channel used by a publication or subscription. Tags can be used to identify or tag a
     * channel so that a configuration can be referenced and reused.
     *
     * @param tags for the channel, publication or subscription.
     * @return this for a fluent API.
     * @see CommonContext#TAGS_PARAM_NAME
     * @see CommonContext#TAG_PREFIX
     */
    public ChannelUriStringBuilder tags(final String tags)
    {
        this.tags = tags;
        return this;
    }

    /**
     * Set the tags to be value which is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#TAGS_PARAM_NAME
     */
    public ChannelUriStringBuilder tags(final ChannelUri channelUri)
    {
        return tags(channelUri.get(TAGS_PARAM_NAME));
    }

    /**
     * Set the tags to the specified channel and publication/subscription tag {@link ChannelUri}. The
     * publication/subscription may be null. If channel tag is null, then the pubSubTag must be null.
     *
     * @param channelTag optional value for the channel tag.
     * @param pubSubTag  option value for the publication/subscription tag.
     * @return this for a fluent API.
     * @throws IllegalArgumentException if channelTag is null and pubSubTag is not.
     * @see CommonContext#TAGS_PARAM_NAME
     */
    public ChannelUriStringBuilder tags(final Long channelTag, final Long pubSubTag)
    {
        if (null == channelTag && null != pubSubTag)
        {
            throw new IllegalArgumentException("null == channelTag && null != pubSubTag");
        }

        if (null == channelTag)
        {
            return tags((String)null);
        }

        return tags(channelTag + (null != pubSubTag ? "," + pubSubTag : ""));
    }

    /**
     * Get the tags for a channel used by a publication or subscription. Tags can be used to identify or tag a
     * channel so that a configuration can be referenced and reused.
     *
     * @return the tags for a channel, publication or subscription.
     * @see CommonContext#TAGS_PARAM_NAME
     * @see CommonContext#TAG_PREFIX
     */
    public String tags()
    {
        return tags;
    }

    /**
     * Toggle the value for {@link #sessionId()} being tagged or not.
     *
     * @param isSessionIdTagged for session id
     * @return this for a fluent API.
     * @see CommonContext#TAGS_PARAM_NAME
     * @see CommonContext#TAG_PREFIX
     */
    public ChannelUriStringBuilder isSessionIdTagged(final boolean isSessionIdTagged)
    {
        this.isSessionIdTagged = isSessionIdTagged;
        return this;
    }

    /**
     * Is the value for {@link #sessionId()} a tag.
     *
     * @return whether the value for {@link #sessionId()} a tag reference or not.
     * @see CommonContext#TAGS_PARAM_NAME
     * @see CommonContext#TAG_PREFIX
     */
    public boolean isSessionIdTagged()
    {
        return isSessionIdTagged;
    }

    /**
     * Set the alias for a URI. Aliases are not interpreted by Aeron and are to be used by the application.
     *
     * @param alias for the URI.
     * @return this for a fluent API.
     * @see CommonContext#ALIAS_PARAM_NAME
     */
    public ChannelUriStringBuilder alias(final String alias)
    {
        this.alias = alias;
        return this;
    }

    /**
     * Set the alias to be value which is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#TAGS_PARAM_NAME
     */
    public ChannelUriStringBuilder alias(final ChannelUri channelUri)
    {
        return alias(channelUri.get(ALIAS_PARAM_NAME));
    }

    /**
     * Get the alias present in the URI.
     *
     * @return alias for the URI.
     * @see CommonContext#ALIAS_PARAM_NAME
     */
    public String alias()
    {
        return alias;
    }

    /**
     * Set the congestion control algorithm to be used on a stream.
     *
     * @param congestionControl for the URI.
     * @return this for a fluent API.
     * @see CommonContext#CONGESTION_CONTROL_PARAM_NAME
     */
    public ChannelUriStringBuilder congestionControl(final String congestionControl)
    {
        this.cc = congestionControl;
        return this;
    }

    /**
     * Set the congestion control to be value which is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#CONGESTION_CONTROL_PARAM_NAME
     */
    public ChannelUriStringBuilder congestionControl(final ChannelUri channelUri)
    {
        return congestionControl(channelUri.get(CONGESTION_CONTROL_PARAM_NAME));
    }

    /**
     * Get the congestion control algorithm to be used on a stream.
     *
     * @return congestion control strategy for the channel.
     * @see CommonContext#CONGESTION_CONTROL_PARAM_NAME
     */
    public String congestionControl()
    {
        return cc;
    }

    /**
     * Set the flow control strategy to be used on a stream.
     *
     * @param flowControl for the URI.
     * @return this for a fluent API.
     * @see CommonContext#FLOW_CONTROL_PARAM_NAME
     */
    public ChannelUriStringBuilder flowControl(final String flowControl)
    {
        this.fc = flowControl;
        return this;
    }

    /**
     * Set tagged flow control settings to be used on a stream. All specified values may be null and the default
     * specified in the MediaDriver.Context will be used instead.
     *
     * @param groupTag     receiver tag for this stream.
     * @param minGroupSize group size required to allow publications for this channel to be moved to connected status.
     * @param timeout      timeout receivers, default is ns, but allows suffixing of time units (e.g. 5s).
     * @return this for fluent API.
     */
    public ChannelUriStringBuilder taggedFlowControl(
        final Long groupTag, final Integer minGroupSize, final String timeout)
    {
        String flowControlValue = "tagged";

        if (null != groupTag || null != minGroupSize)
        {
            flowControlValue += ",g:";

            if (null != groupTag)
            {
                flowControlValue += groupTag;
            }

            if (null != minGroupSize)
            {
                flowControlValue += ("/" + minGroupSize);
            }
        }

        if (null != timeout)
        {
            flowControlValue += (",t:" + timeout);
        }

        return flowControl(flowControlValue);
    }

    /**
     * Set min flow control settings to be used on stream. All specified values may be null and the default
     * specified in the MediaDriver.Context will be used instead.
     *
     * @param minGroupSize group size required to allow publications for this stream to be moved to connected status.
     * @param timeout      timeout receivers, default is ns, but allows suffixing of time units (e.g. 5s).
     * @return this for fluent API.
     */
    public ChannelUriStringBuilder minFlowControl(final Integer minGroupSize, final String timeout)
    {
        String flowControlValue = "min";

        if (null != minGroupSize)
        {
            flowControlValue += (",g:/" + minGroupSize);
        }

        if (null != timeout)
        {
            flowControlValue += (",t:" + timeout);
        }

        return flowControl(flowControlValue);
    }

    /**
     * Set the flow control to be value which is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#FLOW_CONTROL_PARAM_NAME
     */
    public ChannelUriStringBuilder flowControl(final ChannelUri channelUri)
    {
        return flowControl(channelUri.get(FLOW_CONTROL_PARAM_NAME));
    }

    /**
     * Get the flow control strategy to be used on a stream.
     *
     * @return flow control strategy for the stream.
     * @see CommonContext#FLOW_CONTROL_PARAM_NAME
     */
    public String flowControl()
    {
        return fc;
    }

    /**
     * Set the group tag (gtag) to be sent in SMs (Status Messages).
     *
     * @param groupTag to be sent in SMs
     * @return this for fluent API.
     * @see CommonContext#GROUP_TAG_PARAM_NAME
     */
    public ChannelUriStringBuilder groupTag(final Long groupTag)
    {
        this.groupTag = groupTag;
        return this;
    }

    /**
     * Set the group tag (gtag) to be the value which is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#GROUP_TAG_PARAM_NAME
     */
    public ChannelUriStringBuilder groupTag(final ChannelUri channelUri)
    {
        final String groupTagValue = channelUri.get(GROUP_TAG_PARAM_NAME);
        if (null == groupTagValue)
        {
            groupTag = null;
            return this;
        }
        else
        {
            try
            {
                return groupTag(Long.valueOf(groupTagValue));
            }
            catch (final NumberFormatException ex)
            {
                throw new IllegalArgumentException("'gtag' must be a valid long value", ex);
            }
        }
    }

    /**
     * Get the group tag (gtag) to be sent in SMs (Status Messages).
     *
     * @return receiver tag to be sent in SMs.
     * @see CommonContext#GROUP_TAG_PARAM_NAME
     */
    public Long groupTag()
    {
        return groupTag;
    }

    /**
     * Set the subscription semantics for if a stream should be rejoined after going unavailable.
     *
     * @param rejoin false if stream is not to be rejoined.
     * @return this for a fluent API.
     * @see CommonContext#REJOIN_PARAM_NAME
     */
    public ChannelUriStringBuilder rejoin(final Boolean rejoin)
    {
        this.rejoin = rejoin;
        return this;
    }

    /**
     * Set the rejoin value to be what is in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#REJOIN_PARAM_NAME
     */
    public ChannelUriStringBuilder rejoin(final ChannelUri channelUri)
    {
        final String rejoinValue = channelUri.get(REJOIN_PARAM_NAME);
        if (null == rejoinValue)
        {
            rejoin = null;
            return this;
        }
        else
        {
            return rejoin(Boolean.valueOf(rejoinValue));
        }
    }

    /**
     * Get the subscription semantics for if a stream should be rejoined after going unavailable.
     *
     * @return the subscription semantics for if a stream should be rejoined after going unavailable.
     * @see CommonContext#REJOIN_PARAM_NAME
     */
    public Boolean rejoin()
    {
        return rejoin;
    }

    /**
     * Set the publication semantics for whether the presence of spy subscriptions simulate a connection.
     *
     * @param spiesSimulateConnection true if the presence of spy subscriptions simulate a connection.
     * @return this for a fluent API.
     * @see CommonContext#SPIES_SIMULATE_CONNECTION_PARAM_NAME
     */
    public ChannelUriStringBuilder spiesSimulateConnection(final Boolean spiesSimulateConnection)
    {
        this.ssc = spiesSimulateConnection;
        return this;
    }

    /**
     * Set the publication semantics for whether the presence of spy subscriptions simulate a connection to be what is
     * in the {@link ChannelUri} which may be null.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#SPIES_SIMULATE_CONNECTION_PARAM_NAME
     */
    public ChannelUriStringBuilder spiesSimulateConnection(final ChannelUri channelUri)
    {
        final String sscValue = channelUri.get(SPIES_SIMULATE_CONNECTION_PARAM_NAME);
        if (null == sscValue)
        {
            ssc = null;
            return this;
        }
        else
        {
            return spiesSimulateConnection(Boolean.valueOf(sscValue));
        }
    }

    /**
     * Get the publication semantics for whether the presence of spy subscriptions simulate a connection.
     *
     * @return true if the presence of spy subscriptions simulate a connection, otherwise false.
     * @see CommonContext#SPIES_SIMULATE_CONNECTION_PARAM_NAME
     */
    public Boolean spiesSimulateConnection()
    {
        return ssc;
    }

    /**
     * Initialise a channel for restarting a publication at a given position.
     *
     * @param position      at which the publication should be started.
     * @param initialTermId what which the stream would start.
     * @param termLength    for the stream.
     * @return this for a fluent API.
     */
    public ChannelUriStringBuilder initialPosition(final long position, final int initialTermId, final int termLength)
    {
        if (position < 0)
        {
            throw new IllegalArgumentException("invalid position=" + position + " < 0");
        }
        if (0 != (position & (FRAME_ALIGNMENT - 1)))
        {
            throw new IllegalArgumentException(
                "invalid position=" + position + " does not have frame alignment=" + FRAME_ALIGNMENT);
        }

        final int bitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);

        this.initialTermId = initialTermId;
        this.termId = LogBufferDescriptor.computeTermIdFromPosition(position, bitsToShift, initialTermId);
        this.termOffset = (int)(position & (termLength - 1));
        this.termLength = termLength;

        return this;
    }

    /**
     * Set the underlying OS send buffer length.
     *
     * @param socketSndbufLength parameter to be passed as SO_SNDBUF value.
     * @return this for a fluent API.
     * @see CommonContext#SOCKET_SNDBUF_PARAM_NAME
     */
    public ChannelUriStringBuilder socketSndbufLength(final Integer socketSndbufLength)
    {
        this.socketSndbufLength = socketSndbufLength;
        return this;
    }

    /**
     * Set the underlying OS send buffer length from an existing {@link ChannelUri} which may be (null).
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#SOCKET_SNDBUF_PARAM_NAME
     */
    public ChannelUriStringBuilder socketSndbufLength(final ChannelUri channelUri)
    {
        final String valueStr = channelUri.get(SOCKET_SNDBUF_PARAM_NAME);
        if (null == valueStr)
        {
            this.socketSndbufLength = null;
            return this;
        }
        else
        {
            final long value = parseSize(SOCKET_SNDBUF_PARAM_NAME, valueStr);
            if (value > Integer.MAX_VALUE)
            {
                throw new IllegalArgumentException("value exceeds maximum permitted: value=" + value);
            }

            return socketSndbufLength((int)value);
        }
    }

    /**
     * Get the underlying OS send buffer length setting.
     *
     * @return underlying OS send buffer length setting or null if not specified.
     * @see CommonContext#SOCKET_SNDBUF_PARAM_NAME
     */
    public Integer socketSndbufLength()
    {
        return socketSndbufLength;
    }

    /**
     * Set the underlying OS receive buffer length.
     *
     * @param socketRcvbufLength parameter to be passed as SO_RCVBUF value.
     * @return this for a fluent API.
     * @see CommonContext#SOCKET_RCVBUF_PARAM_NAME
     */
    public ChannelUriStringBuilder socketRcvbufLength(final Integer socketRcvbufLength)
    {
        this.socketRcvbufLength = socketRcvbufLength;
        return this;
    }

    /**
     * Set the underlying OS receive buffer length from an existing {@link ChannelUri}, which may have a null value for
     * this field.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#SOCKET_RCVBUF_PARAM_NAME
     */
    public ChannelUriStringBuilder socketRcvbufLength(final ChannelUri channelUri)
    {
        final String valueStr = channelUri.get(SOCKET_RCVBUF_PARAM_NAME);
        if (null == valueStr)
        {
            this.socketRcvbufLength = null;
            return this;
        }
        else
        {
            final long value = parseSize(SOCKET_RCVBUF_PARAM_NAME, valueStr);
            if (value > Integer.MAX_VALUE)
            {
                throw new IllegalArgumentException("value exceeds maximum permitted: value=" + value);
            }

            return socketRcvbufLength((int)value);
        }
    }

    /**
     * Get the underlying OS receive buffer length setting.
     *
     * @return underlying OS receive buffer length setting or null if not specified.
     * @see CommonContext#SOCKET_RCVBUF_PARAM_NAME
     */
    public Integer socketRcvbufLength()
    {
        return socketRcvbufLength;
    }

    /**
     * Set the flow control initial receiver window length for this channel.
     *
     * @param receiverWindowLength initial receiver window length.
     * @return this for a fluent API.
     * @see CommonContext#RECEIVER_WINDOW_LENGTH_PARAM_NAME
     */
    public ChannelUriStringBuilder receiverWindowLength(final Integer receiverWindowLength)
    {
        this.receiverWindowLength = receiverWindowLength;
        return this;
    }

    /**
     * Set the flow control initial receiver window length for this channel from an existing {@link ChannelUri},
     * which may have a null value for this field.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#RECEIVER_WINDOW_LENGTH_PARAM_NAME
     */
    public ChannelUriStringBuilder receiverWindowLength(final ChannelUri channelUri)
    {
        final String valueStr = channelUri.get(RECEIVER_WINDOW_LENGTH_PARAM_NAME);
        if (null == valueStr)
        {
            this.receiverWindowLength = null;
            return this;
        }
        else
        {
            final long value = parseSize(RECEIVER_WINDOW_LENGTH_PARAM_NAME, valueStr);
            if (value > Integer.MAX_VALUE)
            {
                throw new IllegalArgumentException("value exceeds maximum permitted: value=" + value);
            }

            return receiverWindowLength((int)value);
        }
    }

    /**
     * Get the receiver window length to be used as the initial receiver window for flow control.
     *
     * @return receiver window length.
     * @see CommonContext#RECEIVER_WINDOW_LENGTH_PARAM_NAME
     */
    public Integer receiverWindowLength()
    {
        return receiverWindowLength;
    }

    /**
     * Offset into a message to store the media receive timestamp. May also be the special value 'reserved' which means
     * to store the timestamp in the reserved value field.
     *
     * @return current mediaReceiveTimestampOffset value either as string representation of an integer index or the
     * special value 'reserved'
     * @see CommonContext#MEDIA_RCV_TIMESTAMP_OFFSET_PARAM_NAME
     */
    public String mediaReceiveTimestampOffset()
    {
        return mediaReceiveTimestampOffset;
    }

    /**
     * Offset into a message to store the media receive timestamp. May also be the special value 'reserved' which means
     * to store the timestamp in the reserved value field.
     *
     * @param timestampOffset to use as the offset.
     * @return this for a fluent API.
     * @throws IllegalArgumentException if the string is not null and doesn't represent an int or the 'reserved' value.
     * @see CommonContext#MEDIA_RCV_TIMESTAMP_OFFSET_PARAM_NAME
     */
    public ChannelUriStringBuilder mediaReceiveTimestampOffset(final String timestampOffset)
    {
        if (null != timestampOffset && !RESERVED_OFFSET.equals(timestampOffset))
        {
            try
            {
                Integer.parseInt(timestampOffset);
            }
            catch (final NumberFormatException ex)
            {
                throw new IllegalArgumentException(
                    "mediaReceiveTimestampOffset must be a number or the value '" + RESERVED_OFFSET + "' found: " +
                    timestampOffset);
            }
        }

        this.mediaReceiveTimestampOffset = timestampOffset;
        return this;
    }

    /**
     * Offset into a message to store the media receive timestamp. May also be the special value 'reserved' which means
     * to store the timestamp in the reserved value field.
     *
     * @param channelUri the existing URI to extract the mediaReceiveTimestampOffset from
     * @return this for a fluent API.
     * @see CommonContext#MEDIA_RCV_TIMESTAMP_OFFSET_PARAM_NAME
     */
    public ChannelUriStringBuilder mediaReceiveTimestampOffset(final ChannelUri channelUri)
    {
        return mediaReceiveTimestampOffset(channelUri.get(MEDIA_RCV_TIMESTAMP_OFFSET_PARAM_NAME));
    }

    /**
     * Offset into a message to store the channel receive timestamp. May also be the special value 'reserved' which
     * means to store the timestamp in the reserved value field.
     *
     * @return current channelReceiveTimestampOffset value either as string representation of an integer index or
     * the special value 'reserved'
     * @see CommonContext#CHANNEL_RECEIVE_TIMESTAMP_OFFSET_PARAM_NAME
     */
    public String channelReceiveTimestampOffset()
    {
        return channelReceiveTimestampOffset;
    }

    /**
     * Offset into a message to store the channel receive timestamp. May also be the special value 'reserved' which
     * means to store the timestamp in the reserved value field.
     *
     * @param timestampOffset to use as the offset.
     * @return this for a fluent API.
     * @throws IllegalArgumentException if the string doesn't represent an int or the 'reserved' value.
     * @see CommonContext#CHANNEL_RECEIVE_TIMESTAMP_OFFSET_PARAM_NAME
     */
    public ChannelUriStringBuilder channelReceiveTimestampOffset(final String timestampOffset)
    {
        if (null != timestampOffset && !RESERVED_OFFSET.equals(timestampOffset))
        {
            try
            {
                Integer.parseInt(timestampOffset);
            }
            catch (final NumberFormatException ex)
            {
                throw new IllegalArgumentException(
                    "channelReceiveTimestampOffset must be a number or the value '" + RESERVED_OFFSET + "' found: " +
                    timestampOffset);
            }
        }

        this.channelReceiveTimestampOffset = timestampOffset;
        return this;
    }

    /**
     * Offset into a message to store the channel receive timestamp. May also be the special value 'reserved' which
     * means to store the timestamp in the reserved value field.
     *
     * @param channelUri the existing URI to extract the receiveTimestampOffset from.
     * @return this for a fluent API.
     * @see CommonContext#CHANNEL_RECEIVE_TIMESTAMP_OFFSET_PARAM_NAME
     */
    public ChannelUriStringBuilder channelReceiveTimestampOffset(final ChannelUri channelUri)
    {
        return channelReceiveTimestampOffset(channelUri.get(CHANNEL_RECEIVE_TIMESTAMP_OFFSET_PARAM_NAME));
    }

    /**
     * Offset into a message to store the channel send timestamp. May also be the special value 'reserved' which means
     * to store the timestamp in the reserved value field.
     *
     * @param timestampOffset to use as the offset.
     * @return this for a fluent API.
     * @throws IllegalArgumentException if the string is not null doesn't represent an int or the 'reserved' value.
     * @see CommonContext#CHANNEL_SEND_TIMESTAMP_OFFSET_PARAM_NAME
     */
    public ChannelUriStringBuilder channelSendTimestampOffset(final String timestampOffset)
    {
        if (null != timestampOffset && !RESERVED_OFFSET.equals(timestampOffset))
        {
            try
            {
                Integer.parseInt(timestampOffset);
            }
            catch (final NumberFormatException ex)
            {
                throw new IllegalArgumentException(
                    "channelSendTimestampOffset must be a number or the value '" + RESERVED_OFFSET + "' found: " +
                        timestampOffset);
            }
        }

        this.channelSendTimestampOffset = timestampOffset;
        return this;
    }

    /**
     * Offset into a message to store the channel send timestamp. May also be the special value 'reserved' which means
     * to store the timestamp in the reserved value field.
     *
     * @param channelUri the existing URI to extract the channelSendTimestampOffset from.
     * @return this for a fluent API.
     * @see CommonContext#CHANNEL_SEND_TIMESTAMP_OFFSET_PARAM_NAME
     */
    public ChannelUriStringBuilder channelSendTimestampOffset(final ChannelUri channelUri)
    {
        return channelSendTimestampOffset(channelUri.get(CHANNEL_SEND_TIMESTAMP_OFFSET_PARAM_NAME));
    }

    /**
     * Offset into a message to store the channel send timestamp. May also be the special value 'reserved' which means
     * to store the timestamp in the reserved value field.
     *
     * @return current sendTimestampOffset value either as string representation of an integer index or the special
     * value 'reserved'.
     * @see CommonContext#CHANNEL_SEND_TIMESTAMP_OFFSET_PARAM_NAME
     */
    public String channelSendTimestampOffset()
    {
        return channelSendTimestampOffset;
    }

    /**
     * Set the response endpoint to be used for a response channel subscription or publication.
     *
     * @param responseEndpoint  response endpoint to be used in this channel URI.
     * @return this for a fluent API.
     * @see CommonContext#RESPONSE_ENDPOINT_PARAM_NAME
     */
    public ChannelUriStringBuilder responseEndpoint(final String responseEndpoint)
    {
        this.responseEndpoint = responseEndpoint;
        return this;
    }

    /**
     * Set the response endpoint to be used for a response channel subscription or publication by extracting it from the
     * ChannelUri.
     *
     * @param channelUri the existing URI to extract the responseEndpoint from.
     * @return this for a fluent API.
     * @see CommonContext#RESPONSE_ENDPOINT_PARAM_NAME
     */
    public ChannelUriStringBuilder responseEndpoint(final ChannelUri channelUri)
    {
        return responseEndpoint(channelUri.get(RESPONSE_ENDPOINT_PARAM_NAME));
    }

    /**
     * The response endpoint to be used for a response channel subscription or publication.
     *
     * @return response endpoint.
     * @see CommonContext#RESPONSE_ENDPOINT_PARAM_NAME
     */
    public String responseEndpoint()
    {
        return this.responseEndpoint;
    }

    /**
     * Set the correlation id from the image received on the response "server's" subscription to be used by a response
     * publication.
     *
     * @param responseCorrelationId correlation id of an image from the response "server's" subscription.
     * @return this for a fluent API.
     * @see CommonContext#RESPONSE_CORRELATION_ID_PARAM_NAME
     */
    public ChannelUriStringBuilder responseCorrelationId(final Long responseCorrelationId)
    {
        this.responseCorrelationId = responseCorrelationId;
        return this;
    }

    /**
     * Set the correlation id from the image received on the response "server's" subscription to be used by a response
     * publication extracted from the channelUri.
     *
     * @param channelUri the existing URI to extract the responseCorrelationId from.
     * @return this for a fluent API.
     * @see CommonContext#RESPONSE_CORRELATION_ID_PARAM_NAME
     */
    public ChannelUriStringBuilder responseCorrelationId(final ChannelUri channelUri)
    {
        final String responseCorrelationIdString = channelUri.get(RESPONSE_CORRELATION_ID_PARAM_NAME);

        if (null != responseCorrelationIdString)
        {
            try
            {
                responseCorrelationId(Long.valueOf(responseCorrelationIdString));
            }
            catch (final NumberFormatException ex)
            {
                throw new IllegalArgumentException("'response-correlation-id' must be a valid long value", ex);
            }
        }

        return this;
    }

    /**
     * The delay to apply before sending a NAK in response to a gap being detected by the receiver.
     *
     * @param nakDelay express as a numeric value with a suffix, e.g. 10ms, 100us.
     * @return this for a fluent API.
     * @see CommonContext#NAK_DELAY_PARAM_NAME
     */
    public ChannelUriStringBuilder nakDelay(final String nakDelay)
    {
        this.nakDelay = null != nakDelay ? parseDuration(NAK_DELAY_PARAM_NAME, nakDelay) : null;
        return this;
    }

    /**
     * The delay to apply before sending a NAK in response to a gap being detected by the receiver.
     *
     * @param channelUri the existing URI to extract the nakDelay from.
     * @return this for a fluent API.
     * @see CommonContext#NAK_DELAY_PARAM_NAME
     */
    public ChannelUriStringBuilder nakDelay(final ChannelUri channelUri)
    {
        return nakDelay(channelUri.get(NAK_DELAY_PARAM_NAME));
    }

    /**
     * The delay to apply before sending a NAK in response to a gap being detected by the receiver.
     *
     * @return the delay in nanoseconds, null if not set.
     * @see CommonContext#NAK_DELAY_PARAM_NAME
     */
    public Long nakDelay()
    {
        return nakDelay;
    }

    /**
     * The timeout for when an untethered subscription that is outside the window limit will participate in local
     * flow control.
     *
     * @param timeout specified either in nanoseconds or using a units suffix, e.g. 1ms, 1us.
     * @return this for a fluent API.
     * @see CommonContext#UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME
     */
    public ChannelUriStringBuilder untetheredWindowLimitTimeout(final String timeout)
    {
        this.untetheredWindowLimitTimeoutNs = null != timeout ?
            parseDuration(UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME, timeout) : null;
        return this;
    }

    /**
     * The timeout for when an untethered subscription that is outside the window limit will participate in local
     * flow control.
     *
     * @param timeout specified either in nanoseconds.
     * @return this for a fluent API.
     * @see CommonContext#UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME
     */
    public ChannelUriStringBuilder untetheredWindowLimitTimeoutNs(final Long timeout)
    {
        this.untetheredWindowLimitTimeoutNs = timeout;
        return this;
    }

    /**
     * The timeout for when an untethered subscription that is outside the window limit will participate in local
     * flow control.
     *
     * @param channelUri the existing URI to extract the untetheredWindowLimitTimeout from.
     * @return this for a fluent API.
     * @see CommonContext#UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME
     */
    public ChannelUriStringBuilder untetheredWindowLimitTimeout(final ChannelUri channelUri)
    {
        this.untetheredWindowLimitTimeout(channelUri.get(UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME));
        return this;
    }

    /**
     * The timeout for when an untethered subscription that is outside the window limit will participate in local
     * flow control.
     *
     * @return the timeout in ns.
     * @see CommonContext#UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME
     */
    public Long untetheredWindowLimitTimeoutNs()
    {
        return untetheredWindowLimitTimeoutNs;
    }

    /**
     * The time for an untethered subscription to linger.
     *
     * @param timeout specified either in nanoseconds or using a units suffix, e.g. 1ms, 1us.
     * @return this for a fluent API.
     * @see CommonContext#UNTETHERED_LINGER_TIMEOUT_PARAM_NAME
     */
    public ChannelUriStringBuilder untetheredLingerTimeout(final String timeout)
    {
        this.untetheredLingerTimeoutNs = null != timeout ?
            parseDuration(UNTETHERED_LINGER_TIMEOUT_PARAM_NAME, timeout) : null;
        return this;
    }

    /**
     * The time for an untethered subscription to linger.
     *
     * @param timeout specified either in nanoseconds.
     * @return this for a fluent API.
     * @see CommonContext#UNTETHERED_LINGER_TIMEOUT_PARAM_NAME
     */
    public ChannelUriStringBuilder untetheredLingerTimeoutNs(final Long timeout)
    {
        this.untetheredLingerTimeoutNs = timeout;
        return this;
    }

    /**
     * The time for an untethered subscription to linger.
     *
     * @param channelUri the existing URI to extract the untetheredLingerTimeout from.
     * @return this for a fluent API.
     * @see CommonContext#UNTETHERED_LINGER_TIMEOUT_PARAM_NAME
     */
    public ChannelUriStringBuilder untetheredLingerTimeout(final ChannelUri channelUri)
    {
        this.untetheredLingerTimeout(channelUri.get(UNTETHERED_LINGER_TIMEOUT_PARAM_NAME));
        return this;
    }

    /**
     * The time for an untethered subscription to linger.
     *
     * @return the timeout in ns.
     * @see CommonContext#UNTETHERED_LINGER_TIMEOUT_PARAM_NAME
     */
    public Long untetheredLingerTimeoutNs()
    {
        return untetheredLingerTimeoutNs;
    }

    /**
     * The timeout for when an untethered subscription is resting after not being able to keep up before it is allowed
     * to rejoin a stream.
     *
     * @param timeout specified either in nanoseconds or using a units suffix, e.g. 1ms, 1us.
     * @return this for a fluent API.
     * @see CommonContext#UNTETHERED_RESTING_TIMEOUT_PARAM_NAME
     */
    public ChannelUriStringBuilder untetheredRestingTimeout(final String timeout)
    {
        this.untetheredRestingTimeoutNs = null != timeout ?
            parseDuration(UNTETHERED_RESTING_TIMEOUT_PARAM_NAME, timeout) : null;
        return this;
    }

    /**
     * The timeout for when an untethered subscription is resting after not being able to keep up before it is allowed
     * to rejoin a stream.
     *
     * @param timeout specified either in nanoseconds.
     * @return this for a fluent API.
     * @see CommonContext#UNTETHERED_RESTING_TIMEOUT_PARAM_NAME
     */
    public ChannelUriStringBuilder untetheredRestingTimeoutNs(final Long timeout)
    {
        this.untetheredRestingTimeoutNs = timeout;
        return this;
    }

    /**
     * The timeout for when an untethered subscription is resting after not being able to keep up before it is allowed
     * to rejoin a stream.
     *
     * @param channelUri the existing URI to extract the untetheredRestingTimeout from.
     * @return this for a fluent API.
     * @see CommonContext#UNTETHERED_RESTING_TIMEOUT_PARAM_NAME
     */
    public ChannelUriStringBuilder untetheredRestingTimeout(final ChannelUri channelUri)
    {
        this.untetheredRestingTimeout(channelUri.get(UNTETHERED_RESTING_TIMEOUT_PARAM_NAME));
        return this;
    }

    /**
     * The timeout for when an untethered subscription is resting after not being able to keep up before it is allowed
     * to rejoin a stream.
     *
     * @return the timeout in ns.
     * @see CommonContext#UNTETHERED_RESTING_TIMEOUT_PARAM_NAME
     */
    public Long untetheredRestingTimeoutNs()
    {
        return untetheredRestingTimeoutNs;
    }

    /**
     * The max number of retransmit actions.
     *
     * @param maxResend the max number of retransmit actions.
     * @return this for a fluent API.
     * @see CommonContext#MAX_RESEND_PARAM_NAME
     */
    public ChannelUriStringBuilder maxResend(final Integer maxResend)
    {
        this.maxResend = maxResend;
        return this;
    }

    /**
     * The max number of retransmit actions.
     *
     * @param channelUri the existing URI to extract the maxResend from.
     * @return this for a fluent API.
     * @see CommonContext#MAX_RESEND_PARAM_NAME
     */
    public ChannelUriStringBuilder maxResend(final ChannelUri channelUri)
    {
        final String valueStr = channelUri.get(MAX_RESEND_PARAM_NAME);
        if (null == valueStr)
        {
            this.maxResend = null;
            return this;
        }
        else
        {
            try
            {
                return maxResend(Integer.parseInt(valueStr));
            }
            catch (final NumberFormatException ex)
            {
                throw new IllegalArgumentException(
                    MAX_RESEND_PARAM_NAME + " must be a number", ex);
            }
        }
    }

    /**
     * The max number of retransmit actions.
     *
     * @return the max number of outstanding retransmit actions
     * @see CommonContext#MAX_RESEND_PARAM_NAME
     */
    public Integer maxResend()
    {
        return maxResend;
    }

    /**
     * The stream id of the channel.
     *
     * @return the stream or null of no streamId is set.
     */
    public Integer streamId()
    {
        return streamId;
    }

    /**
     * The stream id of the channel.
     *
     * @param streamId of the channel.
     * @return this for a fluent API.
     */
    public ChannelUriStringBuilder streamId(final Integer streamId)
    {
        this.streamId = streamId;
        return this;
    }

    /**
     * The stream id of the channel.
     *
     * @param channelUri the existing URI to extract the streamId from.
     * @return this for a fluent API.
     */
    public ChannelUriStringBuilder streamId(final ChannelUri channelUri)
    {
        final String valueStr = channelUri.get(STREAM_ID_PARAM_NAME);
        if (null == valueStr)
        {
            this.streamId = null;
            return this;
        }
        else
        {
            try
            {
                return streamId(Integer.parseInt(valueStr));
            }
            catch (final NumberFormatException ex)
            {
                throw new IllegalArgumentException(
                    STREAM_ID_PARAM_NAME + " must be a number", ex);
            }
        }
    }

    /**
     * Set the publication window length which defines how far ahead can publication accept offers.
     *
     * @param publicationWindowLength of the channel.
     * @return this for a fluent API.
     * @see CommonContext#PUBLICATION_WINDOW_LENGTH_PARAM_NAME
     */
    public ChannelUriStringBuilder publicationWindowLength(final Integer publicationWindowLength)
    {
        this.publicationWindowLength = publicationWindowLength;
        return this;
    }

    /**
     * Set the publication window length for this channel from an existing {@link ChannelUri},
     * which may have a null value for this field.
     *
     * @param channelUri to read the value from.
     * @return this for a fluent API.
     * @see CommonContext#PUBLICATION_WINDOW_LENGTH_PARAM_NAME
     */
    public ChannelUriStringBuilder publicationWindowLength(final ChannelUri channelUri)
    {
        final String valueStr = channelUri.get(PUBLICATION_WINDOW_LENGTH_PARAM_NAME);
        if (null == valueStr)
        {
            this.publicationWindowLength = null;
            return this;
        }
        else
        {
            final long value = parseSize(PUBLICATION_WINDOW_LENGTH_PARAM_NAME, valueStr);
            if (value > Integer.MAX_VALUE)
            {
                throw new IllegalArgumentException("value exceeds maximum permitted: value=" + value);
            }

            return publicationWindowLength((int)value);
        }
    }

    /**
     * Get the publication window length.
     *
     * @return publication window length or {@code null} if was not set.
     * @see CommonContext#PUBLICATION_WINDOW_LENGTH_PARAM_NAME
     */
    public Integer publicationWindowLength()
    {
        return publicationWindowLength;
    }

    /**
     * Build a channel URI String for the given parameters.
     *
     * @return a channel URI String for the given parameters.
     */
    @SuppressWarnings({ "MethodLength", "DuplicatedCode" })
    public String build()
    {
        sb.setLength(0);

        if (null != prefix && !prefix.isEmpty())
        {
            sb.append(prefix).append(':');
        }

        sb.append(ChannelUri.AERON_SCHEME).append(':').append(media).append('?');

        appendParameter(sb, TAGS_PARAM_NAME, tags);
        appendParameter(sb, ENDPOINT_PARAM_NAME, endpoint);
        appendParameter(sb, INTERFACE_PARAM_NAME, networkInterface);
        appendParameter(sb, MDC_CONTROL_PARAM_NAME, controlEndpoint);
        appendParameter(sb, MDC_CONTROL_MODE_PARAM_NAME, controlMode);
        appendParameter(sb, MTU_LENGTH_PARAM_NAME, mtu);
        appendParameter(sb, TERM_LENGTH_PARAM_NAME, termLength);
        appendParameter(sb, INITIAL_TERM_ID_PARAM_NAME, initialTermId);
        appendParameter(sb, TERM_ID_PARAM_NAME, termId);
        appendParameter(sb, TERM_OFFSET_PARAM_NAME, termOffset);

        if (null != sessionId)
        {
            appendParameter(sb, SESSION_ID_PARAM_NAME, prefixTag(isSessionIdTagged, sessionId));
        }

        appendParameter(sb, TTL_PARAM_NAME, ttl);
        appendParameter(sb, RELIABLE_STREAM_PARAM_NAME, reliable);
        appendParameter(sb, LINGER_PARAM_NAME, linger);
        appendParameter(sb, ALIAS_PARAM_NAME, alias);
        appendParameter(sb, CONGESTION_CONTROL_PARAM_NAME, cc);
        appendParameter(sb, FLOW_CONTROL_PARAM_NAME, fc);
        appendParameter(sb, GROUP_TAG_PARAM_NAME, groupTag);
        appendParameter(sb, SPARSE_PARAM_NAME, sparse);
        appendParameter(sb, EOS_PARAM_NAME, eos);
        appendParameter(sb, TETHER_PARAM_NAME, tether);
        appendParameter(sb, GROUP_PARAM_NAME, group);
        appendParameter(sb, REJOIN_PARAM_NAME, rejoin);
        appendParameter(sb, SPIES_SIMULATE_CONNECTION_PARAM_NAME, ssc);
        appendParameter(sb, SOCKET_SNDBUF_PARAM_NAME, socketSndbufLength);
        appendParameter(sb, SOCKET_RCVBUF_PARAM_NAME, socketRcvbufLength);
        appendParameter(sb, RECEIVER_WINDOW_LENGTH_PARAM_NAME, receiverWindowLength);
        appendParameter(sb, MEDIA_RCV_TIMESTAMP_OFFSET_PARAM_NAME, mediaReceiveTimestampOffset);
        appendParameter(sb, CHANNEL_RECEIVE_TIMESTAMP_OFFSET_PARAM_NAME, channelReceiveTimestampOffset);
        appendParameter(sb, CHANNEL_SEND_TIMESTAMP_OFFSET_PARAM_NAME, channelSendTimestampOffset);
        appendParameter(sb, RESPONSE_ENDPOINT_PARAM_NAME, responseEndpoint);
        appendParameter(sb, RESPONSE_CORRELATION_ID_PARAM_NAME, responseCorrelationId);
        appendParameter(sb, NAK_DELAY_PARAM_NAME, nakDelay);
        appendParameter(sb, UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME, untetheredWindowLimitTimeoutNs);
        appendParameter(sb, UNTETHERED_RESTING_TIMEOUT_PARAM_NAME, untetheredRestingTimeoutNs);
        appendParameter(sb, MAX_RESEND_PARAM_NAME, maxResend);
        appendParameter(sb, STREAM_ID_PARAM_NAME, streamId);
        appendParameter(sb, PUBLICATION_WINDOW_LENGTH_PARAM_NAME, publicationWindowLength);

        final char lastChar = sb.charAt(sb.length() - 1);
        if (lastChar == '|' || lastChar == '?')
        {
            sb.setLength(sb.length() - 1);
        }

        return sb.toString();
    }

    private static void appendParameter(final StringBuilder sb, final String paramName, final Object paramValue)
    {
        if (null != paramValue)
        {
            sb.append(paramName).append('=').append(paramValue).append('|');
        }
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return build();
    }

    private static String prefixTag(final boolean isTagged, final Long value)
    {
        return isTagged ? TAG_PREFIX + value : value.toString();
    }
}
