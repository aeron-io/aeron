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
package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.exceptions.InvalidChannelException;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.BitUtil;
import org.agrona.SystemUtil;

import static io.aeron.ChannelUri.INVALID_TAG;
import static io.aeron.CommonContext.*;

final class PublicationParams
{
    long untetheredLingerTimeoutNs;
    long lingerTimeoutNs;
    long entityTag = ChannelUri.INVALID_TAG;
    long untetheredWindowLimitTimeoutNs;
    long untetheredRestingTimeoutNs;
    long responseCorrelationId = Aeron.NULL_VALUE;
    int termLength;
    int mtuLength;
    int publicationWindowLength;
    int initialTermId;
    int termId;
    int termOffset;
    int sessionId;
    int streamId;
    int maxResend;
    boolean hasPosition;
    boolean isSessionIdTagged;
    boolean signalEos;
    boolean isSparse;
    boolean spiesSimulateConnection;
    boolean isResponse;

    static PublicationParams getPublicationParams(
        final ChannelUri channelUri,
        final MediaDriver.Context ctx,
        final DriverConductor driverConductor,
        final int streamId,
        final String canonicalForm)
    {
        final PublicationParams params = new PublicationParams();

        params.termLength = channelUri.isIpc() ? ctx.ipcTermBufferLength() : ctx.publicationTermBufferLength();
        params.mtuLength = channelUri.isIpc() ? ctx.ipcMtuLength() : ctx.mtuLength();

        params.getStreamId(channelUri, streamId);
        params.getEntityTag(channelUri, driverConductor);
        params.getSessionId(channelUri, driverConductor, streamId, canonicalForm);
        params.getTermBufferLength(channelUri);
        params.getMtuLength(channelUri);
        params.getPublicationWindowLength(channelUri, ctx);
        params.getLingerTimeoutNs(channelUri, ctx);
        params.getEos(channelUri);
        params.getSparse(channelUri, ctx);
        params.getSpiesSimulateConnection(channelUri, ctx);
        params.getUntetheredWindowLimitTimeout(channelUri, ctx);
        params.getUntetheredLingerTimeout(channelUri, ctx);
        params.getUntetheredRestingTimeout(channelUri, ctx);
        params.getMaxResend(channelUri, ctx);

        int count = 0;

        final String initialTermIdStr = channelUri.get(INITIAL_TERM_ID_PARAM_NAME);
        count = initialTermIdStr != null ? count + 1 : count;

        final String termIdStr = channelUri.get(TERM_ID_PARAM_NAME);
        count = termIdStr != null ? count + 1 : count;

        final String termOffsetStr = channelUri.get(TERM_OFFSET_PARAM_NAME);
        count = termOffsetStr != null ? count + 1 : count;

        if (count > 0)
        {
            if (count < 3)
            {
                throw new InvalidChannelException("params must be used as a complete set: " +
                    INITIAL_TERM_ID_PARAM_NAME + " " + TERM_ID_PARAM_NAME + " " + TERM_OFFSET_PARAM_NAME + " channel=" +
                    channelUri);
            }

            params.initialTermId = Integer.parseInt(initialTermIdStr);
            params.termId = Integer.parseInt(termIdStr);
            params.termOffset = Integer.parseInt(termOffsetStr);

            if (params.termOffset > params.termLength)
            {
                throw new InvalidChannelException(
                    TERM_OFFSET_PARAM_NAME + "=" + params.termOffset + " > " +
                    TERM_LENGTH_PARAM_NAME + "=" + params.termLength + ": channel=" + channelUri);
            }

            if (params.termOffset < 0 || params.termOffset > LogBufferDescriptor.TERM_MAX_LENGTH)
            {
                throw new InvalidChannelException(
                    TERM_OFFSET_PARAM_NAME + "=" + params.termOffset + " out of range: channel=" + channelUri);
            }

            if ((params.termOffset & (FrameDescriptor.FRAME_ALIGNMENT - 1)) != 0)
            {
                throw new InvalidChannelException(
                    TERM_OFFSET_PARAM_NAME + "=" + params.termOffset +
                    " must be a multiple of FRAME_ALIGNMENT: channel=" + channelUri);
            }

            if (params.termId - params.initialTermId < 0)
            {
                throw new InvalidChannelException(
                    "difference greater than 2^31 - 1: " + INITIAL_TERM_ID_PARAM_NAME + "=" +
                    params.initialTermId + " when " + TERM_ID_PARAM_NAME + "=" + params.termId + " channel=" +
                    channelUri);
            }

            params.hasPosition = true;
        }
        else
        {
            params.initialTermId = BitUtil.generateRandomisedId();
            params.termId = params.initialTermId;
            params.termOffset = 0;
        }

        params.isResponse = CONTROL_MODE_RESPONSE.equals(channelUri.get(MDC_CONTROL_MODE_PARAM_NAME));
        params.responseCorrelationId = Long.parseLong(channelUri.get(RESPONSE_CORRELATION_ID_PARAM_NAME, "-1"));

        return params;
    }

    private void getPublicationWindowLength(final ChannelUri channelUri, final MediaDriver.Context ctx)
    {
        final String pubWindowParam = channelUri.get(PUBLICATION_WINDOW_LENGTH_PARAM_NAME);
        if (null != pubWindowParam)
        {
            final long pubWindow = SystemUtil.parseSize(PUBLICATION_WINDOW_LENGTH_PARAM_NAME, pubWindowParam);
            if (pubWindow < mtuLength)
            {
                throw new InvalidChannelException(
                    PUBLICATION_WINDOW_LENGTH_PARAM_NAME + "=" + pubWindow + " cannot be less than the " +
                    MTU_LENGTH_PARAM_NAME + "=" + mtuLength);
            }

            if (pubWindow > (termLength >> 1))
            {
                throw new InvalidChannelException(
                    PUBLICATION_WINDOW_LENGTH_PARAM_NAME + "=" + pubWindow + " must not exceed half the " +
                    TERM_LENGTH_PARAM_NAME + "=" + termLength);
            }
            publicationWindowLength = (int)pubWindow;
        }
        else
        {
            publicationWindowLength = Configuration.producerWindowLength(
                termLength,
                channelUri.isIpc() ? ctx.ipcPublicationTermWindowLength() : ctx.publicationTermWindowLength());
        }
    }

    private void getStreamId(final ChannelUri channelUri, final int streamId)
    {
        final String streamIdParam = channelUri.get(STREAM_ID_PARAM_NAME);
        if (null != streamIdParam)
        {
            final int configuredStreamId;
            try
            {
                configuredStreamId = Integer.parseInt(streamIdParam);
            }
            catch (final NumberFormatException ex)
            {
                throw new InvalidChannelException("invalid " + STREAM_ID_PARAM_NAME + ", must be a number", ex);
            }

            if (streamId != configuredStreamId)
            {
                throw new InvalidChannelException(
                    STREAM_ID_PARAM_NAME + "=" + configuredStreamId + " does not match provided streamId=" + streamId);
            }
        }
        this.streamId = streamId;
    }

    private void getEntityTag(final ChannelUri channelUri, final DriverConductor driverConductor)
    {
        final String tagParam = channelUri.entityTag();
        if (null != tagParam)
        {
            this.entityTag = parseEntityTag(tagParam, driverConductor, channelUri);
        }
    }

    private void getTermBufferLength(final ChannelUri channelUri)
    {
        final String termLengthParam = channelUri.get(TERM_LENGTH_PARAM_NAME);
        if (null != termLengthParam)
        {
            final int termLength = (int)SystemUtil.parseSize(TERM_LENGTH_PARAM_NAME, termLengthParam);
            LogBufferDescriptor.checkTermLength(termLength);
            validateTermLength(this, termLength, channelUri);
            this.termLength = termLength;
        }
    }

    private void getMtuLength(final ChannelUri channelUri)
    {
        final String mtuParam = channelUri.get(MTU_LENGTH_PARAM_NAME);
        if (null != mtuParam)
        {
            final int mtuLength = (int)SystemUtil.parseSize(MTU_LENGTH_PARAM_NAME, mtuParam);
            Configuration.validateMtuLength(mtuLength);
            validateMtuLength(this, mtuLength, channelUri);
            this.mtuLength = mtuLength;
        }

        final int maxMessageLength = FrameDescriptor.computeMaxMessageLength(termLength);
        if (mtuLength > maxMessageLength)
        {
            throw new InvalidChannelException("MTU greater than max message length for term length: mtu=" +
                mtuLength + " maxMessageLength=" + maxMessageLength + " termLength=" + termLength + " channel=" +
                channelUri);
        }
    }

    static void validateTermLength(
        final PublicationParams params, final int explicitTermLength, final ChannelUri channelUri)
    {
        if (params.isSessionIdTagged && explicitTermLength != params.termLength)
        {
            throw new InvalidChannelException(
                TERM_LENGTH_PARAM_NAME + "=" + explicitTermLength + " does not match session-id tag value: channel=" +
                channelUri);
        }
    }

    static void validateMtuLength(
        final PublicationParams params, final int explicitMtuLength, final ChannelUri channelUri)
    {
        if (params.isSessionIdTagged && explicitMtuLength != params.mtuLength)
        {
            throw new InvalidChannelException(
                MTU_LENGTH_PARAM_NAME + "=" + explicitMtuLength + " does not match session-id tag value: channel=" +
                channelUri);
        }
    }

    private static String formatMatchError(
        final String paramName,
        final String existingValue,
        final String newValue,
        final String existingChannelUri,
        final String newChannelUri)
    {
        return "existing publication has different '" + paramName + "': existing=" +
            existingValue + " requested=" + newValue + " existingChannel=" + existingChannelUri +
            " channel=" + newChannelUri;
    }

    static void confirmMatch(
        final ChannelUri channelUri,
        final PublicationParams params,
        final RawLog rawLog,
        final int existingSessionId,
        final String existingChannel,
        final int existingInitialTermId,
        final int existingTermId,
        final int existingTermOffset)
    {
        final int mtuLength = LogBufferDescriptor.mtuLength(rawLog.metaData());
        if (channelUri.containsKey(MTU_LENGTH_PARAM_NAME) && mtuLength != params.mtuLength)
        {
            throw new IllegalStateException(formatMatchError(
                MTU_LENGTH_PARAM_NAME,
                String.valueOf(mtuLength),
                String.valueOf(params.mtuLength),
                existingChannel,
                channelUri.toString()));
        }

        if (channelUri.containsKey(TERM_LENGTH_PARAM_NAME) && rawLog.termLength() != params.termLength)
        {
            throw new IllegalStateException(formatMatchError(
                TERM_LENGTH_PARAM_NAME,
                String.valueOf(rawLog.termLength()),
                String.valueOf(params.termLength),
                existingChannel,
                channelUri.toString()));
        }

        if (channelUri.containsKey(SESSION_ID_PARAM_NAME) && params.sessionId != existingSessionId)
        {
            throw new IllegalStateException(formatMatchError(
                SESSION_ID_PARAM_NAME,
                String.valueOf(existingSessionId),
                String.valueOf(params.sessionId),
                existingChannel,
                channelUri.toString()));
        }

        if (channelUri.containsKey(INITIAL_TERM_ID_PARAM_NAME) && params.initialTermId != existingInitialTermId)
        {
            throw new IllegalStateException(formatMatchError(
                INITIAL_TERM_ID_PARAM_NAME,
                String.valueOf(existingInitialTermId),
                String.valueOf(params.initialTermId),
                existingChannel,
                channelUri.toString()));
        }

        if (channelUri.containsKey(TERM_ID_PARAM_NAME) && params.termId != existingTermId)
        {
            throw new IllegalStateException(formatMatchError(
                TERM_ID_PARAM_NAME,
                String.valueOf(existingTermId),
                String.valueOf(params.termId),
                existingChannel,
                channelUri.toString()));
        }

        if (channelUri.containsKey(TERM_OFFSET_PARAM_NAME) && params.termOffset != existingTermOffset)
        {
            throw new IllegalStateException(formatMatchError(
                TERM_OFFSET_PARAM_NAME,
                String.valueOf(existingTermOffset),
                String.valueOf(params.termOffset),
                existingChannel,
                channelUri.toString()));
        }
    }

    static void validateSpiesSimulateConnection(
        final PublicationParams params,
        final boolean existingSpiesSimulateConnection,
        final String channel,
        final String existingChannel)
    {
        if (params.spiesSimulateConnection != existingSpiesSimulateConnection)
        {
            throw new IllegalStateException("existing publication has different spiesSimulateConnection: existing=" +
                existingSpiesSimulateConnection + " requested=" + params.spiesSimulateConnection +
                " existingChannel=" + existingChannel + " channel=" + channel);
        }
    }

    static void validateMtuForSndbuf(
        final PublicationParams params,
        final int channelSocketSndbufLength,
        final MediaDriver.Context ctx,
        final String channel,
        final String existingChannel)
    {
        if (0 != channelSocketSndbufLength && params.mtuLength > channelSocketSndbufLength)
        {
            throw new IllegalStateException(
                "MTU greater than SO_SNDBUF for channel: mtu=" + params.mtuLength +
                " so-sndbuf=" + channelSocketSndbufLength +
                (null == existingChannel ? "" : (" existingChannel=" + existingChannel)) +
                " channel=" + channel);
        }
        else if (0 == channelSocketSndbufLength && params.mtuLength > ctx.osDefaultSocketSndbufLength())
        {
            throw new IllegalStateException(
                "MTU greater than SO_SNDBUF for channel: mtu=" + params.mtuLength +
                " so-sndbuf=" + ctx.osDefaultSocketSndbufLength() + " (OS default)" +
                (null == existingChannel ? "" : (" existingChannel=" + existingChannel)) +
                " channel=" + channel);
        }
    }

    private void getLingerTimeoutNs(final ChannelUri channelUri, final MediaDriver.Context ctx)
    {
        final String lingerParam = channelUri.get(LINGER_PARAM_NAME);
        if (null != lingerParam)
        {
            lingerTimeoutNs = SystemUtil.parseDuration(LINGER_PARAM_NAME, lingerParam);
        }
        else
        {
            lingerTimeoutNs = ctx.publicationLingerTimeoutNs();
        }
    }

    private void getSessionId(
        final ChannelUri channelUri,
        final DriverConductor driverConductor,
        final int streamId,
        final String canonicalForm)
    {
        final String sessionIdStr = channelUri.get(SESSION_ID_PARAM_NAME);
        if (null != sessionIdStr)
        {
            isSessionIdTagged = ChannelUri.isTagged(sessionIdStr);
            if (isSessionIdTagged)
            {
                final long tag;
                try
                {
                    tag = ChannelUri.getTag(sessionIdStr);
                }
                catch (final RuntimeException ex)
                {
                    throw new InvalidChannelException(
                        SESSION_ID_PARAM_NAME + "=" + sessionIdStr + " has an invalid tag: channel=" + channelUri, ex);
                }

                if (channelUri.isIpc())
                {
                    final IpcPublication publication = driverConductor.findIpcPublicationByTag(tag);
                    if (null == publication)
                    {
                        throw new InvalidChannelException(
                            SESSION_ID_PARAM_NAME + "=" + sessionIdStr + " must reference an IPC publication: " +
                            "channel=" + channelUri);
                    }

                    sessionId = publication.sessionId();
                    mtuLength = publication.mtuLength();
                    termLength = publication.termBufferLength();
                }
                else
                {
                    final NetworkPublication publication = driverConductor.findNetworkPublicationByTag(tag);
                    if (null == publication)
                    {
                        throw new InvalidChannelException(
                            SESSION_ID_PARAM_NAME + "=" + sessionIdStr + " must reference a network publication: " +
                            "channel=" + channelUri);
                    }

                    sessionId = publication.sessionId();
                    mtuLength = publication.mtuLength();
                    termLength = publication.termBufferLength();
                }
            }
            else
            {
                sessionId = Integer.parseInt(sessionIdStr);
            }
        }
        else
        {
            sessionId = driverConductor.nextAvailableSessionId(streamId, canonicalForm);
        }
    }

    private void getEos(final ChannelUri channelUri)
    {
        final String eosStr = channelUri.get(EOS_PARAM_NAME);
        signalEos = null == eosStr || "true".equals(eosStr);
    }

    private void getSparse(final ChannelUri channelUri, final MediaDriver.Context ctx)
    {
        final String sparseStr = channelUri.get(SPARSE_PARAM_NAME);
        isSparse = null != sparseStr ? "true".equals(sparseStr) : ctx.termBufferSparseFile();
    }

    private void getSpiesSimulateConnection(final ChannelUri channelUri, final MediaDriver.Context ctx)
    {
        final String sscStr = channelUri.get(SPIES_SIMULATE_CONNECTION_PARAM_NAME);
        spiesSimulateConnection = null != sscStr ? "true".equals(sscStr) : ctx.spiesSimulateConnection();
    }

    private long getTimeoutNs(final ChannelUri channelUri, final String paramName, final long defaultValue)
    {
        final String timeoutString = channelUri.get(paramName);
        return null != timeoutString ? SystemUtil.parseDuration(paramName, timeoutString) : defaultValue;
    }

    private void getUntetheredWindowLimitTimeout(final ChannelUri channelUri, final MediaDriver.Context ctx)
    {
        untetheredWindowLimitTimeoutNs = getTimeoutNs(
            channelUri, UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME, ctx.untetheredWindowLimitTimeoutNs());
    }

    private void getUntetheredLingerTimeout(final ChannelUri channelUri, final MediaDriver.Context ctx)
    {
        final String windowLimitTimeoutString = channelUri.get(UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME);
        final String timeoutString = channelUri.get(UNTETHERED_LINGER_TIMEOUT_PARAM_NAME);
        if (null != timeoutString)
        {
            // linger timeout was explicitly set for the channel.  use the set value
            untetheredLingerTimeoutNs = SystemUtil.parseDuration(UNTETHERED_LINGER_TIMEOUT_PARAM_NAME, timeoutString);
        }
        else if (null != windowLimitTimeoutString)
        {
            // window limit was explicitly set for the channel.  use it
            untetheredLingerTimeoutNs = untetheredWindowLimitTimeoutNs;
        }
        else
        {
            // neither value was explicitely set for the channel.  use the context value for linger
            // context value for linger needs similar checks to default to window timeout if not set
            untetheredLingerTimeoutNs = ctx.untetheredLingerTimeoutNs();
        }
    }

    private void getUntetheredRestingTimeout(final ChannelUri channelUri, final MediaDriver.Context ctx)
    {
        untetheredRestingTimeoutNs = getTimeoutNs(
            channelUri, UNTETHERED_RESTING_TIMEOUT_PARAM_NAME, ctx.untetheredRestingTimeoutNs());
    }

    private void getMaxResend(final ChannelUri channelUri, final MediaDriver.Context ctx)
    {
        final String maxRetransmtsString = channelUri.get(MAX_RESEND_PARAM_NAME);

        if (maxRetransmtsString == null)
        {
            maxResend = ctx.maxResend();
        }
        else
        {
            try
            {
                maxResend = Integer.parseInt(maxRetransmtsString);
            }
            catch (final NumberFormatException ex)
            {
                throw new InvalidChannelException(
                    "invalid " + MAX_RESEND_PARAM_NAME + ", must be a number", ex);
            }

            if (maxResend <= 0 || maxResend > Configuration.MAX_RESEND_MAX)
            {
                throw new InvalidChannelException(
                    "invalid " + MAX_RESEND_PARAM_NAME + "=" + maxResend +
                    ", must be > 0 and <= " + Configuration.MAX_RESEND_MAX);
            }
        }
    }

    private static long parseEntityTag(
        final String tagParam, final DriverConductor driverConductor, final ChannelUri channelUri)
    {
        final long entityTag;
        try
        {
            entityTag = Long.parseLong(tagParam);
        }
        catch (final NumberFormatException ex)
        {
            throw new InvalidChannelException("invalid entity tag, must be a number", ex);
        }

        if (INVALID_TAG == entityTag)
        {
            throw new InvalidChannelException(INVALID_TAG + " tag is reserved: channel=" + channelUri);
        }

        final NetworkPublication networkPublication = driverConductor.findNetworkPublicationByTag(entityTag);
        if (null != networkPublication)
        {
            throw new InvalidChannelException(entityTag + " entityTag already in use: existingChannel=" +
                networkPublication.channel() + " channel=" + channelUri);
        }
        final IpcPublication ipcPublication = driverConductor.findIpcPublicationByTag(entityTag);
        if (null != ipcPublication)
        {
            throw new InvalidChannelException(entityTag + " entityTag already in use: existingChannel=" +
                ipcPublication.channel() + " channel=" + channelUri);
        }

        return entityTag;
    }

    public String toString()
    {
        return "PublicationParams{" +
            "lingerTimeoutNs=" + lingerTimeoutNs +
            ", entityTag=" + entityTag +
            ", termLength=" + termLength +
            ", mtuLength=" + mtuLength +
            ", initialTermId=" + initialTermId +
            ", termId=" + termId +
            ", termOffset=" + termOffset +
            ", streamId=" + streamId +
            ", sessionId=" + sessionId +
            ", hasPosition=" + hasPosition +
            ", isSessionIdTagged=" + isSessionIdTagged +
            ", isSparse=" + isSparse +
            ", signalEos=" + signalEos +
            ", spiesSimulateConnection=" + spiesSimulateConnection +
            ", maxResend=" + maxResend +
            ", publicationWindowLength=" + publicationWindowLength +
            '}';
    }
}
