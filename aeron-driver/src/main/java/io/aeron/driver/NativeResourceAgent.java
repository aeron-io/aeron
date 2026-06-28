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

import io.aeron.ChannelUri;
import io.aeron.driver.buffer.LogFactory;
import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetSocketAddress;
import java.util.ArrayDeque;

import static io.aeron.logbuffer.LogBufferDescriptor.LOG_BUFFER_TYPE_CONCURRENT_PUBLICATION;
import static io.aeron.logbuffer.LogBufferDescriptor.LOG_BUFFER_TYPE_EXCLUSIVE_PUBLICATION;
import static io.aeron.logbuffer.LogBufferDescriptor.LOG_BUFFER_TYPE_PUBLICATION_IMAGE;
import static io.aeron.logbuffer.LogBufferDescriptor.PARTITION_COUNT;
import static io.aeron.logbuffer.LogBufferDescriptor.activeTermCount;
import static io.aeron.logbuffer.LogBufferDescriptor.correlationId;
import static io.aeron.logbuffer.LogBufferDescriptor.endOfStreamPosition;
import static io.aeron.logbuffer.LogBufferDescriptor.entityTag;
import static io.aeron.logbuffer.LogBufferDescriptor.group;
import static io.aeron.logbuffer.LogBufferDescriptor.indexByTerm;
import static io.aeron.logbuffer.LogBufferDescriptor.initialTermId;
import static io.aeron.logbuffer.LogBufferDescriptor.initialiseTailWithTermId;
import static io.aeron.logbuffer.LogBufferDescriptor.isPublicationRevoked;
import static io.aeron.logbuffer.LogBufferDescriptor.isResponse;
import static io.aeron.logbuffer.LogBufferDescriptor.lingerTimeoutNs;
import static io.aeron.logbuffer.LogBufferDescriptor.maxResend;
import static io.aeron.logbuffer.LogBufferDescriptor.mtuLength;
import static io.aeron.logbuffer.LogBufferDescriptor.nextPartitionIndex;
import static io.aeron.logbuffer.LogBufferDescriptor.osDefaultSocketRcvbufLength;
import static io.aeron.logbuffer.LogBufferDescriptor.osDefaultSocketSndbufLength;
import static io.aeron.logbuffer.LogBufferDescriptor.osMaxSocketRcvbufLength;
import static io.aeron.logbuffer.LogBufferDescriptor.osMaxSocketSndbufLength;
import static io.aeron.logbuffer.LogBufferDescriptor.packTail;
import static io.aeron.logbuffer.LogBufferDescriptor.pageSize;
import static io.aeron.logbuffer.LogBufferDescriptor.publicationWindowLength;
import static io.aeron.logbuffer.LogBufferDescriptor.rawTail;
import static io.aeron.logbuffer.LogBufferDescriptor.receiverWindowLength;
import static io.aeron.logbuffer.LogBufferDescriptor.rejoin;
import static io.aeron.logbuffer.LogBufferDescriptor.reliable;
import static io.aeron.logbuffer.LogBufferDescriptor.responseCorrelationId;
import static io.aeron.logbuffer.LogBufferDescriptor.signalEos;
import static io.aeron.logbuffer.LogBufferDescriptor.socketRcvbufLength;
import static io.aeron.logbuffer.LogBufferDescriptor.socketSndbufLength;
import static io.aeron.logbuffer.LogBufferDescriptor.sparse;
import static io.aeron.logbuffer.LogBufferDescriptor.spiesSimulateConnection;
import static io.aeron.logbuffer.LogBufferDescriptor.storeDefaultFrameHeader;
import static io.aeron.logbuffer.LogBufferDescriptor.termLength;
import static io.aeron.logbuffer.LogBufferDescriptor.tether;
import static io.aeron.logbuffer.LogBufferDescriptor.type;
import static io.aeron.logbuffer.LogBufferDescriptor.untetheredLingerTimeoutNs;
import static io.aeron.logbuffer.LogBufferDescriptor.untetheredRestingTimeoutNs;
import static io.aeron.logbuffer.LogBufferDescriptor.untetheredWindowLimitTimeoutNs;
import static io.aeron.protocol.DataHeaderFlyweight.createDefaultHeader;

final class NativeResourceAgent implements Agent
{
    private final DataHeaderFlyweight defaultDataHeader = new DataHeaderFlyweight(createDefaultHeader(0, 0, 0));
    private final ArrayDeque<RawLog> logBuffersToFree = new ArrayDeque<>();
    private final TimeTrackingNameResolver nameResolver;
    private final OneToOneConcurrentArrayQueue<Runnable> taskQueue;
    private final AtomicCounter freeFailsCounter;
    private final int freeLimit;
    private final LogFactory logFactory;
    private final MediaDriver.Context ctx;

    NativeResourceAgent(final MediaDriver.Context ctx)
    {
        this.nameResolver = new TimeTrackingNameResolver(
            ctx.nameResolver(),
            ctx.nanoClock(),
            ctx.nameResolverTimeTracker());
        this.taskQueue = ctx.nativeResourceAgentCommandQueue();
        this.freeFailsCounter = ctx.systemCounters().get(SystemCounterDescriptor.FREE_FAILS);
        this.freeLimit = ctx.resourceFreeLimit();
        logFactory = ctx.logFactory();
        this.ctx = ctx;
    }

    /**
     * {@inheritDoc}
     */
    public void onStart()
    {
        nameResolver.onStart();
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        int workCount = 0;
        workCount += nameResolver.doWork();
        workCount += runTasks();
        workCount += freeEndOfLifeResources();

        return workCount;
    }

    /**
     * {@inheritDoc}
     */
    public void onClose()
    {
        nameResolver.onClose();

        RawLog rawLog;
        while (null != (rawLog = logBuffersToFree.pollFirst()))
        {
            rawLog.free();
        }
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return "aeron-md-nra";
    }

    void onParseChannel(final CommandResult<UdpChannel> result, final String channel, final boolean isDestination)
    {
        try
        {
            result.ok(UdpChannel.parse(channel, nameResolver, isDestination));
        }
        catch (final RuntimeException ex)
        {
            result.error(ex);
        }
    }

    void onReResolveAddress(
        final CommandResult<InetSocketAddress> result, final String endpoint, final String uriParamName)
    {
        try
        {
            result.ok(UdpChannel.resolve(endpoint, uriParamName, true, nameResolver));
        }
        catch (final RuntimeException ex)
        {
            result.error(ex);
        }
    }

    void onDestinationAddress(final CommandResult<InetSocketAddress> result, final ChannelUri channel)
    {
        try
        {
            result.ok(UdpChannel.destinationAddress(channel, nameResolver));
        }
        catch (final RuntimeException ex)
        {
            result.error(ex);
        }
    }

    void onFreeLogBuffer(final RawLog rawLog)
    {
        logBuffersToFree.addLast(rawLog);
    }

    void onNewNetworkPublicationLog(
        final CommandResult<RawLog> result,
        final boolean isExclusive,
        final int streamId,
        final long registrationId,
        final int socketRcvBufLength,
        final int socketSndBufLength,
        final PublicationParams params,
        final boolean hasGroupSemantics)
    {
        try
        {
            final RawLog rawLog = logFactory.newPublication(registrationId, params.termLength, params.isSparse);

            final int receiverWindowLength = 0;
            final boolean tether = false;
            final boolean rejoin = false;
            final boolean reliable = false;
            final int initialTermId = params.initialTermId;
            initLogMetadata(
                isExclusive ? LOG_BUFFER_TYPE_EXCLUSIVE_PUBLICATION : LOG_BUFFER_TYPE_CONCURRENT_PUBLICATION,
                params.sessionId,
                streamId,
                initialTermId,
                params.mtuLength,
                registrationId,
                socketRcvBufLength,
                socketSndBufLength,
                params.termOffset,
                receiverWindowLength,
                tether,
                rejoin,
                reliable,
                params.isSparse,
                hasGroupSemantics,
                params.isResponse,
                params.publicationWindowLength,
                params.untetheredWindowLimitTimeoutNs,
                params.untetheredLingerTimeoutNs,
                params.untetheredRestingTimeoutNs,
                params.maxResend,
                params.lingerTimeoutNs,
                params.signalEos,
                params.spiesSimulateConnection,
                params.entityTag,
                params.responseCorrelationId,
                rawLog);
            initialiseLogBufferPosition(initialTermId, params, rawLog.metaData());

            result.ok(rawLog);
        }
        catch (final Exception ex)
        {
            result.error(ex);
        }
    }

    void onNewIpcPublicationLog(
        final CommandResult<RawLog> result,
        final boolean isExclusive,
        final int streamId,
        final long registrationId,
        final PublicationParams params)
    {
        try
        {
            final RawLog rawLog = logFactory.newPublication(registrationId, params.termLength, params.isSparse);

            final int socketRcvBufLength = 0;
            final int socketSndbufLength = 0;
            final int receiverWindowLength = 0;
            final boolean tether = false;
            final boolean rejoin = false;
            final boolean reliable = false;
            final boolean group = false;
            final int initialTermId = params.initialTermId;
            initLogMetadata(
                isExclusive ? LOG_BUFFER_TYPE_EXCLUSIVE_PUBLICATION : LOG_BUFFER_TYPE_CONCURRENT_PUBLICATION,
                params.sessionId,
                streamId,
                initialTermId,
                params.mtuLength,
                registrationId,
                socketRcvBufLength,
                socketSndbufLength,
                params.termOffset,
                receiverWindowLength,
                tether,
                rejoin,
                reliable,
                params.isSparse,
                group,
                params.isResponse,
                params.publicationWindowLength,
                params.untetheredWindowLimitTimeoutNs,
                params.untetheredLingerTimeoutNs,
                params.untetheredRestingTimeoutNs,
                params.maxResend,
                params.lingerTimeoutNs,
                params.signalEos,
                params.spiesSimulateConnection,
                params.entityTag,
                params.responseCorrelationId,
                rawLog);
            initialiseLogBufferPosition(initialTermId, params, rawLog.metaData());

            result.ok(rawLog);
        }
        catch (final Exception ex)
        {
            result.error(ex);
        }
    }

    void onNewPublicationImageLog(
        final CommandResult<RawLog> result,
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int termOffset,
        final int termBufferLength,
        final int senderMtuLength,
        final ReceiveChannelEndpoint channelEndpoint,
        final long registrationId,
        final SubscriptionParams params,
        final boolean sparse,
        final boolean reliable,
        final boolean groupSemantics)
    {
        try
        {
            final RawLog rawLog = logFactory.newImage(registrationId, termBufferLength, sparse);

            final int publicationWindowLength = 0;
            final int maxResend = 0;
            final long lingerTimeoutNs = 0;
            final boolean signalEos = false;
            final boolean spiesSimulateConnection = false;
            final long entityTag = 0;
            final long responseCorrelationId = 0;
            initLogMetadata(
                LOG_BUFFER_TYPE_PUBLICATION_IMAGE,
                sessionId,
                streamId,
                initialTermId,
                senderMtuLength,
                registrationId,
                channelEndpoint.socketRcvbufLength(),
                channelEndpoint.socketSndbufLength(),
                termOffset,
                params.receiverWindowLength,
                params.isTether,
                params.isRejoin,
                reliable,
                sparse,
                groupSemantics,
                params.isResponse,
                publicationWindowLength,
                params.untetheredWindowLimitTimeoutNs,
                params.untetheredLingerTimeoutNs,
                params.untetheredRestingTimeoutNs,
                maxResend,
                lingerTimeoutNs,
                signalEos,
                spiesSimulateConnection,
                entityTag,
                responseCorrelationId,
                rawLog);

            result.ok(rawLog);
        }
        catch (final Exception ex)
        {
            result.error(ex);
        }
    }

    private void initLogMetadata(
        final byte type,
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int mtuLength,
        final long registrationId,
        final int socketRcvBufLength,
        final int socketSndbufLength,
        final int termOffset,
        final int receiverWindowLength,
        final boolean tether,
        final boolean rejoin,
        final boolean reliable,
        final boolean sparse,
        final boolean group,
        final boolean isResponse,
        final int publicationWindowLength,
        final long untetheredWindowLimitTimeoutNs,
        final long untetheredLingerTimeoutNs,
        final long untetheredRestingTimeoutNs,
        final int maxResend,
        final long lingerTimeoutNs,
        final boolean signalEos,
        final boolean spiesSimulateConnection,
        final long entityTag,
        final long responseCorrelationId,
        final RawLog rawLog)
    {
        final UnsafeBuffer logMetaData = rawLog.metaData();

        defaultDataHeader
            .sessionId(sessionId)
            .streamId(streamId)
            .termId(initialTermId)
            .termOffset(termOffset);
        storeDefaultFrameHeader(logMetaData, defaultDataHeader);

        correlationId(logMetaData, registrationId);
        initialTermId(logMetaData, initialTermId);
        mtuLength(logMetaData, mtuLength);
        termLength(logMetaData, rawLog.termLength());
        pageSize(logMetaData, ctx.filePageSize());

        publicationWindowLength(logMetaData, publicationWindowLength);
        receiverWindowLength(logMetaData, receiverWindowLength);
        socketSndbufLength(logMetaData, socketSndbufLength);
        osDefaultSocketSndbufLength(logMetaData, ctx.osDefaultSocketSndbufLength());
        osMaxSocketSndbufLength(logMetaData, ctx.osMaxSocketSndbufLength());
        socketRcvbufLength(logMetaData, socketRcvBufLength);
        osDefaultSocketRcvbufLength(logMetaData, ctx.osDefaultSocketRcvbufLength());
        osMaxSocketRcvbufLength(logMetaData, ctx.osMaxSocketRcvbufLength());
        maxResend(logMetaData, maxResend);

        rejoin(logMetaData, rejoin);
        reliable(logMetaData, reliable);
        sparse(logMetaData, sparse);
        signalEos(logMetaData, signalEos);
        spiesSimulateConnection(logMetaData, spiesSimulateConnection);
        tether(logMetaData, tether);
        isPublicationRevoked(logMetaData, false);
        group(logMetaData, group);
        isResponse(logMetaData, isResponse);
        type(logMetaData, type);

        entityTag(logMetaData, entityTag);
        responseCorrelationId(logMetaData, responseCorrelationId);
        untetheredWindowLimitTimeoutNs(logMetaData, untetheredWindowLimitTimeoutNs);
        untetheredLingerTimeoutNs(logMetaData, untetheredLingerTimeoutNs);
        untetheredRestingTimeoutNs(logMetaData, untetheredRestingTimeoutNs);
        lingerTimeoutNs(logMetaData, lingerTimeoutNs);

        // Acts like a release fence; so this should be the last statement to ensure that all above writes
        // are ordered before the eos-position.
        endOfStreamPosition(logMetaData, Long.MAX_VALUE);
    }

    private static void initialiseLogBufferPosition(
        final int initialTermId, final PublicationParams params, final UnsafeBuffer logMetaData)
    {
        if (params.hasPosition)
        {
            final int termId = params.termId;
            final int termCount = termId - initialTermId;
            int activeIndex = indexByTerm(initialTermId, termId);

            rawTail(logMetaData, activeIndex, packTail(termId, params.termOffset));
            for (int i = 1; i < PARTITION_COUNT; i++)
            {
                final int expectedTermId = (termId + i) - PARTITION_COUNT;
                activeIndex = nextPartitionIndex(activeIndex);
                initialiseTailWithTermId(logMetaData, activeIndex, expectedTermId);
            }

            activeTermCount(logMetaData, termCount);
        }
        else
        {
            initialiseTailWithTermId(logMetaData, 0, initialTermId);
            for (int i = 1; i < PARTITION_COUNT; i++)
            {
                final int expectedTermId = (initialTermId + i) - PARTITION_COUNT;
                initialiseTailWithTermId(logMetaData, i, expectedTermId);
            }
        }
    }

    private int runTasks()
    {
        final Runnable task = taskQueue.poll();
        if (null != task)
        {
            try
            {
                task.run();
            }
            catch (final RuntimeException ex)
            {
                ctx.countedErrorHandler().onError(ex);
            }
            return 1;
        }
        return 0;
    }

    private int freeEndOfLifeResources()
    {
        int workCount = 0;

        for (int i = 0; i < freeLimit; i++)
        {
            final RawLog rawLog = logBuffersToFree.pollFirst();
            if (null == rawLog)
            {
                break;
            }

            if (rawLog.free())
            {
                workCount++;
            }
            else
            {
                freeFailsCounter.incrementRelease();
                logBuffersToFree.addLast(rawLog);
            }
        }

        return workCount;
    }
}
