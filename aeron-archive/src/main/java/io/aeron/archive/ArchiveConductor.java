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
package io.aeron.archive;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveEvent;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.DutyCycleTracker;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.security.Authenticator;
import io.aeron.security.AuthorisationService;
import org.agrona.*;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2LongCounterMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.MutableLong;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.*;
import static io.aeron.archive.Archive.Configuration.MARK_FILE_UPDATE_INTERVAL_MS;
import static io.aeron.archive.Archive.Configuration.RECORDING_SEGMENT_SUFFIX;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.segmentFileBasePosition;
import static io.aeron.archive.client.ArchiveException.*;
import static io.aeron.archive.codecs.RecordingState.DELETED;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.*;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.agrona.AsciiEncoding.digitCount;
import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;

abstract class ArchiveConductor
    extends SessionWorker<Session>
    implements UnavailableImageHandler, UnavailableCounterHandler
{
    private static final EnumSet<StandardOpenOption> FILE_OPTIONS = EnumSet.of(READ, WRITE);
    static final String DELETE_SUFFIX = ".del";

    private final long closeHandlerRegistrationId;
    private final long unavailableCounterHandlerRegistrationId;
    private final long connectTimeoutMs;
    private final long sessionLivenessCheckIntervalMs;
    private long nextSessionId = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
    private long markFileUpdateDeadlineMs = 0;
    private int replayId = 1;
    private volatile boolean isAbort;

    private final RecordingSummary recordingSummary = new RecordingSummary();
    private final ControlRequestDecoders decoders = new ControlRequestDecoders();
    private final ArrayDeque<Runnable> taskQueue = new ArrayDeque<>();
    private final Long2ObjectHashMap<ReplaySession> replaySessionByIdMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<RecordingSession> recordingSessionByIdMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<ReplicationSession> replicationSessionByIdMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<DeleteSegmentsSession> deleteSegmentsSessionByIdMap = new Long2ObjectHashMap<>();
    private final Int2ObjectHashMap<Counter> counterByIdMap = new Int2ObjectHashMap<>();
    private final Object2ObjectHashMap<String, Subscription> recordingSubscriptionByKeyMap =
        new Object2ObjectHashMap<>();
    private final Long2ObjectHashMap<SessionForReplay> controlSessionByReplayToken = new Long2ObjectHashMap<>();
    private final UnsafeBuffer descriptorBuffer = new UnsafeBuffer();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
    private final UnsafeBuffer counterMetadataBuffer = new UnsafeBuffer(new byte[METADATA_LENGTH]);
    private final Long2LongCounterMap subscriptionRefCountMap = new Long2LongCounterMap(0L);

    private final Aeron aeron;
    private final AgentInvoker aeronAgentInvoker;
    private final AgentInvoker driverAgentInvoker;
    private final EpochClock epochClock;
    private final NanoClock nanoClock;
    private final CachedEpochClock cachedEpochClock = new CachedEpochClock();
    private final File archiveDir;
    private final Subscription controlSubscription;
    private final Subscription localControlSubscription;
    private final Catalog catalog;
    private final ArchiveMarkFile markFile;
    private final RecordingEventsProxy recordingEventsProxy;
    private final Authenticator authenticator;
    private final AuthorisationService authorisationService;
    private final ControlSessionAdapter controlSessionAdapter;
    private final ControlResponseProxy controlResponseProxy = new ControlResponseProxy();
    private final ControlSessionProxy controlSessionProxy = new ControlSessionProxy(controlResponseProxy);
    private final DutyCycleTracker dutyCycleTracker;
    private final Random random;
    private final ExpandableArrayBuffer tempBuffer = new ExpandableArrayBuffer(300);
    final Archive.Context ctx;
    Recorder recorder;
    Replayer replayer;

    ArchiveConductor(final Archive.Context ctx)
    {
        super("archive-conductor", ctx.countedErrorHandler());

        this.ctx = ctx;

        aeron = ctx.aeron();
        aeronAgentInvoker = aeron.conductorAgentInvoker();
        driverAgentInvoker = ctx.mediaDriverAgentInvoker();
        epochClock = ctx.epochClock();
        nanoClock = ctx.nanoClock();
        archiveDir = ctx.archiveDir();
        connectTimeoutMs = TimeUnit.NANOSECONDS.toMillis(ctx.connectTimeoutNs());
        sessionLivenessCheckIntervalMs = TimeUnit.NANOSECONDS.toMillis(ctx.sessionLivenessCheckIntervalNs());
        catalog = ctx.catalog();
        markFile = ctx.archiveMarkFile();
        dutyCycleTracker = ctx.conductorDutyCycleTracker();
        cachedEpochClock.update(epochClock.time());

        random = ctx.secureRandom();

        authenticator = ctx.authenticatorSupplier().get();
        if (null == authenticator)
        {
            throw new ArchiveException("authenticator cannot be null");
        }

        authorisationService = ctx.authorisationServiceSupplier().get();
        if (null == authorisationService)
        {
            throw new ArchiveException("authorisation service cannot be null");
        }

        unavailableCounterHandlerRegistrationId = aeron.addUnavailableCounterHandler(this);
        closeHandlerRegistrationId = aeron.addCloseHandler(this::abort);

        recordingEventsProxy = ctx.recordingEventsEnabled() ? new RecordingEventsProxy(
            aeron.addExclusivePublication(ctx.recordingEventsChannel(), ctx.recordingEventsStreamId())) : null;

        if (ctx.controlChannelEnabled())
        {
            final ChannelUri controlChannelUri = ChannelUri.parse(ctx.controlChannel());
            controlChannelUri.put(CommonContext.SPARSE_PARAM_NAME, Boolean.toString(ctx.controlTermBufferSparse()));
            controlSubscription = aeron.addSubscription(
                controlChannelUri.toString(), ctx.controlStreamId(), null, this);
        }
        else
        {
            controlSubscription = null;
        }

        localControlSubscription = aeron.addSubscription(
            ctx.localControlChannel(), ctx.localControlStreamId(), null, this);

        controlSessionAdapter = new ControlSessionAdapter(
            decoders, controlSubscription, localControlSubscription, this, authorisationService);
    }

    public void onStart()
    {
        recorder = newRecorder();
        replayer = newReplayer();

        dutyCycleTracker.update(nanoClock.nanoTime());
    }

    public void onUnavailableImage(final Image image)
    {
        controlSessionAdapter.abortControlSessionByImage(image);
    }

    public void onUnavailableCounter(
        final CountersReader countersReader, final long registrationId, final int counterId)
    {
        final Counter counter = counterByIdMap.remove(counterId);
        if (null != counter)
        {
            counter.close();
        }
    }

    abstract Recorder newRecorder();

    abstract Replayer newReplayer();

    /**
     * {@inheritDoc}
     */
    protected final void preSessionsClose()
    {
        closeSessionWorkers();
    }

    /**
     * {@inheritDoc}
     */
    protected abstract void closeSessionWorkers();

    /**
     * {@inheritDoc}
     */
    protected void postSessionsClose()
    {
        if (isAbort)
        {
            ctx.abortLatch().countDown();
        }
        else
        {
            aeron.removeCloseHandler(closeHandlerRegistrationId);
            aeron.removeUnavailableCounterHandler(unavailableCounterHandlerRegistrationId);

            if (!ctx.ownsAeronClient())
            {
                for (final Subscription subscription : recordingSubscriptionByKeyMap.values())
                {
                    subscription.close();
                }

                CloseHelper.close(localControlSubscription);
                CloseHelper.close(controlSubscription);
                CloseHelper.close(recordingEventsProxy);
            }
        }

        markFile.signalTerminated();
        ctx.close();
    }

    /**
     * {@inheritDoc}
     */
    protected void abort()
    {
        try
        {
            isAbort = true;

            if (null != recorder)
            {
                recorder.abort();
            }

            if (null != replayer)
            {
                replayer.abort();
            }

            ctx.errorCounter().close();
            if (!ctx.abortLatch().await(AgentRunner.RETRY_CLOSE_TIMEOUT_MS * 3L, TimeUnit.MILLISECONDS))
            {
                errorHandler.onError(new TimeoutException("awaiting abort latch", AeronException.Category.WARN));
            }
        }
        catch (final InterruptedException ignore)
        {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        final long nowNs = nanoClock.nanoTime();
        int workCount = 0;

        if (isAbort)
        {
            throw new AgentTerminationException("unexpected Aeron close");
        }

        dutyCycleTracker.measureAndUpdate(nowNs);

        final long nowMs = epochClock.time();
        if (cachedEpochClock.time() != nowMs)
        {
            cachedEpochClock.update(nowMs);
            workCount += invokeAeronInvoker();

            if (nowMs >= markFileUpdateDeadlineMs)
            {
                markFileUpdateDeadlineMs = nowMs + MARK_FILE_UPDATE_INTERVAL_MS;
                markFile.updateActivityTimestamp(nowMs);
            }
        }

        workCount += controlSessionAdapter.poll();

        workCount += checkReplayTokens(nowNs);
        workCount += invokeDriverConductor();
        workCount += runTasks(taskQueue);

        return workCount + super.doWork();
    }

    Archive.Context context()
    {
        return ctx;
    }

    ControlResponseProxy controlResponseProxy()
    {
        return controlResponseProxy;
    }

    final int invokeAeronInvoker()
    {
        int workCount = 0;

        if (null != aeronAgentInvoker)
        {
            workCount += aeronAgentInvoker.invoke();

            if (isAbort || aeronAgentInvoker.isClosed())
            {
                isAbort = true;
                throw new AgentTerminationException("unexpected Aeron close");
            }
        }

        return workCount;
    }

    final int invokeDriverConductor()
    {
        int workCount = 0;

        if (null != driverAgentInvoker)
        {
            workCount += driverAgentInvoker.invoke();

            if (driverAgentInvoker.isClosed())
            {
                throw new AgentTerminationException("unexpected driver close");
            }
        }

        return workCount;
    }

    void logWarning(final String message)
    {
        errorHandler.onError(new ArchiveEvent(message));
    }

    ControlSession newControlSession(
        final Image image,
        final long correlationId,
        final int streamId,
        final int version,
        final String channel,
        final byte[] encodedCredentials,
        final String clientInfo,
        final ControlSessionAdapter controlSessionAdapter)
    {
        final ChannelUri channelUri = ChannelUri.parse(channel);

        final String mtuStr = channelUri.get(CommonContext.MTU_LENGTH_PARAM_NAME);
        final int mtuLength = null == mtuStr ?
            ctx.controlMtuLength() : (int)SystemUtil.parseSize(MTU_LENGTH_PARAM_NAME, mtuStr);
        final String termLengthStr = channelUri.get(TERM_LENGTH_PARAM_NAME);
        final int termLength = null == termLengthStr ?
            ctx.controlTermBufferLength() : (int)SystemUtil.parseSize(TERM_LENGTH_PARAM_NAME, termLengthStr);
        final String isSparseStr = channelUri.get(SPARSE_PARAM_NAME);
        final boolean isSparse = null == isSparseStr ?
            ctx.controlTermBufferSparse() : Boolean.parseBoolean(isSparseStr);
        final boolean usingResponseChannel = CONTROL_MODE_RESPONSE.equals(channelUri.get(MDC_CONTROL_MODE_PARAM_NAME));

        final ChannelUriStringBuilder urlBuilder = strippedChannelBuilder(channelUri)
            .termLength(termLength)
            .sparse(isSparse)
            .mtu(mtuLength);

        if (usingResponseChannel)
        {
            urlBuilder.responseCorrelationId(image.correlationId());
        }

        final String responseChannel = urlBuilder.build();

        String invalidVersionMessage = null;
        if (SemanticVersion.major(version) != AeronArchive.Configuration.PROTOCOL_MAJOR_VERSION)
        {
            invalidVersionMessage = "invalid client version " + SemanticVersion.toString(version) +
                ", archive is " + SemanticVersion.toString(AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION);
        }

        final long controlSessionId = nextSessionId++;
        final long controlPublicationRegistrationId = aeron.asyncAddExclusivePublication(responseChannel, streamId);

        final String imageInfo = "sourceIdentity=" + image.sourceIdentity() + " sessionId=" + image.sessionId();
        final long sessionCounterRegistrationId = ControlSessionCounter.allocate(
            aeron,
            tempBuffer,
            ctx.archiveId(),
            controlSessionId,
            Strings.isEmpty(clientInfo) ? imageInfo : clientInfo + " " + imageInfo);

        final ControlSession controlSession = new ControlSession(
            controlSessionId,
            correlationId,
            connectTimeoutMs,
            sessionLivenessCheckIntervalMs,
            controlPublicationRegistrationId,
            sessionCounterRegistrationId,
            responseChannel,
            streamId,
            invalidVersionMessage,
            controlSessionAdapter,
            aeron,
            this,
            cachedEpochClock,
            controlResponseProxy,
            authenticator,
            controlSessionProxy);

        authenticator.onConnectRequest(controlSession.sessionId(), encodedCredentials, cachedEpochClock.time());

        addSession(controlSession);
        ctx.controlSessionsCounter().incrementRelease();

        return controlSession;
    }

    void archiveId(final long correlationId, final ControlSession controlSession)
    {
        controlSession.sendOkResponse(correlationId, ctx.archiveId());
    }

    void startRecording(
        final long correlationId,
        final int streamId,
        final SourceLocation sourceLocation,
        final boolean autoStop,
        final String originalChannel,
        final ControlSession controlSession)
    {
        if (recordingSessionByIdMap.size() >= ctx.maxConcurrentRecordings())
        {
            final String msg = "max concurrent recordings reached " + ctx.maxConcurrentRecordings();
            controlSession.sendErrorResponse(correlationId, MAX_RECORDINGS, msg);
            return;
        }

        if (null != isLowStorageSpace(correlationId, controlSession))
        {
            return;
        }

        try
        {
            final ChannelUri channelUri = ChannelUri.parse(originalChannel);
            final String key = makeKey(streamId, channelUri);
            final Subscription oldSubscription = recordingSubscriptionByKeyMap.get(key);

            if (null == oldSubscription)
            {
                final String strippedChannel = strippedChannelBuilder(channelUri).build();
                final String channel = sourceLocation == SourceLocation.LOCAL && channelUri.isUdp() ?
                    SPY_PREFIX + strippedChannel : strippedChannel;

                final AvailableImageHandler handler = (image) -> taskQueue.addLast(() -> startRecordingSession(
                    controlSession, correlationId, strippedChannel, originalChannel, image, autoStop));

                final Subscription subscription = aeron.addSubscription(channel, streamId, handler, null);

                recordingSubscriptionByKeyMap.put(key, subscription);
                subscriptionRefCountMap.incrementAndGet(subscription.registrationId());
                controlSession.sendOkResponse(correlationId, subscription.registrationId());
            }
            else
            {
                final String msg = "recording exists for streamId=" + streamId + " channel=" + originalChannel;
                controlSession.sendErrorResponse(correlationId, ACTIVE_SUBSCRIPTION, msg);
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
            controlSession.sendErrorResponse(correlationId, ex.getMessage());
        }
    }

    void stopRecording(
        final long correlationId, final int streamId, final String channel, final ControlSession controlSession)
    {
        try
        {
            final String key = makeKey(streamId, ChannelUri.parse(channel));
            final Subscription subscription = recordingSubscriptionByKeyMap.remove(key);

            if (null != subscription)
            {
                abortRecordingSessionAndCloseSubscription(subscription);

                controlSession.sendOkResponse(correlationId);
            }
            else
            {
                final String msg = "no recording found for streamId=" + streamId + " channel=" + channel;
                controlSession.sendErrorResponse(correlationId, UNKNOWN_SUBSCRIPTION, msg);
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
            controlSession.sendErrorResponse(correlationId, ex.getMessage());
        }
    }

    void stopRecordingSubscription(
        final long correlationId, final long subscriptionId, final ControlSession controlSession)
    {
        if (stopRecordingSubscription(subscriptionId))
        {
            controlSession.sendOkResponse(correlationId);
        }
        else
        {
            final String msg = "no recording subscription found for subscriptionId=" + subscriptionId;
            controlSession.sendErrorResponse(correlationId, UNKNOWN_SUBSCRIPTION, msg);
        }
    }

    boolean stopRecordingSubscription(final long subscriptionId)
    {
        final Subscription subscription = removeRecordingSubscription(subscriptionId);
        if (null != subscription)
        {
            abortRecordingSessionAndCloseSubscription(subscription);
            return true;
        }

        return false;
    }

    void newListRecordingsSession(
        final long correlationId, final long fromId, final int count, final ControlSession controlSession)
    {
        if (controlSession.hasActiveListing())
        {
            final String msg = "active listing already in progress";
            controlSession.sendErrorResponse(correlationId, ACTIVE_LISTING, msg);
            return;
        }

        final ListRecordingsSession session = new ListRecordingsSession(
            correlationId,
            fromId,
            count,
            catalog,
            controlSession,
            descriptorBuffer);
        addSession(session);
        controlSession.activeListing(session);
    }

    void newListRecordingsForUriSession(
        final long correlationId,
        final long fromRecordingId,
        final int count,
        final int streamId,
        final byte[] channelFragment,
        final ControlSession controlSession)
    {
        if (controlSession.hasActiveListing())
        {
            final String msg = "active listing already in progress";
            controlSession.sendErrorResponse(correlationId, ACTIVE_LISTING, msg);
            return;
        }

        final ListRecordingsForUriSession session = new ListRecordingsForUriSession(
            correlationId,
            fromRecordingId,
            count,
            channelFragment,
            streamId,
            catalog,
            controlSession,
            descriptorBuffer,
            recordingDescriptorDecoder);
        addSession(session);
        controlSession.activeListing(session);
    }

    void listRecording(final long correlationId, final long recordingId, final ControlSession controlSession)
    {
        if (controlSession.hasActiveListing())
        {
            final String msg = "active listing already in progress";
            controlSession.sendErrorResponse(correlationId, ACTIVE_LISTING, msg);
        }
        else if (!catalog.hasRecording(recordingId))
        {
            controlSession.sendRecordingUnknown(correlationId, recordingId);
        }
        else
        {
            final ListRecordingByIdSession session =
                new ListRecordingByIdSession(correlationId, recordingId, catalog, controlSession, descriptorBuffer);
            addSession(session);
            controlSession.activeListing(session);
        }
    }

    void updateChannel(
        final long correlationId,
        final long recordingId,
        final String channel,
        final ControlSession controlSession)
    {
        if (controlSession.hasActiveListing())
        {
            final String msg = "active listing already in progress";
            controlSession.sendErrorResponse(correlationId, ACTIVE_LISTING, msg);
        }
        else if (!catalog.hasRecording(recordingId))
        {
            controlSession.sendRecordingUnknown(correlationId, recordingId);
        }
        else
        {
            final ChannelUri channelUri = ChannelUri.parse(channel);
            final String strippedChannel = strippedChannelBuilder(channelUri).build();

            final UpdateChannelSession updateChannelSession = new UpdateChannelSession(
                correlationId,
                recordingId,
                channel,
                strippedChannel,
                catalog,
                controlSession,
                descriptorBuffer);

            addSession(updateChannelSession);
            controlSession.activeListing(updateChannelSession);
        }
    }

    void findLastMatchingRecording(
        final long correlationId,
        final long minRecordingId,
        final int sessionId,
        final int streamId,
        final byte[] channelFragment,
        final ControlSession controlSession)
    {
        if (minRecordingId < 0)
        {
            final String msg = "minRecordingId=" + minRecordingId + " < 0";
            controlSession.sendErrorResponse(correlationId, UNKNOWN_RECORDING, msg);
        }
        else
        {
            final long recordingId = catalog.findLast(minRecordingId, sessionId, streamId, channelFragment);
            // If not found, recordingId is -1, which matches client side specification.
            controlSession.sendOkResponse(correlationId, recordingId);
        }
    }

    @SuppressWarnings("MethodLength")
    void startReplay(
        final long correlationId,
        final long recordingId,
        final long position,
        final long length,
        final int fileIoMaxLength,
        final int replayStreamId,
        final String replayChannel,
        final Counter limitPositionCounter,
        final ControlSession controlSession)
    {
        if (replaySessionByIdMap.size() >= ctx.maxConcurrentReplays())
        {
            final String msg = "max concurrent replays reached " + ctx.maxConcurrentReplays();
            controlSession.sendErrorResponse(correlationId, MAX_REPLAYS, msg);
            return;
        }

        if (!catalog.hasRecording(recordingId))
        {
            final String msg = "unknown recording id: " + recordingId;
            controlSession.sendErrorResponse(correlationId, UNKNOWN_RECORDING, msg);
            return;
        }

        Counter replayLimitPositionCounter = limitPositionCounter;
        if (null == replayLimitPositionCounter)
        {
            final RecordingSession recordingSession = recordingSessionByIdMap.get(recordingId);
            if (null != recordingSession)
            {
                replayLimitPositionCounter = recordingSession.recordingPosition();
            }
        }
        long limitPosition = NULL_POSITION;
        if (null != replayLimitPositionCounter)
        {
            limitPosition = replayLimitPositionCounter.get();
        }

        catalog.recordingSummary(recordingId, recordingSummary);
        final long startPosition = recordingSummary.startPosition;
        long replayPosition = startPosition;
        if (NULL_POSITION != position)
        {
            if (isInvalidReplayPosition(correlationId, controlSession, recordingId, position, recordingSummary))
            {
                return;
            }
            replayPosition = position;
        }

        if (fileIoMaxLength > 0 && fileIoMaxLength < recordingSummary.mtuLength)
        {
            final String msg = "fileIoMaxLength=" + fileIoMaxLength + " < mtuLength=" + recordingSummary.mtuLength;
            controlSession.sendErrorResponse(correlationId, msg);
            return;
        }
        else if (ctx.replayBuffer().capacity() < recordingSummary.mtuLength)
        {
            final int replayBufferCapacity = ctx.replayBuffer().capacity();
            final String msg = "replayBufferCapacity=" + replayBufferCapacity +
                " < mtuLength=" + recordingSummary.mtuLength;
            controlSession.sendErrorResponse(correlationId, msg);
            return;
        }

        final long stopPosition;
        final long maxLength;
        if (NULL_POSITION != limitPosition)
        {
            if (replayPosition > limitPosition)
            {
                final String msg = "requested replay start position=" + replayPosition +
                    " must be less than the limit position=" + limitPosition + " for recording " + recordingId;
                controlSession.sendErrorResponse(correlationId, msg);
                return;
            }
            stopPosition = limitPosition;
            maxLength = Long.MAX_VALUE - replayPosition;
        }
        else
        {
            stopPosition = recordingSummary.stopPosition;
            maxLength = stopPosition - replayPosition;
        }

        final long replayLength;
        if (AeronArchive.REPLAY_ALL_AND_FOLLOW == length)
        {
            replayLength = maxLength;
        }
        else if (AeronArchive.REPLAY_ALL_AND_STOP == length)
        {
            replayLength = (stopPosition - replayPosition);
            if (0 == replayLength)
            {
                final String msg =
                    "When replaying and stopping the replay length must be non-zero, recordingId=" + recordingId;
                controlSession.sendErrorResponse(correlationId, EMPTY_RECORDING, msg);
            }
        }
        else
        {
            replayLength = min(length, maxLength);
        }

        if (replayLength < 0)
        {
            final String msg = "replay length must be positive: replayLength=" + replayLength + ", length=" + length +
                ", stopPosition=" + stopPosition + ", replayPosition=" + replayPosition + " for recording " +
                recordingId;
            controlSession.sendErrorResponse(correlationId, msg);
            return;
        }

        try
        {
            final ChannelUri channelUri = ChannelUri.parse(replayChannel);
            final ChannelUriStringBuilder channelBuilder = strippedChannelBuilder(channelUri)
                .initialPosition(replayPosition, recordingSummary.initialTermId, recordingSummary.termBufferLength)
                .eos(channelUri)
                .sparse(channelUri)
                .mtu(recordingSummary.mtuLength);

            final String lingerValue = channelUri.get(CommonContext.LINGER_PARAM_NAME);
            channelBuilder.linger(null != lingerValue ? Long.parseLong(lingerValue) : ctx.replayLingerTimeoutNs());

            addSession(new CreateReplayPublicationSession(
                correlationId,
                recordingId,
                replayPosition,
                replayLength,
                startPosition,
                stopPosition,
                recordingSummary.segmentFileLength,
                recordingSummary.termBufferLength,
                recordingSummary.streamId,
                aeron.asyncAddExclusivePublication(channelBuilder.build(), replayStreamId),
                fileIoMaxLength,
                replayLimitPositionCounter,
                aeron,
                controlSession,
                this));
        }
        catch (final Exception ex)
        {
            final String msg = "failed to process replayChannel - " + ex.getMessage();
            controlSession.sendErrorResponse(correlationId, msg);
            throw ex;
        }
    }

    void newReplaySession(
        final long correlationId,
        final long recordingId,
        final long replayPosition,
        final long replayLength,
        final long startPosition,
        final long stopPosition,
        final int segmentFileLength,
        final int termBufferLength,
        final int streamId,
        final int fileIoMaxLength,
        final ControlSession controlSession,
        final Counter replayLimitPosition,
        final ExclusivePublication replayPublication)
    {
        final long replaySessionId = ((long)(replayId++) << 32) | (replayPublication.sessionId() & 0xFFFF_FFFFL);

        final UnsafeBuffer replayBuffer;
        if (0 < fileIoMaxLength && fileIoMaxLength < ctx.replayBuffer().capacity())
        {
            replayBuffer = new UnsafeBuffer(ctx.replayBuffer(), 0, fileIoMaxLength);
        }
        else
        {
            replayBuffer = ctx.replayBuffer();
        }

        final ReplaySession replaySession = new ReplaySession(
            correlationId,
            recordingId,
            replayPosition,
            replayLength,
            startPosition,
            stopPosition,
            segmentFileLength,
            termBufferLength,
            streamId,
            replaySessionId,
            connectTimeoutMs,
            controlSession,
            replayBuffer,
            archiveDir,
            cachedEpochClock,
            nanoClock,
            replayPublication,
            aeron.countersReader(),
            replayLimitPosition,
            ctx.replayChecksum(),
            replayer);

        replaySessionByIdMap.put(replaySessionId, replaySession);
        replayer.addSession(replaySession);
        ctx.replaySessionCounter().incrementRelease();
    }

    void startBoundedReplay(
        final long correlationId,
        final long recordingId,
        final long position,
        final long length,
        final int limitCounterId,
        final int fileIoMaxLength,
        final int replayStreamId,
        final String replayChannel,
        final ControlSession controlSession)
    {
        Counter replayLimitCounter = counterByIdMap.get(limitCounterId);
        if (null == replayLimitCounter)
        {
            try
            {
                replayLimitCounter = new Counter(aeron.countersReader(), NULL_VALUE, limitCounterId);
            }
            catch (final Exception ex)
            {
                final String msg = "unable to create replay limit counter id= " + limitCounterId +
                    " because of: " + ex.getMessage();
                controlSession.sendErrorResponse(correlationId, GENERIC, msg);
                return;
            }

            counterByIdMap.put(limitCounterId, replayLimitCounter);
        }

        startReplay(
            correlationId,
            recordingId,
            position,
            length,
            fileIoMaxLength,
            replayStreamId,
            replayChannel,
            replayLimitCounter,
            controlSession);
    }

    void stopReplay(final long correlationId, final long replaySessionId, final ControlSession controlSession)
    {
        final ReplaySession replaySession = replaySessionByIdMap.get(replaySessionId);
        if (null != replaySession)
        {
            replaySession.abort("stop replay");
        }

        controlSession.sendOkResponse(correlationId);
    }

    void stopAllReplays(final long correlationId, final long recordingId, final ControlSession controlSession)
    {
        for (final ReplaySession replaySession : replaySessionByIdMap.values())
        {
            if (NULL_VALUE == recordingId || replaySession.recordingId() == recordingId)
            {
                replaySession.abort("stop all replays");
            }
        }

        controlSession.sendOkResponse(correlationId);
    }

    /* Returns a Subscription or a String error message indicating why the subscription couldn't be acquired */
    Object extendRecording(
        final long correlationId,
        final long recordingId,
        final int streamId,
        final SourceLocation sourceLocation,
        final boolean autoStop,
        final String originalChannel,
        final ControlSession controlSession)
    {
        if (recordingSessionByIdMap.size() >= ctx.maxConcurrentRecordings())
        {
            final String msg = "max concurrent recordings reached at " + ctx.maxConcurrentRecordings();
            controlSession.sendErrorResponse(correlationId, MAX_RECORDINGS, msg);
            return msg;
        }

        if (!catalog.hasRecording(recordingId))
        {
            final String msg = "unknown recording id: " + recordingId;
            controlSession.sendErrorResponse(correlationId, UNKNOWN_RECORDING, msg);
            return msg;
        }

        catalog.recordingSummary(recordingId, recordingSummary);
        if (streamId != recordingSummary.streamId)
        {
            final String msg = "cannot extend recording " + recordingSummary.recordingId +
                " with streamId=" + streamId + " for existing streamId=" + recordingSummary.streamId;
            controlSession.sendErrorResponse(correlationId, UNKNOWN_RECORDING, msg);
            return msg;
        }

        if (recordingSessionByIdMap.containsKey(recordingId))
        {
            final String msg = "cannot extend active recording " + recordingId;
            controlSession.sendErrorResponse(correlationId, ACTIVE_RECORDING, msg);
            return msg;
        }

        final DeleteSegmentsSession deleteSegmentsSession = deleteSegmentsSessionByIdMap.get(recordingId);
        if (null != deleteSegmentsSession && deleteSegmentsSession.maxDeletePosition() >= recordingSummary.stopPosition)
        {
            final String msg = "cannot extend recording " + recordingId +
                " due to an outstanding delete operation";
            controlSession.sendErrorResponse(correlationId, msg);
            return msg;
        }

        final String lowStorageSpaceMsg = isLowStorageSpace(correlationId, controlSession);
        if (null != lowStorageSpaceMsg)
        {
            return lowStorageSpaceMsg;
        }

        try
        {
            final ChannelUri channelUri = ChannelUri.parse(originalChannel);
            final String key = makeKey(streamId, channelUri);
            final Subscription oldSubscription = recordingSubscriptionByKeyMap.get(key);

            if (null == oldSubscription)
            {
                final String strippedChannel = strippedChannelBuilder(channelUri).build();
                final String channel = originalChannel.contains("udp") && sourceLocation == SourceLocation.LOCAL ?
                    SPY_PREFIX + strippedChannel : strippedChannel;

                final AvailableImageHandler handler = (image) -> taskQueue.addLast(() -> extendRecordingSession(
                    controlSession, correlationId, recordingId, strippedChannel, originalChannel, image, autoStop));

                final Subscription subscription = aeron.addSubscription(channel, streamId, handler, null);

                recordingSubscriptionByKeyMap.put(key, subscription);
                subscriptionRefCountMap.incrementAndGet(subscription.registrationId());
                controlSession.sendOkResponse(correlationId, subscription.registrationId());

                return subscription;
            }
            else
            {
                final String msg = "recording exists for streamId=" + streamId + " channel=" + originalChannel;
                controlSession.sendErrorResponse(correlationId, ACTIVE_SUBSCRIPTION, msg);
                return msg;
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
            controlSession.sendErrorResponse(correlationId, ex.getMessage());
            return ex.getMessage();
        }
    }

    void getStartPosition(final long correlationId, final long recordingId, final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession))
        {
            controlSession.sendOkResponse(correlationId, catalog.startPosition(recordingId));
        }
    }

    void getRecordingPosition(final long correlationId, final long recordingId, final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession))
        {
            final RecordingSession recordingSession = recordingSessionByIdMap.get(recordingId);
            final long position = null == recordingSession ? NULL_POSITION : recordingSession.recordingPosition().get();

            controlSession.sendOkResponse(correlationId, position);
        }
    }

    void getStopPosition(final long correlationId, final long recordingId, final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession))
        {
            controlSession.sendOkResponse(correlationId, catalog.stopPosition(recordingId));
        }
    }

    void getMaxRecordedPosition(
        final long correlationId, final long recordingId, final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession))
        {
            final RecordingSession recordingSession = recordingSessionByIdMap.get(recordingId);
            final long maxRecordedPosition = null != recordingSession ?
                recordingSession.recordingPosition().get() : catalog.stopPosition(recordingId);

            controlSession.sendOkResponse(correlationId, maxRecordedPosition);
        }
    }

    void truncateRecording(
        final long correlationId, final long recordingId, final long position, final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession) &&
            isValidTruncate(correlationId, controlSession, recordingId, position) &&
            isDeleteAllowed(recordingId, correlationId, controlSession))
        {
            final long stopPosition = recordingSummary.stopPosition;
            final int segmentLength = recordingSummary.segmentFileLength;
            final int termLength = recordingSummary.termBufferLength;
            final long startPosition = recordingSummary.startPosition;
            final long segmentBasePosition = segmentFileBasePosition(
                startPosition, position, termLength, segmentLength);
            final int segmentOffset = (int)(position - segmentBasePosition);

            catalog.stopPosition(recordingId, position);

            final ArrayDeque<String> files = new ArrayDeque<>();
            if (startPosition == position)
            {
                listSegmentFiles(recordingId, files::addLast);
            }
            else
            {
                if (segmentOffset > 0)
                {
                    if (stopPosition != position)
                    {
                        final File file = new File(archiveDir, segmentFileName(recordingId, segmentBasePosition));
                        if (!eraseRemainingSegment(
                            correlationId, controlSession, position, segmentLength, segmentOffset, termLength, file))
                        {
                            return;
                        }
                    }
                }
                else
                {
                    files.addLast(segmentFileName(recordingId, segmentBasePosition));
                }

                for (long p = segmentBasePosition + segmentLength; p <= stopPosition; p += segmentLength)
                {
                    files.addLast(segmentFileName(recordingId, p));
                }
            }

            deleteSegments(correlationId, recordingId, controlSession, files);
        }
    }

    void purgeRecording(final long correlationId, final long recordingId, final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession) &&
            isValidPurge(correlationId, controlSession, recordingId) &&
            isDeleteAllowed(recordingId, correlationId, controlSession))
        {
            catalog.changeState(recordingId, DELETED);

            final ArrayDeque<String> files = new ArrayDeque<>();
            listSegmentFiles(recordingId, files::addLast);

            deleteSegments(correlationId, recordingId, controlSession, files);
        }
    }

    void listRecordingSubscriptions(
        final long correlationId,
        final int pseudoIndex,
        final int subscriptionCount,
        final boolean applyStreamId,
        final int streamId,
        final String channelFragment,
        final ControlSession controlSession)
    {
        if (controlSession.hasActiveListing())
        {
            final String msg = "active listing already in progress";
            controlSession.sendErrorResponse(correlationId, ACTIVE_LISTING, msg);
        }
        else if (pseudoIndex < 0 || pseudoIndex >= recordingSubscriptionByKeyMap.size() || subscriptionCount <= 0)
        {
            controlSession.sendSubscriptionUnknown(correlationId);
        }
        else
        {
            final ListRecordingSubscriptionsSession session = new ListRecordingSubscriptionsSession(
                recordingSubscriptionByKeyMap,
                pseudoIndex,
                subscriptionCount,
                streamId,
                applyStreamId,
                channelFragment,
                correlationId,
                controlSession
            );
            addSession(session);
            controlSession.activeListing(session);
        }
    }

    void stopRecordingByIdentity(final long correlationId, final long recordingId, final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession))
        {
            int found = 0;
            final RecordingSession recordingSession = recordingSessionByIdMap.get(recordingId);

            if (null != recordingSession)
            {
                recordingSession.abort("stop recording by identity");

                final long subscriptionId = recordingSession.subscription().registrationId();
                final Subscription subscription = removeRecordingSubscription(subscriptionId);

                if (null != subscription)
                {
                    found = 1;
                    if (0 == subscriptionRefCountMap.decrementAndGet(subscriptionId))
                    {
                        subscription.close();
                    }
                }
            }

            controlSession.sendOkResponse(correlationId, found);
        }
    }

    void closeRecordingSession(final RecordingSession session)
    {
        if (isAbort)
        {
            session.abortClose();
        }
        else
        {
            final Subscription subscription = session.subscription();
            final long position = session.recordedPosition();
            final long recordingId = session.sessionId();
            final long subscriptionId = subscription.registrationId();

            try
            {
                catalog.recordingStopped(recordingId, position, epochClock.time());
                recordingSessionByIdMap.remove(recordingId);
                session.sendPendingError();
                session.controlSession().sendSignal(
                    session.correlationId(),
                    recordingId,
                    subscriptionId,
                    position,
                    RecordingSignal.STOP);
            }
            catch (final Exception ex)
            {
                errorHandler.onError(ex);
            }

            if (subscriptionRefCountMap.decrementAndGet(subscriptionId) <= 0 || session.isAutoStop())
            {
                closeAndRemoveRecordingSubscription(subscription, "close recording session");
            }
            closeSession(session);
            ctx.recordingSessionCounter().decrementRelease();
        }
    }

    void closeReplaySession(final ReplaySession session)
    {
        if (!isAbort)
        {
            try
            {
                session.sendPendingError();
            }
            catch (final Exception ex)
            {
                errorHandler.onError(ex);
            }
        }

        replaySessionByIdMap.remove(session.sessionId());
        closeSession(session);
        ctx.replaySessionCounter().decrementRelease();
    }

    void replicate(
        final long correlationId,
        final long srcRecordingId,
        final long dstRecordingId,
        final long stopPosition,
        final long channelTagId,
        final long subscriptionTagId,
        final int srcControlStreamId,
        final String srcControlChannel,
        final String liveDestination,
        final String replicationChannel,
        final int fileIoMaxLength,
        final int replicationSessionId,
        final byte[] encodedCredentials,
        final String srcResponseChannel,
        final ControlSession controlSession)
    {
        final String replicationChannel0 = Strings.isEmpty(replicationChannel) ?
            ctx.replicationChannel() : replicationChannel;

        final ChannelUri replicationChannelUri = ChannelUri.parse(replicationChannel0);
        if (replicationChannelUri.hasControlModeResponse() && !Strings.isEmpty(liveDestination))
        {
            final String msg = "response channels can't be used with live destinations";
            controlSession.sendErrorResponse(correlationId, GENERIC, msg);
            return;
        }

        if (replicationChannelUri.hasControlModeResponse() &&
            (NULL_VALUE != channelTagId || NULL_VALUE != subscriptionTagId))
        {
            final String msg = "response channels can't be used with tagged replication";
            controlSession.sendErrorResponse(correlationId, GENERIC, msg);
            return;
        }

        final boolean hasRecording = catalog.hasRecording(dstRecordingId);
        if (NULL_VALUE != dstRecordingId && !hasRecording)
        {
            final String msg = "unknown destination recording id " + dstRecordingId;
            controlSession.sendErrorResponse(correlationId, UNKNOWN_RECORDING, msg);
            return;
        }

        if (hasRecording)
        {
            catalog.recordingSummary(dstRecordingId, recordingSummary);

            if (NULL_POSITION == recordingSummary.stopPosition || recordingSessionByIdMap.containsKey(dstRecordingId))
            {
                final String msg = "cannot replicate to active recording " + dstRecordingId;
                controlSession.sendErrorResponse(correlationId, ACTIVE_RECORDING, msg);
                return;
            }
        }

        final AeronArchive.Context remoteArchiveContext = ctx.archiveClientContext().clone()
            .controlRequestChannel(srcControlChannel)
            .controlRequestStreamId(srcControlStreamId);

        if (null != encodedCredentials && 0 < encodedCredentials.length)
        {
            remoteArchiveContext.credentialsSupplier(new ReplicationCredentialsSupplier(encodedCredentials));
        }

        if (!Strings.isEmpty(srcResponseChannel))
        {
            remoteArchiveContext.controlResponseChannel(srcResponseChannel);
        }

        final long replicationId = nextSessionId++;
        remoteArchiveContext.clientName(remoteArchiveContext.clientName() + " replicationSessionId=" + replicationId);
        final ReplicationSession replicationSession = new ReplicationSession(
            srcRecordingId,
            dstRecordingId,
            channelTagId,
            subscriptionTagId,
            replicationId,
            stopPosition,
            liveDestination,
            replicationChannel0,
            fileIoMaxLength,
            replicationSessionId,
            hasRecording ? recordingSummary : null,
            remoteArchiveContext,
            cachedEpochClock,
            catalog,
            controlSession);

        replicationSessionByIdMap.put(replicationId, replicationSession);
        addSession(replicationSession);

        controlSession.sendOkResponse(correlationId, replicationId);
    }

    void stopReplication(final long correlationId, final long replicationId, final ControlSession controlSession)
    {
        final ReplicationSession session = replicationSessionByIdMap.remove(replicationId);
        if (null == session)
        {
            final String msg = "unknown replication id " + replicationId;
            controlSession.sendErrorResponse(correlationId, UNKNOWN_REPLICATION, msg);
        }
        else
        {
            session.abort("stop replication");
            controlSession.sendOkResponse(correlationId);
        }
    }

    void detachSegments(
        final long correlationId,
        final long recordingId,
        final long newStartPosition,
        final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession) &&
            isValidDetach(correlationId, controlSession, recordingId, newStartPosition))
        {
            catalog.startPosition(recordingId, newStartPosition);
            controlSession.sendOkResponse(correlationId);
        }
    }

    void deleteDetachedSegments(final long correlationId, final long recordingId, final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession) &&
            isDeleteAllowed(recordingId, correlationId, controlSession))
        {
            final MutableLong minPosition = new MutableLong(Long.MAX_VALUE);
            final int prefixLength = digitCount(recordingId) + 1;
            listSegmentFiles(
                recordingId,
                (segmentFile) ->
                {
                    final int dotIndex = segmentFile.indexOf('.');
                    final long filePosition =
                        AsciiEncoding.parseLongAscii(segmentFile, prefixLength, dotIndex - prefixLength);
                    minPosition.set(Math.min(minPosition.get(), filePosition));
                });

            final ArrayDeque<String> files = new ArrayDeque<>();
            if (Long.MAX_VALUE != minPosition.get())
            {
                findDetachedSegments(recordingId, files, minPosition.get());
            }
            deleteSegments(correlationId, recordingId, controlSession, files);
        }
    }

    void purgeSegments(
        final long correlationId,
        final long recordingId,
        final long newStartPosition,
        final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession) &&
            isValidDetach(correlationId, controlSession, recordingId, newStartPosition) &&
            isDeleteAllowed(recordingId, correlationId, controlSession))
        {
            catalog.recordingSummary(recordingId, recordingSummary);
            final long oldStartPosition = recordingSummary.startPosition;

            catalog.startPosition(recordingId, newStartPosition);

            final ArrayDeque<String> files = new ArrayDeque<>();
            findDetachedSegments(recordingId, files, oldStartPosition);
            deleteSegments(correlationId, recordingId, controlSession, files);
        }
    }

    void attachSegments(final long correlationId, final long recordingId, final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession))
        {
            catalog.recordingSummary(recordingId, recordingSummary);
            final int segmentLength = recordingSummary.segmentFileLength;
            final int termLength = recordingSummary.termBufferLength;
            final int bitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);
            final int streamId = recordingSummary.streamId;
            long position = recordingSummary.startPosition - segmentLength;
            long count = 0;

            while (position >= 0)
            {
                final File file = new File(archiveDir, segmentFileName(recordingId, position));
                if (!file.exists())
                {
                    break;
                }

                final long fileLength = file.length();
                if (fileLength != segmentLength)
                {
                    final String msg = "fileLength=" + fileLength + " not equal to segmentLength=" + segmentLength;
                    controlSession.sendErrorResponse(correlationId, msg);
                    return;
                }

                try (FileChannel fileChannel = FileChannel.open(file.toPath(), FILE_OPTIONS))
                {
                    final int termCount = (int)(position >> bitsToShift);
                    final int termId = recordingSummary.initialTermId + termCount;
                    final int termOffset = findTermOffsetForStart(
                        correlationId, controlSession, file, fileChannel, streamId, termId, termLength);

                    if (termOffset < 0)
                    {
                        return;
                    }
                    else if (0 == termOffset)
                    {
                        catalog.startPosition(recordingId, position);
                        count += 1;
                        position -= segmentLength;
                    }
                    else
                    {
                        catalog.startPosition(recordingId, position + termOffset);
                        count += 1;
                        break;
                    }
                }
                catch (final IOException ex)
                {
                    controlSession.sendErrorResponse(correlationId, ex.getMessage());
                    LangUtil.rethrowUnchecked(ex);
                }
            }

            controlSession.sendOkResponse(correlationId, count);
        }
    }

    void migrateSegments(
        final long correlationId,
        final long srcRecordingId,
        final long dstRecordingId,
        final ControlSession controlSession)
    {
        if (hasRecording(srcRecordingId, correlationId, controlSession) &&
            hasRecording(dstRecordingId, correlationId, controlSession))
        {
            final RecordingSummary srcSummary = catalog.recordingSummary(srcRecordingId, recordingSummary);
            final RecordingSummary dstSummary = catalog.recordingSummary(dstRecordingId, new RecordingSummary());

            if (isActiveRecording(controlSession, correlationId, srcSummary) ||
                !hasMatchingStreamParameters(controlSession, correlationId, srcSummary, dstSummary))
            {
                return;
            }

            final long joinPosition;

            if (srcSummary.stopPosition == dstSummary.startPosition)
            {
                joinPosition = srcSummary.stopPosition;
            }
            else if (srcSummary.startPosition == dstSummary.stopPosition)
            {
                joinPosition = srcSummary.startPosition;
            }
            else
            {
                final String msg = "invalid migrate: src and dst are not contiguous" +
                    " srcStartPosition=" + srcSummary.startPosition +
                    " srcStopPosition=" + srcSummary.stopPosition +
                    " dstStartPosition=" + dstSummary.startPosition +
                    " dstStopPosition=" + dstSummary.stopPosition;
                controlSession.sendErrorResponse(correlationId, msg);
                return;
            }

            if (isJoinPositionSegmentUnaligned(controlSession, correlationId, "src", srcSummary, joinPosition) ||
                isJoinPositionSegmentUnaligned(controlSession, correlationId, "dst", dstSummary, joinPosition))
            {
                return;
            }

            final ArrayDeque<String> emptyFollowingSrcSegment = new ArrayDeque<>();

            final long movedSegmentCount = moveAllSegments(
                controlSession,
                correlationId,
                srcRecordingId,
                dstRecordingId,
                srcSummary,
                emptyFollowingSrcSegment);

            if (movedSegmentCount >= 0)
            {
                final int toBeDeletedSegmentCount = addDeleteSegmentsSession(
                    correlationId, srcRecordingId, controlSession, emptyFollowingSrcSegment);

                if (toBeDeletedSegmentCount >= 0)
                {
                    if (srcSummary.stopPosition == dstSummary.startPosition)
                    {
                        catalog.startPosition(dstRecordingId, srcSummary.startPosition);
                    }
                    else
                    {
                        catalog.stopPosition(dstRecordingId, srcSummary.stopPosition);
                    }

                    catalog.stopPosition(srcRecordingId, srcSummary.startPosition);

                    controlSession.sendOkResponse(correlationId, movedSegmentCount);

                    final boolean hasSegmentsToDelete = toBeDeletedSegmentCount > 0;
                    if (movedSegmentCount > 0 && !hasSegmentsToDelete)
                    {
                        controlSession.sendSignal(
                            correlationId, srcRecordingId, Aeron.NULL_VALUE, Aeron.NULL_VALUE, RecordingSignal.DELETE);
                    }
                }
            }
        }
    }

    void removeReplicationSession(final ReplicationSession replicationSession)
    {
        replicationSessionByIdMap.remove(replicationSession.sessionId());
    }

    void removeDeleteSegmentsSession(final DeleteSegmentsSession deleteSegmentsSession)
    {
        deleteSegmentsSessionByIdMap.remove(deleteSegmentsSession.sessionId());
    }

    private void findDetachedSegments(
        final long recordingId, final ArrayDeque<String> files, final long prevStartPosition)
    {
        catalog.recordingSummary(recordingId, recordingSummary);
        final int segmentFileLength = recordingSummary.segmentFileLength;

        final long prevSegmentFilePosition = segmentFileBasePosition(
            prevStartPosition, prevStartPosition, recordingSummary.termBufferLength, segmentFileLength);

        final long startSegmentFilePosition = segmentFileBasePosition(
            recordingSummary.startPosition,
            recordingSummary.startPosition,
            recordingSummary.termBufferLength,
            segmentFileLength);

        long filenamePosition = startSegmentFilePosition - segmentFileLength;

        while (filenamePosition >= prevSegmentFilePosition)
        {
            final String segmentFileName = segmentFileName(recordingId, filenamePosition);
            files.addFirst(segmentFileName);
            filenamePosition -= segmentFileLength;
        }
    }

    private int addDeleteSegmentsSession(
        final long correlationId,
        final long recordingId,
        final ControlSession controlSession,
        final ArrayDeque<String> files)
    {
        if (files.isEmpty())
        {
            return 0;
        }

        final ArrayDeque<File> deleteList = new ArrayDeque<>(files.size());
        for (final String name : files)
        {
            deleteList.add(new File(archiveDir, name));
        }

        addSession(new DeleteSegmentsSession(
            recordingId, correlationId, deleteList, controlSession, errorHandler));

        return files.size();
    }

    private void abortRecordingSessionAndCloseSubscription(final Subscription subscription)
    {
        for (final RecordingSession session : recordingSessionByIdMap.values())
        {
            if (subscription == session.subscription())
            {
                session.abort("stop recording");
            }
        }

        if (0 == subscriptionRefCountMap.decrementAndGet(subscription.registrationId()))
        {
            subscription.close();
        }
    }

    private int findTermOffsetForStart(
        final long correlationId,
        final ControlSession controlSession,
        final File file,
        final FileChannel fileChannel,
        final int streamId,
        final int termId,
        final int termLength)
        throws IOException
    {
        int termOffset = 0;
        final UnsafeBuffer buffer = ctx.dataBuffer();
        final ByteBuffer byteBuffer = buffer.byteBuffer();
        byteBuffer.clear().limit(HEADER_LENGTH);

        if (HEADER_LENGTH != fileChannel.read(byteBuffer, 0))
        {
            final String msg = "failed to read segment file";
            controlSession.sendErrorResponse(correlationId, msg);
            return termOffset;
        }

        final int fragmentLength = fragmentLength(buffer, termOffset);
        if (fragmentLength <= 0)
        {
            boolean found = false;
            do
            {
                byteBuffer.clear().limit(min(termLength - termOffset, byteBuffer.capacity()));
                final int bytesRead = fileChannel.read(byteBuffer, termOffset);
                if (bytesRead <= 0)
                {
                    final String msg = "read failed on " + file;
                    controlSession.sendErrorResponse(correlationId, msg);
                    return NULL_VALUE;
                }

                final int limit = bytesRead - (bytesRead & (FRAME_ALIGNMENT - 1));
                int offset = 0;
                while (offset < limit)
                {
                    if (fragmentLength(buffer, offset) > 0)
                    {
                        found = true;
                        break;
                    }

                    offset += FRAME_ALIGNMENT;
                }

                termOffset += offset;
            }
            while (termOffset < termLength && !found);
        }

        if (termOffset >= termLength)
        {
            final String msg = "fragment not found in first term of segment " + file;
            controlSession.sendErrorResponse(correlationId, msg);
            return NULL_VALUE;
        }

        final int fileTermId = termId(buffer, termOffset);
        if (fileTermId != termId)
        {
            final String msg = "term id does not match: actual=" + fileTermId + " expected=" + termId;
            controlSession.sendErrorResponse(correlationId, msg);
            return NULL_VALUE;
        }

        final int fileStreamId = streamId(buffer, termOffset);
        if (fileStreamId != streamId)
        {
            final String msg = "stream id does not match: actual=" + fileStreamId + " expected=" + streamId;
            controlSession.sendErrorResponse(correlationId, msg);
            return NULL_VALUE;
        }

        return termOffset;
    }

    private int runTasks(final ArrayDeque<Runnable> taskQueue)
    {
        int workCount = 0;

        Runnable runnable;
        while (null != (runnable = taskQueue.pollFirst()))
        {
            runnable.run();
            workCount++;
        }

        return workCount;
    }

    static ChannelUriStringBuilder strippedChannelBuilder(final ChannelUri channelUri)
    {
        return new ChannelUriStringBuilder()
            .media(channelUri)
            .endpoint(channelUri)
            .networkInterface(channelUri)
            .controlEndpoint(channelUri)
            .controlMode(channelUri)
            .tags(channelUri)
            .rejoin(channelUri)
            .group(channelUri)
            .tether(channelUri)
            .flowControl(channelUri)
            .groupTag(channelUri)
            .congestionControl(channelUri)
            .socketRcvbufLength(channelUri)
            .socketSndbufLength(channelUri)
            .receiverWindowLength(channelUri)
            .channelSendTimestampOffset(channelUri)
            .channelReceiveTimestampOffset(channelUri)
            .mediaReceiveTimestampOffset(channelUri)
            .sessionId(channelUri)
            .alias(channelUri)
            .responseCorrelationId(channelUri)
            .responseEndpoint(channelUri)
            .ttl(channelUri)
            .streamId(channelUri);
    }

    private static String makeKey(final int streamId, final ChannelUri channelUri)
    {
        final StringBuilder sb = new StringBuilder();

        sb.append(streamId).append(':').append(channelUri.media()).append('?');

        final String endpointStr = channelUri.get(CommonContext.ENDPOINT_PARAM_NAME);
        if (null != endpointStr)
        {
            sb.append(CommonContext.ENDPOINT_PARAM_NAME).append('=').append(endpointStr).append('|');
        }

        final String interfaceStr = channelUri.get(CommonContext.INTERFACE_PARAM_NAME);
        if (null != interfaceStr)
        {
            sb.append(CommonContext.INTERFACE_PARAM_NAME).append('=').append(interfaceStr).append('|');
        }

        final String controlStr = channelUri.get(CommonContext.MDC_CONTROL_PARAM_NAME);
        if (null != controlStr)
        {
            sb.append(CommonContext.MDC_CONTROL_PARAM_NAME).append('=').append(controlStr).append('|');
        }

        final String sessionIdStr = channelUri.get(CommonContext.SESSION_ID_PARAM_NAME);
        if (null != sessionIdStr)
        {
            sb.append(CommonContext.SESSION_ID_PARAM_NAME).append('=').append(sessionIdStr).append('|');
        }

        final String tagsStr = channelUri.get(CommonContext.TAGS_PARAM_NAME);
        if (null != tagsStr)
        {
            sb.append(CommonContext.TAGS_PARAM_NAME).append('=').append(tagsStr).append('|');
        }

        sb.setLength(sb.length() - 1);

        return sb.toString();
    }

    private boolean hasRecording(final long recordingId, final long correlationId, final ControlSession session)
    {
        if (!catalog.hasRecording(recordingId))
        {
            final String msg = "unknown recording id: " + recordingId;
            session.sendErrorResponse(correlationId, UNKNOWN_RECORDING, msg);
            return false;
        }

        return true;
    }

    private boolean isDeleteAllowed(
        final long recordingId, final long correlationId, final ControlSession controlSession)
    {
        if (deleteSegmentsSessionByIdMap.containsKey(recordingId))
        {
            final String msg = "another delete operation in progress for recording id: " + recordingId;
            controlSession.sendErrorResponse(correlationId, msg);
            return false;
        }
        return true;
    }

    private void listSegmentFiles(final long recordingId, final Consumer<String> segmentFileConsumer)
    {
        final String prefix = recordingId + "-";
        final String[] recordingFiles = archiveDir.list();
        if (null != recordingFiles)
        {
            for (final String name : recordingFiles)
            {
                if (name.startsWith(prefix) &&
                    (name.endsWith(RECORDING_SEGMENT_SUFFIX) || name.endsWith(DELETE_SUFFIX)))
                {
                    segmentFileConsumer.accept(name);
                }
            }
        }
    }

    private void startRecordingSession(
        final ControlSession controlSession,
        final long correlationId,
        final String strippedChannel,
        final String originalChannel,
        final Image image,
        final boolean autoStop)
    {
        final int sessionId = image.sessionId();
        final int streamId = image.subscription().streamId();
        final String sourceIdentity = image.sourceIdentity();
        final int termBufferLength = image.termBufferLength();
        final int mtuLength = image.mtuLength();
        final int initialTermId = image.initialTermId();
        final long startPosition = image.joinPosition();
        final int segmentFileLength = max(ctx.segmentFileLength(), termBufferLength);

        final long recordingId = catalog.addNewRecording(
            startPosition,
            cachedEpochClock.time(),
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity);

        final Counter position = RecordingPos.allocate(
            aeron,
            counterMetadataBuffer,
            ctx.archiveId(),
            recordingId,
            sessionId,
            streamId,
            strippedChannel,
            sourceIdentity);
        position.setRelease(startPosition);

        final RecordingSession session = new RecordingSession(
            correlationId,
            recordingId,
            startPosition,
            segmentFileLength,
            originalChannel,
            recordingEventsProxy,
            image,
            position,
            ctx,
            controlSession,
            autoStop,
            recorder);

        controlSession.sendSignal(
            correlationId,
            recordingId,
            image.subscription().registrationId(),
            image.joinPosition(),
            RecordingSignal.START);

        subscriptionRefCountMap.incrementAndGet(image.subscription().registrationId());
        recordingSessionByIdMap.put(recordingId, session);
        recorder.addSession(session);
        ctx.recordingSessionCounter().incrementRelease();
    }

    private void extendRecordingSession(
        final ControlSession controlSession,
        final long correlationId,
        final long recordingId,
        final String strippedChannel,
        final String originalChannel,
        final Image image,
        final boolean autoStop)
    {
        final long subscriptionId = image.subscription().registrationId();
        try
        {
            if (recordingSessionByIdMap.containsKey(recordingId))
            {
                final String msg = "cannot extend active recording " + recordingId +
                    " streamId=" + image.subscription().streamId() + " channel=" + originalChannel;
                controlSession.sendErrorResponse(correlationId, ACTIVE_RECORDING, msg);
                throw new ArchiveEvent(msg);
            }

            final DeleteSegmentsSession deleteSegmentsSession = deleteSegmentsSessionByIdMap.get(recordingId);
            if (null != deleteSegmentsSession &&
                deleteSegmentsSession.maxDeletePosition() >= recordingSummary.stopPosition)
            {
                final String msg = "cannot extend recording " + recordingId +
                    " streamId=" + image.subscription().streamId() + " channel=" + originalChannel +
                    " due to an outstanding delete operation";
                controlSession.sendErrorResponse(correlationId, GENERIC, msg);
                throw new ArchiveEvent(msg);
            }

            catalog.recordingSummary(recordingId, recordingSummary);
            validateImageForExtendRecording(correlationId, controlSession, image, recordingSummary);

            final Counter position = RecordingPos.allocate(
                aeron,
                counterMetadataBuffer,
                ctx.archiveId(),
                recordingId,
                image.sessionId(),
                image.subscription().streamId(),
                strippedChannel,
                image.sourceIdentity());

            position.setRelease(image.joinPosition());

            final RecordingSession session = new RecordingSession(
                correlationId,
                recordingId,
                recordingSummary.startPosition,
                recordingSummary.segmentFileLength,
                originalChannel,
                recordingEventsProxy,
                image,
                position,
                ctx,
                controlSession,
                autoStop,
                recorder);

            catalog.extendRecording(recordingId, controlSession.sessionId(), correlationId, image.sessionId());
            controlSession.sendSignal(
                correlationId, recordingId, subscriptionId, image.joinPosition(), RecordingSignal.EXTEND);

            subscriptionRefCountMap.incrementAndGet(subscriptionId);
            recordingSessionByIdMap.put(recordingId, session);
            recorder.addSession(session);
            ctx.recordingSessionCounter().incrementRelease();
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
            if (autoStop)
            {
                closeAndRemoveRecordingSubscription(image.subscription(), ex.getMessage());
            }
        }
    }

    private Subscription removeRecordingSubscription(final long subscriptionId)
    {
        final Iterator<Subscription> iter = recordingSubscriptionByKeyMap.values().iterator();
        while (iter.hasNext())
        {
            final Subscription subscription = iter.next();
            if (subscription.registrationId() == subscriptionId)
            {
                iter.remove();
                return subscription;
            }
        }

        return null;
    }

    private void validateImageForExtendRecording(
        final long correlationId,
        final ControlSession controlSession,
        final Image image,
        final RecordingSummary recordingSummary)
    {
        if (image.joinPosition() != recordingSummary.stopPosition)
        {
            final String msg = "cannot extend recording " + recordingSummary.recordingId +
                " image.joinPosition=" + image.joinPosition() + " != rec.stopPosition=" + recordingSummary.stopPosition;
            controlSession.sendErrorResponse(correlationId, INVALID_EXTENSION, msg);
            throw new ArchiveEvent(msg);
        }

        if (image.initialTermId() != recordingSummary.initialTermId)
        {
            final String msg = "cannot extend recording " + recordingSummary.recordingId +
                " image.initialTermId=" + image.initialTermId() +
                " != rec.initialTermId=" + recordingSummary.initialTermId;
            controlSession.sendErrorResponse(correlationId, INVALID_EXTENSION, msg);
            throw new ArchiveEvent(msg);
        }

        if (image.termBufferLength() != recordingSummary.termBufferLength)
        {
            final String msg = "cannot extend recording " + recordingSummary.recordingId +
                " image.termBufferLength=" + image.termBufferLength() +
                " != rec.termBufferLength=" + recordingSummary.termBufferLength;
            controlSession.sendErrorResponse(correlationId, INVALID_EXTENSION, msg);
            throw new ArchiveEvent(msg);
        }

        if (image.mtuLength() != recordingSummary.mtuLength)
        {
            final String msg = "cannot extend recording " + recordingSummary.recordingId +
                " image.mtuLength=" + image.mtuLength() + " != rec.mtuLength=" + recordingSummary.mtuLength;
            controlSession.sendErrorResponse(correlationId, INVALID_EXTENSION, msg);
            throw new ArchiveEvent(msg);
        }
    }

    private boolean isValidTruncate(
        final long correlationId, final ControlSession controlSession, final long recordingId, final long position)
    {
        for (final ReplaySession replaySession : replaySessionByIdMap.values())
        {
            if (replaySession.recordingId() == recordingId)
            {
                final String msg = "cannot truncate recording with active replay " + recordingId;
                controlSession.sendErrorResponse(correlationId, ACTIVE_RECORDING, msg);
                return false;
            }
        }

        catalog.recordingSummary(recordingId, recordingSummary);
        final long stopPosition = recordingSummary.stopPosition;
        final long startPosition = recordingSummary.startPosition;

        if (NULL_POSITION == stopPosition)
        {
            final String msg = "cannot truncate active recording";
            controlSession.sendErrorResponse(correlationId, ACTIVE_RECORDING, msg);
            return false;
        }

        if (position < startPosition || position > stopPosition || ((position & (FRAME_ALIGNMENT - 1)) != 0))
        {
            final String msg = "invalid position " + position +
                ": start=" + startPosition + " stop=" + stopPosition + " alignment=" + FRAME_ALIGNMENT;
            controlSession.sendErrorResponse(correlationId, msg);
            return false;
        }

        return true;
    }

    private boolean isValidPurge(final long correlationId, final ControlSession controlSession, final long recordingId)
    {
        for (final ReplaySession replaySession : replaySessionByIdMap.values())
        {
            if (replaySession.recordingId() == recordingId)
            {
                final String msg = "cannot purge recording with active replay " + recordingId;
                controlSession.sendErrorResponse(correlationId, ACTIVE_RECORDING, msg);
                return false;
            }
        }

        catalog.recordingSummary(recordingId, recordingSummary);

        final long stopPosition = recordingSummary.stopPosition;
        if (NULL_POSITION == stopPosition)
        {
            final String msg = "cannot purge active recording " + recordingId;
            controlSession.sendErrorResponse(correlationId, ACTIVE_RECORDING, msg);
            return false;
        }

        return true;
    }

    private boolean isInvalidReplayPosition(
        final long correlationId,
        final ControlSession controlSession,
        final long recordingId,
        final long position,
        final RecordingSummary recordingSummary)
    {
        if ((position & (FRAME_ALIGNMENT - 1)) != 0)
        {
            final String msg = "requested replay start position=" + position +
                " is not a multiple of FRAME_ALIGNMENT (" + FRAME_ALIGNMENT + ") for recording " + recordingId;
            controlSession.sendErrorResponse(correlationId, msg);
            return true;
        }

        final long startPosition = recordingSummary.startPosition;
        if (position - startPosition < 0)
        {
            final String msg = "requested replay start position=" + position +
                " is less than recording start position=" + startPosition + " for recording " + recordingId;
            controlSession.sendErrorResponse(correlationId, msg);
            return true;
        }

        final long stopPosition = recordingSummary.stopPosition;
        if (stopPosition != NULL_POSITION && position >= stopPosition)
        {
            final String msg = "requested replay start position=" + position +
                " must be less than highest recorded position=" + stopPosition + " for recording " + recordingId;
            controlSession.sendErrorResponse(correlationId, msg);
            return true;
        }

        return false;
    }

    private boolean isValidDetach(
        final long correlationId, final ControlSession controlSession, final long recordingId, final long position)
    {
        catalog.recordingSummary(recordingId, recordingSummary);

        final int segmentLength = recordingSummary.segmentFileLength;
        final long startPosition = recordingSummary.startPosition;
        final int termLength = recordingSummary.termBufferLength;
        final long lowerBound =
            segmentFileBasePosition(startPosition, startPosition, termLength, segmentLength) + segmentLength;

        if (position != segmentFileBasePosition(startPosition, position, termLength, segmentLength))
        {
            final String msg = "invalid segment start: newStartPosition=" + position;
            controlSession.sendErrorResponse(correlationId, msg);
            return false;
        }

        if (position < lowerBound)
        {
            final String msg = "invalid detach: newStartPosition=" + position + " lowerBound=" + lowerBound;
            controlSession.sendErrorResponse(correlationId, msg);
            return false;
        }

        final long stopPosition = recordingSummary.stopPosition;
        long endPosition = NULL_VALUE == stopPosition ?
            recordingSessionByIdMap.get(recordingId).recordedPosition() : stopPosition;
        endPosition = segmentFileBasePosition(startPosition, endPosition, termLength, segmentLength);

        if (position > endPosition)
        {
            final String msg = "invalid detach: in use, newStartPosition=" + position + " upperBound=" + endPosition;
            controlSession.sendErrorResponse(correlationId, msg);
            return false;
        }

        ReplaySession minReplaySession = null;
        for (final ReplaySession replaySession : replaySessionByIdMap.values())
        {
            final long replayPos = replaySession.segmentFileBasePosition();
            if (recordingId == replaySession.recordingId() && position > replayPos &&
                (null == minReplaySession || replayPos < minReplaySession.segmentFileBasePosition()))
            {
                minReplaySession = replaySession;
            }
        }

        if (null != minReplaySession)
        {
            final String msg = "invalid detach: replay in progress - " +
                " state=" + minReplaySession.state() +
                " newStartPosition=" + position +
                " upperBound=" + minReplaySession.segmentFileBasePosition() +
                " sessionId=" + (int)minReplaySession.sessionId() +
                " streamId=" + minReplaySession.replayStreamId() +
                " channel=" + minReplaySession.replayChannel();
            controlSession.sendErrorResponse(correlationId, msg);
            return false;
        }

        return true;
    }

    private boolean isJoinPositionSegmentUnaligned(
        final ControlSession controlSession,
        final long correlationId,
        final String label,
        final RecordingSummary recordingSummary,
        final long seamPosition
    )
    {
        final long segmentBasePosition = segmentFileBasePosition(
            recordingSummary.startPosition,
            seamPosition,
            recordingSummary.termBufferLength,
            recordingSummary.segmentFileLength);

        if (segmentBasePosition != seamPosition)
        {
            final String error = "invalid migrate: join position is not on segment boundary of " +
                label + " recording" +
                " seamPosition=" + seamPosition +
                " startPosition=" + recordingSummary.startPosition +
                " stopPosition=" + recordingSummary.stopPosition +
                " termBufferLength=" + recordingSummary.termBufferLength +
                " segmentFileLength=" + recordingSummary.segmentFileLength;
            controlSession.sendErrorResponse(correlationId, error);
            return true;
        }

        return false;
    }

    private boolean isActiveRecording(
        final ControlSession controlSession,
        final long correlationId,
        final RecordingSummary srcRecordingSummary)
    {
        final long srcStopPosition = srcRecordingSummary.stopPosition;
        if (NULL_POSITION == srcStopPosition)
        {
            final String message = "recording " + srcRecordingSummary.recordingId + " is still active";
            controlSession.sendErrorResponse(correlationId, message);
            return true;
        }
        return false;
    }

    private boolean hasMatchingStreamParameters(
        final ControlSession controlSession,
        final long correlationId,
        final RecordingSummary srcRecordingSummary,
        final RecordingSummary dstRecordingSummary)
    {
        final int srcSegmentFileLength = srcRecordingSummary.segmentFileLength;
        final int dstSegmentFileLength = dstRecordingSummary.segmentFileLength;
        if (dstSegmentFileLength != srcSegmentFileLength)
        {
            final String msg = "invalid migrate: srcSegmentFileLength=" + srcSegmentFileLength +
                " dstSegmentFileLength=" + dstSegmentFileLength;
            controlSession.sendErrorResponse(correlationId, msg);
            return false;
        }

        final int srcTermBufferLength = srcRecordingSummary.termBufferLength;
        final int dstTermBufferLength = dstRecordingSummary.termBufferLength;
        if (dstTermBufferLength != srcTermBufferLength)
        {
            final String msg = "invalid migrate: srcTermBufferLength=" + srcTermBufferLength +
                " dstTermBufferLength=" + dstTermBufferLength;
            controlSession.sendErrorResponse(correlationId, msg);
            return false;
        }

        final int srcInitialTermId = srcRecordingSummary.initialTermId;
        final int dstInitialTermId = dstRecordingSummary.initialTermId;
        if (dstInitialTermId != srcInitialTermId)
        {
            final String msg = "invalid migrate: srcInitialTermId=" + srcInitialTermId +
                " dstInitialTermId=" + dstInitialTermId;
            controlSession.sendErrorResponse(correlationId, msg);
            return false;
        }

        final int srcStreamId = srcRecordingSummary.streamId;
        final int dstStreamId = dstRecordingSummary.streamId;
        if (dstStreamId != srcStreamId)
        {
            final String msg = "invalid migrate: srcStreamId=" + srcStreamId +
                " dstStreamId=" + dstStreamId;
            controlSession.sendErrorResponse(correlationId, msg);
            return false;
        }

        final int srcMtuLength = srcRecordingSummary.mtuLength;
        final int dstMtuLength = dstRecordingSummary.mtuLength;
        if (dstMtuLength != srcMtuLength)
        {
            final String msg = "invalid migrate: srcMtuLength=" + srcMtuLength + " dstMtuLength=" + dstMtuLength;
            controlSession.sendErrorResponse(correlationId, msg);
            return false;
        }

        return true;
    }

    private long moveAllSegments(
        final ControlSession controlSession,
        final long correlationId,
        final long srcRecordingId,
        final long dstRecordingId,
        final RecordingSummary srcRecordingSummary,
        final ArrayDeque<String> emptyFollowingSrcSegment)
    {
        final long firstSegmentPos = segmentFileBasePosition(
            srcRecordingSummary.startPosition,
            srcRecordingSummary.startPosition,
            srcRecordingSummary.termBufferLength,
            srcRecordingSummary.segmentFileLength);

        final long lastSegmentPos = segmentFileBasePosition(
            srcRecordingSummary.startPosition,
            srcRecordingSummary.stopPosition,
            srcRecordingSummary.termBufferLength,
            srcRecordingSummary.segmentFileLength);

        long attachedSegmentCount = 0;

        final int segmentLength = srcRecordingSummary.segmentFileLength;

        for (long position = firstSegmentPos; position <= lastSegmentPos; position += segmentLength)
        {
            final String segmentFileName = segmentFileName(srcRecordingId, position);
            final File srcFile = new File(archiveDir, segmentFileName);
            final String dstFileName = segmentFileName(dstRecordingId, position);
            final File dstFile = new File(archiveDir, dstFileName);

            final boolean isEmptyFollowingSrcSegment = position == srcRecordingSummary.stopPosition;
            if (!isEmptyFollowingSrcSegment)
            {
                if (!srcFile.exists())
                {
                    final String msg = "missing src segment file " + srcFile;
                    controlSession.sendErrorResponse(correlationId, msg);
                    return -1L;
                }

                if (dstFile.exists())
                {
                    final String msg = "preexisting dst segment file " + dstFile;
                    controlSession.sendErrorResponse(correlationId, msg);
                    return -1L;
                }
            }
        }

        for (long position = firstSegmentPos; position <= lastSegmentPos; position += segmentLength)
        {
            final String segmentFileName = segmentFileName(srcRecordingId, position);
            final File srcFile = new File(archiveDir, segmentFileName);

            final boolean isEmptyFollowingSrcSegment = position == srcRecordingSummary.stopPosition;
            if (isEmptyFollowingSrcSegment)
            {
                emptyFollowingSrcSegment.addFirst(segmentFileName);
            }
            else
            {
                final String dstFileName = segmentFileName(dstRecordingId, position);
                final File dstFile = new File(archiveDir, dstFileName);
                if (!srcFile.renameTo(dstFile))
                {
                    final String msg = "failed to rename " + srcFile + " to " + dstFile;
                    controlSession.sendErrorResponse(correlationId, msg);
                    return -1L;
                }

                attachedSegmentCount++;
            }
        }

        return attachedSegmentCount;
    }

    private boolean eraseRemainingSegment(
        final long correlationId,
        final ControlSession controlSession,
        final long position,
        final int segmentLength,
        final int segmentOffset,
        final int termLength,
        final File file)
    {
        try (FileChannel channel = FileChannel.open(file.toPath(), FILE_OPTIONS))
        {
            final int termOffset = (int)(position & (termLength - 1));
            final int termCount = (int)(position >> LogBufferDescriptor.positionBitsToShift(termLength));
            final int termId = recordingSummary.initialTermId + termCount;
            final UnsafeBuffer dataBuffer = ctx.dataBuffer();

            if (ReplaySession.notHeaderAligned(
                channel, dataBuffer, segmentOffset, termOffset, termId, recordingSummary.streamId))
            {
                final String msg = position + " position not aligned to a data header";
                controlSession.sendErrorResponse(correlationId, msg);
                return false;
            }

            channel.truncate(segmentOffset);
            dataBuffer.byteBuffer().put(0, (byte)0).limit(1).position(0);

            while (true)
            {
                final int written = channel.write(dataBuffer.byteBuffer(), segmentLength - 1);
                if (1 == written)
                {
                    break;
                }
            }
        }
        catch (final IOException ex)
        {
            controlSession.sendErrorResponse(correlationId, ex.getMessage());
            LangUtil.rethrowUnchecked(ex);
        }

        return true;
    }

    private void closeAndRemoveRecordingSubscription(final Subscription subscription, final String reason)
    {
        final long subscriptionId = subscription.registrationId();
        subscriptionRefCountMap.remove(subscriptionId);

        for (final RecordingSession session : recordingSessionByIdMap.values())
        {
            if (subscription == session.subscription())
            {
                session.abort(reason);
            }
        }

        removeRecordingSubscription(subscriptionId);
        CloseHelper.close(errorHandler, subscription);
    }

    private String isLowStorageSpace(final long correlationId, final ControlSession controlSession)
    {
        try
        {
            final long threshold = ctx.lowStorageSpaceThreshold();
            final long usableSpace = ctx.archiveFileStore().getUsableSpace();

            if (usableSpace <= threshold)
            {
                final String msg = "low storage threshold=" + threshold + " <= usableSpace=" + usableSpace;
                controlSession.sendErrorResponse(correlationId, STORAGE_SPACE, msg);
                return msg;
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return null;
    }

    private void deleteSegments(
        final long correlationId,
        final long recordingId,
        final ControlSession controlSession,
        final ArrayDeque<String> files)
    {
        final int count = addDeleteSegmentsSession(correlationId, recordingId, controlSession, files);
        if (count >= 0)
        {
            controlSession.sendOkResponse(correlationId, count);

            if (0 == count)
            {
                controlSession.sendSignal(
                    correlationId, recordingId, Aeron.NULL_VALUE, Aeron.NULL_VALUE, RecordingSignal.DELETE);
            }
        }
    }

    public long generateReplayToken(final ControlSession session, final long recordingId)
    {
        long replayToken = NULL_VALUE;
        while (NULL_VALUE == replayToken || controlSessionByReplayToken.containsKey(replayToken))
        {
            replayToken = random.nextLong();
        }

        final SessionForReplay sessionForReplay = new SessionForReplay(
            recordingId, session, nanoClock.nanoTime() + TimeUnit.MILLISECONDS.toNanos(connectTimeoutMs));
        controlSessionByReplayToken.put(replayToken, sessionForReplay);

        return replayToken;
    }

    public ControlSession getReplaySession(final long replayToken, final long recordingId)
    {
        final SessionForReplay sessionForReplay = controlSessionByReplayToken.get(replayToken);

        final long nowNs = nanoClock.nanoTime();
        if (null != sessionForReplay &&
            recordingId == sessionForReplay.recordingId &&
            nowNs < sessionForReplay.deadlineNs)
        {
            return sessionForReplay.controlSession;
        }

        return null;
    }

    void removeReplayTokensForSession(final long sessionId)
    {
        //noinspection Java8CollectionRemoveIf
        for (Long2ObjectHashMap<SessionForReplay>.ValueIterator it = controlSessionByReplayToken.values().iterator();
            it.hasNext();)
        {
            final SessionForReplay sessionForReplay = it.next();
            if (sessionForReplay.controlSession.sessionId() == sessionId)
            {
                it.remove();
            }
        }
    }

    private int checkReplayTokens(final long nowNs)
    {
        //noinspection Java8CollectionRemoveIf
        for (Long2ObjectHashMap<SessionForReplay>.ValueIterator it = controlSessionByReplayToken.values().iterator();
            it.hasNext();)
        {
            final SessionForReplay sessionForReplay = it.next();
            if (sessionForReplay.deadlineNs <= nowNs)
            {
                it.remove();
            }
        }

        return 0;
    }

    abstract static class Recorder extends SessionWorker<RecordingSession>
    {
        private long totalWriteBytes;
        private long totalWriteTimeNs;
        private long maxWriteTimeNs;
        private final Counter totalWriteBytesCounter;
        private final Counter totalWriteTimeCounter;
        private final Counter maxWriteTimeCounter;

        Recorder(final CountedErrorHandler errorHandler, final Archive.Context context)
        {
            super("archive-recorder", errorHandler);
            totalWriteBytesCounter = context.totalWriteBytesCounter();
            totalWriteTimeCounter = context.totalWriteTimeCounter();
            maxWriteTimeCounter = context.maxWriteTimeCounter();
        }

        final void bytesWritten(final long bytes)
        {
            totalWriteBytes += bytes;
        }

        final void writeTimeNs(final long nanos)
        {
            totalWriteTimeNs += nanos;

            if (nanos > maxWriteTimeNs)
            {
                maxWriteTimeNs = nanos;
            }
        }

        public int doWork()
        {
            final int workCount = super.doWork();
            if (workCount > 0)
            {
                totalWriteBytesCounter.setRelease(totalWriteBytes);
                totalWriteTimeCounter.setRelease(totalWriteTimeNs);
                maxWriteTimeCounter.setRelease(maxWriteTimeNs);
            }

            return workCount;
        }
    }

    abstract static class Replayer extends SessionWorker<ReplaySession>
    {
        private long totalReadBytes;
        private long totalReadTimeNs;
        private long maxReadTimeNs;
        private final Counter totalReadBytesCounter;
        private final Counter totalReadTimeCounter;
        private final Counter maxReadTimeCounter;

        Replayer(final CountedErrorHandler errorHandler, final Archive.Context context)
        {
            super("archive-replayer", errorHandler);
            totalReadBytesCounter = context.totalReadBytesCounter();
            totalReadTimeCounter = context.totalReadTimeCounter();
            maxReadTimeCounter = context.maxReadTimeCounter();
        }

        final void bytesRead(final long bytes)
        {
            totalReadBytes += bytes;
        }

        final void readTimeNs(final long nanos)
        {
            totalReadTimeNs += nanos;

            if (nanos > maxReadTimeNs)
            {
                maxReadTimeNs = nanos;
            }
        }

        public int doWork()
        {
            final int workCount = super.doWork();
            if (workCount > 0)
            {
                totalReadBytesCounter.setRelease(totalReadBytes);
                totalReadTimeCounter.setRelease(totalReadTimeNs);
                maxReadTimeCounter.setRelease(maxReadTimeNs);
            }

            return workCount;
        }
    }

    private static final class SessionForReplay
    {
        private final long recordingId;
        private final ControlSession controlSession;
        private final long deadlineNs;

        private SessionForReplay(final long recordingId, final ControlSession controlSession, final long deadlineNs)
        {
            this.recordingId = recordingId;
            this.controlSession = controlSession;
            this.deadlineNs = deadlineNs;
        }
    }
}
