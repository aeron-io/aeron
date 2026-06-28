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

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.ArchiveProxy;
import io.aeron.archive.client.ControlResponsePoller;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.client.RecordingDescriptorPoller;
import io.aeron.archive.client.ReplayParams;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.TimeoutException;
import org.agrona.CloseHelper;
import org.agrona.Strings;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.CountedErrorHandler;

import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.ReplayMerge.LIVE_ADD_MAX_WINDOW;
import static io.aeron.archive.codecs.RecordingSignal.MERGE;
import static io.aeron.archive.codecs.RecordingSignal.REPLICATE;
import static io.aeron.archive.codecs.RecordingSignal.REPLICATE_END;
import static io.aeron.archive.codecs.RecordingSignal.SYNC;

class ReplicationSession implements Session, RecordingDescriptorConsumer
{
    private static final int REPLAY_REMOVE_THRESHOLD = 0;
    private static final int RETRY_ATTEMPTS = 3;
    private static final int SOURCE_ARCHIVE_POLL_INTERVAL_MS = 100;
    private final int replicationSessionId;

    @SuppressWarnings("JavadocVariable")
    enum State
    {
        CONNECT,
        REPLICATE_DESCRIPTOR,
        SRC_RECORDING_POSITION,
        EXTEND,
        REPLAY_TOKEN,
        GET_ARCHIVE_PROXY,
        REPLAY,
        AWAIT_IMAGE,
        REPLICATE,
        CATCHUP,
        ATTEMPT_LIVE_JOIN,
        DONE
    }

    private long activeCorrelationId = NULL_VALUE;
    private long srcReplaySessionId = NULL_VALUE;
    private long replayPosition = NULL_POSITION;
    private long srcStopPosition = NULL_POSITION;
    private long srcRecordingPosition = NULL_POSITION;
    private final long dstStopPosition;
    private final boolean isDestinationRecordingEmpty;
    private long timeOfLastActionMs;
    private final long actionTimeoutMs;
    private final long replicationId;
    private final long channelTagId;
    private final long subscriptionTagId;
    private final long srcRecordingId;
    private long dstRecordingId;
    private int replayStreamId;
    private int replaySessionId;
    private int retryAttempts = RETRY_ATTEMPTS;
    private boolean isLiveAdded;
    private final boolean isTagged;
    private final String replicationChannel;
    private final String liveDestination;
    private String replayDestination;
    private final CachedEpochClock epochClock;
    private final ArchiveConductor conductor;
    private final ControlSession controlSession;
    private final Catalog catalog;
    private final int fileIoMaxLength;
    private final Aeron aeron;
    private final AeronArchive.Context context;
    private AeronArchive.AsyncConnect asyncConnect;
    private AeronArchive srcArchive;
    private Subscription recordingSubscription;
    private Image image;
    private State state = State.CONNECT;
    private long replayToken = NULL_VALUE;
    private long responsePublicationRegistrationId = NULL_VALUE;
    private ExclusivePublication responsePublication = null;
    private ArchiveProxy responseArchiveProxy = null;
    private long timeOfLastScheduledSourceArchivePollMs;

    ReplicationSession(
        final long srcRecordingId,
        final long dstRecordingId,
        final long channelTagId,
        final long subscriptionTagId,
        final long replicationId,
        final long stopPosition,
        final String liveDestination,
        final String replicationChannel,
        final int fileIoMaxLength,
        final int replicationSessionId,
        final RecordingSummary recordingSummary,
        final AeronArchive.Context context,
        final CachedEpochClock epochClock,
        final Catalog catalog,
        final ControlSession controlSession)
    {
        this.replicationId = replicationId;
        this.srcRecordingId = srcRecordingId;
        this.dstRecordingId = dstRecordingId;
        this.liveDestination = Strings.isEmpty(liveDestination) ? null : liveDestination;
        this.replicationChannel = replicationChannel;
        this.fileIoMaxLength = fileIoMaxLength;
        this.replicationSessionId = replicationSessionId;
        this.aeron = context.aeron();
        this.context = context;
        this.catalog = catalog;
        this.epochClock = epochClock;
        this.conductor = controlSession.archiveConductor();
        this.controlSession = controlSession;
        this.actionTimeoutMs = TimeUnit.NANOSECONDS.toMillis(context.messageTimeoutNs());
        this.dstStopPosition = stopPosition;

        this.isTagged = NULL_VALUE != channelTagId || NULL_VALUE != subscriptionTagId;
        this.channelTagId = NULL_VALUE == channelTagId ? replicationId : channelTagId;
        this.subscriptionTagId = NULL_VALUE == subscriptionTagId ? replicationId : subscriptionTagId;

        if (null != recordingSummary)
        {
            replayPosition = recordingSummary.stopPosition;
            replayStreamId = recordingSummary.streamId;
            isDestinationRecordingEmpty = recordingSummary.startPosition == recordingSummary.stopPosition;
        }
        else
        {
            isDestinationRecordingEmpty = false;
        }
    }

    /**
     * {@inheritDoc}
     */
    public long sessionId()
    {
        return replicationId;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isDone()
    {
        return State.DONE == state;
    }

    /**
     * {@inheritDoc}
     */
    public void abort(final String reason)
    {
        this.state(State.DONE, "abort");
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        final ArchiveConductor archiveConductor = controlSession.archiveConductor();
        final CountedErrorHandler countedErrorHandler = archiveConductor.context().countedErrorHandler();

        stopRecording();
        stopReplaySession(countedErrorHandler);

        CloseHelper.close(countedErrorHandler, asyncConnect);
        CloseHelper.close(countedErrorHandler, srcArchive);
        CloseHelper.close(countedErrorHandler, responsePublication);

        archiveConductor.removeReplicationSession(this);
        signal(NULL_POSITION, REPLICATE_END);
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        int workCount = 0;

        try
        {
            if (null != recordingSubscription && recordingSubscription.isClosed())
            {
                state(State.DONE, "recording subscription closed");
                return 1;
            }

            switch (state)
            {
                case CONNECT:
                    workCount += connect();
                    break;

                case REPLICATE_DESCRIPTOR:
                    workCount += replicateDescriptor();
                    break;

                case SRC_RECORDING_POSITION:
                    workCount += srcRecordingPosition();
                    break;

                case EXTEND:
                    workCount += extend();
                    break;

                case REPLAY_TOKEN:
                    workCount += replayToken();
                    break;

                case GET_ARCHIVE_PROXY:
                    workCount += getArchiveProxy();
                    break;

                case REPLAY:
                    workCount += replay();
                    break;

                case AWAIT_IMAGE:
                    workCount += awaitImage();
                    break;

                case REPLICATE:
                    workCount += replicate();
                    break;

                case CATCHUP:
                    workCount += catchup();
                    break;

                case ATTEMPT_LIVE_JOIN:
                    workCount += attemptLiveJoin();
                    break;

                case DONE:
                    break;
            }

            pollSourceArchiveEvents();
        }
        catch (final Exception ex)
        {
            state(State.DONE, ex.getMessage());
            error(ex.getMessage(), ArchiveException.GENERIC);
            throw ex;
        }

        return workCount;
    }

    /**
     * {@inheritDoc}
     */
    public void onRecordingDescriptor(
        final long controlSessionId,
        final long correlationId,
        final long recordingId,
        final long startTimestamp,
        final long stopTimestamp,
        final long startPosition,
        final long stopPosition,
        final int initialTermId,
        final int segmentFileLength,
        final int termBufferLength,
        final int mtuLength,
        final int sessionId,
        final int streamId,
        final String strippedChannel,
        final String originalChannel,
        final String sourceIdentity)
    {
        srcStopPosition = stopPosition;
        replayStreamId = streamId;
        if (null == liveDestination && NULL_VALUE != replicationSessionId)
        {
            replaySessionId = replicationSessionId;
        }
        else
        {
            replaySessionId = sessionId;
        }

        if (Aeron.NULL_VALUE != fileIoMaxLength && fileIoMaxLength < mtuLength)
        {
            final String errorMsg = "Replication fileIoMaxLength (" + fileIoMaxLength +
                ") is less than than the recording mtuLength (" + mtuLength + ")";
            state(State.DONE, errorMsg);
            error(errorMsg, ArchiveException.GENERIC);
            return;
        }

        if (NULL_VALUE == dstRecordingId)
        {
            replayPosition = startPosition;
            dstRecordingId = catalog.addNewRecording(
                startPosition,
                startPosition,
                startTimestamp,
                startTimestamp,
                initialTermId,
                segmentFileLength,
                termBufferLength,
                mtuLength,
                sessionId,
                streamId,
                strippedChannel,
                originalChannel,
                sourceIdentity);

            signal(startPosition, REPLICATE);
        }
        else if (isDestinationRecordingEmpty)
        {
            replayPosition = startPosition;
            catalog.replaceRecording(
                dstRecordingId,
                startPosition,
                startPosition,
                startTimestamp,
                startTimestamp,
                initialTermId,
                segmentFileLength,
                termBufferLength,
                mtuLength,
                sessionId,
                streamId,
                strippedChannel,
                originalChannel,
                sourceIdentity);
        }

        State nextState = State.EXTEND;
        String reason = "";

        if (null != liveDestination)
        {
            if (NULL_POSITION != stopPosition)
            {
                final String errorMsg = "cannot live merge without active source recording";
                state(State.DONE, errorMsg);
                error(errorMsg, ArchiveException.GENERIC);
                return;
            }

            nextState = State.SRC_RECORDING_POSITION;
            reason = "liveDestination=" + liveDestination;
        }

        if (startPosition == stopPosition ||
            (NULL_VALUE != dstRecordingId && stopPosition == catalog.stopPosition(dstRecordingId)))
        {
            signal(stopPosition, SYNC);
            nextState = State.DONE;
            reason = "in sync";
        }

        state(nextState, reason);
    }

    private int connect()
    {
        int workCount = 0;

        if (null == asyncConnect)
        {
            asyncConnect = AeronArchive.asyncConnect(context);
            workCount += 1;
        }
        else
        {
            final int step = asyncConnect.step();
            try
            {
                final AeronArchive archive = asyncConnect.poll();

                if (null == archive)
                {
                    if (asyncConnect.step() != step)
                    {
                        workCount += 1;
                    }
                }
                else
                {
                    srcArchive = archive;
                    asyncConnect = null;
                    state(State.REPLICATE_DESCRIPTOR, "connected to the Archive");
                    workCount += 1;
                }
            }
            catch (final AeronException ex)
            {
                state(State.DONE, ex.getMessage());
                error(
                    "Replication connection failed=" + ex.getMessage(),
                    ArchiveException.REPLICATION_CONNECTION_FAILURE);
            }
        }

        return workCount;
    }

    private int replicateDescriptor()
    {
        int workCount = 0;

        if (NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = aeron.nextCorrelationId();
            if (srcArchive.archiveProxy().listRecording(srcRecordingId, correlationId, srcArchive.controlSessionId()))
            {
                workCount += trackAction(correlationId);
                srcArchive.recordingDescriptorPoller().reset(correlationId, 1, this);
            }
            else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
            {
                throw new TimeoutException("failed to list remote recording descriptor");
            }
        }
        else
        {
            final RecordingDescriptorPoller poller = srcArchive.recordingDescriptorPoller();
            final int fragments = poller.poll();

            if (poller.isDispatchComplete() && poller.remainingRecordCount() > 0)
            {
                final String errorMsg = "unknown src recording id " + srcRecordingId;
                state(State.DONE, errorMsg);
                error(errorMsg, ArchiveException.UNKNOWN_RECORDING);
            }

            if (0 == fragments && epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
            {
                throw new TimeoutException("failed to fetch remote recording descriptor");
            }

            workCount += fragments;
        }

        return workCount;
    }

    private int srcRecordingPosition()
    {
        int workCount = 0;

        if (NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = aeron.nextCorrelationId();
            final long controlSessionId = srcArchive.controlSessionId();
            if (srcArchive.archiveProxy().getRecordingPosition(srcRecordingId, correlationId, controlSessionId))
            {
                workCount += trackAction(correlationId);
            }
            else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
            {
                throw new TimeoutException("failed to send recording position request");
            }
        }
        else
        {
            final ControlResponsePoller poller = srcArchive.controlResponsePoller();
            workCount += poller.poll();

            if (hasResponse(poller))
            {
                srcRecordingPosition = poller.relevantId();
                if (NULL_POSITION == srcRecordingPosition && null != liveDestination)
                {
                    throw new ArchiveException("cannot live merge without active source recording");
                }

                state(State.EXTEND, "");
            }
            else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
            {
                throw new TimeoutException("failed to get recording position");
            }
        }

        return workCount;
    }

    private int extend()
    {
        final boolean isMds = isTagged || null != liveDestination;
        final ChannelUri channelUri = ChannelUri.parse(replicationChannel);
        final String endpoint = channelUri.get(CommonContext.ENDPOINT_PARAM_NAME);
        channelUri.put(CommonContext.REJOIN_PARAM_NAME, "false");
        if (!channelUri.hasControlModeResponse())
        {
            channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(replaySessionId));
        }

        if (isMds)
        {
            channelUri.remove(CommonContext.ENDPOINT_PARAM_NAME);
            channelUri.put(CommonContext.TAGS_PARAM_NAME, channelTagId + "," + subscriptionTagId);
            channelUri.put(CommonContext.MDC_CONTROL_MODE_PARAM_NAME, CommonContext.MDC_CONTROL_MODE_MANUAL);
        }

        final String channel = channelUri.toString();
        final Object recordingSubscriptionOrErrorMsg = conductor.extendRecording(
            replicationId, dstRecordingId, replayStreamId, SourceLocation.REMOTE, true, channel, controlSession);

        if (recordingSubscriptionOrErrorMsg instanceof Subscription)
        {
            recordingSubscription = (Subscription)recordingSubscriptionOrErrorMsg;
            if (isMds)
            {
                replayDestination = ChannelUri.createDestinationUri(replicationChannel, endpoint);
                recordingSubscription.asyncAddDestination(replayDestination);
                state(State.REPLAY_TOKEN, "replay destination added: " + replayDestination);
            }
            else
            {
                state(State.REPLAY_TOKEN, "replay destination added");
            }
        }
        else
        {
            state(State.DONE, (String)recordingSubscriptionOrErrorMsg);
        }

        return 1;
    }

    private int replayToken()
    {
        int workCount = 0;

        if (NULL_VALUE != replayToken || !ChannelUri.parse(replicationChannel).hasControlModeResponse())
        {
            workCount++;
            state(State.GET_ARCHIVE_PROXY, "");
            return workCount;
        }

        if (NULL_VALUE == activeCorrelationId)
        {
            final long lastCorrelationId = aeron.nextCorrelationId();
            if (srcArchive.archiveProxy().requestReplayToken(
                lastCorrelationId, srcArchive.controlSessionId(), srcRecordingId))
            {
                workCount++;
                activeCorrelationId = lastCorrelationId;
            }
            else
            {
                return workCount;
            }
        }

        final ControlResponsePoller poller = srcArchive.controlResponsePoller();
        workCount += poller.poll();
        if (hasResponse(poller))
        {
            replayToken = poller.relevantId();
            state(State.GET_ARCHIVE_PROXY, "");
        }

        return workCount;
    }

    private int getArchiveProxy()
    {
        int workCount = 0;

        if (NULL_VALUE == replayToken)
        {
            ++workCount;
            state(State.REPLAY, "");
            return workCount;
        }

        if (NULL_VALUE == responsePublicationRegistrationId)
        {
            final String uri = new ChannelUriStringBuilder(context.controlRequestChannel())
                .responseCorrelationId(recordingSubscription.registrationId())
                .termId((Integer)null).initialTermId((Integer)null).termOffset((Integer)null)
                .termLength(64 * 1024)
                .build();
            final int controlRequestStreamId = srcArchive.context().controlRequestStreamId();

            responsePublicationRegistrationId = aeron.asyncAddExclusivePublication(uri, controlRequestStreamId);
        }

        if (null == responsePublication)
        {
            responsePublication = aeron.getExclusivePublication(responsePublicationRegistrationId);
            if (null != responsePublication)
            {
                ++workCount;
            }
            else
            {
                return workCount;
            }
        }

        if (responsePublication.isConnected())
        {
            ++workCount;
        }
        else
        {
            return workCount;
        }

        if (0 < responsePublication.availableWindow())
        {
            ++workCount;
        }
        else
        {
            return workCount;
        }

        responseArchiveProxy = new ArchiveProxy(responsePublication);
        state(State.REPLAY, "ArchiveProxy created");

        return workCount;
    }

    private int replay()
    {
        int workCount = 0;

        if (NULL_VALUE == activeCorrelationId)
        {
            final String resolvedEndpoint = recordingSubscription.resolvedEndpoint();
            if (null == resolvedEndpoint)
            {
                if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
                {
                    throw new TimeoutException(
                        "failed to resolve subscription endpoint: channel=" + recordingSubscription.channel());
                }

                return workCount;
            }

            final ChannelUri channelUri = ChannelUri.parse(replicationChannel);
            final String endpoint = channelUri.get(CommonContext.ENDPOINT_PARAM_NAME);
            if (null != endpoint)
            {
                channelUri.replaceEndpointWildcardPort(resolvedEndpoint);
            }

            if (null != liveDestination)
            {
                channelUri.put(CommonContext.LINGER_PARAM_NAME, "0");
                channelUri.put(CommonContext.EOS_PARAM_NAME, "false");
            }

            if (channelUri.hasControlModeResponse())
            {
                channelUri.put(
                    CommonContext.RESPONSE_CORRELATION_ID_PARAM_NAME,
                    String.valueOf(recordingSubscription.registrationId()));
            }
            else
            {
                channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(replaySessionId));
            }

            final long correlationId = aeron.nextCorrelationId();

            final ReplayParams replayParams = new ReplayParams()
                .position(replayPosition)
                .length(NULL_POSITION == dstStopPosition ? AeronArchive.NULL_LENGTH : dstStopPosition - replayPosition)
                .fileIoMaxLength(fileIoMaxLength)
                .replayToken(replayToken);

            final ArchiveProxy archiveProxy = null != responseArchiveProxy ?
                responseArchiveProxy : srcArchive.archiveProxy();

            if (archiveProxy.replay(
                srcRecordingId,
                channelUri.toString(),
                replayStreamId,
                replayParams,
                correlationId,
                srcArchive.controlSessionId()))
            {
                workCount += trackAction(correlationId);
            }
            else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
            {
                throw new TimeoutException("failed to send replay request");
            }
        }
        else
        {
            final ControlResponsePoller poller = srcArchive.controlResponsePoller();
            workCount += poller.poll();

            if (hasResponse(poller))
            {
                srcReplaySessionId = poller.relevantId();
                state(State.AWAIT_IMAGE, "srcReplaySessionId=" + srcReplaySessionId);
            }
            else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
            {
                throw new TimeoutException("failed get acknowledgement of replay request to: " + replicationChannel);
            }
        }

        return workCount;
    }

    private int awaitImage()
    {
        int workCount = 0;

        final Image image = recordingSubscription.imageBySessionId((int)srcReplaySessionId);
        if (null != image)
        {
            this.image = image;
            state(null == liveDestination ? State.REPLICATE : State.CATCHUP,
                "image.correlationId=" + image.correlationId() +
                ", image.sessionId=" + image.sessionId() +
                ", image.joinPosition=" + image.joinPosition());
            workCount += 1;
        }
        else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
        {
            throw new TimeoutException(
                "failed get replay image for sessionId=" + (int)srcReplaySessionId +
                " on channel=" + recordingSubscription.channel());
        }

        return workCount;
    }

    private int replicate()
    {
        int workCount = 0;

        final boolean isClosed = image.isClosed();
        final boolean isEndOfStream = image.isEndOfStream();
        final long position = image.position();
        final boolean isSynced = NULL_POSITION != srcStopPosition && position >= srcStopPosition;

        if (isSynced ||
            (NULL_POSITION != dstStopPosition && position >= dstStopPosition) ||
            isEndOfStream || isClosed)
        {
            logReplicationSessionDone(
                controlSession.sessionId(),
                replicationId,
                srcRecordingId,
                replayPosition,
                srcStopPosition,
                dstRecordingId,
                dstStopPosition,
                position,
                isClosed,
                isEndOfStream,
                isSynced);

            if (isSynced)
            {
                signal(position, SYNC);
            }

            srcReplaySessionId = NULL_VALUE;
            state(State.DONE, isSynced ? "sync" : "done");
            workCount += 1;
        }

        return workCount;
    }

    private int catchup()
    {
        int workCount = 0;

        if (image.position() >= srcRecordingPosition)
        {
            state(State.ATTEMPT_LIVE_JOIN, "image position (" + image.position() +
                ") >= srcRecordingPosition (" + srcRecordingPosition + ")");
            workCount += 1;
        }
        else if (image.isClosed())
        {
            throw new ArchiveException("replication image closed unexpectedly");
        }

        return workCount;
    }

    private int attemptLiveJoin()
    {
        int workCount = 0;

        if (NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = aeron.nextCorrelationId();
            if (srcArchive.archiveProxy().getRecordingPosition(
                srcRecordingId, correlationId, srcArchive.controlSessionId()))
            {
                workCount += trackAction(correlationId);
            }
            else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
            {
                throw new TimeoutException("failed to send recording position request");
            }
        }
        else
        {
            final ControlResponsePoller poller = srcArchive.controlResponsePoller();
            workCount += poller.poll();

            if (hasResponse(poller))
            {
                trackAction(NULL_VALUE);
                retryAttempts = RETRY_ATTEMPTS;
                srcRecordingPosition = poller.relevantId();

                if (NULL_POSITION == srcRecordingPosition && null != liveDestination)
                {
                    throw new ArchiveException("cannot live merge without active source recording");
                }

                final long position = image.position();
                if (shouldAddLiveDestination(position))
                {
                    recordingSubscription.asyncAddDestination(liveDestination);
                    isLiveAdded = true;
                }
                else if (shouldStopReplay(position))
                {
                    recordingSubscription.asyncRemoveDestination(replayDestination);
                    replayDestination = null;
                    recordingSubscription = null;
                    signal(position, MERGE);
                    state(State.DONE, "merge");
                }

                workCount += 1;
            }
            else if (image.isClosed())
            {
                throw new ArchiveException("replication image closed unexpectedly");
            }
            else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
            {
                if (--retryAttempts == 0)
                {
                    throw new TimeoutException("failed to get recording position");
                }

                trackAction(NULL_VALUE);
            }
        }

        return workCount;
    }

    private boolean hasResponse(final ControlResponsePoller poller)
    {
        if (poller.isPollComplete() && poller.controlSessionId() == srcArchive.controlSessionId())
        {
            final ControlResponseCode code = poller.code();
            if (ControlResponseCode.ERROR == code)
            {
                throw new ArchiveException(poller.errorMessage(), (int)poller.relevantId());
            }

            return poller.correlationId() == activeCorrelationId && ControlResponseCode.OK == code;
        }

        return false;
    }

    private void error(final String msg, final int errorCode)
    {
        controlSession.sendErrorResponse(replicationId, errorCode, msg);
    }

    private void signal(final long position, final RecordingSignal recordingSignal)
    {
        final long subscriptionId = null != recordingSubscription ? recordingSubscription.registrationId() : NULL_VALUE;
        controlSession.sendSignal(replicationId, dstRecordingId, subscriptionId, position, recordingSignal);
    }

    private void stopReplaySession(final CountedErrorHandler countedErrorHandler)
    {
        if (NULL_VALUE != srcReplaySessionId)
        {
            try
            {
                srcArchive.archiveProxy().stopReplay(
                    srcReplaySessionId, aeron.nextCorrelationId(), srcArchive.controlSessionId());
            }
            catch (final Exception ex)
            {
                countedErrorHandler.onError(ex);
            }
            srcReplaySessionId = NULL_VALUE;
        }
    }

    private void stopRecording()
    {
        if (null != recordingSubscription)
        {
            conductor.stopRecordingSubscription(recordingSubscription.registrationId());
            recordingSubscription = null;
        }
    }

    private boolean shouldAddLiveDestination(final long position)
    {
        return !isLiveAdded &&
            (srcRecordingPosition - position) <= Math.min(image.termBufferLength() >> 2, LIVE_ADD_MAX_WINDOW);
    }

    private boolean shouldStopReplay(final long position)
    {
        return isLiveAdded &&
            (srcRecordingPosition - position) <= REPLAY_REMOVE_THRESHOLD &&
            image.activeTransportCount() >= 2;
    }

    private int trackAction(final long correlationId)
    {
        timeOfLastActionMs = epochClock.time();
        activeCorrelationId = correlationId;
        return 1;
    }

    private void state(final State newState, final String reason)
    {
        logStateChange(state, newState, replicationId,
            srcRecordingId, dstRecordingId,
            null != image ? image.position() : NULL_POSITION,
            null == reason ? "" : reason);
        state = newState;
        activeCorrelationId = NULL_VALUE;
        timeOfLastActionMs = epochClock.time();
    }

    private void pollSourceArchiveEvents()
    {
        if (null != srcArchive && State.DONE != state && NULL_VALUE == activeCorrelationId)
        {
            final long nowMs = epochClock.time();
            if (nowMs > (timeOfLastScheduledSourceArchivePollMs + SOURCE_ARCHIVE_POLL_INTERVAL_MS))
            {
                timeOfLastScheduledSourceArchivePollMs = nowMs;

                final String errorMessage = srcArchive.pollForErrorResponse();
                if (null != errorMessage)
                {
                    throw new ArchiveException(errorMessage);
                }
            }
        }
    }

    private void logStateChange(
        final State oldState,
        final State newState,
        final long replicationId,
        final long srcRecordingId,
        final long dstRecordingId,
        final long position,
        final String reason)
    {
        //System.out.println("ReplicationSession: " + oldState + " -> " + newState + " replicationId=" + replicationId);
    }

    private void logReplicationSessionDone(
        final long controlSessionId,
        final long replicationId,
        final long srcRecordingId,
        final long replayPosition,
        final long srcStopPosition,
        final long dstRecordingId,
        final long dstStopPosition,
        final long position,
        final boolean isClosed,
        final boolean isEndOfStream,
        final boolean isSynced)
    {
//        System.out.println(
//            "ReplicationDone: controlSessionId = " + controlSessionId + ", replicationId = " + replicationId +
//            ", srcRecordingId = " + srcRecordingId + ", srcRecordingPosition = " + srcRecordingPosition +
//            ", srcStopPosition = " + srcStopPosition + ", dstRecordingId = " + dstRecordingId +
//            ", dstStopPosition = " + dstStopPosition + ", position = " + position + ", isClosed = " + isClosed +
//            ", isEndOfStream = " + isEndOfStream + ", isSynced = " + isSynced);
    }
}
