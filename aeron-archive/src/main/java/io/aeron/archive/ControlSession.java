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
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.client.ArchiveEvent;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.security.Authenticator;
import org.agrona.CloseHelper;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayDeque;
import java.util.function.BooleanSupplier;

import static io.aeron.archive.client.ArchiveException.AUTHENTICATION_REJECTED;
import static io.aeron.archive.codecs.ControlResponseCode.ERROR;
import static io.aeron.archive.codecs.ControlResponseCode.OK;
import static io.aeron.archive.codecs.ControlResponseCode.RECORDING_UNKNOWN;
import static io.aeron.archive.codecs.ControlResponseCode.SUBSCRIPTION_UNKNOWN;

/**
 * Control sessions are interacted with from the {@link ArchiveConductor}. The interaction may result in pending
 * send actions being queued for execution by the {@link ArchiveConductor}.
 */
final class ControlSession implements Session
{
    static final String SESSION_CLOSED_MSG = "session closed";
    private static final long RESEND_INTERVAL_MS = 200L;
    private static final String SESSION_REJECTED_MSG = "authentication rejected";
    private final Thread conductorThread;
    private long sessionLivenessCheckDeadlineMs;
    private String abortReason;

    @SuppressWarnings("JavadocVariable")
    enum State
    {
        INIT, CONNECTING, CONNECTED, CHALLENGED, AUTHENTICATED, ACTIVE, INACTIVE, REJECTED, DONE
    }

    private final long controlSessionId;
    private final long connectTimeoutMs;
    private final long sessionLivenessCheckIntervalMs;
    private final long controlPublicationId;
    private final Aeron aeron;
    private final ArchiveConductor conductor;
    private final CachedEpochClock cachedEpochClock;
    private final ControlResponseProxy controlResponseProxy;
    private final Authenticator authenticator;
    private final ControlSessionProxy controlSessionProxy;
    private final ArrayDeque<BooleanSupplier> syncResponseQueue = new ArrayDeque<>(8);
    private final ManyToOneConcurrentLinkedQueue<BooleanSupplier> asyncResponseQueue =
        new ManyToOneConcurrentLinkedQueue<>();
    private final ControlSessionAdapter controlSessionAdapter;
    private final int controlPublicationStreamId;
    private final String controlPublicationChannel;
    private final String invalidVersionMessage;
    private State state = State.INIT;
    private long correlationId;
    private long resendDeadlineMs;
    private long activityDeadlineMs;
    private Session activeListing = null;
    private ExclusivePublication controlPublication;
    private byte[] encodedPrincipal;

    ControlSession(
        final long controlSessionId,
        final long correlationId,
        final long connectTimeoutMs,
        final long sessionLivenessCheckIntervalMs,
        final long controlPublicationId,
        final String controlPublicationChannel,
        final int controlPublicationStreamId,
        final String invalidVersionMessage,
        final ControlSessionAdapter controlSessionAdapter,
        final Aeron aeron,
        final ArchiveConductor conductor,
        final CachedEpochClock cachedEpochClock,
        final ControlResponseProxy controlResponseProxy,
        final Authenticator authenticator,
        final ControlSessionProxy controlSessionProxy)
    {
        this.controlSessionId = controlSessionId;
        this.correlationId = correlationId;
        this.connectTimeoutMs = connectTimeoutMs;
        this.sessionLivenessCheckIntervalMs = sessionLivenessCheckIntervalMs;
        this.controlPublicationChannel = controlPublicationChannel;
        this.controlPublicationStreamId = controlPublicationStreamId;
        this.invalidVersionMessage = invalidVersionMessage;
        this.controlSessionAdapter = controlSessionAdapter;
        this.aeron = aeron;
        this.controlPublicationId = controlPublicationId;
        this.conductor = conductor;
        this.cachedEpochClock = cachedEpochClock;
        this.controlResponseProxy = controlResponseProxy;
        this.authenticator = authenticator;
        this.controlSessionProxy = controlSessionProxy;
        this.activityDeadlineMs = cachedEpochClock.time() + connectTimeoutMs;
        this.sessionLivenessCheckDeadlineMs = cachedEpochClock.time() + sessionLivenessCheckIntervalMs;
        conductorThread = Thread.currentThread();
    }

    /**
     * {@inheritDoc}
     */
    public long sessionId()
    {
        return controlSessionId;
    }

    /**
     * {@inheritDoc}
     */
    public void abort(final String reason)
    {
        abortReason = reason;
        state(State.DONE, reason);
        if (null != activeListing)
        {
            activeListing.abort(reason);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        if (null != activeListing)
        {
            activeListing.abort(abortReason);
        }

        if (null == controlPublication)
        {
            aeron.asyncRemovePublication(controlPublicationId);
        }
        else
        {
            CloseHelper.close(conductor.context().countedErrorHandler(), controlPublication);
        }

        controlSessionAdapter.removeControlSession(controlSessionId);
        if (!conductor.context().controlSessionsCounter().isClosed())
        {
            conductor.context().controlSessionsCounter().decrementRelease();
        }

        if (null != abortReason && !SESSION_CLOSED_MSG.equals(abortReason))
        {
            conductor.errorHandler.onError(new ArchiveEvent(
                "controlSessionId=" + controlSessionId + " (responseStreamId=" + controlPublicationStreamId +
                " responseChannel=" + controlPublicationChannel + ") terminated: " + abortReason));
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean isDone()
    {
        return state == State.DONE;
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        int workCount = 0;
        final long nowMs = cachedEpochClock.time();

        if (hasNoActivity(nowMs))
        {
            abortReason = State.ACTIVE == state ?
                "failed to send response for more than connectTimeoutMs=" + connectTimeoutMs :
                "failed to establish initial connection: state=" + state;
            state(State.INACTIVE, abortReason);
            workCount++;
        }

        switch (state)
        {
            case INIT:
                workCount += waitForPublication(nowMs);
                break;

            case CONNECTING:
                workCount += waitForConnection(nowMs);
                break;

            case CONNECTED:
                workCount += sendConnectResponse(nowMs);
                break;

            case CHALLENGED:
                workCount += waitForChallengeResponse(nowMs);
                break;

            case AUTHENTICATED:
                workCount += waitForRequest(nowMs);
                break;

            case ACTIVE:
            {
                workCount += performLivenessCheck(nowMs);
                workCount += sendResponses(nowMs);
                break;
            }

            case REJECTED:
                workCount += sendReject(nowMs);
                break;

            case INACTIVE:
                state(State.DONE, "inactive");
                break;

            case DONE:
                break;
        }

        return workCount;
    }

    byte[] encodedPrincipal()
    {
        return encodedPrincipal;
    }

    long correlationId()
    {
        return correlationId;
    }

    State state()
    {
        return state;
    }

    ArchiveConductor archiveConductor()
    {
        return conductor;
    }

    ExclusivePublication controlPublication()
    {
        return controlPublication;
    }

    boolean hasActiveListing()
    {
        return null != activeListing;
    }

    void activeListing(final Session activeListing)
    {
        this.activeListing = activeListing;
    }

    void onChallengeResponse(final long correlationId, final byte[] encodedCredentials)
    {
        if (State.CHALLENGED == state)
        {
            this.correlationId = correlationId;
            authenticator.onChallengeResponse(controlSessionId, encodedCredentials, cachedEpochClock.time());
        }
    }

    void onKeepAlive(@SuppressWarnings("unused") final long correlationId)
    {
        attemptToActivate();
    }

    void onStopRecording(final long correlationId, final int streamId, final String channel)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.stopRecording(correlationId, streamId, channel, this);
        }
    }

    void onStopRecordingSubscription(final long correlationId, final long subscriptionId)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.stopRecordingSubscription(correlationId, subscriptionId, this);
        }
    }

    void onStartRecording(
        final long correlationId,
        final int streamId,
        final SourceLocation sourceLocation,
        final boolean autoStop,
        final String channel)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.startRecording(correlationId, streamId, sourceLocation, autoStop, channel, this);
        }
    }

    void onListRecordingsForUri(
        final long correlationId,
        final long fromRecordingId,
        final int recordCount,
        final int streamId,
        final byte[] channelFragment)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.newListRecordingsForUriSession(
                correlationId,
                fromRecordingId,
                recordCount,
                streamId,
                channelFragment,
                this);
        }
    }

    void onListRecordings(final long correlationId, final long fromRecordingId, final int recordCount)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.newListRecordingsSession(correlationId, fromRecordingId, recordCount, this);
        }
    }

    void onListRecording(final long correlationId, final long recordingId)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.listRecording(correlationId, recordingId, this);
        }
    }

    void onFindLastMatchingRecording(
        final long correlationId,
        final long minRecordingId,
        final int sessionId,
        final int streamId,
        final byte[] channelFragment)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.findLastMatchingRecording(
                correlationId,
                minRecordingId,
                sessionId,
                streamId,
                channelFragment,
                this);
        }
    }

    void onStartReplay(
        final long correlationId,
        final long recordingId,
        final long position,
        final long length,
        final int fileIoMaxLength,
        final int replayStreamId,
        final String replayChannel)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.startReplay(
                correlationId,
                recordingId,
                position,
                length,
                fileIoMaxLength,
                replayStreamId,
                replayChannel,
                null,
                this);
        }
    }

    void onStartBoundedReplay(
        final long correlationId,
        final long recordingId,
        final long position,
        final long length,
        final int limitCounterId,
        final int fileIoMaxLength,
        final int replayStreamId,
        final String replayChannel)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.startBoundedReplay(
                correlationId,
                recordingId,
                position,
                length,
                limitCounterId,
                fileIoMaxLength,
                replayStreamId,
                replayChannel,
                this);
        }
    }

    void onStopReplay(final long correlationId, final long replaySessionId)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.stopReplay(correlationId, replaySessionId, this);
        }
    }

    void onStopAllReplays(final long correlationId, final long recordingId)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.stopAllReplays(correlationId, recordingId, this);
        }
    }

    void onExtendRecording(
        final long correlationId,
        final long recordingId,
        final int streamId,
        final SourceLocation sourceLocation,
        final boolean autoStop,
        final String channel)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.extendRecording(correlationId, recordingId, streamId, sourceLocation, autoStop, channel, this);
        }
    }

    void onGetRecordingPosition(final long correlationId, final long recordingId)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.getRecordingPosition(correlationId, recordingId, this);
        }
    }

    void onTruncateRecording(final long correlationId, final long recordingId, final long position)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.truncateRecording(correlationId, recordingId, position, this);
        }
    }

    void onPurgeRecording(final long correlationId, final long recordingId)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.purgeRecording(correlationId, recordingId, this);
        }
    }

    void onGetStopPosition(final long correlationId, final long recordingId)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.getStopPosition(correlationId, recordingId, this);
        }
    }

    void onGetMaxRecordedPosition(final long correlationId, final long recordingId)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.getMaxRecordedPosition(correlationId, recordingId, this);
        }
    }

    void onArchiveId(final long correlationId)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.archiveId(correlationId, this);
        }
    }

    void onListRecordingSubscriptions(
        final long correlationId,
        final int pseudoIndex,
        final int subscriptionCount,
        final boolean applyStreamId,
        final int streamId,
        final String channelFragment)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.listRecordingSubscriptions(
                correlationId,
                pseudoIndex,
                subscriptionCount,
                applyStreamId,
                streamId,
                channelFragment,
                this);
        }
    }

    void onStopRecordingByIdentity(final long correlationId, final long recordingId)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.stopRecordingByIdentity(correlationId, recordingId, this);
        }
    }

    void onReplicate(
        final long correlationId,
        final long srcRecordingId,
        final long dstRecordingId,
        final long stopPosition,
        final long channelTagId,
        final long subscriptionTagId,
        final int srcControlStreamId,
        final int fileIoMaxLength,
        final int replicationSessionId,
        final String srcControlChannel,
        final String liveDestination,
        final String replicationChannel,
        final byte[] encodedCredentials,
        final String srcResponseChannel)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.replicate(
                correlationId,
                srcRecordingId,
                dstRecordingId,
                stopPosition,
                channelTagId,
                subscriptionTagId,
                srcControlStreamId,
                srcControlChannel,
                liveDestination,
                replicationChannel,
                fileIoMaxLength,
                replicationSessionId,
                encodedCredentials,
                srcResponseChannel,
                this);
        }
    }

    void onStopReplication(final long correlationId, final long replicationId)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.stopReplication(correlationId, replicationId, this);
        }
    }

    void onGetStartPosition(final long correlationId, final long recordingId)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.getStartPosition(correlationId, recordingId, this);
        }
    }

    void onDetachSegments(final long correlationId, final long recordingId, final long newStartPosition)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.detachSegments(correlationId, recordingId, newStartPosition, this);
        }
    }

    void onDeleteDetachedSegments(final long correlationId, final long recordingId)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.deleteDetachedSegments(correlationId, recordingId, this);
        }
    }

    void onPurgeSegments(final long correlationId, final long recordingId, final long newStartPosition)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.purgeSegments(correlationId, recordingId, newStartPosition, this);
        }
    }

    void onAttachSegments(final long correlationId, final long recordingId)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.attachSegments(correlationId, recordingId, this);
        }
    }

    void onMigrateSegments(final long correlationId, final long srcRecordingId, final long dstRecordingId)
    {
        attemptToActivate();
        if (State.ACTIVE == state)
        {
            conductor.migrateSegments(correlationId, srcRecordingId, dstRecordingId, this);
        }
    }

    void sendOkResponse(final long correlationId)
    {
        sendResponse(correlationId, 0L, OK, null);
    }

    void sendOkResponse(final long correlationId, final long relevantId)
    {
        sendResponse(correlationId, relevantId, OK, null);
    }

    void sendErrorResponse(final long correlationId, final String errorMessage)
    {
        sendResponse(correlationId, 0L, ERROR, errorMessage);
    }

    void sendErrorResponse(
        final long correlationId, final long relevantId, final String errorMessage)
    {
        sendResponse(correlationId, relevantId, ERROR, errorMessage);
    }

    void sendRecordingUnknown(final long correlationId, final long recordingId)
    {
        sendResponse(correlationId, recordingId, RECORDING_UNKNOWN, null);
    }

    void sendSubscriptionUnknown(final long correlationId)
    {
        sendResponse(correlationId, 0L, SUBSCRIPTION_UNKNOWN, null);
    }

    void sendResponse(
        final long correlationId,
        final long relevantId,
        final ControlResponseCode code,
        final String errorMessage)
    {
        assertCalledOnConductorThread();

        if (!syncResponseQueue.isEmpty() ||
            !controlResponseProxy.sendResponse(controlSessionId, correlationId, relevantId, code, errorMessage, this))
        {
            updateActivityDeadline(cachedEpochClock.time());
            queueResponse(correlationId, relevantId, code, errorMessage);
        }
        else
        {
            activityDeadlineMs = Aeron.NULL_VALUE;
        }
    }

    void asyncSendOkResponse(final long correlationId, final long replaySessionId)
    {
        if (!asyncResponseQueue.offer(() -> controlResponseProxy.sendResponse(
            controlSessionId,
            correlationId,
            replaySessionId,
            OK,
            null,
            this)))
        {
            throw new IllegalStateException("failed to offer async replay response");
        }
    }

    boolean sendDescriptor(final long correlationId, final UnsafeBuffer descriptorBuffer)
    {
        assertCalledOnConductorThread();
        final boolean sent =
            controlResponseProxy.sendDescriptor(controlSessionId, correlationId, descriptorBuffer, this);
        if (!sent)
        {
            updateActivityDeadline(cachedEpochClock.time());
        }
        else
        {
            activityDeadlineMs = Aeron.NULL_VALUE;
        }
        return sent;
    }

    boolean sendSubscriptionDescriptor(final long correlationId, final Subscription subscription)
    {
        assertCalledOnConductorThread();
        final boolean sent =
            controlResponseProxy.sendSubscriptionDescriptor(controlSessionId, correlationId, subscription, this);
        if (!sent)
        {
            updateActivityDeadline(cachedEpochClock.time());
        }
        else
        {
            activityDeadlineMs = Aeron.NULL_VALUE;
        }
        return sent;
    }

    void sendSignal(
        final long correlationId,
        final long recordingId,
        final long subscriptionId,
        final long position,
        final RecordingSignal recordingSignal)
    {
        assertCalledOnConductorThread();
        if (!syncResponseQueue.isEmpty() || !controlResponseProxy.sendSignal(
            controlSessionId,
            correlationId,
            recordingId,
            subscriptionId,
            position,
            recordingSignal,
            controlPublication))
        {
            updateActivityDeadline(cachedEpochClock.time());
            syncResponseQueue.offer(() -> controlResponseProxy.sendSignal(
                controlSessionId,
                correlationId,
                recordingId,
                subscriptionId,
                position,
                recordingSignal,
                controlPublication));
        }
        else
        {
            activityDeadlineMs = Aeron.NULL_VALUE;
        }
    }

    int maxPayloadLength()
    {
        return controlPublication.maxPayloadLength();
    }

    void challenged()
    {
        state(State.CHALLENGED, "challenged");
    }

    void authenticate(final byte[] encodedPrincipal)
    {
        this.encodedPrincipal = encodedPrincipal;
        activityDeadlineMs = Aeron.NULL_VALUE;
        state(State.AUTHENTICATED, "authenticated");
    }

    void reject()
    {
        state(State.REJECTED, SESSION_REJECTED_MSG);
    }

    private void assertCalledOnConductorThread()
    {
        if (Thread.currentThread() != conductorThread)
        {
            throw new IllegalStateException(
                "Invalid concurrent access detected: " + Thread.currentThread() + " != " + conductorThread);
        }
    }

    private void queueResponse(
        final long correlationId, final long relevantId, final ControlResponseCode code, final String message)
    {
        syncResponseQueue.offer(() -> controlResponseProxy.sendResponse(
            controlSessionId,
            correlationId,
            relevantId,
            code,
            message,
            this));
    }

    private int waitForPublication(final long nowMs)
    {
        int workCount = 0;

        final ExclusivePublication publication = aeron.getExclusivePublication(controlPublicationId);
        if (null != publication)
        {
            controlPublication = publication;
            activityDeadlineMs = nowMs + connectTimeoutMs;
            state(State.CONNECTING, "connecting");
            workCount += 1;
        }

        return workCount;
    }

    private int waitForConnection(final long nowMs)
    {
        int workCount = 0;

        if (controlPublication.isConnected())
        {
            state(State.CONNECTED, "connected");
            workCount += 1;
        }

        return workCount;
    }

    private int sendConnectResponse(final long nowMs)
    {
        int workCount = 0;

        if (nowMs > resendDeadlineMs)
        {
            resendDeadlineMs = nowMs + RESEND_INTERVAL_MS;
            if (null != invalidVersionMessage)
            {
                controlResponseProxy.sendResponse(
                    controlSessionId,
                    correlationId,
                    controlSessionId,
                    ERROR,
                    invalidVersionMessage,
                    this);
            }
            else
            {
                authenticator.onConnectedSession(controlSessionProxy.controlSession(this), nowMs);
            }

            workCount += 1;
        }

        return workCount;
    }

    private int waitForChallengeResponse(final long nowMs)
    {
        authenticator.onChallengedSession(controlSessionProxy.controlSession(this), nowMs);
        return 1;
    }

    private int waitForRequest(final long nowMs)
    {
        int workCount = 0;

        if (nowMs > resendDeadlineMs)
        {
            resendDeadlineMs = nowMs + RESEND_INTERVAL_MS;
            if (controlResponseProxy.sendResponse(
                controlSessionId,
                correlationId,
                controlSessionId,
                OK,
                null,
                this))
            {
                activityDeadlineMs = Aeron.NULL_VALUE;
                workCount += 1;
            }
        }

        return workCount;
    }

    private int performLivenessCheck(final long nowMs)
    {
        if (sessionLivenessCheckDeadlineMs - nowMs < 0)
        {
            sessionLivenessCheckDeadlineMs = nowMs + sessionLivenessCheckIntervalMs;
            if (!controlResponseProxy.sendPing(controlSessionId, controlPublication))
            {
                updateActivityDeadline(nowMs);
            }
            else
            {
                activityDeadlineMs = Aeron.NULL_VALUE;
            }
            return 1;
        }
        return 0;
    }

    private int sendResponses(final long nowMs)
    {
        int workCount = 0;

        if (!controlPublication.isConnected())
        {
            abortReason = "control publication not connected";
            state(State.INACTIVE, abortReason);
            workCount++;
        }
        else
        {
            if (!syncResponseQueue.isEmpty())
            {
                if (syncResponseQueue.peekFirst().getAsBoolean())
                {
                    syncResponseQueue.pollFirst();
                    activityDeadlineMs = Aeron.NULL_VALUE;
                    workCount++;
                }
            }

            final BooleanSupplier response = asyncResponseQueue.peek();
            if (null != response)
            {
                if (response.getAsBoolean())
                {
                    asyncResponseQueue.poll();
                    activityDeadlineMs = Aeron.NULL_VALUE;
                    workCount++;
                }
                else
                {
                    updateActivityDeadline(nowMs);
                }
            }
        }

        return workCount;
    }

    private int sendReject(final long nowMs)
    {
        int workCount = 0;

        if (nowMs > resendDeadlineMs)
        {
            resendDeadlineMs = nowMs + RESEND_INTERVAL_MS;
            controlResponseProxy.sendResponse(
                controlSessionId, correlationId, AUTHENTICATION_REJECTED, ERROR, SESSION_REJECTED_MSG, this);

            workCount += 1;
        }

        return workCount;
    }

    private boolean hasNoActivity(final long nowMs)
    {
        return Aeron.NULL_VALUE != activityDeadlineMs && nowMs > activityDeadlineMs;
    }

    private void updateActivityDeadline(final long nowMs)
    {
        if (Aeron.NULL_VALUE == activityDeadlineMs)
        {
            activityDeadlineMs = nowMs + connectTimeoutMs;
        }
    }

    private void attemptToActivate()
    {
        if (State.AUTHENTICATED == state && null == invalidVersionMessage)
        {
            state(State.ACTIVE, "active");
        }
    }

    private void state(final State state, final String reason)
    {
        logStateChange(this.state, state, controlSessionId, reason);
        this.state = state;
    }

    private void logStateChange(
        final State oldState, final State newState, final long controlSessionId, final String reason)
    {
//        System.out.println(controlSessionId + ": " + oldState + " -> " + newState + ", reason=\"" + reason + "\"");
    }

    public String toString()
    {
        return "ControlSession{" +
            "controlSessionId=" + controlSessionId +
            ", correlationId=" + correlationId +
            ", state=" + state +
            ", controlPublication=" + controlPublication +
            '}';
    }
}
