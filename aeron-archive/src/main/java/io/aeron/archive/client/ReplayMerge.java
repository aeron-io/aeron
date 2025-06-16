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
package io.aeron.archive.client;

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.EpochClock;

import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.CommonContext.IPC_CHANNEL;

/**
 * Replay a recorded stream from a starting position and merge with live stream for a full history of a stream.
 * <p>
 * Once constructed either of {@link #poll(FragmentHandler, int)} or {@link #doWork()}, interleaved with consumption
 * of the {@link #image()}, should be called in a duty cycle loop until {@link #isMerged()} is {@code true}.
 * After which the {@link ReplayMerge} can be closed and continued usage can be made of the {@link Image} or its
 * parent {@link Subscription}. If an exception occurs or progress stops, the merge will fail and
 * {@link #hasFailed()} will be {@code true}.
 * <p>
 * If the endpoint on the replay destination uses a port of 0, then the OS will assign a port from the ephemeral
 * range and this will be added to the replay channel for instructing the archive.
 * <p>
 * NOTE: Merging is only supported with UDP streams.
 * <p>
 * NOTE: ReplayMerge is not threadsafe and should <b>not</b> be used with a shared {@link AeronArchive} client.
 */
public final class ReplayMerge implements AutoCloseable
{
    /**
     * The maximum window at which a live destination should be added when trying to merge.
     */
    public static final int LIVE_ADD_MAX_WINDOW = 32 * 1024 * 1024;

    private static final int REPLAY_REMOVE_THRESHOLD = 0;
    private static final long MERGE_PROGRESS_TIMEOUT_DEFAULT_MS = TimeUnit.SECONDS.toMillis(5);
    private static final long INITIAL_GET_MAX_RECORDED_POSITION_BACKOFF_MS = 8;
    private static final long GET_MAX_RECORDED_POSITION_BACKOFF_MAX_MS = 500;
    private static final long ARCHIVE_POLL_INTERVAL_MS = 100;

    @SuppressWarnings("JavadocVariable")
    enum State
    {
        RESOLVE_REPLAY_PORT,
        GET_RECORDING_POSITION,
        REPLAY,
        CATCHUP,
        ATTEMPT_LIVE_JOIN,
        MERGED,
        FAILED,
        CLOSED
    }

    private final long recordingId;
    private final long startPosition;
    private final long mergeProgressTimeoutMs;
    private long replaySessionId = NULL_VALUE;
    private long activeCorrelationId = NULL_VALUE;
    private long nextTargetPosition = NULL_VALUE;
    private long positionOfLastProgress = NULL_VALUE;
    private long timeOfLastProgressMs;
    private long timeOfNextGetMaxRecordedPositionMs;
    private long getMaxRecordedPositionBackoffMs = INITIAL_GET_MAX_RECORDED_POSITION_BACKOFF_MS;
    private long timeOfLastScheduledArchivePollMs;
    private boolean isLiveAdded = false;
    private boolean isReplayActive = false;
    private State state;
    private Image image;

    private final AeronArchive archive;
    private final Subscription subscription;
    private final EpochClock epochClock;
    private final String replayDestination;
    private final String liveDestination;
    private final ChannelUri replayChannelUri;

    /**
     * Create a {@link ReplayMerge} to manage the merging of a replayed stream and switching over to live stream as
     * appropriate.
     *
     * @param subscription           to use for the replay and live stream. Must be a multi-destination subscription.
     * @param archive                to use for the replay.
     * @param replayChannel          to as a template for what the archive will use.
     * @param replayDestination      to send the replay to and the destination added by the {@link Subscription}.
     * @param liveDestination        for the live stream and the destination added by the {@link Subscription}.
     * @param recordingId            for the replay.
     * @param startPosition          for the replay.
     * @param epochClock             to use for progress checks.
     * @param mergeProgressTimeoutMs to use for progress checks.
     */
    public ReplayMerge(
        final Subscription subscription,
        final AeronArchive archive,
        final String replayChannel,
        final String replayDestination,
        final String liveDestination,
        final long recordingId,
        final long startPosition,
        final EpochClock epochClock,
        final long mergeProgressTimeoutMs)
    {
        if (subscription.channel().startsWith(IPC_CHANNEL) ||
            replayChannel.startsWith(IPC_CHANNEL) ||
            replayDestination.startsWith(IPC_CHANNEL) ||
            liveDestination.startsWith(IPC_CHANNEL))
        {
            throw new IllegalArgumentException("IPC merging is not supported");
        }

        if (!subscription.channel().contains("control-mode=manual"))
        {
            throw new IllegalArgumentException(
                "Subscription URI must have 'control-mode=manual' uri=" + subscription.channel());
        }

        this.archive = archive;
        this.subscription = subscription;
        this.epochClock = epochClock;
        this.replayDestination = replayDestination;
        this.liveDestination = liveDestination;
        this.recordingId = recordingId;
        this.startPosition = startPosition;
        this.mergeProgressTimeoutMs = mergeProgressTimeoutMs;

        replayChannelUri = ChannelUri.parse(replayChannel);
        replayChannelUri.put(CommonContext.LINGER_PARAM_NAME, "0");
        replayChannelUri.put(CommonContext.EOS_PARAM_NAME, "false");

        final String replayEndpoint = ChannelUri.parse(replayDestination).get(ENDPOINT_PARAM_NAME);
        if (replayEndpoint.endsWith(":0"))
        {
            state = State.RESOLVE_REPLAY_PORT;
        }
        else
        {
            replayChannelUri.put(ENDPOINT_PARAM_NAME, replayEndpoint);
            state = State.GET_RECORDING_POSITION;
        }

        subscription.asyncAddDestination(replayDestination);
        timeOfLastProgressMs = timeOfNextGetMaxRecordedPositionMs = epochClock.time();
    }

    /**
     * Create a {@link ReplayMerge} to manage the merging of a replayed stream and switching over to live stream as
     * appropriate.
     *
     * @param subscription      to use for the replay and live stream. Must be a multi-destination subscription.
     * @param archive           to use for the replay.
     * @param replayChannel     to use as a template for what the archive will use.
     * @param replayDestination to send the replay to and the destination added by the {@link Subscription}.
     * @param liveDestination   for the live stream and the destination added by the {@link Subscription}.
     * @param recordingId       for the replay.
     * @param startPosition     for the replay.
     */
    public ReplayMerge(
        final Subscription subscription,
        final AeronArchive archive,
        final String replayChannel,
        final String replayDestination,
        final String liveDestination,
        final long recordingId,
        final long startPosition)
    {
        this(
            subscription,
            archive,
            replayChannel,
            replayDestination,
            liveDestination,
            recordingId,
            startPosition,
            archive.context().aeron().context().epochClock(),
            MERGE_PROGRESS_TIMEOUT_DEFAULT_MS);
    }

    /**
     * Close and stop any active replay. Will remove the replay destination from the subscription.
     * This operation Will NOT remove the live destination if it has been added, so it can be used for live consumption.
     */
    public void close()
    {
        final State state = this.state;
        if (State.CLOSED != state)
        {
            if (!archive.context().aeron().isClosed())
            {
                if (State.MERGED != state)
                {
                    subscription.asyncRemoveDestination(replayDestination);
                }

                if (isReplayActive && archive.archiveProxy().publication().isConnected())
                {
                    stopReplay();
                }
            }

            state(State.CLOSED);
        }
    }

    /**
     * Get the {@link Subscription} used to consume the replayed and merged stream.
     *
     * @return the {@link Subscription} used to consume the replayed and merged stream.
     */
    public Subscription subscription()
    {
        return subscription;
    }

    /**
     * Perform the work of replaying and merging. Should only be used if polling the underlying {@link Image} directly,
     * call {@link #poll(FragmentHandler, int)} on this class.
     *
     * @return indication of work done processing the merge.
     */
    public int doWork()
    {
        int workCount = 0;
        final long nowMs = epochClock.time();

        try
        {
            switch (state)
            {
                case RESOLVE_REPLAY_PORT:
                    workCount += resolveReplayPort(nowMs);
                    checkProgress(nowMs);
                    break;

                case GET_RECORDING_POSITION:
                    workCount += getRecordingPosition(nowMs);
                    checkProgress(nowMs);
                    break;

                case REPLAY:
                    workCount += replay(nowMs);
                    checkProgress(nowMs);
                    break;

                case CATCHUP:
                    workCount += catchup(nowMs);
                    checkProgress(nowMs);
                    break;

                case ATTEMPT_LIVE_JOIN:
                    workCount += attemptLiveJoin(nowMs);
                    checkProgress(nowMs);
                    break;

                case MERGED:
                case CLOSED:
                case FAILED:
                    break;
            }
        }
        catch (final Exception ex)
        {
            state(State.FAILED);
            throw ex;
        }

        return workCount;
    }

    /**
     * Poll the {@link Image} used for replay and merging and live stream. The {@link ReplayMerge#doWork()} method
     * will be called before the poll so that processing of the merge can be done.
     *
     * @param fragmentHandler to call for fragments.
     * @param fragmentLimit   for poll call.
     * @return number of fragments processed.
     */
    public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
    {
        doWork();
        return null == image ? 0 : image.poll(fragmentHandler, fragmentLimit);
    }

    /**
     * Is the live stream merged and the replay stopped?
     *
     * @return true if live stream is merged and the replay stopped or false if not.
     */
    public boolean isMerged()
    {
        return state == State.MERGED;
    }

    /**
     * Has the replay merge failed due to an error?
     *
     * @return true if replay merge has failed due to an error.
     */
    public boolean hasFailed()
    {
        return state == State.FAILED;
    }

    /**
     * The {@link Image} which is a merge of the replay and live stream.
     *
     * @return the {@link Image} which is a merge of the replay and live stream.
     */
    public Image image()
    {
        return image;
    }

    /**
     * Is the live destination added to the {@link #subscription()}?
     *
     * @return true if live destination added or false if not.
     */
    public boolean isLiveAdded()
    {
        return isLiveAdded;
    }

    private int resolveReplayPort(final long nowMs)
    {
        int workCount = 0;

        final String resolvedEndpoint = subscription.resolvedEndpoint();
        if (null != resolvedEndpoint)
        {
            replayChannelUri.replaceEndpointWildcardPort(resolvedEndpoint);

            timeOfLastProgressMs = nowMs;
            state(State.GET_RECORDING_POSITION);
            workCount += 1;
        }

        return workCount;
    }

    private int getRecordingPosition(final long nowMs)
    {
        int workCount = 0;

        if (NULL_VALUE == activeCorrelationId)
        {
            if (callGetMaxRecordedPosition(nowMs))
            {
                timeOfLastProgressMs = nowMs;
                workCount += 1;
            }
        }
        else if (pollForResponse(archive, activeCorrelationId))
        {
            nextTargetPosition = polledRelevantId(archive);
            activeCorrelationId = NULL_VALUE;

            if (AeronArchive.NULL_POSITION != nextTargetPosition)
            {
                timeOfLastProgressMs = nowMs;
                state(State.REPLAY);
            }

            workCount += 1;
        }

        return workCount;
    }

    private int replay(final long nowMs)
    {
        int workCount = 0;

        if (NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = archive.context().aeron().nextCorrelationId();

            if (archive.archiveProxy().replay(
                recordingId,
                startPosition,
                Long.MAX_VALUE,
                replayChannelUri.toString(),
                subscription.streamId(),
                correlationId,
                archive.controlSessionId()))
            {
                activeCorrelationId = correlationId;
                timeOfLastProgressMs = nowMs;
                workCount += 1;
            }
        }
        else if (pollForResponse(archive, activeCorrelationId))
        {
            isReplayActive = true;
            replaySessionId = polledRelevantId(archive);
            timeOfLastProgressMs = nowMs;
            activeCorrelationId = NULL_VALUE;

            // reset getRecordingPosition backoff when moving to CATCHUP state
            getMaxRecordedPositionBackoffMs = INITIAL_GET_MAX_RECORDED_POSITION_BACKOFF_MS;
            timeOfNextGetMaxRecordedPositionMs = nowMs;

            state(State.CATCHUP);
            workCount += 1;
        }

        return workCount;
    }

    private int catchup(final long nowMs)
    {
        int workCount = 0;

        if (null == image && subscription.isConnected())
        {
            timeOfLastProgressMs = nowMs;
            final Image image = subscription.imageBySessionId((int)replaySessionId);
            if (null == this.image && null != image)
            {
                this.image = image;
                positionOfLastProgress = image.position();
            }
            else
            {
                positionOfLastProgress = NULL_VALUE;
            }
        }

        if (null != image)
        {
            final long position = image.position();
            if (position >= nextTargetPosition)
            {
                timeOfLastProgressMs = nowMs;
                positionOfLastProgress = position;
                state(State.ATTEMPT_LIVE_JOIN);
                workCount += 1;
            }
            else if (position > positionOfLastProgress)
            {
                timeOfLastProgressMs = nowMs;
                positionOfLastProgress = position;
            }
            else if (image.isClosed())
            {
                throw new IllegalStateException("ReplayMerge Image closed unexpectedly.");
            }
        }

        return workCount;
    }

    private int attemptLiveJoin(final long nowMs)
    {
        int workCount = 0;

        if (NULL_VALUE == activeCorrelationId)
        {
            if (callGetMaxRecordedPosition(nowMs))
            {
                timeOfLastProgressMs = nowMs;
                workCount += 1;
            }
        }
        else if (pollForResponse(archive, activeCorrelationId))
        {
            nextTargetPosition = polledRelevantId(archive);
            activeCorrelationId = NULL_VALUE;

            if (AeronArchive.NULL_POSITION != nextTargetPosition)
            {
                State nextState = State.CATCHUP;

                if (null != image)
                {
                    final long position = image.position();
                    if (shouldAddLiveDestination(position))
                    {
                        subscription.asyncAddDestination(liveDestination);
                        timeOfLastProgressMs = nowMs;
                        positionOfLastProgress = position;
                        isLiveAdded = true;
                    }
                    else if (shouldStopAndRemoveReplay(position))
                    {
                        subscription.asyncRemoveDestination(replayDestination);
                        stopReplay();
                        timeOfLastProgressMs = nowMs;
                        positionOfLastProgress = position;
                        nextState = State.MERGED;
                    }
                }

                state(nextState);
            }

            workCount += 1;
        }

        return workCount;
    }

    private boolean callGetMaxRecordedPosition(final long nowMs)
    {
        if (nowMs < timeOfNextGetMaxRecordedPositionMs)
        {
            return false;
        }

        final long correlationId = archive.context().aeron().nextCorrelationId();

        final boolean result = archive.archiveProxy().getMaxRecordedPosition(
            recordingId, correlationId, archive.controlSessionId());

        if (result)
        {
            activeCorrelationId = correlationId;
        }

        // increase backoff regardless of result
        getMaxRecordedPositionBackoffMs = Long.min(
            getMaxRecordedPositionBackoffMs * 2, GET_MAX_RECORDED_POSITION_BACKOFF_MAX_MS);
        timeOfNextGetMaxRecordedPositionMs = nowMs + getMaxRecordedPositionBackoffMs;

        return result;
    }

    private void stopReplay()
    {
        final long correlationId = archive.context().aeron().nextCorrelationId();
        if (archive.archiveProxy().stopReplay(replaySessionId, correlationId, archive.controlSessionId()))
        {
            isReplayActive = false;
        }
    }

    private void state(final ReplayMerge.State newState)
    {
        //System.out.println(state + " -> " + newState);
        state = newState;
        activeCorrelationId = NULL_VALUE;
    }

    private boolean shouldAddLiveDestination(final long position)
    {
        return !isLiveAdded &&
            (nextTargetPosition - position) <= Math.min(image.termBufferLength() >> 2, LIVE_ADD_MAX_WINDOW);
    }

    private boolean shouldStopAndRemoveReplay(final long position)
    {
        return isLiveAdded &&
            (nextTargetPosition - position) <= REPLAY_REMOVE_THRESHOLD &&
            image.activeTransportCount() >= 2;
    }

    private void checkProgress(final long nowMs)
    {
        if (nowMs > (timeOfLastProgressMs + mergeProgressTimeoutMs))
        {
            final int transportCount = null != image ? image.activeTransportCount() : 0;
            throw new TimeoutException(
                "ReplayMerge no progress: state=" + state + ", activeTransportCount=" + transportCount);
        }

        if (NULL_VALUE == activeCorrelationId &&
            (nowMs > (timeOfLastScheduledArchivePollMs + ARCHIVE_POLL_INTERVAL_MS)))
        {
            timeOfLastScheduledArchivePollMs = nowMs;
            pollForResponse(archive, NULL_VALUE);
        }
    }

    private static boolean pollForResponse(final AeronArchive archive, final long correlationId)
    {
        final ControlResponsePoller poller = archive.controlResponsePoller();

        final int pollCount = poller.poll();
        if (poller.isPollComplete())
        {
            if (poller.controlSessionId() == archive.controlSessionId())
            {
                if (poller.code() == ControlResponseCode.ERROR)
                {
                    throw new ArchiveException("archive response for correlationId=" + poller.correlationId() +
                        ", error: " + poller.errorMessage(),
                        (int)poller.relevantId(),
                        poller.correlationId());
                }

                return poller.correlationId() == correlationId;
            }
        }
        else if (pollCount == 0 && !poller.subscription().isConnected())
        {
            throw new ArchiveException("archive is not connected");
        }

        return false;
    }

    private static long polledRelevantId(final AeronArchive archive)
    {
        return archive.controlResponsePoller().relevantId();
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "ReplayMerge{" +
            "state=" + state +
            ", nextTargetPosition=" + nextTargetPosition +
            ", timeOfLastProgressMs=" + timeOfLastProgressMs +
            ", positionOfLastProgress=" + positionOfLastProgress +
            ", isLiveAdded=" + isLiveAdded +
            ", isReplayActive=" + isReplayActive +
            ", replayChannelUri=" + replayChannelUri +
            ", image=" + image +
            '}';
    }
}
