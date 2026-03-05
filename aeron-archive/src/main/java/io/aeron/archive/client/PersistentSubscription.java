/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aeron.archive.client;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.ErrorCode;
import io.aeron.Image;
import io.aeron.ImageControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.exceptions.AeronEvent;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.exceptions.RegistrationException;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.SystemUtil;
import org.agrona.concurrent.NanoClock;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.REPLAY_ALL_AND_FOLLOW;
import static io.aeron.archive.codecs.ControlResponseCode.OK;
import static io.aeron.archive.codecs.ControlResponseCode.RECORDING_UNKNOWN;

/**
 *
 */
public final class PersistentSubscription implements AutoCloseable
{
    public static final long FROM_START = NULL_POSITION;
    public static final long FROM_LIVE = -2;

    private final ImageControlledFragmentAssembler assembler = new ImageControlledFragmentAssembler(this::onFragment);
    private final ControlledFragmentHandler liveCatchupFragmentHandler = this::onLiveCatchupFragment;
    private final ControlledFragmentHandler replayCatchupFragmentHandler = this::onReplayCatchupFragment;
    private final ListRecordingRequest listRecordingRequest = new ListRecordingRequest();
    private final MaxRecordedPosition maxRecordedPosition = new MaxRecordedPosition();
    private final AsyncArchiveOp replayRequest = new AsyncArchiveOp();
    private final Context ctx;
    private final long recordingId;
    private final PersistentSubscriptionListener listener;
    private final String liveChannel;
    private final int liveStreamId;
    private final String replayChannel;
    private final ChannelUri replayChannelUri;
    private final ReplayChannelType replayChannelType;
    private final int replayStreamId;
    private final Aeron aeron;
    private final NanoClock nanoClock;
    private final AsyncAeronArchive asyncAeronArchive;
    private final long messageTimeoutNs;

    private State state;
    private long replaySessionId = Aeron.NULL_VALUE;
    private long replaySubscriptionId = Aeron.NULL_VALUE;
    private Subscription replaySubscription;
    private long replayImageDeadline;
    private Image replayImage;
    private long liveSubscriptionId = Aeron.NULL_VALUE;
    private Subscription liveSubscription;
    private long liveImageDeadline;
    private boolean liveImageDeadlineBreached;
    private Image liveImage;
    private ControlledFragmentHandler controlledFragmentHandler;
    private long joinError;
    private long nextLivePosition = Aeron.NULL_VALUE;
    private long position;

    private PersistentSubscription(final Context ctx)
    {
        ctx.conclude();

        this.ctx = ctx;
        recordingId = ctx.recordingId;
        liveChannel = ctx.liveChannel;
        liveStreamId = ctx.liveStreamId;
        replayChannel = ctx.replayChannel;
        replayChannelUri = ChannelUri.parse(replayChannel);
        replayChannelType = ReplayChannelType.of(replayChannelUri);
        replayStreamId = ctx.replayStreamId;
        listener = ctx.listener;
        aeron = ctx.aeron;
        nanoClock = aeron.context().nanoClock();
        asyncAeronArchive = new AsyncAeronArchive(ctx.aeronArchiveContext().aeron(aeron), new ArchiveListener());
        messageTimeoutNs = ctx.aeronArchiveContext().messageTimeoutNs();
        position = ctx.startPosition;

        state(State.AWAIT_ARCHIVE_CONNECTION);
    }

    public static PersistentSubscription create(final Context ctx)
    {
        return new PersistentSubscription(ctx);
    }

    public int controlledPoll(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        int workCount = asyncAeronArchive.poll();

        workCount += switch (state)
        {
            case AWAIT_ARCHIVE_CONNECTION -> awaitArchiveConnection();
            case SEND_LIST_RECORDING_REQUEST -> sendListRecordingRequest();
            case AWAIT_LIST_RECORDING_RESPONSE -> awaitListRecordingResponse();
            case SEND_REPLAY_REQUEST -> sendReplayRequest();
            case AWAIT_REPLAY_RESPONSE -> awaitReplayResponse();
            case ADD_REPLAY_SUBSCRIPTION -> addReplaySubscription();
            case AWAIT_REPLAY_SUBSCRIPTION -> awaitReplaySubscription();
            case AWAIT_REPLAY_CHANNEL_ENDPOINT -> awaitReplayChannelEndpoint();
            case REPLAY -> replay(fragmentHandler, fragmentLimit);
            case ATTEMPT_SWITCH -> attemptSwitch(fragmentHandler, fragmentLimit);
            case ADD_LIVE_SUBSCRIPTION -> addLiveSubscription();
            case AWAIT_LIVE -> awaitLive();
            case LIVE -> live(fragmentHandler, fragmentLimit);
            case FAILED -> 0;
        };

        return workCount;
    }

    /**
     * Indicates if the persistent subscription is reading from the live stream.
     *
     * @return true if persistent subscription is reading from the live stream.
     */
    public boolean isLive()
    {
        return state == State.LIVE;
    }

    /**
     * Indicates if the persistent subscription is replaying from a recording.
     *
     * @return true if persistent subscription is replaying from a recording.
     */
    public boolean isReplaying()
    {
        return state == State.REPLAY || state == State.ATTEMPT_SWITCH;
    }

    /**
     * Indicates if the persistent subscription failed.
     * <p>
     * The {@link PersistentSubscriptionListener} will be notified of any terminal errors
     * that can cause the persistent subscription to fail.
     *
     * @return true if persistent subscription has failed.
     * @see PersistentSubscriptionListener#onError(Exception)
     */
    public boolean hasFailed()
    {
        return state == State.FAILED;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.closeAll(this::closeReplay, asyncAeronArchive, ctx::close);
    }

    private void closeReplay()
    {
        cleanUpReplay();

        if (!ctx.ownsAeronClient())
        {
            if (replaySubscriptionId != Aeron.NULL_VALUE)
            {
                aeron.asyncRemoveSubscription(replaySubscriptionId);
            }

            if (replaySubscription != null)
            {
                replaySubscription.close();
            }
        }
    }

    long joinError()
    {
        return joinError;
    }

    private int awaitArchiveConnection()
    {
        if (!asyncAeronArchive.isConnected())
        {
            return 0;
        }

        state(State.SEND_LIST_RECORDING_REQUEST);

        return 1;
    }

    private int sendListRecordingRequest()
    {
        final long correlationId = aeron.nextCorrelationId();

        if (!asyncAeronArchive.trySendListRecordingRequest(correlationId, recordingId))
        {
            if (asyncAeronArchive.isConnected())
            {
                return 0;
            }
            else
            {
                state(State.AWAIT_ARCHIVE_CONNECTION);

                return 1;
            }
        }

        listRecordingRequest.init(correlationId, nanoClock.nanoTime() + messageTimeoutNs);
        listRecordingRequest.remaining = 1;

        state(State.AWAIT_LIST_RECORDING_RESPONSE);

        return 1;
    }

    private int awaitListRecordingResponse()
    {
        if (!listRecordingRequest.responseReceived)
        {
            if (nanoClock.nanoTime() - listRecordingRequest.deadlineNs >= 0)
            {
                state(asyncAeronArchive.isConnected() ?
                    State.SEND_LIST_RECORDING_REQUEST :
                    State.AWAIT_ARCHIVE_CONNECTION);

                return 1;
            }

            return 0;
        }

        final PersistentSubscriptionException error = validateDescriptor();

        if (error != null)
        {
            state(State.FAILED);

            listener.onError(error);
        }
        else
        {
            if (position == FROM_LIVE)
            {
                state(State.ADD_LIVE_SUBSCRIPTION);
            }
            else
            {
                setUpReplay();
            }
        }

        return 1;
    }

    private PersistentSubscriptionException validateDescriptor()
    {
        if (listRecordingRequest.remaining == 0)
        {
            assert listRecordingRequest.recordingId == recordingId : listRecordingRequest.toString();

            if (liveStreamId != listRecordingRequest.streamId)
            {
                return new PersistentSubscriptionException(
                    PersistentSubscriptionException.Reason.STREAM_ID_MISMATCH,
                    "Requested live stream with ID: " + liveStreamId + " does not match stream ID: " +
                    listRecordingRequest.streamId + " for recording: " + recordingId);
            }

            if (position >= 0)
            {
                if (position < listRecordingRequest.startPosition)
                {
                    return new PersistentSubscriptionException(
                        PersistentSubscriptionException.Reason.INVALID_START_POSITION,
                        "Requested position: " + position + " is lower than start position: " +
                        listRecordingRequest.startPosition + " for recording: " + recordingId);
                }

                if (listRecordingRequest.stopPosition != NULL_POSITION && position > listRecordingRequest.stopPosition)
                {
                    return new PersistentSubscriptionException(
                        PersistentSubscriptionException.Reason.INVALID_START_POSITION,
                        "Requested position: " + position + " is greater than stop position: " +
                        listRecordingRequest.stopPosition + " for recording: " + recordingId);
                }
            }
            else if (position == FROM_START)
            {
                position = listRecordingRequest.startPosition;
            }
        }
        else
        {
            assert listRecordingRequest.remaining == 1 &&
                   listRecordingRequest.code == RECORDING_UNKNOWN &&
                   listRecordingRequest.relevantId == recordingId : listRecordingRequest.toString();

            return new PersistentSubscriptionException(
                PersistentSubscriptionException.Reason.RECORDING_NOT_FOUND,
                "No recording found with ID: " + recordingId);
        }

        return null;
    }

    private void setUpReplay()
    {
        joinError = Long.MIN_VALUE;
        maxRecordedPosition.reset(listRecordingRequest.termBufferLength >> 2);

        state(switch (replayChannelType)
        {
            case SESSION_SPECIFIC -> State.SEND_REPLAY_REQUEST;
            case DYNAMIC_PORT -> State.ADD_REPLAY_SUBSCRIPTION;
        });
    }

    private void cleanUpReplay()
    {
        if (replaySessionId != Aeron.NULL_VALUE)
        {
            asyncAeronArchive.trySendStopReplayRequest(aeron.nextCorrelationId(), replaySessionId);

            replaySessionId = Aeron.NULL_VALUE;
        }
    }

    private void cleanUpReplaySubscription()
    {
        if (replaySubscriptionId != Aeron.NULL_VALUE)
        {
            aeron.asyncRemoveSubscription(replaySubscriptionId);
        }

        if (replaySubscription != null)
        {
            aeron.asyncRemoveSubscription(replaySubscription.registrationId());
        }

        replaySubscriptionId = Aeron.NULL_VALUE;
        replaySubscription = null;
        replayImage = null;
    }

    private void cleanUpLiveSubscription()
    {
        if (liveSubscriptionId != Aeron.NULL_VALUE)
        {
            aeron.asyncRemoveSubscription(liveSubscriptionId);
        }

        if (liveSubscription != null)
        {
            aeron.asyncRemoveSubscription(liveSubscription.registrationId());
        }

        liveSubscriptionId = Aeron.NULL_VALUE;
        liveSubscription = null;
        liveImage = null;
    }

    private int sendReplayRequest()
    {
        final long correlationId = aeron.nextCorrelationId();

        final String channel = switch (replayChannelType)
        {
            case SESSION_SPECIFIC -> replayChannel;
            case DYNAMIC_PORT -> replayChannelUri.toString();
        };

        if (!asyncAeronArchive.trySendReplayRequest(
            correlationId,
            recordingId,
            position,
            REPLAY_ALL_AND_FOLLOW,
            replayStreamId,
            channel))
        {
            if (asyncAeronArchive.isConnected())
            {
                return 0;
            }
            else
            {
                cleanUpReplaySubscription();

                state(State.AWAIT_ARCHIVE_CONNECTION);

                return 1;
            }
        }

        replayRequest.init(correlationId, nanoClock.nanoTime() + messageTimeoutNs);

        state(State.AWAIT_REPLAY_RESPONSE);

        return 1;
    }

    private int awaitReplayResponse()
    {
        if (!replayRequest.responseReceived)
        {
            if (nanoClock.nanoTime() - replayRequest.deadlineNs >= 0)
            {
                if (asyncAeronArchive.isConnected())
                {
                    cleanUpReplaySubscription();

                    setUpReplay();
                }
                else
                {
                    state(State.AWAIT_ARCHIVE_CONNECTION);
                }

                return 1;
            }

            return 0;
        }

        if (replayRequest.code != OK)
        {
            state(State.FAILED);

            cleanUpReplaySubscription();

            // TODO translate those to PersistentSubscriptionException whenever we can to make errors consistent?
            listener.onError(new ArchiveException(
                "replay request failed: " + replayRequest.errorMessage,
                (int)replayRequest.relevantId,
                replayRequest.correlationId));

            return 1;
        }

        replaySessionId = replayRequest.relevantId;

        return switch (replayChannelType)
        {
            case SESSION_SPECIFIC ->
            {
                replayChannelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString((int)replaySessionId));

                state(State.ADD_REPLAY_SUBSCRIPTION);

                yield 1;
            }
            case DYNAMIC_PORT ->
            {
                replayImageDeadline = nanoClock.nanoTime() + messageTimeoutNs;

                state(State.REPLAY);

                yield 1;
            }
        };
    }

    private int addReplaySubscription()
    {
        final String channel = switch (replayChannelType)
        {
            case SESSION_SPECIFIC -> replayChannelUri.toString();
            case DYNAMIC_PORT -> replayChannel;
        };

        replaySubscriptionId = aeron.asyncAddSubscription(channel, replayStreamId);

        state(State.AWAIT_REPLAY_SUBSCRIPTION);

        return 1;
    }

    private int awaitReplaySubscription()
    {
        final Subscription subscription;
        try
        {
            subscription = aeron.getSubscription(replaySubscriptionId);
        }
        catch (final RegistrationException e)
        {
            replaySubscriptionId = Aeron.NULL_VALUE;

            cleanUpReplay();

            if (e.errorCode() == ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE)
            {
                setUpReplay();
            }
            else
            {
                state(State.FAILED);
            }

            listener.onError(e);

            return 1;
        }

        if (subscription == null)
        {
            return 0;
        }

        replaySubscriptionId = Aeron.NULL_VALUE;
        replaySubscription = subscription;

        if (replayChannelType == ReplayChannelType.SESSION_SPECIFIC)
        {
            replayImageDeadline = nanoClock.nanoTime() + messageTimeoutNs;
        }

        state(switch (replayChannelType)
        {
            case SESSION_SPECIFIC -> State.REPLAY;
            case DYNAMIC_PORT -> State.AWAIT_REPLAY_CHANNEL_ENDPOINT;
        });

        return 1;
    }

    private int awaitReplayChannelEndpoint()
    {
        final String endpoint = replaySubscription.resolvedEndpoint();

        if (endpoint == null)
        {
            return 0;
        }

        replayChannelUri.put(ENDPOINT_PARAM_NAME, endpoint);

        state(State.SEND_REPLAY_REQUEST);

        return 1;
    }

    private int replay(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        Image replayImage = this.replayImage;

        if (replayImage == null)
        {
            replayImage = replaySubscription.imageBySessionId((int)replaySessionId);

            if (replayImage == null)
            {
                if (nanoClock.nanoTime() - replayImageDeadline >= 0)
                {
                    cleanUpReplay();
                    cleanUpReplaySubscription();
                    setUpReplay();

                    return 1;
                }

                return 0;
            }

            this.replayImage = replayImage;
        }

        if (replayImage.isClosed())
        {
            cleanUpLiveSubscription();
            cleanUpReplay();
            cleanUpReplaySubscription();
            setUpReplay();

            return 1;
        }

        if (liveSubscription == null && liveSubscriptionId != Aeron.NULL_VALUE)
        {
            try
            {
                liveSubscription = aeron.getSubscription(liveSubscriptionId);

                if (liveSubscription != null)
                {
                    liveSubscriptionId = Aeron.NULL_VALUE;
                    setLiveImageDeadline();
                }
            }
            catch (final RegistrationException e)
            {
                liveSubscriptionId = Aeron.NULL_VALUE;

                if (e.errorCode() != ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE)
                {
                    cleanUpReplay();
                    cleanUpReplaySubscription();
                    state(State.FAILED);
                }

                listener.onError(e);

                return 1;
            }
        }

        if (liveSubscription != null)
        {
            if (liveSubscription.imageCount() > 0)
            {
                liveImage = liveSubscription.imageAtIndex(0);

                final long livePosition = liveImage.position();
                final long replayPosition = replayImage.position();
                joinError = livePosition - replayPosition;

                state(State.ATTEMPT_SWITCH);

                return 1;
            }
            else if (!liveImageDeadlineBreached && nanoClock.nanoTime() - liveImageDeadline >= 0)
            {
                onLiveImageDeadlineBreached();
            }
        }

        final int fragments = controlledPoll(replayImage, fragmentHandler, fragmentLimit);

        position = replayImage.position();

        if (liveSubscriptionId == Aeron.NULL_VALUE &&
            liveSubscription == null &&
            maxRecordedPosition.caughtUp(position))
        {
            doAddLiveSubscription();
        }

        return fragments;
    }

    private void doAddLiveSubscription()
    {
        liveImage = null;
        liveSubscription = null;
        liveSubscriptionId = aeron.asyncAddSubscription(liveChannel, liveStreamId);
    }

    private void setLiveImageDeadline()
    {
        liveImageDeadline = nanoClock.nanoTime() + messageTimeoutNs;
        liveImageDeadlineBreached = false;
    }

    private void onLiveImageDeadlineBreached()
    {
        liveImageDeadlineBreached = true;
        listener.onError(new AeronEvent("No image became available on the live subscription within " +
                                        SystemUtil.formatDuration(messageTimeoutNs) + ". This could be " +
                                        "caused by the publisher being down, or by a misconfiguration of the " +
                                        "subscriber or a firewall between them."));
    }

    private int attemptSwitch(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        int fragments = 0;

        final long livePosition = liveImage.position();
        final long replayPosition = replayImage.position();

        if (replayPosition == livePosition)
        {
            state(State.LIVE);
        }
        else
        {
            if (replayImage.isClosed())
            {
                cleanUpLiveSubscription();
                cleanUpReplay();
                cleanUpReplaySubscription();
                setUpReplay();

                return 1;
            }

            if (liveImage.isClosed())
            {
                cleanUpLiveSubscription();

                joinError = Long.MIN_VALUE;
                maxRecordedPosition.reset(listRecordingRequest.termBufferLength >> 2);

                state(State.REPLAY);

                return 1;
            }

            // Let the live channel catch up to the point we are at in the replay (but don't overtake it).
            fragments += liveImage.controlledPoll(liveCatchupFragmentHandler, fragmentLimit);

            // Carry on with the replay for now.
            controlledFragmentHandler = fragmentHandler;
            try
            {
                fragments += replayImage.controlledPoll(replayCatchupFragmentHandler, fragmentLimit);
                position = replayImage.position();
            }
            finally
            {
                controlledFragmentHandler = null;
            }
        }

        if (isLive())
        {
            cleanUpReplay();
            cleanUpReplaySubscription();
            listener.onLiveJoined();
        }

        return fragments;
    }

    private ControlledFragmentHandler.Action onLiveCatchupFragment(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        final long currentLivePosition = header.position();
        final long lastReplayPosition = replayImage.position();
        if (currentLivePosition <= lastReplayPosition)
        {
            return ControlledFragmentHandler.Action.CONTINUE;
        }
        nextLivePosition = currentLivePosition;
        return ControlledFragmentHandler.Action.ABORT;
    }

    private ControlledFragmentHandler.Action onReplayCatchupFragment(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        final long currentReplayPosition = header.position();
        if (currentReplayPosition == nextLivePosition)
        {
            state(State.LIVE);
            return ControlledFragmentHandler.Action.ABORT;
        }
        return assembler.onFragment(buffer, offset, length, header);
    }

    private int addLiveSubscription()
    {
        doAddLiveSubscription();

        state(State.AWAIT_LIVE);

        return 1;
    }

    private int awaitLive()
    {
        // awaiting live subscription or its image before going directly to live (no replay or switch)

        if (liveSubscription == null)
        {
            try
            {
                liveSubscription = aeron.getSubscription(liveSubscriptionId);

                if (liveSubscription != null)
                {
                    liveSubscriptionId = Aeron.NULL_VALUE;
                    setLiveImageDeadline();
                }
            }
            catch (final RegistrationException e)
            {
                liveSubscriptionId = Aeron.NULL_VALUE;

                if (e.errorCode() == ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE)
                {
                    state(State.ADD_LIVE_SUBSCRIPTION);
                }
                else
                {
                    state(State.FAILED);
                }

                listener.onError(e);

                return 1;
            }
        }

        if (liveSubscription != null)
        {
            if (liveSubscription.imageCount() > 0)
            {
                liveImage = liveSubscription.imageAtIndex(0);
                position = liveImage.position();
                joinError = 0;

                state(State.LIVE);
                listener.onLiveJoined();

                return 1;
            }
            else if (!liveImageDeadlineBreached && nanoClock.nanoTime() - liveImageDeadline >= 0)
            {
                onLiveImageDeadlineBreached();
            }
        }

        return 0;
    }

    private int live(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        int workCount = 0;

        final Image image = liveImage;
        if (!image.isClosed())
        {
            workCount += controlledPoll(image, fragmentHandler, fragmentLimit);
            position = image.position(); // TODO what about updating after handler throws? can we query the right image position when we subscribe?
        }
        else
        {
            cleanUpLiveSubscription();
            setUpReplay();
            listener.onLiveLeft();
            workCount++;
        }

        return workCount;
    }

    private void state(final State newState)
    {
        System.out.println("State: " + state + " -> " + newState);
        if (newState != this.state)
        {
            this.state = newState;
        }
    }

    private int controlledPoll(
        final Image image,
        final ControlledFragmentHandler fragmentHandler,
        final int fragmentLimit)
    {
        controlledFragmentHandler = fragmentHandler;
        try
        {
            return image.controlledPoll(assembler, fragmentLimit);
        }
        finally
        {
            controlledFragmentHandler = null;
        }
    }

    private ControlledFragmentHandler.Action onFragment(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        return controlledFragmentHandler.onFragment(buffer, offset, length, header);
    }

    private enum ReplayChannelType
    {
        SESSION_SPECIFIC,
        DYNAMIC_PORT;

        static ReplayChannelType of(final ChannelUri channelUri)
        {
            if (channelUri.isUdp())
            {
                final String endpoint = channelUri.get(ENDPOINT_PARAM_NAME);
                if (endpoint != null && endpoint.endsWith(":0"))
                {
                    return DYNAMIC_PORT;
                }
            }
            return SESSION_SPECIFIC;
        }
    }

    private enum State
    {
        AWAIT_ARCHIVE_CONNECTION,
        SEND_LIST_RECORDING_REQUEST,
        AWAIT_LIST_RECORDING_RESPONSE,
        SEND_REPLAY_REQUEST,
        AWAIT_REPLAY_RESPONSE,
        ADD_REPLAY_SUBSCRIPTION,
        AWAIT_REPLAY_SUBSCRIPTION,
        AWAIT_REPLAY_CHANNEL_ENDPOINT,
        REPLAY,
        ATTEMPT_SWITCH,
        ADD_LIVE_SUBSCRIPTION,
        AWAIT_LIVE,
        LIVE,
        FAILED,
    }

    private static class AsyncArchiveOp
    {
        protected long correlationId;
        protected long deadlineNs;

        protected long relevantId;
        protected ControlResponseCode code;
        protected String errorMessage;

        protected boolean responseReceived;

        void init(final long correlationId, final long deadlineNs)
        {
            this.correlationId = correlationId;
            this.deadlineNs = deadlineNs;

            responseReceived = false;
        }

        void onControlResponse(final long relevantId, final ControlResponseCode code, final String errorMessage)
        {
            this.relevantId = relevantId;
            this.code = code;
            this.errorMessage = errorMessage;

            responseReceived = true;
        }
    }

    private static final class ListRecordingRequest
        extends AsyncArchiveOp
        implements RecordingDescriptorConsumer
    {
        int remaining;

        long recordingId;
        long startPosition;
        long stopPosition;
        int termBufferLength;
        int streamId;

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
            this.recordingId = recordingId;
            this.startPosition = startPosition;
            this.stopPosition = stopPosition;
            this.termBufferLength = termBufferLength;
            this.streamId = streamId;

            if (--remaining == 0)
            {
                responseReceived = true;
            }
        }

        public String toString()
        {
            return "ListRecordingRequest{" +
                   "remaining=" + remaining +
                   ", recordingId=" + recordingId +
                   ", startPosition=" + startPosition +
                   ", stopPosition=" + stopPosition +
                   ", termBufferLength=" + termBufferLength +
                   ", streamId=" + streamId +
                   ", correlationId=" + correlationId +
                   ", deadlineNs=" + deadlineNs +
                   ", relevantId=" + relevantId +
                   ", code=" + code +
                   ", errorMessage='" + errorMessage + '\'' +
                   ", responseReceived=" + responseReceived +
                   '}';
        }
    }

    public static class Context implements Cloneable
    {
        private static final VarHandle IS_CONCLUDED_VH;

        static
        {
            try
            {
                IS_CONCLUDED_VH = MethodHandles.lookup().findVarHandle(Context.class, "isConcluded", boolean.class);
            }
            catch (final ReflectiveOperationException ex)
            {
                throw new ExceptionInInitializerError(ex);
            }
        }

        private volatile boolean isConcluded;
        private Aeron aeron;
        private boolean ownsAeronClient;
        private String aeronDirectoryName;
        private long recordingId = Aeron.NULL_VALUE;
        private long startPosition = FROM_LIVE;
        private String liveChannel = null;
        private int liveStreamId = Aeron.NULL_VALUE;
        private String replayChannel = null;
        private int replayStreamId = Aeron.NULL_VALUE;
        private PersistentSubscriptionListener listener = null;
        private AeronArchive.Context aeronArchiveContext = null;

        public Context aeron(final Aeron aeron)
        {
            this.aeron = aeron;
            return this;
        }

        public Aeron aeron()
        {
            return aeron;
        }

        public boolean ownsAeronClient()
        {
            return ownsAeronClient;
        }

        public Context ownsAeronClient(final boolean ownsAeronClient)
        {
            this.ownsAeronClient = ownsAeronClient;
            return this;
        }

        public String aeronDirectoryName()
        {
            return aeronDirectoryName;
        }

        public Context aeronDirectoryName(final String aeronDirectoryName)
        {
            this.aeronDirectoryName = aeronDirectoryName;
            return this;
        }

        public Context recordingId(final long recordingId)
        {
            this.recordingId = recordingId;
            return this;
        }

        /**
         * The position to start consuming from or {@link #FROM_START} or {@link #FROM_LIVE}.
         *
         * @param startPosition
         * @return
         */
        public Context startPosition(final long startPosition)
        {
            this.startPosition = startPosition;
            return this;
        }

        public Context liveChannel(final String liveChannel)
        {
            this.liveChannel = liveChannel;
            return this;
        }

        public Context liveStreamId(final int liveStreamId)
        {
            this.liveStreamId = liveStreamId;
            return this;
        }

        public Context replayChannel(final String replayChannel)
        {
            this.replayChannel = replayChannel;
            return this;
        }

        public Context replayStreamId(final int replayStreamId)
        {
            this.replayStreamId = replayStreamId;
            return this;
        }

        public Context listener(final PersistentSubscriptionListener listener)
        {
            this.listener = listener;
            return this;
        }

        public Context aeronArchiveContext(final AeronArchive.Context aeronArchiveContext)
        {
            this.aeronArchiveContext = aeronArchiveContext;
            return this;
        }

        public long recordingId()
        {
            return recordingId;
        }

        public long startPosition()
        {
            return startPosition;
        }

        public String liveChannel()
        {
            return liveChannel;
        }

        public int liveStreamId()
        {
            return liveStreamId;
        }

        public String replayChannel()
        {
            return replayChannel;
        }

        public int replayStreamId()
        {
            return replayStreamId;
        }

        public PersistentSubscriptionListener listener()
        {
            return listener;
        }

        public AeronArchive.Context aeronArchiveContext()
        {
            return aeronArchiveContext;
        }

        public boolean isConcluded()
        {
            return isConcluded;
        }

        public void conclude()
        {
            if ((boolean)IS_CONCLUDED_VH.getAndSet(this, true))
            {
                throw new ConcurrentConcludeException();
            }

            if (recordingId == Aeron.NULL_VALUE)
            {
                throw new ConfigurationException("recordingId must be set");
            }

            if (liveStreamId == Aeron.NULL_VALUE)
            {
                throw new ConfigurationException("liveStreamId must be set");
            }

            if (liveChannel == null)
            {
                throw new ConfigurationException("liveChannel must be set");
            }

            if (replayChannel == null)
            {
                throw new ConfigurationException("replayChannel must be set");
            }

            if (replayStreamId == Aeron.NULL_VALUE)
            {
                throw new ConfigurationException("replayStreamId must be set");
            }

            if (aeronArchiveContext == null)
            {
                throw new ConfigurationException("aeronArchiveContext must be set");
            }

            if (listener == null)
            {
                listener = new NoOpPersistentSubscriptionListener();
            }

            if (recordingId < 0)
            {
                throw new ConfigurationException("invalid recordingId " + recordingId);
            }

            if (startPosition < FROM_LIVE)
            {
                throw new ConfigurationException("invalid startPosition " + startPosition);
            }

            if (aeron == null)
            {
                final Aeron.Context aeronCtx = new Aeron.Context()
                    .clientName("PersistentSubscription");
                if (aeronDirectoryName != null)
                {
                    aeronCtx.aeronDirectoryName(aeronDirectoryName);
                }
                aeron = Aeron.connect(aeronCtx);
                ownsAeronClient = true;
            }
        }

        /**
         * Perform a shallow copy of the object.
         *
         * @return a shallow copy of the object.
         */
        public Context clone()
        {
            try
            {
                return (Context)super.clone();
            }
            catch (final CloneNotSupportedException ex)
            {
                throw new RuntimeException(ex);
            }
        }

        public void close()
        {
            if (ownsAeronClient)
            {
                CloseHelper.close(aeron);
            }
        }
    }

    private static class NoOpPersistentSubscriptionListener implements PersistentSubscriptionListener
    {
        public void onLiveJoined()
        {

        }

        public void onLiveLeft()
        {

        }

        public void onError(final Exception e)
        {

        }
    }

    private class MaxRecordedPosition extends AsyncArchiveOp
    {
        private enum MaxRecordedPositionState
        {
            REQUEST_MAX_POSITION,
            AWAIT_MAX_POSITION,
            RECHECK_REQUIRED
        }

        private MaxRecordedPositionState state = MaxRecordedPositionState.REQUEST_MAX_POSITION;
        private long maxRecordedPosition;
        private int closeEnoughThreshold;

        public void reset(final int closeEnoughThreshold)
        {
            this.closeEnoughThreshold = closeEnoughThreshold;
            this.state = MaxRecordedPositionState.REQUEST_MAX_POSITION;
        }

        public boolean caughtUp(final long replayedPosition)
        {
            return switch (state)
            {
                case REQUEST_MAX_POSITION -> requestMaxPosition();
                case AWAIT_MAX_POSITION -> awaitMaxPosition(replayedPosition);
                case RECHECK_REQUIRED -> recheckRequired(replayedPosition);
            };
        }

        private boolean requestMaxPosition()
        {
            final long correlationId = aeron.nextCorrelationId();
            if (asyncAeronArchive.trySendMaxRecordedPositionRequest(correlationId, recordingId))
            {
                init(correlationId, nanoClock.nanoTime() + messageTimeoutNs);
                state = MaxRecordedPositionState.AWAIT_MAX_POSITION;
            }
            return false;
        }

        private boolean awaitMaxPosition(final long replayedPosition)
        {
            if (responseReceived)
            {
                if (code == OK)
                {
                    maxRecordedPosition = relevantId;
                    if (closeEnoughToSwitch(replayedPosition, maxRecordedPosition))
                    {
                        return true;
                    }
                    else
                    {
                        state = MaxRecordedPositionState.RECHECK_REQUIRED;
                        return false;
                    }
                }
                else
                {
                    // An error here is not recoverable, so fail the Persistent Subscription.
                    final ArchiveException archiveException = new ArchiveException("get max position request failed code=" + code +
                        " relevantId=" + relevantId +
                        " errorMessage='" + errorMessage + "'");
                    listener.onError(archiveException);
                    state(State.FAILED);
                }
            }
            else
            {
                if (deadlineNs - nanoClock.nanoTime() < 0)
                {
                    state = MaxRecordedPositionState.REQUEST_MAX_POSITION;
                }
            }
            return false;
        }

        private boolean recheckRequired(final long replayedPosition)
        {
            if (closeEnoughToReCheck(replayedPosition))
            {
                state = MaxRecordedPositionState.REQUEST_MAX_POSITION;
            }
            return false;
        }

        private boolean closeEnoughToSwitch(final long replayedPosition, final long maxRecordedPosition)
        {
            return replayedPosition >= maxRecordedPosition - closeEnoughThreshold;
        }

        private boolean closeEnoughToReCheck(final long replayedPosition)
        {
            return replayedPosition >= maxRecordedPosition;
        }
    }

    private class ArchiveListener implements AsyncAeronArchiveListener
    {
        public void onConnected()
        {
        }

        public void onDisconnected()
        {
            if (state == State.AWAIT_ARCHIVE_CONNECTION ||
                state == State.ATTEMPT_SWITCH ||
                state == State.LIVE ||
                state == State.FAILED)
            {
                return;
            }

            cleanUpLiveSubscription();
            cleanUpReplay();
            cleanUpReplaySubscription();

            state(State.AWAIT_ARCHIVE_CONNECTION);
        }

        public void onControlResponse(
            final long correlationId,
            final long relevantId,
            final ControlResponseCode code,
            final String errorMessage)
        {
            if (correlationId == maxRecordedPosition.correlationId)
            {
                maxRecordedPosition.onControlResponse(relevantId, code, errorMessage);
            }
            else if (correlationId == listRecordingRequest.correlationId)
            {
                listRecordingRequest.onControlResponse(relevantId, code, errorMessage);
            }
            else if (correlationId == replayRequest.correlationId)
            {
                replayRequest.onControlResponse(relevantId, code, errorMessage);
            }
        }

        public void onError(final Exception error)
        {
            if (asyncAeronArchive.isClosed())
            {
                state(State.FAILED);
            }
            listener.onError(error);
        }

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
            if (correlationId == listRecordingRequest.correlationId)
            {
                listRecordingRequest.onRecordingDescriptor(
                    controlSessionId,
                    correlationId,
                    recordingId,
                    startTimestamp,
                    stopTimestamp,
                    startPosition,
                    stopPosition,
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
        }
    }
}
