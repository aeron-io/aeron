/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Counter;
import io.aeron.ErrorCode;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.ImageControlledFragmentAssembler;
import io.aeron.ImageFragmentAssembler;
import io.aeron.RethrowingErrorHandler;
import io.aeron.Subscription;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.exceptions.AeronEvent;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.exceptions.RegistrationException;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.Strings;
import org.agrona.SystemUtil;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.NanoClock;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static io.aeron.AeronCounters.PERSISTENT_SUBSCRIPTION_JOIN_DIFFERENCE_TYPE_ID;
import static io.aeron.AeronCounters.PERSISTENT_SUBSCRIPTION_LIVE_JOINED_COUNT_TYPE_ID;
import static io.aeron.AeronCounters.PERSISTENT_SUBSCRIPTION_LIVE_LEFT_COUNT_TYPE_ID;
import static io.aeron.AeronCounters.PERSISTENT_SUBSCRIPTION_STATE_TYPE_ID;
import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.REPLAY_ALL_AND_FOLLOW;
import static io.aeron.archive.codecs.ControlResponseCode.OK;
import static io.aeron.archive.codecs.ControlResponseCode.RECORDING_UNKNOWN;

/**
 * A {@code PersistentSubscription} allows the consumption of messages from a live publication that is also being
 * recorded to an Archive, in order and without gaps, regardless of when the messages were published.
 * It tries to read messages from the live subscription as much as possible, falling back to an Archive replay when
 * necessary, making any source switches transparent to the application.
 * <p>
 * It offers:
 * <ul>
 *     <li>late join - if any messages have been published after the provided start position, they will be replayed from
 *     the recording before switching to the live subscription,</li>
 *     <li>seamless recovery - if a subscription gets disconnected due to flow control or a network issue, it will
 *     automatically recover and replay any missed messages.</li>
 * </ul>
 * <p>
 * Not thread-safe. Must be polled in a duty cycle. Performs message reassembly.
 */
public final class PersistentSubscription implements AutoCloseable
{
    /**
     * Special value for {@link Context#startPosition(long)} which will make a {@code PersistentSubscription} start by
     * replaying the recording from its start position. Used when an application needs to process all historical and
     * future messages.
     */
    public static final long FROM_START = NULL_POSITION;

    /**
     * Special value for {@link Context#startPosition(long)} which will make a {@code PersistentSubscription} start by
     * joining the live subscription. Used when an application needs to process all future messages from an unspecified
     * join position, but does not need any historical ones.
     */
    public static final long FROM_LIVE = -2;

    private final ImageControlledFragmentAssembler controlledFragmentAssembler = new ImageControlledFragmentAssembler(
        this::onFragmentControlled
    );
    private final ImageFragmentAssembler uncontrolledFragmentAssembler = new ImageFragmentAssembler(
        this::onFragmentUncontrolled
    );
    private final ControlledFragmentHandler liveCatchupFragmentHandler = this::onLiveCatchupFragment;
    private final ControlledFragmentHandler replayCatchupControlledFragmentHandler =
        this::onReplayCatchupFragmentControlled;
    private final ControlledFragmentHandler replayCatchupUncontrolledFragmentHandler =
        this::onReplayCatchupFragmentUncontrolled;
    private final ListRecordingRequest listRecordingRequest = new ListRecordingRequest();
    private final MaxRecordedPosition maxRecordedPosition = new MaxRecordedPosition();
    private final AsyncArchiveOp replayRequest = new AsyncArchiveOp();
    private final AsyncArchiveOp replayTokenRequest = new AsyncArchiveOp();
    private final ReplayParams replayParams = new ReplayParams();
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
    private final Counter stateCounter;
    private final Counter joinDifferenceCounter;
    private final Counter liveLeftCounter;
    private final Counter liveJoinedCounter;

    private State state;
    private long replaySessionId = Aeron.NULL_VALUE;
    private long replaySubscriptionId = Aeron.NULL_VALUE;
    private Subscription replaySubscription;
    private long replayImageDeadline;
    private Image replayImage;
    private long requestPublicationId;
    private ExclusivePublication requestPublication;
    private ArchiveProxy responseChannelArchiveProxy;
    private long replayToken = Aeron.NULL_VALUE;
    private long liveSubscriptionId = Aeron.NULL_VALUE;
    private Subscription liveSubscription;
    private long liveImageDeadline;
    private boolean liveImageDeadlineBreached;
    private Image liveImage;
    private ControlledFragmentHandler controlledFragmentHandler;
    private FragmentHandler uncontrolledFragmentHandler;
    private long joinDifference;
    private long nextLivePosition = Aeron.NULL_VALUE;
    private long position;
    private long lastObservedLivePosition = Aeron.NULL_VALUE;
    private Exception failureReason = null;

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
        asyncAeronArchive = new AsyncAeronArchive(ctx.aeronArchiveContext(), new ArchiveListener());
        messageTimeoutNs = ctx.aeronArchiveContext().messageTimeoutNs();
        stateCounter = ctx.stateCounter;
        joinDifferenceCounter = ctx.joinDifferenceCounter;
        liveLeftCounter = ctx.liveLeftCounter;
        liveJoinedCounter = ctx.liveJoinedCounter;
        position = ctx.startPosition;

        joinDifference(Long.MIN_VALUE);

        state = State.AWAIT_ARCHIVE_CONNECTION;

        if (!stateCounter.isClosed())
        {
            stateCounter.setRelease(state.code);
        }
    }

    /**
     * Creates a new {@code PersistentSubscription} using the given configuration. The returned instance must be polled
     * to perform any work.
     *
     * @param ctx the configuration to use for the new {@code PersistentSubscription}.
     * @return a new PersistentSubscription.
     * @see #poll(FragmentHandler, int)
     * @see #controlledPoll(ControlledFragmentHandler, int)
     */
    public static PersistentSubscription create(final Context ctx)
    {
        return new PersistentSubscription(ctx);
    }

    /**
     * Poll for the next available message(s). The handler receives fully assembled messages; the action it returns
     * applies to the assembled message as a whole.
     * <p>
     * Either this method or {@link #controlledPoll(ControlledFragmentHandler, int)} must be called in a duty cycle for
     * the {@code PersistentSubscription} to perform its work.
     * <p>
     * If an error occurs during polling, the {@link PersistentSubscriptionListener} will be notified.
     * Exceptions thrown from the {@link FragmentHandler} will be passed to the
     * {@link Aeron.Context#subscriberErrorHandler} for the Aeron instance.
     *
     * @param fragmentHandler the handler to receive assembled messages if any are available.
     * @param fragmentLimit the maximum number of fragments to be processed during the poll operation.
     * @return positive number if work has been done, 0 otherwise.
     */
    public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
    {
        try
        {
            this.uncontrolledFragmentHandler = fragmentHandler;
            return doWork(fragmentLimit, false);
        }
        finally
        {
            this.uncontrolledFragmentHandler = null;
        }
    }

    /**
     * Poll for the next available message(s), allowing the {@link ControlledFragmentHandler} to control
     * whether polling continues by returning an {@link io.aeron.logbuffer.ControlledFragmentHandler.Action}.
     * The handler receives fully assembled messages; the action it returns applies to the assembled message as a whole.
     * <p>
     * Either this method or {@link #poll(FragmentHandler, int)} must be called in a duty cycle for the
     * {@code PersistentSubscription} to perform its work.
     * <p>
     * If an error occurs during polling, the {@link PersistentSubscriptionListener} will be notified.
     * Exceptions thrown from the {@link FragmentHandler} will be passed to the
     * {@link Aeron.Context#subscriberErrorHandler} for the Aeron instance.
     *
     * @param fragmentHandler the handler to receive assembled messages if any are available.
     * @param fragmentLimit the maximum number of fragments to be processed during the poll operation.
     * @return positive number if work has been done, 0 otherwise.
     */
    public int controlledPoll(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        try
        {
            controlledFragmentHandler = fragmentHandler;
            return doWork(fragmentLimit, true);
        }
        finally
        {
            controlledFragmentHandler = null;
        }
    }

    private int doWork(final int fragmentLimit, final boolean isControlled)
    {
        int workCount = 0;
        final AgentInvoker agentInvoker = aeron.conductorAgentInvoker();
        if (null != agentInvoker)
        {
            workCount += agentInvoker.invoke();
        }
        workCount += asyncAeronArchive.poll();

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
            case ADD_REQUEST_PUBLICATION -> addRequestPublication();
            case AWAIT_REQUEST_PUBLICATION -> awaitRequestPublication();
            case SEND_REPLAY_TOKEN_REQUEST -> sendReplayTokenRequest();
            case AWAIT_REPLAY_TOKEN -> awaitReplayToken();
            case REPLAY -> replay(fragmentLimit, isControlled);
            case ATTEMPT_SWITCH -> attemptSwitch(fragmentLimit, isControlled);
            case ADD_LIVE_SUBSCRIPTION -> addLiveSubscription();
            case AWAIT_LIVE -> awaitLive();
            case LIVE -> live(fragmentLimit, isControlled);
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
        return State.LIVE == state;
    }

    /**
     * Indicates if the persistent subscription is replaying from a recording.
     *
     * @return true if persistent subscription is replaying from a recording.
     */
    public boolean isReplaying()
    {
        return State.REPLAY == state || State.ATTEMPT_SWITCH == state;
    }

    /**
     * Indicates if the persistent subscription failed.
     * <p>
     * The {@link PersistentSubscriptionListener} will be notified of any terminal errors
     * that can cause the persistent subscription to fail.
     *
     * @return true if persistent subscription has failed.
     * @see PersistentSubscription#failureReason()
     * @see PersistentSubscriptionListener#onError(Exception)
     */
    public boolean hasFailed()
    {
        return State.FAILED == state;
    }

    /**
     * The terminal error that caused the persistent subscription to fail.
     * Only meaningful when {@link #hasFailed()} returns {@code true}.
     *
     * @return exception indicating the failure reason, or {@code null} if not in the failed state.
     * @see PersistentSubscription#hasFailed()
     */
    public Exception failureReason()
    {
        return failureReason;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.closeAll(this::closeLive, this::closeReplay, asyncAeronArchive, ctx::close);
    }

    private void closeLive()
    {
        if (!ctx.ownsAeronClient())
        {
            if (Aeron.NULL_VALUE != liveSubscriptionId)
            {
                aeron.asyncRemoveSubscription(liveSubscriptionId);
            }

            if (null != liveSubscription)
            {
                liveSubscription.close();
            }
        }
    }

    private void closeReplay()
    {
        cleanUpReplay();

        if (!ctx.ownsAeronClient())
        {
            if (Aeron.NULL_VALUE != requestPublicationId)
            {
                aeron.asyncRemovePublication(requestPublicationId);
            }

            if (null != requestPublication)
            {
                requestPublication.close();
            }

            if (Aeron.NULL_VALUE != replaySubscriptionId)
            {
                aeron.asyncRemoveSubscription(replaySubscriptionId);
            }

            if (null != replaySubscription)
            {
                replaySubscription.close();
            }
        }
    }

    Context context()
    {
        return ctx;
    }

    long joinDifference()
    {
        return joinDifference;
    }

    private void joinDifference(final long joinDifference)
    {
        this.joinDifference = joinDifference;

        if (!joinDifferenceCounter.isClosed())
        {
            joinDifferenceCounter.setRelease(joinDifference);
        }
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

        if (null != error)
        {
            state(State.FAILED);
            onTerminalError(error);
        }
        else
        {
            if (FROM_LIVE == position ||
                (NULL_POSITION != listRecordingRequest.stopPosition &&
                position == listRecordingRequest.stopPosition))
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
        if (0 == listRecordingRequest.remaining)
        {
            assert listRecordingRequest.recordingId == recordingId : listRecordingRequest.toString();

            if (liveStreamId != listRecordingRequest.streamId)
            {
                return new PersistentSubscriptionException(
                    PersistentSubscriptionException.Reason.STREAM_ID_MISMATCH,
                    "Requested live stream with ID: " + liveStreamId + " does not match stream ID: " +
                    listRecordingRequest.streamId + " for recording: " + recordingId);
            }

            if (NULL_POSITION != listRecordingRequest.stopPosition &&
                lastObservedLivePosition > listRecordingRequest.stopPosition)
            {
                return new PersistentSubscriptionException(
                    PersistentSubscriptionException.Reason.INVALID_START_POSITION,
                    "recording " + recordingId + " stopped at position=" +
                        listRecordingRequest.stopPosition +
                        " which is earlier than last observed live position=" +
                        lastObservedLivePosition);
            }

            if (position >= 0)
            {
                if (position < listRecordingRequest.startPosition)
                {
                    return new PersistentSubscriptionException(
                        PersistentSubscriptionException.Reason.INVALID_START_POSITION,
                        ArchiveException.buildReplayBeforeStartErrorMsg(
                            recordingId, position, listRecordingRequest.startPosition));
                }

                if (NULL_POSITION != listRecordingRequest.stopPosition && position > listRecordingRequest.stopPosition)
                {
                    return new PersistentSubscriptionException(
                        PersistentSubscriptionException.Reason.INVALID_START_POSITION,
                        ArchiveException.buildReplayExceedsLimitErrorMsg(
                            recordingId, position, listRecordingRequest.stopPosition));
                }
            }
            else if (FROM_START == position)
            {
                position = listRecordingRequest.startPosition;
            }
        }
        else
        {
            assert 1 == listRecordingRequest.remaining &&
                RECORDING_UNKNOWN == listRecordingRequest.code &&
                   listRecordingRequest.relevantId == recordingId : listRecordingRequest.toString();

            return new PersistentSubscriptionException(
                PersistentSubscriptionException.Reason.RECORDING_NOT_FOUND,
                ArchiveException.buildUnknownRecordingErrorMsg(recordingId));
        }

        return null;
    }

    private void setUpReplay()
    {
        resetReplayCatchupState();

        state(switch (replayChannelType)
        {
            case SESSION_SPECIFIC -> State.SEND_REPLAY_REQUEST;
            case DYNAMIC_PORT, RESPONSE_CHANNEL -> State.ADD_REPLAY_SUBSCRIPTION;
        });
    }

    private void refreshRecordingDescriptor()
    {
        resetReplayCatchupState();

        state(State.SEND_LIST_RECORDING_REQUEST);
    }

    private void resetReplayCatchupState()
    {
        joinDifference(Long.MIN_VALUE);
        maxRecordedPosition.reset(listRecordingRequest.termBufferLength >> 2);
        nextLivePosition = Aeron.NULL_VALUE;
    }

    private void cleanUpReplay()
    {
        if (Aeron.NULL_VALUE != replaySessionId)
        {
            asyncAeronArchive.trySendStopReplayRequest(aeron.nextCorrelationId(), replaySessionId);

            replaySessionId = Aeron.NULL_VALUE;
        }
    }

    private void cleanUpReplaySubscription()
    {
        if (Aeron.NULL_VALUE != replaySubscriptionId)
        {
            aeron.asyncRemoveSubscription(replaySubscriptionId);
        }

        if (null != replaySubscription)
        {
            aeron.asyncRemoveSubscription(replaySubscription.registrationId());
        }

        replaySubscriptionId = Aeron.NULL_VALUE;
        replaySubscription = null;
        replayImage = null;
    }

    private void cleanUpRequestPublication()
    {
        if (Aeron.NULL_VALUE != requestPublicationId)
        {
            aeron.asyncRemovePublication(requestPublicationId);
        }

        if (null != requestPublication)
        {
            aeron.asyncRemovePublication(requestPublication.registrationId());
        }

        requestPublicationId = Aeron.NULL_VALUE;
        requestPublication = null;
        responseChannelArchiveProxy = null;
    }

    private void cleanUpLiveSubscription()
    {
        if (Aeron.NULL_VALUE != liveSubscriptionId)
        {
            aeron.asyncRemoveSubscription(liveSubscriptionId);
        }

        if (null != liveSubscription)
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
            case DYNAMIC_PORT, RESPONSE_CHANNEL -> replayChannelUri.toString();
        };

        replayParams.reset();
        replayParams.position(position).length(REPLAY_ALL_AND_FOLLOW);
        final boolean result;
        if (ReplayChannelType.RESPONSE_CHANNEL == replayChannelType)
        {
            replayParams.replayToken(replayToken);

            result = asyncAeronArchive.trySendReplayRequest(
                responseChannelArchiveProxy,
                correlationId,
                recordingId,
                replayStreamId,
                channel,
                replayParams
            );
        }
        else
        {
            result = asyncAeronArchive.trySendReplayRequest(
                correlationId,
                recordingId,
                replayStreamId,
                channel,
                replayParams
            );
        }

        if (!result)
        {
            if (asyncAeronArchive.isConnected())
            {
                return 0;
            }
            else
            {
                cleanUpRequestPublication();
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
                cleanUpRequestPublication();
                cleanUpReplaySubscription();
                if (asyncAeronArchive.isConnected())
                {
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

        if (OK != replayRequest.code)
        {
            state(State.FAILED);

            cleanUpRequestPublication();
            cleanUpReplaySubscription();

            final int errorCode = (int)replayRequest.relevantId;

            final PersistentSubscriptionException.Reason reason = switch (errorCode)
            {
                case ArchiveException.INVALID_POSITION -> PersistentSubscriptionException.Reason.INVALID_START_POSITION;
                case ArchiveException.UNKNOWN_RECORDING -> PersistentSubscriptionException.Reason.RECORDING_NOT_FOUND;
                default -> PersistentSubscriptionException.Reason.GENERIC;
            };

            onTerminalError(new PersistentSubscriptionException(
                reason, "replay request failed: " + replayRequest.errorMessage));

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
            case RESPONSE_CHANNEL ->
            {
                cleanUpRequestPublication();

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
            case DYNAMIC_PORT, RESPONSE_CHANNEL -> replayChannel;
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

            if (ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE == e.errorCode())
            {
                setUpReplay();
                listener.onError(e);
            }
            else
            {
                state(State.FAILED);
                onTerminalError(e);
            }

            return 1;
        }

        if (null == subscription)
        {
            return 0;
        }

        replaySubscriptionId = Aeron.NULL_VALUE;
        replaySubscription = subscription;

        if (ReplayChannelType.SESSION_SPECIFIC == replayChannelType)
        {
            replayImageDeadline = nanoClock.nanoTime() + messageTimeoutNs;
        }

        state(switch (replayChannelType)
        {
            case SESSION_SPECIFIC -> State.REPLAY;
            case DYNAMIC_PORT -> State.AWAIT_REPLAY_CHANNEL_ENDPOINT;
            case RESPONSE_CHANNEL -> State.ADD_REQUEST_PUBLICATION;
        });

        return 1;
    }

    private int awaitReplayChannelEndpoint()
    {
        final String resolvedChannel = replaySubscription.tryResolveChannelEndpointPort();
        if (null == resolvedChannel)
        {
            return 0;
        }

        final String resolvedEndpoint = ChannelUri.parse(resolvedChannel).get(ENDPOINT_PARAM_NAME);
        if (null != resolvedEndpoint)
        {
            replayChannelUri.put(ENDPOINT_PARAM_NAME, resolvedEndpoint);
        }

        state(State.SEND_REPLAY_REQUEST);

        return 1;
    }

    private int addRequestPublication()
    {
        final String controlRequestChannel = ctx.aeronArchiveContext().controlRequestChannel();
        final int controlRequestStreamId = ctx.aeronArchiveContext().controlRequestStreamId();
        final int controlTermBufferLength = ctx.aeronArchiveContext().controlTermBufferLength();
        final ChannelUriStringBuilder uriBuilder = new ChannelUriStringBuilder(controlRequestChannel)
            .sessionId((Integer)null)
            .responseCorrelationId(replaySubscription.registrationId())
            .termId((Integer)null).initialTermId((Integer)null).termOffset((Integer)null)
            .termLength(controlTermBufferLength)
            .spiesSimulateConnection(false);
        final String requestPublicationChannel = uriBuilder.build();

        requestPublicationId = aeron.asyncAddExclusivePublication(requestPublicationChannel, controlRequestStreamId);
        state(State.AWAIT_REQUEST_PUBLICATION);
        return 1;
    }

    private int awaitRequestPublication()
    {
        final ExclusivePublication publication;
        try
        {
            publication = aeron.getExclusivePublication(requestPublicationId);
        }
        catch (final RegistrationException e)
        {
            cleanUpRequestPublication();
            cleanUpReplaySubscription();

            if (ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE == e.errorCode())
            {
                setUpReplay();
                listener.onError(e);
            }
            else
            {
                state(State.FAILED);
                onTerminalError(e);
            }

            return 1;
        }

        if (null == publication)
        {
            return 0;
        }

        requestPublicationId = Aeron.NULL_VALUE;
        requestPublication = publication;
        responseChannelArchiveProxy = new ArchiveProxy(publication);

        state(State.SEND_REPLAY_TOKEN_REQUEST);

        return 1;
    }

    private int sendReplayTokenRequest()
    {
        final long correlationId = aeron.nextCorrelationId();

        if (!asyncAeronArchive.trySendReplayTokenRequest(correlationId, recordingId))
        {
            if (asyncAeronArchive.isConnected())
            {
                return 0;
            }
            else
            {
                cleanUpRequestPublication();
                cleanUpReplaySubscription();

                state(State.AWAIT_ARCHIVE_CONNECTION);

                return 1;
            }
        }

        replayTokenRequest.init(correlationId, nanoClock.nanoTime() + messageTimeoutNs);

        state(State.AWAIT_REPLAY_TOKEN);

        return 1;
    }

    private int awaitReplayToken()
    {
        if (!replayTokenRequest.responseReceived)
        {
            if (nanoClock.nanoTime() - replayTokenRequest.deadlineNs >= 0)
            {
                cleanUpRequestPublication();
                cleanUpReplaySubscription();

                if (asyncAeronArchive.isConnected())
                {
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

        if (OK != replayTokenRequest.code)
        {
            state(State.FAILED);

            cleanUpRequestPublication();
            cleanUpReplaySubscription();

            onTerminalError(new ArchiveException(
                "replay token request failed: " + replayTokenRequest.errorMessage,
                (int)replayTokenRequest.relevantId,
                replayTokenRequest.correlationId));

            return 1;
        }

        replayToken = replayTokenRequest.relevantId;
        if (replayChannelUri.isIpc())
        {
            state(State.SEND_REPLAY_REQUEST);
        }
        else
        {
            state(State.AWAIT_REPLAY_CHANNEL_ENDPOINT);
        }
        return 1;
    }

    @SuppressWarnings("MethodLength")
    private int replay(final int fragmentLimit, final boolean isControlled)
    {
        Image replayImage = this.replayImage;

        if (null == replayImage)
        {
            replayImage = replaySubscription.imageBySessionId((int)replaySessionId);

            if (null == replayImage)
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
            position = replayImage.position();
            cleanUpLiveSubscription();
            cleanUpReplay();
            cleanUpReplaySubscription();
            refreshRecordingDescriptor();

            return 1;
        }

        if (null == liveSubscription && Aeron.NULL_VALUE != liveSubscriptionId)
        {
            try
            {
                liveSubscription = aeron.getSubscription(liveSubscriptionId);

                if (null != liveSubscription)
                {
                    liveSubscriptionId = Aeron.NULL_VALUE;
                    setLiveImageDeadline();
                }
            }
            catch (final RegistrationException e)
            {
                liveSubscriptionId = Aeron.NULL_VALUE;

                if (ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE != e.errorCode())
                {
                    cleanUpReplay();
                    cleanUpReplaySubscription();
                    state(State.FAILED);
                    onTerminalError(e);
                }
                else
                {
                    listener.onError(e);
                }
                return 1;
            }
        }

        if (null != liveSubscription)
        {
            if (liveSubscription.imageCount() > 0)
            {
                liveImage = liveSubscription.imageAtIndex(0);

                final long livePosition = liveImage.position();
                final long replayPosition = replayImage.position();
                joinDifference(livePosition - replayPosition);

                state(State.ATTEMPT_SWITCH);

                return 1;
            }
            else if (!liveImageDeadlineBreached && nanoClock.nanoTime() - liveImageDeadline >= 0)
            {
                onLiveImageDeadlineBreached();
            }
        }

        final int fragments = doPoll(replayImage, fragmentLimit, isControlled);

        position = replayImage.position();

        if (Aeron.NULL_VALUE == liveSubscriptionId &&
            null == liveSubscription &&
            maxRecordedPosition.isCaughtUp(position))
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

    private int attemptSwitch(final int fragmentLimit, final boolean isControlled)
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
                position = replayPosition;
                advanceLastObservedLivePosition(livePosition);
                cleanUpLiveSubscription();
                cleanUpReplay();
                cleanUpReplaySubscription();
                refreshRecordingDescriptor();

                return 1;
            }

            if (liveImage.isClosed())
            {
                cleanUpLiveSubscription();
                resetReplayCatchupState();
                state(State.REPLAY);

                return 1;
            }

            // Let the live channel catch up to the point we are at in the replay (but don't overtake it).
            fragments += liveImage.controlledPoll(liveCatchupFragmentHandler, fragmentLimit);

            // Carry on with the replay for now.
            if (isControlled)
            {
                fragments += replayImage.controlledPoll(replayCatchupControlledFragmentHandler, fragmentLimit);
            }
            else
            {
                fragments += replayImage.controlledPoll(replayCatchupUncontrolledFragmentHandler, fragmentLimit);
            }
        }

        if (isLive())
        {
            cleanUpReplay();
            cleanUpReplaySubscription();
            onLiveJoined();
        }

        return fragments;
    }

    private void onTerminalError(final Exception error)
    {
        failureReason = error;
        listener.onError(error);
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

    private ControlledFragmentHandler.Action onReplayCatchupFragmentControlled(
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
        return controlledFragmentAssembler.onFragment(buffer, offset, length, header);
    }

    private ControlledFragmentHandler.Action onReplayCatchupFragmentUncontrolled(
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
        uncontrolledFragmentAssembler.onFragment(buffer, offset, length, header);
        return ControlledFragmentHandler.Action.CONTINUE;
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

        if (null == liveSubscription)
        {
            try
            {
                liveSubscription = aeron.getSubscription(liveSubscriptionId);

                if (null != liveSubscription)
                {
                    liveSubscriptionId = Aeron.NULL_VALUE;
                    setLiveImageDeadline();
                }
            }
            catch (final RegistrationException e)
            {
                liveSubscriptionId = Aeron.NULL_VALUE;

                if (ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE == e.errorCode())
                {
                    state(State.ADD_LIVE_SUBSCRIPTION);
                    listener.onError(e);
                }
                else
                {
                    state(State.FAILED);
                    onTerminalError(e);
                }

                return 1;
            }
        }

        if (null != liveSubscription)
        {
            if (0 < liveSubscription.imageCount())
            {
                final Image image = liveSubscription.imageAtIndex(0);
                final long livePosition = image.position();
                advanceLastObservedLivePosition(livePosition);

                if (position >= 0)
                {
                    if (livePosition < position)
                    {
                        cleanUpLiveSubscription();
                        state(State.FAILED);
                        onTerminalError(new PersistentSubscriptionException(
                            PersistentSubscriptionException.Reason.GENERIC,
                            "live stream joined at position " + livePosition +
                            " which is earlier than last seen position " + position));

                        return 1;
                    }
                    else if (livePosition > position)
                    {
                        cleanUpLiveSubscription();
                        refreshRecordingDescriptor();

                        return 1;
                    }
                }

                liveImage = image;
                position = livePosition;
                joinDifference(0);
                state(State.LIVE);
                onLiveJoined();

                return 1;
            }
            else if (!liveImageDeadlineBreached && nanoClock.nanoTime() - liveImageDeadline >= 0)
            {
                onLiveImageDeadlineBreached();
            }
        }

        return 0;
    }

    private int live(final int fragmentLimit, final boolean isControlled)
    {
        final Image image = liveImage;
        final int fragments = doPoll(image, fragmentLimit, isControlled);
        if (0 == fragments && image.isClosed())
        {
            final long finalPosition = image.position();
            advanceLastObservedLivePosition(finalPosition);
            position = finalPosition;
            cleanUpLiveSubscription();
            refreshRecordingDescriptor();
            onLiveLeft();

            return 1;
        }
        return fragments;
    }

    private void advanceLastObservedLivePosition(final long livePosition)
    {
        if (livePosition > lastObservedLivePosition)
        {
            lastObservedLivePosition = livePosition;
        }
    }

    private void state(final State newState)
    {
        logStateChange(state, newState, recordingId, replayChannel, replayStreamId, liveChannel, liveStreamId);
        if (newState != this.state)
        {
            this.state = newState;
            if (!stateCounter.isClosed())
            {
                stateCounter.setRelease(state.code);
            }
            if (State.FAILED == newState)
            {
                asyncAeronArchive.close();
            }
        }
    }

    private void logStateChange(
        final State oldState,
        final State newState,
        final long recordingId,
        final String replayChannel,
        final int replayStreamId,
        final String liveChannel,
        final int liveStreamId)
    {
//        System.out.println("State: " + oldState + " -> " + newState);
    }

    private void logJoinedLive(
        final long recordingId,
        final String replayChannel,
        final int replayStreamId,
        final String liveChannel,
        final int liveStreamId,
        final int liveSessionId,
        final long joinPosition)
    {
    }

    private void logLeftLive(
        final long recordingId,
        final String replayChannel,
        final int replayStreamId,
        final String liveChannel,
        final int liveStreamId,
        final long livePosition)
    {
    }

    private void onLiveJoined()
    {
        logJoinedLive(
            recordingId,
            replayChannel,
            replayStreamId,
            liveChannel,
            liveStreamId,
            liveImage.sessionId(),
            liveImage.position()
        );

        if (!liveJoinedCounter.isClosed())
        {
            liveJoinedCounter.incrementRelease();
        }

        listener.onLiveJoined();
    }

    private void onLiveLeft()
    {
        logLeftLive(recordingId, replayChannel, replayStreamId, liveChannel, liveStreamId, position);

        if (!liveLeftCounter.isClosed())
        {
            liveLeftCounter.incrementRelease();
        }

        listener.onLiveLeft();
    }

    private int doPoll(final Image image, final int fragmentLimit, final boolean isControlled)
    {
        if (isControlled)
        {
            return image.controlledPoll(controlledFragmentAssembler, fragmentLimit);
        }
        return image.poll(uncontrolledFragmentAssembler, fragmentLimit);
    }

    private ControlledFragmentHandler.Action onFragmentControlled(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        return controlledFragmentHandler.onFragment(buffer, offset, length, header);
    }

    private void onFragmentUncontrolled(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        uncontrolledFragmentHandler.onFragment(buffer, offset, length, header);
    }

    private enum ReplayChannelType
    {
        SESSION_SPECIFIC,
        DYNAMIC_PORT,
        RESPONSE_CHANNEL;

        static ReplayChannelType of(final ChannelUri channelUri)
        {
            if (channelUri.hasControlModeResponse())
            {
                return RESPONSE_CHANNEL;
            }
            if (channelUri.isUdp())
            {
                final String endpoint = channelUri.get(ENDPOINT_PARAM_NAME);
                if (null != endpoint && endpoint.endsWith(":0"))
                {
                    return DYNAMIC_PORT;
                }
            }
            return SESSION_SPECIFIC;
        }
    }

    private enum State
    {
        AWAIT_ARCHIVE_CONNECTION(0),
        SEND_LIST_RECORDING_REQUEST(1),
        AWAIT_LIST_RECORDING_RESPONSE(2),
        SEND_REPLAY_REQUEST(3),
        AWAIT_REPLAY_RESPONSE(4),
        ADD_REPLAY_SUBSCRIPTION(5),
        AWAIT_REPLAY_SUBSCRIPTION(6),
        AWAIT_REPLAY_CHANNEL_ENDPOINT(7),
        ADD_REQUEST_PUBLICATION(8),
        AWAIT_REQUEST_PUBLICATION(9),
        SEND_REPLAY_TOKEN_REQUEST(10),
        AWAIT_REPLAY_TOKEN(11),
        REPLAY(12),
        ATTEMPT_SWITCH(13),
        ADD_LIVE_SUBSCRIPTION(14),
        AWAIT_LIVE(15),
        LIVE(16),
        FAILED(17);

        private final int code;

        State(final int code)
        {
            if (code != ordinal())
            {
                throw new IllegalArgumentException(name() + " - code must equal ordinal value: code=" + code);
            }

            this.code = code;
        }
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

            if (0 == --remaining)
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

    /**
     * Configuration of a {@code PersistentSubscription} to be created.
     */
    public static final class Context implements Cloneable
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
        private Counter stateCounter = null;
        private Counter joinDifferenceCounter = null;
        private Counter liveLeftCounter = null;
        private Counter liveJoinedCounter = null;

        /**
         * Construct a Context using default values.
         */
        public Context()
        {
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

        /**
         * Conclude configuration by setting up defaults when specifics are not provided.
         */
        @SuppressWarnings("MethodLength")
        public void conclude()
        {
            if ((boolean)IS_CONCLUDED_VH.getAndSet(this, true))
            {
                throw new ConcurrentConcludeException();
            }

            if (Aeron.NULL_VALUE == recordingId)
            {
                throw new ConfigurationException("recordingId must be set");
            }

            if (Aeron.NULL_VALUE == liveStreamId)
            {
                throw new ConfigurationException("liveStreamId must be set");
            }

            if (Strings.isEmpty(liveChannel))
            {
                throw new ConfigurationException("liveChannel must be set");
            }

            if (Strings.isEmpty(replayChannel))
            {
                throw new ConfigurationException("replayChannel must be set");
            }

            if (Aeron.NULL_VALUE == replayStreamId)
            {
                throw new ConfigurationException("replayStreamId must be set");
            }

            if (null == aeronArchiveContext)
            {
                throw new ConfigurationException("aeronArchiveContext must be set");
            }

            if (null == listener)
            {
                listener = new NoOpPersistentSubscriptionListener();
            }

            if (0 > recordingId)
            {
                throw new ConfigurationException("invalid recordingId " + recordingId);
            }

            if (FROM_LIVE > startPosition)
            {
                throw new ConfigurationException("invalid startPosition " + startPosition);
            }

            final ChannelUri replayChannelUri = ChannelUri.parse(replayChannel);

            if (replayChannelUri.hasControlModeResponse())
            {
                final String controlRequestChannel = aeronArchiveContext.controlRequestChannel();
                if (null != controlRequestChannel &&
                    !replayChannelUri.isIpc() == ChannelUri.parse(controlRequestChannel).isIpc()
                )
                {
                    throw new ConfigurationException(
                        "Channel media type mismatch. " +
                            "When using `control-mode=response`, the `replayChannel` media type must match the media" +
                            " type for the archive control channel."
                    );
                }
            }

            replayChannelUri.put(CommonContext.REJOIN_PARAM_NAME, "false");

            replayChannel = replayChannelUri.toString();

            if (null == aeron)
            {
                final Aeron.Context aeronCtx = new Aeron.Context()
                    .clientName("PersistentSubscription")
                    .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE)
                    .useConductorAgentInvoker(true);
                if (null != aeronDirectoryName)
                {
                    aeronCtx.aeronDirectoryName(aeronDirectoryName);
                }
                aeron = Aeron.connect(aeronCtx);
                ownsAeronClient = true;
            }

            if (null == aeronArchiveContext.aeron())
            {
                aeronArchiveContext.aeron(aeron);
            }

            if (null == stateCounter)
            {
                stateCounter = allocatePersistentSubscriptionCounter(
                  aeron,
                  "Persistent Subscription State",
                  PERSISTENT_SUBSCRIPTION_STATE_TYPE_ID,
                  replayStreamId,
                  liveStreamId,
                  replayChannel,
                  liveChannel
                );
            }

            if (null == joinDifferenceCounter)
            {
                joinDifferenceCounter = allocatePersistentSubscriptionCounter(
                  aeron,
                  "Persistent Subscription Join Difference",
                  PERSISTENT_SUBSCRIPTION_JOIN_DIFFERENCE_TYPE_ID,
                  replayStreamId,
                  liveStreamId,
                  replayChannel,
                  liveChannel
                );
            }

            if (null == liveLeftCounter)
            {
                liveLeftCounter = allocatePersistentSubscriptionCounter(
                  aeron,
                  "Persistent Subscription Live Left Count",
                  PERSISTENT_SUBSCRIPTION_LIVE_LEFT_COUNT_TYPE_ID,
                  replayStreamId,
                  liveStreamId,
                  replayChannel,
                  liveChannel
                );
            }

            if (null == liveJoinedCounter)
            {
                liveJoinedCounter = allocatePersistentSubscriptionCounter(
                  aeron,
                  "Persistent Subscription Live Joined Count",
                  PERSISTENT_SUBSCRIPTION_LIVE_JOINED_COUNT_TYPE_ID,
                  replayStreamId,
                  liveStreamId,
                  replayChannel,
                  liveChannel
                );
            }
        }

        /**
         * Has the context had the {@link #conclude()} method called.
         *
         * @return true if the {@link #conclude()} method has been called.
         */
        public boolean isConcluded()
        {
            return isConcluded;
        }

        /**
         * {@link Aeron} client for communicating with the local Media Driver.
         * <p>
         * This client will be closed when the {@link PersistentSubscription#close()} or {@link #close()} methods are
         * called if {@link #ownsAeronClient()} is true.
         *
         * @param aeron client for communicating with the local Media Driver.
         * @return this for a fluent API.
         * @see Aeron#connect()
         */
        public Context aeron(final Aeron aeron)
        {
            this.aeron = aeron;
            return this;
        }

        /**
         * {@link Aeron} client for communicating with the local Media Driver.
         * <p>
         * If not provided then a default will be established during {@link #conclude()} by calling
         * {@link Aeron#connect()}.
         *
         * @return client for communicating with the local Media Driver.
         */
        public Aeron aeron()
        {
            return aeron;
        }

        /**
         * Does this context own the {@link #aeron()} client and thus take responsibility for closing it?
         *
         * @param ownsAeronClient does this context own the {@link #aeron()} client?
         * @return this for a fluent API.
         */
        public Context ownsAeronClient(final boolean ownsAeronClient)
        {
            this.ownsAeronClient = ownsAeronClient;
            return this;
        }

        /**
         * Does this context own the {@link #aeron()} client and thus take responsibility for closing it?
         *
         * @return does this context own the {@link #aeron()} client and thus take responsibility for closing it?
         */
        public boolean ownsAeronClient()
        {
            return ownsAeronClient;
        }

        /**
         * Set the top level Aeron directory used for communication between the Aeron client and Media Driver.
         *
         * @param aeronDirectoryName the top level Aeron directory.
         * @return this for a fluent API.
         */
        public Context aeronDirectoryName(final String aeronDirectoryName)
        {
            this.aeronDirectoryName = aeronDirectoryName;
            return this;
        }

        /**
         * Get the top level Aeron directory used for communication between the Aeron client and Media Driver.
         *
         * @return the top level Aeron directory.
         */
        public String aeronDirectoryName()
        {
            return aeronDirectoryName;
        }

        /**
         * Set the id of the recording to replay from.
         *
         * @param recordingId the recording's id.
         * @return this for a fluent API.
         */
        public Context recordingId(final long recordingId)
        {
            this.recordingId = recordingId;
            return this;
        }

        /**
         * Get the id of the recording to replay from.
         *
         * @return the recordingId.
         */
        public long recordingId()
        {
            return recordingId;
        }

        /**
         * Set the position to start consuming from.
         * This can be a point in the recording, the start of the recording ({@link #FROM_START}) or directly
         * from live ({@link #FROM_LIVE}).
         *
         * @param startPosition the position to start consuming from.
         * @return this for a fluent API.
         */
        public Context startPosition(final long startPosition)
        {
            this.startPosition = startPosition;
            return this;
        }

        /**
         * Get the position to start consuming from.
         *
         * @return the start position.
         */
        public long startPosition()
        {
            return startPosition;
        }

        /**
         * Set the channel which will be used for subscribing to live data.
         *
         * @param liveChannel the channel which will be used for subscribing to live data.
         * @return this for a fluent API.
         */
        public Context liveChannel(final String liveChannel)
        {
            this.liveChannel = liveChannel;
            return this;
        }

        /**
         * Returns the channel which will be used for subscribing to live data.
         *
         * @return the channel which will be used for subscribing to live data.
         */
        public String liveChannel()
        {
            return liveChannel;
        }

        /**
         * Set the stream id of the live data.
         *
         * @param liveStreamId the stream id of the live data.
         * @return this for a fluent API.
         */
        public Context liveStreamId(final int liveStreamId)
        {
            this.liveStreamId = liveStreamId;
            return this;
        }

        /**
         * Returns the stream id of the live data.
         *
         * @return the stream id of the live data.
         */
        public int liveStreamId()
        {
            return liveStreamId;
        }

        /**
         * Set the channel which will be used for replays.
         *
         * @param replayChannel the channel which will be used for replays.
         * @return this for a fluent API.
         */
        public Context replayChannel(final String replayChannel)
        {
            this.replayChannel = replayChannel;
            return this;
        }

        /**
         * Returns the channel which will be used for replays.
         *
         * @return the channel which will be used for replays.
         */
        public String replayChannel()
        {
            return replayChannel;
        }

        /**
         * Set the stream id which will be used for replays.
         *
         * @param replayStreamId the stream id which will be used for replays.
         * @return this for a fluent API.
         */
        public Context replayStreamId(final int replayStreamId)
        {
            this.replayStreamId = replayStreamId;
            return this;
        }

        /**
         * Returns the stream id which will be used for replays.
         *
         * @return the stream id which will be used for replays.
         */
        public int replayStreamId()
        {
            return replayStreamId;
        }

        /**
         * Set the {@link PersistentSubscriptionListener} to use for the {@code PersistentSubscription}.
         *
         * @param listener to use for the {@code PersistentSubscription}.
         * @return this for a fluent API.
         * @see PersistentSubscriptionListener
         */
        public Context listener(final PersistentSubscriptionListener listener)
        {
            this.listener = listener;
            return this;
        }

        /**
         * Get the {@link PersistentSubscriptionListener} in use for the {@code PersistentSubscription}.
         *
         * @return the {@code PersistentSubscriptionListener} in use.
         */
        public PersistentSubscriptionListener listener()
        {
            return listener;
        }

        /**
         * Set the {@link AeronArchive.Context} that should be used for communicating with an
         * Archive.
         *
         * @param aeronArchiveContext that should be used for communicating with an Archive.
         * @return this for a fluent API.
         */
        public Context aeronArchiveContext(final AeronArchive.Context aeronArchiveContext)
        {
            this.aeronArchiveContext = aeronArchiveContext;
            return this;
        }

        /**
         * Get the {@link AeronArchive.Context} that should be used for communicating with an
         * Archive.
         *
         * @return the {@link AeronArchive.Context} that should be used for communicating
         * with an Archive.
         */
        public AeronArchive.Context aeronArchiveContext()
        {
            return aeronArchiveContext;
        }

        /**
         * Set the counter for the current state of the {@code PersistentSubscription}.
         *
         * @param stateCounter the counter for the current state of the {@code PersistentSubscription}.
         * @return this for a fluent API.
         */
        public Context stateCounter(final Counter stateCounter)
        {
            this.stateCounter = stateCounter;
            return this;
        }

        /**
         * Get the counter for the current state of the {@code PersistentSubscription}.
         *
         * @return the counter for the current state of the {@code PersistentSubscription}.
         * @see State
         */
        public Counter stateCounter()
        {
            return stateCounter;
        }

        /**
         * Set the counter for the {@code PersistentSubscription}'s join difference.
         * This represents the difference between the subscription's position in the replay and the position it joined
         * live.
         * When the subscription is not consuming from live, the value of this counter will be {@code Long.MIN_VALUE}.
         *
         * @param joinDifferenceCounter the counter for the {@code PersistentSubscription}'s join difference.
         * @return this for a fluent API.
         */
        public Context joinDifferenceCounter(final Counter joinDifferenceCounter)
        {
            this.joinDifferenceCounter = joinDifferenceCounter;
            return this;
        }

        /**
         * Get the counter for the {@code PersistentSubscription}'s join difference.
         * This is the difference between the subscription's position in the replay and the position it joined
         * live.
         * When the subscription is not consuming from live, the value of this counter will be {@code Long.MIN_VALUE}.
         *
         * @return the counter for the {@code PersistentSubscription}'s join difference.
         */
        public Counter joinDifferenceCounter()
        {
            return joinDifferenceCounter;
        }

        /**
         * Set the counter for the number of times a {@code PersistentSubscription} has left the live stream.
         *
         * @param liveLeftCounter the counter for the number of times the live stream has been left.
         * @return this for a fluent API.
         */
        public Context liveLeftCounter(final Counter liveLeftCounter)
        {
            this.liveLeftCounter = liveLeftCounter;
            return this;
        }

        /**
         * Get the counter for the number of times a {@code PersistentSubscription} has left the live stream.
         *
         * @return the counter for the number of times the live stream has been left.
         */
        public Counter liveLeftCounter()
        {
            return liveLeftCounter;
        }

        /**
         * Set the counter for the number of times a {@code PersistentSubscription} has joined the live stream.
         *
         * @param liveJoinedCounter the counter for the number of times live has been joined.
         * @return this for a fluent API.
         */
        public Context liveJoinedCounter(final Counter liveJoinedCounter)
        {
            this.liveJoinedCounter = liveJoinedCounter;
            return this;
        }

        /**
         * Get the counter for the number of times a {@code PersistentSubscription} has joined the live stream.
         *
         * @return the counter for the number of times live has been joined.
         */
        public Counter liveJoinedCounter()
        {
            return liveJoinedCounter;
        }

        /**
         * Close the context and free applicable resources.
         * <p>
         * If {@link #ownsAeronClient()} is true then the {@link #aeron()} client will be closed.
         */
        public void close()
        {
            if (ownsAeronClient)
            {
                CloseHelper.close(aeron);
            }
            else if (!aeron.isClosed())
            {
                CloseHelper.closeAll(
                    stateCounter,
                    joinDifferenceCounter,
                    liveLeftCounter,
                    liveJoinedCounter
                );
            }
        }
    }

    private static final class NoOpPersistentSubscriptionListener implements PersistentSubscriptionListener
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

    private final class MaxRecordedPosition extends AsyncArchiveOp
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

        public boolean isCaughtUp(final long replayedPosition)
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
                if (OK == code)
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
                    final ArchiveException archiveException = new ArchiveException(
                        "get max position request failed code=" + code + " relevantId=" + relevantId +
                        " errorMessage='" + errorMessage + "'");
                    state(State.FAILED);
                    onTerminalError(archiveException);
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

    private final class ArchiveListener implements AsyncAeronArchiveListener
    {
        public void onConnected()
        {
        }

        public void onDisconnected()
        {
            if (State.AWAIT_ARCHIVE_CONNECTION == state ||
                State.ATTEMPT_SWITCH == state ||
                State.LIVE == state ||
                State.FAILED == state)
            {
                return;
            }

            final Image replayImage = PersistentSubscription.this.replayImage;
            if (null != replayImage)
            {
                position = replayImage.position();
            }

            cleanUpRequestPublication();
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
            else if (correlationId == replayTokenRequest.correlationId)
            {
                replayTokenRequest.onControlResponse(relevantId, code, errorMessage);
            }
        }

        public void onError(final Exception error)
        {
            if (asyncAeronArchive.isClosed())
            {
                state(State.FAILED);
                onTerminalError(error);
            }
            else
            {
                listener.onError(error);
            }
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

    private static Counter allocatePersistentSubscriptionCounter(
        final Aeron aeron,
        final String name,
        final int typeId,
        final int replayStreamId,
        final int liveStreamId,
        final String replayChannel,
        final String liveChannel)
    {
        final String label =
            name + ": " + replayStreamId + " " + replayChannel + " " + liveStreamId + " " + liveChannel;

        return aeron.addCounter(typeId, label);
    }
}
