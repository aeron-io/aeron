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
import io.aeron.Image;
import io.aeron.ImageControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.exceptions.TimeoutException;
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
import static io.aeron.archive.codecs.ControlResponseCode.OK;
import static io.aeron.archive.codecs.ControlResponseCode.RECORDING_UNKNOWN;

/**
 *
 */
public final class PersistentSubscription implements AutoCloseable
{
    private final ImageControlledFragmentAssembler assembler = new ImageControlledFragmentAssembler(this::onFragment);
    private final ListRecordingRequest listRecordingRequest = new ListRecordingRequest();
    private final SwitchDecisionThing switchDecisionThing = new SwitchDecisionThing();
    private final AsyncArchiveOp replayRequest = new AsyncArchiveOp();
    private final Context ctx;
    private final long recordingId;
    private final PersistentSubscriptionListener listener;
    private final String liveChannel;
    private final int liveStreamId;
    private final String replayChannel;
    private final int replayStreamId;
    private final long startPosition;
    private final Aeron aeron;
    private final NanoClock nanoClock;
    private final AsyncAeronArchive asyncAeronArchive;
    private final long messageTimeoutNs;
    private boolean awaitingReplayResponse;

    private State state;
    private Subscription replaySubscription;
    private Subscription liveSubscription;
    private Image replayImage;
    private Image liveImage;
    private ControlledFragmentHandler controlledFragmentHandler;
    private long joinError;

    private long nextLivePosition = Aeron.NULL_VALUE;
    private long lastConsumedLivePosition = Aeron.NULL_VALUE;

    private PersistentSubscription(final Context ctx)
    {
        ctx.conclude();

        this.ctx = ctx;
        recordingId = ctx.recordingId;
        startPosition = ctx.startPosition;
        liveChannel = ctx.liveChannel;
        liveStreamId = ctx.liveStreamId;
        replayChannel = ctx.replayChannel;
        replayStreamId = ctx.replayStreamId;
        listener = ctx.listener;
        aeron = ctx.aeron;
        nanoClock = aeron.context().nanoClock();
        asyncAeronArchive = new AsyncAeronArchive(ctx.aeronArchiveContext().aeron(aeron), new ArchiveListener());
        messageTimeoutNs = ctx.aeronArchiveContext().messageTimeoutNs();

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
            case REPLAY -> replay(fragmentHandler, fragmentLimit);
            case ATTEMPT_SWITCH -> attemptSwitch(fragmentHandler, fragmentLimit);
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
        // TODO do we need to explicitly stop replay if there is one?
        CloseHelper.closeAll(asyncAeronArchive, ctx::close);
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

            if (listener != null)
            {
                listener.onError(error);
            }
        }
        else
        {
            subscribeToReplay(startPosition);

            state(State.REPLAY);
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

            if (startPosition < listRecordingRequest.startPosition)
            {
                return new PersistentSubscriptionException(
                    PersistentSubscriptionException.Reason.INVALID_START_POSITION,
                    "Requested position: " + startPosition + " is lower than start position: " +
                    listRecordingRequest.startPosition + " for recording: " + recordingId);
            }

            if (listRecordingRequest.stopPosition != NULL_POSITION && startPosition > listRecordingRequest.stopPosition)
            {
                return new PersistentSubscriptionException(
                    PersistentSubscriptionException.Reason.INVALID_START_POSITION,
                    "Requested position: " + startPosition + " is greater than stop position: " +
                    listRecordingRequest.stopPosition + " for recording: " + recordingId);
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

    private int replay(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        if (awaitingReplayResponse)
        {
            if (replayRequest.responseReceived)
            {
                awaitingReplayResponse = false;
            }
            else
            {
                checkDeadline(replayRequest.deadlineNs, "awaiting response", replayRequest.correlationId);
            }

            if (awaitingReplayResponse)
            {
                return 0;
            }
        }

        Image replayImage = this.replayImage;

        if (replayImage == null)
        {
            replayImage = replaySubscription.imageBySessionId((int)replayRequest.relevantId);

            if (replayImage == null)
            {
                return 0;
            }

            this.replayImage = replayImage;
        }

        final int fragments = controlledPoll(replayImage, fragmentHandler, fragmentLimit);

        final long replayedPosition = replayImage.position();
        if (switchDecisionThing.shouldSwitch(replayedPosition))
        {
            liveImage = null;
            liveSubscription = aeron.addSubscription(liveChannel, liveStreamId);
            state(State.ATTEMPT_SWITCH);
        }

        return fragments;
    }

    private int attemptSwitch(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        Image liveImage = this.liveImage;

        if (liveImage == null)
        {
            if (liveSubscription.hasNoImages())
            {
                // hacky way of waiting for the subscription to connect.
                return 0;
            }

            this.liveImage = liveImage = liveSubscription.imageAtIndex(0);
        }

        int fragments = 0;

        final long livePosition = liveImage.position();
        final long replayPosition = replayImage.position();

        if (joinError == Long.MIN_VALUE)
        {
            joinError = livePosition - replayPosition;
        }

        if (replayPosition == livePosition)
        {
            state(State.LIVE);
        }
        else
        {
            // Let the live channel catch up to the point we are at in the replay (but don't overtake it).
            fragments += liveImage.controlledPoll((buffer, offset, length, header) -> {
                final long currentLivePosition = header.position();
                final long lastReplayPosition = replayImage.position();
                if (currentLivePosition <= lastReplayPosition)
                {
                    return ControlledFragmentHandler.Action.CONTINUE;
                }
                nextLivePosition = currentLivePosition;
                return ControlledFragmentHandler.Action.ABORT;
            }, fragmentLimit);

            // Carry on with the replay for now.
            controlledFragmentHandler = fragmentHandler;
            try
            {
                fragments += replayImage.controlledPoll((buffer, offset, length, header) -> {
                        final long currentReplayPosition = header.position();
                        if (currentReplayPosition == nextLivePosition)
                        {
                            state(State.LIVE);
                            return ControlledFragmentHandler.Action.ABORT;
                        }
                        return assembler.onFragment(buffer, offset, length, header);
                    },
                    1
                );
            }
            finally
            {
                controlledFragmentHandler = null;
            }
        }

        if (isLive() && replaySubscription.isConnected())
        {
            replayImage = null;
            CloseHelper.close(replaySubscription);
        }

        return fragments;
    }

    private int live(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        int workCount = 0;

        final Image image = liveImage;
        if (!image.isClosed())
        {
            workCount += controlledPoll(image, fragmentHandler, fragmentLimit);
            lastConsumedLivePosition = image.position();
        }
        else
        {
            state(State.REPLAY);

            liveImage = null;
            CloseHelper.close(liveSubscription);

            subscribeToReplay(lastConsumedLivePosition);

            workCount++;
        }

        return workCount;
    }

    private void subscribeToReplay(final long startPosition)
    {
        replayImage = null;
        joinError = Long.MIN_VALUE;

        // TODO async
        replaySubscription = aeron.addSubscription(replayChannel, replayStreamId);

        final ChannelUri replayChannelUri = ChannelUri.parse(replayChannel);
        if (replayChannelUri.isUdp())
        {
            replayChannelUri.put(ENDPOINT_PARAM_NAME, replaySubscription.resolvedEndpoint());
        }

        final long correlationId = aeron.nextCorrelationId();
        if (!asyncAeronArchive.trySendReplayRequest(
            correlationId,
            recordingId,
            startPosition,
            AeronArchive.REPLAY_ALL_AND_FOLLOW,
            replaySubscription.streamId(),
            replayChannelUri.toString()))
        {
            throw new ArchiveException("failed to send replay request"); // TODO
        }
        replayRequest.init(correlationId, nanoClock.nanoTime() + messageTimeoutNs);
        awaitingReplayResponse = true;

        switchDecisionThing.reset(recordingId, listRecordingRequest.termBufferLength >> 2);
    }

    private void state(final State newState)
    {
        System.out.println("State: " + state + " -> " + newState);
        if (newState != this.state)
        {
            this.state = newState;
        }
    }

    private void checkDeadline(final long deadlineNs, final String errorMessage, final long correlationId)
    {
        if (deadlineNs - nanoClock.nanoTime() < 0)
        {
            throw new TimeoutException(
                errorMessage + " - correlationId=" + correlationId + " messageTimeout=" +
                    SystemUtil.formatDuration(messageTimeoutNs));
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

    private enum State
    {
        AWAIT_ARCHIVE_CONNECTION,
        SEND_LIST_RECORDING_REQUEST,
        AWAIT_LIST_RECORDING_RESPONSE,
        REPLAY,
        ATTEMPT_SWITCH,
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
        private long startPosition = 0; // TODO default to FROM_LIVE
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

            if (startPosition < 0)
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
        public void onLive()
        {

        }

        public void onError(final Exception e)
        {

        }
    }

    private class SwitchDecisionThing extends AsyncArchiveOp
    {
        private long maxRecordedPosition = Long.MAX_VALUE;
        private boolean recheckRequired = true;
        private boolean waitingForMaxPosition = false;
        private boolean sentRequestForMaxPosition = false;
        private int closeEnoughThreshold;
        private long recordingId;

        public void reset(final long recordingId, final int closeEnoughThreshold)
        {
            this.recordingId = recordingId;
            this.closeEnoughThreshold = closeEnoughThreshold;
            this.waitingForMaxPosition = true;
        }

        public boolean shouldSwitch(final long replayedPosition)
        {
            if (waitingForMaxPosition)
            {
                pollForMaxPosition();
                return false;
            }

            if (recheckRequired)
            {
                if (replayedPosition >= maxRecordedPosition)
                {
                    startMaxPositionReload();
                    recheckRequired = false;
                }
                return false;
            }

            if (closeEnough(replayedPosition, maxRecordedPosition))
            {
                return true;
            }
            else
            {
                recheckRequired = true;
                return false;
            }
        }

        private void startMaxPositionReload()
        {
            final long correlationId = aeron.nextCorrelationId();
            if (!asyncAeronArchive.trySendMaxRecordedPositionRequest(correlationId, recordingId))
            {
                throw new ArchiveException("failed to send get max recorded position request");
            }
            init(correlationId, nanoClock.nanoTime() + messageTimeoutNs);
            sentRequestForMaxPosition = true;
            waitingForMaxPosition = true;
        }

        private void pollForMaxPosition()
        {
            if (!sentRequestForMaxPosition)
            {
                startMaxPositionReload();
            }

            if (responseReceived)
            {
                waitingForMaxPosition = false;
                sentRequestForMaxPosition = false;
                if (code == OK)
                {
                    maxRecordedPosition = relevantId;
                }
                else
                {
                    // TODO
                    throw new ArchiveException("get max position request failed code=" + code +
                                               " relevantId=" + relevantId +
                                               " errorMessage='" + errorMessage + "'");
                }
            }
            else
            {
                checkDeadline(deadlineNs, "awaiting response", correlationId);
            }
        }

        private boolean closeEnough(final long replayedPosition, final long maxRecordedPosition)
        {
            return replayedPosition >= maxRecordedPosition - closeEnoughThreshold;
        }
    }

    private class ArchiveListener implements AsyncAeronArchiveListener
    {
        public void onConnected()
        {
        }

        public void onDisconnected()
        {
            state(State.FAILED); // TODO recover
        }

        public void onControlResponse(
            final long correlationId,
            final long relevantId,
            final ControlResponseCode code,
            final String errorMessage)
        {
            if (correlationId == switchDecisionThing.correlationId)
            {
                switchDecisionThing.onControlResponse(relevantId, code, errorMessage);
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
            error.printStackTrace(); // TODO
            if (asyncAeronArchive.isClosed())
            {
                state(State.FAILED);
                if (listener != null)
                {
                    listener.onError(error);
                }
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
}
