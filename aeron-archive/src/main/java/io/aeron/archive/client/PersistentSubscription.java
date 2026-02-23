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
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.CloseHelper;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;

/**
 *
 */
public final class PersistentSubscription implements AutoCloseable
{
    private final RecordingDescriptorConsumerImpl descriptor = new RecordingDescriptorConsumerImpl();
    private final AeronArchive aeronArchive;
    private final long recordingId;
    private final PersistentSubscriptionListener listener;
    private final String liveChannel;
    private final int liveStreamId;
    private final long startPosition;

    private State state;
    private long joinError;
    private Subscription replaySubscription;
    private long candidateSwitchPosition;
    private int replaySessionId;
    private Subscription liveSubscription;

    private long nextLivePosition = Aeron.NULL_VALUE;

    private PersistentSubscription(final Context ctx)
    {
        ctx.conclude();
        this.recordingId = ctx.recordingId;
        this.startPosition = ctx.startPosition;
        this.liveChannel = ctx.liveChannel;
        this.liveStreamId = ctx.liveStreamId;
        this.listener = ctx.listener;
        this.aeronArchive = AeronArchive.connect(ctx.aeronArchiveContext.clone());

        state(State.INIT);
    }

    public static PersistentSubscription create(final Context ctx)
    {
        return new PersistentSubscription(ctx);
    }

    public int controlledPoll(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        // TODO handle ArchiveException if we use blocking API

        return switch (state)
        {
            case INIT -> init();
            case REPLAY -> replay(fragmentHandler, fragmentLimit);
            case ATTEMPT_SWITCH -> attemptSwitch(fragmentHandler, fragmentLimit);
            case LIVE -> live(fragmentHandler, fragmentLimit);
            case FAILED -> 0;
        };
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
        CloseHelper.close(aeronArchive);
    }

    long joinError()
    {
        return joinError;
    }

    private int init()
    {
        if (aeronArchive.listRecording(recordingId, descriptor) == 0) // TODO make async
        {
            state(State.FAILED);
            if (listener != null)
            {
                listener.onError(new PersistentSubscriptionException(
                    PersistentSubscriptionException.Reason.RECORDING_NOT_FOUND,
                    "No recording found with ID: " + recordingId)
                );
            }
            return 1;
        }

        assert descriptor.recordingId == recordingId;

        if (liveStreamId != descriptor.streamId)
        {
            state(State.FAILED);
            if (listener != null)
            {
                listener.onError(new PersistentSubscriptionException(
                    PersistentSubscriptionException.Reason.STREAM_ID_MISMATCH,
                    "Requested live stream with ID: " + liveStreamId + " does not match stream ID: " + descriptor.streamId + " for recording: " + recordingId)
                );
            }
            return 1;
        }

        if (startPosition < descriptor.startPosition)
        {
            state(State.FAILED);
            if (listener != null)
            {
                listener.onError(new PersistentSubscriptionException(
                    PersistentSubscriptionException.Reason.INVALID_START_POSITION,
                    "Requested position: " + startPosition + " is lower than start position: " + descriptor.startPosition + " for recording: " + recordingId)
                );
            }
            return 1;
        }
        if (descriptor.stopPosition != NULL_POSITION && startPosition > descriptor.stopPosition)
        {
            state(State.FAILED);
            if (listener != null)
            {
                listener.onError(new PersistentSubscriptionException(
                    PersistentSubscriptionException.Reason.INVALID_START_POSITION,
                    "Requested position: " + startPosition + " is greater than stop position: " + descriptor.stopPosition + " for recording: " + recordingId)
                );
            }
            return 1;
        }

        replaySubscription = aeronArchive.context().aeron()
            .addSubscription("aeron:udp?endpoint=localhost:0", -5);

        candidateSwitchPosition = aeronArchive.getMaxRecordedPosition(recordingId);

        // TODO how are we supposed to use a response channel here?
        // TODO async
        final String replayChannel = "aeron:udp?endpoint=" + replaySubscription.resolvedEndpoint();
        replaySessionId = (int)aeronArchive.startReplay(
            recordingId,
            startPosition,
            AeronArchive.REPLAY_ALL_AND_FOLLOW,
            replayChannel,
            replaySubscription.streamId());

        state(State.REPLAY);
        joinError = Long.MIN_VALUE;

        return 1;
    }

    private int replay(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        final Image replayImage = replaySubscription.imageBySessionId(replaySessionId);

        if (replayImage == null)
        {
            return 0;
        }

        final int fragments = replayImage.controlledPoll((b,o,l,h) -> {
            System.out.println("Consuming from replay at position = " + h.position() + ", message length = " + l);
            return fragmentHandler.onFragment(b,o,l,h);
        }, fragmentLimit);

        final long replayedPosition = replayImage.position();
        if (replayedPosition >= candidateSwitchPosition)
        {
            final long maxRecordedPosition = aeronArchive.getMaxRecordedPosition(recordingId);
            if (closeEnough(replayedPosition, maxRecordedPosition))
            {
                liveSubscription = aeronArchive.context().aeron().addSubscription(liveChannel, liveStreamId);
                state(State.ATTEMPT_SWITCH);
            }
            else
            {
                candidateSwitchPosition = maxRecordedPosition;
            }
        }

        return fragments;
    }

    private boolean closeEnough(final long replayedPosition, final long maxRecordedPosition)
    {
        return replayedPosition >= maxRecordedPosition - (descriptor.termBufferLength >> 2);
    }

    private int attemptSwitch(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        if (!liveSubscription.isConnected())
        {
            // hacky way of waiting for the subscription to connect.
            return 0;
        }

        final Image replayImage = replaySubscription.imageBySessionId(replaySessionId);

        if (replayImage == null)
        {
            return 0;
        }

        int fragments = 0;

        final long joinPosition = liveSubscription.imageAtIndex(0).joinPosition();
        final long replayPosition = replayImage.position();

        if (joinError == Long.MIN_VALUE)
        {
            joinError = joinPosition - replayPosition;

            System.out.println("=== PERSISTENT_SUBSCRIPTION");
            System.out.println("joinPosition = " + joinPosition);
            aeronArchive.context().aeron().printCounters(System.out);
        }

        if (replayPosition == joinPosition)
        {
            state(State.LIVE);
        }
        else
        {
            // Let the live channel catch up to the point we are at in the replay (but don't overtake it).
            fragments += liveSubscription.controlledPoll((buffer, offset, length, header) -> {
                final long currentLivePosition = header.position();
                final long lastReplayPosition = replayImage.position();
                if (currentLivePosition <= lastReplayPosition)
                {
                    System.out.println("Skipping from live at position = " + header.position());
                    return ControlledFragmentHandler.Action.CONTINUE;
                }
                nextLivePosition = currentLivePosition;
                return ControlledFragmentHandler.Action.ABORT;
            }, fragmentLimit);

            // Carry on with the replay for now.
            fragments += replayImage.controlledPoll((buffer, offset, length, header) -> {
                    final long currentReplayPosition = header.position();
                    if (currentReplayPosition == nextLivePosition)
                    {
                        state(State.LIVE);
                        return ControlledFragmentHandler.Action.ABORT;
                    }
                    System.out.println("Consuming from replay at position = " + header.position());
                    return fragmentHandler.onFragment(buffer, offset, length, header);
                },
                1
            );
        }

        if (isLive() && replaySubscription.isConnected())
        {
            CloseHelper.close(replaySubscription);
        }

        return fragments;
    }


    private int live(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        if (!liveSubscription.isConnected())
        {
            // TODO need to actually restart the replay from the right point
            state(State.REPLAY);
            joinError = Long.MIN_VALUE;
            return 0;
        }

        return liveSubscription.controlledPoll((b,o,l,h) -> {
            System.out.println("Consuming from live at position = " + h.position());
            return fragmentHandler.onFragment(b,o,l,h);
            }, fragmentLimit);
    }

    private void state(final State newState)
    {
        System.out.println("State: " + state + " -> " + newState);
        if (newState != this.state)
        {
            this.state = newState;
        }
    }

    private enum State
    {
        INIT,
        REPLAY,
        ATTEMPT_SWITCH,
        LIVE,
        FAILED,
    }

    private static final class RecordingDescriptorConsumerImpl implements RecordingDescriptorConsumer
    {
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
        private long recordingId = Aeron.NULL_VALUE;
        private long startPosition = 0; // TODO default to FROM_LIVE
        private String liveChannel = null;
        private int liveStreamId = Aeron.NULL_VALUE;
        private PersistentSubscriptionListener listener = null;
        private AeronArchive.Context aeronArchiveContext = null;

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
}
