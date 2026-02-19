package io.aeron.archive.client;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.CloseHelper;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static java.util.Objects.requireNonNull;

public final class PersistentSubscription implements AutoCloseable
{
    private final RecordingDescriptorConsumerImpl descriptor = new RecordingDescriptorConsumerImpl();
    private final AeronArchive aeronArchive;
    private final long recordingId;
    private final PersistentSubscriptionListener listener;
    private final String liveChannel;
    private final int streamId;

    private State state;
    private long position;
    private long joinError;
    private Subscription replaySubscription;
    private long candidateSwitchPosition;
    private int replaySessionId;
    private Subscription liveSubscription;

    private long nextLivePosition = Aeron.NULL_VALUE;
    private boolean live = false;

    public PersistentSubscription(
        final AeronArchive aeronArchive, // TODO passing an instance does not allow to reconnect, also, we probably need to own it?
        final long recordingId,
        final long startPosition,
        final String liveChannel,
        final int streamId, // TODO liveStreamId?
        final PersistentSubscriptionListener listener)
    {
        this.liveChannel = liveChannel;
        this.streamId = streamId;
        requireNonNull(aeronArchive);
        requireNonNull(liveChannel);

        if (recordingId < 0)
        {
            throw new IllegalArgumentException("invalid recordingId " + recordingId);
        }

        if (startPosition < 0)
        {
            throw new IllegalArgumentException("invalid startPosition " + startPosition);
        }

        this.listener = listener;
        this.aeronArchive = aeronArchive;
        this.recordingId = recordingId;
        this.position = startPosition;

        state(State.INIT);
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

    public boolean isLive()
    {
        return live;
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

        // TODO should we be checking those or should we just allow replay request to fail?
        if (position < descriptor.startPosition)
        {
            state(State.FAILED);
            if (listener != null)
            {
                listener.onError(new PersistentSubscriptionException(
                    PersistentSubscriptionException.Reason.INVALID_START_POSITION,
                    "Requested position: " + position + " is lower than start position: " + descriptor.startPosition + " for recording: " + recordingId)
                );
            }
            return 1;
        }
        if (descriptor.stopPosition != NULL_POSITION && position > descriptor.stopPosition)
        {
            state(State.FAILED);
            if (listener != null)
            {
                listener.onError(new PersistentSubscriptionException(
                    PersistentSubscriptionException.Reason.INVALID_START_POSITION,
                    "Requested position: " + position + " is greater than stop position: " + descriptor.stopPosition + " for recording: " + recordingId)
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
            position,
            AeronArchive.REPLAY_ALL_AND_FOLLOW,
            replayChannel,
            replaySubscription.streamId());

        state(State.REPLAY);

        return 1;
    }

    private int replay(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        final Image replayImage = replaySubscription.imageBySessionId(replaySessionId);

        if (replayImage == null)
        {
            return 0;
        }

        final int fragments = replayImage.controlledPoll(fragmentHandler, fragmentLimit);

        final long replayedPosition = replayImage.position();
        if (replayedPosition >= candidateSwitchPosition)
        {
            final long maxRecordedPosition = aeronArchive.getMaxRecordedPosition(recordingId);
            if (closeEnough(replayedPosition, maxRecordedPosition))
            {
                liveSubscription = aeronArchive.context().aeron().addSubscription(liveChannel, streamId, i -> {}, i -> {
                    System.out.println("image unavailable: " + i);
                });
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

        fragments += liveSubscription.controlledPoll((buffer, offset, length, header) -> {
            final long currentLivePosition = header.position();
            final long lastReplayPosition = replayImage.position();
            //System.out.println("currentLivePosition = " + currentLivePosition + ", lastReplayPosition = " + lastReplayPosition + " - " + live);
//            if (live)
//            {
//                System.out.println("Consuming at position: " + currentLivePosition + " from live");
//                return fragmentHandler.onFragment(buffer, offset, length, header);
//            }
            if (currentLivePosition <= lastReplayPosition)
            {
                return ControlledFragmentHandler.Action.CONTINUE;
            }
            nextLivePosition = currentLivePosition;
            return ControlledFragmentHandler.Action.ABORT;
        }, fragmentLimit);

        // Carry on with the replay for now.
        fragments += replayImage.controlledPoll((buffer, offset, length, header) -> {
                final long currentReplayPosition = header.position();
                //System.out.println("currentReplayPosition = " + currentReplayPosition);
                if (currentReplayPosition == nextLivePosition)
                {
                    //System.out.println("Replay caught up with live at " + currentReplayPosition);
                    live = true; // TODO transition to a live state.
                    final long joinPosition = liveSubscription.imageAtIndex(0).joinPosition();
                    joinError = currentReplayPosition - joinPosition;
                    state(State.LIVE);
                    return ControlledFragmentHandler.Action.ABORT;
                }
                //System.out.println("Consuming at position: " + currentReplayPosition + " from replay");
                return fragmentHandler.onFragment(buffer, offset, length, header);
            },
//            fragmentLimit
            1
        );
        //System.out.println("fragments (from replay) = " + fragments);

        if (live && replaySubscription.isConnected())
        {
            //System.out.println("Closing replay");
            CloseHelper.close(replaySubscription);
        }

        // Let the live channel catch up to the point we are at in the replay (but don't overtake it).
//        liveSubscription.images().forEach(i -> System.out.println("i = " + i.position()));
        //System.out.println("fragments (total) = " + fragments);
        return fragments;
    }


    private int live(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        if (!liveSubscription.isConnected())
        {
            // TODO need to actually restart the replay from the right point
            state(State.REPLAY);
            live = false;
            return 0;
        }

        int fragments = liveSubscription.controlledPoll((buffer, offset, length, header) -> {
            final long currentLivePosition = header.position();
            //System.out.println("Consuming at position: " + currentLivePosition + " from live");
            return fragmentHandler.onFragment(buffer, offset, length, header);
        }, fragmentLimit);
        return fragments;
    }

    public void close()
    {
        // TODO do we need to explicitly stop replay if there is one?
    }

    private void onLiveAvailable(final Image image)
    {

    }

    private void onLiveUnavailable(final Image image)
    {

    }

    private void state(final State newState)
    {
        System.out.println("State: " + state + " -> " + newState);
        if (newState != this.state)
        {
            this.state = newState;
        }
    }

    public boolean hasFailed()
    {
        return state == State.FAILED;
    }

    public long joinError()
    {
        return joinError;
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
        }
    }
}
