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
package io.aeron.cluster.bridge;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.client.ReplayMerge;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Bridge receiver component that subscribes to messages with replay support.
 * <p>
 * <b>Design Rationale (SOLID - Single Responsibility):</b>
 * This class handles only the receiving side: subscription, replay-merge,
 * deduplication, and checkpointing. Sending is handled by {@link BridgeSender}.
 * <p>
 * <b>Design Rationale (SOLID - Open/Closed):</b>
 * Message processing is delegated to a {@link MessageHandler} callback, allowing
 * different processing logic without modifying this class.
 * <p>
 * <b>Design Rationale (SOLID - Dependency Inversion):</b>
 * The receiver depends on the MessageHandler interface for processing, not on
 * concrete implementations. This allows easy testing and extensibility.
 * <p>
 * <b>Key Feature - ReplayMerge:</b>
 * Uses Aeron's ReplayMerge to seamlessly transition from archive replay to
 * live stream without duplicates. The merge process:
 * <ol>
 *   <li>Starts replay from last checkpoint position</li>
 *   <li>Catches up to current recording position</li>
 *   <li>Adds live destination when within merge window</li>
 *   <li>Removes replay when live stream overtakes</li>
 * </ol>
 * <p>
 * <b>Assumptions:</b>
 * <ul>
 *   <li>Sequence numbers are globally unique per direction</li>
 *   <li>Messages with sequence &lt;= lastAppliedSequence are duplicates</li>
 *   <li>Checkpoint is updated atomically (see {@link BridgeCheckpoint})</li>
 *   <li>Archive is available on same machine as receiver</li>
 * </ul>
 */
public final class BridgeReceiver implements AutoCloseable
{
    private final BridgeConfiguration.Direction direction;
    private final String channel;
    private final int streamId;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final IdleStrategy idleStrategy;

    // Aeron components
    private ArchivingMediaDriver archivingMediaDriver;
    private AeronArchive aeronArchive;
    private Subscription subscription;
    private BridgeCheckpoint checkpoint;
    private ReplayMerge replayMerge;

    // State
    private long lastAppliedSequence = 0;
    private long messagesReceived = 0;
    private long duplicatesDiscarded = 0;
    private boolean merged = false;

    // Message processing callback
    private MessageHandler messageHandler;

    /**
     * Callback interface for processing received messages.
     * <p>
     * <b>Design Rationale (SOLID - Interface Segregation):</b>
     * This interface has a single method, keeping it focused and easy to implement.
     * <p>
     * <b>Thread Safety:</b>
     * Implementations must be safe for single-threaded use (same thread as receiver).
     */
    @FunctionalInterface
    public interface MessageHandler
    {
        /**
         * Called when a valid, non-duplicate message is received.
         * <p>
         * <b>Contract:</b>
         * <ul>
         *   <li>Messages are delivered in sequence order</li>
         *   <li>Duplicates have already been filtered</li>
         *   <li>Buffer contents are only valid during this call</li>
         * </ul>
         *
         * @param sequence  the message sequence number
         * @param timestamp the message timestamp (nanoTime)
         * @param msgType   the message type
         * @param buffer    the payload buffer
         * @param offset    the offset of payload in buffer
         * @param length    the payload length
         */
        void onMessage(long sequence, long timestamp, int msgType, DirectBuffer buffer, int offset, int length);
    }

    /**
     * Create a new bridge receiver for the specified direction.
     *
     * @param direction the message flow direction
     */
    public BridgeReceiver(final BridgeConfiguration.Direction direction)
    {
        this.direction = direction;
        this.channel = BridgeConfiguration.channel();
        this.streamId = BridgeConfiguration.streamId();
        this.idleStrategy = YieldingIdleStrategy.INSTANCE;
    }

    /**
     * Set the message handler callback.
     * <p>
     * <b>Fluent API:</b> Returns this for method chaining.
     *
     * @param handler the message handler
     * @return this receiver
     */
    public BridgeReceiver messageHandler(final MessageHandler handler)
    {
        this.messageHandler = handler;
        return this;
    }

    /**
     * Initialize the receiver with media driver and archive.
     * <p>
     * <b>Recovery Behavior:</b>
     * If a checkpoint exists and archive recordings are available, initiates
     * replay-merge to catch up from last checkpoint position.
     *
     * @throws IOException if checkpoint creation fails
     */
    public void start() throws IOException
    {
        System.out.println("[Bridge Receiver] Starting " + direction + " receiver");
        System.out.println("[Bridge Receiver] Channel: " + channel);
        System.out.println("[Bridge Receiver] Stream ID: " + streamId);

        // Load or create checkpoint
        checkpoint = new BridgeCheckpoint(BridgeConfiguration.checkpointDir(), direction);
        lastAppliedSequence = checkpoint.lastAppliedSequence();
        System.out.println("[Bridge Receiver] Checkpoint: seq=" + lastAppliedSequence +
            ", pos=" + checkpoint.archivePosition());

        final String archiveDir = BridgeConfiguration.archiveDir();
        final File archiveDirFile = new File(archiveDir);
        if (!archiveDirFile.exists())
        {
            archiveDirFile.mkdirs();
        }

        // Configure and launch archiving media driver
        // CRITICAL: Never delete on shutdown to prevent data loss
        final boolean deleteOnStart = Boolean.getBoolean("bridge.delete.on.start");
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(deleteOnStart)
            .dirDeleteOnShutdown(false)  // Never delete on shutdown
            .spiesSimulateConnection(true);

        // Archive requires control and replication channels for client communication
        final int portBase = direction == BridgeConfiguration.Direction.ME_TO_RMS ? 8030 : 8040;
        final String controlChannel = "aeron:udp?endpoint=localhost:" + portBase;
        final String replicationChannel = "aeron:udp?endpoint=localhost:" + (portBase + 1);

        final Archive.Context archiveCtx = new Archive.Context()
            .archiveDir(archiveDirFile)
            .controlChannel(controlChannel)
            .replicationChannel(replicationChannel)
            .deleteArchiveOnStart(false);

        archivingMediaDriver = ArchivingMediaDriver.launch(driverCtx, archiveCtx);

        // Connect to archive with unique response stream ID
        // Must set control channels to match the archive's configuration
        final int responsePort = portBase + 100;
        final String responseChannel = "aeron:udp?endpoint=localhost:" + responsePort;
        final AeronArchive.Context archiveClientCtx = new AeronArchive.Context()
            .controlRequestChannel(controlChannel)
            .controlResponseChannel(responseChannel)
            .controlResponseStreamId(AeronArchive.Configuration.controlResponseStreamId() +
                (direction == BridgeConfiguration.Direction.ME_TO_RMS ? 2 : 4));

        aeronArchive = AeronArchive.connect(archiveClientCtx);

        // Determine if we need replay
        final long recordingId = findLatestRecording();

        if (recordingId >= 0 && checkpoint.archivePosition() > 0 && BridgeConfiguration.replayMerge())
        {
            // Use ReplayMerge for recovery from checkpoint
            startReplayMerge(recordingId);
        }
        else
        {
            // Direct subscription (no replay needed - first run)
            startDirectSubscription();
        }
    }

    /**
     * Start direct subscription without replay (for first-time startup).
     */
    private void startDirectSubscription()
    {
        System.out.println("[Bridge Receiver] Starting direct subscription (no replay)");
        subscription = aeronArchive.context().aeron().addSubscription(channel, streamId);
    }

    /**
     * Start replay-merge for recovery from checkpoint.
     * <p>
     * <b>ReplayMerge Requirements:</b>
     * <ul>
     *   <li>Subscription must use manual control mode</li>
     *   <li>Replay and live destinations must be configured</li>
     *   <li>Recording must be active or have stop position</li>
     * </ul>
     */
    private void startReplayMerge(final long recordingId)
    {
        System.out.println("[Bridge Receiver] Starting replay merge from recordingId=" + recordingId);
        System.out.println("[Bridge Receiver] Replay from position: " + checkpoint.archivePosition());

        final Aeron aeron = aeronArchive.context().aeron();

        // Parse endpoint from channel
        final ChannelUri channelUri = ChannelUri.parse(channel);
        final String endpoint = channelUri.get("endpoint");

        // Multi-destination subscription with manual control (required for ReplayMerge)
        final String subscriptionChannel = new ChannelUriStringBuilder()
            .media("udp")
            .controlMode("manual")
            .build();

        subscription = aeron.addSubscription(subscriptionChannel, streamId);

        // Replay channel configuration
        final String replayChannel = new ChannelUriStringBuilder()
            .media("udp")
            .build();

        // Replay destination uses ephemeral port (0 = OS assigns)
        final String replayDestination = "aeron:udp?endpoint=localhost:0";

        // Live destination uses the configured endpoint
        final String liveDestination = "aeron:udp?endpoint=" + endpoint;

        // Create ReplayMerge instance
        replayMerge = new ReplayMerge(
            subscription,
            aeronArchive,
            replayChannel,
            replayDestination,
            liveDestination,
            recordingId,
            checkpoint.archivePosition(),
            aeron.context().epochClock(),
            BridgeConfiguration.mergeTimeoutMs());
    }

    /**
     * Find the latest recording for this channel/stream.
     *
     * @return the recording ID, or -1 if none found
     */
    private long findLatestRecording()
    {
        final MutableLong lastRecordingId = new MutableLong(-1);

        final RecordingDescriptorConsumer consumer =
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
            {
                lastRecordingId.set(recordingId);
            };

        try
        {
            final int foundCount = aeronArchive.listRecordingsForUri(
                0L, 100, channel, streamId, consumer);

            if (foundCount > 0)
            {
                System.out.println("[Bridge Receiver] Found recording: " + lastRecordingId.get());
            }
            else
            {
                System.out.println("[Bridge Receiver] No recordings found");
            }
        }
        catch (final Exception e)
        {
            System.out.println("[Bridge Receiver] Error listing recordings: " + e.getMessage());
        }

        return lastRecordingId.get();
    }

    /**
     * Poll for messages from the subscription.
     * <p>
     * <b>Behavior:</b>
     * <ul>
     *   <li>If replay-merge active: polls merge and monitors state</li>
     *   <li>If merged or direct: polls subscription directly</li>
     * </ul>
     *
     * @param fragmentLimit maximum fragments to process per call
     * @return number of fragments processed
     */
    public int poll(final int fragmentLimit)
    {
        if (replayMerge != null && !merged)
        {
            final int fragments = replayMerge.poll(this::onFragment, fragmentLimit);

            if (replayMerge.isMerged())
            {
                System.out.println("[Bridge Receiver] Live stream merged at seq=" + lastAppliedSequence);
                merged = true;
            }
            else if (replayMerge.hasFailed())
            {
                System.err.println("[Bridge Receiver] Replay merge failed");
                running.set(false);
            }

            return fragments;
        }
        else if (subscription != null)
        {
            return subscription.poll(this::onFragment, fragmentLimit);
        }

        return 0;
    }

    /**
     * Fragment handler for incoming messages.
     * <p>
     * <b>Processing Steps:</b>
     * <ol>
     *   <li>Validate message size</li>
     *   <li>Decode header</li>
     *   <li>Check for duplicates (sequence &lt;= lastApplied)</li>
     *   <li>Detect gaps (optional warning)</li>
     *   <li>Invoke message handler</li>
     *   <li>Update state and checkpoint</li>
     * </ol>
     */
    private void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        // Validate minimum size
        if (length < BridgeMessageHeader.HEADER_SIZE)
        {
            System.err.println("[Bridge Receiver] Invalid message: too short (" + length + " bytes)");
            return;
        }

        // Decode header
        final long sequence = BridgeMessageHeader.sequence(buffer, offset);
        final long timestamp = BridgeMessageHeader.timestamp(buffer, offset);
        final int msgType = BridgeMessageHeader.msgType(buffer, offset);
        final int payloadLength = BridgeMessageHeader.payloadLength(buffer, offset);

        // Deduplication check - critical for correctness
        if (sequence <= lastAppliedSequence)
        {
            duplicatesDiscarded++;
            return;
        }

        // Gap detection (informational - application may want to know)
        if (sequence != lastAppliedSequence + 1 && lastAppliedSequence > 0)
        {
            System.out.println("[Bridge Receiver] Sequence gap detected: expected " +
                (lastAppliedSequence + 1) + ", received " + sequence);
        }

        // Invoke application handler
        if (messageHandler != null)
        {
            messageHandler.onMessage(
                sequence,
                timestamp,
                msgType,
                buffer,
                offset + BridgeMessageHeader.HEADER_SIZE,
                payloadLength);
        }
        else
        {
            // Default: log message receipt
            final String source = merged ? "live" : (replayMerge != null ? "replay" : "direct");
            System.out.println("[Bridge Receiver] Received (" + source + "): seq=" + sequence +
                ", type=" + msgType + ", len=" + payloadLength);
        }

        // Update state
        lastAppliedSequence = sequence;
        messagesReceived++;

        // Periodic checkpoint (every 100 messages for durability vs performance tradeoff)
        if (messagesReceived % 100 == 0)
        {
            updateCheckpoint();
        }
    }

    /**
     * Update checkpoint with current state.
     */
    private void updateCheckpoint()
    {
        if (checkpoint != null)
        {
            final long recordingId = findLatestRecording();
            final long position = getImagePosition();
            checkpoint.update(lastAppliedSequence, recordingId, position);
        }
    }

    /**
     * Get the current image position for checkpointing.
     */
    private long getImagePosition()
    {
        if (subscription == null)
        {
            return 0;
        }

        for (final Image image : subscription.images())
        {
            return image.position();
        }

        return 0;
    }

    /**
     * Run the receiver loop (blocking).
     */
    public void run()
    {
        while (running.get())
        {
            final int fragments = poll(10);
            idleStrategy.idle(fragments);
        }
    }

    /**
     * Signal graceful shutdown.
     */
    public void shutdown()
    {
        running.set(false);
    }

    /**
     * Check if receiver is running.
     *
     * @return true if running
     */
    public boolean isRunning()
    {
        return running.get();
    }

    /**
     * Get the last applied sequence number.
     *
     * @return the last applied sequence
     */
    public long lastAppliedSequence()
    {
        return lastAppliedSequence;
    }

    /**
     * Get total messages received (excluding duplicates).
     *
     * @return messages received count
     */
    public long messagesReceived()
    {
        return messagesReceived;
    }

    /**
     * Get number of duplicates discarded.
     *
     * @return duplicates discarded count
     */
    public long duplicatesDiscarded()
    {
        return duplicatesDiscarded;
    }

    /**
     * Check if merged to live stream.
     *
     * @return true if merged
     */
    public boolean isMerged()
    {
        return merged;
    }

    /**
     * Close the receiver and release resources.
     */
    @Override
    public void close()
    {
        running.set(false);

        // Final checkpoint
        if (checkpoint != null)
        {
            updateCheckpoint();
            System.out.println("[Bridge Receiver] Final checkpoint: seq=" + lastAppliedSequence);
        }

        CloseHelper.quietClose(replayMerge);
        CloseHelper.quietClose(subscription);
        CloseHelper.quietClose(checkpoint);
        CloseHelper.quietClose(aeronArchive);
        CloseHelper.quietClose(archivingMediaDriver);

        System.out.println("[Bridge Receiver] Closed. Messages received: " + messagesReceived +
            ", Duplicates discarded: " + duplicatesDiscarded);
    }

    /**
     * Main entry point for standalone receiver operation.
     *
     * @param args command line arguments (unused, use system properties)
     * @throws Exception if startup fails
     */
    public static void main(final String[] args) throws Exception
    {
        final BridgeConfiguration.Direction direction = BridgeConfiguration.direction();

        System.out.println("[Bridge Receiver] Direction: " + direction);
        System.out.println("[Bridge Receiver] Waiting for messages...");

        final AtomicBoolean running = new AtomicBoolean(true);
        @SuppressWarnings("try")
        final ShutdownSignalBarrier ignored = new ShutdownSignalBarrier(() -> running.set(false));
        try (BridgeReceiver receiver = new BridgeReceiver(direction))
        {
            receiver.start();

            while (running.get() && receiver.isRunning())
            {
                receiver.poll(10);
                Thread.sleep(1);
            }

            System.out.println("[Bridge Receiver] Shutting down...");
        }
    }
}
