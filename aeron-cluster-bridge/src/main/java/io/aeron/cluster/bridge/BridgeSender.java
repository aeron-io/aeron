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

import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Bridge sender component that publishes messages with Aeron Archive recording.
 * <p>
 * <b>Design Rationale (SOLID - Single Responsibility):</b>
 * This class handles only the sending side of the bridge: message publication
 * and archive recording. Receiving is handled by {@link BridgeReceiver}.
 * <p>
 * <b>Design Rationale (SOLID - Dependency Inversion):</b>
 * The sender depends on Aeron's Publication and AeronArchive abstractions,
 * not on concrete transport implementations. This allows the same code to
 * work with different channel configurations (UDP unicast, multicast, IPC).
 * <p>
 * <b>Design Rationale (Performance - Pre-allocation):</b>
 * The send buffer is pre-allocated once and reused for all messages. This
 * eliminates allocation in the hot path, reducing GC pressure.
 * <p>
 * <b>Thread Safety:</b>
 * This class is NOT thread-safe. Use a single thread for publication.
 * <p>
 * <b>Assumptions:</b>
 * <ul>
 *   <li>Archive directory is shared between sender and receiver on same machine</li>
 *   <li>One sender per direction (ME_TO_RMS or RMS_TO_ME)</li>
 *   <li>Publication back-pressure is handled via retry with IdleStrategy</li>
 * </ul>
 */
public final class BridgeSender implements AutoCloseable
{
    private final BridgeConfiguration.Direction direction;
    private final String channel;
    private final int streamId;
    private final AtomicBoolean running = new AtomicBoolean(true);

    // Pre-allocated buffer for zero-allocation sending
    private final UnsafeBuffer sendBuffer;
    private final IdleStrategy idleStrategy;

    // Aeron components (initialized in start())
    private ArchivingMediaDriver archivingMediaDriver;
    private AeronArchive aeronArchive;
    private Publication publication;

    // State
    private long recordingId = -1;
    private long nextSequence = 1;
    private int recordingCounterId = CountersReader.NULL_COUNTER_ID;

    // Pre-allocated heartbeat buffer (avoids allocation in hot path)
    private final UnsafeBuffer heartbeatPayload = new UnsafeBuffer(new byte[8]);

    /**
     * Create a new bridge sender for the specified direction.
     * <p>
     * <b>Note:</b> Call {@link #start()} before publishing messages.
     *
     * @param direction the message flow direction
     */
    public BridgeSender(final BridgeConfiguration.Direction direction)
    {
        this.direction = direction;
        this.channel = BridgeConfiguration.channel();
        this.streamId = BridgeConfiguration.streamId();

        // Pre-allocate send buffer with cache-line alignment for performance
        this.sendBuffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(
            BridgeConfiguration.MAX_MESSAGE_SIZE, 64));

        this.idleStrategy = YieldingIdleStrategy.INSTANCE;
    }

    /**
     * Initialize the sender with media driver and archive.
     * <p>
     * <b>Lifecycle:</b> Must be called once before any publish operations.
     * <p>
     * <b>Side Effects:</b>
     * <ul>
     *   <li>Launches embedded ArchivingMediaDriver if configured</li>
     *   <li>Starts recording on the configured channel/stream</li>
     *   <li>Creates Publication for sending</li>
     * </ul>
     */
    public void start()
    {
        System.out.println("[Bridge Sender] Starting " + direction + " sender");
        System.out.println("[Bridge Sender] Channel: " + channel);
        System.out.println("[Bridge Sender] Stream ID: " + streamId);

        final String archiveDir = BridgeConfiguration.archiveDir();
        final File archiveDirFile = new File(archiveDir);
        if (!archiveDirFile.exists())
        {
            archiveDirFile.mkdirs();
        }

        // Configure and launch archiving media driver
        // Design Note: Preserve media driver data across restarts in production
        // Use environment variable BRIDGE_DELETE_ON_START=true for testing/demos
        final boolean deleteOnStart = Boolean.getBoolean("bridge.delete.on.start");
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(deleteOnStart)
            .dirDeleteOnShutdown(false)  // CRITICAL: Never delete on shutdown to prevent data loss
            .spiesSimulateConnection(true);  // Allows publication without subscribers

        // Archive requires control and replication channels for client communication
        final int portBase = direction == BridgeConfiguration.Direction.ME_TO_RMS ? 8010 : 8020;
        final String controlChannel = "aeron:udp?endpoint=localhost:" + portBase;
        final String replicationChannel = "aeron:udp?endpoint=localhost:" + (portBase + 1);

        final Archive.Context archiveCtx = new Archive.Context()
            .archiveDir(archiveDirFile)
            .controlChannel(controlChannel)
            .replicationChannel(replicationChannel)
            .deleteArchiveOnStart(deleteOnStart);  // Only delete archive when explicitly requested

        archivingMediaDriver = ArchivingMediaDriver.launch(driverCtx, archiveCtx);

        // Connect to archive with unique response stream ID per direction
        // This prevents response channel conflicts between ME_TO_RMS and RMS_TO_ME
        // Must set control channels to match the archive's configuration
        final int responsePort = portBase + 100;
        final String responseChannel = "aeron:udp?endpoint=localhost:" + responsePort;
        final AeronArchive.Context archiveClientCtx = new AeronArchive.Context()
            .controlRequestChannel(controlChannel)
            .controlResponseChannel(responseChannel)
            .controlResponseStreamId(AeronArchive.Configuration.controlResponseStreamId() +
                (direction == BridgeConfiguration.Direction.ME_TO_RMS ? 1 : 3));

        aeronArchive = AeronArchive.connect(archiveClientCtx);

        // Start recording on the channel/stream
        // SourceLocation.LOCAL means recording happens on this machine
        aeronArchive.startRecording(channel, streamId, SourceLocation.LOCAL);

        // Create publication
        publication = aeronArchive.context().aeron().addPublication(channel, streamId);

        // Wait for recording to start (required before publishing)
        final CountersReader counters = aeronArchive.context().aeron().countersReader();
        final long archiveId = aeronArchive.archiveId();

        while (CountersReader.NULL_COUNTER_ID == recordingCounterId && running.get())
        {
            idleStrategy.idle();
            recordingCounterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId(), archiveId);
        }

        if (running.get())
        {
            recordingId = RecordingPos.getRecordingId(counters, recordingCounterId);
            System.out.println("[Bridge Sender] Recording started: recordingId=" + recordingId);
        }
    }

    /**
     * Publish a message with the specified type and payload.
     * <p>
     * <b>Backpressure Handling:</b>
     * If the publication is back-pressured, this method will retry with
     * the configured IdleStrategy until successful or shutdown.
     * <p>
     * <b>Performance Note:</b>
     * This method uses the pre-allocated sendBuffer and performs no allocations.
     *
     * @param msgType       the message type (see BridgeConfiguration.MSG_TYPE_*)
     * @param payload       the payload buffer (may be null for header-only messages)
     * @param payloadOffset the offset in the payload buffer
     * @param payloadLength the length of the payload (0 for header-only)
     * @return the publication position if successful, or negative error code
     */
    public long publish(final int msgType, final UnsafeBuffer payload, final int payloadOffset, final int payloadLength)
    {
        final long sequence = nextSequence++;
        final long timestamp = System.nanoTime();

        // Encode header into pre-allocated buffer
        BridgeMessageHeader.encode(sendBuffer, 0, sequence, timestamp, msgType, payloadLength);

        // Copy payload if present
        if (payloadLength > 0 && payload != null)
        {
            sendBuffer.putBytes(BridgeMessageHeader.HEADER_SIZE, payload, payloadOffset, payloadLength);
        }

        final int totalLength = BridgeMessageHeader.HEADER_SIZE + payloadLength;
        long result;

        // Retry loop with backpressure handling
        while (true)
        {
            result = publication.offer(sendBuffer, 0, totalLength);

            if (result > 0)
            {
                // Success - message was published
                break;
            }
            else if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION)
            {
                // Transient condition - retry with idle
                idleStrategy.idle();
            }
            else if (result == Publication.NOT_CONNECTED)
            {
                // No subscribers yet - retry with idle
                idleStrategy.idle();
                if (!running.get())
                {
                    return result;
                }
            }
            else if (result == Publication.CLOSED)
            {
                System.err.println("[Bridge Sender] Publication closed unexpectedly");
                return result;
            }
            else if (result == Publication.MAX_POSITION_EXCEEDED)
            {
                // Term exhaustion - should not happen in normal operation
                System.err.println("[Bridge Sender] Max position exceeded - term exhausted");
                return result;
            }
        }

        return result;
    }

    /**
     * Publish a heartbeat message.
     * <p>
     * Heartbeat messages contain only the current timestamp as payload.
     * Used for connectivity testing and latency measurement.
     * <p>
     * <b>Performance Note:</b>
     * Uses pre-allocated buffer to avoid GC pressure in hot path.
     *
     * @return the publication position if successful, or negative error code
     */
    public long publishHeartbeat()
    {
        final long timestamp = System.nanoTime();
        heartbeatPayload.putLong(0, timestamp);
        return publish(BridgeConfiguration.MSG_TYPE_HEARTBEAT, heartbeatPayload, 0, 8);
    }

    /**
     * Wait for the archive to catch up to the publication position.
     * <p>
     * Call this before shutdown to ensure all published messages are recorded.
     */
    public void awaitArchiveCatchup()
    {
        if (recordingCounterId == CountersReader.NULL_COUNTER_ID || publication == null)
        {
            return;
        }

        final CountersReader counters = aeronArchive.context().aeron().countersReader();
        idleStrategy.reset();

        while (counters.getCounterValue(recordingCounterId) < publication.position())
        {
            if (!RecordingPos.isActive(counters, recordingCounterId, recordingId))
            {
                break;
            }
            idleStrategy.idle();
        }
    }

    /**
     * Get the current recording ID.
     *
     * @return the archive recording ID, or -1 if not yet recording
     */
    public long recordingId()
    {
        return recordingId;
    }

    /**
     * Get the current publication position.
     *
     * @return the publication position in bytes
     */
    public long position()
    {
        return publication != null ? publication.position() : 0;
    }

    /**
     * Get the next sequence number that will be used.
     *
     * @return the next sequence number
     */
    public long nextSequence()
    {
        return nextSequence;
    }

    /**
     * Signal graceful shutdown.
     */
    public void shutdown()
    {
        running.set(false);
    }

    /**
     * Check if sender is running.
     *
     * @return true if running
     */
    public boolean isRunning()
    {
        return running.get();
    }

    /**
     * Close the sender and release resources.
     * <p>
     * <b>Cleanup Order:</b>
     * <ol>
     *   <li>Wait for archive to catch up</li>
     *   <li>Stop recording</li>
     *   <li>Close publication</li>
     *   <li>Close archive client</li>
     *   <li>Close media driver</li>
     * </ol>
     */
    @Override
    public void close()
    {
        running.set(false);

        awaitArchiveCatchup();

        if (aeronArchive != null)
        {
            try
            {
                aeronArchive.stopRecording(channel, streamId);
            }
            catch (final Exception e)
            {
                // Ignore errors during shutdown
            }
        }

        CloseHelper.quietClose(publication);
        CloseHelper.quietClose(aeronArchive);
        CloseHelper.quietClose(archivingMediaDriver);

        System.out.println("[Bridge Sender] Closed. Final position: " + position());
    }

    /**
     * Main entry point for standalone sender operation.
     *
     * @param args command line arguments (unused, use system properties)
     * @throws Exception if startup fails
     */
    public static void main(final String[] args) throws Exception
    {
        final BridgeConfiguration.Direction direction = BridgeConfiguration.direction();
        final int messageCount = BridgeConfiguration.messageCount();
        final int intervalMs = BridgeConfiguration.messageIntervalMs();

        System.out.println("[Bridge Sender] Direction: " + direction);
        System.out.println("[Bridge Sender] Message count: " + messageCount);
        System.out.println("[Bridge Sender] Interval: " + intervalMs + "ms");

        final AtomicBoolean running = new AtomicBoolean(true);
        @SuppressWarnings("try")
        final ShutdownSignalBarrier ignored = new ShutdownSignalBarrier(() -> running.set(false));
        try (BridgeSender sender = new BridgeSender(direction))
        {
            sender.start();

            // Send configured number of messages
            for (int i = 0; i < messageCount && running.get() && sender.isRunning(); i++)
            {
                final long result = sender.publishHeartbeat();
                if (result > 0)
                {
                    System.out.println("[Bridge Sender] Sent message seq=" + (sender.nextSequence() - 1));
                }
                else
                {
                    System.err.println("[Bridge Sender] Failed to send: " + result);
                }

                if (intervalMs > 0)
                {
                    Thread.sleep(intervalMs);
                }
            }

            sender.awaitArchiveCatchup();
            System.out.println("[Bridge Sender] All messages sent. Archive position: " + sender.position());
        }
    }
}
