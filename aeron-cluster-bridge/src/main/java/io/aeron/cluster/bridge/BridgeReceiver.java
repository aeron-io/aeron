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

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.client.ReplayMerge;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableLong;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.BiConsumer;

/**
 * Bridge receiver that uses {@link ReplayMerge} to catch up from Archive
 * and seamlessly join the live stream.
 * <p>
 * Two initialization modes are supported:
 * <ul>
 *   <li>{@link #init()} — Replay-merge mode: loads checkpoint, replays from
 *       Archive, and merges with live. Requires an active archive recording.</li>
 *   <li>{@link #initLiveOnly()} — Live-only mode: subscribes directly to the
 *       live channel with no archive replay. Used for fresh starts.</li>
 * </ul>
 * <p>
 * <b>Design (SRP):</b> Sole responsibility is consuming bridge messages from
 * one direction. Application logic is delegated to the injected
 * {@link BiConsumer} handler (OCP — new behaviours without modification).
 * <p>
 * <b>Design (DIP):</b> Depends on the {@link AeronArchive} abstraction,
 * not on concrete driver or archive implementations.
 * <p>
 * <b>Idempotency rule:</b> Messages with {@code sequence <= lastAppliedSequence}
 * are silently skipped. This guarantees exactly-once application semantics
 * across restarts, because Archive replay may re-deliver messages that were
 * already checkpointed.
 * <p>
 * <b>Consistency model:</b> The bridge provides <em>sequential consistency</em>
 * per direction — messages are applied in the exact order produced by the sender.
 * Cross-direction ordering is not guaranteed (each direction is independent).
 */
public final class BridgeReceiver implements AutoCloseable
{
    private final AeronArchive archive;
    private final BridgeCheckpoint checkpoint;
    private final String liveChannel;
    private final int streamId;
    private final BiConsumer<Long, byte[]> messageHandler;

    private final FragmentHandler fragmentHandler;

    private Subscription subscription;
    private ReplayMerge replayMerge;
    private volatile boolean running;
    private boolean merged;
    private long receivedCount;

    /**
     * Create a bridge receiver.
     *
     * @param archive        the Aeron Archive client.
     * @param liveChannel    the live UDP channel.
     * @param streamId       the stream ID.
     * @param direction      the bridge direction constant.
     * @param checkpointDir  directory for checkpoint files.
     * @param messageHandler callback invoked for each applied message: (sequence, payloadBytes).
     */
    public BridgeReceiver(
        final AeronArchive archive,
        final String liveChannel,
        final int streamId,
        final int direction,
        final Path checkpointDir,
        final BiConsumer<Long, byte[]> messageHandler)
    {
        this.archive = archive;
        this.liveChannel = liveChannel;
        this.streamId = streamId;
        this.messageHandler = messageHandler;
        this.checkpoint = new BridgeCheckpoint(checkpointDir, direction);
        this.merged = false;
        this.running = true;
        this.receivedCount = 0;
        // Pre-build the fragment handler once to avoid per-poll lambda allocation.
        // The lambda captures 'this', so it remains valid for the receiver's lifetime.
        this.fragmentHandler = buildFragmentHandler();
    }

    /**
     * Initialize the receiver in replay-merge mode: loads checkpoint, starts
     * {@link ReplayMerge} from the last known archive position.
     * Must be called before {@link #poll()}.
     * <p>
     * Uses a Multi-Destination Subscription (MDS) with {@code control-mode=manual}
     * so ReplayMerge can dynamically add/remove replay and live destinations.
     *
     * @throws IOException if the checkpoint cannot be loaded.
     */
    public void init() throws IOException
    {
        checkpoint.load();

        final long recordingId = findRecording();
        if (recordingId < 0)
        {
            throw new IllegalStateException(
                "No recording found for channel=" + liveChannel + " streamId=" + streamId);
        }

        // MDS subscription: ReplayMerge manages destinations dynamically
        this.subscription = archive.context().aeron().addSubscription(
            BridgeConfiguration.MDS_CHANNEL, streamId);

        final long startPosition = checkpoint.archivePosition();
        final String liveDestination = "aeron:udp?endpoint=" + extractEndpoint(liveChannel);

        replayMerge = new ReplayMerge(
            subscription,
            archive,
            BridgeConfiguration.REPLAY_CHANNEL,
            BridgeConfiguration.REPLAY_DESTINATION,
            liveDestination,
            recordingId,
            startPosition);
    }

    /**
     * Initialize the receiver for live-only mode (no archive replay).
     * Uses a plain subscription on the live channel directly.
     * <p>
     * <b>Assumption:</b> No prior recordings need to be caught up from.
     * This is used for fresh starts or when the sender has not started yet.
     */
    public void initLiveOnly()
    {
        // Direct subscription: simpler, avoids MDS async destination issues
        this.subscription = archive.context().aeron().addSubscription(liveChannel, streamId);
        this.merged = true;
    }

    /**
     * Poll for messages. In replay mode, drives the {@link ReplayMerge}
     * state machine. In merged/live mode, polls the subscription directly.
     * <p>
     * Returns 0 immediately if the receiver has been shut down via
     * {@link #shutdown()}.
     *
     * @return the number of fragments received, or 0 if shut down.
     */
    public int poll()
    {
        if (!running)
        {
            return 0;
        }

        if (replayMerge != null && !merged)
        {
            return pollReplayMerge();
        }
        else
        {
            return pollLive();
        }
    }

    /**
     * Signal the receiver to stop polling. The next {@link #poll()} call
     * will return 0. This is safe to call from another thread.
     * <p>
     * <b>Note:</b> This does not close resources — call {@link #close()}
     * separately after the poll loop has exited.
     */
    public void shutdown()
    {
        running = false;
    }

    /**
     * Check if the receiver is still running (not shut down).
     *
     * @return true if the receiver is running.
     */
    public boolean isRunning()
    {
        return running;
    }

    /**
     * Check if the replay has merged with the live stream.
     *
     * @return true if merged or live-only.
     */
    public boolean isMerged()
    {
        return merged;
    }

    /**
     * Get the total number of messages received and applied (excluding skips).
     *
     * @return the received message count.
     */
    public long receivedCount()
    {
        return receivedCount;
    }

    /**
     * Get the last applied sequence from the checkpoint.
     *
     * @return the last applied sequence.
     */
    public long lastAppliedSequence()
    {
        return checkpoint.lastAppliedSequence();
    }

    /**
     * Check if the subscription is connected (has at least one active image).
     *
     * @return true if connected.
     */
    public boolean isConnected()
    {
        return subscription != null && subscription.isConnected();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Signals shutdown and uses {@link CloseHelper#closeAll} to ensure both
     * resources are closed even if the first close throws an exception.
     */
    public void close()
    {
        running = false;
        CloseHelper.closeAll(replayMerge, subscription);
    }

    private int pollReplayMerge()
    {
        final int fragments = replayMerge.poll(fragmentHandler, BridgeConfiguration.FRAGMENT_COUNT_LIMIT);

        if (replayMerge.isMerged())
        {
            merged = true;
        }
        else if (replayMerge.hasFailed())
        {
            throw new IllegalStateException("ReplayMerge failed");
        }

        return fragments;
    }

    private int pollLive()
    {
        return subscription.poll(fragmentHandler, BridgeConfiguration.FRAGMENT_COUNT_LIMIT);
    }

    private FragmentHandler buildFragmentHandler()
    {
        return (buffer, offset, length, header) ->
        {
            if (!BridgeMessageCodec.isValid(buffer, offset, length))
            {
                return; // Not a bridge message — skip silently
            }

            final long sequence = BridgeMessageCodec.sequence(buffer, offset);

            // Idempotency check: skip already-applied messages (critical for replay)
            if (sequence <= checkpoint.lastAppliedSequence())
            {
                return;
            }

            final int payloadLength = BridgeMessageCodec.payloadLength(buffer, offset);

            // Bounds check: ensure the declared payload fits within the fragment.
            // Protects against corrupted payloadLength causing ArrayIndexOutOfBoundsException.
            if (payloadLength < 0 || payloadLength > length - BridgeMessageCodec.HEADER_LENGTH)
            {
                return;
            }

            final int payloadOffset = BridgeMessageCodec.payloadOffset(offset);
            final byte[] payloadBytes = new byte[payloadLength];
            buffer.getBytes(payloadOffset, payloadBytes);

            // Delegate to application handler (OCP)
            messageHandler.accept(sequence, payloadBytes);
            receivedCount++;

            // Checkpoint after each applied message for minimal replay window.
            // In replay mode, get position from ReplayMerge's image.
            // In live mode, get position from the subscription's first image.
            final long archivePos = resolveArchivePosition();

            try
            {
                checkpoint.save(sequence, archivePos);
            }
            catch (final IOException e)
            {
                throw new RuntimeException("Failed to save checkpoint", e);
            }
        };
    }

    private long resolveArchivePosition()
    {
        // Prefer ReplayMerge image (during replay/merge)
        if (replayMerge != null)
        {
            final Image image = replayMerge.image();
            if (image != null)
            {
                return image.position();
            }
        }

        // In live mode, use the subscription's first connected image
        if (subscription != null && subscription.imageCount() > 0)
        {
            final Image image = subscription.imageAtIndex(0);
            if (image != null)
            {
                return image.position();
            }
        }

        return 0;
    }

    private long findRecording()
    {
        final MutableLong lastRecordingId = new MutableLong(-1);

        final RecordingDescriptorConsumer consumer =
            (controlSessionId,
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
            recStreamId,
            strippedChannel,
            originalChannel,
            sourceIdentity) -> lastRecordingId.set(recordingId);

        archive.listRecordingsForUri(0L, 100, liveChannel, streamId, consumer);

        return lastRecordingId.get();
    }

    private static String extractEndpoint(final String channel)
    {
        final int idx = channel.indexOf("endpoint=");
        if (idx < 0)
        {
            return "localhost:0";
        }

        final int start = idx + "endpoint=".length();
        int end = channel.indexOf('|', start);
        if (end < 0)
        {
            end = channel.indexOf('&', start);
        }
        if (end < 0)
        {
            end = channel.length();
        }

        return channel.substring(start, end);
    }
}
