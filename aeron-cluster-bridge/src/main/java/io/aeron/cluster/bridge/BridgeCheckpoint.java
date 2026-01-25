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

import org.agrona.BufferUtil;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

/**
 * Atomic checkpoint persistence for bridge receivers.
 * <p>
 * <b>Design Rationale (Durability - Atomic Writes):</b>
 * The checkpoint file is 48 bytes, which fits within a single disk sector on most
 * file systems. This ensures that writes are atomic - either the entire checkpoint
 * is written or none of it is. Combined with memory-mapped I/O and force(), this
 * provides strong durability guarantees.
 * <p>
 * <b>Design Rationale (SOLID - Single Responsibility):</b>
 * This class manages only checkpoint state persistence. It does not handle
 * message processing or archive interaction.
 * <p>
 * <b>Design Rationale (SOLID - Open/Closed):</b>
 * The checkpoint format includes a version field, allowing future versions to
 * add fields while maintaining backward compatibility.
 * <p>
 * Checkpoint file layout (48 bytes):
 * <pre>
 * ┌────────────────────────────────────────────────────────┐
 * │              Checkpoint File (Binary)                  │
 * ├──────────┬──────────┬──────────────────────────────────┤
 * │ Offset   │ Type     │ Field                            │
 * ├──────────┼──────────┼──────────────────────────────────┤
 * │ 0        │ int      │ version (1)                      │
 * │ 4        │ int      │ direction (1=ME→RMS, 2=RMS→ME)   │
 * │ 8        │ long     │ lastAppliedSequence              │
 * │ 16       │ long     │ archiveRecordingId               │
 * │ 24       │ long     │ archivePosition                  │
 * │ 32       │ long     │ timestampNanos                   │
 * │ 40       │ long     │ checksum (CRC32)                 │
 * └──────────┴──────────┴──────────────────────────────────┘
 * Total Size: 48 bytes (fits in single sector for atomic write)
 * </pre>
 * <p>
 * <b>Assumptions:</b>
 * <ul>
 *   <li>File system supports memory-mapped I/O</li>
 *   <li>Disk sector size is at least 48 bytes (true for all modern systems)</li>
 *   <li>CRC32 is sufficient for data integrity (not cryptographic security)</li>
 *   <li>One checkpoint file per direction (ME_TO_RMS and RMS_TO_ME)</li>
 * </ul>
 */
public final class BridgeCheckpoint implements AutoCloseable
{
    /** Current checkpoint format version */
    public static final int VERSION = 1;

    /** Total checkpoint size in bytes */
    public static final int CHECKPOINT_SIZE = 48;

    // Field offsets
    private static final int OFFSET_VERSION = 0;
    private static final int OFFSET_DIRECTION = 4;
    private static final int OFFSET_LAST_SEQUENCE = 8;
    private static final int OFFSET_RECORDING_ID = 16;
    private static final int OFFSET_ARCHIVE_POSITION = 24;
    private static final int OFFSET_TIMESTAMP = 32;
    private static final int OFFSET_CHECKSUM = 40;

    private final File checkpointFile;
    private final BridgeConfiguration.Direction direction;
    private final UnsafeBuffer buffer;
    private final MappedByteBuffer mappedBuffer;
    private final CRC32 crc32 = new CRC32();

    // Pre-allocated buffer for checksum computation (avoids allocation in hot path)
    private final byte[] checksumData = new byte[OFFSET_CHECKSUM];

    // Cached state (avoids buffer reads on hot path)
    private long lastAppliedSequence;
    private long archiveRecordingId;
    private long archivePosition;

    /**
     * Create or open a checkpoint file.
     * <p>
     * <b>Behavior:</b>
     * <ul>
     *   <li>If file exists and is valid: loads existing state</li>
     *   <li>If file exists but checksum fails: throws IOException</li>
     *   <li>If file doesn't exist: creates new checkpoint with initial state</li>
     * </ul>
     *
     * @param checkpointDir the directory to store checkpoint files
     * @param direction     the bridge direction (determines filename)
     * @throws IOException if file operations fail or checksum is invalid
     */
    public BridgeCheckpoint(final String checkpointDir, final BridgeConfiguration.Direction direction)
        throws IOException
    {
        this.direction = direction;

        // Ensure directory exists
        final File dir = new File(checkpointDir);
        if (!dir.exists() && !dir.mkdirs())
        {
            throw new IOException("Failed to create checkpoint directory: " + checkpointDir);
        }

        // Checkpoint filename based on direction
        final String fileName = direction.name().toLowerCase() + ".checkpoint";
        this.checkpointFile = new File(dir, fileName);

        final boolean exists = checkpointFile.exists();

        // Memory-map the file for atomic operations
        try (RandomAccessFile raf = new RandomAccessFile(checkpointFile, "rw");
            FileChannel channel = raf.getChannel())
        {
            this.mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, CHECKPOINT_SIZE);
        }

        this.buffer = new UnsafeBuffer(mappedBuffer);

        if (exists && buffer.getInt(OFFSET_VERSION) == VERSION)
        {
            // Validate existing checkpoint
            if (!verifyChecksum())
            {
                throw new IOException("Checkpoint file checksum mismatch: " + checkpointFile +
                    ". File may be corrupted. Delete the file to start fresh.");
            }

            // Load state from checkpoint
            this.lastAppliedSequence = buffer.getLong(OFFSET_LAST_SEQUENCE);
            this.archiveRecordingId = buffer.getLong(OFFSET_RECORDING_ID);
            this.archivePosition = buffer.getLong(OFFSET_ARCHIVE_POSITION);

            System.out.println("[Checkpoint] Loaded existing checkpoint: " +
                "seq=" + lastAppliedSequence +
                ", recordingId=" + archiveRecordingId +
                ", pos=" + archivePosition);
        }
        else
        {
            // Initialize new checkpoint with zero state
            this.lastAppliedSequence = 0;
            this.archiveRecordingId = -1;
            this.archivePosition = 0;
            persist();
            System.out.println("[Checkpoint] Created new checkpoint file: " + checkpointFile);
        }
    }

    /**
     * Get the last applied sequence number.
     * <p>
     * <b>Design Note:</b> Returns cached value to avoid buffer read latency.
     *
     * @return the last successfully processed sequence number
     */
    public long lastAppliedSequence()
    {
        return lastAppliedSequence;
    }

    /**
     * Get the archive recording ID associated with this checkpoint.
     *
     * @return the recording ID, or -1 if not set
     */
    public long archiveRecordingId()
    {
        return archiveRecordingId;
    }

    /**
     * Get the archive position for replay.
     *
     * @return the archive position in bytes
     */
    public long archivePosition()
    {
        return archivePosition;
    }

    /**
     * Update the checkpoint with new values and persist to disk.
     * <p>
     * <b>Atomicity:</b> All fields are written, then force() is called.
     * The 48-byte checkpoint fits in a single disk sector, ensuring atomic update.
     *
     * @param sequence    the new last applied sequence number
     * @param recordingId the archive recording ID
     * @param position    the archive position
     */
    public void update(final long sequence, final long recordingId, final long position)
    {
        this.lastAppliedSequence = sequence;
        this.archiveRecordingId = recordingId;
        this.archivePosition = position;
        persist();
    }

    /**
     * Update only the sequence number (for performance when position hasn't changed).
     *
     * @param sequence the new last applied sequence number
     */
    public void updateSequence(final long sequence)
    {
        this.lastAppliedSequence = sequence;
        persist();
    }

    /**
     * Persist the current state to disk.
     * <p>
     * <b>Implementation:</b>
     * <ol>
     *   <li>Write all fields to memory-mapped buffer</li>
     *   <li>Compute CRC32 checksum over data fields</li>
     *   <li>Write checksum</li>
     *   <li>Force buffer to disk (fsync)</li>
     * </ol>
     */
    private void persist()
    {
        buffer.putInt(OFFSET_VERSION, VERSION);
        buffer.putInt(OFFSET_DIRECTION, direction.code());
        buffer.putLong(OFFSET_LAST_SEQUENCE, lastAppliedSequence);
        buffer.putLong(OFFSET_RECORDING_ID, archiveRecordingId);
        buffer.putLong(OFFSET_ARCHIVE_POSITION, archivePosition);
        buffer.putLong(OFFSET_TIMESTAMP, System.nanoTime());

        // Compute and write checksum
        final long checksum = computeChecksum();
        buffer.putLong(OFFSET_CHECKSUM, checksum);

        // Force write to disk for durability
        mappedBuffer.force();
    }

    /**
     * Compute CRC32 checksum over data fields (excludes checksum field itself).
     * <p>
     * <b>Performance Note:</b> Uses pre-allocated byte array to avoid allocation.
     */
    private long computeChecksum()
    {
        crc32.reset();
        buffer.getBytes(0, checksumData);
        crc32.update(checksumData);
        return crc32.getValue();
    }

    /**
     * Verify the stored checksum matches computed checksum.
     *
     * @return true if checksum is valid
     */
    private boolean verifyChecksum()
    {
        final long stored = buffer.getLong(OFFSET_CHECKSUM);
        final long computed = computeChecksum();
        return stored == computed;
    }

    /**
     * Close the checkpoint, persisting final state.
     */
    @Override
    public void close()
    {
        persist();
        BufferUtil.free(mappedBuffer);
    }

    /**
     * Delete a checkpoint file (utility for testing).
     *
     * @param checkpointDir the checkpoint directory
     * @param direction     the bridge direction
     */
    public static void delete(final String checkpointDir, final BridgeConfiguration.Direction direction)
    {
        final String fileName = direction.name().toLowerCase() + ".checkpoint";
        final File file = new File(checkpointDir, fileName);
        if (file.exists())
        {
            IoUtil.delete(file, false);
        }
    }

    @Override
    public String toString()
    {
        return "BridgeCheckpoint{" +
            "direction=" + direction +
            ", lastAppliedSequence=" + lastAppliedSequence +
            ", archiveRecordingId=" + archiveRecordingId +
            ", archivePosition=" + archivePosition +
            ", file=" + checkpointFile +
            '}';
    }
}
