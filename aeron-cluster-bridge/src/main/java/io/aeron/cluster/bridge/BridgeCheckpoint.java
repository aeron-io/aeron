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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Atomic, file-based checkpoint for the bridge receiver.
 * <p>
 * <b>Design (SRP):</b> Sole responsibility is persisting and loading
 * (direction, lastAppliedSequence, archivePosition) tuples.
 * <p>
 * <b>Atomicity guarantee:</b> Uses write-to-temp + {@code ATOMIC_MOVE}
 * rename, so readers never see a partial checkpoint.
 * <p>
 * <b>Assumption:</b> The filesystem supports atomic rename (true on all
 * modern OS/filesystem combos: ext4, NTFS, APFS). If running on NFS, this
 * guarantee may not hold.
 *
 * <pre>
 * Offset  Size  Field
 * 0       4     magic (0x434B5054 = "CKPT")
 * 4       4     direction
 * 8       8     lastAppliedSequence
 * 16      8     archivePosition
 * </pre>
 */
public final class BridgeCheckpoint
{
    /**
     * Magic bytes: "CKPT".
     */
    public static final int MAGIC = 0x434B_5054;

    /**
     * Size of the checkpoint file in bytes.
     */
    public static final int CHECKPOINT_SIZE = 24;

    private final Path checkpointPath;
    private final Path tempPath;
    private final int direction;
    // Pre-allocated buffer for save() to avoid per-message allocation on the hot path.
    private final ByteBuffer saveBuffer = ByteBuffer.allocate(CHECKPOINT_SIZE);
    private final byte[] saveArray = saveBuffer.array();
    private long lastAppliedSequence;
    private long archivePosition;

    /**
     * Create a checkpoint bound to a specific direction and directory.
     *
     * @param directory the directory for checkpoint files.
     * @param direction the bridge direction (0=ME_TO_RMS, 1=RMS_TO_ME).
     */
    public BridgeCheckpoint(final Path directory, final int direction)
    {
        this.direction = direction;
        final String dirLabel = direction == BridgeConfiguration.DIRECTION_ME_TO_RMS ? "me-to-rms" : "rms-to-me";
        this.checkpointPath = directory.resolve("bridge-checkpoint-" + dirLabel + ".bin");
        this.tempPath = directory.resolve("bridge-checkpoint-" + dirLabel + ".tmp");
        this.lastAppliedSequence = 0;
        this.archivePosition = 0;
    }

    /**
     * Load the checkpoint from disk. If the file does not exist or is
     * corrupt, resets to (sequence=0, position=0).
     *
     * @throws IOException if the file exists but cannot be read.
     */
    public void load() throws IOException
    {
        if (!Files.exists(checkpointPath))
        {
            lastAppliedSequence = 0;
            archivePosition = 0;
            return;
        }

        final byte[] data = Files.readAllBytes(checkpointPath);
        if (data.length < CHECKPOINT_SIZE)
        {
            lastAppliedSequence = 0;
            archivePosition = 0;
            return;
        }

        final ByteBuffer buf = ByteBuffer.wrap(data);
        final int magic = buf.getInt();
        if (magic != MAGIC)
        {
            // Corrupt file — safe to reset (replay will re-deliver)
            lastAppliedSequence = 0;
            archivePosition = 0;
            return;
        }

        buf.getInt(); // direction (stored for diagnostics, not enforced on read)
        lastAppliedSequence = buf.getLong();
        archivePosition = buf.getLong();
    }

    /**
     * Persist the checkpoint atomically (write to temp, rename).
     *
     * @param sequence the last applied sequence number.
     * @param position the archive position corresponding to the sequence.
     * @throws IOException if the file cannot be written.
     */
    public void save(final long sequence, final long position) throws IOException
    {
        this.lastAppliedSequence = sequence;
        this.archivePosition = position;

        // Reuse pre-allocated buffer — avoids ByteBuffer.allocate() per save call.
        saveBuffer.clear();
        saveBuffer.putInt(MAGIC);
        saveBuffer.putInt(direction);
        saveBuffer.putLong(sequence);
        saveBuffer.putLong(position);

        Files.write(tempPath, saveArray);
        Files.move(tempPath, checkpointPath,
            StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * Get the last applied sequence number.
     *
     * @return the last applied sequence, or 0 if no checkpoint exists.
     */
    public long lastAppliedSequence()
    {
        return lastAppliedSequence;
    }

    /**
     * Get the archive position at the last checkpoint.
     *
     * @return the archive position, or 0 if no checkpoint exists.
     */
    public long archivePosition()
    {
        return archivePosition;
    }

    /**
     * Get the path to the checkpoint file.
     *
     * @return the checkpoint file path.
     */
    public Path path()
    {
        return checkpointPath;
    }

    /**
     * Delete the checkpoint file if it exists.
     *
     * @throws IOException if deletion fails.
     */
    public void delete() throws IOException
    {
        Files.deleteIfExists(checkpointPath);
        Files.deleteIfExists(tempPath);
    }
}
