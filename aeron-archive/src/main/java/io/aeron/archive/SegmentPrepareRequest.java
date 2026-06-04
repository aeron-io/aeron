/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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
package io.aeron.archive;

import org.agrona.CloseHelper;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Single-shot request to pre-create the next recording segment file off the recording thread, posted by a
 * {@link RecordingWriter} to a {@link SegmentPreparer} (the {@link ArchiveConductor}). The CAS state machine
 * ({@link #PENDING} to {@link #DONE}/{@link #FAILED}/{@link #CANCELLED}) ensures exactly one side owns cleanup
 * of the file and channel however the roll over and the prepare race.
 */
final class SegmentPrepareRequest
{
    /**
     * Awaiting execution by the preparer.
     */
    static final int PENDING = 0;

    /**
     * Prepared successfully: {@link #channel()} is open on {@link #file()} which is sized to the segment length.
     */
    static final int DONE = 1;

    /**
     * Prepare failed: the preparer has cleaned up; the recording falls back to the inline segment open.
     */
    static final int FAILED = 2;

    /**
     * Cancelled by the recorder before completion: the preparer cleans up when it observes the request.
     */
    static final int CANCELLED = 3;

    private static final AtomicIntegerFieldUpdater<SegmentPrepareRequest> STATE_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(SegmentPrepareRequest.class, "state");

    private final File file;
    private final int segmentLength;
    private volatile int state = PENDING;
    private FileChannel channel; // written before the CAS to DONE, read after observing DONE

    SegmentPrepareRequest(final File file, final int segmentLength)
    {
        this.file = file;
        this.segmentLength = segmentLength;
    }

    File file()
    {
        return file;
    }

    int state()
    {
        return state;
    }

    FileChannel channel()
    {
        return channel;
    }

    /**
     * Recorder side: attempt to cancel before completion.
     *
     * @return true if cancelled, i.e. the preparer owns cleanup when it observes the request; false if the
     * request already reached {@link #DONE} or {@link #FAILED}, in which case the recorder owns cleanup of a
     * {@link #DONE} result.
     */
    boolean cancel()
    {
        return STATE_UPDATER.compareAndSet(this, PENDING, CANCELLED);
    }

    /**
     * Preparer side: create the segment file sized to the segment length and publish the result. A no-op if
     * the request was cancelled before execution. No force here: the open + setLength metadata is made durable
     * by the recording thread's per-block force on the channel and the directory force after the rename.
     *
     * @throws IOException if the prepare failed, after the request is marked {@link #FAILED} and any partial
     * file has been cleaned up.
     */
    void execute() throws IOException
    {
        if (PENDING != state)
        {
            return; // canceled before execution; no file was created
        }

        RandomAccessFile recordingFile = null;
        try
        {
            // A stale prepared file left by a crashed prior instance may hold arbitrary bytes, and catalog
            // recovery scans rely on the unwritten tail of a segment reading as zeroes -- always start from
            // a fresh (sparse, all-zero) file.
            if (file.exists() && !file.delete())
            {
                throw new IOException("failed to delete stale prepared segment: " + file);
            }

            recordingFile = new RandomAccessFile(file, "rw");
            recordingFile.setLength(segmentLength);

            channel = recordingFile.getChannel();
            if (!STATE_UPDATER.compareAndSet(this, PENDING, DONE))
            {
                // the recorder canceled (rolled over or closed) while the prepare was in flight
                channel = null;
                CloseHelper.quietClose(recordingFile);
                deleteFile();
            }
        }
        catch (final IOException ex)
        {
            CloseHelper.quietClose(recordingFile);
            STATE_UPDATER.compareAndSet(this, PENDING, FAILED);
            if (!file.delete() && file.exists())
            {
                ex.addSuppressed(new IOException("failed to delete prepared segment: " + file));
            }
            throw ex;
        }
    }

    private void deleteFile() throws IOException
    {
        if (!file.delete() && file.exists())
        {
            throw new IOException("failed to delete prepared segment: " + file);
        }
    }
}
