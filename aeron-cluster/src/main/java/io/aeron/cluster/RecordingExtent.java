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
package io.aeron.cluster;

import io.aeron.archive.client.RecordingDescriptorConsumer;

/**
 * The extent covered by a recording in the archive in terms of position and time.
 *
 * @see io.aeron.archive.client.AeronArchive#listRecording(long, RecordingDescriptorConsumer)
 */
class RecordingExtent implements RecordingDescriptorConsumer
{
    /**
     * Id of the recording.
     */
    public long recordingId;
    /**
     * Start position.
     */
    public long startPosition;
    /**
     * Stop position.
     */
    public long stopPosition;
    /**
     * Initial term id.
     */
    public int initialTermId;
    /**
     * Term buffer length.
     */
    public int termBufferLength;
    /**
     * MTU length.
     */
    public int mtuLength;
    /**
     * Session id.
     */
    public int sessionId;

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
        this.initialTermId = initialTermId;
        this.termBufferLength = termBufferLength;
        this.mtuLength = mtuLength;
        this.sessionId = sessionId;
    }

    public String toString()
    {
        return "RecordingExtent{" +
            "recordingId=" + recordingId +
            ", startPosition=" + startPosition +
            ", stopPosition=" + stopPosition +
            ", initialTermId=" + initialTermId +
            ", termBufferLength=" + termBufferLength +
            ", mtuLength=" + mtuLength +
            ", sessionId=" + sessionId +
            '}';
    }
}
