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
package io.aeron.archive;

/**
 * Summary of the fixed length fields from a {@link io.aeron.archive.client.RecordingDescriptorConsumer}.
 */
public class RecordingSummary
{
    /**
     * Unique identity of a recording.
     */
    public long recordingId;

    /**
     * Start position of a recording captured by the archive.
     */
    public long startPosition;

    /**
     * Stop position of a recording. This can be {@link io.aeron.archive.client.AeronArchive#NULL_POSITION} if active.
     */
    public long stopPosition;

    /**
     * The initial-term-id for the recorded stream.
     */
    public int initialTermId;

    /**
     * Length of the segment files for the stream which are a multiple of term-length.
     */
    public int segmentFileLength;

    /**
     * The term-length for the recorded stream.
     */
    public int termBufferLength;

    /**
     * UDP datagram length, or MTU, of the recording. Beyond this value messages are fragmented.
     */
    public int mtuLength;

    /**
     * The session-id of the recorded stream which is updated if the recording is extended.
     */
    public int sessionId;

    /**
     * The stream-id of the recorded stream.
     */
    public int streamId;

    /**
     * Default constructor.
     */
    public RecordingSummary()
    {
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "RecordingSummary{" +
            "recordingId=" + recordingId +
            ", startPosition=" + startPosition +
            ", stopPosition=" + stopPosition +
            ", initialTermId=" + initialTermId +
            ", segmentFileLength=" + segmentFileLength +
            ", termBufferLength=" + termBufferLength +
            ", mtuLength=" + mtuLength +
            ", sessionId=" + sessionId +
            ", streamId=" + streamId +
            '}';
    }
}
