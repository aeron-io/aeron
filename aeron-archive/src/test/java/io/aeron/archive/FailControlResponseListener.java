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

import io.aeron.Aeron;
import io.aeron.archive.client.ControlResponseListener;
import io.aeron.archive.codecs.ControlResponseCode;

import static org.junit.jupiter.api.Assertions.fail;

class FailControlResponseListener implements ControlResponseListener
{
    @Override
    public void onResponse(
        final long controlSessionId,
        final long correlationId,
        final long relevantId,
        final ControlResponseCode code,
        final String errorMessage)
    {
        if (Aeron.NULL_VALUE != correlationId)
        {
            fail("controlSessionId=" + controlSessionId + ", correlationId=" + correlationId + ", relevantId=" +
                relevantId + ", responseCode=" + code + ", errorMessage=" + errorMessage);
        }
    }

    @Override
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
        fail("controlSessionId=" + controlSessionId +
            ", correlationId=" + correlationId +
            ", recordingId=" + recordingId +
            ", startTimestamp=" + startTimestamp +
            ", stopTimestamp=" + stopTimestamp +
            ", startPosition=" + startPosition +
            ", stopPosition=" + stopPosition +
            ", initialTermId=" + initialTermId +
            ", segmentFileLength=" + segmentFileLength +
            ", termBufferLength=" + termBufferLength +
            ", mtuLength=" + mtuLength +
            ", sessionId=" + sessionId +
            ", streamId=" + streamId +
            ", strippedChannel=" + strippedChannel +
            ", originalChannel=" + originalChannel +
            ", sourceIdentity=" + sourceIdentity);
    }
}
