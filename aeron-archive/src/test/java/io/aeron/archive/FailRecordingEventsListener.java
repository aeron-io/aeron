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

import io.aeron.archive.client.RecordingEventsListener;

import static org.junit.jupiter.api.Assertions.fail;

class FailRecordingEventsListener implements RecordingEventsListener
{
    @Override
    public void onStart(
        final long recordingId,
        final long startPosition,
        final int sessionId,
        final int streamId,
        final String channel,
        final String sourceIdentity)
    {
        fail("recordingId=" + recordingId + ", startPosition=" + startPosition + ", sessionId=" + sessionId +
            ", streamId=" + streamId + ", channel=" + channel + ", sourceIdentity=" + sourceIdentity);
    }

    @Override
    public void onProgress(final long recordingId, final long startPosition, final long position)
    {
        fail("recordingId=" + recordingId + ", startPosition=" + startPosition + ", position=" + position);
    }

    @Override
    public void onStop(final long recordingId, final long startPosition, final long stopPosition)
    {
        fail("recordingId=" + recordingId + ", startPosition=" + startPosition + ", stopPosition=" + stopPosition);
    }
}
