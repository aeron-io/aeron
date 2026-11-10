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

import io.aeron.archive.client.RecordingSignalConsumer;
import io.aeron.archive.codecs.RecordingSignal;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;

class TestRecordingSignalConsumer implements RecordingSignalConsumer
{
    final long controlSessionId;
    long correlationId;
    long recordingId;
    long subscriptionId;
    long position;
    RecordingSignal signal;

    TestRecordingSignalConsumer(final long controlSessionId)
    {
        this.controlSessionId = controlSessionId;
    }

    @Override
    public void onSignal(
        final long controlSessionId,
        final long correlationId,
        final long recordingId,
        final long subscriptionId,
        final long position,
        final RecordingSignal signal)
    {
        if (this.controlSessionId != controlSessionId)
        {
            throw new IllegalStateException("unexpected controlSessionId=" + controlSessionId);
        }
        this.correlationId = correlationId;
        this.recordingId = recordingId;
        this.subscriptionId = subscriptionId;
        this.position = position;
        this.signal = signal;
    }

    public void reset()
    {
        correlationId = NULL_VALUE;
        recordingId = NULL_VALUE;
        subscriptionId = NULL_VALUE;
        position = NULL_POSITION;
        signal = null;
    }
}
