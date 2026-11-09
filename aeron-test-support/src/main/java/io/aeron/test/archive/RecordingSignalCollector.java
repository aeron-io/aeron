/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron.test.archive;

import io.aeron.archive.client.RecordingSignalConsumer;
import io.aeron.archive.codecs.RecordingSignal;

import java.util.ArrayList;
import java.util.List;

public final class RecordingSignalCollector implements RecordingSignalConsumer
{
    public record CollectedSignal(
        long controlSessionId,
        long correlationId,
        long recordingId,
        long subscriptionId,
        long position,
        RecordingSignal signal)
    {
    }

    private final List<CollectedSignal> collectedSignals = new ArrayList<>();

    @Override
    public void onSignal(
        final long controlSessionId,
        final long correlationId,
        final long recordingId,
        final long subscriptionId,
        final long position,
        final RecordingSignal signal)
    {
        collectedSignals.add(new CollectedSignal(
            controlSessionId, correlationId, recordingId, subscriptionId, position, signal));
    }

    public List<CollectedSignal> collectedSignals()
    {
        return collectedSignals;
    }

    @Override
    public String toString()
    {
        return "RecordingSignalCollector{" +
            "collectedSignals=" + collectedSignals +
            '}';
    }
}
