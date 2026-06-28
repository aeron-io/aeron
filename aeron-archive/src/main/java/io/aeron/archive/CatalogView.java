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

import java.io.File;


import io.aeron.Aeron;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorEncoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderEncoder;

/**
 * Read only view of a {@link Catalog} which can be used for listing entries.
 */
public final class CatalogView
{
    private CatalogView()
    {
    }

    /**
     * List all recording descriptors in a {@link Catalog}.
     *
     * @param archiveDir the directory containing the {@link Catalog}.
     * @param consumer   to which the descriptor are dispatched.
     * @return the count of entries listed.
     */
    public static int listRecordings(final File archiveDir, final RecordingDescriptorConsumer consumer)
    {
        try (Catalog catalog = new Catalog(archiveDir, System::currentTimeMillis))
        {
            return catalog.forEach(new RecordingDescriptorConsumerAdapter(consumer));
        }
    }

    /**
     * List a recording descriptor for a single recording id.
     * <p>
     * If the recording id is not found then nothing is returned.
     *
     * @param archiveDir  the directory containing the {@link Catalog}.
     * @param recordingId to view
     * @param consumer    to which the descriptor are dispatched.
     * @return the true of a descriptor is found.
     */
    public static boolean listRecording(
        final File archiveDir, final long recordingId, final RecordingDescriptorConsumer consumer)
    {
        try (Catalog catalog = new Catalog(archiveDir, System::currentTimeMillis))
        {
            return catalog.forEntry(recordingId, new RecordingDescriptorConsumerAdapter(consumer));
        }
    }

    static class RecordingDescriptorConsumerAdapter implements Catalog.CatalogEntryProcessor
    {
        private final RecordingDescriptorConsumer consumer;

        RecordingDescriptorConsumerAdapter(final RecordingDescriptorConsumer consumer)
        {
            this.consumer = consumer;
        }

        public void accept(
            final int recordingDescriptorOffset,
            final RecordingDescriptorHeaderEncoder headerEncoder,
            final RecordingDescriptorHeaderDecoder headerDecoder,
            final RecordingDescriptorEncoder descriptorEncoder,
            final RecordingDescriptorDecoder descriptorDecoder)
        {
            consumer.onRecordingDescriptor(
                Aeron.NULL_VALUE,
                Aeron.NULL_VALUE,
                descriptorDecoder.recordingId(),
                descriptorDecoder.startTimestamp(),
                descriptorDecoder.stopTimestamp(),
                descriptorDecoder.startPosition(),
                descriptorDecoder.stopPosition(),
                descriptorDecoder.initialTermId(),
                descriptorDecoder.segmentFileLength(),
                descriptorDecoder.termBufferLength(),
                descriptorDecoder.mtuLength(),
                descriptorDecoder.sessionId(),
                descriptorDecoder.streamId(),
                descriptorDecoder.strippedChannel(),
                descriptorDecoder.originalChannel(),
                descriptorDecoder.sourceIdentity());
        }
    }
}
