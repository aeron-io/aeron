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

import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import org.agrona.concurrent.UnsafeBuffer;

class ListRecordingsForUriSession extends AbstractListRecordingsSession
{
    private final int streamId;
    private final byte[] channelFragment;
    private final RecordingDescriptorDecoder descriptorDecoder;

    ListRecordingsForUriSession(
        final long correlationId,
        final long fromRecordingId,
        final int count,
        final byte[] channelFragment,
        final int streamId,
        final Catalog catalog,
        final ControlSession controlSession,
        final UnsafeBuffer descriptorBuffer,
        final RecordingDescriptorDecoder recordingDescriptorDecoder)
    {
        super(
            correlationId,
            fromRecordingId,
            count,
            catalog,
            controlSession,
            descriptorBuffer);

        this.streamId = streamId;
        this.channelFragment = channelFragment;
        descriptorDecoder = recordingDescriptorDecoder;
    }

    @Override
    boolean acceptDescriptor(final UnsafeBuffer descriptorBuffer)
    {
        descriptorDecoder.wrap(
            descriptorBuffer,
            RecordingDescriptorHeaderDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);

        return streamId == descriptorDecoder.streamId() &&
            Catalog.originalChannelContains(descriptorDecoder, channelFragment);
    }
}
