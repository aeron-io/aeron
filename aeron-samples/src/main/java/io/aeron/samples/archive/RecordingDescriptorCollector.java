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
package io.aeron.samples.archive;

import io.aeron.archive.client.RecordingDescriptorConsumer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Sample utility class for collecting {@link RecordingDescriptor}s. This aims to be memory and allocation
 * efficient by pooling instances of the descriptors. The user must decide on the size of the pool at allocation
 * time. If the pool size is set to <code>0</code> then no pooling will occur and all instances will be recreated.
 * This may be desirable if the user wanted to hold onto descriptors over multiple calls to
 * <code>AeronArchive</code>'s list recording methods. If the user wants to have pooling, but needs to retain some
 * instances, then they can call the <code>retain</code> method on the <code>RecordingDescriptor</code>.
 * <br>
 * <br>
 * Typical usage may be something like:
 * <br>
 * <br>
 * <pre>
 * final int pageSize;
 * final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(pageSize);
 * int count;
 * long fromRecordingId = 0;
 * while (0 != (count = aeronArchive.listRecordings(fromRecordingId, pageSize, collector.reset())
 * {
 *     for (final RecordingDescriptor descriptor : collector.descriptors())
 *     {
 *         if (\/* some interesting condition *\/)
 *         {
 *             // Want to hang onto this one...
 *             descriptor.retain();
 *         }
 *     }
 *
 *     fromRecordingId += count;
 * }
 * </pre>
 * <br>
 * <br>
 *
 * @see io.aeron.archive.client.AeronArchive#listRecordings(long, int, RecordingDescriptorConsumer)
 * @see io.aeron.archive.client.AeronArchive#listRecording(long, RecordingDescriptorConsumer)
 * @see io.aeron.archive.client.AeronArchive#listRecordingsForUri(long, int, String, int, RecordingDescriptorConsumer)
 * @see RecordingDescriptor#retain()
 */
public class RecordingDescriptorCollector
{
    private final ArrayList<RecordingDescriptor> descriptors = new ArrayList<>();
    private final Deque<RecordingDescriptor> pool = new ArrayDeque<>();
    private final int poolSize;

    @SuppressWarnings("Convert2Lambda")
    private final RecordingDescriptorConsumer consumer = new RecordingDescriptorConsumer()
    {
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
            RecordingDescriptor recordingDescriptor = pool.pollLast();
            if (null == recordingDescriptor)
            {
                recordingDescriptor = new RecordingDescriptor();
            }

            descriptors.add(recordingDescriptor.set(
                controlSessionId,
                correlationId,
                recordingId,
                startTimestamp,
                stopTimestamp,
                startPosition,
                stopPosition,
                initialTermId,
                segmentFileLength,
                termBufferLength,
                mtuLength,
                sessionId,
                streamId,
                strippedChannel,
                originalChannel,
                sourceIdentity));
        }
    };

    /**
     * Construct the collector with the specified pool size. If the list recordings call returns more descriptors than
     * the size of the pool then this class will still work, but will allocate new instances and leave them to be
     * garbage collected on <code>reset()</code>. Therefore, setting <code>poolSize</code> to <code>0</code> will
     * effectively disable pooling.
     *
     * @param poolSize size of the descriptor pool
     */
    public RecordingDescriptorCollector(final int poolSize)
    {
        this.poolSize = poolSize;
    }

    /**
     * Reset the result list for the collector. Removes all the instances from the collection and returns them the pool
     * if the size of the pool and the retained status of the descriptor allows.
     *
     * @return the consumer to be passed into the <code>AeronArchives</code>'s list recording methods.
     */
    public RecordingDescriptorConsumer reset()
    {
        for (int i = descriptors.size(); -1 < --i;)
        {
            final RecordingDescriptor removed = descriptors.remove(i);
            if (pool.size() < poolSize && !removed.isRetained())
            {
                pool.addLast(removed.reset());
            }
        }

        return consumer;
    }

    /**
     * The results from the list of recording descriptors. This collection is emptied everytime {@link
     * RecordingDescriptorCollector#reset()} is called and refilled on subsequent queries.
     *
     * @return the result list of descriptors.
     */
    public List<RecordingDescriptor> descriptors()
    {
        return descriptors;
    }

    /**
     * The configured pool size.
     *
     * @return number of descriptors that will be pooled.
     */
    public int poolSize()
    {
        return poolSize;
    }
}
