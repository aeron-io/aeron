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

import io.aeron.Aeron;
import org.agrona.collections.LongArrayQueue;

/**
 * Used to track fragmented messages in the log.If upon new election the leader's commit position falls between a
 * fragmented message, the leader will have to replay begin fragments of that fragmented message so {@link LogAdapter}
 * can reassemble the full fragmented message.
 */
public class LogPublisherFragmentedMessageTracker
{
    private final LongArrayQueue fragmentedMessageBounds = new LongArrayQueue(Long.MAX_VALUE);
    private long logAdapterRebuildStartPosition = Aeron.NULL_VALUE;

    LongArrayQueue fragmentedMessageBounds()
    {
        return fragmentedMessageBounds;
    }

    long logAdapterRebuildStartPosition()
    {
        return logAdapterRebuildStartPosition;
    }

    /**
     * This is useful to track fragmented messages in the log.
     *
     * @param maxPayload    of the log publication.
     * @param messageLength of the message.
     * @param startPosition of the message.
     * @param endPosition   of the message.
     */
    public void trackFragmentedMessage(
        final long maxPayload,
        final long messageLength,
        final long startPosition,
        final long endPosition)
    {
        if (maxPayload < messageLength)
        {
            fragmentedMessageBounds.offerLong(endPosition);
            fragmentedMessageBounds.offerLong(startPosition);
        }
    }

    void sweepUncommittedEntriesTo(final long commitPosition)
    {
        while (fragmentedMessageBounds.peekLong() <= commitPosition)
        {
            fragmentedMessageBounds.pollLong();
            fragmentedMessageBounds.pollLong();
        }
    }

    void storePositionToRebuildLogAdapter(final long commitPosition)
    {
        sweepUncommittedEntriesTo(commitPosition);
        if (!fragmentedMessageBounds.isEmpty())
        {
            final long fragmentedMessageEndPosition = fragmentedMessageBounds.pollLong();
            final long fragmentedMessageStartPosition = fragmentedMessageBounds.pollLong();
            if (fragmentedMessageStartPosition < commitPosition && commitPosition < fragmentedMessageEndPosition)
            {
                logAdapterRebuildStartPosition = fragmentedMessageStartPosition;
            }
        }
        fragmentedMessageBounds.clear();
    }

    void onLogReplay(final long replayedPosition, final long commitPosition)
    {
        if (Aeron.NULL_VALUE != logAdapterRebuildStartPosition)
        {
            if (replayedPosition > logAdapterRebuildStartPosition)
            {
                logAdapterRebuildStartPosition = replayedPosition;
            }

            if (logAdapterRebuildStartPosition >= commitPosition)
            {
                logAdapterRebuildStartPosition = Aeron.NULL_VALUE;
            }
        }
    }

    void onLogReplayComplete()
    {
        logAdapterRebuildStartPosition = Aeron.NULL_VALUE;
    }
}
