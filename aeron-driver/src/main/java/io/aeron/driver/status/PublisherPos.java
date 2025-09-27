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
package io.aeron.driver.status;

import io.aeron.AeronCounters;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.UnsafeBufferPosition;

/**
 * The position in bytes a publication has reached appending to the log.
 * <p>
 * <b>Note:</b> This is a not a real-time value like the other and is updated each conductor duty cycle
 *  for monitoring purposes.
 */
public class PublisherPos
{
    /**
     * Type id of a publisher limit counter.
     */
    public static final int PUBLISHER_POS_TYPE_ID = AeronCounters.DRIVER_PUBLISHER_POS_TYPE_ID;

    /**
     * Human-readable name for the counter.
     */
    public static final String NAME = "pub-pos (sampled)";

    /**
     * Allocate a new publication position counter for a stream.
     *
     * @param tempBuffer      to build the label.
     * @param countersManager to allocate the counter from.
     * @param registrationId  associated with the counter.
     * @param sessionId       associated with the counter.
     * @param streamId        associated with the counter.
     * @param channel         associated with the counter.
     * @return the allocated counter.
     */
    public static UnsafeBufferPosition allocate(
        final MutableDirectBuffer tempBuffer,
        final CountersManager countersManager,
        final long registrationId,
        final int sessionId,
        final int streamId,
        final String channel)
    {
        return StreamCounter.allocate(
            tempBuffer, NAME, PUBLISHER_POS_TYPE_ID, countersManager, registrationId, sessionId, streamId, channel);
    }
}
