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
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.UnsafeBufferPosition;

import static io.aeron.Aeron.NULL_VALUE;

/**
 * The highest position the Receiver has observed on a session-channel-stream tuple while rebuilding the stream.
 * It is possible the stream is not complete to this point if the stream has experienced loss.
 */
public final class ReceiverHwm
{
    /**
     * Type id of a receiver high-water-mark position counter.
     */
    public static final int RECEIVER_HWM_TYPE_ID = AeronCounters.DRIVER_RECEIVER_HWM_TYPE_ID;

    /**
     * Human-readable name for the counter.
     */
    public static final String NAME = "rcv-hwm";

    private ReceiverHwm()
    {
    }

    /**
     * Allocate a new receiver high-water-mark position counter for a stream.
     *
     * @param tempBuffer      to build the label.
     * @param countersManager to allocate the counter from.
     * @param clientId        to set as counter owner.
     * @param registrationId  associated with the counter.
     * @param sessionId       associated with the counter.
     * @param streamId        associated with the counter.
     * @param channel         associated with the counter.
     * @return the allocated counter.
     */
    public static UnsafeBufferPosition allocate(
        final MutableDirectBuffer tempBuffer,
        final CountersManager countersManager,
        final long clientId,
        final long registrationId,
        final int sessionId,
        final int streamId,
        final String channel)
    {
        final int counterId = StreamCounter.allocateCounterId(
            tempBuffer,
            NAME,
            RECEIVER_HWM_TYPE_ID,
            countersManager,
            clientId,
            registrationId,
            sessionId,
            streamId,
            channel,
            NULL_VALUE);

        return new UnsafeBufferPosition((UnsafeBuffer)countersManager.valuesBuffer(), counterId, countersManager);
    }
}
