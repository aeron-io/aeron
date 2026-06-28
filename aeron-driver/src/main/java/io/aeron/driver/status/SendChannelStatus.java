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
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

/**
 * The status of a sending channel endpoint represented as a counter value.
 */
public final class SendChannelStatus
{
    /**
     * Type id of a sending channel status indicator.
     */
    public static final int SEND_CHANNEL_STATUS_TYPE_ID = AeronCounters.DRIVER_SEND_CHANNEL_STATUS_TYPE_ID;

    /**
     * Human-readable name for the counter.
     */
    public static final String NAME = "snd-channel";

    private SendChannelStatus()
    {
    }

    /**
     * Allocate a new channel status counter for a stream.
     *
     * @param tempBuffer      to build the label.
     * @param countersManager to allocate the counter from.
     * @param registrationId  associated with the counter.
     * @param channel         associated with the counter.
     * @return the allocated counter.
     */
    public static AtomicCounter allocate(
        final MutableDirectBuffer tempBuffer,
        final CountersManager countersManager,
        final long registrationId,
        final String channel)
    {
        return ChannelEndpointStatus.allocate(
            tempBuffer, NAME, SEND_CHANNEL_STATUS_TYPE_ID, countersManager, registrationId, channel);
    }
}
