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

import io.aeron.Aeron;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.status.CountersReader;
import org.agrona.concurrent.status.StatusIndicator;
import org.agrona.concurrent.status.StatusIndicatorReader;
import org.agrona.concurrent.status.UnsafeBufferStatusIndicator;

/**
 * Functions for working with status counters.
 */
public final class StatusUtil
{
    private StatusUtil()
    {
    }

    /**
     * Return the controllable idle strategy {@link StatusIndicator}.
     *
     * @param countersReader that holds the status indicator.
     * @return status indicator to use or null if not found.
     */
    public static StatusIndicator controllableIdleStrategy(final CountersReader countersReader)
    {
        StatusIndicator statusIndicator = null;
        final MutableInteger id = new MutableInteger(-1);

        countersReader.forEach(
            (counterId, label) ->
            {
                if (counterId == SystemCounterDescriptor.CONTROLLABLE_IDLE_STRATEGY.id() &&
                    label.equals(SystemCounterDescriptor.CONTROLLABLE_IDLE_STRATEGY.label()))
                {
                    id.value = counterId;
                }
            });

        if (Aeron.NULL_VALUE != id.value)
        {
            statusIndicator = new UnsafeBufferStatusIndicator(countersReader.valuesBuffer(), id.value);
        }

        return statusIndicator;
    }

    /**
     * Return the read-only status indicator for the given send channel URI.
     *
     * @param countersReader that holds the status indicator.
     * @param channel        for the send channel.
     * @return read-only status indicator that can be used to query the status of the send channel or null.
     * @see ChannelEndpointStatus for status values and indications.
     */
    public static StatusIndicatorReader sendChannelStatus(final CountersReader countersReader, final String channel)
    {
        StatusIndicatorReader statusReader = null;
        final MutableInteger id = new MutableInteger(-1);

        countersReader.forEach(
            (counterId, typeId, keyBuffer, label) ->
            {
                if (typeId == SendChannelStatus.SEND_CHANNEL_STATUS_TYPE_ID)
                {
                    if (channel.startsWith(keyBuffer.getStringAscii(ChannelEndpointStatus.CHANNEL_OFFSET)))
                    {
                        id.value = counterId;
                    }
                }
            });

        if (Aeron.NULL_VALUE != id.value)
        {
            statusReader = new UnsafeBufferStatusIndicator(countersReader.valuesBuffer(), id.value);
        }

        return statusReader;
    }

    /**
     * Return the read-only status indicator for the given receive channel URI.
     *
     * @param countersReader that holds the status indicator.
     * @param channel        for the receiving endpoint
     * @return read-only status indicator that can be used to query the status of the receive channel endpoint or null.
     * @see ChannelEndpointStatus for status values and indications.
     */
    public static StatusIndicatorReader receiveChannelStatus(final CountersReader countersReader, final String channel)
    {
        StatusIndicatorReader statusReader = null;
        final MutableInteger id = new MutableInteger(-1);

        countersReader.forEach(
            (counterId, typeId, keyBuffer, label) ->
            {
                if (typeId == ReceiveChannelStatus.RECEIVE_CHANNEL_STATUS_TYPE_ID)
                {
                    if (channel.startsWith(keyBuffer.getStringAscii(ChannelEndpointStatus.CHANNEL_OFFSET)))
                    {
                        id.value = counterId;
                    }
                }
            });

        if (Aeron.NULL_VALUE != id.value)
        {
            statusReader = new UnsafeBufferStatusIndicator(countersReader.valuesBuffer(), id.value);
        }

        return statusReader;
    }
}
