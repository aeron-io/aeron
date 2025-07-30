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
package io.aeron.status;

import io.aeron.Aeron;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.concurrent.status.CountersReader.MAX_LABEL_LENGTH;

/**
 * Status of the Aeron media channel for a {@link io.aeron.Publication} or {@link io.aeron.Subscription}.
 */
public class ChannelEndpointStatus
{
    /**
     * Channel is being initialized.
     */
    public static final long INITIALIZING = 0;

    /**
     * Channel has errored. Check error log for information.
     */
    public static final long ERRORED = -1;

    /**
     * Channel has finished initialization successfully and is active.
     */
    public static final long ACTIVE = 1;

    /**
     * Channel is being closed.
     */
    public static final long CLOSING = 2;

    /**
     * No counter ID is allocated yet.
     */
    public static final int NO_ID_ALLOCATED = Aeron.NULL_VALUE;

    /**
     * Offset in the key metadata for the channel of the counter.
     */
    public static final int CHANNEL_OFFSET = 0;

    /**
     * String representation of the channel status.
     *
     * @param status to be converted.
     * @return representation of the channel status.
     */
    public static String status(final long status)
    {
        if (INITIALIZING == status)
        {
            return "INITIALIZING";
        }

        if (ERRORED == status)
        {
            return "ERRORED";
        }

        if (ACTIVE == status)
        {
            return "ACTIVE";
        }

        if (CLOSING == status)
        {
            return "CLOSING";
        }

        return "unknown id=" + status;
    }

    /**
     * The maximum length in bytes of the encoded channel identity.
     */
    public static final int MAX_CHANNEL_LENGTH = CountersReader.MAX_KEY_LENGTH - (CHANNEL_OFFSET + SIZE_OF_INT);

    /**
     * Allocate an indicator for tracking the status of a channel endpoint.
     *
     * @param tempBuffer      to be used for labels and metadata.
     * @param name            of the counter for the label.
     * @param typeId          of the counter for classification.
     * @param countersManager from which the underlying storage is allocated.
     * @param registrationId  of the action the counter is associated with.
     * @param channel         for the stream of messages.
     * @return a new {@link AtomicCounter} for tracking the status.
     */
    public static AtomicCounter allocate(
        final MutableDirectBuffer tempBuffer,
        final String name,
        final int typeId,
        final CountersManager countersManager,
        final long registrationId,
        final String channel)
    {
        final int channelLength = tempBuffer.putStringWithoutLengthAscii(
            CHANNEL_OFFSET + SIZE_OF_INT, channel, 0, MAX_CHANNEL_LENGTH);
        tempBuffer.putInt(CHANNEL_OFFSET, channelLength);

        final int keyLength = channelLength + SIZE_OF_INT;

        int labelLength = 0;
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, name);
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, ": ");
        labelLength += tempBuffer.putStringWithoutLengthAscii(
            keyLength + labelLength, channel, 0, MAX_LABEL_LENGTH - labelLength);

        if (labelLength < MAX_LABEL_LENGTH)
        {
            tempBuffer.putByte(keyLength + labelLength, (byte)' ');
            labelLength += 1;
        }

        final AtomicCounter counter = countersManager.newCounter(
            typeId, tempBuffer, 0, keyLength, tempBuffer, keyLength, labelLength);

        countersManager.setCounterRegistrationId(counter.id(), registrationId);

        return counter;
    }
}
