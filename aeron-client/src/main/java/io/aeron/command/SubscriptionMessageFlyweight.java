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
package io.aeron.command;

import io.aeron.exceptions.ControlProtocolException;
import org.agrona.MutableDirectBuffer;

import static io.aeron.ErrorCode.MALFORMED_COMMAND;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Control message for adding or removing a subscription.
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                          Client ID                            |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                    Command Correlation ID                     |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                 Registration Correlation ID                   |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                         Stream Id                             |
 *  +---------------------------------------------------------------+
 *  |                       Channel Length                          |
 *  +---------------------------------------------------------------+
 *  |                       Channel (ASCII)                        ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class SubscriptionMessageFlyweight extends CorrelatedMessageFlyweight
{
    private static final int REGISTRATION_CORRELATION_ID_OFFSET = CORRELATION_ID_FIELD_OFFSET + SIZE_OF_LONG;
    private static final int STREAM_ID_OFFSET = REGISTRATION_CORRELATION_ID_OFFSET + SIZE_OF_LONG;
    private static final int CHANNEL_OFFSET = STREAM_ID_OFFSET + SIZE_OF_INT;
    private static final int MINIMUM_LENGTH = CHANNEL_OFFSET + SIZE_OF_INT;

    private int lengthOfChannel;

    /**
     * An empty flyweight, which should be wrapped over a buffer.
     *
     * @see #wrap(MutableDirectBuffer, int)
     */
    public SubscriptionMessageFlyweight()
    {
    }

    /**
     * Wrap the buffer at a given offset for updates.
     *
     * @param buffer to wrap.
     * @param offset at which the message begins.
     * @return this for a fluent API.
     */
    public SubscriptionMessageFlyweight wrap(final MutableDirectBuffer buffer, final int offset)
    {
        super.wrap(buffer, offset);

        return this;
    }

    /**
     * The correlation id used in registration field.
     *
     * @return correlation id field.
     */
    public long registrationCorrelationId()
    {
        return buffer.getLong(offset + REGISTRATION_CORRELATION_ID_OFFSET);
    }

    /**
     * Set the registration correlation id field.
     *
     * @param correlationId field value.
     * @return this for a fluent API.
     */
    public SubscriptionMessageFlyweight registrationCorrelationId(final long correlationId)
    {
        buffer.putLong(offset + REGISTRATION_CORRELATION_ID_OFFSET, correlationId);

        return this;
    }

    /**
     * Get the stream id.
     *
     * @return the stream id.
     */
    public int streamId()
    {
        return buffer.getInt(offset + STREAM_ID_OFFSET);
    }

    /**
     * Set the stream id.
     *
     * @param streamId the channel id.
     * @return this for a fluent API.
     */
    public SubscriptionMessageFlyweight streamId(final int streamId)
    {
        buffer.putInt(offset + STREAM_ID_OFFSET, streamId);

        return this;
    }

    /**
     * Get the channel field in ASCII.
     *
     * @return channel field.
     */
    public String channel()
    {
        return buffer.getStringAscii(offset + CHANNEL_OFFSET);
    }

    /**
     * Append the channel value to an {@link Appendable}.
     *
     * @param appendable to append channel to.
     */
    public void appendChannel(final Appendable appendable)
    {
        buffer.getStringAscii(offset + CHANNEL_OFFSET, appendable);
    }

    /**
     * Set channel field in ASCII.
     *
     * @param channel field value.
     * @return this for a fluent API.
     */
    public SubscriptionMessageFlyweight channel(final String channel)
    {
        lengthOfChannel = buffer.putStringAscii(offset + CHANNEL_OFFSET, channel);

        return this;
    }

    /**
     * Length of the message in bytes. Only valid after the channel is set.
     *
     * @return length of the message in bytes.
     */
    public int length()
    {
        return CHANNEL_OFFSET + lengthOfChannel;
    }

    /**
     * Validate buffer length is long enough for message.
     *
     * @param msgTypeId type of message.
     * @param length of message in bytes to validate.
     */
    public void validateLength(final int msgTypeId, final int length)
    {
        if (length < MINIMUM_LENGTH)
        {
            throw new ControlProtocolException(
                MALFORMED_COMMAND, "command=" + msgTypeId + " too short: length=" + length);
        }

        if ((length - MINIMUM_LENGTH) < buffer.getInt(offset + CHANNEL_OFFSET))
        {
            throw new ControlProtocolException(
                MALFORMED_COMMAND, "command=" + msgTypeId + " too short for channel: length=" + length);
        }
    }

    /**
     * Compute the length of the command message for a given channel length.
     *
     * @param channelLength to be appended to the header.
     * @return the length of the command message for a given channel length.
     */
    public static int computeLength(final int channelLength)
    {
        return MINIMUM_LENGTH + channelLength;
    }
}
