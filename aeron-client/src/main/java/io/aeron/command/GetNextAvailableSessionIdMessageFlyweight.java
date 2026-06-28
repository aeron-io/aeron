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

/**
 * Control message for getting next available session id from the media driver
 * ({@link ControlProtocolEvents#GET_NEXT_AVAILABLE_SESSION_ID}).
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
 *  |                         Stream Id                             |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public final class GetNextAvailableSessionIdMessageFlyweight extends CorrelatedMessageFlyweight
{
    private static final int STREAM_ID_OFFSET = CorrelatedMessageFlyweight.LENGTH;
    /**
     * Length of the header.
     */
    public static final int LENGTH = STREAM_ID_OFFSET + SIZE_OF_INT;

    /**
     * An empty flyweight, which should be wrapped over a buffer.
     *
     * @see #wrap(MutableDirectBuffer, int)
     */
    public GetNextAvailableSessionIdMessageFlyweight()
    {
    }

    /**
     * Wrap the buffer at a given offset for updates.
     *
     * @param buffer to wrap.
     * @param offset at which the message begins.
     * @return this for a fluent API.
     */
    public GetNextAvailableSessionIdMessageFlyweight wrap(final MutableDirectBuffer buffer, final int offset)
    {
        super.wrap(buffer, offset);

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
    public GetNextAvailableSessionIdMessageFlyweight streamId(final int streamId)
    {
        buffer.putInt(offset + STREAM_ID_OFFSET, streamId);

        return this;
    }

    /**
     * Length of the message in bytes. Only valid after the channel is set.
     *
     * @return length of the message in bytes.
     */
    public int length()
    {
        return LENGTH;
    }

    /**
     * Validate buffer length is long enough for message.
     *
     * @param msgTypeId type of message.
     * @param length of message in bytes to validate.
     */
    public void validateLength(final int msgTypeId, final int length)
    {
        if (length < LENGTH)
        {
            throw new ControlProtocolException(
                MALFORMED_COMMAND, "command=" + msgTypeId + " too short: length=" + length);
        }
    }
}
