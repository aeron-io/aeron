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
 * Control message to reject an image for a subscription.
 *
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                          Client ID                            |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                       Correlation ID                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                    Image Correlation ID                       |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                           Position                            |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        Reason Length                          |
 *  +---------------------------------------------------------------+
 *  |                        Reason (ASCII)                       ...
 *  ...                                                             |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class RejectImageFlyweight extends CorrelatedMessageFlyweight
{
    private static final int IMAGE_CORRELATION_ID_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + SIZE_OF_LONG;
    private static final int POSITION_FIELD_OFFSET = IMAGE_CORRELATION_ID_FIELD_OFFSET + SIZE_OF_LONG;
    private static final int REASON_FIELD_OFFSET = POSITION_FIELD_OFFSET + SIZE_OF_LONG;
    private static final int MINIMUM_SIZE = REASON_FIELD_OFFSET + SIZE_OF_INT;

    /**
     * An empty flyweight, which should be wrapped over a buffer.
     *
     * @see #wrap(MutableDirectBuffer, int)
     */
    public RejectImageFlyweight()
    {
    }

    /**
     * Wrap the buffer at a given offset for updates.
     *
     * @param buffer to wrap.
     * @param offset at which the message begins.
     * @return this for a fluent API.
     */
    public RejectImageFlyweight wrap(final MutableDirectBuffer buffer, final int offset)
    {
        super.wrap(buffer, offset);
        return this;
    }

    /**
     * Get image correlation id field.
     *
     * @return image correlation id field.
     */
    public long imageCorrelationId()
    {
        return buffer.getLong(offset + IMAGE_CORRELATION_ID_FIELD_OFFSET);
    }

    /**
     * Put image correlation id field.
     *
     * @param position new image correlation id value.
     * @return this for a fluent API.
     */
    public RejectImageFlyweight imageCorrelationId(final long position)
    {
        buffer.putLong(offset + IMAGE_CORRELATION_ID_FIELD_OFFSET, position);
        return this;
    }

    /**
     * Get position field.
     *
     * @return position field.
     */
    public long position()
    {
        return buffer.getLong(offset + POSITION_FIELD_OFFSET);
    }

    /**
     * Put position field.
     *
     * @param position new position value.
     * @return this for a fluent API.
     */
    public RejectImageFlyweight position(final long position)
    {
        buffer.putLong(offset + POSITION_FIELD_OFFSET, position);
        return this;
    }

    /**
     * Put reason field as ASCII. Include the reason length in the message.
     *
     * @param reason for invalidating the image.
     * @return this for a fluent API.
     */
    public RejectImageFlyweight reason(final String reason)
    {
        buffer.putStringAscii(offset + REASON_FIELD_OFFSET, reason);
        return this;
    }


    /**
     * Get reason field as ASCII.
     *
     * @return reason for invalidating the image.
     */
    public String reason()
    {
        return buffer.getStringAscii(offset + REASON_FIELD_OFFSET);
    }

    /**
     * Length of the reason text.
     *
     * @return length of the reason text.
     */
    public int reasonBufferLength()
    {
        // This does make the assumption that the string is stored with the leading 4 bytes representing the length.
        return buffer.getInt(offset + REASON_FIELD_OFFSET);
    }

    /**
     * {@inheritDoc}
     */
    public RejectImageFlyweight clientId(final long clientId)
    {
        super.clientId(clientId);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public RejectImageFlyweight correlationId(final long correlationId)
    {
        super.correlationId(correlationId);
        return this;
    }

    /**
     * Compute the length of the message based on the reason supplied.
     *
     * @param reason message to be return to originator.
     * @return length of the message.
     */
    public static int computeLength(final String reason)
    {
        return MINIMUM_SIZE + reason.length();
    }

    /**
     * Validate buffer length is long enough for message.
     *
     * @param msgTypeId type of message.
     * @param length of message in bytes to validate.
     */
    public void validateLength(final int msgTypeId, final int length)
    {
        if (length < MINIMUM_SIZE)
        {
            throw new ControlProtocolException(
                MALFORMED_COMMAND, "command=" + msgTypeId + " too short: length=" + length);
        }

        if (length < MINIMUM_SIZE + reasonBufferLength())
        {
            throw new ControlProtocolException(
                MALFORMED_COMMAND, "command=" + msgTypeId + " too short: length=" + length);
        }
    }
}
