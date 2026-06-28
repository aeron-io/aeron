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
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Control message for removing a destination for a Publication in multi-destination-cast or a Subscription
 * in multi-destination Subscription.
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
 *  |                    Resource Correlation ID                    |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                   Destination Correlation ID                  |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class DestinationByIdMessageFlyweight extends CorrelatedMessageFlyweight
{
    private static final int RESOURCE_REGISTRATION_ID_OFFSET = CORRELATION_ID_FIELD_OFFSET + SIZE_OF_LONG;
    private static final int DESTINATION_REGISTRATION_ID_OFFSET = RESOURCE_REGISTRATION_ID_OFFSET + SIZE_OF_LONG;

    /**
     * Length of the encoded message in bytes.
     */
    public static final int MESSAGE_LENGTH = LENGTH + (2 * SIZE_OF_LONG);

    /**
     * An empty flyweight, which should be wrapped over a buffer.
     *
     * @see #wrap(MutableDirectBuffer, int)
     */
    public DestinationByIdMessageFlyweight()
    {
    }

    /**
     * Wrap the buffer at a given offset for updates.
     *
     * @param buffer to wrap.
     * @param offset at which the message begins.
     * @return this for a fluent API.
     */
    public DestinationByIdMessageFlyweight wrap(final MutableDirectBuffer buffer, final int offset)
    {
        super.wrap(buffer, offset);

        return this;
    }

    /**
     * Get the registration id used for the resource that the destination has been registered to. Typically, a
     * subscription or publication.
     *
     * @return resource registration id field.
     */
    public long resourceRegistrationId()
    {
        return buffer.getLong(offset + RESOURCE_REGISTRATION_ID_OFFSET);
    }

    /**
     * Set the registration id used for the resource that the destination has been registered to. Typically, a
     * subscription or publication.
     *
     * @param registrationId field value.
     * @return this for a fluent API.
     */
    public DestinationByIdMessageFlyweight resourceRegistrationId(final long registrationId)
    {
        buffer.putLong(offset + RESOURCE_REGISTRATION_ID_OFFSET, registrationId);

        return this;
    }

    /**
     * Returns the registration id for the destination.
     *
     * @return destination registration id.
     */
    public long destinationRegistrationId()
    {
        return buffer.getLong(offset + DESTINATION_REGISTRATION_ID_OFFSET);
    }

    /**
     * Sets the registration id for the destination.
     *
     * @param destinationRegistrationId to reference the destination.
     * @return this for a fluent API.
     */
    public DestinationByIdMessageFlyweight destinationRegistrationId(final long destinationRegistrationId)
    {
        buffer.putLong(offset + DESTINATION_REGISTRATION_ID_OFFSET, destinationRegistrationId);
        return this;
    }

    /**
     * Validate buffer length is long enough for message.
     *
     * @param msgTypeId type of message.
     * @param length of message in bytes to validate.
     */
    public void validateLength(final int msgTypeId, final int length)
    {
        if (length < MESSAGE_LENGTH)
        {
            throw new ControlProtocolException(
                MALFORMED_COMMAND, "command=" + msgTypeId + " too short: length=" + length);
        }
    }
}
