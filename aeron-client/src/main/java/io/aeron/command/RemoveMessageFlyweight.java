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
 * Control message for removing a Publication or Subscription.
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
 *  |                       Registration ID                         |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public abstract class RemoveMessageFlyweight extends CorrelatedMessageFlyweight
{
    static final int REGISTRATION_ID_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + SIZE_OF_LONG;
    private static final int MINIMUM_LENGTH = REGISTRATION_ID_FIELD_OFFSET + SIZE_OF_LONG;

    /**
     * Wrap the buffer at a given offset for updates.
     *
     * @param buffer to wrap.
     * @param offset at which the message begins.
     * @return this for a fluent API.
     */
    public RemoveMessageFlyweight wrap(final MutableDirectBuffer buffer, final int offset)
    {
        super.wrap(buffer, offset);

        return this;
    }

    /**
     * Get the registration id field.
     *
     * @return registration id field.
     */
    public long registrationId()
    {
        return buffer.getLong(offset + REGISTRATION_ID_FIELD_OFFSET);
    }

    /**
     * Set registration id field.
     *
     * @param registrationId field value.
     * @return this for a fluent API.
     */
    public RemoveMessageFlyweight registrationId(final long registrationId)
    {
        buffer.putLong(offset + REGISTRATION_ID_FIELD_OFFSET, registrationId);

        return this;
    }

    /**
     * Length of the message in bytes.
     *
     * @return length of the message in bytes.
     */
    public static int length()
    {
        return LENGTH + SIZE_OF_LONG;
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
    }
}
