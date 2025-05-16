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

import org.agrona.MutableDirectBuffer;

import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Control message for removing a Publication.
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
 *  |                           Flags                               |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class RemovePublicationFlyweight extends RemoveMessageFlyweight
{
    private static final int FLAGS_FIELD_OFFSET = REGISTRATION_ID_FIELD_OFFSET + SIZE_OF_LONG;

    private static final long FLAG_REVOKE = 0x1;

    /**
     * Wrap the buffer at a given offset for updates.
     *
     * @param buffer to wrap.
     * @param offset at which the message begins.
     * @return this for a fluent API.
     */
    public RemovePublicationFlyweight wrap(final MutableDirectBuffer buffer, final int offset)
    {
        super.wrap(buffer, offset);

        return this;
    }

    /**
     * Length of the message in bytes.
     *
     * @return length of the message in bytes.
     */
    public static int length()
    {
        return RemoveMessageFlyweight.length() + SIZE_OF_LONG;
    }

    /**
     * Whether or not the message contains the flags field.
     *
     * @param messageLength the length of the message.
     * @return true if the flags field can be read.
     */
    public boolean flagsFieldIsValid(final int messageLength)
    {
        return messageLength >= FLAGS_FIELD_OFFSET + SIZE_OF_LONG;
    }

    /**
     * Get the value of the revoke field.
     *
     * @return revoked.
     */
    public boolean revoke()
    {
        return (buffer.getLong(offset + FLAGS_FIELD_OFFSET) & FLAG_REVOKE) > 0;
    }

    /**
     * Whether or not the message contains the set revoke flag.
     *
     * @param messageLength the length of the message.
     * @return true if the flags field is present AND the revoked flag is set.
     */
    public boolean revoke(final int messageLength)
    {
        return flagsFieldIsValid(messageLength) && revoke();
    }

    /**
     * Set the value of the revoke field.
     *
     * @param revoke field value.
     * @return this for a fluent API.
     */
    public RemovePublicationFlyweight revoke(final boolean revoke)
    {
        long flags = buffer.getLong(offset + FLAGS_FIELD_OFFSET);

        if (revoke)
        {
            flags |= FLAG_REVOKE;
        }
        else
        {
            flags &= ~FLAG_REVOKE;
        }

        buffer.putLong(offset + FLAGS_FIELD_OFFSET, flags);

        return this;
    }
}
