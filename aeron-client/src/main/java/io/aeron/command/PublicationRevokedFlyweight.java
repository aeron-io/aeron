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
 * Indicate a publication has been revoked.
 *
 * @see ControlProtocolEvents
 * <pre>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                      Registration Id                          |
 * |                                                               |
 * +---------------------------------------------------------------+
 * </pre>
 */
public class PublicationRevokedFlyweight
{
    /**
     * Length of the header.
     */
    public static final int LENGTH = SIZE_OF_LONG;
    private static final int REGISTRATION_ID_FIELD_OFFSET = 0;

    private MutableDirectBuffer buffer;
    private int offset;

    /**
     * Wrap the buffer at a given offset for updates.
     *
     * @param buffer to wrap
     * @param offset at which the message begins.
     * @return this for a fluent API.
     */
    public final PublicationRevokedFlyweight wrap(final MutableDirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;

        return this;
    }

    /**
     * Get registration id field.
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
    public PublicationRevokedFlyweight registrationId(final long registrationId)
    {
        buffer.putLong(offset + REGISTRATION_ID_FIELD_OFFSET, registrationId);

        return this;
    }
}
