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
 * Indicate a client has timed out by the driver.
 *
 * @see ControlProtocolEvents
 * <pre>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                         Client Id                             |
 * |                                                               |
 * +---------------------------------------------------------------+
 * </pre>
 */
public class ClientTimeoutFlyweight
{
    /**
     * Length of the header.
     */
    public static final int LENGTH = SIZE_OF_LONG;
    private static final int CLIENT_ID_FIELD_OFFSET = 0;

    private MutableDirectBuffer buffer;
    private int offset;

    /**
     * An empty flyweight, which should be wrapped over a buffer.
     *
     * @see #wrap(MutableDirectBuffer, int)
     */
    public ClientTimeoutFlyweight()
    {
    }

    /**
     * Wrap the buffer at a given offset for updates.
     *
     * @param buffer to wrap
     * @param offset at which the message begins.
     * @return this for a fluent API.
     */
    public final ClientTimeoutFlyweight wrap(final MutableDirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;

        return this;
    }

    /**
     * Get client id field.
     *
     * @return client id field.
     */
    public long clientId()
    {
        return buffer.getLong(offset + CLIENT_ID_FIELD_OFFSET);
    }

    /**
     * Set client id field.
     *
     * @param clientId field value.
     * @return this for a fluent API.
     */
    public ClientTimeoutFlyweight clientId(final long clientId)
    {
        buffer.putLong(offset + CLIENT_ID_FIELD_OFFSET, clientId);

        return this;
    }
}
