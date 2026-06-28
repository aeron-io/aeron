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

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Message to denote a response to get next session id command
 * ({@link ControlProtocolEvents#ON_NEXT_AVAILABLE_SESSION_ID}).
 *
 * @see ControlProtocolEvents
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                         Correlation ID                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
*  |                         Next Session ID                        |
 *  +---------------------------------------------------------------+
 * </pre>
 * @since 1.49.0
 */
public class NextAvailableSessionIdFlyweight
{
    private static final int CORRELATION_ID_OFFSET = 0;
    private static final int SESSION_ID_OFFSET = CORRELATION_ID_OFFSET + SIZE_OF_LONG;
    /**
     * Length of the header.
     */
    public static final int LENGTH = SESSION_ID_OFFSET + SIZE_OF_INT;

    private MutableDirectBuffer buffer;
    private int offset;

    /**
     * An empty flyweight, which should be wrapped over a buffer.
     *
     * @see #wrap(MutableDirectBuffer, int)
     */
    public NextAvailableSessionIdFlyweight()
    {
    }

    /**
     * Wrap the buffer at a given offset for updates.
     *
     * @param buffer to wrap
     * @param offset at which the message begins.
     * @return this for a fluent API.
     */
    public final NextAvailableSessionIdFlyweight wrap(final MutableDirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;

        return this;
    }

    /**
     * Get the correlation id field.
     *
     * @return correlation id field.
     */
    public long correlationId()
    {
        return buffer.getLong(offset + CORRELATION_ID_OFFSET);
    }

    /**
     * Set the correlation id field.
     *
     * @param correlationId field value.
     * @return this for a fluent API.
     */
    public NextAvailableSessionIdFlyweight correlationId(final long correlationId)
    {
        buffer.putLong(offset + CORRELATION_ID_OFFSET, correlationId);

        return this;
    }

    /**
     * The session id.
     *
     * @return session id.
     */
    public int nextSessionId()
    {
        return buffer.getInt(offset + SESSION_ID_OFFSET);
    }

    /**
     * Set session id field.
     *
     * @param sessionId field value.
     * @return this for a fluent API.
     */
    public NextAvailableSessionIdFlyweight nextSessionId(final int sessionId)
    {
        buffer.putInt(offset + SESSION_ID_OFFSET, sessionId);

        return this;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "NextSessionIdFlyweight{" +
            "correlationId=" + correlationId() +
            ", sessionId=" + nextSessionId() +
            "}";
    }
}
