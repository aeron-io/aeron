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

import io.aeron.exceptions.ConfigurationException;
import io.aeron.exceptions.ControlProtocolException;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import static io.aeron.ErrorCode.MALFORMED_COMMAND;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Command message flyweight to ask the driver process to terminate.
 *
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                          Client ID                            |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        Correlation ID                         |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                         Token Length                          |
 *  +---------------------------------------------------------------+
 *  |                         Token Buffer                         ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 * @see ControlProtocolEvents
 */
public class TerminateDriverFlyweight extends CorrelatedMessageFlyweight
{
    private static final int TOKEN_LENGTH_OFFSET = CORRELATION_ID_FIELD_OFFSET + SIZE_OF_LONG;
    static final int TOKEN_BUFFER_OFFSET = TOKEN_LENGTH_OFFSET + SIZE_OF_INT;
    private static final int MINIMUM_LENGTH = TOKEN_LENGTH_OFFSET + SIZE_OF_INT;

    /**
     * An empty flyweight, which should be wrapped over a buffer.
     *
     * @see #wrap(MutableDirectBuffer, int)
     */
    public TerminateDriverFlyweight()
    {
    }

    /**
     * Wrap the buffer at a given offset for updates.
     *
     * @param buffer to wrap.
     * @param offset at which the message begins.
     * @return this for a fluent API.
     */
    public TerminateDriverFlyweight wrap(final MutableDirectBuffer buffer, final int offset)
    {
        super.wrap(buffer, offset);
        return this;
    }

    /**
     * Relative offset of the token buffer.
     *
     * @return relative offset of the token buffer.
     */
    public int tokenBufferOffset()
    {
        return TOKEN_BUFFER_OFFSET;
    }

    /**
     * Length of the token buffer in bytes.
     *
     * @return length of token buffer in bytes.
     */
    public int tokenBufferLength()
    {
        return buffer.getInt(offset + TOKEN_LENGTH_OFFSET);
    }

    /**
     * Fill the token buffer.
     *
     * @param tokenBuffer containing the optional token for the request.
     * @param tokenOffset within the tokenBuffer at which the token begins.
     * @param tokenLength of the token in the tokenBuffer.
     * @return this for a fluent API.
     */
    public TerminateDriverFlyweight tokenBuffer(
        final DirectBuffer tokenBuffer, final int tokenOffset, final int tokenLength)
    {
        buffer.putInt(offset + TOKEN_LENGTH_OFFSET, tokenLength);
        if (null != tokenBuffer && tokenLength > 0)
        {
            buffer.putBytes(offset + tokenBufferOffset(), tokenBuffer, tokenOffset, tokenLength);
        }

        return this;
    }

    /**
     * Get the length of the current message.
     * <p>
     * NB: must be called after the data is written in order to be correct.
     *
     * @return the length of the current message
     */
    public int length()
    {
        return TOKEN_BUFFER_OFFSET + tokenBufferLength();
    }

    /**
     * Validate buffer length is long enough for message.
     *
     * @param msgTypeId type of message.
     * @param length    of message in bytes to validate.
     */
    public void validateLength(final int msgTypeId, final int length)
    {
        if (length < MINIMUM_LENGTH)
        {
            throw new ControlProtocolException(
                MALFORMED_COMMAND, "command=" + msgTypeId + " too short: length=" + length);
        }

        if ((length - MINIMUM_LENGTH) < buffer.getInt(offset + TOKEN_LENGTH_OFFSET))
        {
            throw new ControlProtocolException(
                MALFORMED_COMMAND, "command=" + msgTypeId + " too short for token buffer: length=" + length);
        }
    }

    /**
     * Compute the length of the command message for a given token length.
     *
     * @param tokenLength to be appended to the header.
     * @return the length of the command message for a given token length.
     */
    public static int computeLength(final int tokenLength)
    {
        if (tokenLength < 0)
        {
            throw new ConfigurationException("token length must be >= 0: " + tokenLength);
        }

        return LENGTH + SIZE_OF_INT + tokenLength;
    }
}
