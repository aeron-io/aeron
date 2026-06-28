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
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import static io.aeron.ErrorCode.MALFORMED_COMMAND;
import static org.agrona.BitUtil.*;

/**
 * Message to get or create a new static counter.
 * <p>
 * <b>Note:</b> Layout should be SBE 2.0 compliant so that the label length is aligned.
 *
 * @see ControlProtocolEvents
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
 *  |                        Registration ID                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        Counter Type ID                        |
 *  +---------------------------------------------------------------+
 *  |                           Key Length                          |
 *  +---------------------------------------------------------------+
 *  |                           Key Buffer                         ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                          Label Length                         |
 *  +---------------------------------------------------------------+
 *  |                          Label (ASCII)                       ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class StaticCounterMessageFlyweight extends CorrelatedMessageFlyweight
{
    private static final int REGISTRATION_ID_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + SIZE_OF_LONG;
    private static final int COUNTER_TYPE_ID_FIELD_OFFSET = REGISTRATION_ID_FIELD_OFFSET + SIZE_OF_LONG;
    private static final int KEY_LENGTH_OFFSET = COUNTER_TYPE_ID_FIELD_OFFSET + SIZE_OF_INT;
    static final int KEY_BUFFER_OFFSET = KEY_LENGTH_OFFSET + SIZE_OF_INT;
    private static final int MINIMUM_LENGTH = KEY_BUFFER_OFFSET + SIZE_OF_INT;

    /**
     * An empty flyweight, which should be wrapped over a buffer.
     *
     * @see #wrap(MutableDirectBuffer, int)
     */
    public StaticCounterMessageFlyweight()
    {
    }

    /**
     * Wrap the buffer at a given offset for updates.
     *
     * @param buffer to wrap.
     * @param offset at which the message begins.
     * @return this for a fluent API.
     */
    public StaticCounterMessageFlyweight wrap(final MutableDirectBuffer buffer, final int offset)
    {
        super.wrap(buffer, offset);

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
     * Set counter registration id field.
     *
     * @param registrationId field value.
     * @return this for a fluent API.
     */
    public StaticCounterMessageFlyweight registrationId(final long registrationId)
    {
        buffer.putLong(offset + REGISTRATION_ID_FIELD_OFFSET, registrationId);
        return this;
    }

    /**
     * Get type id field.
     *
     * @return type id field.
     */
    public int typeId()
    {
        return buffer.getInt(offset + COUNTER_TYPE_ID_FIELD_OFFSET);
    }

    /**
     * Set counter type id field.
     *
     * @param typeId field value.
     * @return this for a fluent API.
     */
    public StaticCounterMessageFlyweight typeId(final int typeId)
    {
        buffer.putInt(offset + COUNTER_TYPE_ID_FIELD_OFFSET, typeId);
        return this;
    }

    /**
     * Relative offset of the key buffer.
     *
     * @return relative offset of the key buffer.
     */
    public int keyBufferOffset()
    {
        return KEY_BUFFER_OFFSET;
    }

    /**
     * Length of the key buffer in bytes.
     *
     * @return length of key buffer in bytes.
     */
    public int keyBufferLength()
    {
        return buffer.getInt(offset + KEY_LENGTH_OFFSET);
    }

    /**
     * Fill the key buffer.
     *
     * @param keyBuffer containing the optional key for the counter.
     * @param keyOffset within the keyBuffer at which the key begins.
     * @param keyLength of the key in the keyBuffer.
     * @return this for a fluent API.
     */
    public StaticCounterMessageFlyweight keyBuffer(
        final DirectBuffer keyBuffer, final int keyOffset, final int keyLength)
    {
        buffer.putInt(offset + KEY_LENGTH_OFFSET, keyLength);
        if (null != keyBuffer && keyLength > 0)
        {
            buffer.putBytes(offset + KEY_BUFFER_OFFSET, keyBuffer, keyOffset, keyLength);
        }

        return this;
    }

    /**
     * Relative offset of label buffer.
     *
     * @return relative offset of label buffer.
     */
    public int labelBufferOffset()
    {
        return labelLengthOffset() + SIZE_OF_INT;
    }

    /**
     * Length of label buffer in bytes.
     *
     * @return length of label buffer in bytes.
     */
    public int labelBufferLength()
    {
        return buffer.getInt(offset + labelLengthOffset());
    }

    /**
     * Fill the label buffer.
     *
     * @param labelBuffer containing the mandatory label for the counter.
     * @param labelOffset within the labelBuffer at which the label begins.
     * @param labelLength of the label in the labelBuffer.
     * @return this for a fluent API.
     */
    public StaticCounterMessageFlyweight labelBuffer(
        final DirectBuffer labelBuffer, final int labelOffset, final int labelLength)
    {
        final int labelLengthOffset = labelLengthOffset();
        buffer.putInt(offset + labelLengthOffset, labelLength);
        if (null != labelBuffer && labelLength > 0)
        {
            buffer.putBytes(offset + labelLengthOffset + SIZE_OF_INT, labelBuffer, labelOffset, labelLength);
        }

        return this;
    }

    /**
     * Fill the label.
     *
     * @param label for the counter.
     * @return this for a fluent API.
     */
    public StaticCounterMessageFlyweight label(final String label)
    {
        buffer.putStringAscii(offset + labelLengthOffset(), label);

        return this;
    }

    /**
     * Get the length of the current message.
     * <p>
     * NB: must be called after the data is written in order to be accurate.
     *
     * @return the length of the current message
     */
    public int length()
    {
        final int labelOffset = labelLengthOffset();
        return labelOffset + SIZE_OF_INT + labelBufferLength();
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

        final int labelOffset = labelLengthOffset();
        if ((length - labelOffset) < SIZE_OF_INT)
        {
            throw new ControlProtocolException(
                MALFORMED_COMMAND, "command=" + msgTypeId + " too short for key: length=" + length);
        }

        final int encodedLength = length();
        if (length < encodedLength)
        {
            throw new ControlProtocolException(
                MALFORMED_COMMAND,
                "command=" + msgTypeId + " too short for label: length=" + length + " encodedLength=" + encodedLength);
        }
    }

    /**
     * Compute the length of the command message given key and label length.
     *
     * @param keyLength   to be appended.
     * @param labelLength to be appended.
     * @return the length of the command message given key and label length.
     */
    public static int computeLength(final int keyLength, final int labelLength)
    {
        return MINIMUM_LENGTH + align(keyLength, SIZE_OF_INT) + SIZE_OF_INT + labelLength;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "StaticCounterMessageFlyweight{" +
            "clientId=" + clientId() +
            ", correlationId=" + correlationId() +
            ", registrationId=" + registrationId() +
            ", typeId=" + typeId() +
            ", keyBufferOffset=" + keyBufferOffset() +
            ", keyBufferLength=" + keyBufferLength() +
            ", labelLengthOffset=" + labelLengthOffset() +
            ", labelBufferOffset=" + labelBufferOffset() +
            ", labelBufferLength=" + labelBufferLength() +
            ", length=" + length() +
            "}";
    }

    private int labelLengthOffset()
    {
        return KEY_BUFFER_OFFSET + align(keyBufferLength(), SIZE_OF_INT);
    }
}
