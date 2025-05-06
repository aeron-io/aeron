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
package io.aeron.protocol;

import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;

/**
 * Flyweight for Data Frame header of a message fragment.
 * <p>
 * <a target="_blank"
 *    href="https://github.com/aeron-io/aeron/wiki/Transport-Protocol-Specification#data-frame">Data Frame</a>
 * wiki page.
 */
public class DataHeaderFlyweight extends HeaderFlyweight
{
    /**
     * Length of the Data Header.
     */
    public static final int HEADER_LENGTH = 32;

    /**
     * (B) - Fragment that Begins a message Flag.
     */
    public static final short BEGIN_FLAG = 0x80;

    /**
     * (E) - Fragment that Ends a message Flag.
     */
    public static final short END_FLAG = 0x40;

    /**
     * Begin and End Flags.
     */
    public static final short BEGIN_AND_END_FLAGS = BEGIN_FLAG | END_FLAG;

    /**
     * (S) - End of Stream (EOS) Flag for heartbeats after the publication is closed.
     */
    public static final short EOS_FLAG = 0x20;

    /**
     * Begin, End, and EOS Flags.
     */
    public static final short BEGIN_END_AND_EOS_FLAGS = BEGIN_FLAG | END_FLAG | EOS_FLAG;

    /**
     * Default value to be placed in the reserved value field.
     */
    public static final long DEFAULT_RESERVE_VALUE = 0L;

    /**
     * Offset in the frame at which the term-offset field begins.
     */
    public static final int TERM_OFFSET_FIELD_OFFSET = 8;

    /**
     * Offset in the frame at which the session-id field begins.
     */
    public static final int SESSION_ID_FIELD_OFFSET = 12;

    /**
     * Offset in the frame at which the stream-id field begins.
     */
    public static final int STREAM_ID_FIELD_OFFSET = 16;

    /**
     * Offset in the frame at which the term-id field begins.
     */
    public static final int TERM_ID_FIELD_OFFSET = 20;

    /**
     * Offset in the frame at which the reserved value field begins.
     */
    public static final int RESERVED_VALUE_OFFSET = 24;

    /**
     * Offset in the frame at which the data payload begins.
     */
    public static final int DATA_OFFSET = HEADER_LENGTH;

    /**
     * Default constructor which can later be used to wrap a frame.
     */
    public DataHeaderFlyweight()
    {
    }

    /**
     * Construct the flyweight over a frame.
     *
     * @param buffer containing the frame.
     */
    public DataHeaderFlyweight(final UnsafeBuffer buffer)
    {
        super(buffer);
    }

    /**
     * Construct the flyweight over a frame.
     *
     * @param buffer containing the frame.
     */
    public DataHeaderFlyweight(final ByteBuffer buffer)
    {
        super(buffer);
    }

    /**
     * Get the fragment length field from the header.
     *
     * @param termBuffer  containing the header.
     * @param frameOffset in the buffer where the header starts.
     * @return the fragment length field from the header.
     */
    public static int fragmentLength(final UnsafeBuffer termBuffer, final int frameOffset)
    {
        return termBuffer.getInt(frameOffset + FRAME_LENGTH_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Is the frame at data frame at the beginning of packet a heartbeat message?
     *
     * @param packet containing the data frame.
     * @param length of the data frame.
     * @return true if a heartbeat otherwise false.
     */
    public static boolean isHeartbeat(final UnsafeBuffer packet, final int length)
    {
        return length == HEADER_LENGTH && packet.getInt(0) == 0;
    }

    /**
     * Does the data frame in the packet have the EOS flag set?
     *
     * @param packet containing the data frame.
     * @return true if the EOS flag is set otherwise false.
     */
    public static boolean isEndOfStream(final UnsafeBuffer packet)
    {
        return (packet.getByte(FLAGS_FIELD_OFFSET) & EOS_FLAG) != 0;
    }

    /**
     * Get the session-id field from the header.
     *
     * @return the session-id field from the header.
     */
    public int sessionId()
    {
        return getInt(SESSION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Get the session-id field from the header.
     *
     * @param termBuffer  containing the header.
     * @param frameOffset in the buffer where the header starts.
     * @return the session-id field from the header.
     */
    public static int sessionId(final UnsafeBuffer termBuffer, final int frameOffset)
    {
        return termBuffer.getInt(frameOffset + SESSION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set the session-id field in the header.
     *
     * @param sessionId value to set.
     * @return this for a fluent API.
     */
    public DataHeaderFlyweight sessionId(final int sessionId)
    {
        putInt(SESSION_ID_FIELD_OFFSET, sessionId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Get the stream-id field from the header.
     *
     * @return the stream-id field from the header.
     */
    public int streamId()
    {
        return getInt(STREAM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Get the stream-id field from the header.
     *
     * @param termBuffer  containing the header.
     * @param frameOffset in the buffer where the header starts.
     * @return the stream-id field from the header.
     */
    public static int streamId(final UnsafeBuffer termBuffer, final int frameOffset)
    {
        return termBuffer.getInt(frameOffset + STREAM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set the stream-id field in the header.
     *
     * @param streamId value to set.
     * @return this for a fluent API.
     */
    public DataHeaderFlyweight streamId(final int streamId)
    {
        putInt(STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Get the term-id field from the header.
     *
     * @return the term-id field from the header.
     */
    public int termId()
    {
        return getInt(TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Get the term-id field from the header.
     *
     * @param termBuffer  containing the header.
     * @param frameOffset in the buffer where the header starts.
     * @return the term-id field from the header.
     */
    public static int termId(final UnsafeBuffer termBuffer, final int frameOffset)
    {
        return termBuffer.getInt(frameOffset + TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set the term-id field in the header.
     *
     * @param termId value to set.
     * @return this for a fluent API.
     */
    public DataHeaderFlyweight termId(final int termId)
    {
        putInt(TERM_ID_FIELD_OFFSET, termId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Get the term-offset field from the header.
     *
     * @return the term-offset field from the header.
     */
    public int termOffset()
    {
        return getInt(TERM_OFFSET_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Get the term-offset field from the header.
     *
     * @param termBuffer  containing the header.
     * @param frameOffset in the buffer where the header starts.
     * @return the term-offset field from the header.
     */
    public static int termOffset(final UnsafeBuffer termBuffer, final int frameOffset)
    {
        return termBuffer.getInt(frameOffset + TERM_OFFSET_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set the term-offset field in the header.
     *
     * @param termOffset value to set.
     * @return this for a fluent API.
     */
    public DataHeaderFlyweight termOffset(final int termOffset)
    {
        putInt(TERM_OFFSET_FIELD_OFFSET, termOffset, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Get the reserved value in LITTLE_ENDIAN format.
     *
     * @return value of the reserved value.
     */
    public long reservedValue()
    {
        return getLong(RESERVED_VALUE_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Get the reserved value field from the header.
     *
     * @param termBuffer  containing the header.
     * @param frameOffset in the buffer where the header starts.
     * @return the reserved value field from the header.
     */
    public static long reservedValue(final UnsafeBuffer termBuffer, final int frameOffset)
    {
        return termBuffer.getLong(frameOffset + RESERVED_VALUE_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set the reserved value in LITTLE_ENDIAN format.
     *
     * @param reservedValue to be stored
     * @return flyweight
     */
    public DataHeaderFlyweight reservedValue(final long reservedValue)
    {
        putLong(RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Return offset in buffer for data.
     *
     * @return offset of data in the buffer.
     */
    public int dataOffset()
    {
        return DATA_OFFSET;
    }

    /**
     * Return an initialised default Data Frame Header.
     *
     * @param sessionId for the header
     * @param streamId  for the header
     * @param termId    for the header
     * @return byte array containing the header
     */
    public static UnsafeBuffer createDefaultHeader(final int sessionId, final int streamId, final int termId)
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(
            BufferUtil.allocateDirectAligned(HEADER_LENGTH, CACHE_LINE_LENGTH));

        buffer.putByte(VERSION_FIELD_OFFSET, CURRENT_VERSION);
        buffer.putByte(FLAGS_FIELD_OFFSET, (byte)BEGIN_AND_END_FLAGS);
        buffer.putShort(TYPE_FIELD_OFFSET, (short)HDR_TYPE_DATA, LITTLE_ENDIAN);
        buffer.putInt(SESSION_ID_FIELD_OFFSET, sessionId, LITTLE_ENDIAN);
        buffer.putInt(STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);
        buffer.putInt(TERM_ID_FIELD_OFFSET, termId, LITTLE_ENDIAN);
        buffer.putLong(RESERVED_VALUE_OFFSET, DEFAULT_RESERVE_VALUE);

        return buffer;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "DATA Header{" +
            "frame-length=" + frameLength() +
            " version=" + version() +
            " flags=" + String.valueOf(flagsToChars(flags())) +
            " type=" + headerType() +
            " term-offset=" + termOffset() +
            " session-id=" + sessionId() +
            " stream-id=" + streamId() +
            " term-id=" + termId() +
            " reserved-value=" + reservedValue() +
            "}";
    }
}
