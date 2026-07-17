/*
 * Copyright 2014-2026 Real Logic Limited.
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

package io.aeron.agent;

import org.agrona.MutableDirectBuffer;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_BYTE;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.BitUtil.SIZE_OF_SHORT;

/**
 * Utility class for CBOR encoding.
 */
public class CborUtil
{
    // Base bytes for major types
    private static final byte TEXT_STRING_TYPE = 0b011_00000;

    static int lengthNumber(final long value)
    {
        final long magnitude = value < 0 ? ~value : value;
        if (magnitude < 24)
        {
            return 1;
        }
        final int numberOfLeadingZeroes = Long.numberOfLeadingZeros(magnitude);
        final int numberOfLeadingBytes = numberOfLeadingZeroes / 8;
        return switch (numberOfLeadingBytes)
        {
            case 0, 1, 2, 3 -> SIZE_OF_LONG + 1;
            case 4, 5 -> SIZE_OF_INT + 1;
            case 6 -> SIZE_OF_SHORT + 1;
            case 7 -> SIZE_OF_BYTE + 1;
            default -> 1;
        };
    }

    static int lengthString(final CharSequence value)
    {
        if (value.length() < 24)
        {
            return 1 + value.length();
        }
        else if (value.length() < (1 << 8))
        {
            return 1 + SIZE_OF_BYTE + value.length();
        }
        else if (value.length() < (1 << 16))
        {
            return 1 + SIZE_OF_SHORT + value.length();
        }
        else
        {
            return 1 + SIZE_OF_INT + value.length();
        }
    }


    /**
     * calculates the total length of the encoded string-long pair.
     *
     * @param key   the key to be encoded.
     * @param value the long value to be encoded.
     * @return the total length of the encoded string-long pair.
     */
    public static int length(final CharSequence key, final long value)
    {
        return lengthString(key) + lengthNumber(value);
    }

    /**
     * @param key   the key to be encoded.
     * @param value the enum value to be encoded.
     * @param <E>   the type of the enum to be encoded.
     * @return the total length of the encoded string-enum pair.
     */
    public static <E extends Enum<E>> int length(final CharSequence key, final E value)
    {
        return length(key, value.name());
    }

    /**
     * calculates the total length of the encoded string-string pair.
     *
     * @param key   the key to be encoded.
     * @param value the value to be encoded.
     * @return the total length of the encoded string-string pair.
     */
    public static int length(final CharSequence key, final CharSequence value)
    {
        return lengthString(key) + lengthString(value);
    }

    private static byte createTypeByte(final byte majorType, final int modifier)
    {
        return (byte)(majorType | modifier);
    }

    private static void encodeNumber(
        final EncodingState encodingState,
        final long value)
    {
        // TODO: handle long (8 byte) case either here or through a new method
        final int offset = encodingState.offset();
        final MutableDirectBuffer buffer = encodingState.buffer();

        byte negativeMask = 0b000_00000;
        final long magnitude;
        if (value < 0)
        {
            // Reference: https://datatracker.ietf.org/doc/html/rfc8949#section-3.1-2.4
            negativeMask = 0b1 << 5;
            magnitude = ~value;
        }
        else
        {
            magnitude = value;
        }

        // Reference: https://datatracker.ietf.org/doc/html/rfc8949#name-specification-of-the-cbor-e
        if (magnitude < 24)
        {
            buffer.putByte(offset, (byte)(magnitude | negativeMask));
            encodingState.incrementOffset(1);
        }
        else
        {
            // Encode based on minimum number of bytes required
            if (magnitude < 256)
            {
                // 1 extra byte case (24)
                buffer.putByte(offset, (byte)(0b000_11000 | negativeMask));
                buffer.putByte(offset + 1, (byte)magnitude);
                encodingState.incrementOffset(1 + SIZE_OF_BYTE);
            }
            else if (magnitude < 65536)
            {
                // 2 extra bytes case (25)
                buffer.putByte(offset, (byte)(0b000_11001 | negativeMask));
                buffer.putShort(offset + 1, (short)magnitude, BIG_ENDIAN);
                encodingState.incrementOffset(1 + SIZE_OF_SHORT);
            }
            else if (magnitude < (1L << 32))
            {
                // 4 extra bytes case (26)
                buffer.putByte(offset, (byte)(0b000_11010 | negativeMask));
                buffer.putInt(offset + 1, (int)magnitude, BIG_ENDIAN);
                encodingState.incrementOffset(1 + SIZE_OF_INT);
            }
            else
            {
                // 8 extra bytes case (27)
                buffer.putByte(offset, (byte)(0b000_11011 | negativeMask));
                buffer.putLong(offset + 1, magnitude, BIG_ENDIAN);
                encodingState.incrementOffset(1 + SIZE_OF_LONG);
            }
        }
    }

    /**
     * encodes a key-value pair of a string and an int.
     *
     * @param encodingState tracks the current state of the encoding.
     * @param key           the key to be encoded.
     * @param value         the value to be encoded.
     */
    public static void encode(
        final EncodingState encodingState,
        final CharSequence key,
        final long value)
    {
        if (encodingState.isReachedLimit())
        {
            return;
        }

        final int length = length(key, value);

        if (encodingState.remaining() < length)
        {
            encodingState.reachedLimit(true);
            return;
        }

        encodeString(encodingState, key);
        encodeNumber(encodingState, value);
    }

    /**
     * encodes a key-value pair of a string and an enum.
     *
     * @param encodingState tracks the current state of the encoding.
     * @param key           the key to be encoded.
     * @param value         the value to be encoded.
     * @param <E>           the type of the enum to be encoded.
     */
    public static <E extends Enum<E>> void encode(
        final EncodingState encodingState,
        final CharSequence key,
        final E value)
    {
        encode(encodingState, key, value.name());
    }

    /**
     * encodes a key-value pair of a string and a string.
     *
     * @param encodingState tracks the current state of the encoding.
     * @param key           the key to be encoded.
     * @param value         the value to be encoded.
     */
    public static void encode(
        final EncodingState encodingState,
        final CharSequence key,
        final CharSequence value)
    {
        if (encodingState.isReachedLimit())
        {
            return;
        }

        final int length = length(key, value);

        if (encodingState.remaining() < length)
        {
            encodingState.reachedLimit(true);
            return;
        }
        encodeString(encodingState, key);
        encodeString(encodingState, value);
    }

    private static void encodeString(final EncodingState encodingState, final CharSequence value)
    {
        final MutableDirectBuffer buffer = encodingState.buffer();
        if (value.length() < 24)
        {
            final int valueStartOffset = encodingState.offset();
            final byte valueType = createTypeByte(TEXT_STRING_TYPE, value.length());
            buffer.putByte(valueStartOffset, valueType);
            buffer.putStringWithoutLengthAscii(valueStartOffset + 1, value);
            encodingState.incrementOffset(1 + value.length());
        }
        else if (value.length() < (1 << 8))
        {
            final byte valueType = createTypeByte(TEXT_STRING_TYPE, (byte)24);
            buffer.putByte(encodingState.offset(), valueType);
            buffer.putByte(encodingState.offset() + 1, (byte)value.length());
            buffer.putStringWithoutLengthAscii(encodingState.offset() + 2, value);
            encodingState.incrementOffset(2 + value.length());
        }
        else if (value.length() < (1 << 16))
        {
            final byte valueType = createTypeByte(TEXT_STRING_TYPE, (byte)25);
            buffer.putByte(encodingState.offset(), valueType);
            buffer.putShort(encodingState.offset() + 1, (short)value.length(), BIG_ENDIAN);
            buffer.putStringWithoutLengthAscii(encodingState.offset() + 1 + SIZE_OF_SHORT, value);
            encodingState.incrementOffset(1 + SIZE_OF_SHORT + value.length());
        }
        else
        {
            final byte valueType = createTypeByte(TEXT_STRING_TYPE, (byte)26);
            buffer.putByte(encodingState.offset(), valueType);
            buffer.putInt(encodingState.offset() + 1, value.length(), BIG_ENDIAN);
            buffer.putStringWithoutLengthAscii(encodingState.offset() + 1 + SIZE_OF_INT, value);
            encodingState.incrementOffset(1 + SIZE_OF_INT + value.length());
        }

    }


    /**
     * @param clusterEventCode
     * @param timestamp
     * @return the length of the header of the Cbor message.
     */
    public static int headerLength(final ClusterEventCode clusterEventCode, final long timestamp)
    {
        return 0;
    }

    /**
     * Encode the header of the Cbor message.
     *
     * @param encodingState    tracks the current state of the encoding.
     * @param clusterEventCode the cluster event code.
     * @param timestamp        the timestamp of the event.
     */
    public static void encodeHeader(
        final EncodingState encodingState,
        final ClusterEventCode clusterEventCode,
        final long timestamp)
    {
        final byte mapType = (byte)0xBF;
        encodingState.buffer().putByte(encodingState.offset(), mapType);
        encodingState.incrementOffset(1);
    }

    /**
     * @param encodingState tracks the current state of the encoding.
     */
    public static void encodeFooter(final EncodingState encodingState)
    {
        final byte terminal = (byte)0b111_11111;
        encodingState.buffer().putByte(encodingState.offset(), terminal);
        encodingState.incrementOffset(1);
    }
}
