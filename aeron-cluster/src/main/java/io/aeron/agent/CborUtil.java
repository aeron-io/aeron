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
    static final int UNSIGNED_INTEGER_MAJOR_TYPE = 0;
    static final int NEGATIVE_INTEGER_MAJOR_TYPE = 1 << 5;
    static final int TEXT_STRING_MAJOR_TYPE = 3 << 5;
    static final int MAP_MAJOR_TYPE = 5 << 5;

    public static final int ADDITIONAL_CONTENT_1_BYTE = 24;
    public static final int ADDITIONAL_CONTENT_2_BYTE = 25;
    public static final int ADDITIONAL_CONTENT_4_BYTE = 26;
    public static final int ADDITIONAL_CONTENT_8_BYTE = 27;
    public static final int ADDITIONAL_CONTENT_INDEFINITE = 31;

    public static final int BREAK = 0xFF;

    private static final String TRUNC_END = "...";

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

    static byte typeByte(final int majorType, final int modifier)
    {
        return (byte)((0b111_00000 & majorType) | (0b000_11111 & modifier));
    }

    /**
     * Calculates the total length of an encoded string-long pair.
     *
     * @param key   to be encoded.
     * @param value to be encoded.
     * @return the total length of the encoded string-long pair.
     */
    public static int length(final CharSequence key, final long value)
    {
        return lengthString(key) + lengthNumber(value);
    }

    /**
     * Calculates the total length of an encoded string-enum pair.
     *
     * @param key   to be encoded.
     * @param value to be encoded.
     * @param <E>   the type of the enum to be encoded.
     * @return the total length of the encoded string-enum pair.
     */
    public static <E extends Enum<E>> int length(final CharSequence key, final E value)
    {
        return length(key, value.name());
    }

    /**
     * Calculates the total length of the encoded string-string pair.
     *
     * @param key   to be encoded.
     * @param value to be encoded.
     * @return the total length of the encoded string-string pair.
     */
    public static int length(final CharSequence key, final CharSequence value)
    {
        return lengthString(key) + lengthString(value);
    }

    private static void encodeNumber(
        final EncodingState encodingState,
        final long value)
    {
        // TODO: handle long (8 byte) case either here or through a new method
        final int offset = encodingState.offset();
        final MutableDirectBuffer buffer = encodingState.buffer();

        final int majorType;
        final long magnitude;
        if (value < 0)
        {
            // Reference: https://datatracker.ietf.org/doc/html/rfc8949#section-3.1-2.4
            majorType = NEGATIVE_INTEGER_MAJOR_TYPE;
            magnitude = ~value;
        }
        else
        {
            majorType = UNSIGNED_INTEGER_MAJOR_TYPE;
            magnitude = value;
        }

        // Reference: https://datatracker.ietf.org/doc/html/rfc8949#name-specification-of-the-cbor-e
        if (magnitude < ADDITIONAL_CONTENT_1_BYTE)
        {
            buffer.putByte(offset, typeByte(majorType, (int)magnitude));
            encodingState.incrementOffset(1);
        }
        else
        {
            // Encode based on minimum number of bytes required
            if (magnitude < (1 << 8))
            {
                buffer.putByte(offset, typeByte(majorType, ADDITIONAL_CONTENT_1_BYTE));
                buffer.putByte(offset + 1, (byte)magnitude);
                encodingState.incrementOffset(1 + SIZE_OF_BYTE);
            }
            else if (magnitude < (1 << 16))
            {
                buffer.putByte(offset, typeByte(majorType, ADDITIONAL_CONTENT_2_BYTE));
                buffer.putShort(offset + 1, (short)magnitude, BIG_ENDIAN);
                encodingState.incrementOffset(1 + SIZE_OF_SHORT);
            }
            else if (magnitude < (1L << 32))
            {
                buffer.putByte(offset, typeByte(majorType, ADDITIONAL_CONTENT_4_BYTE));
                buffer.putInt(offset + 1, (int)magnitude, BIG_ENDIAN);
                encodingState.incrementOffset(1 + SIZE_OF_INT);
            }
            else
            {
                buffer.putByte(offset, typeByte(majorType, ADDITIONAL_CONTENT_8_BYTE));
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
     * Encodes a key-value pair of a string and a string.
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
        encodeString(encodingState, key);
        encodeString(encodingState, value);
    }

    private static void encodeString(final EncodingState encodingState, final CharSequence value)
    {
        final int valueLength = value.length();

        final int lengthFieldBytes;
        final int additionalContent;
        if (valueLength < ADDITIONAL_CONTENT_1_BYTE)
        {
            lengthFieldBytes = 0;
            additionalContent = 0;
        }
        else if (valueLength < (1 << 8))
        {
            lengthFieldBytes = SIZE_OF_BYTE;
            additionalContent = ADDITIONAL_CONTENT_1_BYTE;
        }
        else if (valueLength < (1 << 16))
        {
            lengthFieldBytes = SIZE_OF_SHORT;
            additionalContent = ADDITIONAL_CONTENT_2_BYTE;
        }
        else
        {
            lengthFieldBytes = SIZE_OF_INT;
            additionalContent = ADDITIONAL_CONTENT_4_BYTE;
        }

        // included string length + ellipsis (if truncated)
        final int finalLength = Math.min(
            valueLength,
            encodingState.remaining() - (1 + lengthFieldBytes));

        final MutableDirectBuffer buffer = encodingState.buffer();
        final int offset = encodingState.offset();
        if (lengthFieldBytes > 0)
        {
            buffer.putByte(offset, typeByte(TEXT_STRING_MAJOR_TYPE, additionalContent));
            switch (lengthFieldBytes)
            {
                case SIZE_OF_BYTE -> buffer.putByte(offset + 1, (byte)finalLength);
                case SIZE_OF_SHORT -> buffer.putShort(offset + 1, (short)finalLength, BIG_ENDIAN);
                case SIZE_OF_INT -> buffer.putInt(offset + 1, finalLength, BIG_ENDIAN);
            }
        }
        else
        {
            buffer.putByte(offset, typeByte(TEXT_STRING_MAJOR_TYPE, finalLength));
        }

        encodingState.incrementOffset(1 + lengthFieldBytes);
        if (finalLength < valueLength)
        {
            // Truncated case
            final int includedValueLength = finalLength - TRUNC_END.length();
            buffer.putStringWithoutLengthAscii(
                encodingState.offset(),
                value,
                0,
                includedValueLength);

            buffer.putStringWithoutLengthAscii(
                encodingState.offset() + includedValueLength,
                TRUNC_END);
            encodingState.reachedLimit(true);
        }
        else
        {
            // Regular case
            buffer.putStringWithoutLengthAscii(encodingState.offset(), value);
        }
        encodingState.incrementOffset(finalLength);
    }

    /**
     * Calculate the length of the header of the Cbor message.
     *
     * @param clusterEventCode  for this message.
     * @param timestamp         of this message.
     * @return the length of the header of the Cbor message.
     */
    public static int headerLength(final ClusterEventCode clusterEventCode, final long timestamp)
    {
        return 1;
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
        encodingState.buffer().putByte(encodingState.offset(), typeByte(MAP_MAJOR_TYPE, ADDITIONAL_CONTENT_INDEFINITE));
        encodingState.incrementOffset(1);
    }

    /**
     * @param encodingState tracks the current state of the encoding.
     */
    public static void encodeFooter(final EncodingState encodingState)
    {
        // TODO: Decide how truncation flag should be handled here
        encodingState.buffer().putByte(encodingState.offset(), (byte)BREAK);
        encodingState.incrementOffset(1);
    }
}
