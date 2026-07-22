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
package io.aeron.logging;

import org.agrona.MutableDirectBuffer;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_BYTE;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.BitUtil.SIZE_OF_SHORT;

/**
 * Utility class for CBOR encoding.
 */
public final class CborEncode
{
    // Base bytes for major types
    static final int UNSIGNED_INTEGER_MAJOR_TYPE = 0;
    static final int NEGATIVE_INTEGER_MAJOR_TYPE = 1 << 5;
    static final int TEXT_STRING_MAJOR_TYPE = 3 << 5;
    static final int ARRAY_MAJOR_TYPE = 4 << 5;
    static final int MAP_MAJOR_TYPE = 5 << 5;
    // NOTE: This major type actually includes floats and simple values
    static final int SIMPLE_VALUE_MAJOR_TYPE = 7 << 5;

    static final int ADDITIONAL_CONTENT_1_BYTE = 24;
    static final int ADDITIONAL_CONTENT_2_BYTE = 25;
    static final int ADDITIONAL_CONTENT_4_BYTE = 26;
    static final int ADDITIONAL_CONTENT_8_BYTE = 27;
    static final int ADDITIONAL_CONTENT_INDEFINITE = 31;

    static final int ADDITIONAL_CONTENT_NULL = 22;
    static final int ADDITIONAL_CONTENT_FALSE = 20;
    static final int ADDITIONAL_CONTENT_TRUE = 21;

    // Simple values
    static final byte NULL_VALUE = typeByte(SIMPLE_VALUE_MAJOR_TYPE, ADDITIONAL_CONTENT_NULL);
    static final byte FALSE_VALUE = typeByte(SIMPLE_VALUE_MAJOR_TYPE, ADDITIONAL_CONTENT_FALSE);
    static final byte TRUE_VALUE = typeByte(SIMPLE_VALUE_MAJOR_TYPE, ADDITIONAL_CONTENT_TRUE);
    static final int BREAK = 0xFF;

    private static final String TRUNC_END = "...";
    static final int ENTRIES_LENGTH = 3;

    private CborEncode()
    {
    }

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
        if (null == value)
        {
            return 1;
        }
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

        encodeString(encodingState, key, false);
        encodeNumber(encodingState, value);
    }

    /**
     * Encode a key/string pair.
     *
     * @see #encode(EncodingState, CharSequence, CharSequence, boolean)
     * @param encodingState tracks the current state of the encoding.
     * @param key           the key to be encoded.
     * @param value         the value to be encoded.
     */
    public static void encode(
        final EncodingState encodingState,
        final CharSequence key,
        final CharSequence value
    )
    {
        encode(encodingState, key, value, true);
    }

    /**
     * Encodes a key-value pair of a string and a string.
     *
     * @param encodingState tracks the current state of the encoding.
     * @param key           the key to be encoded.
     * @param value         the value to be encoded.
     * @param allowTruncate   whether the value can be truncated (or is just dropped).
     */
    public static void encode(
        final EncodingState encodingState,
        final CharSequence key,
        final CharSequence value,
        final boolean allowTruncate)
    {
        if (encodingState.isReachedLimit())
        {
            return;
        }
        encodeEntry(encodingState, key, value, allowTruncate);
    }

    private static void encodeEntry(
        final EncodingState encodingState,
        final CharSequence key,
        final CharSequence value,
        final boolean allowTruncate)
    {
        final int keyLength = lengthString(key);
        // Key pre-check
        if (encodingState.remaining() < keyLength)
        {
            encodingState.reachedLimit(true);
            return;
        }

        final int remainingBytes = encodingState.remaining() - keyLength;
        if (null == value)
        {
            if (remainingBytes <= 0)
            {
                encodingState.reachedLimit(true);
                return;
            }
            encodeString(encodingState, key, false);
            encodeNull(encodingState);
            return;
        }

        final int valueLengthFieldBytes = lengthFieldBytes(value.length());
        final int finalValueLength = Math.min(
            value.length(),
            remainingBytes - (1 + valueLengthFieldBytes));
        final boolean needsTruncation = finalValueLength < value.length();

        if (needsTruncation && (!allowTruncate || finalValueLength < TRUNC_END.length()))
        {
            encodingState.reachedLimit(true);
            return;
        }

        encodeString(encodingState, key, false);
        encodeString(encodingState, value, allowTruncate);
    }

    private static void encodeString(
        final EncodingState encodingState,
        final CharSequence value
    )
    {
        encodeString(encodingState, value, false);
    }

    private static int lengthFieldBytes(final int valueLength)
    {
        if (valueLength < ADDITIONAL_CONTENT_1_BYTE)
        {
            return 0;
        }
        else if (valueLength < (1 << 8))
        {
            return SIZE_OF_BYTE;
        }
        else if (valueLength < (1 << 16))
        {
            return SIZE_OF_SHORT;
        }
        else
        {
            return SIZE_OF_INT;
        }
    }

    private static void encodeString(
        final EncodingState encodingState,
        final CharSequence value,
        final boolean allowTruncate)
    {
        final int valueLength = value.length();

        final int lengthFieldBytes = lengthFieldBytes(valueLength);

        // included string length + ellipsis (if truncated)
        final int finalLength = Math.min(
            valueLength,
            encodingState.remaining() - (1 + lengthFieldBytes));

        final boolean needsTruncation = finalLength < valueLength;
        if (needsTruncation && (!allowTruncate || finalLength < TRUNC_END.length()))
        {
            encodingState.reachedLimit(true);
            return;
        }

        final MutableDirectBuffer buffer = encodingState.buffer();
        final int offset = encodingState.offset();
        if (lengthFieldBytes > 0)
        {
            switch (lengthFieldBytes)
            {
                case SIZE_OF_BYTE ->
                {
                    buffer.putByte(offset, typeByte(TEXT_STRING_MAJOR_TYPE, ADDITIONAL_CONTENT_1_BYTE));
                    buffer.putByte(offset + 1, (byte)finalLength);
                }
                case SIZE_OF_SHORT ->
                {
                    buffer.putByte(offset, typeByte(TEXT_STRING_MAJOR_TYPE, ADDITIONAL_CONTENT_2_BYTE));
                    buffer.putShort(offset + 1, (short)finalLength, BIG_ENDIAN);
                }
                case SIZE_OF_INT ->
                {
                    buffer.putByte(offset, typeByte(TEXT_STRING_MAJOR_TYPE, ADDITIONAL_CONTENT_4_BYTE));
                    buffer.putInt(offset + 1, finalLength, BIG_ENDIAN);
                }
            }
        }
        else
        {
            buffer.putByte(offset, typeByte(TEXT_STRING_MAJOR_TYPE, finalLength));
        }

        encodingState.incrementOffset(1 + lengthFieldBytes);
        final int toWriteLength = needsTruncation ? finalLength - TRUNC_END.length() : value.length();
        buffer.putStringWithoutLengthAscii(encodingState.offset(), value, 0, toWriteLength);
        encodingState.incrementOffset(toWriteLength);

        if (needsTruncation)
        {
            buffer.putStringWithoutLengthAscii(encodingState.offset(), TRUNC_END);
            encodingState.incrementOffset(TRUNC_END.length());
            encodingState.reachedLimit(true);
        }
    }

    private static void encodeNull(final EncodingState encodingState)
    {
        encodingState.buffer().putByte(encodingState.offset(), NULL_VALUE);
        encodingState.incrementOffset(1);
    }

    private static void encodeBoolean(final EncodingState encodingState, final boolean value)
    {
        encodingState.buffer().putByte(encodingState.offset(), value ? TRUE_VALUE : FALSE_VALUE);
        encodingState.incrementOffset(1);
    }

    /**
     * Calculate the length of the header of the Cbor message.
     *
     * @param eventCode  for this message.
     * @param timestamp  of this message.
     * @return the length of the header of the Cbor message.
     */
    public static int headerLength(final EventCode eventCode, final long timestamp)
    {
        return 1 + lengthNumber(timestamp) + lengthNumber(eventCode.toEventCodeId()) + 1;
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
        final EventCode clusterEventCode,
        final long timestamp)
    {
        encodingState.buffer().putByte(encodingState.offset(), typeByte(ARRAY_MAJOR_TYPE, ENTRIES_LENGTH));
        encodingState.incrementOffset(1);
        encodeNumber(encodingState, timestamp);
        encodeNumber(encodingState, clusterEventCode.toEventCodeId());
        encodingState.buffer().putByte(encodingState.offset(), typeByte(MAP_MAJOR_TYPE, ADDITIONAL_CONTENT_INDEFINITE));
        encodingState.incrementOffset(1);
    }

    /**
     * Encode the footer.
     *
     * @param encodingState tracks the current state of the encoding.
     */
    public static void encodeFooter(final EncodingState encodingState)
    {
        // TODO: Decide how truncation flag should be handled here
        encodingState.buffer().putByte(encodingState.offset(), (byte)BREAK);
        encodingState.incrementOffset(1);
    }

    /**
     * Encode a key/boolean pair.
     *
     * @param encodingState tracks the current state of the encoding.
     * @param key           the key to be encoded.
     * @param value         the boolean value to be encoded.
     */
    public static void encode(
        final EncodingState encodingState,
        final CharSequence key,
        final boolean value)
    {
        if (encodingState.isReachedLimit())
        {
            return;
        }

        final int keyLength = lengthString(key);
        if (encodingState.remaining() < keyLength + 1)
        {
            encodingState.reachedLimit(true);
            return;
        }

        encodeString(encodingState, key, false);
        encodeBoolean(encodingState, value);
    }

    /**
     * {@return the length of the footer in bytes}
     */
    public static int footerLength()
    {
        return 1;
    }
}
