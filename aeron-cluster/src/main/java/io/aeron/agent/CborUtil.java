package io.aeron.agent;

import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_BYTE;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_SHORT;

public class CborUtil
{
    // Base bytes for major types
    private final static byte BYTE_STRING_TYPE = 0b010_00000;
    private final static byte TEXT_STRING_TYPE = 0b011_00000;

    private final static int BYTES_PER_CHUNK = 0x17;

    public static int length(final CharSequence key, final int value)
    {
        final int keyBytes = 1 + key.length();
        final int magnitude = value < 0 ? ~value : value;
        if (magnitude < 24)
        {
            return 1 + keyBytes;
        }
        final int valueBytes = (magnitude >>> 16) != 0 ? SIZE_OF_INT :
            (magnitude >>> 8) != 0 ? SIZE_OF_SHORT : SIZE_OF_BYTE;

        return keyBytes + 1 + valueBytes;
    }

    public static int length(final CharSequence key, final long value)
    {
        return 0;
    }

    public static <E extends Enum<E>> int length(final CharSequence key, final E value)
    {
        return 0;
    }

    public static <E extends Enum<E>> int length(final CharSequence key, final CharSequence value)
    {
        return 0;
    }

    private static byte createTypeByte(final byte majorType, final int modifier)
    {
        return (byte)(majorType | modifier);
    }

    private static void encodeNumber(
        final EncodingState encodingState,
        int value)
    {
        // TODO: handle long (8 byte) case either here or through a new method
        final int offset = encodingState.offset();
        final MutableDirectBuffer buffer = encodingState.buffer();

        byte negativeMask = 0b000_00000;
        if (value < 0)
        {
            // Reference: https://datatracker.ietf.org/doc/html/rfc8949#section-3.1-2.4
            negativeMask = 0b1 << 5;
            value = ~value;
        }

        // Reference: https://datatracker.ietf.org/doc/html/rfc8949#name-specification-of-the-cbor-e
        if (value < 24)
        {
            buffer.putByte(offset, (byte)(value | negativeMask));
            encodingState.incrementOffset(1);
        }
        else
        {
            // Encode based on minimum number of bytes required
            if (value < 256)
            {
                // 1 extra byte case (24)
                buffer.putByte(offset, (byte)(0b000_11000 | negativeMask));
                buffer.putByte(offset + 1, (byte)value);
                encodingState.incrementOffset(1 + SIZE_OF_BYTE);
            }
            else if (value < 65536)
            {
                // 2 extra bytes case (25)
                buffer.putByte(offset, (byte)(0b000_11001 | negativeMask));
                buffer.putShort(offset + 1, (short)value, BIG_ENDIAN);
                encodingState.incrementOffset(1 + SIZE_OF_SHORT);
            }
            else
            {
                // 4 extra bytes case (26)
                buffer.putByte(offset, (byte)(0b000_11010 | negativeMask));
                buffer.putInt(offset + 1, value, BIG_ENDIAN);
                encodingState.incrementOffset(1 + SIZE_OF_INT);
            }
        }
    }

    public static int encode(
        final EncodingState encodingState,
        final CharSequence key,
        final int value)
    {
        if (encodingState.isReachedLimit())
        {
            return 0;
        }

        final int length = length(key, value);

        if (encodingState.remaining() < length)
        {
            encodingState.reachedLimit(true);
            return 0;
        }

        final byte byteStringType = (byte)((2 << 5 | key.length()) & 0xFF);

        final int offset = encodingState.offset();

        encodingState.buffer().putByte(offset, byteStringType);
        encodingState.buffer().putStringWithoutLengthAscii(offset + 1, key);

        encodingState.incrementOffset(1 + key.length());

        encodeNumber(encodingState, value);

        return 1;
    }

    public static int encode(
        final EncodingState encodingState,
        final CharSequence key,
        final long value)
    {
        if (encodingState.isReachedLimit())
        {
            return 0;
        }

        return 1;
    }

    public static <E extends Enum<E>> int encode(
        final EncodingState encodingState,
        final CharSequence key,
        final E value)
    {
        if (encodingState.isReachedLimit())
        {
            return 0;
        }

        return 1;
    }

    public static int encode(
        final EncodingState encodingState,
        final CharSequence key,
        final CharSequence value)
    {
        if (encodingState.isReachedLimit())
        {
            return 0;
        }

        final int length = length(key, value);

        if (encodingState.remaining() < length)
        {
            encodingState.reachedLimit(true);
            return 0;
        }

        final int offset = encodingState.offset();
        final MutableDirectBuffer buffer = encodingState.buffer();

        final byte keyType = createTypeByte(BYTE_STRING_TYPE, key.length());
        buffer.putByte(offset, keyType);
        buffer.putStringWithoutLengthAscii(offset + 1, key);
        encodingState.incrementOffset(1 + key.length());

        if (value.length() >= 0b11111)
        {
            encodeIndefiniteLengthString(encodingState, value);
        }
        else
        {
            // Definite length string case
            final int valueStartOffset = encodingState.offset();
            final byte valueType = createTypeByte(TEXT_STRING_TYPE, value.length());
            buffer.putByte(valueStartOffset, valueType);
            buffer.putStringWithoutLengthAscii(valueStartOffset + 1, value);
            encodingState.incrementOffset(1 + value.length());
        }

        return 1;
    }

    private static void encodeIndefiniteLengthString(
        final EncodingState encodingState,
        final CharSequence value)
    {
        int offset = encodingState.offset();
        final int valueLength = value.length();
        final MutableDirectBuffer buffer = encodingState.buffer();

        final byte indefiniteStringType = createTypeByte(TEXT_STRING_TYPE, 0b11111);
        buffer.putByte(offset++, indefiniteStringType);
        encodingState.incrementOffset(1);

        for (int i = 0; i <= valueLength / BYTES_PER_CHUNK; i++)
        {
            final int lowerBound = i * BYTES_PER_CHUNK;
            final int upperBound = Math.min((i + 1) * BYTES_PER_CHUNK, valueLength);
            final byte definiteStringType = createTypeByte(TEXT_STRING_TYPE, upperBound - lowerBound);
            buffer.putByte(offset++, definiteStringType);
            encodingState.incrementOffset(1);
            buffer.putStringWithoutLengthAscii(offset, value, lowerBound, upperBound);
            offset += upperBound - lowerBound;
            encodingState.incrementOffset(upperBound - lowerBound);
        }
        buffer.putByte(offset, (byte)0xFF);
        encodingState.incrementOffset(1);

    }

    public static int headerLength(final ClusterEventCode clusterEventCode, final long timestamp)
    {
        return 0;
    }

    public static void encodeHeader(
        final EncodingState encodingState,
        final ClusterEventCode clusterEventCode,
        final long timestamp)
    {
        final byte mapType = (byte)0xBF;
        encodingState.buffer().putByte(encodingState.offset(), mapType);
        encodingState.incrementOffset(1);
    }

    public static void encodeFooter(final EncodingState encodingState)
    {
        final byte terminal = (byte)0b111_11111;
        encodingState.buffer().putByte(encodingState.offset(), terminal);
        encodingState.incrementOffset(1);
    }
}
