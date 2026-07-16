package io.aeron.agent;

import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;

import static java.nio.ByteOrder.BIG_ENDIAN;

public class CborUtil
{
    public static int length(final CharSequence key, final int value)
    {
        final int keyBytes = 1 + key.length();
        final int magnitude = value < 0 ? ~value : value;
        if (magnitude < 24)
        {
            return 1 + keyBytes;
        }
        final int valueBytes = (magnitude >>> 16) != 0 ? BitUtil.SIZE_OF_INT :
            (magnitude >>> 8) != 0 ? BitUtil.SIZE_OF_SHORT : BitUtil.SIZE_OF_BYTE;

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

    private static void encodeNumber(
        final EncodingState encodingState,
        int value
        )
    {
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
            // Encode
            if (value < 256)
            {
                // Case 24: 1 extra byte
                buffer.putByte(offset, (byte)(0b000_11000 | negativeMask));
                buffer.putByte(offset + 1, (byte)value);
                encodingState.incrementOffset(1 + BitUtil.SIZE_OF_BYTE);
            }
            else if (value < 65536)
            {
                // Case 25: 2 extra bytes
                buffer.putByte(offset, (byte)(0b000_11001 | negativeMask));
                buffer.putShort(offset + 1, (short)value, BIG_ENDIAN);
                encodingState.incrementOffset(1 + BitUtil.SIZE_OF_SHORT);
            }
            else
            {
                // Case 26: 4 extra bytes
                buffer.putByte(offset, (byte)(0b000_11010 | negativeMask));
                buffer.putInt(offset + 1, value, BIG_ENDIAN);
                encodingState.incrementOffset(1 + BitUtil.SIZE_OF_INT);
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
        final byte intValue = (byte)(value & 0xFF);

        final int offset = encodingState.offset();

        encodingState.buffer().putByte(offset, byteStringType);
        encodingState.buffer().putStringWithoutLengthAscii(offset + 1, key);
        encodingState.buffer().putByte(offset + 1 + key.length(), intValue);

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

    public static <E extends Enum<E>> int encode(
        final EncodingState encodingState,
        final CharSequence key,
        final CharSequence value)
    {
        if (encodingState.isReachedLimit())
        {
            return 0;
        }

        return 1;
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
