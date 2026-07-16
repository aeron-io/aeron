package io.aeron.agent;

import org.agrona.MutableDirectBuffer;

public class CborUtil
{
    public static int length(final CharSequence key, final int value)
    {
        return 0;
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

//    private int encode(final MutableDirectBuffer buffer, int value)
//    {
//        if (value < 0)
//        {
//
//        }
//        else if (value < 24)
//        {
//
//        }
//    }

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

        encodingState.incrementOffset(1 + key.length() + 1);

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
