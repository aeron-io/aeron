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
package io.aeron.agent;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import static java.lang.Math.min;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Helper class for encoding log events.
 */
public final class CommonEventEncoder
{
    /**
     * Length of log header in bytes.
     */
    public static final int LOG_HEADER_LENGTH = 16;

    /**
     * Max capture length.
     */
    public static final int MAX_CAPTURE_LENGTH = EventConfiguration.MAX_EVENT_LENGTH - LOG_HEADER_LENGTH;

    /**
     * State transition separator.
     */
    public static final String STATE_SEPARATOR = " -> ";

    private CommonEventEncoder()
    {
    }

    /**
     * Encode event log header.
     *
     * @param encodingBuffer log buffer.
     * @param offset         in the log buffer.
     * @param captureLength  capture length.
     * @param length         original length.
     * @return header length in bytes.
     */
    public static int encodeLogHeader(
        final MutableDirectBuffer encodingBuffer, final int offset, final int captureLength, final int length)
    {
        return internalEncodeLogHeader(encodingBuffer, offset, captureLength, length, SystemNanoClock.INSTANCE);
    }

    static int internalEncodeLogHeader(
        final MutableDirectBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final NanoClock nanoClock)
    {
        if (captureLength < 0 || captureLength > length || captureLength > MAX_CAPTURE_LENGTH)
        {
            throw new IllegalArgumentException("invalid input: captureLength=" + captureLength + ", length=" + length);
        }

        int encodedLength = 0;
        /*
         * Stream of values:
         * - capture buffer length (int)
         * - total buffer length (int)
         * - timestamp (long)
         * - buffer (until end)
         */

        encodingBuffer.putInt(offset + encodedLength, captureLength, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, length, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putLong(offset + encodedLength, nanoClock.nanoTime(), LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        return encodedLength;
    }

    /**
     * Encode {@link InetSocketAddress} address.
     *
     * @param encodingBuffer log buffer.
     * @param offset         in the log buffer.
     * @param address        to encode.
     * @return encoded length in bytes.
     */
    public static int encodeSocketAddress(
        final UnsafeBuffer encodingBuffer, final int offset, final InetSocketAddress address)
    {
        int encodedLength = 0;
        /*
         * Stream of values:
         * - port (int) (unsigned short int)
         * - IP address length (int) (4 or 16)
         * - IP address (4 or 16 bytes)
         */

        encodingBuffer.putInt(offset + encodedLength, address.getPort(), LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        final byte[] addressBytes = address.getAddress().getAddress();
        encodingBuffer.putInt(offset + encodedLength, addressBytes.length, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putBytes(offset + encodedLength, addressBytes);
        encodedLength += addressBytes.length;

        return encodedLength;
    }

    /**
     * Encode {@link InetAddress} address.
     *
     * @param encodingBuffer log buffer.
     * @param offset         in the log buffer.
     * @param address        to encode.
     * @return encoded length in bytes.
     */
    public static int encodeInetAddress(
        final UnsafeBuffer encodingBuffer, final int offset, final InetAddress address)
    {
        int encodedLength = 0;

        if (null != address)
        {
            /*
             * Stream of values:
             * - IP address length (int) (4 or 16)
             * - IP address (4 or 16 bytes)
             */
            final byte[] addressBytes = address.getAddress();
            encodingBuffer.putInt(offset + encodedLength, addressBytes.length, LITTLE_ENDIAN);
            encodedLength += SIZE_OF_INT;

            encodingBuffer.putBytes(offset + encodedLength, addressBytes);
            encodedLength += addressBytes.length;
        }
        else
        {
            encodingBuffer.putInt(offset, 0);
            encodedLength += SIZE_OF_INT;
        }

        return encodedLength;
    }

    /**
     * Encode string at the end of the log event.
     *
     * @param encodingBuffer    log buffer.
     * @param offset            in the log buffer.
     * @param remainingCapacity till end of the event.
     * @param value             to encode.
     * @return encoded length in bytes.
     */
    public static int encodeTrailingString(
        final UnsafeBuffer encodingBuffer, final int offset, final int remainingCapacity, final String value)
    {
        final int maxLength = remainingCapacity - SIZE_OF_INT;
        if (value.length() <= maxLength)
        {
            return encodingBuffer.putStringAscii(offset, value, LITTLE_ENDIAN);
        }
        else
        {
            encodingBuffer.putInt(offset, maxLength, LITTLE_ENDIAN);
            encodingBuffer.putStringWithoutLengthAscii(offset + SIZE_OF_INT, value, 0, maxLength - 3);
            encodingBuffer.putStringWithoutLengthAscii(offset + SIZE_OF_INT + maxLength - 3, "...");
            return remainingCapacity;
        }
    }

    /**
     * Encode binary event.
     *
     * @param encodingBuffer log buffer.
     * @param offset         in the log buffer.
     * @param captureLength  capture length.
     * @param length         original length.
     * @param srcBuffer      containing binary message.
     * @param srcOffset      where source event begins.
     * @return encoded length in bytes.
     */
    public static int encode(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final DirectBuffer srcBuffer,
        final int srcOffset)
    {
        final int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        encodingBuffer.putBytes(offset + encodedLength, srcBuffer, srcOffset, captureLength);
        return encodedLength + captureLength;
    }

    /**
     * Encode state transition.
     *
     * @param <E>            enum type.
     * @param encodingBuffer log buffer.
     * @param offset         in the log buffer.
     * @param from           previous state.
     * @param to             new state.
     * @return encoded length in bytes.
     */
    public static <E extends Enum<E>> int encodeStateChange(
        final UnsafeBuffer encodingBuffer, final int offset, final E from, final E to)
    {
        int encodedLength = 0;

        final String fromName = enumName(from);
        final String toName = enumName(to);

        encodingBuffer.putInt(
            offset,
            fromName.length() + STATE_SEPARATOR.length() + toName.length(),
            LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;
        encodedLength += encodingBuffer.putStringWithoutLengthAscii(offset + encodedLength, fromName);
        encodedLength += encodingBuffer.putStringWithoutLengthAscii(offset + encodedLength, STATE_SEPARATOR);
        encodedLength += encodingBuffer.putStringWithoutLengthAscii(offset + encodedLength, toName);

        return encodedLength;
    }

    /**
     * Encode state transition at the end.
     *
     * @param <E>                  enum type.
     * @param encodingBuffer       log buffer.
     * @param offset               in the log buffer.
     * @param runningEncodedLength running     encoding length.
     * @param captureLength        capture length.
     * @param from                 previous state.
     * @param to                   new state.
     * @return encoded length in bytes.
     */
    public static <E extends Enum<E>> int encodeTrailingStateChange(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int runningEncodedLength,
        final int captureLength,
        final E from,
        final E to)
    {
        int encodedLength = runningEncodedLength;
        encodingBuffer.putInt(
            offset + encodedLength,
            captureLength - (runningEncodedLength - LOG_HEADER_LENGTH + SIZE_OF_INT),
            LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        final String fromName = enumName(from);
        final String toName = enumName(to);
        encodedLength += encodingBuffer.putStringWithoutLengthAscii(offset + encodedLength, fromName);
        encodedLength += encodingBuffer.putStringWithoutLengthAscii(offset + encodedLength, STATE_SEPARATOR);
        encodedLength += encodingBuffer.putStringWithoutLengthAscii(offset + encodedLength, toName);

        return encodedLength;
    }

    /**
     * Returns capture length.
     *
     * @param length to compute.
     * @return capture length in bytes capped at {@link #MAX_CAPTURE_LENGTH}.
     */
    public static int captureLength(final int length)
    {
        return min(length, MAX_CAPTURE_LENGTH);
    }

    /**
     * Returns full encoded length for given the capture length.
     *
     * @param captureLength capture length.
     * @return encoded length in bytes.
     */
    public static int encodedLength(final int captureLength)
    {
        return LOG_HEADER_LENGTH + captureLength;
    }

    /**
     * Compute encoded length for {@link InetSocketAddress}.
     *
     * @param address to encode.
     * @return length in bytes.
     */
    public static int socketAddressLength(final InetSocketAddress address)
    {
        return SIZE_OF_INT + inetAddressLength(address.getAddress());
    }

    /**
     * Compute encoded length for {@link InetAddress}.
     *
     * @param address to encode.
     * @return length in bytes.
     */
    public static int inetAddressLength(final InetAddress address)
    {
        return SIZE_OF_INT + (null != address ? address.getAddress().length : 0);
    }

    /**
     * Compute encoded length for trailing string.
     *
     * @param value     new state.
     * @param maxLength max length.
     * @return length in bytes.
     */
    public static int trailingStringLength(final String value, final int maxLength)
    {
        return SIZE_OF_INT + min(value.length(), maxLength);
    }

    /**
     * Compute state transition encoded length in bytes.
     *
     * @param <E>  type of the enum.
     * @param from old state.
     * @param to   new state.
     * @return length in bytes.
     */
    public static <E extends Enum<E>> int stateTransitionStringLength(final E from, final E to)
    {
        return SIZE_OF_INT + enumName(from).length() + STATE_SEPARATOR.length() + enumName(to).length();
    }

    /**
     * Null-safe name for the enum.
     *
     * @param <E>   type of the enum.
     * @param value value to return name for.
     * @return name or {@code "null"} if {@code null == value}
     */
    public static <E extends Enum<E>> String enumName(final E value)
    {
        return null == value ? "null" : value.name();
    }
}
