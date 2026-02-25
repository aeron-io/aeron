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
package io.aeron;

import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.logbuffer.FrameDescriptor.FLAGS_OFFSET;
import static io.aeron.logbuffer.LogBufferDescriptor.computeFragmentedFrameLength;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Reusable Builder for appending a sequence of buffer fragments which grows internal capacity as needed.
 * <p>
 * The underlying buffer can be byte[] backed or a direct {@link ByteBuffer} if the isDirect param to the constructor
 * is true.
 * <p>
 * Similar in concept to {@link StringBuilder}.
 */
public final class BufferBuilder
{
    static final int MAX_CAPACITY = Integer.MAX_VALUE - 8;
    static final int INIT_MIN_CAPACITY = 4096;

    private final boolean isDirect;
    private int limit;
    private int nextTermOffset = NULL_VALUE;
    private int firstFrameLength;
    private final UnsafeBuffer buffer = new UnsafeBuffer();
    final UnsafeBuffer headerBuffer = new UnsafeBuffer();
    final Header completeHeader = new Header(0, 0);

    /**
     * Construct a buffer builder with an initial capacity of zero and isDirect false.
     */
    public BufferBuilder()
    {
        this(0, false);
    }

    /**
     * Construct a buffer builder with an initial capacity and isDirect false.
     *
     * @param initialCapacity at which the capacity will start.
     */
    public BufferBuilder(final int initialCapacity)
    {
        this(initialCapacity, false);
    }

    /**
     * Construct a buffer builder with an initial capacity.
     *
     * @param initialCapacity at which the capacity will start.
     * @param isDirect        is the underlying buffer to be a direct {@link ByteBuffer}
     */
    public BufferBuilder(final int initialCapacity, final boolean isDirect)
    {
        if (initialCapacity < 0 || initialCapacity > MAX_CAPACITY)
        {
            throw new IllegalArgumentException("initialCapacity outside range 0 - " + MAX_CAPACITY +
                ": initialCapacity=" + initialCapacity);
        }

        this.isDirect = isDirect;
        if (isDirect)
        {
            if (initialCapacity > 0)
            {
                buffer.wrap(newDirectBuffer(initialCapacity));
            }
            headerBuffer.wrap(newDirectBuffer(HEADER_LENGTH));
        }
        else
        {
            if (initialCapacity > 0)
            {
                buffer.wrap(new byte[initialCapacity]);
            }
            headerBuffer.wrap(new byte[HEADER_LENGTH]);
        }
    }

    /**
     * The current capacity of the buffer.
     *
     * @return the current capacity of the buffer.
     */
    public int capacity()
    {
        return buffer.capacity();
    }

    /**
     * The current limit of the buffer that has been used by append operations.
     *
     * @return the current limit of the buffer that has been used by append operations.
     */
    public int limit()
    {
        return limit;
    }

    /**
     * Set this limit for this buffer as the position at which the next append operation will occur.
     *
     * @param limit to be the new value.
     */
    public void limit(final int limit)
    {
        if (limit < 0 || limit >= buffer.capacity())
        {
            throw new IllegalArgumentException(
                "limit outside range: capacity=" + buffer.capacity() + " limit=" + limit);
        }

        this.limit = limit;
    }

    /**
     * Get the value which the next term offset for a fragment to be assembled should begin at.
     *
     * @return the value which the next term offset for a fragment to be assembled should begin at.
     */
    public int nextTermOffset()
    {
        return nextTermOffset;
    }

    /**
     * Set the value which the next term offset for a fragment to be assembled should begin at.
     *
     * @param offset which the next term offset for a fragment to be assembled should begin at.
     */
    public void nextTermOffset(final int offset)
    {
        nextTermOffset = offset;
    }

    /**
     * The {@link MutableDirectBuffer} that encapsulates the internal buffer.
     *
     * @return the {@link MutableDirectBuffer} that encapsulates the internal buffer.
     */
    public MutableDirectBuffer buffer()
    {
        return buffer;
    }

    /**
     * Reset the builder to restart append operations. The internal buffer does not shrink.
     *
     * @return the builder for fluent API usage.
     */
    public BufferBuilder reset()
    {
        limit = 0;
        nextTermOffset = NULL_VALUE;
        completeHeader
            .context(null)
            .fragmentedFrameLength(NULL_VALUE);
        return this;
    }

    /**
     * Compact the buffer to reclaim unused space above the limit.
     *
     * @return the builder for fluent API usage.
     */
    public BufferBuilder compact()
    {
        final int newCapacity = Math.max(INIT_MIN_CAPACITY, limit);
        if (newCapacity < buffer.capacity())
        {
            resize(newCapacity);
        }

        return this;
    }

    /**
     * Append a source buffer to the end of the internal buffer, resizing the internal buffer when required.
     *
     * @param srcBuffer from which to copy.
     * @param srcOffset in the source buffer from which to copy.
     * @param length    in bytes to copy from the source buffer.
     * @return the builder for fluent API usage.
     */
    public BufferBuilder append(final DirectBuffer srcBuffer, final int srcOffset, final int length)
    {
        ensureCapacity(length);

        buffer.putBytes(limit, srcBuffer, srcOffset, length);
        limit += length;

        return this;
    }

    /**
     * Capture information available in the header of the very first frame.
     *
     * @param header of the first frame.
     * @return the builder for fluent API usage.
     */
    public BufferBuilder captureHeader(final Header header)
    {
        completeHeader
            .initialTermId(header.initialTermId())
            .positionBitsToShift(header.positionBitsToShift())
            .offset(0)
            .buffer(headerBuffer);

        firstFrameLength = header.frameLength();

        headerBuffer.putBytes(0, header.buffer(), header.offset(), HEADER_LENGTH);
        return this;
    }

    /**
     * Use the information from the header of the last frame to create a header for the assembled message, i.e. fixups
     * the flags and the frame length.
     *
     * @param header of the last frame.
     * @return complete message header.
     */
    public Header completeHeader(final Header header)
    {
        // compute the `fragmented frame length` of the complete message
        final int fragmentedFrameLength = computeFragmentedFrameLength(limit, firstFrameLength - HEADER_LENGTH);
        completeHeader
            .context(header.context())
            .fragmentedFrameLength(fragmentedFrameLength);

        headerBuffer.putInt(FRAME_LENGTH_FIELD_OFFSET, HEADER_LENGTH + limit, LITTLE_ENDIAN);
        // compute complete flags
        headerBuffer.putByte(FLAGS_OFFSET, (byte)(headerBuffer.getByte(FLAGS_OFFSET) | header.flags()));

        return completeHeader;
    }

    private void ensureCapacity(final int additionalLength)
    {
        final long requiredCapacity = (long)limit + additionalLength;
        final int capacity = buffer.capacity();

        if (requiredCapacity > capacity)
        {
            if (requiredCapacity > MAX_CAPACITY)
            {
                throw new IllegalStateException(
                    "insufficient capacity: maxCapacity=" + MAX_CAPACITY +
                    " limit=" + limit +
                    " additionalLength=" + additionalLength);
            }

            resize(findSuitableCapacity(capacity, requiredCapacity));
        }
    }

    private void resize(final int newCapacity)
    {
        if (isDirect)
        {
            final ByteBuffer byteBuffer = newDirectBuffer(newCapacity);
            buffer.getBytes(0, byteBuffer, 0, limit);
            buffer.wrap(byteBuffer);
        }
        else
        {
            buffer.wrap(Arrays.copyOf(buffer.byteArray(), newCapacity));
        }
    }

    private static ByteBuffer newDirectBuffer(final int newCapacity)
    {
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(newCapacity);
        byteBuffer.order(LITTLE_ENDIAN);
        return byteBuffer;
    }

    static int findSuitableCapacity(final int capacity, final long requiredCapacity)
    {
        long newCapacity = Math.max(capacity, INIT_MIN_CAPACITY);

        while (newCapacity < requiredCapacity)
        {
            newCapacity = newCapacity + (newCapacity >> 1);
            if (newCapacity > MAX_CAPACITY)
            {
                newCapacity = MAX_CAPACITY;
                break;
            }
        }

        return (int)newCapacity;
    }
}
