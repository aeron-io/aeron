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

import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.ReadablePosition;

import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.DataHeaderFlyweight.RESERVED_VALUE_OFFSET;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.BitUtil.align;

/**
 * Aeron publisher API for sending messages to subscribers of a given channel and streamId pair. {@link Publication}s
 * are created via the {@link Aeron#addPublication(String, int)} method, and messages are sent via one of the
 * {@link #offer(DirectBuffer)} methods, or a {@link #tryClaim(int, BufferClaim)} and {@link BufferClaim#commit()}
 * method combination.
 * <p>
 * The APIs for tryClaim and offer are non-blocking and thread safe.
 * <p>
 * <b>Note:</b> Instances are threadsafe and can be shared between publishing threads.
 *
 * @see Aeron#addPublication(String, int)
 * @see BufferClaim
 */
public final class ConcurrentPublication extends Publication
{
    ConcurrentPublication(
        final ClientConductor clientConductor,
        final String channel,
        final int streamId,
        final int sessionId,
        final ReadablePosition positionLimit,
        final int channelStatusId,
        final LogBuffers logBuffers,
        final long originalRegistrationId,
        final long registrationId)
    {
        super(
            clientConductor,
            channel,
            streamId,
            sessionId,
            positionLimit,
            channelStatusId,
            logBuffers,
            originalRegistrationId,
            registrationId);
    }

    /**
     * {@inheritDoc}
     */
    public long availableWindow()
    {
        if (isClosed)
        {
            return CLOSED;
        }

        return positionLimit.getVolatile() - position();
    }

    /**
     * Non-blocking publish of a partial buffer containing a message.
     *
     * @param buffer                containing message.
     * @param offset                offset in the buffer at which the encoded message begins.
     * @param length                in bytes of the encoded message.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     */
    public long offer(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final ReservedValueSupplier reservedValueSupplier)
    {
        long newPosition = CLOSED;
        if (!isClosed)
        {
            final long limit = positionLimit.getVolatile();
            final int termCount = activeTermCount(logMetaDataBuffer);
            final int index = indexByTermCount(termCount);
            final UnsafeBuffer termBuffer = termBuffers[index];
            final int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (index * SIZE_OF_LONG);
            final long rawTail = logMetaDataBuffer.getLongVolatile(tailCounterOffset);
            final int termOffset = termOffset(rawTail, termBuffer.capacity());
            final int termId = termId(rawTail);

            if (termCount != (termId - initialTermId))
            {
                return ADMIN_ACTION;
            }

            final long position = computePosition(termId, termOffset, positionBitsToShift, initialTermId);
            if (position < limit)
            {
                if (length <= maxPayloadLength)
                {
                    checkPositiveLength(length);
                    newPosition = appendUnfragmentedMessage(
                        termBuffer, tailCounterOffset, buffer, offset, length, reservedValueSupplier);
                }
                else
                {
                    checkMaxMessageLength(length);
                    newPosition = appendFragmentedMessage(
                        termBuffer, tailCounterOffset, buffer, offset, length, reservedValueSupplier);
                }
            }
            else
            {
                newPosition = backPressureStatus(position, length);
            }
        }

        return newPosition;
    }


    /**
     * Non-blocking publish of a message composed of two parts, e.g. a header and encapsulated payload.
     *
     * @param bufferOne             containing the first part of the message.
     * @param offsetOne             at which the first part of the message begins.
     * @param lengthOne             of the first part of the message.
     * @param bufferTwo             containing the second part of the message.
     * @param offsetTwo             at which the second part of the message begins.
     * @param lengthTwo             of the second part of the message.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     */
    public long offer(
        final DirectBuffer bufferOne,
        final int offsetOne,
        final int lengthOne,
        final DirectBuffer bufferTwo,
        final int offsetTwo,
        final int lengthTwo,
        final ReservedValueSupplier reservedValueSupplier)
    {
        long newPosition = CLOSED;
        if (!isClosed)
        {
            final long limit = positionLimit.getVolatile();
            final int termCount = activeTermCount(logMetaDataBuffer);
            final int index = indexByTermCount(termCount);
            final UnsafeBuffer termBuffer = termBuffers[index];
            final int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (index * SIZE_OF_LONG);
            final long rawTail = logMetaDataBuffer.getLongVolatile(tailCounterOffset);
            final int termOffset = termOffset(rawTail, termBuffer.capacity());
            final int termId = termId(rawTail);

            if (termCount != (termId - initialTermId))
            {
                return ADMIN_ACTION;
            }

            final long position = computePosition(termId, termOffset, positionBitsToShift, initialTermId);

            final int length = validateAndComputeLength(lengthOne, lengthTwo);
            if (position < limit)
            {
                if (length <= maxPayloadLength)
                {
                    newPosition = appendUnfragmentedMessage(
                        termBuffer,
                        tailCounterOffset,
                        bufferOne,
                        offsetOne,
                        lengthOne,
                        bufferTwo,
                        offsetTwo,
                        lengthTwo,
                        reservedValueSupplier);
                }
                else
                {
                    checkMaxMessageLength(length);
                    newPosition = appendFragmentedMessage(
                        termBuffer,
                        tailCounterOffset,
                        bufferOne,
                        offsetOne,
                        lengthOne,
                        bufferTwo,
                        offsetTwo,
                        lengthTwo,
                        reservedValueSupplier);
                }
            }
            else
            {
                newPosition = backPressureStatus(position, length);
            }
        }

        return newPosition;
    }

    /**
     * Non-blocking publish by gathering buffer vectors into a message.
     *
     * @param vectors               which make up the message.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     */
    public long offer(final DirectBufferVector[] vectors, final ReservedValueSupplier reservedValueSupplier)
    {
        final int length = DirectBufferVector.validateAndComputeLength(vectors);
        long newPosition = CLOSED;

        if (!isClosed)
        {
            final long limit = positionLimit.getVolatile();
            final int termCount = activeTermCount(logMetaDataBuffer);
            final int index = indexByTermCount(termCount);
            final UnsafeBuffer termBuffer = termBuffers[index];
            final int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (index * SIZE_OF_LONG);
            final long rawTail = logMetaDataBuffer.getLongVolatile(tailCounterOffset);
            final int termOffset = termOffset(rawTail, termBuffer.capacity());
            final int termId = termId(rawTail);

            if (termCount != (termId - initialTermId))
            {
                return ADMIN_ACTION;
            }

            final long position = computePosition(termId, termOffset, positionBitsToShift, initialTermId);

            if (position < limit)
            {
                if (length <= maxPayloadLength)
                {
                    newPosition = appendUnfragmentedMessage(
                        termBuffer, tailCounterOffset, vectors, length, reservedValueSupplier);
                }
                else
                {
                    checkMaxMessageLength(length);
                    newPosition = appendFragmentedMessage(
                        termBuffer, tailCounterOffset, vectors, length, reservedValueSupplier);
                }
            }
            else
            {
                newPosition = backPressureStatus(position, length);
            }
        }

        return newPosition;
    }

    /**
     * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
     * Once the message has been written then {@link BufferClaim#commit()} should be called thus making it available.
     * <p>
     * <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
     * If the claim is held for more than the aeron.publication.unblock.timeout system property then the driver will
     * assume the publication thread is dead and will unblock the claim thus allowing other threads to make progress or
     * to reach end-of-stream (EOS).
     * <pre>{@code
     *     final BufferClaim bufferClaim = new BufferClaim(); // Can be stored and reused to avoid allocation
     *
     *     if (publication.tryClaim(messageLength, bufferClaim) > 0L)
     *     {
     *         try
     *         {
     *              final MutableDirectBuffer buffer = bufferClaim.buffer();
     *              final int offset = bufferClaim.offset();
     *
     *              // Work with buffer directly or wrap with a flyweight
     *         }
     *         finally
     *         {
     *             bufferClaim.commit();
     *         }
     *     }
     * }</pre>
     *
     * @param length      of the range to claim, in bytes.
     * @param bufferClaim to be populated if the claim succeeds.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     * @throws IllegalArgumentException if the length is greater than {@link #maxPayloadLength()} within an MTU.
     * @see BufferClaim#commit()
     * @see BufferClaim#abort()
     */
    public long tryClaim(final int length, final BufferClaim bufferClaim)
    {
        checkPayloadLength(length);
        long newPosition = CLOSED;

        if (!isClosed)
        {
            final long limit = positionLimit.getVolatile();
            final int termCount = activeTermCount(logMetaDataBuffer);
            final int index = indexByTermCount(termCount);
            final UnsafeBuffer termBuffer = termBuffers[index];
            final int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (index * SIZE_OF_LONG);
            final long rawTail = logMetaDataBuffer.getLongVolatile(tailCounterOffset);
            final int termOffset = termOffset(rawTail, termBuffer.capacity());
            final int termId = termId(rawTail);

            if (termCount != (termId - initialTermId))
            {
                return ADMIN_ACTION;
            }

            final long position = computePosition(termId, termOffset, positionBitsToShift, initialTermId);

            if (position < limit)
            {
                newPosition = claim(termBuffer, tailCounterOffset, length, bufferClaim);
            }
            else
            {
                newPosition = backPressureStatus(position, length);
            }
        }

        return newPosition;
    }

    private long appendUnfragmentedMessage(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int termLength = termBuffer.capacity();

        final long rawTail = logMetaDataBuffer.getAndAddLong(tailCounterOffset, alignedLength);
        final int termId = termId(rawTail);
        final int termOffset = termOffset(rawTail, termLength);

        final int resultingOffset = termOffset + alignedLength;
        final long position = computePosition(termId, resultingOffset, positionBitsToShift, initialTermId);
        if (resultingOffset > termLength)
        {
            return handleEndOfLog(termBuffer, termLength, termId, termOffset, position);
        }
        else
        {
            headerWriter.write(termBuffer, termOffset, frameLength, termId);
            termBuffer.putBytes(termOffset + HEADER_LENGTH, buffer, offset, length);

            if (null != reservedValueSupplier)
            {
                final long reservedValue = reservedValueSupplier.get(termBuffer, termOffset, frameLength);
                termBuffer.putLong(termOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
            }

            frameLengthOrdered(termBuffer, termOffset, frameLength);
        }

        return position;
    }

    private long appendFragmentedMessage(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int framedLength = computeFragmentedFrameLength(length, maxPayloadLength);
        final int termLength = termBuffer.capacity();

        final long rawTail = logMetaDataBuffer.getAndAddLong(tailCounterOffset, framedLength);
        final int termId = termId(rawTail);
        final int termOffset = termOffset(rawTail, termLength);

        final int resultingOffset = termOffset + framedLength;
        final long position = computePosition(termId, resultingOffset, positionBitsToShift, initialTermId);
        if (resultingOffset > termLength)
        {
            return handleEndOfLog(termBuffer, termLength, termId, termOffset, position);
        }
        else
        {
            int frameOffset = termOffset;
            byte flags = BEGIN_FRAG_FLAG;
            int remaining = length;

            do
            {
                final int bytesToWrite = Math.min(remaining, maxPayloadLength);
                final int frameLength = bytesToWrite + HEADER_LENGTH;
                final int alignedLength = align(frameLength, FRAME_ALIGNMENT);

                headerWriter.write(termBuffer, frameOffset, frameLength, termId);
                termBuffer.putBytes(
                    frameOffset + HEADER_LENGTH,
                    buffer,
                    offset + (length - remaining),
                    bytesToWrite);

                if (remaining <= maxPayloadLength)
                {
                    flags |= END_FRAG_FLAG;
                }

                frameFlags(termBuffer, frameOffset, flags);

                if (null != reservedValueSupplier)
                {
                    final long reservedValue = reservedValueSupplier.get(termBuffer, frameOffset, frameLength);
                    termBuffer.putLong(frameOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
                }

                frameLengthOrdered(termBuffer, frameOffset, frameLength);

                flags = 0;
                frameOffset += alignedLength;
                remaining -= bytesToWrite;
            }
            while (remaining > 0);
        }

        return position;
    }

    private long appendUnfragmentedMessage(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final DirectBuffer bufferOne,
        final int offsetOne,
        final int lengthOne,
        final DirectBuffer bufferTwo,
        final int offsetTwo,
        final int lengthTwo,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int frameLength = lengthOne + lengthTwo + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int termLength = termBuffer.capacity();

        final long rawTail = logMetaDataBuffer.getAndAddLong(tailCounterOffset, alignedLength);
        final int termId = termId(rawTail);
        final int termOffset = termOffset(rawTail, termLength);

        final int resultingOffset = termOffset + alignedLength;
        final long position = computePosition(termId, resultingOffset, positionBitsToShift, initialTermId);
        if (resultingOffset > termLength)
        {
            return handleEndOfLog(termBuffer, termLength, termId, termOffset, position);
        }
        else
        {
            headerWriter.write(termBuffer, termOffset, frameLength, termId);
            termBuffer.putBytes(termOffset + HEADER_LENGTH, bufferOne, offsetOne, lengthOne);
            termBuffer.putBytes(termOffset + HEADER_LENGTH + lengthOne, bufferTwo, offsetTwo, lengthTwo);

            if (null != reservedValueSupplier)
            {
                final long reservedValue = reservedValueSupplier.get(termBuffer, termOffset, frameLength);
                termBuffer.putLong(termOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
            }

            frameLengthOrdered(termBuffer, termOffset, frameLength);
        }

        return position;
    }

    private long appendFragmentedMessage(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final DirectBuffer bufferOne,
        final int offsetOne,
        final int lengthOne,
        final DirectBuffer bufferTwo,
        final int offsetTwo,
        final int lengthTwo,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int length = lengthOne + lengthTwo;
        final int framedLength = computeFragmentedFrameLength(length, maxPayloadLength);
        final int termLength = termBuffer.capacity();

        final long rawTail = logMetaDataBuffer.getAndAddLong(tailCounterOffset, framedLength);
        final int termId = termId(rawTail);
        final int termOffset = termOffset(rawTail, termLength);

        final int resultingOffset = termOffset + framedLength;
        final long position = computePosition(termId, resultingOffset, positionBitsToShift, initialTermId);
        if (resultingOffset > termLength)
        {
            return handleEndOfLog(termBuffer, termLength, termId, termOffset, position);
        }
        else
        {
            int frameOffset = termOffset;
            byte flags = BEGIN_FRAG_FLAG;
            int remaining = length;
            int positionOne = 0;
            int positionTwo = 0;

            do
            {
                final int bytesToWrite = Math.min(remaining, maxPayloadLength);
                final int frameLength = bytesToWrite + HEADER_LENGTH;
                final int alignedLength = align(frameLength, FRAME_ALIGNMENT);

                headerWriter.write(termBuffer, frameOffset, frameLength, termId);

                int bytesWritten = 0;
                int payloadOffset = frameOffset + HEADER_LENGTH;
                do
                {
                    final int remainingOne = lengthOne - positionOne;
                    if (remainingOne > 0)
                    {
                        final int numBytes = Math.min(bytesToWrite - bytesWritten, remainingOne);
                        termBuffer.putBytes(payloadOffset, bufferOne, offsetOne + positionOne, numBytes);

                        bytesWritten += numBytes;
                        payloadOffset += numBytes;
                        positionOne += numBytes;
                    }
                    else
                    {
                        final int numBytes = Math.min(bytesToWrite - bytesWritten, lengthTwo - positionTwo);
                        termBuffer.putBytes(payloadOffset, bufferTwo, offsetTwo + positionTwo, numBytes);

                        bytesWritten += numBytes;
                        payloadOffset += numBytes;
                        positionTwo += numBytes;
                    }
                }
                while (bytesWritten < bytesToWrite);

                if (remaining <= maxPayloadLength)
                {
                    flags |= END_FRAG_FLAG;
                }

                frameFlags(termBuffer, frameOffset, flags);

                if (null != reservedValueSupplier)
                {
                    final long reservedValue = reservedValueSupplier.get(termBuffer, frameOffset, frameLength);
                    termBuffer.putLong(frameOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
                }

                frameLengthOrdered(termBuffer, frameOffset, frameLength);

                flags = 0;
                frameOffset += alignedLength;
                remaining -= bytesToWrite;
            }
            while (remaining > 0);
        }

        return position;
    }

    private long appendUnfragmentedMessage(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final DirectBufferVector[] vectors,
        final int length,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int termLength = termBuffer.capacity();

        final long rawTail = logMetaDataBuffer.getAndAddLong(tailCounterOffset, alignedLength);
        final int termId = termId(rawTail);
        final int termOffset = termOffset(rawTail, termLength);

        final int resultingOffset = termOffset + alignedLength;
        final long position = computePosition(termId, resultingOffset, positionBitsToShift, initialTermId);
        if (resultingOffset > termLength)
        {
            return handleEndOfLog(termBuffer, termLength, termId, termOffset, position);
        }
        else
        {
            headerWriter.write(termBuffer, termOffset, frameLength, termId);

            int offset = termOffset + HEADER_LENGTH;
            for (final DirectBufferVector vector : vectors)
            {
                termBuffer.putBytes(offset, vector.buffer(), vector.offset(), vector.length());
                offset += vector.length();
            }

            if (null != reservedValueSupplier)
            {
                final long reservedValue = reservedValueSupplier.get(termBuffer, termOffset, frameLength);
                termBuffer.putLong(termOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
            }

            frameLengthOrdered(termBuffer, termOffset, frameLength);
        }

        return position;
    }

    private long appendFragmentedMessage(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final DirectBufferVector[] vectors,
        final int length,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int framedLength = computeFragmentedFrameLength(length, maxPayloadLength);
        final int termLength = termBuffer.capacity();

        final long rawTail = logMetaDataBuffer.getAndAddLong(tailCounterOffset, framedLength);
        final int termId = termId(rawTail);
        final int termOffset = termOffset(rawTail, termLength);

        final int resultingOffset = termOffset + framedLength;
        final long position = computePosition(termId, resultingOffset, positionBitsToShift, initialTermId);
        if (resultingOffset > termLength)
        {
            return handleEndOfLog(termBuffer, termLength, termId, termOffset, position);
        }
        else
        {
            int frameOffset = termOffset;
            byte flags = BEGIN_FRAG_FLAG;
            int remaining = length;
            int vectorIndex = 0;
            int vectorOffset = 0;

            do
            {
                final int bytesToWrite = Math.min(remaining, maxPayloadLength);
                final int frameLength = bytesToWrite + HEADER_LENGTH;
                final int alignedLength = align(frameLength, FRAME_ALIGNMENT);

                headerWriter.write(termBuffer, frameOffset, frameLength, termId);

                int bytesWritten = 0;
                int payloadOffset = frameOffset + HEADER_LENGTH;
                do
                {
                    final DirectBufferVector vector = vectors[vectorIndex];
                    final int vectorRemaining = vector.length() - vectorOffset;
                    final int numBytes = Math.min(bytesToWrite - bytesWritten, vectorRemaining);

                    termBuffer.putBytes(payloadOffset, vector.buffer(), vector.offset() + vectorOffset, numBytes);

                    bytesWritten += numBytes;
                    payloadOffset += numBytes;
                    vectorOffset += numBytes;

                    if (vectorRemaining <= numBytes)
                    {
                        vectorIndex++;
                        vectorOffset = 0;
                    }
                }
                while (bytesWritten < bytesToWrite);

                if (remaining <= maxPayloadLength)
                {
                    flags |= END_FRAG_FLAG;
                }

                frameFlags(termBuffer, frameOffset, flags);

                if (null != reservedValueSupplier)
                {
                    final long reservedValue = reservedValueSupplier.get(termBuffer, frameOffset, frameLength);
                    termBuffer.putLong(frameOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
                }

                frameLengthOrdered(termBuffer, frameOffset, frameLength);

                flags = 0;
                frameOffset += alignedLength;
                remaining -= bytesToWrite;
            }
            while (remaining > 0);
        }

        return position;
    }

    private long claim(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final int length,
        final BufferClaim bufferClaim)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int termLength = termBuffer.capacity();

        final long rawTail = logMetaDataBuffer.getAndAddLong(tailCounterOffset, alignedLength);
        final int termId = termId(rawTail);
        final int termOffset = termOffset(rawTail, termLength);

        final int resultingOffset = termOffset + alignedLength;
        final long position = computePosition(termId, resultingOffset, positionBitsToShift, initialTermId);
        if (resultingOffset > termLength)
        {
            return handleEndOfLog(termBuffer, termLength, termId, termOffset, position);
        }
        else
        {
            headerWriter.write(termBuffer, termOffset, frameLength, termId);
            bufferClaim.wrap(termBuffer, termOffset, frameLength);
        }

        return position;
    }

    private long handleEndOfLog(
        final UnsafeBuffer termBuffer,
        final int termLength,
        final int termId,
        final int termOffset,
        final long position)
    {
        if (termOffset < termLength)
        {
            final int paddingLength = termLength - termOffset;
            headerWriter.write(termBuffer, termOffset, paddingLength, termId);
            frameType(termBuffer, termOffset, PADDING_FRAME_TYPE);
            frameLengthOrdered(termBuffer, termOffset, paddingLength);
        }

        if (position >= maxPossiblePosition)
        {
            return MAX_POSITION_EXCEEDED;
        }

        rotateLog(logMetaDataBuffer, termId - initialTermId, termId);

        return ADMIN_ACTION;
    }
}
