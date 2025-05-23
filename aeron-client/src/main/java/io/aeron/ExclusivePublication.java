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
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.ReadablePosition;

import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.BitUtil.align;

@SuppressWarnings("unused")
abstract class ExclusivePublicationLhsPadding extends Publication
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;

    ExclusivePublicationLhsPadding(
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
}

abstract class ExclusivePublicationValues extends ExclusivePublicationLhsPadding
{
    int termOffset;
    int termId;
    int activePartitionIndex;
    long termBeginPosition;

    ExclusivePublicationValues(
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
}

/**
 * Aeron publisher API for sending messages to subscribers of a given channel and streamId pair. ExclusivePublications
 * each get their own session id so multiple can be concurrently active on the same media driver as independent streams.
 * <p>
 * {@link ExclusivePublication}s are created via the {@link Aeron#addExclusivePublication(String, int)} method,
 * and messages are sent via one of the {@link #offer(DirectBuffer)} methods, or a
 * {@link #tryClaim(int, BufferClaim)} and {@link BufferClaim#commit()} method combination.
 * <p>
 * {@link ExclusivePublication}s have the potential to provide greater throughput than the default {@link Publication}
 * which supports concurrent access.
 * <p>
 * The APIs for tryClaim and offer are non-blocking.
 * <p>
 * <b>Note:</b> Instances are NOT threadsafe for offer and tryClaim methods but are for the others.
 *
 * @see Aeron#addExclusivePublication(String, int)
 */
public final class ExclusivePublication extends ExclusivePublicationValues
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;

    ExclusivePublication(
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

        final UnsafeBuffer logMetaDataBuffer = logBuffers.metaDataBuffer();
        final int termCount = activeTermCount(logMetaDataBuffer);
        final int index = indexByTermCount(termCount);
        activePartitionIndex = index;

        final long rawTail = rawTail(logMetaDataBuffer, index);
        termId = LogBufferDescriptor.termId(rawTail);
        termOffset = LogBufferDescriptor.termOffset(rawTail);
        termBeginPosition = computeTermBeginPosition(termId, positionBitsToShift, initialTermId);
    }

    /**
     * Mark the publication to be revoked when {@link #close()} is called.  See  {@link #revoke()}
     */
    public void revokeOnClose()
    {
        revokeOnClose = true;
    }

    /**
     * Immediately revoke and {@link #close()} the publication.
     *
     * Revoking disposes of resources as soon as possible. On the publication side the log buffer won't linger,
     * while on the subscription side the image will go unavailable without requiring all data to be drained.
     * Hence, it should be used only when it's known that all subscribers have received all the data,
     * or if it doesn't matter if they have.
     */
    public void revoke()
    {
        if (!isClosed)
        {
            revokeOnClose = true;
            close();
        }
    }

    /**
     * {@inheritDoc}
     */
    public long position()
    {
        if (isClosed)
        {
            return CLOSED;
        }

        return termBeginPosition + termOffset;
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

        return positionLimit.getVolatile() - (termBeginPosition + termOffset);
    }

    /**
     * The current term-id of the publication.
     *
     * @return the current term-id of the publication.
     */
    public int termId()
    {
        return termId;
    }

    /**
     * The current term-offset of the publication.
     *
     * @return the current term-offset of the publication.
     */
    public int termOffset()
    {
        return termOffset;
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
            final long position = termBeginPosition + termOffset;

            if (position < limit)
            {
                final int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (activePartitionIndex * SIZE_OF_LONG);
                final UnsafeBuffer termBuffer = termBuffers[activePartitionIndex];
                final int result;

                if (length <= maxPayloadLength)
                {
                    checkPositiveLength(length);
                    result = appendUnfragmentedMessage(
                        tailCounterOffset, termBuffer, buffer, offset, length, reservedValueSupplier);
                }
                else
                {
                    checkMaxMessageLength(length);
                    result = appendFragmentedMessage(
                        termBuffer, tailCounterOffset, buffer, offset, length, reservedValueSupplier);
                }

                newPosition = newPosition(result);
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
            final long position = termBeginPosition + termOffset;
            final int length = validateAndComputeLength(lengthOne, lengthTwo);

            if (position < limit)
            {
                final int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (activePartitionIndex * SIZE_OF_LONG);
                final UnsafeBuffer termBuffer = termBuffers[activePartitionIndex];
                final int result;

                if (length <= maxPayloadLength)
                {
                    checkPositiveLength(length);
                    result = appendUnfragmentedMessage(
                        termBuffer,
                        tailCounterOffset,
                        bufferOne, offsetOne, lengthOne,
                        bufferTwo, offsetTwo, lengthTwo,
                        reservedValueSupplier);
                }
                else
                {
                    checkMaxMessageLength(length);
                    result = appendFragmentedMessage(
                        termBuffer,
                        tailCounterOffset,
                        bufferOne, offsetOne, lengthOne,
                        bufferTwo, offsetTwo, lengthTwo,
                        maxPayloadLength,
                        reservedValueSupplier);
                }

                newPosition = newPosition(result);
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
            final long position = termBeginPosition + termOffset;

            if (position < limit)
            {
                final int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (activePartitionIndex * SIZE_OF_LONG);
                final UnsafeBuffer termBuffer = termBuffers[activePartitionIndex];
                final int result;

                if (length <= maxPayloadLength)
                {
                    result = appendUnfragmentedMessage(
                        termBuffer, tailCounterOffset, vectors, length, reservedValueSupplier);
                }
                else
                {
                    checkMaxMessageLength(length);
                    result = appendFragmentedMessage(
                        termBuffer, tailCounterOffset, vectors, length, reservedValueSupplier);
                }

                newPosition = newPosition(result);
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
     * Once the message has been written then {@link BufferClaim#commit()} should be called thus making it
     * available.
     * <p>
     * <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
     * If the claim is held after the publication is closed, or the client dies, then it will be unblocked to reach
     * end-of-stream (EOS).
     * <pre>{@code
     *     final BufferClaim bufferClaim = new BufferClaim();
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
            final long position = termBeginPosition + termOffset;

            if (position < limit)
            {
                final int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (activePartitionIndex * SIZE_OF_LONG);
                final UnsafeBuffer termBuffer = termBuffers[activePartitionIndex];
                final int result = claim(termBuffer, tailCounterOffset, length, bufferClaim);

                newPosition = newPosition(result);
            }
            else
            {
                newPosition = backPressureStatus(position, length);
            }
        }

        return newPosition;
    }

    /**
     * Append a padding record to log of a given length to make up the log to a position.
     *
     * @param length of the range to claim, in bytes.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     * @throws IllegalArgumentException if the length is greater than {@link #maxMessageLength() framed}.
     */
    public long appendPadding(final int length)
    {
        if (length > maxFramedLength)
        {
            throw new IllegalArgumentException(
                "padding exceeds maxFramedLength of " + maxFramedLength + ", length=" + length);
        }

        long newPosition = CLOSED;
        if (!isClosed)
        {
            final long limit = positionLimit.getVolatile();
            final long position = termBeginPosition + termOffset;

            if (position < limit)
            {
                checkPositiveLength(length);
                final int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (activePartitionIndex * SIZE_OF_LONG);
                final UnsafeBuffer termBuffer = termBuffers[activePartitionIndex];
                final int result = appendPadding(termBuffer, tailCounterOffset, length);

                newPosition = newPosition(result);
            }
            else
            {
                newPosition = backPressureStatus(position, length);
            }
        }

        return newPosition;
    }

    /**
     * Offer a block of pre-formatted message fragments directly into the current term.
     *
     * @param buffer containing the pre-formatted block of message fragments.
     * @param offset offset in the buffer at which the first fragment begins.
     * @param length in bytes of the encoded block.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     * @throws IllegalArgumentException if the length is greater than remaining size of the current term.
     * @throws IllegalArgumentException if the first frame within the block is not properly formatted, i.e. if the
     *                                  {@code streamId} is not equal to the value returned by the {@link #streamId()}
     *                                  method or if the {@code sessionId} is not equal to the value returned by the
     *                                  {@link #sessionId()} method or if the frame type is not equal to the
     *                                  {@link io.aeron.protocol.HeaderFlyweight#HDR_TYPE_DATA}.
     */
    public long offerBlock(final MutableDirectBuffer buffer, final int offset, final int length)
    {
        if (isClosed)
        {
            return CLOSED;
        }

        if (termOffset >= termBufferLength)
        {
            rotateTerm();
        }

        final long limit = positionLimit.getVolatile();
        final long position = termBeginPosition + termOffset;

        if (position < limit)
        {
            checkBlockLength(length);
            checkFirstFrame(buffer, offset);

            final int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (activePartitionIndex * SIZE_OF_LONG);
            final UnsafeBuffer termBuffer = termBuffers[activePartitionIndex];
            final int result = appendBlock(termBuffer, tailCounterOffset, buffer, offset, length);

            return newPosition(result);
        }
        else
        {
            return backPressureStatus(position, length);
        }
    }

    private void checkBlockLength(final int length)
    {
        final int remaining = termBufferLength - termOffset;
        if (length > remaining)
        {
            throw new IllegalArgumentException(
                "invalid block length " + length + ", remaining space in term is " + remaining);
        }
    }

    private void checkFirstFrame(final MutableDirectBuffer buffer, final int offset)
    {
        final int frameType = HDR_TYPE_DATA;
        final int blockTermOffset = buffer.getInt(offset + TERM_OFFSET_FIELD_OFFSET, LITTLE_ENDIAN);
        final int blockSessionId = buffer.getInt(offset + SESSION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
        final int blockStreamId = buffer.getInt(offset + STREAM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
        final int blockTermId = buffer.getInt(offset + TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
        final int blockFrameType = buffer.getShort(offset + TYPE_FIELD_OFFSET, LITTLE_ENDIAN) & 0xFFFF;

        if (blockTermOffset != termOffset ||
            blockSessionId != sessionId ||
            blockStreamId != streamId ||
            blockTermId != termId ||
            frameType != blockFrameType)
        {
            throw new IllegalArgumentException("improperly formatted block:" +
                " termOffset=" + blockTermOffset + " (expected=" + termOffset + ")," +
                " sessionId=" + blockSessionId + " (expected=" + sessionId + ")," +
                " streamId=" + blockStreamId + " (expected=" + streamId + ")," +
                " termId=" + blockTermId + " (expected=" + termId + ")," +
                " frameType=" + blockFrameType + " (expected=" + frameType + ")");
        }
    }

    private long newPosition(final int resultingOffset)
    {
        if (resultingOffset > 0)
        {
            termOffset = resultingOffset;
            return termBeginPosition + resultingOffset;
        }

        if ((termBeginPosition + termBufferLength) >= maxPossiblePosition)
        {
            return MAX_POSITION_EXCEEDED;
        }

        rotateTerm();

        return ADMIN_ACTION;
    }

    private void rotateTerm()
    {
        final int nextIndex = nextPartitionIndex(activePartitionIndex);
        final int nextTermId = termId + 1;

        activePartitionIndex = nextIndex;
        termOffset = 0;
        termId = nextTermId;
        termBeginPosition += termBufferLength;

        final int termCount = nextTermId - initialTermId;

        initialiseTailWithTermId(logMetaDataBuffer, nextIndex, nextTermId);
        activeTermCountOrdered(logMetaDataBuffer, termCount);
    }

    private int handleEndOfLog(final UnsafeBuffer termBuffer, final int termLength)
    {
        if (termOffset < termLength)
        {
            final int offset = termOffset;
            final int paddingLength = termLength - offset;
            headerWriter.write(termBuffer, offset, paddingLength, termId);
            frameType(termBuffer, offset, PADDING_FRAME_TYPE);
            frameLengthOrdered(termBuffer, offset, paddingLength);
        }

        return -1;
    }

    private int appendUnfragmentedMessage(
        final int tailCounterOffset,
        final UnsafeBuffer termBuffer,
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int length,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int termLength = termBuffer.capacity();

        int resultingOffset = termOffset + alignedLength;
        logMetaDataBuffer.putLongRelease(tailCounterOffset, packTail(termId, resultingOffset));

        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLog(termBuffer, termLength);
        }
        else
        {
            headerWriter.write(termBuffer, termOffset, frameLength, termId);
            termBuffer.putBytes(termOffset + HEADER_LENGTH, srcBuffer, srcOffset, length);

            if (null != reservedValueSupplier)
            {
                final long reservedValue = reservedValueSupplier.get(termBuffer, termOffset, frameLength);
                termBuffer.putLong(termOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
            }

            frameLengthOrdered(termBuffer, termOffset, frameLength);
        }

        return resultingOffset;
    }

    private int appendFragmentedMessage(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int length,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int framedLength = computeFragmentedFrameLength(length, maxPayloadLength);
        final int termLength = termBuffer.capacity();

        int resultingOffset = termOffset + framedLength;
        logMetaDataBuffer.putLongRelease(tailCounterOffset, packTail(termId, resultingOffset));

        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLog(termBuffer, termLength);
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
                    srcBuffer,
                    srcOffset + (length - remaining),
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

        return resultingOffset;
    }

    private int appendUnfragmentedMessage(
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

        int resultingOffset = termOffset + alignedLength;
        logMetaDataBuffer.putLongRelease(tailCounterOffset, packTail(termId, resultingOffset));

        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLog(termBuffer, termLength);
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

        return resultingOffset;
    }

    private int appendFragmentedMessage(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final DirectBuffer bufferOne,
        final int offsetOne,
        final int lengthOne,
        final DirectBuffer bufferTwo,
        final int offsetTwo,
        final int lengthTwo,
        final int maxPayloadLength,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int length = lengthOne + lengthTwo;
        final int framedLength = computeFragmentedFrameLength(length, maxPayloadLength);
        final int termLength = termBuffer.capacity();

        int resultingOffset = termOffset + framedLength;
        logMetaDataBuffer.putLongRelease(tailCounterOffset, packTail(termId, resultingOffset));

        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLog(termBuffer, termLength);
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

        return resultingOffset;
    }

    private int appendUnfragmentedMessage(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final DirectBufferVector[] vectors,
        final int length,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int termLength = termBuffer.capacity();

        int resultingOffset = termOffset + alignedLength;
        logMetaDataBuffer.putLongRelease(tailCounterOffset, packTail(termId, resultingOffset));

        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLog(termBuffer, termLength);
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

        return resultingOffset;
    }

    private int appendFragmentedMessage(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final DirectBufferVector[] vectors,
        final int length,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int framedLength = computeFragmentedFrameLength(length, maxPayloadLength);
        final int termLength = termBuffer.capacity();

        int resultingOffset = termOffset + framedLength;
        logMetaDataBuffer.putLongRelease(tailCounterOffset, packTail(termId, resultingOffset));

        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLog(termBuffer, termLength);
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

        return resultingOffset;
    }

    int claim(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final int length,
        final BufferClaim bufferClaim)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int termLength = termBuffer.capacity();

        int resultingOffset = termOffset + alignedLength;
        logMetaDataBuffer.putLongRelease(tailCounterOffset, packTail(termId, resultingOffset));

        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLog(termBuffer, termLength);
        }
        else
        {
            headerWriter.write(termBuffer, termOffset, frameLength, termId);
            bufferClaim.wrap(termBuffer, termOffset, frameLength);
        }

        return resultingOffset;
    }

    private int appendPadding(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final int length)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int termLength = termBuffer.capacity();

        int resultingOffset = termOffset + alignedLength;
        logMetaDataBuffer.putLongRelease(tailCounterOffset, packTail(termId, resultingOffset));

        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLog(termBuffer, termLength);
        }
        else
        {
            headerWriter.write(termBuffer, termOffset, frameLength, termId);
            frameType(termBuffer, termOffset, PADDING_FRAME_TYPE);
            frameLengthOrdered(termBuffer, termOffset, frameLength);
        }

        return resultingOffset;
    }

    private int appendBlock(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final MutableDirectBuffer buffer,
        final int offset,
        final int length)
    {
        final int resultingOffset = termOffset + length;

        logMetaDataBuffer.putLongRelease(tailCounterOffset, packTail(termId, resultingOffset));

        termBuffer.putBytes(termOffset + HEADER_LENGTH, buffer, offset + HEADER_LENGTH, length - HEADER_LENGTH);
        termBuffer.putLong(termOffset + 24, buffer.getLong(offset + 24));
        termBuffer.putLong(termOffset + 16, buffer.getLong(offset + 16));
        termBuffer.putLong(termOffset + 8, buffer.getLong(offset + 8));
        termBuffer.putLongRelease(termOffset, buffer.getLong(offset));

        return resultingOffset;
    }
}
