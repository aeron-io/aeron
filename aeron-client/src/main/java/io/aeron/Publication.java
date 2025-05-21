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

import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.HeaderWriter;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.status.ChannelEndpointStatus;
import io.aeron.status.LocalSocketAddressStatus;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.ReadablePosition;

import java.util.List;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.BitUtil.align;

/**
 * Aeron publisher API for sending messages to subscribers of a given channel and streamId pair. {@link Publication}s
 * are created via the {@link Aeron#addPublication(String, int)} {@link Aeron#addExclusivePublication(String, int)}
 * methods, and messages are sent via one of the {@link #offer(DirectBuffer)} methods.
 * <p>
 * The APIs used for tryClaim and offer are non-blocking.
 * <p>
 * <b>Note:</b> All methods are threadsafe except offer and tryClaim for the subclass
 * {@link ExclusivePublication}. In the case of {@link ConcurrentPublication} all methods are threadsafe.
 *
 * @see ConcurrentPublication
 * @see ExclusivePublication
 * @see Aeron#addPublication(String, int)
 * @see Aeron#addExclusivePublication(String, int)
 */
public abstract class Publication implements AutoCloseable
{
    /**
     * The publication is not connected to a subscriber, this can be an intermittent state as subscribers come and go.
     */
    public static final long NOT_CONNECTED = -1;

    /**
     * The offer failed due to back pressure from the subscribers preventing further transmission.
     */
    public static final long BACK_PRESSURED = -2;

    /**
     * The offer failed due to an administration action and should be retried.
     * The action is an operation such as log rotation which is likely to have succeeded by the next retry attempt.
     */
    public static final long ADMIN_ACTION = -3;

    /**
     * The {@link Publication} has been closed and should no longer be used.
     */
    public static final long CLOSED = -4;

    /**
     * The offer failed due to reaching the maximum position of the stream given term buffer length times the total
     * possible number of terms.
     * <p>
     * If this happens then the publication should be closed and a new one added. To make it less likely to happen then
     * increase the term buffer length.
     */
    public static final long MAX_POSITION_EXCEEDED = -5;

    final long originalRegistrationId;
    final long registrationId;
    final long maxPossiblePosition;
    final int channelStatusId;
    final int streamId;
    final int sessionId;
    final int maxMessageLength;
    final int maxFramedLength;
    final int initialTermId;
    final int maxPayloadLength;
    final int positionBitsToShift;
    final int termBufferLength;
    volatile boolean isClosed = false;
    boolean revokeOnClose = false;

    final ReadablePosition positionLimit;
    final UnsafeBuffer[] termBuffers;
    final UnsafeBuffer logMetaDataBuffer;
    final HeaderWriter headerWriter;
    final LogBuffers logBuffers;
    final ClientConductor conductor;
    final String channel;

    Publication(
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
        final UnsafeBuffer logMetaDataBuffer = logBuffers.metaDataBuffer();
        this.termBufferLength = logBuffers.termLength();
        this.maxMessageLength = FrameDescriptor.computeMaxMessageLength(termBufferLength);
        this.maxPayloadLength = LogBufferDescriptor.mtuLength(logMetaDataBuffer) - HEADER_LENGTH;
        this.maxFramedLength = computeFragmentedFrameLength(maxMessageLength, maxPayloadLength);
        this.maxPossiblePosition = termBufferLength * (1L << 31);
        this.conductor = clientConductor;
        this.channel = channel;
        this.streamId = streamId;
        this.sessionId = sessionId;
        this.initialTermId = LogBufferDescriptor.initialTermId(logMetaDataBuffer);
        this.termBuffers = logBuffers.duplicateTermBuffers();
        this.logMetaDataBuffer = logMetaDataBuffer;
        this.logBuffers = logBuffers;
        this.originalRegistrationId = originalRegistrationId;
        this.registrationId = registrationId;
        this.positionLimit = positionLimit;
        this.channelStatusId = channelStatusId;
        this.positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termBufferLength);
        this.headerWriter = HeaderWriter.newInstance(defaultFrameHeader(logMetaDataBuffer));

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            final int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (i * SIZE_OF_LONG);
            logMetaDataBuffer.boundsCheck(tailCounterOffset, SIZE_OF_LONG);
        }
    }

    /**
     * Number of bits to right shift a position to get a term count for how far the stream has progressed.
     *
     * @return of bits to right shift a position to get a term count for how far the stream has progressed.
     */
    public int positionBitsToShift()
    {
        return positionBitsToShift;
    }

    /**
     * Get the length in bytes for each term partition in the log buffer.
     *
     * @return the length in bytes for each term partition in the log buffer.
     */
    public int termBufferLength()
    {
        return termBufferLength;
    }

    /**
     * The maximum possible position this stream can reach due to its term buffer length.
     * <p>
     * Maximum possible position is term-length times 2^31 in bytes.
     *
     * @return the maximum possible position this stream can reach due to it term buffer length.
     */
    public long maxPossiblePosition()
    {
        return maxPossiblePosition;
    }

    /**
     * Media address for delivery to the channel.
     *
     * @return Media address for delivery to the channel.
     */
    public String channel()
    {
        return channel;
    }

    /**
     * Stream identity for scoping within the channel media address.
     *
     * @return Stream identity for scoping within the channel media address.
     */
    public int streamId()
    {
        return streamId;
    }

    /**
     * Session under which messages are published. Identifies this Publication instance. Sessions are unique across
     * all active publications on a driver instance.
     *
     * @return the session id for this publication.
     */
    public int sessionId()
    {
        return sessionId;
    }

    /**
     * The initial term id assigned when this {@link Publication} was created. This can be used to determine how many
     * terms have passed since creation.
     *
     * @return the initial term id.
     */
    public int initialTermId()
    {
        return initialTermId;
    }

    /**
     * Maximum message length supported in bytes. Messages may be made of multiple fragments if greater than
     * MTU length.
     *
     * @return maximum message length supported in bytes.
     */
    public int maxMessageLength()
    {
        return maxMessageLength;
    }

    /**
     * Maximum length of a message payload that fits within a message fragment.
     * <p>
     * This is the MTU length minus the message fragment header length.
     *
     * @return maximum message fragment payload length.
     */
    public int maxPayloadLength()
    {
        return maxPayloadLength;
    }

    /**
     * Get the registration used to register this Publication with the media driver by the first publisher.
     *
     * @return original registration id
     */
    public long originalRegistrationId()
    {
        return originalRegistrationId;
    }

    /**
     * Is this Publication the original instance added to the driver? If not then it was added after another client
     * has already added the publication.
     *
     * @return true if this instance is the first added otherwise false.
     */
    public boolean isOriginal()
    {
        return originalRegistrationId == registrationId;
    }

    /**
     * Get the registration id used to register this Publication with the media driver.
     * <p>
     * If this value is different from the {@link #originalRegistrationId()} then a previous active registration exists.
     *
     * @return registration id
     */
    public long registrationId()
    {
        return registrationId;
    }

    /**
     * Has the {@link Publication} seen an active Subscriber recently?
     *
     * @return true if this {@link Publication} has recently seen an active subscriber otherwise false.
     */
    public boolean isConnected()
    {
        return !isClosed && LogBufferDescriptor.isConnected(logMetaDataBuffer);
    }

    /**
     * Remove resources used by this Publication when there are no more references.
     * <p>
     * Publications are reference counted and are only truly closed when the ref count reaches zero.
     */
    public void close()
    {
        if (!isClosed)
        {
            conductor.removePublication(this);
        }
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    public boolean isClosed()
    {
        return isClosed;
    }

    /**
     * Get the status of the media channel for this Publication.
     * <p>
     * The status will be {@link io.aeron.status.ChannelEndpointStatus#ERRORED} if a socket exception occurs on setup
     * and {@link io.aeron.status.ChannelEndpointStatus#ACTIVE} if all is well.
     *
     * @return status for the channel as one of the constants from {@link ChannelEndpointStatus} with it being
     * {@link ChannelEndpointStatus#NO_ID_ALLOCATED} if the publication is closed.
     * @see io.aeron.status.ChannelEndpointStatus
     */
    public long channelStatus()
    {
        if (isClosed)
        {
            return ChannelEndpointStatus.NO_ID_ALLOCATED;
        }

        return conductor.channelStatus(channelStatusId);
    }

    /**
     * Get the counter used to represent the channel status for this publication.
     *
     * @return the counter used to represent the channel status for this publication.
     */
    public int channelStatusId()
    {
        return channelStatusId;
    }

    /**
     * Fetches the local socket address for this publication. If the channel is not
     * {@link io.aeron.status.ChannelEndpointStatus#ACTIVE}, then this will return an empty list.
     * <p>
     * The format is as follows:
     * <br>
     * <br>
     * IPv4: <code>ip address:port</code>
     * <br>
     * IPv6: <code>[ip6 address]:port</code>
     * <br>
     * <br>
     * This is to match the formatting used in the Aeron URI. For publications this will be the control address and
     * is likely to only contain a single entry.
     *
     * @return local socket addresses for this publication.
     * @see #channelStatus()
     */
    public List<String> localSocketAddresses()
    {
        return LocalSocketAddressStatus.findAddresses(conductor.countersReader(), channelStatus(), channelStatusId);
    }

    /**
     * Get the current position to which the publication has advanced for this stream.
     *
     * @return the current position to which the publication has advanced for this stream or {@link #CLOSED}.
     */
    public long position()
    {
        if (isClosed)
        {
            return CLOSED;
        }

        final long rawTail = rawTailVolatile(logMetaDataBuffer);
        final int termOffset = termOffset(rawTail, termBufferLength);

        return computePosition(termId(rawTail), termOffset, positionBitsToShift, initialTermId);
    }

    /**
     * Get the position limit beyond which this {@link Publication} will be back pressured.
     * <p>
     * This should only be used as a guide to determine when back pressure is likely to be applied.
     *
     * @return the position limit beyond which this {@link Publication} will be back pressured.
     */
    public long positionLimit()
    {
        if (isClosed)
        {
            return CLOSED;
        }

        return positionLimit.getVolatile();
    }

    /**
     * Get the counter id for the position limit after which the publication will be back pressured.
     *
     * @return the counter id for the position limit after which the publication will be back pressured.
     */
    public int positionLimitId()
    {
        return positionLimit.id();
    }

    /**
     * Available window for offering into a publication before the {@link #positionLimit()} is reached.
     *
     * @return window for offering into a publication before the {@link #positionLimit()} is reached. If
     * the publication is closed then {@link #CLOSED} will be returned.
     */
    public abstract long availableWindow();

    /**
     * Non-blocking publish of a buffer containing a message.
     *
     * @param buffer containing message.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     */
    public final long offer(final DirectBuffer buffer)
    {
        return offer(buffer, 0, buffer.capacity());
    }

    /**
     * Non-blocking publish of a partial buffer containing a message.
     *
     * @param buffer containing message.
     * @param offset offset in the buffer at which the encoded message begins.
     * @param length in bytes of the encoded message.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     */
    public final long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        return offer(buffer, offset, length, null);
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
    public abstract long offer(
        DirectBuffer buffer, int offset, int length, ReservedValueSupplier reservedValueSupplier);

    /**
     * Non-blocking publish of a message composed of two parts, e.g. a header and encapsulated payload.
     *
     * @param bufferOne containing the first part of the message.
     * @param offsetOne at which the first part of the message begins.
     * @param lengthOne of the first part of the message.
     * @param bufferTwo containing the second part of the message.
     * @param offsetTwo at which the second part of the message begins.
     * @param lengthTwo of the second part of the message.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     */
    public final long offer(
        final DirectBuffer bufferOne,
        final int offsetOne,
        final int lengthOne,
        final DirectBuffer bufferTwo,
        final int offsetTwo,
        final int lengthTwo)
    {
        return offer(bufferOne, offsetOne, lengthOne, bufferTwo, offsetTwo, lengthTwo, null);
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
    public abstract long offer(
        DirectBuffer bufferOne,
        int offsetOne,
        int lengthOne,
        DirectBuffer bufferTwo,
        int offsetTwo,
        int lengthTwo,
        ReservedValueSupplier reservedValueSupplier);

    /**
     * Non-blocking publish by gathering buffer vectors into a message.
     *
     * @param vectors which make up the message.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     */
    public final long offer(final DirectBufferVector[] vectors)
    {
        return offer(vectors, null);
    }

    /**
     * Non-blocking publish by gathering buffer vectors into a message.
     *
     * @param vectors               which make up the message.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     */
    public abstract long offer(DirectBufferVector[] vectors, ReservedValueSupplier reservedValueSupplier);

    /**
     * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
     * Once the message has been written then {@link BufferClaim#commit()} should be called thus making it available.
     * A claim length cannot be greater than {@link #maxPayloadLength()}.
     * <p>
     * <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
     * If the claim is held for more than the aeron.publication.unblock.timeout system property then the driver will
     * assume the publication thread is dead and will unblock the claim thus allowing other threads to make progress
     * for {@link ConcurrentPublication} and other claims to be sent to reach end-of-stream (EOS).
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
    public abstract long tryClaim(int length, BufferClaim bufferClaim);

    /**
     * Add a destination manually to a multi-destination-cast Publication.
     *
     * @param endpointChannel for the destination to add.
     */
    public void addDestination(final String endpointChannel)
    {
        if (isClosed)
        {
            throw new AeronException("Publication is closed");
        }

        conductor.addDestination(originalRegistrationId, endpointChannel);
    }

    /**
     * Remove a previously added destination manually from a multi-destination-cast Publication.
     *
     * @param endpointChannel for the destination to remove.
     */
    public void removeDestination(final String endpointChannel)
    {
        if (isClosed)
        {
            throw new AeronException("Publication is closed");
        }

        conductor.removeDestination(originalRegistrationId, endpointChannel);
    }

    /**
     * Remove a previously added destination manually from a multi-destination-cast Publication.
     *
     * @param registrationId for the destination to remove.
     */
    public void removeDestination(final long registrationId)
    {
        if (isClosed)
        {
            throw new AeronException("Publication is closed");
        }

        conductor.removeDestination(originalRegistrationId, registrationId);
    }

    /**
     * Asynchronously add a destination manually to a multi-destination-cast Publication.
     * <p>
     * Errors will be delivered asynchronously to the {@link Aeron.Context#errorHandler()}. Completion can be
     * tracked by passing the returned correlation id to {@link Aeron#isCommandActive(long)}.
     *
     * @param endpointChannel for the destination to add.
     * @return the correlationId for the command.
     */
    public long asyncAddDestination(final String endpointChannel)
    {
        if (isClosed)
        {
            throw new AeronException("Publication is closed");
        }

        return conductor.asyncAddDestination(registrationId, endpointChannel);
    }

    /**
     * Asynchronously remove a previously added destination from a multi-destination-cast Publication.
     * <p>
     * Errors will be delivered asynchronously to the {@link Aeron.Context#errorHandler()}. Completion can be
     * tracked by passing the returned correlation id to {@link Aeron#isCommandActive(long)}.
     *
     * @param endpointChannel for the destination to remove.
     * @return the correlationId for the command.
     */
    public long asyncRemoveDestination(final String endpointChannel)
    {
        if (isClosed)
        {
            throw new AeronException("Publication is closed");
        }

        return conductor.asyncRemoveDestination(registrationId, endpointChannel);
    }

    /**
     * Asynchronously remove a previously added destination from a multi-destination-cast Publication by registrationId.
     * <p>
     * Errors will be delivered asynchronously to the {@link Aeron.Context#errorHandler()}. Completion can be
     * tracked by passing the returned correlation id to {@link Aeron#isCommandActive(long)}.
     *
     * @param destinationRegistrationId for the destination to remove.
     * @return the correlationId for the command.
     */
    public long asyncRemoveDestination(final long destinationRegistrationId)
    {
        if (isClosed)
        {
            throw new AeronException("Publication is closed");
        }

        return conductor.asyncRemoveDestination(registrationId, destinationRegistrationId);
    }

    void internalClose()
    {
        isClosed = true;
    }

    LogBuffers logBuffers()
    {
        return logBuffers;
    }

    final long backPressureStatus(final long currentPosition, final int messageLength)
    {
        if ((currentPosition + align(messageLength + HEADER_LENGTH, FRAME_ALIGNMENT)) >= maxPossiblePosition)
        {
            return MAX_POSITION_EXCEEDED;
        }

        if (LogBufferDescriptor.isConnected(logMetaDataBuffer))
        {
            return BACK_PRESSURED;
        }

        return NOT_CONNECTED;
    }

    final void checkPositiveLength(final int length)
    {
        if (length < 0)
        {
            throw new IllegalArgumentException("invalid length: " + length);
        }
    }

    final void checkPayloadLength(final int length)
    {
        if (length < 0)
        {
            throw new IllegalArgumentException("invalid length: " + length);
        }

        if (length > maxPayloadLength)
        {
            throw new IllegalArgumentException(
                "claim exceeds maxPayloadLength of " + maxPayloadLength + ", length=" + length);
        }
    }

    final void checkMaxMessageLength(final int length)
    {
        if (length > maxMessageLength)
        {
            throw new IllegalArgumentException(
                "message exceeds maxMessageLength of " + maxMessageLength + ", length=" + length);
        }
    }

    static int validateAndComputeLength(final int lengthOne, final int lengthTwo)
    {
        if (lengthOne < 0)
        {
            throw new IllegalArgumentException("lengthOne < 0: " + lengthOne);
        }

        if (lengthTwo < 0)
        {
            throw new IllegalArgumentException("lengthTwo < 0: " + lengthTwo);
        }

        final int totalLength = lengthOne + lengthTwo;
        if (totalLength < 0)
        {
            throw new IllegalArgumentException("overflow totalLength=" + totalLength);
        }

        return totalLength;
    }

    /**
     * Returns a string representation of a position. Generally used for errors. If the position is a valid error then
     * String name of the error will be returned. If the value is 0 or greater the text will be "NONE". If the position
     * is negative, but not a known error code then "UNKNOWN" will be returned.
     *
     * @param position position value returned from a call to offer.
     * @return String representation of the error.
     */
    public static String errorString(final long position)
    {
        if (MAX_POSITION_EXCEEDED <= position && position < 0)
        {
            final int errorCode = (int)position;
            switch (errorCode)
            {
                case (int)NOT_CONNECTED:
                    return "NOT_CONNECTED";
                case (int)BACK_PRESSURED:
                    return "BACK_PRESSURED";
                case (int)ADMIN_ACTION:
                    return "ADMIN_ACTION";
                case (int)CLOSED:
                    return "CLOSED";
                case (int)MAX_POSITION_EXCEEDED:
                    return "MAX_POSITION_EXCEEDED";
                default:
                    return "UNKNOWN";
            }
        }
        else if (0 <= position)
        {
            return "NONE";
        }
        else
        {
            return "UNKNOWN";
        }
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "Publication{" +
            "originalRegistrationId=" + originalRegistrationId +
            ", registrationId=" + registrationId +
            ", isClosed=" + isClosed +
            ", isConnected=" + isConnected() +
            ", initialTermId=" + initialTermId +
            ", termBufferLength=" + termBufferLength +
            ", sessionId=" + sessionId +
            ", streamId=" + streamId +
            ", channel='" + channel + '\'' +
            ", position=" + position() +
            '}';
    }
}
