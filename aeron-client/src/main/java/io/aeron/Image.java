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

import io.aeron.logbuffer.BlockHandler;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.logbuffer.RawBlockHandler;
import io.aeron.logbuffer.TermBlockScanner;
import org.agrona.BitUtil;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.Position;

import java.nio.channels.FileChannel;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.*;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.DataHeaderFlyweight.TERM_ID_FIELD_OFFSET;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Represents a replicated {@link Publication} from a which matches a {@link Subscription}.
 * Each {@link Image} identifies a source {@link Publication} by {@link #sessionId()}.
 * <p>
 * By default, fragmented messages are not reassembled before delivery. If an application must
 * receive whole messages, whether they were fragmented or not, then the Subscriber
 * should be created with a {@link FragmentAssembler} or a custom implementation.
 * <p>
 * It is an application's responsibility to {@link #poll} the {@link Image} for new messages.
 * <p>
 * <b>Note:</b>Images are not threadsafe and should not be shared between subscribers.
 */
public final class Image
{
    private final long correlationId;
    private final long joinPosition;
    private long finalPosition;
    private final int sessionId;
    private final int initialTermId;
    private final int termLengthMask;
    private final int positionBitsToShift;

    private long eosPosition = Long.MAX_VALUE;
    private boolean isEos;
    private volatile boolean isClosed;
    private volatile boolean isRevoked;

    private final Position subscriberPosition;
    private final UnsafeBuffer[] termBuffers;
    private final Header header;
    private final ErrorHandler errorHandler;
    private final LogBuffers logBuffers;
    private final String sourceIdentity;
    private final Subscription subscription;

    /**
     * Construct a new image over a log to represent a stream of messages from a {@link Publication}.
     *
     * @param subscription       to which this {@link Image} belongs.
     * @param sessionId          of the stream of messages.
     * @param subscriberPosition for indicating the position of the subscriber in the stream.
     * @param logBuffers         containing the stream of messages.
     * @param errorHandler       to be called if an error occurs when polling for messages.
     * @param sourceIdentity     of the source sending the stream of messages.
     * @param correlationId      of the request to the media driver.
     */
    public Image(
        final Subscription subscription,
        final int sessionId,
        final Position subscriberPosition,
        final LogBuffers logBuffers,
        final ErrorHandler errorHandler,
        final String sourceIdentity,
        final long correlationId)
    {
        this.subscription = subscription;
        this.sessionId = sessionId;
        this.subscriberPosition = subscriberPosition;
        this.logBuffers = logBuffers;
        this.errorHandler = errorHandler;
        this.sourceIdentity = sourceIdentity;
        this.correlationId = correlationId;
        this.joinPosition = subscriberPosition.get();

        termBuffers = logBuffers.duplicateTermBuffers();

        final int termLength = logBuffers.termLength();
        this.termLengthMask = termLength - 1;
        this.positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);
        this.initialTermId = LogBufferDescriptor.initialTermId(logBuffers.metaDataBuffer());
        header = new Header(initialTermId, positionBitsToShift, this);
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
        return termLengthMask + 1;
    }

    /**
     * The sessionId for the steam of messages. Sessions are unique within a {@link Subscription} and unique across
     * all {@link Publication}s from a {@link #sourceIdentity()}.
     *
     * @return the sessionId for the steam of messages.
     */
    public int sessionId()
    {
        return sessionId;
    }

    /**
     * The source identity of the sending publisher as an abstract concept appropriate for the media.
     *
     * @return source identity of the sending publisher as an abstract concept appropriate for the media.
     */
    public String sourceIdentity()
    {
        return sourceIdentity;
    }

    /**
     * The length in bytes of the MTU (Maximum Transmission Unit) the Sender used for the datagram.
     *
     * @return length in bytes of the MTU (Maximum Transmission Unit) the Sender used for the datagram.
     */
    public int mtuLength()
    {
        return LogBufferDescriptor.mtuLength(logBuffers.metaDataBuffer());
    }

    /**
     * The initial term at which the stream started for this session.
     *
     * @return the initial term id.
     */
    public int initialTermId()
    {
        return initialTermId;
    }

    /**
     * The correlationId for identification of the image with the media driver.
     *
     * @return the correlationId for identification of the image with the media driver.
     */
    public long correlationId()
    {
        return correlationId;
    }

    /**
     * Get the {@link Subscription} to which this {@link Image} belongs.
     *
     * @return the {@link Subscription} to which this {@link Image} belongs.
     */
    public Subscription subscription()
    {
        return subscription;
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
     * Get the position the subscriber joined this stream at.
     *
     * @return the position the subscriber joined this stream at.
     */
    public long joinPosition()
    {
        return joinPosition;
    }

    /**
     * The position this {@link Image} has been consumed to by the subscriber.
     *
     * @return the position this {@link Image} has been consumed to by the subscriber.
     */
    public long position()
    {
        if (isClosed)
        {
            return finalPosition;
        }

        return subscriberPosition.get();
    }

    /**
     * Set the subscriber position for this {@link Image} to indicate where it has been consumed to.
     *
     * @param newPosition for the consumption point.
     */
    public void position(final long newPosition)
    {
        if (!isClosed)
        {
            validatePosition(newPosition);
            subscriberPosition.setRelease(newPosition);
        }
    }

    /**
     * The counter id for the subscriber position counter.
     *
     * @return the id for the subscriber position counter.
     */
    public int subscriberPositionId()
    {
        return subscriberPosition.id();
    }

    /**
     * Is the current consumed position at the end of the stream?
     *
     * @return true if at the end of the stream or false if not.
     */
    public boolean isEndOfStream()
    {
        if (isClosed)
        {
            return isEos;
        }

        return subscriberPosition.get() >= LogBufferDescriptor.endOfStreamPosition(logBuffers.metaDataBuffer());
    }

    /**
     * The position the stream reached when EOS was received from the publisher. The position will be
     * {@link Long#MAX_VALUE} until the stream ends and EOS is set.
     *
     * @return position the stream reached when EOS was received from the publisher.
     */
    public long endOfStreamPosition()
    {
        if (isClosed)
        {
            return eosPosition;
        }

        return LogBufferDescriptor.endOfStreamPosition(logBuffers.metaDataBuffer());
    }

    /**
     * Count of observed active transports within the image liveness timeout.
     * <p>
     * If the image is closed, then this is 0. This may also be 0 if no actual datagrams have arrived. IPC
     * Images also will be 0.
     *
     * @return count of active transports - 0 if Image is closed, no datagrams yet, or IPC.
     */
    public int activeTransportCount()
    {
        if (isClosed)
        {
            return 0;
        }

        return LogBufferDescriptor.activeTransportCount(logBuffers.metaDataBuffer());
    }

    /**
     * Has the associated publication been revoked?
     *
     * @return true if the associated publication was revoked otherwise false.
     */
    public boolean isPublicationRevoked()
    {
        if (isClosed)
        {
            return isRevoked;
        }

        return LogBufferDescriptor.isPublicationRevoked(logBuffers.metaDataBuffer());
    }

    /**
     * The {@link FileChannel} to the raw log of the Image.
     *
     * @return the {@link FileChannel} to the raw log of the Image.
     */
    public FileChannel fileChannel()
    {
        return logBuffers.fileChannel();
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered to the {@link FragmentHandler} up to a limited number of fragments as specified.
     * <p>
     * Use a {@link FragmentAssembler} to assemble messages which span multiple fragments.
     *
     * @param fragmentHandler to which message fragments are delivered.
     * @param fragmentLimit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     * @see FragmentAssembler
     * @see ImageFragmentAssembler
     */
    public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
    {
        if (isClosed)
        {
            return 0;
        }

        int fragmentsRead = 0;
        final long initialPosition = subscriberPosition.get();
        final int initialOffset = (int)initialPosition & termLengthMask;
        int offset = initialOffset;
        final UnsafeBuffer termBuffer = activeTermBuffer(initialPosition);
        final int capacity = termBuffer.capacity();
        final Header header = this.header;
        header.buffer(termBuffer);

        try
        {
            while (fragmentsRead < fragmentLimit && offset < capacity && !isClosed)
            {
                final int frameLength = frameLengthVolatile(termBuffer, offset);
                if (frameLength <= 0)
                {
                    break;
                }

                final int frameOffset = offset;
                offset += BitUtil.align(frameLength, FRAME_ALIGNMENT);

                if (!isPaddingFrame(termBuffer, frameOffset))
                {
                    ++fragmentsRead;
                    header.offset(frameOffset);
                    fragmentHandler.onFragment(
                        termBuffer, frameOffset + HEADER_LENGTH, frameLength - HEADER_LENGTH, header);
                }
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
        }
        finally
        {
            final long newPosition = initialPosition + (offset - initialOffset);
            if (newPosition > initialPosition && !isClosed)
            {
                subscriberPosition.setRelease(newPosition);
            }
        }

        return fragmentsRead;
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered to the {@link ControlledFragmentHandler} up to a limited number of fragments as specified.
     * <p>
     * Use a {@link ControlledFragmentAssembler} to assemble messages which span multiple fragments.
     *
     * @param handler       to which message fragments are delivered.
     * @param fragmentLimit for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     * @see ControlledFragmentAssembler
     * @see ImageControlledFragmentAssembler
     */
    public int controlledPoll(final ControlledFragmentHandler handler, final int fragmentLimit)
    {
        if (isClosed)
        {
            return 0;
        }

        int fragmentsRead = 0;
        long initialPosition = subscriberPosition.get();
        int initialOffset = (int)initialPosition & termLengthMask;
        int offset = initialOffset;
        final UnsafeBuffer termBuffer = activeTermBuffer(initialPosition);
        final int capacity = termBuffer.capacity();
        final Header header = this.header;
        header.buffer(termBuffer);

        try
        {
            while (fragmentsRead < fragmentLimit && offset < capacity && !isClosed)
            {
                final int length = frameLengthVolatile(termBuffer, offset);
                if (length <= 0)
                {
                    break;
                }

                final int frameOffset = offset;
                final int alignedLength = BitUtil.align(length, FRAME_ALIGNMENT);
                offset += alignedLength;

                if (isPaddingFrame(termBuffer, frameOffset))
                {
                    continue;
                }

                ++fragmentsRead;
                header.offset(frameOffset);

                final Action action = handler.onFragment(
                    termBuffer, frameOffset + HEADER_LENGTH, length - HEADER_LENGTH, header);

                if (ABORT == action)
                {
                    --fragmentsRead;
                    offset -= alignedLength;
                    break;
                }

                if (BREAK == action)
                {
                    break;
                }

                if (COMMIT == action)
                {
                    initialPosition += (offset - initialOffset);
                    initialOffset = offset;
                    if (!isClosed)
                    {
                        subscriberPosition.setRelease(initialPosition);
                    }
                }
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
        }
        finally
        {
            final long resultingPosition = initialPosition + (offset - initialOffset);
            if (resultingPosition > initialPosition && !isClosed)
            {
                subscriberPosition.setRelease(resultingPosition);
            }
        }

        return fragmentsRead;
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered to the {@link FragmentHandler} up to a limited number of fragments as specified or
     * the maximum position specified.
     * <p>
     * Use a {@link FragmentAssembler} to assemble messages which span multiple fragments.
     *
     * @param handler       to which message fragments are delivered.
     * @param limitPosition to consume messages up to.
     * @param fragmentLimit for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     * @see FragmentAssembler
     * @see ImageFragmentAssembler
     */
    public int boundedPoll(final FragmentHandler handler, final long limitPosition, final int fragmentLimit)
    {
        if (isClosed)
        {
            return 0;
        }

        final long initialPosition = subscriberPosition.get();
        if (initialPosition >= limitPosition)
        {
            return 0;
        }

        int fragmentsRead = 0;
        final int initialOffset = (int)initialPosition & termLengthMask;
        int offset = initialOffset;
        final UnsafeBuffer termBuffer = activeTermBuffer(initialPosition);
        final int limitOffset = (int)Math.min(termBuffer.capacity(), (limitPosition - initialPosition) + offset);
        final Header header = this.header;
        header.buffer(termBuffer);

        try
        {
            while (fragmentsRead < fragmentLimit && offset < limitOffset && !isClosed)
            {
                final int length = frameLengthVolatile(termBuffer, offset);
                if (length <= 0)
                {
                    break;
                }

                final int frameOffset = offset;
                final int alignedLength = BitUtil.align(length, FRAME_ALIGNMENT);
                offset += alignedLength;

                if (isPaddingFrame(termBuffer, frameOffset))
                {
                    continue;
                }

                ++fragmentsRead;
                header.offset(frameOffset);
                handler.onFragment(termBuffer, frameOffset + HEADER_LENGTH, length - HEADER_LENGTH, header);
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
        }
        finally
        {
            final long resultingPosition = initialPosition + (offset - initialOffset);
            if (resultingPosition > initialPosition && !isClosed)
            {
                subscriberPosition.setRelease(resultingPosition);
            }
        }

        return fragmentsRead;
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered to the {@link ControlledFragmentHandler} up to a limited number of fragments as specified or
     * the maximum position specified.
     * <p>
     * Use a {@link ControlledFragmentAssembler} to assemble messages which span multiple fragments.
     *
     * @param handler       to which message fragments are delivered.
     * @param limitPosition to consume messages up to.
     * @param fragmentLimit for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     * @see ControlledFragmentAssembler
     * @see ImageControlledFragmentAssembler
     */
    public int boundedControlledPoll(
        final ControlledFragmentHandler handler, final long limitPosition, final int fragmentLimit)
    {
        if (isClosed)
        {
            return 0;
        }

        final Position subscriberPosition = this.subscriberPosition;
        long initialPosition = subscriberPosition.get();
        if (initialPosition >= limitPosition)
        {
            return 0;
        }

        int fragmentsRead = 0;
        int initialOffset = (int)initialPosition & termLengthMask;
        int offset = initialOffset;
        final UnsafeBuffer termBuffer = activeTermBuffer(initialPosition);
        final int limitOffset = (int)Math.min(termBuffer.capacity(), (limitPosition - initialPosition) + offset);
        final Header header = this.header;
        header.buffer(termBuffer);

        try
        {
            while (fragmentsRead < fragmentLimit && offset < limitOffset && !isClosed)
            {
                final int length = frameLengthVolatile(termBuffer, offset);
                if (length <= 0)
                {
                    break;
                }

                final int frameOffset = offset;
                final int alignedLength = BitUtil.align(length, FRAME_ALIGNMENT);
                offset += alignedLength;

                if (isPaddingFrame(termBuffer, frameOffset))
                {
                    continue;
                }

                ++fragmentsRead;
                header.offset(frameOffset);

                final Action action = handler.onFragment(
                    termBuffer, frameOffset + HEADER_LENGTH, length - HEADER_LENGTH, header);

                if (ABORT == action)
                {
                    --fragmentsRead;
                    offset -= alignedLength;
                    break;
                }

                if (BREAK == action)
                {
                    break;
                }

                if (COMMIT == action)
                {
                    initialPosition += (offset - initialOffset);
                    initialOffset = offset;
                    if (!isClosed)
                    {
                        subscriberPosition.setRelease(initialPosition);
                    }
                }
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
        }
        finally
        {
            final long resultingPosition = initialPosition + (offset - initialOffset);
            if (resultingPosition > initialPosition && !isClosed)
            {
                subscriberPosition.setRelease(resultingPosition);
            }
        }

        return fragmentsRead;
    }

    /**
     * Peek for new messages in a stream by scanning forward from an initial position. If new messages are found then
     * they will be delivered to the {@link ControlledFragmentHandler} up to a limited position.
     * <p>
     * Use a {@link ControlledFragmentAssembler} to assemble messages which span multiple fragments. Scans must also
     * start at the beginning of a message so that the assembler is reset.
     *
     * @param initialPosition from which to peek forward.
     * @param handler         to which message fragments are delivered.
     * @param limitPosition   up to which can be scanned.
     * @return the resulting position after the scan terminates which is a complete message.
     * @see ControlledFragmentAssembler
     * @see ImageControlledFragmentAssembler
     */
    public long controlledPeek(
        final long initialPosition, final ControlledFragmentHandler handler, final long limitPosition)
    {
        if (isClosed)
        {
            return initialPosition;
        }

        validatePosition(initialPosition);
        if (initialPosition >= limitPosition)
        {
            return initialPosition;
        }

        int initialOffset = (int)initialPosition & termLengthMask;
        int offset = initialOffset;
        long position = initialPosition;
        final UnsafeBuffer termBuffer = activeTermBuffer(initialPosition);
        final Header header = this.header;
        final int limitOffset = (int)Math.min(termBuffer.capacity(), (limitPosition - initialPosition) + offset);
        header.buffer(termBuffer);
        long resultingPosition = initialPosition;

        try
        {
            while (offset < limitOffset && !isClosed)
            {
                final int length = frameLengthVolatile(termBuffer, offset);
                if (length <= 0)
                {
                    break;
                }

                final int frameOffset = offset;
                offset += BitUtil.align(length, FRAME_ALIGNMENT);

                if (isPaddingFrame(termBuffer, frameOffset))
                {
                    position += (offset - initialOffset);
                    initialOffset = offset;
                    resultingPosition = position;

                    continue;
                }

                header.offset(frameOffset);

                final Action action = handler.onFragment(
                    termBuffer, frameOffset + HEADER_LENGTH, length - HEADER_LENGTH, header);

                if (ABORT == action)
                {
                    break;
                }

                position += (offset - initialOffset);
                initialOffset = offset;

                if ((header.flags() & END_FRAG_FLAG) == END_FRAG_FLAG)
                {
                    resultingPosition = position;
                }

                if (BREAK == action)
                {
                    break;
                }
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
        }

        return resultingPosition;
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered to the {@link BlockHandler} up to a limited number of bytes.
     * <p>
     * A scan will terminate if a padding frame is encountered. If first frame in a scan is padding then a block
     * for the padding is notified. If the padding comes after the first frame in a scan then the scan terminates
     * at the offset the padding frame begins. Padding frames are delivered singularly in a block.
     * <p>
     * Padding frames may be for a greater range than the limit offset but only the header needs to be valid so
     * relevant length of the frame is {@link io.aeron.protocol.DataHeaderFlyweight#HEADER_LENGTH}.
     *
     * @param handler          to which block is delivered.
     * @param blockLengthLimit up to which a block may be in length.
     * @return the number of bytes that have been consumed.
     */
    public int blockPoll(final BlockHandler handler, final int blockLengthLimit)
    {
        if (isClosed)
        {
            return 0;
        }

        final long position = subscriberPosition.get();
        final int offset = (int)position & termLengthMask;
        final int limitOffset = Math.min(offset + blockLengthLimit, termLengthMask + 1);
        final UnsafeBuffer termBuffer = activeTermBuffer(position);
        final int resultingOffset = TermBlockScanner.scan(termBuffer, offset, limitOffset);
        final int length = resultingOffset - offset;

        if (resultingOffset > offset)
        {
            try
            {
                final int termId = termBuffer.getInt(offset + TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
                handler.onBlock(termBuffer, offset, length, sessionId, termId);
            }
            catch (final Exception ex)
            {
                errorHandler.onError(ex);
            }
            finally
            {
                if (!isClosed)
                {
                    subscriberPosition.setRelease(position + length);
                }
            }
        }

        return length;
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered to the {@link RawBlockHandler} up to a limited number of bytes.
     * <p>
     * This method is useful for operations like bulk archiving a stream to file.
     * <p>
     * A scan will terminate if a padding frame is encountered. If first frame in a scan is padding then a block
     * for the padding is notified. If the padding comes after the first frame in a scan then the scan terminates
     * at the offset the padding frame begins. Padding frames are delivered singularly in a block.
     * <p>
     * Padding frames may be for a greater range than the limit offset but only the header needs to be valid so
     * relevant length of the frame is {@link io.aeron.protocol.DataHeaderFlyweight#HEADER_LENGTH}.
     *
     * @param handler          to which block is delivered.
     * @param blockLengthLimit up to which a block may be in length.
     * @return the number of bytes that have been consumed.
     */
    public int rawPoll(final RawBlockHandler handler, final int blockLengthLimit)
    {
        if (isClosed)
        {
            return 0;
        }

        final long position = subscriberPosition.get();
        final int offset = (int)position & termLengthMask;
        final int activeIndex = LogBufferDescriptor.indexByPosition(position, positionBitsToShift);
        final UnsafeBuffer termBuffer = termBuffers[activeIndex];
        final int capacity = termBuffer.capacity();
        final int limitOffset = Math.min(offset + blockLengthLimit, capacity);
        final int resultingOffset = TermBlockScanner.scan(termBuffer, offset, limitOffset);
        final int length = resultingOffset - offset;

        if (resultingOffset > offset)
        {
            try
            {
                final long fileOffset = ((long)capacity * activeIndex) + offset;
                final int termId = termBuffer.getInt(offset + TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);

                handler.onBlock(logBuffers.fileChannel(), fileOffset, termBuffer, offset, length, sessionId, termId);
            }
            catch (final Exception ex)
            {
                errorHandler.onError(ex);
            }
            finally
            {
                if (!isClosed)
                {
                    subscriberPosition.setRelease(position + length);
                }
            }
        }

        return length;
    }

    /**
     * Reject this image.
     *
     * @param reason a String indicating the reason why this image is being rejected.
     */
    public void reject(final String reason)
    {
        subscription.rejectImage(correlationId, position(), reason);
    }

    private UnsafeBuffer activeTermBuffer(final long position)
    {
        return termBuffers[LogBufferDescriptor.indexByPosition(position, positionBitsToShift)];
    }

    private void validatePosition(final long position)
    {
        final long currentPosition = subscriberPosition.get();
        final long limitPosition = (currentPosition - (currentPosition & termLengthMask)) + termLengthMask + 1;
        if (position < currentPosition || position > limitPosition)
        {
            throw new IllegalArgumentException(
                position + " position out of range: " + currentPosition + "-" + limitPosition);
        }

        if (0 != (position & (FRAME_ALIGNMENT - 1)))
        {
            throw new IllegalArgumentException(position + " position not aligned to FRAME_ALIGNMENT");
        }
    }

    LogBuffers logBuffers()
    {
        return logBuffers;
    }

    void close()
    {
        finalPosition = subscriberPosition.getVolatile();
        eosPosition = LogBufferDescriptor.endOfStreamPosition(logBuffers.metaDataBuffer());
        isEos = finalPosition >= eosPosition;
        isRevoked = LogBufferDescriptor.isPublicationRevoked(logBuffers.metaDataBuffer());
        isClosed = true;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "Image{" +
            "correlationId=" + correlationId +
            ", sessionId=" + sessionId +
            ", isClosed=" + isClosed +
            ", isEos=" + isEndOfStream() +
            ", initialTermId=" + initialTermId +
            ", termLength=" + termBufferLength() +
            ", joinPosition=" + joinPosition +
            ", position=" + position() +
            ", endOfStreamPosition=" + endOfStreamPosition() +
            ", activeTransportCount=" + activeTransportCount() +
            ", sourceIdentity='" + sourceIdentity + '\'' +
            ", subscription=" + subscription +
            '}';
    }
}
