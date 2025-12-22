/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archive;

import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.archive.checksum.Checksum;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.CloseHelper;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.FrameDescriptor.frameLength;
import static io.aeron.logbuffer.FrameDescriptor.frameSessionId;
import static io.aeron.logbuffer.FrameDescriptor.frameType;
import static io.aeron.protocol.DataHeaderFlyweight.HDR_TYPE_DATA;
import static io.aeron.protocol.DataHeaderFlyweight.HDR_TYPE_PAD;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET;
import static io.aeron.protocol.DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET;
import static io.aeron.protocol.DataHeaderFlyweight.streamId;
import static io.aeron.protocol.DataHeaderFlyweight.termId;
import static io.aeron.protocol.DataHeaderFlyweight.termOffset;
import static java.lang.Math.min;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.file.StandardOpenOption.READ;
import static org.agrona.BitUtil.align;

/**
 * A replay session with a client which works through the required request response flow and streaming of recorded data.
 * The {@link ArchiveConductor} will initiate a session on receiving a ReplayRequest
 * (see {@link io.aeron.archive.codecs.ReplayRequestDecoder}).
 * <p>
 * The session will:
 * <ul>
 * <li>Validate request parameters and respond with appropriate error if unable to replay.</li>
 * <li>Wait for replay publication to connect to the subscriber. If no subscriber appears within
 * {@link Archive.Configuration#CONNECT_TIMEOUT_PROP_NAME} the session will terminate and respond with an error.</li>
 * <li>Once the replay publication is connected an OK response to control client will be sent.</li>
 * <li>Stream recorded data into the publication {@link ExclusivePublication}.</li>
 * <li>If the replay is aborted part way through, send a ReplayAborted message and terminate.</li>
 * </ul>
 */
class ReplaySession implements Session, AutoCloseable
{
    @SuppressWarnings("JavadocVariable")
    enum State
    {
        INIT, REPLAY, INACTIVE, DONE
    }

    private static final EnumSet<StandardOpenOption> FILE_OPTIONS = EnumSet.of(READ);

    private final long connectDeadlineMs;
    private final long correlationId;
    private final long sessionId;
    private final long recordingId;
    private final long startPosition;
    private long replayPosition;
    private long stopPosition;
    private long replayLimit;
    private long segmentFileBasePosition;
    private int termBaseSegmentOffset;
    private int termOffset;
    private final int streamId;
    private final int termLength;
    private final int segmentLength;

    private final long replayBufferAddress;
    private final Checksum checksum;

    private final ExclusivePublication publication;
    private final ControlSession controlSession;
    private final CachedEpochClock epochClock;

    private final NanoClock nanoClock;
    final ArchiveConductor.Replayer replayer;
    private final File archiveDir;
    private final CountersReader countersReader;
    private final Counter limitPosition;
    private final UnsafeBuffer replayBuffer;
    private FileChannel fileChannel;
    private File segmentFile;
    private State state = State.INIT;
    private String errorMessage = null;
    private boolean revokePublication;
    private volatile boolean isAborted;

    ReplaySession(
        final long correlationId,
        final long recordingId,
        final long replayPosition,
        final long replayLength,
        final long startPosition,
        final long stopPosition,
        final int segmentFileLength,
        final int termBufferLength,
        final int streamId,
        final long replaySessionId,
        final long connectTimeoutMs,
        final ControlSession controlSession,
        final UnsafeBuffer replayBuffer,
        final File archiveDir,
        final CachedEpochClock epochClock,
        final NanoClock nanoClock,
        final ExclusivePublication publication,
        final CountersReader countersReader,
        final Counter replayLimitPosition,
        final Checksum checksum,
        final ArchiveConductor.Replayer replayer)
    {
        this.controlSession = controlSession;
        this.sessionId = replaySessionId;
        this.correlationId = correlationId;
        this.recordingId = recordingId;
        this.segmentLength = segmentFileLength;
        this.termLength = termBufferLength;
        this.streamId = streamId;
        this.epochClock = epochClock;
        this.nanoClock = nanoClock;
        this.archiveDir = archiveDir;
        this.publication = publication;
        this.countersReader = countersReader;
        this.limitPosition = replayLimitPosition;
        this.replayBuffer = replayBuffer;
        this.replayBufferAddress = replayBuffer.addressOffset();
        this.checksum = checksum;
        this.startPosition = startPosition;
        this.stopPosition = stopPosition;
        this.replayer = replayer;

        segmentFileBasePosition = AeronArchive.segmentFileBasePosition(
            startPosition, replayPosition, termLength, segmentLength);
        this.replayPosition = replayPosition;
        replayLimit = replayPosition + replayLength;

        segmentFile = new File(archiveDir, segmentFileName(recordingId, segmentFileBasePosition));
        connectDeadlineMs = epochClock.time() + connectTimeoutMs;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        final CountedErrorHandler errorHandler = controlSession.archiveConductor().context().countedErrorHandler();
        if (revokePublication)
        {
            try
            {
                publication.revoke();
            }
            catch (final Exception ex)
            {
                errorHandler.onError(ex);
            }
        }
        else
        {
            CloseHelper.close(errorHandler, publication);
        }
        CloseHelper.close(errorHandler, fileChannel);
    }

    /**
     * {@inheritDoc}
     */
    public long sessionId()
    {
        return sessionId;
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        int workCount = 0;

        if (isAborted)
        {
            revokePublication = true;
            state(State.INACTIVE, "replay aborted");
        }

        try
        {
            if (State.INIT == state)
            {
                workCount += init();
            }

            if (State.REPLAY == state)
            {
                workCount += replay();
            }
        }
        catch (final IOException ex)
        {
            raiseError(ex.toString(), ex);
        }

        if (State.INACTIVE == state)
        {
            closeRecordingSegment();
            state(State.DONE, "");
        }

        return workCount;
    }

    /**
     * {@inheritDoc}
     */
    public void abort(final String reason)
    {
        isAborted = true;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isDone()
    {
        return state == State.DONE;
    }

    long recordingId()
    {
        return recordingId;
    }

    State state()
    {
        return state;
    }

    String replayChannel()
    {
        return publication.channel();
    }

    int replayStreamId()
    {
        return publication.streamId();
    }

    long segmentFileBasePosition()
    {
        return segmentFileBasePosition;
    }

    void sendPendingError()
    {
        if (null != errorMessage)
        {
            onPendingError(sessionId, recordingId, errorMessage);
            controlSession.sendErrorResponse(correlationId, ArchiveException.GENERIC, errorMessage);
        }
    }

    @SuppressWarnings("unused")
    void onPendingError(final long sessionId, final long recordingId, final String errorMessage)
    {
        // Hook for Agent logging
    }

    private int init() throws IOException
    {
        if (null == fileChannel)
        {
            if (!segmentFile.exists())
            {
                if (epochClock.time() > connectDeadlineMs)
                {
                    raiseError("recording segment file not created", null);
                }

                return 0;
            }
            else
            {
                final long startTermBasePosition = startPosition - (startPosition & (termLength - 1));
                final int segmentOffset = (int)((replayPosition - startTermBasePosition) & (segmentLength - 1));
                final int termId = LogBufferDescriptor.computeTermIdFromPosition(
                    replayPosition, publication.positionBitsToShift(), publication.initialTermId());

                openRecordingSegment();

                termOffset = (int)(replayPosition & (termLength - 1));
                termBaseSegmentOffset = segmentOffset - termOffset;

                if (replayPosition > startPosition && replayPosition != stopPosition)
                {
                    if (notHeaderAligned(fileChannel, replayBuffer, segmentOffset, termOffset, termId, streamId))
                    {
                        raiseError("replayPosition=" + framePosition(0) + " does not point to a valid frame", null);
                        return 0;
                    }
                }

                controlSession.asyncSendOkResponse(correlationId, sessionId);
            }
        }

        if (!publication.isConnected())
        {
            if (epochClock.time() > connectDeadlineMs)
            {
                raiseError("no connection established for replayChannel=" + publication.channel() +
                    ", replayStreamId=" + publication.streamId(), null);
            }

            return 0;
        }

        state(State.REPLAY, "");

        return 1;
    }

    @SuppressWarnings("methodlength")
    private int replay() throws IOException
    {
        if (!publication.isConnected())
        {
            revokePublication = true;
            state(State.INACTIVE, "publication is not connected");
            return 0;
        }

        if (startPosition == stopPosition && 0 == replayLimit)
        {
            state(State.INACTIVE, "empty replay");
            return 0;
        }

        if (null != limitPosition && replayPosition >= stopPosition && notExtended(replayPosition, stopPosition))
        {
            return 0;
        }

        if (termOffset == termLength)
        {
            nextTerm();
        }

        int workCount = 0;
        if (publication.availableWindow() > 0)
        {
            final long startNs = nanoClock.nanoTime();
            final int bytesRead = readRecording(stopPosition - replayPosition);
            if (bytesRead > 0)
            {
                final int sessionId = publication.sessionId();
                final int streamId = publication.streamId();
                final int remaining = (int)Math.min(replayLimit - replayPosition, LogBufferDescriptor.TERM_MAX_LENGTH);
                int batchOffset = 0;
                int paddingFrameLength = 0;

                while (batchOffset < bytesRead && batchOffset < remaining)
                {
                    final int frameLength = frameLength(replayBuffer, batchOffset);
                    if (frameLength <= 0)
                    {
                        raiseError("unexpected end of recording at position=" + framePosition(batchOffset), null);
                    }

                    final int frameType = frameType(replayBuffer, batchOffset);
                    final int alignedLength = align(frameLength, FRAME_ALIGNMENT);

                    if (HDR_TYPE_DATA == frameType)
                    {
                        if (batchOffset + alignedLength > bytesRead)
                        {
                            break;
                        }

                        if (null != checksum)
                        {
                            verifyChecksum(checksum, batchOffset, alignedLength);
                        }

                        replayBuffer.putInt(batchOffset + SESSION_ID_FIELD_OFFSET, sessionId, LITTLE_ENDIAN);
                        replayBuffer.putInt(batchOffset + STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);
                        batchOffset += alignedLength;
                    }
                    else if (HDR_TYPE_PAD == frameType)
                    {
                        paddingFrameLength = frameLength;
                        break;
                    }
                }

                final long readTimeNs = nanoClock.nanoTime() - startNs;
                replayer.bytesRead(bytesRead);
                replayer.readTimeNs(readTimeNs);

                if (batchOffset > 0)
                {
                    final long position = publication.offerBlock(replayBuffer, 0, batchOffset);
                    if (hasPublicationAdvanced(position, batchOffset))
                    {
                        workCount++;
                    }
                    else
                    {
                        paddingFrameLength = 0;
                    }
                }

                if (paddingFrameLength > 0)
                {
                    final long position = publication.appendPadding(paddingFrameLength - HEADER_LENGTH);
                    if (hasPublicationAdvanced(position, align(paddingFrameLength, FRAME_ALIGNMENT)))
                    {
                        workCount++;
                    }
                }
            }
        }

        return workCount;
    }

    private String framePosition(final int frameOffset)
    {
        final long pos = segmentFileBasePosition + termBaseSegmentOffset + termOffset + frameOffset;
        return pos +
            " (segmentFilePosition=" + segmentFileBasePosition +
            ", segmentOffset=" + termBaseSegmentOffset +
            ", termOffset=" + termOffset +
            ", frameOffset=" + frameOffset + ")";
    }

    private boolean hasPublicationAdvanced(final long position, final int alignedLength)
    {
        if (position > 0)
        {
            termOffset += alignedLength;
            replayPosition += alignedLength;

            if (replayPosition >= replayLimit)
            {
                state(State.INACTIVE, "position (" + replayPosition + ") past limit (" + replayLimit + ")");
            }

            return true;
        }
        else if (Publication.CLOSED == position || Publication.NOT_CONNECTED == position)
        {
            revokePublication = true;
            state(State.INACTIVE, "stream closed before replay complete");
        }

        return false;
    }

    private void verifyChecksum(final Checksum checksum, final int frameOffset, final int alignedLength)
    {
        final int computedChecksum = checksum.compute(
            replayBufferAddress, frameOffset + HEADER_LENGTH, alignedLength - HEADER_LENGTH);
        final int recordedChecksum = frameSessionId(replayBuffer, frameOffset);

        if (computedChecksum != recordedChecksum)
        {
            final String message = "CRC checksum mismatch at position=" + framePosition(frameOffset) +
                ": recorded checksum=" + recordedChecksum + ", computed checksum=" + computedChecksum;
            raiseError(message, null);
        }
    }

    private int readRecording(final long availableReplay) throws IOException
    {
        final int limit = min((int)min(availableReplay, replayBuffer.capacity()), termLength - termOffset);
        final ByteBuffer byteBuffer = replayBuffer.byteBuffer();
        byteBuffer.clear().limit(limit);

        int position = termBaseSegmentOffset + termOffset;
        do
        {
            final int bytesRead = fileChannel.read(byteBuffer, position);
            if (bytesRead <= 0)
            {
                break;
            }

            position += bytesRead;
        }
        while (byteBuffer.remaining() > 0);

        return limit;
    }

    private void raiseError(final String errorMessage, final Throwable cause)
    {
        revokePublication = true;
        this.errorMessage = errorMessage +
            ", recordingId=" + recordingId +
            ", replaySessionId=" + sessionId +
            ", segmentFile=" + segmentFileName(recordingId, segmentFileBasePosition);
        state(State.INACTIVE, errorMessage);
        throw new ArchiveException(this.errorMessage, cause, ArchiveException.GENERIC);
    }

    private boolean notExtended(final long replayPosition, final long oldStopPosition)
    {
        final Counter limitPosition = this.limitPosition;
        final long currentLimitPosition = limitPosition.get();
        long newStopPosition = oldStopPosition;

        if (limitPosition.isClosed())
        {
            if (countersReader.getCounterRegistrationId(limitPosition.id()) == limitPosition.registrationId())
            {
                replayLimit = currentLimitPosition;
                newStopPosition = Math.max(oldStopPosition, currentLimitPosition);
            }
            else if (replayLimit >= oldStopPosition)
            {
                replayLimit = oldStopPosition;
            }
        }
        else
        {
            newStopPosition = currentLimitPosition;
        }

        if (replayPosition >= replayLimit)
        {
            state(State.INACTIVE, "position (" + replayPosition + ") past limit (" + replayLimit + ") (notExtended)");
        }
        else if (newStopPosition > oldStopPosition)
        {
            stopPosition = newStopPosition;
            return false;
        }

        return true;
    }

    private void nextTerm() throws IOException
    {
        termOffset = 0;
        termBaseSegmentOffset += termLength;

        if (termBaseSegmentOffset == segmentLength)
        {
            closeRecordingSegment();
            segmentFileBasePosition += segmentLength;
            openRecordingSegment();
            termBaseSegmentOffset = 0;
        }
    }

    private void closeRecordingSegment()
    {
        CloseHelper.close(fileChannel);
        fileChannel = null;
        segmentFile = null;
    }

    private void openRecordingSegment() throws IOException
    {
        if (null == segmentFile)
        {
            final String segmentFileName = segmentFileName(recordingId, segmentFileBasePosition);
            segmentFile = new File(archiveDir, segmentFileName);

            if (!segmentFile.exists())
            {
                raiseError("recording segment not found", null);
                return;
            }
        }

        fileChannel = FileChannel.open(segmentFile.toPath(), FILE_OPTIONS);
    }

    static boolean notHeaderAligned(
        final FileChannel channel,
        final UnsafeBuffer buffer,
        final int segmentOffset,
        final int termOffset,
        final int termId,
        final int streamId) throws IOException
    {
        final ByteBuffer byteBuffer = buffer.byteBuffer();
        byteBuffer.clear().limit(HEADER_LENGTH);
        if (HEADER_LENGTH != channel.read(byteBuffer, segmentOffset))
        {
            throw new IOException("failed to read fragment header");
        }

        return isInvalidHeader(buffer, streamId, termId, termOffset);
    }

    private void state(final State newState, final String reason)
    {
        logStateChange(state, newState, sessionId, recordingId, replayPosition, null == reason ? "" : reason);
        state = newState;
    }

    @SuppressWarnings("unused")
    private void logStateChange(
        final State oldState,
        final State newState,
        final long sessionId,
        final long recordingId,
        final long position,
        final String reason)
    {
        //System.out.println("ReplaySession: " + state + " -> " + newState);
    }

    static boolean isInvalidHeader(
        final UnsafeBuffer buffer, final int streamId, final int termId, final int termOffset)
    {
        return
            termOffset(buffer, 0) != termOffset ||
            termId(buffer, 0) != termId ||
            streamId(buffer, 0) != streamId;
    }
}
