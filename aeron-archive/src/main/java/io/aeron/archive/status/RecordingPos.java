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
package io.aeron.archive.status;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.Counter;
import io.aeron.Image;
import io.aeron.archive.ArchiveCounters;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.*;

/**
 * The position a recording has reached when being archived.
 * <p>
 * Key has the following layout:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                        Recording ID                           |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                         Session ID                            |
 *  +---------------------------------------------------------------+
 *  |                Source Identity for the Image                  |
 *  |                                                              ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                         Archive ID                            |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public final class RecordingPos
{
    /**
     * Type id of a recording position counter.
     */
    public static final int RECORDING_POSITION_TYPE_ID = AeronCounters.ARCHIVE_RECORDING_POSITION_TYPE_ID;

    /**
     * Represents a null recording id when not found.
     */
    public static final long NULL_RECORDING_ID = Aeron.NULL_VALUE;

    /**
     * Human-readable name for the counter.
     */
    public static final String NAME = "rec-pos";

    private RecordingPos()
    {
    }

    static final int RECORDING_ID_OFFSET = 0;
    static final int SESSION_ID_OFFSET = RECORDING_ID_OFFSET + SIZE_OF_LONG;
    static final int SOURCE_IDENTITY_LENGTH_OFFSET = SESSION_ID_OFFSET + SIZE_OF_INT;
    static final int SOURCE_IDENTITY_OFFSET = SOURCE_IDENTITY_LENGTH_OFFSET + SIZE_OF_INT;

    /**
     * Allocated a recording position counter and populate the metadata.
     *
     * @param aeron           on which the counter will be registered.
     * @param tempBuffer      for encoding the metadata.
     * @param archiveId       to which the counter belongs.
     * @param recordingId     for the recording.
     * @param sessionId       for the publication being recorded.
     * @param streamId        for the publication being recorded.
     * @param strippedChannel for the recording subscription.
     * @param sourceIdentity  for the publication.
     * @return the allocated counter.
     */
    public static Counter allocate(
        final Aeron aeron,
        final UnsafeBuffer tempBuffer,
        final long archiveId,
        final long recordingId,
        final int sessionId,
        final int streamId,
        final String strippedChannel,
        final String sourceIdentity)
    {
        tempBuffer.putLong(RECORDING_ID_OFFSET, recordingId);
        tempBuffer.putInt(SESSION_ID_OFFSET, sessionId);

        final int sourceIdentityLength = Math.min(
            sourceIdentity.length(), MAX_KEY_LENGTH - SOURCE_IDENTITY_OFFSET - SIZE_OF_LONG);
        tempBuffer.putInt(SOURCE_IDENTITY_LENGTH_OFFSET, sourceIdentityLength);
        tempBuffer.putStringWithoutLengthAscii(SOURCE_IDENTITY_OFFSET, sourceIdentity, 0, sourceIdentityLength);
        final int archiveIdOffset = SOURCE_IDENTITY_OFFSET + sourceIdentityLength;
        tempBuffer.putLong(archiveIdOffset, archiveId);
        final int keyLength = archiveIdOffset + SIZE_OF_LONG;

        final int labelOffset = BitUtil.align(keyLength, SIZE_OF_INT);
        int labelLength = 0;
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset, NAME + ": ");
        labelLength += tempBuffer.putLongAscii(labelOffset + labelLength, recordingId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, " ");
        labelLength += tempBuffer.putIntAscii(labelOffset + labelLength, sessionId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, " ");
        labelLength += tempBuffer.putIntAscii(labelOffset + labelLength, streamId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, " ");
        labelLength += tempBuffer.putStringWithoutLengthAscii(
            labelOffset + labelLength,
            strippedChannel,
            0,
            MAX_LABEL_LENGTH - labelLength - ArchiveCounters.lengthOfArchiveIdLabel(archiveId));
        labelLength += ArchiveCounters.appendArchiveIdLabel(tempBuffer, labelOffset + labelLength, archiveId);

        return aeron.addCounter(
            RECORDING_POSITION_TYPE_ID, tempBuffer, 0, keyLength, tempBuffer, labelOffset, labelLength);
    }

    /**
     * Find the active counter id for a stream based on the recording id.
     *
     * @param countersReader to search within.
     * @param recordingId    for the active recording.
     * @return the counter id if found otherwise {@link CountersReader#NULL_COUNTER_ID}.
     * @deprecated Use {@link #findCounterIdByRecording(CountersReader, long, long)} instead.
     */
    @Deprecated
    public static int findCounterIdByRecording(final CountersReader countersReader, final long recordingId)
    {
        return findCounterIdByRecording(countersReader, recordingId, Aeron.NULL_VALUE);
    }

    /**
     * Find the active counter id for a stream based on the recording id and archive id.
     *
     * @param countersReader to search within.
     * @param recordingId    for the active recording.
     * @param archiveId      to target specific Archive. Use {@link Aeron#NULL_VALUE} to emulate old behavior.
     * @return the counter id if found otherwise {@link CountersReader#NULL_COUNTER_ID}.
     * @since 1.44.0
     */
    public static int findCounterIdByRecording(
        final CountersReader countersReader, final long recordingId, final long archiveId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        for (int counterId = 0, maxId = countersReader.maxCounterId(); counterId <= maxId; counterId++)
        {
            final int counterState = countersReader.getCounterState(counterId);
            if (RECORD_ALLOCATED == counterState)
            {
                if (countersReader.getCounterTypeId(counterId) == RECORDING_POSITION_TYPE_ID)
                {
                    final int keyOffset = metaDataOffset(counterId) + KEY_OFFSET;
                    if (buffer.getLong(keyOffset + RECORDING_ID_OFFSET) == recordingId)
                    {
                        final int sourceIdentityLength = buffer.getInt(keyOffset + SOURCE_IDENTITY_LENGTH_OFFSET);
                        final int archiveIdOffset = keyOffset + SOURCE_IDENTITY_OFFSET + sourceIdentityLength;
                        if (Aeron.NULL_VALUE == archiveId || buffer.getLong(archiveIdOffset) == archiveId)
                        {
                            return counterId;
                        }
                    }
                }
            }
            else if (RECORD_UNUSED == counterState)
            {
                break;
            }
        }

        return NULL_COUNTER_ID;
    }

    /**
     * Find the active counter id for a stream based on the session id.
     *
     * @param countersReader to search within.
     * @param sessionId      for the active recording.
     * @return the counter id if found otherwise {@link CountersReader#NULL_COUNTER_ID}.
     * @deprecated Use {@link #findCounterIdBySession(CountersReader, int, long)} instead.
     */
    @Deprecated
    public static int findCounterIdBySession(final CountersReader countersReader, final int sessionId)
    {
        return findCounterIdBySession(countersReader, sessionId, Aeron.NULL_VALUE);
    }

    /**
     * Find the active counter id for a stream based on the session id and archive id.
     *
     * @param countersReader to search within.
     * @param sessionId      for the active recording.
     * @param archiveId      to target specific Archive. Use {@link Aeron#NULL_VALUE} to emulate old behavior.
     * @return the counter id if found otherwise {@link CountersReader#NULL_COUNTER_ID}.
     * @since 1.44.0
     */
    public static int findCounterIdBySession(
        final CountersReader countersReader, final int sessionId, final long archiveId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        for (int counterId = 0, maxId = countersReader.maxCounterId(); counterId <= maxId; counterId++)
        {
            final int counterState = countersReader.getCounterState(counterId);
            if (RECORD_ALLOCATED == counterState)
            {
                if (countersReader.getCounterTypeId(counterId) == RECORDING_POSITION_TYPE_ID)
                {
                    final int keyOffset = metaDataOffset(counterId) + KEY_OFFSET;
                    if (buffer.getInt(keyOffset + SESSION_ID_OFFSET) == sessionId)
                    {
                        final int sourceIdentityLength = buffer.getInt(keyOffset + SOURCE_IDENTITY_LENGTH_OFFSET);
                        final int archiveIdOffset = keyOffset + SOURCE_IDENTITY_OFFSET + sourceIdentityLength;
                        if (Aeron.NULL_VALUE == archiveId || buffer.getLong(archiveIdOffset) == archiveId)
                        {
                            return counterId;
                        }
                    }
                }
            }
            else if (RECORD_UNUSED == counterState)
            {
                break;
            }
        }

        return NULL_COUNTER_ID;
    }

    /**
     * Get the recording id for a given counter id.
     *
     * @param countersReader to search within.
     * @param counterId      for the active recording.
     * @return the counter id if found otherwise {@link #NULL_RECORDING_ID}.
     */
    public static long getRecordingId(final CountersReader countersReader, final int counterId)
    {
        if (countersReader.getCounterState(counterId) == RECORD_ALLOCATED &&
            countersReader.getCounterTypeId(counterId) == RECORDING_POSITION_TYPE_ID)
        {
            return countersReader.metaDataBuffer()
                .getLong(metaDataOffset(counterId) + KEY_OFFSET + RECORDING_ID_OFFSET);
        }

        return NULL_RECORDING_ID;
    }

    /**
     * Get the {@link Image#sourceIdentity()} for the recording.
     *
     * @param counters  to search within.
     * @param counterId for the active recording.
     * @return {@link Image#sourceIdentity()} for the recording or null if not found.
     */
    public static String getSourceIdentity(final CountersReader counters, final int counterId)
    {
        if (counters.getCounterState(counterId) == RECORD_ALLOCATED &&
            counters.getCounterTypeId(counterId) == RECORDING_POSITION_TYPE_ID)
        {
            final int recordOffset = metaDataOffset(counterId);
            return counters.metaDataBuffer().getStringAscii(recordOffset + KEY_OFFSET + SOURCE_IDENTITY_LENGTH_OFFSET);
        }

        return null;
    }

    /**
     * Is the recording counter still active.
     *
     * @param counters    to search within.
     * @param counterId   to search for.
     * @param recordingId to confirm it is still the same value.
     * @return true if the counter is still active otherwise false.
     */
    public static boolean isActive(final CountersReader counters, final int counterId, final long recordingId)
    {
        final int recordingIdOffset = metaDataOffset(counterId) + KEY_OFFSET + RECORDING_ID_OFFSET;
        return counters.getCounterState(counterId) == RECORD_ALLOCATED &&
            counters.getCounterTypeId(counterId) == RECORDING_POSITION_TYPE_ID &&
            counters.metaDataBuffer().getLong(recordingIdOffset) == recordingId;
    }
}
