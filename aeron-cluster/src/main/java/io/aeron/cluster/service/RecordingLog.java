/*
 * Copyright 2014-2018 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster.service;

import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.RecoveryPlanDecoder;
import io.aeron.cluster.codecs.RecoveryPlanEncoder;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.BitUtil.*;

/**
 * A log of recordings that make up the history of a Raft log. Entries are in order.
 * <p>
 * The log is made up of entries of log terms or snapshots to roll up state as of a log position and leadership term.
 * <p>
 * The latest state is made up of a the latest snapshot followed by any term logs which follow. It is possible that
 * the a snapshot is taken mid term and therefore the latest state is the snapshot plus the log of messages which
 * begin before the snapshot but continues after it.
 * <p>
 * Record layout as follows:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                        Recording ID                           |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                     Leadership Term ID                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |              Log Position at beginning of term                |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                    Term Position/Length                       |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |   Timestamp at beginning of term or when snapshot was taken   |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                       Member ID vote                          |
 *  +---------------------------------------------------------------+
 *  |                 Entry Type (Log or Snapshot)                  |
 *  +---------------------------------------------------------------+
 *  |                                                               |
 *  |                                                              ...
 * ...                Repeats to the end of the log                 |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class RecordingLog
{
    /**
     * A copy of the entry in the log.
     */
    public static final class Entry
    {
        public final long recordingId;
        public final long leadershipTermId;
        public final long termBaseLogPosition;
        public final long termPosition;
        public final long timestamp;
        public final int votedForMemberId;
        public final int type;
        public final int entryIndex;

        /**
         * A new entry in the recording log.
         *
         * @param recordingId         of the entry in an archive.
         * @param leadershipTermId    of this entry.
         * @param termBaseLogPosition position of the log over leadership terms at the beginning of this term.
         * @param termPosition        position reached within the current leadership term, same as term length.
         * @param timestamp           of this entry.
         * @param votedForMemberId    which member this node voted for in the election.
         * @param type                of the entry as a log of a term or a snapshot.
         * @param entryIndex          of the entry on disk.
         */
        public Entry(
            final long recordingId,
            final long leadershipTermId,
            final long termBaseLogPosition,
            final long termPosition,
            final long timestamp,
            final int votedForMemberId,
            final int type,
            final int entryIndex)
        {
            this.recordingId = recordingId;
            this.leadershipTermId = leadershipTermId;
            this.termBaseLogPosition = termBaseLogPosition;
            this.termPosition = termPosition;
            this.timestamp = timestamp;
            this.votedForMemberId = votedForMemberId;
            this.type = type;
            this.entryIndex = entryIndex;
        }

        public Entry(final RecoveryPlanDecoder.StepsDecoder decoder)
        {
            this.recordingId = decoder.recordingId();
            this.leadershipTermId = decoder.leadershipTermId();
            this.termBaseLogPosition = decoder.termBaseLogPosition();
            this.termPosition = decoder.termPosition();
            this.timestamp = decoder.timestamp();
            this.votedForMemberId = decoder.votedForMemberId();
            this.type = decoder.entryType();
            this.entryIndex = decoder.entryIndex();
        }

        public void encode(final RecoveryPlanEncoder.StepsEncoder encoder)
        {
            encoder
                .recordingId(recordingId)
                .leadershipTermId(leadershipTermId)
                .termBaseLogPosition(termBaseLogPosition)
                .termPosition(termPosition)
                .timestamp(timestamp)
                .votedForMemberId(votedForMemberId)
                .entryType(type)
                .entryIndex(entryIndex);
        }

        public String toString()
        {
            return "Entry{" +
                "recordingId=" + recordingId +
                ", leadershipTermId=" + leadershipTermId +
                ", termBaseLogPosition=" + termBaseLogPosition +
                ", termPosition=" + termPosition +
                ", timestamp=" + timestamp +
                ", votedForMemberId=" + votedForMemberId +
                ", type=" + type +
                ", entryIndex=" + entryIndex +
                '}';
        }
    }

    /**
     * Steps in a recovery plan.
     */
    public static class ReplayStep
    {
        public final long recordingStartPosition;
        public final long recordingStopPosition;
        public final int recordingSessionId;
        public final Entry entry;

        public ReplayStep(
            final long recordingStartPosition,
            final long recordingStopPosition,
            final int recordingSessionId,
            final Entry entry)
        {
            this.recordingStartPosition = recordingStartPosition;
            this.recordingStopPosition = recordingStopPosition;
            this.recordingSessionId = recordingSessionId;
            this.entry = entry;
        }

        public ReplayStep(final RecoveryPlanDecoder.StepsDecoder decoder)
        {
            this.recordingStartPosition = decoder.recordingStartPosition();
            this.recordingStopPosition = decoder.recordingStopPosition();
            this.recordingSessionId = decoder.recordingSessionId();
            entry = new Entry(decoder);
        }

        public void encode(final RecoveryPlanEncoder.StepsEncoder encoder)
        {
            encoder
                .recordingStartPosition(recordingStartPosition)
                .recordingStopPosition(recordingStopPosition)
                .recordingSessionId(recordingSessionId);
            entry.encode(encoder);
        }

        public String toString()
        {
            return "ReplayStep{" +
                "recordingStartPosition=" + recordingStartPosition +
                ", recordingStopPosition=" + recordingStopPosition +
                ", recordingSessionId=" + recordingSessionId +
                ", entry=" + entry +
                '}';
        }
    }

    /**
     * The snapshot and steps to recover the state of a cluster.
     */
    public static class RecoveryPlan
    {
        public final long lastLeadershipTermId;
        public final long lastTermBaseLogPosition;
        public final long lastTermPositionCommitted;
        public final long lastTermPositionAppended;
        public final ReplayStep snapshotStep;
        public final ArrayList<ReplayStep> termSteps;
        public final RecoveryPlanEncoder encoder = new RecoveryPlanEncoder();
        public final RecoveryPlanDecoder decoder = new RecoveryPlanDecoder();

        public RecoveryPlan(
            final long lastLeadershipTermId,
            final long lastTermBaseLogPosition,
            final long lastTermPositionCommitted,
            final long lastTermPositionAppended,
            final ReplayStep snapshotStep,
            final ArrayList<ReplayStep> termSteps)
        {
            this.lastLeadershipTermId = lastLeadershipTermId;
            this.lastTermBaseLogPosition = lastTermBaseLogPosition;
            this.lastTermPositionCommitted = lastTermPositionCommitted;
            this.lastTermPositionAppended = lastTermPositionAppended;
            this.snapshotStep = snapshotStep;
            this.termSteps = termSteps;
        }

        public RecoveryPlan(final DirectBuffer buffer, final int offset)
        {
            decoder.wrap(buffer, offset, RecoveryPlanDecoder.BLOCK_LENGTH, RecoveryPlanDecoder.SCHEMA_VERSION);

            this.lastLeadershipTermId = decoder.lastLeadershipTermId();
            this.lastTermBaseLogPosition = decoder.lastTermBaseLogPosition();
            this.lastTermPositionCommitted = decoder.lastTermPositionCommitted();
            this.lastTermPositionAppended = decoder.lastTermPositionAppended();

            ReplayStep snapshot = null;
            termSteps = new ArrayList<>();
            int stepNumber = 0;

            for (final RecoveryPlanDecoder.StepsDecoder stepDecoder : decoder.steps())
            {
                if (0 == stepNumber && stepDecoder.entryType() == RecordingLog.ENTRY_TYPE_SNAPSHOT)
                {
                    snapshot = new ReplayStep(stepDecoder);
                }
                else
                {
                    termSteps.add(new ReplayStep(stepDecoder));
                }

                stepNumber++;
            }

            this.snapshotStep = snapshot;
        }

        public int encodedLength()
        {
            final int stepsCount = termSteps.size() + (null != snapshotStep ? 1 : 0);

            return RecoveryPlanEncoder.BLOCK_LENGTH +
                RecoveryPlanEncoder.StepsEncoder.sbeHeaderSize() +
                stepsCount * RecoveryPlanEncoder.StepsEncoder.sbeBlockLength();
        }

        public int encode(final MutableDirectBuffer buffer, final int offset)
        {
            encoder.wrap(buffer, offset)
                .lastLeadershipTermId(lastLeadershipTermId)
                .lastTermBaseLogPosition(lastTermBaseLogPosition)
                .lastTermPositionCommitted(lastTermPositionCommitted)
                .lastTermPositionAppended(lastTermPositionAppended);

            final int stepsCount = termSteps.size() + (null != snapshotStep ? 1 : 0);
            final RecoveryPlanEncoder.StepsEncoder stepEncoder = encoder.stepsCount(stepsCount);

            if (null != snapshotStep)
            {
                stepEncoder.next();
                snapshotStep.encode(stepEncoder);
            }

            for (int i = 0, size = termSteps.size(); i < size; i++)
            {
                stepEncoder.next();
                termSteps.get(i).encode(stepEncoder);
            }

            return encoder.encodedLength();
        }

        public String toString()
        {
            return "RecoveryPlan{" +
                "lastLeadershipTermId=" + lastLeadershipTermId +
                ", lastTermBaseLogPosition=" + lastTermBaseLogPosition +
                ", lastTermPositionCommitted=" + lastTermPositionCommitted +
                ", lastTermPositionAppended=" + lastTermPositionAppended +
                ", snapshotStep=" + snapshotStep +
                ", termSteps=" + termSteps +
                '}';
        }
    }

    /**
     * Filename for the history of log terms and snapshot recordings.
     */
    public static final String RECORDING_LOG_FILE_NAME = "recording.log";

    /**
     * Represents a value that is not set or invalid.
     */
    public static final int NULL_VALUE = -1;

    /**
     * The log entry is for a recording of messages within a term to the consensus log.
     */
    public static final int ENTRY_TYPE_TERM = 0;

    /**
     * The log entry is for a recording of a snapshot of state taken as of a position in the log.
     */
    public static final int ENTRY_TYPE_SNAPSHOT = 1;

    /**
     * The offset at which the recording id for the entry is stored.
     */
    public static final int RECORDING_ID_OFFSET = 0;

    /**
     * The offset at which the leadership term id for the entry is stored.
     */
    public static final int LEADERSHIP_TERM_ID_OFFSET = RECORDING_ID_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the log position as of the beginning of the term for the entry is stored.
     */
    public static final int TERM_BASE_LOG_POSITION_OFFSET = LEADERSHIP_TERM_ID_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the term position is stored.
     */
    public static final int TERM_POSITION_OFFSET = TERM_BASE_LOG_POSITION_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the timestamp for the entry is stored.
     */
    public static final int TIMESTAMP_OFFSET = TERM_POSITION_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the voted for member id is recorded.
     */
    public static final int MEMBER_ID_VOTE_OFFSET = TIMESTAMP_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the type of the entry is stored.
     */
    public static final int ENTRY_TYPE_OFFSET = MEMBER_ID_VOTE_OFFSET + SIZE_OF_INT;

    /**
     * The length of each entry.
     */
    private static final int ENTRY_LENGTH = BitUtil.align(ENTRY_TYPE_OFFSET + SIZE_OF_INT, CACHE_LINE_LENGTH);

    private int nextEntryIndex;
    private final File parentDir;
    private final File logFile;
    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096).order(LITTLE_ENDIAN);
    private final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);
    private final ArrayList<Entry> entries = new ArrayList<>();

    /**
     * Create a log that appends to an existing log or creates a new one.
     *
     * @param parentDir in which the log will be created.
     */
    public RecordingLog(final File parentDir)
    {
        this.parentDir = parentDir;
        this.logFile = new File(parentDir, RECORDING_LOG_FILE_NAME);

        reload();
    }

    /**
     * List of currently loaded entries.
     *
     * @return the list of currently loaded entries.
     */
    public List<Entry> entries()
    {
        return entries;
    }

    /**
     * Get the next index to be used when appending an entry to the log.
     *
     * @return the next index to be used when appending an entry to the log.
     */
    public int nextEntryIndex()
    {
        return nextEntryIndex;
    }

    /**
     * Reload the log from disk.
     */
    public void reload()
    {
        entries.clear();

        FileChannel fileChannel = null;
        try
        {
            final boolean newFile = !logFile.exists();
            fileChannel = FileChannel.open(logFile.toPath(), CREATE, READ, WRITE, SYNC);

            if (newFile)
            {
                syncDirectory(parentDir);
                return;
            }

            nextEntryIndex = 0;
            byteBuffer.clear();
            while (true)
            {
                final int bytes = fileChannel.read(byteBuffer);
                if (byteBuffer.remaining() == 0)
                {
                    byteBuffer.flip();
                    captureEntriesFromBuffer(byteBuffer, buffer, entries);
                    byteBuffer.clear();
                }

                if (-1 == bytes)
                {
                    if (byteBuffer.position() > 0)
                    {
                        byteBuffer.flip();
                        captureEntriesFromBuffer(byteBuffer, buffer, entries);
                        byteBuffer.clear();
                    }

                    break;
                }
            }

        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        finally
        {
            CloseHelper.close(fileChannel);
        }
    }

    /**
     * Get the latest snapshot {@link Entry} in the log.
     *
     * @return the latest snapshot {@link Entry} in the log or null if no snapshot exists.
     */
    public Entry getLatestSnapshot()
    {
        for (int i = entries.size() - 1; i >= 0; i--)
        {
            final Entry entry = entries.get(i);
            if (ENTRY_TYPE_SNAPSHOT == entry.type)
            {
                return entry;
            }
        }

        return null;
    }

    /**
     * Create a recovery plan for the cluster that when the steps are replayed will bring the cluster back to the
     * latest stable state.
     *
     * @param archive to lookup recording descriptors.
     * @return a new {@link RecoveryPlan} for the cluster.
     */
    public RecoveryPlan createRecoveryPlan(final AeronArchive archive)
    {
        final ArrayList<ReplayStep> steps = new ArrayList<>();
        final ReplayStep snapshotStep = planRecovery(steps, entries, archive);
        long lastLeadershipTermId = -1;
        long lastLogPosition = 0;
        long lastTermPositionCommitted = -1;
        long lastTermPositionAppended = 0;

        if (null != snapshotStep)
        {
            lastLeadershipTermId = snapshotStep.entry.leadershipTermId;
            lastLogPosition = snapshotStep.entry.termBaseLogPosition;
            lastTermPositionCommitted = snapshotStep.entry.termPosition;
            lastTermPositionAppended = lastTermPositionCommitted;
        }

        final int size = steps.size();
        if (size > 0)
        {
            final ReplayStep replayStep = steps.get(size - 1);
            final Entry entry = replayStep.entry;

            lastLeadershipTermId = entry.leadershipTermId;
            lastLogPosition = entry.termBaseLogPosition;
            lastTermPositionCommitted = entry.termPosition;
            lastTermPositionAppended = replayStep.recordingStopPosition;
        }

        return new RecoveryPlan(
            lastLeadershipTermId,
            lastLogPosition,
            lastTermPositionCommitted,
            lastTermPositionAppended,
            snapshotStep,
            steps);
    }

    /**
     * Get the latest snapshot for a given position within a leadership term.
     *
     * @param leadershipTermId in which the snapshot was taken.
     * @param termPosition     within the leadership term.
     * @return the latest snapshot for a given position or null if no match found.
     */
    public Entry getSnapshot(final long leadershipTermId, final long termPosition)
    {
        for (int i = entries.size() - 1; i >= 0; i--)
        {
            final Entry entry = entries.get(i);
            if (entry.type == ENTRY_TYPE_SNAPSHOT &&
                leadershipTermId == entry.leadershipTermId &&
                termPosition == entry.termPosition)
            {
                return entry;
            }
        }

        return null;
    }

    /**
     * Append a log entry for a Raft term.
     *
     * @param leadershipTermId for the current term.
     * @param logPosition      reached at the beginning of the term.
     * @param timestamp        at the beginning of the term.
     * @param votedForMemberId in the leader election.
     */
    public void appendTerm(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final int votedForMemberId)
    {
        final int size = entries.size();
        if (size > 0)
        {
            final long expectedTermId = leadershipTermId - 1;
            final Entry entry = entries.get(size - 1);

            if (entry.type != NULL_VALUE && entry.leadershipTermId != expectedTermId)
            {
                throw new IllegalStateException("leadershipTermId out of sequence: previous " +
                    entry.leadershipTermId + " this " + leadershipTermId);
            }
        }

        append(ENTRY_TYPE_TERM, NULL_VALUE, leadershipTermId, logPosition, NULL_POSITION, timestamp, votedForMemberId);
    }

    /**
     * Append a log entry for a snapshot.
     *
     * @param recordingId      in the archive for the snapshot.
     * @param leadershipTermId for the current term
     * @param logPosition      at the beginning of the leadership term.
     * @param termPosition     for the position in the current term or length so far for that term.
     * @param timestamp        at which the snapshot was taken.
     */
    public void appendSnapshot(
        final long recordingId,
        final long leadershipTermId,
        final long logPosition,
        final long termPosition,
        final long timestamp)
    {
        final int size = entries.size();
        if (size > 0)
        {
            final Entry entry = entries.get(size - 1);

            if (entry.type == ENTRY_TYPE_TERM && entry.leadershipTermId != leadershipTermId)
            {
                throw new IllegalStateException("leadershipTermId out of sequence: previous " +
                    entry.leadershipTermId + " this " + leadershipTermId);
            }
        }

        append(ENTRY_TYPE_SNAPSHOT, recordingId, leadershipTermId, logPosition, termPosition, timestamp, NULL_VALUE);
    }

    /**
     * Commit the recording id of the log for a leadership term.
     *
     * @param leadershipTermId for committing the recording id for the log.
     * @param recordingId      for the log of the leadership term.
     */
    public void commitLeadershipRecordingId(final long leadershipTermId, final long recordingId)
    {
        final int index = getLeadershipTermEntryIndex(leadershipTermId);

        commitEntryValue(index, recordingId, RECORDING_ID_OFFSET);

        final Entry entry = entries.get(index);
        entries.set(index, new Entry(
            recordingId,
            entry.leadershipTermId,
            entry.termBaseLogPosition,
            entry.termPosition,
            entry.timestamp,
            entry.votedForMemberId,
            entry.type,
            entry.entryIndex));
    }

    /**
     * Commit the position reached in a leadership term before a clean shutdown.
     *
     * @param leadershipTermId for committing the term position reached.
     * @param termPosition     reached in the leadership term.
     */
    public void commitLeadershipTermPosition(final long leadershipTermId, final long termPosition)
    {
        final int index = getLeadershipTermEntryIndex(leadershipTermId);

        commitEntryValue(index, termPosition, TERM_POSITION_OFFSET);

        final Entry entry = entries.get(index);
        entries.set(index, new Entry(
            entry.recordingId,
            entry.leadershipTermId,
            entry.termBaseLogPosition,
            termPosition,
            entry.timestamp,
            entry.votedForMemberId,
            entry.type,
            entry.entryIndex));
    }

    /**
     * Commit the position for the base of a leadership term.
     *
     * @param leadershipTermId for committing the base position.
     * @param logPosition      for the base of a leadership term.
     */
    public void commitLeadershipLogPosition(final long leadershipTermId, final long logPosition)
    {
        final int index = getLeadershipTermEntryIndex(leadershipTermId);

        commitEntryValue(index, logPosition, TERM_BASE_LOG_POSITION_OFFSET);

        final Entry entry = entries.get(index);
        entries.set(index, new Entry(
            entry.recordingId,
            entry.leadershipTermId,
            logPosition,
            entry.termPosition,
            entry.timestamp,
            entry.votedForMemberId,
            entry.type,
            entry.entryIndex));
    }

    /**
     * Tombstone an entry in the log so it is no longer valid.
     *
     * @param leadershipTermId to match for validation.
     * @param entryIndex       reached in the leadership term.
     */
    public void tombstoneEntry(final long leadershipTermId, final int entryIndex)
    {
        int index = -1;
        for (int i = 0, size = entries.size(); i < size; i++)
        {
            final Entry entry = entries.get(i);
            if (entry.leadershipTermId == leadershipTermId && entry.entryIndex == entryIndex)
            {
                index = entry.entryIndex;
                break;
            }
        }

        if (-1 == index)
        {
            throw new IllegalArgumentException("unknown entry index: " + entryIndex);
        }

        buffer.putInt(0, NULL_VALUE, LITTLE_ENDIAN);
        byteBuffer.limit(SIZE_OF_INT).position(0);
        final long filePosition = (index * ENTRY_LENGTH) + ENTRY_TYPE_OFFSET;

        try (FileChannel fileChannel = FileChannel.open(logFile.toPath(), WRITE, SYNC))
        {
            if (SIZE_OF_INT != fileChannel.write(byteBuffer, filePosition))
            {
                throw new IllegalStateException("failed to write field atomically");
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    public String toString()
    {
        return "RecordingLog{" +
            "file=" + logFile.getAbsolutePath() +
            ", entries=" + entries +
            '}';
    }

    private void append(
        final int entryType,
        final long recordingId,
        final long leadershipTermId,
        final long logPosition,
        final long termPosition,
        final long timestamp,
        final int votedForMemberId)
    {
        buffer.putLong(RECORDING_ID_OFFSET, recordingId, LITTLE_ENDIAN);
        buffer.putLong(TERM_BASE_LOG_POSITION_OFFSET, logPosition, LITTLE_ENDIAN);
        buffer.putLong(LEADERSHIP_TERM_ID_OFFSET, leadershipTermId, LITTLE_ENDIAN);
        buffer.putLong(TIMESTAMP_OFFSET, timestamp, LITTLE_ENDIAN);
        buffer.putLong(TERM_POSITION_OFFSET, termPosition, LITTLE_ENDIAN);
        buffer.putInt(MEMBER_ID_VOTE_OFFSET, votedForMemberId, LITTLE_ENDIAN);
        buffer.putInt(ENTRY_TYPE_OFFSET, entryType, LITTLE_ENDIAN);

        byteBuffer.limit(ENTRY_LENGTH).position(0);

        try (FileChannel fileChannel = FileChannel.open(logFile.toPath(), WRITE, APPEND, SYNC))
        {
            if (ENTRY_LENGTH != fileChannel.write(byteBuffer))
            {
                throw new IllegalStateException("failed to write entry atomically");
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        entries.add(new Entry(
            recordingId,
            leadershipTermId,
            logPosition,
            NULL_POSITION,
            timestamp,
            votedForMemberId,
            entryType,
            nextEntryIndex++));
    }

    private void captureEntriesFromBuffer(
        final ByteBuffer byteBuffer, final UnsafeBuffer buffer, final ArrayList<Entry> entries)
    {
        for (int i = 0, length = byteBuffer.limit(); i < length; i += ENTRY_LENGTH)
        {
            final int entryType = buffer.getInt(i + ENTRY_TYPE_OFFSET);

            if (NULL_VALUE != entryType)
            {
                entries.add(new Entry(
                    buffer.getLong(i + RECORDING_ID_OFFSET, LITTLE_ENDIAN),
                    buffer.getLong(i + LEADERSHIP_TERM_ID_OFFSET, LITTLE_ENDIAN),
                    buffer.getLong(i + TERM_BASE_LOG_POSITION_OFFSET, LITTLE_ENDIAN),
                    buffer.getLong(i + TERM_POSITION_OFFSET, LITTLE_ENDIAN),
                    buffer.getLong(i + TIMESTAMP_OFFSET, LITTLE_ENDIAN),
                    buffer.getInt(i + MEMBER_ID_VOTE_OFFSET, LITTLE_ENDIAN),
                    entryType,
                    nextEntryIndex));
            }

            ++nextEntryIndex;
        }
    }

    private static void syncDirectory(final File dir)
    {
        try (FileChannel fileChannel = FileChannel.open(dir.toPath()))
        {
            fileChannel.force(true);
        }
        catch (final IOException ignore)
        {
        }
    }

    private static void getRecordingExtent(
        final AeronArchive archive, final RecordingExtent recordingExtent, final Entry entry)
    {
        if (archive.listRecording(entry.recordingId, recordingExtent) == 0)
        {
            throw new IllegalStateException("unknown recording id: " + entry.recordingId);
        }
    }

    private int getLeadershipTermEntryIndex(final long leadershipTermId)
    {
        for (int i = 0, size = entries.size(); i < size; i++)
        {
            final Entry entry = entries.get(i);
            if (entry.leadershipTermId == leadershipTermId && entry.type == ENTRY_TYPE_TERM)
            {
                return entry.entryIndex;
            }
        }

        throw new IllegalArgumentException("unknown leadershipTermId: " + leadershipTermId);
    }

    private static ReplayStep planRecovery(
        final ArrayList<ReplayStep> steps, final ArrayList<Entry> entries, final AeronArchive archive)
    {
        if (entries.isEmpty())
        {
            return null;
        }

        int snapshotIndex = -1;
        for (int i = entries.size() - 1; i >= 0; i--)
        {
            final Entry entry = entries.get(i);
            if (ENTRY_TYPE_SNAPSHOT == entry.type)
            {
                snapshotIndex = i;
                break;
            }
        }

        final ReplayStep snapshotStep;
        final RecordingExtent recordingExtent = new RecordingExtent();

        if (-1 != snapshotIndex)
        {
            final Entry snapshot = entries.get(snapshotIndex);
            getRecordingExtent(archive, recordingExtent, snapshot);

            snapshotStep = new ReplayStep(
                recordingExtent.startPosition, recordingExtent.stopPosition, recordingExtent.sessionId, snapshot);

            if (snapshotIndex - 1 >= 0)
            {
                for (int i = snapshotIndex - 1; i >= 0; i--)
                {
                    final Entry entry = entries.get(i);
                    if (ENTRY_TYPE_TERM == entry.type)
                    {
                        getRecordingExtent(archive, recordingExtent, entry);
                        final long snapshotPosition = snapshot.termBaseLogPosition + snapshot.termPosition;

                        if (recordingExtent.stopPosition == NULL_POSITION ||
                            (entry.termBaseLogPosition + recordingExtent.stopPosition) > snapshotPosition)
                        {
                            steps.add(new ReplayStep(
                                snapshot.termPosition, recordingExtent.stopPosition, recordingExtent.sessionId, entry));
                        }
                        break;
                    }
                }
            }
        }
        else
        {
            snapshotStep = null;
        }

        for (int i = snapshotIndex + 1, length = entries.size(); i < length; i++)
        {
            final Entry entry = entries.get(i);
            getRecordingExtent(archive, recordingExtent, entry);

            steps.add(new ReplayStep(
                recordingExtent.startPosition, recordingExtent.stopPosition, recordingExtent.sessionId, entry));
        }

        return snapshotStep;
    }

    private void commitEntryValue(final int entryIndex, final long value, final int fieldOffset)
    {
        buffer.putLong(0, value, LITTLE_ENDIAN);
        byteBuffer.limit(SIZE_OF_LONG).position(0);
        final long filePosition = (entryIndex * ENTRY_LENGTH) + fieldOffset;

        try (FileChannel fileChannel = FileChannel.open(logFile.toPath(), WRITE, SYNC))
        {
            if (SIZE_OF_LONG != fileChannel.write(byteBuffer, filePosition))
            {
                throw new IllegalStateException("failed to write field atomically");
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }
}
