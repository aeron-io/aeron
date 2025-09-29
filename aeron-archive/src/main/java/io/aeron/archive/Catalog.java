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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.archive.checksum.Checksum;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.*;
import org.agrona.*;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntConsumer;
import java.util.function.Predicate;

import static io.aeron.archive.Archive.Configuration.FILE_IO_MAX_LENGTH_DEFAULT;
import static io.aeron.archive.Archive.Configuration.RECORDING_SEGMENT_SUFFIX;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.NULL_TIMESTAMP;
import static io.aeron.archive.codecs.RecordingDescriptorDecoder.*;
import static io.aeron.archive.codecs.RecordingState.VALID;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.nio.ByteOrder.nativeOrder;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;
import static java.util.Collections.emptyList;
import static org.agrona.AsciiEncoding.parseLongAscii;
import static org.agrona.BitUtil.*;

/**
 * Catalog for the archive keeps details of recorded images, past and present, and used for browsing.
 * The format is simple, allocating a variable length record for each record descriptor. An index of recording id
 * to offset if maintained for fast access. The first record contains the catalog header and is
 * {@link #DEFAULT_ALIGNMENT} in length.
 * <p>
 *
 * @see RecordingDescriptorHeaderDecoder
 * @see RecordingDescriptorDecoder
 * Catalog file format:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                      Descriptor Length                        |
 *  +---------------------------------------------------------------+
 *  |                            State                              |
 *  +---------------------------------------------------------------+
 *  |                         Check Sum                             |
 *  +---------------------------------------------------------------+
 *  |                          Reserved                             |
 *  +---------------------------------------------------------------+
 *  |                          Reserved                             |
 *  +---------------------------------------------------------------+
 *  |                          Reserved                             |
 *  +---------------------------------------------------------------+
 *  |                          Reserved                             |
 *  +---------------------------------------------------------------+
 *  |                          Reserved                             |
 *  +---------------------------------------------------------------+
 *  |                Recording Descriptor (variable)                |
 *  |                                                              ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                          Repeats...                           |
 *  |                                                              ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */
final class Catalog implements AutoCloseable
{
    @FunctionalInterface
    interface CatalogEntryProcessor
    {
        void accept(
            int recordingDescriptorOffset,
            RecordingDescriptorHeaderEncoder headerEncoder,
            RecordingDescriptorHeaderDecoder headerDecoder,
            RecordingDescriptorEncoder descriptorEncoder,
            RecordingDescriptorDecoder descriptorDecoder);
    }

    static final int PAGE_SIZE = 4096;
    static final int NULL_RECORD_ID = Aeron.NULL_VALUE;

    static final int DESCRIPTOR_HEADER_LENGTH = RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;
    @Deprecated
    static final int DEFAULT_RECORD_LENGTH = 1024;
    static final int DEFAULT_ALIGNMENT = 1024;
    static final long MAX_CATALOG_LENGTH = Integer.MAX_VALUE;
    static final long DEFAULT_CAPACITY = 1024 * 1024;
    static final long MIN_CAPACITY = CatalogHeaderDecoder.BLOCK_LENGTH;

    private final CatalogHeaderDecoder catalogHeaderDecoder = new CatalogHeaderDecoder();
    private final CatalogHeaderEncoder catalogHeaderEncoder = new CatalogHeaderEncoder();

    private final RecordingDescriptorHeaderDecoder descriptorHeaderDecoder = new RecordingDescriptorHeaderDecoder();
    private final RecordingDescriptorHeaderEncoder descriptorHeaderEncoder = new RecordingDescriptorHeaderEncoder();

    private final RecordingDescriptorEncoder descriptorEncoder = new RecordingDescriptorEncoder();
    private final RecordingDescriptorDecoder descriptorDecoder = new RecordingDescriptorDecoder();

    private final boolean forceWrites;
    private final boolean forceMetadata;
    private boolean isClosed;
    private final File catalogFile;
    private final File archiveDir;
    private final EpochClock epochClock;
    private final Checksum checksum;
    private final CatalogIndex catalogIndex = new CatalogIndex();
    private final int alignment;
    private final int firstRecordingDescriptorOffset;

    private FileChannel catalogChannel;
    private MappedByteBuffer catalogByteBuffer;
    private UnsafeBuffer catalogBuffer;
    private UnsafeBuffer fieldAccessBuffer;
    private UnsafeBuffer headerAccessBuffer;
    private long catalogByteBufferAddress;
    private long capacity;
    private long nextRecordingId;
    private int nextRecordingDescriptorOffset;

    Catalog(
        final File archiveDir,
        final FileChannel archiveDirChannel,
        final int fileSyncLevel,
        final long catalogCapacity,
        final EpochClock epochClock,
        final Checksum checksum,
        final UnsafeBuffer buffer)
    {
        this.archiveDir = archiveDir;
        this.forceWrites = fileSyncLevel > 0;
        this.forceMetadata = fileSyncLevel > 1;
        this.epochClock = epochClock;
        this.checksum = checksum;

        validateCapacity(catalogCapacity);

        catalogFile = new File(archiveDir, Archive.Configuration.CATALOG_FILE_NAME);
        try
        {
            final boolean catalogExists = catalogFile.exists();
            MappedByteBuffer catalogMappedByteBuffer = null;
            FileChannel catalogFileChannel = null;

            try
            {
                catalogFileChannel = FileChannel.open(catalogFile.toPath(), CREATE, READ, WRITE, SPARSE);
                if (catalogExists)
                {
                    capacity = max(catalogFileChannel.size(), catalogCapacity);
                }
                else
                {
                    capacity = catalogCapacity;
                }

                catalogMappedByteBuffer = catalogFileChannel.map(READ_WRITE, 0, capacity);
            }
            catch (final Exception ex)
            {
                CloseHelper.close(catalogFileChannel);
                LangUtil.rethrowUnchecked(ex);
            }

            catalogChannel = catalogFileChannel;
            initBuffers(catalogMappedByteBuffer);

            final UnsafeBuffer catalogHeaderBuffer = new UnsafeBuffer(catalogByteBuffer);
            catalogHeaderDecoder.wrap(
                catalogHeaderBuffer, 0, CatalogHeaderDecoder.BLOCK_LENGTH, CatalogHeaderDecoder.SCHEMA_VERSION);
            catalogHeaderEncoder.wrap(catalogHeaderBuffer, 0);

            if (catalogExists)
            {
                final int version = catalogHeaderDecoder.version();
                if (SemanticVersion.major(version) != ArchiveMarkFile.MAJOR_VERSION)
                {
                    throw new ArchiveException(
                        "incompatible catalog file version " + SemanticVersion.toString(version) +
                        ", archive software is " + SemanticVersion.toString(ArchiveMarkFile.SEMANTIC_VERSION));
                }

                alignment = catalogHeaderDecoder.alignment();
                nextRecordingId = catalogHeaderDecoder.nextRecordingId();
            }
            else
            {
                alignment = CACHE_LINE_LENGTH;

                catalogHeaderEncoder
                    .version(ArchiveMarkFile.SEMANTIC_VERSION)
                    .length(CatalogHeaderEncoder.BLOCK_LENGTH)
                    .nextRecordingId(nextRecordingId)
                    .alignment(alignment);

                forceWrites(archiveDirChannel);
            }
            firstRecordingDescriptorOffset = CatalogHeaderEncoder.BLOCK_LENGTH;

            buildIndex(true);
            refreshCatalog(true, checksum, buffer);
        }
        catch (final Exception ex)
        {
            close();
            throw ex;
        }
    }

    Catalog(final File archiveDir, final EpochClock epochClock)
    {
        this(archiveDir, epochClock, MIN_CAPACITY, false, null, null);
    }

    Catalog(
        final File archiveDir,
        final EpochClock epochClock,
        final long catalogCapacity,
        final boolean writable,
        final Checksum checksum,
        final IntConsumer versionCheck)
    {
        this.archiveDir = archiveDir;
        this.forceWrites = false;
        this.forceMetadata = false;
        this.epochClock = epochClock;
        this.catalogChannel = null;
        this.checksum = checksum;
        catalogFile = new File(archiveDir, Archive.Configuration.CATALOG_FILE_NAME);

        validateCapacity(catalogCapacity);

        try
        {
            MappedByteBuffer catalogMappedByteBuffer = null;

            final StandardOpenOption[] openOptions =
                writable ? new StandardOpenOption[]{ READ, WRITE, SPARSE } : new StandardOpenOption[]{ READ };

            try (FileChannel channel = FileChannel.open(catalogFile.toPath(), openOptions))
            {
                capacity = max(channel.size(), catalogCapacity);
                catalogMappedByteBuffer = channel.map(writable ? READ_WRITE : READ_ONLY, 0, capacity);
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            initBuffers(catalogMappedByteBuffer);

            final UnsafeBuffer catalogHeaderBuffer = new UnsafeBuffer(catalogByteBuffer);
            catalogHeaderDecoder.wrap(
                catalogHeaderBuffer, 0, CatalogHeaderDecoder.BLOCK_LENGTH, CatalogHeaderDecoder.SCHEMA_VERSION);
            catalogHeaderEncoder.wrap(catalogHeaderBuffer, 0);

            final int version = catalogHeaderDecoder.version();
            if (null == versionCheck)
            {
                if (SemanticVersion.major(version) != ArchiveMarkFile.MAJOR_VERSION)
                {
                    throw new ArchiveException(
                        "incompatible catalog file version " + SemanticVersion.toString(version) +
                        ", archive software is " + SemanticVersion.toString(ArchiveMarkFile.SEMANTIC_VERSION));
                }
            }
            else
            {
                versionCheck.accept(version);
            }

            final int alignment = catalogHeaderDecoder.alignment();
            if (0 != alignment)
            {
                this.alignment = alignment;
                firstRecordingDescriptorOffset = CatalogHeaderEncoder.BLOCK_LENGTH;
                nextRecordingId = catalogHeaderDecoder.nextRecordingId();
            }
            else
            {
                this.alignment = DEFAULT_ALIGNMENT;
                firstRecordingDescriptorOffset = DEFAULT_ALIGNMENT;
            }

            buildIndex(writable);
            refreshCatalog(false, null, null);
        }
        catch (final Exception ex)
        {
            close();
            throw ex;
        }
    }

    public void close()
    {
        if (!isClosed)
        {
            isClosed = true;
            unmapAndCloseChannel();
        }
    }

    boolean isClosed()
    {
        return isClosed;
    }

    long capacity()
    {
        return capacity;
    }

    int alignment()
    {
        return alignment;
    }

    int entryCount()
    {
        return catalogIndex.size();
    }

    long nextRecordingId()
    {
        return nextRecordingId;
    }

    CatalogIndex index()
    {
        return catalogIndex;
    }

    int version()
    {
        return catalogHeaderDecoder.version();
    }

    void updateVersion(final int version)
    {
        catalogHeaderEncoder.version(version);
    }

    long addNewRecording(
        final long startPosition,
        final long stopPosition,
        final long startTimestamp,
        final long stopTimestamp,
        final int imageInitialTermId,
        final int segmentFileLength,
        final int termBufferLength,
        final int mtuLength,
        final int sessionId,
        final int streamId,
        final String strippedChannel,
        final String originalChannel,
        final String sourceIdentity)
    {
        final int frameLength = recordingDescriptorFrameLength(strippedChannel, originalChannel, sourceIdentity);
        final int recordingDescriptorOffset = nextRecordingDescriptorOffset;

        if (recordingDescriptorOffset + frameLength > capacity)
        {
            growCatalog(MAX_CATALOG_LENGTH, frameLength);
        }

        final long recordingId = nextRecordingId;

        catalogBuffer.wrap(catalogByteBuffer, recordingDescriptorOffset, frameLength);
        descriptorEncoder
            .wrap(catalogBuffer, DESCRIPTOR_HEADER_LENGTH)
            .recordingId(recordingId)
            .startTimestamp(startTimestamp)
            .stopTimestamp(stopTimestamp)
            .startPosition(startPosition)
            .stopPosition(stopPosition)
            .initialTermId(imageInitialTermId)
            .segmentFileLength(segmentFileLength)
            .termBufferLength(termBufferLength)
            .mtuLength(mtuLength)
            .sessionId(sessionId)
            .streamId(streamId)
            .strippedChannel(strippedChannel)
            .originalChannel(originalChannel)
            .sourceIdentity(sourceIdentity);

        final int recordingLength = frameLength - DESCRIPTOR_HEADER_LENGTH;
        descriptorHeaderEncoder
            .wrap(catalogBuffer, 0)
            .length(recordingLength)
            .checksum(computeRecordingDescriptorChecksum(recordingDescriptorOffset, recordingLength))
            .state(VALID);

        catalogHeaderEncoder.nextRecordingId(recordingId + 1);

        forceWrites(catalogChannel);

        nextRecordingId = recordingId + 1;
        nextRecordingDescriptorOffset = recordingDescriptorOffset + frameLength;
        catalogIndex.add(recordingId, recordingDescriptorOffset);

        return recordingId;
    }

    long addNewRecording(
        final long startPosition,
        final long startTimestamp,
        final int imageInitialTermId,
        final int segmentFileLength,
        final int termBufferLength,
        final int mtuLength,
        final int sessionId,
        final int streamId,
        final String strippedChannel,
        final String originalChannel,
        final String sourceIdentity)
    {
        return addNewRecording(
            startPosition,
            NULL_POSITION,
            startTimestamp,
            NULL_TIMESTAMP,
            imageInitialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity);
    }

    boolean wrapDescriptor(final long recordingId, final UnsafeBuffer buffer)
    {
        if (recordingId < 0)
        {
            return false;
        }

        final int recordingDescriptorOffset = recordingDescriptorOffset(recordingId);
        if (recordingDescriptorOffset < 0)
        {
            return false;
        }

        return wrapDescriptorAtOffset(buffer, recordingDescriptorOffset) > 0;
    }

    int wrapDescriptorAtOffset(final UnsafeBuffer buffer, final int recordingDescriptorOffset)
    {
        final int recordingLength = fieldAccessBuffer.getInt(
            recordingDescriptorOffset + RecordingDescriptorHeaderDecoder.lengthEncodingOffset(), BYTE_ORDER);

        if (recordingLength > 0)
        {
            final int frameLength = align(recordingLength + DESCRIPTOR_HEADER_LENGTH, alignment);
            buffer.wrap(catalogByteBuffer, recordingDescriptorOffset, frameLength);
            return frameLength;
        }

        return -1;
    }

    boolean hasRecording(final long recordingId)
    {
        return recordingId >= 0 && recordingDescriptorOffset(recordingId) > 0;
    }

    int forEach(final CatalogEntryProcessor consumer)
    {
        int count = 0;
        int offset = firstRecordingDescriptorOffset;
        while (offset < nextRecordingDescriptorOffset)
        {
            final int frameLength = wrapDescriptorAtOffset(catalogBuffer, offset);
            if (frameLength < 0)
            {
                break;
            }

            invokeEntryProcessor(offset, consumer);

            offset += frameLength;
            count++;
        }

        return count;
    }

    boolean forEntry(final long recordingId, final CatalogEntryProcessor consumer)
    {
        if (recordingId < 0)
        {
            return false;
        }

        final int recordingDescriptorOffset = recordingDescriptorOffset(recordingId);
        if (recordingDescriptorOffset < 0)
        {
            return false;
        }

        if (wrapDescriptorAtOffset(catalogBuffer, recordingDescriptorOffset) > 0)
        {
            invokeEntryProcessor(recordingDescriptorOffset, consumer);

            return true;
        }

        return false;
    }

    long findLast(final long minRecordingId, final int sessionId, final int streamId, final byte[] channelFragment)
    {
        if (minRecordingId < 0)
        {
            return NULL_RECORD_ID;
        }

        final long[] index = catalogIndex.index();
        final int lastPosition = catalogIndex.lastPosition();
        for (int i = lastPosition; i >= 0; i -= 2)
        {
            final long recordingId = index[i];
            if (recordingId < minRecordingId)
            {
                break;
            }

            wrapDescriptorAtOffset(catalogBuffer, (int)index[i + 1]);

            descriptorDecoder.wrap(
                catalogBuffer,
                DESCRIPTOR_HEADER_LENGTH,
                RecordingDescriptorDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.SCHEMA_VERSION);

            if (sessionId == descriptorDecoder.sessionId() &&
                streamId == descriptorDecoder.streamId() &&
                originalChannelContains(descriptorDecoder, channelFragment))
            {
                return recordingId;
            }
        }

        return NULL_RECORD_ID;
    }

    //
    // Methods for accessing specific record fields by recordingId.
    // Note: These methods are thread safe.
    /////////////////////////////////////////////////////////////

    static boolean originalChannelContains(
        final RecordingDescriptorDecoder descriptorDecoder, final byte[] channelFragment)
    {
        final int fragmentLength = channelFragment.length;
        if (0 == fragmentLength)
        {
            return true;
        }

        final int limit = descriptorDecoder.limit();
        final int strippedChannelLength = descriptorDecoder.strippedChannelLength();
        final int originalChannelOffset =
            limit + RecordingDescriptorDecoder.strippedChannelHeaderLength() + strippedChannelLength;

        descriptorDecoder.limit(originalChannelOffset);
        final int channelLength = descriptorDecoder.originalChannelLength();
        descriptorDecoder.limit(limit);

        final DirectBuffer buffer = descriptorDecoder.buffer();
        int offset = descriptorDecoder.offset() + descriptorDecoder.sbeBlockLength() +
            RecordingDescriptorDecoder.strippedChannelHeaderLength() + strippedChannelLength +
            RecordingDescriptorDecoder.originalChannelHeaderLength();

        nextChar:
        for (int end = offset + (channelLength - fragmentLength); offset <= end; offset++)
        {
            for (int i = 0; i < fragmentLength; i++)
            {
                if (buffer.getByte(offset + i) != channelFragment[i])
                {
                    continue nextChar;
                }
            }

            return true;
        }

        return false;
    }

    void recordingStopped(final long recordingId, final long position, final long timestampMs)
    {
        final int recordingDescriptorOffset = recordingDescriptorOffset(recordingId);
        final int offset = recordingDescriptorOffset + DESCRIPTOR_HEADER_LENGTH;
        final long stopPosition = nativeOrder() == BYTE_ORDER ? position : Long.reverseBytes(position);

        fieldAccessBuffer.putLong(offset + stopTimestampEncodingOffset(), timestampMs, BYTE_ORDER);
        fieldAccessBuffer.putLongVolatile(offset + stopPositionEncodingOffset(), stopPosition);
        updateChecksum(recordingDescriptorOffset);
        forceWrites(catalogChannel);
    }

    void stopPosition(final long recordingId, final long position)
    {
        final int recordingDescriptorOffset = recordingDescriptorOffset(recordingId);
        final int offset = recordingDescriptorOffset + DESCRIPTOR_HEADER_LENGTH;
        final long stopPosition = nativeOrder() == BYTE_ORDER ? position : Long.reverseBytes(position);

        fieldAccessBuffer.putLongVolatile(offset + stopPositionEncodingOffset(), stopPosition);
        updateChecksum(recordingDescriptorOffset);
        forceWrites(catalogChannel);
    }

    void extendRecording(
        final long recordingId, final long controlSessionId, final long correlationId, final int sessionId)
    {
        final int recordingDescriptorOffset = recordingDescriptorOffset(recordingId);
        final int offset = recordingDescriptorOffset + DESCRIPTOR_HEADER_LENGTH;
        final long stopPosition = nativeOrder() == BYTE_ORDER ? NULL_POSITION : Long.reverseBytes(NULL_POSITION);

        fieldAccessBuffer.putLong(offset + controlSessionIdEncodingOffset(), controlSessionId, BYTE_ORDER);
        fieldAccessBuffer.putLong(offset + correlationIdEncodingOffset(), correlationId, BYTE_ORDER);
        fieldAccessBuffer.putLong(offset + stopTimestampEncodingOffset(), NULL_TIMESTAMP, BYTE_ORDER);
        fieldAccessBuffer.putInt(offset + sessionIdEncodingOffset(), sessionId, BYTE_ORDER);
        fieldAccessBuffer.putLongVolatile(offset + stopPositionEncodingOffset(), stopPosition);
        updateChecksum(recordingDescriptorOffset);
        forceWrites(catalogChannel);
    }

    void replaceRecording(
        final long recordingId,
        final long startPosition,
        final long stopPosition,
        final long startTimestamp,
        final long stopTimestamp,
        final int imageInitialTermId,
        final int segmentFileLength,
        final int termBufferLength,
        final int mtuLength,
        final int sessionId,
        final int streamId,
        final String strippedChannel,
        final String originalChannel,
        final String sourceIdentity)
    {
        final int recordingOffset = recordingDescriptorOffset(recordingId);
        if (-1 == recordingOffset)
        {
            throw new ArchiveException("unknown recording id: " + recordingId);
        }

        final int recordingLength = fieldAccessBuffer.getInt(
            recordingOffset + RecordingDescriptorHeaderDecoder.lengthEncodingOffset(), BYTE_ORDER);
        final int oldFrameLength = align(recordingLength + DESCRIPTOR_HEADER_LENGTH, alignment);
        final int newFrameLength = recordingDescriptorFrameLength(strippedChannel, originalChannel, sourceIdentity);
        final int length, checksumLength;
        if (newFrameLength > oldFrameLength)
        {
            length = checksumLength = newFrameLength - DESCRIPTOR_HEADER_LENGTH;

            final int shiftBytes = newFrameLength - oldFrameLength;
            final int endOfLastRecording = nextRecordingDescriptorOffset;
            if (endOfLastRecording + shiftBytes > capacity)
            {
                growCatalog(MAX_CATALOG_LENGTH, shiftBytes);
            }

            nextRecordingDescriptorOffset += shiftBytes;

            final long[] index = catalogIndex.index();
            final int lastPosition = catalogIndex.lastPosition();
            if (recordingId != index[lastPosition])
            {
                shiftDataToTheRight(recordingOffset, oldFrameLength, newFrameLength, endOfLastRecording);
                fixupIndexForShifterRecordings(index, lastPosition, recordingId, shiftBytes);
            }

            catalogBuffer.wrap(catalogByteBuffer, recordingOffset, newFrameLength);
        }
        else
        {
            length = oldFrameLength - DESCRIPTOR_HEADER_LENGTH;
            checksumLength = newFrameLength - DESCRIPTOR_HEADER_LENGTH;
            catalogBuffer.wrap(catalogByteBuffer, recordingOffset, oldFrameLength);
            catalogBuffer.setMemory(newFrameLength, oldFrameLength - newFrameLength, (byte)0);
        }

        descriptorEncoder
            .wrap(catalogBuffer, DESCRIPTOR_HEADER_LENGTH)
            .recordingId(recordingId)
            .startTimestamp(startTimestamp)
            .stopTimestamp(stopTimestamp)
            .startPosition(startPosition)
            .stopPosition(stopPosition)
            .initialTermId(imageInitialTermId)
            .segmentFileLength(segmentFileLength)
            .termBufferLength(termBufferLength)
            .mtuLength(mtuLength)
            .sessionId(sessionId)
            .streamId(streamId)
            .strippedChannel(strippedChannel)
            .originalChannel(originalChannel)
            .sourceIdentity(sourceIdentity);

        descriptorHeaderEncoder
            .wrap(catalogBuffer, 0)
            .length(length)
            .checksum(computeRecordingDescriptorChecksum(recordingOffset, checksumLength))
            .state(VALID);

        forceWrites(catalogChannel);
    }

    long startPosition(final long recordingId)
    {
        final int offset = recordingDescriptorOffset(recordingId) +
            DESCRIPTOR_HEADER_LENGTH + startPositionEncodingOffset();

        final long startPosition = fieldAccessBuffer.getLongVolatile(offset);

        return nativeOrder() == BYTE_ORDER ? startPosition : Long.reverseBytes(startPosition);
    }

    void startPosition(final long recordingId, final long position)
    {
        final int recordingDescriptorOffset = recordingDescriptorOffset(recordingId);
        final int offset = recordingDescriptorOffset +
            DESCRIPTOR_HEADER_LENGTH + startPositionEncodingOffset();

        fieldAccessBuffer.putLong(offset, position, BYTE_ORDER);
        updateChecksum(recordingDescriptorOffset);
        forceWrites(catalogChannel);
    }

    long stopPosition(final long recordingId)
    {
        final int offset = recordingDescriptorOffset(recordingId) +
            DESCRIPTOR_HEADER_LENGTH + stopPositionEncodingOffset();

        final long stopPosition = fieldAccessBuffer.getLongVolatile(offset);

        return nativeOrder() == BYTE_ORDER ? stopPosition : Long.reverseBytes(stopPosition);
    }

    boolean changeState(final long recordingId, final RecordingState newState)
    {
        if (recordingId >= 0)
        {
            final long offset = catalogIndex.remove(recordingId);
            if (CatalogIndex.NULL_VALUE != offset)
            {
                fieldAccessBuffer.putInt(
                    (int)offset + RecordingDescriptorHeaderEncoder.stateEncodingOffset(),
                    newState.value(),
                    BYTE_ORDER);

                forceWrites(catalogChannel);

                return true;
            }
        }

        return false;
    }

    RecordingSummary recordingSummary(final long recordingId, final RecordingSummary summary)
    {
        final int offset = recordingDescriptorOffset(recordingId) + DESCRIPTOR_HEADER_LENGTH;

        summary.recordingId = recordingId;
        summary.startPosition = fieldAccessBuffer.getLong(offset + startPositionEncodingOffset(), BYTE_ORDER);
        summary.stopPosition = fieldAccessBuffer.getLong(offset + stopPositionEncodingOffset(), BYTE_ORDER);
        summary.initialTermId = fieldAccessBuffer.getInt(offset + initialTermIdEncodingOffset(), BYTE_ORDER);
        summary.segmentFileLength = fieldAccessBuffer.getInt(offset + segmentFileLengthEncodingOffset(), BYTE_ORDER);
        summary.termBufferLength = fieldAccessBuffer.getInt(offset + termBufferLengthEncodingOffset(), BYTE_ORDER);
        summary.mtuLength = fieldAccessBuffer.getInt(offset + mtuLengthEncodingOffset(), BYTE_ORDER);
        summary.streamId = fieldAccessBuffer.getInt(offset + streamIdEncodingOffset(), BYTE_ORDER);
        summary.sessionId = fieldAccessBuffer.getInt(offset + sessionIdEncodingOffset(), BYTE_ORDER);

        return summary;
    }

    static int descriptorLength(final UnsafeBuffer descriptorBuffer)
    {
        return descriptorBuffer.getInt(RecordingDescriptorHeaderDecoder.lengthEncodingOffset(), BYTE_ORDER);
    }

    static boolean isValidDescriptor(final UnsafeBuffer descriptorBuffer)
    {
        return VALID.value() ==
            descriptorBuffer.getInt(RecordingDescriptorHeaderDecoder.stateEncodingOffset(), BYTE_ORDER);
    }

    static long recordingId(final UnsafeBuffer descriptorBuffer)
    {
        return descriptorBuffer.getLong(DESCRIPTOR_HEADER_LENGTH + recordingIdEncodingOffset(), BYTE_ORDER);
    }

    int recordingDescriptorOffset(final long recordingId)
    {
        final long recordingDescriptorOffset = catalogIndex.recordingOffset(recordingId);
        if (CatalogIndex.NULL_VALUE == recordingDescriptorOffset)
        {
            return -1;
        }
        return (int)recordingDescriptorOffset;
    }

    void growCatalog(final long maxCatalogCapacity, final int frameLength)
    {
        final long oldCapacity = capacity;
        final long recordingOffset = nextRecordingDescriptorOffset;
        final long targetCapacity = recordingOffset + frameLength;
        if (targetCapacity > maxCatalogCapacity)
        {
            if (maxCatalogCapacity == oldCapacity)
            {
                throw new ArchiveException("catalog is full, max capacity reached: " + maxCatalogCapacity);
            }
            else
            {
                throw new ArchiveException(
                    "recording is too big: total recording length is " + frameLength + " bytes," +
                    " available space is " + (maxCatalogCapacity - recordingOffset) + " bytes");
            }
        }

        long newCapacity = oldCapacity;
        while (newCapacity < targetCapacity)
        {
            newCapacity = min(newCapacity + (newCapacity >> 1), maxCatalogCapacity);
        }

        final MappedByteBuffer mappedByteBuffer;
        try
        {
            unmapAndCloseChannel();
            catalogChannel = FileChannel.open(catalogFile.toPath(), READ, WRITE, SPARSE);
            mappedByteBuffer = catalogChannel.map(READ_WRITE, 0, newCapacity);
        }
        catch (final Exception ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
            return;
        }

        capacity = newCapacity;
        initBuffers(mappedByteBuffer);

        final UnsafeBuffer catalogHeaderBuffer = new UnsafeBuffer(catalogByteBuffer);
        catalogHeaderDecoder.wrap(
            catalogHeaderBuffer, 0, CatalogHeaderDecoder.BLOCK_LENGTH, CatalogHeaderDecoder.SCHEMA_VERSION);
        catalogHeaderEncoder.wrap(catalogHeaderBuffer, 0);

        catalogResized(oldCapacity, newCapacity);
    }

    void updateChecksum(final int recordingDescriptorOffset)
    {
        if (null != checksum)
        {
            final UnsafeBuffer headerBuffer = this.headerAccessBuffer;
            final int recordingLength = headerBuffer.getInt(
                recordingDescriptorOffset + RecordingDescriptorHeaderEncoder.lengthEncodingOffset(), BYTE_ORDER);
            final int checksumValue = checksum.compute(
                catalogByteBufferAddress, DESCRIPTOR_HEADER_LENGTH + recordingDescriptorOffset, recordingLength);
            headerBuffer.putInt(
                recordingDescriptorOffset + RecordingDescriptorHeaderEncoder.checksumEncodingOffset(),
                checksumValue,
                BYTE_ORDER);
        }
    }

    int computeRecordingDescriptorChecksum(final int recordingDescriptorOffset, final int recordingLength)
    {
        if (null != checksum)
        {
            return checksum.compute(
                catalogByteBufferAddress,
                DESCRIPTOR_HEADER_LENGTH + recordingDescriptorOffset,
                recordingLength);
        }

        return 0;
    }

    void catalogResized(final long oldCapacity, final long newCapacity)
    {
//        System.out.println("Catalog capacity changed: " + oldCapacity + " bytes => " + newCapacity + " bytes");
    }

    private static void validateCapacity(final long catalogCapacity)
    {
        if (catalogCapacity < MIN_CAPACITY || catalogCapacity > MAX_CATALOG_LENGTH)
        {
            throw new IllegalArgumentException("Invalid catalog capacity provided: expected value >= " +
                MIN_CAPACITY + ", got " + catalogCapacity);
        }
    }

    private void initBuffers(final MappedByteBuffer catalogMappedByteBuffer)
    {
        catalogByteBuffer = catalogMappedByteBuffer;
        catalogByteBuffer.order(BYTE_ORDER);
        catalogBuffer = new UnsafeBuffer(catalogByteBuffer);
        catalogByteBufferAddress = catalogBuffer.addressOffset();
        fieldAccessBuffer = new UnsafeBuffer(catalogByteBuffer);
        headerAccessBuffer = new UnsafeBuffer(catalogByteBuffer);
    }

    private void buildIndex(final boolean writable)
    {
        final int endOffset = (int)capacity;
        int offset = firstRecordingDescriptorOffset;
        long recordingId = -1;
        while (offset < endOffset)
        {
            final int frameLength = wrapDescriptorAtOffset(catalogBuffer, offset);
            if (frameLength < 0)
            {
                break;
            }

            recordingId = recordingId(catalogBuffer);
            if (isValidDescriptor(catalogBuffer))
            {
                catalogIndex.add(recordingId, offset);
            }

            offset += frameLength;
        }

        nextRecordingDescriptorOffset = offset;

        if (0 == nextRecordingId)
        {
            nextRecordingId = recordingId + 1;
        }
        else if (writable && nextRecordingId < recordingId + 1)
        {
            throw new ArchiveException("invalid nextRecordingId: expected value greater or equal to " +
                (recordingId + 1) + ", was " + nextRecordingId);
        }
    }

    private void invokeEntryProcessor(final int recordingDescriptorOffset, final CatalogEntryProcessor consumer)
    {
        descriptorHeaderDecoder.wrap(
            catalogBuffer, 0, DESCRIPTOR_HEADER_LENGTH, RecordingDescriptorHeaderDecoder.SCHEMA_VERSION);

        descriptorHeaderEncoder.wrap(catalogBuffer, 0);

        descriptorDecoder.wrap(
            catalogBuffer,
            DESCRIPTOR_HEADER_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);

        descriptorEncoder.wrap(catalogBuffer, DESCRIPTOR_HEADER_LENGTH);

        consumer.accept(
            recordingDescriptorOffset,
            descriptorHeaderEncoder,
            descriptorHeaderDecoder,
            descriptorEncoder,
            descriptorDecoder);
    }

    private int recordingDescriptorFrameLength(
        final String strippedChannel, final String originalChannel, final String sourceIdentity)
    {
        final int recordingDescriptorLength =
            7 * SIZE_OF_LONG +
            6 * SIZE_OF_INT +
            SIZE_OF_INT + strippedChannel.length() +
            SIZE_OF_INT + originalChannel.length() +
            SIZE_OF_INT + sourceIdentity.length();
        return align(DESCRIPTOR_HEADER_LENGTH + recordingDescriptorLength, alignment);
    }

    private void shiftDataToTheRight(
        final int recordingOffset, final int oldFrameLength, final int newFrameLength, final int endOfLastRecording)
    {
        fieldAccessBuffer.putBytes(
            recordingOffset + newFrameLength,
            fieldAccessBuffer,
            recordingOffset + oldFrameLength,
            endOfLastRecording - (recordingOffset + oldFrameLength));
    }

    private static void fixupIndexForShifterRecordings(
        final long[] index, final int lastPosition, final long recordingId, final int shiftBytes)
    {
        boolean updateOffset = false;
        for (int i = 0; i <= lastPosition; i += 2)
        {
            if (updateOffset)
            {
                index[i + 1] += shiftBytes;
            }
            else if (recordingId == index[i])
            {
                updateOffset = true;
            }
        }
    }

    /**
     * On catalog load we verify entries are in coherent state and attempt to recover entries data where untimely
     * termination of recording has resulted in an unaccounted for stopPosition/stopTimestamp. This operation may be
     * expensive for large catalogs.
     *
     * @param fixOnRefresh set if the catalog should have its entries fixed.
     * @param checksum     to validate the last fragment upon the page straddle.
     * @param buffer       to recover the stop position.
     */
    @SuppressWarnings("checkstyle:indentation")
    private void refreshCatalog(final boolean fixOnRefresh, final Checksum checksum, final UnsafeBuffer buffer)
    {
        if (fixOnRefresh)
        {
            final UnsafeBuffer tmpBuffer = null != buffer ?
                buffer : new UnsafeBuffer(ByteBuffer.allocateDirect(FILE_IO_MAX_LENGTH_DEFAULT));
            final Long2ObjectHashMap<List<String>> segmentFiles = indexSegmentFiles(archiveDir);
            forEach((recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                refreshAndFixDescriptor(
                    headerDecoder, descriptorEncoder, descriptorDecoder, segmentFiles, checksum, tmpBuffer));
        }
    }

    private void refreshAndFixDescriptor(
        final RecordingDescriptorHeaderDecoder headerDecoder,
        final RecordingDescriptorEncoder encoder,
        final RecordingDescriptorDecoder decoder,
        final Long2ObjectHashMap<List<String>> segmentFilesByRecordingId,
        final Checksum checksum,
        final UnsafeBuffer buffer)
    {
        final long recordingId = decoder.recordingId();
        if (VALID == headerDecoder.state() && NULL_POSITION == decoder.stopPosition())
        {
            final List<String> segmentFiles = segmentFilesByRecordingId.getOrDefault(recordingId, emptyList());
            final String maxSegmentFile = findSegmentFileWithHighestPosition(segmentFiles);

            encoder.stopPosition(computeStopPosition(
                archiveDir,
                maxSegmentFile,
                decoder.startPosition(),
                decoder.termBufferLength(),
                decoder.segmentFileLength(),
                checksum,
                buffer,
                (segmentFile) ->
                {
                    throw new ArchiveException(
                        "Found potentially incomplete last fragment straddling page boundary in file: " +
                        segmentFile.getAbsolutePath() +
                        "\nRun `ArchiveTool verify` for corrective action!");
                }));

            encoder.stopTimestamp(epochClock.time());
        }
    }

    private void forceWrites(final FileChannel channel)
    {
        fsync(channel, forceWrites, forceMetadata);
    }

    private void fsync(final FileChannel channel, final boolean forceWrites, final boolean forceMetadata)
    {
        if (null != channel && forceWrites)
        {
            try
            {
                channel.force(forceMetadata);
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }
    }

    static ArrayList<String> listSegmentFiles(final File archiveDir, final long recordingId)
    {
        final ArrayList<String> segmentFiles = new ArrayList<>();
        final String[] files = archiveDir.list();

        if (null != files)
        {
            final String prefix = recordingId + "-";

            for (final String file : files)
            {
                if (file.startsWith(prefix) && file.endsWith(RECORDING_SEGMENT_SUFFIX))
                {
                    segmentFiles.add(file);
                }
            }
        }

        return segmentFiles;
    }

    static Long2ObjectHashMap<List<String>> indexSegmentFiles(final File archiveDir)
    {
        final Long2ObjectHashMap<List<String>> index = new Long2ObjectHashMap<>();
        final String[] files = archiveDir.list();

        if (null != files)
        {
            for (final String file : files)
            {
                if (file.endsWith(RECORDING_SEGMENT_SUFFIX))
                {
                    try
                    {
                        final long recordingId = parseSegmentFileRecordingId(file);
                        index.computeIfAbsent(recordingId, r -> new ArrayList<>()).add(file);
                    }
                    catch (final InvalidRecordingNameException ignore)
                    {
                        // Just skip over invalid files.
                    }
                }
            }
        }

        return index;
    }

    static String findSegmentFileWithHighestPosition(final List<String> segmentFiles)
    {
        long maxSegmentPosition = NULL_POSITION;
        String maxFileName = null;

        for (final String filename : segmentFiles)
        {
            final long filePosition = parseSegmentFilePosition(filename);
            if (filePosition < 0)
            {
                throw new ArchiveException("negative position encoded in the file name: " + filename);
            }

            if (filePosition > maxSegmentPosition)
            {
                maxSegmentPosition = filePosition;
                maxFileName = filename;
            }
        }

        return maxFileName;
    }

    static long parseSegmentFilePosition(final String filename)
    {
        final int dashOffset = filename.indexOf('-');
        if (-1 == dashOffset)
        {
            throw new ArchiveException("invalid filename format: " + filename);
        }

        final int positionOffset = dashOffset + 1;
        final int positionLength = filename.length() - positionOffset - RECORDING_SEGMENT_SUFFIX.length();
        if (0 >= positionLength)
        {
            throw new ArchiveException("no position encoded in the segment file: " + filename);
        }

        return parseLongAscii(filename, positionOffset, positionLength);
    }

    static long parseSegmentFileRecordingId(final String filename)
    {
        final int dashOffset = filename.indexOf('-');
        if (-1 == dashOffset || 0 == dashOffset)
        {
            throw new InvalidRecordingNameException("invalid filename format: " + filename);
        }

        return parseLongAscii(filename, 0, dashOffset);
    }

    static long computeStopPosition(
        final File archiveDir,
        final String maxSegmentFile,
        final long startPosition,
        final int termLength,
        final int segmentLength,
        final Checksum checksum,
        final UnsafeBuffer buffer,
        final Predicate<File> truncateOnPageStraddle)
    {
        if (null == maxSegmentFile)
        {
            return startPosition;
        }
        else
        {
            final int startTermOffset = (int)(startPosition & (termLength - 1));
            final long startTermBasePosition = startPosition - startTermOffset;
            final long segmentFileBasePosition = parseSegmentFilePosition(maxSegmentFile);
            final int fileOffset = segmentFileBasePosition == startTermBasePosition ? startTermOffset : 0;
            final int segmentStopOffset = recoverStopOffset(
                archiveDir, maxSegmentFile, fileOffset, segmentLength, truncateOnPageStraddle, checksum, buffer);

            return max(segmentFileBasePosition + segmentStopOffset, startPosition);
        }
    }

    static boolean fragmentStraddlesPageBoundary(final int fragmentOffset, final int fragmentLength)
    {
        return (fragmentOffset / PAGE_SIZE) != ((fragmentOffset + (fragmentLength - 1)) / PAGE_SIZE);
    }

    private void unmapAndCloseChannel()
    {
        final MappedByteBuffer buffer = this.catalogByteBuffer;
        BufferUtil.free(buffer);
        this.catalogByteBuffer = null;
        fsync(catalogChannel, true, true);
        CloseHelper.close(catalogChannel);
    }

    private static int recoverStopOffset(
        final File archiveDir,
        final String segmentFile,
        final int offset,
        final int segmentFileLength,
        final Predicate<File> truncateOnPageStraddle,
        final Checksum checksum,
        final UnsafeBuffer buffer)
    {
        final File file = new File(archiveDir, segmentFile);
        try (FileChannel segmentFileChannel = FileChannel.open(file.toPath(), READ, WRITE))
        {
            final int offsetLimit = (int)min(segmentFileLength, segmentFileChannel.size());
            final ByteBuffer byteBuffer = buffer.byteBuffer();

            int nextFragmentOffset = offset;
            int lastFragmentLength = 0;
            int bufferOffset = 0;

            out:
            while (nextFragmentOffset < offsetLimit)
            {
                final int bytesRead = readNextChunk(segmentFileChannel, byteBuffer, nextFragmentOffset, offsetLimit);

                bufferOffset = 0;
                while (bufferOffset < bytesRead)
                {
                    final int frameLength = frameLength(buffer, bufferOffset);
                    if (frameLength <= 0)
                    {
                        break out;
                    }

                    lastFragmentLength = align(frameLength, FRAME_ALIGNMENT);
                    nextFragmentOffset += lastFragmentLength;
                    bufferOffset += lastFragmentLength;
                }
            }

            final int lastFragmentOffset = nextFragmentOffset - lastFragmentLength;
            if (fragmentStraddlesPageBoundary(lastFragmentOffset, lastFragmentLength) &&
                !isValidFragment(buffer, bufferOffset - lastFragmentLength, lastFragmentLength, checksum) &&
                truncateOnPageStraddle.test(file))
            {
                final int singleByte = 1;
                segmentFileChannel.truncate(lastFragmentOffset);
                byteBuffer.put(0, (byte)0).limit(singleByte).position(0);
                if (singleByte != segmentFileChannel.write(byteBuffer, segmentFileLength - 1))
                {
                    throw new IllegalStateException("Failed to write single byte to set segment file length");
                }

                return lastFragmentOffset;
            }
            else
            {
                return nextFragmentOffset;
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
            return Aeron.NULL_VALUE;
        }
    }

    private static int readNextChunk(
        final FileChannel segment, final ByteBuffer byteBuffer, final int offset, final int limit) throws IOException
    {
        int position = offset;
        byteBuffer.clear().limit(min(byteBuffer.capacity(), limit - position));
        do
        {
            final int bytesRead = segment.read(byteBuffer, position);
            if (bytesRead < 0)
            {
                break;
            }
            position += bytesRead;
        }
        while (byteBuffer.remaining() > 0);

        return position - offset;
    }

    private static boolean isValidFragment(
        final UnsafeBuffer buffer, final int fragmentOffset, final int alignedFragmentLength, final Checksum checksum)
    {
        return null != checksum && hasValidChecksum(buffer, fragmentOffset, alignedFragmentLength, checksum) ||
            hasDataInAllPagesAfterStraddle(buffer, fragmentOffset, alignedFragmentLength);
    }

    private static boolean hasValidChecksum(
        final UnsafeBuffer buffer, final int fragmentOffset, final int alignedFragmentLength, final Checksum checksum)
    {
        final int computedChecksum = checksum.compute(
            buffer.addressOffset(),
            fragmentOffset + HEADER_LENGTH,
            alignedFragmentLength - HEADER_LENGTH);
        final int recordedChecksum = frameSessionId(buffer, fragmentOffset);

        return recordedChecksum == computedChecksum;
    }

    private static boolean hasDataInAllPagesAfterStraddle(
        final UnsafeBuffer buffer, final int fragmentOffset, final int alignedFragmentLength)
    {
        int straddleOffset = (fragmentOffset / PAGE_SIZE + 1) * PAGE_SIZE;
        final int endOffset = fragmentOffset + alignedFragmentLength;
        while (straddleOffset < endOffset)
        {
            if (isEmptyPage(buffer, straddleOffset, endOffset))
            {
                return false;
            }
            straddleOffset += PAGE_SIZE;
        }

        return true;
    }

    private static boolean isEmptyPage(final UnsafeBuffer buffer, final int pageStart, final int endOffset)
    {
        for (int i = pageStart, pageEnd = min(pageStart + PAGE_SIZE, endOffset); i < pageEnd; i += SIZE_OF_LONG)
        {
            if (0L != buffer.getLong(i))
            {
                return false;
            }
        }

        return true;
    }

    public String toString()
    {
        return "Catalog{" +
            "forceWrites=" + forceWrites +
            ", forceMetadata=" + forceMetadata +
            ", isClosed=" + isClosed +
            ", catalogFile=" + catalogFile +
            ", archiveDir=" + archiveDir +
            ", epochClock=" + epochClock +
            ", checksum=" + checksum +
            ", alignment=" + alignment +
            ", firstRecordingDescriptorOffset=" + firstRecordingDescriptorOffset +
            ", catalogChannel=" + catalogChannel +
            ", capacity=" + capacity +
            ", nextRecordingId=" + nextRecordingId +
            ", nextRecordingDescriptorOffset=" + nextRecordingDescriptorOffset +
            '}';
    }

    static class InvalidRecordingNameException extends ArchiveException
    {
        private static final long serialVersionUID = -2374545383916725365L;

        InvalidRecordingNameException(final String message)
        {
            super(message);
        }
    }
}
