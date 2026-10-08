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

import io.aeron.archive.checksum.Checksum;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.CatalogHeaderEncoder;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import io.aeron.archive.codecs.RecordingState;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BufferUtil;
import org.agrona.IoUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.stream.Stream;

import static io.aeron.archive.Archive.Configuration.CATALOG_FILE_NAME;
import static io.aeron.archive.Archive.Configuration.FILE_IO_MAX_LENGTH_DEFAULT;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.Catalog.*;
import static io.aeron.archive.checksum.Checksums.crc32;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.NULL_TIMESTAMP;
import static io.aeron.archive.codecs.RecordingState.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.nio.ByteBuffer.allocate;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.file.StandardOpenOption.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

class CatalogTest
{
    private static final long CAPACITY = 1024;
    private static final int TERM_LENGTH = 2 * PAGE_SIZE;
    private static final int SEGMENT_LENGTH = 2 * TERM_LENGTH;
    private static final int MTU_LENGTH = 1024;

    private final UnsafeBuffer segmentFileBuffer = new UnsafeBuffer(allocate(FILE_IO_MAX_LENGTH_DEFAULT));
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer();
    private final RecordingDescriptorHeaderDecoder recordingDescriptorHeaderDecoder =
        new RecordingDescriptorHeaderDecoder();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
    private final File archiveDir = ArchiveTests.makeTestDirectory();

    private long currentTimeMs = 1;
    private final EpochClock clock = () -> currentTimeMs;

    private long recordingOneId;
    private long recordingTwoId;
    private long recordingThreeId;

    @BeforeEach
    void before()
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer))
        {
            recordingOneId = catalog.addNewRecording(
                0L, 0L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1, "channelG", "channelG?tag=f", "sourceA");
            recordingTwoId = catalog.addNewRecording(
                0L, 0L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 7, 2, "channelH", "channelH?tag=f", "sourceV");
            recordingThreeId = catalog.addNewRecording(
                0L,
                0L,
                0,
                SEGMENT_LENGTH,
                TERM_LENGTH, MTU_LENGTH,
                8,
                3,
                "channelThatIsVeryLongAndShouldNotBeTruncated",
                "channelThatIsVeryLongAndShouldNotBeTruncated?tag=f",
                "source can also be a very very very long String and it will not be truncated even " +
                    "if gets very very long");
        }
    }

    @AfterEach
    void after()
    {
        IoUtil.delete(archiveDir, false);
    }

    @Test
    void shouldCloseAndRemainReadableWhenTheClosingThreadIsInterrupted()
    {
        final Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer);
        try
        {
            Thread.currentThread().interrupt();

            assertDoesNotThrow(catalog::close);
            assertTrue(Thread.currentThread().isInterrupted());
        }
        finally
        {
            Thread.interrupted();
            catalog.close();
        }

        try (Catalog reopened = new Catalog(archiveDir, clock))
        {
            assertEquals(3, reopened.entryCount());
            assertTrue(reopened.wrapDescriptor(recordingThreeId, new UnsafeBuffer()));
        }
    }

    @Test
    void shouldCloseAfterAnInterruptClosedTheChannelDuringAnEarlierWrite()
    {
        final Catalog catalog = new Catalog(archiveDir, null, 1, CAPACITY, clock, null, segmentFileBuffer);
        try
        {
            Thread.currentThread().interrupt();
            assertThrows(
                ClosedByInterruptException.class, () -> catalog.recordingStopped(recordingTwoId, 4096, 42));
            Thread.interrupted();

            assertDoesNotThrow(catalog::close);
            assertFalse(Thread.currentThread().isInterrupted());
        }
        finally
        {
            Thread.interrupted();
            catalog.close();
        }

        try (Catalog reopened = new Catalog(archiveDir, clock))
        {
            assertEquals(4096, reopened.stopPosition(recordingTwoId));
        }
    }

    @Test
    void shouldUse1KBAlignmentWhenReadingFromOldCatalogFile() throws IOException
    {
        final int oldRecordLength = 1024;

        final File catalogFile = new File(archiveDir, CATALOG_FILE_NAME);
        IoUtil.deleteIfExists(catalogFile);
        Files.write(catalogFile.toPath(), new byte[oldRecordLength], CREATE_NEW);

        try (Catalog catalog = new Catalog(archiveDir, clock, MIN_CAPACITY, true, null, (version) -> {}))
        {
            assertEquals(oldRecordLength, catalog.alignment());
        }
    }

    @Test
    void shouldComputeNextRecordingIdIfValueInHeaderIsZero() throws IOException
    {
        setNextRecordingId(0);

        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer))
        {
            assertEquals(recordingThreeId + 1, catalog.nextRecordingId());
        }
    }

    @Test
    void shouldThrowArchiveExceptionIfNextRecordingIdIsSmallerThanTheActualLastRecordInTheCatalog() throws IOException
    {
        setNextRecordingId(recordingTwoId);

        final ArchiveException exception = assertThrows(ArchiveException.class,
            () -> new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer));
        assertEquals(
            "ERROR - invalid nextRecordingId: expected value greater or equal to " + (recordingThreeId + 1) +
            ", was " + recordingTwoId,
            exception.getMessage());
    }

    @Test
    void shouldThrowArchiveExceptionIfNextRecordingIdIsInvalidWriteableCatalog() throws IOException
    {
        setNextRecordingId(-1);

        final ArchiveException exception = assertThrows(ArchiveException.class,
            () -> new Catalog(archiveDir, clock, MIN_CAPACITY, true, null, null));
        assertEquals(
            "ERROR - invalid nextRecordingId: expected value greater or equal to " + (recordingThreeId + 1) +
            ", was -1",
            exception.getMessage());
    }

    @Test
    void shouldNotThrowArchiveExceptionWhenNextRecordingIdIsInvalidIfCatalogIsReadOnly()
        throws IOException
    {
        setNextRecordingId(recordingTwoId);

        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            assertEquals(recordingTwoId, catalog.nextRecordingId());
        }
    }

    @Test
    void shouldReadNextRecordingIdFromCatalogHeader() throws IOException
    {
        final long nextRecordingId = 10101010;
        setNextRecordingId(nextRecordingId);

        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer))
        {
            assertEquals(nextRecordingId, catalog.nextRecordingId());
        }
    }

    @ParameterizedTest
    @ValueSource(longs = { -1, 0, MIN_CAPACITY - 1 })
    void shouldThrowIllegalArgumentExceptionIfCatalogCapacityIsLessThanMinimalCapacity(final long capacity)
    {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> new Catalog(archiveDir, null, 0, capacity, clock, null, segmentFileBuffer));

        assertEquals(
            "Invalid catalog capacity provided: expected value >= " + MIN_CAPACITY + ", got " + capacity,
            exception.getMessage());
    }

    @Test
    void shouldReloadExistingIndex()
    {
        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            verifyRecordingForId(
                catalog,
                recordingOneId,
                160,
                0L,
                NULL_POSITION,
                0L,
                NULL_TIMESTAMP,
                0,
                SEGMENT_LENGTH,
                TERM_LENGTH,
                MTU_LENGTH,
                6,
                1,
                "channelG",
                "channelG?tag=f",
                "sourceA");

            verifyRecordingForId(
                catalog,
                recordingTwoId,
                160,
                0L,
                NULL_POSITION,
                0L,
                NULL_TIMESTAMP,
                0,
                SEGMENT_LENGTH,
                TERM_LENGTH,
                MTU_LENGTH,
                7,
                2,
                "channelH",
                "channelH?tag=f",
                "sourceV");

            verifyRecordingForId(
                catalog,
                recordingThreeId,
                352,
                0L,
                NULL_POSITION,
                0L,
                NULL_TIMESTAMP,
                0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 8,
                3,
                "channelThatIsVeryLongAndShouldNotBeTruncated",
                "channelThatIsVeryLongAndShouldNotBeTruncated?tag=f",
                "source can also be a very very very long String and it will not be truncated even if gets " +
                "very very long");
        }
    }

    @Test
    void shouldAppendToExistingIndex()
    {
        final long newRecordingId;
        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, () -> 3L, null, segmentFileBuffer))
        {
            newRecordingId = catalog.addNewRecording(
                32,
                128,
                21,
                42,
                5,
                SEGMENT_LENGTH,
                TERM_LENGTH,
                MTU_LENGTH,
                9,
                4,
                "channelJ",
                "channelJ?tag=f",
                "sourceN");
        }

        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            verifyRecordingForId(
                catalog,
                recordingOneId,
                160,
                0L,
                0L, // updated from NULL_POSITION when Catalog was created for write
                0L,
                3L, // updated from NULL_TIMESTAMP when Catalog was created for write
                0,
                SEGMENT_LENGTH,
                TERM_LENGTH,
                MTU_LENGTH,
                6,
                1,
                "channelG",
                "channelG?tag=f",
                "sourceA");

            verifyRecordingForId(
                catalog,
                newRecordingId,
                160,
                32,
                128,
                21,
                42,
                5,
                SEGMENT_LENGTH,
                TERM_LENGTH,
                MTU_LENGTH,
                9,
                4,
                "channelJ",
                "channelJ?tag=f",
                "sourceN");
        }
    }

    @Test
    void shouldAllowMultipleInstancesForSameStream()
    {
        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            assertEquals(CAPACITY, catalog.capacity());
            final long newRecordingId = newRecording();
            assertNotEquals(recordingOneId, newRecordingId);
        }
    }

    @Test
    void shouldIncreaseCapacity()
    {
        final long newCapacity = CAPACITY * 2;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, newCapacity, clock, null, segmentFileBuffer))
        {
            assertEquals(newCapacity, catalog.capacity());
        }
    }

    @ParameterizedTest
    @ValueSource(longs = { MIN_CAPACITY, CAPACITY - 1, CAPACITY })
    void shouldNotDecreaseCapacity(final long newCapacity)
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, newCapacity, clock, null, segmentFileBuffer))
        {
            assertEquals(CAPACITY, catalog.capacity());
        }
    }

    @Test
    void shouldFixTimestampForEmptyRecordingAfterFailure()
    {
        final long newRecordingId = newRecording();

        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            final CatalogEntryProcessor entryProcessor =
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                assertEquals(NULL_TIMESTAMP, descriptorDecoder.stopTimestamp());

            assertTrue(catalog.forEntry(newRecordingId, entryProcessor));
        }

        currentTimeMs = 42L;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer))
        {
            final CatalogEntryProcessor entryProcessor =
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                assertEquals(42L, descriptorDecoder.stopTimestamp());

            assertTrue(catalog.forEntry(newRecordingId, entryProcessor));
        }
    }

    @Test
    void shouldFixTimestampAndPositionAfterFailureSamePage() throws Exception
    {
        final long newRecordingId = newRecording();

        assertTrue(new File(archiveDir, segmentFileName(newRecordingId, 0)).createNewFile());
        assertTrue(new File(archiveDir, segmentFileName(newRecordingId, SEGMENT_LENGTH)).createNewFile());
        assertTrue(new File(archiveDir, segmentFileName(newRecordingId, 2 * SEGMENT_LENGTH)).createNewFile());
        final File segmentFile = new File(archiveDir, segmentFileName(newRecordingId, 3 * SEGMENT_LENGTH));

        try (FileChannel log = FileChannel.open(segmentFile.toPath(), READ, WRITE, CREATE))
        {
            final ByteBuffer bb = allocate(HEADER_LENGTH);
            final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(bb);
            flyweight.frameLength(1024);
            log.write(bb);

            bb.clear();
            flyweight.frameLength(128);
            log.write(bb, 1024);

            bb.clear();
            flyweight.frameLength(0);
            log.write(bb, 1024 + 128);
        }

        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            assertTrue(catalog.forEntry(
                newRecordingId,
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                {
                    assertEquals(NULL_TIMESTAMP, descriptorDecoder.stopTimestamp());
                    assertEquals(NULL_POSITION, descriptorDecoder.stopPosition());
                }));
        }

        currentTimeMs = 42L;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer))
        {
            assertTrue(catalog.forEntry(
                newRecordingId,
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                {
                    assertEquals(42L, descriptorDecoder.stopTimestamp());
                    assertEquals(SEGMENT_LENGTH * 3 + 1024L + 128L, descriptorDecoder.stopPosition());
                }));
        }
    }

    @Test
    void shouldThrowExceptionAfterFailureOnPageStraddle() throws Exception
    {
        final long newRecordingId = newRecording();
        final File segmentFile = new File(archiveDir, segmentFileName(newRecordingId, 0));
        try (FileChannel log = FileChannel.open(segmentFile.toPath(), READ, WRITE, CREATE))
        {
            final ByteBuffer bb = allocate(HEADER_LENGTH);
            final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(bb);
            flyweight.frameLength(PAGE_SIZE - 128);
            log.write(bb);

            bb.clear();
            flyweight.frameLength(256);
            log.write(bb, PAGE_SIZE - 128);

            bb.clear();
            bb.put(0, (byte)0).limit(1).position(0);
            log.write(bb, PAGE_SIZE + 127);
        }

        final ArchiveException exception = assertThrows(
            ArchiveException.class,
            () ->
            {
                final Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer);
                catalog.close();
            });
        assertThat(exception.getMessage(), containsString(segmentFile.getAbsolutePath()));
    }

    @Test
    void shouldUseChecksumToVerifyLastFragmentAfterPageStraddle() throws Exception
    {
        final long newRecordingId = newRecording();
        final File segmentFile = new File(archiveDir, segmentFileName(newRecordingId, 0));
        try (FileChannel log = FileChannel.open(segmentFile.toPath(), READ, WRITE, CREATE))
        {
            final ByteBuffer bb = allocate(HEADER_LENGTH);
            final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(bb);
            flyweight.frameLength(PAGE_SIZE - 128);
            log.write(bb);

            bb.clear();
            flyweight.frameLength(256);
            flyweight.sessionId(1025596259);
            log.write(bb, PAGE_SIZE - 128);

            bb.clear();
            bb.put(0, (byte)0).limit(1).position(0);
            log.write(bb, PAGE_SIZE + 127);
        }

        currentTimeMs = 42L;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, crc32(), null))
        {
            assertTrue(catalog.forEntry(
                newRecordingId,
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                {
                    assertEquals(42L, descriptorDecoder.stopTimestamp());
                    assertEquals(PAGE_SIZE + 128, descriptorDecoder.stopPosition());
                }));
        }
    }

    private long newRecording()
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer))
        {
            return catalog.addNewRecording(
                0L,
                0L,
                0,
                SEGMENT_LENGTH,
                TERM_LENGTH,
                MTU_LENGTH,
                6,
                1,
                "channelG",
                "channelG?tag=f",
                "sourceA");
        }
    }

    @Test
    void shouldFixTimestampAndPositionAfterFailureFullSegment() throws Exception
    {
        final long newRecordingId = newRecording();

        final File segmentFile = new File(archiveDir, segmentFileName(newRecordingId, 0));
        try (FileChannel log = FileChannel.open(segmentFile.toPath(), READ, WRITE, CREATE))
        {
            final ByteBuffer bb = allocate(HEADER_LENGTH);
            final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(bb);
            flyweight.frameLength(SEGMENT_LENGTH - 128);
            log.write(bb);

            bb.clear();
            flyweight.frameLength(128);
            log.write(bb, SEGMENT_LENGTH - 128);
            log.truncate(SEGMENT_LENGTH);
        }

        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            assertTrue(catalog.forEntry(
                newRecordingId,
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                {
                    assertThat(descriptorDecoder.stopTimestamp(), is(NULL_TIMESTAMP));
                    assertThat(descriptorDecoder.stopPosition(), is(NULL_POSITION));
                }));
        }

        currentTimeMs = 42L;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer))
        {
            assertTrue(catalog.forEntry(
                newRecordingId,
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                {
                    assertThat(descriptorDecoder.stopTimestamp(), is(42L));
                    assertThat(descriptorDecoder.stopPosition(), is((long)SEGMENT_LENGTH));
                }));
        }
    }

    @Test
    void shouldNotGrowCatalogWhenReachingFullIfRecordingsFit()
    {
        after();
        final File archiveDir = ArchiveTests.makeTestDirectory();
        final long capacity = 384 + CatalogHeaderEncoder.BLOCK_LENGTH;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, capacity, clock, null, segmentFileBuffer))
        {
            for (int i = 0; i < 2; i++)
            {
                recordingOneId = catalog.addNewRecording(
                    0L,
                    0L,
                    0,
                    SEGMENT_LENGTH,
                    TERM_LENGTH,
                    MTU_LENGTH,
                    6,
                    1,
                    "channelG",
                    "channelG?tag=f",
                    "sourceA");
            }
        }

        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            assertEquals(2, catalog.entryCount());
            assertEquals(capacity, catalog.capacity());
        }
    }

    @Test
    void shouldGrowCatalogWhenMaxCapacityReached()
    {
        after();
        final File archiveDir = ArchiveTests.makeTestDirectory();

        try (Catalog catalog = new Catalog(archiveDir, null, 0, MIN_CAPACITY, clock, null, segmentFileBuffer))
        {
            for (int i = 0; i < 4; i++)
            {
                recordingOneId = catalog.addNewRecording(
                    0L,
                    0L,
                    0,
                    SEGMENT_LENGTH,
                    TERM_LENGTH,
                    MTU_LENGTH,
                    6,
                    1,
                    "channelG",
                    "channelG?tag=f",
                    "sourceA");
            }
        }

        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            assertEquals(4, catalog.entryCount());
            assertEquals(819, catalog.capacity());
        }
    }

    @Test
    void growCatalogThrowsArchiveExceptionIfCatalogIsFull()
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer))
        {
            final ArchiveException exception = assertThrows(
                ArchiveException.class, () -> catalog.growCatalog(CAPACITY, (int)(CAPACITY + 1)));
            assertEquals("ERROR - catalog is full, max capacity reached: " + CAPACITY, exception.getMessage());
        }
    }

    @Test
    void growCatalogThrowsArchiveExceptionIfRecordingIsTooBig()
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer))
        {
            final ArchiveException exception = assertThrows(
                ArchiveException.class, () -> catalog.growCatalog(CAPACITY * 2, Integer.MAX_VALUE));
            assertEquals(
                "ERROR - recording is too big: total recording length is " + Integer.MAX_VALUE +
                " bytes, available space is " + (CAPACITY * 2 - 800) + " bytes",
                exception.getMessage());
        }
    }

    @Test
    void growCatalogShouldNotExceedMaxCatalogCapacity()
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer))
        {
            final long maxCatalogCapacity = CAPACITY * 1024;
            catalog.growCatalog(maxCatalogCapacity, (int)(maxCatalogCapacity - 10_000));
            assertEquals(maxCatalogCapacity, catalog.capacity());
        }
    }

    @Test
    void shouldNotThrowWhenOldRecordingLogsAreDeleted() throws IOException
    {
        final File segmentFile = new File(archiveDir, segmentFileName(recordingThreeId, SEGMENT_LENGTH * 2));
        try (FileChannel log = FileChannel.open(segmentFile.toPath(), READ, WRITE, CREATE))
        {
            final ByteBuffer bb = allocate(HEADER_LENGTH);
            final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(bb);
            flyweight.frameLength(256);
            log.write(bb);
        }

        final Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer);
        catalog.close();
    }

    @Test
    void shouldContainChannelFragment()
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer))
        {
            final String originalChannel = "aeron:udp?endpoint=localhost:7777|tags=777|alias=TestString";
            final String strippedChannel = "strippedChannelUri";
            final long recordingId = catalog.addNewRecording(
                0L,
                0L,
                0,
                SEGMENT_LENGTH,
                TERM_LENGTH,
                MTU_LENGTH,
                6,
                1,
                strippedChannel,
                originalChannel,
                "sourceA");

            assertTrue(catalog.wrapDescriptor(recordingId, unsafeBuffer));

            recordingDescriptorDecoder.wrap(
                unsafeBuffer,
                RecordingDescriptorHeaderDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.SCHEMA_VERSION);

            assertTrue(originalChannelContains(recordingDescriptorDecoder, ArrayUtil.EMPTY_BYTE_ARRAY));

            final byte[] originalChannelBytes = originalChannel.getBytes(US_ASCII);
            assertTrue(originalChannelContains(recordingDescriptorDecoder, originalChannelBytes));

            final byte[] tagsBytes = "tags=777".getBytes(US_ASCII);
            assertTrue(originalChannelContains(recordingDescriptorDecoder, tagsBytes));

            final byte[] testBytes = "TestString".getBytes(US_ASCII);
            assertTrue(originalChannelContains(recordingDescriptorDecoder, testBytes));

            final byte[] wrongBytes = "wrong".getBytes(US_ASCII);
            assertFalse(originalChannelContains(recordingDescriptorDecoder, wrongBytes));
        }
    }

    @ParameterizedTest(name = "fragmentCrossesPageBoundary({0}, {1}, {2})")
    @MethodSource("pageBoundaryTestData")
    void detectPageBoundaryStraddle(final int fragmentOffset, final int fragmentLength, final boolean expected)
    {
        assertEquals(expected, fragmentStraddlesPageBoundary(fragmentOffset, fragmentLength));
    }

    @ParameterizedTest
    @ValueSource(longs = { -1, 4, Long.MAX_VALUE })
    void findLastReturnsNullRecordingIdIfMinRecordingIdIsOutOfRange(final long minRecordingId)
    {
        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            assertEquals(NULL_RECORD_ID, catalog.findLast(minRecordingId, 6, 1, "channelG?tag=f".getBytes(US_ASCII)));
        }
    }

    @Test
    void findLastReturnsLastFoundRecordingMatchingGivenCriteria()
    {
        final int sessionId = 6;
        final int streamId = 1;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer))
        {
            final long recordingF = catalog.addNewRecording(
                0L,
                0L,
                0,
                SEGMENT_LENGTH,
                TERM_LENGTH,
                MTU_LENGTH,
                sessionId,
                streamId,
                "F",
                "channelG?tag=f",
                "sourceA");
            catalog.addNewRecording(
                0L,
                0L,
                0,
                SEGMENT_LENGTH,
                TERM_LENGTH,
                MTU_LENGTH,
                sessionId,
                streamId,
                "X",
                "channelG?tag=x",
                "sourceA");
            catalog.addNewRecording(
                0L,
                0L,
                0,
                SEGMENT_LENGTH,
                TERM_LENGTH,
                MTU_LENGTH,
                sessionId,
                streamId + 1,
                "F",
                "channelG?tag=f",
                "sourceA");
            catalog.addNewRecording(
                0L,
                0L,
                0,
                SEGMENT_LENGTH,
                TERM_LENGTH,
                MTU_LENGTH,
                sessionId + 1,
                streamId,
                "F",
                "channelG?tag=f",
                "sourceA");

            assertEquals(recordingF,
                catalog.findLast(recordingOneId, sessionId, streamId, "channelG?tag=f".getBytes(US_ASCII)));
        }
    }

    @Test
    void findLastReturnsNullRecordingIdIfRecordingIsInTheInvalidState()
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer))
        {
            assertTrue(catalog.changeState(recordingOneId, INVALID));

            assertEquals(NULL_RECORD_ID, catalog.findLast(0, 6, 1, "channelG?tag=f".getBytes(US_ASCII)));
        }
    }

    @ParameterizedTest
    @ValueSource(longs = { -1, Long.MAX_VALUE })
    void changeStateIsANoOpIfUnknownRecordingStateIdIsSpecified(final long recordingId)
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer))
        {
            assertFalse(catalog.changeState(recordingId, NULL_VAL));
        }
    }

    @Test
    void changeStateOfTheIndexFirstEntry()
    {
        testChangeState(recordingOneId, DELETED);
    }

    @Test
    void changeStateOfTheIndexMiddleEntry()
    {
        testChangeState(recordingTwoId, NULL_VAL);
    }

    @Test
    void changeStateOfTheIndexLastEntry()
    {
        testChangeState(recordingThreeId, INVALID);
    }

    @Test
    void shouldComputeChecksumOfTheRecordingDescriptorUponAddingToTheCatalog()
    {
        final Checksum checksum = crc32();
        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, checksum, segmentFileBuffer))
        {
            final long recordingId = catalog.addNewRecording(
                0L, 0L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1, "channelNew", "channelNew?tag=X", "sourceX");
            final long recordingId2 = catalog.addNewRecording(
                1,
                100,
                2,
                222,
                111,
                SEGMENT_LENGTH,
                TERM_LENGTH,
                MTU_LENGTH,
                16,
                12,
                "channelNew2",
                "channelNew?tag=X2",
                "sourceX2");

            catalog.forEach(
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                {
                    if (recordingId == descriptorDecoder.recordingId())
                    {
                        assertEquals(1691549102, headerDecoder.checksum());
                    }
                    else if (recordingId2 == descriptorDecoder.recordingId())
                    {
                        assertEquals(1452384985, headerDecoder.checksum());
                    }
                    else
                    {
                        assertEquals(0, headerDecoder.checksum());
                    }
                });
        }
    }

    @Test
    void recordingStoppedShouldUpdateChecksum()
    {
        final Checksum checksum = crc32();
        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, checksum, segmentFileBuffer))
        {
            assertChecksum(catalog, recordingOneId, 160, 0, null);

            catalog.recordingStopped(recordingOneId, 140, 231723682323L);

            assertChecksum(catalog, recordingOneId, 160, 1656993099, checksum);
        }
    }

    @Test
    void stopPositionShouldUpdateChecksum()
    {
        final Checksum checksum = crc32();
        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, checksum, segmentFileBuffer))
        {
            assertChecksum(catalog, recordingTwoId, 160, 0, null);

            catalog.stopPosition(recordingTwoId, 7777);

            assertChecksum(catalog, recordingTwoId, 160, -1985007076, checksum);
        }
    }

    @Test
    void startPositionShouldUpdateChecksum()
    {
        final Checksum checksum = crc32();
        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, checksum, segmentFileBuffer))
        {
            assertChecksum(catalog, recordingThreeId, 352, 0, null);

            catalog.startPosition(recordingThreeId, 123);

            assertChecksum(catalog, recordingThreeId, 352, -160510802, checksum);
        }
    }

    @Test
    void extendRecordingShouldUpdateChecksum()
    {
        final Checksum checksum = crc32();
        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, checksum, segmentFileBuffer))
        {
            final long recordingId = catalog.addNewRecording(
                0L, 0L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1, "channelNew", "channelNew?tag=X", "sourceX");
            assertChecksum(catalog, recordingId, 160, 1691549102, checksum);

            catalog.extendRecording(recordingId, 555, 13, 31);

            assertChecksum(catalog, recordingId, 160, -1694749833, checksum);
        }
    }

    @Test
    void replaceThrowsArchiveExceptionIfRecordingIdIsUnknown()
    {
        final Checksum checksum = crc32();
        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, checksum, segmentFileBuffer))
        {
            catalog.startPosition(recordingOneId, 0);
            catalog.startPosition(recordingTwoId, 0);
            catalog.startPosition(recordingThreeId, 0);
            assertChecksum(catalog, recordingOneId, 160, -866186973, checksum);
            assertChecksum(catalog, recordingTwoId, 160, -1947831311, checksum);
            assertChecksum(catalog, recordingThreeId, 352, -529628341, checksum);
        }

        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, checksum, segmentFileBuffer))
        {
            final int unknownRecordingId = 1_000_000;
            final ArchiveException exception = assertThrowsExactly(
                ArchiveException.class, () -> catalog.replaceRecording(
                unknownRecordingId,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                "11",
                "12",
                "13"));
            assertEquals("ERROR - unknown recording id: " + unknownRecordingId, exception.getMessage());

            assertEquals(1024, catalog.capacity());
            assertEquals(3, catalog.index().size());
            assertEquals(recordingThreeId + 1, catalog.nextRecordingId());
            assertChecksum(catalog, recordingOneId, 160, -866186973, checksum);
            assertChecksum(catalog, recordingTwoId, 160, -1947831311, checksum);
            assertChecksum(catalog, recordingThreeId, 352, -529628341, checksum);
        }
    }

    @Test
    void replaceRecordingUpdateCatalogFileWhenNewMetadataDoesNotFit()
    {
        final Checksum checksum = crc32();
        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, checksum, segmentFileBuffer))
        {
            catalog.startPosition(recordingOneId, 0);
            catalog.startPosition(recordingTwoId, 0);
            catalog.startPosition(recordingThreeId, 0);
            assertChecksum(catalog, recordingOneId, 160, -866186973, checksum);
            assertChecksum(catalog, recordingTwoId, 160, -1947831311, checksum);
            assertChecksum(catalog, recordingThreeId, 352, -529628341, checksum);
        }

        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, checksum, segmentFileBuffer))
        {
            final long oldCapacity = catalog.capacity();

            final String newSourceIdentity = addSuffix(
                "and the source identity changes as well and is also quite a long one funny thing: ", "!", 2000);
            catalog.replaceRecording(
                recordingTwoId,
                1024,
                16 * 1024,
                3252535612L,
                6238423648L,
                777,
                SEGMENT_LENGTH * 4,
                TERM_LENGTH * 2,
                1344,
                -19,
                42,
                "suppose to be a short description of the channel but can be whatever and surprising. Veni, vidi, vici",
                "aeron:ipc?tag=15|alias=that is very very very long and will overflow the originally assigned length " +
                "for sure and then some",
                newSourceIdentity);

            assertTrue(catalog.capacity() > oldCapacity);
            assertEquals(recordingThreeId + 1, catalog.nextRecordingId());
            verifyRecordingForId(
                catalog,
                recordingOneId,
                160,
                0L,
                0L,
                0L,
                1L,
                0,
                SEGMENT_LENGTH,
                TERM_LENGTH,
                MTU_LENGTH,
                6,
                1,
                "channelG",
                "channelG?tag=f",
                "sourceA");
            assertChecksum(catalog, recordingOneId, 160, -866186973, checksum);

            verifyRecordingForId(
                catalog,
                recordingTwoId,
                2400,
                1024,
                16 * 1024,
                3252535612L,
                6238423648L,
                777,
                SEGMENT_LENGTH * 4,
                TERM_LENGTH * 2,
                1344,
                -19,
                42,
                "suppose to be a short description of the channel but can be whatever and surprising. Veni, vidi, vici",
                "aeron:ipc?tag=15|alias=that is very very very long and will overflow the originally assigned length " +
                "for sure and then some",
                newSourceIdentity);
            assertChecksum(catalog, recordingTwoId, 2400, -2076878182, checksum);

            verifyRecordingForId(
                catalog,
                recordingThreeId,
                352,
                0L,
                0L,
                0L,
                1L,
                0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 8,
                3,
                "channelThatIsVeryLongAndShouldNotBeTruncated",
                "channelThatIsVeryLongAndShouldNotBeTruncated?tag=f",
                "source can also be a very very very long String and it will not be truncated even if gets " +
                "very very long");
            assertChecksum(catalog, recordingThreeId, 352, -529628341, checksum);
        }
    }

    @Test
    void replaceRecordingUpdateCatalogFileWhenNewMetadataDoesNotFitAndDifferenceInSizeIsSmall()
    {
        final Checksum checksum = crc32();
        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, checksum, segmentFileBuffer))
        {
            catalog.startPosition(recordingOneId, 0);
            catalog.startPosition(recordingTwoId, 0);
            catalog.startPosition(recordingThreeId, 0);
            assertChecksum(catalog, recordingOneId, 160, -866186973, checksum);
            assertChecksum(catalog, recordingTwoId, 160, -1947831311, checksum);
            assertChecksum(catalog, recordingThreeId, 352, -529628341, checksum);
        }

        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, checksum, segmentFileBuffer))
        {
            catalog.replaceRecording(
                recordingTwoId,
                1024,
                16 * 1024,
                3252535612L,
                6238423648L,
                777,
                SEGMENT_LENGTH * 4,
                TERM_LENGTH * 2,
                1344,
                -19,
                42,
                "channelH",
                "channelH?tag=f",
                "to source or not to source that is the question");

            assertEquals(recordingThreeId + 1, catalog.nextRecordingId());
            verifyRecordingForId(
                catalog,
                recordingOneId,
                160,
                0L,
                0L,
                0L,
                1L,
                0,
                SEGMENT_LENGTH,
                TERM_LENGTH,
                MTU_LENGTH,
                6,
                1,
                "channelG",
                "channelG?tag=f",
                "sourceA");
            assertChecksum(catalog, recordingOneId, 160, -866186973, checksum);

            verifyRecordingForId(
                catalog,
                recordingTwoId,
                224,
                1024,
                16 * 1024,
                3252535612L,
                6238423648L,
                777,
                SEGMENT_LENGTH * 4,
                TERM_LENGTH * 2,
                1344,
                -19,
                42,
                "channelH",
                "channelH?tag=f",
                "to source or not to source that is the question");
            assertChecksum(catalog, recordingTwoId, 224, -1059517110, checksum);

            verifyRecordingForId(
                catalog,
                recordingThreeId,
                352,
                0L,
                0L,
                0L,
                1L,
                0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 8,
                3,
                "channelThatIsVeryLongAndShouldNotBeTruncated",
                "channelThatIsVeryLongAndShouldNotBeTruncated?tag=f",
                "source can also be a very very very long String and it will not be truncated even if gets " +
                "very very long");
            assertChecksum(catalog, recordingThreeId, 352, -529628341, checksum);
        }
    }

    @Test
    void replaceRecordingShouldUpdateMetadataInPlaceWhenShorterThanTheOldOne()
    {
        final long updateTime = 555555L;
        final Checksum checksum = crc32();
        try (Catalog catalog =
            new Catalog(archiveDir, null, 0, CAPACITY, () -> updateTime, checksum, segmentFileBuffer))
        {
            catalog.startPosition(recordingOneId, 0);
            catalog.startPosition(recordingTwoId, 0);
            catalog.startPosition(recordingThreeId, 0);
            assertChecksum(catalog, recordingOneId, 160, 729050496, checksum);
            assertChecksum(catalog, recordingTwoId, 160, 1825380178, checksum);
            assertChecksum(catalog, recordingThreeId, 352, -1553583482, checksum);
        }

        try (Catalog catalog =
            new Catalog(archiveDir, null, 0, CAPACITY, clock, checksum, segmentFileBuffer))
        {
            final long originalCapacity = catalog.capacity();

            catalog.replaceRecording(
                recordingOneId,
                128,
                512,
                111L,
                222L,
                -19091,
                SEGMENT_LENGTH * 16,
                TERM_LENGTH * 4,
                1372,
                21,
                8,
                "A",
                "B",
                "C");

            assertEquals(recordingThreeId + 1, catalog.nextRecordingId());
            assertEquals(originalCapacity, catalog.capacity());
            final int oldFrameLength = 160;
            verifyRecordingForId(
                catalog,
                recordingOneId,
                oldFrameLength,
                128,
                512,
                111L,
                222L,
                -19091,
                SEGMENT_LENGTH * 16,
                TERM_LENGTH * 4,
                1372,
                21,
                8,
                "A",
                "B",
                "C");
            final int newFrameLength = 96;
            verifyOldExcessiveDataWasErased(oldFrameLength, newFrameLength);
            assertChecksum(catalog, recordingOneId, newFrameLength, 1488471744, checksum);

            verifyRecordingForId(
                catalog,
                recordingTwoId,
                160,
                0L,
                0L,
                0L,
                updateTime,
                0,
                SEGMENT_LENGTH,
                TERM_LENGTH,
                MTU_LENGTH,
                7,
                2,
                "channelH",
                "channelH?tag=f",
                "sourceV");
            assertChecksum(catalog, recordingTwoId, 160, 1825380178, checksum);

            verifyRecordingForId(
                catalog,
                recordingThreeId,
                352,
                0L,
                0L,
                0L,
                updateTime,
                0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 8,
                3,
                "channelThatIsVeryLongAndShouldNotBeTruncated",
                "channelThatIsVeryLongAndShouldNotBeTruncated?tag=f",
                "source can also be a very very very long String and it will not be truncated even if gets " +
                "very very long");
            assertChecksum(catalog, recordingThreeId, 352, -1553583482, checksum);
        }
    }

    private void verifyOldExcessiveDataWasErased(final int oldFrameLength, final int newFrameLength)
    {
        assertTrue(oldFrameLength > newFrameLength);
        for (int i = newFrameLength; i < oldFrameLength; i++)
        {
            assertEquals(0, unsafeBuffer.getByte(RecordingDescriptorHeaderDecoder.BLOCK_LENGTH + i));
        }
    }

    private static void assertChecksum(
        final Catalog catalog,
        final long recordingId,
        final int alignedChecksumLength,
        final int expectedChecksum,
        final Checksum checksum)
    {
        catalog.forEntry(recordingId,
            (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
            {
                assertEquals(expectedChecksum, headerDecoder.checksum());
                if (null != checksum)
                {
                    final int computedChecksum = checksum.compute(
                        descriptorDecoder.buffer().addressOffset(),
                        RecordingDescriptorHeaderDecoder.BLOCK_LENGTH,
                        alignedChecksumLength);
                    assertEquals(expectedChecksum, computedChecksum);
                }
            });
    }

    private void testChangeState(final long recordingId, final RecordingState newState)
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, segmentFileBuffer))
        {
            final int entries = catalog.entryCount();

            assertTrue(catalog.wrapDescriptor(recordingId, unsafeBuffer));

            recordingDescriptorHeaderDecoder.wrap(
                unsafeBuffer,
                0,
                RecordingDescriptorHeaderDecoder.BLOCK_LENGTH,
                RecordingDescriptorHeaderDecoder.SCHEMA_VERSION);

            assertTrue(catalog.changeState(recordingId, newState));

            assertEquals(newState, recordingDescriptorHeaderDecoder.state());
            assertEquals(entries - 1, catalog.entryCount());
            assertFalse(catalog.hasRecording(recordingId));
        }
    }

    private void verifyRecordingForId(
        final Catalog catalog,
        final long id,
        final int length,
        final long startPosition,
        final long stopPosition,
        final long startTimestamp,
        final long stopTimestamp,
        final int initialTermId,
        final int segmentFileLength,
        final int termBufferLength,
        final int mtuLength,
        final int sessionId,
        final int streamId,
        final String strippedChannel,
        final String originalChannel,
        final String sourceIdentity)
    {
        assertTrue(catalog.wrapDescriptor(id, unsafeBuffer));

        recordingDescriptorHeaderDecoder.wrap(
            unsafeBuffer,
            0,
            RecordingDescriptorHeaderDecoder.BLOCK_LENGTH,
            RecordingDescriptorHeaderDecoder.SCHEMA_VERSION);

        assertEquals(VALID, recordingDescriptorHeaderDecoder.state());
        assertEquals(length, recordingDescriptorHeaderDecoder.length());

        recordingDescriptorDecoder.wrap(
            unsafeBuffer,
            RecordingDescriptorHeaderDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);

        assertEquals(id, recordingDescriptorDecoder.recordingId());
        assertEquals(startPosition, recordingDescriptorDecoder.startPosition());
        assertEquals(stopPosition, recordingDescriptorDecoder.stopPosition());
        assertEquals(startTimestamp, recordingDescriptorDecoder.startTimestamp());
        assertEquals(stopTimestamp, recordingDescriptorDecoder.stopTimestamp());
        assertEquals(initialTermId, recordingDescriptorDecoder.initialTermId());
        assertEquals(segmentFileLength, recordingDescriptorDecoder.segmentFileLength());
        assertEquals(termBufferLength, recordingDescriptorDecoder.termBufferLength());
        assertEquals(mtuLength, recordingDescriptorDecoder.mtuLength());
        assertEquals(sessionId, recordingDescriptorDecoder.sessionId());
        assertEquals(streamId, recordingDescriptorDecoder.streamId());
        assertEquals(strippedChannel, recordingDescriptorDecoder.strippedChannel());
        assertEquals(originalChannel, recordingDescriptorDecoder.originalChannel());
        assertEquals(sourceIdentity, recordingDescriptorDecoder.sourceIdentity());
    }

    private static Stream<Arguments> pageBoundaryTestData()
    {
        return Stream.of(
            Arguments.of(0, 64, false),
            Arguments.of(100, 300, false),
            Arguments.of(0, PAGE_SIZE, false),
            Arguments.of(1, PAGE_SIZE - 1, false),
            Arguments.of(PAGE_SIZE - 1, 1, false),
            Arguments.of(PAGE_SIZE * 7 + 32, 256, false),
            Arguments.of(PAGE_SIZE * 3 + 111, PAGE_SIZE - 111, false),
            Arguments.of(0, PAGE_SIZE + 1, true),
            Arguments.of(PAGE_SIZE - 1, 2, true),
            Arguments.of(PAGE_SIZE * 4 + 11, PAGE_SIZE - 10, true));
    }

    private void setNextRecordingId(final long nextRecordingId) throws IOException
    {
        try (FileChannel channel = FileChannel.open(archiveDir.toPath().resolve(CATALOG_FILE_NAME), READ, WRITE))
        {
            final MappedByteBuffer mappedByteBuffer = channel.map(READ_WRITE, 0, CatalogHeaderEncoder.BLOCK_LENGTH);
            mappedByteBuffer.order(CatalogHeaderEncoder.BYTE_ORDER);
            try
            {
                new CatalogHeaderEncoder()
                    .wrap(new UnsafeBuffer(mappedByteBuffer), 0)
                    .nextRecordingId(nextRecordingId);
            }
            finally
            {
                BufferUtil.free(mappedByteBuffer);
            }
        }
    }

    private static String addSuffix(final String prefix, final String suffix, final int times)
    {
        final StringBuilder buff = new StringBuilder(prefix.length() + suffix.length() * times);
        buff.append(prefix);

        for (int i = 0; i < times; i++)
        {
            buff.append(suffix);
        }

        return buff.toString();
    }
}
