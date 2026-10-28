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
import io.aeron.archive.codecs.RecordingState;
import io.aeron.exceptions.AeronException;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.IoUtil;
import org.agrona.collections.MutableBoolean;
import org.agrona.concurrent.EpochClock;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.aeron.archive.Archive.Configuration.CATALOG_FILE_NAME;
import static io.aeron.archive.Archive.Configuration.RECORDING_SEGMENT_SUFFIX;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.ArchiveTool.*;
import static io.aeron.archive.ArchiveTool.VerifyOption.APPLY_CHECKSUM;
import static io.aeron.archive.ArchiveTool.VerifyOption.VERIFY_ALL_SEGMENT_FILES;
import static io.aeron.archive.Catalog.*;
import static io.aeron.archive.checksum.Checksums.crc32;
import static io.aeron.archive.client.AeronArchive.*;
import static io.aeron.archive.codecs.RecordingState.*;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.LogBufferDescriptor.computeTermIdFromPosition;
import static io.aeron.logbuffer.LogBufferDescriptor.positionBitsToShift;
import static io.aeron.protocol.DataHeaderFlyweight.*;
import static java.nio.ByteBuffer.allocate;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Collections.emptySet;
import static java.util.EnumSet.allOf;
import static java.util.EnumSet.of;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

class ArchiveToolTests
{
    private static final int MTU_LENGTH = PAGE_SIZE * 4;
    private static final int TERM_LENGTH = MTU_LENGTH * 8;
    private static final int SEGMENT_LENGTH = TERM_LENGTH * 4;
    private static Path validationDir;

    private long invalidRecording0;
    private long invalidRecording1;
    private long invalidRecording2;
    private long invalidRecording3;
    private long invalidRecording4;
    private long invalidRecording5;
    private long invalidRecording6;
    private long invalidRecording7;
    private long invalidRecording8;
    private long invalidRecording9;
    private long invalidRecording10;
    private long invalidRecording11;
    private long invalidRecording12;
    private long invalidRecording13;
    private long invalidRecording14;
    private long validRecording0;
    private long validRecording1;
    private long validRecording2;
    private long validRecording3;
    private long validRecording4;
    private long validRecording51;
    private long validRecording52;
    private long validRecording53;
    private long validRecording6;

    private long currentTimeMillis = 0;
    private final EpochClock epochClock = () -> currentTimeMillis += 100;
    private final PrintStream out = Mockito.mock(PrintStream.class);
    private File archiveDir;

    @BeforeAll
    static void beforeAll() throws IOException
    {
        validationDir = createTempDirectory("validation-tests");
    }

    @AfterAll
    static void afterAll() throws IOException
    {
        deleteIfExists(validationDir);
    }

    @SuppressWarnings("MethodLength")
    @BeforeEach
    void before() throws IOException
    {
        archiveDir = ArchiveTests.makeTestDirectory();

        try (Catalog catalog = new Catalog(archiveDir, null, 0, 1024, epochClock, null, null))
        {
            invalidRecording0 = catalog.addNewRecording(NULL_POSITION, NULL_POSITION, 1, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording1 = catalog.addNewRecording(FRAME_ALIGNMENT - 7, NULL_POSITION, 2, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording2 = catalog.addNewRecording(1024, FRAME_ALIGNMENT * 2, 3, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording3 = catalog.addNewRecording(0, FRAME_ALIGNMENT * 5 + 11, 4, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording4 = catalog.addNewRecording(SEGMENT_LENGTH, NULL_POSITION, 5, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording5 = catalog.addNewRecording(0, SEGMENT_LENGTH, 6, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording6 = catalog.addNewRecording(0, NULL_POSITION, 7, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording7 = catalog.addNewRecording(0, NULL_POSITION, 8, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording8 = catalog.addNewRecording(0, NULL_POSITION, 9, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording9 = catalog.addNewRecording(0, NULL_POSITION, 10, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording10 = catalog.addNewRecording(128, NULL_POSITION, 11, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording11 = catalog.addNewRecording(0, NULL_POSITION, 12, NULL_TIMESTAMP, 5,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording12 = catalog.addNewRecording(0, NULL_POSITION, 13, NULL_TIMESTAMP, 9,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 6, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording13 = catalog.addNewRecording(0, NULL_POSITION, 14, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 13, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording14 = catalog.addNewRecording(128, NULL_POSITION, -14, 41, -14,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 0, "ch1", "ch1?tag=ERR", "src1");
            validRecording0 = catalog.addNewRecording(0, NULL_POSITION, 15, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "ch2", "ch2?tag=OK", "src2");
            validRecording1 = catalog.addNewRecording(1024, NULL_POSITION, 16, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "ch2", "ch2?tag=OK", "src2");
            validRecording2 = catalog.addNewRecording(3 * TERM_LENGTH + 96, NULL_POSITION, 17, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "ch2", "ch2?tag=OK", "src2");
            validRecording3 = catalog.addNewRecording(
                7 * TERM_LENGTH + 96, 7 * TERM_LENGTH + 128, 18, NULL_TIMESTAMP, 7,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 999, 13, "ch2", "ch2?tag=OK", "src2");
            validRecording4 = catalog.addNewRecording(
                21 * TERM_LENGTH + (TERM_LENGTH - 64), 22 * TERM_LENGTH + 992, 19, 1, -25,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, -999, 7, "ch2", "ch2?tag=OK", "src2");
            validRecording51 = catalog.addNewRecording(0, 64 + PAGE_SIZE, 20, 777, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, -1, 20, "ch2", "ch2?tag=OK", "src2");
            validRecording52 = catalog.addNewRecording(0, NULL_POSITION, 21, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 52, 52, "ch2", "ch2?tag=OK", "src2");
            validRecording53 = catalog.addNewRecording(0, NULL_POSITION, 22, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 53, 53, "ch2", "ch2?tag=OK", "src2");
            validRecording6 = catalog.addNewRecording(352, NULL_POSITION, 23, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 10, 6, "ch2", "ch2?tag=OK", "src2");
        }

        writeToSegmentFile(
            createFile(segmentFileName(invalidRecording4, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) -> fileChannel.write(byteBuffer));

        writeToSegmentFile(
            createFile(segmentFileName(invalidRecording5, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) -> fileChannel.write(byteBuffer));

        createFile(invalidRecording6 + "-" + RECORDING_SEGMENT_SUFFIX);

        createFile(invalidRecording7 + "-this-will-not-parse" + RECORDING_SEGMENT_SUFFIX);

        createFile(segmentFileName(invalidRecording8, -1024));

        createDirectory(segmentFileName(invalidRecording9, 0));

        writeToSegmentFile(
            createFile(segmentFileName(invalidRecording10, 0)),
            150,
            (byteBuffer, dataHeaderFlyweight, fileChannel) -> fileChannel.write(byteBuffer));

        writeToSegmentFile(
            createFile(segmentFileName(invalidRecording11, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.streamId(1);
                dataHeaderFlyweight.termOffset(0);
                dataHeaderFlyweight.termId(5);
                fileChannel.write(byteBuffer);

                byteBuffer.clear();
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(40);
                dataHeaderFlyweight.termId(-1);
                fileChannel.write(byteBuffer, 64);
            });

        writeToSegmentFile(
            createFile(segmentFileName(invalidRecording12, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.streamId(6);
                dataHeaderFlyweight.termOffset(0);
                dataHeaderFlyweight.termId(9);
                fileChannel.write(byteBuffer);

                byteBuffer.clear();
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(50);
                dataHeaderFlyweight.termOffset(-1);
                fileChannel.write(byteBuffer, 64);
            });

        writeToSegmentFile(
            createFile(segmentFileName(invalidRecording13, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.streamId(13);
                dataHeaderFlyweight.termOffset(0);
                dataHeaderFlyweight.termId(0);
                fileChannel.write(byteBuffer);

                byteBuffer.clear();
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(60);
                dataHeaderFlyweight.streamId(-1);
                fileChannel.write(byteBuffer, 64);
            });

        writeToSegmentFile(
            createFile(segmentFileName(invalidRecording14, 0)),
            228,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(100);
                dataHeaderFlyweight.streamId(0);
                dataHeaderFlyweight.termOffset(128);
                dataHeaderFlyweight.termId(-14);
                fileChannel.write(byteBuffer, 128);
            });

        writeToSegmentFile(
            createFile(segmentFileName(validRecording0, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                for (int i = 0, termOffset = 0; i < TERM_LENGTH / MTU_LENGTH - 1; i++, termOffset += MTU_LENGTH)
                {
                    byteBuffer.clear().limit(MTU_LENGTH);
                    dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                    dataHeaderFlyweight.streamId(2);
                    dataHeaderFlyweight.frameLength(MTU_LENGTH);
                    dataHeaderFlyweight.sessionId(876814387);
                    dataHeaderFlyweight.termOffset(termOffset);
                    fileChannel.write(byteBuffer, termOffset);
                }

                byteBuffer.clear().limit(256);
                dataHeaderFlyweight.frameLength(256);
                dataHeaderFlyweight.termOffset(TERM_LENGTH - MTU_LENGTH);
                dataHeaderFlyweight.sessionId(1025596259);
                fileChannel.write(byteBuffer, TERM_LENGTH - MTU_LENGTH);

                byteBuffer.clear().limit(MTU_LENGTH - 256);
                dataHeaderFlyweight.headerType(HDR_TYPE_PAD);
                dataHeaderFlyweight.sessionId(-1); // should not be checked
                dataHeaderFlyweight.frameLength(MTU_LENGTH - 256);
                dataHeaderFlyweight.termOffset(TERM_LENGTH - MTU_LENGTH + 256);
                fileChannel.write(byteBuffer, TERM_LENGTH - MTU_LENGTH + 256);

                byteBuffer.clear().limit(64);
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.termOffset(0);
                dataHeaderFlyweight.termId(1);
                dataHeaderFlyweight.sessionId(420107693);
                fileChannel.write(byteBuffer, TERM_LENGTH);
            });

        writeToSegmentFile(
            createFile(segmentFileName(validRecording2, 3 * TERM_LENGTH)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(0);
                fileChannel.write(byteBuffer, 96);
            });

        writeToSegmentFile(
            createFile(segmentFileName(validRecording3, 7 * TERM_LENGTH)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                int fileOffset = 96;
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(128);
                dataHeaderFlyweight.streamId(13);
                dataHeaderFlyweight.termId(14);
                dataHeaderFlyweight.termOffset(96);
                dataHeaderFlyweight.setMemory(HEADER_LENGTH, 128 - HEADER_LENGTH, (byte)13);
                fileChannel.write(byteBuffer, fileOffset);
                fileOffset += 128;

                final int segmentFileBasePosition = 7 * TERM_LENGTH;
                final int positionBitsToShift = positionBitsToShift(TERM_LENGTH);
                for (int i = 0; i < (SEGMENT_LENGTH / MTU_LENGTH) - 1; i++)
                {
                    byteBuffer.clear();
                    final int termId = computeTermIdFromPosition(
                        segmentFileBasePosition + fileOffset, positionBitsToShift, 7);
                    dataHeaderFlyweight.frameLength(MTU_LENGTH);
                    dataHeaderFlyweight.termId(termId);
                    dataHeaderFlyweight.termOffset(fileOffset & (TERM_LENGTH - 1));
                    dataHeaderFlyweight.setMemory(HEADER_LENGTH, MTU_LENGTH - HEADER_LENGTH, (byte)i);
                    fileChannel.write(byteBuffer, fileOffset);
                    fileOffset += MTU_LENGTH;
                }

                final int lastFrameLength = MTU_LENGTH - 224;
                assertEquals(SEGMENT_LENGTH - lastFrameLength, fileOffset);
                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(lastFrameLength);
                dataHeaderFlyweight.termOffset(fileOffset & (TERM_LENGTH - 1));
                for (int i = 0; i < lastFrameLength - HEADER_LENGTH; i++)
                {
                    dataHeaderFlyweight.setMemory(HEADER_LENGTH + i, 1, (byte)i);
                }
                dataHeaderFlyweight.setMemory(HEADER_LENGTH, 1, (byte)-128);
                dataHeaderFlyweight.setMemory(lastFrameLength - 1, 1, (byte)127);
                fileChannel.write(byteBuffer, fileOffset);
            });

        writeToSegmentFile(
            createFile(segmentFileName(validRecording3, 11 * TERM_LENGTH)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(180);
                dataHeaderFlyweight.streamId(13);
                dataHeaderFlyweight.termId(18);
                dataHeaderFlyweight.termOffset(0);
                fileChannel.write(byteBuffer);

                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(128);
                dataHeaderFlyweight.termId(18);
                dataHeaderFlyweight.termOffset(192);
                fileChannel.write(byteBuffer, 192);
            });

        writeToSegmentFile(
            createFile(segmentFileName(validRecording4, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.streamId(Integer.MAX_VALUE);
                fileChannel.write(byteBuffer);
            });

        writeToSegmentFile(
            createFile(segmentFileName(validRecording4, 21 * TERM_LENGTH)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_PAD);
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.streamId(7);
                dataHeaderFlyweight.termId(-4);
                dataHeaderFlyweight.termOffset(TERM_LENGTH - 64);
                fileChannel.write(byteBuffer, TERM_LENGTH - 64);

                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(992);
                dataHeaderFlyweight.termId(-3);
                dataHeaderFlyweight.termOffset(0);
                fileChannel.write(byteBuffer, TERM_LENGTH);
            });

        // Page straddle: valid checksum
        writeToSegmentFile(
            createFile(segmentFileName(validRecording51, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.streamId(20);
                dataHeaderFlyweight.sessionId(420107693);
                fileChannel.write(byteBuffer);

                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(PAGE_SIZE);
                dataHeaderFlyweight.termOffset(64);
                dataHeaderFlyweight.sessionId(2057703623);
                fileChannel.write(byteBuffer, dataHeaderFlyweight.termOffset());
            });

        // Page straddle: non-zero bytes in every page since the straddle
        writeToSegmentFile(
            createFile(segmentFileName(validRecording52, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(111);
                dataHeaderFlyweight.streamId(52);
                fileChannel.write(byteBuffer);

                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(MTU_LENGTH);
                dataHeaderFlyweight.termOffset(128);
                dataHeaderFlyweight.putBytes(PAGE_SIZE + 1, new byte[]{ 0, 0, -1, 0, 7 });
                dataHeaderFlyweight.putByte(2 * PAGE_SIZE + 512, (byte)127);
                dataHeaderFlyweight.putByte(MTU_LENGTH - 128 - 1, (byte)1);
                dataHeaderFlyweight.putByte(MTU_LENGTH - 1, (byte)-128);
                fileChannel.write(byteBuffer, dataHeaderFlyweight.termOffset());
            });

        // Page straddle: all zeroes in one of the pages after the straddle
        writeToSegmentFile(
            createFile(segmentFileName(validRecording53, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(60);
                dataHeaderFlyweight.streamId(53);
                fileChannel.write(byteBuffer);

                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(3 * PAGE_SIZE);
                dataHeaderFlyweight.termOffset(64);
                dataHeaderFlyweight.setMemory(PAGE_SIZE - 64, PAGE_SIZE, (byte)111);
                dataHeaderFlyweight.setMemory(3 * PAGE_SIZE - 64, 64, (byte)-128);
                fileChannel.write(byteBuffer, dataHeaderFlyweight.termOffset());
            });

        writeToSegmentFile(
            createFile(segmentFileName(validRecording6, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.streamId(6);
                dataHeaderFlyweight.termOffset(352);
                dataHeaderFlyweight.sessionId(-1960800604); // CRC32
                dataHeaderFlyweight.setMemory(HEADER_LENGTH, 20, (byte)1);
                fileChannel.write(byteBuffer, 352);

                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(544);
                dataHeaderFlyweight.termOffset(416);
                dataHeaderFlyweight.sessionId(-327206874); // CRC32
                dataHeaderFlyweight.setMemory(HEADER_LENGTH, 256, (byte)11);
                dataHeaderFlyweight.setMemory(HEADER_LENGTH + 256, 256, (byte)-20);
                fileChannel.write(byteBuffer, 416);
            });
    }

    @AfterEach
    void after()
    {
        IoUtil.delete(archiveDir, false);
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfStartPositionIsNegative()
    {
        assertFalse(verifyRecording(
            out, archiveDir, invalidRecording0, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording0, INVALID, 0, NULL_POSITION, NULL_POSITION, 1, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfStartPositionIsNotFrameAligned()
    {
        assertFalse(verifyRecording(
            out, archiveDir, invalidRecording1, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording1, INVALID, 0, FRAME_ALIGNMENT - 7, NULL_POSITION, 2,
                NULL_TIMESTAMP, 0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfStopPositionIsBeforeStartPosition()
    {
        assertFalse(verifyRecording(
            out, archiveDir, invalidRecording2, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording2, INVALID, 0, 1024, FRAME_ALIGNMENT * 2, 3, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfStopPositionIsNotFrameAligned()
    {
        assertFalse(verifyRecording(
            out, archiveDir, invalidRecording3, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording3, INVALID, 0, 0, FRAME_ALIGNMENT * 5 + 11, 4, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfStartPositionIsOutOfRangeForTheMaxSegmentFile()
    {
        assertFalse(verifyRecording(
            out, archiveDir, invalidRecording4, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording4, INVALID, 0, SEGMENT_LENGTH, NULL_POSITION, 5, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfStopPositionIsOutOfRangeForTheMaxSegmentFile()
    {
        assertFalse(verifyRecording(
            out, archiveDir, invalidRecording5, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording5, INVALID, 0, 0, SEGMENT_LENGTH, 6, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfSegmentFileNameContainsNoPositionInformation()
    {
        assertFalse(verifyRecording(
            out, archiveDir, invalidRecording6, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording6, INVALID, 0, 0, NULL_POSITION, 7, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfSegmentFileNameContainsNonIntegerPositionInformation()
    {
        assertFalse(verifyRecording(
            out, archiveDir, invalidRecording7, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording7, INVALID, 0, 0, NULL_POSITION, 8, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfSegmentFileNameContainsNegativePositionInformation()
    {
        assertFalse(verifyRecording(
            out, archiveDir, invalidRecording8, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording8, INVALID, 0, 0, NULL_POSITION, 9, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfCannotReadFromSegmentFile()
    {
        assertFalse(verifyRecording(
            out, archiveDir, invalidRecording9, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording9, INVALID, 0, 0, NULL_POSITION, 10, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfCannotReadFrameFromSegmentFileAtGivenOffset()
    {
        assertFalse(verifyRecording(
            out, archiveDir, invalidRecording10, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording10, INVALID, 0, 128, NULL_POSITION, 11, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfSegmentFileContainsAFrameWithWrongTermId()
    {
        assertFalse(verifyRecording(
            out, archiveDir, invalidRecording11, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording11, INVALID, 0, 0, NULL_POSITION, 12, NULL_TIMESTAMP,
                5, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfSegmentFileContainsAFrameWithWrongTermOffset()
    {
        assertFalse(verifyRecording(
            out, archiveDir, invalidRecording12, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording12, INVALID, 0, 0, NULL_POSITION, 13, NULL_TIMESTAMP,
                9, 6, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfSegmentFileContainsAFrameWithWrongStreamId()
    {
        assertFalse(verifyRecording(
            out, archiveDir, invalidRecording13, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording13, INVALID, 0, 0, NULL_POSITION, 14, NULL_TIMESTAMP,
                0, 13, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfSegmentFileContainsIncompleteFrame()
    {
        assertFalse(verifyRecording(
            out, archiveDir, invalidRecording14, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording14, INVALID, 0, 128, NULL_POSITION,
                -14, 41, -14, 0, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingValidRecordingShouldComputeStopPositionFromZeroStartPosition()
    {
        assertTrue(verifyRecording(
            out, archiveDir, validRecording0, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording0, VALID, 0, 0, TERM_LENGTH + 64, 15, 100, 0, 2, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingValidRecordingShouldUseStartPositionIfNoSegmentFilesExist()
    {
        assertTrue(verifyRecording(
            out, archiveDir, validRecording1, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording1, VALID, 0, 1024, 1024, 16, 100, 0, 2, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingValidRecordingShouldUseStartPositionWhenNoDataInTheMaxSegmentFileAtAGivenOffset()
    {
        assertTrue(verifyRecording(
            out, archiveDir, validRecording2, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording2, VALID, 0, TERM_LENGTH * 3 + 96, TERM_LENGTH * 3 + 96, 17, 100,
                0, 2, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingValidRecordingShouldComputeStopPositionWhenStartingAtALaterSegment()
    {
        assertTrue(verifyRecording(
            out, archiveDir, validRecording3, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording3, VALID, 0, 7 * TERM_LENGTH + 96, 11 * TERM_LENGTH + 320,
                18, 100, 7, 13, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingValidRecordingShouldNotUpdateStopPositionIfAlreadyCorrect()
    {
        assertTrue(verifyRecording(
            out, archiveDir, validRecording4, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording4, VALID, 0, 21 * TERM_LENGTH + (TERM_LENGTH - 64),
                22 * TERM_LENGTH + 992, 19, 1, -25, 7, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingValidRecordingValidateAllSegmentFiles()
    {
        assertTrue(verifyRecording(
            out, archiveDir, validRecording3, of(VERIFY_ALL_SEGMENT_FILES), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording3, VALID, 0, 7 * TERM_LENGTH + 96, 11 * TERM_LENGTH + 320,
                18, 100, 7, 13, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingInvalidRecordingValidateAllSegmentFiles()
    {
        assertFalse(verifyRecording(
            out, archiveDir, validRecording4, of(VERIFY_ALL_SEGMENT_FILES), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording4, INVALID, 0, 21 * TERM_LENGTH + (TERM_LENGTH - 64),
                22 * TERM_LENGTH + 992, 19, 1, -25, 7, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingValidRecordingTruncateSegmentFileOnPageStraddleValidChecksum()
    {
        assertTrue(verifyRecording(out, archiveDir, validRecording51, emptySet(), crc32(), epochClock, (file) -> true));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording51, VALID, 0, 0, 64 + PAGE_SIZE, 20, 777,
                0, 20, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingValidRecordingTruncateSegmentFileOnPageStraddleNonZeroBytesInEveryPageAfterTheStraddle()
    {
        assertTrue(verifyRecording(out, archiveDir, validRecording52, emptySet(), crc32(), epochClock, (file) -> true));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording52, VALID, 0, 0, 128 + MTU_LENGTH, 21, 100,
                0, 52, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingValidRecordingTruncateSegmentFileOnPageStraddle()
    {
        assertTrue(verifyRecording(
            out, archiveDir, validRecording53, emptySet(), null, epochClock, (file) -> true));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording53, VALID, 0, 0, 64, 22, 100, 0, 53, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingValidRecordingDoNotTruncateSegmentFileOnPageStraddle()
    {
        assertTrue(verifyRecording(
            out, archiveDir, validRecording53, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording53, VALID, 0, 0, 64 + 3 * PAGE_SIZE, 22, 100, 0, 53, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingValidRecordingPerformCRC()
    {
        final Checksum checksum = crc32();
        try (Catalog catalog = openCatalogReadWrite(archiveDir, epochClock, MIN_CAPACITY, checksum, null))
        {
            assertRecording(catalog,
                validRecording6,
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                catalog.updateChecksum(recordingDescriptorOffset));
        }

        assertTrue(verifyRecording(
            out, archiveDir, validRecording6, of(APPLY_CHECKSUM), checksum, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording6, VALID, -175549265, 352, 960, 23, 100, 0, 6, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfCatalogChecksumIsWrong()
    {
        assertFalse(verifyRecording(
            out, archiveDir, validRecording6, of(APPLY_CHECKSUM), crc32(), epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording6, INVALID, 0, 352, NULL_POSITION, 23,
                NULL_TIMESTAMP, 0, 6, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingInvalidRecordingPerformCRC()
    {
        assertFalse(verifyRecording(
            out, archiveDir, validRecording3, of(APPLY_CHECKSUM), crc32(), epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording3, INVALID, 0, 7 * TERM_LENGTH + 96, 7 * TERM_LENGTH + 128,
                18, NULL_TIMESTAMP, 7, 13, "ch2", "src2");
        }
    }

    @ParameterizedTest
    @EnumSource(value = RecordingState.class, mode = EnumSource.Mode.EXCLUDE, names = { "VALID", "INVALID" })
    void verifyRecordingShouldSkipRecordingIfStateIsDeletedOrUnknown(final RecordingState skipState)
    {
        final long recordingId = validRecording3;
        try (Catalog catalog = openCatalogReadWrite(archiveDir, epochClock, MIN_CAPACITY, null, null))
        {
            assertTrue(catalog.changeState(recordingId, skipState));
        }

        assertTrue(verifyRecording(
            out, archiveDir, recordingId, of(APPLY_CHECKSUM), crc32(), epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, recordingId, skipState, 0, 7 * TERM_LENGTH + 96, 7 * TERM_LENGTH + 128,
                18, NULL_TIMESTAMP, 7, 13, "ch2", "src2");
        }
        Mockito.verify(out).println("(recordingId=" + recordingId + ") skipping: " + skipState);
    }

    @Test
    void dumpShouldSkipRecordingsInNonValidStates()
    {
        try (ArchiveMarkFile markFile = new ArchiveMarkFile(
            new File(archiveDir, ArchiveMarkFile.FILENAME), 1024 * 1024, 8096, epochClock, 0))
        {
            markFile.signalReady(epochClock.time()); // dump() requires a mark file; this fixture has none
        }
        assertFalse(verify(out, archiveDir, emptySet(), null, epochClock, (file) -> false));
        try (Catalog catalog = openCatalogReadWrite(archiveDir, epochClock, MIN_CAPACITY, null, null))
        {
            assertTrue(catalog.changeState(validRecording0, DELETED));
        }

        dump(out, archiveDir, 1, (fragmentCount) -> false);

        Mockito.verify(out).println("Recording " + validRecording3);
        Mockito.verify(out, Mockito.never()).println("Recording " + validRecording0);
        Mockito.verify(out, Mockito.never()).println("Recording " + invalidRecording0);
    }

    @ParameterizedTest
    @ValueSource(longs = {-1000, Long.MAX_VALUE})
    void verifyRecordingShouldThrowExceptionIfRecordingIsUnknown(final long recordingId)
    {
        final AeronException exception = assertThrowsExactly(AeronException.class, () -> verifyRecording(
            out, archiveDir, recordingId, of(APPLY_CHECKSUM), crc32(), epochClock, (file) -> false));
        assertEquals("ERROR - no recording found with recordingId: " + recordingId, exception.getMessage());
    }

    @Test
    void verifyNoOptionsDoNotTruncateOnPageStraddle()
    {
        assertFalse(verify(out, archiveDir, emptySet(), null, epochClock, (file) -> false));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording0, INVALID, 0, NULL_POSITION, NULL_POSITION, 1, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording1, INVALID, 0, FRAME_ALIGNMENT - 7, NULL_POSITION, 2,
                NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording2, INVALID, 0, 1024, FRAME_ALIGNMENT * 2, 3, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording3, INVALID, 0, 0, FRAME_ALIGNMENT * 5 + 11, 4, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording4, INVALID, 0, SEGMENT_LENGTH, NULL_POSITION, 5, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording5, INVALID, 0, 0, SEGMENT_LENGTH, 6, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording6, INVALID, 0, 0, NULL_POSITION, 7, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording7, INVALID, 0, 0, NULL_POSITION, 8, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording8, INVALID, 0, 0, NULL_POSITION, 9, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording9, INVALID, 0, 0, NULL_POSITION, 10, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording10, INVALID, 0, 128, NULL_POSITION, 11, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording11, INVALID, 0, 0, NULL_POSITION, 12, NULL_TIMESTAMP,
                5, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording12, INVALID, 0, 0, NULL_POSITION, 13, NULL_TIMESTAMP,
                9, 6, "ch1", "src1");
            assertRecording(catalog, invalidRecording13, INVALID, 0, 0, NULL_POSITION, 14, NULL_TIMESTAMP,
                0, 13, "ch1", "src1");
            assertRecording(catalog, invalidRecording14, INVALID, 0, 128, NULL_POSITION, -14,
                41, -14, 0, "ch1", "src1");
            assertRecording(catalog, validRecording0, VALID, 0, 0, TERM_LENGTH + 64, 15, 100,
                0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording1, VALID, 0, 1024, 1024, 16, 200,
                0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording2, VALID, 0, TERM_LENGTH * 3 + 96, TERM_LENGTH * 3 + 96,
                17, 300, 0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording3, VALID, 0, 7 * TERM_LENGTH + 96,
                11 * TERM_LENGTH + 320, 18, 400, 7, 13, "ch2", "src2");
            assertRecording(catalog, validRecording4, VALID, 0, 21 * TERM_LENGTH + (TERM_LENGTH - 64),
                22 * TERM_LENGTH + 992, 19, 1, -25, 7, "ch2", "src2");
            assertRecording(catalog, validRecording51, VALID, 0, 0, 64 + PAGE_SIZE, 20, 777,
                0, 20, "ch2", "src2");
            assertRecording(catalog, validRecording52, VALID, 0, 0, 128 + MTU_LENGTH, 21, 500,
                0, 52, "ch2", "src2");
            assertRecording(catalog, validRecording53, VALID, 0, 0, 64 + 3 * PAGE_SIZE, 22, 600,
                0, 53, "ch2", "src2");
            assertRecording(catalog, validRecording6, VALID, 0, 352, 960, 23, 700, 0, 6, "ch2", "src2");
        }

        Mockito.verify(out, times(24)).println(any(String.class));
    }

    @Test
    void verifyAllOptionsTruncateOnPageStraddle()
    {
        final Checksum checksum = crc32();
        try (Catalog catalog = openCatalogReadWrite(archiveDir, epochClock, MIN_CAPACITY, checksum, null))
        {
            catalog.forEach(
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                catalog.updateChecksum(recordingDescriptorOffset));
        }

        assertFalse(verify(out, archiveDir, allOf(VerifyOption.class), checksum, epochClock, (file) -> true));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording0, INVALID, -119969720, NULL_POSITION, NULL_POSITION, 1,
                NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording1, INVALID, 768794941, FRAME_ALIGNMENT - 7, NULL_POSITION, 2,
                NULL_TIMESTAMP, 0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording2, INVALID, -1340428433, 1024, FRAME_ALIGNMENT * 2,
                3, NULL_TIMESTAMP, 0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording3, INVALID, 1464972620, 0, FRAME_ALIGNMENT * 5 + 11,
                4, NULL_TIMESTAMP, 0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording4, INVALID, 21473288, SEGMENT_LENGTH, NULL_POSITION, 5,
                NULL_TIMESTAMP, 0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording5, INVALID, -2119992405, 0, SEGMENT_LENGTH, 6, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording6, INVALID, 2054096463, 0, NULL_POSITION, 7, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording7, INVALID, -1050175867, 0, NULL_POSITION, 8, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording8, INVALID, -504693275, 0, NULL_POSITION, 9, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording9, INVALID, -2036430506, 0, NULL_POSITION, 10, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording10, INVALID, -414736820, 128, NULL_POSITION, 11, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording11, INVALID, 1983095657, 0, NULL_POSITION, 12, NULL_TIMESTAMP,
                5, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording12, INVALID, -1308504240, 0, NULL_POSITION, 13, NULL_TIMESTAMP,
                9, 6, "ch1", "src1");
            assertRecording(catalog, invalidRecording13, INVALID, -273182324, 0, NULL_POSITION, 14, NULL_TIMESTAMP,
                0, 13, "ch1", "src1");
            assertRecording(catalog, invalidRecording14, INVALID, 213018412, 128, NULL_POSITION, -14,
                41, -14, 0, "ch1", "src1");
            assertRecording(catalog, validRecording0, VALID, 356725588, 0, TERM_LENGTH + 64, 15, 100,
                0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording1, VALID, -1571032591, 1024, 1024, 16, 200,
                0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording2, VALID, 114203747, TERM_LENGTH * 3 + 96, TERM_LENGTH * 3 + 96,
                17, 300, 0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording3, INVALID, 963969455, 7 * TERM_LENGTH + 96, 7 * TERM_LENGTH + 128,
                18, NULL_TIMESTAMP, 7, 13, "ch2", "src2");
            assertRecording(catalog, validRecording4, INVALID, 162247708, 21 * TERM_LENGTH + (TERM_LENGTH - 64),
                22 * TERM_LENGTH + 992, 19, 1, -25, 7, "ch2", "src2");
            assertRecording(catalog, validRecording51, VALID, -940881948, 0, 64 + PAGE_SIZE, 20, 777,
                0, 20, "ch2", "src2");
            assertRecording(catalog, validRecording52, INVALID, 1046083782, 0, NULL_POSITION, 21, NULL_TIMESTAMP,
                0, 52, "ch2", "src2");
            assertRecording(catalog, validRecording53, INVALID, 428178649, 0, NULL_POSITION, 22, NULL_TIMESTAMP,
                0, 53, "ch2", "src2");
            assertRecording(catalog, validRecording6, VALID, -175549265, 352, 960, 23, 400, 0, 6, "ch2", "src2");
        }

        Mockito.verify(out, times(24)).println(any(String.class));
    }

    @Test
    void verifyAllShouldSkipDeletedRecordings()
    {
        final Checksum checksum = crc32();
        try (Catalog catalog = openCatalogReadWrite(archiveDir, epochClock, MIN_CAPACITY, checksum, null))
        {
            catalog.forEach(
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                catalog.updateChecksum(recordingDescriptorOffset));
            assertTrue(catalog.changeState(validRecording0, DELETED));
            assertTrue(catalog.changeState(validRecording3, NULL_VAL));
            assertTrue(catalog.changeState(validRecording52, DELETED));
        }

        assertFalse(verify(out, archiveDir, allOf(VerifyOption.class), checksum, epochClock, (file) -> true));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording0, INVALID, -119969720, NULL_POSITION, NULL_POSITION, 1,
                NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording1, INVALID, 768794941, FRAME_ALIGNMENT - 7, NULL_POSITION, 2,
                NULL_TIMESTAMP, 0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording2, INVALID, -1340428433, 1024, FRAME_ALIGNMENT * 2,
                3, NULL_TIMESTAMP, 0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording3, INVALID, 1464972620, 0, FRAME_ALIGNMENT * 5 + 11,
                4, NULL_TIMESTAMP, 0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording4, INVALID, 21473288, SEGMENT_LENGTH, NULL_POSITION, 5,
                NULL_TIMESTAMP, 0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording5, INVALID, -2119992405, 0, SEGMENT_LENGTH, 6, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording6, INVALID, 2054096463, 0, NULL_POSITION, 7, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording7, INVALID, -1050175867, 0, NULL_POSITION, 8, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording8, INVALID, -504693275, 0, NULL_POSITION, 9, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording9, INVALID, -2036430506, 0, NULL_POSITION, 10, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording10, INVALID, -414736820, 128, NULL_POSITION, 11, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording11, INVALID, 1983095657, 0, NULL_POSITION, 12, NULL_TIMESTAMP,
                5, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording12, INVALID, -1308504240, 0, NULL_POSITION, 13, NULL_TIMESTAMP,
                9, 6, "ch1", "src1");
            assertRecording(catalog, invalidRecording13, INVALID, -273182324, 0, NULL_POSITION, 14, NULL_TIMESTAMP,
                0, 13, "ch1", "src1");
            assertRecording(catalog, invalidRecording14, INVALID, 213018412, 128, NULL_POSITION, -14,
                41, -14, 0, "ch1", "src1");
            assertRecording(catalog, validRecording0, DELETED, 356725588, 0, NULL_POSITION, 15, NULL_TIMESTAMP,
                0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording1, VALID, -1571032591, 1024, 1024, 16, 100,
                0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording2, VALID, 114203747, TERM_LENGTH * 3 + 96, TERM_LENGTH * 3 + 96,
                17, 200, 0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording3, NULL_VAL, 963969455, 7 * TERM_LENGTH + 96, 7 * TERM_LENGTH + 128,
                18, NULL_TIMESTAMP, 7, 13, "ch2", "src2");
            assertRecording(catalog, validRecording4, INVALID, 162247708, 21 * TERM_LENGTH + (TERM_LENGTH - 64),
                22 * TERM_LENGTH + 992, 19, 1, -25, 7, "ch2", "src2");
            assertRecording(catalog, validRecording51, VALID, -940881948, 0, 64 + PAGE_SIZE, 20, 777,
                0, 20, "ch2", "src2");
            assertRecording(catalog, validRecording52, DELETED, 1046083782, 0, NULL_POSITION, 21, NULL_TIMESTAMP,
                0, 52, "ch2", "src2");
            assertRecording(catalog, validRecording53, INVALID, 428178649, 0, NULL_POSITION, 22, NULL_TIMESTAMP,
                0, 53, "ch2", "src2");
            assertRecording(catalog, validRecording6, VALID, -175549265, 352, 960, 23, 300, 0, 6, "ch2", "src2");
        }

        Mockito.verify(out, times(24)).println(any(String.class));
        Mockito.verify(out).println("(recordingId=" + validRecording0 + ") skipping: DELETED");
        Mockito.verify(out).println("(recordingId=" + validRecording3 + ") skipping: NULL_VAL");
        Mockito.verify(out).println("(recordingId=" + validRecording52 + ") skipping: DELETED");
    }

    @Test
    void verifyChecksum()
    {
        final Checksum checksum = crc32();
        try (Catalog catalog = openCatalogReadWrite(archiveDir, epochClock, MIN_CAPACITY, checksum, null))
        {
            assertRecording(catalog,
                validRecording51,
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                catalog.updateChecksum(recordingDescriptorOffset));

            assertRecording(catalog,
                validRecording6,
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                catalog.updateChecksum(recordingDescriptorOffset));
        }

        assertFalse(verify(out, archiveDir, of(APPLY_CHECKSUM), checksum, epochClock, (file) -> true));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording0, INVALID, 0, 0, NULL_POSITION, 15, NULL_TIMESTAMP,
                0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording1, INVALID, 0, 1024, NULL_POSITION, 16, NULL_TIMESTAMP,
                0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording2, INVALID, 0, TERM_LENGTH * 3 + 96, NULL_POSITION,
                17, NULL_TIMESTAMP, 0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording3, INVALID, 0, 7 * TERM_LENGTH + 96, 7 * TERM_LENGTH + 128,
                18, NULL_TIMESTAMP, 7, 13, "ch2", "src2");
            assertRecording(catalog, validRecording4, INVALID, 0, 21 * TERM_LENGTH + (TERM_LENGTH - 64),
                22 * TERM_LENGTH + 992, 19, 1, -25, 7, "ch2", "src2");
            assertRecording(catalog, validRecording51, VALID, -940881948, 0, 64 + PAGE_SIZE, 20, 777,
                0, 20, "ch2", "src2");
            assertRecording(catalog, validRecording52, INVALID, 0, 0, NULL_POSITION, 21, NULL_TIMESTAMP,
                0, 52, "ch2", "src2");
            assertRecording(catalog, validRecording53, INVALID, 0, 0, NULL_POSITION, 22, NULL_TIMESTAMP,
                0, 53, "ch2", "src2");
            assertRecording(catalog, validRecording6, VALID, -175549265, 352, 960, 23, 100, 0, 6, "ch2", "src2");
        }

        Mockito.verify(out, times(24)).println(any(String.class));
    }

    @Test
    void checksumRecordingLastSegmentFile()
    {
        checksumRecording(out, archiveDir, validRecording3, false, crc32(), epochClock);

        assertTrue(verifyRecording(
            out, archiveDir, validRecording3, of(APPLY_CHECKSUM), crc32(), epochClock, (file) -> false));
        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording3, VALID, 963969455, 7 * TERM_LENGTH + 96, 11 * TERM_LENGTH + 320,
                18, 100, 7, 13, "ch2", "src2");
        }

        verifyRecording(
            out, archiveDir, validRecording3, allOf(VerifyOption.class), crc32(), epochClock, (file) -> false);
        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording3, INVALID, 963969455, 7 * TERM_LENGTH + 96, 11 * TERM_LENGTH + 320,
                18, 100, 7, 13, "ch2", "src2");
        }
    }

    @Test
    void checksumRecordingAllSegmentFiles()
    {
        checksumRecording(out, archiveDir, validRecording3, true, crc32(), epochClock);

        assertTrue(verifyRecording(
            out, archiveDir, validRecording3, allOf(VerifyOption.class), crc32(), epochClock, (file) -> false));
        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording3, VALID, 963969455, 7 * TERM_LENGTH + 96, 11 * TERM_LENGTH + 320,
                18, 100, 7, 13, "ch2", "src2");
        }
    }

    @Test
    void checksumLastSegmentFile()
    {
        checksum(out, archiveDir, false, crc32(), epochClock);

        assertFalse(verify(out, archiveDir, allOf(VerifyOption.class), crc32(), epochClock, (file) -> false));
        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording0, VALID, 356725588, 0, TERM_LENGTH + 64, 15, 100,
                0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording1, VALID, -1571032591, 1024, 1024, 16, 200,
                0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording2, VALID, 114203747, TERM_LENGTH * 3 + 96, TERM_LENGTH * 3 + 96,
                17, 300, 0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording3, INVALID, 963969455, 7 * TERM_LENGTH + 96, 7 * TERM_LENGTH + 128,
                18, NULL_TIMESTAMP, 7, 13, "ch2", "src2");
            assertRecording(catalog, validRecording4, INVALID, 162247708, 21 * TERM_LENGTH + (TERM_LENGTH - 64),
                22 * TERM_LENGTH + 992, 19, 1, -25, 7, "ch2", "src2");
            assertRecording(catalog, validRecording51, VALID, -940881948, 0, 64 + PAGE_SIZE, 20, 777,
                0, 20, "ch2", "src2");
            assertRecording(catalog, validRecording6, VALID, -175549265, 352, 960, 23, 600, 0, 6, "ch2", "src2");
        }
    }

    @Test
    void checksumAllSegmentFile()
    {
        checksum(out, archiveDir, true, crc32(), epochClock);

        assertFalse(verify(out, archiveDir, allOf(VerifyOption.class), crc32(), epochClock, (file) -> false));
        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording0, VALID, 356725588, 0, TERM_LENGTH + 64, 15, 100,
                0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording1, VALID, -1571032591, 1024, 1024, 16, 200,
                0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording2, VALID, 114203747, TERM_LENGTH * 3 + 96, TERM_LENGTH * 3 + 96,
                17, 300, 0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording3, VALID, 963969455, 7 * TERM_LENGTH + 96, 11 * TERM_LENGTH + 320,
                18, 400, 7, 13, "ch2", "src2");
            assertRecording(catalog, validRecording4, INVALID, 162247708, 21 * TERM_LENGTH + (TERM_LENGTH - 64),
                22 * TERM_LENGTH + 992, 19, 1, -25, 7, "ch2", "src2");
            assertRecording(catalog, validRecording51, VALID, -940881948, 0, 64 + PAGE_SIZE, 20, 777,
                0, 20, "ch2", "src2");
            assertRecording(catalog, validRecording6, VALID, -175549265, 352, 960, 23, 700, 0, 6, "ch2", "src2");
        }
    }

    @ParameterizedTest
    @MethodSource("verifyChecksumClassValidation")
    void verifyWithChecksumFlagThrowsIllegalArgumentExceptionIfClassNameNotSpecified(final String[] args)
    {
        assertChecksumClassNameRequired(args);
    }

    @ParameterizedTest
    @MethodSource("checksumClassValidation")
    void checksumThrowsIllegalArgumentExceptionIfClassNameNotSpecified(final String[] args)
    {
        assertChecksumClassNameRequired(args);
    }

    @Test
    void verifyWithoutChecksumClassNameShouldNotVerifyChecksums()
    {
        assertFalse(verify(out, archiveDir, emptySet(), null, (file) -> true));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecordingState(catalog, validRecording4, VALID);
        }
    }

    @Test
    void verifyShouldNotMarkRecordingAsValidIfNoSegmentFilesAreAttached()
    {
        final long recordingId = validRecording4;
        final ArrayList<String> segmentFiles = listSegmentFiles(archiveDir, recordingId);
        for (final String segmentFile : segmentFiles)
        {
            IoUtil.deleteIfExists(new File(archiveDir, segmentFile));
        }

        try (Catalog catalog = openCatalogReadWrite(archiveDir, epochClock, MIN_CAPACITY, null, null))
        {
            assertTrue(catalog.changeState(recordingId, INVALID));
        }

        assertFalse(verify(out, archiveDir, emptySet(), null, (file) -> true));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecordingState(catalog, recordingId, INVALID);
        }
    }

    @Test
    void verifyRecordingWithoutChecksumClassNameShouldNotVerifyChecksums()
    {
        assertTrue(verifyRecording(
            out, archiveDir, validRecording4, emptySet(), null, (file) -> true));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecordingState(catalog, validRecording4, VALID);
        }
    }

    @Test
    void verifyThrowsIllegalArgumentExceptionIfApplyChecksumIsSetWithoutChecksumClassName()
    {
        assertChecksumClassNameValidation(() -> verify(out, archiveDir, of(APPLY_CHECKSUM), null, (file) -> true));
    }

    @Test
    void verifyRecordingThrowsIllegalArgumentExceptionIfApplyChecksumIsSetWithoutChecksumClassName()
    {
        assertChecksumClassNameValidation(
            () -> verifyRecording(out, archiveDir, validRecording4, of(APPLY_CHECKSUM), null, (file) -> true));
    }

    @Test
    void compactDeletesRecordingsInStateInvalidAndDeletedAndDeletesTheCorrespondingSegmentFiles()
    {
        final File catalogFile = new File(archiveDir, CATALOG_FILE_NAME);
        try (Catalog catalog = openCatalogReadWrite(archiveDir, epochClock, MIN_CAPACITY, null, null))
        {
            assertTrue(catalog.changeState(validRecording1, NULL_VAL));
            assertTrue(catalog.changeState(validRecording3, INVALID));
            assertTrue(catalog.changeState(validRecording6, DELETED));
        }

        final List<String> segmentFiles = new ArrayList<>();
        segmentFiles.addAll(listSegmentFiles(archiveDir, validRecording1));
        segmentFiles.addAll(listSegmentFiles(archiveDir, validRecording3));
        segmentFiles.addAll(listSegmentFiles(archiveDir, validRecording6));

        assertTrue(segmentFiles.stream().allMatch((file) -> new File(archiveDir, file).exists()),
            "Non-existing segment files");

        final long fileLengthBeforeCompact = catalogFile.length();
        compact(out, archiveDir, epochClock);
        final long fileLengthAfterCompact = catalogFile.length();
        assertTrue(fileLengthAfterCompact < fileLengthBeforeCompact);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertEquals(catalog.capacity(), fileLengthAfterCompact);
            assertNoRecording(catalog, validRecording1);
            assertNoRecording(catalog, validRecording3);
            assertNoRecording(catalog, validRecording6);

            assertEquals(21, catalog.entryCount());
            assertRecording(catalog, validRecording0, VALID, 0, 0, NULL_POSITION, 15, NULL_TIMESTAMP,
                0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording51, VALID, 0, 0, 64 + PAGE_SIZE, 20, 777,
                0, 20, "ch2", "src2");
        }

        assertTrue(segmentFiles.stream().noneMatch(file -> new File(archiveDir, file).exists()),
            "Segment files not deleted");
        Mockito.verify(out).println("Compaction result: deleted 3 records and reclaimed 576 bytes");
    }

    @Test
    void capacityReturnsCurrentCapacityInBytesOfTheCatalog()
    {
        final long capacity;
        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            capacity = catalog.capacity();
        }

        assertEquals(capacity, capacity(archiveDir));
    }

    @Test
    void capacityIncreasesCapacityOfTheCatalog()
    {
        final long capacity;
        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            capacity = catalog.capacity();
        }

        assertEquals(capacity * 2, capacity(archiveDir, capacity * 2));
    }

    @Test
    void deleteOrphanedSegmentsDeletesSegmentFilesForAllRecordings() throws IOException
    {
        final long rec1;
        final long rec2;
        try (Catalog catalog = new Catalog(archiveDir, epochClock, 1024, true, null, null))
        {
            rec1 = catalog.addNewRecording(0, NULL_POSITION, NULL_TIMESTAMP, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 42, 5, "some ch", "some ch", "rec1");

            rec2 = catalog.addNewRecording(1_000_000, 1024 * 1024 * 1024, NULL_TIMESTAMP, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch2", "ch2", "rec2");
            assertTrue(catalog.changeState(rec2, INVALID));
        }

        final File file11 = createFile(segmentFileName(rec1, -1));
        final File file12 = createFile(segmentFileName(rec1, 0));
        final File file13 = createFile(segmentFileName(rec1, Long.MAX_VALUE));
        final File file14 = createFile(rec1 + "-will-be-deleted.rec");
        final File file15 = createFile(rec1 + "-will-be-skipped.txt");
        final File file16 = createFile(rec1 + "-.rec");
        final File file17 = createFile(rec1 + "invalid_file_name.rec");

        final File file21 = createFile(segmentFileName(rec2, 0));
        final File file22 = createFile(segmentFileName(
            rec2, segmentFileBasePosition(1_000_000, 1_000_000, TERM_LENGTH, SEGMENT_LENGTH)));
        final File file23 = createFile(segmentFileName(
            rec2, segmentFileBasePosition(1_000_000, 5_000_000, TERM_LENGTH, SEGMENT_LENGTH)));
        final File file24 = createFile(segmentFileName(
            rec2, segmentFileBasePosition(1_000_000, 1024 * 1024 * 1024, TERM_LENGTH, SEGMENT_LENGTH)));
        final File file25 = createFile(segmentFileName(
            rec2, segmentFileBasePosition(1_000_000, Long.MAX_VALUE, TERM_LENGTH, SEGMENT_LENGTH)));

        deleteOrphanedSegments(out, archiveDir);

        assertFileExists(file12, file13, file15, file17);
        assertFileDoesNotExist(file11, file14, file16);

        assertFileExists(file22, file23, file24);
        assertFileDoesNotExist(file21, file25);
    }

    @ParameterizedTest
    @EnumSource(RecordingState.class)
    void deleteOrphanedSegmentsDeletesSegmentFilesOfTargetRecording(final RecordingState state) throws IOException
    {
        final long rec1;
        final long rec2;
        try (Catalog catalog = new Catalog(archiveDir, epochClock, 1024, true, null, null))
        {
            rec1 = catalog.addNewRecording(0, NULL_POSITION, NULL_TIMESTAMP, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 42, 5, "some ch", "some ch", "rec1");

            rec2 = catalog.addNewRecording(1_000_000, 1024 * 1024 * 1024, NULL_TIMESTAMP, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch2", "ch2", "rec2");
            assertTrue(catalog.changeState(rec2, state));
        }

        final File file11 = createFile(segmentFileName(rec1, -1));
        final File file12 = createFile(segmentFileName(rec1, 0));
        final File file13 = createFile(segmentFileName(rec1, Long.MAX_VALUE));
        final File file14 = createFile(rec1 + "-will-be-deleted.rec");
        final File file15 = createFile(rec1 + "-will-be-skipped.txt");
        final File file16 = createFile(rec1 + "-.rec");
        final File file17 = createFile(rec1 + "invalid_file_name.rec");

        final File file21 = createFile(segmentFileName(rec2, 0));
        final File file22 = createFile(segmentFileName(
            rec2, segmentFileBasePosition(1_000_000, 1_000_000, TERM_LENGTH, SEGMENT_LENGTH)));
        final File file23 = createFile(segmentFileName(
            rec2, segmentFileBasePosition(1_000_000, 5_000_000, TERM_LENGTH, SEGMENT_LENGTH)));
        final File file24 = createFile(segmentFileName(
            rec2, segmentFileBasePosition(1_000_000, 1024 * 1024 * 1024, TERM_LENGTH, SEGMENT_LENGTH)));
        final File file25 = createFile(segmentFileName(
            rec2, segmentFileBasePosition(1_000_000, Long.MAX_VALUE, TERM_LENGTH, SEGMENT_LENGTH)));

        deleteOrphanedSegments(out, archiveDir, epochClock, rec2);

        assertFileExists(file12, file13, file15, file17, file11, file14, file16);

        assertFileExists(file22, file23, file24);
        assertFileDoesNotExist(file21, file25);
    }

    @Test
    void deleteOrphanedSegmentsOfAnUnknownRecording()
    {
        final int recordingId = 55555555;
        final AeronException exception =
            assertThrowsExactly(AeronException.class, () -> deleteOrphanedSegments(out, archiveDir, recordingId));
        assertEquals("ERROR - no recording found with recordingId: " + recordingId, exception.getMessage());
    }

    @Test
    void markInvalidInvalidatesAnExistingRecording()
    {
        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertTrue(catalog.hasRecording(validRecording3));
        }

        markRecordingInvalid(out, archiveDir, validRecording3);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertFalse(catalog.hasRecording(validRecording3));
            assertRecordingState(catalog, validRecording3, INVALID);
        }
    }

    @Test
    void markInvalidThrowsExceptionIfRecordingIsUnknown()
    {
        final int recordingId = 100_000;
        final AeronException exception =
            assertThrows(AeronException.class, () -> markRecordingInvalid(out, archiveDir, recordingId));
        assertEquals("ERROR - no recording found with recordingId: " + recordingId, exception.getMessage());
    }

    @ParameterizedTest
    @EnumSource(value = RecordingState.class, mode = EnumSource.Mode.EXCLUDE, names = { "VALID", "INVALID" })
    void markInvalidThrowsExceptionIfRecordingIsNotValid(final RecordingState offendingState)
    {
        final long recordingId = validRecording3;
        try (Catalog catalog = openCatalogReadWrite(archiveDir, epochClock, MIN_CAPACITY, null, null))
        {
            assertTrue(catalog.changeState(recordingId, offendingState));
        }

        final AeronException exception =
            assertThrows(AeronException.class, () -> markRecordingInvalid(out, archiveDir, recordingId));
        assertEquals("ERROR - (recordingId=" + recordingId + ") state transition " + offendingState +
            " -> INVALID is not allowed", exception.getMessage());
    }

    @Test
    void markValidValidatesAnExistingRecording()
    {
        try (Catalog catalog = openCatalogReadWrite(archiveDir, epochClock, MIN_CAPACITY, null, null))
        {
            assertTrue(catalog.changeState(validRecording6, INVALID));
            assertRecordingState(catalog, validRecording6, INVALID);
        }

        markRecordingValid(out, archiveDir, validRecording6);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertTrue(catalog.hasRecording(validRecording6));
        }
    }

    @Test
    void markValidThrowsExceptionIfRecordingIsUnknown()
    {
        final long recordingId = Long.MIN_VALUE;
        final AeronException exception =
            assertThrows(AeronException.class, () -> markRecordingValid(out, archiveDir, recordingId));
        assertEquals("ERROR - no recording found with recordingId: " + recordingId, exception.getMessage());
    }

    @ParameterizedTest
    @EnumSource(value = RecordingState.class, mode = EnumSource.Mode.EXCLUDE, names = { "VALID", "INVALID" })
    void markValidThrowsExceptionIfRecordingIsNotValid(final RecordingState offendingState)
    {
        final long recordingId = validRecording3;
        try (Catalog catalog = openCatalogReadWrite(archiveDir, epochClock, MIN_CAPACITY, null, null))
        {
            assertTrue(catalog.changeState(recordingId, offendingState));
        }

        final AeronException exception =
            assertThrows(AeronException.class, () -> markRecordingValid(out, archiveDir, recordingId));
        assertEquals("ERROR - (recordingId=" + recordingId + ") state transition " + offendingState +
            " -> VALID is not allowed", exception.getMessage());
    }

    private static List<Arguments> verifyChecksumClassValidation()
    {
        final String testDir = validationDir.toAbsolutePath().toString();
        return Arrays.asList(
            Arguments.of((Object)new String[]{ testDir, "verify", "-checksum", "\t" }),
            Arguments.of((Object)new String[]{ testDir, "verify", "-a", "-checksum", " " }),
            Arguments.of((Object)new String[]{ testDir, "verify", "42", "-checksum", "" }),
            Arguments.of((Object)new String[]{ testDir, "verify", "42", "-a", "-checksum", "\r\n" })
        );
    }

    private static List<Arguments> checksumClassValidation()
    {
        final String testDir = validationDir.toAbsolutePath().toString();
        return Arrays.asList(
            Arguments.of((Object)new String[]{ testDir, "checksum", "\n" }),
            Arguments.of((Object)new String[]{ testDir, "checksum", "", "42" }),
            Arguments.of((Object)new String[]{ testDir, "checksum", "\t ", "-a" }),
            Arguments.of((Object)new String[]{ testDir, "checksum", " ", "42", "-a" })
        );
    }

    private void assertChecksumClassNameRequired(final String[] args)
    {
        final IllegalArgumentException ex =
            assertThrows(IllegalArgumentException.class, () -> main(args));
        assertEquals("Checksum class name must be specified!", ex.getMessage());
    }

    private void assertChecksumClassNameValidation(final Executable executable)
    {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, executable);
        assertEquals("Checksum class name is required when " + APPLY_CHECKSUM + " option is specified!",
            ex.getMessage());
    }

    @FunctionalInterface
    interface SegmentWriter
    {
        void write(ByteBuffer byteBuffer, DataHeaderFlyweight dataHeaderFlyweight, FileChannel channel)
            throws IOException;
    }

    private File createFile(final String name) throws IOException
    {
        final File file = new File(archiveDir, name);
        assertTrue(file.createNewFile());
        return file;
    }

    private void createDirectory(final String name)
    {
        final File file = new File(archiveDir, name);
        assertTrue(file.mkdir());
    }

    private void writeToSegmentFile(
        final File file, final int fileLength, final SegmentWriter segmentWriter) throws IOException
    {
        final ByteBuffer byteBuffer = allocate(MTU_LENGTH);
        final DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight(byteBuffer);
        try (FileChannel channel = FileChannel.open(file.toPath(), READ, WRITE))
        {
            segmentWriter.write(byteBuffer, dataHeaderFlyweight, channel);
            final long size = channel.size();
            if (fileLength != size)
            {
                channel.truncate(fileLength);
                if (size < fileLength)
                {
                    byteBuffer.put(0, (byte)0).limit(1).position(0);
                    channel.write(byteBuffer, fileLength - 1);
                }
            }
        }
    }

    private void assertRecording(
        final Catalog catalog, final long recordingId, final CatalogEntryProcessor catalogEntryProcessor)
    {
        final MutableBoolean found = new MutableBoolean();
        catalog
            .forEach((recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
            {
                if (recordingId == descriptorDecoder.recordingId())
                {
                    found.set(true);

                    catalogEntryProcessor.accept(
                        recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder);
                }
            });

        assertTrue(found.get(), () -> "recordingId=" + recordingId + " was not found");
    }

    private void assertRecording(
        final Catalog catalog,
        final long recordingId,
        final RecordingState state,
        final int checksum,
        final long startPosition,
        final long stopPosition,
        final long startTimestamp,
        final long stopTimeStamp,
        final int initialTermId,
        final int streamId,
        final String strippedChannel,
        final String sourceIdentity)
    {
        assertRecording(
            catalog,
            recordingId,
            (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
            {
                assertEquals(state, headerDecoder.state());
                assertEquals(checksum, headerDecoder.checksum());

                assertEquals(startPosition, descriptorDecoder.startPosition());
                assertEquals(stopPosition, descriptorDecoder.stopPosition());
                assertEquals(startTimestamp, descriptorDecoder.startTimestamp());
                assertEquals(stopTimeStamp, descriptorDecoder.stopTimestamp());
                assertEquals(initialTermId, descriptorDecoder.initialTermId());
                assertEquals(MTU_LENGTH, descriptorDecoder.mtuLength());
                assertEquals(SEGMENT_LENGTH, descriptorDecoder.segmentFileLength());
                assertEquals(streamId, descriptorDecoder.streamId());
                assertEquals(strippedChannel, descriptorDecoder.strippedChannel());
                assertNotNull(descriptorDecoder.originalChannel());
                assertEquals(sourceIdentity, descriptorDecoder.sourceIdentity());
            });
    }

    private void assertRecordingState(final Catalog catalog, final long recordingId, final RecordingState expectedState)
    {
        assertRecording(catalog, recordingId,
            (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
            assertEquals(expectedState, headerDecoder.state()));
    }

    private void assertNoRecording(final Catalog catalog, final long recordingId)
    {
        final MutableBoolean found = new MutableBoolean();
        catalog
            .forEach((recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
            {
                if (recordingId == descriptorDecoder.recordingId())
                {
                    found.set(true);
                }
            });

        assertFalse(found.get(), () -> "recordingId=" + recordingId + " was found");
    }

    private void assertFileExists(final File... files)
    {
        for (final File file : files)
        {
            assertTrue(file.exists(), () -> file.getName() + " does not exist");
        }
    }

    private void assertFileDoesNotExist(final File... files)
    {
        for (final File file : files)
        {
            assertFalse(file.exists(), () -> file.getName() + " was not deleted");
        }
    }
}
