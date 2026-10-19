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

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.aeron.LogBuffers.MIN_LOG_BUFFER_FILE_SIZE;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class LogBuffersTest
{
    @ParameterizedTest
    @ValueSource(ints = { -100, 0, TERM_MIN_LENGTH >> 1, TERM_MAX_LENGTH + 1, TERM_MAX_LENGTH - 1 })
    void throwsIllegalStateExceptionIfTermLengthIsInvalid(final int termLength, @TempDir final Path dir)
        throws IOException
    {
        final Path logFile = dir.resolve("test.log");
        assertNotNull(Files.createFile(logFile));
        final byte[] contents = new byte[(PARTITION_COUNT * TERM_MIN_LENGTH) + LOG_META_DATA_LENGTH];
        final UnsafeBuffer buffer =
            new UnsafeBuffer(contents, contents.length - LOG_META_DATA_LENGTH, LOG_META_DATA_LENGTH);
        termLength(buffer, termLength);
        assertNotNull(Files.write(logFile, contents));
        assertEquals(contents.length, Files.size(logFile));

        final IllegalStateException exception = assertThrowsExactly(
            IllegalStateException.class, () -> new LogBuffers(logFile.toAbsolutePath().toString()));
        assertThat(exception.getMessage(), allOf(startsWith("Term length"), endsWith("length=" + termLength)));
    }

    @ParameterizedTest
    @ValueSource(ints = { -100, 0, PAGE_MIN_SIZE >> 1, PAGE_MAX_SIZE + 1, PAGE_MAX_SIZE - 1 })
    void throwsIllegalStateExceptionIfPageSizeIsInvalid(final int pageSize, @TempDir final Path dir)
        throws IOException
    {
        final Path logFile = dir.resolve("test.log");
        assertNotNull(Files.createFile(logFile));
        final byte[] contents = new byte[(PARTITION_COUNT * TERM_MIN_LENGTH) + LOG_META_DATA_LENGTH];
        final UnsafeBuffer buffer =
            new UnsafeBuffer(contents, contents.length - LOG_META_DATA_LENGTH, LOG_META_DATA_LENGTH);
        termLength(buffer, TERM_MIN_LENGTH);
        pageSize(buffer, pageSize);
        assertNotNull(Files.write(logFile, contents));
        assertEquals(contents.length, Files.size(logFile));

        final IllegalStateException exception = assertThrowsExactly(
            IllegalStateException.class, () -> new LogBuffers(logFile.toAbsolutePath().toString()));
        assertThat(exception.getMessage(), allOf(startsWith("Page size"), endsWith("page size=" + pageSize)));
    }

    @Test
    void throwsIllegalStateExceptionIfLogFileSizeIsLessThanMinLength(@TempDir final Path dir)
        throws IOException
    {
        final Path logFile = dir.resolve("test.log");
        assertNotNull(Files.createFile(logFile));
        final int fileLength = LOG_META_DATA_LENGTH + 10;
        final byte[] contents = new byte[fileLength];
        final UnsafeBuffer buffer = new UnsafeBuffer(contents);
        termLength(buffer, TERM_MIN_LENGTH);
        pageSize(buffer, PAGE_MIN_SIZE);
        assertNotNull(Files.write(logFile, contents));
        assertEquals(contents.length, Files.size(logFile));

        final IllegalStateException exception = assertThrowsExactly(
            IllegalStateException.class, () -> new LogBuffers(logFile.toAbsolutePath().toString()));
        assertEquals("Log file length less than min length of " + MIN_LOG_BUFFER_FILE_SIZE + ": length=" +
            fileLength + ", filename=" + logFile,
            exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(ints = { TERM_MIN_LENGTH * 2, TERM_MAX_LENGTH, 1024 * 1024 })
    void throwsIllegalStateExceptionIfFileSizeIsTooSmall(final int termLength, @TempDir final Path dir)
        throws IOException
    {
        final Path logFile = dir.resolve("test.log");
        assertNotNull(Files.createFile(logFile));
        final byte[] contents = new byte[(PARTITION_COUNT * TERM_MIN_LENGTH) + LOG_META_DATA_LENGTH];
        final UnsafeBuffer buffer =
            new UnsafeBuffer(contents, contents.length - LOG_META_DATA_LENGTH, LOG_META_DATA_LENGTH);
        termLength(buffer, termLength);
        pageSize(buffer, PAGE_MIN_SIZE);
        assertNotNull(Files.write(logFile, contents));
        assertEquals(contents.length, Files.size(logFile));

        final IllegalStateException exception = assertThrowsExactly(
            IllegalStateException.class, () -> new LogBuffers(logFile.toAbsolutePath().toString()));
        assertEquals("Log file length is too short for its term length: length=" +
                contents.length + ", termLength=" + termLength + ", filename=" + logFile,
            exception.getMessage());
    }
}
