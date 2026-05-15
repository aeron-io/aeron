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
package io.aeron.samples;

import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.CapturingPrintStream;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

class LogInspectorTest
{
    private static final int TERM_LENGTH = TERM_MIN_LENGTH;
    private static final int META_DATA_OFFSET = PARTITION_COUNT * TERM_LENGTH;
    private static final int FILE_SIZE = META_DATA_OFFSET + LOG_META_DATA_LENGTH;
    private static final int INITIAL_TERM_ID = 7;
    private static final int MTU_LENGTH = 1408;

    @TempDir
    private Path tempDir;

    private final CapturingPrintStream capturingOut = new CapturingPrintStream();

    @Test
    void shouldOutputLogMetadata() throws IOException
    {
        final Path logFile = createLogFile(tempDir, new byte[0]);

        LogInspector.run(logFile.toString(), 0, "hex", false, false, capturingOut.resetAndGetPrintStream());

        final String output = capturingOut.flushAndGetContent();
        assertThat(output, containsString("Initial term id: " + INITIAL_TERM_ID));
        assertThat(output, containsString("Term length: " + TERM_LENGTH));
        assertThat(output, containsString("MTU length: " + MTU_LENGTH));
        assertThat(output, containsString("Page size: " + PAGE_MIN_SIZE));
    }

    @Test
    void shouldIncludeDefaultHeaderByDefault() throws IOException
    {
        final Path logFile = createLogFile(tempDir, new byte[0]);

        LogInspector.run(logFile.toString(), 0, "hex", false, false, capturingOut.resetAndGetPrintStream());

        assertThat(capturingOut.flushAndGetContent(), containsString("default "));
    }

    @Test
    void shouldSkipDefaultHeaderWhenRequested() throws IOException
    {
        final Path logFile = createLogFile(tempDir, new byte[0]);

        LogInspector.run(logFile.toString(), 0, "hex", true, false, capturingOut.resetAndGetPrintStream());

        assertThat(capturingOut.flushAndGetContent(), not(containsString("default ")));
    }

    @Test
    void shouldFormatMessageBodyAsHex() throws IOException
    {
        final byte[] body = { 0x0A, 0x0B, 0x0C };
        final Path logFile = createLogFile(tempDir, body);

        LogInspector.run(logFile.toString(), body.length, "hex", false, false, capturingOut.resetAndGetPrintStream());

        assertThat(capturingOut.flushAndGetContent(), containsString("0A0B0C"));
    }

    @Test
    void shouldFormatMessageBodyAsAscii() throws IOException
    {
        final byte[] body = { 0x41, 0x42, 0x43 }; // "ABC"
        final Path logFile = createLogFile(tempDir, body);

        LogInspector.run(logFile.toString(), body.length, "ascii", false, false, capturingOut.resetAndGetPrintStream());

        assertThat(capturingOut.flushAndGetContent(), containsString("ABC"));
    }

    @Test
    void shouldLimitMessageBodyDumpToRequestedSize() throws IOException
    {
        // Sequential bytes make it easy to verify truncation: 01020304 | 05060708
        final byte[] body = { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
        final Path logFile = createLogFile(tempDir, body);

        LogInspector.run(logFile.toString(), 4, "hex", false, false, capturingOut.resetAndGetPrintStream());

        final String output = capturingOut.flushAndGetContent();
        assertThat(output, containsString("01020304"));
        assertThat(output, not(containsString("0102030405060708")));
    }

    private Path createLogFile(final Path dir, final byte[] frameBody) throws IOException
    {
        final byte[] fileBytes = new byte[FILE_SIZE];
        final UnsafeBuffer metaDataBuffer = new UnsafeBuffer(fileBytes, META_DATA_OFFSET, LOG_META_DATA_LENGTH);

        LogBufferDescriptor.termLength(metaDataBuffer, TERM_LENGTH);
        LogBufferDescriptor.pageSize(metaDataBuffer, PAGE_MIN_SIZE);
        LogBufferDescriptor.mtuLength(metaDataBuffer, MTU_LENGTH);
        LogBufferDescriptor.initialTermId(metaDataBuffer, INITIAL_TERM_ID);
        LogBufferDescriptor.storeDefaultFrameHeader(metaDataBuffer, new UnsafeBuffer(new byte[HEADER_LENGTH]));

        if (frameBody.length > 0)
        {
            final int frameLength = HEADER_LENGTH + frameBody.length;
            final UnsafeBuffer term0 = new UnsafeBuffer(fileBytes, 0, TERM_LENGTH);
            term0.putInt(0, frameLength);
            term0.putBytes(HEADER_LENGTH, frameBody);
        }

        final Path logFile = dir.resolve("test.log");
        Files.write(logFile, fileBytes);
        return logFile;
    }
}
