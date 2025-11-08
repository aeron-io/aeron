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

import io.aeron.archive.codecs.RecordingState;
import io.aeron.exceptions.AeronException;
import org.agrona.CloseHelper;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.SystemEpochClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.*;

import static io.aeron.archive.Catalog.PAGE_SIZE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.NULL_TIMESTAMP;
import static io.aeron.archive.codecs.RecordingState.*;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class ArchiveToolCliTest
{
    private static final int MTU_LENGTH = PAGE_SIZE * 4;
    private static final int TERM_LENGTH = MTU_LENGTH * 8;
    private static final int SEGMENT_LENGTH = TERM_LENGTH * 4;

    private @TempDir File archiveDir;
    private ArchiveMarkFile markFile;
    private long currentTimeMillis = 0;
    private final EpochClock epochClock = () -> currentTimeMillis += 100;

    @BeforeEach
    void before()
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, 1024, epochClock, null, null))
        {
            catalog.addNewRecording(0, NULL_POSITION, 15, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "ch2", "ch2?tag=OK", "src2");
            catalog.addNewRecording(0, NULL_POSITION, 15, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "ch2", "ch2?tag=OK", "src2");
            catalog.addNewRecording(0, NULL_POSITION, 15, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "ch2", "ch2?tag=OK", "src2");
            catalog.addNewRecording(0, NULL_POSITION, 15, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "ch2", "ch2?tag=OK", "src2");
        }

        final SystemEpochClock clock = SystemEpochClock.INSTANCE;
        markFile = new ArchiveMarkFile(
            new File(archiveDir, ArchiveMarkFile.FILENAME), 1024 * 64, 8192, clock, 1000);
        markFile.signalReady(clock.time());
    }

    @AfterEach
    void after()
    {
        CloseHelper.close(markFile);
    }

    @ParameterizedTest
    @EnumSource(RecordingState.class)
    void describeRecordingShouldDescribeExistingRecording(final RecordingState state)
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, 1024, epochClock, null, null))
        {
            assertTrue(catalog.changeState(1, state));
        }

        final OutputConsole console = runArchiveTool("describe", "1");
        assertThat(console.systemOutText(), containsString("|recordingId=1|"));
    }

    @Test
    void describeRecordingShouldThrowExceptionOnUnknownRecording()
    {
        final AeronException exception =
            assertThrowsExactly(AeronException.class, () -> runArchiveTool("describe", "10"));
        assertEquals("ERROR - no recording found with recordingId: 10", exception.getMessage());
    }

    @Test
    void describeRecordingsShouldNotShowNonValidRecordings()
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, 1024, epochClock, null, null))
        {
            assertTrue(catalog.changeState(1, DELETED));
            assertTrue(catalog.changeState(3, INVALID));
            assertTrue(catalog.changeState(0, NULL_VAL));
        }

        final OutputConsole console = runArchiveTool("describe");

        final String consoleText = console.systemOutText();
        assertThat(consoleText, allOf(
            containsString("|recordingId=2|"),
            not(containsString("|recordingId=0|")),
            not(containsString("|recordingId=1|")),
            not(containsString("|recordingId=3|"))));
    }

    @Test
    void describeAllRecordingsShouldShowAllRecordings()
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, 1024, epochClock, null, null))
        {
            assertTrue(catalog.changeState(1, DELETED));
            assertTrue(catalog.changeState(2, INVALID));
            assertTrue(catalog.changeState(3, NULL_VAL));
        }

        final OutputConsole console = runArchiveTool("describe-all");

        final String consoleText = console.systemOutText();
        assertThat(consoleText, allOf(
            containsString("|recordingId=0|"),
            containsString("|recordingId=1|"),
            containsString("|recordingId=2|"),
            containsString("|recordingId=3|")));
    }

    private OutputConsole runArchiveTool(final String... args)
    {
        final PrintStream originalSystemOut = System.out;
        try
        {
            final OutputConsole outputConsole = new OutputConsole();
            System.setOut(outputConsole.systemOut());

            final String[] mainArgs = new String[args.length + 1];
            mainArgs[0] = archiveDir.getAbsolutePath();
            System.arraycopy(args, 0, mainArgs, 1, args.length);

            ArchiveTool.main(mainArgs);
            return outputConsole;
        }
        finally
        {
            System.setOut(originalSystemOut);
        }
    }

    private static final class OutputConsole
    {
        private final OutputStream outputBytes = new ByteArrayOutputStream();
        private final PrintStream sysOut = new PrintStream(outputBytes);

        public PrintStream systemOut()
        {
            return sysOut;
        }

        public String systemOutText()
        {
            return outputBytes.toString();
        }
    }
}
