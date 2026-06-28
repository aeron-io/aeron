/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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
package io.aeron.cluster;

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RecordingLogValidatorTest
{
    @TempDir
    File tempDir;

    RecordingLog recordingLog;
    RecordingLogValidator recordingLogValidator;

    AeronArchive mockAeronArchive = mock(AeronArchive.class);

    @BeforeEach
    void setup()
    {
        recordingLog = new RecordingLog(tempDir, true);
    }

    @Test
    void shouldInvalidateSnapshotsWithUnknownRecordings()
    {
        recordingLogValidator = new RecordingLogValidator(
            0,
            mockAeronArchive,
            recordingLog);

        recordingLog.appendSnapshot(0, 0, 0, 100, 0, 0);
        recordingLog.appendSnapshot(1, 0, 0, 100, 0, -1);
        recordingLog.appendSnapshot(2, 0, 0, 200, 0, 0);
        recordingLog.appendSnapshot(3, 0, 0, 200, 0, -1);
        recordingLog.appendTerm(4, 1, 0, 0);
        recordingLog.appendStandbySnapshot(0, 1, 0, 300, 0, 0, "localhost:1234");
        recordingLog.appendStandbySnapshot(1, 1, 0, 300, 0, 0, "localhost:1234");

        final int initialSize = recordingLog.entries().size();

        when(mockAeronArchive.getStartPosition(0))
            .thenThrow(new ArchiveException("", ArchiveException.UNKNOWN_RECORDING));
        when(mockAeronArchive.getStartPosition(1))
            .thenThrow(new ArchiveException("", ArchiveException.UNKNOWN_RECORDING));

        for (int x = 0; x < 100 && !recordingLogValidator.isComplete(); x++)
        {
            recordingLogValidator.poll();
        }

        assertTrue(recordingLogValidator.isComplete());

        for (final RecordingLog.Entry entry : recordingLog.entries())
        {
            if (RecordingLog.ENTRY_TYPE_SNAPSHOT == entry.type &&
                (0 == entry.recordingId || 1 == entry.recordingId))
            {
                assertFalse(entry.isValid);
            }
            else
            {
                assertTrue(entry.isValid);
            }
        }

        assertEquals(initialSize, recordingLog.entries().size());
    }
}
