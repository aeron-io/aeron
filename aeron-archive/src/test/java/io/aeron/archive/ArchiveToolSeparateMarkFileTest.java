/*
 * Copyright 2014-2024 Real Logic Limited.
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

import io.aeron.driver.MediaDriver;
import io.aeron.test.TestContexts;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Objects;

class ArchiveToolSeparateMarkFileTest
{
    @Test
    void shouldDescribe(@TempDir final File archiveDir, @TempDir final File markFileDir)
    {
        final MediaDriver.Context driverContext = new MediaDriver.Context();
        final Archive.Context archiveContext = TestContexts.localhostArchive()
            .aeronDirectoryName(driverContext.aeronDirectoryName())
            .archiveDir(archiveDir)
            .markFileDir(markFileDir);

        try (MediaDriver driver = MediaDriver.launch(driverContext);
            Archive archive = Archive.launch(archiveContext))
        {
            Objects.requireNonNull(driver);
            Objects.requireNonNull(archive);

            ArchiveTool.describe(new PrintStream(new ByteArrayOutputStream()), archiveDir);
        }
    }

    @Test
    void shouldDescribeRecordings(@TempDir final File archiveDir, @TempDir final File markFileDir)
    {
        final MediaDriver.Context driverContext = new MediaDriver.Context();
        final Archive.Context archiveContext = TestContexts.localhostArchive()
            .aeronDirectoryName(driverContext.aeronDirectoryName())
            .archiveDir(archiveDir)
            .markFileDir(markFileDir);

        try (MediaDriver driver = MediaDriver.launch(driverContext);
            Archive archive = Archive.launch(archiveContext))
        {
            Objects.requireNonNull(driver);
            Objects.requireNonNull(archive);

            ArchiveTool.describeAll(new PrintStream(new ByteArrayOutputStream()), archiveDir);
        }
    }

    @Test
    void shouldSetCapacity(@TempDir final File archiveDir, @TempDir final File markFileDir)
    {
        final MediaDriver.Context driverContext = new MediaDriver.Context();
        final Archive.Context archiveContext = TestContexts.localhostArchive()
            .aeronDirectoryName(driverContext.aeronDirectoryName())
            .archiveDir(archiveDir)
            .markFileDir(markFileDir);

        try (MediaDriver driver = MediaDriver.launch(driverContext);
            Archive archive = Archive.launch(archiveContext))
        {
            Objects.requireNonNull(driver);
            Objects.requireNonNull(archive);

            ArchiveTool.capacity(archiveDir, 1 << 16);
        }
    }
}
