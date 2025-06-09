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
package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.ErrorCode;
import io.aeron.exceptions.RegistrationException;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class DriverSpaceTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();
    private Path aeronDir;
    private Path publicationsDir;

    @BeforeEach
    void verifyFileSystemSetup()
    {
        final Path tempfsDir;
        switch (OS.current())
        {
            case WINDOWS:
                tempfsDir = new File("T:/tmp_aeron_dir").toPath();
                break;
            case MAC:
                tempfsDir = new File("/Volumes/tmp_aeron_dir").toPath();
                break;
            default:
                tempfsDir = new File("/mnt/tmp_aeron_dir").toPath();
                break;
        }
        assumeTrue(Files.exists(tempfsDir), () -> tempfsDir + " does not exist");
        assumeTrue(Files.isDirectory(tempfsDir), () -> tempfsDir + " is not a directory");
        assumeTrue(Files.isWritable(tempfsDir), () -> tempfsDir + " is not writable");

        try
        {
            final FileStore fileStore = Files.getFileStore(tempfsDir);
            assumeTrue(fileStore.getUsableSpace() < (64 * 1024 * 1024), "Skipping as file system is too large");
        }
        catch (final IOException e)
        {
            abort("File store not accessible");
        }

        aeronDir = tempfsDir.resolve("aeron-no-space");
        publicationsDir = aeronDir.resolve("publications");
    }

    @Test
    void shouldCreatePublicationUsingSparseFiles()
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(aeronDir.toString())
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .performStorageChecks(false);

        try (TestMediaDriver driver = TestMediaDriver.launch(context, systemTestWatcher);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            final ConcurrentPublication publication =
                aeron.addPublication("aeron:ipc?term-length=1g|sparse=true", 20002);
            assertNotNull(publication);

            final File[] files = publicationsDir.toFile().listFiles();
            assertNotNull(files);
            assertEquals(1, files.length);
            final File file = files[0];
            assertEquals(3221229568L, file.length());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldThrowExceptionIfOutOfDiscSpace(final boolean performStorageChecks)
    {
        assumeTrue(performStorageChecks || OS.WINDOWS == OS.current() || TestMediaDriver.shouldRunCMediaDriver());

        final MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(aeronDir.toString())
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .performStorageChecks(performStorageChecks);

        try (TestMediaDriver driver = TestMediaDriver.launch(context, systemTestWatcher);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            try
            {
                aeron.addPublication("aeron:ipc?term-length=16m|sparse=false", 10001);
                fail("RegistrationException was not thrown");
            }
            catch (final RegistrationException ex)
            {
                assertEquals(ErrorCode.STORAGE_SPACE, ex.errorCode());
                assertTrue(Files.exists(publicationsDir));
                assertArrayEquals(new String[0], publicationsDir.toFile().list(), "Log file was not deleted");
            }
        }
    }
}
