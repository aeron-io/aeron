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

import io.aeron.test.SystemTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.SystemEpochClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static io.aeron.CommonContext.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FilePageSizeTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(driver);
    }

    @ParameterizedTest
    @ValueSource(ints = { 16 * 1024, 2 * 1024 * 1024 })
    void shouldStoreFilePageSizeInTheCnCFileMetadata(final int filePageSize)
    {
        driver = TestMediaDriver.launch(new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .threadingMode(ThreadingMode.SHARED)
            .aeronDirectoryName(generateRandomDirName())
            .filePageSize(filePageSize), systemTestWatcher);

        systemTestWatcher.dataCollector().add(driver.context().aeronDirectory());

        assertEquals(filePageSize, driverFilePageSize(
            new File(driver.aeronDirectoryName()), SystemEpochClock.INSTANCE, TimeUnit.SECONDS.toMillis(10)));
    }
}
