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
import io.aeron.CommonContext;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.aeron.CommonContext.generateRandomDirName;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class NextCorrelationIdTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(driver);
    }

    @Test
    void shouldStoreFilePageSizeInTheCnCFileMetadata()
    {
        driver = TestMediaDriver.launch(new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .threadingMode(ThreadingMode.SHARED)
            .aeronDirectoryName(generateRandomDirName()), systemTestWatcher);

        systemTestWatcher.dataCollector().add(driver.context().aeronDirectory());

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            final long prevId = aeron.nextCorrelationId();
            assertEquals(prevId + 1, CommonContext.nextCorrelationId(
                aeron.context().aeronDirectory(),
                aeron.context().epochClock(),
                aeron.context().driverTimeoutMs()));
        }
    }
}
