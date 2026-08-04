/*
 * Copyright 2014-2026 Real Logic Limited.
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

import io.aeron.CommonContext;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.stream.Stream;

import static io.aeron.CommonContext.THREAD_NAMING_MODE_CLASSIC;
import static io.aeron.CommonContext.THREAD_NAMING_MODE_NEW;
import static io.aeron.CommonContext.THREAD_NAMING_MODE_PROP_NAME;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_CONDUCTOR_THREAD_NAME;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_CONDUCTOR_THREAD_NAME_CLASSIC;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_NATIVE_RESOURCE_THREAD_NAME;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_RECEIVER_THREAD_NAME;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_RECEIVER_THREAD_NAME_CLASSIC;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_SENDER_THREAD_NAME;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_SENDER_THREAD_NAME_CLASSIC;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_SHARED_NETWORK_THREAD_NAME;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_SHARED_THREAD_NAME;
import static java.lang.management.ManagementFactory.getThreadMXBean;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class MediaDriverThreadNamingTest
{
    private static final String TEST_AERON_DIR = CommonContext.generateRandomDirName();

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();
    private TestMediaDriver driver;

    @AfterEach
    void after()
    {
        CloseHelper.close(driver);
    }

    private static Stream<Arguments> expectedNamePerThreadingMode()
    {
        return Stream.of(
            Arguments.of(
                ThreadingMode.DEDICATED,
                THREAD_NAMING_MODE_CLASSIC,
                new String[]{
                    AERON_DRIVER_SENDER_THREAD_NAME_CLASSIC,
                    AERON_DRIVER_RECEIVER_THREAD_NAME_CLASSIC,
                    AERON_DRIVER_CONDUCTOR_THREAD_NAME_CLASSIC,
                    AERON_DRIVER_NATIVE_RESOURCE_THREAD_NAME
                }),
            Arguments.of(
                ThreadingMode.DEDICATED,
                THREAD_NAMING_MODE_NEW,
                new String[]{
                    AERON_DRIVER_SENDER_THREAD_NAME,
                    AERON_DRIVER_RECEIVER_THREAD_NAME,
                    AERON_DRIVER_CONDUCTOR_THREAD_NAME,
                    AERON_DRIVER_NATIVE_RESOURCE_THREAD_NAME
                }),
            Arguments.of(
                ThreadingMode.SHARED_NETWORK,
                THREAD_NAMING_MODE_CLASSIC,
                new String[]{
                    AERON_DRIVER_CONDUCTOR_THREAD_NAME_CLASSIC,
                    AERON_DRIVER_NATIVE_RESOURCE_THREAD_NAME,
                    String.format("%s [%s,%s]",
                        TEST_AERON_DIR,
                        AERON_DRIVER_SENDER_THREAD_NAME_CLASSIC,
                        AERON_DRIVER_RECEIVER_THREAD_NAME_CLASSIC)
                }),
            Arguments.of(
                ThreadingMode.SHARED_NETWORK,
                THREAD_NAMING_MODE_NEW,
                new String[]{
                    AERON_DRIVER_CONDUCTOR_THREAD_NAME,
                    AERON_DRIVER_NATIVE_RESOURCE_THREAD_NAME,
                    AERON_DRIVER_SHARED_NETWORK_THREAD_NAME
                }),
            Arguments.of(
                ThreadingMode.SHARED,
                THREAD_NAMING_MODE_CLASSIC,
                new String[]{
                    String.format("%s [%s,%s,%s,%s]",
                        TEST_AERON_DIR,
                        AERON_DRIVER_SENDER_THREAD_NAME_CLASSIC,
                        AERON_DRIVER_RECEIVER_THREAD_NAME_CLASSIC,
                        AERON_DRIVER_NATIVE_RESOURCE_THREAD_NAME,
                        AERON_DRIVER_CONDUCTOR_THREAD_NAME_CLASSIC)
                }),
            Arguments.of(
                ThreadingMode.SHARED,
                THREAD_NAMING_MODE_NEW,
                new String[]{
                    AERON_DRIVER_SHARED_THREAD_NAME
                })
        );
    }

    @ParameterizedTest
    @MethodSource("expectedNamePerThreadingMode")
    void testSenderThreadName(
        final ThreadingMode threadingMode,
        final String threadNamingMode,
        final String[] expectedThreadNames)
    {
        System.setProperty(THREAD_NAMING_MODE_PROP_NAME, threadNamingMode);
        try
        {

            final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(TEST_AERON_DIR)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .threadingMode(threadingMode);

            driver = TestMediaDriver.launch(context, systemTestWatcher);
            systemTestWatcher.dataCollector().add(driver.context().aeronDirectory());

            final ThreadMXBean threadBean = getThreadMXBean();
            final long[] threadIds = threadBean.getAllThreadIds();
            final ThreadInfo[] threadInfos = threadBean.getThreadInfo(threadIds, 0);
            final String[] desiredThreads = Arrays.stream(threadInfos)
                .map(ThreadInfo::getThreadName)
                .filter(threadName -> Arrays.asList(expectedThreadNames).contains(threadName))
                .sorted()
                .toArray(String[]::new);

            Arrays.sort(expectedThreadNames);
            assertArrayEquals(expectedThreadNames, desiredThreads);
        }
        finally
        {
            System.clearProperty(THREAD_NAMING_MODE_PROP_NAME);
        }
    }
}
