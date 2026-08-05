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

package io.aeron.archive;

import io.aeron.CommonContext;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.SystemUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.aeron.CommonContext.THREAD_NAMING_CLASSIC;
import static io.aeron.CommonContext.THREAD_NAMING_NEW;
import static io.aeron.archive.Archive.AERON_ARCHIVE_CONDUCTOR_THREAD_NAME;
import static io.aeron.archive.Archive.AERON_ARCHIVE_CONDUCTOR_THREAD_NAME_CLASSIC;
import static io.aeron.archive.Archive.AERON_ARCHIVE_RECORDER_THREAD_NAME;
import static io.aeron.archive.Archive.AERON_ARCHIVE_RECORDER_THREAD_NAME_CLASSIC;
import static io.aeron.archive.Archive.AERON_ARCHIVE_REPLAYER_THREAD_NAME;
import static io.aeron.archive.Archive.AERON_ARCHIVE_REPLAYER_THREAD_NAME_CLASSIC;
import static io.aeron.archive.Archive.AERON_ARCHIVE_SHARED_THREAD_NAME;
import static io.aeron.archive.Archive.AERON_ARCHIVE_SHARED_THREAD_NAME_CLASSIC;
import static io.aeron.archive.ArchiveSystemTests.CATALOG_CAPACITY;
import static io.aeron.archive.ArchiveSystemTests.TERM_LENGTH;
import static java.lang.management.ManagementFactory.getThreadMXBean;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class ArchiveThreadNamingTest
{

    private static Stream<Arguments> expectedNamePerThreadingMode()
    {
        return Stream.of(
            Arguments.of(
                ArchiveThreadingMode.DEDICATED,
                THREAD_NAMING_CLASSIC,
                new String[]{
                    AERON_ARCHIVE_RECORDER_THREAD_NAME_CLASSIC,
                    AERON_ARCHIVE_REPLAYER_THREAD_NAME_CLASSIC,
                    AERON_ARCHIVE_CONDUCTOR_THREAD_NAME_CLASSIC
                }),
            Arguments.of(
                ArchiveThreadingMode.DEDICATED,
                THREAD_NAMING_NEW,
                new String[]{
                    AERON_ARCHIVE_RECORDER_THREAD_NAME,
                    AERON_ARCHIVE_REPLAYER_THREAD_NAME,
                    AERON_ARCHIVE_CONDUCTOR_THREAD_NAME
                }),
            Arguments.of(
                ArchiveThreadingMode.SHARED,
                THREAD_NAMING_CLASSIC,
                new String[]{
                    AERON_ARCHIVE_SHARED_THREAD_NAME_CLASSIC
                }),
            Arguments.of(
                ArchiveThreadingMode.SHARED,
                THREAD_NAMING_NEW,
                new String[]{
                    AERON_ARCHIVE_SHARED_THREAD_NAME
                })

        );
    }

    @ParameterizedTest
    @MethodSource("expectedNamePerThreadingMode")
    @SuppressWarnings("try")
    public void shouldHaveCorrectArchiveThreadNames(
        final ArchiveThreadingMode threadingMode,
        final String threadNaming,
        final String[] expectedThreadNames)
    {
        System.setProperty(CommonContext.THREAD_NAMING_PROP_NAME, threadNaming);
        try
        {
            final String aeronDirectoryName = CommonContext.generateRandomDirName();
            final MediaDriver.Context driverCtx = new MediaDriver.Context()
                .aeronDirectoryName(aeronDirectoryName)
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED)
                .spiesSimulateConnection(false)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
            final File archiveDir = new File(SystemUtil.tmpDirName(), "archive");
            final Archive.Context archiveCtx = TestContexts.localhostArchive()
                .catalogCapacity(CATALOG_CAPACITY)
                .segmentFileLength(TERM_LENGTH)
                .aeronDirectoryName(aeronDirectoryName)
                .deleteArchiveOnStart(true)
                .archiveDir(archiveDir)
                .fileSyncLevel(0)
                .maxConcurrentRecordings(50)
                .threadingMode(threadingMode);

            try (TestMediaDriver driver = TestMediaDriver.launch(driverCtx, null);
                Archive archive = Archive.launch(archiveCtx);)
            {
                Arrays.sort(expectedThreadNames);
                final ThreadMXBean threadBean = getThreadMXBean();
                final String[][] desiredThreads = { new String[0] };

                Tests.await(
                    () ->
                    {
                        final long[] threadIds = threadBean.getAllThreadIds();
                        final ThreadInfo[] threadInfos = threadBean.getThreadInfo(threadIds, 0);
                        desiredThreads[0] = Arrays.stream(threadInfos)
                            .filter(Objects::nonNull)
                            .map(ThreadInfo::getThreadName)
                            .filter(threadName -> Arrays.asList(expectedThreadNames).contains(threadName))
                            .sorted()
                            .toArray(String[]::new);

                        return Arrays.equals(expectedThreadNames, desiredThreads[0]);
                    },
                    TimeUnit.SECONDS.toNanos(5));

                assertArrayEquals(expectedThreadNames, desiredThreads[0]);
            }
        }
        finally
        {
            System.clearProperty(CommonContext.THREAD_NAMING_PROP_NAME);
        }
    }
}
