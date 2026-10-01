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
import io.aeron.exceptions.ConfigurationException;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.TestContexts;
import io.aeron.test.ThreadAffinityRecording;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.collections.IntArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static io.aeron.archive.Archive.AERON_ARCHIVE_CONDUCTOR_THREAD_NAME;
import static io.aeron.archive.Archive.AERON_ARCHIVE_RECORDER_THREAD_NAME;
import static io.aeron.archive.Archive.AERON_ARCHIVE_REPLAYER_THREAD_NAME;
import static io.aeron.archive.Archive.AERON_ARCHIVE_SHARED_THREAD_NAME;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@EnabledOnOs(OS.LINUX)
@ExtendWith(InterruptingTestCallback.class)
@SlowTest
class ArchiveThreadAffinityTest
{
    private static final String DRIVER_SHARED_THREAD_NAME = "aeron-md-shd";

    @TempDir
    private Path archiveDir;

    private final String aeronDirectoryName = CommonContext.generateRandomDirName();

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("try")
    void shouldPinDedicatedArchiveThreadsToCpusetIndices()
    {
        final IntArrayList cpus = requireCpus(3);
        try (ThreadAffinityRecording recording = new ThreadAffinityRecording();
            TestMediaDriver ignore = TestMediaDriver.launch(driverContext(), null);
            Archive ignore2 = Archive.launch(archiveContext()
                .threadingMode(ArchiveThreadingMode.DEDICATED)
                .cpusetAffinity(true)
                .conductorCpuAffinity(2)
                .recorderCpuAffinity(0)
                .replayerCpuAffinity(1)))
        {
            recording.awaitAffinity(AERON_ARCHIVE_CONDUCTOR_THREAD_NAME, cpus.getInt(2));
            recording.awaitAffinity(AERON_ARCHIVE_RECORDER_THREAD_NAME, cpus.getInt(0));
            recording.awaitAffinity(AERON_ARCHIVE_REPLAYER_THREAD_NAME, cpus.getInt(1));
        }
    }

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("try")
    void shouldPinSharedArchiveThreadToRawCpu()
    {
        final IntArrayList cpus = requireCpus(2);
        try (ThreadAffinityRecording recording = new ThreadAffinityRecording();
            TestMediaDriver ignore = TestMediaDriver.launch(driverContext(), null);
            Archive ignore2 = Archive.launch(archiveContext()
                .threadingMode(ArchiveThreadingMode.SHARED)
                .conductorCpuAffinity(cpus.getInt(1))))
        {
            recording.awaitAffinity(AERON_ARCHIVE_SHARED_THREAD_NAME, cpus.getInt(1));
        }
    }

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("try")
    void shouldPinDriverAndArchiveInArchivingMediaDriver()
    {
        TestMediaDriver.notSupportedOnCMediaDriver("ArchivingMediaDriver uses the Java Media Driver");
        final IntArrayList cpus = requireCpus(4);
        try (ThreadAffinityRecording recording = new ThreadAffinityRecording();
            ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(
                driverContext().driverCpusetAffinity(true).conductorCpuAffinity(3),
                archiveContext()
                    .threadingMode(ArchiveThreadingMode.DEDICATED)
                    .cpusetAffinity(true)
                    .conductorCpuAffinity(0)
                    .recorderCpuAffinity(1)
                    .replayerCpuAffinity(2)))
        {
            recording.awaitAffinity(DRIVER_SHARED_THREAD_NAME, cpus.getInt(3));
            recording.awaitAffinity(AERON_ARCHIVE_CONDUCTOR_THREAD_NAME, cpus.getInt(0));
            recording.awaitAffinity(AERON_ARCHIVE_RECORDER_THREAD_NAME, cpus.getInt(1));
            recording.awaitAffinity(AERON_ARCHIVE_REPLAYER_THREAD_NAME, cpus.getInt(2));
        }
    }

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("try")
    void shouldRejectCpuClaimedByDriverWhenWarningsAreErrors()
    {
        TestMediaDriver.notSupportedOnCMediaDriver("CPU claims of an out of process driver are not visible");
        requireCpus(1);
        try (TestMediaDriver ignore = TestMediaDriver.launch(
            driverContext().driverCpusetAffinity(true).conductorCpuAffinity(0), null))
        {
            final ConfigurationException ex = assertThrows(
                ConfigurationException.class,
                () -> Archive.launch(archiveContext()
                    .threadingMode(ArchiveThreadingMode.SHARED)
                    .cpusetAffinity(true)
                    .cpusetWarningsAsErrors(true)
                    .conductorCpuAffinity(0)).close());
            assertTrue(ex.getMessage().contains("cpuset warnings as errors"), ex.getMessage());
        }
    }

    private static IntArrayList requireCpus(final int count)
    {
        final IntArrayList cpus = ThreadAffinityRecording.effectiveCpus();
        assumeTrue(cpus.size() >= count, "requires at least " + count + " CPUs in the effective cpuset");
        return cpus;
    }

    private MediaDriver.Context driverContext()
    {
        return new MediaDriver.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true);
    }

    private Archive.Context archiveContext()
    {
        return TestContexts.localhostArchive()
            .aeronDirectoryName(aeronDirectoryName)
            .archiveDir(archiveDir.toFile())
            .deleteArchiveOnStart(true)
            .recordingEventsEnabled(false);
    }
}
