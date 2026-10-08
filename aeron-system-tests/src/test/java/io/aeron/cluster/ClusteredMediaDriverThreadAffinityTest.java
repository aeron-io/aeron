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
package io.aeron.cluster;

import io.aeron.CommonContext;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.TestContexts;
import io.aeron.test.ThreadAffinityRecording;
import io.aeron.test.cluster.StubClusteredService;
import org.agrona.collections.IntArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static io.aeron.cluster.ConsensusModule.AERON_CLUSTER_CONSENSUS_THREAD_NAME;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@EnabledOnOs(OS.LINUX)
@ExtendWith(InterruptingTestCallback.class)
@SlowTest
class ClusteredMediaDriverThreadAffinityTest
{
    // Thread names with the "new" naming, see MediaDriver, Archive and ClusteredServiceContainer thread names.
    private static final String DRIVER_SHARED_THREAD_NAME = "aeron-md-shd";
    private static final String ARCHIVE_CONDUCTOR_THREAD_NAME = "aeron-ar-cnd";
    private static final String ARCHIVE_RECORDER_THREAD_NAME = "aeron-ar-rec";
    private static final String ARCHIVE_REPLAYER_THREAD_NAME = "aeron-ar-rep";
    private static final String SERVICE_THREAD_NAME = "aeron-cl-cs-0";

    @TempDir
    private Path baseDir;

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("try")
    void shouldPinEveryComponentToItsCpusetIndex()
    {
        final IntArrayList cpus = ThreadAffinityRecording.effectiveCpus();
        assumeTrue(cpus.size() >= 6, "requires at least 6 CPUs in the effective cpuset");

        final String aeronDirectoryName = CommonContext.generateRandomDirName();
        try (ThreadAffinityRecording recording = new ThreadAffinityRecording();
            ClusteredMediaDriver ignore = ClusteredMediaDriver.launch(
                new MediaDriver.Context()
                    .aeronDirectoryName(aeronDirectoryName)
                    .threadingMode(ThreadingMode.SHARED)
                    .driverCpusetAffinity(true)
                    .conductorCpuAffinity(5)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true),
                TestContexts.localhostArchive()
                    .archiveDir(baseDir.resolve("archive").toFile())
                    .threadingMode(ArchiveThreadingMode.DEDICATED)
                    .cpusetAffinity(true)
                    .conductorCpuAffinity(1)
                    .recorderCpuAffinity(2)
                    .replayerCpuAffinity(3)
                    .recordingEventsEnabled(false)
                    .deleteArchiveOnStart(true),
                TestContexts.localhostConsensusModule()
                    .clusterDir(baseDir.resolve("cluster").toFile())
                    .cpusetAffinity(true)
                    .cpuAffinity(4)
                    .ingressChannel("aeron:udp")
                    .logChannel("aeron:ipc")
                    .replicationChannel("aeron:udp?endpoint=localhost:0")
                    .terminationHook(() -> {})
                    .deleteDirOnStart(true));
            ClusteredServiceContainer ignore2 = ClusteredServiceContainer.launch(
                new ClusteredServiceContainer.Context()
                    .aeronDirectoryName(aeronDirectoryName)
                    .clusterDir(baseDir.resolve("cluster").toFile())
                    .cpusetAffinity(true)
                    .cpuAffinity(0)
                    .clusteredService(new StubClusteredService())))
        {
            recording.awaitAffinity(SERVICE_THREAD_NAME, cpus.getInt(0));
            recording.awaitAffinity(ARCHIVE_CONDUCTOR_THREAD_NAME, cpus.getInt(1));
            recording.awaitAffinity(ARCHIVE_RECORDER_THREAD_NAME, cpus.getInt(2));
            recording.awaitAffinity(ARCHIVE_REPLAYER_THREAD_NAME, cpus.getInt(3));
            recording.awaitAffinity(AERON_CLUSTER_CONSENSUS_THREAD_NAME, cpus.getInt(4));
            recording.awaitAffinity(DRIVER_SHARED_THREAD_NAME, cpus.getInt(5));
        }
    }
}
