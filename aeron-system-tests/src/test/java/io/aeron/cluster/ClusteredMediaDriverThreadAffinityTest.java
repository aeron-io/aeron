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
import io.aeron.test.TestContexts;
import io.aeron.test.ThreadAffinityRecording;
import io.aeron.test.cluster.StubClusteredService;
import io.aeron.test.driver.TestMediaDriver;
import io.aeron.topology.AffinityRegistry;
import org.agrona.collections.IntArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static io.aeron.cluster.ConsensusModule.AERON_CLUSTER_CONSENSUS_THREAD_NAME;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@EnabledOnOs(OS.LINUX)
class ClusteredMediaDriverThreadAffinityTest
{
    // Thread names with the "new" naming, see MediaDriver and Archive *_THREAD_NAME constants.
    private static final String DRIVER_SHARED_THREAD_NAME = "aeron-md-shd";
    private static final String ARCHIVE_CONDUCTOR_THREAD_NAME = "aeron-ar-cnd";
    private static final String ARCHIVE_RECORDER_THREAD_NAME = "aeron-ar-rec";
    private static final String ARCHIVE_REPLAYER_THREAD_NAME = "aeron-ar-rep";

    @TempDir
    private Path baseDir;

    @Test
    @SuppressWarnings("try")
    void shouldPinAllComponentThreadsToDistinctCpusUsingOneRegistry()
    {
        TestMediaDriver.notSupportedOnCMediaDriver("ClusteredMediaDriver uses the Java Media Driver");
        final IntArrayList cpus = ThreadAffinityRecording.effectiveCpus();
        assumeTrue(cpus.size() >= 5, "requires at least 5 CPUs in the effective cpuset");

        final String aeronDirectoryName = CommonContext.generateRandomDirName();
        try (ThreadAffinityRecording recording = new ThreadAffinityRecording();
            ClusteredMediaDriver clusteredMediaDriver = ClusteredMediaDriver.launch(
                new MediaDriver.Context()
                    .aeronDirectoryName(aeronDirectoryName)
                    .threadingMode(ThreadingMode.SHARED)
                    .conductorCpuAffinity(5)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true),
                TestContexts.localhostArchive()
                    .archiveDir(baseDir.resolve("archive").toFile())
                    .threadingMode(ArchiveThreadingMode.DEDICATED)
                    .conductorCpuAffinity(1)
                    .recorderCpuAffinity(2)
                    .replayerCpuAffinity(3)
                    .recordingEventsEnabled(false)
                    .deleteArchiveOnStart(true),
                TestContexts.localhostConsensusModule()
                    .clusterDir(baseDir.resolve("cluster").toFile())
                    .clusterCpuAffinity(4)
                    .ingressChannel("aeron:udp")
                    .logChannel("aeron:ipc")
                    .replicationChannel("aeron:udp?endpoint=localhost:0")
                    .terminationHook(() -> {})
                    .deleteDirOnStart(true));
            // The consensus module thread is only pinned once its onStart completes, which needs a service.
            ClusteredServiceContainer ignore = ClusteredServiceContainer.launch(
                new ClusteredServiceContainer.Context()
                    .aeronDirectoryName(aeronDirectoryName)
                    .clusterDir(baseDir.resolve("cluster").toFile())
                    .clusteredService(new StubClusteredService())))
        {
            // All components are ranked together, ordered by requested CPU, across the effective cpuset.
            recording.awaitAffinity(ARCHIVE_CONDUCTOR_THREAD_NAME, cpus.getInt(0));
            recording.awaitAffinity(ARCHIVE_RECORDER_THREAD_NAME, cpus.getInt(1));
            recording.awaitAffinity(ARCHIVE_REPLAYER_THREAD_NAME, cpus.getInt(2));
            recording.awaitAffinity(AERON_CLUSTER_CONSENSUS_THREAD_NAME, cpus.getInt(3));
            recording.awaitAffinity(DRIVER_SHARED_THREAD_NAME, cpus.getInt(4));

            final AffinityRegistry registry = clusteredMediaDriver.mediaDriver().context().affinityRegistry();
            assertSame(registry, clusteredMediaDriver.archive().context().affinityRegistry());
            assertSame(registry, clusteredMediaDriver.consensusModule().context().affinityRegistry());
        }
    }
}
