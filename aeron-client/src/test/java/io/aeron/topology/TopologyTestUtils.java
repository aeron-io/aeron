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

package io.aeron.topology;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.aeron.topology.DieLocalityValidator.DIE_ID_DIRECTORY;
import static io.aeron.topology.L3TopologyValidator.SHARED_CPU_LIST_DIRECTORY;
import static io.aeron.topology.ThreadAlignmentValidator.THREAD_SIBLING_LIST;

class TopologyTestUtils
{
    static void setupSiblingThreads(final Path sysfsPath, final List<Pair> siblings) throws IOException
    {
        for (int cpu = 0; cpu < siblings.size(); cpu++)
        {
            final Pair siblingPair = siblings.get(cpu);
            final Path threadSiblingListPath = sysfsPath.resolve("cpu%d".formatted(cpu)).resolve(THREAD_SIBLING_LIST);
            Files.createDirectories(threadSiblingListPath.getParent());
            Files.writeString(threadSiblingListPath, "%d-%d".formatted(siblingPair.first, siblingPair.second));
        }
    }

    static void setupL3Peers(final Path sysfsPath, final List<Pair> peers) throws IOException
    {
        for (int cpu = 0; cpu < peers.size(); cpu++)
        {
            final Pair peer = peers.get(cpu);
            final Path sharedCpuPath = sysfsPath.resolve("cpu%d".formatted(cpu)).resolve(SHARED_CPU_LIST_DIRECTORY);
            Files.createDirectories(sharedCpuPath.getParent());
            Files.writeString(sharedCpuPath, "%d-%d".formatted(peer.first, peer.second));
        }
    }

    static void setupDieLocality(final Path sysfsPath, final List<Integer> dieIds) throws IOException
    {
        for (int cpu = 0; cpu < dieIds.size(); cpu++)
        {
            final int dieId = dieIds.get(cpu);
            final Path sharedCpuPath = sysfsPath.resolve("cpu%d".formatted(cpu)).resolve(DIE_ID_DIRECTORY);
            Files.createDirectories(sharedCpuPath.getParent());
            Files.writeString(sharedCpuPath, Integer.toString(dieId));
        }
    }

    static void setupCpuSet(
        final Path testProcPath,
        final Path testCgroupPath,
        final int pid,
        final String cpuset) throws IOException
    {
        final Path procCgroupFilePath = testProcPath.resolve(pid + "/cgroup");
        Files.createDirectories(procCgroupFilePath.getParent());
        Files.writeString(procCgroupFilePath, "0::/user.slice");

        final Path effectiveCgroupFilePath = testCgroupPath.resolve("user.slice/cpuset.cpus.effective");
        Files.createDirectories(effectiveCgroupFilePath.getParent());
        Files.writeString(effectiveCgroupFilePath, cpuset);
    }

    static long countWarnings(final ByteArrayOutputStream byteStream)
    {
        final String output = byteStream.toString();
        if (output.isEmpty())
        {
            return 0;
        }
        return output.lines().count();
    }

    public record Pair(int first, int second)
    {
    }
}
