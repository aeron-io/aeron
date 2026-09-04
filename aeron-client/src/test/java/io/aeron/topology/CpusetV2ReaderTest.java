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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static io.aeron.topology.TopologyTestUtils.setupCpuSet;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class CpusetV2ReaderTest
{

    public static Stream<Arguments> cpusetList()
    {
        return Stream.of(
            Arguments.of("0", new int[] {0}),
            Arguments.of("0-3", new int[] {0, 1, 2, 3}),
            Arguments.of("0-3,5", new int[] {0, 1, 2, 3, 5}),
            Arguments.of("0-3,5-7", new int[] {0, 1, 2, 3, 5, 6, 7})
        );
    }

    @ParameterizedTest
    @MethodSource("cpusetList")
    void readCpuSet(
        final String cpuset,
        final int[] expectedCpus,
        @TempDir final Path testProcPath,
        @TempDir final Path testCgroupPath) throws IOException
    {
        final int pid = 1234;
        setupCpuSet(testProcPath, testCgroupPath, pid, cpuset);
        final CpusetV2Reader reader = new CpusetV2Reader(testProcPath, testCgroupPath);
        final Cpuset resultCpuset = reader.readCpuSet(pid);
        assertArrayEquals(expectedCpus, resultCpuset.cpus().toIntArray());
    }

    @Test
    void readCpuSetWalksUpCgroupHierarchy(
        @TempDir final Path testProcPath,
        @TempDir final Path testCgroupPath) throws IOException
    {
        final int pid = 1234;
        final Path procCgroupFilePath = testProcPath.resolve(pid + "/cgroup");
        Files.createDirectories(procCgroupFilePath.getParent());
        Files.writeString(procCgroupFilePath, "0::/user.slice/app.scope");

        final Path parentCpusetFile = testCgroupPath.resolve("user.slice/cpuset.cpus.effective");
        Files.createDirectories(parentCpusetFile.getParent());
        Files.writeString(parentCpusetFile, "0-3");

        final CpusetV2Reader reader = new CpusetV2Reader(testProcPath, testCgroupPath);
        final Cpuset resultCpuset = reader.readCpuSet(pid);
        assertArrayEquals(new int[]{0, 1, 2, 3}, resultCpuset.cpus().toIntArray());
    }

}
