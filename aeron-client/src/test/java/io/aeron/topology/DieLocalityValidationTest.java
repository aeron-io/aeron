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

import org.agrona.collections.IntArrayList;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static io.aeron.topology.TopologyTestUtils.setupCpuSet;
import static io.aeron.topology.TopologyTestUtils.setupDieLocality;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DieLocalityValidationTest
{
    public static Stream<Arguments> dieLocalityTestSetup()
    {
        return Stream.of(
            Arguments.of(
                new int[]{0, 1, 2, 3, 4, 5, 6, 7},
                4,
                List.of(1, 1, 25, 25, 30, 30, 5000, 5000),
                new int[][]{{0, 1}, {2, 3}, {4, 5}, {6, 7}}),
            Arguments.of(
                new int[]{0, 1, 4, 6},
                3,
                List.of(1, 1, 25, 25, 30, 30, 5000, 5000),
                new int[][]{{0, 1}, {4}, {6}})
        );
    }

    @ParameterizedTest
    @MethodSource("dieLocalityTestSetup")
    void testDieLocalityGrouping(
        final int[] rawCpuList,
        final int expectedGroupCount,
        final List<Integer> dieIds,
        final int[][] expectedGroups,
        @TempDir final Path sysfsTestDir) throws IOException
    {
        final IntArrayList cpuList = new IntArrayList();
        cpuList.wrap(rawCpuList, rawCpuList.length);
        setupDieLocality(sysfsTestDir, dieIds);
        final DieLocalityGroupGenerator dieLocalityGroupGenerator = new DieLocalityGroupGenerator(sysfsTestDir);
        final var groups = dieLocalityGroupGenerator.group(cpuList);
        var sortedGroups = groups.stream().sorted(Comparator.comparing(a -> a.get(0))).toList();
        assertEquals(expectedGroupCount, groups.size());
        for (int i = 0; i < expectedGroups.length; i++)
        {
            assertArrayEquals(expectedGroups[i], sortedGroups.get(i).toIntArray());
        }
    }

    public static Stream<Arguments> dieLocalityAndCGroupTests()
    {
        return Stream.of(
            Arguments.of(
                "0-7",
                4,
                List.of(1, 1, 25, 25, 30, 30, 5000, 5000),
                new int[][]{{0, 1}, {2, 3}, {4, 5}, {6, 7}}),
            Arguments.of(
                "0,1,4,6",
                3,
                List.of(1, 1, 25, 25, 30, 30, 5000, 5000),
                new int[][]{{0, 1}, {4}, {6}})
        );
    }

    @ParameterizedTest
    @MethodSource("dieLocalityAndCGroupTests")
    void testDieLocalityAgainstCGroups(
        final String cpuset,
        final int expectedGroupCount,
        final List<Integer> dieIds,
        final int[][] expectedGroups,
        @TempDir final Path testProcPath,
        @TempDir final Path testCgroupPath,
        @TempDir final Path sysfsTestDir) throws IOException
    {
        final int pid = 1234;
        setupCpuSet(testProcPath, testCgroupPath, pid, cpuset);
        final CpusetV2Reader reader = new CpusetV2Reader(testProcPath, testCgroupPath);
        final Cpuset resultCpuset = reader.readCpuSet(pid);
        setupDieLocality(sysfsTestDir, dieIds);
        final DieLocalityGroupGenerator dieLocalityGroupGenerator = new DieLocalityGroupGenerator(sysfsTestDir);
        final var groups = dieLocalityGroupGenerator.group(resultCpuset.cpus());
        var sortedGroups = groups.stream().sorted(Comparator.comparing(a -> a.get(0))).toList();
        assertEquals(expectedGroupCount, groups.size());
        for (int i = 0; i < expectedGroups.length; i++)
        {
            assertArrayEquals(expectedGroups[i], sortedGroups.get(i).toIntArray());
        }
    }
}
