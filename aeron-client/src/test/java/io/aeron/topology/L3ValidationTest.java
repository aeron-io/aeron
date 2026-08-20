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

import io.aeron.topology.TopologyTestUtils.Pair;
import org.agrona.collections.IntArrayList;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static io.aeron.topology.TopologyTestUtils.setupCpuSet;
import static io.aeron.topology.TopologyTestUtils.setupL3Peers;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class L3ValidationTest
{
    public static Stream<Arguments> l3CacheTests()
    {
        final List<Pair> commonPeers = List.of(
            new Pair(0, 1), new Pair(0, 1),
            new Pair(2, 3), new Pair(2, 3),
            new Pair(4, 5), new Pair(4, 5),
            new Pair(6, 7), new Pair(6, 7));
        return Stream.of(
            Arguments.of(
                new int[]{0, 1, 2, 3, 4, 5, 6, 7},
                4,
                commonPeers,
                new int[][]{{0, 1}, {2, 3}, {4, 5}, {6, 7}}),
            Arguments.of(
                new int[]{0, 1, 4, 6},
                3,
                commonPeers,
                new int[][]{{0, 1}, {4}, {6}})
        );
    }

    @ParameterizedTest
    @MethodSource("l3CacheTests")
    void testL3CacheGrouping(
        final int[] rawCpuList,
        final int expectedGroupCount,
        final List<Pair> peers,
        final int[][] expectedGroups,
        @TempDir final Path sysfsTestDir) throws IOException
    {
        final IntArrayList cpuList = new IntArrayList();
        cpuList.wrap(rawCpuList, rawCpuList.length);
        setupL3Peers(sysfsTestDir, peers);
        final L3GroupGenerator l3GroupGenerator = new L3GroupGenerator(sysfsTestDir);
        final var groups = l3GroupGenerator.group(cpuList);
        assertEquals(expectedGroupCount, groups.size());
        for (int i = 0; i < expectedGroups.length; i++)
        {
            assertArrayEquals(expectedGroups[i], groups.get(i).toIntArray());
        }
    }

    public static Stream<Arguments> l3CacheAndCGroupsTests()
    {
        final List<Pair> commonPeers = List.of(
            new Pair(0, 1), new Pair(0, 1),
            new Pair(2, 3), new Pair(2, 3),
            new Pair(4, 5), new Pair(4, 5),
            new Pair(6, 7), new Pair(6, 7));
        return Stream.of(
            Arguments.of(
                "0-7",
                4,
                commonPeers,
                new int[][]{{0, 1}, {2, 3}, {4, 5}, {6, 7}}),
            Arguments.of(
                "0,1,4,6",
                3,
                commonPeers,
                new int[][]{{0, 1}, {4}, {6}})
        );
    }

    @ParameterizedTest
    @MethodSource("l3CacheAndCGroupsTests")
    void testL3CacheGroupingAgainstCGroups(
        final String cpuset,
        final int expectedGroupCount,
        final List<Pair> peers,
        final int[][] expectedGroups,
        @TempDir final Path testProcPath,
        @TempDir final Path testCgroupPath,
        @TempDir final Path sysfsTestDir) throws IOException
    {
        final int pid = 1234;
        setupCpuSet(testProcPath, testCgroupPath, pid, cpuset);
        final CpusetV2Reader reader = new CpusetV2Reader(testProcPath, testCgroupPath);
        final Cpuset resultCpuset = reader.readCpuSet(pid);
        setupL3Peers(sysfsTestDir, peers);
        final L3GroupGenerator l3GroupGenerator = new L3GroupGenerator(sysfsTestDir);
        final var groups = l3GroupGenerator.group(resultCpuset.cpus());
        assertEquals(expectedGroupCount, groups.size());
        for (int i = 0; i < expectedGroups.length; i++)
        {
            assertArrayEquals(expectedGroups[i], groups.get(i).toIntArray());
        }
    }
}
