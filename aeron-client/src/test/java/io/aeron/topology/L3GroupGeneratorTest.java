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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static io.aeron.topology.L3GroupGenerator.SHARED_CPU_LIST_DIRECTORY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class L3GroupGeneratorTest
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

    public record Pair(int first, int second)
    {
    }

    private void setupL3Peers(
        final Path sysfsPath,
        final List<Pair> peers) throws IOException
    {
        for (int cpu = 0; cpu < peers.size(); cpu++)
        {
            final Pair peer = peers.get(cpu);
            final Path sharedCpuPath = sysfsPath.resolve("cpu%d".formatted(cpu)).resolve(SHARED_CPU_LIST_DIRECTORY);
            Files.createDirectories(sharedCpuPath.getParent());
            Files.writeString(sharedCpuPath, "%d,%d".formatted(peer.first, peer.second));
        }
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
}
