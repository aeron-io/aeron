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

import io.aeron.test.CapturingPrintStream;
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

import static io.aeron.topology.TopologyTestUtils.countWarnings;
import static io.aeron.topology.TopologyTestUtils.setupCpuSet;
import static io.aeron.topology.TopologyTestUtils.setupL3Peers;
import static org.junit.jupiter.api.Assertions.assertEquals;

class L3ValidationTest
{
    @ParameterizedTest
    @MethodSource("l3CacheTests")
    void testL3CacheGrouping(
        final int[] rawCpuList,
        final List<Pair> peers,
        final long expectedWarningCount,
        @TempDir final Path sysfsTestDir) throws IOException
    {
        final IntArrayList cpuList = new IntArrayList();
        cpuList.wrap(rawCpuList, rawCpuList.length);
        setupL3Peers(sysfsTestDir, peers);
        final L3TopologyValidator l3TopologyValidator = new L3TopologyValidator(sysfsTestDir);

        final CapturingPrintStream out = new CapturingPrintStream();
        final int actualWarningCount = l3TopologyValidator.validate(
            new Cpuset(cpuList, cpuList.toString()), out.resetAndGetPrintStream());
        assertEquals(expectedWarningCount, actualWarningCount);
        assertEquals(expectedWarningCount, countWarnings(out.flushAndGetContent()));
    }

    @ParameterizedTest
    @MethodSource("l3CacheAndCGroupsTests")
    void testL3CacheGroupingAgainstCGroups(
        final String cpuset,
        final List<Pair> peers,
        final long expectedWarningCount,
        @TempDir final Path testProcPath,
        @TempDir final Path testCgroupPath,
        @TempDir final Path sysfsTestDir) throws IOException
    {
        final int pid = 1234;
        setupCpuSet(testProcPath, testCgroupPath, pid, cpuset);
        final CpusetV2Reader reader = new CpusetV2Reader(testProcPath, testCgroupPath);
        final Cpuset resultCpuset = reader.readCpuSet(pid);
        setupL3Peers(sysfsTestDir, peers);

        final L3TopologyValidator l3TopologyValidator = new L3TopologyValidator(sysfsTestDir);

        final CapturingPrintStream out = new CapturingPrintStream();
        final int actualWarningCount = l3TopologyValidator.validate(resultCpuset, out.resetAndGetPrintStream());
        assertEquals(expectedWarningCount, actualWarningCount);
        assertEquals(expectedWarningCount, countWarnings(out.flushAndGetContent()));
    }

    private static Stream<Arguments> l3CacheTests()
    {
        final List<Pair> commonPeers = List.of(
            new Pair(0, 1), new Pair(0, 1),
            new Pair(2, 3), new Pair(2, 3),
            new Pair(4, 5), new Pair(4, 5),
            new Pair(6, 7), new Pair(6, 7));
        return Stream.of(
            Arguments.of(
                new int[]{0, 1, 2, 3, 4, 5, 6, 7},
                commonPeers,
                1L),
            Arguments.of(
                new int[]{0, 1, 4, 6},
                commonPeers,
                1L),
            Arguments.of(
                new int[]{0, 1},
                commonPeers,
                0L)
        );
    }

    private static Stream<Arguments> l3CacheAndCGroupsTests()
    {
        final List<Pair> commonPeers = List.of(
            new Pair(0, 1), new Pair(0, 1),
            new Pair(2, 3), new Pair(2, 3),
            new Pair(4, 5), new Pair(4, 5),
            new Pair(6, 7), new Pair(6, 7));
        return Stream.of(
            Arguments.of("0-7", commonPeers, 1L),
            Arguments.of("0,1,4,6", commonPeers, 1L),
            Arguments.of(
                "0,1,4,6",
                List.of(
                    new Pair(0, 3), new Pair(0, 3), new Pair(0, 3), new Pair(0, 3),
                    new Pair(4, 7), new Pair(4, 7), new Pair(4, 7), new Pair(4, 7)
                ),
                1L
            ),
            Arguments.of(
                "0,1,4,6",
                List.of(
                    new Pair(0, 7), new Pair(0, 7), new Pair(0, 7), new Pair(0, 7),
                    new Pair(0, 7), new Pair(0, 7), new Pair(0, 7), new Pair(0, 7)
                ),
                0L
            )
        );
    }
}
