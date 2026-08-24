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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static io.aeron.topology.TopologyTestUtils.setupCpuSet;
import static io.aeron.topology.TopologyTestUtils.setupSiblingThreads;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AlignmentValidationTest
{
    public static Stream<Arguments> missingGroupWithCGroupTestSetup()
    {
        final List<Pair> commonSiblings = List.of(
            new Pair(0, 1), new Pair(0, 1),
            new Pair(2, 3), new Pair(2, 3),
            new Pair(4, 5), new Pair(4, 5),
            new Pair(6, 7), new Pair(6, 7));
        return Stream.of(
            Arguments.of(
                "0-7",
                commonSiblings,
                new int[]{}),
            Arguments.of(
                "0,1,4,6",
                commonSiblings,
                new int[]{5, 7}),
            Arguments.of(
                "0-3,5-6",
                commonSiblings,
                new int[]{4, 7}),
            Arguments.of(
                "5-10",
                List.of(
                    new Pair(0, 1), new Pair(0, 1), new Pair(2, 3), new Pair(2, 3),
                    new Pair(4, 5), new Pair(4, 5), new Pair(6, 7), new Pair(6, 7),
                    new Pair(8, 9), new Pair(8, 9), new Pair(10, 11), new Pair(10, 11),
                    new Pair(12, 13), new Pair(12, 13), new Pair(14, 15), new Pair(14, 15)
                ),
                new int[]{4, 11}
            )
        );
    }

    @ParameterizedTest
    @MethodSource("missingGroupWithCGroupTestSetup")
    void testMissingSiblingCheckWithCGroup(
        final String cpuset,
        final List<Pair> siblings,
        final int[] expectedMissingThreads,
        @TempDir final Path testProcPath,
        @TempDir final Path testCgroupPath,
        @TempDir final Path sysfsTestDir) throws IOException
    {
        final int pid = 1234;
        setupCpuSet(testProcPath, testCgroupPath, pid, cpuset);
        final CpusetV2Reader reader = new CpusetV2Reader(testProcPath, testCgroupPath);
        final Cpuset resultCpuset = reader.readCpuSet(pid);
        setupSiblingThreads(sysfsTestDir, siblings);
        final ThreadAlignmentChecker threadAlignmentChecker = new ThreadAlignmentChecker(sysfsTestDir);
        final var missingThreads = threadAlignmentChecker.identifyMissingThreads(resultCpuset.cpus()).toIntArray();
        final var sortedMissingThreads = Arrays.stream(missingThreads).sorted().toArray();
        assertEquals(expectedMissingThreads.length, missingThreads.length);
        assertArrayEquals(expectedMissingThreads, sortedMissingThreads);

    }
}
