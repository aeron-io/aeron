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
import static io.aeron.topology.TopologyTestUtils.setupDieLocality;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DieLocalityValidationTest
{
    @ParameterizedTest
    @MethodSource("dieLocalityTestSetup")
    void testDieLocalityGrouping(
        final int[] rawCpuList,
        final List<Integer> dieIds,
        final long expectedWarningCount,
        @TempDir final Path sysfsTestDir) throws IOException
    {
        final IntArrayList cpuList = new IntArrayList();
        cpuList.wrap(rawCpuList, rawCpuList.length);
        setupDieLocality(sysfsTestDir, dieIds);

        final CapturingPrintStream out = new CapturingPrintStream();

        final DieLocalityValidator dieLocalityValidator = new DieLocalityValidator(sysfsTestDir);
        final int actualWarningCount = dieLocalityValidator.validate(
            new Cpuset(cpuList, cpuList.toString()), out.resetAndGetPrintStream());
        assertEquals(expectedWarningCount, actualWarningCount);
        assertEquals(expectedWarningCount, countWarnings(out.flushAndGetContent()));
    }

    @ParameterizedTest
    @MethodSource("dieLocalityAndCGroupTests")
    void testDieLocalityAgainstCGroups(
        final String cpuset,
        final List<Integer> dieIds,
        final long expectedWarningCount,
        @TempDir final Path testProcPath,
        @TempDir final Path testCgroupPath,
        @TempDir final Path sysfsTestDir) throws IOException
    {
        final int pid = 1234;
        setupCpuSet(testProcPath, testCgroupPath, pid, cpuset);
        final CpusetV2Reader reader = new CpusetV2Reader(testProcPath, testCgroupPath);
        final Cpuset resultCpuset = reader.readCpuSet(pid);
        setupDieLocality(sysfsTestDir, dieIds);

        final CapturingPrintStream out = new CapturingPrintStream();
        final DieLocalityValidator dieLocalityValidator = new DieLocalityValidator(sysfsTestDir);
        final int actualWarningCount = dieLocalityValidator.validate(resultCpuset, out.resetAndGetPrintStream());
        assertEquals(expectedWarningCount, actualWarningCount);
        assertEquals(expectedWarningCount, countWarnings(out.flushAndGetContent()));
    }

    private static Stream<Arguments> dieLocalityAndCGroupTests()
    {
        return Stream.of(
            Arguments.of(
                "0-7",
                List.of(1, 1, 25, 25, 30, 30, 5000, 5000),
                1L),
            Arguments.of(
                "0,1,4,6",
                List.of(1, 1, 25, 25, 30, 30, 5000, 5000),
                1L),
            Arguments.of(
                "0-1,2-3",
                List.of(1, 1, 1, 1, 100, 100, 100, 100),
                0L)
        );
    }

    private static Stream<Arguments> dieLocalityTestSetup()
    {
        return Stream.of(
            Arguments.of(
                new int[]{0, 1, 2, 3, 4, 5, 6, 7},
                List.of(1, 1, 25, 25, 30, 30, 5000, 5000),
                1L),
            Arguments.of(
                new int[]{0, 1, 4, 6},
                List.of(1, 1, 25, 25, 30, 30, 5000, 5000),
                1L),
            Arguments.of(
                new int[]{0, 1, 2, 3},
                List.of(1, 1, 1, 1, 30, 30, 5000, 5000),
                0L)
        );
    }
}
