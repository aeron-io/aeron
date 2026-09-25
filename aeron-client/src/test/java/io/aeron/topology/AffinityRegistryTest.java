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

import io.aeron.exceptions.ConfigurationException;
import io.aeron.test.CapturingPrintStream;
import io.aeron.topology.TopologyTestUtils.Pair;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.aeron.topology.TopologyTestUtils.countWarnings;
import static io.aeron.topology.TopologyTestUtils.setupCpuSet;
import static io.aeron.topology.TopologyTestUtils.setupDieLocality;
import static io.aeron.topology.TopologyTestUtils.setupL3Peers;
import static io.aeron.topology.TopologyTestUtils.setupSiblingThreads;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AffinityRegistryTest
{
    @ParameterizedTest
    @MethodSource("validationScenarios")
    void validateReportsWarningsAndThrowsWhenConfigured(
        final String cpuset,
        final Map<String, Integer> affinities,
        final Map<String, Integer> expectedRemappedAffinities,
        final List<Pair> siblings,
        final List<Pair> peers,
        final List<Integer> dieIds,
        final int expectedWarningCount,
        @TempDir final Path sysfsTestDir,
        @TempDir final Path testProcPath,
        @TempDir final Path testCgroupPath) throws IOException
    {
        assertEquals(affinities.keySet(), expectedRemappedAffinities.keySet());
        setupSiblingThreads(sysfsTestDir, siblings);
        setupL3Peers(sysfsTestDir, peers);
        setupDieLocality(sysfsTestDir, dieIds);
        setupCpuSet(testProcPath, testCgroupPath, 0, cpuset);


        final CapturingPrintStream out = new CapturingPrintStream();
        final CpusetV2Reader reader = new CpusetV2Reader(testProcPath, testCgroupPath);
        final AffinityRegistry affinityRegistry = new AffinityRegistry(sysfsTestDir, reader);
        for (final var affinityEntry : affinities.entrySet())
        {
            affinityRegistry.addAffinity(affinityEntry.getKey(), affinityEntry.getValue());
        }
        affinityRegistry.conclude();
        for (final var expectedAffinityEntry : expectedRemappedAffinities.entrySet())
        {
            assertEquals(
                expectedAffinityEntry.getValue(), affinityRegistry.mappedAffinityValue(expectedAffinityEntry.getKey()));
        }
        affinityRegistry.validate(false, out.resetAndGetPrintStream());
        final String output = out.flushAndGetContent();
        assertEquals(expectedWarningCount, countWarnings(output), output);

        if (0 < expectedWarningCount)
        {
            final ConfigurationException ex = assertThrows(
                ConfigurationException.class,
                () -> affinityRegistry.validate(true, new PrintStream(new ByteArrayOutputStream())));
            assertTrue(ex.getMessage().contains(expectedWarningCount + " warnings"));
        }
        else
        {
            assertDoesNotThrow(() -> affinityRegistry.validate(true, new PrintStream(new ByteArrayOutputStream())));
        }
    }

    private static Stream<Arguments> validationScenarios()
    {
        return Stream.of(
            Arguments.of(
                "3-5",
                Map.of("aaa", 1, "bbb", 2),
                Map.of("aaa", 3, "bbb", 4),
                List.of(
                    new Pair(0, 1), new Pair(0, 1), new Pair(2, 3), new Pair(2, 3),
                    new Pair(4, 5), new Pair(4, 5), new Pair(6, 7), new Pair(6, 7)),
                List.of(
                    new Pair(0, 3), new Pair(0, 3), new Pair(0, 3), new Pair(0, 3),
                    new Pair(4, 7), new Pair(4, 7), new Pair(4, 7), new Pair(4, 7)),
                List.of(0, 0, 0, 0, 0, 0, 0),
                1),
            Arguments.of(
                "3-5",
                Map.of("aaa", 1, "bbb", 2, "ccc", 3),
                Map.of("aaa", 3, "bbb", 4, "ccc", 5),
                List.of(
                    new Pair(0, 1), new Pair(0, 1), new Pair(2, 3), new Pair(2, 3),
                    new Pair(4, 5), new Pair(4, 5), new Pair(6, 7), new Pair(6, 7)),
                List.of(
                    new Pair(0, 7), new Pair(0, 7), new Pair(0, 7), new Pair(0, 7),
                    new Pair(0, 7), new Pair(0, 7), new Pair(0, 7), new Pair(0, 7)),
                List.of(0, 0, 0, 0, 0, 0, 0),
                0));
    }
}

