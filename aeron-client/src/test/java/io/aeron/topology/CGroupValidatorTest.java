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
import io.aeron.topology.TopologyTestUtils.Pair;
import org.agrona.collections.IntArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static io.aeron.topology.TopologyTestUtils.countWarnings;
import static io.aeron.topology.TopologyTestUtils.setupDieLocality;
import static io.aeron.topology.TopologyTestUtils.setupL3Peers;
import static io.aeron.topology.TopologyTestUtils.setupSiblingThreads;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CGroupValidatorTest
{


    static Stream<Arguments> validationScenarios()
    {
        return Stream.of(
            Arguments.of(
                new int[]{0, 1, 2, 3},
                List.of(
                    new Pair(0, 1), new Pair(0, 1), new Pair(2, 3), new Pair(2, 3),
                    new Pair(4, 5), new Pair(4, 5), new Pair(6, 7), new Pair(6, 7)),
                List.of(
                    new Pair(0, 3), new Pair(0, 3), new Pair(0, 3), new Pair(0, 3),
                    new Pair(4, 7), new Pair(4, 7), new Pair(4, 7), new Pair(4, 7)),
                List.of(0, 0, 0, 0, 0, 0, 0),
                0),
            Arguments.of(
                new int[]{0, 1, 4, 6},
                List.of(
                    new Pair(0, 1), new Pair(0, 1), new Pair(2, 3), new Pair(2, 3),
                    new Pair(4, 5), new Pair(4, 5), new Pair(6, 7), new Pair(6, 7)),
                List.of(
                    new Pair(0, 7), new Pair(0, 7), new Pair(0, 7), new Pair(0, 7),
                    new Pair(0, 7), new Pair(0, 7), new Pair(0, 7), new Pair(0, 7)),
                List.of(0, 0, 0, 0, 0, 0, 0),
                2),
            Arguments.of(
                new int[]{0, 1, 4, 6},
                List.of(
                    new Pair(0, 1), new Pair(0, 1), new Pair(2, 3), new Pair(2, 3),
                    new Pair(4, 5), new Pair(4, 5), new Pair(6, 7), new Pair(6, 7)),
                List.of(
                    new Pair(0, 1), new Pair(0, 1), new Pair(2, 3), new Pair(2, 3),
                    new Pair(4, 5), new Pair(4, 5), new Pair(6, 7), new Pair(6, 7)),
                List.of(0, 0, 0, 0, 0, 0, 0),
                3),
            Arguments.of(
                new int[]{0, 1, 4, 6},
                List.of(
                    new Pair(0, 1), new Pair(0, 1), new Pair(2, 3), new Pair(2, 3),
                    new Pair(4, 5), new Pair(4, 5), new Pair(6, 7), new Pair(6, 7)),
                List.of(
                    new Pair(0, 7), new Pair(0, 7), new Pair(0, 7), new Pair(0, 7),
                    new Pair(0, 7), new Pair(0, 7), new Pair(0, 7), new Pair(0, 7)),
                List.of(1, 1, 25, 25, 30, 30, 5000, 5000),
                3),
            Arguments.of(
                new int[]{0, 1, 4, 6},
                List.of(
                    new Pair(0, 1), new Pair(0, 1), new Pair(2, 3), new Pair(2, 3),
                    new Pair(4, 5), new Pair(4, 5), new Pair(6, 7), new Pair(6, 7)),
                List.of(
                    new Pair(0, 1), new Pair(0, 1), new Pair(2, 3), new Pair(2, 3),
                    new Pair(4, 5), new Pair(4, 5), new Pair(6, 7), new Pair(6, 7)),
                List.of(1, 1, 25, 25, 30, 30, 5000, 5000),
                4));
    }

    @ParameterizedTest
    @MethodSource("validationScenarios")
    void validateReportsWarningsAndThrowsWhenConfigured(
        final int[] cpus,
        final List<Pair> siblings,
        final List<Pair> peers,
        final List<Integer> dieIds,
        final int expectedWarningCount,
        @TempDir final Path sysfsTestDir) throws IOException
    {
        setupSiblingThreads(sysfsTestDir, siblings);
        setupL3Peers(sysfsTestDir, peers);
        setupDieLocality(sysfsTestDir, dieIds);

        final IntArrayList cpuList = new IntArrayList();
        cpuList.wrap(cpus, cpus.length);

        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        new CGroupValidator(sysfsTestDir).validate(
            new Cpuset(cpuList, cpuList.toString()), false, new PrintStream(buffer));
        assertEquals(expectedWarningCount, countWarnings(buffer));

        if (0 < expectedWarningCount)
        {
            final ConfigurationException ex = assertThrows(
                ConfigurationException.class,
                () -> new CGroupValidator(sysfsTestDir).validate(
                    new Cpuset(cpuList, cpuList.toString()), true, new PrintStream(new ByteArrayOutputStream())));
            assertTrue(ex.getMessage().contains(expectedWarningCount + " warnings"));
        }
        else
        {
            assertDoesNotThrow(() -> new CGroupValidator(sysfsTestDir).validate(
                new Cpuset(cpuList, cpuList.toString()), true, new PrintStream(new ByteArrayOutputStream())));
        }
    }

    @Test
    void validateIgnoresPerCheckIOExceptionAndSkipsThatCheck(@TempDir final Path sysfsTestDir)
    {
        final IntArrayList cpuList = new IntArrayList();
        cpuList.wrap(new int[]{0, 1}, 2);
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        final CGroupValidator validator = new CGroupValidator(sysfsTestDir);
        assertThrows(ConfigurationException.class, () -> validator.validate(
            new Cpuset(cpuList, cpuList.toString()), true, new PrintStream(buffer)));
    }

}
