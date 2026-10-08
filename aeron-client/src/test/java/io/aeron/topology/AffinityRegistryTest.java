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

import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.test.CapturingPrintStream;
import io.aeron.topology.AffinityRegistry.CoreClaim;
import io.aeron.topology.TopologyTestUtils.Pair;
import org.agrona.CloseHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.aeron.topology.TopologyTestUtils.countWarnings;
import static io.aeron.topology.TopologyTestUtils.setupCpuSet;
import static io.aeron.topology.TopologyTestUtils.setupDieLocality;
import static io.aeron.topology.TopologyTestUtils.setupL3Peers;
import static io.aeron.topology.TopologyTestUtils.setupSiblingThreads;
import static org.agrona.concurrent.affinity.ThreadAffinity.NO_AFFINITY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("try")
class AffinityRegistryTest
{
    private static final List<Pair> ALIGNED_SIBLINGS = List.of(
        new Pair(0, 1), new Pair(0, 1), new Pair(2, 3), new Pair(2, 3),
        new Pair(4, 5), new Pair(4, 5), new Pair(6, 7), new Pair(6, 7));
    private static final List<Pair> SPLIT_L3 = List.of(
        new Pair(0, 3), new Pair(0, 3), new Pair(0, 3), new Pair(0, 3),
        new Pair(4, 7), new Pair(4, 7), new Pair(4, 7), new Pair(4, 7));
    private static final List<Pair> SHARED_L3 = List.of(
        new Pair(0, 7), new Pair(0, 7), new Pair(0, 7), new Pair(0, 7),
        new Pair(0, 7), new Pair(0, 7), new Pair(0, 7), new Pair(0, 7));
    private static final List<Integer> SINGLE_DIE = List.of(0, 0, 0, 0, 0, 0, 0, 0);

    @TempDir
    private Path sysfsTestDir;
    @TempDir
    private Path testProcPath;
    @TempDir
    private Path testCgroupPath;

    private final CapturingPrintStream out = new CapturingPrintStream();
    private final List<AffinityRegistry> registries = new ArrayList<>();

    @BeforeEach
    void setUp() throws IOException
    {
        setupSiblingThreads(sysfsTestDir, ALIGNED_SIBLINGS);
        setupDieLocality(sysfsTestDir, SINGLE_DIE);
        setupCpuSet(testProcPath, testCgroupPath, 0, "0-7");
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(registries);
        assertEquals(List.of(), AffinityRegistry.claimedCpus());
    }

    @ParameterizedTest
    @MethodSource("cpusetScenarios")
    void shouldMapIndicesOntoCpusetAndReportWarnings(
        final String cpuset,
        final Map<String, Integer> indices,
        final Map<String, Integer> expectedCpus,
        final List<Pair> l3Peers,
        final int expectedWarningCount) throws IOException
    {
        setupL3Peers(sysfsTestDir, l3Peers);
        setupCpuSet(testProcPath, testCgroupPath, 0, cpuset);

        try (AffinityRegistry registry = newRegistry(indices, true, false, out.resetAndGetPrintStream()))
        {
            registry.conclude();

            expectedCpus.forEach((name, cpu) -> assertEquals(cpu, registry.mappedAffinityValue(name), name));
            final String output = out.flushAndGetContent();
            assertEquals(expectedWarningCount, countWarnings(output), output);
        }

        if (0 < expectedWarningCount)
        {
            final ConfigurationException ex = assertThrows(
                ConfigurationException.class,
                () -> newRegistry(indices, true, true).conclude());
            assertTrue(ex.getMessage().contains(expectedWarningCount + " warnings"), ex.getMessage());
        }
        else
        {
            assertDoesNotThrow(() -> newRegistry(indices, true, true).conclude());
        }
    }

    @Test
    void shouldUseRawCpuIdsWhenCpusetAffinityDisabled() throws IOException
    {
        setupL3Peers(sysfsTestDir, SHARED_L3);

        final AffinityRegistry registry = newRegistry(Map.of("aaa", 6, "bbb", 7, "ccc", NO_AFFINITY), false, true);
        registry.conclude();

        assertEquals(6, registry.mappedAffinityValue("aaa"));
        assertEquals(7, registry.mappedAffinityValue("bbb"));
        assertEquals(NO_AFFINITY, registry.mappedAffinityValue("ccc"));
    }

    @Test
    void shouldRejectIndexOutsideCpuset() throws IOException
    {
        setupL3Peers(sysfsTestDir, SHARED_L3);
        setupCpuSet(testProcPath, testCgroupPath, 0, "2-5");

        final AffinityRegistry registry = newRegistry(Map.of("aaa", 4), true, false);
        final ConfigurationException ex = assertThrows(
            ConfigurationException.class, () -> registry.conclude());

        assertTrue(ex.getMessage().contains("aaa affinity 4 must be less than cpuset count 4"), ex.getMessage());
    }

    @Test
    void shouldRejectRawCpuOutsideCpusetWhenCpusetAffinityDisabled() throws IOException
    {
        setupL3Peers(sysfsTestDir, SHARED_L3);
        setupCpuSet(testProcPath, testCgroupPath, 0, "2-5");

        final AffinityRegistry registry = newRegistry(Map.of("aaa", 6), false, false);
        final ConfigurationException ex = assertThrows(
            ConfigurationException.class, () -> registry.conclude());

        assertTrue(ex.getMessage().contains("aaa affinity 6 is not in cpuset: 2-5"), ex.getMessage());
        assertDoesNotThrow(() -> newRegistry(Map.of("aaa", 3), false, false)
            .conclude());
    }

    @Test
    void shouldWarnWhenThreadsOfComponentShareAffinity() throws IOException
    {
        setupL3Peers(sysfsTestDir, SHARED_L3);

        final AffinityRegistry registry = newRegistry(
            Map.of("aaa", 2, "bbb", 2), false, false, out.resetAndGetPrintStream());
        registry.conclude();

        final String output = out.flushAndGetContent();
        assertEquals(1, countWarnings(output), output);
        assertTrue(output.contains("sharing cpu=2"), output);
        assertThrows(
            ConfigurationException.class,
            () -> newRegistry(Map.of("aaa", 2, "bbb", 2), false, true).conclude());
    }

    @Test
    void shouldWarnWhenCpuClaimedByAnotherComponent() throws IOException
    {
        setupL3Peers(sysfsTestDir, SHARED_L3);

        try (AffinityRegistry first = newRegistry(Map.of("aaa", 2), false, true);
            AffinityRegistry second = newRegistry(
                Map.of("bbb", 2, "ccc", 3), false, false, out.resetAndGetPrintStream()))
        {
            first.conclude();
            second.conclude();

            final String output = out.flushAndGetContent();
            assertEquals(1, countWarnings(output), output);
            assertTrue(output.contains("bbb and aaa are sharing cpu=2"), output);
        }

        assertDoesNotThrow(
            () -> newRegistry(Map.of("bbb", 2), false, true).conclude(),
            "claims are released when the publishing component closes");
    }

    @Test
    void shouldNotTreatOwnPublishedClaimsAsConflicts() throws IOException
    {
        setupL3Peers(sysfsTestDir, SHARED_L3);

        try (AffinityRegistry first = newRegistry(
                Map.of("aaa", 2, "bbb", 3), false, true, out.resetAndGetPrintStream());
            AffinityRegistry second = newRegistry(Map.of("ccc", 2), false, false, out.resetAndGetPrintStream()))
        {
            first.conclude();
            assertEquals(0, countWarnings(out.flushAndGetContent()));

            out.resetAndGetPrintStream();
            second.conclude();
            final String output = out.flushAndGetContent();
            assertEquals(1, countWarnings(output), output);
            assertTrue(output.contains("ccc and aaa are sharing cpu=2"), output);
            assertEquals(3, AffinityRegistry.claimedCpus().size());
        }
    }

    @Test
    void shouldReleaseClaimsWhenValidationAgainstOtherComponentFails() throws IOException
    {
        setupL3Peers(sysfsTestDir, SHARED_L3);

        try (AffinityRegistry first = newRegistry(Map.of("aaa", 2), false, true))
        {
            first.conclude();
            assertThrows(
                ConfigurationException.class,
                () -> newRegistry(Map.of("bbb", 2), false, true).conclude());

            assertEquals(List.of(new CoreClaim("aaa", 2)), AffinityRegistry.claimedCpus());
        }
    }

    @Test
    void shouldValidateLocalityAcrossClaimedCpus() throws IOException
    {
        setupL3Peers(sysfsTestDir, SPLIT_L3);

        try (AffinityRegistry first = newRegistry(Map.of("aaa", 1), false, true);
            AffinityRegistry second = newRegistry(Map.of("bbb", 5), false, false, out.resetAndGetPrintStream()))
        {
            first.conclude();
            second.conclude();

            final String output = out.flushAndGetContent();
            assertEquals(1, countWarnings(output), output);
            assertTrue(output.contains("multiple L3 cache domains"), output);
        }
    }

    @Test
    void shouldClaimPinnedThreadsUntilClosed() throws IOException
    {
        setupL3Peers(sysfsTestDir, SHARED_L3);
        setupCpuSet(testProcPath, testCgroupPath, 0, "4-7");

        try (AffinityRegistry registry = newRegistry(Map.of("aaa", 1, "bbb", NO_AFFINITY), true, true))
        {
            registry.conclude();
            assertEquals(List.of(new CoreClaim("aaa", 5)), AffinityRegistry.claimedCpus());
        }

        assertEquals(List.of(), AffinityRegistry.claimedCpus());
    }

    @Test
    void shouldNotClaimWhenWarningsAreErrors() throws IOException
    {
        setupL3Peers(sysfsTestDir, SHARED_L3);

        final AffinityRegistry registry = newRegistry(Map.of("aaa", 2, "bbb", 2), false, true);
        assertThrows(
            ConfigurationException.class,
            () -> registry.conclude());

        assertEquals(List.of(), AffinityRegistry.claimedCpus());
    }

    @Test
    void shouldNotReadCpusetOrTopologyWhenNoThreadIsPinned(
        @TempDir final Path emptySysfs, @TempDir final Path emptyProc, @TempDir final Path emptyCgroup)
    {
        final AffinityRegistry registry = new AffinityRegistry(
            emptySysfs, new CpusetV2Reader(emptyProc, emptyCgroup), true, false, true, discard())
            .addAffinity("aaa", NO_AFFINITY)
            .addAffinity("bbb", NO_AFFINITY);

        assertDoesNotThrow(() -> registry.conclude());
        assertEquals(NO_AFFINITY, registry.mappedAffinityValue("aaa"));
        assertEquals(NO_AFFINITY, registry.mappedAffinityValue("bbb"));
    }

    @Test
    void shouldRejectPinnedThreadWhenThreadAffinityIsNotSupported(
        @TempDir final Path emptySysfs, @TempDir final Path emptyProc, @TempDir final Path emptyCgroup)
    {
        final AffinityRegistry registry = new AffinityRegistry(
            emptySysfs, new CpusetV2Reader(emptyProc, emptyCgroup), false, false, false, discard())
            .addAffinity("aaa", 2);

        final ConfigurationException ex = assertThrows(
            ConfigurationException.class, () -> registry.conclude());
        assertTrue(ex.getMessage().contains("only supported on Linux"), ex.getMessage());
        assertEquals(List.of(), AffinityRegistry.claimedCpus());
    }

    @Test
    void shouldAllowUnpinnedThreadsWhenThreadAffinityIsNotSupported(
        @TempDir final Path emptySysfs, @TempDir final Path emptyProc, @TempDir final Path emptyCgroup)
    {
        final AffinityRegistry registry = new AffinityRegistry(
            emptySysfs, new CpusetV2Reader(emptyProc, emptyCgroup), false, true, true, discard())
            .addAffinity("aaa", NO_AFFINITY);

        assertDoesNotThrow(() -> registry.conclude());
        assertEquals(NO_AFFINITY, registry.mappedAffinityValue("aaa"));
    }

    @Test
    void shouldGuardLifecycle()
    {
        final AffinityRegistry registry = newRegistry(Map.of(), false, false);
        assertThrows(IllegalStateException.class, () -> registry.mappedAffinityValue("aaa"));

        registry.conclude();

        assertThrows(IllegalArgumentException.class, () -> registry.mappedAffinityValue("aaa"));
        assertThrows(IllegalStateException.class, () -> registry.addAffinity("aaa", 1));
        assertThrows(ConcurrentConcludeException.class, registry::conclude);
    }

    private AffinityRegistry newRegistry(
        final Map<String, Integer> affinities, final boolean cpusetAffinity, final boolean warningsAsErrors)
    {
        return newRegistry(affinities, cpusetAffinity, warningsAsErrors, discard());
    }

    private AffinityRegistry newRegistry(
        final Map<String, Integer> affinities,
        final boolean cpusetAffinity,
        final boolean warningsAsErrors,
        final PrintStream warningStream)
    {
        final AffinityRegistry registry = new AffinityRegistry(
            sysfsTestDir,
            new CpusetV2Reader(testProcPath, testCgroupPath),
            true,
            cpusetAffinity,
            warningsAsErrors,
            warningStream);
        new LinkedHashMap<>(affinities).forEach(registry::addAffinity);
        registries.add(registry);
        return registry;
    }

    private static PrintStream discard()
    {
        return new PrintStream(new ByteArrayOutputStream());
    }

    private static Stream<Arguments> cpusetScenarios()
    {
        return Stream.of(
            // cpuset spans two L3 domains, pinned CPUs share one
            Arguments.of("2-5", Map.of("aaa", 0, "bbb", 1), Map.of("aaa", 2, "bbb", 3), SPLIT_L3, 1),
            // cpuset and pinned CPUs both span two L3 domains
            Arguments.of("2-5", Map.of("aaa", 1, "bbb", 2), Map.of("aaa", 3, "bbb", 4), SPLIT_L3, 2),
            Arguments.of(
                "2-5",
                Map.of("aaa", NO_AFFINITY, "bbb", 2, "ccc", 0),
                Map.of("aaa", NO_AFFINITY, "bbb", 4, "ccc", 2),
                SHARED_L3,
                0));
    }
}
