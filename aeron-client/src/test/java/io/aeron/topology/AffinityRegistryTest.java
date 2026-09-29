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

import io.aeron.AeronCounters;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.test.CapturingPrintStream;
import io.aeron.topology.TopologyTestUtils.Pair;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
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
import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;
import static org.agrona.concurrent.status.CountersReader.COUNTER_LENGTH;
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
    private final CountersManager countersManager = new CountersManager(
        new UnsafeBuffer(ByteBuffer.allocateDirect(64 * METADATA_LENGTH)),
        new UnsafeBuffer(ByteBuffer.allocateDirect(64 * COUNTER_LENGTH)));

    @BeforeEach
    void setUp() throws IOException
    {
        setupSiblingThreads(sysfsTestDir, ALIGNED_SIBLINGS);
        setupDieLocality(sysfsTestDir, SINGLE_DIE);
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

        final AffinityRegistry registry = newRegistry(indices);
        registry.conclude(true, false, countersManager, out.resetAndGetPrintStream());

        expectedCpus.forEach((name, cpu) -> assertEquals(cpu, registry.mappedAffinityValue(name), name));
        final String output = out.flushAndGetContent();
        assertEquals(expectedWarningCount, countWarnings(output), output);

        if (0 < expectedWarningCount)
        {
            final ConfigurationException ex = assertThrows(
                ConfigurationException.class,
                () -> newRegistry(indices).conclude(true, true, countersManager, discard()));
            assertTrue(ex.getMessage().contains(expectedWarningCount + " warnings"), ex.getMessage());
        }
        else
        {
            assertDoesNotThrow(() -> newRegistry(indices).conclude(true, true, countersManager, discard()));
        }
    }

    @Test
    void shouldUseRawCpuIdsWithoutReadingCpusetWhenCpusetAffinityDisabled() throws IOException
    {
        setupL3Peers(sysfsTestDir, SHARED_L3);

        final AffinityRegistry registry = newRegistry(Map.of("aaa", 6, "bbb", 7, "ccc", NO_AFFINITY));
        registry.conclude(false, true, countersManager, discard());

        assertEquals(6, registry.mappedAffinityValue("aaa"));
        assertEquals(7, registry.mappedAffinityValue("bbb"));
        assertEquals(NO_AFFINITY, registry.mappedAffinityValue("ccc"));
    }

    @Test
    void shouldRejectIndexOutsideCpuset() throws IOException
    {
        setupL3Peers(sysfsTestDir, SHARED_L3);
        setupCpuSet(testProcPath, testCgroupPath, 0, "2-5");

        final AffinityRegistry registry = newRegistry(Map.of("aaa", 4));
        final ConfigurationException ex = assertThrows(
            ConfigurationException.class, () -> registry.conclude(true, false, countersManager, discard()));

        assertTrue(ex.getMessage().contains("aaa affinity 4 must be less than cpuset count 4"), ex.getMessage());
    }

    @Test
    void shouldWarnWhenThreadsOfComponentShareAffinity() throws IOException
    {
        setupL3Peers(sysfsTestDir, SHARED_L3);

        final AffinityRegistry registry = newRegistry(Map.of("aaa", 2, "bbb", 2));
        registry.conclude(false, false, countersManager, out.resetAndGetPrintStream());

        final String output = out.flushAndGetContent();
        assertEquals(1, countWarnings(output), output);
        assertTrue(output.contains("sharing cpu affinity=2"), output);
        assertThrows(
            ConfigurationException.class,
            () -> newRegistry(Map.of("aaa", 2, "bbb", 2)).conclude(false, true, countersManager, discard()));
    }

    @Test
    void shouldWarnWhenCpuClaimedByAnotherComponent() throws IOException
    {
        setupL3Peers(sysfsTestDir, SHARED_L3);

        final AffinityRegistry first = newRegistry(Map.of("aaa", 2));
        first.conclude(false, true, countersManager, discard());
        try (AffinityRegistry.Claims ignore = first.publish(countersManager::newCounter))
        {
            final AffinityRegistry second = newRegistry(Map.of("bbb", 2, "ccc", 3));
            second.conclude(false, false, countersManager, out.resetAndGetPrintStream());

            final String output = out.flushAndGetContent();
            assertEquals(1, countWarnings(output), output);
            assertTrue(output.contains("bbb and cpu-affinity: aaa cpu=2"), output);
        }

        assertDoesNotThrow(
            () -> newRegistry(Map.of("bbb", 2)).conclude(false, true, countersManager, discard()),
            "claims are released when the publishing component closes");
    }

    @Test
    void shouldValidateLocalityAcrossClaimedCpus() throws IOException
    {
        setupL3Peers(sysfsTestDir, SPLIT_L3);

        final AffinityRegistry first = newRegistry(Map.of("aaa", 1));
        first.conclude(false, true, countersManager, discard());
        try (AffinityRegistry.Claims ignore = first.publish(countersManager::newCounter))
        {
            final AffinityRegistry second = newRegistry(Map.of("bbb", 5));
            second.conclude(false, false, countersManager, out.resetAndGetPrintStream());

            final String output = out.flushAndGetContent();
            assertEquals(1, countWarnings(output), output);
            assertTrue(output.contains("multiple L3 cache domains"), output);
        }
    }

    @Test
    void shouldPublishCounterPerPinnedThread() throws IOException
    {
        setupL3Peers(sysfsTestDir, SHARED_L3);
        setupCpuSet(testProcPath, testCgroupPath, 0, "4-7");

        final AffinityRegistry registry = newRegistry(Map.of("aaa", 1, "bbb", NO_AFFINITY));
        registry.conclude(true, true, countersManager, discard());

        try (AffinityRegistry.Claims ignore = registry.publish(countersManager::newCounter))
        {
            final Map<String, Integer> claims = AffinityRegistry.readClaims(countersManager);
            final long pid = ProcessHandle.current().pid();
            assertEquals(Map.of("cpu-affinity: aaa cpu=5 requested=1 pid=" + pid, 5), claims);

            countersManager.forEach((counterId, typeId, keyBuffer, label) ->
            {
                if (AeronCounters.CPU_AFFINITY_TYPE_ID == typeId)
                {
                    assertEquals(1, keyBuffer.getInt(AffinityRegistry.REQUESTED_AFFINITY_OFFSET));
                    assertEquals(pid, keyBuffer.getLong(AffinityRegistry.PID_OFFSET));
                    assertEquals(5, countersManager.getCounterValue(counterId));
                }
            });
        }

        assertEquals(Map.of(), AffinityRegistry.readClaims(countersManager));
    }

    @Test
    void shouldNotReadCpusetOrTopologyWhenNoThreadIsPinned(
        @TempDir final Path emptySysfs, @TempDir final Path emptyProc, @TempDir final Path emptyCgroup)
    {
        final AffinityRegistry registry = new AffinityRegistry(
            emptySysfs, new CpusetV2Reader(emptyProc, emptyCgroup), true)
            .addAffinity("aaa", NO_AFFINITY)
            .addAffinity("bbb", NO_AFFINITY);

        assertDoesNotThrow(() -> registry.conclude(false, true, countersManager, discard()));
        assertEquals(NO_AFFINITY, registry.mappedAffinityValue("aaa"));
        assertEquals(NO_AFFINITY, registry.mappedAffinityValue("bbb"));
    }

    @Test
    void shouldGuardLifecycle()
    {
        final AffinityRegistry registry = newRegistry(Map.of());
        assertThrows(IllegalStateException.class, () -> registry.mappedAffinityValue("aaa"));
        assertThrows(IllegalStateException.class, () -> registry.publish(countersManager::newCounter));

        registry.conclude(false, false, null, discard());

        assertThrows(IllegalArgumentException.class, () -> registry.mappedAffinityValue("aaa"));
        assertThrows(IllegalStateException.class, () -> registry.addAffinity("aaa", 1));
        assertThrows(IllegalStateException.class, () -> registry.conclude(false, false, null, discard()));
    }

    private AffinityRegistry newRegistry(final Map<String, Integer> affinities)
    {
        final AffinityRegistry registry = new AffinityRegistry(
            sysfsTestDir, new CpusetV2Reader(testProcPath, testCgroupPath), true);
        new LinkedHashMap<>(affinities).forEach(registry::addAffinity);
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
