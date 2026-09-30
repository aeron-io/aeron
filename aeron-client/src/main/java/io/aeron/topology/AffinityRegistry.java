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
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.SystemUtil;
import org.agrona.collections.IntArrayList;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.affinity.ThreadAffinity;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.concurrent.affinity.ThreadAffinity.NO_AFFINITY;
import static org.agrona.concurrent.status.CountersReader.MAX_LABEL_LENGTH;

/**
 * Registry mapping named thread affinities onto the CPUs of the process's effective cgroup cpuset.
 */
public final class AffinityRegistry
{
    /**
     * Name of the system property to treat the requested CPU affinities of the Archive and Cluster threads as
     * indices into the effective cgroup cpuset, and to validate that cpuset.
     */
    public static final String CPUSET_AFFINITY_PROP_NAME = "aeron.cpuset.affinity";

    /**
     * Name of the system property to treat CPU affinity and topology warnings of the Archive and Cluster as errors.
     */
    public static final String CPUSET_WARNINGS_AS_ERRORS_PROP_NAME = "aeron.cpuset.warnings.as.errors";

    /**
     * The standard sys directory for CPU topology information.
     */
    public static final Path DEFAULT_SYSFS_ROOT = Path.of("/sys/devices/system/cpu");

    static final int CPU_OFFSET = 0;
    static final int REQUESTED_AFFINITY_OFFSET = CPU_OFFSET + SIZE_OF_INT;
    static final int PID_OFFSET = REQUESTED_AFFINITY_OFFSET + SIZE_OF_INT;
    static final int KEY_LENGTH = PID_OFFSET + Long.BYTES;

    private final Map<String, Integer> requestedAffinityByName = new LinkedHashMap<>();
    private final Map<String, Integer> resolvedAffinityByName = new LinkedHashMap<>();
    private final Path sysfsRoot;
    private final CpusetV2Reader cpusetV2Reader;
    private final boolean topologyAvailable;
    private boolean isConcluded = false;

    /**
     * Allocates a counter, e.g. {@code Aeron::addCounter} or {@code CountersManager::newCounter}.
     */
    @FunctionalInterface
    public interface CounterAllocator
    {
        /**
         * Allocate a counter.
         *
         * @param typeId      of the counter.
         * @param keyBuffer   containing the key.
         * @param keyOffset   of the key in the buffer.
         * @param keyLength   of the key.
         * @param labelBuffer containing the label.
         * @param labelOffset of the label in the buffer.
         * @param labelLength of the label.
         * @return the allocated counter.
         */
        AtomicCounter allocate(
            int typeId,
            DirectBuffer keyBuffer,
            int keyOffset,
            int keyLength,
            DirectBuffer labelBuffer,
            int labelOffset,
            int labelLength);
    }

    /**
     * The counters published for the pinned threads of a component, closing them releases the claimed CPUs.
     */
    public static final class AffinityClaims implements AutoCloseable
    {
        private final List<AtomicCounter> counters;

        AffinityClaims(final List<AtomicCounter> counters)
        {
            this.counters = counters;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close()
        {
            CloseHelper.closeAll(counters);
        }
    }

    /**
     * Creates a registry for the process's effective cgroup cpuset and CPU topology.
     */
    public AffinityRegistry()
    {
        this(DEFAULT_SYSFS_ROOT, new CpusetV2Reader(), SystemUtil.isLinux());
    }

    AffinityRegistry(final Path sysfsRoot, final CpusetV2Reader cpusetV2Reader, final boolean topologyAvailable)
    {
        this.sysfsRoot = sysfsRoot;
        this.cpusetV2Reader = cpusetV2Reader;
        this.topologyAvailable = topologyAvailable;
    }

    /**
     * Should the requested CPU affinities of the Archive and Cluster threads be treated as indices into the effective
     * cgroup cpuset.
     *
     * @return true if cpuset affinity is enabled.
     * @see #CPUSET_AFFINITY_PROP_NAME
     */
    public static boolean cpusetAffinity()
    {
        return Boolean.parseBoolean(SystemUtil.getProperty(CPUSET_AFFINITY_PROP_NAME, "false"));
    }

    /**
     * Should CPU affinity and topology warnings of the Archive and Cluster be treated as errors.
     *
     * @return true if warnings should be treated as errors.
     * @see #CPUSET_WARNINGS_AS_ERRORS_PROP_NAME
     */
    public static boolean cpusetWarningsAsErrors()
    {
        return Boolean.parseBoolean(SystemUtil.getProperty(CPUSET_WARNINGS_AS_ERRORS_PROP_NAME, "false"));
    }

    /**
     * Registers the requested affinity for a named thread.
     *
     * @param name     the name of the thread.
     * @param affinity the CPU, or index into the cpuset, requested for the thread, or
     *                 {@link ThreadAffinity#NO_AFFINITY} to leave the thread unpinned.
     * @return this for a fluent API.
     * @throws IllegalStateException if called after conclude.
     */
    public AffinityRegistry addAffinity(final String name, final int affinity)
    {
        if (isConcluded)
        {
            throw new IllegalStateException("cannot add affinity after conclusion");
        }
        requestedAffinityByName.put(name, affinity);
        return this;
    }

    /**
     * Gets the resolved affinity for a named thread.
     *
     * @param name the name of the thread.
     * @return the CPU the thread is to be pinned to, or {@link ThreadAffinity#NO_AFFINITY}.
     * @throws IllegalStateException    if called before conclude.
     * @throws IllegalArgumentException if no affinity was registered for the name.
     */
    public int mappedAffinityValue(final String name)
    {
        if (!isConcluded)
        {
            throw new IllegalStateException("cannot get affinity value before conclusion");
        }
        final Integer affinity = resolvedAffinityByName.get(name);
        if (null == affinity)
        {
            throw new IllegalArgumentException("no affinity registered for " + name);
        }
        return affinity;
    }

    /**
     * Resolves the registered affinities and validates them, writing warnings to {@link System#err}, then publishes
     * a counter for each pinned thread so other components can validate against the claimed CPUs.
     *
     * @param cpusetAffinity   if true, requested values are indices into the effective cgroup cpuset which is also
     *                         validated, otherwise they are raw CPU ids.
     * @param warningsAsErrors if true, throw a {@link ConfigurationException} instead of warning.
     * @param countersReader   to read the CPUs claimed by other components from, may be null.
     * @param allocator        to allocate the claim counters with, may be null to skip publishing.
     * @return the claims which are to be closed when the component closes.
     * @throws IllegalStateException  if already concluded.
     * @throws ConfigurationException if an index, or a raw CPU id when {@code cpusetAffinity} is not set, is
     *                                outside the cpuset, or a warning is found and
     *                                {@code warningsAsErrors} is set.
     */
    public AffinityClaims conclude(
        final boolean cpusetAffinity,
        final boolean warningsAsErrors,
        final CountersReader countersReader,
        final CounterAllocator allocator)
    {
        return conclude(cpusetAffinity, warningsAsErrors, countersReader, allocator, System.err);
    }

    AffinityClaims conclude(
        final boolean cpusetAffinity,
        final boolean warningsAsErrors,
        final CountersReader countersReader,
        final CounterAllocator allocator,
        final PrintStream out)
    {
        if (isConcluded)
        {
            throw new IllegalStateException("already concluded");
        }

        int warnings = validateUnshared(out);

        if (cpusetAffinity && topologyAvailable)
        {
            final Cpuset cpuset = cpusetV2Reader.readCpuSet();
            warnings += new CGroupValidator(sysfsRoot, cpusetV2Reader).check(cpuset, out);
            resolveFromCpuset(cpuset);
        }
        else
        {
            if (topologyAvailable && hasPinnedAffinity())
            {
                validateRawAgainstCpuset(cpusetV2Reader.readCpuSet());
            }
            resolvedAffinityByName.putAll(requestedAffinityByName);
        }

        if (topologyAvailable)
        {
            warnings += validate(countersReader, out);
        }

        isConcluded = true;

        if (warningsAsErrors && 0 < warnings)
        {
            throw new ConfigurationException("cpuset warnings as errors, " + warnings + " warnings");
        }

        return publish(allocator);
    }

    private AffinityClaims publish(final CounterAllocator allocator)
    {
        final List<AtomicCounter> counters = new ArrayList<>();
        if (null == allocator)
        {
            return new AffinityClaims(counters);
        }

        final UnsafeBuffer keyBuffer = new UnsafeBuffer(new byte[KEY_LENGTH]);
        final UnsafeBuffer labelBuffer = new UnsafeBuffer(new byte[MAX_LABEL_LENGTH]);
        final long pid = ProcessHandle.current().pid();

        try
        {
            resolvedAffinityByName.forEach((name, cpu) ->
            {
                if (NO_AFFINITY != cpu)
                {
                    final int requested = requestedAffinityByName.get(name);
                    keyBuffer.putInt(CPU_OFFSET, cpu);
                    keyBuffer.putInt(REQUESTED_AFFINITY_OFFSET, requested);
                    keyBuffer.putLong(PID_OFFSET, pid);

                    final String label =
                        "cpu-affinity: " + name + " cpu=" + cpu + " requested=" + requested + " pid=" + pid;
                    final int labelLength = labelBuffer.putStringWithoutLengthAscii(
                        0, label, 0, MAX_LABEL_LENGTH);

                    final AtomicCounter counter = allocator.allocate(
                        AeronCounters.CPU_AFFINITY_TYPE_ID, keyBuffer, 0, KEY_LENGTH, labelBuffer, 0, labelLength);
                    counter.setRelease(cpu);
                    counters.add(counter);
                }
            });
        }
        catch (final RuntimeException ex)
        {
            CloseHelper.closeAll(counters);
            throw ex;
        }

        return new AffinityClaims(counters);
    }

    private int validateUnshared(final PrintStream out)
    {
        // This is a specific affinity-only validation and does not apply to cpuset validation.
        int warnings = 0;
        final List<Map.Entry<String, Integer>> entries = new ArrayList<>(requestedAffinityByName.entrySet());
        for (int i = 0; i < entries.size(); i++)
        {
            for (int j = i + 1; j < entries.size(); j++)
            {
                final int a = entries.get(i).getValue();
                final int b = entries.get(j).getValue();
                if (NO_AFFINITY != a && a == b)
                {
                    out.printf("WARNING: %s and %s are sharing cpu affinity=%d%n",
                        entries.get(i).getKey(), entries.get(j).getKey(), a);
                    warnings++;
                }
            }
        }

        return warnings;
    }

    private void resolveFromCpuset(final Cpuset cpuset)
    {
        final IntArrayList cpus = cpuset.cpus();
        requestedAffinityByName.forEach((name, index) ->
        {
            if (NO_AFFINITY == index)
            {
                resolvedAffinityByName.put(name, NO_AFFINITY);
            }
            else if (index < 0 || cpus.size() <= index)
            {
                throw new ConfigurationException(
                    name + " affinity " + index + " must be less than cpuset count " + cpus.size() +
                    ", cpuset: " + cpuset.formattedCpus());
            }
            else
            {
                resolvedAffinityByName.put(name, cpus.getInt(index));
            }
        });
    }

    private boolean hasPinnedAffinity()
    {
        for (final int affinity : requestedAffinityByName.values())
        {
            if (NO_AFFINITY != affinity)
            {
                return true;
            }
        }

        return false;
    }

    private void validateRawAgainstCpuset(final Cpuset cpuset)
    {
        final IntArrayList cpus = cpuset.cpus();
        requestedAffinityByName.forEach((name, cpu) ->
        {
            if (NO_AFFINITY != cpu && !cpus.containsInt(cpu))
            {
                throw new ConfigurationException(
                    name + " affinity " + cpu + " is not in cpuset: " + cpuset.formattedCpus());
            }
        });
    }

    private int validate(final CountersReader countersReader, final PrintStream out)
    {
        final Map<String, Integer> pinned = new LinkedHashMap<>();
        resolvedAffinityByName.forEach((name, cpu) ->
        {
            if (NO_AFFINITY != cpu)
            {
                pinned.put(name, cpu);
            }
        });

        if (pinned.isEmpty())
        {
            return 0;
        }

        final Map<String, Integer> claimedCpus = readClaims(countersReader);
        int warnings = 0;
        for (final Map.Entry<String, Integer> own : pinned.entrySet())
        {
            for (final Map.Entry<String, Integer> claim : claimedCpus.entrySet())
            {
                if (own.getValue().equals(claim.getValue()))
                {
                    out.printf("WARNING: %s and %s are sharing cpu=%d%n", own.getKey(), claim.getKey(), own.getValue());
                    warnings++;
                }
            }
        }

        final Map<String, Integer> union = new LinkedHashMap<>(claimedCpus);
        union.putAll(pinned);
        if (1 < union.size())
        {
            final IntArrayList cpus = new IntArrayList();
            union.values().forEach(cpus::addInt);
            final String formattedCpuset = "affinity " + union.entrySet().stream()
                .map((e) -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", ", "[", "]"));
            final Cpuset affinitySet = new Cpuset(cpus, formattedCpuset);

            warnings += new L3TopologyValidator(sysfsRoot).validate(affinitySet, out);
            warnings += new DieLocalityValidator(sysfsRoot).validate(affinitySet, out);
        }

        return warnings;
    }

    static Map<String, Integer> readClaims(final CountersReader countersReader)
    {
        final Map<String, Integer> claimedCpus = new LinkedHashMap<>();
        if (null != countersReader)
        {
            countersReader.forEach((counterId, typeId, keyBuffer, label) ->
            {
                if (AeronCounters.CPU_AFFINITY_TYPE_ID == typeId)
                {
                    claimedCpus.put(label, keyBuffer.getInt(CPU_OFFSET));
                }
            });
        }

        return claimedCpus;
    }
}
