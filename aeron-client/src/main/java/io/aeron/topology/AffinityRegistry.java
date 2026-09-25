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
import org.agrona.collections.Object2IntHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.affinity.ThreadAffinity;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Registry mapping named thread affinities onto the CPUs of the process's effective cgroup cpuset.
 */
public class AffinityRegistry
{
    private final Object2IntHashMap<String> nameToAffinityMap;
    private final Object2ObjectHashMap<String, AffinityValue> finalizedNameToAffinityMap;
    private final IntArrayList finalAffinityList = new IntArrayList();
    private final CpusetV2Reader cpusetV2Reader;
    private boolean isConcluded = false;
    private final Path sysfsRoot;
    private final Cpuset realCpuset;

    /**
     * An affinity as requested and as remapped onto the effective cpuset.
     *
     * @param originalAffinity the CPU requested for the thread.
     * @param remappedAffinity the CPU from the effective cpuset that the request maps to.
     */
    public record AffinityValue(int originalAffinity, int remappedAffinity)
    {

    }

    /**
     * Creates a registry for the process's effective cgroup cpuset.
     *
     * @param sysfsRoot      the root {@code sysfs} CPU topology directory.
     * @param cpusetV2Reader the reader used to obtain the effective cgroup v2 cpuset.
     */
    public AffinityRegistry(final Path sysfsRoot, final CpusetV2Reader cpusetV2Reader)
    {
        this.cpusetV2Reader = cpusetV2Reader;
        this.nameToAffinityMap = new Object2IntHashMap<>(ThreadAffinity.NO_AFFINITY);
        this.finalizedNameToAffinityMap = new Object2ObjectHashMap<>();
        this.sysfsRoot = sysfsRoot;
        this.realCpuset = cpusetV2Reader.readCpuSet();
        this.isConcluded = false;
    }

    /**
     * Registers the requested affinity for a named thread.
     *
     * @param name     the name of the thread.
     * @param affinity the CPU requested for the thread.
     * @throws IllegalStateException if called after {@link #conclude()}.
     */
    public void addAffinity(final String name, final int affinity)
    {
        if (isConcluded)
        {
            throw new IllegalStateException("Cannot add affinity after conclusion");
        }
        nameToAffinityMap.put(name, affinity);
    }

    /**
     * Gets the remapped affinity for a named thread.
     *
     * @param name the name of the thread.
     * @return the CPU from the effective cpuset that the thread's affinity maps to.
     * @throws IllegalStateException if called before {@link #conclude()}.
     */
    public int mappedAffinityValue(final String name)
    {
        if (!isConcluded)
        {
            throw new IllegalStateException("Cannot get affinity value before conclusion");
        }
        return finalizedNameToAffinityMap.get(name).remappedAffinity();
    }

    /**
     * Remaps the registered affinities, in ascending order, onto the CPUs of the effective cpuset and freezes
     * the registry so no further affinities can be added.
     */
    public void conclude()
    {
        final AtomicInteger currentIndex = new AtomicInteger(0);
        nameToAffinityMap.entrySet().stream()
            .map(e -> Map.entry(e.getKey(), e.getValue()))
            .sorted(Comparator.comparingInt(Map.Entry::getValue))
            .forEach(e ->
            {
                final int index = currentIndex.getAndIncrement();
                final int remappedAffinity = realCpuset.cpus().get(index);
                finalizedNameToAffinityMap.put(
                    e.getKey(),
                    new AffinityValue(e.getValue(), remappedAffinity));
                finalAffinityList.add(remappedAffinity);
            });
        this.isConcluded = true;
    }

    /**
     * Validates the remapped affinities against the CPU topology.
     *
     * @param warningsAsErrors if true, throw a {@link io.aeron.exceptions.ConfigurationException} instead of
     *                         warning when any violation is found.
     * @param out              the stream to which warnings are written.
     * @throws IllegalStateException if called before {@link #conclude()}.
     */
    public void validate(final boolean warningsAsErrors, final PrintStream out)
    {
        if (!isConcluded)
        {
            throw new IllegalStateException("Cannot validate before conclusion");
        }
        // Only these are relevant to CPU affinity validation
        final List<TopologyValidator> validators = List.of(
            new DieLocalityValidator(sysfsRoot),
            new L3TopologyValidator(sysfsRoot));

        final CGroupValidator cGroupValidator = new CGroupValidator(validators, cpusetV2Reader);
        final Cpuset affinitySet = new Cpuset(finalAffinityList, null);
        cGroupValidator.validate(affinitySet, warningsAsErrors, out);
    }
}
