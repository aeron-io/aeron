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
import org.agrona.collections.IntArrayList;
import org.agrona.collections.Object2IntHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.affinity.ThreadAffinity;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.agrona.concurrent.affinity.ThreadAffinity.NO_AFFINITY;

/**
 * Registry mapping named thread affinities onto the CPUs of the process's effective cgroup cpuset.
 * <p>
 * Requested affinities are remapped by rank: the lowest requested CPU is assigned the first CPU of the effective
 * cpuset, the next lowest the second, and so on. For the remapping to avoid collisions, all components running in
 * a process (e.g. the Media Driver, Archive and Consensus Module) should register with the same registry before it
 * is concluded. Components in separate processes sharing one cpuset remap independently and may collide.
 */
public class AffinityRegistry
{
    private final Object2IntHashMap<String> nameToAffinityMap;
    private final Object2ObjectHashMap<String, AffinityValue> finalizedNameToAffinityMap;
    private final IntArrayList finalAffinityList = new IntArrayList();
    private final CpusetV2Reader cpusetV2Reader;
    private boolean isConcluded = false;
    private final Path sysfsRoot;
    private Cpuset realCpuset;

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
        this.nameToAffinityMap = new Object2IntHashMap<>(Integer.MIN_VALUE);
        this.finalizedNameToAffinityMap = new Object2ObjectHashMap<>();
        this.sysfsRoot = sysfsRoot;
        this.isConcluded = false;
    }

    /**
     * Creates a registry for the process's effective cgroup cpuset using the default {@code sysfs} and cgroup
     * paths.
     *
     * @return a new, unconcluded registry.
     */
    public static AffinityRegistry newDefault()
    {
        return new AffinityRegistry(CGroupValidator.DEFAULT_SYSFS_ROOT, new CpusetV2Reader());
    }

    /**
     * Resolves the registry a component should use to pin its threads.
     * <p>
     * If {@code existing} is null a new registry is created, the component's affinities are registered with it
     * and it is concluded with {@link #conclude(boolean, boolean)}. Otherwise {@code existing} is assumed to be
     * shared with other components and owned, concluded and validated by whoever supplied it.
     *
     * @param existing         the registry supplied to the component, or null if it should own its own.
     * @param registrar        registers the component's thread affinities when a new registry is created.
     * @param validateTopology passed to {@link #conclude(boolean, boolean)} when a new registry is created.
     * @param warningsAsErrors passed to {@link #conclude(boolean, boolean)} when a new registry is created.
     * @return the concluded registry to use.
     * @throws IllegalStateException if {@code existing} has not been concluded.
     */
    public static AffinityRegistry resolve(
        final AffinityRegistry existing,
        final Consumer<AffinityRegistry> registrar,
        final boolean validateTopology,
        final boolean warningsAsErrors)
    {
        if (null != existing)
        {
            if (!existing.isConcluded())
            {
                throw new IllegalStateException("supplied AffinityRegistry must be concluded before launch");
            }
            return existing;
        }

        final AffinityRegistry registry = newDefault();
        registrar.accept(registry);
        registry.conclude(validateTopology, warningsAsErrors);
        return registry;
    }

    /**
     * Registers the requested affinity for a named thread.
     *
     * @param name     the name of the thread.
     * @param affinity the CPU requested for the thread, or {@link ThreadAffinity#NO_AFFINITY} to leave the
     *                 thread unpinned.
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
     * @return the CPU from the effective cpuset that the thread's affinity maps to, or
     * {@link ThreadAffinity#NO_AFFINITY} if the thread was registered without an affinity.
     * @throws IllegalStateException    if called before {@link #conclude()}.
     * @throws IllegalArgumentException if no affinity was registered for the name.
     */
    public int mappedAffinityValue(final String name)
    {
        if (!isConcluded)
        {
            throw new IllegalStateException("Cannot get affinity value before conclusion");
        }
        final AffinityValue affinityValue = finalizedNameToAffinityMap.get(name);
        if (null == affinityValue)
        {
            throw new IllegalArgumentException("No affinity registered for " + name);
        }
        return affinityValue.remappedAffinity();
    }

    /**
     * Has {@link #conclude()} been called on this registry.
     *
     * @return true if the registry has been concluded.
     */
    public boolean isConcluded()
    {
        return isConcluded;
    }

    /**
     * Remaps the registered affinities and optionally validates both the effective cpuset and the remapped
     * affinities against the CPU topology. This is the common entry point used by all components so the same
     * validations are applied regardless of which component owns the registry.
     *
     * @param validateTopology if true, validate the effective cpuset and the remapped affinities.
     * @param warningsAsErrors if true, throw a {@link ConfigurationException} instead of warning when any
     *                         violation is found.
     * @throws IllegalStateException  if the registry has already been concluded.
     * @throws ConfigurationException if more threads are pinned than there are CPUs in the effective cpuset, or
     *                                if a violation is found and {@code warningsAsErrors} is set.
     */
    public void conclude(final boolean validateTopology, final boolean warningsAsErrors)
    {
        conclude(validateTopology, warningsAsErrors, System.err);
    }

    void conclude(final boolean validateTopology, final boolean warningsAsErrors, final PrintStream out)
    {
        conclude();
        if (validateTopology)
        {
            new CGroupValidator(sysfsRoot, cpusetV2Reader).validate(
                cpusetV2Reader.readCpuSet(), warningsAsErrors, out);
            if (!finalAffinityList.isEmpty())
            {
                validate(warningsAsErrors, out);
            }
        }
    }

    /**
     * Remaps the registered affinities, in ascending order, onto the CPUs of the effective cpuset and freezes
     * the registry so no further affinities can be added.
     *
     * @throws IllegalStateException  if the registry has already been concluded.
     * @throws ConfigurationException if more threads are pinned than there are CPUs in the effective cpuset.
     */
    public void conclude()
    {
        if (isConcluded)
        {
            throw new IllegalStateException("AffinityRegistry already concluded");
        }

        final List<Map.Entry<String, Integer>> pinnedEntries = new ArrayList<>();
        nameToAffinityMap.forEach((key, value) ->
        {
            final int affinity = value;
            if (NO_AFFINITY == affinity)
            {
                finalizedNameToAffinityMap.put(key, new AffinityValue(NO_AFFINITY, NO_AFFINITY));
            }
            else
            {
                pinnedEntries.add(Map.entry(key, affinity));
            }
        });

        if (!pinnedEntries.isEmpty())
        {
            realCpuset = cpusetV2Reader.readCpuSet();
            if (pinnedEntries.size() > realCpuset.cpus().size())
            {
                throw new ConfigurationException(
                    "cannot pin " + pinnedEntries.size() + " threads " +
                    pinnedEntries.stream().map(Map.Entry::getKey).sorted().toList() +
                    " to the " + realCpuset.cpus().size() + " CPUs of the effective cpuset " +
                    realCpuset.formattedCpus());
            }
        }

        final AtomicInteger currentIndex = new AtomicInteger(0);
        pinnedEntries.stream()
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
