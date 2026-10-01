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
import org.agrona.SystemUtil;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.Object2IntHashMap;
import org.agrona.concurrent.affinity.ThreadAffinity;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static org.agrona.concurrent.affinity.ThreadAffinity.NO_AFFINITY;

/**
 * Registry mapping named thread affinities onto the CPUs of the process's effective cgroup cpuset.
 */
public final class AffinityRegistry implements AutoCloseable
{
    private static final List<CoreClaim> GLOBAL_CORE_CLAIMS = new ArrayList<>();

    private final Object2IntHashMap<String> requestedAffinityByName = new Object2IntHashMap<>(Integer.MIN_VALUE);
    private final Object2IntHashMap<String> resolvedAffinityByName = new Object2IntHashMap<>(Integer.MIN_VALUE);
    private final Path sysfsRoot;
    private final CpusetV2Reader cpusetV2Reader;
    private final boolean topologyAvailable;
    private final List<CoreClaim> ownedCoreClaims = new ArrayList<>();
    private boolean isConcluded = false;

    record CoreClaim(String name, int cpu)
    {
    }

    /**
     * Creates a registry for the process's effective cgroup cpuset and CPU topology.
     */
    public AffinityRegistry()
    {
        this(CGroupValidator.DEFAULT_SYSFS_ROOT, new CpusetV2Reader(), SystemUtil.isLinux());
    }

    AffinityRegistry(final Path sysfsRoot, final CpusetV2Reader cpusetV2Reader, final boolean topologyAvailable)
    {
        this.sysfsRoot = sysfsRoot;
        this.cpusetV2Reader = cpusetV2Reader;
        this.topologyAvailable = topologyAvailable;
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
        final int affinity = resolvedAffinityByName.getValue(name);
        if (resolvedAffinityByName.missingValue() == affinity)
        {
            throw new IllegalArgumentException("no affinity registered for " + name);
        }
        return affinity;
    }

    /**
     * Resolves the registered affinities and validates them, including against the CPUs claimed by other registries
     * within the JVM, writing warnings to {@link System#err}, then claims the CPUs of the pinned threads.
     *
     * @param cpusetAffinity   if true, requested values are indices into the effective cgroup cpuset which is also
     *                         validated, otherwise they are raw CPU ids.
     * @param warningsAsErrors if true, throw a {@link ConfigurationException} instead of warning.
     * @throws IllegalStateException  if already concluded.
     * @throws ConfigurationException if a thread is pinned on a platform which does not support thread affinity,
     *                                an index, or a raw CPU id when {@code cpusetAffinity} is not set, is outside
     *                                the cpuset, or a warning is found and {@code warningsAsErrors} is set.
     */
    public void conclude(final boolean cpusetAffinity, final boolean warningsAsErrors)
    {
        conclude(cpusetAffinity, warningsAsErrors, System.err);
    }

    void conclude(final boolean cpusetAffinity, final boolean warningsAsErrors, final PrintStream out)
    {
        if (isConcluded)
        {
            throw new IllegalStateException("already concluded");
        }

        if (!topologyAvailable && hasPinnedAffinity())
        {
            throw new ConfigurationException(
                "thread affinity is only supported on Linux, requested: " + requestedAffinityByName);
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
            if (hasPinnedAffinity())
            {
                validateRawAgainstCpuset(cpusetV2Reader.readCpuSet());
            }
            resolvedAffinityByName.putAll(requestedAffinityByName);
        }

        isConcluded = true;

        final List<CoreClaim> pinned = new ArrayList<>();
        resolvedAffinityByName.forEach((name, cpu) ->
        {
            if (NO_AFFINITY != cpu)
            {
                pinned.add(new CoreClaim(name, cpu));
            }
        });

        synchronized (GLOBAL_CORE_CLAIMS)
        {
            warnings += validateAgainstClaims(pinned, out);

            if (warningsAsErrors && 0 < warnings)
            {
                throw new ConfigurationException("cpuset warnings as errors, " + warnings + " warnings");
            }

            ownedCoreClaims.addAll(pinned);
            GLOBAL_CORE_CLAIMS.addAll(pinned);
        }
    }

    /**
     * Releases the CPUs claimed by this registry.
     */
    @Override
    public void close()
    {
        synchronized (GLOBAL_CORE_CLAIMS)
        {
            for (final CoreClaim coreClaim : ownedCoreClaims)
            {
                GLOBAL_CORE_CLAIMS.remove(coreClaim);
            }
            ownedCoreClaims.clear();
        }
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
        return requestedAffinityByName.values().stream().anyMatch(index -> index != NO_AFFINITY);
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

    private int validateAgainstClaims(final List<CoreClaim> pinned, final PrintStream out)
    {
        if (pinned.isEmpty())
        {
            return 0;
        }

        int warnings = 0;
        for (final CoreClaim own : pinned)
        {
            for (final CoreClaim coreClaim : GLOBAL_CORE_CLAIMS)
            {
                if (own.cpu() == coreClaim.cpu())
                {
                    out.printf("WARNING: %s and %s are sharing cpu=%d%n", own.name(), coreClaim.name(), own.cpu());
                    warnings++;
                }
            }
        }

        final List<CoreClaim> coreClaimUnion = new ArrayList<>(GLOBAL_CORE_CLAIMS);
        coreClaimUnion.addAll(pinned);
        if (1 < coreClaimUnion.size())
        {
            final IntArrayList cpus = new IntArrayList();
            final StringJoiner formattedCpuset = new StringJoiner(", ", "affinity [", "]");
            for (final CoreClaim coreClaim : coreClaimUnion)
            {
                cpus.addInt(coreClaim.cpu());
                formattedCpuset.add(coreClaim.name() + "=" + coreClaim.cpu());
            }
            final Cpuset affinitySet = new Cpuset(cpus, formattedCpuset.toString());

            warnings += new L3TopologyValidator(sysfsRoot).validate(affinitySet, out);
            warnings += new DieLocalityValidator(sysfsRoot).validate(affinitySet, out);
        }

        return warnings;
    }

    static List<CoreClaim> claimedCpus()
    {
        synchronized (GLOBAL_CORE_CLAIMS)
        {
            return List.copyOf(GLOBAL_CORE_CLAIMS);
        }
    }
}
