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

package io.aeron.cpu.topology;

import io.aeron.CommonContext;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.exceptions.ConfigurationException;
import org.agrona.SystemUtil;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.Object2IntHashMap;
import org.agrona.concurrent.affinity.ThreadAffinity;

import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.agrona.concurrent.affinity.ThreadAffinity.NO_AFFINITY;

/**
 * Registry mapping named thread affinities onto the CPUs of the process's effective cgroup cpuset.
 */
public final class AffinityRegistry implements AutoCloseable
{
    private static final List<CoreClaim> GLOBAL_CORE_CLAIMS = new ArrayList<>();
    private static final VarHandle IS_CONCLUDED_VH;

    static
    {
        try
        {
            IS_CONCLUDED_VH = MethodHandles.lookup()
                .findVarHandle(AffinityRegistry.class, "isConcluded", boolean.class);
        }
        catch (final ReflectiveOperationException ex)
        {
            throw new ExceptionInInitializerError(ex);
        }
    }

    private final Object2IntHashMap<String> requestedAffinityByName = new Object2IntHashMap<>(Integer.MIN_VALUE);
    private final Object2IntHashMap<String> resolvedAffinityByName = new Object2IntHashMap<>(Integer.MIN_VALUE);
    private final CpusetV2Reader cpusetV2Reader;
    private final TopologyChecker cpusetChecker;
    private final TopologyChecker affinityChecker;
    private final boolean topologyAvailable;
    private final boolean cpusetAffinity;
    private final boolean warningsAsErrors;
    private final PrintStream warningStream;
    private final List<CoreClaim> ownedCoreClaims = new ArrayList<>();
    private volatile boolean isConcluded;

    record CoreClaim(String name, int cpu)
    {
    }

    /**
     * Creates a registry for the process's effective cgroup cpuset and CPU topology which writes warnings to
     * {@link CommonContext#fallbackLogger()}.
     *
     * @param cpusetAffinity   if true, requested values are indices into the effective cgroup cpuset which is also
     *                         validated, otherwise they are raw CPU ids.
     * @param warningsAsErrors if true, throw a {@link ConfigurationException} on {@link #conclude()} instead of
     *                         warning.
     */
    public AffinityRegistry(final boolean cpusetAffinity, final boolean warningsAsErrors)
    {
        this(
            TopologyChecker.DEFAULT_SYSFS_ROOT,
            new CpusetV2Reader(),
            SystemUtil.isLinux(),
            cpusetAffinity,
            warningsAsErrors,
            CommonContext.fallbackLogger());
    }

    AffinityRegistry(
        final Path sysfsRoot,
        final CpusetV2Reader cpusetV2Reader,
        final boolean topologyAvailable,
        final boolean cpusetAffinity,
        final boolean warningsAsErrors,
        final PrintStream warningStream)
    {
        this.cpusetV2Reader = cpusetV2Reader;
        this.topologyAvailable = topologyAvailable;
        this.cpusetAffinity = cpusetAffinity;
        this.warningsAsErrors = warningsAsErrors;
        this.warningStream = warningStream;
        this.cpusetChecker = new TopologyChecker(sysfsRoot, cpusetV2Reader);
        // Thread alignment is excluded as pinned threads intentionally leave out their siblings.
        this.affinityChecker = new TopologyChecker(cpusetV2Reader, List.of(
            new SharedCpuValidator(),
            new DieLocalityValidator(sysfsRoot),
            new L3TopologyValidator(sysfsRoot)));
    }

    /**
     * Registers the requested affinity for a thread.
     *
     * @param name     identifying the thread's affinity, such as its CPU affinity property name.
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
     * Gets the resolved affinity for a thread.
     *
     * @param name identifying the thread's affinity
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
     * within the JVM, writing warnings to the registry's warning stream, then claims the CPUs of the pinned threads.
     *
     * @throws ConcurrentConcludeException if already concluded.
     * @throws ConfigurationException      if a thread is pinned on a platform which does not support thread
     *                                     affinity, an index, or a raw CPU id when {@code cpusetAffinity} is not
     *                                     set, is outside the cpuset, or a warning is found and
     *                                     {@code warningsAsErrors} is set.
     */
    public void conclude()
    {
        if ((boolean)IS_CONCLUDED_VH.getAndSet(this, true))
        {
            throw new ConcurrentConcludeException();
        }

        if (!topologyAvailable && hasPinnedAffinity())
        {
            throw new ConfigurationException(
                "thread affinity is only supported on Linux, requested: " + requestedAffinityByName);
        }

        int warnings = 0;

        if (cpusetAffinity && topologyAvailable)
        {
            final Cpuset cpuset = cpusetV2Reader.readCpuSet();
            warnings += cpusetChecker.check(new CpuSelection.CpusetSelection(cpuset), warningStream);
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
            warnings += validateAgainstClaims(pinned);

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
        }
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

    private int validateAgainstClaims(final List<CoreClaim> pinned)
    {
        if (pinned.isEmpty())
        {
            return 0;
        }

        // Own claims first so that warnings name this registry's threads before those of other registries.
        final List<CoreClaim> coreClaimUnion = new ArrayList<>(pinned);
        coreClaimUnion.addAll(GLOBAL_CORE_CLAIMS);

        return affinityChecker.check(new CpuSelection.AffinitySelection(coreClaimUnion), warningStream);
    }

    static List<CoreClaim> claimedCpus()
    {
        synchronized (GLOBAL_CORE_CLAIMS)
        {
            return List.copyOf(GLOBAL_CORE_CLAIMS);
        }
    }
}
