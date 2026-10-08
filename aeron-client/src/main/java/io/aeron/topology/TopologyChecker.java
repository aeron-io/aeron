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

import io.aeron.CommonContext;
import io.aeron.exceptions.ConfigurationException;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.List;

/**
 * Checks that a selection of CPUs is optimal for the CPU topology using a configurable set of validators, by default
 * the process's effective cgroup cpuset. Violations are reported as warnings, or as a thrown
 * {@link ConfigurationException} depending on the configuration.
 */
public final class TopologyChecker
{
    // TODO: Move this somewhere more general
    static final Path DEFAULT_SYSFS_ROOT = Path.of("/sys/devices/system/cpu");
    private final List<TopologyValidator> topologyValidators;
    private final CpusetV2Reader cpusetV2Reader;

    /**
     * Default constructor.
     */
    public TopologyChecker()
    {
        this(DEFAULT_SYSFS_ROOT);
    }

    /**
     * Creates a checker that reads CPU topology information from the given {@code sysfs} root.
     *
     * @param sysfsRoot the root {@code sysfs} CPU topology directory.
     */
    public TopologyChecker(final Path sysfsRoot)
    {
        this(sysfsRoot, new CpusetV2Reader());
    }

    TopologyChecker(final Path sysfsRoot, final CpusetV2Reader cpusetV2Reader)
    {
        // TODO: Reconsider using ServiceLoader.
        //  However, this would require a sysfsRoot set method in the interface level
        this(cpusetV2Reader, List.of(
            new DieLocalityValidator(sysfsRoot),
            new L3TopologyValidator(sysfsRoot),
            new ThreadAlignmentValidator(sysfsRoot)));
    }

    /**
     * Creates a checker that applies the given validators.
     *
     * @param cpusetV2Reader     to read the process's effective cgroup cpuset with.
     * @param topologyValidators to apply to a selection of CPUs.
     */
    public TopologyChecker(final CpusetV2Reader cpusetV2Reader, final List<TopologyValidator> topologyValidators)
    {
        this.cpusetV2Reader = cpusetV2Reader;
        this.topologyValidators = List.copyOf(topologyValidators);
    }

    /**
     * Validates the current process's effective cgroup cpuset for various conditions, writing warnings to
     * {@link CommonContext#fallbackLogger()}.
     *
     * @param warningsAsErrors if true, throw a {@link ConfigurationException} instead of warning when any
     *                         violation is found.
     */
    public void validate(final boolean warningsAsErrors)
    {
        validate(
            new CpuSelection.CpusetSelection(cpusetV2Reader.readCpuSet()),
            warningsAsErrors,
            CommonContext.fallbackLogger());
    }

    void validate(final CpuSelection selection, final boolean warningsAsErrors, final PrintStream out)
    {
        final int warnings = check(selection, out);
        if (warningsAsErrors && 0 < warnings)
        {
            throw new ConfigurationException("cpuset warnings as errors, %d warnings".formatted(warnings));
        }
    }

    int check(final CpuSelection selection, final PrintStream out)
    {
        if (selection.cpus().size() < 2)
        {
            return 0;
        }

        int warnings = 0;
        for (final TopologyValidator validator : topologyValidators)
        {
            warnings += validator.validate(selection, out);
        }

        return warnings;
    }
}
