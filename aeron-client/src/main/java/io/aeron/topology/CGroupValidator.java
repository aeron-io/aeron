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

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.List;

/**
 * Validates that a process's effective cgroup cpuset is optimal based on a number of conditions.
 * Violations are reported as warnings, or as a thrown {@link ConfigurationException} depending on the configuration.
 */
public class CGroupValidator
{
    // TODO: Move this somewhere more general
    static final Path DEFAULT_SYSFS_ROOT = Path.of("/sys/devices/system/cpu");
    private final List<TopologyValidator> topologyValidators;
    private final CpusetV2Reader cpusetV2Reader;

    /**
     * Default constructor.
     */
    public CGroupValidator()
    {
        this(DEFAULT_SYSFS_ROOT);
    }

    /**
     * Creates a validator that reads CPU topology information from the given {@code sysfs} root.
     *
     * @param sysfsRoot the root {@code sysfs} CPU topology directory.
     */
    public CGroupValidator(final Path sysfsRoot)
    {
        this(sysfsRoot, new CpusetV2Reader());
    }

    CGroupValidator(final Path sysfsRoot, final CpusetV2Reader cpusetV2Reader)
    {
        this.cpusetV2Reader = cpusetV2Reader;
        // TODO: Reconsider using ServiceLoader.
        //  However, this would require a sysfsRoot set method in the interface level
        //  This should also be dependency injected into the class eventually,
        //  as they have internal caching that can be used for the affinity checks eventually.
        this.topologyValidators = List.of(
            new DieLocalityValidator(sysfsRoot),
            new L3TopologyValidator(sysfsRoot),
            new ThreadAlignmentValidator(sysfsRoot)
        );
    }

    /**
     * Validates the current process's effective cgroup cpuset for various conditions.
     *
     * @param warningsAsErrors if true, throw a {@link ConfigurationException} instead of warning when any
     *                         violation is found.
     */
    public void validate(final boolean warningsAsErrors)
    {
        validate(cpusetV2Reader.readCpuSet(), warningsAsErrors, System.err);
    }

    void validate(final Cpuset cpuset, final boolean warningsAsErrors, final PrintStream out)
    {
        if (cpuset.cpus().size() < 2)
        {
            return;
        }

        int warnings = 0;
        for (final TopologyValidator validator : topologyValidators)
        {
            warnings += validator.validate(cpuset, out);
        }

        if (warningsAsErrors && 0 < warnings)
        {
            throw new ConfigurationException("cpuset warnings as errors, %d warnings".formatted(warnings));
        }
    }
}
