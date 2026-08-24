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

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.List;

/**
 * Validates that a process's effective cgroup cpuset is optimal based on a number of conditions.
 * Violations are reported as warnings, or as a thrown {@link ConfigurationException} depending on the configuration.
 */
public class CGroupValidator
{
    static final Path DEFAULT_SYSFS_ROOT = Path.of("/sys/devices/system/cpu");

    private final L3GroupGenerator l3GroupGenerator;
    private final DieLocalityGroupGenerator dieLocalityGroupGenerator;
    private final ThreadAlignmentChecker threadAlignmentChecker;
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
        this.l3GroupGenerator = new L3GroupGenerator(sysfsRoot);
        this.dieLocalityGroupGenerator = new DieLocalityGroupGenerator(sysfsRoot);
        this.threadAlignmentChecker = new ThreadAlignmentChecker(sysfsRoot);
        this.cpusetV2Reader = cpusetV2Reader;
    }

    /**
     * Validates the current process's effective cgroup cpuset for various conditions.
     *
     * @param warningsAsErrors if true, throw a {@link ConfigurationException} instead of warning when any
     *                         violation is found.
     */
    public void validate(final boolean warningsAsErrors)
    {
        validate(cpusetV2Reader.readCpuSet().cpus(), warningsAsErrors, System.err);
    }

    void validate(final IntArrayList cpuList, final boolean warningsAsErrors, final PrintStream out)
    {
        if (cpuList.size() < 2)
        {
            return;
        }

        final int alignmentWarnings = checkAlignment(cpuList, out);
        final int dieLocalityWarnings = checkDieLocality(cpuList, out);
        final int l3LocalityWarnings = checkL3Locality(cpuList, out);
        final int warnings = alignmentWarnings + dieLocalityWarnings + l3LocalityWarnings;

        if (warningsAsErrors && 0 < warnings)
        {
            throw new ConfigurationException(
                "cpuset topology warnings as errors, %d warning(s) found".formatted(warnings));
        }
    }

    private int checkAlignment(final IntArrayList cpuList, final PrintStream out)
    {
        try
        {
            final IntArrayList missingThreads = threadAlignmentChecker.identifyMissingThreads(cpuList);
            if (!missingThreads.isEmpty())
            {
                out.printf(
                    "WARNING: cpuset %s is missing thread sibling CPU(s) %s (partial physical core(s) in cpuset)%n",
                    cpuList, missingThreads);
                return 1;
            }
            return 0;
        }
        catch (final IOException | NumberFormatException ex)
        {
            // NOTE: This is not a CGroup violation, it will be printed directly to stderr.
            System.err.println(
                "WARNING: skipping thread alignment check for cpuset " + cpuList + ": " + ex.getMessage());
            return 0;
        }
    }

    private int checkDieLocality(final IntArrayList cpuList, final PrintStream out)
    {
        try
        {
            final List<IntArrayList> groups = dieLocalityGroupGenerator.group(cpuList);
            if (1 < groups.size())
            {
                out.println("WARNING: cpuset " + cpuList + " spans " + groups.size() + " CPU die(s): " + groups);
                return 1;
            }
            return 0;
        }
        catch (final IOException | NumberFormatException ex)
        {
            // NOTE: This is not a CGroup violation, it will be printed directly to stderr.
            System.err.println("WARNING: skipping die locality check for cpuset " + cpuList + ": " + ex.getMessage());
            return 0;
        }
    }

    private int checkL3Locality(final IntArrayList cpuList, final PrintStream out)
    {
        try
        {
            final List<IntArrayList> groups = l3GroupGenerator.group(cpuList);
            if (1 < groups.size())
            {
                out.println(
                    "WARNING: cpuset " + cpuList + " spans " + groups.size() + " L3 cache domain(s): " + groups);
                return 1;
            }
            return 0;
        }
        catch (final IOException | NumberFormatException ex)
        {
            // NOTE: This is not a CGroup violation, it will be printed directly to stderr.
            System.err.println("WARNING: skipping L3 locality check for cpuset " + cpuList + ": " + ex.getMessage());
            return 0;
        }
    }
}
