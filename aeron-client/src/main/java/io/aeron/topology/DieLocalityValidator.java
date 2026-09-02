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

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import static io.aeron.topology.CGroupValidator.DEFAULT_SYSFS_ROOT;

class DieLocalityValidator implements TopologyValidator
{
    /**
     * Path, relative to a per-CPU {@code sysfs} directory, of the file listing the CPU's die id.
     */
    public static final String DIE_ID_DIRECTORY = "topology/die_id";
    private final PerCpuListReader perCpuListReader;

    DieLocalityValidator()
    {
        this(DEFAULT_SYSFS_ROOT);
    }

    /**
     * Creates a generator that reads CPU topology information from the given {@code sysfs} root.
     *
     * @param sysfsRoot the root {@code sysfs} CPU topology directory.
     */
    DieLocalityValidator(final Path sysfsRoot)
    {
        this.perCpuListReader = new PerCpuListReader(sysfsRoot, DIE_ID_DIRECTORY);
    }

    public int validate(final IntArrayList cpuList, final PrintStream warningStream)
    {
        try
        {
            final int expectedDieId = this.perCpuListReader.loadId(cpuList.get(0));
            for (int i = 0; i < cpuList.size(); i++)
            {
                final int cpu = cpuList.get(i);
                if (this.perCpuListReader.loadId(cpu) != expectedDieId)
                {
                    // TODO: Match with C warning
                    warningStream.printf("CPU list %s does not contain all cores in the same die %n", cpuList);
                    return 1;
                }
            }
        }
        catch (final IOException e)
        {
            warningStream.printf("Failed to read L3 topology information: %s%n", e.getMessage());
            return 0;
        }
        return 0;
    }
}
