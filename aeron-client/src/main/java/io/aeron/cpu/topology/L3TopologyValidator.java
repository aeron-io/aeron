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

import org.agrona.collections.IntArrayList;
import org.agrona.collections.IntHashSet;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import static io.aeron.cpu.topology.TopologyChecker.DEFAULT_SYSFS_ROOT;

class L3TopologyValidator implements TopologyValidator
{
        /**
     * Path, relative to a per-CPU {@code sysfs} directory, of the file listing the CPUs that share an L3 cache.
     */
    public static final String SHARED_CPU_LIST_DIRECTORY = "cache/index3/shared_cpu_list";
    private final PerCpuListReader perCpuListReader;

    L3TopologyValidator()
    {
        this(DEFAULT_SYSFS_ROOT);
    }

    L3TopologyValidator(final Path sysfsRoot)
    {
        this.perCpuListReader = new PerCpuListReader(sysfsRoot, SHARED_CPU_LIST_DIRECTORY);
    }

    public int validate(final CpuSelection selection, final PrintStream warningStream)
    {
        try
        {
            final IntArrayList cpuList = selection.cpus();
            final IntHashSet expectedPeers = this.perCpuListReader.loadCpuList(cpuList.get(0));
            if (cpuList.stream().anyMatch(cpu -> !expectedPeers.contains(cpu)))
            {
                warningStream.printf(
                    "WARNING: %s spans multiple L3 cache domains, configuration: %s%n",
                    selection.kind(), selection.configuration());
                return 1;
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
