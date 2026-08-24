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

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.IntHashSet;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Groups CPU ids by the L3 cache they share, based on the {@code sysfs} CPU topology information.
 */
public class L3GroupGenerator
{
    /**
     * Path, relative to a per-CPU {@code sysfs} directory, of the file listing the CPUs that share an L3 cache.
     */
    public static final String SHARED_CPU_LIST_DIRECTORY = "cache/index3/shared_cpu_list";
    private final PerCpuListReader perCpuListReader;

    /**
     * Creates a generator that reads CPU topology information from the given {@code sysfs} root.
     *
     * @param sysfsRoot the root {@code sysfs} CPU topology directory.
     */
    public L3GroupGenerator(final Path sysfsRoot)
    {
        this.perCpuListReader = new PerCpuListReader(sysfsRoot, SHARED_CPU_LIST_DIRECTORY);
    }

    /**
     * Groups the given CPU ids by the L3 cache they share.
     *
     * @param cpuList the CPU ids to group.
     * @return one list per L3 cache group, each containing the CPU ids that share that cache.
     * @throws IOException if a CPU's shared cache list cannot be read.
     */
    public List<IntArrayList> group(final IntArrayList cpuList) throws IOException
    {
        // TODO: Make this more efficient.
        final Map<IntHashSet, IntArrayList> map = new HashMap<>();
        for (int i = 0; i < cpuList.size(); i++)
        {
            final int cpu = cpuList.get(i);
            map.computeIfAbsent(perCpuListReader.loadCpuList(cpu), k -> new IntArrayList()).add(cpu);
        }
        return map.values().stream().toList();
    }
}
