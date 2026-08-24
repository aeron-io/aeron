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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Groups CPU ids by the die they belong to, based on the {@code sysfs} CPU topology information.
 */
public class DieLocalityGroupGenerator
{
    /**
     * Path, relative to a per-CPU {@code sysfs} directory, of the file containing the CPU's die id.
     */
    public static final String DIE_ID_DIRECTORY = "topology/die_id";
    private final Path sysfsRoot;

    private int loadDieId(final int cpu) throws IOException
    {
        final Path cpuListPath = sysfsRoot.resolve("cpu%d".formatted(cpu)).resolve(DIE_ID_DIRECTORY);
        final String dieId = Files.readString(cpuListPath);
        return Integer.parseInt(dieId);
    }

    /**
     * Creates a generator that reads CPU topology information from the given {@code sysfs} root.
     *
     * @param sysfsRoot the root {@code sysfs} CPU topology directory.
     */
    public DieLocalityGroupGenerator(final Path sysfsRoot)
    {
        this.sysfsRoot = sysfsRoot;
    }

    /**
     * Groups the given CPU ids by the die they belong to.
     *
     * @param cpuList the CPU ids to group.
     * @return one list per die, each containing the CPU ids belonging to that die.
     * @throws IOException if a CPU's die id cannot be read.
     */
    public List<IntArrayList> group(final IntArrayList cpuList) throws IOException
    {
        final Int2ObjectHashMap<IntArrayList> map = new Int2ObjectHashMap<>();
        for (int i = 0; i < cpuList.size(); i++)
        {
            final int cpu = cpuList.get(i);
            map.computeIfAbsent(loadDieId(cpu), k -> new IntArrayList()).add(cpu);
        }
        return map.values().stream().toList();
    }
}
