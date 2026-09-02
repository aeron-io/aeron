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

import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.IntHashSet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.agrona.AsciiEncoding.parseIntAscii;

class PerCpuListReader
{
    // TODO: Rework this to pre-load entire CPU list
    private final Int2ObjectHashMap<IntHashSet> cpuToCpuListMap = new Int2ObjectHashMap<>();
    private final Int2IntHashMap cpuToIdMap = new Int2IntHashMap(-1);
    private final Path sysfsRoot;
    private final String cpuListDirectory;

    PerCpuListReader(final Path sysfsRoot, final String cpuListDirectory)
    {
        this.sysfsRoot = sysfsRoot;
        this.cpuListDirectory = cpuListDirectory;
    }

    private IntHashSet loadCpuList(final int cpu) throws IOException
    {
        final Path cpuListPath = sysfsRoot.resolve("cpu%d".formatted(cpu)).resolve(cpuListDirectory);
        final String cpuList = Files.readString(cpuListPath);
        final IntHashSet result = new IntHashSet();
        AffinityParser.parse(cpuList, result::add);
        return result;
    }

    Int2ObjectHashMap<IntHashSet> loadCpuList(final IntArrayList cpus) throws IOException
    {
        for (int i = 0, size = cpus.size(); i < size; i++)
        {
            final int cpu = cpus.get(i);
            if (cpuToCpuListMap.containsKey(cpu))
            {
                continue;
            }
            final IntHashSet loadedCpuList = this.loadCpuList(cpu);
            loadedCpuList.forEachInt((j) -> cpuToCpuListMap.put(j, loadedCpuList));
        }
        return cpuToCpuListMap;
    }

    private int loadId(final int cpu) throws IOException
    {
        final Path cpuListPath = sysfsRoot.resolve("cpu%d".formatted(cpu)).resolve(cpuListDirectory);
        final String id = Files.readString(cpuListPath);
        return parseIntAscii(id, 0, id.length());
    }


    Int2IntHashMap loadIds(final IntArrayList cpus) throws IOException
    {
        for (int i = 0, size = cpus.size(); i < size; i++)
        {
            final int cpu = cpus.get(i);
            if (cpuToIdMap.containsKey(cpu))
            {
                continue;
            }
            cpuToIdMap.put(cpu, this.loadId(cpu));
        }

        return cpuToIdMap;
    }
}
