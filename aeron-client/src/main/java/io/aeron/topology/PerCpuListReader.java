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
import org.agrona.collections.IntHashSet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

class PerCpuListReader
{
    private final Int2ObjectHashMap<IntHashSet> cpuListCache = new Int2ObjectHashMap<>();
    private final Path sysfsRoot;
    private final String cpuListDirectory;

    PerCpuListReader(final Path sysfsRoot, final String cpuListDirectory)
    {
        this.sysfsRoot = sysfsRoot;
        this.cpuListDirectory = cpuListDirectory;
    }

    private IntHashSet loadCpuListFile(final int cpu) throws IOException
    {
        final Path cpuListPath = sysfsRoot.resolve("cpu%d".formatted(cpu)).resolve(cpuListDirectory);
        final String cpuList;
        cpuList = Files.readString(cpuListPath);
        return AffinityParser.parse(cpuList, IntHashSet::new);
    }

    IntHashSet loadCpuList(final int cpu) throws IOException
    {
        final IntHashSet cpuList = cpuListCache.get(cpu);
        if (null == cpuList)
        {
            final IntHashSet loadedCpuList = this.loadCpuListFile(cpu);
            loadedCpuList.forEachInt((i) -> cpuListCache.put(i, loadedCpuList));
            cpuListCache.put(cpu, loadedCpuList);
            return loadedCpuList;
        }
        return cpuList;
    }

}
