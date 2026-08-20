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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DieLocalityGroupGenerator
{
    public static final String DIE_ID_DIRECTORY = "topology/die_id";
    private final Path sysfsRoot;
    private final Int2ObjectHashMap<IntHashSet> sharedCpuListCache = new Int2ObjectHashMap<>();

    private int loadDieId(final int cpu) throws IOException
    {
        final Path cpuListPath = sysfsRoot.resolve("cpu%d".formatted(cpu)).resolve(DIE_ID_DIRECTORY);
        final String dieId = Files.readString(cpuListPath);
        return Integer.parseInt(dieId);
    }

    public DieLocalityGroupGenerator(final Path sysfsRoot)
    {
        this.sysfsRoot = sysfsRoot;
    }


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
