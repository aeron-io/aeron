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
import org.agrona.collections.IntHashSet;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import static io.aeron.topology.CGroupValidator.DEFAULT_SYSFS_ROOT;

class ThreadAlignmentValidator implements TopologyValidator
{

    /**
     * Path, relative to a per-CPU {@code sysfs} directory, of the file listing the CPUs that are siblings.
     */
    public static final String THREAD_SIBLING_LIST = "topology/thread_siblings_list";
    private final PerCpuListReader perCpuListReader;

    ThreadAlignmentValidator()
    {
        this(DEFAULT_SYSFS_ROOT);
    }

    ThreadAlignmentValidator(final Path sysfsRoot)
    {
        perCpuListReader = new PerCpuListReader(sysfsRoot, THREAD_SIBLING_LIST);
    }

    IntArrayList identifyMissingThreads(final IntArrayList cpuList) throws IOException
    {
        final IntHashSet requiredThreads = new IntHashSet();
        final IntHashSet presentThreads = new IntHashSet();
        final IntArrayList missingThreads = new IntArrayList();
        for (int i = 0; i < cpuList.size(); i++)
        {
            final int cpu = cpuList.get(i);
            presentThreads.add(cpu);
            final IntHashSet siblings = perCpuListReader.loadCpuList(cpu);
            requiredThreads.addAll(siblings);
        }
        final IntHashSet difference = requiredThreads.difference(presentThreads);
        if (null == difference)
        {
            return missingThreads;
        }
        difference.forEachInt(missingThreads::add);
        return missingThreads;
    }

    private int findCoreCpu(final IntArrayList cpuList, final int missingSibling) throws IOException
    {
        for (int i = 0; i < cpuList.size(); i++)
        {
            final int cpu = cpuList.get(i);
            if (perCpuListReader.loadCpuList(cpu).contains(missingSibling))
            {
                return cpu;
            }
        }
        return -1;
    }

    public int validate(final Cpuset cpuset, final PrintStream warningStream)
    {
        try
        {
            final IntArrayList cpuList = cpuset.cpus();
            final IntArrayList missingThreads = identifyMissingThreads(cpuList);
            for (int i = 0; i < missingThreads.size(); i++)
            {
                final int missingThread = missingThreads.get(i);
                final int coreCpu = findCoreCpu(cpuList, missingThread);
                warningStream.printf(
                    "WARNING: %s is missing sibling CPU(s) %d of the core containing CPU %d (partial core in cpuset)%n",
                    "cpuset", missingThread, coreCpu);
            }
            return missingThreads.size();
        }
        catch (final IOException e)
        {
            warningStream.println("Could not load CPU sibling list");
        }
        return 0;
    }
}
