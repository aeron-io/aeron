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
import java.nio.file.Path;

/**
 * Checks for missing thread siblings according to the {@link ThreadAlignmentChecker#THREAD_SIBLING_LIST} file.
 */
public class ThreadAlignmentChecker
{
    /**
     * Path, relative to a per-CPU {@code sysfs} directory, of the file listing the CPUs that are siblings.
     */
    public static final String THREAD_SIBLING_LIST = "topology/thread_siblings_list";
    private final PerCpuListReader perCpuListReader;

    /**
     * Creates a {@link ThreadAlignmentChecker} that reads CPU topology information from the given {@code sysfs} root.
     *
     * @param sysfsRoot the root {@code sysfs} CPU topology directory, e.g. {@code /sys/devices/system/cpu}.
     */
    public ThreadAlignmentChecker(final Path sysfsRoot)
    {
        perCpuListReader = new PerCpuListReader(sysfsRoot, THREAD_SIBLING_LIST);
    }

    /**
     * Lists missing thread siblings according to the {@link ThreadAlignmentChecker#THREAD_SIBLING_LIST} file.
     *
     * @param cpuList the list of CPUs to check for missing siblings
     * @return the list of missing threads
     * @throws IOException if an I/O error occurs while reading the thread sibling list
     */
    public IntArrayList identifyMissingThreads(final IntArrayList cpuList) throws IOException
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
}
