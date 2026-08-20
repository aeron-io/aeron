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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Reads the effective CPU set of a process from a cgroup v2 {@code cpuset.cpus.effective} file.
 */
public class CpusetV2Reader
{
    private final Path procRoot;
    private final Path cgroupRoot;

    /**
     * Creates a reader using the standard {@code /proc} and {@code /sys/fs/cgroup} paths.
     */
    public CpusetV2Reader()
    {
        // TODO: Move to constants
        this(Path.of("/proc"), Path.of("/sys/fs/cgroup"));
    }

    CpusetV2Reader(final Path procRoot, final Path cgroupRoot)
    {
        this.procRoot = procRoot;
        this.cgroupRoot = cgroupRoot;
    }

    private Path retrieveEffectiveCgroupFilePath(final String pid) throws IOException
    {
        final Path procCgroupFilePath = procRoot.resolve(pid + "/cgroup");
        try (Stream<String> lines = Files.lines(procCgroupFilePath))
        {
            final String effectiveCgroupPathStr = lines.filter(
                line -> line.startsWith("0::/")).findFirst().orElseThrow();
            return cgroupRoot.resolve(effectiveCgroupPathStr.substring(4));
        }
    }

    private Cpuset readCpuSet(final String pid)
    {
        try
        {
            final Path effectiveCgroupFilePath = retrieveEffectiveCgroupFilePath(pid);
            final String cpuGroups = Files.readString(effectiveCgroupFilePath);
            return new Cpuset(AffinityParser.parse(cpuGroups));
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads the effective CPU set of the current process.
     *
     * @return the current process's effective CPU set.
     */
    public Cpuset readCpuSet()
    {
        return readCpuSet("self");
    }

    /**
     * Reads the effective CPU set of the given process.
     *
     * @param pid the process id to read the effective CPU set of.
     * @return the process's effective CPU set.
     */
    public Cpuset readCpuSet(final int pid)
    {
        return readCpuSet(Integer.toString(pid));
    }

}
