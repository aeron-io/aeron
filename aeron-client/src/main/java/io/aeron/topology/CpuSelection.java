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

import io.aeron.topology.AffinityRegistry.CoreClaim;
import org.agrona.collections.IntArrayList;

import java.util.List;
import java.util.StringJoiner;

/**
 * A selection of CPUs to be validated against the CPU topology, along with the information needed to report on it.
 */
interface CpuSelection
{
    /**
     * CPUs in the selection, which may contain duplicates.
     *
     * @return the CPUs in the selection.
     */
    IntArrayList cpus();

    /**
     * The kind of selection, used as the subject of warnings.
     *
     * @return the kind of selection.
     */
    String kind();

    /**
     * Label for the CPU at the given index.
     *
     * @param index into {@link #cpus()}.
     * @return the label for the CPU.
     */
    String label(int index);

    /**
     * The formatted configuration of the whole selection.
     *
     * @return the formatted configuration.
     */
    String configuration();

    /**
     * Selection of the CPUs in a process's effective cgroup cpuset.
     *
     * @param cpuset the cpuset.
     */
    record CpusetSelection(Cpuset cpuset) implements CpuSelection
    {
        public IntArrayList cpus()
        {
            return cpuset.cpus();
        }

        public String kind()
        {
            return "cpuset";
        }

        public String label(final int index)
        {
            return Integer.toString(cpuset.cpus().getInt(index));
        }

        public String configuration()
        {
            return cpuset.formattedCpus();
        }
    }

    /**
     * Selection of the CPUs claimed by named thread affinities.
     */
    final class AffinitySelection implements CpuSelection
    {
        private final List<CoreClaim> claims;
        private final IntArrayList cpus = new IntArrayList();
        private final String configuration;

        AffinitySelection(final List<CoreClaim> claims)
        {
            this.claims = List.copyOf(claims);
            final StringJoiner joiner = new StringJoiner(", ", "[", "]");
            for (final CoreClaim claim : this.claims)
            {
                cpus.addInt(claim.cpu());
                joiner.add(claim.name() + "=" + claim.cpu());
            }
            configuration = joiner.toString();
        }

        public IntArrayList cpus()
        {
            return cpus;
        }

        public String kind()
        {
            return "affinity";
        }

        public String label(final int index)
        {
            return claims.get(index).name();
        }

        public String configuration()
        {
            return configuration;
        }
    }
}
