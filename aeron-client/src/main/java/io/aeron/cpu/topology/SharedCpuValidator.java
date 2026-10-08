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

import java.io.PrintStream;

class SharedCpuValidator implements TopologyValidator
{
    public int validate(final CpuSelection selection, final PrintStream warningStream)
    {
        final IntArrayList cpus = selection.cpus();
        int warnings = 0;
        for (int i = 0; i < cpus.size(); i++)
        {
            for (int j = i + 1; j < cpus.size(); j++)
            {
                final int cpu = cpus.getInt(i);
                if (cpu == cpus.getInt(j))
                {
                    warningStream.printf("WARNING: %s and %s are sharing cpu=%d%n",
                        selection.label(i), selection.label(j), cpu);
                    warnings++;
                }
            }
        }

        return warnings;
    }
}
