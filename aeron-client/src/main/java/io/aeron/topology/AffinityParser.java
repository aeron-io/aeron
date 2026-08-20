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

import java.util.Collection;
import java.util.function.Supplier;

public class AffinityParser
{
    public static IntArrayList parse(final String affinity)
    {
        return parse(affinity, IntArrayList::new);
    }

    public static <T extends Collection<Integer>> T parse(final String affinity, Supplier<T> collectionSupplier)
    {
        final T result = collectionSupplier.get();
        for (String part : affinity.split(","))
        {
            part = part.trim();
            if (part.contains("-"))
            {
                final String[] range = part.split("-");
                final int start = Integer.parseInt(range[0]);
                final int end = Integer.parseInt(range[1]);
                for (int cpu = start; cpu < end + 1; cpu++)
                {
                    result.add(cpu);
                }
            }
            else
            {
                result.add(Integer.parseInt(part));
            }
        }
        return result;
    }
}
