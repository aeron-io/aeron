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

import java.util.function.IntConsumer;

import static org.agrona.AsciiEncoding.parseIntAscii;

/**
 * Parses a Linux-style CPU affinity/CPU list string into a collection of CPU ids.
 */
public final class AffinityParser
{
    private AffinityParser()
    {
    }

    /**
     * Parses a CPU affinity list into an {@link IntArrayList}.
     *
     * @param affinity the CPU affinity list, e.g. {@code "0-3,7"}.
     * @return the parsed CPU ids.
     */
    public static IntArrayList parse(final String affinity)
    {
        final IntArrayList result = new IntArrayList();
        parse(affinity, result::addInt);
        return result;
    }

    /**
     * Parses a CPU affinity list, passing each CPU id to the given consumer.
     *
     * @param affinity the CPU affinity list, e.g. {@code "0-3,7"}.
     * @param consumer receives each parsed CPU id.
     */
    public static void parse(final String affinity, final IntConsumer consumer)
    {
        for (final String rawPart : affinity.split(","))
        {
            final String part = rawPart.trim();
            final int dashIndex = part.indexOf('-');
            if (-1 != dashIndex)
            {
                final int start = parseIntAscii(part, 0, dashIndex);
                final int end = parseIntAscii(part, dashIndex + 1, part.length() - (dashIndex + 1));
                for (int cpu = start; cpu <= end; cpu++)
                {
                    consumer.accept(cpu);
                }
            }
            else
            {
                consumer.accept(Integer.parseInt(part));
            }
        }
    }
}
