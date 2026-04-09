/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.counter.docgen;

import io.aeron.counter.CounterInfo;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

final class CounterDocGenerator implements AutoCloseable
{
    static void generate(final List<CounterInfo> counterInfoCollection, final String outputFilename) throws Exception
    {
        try (CounterDocGenerator generator = new CounterDocGenerator(outputFilename))
        {
            generator.generateDoc(counterInfoCollection);
        }
    }

    private final FileWriter writer;

    private CounterDocGenerator(final String outputFile) throws Exception
    {
        writer = new FileWriter(outputFile);
    }

    @Override
    public void close()
    {
        try
        {
            writer.close();
        }
        catch (final Exception e)
        {
            e.printStackTrace(System.err);
        }
    }

    private void generateDoc(final List<CounterInfo> counterInfoCollection) throws Exception
    {
        final List<CounterInfo> typeIdCounters = sortById(counterInfoCollection.stream()
            .filter(c -> !c.isSystemCounter)
            .collect(Collectors.toList()));

        final List<CounterInfo> systemCounters = sortById(counterInfoCollection.stream()
            .filter(c -> c.isSystemCounter)
            .collect(Collectors.toList()));

        writeSectionHeader("Counter Types");
        for (final CounterInfo counterInfo : typeIdCounters)
        {
            writeCounterEntry(counterInfo);
        }

        writeSectionHeader("System Counters");
        for (final CounterInfo counterInfo : systemCounters)
        {
            writeCounterEntry(counterInfo);
        }
    }

    private void writeCounterEntry(final CounterInfo counterInfo) throws Exception
    {
        writeHeader(
            toHeaderString(counterInfo.name) +
            (counterInfo.existsInC ? "" : " *(JAVA ONLY)*"));
        write("ID", String.valueOf(counterInfo.id));
        write("Description", counterInfo.counterDescription);
        if (counterInfo.counterDescriptionClean != null)
        {
            write("Description (clean)", counterInfo.counterDescriptionClean);
        }
        if (counterInfo.javaFieldName != null)
        {
            writeCode("Java Field", counterInfo.javaFieldName);
        }
        if (counterInfo.existsInC)
        {
            writeCode("C Name", counterInfo.expectedCName);
        }
        writeLine();
    }

    private List<CounterInfo> sortById(final List<CounterInfo> counters)
    {
        return counters
            .stream()
            .sorted(Comparator.comparingInt(a -> a.id))
            .collect(Collectors.toList());
    }

    private void writeSectionHeader(final String title) throws IOException
    {
        writer.write("== " + title + "\n\n");
    }

    private void writeHeader(final String t) throws IOException
    {
        writer.write("=== " + t + "\n\n");
        writer.write("[cols=\"1h,3\"]\n");
        writer.write("|===\n");
    }

    private void writeCode(final String a, final String b) throws IOException
    {
        write(a, "`" + b + "`");
    }

    private void write(final String a, final String b) throws IOException
    {
        final String value = b == null ? "" : b.replaceAll("\n", " ").trim();
        writer.write("| " + a + "\n");
        writer.write("| " + value + "\n\n");
    }

    private void writeLine() throws IOException
    {
        writer.write("|===\n\n");
    }

    private String toHeaderString(final String t)
    {
        final StringBuilder builder = new StringBuilder();

        char previous = '_';
        for (final char next : t.toCharArray())
        {
            if (next == '_')
            {
                builder.append(' ');
            }
            else if (previous == '_')
            {
                builder.append(Character.toUpperCase(next));
            }
            else
            {
                builder.append(Character.toLowerCase(next));
            }
            previous = next;
        }
        return builder.toString();
    }
}
