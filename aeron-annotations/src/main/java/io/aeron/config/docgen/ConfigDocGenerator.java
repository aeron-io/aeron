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
package io.aeron.config.docgen;

import io.aeron.config.ConfigInfo;
import io.aeron.config.DefaultType;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

final class ConfigDocGenerator implements AutoCloseable
{
    static void generate(final List<ConfigInfo> configInfoCollection, final String outputFilename) throws Exception
    {
        try (ConfigDocGenerator generator = new ConfigDocGenerator(outputFilename))
        {
            generator.generateDoc(configInfoCollection);
        }
    }

    private final FileWriter writer;

    private ConfigDocGenerator(final String outputFile) throws Exception
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

    private void generateDoc(final List<ConfigInfo> configInfoCollection) throws Exception
    {
        for (final ConfigInfo configInfo: sort(configInfoCollection))
        {
            writeHeader(
                toHeaderString(configInfo.id) +
                (configInfo.expectations.c.exists ? "" : " *(JAVA ONLY)*") +
                (configInfo.existsInJava ? "" : " *(C ONLY)*") +
                (configInfo.deprecated ? " *(DEPRECATED)*" : ""));
            write("Description", configInfo.propertyNameDescriptionClean != null ?
                configInfo.propertyNameDescriptionClean : configInfo.propertyNameDescription);
            write("Type",
                (DefaultType.isUndefined(configInfo.overrideDefaultValueType) ?
                configInfo.defaultValueType :
                configInfo.overrideDefaultValueType).getSimpleName());
            if (configInfo.existsInJava)
            {
                writeCode("System Property", configInfo.propertyName);
            }
            if (configInfo.context != null && !configInfo.context.isEmpty())
            {
                writeCode("Context", configInfo.context);
            }
            if (configInfo.contextDescription != null && !configInfo.contextDescription.isEmpty())
            {
                write("Context Description", configInfo.contextDescriptionClean != null ?
                    configInfo.contextDescriptionClean : configInfo.contextDescription);
            }
            if (configInfo.uriParam != null && !configInfo.uriParam.isEmpty())
            {
                writeCode("URI Param", configInfo.uriParam);
            }

            if (configInfo.defaultDescription != null)
            {
                write("Default Description", configInfo.defaultDescriptionClean != null ?
                    configInfo.defaultDescriptionClean : configInfo.defaultDescription);
            }
            final String defaultValue = configInfo.overrideDefaultValue == null ?
                (configInfo.defaultValue == null ? "" : configInfo.defaultValue) :
                configInfo.overrideDefaultValue;

            if (configInfo.existsInJava)
            {
                write("Default", getDefaultString(
                    configInfo.defaultValueString == null ? defaultValue : configInfo.defaultValueString,
                    configInfo.isTimeValue,
                    configInfo.timeUnit));
            }
            if (configInfo.isTimeValue == Boolean.TRUE)
            {
                write("Time Unit", configInfo.timeUnit.toString());
            }

            if (configInfo.expectations.c.exists)
            {
                writeCode("C Env Var", configInfo.expectations.c.envVar);
                write("C Default", getDefaultString(
                    configInfo.expectations.c.defaultValue,
                    configInfo.isTimeValue,
                    configInfo.timeUnit));
            }
            writeLine();
        }
    }

    private List<ConfigInfo> sort(final List<ConfigInfo> config)
    {
        return config
            .stream()
            .sorted(Comparator.comparing(a -> a.id))
            .collect(Collectors.toList());
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
        writer.write("| " + a + "\n");
        writer.write("| " + b.replaceAll("\n", " ").trim() + "\n\n");
    }

    private void writeLine() throws IOException
    {
        writer.write("|===\n\n");
    }

    private String toHeaderString(final String t)
    {
        final StringBuilder builder = new StringBuilder();

        char previous = '_';
        for (final char next: t.toCharArray())
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

    private String getDefaultString(
        final String defaultValue,
        final boolean isTimeValue,
        final TimeUnit timeUnit) throws Exception
    {
        if (defaultValue != null && !defaultValue.isEmpty() && defaultValue.chars().allMatch(Character::isDigit))
        {
            final long defaultLong;

            try
            {
                defaultLong = Long.parseLong(defaultValue);
            }
            catch (final NumberFormatException nfe)
            {
                // This shouldn't be possible since we've already validated that every character is a digit
                throw new Exception(nfe);
            }

            final StringBuilder builder = new StringBuilder();

            builder.append(defaultValue);

            if (defaultValue.length() > 3)
            {
                builder.append(" (");
                builder.append(DecimalFormat.getNumberInstance().format(defaultLong));
                builder.append(")");

                int kCount = 0;
                long remainingValue = defaultLong;
                while (remainingValue % 1024 == 0)
                {
                    kCount++;
                    remainingValue = remainingValue / 1024;
                }

                if (kCount > 0 && remainingValue < 1024)
                {
                    builder.append(" (");
                    builder.append(remainingValue);
                    IntStream.range(0, kCount).forEach(i -> builder.append(" * 1024"));
                    builder.append(")");
                }
            }

            if (isTimeValue)
            {
                int tCount = 0;

                long remaining = timeUnit.toNanos(defaultLong);
                while (remaining % 1000 == 0 && tCount < 3)
                {
                    tCount++;
                    remaining = remaining / 1000;
                }
                builder.append(" (");
                builder.append(remaining);

                switch (tCount)
                {
                    case 0:
                        builder.append(" nano");
                        break;
                    case 1:
                        builder.append(" micro");
                        break;
                    case 2:
                        builder.append(" milli");
                        break;
                    case 3:
                        builder.append(" ");
                        break;
                }
                builder.append("second");
                if (remaining != 1)
                {
                    builder.append("s");
                }
                builder.append(")");
            }

            return builder.toString();
        }
        return defaultValue;
    }
}
