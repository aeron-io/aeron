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
package io.aeron.agent;

import org.agrona.Strings;
import org.agrona.concurrent.Agent;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A set of configuration options.
 */
final class ConfigOption
{
    /**
     * Event Buffer log file name system property. If not set then output will default to {@link System#out}.
     */
    static final String LOG_FILENAME = "aeron.event.log.filename";

    /**
     * Event Buffer log file max length system property. If not set then {@link Long#MAX_VALUE} will be used.
     */
    static final String LOG_FILE_MAX_LENGTH = "aeron.event.log.file.max.length";

    /**
     * Event reader {@link Agent} which consumes the {@link EventConfiguration#EVENT_RING_BUFFER} to output log events.
     */
    static final String READER_CLASSNAME = "aeron.event.log.reader.classname";

    /**
     * Driver Event tags system property. This is either:
     * <ul>
     * <li>A comma separated list of {@link DriverEventCode}s to enable.</li>
     * <li>{@code all} which enables all driver events.</li>
     * <li>{@code admin} which enables all driver events except for {@link DriverEventCode#FRAME_IN},
     * {@link DriverEventCode#FRAME_OUT}, {@link DriverEventCode#NAME_RESOLUTION_NEIGHBOR_ADDED},
     * {@link DriverEventCode#NAME_RESOLUTION_NEIGHBOR_REMOVED}.</li>
     * </ul>
     */
    static final String ENABLED_DRIVER_EVENT_CODES = "aeron.event.log";

    /**
     * Disabled Driver Event tags system property. Follows the format specified for
     * {@link #ENABLED_DRIVER_EVENT_CODES}. This property will disable any codes in the set
     * specified there. Defined on its own has no effect.
     */
    static final String DISABLED_DRIVER_EVENT_CODES = "aeron.event.log.disable";

    /**
     * Archive Event tags system property. This is either:
     * <ul>
     * <li>A comma separated list of {@link ArchiveEventCode}s to enable.</li>
     * <li>{@code all} which enables all the codes.</li>
     * </ul>
     */
    static final String ENABLED_ARCHIVE_EVENT_CODES = "aeron.event.archive.log";

    /**
     * Disabled Archive Event tags system property. Follows the format specified for
     * {@link #ENABLED_ARCHIVE_EVENT_CODES}. This property will disable any codes in the
     * set specified there. Defined on its own has no effect.
     */
    static final String DISABLED_ARCHIVE_EVENT_CODES = "aeron.event.archive.log.disable";

    /**
     * Cluster Event tags system property. This is either:
     * <ul>
     * <li>A comma separated list of {@link ClusterEventCode}s to enable.</li>
     * <li>{@code all} which enables all the codes.</li>
     * </ul>
     */
    static final String ENABLED_CLUSTER_EVENT_CODES = "aeron.event.cluster.log";

    /**
     * Disabled Cluster Event tags system property name. Follows the format specified for
     * {@link #ENABLED_CLUSTER_EVENT_CODES}. This property will disable any codes in the
     * set specified there. Defined on its own has no effect.
     */
    static final String DISABLED_CLUSTER_EVENT_CODES = "aeron.event.cluster.log.disable";

    static final String START_COMMAND = "start";
    static final String STOP_COMMAND = "stop";

    private static final char VALUE_SEPARATOR = '=';
    private static final char OPTION_SEPARATOR = '|';

    static Map<String, String> fromSystemProperties()
    {
        final HashMap<String, String> result = new HashMap<>();
        final Properties properties = System.getProperties();
        for (final Map.Entry<Object, Object> entry : properties.entrySet())
        {
            result.put((String)entry.getKey(), (String)entry.getValue());
        }
        return result;
    }

    static String buildAgentArgs(final Map<String, String> configOptions)
    {
        if (configOptions.isEmpty())
        {
            return null;
        }

        final StringBuilder builder = new StringBuilder();
        for (final Map.Entry<String, String> entry : configOptions.entrySet())
        {
            builder.append(entry.getKey())
                .append(VALUE_SEPARATOR)
                .append(entry.getValue())
                .append(OPTION_SEPARATOR);
        }

        if (builder.length() > 0)
        {
            builder.setLength(builder.length() - 1);
        }

        return builder.toString();
    }

    static Map<String, String> parseAgentArgs(final String agentArgs)
    {
        if (Strings.isEmpty(agentArgs))
        {
            throw new IllegalArgumentException("cannot parse empty value");
        }

        final Map<String, String> values = new HashMap<>();

        int optionIndex = -1;
        do
        {
            final int valueIndex = agentArgs.indexOf(VALUE_SEPARATOR, optionIndex);
            if (valueIndex <= 0)
            {
                break;
            }

            int nameIndex = -1;
            while (optionIndex < valueIndex)
            {
                nameIndex = optionIndex;
                optionIndex = agentArgs.indexOf(OPTION_SEPARATOR, optionIndex + 1);
                if (optionIndex < 0)
                {
                    break;
                }
            }

            final String optionName = agentArgs.substring(nameIndex + 1, valueIndex);
            final String value = agentArgs.substring(
                valueIndex + 1,
                optionIndex > 0 ? optionIndex : agentArgs.length());
            values.put(optionName, value);
        }
        while (optionIndex > 0);

        return values;
    }
}
