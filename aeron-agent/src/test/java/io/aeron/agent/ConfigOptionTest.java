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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.aeron.agent.ConfigOption.*;
import static org.junit.jupiter.api.Assertions.*;

class ConfigOptionTest
{
    @Test
    void shouldReadSystemProperties()
    {
        final Map<String, String> expectedValues = new HashMap<>();
        try
        {
            expectedValues.put(DISABLED_ARCHIVE_EVENT_CODES, "abc");
            expectedValues.put(LOG_FILENAME, "");
            expectedValues.put(ENABLED_CLUSTER_EVENT_CODES, "1,2,3");
            for (final Map.Entry<String, String> entry : expectedValues.entrySet())
            {
                System.setProperty(entry.getKey(), entry.getValue());
            }
            System.setProperty("ignore me", "1000");

            final Map<String, String> values = fromSystemProperties();

            assertEquals("abc", values.get(DISABLED_ARCHIVE_EVENT_CODES));
            assertEquals("", values.get(LOG_FILENAME));
            assertEquals("1,2,3", values.get(ENABLED_CLUSTER_EVENT_CODES));
        }
        finally
        {
            System.clearProperty(DISABLED_ARCHIVE_EVENT_CODES);
            System.clearProperty(LOG_FILENAME);
            System.clearProperty(ENABLED_CLUSTER_EVENT_CODES);
            System.clearProperty("ignore me");
        }
    }

    @Test
    void shouldReturnAllSystemProperties()
    {
        final Map<String, String> values = fromSystemProperties();

        assertNotEquals(Collections.emptySet(), values);
    }

    @Test
    void shouldThrowNullPointerExceptionIfConfigOptionsMapIsNull()
    {
        assertThrows(NullPointerException.class, () -> buildAgentArgs(null));
    }

    @Test
    void shouldReturnNullIfConfigOptionsMapIsEmpty()
    {
        assertNull(buildAgentArgs(new HashMap<>()));
    }

    @Test
    void shouldCombineConfigOptionsIntoAnAgentArgsString()
    {
        final Map<String, String> configOptions = new LinkedHashMap<>();
        configOptions.put(READER_CLASSNAME, "reader class");
        configOptions.put(DISABLED_CLUSTER_EVENT_CODES, "CANVASS_POSITION");
        configOptions.put(ENABLED_DRIVER_EVENT_CODES, "all");
        configOptions.put(LOG_FILENAME, "file.txt");
        configOptions.put(DISABLED_DRIVER_EVENT_CODES, "admin");

        final String agentArgs = buildAgentArgs(configOptions);

        assertEquals(
            "aeron.event.log.reader.classname=reader class|" +
            "aeron.event.cluster.log.disable=CANVASS_POSITION|" +
            "aeron.event.log=all|" +
            "aeron.event.log.filename=file.txt|" +
            "aeron.event.log.disable=admin",
            agentArgs);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldThrowExceptionIfNullOrEmptyAgentArgs(final String agentArgs)
    {
        final IllegalArgumentException exception =
            assertThrows(IllegalArgumentException.class, () -> parseAgentArgs(agentArgs));
        assertEquals("cannot parse empty value", exception.getMessage());
    }

    @Test
    void shouldParseAgentArgsAsAConfigOptionsMap()
    {
        final Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put(LOG_FILENAME, "log.out");
        expectedOptions.put(READER_CLASSNAME, "my reader");
        expectedOptions.put(ENABLED_DRIVER_EVENT_CODES, "all");
        expectedOptions.put(DISABLED_DRIVER_EVENT_CODES, "FRAME_IN,FRAME_OUT");
        expectedOptions.put(ENABLED_CLUSTER_EVENT_CODES, "all");
        expectedOptions.put(DISABLED_CLUSTER_EVENT_CODES, "CANVASS_POSITION,STATE_CHANGE");

        final Map<String, String> configOptions = parseAgentArgs(
            "||1600|abc|aeron.event.log.filename=log.out|" +
            "aeron.event.cluster.log=all|" +
            "aeron.event.log.reader.classname=my reader|" +
            "aeron.event.log.disable=FRAME_IN,FRAME_OUT|" +
            "aeron.event.log=all|" +
            "aeron.event.cluster.log.disable=CANVASS_POSITION,STATE_CHANGE");

        assertEquals(expectedOptions, configOptions);
    }

    @Test
    void shouldParseAgentArgsThatEndWithExtraSeparators()
    {
        final Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put(ENABLED_ARCHIVE_EVENT_CODES, "REPLICATION_SESSION_STATE_CHANGE,CMD_IN_START_RECORDING");

        final Map<String, String> configOptions = parseAgentArgs(
            "aeron.event.archive.log=REPLICATION_SESSION_STATE_CHANGE,CMD_IN_START_RECORDING||||");

        assertEquals(expectedOptions, configOptions);
    }
}
