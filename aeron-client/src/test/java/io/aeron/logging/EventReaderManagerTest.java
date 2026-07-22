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
package io.aeron.logging;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventReaderManagerTest
{
    @Test
    void shouldCheckLoggingEnabled()
    {
        final Properties properties = new Properties();
        assertFalse(EventReaderManager.isLoggingEnabled(properties));

        properties.put("aeron.event.log", "none");
        properties.put("aeron.event.archive.log", "none");
        properties.put("aeron.event.cluster.log", "none");
        assertFalse(EventReaderManager.isLoggingEnabled(properties));

        properties.clear();
        properties.put("aeron.event.log", "all");
        assertTrue(EventReaderManager.isLoggingEnabled(properties));

        properties.clear();
        properties.put("aeron.event.archive.log", "all");
        assertTrue(EventReaderManager.isLoggingEnabled(properties));

        properties.clear();
        properties.put("aeron.event.cluster.log", "all");
        assertTrue(EventReaderManager.isLoggingEnabled(properties));

        properties.clear();
        properties.put("aeron.event.anything.log", "whatever");
        assertTrue(EventReaderManager.isLoggingEnabled(properties));
    }
}