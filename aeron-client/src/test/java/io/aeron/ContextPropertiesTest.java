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
package io.aeron;

import org.agrona.SystemUtil;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static io.aeron.ContextProperties.getBoolean;
import static io.aeron.ContextProperties.getDurationInNanos;
import static io.aeron.ContextProperties.getInteger;
import static io.aeron.ContextProperties.getLong;
import static io.aeron.ContextProperties.getNullableProperty;
import static io.aeron.ContextProperties.getProperty;
import static io.aeron.ContextProperties.getSizeAsInt;
import static io.aeron.ContextProperties.getSizeAsLong;
import static io.aeron.ContextProperties.newContext;
import static io.aeron.test.TestPropertiesUtil.backupAndSetSystemProperties;
import static io.aeron.test.TestPropertiesUtil.restoreSystemProperties;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ContextPropertiesTest
{
    @Test
    void shouldReadSystemPropertiesAsBeforeWhenNoPropertiesAreBound()
    {
        final Properties backup = new Properties();
        backupAndSetSystemProperties(
            backup,
            "test.context.properties.string", "value",
            "test.context.properties.null", SystemUtil.NULL_PROPERTY_VALUE,
            "test.context.properties.int", "0x10",
            "test.context.properties.invalid", "invalid",
            "test.context.properties.long", "-42",
            "test.context.properties.boolean", "TRUE",
            "test.context.properties.size", "64k",
            "test.context.properties.duration", "5ms");
        try
        {
            assertNull(ContextProperties.current());
            assertEquals(
                System.getProperty("test.context.properties.string"),
                getProperty("test.context.properties.string"));
            assertEquals(
                System.getProperty("test.context.properties.missing", "default"),
                getProperty("test.context.properties.missing", "default"));
            assertEquals(
                SystemUtil.getProperty("test.context.properties.null", "default"),
                getNullableProperty("test.context.properties.null", "default"));
            assertEquals(
                Integer.getInteger("test.context.properties.int", 7),
                getInteger("test.context.properties.int", 7));
            assertEquals(
                Integer.getInteger("test.context.properties.invalid", 7),
                getInteger("test.context.properties.invalid", 7));
            assertEquals(
                Long.getLong("test.context.properties.long", 7L),
                getLong("test.context.properties.long", 7L));
            assertEquals(
                Long.getLong("test.context.properties.missing", (Long)null),
                getLong("test.context.properties.missing", null));
            assertEquals(
                Boolean.getBoolean("test.context.properties.boolean"),
                getBoolean("test.context.properties.boolean"));
            assertEquals(
                SystemUtil.getSizeAsInt("test.context.properties.size", 1),
                getSizeAsInt("test.context.properties.size", 1));
            assertEquals(
                SystemUtil.getSizeAsLong("test.context.properties.size", 1),
                getSizeAsLong("test.context.properties.size", 1));
            assertEquals(
                SystemUtil.getDurationInNanos("test.context.properties.duration", 1),
                getDurationInNanos("test.context.properties.duration", 1));
        }
        finally
        {
            restoreSystemProperties(backup);
        }
    }

    @Test
    void shouldReadBoundPropertiesInsteadOfSystemProperties()
    {
        final Properties backup = new Properties();
        backupAndSetSystemProperties(
            backup,
            "test.context.properties.string", "system",
            "test.context.properties.size", "64k");
        try
        {
            final Properties properties = new Properties();
            properties.setProperty("test.context.properties.size", "1m");

            ContextProperties.run(
                properties,
                () ->
                {
                    assertSame(properties, ContextProperties.current());
                    assertEquals(1024 * 1024, getSizeAsInt("test.context.properties.size", 1));
                    assertNull(getProperty("test.context.properties.string"));
                });

            assertNull(ContextProperties.current());
            assertEquals("system", getProperty("test.context.properties.string"));
        }
        finally
        {
            restoreSystemProperties(backup);
        }
    }

    @Test
    void shouldRestorePreviousPropertiesWhenCreatingContextFails()
    {
        final Properties outer = new Properties();
        outer.setProperty("test.context.properties.string", "outer");
        final Properties inner = new Properties();
        inner.setProperty("test.context.properties.string", "inner");

        ContextProperties.run(
            outer,
            () ->
            {
                assertThrows(
                    IllegalStateException.class,
                    () -> newContext(
                        inner,
                        () ->
                        {
                            assertEquals("inner", getProperty("test.context.properties.string"));
                            throw new IllegalStateException("failed to create");
                        }));

                assertSame(outer, ContextProperties.current());
                assertEquals("outer", getProperty("test.context.properties.string"));
            });

        assertNull(ContextProperties.current());
    }

    @Test
    void shouldReadJvmWideSettingsFromSystemPropertiesWhenPropertiesAreBound()
    {
        final Properties backup = new Properties();
        backupAndSetSystemProperties(
            backup,
            CommonContext.THREAD_NAMING_PROP_NAME, CommonContext.THREAD_NAMING_NEW,
            CommonContext.FALLBACK_LOGGER_PROP_NAME, "stdout");
        try
        {
            final Properties properties = new Properties();
            properties.setProperty(CommonContext.THREAD_NAMING_PROP_NAME, CommonContext.THREAD_NAMING_CLASSIC);
            properties.setProperty(CommonContext.FALLBACK_LOGGER_PROP_NAME, "no_op");

            ContextProperties.run(
                properties,
                () ->
                {
                    assertEquals("new-name", CommonContext.threadName("new-name", "classic-name"));
                    assertSame(System.out, CommonContext.fallbackLogger());
                });
        }
        finally
        {
            restoreSystemProperties(backup);
        }
    }

    @Test
    void shouldCreateAeronContextFromSuppliedProperties()
    {
        final Properties backup = new Properties();
        backupAndSetSystemProperties(
            backup,
            CommonContext.AERON_DIR_PROP_NAME, "/tmp/aeron-system",
            CommonContext.DRIVER_TIMEOUT_PROP_NAME, "20000",
            Aeron.Configuration.CLIENT_NAME_PROP_NAME, "system",
            Aeron.Configuration.IDLE_SLEEP_DURATION_PROP_NAME, "20ms");
        try
        {
            final Properties properties = new Properties();
            properties.setProperty(CommonContext.AERON_DIR_PROP_NAME, "/tmp/aeron-a");
            properties.setProperty(CommonContext.DRIVER_TIMEOUT_PROP_NAME, "15000");
            properties.setProperty(CommonContext.ENABLE_EXPERIMENTAL_FEATURES_PROP_NAME, "true");
            properties.setProperty(Aeron.Configuration.CLIENT_NAME_PROP_NAME, "a");
            properties.setProperty(Aeron.Configuration.PRE_TOUCH_MAPPED_MEMORY_PROP_NAME, "true");

            final Aeron.Context context = newContext(properties, Aeron.Context::new);

            assertEquals("/tmp/aeron-a", context.aeronDirectoryName());
            assertEquals(15000, context.driverTimeoutMs());
            assertTrue(context.enableExperimentalFeatures());
            assertEquals("a", context.clientName());
            assertTrue(context.preTouchMappedMemory());
            assertEquals(Aeron.Configuration.IDLE_SLEEP_DEFAULT_NS, context.idleSleepDurationNs());

            final Aeron.Context systemContext = new Aeron.Context();

            assertEquals("/tmp/aeron-system", systemContext.aeronDirectoryName());
            assertEquals(20000, systemContext.driverTimeoutMs());
            assertFalse(systemContext.enableExperimentalFeatures());
            assertEquals("system", systemContext.clientName());
            assertEquals(TimeUnit.MILLISECONDS.toNanos(20), systemContext.idleSleepDurationNs());
        }
        finally
        {
            restoreSystemProperties(backup);
        }
    }
}
