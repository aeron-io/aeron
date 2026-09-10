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
package io.aeron.driver;

import io.aeron.CommonContext;
import io.aeron.driver.MediaDriver.Context;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static io.aeron.CommonContext.AERON_DIR_PROP_NAME;
import static io.aeron.driver.Configuration.CLIENT_LIVENESS_TIMEOUT_PROP_NAME;
import static io.aeron.driver.Configuration.DIR_DELETE_ON_START_PROP_NAME;
import static io.aeron.driver.Configuration.MTU_LENGTH_PROP_NAME;
import static io.aeron.driver.Configuration.RESOLVER_NAME_PROP_NAME;
import static io.aeron.driver.Configuration.TERM_BUFFER_LENGTH_PROP_NAME;
import static io.aeron.driver.Configuration.THREADING_MODE_PROP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MediaDriverContextPropertiesTest
{
    @AfterEach
    void tearDown()
    {
        System.clearProperty(MTU_LENGTH_PROP_NAME);
        System.clearProperty(TERM_BUFFER_LENGTH_PROP_NAME);
    }

    @Test
    void suppliedPropertiesConfigureTheContext()
    {
        final Properties properties = new Properties();
        properties.setProperty(AERON_DIR_PROP_NAME, "/tmp/aeron-a");
        properties.setProperty(MTU_LENGTH_PROP_NAME, "4k");
        properties.setProperty(TERM_BUFFER_LENGTH_PROP_NAME, "128k");
        properties.setProperty(CLIENT_LIVENESS_TIMEOUT_PROP_NAME, "7s");
        properties.setProperty(DIR_DELETE_ON_START_PROP_NAME, "true");
        properties.setProperty(RESOLVER_NAME_PROP_NAME, "node-a");

        final Context context = new Context(properties);

        assertSame(properties, context.properties());
        assertEquals("/tmp/aeron-a", context.aeronDirectoryName());
        assertEquals(4 * 1024, context.mtuLength());
        assertEquals(128 * 1024, context.publicationTermBufferLength());
        assertEquals(TimeUnit.SECONDS.toNanos(7), context.clientLivenessTimeoutNs());
        assertTrue(context.dirDeleteOnStart());
        assertEquals("node-a", context.resolverName());
    }

    @Test
    void twoContextsFromDifferentPropertiesAreIndependent()
    {
        final Properties a = new Properties();
        a.setProperty(AERON_DIR_PROP_NAME, "/tmp/aeron-a");
        a.setProperty(MTU_LENGTH_PROP_NAME, "1408");

        final Properties b = new Properties();
        b.setProperty(AERON_DIR_PROP_NAME, "/tmp/aeron-b");
        b.setProperty(MTU_LENGTH_PROP_NAME, "8k");

        final Context contextA = new Context(a);
        final Context contextB = new Context(b);

        assertEquals("/tmp/aeron-a", contextA.aeronDirectoryName());
        assertEquals("/tmp/aeron-b", contextB.aeronDirectoryName());
        assertEquals(1408, contextA.mtuLength());
        assertEquals(8 * 1024, contextB.mtuLength());
        assertNotEquals(contextA.aeronDirectoryName(), contextB.aeronDirectoryName());
    }

    @Test
    void suppliedPropertiesShadowSystemPropertiesAndAreNotWrittenBackToThem()
    {
        System.setProperty(MTU_LENGTH_PROP_NAME, "1408");

        final Properties properties = new Properties();
        properties.setProperty(MTU_LENGTH_PROP_NAME, "8k");

        assertEquals(8 * 1024, new Context(properties).mtuLength());
        assertEquals(1408, new Context().mtuLength());
        assertEquals("1408", System.getProperty(MTU_LENGTH_PROP_NAME));
    }

    @Test
    void emptyPropertiesYieldTheDocumentedDefaults()
    {
        final Context context = new Context(new Properties());

        assertEquals(Configuration.MTU_LENGTH_DEFAULT, context.mtuLength());
        assertEquals(Configuration.TERM_BUFFER_LENGTH_DEFAULT, context.publicationTermBufferLength());
        assertEquals(Configuration.CLIENT_LIVENESS_TIMEOUT_DEFAULT_NS, context.clientLivenessTimeoutNs());
        assertFalse(context.dirDeleteOnStart());
        assertEquals(CommonContext.AERON_DIR_PROP_DEFAULT, context.aeronDirectoryName());
    }

    @Test
    void defaultConstructorStillReadsSystemProperties()
    {
        System.setProperty(TERM_BUFFER_LENGTH_PROP_NAME, "256k");

        assertEquals(256 * 1024, new Context().publicationTermBufferLength());
    }

    @Test
    void concludeTimeLookupsAlsoUseTheSuppliedProperties(@TempDir final Path aeronDir)
    {
        final Properties properties = new Properties();
        properties.setProperty(AERON_DIR_PROP_NAME, aeronDir.toString());
        properties.setProperty(THREADING_MODE_PROP_NAME, ThreadingMode.SHARED.name());

        final Context context = new Context(properties);
        try
        {
            context.conclude();

            assertEquals(ThreadingMode.SHARED, context.threadingMode());
        }
        finally
        {
            context.close();
        }
    }
}
