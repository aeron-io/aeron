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
package io.aeron.archive;

import io.aeron.CommonContext;
import io.aeron.archive.client.AeronArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static io.aeron.CommonContext.AERON_DIR_PROP_NAME;
import static io.aeron.archive.Archive.Configuration.ARCHIVE_DIR_PROP_NAME;
import static io.aeron.archive.Archive.Configuration.ARCHIVE_ID_PROP_NAME;
import static io.aeron.archive.Archive.Configuration.MAX_CONCURRENT_RECORDINGS_PROP_NAME;
import static io.aeron.archive.Archive.Configuration.SEGMENT_FILE_LENGTH_PROP_NAME;
import static io.aeron.archive.Archive.Configuration.THREADING_MODE_PROP_NAME;
import static io.aeron.archive.client.AeronArchive.Configuration.CONTROL_CHANNEL_PROP_NAME;
import static io.aeron.archive.client.AeronArchive.Configuration.MESSAGE_TIMEOUT_PROP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class ArchiveContextPropertiesTest
{
    @AfterEach
    void tearDown()
    {
        System.clearProperty(ARCHIVE_DIR_PROP_NAME);
        System.clearProperty(SEGMENT_FILE_LENGTH_PROP_NAME);
    }

    @Test
    void suppliedPropertiesConfigureTheArchiveContext()
    {
        final Properties properties = new Properties();
        properties.setProperty(AERON_DIR_PROP_NAME, "/tmp/aeron-a");
        properties.setProperty(ARCHIVE_DIR_PROP_NAME, "/tmp/archive-a");
        properties.setProperty(SEGMENT_FILE_LENGTH_PROP_NAME, "256k");
        properties.setProperty(MAX_CONCURRENT_RECORDINGS_PROP_NAME, "7");
        properties.setProperty(ARCHIVE_ID_PROP_NAME, "42");
        properties.setProperty(THREADING_MODE_PROP_NAME, ArchiveThreadingMode.SHARED.name());

        final Archive.Context context = new Archive.Context(properties);

        assertSame(properties, context.properties());
        assertEquals("/tmp/aeron-a", context.aeronDirectoryName());
        assertEquals("/tmp/archive-a", context.archiveDirectoryName());
        assertEquals(256 * 1024, context.segmentFileLength());
        assertEquals(7, context.maxConcurrentRecordings());
        assertEquals(42, context.archiveId());
        assertEquals(ArchiveThreadingMode.SHARED, context.threadingMode());
    }

    @Test
    void twoArchiveContextsFromDifferentPropertiesAreIndependent()
    {
        final Properties a = new Properties();
        a.setProperty(ARCHIVE_DIR_PROP_NAME, "/tmp/archive-a");
        a.setProperty(SEGMENT_FILE_LENGTH_PROP_NAME, "128k");

        final Properties b = new Properties();
        b.setProperty(ARCHIVE_DIR_PROP_NAME, "/tmp/archive-b");
        b.setProperty(SEGMENT_FILE_LENGTH_PROP_NAME, "256k");

        final Archive.Context contextA = new Archive.Context(a);
        final Archive.Context contextB = new Archive.Context(b);

        assertEquals("/tmp/archive-a", contextA.archiveDirectoryName());
        assertEquals("/tmp/archive-b", contextB.archiveDirectoryName());
        assertEquals(128 * 1024, contextA.segmentFileLength());
        assertEquals(256 * 1024, contextB.segmentFileLength());
        assertNotEquals(contextA.archiveDirectoryName(), contextB.archiveDirectoryName());
    }

    @Test
    void suppliedPropertiesShadowSystemPropertiesAndAreNotWrittenBackToThem()
    {
        System.setProperty(SEGMENT_FILE_LENGTH_PROP_NAME, "64k");

        final Properties properties = new Properties();
        properties.setProperty(SEGMENT_FILE_LENGTH_PROP_NAME, "512k");

        assertEquals(512 * 1024, new Archive.Context(properties).segmentFileLength());
        assertEquals(64 * 1024, new Archive.Context().segmentFileLength());
        assertEquals("64k", System.getProperty(SEGMENT_FILE_LENGTH_PROP_NAME));
    }

    @Test
    void emptyPropertiesYieldTheDocumentedDefaults()
    {
        final Archive.Context context = new Archive.Context(new Properties());

        assertEquals(Archive.Configuration.SEGMENT_FILE_LENGTH_DEFAULT, context.segmentFileLength());
        assertEquals(Archive.Configuration.MAX_CONCURRENT_RECORDINGS_DEFAULT, context.maxConcurrentRecordings());
        assertEquals(CommonContext.AERON_DIR_PROP_DEFAULT, context.aeronDirectoryName());
    }

    @Test
    void suppliedPropertiesConfigureTheAeronArchiveClientContext()
    {
        final Properties properties = new Properties();
        properties.setProperty(AERON_DIR_PROP_NAME, "/tmp/aeron-a");
        properties.setProperty(CONTROL_CHANNEL_PROP_NAME, "aeron:udp?endpoint=localhost:9999");
        properties.setProperty(MESSAGE_TIMEOUT_PROP_NAME, "3s");

        final AeronArchive.Context context = new AeronArchive.Context(properties);

        assertSame(properties, context.properties());
        assertEquals("/tmp/aeron-a", context.aeronDirectoryName());
        assertEquals("aeron:udp?endpoint=localhost:9999", context.controlRequestChannel());
        assertEquals(TimeUnit.SECONDS.toNanos(3), context.messageTimeoutNs());
    }

    @Test
    void archiveContextPropagatesItsPropertiesToTheArchiveClientContextItCreates()
    {
        final Properties properties = new Properties();
        properties.setProperty(CONTROL_CHANNEL_PROP_NAME, "aeron:udp?endpoint=localhost:8888");

        final Archive.Context context = new Archive.Context(properties);
        context.archiveClientContext(new AeronArchive.Context(context.properties()));

        assertSame(properties, context.archiveClientContext().properties());
        assertEquals("aeron:udp?endpoint=localhost:8888", context.archiveClientContext().controlRequestChannel());
    }
}
