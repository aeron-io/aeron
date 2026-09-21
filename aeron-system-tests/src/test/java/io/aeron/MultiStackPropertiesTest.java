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
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static io.aeron.CommonContext.AERON_DIR_PROP_NAME;
import static io.aeron.driver.Configuration.DIR_DELETE_ON_START_PROP_NAME;
import static io.aeron.driver.Configuration.IPC_MTU_LENGTH_PROP_NAME;
import static io.aeron.driver.Configuration.IPC_TERM_BUFFER_LENGTH_PROP_NAME;
import static io.aeron.driver.Configuration.THREADING_MODE_PROP_NAME;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Two fully isolated Aeron stacks configured from two {@link Properties} instances in a single JVM, with no
 * system properties involved. This is the scenario system property based configuration cannot express.
 */
@ExtendWith(InterruptingTestCallback.class)
class MultiStackPropertiesTest
{
    private static final int STREAM_ID = 1001;
    private static final String CHANNEL = "aeron:ipc";

    private MediaDriver driverA;
    private MediaDriver driverB;
    private Aeron clientA;
    private Aeron clientB;

    @AfterEach
    void tearDown()
    {
        CloseHelper.quietCloseAll(clientA, clientB, driverA, driverB);
    }

    @Test
    @InterruptAfter(20)
    void twoStacksConfiguredFromPropertiesAreIsolated(@TempDir final Path dirA, @TempDir final Path dirB)
    {
        final Properties propertiesA = stackProperties(dirA, "64k", "2k", "stack-a");
        final Properties propertiesB = stackProperties(dirB, "128k", "4k", "stack-b");

        driverA = MediaDriver.launch(new MediaDriver.Context(propertiesA));
        driverB = MediaDriver.launch(new MediaDriver.Context(propertiesB));

        assertEquals(dirA.toString(), driverA.context().aeronDirectoryName());
        assertEquals(dirB.toString(), driverB.context().aeronDirectoryName());
        assertEquals(64 * 1024, driverA.context().ipcTermBufferLength());
        assertEquals(128 * 1024, driverB.context().ipcTermBufferLength());
        assertEquals(2 * 1024, driverA.context().ipcMtuLength());
        assertEquals(4 * 1024, driverB.context().ipcMtuLength());
        assertNotEquals(driverA.context().aeronDirectory(), driverB.context().aeronDirectory());

        clientA = Aeron.connect(new Aeron.Context(propertiesA));
        clientB = Aeron.connect(new Aeron.Context(propertiesB));

        assertEquals("stack-a", clientA.context().clientName());
        assertEquals("stack-b", clientB.context().clientName());
        assertEquals(dirA.toString(), clientA.context().aeronDirectoryName());
        assertEquals(dirB.toString(), clientB.context().aeronDirectoryName());

        assertEquals("message-a", roundTrip(clientA, "message-a"));
        assertEquals("message-b", roundTrip(clientB, "message-b"));

        assertNull(System.getProperty(AERON_DIR_PROP_NAME));
        assertNull(System.getProperty(IPC_TERM_BUFFER_LENGTH_PROP_NAME));
    }

    private static Properties stackProperties(
        final Path aeronDir, final String termLength, final String mtuLength, final String clientName)
    {
        final Properties properties = new Properties();
        properties.setProperty(AERON_DIR_PROP_NAME, aeronDir.toString());
        properties.setProperty(DIR_DELETE_ON_START_PROP_NAME, "true");
        properties.setProperty(THREADING_MODE_PROP_NAME, ThreadingMode.SHARED.name());
        properties.setProperty(IPC_TERM_BUFFER_LENGTH_PROP_NAME, termLength);
        properties.setProperty(IPC_MTU_LENGTH_PROP_NAME, mtuLength);
        properties.setProperty(Aeron.Configuration.CLIENT_NAME_PROP_NAME, clientName);

        return properties;
    }

    private static String roundTrip(final Aeron aeron, final String message)
    {
        try (Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID);
            Publication publication = aeron.addPublication(CHANNEL, STREAM_ID))
        {
            Tests.awaitConnected(publication);

            final UnsafeBuffer buffer = new UnsafeBuffer(message.getBytes(US_ASCII));
            while (publication.offer(buffer, 0, buffer.capacity()) < 0)
            {
                Tests.yield();
            }

            final AtomicReference<String> received = new AtomicReference<>();
            final FragmentHandler handler =
                (buf, offset, length, header) -> received.set(buf.getStringWithoutLengthAscii(offset, length));

            while (null == received.get())
            {
                if (0 == subscription.poll(handler, 1))
                {
                    Tests.yield();
                }
            }

            return received.get();
        }
    }
}
