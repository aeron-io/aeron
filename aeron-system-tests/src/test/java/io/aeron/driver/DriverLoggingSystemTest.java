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
package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logging.CollectingEventLogReaderAgent;
import io.aeron.logging.EventConfiguration;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith(InterruptingTestCallback.class)
class DriverLoggingSystemTest
{
    private static final String CHANNEL = "aeron:udp?endpoint=localhost:24567";
    private static final int STREAM_ID = 1777;

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private CollectingEventLogReaderAgent reader;

    @BeforeEach
    void before()
    {
        assumeTrue("all".equals(System.getProperty(CommonContext.EVENT_LOG)));
        final Object agent = EventConfiguration.eventReader().agent();
        assumeTrue(agent instanceof CollectingEventLogReaderAgent);
        reader = (CollectingEventLogReaderAgent)agent;
    }

    @AfterEach
    void after()
    {
        reader.reset();
    }

    @Test
    @InterruptAfter(15)
    void largeFramesShouldNotBreakDriverExecution(final @TempDir Path tempDir) throws IOException
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .errorHandler(Tests::onError)
            .mtuLength(16 * 1024);

        try (TestMediaDriver driver = TestMediaDriver.launch(driverCtx, testWatcher))
        {
            testWatcher.dataCollector().add(driver.context().aeronDirectory());

            try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
                Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID);
                Publication publication = aeron.addPublication(CHANNEL, STREAM_ID))
            {
                reader.setCollecting(true);

                final byte[] sentBytes = new byte[driver.context().mtuLength() * 3];
                ThreadLocalRandom.current().nextBytes(sentBytes);
                final UnsafeBuffer srcBuffer = new UnsafeBuffer(sentBytes);

                while (publication.offer(srcBuffer) < 0)
                {
                    Tests.yield();
                }

                final UnsafeBuffer receivedBuffer = new UnsafeBuffer(new byte[sentBytes.length]);
                final MutableInteger receivedLength = new MutableInteger();
                final FragmentAssembler assembler = new FragmentAssembler(
                    (buffer, offset, length, header) ->
                    {
                        buffer.getBytes(offset, receivedBuffer, 0, length);
                        receivedLength.value = length;
                    });

                while (0 == receivedLength.get())
                {
                    subscription.poll(assembler, 10);
                    Tests.yield();
                }

                assertArrayEquals(sentBytes, receivedBuffer.byteArray());
            }
        }

        final Path logFile = tempDir.resolve("driver.log");
        reader.writeToFile(logFile.toString());
        final String content = Files.readString(logFile);
        assertTrue(content.contains("bytes truncated"));
        assertTrue(content.contains("FRAME_IN"));
        assertTrue(content.contains("FRAME_OUT"));
        assertTrue(content.contains("CMD_IN_REMOVE_PUBLICATION"));
        assertTrue(content.contains("CMD_OUT_ON_OPERATION_SUCCESS"));

    }
}
