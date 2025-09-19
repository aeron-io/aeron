/*
 * Copyright 2014-2024 Real Logic Limited.
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
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static java.nio.charset.StandardCharsets.US_ASCII;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class ReconnectStreamsTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private final MediaDriver.Context context = new MediaDriver.Context()
        .dirDeleteOnStart(true)
        .threadingMode(ThreadingMode.SHARED);
    private TestMediaDriver driver;

    @AfterEach
    void tearDown()
    {
        CloseHelper.quietClose(driver);
    }

    private TestMediaDriver launch()
    {
        driver = TestMediaDriver.launch(context, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driver.context().aeronDirectory());
        return driver;
    }

    static class PublicationRunnable implements Runnable
    {
        final Aeron aeron;
        final int publicationCount = 300;
        final List<Publication> publications = new ArrayList<>();
        final DirectBuffer message = new UnsafeBuffer("this is a test message".getBytes(US_ASCII));
        static final String CHANNEL = "aeron:udp?endpoint=localhost:10000|so-rcvbuf=5242880|term-length=2097152";
        static final int STREAM_ID = 10000;
        volatile long totalErrorCount = 0;
        volatile long backToConnectedMs = 0;

        PublicationRunnable(final String aeronDirectory)
        {
            this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectory));
            for (int i = 0; i < publicationCount; i++)
            {
                final Publication publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID);
                publications.add(publication);
                Tests.awaitConnected(publication);
                while (0 == publication.availableWindow())
                {
                    Tests.yieldingIdle("publication available window");
                }
            }
        }

        public void run()
        {
            final Random r = new Random();
            try
            {
                int lastErrorsTotal = 0;
                while (!Thread.currentThread().isInterrupted())
                {
                    final int[] errors = new int[10];
                    int errorsTotal = 0;
                    for (final Publication publication : publications)
                    {
                        //noinspection BusyWait
                        Thread.sleep(1);
                        final long result = publication.offer(message);
                        if (result < 0)
                        {
                            final int index = (int)(-result);
                            errors[index]++;
                            errorsTotal++;
                        }
                    }
                    if (errorsTotal > 0)
                    {
                        //noinspection NonAtomicOperationOnVolatileField
                        totalErrorCount += errorsTotal;
                        printErrors(errors);
                    }
                    else if (lastErrorsTotal != 0)
                    {
                        printErrors(errors);
                        backToConnectedMs = System.currentTimeMillis();
                    }

                    lastErrorsTotal = errorsTotal;
                }
            }
            catch (final InterruptedException ignore)
            {
            }
            finally
            {
                publications.forEach(CloseHelper::quietClose);
                CloseHelper.quietClose(aeron);
            }
        }

        void printErrors(final int[] errors)
        {
            System.out.print("[" + System.currentTimeMillis() + "] ");

            for (int index = 0; index < errors.length; index++)
            {
                final int error = errors[index];
                if (0 != error)
                {
                    System.out.print(Publication.errorString(-index) + "=" + error + " ");
                }
            }

            System.out.println();
        }
    }

    @Test
    @SlowTest
    @InterruptAfter(60)
    @Disabled
    void shouldHandleLargeNumbersOfPublications() throws InterruptedException
    {
        final TestMediaDriver driver = launch();
        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(driver.aeronDirectoryName());

        Thread t = null;

        PublicationRunnable publicationRunnable = null;
        long reconnectTimeMs = 0;

        try
        {
            try (Aeron aeron = Aeron.connect(ctx.clone());
                Subscription subInitial = aeron.addSubscription(
                    PublicationRunnable.CHANNEL, PublicationRunnable.STREAM_ID))
            {
                System.out.println("Creating publications");
                publicationRunnable = new PublicationRunnable(driver.aeronDirectoryName());
                System.out.println("Start thread");
                t = new Thread(publicationRunnable);
                t.start();

                final long deadlineMs = System.currentTimeMillis() + 5000;

                while (System.currentTimeMillis() < deadlineMs)
                {
                    if (0 != subInitial.poll((buffer, offset, length, header) -> {}, 100))
                    {
                        Tests.yield();
                    }
                }
            }

            System.out.println("[" + System.currentTimeMillis() + "] Disconnected");
            Tests.sleep(1_000);
            reconnectTimeMs = System.currentTimeMillis();
            System.out.println("[" + reconnectTimeMs + "] Reconnecting");

            try (Aeron aeron = Aeron.connect(ctx.clone());
                Subscription subInitial = aeron.addSubscription(
                    PublicationRunnable.CHANNEL, PublicationRunnable.STREAM_ID);)
            {
                while (subInitial.imageCount() < publicationRunnable.publicationCount)
                {
                    if (0 != subInitial.poll((buffer, offset, length, header) -> {}, 100))
                    {
                        Tests.yield();
                    }
                }

                System.out.println("[" + System.currentTimeMillis() + "] All images present");

                if (0 != publicationRunnable.totalErrorCount)
                {
                    while (0 == publicationRunnable.backToConnectedMs)
                    {
                        if (0 != subInitial.poll((buffer, offset, length, header) -> {}, 100))
                        {
                            Tests.yield();
                        }
                    }
                }

                if (0 != reconnectTimeMs)
                {
                    System.out.println("Full connection=" + (System.currentTimeMillis() - reconnectTimeMs));
                }
            }
        }
        finally
        {
            if (null != t)
            {
                t.interrupt();
                t.join();
            }
        }
    }
}
