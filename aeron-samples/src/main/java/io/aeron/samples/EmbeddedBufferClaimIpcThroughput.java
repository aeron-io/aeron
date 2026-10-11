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
package io.aeron.samples;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.agrona.SystemUtil.loadPropertiesFiles;

/**
 * Throughput test using {@link Publication#tryClaim(int, BufferClaim)} over IPC transport.
 */
public class EmbeddedBufferClaimIpcThroughput
{
    private static final int BURST_LENGTH = 1_000_000;
    private static final int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final String CHANNEL = CommonContext.IPC_CHANNEL;
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     * @throws InterruptedException if the join on other threads is interrupted.
     */
    public static void main(final String[] args) throws InterruptedException
    {
        loadPropertiesFiles(args);

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED);

        final AtomicBoolean running = new AtomicBoolean(true);
        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier(() -> running.set(false));
            MediaDriver mediaDriver = MediaDriver.launch(ctx.terminationHook(barrier::signalAll));
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
            Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID);
            Publication publication = aeron.addPublication(CHANNEL, STREAM_ID))
        {
            final ImageRateSubscriber subscriber = new ImageRateSubscriber(FRAGMENT_COUNT_LIMIT, running, subscription);
            final Thread subscriberThread = new Thread(subscriber);
            subscriberThread.setName("subscriber");
            final Thread publisherThread = new Thread(new Publisher(running, publication));
            publisherThread.setName("publisher");
            final Thread rateReporterThread = new Thread(new ImageRateReporter(MESSAGE_LENGTH, running, subscriber));
            rateReporterThread.setName("rate-reporter");

            rateReporterThread.start();
            subscriberThread.start();
            publisherThread.start();

            barrier.await();

            subscriberThread.join();
            publisherThread.join();
            rateReporterThread.join();
        }
    }

    static final class Publisher implements Runnable
    {
        private final AtomicBoolean running;
        private final Publication publication;

        Publisher(final AtomicBoolean running, final Publication publication)
        {
            this.running = running;
            this.publication = publication;
        }

        public void run()
        {
            final IdleStrategy idleStrategy = SampleConfiguration.newIdleStrategy();
            final AtomicBoolean running = this.running;
            final Publication publication = this.publication;
            final BufferClaim bufferClaim = new BufferClaim();
            long backPressureCount = 0;
            long totalMessageCount = 0;

            outputResults:
            while (running.get())
            {
                for (int i = 0; i < BURST_LENGTH; i++)
                {
                    idleStrategy.reset();
                    while (publication.tryClaim(MESSAGE_LENGTH, bufferClaim) <= 0)
                    {
                        ++backPressureCount;
                        if (!running.get())
                        {
                            break outputResults;
                        }
                        idleStrategy.idle();
                    }

                    final int offset = bufferClaim.offset();
                    bufferClaim.buffer().putInt(offset, i); // Example field write
                    // Real app would write whatever fields are required via a flyweight like SBE

                    bufferClaim.commit();

                    ++totalMessageCount;
                }
            }

            final double backPressureRatio = backPressureCount / (double)totalMessageCount;
            System.out.format("Publisher back pressure ratio: %f%n", backPressureRatio);
        }
    }
}
