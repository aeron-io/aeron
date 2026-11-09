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
import io.aeron.DirectBufferVector;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.BufferUtil;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.SystemUtil.loadPropertiesFiles;

/**
 * Throughput test using {@link ExclusivePublication#offer(DirectBufferVector[])} over IPC transport.
 */
public class EmbeddedExclusiveVectoredIpcThroughput
{
    private static final int BURST_LENGTH = 1_000_000;
    private static final int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
    private static final int VEC_ONE_LENGTH = 16;
    private static final int VEC_TWO_LENGTH = MESSAGE_LENGTH - VEC_ONE_LENGTH;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final String CHANNEL = CommonContext.IPC_CHANNEL;
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     * @throws InterruptedException if the thread is interrupted while waiting on the threads to join.
     */
    public static void main(final String[] args) throws InterruptedException
    {
        loadPropertiesFiles(args);

        final AtomicBoolean running = new AtomicBoolean(true);

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED);

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier(() -> running.set(false));
            MediaDriver mediaDriver = MediaDriver.launch(ctx.terminationHook(barrier::signalAll));
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
            Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID);
            ExclusivePublication publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID))
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

            subscriberThread.join();
            publisherThread.join();
            rateReporterThread.join();
        }
    }

    static final class Publisher implements Runnable
    {
        private final AtomicBoolean running;
        private final ExclusivePublication publication;

        Publisher(final AtomicBoolean running, final ExclusivePublication publication)
        {
            this.running = running;
            this.publication = publication;
        }

        @Override
        public void run()
        {
            final IdleStrategy idleStrategy = SampleConfiguration.newIdleStrategy();
            final AtomicBoolean running = this.running;
            final ExclusivePublication publication = this.publication;
            final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(MESSAGE_LENGTH, CACHE_LINE_LENGTH);
            final UnsafeBuffer bufferOne = new UnsafeBuffer(byteBuffer, 0, VEC_ONE_LENGTH);
            final UnsafeBuffer bufferTwo = new UnsafeBuffer(byteBuffer, VEC_ONE_LENGTH, VEC_TWO_LENGTH);
            final DirectBufferVector[] vectors = new DirectBufferVector[]
            {
                new DirectBufferVector(bufferOne, 0, VEC_ONE_LENGTH),
                new DirectBufferVector(bufferTwo, 0, VEC_TWO_LENGTH),
            };

            long backPressureCount = 0;
            long totalMessageCount = 0;

            outputResults:
            while (running.get())
            {
                for (int i = 0; i < BURST_LENGTH; i++)
                {
                    idleStrategy.reset();
                    while (publication.offer(vectors, null) <= 0)
                    {
                        ++backPressureCount;
                        if (!running.get())
                        {
                            break outputResults;
                        }

                        idleStrategy.idle();
                    }

                    ++totalMessageCount;
                }
            }

            final double backPressureRatio = backPressureCount / (double)totalMessageCount;
            System.out.format("Publisher back pressure ratio: %f%n", backPressureRatio);
        }
    }
}
