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
import io.aeron.logbuffer.Header;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(InterruptingTestCallback.class)
class MemoryOrderingTest
{
    private static final int STREAM_ID = 1001;
    private static final int FRAGMENT_COUNT_LIMIT = 10;
    private static final int MESSAGE_LENGTH = 2000;
    private static final int TERM_BUFFER_LENGTH = 1024 * 64;
    private static final int NUM_MESSAGES = 15_000;
    private static final int BURST_LENGTH = 7;
    private static final int INTER_BURST_DURATION_NS = 100_000;

    private static volatile String failedMessage = null;

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;
    private Aeron aeron;

    @BeforeEach
    void setUp()
    {
        driver = TestMediaDriver.launch(new MediaDriver.Context()
                .errorHandler(Tests::onError)
                .dirDeleteOnStart(true)
                .threadingMode(ThreadingMode.SHARED)
                .publicationTermBufferLength(TERM_BUFFER_LENGTH),
            testWatcher);
        testWatcher.dataCollector().add(driver.context().aeronDirectory());

        aeron = Aeron.connect();
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldReceiveMessagesInOrderWithFirstLongWordIntact(final String channel) throws Exception
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocate(MESSAGE_LENGTH));
        srcBuffer.setMemory(0, MESSAGE_LENGTH, (byte)7);

        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            Publication publication = aeron.addPublication(channel, STREAM_ID))
        {
            final IdleStrategy idleStrategy = YieldingIdleStrategy.INSTANCE;
            final Thread subscriberThread = new Thread(new Subscriber(subscription));
            subscriberThread.setDaemon(true);
            subscriberThread.start();

            for (int i = 0; i < NUM_MESSAGES; i++)
            {
                failOnError();

                srcBuffer.putLong(0, i);

                while (publication.offer(srcBuffer) < 0L)
                {
                    failOnError();

                    idleStrategy.idle();
                    Tests.checkInterruptStatus();
                }

                if (i % BURST_LENGTH == 0)
                {
                    final long timeoutNs = System.nanoTime() + INTER_BURST_DURATION_NS;
                    long nowNs;
                    do
                    {
                        nowNs = System.nanoTime();
                    }
                    while ((timeoutNs - nowNs) > 0);
                }
            }

            subscriberThread.join();

            failOnError();
        }
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(20)
    void shouldReceiveMessagesInOrderWithFirstLongWordIntactFromExclusivePublication(final String channel)
        throws InterruptedException
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocate(MESSAGE_LENGTH));
        srcBuffer.setMemory(0, MESSAGE_LENGTH, (byte)7);

        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            ExclusivePublication publication = aeron.addExclusivePublication(channel, STREAM_ID))
        {
            final IdleStrategy idleStrategy = YieldingIdleStrategy.INSTANCE;
            final Thread subscriberThread = new Thread(new Subscriber(subscription));
            subscriberThread.setDaemon(true);
            subscriberThread.start();

            for (int i = 0; i < NUM_MESSAGES; i++)
            {
                failOnError();

                srcBuffer.putLong(0, i);

                while (publication.offer(srcBuffer) < 0L)
                {
                    failOnError();

                    idleStrategy.idle();
                    Tests.checkInterruptStatus();
                }

                if (i % BURST_LENGTH == 0)
                {
                    final long timeoutNs = System.nanoTime() + INTER_BURST_DURATION_NS;
                    long nowNs;
                    do
                    {
                        nowNs = System.nanoTime();
                    }
                    while ((timeoutNs - nowNs) > 0);
                }
            }

            subscriberThread.join();

            failOnError();
        }
    }

    private static void failOnError()
    {
        if (null != failedMessage)
        {
            fail(failedMessage);
        }
    }

    private static List<String> channels()
    {
        return asList(
            "aeron:ipc?term-length=4m",
            "aeron:udp?endpoint=localhost:24325|term-length=4m"
        );
    }

    static class Subscriber implements Runnable, FragmentHandler
    {
        private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this);
        private final Subscription subscription;

        long previousValue = -1;
        int messageNum = 0;

        Subscriber(final Subscription subscription)
        {
            this.subscription = subscription;
        }

        public void run()
        {
            final IdleStrategy idleStrategy = YieldingIdleStrategy.INSTANCE;

            while (messageNum < NUM_MESSAGES && null == failedMessage)
            {
                idleStrategy.idle(subscription.poll(fragmentAssembler, FRAGMENT_COUNT_LIMIT));
            }
        }

        public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            final long messageValue = buffer.getLong(offset);

            final long expectedValue = previousValue + 1;
            if (messageValue != expectedValue)
            {
                final long messageValueSecondRead = buffer.getLong(offset);

                final String msg = "Issue at message number transition: " + previousValue + " -> " + messageValue;

                System.out.println(msg + "\n" +
                    "offset: " + offset + "\n" +
                    "length: " + length + "\n" +
                    "expected bytes: " + byteString(expectedValue) + "\n" +
                    "received bytes: " + byteString(messageValue) + "\n" +
                    "expected bits: " + Long.toBinaryString(expectedValue) + "\n" +
                    "received bits: " + Long.toBinaryString(messageValue) + "\n" +
                    "messageValue on second read: " + messageValueSecondRead + "\n" +
                    "messageValue on third read: " + buffer.getLong(offset));

                failedMessage = msg;
            }

            previousValue = messageValue;
            messageNum++;
        }

        private String byteString(final long value)
        {
            return String.format("%x %x %x %x %x %x %x %x",
                (byte)(value >>> 56),
                (byte)(value >>> 48),
                (byte)(value >>> 40),
                (byte)(value >>> 32),
                (byte)(value >>> 24),
                (byte)(value >>> 18),
                (byte)(value >>> 8),
                (byte)value);
        }
    }
}
