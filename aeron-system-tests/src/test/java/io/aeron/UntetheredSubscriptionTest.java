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
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(InterruptingTestCallback.class)
class UntetheredSubscriptionTest
{
    private static List<String> channels()
    {
        return asList(
            "aeron:ipc?term-length=64k",

            "aeron:udp?endpoint=localhost:24325|term-length=64k",
            "aeron-spy:aeron:udp?endpoint=localhost:24325|term-length=64k",

            "aeron:ipc?term-length=64k|untethered-window-limit-timeout=50ms|" +
            "untethered-resting-timeout=50ms|untethered-linger-timeout=50ms",

            "aeron:udp?endpoint=localhost:24325|term-length=64k|" +
            "untethered-window-limit-timeout=50ms|untethered-resting-timeout=50ms|" +
            "untethered-linger-timeout=25ms",

            "aeron-spy:aeron:udp?endpoint=localhost:24325|term-length=64k|" +
            "untethered-window-limit-timeout=50ms|untethered-resting-timeout=50ms|" +
            "untethered-linger-timeout=25ms"
        );
    }

    private static final int STREAM_ID = 1001;
    private static final int FRAGMENT_COUNT_LIMIT = 10;
    private static final int MESSAGE_LENGTH = 512 - DataHeaderFlyweight.HEADER_LENGTH;

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;

    private Aeron aeron;

    @BeforeEach
    void setUp()
    {
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    private void launch(final String channel)
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .errorHandler(Tests::onError)
            .spiesSimulateConnection(true)
            .dirDeleteOnStart(true)
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(10))
            .threadingMode(ThreadingMode.SHARED);

        final ChannelUri channelUri = ChannelUri.parse(channel);
        if (!channelUri.containsKey("untethered-window-limit-timeout"))
        {
            context.untetheredWindowLimitTimeoutNs(TimeUnit.MILLISECONDS.toNanos(50));
        }

        if (!channelUri.containsKey("untethered-resting-timeout"))
        {
            context.untetheredRestingTimeoutNs(TimeUnit.MILLISECONDS.toNanos(50));
        }

        driver = TestMediaDriver.launch(context, testWatcher);
        testWatcher.dataCollector().add(driver.context().aeronDirectory());
        aeron = Aeron.connect(new Aeron.Context().useConductorAgentInvoker(true));
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldBecomeUnavailableWhenNotKeepingUp(final String channel)
    {
        launch(channel);

        final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {};
        final AtomicBoolean unavailableCalled = new AtomicBoolean();
        final UnavailableImageHandler handler = (image) -> unavailableCalled.set(true);

        final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocate(MESSAGE_LENGTH));
        final String untetheredChannel = channel + "|tether=false";
        final String publicationChannel = channel.startsWith("aeron-spy") ? channel.substring(10) : channel;
        boolean pollingUntethered = true;

        try (Subscription tetheredSub = aeron.addSubscription(channel, STREAM_ID);
            Subscription untetheredSub = aeron.addSubscription(untetheredChannel, STREAM_ID, null, handler);
            Publication publication = aeron.addPublication(publicationChannel, STREAM_ID))
        {
            while (!tetheredSub.isConnected() || !untetheredSub.isConnected())
            {
                Tests.yield();
                aeron.conductorAgentInvoker().invoke();
            }

            while (true)
            {
                if (publication.offer(srcBuffer) < 0)
                {
                    Tests.yield();
                    aeron.conductorAgentInvoker().invoke();
                }

                if (pollingUntethered && untetheredSub.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT) > 0)
                {
                    pollingUntethered = false;
                }

                tetheredSub.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);

                if (unavailableCalled.get())
                {
                    assertTrue(tetheredSub.isConnected());
                    assertFalse(untetheredSub.isConnected());

                    while (publication.offer(srcBuffer) < 0)
                    {
                        Tests.yield();
                        aeron.conductorAgentInvoker().invoke();
                    }

                    return;
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldRejoinAfterResting(final String channel)
    {
        launch(channel);

        final AtomicInteger unavailableImageCount = new AtomicInteger();
        final AtomicInteger availableImageCount = new AtomicInteger();
        final UnavailableImageHandler unavailableHandler = (image) -> unavailableImageCount.incrementAndGet();
        final AvailableImageHandler availableHandler = (image) -> availableImageCount.incrementAndGet();
        final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {};

        final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocate(MESSAGE_LENGTH));
        srcBuffer.setMemory(0, MESSAGE_LENGTH, (byte)-1);
        final String untetheredChannel = channel + "|tether=false";
        final String publicationChannel = channel.startsWith("aeron-spy") ? channel.substring(10) : channel;
        boolean pollingUntethered = true;

        try (Subscription tetheredSub = aeron.addSubscription(channel, STREAM_ID);
            Subscription untetheredSub = aeron.addSubscription(
                untetheredChannel, STREAM_ID, availableHandler, unavailableHandler);
            Publication publication = aeron.addPublication(publicationChannel, STREAM_ID))
        {
            while (!tetheredSub.isConnected() || !untetheredSub.isConnected())
            {
                Tests.yield();
                aeron.conductorAgentInvoker().invoke();
            }

            while (0 == unavailableImageCount.get())
            {
                if (publication.offer(srcBuffer, 0, ThreadLocalRandom.current().nextInt(1, MESSAGE_LENGTH)) < 0)
                {
                    Tests.yield();
                    aeron.conductorAgentInvoker().invoke();
                }

                if (pollingUntethered && untetheredSub.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT) > 0)
                {
                    pollingUntethered = false;
                }

                tetheredSub.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);
            }

            while (availableImageCount.get() < 2)
            {
                publication.offer(srcBuffer, 0, ThreadLocalRandom.current().nextInt(1, MESSAGE_LENGTH));
                Tests.yield();
                aeron.conductorAgentInvoker().invoke();
            }

            final Image tetheredImage = tetheredSub.imageAtIndex(0);
            final Image untetheredImage = untetheredSub.imageAtIndex(0);
            while (untetheredImage.position() < publication.position() ||
                tetheredImage.position() < publication.position())
            {
                int fragments = 0;
                fragments += tetheredSub.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);
                fragments += untetheredSub.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);
                if (0 == fragments)
                {
                    Tests.yield();
                    aeron.conductorAgentInvoker().invoke();
                }
            }
        }
    }
}
