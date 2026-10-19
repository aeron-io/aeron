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
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@ExtendWith(InterruptingTestCallback.class)
class MultipleMulticastsSubscriptionsTest
{
    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private final MediaDriver.Context context = new MediaDriver.Context()
        .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
        .threadingMode(ThreadingMode.SHARED);
    private TestMediaDriver driver;

    @BeforeEach
    void setUp()
    {
        assumeFalse(TestMediaDriver.shouldRunCMediaDriver() && OS.current() == OS.WINDOWS);
    }

    private void launch(final MediaDriver.Context context)
    {
        driver = TestMediaDriver.launch(context, watcher);
        watcher.dataCollector().add(driver.context().aeronDirectory());
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.quietClose(driver);
    }

    @Test
    @InterruptAfter(5)
    void shouldBindToMultipleMulticastAddressOnTheSamePort()
    {
        launch(context);

        final String uriA = "aeron:udp?endpoint=239.192.11.87:20123|interface=127.0.0.1";
        final String uriB = "aeron:udp?endpoint=239.192.11.91:20123|interface=127.0.0.1";

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Publication pubA = aeron.addPublication(uriA, 10000);
            Publication pubB = aeron.addPublication(uriB, 10000);
            Subscription subA = aeron.addSubscription(uriA, 10000);
            Subscription subB = aeron.addSubscription(uriB, 10000))
        {
            Tests.awaitConnected(pubA);
            Tests.awaitConnected(pubB);
            Tests.awaitConnected(subA);
            Tests.awaitConnected(subB);

            sendData(pubA, pubB, subA, subB);
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldBindToMultipleMulticastAddressOnTheSamePortAsMdsDestinations()
    {
        launch(context);

        final String uriA = "aeron:udp?endpoint=239.192.11.87:20123|interface=127.0.0.1";
        final String uriB = "aeron:udp?endpoint=239.192.11.91:20123|interface=127.0.0.1";
        final String mdsSubscription = "aeron:udp?control-mode=manual";

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Publication pubA = aeron.addPublication(uriA, 10000);
            Publication pubB = aeron.addPublication(uriB, 10000);
            Subscription subA = aeron.addSubscription(mdsSubscription, 10000);
            Subscription subB = aeron.addSubscription(mdsSubscription, 10000))
        {
            subA.addDestination(uriA);
            subB.addDestination(uriB);

            Tests.awaitConnected(pubA);
            Tests.awaitConnected(pubB);
            Tests.awaitConnected(subA);
            Tests.awaitConnected(subB);

            sendData(pubA, pubB, subA, subB);
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldBindToMultipleMulticastAddressOnTheSamePortMixingMdsAndSubscriptions()
    {
        launch(context);

        final String uriA = "aeron:udp?endpoint=239.192.11.87:20123|interface=127.0.0.1";
        final String uriB = "aeron:udp?endpoint=239.192.11.91:20123|interface=127.0.0.1";
        final String mdsSubscription = "aeron:udp?control-mode=manual";

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Publication pubA = aeron.addPublication(uriA, 10000);
            Publication pubB = aeron.addPublication(uriB, 10000);
            Subscription subA = aeron.addSubscription(uriA, 10000);
            Subscription subB = aeron.addSubscription(mdsSubscription, 10000))
        {
            subB.addDestination(uriB);

            Tests.awaitConnected(pubA);
            Tests.awaitConnected(pubB);
            Tests.awaitConnected(subA);
            Tests.awaitConnected(subB);

            sendData(pubA, pubB, subA, subB);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldMulticastSubscriptionsWithSamePortNotConflict()
    {
        launch(context);

        final String uriA = "aeron:udp?endpoint=239.192.181.39:15492|interface=127.0.0.1";
        final String uriB = "aeron:udp?endpoint=239.192.181.99:15492|interface=127.0.0.1";

        final Aeron.Context aeronCtx = new Aeron.Context()
            .aeronDirectoryName(driver.aeronDirectoryName())
            .useConductorAgentInvoker(true);

        final MutableInteger subAImages = new MutableInteger(0);
        final MutableInteger subBImages = new MutableInteger(0);
        try (Aeron aeron = Aeron.connect(aeronCtx);
            Subscription subA = aeron.addSubscription(uriA, 9382, i -> subAImages.incrementAndGet(), i -> {});
            Subscription subB = aeron.addSubscription(uriB, 9382, i -> subBImages.incrementAndGet(), i -> {});
            Publication pub = aeron.addPublication(uriB, 9382))
        {
            Objects.requireNonNull(subA);
            Objects.requireNonNull(subB);
            Objects.requireNonNull(pub);

            Tests.await(() ->
            {
                aeron.conductorAgentInvoker().invoke();
                return subBImages.get() > 0;
            });

            assertEquals(0, subAImages.get());
        }
    }

    private static void sendData(
        final Publication pubA, final Publication pubB, final Subscription subA, final Subscription subB)
    {
        final byte[] msgA = new byte[128];
        ThreadLocalRandom.current().nextBytes(msgA);
        final byte[] msgB = new byte[1024];
        ThreadLocalRandom.current().nextBytes(msgB);

        final UnsafeBuffer data = new UnsafeBuffer();
        data.wrap(msgA);
        while (pubA.offer(data) < 0)
        {
            Tests.yield();
        }

        data.wrap(msgB);
        while (pubB.offer(data) < 0)
        {
            Tests.yield();
        }

        final FragmentHandler handlerA = (buffer, offset, length, header) ->
        {
            assertEquals(msgA.length, length);
            for (int i = 0; i < length; i++)
            {
                assertEquals(msgA[i], buffer.getByte(offset + i));
            }
        };

        final FragmentHandler handlerB = (buffer, offset, length, header) ->
        {
            assertEquals(msgB.length, length);
            for (int i = 0; i < length; i++)
            {
                assertEquals(msgB[i], buffer.getByte(offset + i));
            }
        };

        final Image imageA = subA.imageBySessionId(pubA.sessionId());
        final Image imageB = subB.imageBySessionId(pubB.sessionId());
        assertNotSame(imageA, imageB);
        assertNotEquals(imageA.correlationId(), imageB.correlationId());

        while (imageA.position() < pubA.position() || imageB.position() < pubB.position())
        {
            int workCount = subA.poll(handlerA, 10);
            workCount += subB.poll(handlerB, 10);
            if (0 == workCount)
            {
                Tests.yield();
            }
        }
    }
}
