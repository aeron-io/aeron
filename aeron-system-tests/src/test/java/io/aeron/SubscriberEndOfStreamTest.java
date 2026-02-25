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
import io.aeron.status.ChannelEndpointStatus;
import io.aeron.status.LocalSocketAddressStatus;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class SubscriberEndOfStreamTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private final DirectBuffer message = new UnsafeBuffer("this is a test message".getBytes(US_ASCII));

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

    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:udp?endpoint=localhost:10000|linger=0s",
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min",
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:/1",
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max"
    })
    @InterruptAfter(20)
    @SlowTest
    void shouldDisconnectPublicationWithEosIfSubscriptionIsClosed(final String channel)
    {
        final TestMediaDriver driver = launch();
        final int streamId = 10000;

        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(driver.aeronDirectoryName());

        final AtomicBoolean imageUnavailable = new AtomicBoolean(false);

        try (Aeron aeron = Aeron.connect(ctx);
            Publication pub = aeron.addPublication(channel, streamId))
        {
            final Subscription sub = aeron.addSubscription(
                channel, streamId, image -> {}, image -> imageUnavailable.set(true));

            Tests.awaitConnected(pub);
            Tests.awaitConnected(sub);

            while (pub.offer(message) < 0)
            {
                Tests.yield();
            }

            while (0 == sub.poll((buffer, offset, length, header) -> {}, 1))
            {
                Tests.yield();
            }

            final Image image = sub.imageAtIndex(0);
            assertEquals(pub.position(), image.position());

            CloseHelper.close(sub);

            final long t0 = System.nanoTime();
            while (pub.isConnected())
            {
                Tests.yield();
            }
            final long t1 = System.nanoTime();
            assertThat(t1 - t0, lessThan(driver.context().publicationConnectionTimeoutNs()));

            while (!imageUnavailable.get())
            {
                Tests.yield();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:udp?control-mode=dynamic|control=localhost:10000|fc=min",
        "aeron:udp?control-mode=dynamic|control=localhost:10000|fc=max",
    })
    @InterruptAfter(20)
    @SlowTest
    void shouldDisconnectPublicationWithEosIfSubscriptionIsClosedMdc(final String publicationChannel)
    {
        final TestMediaDriver driver = launch();
        final int streamId = 10000;
        final String subscriptionChannel = "aeron:udp?control=localhost:10000";

        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(driver.aeronDirectoryName());

        final AtomicBoolean imageUnavailable = new AtomicBoolean(false);

        try (Aeron aeron = Aeron.connect(ctx);
            Publication pub = aeron.addPublication(publicationChannel, streamId))
        {
            final Subscription sub = aeron.addSubscription(
                subscriptionChannel, streamId, image -> {}, image -> imageUnavailable.set(true));

            Tests.awaitConnected(pub);
            Tests.awaitConnected(sub);

            while (pub.offer(message) < 0)
            {
                Tests.yield();
            }

            while (0 == sub.poll((buffer, offset, length, header) -> {}, 1))
            {
                Tests.yield();
            }

            final Image image = sub.imageAtIndex(0);
            assertEquals(pub.position(), image.position());

            CloseHelper.close(sub);

            final long t0 = System.nanoTime();
            while (pub.isConnected())
            {
                Tests.yield();
            }
            final long t1 = System.nanoTime();
            assertThat(t1 - t0, lessThan(driver.context().publicationConnectionTimeoutNs()));

            while (!imageUnavailable.get())
            {
                Tests.yield();
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldDropReceiverOutOfFlowControlOnEndOfStream()
    {
        final TestMediaDriver driver = launch();
        final int streamId = 10000;
        final String subscriptionChannel1 = "aeron:udp?control=localhost:10000|endpoint=localhost:10001";
        final String subscriptionChannel2 = "aeron:udp?control=localhost:10000|endpoint=localhost:10002";
        final String publicationChannel =
            "aeron:udp?control-mode=dynamic|control=localhost:10000|fc=min|term-length=64k";

        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(driver.aeronDirectoryName());

        try (Aeron aeron = Aeron.connect(ctx);
            Publication pub = aeron.addPublication(publicationChannel, streamId);
            Subscription sub1 = aeron.addSubscription(subscriptionChannel1, streamId))
        {
            final Subscription sub2 = aeron.addSubscription(subscriptionChannel2, streamId);

            Tests.awaitConnected(pub);
            Tests.awaitConnected(sub1);
            Tests.awaitConnected(sub2);
            final DirectBuffer message = new UnsafeBuffer(new byte[pub.maxPayloadLength()]);
            final int fcReceiversCounterId = aeron.countersReader().findByTypeIdAndRegistrationId(
                AeronCounters.FLOW_CONTROL_RECEIVERS_COUNTER_TYPE_ID, pub.registrationId());

            while (aeron.countersReader().getCounterValue(fcReceiversCounterId) < 2)
            {
                Tests.yield();
            }

            final Image image = sub1.imageAtIndex(0);
            final Supplier<String> errorMsg =
                () -> "Image.position=" + image.position() + " Publication.position=" + pub.position();

            int messageCount = 0;
            do
            {
                final long position = pub.offer(message);
                if (position == Publication.BACK_PRESSURED && messageCount != 0)
                {
                    break;
                }
                else
                {
                    Tests.yield();
                }

                final int fragments = sub1.poll((buffer, offset, length, header) -> {}, 10);
                if (0 == fragments)
                {
                    Tests.yieldingIdle(errorMsg);
                }
                else
                {
                    messageCount += fragments;
                }
            }
            while (true);

            CloseHelper.close(sub2);
            final long t0 = System.nanoTime();
            while (2 == aeron.countersReader().getCounterValue(fcReceiversCounterId))
            {
                Tests.yield();
            }
            final long t1 = System.nanoTime();
            assertThat(t1 - t0, lessThan(driver.context().publicationConnectionTimeoutNs()));

            final long t2 = System.nanoTime();
            while (0 < pub.offer(message))
            {
                Tests.yield();
            }
            final long t3 = System.nanoTime();
            assertThat(t3 - t2, lessThan(driver.context().publicationConnectionTimeoutNs()));
        }
    }

    @Test
    void shouldNotLingerUnicastPublicationWhenReceivingEndOfStream()
    {
        final String addressAlreadyInUseMessage = "Address already in use";
        systemTestWatcher.ignoreErrorsMatching((s) -> s.contains(addressAlreadyInUseMessage));
        final long lingerTimeoutMs = 5000;

        final TestMediaDriver driver = launch();
        final int streamId = 10000;
        final String subscriptionChannel1 = "aeron:udp?endpoint=localhost:10000";
        final String pubChannel1 =
            "aeron:udp?endpoint=localhost:10000|control=localhost:10001|linger=" + lingerTimeoutMs + "ms|eos=true";

        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(driver.aeronDirectoryName());

        try (Aeron aeron = Aeron.connect(ctx))
        {
            final Publication pub1 = aeron.addPublication(pubChannel1, streamId);
            final Subscription sub1 = aeron.addSubscription(subscriptionChannel1, streamId);

            Tests.awaitConnected(sub1);
            Tests.awaitConnected(pub1);

            while (pub1.offer(message) < 0)
            {
                Tests.yield();
            }

            final int channelStatusId = pub1.channelStatusId();
            CloseHelper.close(pub1);

            final long t0 = System.currentTimeMillis();
            while (null != LocalSocketAddressStatus.findAddress(
                aeron.countersReader(), ChannelEndpointStatus.ACTIVE, channelStatusId))
            {
                Tests.yield();
            }
            final long t1 = System.currentTimeMillis();

            assertThat((t1 - t0), lessThan(lingerTimeoutMs));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:udp?control-mode=manual|fc=min",
        "aeron:udp?control-mode=manual|fc=max",
    })
    @InterruptAfter(20)
    @SlowTest
    void shouldDisconnectPublicationWithEosIfSubscriptionIsClosedMdcManual(final String publicationChannel)
    {
        final TestMediaDriver driver = launch();
        final int streamId = 10000;
        final String subscriptionChannel = "aeron:udp?endpoint=localhost:10000";

        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(driver.aeronDirectoryName());

        final AtomicBoolean imageUnavailable = new AtomicBoolean(false);

        try (Aeron aeron = Aeron.connect(ctx);
            Publication pub = aeron.addPublication(publicationChannel, streamId))
        {
            pub.addDestination(subscriptionChannel);
            final Subscription sub = aeron.addSubscription(
                subscriptionChannel, streamId, image -> {}, image -> imageUnavailable.set(true));

            Tests.awaitConnected(pub);
            Tests.awaitConnected(sub);

            while (pub.offer(message) < 0)
            {
                Tests.yield();
            }

            while (0 == sub.poll((buffer, offset, length, header) -> {}, 1))
            {
                Tests.yield();
            }

            final Image image = sub.imageAtIndex(0);
            assertEquals(pub.position(), image.position());

            CloseHelper.close(sub);

            final long t0 = System.nanoTime();
            while (pub.isConnected())
            {
                Tests.yield();
            }
            final long t1 = System.nanoTime();
            assertThat(t1 - t0, lessThan(driver.context().publicationConnectionTimeoutNs()));

            while (!imageUnavailable.get())
            {
                Tests.yield();
            }
        }
    }
}
