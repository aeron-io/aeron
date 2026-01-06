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
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.*;
import io.aeron.test.driver.RedirectingNameResolver;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static io.aeron.driver.status.SystemCounterDescriptor.*;
import static io.aeron.test.driver.RedirectingNameResolver.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class NameReResolutionTest
{
    private static final String ENDPOINT_NAME = "ReResTestEndpoint";
    private static final int ENDPOINT_PORT = 24326;
    private static final int CONTROL_PORT = 24327;
    private static final String ENDPOINT_WITH_ERROR_NAME = "ReResWithErrEndpoint";
    private static final String PUBLICATION_MANUAL_MDC_URI =
        "aeron:udp?control=localhost:" + CONTROL_PORT + "|control-mode=manual";
    private static final String PUBLICATION_URI = "aeron:udp?endpoint=" + ENDPOINT_NAME + ":" + ENDPOINT_PORT;
    private static final String PUBLICATION_WITH_ERROR_URI =
        "aeron:udp?endpoint=" + ENDPOINT_WITH_ERROR_NAME + ":" + ENDPOINT_PORT;
    private static final String FIRST_SUBSCRIPTION_URI = "aeron:udp?endpoint=127.0.0.1:" + ENDPOINT_PORT;
    private static final String SECOND_SUBSCRIPTION_URI = "aeron:udp?endpoint=127.0.0.2:" + ENDPOINT_PORT;
    private static final String BAD_ADDRESS = "bad.invalid";

    private static final String CONTROL_NAME = "ReResTestControl";
    private static final String FIRST_PUBLICATION_DYNAMIC_MDC_URI =
        "aeron:udp?control=127.0.0.1:" + CONTROL_PORT + "|control-mode=dynamic|linger=0";
    private static final String SECOND_PUBLICATION_DYNAMIC_MDC_URI =
        "aeron:udp?control=127.0.0.2:" + CONTROL_PORT + "|control-mode=dynamic";
    private static final String SUBSCRIPTION_DYNAMIC_MDC_URI =
        "aeron:udp?control=" + CONTROL_NAME + ":" + CONTROL_PORT + "|control-mode=dynamic";
    private static final String SUBSCRIPTION_MDS_URI = "aeron:udp?control-mode=manual";

    private static final String ENDPOINT_WITH_DELAYED_CONNECT_NAME = "test.delayed.connect";

    private static final String STUB_LOOKUP_CONFIGURATION =
        ENDPOINT_NAME + ",127.0.0.1,127.0.0.2|" +
        CONTROL_NAME + ",127.0.0.1,127.0.0.2|" +
        ENDPOINT_WITH_ERROR_NAME + ",localhost," + BAD_ADDRESS + "|" +
        ENDPOINT_WITH_DELAYED_CONNECT_NAME + ",192.168.0.0,127.0.0.1|";

    private static final int STREAM_ID = 1001;

    private Aeron client;
    private TestMediaDriver driver;
    private Subscription subscription;
    private Publication publication;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @RegisterExtension
    final InterruptingTestCallback testCallback = new InterruptingTestCallback();

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private final FragmentHandler handler = mock(FragmentHandler.class);
    private CountersReader countersReader;

    @BeforeEach
    void before()
    {
        assumeBindAddressAvailable("127.0.0.1");
        assumeBindAddressAvailable("127.0.0.2");

        final MediaDriver.Context context = new MediaDriver.Context()
            .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED)
            .nameResolver(new RedirectingNameResolver(STUB_LOOKUP_CONFIGURATION))
            .nameResolverThresholdNs(1);

        driver = TestMediaDriver.launch(context, systemTestWatcher);
        systemTestWatcher.dataCollector().add(context.aeronDirectory());

        client = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()));
        countersReader = client.countersReader();
    }

    @AfterEach
    void after()
    {
        if (null != client)
        {
            CloseHelper.closeAll(client, driver);
        }
    }

    @SlowTest
    @Test
    @InterruptAfter(20)
    void shouldReResolveEndpointOnNotConnected()
    {
        final long initialResolutionChanges = countersReader.getCounterValue(RESOLUTION_CHANGES.id());

        buffer.putInt(0, 1);

        subscription = client.addSubscription(FIRST_SUBSCRIPTION_URI, STREAM_ID);
        publication = client.addPublication(PUBLICATION_URI, STREAM_ID);

        while (!subscription.isConnected())
        {
            Tests.yieldingIdle("No connect to first subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingIdle("No message offer to first subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingIdle("No message received on first subscription");
        }

        subscription.close();

        // wait for disconnect to ensure we stay in lock step
        while (publication.isConnected())
        {
            Tests.sleep(10);
        }

        subscription = client.addSubscription(SECOND_SUBSCRIPTION_URI, STREAM_ID);
        assertTrue(updateNameResolutionStatus(countersReader, ENDPOINT_NAME, USE_RE_RESOLUTION_HOST));

        while (!subscription.isConnected())
        {
            Tests.yieldingIdle("No connection to second subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingIdle("No message offer to second subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingIdle("No message received on second subscription");
        }

        Tests.awaitCounterDelta(countersReader, RESOLUTION_CHANGES.id(), initialResolutionChanges, 1);

        verify(handler, times(2)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(BitUtil.SIZE_OF_INT),
            any(Header.class));
    }

    @SlowTest
    @Test
    @InterruptAfter(30)
    void shouldReResolveEndpointOnNotConnectedWhenNamePointsBackAtTheOriginalAddress()
    {
        final long initialResolutionChanges = countersReader.getCounterValue(RESOLUTION_CHANGES.id());

        buffer.putInt(0, 1);

        subscription = client.addSubscription(FIRST_SUBSCRIPTION_URI, STREAM_ID);
        publication = client.addPublication(PUBLICATION_URI, STREAM_ID);

        Tests.awaitConnected(publication);
        Tests.awaitConnected(subscription);

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingIdle("No message offer to first subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingIdle("No message received on first subscription");
        }

        subscription.close();

        // wait for disconnect to ensure we stay in lock step
        while (publication.isConnected())
        {
            Tests.sleep(10);
        }

        subscription = client.addSubscription(SECOND_SUBSCRIPTION_URI, STREAM_ID);

        assertTrue(updateNameResolutionStatus(countersReader, ENDPOINT_NAME, USE_RE_RESOLUTION_HOST));
        Tests.awaitConnected(subscription);
        Tests.awaitCounterDelta(countersReader, RESOLUTION_CHANGES.id(), initialResolutionChanges, 1);

        subscription.close();

        // wait for disconnect to ensure we stay in lock step
        while (publication.isConnected())
        {
            Tests.sleep(10);
        }

        subscription = client.addSubscription(FIRST_SUBSCRIPTION_URI, STREAM_ID);

        assertTrue(updateNameResolutionStatus(countersReader, ENDPOINT_NAME, USE_INITIAL_RESOLUTION_HOST));
        Tests.awaitConnected(subscription);
        Tests.awaitCounterDelta(countersReader, RESOLUTION_CHANGES.id(), initialResolutionChanges, 2);
    }

    @SlowTest
    @Test
    @InterruptAfter(20)
    void shouldReResolveMdcManualEndpointOnNotConnected()
    {
        final long initialResolutionChanges = countersReader.getCounterValue(RESOLUTION_CHANGES.id());

        buffer.putInt(0, 1);

        subscription = client.addSubscription(FIRST_SUBSCRIPTION_URI, STREAM_ID);
        publication = client.addPublication(PUBLICATION_MANUAL_MDC_URI, STREAM_ID);
        publication.addDestination(PUBLICATION_URI);

        while (!subscription.isConnected())
        {
            Tests.yieldingIdle("No connect to first subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingIdle("No message offer to first subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingIdle("No message received on first subscription");
        }

        subscription.close();

        // wait for disconnect to ensure we stay in lock step
        while (publication.isConnected())
        {
            Tests.sleep(10);
        }

        subscription = client.addSubscription(SECOND_SUBSCRIPTION_URI, STREAM_ID);
        assertTrue(updateNameResolutionStatus(countersReader, ENDPOINT_NAME, USE_RE_RESOLUTION_HOST));

        while (!subscription.isConnected())
        {
            Tests.yieldingIdle("No connection to second subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingIdle("No message offer to second subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingIdle("No message received on second subscription");
        }

        Tests.awaitCounterDelta(countersReader, RESOLUTION_CHANGES.id(), initialResolutionChanges, 1);

        verify(handler, times(2)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(BitUtil.SIZE_OF_INT),
            any(Header.class));
    }

    @SlowTest
    @Test
    @InterruptAfter(20)
    void shouldHandleMdcManualEndpointInitiallyUnresolved()
    {
        final long initialResolutionChanges = countersReader.getCounterValue(RESOLUTION_CHANGES.id());

        buffer.putInt(0, 1);

        while (!updateNameResolutionStatus(countersReader, ENDPOINT_NAME, DISABLE_RESOLUTION))
        {
            Tests.yieldingIdle("Waiting for naming counter");
        }

        subscription = client.addSubscription(FIRST_SUBSCRIPTION_URI, STREAM_ID);
        publication = client.addPublication(PUBLICATION_MANUAL_MDC_URI, STREAM_ID);
        publication.addDestination(PUBLICATION_URI);

        assertEquals(Publication.NOT_CONNECTED, publication.offer(buffer, 0, BitUtil.SIZE_OF_INT));

        updateNameResolutionStatus(countersReader, ENDPOINT_NAME, USE_INITIAL_RESOLUTION_HOST);

        while (!subscription.isConnected())
        {
            Tests.yieldingIdle("No connection to second subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingIdle("No message offer to second subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingIdle("No message received on second subscription");
        }

        Tests.awaitCounterDelta(countersReader, RESOLUTION_CHANGES.id(), initialResolutionChanges, 1);

        verify(handler, times(1)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(BitUtil.SIZE_OF_INT),
            any(Header.class));
    }

    @SlowTest
    @Test
    @InterruptAfter(20)
    void shouldReResolveMdcDynamicControlOnNotConnected()
    {
        final long initialResolutionChanges = countersReader.getCounterValue(RESOLUTION_CHANGES.id());
        buffer.putInt(0, 1);

        subscription = client.addSubscription(SUBSCRIPTION_DYNAMIC_MDC_URI, STREAM_ID);
        publication = client.addPublication(FIRST_PUBLICATION_DYNAMIC_MDC_URI, STREAM_ID);

        while (!subscription.isConnected())
        {
            Tests.yieldingIdle("No connect to first subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingIdle("No message offer to first subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingIdle("No message received on first subscription");
        }

        publication.close();

        // wait for disconnect to ensure we stay in lock step
        while (subscription.isConnected())
        {
            Tests.sleep(10);
        }

        publication = client.addPublication(SECOND_PUBLICATION_DYNAMIC_MDC_URI, STREAM_ID);
        assertTrue(updateNameResolutionStatus(countersReader, CONTROL_NAME, USE_RE_RESOLUTION_HOST));

        while (!subscription.isConnected())
        {
            Tests.yieldingIdle("No connection to second subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingIdle("No message offer to second subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingIdle("No message received on second subscription");
        }

        Tests.awaitCounterDelta(countersReader, RESOLUTION_CHANGES.id(), initialResolutionChanges, 1);

        verify(handler, times(2)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(BitUtil.SIZE_OF_INT),
            any(Header.class));
    }

    @SlowTest
    @Test
    @InterruptAfter(20)
    void shouldReResolveMdcDynamicControlOnManualDestinationSubscriptionOnNotConnected()
    {
        final long initialResolutionChanges = countersReader.getCounterValue(RESOLUTION_CHANGES.id());

        buffer.putInt(0, 1);

        subscription = client.addSubscription(SUBSCRIPTION_MDS_URI, STREAM_ID);
        subscription.addDestination(SUBSCRIPTION_DYNAMIC_MDC_URI);
        publication = client.addPublication(FIRST_PUBLICATION_DYNAMIC_MDC_URI, STREAM_ID);

        while (!subscription.isConnected())
        {
            Tests.yieldingIdle("No connect to first subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingIdle("No message offer to first subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingIdle("No message received on first subscription");
        }

        publication.close();

        // wait for disconnect to ensure we stay in lock step
        while (subscription.isConnected())
        {
            Tests.sleep(10);
        }

        publication = client.addPublication(SECOND_PUBLICATION_DYNAMIC_MDC_URI, STREAM_ID);
        assertTrue(updateNameResolutionStatus(countersReader, CONTROL_NAME, USE_RE_RESOLUTION_HOST));

        while (!subscription.isConnected())
        {
            Tests.yieldingIdle("No connection to second subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingIdle("No message offer to second subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingIdle("No message received on second subscription");
        }

        Tests.awaitCounterDelta(countersReader, RESOLUTION_CHANGES.id(), initialResolutionChanges, 1);

        verify(handler, times(2)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(BitUtil.SIZE_OF_INT),
            any(Header.class));
    }

    @SlowTest
    @Test
    @InterruptAfter(20)
    void shouldReportErrorOnReResolveFailure() throws IOException
    {
        systemTestWatcher.ignoreErrorsMatching((s) -> s.contains("Unable to resolve host"));
        buffer.putInt(0, 1);

        subscription = client.addSubscription(FIRST_SUBSCRIPTION_URI, STREAM_ID);
        publication = client.addPublication(PUBLICATION_WITH_ERROR_URI, STREAM_ID);
        final long initialErrorCount = client.countersReader().getCounterValue(SystemCounterDescriptor.ERRORS.id());

        while (!subscription.isConnected())
        {
            Tests.yieldingIdle("No connect to first subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingIdle("No message offer to first subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingIdle("No message received on first subscription");
        }

        subscription.close();
        assertTrue(updateNameResolutionStatus(countersReader, ENDPOINT_WITH_ERROR_NAME, USE_RE_RESOLUTION_HOST));

        // wait for disconnect to ensure we stay in lock step
        while (publication.isConnected())
        {
            Tests.sleep(10);
        }

        Tests.awaitCounterDelta(
            client.countersReader(), SystemCounterDescriptor.ERRORS.id(), initialErrorCount, 1);

        final Matcher<String> exceptionMessageMatcher =
            CoreMatchers.containsString("endpoint=" + ENDPOINT_WITH_ERROR_NAME);

        SystemTests.waitForErrorToOccur(
            client.context().aeronDirectoryName(),
            exceptionMessageMatcher,
            new SleepingMillisIdleStrategy(100));
    }

    @Test
    @InterruptAfter(10)
    void shouldTrackNameResolutionTime()
    {
        final long thresholdCounter = countersReader.getCounterValue(NAME_RESOLVER_TIME_THRESHOLD_EXCEEDED.id());

        publication = client.addPublication(PUBLICATION_URI, STREAM_ID);
        publication.close();

        assertThat(countersReader.getCounterValue(NAME_RESOLVER_MAX_TIME.id()), CoreMatchers.is(greaterThan(0L)));
        assertThat(
            countersReader.getCounterValue(NAME_RESOLVER_TIME_THRESHOLD_EXCEEDED.id()),
            CoreMatchers.is(greaterThan(thresholdCounter)));
    }

    @SlowTest
    @Test
    @InterruptAfter(20)
    void shouldReResolveUnicastAddressWhenSendChannelEndpointIsReused()
    {
        subscription = client.addSubscription("aeron:udp?endpoint=127.0.0.1:5555", STREAM_ID);

        final long startTimeNs = System.nanoTime();
        while (true)
        {
            try (Publication pub = client.addPublication(
                "aeron:udp?endpoint=" + ENDPOINT_WITH_DELAYED_CONNECT_NAME + ":5555", STREAM_ID))
            {
                if (null != publication)
                {
                    publication.close();
                    assertTrue(updateNameResolutionStatus(
                        countersReader, ENDPOINT_WITH_DELAYED_CONNECT_NAME, USE_RE_RESOLUTION_HOST));
                }
                publication = pub;

                final long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                do
                {
                    if (pub.isConnected() && subscription.isConnected())
                    {
                        final long timeToReResolutionNs = System.nanoTime() - startTimeNs;
                        final long destinationTimeoutNs = TimeUnit.SECONDS.toNanos(5);
                        assertThat(
                            timeToReResolutionNs,
                            CoreMatchers.allOf(greaterThan(destinationTimeoutNs), lessThan(destinationTimeoutNs * 2)));
                        return;
                    }
                    Tests.sleep(100, () -> "Re-resolution not performed");
                }
                while (System.nanoTime() < deadlineNs);
            }
        }
    }

    @Test
    @SlowTest
    @InterruptAfter(10)
    void shouldHandleTaggedSubscriptionsAddressWithReResolutionToMdcPublications()
    {
        final String taggedUri = SUBSCRIPTION_DYNAMIC_MDC_URI + "|tags=22701";

        subscription = client.addSubscription(taggedUri, STREAM_ID);
        assertFalse(subscription.isConnected());

        assertTrue(updateNameResolutionStatus(countersReader, CONTROL_NAME, USE_RE_RESOLUTION_HOST));

        publication = client.addPublication(SECOND_PUBLICATION_DYNAMIC_MDC_URI, STREAM_ID);

        Tests.awaitConnected(subscription);

        try (Subscription taggedSub1 = client.addSubscription("aeron:udp?tags=22701", STREAM_ID);
            Subscription taggedSub2 = client.addSubscription(taggedUri, STREAM_ID))
        {
            Tests.awaitConnected(taggedSub1);
            Tests.awaitConnected(taggedSub2);
        }
    }

    @Test
    @SlowTest
    @InterruptAfter(10)
    void shouldHandleTaggedPublication()
    {
        final String taggedUri = PUBLICATION_URI + "|tags=22701";

        publication = client.addPublication(taggedUri, STREAM_ID);
        assertFalse(publication.isConnected());

        assertTrue(updateNameResolutionStatus(countersReader, ENDPOINT_NAME, USE_RE_RESOLUTION_HOST));

        subscription = client.addSubscription(SECOND_SUBSCRIPTION_URI, STREAM_ID);

        Tests.awaitConnected(publication);

        try (Publication taggedPub1 = client.addPublication("aeron:udp?tags=22701", STREAM_ID);
            Publication taggedPub2 = client.addPublication(taggedUri, STREAM_ID))
        {
            Tests.awaitConnected(taggedPub1);
            Tests.awaitConnected(taggedPub2);
        }
    }

    @SlowTest
    @Test
    @InterruptAfter(20)
    @Disabled
    void shouldReResolveMdcDynamicControlWhenPublicationChangesIPAddress()
    {
        final long initialResolutionChanges = countersReader.getCounterValue(RESOLUTION_CHANGES.id());
        buffer.putInt(0, 1);

        subscription = client.addSubscription(SUBSCRIPTION_DYNAMIC_MDC_URI, STREAM_ID);
        final Publication publication2 = client.addPublication(FIRST_PUBLICATION_DYNAMIC_MDC_URI, 2222);
        publication = client.addPublication(FIRST_PUBLICATION_DYNAMIC_MDC_URI, STREAM_ID);

        while (!subscription.isConnected())
        {
            Tests.yieldingIdle("No connect to first subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingIdle("No message offer to first subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingIdle("No message received on subscription");
        }

        publication.close();

        while (subscription.isConnected())
        {
            Tests.sleep(10);
        }

        publication = client.addPublication(SECOND_PUBLICATION_DYNAMIC_MDC_URI, STREAM_ID);
        assertTrue(updateNameResolutionStatus(countersReader, CONTROL_NAME, USE_RE_RESOLUTION_HOST));

        while (!subscription.isConnected())
        {
            Tests.sleep(10);
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingIdle("No message offer to subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingIdle("No message received on subscription");
        }

        Tests.awaitCounterDelta(countersReader, RESOLUTION_CHANGES.id(), initialResolutionChanges, 1);

        verify(handler, times(2)).onFragment(
          any(DirectBuffer.class),
          anyInt(),
          eq(BitUtil.SIZE_OF_INT),
          any(Header.class));
    }

    private static void assumeBindAddressAvailable(final String address)
    {
        final String message = NetworkTestingUtil.isBindAddressAvailable(address);
        Assumptions.assumeTrue(null == message, message);
    }
}
