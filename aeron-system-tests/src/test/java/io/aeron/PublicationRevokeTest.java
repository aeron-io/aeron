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
import io.aeron.logbuffer.*;
import io.aeron.test.*;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.aeron.Publication.CLOSED;
import static io.aeron.driver.status.SystemCounterDescriptor.PUBLICATIONS_REVOKED;
import static io.aeron.driver.status.SystemCounterDescriptor.PUBLICATION_IMAGES_REVOKED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(InterruptingTestCallback.class)
class PublicationRevokeTest
{
    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private static final String UDP_CHANNEL = "aeron:udp?endpoint=localhost:24325";
    private static final String IPC_CHANNEL = "aeron:ipc";

    private static Stream<Arguments> channels()
    {
        return Stream.of(
            Arguments.of(UDP_CHANNEL, UDP_CHANNEL, 1),
            Arguments.of(IPC_CHANNEL, IPC_CHANNEL, 0),
            Arguments.of(CommonContext.SPY_PREFIX + UDP_CHANNEL, UDP_CHANNEL + "|ssc=true", 0)
        );
    }

    private static final int STREAM_ID = 1001;

    private final MediaDriver.Context driverContext = new MediaDriver.Context()
        .publicationConnectionTimeoutNs(MILLISECONDS.toNanos(300))
        .imageLivenessTimeoutNs(MILLISECONDS.toNanos(500))
        .timerIntervalNs(MILLISECONDS.toNanos(100));

    private final Aeron.Context clientContext = new Aeron.Context()
        .resourceLingerDurationNs(MILLISECONDS.toNanos(200))
        .idleSleepDurationNs(MILLISECONDS.toNanos(100));

    private Aeron client;
    private TestMediaDriver driver;
    private CountersReader countersReader;
    private Subscription subscription;
    private Publication publication;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[8192]);
    private final FragmentHandler fragmentHandler = mock(FragmentHandler.class);
    private final AvailableImageHandler availableImageHandler = mock(AvailableImageHandler.class);
    private final UnavailableImageHandler unavailableImageHandler = mock(UnavailableImageHandler.class);

    private void launch()
    {
        driverContext.dirDeleteOnStart(true).threadingMode(ThreadingMode.SHARED);

        driver = TestMediaDriver.launch(driverContext, watcher);
        watcher.dataCollector().add(driver.context().aeronDirectory());

        client = Aeron.connect(clientContext.clone());

        countersReader = client.countersReader();

        buffer.putInt(0, 1);
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(client, driver);
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void revokeTestSimple(
        final String subscriptionChannel,
        final String publicationChannel,
        final long expectedPublicationImagesRevoked)
    {
        final AtomicInteger unavailableImages = new AtomicInteger(0);
        doAnswer(invocation ->
        {
            final Image image = invocation.getArgument(0, Image.class);
            assertTrue(image.isPublicationRevoked());

            unavailableImages.incrementAndGet();
            return null;
        }).when(unavailableImageHandler).onUnavailableImage(any(Image.class));

        launch();

        subscription = client.addSubscription(subscriptionChannel, STREAM_ID, availableImageHandler, unavailableImageHandler);
        publication = client.addPublication(publicationChannel, STREAM_ID);

        Tests.awaitConnected(subscription);
        Tests.awaitConnected(publication);

        publishMessage();
        assertEquals(1, pollForFragment());

        publishMessage();

        publication.revoke();
        assertTrue(publication.isRevoked());

        assertEquals(CLOSED, publication.offer(buffer, 0, SIZE_OF_INT));

        while (unavailableImages.get() == 0)
        {
            Tests.yield();
        }

        assertTrue(subscription.hasNoImages());

        assertEquals(1, countersReader.getCounterValue(PUBLICATIONS_REVOKED.id()));
        assertEquals(expectedPublicationImagesRevoked, countersReader.getCounterValue(PUBLICATION_IMAGES_REVOKED.id()));
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void revokeTestConcurrent(
        final String subscriptionChannel,
        final String publicationChannel,
        final long expectedPublicationImagesRevoked)
    {
        final AtomicInteger unavailableImages = new AtomicInteger(0);
        doAnswer(invocation ->
        {
            final Image image = invocation.getArgument(0, Image.class);
            assertTrue(image.isPublicationRevoked());

            unavailableImages.incrementAndGet();
            return null;
        }).when(unavailableImageHandler).onUnavailableImage(any(Image.class));

        launch();

        subscription = client.addSubscription(subscriptionChannel, STREAM_ID, availableImageHandler, unavailableImageHandler);
        publication = client.addPublication(publicationChannel, STREAM_ID);

        Tests.awaitConnected(subscription);
        Tests.awaitConnected(publication);

        Publication publicationTwo = client.addPublication(publicationChannel, STREAM_ID);

        Tests.awaitConnected(publicationTwo);

        publishMessage();
        publishMessage(publicationTwo);

        assertEquals(2, pollForFragment());

        publishMessage();

        publication.revoke();
        assertTrue(publication.isRevoked());

        assertEquals(CLOSED, publication.offer(buffer, 0, SIZE_OF_INT));

        while (unavailableImages.get() == 0)
        {
            Tests.yield();
        }

        assertTrue(subscription.hasNoImages());

        while (!publicationTwo.isClosed())
        {
            Tests.yield();
        }

        assertTrue(publicationTwo.isRevoked());

        assertEquals(1, countersReader.getCounterValue(PUBLICATIONS_REVOKED.id()));
        assertEquals(expectedPublicationImagesRevoked, countersReader.getCounterValue(PUBLICATION_IMAGES_REVOKED.id()));
    }

    private void publishMessage()
    {
        publishMessage(publication);
    }

    private void publishMessage(final Publication pub)
    {
        while (pub.offer(buffer, 0, SIZE_OF_INT) < 0L)
        {
            Tests.yield();
        }
    }

    private int pollForFragment()
    {
        while (true)
        {
            final int fragments = subscription.poll(fragmentHandler, 10);
            if (fragments > 0)
            {
                return fragments;
            }

            Tests.yield();
        }
    }
}
