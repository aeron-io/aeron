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
import io.aeron.test.*;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.aeron.Publication.CLOSED;
import static io.aeron.driver.status.SystemCounterDescriptor.PUBLICATIONS_REVOKED;
import static io.aeron.driver.status.SystemCounterDescriptor.PUBLICATION_IMAGES_REVOKED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class PublicationRevokeTest
{
    private static final int TERM_BUFFER_LENGTH = 1024 * 1024;
    private static final int STREAM_ID = 1001;
    private static final String UDP_CHANNEL = "aeron:udp?endpoint=localhost:24325";
    private static final String IPC_CHANNEL = "aeron:ipc";
    private static final String MCAST_CHANNEL = "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost";
    private static final String PUB_MDC_MANUAL_URI = "aeron:udp?control-mode=manual";
    private static final String SUB1_MDC_MANUAL_URI = "aeron:udp?endpoint=localhost:24326|group=true";
    private static final String SUB2_MDC_MANUAL_URI = "aeron:udp?endpoint=localhost:24327|group=true";

    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private final MediaDriver.Context driverContext = new MediaDriver.Context()
        .aeronDirectoryName(CommonContext.generateRandomDirName())
        .ipcTermBufferLength(TERM_BUFFER_LENGTH)
        .publicationTermBufferLength(TERM_BUFFER_LENGTH)
        .publicationConnectionTimeoutNs(MILLISECONDS.toNanos(300))
        .imageLivenessTimeoutNs(MILLISECONDS.toNanos(500))
        .publicationLingerTimeoutNs(SECONDS.toNanos(1))
        .timerIntervalNs(MILLISECONDS.toNanos(100))
        .dirDeleteOnStart(true)
        .threadingMode(ThreadingMode.SHARED);

    private final Aeron.Context clientContext = new Aeron.Context()
        .resourceLingerDurationNs(MILLISECONDS.toNanos(200));

    private Aeron client;
    private TestMediaDriver driver;
    private Aeron clientB;
    private TestMediaDriver driverB;
    private CountersReader countersReader;
    private CountersReader countersReaderB;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[8192]);
    private final FragmentHandler fragmentHandler = mock(FragmentHandler.class);
    private final AvailableImageHandler availableImageHandler = mock(AvailableImageHandler.class);
    private final UnavailableImageHandler unavailableImageHandler = mock(UnavailableImageHandler.class);

    private void launch()
    {
        driver = TestMediaDriver.launch(driverContext.clone(), watcher);
        watcher.dataCollector().add(driver.context().aeronDirectory());

        client = Aeron.connect(clientContext.clone().aeronDirectoryName(driver.aeronDirectoryName()));

        countersReader = client.countersReader();

        buffer.putInt(0, 1);
    }

    private void launch2()
    {
        driver = TestMediaDriver.launch(driverContext.clone(), watcher);
        watcher.dataCollector().add(driver.context().aeronDirectory());
        driverB = TestMediaDriver.launch(
            driverContext.clone().aeronDirectoryName(CommonContext.generateRandomDirName()), watcher);
        watcher.dataCollector().add(driverB.context().aeronDirectory());

        client = Aeron.connect(clientContext.clone().aeronDirectoryName(driver.aeronDirectoryName()));
        clientB = Aeron.connect(clientContext.clone().aeronDirectoryName(driverB.aeronDirectoryName()));

        countersReader = client.countersReader();
        countersReaderB = clientB.countersReader();

        buffer.putInt(0, 1);
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(client, driver, clientB, driverB);
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

        final Subscription subscription = client.addSubscription(
            subscriptionChannel, STREAM_ID, availableImageHandler, unavailableImageHandler);
        try (ExclusivePublication exclusivePublication = client.addExclusivePublication(publicationChannel, STREAM_ID))
        {

            Tests.awaitConnected(subscription);
            Tests.awaitConnected(exclusivePublication);

            publishMessage(exclusivePublication);

            pollUntilFragments(subscription, 1);

            publishMessage(exclusivePublication);

            exclusivePublication.revokeOnClose();
        }

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
    void revokeTestExclusive(
        final String subscriptionChannel,
        final String publicationChannel,
        final long expectedPublicationImagesRevoked)
    {
        final AtomicBoolean publicationShouldBeRevoked = new AtomicBoolean(true);
        final AtomicInteger unavailableImages = new AtomicInteger(0);
        doAnswer(invocation ->
        {
            final Image image = invocation.getArgument(0, Image.class);
            assertEquals(publicationShouldBeRevoked.get(), image.isPublicationRevoked());

            unavailableImages.incrementAndGet();
            return null;
        }).when(unavailableImageHandler).onUnavailableImage(any(Image.class));

        launch();

        final Subscription subscription = client.addSubscription(
            subscriptionChannel, STREAM_ID, availableImageHandler, unavailableImageHandler);
        final ExclusivePublication exclusivePublication = client.addExclusivePublication(publicationChannel, STREAM_ID);

        Tests.awaitConnected(subscription);
        Tests.awaitConnected(exclusivePublication);

        final ExclusivePublication publicationTwo = client.addExclusivePublication(publicationChannel, STREAM_ID);

        Tests.awaitConnected(publicationTwo);

        publishMessage(exclusivePublication);
        publishMessage(publicationTwo);

        pollUntilFragments(subscription, 2);

        publishMessage(exclusivePublication);

        assertEquals(2, subscription.imageCount());

        exclusivePublication.revoke();

        assertEquals(CLOSED, exclusivePublication.offer(buffer, 0, SIZE_OF_INT));

        while (unavailableImages.get() == 0)
        {
            Tests.yield();
        }

        assertEquals(1, subscription.imageCount());

        publishMessage(publicationTwo);
        pollUntilFragments(subscription, 1);

        publicationShouldBeRevoked.set(false);
        subscription.close();

        publicationTwo.close();

        assertEquals(1, countersReader.getCounterValue(PUBLICATIONS_REVOKED.id()));
        assertEquals(expectedPublicationImagesRevoked, countersReader.getCounterValue(PUBLICATION_IMAGES_REVOKED.id()));
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldRevokeBeforeAllMessagesConsumed(
        final String subscriptionChannel,
        final String publicationChannel,
        final long expectedPublicationImagesRevoked)
    {
        final AtomicInteger unavailableImages = new AtomicInteger(0);
        doAnswer(invocation ->
        {
            unavailableImages.incrementAndGet();
            return null;
        }).when(unavailableImageHandler).onUnavailableImage(any(Image.class));

        launch();

        final Subscription subscription = client.addSubscription(
            subscriptionChannel, STREAM_ID, availableImageHandler, unavailableImageHandler);
        final ExclusivePublication exclusivePublication = client.addExclusivePublication(publicationChannel, STREAM_ID);

        Tests.awaitConnected(subscription);
        Tests.awaitConnected(exclusivePublication);
        Tests.await(() -> exclusivePublication.availableWindow() > 0);

        final long availableWindow = exclusivePublication.availableWindow();
        while (exclusivePublication.position() < availableWindow)
        {
            if (exclusivePublication.offer(buffer, 0, SIZE_OF_INT) < 0)
            {
                Tests.yield();
            }
        }
        assertEquals(availableWindow, exclusivePublication.position());

        int messagesReceived = 0;
        while (unavailableImages.get() == 0)
        {
            messagesReceived += subscription.poll(fragmentHandler, 1);

            if (100 == messagesReceived)
            {
                exclusivePublication.revoke();
                break;
            }

            Tests.yield();
        }

        Tests.await(() -> 1 == unavailableImages.get());

        assertEquals(1, countersReader.getCounterValue(PUBLICATIONS_REVOKED.id()));
        assertEquals(expectedPublicationImagesRevoked, countersReader.getCounterValue(PUBLICATION_IMAGES_REVOKED.id()));
    }

    @Test
    @InterruptAfter(10)
    void shouldRevokeMultipleSubscribersMulticast()
    {
        final AtomicInteger unavailableImages = new AtomicInteger(0);
        doAnswer(invocation ->
        {
            final Image image = invocation.getArgument(0, Image.class);
            assertTrue(image.isPublicationRevoked());

            unavailableImages.incrementAndGet();
            return null;
        }).when(unavailableImageHandler).onUnavailableImage(any(Image.class));

        launch2();

        final Subscription subscription = client.addSubscription(
            MCAST_CHANNEL, STREAM_ID, availableImageHandler, unavailableImageHandler);
        final Subscription subscriptionB = clientB.addSubscription(
            MCAST_CHANNEL, STREAM_ID, availableImageHandler, unavailableImageHandler);
        final ExclusivePublication exclusivePublication = client.addExclusivePublication(MCAST_CHANNEL, STREAM_ID);

        Tests.awaitConnected(subscription);
        Tests.awaitConnected(subscriptionB);
        Tests.awaitConnected(exclusivePublication);
        Tests.await(() -> exclusivePublication.availableWindow() > 0);

        final long availableWindow = exclusivePublication.availableWindow();
        while (exclusivePublication.position() < availableWindow)
        {
            if (exclusivePublication.offer(buffer, 0, SIZE_OF_INT) < 0)
            {
                Tests.yield();
            }
        }
        assertEquals(availableWindow, exclusivePublication.position());

        int messagesReceived = 0;
        int messagesReceivedB = 0;
        while (true)
        {
            messagesReceived += subscription.poll(fragmentHandler, 1);
            messagesReceivedB += subscriptionB.poll(fragmentHandler, 1);

            if (messagesReceived >= 100 && messagesReceivedB >= 100)
            {
                exclusivePublication.revoke();
                Tests.sleep(100);
                break;
            }

            Tests.yield();
        }

        Tests.await(() -> 2 == unavailableImages.get());

        assertEquals(1, countersReader.getCounterValue(PUBLICATIONS_REVOKED.id()));
        assertEquals(1, countersReader.getCounterValue(PUBLICATION_IMAGES_REVOKED.id()));
        assertEquals(0, countersReaderB.getCounterValue(PUBLICATIONS_REVOKED.id()));
        assertEquals(1, countersReaderB.getCounterValue(PUBLICATION_IMAGES_REVOKED.id()));
    }

    @Test
    @InterruptAfter(10)
    void shouldRevokeMultipleSubscribersMdc()
    {
        final AtomicInteger unavailableImages = new AtomicInteger(0);
        doAnswer(invocation ->
        {
            final Image image = invocation.getArgument(0, Image.class);
            assertTrue(image.isPublicationRevoked());

            unavailableImages.incrementAndGet();
            return null;
        }).when(unavailableImageHandler).onUnavailableImage(any(Image.class));

        launch2();

        final Subscription subscription = client.addSubscription(
            SUB1_MDC_MANUAL_URI, STREAM_ID, availableImageHandler, unavailableImageHandler);
        final Subscription subscriptionB = clientB.addSubscription(
            SUB2_MDC_MANUAL_URI, STREAM_ID, availableImageHandler, unavailableImageHandler);
        final ExclusivePublication exclusivePublication = client.addExclusivePublication(PUB_MDC_MANUAL_URI, STREAM_ID);
        exclusivePublication.addDestination(SUB1_MDC_MANUAL_URI);
        exclusivePublication.addDestination(SUB2_MDC_MANUAL_URI);

        Tests.awaitConnected(subscription);
        Tests.awaitConnected(subscriptionB);
        Tests.awaitConnected(exclusivePublication);
        Tests.await(() -> exclusivePublication.availableWindow() > 0);

        final long availableWindow = exclusivePublication.availableWindow();
        while (exclusivePublication.position() < availableWindow)
        {
            if (exclusivePublication.offer(buffer, 0, SIZE_OF_INT) < 0)
            {
                Tests.yield();
            }
        }
        assertEquals(availableWindow, exclusivePublication.position());

        int messagesReceived = 0;
        int messagesReceivedB = 0;
        while (true)
        {
            messagesReceived += subscription.poll(fragmentHandler, 1);
            messagesReceivedB += subscriptionB.poll(fragmentHandler, 1);

            if (messagesReceived >= 100 && messagesReceivedB >= 100)
            {
                exclusivePublication.revoke();
                Tests.sleep(100);
                break;
            }

            Tests.yield();
        }

        Tests.await(() -> 2 == unavailableImages.get());

        assertEquals(1, countersReader.getCounterValue(PUBLICATIONS_REVOKED.id()));
        assertEquals(1, countersReader.getCounterValue(PUBLICATION_IMAGES_REVOKED.id()));
        assertEquals(0, countersReaderB.getCounterValue(PUBLICATIONS_REVOKED.id()));
        assertEquals(1, countersReaderB.getCounterValue(PUBLICATION_IMAGES_REVOKED.id()));
    }

    private void publishMessage(final Publication publication)
    {
        while (publication.offer(buffer, 0, SIZE_OF_INT) < 0L)
        {
            Tests.yield();
        }
    }

    private void pollUntilFragments(final Subscription subscription, final int expectedFragments)
    {
        int totalFragments = pollForFragment(subscription);
        while (totalFragments < expectedFragments)
        {
            Tests.yield();
            totalFragments += pollForFragment(subscription);
        }
    }

    private int pollForFragment(final Subscription subscription)
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

    private static Stream<Arguments> channels()
    {
        return Stream.of(
            Arguments.of(UDP_CHANNEL, UDP_CHANNEL, 1),
            Arguments.of(IPC_CHANNEL, IPC_CHANNEL, 0),
            Arguments.of(CommonContext.SPY_PREFIX + UDP_CHANNEL, UDP_CHANNEL + "|ssc=true", 0)
        );
    }
}
