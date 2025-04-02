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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.aeron.Publication.CLOSED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(InterruptingTestCallback.class)
class PublicationRevokeTest
{
    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

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
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(client, driver);
    }

    @Test
    @InterruptAfter(10)
    void revokeTest1() throws Exception
    {
        doAnswer(invocation ->
        {
            System.err.println(" -- unavailable image");
            return null;
        }).when(unavailableImageHandler).onUnavailableImage(any(Image.class));

        launch();

        final String channel = "aeron:udp?endpoint=localhost:24325";

        subscription = client.addSubscription(channel, STREAM_ID, availableImageHandler, unavailableImageHandler);
        publication = client.addPublication(channel, STREAM_ID);

        publishMessage();

        pollForFragment();

        publishMessage();

        Thread.sleep(300);

        publication.revoke();
        //publication.close();

        buffer.putInt(0, 1);
        assertEquals(CLOSED, publication.offer(buffer, 0, SIZE_OF_INT));

        Thread.sleep(300);

        if (subscription.images().isEmpty())
        {
            System.err.println("NO IMAGES!!!");
        }
        else
        {
            pollForFragment();
        }
    }

    @Test
    @InterruptAfter(10)
    void revokeTestConcurrent() throws Exception
    {
        doAnswer(invocation ->
        {
            System.err.println(" -- unavailable image");
            return null;
        }).when(unavailableImageHandler).onUnavailableImage(any(Image.class));

        launch();

        final String channel = "aeron:udp?endpoint=localhost:24325";

        subscription = client.addSubscription(channel, STREAM_ID, availableImageHandler, unavailableImageHandler);
        publication = client.addPublication(channel, STREAM_ID);

        Publication publicationTwo = client.addPublication(channel, STREAM_ID);

        publishMessage();
        publishMessage(publicationTwo);

        Thread.sleep(200);

        pollForFragment();

        publishMessage();

        Thread.sleep(300);

        publication.revoke();
        //publication.close();

        buffer.putInt(0, 1);
        assertEquals(CLOSED, publication.offer(buffer, 0, SIZE_OF_INT));

        Thread.sleep(300);

        if (subscription.images().isEmpty())
        {
            System.err.println("NO IMAGES!!!");
        }
        else
        {
            pollForFragment();
        }

        System.err.println("pub two revoked? :: " + publicationTwo.isRevoked());
        System.err.println("pub two closed?  :: " + publicationTwo.isClosed());

        {
            buffer.putInt(0, 1);

            long x = publicationTwo.offer(buffer, 0, SIZE_OF_INT);
            System.err.println("x == " + x);
        }
    }

    @Test
    @InterruptAfter(10)
    void revokeTestIPC() throws Exception
    {
        doAnswer(invocation ->
        {
            System.err.println(" -- unavailable image");
            return null;
        }).when(unavailableImageHandler).onUnavailableImage(any(Image.class));

        launch();

        final String channel = "aeron:ipc";

        subscription = client.addSubscription(channel, STREAM_ID, availableImageHandler, unavailableImageHandler);
        publication = client.addPublication(channel, STREAM_ID);

        publishMessage();

        pollForFragment();

        publishMessage();

        Thread.sleep(300);

        publication.revoke();
        //publication.close();

        buffer.putInt(0, 1);
        assertEquals(CLOSED, publication.offer(buffer, 0, SIZE_OF_INT));

        Thread.sleep(300);

        if (subscription.images().isEmpty())
        {
            System.err.println("NO IMAGES!!!");
        }
        else
        {
            pollForFragment();
        }
    }

    @Test
    @InterruptAfter(10)
    void revokeTestSpy() throws Exception
    {
        doAnswer(invocation ->
        {
            System.err.println(" -- unavailable image");
            return null;
        }).when(unavailableImageHandler).onUnavailableImage(any(Image.class));

        doAnswer(invocation ->
        {
            System.err.println(" ++++ available image");
            return null;
        }).when(availableImageHandler).onAvailableImage(any(Image.class));

        launch();

        final String channel = "aeron:udp?endpoint=localhost:24325";

        subscription = client.addSubscription(CommonContext.SPY_PREFIX + channel, STREAM_ID, availableImageHandler, unavailableImageHandler);
        publication = client.addPublication(channel + "|ssc=true", STREAM_ID);

        publishMessage();

        pollForFragment();

        publishMessage();

        Thread.sleep(300);

        publication.revoke();
        //publication.close();

        buffer.putInt(0, 1);
        assertEquals(CLOSED, publication.offer(buffer, 0, SIZE_OF_INT));

        Thread.sleep(300);

        if (subscription.images().isEmpty())
        {
            System.err.println("NO IMAGES!!!");
        }
        else
        {
            pollForFragment();
        }
    }

    private void publishMessage()
    {
        publishMessage(publication);
    }

    private void publishMessage(final Publication pub)
    {
        buffer.putInt(0, 1);

        while (pub.offer(buffer, 0, SIZE_OF_INT) < 0L)
        {
            Tests.yield();
        }
    }

    private void pollForFragment()
    {
        while (true)
        {
            final int fragments = subscription.poll(fragmentHandler, 10);
            if (fragments > 0)
            {
                System.err.println("got frags :: " + fragments);
                break;
            }

            Tests.yield();
        }
    }
}
