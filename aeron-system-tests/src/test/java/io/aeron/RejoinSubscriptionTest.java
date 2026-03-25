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
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reproduces the ON_COOL_DOWN stuck state bug: when all {@code rejoin=false} subscriptions
 * lose their image (non-EOS removal), {@code transitionToLinger} skips {@code removeCoolDown},
 * permanently blocking that session in the {@code DataPacketDispatcher}. A service that closes
 * and reopens its subscription cannot get a new image while another subscription keeps the
 * stream ref count above zero.
 * <p>
 * Uses two media drivers so that killing the publisher's driver simulates a crash without
 * sending EOS. The publisher uses a fixed session id so the restarted publisher reuses the
 * same session, hitting the ON_COOL_DOWN entry.
 */
@ExtendWith(InterruptingTestCallback.class)
class RejoinSubscriptionTest
{
    private static final int STREAM_ID = 1001;
    private static final int SESSION_ID = 42;
    private static final String CHANNEL = "aeron:udp?endpoint=localhost:24325|term-length=64k";
    private static final String PUB_CHANNEL = CHANNEL + "|session-id=" + SESSION_ID;

    @TempDir
    private Path tempDir;

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);

    private TestMediaDriver driverPub;
    private TestMediaDriver driverSub;
    private Aeron pubClient;
    private Aeron subClient1;
    private Aeron subClient2;

    @AfterEach
    void after()
    {
        CloseHelper.quietCloseAll(subClient2, subClient1, pubClient, driverSub, driverPub);
    }

    private void launchSubDriver()
    {
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .aeronDirectoryName(tempDir.resolve("sub").toString())
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED)
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(20))
            .imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500))
            .errorHandler(Tests::onError);

        driverSub = TestMediaDriver.launch(ctx, testWatcher);
        testWatcher.dataCollector().add(ctx.aeronDirectory());
    }

    private void launchPubDriver()
    {
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .aeronDirectoryName(tempDir.resolve("pub").toString())
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED)
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(20))
            .publicationConnectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500))
            .errorHandler(Tests::onError);

        driverPub = TestMediaDriver.launch(ctx, testWatcher);
        testWatcher.dataCollector().add(ctx.aeronDirectory());
    }

    /**
     * Two services (clients) on the subscriber driver subscribe with {@code rejoin=false}.
     * The publisher is on a separate driver with a fixed session id. When the publisher's
     * driver is killed (simulating a crash, no EOS), both subscriptions lose their images
     * after the liveness timeout, and ON_COOL_DOWN is set for the session.
     * <p>
     * Service 1 restarts (closes and reopens its subscription). A new publisher starts on
     * the same channel with the same session id. Because service 2's subscription keeps
     * the stream ref count above zero, neither {@code removeSubscription} nor
     * {@code addSubscription} reaches the {@code DataPacketDispatcher}. ON_COOL_DOWN
     * persists and blocks image creation for the restarted publisher's session.
     * <p>
     * The restarted service's new subscription should receive an image.
     */
    @Test
    @InterruptAfter(30)
    void shouldRecoverAfterResubscribeWithRejoinFalseWhenOtherSubscriptionKeepsStreamAlive()
    {
        launchPubDriver();
        launchSubDriver();

        pubClient = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(driverPub.context().aeronDirectoryName()));

        subClient1 = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(driverSub.context().aeronDirectoryName()));

        subClient2 = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(driverSub.context().aeronDirectoryName()));

        final FragmentHandler emptyHandler = (buffer, offset, length, header) -> {};

        final AtomicInteger sub1UnavailableCount = new AtomicInteger();
        final AtomicInteger sub2UnavailableCount = new AtomicInteger();

        Subscription subscription1 = subClient1.addSubscription(
            CHANNEL + "|rejoin=false", STREAM_ID,
            image -> {},
            image -> sub1UnavailableCount.incrementAndGet());

        final Subscription subscription2 = subClient2.addSubscription(
            CHANNEL + "|rejoin=false", STREAM_ID,
            image -> {},
            image -> sub2UnavailableCount.incrementAndGet());

        Publication publication = pubClient.addPublication(PUB_CHANNEL, STREAM_ID);

        while (!subscription1.isConnected() || !subscription2.isConnected())
        {
            Tests.yield();
        }

        while (publication.offer(buffer) < 0)
        {
            Tests.yield();
        }

        while (subscription1.poll(emptyHandler, 10) == 0)
        {
            Tests.yield();
        }

        while (subscription2.poll(emptyHandler, 10) == 0)
        {
            Tests.yield();
        }

        // Phase 2: Kill the publisher's driver.
        CloseHelper.quietCloseAll(pubClient, driverPub);

        while (sub1UnavailableCount.get() < 1 || sub2UnavailableCount.get() < 1)
        {
            Tests.yield();
        }

        // Phase 3: Service 1 restarts — closes old subscription, adds new one.
        // Stream ref count on ReceiveChannelEndpoint: 2→1→2.
        // Neither removeSubscription nor addSubscription reaches the dispatcher.
        subscription1.close();

        final AtomicInteger newAvailableCount = new AtomicInteger();

        subscription1 = subClient1.addSubscription(
            CHANNEL + "|rejoin=false", STREAM_ID,
            image -> newAvailableCount.incrementAndGet(),
            image -> {});

        // Phase 4: New publisher starts with the SAME session id.
        launchPubDriver();

        pubClient = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(driverPub.context().aeronDirectoryName()));

        publication = pubClient.addPublication(PUB_CHANNEL, STREAM_ID);

        // The new subscription should get an image for the restarted publisher's session.
        while (newAvailableCount.get() < 1)
        {
            Tests.yield();
        }

        // Verify data flows on the new image.
        while (publication.offer(buffer) < 0)
        {
            Tests.yield();
        }

        while (subscription1.poll(emptyHandler, 10) == 0)
        {
            Tests.yield();
        }
    }
}
