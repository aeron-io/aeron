/*
 * Copyright 2014-2024 Real Logic Limited.
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
import io.aeron.logbuffer.BufferClaim;
import io.aeron.test.*;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.LangUtil;
import org.agrona.collections.LongHashSet;
import org.agrona.collections.MutableInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SlowTest
@ExtendWith(InterruptingTestCallback.class)
class ConcurrentPublicationTermRotationRaceTest
{
    private static final int NUM_PUBLISHERS = 3;
    private static final int NUM_MESSAGES = NUM_PUBLISHERS * 50_000;
    private static final int ITERATIONS = 100;
    private TestMediaDriver mediaDriver;
    private Aeron aeron;
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @BeforeEach
    void setup()
    {
        final String aeronDir = CommonContext.AERON_DIR_PROP_DEFAULT + "-concurrent-publication";
        mediaDriver = TestMediaDriver.launch(
            new MediaDriver.Context().dirDeleteOnStart(true).aeronDirectoryName(aeronDir), systemTestWatcher);
        systemTestWatcher.dataCollector().add(mediaDriver.context().aeronDirectory());
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(aeron, mediaDriver);
    }

    @Test
    @InterruptAfter(300)
    void handleTermIdMovingAheadBetweenPositionChecksAndTheTermOffsetIncrement() throws InterruptedException
    {
        for (int i = 0; i < ITERATIONS; i++)
        {
            runTest();
        }
    }

    private void runTest() throws InterruptedException
    {
        final int sessionId = BitUtil.generateRandomisedId();
        final String channel =
            "aeron:ipc?alias=concurrency|term-length=64K|init-term-id=11|term-id=16|term-offset=48896|mtu=8192|" +
            "session-id=" + sessionId;
        final int streamId = 555555;

        try (Subscription subscription = aeron.addSubscription(channel, streamId))
        {
            final CountDownLatch startLatch = new CountDownLatch(NUM_PUBLISHERS + 1);
            final AtomicReference<Throwable> errors = new AtomicReference<>();
            final LongHashSet publisherIds = new LongHashSet();
            final ArrayList<MessagePublisher> publishers = new ArrayList<>(NUM_PUBLISHERS);
            final ArrayList<Publication> publications = new ArrayList<>(NUM_PUBLISHERS);
            for (int i = 0; i < NUM_PUBLISHERS; i++)
            {
                final ConcurrentPublication publication = aeron.addPublication(channel, streamId);
                final MessagePublisher publisher = (i & 1) == 0 ?
                new OfferMessagePublisher(publication, 8160, "offer-" + i, startLatch, errors) :
                new TryClaimMessagePublisher(publication, 7777, "try-claim", startLatch, errors);

                publishers.add(publisher);
                publisherIds.add(publisher.publisherId);
                publisher.start();
                publications.add(publication);
            }

            Tests.awaitConnected(subscription);
            Tests.awaitConnected(publications.get(0));

            startLatch.countDown();
            startLatch.await();

            final MutableInteger msgCount = new MutableInteger();
            final ImageFragmentAssembler fragmentHandler = new ImageFragmentAssembler(
                (buffer, offset, length, header) ->
                {
                    final long threadId = buffer.getLong(offset, LITTLE_ENDIAN);
                    assertTrue(publisherIds.contains(threadId));
                    msgCount.increment();
                });
            final Supplier<String> errorMessageSupplier = () -> "missing messages: expected=" + NUM_MESSAGES +
                ", sent=" + publishers.stream().mapToLong(p -> p.sendCount).sum() +
                ", received=" + msgCount;

            assertEquals(1, subscription.imageCount());
            final Image image = subscription.imageBySessionId(sessionId);

            while (msgCount.get() < NUM_MESSAGES)
            {
                if (0 == image.poll(fragmentHandler, 10))
                {
                    final Throwable err = errors.get();
                    if (null != err)
                    {
                        LangUtil.rethrowUnchecked(err);
                    }
                    Tests.yieldingIdle(errorMessageSupplier);
                }
            }

            for (final MessagePublisher publisher : publishers)
            {
                publisher.join();
            }

            CloseHelper.closeAll(publications);

            final Throwable err = errors.get();
            if (null != err)
            {
                LangUtil.rethrowUnchecked(err);
            }
        }
    }

    abstract static class MessagePublisher extends Thread
    {
        private static final AtomicLong NEXT_ID = new AtomicLong(Integer.MAX_VALUE);
        private final CountDownLatch startLatch;
        private final ConcurrentPublication publication;
        private final int messageSize;
        private final AtomicReference<Throwable> errors;
        final long publisherId = NEXT_ID.getAndIncrement();
        long sendCount;

        MessagePublisher(
            final ConcurrentPublication publication,
            final int messageSize,
            final String name,
            final CountDownLatch startLatch,
            final AtomicReference<Throwable> errors)
        {
            this.publication = publication;
            this.messageSize = messageSize;
            this.startLatch = startLatch;
            this.errors = errors;
            setName(name);
            setDaemon(true);
        }

        public void run()
        {
            startLatch.countDown();
            try
            {
                startLatch.await();

                final int numMessages = NUM_MESSAGES / NUM_PUBLISHERS;

                for (int i = 0; i < numMessages; i++)
                {
                    long position;
                    while ((position = publish(publication, publisherId, messageSize)) < 0)
                    {
                        if (Publication.CLOSED == position ||
                            Publication.MAX_POSITION_EXCEEDED == position ||
                            Publication.NOT_CONNECTED == position)
                        {
                            fail("failed to publish: " + Publication.errorString(position));
                        }
                        Tests.yield();
                    }

                    sendCount++;
                }
            }
            catch (final Throwable t)
            {
                if (!errors.compareAndSet(null, t))
                {
                    errors.get().addSuppressed(t);
                }
            }
        }

        abstract long publish(ConcurrentPublication publication, long payload, int size);
    }

    static final class OfferMessagePublisher extends MessagePublisher
    {
        private final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer(1024);

        OfferMessagePublisher(
            final ConcurrentPublication publication,
            final int messageSize,
            final String name,
            final CountDownLatch startLatch,
            final AtomicReference<Throwable> errors)
        {
            super(publication, messageSize, name, startLatch, errors);
        }

        long publish(final ConcurrentPublication publication, final long payload, final int size)
        {
            msgBuffer.checkLimit(size);
            msgBuffer.putLong(0, payload, LITTLE_ENDIAN);
            return publication.offer(msgBuffer, 0, size);
        }
    }

    static final class TryClaimMessagePublisher extends MessagePublisher
    {
        private final BufferClaim bufferClaim = new BufferClaim();

        TryClaimMessagePublisher(
            final ConcurrentPublication publication,
            final int messageSize,
            final String name,
            final CountDownLatch startLatch,
            final AtomicReference<Throwable> errors)
        {
            super(publication, messageSize, name, startLatch, errors);
        }

        long publish(final ConcurrentPublication publication, final long payload, final int size)
        {
            final long position = publication.tryClaim(size, bufferClaim);
            if (position > 0)
            {
                bufferClaim.buffer().putLong(bufferClaim.offset(), payload, LITTLE_ENDIAN);
                bufferClaim.commit();
            }

            return position;
        }
    }
}
