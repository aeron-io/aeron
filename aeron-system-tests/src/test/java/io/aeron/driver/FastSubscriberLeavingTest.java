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
package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class FastSubscriberLeavingTest
{
    @RegisterExtension
    private final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();
    private MediaDriver driver;
    private Aeron aeron;

    @BeforeEach
    void before()
    {
        driver = MediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(CommonContext.generateRandomDirName())
                .threadingMode(ThreadingMode.INVOKER)
                .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
                .ipcTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH));
        systemTestWatcher.dataCollector().add(driver.context().aeronDirectory());

        aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(driver.aeronDirectoryName())
            .useConductorAgentInvoker(true)
            .driverAgentInvoker(driver.sharedAgentInvoker()));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    @Test
    @InterruptAfter(10)
    void shouldCaptureGreatestConsumedPositionWhenFastestSpyLeaves()
    {
        final String pubUri = "aeron:udp?endpoint=localhost:5555|ssc=true";
        final String subUri = "aeron-spy:aeron:udp?endpoint=localhost:5555";
        final int streamId = 43;
        final ExclusivePublication publication = aeron.addExclusivePublication(pubUri, streamId);
        final Subscription spySubscription = aeron.addSubscription(subUri, streamId);
        final Subscription shadowSpySubscription = aeron.addSubscription(subUri, streamId);
        while (publication.availableWindow() <= 0 ||
            !spySubscription.isConnected() ||
            !shadowSpySubscription.isConnected())
        {
            driver.sharedAgentInvoker().invoke();
            aeron.conductorAgentInvoker().invoke();
            Tests.yield();
        }

        final UnsafeBuffer data = new UnsafeBuffer(new byte[100]);
        ThreadLocalRandom.current().nextBytes(data.byteArray());
        while (publication.offer(data) < 0)
        {
            driver.sharedAgentInvoker().invoke();
            aeron.conductorAgentInvoker().invoke();
            Tests.yield();
        }

        final long pubPos = publication.position();
        final CountersReader countersReader = aeron.countersReader();
        final Image image = spySubscription.imageBySessionId(publication.sessionId());
        final FragmentHandler fragmentHandler = (buffer, offset, length, header) ->
        {
            assertEquals(data.capacity(), length);
            for (int i = 0; i < length; i++)
            {
                assertEquals(data.getByte(i), buffer.getByte(offset + i));
            }
        };
        while (image.position() < pubPos)
        {
            if (0 == image.poll(fragmentHandler, 1))
            {
                Tests.yield();
            }
        }

        final int subPosCounterId = image.subscriberPositionId();

        // close Subscription and invoke MediaDriver so that it removes it before updating the positions
        spySubscription.close();

        do
        {
            driver.sharedAgentInvoker().invoke();
            aeron.conductorAgentInvoker().invoke();
        }
        while (countersReader.getCounterState(subPosCounterId) == CountersReader.RECORD_ALLOCATED);

        // await for `snd-pos` to be updated based on the correct max spy position
        final int sndPosCounterId = countersReader.findByTypeIdAndRegistrationId(
            AeronCounters.DRIVER_SENDER_POSITION_TYPE_ID, publication.registrationId());
        while (countersReader.getCounterValue(sndPosCounterId) < pubPos)
        {
            driver.sharedAgentInvoker().invoke();
            aeron.conductorAgentInvoker().invoke();
        }

        final Subscription spySubscription2 = aeron.addSubscription(subUri, streamId);
        while (!spySubscription2.isConnected())
        {
            driver.sharedAgentInvoker().invoke();
            aeron.conductorAgentInvoker().invoke();
        }
        final Image image2 = spySubscription2.imageBySessionId(publication.sessionId());
        assertEquals(pubPos, image2.joinPosition());
    }

    @Test
    @InterruptAfter(10)
    void shouldCaptureGreatestConsumedPositionWhenFastestIpcSubscriberLeaves()
    {
        final int streamId = 43;
        final ExclusivePublication publication = aeron.addExclusivePublication(IPC_CHANNEL, streamId);
        final Subscription subscription = aeron.addSubscription(IPC_CHANNEL, streamId);
        while (publication.availableWindow() <= 0 || !subscription.isConnected())
        {
            driver.sharedAgentInvoker().invoke();
            aeron.conductorAgentInvoker().invoke();
            Tests.yield();
        }

        final UnsafeBuffer data = new UnsafeBuffer(new byte[100]);
        ThreadLocalRandom.current().nextBytes(data.byteArray());
        while (publication.offer(data) < 0)
        {
            driver.sharedAgentInvoker().invoke();
            aeron.conductorAgentInvoker().invoke();
            Tests.yield();
        }

        final long pubPos = publication.position();
        final CountersReader countersReader = aeron.countersReader();
        final Image image = subscription.imageBySessionId(publication.sessionId());
        final FragmentHandler fragmentHandler = (buffer, offset, length, header) ->
        {
            assertEquals(data.capacity(), length);
            for (int i = 0; i < length; i++)
            {
                assertEquals(data.getByte(i), buffer.getByte(offset + i));
            }
        };
        while (image.position() < pubPos)
        {
            if (0 == image.poll(fragmentHandler, 1))
            {
                Tests.yield();
            }
        }

        final int subPosCounterId = image.subscriberPositionId();

        // close Subscription and invoke MediaDriver so that it removes it before updating the positions
        subscription.close();

        do
        {
            driver.sharedAgentInvoker().invoke();
            aeron.conductorAgentInvoker().invoke();
        }
        while (countersReader.getCounterState(subPosCounterId) == CountersReader.RECORD_ALLOCATED);

        final Subscription subscription2 = aeron.addSubscription(IPC_CHANNEL, streamId);
        while (!subscription2.isConnected())
        {
            driver.sharedAgentInvoker().invoke();
            aeron.conductorAgentInvoker().invoke();
        }
        final Image image2 = subscription2.imageBySessionId(publication.sessionId());
        assertEquals(pubPos, image2.joinPosition());
    }
}
