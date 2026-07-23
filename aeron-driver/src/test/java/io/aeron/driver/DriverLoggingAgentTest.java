/*
 * Copyright 2014-2026 Real Logic Limited.
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
import io.aeron.AvailableImageHandler;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.UnavailableImageHandler;
import io.aeron.driver.logging.DriverEventCode;
import io.aeron.logging.EventConfiguration;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.Tests;
import io.aeron.test.agent.CountingEventReaderAgent;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.driver.logging.DriverEventCode.CMD_IN_ADD_EXCLUSIVE_PUBLICATION;
import static io.aeron.driver.logging.DriverEventCode.CMD_IN_ADD_PUBLICATION;
import static io.aeron.driver.logging.DriverEventCode.CMD_IN_ADD_SUBSCRIPTION;
import static io.aeron.driver.logging.DriverEventCode.CMD_IN_CLIENT_CLOSE;
import static io.aeron.driver.logging.DriverEventCode.CMD_IN_REMOVE_PUBLICATION;
import static io.aeron.driver.logging.DriverEventCode.CMD_IN_REMOVE_SUBSCRIPTION;
import static io.aeron.driver.logging.DriverEventCode.CMD_OUT_AVAILABLE_IMAGE;
import static io.aeron.driver.logging.DriverEventCode.CMD_OUT_COUNTER_READY;
import static io.aeron.driver.logging.DriverEventCode.CMD_OUT_EXCLUSIVE_PUBLICATION_READY;
import static io.aeron.driver.logging.DriverEventCode.CMD_OUT_ON_OPERATION_SUCCESS;
import static io.aeron.driver.logging.DriverEventCode.CMD_OUT_ON_UNAVAILABLE_COUNTER;
import static io.aeron.driver.logging.DriverEventCode.CMD_OUT_PUBLICATION_READY;
import static io.aeron.driver.logging.DriverEventCode.CMD_OUT_SUBSCRIPTION_READY;
import static io.aeron.driver.logging.DriverEventCode.FLOW_CONTROL_RECEIVER_ADDED;
import static io.aeron.driver.logging.DriverEventCode.FLOW_CONTROL_RECEIVER_REMOVED;
import static io.aeron.driver.logging.DriverEventCode.FRAME_IN;
import static io.aeron.driver.logging.DriverEventCode.FRAME_OUT;
import static io.aeron.driver.logging.DriverEventCode.PUBLICATION_IMAGE_REVOKE;
import static io.aeron.driver.logging.DriverEventCode.PUBLICATION_REVOKE;
import static io.aeron.driver.logging.DriverEventCode.RECEIVE_CHANNEL_CLOSE;
import static io.aeron.driver.logging.DriverEventCode.RECEIVE_CHANNEL_CREATION;
import static io.aeron.driver.logging.DriverEventCode.REMOVE_IMAGE_CLEANUP;
import static io.aeron.driver.logging.DriverEventCode.REMOVE_PUBLICATION_CLEANUP;
import static io.aeron.driver.logging.DriverEventCode.SEND_CHANNEL_CLOSE;
import static io.aeron.driver.logging.DriverEventCode.SEND_CHANNEL_CREATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.EnumSource.Mode.INCLUDE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@ExtendWith(InterruptingTestCallback.class)
public class DriverLoggingAgentTest
{
    private static final String NETWORK_CHANNEL =
        "aeron:udp?control-mode=dynamic|control=localhost:20550|fc=min,t:1ns";
    private static final int STREAM_ID = 1777;

    private final AvailableImageHandler availableImageHandler = mock(AvailableImageHandler.class);
    private final UnavailableImageHandler unavailableImageHandler = mock(UnavailableImageHandler.class);

    @BeforeEach
    void setUp()
    {
        assumeTrue("all".equals(System.getProperty(CommonContext.EVENT_LOG)));
        final String readerClass = System.getProperty(CommonContext.EVENT_LOG_READER_CLASSNAME_PROP_NAME);
        assumeTrue("io.aeron.test.agent.CountingEventReaderAgent".equals(readerClass));
    }

    @Test
    @InterruptAfter(10)
    void logAllNetworkChannel()
    {
        testLogMediaDriverEvents(NETWORK_CHANNEL, EnumSet.of(
            FRAME_IN,
            FRAME_OUT,
            CMD_IN_ADD_PUBLICATION,
            CMD_IN_REMOVE_PUBLICATION,
            CMD_IN_ADD_SUBSCRIPTION,
            CMD_IN_REMOVE_SUBSCRIPTION,
            CMD_OUT_PUBLICATION_READY,
            CMD_OUT_AVAILABLE_IMAGE,
            CMD_OUT_ON_OPERATION_SUCCESS,
            REMOVE_PUBLICATION_CLEANUP,
            REMOVE_IMAGE_CLEANUP,
            SEND_CHANNEL_CREATION,
            RECEIVE_CHANNEL_CREATION,
            SEND_CHANNEL_CLOSE,
            RECEIVE_CHANNEL_CLOSE,
            CMD_OUT_SUBSCRIPTION_READY,
            CMD_OUT_ON_UNAVAILABLE_COUNTER,
            CMD_OUT_COUNTER_READY,
            CMD_IN_CLIENT_CLOSE,
            FLOW_CONTROL_RECEIVER_ADDED,
            FLOW_CONTROL_RECEIVER_REMOVED));
    }

    @Test
    @InterruptAfter(10)
    void logAllExclusivePublicationNetworkChannel()
    {
        testLogExclusivePublicationMediaDriverEvents(NETWORK_CHANNEL, EnumSet.of(
            FRAME_IN,
            FRAME_OUT,
            CMD_IN_ADD_EXCLUSIVE_PUBLICATION,
            CMD_IN_REMOVE_PUBLICATION,
            CMD_IN_ADD_SUBSCRIPTION,
            CMD_IN_REMOVE_SUBSCRIPTION,
            CMD_OUT_EXCLUSIVE_PUBLICATION_READY,
            CMD_OUT_AVAILABLE_IMAGE,
            CMD_OUT_ON_OPERATION_SUCCESS,
            REMOVE_PUBLICATION_CLEANUP,
            REMOVE_IMAGE_CLEANUP,
            SEND_CHANNEL_CREATION,
            RECEIVE_CHANNEL_CREATION,
            SEND_CHANNEL_CLOSE,
            RECEIVE_CHANNEL_CLOSE,
            CMD_OUT_SUBSCRIPTION_READY,
            CMD_OUT_ON_UNAVAILABLE_COUNTER,
            CMD_OUT_COUNTER_READY,
            CMD_IN_CLIENT_CLOSE,
            FLOW_CONTROL_RECEIVER_ADDED,
            FLOW_CONTROL_RECEIVER_REMOVED,
            PUBLICATION_REVOKE,
            PUBLICATION_IMAGE_REVOKE));
    }

    @Test
    @InterruptAfter(10)
    void logAllIpcChannel()
    {
        testLogMediaDriverEvents(IPC_CHANNEL, EnumSet.of(
            CMD_IN_ADD_PUBLICATION,
            CMD_IN_REMOVE_PUBLICATION,
            CMD_IN_ADD_SUBSCRIPTION,
            CMD_IN_REMOVE_SUBSCRIPTION,
            CMD_OUT_PUBLICATION_READY,
            CMD_OUT_AVAILABLE_IMAGE,
            CMD_OUT_ON_OPERATION_SUCCESS,
            REMOVE_PUBLICATION_CLEANUP,
            CMD_OUT_SUBSCRIPTION_READY,
            CMD_OUT_COUNTER_READY,
            CMD_OUT_ON_UNAVAILABLE_COUNTER,
            CMD_IN_CLIENT_CLOSE));
    }

    @Test
    @InterruptAfter(10)
    void logAllExclusivePublicationIpcChannel()
    {
        testLogExclusivePublicationMediaDriverEvents(IPC_CHANNEL, EnumSet.of(
            CMD_IN_ADD_EXCLUSIVE_PUBLICATION,
            CMD_IN_REMOVE_PUBLICATION,
            CMD_IN_ADD_SUBSCRIPTION,
            CMD_IN_REMOVE_SUBSCRIPTION,
            CMD_OUT_EXCLUSIVE_PUBLICATION_READY,
            CMD_OUT_AVAILABLE_IMAGE,
            CMD_OUT_ON_OPERATION_SUCCESS,
            REMOVE_PUBLICATION_CLEANUP,
            CMD_OUT_SUBSCRIPTION_READY,
            CMD_OUT_COUNTER_READY,
            CMD_OUT_ON_UNAVAILABLE_COUNTER,
            CMD_IN_CLIENT_CLOSE,
            PUBLICATION_REVOKE));
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, mode = INCLUDE, names = {
        "PUBLICATION_REVOKE",
        "PUBLICATION_IMAGE_REVOKE"
    })
    @InterruptAfter(10)
    void logIndividualExclusivePublicationEvents(final DriverEventCode eventCode)
    {
        testLogExclusivePublicationMediaDriverEvents(NETWORK_CHANNEL, EnumSet.of(eventCode));
    }

    private void testLogMediaDriverEvents(final String channel, final EnumSet<DriverEventCode> expectedEvents)
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .errorHandler(Tests::onError)
            .publicationLingerTimeoutNs(1_000_000_000L)
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(1));

        try (MediaDriver mediaDriver = MediaDriver.launch(driverCtx))
        {
            try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
                Subscription subscription =
                    aeron.addSubscription(channel, STREAM_ID, availableImageHandler, unavailableImageHandler);
                Publication publication = aeron.addPublication(channel, STREAM_ID))
            {
                final UnsafeBuffer offerBuffer = new UnsafeBuffer(new byte[32]);
                while (publication.offer(offerBuffer) < 0)
                {
                    Tests.yield();
                }

                final MutableInteger counter = new MutableInteger();
                final FragmentHandler handler = (buffer, offset, length, header) -> counter.value++;

                while (0 == subscription.poll(handler, 1))
                {
                    Tests.yield();
                }

                assertEquals(1, counter.get());
            }

            verifyExpectedEvents(expectedEvents);
        }
    }

    private static void verifyExpectedEvents(final EnumSet<DriverEventCode> expectedEvents)
    {
        final Agent agent = EventConfiguration.eventReader().agent();
        Assertions.assertInstanceOf(CountingEventReaderAgent.class, agent);
        final CountingEventReaderAgent countingAgent = (CountingEventReaderAgent)agent;

        final List<DriverEventCode> pendingList = new ArrayList<>(expectedEvents);

        final Supplier<String> errorMessage = () -> "Pending events: " + pendingList;
        while (!pendingList.isEmpty())
        {
            pendingList.removeIf(
                driverEventCode -> 0 < countingAgent.countDriverEvent(driverEventCode.toEventCodeId()));
            Tests.sleep(1, errorMessage);
        }
    }

    private void testLogExclusivePublicationMediaDriverEvents(
        final String channel,
        final EnumSet<DriverEventCode> expectedEvents)
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .errorHandler(Tests::onError)
            .publicationLingerTimeoutNs(3_000_000_000L)
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(1));

        try (MediaDriver mediaDriver = MediaDriver.launch(driverCtx))
        {
            final AtomicInteger unavailableImages = new AtomicInteger(0);
            doAnswer(invocation ->
            {
                final Image image = invocation.getArgument(0, Image.class);
                assertTrue(image.isPublicationRevoked());

                unavailableImages.incrementAndGet();
                return null;
            }).when(unavailableImageHandler).onUnavailableImage(any(Image.class));

            try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
                Subscription subscription =
                    aeron.addSubscription(channel, STREAM_ID, availableImageHandler, unavailableImageHandler);
                ExclusivePublication publication = aeron.addExclusivePublication(channel, STREAM_ID))
            {
                final UnsafeBuffer offerBuffer = new UnsafeBuffer(new byte[32]);
                while (publication.offer(offerBuffer) < 0)
                {
                    Tests.yield();
                }

                final MutableInteger counter = new MutableInteger();
                final FragmentHandler handler = (buffer, offset, length, header) -> counter.value++;

                while (0 == subscription.poll(handler, 1))
                {
                    Tests.yield();
                }

                assertEquals(1, counter.get());

                publication.revoke();

                while (unavailableImages.get() == 0)
                {
                    Tests.yield();
                }
            }

            verifyExpectedEvents(expectedEvents);
        }
    }
}
