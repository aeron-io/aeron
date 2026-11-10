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

import io.aeron.driver.FlowControlSupplier;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.TaggedMulticastFlowControlSupplier;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.status.SenderLimit;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.SystemUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.aeron.FlowControlTests.awaitConnectionAndStatusMessages;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(InterruptingTestCallback.class)
class TaggedFlowControlSystemTest
{
    private static final String MULTICAST_URI = "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost";
    private static final int STREAM_ID = 1001;

    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int NUM_MESSAGES_PER_TERM = 64;
    private static final int MESSAGE_LENGTH =
        (TERM_BUFFER_LENGTH / NUM_MESSAGES_PER_TERM) - DataHeaderFlyweight.HEADER_LENGTH;
    private static final String ROOT_DIR = SystemUtil.tmpDirName() + "aeron-system-tests" + File.separator;

    private final MediaDriver.Context driverAContext = new MediaDriver.Context();
    private final MediaDriver.Context driverBContext = new MediaDriver.Context();

    private Aeron clientA;
    private Aeron clientB;
    private TestMediaDriver driverA;
    private TestMediaDriver driverB;
    private Publication publication;
    private Subscription subscriptionA;
    private Subscription subscriptionB;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
    private final FragmentHandler fragmentHandlerA = mock(FragmentHandler.class);
    private final FragmentHandler fragmentHandlerB = mock(FragmentHandler.class);

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private void launch()
    {
        buffer.putInt(0, 1);

        final String baseDirA = ROOT_DIR + "A";
        final String baseDirB = ROOT_DIR + "B";

        driverAContext.publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirA)
            .flowControlReceiverTimeoutNs(TimeUnit.MILLISECONDS.toNanos(1000))
            .errorHandler(Tests::onError)
            .threadingMode(ThreadingMode.SHARED);

        driverBContext.publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirB)
            .flowControlReceiverTimeoutNs(TimeUnit.MILLISECONDS.toNanos(1000))
            .errorHandler(Tests::onError)
            .threadingMode(ThreadingMode.SHARED);

        driverA = TestMediaDriver.launch(driverAContext, testWatcher);
        testWatcher.dataCollector().add(driverA.context().aeronDirectory());

        driverB = TestMediaDriver.launch(driverBContext, testWatcher);
        testWatcher.dataCollector().add(driverB.context().aeronDirectory());

        clientA = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(driverAContext.aeronDirectoryName()));

        clientB = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(driverBContext.aeronDirectoryName()));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(subscriptionB, subscriptionA, publication, clientB, clientA, driverB, driverA);
    }

    private static Stream<Arguments> strategyConfigurations()
    {
        return Stream.of(
            Arguments.of(new TaggedMulticastFlowControlSupplier(), null, null, "", "|gtag=-1"),
            Arguments.of(new TaggedMulticastFlowControlSupplier(), null, 2004L, "", "|gtag=2004"),
            Arguments.of(null, 2020L, 2020L, "|fc=tagged", ""),
            Arguments.of(null, null, null, "|fc=tagged,g:123", "|gtag=123"));
    }

    private static final class State
    {
        private int numMessagesToSend;
        private int numMessagesLeftToSend;
        private int numFragmentsReadFromA;
        private int numFragmentsReadFromB;
        private boolean isBClosed = false;

        @Override
        public String toString()
        {
            return "State{" +
                "numMessagesToSend=" + numMessagesToSend +
                ", numMessagesLeftToSend=" + numMessagesLeftToSend +
                ", numFragmentsReadFromA=" + numFragmentsReadFromA +
                ", numFragmentsReadFromB=" + numFragmentsReadFromB +
                ", isBClosed=" + isBClosed +
                '}';
        }
    }

    @ParameterizedTest
    @MethodSource("strategyConfigurations")
    @InterruptAfter(10)
    void shouldSlowToTaggedWithMulticastFlowControlStrategy(
        final FlowControlSupplier flowControlSupplier,
        final Long groupTag,
        final Long flowControlGroupTag,
        final String publisherUriParams,
        final String subscriptionBUriParams)
    {
        final State state = new State();
        state.numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;
        state.numMessagesLeftToSend = state.numMessagesToSend;
        state.numFragmentsReadFromB = 0;

        driverBContext.imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));
        if (null != flowControlSupplier)
        {
            driverAContext.multicastFlowControlSupplier(flowControlSupplier);
        }
        if (null != flowControlGroupTag)
        {
            driverAContext.flowControlGroupTag(flowControlGroupTag);
        }
        if (null != groupTag)
        {
            driverBContext.receiverGroupTag(groupTag);
        }
        driverAContext.flowControlGroupMinSize(1);

        launch();

        subscriptionA = clientA.addSubscription(MULTICAST_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(MULTICAST_URI + subscriptionBUriParams, STREAM_ID);
        publication = clientA.addPublication(MULTICAST_URI + publisherUriParams, STREAM_ID);

        Tests.awaitConnected(subscriptionA);
        Tests.awaitConnected(subscriptionB);
        Tests.awaitConnected(publication);

        for (long i = 0; state.numFragmentsReadFromB < state.numMessagesToSend; i++)
        {
            if (state.numMessagesLeftToSend > 0)
            {
                final long result = publication.offer(buffer, 0, buffer.capacity());
                if (result >= 0L)
                {
                    state.numMessagesLeftToSend--;
                }
                else if (Publication.NOT_CONNECTED == result)
                {
                    fail("Publication not connected, numMessagesLeftToSend=" + state.numMessagesLeftToSend);
                }
                else
                {
                    Tests.yieldingIdle(state::toString);
                }
            }

            Tests.yieldingIdle(state::toString);

            // A keeps up
            pollWithTimeout(subscriptionA, fragmentHandlerA, 10, state::toString);

            // B receives slowly
            if ((i % 2) == 0)
            {
                final int bFragments = pollWithTimeout(subscriptionB, fragmentHandlerB, 1, state::toString);
                if (0 == bFragments && !subscriptionB.isConnected())
                {
                    if (subscriptionB.isClosed())
                    {
                        fail("Subscription B is closed, numFragmentsFromB=" + state.numFragmentsReadFromB);
                    }

                    fail("Subscription B not connected, numFragmentsFromB=" + state.numFragmentsReadFromB);
                }

                state.numFragmentsReadFromB += bFragments;
            }
        }

        verify(fragmentHandlerB, times(state.numMessagesToSend)).onFragment(
            any(DirectBuffer.class), anyInt(), eq(MESSAGE_LENGTH), any(Header.class));
    }

    @Test
    @InterruptAfter(10)
    void shouldRemoveDeadReceiver()
    {
        final State state = new State();
        state.numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;
        state.numMessagesLeftToSend = state.numMessagesToSend;
        state.numFragmentsReadFromA = 0;
        state.numFragmentsReadFromB = 0;
        state.isBClosed = false;

        driverBContext.imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));

        launch();

        publication = clientA.addPublication(MULTICAST_URI + "|fc=tagged,g:123,t:1s", STREAM_ID);

        subscriptionA = clientA.addSubscription(MULTICAST_URI + "|gtag=123", STREAM_ID);
        Tests.awaitConnected(subscriptionA);
        subscriptionB = clientB.addSubscription(MULTICAST_URI + "|gtag=123", STREAM_ID);
        Tests.awaitConnected(subscriptionB);

        Tests.awaitConnected(publication);

        while (state.numFragmentsReadFromA < state.numMessagesToSend)
        {
            if (state.numMessagesLeftToSend > 0)
            {
                final long position = publication.offer(buffer, 0, buffer.capacity());
                if (position >= 0L)
                {
                    state.numMessagesLeftToSend--;
                }
                else
                {
                    Tests.yieldingIdle("position: %d, state: %s", position, state);
                }
            }

            // A keeps up
            state.numFragmentsReadFromA += pollWithTimeout(subscriptionA, fragmentHandlerA, 10, state::toString);

            // B receives up to 1/8 of the messages, then stops
            if (state.numFragmentsReadFromB < (state.numMessagesToSend / 8))
            {
                state.numFragmentsReadFromB += pollWithTimeout(
                    subscriptionB, fragmentHandlerB, 10, state::toString);
            }
            else if (!state.isBClosed)
            {
                subscriptionB.close();
                state.isBClosed = true;
            }
        }

        verify(fragmentHandlerA, times(state.numMessagesToSend)).onFragment(
            any(DirectBuffer.class), anyInt(), eq(MESSAGE_LENGTH), any(Header.class));
    }

    @SuppressWarnings("methodlength")
    @SlowTest
    @Test
    @InterruptAfter(20)
    void shouldPreventConnectionUntilRequiredGroupSizeMatchTagIsMet()
    {
        final Long groupTag = 2701L;
        final Integer groupSize = 3;

        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("224.20.30.39:24326")
            .networkInterface("localhost");

        final String uriWithGroupTag = builder
            .groupTag(groupTag)
            .flowControl((String)null)
            .build();

        final String uriPlain = builder
            .groupTag((Long)null)
            .flowControl((String)null)
            .build();

        final String uriWithTaggedFlowControl = builder
            .groupTag((Long)null)
            .taggedFlowControl(groupTag, groupSize, null)
            .build();

        driverBContext.imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));

        launch();

        TestMediaDriver driverC = null;
        Aeron clientC = null;

        TestMediaDriver driverD = null;
        Aeron clientD = null;

        Publication publication = null;
        Subscription subscription0 = null;
        Subscription subscription1 = null;
        Subscription subscription2 = null;
        Subscription subscription3 = null;
        Subscription subscription4 = null;
        Subscription subscription5 = null;

        try
        {
            driverC = TestMediaDriver.launch(
                new MediaDriver.Context().publicationTermBufferLength(TERM_BUFFER_LENGTH)
                    .aeronDirectoryName(ROOT_DIR + "C")
                    .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                    .errorHandler(Tests::onError)
                    .threadingMode(ThreadingMode.SHARED),
                testWatcher);

            clientC = Aeron.connect(
                new Aeron.Context()
                    .aeronDirectoryName(driverC.aeronDirectoryName()));

            driverD = TestMediaDriver.launch(
                new MediaDriver.Context().publicationTermBufferLength(TERM_BUFFER_LENGTH)
                    .aeronDirectoryName(ROOT_DIR + "D")
                    .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                    .errorHandler(Tests::onError)
                    .threadingMode(ThreadingMode.SHARED),
                testWatcher);

            clientD = Aeron.connect(
                new Aeron.Context()
                    .aeronDirectoryName(driverD.aeronDirectoryName()));

            publication = clientA.addPublication(uriWithTaggedFlowControl, STREAM_ID);

            subscription0 = clientA.addSubscription(uriPlain, STREAM_ID);
            subscription1 = clientA.addSubscription(uriPlain, STREAM_ID);
            subscription2 = clientA.addSubscription(uriPlain, STREAM_ID);
            subscription3 = clientB.addSubscription(uriWithGroupTag, STREAM_ID);
            subscription4 = clientC.addSubscription(uriWithGroupTag, STREAM_ID);

            awaitConnectionAndStatusMessages(
                clientA.countersReader(),
                subscription0, subscription1, subscription2, subscription3, subscription4);

            assertFalse(publication.isConnected());

            subscription5 = clientD.addSubscription(uriWithGroupTag, STREAM_ID);

            // Should now have 3 receivers and publication should eventually be connected.
            while (!publication.isConnected())
            {
                Tests.sleep(1);
            }

            subscription5.close();
            subscription5 = null;

            // Lost a receiver and publication should eventually be disconnected.
            while (publication.isConnected())
            {
                Tests.sleep(1);
            }

            subscription5 = clientD.addSubscription(uriWithGroupTag, STREAM_ID);

            // Aaaaaand reconnect.
            while (!publication.isConnected())
            {
                Tests.sleep(1);
            }
        }
        finally
        {
            CloseHelper.closeAll(
                publication,
                subscription0, subscription1, subscription2, subscription3, subscription4, subscription5,
                clientC, clientD,
                driverC, driverD);
        }
    }

    @Test
    @InterruptAfter(20)
    void shouldPreventConnectionUntilAtLeastOneSubscriberConnectedWithRequiredGroupSizeZero()
    {
        final Long groupTag = 2701L;
        final Integer groupSize = 0;

        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("224.20.30.41:24326")
            .networkInterface("localhost");

        final String plainUri = builder.build();

        final String uriWithGroupTag = builder
            .groupTag(groupTag)
            .flowControl((String)null)
            .build();

        final String uriWithTaggedFlowControl = builder
            .groupTag((Long)null)
            .taggedFlowControl(groupTag, groupSize, null)
            .build();

        launch();

        publication = clientA.addPublication(uriWithTaggedFlowControl, STREAM_ID);
        final Publication otherPublication = clientA.addPublication(plainUri, STREAM_ID + 1);

        clientA.addSubscription(plainUri, STREAM_ID + 1);

        while (!otherPublication.isConnected())
        {
            Tests.sleep(1);
        }
        // We know another publication on the same channel is connected

        assertFalse(publication.isConnected());

        subscriptionA = clientA.addSubscription(uriWithGroupTag, STREAM_ID);

        Tests.awaitConnected(publication);
        Tests.awaitConnected(subscriptionA);
    }

    @Test
    @InterruptAfter(10)
    void shouldHandleSenderLimitCorrectlyWithMinGroupSize()
    {
        final String publisherUri = "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123/1";
        final String groupSubscriberUri = "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|gtag=123";
        final String subscriberUri = "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost";

        driverBContext.imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));

        launch();

        publication = clientA.addPublication(publisherUri, STREAM_ID);

        final CountersReader countersReader = clientA.countersReader();

        final int senderLimitCounterId = FlowControlTests.findCounterIdByRegistrationId(
            countersReader, SenderLimit.SENDER_LIMIT_TYPE_ID, publication.registrationId);
        final long currentSenderLimit = countersReader.getCounterValue(senderLimitCounterId);

        subscriptionA = clientA.addSubscription(subscriberUri, STREAM_ID);

        awaitConnectionAndStatusMessages(countersReader, subscriptionA);
        assertEquals(currentSenderLimit, countersReader.getCounterValue(senderLimitCounterId));

        subscriptionB = clientB.addSubscription(groupSubscriberUri, STREAM_ID);

        while (currentSenderLimit == countersReader.getCounterValue(senderLimitCounterId))
        {
            Tests.sleep(1);
        }
    }

    private int pollWithTimeout(
        final Subscription subscription,
        final FragmentHandler fragmentHandler,
        final int fragmentLimit,
        final Supplier<String> message)
    {
        final int numFragments = subscription.poll(fragmentHandler, fragmentLimit);
        if (0 == numFragments)
        {
            Tests.yieldingIdle(message);
        }

        return numFragments;
    }
}
