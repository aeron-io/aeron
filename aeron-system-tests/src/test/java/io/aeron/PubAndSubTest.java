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
import io.aeron.driver.ext.DebugChannelEndpointConfiguration;
import io.aeron.driver.ext.DebugSendChannelEndpoint;
import io.aeron.driver.ext.LossGenerator;
import io.aeron.exceptions.RegistrationException;
import io.aeron.logbuffer.*;
import io.aeron.test.*;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableBoolean;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.aeron.SystemTests.verifyLossOccurredForStream;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.logbuffer.LogBufferDescriptor.computeFragmentedFrameLength;
import static io.aeron.protocol.DataHeaderFlyweight.*;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.Mockito.*;

@ExtendWith(InterruptingTestCallback.class)
class PubAndSubTest
{
    private static final String IPC_URI = "aeron:ipc";

    private static List<String> channels()
    {
        return asList(
            "aeron:udp?endpoint=localhost:24325",
            "aeron:udp?endpoint=localhost:24325|session=id=55555",
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost",
            IPC_URI);
    }

    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private static final int STREAM_ID = 1001;

    private final MediaDriver.Context driverContext = new MediaDriver.Context()
        .publicationConnectionTimeoutNs(MILLISECONDS.toNanos(300))
        .imageLivenessTimeoutNs(MILLISECONDS.toNanos(500))
        .timerIntervalNs(MILLISECONDS.toNanos(100));

    private final Aeron.Context clientContext = new Aeron.Context()
        .resourceLingerDurationNs(MILLISECONDS.toNanos(200));

    private Aeron publishingClient;
    private Aeron subscribingClient;
    private TestMediaDriver driver;
    private Subscription subscription;
    private Publication publication;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[8192]);
    private final FragmentHandler fragmentHandler = mock(FragmentHandler.class);
    private final RawBlockHandler rawBlockHandler = mock(RawBlockHandler.class);
    private final AvailableImageHandler availableImageHandler = mock(AvailableImageHandler.class);
    private final UnavailableImageHandler unavailableImageHandler = mock(UnavailableImageHandler.class);

    private void launch(final String channel)
    {
        driverContext.dirDeleteOnStart(true).threadingMode(ThreadingMode.SHARED);

        driver = TestMediaDriver.launch(driverContext, watcher);
        watcher.dataCollector().add(driver.context().aeronDirectory());

        subscribingClient = Aeron.connect(clientContext.clone());
        publishingClient = Aeron.connect(clientContext.clone());
        subscription = subscribingClient.addSubscription(
            channel, STREAM_ID, availableImageHandler, unavailableImageHandler);
        publication = publishingClient.addPublication(channel, STREAM_ID);
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(publishingClient, subscribingClient, driver);
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldReceivePublishedMessageViaPollFile(final String channel)
    {
        launch(channel);

        publishMessage();

        while (true)
        {
            final long bytes = subscription.rawPoll(rawBlockHandler, Integer.MAX_VALUE);
            if (bytes > 0)
            {
                break;
            }

            Tests.yield();
        }

        final long expectedOffset = 0L;
        final int expectedLength = BitUtil.align(HEADER_LENGTH + SIZE_OF_INT, FRAME_ALIGNMENT);

        final ArgumentCaptor<FileChannel> channelArgumentCaptor = ArgumentCaptor.forClass(FileChannel.class);
        verify(rawBlockHandler).onBlock(
            channelArgumentCaptor.capture(),
            eq(expectedOffset),
            any(UnsafeBuffer.class),
            eq((int)expectedOffset),
            eq(expectedLength),
            anyInt(),
            anyInt());

        assertTrue(channelArgumentCaptor.getValue().isOpen(), "File Channel is closed");
    }

    @ParameterizedTest
    @ValueSource(ints = { 1408, 128 })
    void shouldSendAndReceiveMessage(final int mtu)
    {
        launch("aeron:udp?endpoint=localhost:24325|mtu=" + mtu);
        final MutableInteger frameLength = new MutableInteger(-1);
        final MutableLong position = new MutableLong(-1);

        final int payloadLength = 500;
        final FragmentHandler handler = (buffer, offset, length, header) ->
        {
            frameLength.set(header.frameLength());
            position.set(header.position());
        };

        final FragmentHandler assembler =
            HEADER_LENGTH + payloadLength < mtu ? handler : new FragmentAssembler(handler);

        while (publication.offer(buffer, 0, payloadLength) < 0)
        {
            Tests.yield();
        }

        while (-1 == frameLength.get() || -1 == position.get())
        {
            subscription.poll(assembler, 10);
            Tests.yield();
        }

        assertEquals(HEADER_LENGTH + payloadLength, frameLength.get());
        assertEquals(subscription.imageAtIndex(0).position(), position.get());
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldContinueAfterBufferRollover(final String channel)
    {
        final int termBufferLength = 64 * 1024;
        final int numMessagesInTermBuffer = 64;
        final int messageLength = (termBufferLength / numMessagesInTermBuffer) - HEADER_LENGTH;
        final int numMessagesToSend = numMessagesInTermBuffer + 1;

        driverContext.publicationTermBufferLength(termBufferLength);

        launch(channel);

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                Tests.yield();
            }

            pollForFragment();
        }

        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldContinueAfterRolloverWithMinimalPaddingHeader(final String channel)
    {
        final int termBufferLength = 64 * 1024;
        final int termBufferLengthMinusPaddingHeader = termBufferLength - HEADER_LENGTH;
        final int num1kMessagesInTermBuffer = 63;
        final int lastMessageLength =
            termBufferLengthMinusPaddingHeader - (num1kMessagesInTermBuffer * 1024) - HEADER_LENGTH;
        final int messageLength = 1024 - HEADER_LENGTH;

        driverContext.publicationTermBufferLength(termBufferLength);

        launch(channel);

        // lock step reception until we get to within 8 messages of the end
        for (int i = 0; i < num1kMessagesInTermBuffer - 7; i++)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                Tests.yield();
            }

            pollForFragment();
        }

        for (int i = 7; i > 0; i--)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                Tests.yield();
            }
        }

        // small enough to leave room for padding that is just a header
        while (publication.offer(buffer, 0, lastMessageLength) < 0L)
        {
            Tests.yield();
        }

        // no roll over
        while (publication.offer(buffer, 0, messageLength) < 0L)
        {
            Tests.yield();
        }

        final MutableInteger fragmentsRead = new MutableInteger();

        Tests.executeUntil(
            () -> fragmentsRead.value == 9,
            (j) ->
            {
                final int fragments = subscription.poll(fragmentHandler, 10);
                if (0 == fragments)
                {
                    Thread.yield();
                }
                fragmentsRead.value += fragments;
            },
            Integer.MAX_VALUE,
            MILLISECONDS.toNanos(500));

        final InOrder inOrder = inOrder(fragmentHandler);

        inOrder.verify(fragmentHandler, times(num1kMessagesInTermBuffer)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
        inOrder.verify(fragmentHandler, times(1)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(lastMessageLength),
            any(Header.class));
        inOrder.verify(fragmentHandler, times(1)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(20)
    @SlowTest
    void shouldReceivePublishedMessageOneForOneWithDataLoss(final String channel) throws IOException
    {
        assumeFalse(IPC_URI.equals(channel));

        final int termBufferLength = 64 * 1024;
        final int numMessagesInTermBuffer = 64;
        final int messageLength = (termBufferLength / numMessagesInTermBuffer) - HEADER_LENGTH;
        final int numMessagesToSend = 2 * numMessagesInTermBuffer;

        final LossGenerator noLossGenerator =
            DebugChannelEndpointConfiguration.lossGeneratorSupplier(0, 0);

        driverContext.publicationTermBufferLength(termBufferLength);

        driverContext.sendChannelEndpointSupplier(
            (udpChannel, statusIndicator, context) ->
                new DebugSendChannelEndpoint(udpChannel, statusIndicator, context, noLossGenerator, noLossGenerator));

        TestMediaDriver.enableRandomLoss(driverContext, 0.1, 0xcafebabeL, true, false);

        launch(channel);

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                Tests.yield();
            }

            pollForFragment();
        }

        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));

        verifyLossOccurredForStream(driverContext.aeronDirectoryName(), STREAM_ID);
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldReceivePublishedMessageBatchedWithDataLoss(final String channel) throws IOException
    {
        assumeFalse(IPC_URI.equals(channel));

        final int termBufferLength = 64 * 1024;
        final int numMessagesInTermBuffer = 64;
        final int messageLength = (termBufferLength / numMessagesInTermBuffer) - HEADER_LENGTH;
        final int numMessagesToSend = 2 * numMessagesInTermBuffer;
        final int numBatches = 4;
        final int numMessagesPerBatch = numMessagesToSend / numBatches;

        final LossGenerator noLossGenerator =
            DebugChannelEndpointConfiguration.lossGeneratorSupplier(0, 0);

        driverContext.publicationTermBufferLength(termBufferLength);

        driverContext.sendChannelEndpointSupplier(
            (udpChannel, statusIndicator, context) ->
                new DebugSendChannelEndpoint(udpChannel, statusIndicator, context, noLossGenerator, noLossGenerator));

        TestMediaDriver.enableRandomLoss(driverContext, 0.1, 0xcafebabeL, true, false);

        launch(channel);

        for (int i = 0; i < numBatches; i++)
        {
            for (int j = 0; j < numMessagesPerBatch; j++)
            {
                while (publication.offer(buffer, 0, messageLength) < 0L)
                {
                    Tests.yield();
                }
            }

            pollForBatch(numMessagesPerBatch);
        }

        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));

        verifyLossOccurredForStream(driverContext.aeronDirectoryName(), STREAM_ID);
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldContinueAfterBufferRolloverBatched(final String channel)
    {
        final int termBufferLength = 64 * 1024;
        final int numBatchesPerTerm = 4;
        final int numMessagesPerBatch = 16;
        final int numMessagesInTermBuffer = numMessagesPerBatch * numBatchesPerTerm;
        final int messageLength = (termBufferLength / numMessagesInTermBuffer) - HEADER_LENGTH;
        final int numMessagesToSend = numMessagesInTermBuffer + 1;

        driverContext.publicationTermBufferLength(termBufferLength);

        launch(channel);

        for (int i = 0; i < numBatchesPerTerm; i++)
        {
            for (int j = 0; j < numMessagesPerBatch; j++)
            {
                while (publication.offer(buffer, 0, messageLength) < 0L)
                {
                    Tests.yield();
                }
            }

            pollForBatch(numMessagesPerBatch);
        }

        while (publication.offer(buffer, 0, messageLength) < 0L)
        {
            Tests.yield();
        }

        final MutableInteger fragmentsRead = new MutableInteger();

        Tests.executeUntil(
            () -> fragmentsRead.value > 0,
            (j) ->
            {
                final int fragments = subscription.poll(fragmentHandler, 10);
                if (0 == fragments)
                {
                    Thread.yield();
                }
                fragmentsRead.value += fragments;
            },
            Integer.MAX_VALUE,
            MILLISECONDS.toNanos(900));

        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldContinueAfterBufferRolloverWithPadding(final String channel)
    {
        /*
         * 65536 bytes in the buffer
         * 63 * 1032 = 65016
         * 65536 - 65016 = 520 bytes padding at the end
         * so, sending 64 messages causes last to overflow
         */
        final int termBufferLength = 64 * 1024;
        final int messageLength = 1032 - HEADER_LENGTH;
        final int numMessagesToSend = 64;

        driverContext.publicationTermBufferLength(termBufferLength);

        launch(channel);

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                Tests.yield();
            }

            pollForFragment();
        }

        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldContinueAfterBufferRolloverWithPaddingBatched(final String channel)
    {
        /*
         * 65536 bytes in the buffer
         * 63 * 1032 = 65016
         * 65536 - 65016 = 520 bytes padding at the end
         * so, sending 64 messages causes last to overflow
         */
        final int termBufferLength = 64 * 1024;
        final int messageLength = 1032 - HEADER_LENGTH;
        final int numMessagesToSend = 64;
        final int numBatchesPerTerm = 4;
        final int numMessagesPerBatch = numMessagesToSend / numBatchesPerTerm;

        driverContext.publicationTermBufferLength(termBufferLength);

        launch(channel);

        for (int i = 0; i < numBatchesPerTerm; i++)
        {
            for (int j = 0; j < numMessagesPerBatch; j++)
            {
                while (publication.offer(buffer, 0, messageLength) < 0L)
                {
                    Tests.yield();
                }
            }

            pollForBatch(numMessagesPerBatch);
        }

        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldReceiveOnlyAfterSendingUpToFlowControlLimit(final String channel)
    {
        /*
         * The subscriber will flow control before an entire term buffer. So, send until can't send anymore.
         * Then start up subscriber to drain.
         */
        final int termBufferLength = 64 * 1024;
        final int numMessagesPerTerm = 64;
        final int messageLength = (termBufferLength / numMessagesPerTerm) - HEADER_LENGTH;
        final int maxFails = 10000;
        int messagesSent = 0;

        driverContext.publicationTermBufferLength(termBufferLength);

        launch(channel);

        for (int i = 0; i < numMessagesPerTerm; i++)
        {
            int offerFails = 0;

            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                if (++offerFails > maxFails)
                {
                    break;
                }

                Tests.yield();
            }

            if (offerFails > maxFails)
            {
                break;
            }

            messagesSent++;
        }

        final MutableInteger fragmentsRead = new MutableInteger();
        final int messagesToReceive = messagesSent;

        Tests.executeUntil(
            () -> fragmentsRead.value >= messagesToReceive,
            (j) ->
            {
                final int fragments = subscription.poll(fragmentHandler, 10);
                if (0 == fragments)
                {
                    Thread.yield();
                }
                fragmentsRead.value += fragments;
            },
            Integer.MAX_VALUE,
            MILLISECONDS.toNanos(500));

        verify(fragmentHandler, times(messagesToReceive)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldReceivePublishedMessageOneForOneWithReSubscription(final String channel)
    {
        final int termBufferLength = 64 * 1024;
        final int numMessagesInTermBuffer = 64;
        final int messageLength = (termBufferLength / numMessagesInTermBuffer) - HEADER_LENGTH;
        final int numMessagesToSendStageOne = numMessagesInTermBuffer / 2;
        final int numMessagesToSendStageTwo = numMessagesInTermBuffer;

        driverContext.publicationTermBufferLength(termBufferLength);

        launch(channel);

        Tests.awaitConnected(subscription);

        for (int i = 0; i < numMessagesToSendStageOne; i++)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                Tests.yield();
            }

            pollForFragment();
        }

        assertEquals(publication.position(), subscription.imageAtIndex(0).position());

        subscription.close();
        subscription = Tests.reAddSubscription(subscribingClient, channel, STREAM_ID);

        Tests.awaitConnected(subscription);

        for (int i = 0; i < numMessagesToSendStageTwo; i++)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                Tests.yield();
            }

            pollForFragment();
        }

        assertEquals(publication.position(), subscription.imageAtIndex(0).position());
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldFragmentExactMessageLengthsCorrectly(final String channel)
    {
        final int termBufferLength = 64 * 1024;
        final int numFragmentsPerMessage = 2;
        final int mtuLength = driverContext.mtuLength();
        final int frameLength = mtuLength - HEADER_LENGTH;
        final int messageLength = frameLength * numFragmentsPerMessage;
        final int numMessagesToSend = 2;
        final int numFramesToExpect = numMessagesToSend * numFragmentsPerMessage;

        driverContext.publicationTermBufferLength(termBufferLength);

        launch(channel);

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                Tests.yield();
            }
        }

        final MutableInteger fragmentsRead = new MutableInteger();

        Tests.executeUntil(
            () -> fragmentsRead.value > numFramesToExpect,
            (j) ->
            {
                final int fragments = subscription.poll(fragmentHandler, 10);
                if (0 == fragments)
                {
                    Thread.yield();
                }
                fragmentsRead.value += fragments;
            },
            Integer.MAX_VALUE,
            MILLISECONDS.toNanos(500));

        verify(fragmentHandler, times(numFramesToExpect)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(frameLength),
            any(Header.class));
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldNoticeDroppedSubscriber(final String channel)
    {
        launch(channel);

        Tests.awaitConnected(publication);

        subscription.close();

        while (publication.isConnected())
        {
            Tests.yield();
        }
    }

    @Test
    void shouldAllowSubscriptionsIfUsingTagsAndParametersAndAllMatch()
    {
        launch("aeron:ipc");

        final String pubChannel = "aeron:udp?control-mode=dynamic|control=127.0.0.1:9999";
        final String channel = "aeron:udp?endpoint=127.0.0.1:0|control=127.0.0.1:9999|tags=1001";
        try (
            Publication pub = subscribingClient.addPublication(pubChannel, 1000);
            Subscription sub1 = subscribingClient.addSubscription(channel, 1000);
            Subscription sub2 = subscribingClient.addSubscription(channel, 1000))
        {
            Tests.awaitConnected(sub1);
            Tests.awaitConnected(sub2);
            Tests.awaitConnected(pub);
        }
    }

    @Test
    void shouldRejectSubscriptionsIfUsingTagsAndParametersAndEndpointDoesNotMatchEndpointWithExplicitControl()
    {
        watcher.ignoreErrorsMatching((s) -> s.contains("has mismatched endpoint or control"));
        launch("aeron:ipc");

        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder(
            "aeron:udp?endpoint=127.0.0.1:0|control=127.0.0.1:9999|tags=1001");

        try (Subscription ignore1 = subscribingClient.addSubscription(builder.build(), 1000))
        {
            Objects.requireNonNull(ignore1);
            assertThrows(
                RegistrationException.class,
                () -> subscribingClient.addSubscription(builder.endpoint("127.0.0.1:9999").build(), 1000));
            assertThrows(
                RegistrationException.class,
                () -> subscribingClient.addSubscription(builder.endpoint("127.0.0.2:0").build(), 1000));
        }
    }

    @Test
    void shouldRejectSubscriptionsIfUsingTagsAndParametersAndEndpointDoesNotMatchEndpointWithoutControl()
    {
        watcher.ignoreErrorsMatching((s) -> s.contains("has mismatched endpoint or control"));
        launch("aeron:ipc");

        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder(
            "aeron:udp?endpoint=127.0.0.1:9999|tags=1001");

        try (Subscription ignore1 = subscribingClient.addSubscription(builder.build(), 1000))
        {
            Objects.requireNonNull(ignore1);
            assertThrows(
                RegistrationException.class,
                () -> subscribingClient.addSubscription(builder.endpoint("127.0.0.1:0").build(), 1000));
            assertThrows(
                RegistrationException.class,
                () -> subscribingClient.addSubscription(builder.endpoint("127.0.0.2:9999").build(), 1000));
        }
    }

    @Test
    void shouldRejectSubscriptionsIfUsingTagsAndParametersAndEndpointDoesNotMatchControl()
    {
        watcher.ignoreErrorsMatching((s) -> s.contains("has mismatched endpoint or control"));
        launch("aeron:ipc");

        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder(
            "aeron:udp?endpoint=127.0.0.1:0|control=127.0.0.1:9999|tags=1001");

        try (Subscription ignore1 = subscribingClient.addSubscription(builder.build(), 1000))
        {
            Objects.requireNonNull(ignore1);
            assertThrows(
                RegistrationException.class,
                () -> subscribingClient.addSubscription(builder.controlEndpoint("127.0.0.1:10000").build(), 1000));
            assertThrows(
                RegistrationException.class,
                () -> subscribingClient.addSubscription(builder.controlEndpoint("127.0.0.2:9999").build(), 1000));
        }
    }

    @Test
    void shouldRejectSubscriptionsIfUsingTagsAndParametersAndEndpointDoesNotMatchSocketReceiveBufferLength()
    {
        watcher.ignoreErrorsMatching((s) -> true);
        launch("aeron:ipc");

        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder(
            "aeron:udp?endpoint=127.0.0.1:0|control=127.0.0.1:9999|so-rcvbuf=128K|tags=1001");

        try (Subscription ignore1 = subscribingClient.addSubscription(builder.build(), 1000))
        {
            Objects.requireNonNull(ignore1);
            assertThrows(
                RegistrationException.class,
                () -> subscribingClient.addSubscription(builder.socketRcvbufLength(64 * 1024).build(), 1000));
        }
    }

    @Test
    void shouldAllowPublicationsIfUsingTagsAndParametersAndAllMatch()
    {
        launch("aeron:ipc");

        final String channel = "aeron:udp?endpoint=127.0.0.1:9999|control=127.0.0.1:0|tags=1001";
        try (
            Publication ignore1 = subscribingClient.addPublication(channel, 1000);
            Publication ignore2 = subscribingClient.addPublication(channel, 1000))
        {
            Objects.requireNonNull(ignore1);
            Objects.requireNonNull(ignore2);
        }
    }

    @Test
    void shouldRejectPublicationsIfUsingTagsAndParametersAndEndpointDoesNotMatchEndpointWithExplicitControl()
    {
        watcher.ignoreErrorsMatching((s) -> s.contains("has mismatched endpoint or control"));
        launch("aeron:ipc");

        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder(
            "aeron:udp?endpoint=127.0.0.1:10000|control=127.0.0.1:0|tags=1001");

        try (Publication ignore1 = subscribingClient.addPublication(builder.build(), 1000))
        {
            Objects.requireNonNull(ignore1);
            assertThrows(
                RegistrationException.class,
                () -> subscribingClient.addPublication(builder.endpoint("127.0.0.1:9999").build(), 1000));
            assertThrows(
                RegistrationException.class,
                () -> subscribingClient.addPublication(builder.endpoint("127.0.0.2:10000").build(), 1000));
        }
    }

    @Test
    void shouldRejectPublicationsIfUsingTagsAndParametersAndEndpointDoesNotMatchEndpointWithoutControl()
    {
        watcher.ignoreErrorsMatching((s) -> s.contains("has mismatched endpoint or control"));
        launch("aeron:ipc");

        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder(
            "aeron:udp?endpoint=127.0.0.1:10000|tags=1001");

        try (Subscription ignore1 = subscribingClient.addSubscription(builder.build(), 1000))
        {
            Objects.requireNonNull(ignore1);
            assertThrows(
                RegistrationException.class,
                () -> subscribingClient.addSubscription(builder.endpoint("127.0.0.1:9999").build(), 1000));
            assertThrows(
                RegistrationException.class,
                () -> subscribingClient.addSubscription(builder.endpoint("127.0.0.2:10000").build(), 1000));
        }
    }

    @Test
    void shouldRejectPublicationsIfUsingTagsAndParametersAndEndpointDoesNotMatchControl()
    {
        watcher.ignoreErrorsMatching((s) -> s.contains("has mismatched endpoint or control"));
        launch("aeron:ipc");

        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder(
            "aeron:udp?endpoint=127.0.0.1:10000|control=127.0.0.1:0|tags=1001");

        try (Publication ignore1 = subscribingClient.addPublication(builder.build(), 1000))
        {
            Objects.requireNonNull(ignore1);
            assertThrows(
                RegistrationException.class,
                () -> subscribingClient.addPublication(builder.controlEndpoint("127.0.0.1:10000").build(), 1000));
            assertThrows(
                RegistrationException.class,
                () -> subscribingClient.addPublication(builder.controlEndpoint("127.0.0.2:0").build(), 1000));
        }
    }

    @Test
    void shouldRejectPublicationsIfUsingTagsAndParametersAndMtuDoesNotMatch()
    {
        watcher.ignoreErrorsMatching((s) -> true);
        launch("aeron:ipc");

        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder(
            "aeron:udp?endpoint=127.0.0.1:10000|control=127.0.0.1:0|mtu=1408|tags=1001");

        try (Publication ignore1 = subscribingClient.addPublication(builder.build(), 1000))
        {
            Objects.requireNonNull(ignore1);
            assertThrows(
                RegistrationException.class,
                () -> subscribingClient.addPublication(builder.mtu(8192).build(), 1000));
        }
    }

    @ParameterizedTest
    @MethodSource("fragmentAssemblers")
    @SuppressWarnings("MethodLength")
    void shouldReturnCompleteHeaderForAssembledMessages(final Class<?> assemblerClass)
        throws ReflectiveOperationException
    {
        final ArrayList<ExpectedFragment> messages = new ArrayList<>();
        final MutableInteger messageIndex = new MutableInteger();
        final FragmentHandler rawFragmentHandler =
            (buffer, offset, length, header) -> verifyFragment(buffer, offset, length, header, messageIndex, messages);
        final FragmentHandler fragmentAssembler;
        final ControlledFragmentHandler controlledFragmentAssembler;
        if (FragmentHandler.class.isAssignableFrom(assemblerClass))
        {
            fragmentAssembler = (FragmentHandler)assemblerClass.getConstructor(FragmentHandler.class)
                .newInstance(rawFragmentHandler);
            controlledFragmentAssembler = null;
        }
        else
        {
            final ControlledFragmentHandler delegate = (buffer, offset, length, header) ->
            {
                rawFragmentHandler.onFragment(buffer, offset, length, header);
                return ControlledFragmentHandler.Action.COMMIT;
            };
            controlledFragmentAssembler =
                (ControlledFragmentHandler)assemblerClass.getConstructor(ControlledFragmentHandler.class)
                    .newInstance(delegate);
            fragmentAssembler = null;
        }

        final int mtu = 2048;
        final int maxPayloadLength = mtu - HEADER_LENGTH;
        final int termLength = 64 * 1024;
        final int paddingLength = 1376;
        final int termOffset = termLength - paddingLength;
        final int initialTermId = 5;
        final int termId = 13;
        final long initialPosition = termLength * (termId - initialTermId) + termOffset;
        final UnsafeBuffer data = new UnsafeBuffer(new byte[maxPayloadLength * 3 + 317]);
        ThreadLocalRandom.current().nextBytes(data.byteArray());
        launch("aeron:ipc?mtu=" + mtu + "|term-length=64K|init-term-id=" + initialTermId +
            "|term-id=" + termId + "|term-offset=" + termOffset);

        try (Subscription unfragmentedSubscription =
            subscribingClient.addSubscription(subscription.channel(), STREAM_ID))
        {
            Tests.awaitConnected(publication);
            Tests.awaitConnected(subscription);
            Tests.awaitConnected(unfragmentedSubscription);

            final BufferClaim bufferClaim = new BufferClaim();
            final int firstMessageLength = 100;
            while (publication.tryClaim(firstMessageLength, bufferClaim) < 0)
            {
                Tests.yield();
            }
            final int headerType = HDR_TYPE_RSP_SETUP;
            final byte expectedFlags = (byte)(BEGIN_END_AND_EOS_FLAGS | 0xA);
            final long reservedValue = 13131313139871L;
            bufferClaim.headerType(headerType);
            bufferClaim.flags(expectedFlags);
            bufferClaim.reservedValue(reservedValue);
            bufferClaim.buffer().putBytes(bufferClaim.offset(), data, 0, firstMessageLength);
            bufferClaim.commit();

            final long secondReservedValue = -4239462982749823794L;
            final MutableLong fragmentedReservedValue = new MutableLong(secondReservedValue);
            final ReservedValueSupplier reservedValueSupplier =
                (tb, to, fl) -> fragmentedReservedValue.getAndAdd(reservedValue);
            while (publication.offer(data, 0, data.capacity(), reservedValueSupplier) < 0)
            {
                Tests.yield();
            }
            final int fragmentedMessageLength = computeFragmentedFrameLength(data.capacity(), maxPayloadLength);
            final int frameLength = HEADER_LENGTH + data.capacity();
            final int position1 = ((termId + 1 - initialTermId) * termLength) + fragmentedMessageLength;

            long position2;
            while ((position2 = publication.tryClaim(maxPayloadLength, bufferClaim)) < 0)
            {
                Tests.yield();
            }
            bufferClaim.headerType(HDR_TYPE_SM);
            bufferClaim.flags((byte)0xFF);
            bufferClaim.reservedValue(42);
            bufferClaim.buffer().putBytes(bufferClaim.offset(), data, 0, maxPayloadLength);
            bufferClaim.commit();

            messageIndex.set(0);
            messages.clear();
            final Image fragmentedImage = subscription.imageAtIndex(0);
            messages.add(new ExpectedFragment(
                fragmentedImage,
                firstMessageLength + HEADER_LENGTH,
                CURRENT_VERSION,
                expectedFlags,
                (short)headerType,
                termOffset,
                publication.sessionId(),
                STREAM_ID,
                termId,
                reservedValue,
                initialTermId,
                publication.positionBitsToShift(),
                BitUtil.align(initialPosition + firstMessageLength + HEADER_LENGTH, FRAME_ALIGNMENT),
                data,
                0,
                firstMessageLength));
            messages.add(new ExpectedFragment(
                fragmentedImage,
                frameLength,
                CURRENT_VERSION,
                (byte)BEGIN_AND_END_FLAGS,
                (short)HDR_TYPE_DATA,
                0,
                publication.sessionId(),
                STREAM_ID,
                termId + 1,
                secondReservedValue,
                initialTermId,
                publication.positionBitsToShift(),
                position1,
                data,
                0,
                data.capacity()));
            messages.add(new ExpectedFragment(
                fragmentedImage,
                mtu,
                CURRENT_VERSION,
                (byte)0xFF,
                (short)HDR_TYPE_SM,
                fragmentedMessageLength,
                publication.sessionId(),
                STREAM_ID,
                termId + 1,
                42,
                initialTermId,
                publication.positionBitsToShift(),
                position2,
                data,
                0,
                maxPayloadLength));

            if (null != fragmentAssembler)
            {
                while (messages.size() != messageIndex.get())
                {
                    if (0 == subscription.poll(fragmentAssembler, 10))
                    {
                        Tests.yield();
                    }
                }
            }
            else
            {
                while (messages.size() != messageIndex.get())
                {
                    if (0 == subscription.controlledPoll(controlledFragmentAssembler, 10))
                    {
                        Tests.yield();
                    }
                }
            }

            messageIndex.set(0);
            messages.clear();
            final Image unfragmentedImage = unfragmentedSubscription.imageAtIndex(0);
            messages.add(new ExpectedFragment(
                unfragmentedImage,
                firstMessageLength + HEADER_LENGTH,
                CURRENT_VERSION,
                expectedFlags,
                (short)headerType,
                termOffset,
                publication.sessionId(),
                STREAM_ID,
                termId,
                reservedValue,
                initialTermId,
                publication.positionBitsToShift(),
                BitUtil.align(initialPosition + firstMessageLength + HEADER_LENGTH, FRAME_ALIGNMENT),
                data,
                0,
                firstMessageLength));
            messages.add(new ExpectedFragment(
                unfragmentedImage,
                mtu,
                CURRENT_VERSION,
                BEGIN_FRAG_FLAG,
                (short)HDR_TYPE_DATA,
                0,
                publication.sessionId(),
                STREAM_ID,
                termId + 1,
                secondReservedValue,
                initialTermId,
                publication.positionBitsToShift(),
                (termId + 1 - initialTermId) * termLength + mtu,
                data,
                0,
                maxPayloadLength));
            messages.add(new ExpectedFragment(
                unfragmentedImage,
                mtu,
                CURRENT_VERSION,
                (byte)0,
                (short)HDR_TYPE_DATA,
                mtu,
                publication.sessionId(),
                STREAM_ID,
                termId + 1,
                secondReservedValue + reservedValue,
                initialTermId,
                publication.positionBitsToShift(),
                ((termId + 1 - initialTermId) * termLength) + 2 * mtu,
                data,
                maxPayloadLength,
                maxPayloadLength));
            messages.add(new ExpectedFragment(
                unfragmentedImage,
                mtu,
                CURRENT_VERSION,
                (byte)0,
                (short)HDR_TYPE_DATA,
                2 * mtu,
                publication.sessionId(),
                STREAM_ID,
                termId + 1,
                secondReservedValue + 2 * reservedValue,
                initialTermId,
                publication.positionBitsToShift(),
                ((termId + 1 - initialTermId) * termLength) + 3 * mtu,
                data,
                2 * maxPayloadLength,
                maxPayloadLength));
            messages.add(new ExpectedFragment(
                unfragmentedImage,
                317 + HEADER_LENGTH,
                CURRENT_VERSION,
                END_FRAG_FLAG,
                (short)HDR_TYPE_DATA,
                3 * mtu,
                publication.sessionId(),
                STREAM_ID,
                termId + 1,
                secondReservedValue + 3 * reservedValue,
                initialTermId,
                publication.positionBitsToShift(),
                position1,
                data,
                3 * maxPayloadLength,
                317));
            messages.add(new ExpectedFragment(
                unfragmentedImage,
                mtu,
                CURRENT_VERSION,
                (byte)0xFF,
                (short)HDR_TYPE_SM,
                fragmentedMessageLength,
                publication.sessionId(),
                STREAM_ID,
                termId + 1,
                42,
                initialTermId,
                publication.positionBitsToShift(),
                position2,
                data,
                0,
                maxPayloadLength));

            while (messages.size() != messageIndex.get())
            {
                if (0 == unfragmentedSubscription.poll(rawFragmentHandler, 1))
                {
                    Tests.yield();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("channels")
    void shouldMarkPublicationNotConnectedWhenItLoosesAllSubscribers(final String channel)
    {
        TestMediaDriver.notSupportedOnCMediaDriver("publication image state management");

        driverContext.timerIntervalNs(MILLISECONDS.toNanos(1500))
            .publicationConnectionTimeoutNs(SECONDS.toNanos(2));
        launch(channel);
        Tests.awaitConnected(publication);
        Tests.awaitConnected(subscription);

        final BufferClaim bufferClaim = new BufferClaim();
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        while (publication.tryClaim(random.nextInt(1, 1000), bufferClaim) < 0)
        {
            Tests.yield();
        }
        final int msgLength = bufferClaim.length();
        bufferClaim.buffer().setMemory(bufferClaim.offset(), msgLength, (byte)random.nextInt());
        bufferClaim.commit();

        final MutableBoolean received = new MutableBoolean();
        final FragmentHandler fragmentHandler = (buffer, offset, length, header) ->
        {
            received.set(true);
            assertEquals(msgLength, length);
        };
        while (0 == subscription.poll(fragmentHandler, 1))
        {
            Tests.yield();
        }
        assertTrue(received.get());

        subscription.close();

        final long startNs = System.nanoTime();
        Tests.await(() -> !publication.isConnected());
        final long durationNs = System.nanoTime() - startNs;
        assertThat(durationNs, lessThan(driver.context().timerIntervalNs() / 2));
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldCleanupImagesWhenPublicationIsClosed(final String channel)
    {
        final AtomicInteger unavailableCount = new AtomicInteger();
        doAnswer(invocation ->
        {
            unavailableCount.getAndIncrement();
            return null;
        }).when(unavailableImageHandler).onUnavailableImage(any(Image.class));

        launch(channel);
        final Subscription sub2 =
            subscribingClient.addSubscription(channel, STREAM_ID, availableImageHandler, unavailableImageHandler);
        Tests.awaitConnected(publication);
        Tests.awaitConnected(subscription);
        Tests.awaitConnected(sub2);

        final BufferClaim bufferClaim = new BufferClaim();
        final int msgLength = 555;
        while (publication.tryClaim(msgLength, bufferClaim) < 0)
        {
            Tests.yield();
        }
        bufferClaim.buffer().setMemory(bufferClaim.offset(), msgLength, (byte)0xFF);
        bufferClaim.commit();

        final Image image = subscription.imageAtIndex(0);
        final Image image2 = sub2.imageAtIndex(0);
        assertNotSame(image, image2);
        assertEquals(image.correlationId(), image2.correlationId());

        while (0 == image.poll(fragmentHandler, 1) || 0 == image2.poll(fragmentHandler, 1))
        {
            Tests.yield();
        }
        verify(fragmentHandler, times(2)).onFragment(
            any(DirectBuffer.class), eq(HEADER_LENGTH), eq(msgLength), any(Header.class));

        final Path aeronDir = driver.context().aeronDirectory().toPath();
        final Path pubLogBuffer = aeronDir.resolve("publications/" + publication.registrationId() + ".logbuffer");
        final Path imageLogBuffer;
        if (ChannelUri.parse(channel).isIpc())
        {
            imageLogBuffer = pubLogBuffer;
        }
        else
        {
            imageLogBuffer = aeronDir.resolve("images/" + image.correlationId() + ".logbuffer");
        }
        assertTrue(Files.exists(pubLogBuffer));
        assertTrue(Files.exists(imageLogBuffer));

        publication.close();

        Tests.await(() -> 2 == unavailableCount.get());
        assertEquals(0, subscription.imageCount());
        assertEquals(0, sub2.imageCount());

        while (Files.exists(pubLogBuffer) || Files.exists(imageLogBuffer))
        {
            Tests.yield();
        }
        assertFalse(Files.exists(pubLogBuffer));
        assertFalse(Files.exists(imageLogBuffer));
    }

    private static void verifyFragment(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header,
        final MutableInteger messageIndex,
        final ArrayList<ExpectedFragment> messages)
    {
        final int index = messageIndex.getAndIncrement();
        final Supplier<String> errorMsg = () -> "index=" + index;
        final ExpectedFragment expectedFragment = messages.get(index);
        assertSame(expectedFragment.image, header.context(), errorMsg);
        assertEquals(expectedFragment.frameLength, header.frameLength(), errorMsg);
        assertEquals(
            expectedFragment.version,
            header.buffer().getByte(header.offset() + VERSION_FIELD_OFFSET),
            errorMsg);
        assertEquals(expectedFragment.flags, header.flags(), errorMsg);
        assertEquals(expectedFragment.type, (short)header.type(), errorMsg);
        assertEquals(expectedFragment.termOffset, header.termOffset(), errorMsg);
        assertEquals(expectedFragment.sessionId, header.sessionId(), errorMsg);
        assertEquals(expectedFragment.streamId, header.streamId(), errorMsg);
        assertEquals(expectedFragment.termId, header.termId(), errorMsg);
        assertEquals(expectedFragment.reservedValue, header.reservedValue(), errorMsg);
        assertEquals(expectedFragment.initialTermId, header.initialTermId(), errorMsg);
        assertEquals(expectedFragment.positionBitsToShift, header.positionBitsToShift(), errorMsg);
        assertEquals(expectedFragment.position, header.position(), errorMsg);
        assertEquals(expectedFragment.payload.length, length, errorMsg);
        for (int i = 0; i < length; i++)
        {
            assertEquals(expectedFragment.payload[i], buffer.getByte(offset + i), errorMsg);
        }
    }

    private void publishMessage()
    {
        buffer.putInt(0, 1);

        while (publication.offer(buffer, 0, SIZE_OF_INT) < 0L)
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
                break;
            }

            Tests.yield();
        }
    }

    private void pollForBatch(final int batchSize)
    {
        long fragmentsRead = 0;

        while (true)
        {
            final int fragments = subscription.poll(fragmentHandler, 10);
            fragmentsRead += fragments;

            if (fragmentsRead >= batchSize)
            {
                break;
            }

            if (0 == fragments)
            {
                Tests.yield();
            }
        }
    }

    private static List<Class<?>> fragmentAssemblers()
    {
        return Arrays.asList(
            FragmentAssembler.class,
            ImageFragmentAssembler.class,
            ControlledFragmentAssembler.class,
            ImageControlledFragmentAssembler.class);
    }

    private static final class ExpectedFragment
    {
        private final Image image;
        final int frameLength;
        final byte version;
        final byte flags;
        final short type;
        final int termOffset;
        final int sessionId;
        final int streamId;
        final int termId;
        final long reservedValue;
        final int initialTermId;
        final int positionBitsToShift;
        final long position;
        final byte[] payload;

        private ExpectedFragment(
            final Image image,
            final int frameLength,
            final byte version,
            final byte flags,
            final short type,
            final int termOffset,
            final int sessionId,
            final int streamId,
            final int termId,
            final long reservedValue,
            final int initialTermId,
            final int positionBitsToShift,
            final long position,
            final UnsafeBuffer payload,
            final int payloadOffset,
            final int payloadLength)
        {
            this.image = image;
            this.frameLength = frameLength;
            this.version = version;
            this.flags = flags;
            this.type = type;
            this.termOffset = termOffset;
            this.sessionId = sessionId;
            this.streamId = streamId;
            this.termId = termId;
            this.reservedValue = reservedValue;
            this.initialTermId = initialTermId;
            this.positionBitsToShift = positionBitsToShift;
            this.position = position;
            this.payload = new byte[payloadLength];
            payload.getBytes(payloadOffset, this.payload);
        }
    }
}
