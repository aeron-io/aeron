/*
 * Copyright 2014-2023 Real Logic Limited.
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
import io.aeron.exceptions.RegistrationException;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.response.ResponseClient;
import io.aeron.response.ResponseServer;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.aeron.AeronCounters.DRIVER_PUBLISHER_POS_TYPE_ID;
import static io.aeron.CommonContext.*;
import static io.aeron.driver.status.SendChannelStatus.SEND_CHANNEL_STATUS_TYPE_ID;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(InterruptingTestCallback.class)
public class ResponseChannelsTest
{
    private static final String REQUEST_ENDPOINT = "localhost:10000";
    private static final int REQUEST_STREAM_ID = 10000;
    private static final String RESPONSE_CONTROL = "localhost:10001";
    private static final int RESPONSE_STREAM_ID = 10001;

    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private TestMediaDriver driver1;
    private TestMediaDriver driver2;

    @BeforeEach
    void setUp()
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(generateRandomDirName())
            .dirDeleteOnShutdown(true)
            .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
            .threadingMode(ThreadingMode.SHARED);

        driver1 = TestMediaDriver.launch(
            context.clone()
                .aeronDirectoryName(context.aeronDirectoryName() + "-1")
                /*
                For some reason, revoke() works much quicker against the java media driver.
                There's a check in the LINGER state for having received a unicast EOS.  In java, we usually/always
                see it more or less immediately, but in C, we don't.  So in C, we have to wait for the linger timeout.
                Ultimately, it would be good to get this addressed in the C media driver.
                 */
                .publicationLingerTimeoutNs(200_000_000L),
            watcher);
        driver2 = TestMediaDriver.launch(
            context.clone().aeronDirectoryName(context.aeronDirectoryName() + "-2"), watcher);
        watcher.dataCollector().add(driver1.context().aeronDirectory());
        watcher.dataCollector().add(driver2.context().aeronDirectory());
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.quietCloseAll(driver1, driver2);
    }

    @Test
    @InterruptAfter(10)
    void shouldReceiveResponsesOnAPerClientBasis() throws Exception
    {
        final String textA = "hello from client A";
        final String textB = "hello from client B";
        final MutableDirectBuffer messageA = new UnsafeBuffer(textA.getBytes(UTF_8));
        final MutableDirectBuffer messageB = new UnsafeBuffer(textB.getBytes(UTF_8));
        final IdleStrategy idleStrategy = YieldingIdleStrategy.INSTANCE;
        final List<String> responsesA = new ArrayList<>();
        final List<String> responsesB = new ArrayList<>();
        final FragmentHandler fragmentHandlerA =
            (buffer, offset, length, header) -> responsesA.add(buffer.getStringWithoutLengthUtf8(offset, length));
        final FragmentHandler fragmentHandlerB =
            (buffer, offset, length, header) -> responsesB.add(buffer.getStringWithoutLengthUtf8(offset, length));

        try (Aeron server = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron clientA = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron clientB = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver2.aeronDirectoryName()));
            ResponseServer responseServer = new ResponseServer(
                server, (image) -> new EchoHandler(), REQUEST_ENDPOINT, REQUEST_STREAM_ID,
                RESPONSE_CONTROL, RESPONSE_STREAM_ID, null, null);
            ResponseClient responseClientA = new ResponseClient(
                clientA, fragmentHandlerA, REQUEST_ENDPOINT, REQUEST_STREAM_ID, RESPONSE_CONTROL, RESPONSE_STREAM_ID);
            ResponseClient responseClientB = new ResponseClient(
                clientB, fragmentHandlerB, REQUEST_ENDPOINT, REQUEST_STREAM_ID, RESPONSE_CONTROL, RESPONSE_STREAM_ID))
        {
            final Supplier<String> msg = () -> "responseServer.sessionCount=" + responseServer.sessionCount() + " " +
                "clientA=" + responseClientA + " clientB=" + responseClientB;

            while (responseServer.sessionCount() < 2 ||
                !responseClientA.isConnected() ||
                !responseClientB.isConnected())
            {
                idleStrategy.idle(run(responseServer, responseClientA, responseClientB));
                Tests.checkInterruptStatus(msg);
            }

            while (0 > responseClientA.offer(messageA))
            {
                idleStrategy.idle(run(responseServer, responseClientA, responseClientB));
                Tests.checkInterruptStatus("unable to offer message to client A");
            }

            while (0 > responseClientB.offer(messageB))
            {
                idleStrategy.idle(run(responseServer, responseClientA, responseClientB));
                Tests.checkInterruptStatus("unable to offer message to client B");
            }

            while (!responsesA.contains(textA) || !responsesB.contains(textB))
            {
                idleStrategy.idle(run(responseServer, responseClientA, responseClientB));
                Tests.checkInterruptStatus("failed to receive responses");
            }

            assertEquals(1, responsesA.size(), "A=" + responsesA + ", B=" + responsesB);
            assertEquals(1, responsesB.size(), "A=" + responsesA + ", B=" + responsesB);
        }
    }

    @Test
    @InterruptAfter(15)
    void shouldConnectResponsePublicationUsingImage()
    {
        try (Aeron server = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron client = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Subscription subReq = server.addSubscription(
                "aeron:udp?endpoint=localhost:10001", REQUEST_STREAM_ID);
            Subscription subRsp = client.addSubscription(
                "aeron:udp?control-mode=response|control=localhost:10002", RESPONSE_STREAM_ID);
            Publication pubReq = client.addPublication(
                "aeron:udp?endpoint=localhost:10001|response-correlation-id=" + subRsp.registrationId(),
                REQUEST_STREAM_ID))
        {
            Tests.awaitConnected(subReq);
            Tests.awaitConnected(pubReq);
            Objects.requireNonNull(subRsp);

            final Image image = subReq.imageAtIndex(0);
            final String url = "aeron:udp?control-mode=response|control=localhost:10002|response-correlation-id=" +
                image.correlationId();

            try (Publication pubRsp = server.addPublication(url, RESPONSE_STREAM_ID))
            {
                Tests.awaitConnected(subRsp);
                Tests.awaitConnected(pubRsp);
            }
        }
    }

    @ParameterizedTest
    @InterruptAfter(5)
    @ValueSource(booleans = { true, false })
    void shouldConnectResponsePublicationUsingImageAndIpc(final boolean useExclusive)
    {
        CloseHelper.quietClose(driver2);

        try (Aeron server = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron client = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Subscription subReq = server.addSubscription("aeron:ipc", REQUEST_STREAM_ID))
        {
            try (Subscription subRsp1 = client.addSubscription(
                "aeron:ipc?control-mode=response|alias=client1", RESPONSE_STREAM_ID);
                Publication pubReq1 = newPublication(
                    useExclusive,
                    client,
                    "aeron:ipc?response-correlation-id=" + subRsp1.registrationId(),
                    REQUEST_STREAM_ID);
                Subscription subRsp2 = client.addSubscription(
                    "aeron:ipc?control-mode=response|alias=client2", RESPONSE_STREAM_ID);
                Publication pubReq2 = newPublication(
                    useExclusive,
                    client,
                    "aeron:ipc?response-correlation-id=" + subRsp2.registrationId(),
                    REQUEST_STREAM_ID))
            {
                Tests.awaitConnected(pubReq1);
                Tests.awaitConnected(pubReq2);
                Tests.await(() -> 2 == subReq.imageCount());

                final String url1 = "aeron:ipc?control-mode=response|response-correlation-id=" +
                    subReq.imageAtIndex(0).correlationId();
                final String url2 = "aeron:ipc?control-mode=response|response-correlation-id=" +
                    subReq.imageAtIndex(1).correlationId();

                try (Publication pubRsp1 = newPublication(useExclusive, server, url1, RESPONSE_STREAM_ID);
                    Publication pubRsp2 = newPublication(useExclusive, server, url2, RESPONSE_STREAM_ID))
                {
                    Tests.awaitConnected(subRsp1);
                    Tests.awaitConnected(subRsp2);
                    Tests.awaitConnected(pubRsp1);
                    Tests.awaitConnected(pubRsp2);

                    final DirectBuffer msg1 = new UnsafeBuffer("msg1".getBytes(UTF_8));
                    final DirectBuffer msg2 = new UnsafeBuffer("msg2".getBytes(UTF_8));

                    while (pubRsp1.offer(msg1) < 0)
                    {
                        Tests.yield();
                    }

                    while (pubRsp2.offer(msg2) < 0)
                    {
                        Tests.yield();
                    }

                    final long deadlineMs = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(1);
                    int sub1FragmentCount = 0;
                    int sub2FragmentCount = 0;
                    while (System.currentTimeMillis() < deadlineMs)
                    {
                        sub1FragmentCount += subRsp1.poll((buffer, offset, length, header) -> {}, 10);
                        sub2FragmentCount += subRsp2.poll((buffer, offset, length, header) -> {}, 10);
                        Tests.yield();

                        assertTrue(sub1FragmentCount < 2);
                        assertTrue(sub2FragmentCount < 2);
                    }
                }
            }
        }
    }

    private static Publication newPublication(
        final boolean useExclusive,
        final Aeron client,
        final String channel,
        final int streamId)
    {
        return useExclusive ? client.addExclusivePublication(channel, streamId) :
            client.addPublication(channel, streamId);
    }

    @Test
    @InterruptAfter(20)
    void shouldCorrectlyHandleSubscriptionClosesOnPartiallyCreatedResponseSubscriptions()
    {
        final int responseStreamIdB = RESPONSE_STREAM_ID + 1;
        try (Aeron server = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron client = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Subscription subReq = server.addSubscription("aeron:udp?endpoint=localhost:10001", REQUEST_STREAM_ID);
            Subscription subRspB = client.addSubscription(
                "aeron:udp?control-mode=response|control=localhost:10002|endpoint=localhost:10003",
                responseStreamIdB))
        {
            Objects.requireNonNull(subRspB);

            try (
                Subscription subRspA = client.addSubscription(
                    "aeron:udp?control-mode=response|control=localhost:10002|endpoint=localhost:10003",
                    RESPONSE_STREAM_ID);
                Publication pubReqA = client.addPublication(
                    "aeron:udp?endpoint=localhost:10001|response-correlation-id=" + subRspA.registrationId(),
                    REQUEST_STREAM_ID))
            {
                Tests.awaitConnected(subReq);
                Tests.awaitConnected(pubReqA);

                final Image image = subReq.imageAtIndex(0);
                final String url = "aeron:udp?control-mode=response|control=localhost:10002|response-correlation-id=" +
                    image.correlationId();

                try (Publication pubRsp = client.addPublication(url, RESPONSE_STREAM_ID))
                {
                    Tests.awaitConnected(subRspA);
                    Tests.awaitConnected(pubRsp);
                }

                while (subRspA.isConnected())
                {
                    Tests.yield();
                }
            }

            while (subReq.isConnected())
            {
                Tests.yield();
            }

            try (
                Publication pubReqB = client.addPublication(
                    "aeron:udp?endpoint=localhost:10001|response-correlation-id=" + subRspB.registrationId(),
                    REQUEST_STREAM_ID))
            {
                Tests.awaitConnected(subReq);
                Tests.awaitConnected(pubReqB);

                final Image image = subReq.imageAtIndex(0);
                final String url = "aeron:udp?control-mode=response|control=localhost:10002|response-correlation-id=" +
                    image.correlationId();

                try (Publication pubRsp = client.addPublication(url, responseStreamIdB))
                {
                    Tests.awaitConnected(subRspB);
                    Tests.awaitConnected(pubRsp);
                }

                while (subRspB.isConnected())
                {
                    Tests.yield();
                }
            }
        }
    }

    @Test
    @InterruptAfter(15)
    void shouldNotConnectSecondResponseSubscriptionUntilMatchingPublicationIsCreated()
    {
        try (Aeron server = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron client = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Subscription subReq = server.addSubscription(
                "aeron:udp?endpoint=localhost:10001", REQUEST_STREAM_ID);
            Subscription subRsp = client.addSubscription(
                "aeron:udp?control-mode=response|control=localhost:10002", RESPONSE_STREAM_ID);
            Subscription subRspAux = client.addSubscription(
                "aeron:udp?control-mode=response|control=localhost:10002", RESPONSE_STREAM_ID);
            Publication pubReq = client.addPublication(
                "aeron:udp?endpoint=localhost:10001|response-correlation-id=" + subRsp.registrationId(),
                REQUEST_STREAM_ID))
        {
            Tests.awaitConnected(subReq);
            Tests.awaitConnected(pubReq);
            Objects.requireNonNull(subRsp);

            final Image image = subReq.imageAtIndex(0);
            final String url = "aeron:udp?control-mode=response|control=localhost:10002|response-correlation-id=" +
                image.correlationId();

            try (Publication pubRsp = client.addPublication(url, RESPONSE_STREAM_ID))
            {
                Tests.awaitConnected(subRsp);
                Tests.awaitConnected(pubRsp);

                final long deadlineMs = System.currentTimeMillis() + 1_000;
                while (System.currentTimeMillis() <= deadlineMs)
                {
                    assertFalse(subRspAux.isConnected());
                }
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldUseResponseCorrelationIdAsAPublicationMatchingCriteria()
    {
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Subscription sub = aeron.addSubscription("aeron:udp?endpoint=localhost:10000", 10001);
            Publication pubA = aeron.addPublication("aeron:udp?endpoint=localhost:10000", 10001);
            Publication pubB = aeron.addPublication("aeron:udp?endpoint=localhost:10000", 10001))
        {
            Tests.awaitConnected(sub);
            Tests.awaitConnected(pubA);
            Tests.awaitConnected(pubB);

            assertEquals(pubA.originalRegistrationId(), pubB.originalRegistrationId());
        }

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Subscription sub = aeron.addSubscription("aeron:udp?endpoint=localhost:10000", 10001);
            Subscription rspSub = aeron.addSubscription("aeron:udp?control-mode=response", 10001);
            Publication pubA = aeron.addPublication(
                "aeron:udp?endpoint=localhost:10000|response-correlation-id=" + rspSub.registrationId(), 10001);
            Publication pubB = aeron.addPublication(
                "aeron:udp?endpoint=localhost:10000|response-correlation-id=" + rspSub.registrationId(), 10001))
        {
            Tests.awaitConnected(sub);
            Tests.awaitConnected(pubA);
            Tests.awaitConnected(pubB);

            assertEquals(pubA.originalRegistrationId(), pubB.originalRegistrationId());
        }

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Subscription sub = aeron.addSubscription("aeron:udp?endpoint=localhost:10000", 10001);
            Subscription rspSubA = aeron.addSubscription("aeron:udp?control-mode=response", 10001);
            Subscription rspSubB = aeron.addSubscription("aeron:udp?control-mode=response", 10001);
            Publication pubA = aeron.addPublication(
                "aeron:udp?endpoint=localhost:10000|response-correlation-id=" + rspSubA.registrationId(), 10001);
            Publication pubB = aeron.addPublication(
                "aeron:udp?endpoint=localhost:10000|response-correlation-id=" + rspSubB.registrationId(), 10001))
        {
            Tests.awaitConnected(sub);
            Tests.awaitConnected(pubA);
            Tests.awaitConnected(pubB);

            assertNotEquals(pubA.originalRegistrationId(), pubB.originalRegistrationId());
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldErrorCreatingResponsePublicationWithImageThatDidNotRequestAResponseChannel()
    {
        watcher.ignoreErrorsMatching(s -> s.contains("did not request a response channel"));

        final int reqStreamId = 10001;
        final int rspStreamId = 10002;

        try (Aeron server = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron client = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Subscription subReq = server.addSubscription(
                "aeron:udp?endpoint=localhost:10001", reqStreamId);
            Publication pubReq = client.addPublication(
                "aeron:udp?endpoint=localhost:10001", reqStreamId))
        {
            Tests.awaitConnected(subReq);
            Tests.awaitConnected(pubReq);

            final Image image = subReq.imageAtIndex(0);
            final String url = "aeron:udp?control-mode=response|control=localhost:10002|response-correlation-id=" +
                image.correlationId();

            assertThrows(Exception.class, () -> client.addPublication(url, rspStreamId));
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldErrorCreatingResponsePublicationWithMissingPublicationImage()
    {
        watcher.ignoreErrorsMatching(s -> s.contains("image.correlationId=") && s.contains(" not found"));

        final int reqStreamId = 10001;
        final int rspStreamId = 10002;

        try (Aeron server = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron client = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver2.aeronDirectoryName()));
            Subscription subReq = server.addSubscription(
                "aeron:udp?endpoint=localhost:10001", reqStreamId);
            Publication pubReq = client.addPublication(
                "aeron:udp?endpoint=localhost:10001", reqStreamId))
        {
            Tests.awaitConnected(subReq);
            Tests.awaitConnected(pubReq);

            final Image image = subReq.imageAtIndex(0);
            final String url = "aeron:udp?control-mode=response|control=localhost:10002|response-correlation-id=" +
                image.correlationId();

            assertThrows(Exception.class, () -> client.addPublication(url, rspStreamId));
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldErrorCreatingResponsePublicationWithUnknownImage()
    {
        watcher.ignoreErrorsMatching(s -> s.contains("not found"));

        final int reqStreamId = 10001;
        final int rspStreamId = 10002;

        try (Aeron server = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron client = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver2.aeronDirectoryName()));
            Subscription subReq = server.addSubscription(
                "aeron:udp?endpoint=localhost:10001", reqStreamId);
            Publication pubReq = client.addPublication(
                "aeron:udp?endpoint=localhost:10001", reqStreamId))
        {
            Tests.awaitConnected(subReq);
            Tests.awaitConnected(pubReq);

            final Image image = subReq.imageAtIndex(0);
            final long wrongCorrelationId = image.correlationId() + 10;
            final String url =
                "aeron:udp?control-mode=response|control=localhost:10002|response-correlation-id=" + wrongCorrelationId;

            assertThrows(Exception.class, () -> client.addPublication(url, rspStreamId));
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldErrorIfResponseCorrelationIdIsMissingFromAControlModeResponsePublication()
    {
        watcher.ignoreErrorsMatching(
            s -> s.contains("control-mode=response was specified, but no response-correlation-id set"));

        final int reqStreamId = 10001;
        final int rspStreamId = 10002;

        try (Aeron server = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron client = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver2.aeronDirectoryName()));
            Subscription subReq = server.addSubscription(
                "aeron:udp?endpoint=localhost:10001", reqStreamId);
            Publication pubReq = client.addPublication(
                "aeron:udp?endpoint=localhost:10001", reqStreamId))
        {
            Tests.awaitConnected(subReq);
            Tests.awaitConnected(pubReq);

            final String url = "aeron:udp?control-mode=response|control=localhost:10002";

            assertThrows(Exception.class, () -> client.addPublication(url, rspStreamId));
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldBeAbleToProcessMultipleTermsWithMultipleResponseChannels() throws Exception
    {
        final UnsafeBuffer message = new UnsafeBuffer(new byte[4096]);
        message.setMemory(0, 4096, (byte)'x');
        final long stopPosition = 4 * 64 * 1024 + 1;

        final IdleStrategy idleStrategy = YieldingIdleStrategy.INSTANCE;
        final MutableLong pubACount = new MutableLong(0);
        final MutableLong pubBCount = new MutableLong(0);
        final MutableLong subACount = new MutableLong(0);
        final MutableLong subBCount = new MutableLong(0);

        final FragmentHandler recvA = (buffer, offset, length, header) -> subACount.set(header.reservedValue());
        final FragmentHandler recvB = (buffer, offset, length, header) -> subBCount.set(header.reservedValue());

        try (Aeron server = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron clientA = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron clientB = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver2.aeronDirectoryName()));
            ResponseServer responseServer = new ResponseServer(
                server, (image) -> new EchoHandler(), REQUEST_ENDPOINT, REQUEST_STREAM_ID,
                RESPONSE_CONTROL, RESPONSE_STREAM_ID, null, "aeron:udp?term-length=64k");
            ResponseClient responseClientA = new ResponseClient(
                clientA, recvA, REQUEST_ENDPOINT, REQUEST_STREAM_ID, RESPONSE_CONTROL, RESPONSE_STREAM_ID,
                "aeron:udp?term-length=64k", null);
            ResponseClient responseClientB = new ResponseClient(
                clientB, recvB, REQUEST_ENDPOINT, REQUEST_STREAM_ID, RESPONSE_CONTROL, RESPONSE_STREAM_ID,
                "aeron:udp?term-length=64k", null))
        {
            final Supplier<String> msg = () -> "responseServer.sessionCount=" + responseServer.sessionCount() + " " +
                "clientA=" + responseClientA + " clientB=" + responseClientB;

            while (responseServer.sessionCount() < 2 ||
                !responseClientA.isConnected() ||
                !responseClientB.isConnected())
            {
                idleStrategy.idle(run(responseServer, responseClientA, responseClientB));
                Tests.checkInterruptStatus(msg);
            }

            long subAStopPosition = 0;
            long subBStopPosition = 0;

            final Publication pubA = responseClientA.publication();
            final Publication pubB = responseClientB.publication();
            final MutableLong timeOfLastSubPositionChange = new MutableLong(System.currentTimeMillis());

            final Supplier<String> errorMessage =
                () ->
                "pubA.position=" + pubA.position() +
                ", subA.position=" + responseClientA.subscription().imageAtIndex(0).position() +
                ", pubA.count=" + (pubACount.get() - 1) +
                ", pubB.position=" + pubB.position() +
                ", subB.position=" + responseClientB.subscription().imageAtIndex(0).position() +
                ", pubB.count=" + (pubBCount.get() - 1) +
                ", idleTime=" + (System.currentTimeMillis() - timeOfLastSubPositionChange.get()) + "ms";

            long lastSubAPosition = 0;

            while (pubA.position() < stopPosition ||
                responseClientA.subscription().imageAtIndex(0).position() < subAStopPosition ||
                pubB.position() < stopPosition ||
                responseClientB.subscription().imageAtIndex(0).position() < subBStopPosition)
            {
                idleStrategy.idle(run(responseServer, responseClientA, responseClientB));
                Tests.checkInterruptStatus(errorMessage);

                if (pubA.position() < stopPosition)
                {
                    if (0 > pubA.offer(
                        message, 0, pubA.maxPayloadLength(),
                        (termBuffer, termOffset, frameLength) -> pubACount.getAndIncrement()))
                    {
                        Tests.yieldingIdle(errorMessage);
                    }
                }
                subAStopPosition = pubA.position();

                if (pubB.position() < stopPosition)
                {
                    if (0 > pubB.offer(
                        message, 0, pubB.maxPayloadLength(),
                        (termBuffer, termOffset, frameLength) -> pubBCount.getAndIncrement()))
                    {
                        Tests.yieldingIdle(errorMessage);
                    }
                }

                subBStopPosition = pubB.position();

                final long subAPosition = responseClientA.subscription().imageAtIndex(0).position();
                if (lastSubAPosition != subAPosition)
                {
                    lastSubAPosition = subAPosition;
                    timeOfLastSubPositionChange.set(System.currentTimeMillis());
                }
            }

            assertEquals(pubACount.get() - 1, subACount.get());
            assertEquals(pubBCount.get() - 1, subBCount.get());
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldErrorIfNoResponseSubscriptionFound()
    {
        watcher.ignoreErrorsMatching(s -> s.contains("unable to find response subscription"));

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Subscription rspSub = aeron.addSubscription("aeron:udp?control-mode=response", 10001))
        {
            final long wrongCorrelationId = rspSub.registrationId() + 10;
            assertThrows(Exception.class, () -> aeron.addPublication(
                "aeron:udp?endpoint=localhost:10000|response-correlation-id=" + wrongCorrelationId, 10001));
        }
    }

    @Test
    @InterruptAfter(15)
    void shouldHandleMultipleConnectionsToTheResponseChannel()
    {
        try (Aeron server = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron client = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Subscription subReq = server.addSubscription(
                "aeron:udp?endpoint=localhost:10001", REQUEST_STREAM_ID);
            Subscription subRsp = client.addSubscription(
                "aeron:udp?control-mode=response|control=localhost:10002", RESPONSE_STREAM_ID);
            Publication pubReq = client.addPublication(
                "aeron:udp?endpoint=localhost:10001|response-correlation-id=" + subRsp.registrationId(),
                REQUEST_STREAM_ID))
        {
            Tests.awaitConnected(subReq);
            Tests.awaitConnected(pubReq);
            Objects.requireNonNull(subRsp);

            final Image image = subReq.imageAtIndex(0);
            final String url = "aeron:udp?control-mode=response|control=localhost:10002|response-correlation-id=" +
                image.correlationId();

            try (Publication pubRspA = client.addPublication(url, RESPONSE_STREAM_ID))
            {
                Tests.awaitConnected(subRsp);
                Tests.awaitConnected(pubRspA);

                try (Publication pubRspB = client.addPublication(url, RESPONSE_STREAM_ID))
                {
                    Tests.awaitConnected(pubRspB);

                    assertEquals(pubRspA.originalRegistrationId(), pubRspB.originalRegistrationId());
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
        "aeron:udp?endpoint=localhost:8282, aeron:udp?endpoint=localhost:8282|control-mode=response",
        "aeron:udp?control=localhost:8282, aeron:udp?control=localhost:8282|control-mode=response",
        "aeron:udp?control=localhost:8282, aeron:udp?control-mode=response|control=localhost:8282",
        "aeron:udp?endpoint=localhost:5555|control-mode=response, aeron:udp?endpoint=localhost:5555",
        "aeron:udp?control=localhost:5555|control-mode=response, aeron:udp?control=localhost:5555",
        "aeron:udp?control-mode=response|control=localhost:5555, aeron:udp?control=localhost:5555",
    })
    void shouldRejectSubscriptionIfResponseConfigurationDoesNotMatch(final String channel1, final String channel2)
    {
        watcher.ignoreErrorsMatching(s -> s.contains("option conflicts with existing subscription"));

        final int streamId = 42;
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName())))
        {
            assertNotNull(aeron.addSubscription(channel1, streamId));

            final RegistrationException exception =
                assertThrowsExactly(RegistrationException.class, () -> aeron.addSubscription(channel2, streamId));
            MatcherAssert.assertThat(
                exception.getMessage(),
                CoreMatchers.containsString(
                "option conflicts with existing subscription: isResponse=" +
                CONTROL_MODE_RESPONSE.equals(ChannelUri.parse(channel2).get(MDC_CONTROL_MODE_PARAM_NAME)) +
                " existingChannel=" + channel1 + " channel=" + channel2));
        }
    }

    @ParameterizedTest
    @CsvSource({ "what", "-2", "-3" })
    void shouldRejectPublicationsWithBadResponseCorrelationIds(final String rci)
    {
        watcher.ignoreErrorsMatching(s -> s.contains(
            "invalid response-correlation-id, must be a number greater than or equal to -1, or 'prototype'"));

        final int streamId = 42;
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName())))
        {
            final String channel =
                "aeron:udp?endpoint=localhost:8282|control-mode=response|response-correlation-id=" + rci;
            assertThrows(RegistrationException.class, () -> aeron.addExclusivePublication(channel, streamId));
        }
    }

    @ParameterizedTest
    @CsvSource({ "true", "false" })
    @InterruptAfter(5)
    void shouldCreateNewSendChannelWithoutPrototype(final boolean usePrototype) throws Exception
    {
        final IdleStrategy idleStrategy = YieldingIdleStrategy.INSTANCE;
        final List<String> responsesA = new ArrayList<>();
        final FragmentHandler fragmentHandlerA =
            (buffer, offset, length, header) -> responsesA.add(buffer.getStringWithoutLengthUtf8(offset, length));
        final MutableReference<String> firstSendChannelLabel = new MutableReference<>();
        final MutableReference<String> secondSendChannelLabel = new MutableReference<>();

        try (Aeron server = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron clientA = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron clientB = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver2.aeronDirectoryName()));
            ResponseServer responseServer = new ResponseServer(
                server, (image) -> new EchoHandler(), REQUEST_ENDPOINT, REQUEST_STREAM_ID,
                RESPONSE_CONTROL, RESPONSE_STREAM_ID, null, null))
        {
            responseServer.usePrototype(usePrototype);

            {
                final ResponseClient responseClientA = new ResponseClient(
                    clientA, fragmentHandlerA, REQUEST_ENDPOINT,
                    REQUEST_STREAM_ID, RESPONSE_CONTROL, RESPONSE_STREAM_ID);

                final Supplier<String> msg = () -> "responseServer.sessionCount=" + responseServer.sessionCount() +
                    " " + "clientA=" + responseClientA;

                while (responseServer.sessionCount() < 1 ||
                    !responseClientA.isConnected())
                {
                    idleStrategy.idle(run(responseServer, responseClientA));
                    Tests.checkInterruptStatus(msg);
                }

                while (0 > responseClientA.offer(new UnsafeBuffer("hello from clientA".getBytes(UTF_8))))
                {
                    idleStrategy.idle(run(responseServer, responseClientA));
                    Tests.checkInterruptStatus("unable to offer message to client A");
                }

                while (responsesA.isEmpty())
                {
                    idleStrategy.idle(run(responseServer, responseClientA));
                    Tests.checkInterruptStatus("failed to receive responses");
                }

                idleStrategy.idle(run(responseClientA));

                driver1.counters().forEach((counterId, typeId, keyBuffer, label) ->
                {
                    if (SEND_CHANNEL_STATUS_TYPE_ID == typeId && label.contains("control-mode=response"))
                    {
                        firstSendChannelLabel.set(label);
                    }
                });

                responseClientA.close();
            }

            final MutableInteger publishers = new MutableInteger(0);
            do
            {
                idleStrategy.idle(run(responseServer));
                Tests.checkInterruptStatus("failed to detect publishers being closed");

                publishers.set(0);
                driver1.counters().forEach((counterId, typeId, keyBuffer, label) ->
                {
                    if (typeId == DRIVER_PUBLISHER_POS_TYPE_ID && !label.contains("prototype"))
                    {
                        publishers.increment();
                    }
                });
            }
            while (publishers.get() != 0);

            try (ResponseClient responseClientB = new ResponseClient(clientB,
                (b, o, l, h) -> {}, REQUEST_ENDPOINT, REQUEST_STREAM_ID, RESPONSE_CONTROL, RESPONSE_STREAM_ID))
            {
                final Supplier<String> msg = () -> "responseServer.sessionCount=" + responseServer.sessionCount() +
                    " " + "clientB=" + responseClientB;

                while (responseServer.sessionCount() < 1 ||
                    !responseClientB.isConnected())
                {
                    idleStrategy.idle(run(responseServer, responseClientB));
                    Tests.checkInterruptStatus(msg);
                }

                driver1.counters().forEach((counterId, typeId, keyBuffer, label) ->
                {
                    if (SEND_CHANNEL_STATUS_TYPE_ID == typeId && label.contains("control-mode=response"))
                    {
                        secondSendChannelLabel.set(label);
                    }
                });
            }

            assertEquals(usePrototype, firstSendChannelLabel.get().equals(secondSendChannelLabel.get()));
        }
    }

    @Test
    @InterruptAfter(15)
    void shouldUseLargerTermLengthWhenUsingPrototype()
    {
        try (Aeron server = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron client = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Subscription subReq = server.addSubscription(
                "aeron:udp?endpoint=localhost:10001", REQUEST_STREAM_ID);
            ExclusivePublication protoRspPub = server.addExclusivePublication(
                "aeron:udp?control-mode=response|control=localhost:10002|response-correlation-id=prototype",
                RESPONSE_STREAM_ID);
            Subscription subRsp = client.addSubscription(
                "aeron:udp?control-mode=response|control=localhost:10002", RESPONSE_STREAM_ID);
            Publication pubReq = client.addPublication(
                "aeron:udp?endpoint=localhost:10001|response-correlation-id=" + subRsp.registrationId(),
                REQUEST_STREAM_ID))
        {
            protoRspPub.revokeOnClose();

            Tests.awaitConnected(subReq);
            Tests.awaitConnected(pubReq);
            Objects.requireNonNull(subRsp);

            // TODO set different term lengths, and then verify things

            final Image image = subReq.imageAtIndex(0);
            final String url = "aeron:udp?control-mode=response|control=localhost:10002|response-correlation-id=" +
                image.correlationId();

            try (Publication pubRsp = server.addPublication(url, RESPONSE_STREAM_ID))
            {
                //System.err.println(" :: " + pubRsp.termBufferLength());
                Tests.awaitConnected(subRsp);
                Tests.awaitConnected(pubRsp);
            }
        }
    }

    private static final class EchoHandler implements ResponseServer.ResponseHandler
    {
        public boolean onMessage(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header,
            final Publication responsePublication)
        {
            return 0 < responsePublication.offer(
                buffer, offset, length, (termBuffer, termOffset, frameLength) -> header.reservedValue());
        }
    }

    private static int run(final Agent... agents) throws Exception
    {
        int work = 0;

        for (final Agent agent : agents)
        {
            work += agent.doWork();
        }

        return work;
    }
}
