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
import io.aeron.driver.status.ReceiverNaksSent;
import io.aeron.driver.status.SenderNaksReceived;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class DataLossAndRecoverySystemTest
{
    private static final int LOSS_LENGTH = 100_000;
    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private final MediaDriver.Context context = new MediaDriver.Context()
        .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
        .threadingMode(ThreadingMode.SHARED);
    private TestMediaDriver driver;

    @BeforeEach
    void setUp()
    {
        TestMediaDriver.enableFixedLoss(context, 5, 102, LOSS_LENGTH);
    }

    private void launch(final MediaDriver.Context context)
    {
        driver = TestMediaDriver.launch(context, watcher);
        watcher.dataCollector().add(driver.context().aeronDirectory());
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.quietClose(driver);
    }

    @Test
    void shouldSendStreamOfDataAndHandleLargeGapWithingSingleNakAndRetransmit()
    {
        launch(context);

        final StreamNakCounters streamCounters = sendAndReceive(
            "aeron:udp?endpoint=localhost:10000|term-length=1m|init-term-id=0|term-id=0|term-offset=0",
            10 * 1024 * 1024);

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            final CountersReader countersReader = aeron.countersReader();
            final long retransmitCount = countersReader
                .getCounterValue(SystemCounterDescriptor.RETRANSMITS_SENT.id());
            final long nakSent = countersReader
                .getCounterValue(SystemCounterDescriptor.NAK_MESSAGES_SENT.id());
            final long nakReceived = countersReader
                .getCounterValue(SystemCounterDescriptor.NAK_MESSAGES_RECEIVED.id());
            final long retransmittedBytes = countersReader
                .getCounterValue(SystemCounterDescriptor.RETRANSMITTED_BYTES.id());
            assertThat(nakSent, equalTo(streamCounters.naksSent));
            assertThat(nakReceived, equalTo(streamCounters.naksReceived));
            assertThat(retransmitCount, lessThanOrEqualTo(nakSent));
            assertThat(retransmittedBytes, greaterThanOrEqualTo((long)LOSS_LENGTH));
        }
    }

    @Test
    void shouldConfigureNakDelayPerStream()
    {
        dontCoalesceNaksOnReceiverByDefault();
        launch(context);

        final StreamNakCounters streamCounters = sendAndReceive(
            "aeron:udp?endpoint=localhost:10000|term-length=1m|init-term-id=0|term-id=0|term-offset=0|nak-delay=100us",
            10 * 1024 * 1024);

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            final CountersReader countersReader = aeron.countersReader();
            final long retransmitCount = countersReader
                .getCounterValue(SystemCounterDescriptor.RETRANSMITS_SENT.id());
            final long nakSent = countersReader
                .getCounterValue(SystemCounterDescriptor.NAK_MESSAGES_SENT.id());
            final long nakReceived = countersReader
                .getCounterValue(SystemCounterDescriptor.NAK_MESSAGES_RECEIVED.id());
            final long retransmittedBytes = countersReader
                .getCounterValue(SystemCounterDescriptor.RETRANSMITTED_BYTES.id());
            assertThat(nakSent, equalTo(streamCounters.naksSent));
            assertThat(nakReceived, equalTo(streamCounters.naksReceived));
            assertThat(retransmitCount, lessThanOrEqualTo(nakSent));
            assertThat(retransmittedBytes, greaterThanOrEqualTo((long)LOSS_LENGTH));
        }
    }

    @Test
    void shouldSendStreamOfDataAndHandleLargeGapWithSingleRetransmitEvenIfNakingFrequently()
    {
        dontCoalesceNaksOnReceiverByDefault();
        launch(context);

        final StreamNakCounters streamCounters = sendAndReceive(
            "aeron:udp?endpoint=localhost:10000|term-length=1m|init-term-id=0|term-id=0|term-offset=0",
            10 * 1024 * 1024);

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            final CountersReader countersReader = aeron.countersReader();
            final long retransmitCount = countersReader
                .getCounterValue(SystemCounterDescriptor.RETRANSMITS_SENT.id());
            final long nakSent = countersReader
                .getCounterValue(SystemCounterDescriptor.NAK_MESSAGES_SENT.id());
            final long nakReceived = countersReader
                .getCounterValue(SystemCounterDescriptor.NAK_MESSAGES_RECEIVED.id());
            final long retransmittedBytes = countersReader
                .getCounterValue(SystemCounterDescriptor.RETRANSMITTED_BYTES.id());
            assertThat(nakSent, equalTo(streamCounters.naksSent));
            assertThat(nakReceived, equalTo(streamCounters.naksReceived));
            assertThat(retransmitCount, lessThanOrEqualTo(nakSent));
            assertThat(retransmittedBytes, greaterThanOrEqualTo((long)LOSS_LENGTH));
        }
    }

    @Test
    @Disabled
    void shouldRetransmitForAllMdcSubscribers()
    {
        dontCoalesceNaksOnReceiverByDefault();
        final String baseDir = CommonContext.getAeronDirectoryName() + File.separator;

        final MediaDriver.Context contextA = context.aeronDirectoryName(baseDir + "driver-a");

        final MediaDriver.Context contextB = new MediaDriver.Context()
            .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
            .threadingMode(ThreadingMode.SHARED)
            .aeronDirectoryName(baseDir + "driver-b");

        final int streamId = 10000;
        final byte[] input = new byte[10 * 1024 * 1024];
        final byte[] outputA = new byte[10 * 1024 * 1024];
        final byte[] outputB = new byte[10 * 1024 * 1024];
        final Random r = new Random(1);
        r.nextBytes(input);
        final UnsafeBuffer sendBuffer = new UnsafeBuffer();

        int inputPosition = 0;
        final MutableInteger outputPositionA = new MutableInteger(0);
        final MutableInteger outputPositionB = new MutableInteger(0);

        final String termUriParameters = "term-length=1m|init-term-id=0|term-id=0|term-offset=0";

        try (
            TestMediaDriver driverA = TestMediaDriver.launch(contextA, watcher);
            TestMediaDriver driverB = TestMediaDriver.launch(contextB, watcher);
            Aeron aeronA = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverA.aeronDirectoryName()));
            Aeron aeronB = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverB.aeronDirectoryName()));
            ExclusivePublication pub = aeronA.addExclusivePublication(
                "aeron:udp?control-mode=dynamic|control=localhost:10000|" + termUriParameters, streamId);
            Subscription subA = aeronA.addSubscription(
                "aeron:udp?endpoint=localhost:10001|control-mode=dynamic|control=localhost:10000", streamId);
            Subscription subB = aeronB.addSubscription(
                "aeron:udp?endpoint=localhost:10002|control-mode=dynamic|control=localhost:10000", streamId))
        {
            watcher.dataCollector().add(driverA.context().aeronDirectory());
            watcher.dataCollector().add(driverB.context().aeronDirectory());

            Tests.awaitConnected(pub);
            Tests.awaitConnected(subA);
            Tests.awaitConnected(subB);

            final FragmentAssembler handlerA = new FragmentAssembler(
                (buffer, offset, length, header) ->
                {
                    buffer.getBytes(offset, outputA, outputPositionA.get(), length);
                    outputPositionA.addAndGet(length);
                });

            final FragmentAssembler handlerB = new FragmentAssembler(
                (buffer, offset, length, header) ->
                {
                    buffer.getBytes(offset, outputB, outputPositionB.get(), length);
                    outputPositionB.addAndGet(length);
                });

            while (inputPosition < input.length ||
                outputPositionA.get() < outputA.length ||
                outputPositionB.get() < outputB.length)
            {
                if (inputPosition < input.length)
                {
                    final int length = Math.min(input.length - inputPosition, pub.maxMessageLength());
                    sendBuffer.wrap(input, inputPosition, length);
                    if (0 < pub.offer(sendBuffer))
                    {
                        inputPosition += length;
                    }
                }

                if (outputPositionA.get() < outputA.length)
                {
                    subA.poll(handlerA, 10);
                }

                if (outputPositionB.get() < outputB.length)
                {
                    subB.poll(handlerB, 10);
                }
            }

            assertArrayEquals(input, outputA);
            assertArrayEquals(input, outputB);

            final long retransmitCount = aeronA.countersReader()
                .getCounterValue(SystemCounterDescriptor.RETRANSMITS_SENT.id());
            final long nakCount = aeronA.countersReader()
                .getCounterValue(SystemCounterDescriptor.NAK_MESSAGES_SENT.id());
            final long retransmittedBytes = aeronA.countersReader()
                .getCounterValue(SystemCounterDescriptor.RETRANSMITTED_BYTES.id());
            assertThat(nakCount, greaterThanOrEqualTo(1L));
            // in CI, we occasionally see an extra retransmission
            assertThat(retransmitCount, oneOf(1L, 2L));
            // MDC retransmits to each subscriber
            assertThat(retransmittedBytes, greaterThanOrEqualTo(LOSS_LENGTH * 2L));
        }
    }

    @Test
    @Disabled
    void shouldIncludeRetransmittedBytesInTotalBytesSent()
    {
        final int lossLength = 1024 * 1024 - 512;

        TestMediaDriver.enableFixedLoss(context, 0, 512, lossLength);

        launch(context);

        sendAndReceive(
            "aeron:udp?endpoint=localhost:10000|term-length=1m|init-term-id=0|term-id=0|term-offset=0",
            1024 * 1024);

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            final long retransmittedBytes = aeron.countersReader()
                .getCounterValue(SystemCounterDescriptor.RETRANSMITTED_BYTES.id());
            final long totalBytes = aeron.countersReader()
                .getCounterValue(SystemCounterDescriptor.BYTES_SENT.id());
            assertThat(totalBytes, greaterThanOrEqualTo(512 + 2 * retransmittedBytes));
            assertThat(retransmittedBytes, greaterThanOrEqualTo((long)lossLength));
        }
    }

    private void dontCoalesceNaksOnReceiverByDefault()
    {
        TestMediaDriver.dontCoalesceNaksOnReceiverByDefault(context);
    }

    private StreamNakCounters sendAndReceive(final String channel, final int publicationLength)
    {
        final int streamId = 10000;
        final byte[] input = new byte[publicationLength];
        final byte[] output = new byte[publicationLength];
        final Random r = new Random(1);
        r.nextBytes(input);
        final UnsafeBuffer sendBuffer = new UnsafeBuffer();

        int inputPosition = 0;
        final MutableInteger outputPosition = new MutableInteger(0);

        final StreamNakCounters counters;
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            ExclusivePublication pub = aeron.addExclusivePublication(channel, streamId);
            Subscription sub = aeron.addSubscription(channel, streamId))
        {
            Tests.awaitConnected(pub);
            Tests.awaitConnected(sub);

            final FragmentAssembler handler = new FragmentAssembler(
                (buffer, offset, length, header) ->
                {
                    buffer.getBytes(offset, output, outputPosition.get(), length);
                    outputPosition.addAndGet(length);
                });

            while (inputPosition < input.length || outputPosition.get() < output.length)
            {
                if (inputPosition < input.length)
                {
                    final int length = Math.min(input.length - inputPosition, pub.maxMessageLength());
                    sendBuffer.wrap(input, inputPosition, length);
                    if (0 < pub.offer(sendBuffer))
                    {
                        inputPosition += length;
                    }
                }

                if (outputPosition.get() < output.length)
                {
                    sub.poll(handler, 10);
                }
            }

            final CountersReader countersReader = aeron.countersReader();
            final int receiverNaksSentCounterId = countersReader.findByTypeIdAndRegistrationId(
                ReceiverNaksSent.TYPE_ID, sub.imageAtIndex(0).correlationId());
            assertNotEquals(CountersReader.NULL_COUNTER_ID, receiverNaksSentCounterId);
            final int senderNaksReceivedCounterId =
                countersReader.findByTypeIdAndRegistrationId(SenderNaksReceived.TYPE_ID, pub.registrationId());
            assertNotEquals(CountersReader.NULL_COUNTER_ID, senderNaksReceivedCounterId);

            counters = new StreamNakCounters(
                countersReader.getCounterValue(receiverNaksSentCounterId),
                countersReader.getCounterValue(senderNaksReceivedCounterId));
            assertThat(counters.naksSent, greaterThanOrEqualTo(1L));
            assertThat(counters.naksReceived, greaterThanOrEqualTo(1L));
        }

        assertArrayEquals(input, output);

        return counters;
    }

    private record StreamNakCounters(long naksSent, long naksReceived)
    {
    }
}
