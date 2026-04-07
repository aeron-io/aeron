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
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MultiGapLossAndRecoverySystemTest
{
    private static final int TOTAL_GAPS = 100;
    private static final int GAP_LENGTH = 128;
    private static final int GAP_RADIX = 4096;
    private static final int TERM_ID = 0;

    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private final MediaDriver.Context context = new MediaDriver.Context()
        .aeronDirectoryName(CommonContext.generateRandomDirName())
        .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
        .threadingMode(ThreadingMode.SHARED)
        .reliableStream(true);
    private TestMediaDriver driver;

    @BeforeEach
    void setUp()
    {
        TestMediaDriver.enableMultiGapLoss(context, TERM_ID, GAP_RADIX, GAP_LENGTH, TOTAL_GAPS);
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
    void shouldSendStreamOfDataAndHandleMultipleGaps()
    {
        launch(context);

        sendAndReceive(
            "aeron:udp?endpoint=localhost:10000|term-length=1m|init-term-id=0|term-id=0|term-offset=0|nak-delay=50us",
            10 * 1024 * 1024
        );

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            final long retransmitCount = aeron.countersReader()
                .getCounterValue(SystemCounterDescriptor.RETRANSMITS_SENT.id());
            final long nakCount = aeron.countersReader()
                .getCounterValue(SystemCounterDescriptor.NAK_MESSAGES_SENT.id());
            // Prior to the advent of the UnicastRetransmitHandler, we'd end up dropping NAKs when we blew past
            // the max retransmit action limit.  In that case, we'd have to send more than the 100 NAKs required.
            // Now, however, the UnicastRetransmitHandler treats new NAKs as a tacit admission that the previous
            // NAK did its job and the prior gap was filled, so we can immediately handle the new NAK.

            final long gapCount = TOTAL_GAPS;
            final long expectedCountWithBuffer = gapCount * 2;
            assertThat(
                retransmitCount,
                allOf(greaterThanOrEqualTo(gapCount), lessThanOrEqualTo(expectedCountWithBuffer)));
            assertThat(
                nakCount,
                allOf(greaterThanOrEqualTo(gapCount), lessThanOrEqualTo(expectedCountWithBuffer)));
            assertThat(nakCount, lessThanOrEqualTo(expectedCountWithBuffer));
        }
    }

    private void sendAndReceive(final String channel, final int publicationLength)
    {
        final int streamId = 10000;
        final Random r = new Random(4234266349L);
        final MutableLong receiverValue = new MutableLong();

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            ExclusivePublication pub = aeron.addExclusivePublication(channel, streamId);
            Subscription sub = aeron.addSubscription(channel, streamId))
        {
            Tests.awaitConnected(pub);
            Tests.awaitConnected(sub);

            final Image image = sub.imageBySessionId(pub.sessionId());

            final FragmentAssembler handler = new FragmentAssembler(
                (buffer, offset, length, header) -> receiverValue.set(buffer.getLong(offset)));

            final BufferClaim bufferClaim = new BufferClaim();
            while (pub.position() < publicationLength)
            {
                while (pub.tryClaim(pub.maxPayloadLength(), bufferClaim) < 0)
                {
                    Tests.yield();
                }

                final long value = r.nextLong();
                bufferClaim.buffer().putLong(DataHeaderFlyweight.HEADER_LENGTH, value);
                bufferClaim.commit();

                while (0 == image.poll(handler, 1))
                {
                    Tests.yield();
                }
                assertEquals(value, receiverValue.get());
            }
        }
    }
}
