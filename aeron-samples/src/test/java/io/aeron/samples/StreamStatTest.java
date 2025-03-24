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
package io.aeron.samples;

import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.status.PublisherLimit;
import io.aeron.driver.status.PublisherPos;
import io.aeron.driver.status.ReceiverHwm;
import io.aeron.driver.status.ReceiverPos;
import io.aeron.driver.status.SenderLimit;
import io.aeron.driver.status.SenderPos;
import io.aeron.driver.status.StreamCounter;
import io.aeron.driver.status.SubscriberPos;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(InterruptingTestCallback.class)
class StreamStatTest
{
    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();
    private TestMediaDriver driver;
    private Aeron aeron;

    @BeforeEach
    void setup(@TempDir final Path tempDir)
    {
        driver = TestMediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(tempDir.resolve("aeron").toString()), testWatcher);
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));

        testWatcher.dataCollector().add(driver.context().aeronDirectory());
    }

    @AfterEach
    void teardown()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    @Test
    @SuppressWarnings("MethodLength")
    @InterruptAfter(10)
    void shouldCollectStreamStats()
    {
        final ExclusivePublication pub1 = aeron.addExclusivePublication(
            "aeron:ipc?alias=test-should-collect-stats|term-length=65536|tether=true|reliable=true|" +
            "ssc=true|sparse=false|linger=60s|mtu=4096",
            2000);
        final ConcurrentPublication pub2 =
            aeron.addPublication("aeron:udp?session-id=" + (pub1.sessionId() - 333) +
            "|endpoint=localhost:5555|mtu=2k|term-length=128K",
            909);
        final ConcurrentPublication pub3 = aeron.addPublication(pub2.channel(), pub2.streamId());
        final ExclusivePublication pub4 = aeron.addExclusivePublication(
            "aeron:udp?endpoint=localhost:7777|interface=127.0.0.1|init-term-id=-6548658|term-id=42|" +
            "term-offset=1024|mtu=8192|ttl=5|group=true|term-length=256K|so-rcvbuf=128K|so-sndbuf=128K|rcv-wnd=128K|" +
            "sparse=true|linger=0|nak-delay=100ms|tether=false|reliable=false|ssc=false|" +
            Tests.generateStringWithSuffix("alias=", "x", 300),
            500);
        final ExclusivePublication pub5 = aeron.addExclusivePublication(
            "aeron:udp?session-id=" + pub2.sessionId() +
            "|alias= @test @not @split @channel @uri @by @at @even @if @no @subscription @exists|" +
            "endpoint=localhost:6666|interface=127.0.0.1|term-length=512K|mtu=1376", 1);
        assertEquals(pub1.sessionId() + 1, pub4.sessionId());
        assertEquals(pub1.sessionId() - 333, pub2.sessionId());
        assertEquals(pub2.sessionId(), pub3.sessionId());
        assertNotSame(pub2, pub3);
        assertEquals(pub2.sessionId(), pub5.sessionId());

        final Subscription sub1 = aeron.addSubscription(pub1.channel(), pub1.streamId());
        final Subscription sub2 = aeron.addSubscription(pub2.channel(), pub2.streamId());
        final Subscription sub4 = aeron.addSubscription(pub4.channel(), pub4.streamId());

        Tests.awaitConnected(pub1);
        Tests.awaitConnected(pub2);
        Tests.awaitConnected(pub3);
        Tests.awaitConnected(pub4);
        Tests.awaitConnected(sub1);
        Tests.awaitConnected(sub2);
        Tests.awaitConnected(sub4);

        final int sub2RcvPosCounterId = findCounterIdByStream(
            aeron.countersReader(), pub2.streamId());
        final int sub4RcvPosCounterId = findCounterIdByStream(
            aeron.countersReader(), pub4.streamId());

        final UnsafeBuffer msg = new UnsafeBuffer(new byte[128]);
        ThreadLocalRandom.current().nextBytes(msg.byteArray());
        for (int i = 0; i < 5; i++)
        {
            while ((pub1.offer(msg) < 0))
            {
                Tests.yield();
            }
            while ((pub2.offer(msg, 0, 64) < 0))
            {
                Tests.yield();
            }
            while ((pub3.offer(msg, 0, 80) < 0))
            {
                Tests.yield();
            }
            while ((pub4.offer(msg, 105, 13) < 0))
            {
                Tests.yield();
            }
        }

        final Subscription sub5 = aeron.addSubscription(pub4.channel(), pub4.streamId());
        assertNotSame(sub4, sub5);
        Tests.awaitConnected(sub5);
        final CountingFragmentHandler sub1Handler = new CountingFragmentHandler(sub1);
        final CountingFragmentHandler sub2Handler = new CountingFragmentHandler(sub2);
        final CountingFragmentHandler sub4Handler = new CountingFragmentHandler(sub4);

        sub1Handler.pollUntil(3);
        sub2Handler.pollUntil(7);
        sub4Handler.pollUntil(5);

        while (1120 != aeron.countersReader().getCounterValue(sub2RcvPosCounterId))
        {
            Tests.yield();
        }

        while (1716702414144L != aeron.countersReader().getCounterValue(sub4RcvPosCounterId))
        {
            Tests.yield();
        }

        final StreamStat streamStat = new StreamStat(aeron.countersReader());
        final Map<StreamStat.StreamCompositeKey, List<StreamStat.StreamPosition>> snapshot = streamStat.snapshot();
        assertInstanceOf(SortedMap.class, snapshot);
        final List<Map.Entry<StreamStat.StreamCompositeKey, List<StreamStat.StreamPosition>>> entries =
            new ArrayList<>(snapshot.entrySet());

        Map.Entry<StreamStat.StreamCompositeKey, List<StreamStat.StreamPosition>> entry = entries.get(0);
        assertStreamInfo(
            entry.getKey(),
            pub5.sessionId(),
            pub5.streamId(),
            pub5.channel().substring(0, StreamCounter.MAX_CHANNEL_LENGTH),
            pub5.channel());
        assertEquals(4, entry.getValue().size());
        assertStreamPosition(entry.getValue().get(0), PublisherPos.PUBLISHER_POS_TYPE_ID, 0);
        assertStreamPosition(entry.getValue().get(1), PublisherLimit.PUBLISHER_LIMIT_TYPE_ID, 0);
        assertStreamPosition(entry.getValue().get(2), SenderPos.SENDER_POSITION_TYPE_ID, 0);
        assertStreamPosition(entry.getValue().get(3), SenderLimit.SENDER_LIMIT_TYPE_ID, 0);

        entry = entries.get(1);
        assertStreamInfo(entry.getKey(), pub2.sessionId(), pub2.streamId(), pub2.channel(), pub2.channel());
        assertEquals(7, entry.getValue().size());
        assertStreamPosition(entry.getValue().get(0), PublisherPos.PUBLISHER_POS_TYPE_ID, 1120);
        assertStreamPosition(entry.getValue().get(1), PublisherLimit.PUBLISHER_LIMIT_TYPE_ID, 66656);
        assertStreamPosition(entry.getValue().get(2), SenderPos.SENDER_POSITION_TYPE_ID, 1120);
        assertStreamPosition(entry.getValue().get(3), SenderLimit.SENDER_LIMIT_TYPE_ID, 65536);
        assertStreamPosition(entry.getValue().get(4), SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID, 448);
        assertStreamPosition(entry.getValue().get(5), ReceiverHwm.RECEIVER_HWM_TYPE_ID, 1120);
        assertStreamPosition(entry.getValue().get(6), ReceiverPos.RECEIVER_POS_TYPE_ID, 1120);

        entry = entries.get(2);
        assertStreamInfo(
            entry.getKey(),
            pub1.sessionId(),
            pub1.streamId(),
            pub1.channel().substring(0, StreamCounter.MAX_CHANNEL_LENGTH),
            pub1.channel());
        assertEquals(3, entry.getValue().size());
        assertStreamPosition(entry.getValue().get(0), PublisherPos.PUBLISHER_POS_TYPE_ID, 800);
        assertStreamPosition(entry.getValue().get(1), PublisherLimit.PUBLISHER_LIMIT_TYPE_ID, 32768);
        assertStreamPosition(entry.getValue().get(2), SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID, 320);

        entry = entries.get(3);
        final int pub4LimitCounterId = aeron.countersReader()
            .findByTypeIdAndRegistrationId(PublisherLimit.PUBLISHER_LIMIT_TYPE_ID, pub4.registrationId());
        assertNotEquals(CountersReader.NULL_COUNTER_ID, pub4LimitCounterId);
        final String pub4LimitLabel = aeron.countersReader().getCounterLabel(pub4LimitCounterId);
        assertStreamInfo(
            entry.getKey(),
            pub4.sessionId(),
            pub4.streamId(),
            pub4.channel().substring(0, StreamCounter.MAX_CHANNEL_LENGTH),
            pub4LimitLabel.substring(pub4LimitLabel.indexOf("aeron:")));
        assertEquals(8, entry.getValue().size());
        assertStreamPosition(entry.getValue().get(0), PublisherPos.PUBLISHER_POS_TYPE_ID, 1716702414144L);
        assertStreamPosition(entry.getValue().get(1), PublisherLimit.PUBLISHER_LIMIT_TYPE_ID, 1716702545216L);
        assertStreamPosition(entry.getValue().get(2), SenderPos.SENDER_POSITION_TYPE_ID, 1716702414144L);
        assertStreamPosition(entry.getValue().get(3), SenderLimit.SENDER_LIMIT_TYPE_ID, 1716702544896L);
        assertStreamPosition(entry.getValue().get(4), SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID, 1716702414016L);
        assertStreamPosition(entry.getValue().get(5), ReceiverHwm.RECEIVER_HWM_TYPE_ID, 1716702414144L);
        assertStreamPosition(entry.getValue().get(6), ReceiverPos.RECEIVER_POS_TYPE_ID, 1716702414144L);
        assertStreamPosition(entry.getValue().get(7), SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID, 1716702413824L);
    }

    private int findCounterIdByStream(final CountersReader countersReader, final int streamId)
    {
        final MutableInteger counterId = new MutableInteger(-1);

        countersReader.forEach(
            (counterId1, typeId1, keyBuffer, label) ->
            {
                final int counterStreamId = keyBuffer.getInt(StreamCounter.STREAM_ID_OFFSET);
                if (ReceiverPos.RECEIVER_POS_TYPE_ID == typeId1 && streamId == counterStreamId)
                {
                    assertEquals(-1, counterId.intValue(), () -> "multiple rcv-pos found for streamId=" + streamId);
                    counterId.set(counterId1);
                }
            });

        assertNotEquals(-1, counterId.intValue());

        return counterId.intValue();
    }

    private static void assertStreamInfo(
        final StreamStat.StreamCompositeKey key,
        final int expectedSessionId,
        final int expectedStreamId,
        final String expectedChannel,
        final String expectedFullChannel)
    {
        assertEquals(expectedSessionId, key.sessionId());
        assertEquals(expectedStreamId, key.streamId());
        assertEquals(expectedChannel, key.channel());
        assertEquals(expectedFullChannel, key.fullChannel);
    }

    private static void assertStreamPosition(
        final StreamStat.StreamPosition position,
        final int expectedTypeId,
        final long expectedValue)
    {
        assertEquals(expectedTypeId, position.typeId());
        assertThat(position.value(), greaterThanOrEqualTo(expectedValue));
    }

    private static final class CountingFragmentHandler implements FragmentHandler
    {
        private final Subscription subscription;
        private int count;

        private CountingFragmentHandler(final Subscription subscription)
        {

            this.subscription = subscription;
        }

        public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            count++;
        }

        public void pollUntil(final int expectedCount)
        {
            while (count < expectedCount)
            {
                final int poll = subscription.poll(this, 1);
                if (poll <= 0)
                {
                    Tests.yield();
                }
                else
                {
                    count += poll;
                }
            }
        }
    }
}
