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

import io.aeron.ChannelUri;
import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.reports.LossReport;
import io.aeron.driver.status.ReceiverHwm;
import io.aeron.driver.status.ReceiverPos;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.Position;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PublicationImageTest
{
    private static final int TERM_LENGTH = 64 * 1024;
    private static final int INITIAL_WINDOW_LENGTH = 128 * 1024;
    private static final int MAX_WINDOW_LENGHT = 1024 * 1024;
    private static final long CORRELATION_ID = 42;
    private static final int TRANSPORT_INDEX = 3;
    private static final int SESSION_ID = 888;
    private static final int STREAM_ID = 101010;
    private static final int INITIAL_TERM_ID = -444666;
    private static final int ACTIVE_TERM_ID = INITIAL_TERM_ID + 111;
    private static final int TERM_OFFSET = TERM_LENGTH - TERM_LENGTH / 4;
    private static final short FLAGS = FrameDescriptor.UNFRAGMENTED;
    private static final String SOURCE_IDENTITY = "aeron:udp?endpoint=localhost:5555";
    private final MediaDriver.Context ctx = new MediaDriver.Context();
    private final ReceiveChannelEndpoint receiveChannelEndpoint = mock(ReceiveChannelEndpoint.class);
    private final InetSocketAddress controlAddress = mock(InetSocketAddress.class);
    private final RawLog rawLog = mock(RawLog.class);
    private final FeedbackDelayGenerator feedbackDelayGenerator = mock(FeedbackDelayGenerator.class);
    private final CongestionControl congestionControl = mock(CongestionControl.class);
    private final CachedEpochClock epochClock = new CachedEpochClock();
    private final CachedNanoClock nanoClock = new CachedNanoClock();
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
    private final CountersManager countersManager = new CountersManager(
        new UnsafeBuffer(ByteBuffer.allocateDirect(256 * 1024)),
        new UnsafeBuffer(ByteBuffer.allocateDirect(64 * 1024)),
        StandardCharsets.US_ASCII);
    private final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
    private final LossReport lossReport = mock(LossReport.class);
    private Position hwmPosition;
    private Position rcvPosition;
    private PublicationImage image;

    @BeforeEach
    void before()
    {
        epochClock.update(TimeUnit.HOURS.toMillis(1));
        nanoClock.update(TimeUnit.HOURS.toNanos(1));
        ctx
            .receiverCachedNanoClock(nanoClock)
            .nanoClock(nanoClock)
            .epochClock(epochClock)
            .imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(10))
            .untetheredWindowLimitTimeoutNs(TimeUnit.SECONDS.toNanos(1))
            .untetheredRestingTimeoutNs(TimeUnit.SECONDS.toNanos(1))
            .statusMessageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(150))
            .systemCounters(new SystemCounters(countersManager))
            .lossReport(lossReport);

        final String channel = "aeron:udp?endpoint=localhost:5555";
        final ChannelUri channelUri = ChannelUri.parse(channel);
        final UdpChannel udpChannel = mock(UdpChannel.class);
        when(udpChannel.channelUri()).thenReturn(channelUri);
        when(receiveChannelEndpoint.subscriptionUdpChannel()).thenReturn(udpChannel);
        when(receiveChannelEndpoint.originalUriString()).thenReturn(channel);

        final SubscriptionLink subscriptionLink1 = mock(SubscriptionLink.class);
        when(subscriptionLink1.isReliable()).thenReturn(true);
        when(subscriptionLink1.isTether()).thenReturn(true);
        final SubscriberPosition subscriberPosition1 = mock(SubscriberPosition.class);
        when(subscriberPosition1.subscription()).thenReturn(subscriptionLink1);
        final SubscriptionLink subscriptionLink2 = mock(SubscriptionLink.class);
        when(subscriptionLink1.isReliable()).thenReturn(false);
        when(subscriptionLink1.isTether()).thenReturn(false);
        final SubscriberPosition subscriberPosition2 = mock(SubscriberPosition.class);
        when(subscriberPosition2.subscription()).thenReturn(subscriptionLink2);
        final ArrayList<SubscriberPosition> subscriberPositions = new ArrayList<>();
        subscriberPositions.add(subscriberPosition1);
        subscriberPositions.add(subscriberPosition2);

        final UnsafeBuffer[] termBuffers = new UnsafeBuffer[PARTITION_COUNT];
        for (int i = 0; i < termBuffers.length; i++)
        {
            termBuffers[i] = new UnsafeBuffer(new byte[TERM_LENGTH]);
        }
        when(rawLog.termBuffers()).thenReturn(termBuffers);
        when(rawLog.metaData()).thenReturn(new UnsafeBuffer(new byte[LOG_META_DATA_LENGTH]));
        when(rawLog.termLength()).thenReturn(TERM_LENGTH);

        when(congestionControl.initialWindowLength()).thenReturn(INITIAL_WINDOW_LENGTH);
        when(congestionControl.maxWindowLength()).thenReturn(MAX_WINDOW_LENGHT);

        final long registrationId = 73249234983274L;
        final ExpandableArrayBuffer tempBuffer = new ExpandableArrayBuffer();
        hwmPosition = ReceiverHwm.allocate(tempBuffer, countersManager, registrationId, SESSION_ID, STREAM_ID, channel);
        rcvPosition = ReceiverPos.allocate(
            tempBuffer, countersManager, registrationId, SESSION_ID, STREAM_ID, channel);

        image = new PublicationImage(
            CORRELATION_ID,
            ctx,
            receiveChannelEndpoint,
            TRANSPORT_INDEX,
            controlAddress,
            SESSION_ID,
            STREAM_ID,
            INITIAL_TERM_ID,
            ACTIVE_TERM_ID,
            TERM_OFFSET,
            FLAGS,
            rawLog,
            feedbackDelayGenerator,
            subscriberPositions,
            hwmPosition,
            rcvPosition,
            SOURCE_IDENTITY,
            congestionControl);

        final long position = computePosition(
            ACTIVE_TERM_ID, TERM_OFFSET, positionBitsToShift(TERM_LENGTH), INITIAL_TERM_ID);
        assertEquals(position, hwmPosition.get());
        assertEquals(position, rcvPosition.get());

        ThreadLocalRandom.current().nextBytes(buffer.byteArray());
    }

    @Test
    void shouldAdvanceHighWaterMarkByPacketLengthWhenItContainsPaddingFrame()
    {
        final int totalLength = 512;
        final int packetLength = 288;
        final int termId = ACTIVE_TERM_ID;
        final int termOffset = TERM_LENGTH - totalLength;
        int offset = 0;
        offset += writeFrame(offset, termOffset, termId, 65, BEGIN_AND_END_FLAGS, HDR_TYPE_DATA, 65);
        offset += writeFrame(offset, termOffset + offset, termId, 96, BEGIN_AND_END_FLAGS, HDR_TYPE_DATA, 96);
        offset += writeFrame(offset, termOffset + offset, termId, 224, BEGIN_AND_END_FLAGS, HDR_TYPE_PAD, 0x888AA888);
        assertEquals(totalLength, offset);
        final InetSocketAddress srcAddress = mock(InetSocketAddress.class);

        final int bytes = image.insertPacket(termId, termOffset, buffer, packetLength, TRANSPORT_INDEX, srcAddress);

        assertEquals(packetLength, bytes);
        final int positionBitsToShift = positionBitsToShift(TERM_LENGTH);
        final long packetPosition = computePosition(termId, termOffset, positionBitsToShift, INITIAL_TERM_ID);
        assertEquals(packetPosition + packetLength, hwmPosition.get());
        final UnsafeBuffer activeTermBuffer =
            rawLog.termBuffers()[indexByPosition(packetPosition, positionBitsToShift)];
        for (int i = 0; i < packetLength; i++)
        {
            assertEquals(buffer.getByte(i), activeTermBuffer.getByte(termOffset + i));
        }
        for (int i = packetLength; i < totalLength; i++)
        {
            assertEquals(0, activeTermBuffer.getByte(termOffset + i));
        }
    }

    @Test
    void shouldAdvanceHighWaterMarkPositionOnHeartbeat()
    {
        final int termId = ACTIVE_TERM_ID;
        final int termOffset = TERM_OFFSET + 1024;
        writeFrame(0, termOffset, termId, 0, BEGIN_AND_END_FLAGS, HDR_TYPE_DATA, -1);
        FrameDescriptor.frameLengthOrdered(buffer, 0, 0);
        final InetSocketAddress srcAddress = mock(InetSocketAddress.class);
        final int packetLength = HEADER_LENGTH;
        final AtomicCounter heartBeatsCounter = ctx.systemCounters().get(SystemCounterDescriptor.HEARTBEATS_RECEIVED);
        final long oldHeartBeatCount = heartBeatsCounter.getWeak();

        final int bytes = image.insertPacket(termId, termOffset, buffer, packetLength, TRANSPORT_INDEX, srcAddress);

        assertEquals(packetLength, bytes);
        final int positionBitsToShift = positionBitsToShift(TERM_LENGTH);
        final long packetPosition = computePosition(termId, termOffset, positionBitsToShift, INITIAL_TERM_ID);
        assertEquals(packetPosition, hwmPosition.get());
        assertEquals(oldHeartBeatCount + 1, heartBeatsCounter.getWeak());
        final UnsafeBuffer activeTermBuffer =
            rawLog.termBuffers()[indexByPosition(packetPosition, positionBitsToShift)];
        for (int i = 0; i < packetLength; i++)
        {
            assertEquals(0, activeTermBuffer.getByte(termOffset + i));
        }
    }

    @Test
    void shouldOnlyRecordUniqueLoss()
    {
        final LossReport.ReportEntry reportEntry = mock(LossReport.ReportEntry.class);
        when(lossReport.createEntry(anyLong(), anyLong(), anyInt(), anyInt(), anyString(), anyString()))
            .thenReturn(reportEntry);
        final InOrder inOrder = inOrder(lossReport, reportEntry);

        final int termId = 0;
        final int offset = 0;
        final int length = 1024;

        // first activation => must be recorded
        epochClock.update(100);
        image.onGapDetected(termId, offset, length);
        assertEquals(1L, image.beginLossChange);
        assertEquals(termId, image.lossTermId);
        assertEquals(offset, image.lossTermOffset);
        assertEquals(length, image.lossLength);
        assertEquals(1L, image.endLossChange);

        // same loss => no reporting
        epochClock.update(200);
        image.onGapDetected(termId, offset, length);
        assertEquals(2L, image.beginLossChange);
        assertEquals(termId, image.lossTermId);
        assertEquals(offset, image.lossTermOffset);
        assertEquals(length, image.lossLength);
        assertEquals(2L, image.endLossChange);

        // smaller loss => no reporting
        epochClock.update(300);
        image.onGapDetected(termId, offset, 32);
        assertEquals(3L, image.beginLossChange);
        assertEquals(termId, image.lossTermId);
        assertEquals(offset, image.lossTermOffset);
        assertEquals(32, image.lossLength);
        assertEquals(3L, image.endLossChange);

        // loss length increased => record
        epochClock.update(400);
        image.onGapDetected(termId, offset, length + 128);
        assertEquals(4L, image.beginLossChange);
        assertEquals(termId, image.lossTermId);
        assertEquals(offset, image.lossTermOffset);
        assertEquals(length + 128, image.lossLength);
        assertEquals(4L, image.endLossChange);

        // overlapping loss => record
        epochClock.update(500);
        image.onGapDetected(termId, offset + 512, 800);
        assertEquals(5L, image.beginLossChange);
        assertEquals(termId, image.lossTermId);
        assertEquals(offset + 512, image.lossTermOffset);
        assertEquals(800, image.lossLength);
        assertEquals(5L, image.endLossChange);

        // non-overlapping loss => record
        epochClock.update(600);
        image.onGapDetected(termId, offset + 512 + 800, 32);
        assertEquals(6L, image.beginLossChange);
        assertEquals(termId, image.lossTermId);
        assertEquals(offset + 512 + 800, image.lossTermOffset);
        assertEquals(32, image.lossLength);
        assertEquals(6L, image.endLossChange);

        // non-overlapping loss => record
        epochClock.update(700);
        image.onGapDetected(termId, offset + 4096, 2048);
        assertEquals(7L, image.beginLossChange);
        assertEquals(termId, image.lossTermId);
        assertEquals(offset + 4096, image.lossTermOffset);
        assertEquals(2048, image.lossLength);
        assertEquals(7L, image.endLossChange);

        // loss in different term => record
        epochClock.update(800);
        image.onGapDetected(termId + 11, 0, 256);
        assertEquals(8L, image.beginLossChange);
        assertEquals(termId + 11, image.lossTermId);
        assertEquals(0, image.lossTermOffset);
        assertEquals(256, image.lossLength);
        assertEquals(8L, image.endLossChange);

        inOrder.verify(lossReport).createEntry(
            length, 100, SESSION_ID, STREAM_ID, receiveChannelEndpoint.originalUriString(), SOURCE_IDENTITY);
        inOrder.verify(reportEntry).recordObservation(128, 400);
        inOrder.verify(reportEntry).recordObservation(160, 500);
        inOrder.verify(reportEntry).recordObservation(32, 600);
        inOrder.verify(reportEntry).recordObservation(2048, 700);
        inOrder.verify(reportEntry).recordObservation(256, 800);
        inOrder.verifyNoMoreInteractions();
    }

    private int writeFrame(
        final int offset,
        final int termOffset,
        final int termId,
        final int length,
        final short flags,
        final int type,
        final int reservedValue)
    {
        final int frameLength = length + HEADER_LENGTH;
        headerFlyweight.wrap(buffer, offset, frameLength);
        headerFlyweight
            .frameLength(frameLength)
            .version(CURRENT_VERSION)
            .flags(flags)
            .headerType(type);
        headerFlyweight
            .termOffset(termOffset)
            .sessionId(SESSION_ID)
            .streamId(STREAM_ID)
            .termId(termId)
            .reservedValue(reservedValue);

        return BitUtil.align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
    }
}
