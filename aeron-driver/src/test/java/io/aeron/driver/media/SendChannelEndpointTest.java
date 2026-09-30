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
package io.aeron.driver.media;

import io.aeron.driver.DriverConductorProxy;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.NetworkPublication;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.ErrorFlyweight;
import io.aeron.protocol.NakFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.driver.media.SendChannelEndpoint.DESTINATION_TIMEOUT;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class SendChannelEndpointTest
{
    private final SystemCounters mockSystemCounters = mock(SystemCounters.class);
    private final AtomicCounter mockStatusIndicator = mock(AtomicCounter.class);
    private final DriverConductorProxy mockConductorProxy = mock(DriverConductorProxy.class);
    private final CachedNanoClock nanoClock = new CachedNanoClock();
    private final AtomicCounter[] systemCounters = new AtomicCounter[SystemCounterDescriptor.values().length];
    private final EpochNanoClock sendTimestampClock = mock(EpochNanoClock.class);
    private final DataHeaderFlyweight header = new DataHeaderFlyweight();

    private final MediaDriver.Context context = new MediaDriver.Context()
        .systemCounters(mockSystemCounters)
        .cachedNanoClock(nanoClock)
        .senderCachedNanoClock(nanoClock)
        .receiverCachedNanoClock(nanoClock)
        .channelSendTimestampClock(sendTimestampClock)
        .receiveChannelEndpointThreadLocals(new ReceiveChannelEndpointThreadLocals())
        .senderPortManager(new WildcardPortManager(WildcardPortManager.EMPTY_PORT_RANGE, true))
        .receiverPortManager(new WildcardPortManager(WildcardPortManager.EMPTY_PORT_RANGE, false));

    private SendChannelEndpoint endpoint;

    @BeforeEach
    void setUp()
    {
        for (final SystemCounterDescriptor descriptor : SystemCounterDescriptor.values())
        {
            systemCounters[descriptor.id()] = mock(AtomicCounter.class);
        }
        when(mockSystemCounters.get(any()))
            .thenAnswer(invocation ->
            {
                final SystemCounterDescriptor descriptor = invocation.getArgument(0);
                return systemCounters[descriptor.id()];
            });
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.close(endpoint);
    }

    @Test
    void shouldSkipReResolveForResponseControlMode()
    {
        final UdpChannel responseChannel = UdpChannel.parse(
            "aeron:udp?control-mode=response|control=127.0.0.1:10001|endpoint=127.0.0.1:10002");

        endpoint = new SendChannelEndpoint(responseChannel, mockStatusIndicator, context);

        endpoint.checkForReResolution(DESTINATION_TIMEOUT * 2, mockConductorProxy);

        verify(mockConductorProxy, never()).reResolveEndpoint(
            anyString(), any(SendChannelEndpoint.class), nullable(InetSocketAddress.class));
    }

    @Test
    void shouldReResolveExplicitEndpointAfterTimeout()
    {
        final UdpChannel channel = UdpChannel.parse("aeron:udp?endpoint=127.0.0.1:10002");

        endpoint = new SendChannelEndpoint(channel, mockStatusIndicator, context);

        endpoint.checkForReResolution(DESTINATION_TIMEOUT * 2, mockConductorProxy);

        verify(mockConductorProxy, times(1)).reResolveEndpoint(
            anyString(), any(SendChannelEndpoint.class), nullable(InetSocketAddress.class));
    }

    @Test
    void shouldRejectInvalidErrorFrame()
    {
        endpoint = new SendChannelEndpoint(
            UdpChannel.parse("aeron:udp?endpoint=localhost:5555"), mockStatusIndicator, context);

        final ErrorFlyweight flyweight = new ErrorFlyweight(new UnsafeBuffer(new byte[ErrorFlyweight.HEADER_LENGTH]));

        endpoint.onError(
            flyweight, flyweight, ErrorFlyweight.HEADER_LENGTH, mock(InetSocketAddress.class), mockConductorProxy);

        final AtomicCounter invalidPackets = mockSystemCounters.get(SystemCounterDescriptor.INVALID_PACKETS);
        final AtomicCounter errorFramesReceived = mockSystemCounters.get(SystemCounterDescriptor.ERROR_FRAMES_RECEIVED);
        verify(invalidPackets).increment();
        verifyNoMoreInteractions(invalidPackets);
        verifyNoInteractions(errorFramesReceived);
    }

    @Test
    void shouldRejectInvalidNakFrame()
    {
        endpoint = new SendChannelEndpoint(
            UdpChannel.parse("aeron:udp?endpoint=localhost:5555"), mockStatusIndicator, context);
        final NetworkPublication publication = mock(NetworkPublication.class);
        endpoint.registerForSend(publication);

        final NakFlyweight flyweight = new NakFlyweight(new UnsafeBuffer(new byte[NakFlyweight.HEADER_LENGTH]));
        flyweight.termOffset(-1);

        endpoint.onNakMessage(
            flyweight, flyweight, NakFlyweight.HEADER_LENGTH, new InetSocketAddress("192.168.0.10", 2048));

        final AtomicCounter invalidPackets = mockSystemCounters.get(SystemCounterDescriptor.INVALID_PACKETS);
        final AtomicCounter nakFramesReceived = mockSystemCounters.get(SystemCounterDescriptor.NAK_MESSAGES_RECEIVED);
        final InOrder inOrder = inOrder(invalidPackets, nakFramesReceived, publication);
        inOrder.verify(publication).termBufferLength();
        inOrder.verify(invalidPackets).increment();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldRejectInvalidStatusMessageFrame()
    {
        endpoint = new SendChannelEndpoint(
            UdpChannel.parse("aeron:udp?endpoint=localhost:5555"), mockStatusIndicator, context);
        final NetworkPublication publication = mock(NetworkPublication.class);
        final int termLength = 128 * 1024;
        when(publication.termBufferLength()).thenReturn(termLength);
        endpoint.registerForSend(publication);

        final StatusMessageFlyweight flyweight =
            new StatusMessageFlyweight(new UnsafeBuffer(new byte[StatusMessageFlyweight.HEADER_LENGTH]));
        flyweight.consumptionTermOffset(termLength + 1).receiverWindowLength(1024);

        endpoint.onStatusMessage(
            flyweight,
            flyweight,
            StatusMessageFlyweight.HEADER_LENGTH,
            mock(InetSocketAddress.class),
            mockConductorProxy);

        final AtomicCounter invalidPackets = mockSystemCounters.get(SystemCounterDescriptor.INVALID_PACKETS);
        final AtomicCounter smReceived = mockSystemCounters.get(SystemCounterDescriptor.STATUS_MESSAGES_RECEIVED);
        final InOrder inOrder = inOrder(invalidPackets, smReceived, publication);
        inOrder.verify(publication).termBufferLength();
        inOrder.verify(invalidPackets).increment();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldRejectInvalidStatusMessageFrameEvenWhenThereIsNoPublication()
    {
        endpoint = new SendChannelEndpoint(
            UdpChannel.parse("aeron:udp?endpoint=localhost:5555"), mockStatusIndicator, context);

        final StatusMessageFlyweight flyweight =
            new StatusMessageFlyweight(new UnsafeBuffer(new byte[StatusMessageFlyweight.HEADER_LENGTH]));
        flyweight.consumptionTermOffset(0).receiverWindowLength(LogBufferDescriptor.TERM_MAX_LENGTH);

        endpoint.onStatusMessage(
            flyweight,
            flyweight,
            StatusMessageFlyweight.HEADER_LENGTH,
            mock(InetSocketAddress.class),
            mockConductorProxy);

        final AtomicCounter invalidPackets = mockSystemCounters.get(SystemCounterDescriptor.INVALID_PACKETS);
        final AtomicCounter smReceived = mockSystemCounters.get(SystemCounterDescriptor.STATUS_MESSAGES_RECEIVED);
        final InOrder inOrder = inOrder(invalidPackets, smReceived);
        inOrder.verify(invalidPackets).increment();
        inOrder.verifyNoMoreInteractions();
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, DataHeaderFlyweight.HEADER_LENGTH - 1 })
    void applySendChannelTimestampIsANoOpIfPacketIsShorterThanDataFrame(final int packetLength)
    {
        endpoint = new SendChannelEndpoint(
            UdpChannel.parse("aeron:udp?endpoint=localhost:5555|channel-snd-ts-offset=0"),
            mockStatusIndicator,
            context);
        final ByteBuffer buffer = ByteBuffer.allocate(10 + packetLength);
        buffer.position(10);
        assertEquals(packetLength, buffer.remaining());

        endpoint.applyChannelSendTimestamp(buffer);

        verifyNoInteractions(sendTimestampClock);
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 0 })
    void applySendChannelTimestampIsANoOpIfFirstFrameIsEmpty(final int firstFrameLength)
    {
        endpoint = new SendChannelEndpoint(
            UdpChannel.parse("aeron:udp?endpoint=localhost:5555|channel-snd-ts-offset=16"),
            mockStatusIndicator,
            context);
        final ByteBuffer buffer = ByteBuffer.allocate(DataHeaderFlyweight.HEADER_LENGTH);
        assertEquals(DataHeaderFlyweight.HEADER_LENGTH, buffer.remaining());
        header.wrap(buffer);
        header.frameLength(firstFrameLength);

        endpoint.applyChannelSendTimestamp(buffer);

        verifyNoInteractions(sendTimestampClock);
    }

    @Test
    @SuppressWarnings("MethodLength")
    void applySendChannelTimestampsToAllBeginDataFramesWithinAPacketWhenOffsetIsWithinMessageBody()
    {
        endpoint = new SendChannelEndpoint(
            UdpChannel.parse("aeron:udp?endpoint=localhost:5555|channel-snd-ts-offset=16"),
            mockStatusIndicator,
            context);
        final ByteBuffer buffer = ByteBuffer.allocate(1024);
        final int position = 64;
        buffer.position(position);
        assertEquals(960, buffer.remaining());
        final long sendTimestampNs = 47239472340932L;
        when(sendTimestampClock.nanoTime()).thenReturn(sendTimestampNs).thenThrow(new RuntimeException("second call"));

        // empty data frame
        header.wrap(buffer, position, 32);
        header
            .frameLength(32)
            .version(DataHeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(DataHeaderFlyweight.HDR_TYPE_DATA);

        // padding frame
        header.wrap(buffer, position + 32, 32);
        header
            .frameLength(128)
            .version(DataHeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(DataHeaderFlyweight.HDR_TYPE_PAD);

        // start of the fragmented frame
        header.wrap(buffer, position + 160, 32);
        header
            .frameLength(56)
            .version(DataHeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_FLAG)
            .headerType(DataHeaderFlyweight.HDR_TYPE_DATA);

        // continues
        header.wrap(buffer, position + 224, 32);
        header
            .frameLength(100)
            .version(DataHeaderFlyweight.CURRENT_VERSION)
            .flags((short)0)
            .headerType(DataHeaderFlyweight.HDR_TYPE_DATA);

        // continues
        header.wrap(buffer, position + 352, 32);
        header
            .frameLength(100)
            .version(DataHeaderFlyweight.CURRENT_VERSION)
            .flags((short)0)
            .headerType(DataHeaderFlyweight.HDR_TYPE_DATA);

        // end of fragmented frame
        header.wrap(buffer, position + 480, 32);
        header
            .frameLength(70)
            .version(DataHeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.END_FLAG)
            .headerType(DataHeaderFlyweight.HDR_TYPE_DATA);

        // non-fragmented message
        header.wrap(buffer, position + 576, 32);
        header
            .frameLength(100)
            .version(DataHeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(DataHeaderFlyweight.HDR_TYPE_DATA);

        // padding frame
        header.wrap(buffer, position + 704, 32);
        header
            .frameLength(4096)
            .version(DataHeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(DataHeaderFlyweight.HDR_TYPE_PAD);

        endpoint.applyChannelSendTimestamp(buffer);

        final int tsOffset = DataHeaderFlyweight.DATA_OFFSET + 16;

        // padding frame is skipped
        header.wrap(buffer, position + 32, tsOffset + SIZE_OF_LONG);
        assertEquals(0, header.getLong(tsOffset, LITTLE_ENDIAN));

        // fragment message start
        header.wrap(buffer, position + 160, tsOffset + SIZE_OF_LONG);
        assertEquals(sendTimestampNs, header.getLong(tsOffset, LITTLE_ENDIAN));

        header.wrap(buffer, position + 224, tsOffset + SIZE_OF_LONG);
        assertEquals(0, header.getLong(tsOffset, LITTLE_ENDIAN));

        header.wrap(buffer, position + 352, tsOffset + SIZE_OF_LONG);
        assertEquals(0, header.getLong(tsOffset, LITTLE_ENDIAN));

        // fragment message end
        header.wrap(buffer, position + 480, tsOffset + SIZE_OF_LONG);
        assertEquals(0, header.getLong(tsOffset, LITTLE_ENDIAN));

        // non-fragmented message
        header.wrap(buffer, position + 576, tsOffset + SIZE_OF_LONG);
        assertEquals(sendTimestampNs, header.getLong(tsOffset, LITTLE_ENDIAN));

        // last padding frame
        header.wrap(buffer, position + 704, tsOffset + SIZE_OF_LONG);
        assertEquals(0, header.getLong(tsOffset, LITTLE_ENDIAN));

        verify(sendTimestampClock).nanoTime();
        verifyNoMoreInteractions(sendTimestampClock);
    }

    @Test
    @SuppressWarnings("MethodLength")
    void applySendChannelTimestampsToAllBeginDataFramesWithinAPacketWhenUsingReservedValue()
    {
        endpoint = new SendChannelEndpoint(
            UdpChannel.parse("aeron:udp?endpoint=localhost:5555|channel-snd-ts-offset=reserved"),
            mockStatusIndicator,
            context);
        final ByteBuffer buffer = ByteBuffer.allocate(1024);
        final int position = 64;
        buffer.position(position);
        assertEquals(960, buffer.remaining());
        final long sendTimestampNs = 12391236812638L;
        when(sendTimestampClock.nanoTime()).thenReturn(sendTimestampNs).thenThrow(new RuntimeException("second call"));

        // empty data frame
        header.wrap(buffer, position, 32);
        header
            .frameLength(32)
            .version(DataHeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(DataHeaderFlyweight.HDR_TYPE_DATA);

        // padding frame
        header.wrap(buffer, position + 32, 32);
        header
            .frameLength(128)
            .version(DataHeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(DataHeaderFlyweight.HDR_TYPE_PAD);

        // start of the fragmented frame
        header.wrap(buffer, position + 160, 32);
        header
            .frameLength(56)
            .version(DataHeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_FLAG)
            .headerType(DataHeaderFlyweight.HDR_TYPE_DATA);

        // continues
        header.wrap(buffer, position + 224, 32);
        header
            .frameLength(100)
            .version(DataHeaderFlyweight.CURRENT_VERSION)
            .flags((short)0)
            .headerType(DataHeaderFlyweight.HDR_TYPE_DATA);

        // continues
        header.wrap(buffer, position + 352, 32);
        header
            .frameLength(100)
            .version(DataHeaderFlyweight.CURRENT_VERSION)
            .flags((short)0)
            .headerType(DataHeaderFlyweight.HDR_TYPE_DATA);

        // end of fragmented frame
        header.wrap(buffer, position + 480, 32);
        header
            .frameLength(70)
            .version(DataHeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.END_FLAG)
            .headerType(DataHeaderFlyweight.HDR_TYPE_DATA);

        // non-fragmented message
        header.wrap(buffer, position + 576, 32);
        header
            .frameLength(100)
            .version(DataHeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(DataHeaderFlyweight.HDR_TYPE_DATA);

        // padding frame
        header.wrap(buffer, position + 704, 32);
        header
            .frameLength(4096)
            .version(DataHeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(DataHeaderFlyweight.HDR_TYPE_PAD);

        endpoint.applyChannelSendTimestamp(buffer);

        // empty data frame
        header.wrap(buffer, position, 32);
        assertEquals(sendTimestampNs, header.reservedValue());

        // padding frame is skipped
        header.wrap(buffer, position + 32, 32);
        assertEquals(0, header.reservedValue());

        // fragment message start
        header.wrap(buffer, position + 160, 32);
        assertEquals(sendTimestampNs, header.reservedValue());

        header.wrap(buffer, position + 224, 32);
        assertEquals(0, header.reservedValue());

        header.wrap(buffer, position + 352, 32);
        assertEquals(0, header.reservedValue());

        // fragment message end
        header.wrap(buffer, position + 480, 32);
        assertEquals(0, header.reservedValue());

        // non-fragmented message
        header.wrap(buffer, position + 576, 32);
        assertEquals(sendTimestampNs, header.reservedValue());

        // last padding frame
        header.wrap(buffer, position + 704, 32);
        assertEquals(0, header.reservedValue());

        verify(sendTimestampClock).nanoTime();
        verifyNoMoreInteractions(sendTimestampClock);
    }

    @SuppressWarnings("auxiliaryclass")
    @Test
    void shouldRemoveOnlyInactiveDestinationsWhenTheLastOneIsAlsoInactive()
    {
        final CachedNanoClock clock = new CachedNanoClock();
        final DynamicSndMultiDestination destinations =
            new DynamicSndMultiDestination(clock, mock(ErrorHandler.class));
        destinations.destinationsCounter = mock(AtomicCounter.class);

        final StatusMessageFlyweight sm = new StatusMessageFlyweight(
            new UnsafeBuffer(ByteBuffer.allocateDirect(StatusMessageFlyweight.HEADER_LENGTH)));
        final InetSocketAddress one = InetSocketAddress.createUnresolved("localhost", 10001);
        final InetSocketAddress two = InetSocketAddress.createUnresolved("localhost", 10002);
        final InetSocketAddress three = InetSocketAddress.createUnresolved("localhost", 10003);

        clock.update(0);
        destinations.onStatusMessage(sm.receiverId(1), one);
        destinations.onStatusMessage(sm.receiverId(2), two);
        destinations.onStatusMessage(sm.receiverId(3), three);
        assertEquals(3, destinations.destinations.length);

        clock.update(DESTINATION_TIMEOUT);
        destinations.onStatusMessage(sm.receiverId(2), two);

        // one and three have timed out, two has not
        clock.update(DESTINATION_TIMEOUT * 2);
        destinations.send(null, ByteBuffer.allocateDirect(64), null, 64);

        assertEquals(1, destinations.destinations.length);
        assertEquals(2, destinations.destinations[0].receiverId);
    }
}
