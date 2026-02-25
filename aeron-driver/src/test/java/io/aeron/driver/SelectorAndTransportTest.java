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

import io.aeron.driver.media.ControlTransportPoller;
import io.aeron.driver.media.DataTransportPoller;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.ReceiveChannelEndpointThreadLocals;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.media.UdpTransportPoller;
import io.aeron.driver.media.WildcardPortManager;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import org.agrona.BitUtil;
import org.agrona.ErrorHandler;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(InterruptingTestCallback.class)
class SelectorAndTransportTest
{
    private static final int RCV_PORT = 40123;
    private static final int SRC_PORT = 40124;
    private static final int SESSION_ID = 0xdeadbeef;
    private static final int STREAM_ID = 0x44332211;
    private static final int TERM_ID = 0x99887766;
    private static final int FRAME_LENGTH = 24;

    private static final UdpChannel SRC_DST =
        UdpChannel.parse("aeron:udp?interface=localhost:" + SRC_PORT + "|endpoint=localhost:" + RCV_PORT);
    private static final UdpChannel RCV_DST = UdpChannel.parse("aeron:udp?endpoint=localhost:" + RCV_PORT);

    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(256);
    private final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);

    private final DataHeaderFlyweight encodeDataHeader = new DataHeaderFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();

    private final InetSocketAddress rcvRemoteAddress = new InetSocketAddress("localhost", SRC_PORT);

    private final SystemCounters mockSystemCounters = mock(SystemCounters.class);
    private final AtomicCounter mockStatusMessagesReceivedCounter = mock(AtomicCounter.class);
    private final AtomicCounter mockSendStatusIndicator = mock(AtomicCounter.class);
    private final AtomicCounter mockReceiveStatusIndicator = mock(AtomicCounter.class);

    private final DataPacketDispatcher mockDispatcher = mock(DataPacketDispatcher.class);
    private final NetworkPublication mockPublication = mock(NetworkPublication.class);
    private final DriverConductorProxy mockDriverConductorProxy = mock(DriverConductorProxy.class);
    private final ErrorHandler errorHandler = mock(ErrorHandler.class);

    private final DataTransportPoller dataTransportPoller = new DataTransportPoller(errorHandler);
    private final ControlTransportPoller controlTransportPoller = new ControlTransportPoller(
        errorHandler, mockDriverConductorProxy);
    private SendChannelEndpoint sendChannelEndpoint;
    private ReceiveChannelEndpoint receiveChannelEndpoint;

    private final CachedNanoClock nanoClock = new CachedNanoClock();
    private final MediaDriver.Context context = new MediaDriver.Context()
        .systemCounters(mockSystemCounters)
        .cachedNanoClock(nanoClock)
        .senderCachedNanoClock(nanoClock)
        .receiverCachedNanoClock(nanoClock)
        .receiveChannelEndpointThreadLocals(new ReceiveChannelEndpointThreadLocals())
        .senderPortManager(new WildcardPortManager(WildcardPortManager.EMPTY_PORT_RANGE, true))
        .receiverPortManager(new WildcardPortManager(WildcardPortManager.EMPTY_PORT_RANGE, false));

    @BeforeEach
    void setup()
    {
        when(mockSystemCounters.get(any())).thenReturn(mockStatusMessagesReceivedCounter);
        when(mockPublication.streamId()).thenReturn(STREAM_ID);
        when(mockPublication.sessionId()).thenReturn(SESSION_ID);
    }

    @AfterEach
    void tearDown()
    {
        try
        {
            if (null != sendChannelEndpoint)
            {
                sendChannelEndpoint.close();
                processLoop(controlTransportPoller, 5);
            }

            if (null != receiveChannelEndpoint)
            {
                receiveChannelEndpoint.close();
                processLoop(dataTransportPoller, 5);
            }

            dataTransportPoller.close();
            controlTransportPoller.close();
        }
        catch (final Exception ex)
        {
            throw ex;
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldHandleBasicSetupAndTearDown()
    {
        receiveChannelEndpoint = new ReceiveChannelEndpoint(
            RCV_DST, mockDispatcher, mockReceiveStatusIndicator, context);
        sendChannelEndpoint = new SendChannelEndpoint(SRC_DST, mockSendStatusIndicator, context);

        receiveChannelEndpoint.openDatagramChannel(mockReceiveStatusIndicator);
        dataTransportPoller.registerForRead(receiveChannelEndpoint, receiveChannelEndpoint, 0);
        sendChannelEndpoint.openDatagramChannel(mockSendStatusIndicator);
        controlTransportPoller.registerForRead(sendChannelEndpoint);

        processLoop(dataTransportPoller, 5);
    }

    @Test
    void shouldSetSocketBufferSizesFromUdpChannelForReceiveChannel() throws IOException
    {
        final DatagramChannel spyChannel = spy(DatagramChannel.open(StandardProtocolFamily.INET));
        final UdpChannel channel = UdpChannel.parse(
            "aeron:udp?endpoint=localhost:" + RCV_PORT + "|so-sndbuf=8k|so-rcvbuf=4k");
        receiveChannelEndpoint = new ReceiveChannelEndpoint(
            channel, mockDispatcher, mockReceiveStatusIndicator, context);

        try (MockedStatic<DatagramChannel> mockDatagramChannel = Mockito.mockStatic(DatagramChannel.class))
        {
            mockDatagramChannel.when(() -> DatagramChannel.open(StandardProtocolFamily.INET)).thenReturn(spyChannel);
            receiveChannelEndpoint.openDatagramChannel(mockReceiveStatusIndicator);

            verify(spyChannel).setOption(StandardSocketOptions.SO_SNDBUF, 8192);
            verify(spyChannel).setOption(StandardSocketOptions.SO_RCVBUF, 4096);
        }
    }

    @Test
    void shouldSetSocketBufferSizesFromUdpChannelForSendChannel() throws IOException
    {
        final DatagramChannel spyChannel = spy(DatagramChannel.open(StandardProtocolFamily.INET));
        final UdpChannel channel = UdpChannel.parse(
            "aeron:udp?endpoint=localhost:" + RCV_PORT + "|so-sndbuf=8k|so-rcvbuf=4k");
        sendChannelEndpoint = new SendChannelEndpoint(channel, mockReceiveStatusIndicator, context);

        try (MockedStatic<DatagramChannel> mockDatagramChannel = Mockito.mockStatic(DatagramChannel.class))
        {
            mockDatagramChannel.when(() -> DatagramChannel.open(StandardProtocolFamily.INET)).thenReturn(spyChannel);
            sendChannelEndpoint.openDatagramChannel(mockReceiveStatusIndicator);

            verify(spyChannel).setOption(StandardSocketOptions.SO_SNDBUF, 8192);
            verify(spyChannel).setOption(StandardSocketOptions.SO_RCVBUF, 4096);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldSendEmptyDataFrameUnicastFromSourceToReceiver()
    {
        final MutableInteger dataHeadersReceived = new MutableInteger(0);

        doAnswer(
            (invocation) ->
            {
                dataHeadersReceived.value++;
                return null;
            })
            .when(mockDispatcher).onDataPacket(
                any(ReceiveChannelEndpoint.class),
                any(DataHeaderFlyweight.class),
                any(UnsafeBuffer.class),
                anyInt(),
                any(InetSocketAddress.class),
                anyInt());

        receiveChannelEndpoint = new ReceiveChannelEndpoint(
            RCV_DST, mockDispatcher, mockReceiveStatusIndicator, context);
        sendChannelEndpoint = new SendChannelEndpoint(SRC_DST, mockSendStatusIndicator, context);

        receiveChannelEndpoint.openDatagramChannel(mockReceiveStatusIndicator);
        dataTransportPoller.registerForRead(receiveChannelEndpoint, receiveChannelEndpoint, 0);
        sendChannelEndpoint.openDatagramChannel(mockSendStatusIndicator);
        controlTransportPoller.registerForRead(sendChannelEndpoint);

        encodeDataHeader.wrap(buffer);
        encodeDataHeader
            .version(HeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(HeaderFlyweight.HDR_TYPE_DATA)
            .frameLength(FRAME_LENGTH);
        encodeDataHeader
            .sessionId(SESSION_ID)
            .streamId(STREAM_ID)
            .termId(TERM_ID);
        byteBuffer.position(0).limit(FRAME_LENGTH);

        processLoop(dataTransportPoller, 5);
        sendChannelEndpoint.send(byteBuffer);
        while (dataHeadersReceived.get() < 1)
        {
            processLoop(dataTransportPoller, 1);
        }

        assertEquals(1, dataHeadersReceived.get());
    }

    @Test
    @InterruptAfter(10)
    void shouldSendMultipleDataFramesPerDatagramUnicastFromSourceToReceiver()
    {
        final MutableInteger dataHeadersReceived = new MutableInteger(0);

        doAnswer(
            (invocation) ->
            {
                dataHeadersReceived.value++;
                return null;
            })
            .when(mockDispatcher).onDataPacket(
                any(ReceiveChannelEndpoint.class),
                any(DataHeaderFlyweight.class),
                any(UnsafeBuffer.class),
                anyInt(),
                any(InetSocketAddress.class),
                anyInt());

        receiveChannelEndpoint = new ReceiveChannelEndpoint(
            RCV_DST, mockDispatcher, mockReceiveStatusIndicator, context);
        sendChannelEndpoint = new SendChannelEndpoint(SRC_DST, mockSendStatusIndicator, context);

        receiveChannelEndpoint.openDatagramChannel(mockReceiveStatusIndicator);
        dataTransportPoller.registerForRead(receiveChannelEndpoint, receiveChannelEndpoint, 0);
        sendChannelEndpoint.openDatagramChannel(mockSendStatusIndicator);
        controlTransportPoller.registerForRead(sendChannelEndpoint);

        encodeDataHeader.wrap(buffer);
        encodeDataHeader
            .version(HeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(HeaderFlyweight.HDR_TYPE_DATA)
            .frameLength(FRAME_LENGTH);
        encodeDataHeader
            .sessionId(SESSION_ID)
            .streamId(STREAM_ID)
            .termId(TERM_ID);

        final int alignedFrameLength = BitUtil.align(FRAME_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
        encodeDataHeader.wrap(buffer, alignedFrameLength, buffer.capacity() - alignedFrameLength);
        encodeDataHeader
            .version(HeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(HeaderFlyweight.HDR_TYPE_DATA)
            .frameLength(24);
        encodeDataHeader
            .sessionId(SESSION_ID)
            .streamId(STREAM_ID)
            .termId(TERM_ID);

        byteBuffer.position(0).limit(2 * alignedFrameLength);

        processLoop(dataTransportPoller, 5);
        sendChannelEndpoint.send(byteBuffer);
        while (dataHeadersReceived.get() < 1)
        {
            processLoop(dataTransportPoller, 1);
        }

        assertEquals(1, dataHeadersReceived.get());
    }

    @Test
    @InterruptAfter(10)
    void shouldHandleSmFrameFromReceiverToSender()
    {
        final MutableInteger controlMessagesReceived = new MutableInteger(0);

        doAnswer(
            (invocation) ->
            {
                controlMessagesReceived.value++;
                return null;
            })
            .when(mockPublication).onStatusMessage(any(), any(), any());

        receiveChannelEndpoint = new ReceiveChannelEndpoint(
            RCV_DST, mockDispatcher, mockReceiveStatusIndicator, context);
        sendChannelEndpoint = new SendChannelEndpoint(SRC_DST, mockSendStatusIndicator, context);
        sendChannelEndpoint.registerForSend(mockPublication);

        receiveChannelEndpoint.openDatagramChannel(mockReceiveStatusIndicator);
        dataTransportPoller.registerForRead(receiveChannelEndpoint, receiveChannelEndpoint, 0);
        sendChannelEndpoint.openDatagramChannel(mockSendStatusIndicator);
        controlTransportPoller.registerForRead(sendChannelEndpoint);

        statusMessage.wrap(buffer);
        statusMessage
            .streamId(STREAM_ID)
            .sessionId(SESSION_ID)
            .consumptionTermId(TERM_ID)
            .receiverWindowLength(1000)
            .consumptionTermOffset(0)
            .version(HeaderFlyweight.CURRENT_VERSION)
            .flags((short)0)
            .headerType(HeaderFlyweight.HDR_TYPE_SM)
            .frameLength(StatusMessageFlyweight.HEADER_LENGTH);
        byteBuffer.position(0).limit(statusMessage.frameLength());

        processLoop(dataTransportPoller, 5);
        receiveChannelEndpoint.sendTo(byteBuffer, rcvRemoteAddress);

        while (controlMessagesReceived.get() < 1)
        {
            processLoop(controlTransportPoller, 1);
        }

        verify(mockStatusMessagesReceivedCounter, times(1)).incrementRelease();
    }

    private void processLoop(final UdpTransportPoller transportPoller, final int iterations)
    {
        for (int i = 0; i < iterations; i++)
        {
            transportPoller.pollTransports();
        }
    }
}
