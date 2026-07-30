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
import io.aeron.protocol.ErrorFlyweight;
import io.aeron.protocol.NakFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.CloseHelper;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.net.InetSocketAddress;

import static io.aeron.driver.media.SendChannelEndpoint.DESTINATION_TIMEOUT;
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

    private final MediaDriver.Context context = new MediaDriver.Context()
        .systemCounters(mockSystemCounters)
        .cachedNanoClock(nanoClock)
        .senderCachedNanoClock(nanoClock)
        .receiverCachedNanoClock(nanoClock)
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
}
