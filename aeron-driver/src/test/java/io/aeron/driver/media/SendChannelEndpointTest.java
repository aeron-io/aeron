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
import io.aeron.driver.status.SystemCounters;
import org.agrona.CloseHelper;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static io.aeron.driver.media.SendChannelEndpoint.DESTINATION_TIMEOUT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SendChannelEndpointTest
{
    private final SystemCounters mockSystemCounters = mock(SystemCounters.class);
    private final AtomicCounter mockCounter = mock(AtomicCounter.class);
    private final AtomicCounter mockStatusIndicator = mock(AtomicCounter.class);
    private final DriverConductorProxy mockConductorProxy = mock(DriverConductorProxy.class);
    private final CachedNanoClock nanoClock = new CachedNanoClock();

    private final MediaDriver.Context context = new MediaDriver.Context()
        .systemCounters(mockSystemCounters)
        .cachedNanoClock(nanoClock)
        .senderCachedNanoClock(nanoClock)
        .receiverCachedNanoClock(nanoClock)
        .receiveChannelEndpointThreadLocals(new ReceiveChannelEndpointThreadLocals())
        .senderPortManager(new WildcardPortManager(WildcardPortManager.EMPTY_PORT_RANGE, true))
        .receiverPortManager(new WildcardPortManager(WildcardPortManager.EMPTY_PORT_RANGE, false));

    private SendChannelEndpoint endpoint;

    SendChannelEndpointTest()
    {
        when(mockSystemCounters.get(any())).thenReturn(mockCounter);
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
}
