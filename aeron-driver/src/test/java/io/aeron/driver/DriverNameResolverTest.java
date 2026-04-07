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

import io.aeron.driver.media.PortManager;
import io.aeron.driver.media.UdpNameResolutionTransport;
import io.aeron.driver.media.WildcardPortManager;
import io.aeron.driver.status.SystemCounters;
import io.aeron.test.Tests;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DriverNameResolverTest
{
    private static final String LOCAL_RESOLVER_NAME = "local-driver";
    private static final long TIMEOUT_MS = TimeUnit.SECONDS.toMillis(20);

    private DriverNameResolver driverNameResolver;
    private MediaDriver.Context mediaDriverCtx;
    private DriverConductorProxy driverConductorProxy = mock(DriverConductorProxy.class);
    private SystemCounters systemCounters = mock(SystemCounters.class);
    private AtomicCounter mockCounter = mock(AtomicCounter.class);
    private CachedEpochClock epochClock = new CachedEpochClock();
    private CachedNanoClock nanoClock = new CachedNanoClock();
    private CountersManager countersManager = Tests.newCountersManager(1024);
    private PortManager portManager = new WildcardPortManager(WildcardPortManager.EMPTY_PORT_RANGE, false);
    private DriverNameResolver.UdpNameResolutionTransportFactory udpNameResolutionTransportFactory =
        mock(DriverNameResolver.UdpNameResolutionTransportFactory.class);
    private UdpNameResolutionTransport transport = mock(UdpNameResolutionTransport.class);
    private DutyCycleTracker dutyCycleTracker = mock(DutyCycleTracker.class);

    @BeforeEach
    void beforeEach()
    {
        epochClock.update(0);
        nanoClock.update(0);

        when(systemCounters.get(any())).thenReturn(mockCounter);

        when(udpNameResolutionTransportFactory.newInstance(any(), any(), any(), any())).thenReturn(transport);

        mediaDriverCtx = mock(MediaDriver.Context.class);
        when(mediaDriverCtx.driverConductorProxy()).thenReturn(driverConductorProxy);
        when(mediaDriverCtx.mtuLength()).thenReturn(Configuration.mtuLength());
        when(mediaDriverCtx.systemCounters()).thenReturn(systemCounters);
        when(mediaDriverCtx.nameResolver()).thenReturn(DefaultNameResolver.INSTANCE);
        when(mediaDriverCtx.resolverName()).thenReturn(LOCAL_RESOLVER_NAME);
        when(mediaDriverCtx.resolverInterface()).thenReturn("0.0.0.0:0");
        when(mediaDriverCtx.resolverBootstrapNeighbor()).thenReturn("127.0.0.1:1234");
        when(mediaDriverCtx.epochClock()).thenReturn(epochClock);
        when(mediaDriverCtx.nanoClock()).thenReturn(nanoClock);
        when(mediaDriverCtx.nameResolverTimeTracker()).thenReturn(dutyCycleTracker);
        when(mediaDriverCtx.countersManager()).thenReturn(countersManager);
        when(mediaDriverCtx.receiverPortManager()).thenReturn(portManager);
    }

    @Test
    void shouldUseAllBootstrapNeighbors()
    {
        final String bootstrapNeighborAddresses = "186.123.23.1:1234,224.0.1.1:9713,123.91.72.255:7123";
        when(mediaDriverCtx.resolverBootstrapNeighbor()).thenReturn(bootstrapNeighborAddresses);

        driverNameResolver = new DriverNameResolver(mediaDriverCtx, udpNameResolutionTransportFactory);

        driverNameResolver.doWork(TIMEOUT_MS * 0);
        verify(transport).sendTo(any(), eq(new InetSocketAddress("186.123.23.1", 1234)));
        verify(transport).sendTo(any(), eq(new InetSocketAddress("224.0.1.1", 9713)));
        verify(transport).sendTo(any(), eq(new InetSocketAddress("123.91.72.255", 7123)));
    }
}