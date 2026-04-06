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

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.driver.media.PortManager;
import io.aeron.driver.media.WildcardPortManager;
import io.aeron.driver.status.SystemCounters;
import io.aeron.test.Tests;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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
    private CountersManager countersManager = Tests.newCountersManager(1024);
    private PortManager portManager = new WildcardPortManager(WildcardPortManager.EMPTY_PORT_RANGE, false);


    @BeforeEach
    void beforeEach()
    {
        epochClock.update(0);

        when(systemCounters.get(any())).thenReturn(mockCounter);

        mediaDriverCtx = mock(MediaDriver.Context.class);
        when(mediaDriverCtx.driverConductorProxy()).thenReturn(driverConductorProxy);
        when(mediaDriverCtx.mtuLength()).thenReturn(Configuration.mtuLength());
        when(mediaDriverCtx.systemCounters()).thenReturn(systemCounters);
        when(mediaDriverCtx.nameResolver()).thenReturn(DefaultNameResolver.INSTANCE);
        when(mediaDriverCtx.resolverName()).thenReturn(LOCAL_RESOLVER_NAME);
        when(mediaDriverCtx.resolverInterface()).thenReturn("0.0.0.0:0");
        when(mediaDriverCtx.resolverBootstrapNeighbor()).thenReturn("127.0.0.1:1234");
        when(mediaDriverCtx.epochClock()).thenReturn(epochClock);
        when(mediaDriverCtx.countersManager()).thenReturn(countersManager);
        when(mediaDriverCtx.receiverPortManager()).thenReturn(portManager);
    }

    @Test
    void shouldUseAllBootstrapNeighbors()
    {
        final String bootstrapNeighborAddresses = "186.123.23.1:1234,224.0.1.1:9713,123.91.72.255:7123";
        final String[] bootstrapNeighborAddressArray = bootstrapNeighborAddresses.split(",");
        when(mediaDriverCtx.resolverBootstrapNeighbor()).thenReturn(bootstrapNeighborAddresses);

        driverNameResolver = new DriverNameResolver(mediaDriverCtx);
        final int neighborsCounterId = neighborsCounterId();

        driverNameResolver.doWork(TIMEOUT_MS * 0);
        verify(driverConductorProxy).reResolveBootstrapNeighbor(eq(bootstrapNeighborAddressArray[0]));
        driverNameResolver.onBootstrapNeighborAddressResolutionChange(new InetSocketAddress("186.123.23.1", 1234));
        final String firstLabel = countersManager.getCounterLabel(neighborsCounterId);
        assertThat(firstLabel, containsString(bootstrapNeighborAddressArray[0]));

        driverNameResolver.doWork(TIMEOUT_MS * 1);
        verify(driverConductorProxy).reResolveBootstrapNeighbor(eq(bootstrapNeighborAddressArray[1]));
        driverNameResolver.onBootstrapNeighborAddressResolutionChange(new InetSocketAddress("224.0.1.1", 9713));
        final String secondLabel = countersManager.getCounterLabel(neighborsCounterId);
        assertThat(secondLabel, containsString(bootstrapNeighborAddressArray[1]));

        driverNameResolver.doWork(TIMEOUT_MS * 2);
        verify(driverConductorProxy).reResolveBootstrapNeighbor(eq(bootstrapNeighborAddressArray[2]));
        driverNameResolver.onBootstrapNeighborAddressResolutionChange(new InetSocketAddress("123.91.72.255", 7123));
        final String thirdLabel = countersManager.getCounterLabel(neighborsCounterId);
        assertThat(thirdLabel, containsString(bootstrapNeighborAddressArray[2]));
    }

    private int neighborsCounterId()
    {
        final MutableInteger counterIdRef = new MutableInteger(Aeron.NULL_VALUE);
        countersManager.forEach((counterId, typeId, keyBuffer, label) ->
        {
            if (AeronCounters.NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID == typeId)
            {
                counterIdRef.set(counterId);
            }
        });
        Assertions.assertNotEquals(Aeron.NULL_VALUE, counterIdRef.get());
        return counterIdRef.get();
    }
}