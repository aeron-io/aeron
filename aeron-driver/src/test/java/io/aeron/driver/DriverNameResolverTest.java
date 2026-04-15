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

import io.aeron.AeronCounters;
import io.aeron.driver.media.PortManager;
import io.aeron.driver.media.UdpNameResolutionTransport;
import io.aeron.driver.media.WildcardPortManager;
import io.aeron.driver.status.SystemCounters;
import io.aeron.exceptions.AeronException;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.ResolutionEntryFlyweight;
import io.aeron.test.Tests;
import io.aeron.test.driver.RedirectingNameResolver;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.protocol.HeaderFlyweight.MIN_HEADER_LENGTH;
import static io.aeron.protocol.ResolutionEntryFlyweight.RES_TYPE_NAME_TO_IP4_MD;
import static io.aeron.protocol.ResolutionEntryFlyweight.SELF_FLAG;
import static io.aeron.test.driver.RedirectingNameResolver.DISABLE_RESOLUTION;
import static io.aeron.test.driver.RedirectingNameResolver.USE_INITIAL_RESOLUTION_HOST;
import static io.aeron.test.driver.RedirectingNameResolver.USE_RE_RESOLUTION_HOST;
import static io.aeron.test.driver.RedirectingNameResolver.updateNameResolutionStatus;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class DriverNameResolverTest
{
    private static final String LOCAL_RESOLVER_NAME = "local-driver";
    private static final long TIMEOUT_MS = TimeUnit.SECONDS.toMillis(20);

    private DriverNameResolver driverNameResolver;
    private MediaDriver.Context mediaDriverCtx;
    private final DefaultNameResolver defaultNameResolver = new DefaultNameResolver();
    private final DefaultNameResolver delegateResolver = spy(defaultNameResolver);
    private final DriverConductorProxy driverConductorProxy = mock(DriverConductorProxy.class);
    private final SystemCounters systemCounters = mock(SystemCounters.class);
    private final AtomicCounter mockCounter = mock(AtomicCounter.class);
    private final CachedEpochClock epochClock = new CachedEpochClock();
    private final CachedNanoClock nanoClock = new CachedNanoClock();
    private final CountersManager countersManager = Tests.newCountersManager(1024);
    private final PortManager portManager = new WildcardPortManager(WildcardPortManager.EMPTY_PORT_RANGE, false);
    private final DriverNameResolver.UdpNameResolutionTransportFactory udpNameResolutionTransportFactory =
        mock(DriverNameResolver.UdpNameResolutionTransportFactory.class);
    private final UdpNameResolutionTransport transport = mock(UdpNameResolutionTransport.class);
    private final ArgumentCaptor<UnsafeBuffer> bufferCaptor = ArgumentCaptor.forClass(UnsafeBuffer.class);
    private final DutyCycleTracker dutyCycleTracker = mock(DutyCycleTracker.class);
    private final CountedErrorHandler countedErrorHandler = mock(CountedErrorHandler.class);

    private final HeaderFlyweight headerFlyweight = new HeaderFlyweight();
    private final ResolutionEntryFlyweight resolutionEntryFlyweight = new ResolutionEntryFlyweight();

    @BeforeEach
    void beforeEach()
    {
        epochClock.update(0);
        nanoClock.update(0);

        when(systemCounters.get(any())).thenReturn(mockCounter);

        when(udpNameResolutionTransportFactory.newInstance(any(), any(), bufferCaptor.capture(), any()))
            .thenReturn(transport);

        mediaDriverCtx = mock(MediaDriver.Context.class);
        when(mediaDriverCtx.driverConductorProxy()).thenReturn(driverConductorProxy);
        when(mediaDriverCtx.countedErrorHandler()).thenReturn(countedErrorHandler);
        when(mediaDriverCtx.nameResolver()).thenReturn(delegateResolver);
        when(mediaDriverCtx.mtuLength()).thenReturn(Configuration.mtuLength());
        when(mediaDriverCtx.systemCounters()).thenReturn(systemCounters);
        when(mediaDriverCtx.resolverName()).thenReturn(LOCAL_RESOLVER_NAME);
        when(mediaDriverCtx.resolverInterface()).thenReturn("0.0.0.0:0");
        when(mediaDriverCtx.resolverBootstrapNeighbor()).thenReturn("127.0.0.1:1234");
        when(mediaDriverCtx.epochClock()).thenReturn(epochClock);
        when(mediaDriverCtx.cachedEpochClock()).thenReturn(epochClock);
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
        driverNameResolver.init(countersManager, countersManager::newCounter);
        driverNameResolver.onStart();

        epochClock.update(TIMEOUT_MS * 0);
        driverNameResolver.doWork();
        verify(transport).sendTo(any(), eq(new InetSocketAddress("186.123.23.1", 1234)));
        verify(transport).sendTo(any(), eq(new InetSocketAddress("224.0.1.1", 9713)));
        verify(transport).sendTo(any(), eq(new InetSocketAddress("123.91.72.255", 7123)));
    }

    @Test
    void shouldReResolveBootstrapNeighbor()
    {
        final String nameOne = "driver-a";
        final String addressOne = "186.123.23.1";
        final int portOne = 1234;
        final String endpointOne = addressOne + ":" + portOne;
        final String nameTwo = "driver-b";
        final String addressTwo = "123.91.72.255";
        final int portTwo = 7123;
        final String endpointTwo = addressTwo + ":" + portTwo;
        final String bootstrapNeighborAddresses = endpointOne + "," + endpointTwo;

        when(mediaDriverCtx.resolverBootstrapNeighbor()).thenReturn(bootstrapNeighborAddresses);
        when(mediaDriverCtx.resolverNeighborTimeoutNs()).thenReturn(Long.MAX_VALUE);
        when(mediaDriverCtx.resolverBootstrapNeighborResolutionIntervalNs())
            .thenReturn(TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS));

        driverNameResolver = new DriverNameResolver(mediaDriverCtx, udpNameResolutionTransportFactory);
        driverNameResolver.init(countersManager, countersManager::newCounter);
        driverNameResolver.onStart();

        verify(delegateResolver).init(any(), any());
        verify(delegateResolver).onStart();

        verify(delegateResolver).lookup(eq(endpointOne), anyString(), eq(false));
        verify(delegateResolver).resolve(eq(addressOne), anyString(), eq(false));
        verify(delegateResolver).lookup(eq(endpointTwo), anyString(), eq(false));
        verify(delegateResolver).resolve(eq(addressTwo), anyString(), eq(false));
        verify(delegateResolver).lookup(eq("0.0.0.0:0"), anyString(), eq(false));
        verify(delegateResolver).resolve(eq("0.0.0.0"), anyString(), eq(false));

        epochClock.update(TIMEOUT_MS * 0);
        driverNameResolver.doWork();
        verify(delegateResolver).doWork();
        onNeighborFrame(nameOne, addressOne, portOne, TIMEOUT_MS * 0);

        epochClock.update(TIMEOUT_MS * 1);
        driverNameResolver.doWork();
        verify(delegateResolver, times(2)).doWork();
        verify(delegateResolver, times(2)).lookup(eq(endpointTwo), anyString(), eq(false));
        verify(delegateResolver, times(2)).resolve(eq(addressTwo), anyString(), eq(false));

        epochClock.update(TIMEOUT_MS * 2);
        driverNameResolver.doWork();
        verify(delegateResolver, times(3)).doWork();
        verify(delegateResolver, times(3)).lookup(eq(endpointTwo), anyString(), eq(false));
        verify(delegateResolver, times(3)).resolve(eq(addressTwo), anyString(), eq(false));

        epochClock.update(TIMEOUT_MS * 3);
        driverNameResolver.doWork();
        verify(delegateResolver, times(4)).lookup(eq(endpointTwo), anyString(), eq(false));
        verify(delegateResolver, times(4)).resolve(eq(addressTwo), anyString(), eq(false));

        onNeighborFrame(nameTwo, addressTwo, portTwo, TIMEOUT_MS * 2);

        epochClock.update(TIMEOUT_MS * 3);
        driverNameResolver.doWork();
        verify(delegateResolver, times(4)).doWork();
        verifyNoMoreInteractions(delegateResolver);
    }

    @Test
    void shouldReResolveBootstrapNeighborEvictedFromNeighborList()
    {
        final String nameOne = "driver-a";
        final String addressOne = "186.123.23.1";
        final int portOne = 1234;
        final String endpointOne = addressOne + ":" + portOne;
        final String nameTwo = "driver-b";
        final String addressTwo = "123.91.72.255";
        final int portTwo = 7123;
        final String endpointTwo = addressTwo + ":" + portTwo;
        final String bootstrapNeighborAddresses = endpointOne + "," + endpointTwo;

        final long neighborTimeoutMs = TimeUnit.SECONDS.toMillis(10);

        when(mediaDriverCtx.resolverBootstrapNeighbor()).thenReturn(bootstrapNeighborAddresses);
        when(mediaDriverCtx.resolverNeighborTimeoutNs()).thenReturn(TimeUnit.MILLISECONDS.toNanos(neighborTimeoutMs));
        when(mediaDriverCtx.resolverBootstrapNeighborResolutionIntervalNs()).thenReturn(TimeUnit.SECONDS.toNanos(1));

        driverNameResolver = new DriverNameResolver(mediaDriverCtx, udpNameResolutionTransportFactory);
        driverNameResolver.init(countersManager, countersManager::newCounter);
        driverNameResolver.onStart();

        verify(delegateResolver).init(any(), any());
        verify(delegateResolver).onStart();

        verify(delegateResolver).lookup(eq(endpointOne), anyString(), eq(false));
        verify(delegateResolver).resolve(eq(addressOne), anyString(), eq(false));
        verify(delegateResolver).lookup(eq(endpointTwo), anyString(), eq(false));
        verify(delegateResolver).resolve(eq(addressTwo), anyString(), eq(false));
        verify(delegateResolver).lookup(eq("0.0.0.0:0"), anyString(), eq(false));
        verify(delegateResolver).resolve(eq("0.0.0.0"), anyString(), eq(false));

        epochClock.update(neighborTimeoutMs * 0);
        onNeighborFrame(nameOne, addressOne, portOne, neighborTimeoutMs * 0);
        onNeighborFrame(nameTwo, addressTwo, portTwo, neighborTimeoutMs * 0);
        driverNameResolver.doWork();
        verify(delegateResolver).doWork();
        verifyNoMoreInteractions(delegateResolver);

        epochClock.update(neighborTimeoutMs / 2);
        driverNameResolver.doWork();
        verify(delegateResolver, times(2)).doWork();
        verifyNoMoreInteractions(delegateResolver);

        epochClock.update(neighborTimeoutMs / 2);
        onNeighborFrame(nameOne, addressOne, portOne, neighborTimeoutMs / 2);
        driverNameResolver.doWork();
        verifyNoMoreInteractions(delegateResolver);

        epochClock.update(neighborTimeoutMs);
        driverNameResolver.doWork();
        verify(delegateResolver, times(3)).doWork();
        verify(delegateResolver, times(2)).lookup(eq(endpointTwo), anyString(), eq(false));
        verify(delegateResolver, times(2)).resolve(eq(addressTwo), anyString(), eq(false));
        verifyNoMoreInteractions(delegateResolver);
    }

    @Test
    void shouldNotAllowMoreThanTwentyBootstrapNeighbors()
    {
        final StringBuilder s = new StringBuilder();
        for (int i = 0; i < 21; i++)
        {
            s.append("localhost:").append(10000 + i).append(",");
        }
        s.setLength(s.length() - 1);

        when(mediaDriverCtx.resolverBootstrapNeighbor()).thenReturn(s.toString());
        final AeronException ex = assertThrows(
            AeronException.class,
            () -> driverNameResolver = new DriverNameResolver(mediaDriverCtx, udpNameResolutionTransportFactory));

        assertThat(ex.getMessage(), containsString("Bootstrap Neighbor list too large"));
    }

    @Test
    void shouldHandleBootstrapNeighborCounter()
    {
        when(mediaDriverCtx.resolverNeighborTimeoutNs()).thenReturn(TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS));
        driverNameResolver = new DriverNameResolver(mediaDriverCtx, udpNameResolutionTransportFactory);
        driverNameResolver.init(countersManager, countersManager::newCounter);

        driverNameResolver.onStart();

        final MutableInteger bootstrapNeighborCounterId = new MutableInteger(NULL_VALUE);
        countersManager.forEach((counterId, typeId, keyBuffer, label) ->
        {
            if (AeronCounters.NAME_RESOLVER_BOOTSTRAP_NEIGHBOR_COUNTER_TYPE_ID == typeId)
            {
                bootstrapNeighborCounterId.set(counterId);
            }
        });
        assertNotEquals(NULL_VALUE, bootstrapNeighborCounterId.get());

        final String onStartLabel = "Bootstrap neighbor: name=127.0.0.1:1234 resolved=127.0.0.1:1234";
        assertEquals(onStartLabel, countersManager.getCounterLabel(bootstrapNeighborCounterId.get()));

        epochClock.update(0);
        driverNameResolver.doWork();
        assertEquals(0L, countersManager.getCounterValue(bootstrapNeighborCounterId.get()));

        onNeighborFrame("driver-b", "127.0.0.1", 1234, TIMEOUT_MS);
        epochClock.update(TIMEOUT_MS);
        driverNameResolver.doWork();
        assertEquals(1L, countersManager.getCounterValue(bootstrapNeighborCounterId.get()));

        epochClock.update(TIMEOUT_MS * 2);
        onNeighborFrame("driver-b", "127.0.0.1", 1234, TIMEOUT_MS * 2);
        driverNameResolver.doWork();
        assertEquals(1L, countersManager.getCounterValue(bootstrapNeighborCounterId.get()));

        epochClock.update(TIMEOUT_MS * 3);
        driverNameResolver.doWork();
        assertEquals(0L, countersManager.getCounterValue(bootstrapNeighborCounterId.get()));
    }

    @Test
    void shouldHandleBootstrapNeighborCounterOnReresolution()
    {
        final String driverAddr = "driver";
        final int driverPort = 1234;
        final String driver = driverAddr + ":" + driverPort;

        final RedirectingNameResolver redirectingNameResolver = new RedirectingNameResolver(
            "driver,127.0.0.1,127.0.0.2");
        when(mediaDriverCtx.resolverBootstrapNeighbor()).thenReturn(driver);
        when(mediaDriverCtx.nameResolver()).thenReturn(redirectingNameResolver);

        driverNameResolver = new DriverNameResolver(mediaDriverCtx, udpNameResolutionTransportFactory);
        driverNameResolver.init(countersManager, countersManager::newCounter);

        assertTrue(updateNameResolutionStatus(countersManager, driverAddr, USE_INITIAL_RESOLUTION_HOST));

        driverNameResolver.onStart();

        final MutableInteger bootstrapNeighborCounterId = new MutableInteger(NULL_VALUE);
        countersManager.forEach((counterId, typeId, keyBuffer, label) ->
        {
            if (AeronCounters.NAME_RESOLVER_BOOTSTRAP_NEIGHBOR_COUNTER_TYPE_ID == typeId)
            {
                bootstrapNeighborCounterId.set(counterId);
            }
        });
        assertNotEquals(NULL_VALUE, bootstrapNeighborCounterId.get());

        final String onStartLabel = "Bootstrap neighbor: name=driver:1234 resolved=127.0.0.1:1234";
        assertEquals(onStartLabel, countersManager.getCounterLabel(bootstrapNeighborCounterId.get()));

        assertTrue(updateNameResolutionStatus(countersManager, "driver", USE_RE_RESOLUTION_HOST));
        epochClock.update(TIMEOUT_MS);
        driverNameResolver.doWork();

        final String onReResolutionLabel = "Bootstrap neighbor: name=driver:1234 resolved=127.0.0.2:1234";
        assertEquals(onReResolutionLabel, countersManager.getCounterLabel(bootstrapNeighborCounterId.get()));

        assertTrue(updateNameResolutionStatus(countersManager, "driver", DISABLE_RESOLUTION));
        epochClock.update(TIMEOUT_MS * 2);
        driverNameResolver.doWork();
        verify(countedErrorHandler).onError(any());

        final String unResolvableLabel = "Bootstrap neighbor: name=driver:1234 resolved=";
        assertEquals(unResolvableLabel, countersManager.getCounterLabel(bootstrapNeighborCounterId.get()));
    }

    private void onNeighborFrame(final String name, final String address, final int port, final long time)
    {
        final InetSocketAddress socketAddress = new InetSocketAddress(address, port);

        final UnsafeBuffer unsafeBuffer = bufferCaptor.getValue();
        headerFlyweight.wrap(unsafeBuffer);
        headerFlyweight
            .headerType(HeaderFlyweight.HDR_TYPE_RES)
            .flags((short)0)
            .version(HeaderFlyweight.CURRENT_VERSION);

        resolutionEntryFlyweight.wrap(
            unsafeBuffer,
            HeaderFlyweight.MIN_HEADER_LENGTH,
            unsafeBuffer.capacity() - HeaderFlyweight.MIN_HEADER_LENGTH);

        resolutionEntryFlyweight
            .resType(RES_TYPE_NAME_TO_IP4_MD)
            .flags(SELF_FLAG)
            .udpPort((short)socketAddress.getPort())
            .ageInMs(0)
            .putAddress(socketAddress.getAddress().getAddress())
            .putName(name.getBytes(StandardCharsets.US_ASCII));

        final int frameLength = MIN_HEADER_LENGTH + resolutionEntryFlyweight.entryLength();
        driverNameResolver.onFrame(unsafeBuffer, frameLength, socketAddress, time);
    }
}