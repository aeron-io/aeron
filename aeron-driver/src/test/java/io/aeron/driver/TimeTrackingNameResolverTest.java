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

import io.aeron.CounterProvider;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.net.InetAddress;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TimeTrackingNameResolverTest
{
    @Test
    void throwsNullPointerExceptionIfDelegateResolverIsNull()
    {
        assertThrowsExactly(
            NullPointerException.class,
            () -> new TimeTrackingNameResolver(null, mock(NanoClock.class), mock(DutyCycleTracker.class)));
    }

    @Test
    void throwsNullPointerExceptionIfNanoClockIsNull()
    {
        assertThrowsExactly(
            NullPointerException.class,
            () -> new TimeTrackingNameResolver(mock(NameResolverAgent.class), null, mock(DutyCycleTracker.class)));
    }

    @Test
    void throwsNullPointerExceptionIfDutyCycleTrackerIsNull()
    {
        assertThrowsExactly(
            NullPointerException.class,
            () -> new TimeTrackingNameResolver(mock(NameResolverAgent.class), mock(NanoClock.class), null));
    }

    @Test
    void closeIsANoOpIfDelegateResolverIsNotCloseable()
    {
        final NameResolverAgent delegateResolver = mock(NameResolverAgent.class);
        final NanoClock clock = mock(NanoClock.class);
        final DutyCycleTracker maxTime = mock(DutyCycleTracker.class);
        final TimeTrackingNameResolver resolver = new TimeTrackingNameResolver(delegateResolver, clock, maxTime);

        resolver.onClose();

        verify(delegateResolver).onClose();
        verifyNoMoreInteractions(delegateResolver, clock, maxTime);
    }

    @Test
    void closeIShouldCloseDelegateResolver()
    {
        final NameResolverAgent delegateResolver = mock(NameResolverAgent.class);
        final NanoClock clock = mock(NanoClock.class);
        final DutyCycleTracker maxTime = mock(DutyCycleTracker.class);
        final TimeTrackingNameResolver resolver = new TimeTrackingNameResolver(delegateResolver, clock, maxTime);

        resolver.onClose();

        verify(delegateResolver).onClose();
        verifyNoMoreInteractions(delegateResolver);
        verifyNoInteractions(clock, maxTime);
    }

    @Test
    void doWorkShouldCallActualMethod()
    {
        final NameResolverAgent delegateResolver = mock(NameResolverAgent.class);
        final NanoClock clock = mock(NanoClock.class);
        final DutyCycleTracker maxTime = mock(DutyCycleTracker.class);
        final TimeTrackingNameResolver resolver = new TimeTrackingNameResolver(delegateResolver, clock, maxTime);

        resolver.doWork();

        verify(delegateResolver).doWork();
        verifyNoMoreInteractions(delegateResolver);
        verifyNoInteractions(clock, maxTime);
    }

    @Test
    void initShouldCallActualMethod()
    {
        final NameResolverAgent delegateResolver = mock(NameResolverAgent.class);
        final NanoClock clock = mock(NanoClock.class);
        final DutyCycleTracker maxTime = mock(DutyCycleTracker.class);
        final TimeTrackingNameResolver resolver = new TimeTrackingNameResolver(delegateResolver, clock, maxTime);

        final CountersReader countersReader = mock(CountersReader.class);
        final CounterProvider factory = mock(CounterProvider.class);
        resolver.init(countersReader, factory);

        verify(delegateResolver).init(countersReader, factory);
        verifyNoMoreInteractions(delegateResolver);
        verifyNoInteractions(clock, maxTime);
    }

    @Test
    void lookupShouldMeasureExecutionTime()
    {
        final NameResolverAgent delegateResolver = mock(NameResolverAgent.class);
        when(delegateResolver.lookup(anyString(), anyString(), anyBoolean()))
            .thenAnswer(invocation ->
            {
                final String name = invocation.getArgument(0);
                return name.substring(0, name.indexOf(':'));
            });
        final NanoClock clock = mock(NanoClock.class);
        final long beginNs = 0;
        final long endNs = 123456789;
        when(clock.nanoTime()).thenReturn(beginNs, endNs);
        final DutyCycleTracker maxTime = mock(DutyCycleTracker.class);
        final TimeTrackingNameResolver resolver = new TimeTrackingNameResolver(delegateResolver, clock, maxTime);

        final String name = "my-host:8080";
        final String endpoint = "endpoint";
        final boolean isReLookup = false;
        assertEquals("my-host", resolver.lookup(name, endpoint, isReLookup));

        final InOrder inOrder = inOrder(delegateResolver, clock, maxTime);
        inOrder.verify(clock).nanoTime();
        inOrder.verify(maxTime).update(beginNs);
        inOrder.verify(delegateResolver).lookup(name, endpoint, isReLookup);
        inOrder.verify(clock).nanoTime();
        inOrder.verify(maxTime).measureAndUpdate(endNs);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void lookupShouldMeasureExecutionTimeEvenIfExceptionIsThrown()
    {
        final NameResolverAgent delegateResolver = mock(NameResolverAgent.class);
        final Error error = new Error("broken");
        when(delegateResolver.lookup(anyString(), anyString(), anyBoolean())).thenThrow(error);
        final NanoClock clock = mock(NanoClock.class);
        final long beginNs = 236745823658245L;
        final long endNs = 7534957349857893459L;
        when(clock.nanoTime()).thenReturn(beginNs, endNs);
        final DutyCycleTracker maxTime = mock(DutyCycleTracker.class);
        final TimeTrackingNameResolver resolver = new TimeTrackingNameResolver(delegateResolver, clock, maxTime);

        final String name = "test:555";
        final String endpoint = "control";
        final boolean isReLookup = true;
        final Error exception = assertThrowsExactly(Error.class, () -> resolver.lookup(name, endpoint, isReLookup));
        assertSame(error, exception);

        final InOrder inOrder = inOrder(delegateResolver, clock, maxTime);
        inOrder.verify(clock).nanoTime();
        inOrder.verify(maxTime).update(beginNs);
        inOrder.verify(delegateResolver).lookup(name, endpoint, isReLookup);
        inOrder.verify(clock).nanoTime();
        inOrder.verify(maxTime).measureAndUpdate(endNs);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void resolveShouldMeasureExecutionTime()
    {
        final NameResolverAgent delegateResolver = mock(NameResolverAgent.class);
        when(delegateResolver.resolve(anyString(), anyString(), anyBoolean()))
            .thenAnswer(invocation -> InetAddress.getByName(invocation.getArgument(0)));
        final NanoClock clock = mock(NanoClock.class);
        final long beginNs = SECONDS.toNanos(1);
        final long endNs = SECONDS.toNanos(9);
        when(clock.nanoTime()).thenReturn(beginNs, endNs);
        final DutyCycleTracker maxTime = mock(DutyCycleTracker.class);
        final TimeTrackingNameResolver resolver = new TimeTrackingNameResolver(delegateResolver, clock, maxTime);

        final String name = "localhost";
        final String endpoint = "endpoint";
        final boolean isReLookup = true;
        assertEquals(InetAddress.getLoopbackAddress(), resolver.resolve(name, endpoint, isReLookup));

        final InOrder inOrder = inOrder(delegateResolver, clock, maxTime);
        inOrder.verify(clock).nanoTime();
        inOrder.verify(maxTime).update(beginNs);
        inOrder.verify(delegateResolver).resolve(name, endpoint, isReLookup);
        inOrder.verify(clock).nanoTime();
        inOrder.verify(maxTime).measureAndUpdate(endNs);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void resolveShouldMeasureExecutionTimeEvenWhenExceptionIsThrown()
    {
        final NameResolverAgent delegateResolver = mock(NameResolverAgent.class);
        final IllegalStateException exception = new IllegalStateException("error");
        when(delegateResolver.resolve(anyString(), anyString(), anyBoolean()))
            .thenThrow(exception);
        final NanoClock clock = mock(NanoClock.class);
        final long beginNs = SECONDS.toNanos(0);
        final long endNs = SECONDS.toNanos(3);
        when(clock.nanoTime()).thenReturn(beginNs, endNs);
        final DutyCycleTracker maxTime = mock(DutyCycleTracker.class);
        final TimeTrackingNameResolver resolver = new TimeTrackingNameResolver(delegateResolver, clock, maxTime);

        final String name = "localhost";
        final String endpoint = "endpoint";
        final boolean isReLookup = true;
        final IllegalStateException error =
            assertThrowsExactly(IllegalStateException.class, () -> resolver.resolve(name, endpoint, isReLookup));
        assertSame(exception, error);

        final InOrder inOrder = inOrder(delegateResolver, clock, maxTime);
        inOrder.verify(clock).nanoTime();
        inOrder.verify(maxTime).update(beginNs);
        inOrder.verify(delegateResolver).resolve(name, endpoint, isReLookup);
        inOrder.verify(clock).nanoTime();
        inOrder.verify(maxTime).measureAndUpdate(endNs);
        inOrder.verifyNoMoreInteractions();
    }
}
