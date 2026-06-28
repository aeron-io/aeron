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

import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.driver.status.SystemCounters;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class NativeResourceAgentTest
{
    private final NameResolverAgent nameResolver = mock(NameResolverAgent.class);
    private final OneToOneConcurrentArrayQueue<Runnable> queue =
        new OneToOneConcurrentArrayQueue<>(5);
    private final AtomicCounter freeFailsCounter = mock(AtomicCounter.class);
    private NativeResourceAgent executor;

    @BeforeEach
    void before()
    {
        final SystemCounters systemCounters = mock(SystemCounters.class);
        when(systemCounters.get(SystemCounterDescriptor.FREE_FAILS)).thenReturn(freeFailsCounter);
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .nativeResourceAgentCommandQueue(queue)
            .resourceFreeLimit(3)
            .systemCounters(systemCounters)
            .nameResolver(nameResolver)
            .countedErrorHandler(mock(CountedErrorHandler.class))
            .nanoClock(SystemNanoClock.INSTANCE)
            .nameResolverTimeTracker(mock(DutyCycleTracker.class));
        executor = new NativeResourceAgent(ctx);
    }

    @Test
    void shouldCallDoWorkOnNameResolver()
    {
        when(nameResolver.doWork()).thenReturn(0, 1, 3, 0);

        assertEquals(0, executor.doWork());
        assertEquals(1, executor.doWork());
        assertEquals(3, executor.doWork());
        assertEquals(0, executor.doWork());
        assertEquals(0, executor.doWork());

        verify(nameResolver, times(5)).doWork();
    }

    @Test
    void shouldCallDoWorkOnNameResolverProcessQueuedCommandsAndFreeResources()
    {
        when(nameResolver.doWork()).thenReturn(1, 7, 2, 5, 0);

        final Runnable task1 = mock(Runnable.class);
        final Runnable task2 = mock(Runnable.class);
        final Runnable task3 = mock(Runnable.class);
        queue.add(task1);
        queue.add(task2);
        queue.add(task3);

        final RawLog resource1 = mock(RawLog.class);
        when(resource1.free()).thenReturn(true);
        final RawLog resource2 = mock(RawLog.class);
        when(resource2.free()).thenReturn(false, false, true);
        final RawLog resource3 = mock(RawLog.class);
        when(resource3.free()).thenReturn(false, true);
        final RawLog resource4 = mock(RawLog.class);
        when(resource4.free()).thenReturn(true);
        executor.onFreeLogBuffer(resource1);
        executor.onFreeLogBuffer(resource2);
        executor.onFreeLogBuffer(resource3);
        executor.onFreeLogBuffer(resource4);

        assertEquals(3, executor.doWork());
        assertEquals(10, executor.doWork());
        assertEquals(4, executor.doWork());
        assertEquals(5, executor.doWork());

        final InOrder inOrder = inOrder(
            nameResolver, task1, task2, task3, resource1, resource2, resource3, resource4, freeFailsCounter);
        inOrder.verify(nameResolver).doWork();
        inOrder.verify(task1).run();
        inOrder.verify(resource1).free();
        inOrder.verify(resource2).free();
        inOrder.verify(freeFailsCounter).incrementRelease();
        inOrder.verify(resource3).free();
        inOrder.verify(freeFailsCounter).incrementRelease();

        inOrder.verify(nameResolver).doWork();
        inOrder.verify(task2).run();
        inOrder.verify(resource4).free();
        inOrder.verify(resource2).free();
        inOrder.verify(freeFailsCounter).incrementRelease();
        inOrder.verify(resource3).free();

        inOrder.verify(nameResolver).doWork();
        inOrder.verify(task3).run();
        inOrder.verify(resource2).free();

        inOrder.verify(nameResolver).doWork();

        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldCallOnStartOnNameResolver()
    {
        executor.onStart();

        verify(nameResolver).onStart();
        verifyNoMoreInteractions(nameResolver);
    }

    @Test
    void shouldCloseNameResolver()
    {
        final RawLog resource1 = mock(RawLog.class);
        final RawLog resource2 = mock(RawLog.class);
        final RawLog resource3 = mock(RawLog.class);
        executor.onFreeLogBuffer(resource1);
        executor.onFreeLogBuffer(resource2);
        executor.onFreeLogBuffer(resource3);

        executor.onClose();

        final InOrder inOrder = inOrder(resource1, resource2, resource3, nameResolver);
        inOrder.verify(nameResolver).onClose();
        inOrder.verify(resource1).free();
        inOrder.verify(resource2).free();
        inOrder.verify(resource3).free();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void roleName()
    {
        assertEquals("aeron-md-nra", executor.roleName());
    }
}
