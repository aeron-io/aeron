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
package io.aeron.cluster;

import io.aeron.Aeron;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Implementation of the {@link TimerService} that uses a priority heap to order the timestamps.
 *
 * <p>
 * <b>Caveats</b>
 * <p>
 * Timers with the same deadline are not be ordered with one another. In contrast, the timers with different deadlines
 * are guaranteed to expire in order even after Cluster restart, i.e. when the deadlines are in the past.
 * <p>
 * <b>Note:</b> Not thread safe.
 */
final class PriorityHeapTimerService implements TimerService
{
    private static final Timer[] EMPTY_TIMERS = new Timer[0];
    private static final int MIN_CAPACITY = 8;

    private final TimerHandler timerHandler;
    private final Long2ObjectHashMap<Timer> timerByCorrelationId = new Long2ObjectHashMap<>();
    private Timer[] timers = EMPTY_TIMERS;
    private Timer[] freeTimers = EMPTY_TIMERS;
    private int size;
    private int freeTimerCount;

    /**
     * Construct a Priority Heap Timer Service using the supplied handler to
     * callback for expired timers.
     *
     * @param timerHandler to callback when a timer expires.
     */
    PriorityHeapTimerService(final TimerHandler timerHandler)
    {
        this.timerHandler = Objects.requireNonNull(timerHandler, "TimerHandler");
    }

    /**
     * Poll for expired timers, firing the callback supplied in the constructor.
     *
     * @param now current time.
     * @return the number of expired timers
     */
    @Override
    public int poll(final long now)
    {
        int expiredTimers = 0;
        final Timer[] timers = this.timers;
        final TimerHandler timerHandler = this.timerHandler;

        while (size > 0 && expiredTimers < POLL_LIMIT)
        {
            final Timer timer = timers[0];
            if (timer.deadline > now)
            {
                break;
            }

            if (!timerHandler.onTimerEvent(timer.correlationId))
            {
                break;
            }

            expiredTimers++;
            final int lastIndex = --size;
            final Timer lastTimer = timers[lastIndex];
            timers[lastIndex] = null;

            if (0 != lastIndex)
            {
                shiftDown(timers, lastIndex, 0, lastTimer);
            }

            timerByCorrelationId.remove(timer.correlationId);
            addToFreeList(timer);
        }

        return expiredTimers;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void scheduleTimerForCorrelationId(final long correlationId, final long deadline)
    {
        final Timer existingTimer = timerByCorrelationId.get(correlationId);
        if (null != existingTimer)
        {
            if (deadline < existingTimer.deadline)
            {
                existingTimer.deadline = deadline;
                shiftUp(timers, existingTimer.index, existingTimer);
            }
            else if (deadline > existingTimer.deadline)
            {
                existingTimer.deadline = deadline;
                shiftDown(timers, size, existingTimer.index, existingTimer);
            }
        }
        else
        {
            ensureCapacity(size + 1);

            final int index = size++;
            final Timer timer;
            if (freeTimerCount > 0)
            {
                final int freeIndex = --freeTimerCount;
                timer = freeTimers[freeIndex];
                freeTimers[freeIndex] = null;
                timer.reset(correlationId, deadline, index);
            }
            else
            {
                timer = new Timer(correlationId, deadline, index);
            }

            timerByCorrelationId.put(correlationId, timer);
            shiftUp(timers, index, timer);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean cancelTimerByCorrelationId(final long correlationId)
    {
        final Timer removedTimer = timerByCorrelationId.remove(correlationId);
        if (null == removedTimer)
        {
            return false;
        }

        final int lastIndex = --size;
        final Timer lastTimer = timers[lastIndex];
        timers[lastIndex] = null;

        if (lastIndex != removedTimer.index)
        {
            shiftDown(timers, lastIndex, removedTimer.index, lastTimer);
            if (timers[removedTimer.index] == lastTimer)
            {
                shiftUp(timers, removedTimer.index, lastTimer);
            }
        }

        addToFreeList(removedTimer);

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void snapshot(final TimerSnapshotTaker snapshotTaker)
    {
        final Timer[] timers = this.timers;
        for (int i = 0, size = this.size; i < size; i++)
        {
            final Timer timer = timers[i];
            snapshotTaker.snapshotTimer(timer.correlationId, timer.deadline);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void currentTime(final long now)
    {
    }

    void forEach(final Consumer<PriorityHeapTimerService.Timer> consumer)
    {
        final Timer[] timers = this.timers;
        for (int i = 0, size = this.size; i < size; i++)
        {
            consumer.accept(timers[i]);
        }
    }

    private static void shiftUp(final Timer[] timers, final int startIndex, final Timer timer)
    {
        int index = startIndex;
        while (index > 0)
        {
            final int prevIndex = (index - 1) >>> 1;
            final Timer prevTimer = timers[prevIndex];

            if (timer.deadline >= prevTimer.deadline)
            {
                break;
            }

            timers[index] = prevTimer;
            prevTimer.index = index;
            index = prevIndex;
        }

        timers[index] = timer;
        timer.index = index;
    }

    private static void shiftDown(final Timer[] timers, final int size, final int startIndex, final Timer timer)
    {
        final int half = size >>> 1;
        int index = startIndex;
        while (index < half)
        {
            int nextIndex = (index << 1) + 1;
            final int right = nextIndex + 1;
            Timer nextTimer = timers[nextIndex];

            if (right < size && nextTimer.deadline > timers[right].deadline)
            {
                nextIndex = right;
                nextTimer = timers[nextIndex];
            }

            if (timer.deadline < nextTimer.deadline)
            {
                break;
            }

            timers[index] = nextTimer;
            nextTimer.index = index;
            index = nextIndex;
        }

        timers[index] = timer;
        timer.index = index;
    }

    private void ensureCapacity(final int requiredCapacity)
    {
        final int currentCapacity = timers.length;

        if (requiredCapacity > currentCapacity)
        {
            if (requiredCapacity > ArrayUtil.MAX_CAPACITY)
            {
                throw new IllegalStateException("max capacity reached: " + ArrayUtil.MAX_CAPACITY);
            }

            if (EMPTY_TIMERS == timers)
            {
                timers = new Timer[MIN_CAPACITY];
                freeTimers = new Timer[MIN_CAPACITY];
            }
            else
            {
                int newCapacity = currentCapacity + (currentCapacity >> 1);
                if (newCapacity < 0 || newCapacity > ArrayUtil.MAX_CAPACITY)
                {
                    newCapacity = ArrayUtil.MAX_CAPACITY;
                }

                timers = Arrays.copyOf(timers, newCapacity);
                freeTimers = Arrays.copyOf(freeTimers, newCapacity);
            }
        }
    }

    private void addToFreeList(final Timer timer)
    {
        timer.reset(Aeron.NULL_VALUE, Aeron.NULL_VALUE, Aeron.NULL_VALUE);
        freeTimers[freeTimerCount++] = timer;
    }

    static final class Timer
    {
        long correlationId;
        long deadline;
        int index;

        Timer(final long correlationId, final long deadline, final int index)
        {
            reset(correlationId, deadline, index);
        }

        void reset(final long correlationId, final long deadline, final int index)
        {
            this.correlationId = correlationId;
            this.deadline = deadline;
            this.index = index;
        }

        @Override
        public String toString()
        {
            return "PriorityHeapTimerService.Timer{" +
                "correlationId=" + correlationId +
                ", deadline=" + deadline +
                ", index=" + index +
                '}';
        }
    }
}
