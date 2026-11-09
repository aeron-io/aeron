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

import org.agrona.DeadlineTimerWheel;
import org.agrona.collections.Long2LongHashMap;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of the {@link TimerService} that is based on {@link DeadlineTimerWheel}.
 *
 * <p>
 * <b>Caveats</b>
 * <p>
 * Timers that expire in the same tick are not be ordered with one another. As ticks are
 * fairly coarse resolution normally, this means that some timers may expire out of order.
 * <p>
 * Upon Cluster restart the expiration order of the already expired timers is <em>not guaranteed</em>, i.e. timers with
 * the later deadlines might expire before timers with the earlier deadlines. To avoid this behavior use the
 * {@link PriorityHeapTimerService} instead.
 * <p>
 * <b>Note:</b> Not thread safe.
 */
final class WheelTimerService extends DeadlineTimerWheel implements DeadlineTimerWheel.TimerHandler, TimerService
{
    private final TimerService.TimerHandler timerHandler;
    private final Long2LongHashMap timerIdByCorrelationIdMap = new Long2LongHashMap(Long.MAX_VALUE);
    private final Long2LongHashMap correlationIdByTimerIdMap = new Long2LongHashMap(Long.MAX_VALUE);
    private boolean isAbort;

    WheelTimerService(
        final TimerService.TimerHandler timerHandler,
        final TimeUnit timeUnit,
        final long startTime,
        final long tickResolution,
        final int ticksPerWheel)
    {
        super(timeUnit, startTime, tickResolution, ticksPerWheel);
        this.timerHandler = Objects.requireNonNull(timerHandler);
    }

    @Override
    public int poll(final long now)
    {
        int expired = 0;
        isAbort = false;

        do
        {
            expired += poll(now, this, POLL_LIMIT);

            if (isAbort)
            {
                break;
            }
        }
        while (expired < POLL_LIMIT && currentTickTime() < now);

        return expired;
    }

    @Override
    public boolean onTimerExpiry(final TimeUnit timeUnit, final long now, final long timerId)
    {
        final long correlationId = correlationIdByTimerIdMap.get(timerId);

        if (timerHandler.onTimerEvent(correlationId))
        {
            correlationIdByTimerIdMap.remove(timerId);
            timerIdByCorrelationIdMap.remove(correlationId);

            return true;
        }
        else
        {
            isAbort = true;

            return false;
        }
    }

    @Override
    public void scheduleTimerForCorrelationId(final long correlationId, final long deadline)
    {
        cancelTimerByCorrelationId(correlationId);

        final long timerId = scheduleTimer(deadline);
        timerIdByCorrelationIdMap.put(correlationId, timerId);
        correlationIdByTimerIdMap.put(timerId, correlationId);
    }

    @Override
    public boolean cancelTimerByCorrelationId(final long correlationId)
    {
        final long timerId = timerIdByCorrelationIdMap.remove(correlationId);
        if (Long.MAX_VALUE != timerId)
        {
            cancelTimer(timerId);
            correlationIdByTimerIdMap.remove(timerId);

            return true;
        }

        return false;
    }

    @Override
    public void snapshot(final TimerSnapshotTaker snapshotTaker)
    {
        final Long2LongHashMap.EntryIterator iter = timerIdByCorrelationIdMap.entrySet().iterator();

        while (iter.hasNext())
        {
            iter.next();

            final long correlationId = iter.getLongKey();
            final long deadline = deadline(iter.getLongValue());

            snapshotTaker.snapshotTimer(correlationId, deadline);
        }
    }

    @Override
    public void currentTime(final long now)
    {
        currentTickTime(now);
    }
}
