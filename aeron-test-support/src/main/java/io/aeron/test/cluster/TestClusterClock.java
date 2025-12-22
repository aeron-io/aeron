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
package io.aeron.test.cluster;

import io.aeron.cluster.service.ClusterClock;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TestClusterClock implements ClusterClock, NanoClock
{
    private final AtomicLong tick = new AtomicLong();
    private final TimeUnit timeUnit;

    public TestClusterClock()
    {
        this(TimeUnit.MILLISECONDS);
    }

    public TestClusterClock(final TimeUnit timeUnit)
    {
        this.timeUnit = timeUnit;
    }

    public TimeUnit timeUnit()
    {
        return timeUnit;
    }

    public long time()
    {
        return tick.get();
    }

    public long nanoTime()
    {
        return timeUnit.toNanos(tick.get());
    }

    public long timeMillis()
    {
        return timeUnit.toMillis(tick.get());
    }

    public long timeMicros()
    {
        return timeUnit.toMicros(tick.get());
    }

    public long timeNanos()
    {
        return timeUnit.toNanos(tick.get());
    }

    public long convertToNanos(final long time)
    {
        return timeUnit.toNanos(time);
    }

    public void update(final long tick, final TimeUnit tickTimeUnit)
    {
        this.tick.set(timeUnit.convert(tick, tickTimeUnit));
    }

    public long increment(final long tick, final TimeUnit tickTimeUnit)
    {
        return this.tick.addAndGet(timeUnit.convert(tick, tickTimeUnit));
    }

    public long increment(final long tick)
    {
        return increment(tick, timeUnit());
    }

    public EpochClock asEpochClock()
    {
        return this::timeMillis;
    }
}
