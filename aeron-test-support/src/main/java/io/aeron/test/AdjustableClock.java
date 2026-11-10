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
package io.aeron.test;

import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.SystemNanoClock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class AdjustableClock implements NanoClock, EpochClock
{
    private final AtomicLong nanoCallCount = new AtomicLong(0);
    private final AtomicLong epochCallCount = new AtomicLong(0);
    private final EpochClock baseEpochClock = SystemEpochClock.INSTANCE;
    private final NanoClock baseNanoClock = SystemNanoClock.INSTANCE;
    private final AtomicLong offsetNs = new AtomicLong(0);

    @Override
    public long time()
    {
        return baseEpochClock.time() + TimeUnit.NANOSECONDS.toMillis(offsetNs.get());
    }

    @Override
    public long nanoTime()
    {
        nanoCallCount.incrementAndGet();
        return baseNanoClock.nanoTime() + offsetNs.get();
    }

    public void incrementOffsetNs(final long incrementNs)
    {
        epochCallCount.incrementAndGet();
        offsetNs.addAndGet(incrementNs);
    }

    public long epochCallCount()
    {
        return epochCallCount.get();
    }

    public long nanoCallCount()
    {
        return nanoCallCount.get();
    }

    public void slewTimeDelta(final long timeoutNs, final long incrementNs)
    {
        final long nowMs = time();
        while (time() < (nowMs + TimeUnit.NANOSECONDS.toMillis(timeoutNs)))
        {
            incrementOffsetNs(incrementNs);
            final long count = nanoCallCount();
            while (nanoCallCount() < count + 2)
            {
                Tests.yieldingIdle("count=" + count + ", nanoCallCount=" + nanoCallCount());
            }
        }
    }
}
