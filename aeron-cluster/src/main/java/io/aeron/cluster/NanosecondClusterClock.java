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

import io.aeron.cluster.service.ClusterClock;
import org.agrona.concurrent.HighResolutionClock;

import java.util.concurrent.TimeUnit;

/**
 * A {@link ClusterClock} implemented by calling {@link HighResolutionClock#epochNanos()}.
 */
public class NanosecondClusterClock implements ClusterClock
{
    /**
     * Default constructor.
     */
    public NanosecondClusterClock()
    {
    }

    /**
     * {@inheritDoc}
     */
    public TimeUnit timeUnit()
    {
        return TimeUnit.NANOSECONDS;
    }

    /**
     * {@inheritDoc}
     */
    public long time()
    {
        return HighResolutionClock.epochNanos();
    }

    /**
     * {@inheritDoc}
     */
    public long timeMillis()
    {
        return System.currentTimeMillis();
    }

    /**
     * {@inheritDoc}
     */
    public long timeMicros()
    {
        return HighResolutionClock.epochMicros();
    }

    /**
     * {@inheritDoc}
     */
    public long timeNanos()
    {
        return HighResolutionClock.epochNanos();
    }

    /**
     * {@inheritDoc}
     */
    public long convertToNanos(final long time)
    {
        return time;
    }
}
