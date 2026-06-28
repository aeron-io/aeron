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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A {@link ClusterClock} implemented by calling {@link System#currentTimeMillis()}.
 */
public class MillisecondClusterClock implements ClusterClock
{
    /**
     * Default constructor.
     */
    public MillisecondClusterClock()
    {
    }

    /**
     * {@inheritDoc}
     */
    public long time()
    {
        return System.currentTimeMillis();
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
        return MILLISECONDS.toMicros(System.currentTimeMillis());
    }

    /**
     * {@inheritDoc}
     */
    public long timeNanos()
    {
        return MILLISECONDS.toNanos(System.currentTimeMillis());
    }

    /**
     * {@inheritDoc}
     */
    public long convertToNanos(final long time)
    {
        return MILLISECONDS.toNanos(time);
    }
}
