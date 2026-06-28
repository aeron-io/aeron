/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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
import org.agrona.concurrent.EpochClock;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

final class OffsetMillisecondClusterClock implements ClusterClock
{
    private static final VarHandle OFFSET_VH;

    static
    {
        try
        {
            OFFSET_VH = MethodHandles.lookup()
                .findVarHandle(OffsetMillisecondClusterClock.class, "offset", long.class);
        }
        catch (final ReflectiveOperationException ex)
        {
            throw new ExceptionInInitializerError(ex);
        }
    }

    private final EpochClock epochClock;
    @SuppressWarnings("unused")
    private volatile long offset;

    OffsetMillisecondClusterClock(final EpochClock epochClock)
    {
        this.epochClock = epochClock;
    }

    public long time()
    {
        return epochClock.time() + (long)OFFSET_VH.getAcquire(this);
    }

    void addOffset(final long deltaMs)
    {
        OFFSET_VH.getAndAddRelease(this, deltaMs);
    }
}
