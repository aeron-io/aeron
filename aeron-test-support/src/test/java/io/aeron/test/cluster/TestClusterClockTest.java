/*
 * Copyright 2025 Adaptive Financial Consulting Limited.
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

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestClusterClockTest
{
    @Test
    void testMillisecondClock()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        assertEquals(TimeUnit.MILLISECONDS, clock.timeUnit());

        clock.update(1765290339569L, TimeUnit.MILLISECONDS);
        assertEquals(1765290339569L, clock.time());
        assertEquals(1765290339569L, clock.timeMillis());
        assertEquals(1765290339569L, clock.asEpochClock().time());
        assertEquals(1765290339569000L, clock.timeMicros());
        assertEquals(1765290339569000000L, clock.timeNanos());
        assertEquals(1765290339569000000L, clock.nanoTime());

        clock.update(1765383800L, TimeUnit.SECONDS);
        assertEquals(1765383800000L, clock.time());

        clock.increment(3, TimeUnit.SECONDS);
        assertEquals(1765383803000L, clock.time());

        clock.increment(1);
        assertEquals(1765383803001L, clock.time());
    }

    @Test
    void testNanosecondClock()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.NANOSECONDS);
        assertEquals(TimeUnit.NANOSECONDS, clock.timeUnit());

        clock.update(1765383558894627344L, TimeUnit.NANOSECONDS);
        assertEquals(1765383558894627344L, clock.time());
        assertEquals(1765383558894627344L, clock.timeNanos());
        assertEquals(1765383558894627344L, clock.nanoTime());
        assertEquals(1765383558894627L, clock.timeMicros());
        assertEquals(1765383558894L, clock.timeMillis());
        assertEquals(1765383558894L, clock.asEpochClock().time());

        clock.update(1765384128532L, TimeUnit.MILLISECONDS);
        assertEquals(1765384128532000000L, clock.time());

        clock.increment(123, TimeUnit.MICROSECONDS);
        assertEquals(1765384128532123000L, clock.time());

        clock.increment(1);
        assertEquals(1765384128532123001L, clock.time());
    }
}
