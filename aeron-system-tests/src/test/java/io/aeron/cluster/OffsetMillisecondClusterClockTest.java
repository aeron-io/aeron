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

import org.agrona.collections.MutableLong;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class OffsetMillisecondClusterClockTest
{
    @Test
    void shouldOffsetDelegateTime()
    {
        final MutableLong time = new MutableLong(1000);
        final OffsetMillisecondClusterClock clock = new OffsetMillisecondClusterClock(time::get);
        assertEquals(TimeUnit.MILLISECONDS, clock.timeUnit());
        assertEquals(1000, clock.time());
        clock.addOffset(7);
        assertEquals(1007, clock.time());
        time.increment();
        assertEquals(1008, clock.time());
        clock.addOffset(3);
        time.increment();
        assertEquals(1012, clock.time());
    }
}
