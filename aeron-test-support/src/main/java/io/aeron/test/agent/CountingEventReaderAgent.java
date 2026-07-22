/*
 * Copyright 2014-2026 Real Logic Limited.
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
package io.aeron.test.agent;

import io.aeron.logging.EventCodeType;
import io.aeron.logging.EventConfiguration;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class CountingEventReaderAgent implements Agent
{
    private final ManyToOneRingBuffer ringBuffer = EventConfiguration.eventReader().ringBuffer();
    private final ConcurrentHashMap<Integer, AtomicLong> eventCounts = new ConcurrentHashMap<>();
    private final AtomicLong zeroCount = new AtomicLong(0);

    public int doWork() throws Exception
    {
        return ringBuffer.read(this::onMessage, 10);
    }

    public String roleName()
    {
        return "test-counting-event-reader";
    }

    private long count(final int eventCodeId, final EventCodeType eventCodeType)
    {
        final int msgTypeId = eventCodeType.getTypeCode() << 16 | (eventCodeId & 0xFFFF);
        return eventCounts.getOrDefault(msgTypeId, zeroCount).get();
    }

    public long countDriverEvent(final int driverEventCodeId)
    {
        return count(driverEventCodeId, EventCodeType.DRIVER);
    }

    public long countArchiveEvent(final int archiveEventCodeId)
    {
        return count(archiveEventCodeId, EventCodeType.ARCHIVE);
    }

    public long countClusterEvent(final int clusterEventCodeId)
    {
        return count(clusterEventCodeId, EventCodeType.CLUSTER);
    }

    private void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
    {
        eventCounts.computeIfAbsent(msgTypeId, (ignore) -> new AtomicLong(0)).incrementAndGet();
    }
}
