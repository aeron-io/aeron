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
package io.aeron.cluster.service;

import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;

import static io.aeron.AeronCounters.ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_CONTROL_TOGGLE_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_RECOVERY_STATE_TYPE_ID;
import static io.aeron.AeronCounters.DRIVER_HEARTBEAT_TYPE_ID;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.agrona.concurrent.status.CountersReader.COUNTER_LENGTH;
import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ClusterCountersTest
{
    private static final int NUM_COUNTERS = 4;
    private final UnsafeBuffer metaDataBuffer =
        new UnsafeBuffer(allocateDirectAligned(METADATA_LENGTH * NUM_COUNTERS, 16));
    private final UnsafeBuffer valuesBuffer =
        new UnsafeBuffer(allocateDirectAligned(COUNTER_LENGTH * NUM_COUNTERS, 16));
    private final CachedEpochClock epochClock = new CachedEpochClock();
    private final CountersManager countersManager = new CountersManager(
        metaDataBuffer,
        valuesBuffer,
        StandardCharsets.US_ASCII,
        epochClock,
        0);
    private final CountersReader countersReader = new CountersReader(metaDataBuffer, valuesBuffer);
    private final UnsafeBuffer scratchBuffer = new UnsafeBuffer(new byte[128]);

    @ParameterizedTest
    @ValueSource(ints = { NUM_COUNTERS - 1, NUM_COUNTERS - 2, 1, 0 })
    void shouldFindCounter(final int numOtherCounters)
    {
        scratchBuffer.putStringWithoutLengthAscii(0, "test");
        for (int i = 0; i < numOtherCounters; i++)
        {
            countersManager.allocate(DRIVER_HEARTBEAT_TYPE_ID, null, 0, 0, scratchBuffer, 0, 4);
        }

        final int clusterId = 5;
        final String label = "test";
        scratchBuffer.putInt(0, clusterId, LITTLE_ENDIAN);
        scratchBuffer.putStringWithoutLengthAscii(SIZE_OF_INT, label);

        final int expectedCounterId = countersManager.allocate(
            CLUSTER_CONTROL_TOGGLE_TYPE_ID,
            scratchBuffer,
            0,
            SIZE_OF_INT,
            scratchBuffer,
            SIZE_OF_INT,
            label.length());

        assertEquals(
            expectedCounterId, ClusterCounters.find(countersReader, CLUSTER_CONTROL_TOGGLE_TYPE_ID, clusterId));
    }

    @Test
    void findIsANoOpIfNoCountersAllocated()
    {
        assertEquals(NULL_COUNTER_ID, ClusterCounters.find(countersReader, CLUSTER_RECOVERY_STATE_TYPE_ID, 0));
    }

    @Test
    void findMustMatchOnTypeIdAndClusterId()
    {
        final String label = "test";
        scratchBuffer.putInt(0, 42, LITTLE_ENDIAN);
        scratchBuffer.putStringWithoutLengthAscii(SIZE_OF_INT, label);

        countersManager.allocate(
            ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID,
            scratchBuffer,
            0,
            SIZE_OF_INT,
            scratchBuffer,
            SIZE_OF_INT,
            label.length());

        final int expected = countersManager.allocate(
            CLUSTER_RECOVERY_STATE_TYPE_ID,
            scratchBuffer,
            0,
            SIZE_OF_INT,
            scratchBuffer,
            SIZE_OF_INT,
            label.length());

        assertEquals(NULL_COUNTER_ID, ClusterCounters.find(countersReader, CLUSTER_RECOVERY_STATE_TYPE_ID, 19));
        assertEquals(expected, ClusterCounters.find(countersReader, CLUSTER_RECOVERY_STATE_TYPE_ID, 42));
    }
}
