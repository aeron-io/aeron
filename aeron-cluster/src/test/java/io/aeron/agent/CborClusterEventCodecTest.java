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
package io.aeron.agent;

import io.aeron.cluster.ElectionState;
import io.aeron.test.Tests;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class CborClusterEventCodecTest
{
    private final ManyToOneRingBuffer ringBuffer = new ManyToOneRingBuffer(
        new UnsafeBuffer(new byte[64 * 1024 + TRAILER_LENGTH]));

    @Test
    void encodeDecodeLogElectionStateChange()
    {
        final ClusterEventLogger mockClusterEventLogger = mock(ClusterEventLogger.class);

        final CborClusterEventLogger cborClusterEventLogger = new CborClusterEventLogger(ringBuffer);
        final CborClusterEventDecode cborClusterEventDecode = new CborClusterEventDecode(
            List.of(mockClusterEventLogger));

        final CborDecode cborDecode = new CborDecode(List.of(cborClusterEventDecode));

        cborClusterEventLogger.logElectionStateChange(
            12,
            ElectionState.CANVASS,
            ElectionState.CLOSED,
            2342,
            23434,
            62354,
            2789345,
            87345,
            345345,
            2345,
            "invalid");

        while (0 == ringBuffer.read(cborDecode))
        {
            Tests.yield();
        }

        verify(mockClusterEventLogger).logElectionStateChange(
            12,
            ElectionState.CANVASS,
            ElectionState.CLOSED,
            2342,
            23434,
            62354,
            2789345,
            87345,
            345345,
            2345,
            "invalid");
    }
}