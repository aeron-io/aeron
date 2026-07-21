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