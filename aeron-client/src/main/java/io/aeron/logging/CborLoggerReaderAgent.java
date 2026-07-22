package io.aeron.logging;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.util.List;

import static io.aeron.logging.EventConfiguration.EVENT_READER_FRAME_LIMIT;

public class CborLoggerReaderAgent implements Agent
{
    private final ManyToOneRingBuffer ringBuffer = EventConfiguration.eventReader().ringBuffer();
    private final CborDecode messageHandler = new CborDecode(List.of(new PrintLoggerEventCallback(System.out)));

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        try
        {
            return ringBuffer.read(messageHandler, EVENT_READER_FRAME_LIMIT);
        }
        catch (final Exception ex)
        {
            ex.printStackTrace(System.err);
        }

        return 0;
    }

    public String roleName()
    {
        return "aeron-event-log-reader";
    }
}
