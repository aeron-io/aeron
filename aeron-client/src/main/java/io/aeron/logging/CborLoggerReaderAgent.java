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
package io.aeron.logging;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.util.List;

import static io.aeron.logging.EventConfiguration.EVENT_READER_FRAME_LIMIT;

/**
 * Reader for CBOR messages in the {@link ManyToOneRingBuffer} held by {@link EventConfiguration#eventReader} that uses
 * {@link CborDecode} to decode the log events.
 */
public final class CborLoggerReaderAgent implements Agent
{
    private final ManyToOneRingBuffer ringBuffer = EventConfiguration.eventReader().ringBuffer();
    private final CborDecode messageHandler = new CborDecode(List.of(new PrintLoggerEventCallback(System.out)));

    /**
     * Default constructor.
     */
    public CborLoggerReaderAgent()
    {

    }

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

    /**
     * {@inheritDoc}}
     */
    public String roleName()
    {
        return "aeron-event-log-reader";
    }
}
