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

import org.agrona.Strings;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.IntFunction;

import static java.lang.System.err;
import static java.lang.System.lineSeparator;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.agrona.SystemUtil.getSizeAsInt;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

/**
 * Common configuration elements between event loggers and event reader side.
 */
public final class EventConfiguration
{
    /**
     * Event buffer length system property name.
     */
    public static final String BUFFER_LENGTH_PROP_NAME = "aeron.event.buffer.length";

    /**
     * Event Buffer default length (in bytes).
     */
    public static final int BUFFER_LENGTH_DEFAULT = 8 * 1024 * 1024;

    /**
     * Maximum length of an event in bytes.
     */
    public static final int MAX_EVENT_LENGTH = 4096 - lineSeparator().length();

    /**
     * Iteration limit for event reader loop.
     */
    public static final int EVENT_READER_FRAME_LIMIT = 20;

    /**
     * Ring Buffer to use for logging that will be read by {@link ConfigOption#READER_CLASSNAME}.
     */
    public static final ManyToOneRingBuffer EVENT_RING_BUFFER;

    static
    {
        EVENT_RING_BUFFER = new ManyToOneRingBuffer(new UnsafeBuffer(allocateDirectAligned(
            getSizeAsInt(BUFFER_LENGTH_PROP_NAME, BUFFER_LENGTH_DEFAULT) + TRAILER_LENGTH, CACHE_LINE_LENGTH)));

        try
        {
            final ArrayList<ModuleLogger> loggers = new ArrayList<>();
            for (final ModuleLogger componentLogger : ServiceLoader.load(ModuleLogger.class))
            {
                loggers.add(componentLogger);
            }

            final ModuleLoggerReaderAgent moduleLoggerReaderAgent = new ModuleLoggerReaderAgent(
                System.getProperties(), loggers);

            final AgentRunner readerAgentRunner = new AgentRunner(
                new SleepingMillisIdleStrategy(1L),
                Throwable::printStackTrace,
                null,
                moduleLoggerReaderAgent);

            final Thread thread = new Thread(readerAgentRunner);
            thread.setName("event-log-reader");
            thread.setDaemon(true);
            thread.start();
        }
        catch (final Exception ex)
        {
            ex.printStackTrace(System.err);
        }
    }

    private EventConfiguration()
    {
    }

    /**
     * Parse agent configuration.
     *
     * @param <E> type of the enum values.
     * @param eventCodeType for the enum.
     * @param eventCodes passed by the user.
     * @param specialEvents such as {@code admin} and {@code all}.
     * @param eventCodeById function to parse event id.
     * @param eventCodeByName function to parse event name.
     * @return set of enabled events.
     */
    public static <E extends Enum<E>> EnumSet<E> parseEventCodes(
        final Class<E> eventCodeType,
        final String eventCodes,
        final Map<String, EnumSet<E>> specialEvents,
        final IntFunction<E> eventCodeById,
        final Function<String, E> eventCodeByName)
    {
        if (Strings.isEmpty(eventCodes))
        {
            return EnumSet.noneOf(eventCodeType);
        }

        final EnumSet<E> eventCodeSet = EnumSet.noneOf(eventCodeType);
        final String[] codeIds = eventCodes.split(",");

        for (final String codeId : codeIds)
        {
            final EnumSet<E> specialCodes = specialEvents.get(codeId);
            if (null != specialCodes)
            {
                eventCodeSet.addAll(specialCodes);
            }
            else
            {
                E code = null;
                try
                {
                    code = eventCodeByName.apply(codeId);
                }
                catch (final IllegalArgumentException ignore)
                {
                }

                if (null == code)
                {
                    try
                    {
                        code = eventCodeById.apply(Integer.parseInt(codeId));
                    }
                    catch (final IllegalArgumentException ignore)
                    {
                    }
                }

                if (null != code)
                {
                    eventCodeSet.add(code);
                }
                else
                {
                    err.println("unknown event code: " + codeId);
                }
            }
        }

        return eventCodeSet;
    }
}
