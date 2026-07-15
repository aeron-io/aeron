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

import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;

import static io.aeron.agent.EventConfiguration.BUFFER_LENGTH_DEFAULT;
import static io.aeron.agent.EventConfiguration.BUFFER_LENGTH_PROP_NAME;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.agrona.SystemUtil.getSizeAsInt;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

/**
 * Manages the lifecycle of the reader agent and its associated runner.
 */
public final class EventReaderManager
{
    /**
     * Configuration option for the log reader.
     */
    public static final String READER_CLASSNAME = "aeron.event.log.reader.classname";

    /**
     * Default reader agent class.
     */
    public static final String READER_CLASSNAME_DEFAULT
        = "io.aeron.agent.ModuleLoggerReaderAgent";

    private final ManyToOneRingBuffer ringBuffer;
    private Thread readerThread;
    private AgentRunner readerAgentRunner;
    private volatile Agent moduleLoggerReaderAgent;

    EventReaderManager()
    {
        ringBuffer = new ManyToOneRingBuffer(new UnsafeBuffer(allocateDirectAligned(
            getSizeAsInt(BUFFER_LENGTH_PROP_NAME, BUFFER_LENGTH_DEFAULT) + TRAILER_LENGTH, CACHE_LINE_LENGTH)));
    }

    /**
     * The ring buffer used for logging that will be read by the reader agent.
     *
     * @return the ring buffer.
     */
    public ManyToOneRingBuffer ringBuffer()
    {
        return ringBuffer;
    }

    /**
     * @param properties Restart the reader with the new properties.
     */
    void restart(final Properties properties)
    {
        readerThread = null;
        CloseHelper.close(readerAgentRunner);
        ringBuffer.unblock();
        start(properties);
    }

    /**
     * Start the reader agent and the buffer. Should only be called once
     *
     * @param properties to start the reader with.
     */
    public void start(final Properties properties)
    {
        final ArrayList<ModuleLogger> loggers = new ArrayList<>();
        try
        {

            for (final ModuleLogger componentLogger : ServiceLoader.load(ModuleLogger.class))
            {
                loggers.add(componentLogger);
            }


            moduleLoggerReaderAgent = newReaderAgent(properties, loggers);

            readerAgentRunner = new AgentRunner(
                new SleepingMillisIdleStrategy(1L),
                Throwable::printStackTrace,
                null,
                moduleLoggerReaderAgent);

            readerThread = new Thread(readerAgentRunner);
            readerThread.setName("event-log-reader");
            readerThread.setDaemon(true);
            readerThread.start();
        }
        catch (final Exception ex)
        {
            ex.printStackTrace(System.err);
        }
    }

    private Agent newReaderAgent(final Properties configOptions, final List<ModuleLogger> loggers)
    {
        try
        {
            final Class<?> aClass = Class.forName(
                configOptions.getProperty(READER_CLASSNAME, READER_CLASSNAME_DEFAULT));

            try
            {
                final Constructor<?> constructor = aClass.getDeclaredConstructor(Properties.class, List.class);
                return (Agent)constructor.newInstance(configOptions, loggers);
            }
            catch (final NoSuchMethodException ignore)
            {
            }

            return (Agent)aClass.getDeclaredConstructor().newInstance();
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * {@return the install logging agent}
     */
    public Agent agent()
    {
        return moduleLoggerReaderAgent;
    }
}
