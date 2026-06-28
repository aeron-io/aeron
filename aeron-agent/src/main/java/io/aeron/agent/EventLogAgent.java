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

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.TypeValidation;
import net.bytebuddy.utility.JavaModule;
import org.agrona.CloseHelper;
import org.agrona.Strings;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static io.aeron.agent.ConfigOption.*;
import static io.aeron.agent.EventConfiguration.EVENT_RING_BUFFER;

/**
 * A Java agent which when attached to a JVM will weave byte code to intercept events as defined by
 * {@link DriverEventCode}. Events are recorded to an in-memory {@link org.agrona.concurrent.ringbuffer.RingBuffer}
 * which is consumed and appended asynchronous to a log as defined by the class {@link #READER_CLASSNAME_PROP_NAME}
 * which defaults to {@link EventLogReaderAgent}.
 */
public final class EventLogAgent
{
    /**
     * Event reader {@link Agent} which consumes the {@link EventConfiguration#EVENT_RING_BUFFER} to output log events.
     */
    public static final String READER_CLASSNAME_PROP_NAME = READER_CLASSNAME;
    /**
     * Default value for the log reader agent.
     */
    public static final String READER_CLASSNAME_DEFAULT = "io.aeron.agent.EventLogReaderAgent";

    private static final long SLEEP_PERIOD_MS = 1L;

    private static List<ComponentLogger> loggers;
    private static AgentRunner readerAgentRunner;
    private static Instrumentation instrumentation;
    private static ResettableClassFileTransformer logTransformer;
    private static Thread thread;

    private EventLogAgent()
    {
    }

    /**
     * Premain method to run before the main method of the application.
     *
     * @param agentArgs       which are ignored.
     * @param instrumentation for applying to the agent.
     */
    public static void premain(final String agentArgs, final Instrumentation instrumentation)
    {
        startLogging(instrumentation, ConfigOption.fromSystemProperties());
    }

    /**
     * Agent main method for dynamic attach.
     *
     * @param agentArgs       containing configuration options or command to stop.
     * @param instrumentation for applying to the agent.
     */
    public static void agentmain(final String agentArgs, final Instrumentation instrumentation)
    {
        if (STOP_COMMAND.equals(agentArgs))
        {
            stopLogging();
        }
        else
        {
            final Map<String, String> configOptions = Strings.isEmpty(agentArgs) ? fromSystemProperties() :
                parseAgentArgs(agentArgs);
            startLogging(instrumentation, configOptions);
        }
    }

    /**
     * Remove the transformer and close the agent runner for the event log reader.
     */
    @Deprecated
    public static void removeTransformer()
    {
        stopLogging();
    }

    /**
     * Remove the transformer and close the agent runner for the event log reader.
     */
    public static synchronized void stopLogging()
    {
        if (logTransformer != null)
        {
            logTransformer.reset(instrumentation, AgentBuilder.RedefinitionStrategy.RETRANSFORMATION);
            instrumentation = null;
            logTransformer = null;
            thread = null;

            for (final ComponentLogger logger : loggers)
            {
                logger.reset();
            }
            loggers = null;
            EVENT_RING_BUFFER.unblock();

            CloseHelper.close(readerAgentRunner);
            readerAgentRunner = null;
        }
    }

    private static synchronized void startLogging(
        final Instrumentation instrumentation, final Map<String, String> configOptions)
    {
        if (null != logTransformer)
        {
            throw new IllegalStateException("agent already instrumented");
        }

        final ArrayList<ComponentLogger> loggers = new ArrayList<>();
        for (final ComponentLogger componentLogger : ServiceLoader.load(ComponentLogger.class))
        {
            loggers.add(componentLogger);
        }

        AgentBuilder agentBuilder = new AgentBuilder.Default(new ByteBuddy()
            .with(TypeValidation.DISABLED))
            .disableClassFormatChanges()
            .with(new AgentBuilderListener())
            .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
            .with(AgentBuilder.RedefinitionStrategy.DiscoveryStrategy.Reiterating.INSTANCE);

        final AgentBuilder initialAgentBuilder = agentBuilder;

        for (final ComponentLogger componentLogger : loggers)
        {
            agentBuilder = componentLogger.addInstrumentation(agentBuilder, configOptions);
        }

        if (initialAgentBuilder == agentBuilder)
        {
            return; // no log events configured
        }

        EventLogAgent.instrumentation = instrumentation;

        logTransformer = agentBuilder.installOn(instrumentation);

        EventLogAgent.loggers = loggers;

        readerAgentRunner = new AgentRunner(
            new SleepingMillisIdleStrategy(SLEEP_PERIOD_MS),
            Throwable::printStackTrace,
            null,
            newReaderAgent(configOptions, loggers));

        thread = new Thread(readerAgentRunner);
        thread.setName("event-log-reader");
        thread.setDaemon(true);
        thread.start();
    }

    private static Agent newReaderAgent(final Map<String, String> configOptions, final List<ComponentLogger> loggers)
    {
        try
        {
            final Class<?> aClass = Class.forName(
                configOptions.getOrDefault(READER_CLASSNAME, READER_CLASSNAME_DEFAULT));

            try
            {
                final Constructor<?> constructor = aClass.getDeclaredConstructor(Map.class, List.class);
                return (Agent)constructor.newInstance(configOptions, loggers);
            }
            catch (final NoSuchMethodException ignore)
            {
            }

            try
            {
                final Constructor<?> constructor = aClass.getDeclaredConstructor(String.class, List.class);
                return (Agent)constructor.newInstance(configOptions.get(LOG_FILENAME), loggers);
            }
            catch (final NoSuchMethodException ignore)
            {
            }

            try
            {
                final Constructor<?> constructor = aClass.getDeclaredConstructor(String.class);
                return (Agent)constructor.newInstance(configOptions.get(LOG_FILENAME));
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
}

final class AgentBuilderListener implements AgentBuilder.Listener
{
    public void onDiscovery(
        final String typeName,
        final ClassLoader classLoader,
        final JavaModule javaModule,
        final boolean loaded)
    {
    }

    public void onTransformation(
        final TypeDescription typeDescription,
        final ClassLoader classLoader,
        final JavaModule javaModule,
        final boolean loaded,
        final DynamicType dynamicType)
    {
    }

    public void onIgnored(
        final TypeDescription typeDescription,
        final ClassLoader classLoader,
        final JavaModule javaModule,
        final boolean loaded)
    {
    }

    public void onError(
        final String typeName,
        final ClassLoader classLoader,
        final JavaModule javaModule,
        final boolean loaded,
        final Throwable throwable)
    {
        System.err.println("ERROR " + typeName);
        throwable.printStackTrace(System.err);
    }

    public void onComplete(
        final String typeName,
        final ClassLoader classLoader,
        final JavaModule javaModule,
        final boolean loaded)
    {
    }
}
