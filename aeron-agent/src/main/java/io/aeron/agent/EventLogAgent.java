/*
 * Copyright 2016 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.agent;

import io.aeron.driver.EventLog;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.TypeValidation;
import net.bytebuddy.utility.JavaModule;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SleepingIdleStrategy;

import javax.management.*;
import java.lang.instrument.Instrumentation;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static net.bytebuddy.asm.Advice.to;
import static net.bytebuddy.matcher.ElementMatchers.*;

public class EventLogAgent
{
    private static final long SLEEP_PERIOD_NS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final EventLogReaderAgent EVENT_LOG_READER_AGENT = new EventLogReaderAgent();

    private static final AgentRunner EVENT_LOG_READER_AGENT_RUNNER =
        new AgentRunner(new SleepingIdleStrategy(SLEEP_PERIOD_NS), EventLogAgent::errorHandler, null, EVENT_LOG_READER_AGENT);

    private static final Thread EVENT_LOG_READER_THREAD = new Thread(EVENT_LOG_READER_AGENT_RUNNER);

    private static volatile ResettableClassFileTransformer logTransformer;
    private static volatile Instrumentation instrumentation;

    private static final AgentBuilder.Listener LISTENER = new AgentBuilder.Listener()
    {
        public void onTransformation(
            final TypeDescription typeDescription,
            final ClassLoader classLoader,
            final JavaModule module,
            final DynamicType dynamicType)
        {
            System.out.format("TRANSFORM %s%n", typeDescription.getName());
        }

        public void onIgnored(final TypeDescription typeDescription, final ClassLoader classLoader, final JavaModule module)
        {
        }

        public void onError(
            final String typeName, final ClassLoader classLoader, final JavaModule module, final Throwable throwable)
        {
            System.out.format("ERROR %s%n", typeName);
            throwable.printStackTrace(System.out);
        }

        public void onComplete(final String typeName, final ClassLoader classLoader, final JavaModule module)
        {
        }
    };

    private static void errorHandler(final Throwable throwable)
    {
    }

    private static void agent(final boolean redefine, final Instrumentation instrumentation)
    {
        if (EventConfiguration.ENABLED_EVENT_CODES != 0)
        {
            /*
             * Intercept based on enabled events:
             *  SenderProxy
             *  ReceiverProxy
             *  ClientProxy
             *  DriverConductor (onClientCommand)
             *  SendChannelEndpoint
             *  ReceiveChannelEndpoint
             */

            logTransformer = new AgentBuilder.Default(new ByteBuddy().with(TypeValidation.DISABLED))
                .with(LISTENER)
                .disableClassFormatChanges()
                .with(redefine ? AgentBuilder.RedefinitionStrategy.RETRANSFORMATION : AgentBuilder.RedefinitionStrategy.DISABLED)
                .type(nameEndsWith("DriverConductor"))
                .transform((builder, typeDescription, classLoader) ->
                    builder
                        .visit(to(CmdInterceptor.class).on(named("onClientCommand")))
                        .visit(to(CleanupInterceptor.DriverConductorInterceptor.CleanupImage.class)
                            .on(named("cleanupImage")))
                        .visit(to(CleanupInterceptor.DriverConductorInterceptor.CleanupPublication.class)
                            .on(named("cleanupPublication")))
                        .visit(to(CleanupInterceptor.DriverConductorInterceptor.CleanupSubscriptionLink.class)
                            .on(named("cleanupSubscriptionLink"))))
                .type(nameEndsWith("ClientProxy"))
                .transform((builder, typeDescription, classLoader) ->
                    builder.visit(to(CmdInterceptor.class).on(named("transmit"))))
                .type(nameEndsWith("SenderProxy"))
                .transform((builder, typeDescription, classLoader) ->
                    builder
                        .visit(to(ChannelEndpointInterceptor.SenderProxyInterceptor.RegisterSendChannelEndpoint.class)
                            .on(named("registerSendChannelEndpoint")))
                        .visit(to(ChannelEndpointInterceptor.SenderProxyInterceptor.CloseSendChannelEndpoint.class)
                            .on(named("closeSendChannelEndpoint"))))
                .type(nameEndsWith("ReceiverProxy"))
                .transform((builder, typeDescription, classLoader) ->
                    builder
                        .visit(to(ChannelEndpointInterceptor.ReceiverProxyInterceptor.RegisterReceiveChannelEndpoint.class)
                            .on(named("registerReceiveChannelEndpoint")))
                        .visit(to(ChannelEndpointInterceptor.ReceiverProxyInterceptor.CloseReceiveChannelEndpoint.class)
                            .on(named("closeReceiveChannelEndpoint"))))
                .type(inheritsAnnotation(EventLog.class))
                .transform((builder, typeDescription, classLoader) ->
                    builder
                        .visit(to(ChannelEndpointInterceptor.SendChannelEndpointInterceptor.Presend.class)
                            .on(named("presend")))
                        .visit(to(ChannelEndpointInterceptor.ReceiveChannelEndpointInterceptor.SendTo.class)
                            .on(named("sendTo")))
                        .visit(to(ChannelEndpointInterceptor.ReceiveChannelEndpointInterceptor.Dispatch.class)
                            .on(named("dispatch"))))
                .installOn(instrumentation);

            EVENT_LOG_READER_THREAD.setName("event log reader");
            EVENT_LOG_READER_THREAD.setDaemon(true);
            EVENT_LOG_READER_THREAD.start();
        }
    }

    private static void init(final String agentArgs, final Instrumentation instrumentation)
    {
        EventLogAgent.instrumentation = instrumentation;
        if (agentArgs != null && agentArgs.contains("mbean"))
        {
            try
            {
                final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                mBeanServer.registerMBean(new EventLogControl(), new ObjectName("io.aeron:type=EventLogControl"));
            }
            catch (final Exception exception)
            {
                System.out.println("Could not register MXBean");
                exception.printStackTrace(System.out);
            }
        }

    }

    public static void premain(final String agentArgs, final Instrumentation instrumentation)
    {
        init(agentArgs, instrumentation);
        agent(false, instrumentation);
    }

    public static void agentmain(final String agentArgs, final Instrumentation instrumentation)
    {
        init(agentArgs, instrumentation);
        agent(true, instrumentation);
    }

    static synchronized boolean enableLogging()
    {
        if (logTransformer == null)
        {
            agent(true, instrumentation);
            return true;
        }
        else
        {
            return false;
        }
    }

    static synchronized boolean disableLogging()
    {
        if (logTransformer != null)
        {
            final ResettableClassFileTransformer.Reset reset = logTransformer.reset(instrumentation,
                    AgentBuilder.RedefinitionStrategy.RETRANSFORMATION);
            for (final Map.Entry<Class<?>, Throwable> error : reset.getErrors().entrySet())
            {
                System.out.format("Could not reset %s%n", error.getKey());
                error.getValue().printStackTrace(System.out);
            }
            logTransformer = null;
            return true;
        }
        else
        {
            return false;
        }
    }

    static boolean isLogging()
    {
        return logTransformer != null;
    }
}
