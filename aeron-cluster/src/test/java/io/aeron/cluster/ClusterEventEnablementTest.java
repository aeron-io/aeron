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
package io.aeron.cluster;

import io.aeron.CommonContext;
import io.aeron.agent.ClusterEventCode;
import io.aeron.agent.EventConfiguration;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.LoggingTest;
import io.aeron.test.Tests;
import io.aeron.test.agent.CountingEventReaderAgent;
import org.agrona.concurrent.Agent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.aeron.agent.ClusterEventCode.DYNAMIC_JOIN_STATE_CHANGE_UNUSED;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(InterruptingTestCallback.class)
@LoggingTest(
    readerClassname = CountingEventReaderAgent.class,
    enabledEventsKey = "aeron.event.cluster.log",
    enabledEvents = "all")
public class ClusterEventEnablementTest
{
    private final Map<Class<?>, Object> defaultValues = new HashMap<>();

    @BeforeEach
    void setUp()
    {
        defaultValues.put(String.class, "");
        defaultValues.put(Long.TYPE, 0L);
        defaultValues.put(Integer.TYPE, 0);
        defaultValues.put(Short.TYPE, (short)0);
        defaultValues.put(Boolean.TYPE, false);
        defaultValues.put(Enum.class, TimeUnit.MINUTES);
        defaultValues.put(TimeUnit.class, TimeUnit.MINUTES);
        defaultValues.put(CloseReason.class, CloseReason.CLIENT_ACTION);
    }

    @Test
    @InterruptAfter(2)
    void shouldLogConfiguredEvents()
    {
        final String eventConfig = System.getProperty(CommonContext.CLUSTER_EVENT_LOG);
        final String disableConfig = System.getProperty("aeron.event.cluster.log.disable");

        final List<ClusterEventCode> disabledEvents;
        if (null != disableConfig)
        {
            disabledEvents = Arrays.stream(disableConfig.split(","))
                .map(ClusterEventCode::valueOf)
                .toList();
        }
        else
        {
            disabledEvents = Collections.emptyList();
        }

        final List<ClusterEventCode> enabledEvents;
        if ("all".equalsIgnoreCase(eventConfig))
        {
            enabledEvents = Arrays.stream(ClusterEventCode.values())
                .filter((c) -> !disabledEvents.contains(c))
                .filter((c) -> DYNAMIC_JOIN_STATE_CHANGE_UNUSED != c)
                .toList();
        }
        else if ("none".equalsIgnoreCase(eventConfig))
        {
            enabledEvents = Collections.emptyList();
        }
        else if (null != eventConfig)
        {
            enabledEvents = Arrays.stream(eventConfig.split(","))
                .map(ClusterEventCode::valueOf)
                .filter((c) -> !disabledEvents.contains(c))
                .toList();
        }
        else
        {
            enabledEvents = Collections.emptyList();
        }

        final Agent agent = EventConfiguration.eventReader().agent();
        Assertions.assertInstanceOf(CountingEventReaderAgent.class, agent);
        final CountingEventReaderAgent countingAgent = (CountingEventReaderAgent)agent;

        callGeneralLogMethods();

        for (final ClusterEventCode code : enabledEvents)
        {
            validateLogSend(code, countingAgent);
        }

        if (disabledEvents.isEmpty())
        {
            return;
        }

        final ClusterEventCode[] codes = ClusterEventCode.values();
        final long deadlineMs = System.currentTimeMillis() + 1_000L;
        while (System.currentTimeMillis() < deadlineMs)
        {
            for (final ClusterEventCode code : codes)
            {
                if (disabledEvents.contains(code))
                {
                    validateLogNotSent(code, countingAgent);
                }
            }
        }
    }

    private void callGeneralLogMethods()
    {
        final List<Method> logMethods = Arrays.stream(ClusterLog.class.getDeclaredMethods())
            .filter((m) -> m.getName().startsWith("log"))
            .filter((m) -> 0 != (m.getModifiers() & Modifier.STATIC))
            .toList();

        for (final Method logMethod : logMethods)
        {
            callLogMethod(logMethod);
        }
    }

    private void callLogMethod(final Method logMethod)
    {
        final Parameter[] parameters = logMethod.getParameters();

        final Object[] params = new Object[parameters.length];

        for (int i = 0; i < parameters.length; i++)
        {
            final Parameter parameter = parameters[i];
            params[i] = requireNonNull(defaultValues.get(parameter.getType()));
        }

        try
        {
            logMethod.invoke(null, params);
        }
        catch (final Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void validateLogSend(final ClusterEventCode code, final CountingEventReaderAgent countingAgent)
    {
        final Supplier<String> msg = () -> "Did not see event: " + code;
        while (0 == countingAgent.countClusterEvent(code.toEventCodeId()))
        {
            Tests.sleep(1, msg);
        }
    }

    private static void validateLogNotSent(final ClusterEventCode code, final CountingEventReaderAgent countingAgent)
    {
        assertEquals(0, countingAgent.countClusterEvent(code.toEventCodeId()), code.toString());
    }
}
