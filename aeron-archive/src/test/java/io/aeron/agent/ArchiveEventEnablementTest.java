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
package io.aeron.agent;

import io.aeron.archive.ArchiveLog;
import io.aeron.archive.codecs.MessageHeaderEncoder;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.LoggingTest;
import io.aeron.test.Tests;
import io.aeron.test.agent.CountingEventReaderAgent;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;
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

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(InterruptingTestCallback.class)
@LoggingTest(
    readerClassname = CountingEventReaderAgent.class,
    enabledEventsKey = "aeron.event.archive.log",
    enabledEvents = "all")
public class ArchiveEventEnablementTest
{
    private final Map<Class<?>, Object> defaultValues = new HashMap<>();

    @BeforeEach
    void setUp()
    {
        defaultValues.put(String.class, "");
        defaultValues.put(Long.TYPE, 0L);
        defaultValues.put(Integer.TYPE, 0);
        defaultValues.put(Boolean.TYPE, false);
        defaultValues.put(DirectBuffer.class, new ExpandableArrayBuffer(0));
        defaultValues.put(Enum.class, TimeUnit.MINUTES);
    }

    @Test
    @InterruptAfter(2)
    void shouldLogConfiguredEvents()
    {
        final String eventConfig = System.getProperty("aeron.event.archive.log");
        final String disableConfig = System.getProperty("aeron.event.archive.log.disable");

        final List<ArchiveEventCode> disabledEvents;
        if (null != disableConfig)
        {
            disabledEvents = Arrays.stream(disableConfig.split(","))
                .map(ArchiveEventCode::valueOf)
                .toList();
        }
        else
        {
            disabledEvents = Collections.emptyList();
        }

        final List<ArchiveEventCode> enabledEvents;
        if ("all".equalsIgnoreCase(eventConfig))
        {
            enabledEvents = Arrays.stream(ArchiveEventCode.values())
                .filter((c) -> !disabledEvents.contains(c))
                .toList();
        }
        else if ("none".equalsIgnoreCase(eventConfig))
        {
            enabledEvents = Collections.emptyList();
        }
        else if (null != eventConfig)
        {
            enabledEvents = Arrays.stream(eventConfig.split(","))
                .map(ArchiveEventCode::valueOf)
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
        callControlRequestLogMethods();

        for (final ArchiveEventCode code : enabledEvents)
        {
            validateLogSend(code, countingAgent);
        }

        if (disabledEvents.isEmpty())
        {
            return;
        }

        final ArchiveEventCode[] codes = ArchiveEventCode.values();
        final long deadlineMs = System.currentTimeMillis() + 1_000L;
        while (System.currentTimeMillis() < deadlineMs)
        {
            for (final ArchiveEventCode code : codes)
            {
                if (disabledEvents.contains(code))
                {
                    validateLogNotSent(code, countingAgent);
                }
            }
        }
    }

    private static void callControlRequestLogMethods()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MessageHeaderEncoder.ENCODED_LENGTH]);
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

        for (final ArchiveEventCode code : ArchiveEventCode.values())
        {
            if (code.name().startsWith("CMD_IN_"))
            {
                headerEncoder.wrap(buffer, 0).templateId(code.templateId());
                ArchiveLog.logControlRequest(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH);
            }
        }
    }

    private void callGeneralLogMethods()
    {
        final List<Method> logMethods = Arrays.stream(ArchiveLog.class.getMethods())
            .filter((m) -> m.getName().startsWith("log"))
            .filter((m) -> 0 != (m.getModifiers() & Modifier.STATIC))
            .filter((m) -> !"logControlRequest".equals(m.getName()))
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

    private static void validateLogSend(final ArchiveEventCode code, final CountingEventReaderAgent countingAgent)
    {
        final Supplier<String> msg = () -> "Did not see event: " + code;
        while (0 == countingAgent.countArchiveEvent(code.toEventCodeId()))
        {
            Tests.sleep(1, msg);
        }
    }

    private static void validateLogNotSent(final ArchiveEventCode code, final CountingEventReaderAgent countingAgent)
    {
        assertEquals(0, countingAgent.countArchiveEvent(code.toEventCodeId()), code.toString());
    }
}
