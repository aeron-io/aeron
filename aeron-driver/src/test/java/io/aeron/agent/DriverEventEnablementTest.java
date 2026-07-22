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

import io.aeron.command.ControlProtocolEvents;
import io.aeron.driver.DriverLog;
import io.aeron.driver.media.ImageConnection;
import io.aeron.logging.EventConfiguration;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.LoggingTest;
import io.aeron.test.Tests;
import io.aeron.test.agent.CountingEventReaderAgent;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.Agent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
@LoggingTest
public class DriverEventEnablementTest
{
    private final Map<Class<?>, Object> defaultValues = new HashMap<>();

    @BeforeEach
    void setUp()
    {
        defaultValues.put(String.class, "");
        defaultValues.put(Long.TYPE, 0L);
        defaultValues.put(Integer.TYPE, 0);
        defaultValues.put(Character.TYPE, (char)0);
        defaultValues.put(Short.TYPE, (short)0);
        defaultValues.put(Byte.TYPE, (byte)0);
        defaultValues.put(Double.TYPE, 0D);
        defaultValues.put(Float.TYPE, 0F);
        defaultValues.put(Boolean.TYPE, false);
        defaultValues.put(InetSocketAddress.class, new InetSocketAddress("localhost", 0));
        defaultValues.put(ByteBuffer.class, ByteBuffer.allocate(0));
        defaultValues.put(DirectBuffer.class, new ExpandableArrayBuffer(0));
        defaultValues.put(Enum.class, TimeUnit.MINUTES);
        defaultValues.put(InetAddress.class, InetAddress.getLoopbackAddress());
        defaultValues.put(
            ImageConnection[].class,
            new ImageConnection[]{ new ImageConnection(0, new InetSocketAddress("localhost", 0)) });
    }

    @Test
    @InterruptAfter(2)
    void shouldLogConfiguredEvents()
    {
        final String eventConfig = System.getProperty("aeron.event.log");
        final String disableConfig = System.getProperty("aeron.event.log.disable");

        final List<DriverEventCode> disabledEvents;
        if (null != disableConfig)
        {
            disabledEvents = Arrays.stream(disableConfig.split(","))
                .map(DriverEventCode::valueOf)
                .toList();
        }
        else
        {
            disabledEvents = Collections.emptyList();
        }

        final List<DriverEventCode> enabledEvents;
        if ("all".equalsIgnoreCase(eventConfig))
        {
            enabledEvents = Arrays.stream(DriverEventCode.values())
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
                .map(DriverEventCode::valueOf)
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
        callCmdInLogMethods();
        callCmdOutLogMethods();

        for (final DriverEventCode code : enabledEvents)
        {
            validateLogSend(code, countingAgent);
        }

        if (disabledEvents.isEmpty())
        {
            return;
        }

        final DriverEventCode[] codes = DriverEventCode.values();
        final long deadlineMs = System.currentTimeMillis() + 1_000L;
        while (System.currentTimeMillis() < deadlineMs)
        {
            for (final DriverEventCode code : codes)
            {
                if (disabledEvents.contains(code))
                {
                    validateLogNotSent(code, countingAgent);
                }
            }
        }
    }

    private static void callCmdInLogMethods()
    {
        final DriverEventCode[] codes = DriverEventCode.values();

        for (final DriverEventCode code : codes)
        {
            final String cmdIn = "CMD_IN_";
            if (code.name().startsWith(cmdIn))
            {
                final String protocolName = code.name().substring(cmdIn.length());
                try
                {
                    final Field declaredField = ControlProtocolEvents.class.getDeclaredField(protocolName);
                    final int msgTypeId = (int)(Integer)declaredField.get(null);
                    DriverLog.logCmd(msgTypeId, new ExpandableArrayBuffer(0), 0, 0);
                }
                catch (final NoSuchFieldException | IllegalAccessException ignore)
                {
                    // Some fields failed to follow the naming convention.
                }
            }
        }

        DriverLog.logCmd(ControlProtocolEvents.CLIENT_KEEPALIVE, new ExpandableArrayBuffer(0), 0, 0);
    }

    private static void callCmdOutLogMethods()
    {
        final DriverEventCode[] codes = DriverEventCode.values();

        for (final DriverEventCode code : codes)
        {
            final String cmdOut = "CMD_OUT_";
            if (code.name().startsWith(cmdOut))
            {
                final String name = code.name().substring(cmdOut.length());
                final String protocolName = name.startsWith("ON_") ? name : "ON_" + name;
                try
                {
                    final Field declaredField = ControlProtocolEvents.class.getDeclaredField(protocolName);
                    final int msgTypeId = (int)(Integer)declaredField.get(null);
                    DriverLog.logCmd(msgTypeId, new ExpandableArrayBuffer(0), 0, 0);
                }
                catch (final NoSuchFieldException | IllegalAccessException ignore)
                {
                    // Some fields failed to follow the naming convention.
                }
            }
        }
    }

    private void callGeneralLogMethods()
    {
        final List<Method> logMethods = Arrays.stream(DriverLog.class.getMethods())
            .filter((m) -> m.getName().startsWith("log"))
            .filter((m) -> 0 != (m.getModifiers() & Modifier.STATIC))
            .filter((m) -> !"logCmd".equals(m.getName()))
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
            params[i] = requireNonNull(defaultValues.get(parameter.getType()), logMethod.toString());
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

    private static void validateLogSend(final DriverEventCode code, final CountingEventReaderAgent countingAgent)
    {
        final Supplier<String> msg = () -> "Did not see event: " + code;
        while (0 == countingAgent.countDriverEvent(code.toEventCodeId()))
        {
            Tests.sleep(1, msg);
        }
    }

    private static void validateLogNotSent(final DriverEventCode code, final CountingEventReaderAgent countingAgent)
    {
        assertEquals(0, countingAgent.countDriverEvent(code.toEventCodeId()), code.toString());
    }
}
