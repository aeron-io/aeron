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

import io.aeron.AeronCounters;
import io.aeron.logging.EventCodeType;
import io.aeron.logging.EventConfiguration;
import io.aeron.logging.ModuleLogger;
import io.aeron.version.Versioned;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Object2ObjectHashMap;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import static io.aeron.agent.DriverEventCode.FRAME_IN;
import static io.aeron.agent.DriverEventCode.FRAME_OUT;

/**
 * Implementation of the {@link ModuleLogger} to handle logging and decode of Driver log events.
 */
@Versioned
public class DriverModuleLogger implements ModuleLogger
{
    private static final Object2ObjectHashMap<String, EnumSet<DriverEventCode>> SPECIAL_EVENTS =
        new Object2ObjectHashMap<>();

    private static final Set<DriverEventCode> ENABLED_EVENT_CODES;

    static
    {
        SPECIAL_EVENTS.put("all", EnumSet.allOf(DriverEventCode.class));
        SPECIAL_EVENTS.put("admin", EnumSet.complementOf(EnumSet.of(FRAME_IN, FRAME_OUT)));

        final String enabledEventCodes = System.getProperty("aeron.event.log");
        final String disabledEventCodes = System.getProperty("aeron.event.log.disable");

        final EnumSet<DriverEventCode> disabledEventCodeSet = EventConfiguration.parseEventCodes(
            DriverEventCode.class,
            disabledEventCodes,
            SPECIAL_EVENTS,
            DriverEventCode::get,
            DriverEventCode::get);

        final EnumSet<DriverEventCode> enabledEventCodeSet = EventConfiguration.parseEventCodes(
            DriverEventCode.class,
            enabledEventCodes,
            SPECIAL_EVENTS,
            DriverEventCode::get,
            DriverEventCode::get);

        enabledEventCodeSet.removeAll(disabledEventCodeSet);

        ENABLED_EVENT_CODES = Collections.unmodifiableSet(enabledEventCodeSet);
    }

    /**
     * Create a DriverModuleLogger, used by java service API.
     */
    public DriverModuleLogger()
    {
    }

    /**
     * Determine if a given event code is configured/enabled for logging.
     *
     * @param driverEventCode to check for enablement.
     * @return <code>true</code> if enabled, <code>false</code> otherwise.
     */
    public static boolean isEnabled(final DriverEventCode driverEventCode)
    {
        return ENABLED_EVENT_CODES.contains(driverEventCode);
    }

    /**
     * {@inheritDoc}
     */
    public int typeCode()
    {
        return EventCodeType.DRIVER.getTypeCode();
    }

    /**
     * {@inheritDoc}
     */
    public void decode(
        final MutableDirectBuffer buffer,
        final int offset,
        final int eventCodeId,
        final StringBuilder builder)
    {
        DriverEventCode.get(eventCodeId).decode(buffer, offset, builder);
    }

    /**
     * {@inheritDoc}
     */
    public void reset()
    {
        ENABLED_EVENT_CODES.clear();
    }

    /**
     * {@inheritDoc}
     */
    public String version()
    {
        return AeronCounters.formatVersionInfo(
            DriverModuleLoggerVersion.VERSION, DriverModuleLoggerVersion.GIT_SHA);
    }
}
