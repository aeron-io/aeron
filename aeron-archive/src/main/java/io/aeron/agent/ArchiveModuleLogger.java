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
import io.aeron.version.Versioned;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Object2ObjectHashMap;

import java.util.EnumSet;

/**
 * Implementation of the {@link ModuleLogger} to handle logging and decode of Archive log events.
 */
@Versioned
public class ArchiveModuleLogger implements ModuleLogger
{
    private static final Object2ObjectHashMap<String, EnumSet<ArchiveEventCode>> SPECIAL_EVENTS =
        new Object2ObjectHashMap<>();

    static final EnumSet<ArchiveEventCode> ENABLED_EVENT_CODES;

    static
    {
        SPECIAL_EVENTS.put("all", EnumSet.allOf(ArchiveEventCode.class));
        final String enabledEventCodes = System.getProperty("aeron.event.archive.log");
        final String disabledEventCodes = System.getProperty("aeron.event.archive.log.disable");
        final EnumSet<ArchiveEventCode> disabledEventCodeSet = EventConfiguration.parseEventCodes(
            ArchiveEventCode.class,
            disabledEventCodes,
            SPECIAL_EVENTS,
            ArchiveEventCode::get,
            ArchiveEventCode::valueOf);
        ENABLED_EVENT_CODES = EventConfiguration.parseEventCodes(
            ArchiveEventCode.class,
            enabledEventCodes,
            SPECIAL_EVENTS,
            ArchiveEventCode::get,
            ArchiveEventCode::valueOf);

        ENABLED_EVENT_CODES.removeAll(disabledEventCodeSet);
    }

    /**
     * Determine if a given event code is configured/enabled for logging.
     *
     * @param archiveEventCode to check for enablement.
     * @return <code>true</code> if enabled, <code>false</code> otherwise.
     */
    public static boolean isEnabled(final ArchiveEventCode archiveEventCode)
    {
        return ENABLED_EVENT_CODES.contains(archiveEventCode);
    }

    /**
     * {@inheritDoc}
     */
    public int typeCode()
    {
        return EventCodeType.ARCHIVE.getTypeCode();
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
        ArchiveEventCode.get(eventCodeId).decode(buffer, offset, builder);
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
            ArchiveModuleLoggerVersion.VERSION, ArchiveModuleLoggerVersion.GIT_SHA);
    }
}
