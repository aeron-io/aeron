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

import io.aeron.AeronCounters;
import io.aeron.version.Versioned;
import net.bytebuddy.agent.builder.AgentBuilder;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Object2ObjectHashMap;

import java.util.EnumSet;
import java.util.Map;

import static io.aeron.agent.ArchiveEventCode.*;
import static io.aeron.agent.ConfigOption.DISABLED_ARCHIVE_EVENT_CODES;
import static io.aeron.agent.ConfigOption.ENABLED_ARCHIVE_EVENT_CODES;
import static io.aeron.agent.EventConfiguration.parseEventCodes;
import static net.bytebuddy.asm.Advice.to;
import static net.bytebuddy.matcher.ElementMatchers.nameEndsWith;
import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * Implementation of a component logger for archive log events.
 */
@Versioned
public class ArchiveComponentLogger implements ComponentLogger
{
    static final EnumSet<ArchiveEventCode> ENABLED_EVENTS = EnumSet.noneOf(ArchiveEventCode.class);
    private static final Object2ObjectHashMap<String, EnumSet<ArchiveEventCode>> SPECIAL_EVENTS =
        new Object2ObjectHashMap<>();

    static
    {
        SPECIAL_EVENTS.put("all", EnumSet.allOf(ArchiveEventCode.class));
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
        final MutableDirectBuffer buffer, final int offset, final int eventCodeId, final StringBuilder builder)
    {
        ArchiveEventCode.get(eventCodeId).decode(buffer, offset, builder);
    }

    /**
     * {@inheritDoc}
     */
    public AgentBuilder addInstrumentation(final AgentBuilder agentBuilder, final Map<String, String> configOptions)
    {
        ENABLED_EVENTS.clear();
        ENABLED_EVENTS.addAll(getArchiveEventCodes(configOptions.get(ENABLED_ARCHIVE_EVENT_CODES)));
        ENABLED_EVENTS.removeAll(getArchiveEventCodes(configOptions.get(DISABLED_ARCHIVE_EVENT_CODES)));

        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addArchiveControlSessionAdapterInstrumentation(tempBuilder);

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            CMD_OUT_RESPONSE,
            "ControlResponseProxy",
            ControlInterceptor.ControlResponse.class,
            "logSendResponse");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            RECORDING_SIGNAL,
            "ControlResponseProxy",
            ControlInterceptor.RecordingSignal.class,
            "logSendSignal");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            REPLAY_SESSION_STATE_CHANGE,
            "ReplaySession",
            ArchiveInterceptor.ReplaySessionStateChange.class,
            "logStateChange");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            RECORDING_SESSION_STATE_CHANGE,
            "RecordingSession",
            ArchiveInterceptor.RecordingSessionStateChange.class,
            "logStateChange");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            REPLICATION_SESSION_STATE_CHANGE,
            "ReplicationSession",
            ArchiveInterceptor.ReplicationSessionStateChange.class,
            "logStateChange");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            REPLICATION_SESSION_DONE,
            "ReplicationSession",
            ArchiveInterceptor.ReplicationSessionDone.class,
            "logReplicationSessionDone");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            CONTROL_SESSION_STATE_CHANGE,
            "ControlSession",
            ArchiveInterceptor.ControlSessionStateChange.class,
            "logStateChange");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            REPLAY_SESSION_ERROR,
            "ReplaySession",
            ArchiveInterceptor.ReplaySession.class,
            "onPendingError");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            CATALOG_RESIZE,
            "Catalog",
            ArchiveInterceptor.Catalog.class,
            "catalogResized");

        return tempBuilder;
    }

    /**
     * {@inheritDoc}
     */
    public void reset()
    {
        ENABLED_EVENTS.clear();
    }

    /**
     * {@inheritDoc}
     */
    public String version()
    {
        return AeronCounters.formatVersionInfo(
            ArchiveComponentLoggerVersion.VERSION, ArchiveComponentLoggerVersion.GIT_SHA);
    }

    private static EnumSet<ArchiveEventCode> getArchiveEventCodes(final String enabledEventCodes)
    {
        return parseEventCodes(
            ArchiveEventCode.class,
            enabledEventCodes,
            SPECIAL_EVENTS,
            ArchiveEventCode::get,
            ArchiveEventCode::valueOf);
    }

    private static AgentBuilder addArchiveControlSessionAdapterInstrumentation(final AgentBuilder agentBuilder)
    {
        if (ArchiveEventLogger.CONTROL_REQUEST_EVENTS.stream().noneMatch(ENABLED_EVENTS::contains))
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("ControlSessionAdapter"))
            .transform(((builder, typeDescription, classLoader, module, protectionDomain) -> builder
                .visit(to(ControlInterceptor.ControlRequest.class)
                    .on(named("onFragment")))));
    }

    private static AgentBuilder addEventInstrumentation(
        final AgentBuilder agentBuilder,
        final ArchiveEventCode code,
        final String typeName,
        final Class<?> interceptorClass,
        final String interceptorMethod)
    {
        if (!ENABLED_EVENTS.contains(code))
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith(typeName))
            .transform((builder, typeDescription, classLoader, javaModule, protectionDomain) ->
                builder.visit(to(interceptorClass).on(named(interceptorMethod))));
    }
}
