package io.aeron.agent;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Object2ObjectHashMap;

import java.util.EnumSet;

public class ClusterModuleLogger implements ModuleLogger
{
    private static final Object2ObjectHashMap<String, EnumSet<ClusterEventCode>> SPECIAL_EVENTS =
        new Object2ObjectHashMap<>();

    public static final EnumSet<ClusterEventCode> ENABLED_EVENT_CODES;

    static
    {
        SPECIAL_EVENTS.put("all", EnumSet.allOf(ClusterEventCode.class));
        final String enabledEventCodes = System.getProperty("aeron.event.cluster.log");

        ENABLED_EVENT_CODES = EventConfiguration.parseEventCodes(
            ClusterEventCode.class,
            enabledEventCodes,
            SPECIAL_EVENTS,
            ClusterEventCode::get,
            ClusterEventCode::valueOf);
    }

    public static boolean isEnabled(final ClusterEventCode clusterEventCode)
    {
        return ENABLED_EVENT_CODES.contains(clusterEventCode);
    }

    public int typeCode()
    {
        return EventCodeType.CLUSTER.getTypeCode();
    }

    public void decode(
        final MutableDirectBuffer buffer,
        final int offset,
        final int eventCodeId,
        final StringBuilder builder)
    {
        ClusterEventCode.get(eventCodeId).decode(buffer, offset, builder);
    }

    public void reset()
    {
        ENABLED_EVENT_CODES.clear();
    }

    public String version()
    {
        return "";
    }
}
