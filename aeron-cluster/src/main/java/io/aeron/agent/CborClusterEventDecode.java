package io.aeron.agent;

import java.util.List;

public class CborClusterEventDecode implements LoggerEventCallback
{
    private final LogElectionStateChangeFlyweight logElectionStateChangeFlyweight;
    private LoggerEventCallback currentDecoder = null;

    public CborClusterEventDecode(final List<ClusterEventLogger> loggers)
    {
        this.logElectionStateChangeFlyweight = new LogElectionStateChangeFlyweight(loggers);
    }

    public void onHeader(final int eventType, final int eventCode, final long timestamp)
    {
        final EventCodeType eventCodeType = EventCodeType.fromTypeCode(eventType);
        if (eventCodeType != EventCodeType.CLUSTER)
        {
            currentDecoder = null;
            return;
        }

        final ClusterEventCode clusterEventCode = ClusterEventCode.fromEventCodeId(eventCode);
        switch (clusterEventCode)
        {
            case ELECTION_STATE_CHANGE:
            {
                logElectionStateChangeFlyweight.reset();
                currentDecoder = logElectionStateChangeFlyweight;
            }
        }

        if (null == currentDecoder)
        {
            return;
        }

        currentDecoder.onHeader(eventType, eventCode, timestamp);
    }

    public void onValue(final CharSequence name, final CharSequence value)
    {
        if (null == currentDecoder)
        {
            return;
        }

        currentDecoder.onValue(name, value);
    }

    public void onValue(final CharSequence name, final long value)
    {
        if (null == currentDecoder)
        {
            return;
        }

        currentDecoder.onValue(name, value);
    }

    public void onFooter(final boolean truncated)
    {
        if (null == currentDecoder)
        {
            return;
        }

        currentDecoder.onFooter(truncated);
        currentDecoder = null;
    }
}
