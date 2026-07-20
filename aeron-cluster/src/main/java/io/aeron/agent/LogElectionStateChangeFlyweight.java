package io.aeron.agent;

import io.aeron.cluster.ElectionState;

import java.util.List;

public class LogElectionStateChangeFlyweight implements LoggerEventCallback
{
    private final List<ClusterEventLogger> loggers;
    private int memberId;
    private ElectionState oldState;
    private ElectionState newState;
    private int leaderId;
    private long candidateTermId;
    private long leadershipTermId;
    private long logPosition;
    private long logLeadershipTermId;
    private long appendPosition;
    private long catchupPosition;
    private String reason;
    private boolean truncated;

    public LogElectionStateChangeFlyweight(final List<ClusterEventLogger> loggers)
    {
        this.loggers = loggers;
    }

    public int memberId()
    {
        return memberId;
    }

    public void memberId(final int memberId)
    {
        this.memberId = memberId;
    }

    public ElectionState oldState()
    {
        return oldState;
    }

    public void oldState(final ElectionState oldState)
    {
        this.oldState = oldState;
    }

    public ElectionState newState()
    {
        return newState;
    }

    public void newState(final ElectionState newState)
    {
        this.newState = newState;
    }

    public int leaderId()
    {
        return leaderId;
    }

    public void leaderId(final int leaderId)
    {
        this.leaderId = leaderId;
    }

    public long candidateTermId()
    {
        return candidateTermId;
    }

    public void candidateTermId(final long candidateTermId)
    {
        this.candidateTermId = candidateTermId;
    }

    public long leadershipTermId()
    {
        return leadershipTermId;
    }

    public void leadershipTermId(final long leadershipTermId)
    {
        this.leadershipTermId = leadershipTermId;
    }

    public long logPosition()
    {
        return logPosition;
    }

    public void logPosition(final long logPosition)
    {
        this.logPosition = logPosition;
    }

    public long logLeadershipTermId()
    {
        return logLeadershipTermId;
    }

    public void logLeadershipTermId(final long logLeadershipTermId)
    {
        this.logLeadershipTermId = logLeadershipTermId;
    }

    public long appendPosition()
    {
        return appendPosition;
    }

    public void appendPosition(final long appendPosition)
    {
        this.appendPosition = appendPosition;
    }

    public long catchupPosition()
    {
        return catchupPosition;
    }

    public void catchupPosition(final long catchupPosition)
    {
        this.catchupPosition = catchupPosition;
    }

    public String reason()
    {
        return reason;
    }

    public void reason(final String reason)
    {
        this.reason = reason;
    }

    public void onHeader(final int eventType, final int eventCode, final long timestamp)
    {

    }

    public void onFooter(final boolean truncated)
    {
        for (ClusterEventLogger logger : loggers)
        {
            dispatch(logger);
        }
    }

    public void onValue(final CharSequence name, final CharSequence value)
    {
        switch (name.toString())
        {
            case "oldState":
            {
                oldState(ElectionState.valueOf(value.toString()));
                break;
            }
            case "newState":
            {
                newState(ElectionState.valueOf(value.toString()));
                break;
            }
            case "reason":
            {
                reason(value.toString());
                break;
            }
        }
    }

    public void onValue(final CharSequence name, final long value)
    {
        switch (name.toString())
        {
            case "memberId":
            {
                memberId((int)value);
                break;
            }
            case "leaderId":
            {
                leaderId((int)value);
                break;
            }
            case "candidateTermId":
            {
                candidateTermId(value);
                break;
            }
            case "leadershipTermId":
            {
                leadershipTermId(value);
                break;
            }
            case "logPosition":
            {
                logPosition(value);
                break;
            }
            case "logLeadershipTermId":
            {
                logLeadershipTermId(value);
                break;
            }
            case "appendPosition":
            {
                appendPosition(value);
                break;
            }
            case "catchupPosition":
            {
                catchupPosition(value);
                break;
            }
        }
    }

    void dispatch(final ClusterEventLogger logger)
    {
        logger.logElectionStateChange(
            memberId, oldState, newState, leaderId, candidateTermId, leadershipTermId, logPosition, logLeadershipTermId,
            appendPosition, catchupPosition, reason);
    }

    void reset()
    {
        memberId = 0;
        oldState = null;
        newState = null;
        leaderId = 0;
        candidateTermId = 0;
        leadershipTermId = 0;
        logPosition = 0;
        logLeadershipTermId = 0;
        appendPosition = 0;
        catchupPosition = 0;
        reason = null;
        truncated = false;
    }
}
