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
package io.aeron.cluster.logging;

import io.aeron.cluster.ElectionState;
import io.aeron.logging.LoggerEventCallback;

import java.util.List;

class LogElectionStateChangeFlyweight implements LoggerEventCallback
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

    LogElectionStateChangeFlyweight(final List<ClusterEventLogger> loggers)
    {
        this.loggers = loggers;
    }

    int memberId()
    {
        return memberId;
    }

    void memberId(final int memberId)
    {
        this.memberId = memberId;
    }

    ElectionState oldState()
    {
        return oldState;
    }

    void oldState(final ElectionState oldState)
    {
        this.oldState = oldState;
    }

    ElectionState newState()
    {
        return newState;
    }

    void newState(final ElectionState newState)
    {
        this.newState = newState;
    }

    int leaderId()
    {
        return leaderId;
    }

    void leaderId(final int leaderId)
    {
        this.leaderId = leaderId;
    }

    long candidateTermId()
    {
        return candidateTermId;
    }

    void candidateTermId(final long candidateTermId)
    {
        this.candidateTermId = candidateTermId;
    }

    long leadershipTermId()
    {
        return leadershipTermId;
    }

    void leadershipTermId(final long leadershipTermId)
    {
        this.leadershipTermId = leadershipTermId;
    }

    long logPosition()
    {
        return logPosition;
    }

    void logPosition(final long logPosition)
    {
        this.logPosition = logPosition;
    }

    long logLeadershipTermId()
    {
        return logLeadershipTermId;
    }

    void logLeadershipTermId(final long logLeadershipTermId)
    {
        this.logLeadershipTermId = logLeadershipTermId;
    }

    long appendPosition()
    {
        return appendPosition;
    }

    void appendPosition(final long appendPosition)
    {
        this.appendPosition = appendPosition;
    }

    long catchupPosition()
    {
        return catchupPosition;
    }

    void catchupPosition(final long catchupPosition)
    {
        this.catchupPosition = catchupPosition;
    }

    String reason()
    {
        return reason;
    }

    void reason(final String reason)
    {
        this.reason = reason;
    }

    /**
     * {@inheritDoc}
     */
    public void onHeader(final int eventType, final int eventCode, final long timestamp)
    {

    }

    /**
     * {@inheritDoc}
     */
    public void onFooter(final boolean truncated)
    {
        for (int i = 0, n = loggers.size(); i < n; i++)
        {
            final ClusterEventLogger logger = loggers.get(i);
            dispatch(logger);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void onValue(final CharSequence name, final CharSequence value, final long tags)
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

    /**
     * {@inheritDoc}
     */
    public void onValue(final CharSequence name, final long value, final long tags)
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

    /**
     * {@inheritDoc}
     */
    public void onValue(final CharSequence name, final boolean value, final long tags)
    {

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
