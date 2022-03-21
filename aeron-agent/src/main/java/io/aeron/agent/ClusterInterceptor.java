/*
 * Copyright 2014-2022 Real Logic Limited.
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

import net.bytebuddy.asm.Advice;

import java.util.concurrent.TimeUnit;

import static io.aeron.agent.ClusterEventCode.ROLE_CHANGE;
import static io.aeron.agent.ClusterEventCode.STATE_CHANGE;
import static io.aeron.agent.ClusterEventLogger.LOGGER;

class ClusterInterceptor
{
    static class ElectionStateChange
    {
        @Advice.OnMethodEnter
        static <E extends Enum<E>> void stateChange(
            final E oldState,
            final E newState,
            final int memberId,
            final int leaderId,
            final long candidateTermId,
            final long leadershipTermId,
            final long logPosition,
            final long logLeadershipTermId,
            final long appendPosition,
            final long catchupPosition)
        {
            LOGGER.logElectionStateChange(
                oldState,
                newState,
                memberId,
                leaderId,
                candidateTermId,
                leadershipTermId,
                logPosition,
                logLeadershipTermId,
                appendPosition,
                catchupPosition);
        }
    }

    static class NewLeadershipTerm
    {
        @Advice.OnMethodEnter
        static void onNewLeadershipTerm(
            final long logLeadershipTermId,
            final long nextLeadershipTermId,
            final long nextTermBaseLogPosition,
            final long nextLogPosition,
            final long leadershipTermId,
            final long termBaseLogPosition,
            final long logPosition,
            final long leaderRecordingId,
            final long timestamp,
            final int leaderMemberId,
            final int logSessionId,
            final boolean isStartup)
        {
            LOGGER.logNewLeadershipTerm(
                logLeadershipTermId,
                nextLeadershipTermId,
                nextTermBaseLogPosition,
                nextLogPosition,
                leadershipTermId,
                termBaseLogPosition,
                logPosition,
                leaderRecordingId,
                timestamp,
                leaderMemberId,
                logSessionId,
                isStartup);
        }
    }

    static class ConsensusModuleStateChange
    {
        @Advice.OnMethodEnter
        static <E extends Enum<E>> void stateChange(final E oldState, final E newState, final int memberId)
        {
            LOGGER.logStateChange(STATE_CHANGE, oldState, newState, memberId);
        }
    }

    static class ConsensusModuleRoleChange
    {
        @Advice.OnMethodEnter
        static <E extends Enum<E>> void roleChange(final E oldRole, final E newRole, final int memberId)
        {
            LOGGER.logStateChange(ROLE_CHANGE, oldRole, newRole, memberId);
        }
    }

    static class CanvassPosition
    {
        @Advice.OnMethodEnter
        static void onCanvassPosition(
            final long logLeadershipTermId,
            final long logPosition,
            final long leadershipTermId,
            final int followerMemberId)
        {
            LOGGER.logCanvassPosition(logLeadershipTermId, leadershipTermId, logPosition, followerMemberId);
        }
    }

    static class RequestVote
    {
        @Advice.OnMethodEnter
        static void onRequestVote(
            final long logLeadershipTermId, final long logPosition, final long candidateTermId, final int candidateId)
        {
            LOGGER.logRequestVote(logLeadershipTermId, logPosition, candidateTermId, candidateId);
        }
    }

    static class CatchupPosition
    {
        @Advice.OnMethodEnter
        static void onCatchupPosition(
            final long leadershipTermId,
            final long logPosition,
            final int followerMemberId,
            final String catchupEndpoint)
        {
            LOGGER.logCatchupPosition(leadershipTermId, logPosition, followerMemberId, catchupEndpoint);
        }
    }

    static class StopCatchup
    {
        @Advice.OnMethodEnter
        static void onStopCatchup(final long leadershipTermId, final int followerMemberId)
        {
            LOGGER.logStopCatchup(leadershipTermId, followerMemberId);
        }
    }

    static class TruncateLogEntry
    {
        @Advice.OnMethodEnter
        static <E extends Enum<E>> void onTruncateLogEntry(
            final int memberId,
            final E state,
            final long logLeadershipTermId,
            final long leadershipTermId,
            final long candidateTermId,
            final long commitPosition,
            final long logPosition,
            final long appendPosition,
            final long oldPosition,
            final long newPosition)
        {
            LOGGER.logTruncateLogEntry(
                memberId,
                state,
                logLeadershipTermId,
                leadershipTermId,
                candidateTermId,
                commitPosition,
                logPosition,
                appendPosition,
                oldPosition,
                newPosition);
        }
    }

    static class ReplayNewLeadershipTerm
    {
        @Advice.OnMethodEnter
        static void onReplayNewLeadershipTermEvent0(
            final int memberId,
            final boolean isInElection,
            final long leadershipTermId,
            final long logPosition,
            final long timestamp,
            final long termBaseLogPosition,
            final TimeUnit timeUnit,
            final int appVersion)
        {
            LOGGER.logReplayNewLeadershipTermEvent(
                memberId,
                isInElection,
                leadershipTermId,
                logPosition,
                timestamp,
                termBaseLogPosition,
                timeUnit,
                appVersion);
        }
    }

    static class AppendPosition
    {
        @Advice.OnMethodEnter
        static void onAppendPosition(
            final long leadershipTermId,
            final long logPosition,
            final int memberId,
            final int flags)
        {
            LOGGER.logAppendPosition(
                leadershipTermId,
                logPosition,
                memberId,
                flags);
        }
    }

    static class CommitPosition
    {
        @Advice.OnMethodEnter
        static void onCommitPosition(
            final long leadershipTermId,
            final long logPosition,
            final int memberId)
        {
            LOGGER.logCommitPosition(
                leadershipTermId,
                logPosition,
                memberId);
        }
    }
}
