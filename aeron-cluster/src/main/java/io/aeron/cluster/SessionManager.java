/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.cluster.codecs.BackupQueryDecoder;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.codecs.HeartbeatRequestDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.StandbySnapshotDecoder;
import io.aeron.cluster.service.ClusterClock;
import io.aeron.security.Authenticator;
import io.aeron.security.AuthorisationService;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.aeron.cluster.ClusterSession.State.AUTHENTICATED;
import static io.aeron.cluster.ClusterSession.State.CHALLENGED;
import static io.aeron.cluster.ClusterSession.State.CONNECTED;
import static io.aeron.cluster.ClusterSession.State.CONNECTING;
import static io.aeron.cluster.ClusterSession.State.INIT;
import static io.aeron.cluster.ClusterSession.State.INVALID;
import static io.aeron.cluster.ClusterSession.State.REJECTED;
import static io.aeron.cluster.ConsensusModuleAgent.logAppendSessionOpen;

class SessionManager
{
    final Long2ObjectHashMap<ClusterSession> sessionByIdMap = new Long2ObjectHashMap<>();
    final ArrayList<ClusterSession> sessions = new ArrayList<>();
    final ArrayList<ClusterSession> pendingUserSessions = new ArrayList<>();
    final ArrayList<ClusterSession> rejectedUserSessions = new ArrayList<>();
    final ArrayList<ClusterSession> redirectUserSessions = new ArrayList<>();

    final ArrayList<ClusterSession> pendingBackupSessions = new ArrayList<>();
    final ArrayList<ClusterSession> rejectedBackupSessions = new ArrayList<>();

    final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();

    long nextSessionId = 1;
    long nextCommittedSessionId = nextSessionId;

    Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap()
    {
        return clusterMemberByIdMap;
    }

    ArrayList<ClusterSession> pendingBackupSessions()
    {
        return pendingBackupSessions;
    }

    ArrayList<ClusterSession> pendingUserSessions()
    {
        return pendingUserSessions;
    }

    ArrayList<ClusterSession> redirectUserSessions()
    {
        return redirectUserSessions;
    }

    ArrayList<ClusterSession> rejectedBackupSessions()
    {
        return rejectedBackupSessions;
    }

    ArrayList<ClusterSession> rejectedUserSessions()
    {
        return rejectedUserSessions;
    }

    Long2ObjectHashMap<ClusterSession> sessionByIdMap()
    {
        return sessionByIdMap;
    }

    ArrayList<ClusterSession> sessions()
    {
        return sessions;
    }

    void addSession(final ClusterSession session)
    {
        sessionByIdMap.put(session.id(), session);

        final int size = sessions.size();
        int addIndex = size;
        for (int i = size - 1; i >= 0; i--)
        {
            if (sessions.get(i).id() < session.id())
            {
                addIndex = i + 1;
                break;
            }
        }

        if (size == addIndex)
        {
            sessions.add(session);
        }
        else
        {
            sessions.add(addIndex, session);
        }
    }

    @SuppressWarnings("checkstyle:methodlength")
    int processPendingSessions(
        final ArrayList<ClusterSession> pendingSessions,
        final ArrayList<ClusterSession> rejectedSessions,
        final long nowNs,
        final int memberId,
        final int leaderMemberId,
        final long leadershipTermId,
        final int commitPositionCounterId,
        final ClusterMember[] activeMembers,
        final ClusterSessionProxy sessionProxy,
        final RecordingLog recordingLog,
        final LogPublisher logPublisher,
        final EgressPublisher egressPublisher,
        final ConsensusModuleExtension consensusModuleExtension,
        final ConsensusPublisher consensusPublisher,
        final RecordingLog.RecoveryPlan recoveryPlan,
        final Authenticator authenticator,
        final AuthorisationService authorisationService,
        final ConsensusModule.Context ctx)
    {
        final ClusterClock clusterClock = ctx.clusterClock();
        final TimeUnit clusterTimeUnit = clusterClock.timeUnit();
        final Aeron aeron = ctx.aeron();
        final long sessionTimeoutNs = ctx.sessionTimeoutNs();

        int workCount = 0;

        for (int lastIndex = pendingSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = pendingSessions.get(i);

            if (session.state() == INVALID)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                session.close(aeron, ctx.countedErrorHandler(), "invalid session");
                continue;
            }

            if (nowNs > (session.timeOfLastActivityNs() + sessionTimeoutNs) && session.state() != INIT)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                session.close(aeron, ctx.countedErrorHandler(), "session timed out");
                ctx.timedOutClientCounter().incrementRelease();
                continue;
            }

            if (session.state() == INIT || session.state() == CONNECTING || session.state() == CONNECTED)
            {
                if (session.isResponsePublicationConnected(aeron, nowNs))
                {
                    session.state(CONNECTED, "connected");
                    authenticator.onConnectedSession(sessionProxy.session(session), clusterClock.timeMillis());
                }
            }

            if (session.state() == CHALLENGED)
            {
                if (session.isResponsePublicationConnected(aeron, nowNs))
                {
                    authenticator.onChallengedSession(sessionProxy.session(session), clusterClock.timeMillis());
                }
            }

            if (session.state() == AUTHENTICATED)
            {
                switch (session.action())
                {
                    case CLIENT:
                    {
                        if (session.appendSessionToLogAndSendOpen(
                            logPublisher, egressPublisher, leadershipTermId, memberId, nowNs, clusterClock.time()))
                        {
                            logAppendSessionOpen(
                                memberId,
                                session.id(),
                                leadershipTermId,
                                session.openedLogPosition(),
                                nowNs,
                                clusterTimeUnit);
                            if (session.id() >= nextCommittedSessionId)
                            {
                                nextCommittedSessionId = session.id() + 1;
                            }
                            ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                            addSession(session);
                            workCount += 1;
                            if (null != consensusModuleExtension)
                            {
                                consensusModuleExtension.onSessionOpened(session.id());
                            }
                        }
                        break;
                    }

                    case BACKUP:
                    {
                        if (!authorisationService.isAuthorised(
                            MessageHeaderDecoder.SCHEMA_ID,
                            BackupQueryDecoder.TEMPLATE_ID,
                            null,
                            session.encodedPrincipal()))
                        {
                            session.reject(
                                EventCode.AUTHENTICATION_REJECTED,
                                "Not authorised for BackupQuery",
                                ctx.errorLog());
                            break;
                        }

                        final RecordingLog.Entry entry = recordingLog.findLastTerm();
                        if (null != entry && consensusPublisher.backupResponse(
                            session,
                            commitPositionCounterId,
                            leaderMemberId,
                            memberId,
                            entry,
                            recoveryPlan,
                            ClusterMember.encodeAsString(activeMembers)))
                        {
                            ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                            session.close(aeron, ctx.countedErrorHandler(), "done");
                            workCount += 1;
                        }
                        break;
                    }

                    case HEARTBEAT:
                    {
                        if (!authorisationService.isAuthorised(
                            MessageHeaderDecoder.SCHEMA_ID,
                            HeartbeatRequestDecoder.TEMPLATE_ID,
                            null,
                            session.encodedPrincipal()))
                        {
                            session.reject(
                                EventCode.AUTHENTICATION_REJECTED,
                                "Not authorised for Heartbeat",
                                ctx.errorLog());
                            break;
                        }

                        if (consensusPublisher.heartbeatResponse(session))
                        {
                            ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                            session.close(aeron, ctx.countedErrorHandler(), "done");
                            workCount += 1;
                        }
                        break;
                    }

                    case STANDBY_SNAPSHOT:
                    {
                        if (!authorisationService.isAuthorised(
                            MessageHeaderDecoder.SCHEMA_ID,
                            StandbySnapshotDecoder.TEMPLATE_ID,
                            null,
                            session.encodedPrincipal()))
                        {
                            session.reject(
                                EventCode.AUTHENTICATION_REJECTED,
                                "Not authorised for StandbySnapshot",
                                ctx.errorLog());
                            break;
                        }

                        @SuppressWarnings("unchecked")
                        final List<StandbySnapshotEntry> standbySnapshotEntries =
                            (List<StandbySnapshotEntry>)session.requestInput();

                        for (final StandbySnapshotEntry standbySnapshotEntry : standbySnapshotEntries)
                        {
                            ConsensusModuleAgent.logStandbySnapshotNotification(
                                memberId,
                                standbySnapshotEntry.recordingId(),
                                standbySnapshotEntry.leadershipTermId(),
                                standbySnapshotEntry.termBaseLogPosition(),
                                standbySnapshotEntry.logPosition(),
                                standbySnapshotEntry.timestamp(),
                                ctx.clusterClock().timeUnit(),
                                standbySnapshotEntry.serviceId(),
                                standbySnapshotEntry.archiveEndpoint());

                            recordingLog.appendStandbySnapshot(
                                standbySnapshotEntry.recordingId(),
                                standbySnapshotEntry.leadershipTermId(),
                                standbySnapshotEntry.termBaseLogPosition(),
                                standbySnapshotEntry.logPosition(),
                                standbySnapshotEntry.timestamp(),
                                standbySnapshotEntry.serviceId(),
                                standbySnapshotEntry.archiveEndpoint());
                        }

                        ctx.standbySnapshotCounter().increment();
                        ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                        session.close(aeron, ctx.countedErrorHandler(), "done");
                        workCount += 1;
                    }
                }
            }
            else if (session.state() == REJECTED)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                rejectedSessions.add(session);
            }
        }

        return workCount;
    }

}
