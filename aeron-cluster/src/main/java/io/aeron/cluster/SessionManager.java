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
import io.aeron.Counter;
import io.aeron.Image;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.BackupQueryDecoder;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.codecs.HeartbeatRequestDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.StandbySnapshotDecoder;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterClock;
import io.aeron.logbuffer.Header;
import io.aeron.security.Authenticator;
import io.aeron.security.AuthorisationService;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.SemanticVersion;
import org.agrona.Strings;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.errors.DistinctErrorLog;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.ClusterSession.State.AUTHENTICATED;
import static io.aeron.cluster.ClusterSession.State.CHALLENGED;
import static io.aeron.cluster.ClusterSession.State.CLOSING;
import static io.aeron.cluster.ClusterSession.State.CONNECTED;
import static io.aeron.cluster.ClusterSession.State.CONNECTING;
import static io.aeron.cluster.ClusterSession.State.INIT;
import static io.aeron.cluster.ClusterSession.State.INVALID;
import static io.aeron.cluster.ClusterSession.State.REJECTED;
import static io.aeron.cluster.ConsensusModule.Configuration.SESSION_INVALID_VERSION_MSG;
import static io.aeron.cluster.ConsensusModule.Configuration.SESSION_LIMIT_MSG;
import static io.aeron.cluster.ConsensusModuleAgent.logAppendSessionClose;
import static io.aeron.cluster.ConsensusModuleAgent.logAppendSessionOpen;
import static io.aeron.cluster.client.AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

class SessionManager
{
    private final Long2ObjectHashMap<ClusterSession> sessionByIdMap = new Long2ObjectHashMap<>();
    private final ArrayList<ClusterSession> sessions = new ArrayList<>();

    private final ArrayList<ClusterSession> pendingUserSessions = new ArrayList<>();
    private final ArrayList<ClusterSession> rejectedUserSessions = new ArrayList<>();
    private final ArrayList<ClusterSession> redirectUserSessions = new ArrayList<>();

    private final ArrayList<ClusterSession> pendingBackupSessions = new ArrayList<>();
    private final ArrayList<ClusterSession> rejectedBackupSessions = new ArrayList<>();

    private final ArrayDeque<ClusterSession> uncommittedClosedSessions = new ArrayDeque<>();

    private final int memberId;
    private final ClusterClock clusterClock;
    private final ClusterMember[] activeMembers;
    private final ClusterSessionProxy sessionProxy;
    private final EgressPublisher egressPublisher;
    private final TimeUnit clusterTimeUnit;
    private final Aeron aeron;
    private final long sessionTimeoutNs;
    private final CountedErrorHandler errorHandler;
    private final Counter timedOutCounter;
    private final DistinctErrorLog errorLog;
    private final Counter standbySnapshotCounter;
    private final Authenticator authenticator;
    private final AuthorisationService authorisationService;
    private final RecordingLog recordingLog;
    private final LogPublisher logPublisher;
    private final ConsensusPublisher consensusPublisher;
    private final ConsensusModuleExtension consensusModuleExtension;
    private final int commitPositionCounterId;
    private final int clusterId;
    private final int maxConcurrentSessions;
    private final MutableDirectBuffer tempBuffer = new ExpandableArrayBuffer();

    private long nextSessionId = 1;
    private long nextCommittedSessionId = nextSessionId;

    SessionManager(
        final ConsensusModule.Context ctx,
        final ClusterMember[] activeMembers,
        final ConsensusPublisher consensusPublisher)
    {
        this.memberId = ctx.clusterMemberId();
        this.clusterClock = ctx.clusterClock();
        this.activeMembers = activeMembers;
        this.egressPublisher = ctx.egressPublisher();
        this.sessionProxy = new ClusterSessionProxy(egressPublisher);
        this.logPublisher = ctx.logPublisher();
        this.consensusPublisher = consensusPublisher;
        this.clusterTimeUnit = clusterClock.timeUnit();
        this.aeron = ctx.aeron();
        this.sessionTimeoutNs = ctx.sessionTimeoutNs();
        this.errorHandler = ctx.countedErrorHandler();
        this.timedOutCounter = ctx.timedOutClientCounter();
        this.errorLog = ctx.errorLog();
        this.standbySnapshotCounter = ctx.standbySnapshotCounter();
        this.authenticator = ctx.authenticatorSupplier().get();
        this.authorisationService = ctx.authorisationServiceSupplier().get();
        this.recordingLog = ctx.recordingLog();
        this.consensusModuleExtension = ctx.consensusModuleExtension();
        this.commitPositionCounterId = ctx.commitPositionCounter().id();
        this.clusterId = ctx.clusterId();
        this.maxConcurrentSessions = ctx.maxConcurrentSessions();
    }

    ClusterSession findBySessionId(final long clusterSessionId)
    {
        return sessionByIdMap.get(clusterSessionId);
    }

    AuthorisationService authorisationService()
    {
        return authorisationService;
    }

    long nextCommittedSessionId()
    {
        return nextCommittedSessionId;
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

    void closeSession(final ClusterSession session)
    {
        final long sessionId = session.id();

        sessionByIdMap.remove(sessionId);
        for (int i = sessions.size() - 1; i >= 0; i--)
        {
            if (sessions.get(i).id() == sessionId)
            {
                sessions.remove(i);
                break;
            }
        }

        session.close(aeron, errorHandler, "closed");

        if (null != consensusModuleExtension && null != session.closeReason())
        {
            consensusModuleExtension.onSessionClosed(sessionId, session.closeReason());
        }
    }

    public void onSessionConnect(
        final long correlationId,
        final int responseStreamId,
        final int version,
        final String responseChannel,
        final byte[] encodedCredentials,
        final String clientInfo,
        final Header header,
        final Cluster.Role role)
    {
        final long clusterSessionId = Cluster.Role.LEADER == role ? nextSessionId++ : NULL_VALUE;
        final ClusterSession session = new ClusterSession(
            memberId,
            clusterSessionId,
            responseStreamId,
            responseChannel,
            sessionInfo(clientInfo, header));

        session.asyncConnect(aeron, tempBuffer, clusterId);
        final long nowNs = clusterClock.timeNanos();
        session.lastActivityNs(nowNs, correlationId);

        if (Cluster.Role.LEADER != role)
        {
            redirectUserSessions.add(session);
        }
        else
        {
            if (AeronCluster.Configuration.PROTOCOL_MAJOR_VERSION != SemanticVersion.major(version))
            {
                final String detail = SESSION_INVALID_VERSION_MSG + " " + SemanticVersion.toString(version) +
                    ", cluster is " + SemanticVersion.toString(PROTOCOL_SEMANTIC_VERSION);
                session.reject(EventCode.ERROR, detail, errorLog);
                rejectedUserSessions.add(session);
            }
            else if (pendingUserSessions.size() + sessions.size() >= maxConcurrentSessions)
            {
                session.reject(EventCode.ERROR, SESSION_LIMIT_MSG, errorLog);
                rejectedUserSessions.add(session);
            }
            else
            {
                session.linkIngressImage(header);
                authenticator.onConnectRequest(session.id(), encodedCredentials, NANOSECONDS.toMillis(nowNs));
                pendingUserSessions.add(session);
            }
        }
    }

    void onSessionClose(final long leadershipTermId, final long clusterSessionId)
    {
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null != session && session.isOpen())
        {
            session.closing(CloseReason.CLIENT_ACTION);
            session.disconnect(aeron, errorHandler);

            final long timestamp = clusterClock.time();
            if (logPublisher.appendSessionClose(memberId, session, leadershipTermId, timestamp, clusterTimeUnit))
            {
                logAppendSessionClose(
                    memberId, session.id(), session.closeReason(), leadershipTermId, timestamp, clusterTimeUnit);
                session.closedLogPosition(logPublisher.position());
                uncommittedClosedSessions.addLast(session);
                closeSession(session);
            }
        }
    }

    void onStandbySnapshot(
        final long correlationId,
        final int version,
        final List<StandbySnapshotEntry> standbySnapshotEntries,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedCredentials,
        final Header header)
    {
        final ClusterSession session = new ClusterSession(
            memberId,
            NULL_VALUE,
            responseStreamId,
            responseChannel,
            sessionInfo(ClusterSession.Action.STANDBY_SNAPSHOT.name(), header));

        final long nowNs = clusterClock.timeNanos();

        session.action(ClusterSession.Action.STANDBY_SNAPSHOT);
        session.asyncConnect(aeron, tempBuffer, clusterId);
        session.lastActivityNs(nowNs, correlationId);
        session.requestInput(standbySnapshotEntries);

        if (AeronCluster.Configuration.PROTOCOL_MAJOR_VERSION == SemanticVersion.major(version))
        {
            authenticator.onConnectRequest(session.id(), encodedCredentials, NANOSECONDS.toMillis(nowNs));
            pendingBackupSessions.add(session);
        }
        else
        {
            final String detail = SESSION_INVALID_VERSION_MSG + " " + SemanticVersion.toString(version) +
                ", cluster=" + SemanticVersion.toString(PROTOCOL_SEMANTIC_VERSION);
            session.reject(EventCode.ERROR, detail, errorLog);
            rejectedBackupSessions.add(session);
        }
    }

    void onBackupQuery(
        final long correlationId,
        final int responseStreamId,
        final int version,
        final String responseChannel,
        final byte[] encodedCredentials,
        final Header header)
    {
        final ClusterSession session = new ClusterSession(
            memberId,
            NULL_VALUE,
            responseStreamId,
            responseChannel,
            sessionInfo(ClusterSession.Action.BACKUP.name(), header));

        final long nowNs = clusterClock.timeNanos();

        session.action(ClusterSession.Action.BACKUP);
        session.asyncConnect(aeron, tempBuffer, clusterId);
        session.lastActivityNs(nowNs, correlationId);

        if (AeronCluster.Configuration.PROTOCOL_MAJOR_VERSION == SemanticVersion.major(version))
        {
            authenticator.onConnectRequest(session.id(), encodedCredentials, NANOSECONDS.toMillis(nowNs));
            pendingBackupSessions.add(session);
        }
        else
        {
            final String detail = SESSION_INVALID_VERSION_MSG + " " + SemanticVersion.toString(version) +
                ", cluster=" + SemanticVersion.toString(PROTOCOL_SEMANTIC_VERSION);
            session.reject(EventCode.ERROR, detail, errorLog);
            rejectedBackupSessions.add(session);
        }
    }

    void onHeartbeatRequest(
        final long correlationId,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedCredentials,
        final Header header)
    {
        final ClusterSession session = new ClusterSession(
            memberId,
            NULL_VALUE,
            responseStreamId,
            responseChannel,
            sessionInfo(ClusterSession.Action.HEARTBEAT.name(), header));

        final long nowNs = clusterClock.timeNanos();
        session.action(ClusterSession.Action.HEARTBEAT);
        session.asyncConnect(aeron, tempBuffer, clusterId);
        session.lastActivityNs(nowNs, correlationId);

        authenticator.onConnectRequest(session.id(), encodedCredentials, NANOSECONDS.toMillis(nowNs));
        pendingBackupSessions.add(session);
    }

    void onChallengeResponseForBackupSession(
        final long correlationId,
        final long clusterSessionId,
        final byte[] encodedCredentials)
    {
        onChallengeResponseForSession(pendingBackupSessions, correlationId, clusterSessionId, encodedCredentials);
    }

    void onChallengeResponseForUserSession(
        final long correlationId,
        final long clusterSessionId,
        final byte[] encodedCredentials)
    {
        onChallengeResponseForSession(pendingUserSessions, correlationId, clusterSessionId, encodedCredentials);
    }

    private void onChallengeResponseForSession(
        final ArrayList<ClusterSession> pendingSessions,
        final long correlationId,
        final long clusterSessionId,
        final byte[] encodedCredentials)
    {
        for (int lastIndex = pendingSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = pendingSessions.get(i);

            if (session.id() == clusterSessionId && session.state() == CHALLENGED)
            {
                final long nowNs = clusterClock.timeNanos();
                session.lastActivityNs(nowNs, correlationId);
                authenticator.onChallengeResponse(clusterSessionId, encodedCredentials, NANOSECONDS.toMillis(nowNs));
                break;
            }
        }
    }

    void disconnectSessions()
    {
        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            final ClusterSession session = sessions.get(i);
            session.unlinkIngressImage();
            session.disconnect(aeron, errorHandler);
        }
    }

    void onServiceCloseSession(final long clusterSessionId, final boolean isActiveLeader, final long leadershipTermId)
    {
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null != session)
        {
            session.closing(CloseReason.SERVICE_ACTION);

            if (isActiveLeader)
            {
                final long timestamp = clusterClock.time();
                if (logPublisher.appendSessionClose(memberId, session, leadershipTermId, timestamp, clusterTimeUnit))
                {
                    logAppendSessionClose(
                        memberId, session.id(), session.closeReason(), leadershipTermId, timestamp, clusterTimeUnit);
                    final String msg = CloseReason.SERVICE_ACTION.name();
                    egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.CLOSED, msg);
                    session.closedLogPosition(logPublisher.position());
                    uncommittedClosedSessions.addLast(session);
                    closeSession(session);
                }
            }
        }
    }

    void onReplaySessionOpen(
        final long logPosition,
        final long correlationId,
        final long clusterSessionId,
        final long timestamp,
        final int responseStreamId,
        final String responseChannel)
    {
        final ClusterSession session = new ClusterSession(
            memberId, clusterSessionId, responseStreamId, responseChannel, responseChannel);
        session.open(logPosition);
        session.lastActivityNs(clusterClock.convertToNanos(timestamp), correlationId);

        addSession(session);
        if (clusterSessionId >= nextSessionId)
        {
            nextSessionId = clusterSessionId + 1;
            nextCommittedSessionId = nextSessionId;
        }

        if (null != consensusModuleExtension)
        {
            consensusModuleExtension.onSessionOpened(clusterSessionId);
        }
    }

    void onReplaySessionClose(final long clusterSessionId, final CloseReason closeReason)
    {
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null != session)
        {
            session.closing(closeReason);
            closeSession(session);
        }
    }

    int processAllPendingSessions(
        final long nowNs,
        final int leaderMemberId,
        final long leadershipTermId,
        final RecordingLog.RecoveryPlan recoveryPlan)
    {
        int workCount = 0;
        workCount += processPendingSessions(
            pendingUserSessions,
            rejectedUserSessions,
            nowNs,
            leaderMemberId,
            leadershipTermId,
            recoveryPlan);

        workCount += processPendingSessions(
            pendingBackupSessions,
            rejectedBackupSessions,
            nowNs,
            leaderMemberId,
            leadershipTermId,
            recoveryPlan);

        return workCount;
    }

    void onLoadClusterSession(
        final long clusterSessionId,
        final long correlationId,
        final long openedPosition,
        final long timeOfLastActivity,
        final CloseReason closeReason,
        final int responseStreamId,
        final String responseChannel)
    {
        final ClusterSession session = new ClusterSession(
            memberId, clusterSessionId, responseStreamId, responseChannel, responseChannel);

        session.loadSnapshotState(correlationId, openedPosition, timeOfLastActivity, closeReason);

        addSession(session);

        if (clusterSessionId >= nextSessionId)
        {
            nextSessionId = clusterSessionId + 1;
            nextCommittedSessionId = nextSessionId;
        }
    }

    void loadNextSessionId(final long nextSessionId)
    {
        this.nextSessionId = nextSessionId;
        this.nextCommittedSessionId = nextSessionId;
    }

    int processPendingBackupSessions(
        final long nowNs,
        final int leaderMemberId,
        final long leadershipTermId,
        final RecordingLog.RecoveryPlan recoveryPlan)
    {
        return processPendingSessions(
            pendingBackupSessions,
            rejectedBackupSessions,
            nowNs,
            leaderMemberId,
            leadershipTermId,
            recoveryPlan);
    }

    @SuppressWarnings("checkstyle:methodlength")
    int processPendingSessions(
        final ArrayList<ClusterSession> pendingSessions,
        final ArrayList<ClusterSession> rejectedSessions,
        final long nowNs,
        final int leaderMemberId,
        final long leadershipTermId,
        final RecordingLog.RecoveryPlan recoveryPlan)
    {
        int workCount = 0;

        for (int lastIndex = pendingSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = pendingSessions.get(i);

            if (session.state() == INVALID)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                session.close(aeron, errorHandler, "invalid session");
                continue;
            }

            if (nowNs > (session.timeOfLastActivityNs() + sessionTimeoutNs) && session.state() != INIT)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                session.close(aeron, errorHandler, "session timed out");
                timedOutCounter.incrementRelease();
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
                                errorLog);
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
                            session.close(aeron, errorHandler, "done");
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
                                errorLog);
                            break;
                        }

                        if (consensusPublisher.heartbeatResponse(session))
                        {
                            ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                            session.close(aeron, errorHandler, "done");
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
                                errorLog);
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
                                clusterTimeUnit,
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

                        standbySnapshotCounter.increment();
                        ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                        session.close(aeron, errorHandler, "done");
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

    int checkSessions(
        final long nowNs,
        final long leadershipTermId,
        final int leaderMemberId,
        final String ingressEndpoints)
    {
        final ArrayList<ClusterSession> sessions = this.sessions;
        int workCount = 0;

        for (int i = sessions.size() - 1; i >= 0; i--)
        {
            final ClusterSession session = sessions.get(i);

            if (nowNs > (session.timeOfLastActivityNs() + sessionTimeoutNs))
            {
                switch (session.state())
                {
                    case OPEN:
                    {
                        session.closing(CloseReason.TIMEOUT);

                        final long timestamp = clusterClock.time();
                        if (logPublisher.appendSessionClose(
                            memberId, session, leadershipTermId, timestamp, clusterTimeUnit))
                        {
                            logAppendSessionClose(
                                memberId,
                                session.id(),
                                session.closeReason(),
                                leadershipTermId,
                                timestamp,
                                clusterTimeUnit);
                            final String msg = session.closeReason().name();
                            egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.CLOSED, msg);
                            session.closedLogPosition(logPublisher.position());
                            uncommittedClosedSessions.addLast(session);
                            timedOutCounter.incrementRelease();
                            closeSession(session);
                        }
                        workCount++;
                        break;
                    }

                    case CLOSING:
                    {
                        final long timestamp = clusterClock.time();
                        if (logPublisher.appendSessionClose(
                            memberId, session, leadershipTermId, timestamp, clusterTimeUnit))
                        {
                            logAppendSessionClose(
                                memberId,
                                session.id(),
                                session.closeReason(),
                                leadershipTermId,
                                timestamp,
                                clusterTimeUnit);
                            final String msg = session.closeReason().name();
                            egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.CLOSED, msg);
                            session.closedLogPosition(logPublisher.position());
                            uncommittedClosedSessions.addLast(session);
                            if (session.closeReason() == CloseReason.TIMEOUT)
                            {
                                timedOutCounter.incrementRelease();
                            }
                            closeSession(session);
                            workCount++;
                        }
                        break;
                    }

                    default:
                    {
                        closeSession(session);
                        workCount++;
                        break;
                    }
                }
            }
            else if (session.hasOpenEventPending())
            {
                workCount += session.sendSessionOpenEvent(egressPublisher, leadershipTermId, memberId);
            }
            else if (session.hasNewLeaderEventPending())
            {
                workCount += sendNewLeaderEvent(session, leadershipTermId, leaderMemberId, ingressEndpoints);
            }
        }

        return workCount;
    }

    void prepareSessionsForNewTerm(final boolean isStartup)
    {
        if (isStartup)
        {
            for (int i = 0, size = sessions.size(); i < size; i++)
            {
                final ClusterSession session = sessions.get(i);
                if (session.state() == ClusterSession.State.OPEN)
                {
                    session.closing(CloseReason.TIMEOUT);
                }
            }
        }
        else
        {
            for (int i = 0, size = sessions.size(); i < size; i++)
            {
                final ClusterSession session = sessions.get(i);
                if (session.state() == ClusterSession.State.OPEN)
                {
                    session.connect(errorHandler, aeron, tempBuffer, clusterId);
                }
            }

            final long nowNs = clusterClock.timeNanos();
            for (int i = 0, size = sessions.size(); i < size; i++)
            {
                final ClusterSession session = sessions.get(i);
                if (session.state() == ClusterSession.State.OPEN)
                {
                    session.timeOfLastActivityNs(nowNs);
                    session.hasNewLeaderEventPending(true);
                }
            }
        }
    }

    int sendRedirects(
        final long leadershipTermId,
        final int leaderMemberId,
        final String ingressEndpoints,
        final long nowNs)
    {
        return sendRedirects(redirectUserSessions, leadershipTermId, leaderMemberId, ingressEndpoints, nowNs);
    }

    private int sendRedirects(
        final ArrayList<ClusterSession> redirectSessions,
        final long leadershipTermId,
        final int leaderMemberId,
        final String ingressEndpoints,
        final long nowNs)
    {
        int workCount = 0;

        for (int lastIndex = redirectSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = redirectSessions.get(i);
            final EventCode eventCode = EventCode.REDIRECT;

            if (session.isResponsePublicationConnected(aeron, nowNs) &&
                egressPublisher.sendEvent(session, leadershipTermId, leaderMemberId, eventCode, ingressEndpoints))
            {
                ArrayListUtil.fastUnorderedRemove(redirectSessions, i, lastIndex--);
                session.close(aeron, errorHandler, eventCode.name());
                workCount++;
            }
            else if (session.state() != ClusterSession.State.INIT &&
                nowNs > (session.timeOfLastActivityNs() + sessionTimeoutNs))
            {
                ArrayListUtil.fastUnorderedRemove(redirectSessions, i, lastIndex--);
                session.close(aeron, errorHandler, "session timed out");
                workCount++;
            }
            else if (session.state() == INVALID)
            {
                ArrayListUtil.fastUnorderedRemove(redirectSessions, i, lastIndex--);
                session.close(aeron, errorHandler, "invalid");
                workCount++;
            }
        }

        return workCount;
    }

    int sendRejections(
        final long leadershipTermId,
        final int leaderMemberId,
        final long nowNs)
    {
        int workCount = 0;

        workCount += sendRejections(rejectedUserSessions, leadershipTermId, leaderMemberId, nowNs);
        workCount += sendRejections(rejectedBackupSessions, leadershipTermId, leaderMemberId, nowNs);

        return workCount;
    }

    private int sendRejections(
        final ArrayList<ClusterSession> rejectedSessions,
        final long leadershipTermId,
        final int leaderMemberId,
        final long nowNs)
    {
        int workCount = 0;

        for (int lastIndex = rejectedSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = rejectedSessions.get(i);
            final String detail = session.responseDetail();
            final EventCode eventCode = session.eventCode();

            if (session.isResponsePublicationConnected(aeron, nowNs) &&
                egressPublisher.sendEvent(session, leadershipTermId, leaderMemberId, eventCode, detail))
            {
                ArrayListUtil.fastUnorderedRemove(rejectedSessions, i, lastIndex--);
                session.close(aeron, errorHandler, eventCode.name());
                workCount++;
            }
            else if (session.state() != ClusterSession.State.INIT &&
                nowNs > (session.timeOfLastActivityNs() + sessionTimeoutNs))
            {
                ArrayListUtil.fastUnorderedRemove(rejectedSessions, i, lastIndex--);
                session.close(aeron, errorHandler, "session timed out");
                workCount++;
            }
            else if (session.state() == INVALID)
            {
                ArrayListUtil.fastUnorderedRemove(rejectedSessions, i, lastIndex--);
                session.close(aeron, errorHandler, "invalid");
                workCount++;
            }
        }

        return workCount;
    }

    void clearSessionsAfter(final long logPosition, final long leadershipTermId)
    {
        for (int i = sessions.size() - 1; i >= 0; i--)
        {
            final ClusterSession session = sessions.get(i);
            if (session.openedLogPosition() > logPosition)
            {
                egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.CLOSED, "election");
                closeSession(session);
            }
        }

        for (final ClusterSession session : pendingUserSessions)
        {
            egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.CLOSED, "election");
            session.close(aeron, errorHandler, "election");
        }

        pendingUserSessions.clear();
    }

    void closeSessions(final CountedErrorHandler errorHandler, final ConsensusModuleAgent consensusModuleAgent)
    {
        for (final ClusterSession session : sessionByIdMap.values())
        {
            session.close(aeron, errorHandler, "Cluster node terminated");
        }
    }

    void sweepUncommittedSessions(final long commitPosition)
    {
        while (true)
        {
            final ClusterSession clusterSession = uncommittedClosedSessions.peekFirst();
            if (null == clusterSession || clusterSession.closedLogPosition() > commitPosition)
            {
                break;
            }

            uncommittedClosedSessions.pollFirst();
        }
    }

    void restoreUncommittedSessions(final long commitPosition)
    {
        ClusterSession session;
        while (null != (session = uncommittedClosedSessions.pollFirst()))
        {
            if (session.closedLogPosition() > commitPosition)
            {
                session.closedLogPosition(NULL_POSITION);
                session.state(CLOSING, "uncommitted session");
                addSession(session);
            }
        }
    }

    void updateTimeOfLastActivity()
    {
        final long nowNs = clusterClock.timeNanos();
        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            sessions.get(i).timeOfLastActivityNs(nowNs);
        }
    }

    void snapshotSessions(final ConsensusModuleSnapshotTaker snapshotTaker)
    {
        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            final ClusterSession session = sessions.get(i);
            final ClusterSession.State sessionState = session.state();

            if (sessionState == ClusterSession.State.OPEN || sessionState == CLOSING)
            {
                snapshotTaker.snapshotSession(session);
            }
        }
    }

    void timeoutOnUnavailableImage(final long imageCorrelationId, final ConsensusModuleAgent consensusModuleAgent)
    {
        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            final ClusterSession session = sessions.get(i);

            if (session.ingressImageCorrelationId() == imageCorrelationId && session.isOpen())
            {
                session.closing(CloseReason.TIMEOUT);
            }
        }
    }

    private int sendNewLeaderEvent(
        final ClusterSession session,
        final long leadershipTermId,
        final int leaderMemberId,
        final String ingressEndpoints)
    {
        if (egressPublisher.newLeader(session, leadershipTermId, leaderMemberId, ingressEndpoints))
        {
            session.hasNewLeaderEventPending(false);
            return 1;
        }

        return 0;
    }

    private static String sessionInfo(final String clientInfo, final Header header)
    {
        final Image image = (Image)header.context();
        final String imageInfo = "sourceIdentity=" + image.sourceIdentity() + " sessionId=" + image.sessionId();
        return Strings.isEmpty(clientInfo) ? imageInfo : clientInfo + " " + imageInfo;
    }
}
