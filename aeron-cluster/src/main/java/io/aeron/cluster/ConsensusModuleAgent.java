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
package io.aeron.cluster;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.RecordingSignalPoller;
import io.aeron.archive.client.ReplicationParams;
import io.aeron.archive.codecs.*;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.*;
import io.aeron.cluster.service.*;
import io.aeron.driver.DutyCycleTracker;
import io.aeron.driver.media.UdpChannel;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.security.Authenticator;
import io.aeron.security.AuthorisationService;
import io.aeron.status.LocalSocketAddressStatus;
import io.aeron.status.ReadableCounter;
import org.agrona.*;
import org.agrona.collections.*;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersReader;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.*;
import static io.aeron.archive.client.AeronArchive.NULL_LENGTH;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.ArchiveException.UNKNOWN_REPLAY;
import static io.aeron.archive.client.ReplayMerge.LIVE_ADD_MAX_WINDOW;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.cluster.ClusterSession.State.*;
import static io.aeron.cluster.ConsensusModule.CLUSTER_ACTION_FLAGS_DEFAULT;
import static io.aeron.cluster.ConsensusModule.CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT;
import static io.aeron.cluster.ConsensusModule.Configuration.*;
import static io.aeron.cluster.client.AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.MARK_FILE_UPDATE_INTERVAL_NS;
import static io.aeron.exceptions.AeronException.Category.WARN;
import static java.util.concurrent.TimeUnit.*;

final class ConsensusModuleAgent
    implements Agent, IdleStrategy, TimerService.TimerHandler, ConsensusModuleSnapshotListener, ConsensusModuleControl
{
    static final long SLOW_TICK_INTERVAL_NS = MILLISECONDS.toNanos(10);
    static final short APPEND_POSITION_FLAG_NONE = 0;
    static final short APPEND_POSITION_FLAG_CATCHUP = 1;

    private final long sessionTimeoutNs;
    private final long leaderHeartbeatIntervalNs;
    private final long leaderHeartbeatTimeoutNs;
    private long unavailableCounterHandlerRegistrationId;
    private long nextSessionId = 1;
    private long nextCommittedSessionId = nextSessionId;
    private long leadershipTermId = NULL_VALUE;
    private long expectedAckPosition = 0;
    private long serviceAckId = 0;
    private long terminationPosition = NULL_POSITION;
    private long notifiedCommitPosition = 0;
    private long lastAppendPosition = NULL_POSITION;
    private long timeOfLastLogUpdateNs = 0;
    private long timeOfLastAppendPositionUpdateNs = 0;
    private long timeOfLastAppendPositionSendNs = 0;
    private long timeOfLastLeaderUpdateNs;
    private long slowTickDeadlineNs = 0;
    private long markFileUpdateDeadlineNs = 0;

    private final ClusterMember[] activeMembers;
    private final ClusterMember thisMember;
    private final long[] rankedPositions;
    private final long[] serviceClientIds;
    private final int serviceCount;
    private final int memberId;
    private final Counter commitPosition;

    private long logPublicationChannelTag;
    private ReadableCounter appendPosition = null;
    private ConsensusModule.State state = ConsensusModule.State.INIT;
    private Cluster.Role role = Cluster.Role.FOLLOWER;
    private ClusterMember leaderMember;
    private final ArrayDeque<ServiceAck>[] serviceAckQueues;
    private final Counter clusterRoleCounter;
    private final ClusterMarkFile markFile;
    private final AgentInvoker aeronClientInvoker;
    private final ClusterClock clusterClock;
    private final LongConsumer clusterTimeConsumer;
    private final TimeUnit clusterTimeUnit;
    private final TimerService timerService;
    private final Counter moduleState;
    private final Counter controlToggle;
    private final Counter nodeControlToggle;
    private final ConsensusModuleAdapter consensusModuleAdapter;
    private final ServiceProxy serviceProxy;
    private final IngressAdapter ingressAdapter;
    private final EgressPublisher egressPublisher;
    private final LogPublisher logPublisher;
    private final LogAdapter logAdapter;
    private final ConsensusAdapter consensusAdapter;
    private final ConsensusPublisher consensusPublisher = new ConsensusPublisher();
    private final Long2ObjectHashMap<ClusterSession> sessionByIdMap = new Long2ObjectHashMap<>();
    private final ArrayList<ClusterSession> sessions = new ArrayList<>();
    private final ArrayList<ClusterSession> pendingUserSessions = new ArrayList<>();
    private final ArrayList<ClusterSession> rejectedUserSessions = new ArrayList<>();
    private final ArrayList<ClusterSession> redirectUserSessions = new ArrayList<>();

    private final ArrayList<ClusterSession> pendingBackupSessions = new ArrayList<>();
    private final ArrayList<ClusterSession> rejectedBackupSessions = new ArrayList<>();

    private final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();
    private final Long2LongCounterMap expiredTimerCountByCorrelationIdMap = new Long2LongCounterMap(0);
    private final ArrayDeque<ClusterSession> uncommittedClosedSessions = new ArrayDeque<>();
    private final LongArrayQueue uncommittedTimers = new LongArrayQueue(Long.MAX_VALUE);
    private final PendingServiceMessageTracker[] pendingServiceMessageTrackers;
    private final ConsensusModuleExtension consensusModuleExtension;
    private final Authenticator authenticator;
    private final AuthorisationService authorisationService;
    private final ClusterSessionProxy sessionProxy;
    private final Aeron aeron;
    private final ConsensusModule.Context ctx;
    private final IdleStrategy idleStrategy;
    private final RecordingLog recordingLog;
    private final DutyCycleTracker dutyCycleTracker;
    private final SnapshotDurationTracker totalSnapshotDurationTracker;
    private final ChannelUri responseChannelTemplate;
    private RecordingLog.RecoveryPlan recoveryPlan;
    private AeronArchive archive;
    private AeronArchive extensionArchive;
    private RecordingSignalPoller recordingSignalPoller;
    private Election election;
    private ClusterTermination clusterTermination;
    private long logSubscriptionId = NULL_VALUE;
    private long logRecordingId = NULL_VALUE;
    private long logRecordingStopPosition = 0;
    private String liveLogDestination;
    private String catchupLogDestination;
    private String ingressEndpoints;
    private StandbySnapshotReplicator standbySnapshotReplicator = null;
    private String localLogChannel;

    ConsensusModuleAgent(final ConsensusModule.Context ctx)
    {
        this.ctx = ctx;
        this.aeron = ctx.aeron();
        this.clusterClock = ctx.clusterClock();
        this.clusterTimeUnit = clusterClock.timeUnit();
        this.clusterTimeConsumer = ctx.clusterTimeConsumerSupplier().apply(ctx);
        this.timerService = ctx.timerServiceSupplier().newInstance(clusterTimeUnit, this);
        this.sessionTimeoutNs = ctx.sessionTimeoutNs();
        this.leaderHeartbeatIntervalNs = ctx.leaderHeartbeatIntervalNs();
        this.leaderHeartbeatTimeoutNs = ctx.leaderHeartbeatTimeoutNs();
        this.egressPublisher = ctx.egressPublisher();
        this.moduleState = ctx.moduleStateCounter();
        this.commitPosition = ctx.commitPositionCounter();
        this.controlToggle = ctx.controlToggleCounter();
        this.nodeControlToggle = ctx.nodeControlToggleCounter();
        this.logPublisher = ctx.logPublisher();
        this.idleStrategy = ctx.idleStrategy();
        this.activeMembers = ClusterMember.parse(ctx.clusterMembers());
        this.sessionProxy = new ClusterSessionProxy(egressPublisher);
        this.memberId = ctx.clusterMemberId();
        this.clusterRoleCounter = ctx.clusterNodeRoleCounter();
        this.markFile = ctx.clusterMarkFile();
        this.recordingLog = ctx.recordingLog();
        this.serviceClientIds = new long[ctx.serviceCount()];
        Arrays.fill(serviceClientIds, NULL_VALUE);
        this.serviceCount = ctx.serviceCount();
        this.serviceAckQueues = ServiceAck.newArrayOfQueues(serviceCount);
        this.dutyCycleTracker = ctx.dutyCycleTracker();
        this.totalSnapshotDurationTracker = ctx.totalSnapshotDurationTracker();

        aeronClientInvoker = aeron.conductorAgentInvoker();
        aeronClientInvoker.invoke();

        rankedPositions = new long[ClusterMember.quorumThreshold(activeMembers.length)];
        role(Cluster.Role.FOLLOWER);

        ClusterMember.addClusterMemberIds(activeMembers, clusterMemberByIdMap);
        thisMember = ClusterMember.determineMember(activeMembers, ctx.clusterMemberId(), ctx.memberEndpoints());
        leaderMember = thisMember;

        final ChannelUri consensusUri = ChannelUri.parse(ctx.consensusChannel());
        if (!consensusUri.containsKey(ENDPOINT_PARAM_NAME))
        {
            consensusUri.put(ENDPOINT_PARAM_NAME, thisMember.consensusEndpoint());
        }

        consensusAdapter = new ConsensusAdapter(
            aeron.addSubscription(consensusUri.toString(), ctx.consensusStreamId()), this);

        ingressAdapter = new IngressAdapter(ctx.ingressFragmentLimit(), this);
        logAdapter = new LogAdapter(this, ctx.logFragmentLimit());

        consensusModuleAdapter = new ConsensusModuleAdapter(
            aeron.addSubscription(ctx.controlChannel(), ctx.consensusModuleStreamId()), this);
        serviceProxy = new ServiceProxy(aeron.addPublication(ctx.controlChannel(), ctx.serviceStreamId()));

        authenticator = ctx.authenticatorSupplier().get();
        authorisationService = ctx.authorisationServiceSupplier().get();

        pendingServiceMessageTrackers = new PendingServiceMessageTracker[ctx.serviceCount()];
        for (int i = 0, size = ctx.serviceCount(); i < size; i++)
        {
            pendingServiceMessageTrackers[i] = new PendingServiceMessageTracker(
                i, commitPosition, logPublisher, clusterClock);
        }
        this.consensusModuleExtension = ctx.consensusModuleExtension();
        responseChannelTemplate = Strings.isEmpty(ctx.egressChannel()) ? null : ChannelUri.parse(ctx.egressChannel());
    }

    /**
     * {@inheritDoc}
     */
    public void onClose()
    {
        if (!aeron.isClosed())
        {
            aeron.removeUnavailableCounterHandler(unavailableCounterHandlerRegistrationId);
            CloseHelper.close(consensusModuleExtension);

            final CountedErrorHandler errorHandler = ctx.countedErrorHandler();
            logPublisher.disconnect(errorHandler);
            CloseHelper.close(logAdapter.subscription());
            tryStopLogRecording();

            if (!ctx.ownsAeronClient())
            {
                ClusterMember.closeConsensusPublications(errorHandler, activeMembers);
                CloseHelper.close(errorHandler, ingressAdapter);
                CloseHelper.close(errorHandler, consensusAdapter);
                CloseHelper.close(errorHandler, serviceProxy);
                CloseHelper.close(errorHandler, consensusModuleAdapter);
                CloseHelper.close(errorHandler, archive);

                for (final ClusterSession session : sessionByIdMap.values())
                {
                    session.close(aeron, errorHandler);
                }
            }

            state(ConsensusModule.State.CLOSED);
        }

        markFile.updateActivityTimestamp(NULL_VALUE);
        markFile.force();
        ctx.close();
    }

    /**
     * {@inheritDoc}
     */
    public void onStart()
    {
        archive = AeronArchive.connect(ctx.archiveContext().clone());
        recordingSignalPoller = new RecordingSignalPoller(
            archive.controlSessionId(), archive.controlResponsePoller().subscription());

        final long lastTermRecordingId = recordingLog.findLastTermRecordingId();
        if (NULL_VALUE != lastTermRecordingId)
        {
            archive.tryStopRecordingByIdentity(lastTermRecordingId);
        }

        if (null == ctx.boostrapState())
        {
            replicateStandbySnapshotsForStartup();
            recoveryPlan = recoverFromSnapshotAndLog();
        }
        else
        {
            recoveryPlan = recoverFromBootstrapState();
        }

        ClusterMember.addConsensusPublications(
            activeMembers,
            thisMember,
            ctx.consensusChannel(),
            ctx.consensusStreamId(),
            aeron,
            ctx.countedErrorHandler());

        final long lastLeadershipTermId = recoveryPlan.lastLeadershipTermId;
        final long commitPosition = this.commitPosition.getWeak();
        final long appendedPosition = recoveryPlan.appendedLogPosition;
        logNewElection(memberId, lastLeadershipTermId, commitPosition, appendedPosition, "node started");

        election = new Election(
            true,
            NULL_VALUE,
            lastLeadershipTermId,
            recoveryPlan.lastTermBaseLogPosition,
            commitPosition,
            appendedPosition,
            activeMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            this);

        election.doWork(clusterClock.timeNanos());
        state(ConsensusModule.State.ACTIVE);

        if (null != consensusModuleExtension)
        {
            extensionArchive = AeronArchive.connect(ctx.archiveContext().clone());
        }

        unavailableCounterHandlerRegistrationId = aeron.addUnavailableCounterHandler(this::onUnavailableCounter);
        dutyCycleTracker.update(clusterClock.timeNanos());
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        final long timestamp = clusterClock.time();
        final long nowNs = clusterClock.convertToNanos(timestamp);
        int workCount = 0;

        dutyCycleTracker.measureAndUpdate(nowNs);

        try
        {
            if (nowNs >= slowTickDeadlineNs)
            {
                final int slowTickWorkCount = slowTickWork(nowNs);

                workCount += slowTickWorkCount;
                slowTickDeadlineNs = slowTickWorkCount > 0 ? nowNs + 1 : nowNs + SLOW_TICK_INTERVAL_NS;
            }

            workCount += consensusAdapter.poll();

            if (null != election)
            {
                workCount += election.doWork(nowNs);
            }
            else
            {
                workCount += consensusWork(timestamp, nowNs);
            }
        }
        catch (final AgentTerminationException ex)
        {
            runTerminationHook();
            throw ex;
        }
        catch (final Exception ex)
        {
            if (null != election)
            {
                election.handleError(nowNs, ex);
            }
            else
            {
                throw ex;
            }
        }

        clusterTimeConsumer.accept(timestamp);

        return workCount;
    }

    public ConsensusModule.Context context()
    {
        return ctx;
    }

    /**
     * {@inheritDoc}
     */
    public int memberId()
    {
        return memberId;
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return ctx.agentRoleName();
    }

    /**
     * {@inheritDoc}
     */
    public long time()
    {
        return clusterClock.time();
    }

    /**
     * {@inheritDoc}
     */
    public TimeUnit timeUnit()
    {
        return clusterTimeUnit;
    }

    /**
     * {@inheritDoc}
     */
    public IdleStrategy idleStrategy()
    {
        return this;
    }

    public void idle()
    {
        checkInterruptStatus();
        aeronClientInvoker.invoke();
        if (aeron.isClosed())
        {
            throw new AgentTerminationException("unexpected Aeron close");
        }

        idleStrategy.idle();
        pollArchiveEvents();
    }

    public void idle(final int workCount)
    {
        checkInterruptStatus();
        aeronClientInvoker.invoke();
        if (aeron.isClosed())
        {
            throw new AgentTerminationException("unexpected Aeron close");
        }

        idleStrategy.idle(workCount);

        if (0 == workCount)
        {
            pollArchiveEvents();
        }
    }

    public void reset()
    {
        idleStrategy.reset();
    }

    /**
     * {@inheritDoc}
     */
    public Aeron aeron()
    {
        return aeron;
    }

    /**
     * {@inheritDoc}
     */
    public AeronArchive archive()
    {
        return extensionArchive;
    }

    /**
     * {@inheritDoc}
     */
    public AuthorisationService authorisationService()
    {
        return authorisationService;
    }

    /**
     * {@inheritDoc}
     */
    public ClusterClientSession getClientSession(final long clusterSessionId)
    {
        return sessionByIdMap.get(clusterSessionId);
    }

    /**
     * {@inheritDoc}
     */
    public void closeClusterSession(final long clusterSessionId)
    {
        onServiceCloseSession(clusterSessionId);
    }

    /**
     * {@inheritDoc}
     */
    public int commitPositionCounterId()
    {
        return ctx.commitPositionCounter().id();
    }

    /**
     * {@inheritDoc}
     */
    public int clusterId()
    {
        return ctx.clusterId();
    }

    public ClusterMember clusterMember()
    {
        return thisMember;
    }

    public void onLoadBeginSnapshot(
        final int appVersion, final TimeUnit timeUnit, final DirectBuffer buffer, final int offset, final int length)
    {
        if (!ctx.appVersionValidator().isVersionCompatible(ctx.appVersion(), appVersion))
        {
            throw new ClusterException(
                "incompatible version: " + SemanticVersion.toString(ctx.appVersion()) +
                " snapshot=" + SemanticVersion.toString(appVersion),
                AeronException.Category.FATAL);
        }

        if (timeUnit != clusterTimeUnit)
        {
            throw new ClusterException(
                "incompatible time unit: " + clusterTimeUnit + " snapshot=" + timeUnit, AeronException.Category.FATAL);
        }
    }

    public ControlledFragmentHandler.Action onExtensionMessage(
        final int actingBlockLength,
        final int templateId,
        final int schemaId,
        final int actingVersion,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        if (null != consensusModuleExtension)
        {
            return consensusModuleExtension.onIngressExtensionMessage(
                actingBlockLength, templateId, schemaId, actingVersion, buffer, offset, length, header);
        }
        else
        {
            ctx.countedErrorHandler().onError(new ClusterEvent(
                "expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId));

            return ControlledFragmentHandler.Action.CONTINUE;
        }
    }

    public void onLoadEndSnapshot(final DirectBuffer buffer, final int offset, final int length)
    {
    }

    public void onLoadClusterSession(
        final long clusterSessionId,
        final long correlationId,
        final long openedPosition,
        final long timeOfLastActivity,
        final CloseReason closeReason,
        final int responseStreamId,
        final String responseChannel,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        final ClusterSession session = new ClusterSession(clusterSessionId, responseStreamId, responseChannel);

        session.loadSnapshotState(correlationId, openedPosition, timeOfLastActivity, closeReason);

        addSession(session);

        if (clusterSessionId >= nextSessionId)
        {
            nextSessionId = clusterSessionId + 1;
            nextCommittedSessionId = nextSessionId;
        }
    }

    public void onLoadConsensusModuleState(
        final long nextSessionId,
        final long nextServiceSessionId,
        final long logServiceSessionId,
        final int pendingMessageCapacity,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        this.nextSessionId = nextSessionId;
        this.nextCommittedSessionId = nextSessionId;
        if (pendingServiceMessageTrackers.length > 0)
        {
            pendingServiceMessageTrackers[0].loadState(
                nextServiceSessionId, logServiceSessionId, pendingMessageCapacity);
        }
    }

    public void onLoadPendingMessageTracker(
        final long nextServiceSessionId,
        final long logServiceSessionId,
        final int pendingMessageCapacity,
        final int serviceId,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        if (serviceId < 0 || serviceId >= pendingServiceMessageTrackers.length)
        {
            throw new ClusterException(
                "serviceId=" + serviceId + " invalid for serviceCount=" + pendingServiceMessageTrackers.length);
        }

        pendingServiceMessageTrackers[serviceId].loadState(
            nextServiceSessionId, logServiceSessionId, pendingMessageCapacity);
    }

    public void onLoadPendingMessage(
        final long clusterSessionId, final DirectBuffer buffer, final int offset, final int length)
    {
        final int serviceId = PendingServiceMessageTracker.serviceIdFromLogMessage(clusterSessionId);
        pendingServiceMessageTrackers[serviceId].appendMessage(buffer, offset, length);
    }

    public void onLoadTimer(
        final long correlationId, final long deadline, final DirectBuffer buffer, final int offset, final int length)
    {
        onScheduleTimer(correlationId, deadline);
    }

    public void onSessionConnect(
        final long correlationId,
        final int responseStreamId,
        final int version,
        final String responseChannel,
        final byte[] encodedCredentials)
    {
        final long clusterSessionId = Cluster.Role.LEADER == role ? nextSessionId++ : NULL_VALUE;
        final ClusterSession session = new ClusterSession(clusterSessionId, responseStreamId,
            refineResponseChannel(responseChannel));

        session.asyncConnect(aeron);
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
                session.reject(EventCode.ERROR, detail, ctx.errorLog(), clusterMemberId());
                rejectedUserSessions.add(session);
            }
            else if (pendingUserSessions.size() + sessions.size() >= ctx.maxConcurrentSessions())
            {
                session.reject(EventCode.ERROR, SESSION_LIMIT_MSG, ctx.errorLog(), clusterMemberId());
                rejectedUserSessions.add(session);
            }
            else
            {
                authenticator.onConnectRequest(session.id(), encodedCredentials, NANOSECONDS.toMillis(nowNs));
                pendingUserSessions.add(session);
            }
        }
    }

    void onSessionClose(final long leadershipTermId, final long clusterSessionId)
    {
        if (leadershipTermId == this.leadershipTermId && Cluster.Role.LEADER == role)
        {
            final ClusterSession session = sessionByIdMap.get(clusterSessionId);
            if (null != session && session.isOpen())
            {
                session.closing(CloseReason.CLIENT_ACTION);
                session.disconnect(aeron, ctx.countedErrorHandler());

                if (logPublisher.appendSessionClose(
                    memberId, session, leadershipTermId, clusterClock.time(), clusterTimeUnit))
                {
                    session.closedLogPosition(logPublisher.position());
                    uncommittedClosedSessions.addLast(session);
                    closeSession(session);
                }
            }
        }
    }

    void onAdminRequest(
        final long leadershipTermId,
        final long clusterSessionId,
        final long correlationId,
        final AdminRequestType requestType,
        final DirectBuffer payload,
        final int payloadOffset,
        final int payloadLength)
    {
        if (Cluster.Role.LEADER != role || leadershipTermId != this.leadershipTermId)
        {
            return;
        }

        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null == session || session.state() != ClusterSession.State.OPEN)
        {
            return;
        }

        if (!authorisationService.isAuthorised(
            MessageHeaderDecoder.SCHEMA_ID, AdminRequestDecoder.TEMPLATE_ID, requestType, session.encodedPrincipal()))
        {
            final String msg = "Execution of the " + requestType + " request was not authorised";
            egressPublisher.sendAdminResponse(
                session, correlationId, requestType, AdminResponseCode.UNAUTHORISED_ACCESS, msg);
            return;
        }

        if (AdminRequestType.SNAPSHOT == requestType)
        {
            if (ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle))
            {
                egressPublisher.sendAdminResponse(session, correlationId, requestType, AdminResponseCode.OK, "");
            }
            else
            {
                final String msg = "Failed to switch ClusterControl to the ToggleState.SNAPSHOT state";
                egressPublisher.sendAdminResponse(session, correlationId, requestType, AdminResponseCode.ERROR, msg);
            }
        }
        else
        {
            egressPublisher.sendAdminResponse(
                session, correlationId, requestType, AdminResponseCode.ERROR, "Unknown request type: " + requestType);
        }
    }

    ControlledFragmentAssembler.Action onIngressMessage(
        final long leadershipTermId,
        final long clusterSessionId,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        if (leadershipTermId == this.leadershipTermId && Cluster.Role.LEADER == role)
        {
            final ClusterSession session = sessionByIdMap.get(clusterSessionId);
            if (null != session && session.isOpen())
            {
                final long timestamp = clusterClock.time();
                if (logPublisher.appendMessage(
                    leadershipTermId, clusterSessionId, timestamp, buffer, offset, length) > 0)
                {
                    session.timeOfLastActivityNs(clusterClock.convertToNanos(timestamp));
                }
                else
                {
                    return ControlledFragmentHandler.Action.ABORT;
                }
            }
        }

        return ControlledFragmentHandler.Action.CONTINUE;
    }

    void onSessionKeepAlive(final long leadershipTermId, final long clusterSessionId)
    {
        if (leadershipTermId == this.leadershipTermId && Cluster.Role.LEADER == role)
        {
            final ClusterSession session = sessionByIdMap.get(clusterSessionId);
            if (null != session && session.state() == ClusterSession.State.OPEN)
            {
                session.timeOfLastActivityNs(clusterClock.timeNanos());
            }
        }
    }

    void onIngressChallengeResponse(
        final long correlationId, final long clusterSessionId, final byte[] encodedCredentials)
    {
        if (Cluster.Role.LEADER == role)
        {
            onChallengeResponseForSession(pendingUserSessions, correlationId, clusterSessionId, encodedCredentials);
        }
        else
        {
            consensusPublisher.challengeResponse(
                leaderMember.publication(), correlationId, clusterSessionId, encodedCredentials);
        }
    }

    void onConsensusChallengeResponse(
        final long correlationId, final long clusterSessionId, final byte[] encodedCredentials)
    {
        onChallengeResponseForSession(pendingBackupSessions, correlationId, clusterSessionId, encodedCredentials);
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

    public boolean onTimerEvent(final long correlationId)
    {
        final long appendPosition = logPublisher.appendTimer(correlationId, leadershipTermId, clusterClock.time());
        if (appendPosition > 0)
        {
            uncommittedTimers.offerLong(appendPosition);
            uncommittedTimers.offerLong(correlationId);
            return true;
        }

        return false;
    }

    void onCanvassPosition(
        final long logLeadershipTermId,
        final long logPosition,
        final long leadershipTermId,
        final int followerMemberId,
        final int protocolVersion)
    {
        logOnCanvassPosition(
            memberId, logLeadershipTermId, logPosition, leadershipTermId, followerMemberId, protocolVersion);
        checkFollowerForConsensusPublication(followerMemberId);

        if (null != election)
        {
            election.onCanvassPosition(
                logLeadershipTermId, logPosition, leadershipTermId, followerMemberId, protocolVersion);
        }
        else if (Cluster.Role.LEADER == role)
        {
            final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
            if (null != follower && logLeadershipTermId <= this.leadershipTermId)
            {
                stopExistingCatchupReplay(follower);

                final RecordingLog.Entry currentTermEntry = recordingLog.getTermEntry(this.leadershipTermId);
                final long termBaseLogPosition = currentTermEntry.termBaseLogPosition;
                long nextLogLeadershipTermId = NULL_VALUE;
                long nextTermBaseLogPosition = NULL_POSITION;
                long nextLogPosition = NULL_POSITION;

                if (logLeadershipTermId < this.leadershipTermId)
                {
                    final RecordingLog.Entry nextLogEntry = recordingLog.findTermEntry(logLeadershipTermId + 1);
                    nextLogLeadershipTermId = null != nextLogEntry ?
                        nextLogEntry.leadershipTermId : this.leadershipTermId;
                    nextTermBaseLogPosition = null != nextLogEntry ?
                        nextLogEntry.termBaseLogPosition : termBaseLogPosition;
                    nextLogPosition = null != nextLogEntry ? nextLogEntry.logPosition : NULL_POSITION;
                }

                consensusPublisher.newLeadershipTerm(
                    follower.publication(),
                    logLeadershipTermId,
                    nextLogLeadershipTermId,
                    nextTermBaseLogPosition,
                    nextLogPosition,
                    this.leadershipTermId,
                    termBaseLogPosition,
                    logPublisher.position(),
                    logRecordingId,
                    clusterClock.time(),
                    memberId,
                    logPublisher.sessionId(),
                    ctx.appVersion(),
                    false);
            }
        }
    }

    void onRequestVote(
        final long logLeadershipTermId,
        final long logPosition,
        final long candidateTermId,
        final int candidateId,
        final int protocolVersion)
    {
        logOnRequestVote(memberId, logLeadershipTermId, logPosition, candidateTermId, candidateId, protocolVersion);
        if (null != election)
        {
            election.onRequestVote(logLeadershipTermId, logPosition, candidateTermId, candidateId, protocolVersion);
        }
        else if (candidateTermId > leadershipTermId)
        {
            enterElection(false, "unexpected vote request");
        }
    }

    void onVote(
        final long candidateTermId,
        final long logLeadershipTermId,
        final long logPosition,
        final int candidateMemberId,
        final int followerMemberId,
        final boolean vote)
    {
        if (null != election)
        {
            election.onVote(
                candidateTermId, logLeadershipTermId, logPosition, candidateMemberId, followerMemberId, vote);
        }
    }

    void onNewLeadershipTerm(
        final long logLeadershipTermId,
        final long nextLeadershipTermId,
        final long nextTermBaseLogPosition,
        final long nextLogPosition,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long leaderRecordingId,
        final long timestamp,
        final int leaderId,
        final int logSessionId,
        final int appVersion,
        final boolean isStartup)
    {
        logOnNewLeadershipTerm(
            memberId,
            logLeadershipTermId,
            nextLeadershipTermId,
            nextTermBaseLogPosition,
            nextLogPosition,
            leadershipTermId,
            termBaseLogPosition,
            logPosition,
            leaderRecordingId,
            timestamp,
            leaderId,
            logSessionId,
            appVersion,
            isStartup);

        if (!ctx.appVersionValidator().isVersionCompatible(ctx.appVersion(), appVersion))
        {
            ctx.countedErrorHandler().onError(new ClusterException(
                "incompatible version: " + SemanticVersion.toString(ctx.appVersion()) +
                " log=" + SemanticVersion.toString(appVersion)));
            unexpectedTermination();
        }

        final long nowNs = clusterClock.timeNanos();
        if (leadershipTermId >= this.leadershipTermId)
        {
            timeOfLastLeaderUpdateNs = nowNs;
        }

        if (null != election)
        {
            election.onNewLeadershipTerm(
                logLeadershipTermId,
                nextLeadershipTermId,
                nextTermBaseLogPosition,
                nextLogPosition,
                leadershipTermId,
                termBaseLogPosition,
                logPosition,
                leaderRecordingId,
                timestamp,
                leaderId,
                logSessionId,
                isStartup);
        }
        else if (Cluster.Role.FOLLOWER == role &&
            leadershipTermId == this.leadershipTermId &&
            leaderId == leaderMember.id())
        {
            notifiedCommitPosition = Math.max(notifiedCommitPosition, logPosition);
            timeOfLastLogUpdateNs = nowNs;
        }
        else if (leadershipTermId > this.leadershipTermId)
        {
            enterElection(false, "unexpected new leadership term event");
        }
    }

    void onAppendPosition(
        final long leadershipTermId,
        final long logPosition,
        final int followerMemberId,
        final short flags)
    {
        logOnAppendPosition(memberId, leadershipTermId, logPosition, followerMemberId, flags);
        if (null != election)
        {
            election.onAppendPosition(leadershipTermId, logPosition, followerMemberId, flags);
        }
        else if (leadershipTermId <= this.leadershipTermId && Cluster.Role.LEADER == role)
        {
            final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
            if (null != follower)
            {
                follower
                    .logPosition(logPosition)
                    .timeOfLastAppendPositionNs(clusterClock.timeNanos());
                trackCatchupCompletion(follower, leadershipTermId, flags);
            }
        }
    }

    void onCommitPosition(final long leadershipTermId, final long logPosition, final int leaderMemberId)
    {
        logOnCommitPosition(memberId, leadershipTermId, logPosition, leaderMemberId);

        final long nowNs = clusterClock.timeNanos();
        if (leadershipTermId >= this.leadershipTermId)
        {
            timeOfLastLeaderUpdateNs = nowNs;
        }

        if (null != election)
        {
            election.onCommitPosition(leadershipTermId, logPosition, leaderMemberId);
        }
        else if (leadershipTermId == this.leadershipTermId &&
            leaderMemberId == leaderMember.id() &&
            Cluster.Role.FOLLOWER == role)
        {
            notifiedCommitPosition = logPosition;
            timeOfLastLogUpdateNs = nowNs;
        }
        else if (leadershipTermId > this.leadershipTermId)
        {
            enterElection(false, "unexpected commit position from new leader");
        }
    }

    void onCatchupPosition(
        final long leadershipTermId, final long logPosition, final int followerMemberId, final String catchupEndpoint)
    {
        logOnCatchupPosition(memberId, leadershipTermId, logPosition, followerMemberId, catchupEndpoint);
        if (leadershipTermId <= this.leadershipTermId && Cluster.Role.LEADER == role)
        {
            final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
            if (null != follower && follower.catchupReplaySessionId() == NULL_VALUE)
            {
                final ChannelUri channel = ChannelUri.parse(ctx.followerCatchupChannel());
                channel.put(ENDPOINT_PARAM_NAME, catchupEndpoint);
                channel.put(SESSION_ID_PARAM_NAME, Integer.toString(logPublisher.sessionId()));
                channel.put(LINGER_PARAM_NAME, "0");
                channel.put(EOS_PARAM_NAME, "false");

                follower.catchupReplaySessionId(archive.startReplay(
                    logRecordingId, logPosition, Long.MAX_VALUE, channel.toString(), ctx.logStreamId()));
                follower.catchupReplayCorrelationId(archive.lastCorrelationId());
            }
        }
    }

    void onStopCatchup(final long leadershipTermId, final int followerMemberId)
    {
        logOnStopCatchup(memberId, leadershipTermId, followerMemberId);
        if (leadershipTermId == this.leadershipTermId && followerMemberId == memberId)
        {
            if (null != catchupLogDestination)
            {
                logAdapter.asyncRemoveDestination(catchupLogDestination);
                catchupLogDestination = null;
            }
        }
    }

    void onTerminationPosition(final long leadershipTermId, final long logPosition)
    {
        logOnTerminationPosition(memberId, leadershipTermId, logPosition);

        if (leadershipTermId == this.leadershipTermId && Cluster.Role.FOLLOWER == role)
        {
            terminationPosition = logPosition;
            timeOfLastLogUpdateNs = clusterClock.timeNanos();
        }
    }

    void onTerminationAck(final long leadershipTermId, final long logPosition, final int memberId)
    {
        logOnTerminationAck(this.memberId, leadershipTermId, logPosition, memberId);

        if (leadershipTermId == this.leadershipTermId &&
            logPosition >= terminationPosition &&
            Cluster.Role.LEADER == role)
        {
            final ClusterMember member = clusterMemberByIdMap.get(memberId);
            if (null != member)
            {
                member.hasTerminated(true);

                if (clusterTermination.canTerminate(activeMembers, clusterClock.timeNanos()))
                {
                    recordingLog.commitLogPosition(leadershipTermId, terminationPosition);
                    closeAndTerminate();
                }
            }
        }
    }

    void onBackupQuery(
        final long correlationId,
        final int responseStreamId,
        final int version,
        final String responseChannel,
        final byte[] encodedCredentials)
    {
        if (null == election)
        {
            if (state == ConsensusModule.State.ACTIVE || state == ConsensusModule.State.SUSPENDED)
            {
                final ClusterSession session = new ClusterSession(
                    Aeron.NULL_VALUE,
                    responseStreamId,
                    refineResponseChannel(responseChannel));

                final long nowNs = clusterClock.timeNanos();

                session.action(ClusterSession.Action.BACKUP);
                session.asyncConnect(aeron);
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
                    session.reject(EventCode.ERROR, detail, ctx.errorLog(), clusterMemberId());
                    rejectedBackupSessions.add(session);
                }
            }
        }
    }

    public void onHeartbeatRequest(
        final long correlationId,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedCredentials)
    {
        if (null == election)
        {
            if (state == ConsensusModule.State.ACTIVE || state == ConsensusModule.State.SUSPENDED)
            {
                final ClusterSession session = new ClusterSession(
                    Aeron.NULL_VALUE,
                    responseStreamId,
                    refineResponseChannel(responseChannel));

                session.action(ClusterSession.Action.HEARTBEAT);
                session.asyncConnect(aeron);

                final long nowNs = clusterClock.timeNanos();

                session.lastActivityNs(nowNs, correlationId);
                authenticator.onConnectRequest(session.id(), encodedCredentials, NANOSECONDS.toMillis(nowNs));
                pendingBackupSessions.add(session);
            }
        }
    }

    void onClusterMembersQuery(final long correlationId, final boolean isExtendedRequest)
    {
        if (isExtendedRequest)
        {
            serviceProxy.clusterMembersExtendedResponse(
                correlationId, clusterClock.timeNanos(), leaderMember.id(), memberId, activeMembers);
        }
        else
        {
            serviceProxy.clusterMembersResponse(
                correlationId,
                leaderMember.id(),
                ClusterMember.encodeAsString(activeMembers));
        }
    }

    void onStandbySnapshot(
        final long correlationId,
        final int version,
        final List<StandbySnapshotEntry> standbySnapshotEntries,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedCredentials)
    {
        if (null == election)
        {
            if (state == ConsensusModule.State.ACTIVE || state == ConsensusModule.State.SUSPENDED)
            {
                final ClusterSession session = new ClusterSession(
                    Aeron.NULL_VALUE,
                    responseStreamId,
                    refineResponseChannel(responseChannel));

                final long nowNs = clusterClock.timeNanos();

                session.action(ClusterSession.Action.STANDBY_SNAPSHOT);
                session.asyncConnect(aeron);
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
                    session.reject(EventCode.ERROR, detail, ctx.errorLog(), clusterMemberId());
                    rejectedBackupSessions.add(session);
                }
            }
        }
    }

    void state(final ConsensusModule.State newState)
    {
        if (newState != state)
        {
            logStateChange(memberId, state, newState);
            state = newState;
            if (!moduleState.isClosed())
            {
                moduleState.set(newState.code());
            }
        }
    }

    ConsensusModule.State state()
    {
        return state;
    }

    private void logStateChange(
        final int memberId, final ConsensusModule.State oldState, final ConsensusModule.State newState)
    {
        //System.out.println("CM State memberId=" + memberId + " " + oldState + " -> " + newState);
    }

    void role(final Cluster.Role newRole)
    {
        if (newRole != role)
        {
            logRoleChange(memberId, role, newRole);
            role = newRole;
            if (!clusterRoleCounter.isClosed())
            {
                clusterRoleCounter.set(newRole.code());
            }
        }
    }

    private void logRoleChange(final int memberId, final Cluster.Role oldRole, final Cluster.Role newRole)
    {
        //System.out.println("CM Role memberId=" + memberId + " " + oldRole + " -> " + newRole);
    }

    Cluster.Role role()
    {
        return role;
    }

    long prepareForNewLeadership(final long logPosition, final long nowNs)
    {
        role(Cluster.Role.FOLLOWER);

        CloseHelper.close(ctx.countedErrorHandler(), ingressAdapter);

        if (null != catchupLogDestination)
        {
            logAdapter.asyncRemoveDestination(catchupLogDestination);
            catchupLogDestination = null;
        }

        if (null != liveLogDestination)
        {
            logAdapter.asyncRemoveDestination(liveLogDestination);
            liveLogDestination = null;
        }

        final long logSubscriptionRegistrationId = logAdapter.disconnect(ctx.countedErrorHandler());
        logPublisher.disconnect(ctx.countedErrorHandler());
        ClusterControl.ToggleState.deactivate(controlToggle);
        tryStopLogRecording();

        if (RecordingPos.NULL_RECORDING_ID != logRecordingId)
        {
            lastAppendPosition = getLastAppendedPosition();
            timeOfLastAppendPositionUpdateNs = nowNs;
            recoveryPlan = recordingLog.createRecoveryPlan(archive, serviceCount, logRecordingId);

            clearSessionsAfter(logPosition);
            for (int i = 0, size = sessions.size(); i < size; i++)
            {
                sessions.get(i).disconnect(aeron, ctx.countedErrorHandler());
            }

            commitPosition.setRelease(logPosition);
            restoreUncommittedEntries(logPosition);

            final CountersReader counters = ctx.aeron().countersReader();
            final long archiveId = archive.archiveId();
            while (CountersReader.NULL_COUNTER_ID !=
                RecordingPos.findCounterIdByRecording(counters, logRecordingId, archiveId))
            {
                idle();
            }
        }

        if (NULL_VALUE != logSubscriptionRegistrationId)
        {
            awaitLocalSocketsClosed(logSubscriptionRegistrationId);
        }
        if (null != consensusModuleExtension)
        {
            consensusModuleExtension.onPrepareForNewLeadership();
        }

        return lastAppendPosition;
    }

    void onServiceCloseSession(final long clusterSessionId)
    {
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null != session)
        {
            session.closing(CloseReason.SERVICE_ACTION);

            if (Cluster.Role.LEADER == role && ConsensusModule.State.ACTIVE == state)
            {
                if (logPublisher.appendSessionClose(
                    memberId, session, leadershipTermId, clusterClock.time(), clusterTimeUnit))
                {
                    final String msg = CloseReason.SERVICE_ACTION.name();
                    egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.CLOSED, msg);
                    session.closedLogPosition(logPublisher.position());
                    uncommittedClosedSessions.addLast(session);
                    closeSession(session);
                }
            }
        }
    }

    void onServiceMessage(final long clusterSessionId, final DirectBuffer buffer, final int offset, final int length)
    {
        final int serviceId = PendingServiceMessageTracker.serviceIdFromServiceMessage(clusterSessionId);
        pendingServiceMessageTrackers[serviceId].enqueueMessage((MutableDirectBuffer)buffer, offset, length);
    }

    void onScheduleTimer(final long correlationId, final long deadline)
    {
        if (expiredTimerCountByCorrelationIdMap.get(correlationId) == 0)
        {
            timerService.scheduleTimerForCorrelationId(correlationId, deadline);
        }
        else
        {
            expiredTimerCountByCorrelationIdMap.decrementAndGet(correlationId);
        }
    }

    void onCancelTimer(final long correlationId)
    {
        timerService.cancelTimerByCorrelationId(correlationId);
    }

    void onServiceAck(
        final long logPosition, final long timestamp, final long ackId, final long relevantId, final int serviceId)
    {
        logOnServiceAck(memberId, logPosition, timestamp, clusterTimeUnit, ackId, relevantId, serviceId);
        captureServiceAck(logPosition, ackId, relevantId, serviceId);

        if (ServiceAck.hasReached(logPosition, serviceAckId, serviceAckQueues))
        {
            switch (state)
            {
                case SNAPSHOT:
                    ++serviceAckId;
                    snapshotOnServiceAck(logPosition, timestamp, pollServiceAcks(logPosition, serviceId));
                    break;

                case QUITTING:
                    closeAndTerminate();
                    break;

                case TERMINATING:
                    terminateOnServiceAck(logPosition);
                    break;

                default:
                    break;
            }
        }
    }

    void onReplaySessionMessage(final long clusterSessionId, final long timestamp)
    {
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null != session)
        {
            session.timeOfLastActivityNs(clusterTimeUnit.toNanos(timestamp));
        }
        else if (clusterSessionId < 0)
        {
            final int serviceId = PendingServiceMessageTracker.serviceIdFromLogMessage(clusterSessionId);
            pendingServiceMessageTrackers[serviceId].sweepFollowerMessages(clusterSessionId);
        }
    }

    public ControlledFragmentHandler.Action onReplayExtensionMessage(
        final int actingBlockLength,
        final int templateId,
        final int schemaId,
        final int actingVersion,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        if (null != consensusModuleExtension)
        {
            final int remainingMessageOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
            final int remainingMessageLength = length - MessageHeaderDecoder.ENCODED_LENGTH;

            return consensusModuleExtension.onLogExtensionMessage(
                actingBlockLength,
                templateId,
                schemaId,
                actingVersion,
                buffer,
                remainingMessageOffset,
                remainingMessageLength,
                header);
        }

        throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
    }

    void onReplayTimerEvent(final long correlationId)
    {
        if (!timerService.cancelTimerByCorrelationId(correlationId))
        {
            expiredTimerCountByCorrelationIdMap.getAndIncrement(correlationId);
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
        final ClusterSession session = new ClusterSession(clusterSessionId, responseStreamId, responseChannel);
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

    void onReplayClusterAction(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final ClusterAction action,
        final int flags)
    {
        if (leadershipTermId == this.leadershipTermId)
        {
            if (ClusterAction.SUSPEND == action)
            {
                state(ConsensusModule.State.SUSPENDED);
            }
            else if (ClusterAction.RESUME == action)
            {
                state(ConsensusModule.State.ACTIVE);
            }
            else if (ClusterAction.SNAPSHOT == action && CLUSTER_ACTION_FLAGS_DEFAULT == flags)
            {
                state(ConsensusModule.State.SNAPSHOT);
                totalSnapshotDurationTracker.onSnapshotBegin(clusterClock.timeNanos());
                if (0 == serviceCount)
                {
                    snapshotOnServiceAck(logPosition, timestamp, ServiceAck.EMPTY_SERVICE_ACKS);
                }
            }
        }
    }

    void onReplayNewLeadershipTermEvent(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        logOnReplayNewLeadershipTermEvent(
            memberId,
            null != election,
            leadershipTermId,
            logPosition,
            timestamp,
            termBaseLogPosition,
            timeUnit,
            appVersion);

        if (timeUnit != clusterTimeUnit)
        {
            ctx.countedErrorHandler().onError(new ClusterException(
                "incompatible timestamp units: " + clusterTimeUnit + " log=" + timeUnit,
                AeronException.Category.FATAL));
            unexpectedTermination();
        }

        if (!ctx.appVersionValidator().isVersionCompatible(ctx.appVersion(), appVersion))
        {
            ctx.countedErrorHandler().onError(new ClusterException(
                "incompatible version: " + SemanticVersion.toString(ctx.appVersion()) +
                " log=" + SemanticVersion.toString(appVersion),
                AeronException.Category.FATAL));
            unexpectedTermination();
        }

        leadershipTermId(leadershipTermId);

        if (null != election)
        {
            election.onReplayNewLeadershipTermEvent(leadershipTermId, logPosition, timestamp, termBaseLogPosition);
        }
        if (null != consensusModuleExtension)
        {
            consensusModuleExtension.onNewLeadershipTerm(
                new ConsensusControlState(null, logRecordingId, leadershipTermId, null));
        }
    }

    int addLogPublication(final long appendPosition)
    {
        final long logPublicationTag = aeron.nextCorrelationId();
        logPublicationChannelTag = aeron.nextCorrelationId();
        final ChannelUri channelUri = ChannelUri.parse(ctx.logChannel());

        channelUri.put(ALIAS_PARAM_NAME, "log");
        channelUri.put(TAGS_PARAM_NAME, logPublicationChannelTag + "," + logPublicationTag);

        if (channelUri.isUdp())
        {
            if (ctx.isLogMdc())
            {
                channelUri.put(MDC_CONTROL_MODE_PARAM_NAME, MDC_CONTROL_MODE_MANUAL);
            }

            channelUri.put(SPIES_SIMULATE_CONNECTION_PARAM_NAME, Boolean.toString(activeMembers.length == 1));
        }

        final RecordingLog.Log clusterLog = recoveryPlan.log;
        if (null != clusterLog)
        {
            channelUri.initialPosition(appendPosition, clusterLog.initialTermId, clusterLog.termBufferLength);
            channelUri.put(MTU_LENGTH_PARAM_NAME, Integer.toString(clusterLog.mtuLength));
        }
        else
        {
            ensureConsistentInitialTermId(channelUri);
        }

        final String channel = channelUri.toString();
        final ExclusivePublication publication = aeron.addExclusivePublication(channel, ctx.logStreamId());
        logPublisher.publication(publication);

        if (ctx.isLogMdc())
        {
            for (final ClusterMember member : activeMembers)
            {
                if (member.id() != memberId)
                {
                    logPublisher.addDestination(member.logEndpoint());
                }
            }
        }

        return publication.sessionId();
    }

    void joinLogAsLeader(
        final long leadershipTermId, final long logPosition, final int logSessionId, final boolean isStartup)
    {
        final boolean isIpc = ctx.logChannel().startsWith(IPC_CHANNEL);
        final String channel = (isIpc ? IPC_CHANNEL : UDP_CHANNEL) +
            "?tags=" + logPublicationChannelTag + "|session-id=" + logSessionId + "|alias=log";

        leadershipTermId(leadershipTermId);
        startLogRecording(channel, ctx.logStreamId(), SourceLocation.LOCAL);
        while (!tryCreateAppendPosition(logSessionId))
        {
            idle();
        }

        localLogChannel = isIpc ? channel : SPY_PREFIX + channel;
        awaitServicesReady(
            localLogChannel,
            ctx.logStreamId(),
            logSessionId,
            logPosition,
            Long.MAX_VALUE,
            isStartup,
            Cluster.Role.LEADER);
    }

    void liveLogDestination(final String liveLogDestination)
    {
        this.liveLogDestination = liveLogDestination;
    }

    String liveLogDestination()
    {
        return liveLogDestination;
    }

    void catchupLogDestination(final String catchupLogDestination)
    {
        this.catchupLogDestination = catchupLogDestination;
    }

    String catchupLogDestination()
    {
        return catchupLogDestination;
    }

    boolean tryJoinLogAsFollower(final Image image, final boolean isLeaderStartup, final long nowNs)
    {
        final Subscription logSubscription = image.subscription();

        if (NULL_VALUE == logSubscriptionId)
        {
            startLogRecording(logSubscription.channel(), logSubscription.streamId(), SourceLocation.REMOTE);
        }

        if (tryCreateAppendPosition(image.sessionId()))
        {
            logAdapter.image(image);
            lastAppendPosition = image.joinPosition();
            timeOfLastAppendPositionUpdateNs = nowNs;

            awaitServicesReady(
                logSubscription.channel(),
                logSubscription.streamId(),
                image.sessionId(),
                image.joinPosition(),
                Long.MAX_VALUE,
                isLeaderStartup,
                Cluster.Role.FOLLOWER);

            return true;
        }

        return false;
    }

    void awaitServicesReady(
        final String logChannel,
        final int streamId,
        final int logSessionId,
        final long logPosition,
        final long maxLogPosition,
        final boolean isStartup,
        final Cluster.Role role)
    {
        if (serviceCount > 0)
        {
            serviceProxy.joinLog(
                logPosition,
                maxLogPosition,
                memberId,
                logSessionId,
                streamId,
                isStartup,
                role,
                logChannel);

            expectedAckPosition = logPosition;

            while (!ServiceAck.hasReached(logPosition, serviceAckId, serviceAckQueues))
            {
                idle(consensusModuleAdapter.poll());
            }

            ServiceAck.removeHead(serviceAckQueues);
            ++serviceAckId;
        }
    }

    LogReplay newLogReplay(final long logPosition, final long appendPosition)
    {
        return new LogReplay(archive, logRecordingId, logPosition, appendPosition, logAdapter, ctx);
    }

    int replayLogPoll(final LogAdapter logAdapter, final long stopPosition)
    {
        int workCount = 0;

        if (ConsensusModule.State.ACTIVE == state || ConsensusModule.State.SUSPENDED == state)
        {
            logAdapter.poll(stopPosition);
            final long position = logAdapter.position();

            if (commitPosition.getWeak() < position)
            {
                commitPosition.setRelease(position);
                workCount++;
            }
            else if (logAdapter.isImageClosed() && position < stopPosition)
            {
                throw new ClusterEvent("unexpected image close when replaying log: position=" + position);
            }
        }

        workCount += consensusModuleAdapter.poll();

        return workCount;
    }

    long logRecordingId()
    {
        return logRecordingId;
    }

    void logRecordingId(final long recordingId)
    {
        if (NULL_VALUE != recordingId)
        {
            logRecordingId = recordingId;
        }
    }

    void truncateLogEntry(final long leadershipTermId, final long logPosition)
    {
        archive.stopAllReplays(logRecordingId);
        archive.truncateRecording(logRecordingId, logPosition);
        if (NULL_VALUE != leadershipTermId)
        {
            recordingLog.commitLogPosition(leadershipTermId, logPosition);
        }
        logAdapter.disconnect(ctx.countedErrorHandler(), logPosition);
        logRecordingStopPosition = logPosition;
    }

    boolean appendNewLeadershipTermEvent(final long nowNs)
    {
        return logPublisher.appendNewLeadershipTermEvent(
            leadershipTermId,
            clusterTimeUnit.convert(nowNs, NANOSECONDS),
            election.logPosition(),
            memberId,
            logPublisher.sessionId(),
            clusterTimeUnit,
            ctx.appVersion());
    }

    void electionComplete(final long nowNs)
    {
        leadershipTermId(election.leadershipTermId());

        if (Cluster.Role.LEADER == role)
        {
            timeOfLastLogUpdateNs = nowNs - leaderHeartbeatIntervalNs;
            timerService.currentTime(clusterTimeUnit.convert(nowNs, NANOSECONDS));
            ClusterControl.ToggleState.activate(controlToggle);
            prepareSessionsForNewTerm(election.isLeaderStartup());
        }
        else
        {
            timeOfLastLogUpdateNs = nowNs;
            timeOfLastAppendPositionUpdateNs = nowNs;
            timeOfLastAppendPositionSendNs = nowNs;
            localLogChannel = null;
        }
        NodeControl.ToggleState.activate(nodeControlToggle);

        recoveryPlan = recordingLog.createRecoveryPlan(archive, serviceCount, logRecordingId);

        final long logPosition = election.logPosition();
        notifiedCommitPosition = logPosition;
        commitPosition.setRelease(logPosition);
        updateMemberDetails(election.leader());

        connectIngress();
        if (null != consensusModuleExtension)
        {
            consensusModuleExtension.onElectionComplete(new ConsensusControlState(
                logPublisher.publication(), logRecordingId, leadershipTermId, localLogChannel));
        }

        election = null;
    }

    void trackCatchupCompletion(
        final ClusterMember follower, final long leadershipTermId, final short appendPositionFlags)
    {
        if (NULL_VALUE != follower.catchupReplaySessionId() || isCatchupAppendPosition(appendPositionFlags))
        {
            if (follower.logPosition() >= logPublisher.position())
            {
                if (NULL_VALUE != follower.catchupReplayCorrelationId())
                {
                    if (archive.archiveProxy().stopReplay(
                        follower.catchupReplaySessionId(), aeron.nextCorrelationId(), archive.controlSessionId()))
                    {
                        follower.catchupReplayCorrelationId(NULL_VALUE);
                    }
                }

                if (consensusPublisher.stopCatchup(follower.publication(), leadershipTermId, follower.id()))
                {
                    follower.catchupReplaySessionId(NULL_VALUE);
                }
            }
        }
    }

    void catchupInitiated(final long nowNs)
    {
        timeOfLastAppendPositionUpdateNs = nowNs;
        timeOfLastAppendPositionSendNs = nowNs;
    }

    int catchupPoll(final long limitPosition, final long nowNs)
    {
        int workCount = 0;

        if (ConsensusModule.State.ACTIVE == state || ConsensusModule.State.SUSPENDED == state)
        {
            final int fragments = logAdapter.poll(Math.min(appendPosition.get(), limitPosition));
            workCount += fragments;
            if (fragments == 0 && logAdapter.image().isClosed())
            {
                throw new ClusterEvent(
                    "unexpected image close during catchup: position=" + logAdapter.image().position());
            }

            final ExclusivePublication publication = election.leader().publication();
            workCount += updateFollowerPosition(
                publication, nowNs, leadershipTermId, appendPosition.get(), APPEND_POSITION_FLAG_CATCHUP);
            commitPosition.proposeMaxRelease(logAdapter.position());
        }

        if (nowNs > (timeOfLastAppendPositionUpdateNs + leaderHeartbeatTimeoutNs) &&
            ConsensusModule.State.ACTIVE == state)
        {
            throw new ClusterEvent(
                "no catchup progress commitPosition=" + commitPosition.getWeak() + " limitPosition=" + limitPosition +
                " lastAppendPosition=" + lastAppendPosition +
                " appendPosition=" + (null != appendPosition ? appendPosition.get() : -1) +
                " logPosition=" + election.logPosition());
        }

        workCount += consensusModuleAdapter.poll();

        return workCount;
    }

    boolean isCatchupNearLive(final long position)
    {
        final Image image = logAdapter.image();
        if (null != image)
        {
            final long localPosition = image.position();
            final long window = Math.min(image.termBufferLength() >> 2, LIVE_ADD_MAX_WINDOW);

            return localPosition >= (position - window);
        }

        return false;
    }

    void stopAllCatchups()
    {
        for (final ClusterMember member : activeMembers)
        {
            if (member.catchupReplaySessionId() != NULL_VALUE)
            {
                if (member.catchupReplayCorrelationId() != NULL_VALUE)
                {
                    try
                    {
                        archive.stopReplay(member.catchupReplaySessionId());
                    }
                    catch (final Exception ex)
                    {
                        ctx.countedErrorHandler().onError(new ClusterEvent("replay already stopped for catchup"));
                    }
                }

                member.catchupReplaySessionId(NULL_VALUE);
                member.catchupReplayCorrelationId(NULL_VALUE);
            }
        }
    }

    int pollArchiveEvents()
    {
        int workCount = 0;

        if (null != archive)
        {
            final RecordingSignalPoller poller = this.recordingSignalPoller;
            workCount += poller.poll();

            if (poller.isPollComplete())
            {
                final int templateId = poller.templateId();

                if (ControlResponseDecoder.TEMPLATE_ID == templateId && poller.code() == ControlResponseCode.ERROR)
                {
                    for (final ClusterMember member : activeMembers)
                    {
                        if (member.catchupReplayCorrelationId() == poller.correlationId())
                        {
                            member.catchupReplaySessionId(NULL_VALUE);
                            member.catchupReplayCorrelationId(NULL_VALUE);

                            final String message = "catchup replay failed - " + poller.errorMessage();
                            ctx.countedErrorHandler().onError(new ClusterEvent(message));
                            return workCount;
                        }
                    }

                    if (UNKNOWN_REPLAY == poller.relevantId())
                    {
                        final String message = "replay no longer relevant - " + poller.errorMessage();
                        ctx.countedErrorHandler().onError(new ClusterEvent(message));
                        return workCount;
                    }

                    final ArchiveException ex = new ArchiveException(
                        poller.errorMessage(), (int)poller.relevantId(), poller.correlationId());

                    if (ex.errorCode() == ArchiveException.STORAGE_SPACE)
                    {
                        ctx.countedErrorHandler().onError(ex);
                        unexpectedTermination();
                    }

                    if (null != election)
                    {
                        election.handleError(clusterClock.timeNanos(), ex);
                    }
                }
                else if (RecordingSignalEventDecoder.TEMPLATE_ID == templateId)
                {
                    final long recordingId = poller.recordingId();
                    final long position = poller.recordingPosition();
                    final RecordingSignal signal = poller.recordingSignal();

                    if (RecordingSignal.STOP == signal && recordingId == logRecordingId)
                    {
                        logRecordingStopPosition = position;

                        if (null == election && ConsensusModule.State.ACTIVE == state)
                        {
                            final boolean isEos = logAdapter.isLogEndOfStreamAt(position);
                            enterElection(isEos, "log recording stopped: eos=" + isEos);
                            return workCount;
                        }
                    }

                    if (null != election)
                    {
                        election.onRecordingSignal(poller.correlationId(), recordingId, position, signal);
                    }
                }
            }
            else if (0 == workCount && !poller.subscription().isConnected())
            {
                ctx.countedErrorHandler().onError(new ClusterEvent("local archive is not connected"));
                unexpectedTermination();
            }
        }

        return workCount;
    }

    void leadershipTermId(final long leadershipTermId)
    {
        this.leadershipTermId = leadershipTermId;
        ctx.leadershipTermIdCounter().setRelease(leadershipTermId);
        for (final PendingServiceMessageTracker tracker : pendingServiceMessageTrackers)
        {
            tracker.leadershipTermId(leadershipTermId);
        }
    }

    private static void logOnNewLeadershipTerm(
        final int memberId,
        final long logLeadershipTermId,
        final long nextLeadershipTermId,
        final long nextTermBaseLogPosition,
        final long nextLogPosition,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long leaderRecordingId,
        final long timestamp,
        final int leaderId,
        final int logSessionId,
        final int appVersion,
        final boolean isStartup)
    {
    }

    private static void logOnCommitPosition(
        final int memberId,
        final long leadershipTermId,
        final long logPosition,
        final int leaderMemberId)
    {
    }

    private static void logOnAddPassiveMember(
        final int memberId,
        final long correlationId,
        final String memberEndpoints)
    {
    }

    private static void logOnReplayNewLeadershipTermEvent(
        final int memberId,
        final boolean isInElection,
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final TimeUnit timeUnit,
        final int appVersion)
    {
    }

    private static void logOnRequestVote(
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition,
        final long candidateTermId,
        final int candidateId,
        final int protocolVersion)
    {
    }

    private static void logOnAppendPosition(
        final int memberId,
        final long leadershipTermId,
        final long logPosition,
        final int followerMemberId,
        final short flags)
    {
    }

    private static void logOnCanvassPosition(
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition,
        final long leadershipTermId,
        final int followerMemberId,
        final int protocolVersion)
    {
    }

    private void logStandbySnapshotNotification(
        final int memberId,
        final long recordingId,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long timestamp,
        final TimeUnit timeUnit,
        final int serviceId,
        final String archiveEndpoint)
    {
    }

    private static void logOnStopCatchup(final int memberId, final long leadershipTermId, final int followerMemberId)
    {
    }

    private static void logOnCatchupPosition(
        final int memberId,
        final long leadershipTermId,
        final long logPosition,
        final int followerMemberId,
        final String catchupEndpoint)
    {
    }

    private static void logOnTerminationPosition(
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition)
    {
    }

    private static void logOnTerminationAck(
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition,
        final int senderMemberId)
    {
    }

    private static void logOnServiceAck(
        final int memberId,
        final long logPosition,
        final long timestamp,
        final TimeUnit timeUnit,
        final long ackId,
        final long relevantId,
        final int serviceId)
    {
    }

    private static void logNewElection(
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition,
        final long appendedPosition,
        final String reason)
    {
    }

    static void logReplicationEnded(
        final int memberId,
        final String purpose,
        final String controlUri,
        final long srcRecordingId,
        final long dstRecordingId,
        final long position,
        final boolean hasSynced)
    {
    }

    private void startLogRecording(final String channel, final int streamId, final SourceLocation sourceLocation)
    {
        try
        {
            final long logRecordingId = recordingLog.findLastTermRecordingId();

            logSubscriptionId = RecordingPos.NULL_RECORDING_ID == logRecordingId ?
                archive.startRecording(channel, streamId, sourceLocation, true) :
                archive.extendRecording(logRecordingId, channel, streamId, sourceLocation, true);
        }
        catch (final ArchiveException ex)
        {
            if (ex.errorCode() == ArchiveException.STORAGE_SPACE)
            {
                ctx.countedErrorHandler().onError(ex);
                unexpectedTermination();
            }

            throw ex;
        }
    }

    private void prepareSessionsForNewTerm(final boolean isStartup)
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
                    session.connect(ctx.countedErrorHandler(), aeron);
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

    private void updateMemberDetails(final ClusterMember newLeader)
    {
        leaderMember = newLeader;

        for (final ClusterMember clusterMember : activeMembers)
        {
            clusterMember.isLeader(clusterMember.id() == leaderMember.id());
        }

        ingressEndpoints = ClusterMember.ingressEndpoints(activeMembers);
    }

    private int slowTickWork(final long nowNs)
    {
        int workCount = aeronClientInvoker.invoke();
        if (aeron.isClosed())
        {
            throw new AgentTerminationException("unexpected Aeron close");
        }
        else if (ConsensusModule.State.CLOSED == state)
        {
            unexpectedTermination();
        }

        if (nowNs >= markFileUpdateDeadlineNs)
        {
            markFileUpdateDeadlineNs = nowNs + MARK_FILE_UPDATE_INTERVAL_NS;
            markFile.updateActivityTimestamp(clusterClock.timeMillis());
        }

        workCount += pollArchiveEvents();
        workCount += sendRedirects(redirectUserSessions, nowNs);

        workCount += sendRejections(rejectedUserSessions, nowNs);
        workCount += sendRejections(rejectedBackupSessions, nowNs);

        if (null == election)
        {
            if (Cluster.Role.LEADER == role)
            {
                workCount += checkClusterControlToggle(nowNs);

                if (ConsensusModule.State.ACTIVE == state)
                {
                    workCount += processPendingSessions(pendingUserSessions, rejectedUserSessions, nowNs);
                    workCount += processPendingSessions(pendingBackupSessions, rejectedBackupSessions, nowNs);
                    workCount += checkSessions(sessions, nowNs);

                    if (!ClusterMember.hasActiveQuorum(activeMembers, nowNs, leaderHeartbeatTimeoutNs))
                    {
                        enterElection(false, "inactive follower quorum");
                        workCount += 1;
                    }
                }
                else if (ConsensusModule.State.TERMINATING == state)
                {
                    if (clusterTermination.canTerminate(activeMembers, nowNs))
                    {
                        recordingLog.commitLogPosition(leadershipTermId, terminationPosition);
                        closeAndTerminate();
                    }
                }
            }
            else
            {
                if (Cluster.Role.FOLLOWER == role && ConsensusModule.State.ACTIVE == state)
                {
                    workCount += processPendingSessions(pendingBackupSessions, rejectedBackupSessions, nowNs);
                }

                if (ConsensusModule.State.ACTIVE == state || ConsensusModule.State.SUSPENDED == state)
                {
                    if (nowNs >= (timeOfLastLogUpdateNs + leaderHeartbeatTimeoutNs) &&
                        NULL_POSITION == terminationPosition)
                    {
                        enterElection(false, "leader heartbeat timeout");
                        workCount += 1;
                    }
                }
            }

            if (ConsensusModule.State.ACTIVE == state)
            {
                workCount += checkNodeControlToggle();
            }
        }

        return workCount;
    }

    private int consensusWork(final long timestamp, final long nowNs)
    {
        int workCount = 0;

        if (Cluster.Role.LEADER == role)
        {
            if (ConsensusModule.State.ACTIVE == state)
            {
                workCount += timerService.poll(timestamp);
                for (final PendingServiceMessageTracker tracker : pendingServiceMessageTrackers)
                {
                    workCount += tracker.poll();
                }
                workCount += ingressAdapter.poll();
            }

            workCount += updateLeaderPosition(nowNs);
        }
        else
        {
            if (ConsensusModule.State.ACTIVE == state || ConsensusModule.State.SUSPENDED == state)
            {
                if (NULL_POSITION != terminationPosition && logAdapter.position() >= terminationPosition)
                {
                    state(ConsensusModule.State.TERMINATING);
                    if (serviceCount > 0)
                    {
                        serviceProxy.terminationPosition(terminationPosition, ctx.countedErrorHandler());
                    }
                    else
                    {
                        terminateOnServiceAck(logAdapter.position());
                    }
                }
                else
                {
                    final long limit = null != appendPosition ? appendPosition.get() : logRecordingStopPosition;
                    final int count = logAdapter.poll(Math.min(notifiedCommitPosition, limit));
                    if (0 == count && logAdapter.isImageClosed())
                    {
                        final boolean isEos = logAdapter.isLogEndOfStream();
                        enterElection(isEos, "log disconnected from leader: eos=" + isEos);
                        return 1;
                    }

                    commitPosition.proposeMaxRelease(logAdapter.position());
                    workCount += ingressAdapter.poll();
                    workCount += count;
                }
            }

            workCount += updateFollowerPosition(nowNs);
        }

        workCount += consensusModuleAdapter.poll();
        workCount += pollStandbySnapshotReplication(nowNs);
        if (null != consensusModuleExtension)
        {
            workCount += consensusModuleExtension.doWork(nowNs);
        }

        return workCount;
    }

    @SuppressWarnings("MethodLength")
    private int checkClusterControlToggle(final long nowNs)
    {
        if (ConsensusModule.State.ACTIVE == state)
        {
            switch (ClusterControl.ToggleState.get(controlToggle))
            {
                case SUSPEND:
                {
                    final long timestamp = clusterClock.time();
                    if (appendAction(ClusterAction.SUSPEND, timestamp, CLUSTER_ACTION_FLAGS_DEFAULT))
                    {
                        state(ConsensusModule.State.SUSPENDED);
                    }
                    break;
                }

                case SNAPSHOT:
                {
                    final long timestamp = clusterClock.time();
                    if (appendAction(ClusterAction.SNAPSHOT, timestamp, CLUSTER_ACTION_FLAGS_DEFAULT))
                    {
                        state(ConsensusModule.State.SNAPSHOT);
                        totalSnapshotDurationTracker.onSnapshotBegin(nowNs);
                        if (0 == serviceCount)
                        {
                            snapshotOnServiceAck(logPublisher.position(), timestamp, ServiceAck.EMPTY_SERVICE_ACKS);
                        }
                    }
                    break;
                }

                case STANDBY_SNAPSHOT:
                {
                    final long timestamp = clusterClock.time();
                    if (appendAction(ClusterAction.SNAPSHOT, timestamp, CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT))
                    {
                        ClusterControl.ToggleState.reset(controlToggle);
                    }
                    break;
                }

                case SHUTDOWN:
                {
                    final long timestamp = clusterClock.time();
                    if (appendAction(ClusterAction.SNAPSHOT, timestamp, CLUSTER_ACTION_FLAGS_DEFAULT))
                    {
                        final long position = logPublisher.position();

                        clusterTermination = new ClusterTermination(nowNs + ctx.terminationTimeoutNs(), serviceCount);
                        clusterTermination.terminationPosition(
                            ctx.countedErrorHandler(),
                            consensusPublisher,
                            activeMembers,
                            thisMember,
                            leadershipTermId,
                            position);
                        terminationPosition = position;

                        state(ConsensusModule.State.SNAPSHOT);
                        totalSnapshotDurationTracker.onSnapshotBegin(nowNs);
                        if (0 == serviceCount)
                        {
                            snapshotOnServiceAck(position, timestamp, ServiceAck.EMPTY_SERVICE_ACKS);
                        }
                    }
                    break;
                }

                case ABORT:
                {
                    final CountedErrorHandler errorHandler = ctx.countedErrorHandler();
                    final long position = logPublisher.position();
                    clusterTermination = new ClusterTermination(nowNs + ctx.terminationTimeoutNs(), serviceCount);
                    clusterTermination.terminationPosition(
                        errorHandler, consensusPublisher, activeMembers, thisMember, leadershipTermId, position);
                    terminationPosition = position;
                    if (serviceCount > 0)
                    {
                        serviceProxy.terminationPosition(terminationPosition, errorHandler);
                    }
                    state(ConsensusModule.State.TERMINATING);
                    break;
                }

                default:
                    return 0;
            }

            return 1;
        }
        else if (ConsensusModule.State.SUSPENDED == state)
        {
            if (ClusterControl.ToggleState.RESUME == ClusterControl.ToggleState.get(controlToggle))
            {
                final long timestamp = clusterClock.time();
                if (appendAction(ClusterAction.RESUME, timestamp, CLUSTER_ACTION_FLAGS_DEFAULT))
                {
                    state(ConsensusModule.State.ACTIVE);
                    ClusterControl.ToggleState.reset(controlToggle);
                }

                return 1;
            }
        }

        return 0;
    }

    private int checkNodeControlToggle()
    {
        if (NodeControl.ToggleState.REPLICATE_STANDBY_SNAPSHOT == NodeControl.ToggleState.get(nodeControlToggle))
        {
            if (null == standbySnapshotReplicator)
            {
                standbySnapshotReplicator = StandbySnapshotReplicator.newInstance(
                    ctx.clusterMemberId(),
                    ctx.archiveContext(),
                    recordingLog,
                    serviceCount,
                    ctx.leaderArchiveControlChannel(),
                    ctx.archiveContext().controlRequestStreamId(),
                    ctx.replicationChannel(),
                    ctx.fileSyncLevel());
            }

            NodeControl.ToggleState.reset(nodeControlToggle);

            return 1;
        }

        return 0;
    }

    private boolean appendAction(final ClusterAction action, final long timestamp, final int flags)
    {
        return logPublisher.appendClusterAction(leadershipTermId, timestamp, action, flags);
    }

    @SuppressWarnings("checkstyle:methodlength")
    private int processPendingSessions(
        final ArrayList<ClusterSession> pendingSessions,
        final ArrayList<ClusterSession> rejectedSessions,
        final long nowNs)
    {
        int workCount = 0;

        for (int lastIndex = pendingSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = pendingSessions.get(i);

            if (session.state() == INVALID)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                session.close(aeron, ctx.countedErrorHandler());
                continue;
            }

            if (nowNs > (session.timeOfLastActivityNs() + sessionTimeoutNs) && session.state() != INIT)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                session.close(aeron, ctx.countedErrorHandler());
                ctx.timedOutClientCounter().incrementRelease();
                continue;
            }

            if (session.state() == INIT || session.state() == CONNECTING || session.state() == CONNECTED)
            {
                if (session.isResponsePublicationConnected(aeron, nowNs))
                {
                    session.state(CONNECTED);
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
                        if (Cluster.Role.LEADER == role)
                        {
                            nextCommittedSessionId = Math.max(session.id() + 1, nextCommittedSessionId);
                        }
                        if (session.appendSessionToLogAndSendOpen(
                            logPublisher, egressPublisher, leadershipTermId,
                            memberId, nowNs, clusterClock.time()))
                        {
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
                                ctx.errorLog(),
                                clusterMemberId());
                            break;
                        }

                        final RecordingLog.Entry entry = recordingLog.findLastTerm();
                        if (null != entry && consensusPublisher.backupResponse(
                            session,
                            commitPosition.id(),
                            leaderMember.id(),
                            thisMember.id(),
                            entry,
                            recoveryPlan,
                            ClusterMember.encodeAsString(activeMembers)))
                        {
                            ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                            session.close(aeron, ctx.countedErrorHandler());
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
                                ctx.errorLog(),
                                clusterMemberId());
                            break;
                        }

                        if (consensusPublisher.heartbeatResponse(session))
                        {
                            ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                            session.close(aeron, ctx.countedErrorHandler());
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
                                ctx.errorLog(),
                                clusterMemberId());
                            break;
                        }

                        @SuppressWarnings("unchecked")
                        final List<StandbySnapshotEntry> standbySnapshotEntries =
                            (List<StandbySnapshotEntry>)session.requestInput();

                        for (final StandbySnapshotEntry standbySnapshotEntry : standbySnapshotEntries)
                        {
                            logStandbySnapshotNotification(
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
                        session.close(aeron, ctx.countedErrorHandler());
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

    private int sendRejections(final ArrayList<ClusterSession> rejectedSessions, final long nowNs)
    {
        int workCount = 0;

        for (int lastIndex = rejectedSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = rejectedSessions.get(i);
            final String detail = session.responseDetail();
            final EventCode eventCode = session.eventCode();

            if ((session.isResponsePublicationConnected(aeron, nowNs) &&
                egressPublisher.sendEvent(session, leadershipTermId, leaderMember.id(), eventCode, detail)) ||
                (session.state() != INIT && nowNs > (session.timeOfLastActivityNs() + sessionTimeoutNs)) ||
                session.state() == INVALID)
            {
                ArrayListUtil.fastUnorderedRemove(rejectedSessions, i, lastIndex--);
                session.close(aeron, ctx.countedErrorHandler());
                workCount++;
            }
        }

        return workCount;
    }

    private int sendRedirects(final ArrayList<ClusterSession> redirectSessions, final long nowNs)
    {
        int workCount = 0;

        for (int lastIndex = redirectSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = redirectSessions.get(i);
            final EventCode eventCode = EventCode.REDIRECT;
            final int leaderId = leaderMember.id();

            if ((session.isResponsePublicationConnected(aeron, nowNs) &&
                egressPublisher.sendEvent(session, leadershipTermId, leaderId, eventCode, ingressEndpoints)) ||
                (session.state() != INIT && nowNs > (session.timeOfLastActivityNs() + sessionTimeoutNs)) ||
                session.state() == INVALID)
            {
                ArrayListUtil.fastUnorderedRemove(redirectSessions, i, lastIndex--);
                session.close(aeron, ctx.countedErrorHandler());
                workCount++;
            }
        }

        return workCount;
    }

    private int checkSessions(final ArrayList<ClusterSession> sessions, final long nowNs)
    {
        int workCount = 0;

        for (int i = sessions.size() - 1; i >= 0; i--)
        {
            final ClusterSession session = sessions.get(i);

            if (nowNs > (session.timeOfLastActivityNs() + sessionTimeoutNs))
            {
                switch (session.state())
                {
                    case OPEN:
                        session.closing(CloseReason.TIMEOUT);

                        if (logPublisher.appendSessionClose(
                            memberId, session, leadershipTermId, clusterClock.time(), clusterTimeUnit))
                        {
                            final String msg = session.closeReason().name();
                            egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.CLOSED, msg);
                            session.closedLogPosition(logPublisher.position());
                            uncommittedClosedSessions.addLast(session);
                            ctx.timedOutClientCounter().incrementRelease();
                            closeSession(session);
                        }
                        break;

                    case CLOSING:
                        if (logPublisher.appendSessionClose(
                            memberId, session, leadershipTermId, clusterClock.time(), clusterTimeUnit))
                        {
                            final String msg = session.closeReason().name();
                            egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.CLOSED, msg);
                            session.closedLogPosition(logPublisher.position());
                            uncommittedClosedSessions.addLast(session);
                            if (session.closeReason() == CloseReason.TIMEOUT)
                            {
                                ctx.timedOutClientCounter().incrementRelease();
                            }
                            closeSession(session);
                        }
                        break;

                    default:
                        closeSession(session);
                        break;
                }

                workCount++;
            }
            else if (session.hasOpenEventPending())
            {
                workCount += session.sendSessionOpenEvent(egressPublisher, leadershipTermId, memberId);
            }
            else if (session.hasNewLeaderEventPending())
            {
                workCount += sendNewLeaderEvent(session);
            }
        }

        return workCount;
    }

    private void captureServiceAck(final long logPosition, final long ackId, final long relevantId, final int serviceId)
    {
        if (0 == ackId && NULL_VALUE != serviceClientIds[serviceId])
        {
            throw new ClusterException(
                "initial ack already received from service: possible duplicate serviceId=" + serviceId);
        }

        serviceAckQueues[serviceId].offerLast(new ServiceAck(ackId, logPosition, relevantId));
    }

    private ServiceAck[] pollServiceAcks(final long logPosition, final int serviceId)
    {
        final ServiceAck[] serviceAcks = new ServiceAck[serviceAckQueues.length];
        for (int id = 0, length = serviceAckQueues.length; id < length; id++)
        {
            final ServiceAck serviceAck = serviceAckQueues[id].pollFirst();
            if (null == serviceAck || serviceAck.logPosition() != logPosition)
            {
                throw new ClusterException(
                    "invalid ack for serviceId=" + serviceId + " logPosition=" + logPosition + " " + serviceAck);
            }

            serviceAcks[id] = serviceAck;
        }

        return serviceAcks;
    }

    private int sendNewLeaderEvent(final ClusterSession session)
    {
        if (egressPublisher.newLeader(session, leadershipTermId, leaderMember.id(), ingressEndpoints))
        {
            session.hasNewLeaderEventPending(false);
            return 1;
        }

        return 0;
    }

    private boolean tryCreateAppendPosition(final int logSessionId)
    {
        final CountersReader counters = aeron.countersReader();
        final int counterId = RecordingPos.findCounterIdBySession(counters, logSessionId, archive.archiveId());
        if (CountersReader.NULL_COUNTER_ID == counterId)
        {
            return false;
        }

        final long registrationId = counters.getCounterRegistrationId(counterId);
        if (0 == registrationId)
        {
            return false;
        }

        final long recordingId = RecordingPos.getRecordingId(counters, counterId);
        if (RecordingPos.NULL_RECORDING_ID == recordingId)
        {
            return false;
        }

        logRecordingId(recordingId);
        appendPosition = new ReadableCounter(counters, registrationId, counterId);

        return true;
    }

    private int updateFollowerPosition(final long nowNs)
    {
        final long recordedPosition = null != appendPosition ? appendPosition.get() : logRecordingStopPosition;
        return updateFollowerPosition(
            leaderMember.publication(), nowNs, this.leadershipTermId, recordedPosition, APPEND_POSITION_FLAG_NONE);
    }

    private int updateFollowerPosition(
        final ExclusivePublication publication,
        final long nowNs,
        final long leadershipTermId,
        final long appendPosition,
        final short flags)
    {
        final long position = Math.max(appendPosition, lastAppendPosition);
        if ((position > lastAppendPosition ||
            nowNs >= (timeOfLastAppendPositionSendNs + leaderHeartbeatIntervalNs)))
        {
            if (consensusPublisher.appendPosition(publication, leadershipTermId, position, memberId, flags))
            {
                if (position > lastAppendPosition)
                {
                    lastAppendPosition = position;
                    timeOfLastAppendPositionUpdateNs = nowNs;
                }
                timeOfLastAppendPositionSendNs = nowNs;

                return 1;
            }
        }

        return 0;
    }

    private void loadSnapshot(final RecordingLog.Snapshot snapshot, final AeronArchive archive)
    {
        final String channel = ctx.replayChannel();
        final int streamId = ctx.replayStreamId();
        final int sessionId = (int)archive.startReplay(snapshot.recordingId, 0, NULL_LENGTH, channel, streamId);
        final String replayChannel = ChannelUri.addSessionId(channel, sessionId);

        try (Subscription subscription = aeron.addSubscription(replayChannel, streamId))
        {
            final Image image = awaitImage(sessionId, subscription);
            final ConsensusModuleSnapshotAdapter adapter = new ConsensusModuleSnapshotAdapter(image, this);

            while (true)
            {
                final int fragments = adapter.poll();
                if (adapter.isDone())
                {
                    break;
                }

                if (0 == fragments)
                {
                    pollArchiveEvents();
                    if (image.isClosed())
                    {
                        throw new ClusterException("snapshot ended unexpectedly: " + image);
                    }
                }

                idle(fragments);
            }

            for (final PendingServiceMessageTracker tracker : pendingServiceMessageTrackers)
            {
                tracker.verify();
                tracker.reset();
            }

            if (null != consensusModuleExtension)
            {
                consensusModuleExtension.onStart(this, image);
            }
        }

        timerService.currentTime(clusterClock.time());
        commitPosition.setRelease(snapshot.logPosition);
        leadershipTermId(snapshot.leadershipTermId);
        expectedAckPosition = snapshot.logPosition;
    }

    private Image awaitImage(final int sessionId, final Subscription subscription)
    {
        idleStrategy.reset();
        Image image;
        while ((image = subscription.imageBySessionId(sessionId)) == null)
        {
            idle();
        }

        return image;
    }

    private Counter addRecoveryStateCounter(final RecordingLog.RecoveryPlan plan)
    {
        final int snapshotsCount = plan.snapshots.size();

        if (snapshotsCount > 0)
        {
            final long[] serviceSnapshotRecordingIds = new long[snapshotsCount - 1];
            final RecordingLog.Snapshot snapshot = plan.snapshots.get(0);

            for (int i = 1; i < snapshotsCount; i++)
            {
                final RecordingLog.Snapshot serviceSnapshot = plan.snapshots.get(i);
                serviceSnapshotRecordingIds[serviceSnapshot.serviceId] = serviceSnapshot.recordingId;
            }

            return RecoveryState.allocate(
                aeron,
                snapshot.leadershipTermId,
                snapshot.logPosition,
                snapshot.timestamp,
                ctx.clusterId(),
                serviceSnapshotRecordingIds);
        }

        return RecoveryState.allocate(aeron, leadershipTermId, 0, 0, ctx.clusterId());
    }

    private void captureServiceClientIds()
    {
        for (int i = 0, length = serviceClientIds.length; i < length; i++)
        {
            final ServiceAck serviceAck = serviceAckQueues[i].pollFirst();
            serviceClientIds[i] = Objects.requireNonNull(serviceAck).relevantId();
        }
    }

    private int updateLeaderPosition(final long nowNs)
    {
        if (null != appendPosition)
        {
            return updateLeaderPosition(nowNs, appendPosition.get());
        }

        return 0;
    }

    long quorumPosition()
    {
        return ClusterMember.quorumPosition(activeMembers, rankedPositions);
    }

    long timeOfLastLeaderUpdateNs()
    {
        return timeOfLastLeaderUpdateNs;
    }

    int updateLeaderPosition(final long nowNs, final long position)
    {
        thisMember.logPosition(position).timeOfLastAppendPositionNs(nowNs);
        final long commitPosition = Math.min(quorumPosition(), position);

        if (commitPosition > this.commitPosition.getWeak() ||
            nowNs >= (timeOfLastLogUpdateNs + leaderHeartbeatIntervalNs))
        {
            publishCommitPosition(commitPosition);

            this.commitPosition.setRelease(commitPosition);
            timeOfLastLogUpdateNs = nowNs;

            sweepUncommittedEntriesTo(commitPosition);
            return 1;
        }

        return 0;
    }

    void publishCommitPosition(final long commitPosition)
    {
        for (final ClusterMember member : activeMembers)
        {
            if (member.id() != memberId)
            {
                consensusPublisher.commitPosition(member.publication(), leadershipTermId, commitPosition, memberId);
            }
        }
    }

    RecordingReplication newLogReplication(
        final String leaderArchiveEndpoint,
        final String responseArchiveEndpoint,
        final long leaderRecordingId,
        final long stopPosition,
        final long nowNs)
    {
        String replicationChannel = ctx.replicationChannel();
        final ReplicationParams replicationParams = new ReplicationParams()
            .dstRecordingId(logRecordingId)
            .stopPosition(stopPosition)
            .replicationSessionId((int)aeron.nextCorrelationId());

        if (null != responseArchiveEndpoint)
        {
            final ChannelUri channelUri = ChannelUri.parse(replicationChannel);
            channelUri.remove(ENDPOINT_PARAM_NAME);
            channelUri.put(MDC_CONTROL_PARAM_NAME, responseArchiveEndpoint);
            channelUri.put(MDC_CONTROL_MODE_PARAM_NAME, CONTROL_MODE_RESPONSE);
            replicationChannel = channelUri.toString();

            replicationParams.srcResponseChannel(replicationChannel);
        }

        replicationParams.replicationChannel(replicationChannel);

        return new RecordingReplication(
            archive,
            leaderRecordingId,
            ChannelUri.createDestinationUri(ctx.leaderArchiveControlChannel(), leaderArchiveEndpoint),
            archive.context().controlRequestStreamId(),
            replicationParams,
            ctx.leaderHeartbeatTimeoutNs(),
            ctx.leaderHeartbeatIntervalNs(),
            nowNs);
    }

    void awaitLocalSocketsClosed(final long registrationId)
    {
        final CountersReader countersReader = aeron.countersReader();
        while (LocalSocketAddressStatus.findNumberOfAddressesByRegistrationId(countersReader, registrationId) > 0)
        {
            idle();
        }
    }

    private void clearSessionsAfter(final long logPosition)
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
            session.close(aeron, ctx.countedErrorHandler());
        }

        pendingUserSessions.clear();
    }

    private void sweepUncommittedEntriesTo(final long commitPosition)
    {
        for (final PendingServiceMessageTracker tracker : pendingServiceMessageTrackers)
        {
            tracker.sweepLeaderMessages();
        }

        while (uncommittedTimers.peekLong() <= commitPosition)
        {
            uncommittedTimers.pollLong();
            uncommittedTimers.pollLong();
        }

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

    private void restoreUncommittedEntries(final long commitPosition)
    {
        for (final LongArrayQueue.LongIterator i = uncommittedTimers.iterator(); i.hasNext(); )
        {
            final long appendPosition = i.nextValue();
            final long correlationId = i.nextValue();

            if (appendPosition > commitPosition)
            {
                timerService.scheduleTimerForCorrelationId(correlationId, 0);
            }
        }
        uncommittedTimers.clear();

        for (final PendingServiceMessageTracker tracker : pendingServiceMessageTrackers)
        {
            tracker.restoreUncommittedMessages();
        }

        ClusterSession session;
        while (null != (session = uncommittedClosedSessions.pollFirst()))
        {
            if (session.closedLogPosition() > commitPosition)
            {
                session.closedLogPosition(NULL_POSITION);
                session.state(CLOSING);
                addSession(session);
            }
        }
    }

    private void enterElection(final boolean isLogEndOfStream, final String reason)
    {
        if (null != election)
        {
            throw new IllegalStateException("election in progress");
        }

        role(Cluster.Role.FOLLOWER);

        final long leadershipTermId = this.leadershipTermId;
        final RecordingLog.Entry termEntry = recordingLog.findTermEntry(leadershipTermId);
        final long termBaseLogPosition = null != termEntry ?
            termEntry.termBaseLogPosition : recoveryPlan.lastTermBaseLogPosition;
        final long appendedPosition = null != appendPosition ?
            appendPosition.get() : Math.max(recoveryPlan.appendedLogPosition, logRecordingStopPosition);
        final long commitPosition = this.commitPosition.getWeak();

        logNewElection(memberId, leadershipTermId, commitPosition, appendedPosition, reason);
        ctx.countedErrorHandler().onError(new ClusterEvent(reason));

        election = new Election(
            false,
            isLogEndOfStream ? leaderMember.id() : NULL_VALUE,
            leadershipTermId,
            termBaseLogPosition,
            commitPosition,
            appendedPosition,
            activeMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            this);

        election.doWork(clusterClock.timeNanos());
    }

    private static void checkInterruptStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new AgentTerminationException("interrupted");
        }
    }

    private void snapshotOnServiceAck(final long logPosition, final long timestamp, final ServiceAck[] serviceAcks)
    {
        if (isSnapshotSetComplete(serviceAcks))
        {
            takeSnapshot(timestamp, logPosition, serviceAcks);
        }

        final long nowNs = clusterClock.timeNanos();
        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            sessions.get(i).timeOfLastActivityNs(nowNs);
        }

        if (null != clusterTermination)
        {
            if (serviceCount > 0)
            {
                serviceProxy.terminationPosition(terminationPosition, ctx.countedErrorHandler());
            }
            clusterTermination.deadlineNs(clusterClock.timeNanos() + ctx.terminationTimeoutNs());
            state(ConsensusModule.State.TERMINATING);
        }
        else
        {
            state(ConsensusModule.State.ACTIVE);
            if (Cluster.Role.LEADER == role)
            {
                ClusterControl.ToggleState.reset(controlToggle);
            }
        }
    }

    private boolean isSnapshotSetComplete(final ServiceAck[] serviceAcks)
    {
        boolean isSetComplete = true;
        for (int serviceId = serviceAcks.length - 1; serviceId >= 0; serviceId--)
        {
            final long snapshotId = serviceAcks[serviceId].relevantId();
            if (NULL_VALUE == snapshotId)
            {
                ctx.errorLog().record(new ClusterEvent("service=" + serviceId + " failed to take snapshot"));
                isSetComplete = false;
            }
        }

        return isSetComplete;
    }

    private void takeSnapshot(final long timestamp, final long logPosition, final ServiceAck[] serviceAcks)
    {
        final long recordingId;
        try (ExclusivePublication publication = aeron.addExclusivePublication(
            ctx.snapshotChannel(), ctx.snapshotStreamId()))
        {
            final String channel = ChannelUri.addSessionId(ctx.snapshotChannel(), publication.sessionId());
            archive.startRecording(channel, ctx.snapshotStreamId(), LOCAL, true);
            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitRecordingCounter(counters, publication.sessionId(), archive.archiveId());
            recordingId = RecordingPos.getRecordingId(counters, counterId);

            snapshotState(publication, logPosition, leadershipTermId);

            if (null != consensusModuleExtension)
            {
                consensusModuleExtension.onTakeSnapshot(publication);
            }

            awaitRecordingComplete(recordingId, publication.position(), counters, counterId);
        }
        catch (final ArchiveException ex)
        {
            if (ex.errorCode() == ArchiveException.STORAGE_SPACE)
            {
                ctx.countedErrorHandler().onError(ex);
                unexpectedTermination();
            }

            throw ex;
        }

        final long termBaseLogPosition = recordingLog.getTermEntry(leadershipTermId).termBaseLogPosition;

        for (int serviceId = serviceAcks.length - 1; serviceId >= 0; serviceId--)
        {
            final long snapshotId = serviceAcks[serviceId].relevantId();
            recordingLog.appendSnapshot(
                snapshotId, leadershipTermId, termBaseLogPosition, logPosition, timestamp, serviceId);
        }

        recordingLog.appendSnapshot(
            recordingId, leadershipTermId, termBaseLogPosition, logPosition, timestamp, SERVICE_ID);

        recordingLog.force(ctx.fileSyncLevel());
        recoveryPlan = recordingLog.createRecoveryPlan(archive, serviceCount, Aeron.NULL_VALUE);
        ctx.snapshotCounter().incrementRelease();
        totalSnapshotDurationTracker.onSnapshotEnd(clusterClock.timeNanos());
    }

    private void awaitRecordingComplete(
        final long recordingId, final long position, final CountersReader counters, final int counterId)
    {
        idleStrategy.reset();
        while (counters.getCounterValue(counterId) < position)
        {
            idle();

            if (!RecordingPos.isActive(counters, counterId, recordingId))
            {
                throw new ClusterException("recording has stopped unexpectedly: " + recordingId);
            }
        }
    }

    private int awaitRecordingCounter(final CountersReader counters, final int sessionId, final long archiveId)
    {
        idleStrategy.reset();
        int counterId = RecordingPos.findCounterIdBySession(counters, sessionId, archiveId);
        while (CountersReader.NULL_COUNTER_ID == counterId)
        {
            idle();
            counterId = RecordingPos.findCounterIdBySession(counters, sessionId, archiveId);
        }

        return counterId;
    }

    private void snapshotState(
        final ExclusivePublication publication, final long logPosition, final long leadershipTermId)
    {
        final ConsensusModuleSnapshotTaker snapshotTaker = new ConsensusModuleSnapshotTaker(
            publication, idleStrategy, aeronClientInvoker);

        snapshotTaker.markBegin(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0, clusterTimeUnit, ctx.appVersion());

        final long actualNextSessionId = role == Cluster.Role.LEADER ? nextCommittedSessionId : nextSessionId;
        if (pendingServiceMessageTrackers.length > 0)
        {
            final PendingServiceMessageTracker trackerOne = pendingServiceMessageTrackers[0];
            snapshotTaker.snapshotConsensusModuleState(
                actualNextSessionId, trackerOne.nextServiceSessionId(),
                trackerOne.logServiceSessionId(), trackerOne.size());
        }
        else
        {
            snapshotTaker.snapshotConsensusModuleState(actualNextSessionId, 0, 0, 0);
        }

        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            final ClusterSession session = sessions.get(i);
            final ClusterSession.State sessionState = session.state();

            if (sessionState == ClusterSession.State.OPEN || sessionState == CLOSING)
            {
                snapshotTaker.snapshotSession(session);
            }
        }

        timerService.snapshot(snapshotTaker);

        for (final PendingServiceMessageTracker tracker : pendingServiceMessageTrackers)
        {
            snapshotTaker.snapshot(tracker, ctx.errorHandler());
        }

        snapshotTaker.markEnd(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0, clusterTimeUnit, ctx.appVersion());
    }

    private void onUnavailableIngressImage(final Image image)
    {
        final boolean isIpc = image.subscription().channel().startsWith(IPC_CHANNEL);
        ingressAdapter.freeSessionBuffer(image.sessionId(), isIpc);
    }

    private void onUnavailableCounter(final CountersReader counters, final long registrationId, final int counterId)
    {
        if (ConsensusModule.State.TERMINATING != state && ConsensusModule.State.QUITTING != state)
        {
            for (final long clientId : serviceClientIds)
            {
                if (registrationId == clientId)
                {
                    ctx.countedErrorHandler().onError(new ClusterEvent("Aeron client in service closed unexpectedly"));
                    state(ConsensusModule.State.CLOSED);
                    return;
                }
            }

            if (null != appendPosition && appendPosition.registrationId() == registrationId)
            {
                appendPosition.close();
                appendPosition = null;
                logSubscriptionId = NULL_VALUE;
            }
        }
    }

    private void closeAndTerminate()
    {
        tryStopLogRecording();
        state(ConsensusModule.State.CLOSED);
        throw new ClusterTerminationException(true);
    }

    private void unexpectedTermination()
    {
        aeron.removeUnavailableCounterHandler(unavailableCounterHandlerRegistrationId);
        if (serviceCount > 0)
        {
            serviceProxy.terminationPosition(0, ctx.countedErrorHandler());
        }
        tryStopLogRecording();
        state(ConsensusModule.State.CLOSED);
        throw new ClusterTerminationException(false);
    }

    private void terminateOnServiceAck(final long logPosition)
    {
        if (null == clusterTermination)
        {
            consensusPublisher.terminationAck(
                leaderMember.publication(), leadershipTermId, logPosition, memberId);
            recordingLog.commitLogPosition(leadershipTermId, logPosition);
            closeAndTerminate();
        }
        else
        {
            clusterTermination.onServicesTerminated();
            if (clusterTermination.canTerminate(activeMembers, clusterClock.timeNanos()))
            {
                recordingLog.commitLogPosition(leadershipTermId, logPosition);
                closeAndTerminate();
            }
        }
    }

    private void tryStopLogRecording()
    {
        appendPosition = null;

        if (NULL_VALUE != logSubscriptionId && archive.archiveProxy().publication().isConnected())
        {
            try
            {
                archive.tryStopRecording(logSubscriptionId);
            }
            catch (final Exception ex)
            {
                ctx.countedErrorHandler().onError(new ClusterException(ex, WARN));
            }

            logSubscriptionId = NULL_VALUE;
        }
        else if (NULL_VALUE != logRecordingId && archive.archiveProxy().publication().isConnected())
        {
            try
            {
                archive.tryStopRecordingByIdentity(logRecordingId);
            }
            catch (final Exception ex)
            {
                ctx.countedErrorHandler().onError(new ClusterException(ex, WARN));
            }
        }
    }

    private long getLastAppendedPosition()
    {
        idleStrategy.reset();
        while (true)
        {
            final long appendPosition = archive.getStopPosition(logRecordingId);
            if (NULL_POSITION != appendPosition)
            {
                return appendPosition;
            }

            idle();
        }
    }

    private void connectIngress()
    {
        final ChannelUri ingressUri = ChannelUri.parse(ctx.ingressChannel());
        if (!ingressUri.containsKey(ENDPOINT_PARAM_NAME))
        {
            ingressUri.put(ENDPOINT_PARAM_NAME, thisMember.ingressEndpoint());
        }

        if (Cluster.Role.LEADER != role && UdpChannel.isMulticastDestinationAddress(ingressUri))
        {
            return; // don't subscribe to ingress if follower and multicast ingress
        }

        final Subscription subscription = aeron.addSubscription(
            ingressUri.toString(), ctx.ingressStreamId(), null, this::onUnavailableIngressImage);

        Subscription ipcSubscription = null;
        if (Cluster.Role.LEADER == role && ctx.isIpcIngressAllowed())
        {
            ipcSubscription = aeron.addSubscription(
                IPC_CHANNEL, ctx.ingressStreamId(), null, this::onUnavailableIngressImage);
        }

        ingressAdapter.connect(subscription, ipcSubscription);
    }

    private void ensureConsistentInitialTermId(final ChannelUri channelUri)
    {
        channelUri.put(INITIAL_TERM_ID_PARAM_NAME, "0");
        channelUri.put(TERM_ID_PARAM_NAME, "0");
        channelUri.put(TERM_OFFSET_PARAM_NAME, "0");
    }

    private void checkFollowerForConsensusPublication(final int followerMemberId)
    {
        final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
        if (null != follower && null == follower.publication())
        {
            ClusterMember.addConsensusPublication(
                follower, ctx.consensusChannel(), ctx.consensusStreamId(), aeron, ctx.countedErrorHandler());
        }
    }

    private void runTerminationHook()
    {
        try
        {
            ctx.terminationHook().run();
        }
        catch (final Exception ex)
        {
            ctx.countedErrorHandler().onError(ex);
        }
    }

    private String refineResponseChannel(final String responseChannel)
    {
        if (null == responseChannelTemplate)
        {
            return responseChannel;
        }
        else if (responseChannel.startsWith(IPC_CHANNEL))
        {
            return ctx.isIpcIngressAllowed() ? responseChannel : ctx.egressChannel();
        }
        else
        {
            final ChannelUri channelUri = ChannelUri.parse(responseChannel);
            responseChannelTemplate.forEachParameter(channelUri::put);
            return channelUri.toString();
        }
    }

    private void stopExistingCatchupReplay(final ClusterMember follower)
    {
        if (NULL_VALUE != follower.catchupReplaySessionId())
        {
            if (archive.archiveProxy().stopReplay(
                follower.catchupReplaySessionId(), aeron.nextCorrelationId(), archive.controlSessionId()))
            {
                follower.catchupReplaySessionId(NULL_VALUE);
                follower.catchupReplayCorrelationId(NULL_VALUE);
            }
        }
    }

    private static boolean isCatchupAppendPosition(final short flags)
    {
        return 0 != (APPEND_POSITION_FLAG_CATCHUP & flags);
    }

    private void addSession(final ClusterSession session)
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

    private void closeSession(final ClusterSession session)
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

        session.close(aeron, ctx.countedErrorHandler());

        if (null != consensusModuleExtension && null != session.closeReason())
        {
            consensusModuleExtension.onSessionClosed(sessionId);
        }
    }

    @SuppressWarnings("try")
    private RecordingLog.RecoveryPlan recoverFromSnapshotAndLog()
    {
        final RecordingLog.RecoveryPlan recoveryPlan = recordingLog.createRecoveryPlan(
            archive, serviceCount, logRecordingId);
        if (null != recoveryPlan.log)
        {
            logRecordingId(recoveryPlan.log.recordingId);
        }

        try (Counter ignore = addRecoveryStateCounter(recoveryPlan))
        {
            if (!recoveryPlan.snapshots.isEmpty())
            {
                loadSnapshot(recoveryPlan.snapshots.get(0), archive);
            }
            else if (null != consensusModuleExtension)
            {
                consensusModuleExtension.onStart(this, null);
            }

            while (!ServiceAck.hasReached(expectedAckPosition, serviceAckId, serviceAckQueues))
            {
                idle(consensusModuleAdapter.poll());
            }

            captureServiceClientIds();
            ++serviceAckId;
        }

        return recoveryPlan;
    }

    private RecordingLog.RecoveryPlan recoverFromBootstrapState()
    {
        final ConsensusModuleStateExport boostrapState = ctx.boostrapState();

        logRecordingId(boostrapState.logRecordingId);
        final RecordingLog.RecoveryPlan recoveryPlan = recordingLog.createRecoveryPlan(
            archive, serviceCount, logRecordingId);

        expectedAckPosition = boostrapState.expectedAckPosition;
        serviceAckId = boostrapState.serviceAckId;
        leadershipTermId = boostrapState.leadershipTermId;
        nextSessionId = boostrapState.nextSessionId;

        for (final ConsensusModuleStateExport.TimerStateExport timer : boostrapState.timers)
        {
            onLoadTimer(timer.correlationId, timer.deadline, null, 0, 0);
        }

        for (final ConsensusModuleStateExport.ClusterSessionStateExport sessionExport : boostrapState.sessions)
        {
            onLoadClusterSession(
                sessionExport.id,
                sessionExport.correlationId,
                sessionExport.openedLogPosition,
                sessionExport.timeOfLastActivityNs,
                sessionExport.closeReason,
                sessionExport.responseStreamId,
                sessionExport.responseChannel,
                null,
                0,
                0);
        }

        final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        final SessionMessageHeaderDecoder sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
        final ExpandableRingBuffer.MessageConsumer consumer =
            (buffer, offset, length, headOffset) ->
            {
                sessionMessageHeaderDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());
                onLoadPendingMessage(sessionMessageHeaderDecoder.clusterSessionId(), buffer, offset, length);
                return true;
            };

        for (final ConsensusModuleStateExport.PendingServiceMessageTrackerStateExport tracker :
            boostrapState.pendingMessageTrackers)
        {
            onLoadPendingMessageTracker(
                tracker.nextServiceSessionId,
                tracker.logServiceSessionId,
                tracker.capacity,
                tracker.serviceId,
                null, 0, 0);

            tracker.pendingMessages.forEach(consumer, Integer.MAX_VALUE);
        }

        serviceProxy.requestServiceAck(expectedAckPosition);

        while (!ServiceAck.hasReached(expectedAckPosition, serviceAckId, serviceAckQueues))
        {
            idle(consensusModuleAdapter.poll());
        }

        captureServiceClientIds();
        ++serviceAckId;

        return recoveryPlan;
    }

    private void replicateStandbySnapshotsForStartup()
    {
        try (StandbySnapshotReplicator standbySnapshotReplicator = StandbySnapshotReplicator.newInstance(
            ctx.clusterMemberId(),
            ctx.archiveContext(),
            recordingLog,
            serviceCount,
            ctx.leaderArchiveControlChannel(),
            ctx.archiveContext().controlRequestStreamId(),
            ctx.replicationChannel(),
            ctx.fileSyncLevel()))
        {
            while (!standbySnapshotReplicator.isComplete())
            {
                try
                {
                    ctx.idleStrategy().idle(standbySnapshotReplicator.poll(ctx.clusterClock().timeNanos()));
                }
                catch (final ClusterException ex)
                {
                    ctx.errorHandler().onError(ex);
                    break;
                }

                checkInterruptStatus();
                aeronClientInvoker.invoke();
                if (aeron.isClosed())
                {
                    throw new AgentTerminationException("unexpected Aeron close");
                }
            }
        }
    }

    private int pollStandbySnapshotReplication(final long nowNs)
    {
        int workCount = 0;

        if (null != standbySnapshotReplicator)
        {
            workCount += standbySnapshotReplicator.poll(nowNs);

            if (standbySnapshotReplicator.isComplete())
            {
                recoveryPlan = recordingLog.createRecoveryPlan(archive, ctx.serviceCount(), Aeron.NULL_VALUE);
                ctx.snapshotCounter().incrementRelease();
                CloseHelper.quietClose(standbySnapshotReplicator);
                standbySnapshotReplicator = null;
            }
        }

        return workCount;
    }

    public String toString()
    {
        return "ConsensusModuleAgent{" +
            "memberId=" + memberId +
            ", election=" + election +
            '}';
    }
}
