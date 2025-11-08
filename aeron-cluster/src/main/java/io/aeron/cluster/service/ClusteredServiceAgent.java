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
package io.aeron.cluster.service;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.DirectBufferVector;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionMessageHeaderEncoder;
import io.aeron.driver.Configuration;
import io.aeron.driver.DutyCycleTracker;
import io.aeron.exceptions.AeronEvent;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.status.ReadableCounter;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.SemanticVersion;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.cluster.ConsensusModule.CLUSTER_ACTION_FLAGS_DEFAULT;
import static io.aeron.cluster.ConsensusModule.CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT;
import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.COMMIT_POSITION_TYPE_ID;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.MARK_FILE_UPDATE_INTERVAL_NS;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.SNAPSHOT_TYPE_ID;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

abstract class ClusteredServiceAgentLhsPadding
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
}

abstract class ClusteredServiceAgentHotFields extends ClusteredServiceAgentLhsPadding
{
    static final int LIFECYCLE_CALLBACK_NONE = 0;
    static final int LIFECYCLE_CALLBACK_ON_START = 1;
    static final int LIFECYCLE_CALLBACK_ON_TERMINATE = 2;
    static final int LIFECYCLE_CALLBACK_ON_ROLE_CHANGE = 3;
    static final int LIFECYCLE_CALLBACK_DO_BACKGROUND_WORK = 4;

    static String lifecycleName(final int activeLifecycleCallback)
    {
        switch (activeLifecycleCallback)
        {
            case LIFECYCLE_CALLBACK_NONE:
                return "none";
            case LIFECYCLE_CALLBACK_ON_START:
                return "onStart";
            case LIFECYCLE_CALLBACK_ON_TERMINATE:
                return "onTerminate";
            case LIFECYCLE_CALLBACK_ON_ROLE_CHANGE:
                return "onRoleChange";
            case LIFECYCLE_CALLBACK_DO_BACKGROUND_WORK:
                return "doBackgroundWork";
            default:
                return "unknown";
        }
    }

    int activeLifecycleCallback;
}

abstract class ClusteredServiceAgentRhsPadding extends ClusteredServiceAgentHotFields
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;
}

final class ClusteredServiceAgent extends ClusteredServiceAgentRhsPadding implements Agent, Cluster, IdleStrategy
{
    private static final long ONE_MILLISECOND_NS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long MARK_FILE_UPDATE_INTERVAL_MS =
        TimeUnit.NANOSECONDS.toMillis(MARK_FILE_UPDATE_INTERVAL_NS);

    private volatile boolean isAbort;
    private boolean isServiceActive;
    private final int serviceId;
    private int memberId = NULL_VALUE;
    private long closeHandlerRegistrationId;
    private long ackId = 0;
    private long terminationPosition = NULL_POSITION;
    private long markFileUpdateDeadlineMs;
    private long lastSlowTickNs;
    private long clusterTime;
    private long logPosition = NULL_POSITION;

    private final IdleStrategy idleStrategy;
    private final ClusterMarkFile markFile;
    private final ClusteredServiceContainer.Context ctx;
    private final Aeron aeron;
    private final AgentInvoker aeronAgentInvoker;
    private final ClusteredService service;
    private final ConsensusModuleProxy consensusModuleProxy;
    private final ServiceAdapter serviceAdapter;
    private final EpochClock epochClock;
    private final NanoClock nanoClock;
    private final UnsafeBuffer messageBuffer = new UnsafeBuffer(new byte[Configuration.MAX_UDP_PAYLOAD_LENGTH]);
    private final UnsafeBuffer headerBuffer = new UnsafeBuffer(
        messageBuffer,
        DataHeaderFlyweight.HEADER_LENGTH,
        Configuration.MAX_UDP_PAYLOAD_LENGTH - DataHeaderFlyweight.HEADER_LENGTH);
    private final DirectBufferVector headerVector = new DirectBufferVector(headerBuffer, 0, SESSION_HEADER_LENGTH);
    private final SessionMessageHeaderEncoder sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();
    private final ArrayList<ContainerClientSession> sessions = new ArrayList<>();
    private final Long2ObjectHashMap<ContainerClientSession> sessionByIdMap = new Long2ObjectHashMap<>();
    private final Collection<ClientSession> unmodifiableClientSessions = Collections.unmodifiableCollection(sessions);
    private final BoundedLogAdapter logAdapter;
    private final DutyCycleTracker dutyCycleTracker;
    private final SnapshotDurationTracker snapshotDurationTracker;
    private final String subscriptionAlias;
    private final int standbySnapshotFlags;

    private ReadableCounter commitPosition;
    private ActiveLogEvent activeLogEvent;
    private Role role = Role.FOLLOWER;
    private TimeUnit timeUnit = null;
    private long requestedAckPosition = NULL_POSITION;

    ClusteredServiceAgent(final ClusteredServiceContainer.Context ctx)
    {
        logAdapter = new BoundedLogAdapter(this, ctx.logFragmentLimit());
        this.ctx = ctx;

        markFile = ctx.clusterMarkFile();
        aeron = ctx.aeron();
        aeronAgentInvoker = ctx.aeron().conductorAgentInvoker();
        service = ctx.clusteredService();
        idleStrategy = ctx.idleStrategy();
        serviceId = ctx.serviceId();
        epochClock = ctx.epochClock();
        nanoClock = ctx.nanoClock();
        dutyCycleTracker = ctx.dutyCycleTracker();
        snapshotDurationTracker = ctx.snapshotDurationTracker();
        subscriptionAlias = "log-sc-" + ctx.serviceId();

        final String channel = ctx.controlChannel();
        consensusModuleProxy = new ConsensusModuleProxy(aeron.addPublication(channel, ctx.consensusModuleStreamId()));
        serviceAdapter = new ServiceAdapter(aeron.addSubscription(channel, ctx.serviceStreamId()), this);
        sessionMessageHeaderEncoder.wrapAndApplyHeader(headerBuffer, 0, new MessageHeaderEncoder());
        this.standbySnapshotFlags = ctx.standbySnapshotEnabled() ? CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT :
            CLUSTER_ACTION_FLAGS_DEFAULT;
    }

    public void onStart()
    {
        closeHandlerRegistrationId = aeron.addCloseHandler(this::abort);
        aeron.addUnavailableCounterHandler(this::counterUnavailable);
        final CountersReader counters = aeron.countersReader();
        commitPosition = awaitCommitPositionCounter(counters, ctx.clusterId());

        recoverState(counters);
        dutyCycleTracker.update(nanoClock.nanoTime());
        isServiceActive = true;
    }

    public void onClose()
    {
        aeron.removeCloseHandler(closeHandlerRegistrationId);

        if (isAbort)
        {
            ctx.abortLatch().countDown();
        }
        else
        {
            final CountedErrorHandler errorHandler = ctx.countedErrorHandler();
            if (isServiceActive)
            {
                isServiceActive = false;
                try
                {
                    service.onTerminate(this);
                }
                catch (final Exception ex)
                {
                    errorHandler.onError(ex);
                }
            }

            CloseHelper.close(errorHandler, logAdapter);

            if (!ctx.ownsAeronClient() && !aeron.isClosed())
            {
                CloseHelper.close(errorHandler, serviceAdapter);
                CloseHelper.close(errorHandler, consensusModuleProxy);
                disconnectEgress(errorHandler);
            }
        }

        markFile.signalTerminated();
        ctx.close();
    }

    public int doWork()
    {
        int workCount = 0;

        final long nowNs = nanoClock.nanoTime();
        dutyCycleTracker.measureAndUpdate(nowNs);

        try
        {
            if (checkForClockTick(nowNs))
            {
                workCount += pollServiceAdapter();
            }

            if (null != logAdapter.image())
            {
                final int polled = logAdapter.poll(commitPosition.get());
                workCount += polled;

                if (0 == polled && logAdapter.isDone())
                {
                    closeLog();
                }
            }

            workCount += invokeBackgroundWork(nowNs);
        }
        catch (final AgentTerminationException ex)
        {
            runTerminationHook();
            throw ex;
        }

        return workCount;
    }

    public String roleName()
    {
        return ctx.serviceName();
    }

    public Cluster.Role role()
    {
        return role;
    }

    public int memberId()
    {
        return memberId;
    }

    public Aeron aeron()
    {
        return aeron;
    }

    public ClusteredServiceContainer.Context context()
    {
        return ctx;
    }

    public ClientSession getClientSession(final long clusterSessionId)
    {
        return sessionByIdMap.get(clusterSessionId);
    }

    public Collection<ClientSession> clientSessions()
    {
        return unmodifiableClientSessions;
    }

    public void forEachClientSession(final Consumer<? super ClientSession> action)
    {
        sessions.forEach(action);
    }

    public boolean closeClientSession(final long clusterSessionId)
    {
        checkForValidInvocation();

        final ContainerClientSession clientSession = sessionByIdMap.get(clusterSessionId);
        if (clientSession == null)
        {
            throw new ClusterException("unknown clusterSessionId: " + clusterSessionId);
        }

        if (clientSession.isClosing())
        {
            return true;
        }

        int attempts = 3;
        do
        {
            if (consensusModuleProxy.closeSession(clusterSessionId))
            {
                clientSession.markClosing();
                return true;
            }
            idle();
        }
        while (--attempts > 0);

        return false;
    }

    public TimeUnit timeUnit()
    {
        return timeUnit;
    }

    public long time()
    {
        return clusterTime;
    }

    public long logPosition()
    {
        return logPosition;
    }

    public boolean scheduleTimer(final long correlationId, final long deadline)
    {
        checkForValidInvocation();

        return consensusModuleProxy.scheduleTimer(correlationId, deadline);
    }

    public boolean cancelTimer(final long correlationId)
    {
        checkForValidInvocation();

        return consensusModuleProxy.cancelTimer(correlationId);
    }

    public long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        checkForValidInvocation();
        sessionMessageHeaderEncoder.clusterSessionId(context().serviceId());

        return consensusModuleProxy.offer(headerBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length);
    }

    public long offer(final DirectBufferVector[] vectors)
    {
        checkForValidInvocation();
        sessionMessageHeaderEncoder.clusterSessionId(context().serviceId());
        vectors[0] = headerVector;

        return consensusModuleProxy.offer(vectors);
    }

    public long tryClaim(final int length, final BufferClaim bufferClaim)
    {
        checkForValidInvocation();
        sessionMessageHeaderEncoder.clusterSessionId(context().serviceId());

        return consensusModuleProxy.tryClaim(length + SESSION_HEADER_LENGTH, bufferClaim, headerBuffer);
    }

    public IdleStrategy idleStrategy()
    {
        return this;
    }

    public void reset()
    {
        idleStrategy.reset();
    }

    public void idle()
    {
        idleStrategy.idle();
        doIdleWork();
    }

    public void idle(final int workCount)
    {
        idleStrategy.idle(workCount);
        if (workCount <= 0)
        {
            doIdleWork();
        }
    }

    private void doIdleWork()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new AgentTerminationException("interrupted");
        }

        final long nowNs = nanoClock.nanoTime();

        checkForClockTick(nowNs);

        if (isServiceActive)
        {
            invokeBackgroundWork(nowNs);
        }
    }

    void onJoinLog(
        final long logPosition,
        final long maxLogPosition,
        final int memberId,
        final int logSessionId,
        final int logStreamId,
        final boolean isStartup,
        final Cluster.Role role,
        final String logChannel)
    {
        logAdapter.maxLogPosition(logPosition);
        activeLogEvent = new ActiveLogEvent(
            logPosition,
            maxLogPosition,
            memberId,
            logSessionId,
            logStreamId,
            isStartup,
            role,
            logChannel);
    }

    void onServiceTerminationPosition(final long logPosition)
    {
        terminationPosition = logPosition;
    }

    void onRequestServiceAck(final long logPosition)
    {
        requestedAckPosition = logPosition;
    }

    void onSessionMessage(
        final long logPosition,
        final long clusterSessionId,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        this.logPosition = logPosition;
        clusterTime = timestamp;
        final ClientSession clientSession = sessionByIdMap.get(clusterSessionId);

        service.onSessionMessage(clientSession, timestamp, buffer, offset, length, header);
    }

    void onTimerEvent(final long logPosition, final long correlationId, final long timestamp)
    {
        this.logPosition = logPosition;
        clusterTime = timestamp;
        service.onTimerEvent(correlationId, timestamp);
    }

    void onSessionOpen(
        final long leadershipTermId,
        final long logPosition,
        final long clusterSessionId,
        final long timestamp,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedPrincipal)
    {
        this.logPosition = logPosition;
        clusterTime = timestamp;

        if (sessionByIdMap.containsKey(clusterSessionId))
        {
            throw new ClusterException("clashing open clusterSessionId=" + clusterSessionId +
                " leadershipTermId=" + leadershipTermId + " logPosition=" + logPosition);
        }

        final ContainerClientSession session = new ContainerClientSession(
            clusterSessionId, responseStreamId, responseChannel, encodedPrincipal, this);

        if (Role.LEADER == role && ctx.isRespondingService())
        {
            session.connect(aeron);
        }

        addSession(session);
        service.onSessionOpen(session, timestamp);
    }

    void onSessionClose(
        final long leadershipTermId,
        final long logPosition,
        final long clusterSessionId,
        final long timestamp,
        final CloseReason closeReason)
    {
        this.logPosition = logPosition;
        clusterTime = timestamp;

        final ContainerClientSession session = sessionByIdMap.remove(clusterSessionId);
        if (null == session)
        {
            ctx.countedErrorHandler().onError(new ClusterEvent(
                "unknown session close: clusterSessionId=" + clusterSessionId + " closeReason=" + closeReason +
                " leadershipTermId=" + leadershipTermId + " logPosition=" + logPosition));
        }
        else
        {
            for (int i = 0, size = sessions.size(); i < size; i++)
            {
                if (sessions.get(i).id() == clusterSessionId)
                {
                    sessions.remove(i);
                    break;
                }
            }

            session.disconnect(ctx.countedErrorHandler());
            service.onSessionClose(session, timestamp, closeReason);
        }
    }

    void onServiceAction(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final ClusterAction action,
        final int flags)
    {
        this.logPosition = logPosition;
        clusterTime = timestamp;
        executeAction(action, logPosition, leadershipTermId, flags);
    }

    void onNewLeadershipTermEvent(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final int leaderMemberId,
        final int logSessionId,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        if (!ctx.appVersionValidator().isVersionCompatible(ctx.appVersion(), appVersion))
        {
            ctx.countedErrorHandler().onError(new ClusterException(
                "incompatible version: " + SemanticVersion.toString(ctx.appVersion()) +
                " log=" + SemanticVersion.toString(appVersion)));
            throw new AgentTerminationException();
        }

        sessionMessageHeaderEncoder.leadershipTermId(leadershipTermId);
        this.logPosition = logPosition;
        clusterTime = timestamp;
        this.timeUnit = timeUnit;

        service.onNewLeadershipTermEvent(
            leadershipTermId,
            logPosition,
            timestamp,
            termBaseLogPosition,
            leaderMemberId,
            logSessionId,
            timeUnit,
            appVersion);
    }

    void addSession(
        final long clusterSessionId,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedPrincipal)
    {
        final ContainerClientSession session = new ContainerClientSession(
            clusterSessionId, responseStreamId, responseChannel, encodedPrincipal, this);

        addSession(session);
    }

    private void addSession(final ContainerClientSession session)
    {
        final long clusterSessionId = session.id();
        sessionByIdMap.put(clusterSessionId, session);

        final int size = sessions.size();
        int addIndex = size;
        for (int i = size - 1; i >= 0; i--)
        {
            if (sessions.get(i).id() < clusterSessionId)
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

    void handleError(final Throwable ex)
    {
        ctx.countedErrorHandler().onError(ex);
    }

    long offer(
        final long clusterSessionId,
        final Publication publication,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        checkForValidInvocation();

        if (Cluster.Role.LEADER != role)
        {
            return ClientSession.MOCKED_OFFER;
        }

        if (null == publication)
        {
            return Publication.NOT_CONNECTED;
        }

        sessionMessageHeaderEncoder
            .clusterSessionId(clusterSessionId)
            .timestamp(clusterTime);

        return publication.offer(headerBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length, null);
    }

    long offer(final long clusterSessionId, final Publication publication, final DirectBufferVector[] vectors)
    {
        checkForValidInvocation();

        if (Cluster.Role.LEADER != role)
        {
            return ClientSession.MOCKED_OFFER;
        }

        if (null == publication)
        {
            return Publication.NOT_CONNECTED;
        }

        sessionMessageHeaderEncoder
            .clusterSessionId(clusterSessionId)
            .timestamp(clusterTime);

        vectors[0] = headerVector;

        return publication.offer(vectors, null);
    }

    long tryClaim(
        final long clusterSessionId,
        final Publication publication,
        final int length,
        final BufferClaim bufferClaim)
    {
        checkForValidInvocation();

        if (Cluster.Role.LEADER != role)
        {
            final int maxPayloadLength = headerBuffer.capacity() - SESSION_HEADER_LENGTH;
            if (length > maxPayloadLength)
            {
                throw new IllegalArgumentException(
                    "claim exceeds maxPayloadLength=" + maxPayloadLength + ", length=" + length);
            }

            bufferClaim.wrap(
                messageBuffer, 0, DataHeaderFlyweight.HEADER_LENGTH + SESSION_HEADER_LENGTH + length);
            return ClientSession.MOCKED_OFFER;
        }

        if (null == publication)
        {
            return Publication.NOT_CONNECTED;
        }

        final long offset = publication.tryClaim(SESSION_HEADER_LENGTH + length, bufferClaim);
        if (offset > 0)
        {
            sessionMessageHeaderEncoder
                .clusterSessionId(clusterSessionId)
                .timestamp(clusterTime);

            bufferClaim.putBytes(headerBuffer, 0, SESSION_HEADER_LENGTH);
        }

        return offset;
    }

    private void role(final Role newRole)
    {
        if (newRole != role)
        {
            role = newRole;
            activeLifecycleCallback = LIFECYCLE_CALLBACK_ON_ROLE_CHANGE;
            try
            {
                service.onRoleChange(newRole);
            }
            finally
            {
                activeLifecycleCallback = LIFECYCLE_CALLBACK_NONE;
            }
        }
    }

    private void recoverState(final CountersReader counters)
    {
        final int recoveryCounterId = awaitRecoveryCounter(counters);
        logPosition = RecoveryState.getLogPosition(counters, recoveryCounterId);
        clusterTime = RecoveryState.getTimestamp(counters, recoveryCounterId);
        final long leadershipTermId = RecoveryState.getLeadershipTermId(counters, recoveryCounterId);
        sessionMessageHeaderEncoder.leadershipTermId(leadershipTermId);

        activeLifecycleCallback = LIFECYCLE_CALLBACK_ON_START;
        try
        {
            if (NULL_VALUE != leadershipTermId)
            {
                loadSnapshot(RecoveryState.getSnapshotRecordingId(counters, recoveryCounterId, serviceId));
            }
            else
            {
                service.onStart(this, null);
            }
        }
        finally
        {
            activeLifecycleCallback = LIFECYCLE_CALLBACK_NONE;
        }

        final long id = ackId++;
        while (!consensusModuleProxy.ack(logPosition, clusterTime, id, aeron.clientId(), serviceId))
        {
            idle();
        }
    }

    private int awaitRecoveryCounter(final CountersReader counters)
    {
        idleStrategy.reset();
        int counterId = RecoveryState.findCounterId(counters, ctx.clusterId());
        while (NULL_COUNTER_ID == counterId)
        {
            idle();
            counterId = RecoveryState.findCounterId(counters, ctx.clusterId());
        }

        return counterId;
    }

    private void closeLog()
    {
        logPosition = Math.max(logAdapter.image().position(), logPosition);
        CloseHelper.close(ctx.countedErrorHandler(), logAdapter);
        disconnectEgress(ctx.countedErrorHandler());
        role(Role.FOLLOWER);
    }

    private void disconnectEgress(final CountedErrorHandler errorHandler)
    {
        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            sessions.get(i).disconnect(errorHandler);
        }
    }

    private void joinActiveLog(final ActiveLogEvent activeLog)
    {
        if (Role.LEADER != activeLog.role)
        {
            disconnectEgress(ctx.countedErrorHandler());
        }

        final String channel = new ChannelUriStringBuilder(activeLog.channel)
            .alias(subscriptionAlias)
            .build();

        Subscription logSubscription = aeron.addSubscription(channel, activeLog.streamId);
        try
        {
            final Image image = awaitImage(activeLog.sessionId, logSubscription);
            if (image.joinPosition() != logPosition)
            {
                throw new ClusterException("Cluster log must be contiguous for joining image: " +
                    "expectedPosition=" + logPosition + " joinPosition=" + image.joinPosition());
            }

            if (activeLog.logPosition != logPosition)
            {
                throw new ClusterException("Cluster log must be contiguous for active log event: " +
                    "expectedPosition=" + logPosition + " eventPosition=" + activeLog.logPosition);
            }

            logAdapter.image(image);
            logAdapter.maxLogPosition(activeLog.maxLogPosition);
            logSubscription = null;

            final long id = ackId++;
            while (!consensusModuleProxy.ack(activeLog.logPosition, clusterTime, id, NULL_VALUE, serviceId))
            {
                idle();
            }
        }
        finally
        {
            CloseHelper.quietClose(logSubscription);
        }

        memberId = activeLog.memberId;
        markFile.memberId(memberId);

        if (Role.LEADER == activeLog.role)
        {
            for (int i = 0, size = sessions.size(); i < size; i++)
            {
                final ContainerClientSession session = sessions.get(i);

                if (ctx.isRespondingService() && !activeLog.isStartup)
                {
                    session.connect(aeron);
                }

                session.resetClosing();
            }
        }

        role(activeLog.role);
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

    private ReadableCounter awaitCommitPositionCounter(final CountersReader counters, final int clusterId)
    {
        idleStrategy.reset();
        int counterId = ClusterCounters.find(counters, COMMIT_POSITION_TYPE_ID, clusterId);
        while (NULL_COUNTER_ID == counterId)
        {
            idle();
            counterId = ClusterCounters.find(counters, COMMIT_POSITION_TYPE_ID, clusterId);
        }

        return new ReadableCounter(counters, counters.getCounterRegistrationId(counterId), counterId);
    }

    private void loadSnapshot(final long recordingId)
    {
        try (AeronArchive archive = AeronArchive.connect(ctx.archiveContext().clone()))
        {
            final String channel = ctx.replayChannel();
            final int streamId = ctx.replayStreamId();
            final int sessionId = (int)archive.startReplay(recordingId, 0, NULL_VALUE, channel, streamId);

            final String replaySessionChannel = ChannelUri.addSessionId(channel, sessionId);
            try (Subscription subscription = aeron.addSubscription(replaySessionChannel, streamId))
            {
                final Image image = awaitImage(sessionId, subscription);
                loadState(image, archive);
                service.onStart(this, image);
            }
        }
    }

    private void loadState(final Image image, final AeronArchive archive)
    {
        final ServiceSnapshotLoader snapshotLoader = new ServiceSnapshotLoader(image, this);
        while (true)
        {
            final int fragments = snapshotLoader.poll();
            if (snapshotLoader.isDone())
            {
                break;
            }

            if (0 == fragments)
            {
                archive.checkForErrorResponse();
                if (image.isClosed())
                {
                    throw new ClusterException("snapshot ended unexpectedly: " + image);
                }
            }

            idle(fragments);
        }

        final int appVersion = snapshotLoader.appVersion();
        if (!ctx.appVersionValidator().isVersionCompatible(ctx.appVersion(), appVersion))
        {
            throw new ClusterException(
                "incompatible app version: " + SemanticVersion.toString(ctx.appVersion()) +
                " snapshot=" + SemanticVersion.toString(appVersion));
        }

        timeUnit = snapshotLoader.timeUnit();
    }

    private long onTakeSnapshot(final long logPosition, final long leadershipTermId)
    {
        try (AeronArchive archive = AeronArchive.connect(ctx.archiveContext().clone());
            ExclusivePublication publication = aeron.addExclusivePublication(
                ctx.snapshotChannel(), ctx.snapshotStreamId()))
        {
            final String channel = ChannelUri.addSessionId(ctx.snapshotChannel(), publication.sessionId());
            archive.startRecording(channel, ctx.snapshotStreamId(), LOCAL, true);
            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitRecordingCounter(publication.sessionId(), counters, archive);
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);

            snapshotState(publication, logPosition, leadershipTermId);
            checkForClockTick(nanoClock.nanoTime());
            archive.checkForErrorResponse();

            service.onTakeSnapshot(publication);

            awaitRecordingComplete(recordingId, publication.position(), counters, counterId, archive);

            return recordingId;
        }
        catch (final ArchiveException ex)
        {
            if (ex.errorCode() == ArchiveException.STORAGE_SPACE)
            {
                throw new AgentTerminationException(ex);
            }

            throw ex;
        }
    }

    private void awaitRecordingComplete(
        final long recordingId,
        final long position,
        final CountersReader counters,
        final int counterId,
        final AeronArchive archive)
    {
        idleStrategy.reset();
        while (counters.getCounterValue(counterId) < position)
        {
            idle();
            archive.checkForErrorResponse();

            if (!RecordingPos.isActive(counters, counterId, recordingId))
            {
                throw new ClusterException("recording stopped unexpectedly: " + recordingId);
            }
        }
    }

    private void snapshotState(
        final ExclusivePublication publication, final long logPosition, final long leadershipTermId)
    {
        final ServiceSnapshotTaker snapshotTaker = new ServiceSnapshotTaker(
            publication, idleStrategy, aeronAgentInvoker);

        snapshotTaker.markBegin(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0, timeUnit, ctx.appVersion());

        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            snapshotTaker.snapshotSession(sessions.get(i));
        }

        snapshotTaker.markEnd(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0, timeUnit, ctx.appVersion());
    }

    private void executeAction(
        final ClusterAction action,
        final long logPosition,
        final long leadershipTermId,
        final int flags)
    {
        if (ClusterAction.SNAPSHOT == action && shouldSnapshot(flags))
        {
            long recordingId = NULL_VALUE;
            Exception exception = null;
            snapshotDurationTracker.onSnapshotBegin(nanoClock.nanoTime());
            try
            {
                recordingId = onTakeSnapshot(logPosition, leadershipTermId);
            }
            catch (final Exception ex)
            {
                exception = ex;
            }
            finally
            {
                snapshotDurationTracker.onSnapshotEnd(nanoClock.nanoTime());
            }

            final long id = ackId++;
            while (!consensusModuleProxy.ack(logPosition, clusterTime, id, recordingId, serviceId))
            {
                idle();
            }

            if (null != exception)
            {
                LangUtil.rethrowUnchecked(exception);
            }
        }
    }

    private boolean shouldSnapshot(final int flags)
    {
        return CLUSTER_ACTION_FLAGS_DEFAULT == flags || 0 != (flags & standbySnapshotFlags);
    }

    private int awaitRecordingCounter(final int sessionId, final CountersReader counters, final AeronArchive archive)
    {
        idleStrategy.reset();
        final long archiveId = archive.archiveId();
        int counterId = RecordingPos.findCounterIdBySession(counters, sessionId, archiveId);
        while (NULL_COUNTER_ID == counterId)
        {
            idle();
            archive.checkForErrorResponse();
            counterId = RecordingPos.findCounterIdBySession(counters, sessionId, archiveId);
        }

        return counterId;
    }

    private boolean checkForClockTick(final long nowNs)
    {
        if (isAbort || aeron.isClosed())
        {
            isAbort = true;
            throw new AgentTerminationException("unexpected Aeron close");
        }

        if (nowNs - lastSlowTickNs > ONE_MILLISECOND_NS)
        {
            lastSlowTickNs = nowNs;

            if (null != aeronAgentInvoker)
            {
                aeronAgentInvoker.invoke();
                if (isAbort || aeron.isClosed())
                {
                    isAbort = true;
                    throw new AgentTerminationException("unexpected Aeron close");
                }
            }

            if (null != commitPosition && commitPosition.isClosed())
            {
                ctx.errorLog().record(new AeronEvent(
                    "commit-pos counter unexpectedly closed, terminating", AeronException.Category.WARN));

                throw new ClusterTerminationException(true);
            }

            final long nowMs = epochClock.time();
            if (nowMs >= markFileUpdateDeadlineMs)
            {
                markFileUpdateDeadlineMs = nowMs + MARK_FILE_UPDATE_INTERVAL_MS;
                markFile.updateActivityTimestamp(nowMs);
            }

            return true;
        }

        return false;
    }

    private int pollServiceAdapter()
    {
        int workCount = 0;

        workCount += serviceAdapter.poll();

        if (null != activeLogEvent && null == logAdapter.image())
        {
            final ActiveLogEvent event = activeLogEvent;
            activeLogEvent = null;
            joinActiveLog(event);
        }

        if (NULL_POSITION != terminationPosition && logPosition >= terminationPosition)
        {
            if (logPosition > terminationPosition)
            {
                ctx.countedErrorHandler().onError(new ClusterEvent(
                    "service terminate: logPosition=" + logPosition + " > terminationPosition=" + terminationPosition));
            }

            terminate(logPosition == terminationPosition);
        }

        if (NULL_POSITION != requestedAckPosition && logPosition >= requestedAckPosition)
        {
            if (logPosition > requestedAckPosition)
            {
                ctx.countedErrorHandler().onError(new ClusterEvent(
                    "invalid ack request: logPosition=" + logPosition +
                    " > requestedAckPosition=" + requestedAckPosition));
            }

            final long id = ackId++;
            while (!consensusModuleProxy.ack(logPosition, clusterTime, id, NULL_VALUE, serviceId))
            {
                idle();
            }
            requestedAckPosition = NULL_POSITION;
        }

        return workCount;
    }

    private void terminate(final boolean isTerminationExpected)
    {
        isServiceActive = false;
        activeLifecycleCallback = LIFECYCLE_CALLBACK_ON_TERMINATE;
        try
        {
            service.onTerminate(this);
        }
        catch (final Exception ex)
        {
            ctx.countedErrorHandler().onError(ex);
        }
        finally
        {
            activeLifecycleCallback = LIFECYCLE_CALLBACK_NONE;
        }

        try
        {
            int attempts = 5;
            final long id = ackId++;
            while (!consensusModuleProxy.ack(logPosition, clusterTime, id, NULL_VALUE, serviceId))
            {
                if (0 == --attempts)
                {
                    break;
                }
                idle();
            }
        }
        catch (final Exception ex)
        {
            ctx.countedErrorHandler().onError(ex);
        }

        terminationPosition = NULL_VALUE;
        throw new ClusterTerminationException(isTerminationExpected);
    }

    private void checkForValidInvocation()
    {
        if (LIFECYCLE_CALLBACK_NONE != activeLifecycleCallback)
        {
            throw new ClusterException(
                "sending messages or scheduling timers is not allowed from " + lifecycleName(activeLifecycleCallback));
        }
    }

    private void abort()
    {
        isAbort = true;

        try
        {
            if (!ctx.abortLatch().await(AgentRunner.RETRY_CLOSE_TIMEOUT_MS * 3L, TimeUnit.MILLISECONDS))
            {
                ctx.countedErrorHandler().onError(
                    new TimeoutException("awaiting abort latch", AeronException.Category.WARN));
            }
        }
        catch (final InterruptedException ignore)
        {
            Thread.currentThread().interrupt();
        }
    }

    private void counterUnavailable(final CountersReader countersReader, final long registrationId, final int counterId)
    {
        final ReadableCounter commitPosition = this.commitPosition;
        if (null != commitPosition &&
            commitPosition.counterId() == counterId &&
            commitPosition.registrationId() == registrationId)
        {
            commitPosition.close();
        }
    }

    private int invokeBackgroundWork(final long nowNs)
    {
        activeLifecycleCallback = LIFECYCLE_CALLBACK_DO_BACKGROUND_WORK;
        try
        {
            return service.doBackgroundWork(nowNs);
        }
        finally
        {
            activeLifecycleCallback = LIFECYCLE_CALLBACK_NONE;
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

    private void logAck(
        final int memberId,
        final long logPosition,
        final long clusterTime,
        final long id,
        final long recordingId,
        final int serviceId)
    {
    }
}
