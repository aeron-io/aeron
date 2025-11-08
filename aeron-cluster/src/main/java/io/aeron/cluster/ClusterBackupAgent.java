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

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.ControlResponsePoller;
import io.aeron.archive.client.RecordingSignalPoller;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.ControlResponseDecoder;
import io.aeron.archive.codecs.RecordingSignalEventDecoder;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.BackupResponseDecoder;
import io.aeron.cluster.codecs.ChallengeDecoder;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.SessionEventDecoder;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.status.CountersReader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.*;
import static io.aeron.archive.client.AeronArchive.NULL_LENGTH;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.NULL_TIMESTAMP;
import static io.aeron.archive.codecs.SourceLocation.REMOTE;
import static io.aeron.cluster.ClusterBackup.State.BACKING_UP;
import static io.aeron.cluster.ClusterBackup.State.BACKUP_QUERY;
import static io.aeron.cluster.ClusterBackup.State.CLOSED;
import static io.aeron.cluster.ClusterBackup.State.LIVE_LOG_RECORD;
import static io.aeron.cluster.ClusterBackup.State.LIVE_LOG_REPLAY;
import static io.aeron.cluster.ClusterBackup.State.RESET_BACKUP;
import static io.aeron.cluster.ClusterBackup.State.SNAPSHOT_RETRIEVE;
import static io.aeron.cluster.ClusterBackup.State.UPDATE_RECORDING_LOG;
import static io.aeron.exceptions.AeronException.Category;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

/**
 * {@link Agent} which backs up a remote cluster by replicating the log and polling for snapshots.
 */
public final class ClusterBackupAgent implements Agent
{
    /**
     * Update interval for cluster mark file.
     */
    public static final long MARK_FILE_UPDATE_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);

    private static final int SLOW_TICK_INTERVAL_MS = 10;
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final BackupResponseDecoder backupResponseDecoder = new BackupResponseDecoder();
    private final ChallengeDecoder challengeDecoder = new ChallengeDecoder();
    private final SessionEventDecoder sessionEventDecoder = new SessionEventDecoder();

    private final ClusterBackup.Context ctx;
    private final ClusterMarkFile markFile;
    private final AgentInvoker aeronClientInvoker;
    private final EpochClock epochClock;
    private final Aeron aeron;
    private final ConsensusPublisher consensusPublisher = new ConsensusPublisher();
    private final ArrayList<RecordingLog.Snapshot> snapshotsToRetrieve = new ArrayList<>(4);
    private final ArrayList<RecordingLog.Snapshot> snapshotsRetrieved = new ArrayList<>(4);
    private final Counter stateCounter;
    private final Counter liveLogPositionCounter;
    private final Counter nextQueryDeadlineMsCounter;
    private final ClusterBackupEventsListener eventsListener;
    private final long backupResponseTimeoutMs;
    private final long backupQueryIntervalMs;
    private final long backupProgressTimeoutMs;
    private final long coolDownIntervalMs;
    private final long unavailableCounterHandlerRegistrationId;
    private final PublicationGroup<ExclusivePublication> consensusPublicationGroup;
    private final LogSourceValidator logSourceValidator;

    private ClusterBackup.State state = BACKUP_QUERY;

    private RecordingSignalPoller recordingSignalPoller;
    private AeronArchive backupArchive;
    private AeronArchive.AsyncConnect clusterArchiveAsyncConnect;
    private AeronArchive clusterArchive;

    private SnapshotReplication snapshotReplication;

    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this::onFragment);
    private final Subscription consensusSubscription;
    private ClusterMember[] clusterMembers;
    private ClusterMember logSupplierMember;
    private RecordingLog recordingLog;
    private RecordingLog.Entry leaderLogEntry;
    private RecordingLog.Entry leaderLastTermEntry;
    private Subscription recordingSubscription;
    private String replayChannel;
    private String recordingChannel;

    private long slowTickDeadlineMs = 0;
    private long markFileUpdateDeadlineMs = 0;
    private long timeOfLastBackupQueryMs = 0;
    private long timeOfLastProgressMs = 0;
    private long coolDownDeadlineMs = NULL_VALUE;
    private long correlationId = NULL_VALUE;
    private long clusterLogRecordingId = NULL_VALUE;
    private long liveLogRecordingSubscriptionId = NULL_VALUE;
    private long liveLogRecordingId = NULL_VALUE;
    private long liveLogReplaySessionId = NULL_VALUE;
    private int leaderCommitPositionCounterId = NULL_VALUE;
    private int liveLogRecordingCounterId = NULL_COUNTER_ID;
    private int liveLogRecordingSessionId = NULL_VALUE;

    ClusterBackupAgent(final ClusterBackup.Context ctx)
    {
        this.ctx = ctx;
        aeron = ctx.aeron();
        epochClock = ctx.epochClock();

        backupResponseTimeoutMs = TimeUnit.NANOSECONDS.toMillis(ctx.clusterBackupResponseTimeoutNs());
        backupQueryIntervalMs = TimeUnit.NANOSECONDS.toMillis(ctx.clusterBackupIntervalNs());
        backupProgressTimeoutMs = TimeUnit.NANOSECONDS.toMillis(ctx.clusterBackupProgressTimeoutNs());
        coolDownIntervalMs = TimeUnit.NANOSECONDS.toMillis(ctx.clusterBackupCoolDownIntervalNs());
        markFile = ctx.clusterMarkFile();
        eventsListener = ctx.eventsListener();

        final String[] clusterConsensusEndpoints = ctx.clusterConsensusEndpoints().split(",");

        consensusPublicationGroup = new PublicationGroup<>(
            clusterConsensusEndpoints, ctx.consensusChannel(), ctx.consensusStreamId(), Aeron::addExclusivePublication);
        consensusPublicationGroup.shuffle();

        aeronClientInvoker = aeron.conductorAgentInvoker();
        aeronClientInvoker.invoke();

        unavailableCounterHandlerRegistrationId = aeron.addUnavailableCounterHandler(this::onUnavailableCounter);

        consensusSubscription = aeron.addSubscription(ctx.consensusChannel(), ctx.consensusStreamId());

        stateCounter = ctx.stateCounter();
        liveLogPositionCounter = ctx.liveLogPositionCounter();
        nextQueryDeadlineMsCounter = ctx.nextQueryDeadlineMsCounter();
        logSourceValidator = new LogSourceValidator(ctx.sourceType());
    }

    /**
     * {@inheritDoc}
     */
    public void onStart()
    {
        recordingLog = new RecordingLog(ctx.clusterDir(), true);
        backupArchive = AeronArchive.connect(ctx.archiveContext().clone());
        recordingSignalPoller = new RecordingSignalPoller(
            backupArchive.controlSessionId(),
            backupArchive.controlResponsePoller().subscription());

        final long nowMs = epochClock.time();
        nextQueryDeadlineMsCounter.setRelease(nowMs - 1);
        timeOfLastProgressMs = nowMs;
    }

    /**
     * {@inheritDoc}
     */
    public void onClose()
    {
        if (!aeron.isClosed())
        {
            aeron.removeUnavailableCounterHandler(unavailableCounterHandlerRegistrationId);

            CloseHelper.close(ctx.countedErrorHandler(), snapshotReplication);
            stopReplay();
            stopRecording();

            if (!ctx.ownsAeronClient())
            {
                CloseHelper.closeAll(
                    (ErrorHandler)ctx.countedErrorHandler(),
                    consensusSubscription,
                    consensusPublicationGroup,
                    recordingSubscription);
            }

            state(CLOSED, epochClock.time());
        }

        CloseHelper.closeAll(
            (ErrorHandler)ctx.countedErrorHandler(),
            backupArchive,
            clusterArchiveAsyncConnect,
            clusterArchive,
            recordingLog);

        markFile.signalTerminated();
        ctx.close();
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        final long nowMs = epochClock.time();
        int workCount = 0;

        try
        {
            if (nowMs > slowTickDeadlineMs)
            {
                slowTickDeadlineMs = nowMs + SLOW_TICK_INTERVAL_MS;
                workCount += slowTick(nowMs);
            }

            workCount += consensusSubscription.poll(fragmentAssembler, ConsensusAdapter.FRAGMENT_LIMIT);

            switch (state)
            {
                case BACKUP_QUERY:
                    workCount += backupQuery(nowMs);
                    break;

                case SNAPSHOT_RETRIEVE:
                    workCount += snapshotRetrieve(nowMs);
                    break;

                case LIVE_LOG_RECORD:
                    workCount += liveLogRecord(nowMs);
                    break;

                case LIVE_LOG_REPLAY:
                    workCount += liveLogReplay(nowMs);
                    break;

                case UPDATE_RECORDING_LOG:
                    workCount += updateRecordingLog(nowMs);
                    break;

                case BACKING_UP:
                    workCount += backingUp(nowMs);
                    break;

                case RESET_BACKUP:
                    workCount += resetBackup(nowMs);
                    break;

                case CLOSED:
                    return workCount;
            }

            if (hasProgressStalled(nowMs))
            {
                if (null != eventsListener)
                {
                    eventsListener.onPossibleFailure(new TimeoutException("progress has stalled", Category.WARN));
                }

                state(RESET_BACKUP, nowMs);
            }
        }
        catch (final AgentTerminationException ex)
        {
            runTerminationHook(ex);
        }
        catch (final Exception ex)
        {
            if (null != eventsListener)
            {
                eventsListener.onPossibleFailure(ex);
            }

            state(RESET_BACKUP, nowMs);
            throw ex;
        }

        return workCount;
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return "cluster-backup";
    }

    private void reset()
    {
        clusterMembers = null;
        logSupplierMember = null;
        leaderLogEntry = null;
        leaderLastTermEntry = null;
        liveLogRecordingCounterId = NULL_COUNTER_ID;
        liveLogRecordingId = NULL_VALUE;
        liveLogRecordingSessionId = NULL_VALUE;

        snapshotsToRetrieve.clear();
        snapshotsRetrieved.clear();
        fragmentAssembler.clear();

        CloseHelper.close(snapshotReplication);
        snapshotReplication = null;

        stopRecording();
        stopReplay();

        CloseHelper.closeAll(
            (ErrorHandler)ctx.countedErrorHandler(),
            consensusPublicationGroup,
            clusterArchive,
            clusterArchiveAsyncConnect,
            recordingSubscription);

        clusterArchive = null;
        clusterArchiveAsyncConnect = null;
        recordingSubscription = null;
    }

    private void stopReplay()
    {
        if (NULL_VALUE != liveLogReplaySessionId)
        {
            if (null != clusterArchive)
            {
                try
                {
                    clusterArchive.stopReplay(liveLogReplaySessionId);
                }
                catch (final Exception ex)
                {
                    ctx.countedErrorHandler().onError(new ClusterEvent("failed to stop log replay: " + ex));
                }
            }
            liveLogReplaySessionId = NULL_VALUE;
        }
    }

    private void stopRecording()
    {
        if (NULL_VALUE != liveLogRecordingSubscriptionId)
        {
            try
            {
                backupArchive.tryStopRecording(liveLogRecordingSubscriptionId);
            }
            catch (final Exception ex)
            {
                ctx.countedErrorHandler().onError(new ClusterEvent("failed to stop log recording: " + ex));
            }
            liveLogRecordingSubscriptionId = NULL_VALUE;
        }
    }

    private void onUnavailableCounter(final CountersReader counters, final long registrationId, final int counterId)
    {
        if (counterId == liveLogRecordingCounterId)
        {
            if (null != eventsListener)
            {
                eventsListener.onPossibleFailure(new ClusterEvent("log recording counter unexpectedly unavailable"));
            }

            state(RESET_BACKUP, epochClock.time());
        }
    }

    private void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        switch (messageHeaderDecoder.templateId())
        {
            case BackupResponseDecoder.TEMPLATE_ID:
                backupResponseDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final int memberId = BackupResponseDecoder.memberIdNullValue() != backupResponseDecoder.memberId() ?
                    backupResponseDecoder.memberId() : NULL_VALUE;

                onBackupResponse(
                    backupResponseDecoder.correlationId(),
                    backupResponseDecoder.logRecordingId(),
                    backupResponseDecoder.logLeadershipTermId(),
                    backupResponseDecoder.logTermBaseLogPosition(),
                    backupResponseDecoder.lastLeadershipTermId(),
                    backupResponseDecoder.lastTermBaseLogPosition(),
                    backupResponseDecoder.commitPositionCounterId(),
                    backupResponseDecoder.leaderMemberId(),
                    memberId,
                    backupResponseDecoder);
                break;

            case ChallengeDecoder.TEMPLATE_ID:
                challengeDecoder.wrapAndApplyHeader(buffer, offset, messageHeaderDecoder);
                final byte[] encodedChallenge = new byte[challengeDecoder.encodedChallengeLength()];
                challengeDecoder.getEncodedChallenge(encodedChallenge, 0, challengeDecoder.encodedChallengeLength());

                onChallenge(challengeDecoder.clusterSessionId(), encodedChallenge);
                break;

            case SessionEventDecoder.TEMPLATE_ID:
                sessionEventDecoder.wrapAndApplyHeader(buffer, offset, messageHeaderDecoder);

                final long correlationId = sessionEventDecoder.correlationId();
                final int leaderMemberId = sessionEventDecoder.leaderMemberId();
                final EventCode eventCode = sessionEventDecoder.code();
                final String detail = sessionEventDecoder.detail();

                onSessionEvent(correlationId, leaderMemberId, eventCode, detail);
                break;
        }
    }

    @SuppressWarnings("MethodLength")
    private void onBackupResponse(
        final long correlationId,
        final long logRecordingId,
        final long logLeadershipTermId,
        final long logTermBaseLogPosition,
        final long lastLeadershipTermId,
        final long lastTermBaseLogPosition,
        final int commitPositionCounterId,
        final int leaderMemberId,
        final int memberId,
        final BackupResponseDecoder backupResponseDecoder)
    {
        if (NULL_VALUE == memberId)
        {
            ctx.errorHandler().onError(new ClusterEvent(
                "onBackupResponse(): memberId is null, retrying for compatible node"));
            state(RESET_BACKUP, epochClock.time());
            return;
        }
        else if (!logSourceValidator.isAcceptable(leaderMemberId, memberId))
        {
            consensusPublicationGroup.closeAndExcludeCurrent();
            state(RESET_BACKUP, epochClock.time());
            return;
        }
        else if (null != logSupplierMember && logSupplierMember.id() != memberId)
        {
            state(RESET_BACKUP, epochClock.time());
            return;
        }

        if (BACKUP_QUERY == state && correlationId == this.correlationId)
        {
            final BackupResponseDecoder.SnapshotsDecoder snapshotsDecoder = backupResponseDecoder.snapshots();

            if (snapshotsDecoder.count() > 0)
            {
                for (final BackupResponseDecoder.SnapshotsDecoder snapshot : snapshotsDecoder)
                {
                    final RecordingLog.Entry entry = recordingLog.getLatestSnapshot(snapshot.serviceId());

                    if (null != entry && snapshot.logPosition() == entry.logPosition)
                    {
                        continue;
                    }

                    snapshotsToRetrieve.add(new RecordingLog.Snapshot(
                        snapshot.recordingId(),
                        snapshot.leadershipTermId(),
                        snapshot.termBaseLogPosition(),
                        snapshot.logPosition(),
                        snapshot.timestamp(),
                        snapshot.serviceId()));
                }
            }

            if (null == logSupplierMember ||
                memberId != logSupplierMember.id() ||
                logRecordingId != clusterLogRecordingId)
            {
                clusterLogRecordingId = logRecordingId;
                leaderLogEntry = new RecordingLog.Entry(
                    logRecordingId,
                    logLeadershipTermId,
                    logTermBaseLogPosition,
                    NULL_POSITION,
                    NULL_TIMESTAMP,
                    NULL_VALUE,
                    RecordingLog.ENTRY_TYPE_TERM,
                    null,
                    true,
                    -1);
            }

            final RecordingLog.Entry lastTerm = recordingLog.findLastTerm();

            if (null == lastTerm || lastLeadershipTermId != lastTerm.leadershipTermId)
            {
                leaderLastTermEntry = new RecordingLog.Entry(
                    logRecordingId,
                    lastLeadershipTermId,
                    lastTermBaseLogPosition,
                    NULL_POSITION,
                    NULL_TIMESTAMP,
                    NULL_VALUE,
                    RecordingLog.ENTRY_TYPE_TERM,
                    null,
                    true,
                    -1);
            }

            timeOfLastBackupQueryMs = 0;
            this.correlationId = NULL_VALUE;
            leaderCommitPositionCounterId = commitPositionCounterId;

            clusterMembers = ClusterMember.parse(backupResponseDecoder.clusterMembers());
            ClusterMember.setIsLeader(clusterMembers, leaderMemberId);

            logSupplierMember = ClusterMember.findMember(clusterMembers, memberId);
            if (null == logSupplierMember)
            {
                throw new ClusterException(memberId + " not found in " + Arrays.toString(clusterMembers));
            }

            logSupplierMember.leadershipTermId(logLeadershipTermId);

            if (null != eventsListener)
            {
                eventsListener.onBackupResponse(clusterMembers, logSupplierMember, snapshotsToRetrieve);
            }

            if (null == clusterArchive)
            {
                CloseHelper.close(clusterArchiveAsyncConnect);

                final AeronArchive.Context context = ctx.clusterArchiveContext().clone();
                final ChannelUri logSupplierArchiveUri = ChannelUri.parse(context.controlRequestChannel());
                logSupplierArchiveUri.put(ENDPOINT_PARAM_NAME, logSupplierMember.archiveEndpoint());
                context.controlRequestChannel(logSupplierArchiveUri.toString());

                if (null != logSupplierMember.archiveResponseEndpoint())
                {
                    final ChannelUri logSupplierResponseUri = ChannelUri.parse(context.controlResponseChannel());
                    logSupplierResponseUri.put(MDC_CONTROL_MODE_PARAM_NAME, CONTROL_MODE_RESPONSE);
                    logSupplierResponseUri.remove(ENDPOINT_PARAM_NAME);
                    logSupplierResponseUri.put(MDC_CONTROL_PARAM_NAME, logSupplierMember.archiveResponseEndpoint());
                    context.controlResponseChannel(logSupplierResponseUri.toString());
                }

                clusterArchiveAsyncConnect = AeronArchive.asyncConnect(context);
            }

            final long nowMs = epochClock.time();
            timeOfLastProgressMs = nowMs;
            state(snapshotsToRetrieve.isEmpty() ? LIVE_LOG_RECORD : SNAPSHOT_RETRIEVE, nowMs);
        }
    }

    private void onChallenge(final long clusterSessionId, final byte[] encodedChallenge)
    {
        final byte[] challengeResponse = ctx.credentialsSupplier().onChallenge(encodedChallenge);

        correlationId = ctx.aeron().nextCorrelationId();
        consensusPublisher.challengeResponse(
            consensusPublicationGroup.current(), this.correlationId, clusterSessionId, challengeResponse);
    }

    private void onSessionEvent(
        final long correlationId,
        final int leaderMemberId,
        final EventCode eventCode,
        final String detail)
    {
        if (this.correlationId == correlationId)
        {
            if (EventCode.ERROR == eventCode || EventCode.AUTHENTICATION_REJECTED == eventCode)
            {
                throw new ClusterException(eventCode + ": " + detail);
            }
            else if (EventCode.REDIRECT == eventCode)
            {
                consensusPublicationGroup.closeAndExcludeCurrent();
                state(RESET_BACKUP, epochClock.time());
            }
        }
    }

    private int slowTick(final long nowMs)
    {
        int workCount = aeronClientInvoker.invoke();
        if (aeron.isClosed())
        {
            throw new AgentTerminationException("unexpected Aeron close");
        }

        if (nowMs >= markFileUpdateDeadlineMs)
        {
            markFileUpdateDeadlineMs = nowMs + MARK_FILE_UPDATE_INTERVAL_MS;
            markFile.updateActivityTimestamp(nowMs);
        }

        workCount += pollBackupArchiveEvents();

        if (NULL_VALUE == correlationId && null != clusterArchive)
        {
            final String errorResponse = clusterArchive.pollForErrorResponse();
            if (null != errorResponse)
            {
                ctx.countedErrorHandler().onError(new ClusterEvent("cluster archive error: " + errorResponse));
                state(RESET_BACKUP, nowMs);
            }
        }

        return workCount;
    }

    private int resetBackup(final long nowMs)
    {
        timeOfLastProgressMs = nowMs;

        if (NULL_VALUE == coolDownDeadlineMs)
        {
            coolDownDeadlineMs = nowMs + coolDownIntervalMs;
            reset();
            return 1;
        }
        else if (nowMs > coolDownDeadlineMs)
        {
            coolDownDeadlineMs = NULL_VALUE;
            state(BACKUP_QUERY, nowMs);
            return 1;
        }

        return 0;
    }

    private int backupQuery(final long nowMs)
    {
        if (null == consensusPublicationGroup.current() || nowMs > (timeOfLastBackupQueryMs + backupResponseTimeoutMs))
        {
            consensusPublicationGroup.next(aeron);
            correlationId = NULL_VALUE;
            timeOfLastBackupQueryMs = nowMs;
            return 1;
        }
        else if (NULL_VALUE == correlationId && consensusPublicationGroup.isConnected())
        {
            final long correlationId = aeron.nextCorrelationId();

            if (consensusPublisher.backupQuery(
                consensusPublicationGroup.current(),
                correlationId,
                ctx.consensusStreamId(),
                AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION,
                ctx.consensusChannel(),
                ctx.credentialsSupplier().encodedCredentials()))
            {
                timeOfLastBackupQueryMs = nowMs;
                this.correlationId = correlationId;

                return 1;
            }
        }

        return 0;
    }

    private int snapshotRetrieve(final long nowMs)
    {
        int workCount = 0;

        if (null == clusterArchive)
        {
            final int step = clusterArchiveAsyncConnect.step();
            clusterArchive = clusterArchiveAsyncConnect.poll();
            return null == clusterArchive ? clusterArchiveAsyncConnect.step() - step : 1;
        }

        if (null == snapshotReplication)
        {
            final ChannelUri replicationUri = ChannelUri.parse(ctx.catchupChannel());
            replicationUri.put(ENDPOINT_PARAM_NAME, ctx.catchupEndpoint());
            snapshotReplication = new SnapshotReplication(
                backupArchive,
                clusterArchive.context().controlRequestStreamId(),
                clusterArchive.context().controlRequestChannel(),
                replicationUri.toString(),
                ctx.replicationProgressTimeoutNs(),
                ctx.replicationProgressIntervalNs());

            snapshotsToRetrieve.forEach(snapshotReplication::addSnapshot);
            workCount++;
        }

        workCount += snapshotReplication.poll(TimeUnit.MILLISECONDS.toNanos(nowMs));
        workCount += pollBackupArchiveEvents();
        timeOfLastProgressMs = nowMs;

        if (snapshotReplication.isComplete())
        {
            snapshotsRetrieved.addAll(snapshotReplication.snapshotsRetrieved());

            snapshotReplication.close();
            snapshotReplication = null;

            state(LIVE_LOG_RECORD, nowMs);
            workCount++;
        }

        return workCount;
    }

    private int liveLogRecord(final long nowMs)
    {
        int workCount = 0;

        if (NULL_VALUE == liveLogRecordingSubscriptionId)
        {
            if (NULL_VALUE == liveLogRecordingSessionId)
            {
                liveLogRecordingSessionId = BitUtil.generateRandomisedId();
            }

            final String catchupEndpoint = ctx.catchupEndpoint();
            if (catchupEndpoint.endsWith(":0"))
            {
                if (null == recordingSubscription)
                {
                    final ChannelUri channelUri = ChannelUri.parse(ctx.catchupChannel());
                    channelUri.remove(ENDPOINT_PARAM_NAME);
                    channelUri.put(TAGS_PARAM_NAME, aeron.nextCorrelationId() + "," + aeron.nextCorrelationId());
                    channelUri.put(SESSION_ID_PARAM_NAME, Integer.toString(liveLogRecordingSessionId));
                    recordingChannel = channelUri.toString();

                    channelUri.put(ENDPOINT_PARAM_NAME, catchupEndpoint);
                    recordingSubscription = aeron.addSubscription(channelUri.toString(), ctx.logStreamId());
                    timeOfLastProgressMs = nowMs;
                    return 1;
                }
                else
                {
                    final String resolvedEndpoint = recordingSubscription.resolvedEndpoint();
                    if (null == resolvedEndpoint)
                    {
                        return 0;
                    }

                    final ChannelUri channelUri = ChannelUri.parse(ctx.catchupChannel());
                    channelUri.put(ENDPOINT_PARAM_NAME, catchupEndpoint);
                    channelUri.replaceEndpointWildcardPort(resolvedEndpoint);
                    channelUri.put(SESSION_ID_PARAM_NAME, Integer.toString(liveLogRecordingSessionId));

                    replayChannel = channelUri.toString();
                }
            }
            else
            {
                final ChannelUri channelUri = ChannelUri.parse(ctx.catchupChannel());
                channelUri.put(ENDPOINT_PARAM_NAME, catchupEndpoint);
                channelUri.put(SESSION_ID_PARAM_NAME, Integer.toString(liveLogRecordingSessionId));
                replayChannel = channelUri.toString();
                recordingChannel = replayChannel;
            }

            liveLogRecordingSubscriptionId = startLogRecording();
        }

        timeOfLastProgressMs = nowMs;
        state(LIVE_LOG_REPLAY, nowMs);
        workCount += 1;

        return workCount;
    }

    private int liveLogReplay(final long nowMs)
    {
        int workCount = 0;

        if (NULL_VALUE == liveLogRecordingId)
        {
            if (null == clusterArchive)
            {
                final int step = clusterArchiveAsyncConnect.step();
                clusterArchive = clusterArchiveAsyncConnect.poll();
                return null == clusterArchive ? clusterArchiveAsyncConnect.step() - step : 1;
            }

            if (NULL_VALUE == correlationId)
            {
                final RecordingLog.Entry logEntry = recordingLog.findLastTerm();
                final long startPosition = replayStartPosition(logEntry);
                final long replayId = ctx.aeron().nextCorrelationId();

                if (clusterArchive.archiveProxy().boundedReplay(
                    clusterLogRecordingId,
                    startPosition,
                    NULL_LENGTH,
                    leaderCommitPositionCounterId,
                    replayChannel,
                    ctx.logStreamId(),
                    replayId,
                    clusterArchive.controlSessionId()))
                {
                    replayChannel = null;
                    correlationId = replayId;
                    timeOfLastProgressMs = nowMs;
                    workCount++;
                }
            }
            else if (NULL_VALUE == liveLogReplaySessionId)
            {
                final ControlResponsePoller poller = clusterArchive.controlResponsePoller();
                if (0 != poller.poll() && poller.isPollComplete() &&
                    poller.controlSessionId() == clusterArchive.controlSessionId() &&
                    poller.correlationId() == correlationId)
                {
                    switch (poller.code())
                    {
                        case OK ->
                        {
                            liveLogReplaySessionId = poller.relevantId();
                            timeOfLastProgressMs = nowMs;
                            workCount++;
                        }
                        case ERROR -> throwReplayFailedException(poller);
                        default -> throw new ClusterException("Live log replay failed: " + poller.code());
                    }
                }
            }
            else if (NULL_COUNTER_ID == liveLogRecordingCounterId)
            {
                final CountersReader countersReader = aeron.countersReader();

                liveLogRecordingCounterId = RecordingPos.findCounterIdBySession(
                    countersReader, (int)liveLogReplaySessionId, backupArchive.archiveId());
                if (NULL_COUNTER_ID != liveLogRecordingCounterId)
                {
                    liveLogPositionCounter.setRelease(countersReader.getCounterValue(liveLogRecordingCounterId));
                    liveLogRecordingId = RecordingPos.getRecordingId(countersReader, liveLogRecordingCounterId);
                    timeOfLastBackupQueryMs = nowMs;
                    timeOfLastProgressMs = nowMs;
                    state(UPDATE_RECORDING_LOG, nowMs);
                    workCount++;
                }
                else
                {
                    final ControlResponsePoller poller = clusterArchive.controlResponsePoller();
                    if (0 != poller.poll() && poller.isPollComplete() &&
                        poller.controlSessionId() == clusterArchive.controlSessionId() &&
                        ControlResponseCode.ERROR == poller.code())
                    {
                        throwReplayFailedException(poller);
                    }
                }
            }
        }
        else
        {
            timeOfLastProgressMs = nowMs;
            state(UPDATE_RECORDING_LOG, nowMs);
        }

        return workCount;
    }

    private void throwReplayFailedException(final ControlResponsePoller poller)
    {
        throw new ClusterException("Live log replay failed (replaySessionId=" + liveLogReplaySessionId + "):" +
            " errorMessage=" + poller.errorMessage() + ", errorCode=" +
            ArchiveException.errorCodeAsString((int)poller.relevantId()));
    }

    private int updateRecordingLog(final long nowMs)
    {
        boolean wasRecordingLogUpdated = false;
        try
        {
            final long snapshotLeadershipTermId = snapshotsRetrieved.isEmpty() ?
                NULL_VALUE : snapshotsRetrieved.get(0).leadershipTermId;

            if (null != leaderLogEntry &&
                recordingLog.isUnknown(leaderLogEntry.leadershipTermId) &&
                leaderLogEntry.leadershipTermId <= snapshotLeadershipTermId)
            {
                recordingLog.appendTerm(
                    liveLogRecordingId,
                    leaderLogEntry.leadershipTermId,
                    leaderLogEntry.termBaseLogPosition,
                    leaderLogEntry.timestamp);

                wasRecordingLogUpdated = true;
                leaderLogEntry = null;
            }

            if (!snapshotsRetrieved.isEmpty())
            {
                for (int i = snapshotsRetrieved.size() - 1; i >= 0; i--)
                {
                    final RecordingLog.Snapshot snapshot = snapshotsRetrieved.get(i);

                    recordingLog.appendSnapshot(
                        snapshot.recordingId,
                        snapshot.leadershipTermId,
                        snapshot.termBaseLogPosition,
                        snapshot.logPosition,
                        snapshot.timestamp,
                        snapshot.serviceId);
                }

                wasRecordingLogUpdated = true;
            }

            if (null != leaderLastTermEntry && recordingLog.isUnknown(leaderLastTermEntry.leadershipTermId))
            {
                recordingLog.appendTerm(
                    liveLogRecordingId,
                    leaderLastTermEntry.leadershipTermId,
                    leaderLastTermEntry.termBaseLogPosition,
                    leaderLastTermEntry.timestamp);

                wasRecordingLogUpdated = true;
                leaderLastTermEntry = null;
            }
        }
        catch (final Exception ex)
        {
            ctx.countedErrorHandler().onError(ex);
            throw new AgentTerminationException("failed to update recording log");
        }

        if (wasRecordingLogUpdated)
        {
            recordingLog.force(2);
            if (!snapshotsRetrieved.isEmpty())
            {
                ctx.snapshotRetrieveCounter().incrementRelease();
            }

            if (null != eventsListener)
            {
                eventsListener.onUpdatedRecordingLog(recordingLog, snapshotsRetrieved);
            }
        }

        snapshotsRetrieved.clear();
        snapshotsToRetrieve.clear();

        timeOfLastProgressMs = nowMs;
        nextQueryDeadlineMsCounter.setRelease(nowMs + backupQueryIntervalMs);
        state(BACKING_UP, nowMs);

        return 1;
    }

    private int backingUp(final long nowMs)
    {
        int workCount = 0;

        if (nowMs > nextQueryDeadlineMsCounter.get())
        {
            timeOfLastBackupQueryMs = nowMs;
            timeOfLastProgressMs = nowMs;
            state(BACKUP_QUERY, nowMs);
            workCount += 1;
        }

        if (NULL_COUNTER_ID != liveLogRecordingCounterId)
        {
            final long liveLogPosition = aeron.countersReader().getCounterValue(liveLogRecordingCounterId);

            if (liveLogPositionCounter.proposeMaxRelease(liveLogPosition))
            {
                if (null != eventsListener)
                {
                    eventsListener.onLiveLogProgress(liveLogRecordingId, liveLogRecordingCounterId, liveLogPosition);
                }

                workCount += 1;
            }
        }

        return workCount;
    }

    private void state(final ClusterBackup.State newState, final long nowMs)
    {
        logStateChange(state, newState, nowMs);

        if (BACKUP_QUERY == newState && null != eventsListener)
        {
            eventsListener.onBackupQuery();
        }

        if (!stateCounter.isClosed())
        {
            stateCounter.setRelease(newState.code());
        }

        state = newState;
        correlationId = NULL_VALUE;
    }

    private void logStateChange(
        final ClusterBackup.State oldState, final ClusterBackup.State newState, final long nowMs)
    {
        //System.out.println("ClusterBackup: " + oldState + " -> " + newState + " nowMs=" + nowMs);
    }

    private int pollBackupArchiveEvents()
    {
        int workCount = 0;

        if (null != backupArchive)
        {
            final RecordingSignalPoller poller = this.recordingSignalPoller;
            workCount += poller.poll();

            if (poller.isPollComplete())
            {
                final int templateId = poller.templateId();

                if (ControlResponseDecoder.TEMPLATE_ID == templateId && poller.code() == ControlResponseCode.ERROR)
                {
                    final ArchiveException ex = new ArchiveException(
                        poller.errorMessage(), (int)poller.relevantId(), poller.correlationId());

                    if (ex.errorCode() == ArchiveException.STORAGE_SPACE)
                    {
                        ctx.countedErrorHandler().onError(ex);
                        throw new AgentTerminationException();
                    }
                    else
                    {
                        throw ex;
                    }
                }
                else if (RecordingSignalEventDecoder.TEMPLATE_ID == templateId && null != snapshotReplication)
                {
                    snapshotReplication.onSignal(
                        poller.correlationId(),
                        poller.recordingId(),
                        poller.recordingPosition(),
                        poller.recordingSignal());
                }
            }
            else if (0 == workCount && !poller.subscription().isConnected())
            {
                ctx.countedErrorHandler().onError(new ClusterEvent("local archive not connected"));
                throw new AgentTerminationException();
            }
        }

        return workCount;
    }

    private long startLogRecording()
    {
        final RecordingLog.Entry logEntry = recordingLog.findLastTerm();
        final int streamId = ctx.logStreamId();
        final long recordingSubscriptionId = null == logEntry ?
            backupArchive.startRecording(recordingChannel, streamId, REMOTE, true) :
            backupArchive.extendRecording(logEntry.recordingId, recordingChannel, streamId, REMOTE, true);

        CloseHelper.close(ctx.countedErrorHandler(), recordingSubscription);
        recordingChannel = null;
        recordingSubscription = null;

        return recordingSubscriptionId;
    }

    private boolean hasProgressStalled(final long nowMs)
    {
        return (NULL_COUNTER_ID == liveLogRecordingCounterId) &&
            (nowMs > (timeOfLastProgressMs + backupProgressTimeoutMs));
    }

    private long replayStartPosition(final RecordingLog.Entry lastTerm)
    {
        return replayStartPosition(lastTerm, snapshotsRetrieved, ctx.initialReplayStart(), backupArchive);
    }

    static long replayStartPosition(
        final RecordingLog.Entry lastTerm,
        final List<RecordingLog.Snapshot> snapshotsRetrieved,
        final ClusterBackup.Configuration.ReplayStart replayStart,
        final AeronArchive backupArchive)
    {
        if (null != lastTerm)
        {
            return backupArchive.getStopPosition(lastTerm.recordingId);
        }

        if (ClusterBackup.Configuration.ReplayStart.BEGINNING == replayStart)
        {
            return NULL_POSITION;
        }

        long replayStartPosition = NULL_POSITION;
        for (final RecordingLog.Snapshot snapshot : snapshotsRetrieved)
        {
            if (ConsensusModule.Configuration.SERVICE_ID == snapshot.serviceId)
            {
                if (replayStartPosition < snapshot.logPosition)
                {
                    replayStartPosition = snapshot.logPosition;
                }
            }
        }

        return replayStartPosition;
    }

    private void runTerminationHook(final AgentTerminationException ex)
    {
        try
        {
            ctx.terminationHook().run();
        }
        catch (final Exception e)
        {
            ctx.countedErrorHandler().onError(e);
        }

        throw ex;
    }
}
