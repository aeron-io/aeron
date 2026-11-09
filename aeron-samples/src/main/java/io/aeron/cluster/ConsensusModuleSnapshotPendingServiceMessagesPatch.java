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
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.ConsensusModuleEncoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.PendingMessageTrackerEncoder;
import io.aeron.cluster.codecs.SessionMessageHeaderEncoder;
import io.aeron.cluster.service.ClusterNodeControlProperties;
import io.aeron.samples.archive.RecordingSignalCapture;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.Publication.*;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * A tool to patch the latest consensus module snapshot if it has divergence in the pending service messages state.
 * This tool patches the state on a single node. It is recommended to run the tool on the leader node and distribute
 * the patched snapshot to all other nodes in a cluster.
 * <p>
 * The tool will attempt to connect to the running MediaDriver and the Archive of the cluster node.
 * <p>
 * <em>Note: Run the tool only if the cluster is suspended or completely stopped!</em>
 */
public class ConsensusModuleSnapshotPendingServiceMessagesPatch
{
    static final int SNAPSHOT_REPLAY_STREAM_ID = 103;
    static final int SNAPSHOT_RECORDING_STREAM_ID = 107;
    static final String PATCH_CHANNEL = "aeron:ipc?alias=consensus-module-snapshot-patch|term-length=64m";
    private final String archiveLocalRequestChannel;
    private final int archiveLocalRequestStreamId;

    /**
     * Create new patch instance this default Archive values initialized to
     * {@link AeronArchive.Configuration#localControlChannel()} and
     * {@link AeronArchive.Configuration#localControlStreamId()}.
     */
    public ConsensusModuleSnapshotPendingServiceMessagesPatch()
    {
        this(AeronArchive.Configuration.localControlChannel(), AeronArchive.Configuration.localControlStreamId());
    }

    ConsensusModuleSnapshotPendingServiceMessagesPatch(
        final String archiveLocalRequestChannel, final int archiveRequestStreamId)
    {
        this.archiveLocalRequestChannel = archiveLocalRequestChannel;
        this.archiveLocalRequestStreamId = archiveRequestStreamId;
    }

    /**
     * Execute the code to patch the latest snapshot.
     *
     * @param clusterDir of the cluster node to run the patch tool on.
     * @return {@code true} if the patch was applied or {@code false} if consensus module state was already correct.
     * @throws NullPointerException     if {@code null == clusterDir}.
     * @throws IllegalArgumentException if {@code clusterDir} does not exist or is not a directory.
     * @throws ClusterException         if {@code clusterDir} does not contain a recording log or if the log does not
     *                                  contain a valid snapshot.
     */
    public boolean execute(final File clusterDir)
    {
        if (!clusterDir.exists() || !clusterDir.isDirectory())
        {
            throw new IllegalArgumentException("invalid cluster directory: " + clusterDir.getAbsolutePath());
        }

        final RecordingLog.Entry entry = ClusterTool.findLatestValidSnapshot(clusterDir);
        if (null == entry)
        {
            throw new ClusterException("no valid snapshot found");
        }

        final long recordingId = entry.recordingId;
        final ClusterNodeControlProperties properties = ClusterTool.loadControlProperties(clusterDir);
        final RecordingSignalCapture recordingSignalCapture = new RecordingSignalCapture();
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(properties.aeronDirectoryName));
            AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                .controlRequestChannel(archiveLocalRequestChannel)
                .controlRequestStreamId(archiveLocalRequestStreamId)
                .controlResponseChannel(IPC_CHANNEL)
                .recordingSignalConsumer(recordingSignalCapture)
                .aeron(aeron)))
        {
            final SnapshotReader snapshotReader = new SnapshotReader();
            replayLocalSnapshotRecording(aeron, archive, recordingId, snapshotReader);

            final TargetState[] targetStates =
                TargetState.compute(snapshotReader.serviceCount, snapshotReader.pendingMessageTrackers);

            if (snapshotIsNotValid(snapshotReader, targetStates))
            {
                final long tempRecordingId = createNewSnapshotRecording(aeron, archive, recordingId, targetStates);

                final long stopPosition = awaitRecordingStopPosition(archive, recordingId);
                final long newStopPosition = awaitRecordingStopPosition(archive, tempRecordingId);
                if (stopPosition != newStopPosition)
                {
                    throw new ClusterException("new snapshot recording incomplete: expectedStopPosition=" +
                        stopPosition + ", actualStopPosition=" + newStopPosition);
                }

                recordingSignalCapture.reset();
                archive.truncateRecording(recordingId, 0);
                recordingSignalCapture.awaitSignalForRecordingId(archive, recordingId, RecordingSignal.DELETE);

                final long replicationId = archive.replicate(
                    tempRecordingId, recordingId, archive.context().controlRequestStreamId(), IPC_CHANNEL, null);

                recordingSignalCapture.reset();
                recordingSignalCapture.awaitSignalForCorrelationId(archive, replicationId, RecordingSignal.SYNC);

                final long replicatedStopPosition = recordingSignalCapture.position();
                if (stopPosition != replicatedStopPosition)
                {
                    throw new ClusterException("incomplete replication of the new recording: expectedStopPosition=" +
                        stopPosition + ", replicatedStopPosition=" + replicatedStopPosition);
                }

                recordingSignalCapture.reset();
                archive.purgeRecording(tempRecordingId);
                recordingSignalCapture.awaitSignalForRecordingId(archive, tempRecordingId, RecordingSignal.DELETE);

                return true;
            }
        }

        return false;
    }

    static void replayLocalSnapshotRecording(
        final Aeron aeron,
        final AeronArchive archive,
        final long recordingId,
        final ConsensusModuleSnapshotListener listener)
    {
        final String channel = IPC_CHANNEL;
        final int streamId = SNAPSHOT_REPLAY_STREAM_ID;
        final int sessionId = (int)archive.startReplay(recordingId, 0, AeronArchive.NULL_LENGTH, channel, streamId);
        try
        {
            final String replayChannel = ChannelUri.addSessionId(channel, sessionId);
            try (Subscription subscription = aeron.addSubscription(replayChannel, streamId))
            {
                Image image;
                while (null == (image = subscription.imageBySessionId(sessionId)))
                {
                    archive.checkForErrorResponse();
                    Thread.yield();
                }

                final ConsensusModuleSnapshotAdapter adapter = new ConsensusModuleSnapshotAdapter(image, listener);
                while (true)
                {
                    final int fragments = adapter.poll();
                    if (adapter.isDone())
                    {
                        break;
                    }

                    if (0 == fragments)
                    {
                        if (image.isClosed())
                        {
                            throw new ClusterException("snapshot ended unexpectedly: " + image);
                        }

                        archive.checkForErrorResponse();
                        Thread.yield();
                    }
                }
            }
        }
        finally
        {
            archive.stopAllReplays(recordingId);
        }
    }

    private static boolean snapshotIsNotValid(final SnapshotReader snapshotReader, final TargetState[] targetStates)
    {
        for (int i = 0; i < targetStates.length; i++)
        {
            final TargetState targetState = targetStates[i];
            final PendingMessageTrackerState actualState = snapshotReader.pendingMessageTrackers[i];
            if (targetState.nextServiceSessionId != actualState.nextServiceSessionId ||
                targetState.logServiceSessionId != actualState.logServiceSessionId ||
                0 != actualState.pendingServiceMessageCount &&
                (targetState.logServiceSessionId + 1 != actualState.minClusterSessionId ||
                targetState.nextServiceSessionId - 1 != actualState.maxClusterSessionId))
            {
                return true;
            }
        }

        final TargetState defaultTracker = targetStates[0];
        return defaultTracker.nextServiceSessionId != snapshotReader.nextServiceSessionId ||
            defaultTracker.logServiceSessionId != snapshotReader.logServiceSessionId;
    }

    private static long createNewSnapshotRecording(
        final Aeron aeron, final AeronArchive archive, final long oldRecordingId, final TargetState[] targetStates)
    {
        try (ExclusivePublication publication = archive.addRecordedExclusivePublication(
            PATCH_CHANNEL, SNAPSHOT_RECORDING_STREAM_ID))
        {
            try
            {
                final int publicationSessionId = publication.sessionId();
                final CountersReader countersReader = aeron.countersReader();
                final int counterId = awaitRecordingCounter(publicationSessionId, archive.archiveId(), countersReader);
                final long newRecordingId = RecordingPos.getRecordingId(countersReader, counterId);

                replayLocalSnapshotRecording(
                    aeron, archive, oldRecordingId, new SnapshotWriter(publication, targetStates));

                awaitRecordingComplete(countersReader, counterId, publication.position(), newRecordingId);

                return newRecordingId;
            }
            finally
            {
                archive.stopRecording(publication);
            }
        }
    }

    private static int awaitRecordingCounter(
        final int publicationSessionId, final long archiveId, final CountersReader countersReader)
    {
        int counterId;
        while (NULL_VALUE ==
            (counterId = RecordingPos.findCounterIdBySession(countersReader, publicationSessionId, archiveId)))
        {
            Thread.yield();
        }

        return counterId;
    }

    private static void awaitRecordingComplete(
        final CountersReader counters, final int counterId, final long position, final long recordingId)
    {
        while (counters.getCounterValue(counterId) < position)
        {
            Thread.yield();
            if (!RecordingPos.isActive(counters, counterId, recordingId))
            {
                throw new ClusterException("recording has stopped unexpectedly: " + recordingId);
            }
        }
    }

    private static long awaitRecordingStopPosition(final AeronArchive archive, final long recordingId)
    {
        long stopPosition;
        while (NULL_VALUE == (stopPosition = archive.getStopPosition(recordingId)))
        {
            Thread.yield();
        }

        return stopPosition;
    }

    /**
     * Entry point to launch the tool. Requires a single parameter of a cluster directory.
     *
     * @param args for the tool.
     */
    public static void main(final String[] args)
    {
        if (1 != args.length)
        {
            System.out.println("Usage: <cluster-dir>");
            System.exit(-1);
        }

        new ConsensusModuleSnapshotPendingServiceMessagesPatch().execute(new File(args[0]));
    }

    private static final class TargetState
    {
        final long nextServiceSessionId;
        final long logServiceSessionId;
        long clusterSessionId;

        private TargetState(final long nextServiceSessionId, final long logServiceSessionId)
        {
            this.nextServiceSessionId = nextServiceSessionId;
            this.logServiceSessionId = logServiceSessionId;
            clusterSessionId = logServiceSessionId + 1;
        }

        static TargetState[] compute(final int serviceCount, final PendingMessageTrackerState[] states)
        {
            final TargetState[] targetStates = new TargetState[serviceCount];
            for (int i = 0; i < serviceCount; i++)
            {
                final PendingMessageTrackerState state = states[i];
                final long targetNextServiceSessionId = max(
                    max(state.nextServiceSessionId, state.maxClusterSessionId + 1),
                    state.logServiceSessionId + 1 + state.pendingServiceMessageCount);
                final long targetLogServiceSessionId =
                    targetNextServiceSessionId - 1 - state.pendingServiceMessageCount;
                targetStates[i] = new TargetState(targetNextServiceSessionId, targetLogServiceSessionId);
            }
            return targetStates;
        }
    }

    private static final class PendingMessageTrackerState
    {
        long nextServiceSessionId = Long.MIN_VALUE;
        long logServiceSessionId = Long.MIN_VALUE;
        long minClusterSessionId = Long.MAX_VALUE;
        long maxClusterSessionId = Long.MIN_VALUE;
        int pendingServiceMessageCount;
    }

    private static final class SnapshotReader implements ConsensusModuleSnapshotListener
    {
        long nextServiceSessionId = Long.MIN_VALUE;
        long logServiceSessionId = Long.MIN_VALUE;
        final PendingMessageTrackerState[] pendingMessageTrackers = new PendingMessageTrackerState[127];
        int serviceCount;

        @Override
        public void onLoadBeginSnapshot(
            final int appVersion,
            final TimeUnit timeUnit,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        @Override
        public void onLoadConsensusModuleState(
            final long nextSessionId,
            final long nextServiceSessionId,
            final long logServiceSessionId,
            final int pendingMessageCapacity,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            this.nextServiceSessionId = nextServiceSessionId;
            this.logServiceSessionId = logServiceSessionId;
        }

        @Override
        public void onLoadPendingMessage(
            final long clusterSessionId, final DirectBuffer buffer, final int offset, final int length)
        {
            final int serviceId = PendingServiceMessageTracker.serviceIdFromLogMessage(clusterSessionId);
            final PendingMessageTrackerState trackerState = tracker(serviceId);
            trackerState.pendingServiceMessageCount++;
            trackerState.minClusterSessionId = min(trackerState.minClusterSessionId, clusterSessionId);
            trackerState.maxClusterSessionId = max(trackerState.maxClusterSessionId, clusterSessionId);
        }

        @Override
        public void onLoadClusterSession(
            final long clusterSessionId,
            final long correlationId,
            final long openedLogPosition,
            final long timeOfLastActivity,
            final CloseReason closeReason,
            final int responseStreamId,
            final String responseChannel,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        @Override
        public void onLoadTimer(
            final long correlationId,
            final long deadline,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        @Override
        public void onLoadPendingMessageTracker(
            final long nextServiceSessionId,
            final long logServiceSessionId,
            final int pendingMessageCapacity,
            final int serviceId,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            final PendingMessageTrackerState trackerState = tracker(serviceId);
            trackerState.nextServiceSessionId = nextServiceSessionId;
            trackerState.logServiceSessionId = logServiceSessionId;
        }

        @Override
        public void onLoadEndSnapshot(final DirectBuffer buffer, final int offset, final int length)
        {
        }

        private PendingMessageTrackerState tracker(final int serviceId)
        {
            PendingMessageTrackerState trackerState = pendingMessageTrackers[serviceId];
            if (null == trackerState)
            {
                trackerState = new PendingMessageTrackerState();
                pendingMessageTrackers[serviceId] = trackerState;
                serviceCount++;
            }
            return trackerState;
        }
    }

    private static final class SnapshotWriter implements ConsensusModuleSnapshotListener
    {
        private final ExpandableArrayBuffer tempBuffer = new ExpandableArrayBuffer(1024);
        private final ConsensusModuleEncoder consensusModuleEncoder = new ConsensusModuleEncoder();
        private final SessionMessageHeaderEncoder sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();
        private final PendingMessageTrackerEncoder pendingMessageTrackerEncoder = new PendingMessageTrackerEncoder();
        private final ExclusivePublication snapshotPublication;
        private final TargetState[] targetStates;

        SnapshotWriter(
            final ExclusivePublication snapshotPublication,
            final TargetState[] targetStates)
        {
            this.snapshotPublication = snapshotPublication;
            this.targetStates = targetStates;
        }

        @Override
        public void onLoadBeginSnapshot(
            final int appVersion,
            final TimeUnit timeUnit,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            writeToSnapshot(buffer, offset, length);
        }

        @Override
        public void onLoadConsensusModuleState(
            final long nextSessionId,
            final long nextServiceSessionId,
            final long logServiceSessionId,
            final int pendingMessageCapacity,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            final TargetState defaultTrackerState = targetStates[0];

            tempBuffer.putBytes(0, buffer, offset, length);
            consensusModuleEncoder
                .wrap(tempBuffer, MessageHeaderEncoder.ENCODED_LENGTH)
                .logServiceSessionId(defaultTrackerState.logServiceSessionId)
                .nextServiceSessionId(defaultTrackerState.nextServiceSessionId);

            writeToSnapshot(tempBuffer, 0, length);
        }

        @Override
        public void onLoadPendingMessage(
            final long clusterSessionId,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            final int serviceId = PendingServiceMessageTracker.serviceIdFromLogMessage(clusterSessionId);
            final TargetState targetState = targetStates[serviceId];

            tempBuffer.putBytes(0, buffer, offset, length);
            sessionMessageHeaderEncoder
                .wrap(tempBuffer, MessageHeaderEncoder.ENCODED_LENGTH)
                .clusterSessionId(targetState.clusterSessionId++);

            writeToSnapshot(tempBuffer, 0, length);
        }

        @Override
        public void onLoadClusterSession(
            final long clusterSessionId,
            final long correlationId,
            final long openedLogPosition,
            final long timeOfLastActivity,
            final CloseReason closeReason,
            final int responseStreamId,
            final String responseChannel,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            writeToSnapshot(buffer, offset, length);
        }

        @Override
        public void onLoadTimer(
            final long correlationId,
            final long deadline,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            writeToSnapshot(buffer, offset, length);
        }

        @Override
        public void onLoadPendingMessageTracker(
            final long nextServiceSessionId,
            final long logServiceSessionId,
            final int pendingMessageCapacity,
            final int serviceId,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            final TargetState targetState = targetStates[serviceId];

            tempBuffer.putBytes(0, buffer, offset, length);
            pendingMessageTrackerEncoder
                .wrap(tempBuffer, MessageHeaderEncoder.ENCODED_LENGTH)
                .logServiceSessionId(targetState.logServiceSessionId)
                .nextServiceSessionId(targetState.nextServiceSessionId);

            writeToSnapshot(tempBuffer, 0, length);
        }

        @Override
        public void onLoadEndSnapshot(final DirectBuffer buffer, final int offset, final int length)
        {
            writeToSnapshot(buffer, offset, length);
        }

        private void writeToSnapshot(final DirectBuffer buffer, final int offset, final int length)
        {
            long position;
            while ((position = snapshotPublication.offer(buffer, offset, length)) < 0)
            {
                if (position == CLOSED ||
                    position == NOT_CONNECTED ||
                    position == MAX_POSITION_EXCEEDED)
                {
                    throw new ClusterException("cannot offer into a snapshot: " + Publication.errorString(position));
                }

                Thread.yield();
            }
        }
    }
}
