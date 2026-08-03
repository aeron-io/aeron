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
package io.aeron.archive.logging;

import io.aeron.archive.codecs.*;
import io.aeron.logging.BinaryRenderer;
import org.agrona.DirectBuffer;

/**
 * Binary renderer for the Archive admin commands.
 */
public class ArchiveControlBinaryRenderer implements BinaryRenderer
{
    private final ConnectRequestDecoder connectRequestDecoder = new ConnectRequestDecoder();
    private final CloseSessionRequestDecoder closeSessionRequestDecoder = new CloseSessionRequestDecoder();
    private final StartRecordingRequestDecoder startRecordingRequestDecoder =
        new StartRecordingRequestDecoder();
    private final StartRecordingRequest2Decoder startRecordingRequest2Decoder =
        new StartRecordingRequest2Decoder();
    private final StopRecordingRequestDecoder stopRecordingRequestDecoder = new StopRecordingRequestDecoder();
    private final ReplayRequestDecoder replayRequestDecoder = new ReplayRequestDecoder();
    private final StopReplayRequestDecoder stopReplayRequestDecoder = new StopReplayRequestDecoder();
    private final ListRecordingsRequestDecoder listRecordingsRequestDecoder =
        new ListRecordingsRequestDecoder();
    private final ListRecordingsForUriRequestDecoder listRecordingsForUriRequestDecoder =
        new ListRecordingsForUriRequestDecoder();
    private final ListRecordingRequestDecoder listRecordingRequestDecoder = new ListRecordingRequestDecoder();
    private final ExtendRecordingRequestDecoder extendRecordingRequestDecoder =
        new ExtendRecordingRequestDecoder();
    private final ExtendRecordingRequest2Decoder extendRecordingRequest2Decoder =
        new ExtendRecordingRequest2Decoder();
    private final RecordingPositionRequestDecoder recordingPositionRequestDecoder =
        new RecordingPositionRequestDecoder();
    private final MaxRecordedPositionRequestDecoder maxRecordedPositionRequestDecoder =
        new MaxRecordedPositionRequestDecoder();
    private final TruncateRecordingRequestDecoder truncateRecordingRequestDecoder =
        new TruncateRecordingRequestDecoder();
    private final StopRecordingSubscriptionRequestDecoder stopRecordingSubscriptionRequestDecoder =
        new StopRecordingSubscriptionRequestDecoder();
    private final StopPositionRequestDecoder stopPositionRequestDecoder = new StopPositionRequestDecoder();
    private final FindLastMatchingRecordingRequestDecoder findLastMatchingRecordingRequestDecoder =
        new FindLastMatchingRecordingRequestDecoder();
    private final ListRecordingSubscriptionsRequestDecoder listRecordingSubscriptionsRequestDecoder =
        new ListRecordingSubscriptionsRequestDecoder();
    private final BoundedReplayRequestDecoder boundedReplayRequestDecoder = new BoundedReplayRequestDecoder();
    private final StopAllReplaysRequestDecoder stopAllReplaysRequestDecoder =
        new StopAllReplaysRequestDecoder();
    private final ReplicateRequestDecoder replicateRequestDecoder = new ReplicateRequestDecoder();
    private final ReplicateRequest2Decoder replicateRequest2Decoder = new ReplicateRequest2Decoder();
    private final StopReplicationRequestDecoder stopReplicationRequestDecoder =
        new StopReplicationRequestDecoder();
    private final StartPositionRequestDecoder startPositionRequestDecoder = new StartPositionRequestDecoder();
    private final DetachSegmentsRequestDecoder detachSegmentsRequestDecoder =
        new DetachSegmentsRequestDecoder();
    private final DeleteDetachedSegmentsRequestDecoder deleteDetachedSegmentsRequestDecoder =
        new DeleteDetachedSegmentsRequestDecoder();
    private final PurgeSegmentsRequestDecoder purgeSegmentsRequestDecoder = new PurgeSegmentsRequestDecoder();
    private final AttachSegmentsRequestDecoder attachSegmentsRequestDecoder =
        new AttachSegmentsRequestDecoder();
    private final MigrateSegmentsRequestDecoder migrateSegmentsRequestDecoder =
        new MigrateSegmentsRequestDecoder();
    private final AuthConnectRequestDecoder authConnectRequestDecoder = new AuthConnectRequestDecoder();
    private final KeepAliveRequestDecoder keepAliveRequestDecoder = new KeepAliveRequestDecoder();
    private final TaggedReplicateRequestDecoder taggedReplicateRequestDecoder =
        new TaggedReplicateRequestDecoder();
    private final StopRecordingByIdentityRequestDecoder stopRecordingByIdentityRequestDecoder =
        new StopRecordingByIdentityRequestDecoder();
    private final PurgeRecordingRequestDecoder purgeRecordingRequestDecoder =
        new PurgeRecordingRequestDecoder();
    private final ReplayTokenRequestDecoder replayTokenRequestDecoder = new ReplayTokenRequestDecoder();
    private final ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
    private final RecordingSignalEventDecoder recordingSignalEventDecoder = new RecordingSignalEventDecoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

    private static final int[] MSG_TYPE_IDS = {
        ArchiveEventCode.CMD_IN_CONNECT.toEventCodeId(),
        ArchiveEventCode.CMD_IN_CLOSE_SESSION.toEventCodeId(),
        ArchiveEventCode.CMD_IN_START_RECORDING.toEventCodeId(),
        ArchiveEventCode.CMD_IN_STOP_RECORDING.toEventCodeId(),
        ArchiveEventCode.CMD_IN_REPLAY.toEventCodeId(),
        ArchiveEventCode.CMD_IN_STOP_REPLAY.toEventCodeId(),
        ArchiveEventCode.CMD_IN_LIST_RECORDINGS.toEventCodeId(),
        ArchiveEventCode.CMD_IN_LIST_RECORDINGS_FOR_URI.toEventCodeId(),
        ArchiveEventCode.CMD_IN_LIST_RECORDING.toEventCodeId(),
        ArchiveEventCode.CMD_IN_EXTEND_RECORDING.toEventCodeId(),
        ArchiveEventCode.CMD_IN_RECORDING_POSITION.toEventCodeId(),
        ArchiveEventCode.CMD_IN_TRUNCATE_RECORDING.toEventCodeId(),
        ArchiveEventCode.CMD_IN_STOP_RECORDING_SUBSCRIPTION.toEventCodeId(),
        ArchiveEventCode.CMD_IN_STOP_POSITION.toEventCodeId(),
        ArchiveEventCode.CMD_IN_FIND_LAST_MATCHING_RECORD.toEventCodeId(),
        ArchiveEventCode.CMD_IN_LIST_RECORDING_SUBSCRIPTIONS.toEventCodeId(),
        ArchiveEventCode.CMD_IN_START_BOUNDED_REPLAY.toEventCodeId(),
        ArchiveEventCode.CMD_IN_STOP_ALL_REPLAYS.toEventCodeId(),
        ArchiveEventCode.CMD_IN_REPLICATE.toEventCodeId(),
        ArchiveEventCode.CMD_IN_STOP_REPLICATION.toEventCodeId(),
        ArchiveEventCode.CMD_IN_START_POSITION.toEventCodeId(),
        ArchiveEventCode.CMD_IN_DETACH_SEGMENTS.toEventCodeId(),
        ArchiveEventCode.CMD_IN_DELETE_DETACHED_SEGMENTS.toEventCodeId(),
        ArchiveEventCode.CMD_IN_PURGE_SEGMENTS.toEventCodeId(),
        ArchiveEventCode.CMD_IN_ATTACH_SEGMENTS.toEventCodeId(),
        ArchiveEventCode.CMD_IN_MIGRATE_SEGMENTS.toEventCodeId(),
        ArchiveEventCode.CMD_IN_AUTH_CONNECT.toEventCodeId(),
        ArchiveEventCode.CMD_IN_KEEP_ALIVE.toEventCodeId(),
        ArchiveEventCode.CMD_IN_TAGGED_REPLICATE.toEventCodeId(),
        ArchiveEventCode.CMD_OUT_RESPONSE.toEventCodeId(),
        ArchiveEventCode.CMD_IN_START_RECORDING2.toEventCodeId(),
        ArchiveEventCode.CMD_IN_EXTEND_RECORDING2.toEventCodeId(),
        ArchiveEventCode.CMD_IN_STOP_RECORDING_BY_IDENTITY.toEventCodeId(),
        ArchiveEventCode.CMD_IN_PURGE_RECORDING.toEventCodeId(),
        ArchiveEventCode.CMD_IN_REPLICATE2.toEventCodeId(),
        ArchiveEventCode.RECORDING_SIGNAL.toEventCodeId(),
        ArchiveEventCode.CMD_IN_REQUEST_REPLAY_TOKEN.toEventCodeId(),
        ArchiveEventCode.CMD_IN_MAX_RECORDED_POSITION.toEventCodeId()
    };

    /**
     * Default constructor.
     */
    public ArchiveControlBinaryRenderer()
    {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int[] supportingMsgTypeIds()
    {
        return MSG_TYPE_IDS;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("MethodLength")
    public void append(
        final StringBuilder sb,
        final int msgTypeId,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        headerDecoder.wrap(buffer, offset);
        final int payloadOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        final int blockLength = headerDecoder.blockLength();
        final int schemaVersion = headerDecoder.version();
        final ArchiveEventCode code = ArchiveEventCode.fromEventCodeId(msgTypeId);

        switch (code)
        {
            case CMD_IN_CONNECT:
                connectRequestDecoder.wrap(
                    buffer, payloadOffset, blockLength, schemaVersion);
                renderConnect(sb);
                break;

            case CMD_IN_CLOSE_SESSION:
                closeSessionRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderCloseSession(sb);
                break;

            case CMD_IN_START_RECORDING:
                startRecordingRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStartRecording(sb);
                break;

            case CMD_IN_START_RECORDING2:
                startRecordingRequest2Decoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStartRecording2(sb);
                break;

            case CMD_IN_STOP_RECORDING:
                stopRecordingRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStopRecording(sb);
                break;

            case CMD_IN_REPLAY:
                replayRequestDecoder.wrap(
                    buffer, payloadOffset, blockLength, schemaVersion);
                renderReplay(sb);
                break;

            case CMD_IN_STOP_REPLAY:
                stopReplayRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStopReplay(sb);
                break;

            case CMD_IN_LIST_RECORDINGS:
                listRecordingsRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderListRecordings(sb);
                break;

            case CMD_IN_LIST_RECORDINGS_FOR_URI:
                listRecordingsForUriRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderListRecordingsForUri(sb);
                break;

            case CMD_IN_LIST_RECORDING:
                listRecordingRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderListRecording(sb);
                break;

            case CMD_IN_EXTEND_RECORDING:
                extendRecordingRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderExtendRecording(sb);
                break;

            case CMD_IN_EXTEND_RECORDING2:
                extendRecordingRequest2Decoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderExtendRecording2(sb);
                break;

            case CMD_IN_RECORDING_POSITION:
                recordingPositionRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderRecordingPosition(sb);
                break;

            case CMD_IN_MAX_RECORDED_POSITION:
                maxRecordedPositionRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderMaxRecordedPosition(sb);
                break;

            case CMD_IN_TRUNCATE_RECORDING:
                truncateRecordingRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderTruncateRecording(sb);
                break;

            case CMD_IN_STOP_RECORDING_SUBSCRIPTION:
                stopRecordingSubscriptionRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStopRecordingSubscription(sb);
                break;

            case CMD_IN_STOP_RECORDING_BY_IDENTITY:
                stopRecordingByIdentityRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStopRecordingByIdentity(sb);
                break;

            case CMD_IN_STOP_POSITION:
                stopPositionRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStopPosition(sb);
                break;

            case CMD_IN_FIND_LAST_MATCHING_RECORD:
                findLastMatchingRecordingRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderFindLastMatchingRecord(sb);
                break;

            case CMD_IN_LIST_RECORDING_SUBSCRIPTIONS:
                listRecordingSubscriptionsRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderListRecordingSubscriptions(sb);
                break;

            case CMD_IN_START_BOUNDED_REPLAY:
                boundedReplayRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStartBoundedReplay(sb);
                break;

            case CMD_IN_STOP_ALL_REPLAYS:
                stopAllReplaysRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStopAllReplays(sb);
                break;

            case CMD_IN_REPLICATE:
                replicateRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderReplicate(sb);
                break;

            case CMD_IN_REPLICATE2:
                replicateRequest2Decoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderReplicate2(sb);
                break;

            case CMD_IN_STOP_REPLICATION:
                stopReplicationRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStopReplication(sb);
                break;

            case CMD_IN_START_POSITION:
                startPositionRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStartPosition(sb);
                break;

            case CMD_IN_DETACH_SEGMENTS:
                detachSegmentsRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderDetachSegments(sb);
                break;

            case CMD_IN_DELETE_DETACHED_SEGMENTS:
                deleteDetachedSegmentsRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderDeleteDetachedSegments(sb);
                break;

            case CMD_IN_PURGE_SEGMENTS:
                purgeSegmentsRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderPurgeSegments(sb);
                break;

            case CMD_IN_ATTACH_SEGMENTS:
                attachSegmentsRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderAttachSegments(sb);
                break;

            case CMD_IN_MIGRATE_SEGMENTS:
                migrateSegmentsRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderMigrateSegments(sb);
                break;

            case CMD_IN_AUTH_CONNECT:
                authConnectRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderAuthConnect(sb);
                break;

            case CMD_IN_KEEP_ALIVE:
                keepAliveRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderKeepAlive(sb);
                break;

            case CMD_IN_TAGGED_REPLICATE:
                taggedReplicateRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderTaggedReplicate(sb);
                break;

            case CMD_IN_PURGE_RECORDING:
                purgeRecordingRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderPurgeRecording(sb);
                break;

            case CMD_IN_REQUEST_REPLAY_TOKEN:
                replayTokenRequestDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderReplayToken(sb);
                break;
            // Moved from original response method
            case CMD_OUT_RESPONSE:
                controlResponseDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderControlResponse(sb);
                break;
            // Moved from original signal method
            case RECORDING_SIGNAL:
                recordingSignalEventDecoder.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderRecordingSignal(sb);
                break;

            default:
                sb.append("unknown command");
                break;
        }
    }

    private void renderConnect(final StringBuilder sb)
    {
        sb.append("correlationId=").append(connectRequestDecoder.correlationId())
            .append(" responseStreamId=").append(connectRequestDecoder.responseStreamId())
            .append(" version=").append(connectRequestDecoder.version())
            .append(" responseChannel=");

        connectRequestDecoder.getResponseChannel(sb);
    }

    private void renderAuthConnect(final StringBuilder sb)
    {
        sb.append("correlationId=").append(authConnectRequestDecoder.correlationId())
            .append(" responseStreamId=").append(authConnectRequestDecoder.responseStreamId())
            .append(" version=").append(authConnectRequestDecoder.version())
            .append(" responseChannel=");

        authConnectRequestDecoder.getResponseChannel(sb);

        final int credentialsLength = authConnectRequestDecoder.encodedCredentialsLength();
        final int credentialsOffset =
            authConnectRequestDecoder.limit() + AuthConnectRequestDecoder.encodedCredentialsHeaderLength();

        sb.append(" encodedCredentialsLength=").append(credentialsLength);

        authConnectRequestDecoder.skipEncodedCredentials();

        sb.append(" clientInfo=").append(authConnectRequestDecoder.clientInfo());
    }

    private void renderCloseSession(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(closeSessionRequestDecoder.controlSessionId());
    }

    private void renderStartRecording(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(startRecordingRequestDecoder.controlSessionId())
            .append(" correlationId=").append(startRecordingRequestDecoder.correlationId())
            .append(" streamId=").append(startRecordingRequestDecoder.streamId())
            .append(" sourceLocation=").append(startRecordingRequestDecoder.sourceLocation())
            .append(" channel=");

        startRecordingRequestDecoder.getChannel(sb);
    }

    private void renderStartRecording2(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(startRecordingRequest2Decoder.controlSessionId())
            .append(" correlationId=").append(startRecordingRequest2Decoder.correlationId())
            .append(" streamId=").append(startRecordingRequest2Decoder.streamId())
            .append(" sourceLocation=").append(startRecordingRequest2Decoder.sourceLocation())
            .append(" autoStop=").append(startRecordingRequest2Decoder.autoStop())
            .append(" channel=");

        startRecordingRequest2Decoder.getChannel(sb);
    }

    private void renderStopRecording(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(stopRecordingRequestDecoder.controlSessionId())
            .append(" correlationId=").append(stopRecordingRequestDecoder.correlationId())
            .append(" streamId=").append(stopRecordingRequestDecoder.streamId())
            .append(" channel=");

        stopRecordingRequestDecoder.getChannel(sb);
    }

    private void renderReplay(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(replayRequestDecoder.controlSessionId())
            .append(" correlationId=").append(replayRequestDecoder.correlationId())
            .append(" recordingId=").append(replayRequestDecoder.recordingId())
            .append(" position=").append(replayRequestDecoder.position())
            .append(" length=").append(replayRequestDecoder.length())
            .append(" replayStreamId=").append(replayRequestDecoder.replayStreamId())
            .append(" replayChannel=");

        replayRequestDecoder.getReplayChannel(sb);
    }

    private void renderStopReplay(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(stopReplayRequestDecoder.controlSessionId())
            .append(" correlationId=").append(stopReplayRequestDecoder.correlationId())
            .append(" replaySessionId=").append(stopReplayRequestDecoder.replaySessionId());
    }

    private void renderListRecordings(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(listRecordingsRequestDecoder.controlSessionId())
            .append(" correlationId=").append(listRecordingsRequestDecoder.correlationId())
            .append(" fromRecordingId=").append(listRecordingsRequestDecoder.fromRecordingId())
            .append(" recordCount=").append(listRecordingsRequestDecoder.recordCount());
    }

    private void renderListRecording(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(listRecordingRequestDecoder.controlSessionId())
            .append(" correlationId=").append(listRecordingRequestDecoder.correlationId())
            .append(" recordingId=").append(listRecordingRequestDecoder.recordingId());
    }

    private void renderListRecordingsForUri(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(listRecordingsForUriRequestDecoder.controlSessionId())
            .append(" correlationId=").append(listRecordingsForUriRequestDecoder.correlationId())
            .append(" fromRecordingId=").append(listRecordingsForUriRequestDecoder.fromRecordingId())
            .append(" recordCount=").append(listRecordingsForUriRequestDecoder.recordCount())
            .append(" streamId=").append(listRecordingsForUriRequestDecoder.streamId())
            .append(" channel=");

        listRecordingsForUriRequestDecoder.getChannel(sb);
    }

    private void renderExtendRecording(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(extendRecordingRequestDecoder.controlSessionId())
            .append(" correlationId=").append(extendRecordingRequestDecoder.correlationId())
            .append(" recordingId=").append(extendRecordingRequestDecoder.recordingId())
            .append(" streamId=").append(extendRecordingRequestDecoder.streamId())
            .append(" sourceLocation=").append(extendRecordingRequestDecoder.sourceLocation())
            .append(" channel=");

        extendRecordingRequestDecoder.getChannel(sb);
    }

    private void renderExtendRecording2(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(extendRecordingRequest2Decoder.controlSessionId())
            .append(" correlationId=").append(extendRecordingRequest2Decoder.correlationId())
            .append(" recordingId=").append(extendRecordingRequest2Decoder.recordingId())
            .append(" streamId=").append(extendRecordingRequest2Decoder.streamId())
            .append(" sourceLocation=").append(extendRecordingRequest2Decoder.sourceLocation())
            .append(" autoStop=").append(extendRecordingRequest2Decoder.autoStop())
            .append(" channel=");

        extendRecordingRequest2Decoder.getChannel(sb);
    }

    private void renderRecordingPosition(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(recordingPositionRequestDecoder.controlSessionId())
            .append(" correlationId=").append(recordingPositionRequestDecoder.correlationId())
            .append(" recordingId=").append(recordingPositionRequestDecoder.recordingId());
    }

    private void renderMaxRecordedPosition(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(maxRecordedPositionRequestDecoder.controlSessionId())
            .append(" correlationId=").append(maxRecordedPositionRequestDecoder.correlationId())
            .append(" recordingId=").append(maxRecordedPositionRequestDecoder.recordingId());
    }

    private void renderTruncateRecording(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(truncateRecordingRequestDecoder.controlSessionId())
            .append(" correlationId=").append(truncateRecordingRequestDecoder.correlationId())
            .append(" recordingId=").append(truncateRecordingRequestDecoder.recordingId())
            .append(" position=").append(truncateRecordingRequestDecoder.position());
    }

    private void renderStopRecordingSubscription(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(stopRecordingSubscriptionRequestDecoder.controlSessionId())
            .append(" correlationId=").append(stopRecordingSubscriptionRequestDecoder.correlationId())
            .append(" subscriptionId=").append(stopRecordingSubscriptionRequestDecoder.subscriptionId());
    }

    private void renderStopRecordingByIdentity(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(stopRecordingByIdentityRequestDecoder.controlSessionId())
            .append(" correlationId=").append(stopRecordingByIdentityRequestDecoder.correlationId())
            .append(" recordingId=").append(stopRecordingByIdentityRequestDecoder.recordingId());
    }

    private void renderStopPosition(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(stopPositionRequestDecoder.controlSessionId())
            .append(" correlationId=").append(stopPositionRequestDecoder.correlationId())
            .append(" recordingId=").append(stopPositionRequestDecoder.recordingId());
    }

    private void renderFindLastMatchingRecord(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(findLastMatchingRecordingRequestDecoder.controlSessionId())
            .append(" correlationId=").append(findLastMatchingRecordingRequestDecoder.correlationId())
            .append(" minRecordingId=").append(findLastMatchingRecordingRequestDecoder.minRecordingId())
            .append(" sessionId=").append(findLastMatchingRecordingRequestDecoder.sessionId())
            .append(" streamId=").append(findLastMatchingRecordingRequestDecoder.streamId())
            .append(" channel=");

        findLastMatchingRecordingRequestDecoder.getChannel(sb);
    }

    private void renderListRecordingSubscriptions(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(listRecordingSubscriptionsRequestDecoder.controlSessionId())
            .append(" correlationId=").append(listRecordingSubscriptionsRequestDecoder.correlationId())
            .append(" pseudoIndex=").append(listRecordingSubscriptionsRequestDecoder.pseudoIndex())
            .append(" applyStreamId=").append(listRecordingSubscriptionsRequestDecoder.applyStreamId())
            .append(" subscriptionCount=").append(listRecordingSubscriptionsRequestDecoder.subscriptionCount())
            .append(" streamId=").append(listRecordingSubscriptionsRequestDecoder.streamId())
            .append(" channel=");

        listRecordingSubscriptionsRequestDecoder.getChannel(sb);
    }

    private void renderStartBoundedReplay(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(boundedReplayRequestDecoder.controlSessionId())
            .append(" correlationId=").append(boundedReplayRequestDecoder.correlationId())
            .append(" recordingId=").append(boundedReplayRequestDecoder.recordingId())
            .append(" position=").append(boundedReplayRequestDecoder.position())
            .append(" length=").append(boundedReplayRequestDecoder.length())
            .append(" limitCounterId=").append(boundedReplayRequestDecoder.limitCounterId())
            .append(" replayStreamId=").append(boundedReplayRequestDecoder.replayStreamId())
            .append(" replayChannel=");

        boundedReplayRequestDecoder.getReplayChannel(sb);
    }

    private void renderStopAllReplays(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(stopAllReplaysRequestDecoder.controlSessionId())
            .append(" correlationId=").append(stopAllReplaysRequestDecoder.correlationId())
            .append(" recordingId=").append(stopAllReplaysRequestDecoder.recordingId());
    }

    private void renderReplicate(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(replicateRequestDecoder.controlSessionId())
            .append(" correlationId=").append(replicateRequestDecoder.correlationId())
            .append(" srcRecordingId=").append(replicateRequestDecoder.srcRecordingId())
            .append(" dstRecordingId=").append(replicateRequestDecoder.dstRecordingId())
            .append(" srcControlStreamId=").append(replicateRequestDecoder.srcControlStreamId())
            .append(" srcControlChannel=");

        replicateRequestDecoder.getSrcControlChannel(sb);

        sb.append(" liveDestination=");
        replicateRequestDecoder.getLiveDestination(sb);
    }

    private void renderReplicate2(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(replicateRequest2Decoder.controlSessionId())
            .append(" correlationId=").append(replicateRequest2Decoder.correlationId())
            .append(" srcRecordingId=").append(replicateRequest2Decoder.srcRecordingId())
            .append(" dstRecordingId=").append(replicateRequest2Decoder.dstRecordingId())
            .append(" stopPosition=").append(replicateRequest2Decoder.stopPosition())
            .append(" channelTagId=").append(replicateRequest2Decoder.channelTagId())
            .append(" subscriptionTagId=").append(replicateRequest2Decoder.subscriptionTagId())
            .append(" srcControlStreamId=").append(replicateRequest2Decoder.srcControlStreamId())
            .append(" srcControlChannel=");

        replicateRequest2Decoder.getSrcControlChannel(sb);

        sb.append(" liveDestination=");
        replicateRequest2Decoder.getLiveDestination(sb);

        sb.append(" replicationChannel=");
        replicateRequest2Decoder.getReplicationChannel(sb);
    }

    private void renderStopReplication(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(stopReplicationRequestDecoder.controlSessionId())
            .append(" correlationId=").append(stopReplicationRequestDecoder.correlationId())
            .append(" replicationId=").append(stopReplicationRequestDecoder.replicationId());
    }

    private void renderStartPosition(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(startPositionRequestDecoder.controlSessionId())
            .append(" correlationId=").append(startPositionRequestDecoder.correlationId())
            .append(" recordingId=").append(startPositionRequestDecoder.recordingId());
    }

    private void renderDetachSegments(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(detachSegmentsRequestDecoder.controlSessionId())
            .append(" correlationId=").append(detachSegmentsRequestDecoder.correlationId())
            .append(" recordingId=").append(detachSegmentsRequestDecoder.recordingId());
    }

    private void renderDeleteDetachedSegments(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(deleteDetachedSegmentsRequestDecoder.controlSessionId())
            .append(" correlationId=").append(deleteDetachedSegmentsRequestDecoder.correlationId())
            .append(" recordingId=").append(deleteDetachedSegmentsRequestDecoder.recordingId());
    }

    private void renderPurgeSegments(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(purgeSegmentsRequestDecoder.controlSessionId())
            .append(" correlationId=").append(purgeSegmentsRequestDecoder.correlationId())
            .append(" recordingId=").append(purgeSegmentsRequestDecoder.recordingId())
            .append(" newStartPosition=").append(purgeSegmentsRequestDecoder.newStartPosition());
    }

    private void renderAttachSegments(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(attachSegmentsRequestDecoder.controlSessionId())
            .append(" correlationId=").append(attachSegmentsRequestDecoder.correlationId())
            .append(" recordingId=").append(attachSegmentsRequestDecoder.recordingId());
    }

    private void renderMigrateSegments(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(migrateSegmentsRequestDecoder.controlSessionId())
            .append(" correlationId=").append(migrateSegmentsRequestDecoder.correlationId())
            .append(" srcRecordingId=").append(migrateSegmentsRequestDecoder.srcRecordingId())
            .append(" dstRecordingId=").append(migrateSegmentsRequestDecoder.dstRecordingId());
    }

    private void renderKeepAlive(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(keepAliveRequestDecoder.controlSessionId())
            .append(" correlationId=").append(keepAliveRequestDecoder.correlationId());
    }

    private void renderTaggedReplicate(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(taggedReplicateRequestDecoder.controlSessionId())
            .append(" correlationId=").append(taggedReplicateRequestDecoder.correlationId())
            .append(" srcRecordingId=").append(taggedReplicateRequestDecoder.srcRecordingId())
            .append(" dstRecordingId=").append(taggedReplicateRequestDecoder.dstRecordingId())
            .append(" channelTagId=").append(taggedReplicateRequestDecoder.channelTagId())
            .append(" subscriptionTagId=").append(taggedReplicateRequestDecoder.subscriptionTagId())
            .append(" srcControlStreamId=").append(taggedReplicateRequestDecoder.srcControlStreamId())
            .append(" srcControlChannel=");

        taggedReplicateRequestDecoder.getSrcControlChannel(sb);

        sb.append(" liveDestination=");
        taggedReplicateRequestDecoder.getLiveDestination(sb);
    }

    private void renderPurgeRecording(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(purgeRecordingRequestDecoder.controlSessionId())
            .append(" correlationId=").append(purgeRecordingRequestDecoder.correlationId())
            .append(" recordingId=").append(purgeRecordingRequestDecoder.recordingId());
    }

    private void renderReplayToken(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(replayTokenRequestDecoder.controlSessionId())
            .append(" correlationId=").append(replayTokenRequestDecoder.correlationId())
            .append(" recordingId=").append(replayTokenRequestDecoder.recordingId());
    }

    private void renderControlResponse(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(controlResponseDecoder.controlSessionId())
            .append(" correlationId=").append(controlResponseDecoder.correlationId())
            .append(" relevantId=").append(controlResponseDecoder.relevantId())
            .append(" code=").append(controlResponseDecoder.code())
            .append(" version=").append(controlResponseDecoder.version())
            .append(" errorMessage=");

        controlResponseDecoder.getErrorMessage(sb);
    }

    private void renderRecordingSignal(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(recordingSignalEventDecoder.controlSessionId())
            .append(" correlationId=").append(recordingSignalEventDecoder.correlationId())
            .append(" recordingId=").append(recordingSignalEventDecoder.recordingId())
            .append(" subscriptionId=").append(recordingSignalEventDecoder.subscriptionId())
            .append(" position=").append(recordingSignalEventDecoder.position())
            .append(" signal=").append(recordingSignalEventDecoder.signal());
    }
}
