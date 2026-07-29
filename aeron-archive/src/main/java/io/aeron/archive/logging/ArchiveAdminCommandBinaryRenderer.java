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
public class ArchiveAdminCommandBinaryRenderer implements BinaryRenderer
{
    private static final ConnectRequestDecoder CONNECT_REQUEST_DECODER = new ConnectRequestDecoder();
    private static final CloseSessionRequestDecoder CLOSE_SESSION_REQUEST_DECODER = new CloseSessionRequestDecoder();
    private static final StartRecordingRequestDecoder START_RECORDING_REQUEST_DECODER =
        new StartRecordingRequestDecoder();
    private static final StartRecordingRequest2Decoder START_RECORDING_REQUEST2_DECODER =
        new StartRecordingRequest2Decoder();
    private static final StopRecordingRequestDecoder STOP_RECORDING_REQUEST_DECODER = new StopRecordingRequestDecoder();
    private static final ReplayRequestDecoder REPLAY_REQUEST_DECODER = new ReplayRequestDecoder();
    private static final StopReplayRequestDecoder STOP_REPLAY_REQUEST_DECODER = new StopReplayRequestDecoder();
    private static final ListRecordingsRequestDecoder LIST_RECORDINGS_REQUEST_DECODER =
        new ListRecordingsRequestDecoder();
    private static final ListRecordingsForUriRequestDecoder LIST_RECORDINGS_FOR_URI_REQUEST_DECODER =
        new ListRecordingsForUriRequestDecoder();
    private static final ListRecordingRequestDecoder LIST_RECORDING_REQUEST_DECODER = new ListRecordingRequestDecoder();
    private static final ExtendRecordingRequestDecoder EXTEND_RECORDING_REQUEST_DECODER =
        new ExtendRecordingRequestDecoder();
    private static final ExtendRecordingRequest2Decoder EXTEND_RECORDING_REQUEST2_DECODER =
        new ExtendRecordingRequest2Decoder();
    private static final RecordingPositionRequestDecoder RECORDING_POSITION_REQUEST_DECODER =
        new RecordingPositionRequestDecoder();
    private static final MaxRecordedPositionRequestDecoder MAX_RECORDED_POSITION_REQUEST_DECODER =
        new MaxRecordedPositionRequestDecoder();
    private static final TruncateRecordingRequestDecoder TRUNCATE_RECORDING_REQUEST_DECODER =
        new TruncateRecordingRequestDecoder();
    private static final StopRecordingSubscriptionRequestDecoder STOP_RECORDING_SUBSCRIPTION_REQUEST_DECODER =
        new StopRecordingSubscriptionRequestDecoder();
    private static final StopPositionRequestDecoder STOP_POSITION_REQUEST_DECODER = new StopPositionRequestDecoder();
    private static final FindLastMatchingRecordingRequestDecoder FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER =
        new FindLastMatchingRecordingRequestDecoder();
    private static final ListRecordingSubscriptionsRequestDecoder LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER =
        new ListRecordingSubscriptionsRequestDecoder();
    private static final BoundedReplayRequestDecoder BOUNDED_REPLAY_REQUEST_DECODER = new BoundedReplayRequestDecoder();
    private static final StopAllReplaysRequestDecoder STOP_ALL_REPLAYS_REQUEST_DECODER =
        new StopAllReplaysRequestDecoder();
    private static final ReplicateRequestDecoder REPLICATE_REQUEST_DECODER = new ReplicateRequestDecoder();
    private static final ReplicateRequest2Decoder REPLICATE_REQUEST2_DECODER = new ReplicateRequest2Decoder();
    private static final StopReplicationRequestDecoder STOP_REPLICATION_REQUEST_DECODER =
        new StopReplicationRequestDecoder();
    private static final StartPositionRequestDecoder START_POSITION_REQUEST_DECODER = new StartPositionRequestDecoder();
    private static final DetachSegmentsRequestDecoder DETACH_SEGMENTS_REQUEST_DECODER =
        new DetachSegmentsRequestDecoder();
    private static final DeleteDetachedSegmentsRequestDecoder DELETE_DETACHED_SEGMENTS_REQUEST_DECODER =
        new DeleteDetachedSegmentsRequestDecoder();
    private static final PurgeSegmentsRequestDecoder PURGE_SEGMENTS_REQUEST_DECODER = new PurgeSegmentsRequestDecoder();
    private static final AttachSegmentsRequestDecoder ATTACH_SEGMENTS_REQUEST_DECODER =
        new AttachSegmentsRequestDecoder();
    private static final MigrateSegmentsRequestDecoder MIGRATE_SEGMENTS_REQUEST_DECODER =
        new MigrateSegmentsRequestDecoder();
    private static final AuthConnectRequestDecoder AUTH_CONNECT_REQUEST_DECODER = new AuthConnectRequestDecoder();
    private static final KeepAliveRequestDecoder KEEP_ALIVE_REQUEST_DECODER = new KeepAliveRequestDecoder();
    private static final TaggedReplicateRequestDecoder TAGGED_REPLICATE_REQUEST_DECODER =
        new TaggedReplicateRequestDecoder();
    private static final StopRecordingByIdentityRequestDecoder STOP_RECORDING_BY_IDENTITY_REQUEST_DECODER =
        new StopRecordingByIdentityRequestDecoder();
    private static final PurgeRecordingRequestDecoder PURGE_RECORDING_REQUEST_DECODER =
        new PurgeRecordingRequestDecoder();
    private static final ReplayTokenRequestDecoder REPLAY_TOKEN_REQUEST_DECODER = new ReplayTokenRequestDecoder();
    private static final ControlResponseDecoder CONTROL_RESPONSE_DECODER = new ControlResponseDecoder();
    private static final RecordingSignalEventDecoder RECORDING_SIGNAL_EVENT_DECODER = new RecordingSignalEventDecoder();
    private static final MessageHeaderDecoder HEADER_DECODER = new MessageHeaderDecoder();

    private static final int[] MSG_TYPE_ID = {
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
    public ArchiveAdminCommandBinaryRenderer()
    {
    }

    /**
     * {@inheritDoc}
     */
    public int[] supportingMsgTypeIds()
    {
        return MSG_TYPE_ID;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("MethodLength")
    public void append(
        final StringBuilder sb,
        final int msgTypeId,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        HEADER_DECODER.wrap(buffer, offset);
        final int payloadOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        final int blockLength = HEADER_DECODER.blockLength();
        final int schemaVersion = HEADER_DECODER.version();
        final ArchiveEventCode code = ArchiveEventCode.fromEventCodeId(msgTypeId);

        switch (code)
        {
            case CMD_IN_CONNECT:
                CONNECT_REQUEST_DECODER.wrap(
                    buffer, payloadOffset, blockLength, schemaVersion);
                renderConnect(sb);
                break;

            case CMD_IN_CLOSE_SESSION:
                CLOSE_SESSION_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderCloseSession(sb);
                break;

            case CMD_IN_START_RECORDING:
                START_RECORDING_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStartRecording(sb);
                break;

            case CMD_IN_START_RECORDING2:
                START_RECORDING_REQUEST2_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStartRecording2(sb);
                break;

            case CMD_IN_STOP_RECORDING:
                STOP_RECORDING_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStopRecording(sb);
                break;

            case CMD_IN_REPLAY:
                REPLAY_REQUEST_DECODER.wrap(
                    buffer, payloadOffset, blockLength, schemaVersion);
                renderReplay(sb);
                break;

            case CMD_IN_STOP_REPLAY:
                STOP_REPLAY_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStopReplay(sb);
                break;

            case CMD_IN_LIST_RECORDINGS:
                LIST_RECORDINGS_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderListRecordings(sb);
                break;

            case CMD_IN_LIST_RECORDINGS_FOR_URI:
                LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderListRecordingsForUri(sb);
                break;

            case CMD_IN_LIST_RECORDING:
                LIST_RECORDING_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderListRecording(sb);
                break;

            case CMD_IN_EXTEND_RECORDING:
                EXTEND_RECORDING_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderExtendRecording(sb);
                break;

            case CMD_IN_EXTEND_RECORDING2:
                EXTEND_RECORDING_REQUEST2_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderExtendRecording2(sb);
                break;

            case CMD_IN_RECORDING_POSITION:
                RECORDING_POSITION_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderRecordingPosition(sb);
                break;

            case CMD_IN_MAX_RECORDED_POSITION:
                MAX_RECORDED_POSITION_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderMaxRecordedPosition(sb);
                break;

            case CMD_IN_TRUNCATE_RECORDING:
                TRUNCATE_RECORDING_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderTruncateRecording(sb);
                break;

            case CMD_IN_STOP_RECORDING_SUBSCRIPTION:
                STOP_RECORDING_SUBSCRIPTION_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStopRecordingSubscription(sb);
                break;

            case CMD_IN_STOP_RECORDING_BY_IDENTITY:
                STOP_RECORDING_BY_IDENTITY_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStopRecordingByIdentity(sb);
                break;

            case CMD_IN_STOP_POSITION:
                STOP_POSITION_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStopPosition(sb);
                break;

            case CMD_IN_FIND_LAST_MATCHING_RECORD:
                FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderFindLastMatchingRecord(sb);
                break;

            case CMD_IN_LIST_RECORDING_SUBSCRIPTIONS:
                LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderListRecordingSubscriptions(sb);
                break;

            case CMD_IN_START_BOUNDED_REPLAY:
                BOUNDED_REPLAY_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStartBoundedReplay(sb);
                break;

            case CMD_IN_STOP_ALL_REPLAYS:
                STOP_ALL_REPLAYS_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStopAllReplays(sb);
                break;

            case CMD_IN_REPLICATE:
                REPLICATE_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderReplicate(sb);
                break;

            case CMD_IN_REPLICATE2:
                REPLICATE_REQUEST2_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderReplicate2(sb);
                break;

            case CMD_IN_STOP_REPLICATION:
                STOP_REPLICATION_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStopReplication(sb);
                break;

            case CMD_IN_START_POSITION:
                START_POSITION_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderStartPosition(sb);
                break;

            case CMD_IN_DETACH_SEGMENTS:
                DETACH_SEGMENTS_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderDetachSegments(sb);
                break;

            case CMD_IN_DELETE_DETACHED_SEGMENTS:
                DELETE_DETACHED_SEGMENTS_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderDeleteDetachedSegments(sb);
                break;

            case CMD_IN_PURGE_SEGMENTS:
                PURGE_SEGMENTS_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderPurgeSegments(sb);
                break;

            case CMD_IN_ATTACH_SEGMENTS:
                ATTACH_SEGMENTS_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderAttachSegments(sb);
                break;

            case CMD_IN_MIGRATE_SEGMENTS:
                MIGRATE_SEGMENTS_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderMigrateSegments(sb);
                break;

            case CMD_IN_AUTH_CONNECT:
                AUTH_CONNECT_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderAuthConnect(sb);
                break;

            case CMD_IN_KEEP_ALIVE:
                KEEP_ALIVE_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderKeepAlive(sb);
                break;

            case CMD_IN_TAGGED_REPLICATE:
                TAGGED_REPLICATE_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderTaggedReplicate(sb);
                break;

            case CMD_IN_PURGE_RECORDING:
                PURGE_RECORDING_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderPurgeRecording(sb);
                break;

            case CMD_IN_REQUEST_REPLAY_TOKEN:
                REPLAY_TOKEN_REQUEST_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderReplayToken(sb);
                break;
            // Moved from original response method
            case CMD_OUT_RESPONSE:
                CONTROL_RESPONSE_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderControlResponse(sb);
                break;
            // Moved from original signal method
            case RECORDING_SIGNAL:
                RECORDING_SIGNAL_EVENT_DECODER.wrap(
                    buffer, payloadOffset,
                    blockLength, schemaVersion);
                renderRecordingSignal(sb);
                break;

            default:
                sb.append("unknown command");
                break;
        }
    }

    private static void renderConnect(final StringBuilder sb)
    {
        sb.append("correlationId=").append(CONNECT_REQUEST_DECODER.correlationId())
            .append(" responseStreamId=").append(CONNECT_REQUEST_DECODER.responseStreamId())
            .append(" version=").append(CONNECT_REQUEST_DECODER.version())
            .append(" responseChannel=");

        CONNECT_REQUEST_DECODER.getResponseChannel(sb);
    }

    private static void renderAuthConnect(final StringBuilder sb)
    {
        sb.append("correlationId=").append(AUTH_CONNECT_REQUEST_DECODER.correlationId())
            .append(" responseStreamId=").append(AUTH_CONNECT_REQUEST_DECODER.responseStreamId())
            .append(" version=").append(AUTH_CONNECT_REQUEST_DECODER.version())
            .append(" responseChannel=");

        AUTH_CONNECT_REQUEST_DECODER.getResponseChannel(sb);

        sb.append(" encodedCredentialsLength=").append(AUTH_CONNECT_REQUEST_DECODER.encodedCredentialsLength());

        AUTH_CONNECT_REQUEST_DECODER.skipEncodedCredentials();

        sb.append(" clientInfo=").append(AUTH_CONNECT_REQUEST_DECODER.clientInfo());
    }

    private static void renderCloseSession(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(CLOSE_SESSION_REQUEST_DECODER.controlSessionId());
    }

    private static void renderStartRecording(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(START_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(START_RECORDING_REQUEST_DECODER.correlationId())
            .append(" streamId=").append(START_RECORDING_REQUEST_DECODER.streamId())
            .append(" sourceLocation=").append(START_RECORDING_REQUEST_DECODER.sourceLocation())
            .append(" channel=");

        START_RECORDING_REQUEST_DECODER.getChannel(sb);
    }

    private static void renderStartRecording2(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(START_RECORDING_REQUEST2_DECODER.controlSessionId())
            .append(" correlationId=").append(START_RECORDING_REQUEST2_DECODER.correlationId())
            .append(" streamId=").append(START_RECORDING_REQUEST2_DECODER.streamId())
            .append(" sourceLocation=").append(START_RECORDING_REQUEST2_DECODER.sourceLocation())
            .append(" autoStop=").append(START_RECORDING_REQUEST2_DECODER.autoStop())
            .append(" channel=");

        START_RECORDING_REQUEST2_DECODER.getChannel(sb);
    }

    private static void renderStopRecording(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(STOP_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(STOP_RECORDING_REQUEST_DECODER.correlationId())
            .append(" streamId=").append(STOP_RECORDING_REQUEST_DECODER.streamId())
            .append(" channel=");

        STOP_RECORDING_REQUEST_DECODER.getChannel(sb);
    }

    private static void renderReplay(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(REPLAY_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(REPLAY_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(REPLAY_REQUEST_DECODER.recordingId())
            .append(" position=").append(REPLAY_REQUEST_DECODER.position())
            .append(" length=").append(REPLAY_REQUEST_DECODER.length())
            .append(" replayStreamId=").append(REPLAY_REQUEST_DECODER.replayStreamId())
            .append(" replayChannel=");

        REPLAY_REQUEST_DECODER.getReplayChannel(sb);
    }

    private static void renderStopReplay(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(STOP_REPLAY_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(STOP_REPLAY_REQUEST_DECODER.correlationId())
            .append(" replaySessionId=").append(STOP_REPLAY_REQUEST_DECODER.replaySessionId());
    }

    private static void renderListRecordings(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(LIST_RECORDINGS_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(LIST_RECORDINGS_REQUEST_DECODER.correlationId())
            .append(" fromRecordingId=").append(LIST_RECORDINGS_REQUEST_DECODER.fromRecordingId())
            .append(" recordCount=").append(LIST_RECORDINGS_REQUEST_DECODER.recordCount());
    }

    private static void renderListRecording(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(LIST_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(LIST_RECORDING_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(LIST_RECORDING_REQUEST_DECODER.recordingId());
    }

    private static void renderListRecordingsForUri(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.correlationId())
            .append(" fromRecordingId=").append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.fromRecordingId())
            .append(" recordCount=").append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.recordCount())
            .append(" streamId=").append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.streamId())
            .append(" channel=");

        LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.getChannel(sb);
    }

    private static void renderExtendRecording(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(EXTEND_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(EXTEND_RECORDING_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(EXTEND_RECORDING_REQUEST_DECODER.recordingId())
            .append(" streamId=").append(EXTEND_RECORDING_REQUEST_DECODER.streamId())
            .append(" sourceLocation=").append(EXTEND_RECORDING_REQUEST_DECODER.sourceLocation())
            .append(" channel=");

        EXTEND_RECORDING_REQUEST_DECODER.getChannel(sb);
    }

    private static void renderExtendRecording2(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(EXTEND_RECORDING_REQUEST2_DECODER.controlSessionId())
            .append(" correlationId=").append(EXTEND_RECORDING_REQUEST2_DECODER.correlationId())
            .append(" recordingId=").append(EXTEND_RECORDING_REQUEST2_DECODER.recordingId())
            .append(" streamId=").append(EXTEND_RECORDING_REQUEST2_DECODER.streamId())
            .append(" sourceLocation=").append(EXTEND_RECORDING_REQUEST2_DECODER.sourceLocation())
            .append(" autoStop=").append(EXTEND_RECORDING_REQUEST2_DECODER.autoStop())
            .append(" channel=");

        EXTEND_RECORDING_REQUEST2_DECODER.getChannel(sb);
    }

    private static void renderRecordingPosition(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(RECORDING_POSITION_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(RECORDING_POSITION_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(RECORDING_POSITION_REQUEST_DECODER.recordingId());
    }

    private static void renderMaxRecordedPosition(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(MAX_RECORDED_POSITION_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(MAX_RECORDED_POSITION_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(MAX_RECORDED_POSITION_REQUEST_DECODER.recordingId());
    }

    private static void renderTruncateRecording(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(TRUNCATE_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(TRUNCATE_RECORDING_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(TRUNCATE_RECORDING_REQUEST_DECODER.recordingId())
            .append(" position=").append(TRUNCATE_RECORDING_REQUEST_DECODER.position());
    }

    private static void renderStopRecordingSubscription(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(STOP_RECORDING_SUBSCRIPTION_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(STOP_RECORDING_SUBSCRIPTION_REQUEST_DECODER.correlationId())
            .append(" subscriptionId=").append(STOP_RECORDING_SUBSCRIPTION_REQUEST_DECODER.subscriptionId());
    }

    private static void renderStopRecordingByIdentity(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(STOP_RECORDING_BY_IDENTITY_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(STOP_RECORDING_BY_IDENTITY_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(STOP_RECORDING_BY_IDENTITY_REQUEST_DECODER.recordingId());
    }

    private static void renderStopPosition(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(STOP_POSITION_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(STOP_POSITION_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(STOP_POSITION_REQUEST_DECODER.recordingId());
    }

    private static void renderFindLastMatchingRecord(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.correlationId())
            .append(" minRecordingId=").append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.minRecordingId())
            .append(" sessionId=").append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.sessionId())
            .append(" streamId=").append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.streamId())
            .append(" channel=");

        FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.getChannel(sb);
    }

    private static void renderListRecordingSubscriptions(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.correlationId())
            .append(" pseudoIndex=").append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.pseudoIndex())
            .append(" applyStreamId=").append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.applyStreamId())
            .append(" subscriptionCount=").append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.subscriptionCount())
            .append(" streamId=").append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.streamId())
            .append(" channel=");

        LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.getChannel(sb);
    }

    private static void renderStartBoundedReplay(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(BOUNDED_REPLAY_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(BOUNDED_REPLAY_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(BOUNDED_REPLAY_REQUEST_DECODER.recordingId())
            .append(" position=").append(BOUNDED_REPLAY_REQUEST_DECODER.position())
            .append(" length=").append(BOUNDED_REPLAY_REQUEST_DECODER.length())
            .append(" limitCounterId=").append(BOUNDED_REPLAY_REQUEST_DECODER.limitCounterId())
            .append(" replayStreamId=").append(BOUNDED_REPLAY_REQUEST_DECODER.replayStreamId())
            .append(" replayChannel=");

        BOUNDED_REPLAY_REQUEST_DECODER.getReplayChannel(sb);
    }

    private static void renderStopAllReplays(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(STOP_ALL_REPLAYS_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(STOP_ALL_REPLAYS_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(STOP_ALL_REPLAYS_REQUEST_DECODER.recordingId());
    }

    private static void renderReplicate(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(REPLICATE_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(REPLICATE_REQUEST_DECODER.correlationId())
            .append(" srcRecordingId=").append(REPLICATE_REQUEST_DECODER.srcRecordingId())
            .append(" dstRecordingId=").append(REPLICATE_REQUEST_DECODER.dstRecordingId())
            .append(" srcControlStreamId=").append(REPLICATE_REQUEST_DECODER.srcControlStreamId())
            .append(" srcControlChannel=");

        REPLICATE_REQUEST_DECODER.getSrcControlChannel(sb);

        sb.append(" liveDestination=");
        REPLICATE_REQUEST_DECODER.getLiveDestination(sb);
    }

    private static void renderReplicate2(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(REPLICATE_REQUEST2_DECODER.controlSessionId())
            .append(" correlationId=").append(REPLICATE_REQUEST2_DECODER.correlationId())
            .append(" srcRecordingId=").append(REPLICATE_REQUEST2_DECODER.srcRecordingId())
            .append(" dstRecordingId=").append(REPLICATE_REQUEST2_DECODER.dstRecordingId())
            .append(" stopPosition=").append(REPLICATE_REQUEST2_DECODER.stopPosition())
            .append(" channelTagId=").append(REPLICATE_REQUEST2_DECODER.channelTagId())
            .append(" subscriptionTagId=").append(REPLICATE_REQUEST2_DECODER.subscriptionTagId())
            .append(" srcControlStreamId=").append(REPLICATE_REQUEST2_DECODER.srcControlStreamId())
            .append(" srcControlChannel=");

        REPLICATE_REQUEST2_DECODER.getSrcControlChannel(sb);

        sb.append(" liveDestination=");
        REPLICATE_REQUEST2_DECODER.getLiveDestination(sb);

        sb.append(" replicationChannel=");
        REPLICATE_REQUEST2_DECODER.getReplicationChannel(sb);
    }

    private static void renderStopReplication(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(STOP_REPLICATION_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(STOP_REPLICATION_REQUEST_DECODER.correlationId())
            .append(" replicationId=").append(STOP_REPLICATION_REQUEST_DECODER.replicationId());
    }

    private static void renderStartPosition(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(START_POSITION_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(START_POSITION_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(START_POSITION_REQUEST_DECODER.recordingId());
    }

    private static void renderDetachSegments(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(DETACH_SEGMENTS_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(DETACH_SEGMENTS_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(DETACH_SEGMENTS_REQUEST_DECODER.recordingId());
    }

    private static void renderDeleteDetachedSegments(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(DELETE_DETACHED_SEGMENTS_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(DELETE_DETACHED_SEGMENTS_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(DELETE_DETACHED_SEGMENTS_REQUEST_DECODER.recordingId());
    }

    private static void renderPurgeSegments(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(PURGE_SEGMENTS_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(PURGE_SEGMENTS_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(PURGE_SEGMENTS_REQUEST_DECODER.recordingId())
            .append(" newStartPosition=").append(PURGE_SEGMENTS_REQUEST_DECODER.newStartPosition());
    }

    private static void renderAttachSegments(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(ATTACH_SEGMENTS_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(ATTACH_SEGMENTS_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(ATTACH_SEGMENTS_REQUEST_DECODER.recordingId());
    }

    private static void renderMigrateSegments(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(MIGRATE_SEGMENTS_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(MIGRATE_SEGMENTS_REQUEST_DECODER.correlationId())
            .append(" srcRecordingId=").append(MIGRATE_SEGMENTS_REQUEST_DECODER.srcRecordingId())
            .append(" dstRecordingId=").append(MIGRATE_SEGMENTS_REQUEST_DECODER.dstRecordingId());
    }

    private static void renderKeepAlive(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(KEEP_ALIVE_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(KEEP_ALIVE_REQUEST_DECODER.correlationId());
    }

    private static void renderTaggedReplicate(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(TAGGED_REPLICATE_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(TAGGED_REPLICATE_REQUEST_DECODER.correlationId())
            .append(" srcRecordingId=").append(TAGGED_REPLICATE_REQUEST_DECODER.srcRecordingId())
            .append(" dstRecordingId=").append(TAGGED_REPLICATE_REQUEST_DECODER.dstRecordingId())
            .append(" channelTagId=").append(TAGGED_REPLICATE_REQUEST_DECODER.channelTagId())
            .append(" subscriptionTagId=").append(TAGGED_REPLICATE_REQUEST_DECODER.subscriptionTagId())
            .append(" srcControlStreamId=").append(TAGGED_REPLICATE_REQUEST_DECODER.srcControlStreamId())
            .append(" srcControlChannel=");

        TAGGED_REPLICATE_REQUEST_DECODER.getSrcControlChannel(sb);

        sb.append(" liveDestination=");
        TAGGED_REPLICATE_REQUEST_DECODER.getLiveDestination(sb);
    }

    private static void renderPurgeRecording(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(PURGE_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(PURGE_RECORDING_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(PURGE_RECORDING_REQUEST_DECODER.recordingId());
    }

    private static void renderReplayToken(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(REPLAY_TOKEN_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(REPLAY_TOKEN_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(REPLAY_TOKEN_REQUEST_DECODER.recordingId());
    }

    private static void renderControlResponse(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(CONTROL_RESPONSE_DECODER.controlSessionId())
            .append(" correlationId=").append(CONTROL_RESPONSE_DECODER.correlationId())
            .append(" relevantId=").append(CONTROL_RESPONSE_DECODER.relevantId())
            .append(" code=").append(CONTROL_RESPONSE_DECODER.code())
            .append(" version=").append(CONTROL_RESPONSE_DECODER.version())
            .append(" errorMessage=");

        CONTROL_RESPONSE_DECODER.getErrorMessage(sb);
    }

    private static void renderRecordingSignal(final StringBuilder sb)
    {
        sb.append("controlSessionId=").append(RECORDING_SIGNAL_EVENT_DECODER.controlSessionId())
            .append(" correlationId=").append(RECORDING_SIGNAL_EVENT_DECODER.correlationId())
            .append(" recordingId=").append(RECORDING_SIGNAL_EVENT_DECODER.recordingId())
            .append(" subscriptionId=").append(RECORDING_SIGNAL_EVENT_DECODER.subscriptionId())
            .append(" position=").append(RECORDING_SIGNAL_EVENT_DECODER.position())
            .append(" signal=").append(RECORDING_SIGNAL_EVENT_DECODER.signal());
    }
}
