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
package io.aeron.agent;

import io.aeron.archive.codecs.*;
import org.agrona.MutableDirectBuffer;

import static io.aeron.agent.ArchiveEventCode.*;
import static io.aeron.agent.CommonEventDissector.dissectLogHeader;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_BYTE;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

final class ArchiveEventDissector
{
    static final String CONTEXT = "ARCHIVE";

    private static final MessageHeaderDecoder HEADER_DECODER = new MessageHeaderDecoder();
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
    private static final ControlResponseDecoder CONTROL_RESPONSE_DECODER = new ControlResponseDecoder();
    private static final RecordingSignalEventDecoder RECORDING_SIGNAL_EVENT_DECODER = new RecordingSignalEventDecoder();
    private static final ReplayTokenRequestDecoder REPLAY_TOKEN_REQUEST_DECODER = new ReplayTokenRequestDecoder();
    private static final MaxRecordedPositionRequestDecoder MAX_RECORDED_POSITION_REQUEST_DECODER =
        new MaxRecordedPositionRequestDecoder();

    private ArchiveEventDissector()
    {
    }

    @SuppressWarnings("MethodLength")
    static void dissectControlRequest(
        final ArchiveEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int encodedLength = dissectLogHeader(CONTEXT, eventCode, buffer, offset, builder);

        HEADER_DECODER.wrap(buffer, offset + encodedLength);
        encodedLength += MessageHeaderDecoder.ENCODED_LENGTH;

        switch (eventCode)
        {
            case CMD_IN_CONNECT:
                CONNECT_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendConnect(builder);
                break;

            case CMD_IN_CLOSE_SESSION:
                CLOSE_SESSION_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendCloseSession(builder);
                break;

            case CMD_IN_START_RECORDING:
                START_RECORDING_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendStartRecording(builder);
                break;

            case CMD_IN_STOP_RECORDING:
                STOP_RECORDING_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendStopRecording(builder);
                break;

            case CMD_IN_REPLAY:
                REPLAY_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendReplay(builder);
                break;

            case CMD_IN_STOP_REPLAY:
                STOP_REPLAY_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendStopReplay(builder);
                break;

            case CMD_IN_LIST_RECORDINGS:
                LIST_RECORDINGS_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendListRecordings(builder);
                break;

            case CMD_IN_LIST_RECORDINGS_FOR_URI:
                LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendListRecordingsForUri(builder);
                break;

            case CMD_IN_LIST_RECORDING:
                LIST_RECORDING_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendListRecording(builder);
                break;

            case CMD_IN_EXTEND_RECORDING:
                EXTEND_RECORDING_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendExtendRecording(builder);
                break;

            case CMD_IN_RECORDING_POSITION:
                RECORDING_POSITION_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendRecordingPosition(builder);
                break;

            case CMD_IN_TRUNCATE_RECORDING:
                TRUNCATE_RECORDING_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendTruncateRecording(builder);
                break;

            case CMD_IN_STOP_RECORDING_SUBSCRIPTION:
                STOP_RECORDING_SUBSCRIPTION_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendStopRecordingSubscription(builder);
                break;

            case CMD_IN_STOP_POSITION:
                STOP_POSITION_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendStopPosition(builder);
                break;

            case CMD_IN_FIND_LAST_MATCHING_RECORD:
                FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendFindLastMatchingRecord(builder);
                break;

            case CMD_IN_LIST_RECORDING_SUBSCRIPTIONS:
                LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendListRecordingSubscriptions(builder);
                break;

            case CMD_IN_START_BOUNDED_REPLAY:
                BOUNDED_REPLAY_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendStartBoundedReplay(builder);
                break;

            case CMD_IN_STOP_ALL_REPLAYS:
                STOP_ALL_REPLAYS_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendStopAllReplays(builder);
                break;

            case CMD_IN_REPLICATE:
                REPLICATE_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendReplicate(builder);
                break;

            case CMD_IN_STOP_REPLICATION:
                STOP_REPLICATION_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendStopReplication(builder);
                break;

            case CMD_IN_START_POSITION:
                START_POSITION_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendStartPosition(builder);
                break;

            case CMD_IN_DETACH_SEGMENTS:
                DETACH_SEGMENTS_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendDetachSegments(builder);
                break;

            case CMD_IN_DELETE_DETACHED_SEGMENTS:
                DELETE_DETACHED_SEGMENTS_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendDeleteDetachedSegments(builder);
                break;

            case CMD_IN_PURGE_SEGMENTS:
                PURGE_SEGMENTS_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendPurgeSegments(builder);
                break;

            case CMD_IN_ATTACH_SEGMENTS:
                ATTACH_SEGMENTS_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendAttachSegments(builder);
                break;

            case CMD_IN_MIGRATE_SEGMENTS:
                MIGRATE_SEGMENTS_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendMigrateSegments(builder);
                break;

            case CMD_IN_AUTH_CONNECT:
                AUTH_CONNECT_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendAuthConnect(builder);
                break;

            case CMD_IN_KEEP_ALIVE:
                KEEP_ALIVE_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendKeepAlive(builder);
                break;

            case CMD_IN_TAGGED_REPLICATE:
                TAGGED_REPLICATE_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendTaggedReplicate(builder);
                break;

            case CMD_IN_START_RECORDING2:
                START_RECORDING_REQUEST2_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendStartRecording2(builder);
                break;

            case CMD_IN_EXTEND_RECORDING2:
                EXTEND_RECORDING_REQUEST2_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendExtendRecording2(builder);
                break;

            case CMD_IN_STOP_RECORDING_BY_IDENTITY:
                STOP_RECORDING_BY_IDENTITY_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendStopRecordingByIdentity(builder);
                break;

            case CMD_IN_PURGE_RECORDING:
                PURGE_RECORDING_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendPurgeRecording(builder);
                break;

            case CMD_IN_REPLICATE2:
                REPLICATE_REQUEST2_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendReplicate2(builder);
                break;

            case CMD_IN_REQUEST_REPLAY_TOKEN:
                REPLAY_TOKEN_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendReplayToken(builder);
                break;

            case CMD_IN_MAX_RECORDED_POSITION:
                MAX_RECORDED_POSITION_REQUEST_DECODER.wrap(
                    buffer,
                    offset + encodedLength,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendMaxRecordedPosition(builder);
                break;

            default:
                builder.append(": unknown command");
        }
    }

    static void dissectControlResponse(final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int encodedLength = dissectLogHeader(CONTEXT, CMD_OUT_RESPONSE, buffer, offset, builder);

        HEADER_DECODER.wrap(buffer, offset + encodedLength);
        encodedLength += MessageHeaderDecoder.ENCODED_LENGTH;

        CONTROL_RESPONSE_DECODER.wrap(
            buffer,
            offset + encodedLength,
            HEADER_DECODER.blockLength(),
            HEADER_DECODER.version());

        builder.append(": controlSessionId=").append(CONTROL_RESPONSE_DECODER.controlSessionId())
            .append(" correlationId=").append(CONTROL_RESPONSE_DECODER.correlationId())
            .append(" relevantId=").append(CONTROL_RESPONSE_DECODER.relevantId())
            .append(" code=").append(CONTROL_RESPONSE_DECODER.code())
            .append(" version=").append(CONTROL_RESPONSE_DECODER.version())
            .append(" errorMessage=");

        CONTROL_RESPONSE_DECODER.getErrorMessage(builder);
    }

    static void dissectRecordingSignal(final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int encodedLength = dissectLogHeader(CONTEXT, RECORDING_SIGNAL, buffer, offset, builder);

        HEADER_DECODER.wrap(buffer, offset + encodedLength);
        encodedLength += MessageHeaderDecoder.ENCODED_LENGTH;

        RECORDING_SIGNAL_EVENT_DECODER.wrap(
            buffer,
            offset + encodedLength,
            HEADER_DECODER.blockLength(),
            HEADER_DECODER.version());

        builder.append(": controlSessionId=").append(RECORDING_SIGNAL_EVENT_DECODER.controlSessionId())
            .append(" correlationId=").append(RECORDING_SIGNAL_EVENT_DECODER.correlationId())
            .append(" recordingId=").append(RECORDING_SIGNAL_EVENT_DECODER.recordingId())
            .append(" subscriptionId=").append(RECORDING_SIGNAL_EVENT_DECODER.subscriptionId())
            .append(" position=").append(RECORDING_SIGNAL_EVENT_DECODER.position())
            .append(" signal=").append(RECORDING_SIGNAL_EVENT_DECODER.signal());
    }

    static void dissectReplicationSessionDone(
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, REPLICATION_SESSION_DONE, buffer, offset, builder);

        final long controlSessionId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long replicationId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long srcRecordingId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long replayPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long srcStopPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long dstRecordingId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long dstStopPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long position = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final boolean isClosed = 1 == buffer.getByte(absoluteOffset);
        absoluteOffset += SIZE_OF_BYTE;
        final boolean isEndOfStream = 1 == buffer.getByte(absoluteOffset);
        absoluteOffset += SIZE_OF_BYTE;
        final boolean isSynced = 1 == buffer.getByte(absoluteOffset);
        absoluteOffset += SIZE_OF_BYTE;

        builder
            .append(": controlSessionId=").append(controlSessionId)
            .append(" replicationId=").append(replicationId)
            .append(" srcRecordingId=").append(srcRecordingId)
            .append(" replayPosition=").append(replayPosition)
            .append(" srcStopPosition=").append(srcStopPosition)
            .append(" dstRecordingId=").append(dstRecordingId)
            .append(" dstStopPosition=").append(dstStopPosition)
            .append(" position=").append(position)
            .append(" isClosed=").append(isClosed)
            .append(" isEndOfStream=").append(isEndOfStream)
            .append(" isSynced=").append(isSynced);
    }

    static void dissectReplaySessionStateChange(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, REPLAY_SESSION_STATE_CHANGE, buffer, absoluteOffset, builder);

        final long replaySessionId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long recordingId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long position = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        builder.append(": replaySessionId=").append(replaySessionId);
        builder.append(" replayId=").append(replaySessionId >> 32);
        builder.append(" sessionId=").append((int)replaySessionId);
        builder.append(" recordingId=").append(recordingId);
        builder.append(" position=").append(position);

        builder.append(" ");
        absoluteOffset += buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(" reason=\"");
        buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
        builder.append("\"");
    }

    static void dissectRecordingSessionStateChange(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, RECORDING_SESSION_STATE_CHANGE, buffer, absoluteOffset, builder);

        final long recordingId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long position = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        builder.append(": recordingId=").append(recordingId);
        builder.append(" position=").append(position);

        builder.append(" ");
        absoluteOffset += buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(" reason=\"");
        buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
        builder.append("\"");
    }

    static void dissectReplicationSessionStateChange(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, REPLICATION_SESSION_STATE_CHANGE, buffer, absoluteOffset, builder);

        final long replicationId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long srcRecordingId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long dstRecordingId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long position = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        builder.append(": replicationId=").append(replicationId);
        builder.append(" srcRecordingId=").append(srcRecordingId);
        builder.append(" dstRecordingId=").append(dstRecordingId);
        builder.append(" position=").append(position);

        builder.append(" ");
        absoluteOffset += buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(" reason=\"");
        buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
        builder.append("\"");
    }

    static void dissectControlSessionStateChange(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, CONTROL_SESSION_STATE_CHANGE, buffer, absoluteOffset, builder);

        final long controlSessionId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        builder.append(": controlSessionId=").append(controlSessionId);
        builder.append(" ");
        absoluteOffset += buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(" reason=\"");
        buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
        builder.append("\"");
    }

    static void dissectReplaySessionError(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, REPLAY_SESSION_ERROR, buffer, absoluteOffset, builder);

        final long sessionId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long recordingId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        builder.append(": sessionId=").append(sessionId);
        builder.append(" recordingId=").append(recordingId);
        builder.append(" errorMessage=");
        buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
    }

    static void dissectCatalogResize(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, CATALOG_RESIZE, buffer, absoluteOffset, builder);

        final long catalogLength = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long newCatalogLength = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);

        builder.append(": ").append(catalogLength).append(" bytes => ").append(newCatalogLength).append(" bytes");
    }

    private static void appendConnect(final StringBuilder builder)
    {
        builder.append(": correlationId=").append(CONNECT_REQUEST_DECODER.correlationId())
            .append(" responseStreamId=").append(CONNECT_REQUEST_DECODER.responseStreamId())
            .append(" version=").append(CONNECT_REQUEST_DECODER.version())
            .append(" responseChannel=");

        CONNECT_REQUEST_DECODER.getResponseChannel(builder);
    }

    private static void appendAuthConnect(final StringBuilder builder)
    {
        builder.append(": correlationId=").append(AUTH_CONNECT_REQUEST_DECODER.correlationId())
            .append(" responseStreamId=").append(AUTH_CONNECT_REQUEST_DECODER.responseStreamId())
            .append(" version=").append(AUTH_CONNECT_REQUEST_DECODER.version())
            .append(" responseChannel=");

        AUTH_CONNECT_REQUEST_DECODER.getResponseChannel(builder);

        builder.append(" encodedCredentialsLength=").append(AUTH_CONNECT_REQUEST_DECODER.encodedCredentialsLength());
    }

    private static void appendCloseSession(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(CLOSE_SESSION_REQUEST_DECODER.controlSessionId());
    }

    private static void appendStartRecording(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(START_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(START_RECORDING_REQUEST_DECODER.correlationId())
            .append(" streamId=").append(START_RECORDING_REQUEST_DECODER.streamId())
            .append(" sourceLocation=").append(START_RECORDING_REQUEST_DECODER.sourceLocation())
            .append(" channel=");

        START_RECORDING_REQUEST_DECODER.getChannel(builder);
    }

    private static void appendStartRecording2(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(START_RECORDING_REQUEST2_DECODER.controlSessionId())
            .append(" correlationId=").append(START_RECORDING_REQUEST2_DECODER.correlationId())
            .append(" streamId=").append(START_RECORDING_REQUEST2_DECODER.streamId())
            .append(" sourceLocation=").append(START_RECORDING_REQUEST2_DECODER.sourceLocation())
            .append(" autoStop=").append(START_RECORDING_REQUEST2_DECODER.autoStop())
            .append(" channel=");

        START_RECORDING_REQUEST2_DECODER.getChannel(builder);
    }

    private static void appendStopRecording(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(STOP_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(STOP_RECORDING_REQUEST_DECODER.correlationId())
            .append(" streamId=").append(STOP_RECORDING_REQUEST_DECODER.streamId())
            .append(" channel=");

        STOP_RECORDING_REQUEST_DECODER.getChannel(builder);
    }

    private static void appendReplay(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(REPLAY_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(REPLAY_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(REPLAY_REQUEST_DECODER.recordingId())
            .append(" position=").append(REPLAY_REQUEST_DECODER.position())
            .append(" length=").append(REPLAY_REQUEST_DECODER.length())
            .append(" replayStreamId=").append(REPLAY_REQUEST_DECODER.replayStreamId())
            .append(" replayChannel=");

        REPLAY_REQUEST_DECODER.getReplayChannel(builder);
    }

    private static void appendStopReplay(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(STOP_REPLAY_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(STOP_REPLAY_REQUEST_DECODER.correlationId())
            .append(" replaySessionId=").append(STOP_REPLAY_REQUEST_DECODER.replaySessionId());
    }

    private static void appendListRecordings(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(LIST_RECORDINGS_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(LIST_RECORDINGS_REQUEST_DECODER.correlationId())
            .append(" fromRecordingId=").append(LIST_RECORDINGS_REQUEST_DECODER.fromRecordingId())
            .append(" recordCount=").append(LIST_RECORDINGS_REQUEST_DECODER.recordCount());
    }

    private static void appendListRecording(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(LIST_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(LIST_RECORDING_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(LIST_RECORDING_REQUEST_DECODER.recordingId());
    }

    private static void appendListRecordingsForUri(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.correlationId())
            .append(" fromRecordingId=").append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.fromRecordingId())
            .append(" recordCount=").append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.recordCount())
            .append(" streamId=").append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.streamId())
            .append(" channel=");

        LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.getChannel(builder);
    }

    private static void appendExtendRecording(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(EXTEND_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(EXTEND_RECORDING_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(EXTEND_RECORDING_REQUEST_DECODER.recordingId())
            .append(" streamId=").append(EXTEND_RECORDING_REQUEST_DECODER.streamId())
            .append(" sourceLocation=").append(EXTEND_RECORDING_REQUEST_DECODER.sourceLocation())
            .append(" channel=");

        EXTEND_RECORDING_REQUEST_DECODER.getChannel(builder);
    }

    private static void appendExtendRecording2(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(EXTEND_RECORDING_REQUEST2_DECODER.controlSessionId())
            .append(" correlationId=").append(EXTEND_RECORDING_REQUEST2_DECODER.correlationId())
            .append(" recordingId=").append(EXTEND_RECORDING_REQUEST2_DECODER.recordingId())
            .append(" streamId=").append(EXTEND_RECORDING_REQUEST2_DECODER.streamId())
            .append(" sourceLocation=").append(EXTEND_RECORDING_REQUEST2_DECODER.sourceLocation())
            .append(" autoStop=").append(EXTEND_RECORDING_REQUEST2_DECODER.autoStop())
            .append(" channel=");

        EXTEND_RECORDING_REQUEST2_DECODER.getChannel(builder);
    }

    private static void appendRecordingPosition(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(RECORDING_POSITION_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(RECORDING_POSITION_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(RECORDING_POSITION_REQUEST_DECODER.recordingId());
    }

    private static void appendMaxRecordedPosition(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(MAX_RECORDED_POSITION_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(MAX_RECORDED_POSITION_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(MAX_RECORDED_POSITION_REQUEST_DECODER.recordingId());
    }

    private static void appendTruncateRecording(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(TRUNCATE_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(TRUNCATE_RECORDING_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(TRUNCATE_RECORDING_REQUEST_DECODER.recordingId())
            .append(" position=").append(TRUNCATE_RECORDING_REQUEST_DECODER.position());
    }

    private static void appendStopRecordingSubscription(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(STOP_RECORDING_SUBSCRIPTION_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(STOP_RECORDING_SUBSCRIPTION_REQUEST_DECODER.correlationId())
            .append(" subscriptionId=").append(STOP_RECORDING_SUBSCRIPTION_REQUEST_DECODER.subscriptionId());
    }

    private static void appendStopRecordingByIdentity(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(STOP_RECORDING_BY_IDENTITY_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(STOP_RECORDING_BY_IDENTITY_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(STOP_RECORDING_BY_IDENTITY_REQUEST_DECODER.recordingId());
    }

    private static void appendStopPosition(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(STOP_POSITION_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(STOP_POSITION_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(STOP_POSITION_REQUEST_DECODER.recordingId());
    }

    private static void appendFindLastMatchingRecord(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.correlationId())
            .append(" minRecordingId=").append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.minRecordingId())
            .append(" sessionId=").append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.sessionId())
            .append(" streamId=").append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.streamId())
            .append(" channel=");

        FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.getChannel(builder);
    }

    private static void appendListRecordingSubscriptions(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.correlationId())
            .append(" pseudoIndex=").append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.pseudoIndex())
            .append(" applyStreamId=").append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.applyStreamId())
            .append(" subscriptionCount=").append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.subscriptionCount())
            .append(" streamId=").append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.streamId())
            .append(" channel=");

        LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.getChannel(builder);
    }

    private static void appendStartBoundedReplay(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(BOUNDED_REPLAY_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(BOUNDED_REPLAY_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(BOUNDED_REPLAY_REQUEST_DECODER.recordingId())
            .append(" position=").append(BOUNDED_REPLAY_REQUEST_DECODER.position())
            .append(" length=").append(BOUNDED_REPLAY_REQUEST_DECODER.length())
            .append(" limitCounterId=").append(BOUNDED_REPLAY_REQUEST_DECODER.limitCounterId())
            .append(" replayStreamId=").append(BOUNDED_REPLAY_REQUEST_DECODER.replayStreamId())
            .append(" replayChannel=");

        BOUNDED_REPLAY_REQUEST_DECODER.getReplayChannel(builder);
    }

    private static void appendStopAllReplays(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(STOP_ALL_REPLAYS_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(STOP_ALL_REPLAYS_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(STOP_ALL_REPLAYS_REQUEST_DECODER.recordingId());
    }

    private static void appendReplicate(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(REPLICATE_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(REPLICATE_REQUEST_DECODER.correlationId())
            .append(" srcRecordingId=").append(REPLICATE_REQUEST_DECODER.srcRecordingId())
            .append(" dstRecordingId=").append(REPLICATE_REQUEST_DECODER.dstRecordingId())
            .append(" srcControlStreamId=").append(REPLICATE_REQUEST_DECODER.srcControlStreamId())
            .append(" srcControlChannel=");

        REPLICATE_REQUEST_DECODER.getSrcControlChannel(builder);

        builder.append(" liveDestination=");
        REPLICATE_REQUEST_DECODER.getLiveDestination(builder);
    }

    private static void appendReplicate2(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(REPLICATE_REQUEST2_DECODER.controlSessionId())
            .append(" correlationId=").append(REPLICATE_REQUEST2_DECODER.correlationId())
            .append(" srcRecordingId=").append(REPLICATE_REQUEST2_DECODER.srcRecordingId())
            .append(" dstRecordingId=").append(REPLICATE_REQUEST2_DECODER.dstRecordingId())
            .append(" stopPosition=").append(REPLICATE_REQUEST2_DECODER.stopPosition())
            .append(" channelTagId=").append(REPLICATE_REQUEST2_DECODER.channelTagId())
            .append(" subscriptionTagId=").append(REPLICATE_REQUEST2_DECODER.subscriptionTagId())
            .append(" srcControlStreamId=").append(REPLICATE_REQUEST2_DECODER.srcControlStreamId())
            .append(" srcControlChannel=");

        REPLICATE_REQUEST2_DECODER.getSrcControlChannel(builder);

        builder.append(" liveDestination=");
        REPLICATE_REQUEST2_DECODER.getLiveDestination(builder);

        builder.append(" replicationChannel=");
        REPLICATE_REQUEST2_DECODER.getReplicationChannel(builder);
    }

    private static void appendStopReplication(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(STOP_REPLICATION_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(STOP_REPLICATION_REQUEST_DECODER.correlationId())
            .append(" replicationId=").append(STOP_REPLICATION_REQUEST_DECODER.replicationId());
    }

    private static void appendStartPosition(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(START_POSITION_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(START_POSITION_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(START_POSITION_REQUEST_DECODER.recordingId());
    }

    private static void appendDetachSegments(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(DETACH_SEGMENTS_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(DETACH_SEGMENTS_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(DETACH_SEGMENTS_REQUEST_DECODER.recordingId());
    }

    private static void appendDeleteDetachedSegments(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(DELETE_DETACHED_SEGMENTS_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(DELETE_DETACHED_SEGMENTS_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(DELETE_DETACHED_SEGMENTS_REQUEST_DECODER.recordingId());
    }

    private static void appendPurgeSegments(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(PURGE_SEGMENTS_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(PURGE_SEGMENTS_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(PURGE_SEGMENTS_REQUEST_DECODER.recordingId())
            .append(" newStartPosition=").append(PURGE_SEGMENTS_REQUEST_DECODER.newStartPosition());
    }

    private static void appendAttachSegments(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(ATTACH_SEGMENTS_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(ATTACH_SEGMENTS_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(ATTACH_SEGMENTS_REQUEST_DECODER.recordingId());
    }

    private static void appendMigrateSegments(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(MIGRATE_SEGMENTS_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(MIGRATE_SEGMENTS_REQUEST_DECODER.correlationId())
            .append(" srcRecordingId=").append(MIGRATE_SEGMENTS_REQUEST_DECODER.srcRecordingId())
            .append(" dstRecordingId=").append(MIGRATE_SEGMENTS_REQUEST_DECODER.dstRecordingId());
    }

    private static void appendKeepAlive(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(KEEP_ALIVE_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(KEEP_ALIVE_REQUEST_DECODER.correlationId());
    }

    private static void appendTaggedReplicate(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(TAGGED_REPLICATE_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(TAGGED_REPLICATE_REQUEST_DECODER.correlationId())
            .append(" srcRecordingId=").append(TAGGED_REPLICATE_REQUEST_DECODER.srcRecordingId())
            .append(" dstRecordingId=").append(TAGGED_REPLICATE_REQUEST_DECODER.dstRecordingId())
            .append(" channelTagId=").append(TAGGED_REPLICATE_REQUEST_DECODER.channelTagId())
            .append(" subscriptionTagId=").append(TAGGED_REPLICATE_REQUEST_DECODER.subscriptionTagId())
            .append(" srcControlStreamId=").append(TAGGED_REPLICATE_REQUEST_DECODER.srcControlStreamId())
            .append(" srcControlChannel=");

        TAGGED_REPLICATE_REQUEST_DECODER.getSrcControlChannel(builder);

        builder.append(" liveDestination=");
        TAGGED_REPLICATE_REQUEST_DECODER.getLiveDestination(builder);
    }

    private static void appendPurgeRecording(final StringBuilder builder)
    {
        builder.append(": controlSessionId=").append(PURGE_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(PURGE_RECORDING_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(PURGE_RECORDING_REQUEST_DECODER.recordingId());
    }

    private static void appendReplayToken(final StringBuilder builder)
    {
        builder
            .append(": controlSessionId=").append(REPLAY_TOKEN_REQUEST_DECODER.controlSessionId())
            .append(" correlationId=").append(REPLAY_TOKEN_REQUEST_DECODER.correlationId())
            .append(" recordingId=").append(REPLAY_TOKEN_REQUEST_DECODER.recordingId());
    }
}
