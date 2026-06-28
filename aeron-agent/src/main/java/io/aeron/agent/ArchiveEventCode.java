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

import io.aeron.archive.client.PersistentSubscription;
import io.aeron.archive.codecs.*;
import org.agrona.MutableDirectBuffer;

import java.util.Arrays;
import java.util.function.ToIntFunction;

import static io.aeron.agent.ArchiveEventDissector.*;

/**
 * Events that can be enabled for logging in the archive module.
 */
public enum ArchiveEventCode implements EventCode
{
    /**
     * Archive logging event for {@code connect} command.
     */
    CMD_IN_CONNECT(1, ConnectRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code close-session} command.
     */
    CMD_IN_CLOSE_SESSION(2, CloseSessionRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code start-recording} command.
     */
    CMD_IN_START_RECORDING(3, StartRecordingRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code stop-recording} command.
     */
    CMD_IN_STOP_RECORDING(4, StopRecordingRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code replay} command.
     */
    CMD_IN_REPLAY(5, ReplayRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code stop-replay} command.
     */
    CMD_IN_STOP_REPLAY(6, StopReplayRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code list-recordings} command.
     */
    CMD_IN_LIST_RECORDINGS(7, ListRecordingsRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code list-recordings-by-uri} command.
     */
    CMD_IN_LIST_RECORDINGS_FOR_URI(8, ListRecordingsForUriRequestDecoder.TEMPLATE_ID,
        ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code list-recording} command.
     */
    CMD_IN_LIST_RECORDING(9, ListRecordingRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code extend-recording} command.
     */
    CMD_IN_EXTEND_RECORDING(10, ExtendRecordingRequestDecoder.TEMPLATE_ID,
        ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code recording-position} command.
     */
    CMD_IN_RECORDING_POSITION(11, RecordingPositionRequestDecoder.TEMPLATE_ID,
        ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code truncate-recording} command.
     */
    CMD_IN_TRUNCATE_RECORDING(12, TruncateRecordingRequestDecoder.TEMPLATE_ID,
        ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code stop-recording-subscription} command.
     */
    CMD_IN_STOP_RECORDING_SUBSCRIPTION(13, StopRecordingSubscriptionRequestDecoder.TEMPLATE_ID,
        ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code stop-position} command.
     */
    CMD_IN_STOP_POSITION(14, StopPositionRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code find-last-matching-recording} command.
     */
    CMD_IN_FIND_LAST_MATCHING_RECORD(15, FindLastMatchingRecordingRequestDecoder.TEMPLATE_ID,
        ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code list-recording-subscriptions} command.
     */
    CMD_IN_LIST_RECORDING_SUBSCRIPTIONS(16, ListRecordingSubscriptionsRequestDecoder.TEMPLATE_ID,
        ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code start-bounded-replay} command.
     */
    CMD_IN_START_BOUNDED_REPLAY(17, BoundedReplayRequestDecoder.TEMPLATE_ID,
        ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code stop-all-replays} command.
     */
    CMD_IN_STOP_ALL_REPLAYS(18, StopAllReplaysRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code replicate} command.
     */
    CMD_IN_REPLICATE(19, ReplicateRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code stop-replication} command.
     */
    CMD_IN_STOP_REPLICATION(20, StopReplicationRequestDecoder.TEMPLATE_ID,
        ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code start-position} command.
     */
    CMD_IN_START_POSITION(21, StartPositionRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code detach-segments} command.
     */
    CMD_IN_DETACH_SEGMENTS(22, DetachSegmentsRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code delete-detached-segments} command.
     */
    CMD_IN_DELETE_DETACHED_SEGMENTS(23, DeleteDetachedSegmentsRequestDecoder.TEMPLATE_ID,
        ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code purge-segments} command.
     */
    CMD_IN_PURGE_SEGMENTS(24, PurgeSegmentsRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code attach-segments} command.
     */
    CMD_IN_ATTACH_SEGMENTS(25, AttachSegmentsRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code migrate-segments} command.
     */
    CMD_IN_MIGRATE_SEGMENTS(26, MigrateSegmentsRequestDecoder.TEMPLATE_ID,
        ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code auth-connect} command.
     */
    CMD_IN_AUTH_CONNECT(27, AuthConnectRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code keep-alive} command.
     */
    CMD_IN_KEEP_ALIVE(28, KeepAliveRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code tagged-replicate} command.
     */
    CMD_IN_TAGGED_REPLICATE(29, TaggedReplicateRequestDecoder.TEMPLATE_ID,
        ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for Archive response.
     */
    CMD_OUT_RESPONSE(30, ControlResponseDecoder.TEMPLATE_ID,
        (event, buffer, offset, builder) -> dissectControlResponse(buffer, offset, builder)),
    /**
     * Archive logging event for {@code start-recording2} command.
     */
    CMD_IN_START_RECORDING2(31, StartRecordingRequest2Decoder.TEMPLATE_ID,
        ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code extend-recording2} command.
     */
    CMD_IN_EXTEND_RECORDING2(32, ExtendRecordingRequest2Decoder.TEMPLATE_ID,
        ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code stop-recording-by-identity} command.
     */
    CMD_IN_STOP_RECORDING_BY_IDENTITY(33, StopRecordingByIdentityRequestDecoder.TEMPLATE_ID,
        ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for replication state change.
     */
    REPLICATION_SESSION_STATE_CHANGE(34, -1,
        (event, buffer, offset, builder) -> dissectReplicationSessionStateChange(buffer, offset, builder)),
    /**
     * Archive logging event for control session state change.
     */
    CONTROL_SESSION_STATE_CHANGE(35, -1,
        (event, buffer, offset, builder) -> dissectControlSessionStateChange(buffer, offset, builder)),
    /**
     * Archive logging event for replay session error.
     */
    REPLAY_SESSION_ERROR(36, -1,
        (event, buffer, offset, builder) -> dissectReplaySessionError(buffer, offset, builder)),
    /**
     * Archive logging event for Catalog resize.
     */
    CATALOG_RESIZE(37, -1,
        (event, buffer, offset, builder) -> dissectCatalogResize(buffer, offset, builder)),
    /**
     * Archive logging event for {@code purge-recording} command.
     */
    CMD_IN_PURGE_RECORDING(38, PurgeRecordingRequestDecoder.TEMPLATE_ID,
        ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@code replicate2} command.
     */
    CMD_IN_REPLICATE2(39, ReplicateRequest2Decoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for recording signal.
     */
    RECORDING_SIGNAL(40, RecordingSignalEventDecoder.TEMPLATE_ID,
        (event, buffer, offset, builder) -> dissectRecordingSignal(buffer, offset, builder)),
    /**
     * Archive logging event for replication session done.
     */
    REPLICATION_SESSION_DONE(
        41, -1, (event, buffer, offset, builder) -> dissectReplicationSessionDone(buffer, offset, builder)),
    /**
     * Archive logging event for {@code request-replay-token}.
     */
    CMD_IN_REQUEST_REPLAY_TOKEN(
        42, ReplayTokenRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for replay state change.
     */
    REPLAY_SESSION_STATE_CHANGE(43, -1,
        (event, buffer, offset, builder) -> dissectReplaySessionStateChange(buffer, offset, builder)),
    /**
     * Archive logging event for recording state change.
     */
    RECORDING_SESSION_STATE_CHANGE(44, -1,
        (event, buffer, offset, builder) -> dissectRecordingSessionStateChange(buffer, offset, builder)),
    /**
     * Archive logging event for {@code max-recorded-position} command.
     */
    CMD_IN_MAX_RECORDED_POSITION(
        45, MaxRecordedPositionRequestDecoder.TEMPLATE_ID, ArchiveEventDissector::dissectControlRequest),
    /**
     * Archive logging event for {@link PersistentSubscription} state change.
     */
    PERSISTENT_SUBSCRIPTION_STATE_CHANGE(46, -1, ArchiveEventDissector::dissectPersistentSubscriptionStateChange),
    /**
     * Archive logging event for {@link PersistentSubscription} joining live.
     */
    PERSISTENT_SUBSCRIPTION_JOINED_LIVE(47, -1, ArchiveEventDissector::dissectPersistentSubscriptionJoinedLive),
    /**
     * Archive logging event for {@link PersistentSubscription} leaving live.
     */
    PERSISTENT_SUBSCRIPTION_LEFT_LIVE(48, -1,
        ArchiveEventDissector::dissectPersistentSubscriptionLeftLive);

    static final int EVENT_CODE_TYPE = EventCodeType.ARCHIVE.getTypeCode();
    private static final ArchiveEventCode[] EVENT_CODE_BY_ID;
    private static final ArchiveEventCode[] EVENT_CODE_BY_TEMPLATE_ID;

    private final int id;
    private final int templateId;
    private final DissectFunction<ArchiveEventCode> dissector;

    static
    {
        final ArchiveEventCode[] codes = ArchiveEventCode.values();
        EVENT_CODE_BY_ID = createLookupArray(codes, ArchiveEventCode::id);
        EVENT_CODE_BY_TEMPLATE_ID = createLookupArray(codes, ArchiveEventCode::templateId);
    }

    private static ArchiveEventCode[] createLookupArray(
        final ArchiveEventCode[] codes, final ToIntFunction<ArchiveEventCode> idSupplier)
    {
        final int maxId = Arrays.stream(codes).mapToInt(idSupplier).max().orElse(0);
        if (maxId > 100_000)
        {
            throw new IllegalStateException("length of the lookup array exceeds 100000: " + maxId);
        }
        final ArchiveEventCode[] array = new ArchiveEventCode[maxId + 1];

        for (final ArchiveEventCode code : codes)
        {
            final int id = idSupplier.applyAsInt(code);
            if (id >= 0)
            {
                if (null != array[id])
                {
                    throw new IllegalArgumentException("id already in use: " + id);
                }

                array[id] = code;
            }
        }

        return array;
    }

    ArchiveEventCode(final int id, final int templateId, final DissectFunction<ArchiveEventCode> dissector)
    {
        this.id = id;
        this.templateId = templateId;
        this.dissector = dissector;
    }

    static ArchiveEventCode get(final int id)
    {
        if (id < 0 || id >= EVENT_CODE_BY_ID.length)
        {
            throw new IllegalArgumentException("no ArchiveEventCode for id: " + id);
        }

        final ArchiveEventCode code = EVENT_CODE_BY_ID[id];
        if (null == code)
        {
            throw new IllegalArgumentException("no ArchiveEventCode for id: " + id);
        }

        return code;
    }

    static ArchiveEventCode getByTemplateId(final int templateId)
    {
        return templateId >= 0 && templateId < EVENT_CODE_BY_TEMPLATE_ID.length ?
            EVENT_CODE_BY_TEMPLATE_ID[templateId] : null;
    }

    /**
     * {@inheritDoc}
     */
    public int id()
    {
        return id;
    }

    /**
     * Template ID of the SBE message.
     *
     * @return template ID of the SBE message.
     */
    public int templateId()
    {
        return templateId;
    }

    /**
     * Get {@link ArchiveEventCode#id()} from {@link #id()}.
     *
     * @return get {@link ArchiveEventCode#id()} from {@link #id()}.
     */
    public int toEventCodeId()
    {
        return EVENT_CODE_TYPE << 16 | (id & 0xFFFF);
    }

    /**
     * Get {@link ArchiveEventCode} from its event code id.
     *
     * @param eventCodeId to convert.
     * @return {@link ArchiveEventCode} from its event code id.
     */
    public static ArchiveEventCode fromEventCodeId(final int eventCodeId)
    {
        return get(eventCodeId - (EVENT_CODE_TYPE << 16));
    }

    /**
     * Decode an event serialised in a buffer to a provided {@link StringBuilder}.
     *
     * @param buffer  containing the encoded event.
     * @param offset  offset at which the event begins.
     * @param builder to write the decoded event to.
     */
    public void decode(final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        dissector.dissect(this, buffer, offset, builder);
    }
}
