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

import io.aeron.archive.client.PersistentSubscription;
import io.aeron.eventlog.AllowTruncate;
import io.aeron.eventlog.GeneratedLogger;
import io.aeron.eventlog.LoggerMethod;
import io.aeron.eventlog.Tag;
import io.aeron.logging.EventConfiguration;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.util.EnumSet;

import static io.aeron.archive.logging.ArchiveEventCode.*;
import static io.aeron.logging.CborUtils.AERON_ARCHIVE_ADMIN_TAG;
import static io.aeron.logging.EventConfiguration.eventReader;
import static java.util.EnumSet.complementOf;
import static java.util.EnumSet.of;

/**
 * Event logger interface used by interceptors for recording events into a {@link RingBuffer} for an
 * {@link io.aeron.archive.Archive} via a Java Agent. The implementation is generated at compile time from the
 * {@link LoggerMethod}-annotated methods below.
 */
@GeneratedLogger(eventCodeType = "io.aeron.archive.logging.ArchiveEventCode")
public interface ArchiveEventLogger
{
    /**
     * Logger for writing into the {@link ManyToOneRingBuffer} held by {@link EventConfiguration#eventReader}.
     */
    ArchiveEventLogger LOGGER = new CborArchiveEventLogger(eventReader().ringBuffer());

    /**
     * Set of event codes that represent an incoming control request, i.e. everything a caller resolving a control
     * request's event code (see {@code ArchiveLog#logControlRequest}) may be asked to log.
     */
    EnumSet<ArchiveEventCode> CONTROL_REQUEST_EVENTS = complementOf(of(
        CMD_OUT_RESPONSE,
        REPLICATION_SESSION_STATE_CHANGE,
        CONTROL_SESSION_STATE_CHANGE,
        REPLAY_SESSION_ERROR,
        CATALOG_RESIZE,
        RECORDING_SIGNAL,
        REPLICATION_SESSION_DONE,
        REPLAY_SESSION_STATE_CHANGE,
        RECORDING_SESSION_STATE_CHANGE));

    /**
     * Log an Archive control event.
     *
     * @param eventCode     for the type of control event.
     * @param buffer        containing the encoded event.
     * @param offset        in the buffer at which the event begins.
     * @param messageLength of the encoded event.
     */
    @LoggerMethod(bufferView = { "buffer", "offset", "messageLength" })
    default void logControlRequest(
        final ArchiveEventCode eventCode,
        @Tag(AERON_ARCHIVE_ADMIN_TAG) @AllowTruncate final DirectBuffer buffer,
        final int offset,
        final int messageLength)
    {
    }

    /**
     * Log an outgoing control response from the archive.
     *
     * @param buffer        containing the encoded response.
     * @param offset        at which response message begins.
     * @param messageLength of the response in the buffer.
     */
    @LoggerMethod(eventCode = "CMD_OUT_RESPONSE", bufferView = { "buffer", "offset", "messageLength" })
    default void logControlResponse(
        @Tag(AERON_ARCHIVE_ADMIN_TAG) @AllowTruncate final DirectBuffer buffer,
        final int offset,
        final int messageLength)
    {
    }

    /**
     * Log the {@link io.aeron.archive.codecs.RecordingSignal} being send.
     *
     * @param buffer        containing the encoded response.
     * @param offset        at which response message begins.
     * @param messageLength of the response in the buffer.
     */
    @LoggerMethod(eventCode = "RECORDING_SIGNAL", bufferView = { "buffer", "offset", "messageLength" })
    default void logRecordingSignal(
        @Tag(AERON_ARCHIVE_ADMIN_TAG) @AllowTruncate final DirectBuffer buffer,
        final int offset,
        final int messageLength)
    {
    }

    /**
     * Log a state change event for an archive replay session.
     *
     * @param <E>         type representing the state change.
     * @param oldState    before the change.
     * @param newState    after the change.
     * @param sessionId   identity for the replay session on the Archive.
     * @param recordingId recording id on the Archive.
     * @param position    position of state change ({@link io.aeron.archive.client.AeronArchive#NULL_POSITION}
     *                    if not relevant).
     * @param reason      a string indicating the reason for the state change.
     */
    @LoggerMethod(eventCode = "REPLAY_SESSION_STATE_CHANGE")
    default <E extends Enum<E>> void logReplaySessionStateChange(
        final E oldState,
        final E newState,
        final long sessionId,
        final long recordingId,
        final long position,
        final String reason)
    {
    }

    /**
     * Log a state change event for {@link PersistentSubscription}.
     *
     * @param <E>            type representing the state change.
     * @param oldState       before the change.
     * @param newState       after the change.
     * @param recordingId    recording id used by the {@link PersistentSubscription}.
     * @param replayChannel  the replay channel used by the {@link PersistentSubscription}.
     * @param replayStreamId the replay stream id used by the {@link PersistentSubscription}.
     * @param liveChannel    the live channel used by the {@link PersistentSubscription}.
     * @param liveStreamId   the live stream id used by the {@link PersistentSubscription}.
     */
    @LoggerMethod(eventCode = "PERSISTENT_SUBSCRIPTION_STATE_CHANGE")
    default <E extends Enum<E>> void logPersistentSubscriptionStateChange(
        final E oldState,
        final E newState,
        final long recordingId,
        final String replayChannel,
        final int replayStreamId,
        final String liveChannel,
        final int liveStreamId)
    {
    }

    /**
     * Log the state of {@link PersistentSubscription} when it joins live.
     *
     * @param recordingId    recording id used by the {@link PersistentSubscription}.
     * @param replayChannel  the replay channel used by the {@link PersistentSubscription}.
     * @param replayStreamId the replay stream id used by the {@link PersistentSubscription}.
     * @param liveChannel    the live channel used by the {@link PersistentSubscription}.
     * @param liveStreamId   the live stream id used by the {@link PersistentSubscription}.
     * @param liveSessionId  identity for the live image in the {@link PersistentSubscription}.
     * @param joinPosition   the position the {@link PersistentSubscription} joined the live stream at.
     */
    @LoggerMethod(eventCode = "PERSISTENT_SUBSCRIPTION_JOINED_LIVE")
    default void logPersistentSubscriptionJoinedLive(
        final long recordingId,
        final String replayChannel,
        final int replayStreamId,
        final String liveChannel,
        final int liveStreamId,
        final int liveSessionId,
        final long joinPosition)
    {
    }

    /**
     * Log the state of {@link PersistentSubscription} when it leaves live.
     *
     * @param recordingId    recording id used by the {@link PersistentSubscription}.
     * @param replayChannel  the replay channel used by the {@link PersistentSubscription}.
     * @param replayStreamId the replay stream id used by the {@link PersistentSubscription}.
     * @param liveChannel    the live channel used by the {@link PersistentSubscription}.
     * @param liveStreamId   the live stream id used by the {@link PersistentSubscription}.
     * @param livePosition   the live position when the {@link PersistentSubscription} left.
     */
    @LoggerMethod(eventCode = "PERSISTENT_SUBSCRIPTION_LEFT_LIVE")
    default void logPersistentSubscriptionLeftLive(
        final long recordingId,
        final String replayChannel,
        final int replayStreamId,
        final String liveChannel,
        final int liveStreamId,
        final long livePosition)
    {
    }

    /**
     * Log a state change event for an archive recording session.
     *
     * @param <E>         type representing the state change.
     * @param oldState    before the change.
     * @param newState    after the change.
     * @param recordingId recording id on the Archive.
     * @param position    position of state change ({@link io.aeron.archive.client.AeronArchive#NULL_POSITION}
     *                    if not relevant).
     * @param reason      a string indicating the reason for the state change.
     */
    @LoggerMethod(eventCode = "RECORDING_SESSION_STATE_CHANGE")
    default <E extends Enum<E>> void logRecordingSessionStateChange(
        final E oldState,
        final E newState,
        final long recordingId,
        final long position,
        final String reason)
    {
    }

    /**
     * Log a state change event for an archive replication session.
     *
     * @param <E>            type representing the state change.
     * @param oldState       before the change.
     * @param newState       after the change.
     * @param replicationId  replication id on the Archive.
     * @param srcRecordingId source recording id on the Archive.
     * @param dstRecordingId destination recording id on the Archive.
     * @param position       position of state change ({@link io.aeron.archive.client.AeronArchive#NULL_POSITION}
     *                       if not relevant).
     * @param reason         a string indicating the reason for the state change.
     */
    @LoggerMethod(eventCode = "REPLICATION_SESSION_STATE_CHANGE")
    default <E extends Enum<E>> void logReplicationSessionStateChange(
        final E oldState,
        final E newState,
        final long replicationId,
        final long srcRecordingId,
        final long dstRecordingId,
        final long position,
        final String reason)
    {
    }

    /**
     * Log a state change event for an archive control session.
     *
     * @param <E>              type representing the state change.
     * @param oldState         before the change.
     * @param newState         after the change.
     * @param controlSessionId identity for the control session on the Archive.
     * @param reason           a string indicating the reason for the state change.
     */
    @LoggerMethod(eventCode = "CONTROL_SESSION_STATE_CHANGE")
    default <E extends Enum<E>> void logControlSessionStateChange(
        final E oldState,
        final E newState,
        final long controlSessionId,
        final String reason)
    {
    }

    /**
     * Log the replication session done event.
     *
     * @param controlSessionId identity for the control session on the Archive.
     * @param replicationId    identity for the replication session.
     * @param srcRecordingId   identity for the recording in the source Archive.
     * @param replayPosition   position to start the replay from.
     * @param srcStopPosition  stop position of the source recording.
     * @param dstRecordingId   identity for the recording in the destination Archive.
     * @param dstStopPosition  stop position of the destination recording.
     * @param position         position of the replication when the session stopped.
     * @param isClosed         is the source image closed.
     * @param isEndOfStream    is the source image at the end of the stream.
     * @param isSynced         has the destination recording position reached the stop position of the source
     *                         recording.
     */
    @LoggerMethod(eventCode = "REPLICATION_SESSION_DONE")
    default void logReplicationSessionDone(
        final long controlSessionId,
        final long replicationId,
        final long srcRecordingId,
        final long replayPosition,
        final long srcStopPosition,
        final long dstRecordingId,
        final long dstStopPosition,
        final long position,
        final boolean isClosed,
        final boolean isEndOfStream,
        final boolean isSynced)
    {
    }

    /**
     * Log a control response error.
     *
     * @param sessionId    associated with the response.
     * @param recordingId  to which the error applies.
     * @param errorMessage which resulted.
     */
    @LoggerMethod(eventCode = "REPLAY_SESSION_ERROR")
    default void logReplaySessionError(final long sessionId, final long recordingId, final String errorMessage)
    {
    }

    /**
     * Log a Catalog resize event.
     *
     * @param oldCatalogLength before the resize.
     * @param newCatalogLength after the resize.
     */
    @LoggerMethod(eventCode = "CATALOG_RESIZE")
    default void logCatalogResize(final long oldCatalogLength, final long newCatalogLength)
    {
    }
}
