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
package io.aeron.archive;

import io.aeron.agent.ArchiveEventCode;
import io.aeron.agent.ArchiveEventLogger;
import io.aeron.agent.ArchiveModuleLogger;
import org.agrona.DirectBuffer;

import static io.aeron.agent.ArchiveModuleLogger.isEnabled;

/**
 * Facade used to log Archive events directly, without requiring the ByteBuddy Java agent to be attached.
 */
public final class ArchiveLog
{
    private static final boolean LOG_ANY_CONTROL_REQUEST_ENABLED =
        ArchiveEventLogger.CONTROL_REQUEST_EVENTS.stream().anyMatch(ArchiveModuleLogger::isEnabled);
    private static final boolean LOG_CONTROL_RESPONSE_ENABLED = isEnabled(ArchiveEventCode.CMD_OUT_RESPONSE);
    private static final boolean LOG_RECORDING_SIGNAL_ENABLED = isEnabled(ArchiveEventCode.RECORDING_SIGNAL);
    private static final boolean LOG_REPLAY_SESSION_STATE_CHANGE_ENABLED =
        isEnabled(ArchiveEventCode.REPLAY_SESSION_STATE_CHANGE);
    private static final boolean LOG_REPLAY_SESSION_ERROR_ENABLED =
        isEnabled(ArchiveEventCode.REPLAY_SESSION_ERROR);
    private static final boolean LOG_RECORDING_SESSION_STATE_CHANGE_ENABLED =
        isEnabled(ArchiveEventCode.RECORDING_SESSION_STATE_CHANGE);
    private static final boolean LOG_REPLICATION_SESSION_STATE_CHANGE_ENABLED =
        isEnabled(ArchiveEventCode.REPLICATION_SESSION_STATE_CHANGE);
    private static final boolean LOG_REPLICATION_SESSION_DONE_ENABLED =
        isEnabled(ArchiveEventCode.REPLICATION_SESSION_DONE);
    private static final boolean LOG_CONTROL_SESSION_STATE_CHANGE_ENABLED =
        isEnabled(ArchiveEventCode.CONTROL_SESSION_STATE_CHANGE);
    private static final boolean LOG_CATALOG_RESIZE_ENABLED = isEnabled(ArchiveEventCode.CATALOG_RESIZE);
    private static final boolean LOG_PERSISTENT_SUBSCRIPTION_STATE_CHANGE_ENABLED =
        isEnabled(ArchiveEventCode.PERSISTENT_SUBSCRIPTION_STATE_CHANGE);
    private static final boolean LOG_PERSISTENT_SUBSCRIPTION_JOINED_LIVE_ENABLED =
        isEnabled(ArchiveEventCode.PERSISTENT_SUBSCRIPTION_JOINED_LIVE);
    private static final boolean LOG_PERSISTENT_SUBSCRIPTION_LEFT_LIVE_ENABLED =
        isEnabled(ArchiveEventCode.PERSISTENT_SUBSCRIPTION_LEFT_LIVE);

    private ArchiveLog()
    {
    }

    /**
     * Log an incoming control request to the archive.
     *
     * @param buffer containing the encoded request.
     * @param offset in the buffer at which the request begins.
     * @param length of the request in the buffer.
     */
    public static void logControlRequest(final DirectBuffer buffer, final int offset, final int length)
    {
        if (!LOG_ANY_CONTROL_REQUEST_ENABLED)
        {
            return;
        }

        ArchiveEventLogger.LOGGER.logControlRequest(buffer, offset, length);
    }

    /**
     * Log an outgoing control response from the archive.
     *
     * @param buffer containing the encoded response.
     * @param offset at which response message begins.
     * @param length of the response in the buffer.
     */
    public static void logControlResponse(final DirectBuffer buffer, final int offset, final int length)
    {
        if (!LOG_CONTROL_RESPONSE_ENABLED)
        {
            return;
        }

        ArchiveEventLogger.LOGGER.logControlResponse(buffer, offset, length);
    }

    /**
     * Log the {@link io.aeron.archive.codecs.RecordingSignal} being sent.
     *
     * @param buffer containing the encoded response.
     * @param offset at which response message begins.
     * @param length of the response in the buffer.
     */
    public static void logRecordingSignal(final DirectBuffer buffer, final int offset, final int length)
    {
        if (!LOG_RECORDING_SIGNAL_ENABLED)
        {
            return;
        }

        ArchiveEventLogger.LOGGER.logRecordingSignal(buffer, offset, length);
    }

    /**
     * Log a state change event for an archive replay session.
     *
     * @param <E>         type representing the state change.
     * @param oldState    before the change.
     * @param newState    after the change.
     * @param sessionId   identity for the replay session on the Archive.
     * @param recordingId recording id on the Archive.
     * @param position    position of state change.
     * @param reason      a string indicating the reason for the state change.
     */
    public static <E extends Enum<E>> void logReplaySessionStateChange(
        final E oldState,
        final E newState,
        final long sessionId,
        final long recordingId,
        final long position,
        final String reason)
    {
        if (!LOG_REPLAY_SESSION_STATE_CHANGE_ENABLED)
        {
            return;
        }

        ArchiveEventLogger.LOGGER.logReplaySessionStateChange(oldState, newState, sessionId, recordingId,
            position, reason);
    }

    /**
     * Log a control response error.
     *
     * @param sessionId    associated with the response.
     * @param recordingId  to which the error applies.
     * @param errorMessage which resulted.
     */
    public static void logReplaySessionError(final long sessionId, final long recordingId, final String errorMessage)
    {
        if (!LOG_REPLAY_SESSION_ERROR_ENABLED)
        {
            return;
        }

        ArchiveEventLogger.LOGGER.logReplaySessionError(sessionId, recordingId, errorMessage);
    }

    /**
     * Log a state change event for an archive recording session.
     *
     * @param <E>         type representing the state change.
     * @param oldState    before the change.
     * @param newState    after the change.
     * @param recordingId recording id on the Archive.
     * @param position    position of state change.
     * @param reason      a string indicating the reason for the state change.
     */
    public static <E extends Enum<E>> void logRecordingSessionStateChange(
        final E oldState,
        final E newState,
        final long recordingId,
        final long position,
        final String reason)
    {
        if (!LOG_RECORDING_SESSION_STATE_CHANGE_ENABLED)
        {
            return;
        }

        ArchiveEventLogger.LOGGER.logRecordingSessionStateChange(oldState, newState, recordingId, position, reason);
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
     * @param position       position of state change.
     * @param reason         a string indicating the reason for the state change.
     */
    public static <E extends Enum<E>> void logReplicationSessionStateChange(
        final E oldState,
        final E newState,
        final long replicationId,
        final long srcRecordingId,
        final long dstRecordingId,
        final long position,
        final String reason)
    {
        if (!LOG_REPLICATION_SESSION_STATE_CHANGE_ENABLED)
        {
            return;
        }

        ArchiveEventLogger.LOGGER.logReplicationSessionStateChange(
            oldState, newState, replicationId, srcRecordingId, dstRecordingId, position, reason);
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
    public static void logReplicationSessionDone(
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
        if (!LOG_REPLICATION_SESSION_DONE_ENABLED)
        {
            return;
        }

        ArchiveEventLogger.LOGGER.logReplicationSessionDone(
            controlSessionId,
            replicationId,
            srcRecordingId,
            replayPosition,
            srcStopPosition,
            dstRecordingId,
            dstStopPosition,
            position,
            isClosed,
            isEndOfStream,
            isSynced);
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
    public static <E extends Enum<E>> void logControlSessionStateChange(
        final E oldState,
        final E newState,
        final long controlSessionId,
        final String reason)
    {
        if (!LOG_CONTROL_SESSION_STATE_CHANGE_ENABLED)
        {
            return;
        }

        ArchiveEventLogger.LOGGER.logControlSessionStateChange(oldState, newState, controlSessionId, reason);
    }

    /**
     * Log a Catalog resize event.
     *
     * @param oldCatalogLength before the resize.
     * @param newCatalogLength after the resize.
     */
    public static void logCatalogResize(final long oldCatalogLength, final long newCatalogLength)
    {
        if (!LOG_CATALOG_RESIZE_ENABLED)
        {
            return;
        }

        ArchiveEventLogger.LOGGER.logCatalogResize(oldCatalogLength, newCatalogLength);
    }

    /**
     * Log a state change event for a {@link io.aeron.archive.client.PersistentSubscription}.
     *
     * @param <E>            type representing the state change.
     * @param oldState       before the change.
     * @param newState       after the change.
     * @param recordingId    recording id used by the {@link io.aeron.archive.client.PersistentSubscription}.
     * @param replayChannel  the replay channel used by the {@link io.aeron.archive.client.PersistentSubscription}.
     * @param replayStreamId the replay stream id used by the {@link io.aeron.archive.client.PersistentSubscription}.
     * @param liveChannel    the live channel used by the {@link io.aeron.archive.client.PersistentSubscription}.
     * @param liveStreamId   the live stream id used by the {@link io.aeron.archive.client.PersistentSubscription}.
     */
    public static <E extends Enum<E>> void logPersistentSubscriptionStateChange(
        final E oldState,
        final E newState,
        final long recordingId,
        final String replayChannel,
        final int replayStreamId,
        final String liveChannel,
        final int liveStreamId)
    {
        if (!LOG_PERSISTENT_SUBSCRIPTION_STATE_CHANGE_ENABLED)
        {
            return;
        }

        ArchiveEventLogger.LOGGER.logPersistentSubscriptionStateChange(
            oldState, newState, recordingId, replayChannel, replayStreamId, liveChannel, liveStreamId);
    }

    /**
     * Log the state of a {@link io.aeron.archive.client.PersistentSubscription} when it joins live.
     *
     * @param recordingId    recording id used by the {@link io.aeron.archive.client.PersistentSubscription}.
     * @param replayChannel  the replay channel used by the {@link io.aeron.archive.client.PersistentSubscription}.
     * @param replayStreamId the replay stream id used by the {@link io.aeron.archive.client.PersistentSubscription}.
     * @param liveChannel    the live channel used by the {@link io.aeron.archive.client.PersistentSubscription}.
     * @param liveStreamId   the live stream id used by the {@link io.aeron.archive.client.PersistentSubscription}.
     * @param liveSessionId  identity for the live image in the {@link io.aeron.archive.client.PersistentSubscription}.
     * @param joinPosition   the position the {@link io.aeron.archive.client.PersistentSubscription} joined the
     *                       live stream at.
     */
    public static void logPersistentSubscriptionJoinedLive(
        final long recordingId,
        final String replayChannel,
        final int replayStreamId,
        final String liveChannel,
        final int liveStreamId,
        final int liveSessionId,
        final long joinPosition)
    {
        if (!LOG_PERSISTENT_SUBSCRIPTION_JOINED_LIVE_ENABLED)
        {
            return;
        }

        ArchiveEventLogger.LOGGER.logPersistentSubscriptionJoinedLive(
            recordingId, replayChannel, replayStreamId, liveChannel, liveStreamId, liveSessionId, joinPosition);
    }

    /**
     * Log the state of a {@link io.aeron.archive.client.PersistentSubscription} when it leaves live.
     *
     * @param recordingId    recording id used by the {@link io.aeron.archive.client.PersistentSubscription}.
     * @param replayChannel  the replay channel used by the {@link io.aeron.archive.client.PersistentSubscription}.
     * @param replayStreamId the replay stream id used by the {@link io.aeron.archive.client.PersistentSubscription}.
     * @param liveChannel    the live channel used by the {@link io.aeron.archive.client.PersistentSubscription}.
     * @param liveStreamId   the live stream id used by the {@link io.aeron.archive.client.PersistentSubscription}.
     * @param livePosition   the live position when the {@link io.aeron.archive.client.PersistentSubscription} left.
     */
    public static void logPersistentSubscriptionLeftLive(
        final long recordingId,
        final String replayChannel,
        final int replayStreamId,
        final String liveChannel,
        final int liveStreamId,
        final long livePosition)
    {
        if (!LOG_PERSISTENT_SUBSCRIPTION_LEFT_LIVE_ENABLED)
        {
            return;
        }

        ArchiveEventLogger.LOGGER.logPersistentSubscriptionLeftLive(
            recordingId, replayChannel, replayStreamId, liveChannel, liveStreamId, livePosition);
    }
}
