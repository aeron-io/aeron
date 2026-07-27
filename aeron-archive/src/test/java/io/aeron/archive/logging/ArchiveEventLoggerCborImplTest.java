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

import io.aeron.logging.CborDecode;
import io.aeron.logging.EventCodeType;
import io.aeron.logging.LoggerEventCallback;
import io.aeron.test.Tests;
import io.aeron.test.logging.ProxyLoggerEventCallback;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import static io.aeron.logging.CborUtils.AERON_ARCHIVE_ADMIN_TAG;
import static io.aeron.logging.CborUtils.ENUM_TAG;
import static io.aeron.logging.CborUtils.NO_TAG;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class ArchiveEventLoggerCborImplTest
{
    private final ManyToOneRingBuffer ringBuffer = new ManyToOneRingBuffer(
        new UnsafeBuffer(BufferUtil.allocateDirectAligned(64 * 1024 + TRAILER_LENGTH, CACHE_LINE_LENGTH)));

    private final LoggerEventCallback mockLoggingCallback = mock(LoggerEventCallback.class);
    private final CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(mockLoggingCallback)));
    private final ArchiveEventLogger logger = new ArchiveEventLoggerCborImpl(ringBuffer);

    private void drain()
    {
        while (0 == ringBuffer.read(cborDecode))
        {
            Tests.yield();
        }
    }

    private void verifyHeader(final ArchiveEventCode eventCode)
    {
        verify(mockLoggingCallback).onHeader(
            eq(EventCodeType.ARCHIVE.getTypeCode()), eq(eventCode.id()), eq(eventCode.name()), anyLong());
    }

    @Test
    void logUsesTheArchiveAdminTagAndTheBufferViewSubRange()
    {
        final byte[] backing = new byte[1024];
        Arrays.fill(backing, (byte)0xAA);
        final byte[] subRange = new byte[16];
        Arrays.fill(subRange, (byte)0xAA);
        final UnsafeBuffer buffer = new UnsafeBuffer(backing);

        logger.logControlRequest(ArchiveEventCode.CMD_IN_CONNECT, buffer, 100, 16);

        drain();

        final InOrder inOrder = Mockito.inOrder(mockLoggingCallback);
        inOrder.verify(mockLoggingCallback).onHeader(
            eq(EventCodeType.ARCHIVE.getTypeCode()),
            eq(ArchiveEventCode.CMD_IN_CONNECT.id()),
            eq(ArchiveEventCode.CMD_IN_CONNECT.name()),
            anyLong());
        inOrder.verify(mockLoggingCallback).onValue("buffer", AERON_ARCHIVE_ADMIN_TAG, new UnsafeBuffer(subRange));
        inOrder.verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logTruncatesAnOversizedBufferSinceAllowTruncateIsSet()
    {
        final byte[] backing = new byte[10_000];
        Arrays.fill(backing, (byte)0x11);
        final UnsafeBuffer buffer = new UnsafeBuffer(backing);

        logger.logControlRequest(ArchiveEventCode.CMD_IN_CONNECT, buffer, 0, backing.length);

        drain();

        verify(mockLoggingCallback, never()).onFooter(false);
        verify(mockLoggingCallback).onValue(eq("buffer"), eq(AERON_ARCHIVE_ADMIN_TAG), any(DirectBuffer.class));
        verify(mockLoggingCallback).onFooter(true);
    }

    @Test
    void logControlResponseUsesTheFixedEventCodeAndTheArchiveAdminTag()
    {
        final byte[] backing = new byte[64];
        Arrays.fill(backing, (byte)0x1);
        final UnsafeBuffer buffer = new UnsafeBuffer(backing);

        logger.logControlResponse(buffer, 4, 60);

        drain();

        final byte[] subRange = new byte[60];
        Arrays.fill(subRange, (byte)0x1);
        final InOrder inOrder = Mockito.inOrder(mockLoggingCallback);
        inOrder.verify(mockLoggingCallback).onHeader(
            eq(EventCodeType.ARCHIVE.getTypeCode()),
            eq(ArchiveEventCode.CMD_OUT_RESPONSE.id()),
            eq(ArchiveEventCode.CMD_OUT_RESPONSE.name()),
            anyLong());
        inOrder.verify(mockLoggingCallback).onValue("buffer", AERON_ARCHIVE_ADMIN_TAG, new UnsafeBuffer(subRange));
        inOrder.verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logRecordingSignalUsesTheFixedEventCodeAndTheArchiveAdminTag()
    {
        final byte[] backing = new byte[64];
        Arrays.fill(backing, (byte)0x3);
        final UnsafeBuffer buffer = new UnsafeBuffer(backing);

        logger.logRecordingSignal(buffer, 10, 31);

        drain();

        final byte[] subRange = new byte[31];
        Arrays.fill(subRange, (byte)0x3);
        verifyHeader(ArchiveEventCode.RECORDING_SIGNAL);
        verify(mockLoggingCallback).onValue("buffer", AERON_ARCHIVE_ADMIN_TAG, new UnsafeBuffer(subRange));
        verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logReplaySessionStateChange()
    {
        final ChronoUnit oldState = ChronoUnit.CENTURIES;
        final ChronoUnit newState = ChronoUnit.MICROS;

        logger.logReplaySessionStateChange(oldState, newState, 111L, 222L, 333L, "a reason");

        drain();

        final InOrder inOrder = Mockito.inOrder(mockLoggingCallback);
        inOrder.verify(mockLoggingCallback).onHeader(
            eq(EventCodeType.ARCHIVE.getTypeCode()),
            eq(ArchiveEventCode.REPLAY_SESSION_STATE_CHANGE.id()),
            eq(ArchiveEventCode.REPLAY_SESSION_STATE_CHANGE.name()),
            anyLong());
        inOrder.verify(mockLoggingCallback).onValue("oldState", ENUM_TAG, oldState.name());
        inOrder.verify(mockLoggingCallback).onValue("newState", ENUM_TAG, newState.name());
        inOrder.verify(mockLoggingCallback).onValue("sessionId", NO_TAG, 111L);
        inOrder.verify(mockLoggingCallback).onValue("recordingId", NO_TAG, 222L);
        inOrder.verify(mockLoggingCallback).onValue("position", NO_TAG, 333L);
        inOrder.verify(mockLoggingCallback).onValue("reason", NO_TAG, "a reason");
        inOrder.verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logPersistentSubscriptionStateChange()
    {
        final ChronoUnit oldState = ChronoUnit.CENTURIES;
        final ChronoUnit newState = ChronoUnit.MICROS;

        logger.logPersistentSubscriptionStateChange(
            oldState, newState, 555L, "replay-channel", 10, "live-channel", 11);

        drain();

        verifyHeader(ArchiveEventCode.PERSISTENT_SUBSCRIPTION_STATE_CHANGE);
        verify(mockLoggingCallback).onValue("oldState", ENUM_TAG, oldState.name());
        verify(mockLoggingCallback).onValue("newState", ENUM_TAG, newState.name());
        verify(mockLoggingCallback).onValue("recordingId", NO_TAG, 555L);
        verify(mockLoggingCallback).onValue("replayChannel", NO_TAG, "replay-channel");
        verify(mockLoggingCallback).onValue("replayStreamId", NO_TAG, 10);
        verify(mockLoggingCallback).onValue("liveChannel", NO_TAG, "live-channel");
        verify(mockLoggingCallback).onValue("liveStreamId", NO_TAG, 11);
        verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logPersistentSubscriptionJoinedLive()
    {
        logger.logPersistentSubscriptionJoinedLive(
            555L, "replay-channel", 10, "live-channel", 11, 5, 128L);

        drain();

        verifyHeader(ArchiveEventCode.PERSISTENT_SUBSCRIPTION_JOINED_LIVE);
        verify(mockLoggingCallback).onValue("recordingId", NO_TAG, 555L);
        verify(mockLoggingCallback).onValue("replayChannel", NO_TAG, "replay-channel");
        verify(mockLoggingCallback).onValue("replayStreamId", NO_TAG, 10);
        verify(mockLoggingCallback).onValue("liveChannel", NO_TAG, "live-channel");
        verify(mockLoggingCallback).onValue("liveStreamId", NO_TAG, 11);
        verify(mockLoggingCallback).onValue("liveSessionId", NO_TAG, 5);
        verify(mockLoggingCallback).onValue("joinPosition", NO_TAG, 128L);
        verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logPersistentSubscriptionLeftLive()
    {
        logger.logPersistentSubscriptionLeftLive(555L, "replay-channel", 10, "live-channel", 11, 256L);

        drain();

        verifyHeader(ArchiveEventCode.PERSISTENT_SUBSCRIPTION_LEFT_LIVE);
        verify(mockLoggingCallback).onValue("recordingId", NO_TAG, 555L);
        verify(mockLoggingCallback).onValue("replayChannel", NO_TAG, "replay-channel");
        verify(mockLoggingCallback).onValue("replayStreamId", NO_TAG, 10);
        verify(mockLoggingCallback).onValue("liveChannel", NO_TAG, "live-channel");
        verify(mockLoggingCallback).onValue("liveStreamId", NO_TAG, 11);
        verify(mockLoggingCallback).onValue("livePosition", NO_TAG, 256L);
        verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logRecordingSessionStateChange()
    {
        final ChronoUnit oldState = ChronoUnit.ERAS;
        final ChronoUnit newState = ChronoUnit.MILLENNIA;

        logger.logRecordingSessionStateChange(oldState, newState, 42L, 84L, "a reason");

        drain();

        verifyHeader(ArchiveEventCode.RECORDING_SESSION_STATE_CHANGE);
        verify(mockLoggingCallback).onValue("oldState", ENUM_TAG, oldState.name());
        verify(mockLoggingCallback).onValue("newState", ENUM_TAG, newState.name());
        verify(mockLoggingCallback).onValue("recordingId", NO_TAG, 42L);
        verify(mockLoggingCallback).onValue("position", NO_TAG, 84L);
        verify(mockLoggingCallback).onValue("reason", NO_TAG, "a reason");
        verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logReplicationSessionStateChange()
    {
        final ChronoUnit oldState = ChronoUnit.ERAS;
        final ChronoUnit newState = ChronoUnit.MILLENNIA;

        logger.logReplicationSessionStateChange(oldState, newState, 1L, 2L, 3L, 4L, "some text goes here");

        drain();

        verifyHeader(ArchiveEventCode.REPLICATION_SESSION_STATE_CHANGE);
        verify(mockLoggingCallback).onValue("oldState", ENUM_TAG, oldState.name());
        verify(mockLoggingCallback).onValue("newState", ENUM_TAG, newState.name());
        verify(mockLoggingCallback).onValue("replicationId", NO_TAG, 1L);
        verify(mockLoggingCallback).onValue("srcRecordingId", NO_TAG, 2L);
        verify(mockLoggingCallback).onValue("dstRecordingId", NO_TAG, 3L);
        verify(mockLoggingCallback).onValue("position", NO_TAG, 4L);
        verify(mockLoggingCallback).onValue("reason", NO_TAG, "some text goes here");
        verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logControlSessionStateChange()
    {
        final ChronoUnit oldState = ChronoUnit.CENTURIES;
        final ChronoUnit newState = ChronoUnit.MICROS;

        logger.logControlSessionStateChange(oldState, newState, 555_000_000_000L, "test reason to check");

        drain();

        verifyHeader(ArchiveEventCode.CONTROL_SESSION_STATE_CHANGE);
        verify(mockLoggingCallback).onValue("oldState", ENUM_TAG, oldState.name());
        verify(mockLoggingCallback).onValue("newState", ENUM_TAG, newState.name());
        verify(mockLoggingCallback).onValue("controlSessionId", NO_TAG, 555_000_000_000L);
        verify(mockLoggingCallback).onValue("reason", NO_TAG, "test reason to check");
        verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logReplicationSessionDone()
    {
        logger.logReplicationSessionDone(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, true, true, false);

        drain();

        verifyHeader(ArchiveEventCode.REPLICATION_SESSION_DONE);
        verify(mockLoggingCallback).onValue("controlSessionId", NO_TAG, 1L);
        verify(mockLoggingCallback).onValue("replicationId", NO_TAG, 2L);
        verify(mockLoggingCallback).onValue("srcRecordingId", NO_TAG, 3L);
        verify(mockLoggingCallback).onValue("replayPosition", NO_TAG, 4L);
        verify(mockLoggingCallback).onValue("srcStopPosition", NO_TAG, 5L);
        verify(mockLoggingCallback).onValue("dstRecordingId", NO_TAG, 6L);
        verify(mockLoggingCallback).onValue("dstStopPosition", NO_TAG, 7L);
        verify(mockLoggingCallback).onValue("position", NO_TAG, 8L);
        verify(mockLoggingCallback).onValue("isClosed", NO_TAG, true);
        verify(mockLoggingCallback).onValue("isEndOfStream", NO_TAG, true);
        verify(mockLoggingCallback).onValue("isSynced", NO_TAG, false);
        verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logReplaySessionError()
    {
        logger.logReplaySessionError(123L, Long.MIN_VALUE, "the error");

        drain();

        verifyHeader(ArchiveEventCode.REPLAY_SESSION_ERROR);
        verify(mockLoggingCallback).onValue("sessionId", NO_TAG, 123L);
        verify(mockLoggingCallback).onValue("recordingId", NO_TAG, Long.MIN_VALUE);
        verify(mockLoggingCallback).onValue("errorMessage", NO_TAG, "the error");
        verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logCatalogResize()
    {
        logger.logCatalogResize(42L, 142L);

        drain();

        verifyHeader(ArchiveEventCode.CATALOG_RESIZE);
        verify(mockLoggingCallback).onValue("oldCatalogLength", NO_TAG, 42L);
        verify(mockLoggingCallback).onValue("newCatalogLength", NO_TAG, 142L);
        verify(mockLoggingCallback).onFooter(false);
    }
}
