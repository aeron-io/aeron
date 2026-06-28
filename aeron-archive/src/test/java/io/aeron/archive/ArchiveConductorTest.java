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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Counter;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.security.Authenticator;
import io.aeron.security.AuthorisationService;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.FileStore;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.ArgumentCaptor;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ArchiveConductorTest
{
    @TempDir
    File archiveDir;

    private final Aeron mockAeron = mock(Aeron.class);
    private final CountedErrorHandler mockErrorHandler = mock(CountedErrorHandler.class);
    private final Authenticator mockAuthenticator = mock(Authenticator.class);
    private final AuthorisationService mockAuthorisationService = mock(AuthorisationService.class);
    private final FileStore mockFileStore = mock(FileStore.class);
    private final Catalog mockCatalog = mock(Catalog.class);

    private TestArchiveConductor conductor = null;

    private int maxConcurrentRecordings = 5;
    private int maxConcurrentReplays = 5;

    private void createTestConductor()
    {
        conductor = new TestArchiveConductor(new Archive.Context()
            .countedErrorHandler(mockErrorHandler)
            .nanoClock(SystemNanoClock.INSTANCE)
            .epochClock(SystemEpochClock.INSTANCE)
            .aeron(mockAeron)
            .archiveDir(archiveDir)
            .archiveFileStore(mockFileStore)
            .catalog(mockCatalog)
            .authenticatorSupplier(() -> mockAuthenticator)
            .authorisationServiceSupplier(() -> mockAuthorisationService)
            .controlChannelEnabled(false)
            .recordingEventsEnabled(false)
            .maxConcurrentRecordings(maxConcurrentRecordings)
            .maxConcurrentReplays(maxConcurrentReplays));
    }

    @Test
    void startRecordingShouldSendErrorOnMaxConcurrentRecordings()
    {
        maxConcurrentRecordings = 0;
        createTestConductor();

        final ControlSession mockControlSession = mock(ControlSession.class);
        final long correlationId = 1L;

        conductor.startRecording(correlationId, 1, SourceLocation.REMOTE, false, "aeron:ipc", mockControlSession);

        verify(mockControlSession).sendErrorResponse(
            eq(correlationId), eq((long)ArchiveException.MAX_RECORDINGS), anyString());
    }

    @Test
    void startReplayShouldSendErrorOnMaxConcurrentReplays()
    {
        maxConcurrentReplays = 0;
        createTestConductor();

        final ControlSession mockControlSession = mock(ControlSession.class);
        final long correlationId = 1L;

        conductor.startReplay(correlationId, 0L, 0L, 0L, 0, 1, "aeron:ipc", null, mockControlSession);

        verify(mockControlSession).sendErrorResponse(
            eq(correlationId), eq((long)ArchiveException.MAX_REPLAYS), anyString());
    }

    @Test
    void startRecordingShouldSendErrorOnLowStorageSpace()
    {
        createTestConductor();

        final ControlSession mockControlSession = mock(ControlSession.class);
        final long correlationId = 1L;

        conductor.startRecording(correlationId, 1, SourceLocation.REMOTE, false, "aeron:ipc", mockControlSession);

        verify(mockControlSession).sendErrorResponse(
            eq(correlationId), eq((long)ArchiveException.STORAGE_SPACE), anyString());
    }

    @Test
    void startReplayShouldSendErrorForBoundedReplayIntoOutstandingDeleteRegion()
    {
        createTestConductor();

        final long recordingId = 42L;
        final int segmentLength = Archive.Configuration.SEGMENT_FILE_LENGTH_DEFAULT;
        final long originalStopPosition = 2L * segmentLength;
        final long truncatePosition = segmentLength;
        final long[] catalogStopPosition = {originalStopPosition};

        when(mockCatalog.hasRecording(recordingId)).thenReturn(true);
        doAnswer(invocation ->
        {
            final RecordingSummary summary = invocation.getArgument(1);
            summary.startPosition = 0L;
            summary.stopPosition = catalogStopPosition[0];
            summary.segmentFileLength = segmentLength;
            summary.termBufferLength = 64 * 1024;
            return summary;
        }).when(mockCatalog).recordingSummary(eq(recordingId), any(RecordingSummary.class));
        doAnswer(invocation ->
        {
            catalogStopPosition[0] = invocation.getArgument(1);
            return null;
        }).when(mockCatalog).stopPosition(eq(recordingId), anyLong());

        final ControlSession mockControlSession = mock(ControlSession.class);

        conductor.truncateRecording(1L, recordingId, truncatePosition, mockControlSession);

        final Counter mockLimitCounter = mock(Counter.class);
        when(mockLimitCounter.get()).thenReturn(originalStopPosition);

        conductor.startReplay(2L, recordingId, 0L, 2L * segmentLength, 0, 1, "aeron:ipc",
            mockLimitCounter, mockControlSession);

        final ArgumentCaptor<String> errorCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockControlSession).sendErrorResponse(eq(2L), errorCaptor.capture());
        assertEquals(
            "cannot start replay of recording " + recordingId + " due to an outstanding delete operation",
            errorCaptor.getValue());
    }

    @Test
    void strippedChannelBuilderShouldCopyKeyParameters() throws ReflectiveOperationException
    {
        final ChannelUri uri = ChannelUri.parse("""
            aeron:udp?term-length=1g|mtu=2048|session-id=tag:424242424242|rcv-wnd=32k|so-sndbuf=2m|so-rcvbuf=1m|\
            endpoint=some-host:7777|interface=192.168.0.1/24|control-mode=dynamic|control=localhost:5555|\
            tether=false|reliable=false|linger=1321ms|sparse=false|cc=cubic|alias=test-stripped-channel|\
            fc=tagged,g:1111/3,t:4s|gtag=2222|tags=17,432|tag=5|ttl=3|eos=true|rejoin=false|ssc=true|group=true|\
            response-correlation-id=88888888881|response-endpoint=publisher:10101|\
            media-rcv-ts-offset=reserved|channel-rcv-ts-offset=136|channel-snd-ts-offset=144|\
            init-term-id=300|term-id=341|term-offset=4096|nak-delay=100ns|\
            untethered-window-limit-timeout=3s|untethered-linger-timeout=2s|untethered-resting-timeout=1s|\
            max-resend=13|stream-id=-654|pub-wnd=32m
            """.stripIndent());

        final ChannelUriStringBuilder builder = ArchiveConductor.strippedChannelBuilder(uri);
        assertNotNull(builder);
        assertEquals("udp", builder.media());
        assertEquals("some-host:7777", builder.endpoint());
        assertEquals("192.168.0.1/24", builder.networkInterface());
        assertEquals("localhost:5555", builder.controlEndpoint());
        assertEquals("dynamic", builder.controlMode());
        assertEquals("17,432", builder.tags());
        assertEquals(false, builder.rejoin());
        assertEquals(true, builder.group());
        assertEquals(false, builder.tether());
        assertEquals("tagged,g:1111/3,t:4s", builder.flowControl());
        assertEquals(2222, builder.groupTag());
        assertEquals("cubic", builder.congestionControl());
        assertEquals(1024 * 1024, builder.socketRcvbufLength());
        assertEquals(2 * 1024 * 1024, builder.socketSndbufLength());
        assertEquals(32 * 1024, builder.receiverWindowLength());
        assertEquals("144", builder.channelSendTimestampOffset());
        assertEquals("136", builder.channelReceiveTimestampOffset());
        assertEquals("reserved", builder.mediaReceiveTimestampOffset());
        assertTrue(builder.isSessionIdTagged());
        final Field sessionIdField = builder.getClass().getDeclaredField("sessionId");
        sessionIdField.trySetAccessible();
        assertEquals(424242424242L, sessionIdField.get(builder));
        assertEquals("test-stripped-channel", builder.alias());
        assertEquals("88888888881", builder.responseCorrelationId());
        assertEquals("publisher:10101", builder.responseEndpoint());
        assertEquals(3, builder.ttl());
        assertEquals(-654, builder.streamId());
        assertNull(builder.reliable());
        assertNull(builder.linger());
        assertNull(builder.sparse());
        assertNull(builder.eos());
        assertNull(builder.spiesSimulateConnection());
        assertNull(builder.initialTermId());
        assertNull(builder.termId());
        assertNull(builder.termOffset());
        assertNull(builder.nakDelay());
        assertNull(builder.untetheredWindowLimitTimeoutNs());
        assertNull(builder.untetheredLingerTimeoutNs());
        assertNull(builder.untetheredRestingTimeoutNs());
        assertNull(builder.maxResend());
        assertNull(builder.publicationWindowLength());
    }

    static class TestArchiveConductor extends ArchiveConductor
    {
        final List<Session> capturedSessions = new ArrayList<>();

        TestArchiveConductor(final Archive.Context ctx)
        {
            super(ctx);
        }

        @Override
        protected void addSession(final Session session)
        {
            super.addSession(session);
            capturedSessions.add(session);
        }

        @Override
        Recorder newRecorder()
        {
            return null;
        }

        @Override
        Replayer newReplayer()
        {
            return null;
        }

        @Override
        protected void closeSessionWorkers()
        {
        }
    }
}
