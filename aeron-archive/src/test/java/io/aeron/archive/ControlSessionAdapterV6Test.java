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
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.codecs.v6.*;
import io.aeron.logbuffer.Header;
import io.aeron.security.AuthorisationService;
import io.aeron.security.NullCredentialsSupplier;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class ControlSessionAdapterV6Test
{

    public static final long CONTROL_SESSION_ID = 928374L;
    private final ArchiveConductor mockConductor = mock(ArchiveConductor.class);
    private final Subscription mockControlSubsciption = mock(Subscription.class);
    private final Subscription mockLocalControlSubsciption = mock(Subscription.class);
    private final AuthorisationService mockAuthorisationService = mock(AuthorisationService.class);
    private final Header mockHeader = mock(Header.class);
    private final ControlSession mockSession = mock(ControlSession.class);

    public static final int SCHEMA_VERSION_6 = 6;

    @BeforeEach
    void before()
    {
        final Image image = mock(Image.class);
        when(mockHeader.context()).thenReturn(image);
    }

    @Test
    void shouldHandleVersion6ReplicationRequest2()
    {
        final ControlSessionAdapter controlSessionAdapter = new ControlSessionAdapter(
            new ControlRequestDecoders(),
            mockControlSubsciption,
            mockLocalControlSubsciption,
            mockConductor,
            mockAuthorisationService);
        setupControlSession(controlSessionAdapter);

        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final ReplicateRequest2Encoder replicateRequest2Encoder = new ReplicateRequest2Encoder();

        replicateRequest2Encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);

        final int fileIoMaxLength = Aeron.NULL_VALUE; // Since v7
        final int sessionId = Aeron.NULL_VALUE; // Since v8
        final byte[] encodedCredentials = NullCredentialsSupplier.NULL_CREDENTIAL; // Since v8

        replicateRequest2Encoder
            .controlSessionId(928374L)
            .correlationId(9382475L)
            .srcRecordingId(1234234L)
            .dstRecordingId(2532453245L)
            .stopPosition(2315345L)
            .channelTagId(234L)
            .subscriptionTagId(235L)
            .srcControlStreamId(982374)
            .srcControlChannel("src")
            .liveDestination("live")
            .replicationChannel("replication");
        final int replicateRequestLength = replicateRequest2Encoder.encodedLength();

        controlSessionAdapter.onFragment(buffer, 0, replicateRequestLength, mockHeader);

        final ReplicateRequest2Decoder expected = new ReplicateRequest2Decoder()
            .wrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());
        verify(mockSession).onReplicate(
            expected.correlationId(),
            expected.srcRecordingId(),
            expected.dstRecordingId(),
            expected.stopPosition(),
            expected.channelTagId(),
            expected.subscriptionTagId(),
            expected.srcControlStreamId(),
            fileIoMaxLength,
            sessionId,
            expected.srcControlChannel(),
            expected.liveDestination(),
            expected.replicationChannel(),
            encodedCredentials,
            "",
            0L);
    }

    @Test
    void shouldHandleVersion6ReplayRequest2()
    {
        final ControlSessionAdapter controlSessionAdapter = new ControlSessionAdapter(
            new ControlRequestDecoders(),
            mockControlSubsciption,
            mockLocalControlSubsciption,
            mockConductor,
            mockAuthorisationService);
        setupControlSession(controlSessionAdapter);

        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final ReplayRequestEncoder replayRequestEncoder = new ReplayRequestEncoder();

        replayRequestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);

        final int fileIoMaxLength = Aeron.NULL_VALUE; // Since v7

        replayRequestEncoder
            .controlSessionId(928374L)
            .correlationId(9382475L)
            .recordingId(9827345897L)
            .position(982374L)
            .replayStreamId(9832475)
            .replayChannel("aeron:ipc");

        final int replicateRequestLength = replayRequestEncoder.encodedLength();

        controlSessionAdapter.onFragment(buffer, 0, replicateRequestLength, mockHeader);

        final ReplayRequestDecoder expected = new ReplayRequestDecoder()
            .wrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());

        verify(mockSession).onStartReplay(
            expected.correlationId(),
            expected.recordingId(),
            expected.position(),
            expected.length(),
            fileIoMaxLength,
            0L,
            expected.replayStreamId(),
            expected.replayChannel());
    }

    @Test
    void shouldHandleVersion6BoundedReplayRequest()
    {
        final ControlSessionAdapter controlSessionAdapter = new ControlSessionAdapter(
            new ControlRequestDecoders(),
            mockControlSubsciption,
            mockLocalControlSubsciption,
            mockConductor,
            mockAuthorisationService);
        setupControlSession(controlSessionAdapter);

        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final BoundedReplayRequestEncoder replayRequestEncoder = new BoundedReplayRequestEncoder();

        replayRequestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);

        replayRequestEncoder
            .controlSessionId(928374L)
            .correlationId(9382475L)
            .recordingId(9827345897L)
            .position(982374L)
            .limitCounterId(92734)
            .replayStreamId(9832475)
            .replayChannel("aeron:ipc?alias=replay");

        final int replicateRequestLength = replayRequestEncoder.encodedLength();

        controlSessionAdapter.onFragment(buffer, 0, replicateRequestLength, mockHeader);

        final BoundedReplayRequestDecoder expected = new BoundedReplayRequestDecoder()
            .wrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());

        verify(mockSession).onStartBoundedReplay(
            expected.correlationId(),
            expected.recordingId(),
            expected.position(),
            expected.length(),
            expected.limitCounterId(),
            Aeron.NULL_VALUE,
            0L,
            expected.replayStreamId(),
            expected.replayChannel());
    }

    private void setupControlSession(final ControlSessionAdapter controlSessionAdapter)
    {
        final MutableDirectBuffer buffer = new ExpandableArrayBuffer();
        final MessageHeaderEncoder headerEncoder2 = new MessageHeaderEncoder();
        final ConnectRequestEncoder connectRequestEncoder = new ConnectRequestEncoder();
        connectRequestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder2);
        connectRequestEncoder
            .correlationId(100)
            .responseStreamId(100)
            .version(SCHEMA_VERSION_6)
            .responseChannel("foo");
        final int connectRequestLength = connectRequestEncoder.encodedLength();

        doReturn(mockSession).when(mockConductor).newControlSession(
            any(), anyLong(), anyInt(), anyInt(), anyString(), any(), anyString(), any());
        doReturn(CONTROL_SESSION_ID).when(mockSession).sessionId();
        doReturn(true).when(mockAuthorisationService).isAuthorised(anyInt(), anyInt(), any(), any());

        controlSessionAdapter.onFragment(buffer, 0, connectRequestLength, mockHeader);
    }
}
