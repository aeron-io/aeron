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
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.aeron.archive.codecs.ControlResponseCode.NULL_VAL;
import static io.aeron.logging.EventConfiguration.MAX_EVENT_LENGTH;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ArchiveControlBinaryRendererTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_EVENT_LENGTH]);
    private final StringBuilder sb = new StringBuilder();
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final ArchiveControlBinaryRenderer renderer = new ArchiveControlBinaryRenderer();

    @Test
    void renderConnect()
    {
        final ConnectRequestEncoder requestEncoder = new ConnectRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .correlationId(88)
            .responseStreamId(42)
            .version(-10)
            .responseChannel("call me maybe");

        renderer.append(sb, ArchiveEventCode.CMD_IN_CONNECT.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "correlationId=88 responseStreamId=42 version=-10 responseChannel=call me maybe",
            sb.toString());
    }

    @Test
    void renderCloseSession()
    {
        final CloseSessionRequestEncoder requestEncoder = new CloseSessionRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(-1);

        renderer.append(sb, ArchiveEventCode.CMD_IN_CLOSE_SESSION.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=-1", sb.toString());
    }

    @Test
    void renderStartRecording()
    {
        final StartRecordingRequestEncoder requestEncoder = new StartRecordingRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(5)
            .correlationId(13)
            .streamId(7)
            .sourceLocation(SourceLocation.REMOTE)
            .channel("foo");

        renderer.append(sb, ArchiveEventCode.CMD_IN_START_RECORDING.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "controlSessionId=5 correlationId=13 streamId=7 sourceLocation=" + SourceLocation.REMOTE +
            " channel=foo",
            sb.toString());
    }

    @Test
    void renderStartRecording2()
    {
        final StartRecordingRequest2Encoder requestEncoder = new StartRecordingRequest2Encoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(5)
            .correlationId(13)
            .streamId(7)
            .sourceLocation(SourceLocation.REMOTE)
            .autoStop(BooleanType.TRUE)
            .channel("foo");

        renderer.append(sb, ArchiveEventCode.CMD_IN_START_RECORDING2.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "controlSessionId=5 correlationId=13 streamId=7 sourceLocation=" + SourceLocation.REMOTE +
            " autoStop=" + BooleanType.TRUE + " channel=foo",
            sb.toString());
    }

    @Test
    void renderStopRecording()
    {
        final StopRecordingRequestEncoder requestEncoder = new StopRecordingRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(5)
            .correlationId(42)
            .streamId(7)
            .channel("bar");

        renderer.append(sb, ArchiveEventCode.CMD_IN_STOP_RECORDING.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=5 correlationId=42 streamId=7 channel=bar", sb.toString());
    }

    @Test
    void renderReplay()
    {
        final ReplayRequestEncoder requestEncoder = new ReplayRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(5)
            .correlationId(42)
            .recordingId(178)
            .position(Long.MAX_VALUE)
            .length(2000)
            .replayStreamId(99)
            .replayChannel("replay channel");

        renderer.append(sb, ArchiveEventCode.CMD_IN_REPLAY.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "controlSessionId=5 correlationId=42 recordingId=178 position=" + Long.MAX_VALUE +
            " length=2000 replayStreamId=99 replayChannel=replay channel",
            sb.toString());
    }

    @Test
    void renderStopReplay()
    {
        final StopReplayRequestEncoder requestEncoder = new StopReplayRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(5)
            .correlationId(42)
            .replaySessionId(66);

        renderer.append(sb, ArchiveEventCode.CMD_IN_STOP_REPLAY.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=5 correlationId=42 replaySessionId=66", sb.toString());
    }

    @Test
    void renderListRecordings()
    {
        final ListRecordingsRequestEncoder requestEncoder = new ListRecordingsRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(9)
            .correlationId(78)
            .fromRecordingId(45)
            .recordCount(10);

        renderer.append(sb, ArchiveEventCode.CMD_IN_LIST_RECORDINGS.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=9 correlationId=78 fromRecordingId=45 recordCount=10", sb.toString());
    }

    @Test
    void renderListRecordingsForUri()
    {
        final ListRecordingsForUriRequestEncoder requestEncoder = new ListRecordingsForUriRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(9)
            .correlationId(78)
            .fromRecordingId(45)
            .recordCount(10)
            .streamId(200)
            .channel("CH");

        renderer.append(
            sb, ArchiveEventCode.CMD_IN_LIST_RECORDINGS_FOR_URI.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "controlSessionId=9 correlationId=78 fromRecordingId=45 recordCount=10 streamId=200 channel=CH",
            sb.toString());
    }

    @Test
    void renderListRecording()
    {
        final ListRecordingRequestEncoder requestEncoder = new ListRecordingRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(19)
            .correlationId(178)
            .recordingId(1010101);

        renderer.append(sb, ArchiveEventCode.CMD_IN_LIST_RECORDING.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=19 correlationId=178 recordingId=1010101", sb.toString());
    }

    @Test
    void renderExtendRecording()
    {
        final ExtendRecordingRequestEncoder requestEncoder = new ExtendRecordingRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(9)
            .correlationId(78)
            .recordingId(1010101)
            .streamId(43)
            .sourceLocation(SourceLocation.LOCAL)
            .channel("extend me");

        renderer.append(sb, ArchiveEventCode.CMD_IN_EXTEND_RECORDING.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "controlSessionId=9 correlationId=78 recordingId=1010101 streamId=43 sourceLocation=" +
            SourceLocation.LOCAL + " channel=extend me",
            sb.toString());
    }

    @Test
    void renderExtendRecording2()
    {
        final ExtendRecordingRequest2Encoder requestEncoder = new ExtendRecordingRequest2Encoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(9)
            .correlationId(78)
            .recordingId(1010101)
            .streamId(43)
            .sourceLocation(SourceLocation.LOCAL)
            .autoStop(BooleanType.TRUE)
            .channel("extend me");

        renderer.append(sb, ArchiveEventCode.CMD_IN_EXTEND_RECORDING2.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "controlSessionId=9 correlationId=78 recordingId=1010101 streamId=43 sourceLocation=" +
            SourceLocation.LOCAL + " autoStop=" + BooleanType.TRUE + " channel=extend me",
            sb.toString());
    }

    @Test
    void renderRecordingPosition()
    {
        final RecordingPositionRequestEncoder requestEncoder = new RecordingPositionRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(2)
            .correlationId(3)
            .recordingId(6);

        renderer.append(
            sb, ArchiveEventCode.CMD_IN_RECORDING_POSITION.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=2 correlationId=3 recordingId=6", sb.toString());
    }

    @Test
    void renderTruncateRecording()
    {
        final TruncateRecordingRequestEncoder requestEncoder = new TruncateRecordingRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(2)
            .correlationId(3)
            .recordingId(8)
            .position(1_000_000);

        renderer.append(
            sb, ArchiveEventCode.CMD_IN_TRUNCATE_RECORDING.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=2 correlationId=3 recordingId=8 position=1000000", sb.toString());
    }

    @Test
    void renderStopRecordingSubscription()
    {
        final StopRecordingSubscriptionRequestEncoder requestEncoder = new StopRecordingSubscriptionRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(22)
            .correlationId(33)
            .subscriptionId(888);

        renderer.append(
            sb, ArchiveEventCode.CMD_IN_STOP_RECORDING_SUBSCRIPTION.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=22 correlationId=33 subscriptionId=888", sb.toString());
    }

    @Test
    void renderStopRecordingByIdentity()
    {
        final StopRecordingByIdentityRequestEncoder requestEncoder = new StopRecordingByIdentityRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(22)
            .correlationId(33)
            .recordingId(777);

        renderer.append(
            sb, ArchiveEventCode.CMD_IN_STOP_RECORDING_BY_IDENTITY.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=22 correlationId=33 recordingId=777", sb.toString());
    }

    @Test
    void renderStopPosition()
    {
        final StopPositionRequestEncoder requestEncoder = new StopPositionRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(22)
            .correlationId(33)
            .recordingId(44);

        renderer.append(sb, ArchiveEventCode.CMD_IN_STOP_POSITION.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=22 correlationId=33 recordingId=44", sb.toString());
    }

    @Test
    void renderFindLastMatchingRecord()
    {
        final FindLastMatchingRecordingRequestEncoder requestEncoder = new FindLastMatchingRecordingRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(1)
            .correlationId(2)
            .minRecordingId(3)
            .sessionId(4)
            .streamId(5)
            .channel("this is a channel");

        renderer.append(
            sb, ArchiveEventCode.CMD_IN_FIND_LAST_MATCHING_RECORD.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "controlSessionId=1 correlationId=2 minRecordingId=3 sessionId=4 streamId=5 " +
            "channel=this is a channel",
            sb.toString());
    }

    @Test
    void renderListRecordingSubscriptions()
    {
        final ListRecordingSubscriptionsRequestEncoder requestEncoder =
            new ListRecordingSubscriptionsRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(1)
            .correlationId(2)
            .pseudoIndex(1111111)
            .applyStreamId(BooleanType.TRUE)
            .subscriptionCount(777)
            .streamId(555)
            .channel("ch2");

        renderer.append(
            sb, ArchiveEventCode.CMD_IN_LIST_RECORDING_SUBSCRIPTIONS.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "controlSessionId=1 correlationId=2 pseudoIndex=1111111 applyStreamId=" + BooleanType.TRUE +
            " subscriptionCount=777 streamId=555 channel=ch2",
            sb.toString());
    }

    @Test
    void renderStartBoundedReplay()
    {
        final BoundedReplayRequestEncoder requestEncoder = new BoundedReplayRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(10)
            .correlationId(20)
            .recordingId(30)
            .position(40)
            .length(50)
            .limitCounterId(-123)
            .replayStreamId(14)
            .replayChannel("rep ch");

        renderer.append(
            sb, ArchiveEventCode.CMD_IN_START_BOUNDED_REPLAY.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "controlSessionId=10 correlationId=20 recordingId=30 position=40 length=50 limitCounterId=-123 " +
            "replayStreamId=14 replayChannel=rep ch",
            sb.toString());
    }

    @Test
    void renderStopAllReplays()
    {
        final StopAllReplaysRequestEncoder requestEncoder = new StopAllReplaysRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(10)
            .correlationId(20)
            .recordingId(30);

        renderer.append(sb, ArchiveEventCode.CMD_IN_STOP_ALL_REPLAYS.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=10 correlationId=20 recordingId=30", sb.toString());
    }

    @Test
    void renderReplicate()
    {
        final ReplicateRequestEncoder requestEncoder = new ReplicateRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(2)
            .correlationId(5)
            .srcRecordingId(17)
            .dstRecordingId(2048)
            .srcControlStreamId(10)
            .srcControlChannel("CTRL ch")
            .liveDestination("live destination");

        renderer.append(sb, ArchiveEventCode.CMD_IN_REPLICATE.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "controlSessionId=2 correlationId=5 srcRecordingId=17 dstRecordingId=2048 srcControlStreamId=10 " +
            "srcControlChannel=CTRL ch liveDestination=live destination",
            sb.toString());
    }

    @Test
    void renderReplicate2()
    {
        final ReplicateRequest2Encoder requestEncoder = new ReplicateRequest2Encoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(2)
            .correlationId(5)
            .srcRecordingId(17)
            .dstRecordingId(2048)
            .stopPosition(4096)
            .channelTagId(123)
            .subscriptionTagId(321)
            .srcControlStreamId(10)
            .srcControlChannel("CTRL ch")
            .liveDestination("live destination")
            .replicationChannel("replication channel");

        renderer.append(sb, ArchiveEventCode.CMD_IN_REPLICATE2.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "controlSessionId=2 correlationId=5 srcRecordingId=17 dstRecordingId=2048 stopPosition=4096 " +
            "channelTagId=123 subscriptionTagId=321 srcControlStreamId=10 srcControlChannel=CTRL ch " +
            "liveDestination=live destination replicationChannel=replication channel",
            sb.toString());
    }

    @Test
    void renderStopReplication()
    {
        final StopReplicationRequestEncoder requestEncoder = new StopReplicationRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(-2)
            .correlationId(-5)
            .replicationId(-999);

        renderer.append(sb, ArchiveEventCode.CMD_IN_STOP_REPLICATION.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=-2 correlationId=-5 replicationId=-999", sb.toString());
    }

    @Test
    void renderStartPosition()
    {
        final StartPositionRequestEncoder requestEncoder = new StartPositionRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(3)
            .correlationId(16)
            .recordingId(1);

        renderer.append(sb, ArchiveEventCode.CMD_IN_START_POSITION.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=3 correlationId=16 recordingId=1", sb.toString());
    }

    @Test
    void renderDetachSegments()
    {
        final DetachSegmentsRequestEncoder requestEncoder = new DetachSegmentsRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(3)
            .correlationId(16)
            .recordingId(1);

        renderer.append(sb, ArchiveEventCode.CMD_IN_DETACH_SEGMENTS.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=3 correlationId=16 recordingId=1", sb.toString());
    }

    @Test
    void renderDeleteDetachedSegments()
    {
        final DeleteDetachedSegmentsRequestEncoder requestEncoder = new DeleteDetachedSegmentsRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(53)
            .correlationId(516)
            .recordingId(51);

        renderer.append(
            sb, ArchiveEventCode.CMD_IN_DELETE_DETACHED_SEGMENTS.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=53 correlationId=516 recordingId=51", sb.toString());
    }

    @Test
    void renderPurgeSegments()
    {
        final PurgeSegmentsRequestEncoder requestEncoder = new PurgeSegmentsRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(3)
            .correlationId(56)
            .recordingId(15)
            .newStartPosition(100);

        renderer.append(sb, ArchiveEventCode.CMD_IN_PURGE_SEGMENTS.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=3 correlationId=56 recordingId=15 newStartPosition=100", sb.toString());
    }

    @Test
    void renderAttachSegments()
    {
        final AttachSegmentsRequestEncoder requestEncoder = new AttachSegmentsRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(30)
            .correlationId(560)
            .recordingId(50);

        renderer.append(sb, ArchiveEventCode.CMD_IN_ATTACH_SEGMENTS.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=30 correlationId=560 recordingId=50", sb.toString());
    }

    @Test
    void renderMigrateSegments()
    {
        final MigrateSegmentsRequestEncoder requestEncoder = new MigrateSegmentsRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(7)
            .correlationId(6)
            .srcRecordingId(1)
            .dstRecordingId(21902);

        renderer.append(sb, ArchiveEventCode.CMD_IN_MIGRATE_SEGMENTS.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=7 correlationId=6 srcRecordingId=1 dstRecordingId=21902", sb.toString());
    }

    @Test
    void renderAuthConnect()
    {
        final AuthConnectRequestEncoder requestEncoder = new AuthConnectRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .correlationId(16)
            .responseStreamId(19)
            .version(2)
            .responseChannel("English Channel")
            .putEncodedCredentials("hello".getBytes(US_ASCII), 0, 5)
            .clientInfo("my test client \"ABC\" 42");

        renderer.append(sb, ArchiveEventCode.CMD_IN_AUTH_CONNECT.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "correlationId=16 responseStreamId=19 version=2 responseChannel=English Channel " +
            "encodedCredentialsLength=5 clientInfo=my test client \"ABC\" 42",
            sb.toString());
    }

    @Test
    void renderKeepAlive()
    {
        final KeepAliveRequestEncoder requestEncoder = new KeepAliveRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(31)
            .correlationId(119);

        renderer.append(sb, ArchiveEventCode.CMD_IN_KEEP_ALIVE.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=31 correlationId=119", sb.toString());
    }

    @Test
    void renderTaggedReplicate()
    {
        final TaggedReplicateRequestEncoder requestEncoder = new TaggedReplicateRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(1)
            .correlationId(-10)
            .srcRecordingId(9)
            .dstRecordingId(31)
            .channelTagId(4)
            .subscriptionTagId(7)
            .srcControlStreamId(15)
            .srcControlChannel("src")
            .liveDestination("alive and well");

        renderer.append(sb, ArchiveEventCode.CMD_IN_TAGGED_REPLICATE.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "controlSessionId=1 correlationId=-10 srcRecordingId=9 dstRecordingId=31 channelTagId=4 " +
            "subscriptionTagId=7 srcControlStreamId=15 srcControlChannel=src liveDestination=alive and well",
            sb.toString());
    }

    @Test
    void renderPurgeRecording()
    {
        final PurgeRecordingRequestEncoder requestEncoder = new PurgeRecordingRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(15)
            .correlationId(421)
            .recordingId(6);

        renderer.append(sb, ArchiveEventCode.CMD_IN_PURGE_RECORDING.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("controlSessionId=15 correlationId=421 recordingId=6", sb.toString());
    }

    @Test
    void renderControlResponse()
    {
        final ControlResponseEncoder responseEncoder = new ControlResponseEncoder();
        responseEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(13)
            .correlationId(42)
            .relevantId(8)
            .code(NULL_VAL)
            .version(111)
            .errorMessage("the %ERR% msg");

        renderer.append(sb, ArchiveEventCode.CMD_OUT_RESPONSE.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "controlSessionId=13 correlationId=42 relevantId=8 code=" + NULL_VAL +
            " version=111 errorMessage=the %ERR% msg",
            sb.toString());
    }

    @Test
    void renderRecordingSignal()
    {
        final RecordingSignalEventEncoder encoder = new RecordingSignalEventEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .controlSessionId(49)
            .correlationId(-100)
            .recordingId(42)
            .subscriptionId(15)
            .position(234723197419023749L)
            .signal(RecordingSignal.DELETE);

        renderer.append(sb, ArchiveEventCode.RECORDING_SIGNAL.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "controlSessionId=49 correlationId=-100 recordingId=42 subscriptionId=15 " +
            "position=234723197419023749 signal=DELETE",
            sb.toString());
    }

    @Test
    void renderUnknownCommand()
    {
        renderer.append(
            sb, ArchiveEventCode.REPLICATION_SESSION_DONE.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("unknown command", sb.toString());
    }

    @ParameterizedTest
    @ValueSource(ints = { MessageHeaderDecoder.ENCODED_LENGTH - 1, MessageHeaderDecoder.ENCODED_LENGTH + 1 })
    void renderDoesNotThrowWhenBufferIsShort(final int bufferLength)
    {
        final UnsafeBuffer shortBuffer = new UnsafeBuffer(new byte[bufferLength]);

        for (final int msgTypeId : renderer.supportingMsgTypeIds())
        {
            sb.setLength(0);
            renderer.append(sb, msgTypeId, shortBuffer, 0, shortBuffer.capacity());
        }
    }
}
