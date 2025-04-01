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
package io.aeron.driver;

import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.buffer.TestLogFactory;
import io.aeron.driver.media.ControlTransportPoller;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.HeaderWriter;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.SetupFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.AtomicLongPosition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.FrameDescriptor.frameLengthOrdered;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.agrona.BitUtil.align;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class SenderTest
{
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int MAX_FRAME_LENGTH = 1024;
    private static final int SESSION_ID = 1;
    private static final int STREAM_ID = 1002;
    private static final int INITIAL_TERM_ID = 3;
    private static final byte[] PAYLOAD = "Payload is here!".getBytes();

    private static final UnsafeBuffer HEADER = DataHeaderFlyweight.createDefaultHeader(
        SESSION_ID, STREAM_ID, INITIAL_TERM_ID);
    private static final int FRAME_LENGTH = HEADER.capacity() + PAYLOAD.length;
    private static final int ALIGNED_FRAME_LENGTH = align(FRAME_LENGTH, FRAME_ALIGNMENT);

    private final ControlTransportPoller mockTransportPoller = mock(ControlTransportPoller.class);

    private final RawLog rawLog = TestLogFactory.newLogBuffers(TERM_BUFFER_LENGTH);
    private NetworkPublication publication;
    private Sender sender;

    private final FlowControl flowControl = spy(new UnicastFlowControl());
    private final RetransmitHandler mockRetransmitHandler = mock(RetransmitHandler.class);
    private final DriverConductorProxy mockDriverConductorProxy = mock(DriverConductorProxy.class);

    private final CachedNanoClock nanoClock = new CachedNanoClock();

    private final Queue<ByteBuffer> receivedFrames = new ArrayDeque<>();

    private final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?endpoint=localhost:40123");
    private final InetSocketAddress rcvAddress = udpChannel.remoteData();
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final SetupFlyweight setupHeader = new SetupFlyweight();
    private final SystemCounters mockSystemCounters = mock(SystemCounters.class);
    private final OneToOneConcurrentArrayQueue<Runnable> senderCommandQueue =
        new OneToOneConcurrentArrayQueue<>(Configuration.CMD_QUEUE_CAPACITY);

    private final HeaderWriter headerWriter = HeaderWriter.newInstance(HEADER);

    private final Answer<Integer> saveByteBufferAnswer =
        (invocation) ->
        {
            final Object[] args = invocation.getArguments();
            final ByteBuffer buffer = (ByteBuffer)args[0];

            final int length = buffer.limit() - buffer.position();
            receivedFrames.add(ByteBuffer.allocateDirect(length).put(buffer));

            // we don't pass on the args, so don't reset buffer.position() back
            return length;
        };

    private final ErrorHandler errorHandler = mock(ErrorHandler.class);

    @BeforeEach
    void setUp()
    {
        final SendChannelEndpoint mockSendChannelEndpoint = mock(SendChannelEndpoint.class);
        when(mockSendChannelEndpoint.udpChannel()).thenReturn(udpChannel);
        when(mockSendChannelEndpoint.send(any())).thenAnswer(saveByteBufferAnswer);
        when(mockSystemCounters.get(any())).thenReturn(mock(AtomicCounter.class));

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .cachedEpochClock(new CachedEpochClock())
            .cachedNanoClock(nanoClock)
            .senderCachedNanoClock(nanoClock)
            .receiverCachedNanoClock(nanoClock)
            .controlTransportPoller(mockTransportPoller)
            .systemCounters(mockSystemCounters)
            .senderCommandQueue(senderCommandQueue)
            .nanoClock(nanoClock)
            .errorHandler(errorHandler)
            .senderDutyCycleTracker(new DutyCycleTracker());
        sender = new Sender(ctx);

        LogBufferDescriptor.initialiseTailWithTermId(rawLog.metaData(), 0, INITIAL_TERM_ID);

        final PublicationParams params = new PublicationParams();
        params.entityTag = 101;
        params.mtuLength = MAX_FRAME_LENGTH;
        params.lingerTimeoutNs = Configuration.publicationLingerTimeoutNs();
        params.signalEos = true;

        publication = new NetworkPublication(
            1,
            ctx,
            params,
            mockSendChannelEndpoint,
            rawLog,
            Configuration.producerWindowLength(TERM_BUFFER_LENGTH, Configuration.publicationTermWindowLength()),
            new AtomicLongPosition(),
            new AtomicLongPosition(),
            new AtomicLongPosition(),
            new AtomicLongPosition(),
            mock(AtomicCounter.class),
            SESSION_ID,
            STREAM_ID,
            INITIAL_TERM_ID,
            flowControl,
            mockRetransmitHandler,
            new NetworkPublicationThreadLocals(),
            false);

        assertTrue(senderCommandQueue.offer(() -> sender.onNewNetworkPublication(publication)));
    }

    @AfterEach
    void tearDown()
    {
        sender.onClose();
    }

    @Test
    void shouldSendSetupFrameOnChannelWhenTimeoutWithoutStatusMessage()
    {
        sender.doWork();
        assertThat(receivedFrames.size(), is(1));
        nanoClock.advance(Configuration.PUBLICATION_SETUP_TIMEOUT_NS - 1);
        sender.doWork();
        assertThat(receivedFrames.size(), is(1));

        nanoClock.advance(10);
        sender.doWork();
        assertThat(receivedFrames.size(), is(2));

        setupHeader.wrap(new UnsafeBuffer(receivedFrames.remove()));
        assertThat(setupHeader.frameLength(), is(SetupFlyweight.HEADER_LENGTH));
        assertThat(setupHeader.initialTermId(), is(INITIAL_TERM_ID));
        assertThat(setupHeader.activeTermId(), is(INITIAL_TERM_ID));
        assertThat(setupHeader.streamId(), is(STREAM_ID));
        assertThat(setupHeader.sessionId(), is(SESSION_ID));
        assertThat(setupHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_SETUP));
        assertThat(setupHeader.flags(), is((short)0));
        assertThat(setupHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
    }

    @Test
    void shouldSendMultipleSetupFramesOnChannelWhenTimeoutWithoutStatusMessage()
    {
        sender.doWork();
        assertThat(receivedFrames.size(), is(1));

        nanoClock.advance(Configuration.PUBLICATION_SETUP_TIMEOUT_NS - 1);
        sender.doWork();

        nanoClock.advance(10);
        sender.doWork();

        assertThat(receivedFrames.size(), is(2));
    }

    @Test
    void shouldNotSendSetupFrameOnlyOnceAfterReceivingStatusMessage()
    {
        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(INITIAL_TERM_ID);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(0);

        publication.onStatusMessage(msg, rcvAddress, mockDriverConductorProxy);
        sender.doWork();

        assertThat(receivedFrames.size(), is(1));
        dataHeader.wrap(receivedFrames.remove());
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA)); // heartbeat
        assertThat(dataHeader.frameLength(), is(0));

        nanoClock.advance(Configuration.PUBLICATION_SETUP_TIMEOUT_NS + 10);
        sender.doWork();

        assertThat(receivedFrames.size(), is(1));
        dataHeader.wrap(receivedFrames.remove());
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA)); // heartbeat
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
    }

    @Test
    void shouldSendSetupFrameAfterReceivingStatusMessageWithSetupBit()
    {
        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(INITIAL_TERM_ID);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(ALIGNED_FRAME_LENGTH);

        sender.doWork();

        assertThat(receivedFrames.size(), is(1)); // setup frame
        receivedFrames.remove();

        publication.onStatusMessage(msg, rcvAddress, mockDriverConductorProxy);

        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        appendUnfragmentedMessage(rawLog, 0, INITIAL_TERM_ID, 0, headerWriter, buffer, 0, PAYLOAD.length);
        sender.doWork();

        assertThat(receivedFrames.size(), is(1)); // data
        receivedFrames.remove();

        publication.triggerSendSetupFrame(msg, rcvAddress);

        sender.doWork();
        assertThat(receivedFrames.size(), is(0)); // setup has been sent already, have to wait

        nanoClock.advance(Configuration.PUBLICATION_SETUP_TIMEOUT_NS + 10);
        sender.doWork();

        assertThat(receivedFrames.size(), is(2));

        setupHeader.wrap(new UnsafeBuffer(receivedFrames.remove()));
        assertThat(setupHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_SETUP));
        dataHeader.wrap(receivedFrames.remove());
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.frameLength(), is(0));
    }

    @Test
    void shouldSendHeartbeatsEvenIfSendingPeriodicSetupFrames()
    {
        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(INITIAL_TERM_ID);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(ALIGNED_FRAME_LENGTH);

        publication.onStatusMessage(msg, rcvAddress, mockDriverConductorProxy);

        sender.doWork();

        assertThat(receivedFrames.size(), is(1)); // heartbeat
        receivedFrames.remove();

        publication.triggerSendSetupFrame(msg, rcvAddress);
        nanoClock.advance(Configuration.PUBLICATION_SETUP_TIMEOUT_NS + 10);
        sender.doWork();

        assertThat(receivedFrames.size(), is(2));

        setupHeader.wrap(new UnsafeBuffer(receivedFrames.remove()));
        assertThat(setupHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_SETUP));
        dataHeader.wrap(receivedFrames.remove());
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA)); // heartbeat is sent after setup
        assertThat(dataHeader.frameLength(), is(0));
    }

    @Test
    void shouldBeAbleToSendOnChannel()
    {
        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(INITIAL_TERM_ID);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(ALIGNED_FRAME_LENGTH);

        publication.onStatusMessage(msg, rcvAddress, mockDriverConductorProxy);

        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        appendUnfragmentedMessage(rawLog, 0, INITIAL_TERM_ID, 0, headerWriter, buffer, 0, PAYLOAD.length);
        sender.doWork();

        assertThat(receivedFrames.size(), is(1));

        dataHeader.wrap(new UnsafeBuffer(receivedFrames.remove()));
        assertThat(dataHeader.frameLength(), is(FRAME_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.streamId(), is(STREAM_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
    }

    @Test
    void shouldBeAbleToSendOnChannelTwice()
    {
        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(INITIAL_TERM_ID);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(2 * ALIGNED_FRAME_LENGTH);

        publication.onStatusMessage(msg, rcvAddress, mockDriverConductorProxy);

        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        final int offset = appendUnfragmentedMessage(
            rawLog, 0, INITIAL_TERM_ID, 0, headerWriter, buffer, 0, PAYLOAD.length);

        sender.doWork();

        appendUnfragmentedMessage(rawLog, 0, INITIAL_TERM_ID, offset, headerWriter, buffer, 0, PAYLOAD.length);

        sender.doWork();

        assertThat(receivedFrames.size(), is(2));

        dataHeader.wrap(new UnsafeBuffer(receivedFrames.remove()));
        assertThat(dataHeader.frameLength(), is(FRAME_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.streamId(), is(STREAM_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));

        dataHeader.wrap(new UnsafeBuffer(receivedFrames.remove()));
        assertThat(dataHeader.frameLength(), is(FRAME_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.streamId(), is(STREAM_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(2)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
    }

    @Test
    void shouldNotSendUntilStatusMessageReceived()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);
        appendUnfragmentedMessage(rawLog, 0, INITIAL_TERM_ID, 0, headerWriter, buffer, 0, PAYLOAD.length);

        sender.doWork();
        assertThat(receivedFrames.size(), is(1));
        setupHeader.wrap(receivedFrames.remove());
        assertThat(setupHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_SETUP));

        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(INITIAL_TERM_ID);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(ALIGNED_FRAME_LENGTH);

        publication.onStatusMessage(msg, rcvAddress, mockDriverConductorProxy);
        sender.doWork();

        assertThat(receivedFrames.size(), is(1));

        dataHeader.wrap(new UnsafeBuffer(receivedFrames.remove()));

        assertThat(dataHeader.frameLength(), is(FRAME_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.streamId(), is(STREAM_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
    }

    @Test
    void shouldNotBeAbleToSendAfterUsingUpYourWindow()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);
        appendUnfragmentedMessage(rawLog, 0, INITIAL_TERM_ID, 0, headerWriter, buffer, 0, PAYLOAD.length);

        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(INITIAL_TERM_ID);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(ALIGNED_FRAME_LENGTH);

        publication.onStatusMessage(msg, rcvAddress, mockDriverConductorProxy);

        sender.doWork();

        assertThat(receivedFrames.size(), is(1));

        dataHeader.wrap(new UnsafeBuffer(receivedFrames.remove()));
        assertThat(dataHeader.frameLength(), is(FRAME_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.streamId(), is(STREAM_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));

        appendUnfragmentedMessage(rawLog, 0, INITIAL_TERM_ID, 0, headerWriter, buffer, 0, PAYLOAD.length);

        sender.doWork();

        assertThat(receivedFrames.size(), is(0));
    }

    @Test
    void shouldSendLastDataFrameAsHeartbeatWhenIdle()
    {
        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(INITIAL_TERM_ID);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(ALIGNED_FRAME_LENGTH);

        publication.onStatusMessage(msg, rcvAddress, mockDriverConductorProxy);

        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        appendUnfragmentedMessage(rawLog, 0, INITIAL_TERM_ID, 0, headerWriter, buffer, 0, PAYLOAD.length);

        sender.doWork();

        assertThat(receivedFrames.size(), is(1));  // should send ticks
        receivedFrames.remove();                   // skip data frame

        nanoClock.advance(Configuration.PUBLICATION_HEARTBEAT_TIMEOUT_NS - 1);
        sender.doWork();

        assertThat(receivedFrames.size(), is(0));  // should not send yet
        nanoClock.advance(10);
        sender.doWork();

        assertThat(receivedFrames.size(), greaterThanOrEqualTo(1));  // should send ticks

        dataHeader.wrap(receivedFrames.remove());
        assertThat(dataHeader.frameLength(), is(0));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(2)));
    }

    @Test
    void shouldSendMultipleDataFramesAsHeartbeatsWhenIdle()
    {
        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(INITIAL_TERM_ID);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(ALIGNED_FRAME_LENGTH);

        publication.onStatusMessage(msg, rcvAddress, mockDriverConductorProxy);

        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        appendUnfragmentedMessage(rawLog, 0, INITIAL_TERM_ID, 0, headerWriter, buffer, 0, PAYLOAD.length);

        sender.doWork();

        assertThat(receivedFrames.size(), is(1));  // should send ticks
        receivedFrames.remove();                   // skip data (heartbeat) frame

        nanoClock.advance(Configuration.PUBLICATION_HEARTBEAT_TIMEOUT_NS - 1);
        sender.doWork();
        assertThat(receivedFrames.size(), is(0));  // should not send yet

        nanoClock.advance(10);
        sender.doWork();
        assertThat(receivedFrames.size(), greaterThanOrEqualTo(1));  // should send ticks

        dataHeader.wrap(receivedFrames.remove());
        assertThat(dataHeader.frameLength(), is(0));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(2)));

        nanoClock.advance(Configuration.PUBLICATION_HEARTBEAT_TIMEOUT_NS - 1);
        sender.doWork();
        assertThat(receivedFrames.size(), is(0));  // should not send yet

        nanoClock.advance(10);
        sender.doWork();
        assertThat(receivedFrames.size(), greaterThanOrEqualTo(1));  // should send ticks

        dataHeader.wrap(receivedFrames.remove());
        assertThat(dataHeader.frameLength(), is(0));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(2)));
    }

    private int offsetOfMessage(final int offset)
    {
        return (offset - 1) * align(HEADER.capacity() + PAYLOAD.length, FRAME_ALIGNMENT);
    }

    private int appendUnfragmentedMessage(
        final RawLog rawLog,
        final int partitionIndex,
        final int termId,
        final int termOffset,
        final HeaderWriter header,
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int length)
    {
        final UnsafeBuffer termBuffer = rawLog.termBuffers()[partitionIndex];
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int resultingOffset = termOffset + alignedLength;
        final long rawTail = LogBufferDescriptor.packTail(termId, resultingOffset);

        LogBufferDescriptor.rawTail(rawLog.metaData(), partitionIndex, rawTail);

        header.write(termBuffer, termOffset, frameLength, termId);
        termBuffer.putBytes(termOffset + HEADER_LENGTH, srcBuffer, srcOffset, length);

        frameLengthOrdered(termBuffer, termOffset, frameLength);

        return resultingOffset;
    }
}
