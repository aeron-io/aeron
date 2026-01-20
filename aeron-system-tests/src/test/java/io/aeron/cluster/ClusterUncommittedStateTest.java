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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.NewLeadershipTermEventEncoder;
import io.aeron.cluster.codecs.SessionCloseEventEncoder;
import io.aeron.cluster.codecs.SessionOpenEventEncoder;
import io.aeron.cluster.service.ClientSession;
import io.aeron.driver.Configuration;
import io.aeron.driver.DataPacketDispatcher;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ReceiveChannelEndpointSupplier;
import io.aeron.driver.SendChannelEndpointSupplier;
import io.aeron.driver.ext.DebugReceiveChannelEndpoint;
import io.aeron.driver.ext.DebugSendChannelEndpoint;
import io.aeron.driver.ext.LossGenerator;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.IntArrayList;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.computeFragmentedFrameLength;
import static io.aeron.test.cluster.TestCluster.aCluster;
import static io.aeron.test.driver.TestMediaDriver.shouldRunJavaMediaDriver;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class ClusterUncommittedStateTest
{
    private static final int NODE_COUNT = 3;

    private static final int EIGHT_MEGABYTES = 8 * 1024 * 1024;
    private static final int FRAME_LENGTH = Configuration.mtuLength() - DataHeaderFlyweight.HEADER_LENGTH;
    static final int EIGHT_MEGABYTES_LENGTH = computeFragmentedFrameLength(EIGHT_MEGABYTES, FRAME_LENGTH);
    static final int SESSION_CLOSE_LENGTH = computeFragmentedFrameLength(
        MessageHeaderEncoder.ENCODED_LENGTH + SessionCloseEventEncoder.BLOCK_LENGTH, FRAME_LENGTH);
    static final int SESSION_OPEN_BLOCK_LENGTH = computeFragmentedFrameLength(
        MessageHeaderEncoder.ENCODED_LENGTH + SessionOpenEventEncoder.BLOCK_LENGTH, FRAME_LENGTH);
    static final int NEW_LEADERSHIP_TERM_LENGTH = computeFragmentedFrameLength(
        MessageHeaderEncoder.ENCODED_LENGTH + NewLeadershipTermEventEncoder.BLOCK_LENGTH, FRAME_LENGTH);

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();
    private TestCluster cluster;
    private final ToggledLossControl[] toggledLossControls = new ToggledLossControl[NODE_COUNT];

    @BeforeEach
    void setUp()
    {
        for (int i = 0; i < NODE_COUNT; ++i)
        {
            toggledLossControls[i] = new ToggledLossControl();
        }
    }

    @Test
    @SlowTest
    @InterruptAfter(20)
    void shouldElectionBetweenFragmentedServiceMessageAvoidDuplicateServiceMessage()
    {
        assumeTrue(shouldRunJavaMediaDriver());

        final AtomicBoolean waitingToOfferFragmentedMessage = new AtomicBoolean(true);
        final AtomicBoolean leaderBackPressureLog = new AtomicBoolean(false);
        final AtomicBoolean followerBackPressureLog = new AtomicBoolean(true);
        final IntFunction<TestNode.TestService[]> serviceSupplier = (index) ->
        {
            if (0 == index)
            {
                return new TestNode.TestService[]
                {
                    new OfferMessageOnSessionCloseService(waitingToOfferFragmentedMessage, leaderBackPressureLog)
                };
            }
            else
            {
                return new TestNode.TestService[]
                {
                    new OfferMessageOnSessionCloseService(waitingToOfferFragmentedMessage, followerBackPressureLog)
                };
            }
        };

        cluster = aCluster()
            .withStaticNodes(NODE_COUNT)
            .withReceiveChannelEndpointSupplier((index) -> toggledLossControls[index])
            .withSendChannelEndpointSupplier((index) -> toggledLossControls[index])
            .withAppointedLeader(0)
            .withLogChannel("aeron:udp?term-length=64m|alias=raft")
            .withControlChannel("aeron:ipc")
            .withServiceSupplier(serviceSupplier)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final TestNode follower1 = cluster.node(1);
        final TestNode follower2 = cluster.node(2);
        final ToggledLossControl leaderLossControl = toggledLossControls[leader.memberId()];

        CloseHelper.close(cluster.connectClient());
        final long expectedPositionLowerBound =
            NEW_LEADERSHIP_TERM_LENGTH + SESSION_OPEN_BLOCK_LENGTH + SESSION_CLOSE_LENGTH;
        Tests.await(() ->
        {
            final long leaderCommitPosition = leader.commitPosition();
            return leaderCommitPosition > expectedPositionLowerBound &&
                leaderCommitPosition == follower1.commitPosition() &&
                leaderCommitPosition == follower2.commitPosition();
        });

        final long commitPositionBeforeFragmentedMessage = leader.commitPosition();
        waitingToOfferFragmentedMessage.set(false);
        Tests.await(() ->
        {
            final long leaderCommitPosition = leader.commitPosition();
            return leaderCommitPosition > (commitPositionBeforeFragmentedMessage) &&
                leaderCommitPosition < (commitPositionBeforeFragmentedMessage + EIGHT_MEGABYTES);
        });

        final long expectedAppendPosition = commitPositionBeforeFragmentedMessage + EIGHT_MEGABYTES_LENGTH;
        Tests.await(() -> leader.appendPosition() == expectedAppendPosition);

        leaderLossControl.toggleLoss(true);
        Tests.await(() -> 0 < leaderLossControl.droppedOutboundFrames.get() &&
            0 < leaderLossControl.droppedInboundFrames.get());

        Tests.await(() -> ElectionState.CLOSED != leader.electionState());
        assertTrue(leader.commitPosition() > commitPositionBeforeFragmentedMessage &&
            leader.commitPosition() < (commitPositionBeforeFragmentedMessage + EIGHT_MEGABYTES));

        leaderLossControl.toggleLoss(false);
        followerBackPressureLog.set(false);

        CloseHelper.close(cluster.connectClient());

        Tests.await(() ->
        {
            for (int i = 0; i < cluster.memberCount(); ++i)
            {
                final OfferMessageOnSessionCloseService service =
                    (OfferMessageOnSessionCloseService)cluster.node(i).container(0).context().clusteredService();
                if (2 != service.receivedMessage.get())
                {
                    return false;
                }
            }
            return true;
        });

        for (int i = 0; i < cluster.memberCount(); ++i)
        {
            final OfferMessageOnSessionCloseService service =
                (OfferMessageOnSessionCloseService)cluster.node(i).container(0).context().clusteredService();
            assertEquals(2, service.sentMessages.size(), "Node " + i + " sent " + service.sentMessages);
            assertEquals(2, service.receivedMessages.size(), "Node " + i + " received " + service.receivedMessages);
        }
    }

    static class OfferMessageOnSessionCloseService extends TestNode.TestService
    {
        private final AtomicBoolean waitingToOfferServiceMessage;
        private final AtomicBoolean causeBackPressure;

        private final UnsafeBuffer messageBuffer = new UnsafeBuffer(new byte[EIGHT_MEGABYTES - SESSION_HEADER_LENGTH]);
        private int nextSentMessage = 1;
        private final IntArrayList sentMessages = new IntArrayList(8, Aeron.NULL_VALUE);
        private final IntArrayList receivedMessages = new IntArrayList(8, Aeron.NULL_VALUE);
        private final AtomicInteger receivedMessage = new AtomicInteger(0);


        OfferMessageOnSessionCloseService(
            final AtomicBoolean waitingToOfferServiceMessage,
            final AtomicBoolean causeBackPressure)
        {
            this.waitingToOfferServiceMessage = waitingToOfferServiceMessage;
            this.causeBackPressure = causeBackPressure;
        }

        public void onSessionOpen(final ClientSession session, final long timestamp)
        {
        }

        public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
        {
            cluster.idleStrategy().reset();
            while (waitingToOfferServiceMessage.get())
            {
                cluster.idleStrategy().idle();
            }

            messageBuffer.putInt(0, nextSentMessage, MessageHeaderEncoder.BYTE_ORDER);
            while (0 > cluster.offer(messageBuffer, 0, messageBuffer.capacity()))
            {
                cluster.idleStrategy().idle();
            }
            sentMessages.add(nextSentMessage++);

            while (causeBackPressure.get())
            {
                cluster.idleStrategy().idle();
            }
        }

        public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            final int receiveMessage = buffer.getInt(offset, MessageHeaderEncoder.BYTE_ORDER);
            receivedMessages.add(receiveMessage);
            receivedMessage.set(receiveMessage);
        }

        public void onTimerEvent(final long correlationId, final long timestamp)
        {
        }

        public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
        {
        }
    }

    private static final class ToggledLossControl implements ReceiveChannelEndpointSupplier, SendChannelEndpointSupplier
    {
        final AtomicBoolean shouldDropOutboundFrames = new AtomicBoolean(false);
        final AtomicInteger droppedOutboundFrames = new AtomicInteger(0);
        final ToggledLossGenerator outboundLossGenerator = new ToggledLossGenerator(
            shouldDropOutboundFrames, droppedOutboundFrames);

        final AtomicBoolean shouldDropInboundFrames = new AtomicBoolean(false);
        final AtomicInteger droppedInboundFrames = new AtomicInteger(0);
        final ToggledLossGenerator inboundLossGenerator = new ToggledLossGenerator(
            shouldDropInboundFrames, droppedInboundFrames);

        public ReceiveChannelEndpoint newInstance(
            final UdpChannel udpChannel,
            final DataPacketDispatcher dispatcher,
            final AtomicCounter statusIndicator,
            final MediaDriver.Context context)
        {
            return new DebugReceiveChannelEndpoint(
                udpChannel, dispatcher, statusIndicator, context, inboundLossGenerator, inboundLossGenerator);
        }

        public SendChannelEndpoint newInstance(
            final UdpChannel udpChannel,
            final AtomicCounter statusIndicator,
            final MediaDriver.Context context)
        {
            return new DebugSendChannelEndpoint(
                udpChannel, statusIndicator, context, outboundLossGenerator, outboundLossGenerator);
        }

        void toggleLoss(final boolean loss)
        {
            shouldDropOutboundFrames.set(loss);
            shouldDropInboundFrames.set(loss);
        }

        private static final class ToggledLossGenerator implements LossGenerator
        {
            private final AtomicBoolean shouldDropFrame;
            private final AtomicInteger droppedFrames;

            private ToggledLossGenerator(final AtomicBoolean shouldDropFrame, final AtomicInteger droppedFrames)
            {
                this.shouldDropFrame = shouldDropFrame;
                this.droppedFrames = droppedFrames;
            }

            public boolean shouldDropFrame(final InetSocketAddress address, final UnsafeBuffer buffer, final int length)
            {
                droppedFrames.incrementAndGet();
                return shouldDropFrame.get();
            }

            public boolean shouldDropFrame(
                final InetSocketAddress address,
                final UnsafeBuffer buffer,
                final int streamId,
                final int sessionId,
                final int termId,
                final int termOffset,
                final int length)
            {
                droppedFrames.incrementAndGet();
                return shouldDropFrame.get();
            }
        }
    }
}
