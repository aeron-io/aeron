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
package io.aeron.test.cluster;

import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveTool;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.ClusterMembership;
import io.aeron.cluster.ClusterTool;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.ElectionState;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterTerminationException;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.test.DataCollector;
import io.aeron.test.Tests;
import io.aeron.test.driver.RedirectingNameResolver;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Hashing;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.LongArrayList;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntPredicate;
import java.util.zip.CRC32;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.junit.jupiter.api.Assertions.fail;

public final class TestNode implements AutoCloseable
{
    private final Archive archive;
    private final ConsensusModule consensusModule;
    private final ClusteredServiceContainer[] containers;
    private final TestService[] services;
    private final Context context;
    private final TestMediaDriver mediaDriver;
    private boolean isClosed = false;

    TestNode(final Context context, final DataCollector dataCollector)
    {
        this.context = context;

        try
        {
            mediaDriver = TestMediaDriver.launch(
                context.mediaDriverContext, TestCluster.clientDriverOutputConsumer(dataCollector));

            final String aeronDirectoryName = mediaDriver.context().aeronDirectoryName();
            archive = Archive.launch(context.archiveContext.aeronDirectoryName(aeronDirectoryName));

            services = context.services;

            context.consensusModuleContext
                .serviceCount(services.length)
                .aeronDirectoryName(aeronDirectoryName)
                .isIpcIngressAllowed(true)
                .terminationHook(ClusterTests.terminationHook(
                context.isTerminationExpected, context.hasMemberTerminated));

            final AeronArchive.Context archiveContext = context.consensusModuleContext.archiveContext().clone();

            consensusModule = ConsensusModule.launch(context.consensusModuleContext);
            final File baseDir = context.consensusModuleContext.clusterDir().getParentFile();
            dataCollector.addForCleanup(baseDir);

            containers = new ClusteredServiceContainer[services.length];
            for (int i = 0; i < services.length; i++)
            {
                final ClusteredServiceContainer.Context ctx = context.serviceContainerContext.clone();

                final int clusterId = context.consensusModuleContext.clusterId();
                final int memberId = context.consensusModuleContext.clusterMemberId();

                ctx.aeronDirectoryName(aeronDirectoryName)
                    .archiveContext(archiveContext.clone())
                    .terminationHook(ClusterTests.terminationHook(
                        context.isTerminationExpected, context.hasServiceTerminated[i]))
                    .clusterDir(context.consensusModuleContext.clusterDir())
                    .markFileDir(context.consensusModuleContext.markFileDir())
                    .clusteredService(services[i])
                    .snapshotDurationThresholdNs(TimeUnit.MILLISECONDS.toNanos(100))
                    .serviceName("clustered-service-" + clusterId + "-" + memberId + "-" + i)
                    .serviceId(i);
                containers[i] = ClusteredServiceContainer.launch(ctx);
            }

            dataCollector.add(consensusModule.context().clusterDir().toPath());
            dataCollector.add(archive.context().archiveDir().toPath());
            dataCollector.add(mediaDriver.context().aeronDirectory().toPath());
        }
        catch (final RuntimeException ex)
        {
            try
            {
                close();
            }
            catch (final Exception e)
            {
                ex.addSuppressed(e);
            }

            throw ex;
        }
    }

    public void stopServiceContainers()
    {
        CloseHelper.closeAll(containers);
        for (final AtomicBoolean terminationFlag : context.hasServiceTerminated)
        {
            terminationFlag.set(true);
        }
    }

    public TestMediaDriver mediaDriver()
    {
        return mediaDriver;
    }

    public Archive archive()
    {
        return archive;
    }

    public ConsensusModule consensusModule()
    {
        return consensusModule;
    }

    public ClusteredServiceContainer container()
    {
        if (1 != containers.length)
        {
            throw new IllegalStateException("container count expected=1 actual=" + containers.length);
        }

        return container(0);
    }

    public ClusteredServiceContainer container(final int index)
    {
        return containers[index];
    }

    public TestService service()
    {
        if (1 != services.length)
        {
            throw new IllegalStateException("service count expected=1 actual=" + services.length);
        }

        return services[0];
    }

    public TestService[] services()
    {
        return services;
    }

    public void close()
    {
        if (!isClosed)
        {
            isClosed = true;
            CloseHelper.closeAll(consensusModule, () -> CloseHelper.closeAll(containers), archive, mediaDriver);
        }
    }

    public boolean isClosed()
    {
        return isClosed;
    }

    public void gracefulClose()
    {
        CloseHelper.closeAll(consensusModule, () -> CloseHelper.closeAll(containers));
    }

    public Cluster.Role role()
    {
        final Counter roleCounter = consensusModule.context().clusterNodeRoleCounter();
        if (!roleCounter.isClosed())
        {
            return Cluster.Role.get(roleCounter);
        }
        return Cluster.Role.FOLLOWER;
    }

    public void awaitElectionState(final ElectionState electionState)
    {
        while (electionState() != electionState)
        {
            Tests.sleep(1);
        }
    }

    public ElectionState electionState()
    {
        return ElectionState.get(consensusModule.context().electionStateCounter());
    }

    ConsensusModule.State moduleState()
    {
        return ConsensusModule.State.get(consensusModule.context().moduleStateCounter());
    }

    public long commitPosition()
    {
        final Counter counter = consensusModule.context().commitPositionCounter();
        if (counter.isClosed())
        {
            return NULL_POSITION;
        }

        return counter.get();
    }

    public long appendPosition()
    {
        final long recordingId = consensusModule().context().recordingLog().findLastTermRecordingId();
        if (RecordingPos.NULL_RECORDING_ID == recordingId)
        {
            fail("no recording for last term");
        }

        final CountersReader countersReader = countersReader();
        final int counterId =
            RecordingPos.findCounterIdByRecording(countersReader, recordingId, archive.context().archiveId());
        if (NULL_VALUE == counterId)
        {
            ArchiveTool.describeRecording(System.out, archive().context().archiveDir(), recordingId);
            fail("recording not active " + recordingId);
        }

        return countersReader.getCounterValue(counterId);
    }

    boolean isLeader()
    {
        return role() == Cluster.Role.LEADER && moduleState() != ConsensusModule.State.CLOSED;
    }

    boolean isFollower()
    {
        return role() == Cluster.Role.FOLLOWER;
    }

    public void isTerminationExpected(final boolean isTerminationExpected)
    {
        context.isTerminationExpected.set(isTerminationExpected);
    }

    boolean hasServiceTerminated()
    {
        if (1 != services.length)
        {
            throw new IllegalStateException("service count expected=1 actual=" + services.length);
        }

        return context.hasServiceTerminated[0].get();
    }

    public boolean hasMemberTerminated()
    {
        return context.hasMemberTerminated.get();
    }

    public int index()
    {
        return services[0].index();
    }

    CountersReader countersReader()
    {
        return mediaDriver.counters();
    }

    public ClusterMembership clusterMembership()
    {
        final ClusterMembership clusterMembership = new ClusterMembership();
        final File clusterDir = consensusModule.context().clusterDir();

        if (!ClusterTool.listMembers(clusterMembership, clusterDir, TimeUnit.SECONDS.toMillis(3)))
        {
            throw new IllegalStateException("timeout waiting for cluster members info");
        }

        return clusterMembership;
    }

    public String hostname()
    {
        return TestCluster.hostname(index());
    }

    public boolean allSnapshotsLoaded()
    {
        for (final TestService service : services)
        {
            if (!service.wasSnapshotLoaded())
            {
                return false;
            }
        }

        return true;
    }

    public static class TestService extends StubClusteredService
    {
        static final int SNAPSHOT_FRAGMENT_COUNT = 500;
        static final int SNAPSHOT_MSG_LENGTH = 1000;

        volatile boolean wasSnapshotTaken = false;
        volatile boolean wasSnapshotLoaded = false;
        volatile boolean failNextSnapshot = false;
        private int index;
        private volatile boolean hasReceivedUnexpectedMessage = false;
        private volatile Cluster.Role roleChangedTo = null;
        private final AtomicInteger activeSessionCount = new AtomicInteger();
        protected final AtomicInteger messageCount = new AtomicInteger();

        final AtomicInteger liveMessageCount = new AtomicInteger();
        final AtomicInteger snapshotMessageCount = new AtomicInteger();
        final AtomicInteger timerCount = new AtomicInteger();

        public TestService index(final int index)
        {
            this.index = index;
            return this;
        }

        int index()
        {
            return index;
        }

        int activeSessionCount()
        {
            return activeSessionCount.get();
        }

        public int messageCount()
        {
            return messageCount.get();
        }

        public int timerCount()
        {
            return timerCount.get();
        }

        public boolean wasSnapshotTaken()
        {
            return wasSnapshotTaken;
        }

        public void resetSnapshotTaken()
        {
            wasSnapshotTaken = false;
        }

        public boolean wasSnapshotLoaded()
        {
            return wasSnapshotLoaded;
        }

        public Cluster.Role roleChangedTo()
        {
            return roleChangedTo;
        }

        public Cluster cluster()
        {
            return cluster;
        }

        boolean hasReceivedUnexpectedMessage()
        {
            return hasReceivedUnexpectedMessage;
        }

        public void onStart(final Cluster cluster, final Image snapshotImage)
        {
            super.onStart(cluster, snapshotImage);

            if (null != snapshotImage)
            {
                activeSessionCount.set(cluster.clientSessions().size());

                final FragmentHandler handler =
                    (buffer, offset, length, header) ->
                    {
                        final int value = buffer.getInt(offset);
                        messageCount.set(value);
                        snapshotMessageCount.set(value);
                    };

                int fragmentCount = 0;
                while (true)
                {
                    final int fragments = snapshotImage.poll(handler, 10);
                    fragmentCount += fragments;

                    if (snapshotImage.isClosed() || snapshotImage.isEndOfStream())
                    {
                        break;
                    }

                    idleStrategy.idle(fragments);
                }

                if (fragmentCount != SNAPSHOT_FRAGMENT_COUNT)
                {
                    throw new AgentTerminationException(
                        "unexpected snapshot length: expected=" + SNAPSHOT_FRAGMENT_COUNT + " actual=" + fragmentCount);
                }

                wasSnapshotLoaded = true;
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
            final String message = buffer.getStringWithoutLengthAscii(offset, length);
            if (ClusterTests.REGISTER_TIMER_MSG.equals(message))
            {
                idleStrategy.reset();
                while (!cluster.scheduleTimer(1, cluster.time() + 1_000))
                {
                    idleStrategy.idle();
                }
            }

            if (message.startsWith(ClusterTests.PAUSE))
            {
                try
                {
                    final String[] messageComponents = message.split("\\|");
                    final int nodeIndex = Integer.parseInt(messageComponents[1]);

                    if (nodeIndex == index)
                    {
                        final long durationNs = Long.parseLong(messageComponents[2]);
                        LockSupport.parkNanos(durationNs);
                    }
                }
                catch (final NumberFormatException ex)
                {
                    throw new AeronException("Invalid message components with message: " + message, ex);
                }
            }

            switch (message)
            {
                case ClusterTests.UNEXPECTED_MSG:
                    hasReceivedUnexpectedMessage = true;
                    throw new IllegalStateException("unexpected message received");

                case ClusterTests.TERMINATE_MSG:
                    throw new ClusterTerminationException(false);

                case ClusterTests.ECHO_SERVICE_IPC_INGRESS_MSG:
                    sendServiceIpcMessage(session, buffer, offset, length);
                    break;

                case ClusterTests.ECHO_SERVICE_IPC_INGRESS_MSG_SKIP_FOLLOWER:
                    simulateBuggyApplicationCodeThatSkipsServiceMessageOnFollower(session, buffer, offset, length);
                    break;

                default:
                    if (ClusterTests.ERROR_MSG.equals(message))
                    {
                        cluster.context().errorHandler().onError(new Exception(message));
                    }

                    if (null != session)
                    {
                        while (session.offer(buffer, offset, length) < 0)
                        {
                            idleStrategy.idle();
                        }
                    }
                    break;
            }

            messageCount.incrementAndGet();
            liveMessageCount.incrementAndGet();
        }

        private void simulateBuggyApplicationCodeThatSkipsServiceMessageOnFollower(
            final ClientSession session,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            if (Cluster.Role.LEADER == cluster.role())
            {
                sendServiceIpcMessage(session, buffer, offset, length);
            }
        }

        private void sendServiceIpcMessage(
            final ClientSession session,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            if (null != session)
            {
                idleStrategy.reset();
                while (cluster.offer(buffer, offset, length) < 0)
                {
                    idleStrategy.idle();
                }
            }
            else
            {
                for (final ClientSession clientSession : cluster.clientSessions())
                {
                    echoMessage(clientSession, buffer, offset, length);
                }
            }
        }

        public void onTimerEvent(final long correlationId, final long timestamp)
        {
            timerCount.incrementAndGet();
        }

        public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
        {
            if (failNextSnapshot)
            {
                failNextSnapshot = false;
                throw new RuntimeException("This is a simulated failure for this snapshot");
            }

            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[SNAPSHOT_MSG_LENGTH]);
            buffer.putInt(0, messageCount.get());
            buffer.putInt(SNAPSHOT_MSG_LENGTH - SIZE_OF_INT, messageCount.get());

            for (int i = 0; i < SNAPSHOT_FRAGMENT_COUNT; i++)
            {
                idleStrategy.reset();
                while (snapshotPublication.offer(buffer, 0, SNAPSHOT_MSG_LENGTH) < 0)
                {
                    idleStrategy.idle();
                }
            }

            wasSnapshotTaken = true;
        }

        public void onSessionOpen(final ClientSession session, final long timestamp)
        {
            super.onSessionOpen(session, timestamp);
            activeSessionCount.incrementAndGet();
        }

        public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
        {
            super.onSessionClose(session, timestamp, closeReason);
            activeSessionCount.decrementAndGet();
        }

        public void onRoleChange(final Cluster.Role newRole)
        {
            roleChangedTo = newRole;
        }

        public void awaitServiceMessageCount(final int messageCount, final Runnable keepAlive, final Object node)
        {
            awaitServiceMessagePredicate(atLeast(messageCount), keepAlive, node);
        }

        public void awaitServiceMessagePredicate(
            final IntPredicate predicate,
            final Runnable keepAlive,
            final Object node)
        {
            while (true)
            {
                final int count = messageCount();
                if (predicate.test(count))
                {
                    return;
                }

                Thread.yield();
                if (Thread.interrupted())
                {
                    throw new TimeoutException("count=" + count + " awaiting=" + predicate + " node=" + node);
                }

                if (hasReceivedUnexpectedMessage())
                {
                    fail("service received unexpected message");
                }

                keepAlive.run();
            }
        }

        public void awaitLiveAndSnapshotMessageCount(
            final IntPredicate livePredicate,
            final IntPredicate snapshotPredicate,
            final Runnable keepAlive,
            final Object node)
        {
            while (true)
            {
                final int liveCount = liveMessageCount.get();
                final int snapshotCount = snapshotMessageCount.get();
                if (livePredicate.test(liveCount) && snapshotPredicate.test(snapshotCount))
                {
                    return;
                }

                Thread.yield();
                if (Thread.interrupted())
                {
                    throw new TimeoutException(
                        "liveCount=" + liveCount + " snapshotCount=" + snapshotCount +
                        " awaitingLive=" + livePredicate + " awaitingSnapshot=" + snapshotPredicate + " node=" + node);
                }

                if (hasReceivedUnexpectedMessage())
                {
                    fail("service received unexpected message");
                }

                keepAlive.run();
            }
        }

        public void failNextSnapshot(final boolean failNextSnapshot)
        {
            this.failNextSnapshot = failNextSnapshot;
        }

        public String toString()
        {
            return "TestService{" +
                "wasSnapshotTaken=" + wasSnapshotTaken +
                ", wasSnapshotLoaded=" + wasSnapshotLoaded +
                ", failNextSnapshot=" + failNextSnapshot +
                ", index=" + index +
                ", hasReceivedUnexpectedMessage=" + hasReceivedUnexpectedMessage +
                ", roleChangedTo=" + roleChangedTo +
                ", activeSessionCount=" + activeSessionCount +
                ", messageCount=" + messageCount +
                ", liveMessageCount=" + liveMessageCount +
                ", snapshotMessageCount=" + snapshotMessageCount +
                ", timerCount=" + timerCount +
                '}';
        }
    }

    public static class SleepOnSnapshotTestService extends TestNode.TestService
    {
        long snapshotDelayMs = 0;

        public TestService snapshotDelayMs(final long snapshotDelayMs)
        {
            this.snapshotDelayMs = snapshotDelayMs;
            return this;
        }

        public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
        {
            super.onTakeSnapshot(snapshotPublication);

            if (snapshotDelayMs > 0)
            {
                Tests.sleep(snapshotDelayMs);
            }
        }
    }

    public static class ChecksumService extends TestNode.TestService
    {
        private final BufferClaim bufferClaim = new BufferClaim();
        private final CRC32 crc32 = new CRC32();
        private long checksum;

        public long checksum()
        {
            return checksum;
        }

        public void onStart(final Cluster cluster, final Image snapshotImage)
        {
            checksum = 0;
            wasSnapshotLoaded = false;
            this.cluster = cluster;
            this.idleStrategy = cluster.idleStrategy();

            if (null != snapshotImage)
            {
                final FragmentHandler handler =
                    (buffer, offset, length, header) -> checksum = buffer.getLong(offset, LITTLE_ENDIAN);
                while (true)
                {
                    final int fragments = snapshotImage.poll(handler, 1);

                    if (snapshotImage.isClosed() || snapshotImage.isEndOfStream())
                    {
                        break;
                    }

                    idleStrategy.idle(fragments);
                }
                wasSnapshotLoaded = true;
            }
        }

        public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
        {
            idleStrategy.reset();
            while (true)
            {
                if (snapshotPublication.tryClaim(SIZE_OF_LONG, bufferClaim) > 0)
                {
                    bufferClaim.buffer().putLong(bufferClaim.offset(), checksum, LITTLE_ENDIAN);
                    bufferClaim.commit();
                    break;
                }
                idleStrategy.idle();
            }
            wasSnapshotTaken = true;
        }

        public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            final int payloadLength = length - SIZE_OF_INT;
            final int msgChecksum = buffer.getInt(offset + payloadLength, LITTLE_ENDIAN);
            crc32.reset();
            crc32.update(buffer.byteArray(), offset, payloadLength);
            final int computedChecksum = (int)crc32.getValue();
            if (computedChecksum != msgChecksum)
            {
                throw new ClusterException("checksum mismatch");
            }

            checksum = Hashing.hash(checksum ^ msgChecksum);
        }
    }

    public static class MessageTrackingService extends TestNode.TestService
    {
        public static final int TIMER_MESSAGES_PER_INGRESS = 2;
        public static final int SERVICE_MESSAGES_PER_INGRESS = 3;

        private static volatile boolean delaySessionMessageProcessing;
        private static final byte SNAPSHOT_COUNTERS = (byte)1;
        private static final byte SNAPSHOT_CLIENT_MESSAGES = (byte)2;
        private static final byte SNAPSHOT_SERVICE_MESSAGES = (byte)3;
        private static final byte SNAPSHOT_TIMERS = (byte)4;
        private final int serviceId;
        private final ExpandableArrayBuffer messageBuffer = new ExpandableArrayBuffer();
        private final IntArrayList clientMessages = new IntArrayList();
        private final IntArrayList serviceMessages = new IntArrayList();
        private final LongArrayList timers = new LongArrayList();
        private int nextServiceMessageNumber;
        private long nextTimerCorrelationId;

        public static void delaySessionMessageProcessing(final boolean shouldDelay)
        {
            delaySessionMessageProcessing = shouldDelay;
        }

        @SuppressWarnings("this-escape")
        public MessageTrackingService(final int serviceId, final int index)
        {
            this.serviceId = serviceId;
            index(index);
        }

        public IntArrayList clientMessages()
        {
            return copy(clientMessages);
        }

        public IntArrayList serviceMessages()
        {
            return copy(serviceMessages);
        }

        public LongArrayList timers()
        {
            return copy(timers);
        }

        public void onStart(final Cluster cluster, final Image snapshotImage)
        {
            nextServiceMessageNumber = 1_000_000 * serviceId;
            nextTimerCorrelationId = -1L * 1_000_000 * serviceId;
            clientMessages.clear();
            serviceMessages.clear();
            timers.clear();
            wasSnapshotLoaded = false;
            this.cluster = cluster;
            this.idleStrategy = cluster.idleStrategy();

            if (null != snapshotImage)
            {
                final FragmentHandler handler =
                    new FragmentAssembler((buffer, offset, length, header) ->
                    {
                        int index = offset;
                        final byte snapshotType = buffer.getByte(index);
                        index++;
                        if (SNAPSHOT_COUNTERS == snapshotType)
                        {
                            final int storedServiceId = buffer.getInt(index, LITTLE_ENDIAN);
                            if (serviceId != storedServiceId)
                            {
                                throw new IllegalStateException("Invalid snapshot!");
                            }
                            index += SIZE_OF_INT;
                            messageCount.set(buffer.getInt(index, LITTLE_ENDIAN));
                            index += SIZE_OF_INT;
                            timerCount.set(buffer.getInt(index, LITTLE_ENDIAN));
                            index += SIZE_OF_INT;
                            nextServiceMessageNumber = buffer.getInt(index, LITTLE_ENDIAN);
                            index += SIZE_OF_INT;
                            nextTimerCorrelationId = buffer.getLong(index, LITTLE_ENDIAN);
                        }
                        else if (SNAPSHOT_CLIENT_MESSAGES == snapshotType) // client messages
                        {
                            restoreMessages(buffer, index, clientMessages);
                        }
                        else if (SNAPSHOT_SERVICE_MESSAGES == snapshotType) // service messages
                        {
                            restoreMessages(buffer, index, serviceMessages);
                        }
                        else if (SNAPSHOT_TIMERS == snapshotType) // timers
                        {
                            restoreTimers(buffer, index);
                        }
                        else
                        {
                            throw new IllegalStateException("Unknown snapshot type: " + snapshotType);
                        }
                    });

                while (true)
                {
                    final int fragments = snapshotImage.poll(handler, 1);

                    if (snapshotImage.isClosed() || snapshotImage.isEndOfStream())
                    {
                        break;
                    }

                    idleStrategy.idle(fragments);
                }
                wasSnapshotLoaded = true;
            }
        }

        public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
        {
            wasSnapshotTaken = false;

            int offset = 0;

            messageBuffer.putByte(offset, SNAPSHOT_COUNTERS);
            offset++;
            messageBuffer.putInt(offset, serviceId, LITTLE_ENDIAN);
            offset += SIZE_OF_INT;
            messageBuffer.putInt(offset, messageCount(), LITTLE_ENDIAN);
            offset += SIZE_OF_INT;
            messageBuffer.putInt(offset, timerCount(), LITTLE_ENDIAN);
            offset += SIZE_OF_INT;
            messageBuffer.putInt(offset, nextServiceMessageNumber, LITTLE_ENDIAN);
            offset += SIZE_OF_INT;
            messageBuffer.putLong(offset, nextTimerCorrelationId, LITTLE_ENDIAN);
            offset += SIZE_OF_LONG;
            idleStrategy.reset();
            while (snapshotPublication.offer(messageBuffer, 0, offset) < 0)
            {
                idleStrategy.idle();
            }

            snapshotMessages(snapshotPublication, SNAPSHOT_CLIENT_MESSAGES, clientMessages);
            snapshotMessages(snapshotPublication, SNAPSHOT_SERVICE_MESSAGES, serviceMessages);
            snapshotTimers(snapshotPublication);

            wasSnapshotTaken = true;
        }

        public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            if (delaySessionMessageProcessing)
            {
                Tests.sleep(1);
            }

            if (null != session)
            {
                final int messageId = buffer.getInt(offset, LITTLE_ENDIAN);
                clientMessages.addInt(messageId);

                // Send 3 service messages
                for (int i = 0; i < SERVICE_MESSAGES_PER_INGRESS; i++)
                {
                    messageBuffer.putInt(0, ++nextServiceMessageNumber, LITTLE_ENDIAN);

                    idleStrategy.reset();
                    while (cluster.offer(messageBuffer, 0, SIZE_OF_INT) < 0)
                    {
                        idleStrategy.idle();
                    }
                }

                // Schedule two timers
                for (int i = 0; i < TIMER_MESSAGES_PER_INGRESS; i++)
                {
                    final long timerId = --nextTimerCorrelationId;
                    idleStrategy.reset();
                    while (!cluster.scheduleTimer(timerId, cluster.time() - 1))
                    {
                        idleStrategy.idle();
                    }
                }

                // Echo input message back to the client
                while (session.offer(buffer, offset, length) < 0)
                {
                    idleStrategy.idle();
                }
            }
            else
            {
                final int serviceMessageId = buffer.getInt(offset, LITTLE_ENDIAN);
                serviceMessages.addInt(serviceMessageId);
            }

            messageCount.incrementAndGet(); // count all messages
        }

        public void onTimerEvent(final long correlationId, final long timestamp)
        {
            timers.add(correlationId);
            super.onTimerEvent(correlationId, timestamp);
        }

        public String toString()
        {
            return "MessageTrackingService{" +
                "serviceId=" + serviceId +
                ", messageCount=" + messageCount() +
                ", timerCount=" + timerCount() +
                ", nextServiceMessageNumber=" + nextServiceMessageNumber +
                ", nextTimerCorrelationId=" + nextTimerCorrelationId +
                '}';
        }

        private void snapshotMessages(
            final ExclusivePublication snapshotPublication, final byte snapshotType, final IntArrayList messages)
        {
            final MutableInteger offset = new MutableInteger();
            messageBuffer.putByte(offset.get(), snapshotType);
            offset.increment();
            messageBuffer.putInt(offset.get(), messages.size(), LITTLE_ENDIAN);
            offset.addAndGet(SIZE_OF_INT);

            messages.forEachInt(
                (messageId) ->
                {
                    messageBuffer.putInt(offset.get(), messageId);
                    offset.addAndGet(SIZE_OF_INT);
                });

            idleStrategy.reset();
            while (snapshotPublication.offer(messageBuffer, 0, offset.get()) < 0)
            {
                idleStrategy.idle();
            }
        }

        private void snapshotTimers(final ExclusivePublication snapshotPublication)
        {
            final MutableInteger offset = new MutableInteger();
            messageBuffer.putByte(offset.get(), SNAPSHOT_TIMERS);
            offset.increment();
            messageBuffer.putInt(offset.get(), timers.size(), LITTLE_ENDIAN);
            offset.addAndGet(SIZE_OF_INT);

            timers.forEachLong(
                (correlationId) ->
                {
                    messageBuffer.putLong(offset.get(), correlationId);
                    offset.addAndGet(SIZE_OF_LONG);
                });

            idleStrategy.reset();
            while (snapshotPublication.offer(messageBuffer, 0, offset.get()) < 0)
            {
                idleStrategy.idle();
            }
        }

        private void restoreMessages(final DirectBuffer buffer, final int offset, final IntArrayList messages)
        {
            int absoluteOffset = offset;
            final int count = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
            absoluteOffset += SIZE_OF_INT;
            for (int i = 0; i < count; i++)
            {
                final int messageId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
                absoluteOffset += SIZE_OF_INT;
                messages.addInt(messageId);
            }
        }

        private void restoreTimers(final DirectBuffer buffer, final int offset)
        {
            int absoluteOffset = offset;
            final int count = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
            absoluteOffset += SIZE_OF_INT;
            for (int i = 0; i < count; i++)
            {
                final long correlationId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
                absoluteOffset += SIZE_OF_LONG;
                timers.add(correlationId);
            }
        }

        private static IntArrayList copy(final IntArrayList values)
        {
            return new IntArrayList(values.toIntArray(), values.size(), values.nullValue());
        }

        private static LongArrayList copy(final LongArrayList values)
        {
            return new LongArrayList(values.toLongArray(), values.size(), values.nullValue());
        }
    }

    static class Context
    {
        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context();
        final Archive.Context archiveContext = new Archive.Context();
        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context();
        final ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context();
        final ClusteredServiceContainer.Context serviceContainerContext = new ClusteredServiceContainer.Context();
        final AtomicBoolean isTerminationExpected = new AtomicBoolean();
        final AtomicBoolean hasMemberTerminated = new AtomicBoolean();
        final AtomicBoolean[] hasServiceTerminated;
        final TestService[] services;

        Context(final TestService[] services, final String nodeMappings)
        {
            mediaDriverContext.nameResolver(new RedirectingNameResolver(nodeMappings));

            this.services = services;
            hasServiceTerminated = new AtomicBoolean[services.length];
            for (int i = 0; i < services.length; i++)
            {
                hasServiceTerminated[i] = new AtomicBoolean();
            }
        }
    }

    public String toString()
    {
        return "TestNode{" +
            "memberId=" + index() +
            ", role=" + role() +
            ", services=" + Arrays.toString(services) +
            '}';
    }

    public static IntPredicate atLeast(final int count)
    {
        return new IntPredicate()
        {
            public boolean test(final int value)
            {
                return count <= value;
            }

            public String toString()
            {
                return "atLeast(" + count + ")";
            }
        };
    }

    public static IntPredicate atMost(final int count)
    {
        return new IntPredicate()
        {
            public boolean test(final int value)
            {
                return value <= count;
            }

            public String toString()
            {
                return "atMost(" + count + ")";
            }
        };
    }
}
