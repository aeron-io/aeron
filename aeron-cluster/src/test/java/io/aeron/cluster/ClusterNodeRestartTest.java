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

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.test.*;
import io.aeron.test.cluster.ClusterTests;
import io.aeron.test.cluster.StubClusteredService;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.PrintStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;


import static io.aeron.cluster.ClusterTestConstants.CLUSTER_MEMBERS;
import static io.aeron.cluster.ClusterTestConstants.INGRESS_ENDPOINTS;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith({InterruptingTestCallback.class, HideStdErrExtension.class})
class ClusterNodeRestartTest
{
    private static final int MESSAGE_LENGTH = SIZE_OF_INT;
    private static final int TIMER_MESSAGE_LENGTH = SIZE_OF_INT + SIZE_OF_LONG + SIZE_OF_LONG;
    private static final int MESSAGE_VALUE_OFFSET = 0;
    private static final int TIMER_MESSAGE_ID_OFFSET = MESSAGE_VALUE_OFFSET + SIZE_OF_INT;
    private static final int TIMER_MESSAGE_DELAY_OFFSET = TIMER_MESSAGE_ID_OFFSET + SIZE_OF_LONG;
    public static final long TIMER_CORRELATION_ID = 777;

    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;
    private AeronCluster aeronCluster;

    private final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
    private final AtomicReference<String> serviceState = new AtomicReference<>();
    private final CountDownLatch terminationLatch = new CountDownLatch(1);

    @BeforeEach
    void before()
    {
        launchClusteredMediaDriver(true);
    }

    @AfterEach
    void after()
    {
        if (null == clusteredMediaDriver)
        {
            CloseHelper.closeAll(aeronCluster, container);
        }
        else
        {
            CloseHelper.closeAll(clusteredMediaDriver.consensusModule(), aeronCluster, container, clusteredMediaDriver);
            clusteredMediaDriver.consensusModule().context().deleteDirectory();
            clusteredMediaDriver.archive().context().deleteDirectory();
            clusteredMediaDriver.mediaDriver().context().deleteDirectory();
            container.context().deleteDirectory();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldRestartServiceWithReplay()
    {
        final AtomicLong serviceMsgCount = new AtomicLong(0);
        final AtomicLong restartServiceMsgCount = new AtomicLong(0);

        launchService(serviceMsgCount);
        connectClient();

        sendNumberedMessageIntoCluster(0);
        Tests.awaitValue(serviceMsgCount, 1);

        forceCloseForRestart();

        launchClusteredMediaDriver(false);
        launchService(restartServiceMsgCount);
        connectClient();

        Tests.awaitValue(restartServiceMsgCount, 1);

        ClusterTests.failOnClusterError();
    }

    @Test
    @InterruptAfter(10)
    void shouldRestartServiceWithReplayAndContinue()
    {
        final AtomicLong serviceMsgCount = new AtomicLong(0);

        launchService(serviceMsgCount);
        connectClient();

        sendNumberedMessageIntoCluster(0);
        Tests.awaitValue(serviceMsgCount, 1);

        forceCloseForRestart();

        final AtomicLong restartServiceMsgCounter = new AtomicLong(0);

        launchClusteredMediaDriver(false);
        launchService(restartServiceMsgCounter);
        connectClient();

        sendNumberedMessageIntoCluster(1);
        Tests.awaitValue(restartServiceMsgCounter, 1);

        ClusterTests.failOnClusterError();
    }

    @Test
    @InterruptAfter(10)
    void shouldRestartServiceFromEmptySnapshot()
    {
        final AtomicLong serviceMsgCount = new AtomicLong(0);

        launchService(serviceMsgCount);
        connectClient();

        final AtomicCounter controlToggle = getControlToggle();
        assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));

        Tests.awaitValue(clusteredMediaDriver.consensusModule().context().snapshotCounter(), 1);

        forceCloseForRestart();

        serviceState.set(null);
        launchClusteredMediaDriver(false);
        launchService(serviceMsgCount);
        connectClient();

        Tests.await(() -> null != serviceState.get());
        assertEquals("0", serviceState.get());

        ClusterTests.failOnClusterError();
    }

    @Test
    @InterruptAfter(10)
    void shouldRestartServiceFromSnapshot()
    {
        final AtomicLong serviceMsgCount = new AtomicLong(0);

        launchService(serviceMsgCount);
        connectClient();

        sendNumberedMessageIntoCluster(0);
        sendNumberedMessageIntoCluster(1);
        sendNumberedMessageIntoCluster(2);

        Tests.awaitValue(serviceMsgCount, 3);

        final AtomicCounter controlToggle = getControlToggle();
        assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));

        Tests.awaitValue(clusteredMediaDriver.consensusModule().context().snapshotCounter(), 1);

        forceCloseForRestart();

        serviceState.set(null);
        launchClusteredMediaDriver(false);
        launchService(serviceMsgCount);
        connectClient();

        Tests.await(() -> null != serviceState.get());
        assertEquals("3", serviceState.get());

        ClusterTests.failOnClusterError();
    }

    @Test
    @InterruptAfter(10)
    void shouldShutDownServiceAfterSnapshot()
    {
        final AtomicLong serviceMsgCount = new AtomicLong(0);

        launchService(serviceMsgCount);
        connectClient();

        sendNumberedMessageIntoCluster(0);
        sendNumberedMessageIntoCluster(1);
        sendNumberedMessageIntoCluster(2);

        Tests.awaitValue(serviceMsgCount, 3);

        final AtomicCounter controlToggle = getControlToggle();
        assertTrue(ClusterControl.ToggleState.SHUTDOWN.toggle(controlToggle));

        Tests.awaitValue(clusteredMediaDriver.consensusModule().context().snapshotCounter(), 1);

        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
        CloseHelper.closeAll(aeronCluster, clusteredMediaDriver);

        ClusterTests.failOnClusterWarning();
        ClusterTests.failOnClusterError();
    }

    @Test
    @InterruptAfter(10)
    void shouldRestartServiceFromSnapshotWithFurtherLog()
    {
        final AtomicLong serviceMsgCount = new AtomicLong(0);

        launchService(serviceMsgCount);
        connectClient();

        sendNumberedMessageIntoCluster(0);
        sendNumberedMessageIntoCluster(1);
        sendNumberedMessageIntoCluster(2);

        Tests.awaitValue(serviceMsgCount, 3);

        final AtomicCounter controlToggle = getControlToggle();
        assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));

        Tests.awaitValue(clusteredMediaDriver.consensusModule().context().snapshotCounter(), 1);

        sendNumberedMessageIntoCluster(3);
        Tests.awaitValue(serviceMsgCount, 4);

        forceCloseForRestart();

        serviceMsgCount.set(0);
        launchClusteredMediaDriver(false);
        launchService(serviceMsgCount);
        connectClient();

        Tests.awaitValue(serviceMsgCount, 1);
        assertEquals("4", serviceState.get());

        ClusterTests.failOnClusterError();
    }

    @Test
    @InterruptAfter(10)
    void shouldTakeMultipleSnapshots()
    {
        final AtomicLong serviceMsgCount = new AtomicLong(0);
        launchService(serviceMsgCount);
        connectClient();

        final AtomicCounter controlToggle = getControlToggle();

        for (int i = 0; i < 3; i++)
        {
            assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));

            while (controlToggle.get() != ClusterControl.ToggleState.NEUTRAL.code())
            {
                Tests.sleep(1, "snapshot %d", i);
            }
        }

        assertEquals(3L, clusteredMediaDriver.consensusModule().context().snapshotCounter().get());

        ClusterTests.failOnClusterError();
    }

    @Test
    @InterruptAfter(10)
    void shouldRestartServiceWithTimerFromSnapshotWithFurtherLog()
    {
        final AtomicLong serviceMsgCount = new AtomicLong(0);

        launchService(serviceMsgCount);
        connectClient();

        sendNumberedMessageIntoCluster(0);
        sendNumberedMessageIntoCluster(1);
        sendNumberedMessageIntoCluster(2);
        sendTimerMessageIntoCluster(3, TimeUnit.HOURS.toMillis(10));

        Tests.awaitValue(serviceMsgCount, 4);

        final AtomicCounter controlToggle = getControlToggle();
        assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));

        Tests.awaitValue(clusteredMediaDriver.consensusModule().context().snapshotCounter(), 1);

        sendNumberedMessageIntoCluster(4);
        Tests.awaitValue(serviceMsgCount, 5);

        forceCloseForRestart();

        serviceMsgCount.set(0);
        launchClusteredMediaDriver(false);
        launchService(serviceMsgCount);
        connectClient();

        Tests.awaitValue(serviceMsgCount, 1);
        assertEquals("5", serviceState.get());

        ClusterTests.failOnClusterError();
    }

    @Test
    @InterruptAfter(10)
    void shouldTriggerRescheduledTimerAfterReplay()
    {
        final AtomicLong triggeredTimersCount = new AtomicLong();

        launchReschedulingService(triggeredTimersCount);
        connectClient();

        sendNumberedMessageIntoCluster(0);
        Tests.awaitValue(triggeredTimersCount, 2);

        forceCloseForRestart();

        final long triggeredSinceStart = triggeredTimersCount.getAndSet(0);

        launchClusteredMediaDriver(false);
        launchReschedulingService(triggeredTimersCount);

        Tests.awaitValue(triggeredTimersCount, triggeredSinceStart + 1);

        ClusterTests.failOnClusterError();
    }

    @Test
    @InterruptAfter(20)
    void shouldRestartServiceTwiceWithInvalidSnapshotAndFurtherLog()
    {
        final AtomicLong serviceMsgCount = new AtomicLong(0);

        launchService(serviceMsgCount);
        connectClient();

        sendNumberedMessageIntoCluster(0);
        sendNumberedMessageIntoCluster(1);
        sendNumberedMessageIntoCluster(2);

        Tests.awaitValue(serviceMsgCount, 3);

        final AtomicCounter controlToggle = getControlToggle();
        assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));

        Tests.awaitValue(clusteredMediaDriver.consensusModule().context().snapshotCounter(), 1);

        sendNumberedMessageIntoCluster(3);
        Tests.awaitValue(serviceMsgCount, 4);

        forceCloseForRestart();

        final PrintStream mockOut = mock(PrintStream.class);
        final File clusterDir = clusteredMediaDriver.consensusModule().context().clusterDir();
        assertTrue(ClusterTool.invalidateLatestSnapshot(mockOut, clusterDir));

        verify(mockOut).println(" invalidate latest snapshot: true");

        serviceMsgCount.set(0);
        launchClusteredMediaDriver(false);
        launchService(serviceMsgCount);

        Tests.awaitValue(serviceMsgCount, 4);
        assertEquals("4", serviceState.get());

        connectClient();
        sendNumberedMessageIntoCluster(4);
        Tests.awaitValue(serviceMsgCount, 5);

        forceCloseForRestart();

        serviceMsgCount.set(0);
        launchClusteredMediaDriver(false);
        launchService(serviceMsgCount);

        connectClient();
        assertEquals("5", serviceState.get());

        ClusterTests.failOnClusterError();
    }

    @Test
    @InterruptAfter(20)
    @IgnoreStdErr
    void shouldRestartServiceAfterShutdownWithInvalidatedSnapshot() throws InterruptedException
    {
        final AtomicLong serviceMsgCount = new AtomicLong(0);

        launchService(serviceMsgCount);
        connectClient();

        sendNumberedMessageIntoCluster(0);
        sendNumberedMessageIntoCluster(1);
        sendNumberedMessageIntoCluster(2);

        Tests.awaitValue(serviceMsgCount, 3);

        final AtomicCounter controlToggle = getControlToggle();
        assertTrue(ClusterControl.ToggleState.SHUTDOWN.toggle(controlToggle));

        terminationLatch.await();
        forceCloseForRestart();

        final PrintStream mockOut = mock(PrintStream.class);
        final File clusterDir = clusteredMediaDriver.consensusModule().context().clusterDir();
        assertTrue(ClusterTool.invalidateLatestSnapshot(mockOut, clusterDir));

        verify(mockOut).println(" invalidate latest snapshot: true");

        serviceMsgCount.set(0);
        launchClusteredMediaDriver(false);
        launchService(serviceMsgCount);

        Tests.awaitValue(serviceMsgCount, 3);
        assertEquals("3", serviceState.get());

        connectClient();
        sendNumberedMessageIntoCluster(3);
        Tests.awaitValue(serviceMsgCount, 4);

        ClusterTests.failOnClusterError();
    }

    private AtomicCounter getControlToggle()
    {
        final int clusterId = container.context().clusterId();
        final CountersReader counters = container.context().aeron().countersReader();
        final AtomicCounter controlToggle = ClusterControl.findControlToggle(counters, clusterId);
        assertNotNull(controlToggle);

        return controlToggle;
    }

    private void sendNumberedMessageIntoCluster(final int value)
    {
        msgBuffer.putInt(MESSAGE_VALUE_OFFSET, value);

        sendMessageIntoCluster(aeronCluster, msgBuffer, MESSAGE_LENGTH);
    }

    private void sendTimerMessageIntoCluster(final int value, final long delay)
    {
        msgBuffer.putInt(MESSAGE_VALUE_OFFSET, value);
        msgBuffer.putLong(TIMER_MESSAGE_ID_OFFSET, TIMER_CORRELATION_ID);
        msgBuffer.putLong(TIMER_MESSAGE_DELAY_OFFSET, delay);

        sendMessageIntoCluster(aeronCluster, msgBuffer, TIMER_MESSAGE_LENGTH);
    }

    private static void sendMessageIntoCluster(final AeronCluster cluster, final DirectBuffer buffer, final int length)
    {
        while (true)
        {
            final long result = cluster.offer(buffer, 0, length);
            if (result > 0)
            {
                break;
            }

            checkResult(result);
            if (cluster.isClosed())
            {
                fail("unexpected cluster client close");
            }

            Tests.yield();
        }
    }

    private void launchService(final AtomicLong msgCounter)
    {
        final ClusteredService service = new StubClusteredService()
        {
            private int nextCorrelationId = 0;
            private int counterValue = 0;

            public void onStart(final Cluster cluster, final Image snapshotImage)
            {
                super.onStart(cluster, snapshotImage);

                if (null != snapshotImage)
                {
                    final FragmentHandler fragmentHandler =
                        (buffer, offset, length, header) ->
                        {
                            nextCorrelationId = buffer.getInt(offset);
                            offset += SIZE_OF_INT;

                            counterValue = buffer.getInt(offset);
                            offset += SIZE_OF_INT;

                            serviceState.set(buffer.getStringAscii(offset));
                        };

                    while (true)
                    {
                        final int fragments = snapshotImage.poll(fragmentHandler, 1);
                        if (fragments == 1 || snapshotImage.isEndOfStream())
                        {
                            break;
                        }

                        idleStrategy.idle();
                    }
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
                final int sentValue = buffer.getInt(offset + MESSAGE_VALUE_OFFSET);
                assertEquals(counterValue, sentValue);

                counterValue++;
                serviceState.set(Integer.toString(counterValue));
                msgCounter.getAndIncrement();

                if (TIMER_MESSAGE_LENGTH == length)
                {
                    final long correlationId = serviceCorrelationId(nextCorrelationId++);
                    final long deadlineMs = timestamp + buffer.getLong(offset + TIMER_MESSAGE_DELAY_OFFSET);

                    idleStrategy.reset();
                    while (!cluster.scheduleTimer(correlationId, deadlineMs))
                    {
                        idleStrategy.idle();
                    }
                }
            }

            public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
            {
                final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

                int length = 0;
                buffer.putInt(length, nextCorrelationId);
                length += SIZE_OF_INT;

                buffer.putInt(length, counterValue);
                length += SIZE_OF_INT;

                length += buffer.putStringAscii(length, Integer.toString(counterValue));

                snapshotPublication.offer(buffer, 0, length);
            }
        };

        container = ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(service)
                .terminationHook(ClusterTests.NOOP_TERMINATION_HOOK)
                .errorHandler(ClusterTests.errorHandler(0)));
    }

    private void launchReschedulingService(final AtomicLong triggeredTimersCounter)
    {
        final ClusteredService service = new StubClusteredService()
        {
            public void onSessionMessage(
                final ClientSession session,
                final long timestamp,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header)
            {
                scheduleNext(serviceCorrelationId(7), timestamp + 200);
            }

            public void onTimerEvent(final long correlationId, final long timestamp)
            {
                triggeredTimersCounter.getAndIncrement();
                scheduleNext(correlationId, timestamp + 200);
            }

            public void onStart(final Cluster cluster, final Image snapshotImage)
            {
                super.onStart(cluster, snapshotImage);

                if (null != snapshotImage)
                {
                    final FragmentHandler fragmentHandler =
                        (buffer, offset, length, header) -> triggeredTimersCounter.set(buffer.getLong(offset));

                    while (true)
                    {
                        final int fragments = snapshotImage.poll(fragmentHandler, 1);
                        if (fragments == 1 || snapshotImage.isEndOfStream())
                        {
                            break;
                        }

                        idleStrategy.idle();
                    }
                }
            }

            public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
            {
                final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
                buffer.putLong(0, triggeredTimersCounter.get());

                idleStrategy.reset();
                while (snapshotPublication.offer(buffer, 0, SIZE_OF_INT) < 0)
                {
                    idleStrategy.idle();
                }
            }

            private void scheduleNext(final long correlationId, final long deadline)
            {
                idleStrategy.reset();
                while (!cluster.scheduleTimer(correlationId, deadline))
                {
                    idleStrategy.idle();
                }
            }
        };

        container = ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(service)
                .terminationHook(ClusterTests.NOOP_TERMINATION_HOOK)
                .errorHandler(ClusterTests.errorHandler(0)));
    }

    private void forceCloseForRestart()
    {
        CloseHelper.closeAll(clusteredMediaDriver.consensusModule(), aeronCluster, container, clusteredMediaDriver);
    }

    private void connectClient()
    {
        CloseHelper.close(aeronCluster);
        aeronCluster = AeronCluster.connect(new AeronCluster.Context()
                .ingressChannel("aeron:udp")
                .ingressEndpoints(INGRESS_ENDPOINTS)
                .egressChannel("aeron:udp?endpoint=localhost:0"));
    }

    private void launchClusteredMediaDriver(final boolean initialLaunch)
    {
        clusteredMediaDriver = ClusteredMediaDriver.launch(
            new MediaDriver.Context()
                .warnIfDirectoryExists(initialLaunch)
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .errorHandler(ClusterTests.errorHandler(0))
                .dirDeleteOnStart(true),
            TestContexts.localhostArchive()
                .catalogCapacity(ClusterTestConstants.CATALOG_CAPACITY)
                .recordingEventsEnabled(false)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(initialLaunch),
            new ConsensusModule.Context()
                .errorHandler(ClusterTests.errorHandler(0))
                .terminationHook(terminationLatch::countDown)
                .deleteDirOnStart(initialLaunch)
                .ingressChannel("aeron:udp")
                .clusterMembers(CLUSTER_MEMBERS)
                .replicationChannel("aeron:udp?endpoint=localhost:0"));
    }

    private static void checkResult(final long position)
    {
        if (position == Publication.NOT_CONNECTED ||
            position == Publication.CLOSED ||
            position == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new IllegalStateException("unexpected publication state: " + Publication.errorString(position));
        }
    }
}
