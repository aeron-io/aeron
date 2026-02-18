package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.PersistentSubscription;
import io.aeron.archive.client.PersistentSubscriptionListener;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.SystemUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.CommonContext.UDP_CHANNEL;
import static io.aeron.archive.ArchiveSystemTests.CATALOG_CAPACITY;
import static io.aeron.archive.ArchiveSystemTests.TERM_LENGTH;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class PersistentSubscriptionTest
{
    private final String MDC_CHANNEL = UDP_CHANNEL + "?control=localhost:2000";
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;
    private File archiveDir;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive aeronArchive;

    @BeforeEach
    void setUp()
    {
        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .publicationTermBufferLength(TERM_LENGTH)
            .ipcTermBufferLength(TERM_LENGTH)
            .dirDeleteOnShutdown(true)
            .imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(3))
            .spiesSimulateConnection(true);

        archiveDir = new File(SystemUtil.tmpDirName(), "archive");

        final Archive.Context archiveCtx = TestContexts.localhostArchive()
            .catalogCapacity(CATALOG_CAPACITY)
            .segmentFileLength(TERM_LENGTH)
            .aeronDirectoryName(aeronDirectoryName)
            .deleteArchiveOnStart(true)
            .archiveDir(archiveDir)
            .threadingMode(ArchiveThreadingMode.SHARED);

        driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());
        archive = Archive.launch(archiveCtx);
        systemTestWatcher.dataCollector().add(archiveCtx.archiveDir());

        aeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(aeronDirectoryName));

        aeronArchive = AeronArchive.connect(
            TestContexts.localhostAeronArchive()
                .aeron(aeron));
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(archive, driver, () -> IoUtil.delete(archiveDir, true));
    }

    @Test
    @InterruptAfter(10)
    void shouldErrorIfRecordingDoesNotExist()
    {
        try (PersistentSubscription persistentSubscription =
            new PersistentSubscription(aeronArchive, 13, 0, IPC_CHANNEL, 1000, null, driver.counters()))
        {
            persistentSubscription.controlledPoll(null, 1);
            // TODO
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldReplayExistingRecording()
    {
        final ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(IPC_CHANNEL, 1000);

        final CountersReader counters = aeron.countersReader();
        final int counterId = Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);

        final List<byte[]> payloads = generateRandomPayloads(5);
        final UnsafeBuffer buffer = new UnsafeBuffer();

        long offeredPosition = 0;
        for (final byte[] payload : payloads)
        {
            buffer.wrap(payload);
            while ((offeredPosition = publication.offer(buffer)) < 0)
            {
                Tests.yield();
            }
        }
        Tests.awaitPosition(counters, counterId, offeredPosition);

        final PersistentSubscriptionListenerImpl listener = new PersistentSubscriptionListenerImpl();
        try (PersistentSubscription persistentSubscription =
            new PersistentSubscription(aeronArchive, recordingId, 0, IPC_CHANNEL, 1000, listener, counters))
        {
            final List<byte[]> receivedPayloads = new ArrayList<>();
//            while (listener.onLiveCount == 0) // TODO should get the onLiveCallback
            while (receivedPayloads.size() != payloads.size())
            {
                final int workCount = persistentSubscription.controlledPoll((buffer1, offset, length, header) -> {
                    final byte[] bytes = new byte[length];
                    buffer1.getBytes(offset, bytes);
                    receivedPayloads.add(bytes);
                    return ControlledFragmentHandler.Action.CONTINUE;
                }, 10);
                if (workCount == 0)
                {
                    Tests.yield();
                }
                if (receivedPayloads.size() > 0)
                {
                    assertEquals(1, archive.context().replaySessionCounter().get());
                }
            }

            assertEquals(payloads.size(), receivedPayloads.size());
            for (int i = 0; i < receivedPayloads.size(); i++)
            {
                assertArrayEquals(payloads.get(i), receivedPayloads.get(i));
            }

            // send some more message
            final List<byte[]> payloads2 = generateRandomPayloads(5);

            for (final byte[] payload : payloads2)
            {
                buffer.wrap(payload);
                while ((offeredPosition = publication.offer(buffer)) < 0)
                {
                    Tests.yield();
                }
            }
            Tests.awaitPosition(counters, counterId, offeredPosition);

            while (receivedPayloads.size() < payloads.size() + payloads2.size())
            {
                final int workCount = persistentSubscription.controlledPoll((buffer1, offset, length, header) -> {
                    final byte[] bytes = new byte[length];
                    buffer1.getBytes(offset, bytes);
                    receivedPayloads.add(bytes);
                    return ControlledFragmentHandler.Action.CONTINUE;
                }, 10);
                if (workCount == 0)
                {
                    Tests.yield();
                }
            }

            assertTrue(persistentSubscription.isLive());

            Tests.await(() -> archive.context().replaySessionCounter().get() == 0);
        }
    }


    @Test
    @InterruptAfter(15)
    void shouldDropFromLiveBackToReplay() throws InterruptedException
    {
        final ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(UDP_CHANNEL +
            "?control=localhost:2000|control-mode=dynamic|fc=max", 1000);

        final CountersReader counters = aeron.countersReader();
        final int counterId = Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);

        final List<byte[]> payloads = generateRandomPayloads(5);
        final UnsafeBuffer buffer = new UnsafeBuffer();

        long offeredPosition = 0;
        for (final byte[] payload : payloads)
        {
            buffer.wrap(payload);
            while ((offeredPosition = publication.offer(buffer)) < 0)
            {
                Tests.yield();
            }
        }
        Tests.awaitPosition(counters, counterId, offeredPosition);
        final int liveStreamId = 1000;


        final PersistentSubscriptionListenerImpl listener = new PersistentSubscriptionListenerImpl();

        try (PersistentSubscription persistentSubscription =
            new PersistentSubscription(aeronArchive, recordingId, 0, MDC_CHANNEL, liveStreamId, listener, counters))
        {
            final List<byte[]> receivedPayloads = new ArrayList<>();
//            while (listener.onLiveCount == 0) // TODO should get the onLiveCallback
            while (receivedPayloads.size() != payloads.size())
            {
                final int workCount = persistentSubscription.controlledPoll((buffer1, offset, length, header) -> {
                    final byte[] bytes = new byte[length];
                    buffer1.getBytes(offset, bytes);
                    receivedPayloads.add(bytes);
                    return ControlledFragmentHandler.Action.CONTINUE;
                }, 10);
                if (workCount == 0)
                {
                    Tests.yield();
                }
                if (receivedPayloads.size() > 0)
                {
                    assertEquals(1, archive.context().replaySessionCounter().get());
                }
            }

            assertEquals(payloads.size(), receivedPayloads.size());
            for (int i = 0; i < receivedPayloads.size(); i++)
            {
                assertArrayEquals(payloads.get(i), receivedPayloads.get(i));
            }

            Thread.sleep(100); // TODO
            // send some more message
            final List<byte[]> payloads2 = generateRandomPayloads(5);

            for (final byte[] payload : payloads2)
            {
                buffer.wrap(payload);
                while ((offeredPosition = publication.offer(buffer)) < 0)
                {
                    Tests.yield();
                }
            }
            Tests.awaitPosition(counters, counterId, offeredPosition);

            while (receivedPayloads.size() < payloads.size() + payloads2.size())
            {
                final int workCount = persistentSubscription.controlledPoll((buffer1, offset, length, header) -> {
                    final byte[] bytes = new byte[length];
                    buffer1.getBytes(offset, bytes);
                    receivedPayloads.add(bytes);
                    return ControlledFragmentHandler.Action.CONTINUE;
                }, 10);
                if (workCount == 0)
                {
                    Tests.yield();
                }
            }

            assertTrue(persistentSubscription.isLive());

            Tests.await(() -> archive.context().replaySessionCounter().get() == 0);

            try(MediaDriver mediaDriver = MediaDriver.launchEmbedded(); final Aeron aeron =
                Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName())))
            {
                final Subscription subscription = aeron.addSubscription(MDC_CHANNEL, liveStreamId);

                Tests.awaitConnected(subscription);

                offeredPosition = 0;
                for (int i = 0; i < 64; i++)
                {
                    final UnsafeBuffer offerBuffer = new UnsafeBuffer(new byte[1024]);
                    while ((offeredPosition = publication.offer(offerBuffer)) < 0)
                    {
                        Tests.yieldingIdle("offeredPos=" + Publication.errorString(offeredPosition));
                    }

                    while (subscription.poll((buffer1, offset, length, header) ->
                    {
                    }, 1) < 1)
                    {
                        Tests.yield();
                    }
                }

                while (persistentSubscription.isLive())
                {
                    persistentSubscription.controlledPoll((buffer1, offset, length, header) ->
                        ControlledFragmentHandler.Action.CONTINUE, 10);
                }
            }
        }
    }

    private List<byte[]> generateRandomPayloads(final int count)
    {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final List<byte[]> randomPayloads = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
        {
            final int length = random.nextInt(1024);// TODO increase later to add fragmentation
            final byte[] bytes = new byte[length];
            random.nextBytes(bytes);
            randomPayloads.add(bytes);
        }
        return randomPayloads;
    }

    private static final class PersistentSubscriptionListenerImpl implements PersistentSubscriptionListener
    {
        long onLiveCount;

        public void onLive()
        {
            onLiveCount++;
        }
    }
}
