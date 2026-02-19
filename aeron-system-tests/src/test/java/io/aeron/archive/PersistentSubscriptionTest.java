package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.RethrowingErrorHandler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.PersistentSubscription;
import io.aeron.archive.client.PersistentSubscriptionException;
import io.aeron.archive.client.PersistentSubscriptionListener;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.SystemUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.CommonContext.IPC_MEDIA;
import static io.aeron.CommonContext.UDP_CHANNEL;
import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.archive.ArchiveSystemTests.CATALOG_CAPACITY;
import static io.aeron.archive.ArchiveSystemTests.TERM_LENGTH;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class PersistentSubscriptionTest
{
    private static final String MDC_CHANNEL = UDP_CHANNEL + "?control=localhost:2000";
    private static final int STREAM_ID = 1000;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private final MediaDriver.Context driverCtxTpl = new MediaDriver.Context()
        .termBufferSparseFile(true)
        .threadingMode(ThreadingMode.SHARED)
        .publicationTermBufferLength(TERM_LENGTH)
        .ipcTermBufferLength(TERM_LENGTH)
        .dirDeleteOnShutdown(true)
        .imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(3))
        .spiesSimulateConnection(true);

    private final Aeron.Context aeronCtxTpl = new Aeron.Context()
        .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE);

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private TestMediaDriver driver;
    private File archiveDir;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive aeronArchive;

    @BeforeEach
    void setUp()
    {
        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        final MediaDriver.Context driverCtx = driverCtxTpl.clone()
            .aeronDirectoryName(aeronDirectoryName);

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

        aeron = Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeronDirectoryName));

        aeronArchive = AeronArchive.connect(
            TestContexts.localhostAeronArchive()
                .aeron(aeron));
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(
            this::closeCloseables,
            aeronArchive,
            aeron,
            archive,
            driver,
            () -> IoUtil.delete(archiveDir, true));
    }

    @Test
    @InterruptAfter(10)
    void shouldNotRequireEventListener()
    {
        final PersistentSubscriptionListenerImpl listener = null; // <-- null listener
        try (PersistentSubscription persistentSubscription =
            new PersistentSubscription(aeronArchive, 13, 0, IPC_CHANNEL, 1000, listener))
        {
            while (!persistentSubscription.hasFailed())
            {
                persistentSubscription.controlledPoll(null, 1);
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldErrorIfRecordingDoesNotExist()
    {
        final PersistentSubscriptionListenerImpl listener = new PersistentSubscriptionListenerImpl();
        final int recordingId = 13; // <-- does not exist
        try (PersistentSubscription persistentSubscription =
            new PersistentSubscription(aeronArchive, recordingId, 0, IPC_CHANNEL, 1000, listener))
        {
            while (!persistentSubscription.hasFailed())
            {
                persistentSubscription.controlledPoll(null, 1);
            }

            assertEquals(1, listener.errorCount);
            assertEquals(
                PersistentSubscriptionException.Reason.RECORDING_NOT_FOUND,
                ((PersistentSubscriptionException)listener.lastException).reason()
            );
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldErrorIfRecordingPositionIsBeforeStartPosition()
    {
        final String channel = new ChannelUriStringBuilder()
            .media(IPC_MEDIA)
            .initialPosition(1024, 0, TERM_LENGTH) // <-- Recording starts at 1024
            .build();
        final ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(channel, 1000);
        final CountersReader counters = aeron.countersReader();
        final int counterId = Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);

        final PersistentSubscriptionListenerImpl listener = new PersistentSubscriptionListenerImpl();
        final int startPosition = 0; // <-- Trying to start from zero
        try (PersistentSubscription persistentSubscription =
            new PersistentSubscription(aeronArchive, recordingId, startPosition, IPC_CHANNEL, 1000, listener))
        {
            while (!persistentSubscription.hasFailed())
            {
                persistentSubscription.controlledPoll(null, 1);
            }

            assertEquals(1, listener.errorCount);
            assertEquals(
                PersistentSubscriptionException.Reason.INVALID_START_POSITION,
                ((PersistentSubscriptionException)listener.lastException).reason()
            );
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldErrorIfRecordingPositionIsAfterStopPosition()
    {
        final ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(IPC_CHANNEL, 1000);
        final CountersReader counters = aeron.countersReader();
        final int counterId = Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);

        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
        publication.offer(buffer);

        aeronArchive.stopRecording(publication);
        final long stopPosition = aeronArchive.getStopPosition(recordingId);
        assertTrue(stopPosition > 0);
        final PersistentSubscriptionListenerImpl listener = new PersistentSubscriptionListenerImpl();
        final long startPosition = stopPosition * 2; // <-- after end of recording

        try (PersistentSubscription persistentSubscription =
            new PersistentSubscription(aeronArchive, recordingId, startPosition, IPC_CHANNEL, 1000, listener))
        {
            while (!persistentSubscription.hasFailed())
            {
                persistentSubscription.controlledPoll(null, 1);
            }

            assertEquals(1, listener.errorCount);
            assertEquals(
                PersistentSubscriptionException.Reason.INVALID_START_POSITION,
                ((PersistentSubscriptionException)listener.lastException).reason()
            );
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
            new PersistentSubscription(aeronArchive, recordingId, 0, IPC_CHANNEL, 1000, listener))
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
            new PersistentSubscription(aeronArchive, recordingId, 0, MDC_CHANNEL, liveStreamId, listener))
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

            final MediaDriver.Context ctx = new MediaDriver.Context().aeronDirectoryName(CommonContext.generateRandomDirName());
            try (MediaDriver mediaDriver = MediaDriver.launch(ctx);
                final Aeron aeron =
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

    @RepeatedTest(3)
    @InterruptAfter(20)
    void testLiveJoin() throws Exception
    {
        final String pubChannel = "aeron:udp?term-length=16m|control=localhost:24325|control-mode=dynamic|fc=min";
        final String subChannel = "aeron:udp?control=localhost:24325|group=true";

        final String aeronDir2 = CommonContext.generateRandomDirName();
        final MediaDriver.Context driver2Ctx = driverCtxTpl.clone().aeronDirectoryName(aeronDir2);
        addCloseable(TestMediaDriver.launch(driver2Ctx, systemTestWatcher));
        systemTestWatcher.dataCollector().add(driver2Ctx.aeronDirectory());
        final Aeron aeron2 = addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeronDir2)));
        final AeronArchive aeronArchive2 = addCloseable(AeronArchive.connect(
            TestContexts.localhostAeronArchive().aeron(aeron2)));

        final ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(pubChannel, STREAM_ID);
        final Subscription controlSubscription = aeron.addSubscription(subChannel, STREAM_ID);
        Tests.awaitConnected(controlSubscription);

        final CountersReader counters = aeron.countersReader();
        final int counterId = Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);

        final int maxSeconds = 20;
        final int ratePerSecond = 10_000;
        final long maxProcessingTime = 1_000_000_000 / ratePerSecond / 2;
        final long t0 = System.nanoTime();
        final PerSecondStats publisherMessagesPerSecond = new PerSecondStats(t0, maxSeconds);
        final PerSecondStats publisherBpePerSecond = new PerSecondStats(t0, maxSeconds);
        final PerSecondStats controlMessagesPerSecond = new PerSecondStats(t0, maxSeconds);

        final Thread control = new Thread(
            () ->
            {
                final FragmentAssembler handler = new FragmentAssembler(
                    (buffer, offset, length, header) ->
                    {
                        controlMessagesPerSecond.record(System.nanoTime());
                        simulateWork(maxProcessingTime);
                    });
                while (!Thread.currentThread().isInterrupted())
                {
                    controlSubscription.poll(handler, 10);
                }
            },
            "testLiveJoinControl");
        control.start();
        addCloseable(() -> interruptAndJoin(control));

        final Thread publisher = new Thread(
            () ->
            {
                final ThreadLocalRandom random = ThreadLocalRandom.current();
                final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]); // TODO increase to test fragmentation
                long messageId = 0;
                long nextMessageAt = System.nanoTime() + exponentialArrivalDelay(ratePerSecond);
                while (!Thread.currentThread().isInterrupted())
                {
                    final long now = System.nanoTime();
                    if (now - nextMessageAt >= 0)
                    {
                        final int length = random.nextInt(2 * SIZE_OF_LONG, buffer.capacity() + 1);
                        buffer.putLong(0, messageId);
                        buffer.putLong(length - SIZE_OF_LONG, messageId);
                        final long result = publication.offer(buffer, 0, length);
                        if (result > 0)
                        {
                            messageId++;
                            nextMessageAt = now + exponentialArrivalDelay(ratePerSecond);
                            publisherMessagesPerSecond.record(now);
                        }
                        else if (result == BACK_PRESSURED)
                        {
                            publisherBpePerSecond.record(now);
                        }
                    }
                }
            },
            "testLiveJoinPublisher");
        publisher.start();
        addCloseable(() -> interruptAndJoin(publisher));

        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(4));

        try (PersistentSubscription persistentSubscription =
            new PersistentSubscription(aeronArchive2, recordingId, 0, subChannel, STREAM_ID, null))
        {
            final MessageVerifier handler = new MessageVerifier(maxProcessingTime);
            final ControlledFragmentAssembler assembler = new ControlledFragmentAssembler(handler);

            while (!persistentSubscription.isLive())
            {
                if (persistentSubscription.controlledPoll(assembler, 10) == 0)
                {
                    checkForInterrupt("failed to transition to live");
                }
            }

            interruptAndJoin(publisher);
            final long lastPosition = publication.position();

            while (handler.position < lastPosition)
            {
                if (persistentSubscription.controlledPoll(assembler, 10) == 0)
                {
                    checkForInterrupt("failed to drain the stream");
                }
            }

            interruptAndJoin(control);

            final int elapsedSeconds = (int)((System.nanoTime() - t0 + 999_999_999) / 1_000_000_000);

            System.out.println("join error = " + persistentSubscription.joinError());
            System.out.println("expected rate per second = " + ratePerSecond);
            System.out.println("second,published,publisherBpe,control");
            for (int i = 0; i < elapsedSeconds; i++)
            {
                System.out.println(i +
                                   "," + publisherMessagesPerSecond.get(i) +
                                   "," + publisherBpePerSecond.get(i) +
                                   "," + controlMessagesPerSecond.get(i));
            }
        }
    }

    private static void interruptAndJoin(final Thread thread) throws InterruptedException
    {
        thread.interrupt();
        thread.join();
    }

    private static void checkForInterrupt(final String message)
    {
        if (Thread.interrupted())
        {
            fail(message);
        }
    }

    private static void simulateWork(final long maxProcessingTime)
    {
        if (maxProcessingTime > 0)
        {
            LockSupport.parkNanos(ThreadLocalRandom.current().nextLong(maxProcessingTime));
        }
    }

    private static long exponentialArrivalDelay(final long ratePerSecond)
    {
        final double uniform = ThreadLocalRandom.current().nextDouble();
        final double secondFraction = -Math.log(1.0 - uniform) / ratePerSecond;
        return (long)(secondFraction * 1e9);
    }

    private static final class MessageVerifier implements ControlledFragmentHandler
    {
        private final long maxProcessingTime;
        long expectedMessageId;
        long position;

        private MessageVerifier(final long maxProcessingTime)
        {
            this.maxProcessingTime = maxProcessingTime;
        }

        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            if (length < 2 * SIZE_OF_LONG)
            {
                throw new IllegalStateException("length was " + length);
            }
            final long messageId1 = buffer.getLong(offset);
            final long messageId2 = buffer.getLong(offset + length - SIZE_OF_LONG);
            if (messageId1 != messageId2)
            {
                throw new IllegalStateException("message had different ids " + messageId1 + " and " + messageId2);
            }
            if (messageId1 != expectedMessageId)
            {
                throw new IllegalStateException("expected id " + expectedMessageId + ", but got " + messageId1);
            }
            expectedMessageId = messageId1 + 1;
            position = header.position();
            simulateWork(maxProcessingTime);
            return Action.CONTINUE;
        }
    }

    private static final class PerSecondStats
    {
        private final long[] perSecond;
        private final long t0;

        PerSecondStats(final long t0, final int maxSeconds)
        {
            this.t0 = t0;
            this.perSecond = new long[maxSeconds];
        }

        void record(final long ts)
        {
            final int second = (int)((ts - t0) / 1_000_000_000);
            if (second < perSecond.length)
            {
                perSecond[second]++;
            }
        }

        long get(final int second)
        {
            return perSecond[second];
        }
    }

    private <T extends AutoCloseable> T addCloseable(final T closeable)
    {
        closeables.add(closeable);
        return closeable;
    }

    private void closeCloseables() throws Exception
    {
        Exception ex = null;

        for (int i = closeables.size() - 1; i >= 0; i--)
        {
            final AutoCloseable closeable = closeables.get(i);
            try
            {
                closeable.close();
            }
            catch (final Exception e)
            {
                if (ex == null)
                {
                    ex = e;
                }
                else
                {
                    ex.addSuppressed(e);
                }
            }
        }

        if (ex != null)
        {
            throw ex;
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
        int errorCount = 0;
        Exception lastException = null;

        public void onLive()
        {
            onLiveCount++;
        }

        public void onError(final Exception e)
        {
            errorCount++;
            lastException = e;
        }
    }
}
