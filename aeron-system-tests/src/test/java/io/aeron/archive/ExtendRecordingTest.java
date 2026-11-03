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
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.samples.archive.RecordingDescriptor;
import io.aeron.samples.archive.RecordingDescriptorCollector;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.LangUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.ArchiveSystemTests.FRAGMENT_LIMIT;
import static io.aeron.archive.ArchiveSystemTests.TERM_LENGTH;
import static io.aeron.archive.ArchiveSystemTests.awaitSignal;
import static io.aeron.archive.ArchiveSystemTests.injectRecordingSignalConsumer;
import static io.aeron.archive.client.ArchiveException.ACTIVE_RECORDING;
import static io.aeron.archive.codecs.RecordingSignal.EXTEND;
import static io.aeron.archive.codecs.RecordingSignal.START;
import static io.aeron.archive.codecs.RecordingSignal.STOP;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class ExtendRecordingTest
{
    private static final String MY_ALIAS = "my-log";
    private static final String MESSAGE_PREFIX = "Message-Prefix-";
    private static final int MTU_LENGTH = Configuration.mtuLength();

    private static final int RECORDED_STREAM_ID = 33;
    private static final String RECORDED_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:3333")
        .mtu(MTU_LENGTH)
        .termLength(TERM_LENGTH)
        .alias(MY_ALIAS)
        .build();

    private static final int REPLAY_STREAM_ID = 66;
    private static final String REPLAY_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:6666")
        .build();

    private static final String EXTEND_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:3333")
        .build();

    private TestMediaDriver driver;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive aeronArchive;

    private TestRecordingSignalConsumer recordingSignalConsumer;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @BeforeEach
    void before(@TempDir final Path tempDir)
    {
        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .spiesSimulateConnection(false)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true);

        final Archive.Context archiveCtx = TestContexts.localhostArchive()
            .catalogCapacity(ArchiveSystemTests.CATALOG_CAPACITY)
            .aeronDirectoryName(aeronDirectoryName)
            .archiveDir(tempDir.resolve("archive").toFile())
            .fileSyncLevel(0)
            .segmentFileLength(TERM_MIN_LENGTH)
            .threadingMode(ArchiveThreadingMode.SHARED);

        driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());
        archive = Archive.launch(archiveCtx.clone());
        systemTestWatcher.dataCollector().add(archiveCtx.archiveDir());

        aeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(aeronDirectoryName));

        aeronArchive = AeronArchive.connect(TestContexts.localhostAeronArchive().aeron(aeron).errorHandler(null));

        recordingSignalConsumer = injectRecordingSignalConsumer(aeronArchive);
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeronArchive, aeron, archive, driver);
    }

    private interface PublicationFactory
    {
        Publication create(Aeron aeron, String uri, int streamId);
    }

    @InterruptAfter(10)
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldExtendRecordingAndReplay(final boolean exclusive)
    {
        final int messageCount = 10;
        final long subscriptionIdOne;
        final long subscriptionIdTwo;
        final long stopOne;
        final long stopTwo;
        final long recordingId;

        final PublicationFactory publicationFactory =
            exclusive ? Aeron::addExclusivePublication : Aeron::addPublication;

        try (Publication publication = publicationFactory.create(aeron, RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {

            subscriptionIdOne = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);
            awaitSignal(aeronArchive, recordingSignalConsumer, START);

            try
            {
                offer(publication, 0, messageCount);

                final CountersReader counters = aeron.countersReader();
                final int counterId =
                    RecordingPos.findCounterIdBySession(counters, publication.sessionId(), aeronArchive.archiveId());
                recordingId = RecordingPos.getRecordingId(counters, counterId);

                consume(subscription, 0, messageCount);

                stopOne = publication.position();
                Tests.awaitPosition(counters, counterId, stopOne);
            }
            finally
            {
                final long recId = recordingSignalConsumer.recordingId;
                recordingSignalConsumer.reset();
                aeronArchive.stopRecording(subscriptionIdOne);
                awaitSignal(aeronArchive, recordingSignalConsumer, recId, STOP);
            }
        }

        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(10);
        assertEquals(
            1L, aeronArchive.listRecordingsForUri(0, 10, "alias=" + MY_ALIAS, RECORDED_STREAM_ID, collector.reset()));
        final RecordingDescriptor recording = collector.descriptors().get(0);
        assertEquals(recordingId, recording.recordingId());

        final String publicationExtendChannel = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("localhost:3333")
            .initialPosition(recording.stopPosition(), recording.initialTermId(), recording.termBufferLength())
            .mtu(recording.mtuLength())
            .alias(MY_ALIAS)
            .build();

        try (Subscription subscription = Tests.reAddSubscription(aeron, EXTEND_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = publicationFactory.create(aeron, publicationExtendChannel, RECORDED_STREAM_ID))
        {
            recordingSignalConsumer.reset();
            subscriptionIdTwo = aeronArchive.extendRecording(recordingId, EXTEND_CHANNEL, RECORDED_STREAM_ID, LOCAL);
            awaitSignal(aeronArchive, recordingSignalConsumer, recordingId, EXTEND);

            try
            {
                offer(publication, messageCount, messageCount);

                final CountersReader counters = aeron.countersReader();
                final int counterId =
                    RecordingPos.findCounterIdBySession(counters, publication.sessionId(), aeronArchive.archiveId());

                consume(subscription, messageCount, messageCount);

                stopTwo = publication.position();
                Tests.awaitPosition(counters, counterId, stopTwo);
            }
            finally
            {
                aeronArchive.stopRecording(subscriptionIdTwo);
                awaitSignal(aeronArchive, recordingSignalConsumer, recordingId, STOP);
            }
        }

        replay(messageCount, stopTwo, recordingId);
    }

    @Test
    @SuppressWarnings("MethodLength")
    void shouldTruncateAndExtendFromTheMiddleOfTheTerm()
    {
        final int[] data = ThreadLocalRandom.current().ints(5000).toArray();
        final BufferClaim bufferClaim = new BufferClaim();

        final int termLength = TERM_MIN_LENGTH;
        final String channel = "aeron:ipc?ssc=true|term-length=" + termLength;
        final int streamId = 42;
        final int initialTermId;
        final long recordingId;
        final long publicationPosition;
        try (ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(channel, streamId))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
            recordingId = RecordingPos.getRecordingId(counters, counterId);

            Tests.awaitConnected(publication);
            initialTermId = publication.initialTermId();

            for (final int value : data)
            {
                while (publication.tryClaim(SIZE_OF_INT, bufferClaim) < 0)
                {
                    Tests.yield();
                }

                bufferClaim.buffer().putInt(bufferClaim.offset(), value);
                bufferClaim.commit();
            }

            Tests.awaitPosition(counters, counterId, publication.position());
            publicationPosition = publication.position();
        }

        Tests.await(() -> publicationPosition == aeronArchive.getStopPosition(recordingId));

        final int truncateIndex = 1139;
        final int truncatePosition = truncateIndex * 64;
        assertEquals(3, aeronArchive.truncateRecording(recordingId, truncatePosition));

        final int extendMessageCount = 100;
        final long extendPosition;
        try (ExclusivePublication publication = aeron.addExclusivePublication(
            new ChannelUriStringBuilder(channel).initialPosition(truncatePosition, initialTermId, termLength).build(),
            streamId))
        {
            assertNotEquals(NULL_VALUE, aeronArchive.extendRecording(recordingId, channel, streamId, LOCAL, true));

            final CountersReader counters = aeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());

            Tests.awaitConnected(publication);

            for (int i = 0; i < extendMessageCount; i++)
            {
                while (publication.tryClaim(SIZE_OF_INT, bufferClaim) < 0)
                {
                    Tests.yield();
                }

                bufferClaim.buffer().putInt(bufferClaim.offset(), data[i]);
                bufferClaim.commit();
            }

            Tests.awaitPosition(counters, counterId, publication.position());
            extendPosition = publication.position();
        }

        Tests.await(() -> extendPosition == aeronArchive.getStopPosition(recordingId));

        final String replayChannel = "aeron:ipc";
        final int replayStreamId = -96;
        try (Subscription subscription =
            aeronArchive.replay(recordingId, termLength, Long.MAX_VALUE, replayChannel, replayStreamId))
        {
            Tests.awaitConnected(subscription);

            assertEquals(1, subscription.imageCount());
            final Image image = subscription.imageAtIndex(0);
            final MutableInteger msgCount = new MutableInteger();
            final int[] replayData = new int[300];
            final FragmentHandler fragmentHandler = (buffer, offset, length, header) ->
            {
                replayData[msgCount.get()] = buffer.getInt(offset);
                msgCount.increment();
            };

            while (image.position() < extendPosition && 0 == subscription.poll(fragmentHandler, Integer.MAX_VALUE))
            {
                Tests.yield();
            }

            final int replayIndex = 1024;
            assertEquals(truncateIndex - replayIndex + extendMessageCount, msgCount.get());

            int j = 0;
            for (int i = replayIndex; i < truncateIndex; i++, j++)
            {
                assertEquals(data[i], replayData[j]);
            }
            for (int i = 0; i < extendMessageCount; i++, j++)
            {
                assertEquals(data[i], replayData[j]);
            }
            assertEquals(msgCount.get(), j);
        }
    }

    @Test
    void shouldDisallowConcurrentExtendRecordingOperations() throws InterruptedException
    {
        final String channel = "aeron:ipc?term-length=64k|mtu=1408|init-term-id=5|term-id=15|term-offset=1024";
        final int streamId = 555;
        final long recordingId;
        try (ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(channel, streamId))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
            recordingId = RecordingPos.getRecordingId(counters, counterId);

            recordingSignalConsumer.reset();
            assertTrue(aeronArchive.tryStopRecordingByIdentity(recordingId));
            awaitSignal(aeronArchive, recordingSignalConsumer, recordingId, STOP);
        }

        final int threads = 3;
        final CountDownLatch start = new CountDownLatch(threads + 1);
        final CountDownLatch end = new CountDownLatch(threads);
        final AtomicInteger success = new AtomicInteger();
        final AtomicInteger error = new AtomicInteger();
        final AtomicReference<Throwable> exception = new AtomicReference<>();

        for (int i = 0; i < threads; i++)
        {
            final Thread thread = new Thread(() ->
            {
                try (ExclusivePublication publication = aeron.addExclusivePublication(channel, streamId);
                    AeronArchive localArchive = AeronArchive.connect(
                        TestContexts.localhostAeronArchive().aeron(aeron).errorHandler(null)))
                {
                    start.countDown();
                    start.await();

                    try
                    {
                        localArchive.extendRecording(
                            recordingId, ChannelUri.addSessionId(channel, publication.sessionId()), streamId, LOCAL);
                        success.incrementAndGet();

                        while (error.get() != threads - 1)
                        {
                            localArchive.checkForErrorResponse();
                            Tests.sleep(1);
                        }
                    }
                    catch (final ArchiveException ex)
                    {
                        assertEquals(ACTIVE_RECORDING, ex.errorCode());
                        error.incrementAndGet();
                    }
                }
                catch (final Exception ex)
                {
                    if (null != exception.get() || !exception.compareAndSet(null, ex))
                    {
                        exception.get().addSuppressed(ex);
                    }
                    error.incrementAndGet();
                }
                finally
                {
                    end.countDown();
                }
            });
            thread.setDaemon(true);
            thread.start();
        }

        start.countDown();
        start.await();

        end.await();

        if (null != exception.get())
        {
            LangUtil.rethrowUnchecked(exception.get());
        }
        assertThat(success.get(), lessThanOrEqualTo(threads));
        assertEquals(threads - 1, error.get());
    }

    @Test
    void shouldRejectExtendRecordingAttemptIfRecordingSessionIsPresent()
    {
        final String channel = "aeron:ipc?term-length=64k";
        final int streamId = 777;
        try (ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(channel, streamId))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);

            final ChannelUri uri = ChannelUri.parse(channel);
            uri.initialPosition(publication.position(), publication.initialTermId(), publication.termBufferLength());
            final ArchiveException exception = assertThrowsExactly(
                ArchiveException.class,
                () -> aeronArchive.extendRecording(recordingId, uri.toString(), streamId, LOCAL));
            assertEquals(ACTIVE_RECORDING, exception.errorCode());
        }
    }

    private void replay(final int messageCount, final long secondStopPosition, final long recordingId)
    {
        final long fromPosition = 0L;
        final long length = secondStopPosition - fromPosition;

        try (Subscription subscription = aeronArchive.replay(
            recordingId, fromPosition, length, REPLAY_CHANNEL, REPLAY_STREAM_ID))
        {
            consume(subscription, 0, messageCount * 2);
            assertEquals(secondStopPosition, subscription.imageAtIndex(0).position());
        }
    }

    private static void offer(final Publication publication, final int startIndex, final int count)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

        for (int i = startIndex; i < (startIndex + count); i++)
        {
            final int length = buffer.putStringWithoutLengthAscii(0, MESSAGE_PREFIX + i);

            while (publication.offer(buffer, 0, length) <= 0)
            {
                Tests.yield();
            }
        }
    }

    private static void consume(final Subscription subscription, final int startIndex, final int count)
    {
        final MutableInteger received = new MutableInteger(startIndex);

        final FragmentHandler fragmentHandler = new FragmentAssembler(
            (buffer, offset, length, header) ->
            {
                final String expected = MESSAGE_PREFIX + received.value;
                final String actual = buffer.getStringWithoutLengthAscii(offset, length);

                assertEquals(expected, actual);

                received.value++;
            });

        while (received.value < (startIndex + count))
        {
            if (0 == subscription.poll(fragmentHandler, FRAGMENT_LIMIT))
            {
                Tests.yield();
            }
        }

        assertEquals(startIndex + count, received.get());
    }
}
