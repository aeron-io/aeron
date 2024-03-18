/*
 * Copyright 2014-2024 Real Logic Limited.
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
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.Archive.Context;
import io.aeron.archive.checksum.Checksum;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.RecordingSubscriptionDescriptorConsumer;
import io.aeron.archive.codecs.TruncateRecordingRequestDecoder;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.security.AuthorisationServiceSupplier;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

import static io.aeron.archive.ArchiveThreadingMode.DEDICATED;
import static io.aeron.archive.ArchiveThreadingMode.SHARED;
import static io.aeron.archive.Catalog.MIN_CAPACITY;
import static io.aeron.archive.client.AeronArchive.segmentFileBasePosition;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.test.TestContexts.*;
import static org.agrona.BitUtil.align;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(InterruptingTestCallback.class)
@SuppressWarnings("try")
class ArchiveTest
{
    private static final FragmentHandler NO_OP_FRAGMENT_HANDLER = (buffer, offset, length, header) -> {};

    @Test
    void shouldGenerateRecordingName()
    {
        final long recordingId = 1L;
        final long segmentPosition = 2 * 64 * 1024;
        final String expected = "1-" + (2 * 64 * 1024) + ".rec";

        final String actual = Archive.segmentFileName(recordingId, segmentPosition);

        assertEquals(expected, actual);
    }

    @Test
    void shouldCalculateCorrectSegmentFilePosition()
    {
        final int termLength = LogBufferDescriptor.TERM_MIN_LENGTH;
        final int segmentLength = termLength * 8;

        long startPosition = 0;
        long position = 0;
        assertEquals(0L, segmentFileBasePosition(startPosition, position, termLength, segmentLength));

        startPosition = 0;
        position = termLength * 2;
        assertEquals(0L, segmentFileBasePosition(startPosition, position, termLength, segmentLength));

        startPosition = 0;
        position = segmentLength;
        assertEquals(segmentLength, segmentFileBasePosition(startPosition, position, termLength, segmentLength));

        startPosition = termLength;
        position = termLength;
        assertEquals(startPosition, segmentFileBasePosition(startPosition, position, termLength, segmentLength));

        startPosition = termLength * 4;
        position = termLength * 4;
        assertEquals(startPosition, segmentFileBasePosition(startPosition, position, termLength, segmentLength));

        startPosition = termLength;
        position = termLength + segmentLength;
        assertEquals(
            termLength + segmentLength, segmentFileBasePosition(startPosition, position, termLength, segmentLength));

        startPosition = termLength;
        position = termLength + segmentLength - FrameDescriptor.FRAME_ALIGNMENT;
        assertEquals(termLength, segmentFileBasePosition(startPosition, position, termLength, segmentLength));

        startPosition = termLength + FrameDescriptor.FRAME_ALIGNMENT;
        position = termLength + segmentLength;
        assertEquals(
            termLength + segmentLength, segmentFileBasePosition(startPosition, position, termLength, segmentLength));
    }

    @Test
    void shouldAllowMultipleConnectionsInParallel() throws InterruptedException
    {
        final int numberOfArchiveClients = 5;
        final long connectTimeoutNs = TimeUnit.SECONDS.toNanos(10);
        final CountDownLatch latch = new CountDownLatch(numberOfArchiveClients);
        final ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(numberOfArchiveClients);
        final ManyToOneConcurrentLinkedQueue<AeronArchive> archiveClientQueue = new ManyToOneConcurrentLinkedQueue<>();
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .errorHandler(Tests::onError)
            .clientLivenessTimeoutNs(connectTimeoutNs)
            .dirDeleteOnStart(true)
            .publicationUnblockTimeoutNs(connectTimeoutNs * 2)
            .threadingMode(ThreadingMode.SHARED);
        final Archive.Context archiveCtx = TestContexts.localhostArchive()
            .threadingMode(SHARED)
            .connectTimeoutNs(connectTimeoutNs);
        executor.prestartAllCoreThreads();

        try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(driverCtx, archiveCtx))
        {
            for (int i = 0; i < numberOfArchiveClients; i++)
            {
                executor.execute(
                    () ->
                    {
                        final AeronArchive.Context ctx = TestContexts.localhostAeronArchive()
                            .messageTimeoutNs(connectTimeoutNs);
                        final AeronArchive archive = AeronArchive.connect(ctx);
                        archiveClientQueue.add(archive);
                        latch.countDown();
                    });
            }

            assertTrue(latch.await(driver.archive().context().connectTimeoutNs() * 2, TimeUnit.NANOSECONDS));

            AeronArchive archiveClient;
            while (null != (archiveClient = archiveClientQueue.poll()))
            {
                CloseHelper.quietClose(archiveClient);
            }
        }
        finally
        {
            executor.shutdownNow();
            archiveCtx.deleteDirectory();
            driverCtx.deleteDirectory();
        }
    }

    @Test
    void shouldRecoverRecordingWithNonZeroStartPosition()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);
        final Archive.Context archiveCtx = TestContexts.localhostArchive().threadingMode(SHARED);

        long resultingPosition;
        final int initialPosition = DataHeaderFlyweight.HEADER_LENGTH * 9;
        final long recordingId;

        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(driverCtx.clone(), archiveCtx.clone());
            AeronArchive archive = AeronArchive.connect(TestContexts.localhostAeronArchive()))
        {
            final int termLength = 128 * 1024;
            final int initialTermId = 29;

            final String channel = new ChannelUriStringBuilder()
                .media(CommonContext.IPC_MEDIA)
                .initialPosition(initialPosition, initialTermId, termLength)
                .build();

            final Publication publication = archive.addRecordedExclusivePublication(channel, 1);
            final DirectBuffer buffer = new UnsafeBuffer("Hello World".getBytes(StandardCharsets.US_ASCII));

            while ((resultingPosition = publication.offer(buffer)) <= 0)
            {
                Tests.yield();
            }

            final Aeron aeron = archive.context().aeron();

            int counterId;
            final int sessionId = publication.sessionId();
            final CountersReader countersReader = aeron.countersReader();
            while (Aeron.NULL_VALUE == (counterId = RecordingPos.findCounterIdBySession(
                countersReader, sessionId, archive.archiveId())))
            {
                Tests.yield();
            }

            recordingId = RecordingPos.getRecordingId(countersReader, counterId);

            while (countersReader.getCounterValue(counterId) < resultingPosition)
            {
                Tests.yield();
            }
        }

        try (Catalog catalog = openCatalog(archiveCtx.archiveDirectoryName()))
        {
            final Catalog.CatalogEntryProcessor catalogEntryProcessor =
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                descriptorEncoder.stopPosition(Aeron.NULL_VALUE);

            assertTrue(catalog.forEntry(recordingId, catalogEntryProcessor));
        }

        final Archive.Context archiveCtxClone = archiveCtx.clone();
        final MediaDriver.Context driverCtxClone = driverCtx.clone();
        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(driverCtxClone, archiveCtxClone);
            AeronArchive archive = AeronArchive.connect(TestContexts.localhostAeronArchive()))
        {
            assertEquals(initialPosition, archive.getStartPosition(recordingId));
            assertEquals(resultingPosition, archive.getStopPosition(recordingId));
        }
        finally
        {
            archiveCtxClone.deleteDirectory();
            driverCtxClone.deleteDirectory();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldListRegisteredRecordingSubscriptions()
    {
        final int expectedStreamId = 7;
        final String channelOne = "aeron:ipc";
        final String channelTwo = "aeron:udp?endpoint=localhost:5678";
        final String channelThree = "aeron:udp?endpoint=localhost:4321";

        final ArrayList<SubscriptionDescriptor> descriptors = new ArrayList<>();
        @SuppressWarnings("Indentation") final RecordingSubscriptionDescriptorConsumer consumer =
            (controlSessionId, correlationId, subscriptionId, streamId, strippedChannel) ->
                descriptors.add(new SubscriptionDescriptor(
                    controlSessionId, correlationId, subscriptionId, streamId, strippedChannel));

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);
        final Archive.Context archiveCtx = TestContexts.localhostArchive().threadingMode(SHARED);
        assertNull(archiveCtx.aeron());
        assertFalse(archiveCtx.ownsAeronClient());

        try (ArchivingMediaDriver archivingMediaDriver = ArchivingMediaDriver.launch(driverCtx, archiveCtx);
            AeronArchive aeronArchive = AeronArchive.connect(TestContexts.localhostAeronArchive()))
        {
            final Context archiveContext = archivingMediaDriver.archive().context();
            assertNotNull(archiveContext.aeron());
            assertTrue(archiveContext.ownsAeronClient());
            assertTrue(archiveContext.aeron().context().useConductorAgentInvoker());

            final AeronArchive.Context clientContext = aeronArchive.context();
            assertNotNull(clientContext.aeron());
            assertTrue(clientContext.ownsAeronClient());

            final long subIdOne = aeronArchive.startRecording(channelOne, expectedStreamId, LOCAL);
            final long subIdTwo = aeronArchive.startRecording(channelTwo, expectedStreamId + 1, LOCAL);
            final long subOdThree = aeronArchive.startRecording(channelThree, expectedStreamId + 2, LOCAL);

            final int countOne = aeronArchive.listRecordingSubscriptions(
                0, 5, "ipc", expectedStreamId, true, consumer);

            assertEquals(1, descriptors.size());
            assertEquals(1, countOne);

            descriptors.clear();
            final int countTwo = aeronArchive.listRecordingSubscriptions(
                0, 5, "", expectedStreamId, false, consumer);

            assertEquals(3, descriptors.size());
            assertEquals(3, countTwo);

            aeronArchive.stopRecording(subIdTwo);

            descriptors.clear();
            final int countThree = aeronArchive.listRecordingSubscriptions(
                0, 5, "", expectedStreamId, false, consumer);

            assertEquals(2, descriptors.size());
            assertEquals(2, countThree);

            assertEquals(1L, descriptors.stream().filter((sd) -> sd.subscriptionId == subIdOne).count());
            assertEquals(1L, descriptors.stream().filter((sd) -> sd.subscriptionId == subOdThree).count());
        }
        finally
        {
            archiveCtx.deleteDirectory();
            driverCtx.deleteDirectory();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldErrorOnLowSpace() throws IOException
    {
        final int streamId = 7;
        final String channel = "aeron:ipc";
        final long usableSpace = 100;
        final long threshold = 101;
        final FileStore fileStore = mock(FileStore.class);
        when(fileStore.getUsableSpace()).thenReturn(usableSpace);

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);
        final Archive.Context archiveCtx = TestContexts.localhostArchive()
            .archiveFileStore(fileStore)
            .lowStorageSpaceThreshold(threshold)
            .deleteArchiveOnStart(true)
            .threadingMode(SHARED);

        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(driverCtx, archiveCtx);
            AeronArchive archive = AeronArchive.connect(TestContexts.localhostAeronArchive()))
        {
            try
            {
                archive.startRecording(channel, streamId, LOCAL);
            }
            catch (final ArchiveException ex)
            {
                assertEquals(ArchiveException.STORAGE_SPACE, ex.errorCode());
                return;
            }

            fail("Expected exception");
        }
        finally
        {
            archiveCtx.deleteDirectory();
            driverCtx.deleteDirectory();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldErrorWhenUnauthorised()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);

        final AuthorisationServiceSupplier authorisationServiceSupplier = () ->
            (protocolId, actionId, type, encodedPrincipal) -> actionId != TruncateRecordingRequestDecoder.TEMPLATE_ID;

        final Archive.Context archiveCtx = TestContexts.localhostArchive()
            .deleteArchiveOnStart(true)
            .authorisationServiceSupplier(authorisationServiceSupplier)
            .threadingMode(SHARED);

        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(driverCtx, archiveCtx);
            AeronArchive archive = AeronArchive.connect(TestContexts.localhostAeronArchive()))
        {
            try
            {
                archive.truncateRecording(0, 0);
            }
            catch (final ArchiveException ex)
            {
                assertEquals(ArchiveException.UNAUTHORISED_ACTION, ex.errorCode());
                return;
            }

            fail("Expected exception");
        }
        finally
        {
            archiveCtx.deleteDirectory();
            driverCtx.deleteDirectory();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldResolveArchiveId(@TempDir final Path dir)
    {
        final Path aeronDir = dir.resolve("driver");
        final Path archive1Dir = dir.resolve("archive1");
        final Path archive2Dir = dir.resolve("archive2");
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED)
            .aeronDirectoryName(aeronDir.toString());

        try (MediaDriver ignore = MediaDriver.launch(driverCtx);
            Archive archive1 = Archive.launch(
                new Archive.Context()
                .controlChannel(LOCALHOST_CONTROL_REQUEST_CHANNEL)
                .replicationChannel(LOCALHOST_REPLICATION_CHANNEL)
                .deleteArchiveOnStart(true)
                .threadingMode(SHARED)
                .aeronDirectoryName(aeronDir.toString())
                .archiveDir(archive1Dir.toFile())
                .archiveId(42));
            Archive archive2 = Archive.launch(
                new Archive.Context()
                .controlChannel("aeron:udp?endpoint=localhost:8011")
                .replicationChannel(LOCALHOST_REPLICATION_CHANNEL)
                .deleteArchiveOnStart(true)
                .threadingMode(SHARED)
                .aeronDirectoryName(aeronDir.toString())
                .archiveDir(archive2Dir.toFile()));
            AeronArchive client1 = AeronArchive.connect(new AeronArchive.Context()
                .aeronDirectoryName(aeronDir.toString())
                .controlRequestChannel(archive1.context().controlChannel())
                .controlResponseChannel(LOCALHOST_CONTROL_RESPONSE_CHANNEL));
            AeronArchive client2 = AeronArchive.connect(new AeronArchive.Context()
                .aeronDirectoryName(aeronDir.toString())
                .controlRequestChannel(archive2.context().controlChannel())
                .controlResponseChannel(LOCALHOST_CONTROL_RESPONSE_CHANNEL)))
        {
            assertEquals(42, client1.archiveId());
            assertEquals(archive2.context().archiveId(), client2.archiveId());
        }
    }

    @Test
    void dataBufferIsAllocatedOnDemand()
    {
        final Context context = new Context();

        final UnsafeBuffer buffer = context.dataBuffer();

        assertNotNull(buffer);
        assertEquals(context.fileIoMaxLength(), buffer.capacity());
        assertSame(buffer, context.dataBuffer());
    }

    @Test
    void dataBufferReturnsValueAssigned()
    {
        final UnsafeBuffer buffer = mock(UnsafeBuffer.class);
        final Context context = new Context();
        context.dataBuffer(buffer);

        assertSame(buffer, context.dataBuffer());
    }

    @Test
    void replayBufferIsAllocatedOnDemandIfThreadingModeIsDEDICATED()
    {
        final Context context = new Context().threadingMode(DEDICATED);

        final UnsafeBuffer buffer = context.replayBuffer();

        assertNotNull(buffer);
        assertEquals(context.fileIoMaxLength(), buffer.capacity());
        assertSame(buffer, context.replayBuffer());
        assertNotSame(context.dataBuffer(), buffer);
    }

    @Test
    void replayBufferReturnsValueAssignedIfThreadingModeIsDEDICATED()
    {
        final UnsafeBuffer buffer = mock(UnsafeBuffer.class);
        final Archive.Context context = new Archive.Context().threadingMode(DEDICATED);
        context.replayBuffer(buffer);

        assertSame(buffer, context.replayBuffer());
    }

    @ParameterizedTest
    @EnumSource(value = ArchiveThreadingMode.class, mode = EXCLUDE, names = "DEDICATED")
    void replayBufferReturnsDataBufferIfThreadingModeIsNotDEDICATED(final ArchiveThreadingMode threadingMode)
    {
        final Archive.Context context = new Archive.Context().threadingMode(threadingMode);

        final UnsafeBuffer buffer = context.replayBuffer();

        assertSame(context.dataBuffer(), buffer);
    }

    @Test
    void recordChecksumBufferReturnsNullIfRecordChecksumIsNull()
    {
        final Archive.Context context = new Archive.Context();
        assertNull(context.recordChecksumBuffer());
    }

    @Test
    void recordChecksumBufferIsAllocatedOnDemandIfThreadingModeIsDEDICATED()
    {
        final Checksum recordChecksum = mock(Checksum.class);
        final Archive.Context context = new Archive.Context().recordChecksum(recordChecksum).threadingMode(DEDICATED);

        final UnsafeBuffer buffer = context.recordChecksumBuffer();

        assertNotNull(buffer);
        assertEquals(context.fileIoMaxLength(), buffer.capacity());
        assertSame(buffer, context.recordChecksumBuffer());
        assertNotSame(context.dataBuffer(), buffer);
    }

    @Test
    void recordChecksumBufferReturnsValueAssignedIfThreadingModeIsDEDICATED()
    {
        final UnsafeBuffer buffer = mock(UnsafeBuffer.class);
        final Checksum recordChecksum = mock(Checksum.class);
        final Archive.Context context = new Archive.Context().recordChecksum(recordChecksum).threadingMode(DEDICATED);
        context.recordChecksumBuffer(buffer);

        assertSame(buffer, context.recordChecksumBuffer());
    }

    @ParameterizedTest
    @EnumSource(value = ArchiveThreadingMode.class, mode = EXCLUDE, names = "DEDICATED")
    void recordChecksumBufferReturnsDataBufferIfThreadingModeIsNotDEDICATED(final ArchiveThreadingMode threadingMode)
    {
        final Checksum recordChecksum = mock(Checksum.class);
        final Archive.Context context = new Archive.Context()
            .recordChecksum(recordChecksum)
            .threadingMode(threadingMode);

        final UnsafeBuffer buffer = context.recordChecksumBuffer();

        assertSame(context.dataBuffer(), buffer);
    }

    @ParameterizedTest
    @ValueSource(strings = { "localhost:0", "localhost:8888" })
    void shouldResolveControlResponseEndpointAddress(final String endpoint)
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);
        final Archive.Context archiveCtx = TestContexts.localhostArchive().threadingMode(SHARED);
        final String controlResponseChannel = "aeron:udp?endpoint=" + endpoint;
        final AeronArchive.Context clientContext = TestContexts.localhostAeronArchive()
            .controlResponseChannel(controlResponseChannel);

        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(driverCtx, archiveCtx);
            AeronArchive archive = AeronArchive.connect(clientContext))
        {
            final int count = archive.listRecordings(0, 10,
                (controlSessionId,
                correlationId,
                recordingId,
                startTimestamp,
                stopTimestamp,
                startPosition,
                stopPosition,
                initialTermId,
                segmentFileLength,
                termBufferLength,
                mtuLength,
                sessionId,
                streamId,
                strippedChannel,
                originalChannel,
                sourceIdentity) -> {});

            assertEquals(0, count);
        }
        finally
        {
            archiveCtx.deleteDirectory();
            driverCtx.deleteDirectory();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldRunWithControlChannelDisabled()
    {
        final int streamId = 7;
        final String channel = "aeron:ipc";

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);
        final Archive.Context archiveCtx = TestContexts.localhostArchive()
            .controlChannelEnabled(false)
            .controlChannel(null)
            .archiveClientContext(new AeronArchive.Context().controlResponseChannel("aeron:udp?endpoint=localhost:0"))
            .deleteArchiveOnStart(true)
            .threadingMode(SHARED);

        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(driverCtx, archiveCtx);
            AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                .controlRequestChannel(archiveCtx.localControlChannel())
                .controlResponseChannel(archiveCtx.localControlChannel()));
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverCtx.aeronDirectoryName())))
        {
            final long subscriptionId = archive.startRecording(channel, streamId, LOCAL);
            try (Publication publication = aeron.addExclusivePublication(channel, streamId))
            {
                final int sessionId = publication.sessionId();

                final CountersReader counters = aeron.countersReader();
                final int counterId = Tests.awaitRecordingCounterId(counters, sessionId, archive.archiveId());

                final BufferClaim bufferClaim = new BufferClaim();
                for (int i = 0; i < 111; i++)
                {
                    while (publication.tryClaim(4, bufferClaim) < 0)
                    {
                        Tests.yield();
                    }
                    bufferClaim.buffer().putInt(bufferClaim.offset(), i);
                    bufferClaim.commit();
                }

                Tests.awaitPosition(counters, counterId, publication.position());
            }
            archive.stopRecording(subscriptionId);
        }
        finally
        {
            archiveCtx.deleteDirectory();
            driverCtx.deleteDirectory();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldReplayPaddingGreaterThanMaxMessageLengthDueToFragmentationNearEndOfTerm() throws IOException
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);
        final Aeron.Context aeronCtx = new Aeron.Context()
            .aeronDirectoryName(driverCtx.aeronDirectoryName());
        final Archive.Context archiveCtx = TestContexts.localhostArchive()
            .deleteArchiveOnStart(true)
            .threadingMode(SHARED);

        try (ArchivingMediaDriver archivingDriver = ArchivingMediaDriver.launch(driverCtx, archiveCtx);
            Aeron aeron = Aeron.connect(aeronCtx);
            AeronArchive archive = AeronArchive.connect(TestContexts.localhostAeronArchive()))
        {
            final long recordingId;
            final long finalPubPos;
            final long finalSubPos;

            final IdleStrategy idleStrategy = YieldingIdleStrategy.INSTANCE;

            final String channel = new ChannelUriStringBuilder()
                .media(CommonContext.IPC_MEDIA)
                .termLength(256 * 1024)
                .build();

            try (Publication publication = archive.addRecordedExclusivePublication(channel, 1))
            {
                int counterId;
                final int sessionId = publication.sessionId();
                final CountersReader countersReader = aeron.countersReader();

                while (Aeron.NULL_VALUE == (counterId = RecordingPos.findCounterIdBySession(
                    countersReader, sessionId, archive.archiveId())))
                {
                    Tests.yield();
                }

                recordingId = RecordingPos.getRecordingId(countersReader, counterId);

                final UnsafeBuffer smallMessage = new UnsafeBuffer(new byte[0]);
                final int maxMessageLength = publication.maxMessageLength();
                final int requiredLength = calculateFragmentedMessageLength(publication, maxMessageLength);
                while (publication.position() + requiredLength <= publication.termBufferLength())
                {
                    if (publication.offer(smallMessage) < 0)
                    {
                        idleStrategy.idle();
                        Tests.checkInterruptStatus();
                    }
                }

                final UnsafeBuffer bigFragmentedMessage = new UnsafeBuffer(new byte[maxMessageLength]);
                while (publication.offer(bigFragmentedMessage) < 0)
                {
                    idleStrategy.idle();
                    Tests.checkInterruptStatus();
                }

                finalPubPos = publication.position();
            }

            try (Subscription subscription = archive.replay(recordingId, 0, Aeron.NULL_VALUE, channel, 2))
            {
                while (subscription.imageCount() == 0)
                {
                    idleStrategy.idle();
                    Tests.checkInterruptStatus();
                }

                final Image image = subscription.imageAtIndex(0);
                final AtomicCounter errorCounter = archivingDriver.archive().context().errorCounter();

                while (!image.isClosed() && !image.isEndOfStream())
                {
                    final int fragmentCount = image.poll(NO_OP_FRAGMENT_HANDLER, 10);
                    idleStrategy.idle(fragmentCount);

                    if (fragmentCount == 0 && errorCounter.get() > 0)
                    {
                        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                            PrintStream outStream = new PrintStream(byteStream))
                        {
                            ArchiveTool.printErrors(outStream, archivingDriver.archive().context().archiveDir());
                            fail(byteStream.toString(StandardCharsets.UTF_8.name()));
                        }
                    }
                }

                finalSubPos = image.position();
            }

            assertEquals(finalPubPos, finalSubPos);
        }
        finally
        {
            archiveCtx.deleteDirectory();
            driverCtx.deleteDirectory();
        }
    }

    @Test
    void shouldRejectArchiveCreationIfAnotherArchiveWithTheSameArchiveIdIsAlreadyRunning(@TempDir final Path root)
    {
        final Path aeronDir = root.resolve("media-driver");
        final Path archiveDir1 = root.resolve("archive1");
        final Path archiveDir2 = root.resolve("archive2");
        final long archiveId = -432946792374923L;
        try (MediaDriver driver =
            MediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(aeronDir.toString()));
            Archive archive = Archive.launch(TestContexts.localhostArchive()
                .archiveId(archiveId)
                .archiveDir(archiveDir1.toFile())
                .aeronDirectoryName(driver.context().aeronDirectoryName())))
        {
            final Context archiveContext2 = TestContexts.localhostArchive()
                .archiveId(archive.context().archiveId())
                .archiveDir(archiveDir2.toFile())
                .aeronDirectoryName(driver.context().aeronDirectoryName());
            try
            {
                final ArchiveException exception = assertThrowsExactly(ArchiveException.class, archiveContext2::conclude);
                assertEquals("ERROR - found existing archive for archiveId=" + archiveId, exception.getMessage());
            }
            finally
            {
                archiveContext2.close();
            }
        }
    }

    private static int calculateFragmentedMessageLength(final Publication publication, final int maxMessageLength)
    {
        final int maxPayloadLength = publication.maxPayloadLength();
        final int numMaxPayloads = maxMessageLength / maxPayloadLength;
        final int remainingPayload = maxMessageLength % maxPayloadLength;
        final int lastFrameLength = remainingPayload > 0 ? align(remainingPayload + HEADER_LENGTH, FRAME_ALIGNMENT) : 0;
        return (numMaxPayloads * (maxPayloadLength + HEADER_LENGTH)) + lastFrameLength;
    }

    private static Catalog openCatalog(final String archiveDirectoryName)
    {
        final IntConsumer intConsumer = (version) ->
        {
        };
        return new Catalog(
            new File(archiveDirectoryName),
            new SystemEpochClock(),
            MIN_CAPACITY,
            true,
            null,
            intConsumer,
            false);
    }

    static final class SubscriptionDescriptor
    {
        final long controlSessionId;
        final long correlationId;
        final long subscriptionId;
        final int streamId;
        final String strippedChannel;

        SubscriptionDescriptor(
            final long controlSessionId,
            final long correlationId,
            final long subscriptionId,
            final int streamId,
            final String strippedChannel)
        {
            this.controlSessionId = controlSessionId;
            this.correlationId = correlationId;
            this.subscriptionId = subscriptionId;
            this.streamId = streamId;
            this.strippedChannel = strippedChannel;
        }

        public String toString()
        {
            return "SubscriptionDescriptor{" +
                "controlSessionId=" + controlSessionId +
                ", correlationId=" + correlationId +
                ", subscriptionId=" + subscriptionId +
                ", streamId=" + streamId +
                ", strippedChannel='" + strippedChannel + '\'' +
                '}';
        }
    }
}
