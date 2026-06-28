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
import io.aeron.AeronCounters;
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Counter;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.RethrowingErrorHandler;
import io.aeron.Subscription;
import io.aeron.archive.Archive.Context;
import io.aeron.archive.checksum.Checksum;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.AeronArchiveVersion;
import io.aeron.archive.client.ArchiveEvent;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.RecordingSubscriptionDescriptorConsumer;
import io.aeron.archive.codecs.TruncateRecordingRequestDecoder;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.status.PublisherPos;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.security.AuthorisationServiceSupplier;
import io.aeron.security.CredentialsSupplier;
import io.aeron.status.HeartbeatTimestamp;
import io.aeron.status.LocalSocketAddressStatus;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.CONTROL_MODE_RESPONSE;
import static io.aeron.CommonContext.IPC_MEDIA;
import static io.aeron.CommonContext.MDC_CONTROL_MODE_PARAM_NAME;
import static io.aeron.CommonContext.SESSION_ID_PARAM_NAME;
import static io.aeron.CommonContext.generateRandomDirName;
import static io.aeron.archive.ArchiveThreadingMode.DEDICATED;
import static io.aeron.archive.ArchiveThreadingMode.SHARED;
import static io.aeron.archive.Catalog.MIN_CAPACITY;
import static io.aeron.archive.client.AeronArchive.segmentFileBasePosition;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.status.HeartbeatTimestamp.HEARTBEAT_TYPE_ID;
import static io.aeron.test.TestContexts.LOCALHOST_CONTROL_REQUEST_CHANNEL;
import static io.aeron.test.TestContexts.LOCALHOST_CONTROL_RESPONSE_CHANNEL;
import static io.aeron.test.TestContexts.LOCALHOST_REPLICATION_CHANNEL;
import static org.agrona.BitUtil.align;
import static org.agrona.concurrent.status.CountersReader.KEY_OFFSET;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;
import static org.agrona.concurrent.status.CountersReader.RECORD_RECLAIMED;
import static org.agrona.concurrent.status.CountersReader.metaDataOffset;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(InterruptingTestCallback.class)
@InterruptAfter(10)
@SuppressWarnings("try")
class ArchiveTest
{
    private static final FragmentHandler NO_OP_FRAGMENT_HANDLER = (buffer, offset, length, header) -> {};

    @TempDir
    Path tmpDir;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

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
                .media(IPC_MEDIA)
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
            while (NULL_VALUE == (counterId = RecordingPos.findCounterIdBySession(
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

        try (TestMediaDriver driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
            Archive archive1 = Archive.launch(
                new Archive.Context()
                .controlChannel(LOCALHOST_CONTROL_REQUEST_CHANNEL)
                .replicationChannel(LOCALHOST_REPLICATION_CHANNEL)
                .deleteArchiveOnStart(true)
                .threadingMode(SHARED)
                .aeronDirectoryName(driver.aeronDirectoryName())
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
                .aeronDirectoryName(driver.aeronDirectoryName())
                .controlRequestChannel(archive1.context().controlChannel())
                .controlResponseChannel(LOCALHOST_CONTROL_RESPONSE_CHANNEL));
            AeronArchive client2 = AeronArchive.connect(new AeronArchive.Context()
                .aeronDirectoryName(driver.aeronDirectoryName())
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
                .media(IPC_MEDIA)
                .termLength(256 * 1024)
                .build();

            try (Publication publication = archive.addRecordedExclusivePublication(channel, 1))
            {
                int counterId;
                final int sessionId = publication.sessionId();
                final CountersReader countersReader = aeron.countersReader();

                while (NULL_VALUE == (counterId = RecordingPos.findCounterIdBySession(
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

            try (Subscription subscription = archive.replay(recordingId, 0, NULL_VALUE, channel, 2))
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
                            fail(byteStream.toString(StandardCharsets.UTF_8));
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
        try (TestMediaDriver driver = TestMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(aeronDir.toString()), systemTestWatcher);
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
                final ArchiveException exception =
                    assertThrowsExactly(ArchiveException.class, archiveContext2::conclude);
                assertEquals("ERROR - found existing archive for archiveId=" + archiveId, exception.getMessage());
            }
            finally
            {
                archiveContext2.close();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 888 })
    @InterruptAfter(10)
    void shouldAssignClientName(final int archiveId) throws IOException
    {
        final Path root = Files.createTempDirectory("test");
        final String aeronDir = root.resolve("media-driver").toString();
        try (TestMediaDriver driver =
            TestMediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(aeronDir), systemTestWatcher);
            Archive archive = Archive.launch(TestContexts.localhostArchive()
                .archiveId(archiveId)
                .archiveDir(root.resolve("archive1").toFile())
                .aeronDirectoryName(driver.aeronDirectoryName()));
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir)))
        {
            final long archiveClientId = archive.context().aeron().clientId();
            assertNotEquals(aeron.clientId(), archiveClientId);
            final CountersReader countersReader = aeron.countersReader();
            int counterId = NULL_COUNTER_ID;
            String counterLabel = null;
            while (true)
            {
                if (NULL_COUNTER_ID == counterId)
                {
                    counterId = HeartbeatTimestamp.findCounterIdByRegistrationId(
                        countersReader, HEARTBEAT_TYPE_ID, archiveClientId);
                }
                else if (null == counterLabel || !counterLabel.contains("name="))
                {
                    counterLabel = countersReader.getCounterLabel(counterId);
                }
                else
                {
                    assertThat(
                        counterLabel,
                        CoreMatchers.containsString("name=archive archiveId=" + archive.context().archiveId()));
                    break;
                }
                Tests.checkInterruptStatus();
            }
        }
        finally
        {
            IoUtil.delete(root.toFile(), true);
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

    @SuppressWarnings("MethodLength")
    @ParameterizedTest
    @CsvSource({
        "aeron:udp?endpoint=localhost:8010, aeron:udp?endpoint=localhost:0",
        "aeron:ipc, aeron:ipc",
        "aeron:udp?endpoint=localhost:8010, aeron:udp?control=localhost:9090|control-mode=response" })
    @InterruptAfter(15)
    void shouldTimeoutInactiveArchiveClients(final String controlRequestChannel, final String controlResponseChannel)
    {
        final long archiveId = -743746574;
        final ErrorHandler errorHandler = mock(ErrorHandler.class);
        try (TestMediaDriver driver = TestMediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(generateRandomDirName())
            .dirDeleteOnShutdown(true)
            .statusMessageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(80))
            .imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(1000))
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100)), systemTestWatcher);
            Archive archive = Archive.launch(TestContexts.localhostArchive()
                .controlChannel("aeron:udp?endpoint=localhost:8010")
                .localControlChannel("aeron:ipc?term-length=64k")
                .controlStreamId(888)
                .localControlStreamId(888)
                .archiveId(archiveId)
                .archiveDir(tmpDir.resolve("archive").toFile())
                .aeronDirectoryName(driver.context().aeronDirectoryName())
                .connectTimeoutNs(TimeUnit.MILLISECONDS.toNanos(678))
                .sessionLivenessCheckIntervalNs(TimeUnit.MILLISECONDS.toNanos(1))
                .errorHandler(errorHandler)))
        {
            final AeronArchive.Context ctx = new AeronArchive.Context()
                .aeronDirectoryName(driver.context().aeronDirectoryName())
                .controlRequestChannel(controlRequestChannel)
                .controlRequestStreamId(archive.context().controlStreamId())
                .controlResponseChannel(controlResponseChannel);
            try (AeronArchive client1 = AeronArchive.connect(ctx.clone().controlResponseStreamId(777));
                AeronArchive client2 = AeronArchive.connect(ctx.clone().controlResponseStreamId(999)))
            {
                assertEquals(archiveId, client1.archiveId());
                assertEquals(archiveId, client2.archiveId());
                assertNotEquals(client1.controlSessionId(), client2.controlSessionId());

                final long timeToFillResponseWindowNs =
                    (ctx.controlTermBufferLength() / 2 / 64) * archive.context().sessionLivenessCheckIntervalNs();

                final Counter sessionsCounter = archive.context().controlSessionsCounter();

                final long startNs = System.nanoTime();
                while (2 == sessionsCounter.get())
                {
                    assertNull(client1.pollForErrorResponse());
                    Tests.sleep(1);
                }
                final long endNs = System.nanoTime();

                assertThat(
                    endNs - startNs,
                    greaterThanOrEqualTo(timeToFillResponseWindowNs + archive.context().connectTimeoutNs()));

                Tests.await(() -> 1 == archive.context().errorCounter().get());
                final ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
                verify(errorHandler, timeout(1000)).onError(captor.capture());
                final ArchiveEvent event = assertInstanceOf(ArchiveEvent.class, captor.getValue());
                assertEquals(AeronException.Category.WARN, event.category());
                final ChannelUri parsedResponseChannel = ChannelUri.parse(client2.context().controlResponseChannel());
                assertThat(event.getMessage(), allOf(
                    Matchers.startsWith("WARN - controlSessionId=" + client2.controlSessionId() + " ("),
                    Matchers.endsWith(") terminated: failed to send response for more than connectTimeoutMs=" +
                    TimeUnit.NANOSECONDS.toMillis(archive.context().connectTimeoutNs())),
                    Matchers.containsString("controlResponseStreamId=999"),
                    Matchers.containsString("controlResponseChannel=aeron:" + parsedResponseChannel.media())));

                if (CONTROL_MODE_RESPONSE.equals(parsedResponseChannel.get(MDC_CONTROL_MODE_PARAM_NAME)))
                {
                    assertThat(event.getMessage(), allOf(
                        Matchers.containsString("control-mode=response"),
                        Matchers.containsString("response-correlation-id=")));
                }
                else
                {
                    assertThat(
                        event.getMessage(),
                        Matchers.containsString("session-id=" + parsedResponseChannel.get(SESSION_ID_PARAM_NAME)));
                }

                while (AeronArchive.State.CONNECTED == client2.state())
                {
                    assertNull(client1.pollForErrorResponse());
                    Tests.sleep(1);
                }

                // Closed via UnavailableImageHandler
                assertEquals(AeronArchive.State.DISCONNECTED, client2.state());
                assertFalse(client2.controlResponsePoller().subscription().isConnected());
                Tests.await(() -> !client2.archiveProxy().publication().isConnected());

                assertEquals(AeronArchive.State.CONNECTED, client1.state());
                assertTrue(client1.archiveProxy().publication().isConnected());
                assertTrue(client1.controlResponsePoller().subscription().isConnected());
            }
        }
    }

    @Test
    void archiveMarkFileShouldContainArchiveIdSetOnContextConclude()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .errorHandler(Tests::onError)
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);
        final Archive.Context archiveCtx = TestContexts.localhostArchive().threadingMode(SHARED);

        assertEquals(NULL_VALUE, archiveCtx.archiveId()); // <-- Should not have been set yet.
        try (ArchivingMediaDriver ignored = ArchivingMediaDriver.launch(driverCtx, archiveCtx);
            AeronArchive archiveClient = AeronArchive.connect(TestContexts.localhostAeronArchive()))
        {
            final long archiveId = archiveCtx.archiveId(); // <-- Should have been set now as the context is concluded.
            assertNotEquals(NULL_VALUE, archiveId);
            assertEquals(archiveId, archiveClient.archiveId());
            assertEquals(archiveId, archiveCtx.archiveMarkFile().archiveId());
        }
        finally
        {
            archiveCtx.deleteDirectory();
            driverCtx.deleteDirectory();
        }
    }

    @Test
    void closedArchiveClientShouldBeInStateClosed(@TempDir final Path temp)
    {
        try (TestMediaDriver driver = TestMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(generateRandomDirName()), systemTestWatcher);
            Archive archive = Archive.launch(TestContexts.localhostArchive()
                .archiveDir(temp.toFile())
                .archiveId(ThreadLocalRandom.current().nextLong())
                .aeronDirectoryName(driver.context().aeronDirectoryName()));
            AeronArchive aeronArchive = AeronArchive.connect(TestContexts.localhostAeronArchive()
                .aeronDirectoryName(driver.context().aeronDirectoryName())))
        {
            assertEquals(archive.context().archiveId(), aeronArchive.archiveId());

            final Publication publication = aeronArchive.archiveProxy().publication();
            final Subscription subscription = aeronArchive.controlResponsePoller().subscription();

            aeronArchive.close();

            Tests.await(publication::isClosed);
            Tests.await(subscription::isClosed);
            assertTrue(aeronArchive.context().aeron().isClosed());
            assertEquals(AeronArchive.State.CLOSED, aeronArchive.state());
        }
    }

    @Test
    void closedArchiveClientShouldBeInStateClosedCustomAeronClient(@TempDir final Path temp)
    {
        try (TestMediaDriver driver = TestMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(generateRandomDirName()), systemTestWatcher);
            Archive archive = Archive.launch(TestContexts.localhostArchive()
                .archiveDir(temp.toFile())
                .archiveId(ThreadLocalRandom.current().nextLong())
                .aeronDirectoryName(driver.context().aeronDirectoryName()));
            Aeron aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(driver.context().aeronDirectoryName())
                .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE));
            AeronArchive aeronArchive = AeronArchive.connect(TestContexts.localhostAeronArchive()
                .aeronDirectoryName(null)
                .aeron(aeron)))
        {
            assertEquals(archive.context().archiveId(), aeronArchive.archiveId());

            final Publication publication = aeronArchive.archiveProxy().publication();
            final Subscription subscription = aeronArchive.controlResponsePoller().subscription();

            aeronArchive.close();

            Tests.await(publication::isClosed);
            Tests.await(subscription::isClosed);
            assertFalse(aeron.isClosed());
            assertFalse(aeronArchive.context().aeron().isClosed());
            assertEquals(AeronArchive.State.CLOSED, aeronArchive.state());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "test client 5" })
    void shouldCreateControlSessionCounter(final String clientName, @TempDir final Path temp)
    {
        try (TestMediaDriver driver = TestMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(generateRandomDirName()), systemTestWatcher);
            Archive archive = Archive.launch(TestContexts.localhostArchive()
                .archiveDir(temp.toFile())
                .archiveId(519)
                .aeronDirectoryName(driver.context().aeronDirectoryName()));
            AeronArchive aeronArchive = AeronArchive.connect(TestContexts.localhostAeronArchive()
                .aeronDirectoryName(driver.context().aeronDirectoryName())
                .clientName(clientName)))
        {
            final CountersReader countersReader = archive.context().aeron().countersReader();
            assertEquals(archive.context().archiveId(), aeronArchive.archiveId());

            final int counterId = ControlSessionCounter.findByControlSessionId(
                countersReader, aeronArchive.archiveId(), aeronArchive.controlSessionId());
            assertNotEquals(NULL_COUNTER_ID, counterId);
            assertEquals(aeronArchive.controlSessionId(), countersReader.getCounterValue(counterId));

            final int keyOffset = metaDataOffset(counterId) + KEY_OFFSET;
            final AtomicBuffer metaDataBuffer = countersReader.metaDataBuffer();
            assertEquals(
                aeronArchive.archiveId(),
                metaDataBuffer.getLong(keyOffset + ControlSessionCounter.ARCHIVE_ID_KEY_OFFSET));
            assertEquals(
                aeronArchive.controlSessionId(),
                metaDataBuffer.getLong(keyOffset + ControlSessionCounter.CONTROL_SESSION_ID_KEY_OFFSET));

            final long responsePubRegistrationId = countersReader.getCounterReferenceId(counterId);
            assertNotEquals(NULL_VALUE, responsePubRegistrationId);
            final int responsePubCounterId = countersReader.findByTypeIdAndRegistrationId(
                PublisherPos.PUBLISHER_POS_TYPE_ID, responsePubRegistrationId);
            assertNotEquals(NULL_COUNTER_ID, responsePubCounterId);

            final Publication requestPublication = aeronArchive.archiveProxy().publication();
            final String localAddress = LocalSocketAddressStatus.findAddress(
                countersReader, requestPublication.channelStatus(), requestPublication.channelStatusId());
            assertEquals(ControlSessionCounter.NAME + ": name=" + clientName + " " +
                AeronCounters.formatVersionInfo(AeronArchiveVersion.VERSION, AeronArchiveVersion.GIT_SHA) +
                " sourceIdentity=" + localAddress +
                " sessionId=" + requestPublication.sessionId() +
                ArchiveCounters.ARCHIVE_ID_LABEL_SUFFIX + aeronArchive.archiveId(),
                countersReader.getCounterLabel(counterId));

            aeronArchive.close();

            Tests.await(() -> countersReader.getCounterState(counterId) == RECORD_RECLAIMED);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "test client 42" })
    void shouldCreateControlSessionCounterIpcConnection(final String clientName, @TempDir final Path temp)
    {
        try (TestMediaDriver driver = TestMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(generateRandomDirName()), systemTestWatcher);
            Archive archive = Archive.launch(TestContexts.localhostArchive()
                .archiveDir(temp.toFile())
                .archiveId(-187)
                .aeronDirectoryName(driver.context().aeronDirectoryName()));
            AeronArchive aeronArchive = AeronArchive.connect(TestContexts.ipcAeronArchive()
                .aeronDirectoryName(driver.context().aeronDirectoryName())
                .credentialsSupplier(new CredentialsSupplier()
                {
                    public byte[] encodedCredentials()
                    {
                        return "admin:test".getBytes(StandardCharsets.US_ASCII);
                    }

                    public byte[] onChallenge(final byte[] encodedChallenge)
                    {
                        return "challenged".getBytes(StandardCharsets.US_ASCII);
                    }
                })
                .clientName(clientName)))
        {
            final CountersReader countersReader = archive.context().aeron().countersReader();
            assertEquals(archive.context().archiveId(), aeronArchive.archiveId());

            final int counterId = ControlSessionCounter.findByControlSessionId(
                countersReader, aeronArchive.archiveId(), aeronArchive.controlSessionId());
            assertNotEquals(NULL_COUNTER_ID, counterId);
            assertEquals(aeronArchive.controlSessionId(), countersReader.getCounterValue(counterId));

            final int keyOffset = metaDataOffset(counterId) + KEY_OFFSET;
            final AtomicBuffer metaDataBuffer = countersReader.metaDataBuffer();
            assertEquals(
                aeronArchive.archiveId(),
                metaDataBuffer.getLong(keyOffset + ControlSessionCounter.ARCHIVE_ID_KEY_OFFSET));
            assertEquals(
                aeronArchive.controlSessionId(),
                metaDataBuffer.getLong(keyOffset + ControlSessionCounter.CONTROL_SESSION_ID_KEY_OFFSET));

            final long responsePubRegistrationId = countersReader.getCounterReferenceId(counterId);
            assertNotEquals(NULL_VALUE, responsePubRegistrationId);
            final int responsePubCounterId = countersReader.findByTypeIdAndRegistrationId(
                PublisherPos.PUBLISHER_POS_TYPE_ID, responsePubRegistrationId);
            assertNotEquals(NULL_COUNTER_ID, responsePubCounterId);

            final Publication requestPublication = aeronArchive.archiveProxy().publication();
            assertEquals(ControlSessionCounter.NAME + ": name=" + clientName + " " +
                AeronCounters.formatVersionInfo(AeronArchiveVersion.VERSION, AeronArchiveVersion.GIT_SHA) +
                " sourceIdentity=aeron:ipc" +
                " sessionId=" + requestPublication.sessionId() +
                ArchiveCounters.ARCHIVE_ID_LABEL_SUFFIX + aeronArchive.archiveId(),
                countersReader.getCounterLabel(counterId));

            aeronArchive.close();

            Tests.await(() -> countersReader.getCounterState(counterId) == RECORD_RECLAIMED);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:udp?endpoint=localhost:0",
        "aeron:udp?control=localhost:10000|control-mode=response" })
    void shouldConnectUsingInvokersWithAsyncConnect(final String responseChannel, @TempDir final Path temp)
    {
        final String requestChannel = "aeron:udp?endpoint=localhost:8888";
        try (TestMediaDriver driver = TestMediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(generateRandomDirName())
            .threadingMode(ThreadingMode.INVOKER), systemTestWatcher);
            Archive archive = Archive.launch(new Archive.Context()
                .threadingMode(ArchiveThreadingMode.INVOKER)
                .mediaDriverAgentInvoker(driver.sharedAgentInvoker())
                .archiveDir(temp.toFile())
                .archiveId(42)
                .controlChannel(requestChannel)
                .controlStreamId(1919)
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .aeronDirectoryName(driver.context().aeronDirectoryName()));
            Aeron aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(driver.aeronDirectoryName())
                .useConductorAgentInvoker(true)
                .driverAgentInvoker(driver.sharedAgentInvoker())
                .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE)))
        {
            final AeronArchive.AsyncConnect asyncConnect = AeronArchive.asyncConnect(
                new AeronArchive.Context()
                    .controlRequestChannel(requestChannel)
                    .controlRequestStreamId(archive.context().controlStreamId())
                    .controlResponseChannel(responseChannel)
                    .controlResponseStreamId(9191)
                    .aeron(aeron)
                    .agentInvoker(archive.invoker()));

            AeronArchive aeronArchive;
            while (null == (aeronArchive = asyncConnect.poll()))
            {
                Tests.yield();
            }

            assertEquals(archive.context().archiveId(), aeronArchive.archiveId());
        }
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
            intConsumer);
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
