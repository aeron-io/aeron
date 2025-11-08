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
import io.aeron.CommonContext;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.mark.MarkFileHeaderDecoder;
import io.aeron.archive.codecs.mark.MarkFileHeaderEncoder;
import io.aeron.archive.codecs.mark.MessageHeaderDecoder;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.TestContexts;
import org.agrona.BitUtil;
import org.agrona.IoUtil;
import org.agrona.MarkFile;
import org.agrona.SemanticVersion;
import org.agrona.SystemUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.logbuffer.LogBufferDescriptor.PAGE_MIN_SIZE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ArchiveMarkFileTest
{
    private static final int ERROR_BUFFER_LENGTH = 4096;

    @Test
    void shouldUseMarkFileDirectory(final @TempDir File tempDir)
    {
        final File archiveDir = new File(tempDir, "archive_dir");
        final File markFileDir = new File(tempDir, "mark-file-home");
        final File markFile = new File(markFileDir, ArchiveMarkFile.FILENAME);
        assertTrue(markFileDir.mkdirs());
        assertFalse(markFile.exists());

        final Aeron aeron = mock(Aeron.class);
        final Aeron.Context aeronCtx = mock(Aeron.Context.class);
        when(aeronCtx.filePageSize()).thenReturn(PAGE_MIN_SIZE);
        when(aeron.context()).thenReturn(aeronCtx);

        final Archive.Context context = TestContexts.localhostArchive()
            .archiveDir(archiveDir)
            .markFileDir(markFileDir)
            .aeron(aeron);
        shouldEncodeMarkFileFromArchiveContext(context);

        assertTrue(markFile.exists());
        assertFalse(archiveDir.exists());
    }

    @Test
    @SuppressWarnings("try")
    void shouldCreateLinkFileToMarkFileDirAndRemoveLinkFileIfArchiveDirMatchesMarkFileDir(final @TempDir File tempDir)
    {
        final File aeronDir = new File(tempDir, "aeron");
        final File archiveDir = new File(tempDir, "archive_dir");
        final File markFileDir = new File(tempDir, "mark-file-home");

        final MediaDriver.Context driverContext = new MediaDriver.Context()
            .aeronDirectoryName(aeronDir.getAbsolutePath())
            .threadingMode(ThreadingMode.SHARED);
        final Archive.Context archiveContext = TestContexts.localhostArchive()
            .aeronDirectoryName(driverContext.aeronDirectoryName())
            .archiveDir(archiveDir)
            .markFileDir(markFileDir)
            .epochClock(SystemEpochClock.INSTANCE)
            .threadingMode(ArchiveThreadingMode.SHARED);

        try (MediaDriver ignored = MediaDriver.launch(driverContext.clone());
            Archive ignored1 = Archive.launch(archiveContext.clone()))
        {
            assertTrue(new File(markFileDir, ArchiveMarkFile.FILENAME).exists());
            assertTrue(new File(archiveDir, ArchiveMarkFile.LINK_FILENAME).exists());
            assertFalse(new File(archiveDir, ArchiveMarkFile.FILENAME).exists());
        }

        archiveContext.markFileDir(null);
        try (MediaDriver ignored = MediaDriver.launch(driverContext.clone());
            Archive ignored1 = Archive.launch(archiveContext.clone()))
        {
            assertTrue(new File(archiveDir, ArchiveMarkFile.FILENAME).exists());
            assertFalse(new File(archiveDir, ArchiveMarkFile.LINK_FILENAME).exists());
        }
    }

    @Test
    @SuppressWarnings("try")
    void anErrorOnStartupShouldNotLeaveAnUninitialisedMarkFile(final @TempDir File tempDir)
    {
        final File aeronDir = new File(tempDir, "aeron");
        final File archiveDir = new File(tempDir, "archive_dir");
        final File markFileDir = new File(tempDir, "mark/file/dir");
        final File archiveMarkFile = new File(markFileDir, ArchiveMarkFile.FILENAME);

        final Aeron aeron = mock(Aeron.class);
        final Aeron.Context aeronCtx = mock(Aeron.Context.class);
        when(aeronCtx.aeronDirectoryName()).thenReturn("/dev/shm/aeron");
        when(aeronCtx.filePageSize()).thenReturn(PAGE_MIN_SIZE);
        when(aeronCtx.useConductorAgentInvoker()).thenReturn(false);
        when(aeron.context()).thenReturn(aeronCtx);

        final MediaDriver.Context driverContext = new MediaDriver.Context()
            .aeronDirectoryName(aeronDir.getAbsolutePath())
            .threadingMode(ThreadingMode.SHARED);
        final Archive.Context archiveContext = TestContexts.localhostArchive()
            .aeronDirectoryName(driverContext.aeronDirectoryName())
            .archiveDir(archiveDir)
            .markFileDir(markFileDir)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .epochClock(SystemEpochClock.INSTANCE)
            .aeron(aeron);

        // Force an error on startup by attempting to use invalid Aeron client
        try (Archive ignored = Archive.launch(archiveContext.clone()))
        {
            fail("Expected archive to timeout as no media driver is running.");
        }
        catch (final ArchiveException ex)
        {
            // Should be able to read the mark file and the activity timestamp should not have been set.
            try (ArchiveMarkFile testMarkFile = new ArchiveMarkFile(
                markFileDir,
                ArchiveMarkFile.FILENAME,
                SystemEpochClock.INSTANCE,
                10_000,
                null))
            {
                assertEquals(ArchiveMarkFile.SEMANTIC_VERSION, testMarkFile.decoder().version());
                assertEquals(NULL_VALUE, testMarkFile.decoder().activityTimestamp());
            }
        }

        // Should be able to successfully start the archive
        try (MediaDriver ignored1 = MediaDriver.launch(driverContext.clone());
            Archive archive = Archive.launch(archiveContext.clone().aeron(null)))
        {
            assertTrue(archiveMarkFile.exists());
            final long activityTimestamp = archive.context().archiveMarkFile().activityTimestampVolatile();
            assertThat(activityTimestamp, greaterThan(0L));
        }
    }

    @Test
    @DisabledForJreRange(min = JRE.JAVA_21)
    void shouldCallForceIfMarkFileIsNotClosed()
    {
        final MarkFile markFile = mock(MarkFile.class);
        final MappedByteBuffer mappedByteBuffer = mock(MappedByteBuffer.class);
        when(markFile.mappedByteBuffer()).thenReturn(mappedByteBuffer);
        when(markFile.buffer()).thenReturn(new UnsafeBuffer(new byte[128]));
        try (ArchiveMarkFile clusterMarkFile = new ArchiveMarkFile(markFile))
        {
            clusterMarkFile.force();

            final InOrder inOrder = inOrder(markFile, mappedByteBuffer);
            inOrder.verify(markFile).isClosed();
            inOrder.verify(markFile).mappedByteBuffer();
            inOrder.verify(mappedByteBuffer).force();
            inOrder.verifyNoMoreInteractions();
        }
    }

    @Test
    @DisabledForJreRange(min = JRE.JAVA_21)
    void shouldNotCallForceIfMarkFileIsClosed()
    {
        final MarkFile markFile = mock(MarkFile.class);
        final MappedByteBuffer mappedByteBuffer = mock(MappedByteBuffer.class);
        when(markFile.mappedByteBuffer()).thenReturn(mappedByteBuffer);
        when(markFile.buffer()).thenReturn(new UnsafeBuffer(new byte[128]));
        when(markFile.isClosed()).thenReturn(true);
        try (ArchiveMarkFile clusterMarkFile = new ArchiveMarkFile(markFile))
        {
            clusterMarkFile.force();

            final InOrder inOrder = inOrder(markFile, mappedByteBuffer);
            inOrder.verify(markFile).isClosed();
            inOrder.verifyNoMoreInteractions();
        }
    }

    @Test
    void shouldBeAbleToReadOldMarkFileV0(final @TempDir File tempDir) throws IOException
    {
        final int version = SemanticVersion.compose(ArchiveMarkFile.MAJOR_VERSION, 28, 16);
        final long activityTimestamp = 234783974944L;
        final int pid = -55555;
        final int controlStreamId = 1010;
        final int localControlStreamId = -9;
        final int eventsStreamId = 111;
        final String controlChannel = "aeron:udp?endpoint=localhost:8010";
        final String localControlChannel = "aeron:ipc?alias=local-control|term-length=64k";
        final String eventsChannels = "aeron:ipc?alias=events";
        final String aeronDirectory = tempDir.toPath().resolve("aeron/dir/path").toAbsolutePath().toString();

        final Path file = Files.write(
            tempDir.toPath().resolve(ArchiveMarkFile.FILENAME), new byte[1024], StandardOpenOption.CREATE_NEW);

        final MarkFile markFile = new MarkFile(
            IoUtil.mapExistingFile(file.toFile(), ArchiveMarkFile.FILENAME),
            io.aeron.archive.codecs.mark.v0.MarkFileHeaderDecoder.versionEncodingOffset(),
            io.aeron.archive.codecs.mark.v0.MarkFileHeaderDecoder.activityTimestampEncodingOffset());

        final io.aeron.archive.codecs.mark.v0.MarkFileHeaderEncoder encoder =
            new io.aeron.archive.codecs.mark.v0.MarkFileHeaderEncoder();
        encoder.wrap(markFile.buffer(), 0);

        encoder
            .version(version)
            .activityTimestamp(activityTimestamp)
            .pid(pid)
            .controlStreamId(controlStreamId)
            .localControlStreamId(localControlStreamId)
            .eventsStreamId(eventsStreamId)
            .controlChannel(controlChannel)
            .localControlChannel(localControlChannel)
            .eventsChannel(eventsChannels)
            .aeronDirectory(aeronDirectory);

        try (ArchiveMarkFile archiveMarkFile = new ArchiveMarkFile(markFile))
        {
            assertEquals(version, archiveMarkFile.decoder().version());
            assertEquals(activityTimestamp, archiveMarkFile.decoder().activityTimestamp());
            assertEquals(pid, archiveMarkFile.decoder().pid());
            assertEquals(controlStreamId, archiveMarkFile.decoder().controlStreamId());
            assertEquals(localControlStreamId, archiveMarkFile.decoder().localControlStreamId());
            assertEquals(eventsStreamId, archiveMarkFile.decoder().eventsStreamId());
            assertEquals(controlChannel, archiveMarkFile.decoder().controlChannel());
            assertEquals(localControlChannel, archiveMarkFile.decoder().localControlChannel());
            assertEquals(eventsChannels, archiveMarkFile.decoder().eventsChannel());
            assertEquals(aeronDirectory, archiveMarkFile.decoder().aeronDirectory());

            assertEquals(aeronDirectory, archiveMarkFile.aeronDirectory());
            assertEquals(NULL_VALUE, archiveMarkFile.archiveId());
        }

        final Aeron aeron = mock(Aeron.class);
        final Aeron.Context aeronCtx = mock(Aeron.Context.class);
        when(aeronCtx.filePageSize()).thenReturn(PAGE_MIN_SIZE);
        when(aeron.context()).thenReturn(aeronCtx);

        final Archive.Context context = new Archive.Context()
            .archiveDir(new File(tempDir, "archive"))
            .markFileDir(file.getParent().toFile())
            .aeron(aeron);
        shouldEncodeMarkFileFromArchiveContext(context);
    }

    @Test
    void shouldBeAbleToReadOldMarkFileV1(final @TempDir File tempDir) throws IOException
    {
        final int version = SemanticVersion.compose(ArchiveMarkFile.MAJOR_VERSION, 4, 4);
        final long activityTimestamp = 234783974944L;
        final int pid = 1;
        final int controlStreamId = 42;
        final int localControlStreamId = 19;
        final int eventsStreamId = -87;
        final int errorBufferLength = 8192;
        final String controlChannel = "aeron:udp?endpoint=localhost:8010";
        final String localControlChannel = "aeron:ipc?alias=local-control|term-length=64k";
        final String eventsChannels = "aeron:ipc?alias=events";
        final String aeronDirectory = tempDir.toPath().resolve("aeron/dir/path").toAbsolutePath().toString();

        final Path file = Files.write(
            tempDir.toPath().resolve(ArchiveMarkFile.FILENAME), new byte[32 * 1024], StandardOpenOption.CREATE_NEW);

        final MarkFile markFile = new MarkFile(
            IoUtil.mapExistingFile(file.toFile(), ArchiveMarkFile.FILENAME),
            io.aeron.archive.codecs.mark.v1.MarkFileHeaderDecoder.versionEncodingOffset(),
            io.aeron.archive.codecs.mark.v1.MarkFileHeaderDecoder.activityTimestampEncodingOffset());

        final io.aeron.archive.codecs.mark.v1.MarkFileHeaderEncoder encoder =
            new io.aeron.archive.codecs.mark.v1.MarkFileHeaderEncoder();
        encoder.wrap(markFile.buffer(), 0);

        encoder
            .version(version)
            .activityTimestamp(activityTimestamp)
            .pid(pid)
            .controlStreamId(controlStreamId)
            .localControlStreamId(localControlStreamId)
            .eventsStreamId(eventsStreamId)
            .headerLength(ArchiveMarkFile.HEADER_LENGTH)
            .errorBufferLength(errorBufferLength)
            .controlChannel(controlChannel)
            .localControlChannel(localControlChannel)
            .eventsChannel(eventsChannels)
            .aeronDirectory(aeronDirectory);

        try (ArchiveMarkFile archiveMarkFile = new ArchiveMarkFile(markFile))
        {
            assertEquals(version, archiveMarkFile.decoder().version());
            assertEquals(activityTimestamp, archiveMarkFile.decoder().activityTimestamp());
            assertEquals(pid, archiveMarkFile.decoder().pid());
            assertEquals(controlStreamId, archiveMarkFile.decoder().controlStreamId());
            assertEquals(localControlStreamId, archiveMarkFile.decoder().localControlStreamId());
            assertEquals(eventsStreamId, archiveMarkFile.decoder().eventsStreamId());
            assertEquals(ArchiveMarkFile.HEADER_LENGTH, archiveMarkFile.decoder().headerLength());
            assertEquals(errorBufferLength, archiveMarkFile.decoder().errorBufferLength());
            assertEquals(controlChannel, archiveMarkFile.decoder().controlChannel());
            assertEquals(localControlChannel, archiveMarkFile.decoder().localControlChannel());
            assertEquals(eventsChannels, archiveMarkFile.decoder().eventsChannel());
            assertEquals(aeronDirectory, archiveMarkFile.decoder().aeronDirectory());

            assertEquals(aeronDirectory, archiveMarkFile.aeronDirectory());
            assertEquals(NULL_VALUE, archiveMarkFile.archiveId());
        }

        final Aeron aeron = mock(Aeron.class);
        final Aeron.Context aeronCtx = mock(Aeron.Context.class);
        when(aeronCtx.filePageSize()).thenReturn(PAGE_MIN_SIZE);
        when(aeron.context()).thenReturn(aeronCtx);

        final Archive.Context context = new Archive.Context()
            .archiveDir(new File(tempDir, "archive/a/long/path/to"))
            .markFileDir(file.getParent().toFile())
            .aeron(aeron)
            .errorBufferLength(errorBufferLength);
        shouldEncodeMarkFileFromArchiveContext(context);
    }

    @Test
    void shouldUnmapMarkFileBufferUponClose(final @TempDir Path tempDir)
    {
        final ArchiveMarkFile archiveMarkFile = new ArchiveMarkFile(
            tempDir.resolve("archive.mark").toFile(),
            ArchiveMarkFile.HEADER_LENGTH + ERROR_BUFFER_LENGTH,
            ERROR_BUFFER_LENGTH,
            SystemEpochClock.INSTANCE,
            100);
        assertFalse(archiveMarkFile.isClosed());

        final MarkFileHeaderEncoder encoder = archiveMarkFile.encoder();
        final MarkFileHeaderDecoder decoder = archiveMarkFile.decoder();
        final AtomicBuffer errorBuffer = archiveMarkFile.errorBuffer();

        assertSame(errorBuffer.byteBuffer(), encoder.buffer().byteBuffer());
        assertSame(errorBuffer.byteBuffer(), decoder.buffer().byteBuffer());

        archiveMarkFile.close();

        assertTrue(archiveMarkFile.isClosed());
        assertNull(errorBuffer.byteBuffer());
        assertEquals(0, errorBuffer.capacity());
        assertNull(encoder.buffer().byteBuffer());
        assertEquals(0, encoder.buffer().capacity());
        assertNull(decoder.buffer().byteBuffer());
        assertEquals(0, decoder.buffer().capacity());

        archiveMarkFile.close();
        assertTrue(archiveMarkFile.isClosed());
    }

    @Test
    void activityTimestamp(final @TempDir Path tempDir)
    {
        final ArchiveMarkFile archiveMarkFile = new ArchiveMarkFile(
            tempDir.resolve("archive.mark").toFile(),
            ArchiveMarkFile.HEADER_LENGTH + ERROR_BUFFER_LENGTH,
            ERROR_BUFFER_LENGTH,
            SystemEpochClock.INSTANCE,
            100);

        assertEquals(0, archiveMarkFile.activityTimestampVolatile());
        assertEquals(0, archiveMarkFile.decoder().activityTimestamp());

        final long activityTimestamp = 7383439454305L;
        archiveMarkFile.updateActivityTimestamp(activityTimestamp);
        assertEquals(activityTimestamp, archiveMarkFile.activityTimestampVolatile());
        assertEquals(activityTimestamp, archiveMarkFile.decoder().activityTimestamp());

        archiveMarkFile.close();

        archiveMarkFile.updateActivityTimestamp(1111);
        assertEquals(NULL_VALUE, archiveMarkFile.activityTimestampVolatile());
    }

    @Test
    void archiveId(final @TempDir Path tempDir)
    {
        final ArchiveMarkFile archiveMarkFile = new ArchiveMarkFile(
            tempDir.resolve("archive.mark").toFile(),
            ArchiveMarkFile.HEADER_LENGTH + ERROR_BUFFER_LENGTH,
            ERROR_BUFFER_LENGTH,
            SystemEpochClock.INSTANCE,
            100);

        assertEquals(0, archiveMarkFile.archiveId());
        assertEquals(0, archiveMarkFile.decoder().archiveId());

        final long archiveId = -462348234343L;
        archiveMarkFile.encoder().archiveId(archiveId);
        assertEquals(archiveId, archiveMarkFile.archiveId());
        assertEquals(archiveId, archiveMarkFile.decoder().archiveId());

        archiveMarkFile.close();

        assertEquals(NULL_VALUE, archiveMarkFile.archiveId());
    }

    @Test
    void signalReady(final @TempDir Path tempDir)
    {
        final ArchiveMarkFile archiveMarkFile = new ArchiveMarkFile(
            tempDir.resolve("archive.mark").toFile(),
            ArchiveMarkFile.HEADER_LENGTH + ERROR_BUFFER_LENGTH,
            ERROR_BUFFER_LENGTH,
            SystemEpochClock.INSTANCE,
            100);

        assertEquals(0, archiveMarkFile.decoder().version());

        archiveMarkFile.signalReady(5555);

        assertEquals(ArchiveMarkFile.SEMANTIC_VERSION, archiveMarkFile.decoder().version());
        assertEquals(5555, archiveMarkFile.decoder().activityTimestamp());

        archiveMarkFile.close();

        archiveMarkFile.signalReady(7777);
        assertTrue(archiveMarkFile.isClosed());
    }

    @Test
    void shouldAlignMarkFileLengthToFilePageSizeFromAeronClient(final @TempDir Path tempDir) throws IOException
    {
        final Path aeronDir = tempDir.resolve("aeron");
        final Path archiveDir = tempDir.resolve("archive");
        final Path markFileDir = tempDir.resolve("mark");
        Files.createDirectories(aeronDir);
        Files.createDirectories(archiveDir);
        Files.createDirectories(markFileDir);

        final Archive.Context context = new Archive.Context();
        final Aeron aeron = mock(Aeron.class);
        final Aeron.Context aeronContext = mock(Aeron.Context.class);
        final int filePageSize = 16 * 1024;
        when(aeronContext.filePageSize()).thenReturn(filePageSize);
        when(aeronContext.aeronDirectory()).thenReturn(aeronDir.toFile());
        when(aeronContext.aeronDirectoryName()).thenReturn(aeronDir.toString());
        when(aeron.context()).thenReturn(aeronContext);
        context
            .aeron(aeron)
            .archiveDir(archiveDir.toFile())
            .markFileDir(markFileDir.toFile())
            .epochClock(SystemEpochClock.INSTANCE);

        try (ArchiveMarkFile archiveMarkFile = new ArchiveMarkFile(context))
        {
            assertEquals(
                BitUtil.align(ArchiveMarkFile.HEADER_LENGTH + context.errorBufferLength(), filePageSize),
                archiveMarkFile.buffer().capacity());
        }

        verify(aeronContext).filePageSize();
    }

    @Test
    void shouldAlignMarkFileLengthTo4KIfAeronClientReturnsZero(final @TempDir Path tempDir) throws IOException
    {
        final Path aeronDir = tempDir.resolve("aeron");
        final Path archiveDir = tempDir.resolve("archive");
        final Path markFileDir = tempDir.resolve("mark");
        Files.createDirectories(aeronDir);
        Files.createDirectories(archiveDir);
        Files.createDirectories(markFileDir);

        final Archive.Context context = new Archive.Context();
        final Aeron aeron = mock(Aeron.class);
        final Aeron.Context aeronContext = mock(Aeron.Context.class);
        when(aeronContext.filePageSize()).thenReturn(PAGE_MIN_SIZE);
        when(aeronContext.aeronDirectory()).thenReturn(aeronDir.toFile());
        when(aeronContext.aeronDirectoryName()).thenReturn(aeronDir.toString());
        when(aeron.context()).thenReturn(aeronContext);
        context
            .aeron(aeron)
            .archiveDir(archiveDir.toFile())
            .markFileDir(markFileDir.toFile())
            .epochClock(SystemEpochClock.INSTANCE)
            .errorBufferLength(1111111);

        try (ArchiveMarkFile archiveMarkFile = new ArchiveMarkFile(context))
        {
            assertEquals(
                BitUtil.align(ArchiveMarkFile.HEADER_LENGTH + context.errorBufferLength(), 4096),
                archiveMarkFile.buffer().capacity());
        }

        verify(aeronContext).filePageSize();
    }


    @Test
    @SuppressWarnings("try")
    void shouldAlignMarkFileLengthToFilePageSizeFromMediaDriver(final @TempDir Path tempDir) throws IOException
    {
        final Path archiveDir = tempDir.resolve("archive");
        Files.createDirectories(archiveDir);

        final int filePageSize = 2 * 1024 * 1024;
        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .dirDeleteOnShutdown(true)
            .threadingMode(ThreadingMode.SHARED)
            .filePageSize(filePageSize);

        final Archive.Context context = new Archive.Context();
        context
            .aeronDirectoryName(mediaDriverContext.aeronDirectoryName())
            .markFileDir(new File(mediaDriverContext.aeronDirectoryName()))
            .archiveDir(archiveDir.toFile())
            .epochClock(SystemEpochClock.INSTANCE)
            .errorBufferLength(1234567);

        try (MediaDriver ignored = MediaDriver.launch(mediaDriverContext);
            ArchiveMarkFile archiveMarkFile = new ArchiveMarkFile(context))
        {
            assertEquals(filePageSize, archiveMarkFile.buffer().capacity());
        }
    }

    private static void shouldEncodeMarkFileFromArchiveContext(final Archive.Context ctx)
    {
        final CachedEpochClock epochClock = new CachedEpochClock();
        epochClock.advance(12345678909876L);
        ctx
            .controlStreamId(42)
            .localControlStreamId(-118)
            .recordingEventsStreamId(85858585)
            .archiveId(46238467823468L)
            .controlChannel("aeron:udp?endpoint=localhost:55555|alias=control")
            .localControlChannel(AeronArchive.Configuration.localControlChannel())
            .recordingEventsChannel("aeron:udp?endpoint=localhost:0")
            .epochClock(epochClock);

        try (ArchiveMarkFile archiveMarkFile = new ArchiveMarkFile(ctx))
        {
            final long time = epochClock.time();
            archiveMarkFile.signalReady(time);

            final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
            messageHeaderDecoder.wrap(archiveMarkFile.buffer(), 0);
            assertEquals(MarkFileHeaderDecoder.BLOCK_LENGTH, messageHeaderDecoder.blockLength());
            assertEquals(MarkFileHeaderDecoder.SCHEMA_ID, messageHeaderDecoder.schemaId());
            assertEquals(MarkFileHeaderDecoder.TEMPLATE_ID, messageHeaderDecoder.templateId());
            assertEquals(MarkFileHeaderDecoder.SCHEMA_VERSION, messageHeaderDecoder.version());

            assertEquals(ArchiveMarkFile.SEMANTIC_VERSION, archiveMarkFile.decoder().version());
            assertEquals(time, archiveMarkFile.decoder().startTimestamp());
            assertEquals(SystemUtil.getPid(), archiveMarkFile.decoder().pid());
            assertEquals(ctx.controlStreamId(), archiveMarkFile.decoder().controlStreamId());
            assertEquals(ctx.localControlStreamId(), archiveMarkFile.decoder().localControlStreamId());
            assertEquals(ctx.recordingEventsStreamId(), archiveMarkFile.decoder().eventsStreamId());
            assertEquals(ArchiveMarkFile.HEADER_LENGTH, archiveMarkFile.decoder().headerLength());
            assertEquals(ctx.errorBufferLength(), archiveMarkFile.decoder().errorBufferLength());
            assertEquals(ctx.archiveId(), archiveMarkFile.decoder().archiveId());
            assertEquals(ctx.controlChannel(), archiveMarkFile.decoder().controlChannel());
            assertEquals(ctx.localControlChannel(), archiveMarkFile.decoder().localControlChannel());
            assertEquals(ctx.recordingEventsChannel(), archiveMarkFile.decoder().eventsChannel());
            assertEquals(ctx.aeronDirectoryName(), archiveMarkFile.decoder().aeronDirectory());

            assertInstanceOf(MarkFileHeaderDecoder.class, archiveMarkFile.decoder());
            assertInstanceOf(MarkFileHeaderEncoder.class, archiveMarkFile.encoder());
            assertEquals(ctx.archiveId(), archiveMarkFile.archiveId());

            assertEquals(ctx.aeronDirectoryName(), archiveMarkFile.aeronDirectory());
        }
    }
}
