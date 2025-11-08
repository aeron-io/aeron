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
package io.aeron.cluster.service;

import io.aeron.Aeron;
import io.aeron.cluster.codecs.mark.ClusterComponentType;
import io.aeron.cluster.codecs.mark.MarkFileHeaderDecoder;
import io.aeron.cluster.codecs.mark.MarkFileHeaderEncoder;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.aeron.cluster.service.ClusterMarkFile.ERROR_BUFFER_MAX_LENGTH;
import static io.aeron.cluster.service.ClusterMarkFile.ERROR_BUFFER_MIN_LENGTH;
import static io.aeron.cluster.service.ClusterMarkFile.HEADER_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.PAGE_MIN_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClusterMarkFileTest
{
    @TempDir
    private Path tempDir;

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -100, ERROR_BUFFER_MIN_LENGTH - 1, ERROR_BUFFER_MAX_LENGTH + 1 })
    void throwsExceptionIfErrorBufferLengthIsInvalid(final int errorBufferLength)
    {
        final IllegalArgumentException exception = assertThrowsExactly(
            IllegalArgumentException.class,
            () -> new ClusterMarkFile(
                tempDir.resolve("test.cfg").toFile(),
                ClusterComponentType.CONSENSUS_MODULE,
                errorBufferLength,
                SystemEpochClock.INSTANCE,
                10,
                PAGE_MIN_SIZE));
        assertEquals("Invalid errorBufferLength: " + errorBufferLength, exception.getMessage());
    }

    @Test
    @DisabledForJreRange(min = JRE.JAVA_21)
    void shouldCallForceIfMarkFileIsNotClosed()
    {
        final MarkFile markFile = mock(MarkFile.class);
        final MappedByteBuffer mappedByteBuffer = mock(MappedByteBuffer.class);
        when(markFile.mappedByteBuffer()).thenReturn(mappedByteBuffer);
        when(markFile.buffer()).thenReturn(new UnsafeBuffer(new byte[128]));
        try (ClusterMarkFile clusterMarkFile = new ClusterMarkFile(markFile))
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
        try (ClusterMarkFile clusterMarkFile = new ClusterMarkFile(markFile))
        {
            clusterMarkFile.force();

            final InOrder inOrder = inOrder(markFile, mappedByteBuffer);
            inOrder.verify(markFile).isClosed();
            inOrder.verifyNoMoreInteractions();
        }
    }

    @ParameterizedTest
    @EnumSource(ClusterComponentType.class)
    void shouldCreateNewMarkFile(final ClusterComponentType componentType)
    {
        final File file = tempDir.resolve(ClusterMarkFile.FILENAME).toFile();
        assertFalse(file.exists());

        final CachedEpochClock epochClock = new CachedEpochClock();
        epochClock.advance(35984758934759843L);

        try (ClusterMarkFile clusterMarkFile =
            new ClusterMarkFile(file, componentType, ERROR_BUFFER_MIN_LENGTH, epochClock, 1000, PAGE_MIN_SIZE))
        {
            assertTrue(file.exists());
            assertEquals(HEADER_LENGTH + ERROR_BUFFER_MIN_LENGTH, file.length());

            final long startTimestamp = epochClock.time();
            clusterMarkFile.signalReady(startTimestamp);

            verifyMarkFileContents(
                clusterMarkFile,
                ClusterMarkFile.SEMANTIC_VERSION,
                componentType,
                startTimestamp,
                startTimestamp,
                SystemUtil.getPid(),
                Aeron.NULL_VALUE,
                0,
                0,
                0,
                0,
                0,
                0,
                HEADER_LENGTH,
                ERROR_BUFFER_MIN_LENGTH,
                0,
                "",
                "",
                "",
                "",
                "",
                "");

            assertInstanceOf(MarkFileHeaderEncoder.class, clusterMarkFile.encoder());
            assertInstanceOf(MarkFileHeaderDecoder.class, clusterMarkFile.decoder());
        }
    }

    @Test
    void shouldUpdateExistingMarkFile()
    {
        final long activityTimestamp = 112211443311L;
        final long candidateTermId = 753475487L;
        final int archiveStreamId = 4;
        final int serviceStreamId = 5;
        final int consensusModuleStreamId = 108;
        final int insgresStreamId = 101;
        final int memberId = 8;
        final int serviceId = 1;
        final int clusterId = -9;
        final String aeronDir = tempDir.resolve("aeron").toString();
        final String controlChannel = "aeron:ipc";
        final String ingressChannel = "aeron:udp?alias=ingress";
        final String serviceName = "io.aeron.cluster.TestService";
        final String authenticator = "authenticator";
        final String clusterDir = "cluster dir";

        final File file = tempDir.resolve(ClusterMarkFile.FILENAME).toFile();
        assertFalse(file.exists());

        final CachedEpochClock epochClock = new CachedEpochClock();
        epochClock.update(123456L);

        try (ClusterMarkFile clusterMarkFile = new ClusterMarkFile(
            file, ClusterComponentType.BACKUP, ERROR_BUFFER_MIN_LENGTH, epochClock, 1000, PAGE_MIN_SIZE))
        {
            clusterMarkFile.signalReady(10);

            clusterMarkFile.encoder()
                .activityTimestamp(activityTimestamp)
                .startTimestamp(-900000)
                .pid(Long.MIN_VALUE)
                .candidateTermId(candidateTermId)
                .archiveStreamId(archiveStreamId)
                .serviceStreamId(serviceStreamId)
                .consensusModuleStreamId(consensusModuleStreamId)
                .ingressStreamId(insgresStreamId)
                .memberId(memberId)
                .serviceId(serviceId)
                .headerLength(444444)
                .errorBufferLength(555555)
                .clusterId(clusterId)
                .aeronDirectory(aeronDir)
                .controlChannel(controlChannel)
                .ingressChannel(ingressChannel)
                .serviceName(serviceName)
                .authenticator(authenticator)
                .servicesClusterDir(clusterDir);
        }

        epochClock.update(753498573948593L);
        try (ClusterMarkFile clusterMarkFile = new ClusterMarkFile(
            file, ClusterComponentType.CONSENSUS_MODULE, ERROR_BUFFER_MIN_LENGTH * 2, epochClock, 2222, PAGE_MIN_SIZE))
        {
            verifyMarkFileContents(
                clusterMarkFile,
                ClusterMarkFile.SEMANTIC_VERSION,
                ClusterComponentType.CONSENSUS_MODULE,
                MarkFile.ACTIVATION_IN_PROGRESS_TIMESTAMP,
                epochClock.time(),
                SystemUtil.getPid(),
                candidateTermId,
                archiveStreamId,
                serviceStreamId,
                consensusModuleStreamId,
                insgresStreamId,
                memberId,
                serviceId,
                HEADER_LENGTH,
                ERROR_BUFFER_MIN_LENGTH * 2,
                clusterId,
                aeronDir,
                controlChannel,
                ingressChannel,
                serviceName,
                authenticator,
                clusterDir);
        }
    }

    @ParameterizedTest
    @EnumSource(io.aeron.cluster.codecs.mark.v0.ClusterComponentType.class)
    @SuppressWarnings("MethodLength")
    void shouldHandleExistingMarkFileV0(final io.aeron.cluster.codecs.mark.v0.ClusterComponentType componentType)
        throws IOException
    {
        final int version = SemanticVersion.compose(ClusterMarkFile.MAJOR_VERSION, 98, 157);
        final ClusterComponentType currentComponentType = ClusterComponentType.get(componentType.value());
        final int activityTimestamp = 89898989;
        final int startTimestamp = -94237423;
        final long pid = 42;
        final long candidateTermId = -78;
        final int archiveStreamId = 33;
        final int serviceStreamId = 777;
        final int consensusModuleStreamId = -87;
        final int ingressStreamId = 5;
        final int memberId = 16;
        final int serviceId = 6;
        final int headerLength = 2048;
        final int errorBufferLength = 1500;
        final int clusterId = 3;
        final String aeronDir = tempDir.resolve("path/to/dev/shm").toString();
        final String controlChannel = "control";
        final String ingressChannel = "aeron:udp?endpoint=9999";
        final String serviceName = "service name";
        final String authenticator = "auth";
        final String clusterDir = tempDir.resolve("cluster").toString();

        final Path file =
            Files.write(tempDir.resolve("test.txt"), new byte[4096], StandardOpenOption.CREATE_NEW);

        final MarkFile markFile = new MarkFile(
            IoUtil.mapExistingFile(file.toFile(), ClusterMarkFile.FILENAME),
            io.aeron.cluster.codecs.mark.v0.MarkFileHeaderDecoder.versionEncodingOffset(),
            io.aeron.cluster.codecs.mark.v0.MarkFileHeaderDecoder.activityTimestampEncodingOffset());

        final io.aeron.cluster.codecs.mark.v0.MarkFileHeaderEncoder encoder =
            new io.aeron.cluster.codecs.mark.v0.MarkFileHeaderEncoder();
        encoder.wrap(markFile.buffer(), 0);

        encoder
            .version(version)
            .componentType(componentType)
            .activityTimestamp(activityTimestamp)
            .startTimestamp(startTimestamp)
            .pid(pid)
            .candidateTermId(candidateTermId)
            .archiveStreamId(archiveStreamId)
            .serviceStreamId(serviceStreamId)
            .consensusModuleStreamId(consensusModuleStreamId)
            .ingressStreamId(ingressStreamId)
            .memberId(memberId)
            .serviceId(serviceId)
            .headerLength(headerLength)
            .errorBufferLength(errorBufferLength)
            .clusterId(clusterId)
            .aeronDirectory(aeronDir)
            .controlChannel(controlChannel)
            .ingressChannel(ingressChannel)
            .serviceName(serviceName)
            .authenticator(authenticator);

        markFile.buffer().putStringAscii(encoder.encodedLength(), clusterDir);

        try (ClusterMarkFile clusterMarkFile = new ClusterMarkFile(markFile))
        {
            verifyMarkFileContents(
                clusterMarkFile,
                version,
                currentComponentType,
                activityTimestamp,
                startTimestamp,
                pid,
                candidateTermId,
                archiveStreamId,
                serviceStreamId,
                consensusModuleStreamId,
                ingressStreamId,
                memberId,
                serviceId,
                headerLength,
                errorBufferLength,
                clusterId,
                aeronDir,
                controlChannel,
                ingressChannel,
                serviceName,
                authenticator,
                clusterDir);

            clusterMarkFile.signalFailedStart();
            assertEquals(ClusterMarkFile.VERSION_FAILED, clusterMarkFile.decoder().version());

            clusterMarkFile.signalReady(activityTimestamp * 2);
            assertEquals(ClusterMarkFile.SEMANTIC_VERSION, clusterMarkFile.decoder().version());

            clusterMarkFile.memberId(42);
            assertEquals(42, clusterMarkFile.memberId());

            clusterMarkFile.clusterId(8888888);
            assertEquals(8888888, clusterMarkFile.clusterId());

            assertEquals(candidateTermId, clusterMarkFile.candidateTermId());
            assertEquals(componentType.value(), clusterMarkFile.decoder().componentType().value());

            clusterMarkFile.decoder().sbeRewind();
            assertEquals(aeronDir, clusterMarkFile.decoder().aeronDirectory());
        }

        final CachedEpochClock epochClock = new CachedEpochClock();
        epochClock.advance(5436547234L);

        try (ClusterMarkFile clusterMarkFile = new ClusterMarkFile(
            file.getParent().toFile(),
            file.getFileName().toString(),
            epochClock,
            50_000,
            null))
        {
            verifyMarkFileContents(
                clusterMarkFile,
                ClusterMarkFile.SEMANTIC_VERSION,
                currentComponentType,
                activityTimestamp * 2,
                startTimestamp,
                pid,
                candidateTermId,
                archiveStreamId,
                serviceStreamId,
                consensusModuleStreamId,
                ingressStreamId,
                42,
                serviceId,
                headerLength,
                errorBufferLength,
                8888888,
                aeronDir,
                controlChannel,
                ingressChannel,
                serviceName,
                authenticator,
                clusterDir);

            clusterMarkFile.signalReady(activityTimestamp * 3);
            assertEquals(ClusterMarkFile.SEMANTIC_VERSION, clusterMarkFile.decoder().version());
            assertEquals(activityTimestamp * 3, clusterMarkFile.decoder().activityTimestamp());
        }

        // should overwrite existing data when message header offset is being added
        try (ClusterMarkFile clusterMarkFile = new ClusterMarkFile(
            file.toFile(),
            currentComponentType,
            ERROR_BUFFER_MIN_LENGTH * 2,
            epochClock,
            1000,
            PAGE_MIN_SIZE))
        {
            clusterMarkFile.memberId(8);
            clusterMarkFile.clusterId(3);

            verifyMarkFileContents(
                clusterMarkFile,
                0,
                currentComponentType,
                0,
                epochClock.time(),
                SystemUtil.getPid(),
                candidateTermId,
                0,
                0,
                0,
                0,
                8,
                0,
                HEADER_LENGTH,
                ERROR_BUFFER_MIN_LENGTH * 2,
                3,
                "",
                "",
                "",
                "",
                "",
                "");
        }
    }

    @Test
    void shouldUnmapBufferUponClose()
    {
        final ClusterMarkFile clusterMarkFile = new ClusterMarkFile(
            tempDir.resolve("test-mark.file").toFile(),
            ClusterComponentType.STANDBY,
            ERROR_BUFFER_MIN_LENGTH,
            SystemEpochClock.INSTANCE,
            100,
            PAGE_MIN_SIZE);

        final MarkFileHeaderEncoder encoder = clusterMarkFile.encoder();
        final MarkFileHeaderDecoder decoder = clusterMarkFile.decoder();
        final AtomicBuffer errorBuffer = clusterMarkFile.errorBuffer();

        assertNotNull(errorBuffer.byteBuffer());
        assertSame(errorBuffer.byteBuffer(), encoder.buffer().byteBuffer());
        assertSame(errorBuffer.byteBuffer(), decoder.buffer().byteBuffer());

        clusterMarkFile.close();

        assertTrue(clusterMarkFile.isClosed());
        assertNull(errorBuffer.byteBuffer());
        assertEquals(0, errorBuffer.capacity());
        assertNull(encoder.buffer().byteBuffer());
        assertEquals(0, encoder.buffer().capacity());
        assertNull(decoder.buffer().byteBuffer());
        assertEquals(0, decoder.buffer().capacity());

        clusterMarkFile.close();

        assertTrue(clusterMarkFile.isClosed());
    }

    @Test
    void clusterIdAccessors()
    {
        final ClusterMarkFile clusterMarkFile = new ClusterMarkFile(
            tempDir.resolve("test-mark.file").toFile(),
            ClusterComponentType.STANDBY,
            ERROR_BUFFER_MIN_LENGTH,
            SystemEpochClock.INSTANCE,
            100,
            PAGE_MIN_SIZE);

        assertEquals(0, clusterMarkFile.clusterId());

        clusterMarkFile.clusterId(42);
        assertEquals(42, clusterMarkFile.clusterId());

        clusterMarkFile.encoder().clusterId(123);
        assertEquals(123, clusterMarkFile.clusterId());
        assertEquals(123, clusterMarkFile.decoder().clusterId());

        clusterMarkFile.close();

        clusterMarkFile.clusterId(2);
        assertEquals(Aeron.NULL_VALUE, clusterMarkFile.clusterId());
    }

    @Test
    void memberIdAccessors()
    {
        final ClusterMarkFile clusterMarkFile = new ClusterMarkFile(
            tempDir.resolve("test-mark.file").toFile(),
            ClusterComponentType.STANDBY,
            ERROR_BUFFER_MIN_LENGTH,
            SystemEpochClock.INSTANCE,
            100,
            PAGE_MIN_SIZE);

        assertEquals(0, clusterMarkFile.memberId());

        clusterMarkFile.memberId(7);
        assertEquals(7, clusterMarkFile.memberId());

        clusterMarkFile.encoder().memberId(-5);
        assertEquals(-5, clusterMarkFile.memberId());
        assertEquals(-5, clusterMarkFile.decoder().memberId());

        clusterMarkFile.close();

        clusterMarkFile.memberId(111);
        assertEquals(Aeron.NULL_VALUE, clusterMarkFile.memberId());
    }

    @Test
    void candidateTermIdAccessors()
    {
        final ClusterMarkFile clusterMarkFile = new ClusterMarkFile(
            tempDir.resolve("test-mark.file").toFile(),
            ClusterComponentType.STANDBY,
            ERROR_BUFFER_MIN_LENGTH,
            SystemEpochClock.INSTANCE,
            100,
            PAGE_MIN_SIZE);

        assertEquals(Aeron.NULL_VALUE, clusterMarkFile.candidateTermId());

        clusterMarkFile.encoder().candidateTermId(123);
        assertEquals(123, clusterMarkFile.candidateTermId());
        assertEquals(123, clusterMarkFile.decoder().candidateTermId());

        clusterMarkFile.close();

        assertEquals(Aeron.NULL_VALUE, clusterMarkFile.candidateTermId());
    }

    @Test
    void activityTimestampAccessors()
    {
        final ClusterMarkFile clusterMarkFile = new ClusterMarkFile(
            tempDir.resolve("test-mark.file").toFile(),
            ClusterComponentType.STANDBY,
            ERROR_BUFFER_MIN_LENGTH,
            SystemEpochClock.INSTANCE,
            100,
            PAGE_MIN_SIZE);

        assertEquals(0, clusterMarkFile.activityTimestampVolatile());

        clusterMarkFile.updateActivityTimestamp(4444555555L);
        assertEquals(4444555555L, clusterMarkFile.activityTimestampVolatile());

        clusterMarkFile.encoder().activityTimestamp(439856438756348L);
        assertEquals(439856438756348L, clusterMarkFile.activityTimestampVolatile());
        assertEquals(439856438756348L, clusterMarkFile.decoder().activityTimestamp());

        clusterMarkFile.close();

        clusterMarkFile.updateActivityTimestamp(222);
        assertEquals(Aeron.NULL_VALUE, clusterMarkFile.activityTimestampVolatile());
    }

    @Test
    void loadControlPropertiesAccessors()
    {
        final String controlChannel = "aeron:udp?endpoint=192.168.0.10:5555";
        final int memberId = 42;
        final int serviceStreamId = 61;
        final int consensusModuleStreamId = 4;
        final String aeronDir = "/dev/shm/aeron";
        final String ingressChannel = "aeron:udp?endpoint=localhost:8080";
        final String clusterDir = "/data/cluster/dir";

        final ClusterMarkFile clusterMarkFile = new ClusterMarkFile(
            tempDir.resolve("test-mark.file").toFile(),
            ClusterComponentType.STANDBY,
            ERROR_BUFFER_MIN_LENGTH,
            SystemEpochClock.INSTANCE,
            100,
            PAGE_MIN_SIZE);

        clusterMarkFile.encoder()
            .clusterId(123)
            .memberId(memberId)
            .serviceId(7)
            .candidateTermId(-5)
            .serviceStreamId(serviceStreamId)
            .consensusModuleStreamId(consensusModuleStreamId)
            .aeronDirectory(aeronDir)
            .controlChannel(controlChannel)
            .ingressChannel(ingressChannel)
            .serviceName("test")
            .authenticator("authenticator")
            .servicesClusterDir(clusterDir);

        clusterMarkFile.decoder().skipAeronDirectory();
        clusterMarkFile.decoder().skipControlChannel();
        assertEquals(ingressChannel, clusterMarkFile.decoder().ingressChannel());
        clusterMarkFile.decoder().skipServiceName();
        clusterMarkFile.decoder().skipAuthenticator();
        assertEquals(clusterDir, clusterMarkFile.decoder().servicesClusterDir());

        final ClusterNodeControlProperties controlProperties = clusterMarkFile.loadControlProperties();
        assertNotNull(controlProperties);
        assertEquals(memberId, controlProperties.memberId);
        assertEquals(serviceStreamId, controlProperties.serviceStreamId);
        assertEquals(consensusModuleStreamId, controlProperties.consensusModuleStreamId);
        assertEquals(aeronDir, controlProperties.aeronDirectoryName);
        assertEquals(controlChannel, controlProperties.controlChannel);

        clusterMarkFile.close();

        assertNull(clusterMarkFile.loadControlProperties());
    }

    @Test
    void signalReady()
    {
        final ClusterMarkFile clusterMarkFile = new ClusterMarkFile(
            tempDir.resolve("test-mark.file").toFile(),
            ClusterComponentType.STANDBY,
            ERROR_BUFFER_MIN_LENGTH,
            SystemEpochClock.INSTANCE,
            100,
            PAGE_MIN_SIZE);

        assertEquals(0, clusterMarkFile.decoder().version());

        clusterMarkFile.signalReady(1000);
        assertEquals(ClusterMarkFile.SEMANTIC_VERSION, clusterMarkFile.decoder().version());
        assertEquals(1000, clusterMarkFile.decoder().activityTimestamp());

        clusterMarkFile.close();

        clusterMarkFile.signalReady(2000);
    }

    @Test
    void signalFailedStart()
    {
        final ClusterMarkFile clusterMarkFile = new ClusterMarkFile(
            tempDir.resolve("test-mark.file").toFile(),
            ClusterComponentType.STANDBY,
            ERROR_BUFFER_MIN_LENGTH,
            SystemEpochClock.INSTANCE,
            100,
            PAGE_MIN_SIZE);

        assertEquals(0, clusterMarkFile.decoder().version());

        clusterMarkFile.signalFailedStart();
        assertEquals(ClusterMarkFile.VERSION_FAILED, clusterMarkFile.decoder().version());
        assertEquals(Aeron.NULL_VALUE, clusterMarkFile.decoder().activityTimestamp());

        clusterMarkFile.close();

        clusterMarkFile.signalFailedStart();
    }

    @ParameterizedTest
    @ValueSource(ints = { 4096, 32 * 1024 })
    @SuppressWarnings("try")
    void shouldAlignMarkFileLengthBasedOnTheFilePageSizeFromAeronClient(final int filePageSize)
    {
        final int errorBufferLength = 1345679;

        final File file = tempDir.resolve("test-mark.file").toFile();
        try (ClusterMarkFile ignored = new ClusterMarkFile(
            file,
            ClusterComponentType.STANDBY,
            errorBufferLength,
            SystemEpochClock.INSTANCE,
            100,
            filePageSize))
        {
            assertEquals(BitUtil.align(HEADER_LENGTH + errorBufferLength, filePageSize), file.length());
        }
    }

    @Test
    void shouldRejectFilePageSizeIfNotPowerOf2()
    {
        final IllegalStateException exception = assertThrowsExactly(
            IllegalStateException.class, () -> new ClusterMarkFile(
                tempDir.resolve("test-mark.file").toFile(),
                ClusterComponentType.STANDBY,
                ERROR_BUFFER_MIN_LENGTH,
                SystemEpochClock.INSTANCE,
                100,
                4199));

        assertEquals("Page size not a power of 2: page size=4199", exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, -5, 4095 })
    void shouldRejectFilePageSizeIfTooSmall(final int pageSize)
    {
        final IllegalStateException exception = assertThrowsExactly(
            IllegalStateException.class, () -> new ClusterMarkFile(
                tempDir.resolve("test-mark.file").toFile(),
                ClusterComponentType.STANDBY,
                ERROR_BUFFER_MIN_LENGTH,
                SystemEpochClock.INSTANCE,
                100,
                pageSize));

        assertEquals("Page size less than min size of 4096: page size=" + pageSize, exception.getMessage());
    }

    @Test
    void shouldRejectFilePageSizeIfTooBig()
    {
        final IllegalStateException exception = assertThrowsExactly(
            IllegalStateException.class, () -> new ClusterMarkFile(
                tempDir.resolve("test-mark.file").toFile(),
                ClusterComponentType.STANDBY,
                ERROR_BUFFER_MIN_LENGTH,
                SystemEpochClock.INSTANCE,
                100,
                Integer.MAX_VALUE));

        assertEquals("Page size more than max size of 1073741824: page size=2147483647", exception.getMessage());
    }

    private static void verifyMarkFileContents(
        final ClusterMarkFile clusterMarkFile,
        final int version,
        final ClusterComponentType clusterComponentType,
        final long activityTimestamp,
        final long startTimestamp,
        final long pid,
        final long candidateTermId,
        final int archiveStreamId,
        final int serviceStreamId,
        final int consensusModuleStreamId,
        final int ingressStreamId,
        final int memberId,
        final int serviceId,
        final int headerLength,
        final int errorBufferLength,
        final int clusterId,
        final String aeronDir,
        final String controlChannel,
        final String ingressChannel,
        final String serviceName,
        final String authenticator,
        final String clusterDir)
    {
        assertEquals(version, clusterMarkFile.decoder().version());
        assertEquals(clusterComponentType, clusterMarkFile.decoder().componentType());
        assertEquals(activityTimestamp, clusterMarkFile.decoder().activityTimestamp());
        assertEquals(startTimestamp, clusterMarkFile.decoder().startTimestamp());
        assertEquals(pid, clusterMarkFile.decoder().pid());
        assertEquals(candidateTermId, clusterMarkFile.decoder().candidateTermId());
        assertEquals(archiveStreamId, clusterMarkFile.decoder().archiveStreamId());
        assertEquals(serviceStreamId, clusterMarkFile.decoder().serviceStreamId());
        assertEquals(consensusModuleStreamId, clusterMarkFile.decoder().consensusModuleStreamId());
        assertEquals(ingressStreamId, clusterMarkFile.decoder().ingressStreamId());
        assertEquals(memberId, clusterMarkFile.decoder().memberId());
        assertEquals(serviceId, clusterMarkFile.decoder().serviceId());
        assertEquals(headerLength, clusterMarkFile.decoder().headerLength());
        assertEquals(errorBufferLength, clusterMarkFile.decoder().errorBufferLength());
        assertEquals(clusterId, clusterMarkFile.decoder().clusterId());
        assertEquals(aeronDir, clusterMarkFile.decoder().aeronDirectory());
        assertEquals(controlChannel, clusterMarkFile.decoder().controlChannel());
        assertEquals(ingressChannel, clusterMarkFile.decoder().ingressChannel());
        assertEquals(serviceName, clusterMarkFile.decoder().serviceName());
        assertEquals(authenticator, clusterMarkFile.decoder().authenticator());
        assertEquals(clusterDir, clusterMarkFile.decoder().servicesClusterDir());

        assertEquals(memberId, clusterMarkFile.memberId());
        assertEquals(clusterId, clusterMarkFile.clusterId());
        assertEquals(candidateTermId, clusterMarkFile.candidateTermId());
    }
}
