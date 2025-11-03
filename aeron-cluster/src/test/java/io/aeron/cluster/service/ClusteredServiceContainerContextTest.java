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
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.Counter;
import io.aeron.RethrowingErrorHandler;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.mark.ClusterComponentType;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.aeron.cluster.service.ClusterMarkFile.ERROR_BUFFER_MIN_LENGTH;
import static io.aeron.cluster.service.ClusterMarkFile.HEADER_LENGTH;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.MARK_FILE_DIR_PROP_NAME;
import static io.aeron.logbuffer.LogBufferDescriptor.PAGE_MIN_SIZE;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ClusteredServiceContainerContextTest
{
    @TempDir
    private File clusterDir;
    private ClusteredServiceContainer.Context context;
    private final int serviceId = 1;

    @BeforeEach
    void setUp()
    {
        final RethrowingErrorHandler errorHandler = mock(RethrowingErrorHandler.class);
        final Aeron.Context aeronContext = mock(Aeron.Context.class);
        when(aeronContext.aeronDirectoryName()).thenReturn("funny");
        when(aeronContext.subscriberErrorHandler()).thenReturn(errorHandler);
        when(aeronContext.filePageSize()).thenReturn(PAGE_MIN_SIZE);
        final Aeron aeron = mock(Aeron.class);
        when(aeron.addCounter(
            anyInt(), any(DirectBuffer.class), anyInt(), anyInt(), any(DirectBuffer.class), anyInt(), anyInt()))
            .thenAnswer(invocation -> mock(Counter.class));
        when(aeron.context()).thenReturn(aeronContext);
        final AtomicCounter errorCounter = mock(AtomicCounter.class);
        final ClusteredService clusteredService = mock(ClusteredService.class);
        context = new ClusteredServiceContainer.Context()
            .aeron(aeron)
            .errorCounter(errorCounter)
            .errorHandler(errorHandler)
            .clusteredService(clusteredService)
            .clusterDir(clusterDir)
            .serviceId(serviceId);
    }

    @AfterEach
    void tearDown()
    {
        context.close();
    }

    @Test
    void throwsIllegalStateExceptionIfAnActiveMarkFileExists()
    {
        final ClusteredServiceContainer.Context anotherInstance = context.clone();

        context.conclude();

        final IllegalStateException exception =
            assertThrowsExactly(IllegalStateException.class, anotherInstance::conclude);
        assertThat(exception.getMessage(), startsWith("active mark file detected: "));
    }

    @Test
    void concludeShouldCreateMarkFileDirSetViaSystemProperty(final @TempDir File tempDir) throws IOException
    {
        final File rootDir = new File(tempDir, "root");
        final File markFileDir = new File(rootDir, "mark-file/../dir");
        assertFalse(markFileDir.getCanonicalFile().exists());
        context.serviceId(serviceId);

        System.setProperty(MARK_FILE_DIR_PROP_NAME, markFileDir.getAbsolutePath());
        try
        {
            assertNull(context.markFileDir());

            context.conclude();

            assertEquals(markFileDir.getCanonicalFile(), context.markFileDir());
            assertTrue(markFileDir.getCanonicalFile().exists());
            assertTrue(
                new File(context.clusterDir(), ClusterMarkFile.linkFilenameForService(context.serviceId())).exists());
        }
        finally
        {
            System.clearProperty(MARK_FILE_DIR_PROP_NAME);
        }
    }

    @Test
    void concludeShouldCreateMarkFileDirSetDirectly(final @TempDir File tempDir) throws IOException
    {
        final File rootDir = new File(tempDir, "root");
        final File markFileDir = new File(rootDir, "mark/.././file-dir");
        assertFalse(markFileDir.exists());
        context.serviceId(serviceId).markFileDir(markFileDir);

        context.conclude();

        assertEquals(markFileDir.getCanonicalFile(), context.markFileDir());
        assertTrue(markFileDir.getCanonicalFile().exists());
        assertTrue(
            new File(context.clusterDir(), ClusterMarkFile.linkFilenameForService(context.serviceId())).exists());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldRemoveLinkIfMarkFileIsInClusterDir(final boolean isSet) throws IOException
    {
        final File markFileDir = isSet ? context.clusterDir() : null;

        context.serviceId(serviceId).markFileDir(markFileDir);
        final File oldLinkFile = new File(context.clusterDir(), ClusterMarkFile.linkFilenameForService(serviceId));
        assertTrue(oldLinkFile.createNewFile());
        assertTrue(oldLinkFile.exists());

        context.conclude();

        assertFalse(oldLinkFile.exists());
    }

    @Test
    void concludeShouldCreateMarkFileLinkInTheParentDirectoryOfTheClusterMarkFile(
        final @TempDir File clusterDir,
        final @TempDir File markFileDir,
        final @TempDir File otherDir) throws IOException
    {
        final ClusterMarkFile clusterMarkFile = new ClusterMarkFile(
            new File(otherDir, "test.not"),
            ClusterComponentType.CONSENSUS_MODULE,
            ERROR_BUFFER_MIN_LENGTH,
            SystemEpochClock.INSTANCE,
            10,
            PAGE_MIN_SIZE);
        context
            .serviceId(serviceId)
            .clusterDir(clusterDir)
            .markFileDir(markFileDir)
            .clusterMarkFile(clusterMarkFile);

        context.conclude();

        assertEquals(clusterDir.getCanonicalFile(), context.clusterDir());
        assertEquals(markFileDir.getCanonicalFile(), context.markFileDir());
        assertEquals(otherDir, context.clusterMarkFile().parentDirectory());
        assertTrue(clusterDir.getCanonicalFile().exists());
        assertTrue(markFileDir.getCanonicalFile().exists());
        final File linkFile = new File(context.clusterDir(), ClusterMarkFile.linkFilenameForService(serviceId));
        assertTrue(linkFile.exists());
        assertEquals(otherDir.getCanonicalPath(), new String(Files.readAllBytes(linkFile.toPath()), US_ASCII));
    }

    @Test
    void shouldInitializeClusterDirectoryNameFromTheAssignedClusterDir(@TempDir final Path dir) throws IOException
    {
        final Path clusterDir = dir.resolve("explicit/cluster/./dir");
        context.clusterDir(clusterDir.toFile()).clusterDirectoryName("replace-me");

        context.conclude();

        assertEquals(clusterDir.toFile().getCanonicalFile(), context.clusterDir());
        assertEquals(context.clusterDir().getAbsolutePath(), context.clusterDirectoryName());
    }

    @Test
    void shouldInitializeClusterDirectoryFromTheGivenDirectoryName(@TempDir final Path temp) throws IOException
    {
        final Path dir = Paths.get(temp.toString(), "/some/../path");
        context.clusterDir(null).clusterDirectoryName(dir.toString());

        context.conclude();

        assertEquals(dir.toFile().getCanonicalFile(), context.clusterDir());
        assertEquals(context.clusterDir().getAbsolutePath(), context.clusterDirectoryName());
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldSetServiceNameIfNotSpecified(final String serviceName)
    {
        context.clusterId(7).serviceId(5).serviceName(serviceName);

        context.conclude();

        assertEquals("clustered-service-7-5", context.serviceName());
    }

    @Test
    void shouldSetServiceNameExplicitly()
    {
        context.clusterId(7).serviceId(5).serviceName("test 13");

        context.conclude();

        assertEquals("test 13", context.serviceName());
    }

    @Test
    void shouldNotSetClientName()
    {
        context.clusterId(42).serviceId(0).serviceName("test 13");

        context.conclude();

        verify(context.aeron().context(), never()).clientName(anyString());
    }

    @Test
    void shouldUseExplicitlyAssignArchiveContext()
    {
        final AeronArchive.Context archiveContext = new AeronArchive.Context()
            .controlRequestChannel("aeron:ipc")
            .controlResponseChannel("aeron:ipc");
        context.archiveContext(archiveContext);
        assertSame(archiveContext, context.archiveContext());

        context.conclude();

        assertSame(archiveContext, context.archiveContext());
        assertSame(context.aeron(), archiveContext.aeron());
        assertFalse(archiveContext.ownsAeronClient());
        assertSame(context.countedErrorHandler(), archiveContext.errorHandler());
        assertSame(NoOpLock.INSTANCE, archiveContext.lock());
    }

    @Test
    void shouldCreateArchiveContextUsingLocalChannelConfiguration()
    {
        final String controlChannel = "aeron:ipc?alias=test";
        final int localControlStreamId = 8;
        System.setProperty(AeronArchive.Configuration.LOCAL_CONTROL_CHANNEL_PROP_NAME, controlChannel);
        System.setProperty(
            AeronArchive.Configuration.LOCAL_CONTROL_STREAM_ID_PROP_NAME, Integer.toString(localControlStreamId));
        context.archiveContext(null);
        assertNull(context.archiveContext());

        try
        {
            context.conclude();

            final AeronArchive.Context archiveContext = context.archiveContext();
            assertNotNull(archiveContext);
            assertSame(context.aeron(), archiveContext.aeron());
            assertFalse(archiveContext.ownsAeronClient());
            assertSame(context.countedErrorHandler(), archiveContext.errorHandler());
            assertSame(NoOpLock.INSTANCE, archiveContext.lock());
            assertEquals(controlChannel, archiveContext.controlRequestChannel());
            assertEquals(controlChannel, archiveContext.controlResponseChannel());
            assertEquals(localControlStreamId, archiveContext.controlRequestStreamId());
            assertNotEquals(localControlStreamId, archiveContext.controlResponseStreamId());
        }
        finally
        {
            System.clearProperty(AeronArchive.Configuration.LOCAL_CONTROL_CHANNEL_PROP_NAME);
            System.clearProperty(AeronArchive.Configuration.LOCAL_CONTROL_STREAM_ID_PROP_NAME);
        }
    }

    @ParameterizedTest
    @CsvSource({ "42,88", "0,19" })
    void shouldCreateAliasForControlStreams(final int clusterId, final int controlResponseStreamId)
    {
        final String controlChannel = "aeron:ipc?term-length=64k";
        final int localControlStreamId = 0;
        System.setProperty(AeronArchive.Configuration.LOCAL_CONTROL_CHANNEL_PROP_NAME, controlChannel);
        System.setProperty(
            AeronArchive.Configuration.LOCAL_CONTROL_STREAM_ID_PROP_NAME, Integer.toString(localControlStreamId));
        System.setProperty(
            AeronArchive.Configuration.CONTROL_RESPONSE_STREAM_ID_PROP_NAME, Integer.toString(controlResponseStreamId));
        context.archiveContext(null).serviceId(5).clusterId(clusterId);
        assertNull(context.archiveContext());

        try
        {
            context.conclude();

            final AeronArchive.Context archiveContext = context.archiveContext();
            assertNotNull(archiveContext);
            assertThat(
                archiveContext.controlRequestChannel(),
                Matchers.containsString("alias=sc-5-archive-ctrl-req-cluster-" + clusterId));
            assertThat(
                archiveContext.controlResponseChannel(),
                Matchers.containsString("alias=sc-5-archive-ctrl-resp-cluster-" + clusterId));
            assertEquals(localControlStreamId, archiveContext.controlRequestStreamId());
            assertEquals(
                clusterId * 100 + 100 + controlResponseStreamId + (context.serviceId() + 1),
                archiveContext.controlResponseStreamId());
        }
        finally
        {
            System.clearProperty(AeronArchive.Configuration.LOCAL_CONTROL_CHANNEL_PROP_NAME);
            System.clearProperty(AeronArchive.Configuration.LOCAL_CONTROL_STREAM_ID_PROP_NAME);
            System.clearProperty(AeronArchive.Configuration.CONTROL_RESPONSE_STREAM_ID_PROP_NAME);
        }
    }

    @ParameterizedTest
    @CsvSource({
        "aeron:ipc,aeron:ipc?term-length=64k|mtu=8k," +
            "aeron:ipc?alias=sc-88-archive-ctrl-req-cluster-99," +
            "aeron:ipc?term-length=64k|mtu=8k|alias=sc-88-archive-ctrl-resp-cluster-99",
        "aeron:ipc?alias=x,aeron:ipc?alias=y,aeron:ipc?alias=x,aeron:ipc?alias=y"
    })
    void shouldCreateAliasForControlStreamsEvenWhenArchiveContextAssignedExplicitly(
        final String controlRequestChannel,
        final String controlResponseChannel,
        final String expectedControlRequestChannel,
        final String expectedControlResponseChannel)
    {
        final AeronArchive.Context archiveContext = new AeronArchive.Context()
            .controlRequestChannel(controlRequestChannel)
            .controlResponseChannel(controlResponseChannel)
            .controlRequestStreamId(42)
            .controlResponseStreamId(18);
        context.archiveContext(archiveContext).clusterId(99).serviceId(88);

        context.conclude();

        assertEquals(
            ChannelUri.parse(archiveContext.controlRequestChannel()),
            ChannelUri.parse(expectedControlRequestChannel));
        assertEquals(
            ChannelUri.parse(archiveContext.controlResponseChannel()),
            ChannelUri.parse(expectedControlResponseChannel));
        assertEquals(42, archiveContext.controlRequestStreamId());
        assertEquals(18, archiveContext.controlResponseStreamId());
    }

    @ParameterizedTest
    @ValueSource(ints = { 8192, 32 * 1024 })
    void shouldAlignMarkFileToTheAeronClientFilePageSize(final int filePageSize)
    {
        final Aeron.Context aeronContext = context.aeron().context();
        when(aeronContext.filePageSize()).thenReturn(filePageSize);

        context.conclude();

        final File file = new File(context.markFileDir(), ClusterMarkFile.markFilenameForService(serviceId));
        assertTrue(file.exists());
        assertEquals(BitUtil.align(context.errorBufferLength() + HEADER_LENGTH, filePageSize), file.length());

        verify(aeronContext).filePageSize();
    }

    @Test
    void shouldAlignMarkFileBasedOnTheMediaDriverFilePageSize() throws IOException
    {
        final Path aeronDir = Paths.get(CommonContext.generateRandomDirName());
        Files.createDirectories(aeronDir);

        final int filePageSize = 1024 * 1024;
        try (MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(aeronDir.toString())
            .dirDeleteOnShutdown(true)
            .threadingMode(ThreadingMode.SHARED)
            .filePageSize(filePageSize)))
        {
            context
                .aeron(null)
                .aeronDirectoryName(driver.aeronDirectoryName())
                .errorBufferLength(1919191);

            context.conclude();

            final File file = new File(context.markFileDir(), ClusterMarkFile.markFilenameForService(serviceId));
            assertTrue(file.exists());
            assertEquals(BitUtil.align(context.errorBufferLength() + HEADER_LENGTH, filePageSize), file.length());
        }
    }
}
