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
import io.aeron.Counter;
import io.aeron.RethrowingErrorHandler;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.driver.status.DutyCycleStallTracker;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.security.AuthorisationService;
import io.aeron.security.AuthorisationServiceSupplier;
import io.aeron.test.TestContexts;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.AeronCounters.ARCHIVE_CONTROL_SESSIONS_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID;
import static io.aeron.archive.Archive.Configuration.ARCHIVE_ID_PROP_NAME;
import static io.aeron.archive.Archive.Configuration.AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME;
import static io.aeron.archive.Archive.Configuration.CONTROL_CHANNEL_ENABLED_PROP_NAME;
import static io.aeron.archive.Archive.Configuration.DEFAULT_AUTHORISATION_SERVICE_SUPPLIER;
import static io.aeron.archive.Archive.Configuration.ERROR_BUFFER_LENGTH_DEFAULT;
import static io.aeron.archive.Archive.Configuration.ERROR_BUFFER_LENGTH_PROP_NAME;
import static io.aeron.archive.Archive.Configuration.MARK_FILE_DIR_PROP_NAME;
import static io.aeron.driver.Configuration.MAX_UDP_PAYLOAD_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.PAGE_MIN_SIZE;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MAX_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class ArchiveContextTest
{
    private final Aeron aeron = mock(Aeron.class);
    private final Archive.Context context = TestContexts.localhostArchive();
    private static final int ARCHIVE_CONTROL_SESSIONS_COUNTER_ID = 928234;

    @BeforeEach
    void beforeEach(final @TempDir Path tempDir)
    {
        final CountersReader countersReader = mock(CountersReader.class);
        final MutableInteger nextCounterId = new MutableInteger(1000);
        when(aeron.addCounter(
            anyInt(), any(DirectBuffer.class), anyInt(), anyInt(), any(DirectBuffer.class), anyInt(), anyInt()))
            .thenAnswer(invocation ->
            {
                final int typeId = invocation.getArgument(0);
                final DirectBuffer labelBuffer = invocation.getArgument(4);
                final int labelOffset = invocation.getArgument(5);
                final int labelLength = invocation.getArgument(6);
                final String label = labelBuffer.getStringWithoutLengthAscii(labelOffset, labelLength);
                return mockCounter(countersReader, typeId, nextCounterId.getAndIncrement(), label);
            });
        final Aeron.Context aeronContext = mock(Aeron.Context.class);
        when(aeronContext.subscriberErrorHandler()).thenReturn(RethrowingErrorHandler.INSTANCE);
        when(aeronContext.useConductorAgentInvoker()).thenReturn(true);
        when(aeronContext.aeronDirectoryName()).thenReturn("test-archive-config");
        when(aeronContext.filePageSize()).thenReturn(PAGE_MIN_SIZE);
        when(aeron.context()).thenReturn(aeronContext);
        when(aeron.countersReader()).thenReturn(countersReader);

        final File archiveDir = tempDir.resolve("archive-test").toFile();
        init(context, archiveDir);
    }

    private void init(final Archive.Context context, final File archiveDir)
    {
        final CountersReader countersReader = aeron.countersReader();
        context.archiveDir(archiveDir)
            .aeron(aeron)
            .errorCounter(mock(AtomicCounter.class))
            .controlSessionsCounter(mockCounter(
                countersReader, ARCHIVE_CONTROL_SESSIONS_TYPE_ID, ARCHIVE_CONTROL_SESSIONS_COUNTER_ID, "label"))
            .recordingSessionCounter(mockCounter(countersReader, ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID, 101, "label"))
            .replaySessionCounter(mockCounter(countersReader, ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID, 102, "label"))
            .totalWriteBytesCounter(mockCounter(
                countersReader, ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID, 111, "label"))
            .totalWriteTimeCounter(mockCounter(countersReader, ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID, 222, "label"))
            .maxWriteTimeCounter(mockCounter(countersReader, ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID, 333, "label"))
            .totalReadBytesCounter(mockCounter(countersReader, ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID, 77, "label"))
            .totalReadTimeCounter(mockCounter(countersReader, ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID, 88, "label"))
            .maxReadTimeCounter(mockCounter(countersReader, ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID, 99, "label"));
    }

    @AfterEach
    void afterEach()
    {
        context.close();
    }

    @Test
    void defaultAuthorisationServiceSupplierReturnsAnAllowAllAuthorisationService()
    {
        assertSame(AuthorisationService.ALLOW_ALL, DEFAULT_AUTHORISATION_SERVICE_SUPPLIER.get());
    }

    @Test
    void shouldUseDefaultAuthorisationServiceSupplierIfTheSystemPropertyIsNotSet()
    {
        assertNull(context.authorisationServiceSupplier());

        context.conclude();

        System.clearProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME);
        assertSame(DEFAULT_AUTHORISATION_SERVICE_SUPPLIER, context.authorisationServiceSupplier());
    }

    @Test
    void shouldUseDefaultAuthorisationServiceSupplierIfTheSystemPropertyIsSetToEmptyValue()
    {
        System.setProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME, "");
        try
        {
            assertNull(context.authorisationServiceSupplier());

            context.conclude();

            assertSame(DEFAULT_AUTHORISATION_SERVICE_SUPPLIER, context.authorisationServiceSupplier());
        }
        finally
        {
            System.clearProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME);
        }
    }

    @Test
    void shouldInstantiateAuthorisationServiceSupplierBasedOnTheSystemProperty()
    {
        System.setProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME, TestAuthorisationSupplier.class.getName());
        try
        {
            context.conclude();
            final AuthorisationServiceSupplier supplier = context.authorisationServiceSupplier();
            assertNotSame(DEFAULT_AUTHORISATION_SERVICE_SUPPLIER, supplier);
            assertInstanceOf(TestAuthorisationSupplier.class, supplier);
        }
        finally
        {
            System.clearProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME);
        }
    }

    @Test
    void shouldUseProvidedAuthorisationServiceSupplierInstance()
    {
        final AuthorisationServiceSupplier providedSupplier = mock(AuthorisationServiceSupplier.class);
        context.authorisationServiceSupplier(providedSupplier);
        assertSame(providedSupplier, context.authorisationServiceSupplier());

        System.setProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME, TestAuthorisationSupplier.class.getName());
        try
        {
            context.conclude();
            assertSame(providedSupplier, context.authorisationServiceSupplier());
        }
        finally
        {
            System.clearProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME);
        }
    }

    @Test
    void shouldThrowIfReplicationChannelIsNotSet()
    {
        context.replicationChannel(null);
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void shouldDeriveArchiveClientContextResponseChannelFromArchiveControlChannel()
    {
        context.controlChannel("aeron:udp?endpoint=127.0.0.2:23005");
        context.conclude();
        assertEquals("aeron:udp?endpoint=127.0.0.2:0", context.archiveClientContext().controlResponseChannel());
    }

    @Test
    void shouldThrowConfigurationExceptionIfUnableToDeriveArchiveClientContextResponseChannelDueToEndpointFormat()
    {
        context.controlChannel("aeron:udp?endpoint=some_logical_name");
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void shouldThrowConfigurationExceptionIfUnableToDeriveArchiveClientContextResponseChannelDueToEndpointNull()
    {
        context.controlChannel("aeron:udp?control-mode=dynamic|control=192.168.0.1:12345");
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void shouldThrowIllegalStateExceptionIfThereIsAnActiveMarkFile()
    {
        context.conclude();
        assertNotNull(context.archiveMarkFile());
        assertNotEquals(0, context.archiveMarkFile().activityTimestampVolatile());

        final Archive.Context anotherContext = TestContexts.localhostArchive()
            .archiveDir(context.archiveDir())
            .errorHandler(context.errorHandler())
            .aeron(context.aeron());

        final IllegalStateException exception =
            assertThrowsExactly(IllegalStateException.class, anotherContext::conclude);
        assertThat(exception.getMessage(), startsWith("active mark file detected: "));
    }

    @Test
    void shouldValidateThatSessionCounterIsOfTheCorrectType()
    {
        when(context.aeron().countersReader().getCounterTypeId(ARCHIVE_CONTROL_SESSIONS_COUNTER_ID))
            .thenReturn(AeronCounters.ARCHIVE_ERROR_COUNT_TYPE_ID);

        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void markFileDirShouldReturnExplicitlySetDirectory(final @TempDir File tempDir)
    {
        final File archiveDir = new File(tempDir, "archiveDir");
        final File markFileDir = new File(tempDir, "markFileDir");
        context.archiveDir(archiveDir);
        context.markFileDir(markFileDir);

        assertSame(markFileDir, context.markFileDir());
        assertSame(archiveDir, context.archiveDir());
    }

    @Test
    void configurationMarkFileDirReturnsNullIfPropertyNotSet()
    {
        System.clearProperty(MARK_FILE_DIR_PROP_NAME);
        assertNull(Archive.Configuration.markFileDir());
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "abc", "x/y/z" })
    void configurationMarkFileDirReturnsValueSet(final String markFileDir)
    {
        System.setProperty(MARK_FILE_DIR_PROP_NAME, markFileDir);
        try
        {
            assertEquals(markFileDir, Archive.Configuration.markFileDir());
        }
        finally
        {
            System.clearProperty(MARK_FILE_DIR_PROP_NAME);
        }
    }

    @Test
    void concludeShouldCreateMarkFileDirSetViaSystemProperty(final @TempDir File tempDir) throws IOException
    {
        final File rootDir = new File(tempDir, "root");
        final File markFileDir = new File(rootDir, "mark/./file/../dir");
        assertFalse(markFileDir.exists());

        System.setProperty(MARK_FILE_DIR_PROP_NAME, markFileDir.getPath());
        try
        {
            assertNull(context.markFileDir());

            context.conclude();

            assertEquals(markFileDir.getCanonicalFile(), context.markFileDir());
            assertTrue(markFileDir.getCanonicalFile().exists());
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
        final File markFileDir = new File(rootDir, "mark/../file/./dir");
        assertFalse(markFileDir.exists());
        context.markFileDir(markFileDir);

        context.conclude();

        assertEquals(markFileDir.getCanonicalFile(), context.markFileDir());
        assertTrue(markFileDir.getCanonicalFile().exists());
    }

    @Test
    void concludeCreatesTotalWriteBytesCounter()
    {
        context.totalWriteBytesCounter(null);

        final long archiveId = 555;
        final ArgumentCaptor<DirectBuffer> tempBuffer = ArgumentCaptor.forClass(DirectBuffer.class);
        final Counter counter = mockArchiveCounter(
            archiveId, ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID, 42, tempBuffer);

        context.conclude();

        assertSame(counter, context.totalWriteBytesCounter());
        final DirectBuffer buffer = tempBuffer.getValue();
        assertEquals(archiveId, buffer.getLong(0));
        final String expectedLabel = "archive-recorder total write bytes - archiveId=" + archiveId;
        assertEquals(expectedLabel, buffer.getStringWithoutLengthAscii(SIZE_OF_LONG, expectedLabel.length()));
    }

    @Test
    void concludeValidatesTotalWriteBytesCounter()
    {
        final Counter counter = mock(Counter.class);
        context.totalWriteBytesCounter(counter);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().endsWith("expected=" + ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID));
    }

    @Test
    void concludeCreatesTotalWriteTimeCounter()
    {
        context.totalWriteTimeCounter(null);

        final long archiveId = 89;
        final ArgumentCaptor<DirectBuffer> tempBuffer = ArgumentCaptor.forClass(DirectBuffer.class);
        final Counter counter = mockArchiveCounter(
            archiveId, ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID, -666, tempBuffer);


        context.conclude();

        assertSame(counter, context.totalWriteTimeCounter());
        final DirectBuffer buffer = tempBuffer.getValue();
        assertEquals(archiveId, buffer.getLong(0));
        final String expectedLabel = "archive-recorder total write time in ns - archiveId=" + archiveId;
        assertEquals(expectedLabel, buffer.getStringWithoutLengthAscii(SIZE_OF_LONG, expectedLabel.length()));
    }

    @Test
    void concludeValidatesTotalWriteTimeCounter()
    {
        final Counter counter = mock(Counter.class);
        context.totalWriteTimeCounter(counter);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().endsWith("expected=" + ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID));
    }

    @Test
    void concludeCreatesMaxWriteTimeCounter()
    {
        context.maxWriteTimeCounter(null);

        final long archiveId = -76555;
        final ArgumentCaptor<DirectBuffer> tempBuffer = ArgumentCaptor.forClass(DirectBuffer.class);
        final Counter counter = mockArchiveCounter(
            archiveId, ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID, 234126361, tempBuffer);

        context.conclude();

        assertSame(counter, context.maxWriteTimeCounter());
        final DirectBuffer buffer = tempBuffer.getValue();
        assertEquals(archiveId, buffer.getLong(0));
        final String expectedLabel = "archive-recorder max write time in ns - archiveId=" + archiveId;
        assertEquals(expectedLabel, buffer.getStringWithoutLengthAscii(SIZE_OF_LONG, expectedLabel.length()));
    }

    @Test
    void concludeValidatesMaxWriteTimeCounter()
    {
        final Counter counter = mock(Counter.class);
        context.maxWriteTimeCounter(counter);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().endsWith("expected=" + ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID));
    }

    @Test
    void concludeCreatesTotalReadBytesCounter()
    {
        context.totalReadBytesCounter(null);

        final long archiveId = 4234623784689L;
        final ArgumentCaptor<DirectBuffer> tempBuffer = ArgumentCaptor.forClass(DirectBuffer.class);
        final Counter counter = mockArchiveCounter(
            archiveId, ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID, 999, tempBuffer);

        context.conclude();

        assertSame(counter, context.totalReadBytesCounter());
        final DirectBuffer buffer = tempBuffer.getValue();
        assertEquals(archiveId, buffer.getLong(0));
        final String expectedLabel = "archive-replayer total read bytes - archiveId=" + archiveId;
        assertEquals(expectedLabel, buffer.getStringWithoutLengthAscii(SIZE_OF_LONG, expectedLabel.length()));
    }

    @Test
    void concludeValidatesTotalReadBytesCounter()
    {
        final Counter counter = mock(Counter.class);
        context.totalReadBytesCounter(counter);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().endsWith("expected=" + ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID));
    }

    @Test
    void concludeCreatesTotalReadTimeCounter()
    {
        context.totalReadTimeCounter(null);

        final long archiveId = 3;
        final ArgumentCaptor<DirectBuffer> tempBuffer = ArgumentCaptor.forClass(DirectBuffer.class);
        final Counter counter = mockArchiveCounter(
            archiveId, ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID, 0, tempBuffer);

        context.conclude();

        assertSame(counter, context.totalReadTimeCounter());
        final DirectBuffer buffer = tempBuffer.getValue();
        assertEquals(archiveId, buffer.getLong(0));
        final String expectedLabel = "archive-replayer total read time in ns - archiveId=" + archiveId;
        assertEquals(expectedLabel, buffer.getStringWithoutLengthAscii(SIZE_OF_LONG, expectedLabel.length()));
    }

    @Test
    void concludeValidatesTotalReadTimeCounter()
    {
        final Counter counter = mock(Counter.class);
        context.totalReadTimeCounter(counter);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().endsWith("expected=" + ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID));
    }

    @Test
    void concludeCreatesMaxReadTimeCounter()
    {
        context.maxReadTimeCounter(null);

        final long archiveId = 4321L;
        final ArgumentCaptor<DirectBuffer> tempBuffer = ArgumentCaptor.forClass(DirectBuffer.class);
        final Counter counter = mockArchiveCounter(archiveId, ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID, -76, tempBuffer);

        context.conclude();

        assertSame(counter, context.maxReadTimeCounter());
    }

    @Test
    void concludeValidatesMaxReadTimeCounter()
    {
        final Counter counter = mock(Counter.class);
        context.maxReadTimeCounter(counter);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().endsWith("expected=" + ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID));
    }

    @Test
    void archiveIdIsNullValueByDefault()
    {
        assertEquals(NULL_VALUE, context.archiveId());
    }

    @ParameterizedTest
    @ValueSource(longs = { Long.MIN_VALUE, Long.MAX_VALUE, 0, 5, 28, -17 })
    void archiveIdReturnsAssignedValue(final long archiveId)
    {
        context.archiveId(archiveId);
        assertEquals(archiveId, context.archiveId());

        context.conclude();
        assertEquals(archiveId, context.archiveId());
    }

    @Test
    void concludeUsesSystemPropertyToAssignArchiveId()
    {
        final long archiveId = 53110011;
        System.setProperty(ARCHIVE_ID_PROP_NAME, Long.toString(archiveId));
        try
        {
            final Archive.Context ctx = new Archive.Context();

            assertEquals(archiveId, ctx.archiveId());
        }
        finally
        {
            System.clearProperty(ARCHIVE_ID_PROP_NAME);
        }
    }

    @Test
    void concludeUsesAeronClientIdIfSystemPropertyIsNotSet()
    {
        final long archiveId = -236462348238L;
        when(context.aeron().clientId()).thenReturn(archiveId);

        context.conclude();

        assertEquals(archiveId, context.archiveId());
    }

    @Test
    void concludeUsesAeronClientIdIfSystemPropertyIsEmpty(@TempDir final Path archiveDir)
    {
        System.setProperty(ARCHIVE_ID_PROP_NAME, "");
        final long archiveId = 42;
        final Archive.Context ctx = TestContexts.localhostArchive();
        try
        {
            init(ctx, archiveDir.toFile());
            when(aeron.clientId()).thenReturn(archiveId);

            ctx.conclude();

            assertEquals(archiveId, ctx.archiveId());
        }
        finally
        {
            ctx.close();
            System.clearProperty(ARCHIVE_ID_PROP_NAME);
        }
    }

    @Test
    void concludeUsesAeronClientIdIfSystemPropertyIsSetToNullValue(@TempDir final Path archiveDir)
    {
        System.setProperty(ARCHIVE_ID_PROP_NAME, "-1");
        final long archiveId = 888;
        final Archive.Context ctx = TestContexts.localhostArchive();
        try
        {
            init(ctx, archiveDir.toFile());
            when(aeron.clientId()).thenReturn(archiveId);

            ctx.conclude();

            assertEquals(archiveId, ctx.archiveId());
        }
        finally
        {
            ctx.close();
            System.clearProperty(ARCHIVE_ID_PROP_NAME);
        }
    }

    @ParameterizedTest
    @ValueSource(longs = { 119, 0, -5 })
    void shouldPrintArchiveId(final long archiveId)
    {
        context.archiveId(archiveId);
        context.conclude();

        assertThat(context.toString(), containsString("archiveId=" + archiveId));
    }

    @ParameterizedTest
    @ValueSource(ints = { -31, HEADER_LENGTH, MAX_UDP_PAYLOAD_LENGTH + 1, 69 })
    void shouldValidateControlMtuLength(final int controlMtuLength)
    {
        context.controlMtuLength(controlMtuLength);

        final ConfigurationException exception =
            assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().contains("mtuLength=" + controlMtuLength));
    }

    @ParameterizedTest
    @ValueSource(ints = { -100, 0, TERM_MIN_LENGTH - 1, TERM_MAX_LENGTH + 64, 100000 })
    void shouldValidateControlTermBufferLength(final int controlTermBufferLength)
    {
        context.controlTermBufferLength(controlTermBufferLength);

        final IllegalStateException exception =
            assertThrowsExactly(IllegalStateException.class, context::conclude);
        assertTrue(exception.getMessage().contains(": length=" + controlTermBufferLength));
    }

    @ParameterizedTest
    @ValueSource(ints = { -3, ERROR_BUFFER_LENGTH_DEFAULT - 1, Integer.MAX_VALUE })
    void shouldValidateErrorBufferLengthSetExplicitly(final int errorBufferLength)
    {
        context.errorBufferLength(errorBufferLength);

        final ConfigurationException exception =
            assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals("ERROR - invalid errorBufferLength=" + errorBufferLength, exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, ERROR_BUFFER_LENGTH_DEFAULT - 1, Integer.MAX_VALUE })
    void shouldValidateErrorBufferLengthSetViaSystemProperty(final int errorBufferLength)
    {
        System.setProperty(ERROR_BUFFER_LENGTH_PROP_NAME, String.valueOf(errorBufferLength));
        try
        {
            final Archive.Context ctx = TestContexts.localhostArchive();
            ctx.archiveId(42);
            final ConfigurationException exception =
                assertThrowsExactly(ConfigurationException.class, ctx::conclude);
            assertEquals("ERROR - invalid errorBufferLength=" + errorBufferLength, exception.getMessage());
        }
        finally
        {
            System.clearProperty(ERROR_BUFFER_LENGTH_PROP_NAME);
        }
    }

    @Test
    void controlChannelEnabledReturnsTrueWhenPropertyIsNotSet()
    {
        System.clearProperty(CONTROL_CHANNEL_ENABLED_PROP_NAME);
        assertTrue(Archive.Configuration.controlChannelEnabled());
    }

    @ParameterizedTest
    @CsvSource({ "'', false", "true, true", "True, false", "xyz, false" })
    void controlChannelEnabledReturnsTrueWhenPropertyIsNotSet(final String propValue, final boolean expected)
    {
        System.setProperty(CONTROL_CHANNEL_ENABLED_PROP_NAME, propValue);
        try
        {
            assertEquals(expected, Archive.Configuration.controlChannelEnabled());
        }
        finally
        {
            System.clearProperty(CONTROL_CHANNEL_ENABLED_PROP_NAME);
        }
    }

    @Test
    void controlChannelMustBeSpecified()
    {
        context.controlChannel(null);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals("ERROR - Archive.Context.controlChannel must be set", exception.getMessage());
    }

    @Test
    void controlChannelMustBeUdpChannel()
    {
        context.controlChannel("aeron:ipc");

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals(
            "ERROR - Archive.Context.controlChannel must be UDP media: uri=" + context.controlChannel(),
            exception.getMessage());
    }

    @Test
    void controlChannelMustHaveValidEndpointSpecifiedIfControlResponseChannelOfTheReplicationClientIsNotSet()
    {
        context.controlChannel("aeron:udp?endpoint=localhost");

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals(
            "ERROR - Unable to derive Archive.Context.archiveClientContext.controlResponseChannel as " +
            "Archive.Context.controlChannel.endpoint=localhost and is not in the <host>:<port> format",
            exception.getMessage());
    }

    @Test
    void whenControlChannelIsDisabledTheControlResponseChannelOnTheReplicationClientMustBeSet()
    {
        context.controlChannelEnabled(false);
        context.controlChannel("rubbish");
        final AeronArchive.Context archiveClientContext = new AeronArchive.Context();
        archiveClientContext.controlResponseChannel(null);
        context.archiveClientContext(archiveClientContext);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals(
            "ERROR - Archive.Context.archiveClientContext.controlResponseChannel must be set if " +
            "Archive.Context.controlChannelEnabled is false",
            exception.getMessage());
    }

    @Test
    void controlChannelCanBeDisabled()
    {
        context.controlChannelEnabled(false);
        context.controlChannel(null);
        final AeronArchive.Context archiveClientContext = new AeronArchive.Context();
        final String responseChannel = "aeron:udp?localhost:0";
        archiveClientContext.controlResponseChannel(responseChannel);
        context.archiveClientContext(archiveClientContext);

        context.conclude();

        assertFalse(context.controlChannelEnabled());
        assertNull(context.controlChannel());
        assertSame(archiveClientContext, context.archiveClientContext());
        assertEquals(responseChannel, context.archiveClientContext().controlResponseChannel());
    }

    @Test
    void closeOrderWhenArchiveOwnsAeronClient() throws Exception
    {
        final Aeron aeron = mock(Aeron.class);
        final ArchiveMarkFile archiveMarkFile = mock(ArchiveMarkFile.class);
        final FileChannel archiveDirChannel = mock(FileChannel.class);
        final Exception fileChannelException = throwingClose(archiveDirChannel, new IOException("file close"));
        final Catalog catalog = mock(Catalog.class);
        final Exception catalogException = throwingClose(catalog, new IllegalStateException("catalog"));
        final Counter controlSessionsCounter = mock(Counter.class);
        final AtomicCounter errorCounter = mock(AtomicCounter.class);
        final ErrorHandler errorHandler = mock(ErrorHandler.class, withSettings().extraInterfaces(AutoCloseable.class));
        final CountedErrorHandler countedErrorHandler = mock(CountedErrorHandler.class);
        context.aeron(aeron)
            .ownsAeronClient(true)
            .archiveMarkFile(archiveMarkFile)
            .archiveDirChannel(archiveDirChannel)
            .catalog(catalog)
            .errorHandler(errorHandler)
            .countedErrorHandler(countedErrorHandler)
            .controlSessionsCounter(controlSessionsCounter)
            .errorCounter(errorCounter);

        context.close();

        final InOrder inOrder = inOrder(
            aeron,
            archiveMarkFile,
            archiveDirChannel,
            catalog,
            errorHandler,
            countedErrorHandler,
            controlSessionsCounter,
            errorCounter);
        inOrder.verify(catalog).close();
        inOrder.verify(countedErrorHandler).onError(catalogException);
        inOrder.verify(archiveDirChannel).close();
        inOrder.verify(countedErrorHandler).onError(fileChannelException);
        inOrder.verify(aeron).close();
        inOrder.verify((AutoCloseable)errorHandler).close();
        inOrder.verify(archiveMarkFile).close();
        inOrder.verifyNoMoreInteractions();
    }

    @SuppressWarnings({ "methodLength", "indentation" })
    @Test
    void closeOrderWhenAeronClientIsNotOwned() throws Exception
    {
        final Aeron aeron = mock(Aeron.class);
        final ArchiveMarkFile archiveMarkFile = mock(ArchiveMarkFile.class);
        final FileChannel archiveDirChannel = mock(FileChannel.class);
        final Exception fileChannelException = throwingClose(archiveDirChannel, new IOException("file close"));
        final Catalog catalog = mock(Catalog.class);
        final Exception catalogException = throwingClose(catalog, new IllegalStateException("catalog"));
        final Counter controlSessionsCounter = mock(Counter.class);
        final Exception controlSessionsCounterException = throwingClose(
            controlSessionsCounter, new UnsupportedOperationException("controlSessionsCounter"));
        final Counter totalWriteBytesCounter = mock(Counter.class);
        final Exception totalWriteBytesCounterException = throwingClose(
            totalWriteBytesCounter, new IllegalStateException("totalWriteBytesCounter"));
        final Counter totalWriteTimeCounter = mock(Counter.class);
        final Exception totalWriteTimeCounterException = throwingClose(
            totalWriteTimeCounter, new IllegalStateException("totalWriteTimeCounter"));
        final Counter maxWriteTimeCounter = mock(Counter.class);
        final Exception maxWriteTimeCounterException = throwingClose(
            maxWriteTimeCounter, new NumberFormatException("maxWriteTimeCounter"));
        final Counter totalReadBytesCounter = mock(Counter.class);
        final Exception totalReadBytesCounterException = throwingClose(
            totalReadBytesCounter, new NumberFormatException("totalReadBytesCounter"));
        final Counter totalReadTimeCounter = mock(Counter.class);
        final Exception totalReadTimeCounterException = throwingClose(
            totalReadTimeCounter, new NoSuchMethodException("totalReadTimeCounter"));
        final Counter maxReadTimeCounter = mock(Counter.class);
        final Exception maxReadTimeCounterException = throwingClose(
            maxReadTimeCounter, new ConfigurationException("maxReadTimeCounter"));
        final AtomicCounter errorCounter = mock(AtomicCounter.class);
        final AtomicCounter conductorDutyCycleTrackerMaxCycleTime = mock(AtomicCounter.class);
        final Exception conductorDutyCycleTrackerMaxCycleTimeException = throwingClose(
            conductorDutyCycleTrackerMaxCycleTime, new NoSuchFileException("conductorDutyCycleTrackerMaxCycleTime"));
        final AtomicCounter conductorDutyCycleTrackerCycleTimeThresholdExceededCount = mock(AtomicCounter.class);
        final Exception conductorDutyCycleTrackerCycleTimeThresholdExceededCountException = throwingClose(
            conductorDutyCycleTrackerCycleTimeThresholdExceededCount,
            new NoSuchFileException("conductorDutyCycleTrackerMaxCycleTime"));
        final AtomicCounter recorderDutyCycleTrackerMaxCycleTime = mock(AtomicCounter.class);
        final Exception recorderDutyCycleTrackerMaxCycleTimeException = throwingClose(
            recorderDutyCycleTrackerMaxCycleTime,
            new UnsupportedOperationException("recorderDutyCycleTrackerMaxCycleTimeException"));
        final AtomicCounter recorderDutyCycleTrackerCycleTimeThresholdExceededCount = mock(AtomicCounter.class);
        final Exception recorderDutyCycleTrackerCycleTimeThresholdExceededCountException = throwingClose(
            recorderDutyCycleTrackerCycleTimeThresholdExceededCount,
            new UnsupportedOperationException("recorderDutyCycleTrackerCycleTimeThresholdExceededCount"));
        final AtomicCounter replayerDutyCycleTrackerMaxCycleTime = mock(AtomicCounter.class);
        final Exception replayerDutyCycleTrackerMaxCycleTimeException = throwingClose(
            replayerDutyCycleTrackerMaxCycleTime,
            new UnsupportedOperationException("replayerDutyCycleTrackerMaxCycleTime"));
        final AtomicCounter replayerDutyCycleTrackerCycleTimeThresholdExceededCount = mock(AtomicCounter.class);
        final Exception replayerDutyCycleTrackerCycleTimeThresholdExceededCountException = throwingClose(
            replayerDutyCycleTrackerCycleTimeThresholdExceededCount,
            new UnsupportedOperationException("replayerDutyCycleTrackerCycleTimeThresholdExceededCount"));
        final ErrorHandler errorHandler = mock(ErrorHandler.class, withSettings().extraInterfaces(AutoCloseable.class));
        final CountedErrorHandler countedErrorHandler = mock(CountedErrorHandler.class);
        context.aeron(aeron)
            .archiveMarkFile(archiveMarkFile)
            .archiveDirChannel(archiveDirChannel)
            .catalog(catalog)
            .errorHandler(errorHandler)
            .errorCounter(errorCounter)
            .countedErrorHandler(countedErrorHandler)
            .controlSessionsCounter(controlSessionsCounter)
            .totalWriteBytesCounter(totalWriteBytesCounter)
            .totalWriteTimeCounter(totalWriteTimeCounter)
            .maxWriteTimeCounter(maxWriteTimeCounter)
            .totalReadBytesCounter(totalReadBytesCounter)
            .totalReadTimeCounter(totalReadTimeCounter)
            .maxReadTimeCounter(maxReadTimeCounter)
            .conductorDutyCycleTracker(new DutyCycleStallTracker(
            conductorDutyCycleTrackerMaxCycleTime, conductorDutyCycleTrackerCycleTimeThresholdExceededCount, 1))
            .recorderDutyCycleTracker(new DutyCycleStallTracker(
            recorderDutyCycleTrackerMaxCycleTime, recorderDutyCycleTrackerCycleTimeThresholdExceededCount, 1))
            .replayerDutyCycleTracker(new DutyCycleStallTracker(
            replayerDutyCycleTrackerMaxCycleTime, replayerDutyCycleTrackerCycleTimeThresholdExceededCount, 1));

        context.close();

        final InOrder inOrder = inOrder(
            aeron,
            archiveMarkFile,
            archiveDirChannel,
            catalog,
            errorHandler,
            countedErrorHandler,
            controlSessionsCounter,
            totalWriteBytesCounter,
            totalWriteTimeCounter,
            maxWriteTimeCounter,
            totalReadBytesCounter,
            totalReadTimeCounter,
            maxReadTimeCounter,
            conductorDutyCycleTrackerMaxCycleTime,
            conductorDutyCycleTrackerCycleTimeThresholdExceededCount,
            recorderDutyCycleTrackerMaxCycleTime,
            recorderDutyCycleTrackerCycleTimeThresholdExceededCount,
            replayerDutyCycleTrackerMaxCycleTime,
            replayerDutyCycleTrackerCycleTimeThresholdExceededCount,
            errorCounter);
        inOrder.verify(catalog).close();
        inOrder.verify(countedErrorHandler).onError(catalogException);
        inOrder.verify(archiveDirChannel).close();
        inOrder.verify(countedErrorHandler).onError(fileChannelException);
        inOrder.verify(controlSessionsCounter).close();
        inOrder.verify(countedErrorHandler).onError(controlSessionsCounterException);
        inOrder.verify(totalWriteBytesCounter).close();
        inOrder.verify(countedErrorHandler).onError(totalWriteBytesCounterException);
        inOrder.verify(totalWriteTimeCounter).close();
        inOrder.verify(countedErrorHandler).onError(totalWriteTimeCounterException);
        inOrder.verify(maxWriteTimeCounter).close();
        inOrder.verify(countedErrorHandler).onError(maxWriteTimeCounterException);
        inOrder.verify(totalReadBytesCounter).close();
        inOrder.verify(countedErrorHandler).onError(totalReadBytesCounterException);
        inOrder.verify(totalReadTimeCounter).close();
        inOrder.verify(countedErrorHandler).onError(totalReadTimeCounterException);
        inOrder.verify(maxReadTimeCounter).close();
        inOrder.verify(countedErrorHandler).onError(maxReadTimeCounterException);
        inOrder.verify(conductorDutyCycleTrackerMaxCycleTime).close();
        inOrder.verify(countedErrorHandler).onError(conductorDutyCycleTrackerMaxCycleTimeException);
        inOrder.verify(conductorDutyCycleTrackerCycleTimeThresholdExceededCount).close();
        inOrder.verify(countedErrorHandler).onError(conductorDutyCycleTrackerCycleTimeThresholdExceededCountException);
        inOrder.verify(recorderDutyCycleTrackerMaxCycleTime).close();
        inOrder.verify(countedErrorHandler).onError(recorderDutyCycleTrackerMaxCycleTimeException);
        inOrder.verify(recorderDutyCycleTrackerCycleTimeThresholdExceededCount).close();
        inOrder.verify(countedErrorHandler).onError(recorderDutyCycleTrackerCycleTimeThresholdExceededCountException);
        inOrder.verify(replayerDutyCycleTrackerMaxCycleTime).close();
        inOrder.verify(countedErrorHandler).onError(replayerDutyCycleTrackerMaxCycleTimeException);
        inOrder.verify(replayerDutyCycleTrackerCycleTimeThresholdExceededCount).close();
        inOrder.verify(countedErrorHandler).onError(replayerDutyCycleTrackerCycleTimeThresholdExceededCountException);
        inOrder.verify(errorCounter).close();
        inOrder.verify((AutoCloseable)errorHandler).close();
        inOrder.verify(archiveMarkFile).close();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldNotCreateLinkToTheDefaultMarkFile(@TempDir final Path tempDir) throws IOException
    {
        final Path archiveDir = tempDir.resolve("archive-dir");
        Files.createDirectories(archiveDir);
        final Path linkFile = archiveDir.resolve(ArchiveMarkFile.LINK_FILENAME);
        Files.createFile(linkFile);
        context.archiveDir(archiveDir.toFile());

        context.conclude();

        assertEquals(archiveDir.toFile().getCanonicalFile(), context.markFileDir());
        final ArchiveMarkFile archiveMarkFile = context.archiveMarkFile();
        assertNotNull(archiveMarkFile);
        assertEquals(archiveDir.toFile().getCanonicalFile(), archiveMarkFile.parentDirectory());
        final Path markFile = archiveDir.resolve(ArchiveMarkFile.FILENAME);
        assertTrue(Files.exists(markFile));
        assertTrue(Files.notExists(linkFile));
    }

    @Test
    void shouldCreateALinkToTheArchiveMarkFileInAnotherDirectory(
        @TempDir final Path archiveDir, @TempDir final Path temp2) throws IOException
    {
        final File markFileDirectory = temp2.resolve("x/y/../z/../w").toFile();
        context.archiveDir(archiveDir.toFile()).markFileDir(markFileDirectory);

        context.conclude();

        assertEquals(archiveDir.toFile().getCanonicalFile(), context.archiveDir());
        assertEquals(markFileDirectory.getCanonicalFile(), context.markFileDir());
        final ArchiveMarkFile archiveMarkFile = context.archiveMarkFile();
        assertNotNull(archiveMarkFile);
        assertEquals(markFileDirectory.getCanonicalFile(), archiveMarkFile.parentDirectory());
        final Path markFile = markFileDirectory.getCanonicalFile().toPath().resolve(ArchiveMarkFile.FILENAME);
        assertTrue(Files.exists(markFile));
        final Path linkFile = archiveDir.resolve(ArchiveMarkFile.LINK_FILENAME);
        assertTrue(Files.exists(linkFile));
        assertEquals(markFileDirectory.getCanonicalPath(), new String(Files.readAllBytes(linkFile), US_ASCII));
    }

    @Test
    void shouldCreateALinkToTheArchiveMarkFileWhichIsExplicitlyAssigned(
        @TempDir final Path archiveDir,
        @TempDir final Path markFileDir,
        @TempDir final Path archiveMarkFileDir) throws IOException
    {
        final ArchiveMarkFile archiveMarkFile = new ArchiveMarkFile(
            archiveMarkFileDir.resolve("my-funny-file.txt").toFile(), 1024 * 1024, 8096, SystemEpochClock.INSTANCE, 0);
        context
            .archiveDir(archiveDir.toFile())
            .markFileDir(markFileDir.toFile())
            .archiveMarkFile(archiveMarkFile);

        context.conclude();

        assertEquals(archiveDir.toFile().getCanonicalFile(), context.archiveDir());
        assertEquals(markFileDir.toFile().getCanonicalFile(), context.markFileDir());
        assertSame(archiveMarkFile, context.archiveMarkFile());
        assertEquals(archiveMarkFileDir.toFile(), archiveMarkFile.parentDirectory());
        final Path linkFile = archiveDir.resolve(ArchiveMarkFile.LINK_FILENAME);
        assertTrue(Files.exists(linkFile));
        assertEquals(
            archiveMarkFileDir.toFile().getCanonicalPath(), new String(Files.readAllBytes(linkFile), US_ASCII));
    }

    @Test
    void shouldVerifyConductorInvokeModeOnAeronClient()
    {
        assertSame(aeron, context.aeron());
        when(aeron.context().useConductorAgentInvoker()).thenReturn(false);

        final ArchiveException exception = assertThrowsExactly(ArchiveException.class, context::conclude);
        assertEquals(
            "ERROR - Aeron client instance must set Aeron.Context.useConductorInvoker(true)",
            exception.getMessage());
    }

    @Test
    void shouldUseExplicitlyAssignedClient()
    {
        assertSame(aeron, context.aeron());
        assertFalse(context.ownsAeronClient());

        context.conclude();

        assertSame(aeron, context.aeron());
        assertFalse(context.ownsAeronClient());
    }

    @Test
    void shouldInitializeAeronDirectoryFromTheClient()
    {
        context.aeronDirectoryName(null);
        final String clientDirectory = "target/dir";
        when(context.aeron().context().aeronDirectoryName()).thenReturn(clientDirectory);
        assertNull(context.aeronDirectoryName());

        context.conclude();

        assertEquals(clientDirectory, context.aeronDirectoryName());
    }

    @Test
    void shouldInitializeArchiveDirectoryNameFromArchiveDir(@TempDir final Path root) throws IOException
    {
        final File archiveDir = root.resolve("n/m/../x/./1111").toFile();
        context.archiveDir(archiveDir);

        context.conclude();

        assertEquals(archiveDir.getCanonicalPath(), context.archiveDirectoryName());
    }

    @ParameterizedTest
    @EnumSource(ArchiveThreadingMode.class)
    void shouldExposeThreadingModeInfoViaDutyCycleTrackers(final ArchiveThreadingMode threadingMode)
    {
        context
            .conductorCycleThresholdNs(TimeUnit.MILLISECONDS.toNanos(150))
            .recorderCycleThresholdNs(TimeUnit.SECONDS.toNanos(2))
            .replayerCycleThresholdNs(TimeUnit.MICROSECONDS.toNanos(879))
            .threadingMode(threadingMode)
            .archiveId(888);

        context.conclude();

        final DutyCycleStallTracker conductorDutyCycleTracker =
            (DutyCycleStallTracker)context.conductorDutyCycleTracker();
        assertNotNull(conductorDutyCycleTracker);
        assertEquals(
            "archive-conductor max cycle time in ns: " + threadingMode + " - archiveId=" + context.archiveId(),
            conductorDutyCycleTracker.maxCycleTime().label());
        assertEquals(
            "archive-conductor work cycle time exceeded count: threshold=150ms " + threadingMode +
                " - archiveId=" + context.archiveId(),
            conductorDutyCycleTracker.cycleTimeThresholdExceededCount().label());

        if (ArchiveThreadingMode.DEDICATED == threadingMode)
        {
            final DutyCycleStallTracker recorderDutyCycleTracker =
                (DutyCycleStallTracker)context.recorderDutyCycleTracker();
            assertNotNull(recorderDutyCycleTracker);
            assertEquals(
                "archive-recorder max cycle time in ns: " + threadingMode + " - archiveId=" + context.archiveId(),
                recorderDutyCycleTracker.maxCycleTime().label());
            assertEquals(
                "archive-recorder work cycle time exceeded count: threshold=2s " + threadingMode +
                    " - archiveId=" + context.archiveId(),
                recorderDutyCycleTracker.cycleTimeThresholdExceededCount().label());

            final DutyCycleStallTracker replayerDutyCycleTracker =
                (DutyCycleStallTracker)context.replayerDutyCycleTracker();
            assertNotNull(replayerDutyCycleTracker);
            assertEquals(
                "archive-replayer max cycle time in ns: " + threadingMode + " - archiveId=" + context.archiveId(),
                replayerDutyCycleTracker.maxCycleTime().label());
            assertEquals(
                "archive-replayer work cycle time exceeded count: threshold=879us " + threadingMode +
                    " - archiveId=" + context.archiveId(),
                replayerDutyCycleTracker.cycleTimeThresholdExceededCount().label());
        }
    }

    @Test
    void shouldNotSetClientNameOnAnExplicitlyAssignedAeronClient()
    {
        final Aeron.Context aeronContext = aeron.context();
        when(aeronContext.clientName()).thenReturn("sample");
        context.archiveId(42);

        context.conclude();

        assertEquals("sample", aeronContext.clientName());
    }

    @ParameterizedTest
    @EnumSource(value = ArchiveThreadingMode.class, names = "INVOKER", mode = EnumSource.Mode.EXCLUDE)
    void shouldRequireInvokerModeIfMediaDriverInvokerIsSpecified(final ArchiveThreadingMode threadingMode)
    {
        final AgentInvoker mediaDriverAgentInvoker = mock(AgentInvoker.class);
        context
            .mediaDriverAgentInvoker(mediaDriverAgentInvoker)
            .threadingMode(threadingMode);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals(
            "ERROR - Archive.Context.threadingMode(ArchiveThreadingMode.INVOKER) must be set if " +
            "Archive.Context.mediaDriverAgentInvoker is set",
            exception.getMessage());
    }

    private Counter mockArchiveCounter(
        final long archiveId, final int typeId, final int id, final ArgumentCaptor<DirectBuffer> tempBuffer)
    {
        context.archiveId(archiveId);
        final Aeron aeron = context.aeron();
        final Counter counter = mockCounter(aeron.countersReader(), typeId, id, "label");
        when(aeron.addCounter(
            eq(typeId), tempBuffer.capture(), eq(0), eq(SIZE_OF_LONG), any(), eq(SIZE_OF_LONG), anyInt()))
            .thenReturn(counter);
        return counter;
    }

    private static Counter mockCounter(
        final CountersReader countersReader, final int typeId, final int id, final String label)
    {
        final Counter counter = mock(Counter.class);
        when(counter.id()).thenReturn(id);
        when(counter.label()).thenReturn(label);
        when(countersReader.getCounterTypeId(id)).thenReturn(typeId);
        return counter;
    }

    public static class TestAuthorisationSupplier implements AuthorisationServiceSupplier
    {
        public AuthorisationService get()
        {
            return new TestAuthorisationService();
        }
    }

    static class TestAuthorisationService implements AuthorisationService
    {
        public boolean isAuthorised(
            final int protocolId, final int actionId, final Object type, final byte[] encodedPrincipal)
        {
            return false;
        }
    }

    private static Exception throwingClose(final AutoCloseable resource, final Exception exception) throws Exception
    {
        doThrow(exception).when(resource).close();
        return exception;
    }
}
