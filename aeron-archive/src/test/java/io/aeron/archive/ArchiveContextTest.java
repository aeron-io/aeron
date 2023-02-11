/*
 * Copyright 2014-2023 Real Logic Limited.
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
import io.aeron.exceptions.ConfigurationException;
import io.aeron.security.AuthorisationService;
import io.aeron.security.AuthorisationServiceSupplier;
import io.aeron.test.TestContexts;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.nio.file.Path;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.AeronCounters.ARCHIVE_CONTROL_SESSIONS_TYPE_ID;
import static io.aeron.AeronCounters.*;
import static io.aeron.archive.Archive.Configuration.*;
import static io.aeron.driver.Configuration.MAX_UDP_PAYLOAD_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MAX_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.RECORD_ALLOCATED;
import static org.agrona.concurrent.status.CountersReader.RECORD_UNUSED;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ArchiveContextTest
{
    private final Archive.Context context = TestContexts.localhostArchive();
    private static final int ARCHIVE_CONTROL_SESSIONS_COUNTER_ID = 928234;

    @BeforeEach
    void beforeEach(final @TempDir Path tempDir)
    {
        final Aeron aeron = mock(Aeron.class);
        final CountersReader countersReader = mock(CountersReader.class);
        final Aeron.Context aeronContext = new Aeron.Context();
        aeronContext.subscriberErrorHandler(RethrowingErrorHandler.INSTANCE);
        aeronContext.aeronDirectoryName("test-archive-config");
        when(aeron.context()).thenReturn(aeronContext);
        when(aeron.countersReader()).thenReturn(countersReader);

        context
            .aeron(aeron)
            .errorCounter(mock(AtomicCounter.class))
            .controlSessionsCounter(
                mockCounter(countersReader, ARCHIVE_CONTROL_SESSIONS_TYPE_ID, ARCHIVE_CONTROL_SESSIONS_COUNTER_ID))
            .totalWriteBytesCounter(mockCounter(countersReader, ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID, 111))
            .totalWriteTimeCounter(mockCounter(countersReader, ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID, 222))
            .maxWriteTimeCounter(mockCounter(countersReader, ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID, 333))
            .totalReadBytesCounter(mockCounter(countersReader, ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID, 77))
            .totalReadTimeCounter(mockCounter(countersReader, ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID, 88))
            .maxReadTimeCounter(mockCounter(countersReader, ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID, 99))
            .archiveDir(tempDir.resolve("archive-test").toFile());
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

        final RuntimeException exception = assertThrowsExactly(RuntimeException.class, anotherContext::conclude);
        final Throwable cause = exception.getCause();
        assertInstanceOf(IllegalStateException.class, cause);
        assertEquals("active Mark file detected", cause.getMessage());
    }

    @Test
    void shouldValidateThatSessionCounterIsOfTheCorrectType()
    {
        when(context.aeron().countersReader().getCounterTypeId(ARCHIVE_CONTROL_SESSIONS_COUNTER_ID))
            .thenReturn(AeronCounters.ARCHIVE_ERROR_COUNT_TYPE_ID);

        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void markFileDirShouldReturnArchiveDirWhenNotSet(final @TempDir File archiveDir)
    {
        context.archiveDir(archiveDir);

        assertSame(archiveDir, context.markFileDir());
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
    void concludeShouldCreateMarkFileDirSetViaSystemProperty(final @TempDir File tempDir)
    {
        final File rootDir = new File(tempDir, "root");
        final File markFileDir = new File(rootDir, "mark-file-dir");
        assertFalse(markFileDir.exists());

        System.setProperty(MARK_FILE_DIR_PROP_NAME, markFileDir.getAbsolutePath());
        try
        {
            assertSame(context.archiveDir(), context.markFileDir());

            context.conclude();

            assertEquals(markFileDir, context.markFileDir());
            assertTrue(markFileDir.exists());
        }
        finally
        {
            System.clearProperty(MARK_FILE_DIR_PROP_NAME);
        }
    }

    @Test
    void concludeShouldCreateMarkFileDirSetDirectly(final @TempDir File tempDir)
    {
        final File rootDir = new File(tempDir, "root");
        final File markFileDir = new File(rootDir, "mark-file-dir");
        assertFalse(markFileDir.exists());
        context.markFileDir(markFileDir);

        context.conclude();

        assertEquals(markFileDir, context.markFileDir());
        assertTrue(markFileDir.exists());
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
    void concludeThrowsConfigurationExceptionIfMaxWriteCounterAlreadyExistsForTheCurrentArchive()
    {
        context.maxWriteTimeCounter(null);

        final long archiveId = 42;
        final int id = 2;
        context.archiveId(archiveId);
        final AtomicBuffer metadataBuffer = mock(AtomicBuffer.class);
        final CountersReader countersReader = context.aeron().countersReader();
        when(countersReader.metaDataBuffer()).thenReturn(metadataBuffer);
        when(countersReader.maxCounterId()).thenReturn(id + 1);
        when(countersReader.getCounterState(anyInt())).thenReturn(
            RECORD_ALLOCATED, RECORD_ALLOCATED, RECORD_ALLOCATED, RECORD_UNUSED);
        when(countersReader.getCounterTypeId(anyInt())).thenReturn(
            ARCHIVE_CONTROL_SESSIONS_TYPE_ID, ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID);
        when(metadataBuffer.getLong(anyInt())).thenReturn(archiveId, Long.MAX_VALUE, archiveId);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals(
            "ERROR - existing max write time counter detected for archiveId=" + archiveId,
            exception.getMessage());
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
            context.conclude();

            assertEquals(archiveId, context.archiveId());
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
    void concludeUsesAeronClientIdIfSystemPropertyIsEmpty()
    {
        System.setProperty(ARCHIVE_ID_PROP_NAME, "");
        try
        {
            final long archiveId = 42;
            when(context.aeron().clientId()).thenReturn(archiveId);

            context.conclude();

            assertEquals(archiveId, context.archiveId());
        }
        finally
        {
            System.clearProperty(ARCHIVE_ID_PROP_NAME);
        }
    }

    @Test
    void concludeUsesAeronClientIdIfSystemPropertyIsSetToNullValue()
    {
        System.setProperty(ARCHIVE_ID_PROP_NAME, "-1");
        try
        {
            final long archiveId = 888;
            when(context.aeron().clientId()).thenReturn(archiveId);

            context.conclude();

            assertEquals(archiveId, context.archiveId());
        }
        finally
        {
            System.clearProperty(ARCHIVE_ID_PROP_NAME);
        }
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

    private Counter mockArchiveCounter(
        final long archiveId, final int typeId, final int id, final ArgumentCaptor<DirectBuffer> tempBuffer)
    {
        context.archiveId(archiveId);
        final Aeron aeron = context.aeron();
        final Counter counter = mockCounter(aeron.countersReader(), typeId, id);
        when(aeron.addCounter(
            eq(typeId), tempBuffer.capture(), eq(0), eq(SIZE_OF_LONG), any(), eq(SIZE_OF_LONG), anyInt()))
            .thenReturn(counter);
        return counter;
    }

    private static Counter mockCounter(final CountersReader countersReader, final int typeId, final int id)
    {
        final Counter counter = mock(Counter.class);
        when(counter.id()).thenReturn(id);

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
}
