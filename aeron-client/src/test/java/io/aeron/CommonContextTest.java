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
package io.aeron;

import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.test.Tests;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.errors.LoggingErrorHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

import static io.aeron.CommonContext.DATE_TIME_FORMATTER;
import static io.aeron.CommonContext.FALLBACK_LOGGER_PROP_NAME;
import static io.aeron.CommonContext.FILE_NAME_FORMATTER;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.withSettings;

class CommonContextTest
{
    @TempDir
    private Path tempDir;

    @Test
    void shouldNotAllowConcludeMoreThanOnce()
    {
        final CommonContext ctx = new CommonContext();
        ctx.conclude();

        assertThrows(ConcurrentConcludeException.class, ctx::conclude);
    }

    @Test
    void setupErrorHandlerReturnsALoggingErrorHandlerInstanceIfNoUserErrorHandlerSupplied()
    {
        final DistinctErrorLog distinctErrorLog = mock(DistinctErrorLog.class);

        final ErrorHandler errorHandler = CommonContext.setupErrorHandler(null, distinctErrorLog);

        assertNotNull(errorHandler);
        final LoggingErrorHandler loggingErrorHandler = assertInstanceOf(LoggingErrorHandler.class, errorHandler);
        assertSame(distinctErrorLog, loggingErrorHandler.distinctErrorLog());
        assertSame(System.err, loggingErrorHandler.errorOverflow());
    }

    @Test
    void setupErrorHandlerUsesAFallBackLoggingHandlerForTheOverflow()
    {
        System.setProperty(FALLBACK_LOGGER_PROP_NAME, "no_op");
        try
        {
            final DistinctErrorLog distinctErrorLog = mock(DistinctErrorLog.class);

            final ErrorHandler errorHandler = CommonContext.setupErrorHandler(null, distinctErrorLog);

            assertNotNull(errorHandler);
            final LoggingErrorHandler loggingErrorHandler = assertInstanceOf(LoggingErrorHandler.class, errorHandler);
            assertSame(distinctErrorLog, loggingErrorHandler.distinctErrorLog());
            assertSame(CommonContext.fallbackLogger(), loggingErrorHandler.errorOverflow());
        }
        finally
        {
            System.clearProperty(FALLBACK_LOGGER_PROP_NAME);
        }
    }

    @Test
    void setupErrorHandlerReturnsAnErrorHandlerThatFirstInvokesLoggingErrorHandlerBeforeCallingSuppliedErrorHandler()
    {
        final Throwable throwable = new Throwable("Hello, world!");
        final ErrorHandler userErrorHandler = mock(ErrorHandler.class);
        final AssertionError userHandlerError = new AssertionError("user handler error");
        doThrow(userHandlerError).when(userErrorHandler).onError(throwable);
        final DistinctErrorLog distinctErrorLog = mock(DistinctErrorLog.class);
        doReturn(true).when(distinctErrorLog).record(any(Throwable.class));
        final InOrder inOrder = inOrder(userErrorHandler, distinctErrorLog);

        final ErrorHandler errorHandler = CommonContext.setupErrorHandler(userErrorHandler, distinctErrorLog);

        assertNotNull(errorHandler);
        assertNotSame(userErrorHandler, errorHandler);

        final AssertionError error = assertThrowsExactly(AssertionError.class, () -> errorHandler.onError(throwable));
        assertSame(userHandlerError, error);

        inOrder.verify(distinctErrorLog).record(throwable);
        inOrder.verify(userErrorHandler).onError(throwable);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void setupErrorHandlerShouldCloseUserErrorHandlerAfterClosingTheLoggingErrorHandler() throws Exception
    {
        final Throwable throwable = mock(Throwable.class);
        final ErrorHandler userErrorHandler =
            mock(ErrorHandler.class, withSettings().extraInterfaces(AutoCloseable.class));
        ((AutoCloseable)doThrow(new IOException("failed to close")).when(userErrorHandler)).close();
        final DistinctErrorLog distinctErrorLog = mock(DistinctErrorLog.class);
        doReturn(true).when(distinctErrorLog).record(any(Throwable.class));
        final PrintStream fallbackErrorStream = mock(PrintStream.class);

        final InOrder inOrder = inOrder(userErrorHandler, distinctErrorLog, throwable, fallbackErrorStream);

        final ErrorHandler errorHandler =
            CommonContext.setupErrorHandler(userErrorHandler, distinctErrorLog, fallbackErrorStream);
        assertInstanceOf(AutoCloseable.class, errorHandler);

        ((AutoCloseable)errorHandler).close();

        errorHandler.onError(throwable);

        inOrder.verify((AutoCloseable)userErrorHandler).close();
        inOrder.verify(fallbackErrorStream).println("error log is closed");
        inOrder.verify(throwable).printStackTrace(fallbackErrorStream);
        inOrder.verify(userErrorHandler).onError(throwable);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void saveExistingErrorsIsANoOpIfErrorBufferIsEmpty()
    {
        final File markFile = tempDir.resolve("mark.dat").toFile();
        final UnsafeBuffer errorBuffer = new UnsafeBuffer(new byte[0]);
        final PrintStream logger = mock(PrintStream.class);
        final String errorFilePrefix = "test-error-";

        CommonContext.saveExistingErrors(markFile, errorBuffer, logger, errorFilePrefix);

        final File[] files = tempDir.toFile().listFiles(
            (dir, name) -> name.endsWith("-error.log") && name.startsWith(errorFilePrefix));
        assertNotNull(files);
        assertEquals(0, files.length);

        verifyNoInteractions(logger);
    }

    @Test
    void saveExistingErrorsCreatesErrorFileInTheSameDirectoryAsTheCorrespondingMarkFile()
    {
        final File markFile = tempDir.resolve("mark.dat").toFile();
        final DistinctErrorLog errorLog =
            new DistinctErrorLog(new UnsafeBuffer(allocateDirect(16 * 1024)), SystemEpochClock.INSTANCE);
        assertTrue(errorLog.record(new Exception("Just to test")));
        final PrintStream logger = mock(PrintStream.class);
        final String errorFilePrefix = "my-file-";

        CommonContext.saveExistingErrors(markFile, errorLog.buffer(), logger, errorFilePrefix);

        final File[] files = tempDir.toFile().listFiles(
            (dir, name) -> name.endsWith("-error.log") && name.startsWith(errorFilePrefix));
        assertNotNull(files);
        assertEquals(1, files.length);
        final File file = files[0];

        verify(logger).println("WARNING: existing errors saved to: " + file.getAbsolutePath());
        verifyNoMoreInteractions(logger);
    }

    @Test
    void saveExistingErrorsFailsWithNullPointerExceptionIfMarkFileIsNull()
    {
        assertThrowsExactly(
            NullPointerException.class,
            () -> CommonContext.saveExistingErrors(
                null,
                mock(AtomicBuffer.class),
                mock(PrintStream.class),
                ""));
    }

    @Test
    void saveExistingErrorsFailsWithNullPointerExceptionIfErrorBufferIsNull()
    {
        assertThrowsExactly(
            NullPointerException.class,
            () -> CommonContext.saveExistingErrors(
                new File("test.dat"),
                null,
                mock(PrintStream.class),
                ""));
    }

    @Test
    void saveExistingErrorsFailsWithNullPointerExceptionIfLoggerIsNull()
    {
        assertThrowsExactly(
            NullPointerException.class,
            () -> CommonContext.saveExistingErrors(
                new File("test.dat"),
                mock(AtomicBuffer.class),
                null,
                ""));
    }

    @Test
    void saveExistingErrorsFailsWithNullPointerExceptionIfErrorFilePrefixIsNull()
    {
        assertThrowsExactly(
            NullPointerException.class,
            () -> CommonContext.saveExistingErrors(
                new File("test.dat"),
                mock(AtomicBuffer.class),
                mock(PrintStream.class),
                null));
    }

    @Test
    @EnabledOnOs(OS.MAC)
    void saveExistingErrorsShouldDumpErrorsToLoggerIfSavingToFileFails() throws Exception
    {
        final File markFile = tempDir.resolve("test.dat").toFile();
        final DistinctErrorLog errorLog =
            new DistinctErrorLog(new UnsafeBuffer(allocateDirect(16 * 1024)), SystemEpochClock.INSTANCE);
        final IndexOutOfBoundsException customError = new IndexOutOfBoundsException("test me");
        assertTrue(errorLog.record(customError));
        final PrintStream logger = mock(PrintStream.class);
        final String errorFilePrefix = "test";

        Tests.markImmutable(tempDir);
        try
        {
            CommonContext.saveExistingErrors(markFile, errorLog.buffer(), logger, errorFilePrefix);
        }
        finally
        {
            Tests.unmarkImmutable(tempDir);
        }

        final InOrder inOrder = inOrder(logger);
        final ArgumentCaptor<String> fileNameCaptor = ArgumentCaptor.forClass(String.class);
        inOrder.verify(logger).println(fileNameCaptor.capture());
        final String msg = fileNameCaptor.getValue();
        assertThat(msg, startsWith("ERROR: Failed to save existing errors to: "));
        final Path errorFilePath = Paths.get(msg.substring(msg.lastIndexOf(": ") + 2));
        assertEquals(tempDir, errorFilePath.getParent());
        final String errorFileName = errorFilePath.getFileName().toString();
        assertThat(
            errorFileName,
            allOf(startsWith(errorFilePrefix + "-"), endsWith("-error.log")));

        final ArgumentCaptor<Object> exceptionCaptor = ArgumentCaptor.forClass(Object.class);
        inOrder.verify(logger, atLeastOnce()).println(exceptionCaptor.capture());
        final String errorMessage = exceptionCaptor.getAllValues().get(0).toString();
        assertThat(errorMessage, containsString(": "));
        final Class<?> actualIoErrorClass = Class.forName(errorMessage.substring(0, errorMessage.indexOf(": ")));
        assertTrue(IOException.class.isAssignableFrom(actualIoErrorClass));

        inOrder.verify(logger).println();
        inOrder.verify(logger).println("Dumping errors here:");
        inOrder.verify(logger).println();

        final ArgumentCaptor<byte[]> savedErrorsCaptor = ArgumentCaptor.forClass(byte[].class);
        inOrder.verify(logger).write(savedErrorsCaptor.capture(), anyInt(), anyInt());
        final byte[] buff = savedErrorsCaptor.getValue();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CommonContext.printErrorLog(errorLog.buffer(), new PrintStream(baos, false, US_ASCII));
        final byte[] expected = baos.toByteArray();
        assertEquals(
            -1,
            Arrays.mismatch(expected, 0, expected.length, buff, 0, expected.length));
    }

    @Test
    void fallbackLoggerReturnsSystemErrorIfNothingSpecified()
    {
        System.clearProperty(FALLBACK_LOGGER_PROP_NAME);
        assertSame(System.err, CommonContext.fallbackLogger());
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "stderr", "gaga" })
    void fallbackLoggerReturnsSystemError(final String logger)
    {
        System.setProperty(FALLBACK_LOGGER_PROP_NAME, logger);
        try
        {
            assertSame(System.err, CommonContext.fallbackLogger());
        }
        finally
        {
            System.clearProperty(FALLBACK_LOGGER_PROP_NAME);
        }
    }

    @Test
    void fallbackLoggerReturnsSystemOutIfConfigured()
    {
        System.setProperty(FALLBACK_LOGGER_PROP_NAME, "stdout");
        try
        {
            assertSame(System.out, CommonContext.fallbackLogger());
        }
        finally
        {
            System.clearProperty(FALLBACK_LOGGER_PROP_NAME);
        }
    }

    @Test
    void fallbackLoggerReturnsNoOpLoggerIfConfigured()
    {
        System.setProperty(FALLBACK_LOGGER_PROP_NAME, "no_op");
        try
        {
            final PrintStream logger = CommonContext.fallbackLogger();
            assertNotNull(logger);
            assertNotSame(System.err, logger);
            assertNotSame(System.out, logger);
            assertSame(logger, CommonContext.fallbackLogger());
        }
        finally
        {
            System.clearProperty(FALLBACK_LOGGER_PROP_NAME);
        }
    }

    @Test
    void shouldConcludeAeronDirectory(@TempDir final Path tempDir) throws IOException
    {
        final Path aeronDirectory = tempDir.resolve("aeron.dir");
        final CommonContext commonContext = new CommonContext();
        commonContext.aeronDirectoryName(aeronDirectory.toString());
        assertNull(commonContext.aeronDirectory());

        assertSame(commonContext, commonContext.concludeAeronDirectory());

        final File concludedDir = commonContext.aeronDirectory();
        assertEquals(aeronDirectory.toFile().getCanonicalFile(), concludedDir);

        commonContext.concludeAeronDirectory();
        assertSame(concludedDir, commonContext.aeronDirectory());
    }

    @Test
    void shouldCanonicalizeAeronDirectoryPath(@TempDir final Path tempDir) throws IOException
    {
        final Path path = tempDir.resolve("one/two/../three/four/./x/y/z");
        final CommonContext commonContext = new CommonContext();
        commonContext.aeronDirectoryName(path.toString());
        assertNull(commonContext.aeronDirectory());

        assertSame(commonContext, commonContext.concludeAeronDirectory());

        assertEquals(
            tempDir.resolve("one/three/four/x/y/z").toFile().getCanonicalFile(),
            commonContext.aeronDirectory());
    }

    @ParameterizedTest
    @MethodSource("fileNameFormats")
    void fileNameFormatter(final long epochTimestampMs, final ZoneId zone, final String expected)
    {
        assertEquals(
            expected,
            FILE_NAME_FORMATTER.format(OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochTimestampMs), zone)));
    }

    @ParameterizedTest
    @MethodSource("timestampFormats")
    void timestampFormatter(final long epochTimestampMs, final ZoneId zone, final String expected)
    {
        assertEquals(
            expected,
            DATE_TIME_FORMATTER.format(OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochTimestampMs), zone)));
    }

    private static List<Arguments> fileNameFormats()
    {
        return List.of(
          Arguments.arguments(0L, ZoneOffset.UTC, "1970-01-01-00-00-00-000000+0000"),
          Arguments.arguments(1L, ZoneOffset.UTC, "1970-01-01-00-00-00-001000+0000"),
          Arguments.arguments(372492374937L, ZoneOffset.UTC, "1981-10-21-06-06-14-937000+0000"),
          Arguments.arguments(458398503485L, ZoneOffset.ofHours(5), "1984-07-11-17-55-03-485000+0500"),
          Arguments.arguments(372492374937L, ZoneOffset.ofHours(-4), "1981-10-21-02-06-14-937000-0400"),
          Arguments.arguments(1790011478302L, ZoneOffset.ofHours(1), "2026-09-21-18-24-38-302000+0100"));
    }

    private static List<Arguments> timestampFormats()
    {
        return List.of(
          Arguments.arguments(0L, ZoneOffset.UTC, "1970-01-01 00:00:00.000000+0000"),
          Arguments.arguments(1L, ZoneOffset.UTC, "1970-01-01 00:00:00.001000+0000"),
          Arguments.arguments(372492374937L, ZoneOffset.UTC, "1981-10-21 06:06:14.937000+0000"),
          Arguments.arguments(2458398503485L, ZoneOffset.ofHours(5), "2047-11-26 21:28:23.485000+0500"),
          Arguments.arguments(372492374937L, ZoneOffset.ofHours(-4), "1981-10-21 02:06:14.937000-0400"),
          Arguments.arguments(1790011478302L, ZoneOffset.ofHours(1), "2026-09-21 18:24:38.302000+0100"));
    }
}
