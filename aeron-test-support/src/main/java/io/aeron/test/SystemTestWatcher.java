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
package io.aeron.test;

import io.aeron.CommonContext;
import io.aeron.archive.ArchiveMarkFile;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.ClusterTerminationException;
import io.aeron.samples.SamplesUtil;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.driver.DriverOutputConsumer;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.SemanticVersion;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableReference;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.errors.ErrorConsumer;
import org.agrona.concurrent.errors.ErrorLogReader;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.opentest4j.AssertionFailedError;
import org.opentest4j.TestAbortedException;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.aeron.CncFileDescriptor.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SystemTestWatcher implements
    DriverOutputConsumer,
    AfterTestExecutionCallback,
    BeforeEachCallback,
    AfterEachCallback
{
    public static final Pattern PARAMETERISED_TEST_INDEX_PATTERN = Pattern.compile("\\[([0-9]+)].*");
    private static final String CLUSTER_TERMINATION_EXCEPTION = ClusterTerminationException.class.getName();
    private static final String UNKNOWN_HOST_EXCEPTION = UnknownHostException.class.getName();
    private static final String ATS_GCM_DECRYPT_ERROR =
        "ats_gcm_decrypt final_ex: error:00000000:lib(0):func(0):reason(0)";
    private static final String ATS_GCM_DECRYPT_ERROR_OTHER =
        "ats_gcm_decrypt final_ex: error:00000000:lib(0)::reason(0)";
    public static final Predicate<String> UNKNOWN_HOST_FILTER =
        (s) -> s.contains(UNKNOWN_HOST_EXCEPTION) || s.contains("unknown host");
    public static final Predicate<String> WARNING_FILTER = (s) -> s.contains("WARN");
    public static final Predicate<String> CLUSTER_TERMINATION_FILTER =
        (s) -> s.contains(CLUSTER_TERMINATION_EXCEPTION);
    public static final Predicate<String> ATS_GCM_DECRYPT_ERROR_FILTER =
        (s) -> s.contains(ATS_GCM_DECRYPT_ERROR) || s.contains(ATS_GCM_DECRYPT_ERROR_OTHER);
    public static final Predicate<String> TEST_CLUSTER_DEFAULT_LOG_FILTER =
        WARNING_FILTER.negate()
        .and(CLUSTER_TERMINATION_FILTER.negate())
        .and(ATS_GCM_DECRYPT_ERROR_FILTER.negate());

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

    private final MediaDriverTestUtil mediaDriverTestUtil = new MediaDriverTestUtil();
    private final Map<String, MarkFileDissector> errorDissectors = new Object2ObjectHashMap<>();

    private Predicate<String> logFilter = TEST_CLUSTER_DEFAULT_LOG_FILTER;
    private DataCollector dataCollector = new DataCollector();
    private final ArrayList<AutoCloseable> closeables = new ArrayList<>();
    private long startTimeNs;

    public SystemTestWatcher()
    {
        addDissectorInternal(new ArchiveMarkFileDissector());
        addDissectorInternal(new ConsensusModuleMarkFileDissector());
        addDissectorInternal(new ClusteredServiceMarkFileDissector());
    }

    public SystemTestWatcher cluster(final TestCluster testCluster)
    {
        this.dataCollector = testCluster.dataCollector();
        return addClosable(testCluster);
    }

    public SystemTestWatcher addClosable(final AutoCloseable closeable)
    {
        closeables.add(Objects.requireNonNull(closeable));
        return this;
    }

    private void addDissectorInternal(final MarkFileDissector markFileDissector)
    {
        errorDissectors.put(markFileDissector.filename(), markFileDissector);
    }

    @SuppressWarnings("unused")
    public SystemTestWatcher addDissector(final MarkFileDissector markFileDissector)
    {
        addDissectorInternal(markFileDissector);
        return this;
    }

    public DataCollector dataCollector()
    {
        return dataCollector;
    }

    public void outputFiles(final String aeronDirectoryName, final File stdoutFile, final File stderrFile)
    {
        mediaDriverTestUtil.outputFiles(aeronDirectoryName, stdoutFile, stderrFile);
        dataCollector.add(stdoutFile);
        dataCollector.add(stderrFile);
    }

    public void exitCode(final String aeronDirectoryName, final int exitValue, final String exitMessage)
    {
        mediaDriverTestUtil.exitCode(aeronDirectoryName, exitValue, exitMessage);
    }

    public void environmentVariables(final String aeronDirectoryName, final Map<String, String> environment)
    {
        mediaDriverTestUtil.environmentVariables(aeronDirectoryName, environment);
    }

    @SuppressWarnings("UnusedReturnValue")
    public SystemTestWatcher ignoreErrorsMatching(final Predicate<String> logFilter)
    {
        this.logFilter = this.logFilter.and(logFilter.negate());
        return this;
    }

    /**
     * Useful when debugging tests to get them to fail on warnings as well as errors.
     */
    @SuppressWarnings("unused")
    public void showAllErrors()
    {
        this.logFilter = (s) -> true;
    }

    public void beforeEach(final ExtensionContext context)
    {
        Thread.interrupted(); // clean the interrupted flag so that it does not affect the next test
        startTimeNs = System.nanoTime();
    }

    @SuppressWarnings("MethodLength")
    public void afterTestExecution(final ExtensionContext context)
    {
        final long endTimeNs = System.nanoTime();
        Thread.interrupted(); // clean the interrupted flag so that it does not prevent cleanup in the tests

        Throwable error = context.getExecutionException()
            .filter((t) -> !(t instanceof TestAbortedException))
            .orElse(null);
        try
        {
            try
            {
                mediaDriverTestUtil.afterTestExecution(context);
            }
            catch (final Throwable t)
            {
                error = Tests.setOrUpdateError(error, t);
            }

            try
            {
                final MutableInteger count = new MutableInteger();
                final StringBuilder errors = new StringBuilder();
                filterErrors(count, errors);

                if (null == error)
                {
                    assertEquals(
                        0, count.get(), () -> "Errors observed in " + context.getDisplayName() + ":\n" + errors);
                }
                else if (0 != count.get())
                {
                    error = Tests.setOrUpdateError(error, new AssertionFailedError(
                        "Errors observed in " + context.getDisplayName() + ":\n" + errors));
                }
            }
            catch (final Throwable t)
            {
                error = Tests.setOrUpdateError(error, t);
            }

            if (null != error)
            {
                final String testMethod = context.getTestClass().map(Class::getName).orElse("unknown") + "-" +
                    context.getTestMethod().map(Method::getName).orElse("unknown");
                final String testName;
                final String directoryName;
                if (context.getTestMethod().map((m) -> m.getAnnotation(ParameterizedTest.class)).isPresent())
                {
                    testName = testMethod + "(" + context.getDisplayName() + ")";
                    final Matcher matcher = PARAMETERISED_TEST_INDEX_PATTERN.matcher(context.getDisplayName());
                    if (matcher.matches())
                    {
                        directoryName = testMethod + "_" + matcher.group(1);
                    }
                    else
                    {
                        directoryName = testMethod + "_" + System.nanoTime();
                    }
                }
                else
                {
                    testName = testMethod + "()";
                    directoryName = testMethod;
                }

                System.out.println(
                    "*** " + testName + " failed in endTimeNs(" + endTimeNs + ") - startTimeNs(" + startTimeNs + ") " +
                    " = " + NANOSECONDS.toMillis(endTimeNs - startTimeNs) + " ms, cause: " + error);
                final Throwable terminateError = reportAndTerminate(directoryName);
                error = Tests.setOrUpdateError(error, terminateError);
                try
                {
                    mediaDriverTestUtil.testFailed();
                }
                catch (final Throwable t)
                {
                    error = Tests.setOrUpdateError(error, t);
                }
            }
            else
            {
                setTerminationExpected();
                try
                {
                    CloseHelper.closeAll(closeables);
                }
                catch (final Throwable t)
                {
                    error = Tests.setOrUpdateError(error, t);
                }

                try
                {
                    mediaDriverTestUtil.testSuccessful();
                }
                catch (final Throwable t)
                {
                    error = Tests.setOrUpdateError(error, t);
                }
            }
        }
        finally
        {
            if (null != error)
            {
                System.out.println("*** Complete stack trace (PID=" + SystemUtil.getPid() + "): ");
                error.printStackTrace(System.out);
                LangUtil.rethrowUnchecked(error);
            }
        }
    }

    public void afterEach(final ExtensionContext context)
    {
        deleteAllLocations();
    }

    private void setTerminationExpected()
    {
        for (final AutoCloseable closeable : closeables)
        {
            if (closeable instanceof TestCluster)
            {
                ((TestCluster)closeable).terminationsExpected(true);
            }
        }
    }

    private void filterErrors(final MutableInteger count, final StringBuilder errors) throws IOException
    {
        filterCncFileErrors(dataCollector.cncFiles(), count, errors);

        for (final MarkFileDissector dissector : errorDissectors.values())
        {
            filterErrors(dissector, count, errors);
        }
    }

    private void filterErrors(
        final MarkFileDissector markFileDissector,
        final MutableInteger count,
        final StringBuilder errors) throws IOException
    {
        final List<Path> paths = dataCollector.markFiles(markFileDissector);
        markFileDissector.filterErrors(paths, count, errors, logFilter);
    }

    private void filterCncFileErrors(final List<Path> paths, final MutableInteger count, final StringBuilder errors)
    {
        for (final Path path : paths)
        {
            final File file = path.toFile();
            if (file.exists() && file.length() > 0)
            {
                final MappedByteBuffer mmap = SamplesUtil.mapExistingFileReadOnly(file);
                try
                {
                    final UnsafeBuffer metaDataBuffer = createMetaDataBuffer(mmap);
                    final int errorLogBufferLength = metaDataBuffer.getInt(errorLogBufferLengthOffset(0));
                    if (errorLogBufferLength > 0)
                    {
                        readErrors(path, CommonContext.errorLogBuffer(mmap), count, errors, logFilter);
                    }
                }
                finally
                {
                    IoUtil.unmap(mmap);
                }
            }
        }
    }

    public static void readErrors(
        final Path path,
        final AtomicBuffer buffer,
        final MutableInteger count,
        final StringBuilder errors,
        final Predicate<String> logFilter)
    {
        ErrorLogReader.read(
            buffer,
            (observationCount, firstObservationTimestamp, lastObservationTimestamp, encodedException) ->
            {
                if (logFilter.test(encodedException))
                {
                    count.set(count.get() + observationCount);
                    appendError(errors, path, encodedException);
                }
            });
    }

    private static void appendError(final StringBuilder errors, final Path path, final String encodedException)
    {
        errors.append(path).append(": ").append(encodedException).append('\n');
    }

    private static ClusterMarkFile openClusterMarkFile(final Path path)
    {
        try
        {
            return new ClusterMarkFile(
                path.getParent().toFile(), path.getFileName().toString(), SystemEpochClock.INSTANCE, 0, (s) -> {});
        }
        catch (final RuntimeException ex)
        {
            throw new RuntimeException("Failed to open mark file=" + path, ex);
        }
    }

    private static ArchiveMarkFile openArchiveMarkFile(final Path path)
    {
        return new ArchiveMarkFile(
            path.getParent().toFile(), path.getFileName().toString(), SystemEpochClock.INSTANCE, 0, (s) -> {});
    }

    private void printObservationCallback(
        final int observationCount,
        final long firstObservationTimestamp,
        final long lastObservationTimestamp,
        final String encodedException)
    {
        final String ignored = !logFilter.test(encodedException) ? "(ignored) " : "";
        System.out.format(
            "***%n%s%d observations from %s to %s for:%n %s%n",
            ignored,
            observationCount,
            DATE_FORMAT.format(new Date(firstObservationTimestamp)),
            DATE_FORMAT.format(new Date(lastObservationTimestamp)),
            encodedException);
    }

    private Throwable reportAndTerminate(final String directoryName)
    {
        final MutableReference<Throwable> error = new MutableReference<>();
        setOrUpdateError(error, printCncInfo(dataCollector.cncFiles()));

        errorDissectors.forEach((filename, dissector) -> setOrUpdateError(
            error,
            dissector.printErrors(dataCollector.markFiles(dissector), this::printObservationCallback)));

        //grab thread dump while components are still running
        final byte[] threadDump = SystemUtil.threadDump().getBytes(UTF_8);

        try
        {
            System.out.println("Reported and termination: " + closeables);
            CloseHelper.closeAll(closeables);
        }
        catch (final Throwable t)
        {
            setOrUpdateError(error, t);
        }

        try
        {
            dataCollector.dumpData(directoryName, threadDump);
        }
        catch (final Throwable t)
        {
            setOrUpdateError(error, t);
        }

        return error.get();
    }

    private static void setOrUpdateError(final MutableReference<Throwable> existingError, final Throwable newError)
    {
        if (null == existingError.get())
        {
            existingError.set(newError);
        }
        else if (null != newError)
        {
            existingError.get().addSuppressed(newError);
        }
    }

    private Throwable printCncInfo(final List<Path> paths)
    {
        Throwable error = null;
        for (final Path path : paths)
        {
            final File cncFile = path.toFile();
            if (!cncFile.exists() || 0 == cncFile.length())
            {
                System.out.printf("%n%nCommand `n Control file %s was not created!%n", cncFile);
                continue;
            }
            System.out.printf("%n%nCommand `n Control file %s, length=%d%n", cncFile, cncFile.length());
            System.out.println("---------------------------------------------------------------------------------");
            final MappedByteBuffer mappedByteBuffer = SamplesUtil.mapExistingFileReadOnly(cncFile);
            try
            {
                final UnsafeBuffer metaDataBuffer = createMetaDataBuffer(mappedByteBuffer);
                final int cncVersion = metaDataBuffer.getInt(cncVersionOffset(0));
                System.out.printf(
                    "%27s: %s%n", "version", 0 == cncVersion ? "N/A" : SemanticVersion.toString(cncVersion));
                System.out.printf(
                    "%27s: %d%n", "toDriverBufferLength", metaDataBuffer.getInt(toDriverBufferLengthOffset(0)));
                System.out.printf(
                    "%27s: %d%n", "toClientsBufferLength", metaDataBuffer.getInt(toClientsBufferLengthOffset(0)));
                final int counterMetaDataBufferLength = metaDataBuffer.getInt(countersMetaDataBufferLengthOffset(0));
                System.out.printf("%27s: %d%n", "counterMetaDataBufferLength", counterMetaDataBufferLength);
                final int counterValuesBufferLength = metaDataBuffer.getInt(countersValuesBufferLengthOffset(0));
                System.out.printf("%27s: %d%n", "counterValuesBufferLength", counterValuesBufferLength);
                final int errorLogBufferLength = metaDataBuffer.getInt(errorLogBufferLengthOffset(0));
                System.out.printf("%27s: %d%n", "errorLogBufferLength", errorLogBufferLength);
                System.out.printf(
                    "%27s: %d%n", "clientLivenessTimeoutNs", metaDataBuffer.getLong(clientLivenessTimeoutOffset(0)));
                System.out.printf(
                    "%27s: %d%n", "startTimestampMs", metaDataBuffer.getLong(startTimestampOffset(0)));
                System.out.printf("%27s: %d%n", "pid", metaDataBuffer.getLong(pidOffset(0)));
                final UnsafeBuffer toDriverBuffer = createToDriverBuffer(mappedByteBuffer, metaDataBuffer);
                final int driveHeartbeatOffset = toDriverBuffer.capacity() - RingBufferDescriptor.TRAILER_LENGTH +
                    RingBufferDescriptor.CONSUMER_HEARTBEAT_OFFSET;
                System.out.printf("%27s: %s%n", "driverHeartbeatMs",
                    driveHeartbeatOffset < 0 ? "N/A" : toDriverBuffer.getLong(driveHeartbeatOffset));
                System.out.println("---------------------------------------------------------------------------------");

                if (counterMetaDataBufferLength > 0 && counterValuesBufferLength > 0)
                {
                    final CountersReader countersReader = new CountersReader(
                        createCountersMetaDataBuffer(mappedByteBuffer, metaDataBuffer),
                        createCountersValuesBuffer(mappedByteBuffer, metaDataBuffer),
                        StandardCharsets.US_ASCII);
                    countersReader.forEach(
                        (counterId, label) ->
                        {
                            final long value = countersReader.getCounterValue(counterId);
                            System.out.format("%3d: %,20d - %s%n", counterId, value, label);
                        });
                    System.out.println(
                        "---------------------------------------------------------------------------------");
                }

                if (errorLogBufferLength > 0)
                {
                    final AtomicBuffer buffer =
                        createErrorLogBuffer(mappedByteBuffer, metaDataBuffer);
                    System.out.printf("%nCommand `n Control Errors%n");
                    final int distinctErrorCount = ErrorLogReader.read(buffer, this::printObservationCallback);
                    System.out.format("%d distinct errors observed.%n", distinctErrorCount);
                }
            }
            catch (final Throwable t)
            {
                error = Tests.setOrUpdateError(error, t);
            }
            finally
            {
                IoUtil.unmap(mappedByteBuffer);
            }
        }

        return error;
    }

    private void deleteAllLocations()
    {
        for (final Path path : dataCollector.cleanupLocations())
        {
            try
            {
                IoUtil.delete(path.toFile(), true);
            }
            catch (final Exception e)
            {
                System.err.println("Failed to delete: '" + path + "', skipping: " + e.getMessage());
            }
        }
    }

    public interface MarkFileDissector
    {
        String filename();

        Throwable printErrors(List<Path> paths, ErrorConsumer errorConsumer);

        boolean isRelevantFile(File file);

        void filterErrors(List<Path> paths, MutableInteger count, StringBuilder errors, Predicate<String> logFilter)
            throws IOException;
    }

    private static final class ArchiveMarkFileDissector implements MarkFileDissector
    {
        public String filename()
        {
            return ArchiveMarkFile.FILENAME;
        }

        public boolean isRelevantFile(final File file)
        {
            return ArchiveMarkFile.FILENAME.equals(file.getName());
        }

        public Throwable printErrors(final List<Path> paths, final ErrorConsumer errorConsumer)
        {
            Throwable error = null;
            for (final Path path : paths)
            {
                if (Files.exists(path) && path.toFile().length() > 0)
                {
                    try (ArchiveMarkFile archiveFile = openArchiveMarkFile(path))
                    {
                        final AtomicBuffer buffer = archiveFile.errorBuffer();

                        System.out.printf("%n%n%s file %s%n", "Archive Errors", path);
                        final int distinctErrorCount = ErrorLogReader.read(buffer, errorConsumer);
                        System.out.format("%d distinct errors observed.%n", distinctErrorCount);
                    }
                    catch (final Throwable t)
                    {
                        error = Tests.setOrUpdateError(error, t);
                    }
                }
            }

            return error;
        }

        public void filterErrors(
            final List<Path> paths,
            final MutableInteger count,
            final StringBuilder errors,
            final Predicate<String> logFilter) throws IOException
        {
            for (final Path path : paths)
            {
                if (Files.exists(path) && Files.size(path) > 0)
                {
                    try (ArchiveMarkFile archive = openArchiveMarkFile(path))
                    {
                        final AtomicBuffer buffer = archive.errorBuffer();
                        readErrors(path, buffer, count, errors, logFilter);
                    }
                }
            }
        }
    }

    private abstract static class ClusterMarkFileDissector implements MarkFileDissector
    {
        public Throwable printErrors(final List<Path> paths, final ErrorConsumer errorConsumer)
        {
            Throwable error = null;
            for (final Path path : paths)
            {
                if (Files.exists(path) && path.toFile().length() > 0)
                {
                    try (ClusterMarkFile clusterMarkFile = openClusterMarkFile(path))
                    {
                        final AtomicBuffer buffer = clusterMarkFile.errorBuffer();

                        System.out.printf("%n%n%s file %s%n", fileDescription(), path);
                        final int distinctErrorCount = ErrorLogReader.read(buffer, errorConsumer);
                        System.out.format("%d distinct errors observed.%n", distinctErrorCount);
                    }
                    catch (final Throwable t)
                    {
                        error = Tests.setOrUpdateError(error, t);
                    }
                }
            }
            return error;
        }

        public void filterErrors(
            final List<Path> paths,
            final MutableInteger count,
            final StringBuilder errors,
            final Predicate<String> logFilter) throws IOException
        {
            for (final Path path : paths)
            {
                if (Files.exists(path) && Files.size(path) > 0)
                {
                    try (ClusterMarkFile clusterMarkFile = openClusterMarkFile(path))
                    {
                        final AtomicBuffer buffer = clusterMarkFile.errorBuffer();
                        readErrors(path, buffer, count, errors, logFilter);
                    }
                }
            }
        }

        protected abstract String fileDescription();
    }

    private static final class ConsensusModuleMarkFileDissector extends ClusterMarkFileDissector
    {
        public String filename()
        {
            return ClusterMarkFile.FILENAME;
        }

        public boolean isRelevantFile(final File file)
        {
            return ClusterMarkFile.FILENAME.equals(file.getName());
        }

        protected String fileDescription()
        {
            return "Consensus Module";
        }
    }

    private static final class ClusteredServiceMarkFileDissector extends ClusterMarkFileDissector
    {
        public String filename()
        {
            return ClusterMarkFile.SERVICE_FILENAME_PREFIX + "X" + ClusterMarkFile.FILE_EXTENSION;
        }

        public boolean isRelevantFile(final File file)
        {
            final String name = file.getName();
            return name.startsWith(ClusterMarkFile.SERVICE_FILENAME_PREFIX) &&
                name.endsWith(ClusterMarkFile.FILE_EXTENSION);
        }

        protected String fileDescription()
        {
            return "Clustered Service";
        }
    }
}
