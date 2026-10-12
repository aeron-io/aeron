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
import io.aeron.CommonContext;
import io.aeron.Counter;
import io.aeron.Image;
import io.aeron.RethrowingErrorHandler;
import io.aeron.archive.checksum.Checksum;
import io.aeron.archive.checksum.Checksums;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.config.Config;
import io.aeron.config.DefaultType;
import io.aeron.driver.DutyCycleTracker;
import io.aeron.driver.status.DutyCycleStallTracker;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.security.Authenticator;
import io.aeron.security.AuthenticatorSupplier;
import io.aeron.security.AuthorisationService;
import io.aeron.security.AuthorisationServiceSupplier;
import io.aeron.version.Versioned;
import org.agrona.AsciiEncoding;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.MarkFile;
import org.agrona.Strings;
import org.agrona.SystemUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.StatusIndicator;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.channels.FileChannel;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.AeronCounters.ARCHIVE_CONTROL_SESSIONS_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_ERROR_COUNT_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID;
import static io.aeron.AeronCounters.validateCounterTypeId;
import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.CommonContext.fallbackLogger;
import static io.aeron.PropertiesUtil.getDurationInNanos;
import static io.aeron.PropertiesUtil.getInteger;
import static io.aeron.PropertiesUtil.getSizeAsInt;
import static io.aeron.PropertiesUtil.getSizeAsLong;
import static io.aeron.archive.Archive.Configuration.ERROR_BUFFER_LENGTH_DEFAULT;
import static io.aeron.archive.Archive.Configuration.SESSION_LIVENESS_CHECK_INTERVAL_DEFAULT_NS;
import static io.aeron.archive.Archive.Configuration.SESSION_LIVENESS_CHECK_INTERVAL_PROP_NAME;
import static io.aeron.archive.ArchiveThreadingMode.DEDICATED;
import static io.aeron.exceptions.AeronException.Category.ERROR;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MAX_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.checkTermLength;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BitUtil.isPowerOfTwo;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.agrona.SystemUtil.loadPropertiesFiles;

/**
 * The Aeron Archive which allows for the recording and replay of local and remote {@link io.aeron.Publication}s.
 */
@Versioned
public final class Archive implements AutoCloseable
{
    static final String AERON_ARCHIVE_RECORDER_THREAD_NAME = "aeron-ar-rec";
    static final String AERON_ARCHIVE_REPLAYER_THREAD_NAME = "aeron-ar-rep";
    static final String AERON_ARCHIVE_CONDUCTOR_THREAD_NAME = "aeron-ar-cnd";
    static final String AERON_ARCHIVE_SHARED_THREAD_NAME = "aeron-ar-shd";

    static final String AERON_ARCHIVE_RECORDER_THREAD_NAME_CLASSIC = "archive-recorder";
    static final String AERON_ARCHIVE_REPLAYER_THREAD_NAME_CLASSIC = "archive-replayer";
    static final String AERON_ARCHIVE_CONDUCTOR_THREAD_NAME_CLASSIC = "archive-conductor";
    static final String AERON_ARCHIVE_SHARED_THREAD_NAME_CLASSIC = "archive-conductor";


    private final Context ctx;
    private final AgentRunner conductorRunner;
    private final AgentInvoker conductorInvoker;

    Archive(final Context ctx)
    {
        try
        {
            ctx.conclude();
            this.ctx = ctx;

            final ArchiveConductor conductor = DEDICATED == ctx.threadingMode() ?
                (new DedicatedModeArchiveConductor(ctx)) : (new SharedModeArchiveConductor(ctx));

            if (ArchiveThreadingMode.INVOKER == ctx.threadingMode())
            {
                conductorInvoker = new AgentInvoker(ctx.errorHandler(), ctx.errorCounter(), conductor);
                conductorRunner = null;
            }
            else
            {
                conductorInvoker = null;
                conductorRunner = new AgentRunner(
                    ctx.idleStrategy(), ctx.errorHandler(), ctx.errorCounter(), conductor);
            }
        }
        catch (final ConcurrentConcludeException ex)
        {
            throw ex;
        }
        catch (final Exception ex)
        {
            final ArchiveMarkFile markFile = ctx.markFile;
            if (null != markFile)
            {
                markFile.signalReady(NULL_VALUE);
            }
            CloseHelper.quietClose(ctx::close);
            throw ex;
        }
    }

    /**
     * Launch an {@link Archive} with that communicates with an out of process {@link io.aeron.driver.MediaDriver}
     * and await a shutdown signal.
     *
     * @param args command line argument which is a list for properties files as URLs or filenames.
     */
    @SuppressWarnings("try")
    public static void main(final String[] args)
    {
        loadPropertiesFiles(args);

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
            Archive ignore = launch(new Context().errorHandler(
                (throwable) ->
                {
                    if (throwable instanceof AgentTerminationException)
                    {
                        barrier.signal();
                    }
                    else if (AeronException.isFatal(throwable))
                    {
                        barrier.signal();
                    }
                })))
        {
            barrier.await();
            System.out.println("Shutdown Archive...");
        }
    }

    /**
     * Get the {@link Archive.Context} that is used by this {@link Archive}.
     *
     * @return the {@link Archive.Context} that is used by this {@link Archive}.
     */
    public Context context()
    {
        return ctx;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        CloseHelper.close(conductorInvoker);
        CloseHelper.close(conductorRunner);
    }

    /**
     * Get the {@link AgentInvoker} for the archive if it is running in {@link ArchiveThreadingMode#INVOKER}.
     *
     * @return the {@link AgentInvoker} for the archive if it is running in {@link ArchiveThreadingMode#INVOKER}
     * otherwise null.
     */
    public AgentInvoker invoker()
    {
        return conductorInvoker;
    }

    /**
     * Launch an Archive using a default configuration.
     *
     * @return a new instance of an Archive.
     */
    public static Archive launch()
    {
        return launch(new Context());
    }

    /**
     * Launch an Archive by providing a configuration context.
     *
     * @param ctx for the configuration parameters.
     * @return a new instance of an Archive.
     */
    public static Archive launch(final Context ctx)
    {
        final Archive archive = new Archive(ctx);
        if (ArchiveThreadingMode.INVOKER == ctx.threadingMode())
        {
            archive.conductorInvoker.start();
        }
        else
        {
            AgentRunner.startOnThread(archive.conductorRunner, ctx.threadFactory());
        }

        return archive;
    }

    /**
     * Configuration for system properties and defaults.
     * <p>
     * Details for the individual parameters can be found in the Javadoc for the {@link Context} setters.
     */
    @Config(existsInC = false)
    public static final class Configuration
    {
        private Configuration()
        {
        }

        /**
         * Filename for the single instance of a {@link Catalog} contents for an archive.
         */
        static final String CATALOG_FILE_NAME = "archive.catalog";

        /**
         * Recording segment file suffix extension.
         */
        static final String RECORDING_SEGMENT_SUFFIX = ".rec";

        /**
         * Default block length of data in a single IO operation during a recording or replay.
         */
        @Config
        public static final int FILE_IO_MAX_LENGTH_DEFAULT = 1024 * 1024;

        /**
         * Maximum length of a file IO operation for recording or replay. Must be a power of 2.
         */
        @Config
        public static final String FILE_IO_MAX_LENGTH_PROP_NAME = "aeron.archive.file.io.max.length";

        /**
         * Directory in which the archive stores it files such as the catalog and recordings.
         */
        @Config
        public static final String ARCHIVE_DIR_PROP_NAME = "aeron.archive.dir";

        /**
         * Default directory for the archive files.
         *
         * @see #ARCHIVE_DIR_PROP_NAME
         */
        @Config
        public static final String ARCHIVE_DIR_DEFAULT = "aeron-archive";

        /**
         * Alternative directory to store mark file (i.e. {@code archive-mark.dat}).
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String MARK_FILE_DIR_PROP_NAME = "aeron.archive.mark.file.dir";

        /**
         * Recordings will be segmented on disk in files limited to the segment length which must be a multiple of
         * the term length for each stream. For lots of small recording this value may be reduced.
         */
        @Config
        public static final String SEGMENT_FILE_LENGTH_PROP_NAME = "aeron.archive.segment.file.length";

        /**
         * Default segment file length which is multiple of terms.
         *
         * @see #SEGMENT_FILE_LENGTH_PROP_NAME
         */
        @Config
        public static final int SEGMENT_FILE_LENGTH_DEFAULT = 128 * 1024 * 1024;

        /**
         * Threshold below which the archive will reject new recording requests.
         */
        @Config
        public static final String LOW_STORAGE_SPACE_THRESHOLD_PROP_NAME = "aeron.archive.low.storage.space.threshold";

        /**
         * Default threshold below which the archive will reject new recording requests.
         *
         * @see #LOW_STORAGE_SPACE_THRESHOLD_PROP_NAME
         */
        @Config
        public static final int LOW_STORAGE_SPACE_THRESHOLD_DEFAULT = SEGMENT_FILE_LENGTH_DEFAULT;

        /**
         * The level at which recording files should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         */
        @Config
        public static final String FILE_SYNC_LEVEL_PROP_NAME = "aeron.archive.file.sync.level";

        /**
         * Default is to use normal file writes which may mean some data loss in the event of a power failure.
         *
         * @see #FILE_SYNC_LEVEL_PROP_NAME
         */
        @Config
        public static final int FILE_SYNC_LEVEL_DEFAULT = 0;

        /**
         * The level at which catalog updates and directory should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         */
        @Config
        public static final String CATALOG_FILE_SYNC_LEVEL_PROP_NAME = "aeron.archive.catalog.file.sync.level";

        /**
         * Default is to use normal file writes which may mean some data loss in the event of a power failure.
         *
         * @see #CATALOG_FILE_SYNC_LEVEL_PROP_NAME
         */
        @Config
        public static final int CATALOG_FILE_SYNC_LEVEL_DEFAULT = FILE_SYNC_LEVEL_DEFAULT;

        /**
         * What {@link ArchiveThreadingMode} should be used.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "DEDICATED")
        public static final String THREADING_MODE_PROP_NAME = "aeron.archive.threading.mode";

        /**
         * Default {@link IdleStrategy} to be used for the archive {@link Agent}s when not busy.
         */
        @Config
        public static final String ARCHIVE_IDLE_STRATEGY_PROP_NAME = "aeron.archive.idle.strategy";

        /**
         * The {@link IdleStrategy} to be used for the archive recorder {@link Agent} when not busy.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String ARCHIVE_RECORDER_IDLE_STRATEGY_PROP_NAME = "aeron.archive.recorder.idle.strategy";

        /**
         * The {@link IdleStrategy} to be used for the archive replayer {@link Agent} when not busy.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String ARCHIVE_REPLAYER_IDLE_STRATEGY_PROP_NAME = "aeron.archive.replayer.idle.strategy";

        /**
         * Default {@link IdleStrategy} to be used for the archive {@link Agent}s when not busy.
         *
         * @see #ARCHIVE_IDLE_STRATEGY_PROP_NAME
         */
        @Config(id = "ARCHIVE_IDLE_STRATEGY")
        public static final String DEFAULT_IDLE_STRATEGY = "org.agrona.concurrent.BackoffIdleStrategy";

        /**
         * Maximum number of concurrent recordings which can be active at a time. Going beyond this number will
         * result in an exception and further recordings will be rejected. Since wildcard subscriptions can have
         * multiple images, and thus multiple recordings, then the limit may come later. It is best to
         * use session based subscriptions.
         */
        @Config
        public static final String MAX_CONCURRENT_RECORDINGS_PROP_NAME = "aeron.archive.max.concurrent.recordings";

        /**
         * Default maximum number of concurrent recordings. Unless on a very fast SSD and having sufficient memory
         * for the page cache then this number should be kept low, especially when sync'ing writes.
         *
         * @see #MAX_CONCURRENT_RECORDINGS_PROP_NAME
         */
        @Config
        public static final int MAX_CONCURRENT_RECORDINGS_DEFAULT = 20;

        /**
         * Maximum number of concurrent replays. Beyond this maximum an exception will be raised and further replays
         * will be rejected.
         */
        @Config
        public static final String MAX_CONCURRENT_REPLAYS_PROP_NAME = "aeron.archive.max.concurrent.replays";

        /**
         * Default maximum number of concurrent replays. Unless on a fast SSD and having sufficient memory
         * for the page cache then this number should be kept low.
         */
        @Config
        public static final int MAX_CONCURRENT_REPLAYS_DEFAULT = 20;

        /**
         * Maximum number of entries for the archive {@link Catalog}. Increasing this limit will require use of the
         * {@link CatalogTool}. The number of entries can be reduced by extending existing recordings rather than
         * creating new ones.
         *
         * @deprecated Use {@link #CATALOG_CAPACITY_PROP_NAME} instead.
         */
        @Deprecated
        @Config
        public static final String MAX_CATALOG_ENTRIES_PROP_NAME = "aeron.archive.max.catalog.entries";

        /**
         * Default limit for the entries in the {@link Catalog}.
         *
         * @see #MAX_CATALOG_ENTRIES_PROP_NAME
         */
        @Deprecated
        @Config
        public static final long MAX_CATALOG_ENTRIES_DEFAULT = 8 * 1024;

        /**
         * Default capacity in bytes of the archive {@link Catalog}. {@link Catalog} will resize itself when this
         * limit is reached.
         */
        @Config
        public static final String CATALOG_CAPACITY_PROP_NAME = "aeron.archive.catalog.capacity";

        /**
         * Default capacity in bytes for the {@link Catalog}.
         *
         * @see #CATALOG_CAPACITY_PROP_NAME
         */
        @Config
        public static final long CATALOG_CAPACITY_DEFAULT = Catalog.DEFAULT_CAPACITY;

        /**
         * Timeout for making a connection back to a client for a control session or replay.
         */
        @Config
        public static final String CONNECT_TIMEOUT_PROP_NAME = "aeron.archive.connect.timeout";

        /**
         * Default timeout for connecting back to a client for a control session or replay. You may want to
         * increase this on higher latency networks.
         *
         * @see #CONNECT_TIMEOUT_PROP_NAME
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 5L * 1000 * 1000 * 1000)
        public static final long CONNECT_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

        /**
         * Time interval in nanoseconds for checking session liveness checks.
         *
         * @since 1.47.0
         */
        @Config
        public static final String SESSION_LIVENESS_CHECK_INTERVAL_PROP_NAME =
            "aeron.archive.session.liveness.check.interval";

        /**
         * Default time interval in nanoseconds for checking session liveness.
         *
         * @see #SESSION_LIVENESS_CHECK_INTERVAL_PROP_NAME
         * @since 1.47.0
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 1000L * 1000 * 1000)
        public static final long SESSION_LIVENESS_CHECK_INTERVAL_DEFAULT_NS = TimeUnit.SECONDS.toNanos(1);

        /**
         * How long a replay publication should linger after all data is sent. Longer linger can help avoid tail loss.
         */
        @Config
        public static final String REPLAY_LINGER_TIMEOUT_PROP_NAME = "aeron.archive.replay.linger.timeout";

        /**
         * Default for long to linger a replay connection which defaults to
         * {@link io.aeron.driver.Configuration#publicationLingerTimeoutNs()}.
         *
         * @see #REPLAY_LINGER_TIMEOUT_PROP_NAME
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 5L * 1000 * 1000 * 1000)
        public static final long REPLAY_LINGER_TIMEOUT_DEFAULT_NS =
            io.aeron.driver.Configuration.publicationLingerTimeoutNs();

        /**
         * Property name for threshold value for the conductor work cycle threshold to track for being exceeded.
         */
        @Config
        public static final String CONDUCTOR_CYCLE_THRESHOLD_PROP_NAME = "aeron.archive.conductor.cycle.threshold";

        /**
         * Default threshold value for the conductor work cycle threshold to track for being exceeded.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 100_000_000L)
        public static final long CONDUCTOR_CYCLE_THRESHOLD_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(100);

        /**
         * Property name for threshold value for the recorder work cycle threshold to track for being exceeded.
         */
        @Config
        public static final String RECORDER_CYCLE_THRESHOLD_PROP_NAME = "aeron.archive.recorder.cycle.threshold";

        /**
         * Default threshold value for the recorder work cycle threshold to track for being exceeded.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 100_000_000L)
        public static final long RECORDER_CYCLE_THRESHOLD_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(100);

        /**
         * Property name for threshold value for the replayer work cycle threshold to track for being exceeded.
         */
        @Config
        public static final String REPLAYER_CYCLE_THRESHOLD_PROP_NAME = "aeron.archive.replayer.cycle.threshold";

        /**
         * Default threshold value for the replayer work cycle threshold to track for being exceeded.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 100_000_000L)
        public static final long REPLAYER_CYCLE_THRESHOLD_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(100);

        /**
         * Should the archive delete existing files on start. Default is false and should only be true for testing.
         */
        @Config(defaultType = DefaultType.BOOLEAN, defaultBoolean = false)
        public static final String ARCHIVE_DIR_DELETE_ON_START_PROP_NAME = "aeron.archive.dir.delete.on.start";

        /**
         * Channel for receiving replication streams replayed from another archive.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String REPLICATION_CHANNEL_PROP_NAME = "aeron.archive.replication.channel";

        /**
         * Name of the system property for specifying a supplier of {@link Authenticator} for the archive.
         */
        @Config
        public static final String AUTHENTICATOR_SUPPLIER_PROP_NAME = "aeron.archive.authenticator.supplier";

        /**
         * Name of the class to use as a supplier of {@link Authenticator} for the archive. Default is
         * a non-authenticating option.
         */
        @Config
        public static final String AUTHENTICATOR_SUPPLIER_DEFAULT = "io.aeron.security.DefaultAuthenticatorSupplier";

        /**
         * Name of the system property for specifying a supplier of {@link AuthorisationService} for the archive.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME =
            "aeron.archive.authorisation.service.supplier";

        /**
         * Default {@link AuthorisationServiceSupplier} that returns {@link AuthorisationService} that allows any
         * command to be executed (i.e. {@link AuthorisationService#ALLOW_ALL}).
         */
        public static final AuthorisationServiceSupplier DEFAULT_AUTHORISATION_SERVICE_SUPPLIER =
            () -> AuthorisationService.ALLOW_ALL;

        /**
         * The type id of the {@link Counter} used for keeping track of the number of errors that have occurred.
         */
        public static final int ARCHIVE_ERROR_COUNT_TYPE_ID = AeronCounters.ARCHIVE_ERROR_COUNT_TYPE_ID;

        /**
         * The type id of the {@link Counter} used for keeping track of the count of concurrent control sessions.
         */
        public static final int ARCHIVE_CONTROL_SESSIONS_TYPE_ID = AeronCounters.ARCHIVE_CONTROL_SESSIONS_TYPE_ID;

        /**
         * Size in bytes of the error buffer for the archive when not externally provided.
         */
        @Config
        public static final String ERROR_BUFFER_LENGTH_PROP_NAME = "aeron.archive.error.buffer.length";

        /**
         * Size in bytes of the error buffer for the archive when not eternally provided.
         */
        @Config
        public static final int ERROR_BUFFER_LENGTH_DEFAULT = 1024 * 1024;

        /**
         * Property that specifies fully qualified class name of the {@link io.aeron.archive.checksum.Checksum}
         * to be used for checksum computation during recording.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String RECORD_CHECKSUM_PROP_NAME = "aeron.archive.record.checksum";

        /**
         * Property that specifies fully qualified class name of the {@link io.aeron.archive.checksum.Checksum}
         * to be used for checksum validation during replay.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String REPLAY_CHECKSUM_PROP_NAME = "aeron.archive.replay.checksum";

        /**
         * Property name for the identity of the Archive instance. If not specified defaults to the
         * {@link Aeron#clientId()} of the assigned {@link Aeron} instance.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = -1)
        public static final String ARCHIVE_ID_PROP_NAME = "aeron.archive.id";

        /**
         * Is network (UDP) control channel enabled. Defaults to {@code true}.
         * <p>
         * If set to anything other than {@code true} then control channel is disabled, i.e. the Archive will run in
         * IPC-only mode.
         */
        @Config(defaultType = DefaultType.BOOLEAN, defaultBoolean = true)
        public static final String CONTROL_CHANNEL_ENABLED_PROP_NAME = "aeron.archive.control.channel.enabled";

        /**
         * Update interval in ms for archive mark file.
         */
        static final long MARK_FILE_UPDATE_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);

        /**
         * Timeout in milliseconds for detecting if there is an active Archive instance.
         */
        static final long LIVENESS_TIMEOUT_MS = 10 * MARK_FILE_UPDATE_INTERVAL_MS;

        /**
         * Get the directory name to be used for storing the archive.
         *
         * @return the directory name to be used for storing the archive.
         */
        public static String archiveDirName()
        {
            return archiveDirName(System.getProperties());
        }

        /**
         * Get the directory name to be used for storing the archive.
         *
         * @param properties to read the configuration from.
         * @return the directory name to be used for storing the archive.
         */
        public static String archiveDirName(final Properties properties)
        {
            return properties.getProperty(ARCHIVE_DIR_PROP_NAME, ARCHIVE_DIR_DEFAULT);
        }

        /**
         * Get the alternative directory to be used for storing the archive mark file.
         *
         * @return the directory to be used for storing the archive mark file.
         */
        public static String markFileDir()
        {
            return markFileDir(System.getProperties());
        }

        /**
         * Get the alternative directory to be used for storing the archive mark file.
         *
         * @param properties to read the configuration from.
         * @return the directory to be used for storing the archive mark file.
         */
        public static String markFileDir(final Properties properties)
        {
            return properties.getProperty(MARK_FILE_DIR_PROP_NAME);
        }

        /**
         * The maximum length of a file IO operation.
         *
         * @return the maximum length of a file IO operation.
         */
        public static int fileIoMaxLength()
        {
            return fileIoMaxLength(System.getProperties());
        }

        /**
         * The maximum length of a file IO operation.
         *
         * @param properties to read the configuration from.
         * @return the maximum length of a file IO operation.
         */
        public static int fileIoMaxLength(final Properties properties)
        {
            return getSizeAsInt(properties, FILE_IO_MAX_LENGTH_PROP_NAME, FILE_IO_MAX_LENGTH_DEFAULT);
        }

        /**
         * The length of file to be used for storing recording segments that must be a power of 2.
         * <p>
         * If the {@link Image#termBufferLength()} is greater than this will take priority.
         *
         * @return length of file to be used for storing recording segments.
         */
        public static int segmentFileLength()
        {
            return segmentFileLength(System.getProperties());
        }

        /**
         * The length of file to be used for storing recording segments that must be a power of 2.
         * <p>
         * If the {@link Image#termBufferLength()} is greater than this will take priority.
         *
         * @param properties to read the configuration from.
         * @return length of file to be used for storing recording segments.
         */
        public static int segmentFileLength(final Properties properties)
        {
            return getSizeAsInt(properties, SEGMENT_FILE_LENGTH_PROP_NAME, SEGMENT_FILE_LENGTH_DEFAULT);
        }

        /**
         * The low storage space threshold beyond which the archive will reject new requests to record streams.
         *
         * @return threshold beyond which the archive will reject new requests to record streams.
         */
        public static long lowStorageSpaceThreshold()
        {
            return lowStorageSpaceThreshold(System.getProperties());
        }

        /**
         * The low storage space threshold beyond which the archive will reject new requests to record streams.
         *
         * @param properties to read the configuration from.
         * @return threshold beyond which the archive will reject new requests to record streams.
         */
        public static long lowStorageSpaceThreshold(final Properties properties)
        {
            return getSizeAsLong(
                properties, LOW_STORAGE_SPACE_THRESHOLD_PROP_NAME, LOW_STORAGE_SPACE_THRESHOLD_DEFAULT);
        }

        /**
         * The level at which files should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         *
         * @return level at which files should be sync'ed to disk.
         * @see #FILE_SYNC_LEVEL_PROP_NAME
         */
        public static int fileSyncLevel()
        {
            return fileSyncLevel(System.getProperties());
        }

        /**
         * The level at which files should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         *
         * @param properties to read the configuration from.
         * @return level at which files should be sync'ed to disk.
         * @see #FILE_SYNC_LEVEL_PROP_NAME
         */
        public static int fileSyncLevel(final Properties properties)
        {
            return getInteger(properties, FILE_SYNC_LEVEL_PROP_NAME, FILE_SYNC_LEVEL_DEFAULT);
        }

        /**
         * The level at which the catalog file and directory should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         *
         * @return level at which files should be sync'ed to disk.
         * @see #CATALOG_FILE_SYNC_LEVEL_PROP_NAME
         */
        public static int catalogFileSyncLevel()
        {
            return catalogFileSyncLevel(System.getProperties());
        }

        /**
         * The level at which the catalog file and directory should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         *
         * @param properties to read the configuration from.
         * @return level at which files should be sync'ed to disk.
         * @see #CATALOG_FILE_SYNC_LEVEL_PROP_NAME
         */
        public static int catalogFileSyncLevel(final Properties properties)
        {
            return getInteger(properties, CATALOG_FILE_SYNC_LEVEL_PROP_NAME, CATALOG_FILE_SYNC_LEVEL_DEFAULT);
        }

        /**
         * The threading mode to be employed by the archive.
         *
         * @return the threading mode to be employed by the archive.
         */
        public static ArchiveThreadingMode threadingMode()
        {
            return threadingMode(System.getProperties());
        }

        /**
         * The threading mode to be employed by the archive.
         *
         * @param properties to read the configuration from.
         * @return the threading mode to be employed by the archive.
         */
        public static ArchiveThreadingMode threadingMode(final Properties properties)
        {
            return ArchiveThreadingMode.valueOf(properties.getProperty(THREADING_MODE_PROP_NAME, DEDICATED.name()));
        }

        /**
         * Create a supplier of {@link IdleStrategy}s for the {@link #ARCHIVE_IDLE_STRATEGY_PROP_NAME}
         * system property.
         *
         * @param controllableStatus if a {@link org.agrona.concurrent.ControllableIdleStrategy} is required.
         * @return the new idle strategy {@link Supplier}.
         */
        public static Supplier<IdleStrategy> idleStrategySupplier(final StatusIndicator controllableStatus)
        {
            return idleStrategySupplier(System.getProperties(), controllableStatus);
        }

        /**
         * Create a supplier of {@link IdleStrategy}s for the {@link #ARCHIVE_IDLE_STRATEGY_PROP_NAME} property.
         *
         * @param properties         to read the configuration from.
         * @param controllableStatus if a {@link org.agrona.concurrent.ControllableIdleStrategy} is required.
         * @return the new idle strategy {@link Supplier}.
         */
        public static Supplier<IdleStrategy> idleStrategySupplier(
            final Properties properties, final StatusIndicator controllableStatus)
        {
            return () ->
            {
                final String name = properties.getProperty(ARCHIVE_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);
                return io.aeron.driver.Configuration.agentIdleStrategy(name, controllableStatus);
            };
        }

        /**
         * Create a supplier of {@link IdleStrategy}s for the {@link #ARCHIVE_RECORDER_IDLE_STRATEGY_PROP_NAME}
         * system property.
         *
         * @param controllableStatus if a {@link org.agrona.concurrent.ControllableIdleStrategy} is required.
         * @return the new idle strategy {@link Supplier}.
         */
        public static Supplier<IdleStrategy> recorderIdleStrategySupplier(final StatusIndicator controllableStatus)
        {
            return recorderIdleStrategySupplier(System.getProperties(), controllableStatus);
        }

        /**
         * Create a supplier of {@link IdleStrategy}s for the {@link #ARCHIVE_RECORDER_IDLE_STRATEGY_PROP_NAME}
         * property.
         *
         * @param properties         to read the configuration from.
         * @param controllableStatus if a {@link org.agrona.concurrent.ControllableIdleStrategy} is required.
         * @return the new idle strategy {@link Supplier}.
         */
        public static Supplier<IdleStrategy> recorderIdleStrategySupplier(
            final Properties properties, final StatusIndicator controllableStatus)
        {
            final String name = properties.getProperty(ARCHIVE_RECORDER_IDLE_STRATEGY_PROP_NAME);
            if (null == name)
            {
                return null;
            }

            return () -> io.aeron.driver.Configuration.agentIdleStrategy(name, controllableStatus);
        }

        /**
         * Create a supplier of {@link IdleStrategy}s for the {@link #ARCHIVE_REPLAYER_IDLE_STRATEGY_PROP_NAME}
         * system property.
         *
         * @param controllableStatus if a {@link org.agrona.concurrent.ControllableIdleStrategy} is required.
         * @return the new idle strategy {@link Supplier}.
         */
        public static Supplier<IdleStrategy> replayerIdleStrategySupplier(final StatusIndicator controllableStatus)
        {
            return replayerIdleStrategySupplier(System.getProperties(), controllableStatus);
        }

        /**
         * Create a supplier of {@link IdleStrategy}s for the {@link #ARCHIVE_REPLAYER_IDLE_STRATEGY_PROP_NAME}
         * property.
         *
         * @param properties         to read the configuration from.
         * @param controllableStatus if a {@link org.agrona.concurrent.ControllableIdleStrategy} is required.
         * @return the new idle strategy {@link Supplier}.
         */
        public static Supplier<IdleStrategy> replayerIdleStrategySupplier(
            final Properties properties, final StatusIndicator controllableStatus)
        {
            final String name = properties.getProperty(ARCHIVE_REPLAYER_IDLE_STRATEGY_PROP_NAME);
            if (null == name)
            {
                return null;
            }

            return () -> io.aeron.driver.Configuration.agentIdleStrategy(name, controllableStatus);
        }

        /**
         * The maximum number of recordings that can operate concurrently after which new requests will be rejected.
         *
         * @return the maximum number of recordings that can operate concurrently.
         */
        public static int maxConcurrentRecordings()
        {
            return maxConcurrentRecordings(System.getProperties());
        }

        /**
         * The maximum number of recordings that can operate concurrently after which new requests will be rejected.
         *
         * @param properties to read the configuration from.
         * @return the maximum number of recordings that can operate concurrently.
         */
        public static int maxConcurrentRecordings(final Properties properties)
        {
            return getInteger(properties, MAX_CONCURRENT_RECORDINGS_PROP_NAME, MAX_CONCURRENT_RECORDINGS_DEFAULT);
        }

        /**
         * The maximum number of replays that can operate concurrently after which new requests will be rejected.
         *
         * @return the maximum number of replays that can operate concurrently.
         */
        public static int maxConcurrentReplays()
        {
            return maxConcurrentReplays(System.getProperties());
        }

        /**
         * The maximum number of replays that can operate concurrently after which new requests will be rejected.
         *
         * @param properties to read the configuration from.
         * @return the maximum number of replays that can operate concurrently.
         */
        public static int maxConcurrentReplays(final Properties properties)
        {
            return getInteger(properties, MAX_CONCURRENT_REPLAYS_PROP_NAME, MAX_CONCURRENT_REPLAYS_DEFAULT);
        }

        /**
         * Maximum number of catalog entries to allocate for the catalog file.
         *
         * @return the maximum number of catalog entries to support for the catalog file.
         * @see #catalogCapacity()
         * @deprecated Use {@link #catalogCapacity()} instead.
         */
        @Deprecated
        public static long maxCatalogEntries()
        {
            return maxCatalogEntries(System.getProperties());
        }

        /**
         * Maximum number of catalog entries to allocate for the catalog file.
         *
         * @param properties to read the configuration from.
         * @return the maximum number of catalog entries to support for the catalog file.
         * @see #catalogCapacity()
         * @deprecated Use {@link #catalogCapacity()} instead.
         */
        @Deprecated
        public static long maxCatalogEntries(final Properties properties)
        {
            return getSizeAsLong(properties, MAX_CATALOG_ENTRIES_PROP_NAME, MAX_CATALOG_ENTRIES_DEFAULT);
        }

        /**
         * Default capacity (size) in bytes for the catalog file.
         *
         * @return default size of the catalog file in bytes.
         */
        public static long catalogCapacity()
        {
            return catalogCapacity(System.getProperties());
        }

        /**
         * Default capacity (size) in bytes for the catalog file.
         *
         * @param properties to read the configuration from.
         * @return default size of the catalog file in bytes.
         */
        public static long catalogCapacity(final Properties properties)
        {
            return getSizeAsLong(properties, CATALOG_CAPACITY_PROP_NAME, CATALOG_CAPACITY_DEFAULT);
        }

        /**
         * The timeout in nanoseconds to wait for a connection.
         *
         * @return timeout in nanoseconds to wait for a connection.
         * @see #CONNECT_TIMEOUT_PROP_NAME
         */
        public static long connectTimeoutNs()
        {
            return connectTimeoutNs(System.getProperties());
        }

        /**
         * The timeout in nanoseconds to wait for a connection.
         *
         * @param properties to read the configuration from.
         * @return timeout in nanoseconds to wait for a connection.
         * @see #CONNECT_TIMEOUT_PROP_NAME
         */
        public static long connectTimeoutNs(final Properties properties)
        {
            return getDurationInNanos(properties, CONNECT_TIMEOUT_PROP_NAME, CONNECT_TIMEOUT_DEFAULT_NS);
        }

        /**
         * The timeout in nanoseconds to for a replay network publication to linger after draining.
         *
         * @return timeout in nanoseconds for a replay network publication to wait in linger.
         * @see #REPLAY_LINGER_TIMEOUT_PROP_NAME
         * @see io.aeron.driver.Configuration#PUBLICATION_LINGER_PROP_NAME
         */
        public static long replayLingerTimeoutNs()
        {
            return replayLingerTimeoutNs(System.getProperties());
        }

        /**
         * The timeout in nanoseconds to for a replay network publication to linger after draining.
         *
         * @param properties to read the configuration from.
         * @return timeout in nanoseconds for a replay network publication to wait in linger.
         * @see #REPLAY_LINGER_TIMEOUT_PROP_NAME
         * @see io.aeron.driver.Configuration#PUBLICATION_LINGER_PROP_NAME
         */
        public static long replayLingerTimeoutNs(final Properties properties)
        {
            return getDurationInNanos(properties, REPLAY_LINGER_TIMEOUT_PROP_NAME, REPLAY_LINGER_TIMEOUT_DEFAULT_NS);
        }

        /**
         * Get threshold value for the conductor work cycle threshold to track for being exceeded.
         *
         * @return threshold value in nanoseconds.
         */
        public static long conductorCycleThresholdNs()
        {
            return conductorCycleThresholdNs(System.getProperties());
        }

        /**
         * Get threshold value for the conductor work cycle threshold to track for being exceeded.
         *
         * @param properties to read the configuration from.
         * @return threshold value in nanoseconds.
         */
        public static long conductorCycleThresholdNs(final Properties properties)
        {
            return getDurationInNanos(
                properties, CONDUCTOR_CYCLE_THRESHOLD_PROP_NAME, CONDUCTOR_CYCLE_THRESHOLD_DEFAULT_NS);
        }

        /**
         * Get threshold value for the recorder work cycle threshold to track for being exceeded.
         *
         * @return threshold value in nanoseconds.
         */
        public static long recorderCycleThresholdNs()
        {
            return recorderCycleThresholdNs(System.getProperties());
        }

        /**
         * Get threshold value for the recorder work cycle threshold to track for being exceeded.
         *
         * @param properties to read the configuration from.
         * @return threshold value in nanoseconds.
         */
        public static long recorderCycleThresholdNs(final Properties properties)
        {
            return getDurationInNanos(
                properties, RECORDER_CYCLE_THRESHOLD_PROP_NAME, RECORDER_CYCLE_THRESHOLD_DEFAULT_NS);
        }

        /**
         * Get threshold value for the replayer work cycle threshold to track for being exceeded.
         *
         * @return threshold value in nanoseconds.
         */
        public static long replayerCycleThresholdNs()
        {
            return replayerCycleThresholdNs(System.getProperties());
        }

        /**
         * Get threshold value for the replayer work cycle threshold to track for being exceeded.
         *
         * @param properties to read the configuration from.
         * @return threshold value in nanoseconds.
         */
        public static long replayerCycleThresholdNs(final Properties properties)
        {
            return getDurationInNanos(
                properties, REPLAYER_CYCLE_THRESHOLD_PROP_NAME, REPLAYER_CYCLE_THRESHOLD_DEFAULT_NS);
        }

        /**
         * Whether to delete directory on start or not.
         *
         * @return whether to delete directory on start or not.
         * @see #ARCHIVE_DIR_DELETE_ON_START_PROP_NAME
         */
        public static boolean deleteArchiveOnStart()
        {
            return deleteArchiveOnStart(System.getProperties());
        }

        /**
         * Whether to delete directory on start or not.
         *
         * @param properties to read the configuration from.
         * @return whether to delete directory on start or not.
         * @see #ARCHIVE_DIR_DELETE_ON_START_PROP_NAME
         */
        public static boolean deleteArchiveOnStart(final Properties properties)
        {
            return "true".equals(properties.getProperty(ARCHIVE_DIR_DELETE_ON_START_PROP_NAME, "false"));
        }

        /**
         * The system property {@link #REPLICATION_CHANNEL_PROP_NAME} if set, null otherwise.
         *
         * @return system property {@link #REPLICATION_CHANNEL_PROP_NAME} if set.
         */
        public static String replicationChannel()
        {
            return replicationChannel(System.getProperties());
        }

        /**
         * The system property {@link #REPLICATION_CHANNEL_PROP_NAME} if set, null otherwise.
         *
         * @param properties to read the configuration from.
         * @return system property {@link #REPLICATION_CHANNEL_PROP_NAME} if set.
         */
        public static String replicationChannel(final Properties properties)
        {
            return properties.getProperty(REPLICATION_CHANNEL_PROP_NAME);
        }

        /**
         * Size in bytes of the error buffer in the mark file.
         *
         * @return length of error buffer in bytes.
         * @see #ERROR_BUFFER_LENGTH_PROP_NAME
         */
        public static int errorBufferLength()
        {
            return errorBufferLength(System.getProperties());
        }

        /**
         * Size in bytes of the error buffer in the mark file.
         *
         * @param properties to read the configuration from.
         * @return length of error buffer in bytes.
         * @see #ERROR_BUFFER_LENGTH_PROP_NAME
         */
        public static int errorBufferLength(final Properties properties)
        {
            return getSizeAsInt(properties, ERROR_BUFFER_LENGTH_PROP_NAME, ERROR_BUFFER_LENGTH_DEFAULT);
        }

        /**
         * The value {@link #AUTHENTICATOR_SUPPLIER_DEFAULT} or system property
         * {@link #AUTHENTICATOR_SUPPLIER_PROP_NAME} if set.
         *
         * @return {@link #AUTHENTICATOR_SUPPLIER_DEFAULT} or system property
         * {@link #AUTHENTICATOR_SUPPLIER_PROP_NAME} if set.
         */
        public static AuthenticatorSupplier authenticatorSupplier()
        {
            return authenticatorSupplier(System.getProperties());
        }

        /**
         * The value {@link #AUTHENTICATOR_SUPPLIER_DEFAULT} or system property
         * {@link #AUTHENTICATOR_SUPPLIER_PROP_NAME} if set.
         *
         * @param properties to read the configuration from.
         * @return {@link #AUTHENTICATOR_SUPPLIER_DEFAULT} or system property
         * {@link #AUTHENTICATOR_SUPPLIER_PROP_NAME} if set.
         */
        public static AuthenticatorSupplier authenticatorSupplier(final Properties properties)
        {
            final String supplierClassName = properties.getProperty(
                AUTHENTICATOR_SUPPLIER_PROP_NAME, AUTHENTICATOR_SUPPLIER_DEFAULT);

            AuthenticatorSupplier supplier = null;
            try
            {
                supplier = (AuthenticatorSupplier)Class.forName(supplierClassName).getConstructor().newInstance();
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            return supplier;
        }

        /**
         * The {@link AuthorisationServiceSupplier} specified in the
         * {@link #AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME} system property or the
         * {@link #DEFAULT_AUTHORISATION_SERVICE_SUPPLIER}.
         *
         * @return system property {@link #AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME} if set or
         * {@link #DEFAULT_AUTHORISATION_SERVICE_SUPPLIER} otherwise.
         */
        public static AuthorisationServiceSupplier authorisationServiceSupplier()
        {
            return authorisationServiceSupplier(System.getProperties());
        }

        /**
         * The {@link AuthorisationServiceSupplier} specified in the
         * {@link #AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME} system property or the
         * {@link #DEFAULT_AUTHORISATION_SERVICE_SUPPLIER}.
         *
         * @param properties to read the configuration from.
         * @return system property {@link #AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME} if set or
         * {@link #DEFAULT_AUTHORISATION_SERVICE_SUPPLIER} otherwise.
         */
        public static AuthorisationServiceSupplier authorisationServiceSupplier(final Properties properties)
        {
            final String supplierClassName = properties.getProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME);
            if (Strings.isEmpty(supplierClassName))
            {
                return DEFAULT_AUTHORISATION_SERVICE_SUPPLIER;
            }
            else if (AuthorisationService.DENY_ALL_NAME.equals(supplierClassName))
            {
                return () -> AuthorisationService.DENY_ALL;
            }
            else if (AuthorisationService.ALLOW_ALL_NAME.equals(supplierClassName))
            {
                fallbackLogger(properties).println(
                    "Warning: Cluster authorisation service set to allow all requests");
                return () -> AuthorisationService.ALLOW_ALL;
            }

            try
            {
                return (AuthorisationServiceSupplier)Class.forName(supplierClassName).getConstructor().newInstance();
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
                return null;
            }
        }

        /**
         * Fully qualified class name of the {@link io.aeron.archive.checksum.Checksum} implementation to use during
         * recording to compute checksums. Non-empty value means that checksum is enabled for recording.
         *
         * @return class that implements {@link io.aeron.archive.checksum.Checksum} interface
         * @see Configuration#RECORD_CHECKSUM_PROP_NAME
         */
        public static String recordChecksum()
        {
            return recordChecksum(System.getProperties());
        }

        /**
         * Fully qualified class name of the {@link io.aeron.archive.checksum.Checksum} implementation to use during
         * recording to compute checksums. Non-empty value means that checksum is enabled for recording.
         *
         * @param properties to read the configuration from.
         * @return class that implements {@link io.aeron.archive.checksum.Checksum} interface
         * @see Configuration#RECORD_CHECKSUM_PROP_NAME
         */
        public static String recordChecksum(final Properties properties)
        {
            return properties.getProperty(RECORD_CHECKSUM_PROP_NAME);
        }

        /**
         * Fully qualified class name of the {@link io.aeron.archive.checksum.Checksum} implementation to use during
         * replay for the checksum. Non-empty value means that checksum is enabled for replay.
         *
         * @return class that implements {@link io.aeron.archive.checksum.Checksum} interface
         * @see Configuration#REPLAY_CHECKSUM_PROP_NAME
         */
        public static String replayChecksum()
        {
            return replayChecksum(System.getProperties());
        }

        /**
         * Fully qualified class name of the {@link io.aeron.archive.checksum.Checksum} implementation to use during
         * replay for the checksum. Non-empty value means that checksum is enabled for replay.
         *
         * @param properties to read the configuration from.
         * @return class that implements {@link io.aeron.archive.checksum.Checksum} interface
         * @see Configuration#REPLAY_CHECKSUM_PROP_NAME
         */
        public static String replayChecksum(final Properties properties)
        {
            return properties.getProperty(REPLAY_CHECKSUM_PROP_NAME);
        }

        /**
         * Should the network (UDP) control channel be enabled.
         *
         * @return {@code true} if the network control channel to be enabled.
         * @see #CONTROL_CHANNEL_ENABLED_PROP_NAME
         */
        public static boolean controlChannelEnabled()
        {
            return controlChannelEnabled(System.getProperties());
        }

        /**
         * Should the network (UDP) control channel be enabled.
         *
         * @param properties to read the configuration from.
         * @return {@code true} if the network control channel to be enabled.
         * @see #CONTROL_CHANNEL_ENABLED_PROP_NAME
         */
        public static boolean controlChannelEnabled(final Properties properties)
        {
            return "true".equals(properties.getProperty(CONTROL_CHANNEL_ENABLED_PROP_NAME, "true"));
        }

        /**
         * Return configured id for the Archive.
         *
         * @return id for the Archive or {@link Aeron#NULL_VALUE}.
         * @see #ARCHIVE_ID_PROP_NAME
         */
        public static long archiveId()
        {
            return archiveId(System.getProperties());
        }

        /**
         * Return configured id for the Archive.
         *
         * @param properties to read the configuration from.
         * @return id for the Archive or {@link Aeron#NULL_VALUE}.
         * @see #ARCHIVE_ID_PROP_NAME
         */
        public static long archiveId(final Properties properties)
        {
            final String prop = properties.getProperty(Configuration.ARCHIVE_ID_PROP_NAME);
            if (!Strings.isEmpty(prop))
            {
                return AsciiEncoding.parseLongAscii(prop, 0, prop.length());
            }
            return NULL_VALUE;
        }
    }

    /**
     * Overrides for the defaults and system properties.
     * <p>
     * The context will be owned by {@link ArchiveConductor} after a successful
     * {@link Archive#launch(Context)} and closed via {@link Archive#close()}.
     */
    public static final class Context implements Cloneable
    {
        private static final VarHandle IS_CONCLUDED_VH;

        static
        {
            try
            {
                IS_CONCLUDED_VH = MethodHandles.lookup().findVarHandle(Context.class, "isConcluded", boolean.class);
            }
            catch (final ReflectiveOperationException ex)
            {
                throw new ExceptionInInitializerError(ex);
            }
        }

        private final Properties properties;
        private volatile boolean isConcluded;
        private boolean deleteArchiveOnStart;
        private boolean ownsAeronClient;
        private String aeronDirectoryName;
        private Aeron aeron;
        private File archiveDir;
        private File markFileDir;
        private String archiveDirectoryName;
        private FileChannel archiveDirChannel;
        private FileStore archiveFileStore;
        private Catalog catalog;
        private ArchiveMarkFile markFile;
        private AeronArchive.Context archiveClientContext;
        private AgentInvoker mediaDriverAgentInvoker;

        private boolean controlChannelEnabled;
        private String controlChannel;
        private int controlStreamId;
        private String localControlChannel;
        private int localControlStreamId;
        private boolean controlTermBufferSparse;
        private int controlTermBufferLength;
        private int controlMtuLength;
        private String recordingEventsChannel;
        private int recordingEventsStreamId;
        private boolean recordingEventsEnabled;
        private String replicationChannel;

        private long connectTimeoutNs;
        private long sessionLivenessCheckIntervalNs;
        private long replayLingerTimeoutNs;
        private long conductorCycleThresholdNs;
        private long recorderCycleThresholdNs;
        private long replayerCycleThresholdNs;
        private long catalogCapacity;
        private long lowStorageSpaceThreshold;
        private int segmentFileLength;
        private int fileSyncLevel;
        private int catalogFileSyncLevel;
        private int maxConcurrentRecordings;
        private int maxConcurrentReplays;
        private int fileIoMaxLength;
        private long archiveId;
        private ArchiveThreadingMode threadingMode;
        private ThreadFactory threadFactory;
        private ThreadFactory recorderThreadFactory;
        private ThreadFactory replayerThreadFactory;
        private CountDownLatch abortLatch;

        private Supplier<IdleStrategy> idleStrategySupplier;
        private Supplier<IdleStrategy> replayerIdleStrategySupplier;
        private Supplier<IdleStrategy> recorderIdleStrategySupplier;
        private EpochClock epochClock;
        private NanoClock nanoClock;
        private AuthenticatorSupplier authenticatorSupplier;
        private AuthorisationServiceSupplier authorisationServiceSupplier;
        private Counter controlSessionsCounter;
        private Counter recordingSessionCounter;
        private Counter replaySessionCounter;
        private int errorBufferLength;
        private ErrorHandler errorHandler;
        private AtomicCounter errorCounter;
        private CountedErrorHandler countedErrorHandler;

        private Checksum recordChecksum;
        private Checksum replayChecksum;

        private UnsafeBuffer dataBuffer;
        private UnsafeBuffer replayBuffer;
        private UnsafeBuffer recordChecksumBuffer;
        private DutyCycleTracker conductorDutyCycleTracker;
        private DutyCycleTracker recorderDutyCycleTracker;
        private DutyCycleTracker replayerDutyCycleTracker;

        private Counter totalWriteBytesCounter;
        private Counter totalWriteTimeCounter;
        private Counter maxWriteTimeCounter;
        private Counter totalReadBytesCounter;
        private Counter totalReadTimeCounter;
        private Counter maxReadTimeCounter;
        private String secureRandomAlgorithm;

        /**
         * Construct a Context using default values and loading from system properties.
         */
        public Context()
        {
            this(System.getProperties());
        }

        /**
         * Construct a Context using default values loaded from the supplied properties.
         *
         * @param properties to load the configuration from.
         */
        public Context(final Properties properties)
        {
            this.properties = properties;
            applyDefaults(properties);
        }

        /**
         * The properties this context was constructed from, to be propagated to any context it
         * creates.
         *
         * @return the properties this context was constructed from.
         */
        public Properties properties()
        {
            return properties;
        }

        private void applyDefaults(final Properties properties)
        {
            deleteArchiveOnStart = Configuration.deleteArchiveOnStart(properties);
            ownsAeronClient = false;
            aeronDirectoryName = CommonContext.getAeronDirectoryName(properties);
            archiveDirectoryName = Configuration.archiveDirName(properties);
            controlChannelEnabled = Configuration.controlChannelEnabled(properties);
            controlChannel = AeronArchive.Configuration.controlChannel(properties);
            controlStreamId = AeronArchive.Configuration.controlStreamId(properties);
            localControlChannel = AeronArchive.Configuration.localControlChannel(properties);
            localControlStreamId = AeronArchive.Configuration.localControlStreamId(properties);
            controlTermBufferSparse = AeronArchive.Configuration.controlTermBufferSparse(properties);
            controlTermBufferLength = AeronArchive.Configuration.controlTermBufferLength(properties);
            controlMtuLength = AeronArchive.Configuration.controlMtuLength(properties);
            recordingEventsChannel = AeronArchive.Configuration.recordingEventsChannel(properties);
            recordingEventsStreamId = AeronArchive.Configuration.recordingEventsStreamId(properties);
            recordingEventsEnabled = AeronArchive.Configuration.recordingEventsEnabled(properties);
            replicationChannel = Configuration.replicationChannel(properties);

            connectTimeoutNs = Configuration.connectTimeoutNs(properties);
            sessionLivenessCheckIntervalNs = getDurationInNanos(
                properties, SESSION_LIVENESS_CHECK_INTERVAL_PROP_NAME, SESSION_LIVENESS_CHECK_INTERVAL_DEFAULT_NS);
            replayLingerTimeoutNs = Configuration.replayLingerTimeoutNs(properties);
            conductorCycleThresholdNs = Configuration.conductorCycleThresholdNs(properties);
            recorderCycleThresholdNs = Configuration.recorderCycleThresholdNs(properties);
            replayerCycleThresholdNs = Configuration.replayerCycleThresholdNs(properties);
            catalogCapacity = Configuration.catalogCapacity(properties);
            lowStorageSpaceThreshold = Configuration.lowStorageSpaceThreshold(properties);
            segmentFileLength = Configuration.segmentFileLength(properties);
            fileSyncLevel = Configuration.fileSyncLevel(properties);
            catalogFileSyncLevel = Configuration.catalogFileSyncLevel(properties);
            maxConcurrentRecordings = Configuration.maxConcurrentRecordings(properties);
            maxConcurrentReplays = Configuration.maxConcurrentReplays(properties);
            fileIoMaxLength = Configuration.fileIoMaxLength(properties);
            archiveId = Configuration.archiveId(properties);
            threadingMode = Configuration.threadingMode(properties);
            errorBufferLength = Configuration.errorBufferLength(properties);
            secureRandomAlgorithm = CommonContext.getSecureRandomAlgorithm(properties);
        }

        /**
         * Perform a shallow copy of the object.
         *
         * @return a shallow copy of the object.
         */
        public Context clone()
        {
            try
            {
                return (Context)super.clone();
            }
            catch (final CloneNotSupportedException ex)
            {
                throw new RuntimeException(ex);
            }
        }

        /**
         * Conclude the configuration parameters by resolving dependencies and null values to use defaults.
         */
        @SuppressWarnings("MethodLength")
        public void conclude()
        {
            if ((boolean)IS_CONCLUDED_VH.getAndSet(this, true))
            {
                throw new ConcurrentConcludeException();
            }

            if (catalogFileSyncLevel < fileSyncLevel)
            {
                throw new ConfigurationException(
                    "catalogFileSyncLevel " + catalogFileSyncLevel + " < fileSyncLevel " + fileSyncLevel);
            }

            if (fileIoMaxLength < TERM_MIN_LENGTH || !BitUtil.isPowerOfTwo(fileIoMaxLength))
            {
                throw new ConfigurationException("invalid fileIoMaxLength=" + fileIoMaxLength);
            }

            io.aeron.driver.Configuration.validateMtuLength(controlMtuLength);
            checkTermLength(controlTermBufferLength);

            if (controlChannelEnabled)
            {
                if (null == controlChannel)
                {
                    throw new ConfigurationException("Archive.Context.controlChannel must be set");
                }

                if (!controlChannel.startsWith(CommonContext.UDP_CHANNEL))
                {
                    throw new ConfigurationException(
                        "Archive.Context.controlChannel must be UDP media: uri=" + controlChannel);
                }
            }

            if (!localControlChannel.startsWith(CommonContext.IPC_CHANNEL))
            {
                throw new ConfigurationException("local control channel must be IPC media: uri=" + localControlChannel);
            }

            if (null == replicationChannel)
            {
                throw new ConfigurationException("Archive.Context.replicationChannel must be set");
            }

            if (recordingEventsEnabled() && null == recordingEventsChannel())
            {
                throw new ConfigurationException(
                    "Archive.Context.recordingEventsChannel must be set if " +
                        "Archive.Context.recordingEventsEnabled is true");
            }

            if (null != mediaDriverAgentInvoker && ArchiveThreadingMode.INVOKER != threadingMode)
            {
                throw new ConfigurationException(
                    "Archive.Context.threadingMode(ArchiveThreadingMode.INVOKER) must be set if " +
                        "Archive.Context.mediaDriverAgentInvoker is set");
            }

            if (null == archiveDir)
            {
                archiveDir = new File(archiveDirectoryName);
            }

            if (null == markFileDir)
            {
                final String markFileDirPath = Configuration.markFileDir(properties);
                markFileDir = !Strings.isEmpty(markFileDirPath) ? new File(markFileDirPath) : archiveDir;
            }

            try
            {
                archiveDir = archiveDir.getCanonicalFile();
                archiveDirectoryName = archiveDir.getAbsolutePath();
                markFileDir = markFileDir.getCanonicalFile();
            }
            catch (final IOException e)
            {
                throw new UncheckedIOException(e);
            }

            if (deleteArchiveOnStart)
            {
                IoUtil.delete(archiveDir, false);
            }

            IoUtil.ensureDirectoryExists(archiveDir, "archive");
            IoUtil.ensureDirectoryExists(markFileDir, "mark file");

            archiveDirChannel = channelForDirectorySync(archiveDir, catalogFileSyncLevel);

            if (null == archiveFileStore)
            {
                try
                {
                    archiveFileStore = Files.getFileStore(archiveDir.toPath());
                }
                catch (final IOException ex)
                {
                    throw new UncheckedIOException(ex);
                }
            }

            if (null == epochClock)
            {
                epochClock = SystemEpochClock.INSTANCE;
            }

            if (null == nanoClock)
            {
                nanoClock = SystemNanoClock.INSTANCE;
            }

            if (null != aeron)
            {
                aeronDirectoryName = aeron.context().aeronDirectoryName();
            }

            concludeArchiveId();

            if (null == markFile)
            {
                if (errorBufferLength < ERROR_BUFFER_LENGTH_DEFAULT ||
                    errorBufferLength > Integer.MAX_VALUE - ArchiveMarkFile.HEADER_LENGTH)
                {
                    throw new ConfigurationException("invalid errorBufferLength=" + errorBufferLength);
                }

                markFile = new ArchiveMarkFile(this);
            }

            MarkFile.ensureMarkFileLink(
                archiveDir,
                new File(markFile.parentDirectory(), ArchiveMarkFile.FILENAME),
                ArchiveMarkFile.LINK_FILENAME);

            errorHandler = CommonContext.setupErrorHandler(
                properties,
                errorHandler, new DistinctErrorLog(markFile.errorBuffer(), epochClock, US_ASCII));

            final ExpandableArrayBuffer tempBuffer = new ExpandableArrayBuffer();

            final String clientName = "archive archiveId=" + archiveId;
            if (null == aeron)
            {
                ownsAeronClient = true;

                aeron = Aeron.connect(
                    new Aeron.Context(properties)
                        .aeronDirectoryName(aeronDirectoryName)
                        .epochClock(epochClock)
                        .nanoClock(nanoClock)
                        .errorHandler(errorHandler)
                        .driverAgentInvoker(mediaDriverAgentInvoker)
                        .useConductorAgentInvoker(true)
                        .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE)
                        .awaitingIdleStrategy(YieldingIdleStrategy.INSTANCE)
                        .clientLock(NoOpLock.INSTANCE)
                        .clientName(clientName));

                if (null == errorCounter)
                {
                    if (NULL_VALUE !=
                        ArchiveCounters.find(aeron.countersReader(), ARCHIVE_ERROR_COUNT_TYPE_ID, archiveId))
                    {
                        throw new ArchiveException("found existing archive for archiveId=" + archiveId);
                    }
                    errorCounter = ArchiveCounters.allocateErrorCounter(aeron, tempBuffer, archiveId);
                }
            }
            else if (!aeron.context().useConductorAgentInvoker())
            {
                throw new ArchiveException(
                    "Aeron client instance must set Aeron.Context.useConductorInvoker(true)");
            }

            if (!(aeron.context().subscriberErrorHandler() instanceof RethrowingErrorHandler))
            {
                throw new ArchiveException("Aeron client must use a RethrowingErrorHandler");
            }

            Objects.requireNonNull(errorCounter, "Error counter must be supplied if aeron client is");

            if (null == countedErrorHandler)
            {
                countedErrorHandler = new CountedErrorHandler(errorHandler, errorCounter);
            }

            if (null == threadFactory)
            {
                threadFactory = Thread::new;
            }

            if (null == recorderThreadFactory)
            {
                recorderThreadFactory = threadFactory;
            }

            if (null == replayerThreadFactory)
            {
                replayerThreadFactory = threadFactory;
            }

            if (null == idleStrategySupplier)
            {
                idleStrategySupplier = Configuration.idleStrategySupplier(properties, null);
            }

            if (null == conductorDutyCycleTracker)
            {
                conductorDutyCycleTracker = new DutyCycleStallTracker(
                    ArchiveCounters.allocate(
                        aeron,
                        tempBuffer,
                        AeronCounters.ARCHIVE_MAX_CYCLE_TIME_TYPE_ID,
                        "archive-conductor max cycle time in ns: " + threadingMode.name(),
                        archiveId),
                    ArchiveCounters.allocate(
                        aeron,
                        tempBuffer,
                        AeronCounters.ARCHIVE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID,
                        "archive-conductor work cycle time exceeded count: threshold=" +
                            SystemUtil.formatDuration(conductorCycleThresholdNs) + " " + threadingMode.name(),
                        archiveId),
                    conductorCycleThresholdNs);
            }

            if (DEDICATED == threadingMode)
            {
                if (null == recorderIdleStrategySupplier)
                {
                    recorderIdleStrategySupplier = Configuration.recorderIdleStrategySupplier(properties, null);
                    if (null == recorderIdleStrategySupplier)
                    {
                        recorderIdleStrategySupplier = idleStrategySupplier;
                    }
                }

                if (null == replayerIdleStrategySupplier)
                {
                    replayerIdleStrategySupplier = Configuration.replayerIdleStrategySupplier(properties, null);
                    if (null == replayerIdleStrategySupplier)
                    {
                        replayerIdleStrategySupplier = idleStrategySupplier;
                    }
                }

                if (null == recorderDutyCycleTracker)
                {
                    recorderDutyCycleTracker = new DutyCycleStallTracker(
                        ArchiveCounters.allocate(
                            aeron,
                            tempBuffer,
                            AeronCounters.ARCHIVE_MAX_CYCLE_TIME_TYPE_ID,
                            "archive-recorder max cycle time in ns: " + threadingMode.name(),
                            archiveId),
                        ArchiveCounters.allocate(
                            aeron,
                            tempBuffer,
                            AeronCounters.ARCHIVE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID,
                            "archive-recorder work cycle time exceeded count: threshold=" +
                                SystemUtil.formatDuration(recorderCycleThresholdNs) + " " + threadingMode.name(),
                            archiveId),
                        recorderCycleThresholdNs);
                }

                if (null == replayerDutyCycleTracker)
                {
                    replayerDutyCycleTracker = new DutyCycleStallTracker(
                        ArchiveCounters.allocate(
                            aeron,
                            tempBuffer,
                            AeronCounters.ARCHIVE_MAX_CYCLE_TIME_TYPE_ID,
                            "archive-replayer max cycle time in ns: " + threadingMode.name(),
                            archiveId),
                        ArchiveCounters.allocate(
                            aeron,
                            tempBuffer,
                            AeronCounters.ARCHIVE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID,
                            "archive-replayer work cycle time exceeded count: threshold=" +
                                SystemUtil.formatDuration(replayerCycleThresholdNs) + " " + threadingMode.name(),
                            archiveId),
                        replayerCycleThresholdNs);
                }
            }

            if (!isPowerOfTwo(segmentFileLength))
            {
                throw new ArchiveException("segment file length not a power of 2: " + segmentFileLength);
            }
            else if (segmentFileLength < TERM_MIN_LENGTH || segmentFileLength > TERM_MAX_LENGTH)
            {
                throw new ArchiveException("segment file length not in valid range: " + segmentFileLength);
            }

            if (null == authenticatorSupplier)
            {
                authenticatorSupplier = Configuration.authenticatorSupplier(properties);
            }

            if (null == authorisationServiceSupplier)
            {
                authorisationServiceSupplier = Configuration.authorisationServiceSupplier(properties);
            }

            concludeRecordChecksum();
            concludeReplayChecksum();

            if (null == catalog)
            {
                catalog = new Catalog(
                    archiveDir,
                    archiveDirChannel,
                    catalogFileSyncLevel,
                    catalogCapacity,
                    epochClock,
                    recordChecksum,
                    null != recordChecksum ? recordChecksumBuffer() : dataBuffer());
            }

            if (null == archiveClientContext)
            {
                archiveClientContext = new AeronArchive.Context(properties);
            }

            if (null == archiveClientContext.controlResponseChannel())
            {
                if (controlChannelEnabled)
                {
                    final ChannelUri controlChannelUri = ChannelUri.parse(controlChannel);
                    final String endpoint = controlChannelUri.get(ENDPOINT_PARAM_NAME);
                    final int separatorIndex;

                    if (null == endpoint || -1 == (separatorIndex = endpoint.lastIndexOf(':')))
                    {
                        throw new ConfigurationException(
                            "Unable to derive Archive.Context.archiveClientContext.controlResponseChannel as " +
                                "Archive.Context.controlChannel.endpoint=" + endpoint +
                                " and is not in the <host>:<port> format");
                    }

                    final String responseEndpoint = endpoint.substring(0, separatorIndex) + ":0";
                    final String responseChannel = new ChannelUriStringBuilder()
                        .media("udp")
                        .endpoint(responseEndpoint)
                        .build();

                    archiveClientContext.controlResponseChannel(responseChannel);
                }
                else
                {
                    throw new ConfigurationException(
                        "Archive.Context.archiveClientContext.controlResponseChannel must be set if " +
                            "Archive.Context.controlChannelEnabled is false"
                    );
                }
            }

            archiveClientContext
                .aeron(aeron)
                .lock(NoOpLock.INSTANCE)
                .errorHandler(errorHandler)
                .clientName(clientName);

            if (null == controlSessionsCounter)
            {
                controlSessionsCounter = ArchiveCounters.allocate(
                    aeron, tempBuffer, ARCHIVE_CONTROL_SESSIONS_TYPE_ID, "Archive Control Sessions", archiveId);
            }
            validateCounterTypeId(aeron, controlSessionsCounter, ARCHIVE_CONTROL_SESSIONS_TYPE_ID);

            if (null == recordingSessionCounter)
            {
                recordingSessionCounter = ArchiveCounters.allocate(
                    aeron,
                    tempBuffer,
                    ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID,
                    "Archive Recording Sessions",
                    archiveId);
            }
            validateCounterTypeId(aeron, recordingSessionCounter, ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID);

            if (null == replaySessionCounter)
            {
                replaySessionCounter = ArchiveCounters.allocate(
                    aeron,
                    tempBuffer,
                    ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID,
                    "Archive Replay Sessions",
                    archiveId);
            }
            validateCounterTypeId(aeron, replaySessionCounter, ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID);

            if (null == maxWriteTimeCounter)
            {
                final int counterId = ArchiveCounters.find(
                    aeron.countersReader(), ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID, archiveId);
                if (NULL_VALUE != counterId)
                {
                    throw new ConfigurationException(
                        "existing max write time counter detected for archiveId=" + archiveId);
                }

                maxWriteTimeCounter = ArchiveCounters.allocate(
                    aeron,
                    tempBuffer,
                    ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID,
                    "archive-recorder max write time in ns",
                    archiveId);
            }
            validateCounterTypeId(aeron, maxWriteTimeCounter, ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID);

            if (null == totalWriteBytesCounter)
            {
                totalWriteBytesCounter = ArchiveCounters.allocate(
                    aeron,
                    tempBuffer,
                    ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID,
                    "archive-recorder total write bytes",
                    archiveId);
            }
            validateCounterTypeId(aeron, totalWriteBytesCounter, ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID);

            if (null == totalWriteTimeCounter)
            {
                totalWriteTimeCounter = ArchiveCounters.allocate(
                    aeron,
                    tempBuffer,
                    ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID,
                    "archive-recorder total write time in ns",
                    archiveId);
            }
            validateCounterTypeId(aeron, totalWriteTimeCounter, ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID);

            if (null == maxReadTimeCounter)
            {
                maxReadTimeCounter = ArchiveCounters.allocate(
                    aeron,
                    tempBuffer,
                    ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID,
                    "archive-replayer max read time in ns",
                    archiveId);
            }
            validateCounterTypeId(aeron, maxReadTimeCounter, ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID);

            if (null == totalReadBytesCounter)
            {
                totalReadBytesCounter = ArchiveCounters.allocate(
                    aeron,
                    tempBuffer,
                    ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID,
                    "archive-replayer total read bytes",
                    archiveId);
            }
            validateCounterTypeId(aeron, totalReadBytesCounter, ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID);

            if (null == totalReadTimeCounter)
            {
                totalReadTimeCounter = ArchiveCounters.allocate(
                    aeron,
                    tempBuffer,
                    ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID,
                    "archive-replayer total read time in ns",
                    archiveId);
            }
            validateCounterTypeId(aeron, totalReadTimeCounter, ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID);

            int expectedCount = DEDICATED == threadingMode ? 2 : 0;
            expectedCount += aeron.conductorAgentInvoker() == null ? 1 : 0;
            abortLatch = new CountDownLatch(expectedCount);

            markFile.signalReady(epochClock.time());

            if (CommonContext.shouldPrintConfigurationOnStart(properties))
            {
                System.out.println(this);
            }
        }

        /**
         * Has the context had the {@link #conclude()} method called.
         *
         * @return true of the {@link #conclude()} method has been called.
         */
        public boolean isConcluded()
        {
            return isConcluded;
        }

        /**
         * Should an existing archive be deleted on start. Useful only for testing.
         *
         * @param deleteArchiveOnStart true if an existing archive should be deleted on startup.
         * @return this for a fluent API.
         */
        public Context deleteArchiveOnStart(final boolean deleteArchiveOnStart)
        {
            this.deleteArchiveOnStart = deleteArchiveOnStart;
            return this;
        }

        /**
         * Should an existing archive be deleted on start. Useful only for testing.
         *
         * @return {@code true} if an existing archive should be deleted on start up.
         */
        @Config(id = "ARCHIVE_DIR_DELETE_ON_START")
        public boolean deleteArchiveOnStart()
        {
            return deleteArchiveOnStart;
        }

        /**
         * Set the directory name to be used for the archive to store recordings and the {@link Catalog}.
         * This name is used if {@link #archiveDir(File)} is not set.
         *
         * @param archiveDirectoryName to store recordings and the {@link Catalog}.
         * @return this for a fluent API.
         * @see Configuration#ARCHIVE_DIR_PROP_NAME
         */
        public Context archiveDirectoryName(final String archiveDirectoryName)
        {
            this.archiveDirectoryName = archiveDirectoryName;
            return this;
        }

        /**
         * Get the directory name to be used to store recordings and the {@link Catalog}.
         *
         * @return the directory name to be used for the archive to store recordings and the {@link Catalog}.
         */
        @Config(id = "ARCHIVE_DIR")
        public String archiveDirectoryName()
        {
            return archiveDirectoryName;
        }

        /**
         * Get the directory in which the Archive will store recordings and the {@link Catalog}.
         *
         * @return the directory in which the Archive will store recordings and the {@link Catalog}.
         */
        public File archiveDir()
        {
            return archiveDir;
        }

        /**
         * Set the directory in which the Archive will store recordings and the {@link Catalog}.
         *
         * @param archiveDir the directory in which the Archive will store recordings and the {@link Catalog}.
         * @return this for a fluent API.
         */
        public Context archiveDir(final File archiveDir)
        {
            this.archiveDir = archiveDir;
            return this;
        }

        /**
         * Get the directory in which the Archive will store mark file (i.e. {@code archive-mark.dat}). It defaults to
         * {@link #archiveDir()} if it is not set explicitly via the
         * {@link Configuration#MARK_FILE_DIR_PROP_NAME}.
         *
         * @return the directory in which the Archive will store mark file (i.e. {@code archive-mark.dat}).
         * @see Configuration#MARK_FILE_DIR_PROP_NAME
         * @see #archiveDir()
         */
        @Config
        public File markFileDir()
        {
            return markFileDir;
        }

        /**
         * Set the directory in which the Archive will store mark file (i.e. {@code archive-mark.dat}).
         *
         * @param markFileDir the directory in which the Archive will store mark file (i.e. {@code archive-mark.dat}).
         * @return this for a fluent API.
         */
        public Context markFileDir(final File markFileDir)
        {
            this.markFileDir = markFileDir;
            return this;
        }

        /**
         * Get the {@link FileStore} where the archive will record streams.
         *
         * @return the {@link FileStore} where the archive will record streams.
         */
        public FileStore archiveFileStore()
        {
            return archiveFileStore;
        }

        /**
         * Set the {@link FileStore} where the archive will record streams. This should only be used for testing.
         *
         * @param fileStore where the archive will record streams.
         * @return this for a fluent API.
         */
        public Context archiveFileStore(final FileStore fileStore)
        {
            this.archiveFileStore = fileStore;
            return this;
        }

        /**
         * Get the {@link FileChannel} for the directory in which the Archive will store recordings and the
         * {@link Catalog}. This can be used for sync'ing the directory.
         *
         * @return the directory in which the Archive will store recordings and the {@link Catalog}.
         */
        public FileChannel archiveDirChannel()
        {
            return archiveDirChannel;
        }

        Context archiveDirChannel(final FileChannel archiveDirChannel)
        {
            this.archiveDirChannel = archiveDirChannel;
            return this;
        }

        /**
         * Set the {@link io.aeron.archive.client.AeronArchive.Context} that should be used for communicating
         * with a remote archive for replication.
         *
         * @param archiveContext that should be used for communicating with a remote Archive.
         * @return this for a fluent API.
         */
        public Context archiveClientContext(final AeronArchive.Context archiveContext)
        {
            this.archiveClientContext = archiveContext;
            return this;
        }

        /**
         * Get the {@link io.aeron.archive.client.AeronArchive.Context} that should be used for communicating
         * with a remote archive for replication.
         *
         * @return the {@link io.aeron.archive.client.AeronArchive.Context} that should be used for communicating
         * with a remote archive for replication.
         */
        public AeronArchive.Context archiveClientContext()
        {
            return archiveClientContext;
        }

        /**
         * Should the UDP control channel be enabled.
         *
         * @return {@code true} if the UDP control channel should be enabled.
         * @see Configuration#CONTROL_CHANNEL_ENABLED_PROP_NAME
         */
        @Config
        public boolean controlChannelEnabled()
        {
            return controlChannelEnabled;
        }

        /**
         * Set if the UDP control channel should be enabled.
         *
         * @param controlChannelEnabled indication of if the network control channel should be enabled.
         * @return this for a fluent API.
         * @see Configuration#CONTROL_CHANNEL_ENABLED_PROP_NAME
         */
        public Context controlChannelEnabled(final boolean controlChannelEnabled)
        {
            this.controlChannelEnabled = controlChannelEnabled;
            return this;
        }

        /**
         * Get the channel URI on which the control request subscription will listen.
         *
         * @return the channel URI on which the control request subscription will listen.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_CHANNEL_PROP_NAME
         */
        public String controlChannel()
        {
            return controlChannel;
        }

        /**
         * Set the channel URI on which the control request subscription will listen.
         *
         * @param controlChannel channel URI on which the control request subscription will listen.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_CHANNEL_PROP_NAME
         */
        public Context controlChannel(final String controlChannel)
        {
            this.controlChannel = controlChannel;
            return this;
        }

        /**
         * Get the stream id on which the control request subscription will listen.
         *
         * @return the stream id on which the control request subscription will listen.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_STREAM_ID_PROP_NAME
         */
        public int controlStreamId()
        {
            return controlStreamId;
        }

        /**
         * Set the stream id on which the control request subscription will listen.
         *
         * @param controlStreamId stream id on which the control request subscription will listen.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_STREAM_ID_PROP_NAME
         */
        public Context controlStreamId(final int controlStreamId)
        {
            this.controlStreamId = controlStreamId;
            return this;
        }

        /**
         * Get the driver local channel URI on which the control request subscription will listen.
         *
         * @return the channel URI on which the control request subscription will listen.
         * @see io.aeron.archive.client.AeronArchive.Configuration#LOCAL_CONTROL_CHANNEL_PROP_NAME
         */
        public String localControlChannel()
        {
            return localControlChannel;
        }

        /**
         * Set the driver local channel URI on which the control request subscription will listen.
         *
         * @param controlChannel channel URI on which the control request subscription will listen.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#LOCAL_CONTROL_CHANNEL_PROP_NAME
         */
        public Context localControlChannel(final String controlChannel)
        {
            this.localControlChannel = controlChannel;
            return this;
        }

        /**
         * Get the local stream id on which the control request subscription will listen.
         *
         * @return the stream id on which the control request subscription will listen.
         * @see io.aeron.archive.client.AeronArchive.Configuration#LOCAL_CONTROL_STREAM_ID_PROP_NAME
         */
        public int localControlStreamId()
        {
            return localControlStreamId;
        }

        /**
         * Should the control streams use sparse file term buffers.
         *
         * @param controlTermBufferSparse for the control stream.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_TERM_BUFFER_SPARSE_PROP_NAME
         */
        public Context controlTermBufferSparse(final boolean controlTermBufferSparse)
        {
            this.controlTermBufferSparse = controlTermBufferSparse;
            return this;
        }

        /**
         * Should the control streams use sparse file term buffers.
         *
         * @return {@code true} if the control stream should use sparse file term buffers.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_TERM_BUFFER_SPARSE_PROP_NAME
         */
        public boolean controlTermBufferSparse()
        {
            return controlTermBufferSparse;
        }

        /**
         * Set the term buffer length for the control streams.
         *
         * @param controlTermBufferLength for the control streams.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_TERM_BUFFER_LENGTH_PROP_NAME
         */
        public Context controlTermBufferLength(final int controlTermBufferLength)
        {
            this.controlTermBufferLength = controlTermBufferLength;
            return this;
        }

        /**
         * Get the term buffer length for the control streams.
         *
         * @return the term buffer length for the control streams.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_TERM_BUFFER_LENGTH_PROP_NAME
         */
        public int controlTermBufferLength()
        {
            return controlTermBufferLength;
        }

        /**
         * Set the MTU length for the control streams.
         *
         * @param controlMtuLength for the control streams.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_MTU_LENGTH_PROP_NAME
         */
        public Context controlMtuLength(final int controlMtuLength)
        {
            this.controlMtuLength = controlMtuLength;
            return this;
        }

        /**
         * Get the MTU length for the control streams.
         *
         * @return the MTU length for the control streams.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_MTU_LENGTH_PROP_NAME
         */
        public int controlMtuLength()
        {
            return controlMtuLength;
        }

        /**
         * Set the local stream id on which the control request subscription will listen.
         *
         * @param controlStreamId stream id on which the control request subscription will listen.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#LOCAL_CONTROL_STREAM_ID_PROP_NAME
         */
        public Context localControlStreamId(final int controlStreamId)
        {
            this.localControlStreamId = controlStreamId;
            return this;
        }

        /**
         * Get the channel URI on which the recording events publication will publish. Will be null if not configured.
         *
         * @return the channel URI on which the recording events publication will publish.
         * @see io.aeron.archive.client.AeronArchive.Configuration#RECORDING_EVENTS_CHANNEL_PROP_NAME
         */
        public String recordingEventsChannel()
        {
            return recordingEventsChannel;
        }

        /**
         * Set the channel URI on which the recording events publication will publish.
         * <p>
         * To support dynamic subscribers then this can be set to multicast or MDC (Multi-Destination-Cast) if
         * multicast cannot be supported for on the available the network infrastructure.
         *
         * @param recordingEventsChannel channel URI on which the recording events publication will publish.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#RECORDING_EVENTS_CHANNEL_PROP_NAME
         * @see io.aeron.CommonContext#MDC_CONTROL_PARAM_NAME
         */
        public Context recordingEventsChannel(final String recordingEventsChannel)
        {
            this.recordingEventsChannel = recordingEventsChannel;
            return this;
        }

        /**
         * Get the stream id on which the recording events publication will publish.
         *
         * @return the stream id on which the recording events publication will publish.
         * @see io.aeron.archive.client.AeronArchive.Configuration#RECORDING_EVENTS_STREAM_ID_PROP_NAME
         */
        public int recordingEventsStreamId()
        {
            return recordingEventsStreamId;
        }

        /**
         * Set the stream id on which the recording events publication will publish.
         *
         * @param recordingEventsStreamId stream id on which the recording events publication will publish.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#RECORDING_EVENTS_STREAM_ID_PROP_NAME
         */
        public Context recordingEventsStreamId(final int recordingEventsStreamId)
        {
            this.recordingEventsStreamId = recordingEventsStreamId;
            return this;
        }

        /**
         * Should the recording events channel be enabled.
         *
         * @return {@code true} if the recording events channel should be enabled.
         * @see io.aeron.archive.client.AeronArchive.Configuration#RECORDING_EVENTS_ENABLED_PROP_NAME
         */
        @Config
        public boolean recordingEventsEnabled()
        {
            return recordingEventsEnabled;
        }

        /**
         * Set if the recording events channel should be enabled.
         *
         * @param recordingEventsEnabled indication of if the recording events channel should be enabled.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#RECORDING_EVENTS_ENABLED_PROP_NAME
         */
        public Context recordingEventsEnabled(final boolean recordingEventsEnabled)
        {
            this.recordingEventsEnabled = recordingEventsEnabled;
            return this;
        }

        /**
         * Get the channel URI for replicating stream from another archive as replays.
         *
         * @return the channel URI for replicating stream from another archive as replays.
         * @see Archive.Configuration#REPLICATION_CHANNEL_PROP_NAME
         */
        @Config
        public String replicationChannel()
        {
            return replicationChannel;
        }

        /**
         * The channel URI for replicating stream from another archive as replays.
         *
         * @param replicationChannel channel URI for replicating stream from another archive as replays.
         * @return this for a fluent API.
         * @see Archive.Configuration#REPLICATION_CHANNEL_PROP_NAME
         */
        public Context replicationChannel(final String replicationChannel)
        {
            this.replicationChannel = replicationChannel;
            return this;
        }

        /**
         * The timeout in nanoseconds to wait for connection to be established.
         *
         * @param connectTimeoutNs to wait for a connection to be established.
         * @return this for a fluent API.
         * @see Configuration#CONNECT_TIMEOUT_PROP_NAME
         */
        public Context connectTimeoutNs(final long connectTimeoutNs)
        {
            this.connectTimeoutNs = connectTimeoutNs;
            return this;
        }

        /**
         * The timeout in nanoseconds to wait for connection to be established.
         *
         * @return the message timeout in nanoseconds to wait for a connection to be established.
         * @see Configuration#CONNECT_TIMEOUT_PROP_NAME
         */
        @Config
        public long connectTimeoutNs()
        {
            return connectTimeoutNs;
        }

        /**
         * The time internal in nanoseconds at which session liveness checks are performed.
         *
         * @param sessionLivenessCheckIntervalNs of a liveness check.
         * @return this for a fluent API.
         * @see Configuration#SESSION_LIVENESS_CHECK_INTERVAL_PROP_NAME
         * @since 1.47.0
         */
        public Context sessionLivenessCheckIntervalNs(final long sessionLivenessCheckIntervalNs)
        {
            this.sessionLivenessCheckIntervalNs = sessionLivenessCheckIntervalNs;
            return this;
        }

        /**
         * The time internal in nanoseconds at which session liveness checks are performed.
         *
         * @return the time internal in nanoseconds at which session liveness checks are performed.
         * @see Configuration#SESSION_LIVENESS_CHECK_INTERVAL_PROP_NAME
         * @since 1.47.0
         */
        @Config
        public long sessionLivenessCheckIntervalNs()
        {
            return sessionLivenessCheckIntervalNs;
        }

        /**
         * The timeout in nanoseconds for or a replay publication to linger after draining.
         *
         * @param replayLingerTimeoutNs in nanoseconds for a replay publication to linger after draining.
         * @return this for a fluent API.
         * @see Configuration#REPLAY_LINGER_TIMEOUT_PROP_NAME
         * @see io.aeron.driver.Configuration#PUBLICATION_LINGER_PROP_NAME
         */
        public Context replayLingerTimeoutNs(final long replayLingerTimeoutNs)
        {
            this.replayLingerTimeoutNs = replayLingerTimeoutNs;
            return this;
        }

        /**
         * The timeout in nanoseconds for a replay publication to linger after draining.
         *
         * @return the timeout in nanoseconds for a replay publication to linger after draining.
         * @see Configuration#REPLAY_LINGER_TIMEOUT_PROP_NAME
         * @see io.aeron.driver.Configuration#PUBLICATION_LINGER_PROP_NAME
         */
        @Config
        public long replayLingerTimeoutNs()
        {
            return replayLingerTimeoutNs;
        }

        /**
         * Set a threshold for the conductor work cycle time which when exceed it will increment the
         * conductor cycle time exceeded count.
         *
         * @param thresholdNs value in nanoseconds
         * @return this for fluent API.
         * @see io.aeron.archive.Archive.Configuration#CONDUCTOR_CYCLE_THRESHOLD_PROP_NAME
         * @see io.aeron.archive.Archive.Configuration#CONDUCTOR_CYCLE_THRESHOLD_DEFAULT_NS
         */
        public Context conductorCycleThresholdNs(final long thresholdNs)
        {
            this.conductorCycleThresholdNs = thresholdNs;
            return this;
        }

        /**
         * Threshold for the conductor work cycle time which when exceed it will increment the
         * conductor cycle time exceeded count.
         *
         * @return threshold to track for the conductor work cycle time.
         */
        @Config
        public long conductorCycleThresholdNs()
        {
            return conductorCycleThresholdNs;
        }

        /**
         * Set a threshold for the recorder work cycle time which when exceed it will increment the
         * recorder cycle time exceeded count.
         *
         * @param thresholdNs value in nanoseconds
         * @return this for fluent API.
         * @see io.aeron.archive.Archive.Configuration#RECORDER_CYCLE_THRESHOLD_PROP_NAME
         * @see io.aeron.archive.Archive.Configuration#RECORDER_CYCLE_THRESHOLD_DEFAULT_NS
         */
        public Context recorderCycleThresholdNs(final long thresholdNs)
        {
            this.recorderCycleThresholdNs = thresholdNs;
            return this;
        }

        /**
         * Threshold for the recorder work cycle time which when exceed it will increment the
         * recorder cycle time exceeded count.
         *
         * @return threshold to track for the recorder work cycle time.
         */
        @Config
        public long recorderCycleThresholdNs()
        {
            return recorderCycleThresholdNs;
        }

        /**
         * Set a threshold for the replayer work cycle time which when exceed it will increment the
         * replayer cycle time exceeded count.
         *
         * @param thresholdNs value in nanoseconds
         * @return this for fluent API.
         * @see io.aeron.archive.Archive.Configuration#REPLAYER_CYCLE_THRESHOLD_PROP_NAME
         * @see io.aeron.archive.Archive.Configuration#REPLAYER_CYCLE_THRESHOLD_DEFAULT_NS
         */
        public Context replayerCycleThresholdNs(final long thresholdNs)
        {
            this.replayerCycleThresholdNs = thresholdNs;
            return this;
        }

        /**
         * Threshold for the replayer work cycle time which when exceed it will increment the
         * replayer cycle time exceeded count.
         *
         * @return threshold to track for the replayer work cycle time.
         */
        @Config
        public long replayerCycleThresholdNs()
        {
            return replayerCycleThresholdNs;
        }

        /**
         * Set the duty cycle tracker for the conductor.
         *
         * @param dutyCycleTracker for the conductor.
         * @return this for a fluent API.
         */
        public Context conductorDutyCycleTracker(final DutyCycleTracker dutyCycleTracker)
        {
            this.conductorDutyCycleTracker = dutyCycleTracker;
            return this;
        }

        /**
         * The duty cycle tracker for the conductor.
         *
         * @return duty cycle tracker for the conductor.
         */
        public DutyCycleTracker conductorDutyCycleTracker()
        {
            return conductorDutyCycleTracker;
        }

        /**
         * Set the duty cycle tracker for the recorder.
         * NOTE: Only used in DEDICATED threading mode.
         *
         * @param dutyCycleTracker for the recorder.
         * @return this for a fluent API.
         */
        public Context recorderDutyCycleTracker(final DutyCycleTracker dutyCycleTracker)
        {
            this.recorderDutyCycleTracker = dutyCycleTracker;
            return this;
        }

        /**
         * The duty cycle tracker for the recorder.
         * NOTE: Only used in DEDICATED threading mode.
         *
         * @return duty cycle tracker for the recorder.
         */
        public DutyCycleTracker recorderDutyCycleTracker()
        {
            return recorderDutyCycleTracker;
        }

        /**
         * Set the duty cycle tracker for the replayer.
         * NOTE: Only used in DEDICATED threading mode.
         *
         * @param dutyCycleTracker for the replayer.
         * @return this for a fluent API.
         */
        public Context replayerDutyCycleTracker(final DutyCycleTracker dutyCycleTracker)
        {
            this.replayerDutyCycleTracker = dutyCycleTracker;
            return this;
        }

        /**
         * The duty cycle tracker for the replayer.
         * NOTE: Only used in DEDICATED threading mode.
         *
         * @return duty cycle tracker for the replayer.
         */
        public DutyCycleTracker replayerDutyCycleTracker()
        {
            return replayerDutyCycleTracker;
        }

        /**
         * Provides an explicit {@link Checksum} for checksum computation during recording.
         *
         * @param recordChecksum to be used for recordings.
         * @return this for a fluent API.
         * @see Configuration#RECORD_CHECKSUM_PROP_NAME
         */
        public Context recordChecksum(final Checksum recordChecksum)
        {
            this.recordChecksum = recordChecksum;
            return this;
        }

        /**
         * Get the {@link Checksum} for checksum computation during recording.
         *
         * @return the {@link Checksum} instance for checksum computation during recording or
         * {@code null} if no {@link Checksum} was configured.
         * @see Configuration#RECORD_CHECKSUM_PROP_NAME
         */
        @Config
        public Checksum recordChecksum()
        {
            return recordChecksum;
        }

        /**
         * The {@link Checksum} for checksum computation during replay.
         *
         * @param replayChecksum to be used for replays.
         * @return this for a fluent API.
         * @see Configuration#REPLAY_CHECKSUM_PROP_NAME
         */
        public Context replayChecksum(final Checksum replayChecksum)
        {
            this.replayChecksum = replayChecksum;
            return this;
        }

        /**
         * Get the {@link Checksum} for checksum computation during replay.
         *
         * @return the {@link Checksum} instance for checksum computation during replay or
         * {@code null} if no replay {@link Checksum} was configured.
         * @see Configuration#REPLAY_CHECKSUM_PROP_NAME
         */
        @Config
        public Checksum replayChecksum()
        {
            return replayChecksum;
        }

        /**
         * Provides an {@link IdleStrategy} supplier for idling the conductor or composite {@link Agent}. Which is also
         * the default for recorder and replayer {@link Agent}s.
         *
         * @param idleStrategySupplier supplier for idling the conductor.
         * @return this for a fluent API.
         */
        public Context idleStrategySupplier(final Supplier<IdleStrategy> idleStrategySupplier)
        {
            this.idleStrategySupplier = idleStrategySupplier;
            return this;
        }

        /**
         * Get a new {@link IdleStrategy} for idling the conductor or composite {@link Agent}. Which is also
         * the default for recorder and replayer {@link Agent}s.
         *
         * @return a new {@link IdleStrategy} for idling the conductor or composite {@link Agent}.
         */
        @Config(id = "ARCHIVE_IDLE_STRATEGY")
        public IdleStrategy idleStrategy()
        {
            return idleStrategySupplier.get();
        }

        /**
         * Provides an {@link IdleStrategy} supplier for idling the recorder {@link Agent}.
         *
         * @param idleStrategySupplier supplier for idling the conductor.
         * @return this for a fluent API.
         */
        public Context recorderIdleStrategySupplier(final Supplier<IdleStrategy> idleStrategySupplier)
        {
            this.recorderIdleStrategySupplier = idleStrategySupplier;
            return this;
        }

        /**
         * Get a new {@link IdleStrategy} for idling the recorder {@link Agent}.
         *
         * @return a new {@link IdleStrategy} for idling the recorder {@link Agent}.
         */
        @Config(id = "ARCHIVE_RECORDER_IDLE_STRATEGY")
        public IdleStrategy recorderIdleStrategy()
        {
            return recorderIdleStrategySupplier.get();
        }

        /**
         * Provides an {@link IdleStrategy} supplier for idling the replayer {@link Agent}.
         *
         * @param idleStrategySupplier supplier for idling the replayer.
         * @return this for a fluent API.
         */
        public Context replayerIdleStrategySupplier(final Supplier<IdleStrategy> idleStrategySupplier)
        {
            this.replayerIdleStrategySupplier = idleStrategySupplier;
            return this;
        }

        /**
         * Get a new {@link IdleStrategy} for idling the replayer {@link Agent}.
         *
         * @return a new {@link IdleStrategy} for idling the replayer {@link Agent}.
         */
        @Config(id = "ARCHIVE_REPLAYER_IDLE_STRATEGY")
        public IdleStrategy replayerIdleStrategy()
        {
            return replayerIdleStrategySupplier.get();
        }

        /**
         * Set the {@link EpochClock} to be used for tracking wall clock time.
         *
         * @param clock {@link EpochClock} to be used for tracking wall clock time.
         * @return this for a fluent API.
         */
        public Context epochClock(final EpochClock clock)
        {
            this.epochClock = clock;
            return this;
        }

        /**
         * Get the {@link EpochClock} to used for tracking wall clock time.
         *
         * @return the {@link EpochClock} to used for tracking wall clock time.
         */
        public EpochClock epochClock()
        {
            return epochClock;
        }

        /**
         * Set the {@link NanoClock} to be used for tracking wall clock time.
         *
         * @param clock {@link NanoClock} to be used for tracking wall clock time.
         * @return this for a fluent API.
         */
        public Context nanoClock(final NanoClock clock)
        {
            this.nanoClock = clock;
            return this;
        }

        /**
         * Get the {@link NanoClock} to used for tracking wall clock time.
         *
         * @return the {@link NanoClock} to used for tracking wall clock time.
         */
        public NanoClock nanoClock()
        {
            return nanoClock;
        }

        /**
         * Get the file length used for recording data segment files.
         *
         * @return the file length used for recording data segment files
         * @see Configuration#SEGMENT_FILE_LENGTH_PROP_NAME
         */
        @Config
        public int segmentFileLength()
        {
            return segmentFileLength;
        }

        /**
         * Set the file length to be used for recording data segment files. If the {@link Image#termBufferLength()} is
         * larger than the segment file length then the term length will be used.
         *
         * @param segmentFileLength the file length to be used for recording data segment files.
         * @return this for a fluent API.
         * @see Configuration#SEGMENT_FILE_LENGTH_PROP_NAME
         */
        public Context segmentFileLength(final int segmentFileLength)
        {
            this.segmentFileLength = segmentFileLength;
            return this;
        }

        /**
         * Get level at which files should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         *
         * @return the level to be applied for file write.
         * @see #catalogFileSyncLevel()
         * @see Configuration#FILE_SYNC_LEVEL_PROP_NAME
         */
        @Config
        public int fileSyncLevel()
        {
            return fileSyncLevel;
        }

        /**
         * Set level at which files should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         *
         * @param syncLevel to be applied for file writes.
         * @return this for a fluent API.
         * @see #catalogFileSyncLevel()
         * @see Configuration#FILE_SYNC_LEVEL_PROP_NAME
         */
        public Context fileSyncLevel(final int syncLevel)
        {
            this.fileSyncLevel = syncLevel;
            return this;
        }

        /**
         * Get level at which the catalog file should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         *
         * @return the level to be applied for file write.
         * @see #fileSyncLevel()
         * @see Configuration#CATALOG_FILE_SYNC_LEVEL_PROP_NAME
         */
        @Config
        public int catalogFileSyncLevel()
        {
            return catalogFileSyncLevel;
        }

        /**
         * Set level at which the catalog file should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         *
         * @param syncLevel to be applied for file writes.
         * @return this for a fluent API.
         * @see #fileSyncLevel()
         * @see Configuration#CATALOG_FILE_SYNC_LEVEL_PROP_NAME
         */
        public Context catalogFileSyncLevel(final int syncLevel)
        {
            this.catalogFileSyncLevel = syncLevel;
            return this;
        }

        /**
         * Get the {@link AgentInvoker} that should be used for the Media Driver if running in a lightweight mode.
         *
         * @return the {@link AgentInvoker} that should be used for the Media Driver if running in a lightweight mode.
         */
        public AgentInvoker mediaDriverAgentInvoker()
        {
            return mediaDriverAgentInvoker;
        }

        /**
         * Set the {@link AgentInvoker} that should be used for the Media Driver if running in a lightweight mode.
         *
         * @param mediaDriverAgentInvoker that should be used for the Media Driver if running in a lightweight mode.
         * @return this for a fluent API.
         */
        public Context mediaDriverAgentInvoker(final AgentInvoker mediaDriverAgentInvoker)
        {
            this.mediaDriverAgentInvoker = mediaDriverAgentInvoker;
            return this;
        }

        /**
         * Get the {@link ErrorHandler} to be used by the Archive.
         *
         * @return the {@link ErrorHandler} to be used by the Archive.
         */
        public ErrorHandler errorHandler()
        {
            return errorHandler;
        }

        /**
         * Set the {@link ErrorHandler} to be used by the Archive.
         *
         * @param errorHandler the error handler to be used by the Archive.
         * @return this for a fluent API
         */
        public Context errorHandler(final ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        /**
         * Non-default for context.
         *
         * @param countedErrorHandler to override the default.
         * @return this for a fluent API.
         */
        public Context countedErrorHandler(final CountedErrorHandler countedErrorHandler)
        {
            this.countedErrorHandler = countedErrorHandler;
            return this;
        }

        /**
         * The {@link #errorHandler()} that will increment {@link #errorCounter()} by default.
         *
         * @return {@link #errorHandler()} that will increment {@link #errorCounter()} by default.
         */
        public CountedErrorHandler countedErrorHandler()
        {
            return countedErrorHandler;
        }

        /**
         * Get the archive threading mode.
         *
         * @return the archive threading mode.
         * @see Configuration#THREADING_MODE_PROP_NAME
         */
        @Config
        public ArchiveThreadingMode threadingMode()
        {
            return threadingMode;
        }

        /**
         * Set the archive threading mode.
         *
         * @param threadingMode archive threading mode.
         * @return this for a fluent API.
         * @see Configuration#THREADING_MODE_PROP_NAME
         */
        public Context threadingMode(final ArchiveThreadingMode threadingMode)
        {
            this.threadingMode = threadingMode;
            return this;
        }

        /**
         * Get the thread factory used for creating threads in {@link ArchiveThreadingMode#SHARED} and
         * {@link ArchiveThreadingMode#DEDICATED} threading modes.
         *
         * @return thread factory used for creating threads in SHARED and DEDICATED threading modes.
         */
        public ThreadFactory threadFactory()
        {
            return threadFactory;
        }

        /**
         * Set the thread factory used for creating threads in {@link ArchiveThreadingMode#SHARED} and
         * {@link ArchiveThreadingMode#DEDICATED} threading modes. The thread factories can be overridden for the
         * recorder and replayer by using {@link #recorderThreadFactory(ThreadFactory)} and
         * {@link #replayerThreadFactory(ThreadFactory)} respectively.
         *
         * @param threadFactory used for creating threads in SHARED and DEDICATED threading modes.
         * @return this for a fluent API.
         */
        public Context threadFactory(final ThreadFactory threadFactory)
        {
            this.threadFactory = threadFactory;
            return this;
        }

        /**
         * Get the thread factory used for creating the recorder thread when running in DEDICATED threading mode.
         *
         * @return the thread factory used for creating the recorder thread when running in DEDICATED threading mode.
         */
        public ThreadFactory recorderThreadFactory()
        {
            return recorderThreadFactory;
        }

        /**
         * Set the thread factory used for creating the recorder thread when running in DEDICATED threading mode. This
         * will be used to override {@link #threadFactory}.
         *
         * @param threadFactory used for creating the recorder thread when running in DEDICATED threading mode.
         * @return this for a fluent API.
         */
        public Context recorderThreadFactory(final ThreadFactory threadFactory)
        {
            this.recorderThreadFactory = threadFactory;
            return this;
        }

        /**
         * Get the thread factory used for creating the replayer thread when running in DEDICATED threading mode.
         *
         * @return the thread factory used for creating the replayer thread when running in DEDICATED threading mode.
         */
        public ThreadFactory replayerThreadFactory()
        {
            return replayerThreadFactory;
        }

        /**
         * Set the thread factory used for creating the replayer thread when running in DEDICATED threading mode. This
         * will be used to override {@link #threadFactory}.
         *
         * @param threadFactory used for creating the replayer thread when running in DEDICATED threading mode.
         * @return this for a fluent API.
         */
        public Context replayerThreadFactory(final ThreadFactory threadFactory)
        {
            this.replayerThreadFactory = threadFactory;
            return this;
        }

        /**
         * Set the error buffer length in bytes to use.
         *
         * @param errorBufferLength in bytes to use.
         * @return this for a fluent API.
         * @see Configuration#ERROR_BUFFER_LENGTH_PROP_NAME
         */
        public Context errorBufferLength(final int errorBufferLength)
        {
            this.errorBufferLength = errorBufferLength;
            return this;
        }

        /**
         * The error buffer length in bytes.
         *
         * @return error buffer length in bytes.
         * @see Configuration#ERROR_BUFFER_LENGTH_PROP_NAME
         */
        @Config
        public int errorBufferLength()
        {
            return errorBufferLength;
        }

        /**
         * Set the id for this Archive instance.
         *
         * @param archiveId for this Archive instance.
         * @return this for a fluent API
         * @see io.aeron.archive.Archive.Configuration#ARCHIVE_ID_PROP_NAME
         */
        public Context archiveId(final long archiveId)
        {
            this.archiveId = archiveId;
            return this;
        }

        /**
         * Get the id of this Archive instance.
         *
         * @return the id of this Archive instance.
         * @see io.aeron.archive.Archive.Configuration#ARCHIVE_ID_PROP_NAME
         */
        @Config
        public long archiveId()
        {
            return archiveId;
        }

        /**
         * Get the error counter that will record the number of errors observed.
         *
         * @return the error counter that will record the number of errors observed.
         */
        public AtomicCounter errorCounter()
        {
            return errorCounter;
        }

        /**
         * Set the error counter that will record the number of errors observed.
         *
         * @param errorCounter the error counter that will record the number of errors observed.
         * @return this for a fluent API.
         */
        public Context errorCounter(final AtomicCounter errorCounter)
        {
            this.errorCounter = errorCounter;
            return this;
        }

        /**
         * Get the counter used to track the number of active control sessions.
         *
         * @return the counter used to track the number of active control sessions.
         */
        public Counter controlSessionsCounter()
        {
            return controlSessionsCounter;
        }

        /**
         * Set the counter used to track the number of active control sessions.
         *
         * @param controlSessionsCounter the counter used to track the number of active control sessions.
         * @return this for a fluent API.
         */
        public Context controlSessionsCounter(final Counter controlSessionsCounter)
        {
            this.controlSessionsCounter = controlSessionsCounter;
            return this;
        }

        /**
         * Get the counter used to track the count of concurrent recording sessions.
         *
         * @return the counter used to track the count of concurrent recording sessions.
         */
        public Counter recordingSessionCounter()
        {
            return recordingSessionCounter;
        }

        /**
         * Set the counter used to track the count of concurrent recording sessions.
         *
         * @param counter used to track the count of concurrent recording sessions.
         * @return this for a fluent API.
         */
        public Context recordingSessionCounter(final Counter counter)
        {
            this.recordingSessionCounter = counter;
            return this;
        }

        /**
         * Get the counter used to track the count of concurrent replay sessions.
         *
         * @return the counter used to track the count of concurrent replay sessions.
         */
        public Counter replaySessionCounter()
        {
            return replaySessionCounter;
        }

        /**
         * Set the counter used to track the count of concurrent replay sessions.
         *
         * @param counter used to track the count of concurrent replay sessions.
         * @return this for a fluent API.
         */
        public Context replaySessionCounter(final Counter counter)
        {
            this.replaySessionCounter = counter;
            return this;
        }

        /**
         * Get the counter used to track the total number of bytes written by the recorder.
         *
         * @return the counter used to track the total number of bytes written by the recorder.
         */
        public Counter totalWriteBytesCounter()
        {
            return totalWriteBytesCounter;
        }

        /**
         * Set the counter used to track the total number of bytes written by the recorder.
         *
         * @param counter used to track the total number of bytes written by the recorder.
         * @return this for a fluent API.
         */
        public Context totalWriteBytesCounter(final Counter counter)
        {
            this.totalWriteBytesCounter = counter;
            return this;
        }

        /**
         * Get the counter used to track the total time used by the recorder to write data.
         *
         * @return the counter used to track the total time used by the recorder to write data.
         */
        public Counter totalWriteTimeCounter()
        {
            return totalWriteTimeCounter;
        }

        /**
         * Set the counter used to track the total time used by the recorder to write data.
         *
         * @param counter used to track the total time used by the recorder to write data.
         * @return this for a fluent API.
         */
        public Context totalWriteTimeCounter(final Counter counter)
        {
            this.totalWriteTimeCounter = counter;
            return this;
        }

        /**
         * Get the counter used to track the max time used by the recorder to write a block of data.
         *
         * @return the counter used to track the max time used by the recorder to write a block of data.
         */
        public Counter maxWriteTimeCounter()
        {
            return maxWriteTimeCounter;
        }

        /**
         * Set the counter used to track the max time used by the recorder to write a block of data.
         *
         * @param counter used to track the max time used by the recorder to write a block of data.
         * @return this for a fluent API.
         */
        public Context maxWriteTimeCounter(final Counter counter)
        {
            maxWriteTimeCounter = counter;
            return this;
        }

        /**
         * Get the counter used to track the total number of bytes read by the replayer.
         *
         * @return the counter used to track the total number of bytes read by the replayer.
         */
        public Counter totalReadBytesCounter()
        {
            return totalReadBytesCounter;
        }

        /**
         * Set the counter used to track the total number of bytes read by the replayer.
         *
         * @param counter used to track the total number of bytes read by the replayer.
         * @return this for a fluent API.
         */
        public Context totalReadBytesCounter(final Counter counter)
        {
            this.totalReadBytesCounter = counter;
            return this;
        }

        /**
         * Get the counter used to track the total time used by the replayer to read data.
         *
         * @return the counter used to track the total time used by the replayer to read data.
         */
        public Counter totalReadTimeCounter()
        {
            return totalReadTimeCounter;
        }

        /**
         * Set the counter used to track the total time used by the replayer to read data.
         *
         * @param counter used to track the total time used by the replayer to read data.
         * @return this for a fluent API.
         */
        public Context totalReadTimeCounter(final Counter counter)
        {
            this.totalReadTimeCounter = counter;
            return this;
        }

        /**
         * Get the counter used to track the max time used by the replayer to read a block of data.
         *
         * @return the counter used to track the max time used by the replayer to read a block of data.
         */
        public Counter maxReadTimeCounter()
        {
            return maxReadTimeCounter;
        }

        /**
         * Set the counter used to track the max time used by the replayer to read a block of data.
         *
         * @param counter used to track the max time used by the replayer to read a block of data.
         * @return this for a fluent API.
         */
        public Context maxReadTimeCounter(final Counter counter)
        {
            this.maxReadTimeCounter = counter;
            return this;
        }

        /**
         * Get the max number of concurrent recordings.
         *
         * @return the max number of concurrent recordings.
         * @see Configuration#MAX_CONCURRENT_RECORDINGS_PROP_NAME
         */
        @Config
        public int maxConcurrentRecordings()
        {
            return maxConcurrentRecordings;
        }

        /**
         * Set the max number of concurrent recordings.
         *
         * @param maxConcurrentRecordings the max number of concurrent recordings.
         * @return this for a fluent API.
         * @see Configuration#MAX_CONCURRENT_RECORDINGS_PROP_NAME
         */
        public Context maxConcurrentRecordings(final int maxConcurrentRecordings)
        {
            this.maxConcurrentRecordings = maxConcurrentRecordings;
            return this;
        }

        /**
         * Get the max number of concurrent replays.
         *
         * @return the max number of concurrent replays.
         * @see Configuration#MAX_CONCURRENT_REPLAYS_PROP_NAME
         */
        @Config
        public int maxConcurrentReplays()
        {
            return maxConcurrentReplays;
        }

        /**
         * Set the max number of concurrent replays.
         *
         * @param maxConcurrentReplays the max number of concurrent replays.
         * @return this for a fluent API.
         * @see Configuration#MAX_CONCURRENT_REPLAYS_PROP_NAME
         */
        public Context maxConcurrentReplays(final int maxConcurrentReplays)
        {
            this.maxConcurrentReplays = maxConcurrentReplays;
            return this;
        }

        /**
         * Get the max length of a file IO operation.
         *
         * @return the max length of a file IO operation.
         * @see Configuration#FILE_IO_MAX_LENGTH_PROP_NAME
         */
        @Config
        public int fileIoMaxLength()
        {
            return fileIoMaxLength;
        }

        /**
         * Set the max length of a file IO operation.
         *
         * @param fileIoMaxLength the max length of a file IO operation.
         * @return this for a fluent API.
         * @see Configuration#FILE_IO_MAX_LENGTH_PROP_NAME
         */
        public Context fileIoMaxLength(final int fileIoMaxLength)
        {
            this.fileIoMaxLength = fileIoMaxLength;
            return this;
        }

        /**
         * Threshold below which the archive will reject new recording requests.
         *
         * @param lowStorageSpaceThreshold in bytes.
         * @return this for a fluent API.
         * @see Configuration#LOW_STORAGE_SPACE_THRESHOLD_PROP_NAME
         */
        public Context lowStorageSpaceThreshold(final long lowStorageSpaceThreshold)
        {
            this.lowStorageSpaceThreshold = lowStorageSpaceThreshold;
            return this;
        }

        /**
         * Threshold below which the archive will reject new recording requests.
         *
         * @return threshold below which the archive will reject new recording requests in bytes.
         * @see Configuration#LOW_STORAGE_SPACE_THRESHOLD_PROP_NAME
         */
        @Config
        public long lowStorageSpaceThreshold()
        {
            return lowStorageSpaceThreshold;
        }

        /**
         * Delete the archive directory if the {@link #archiveDir()} value is not null.
         * <p>
         * Use {@link #deleteDirectory()} instead.
         */
        @Deprecated
        public void deleteArchiveDirectory()
        {
            deleteDirectory();
        }

        /**
         * Delete the archive directory if the {@link #archiveDir()} value is not null.
         */
        public void deleteDirectory()
        {
            if (null != archiveDir)
            {
                IoUtil.delete(archiveDir, false);
            }
        }

        /**
         * Set the top level Aeron directory used for communication between the Aeron client and Media Driver.
         *
         * @param aeronDirectoryName the top level Aeron directory.
         * @return this for a fluent API.
         */
        public Context aeronDirectoryName(final String aeronDirectoryName)
        {
            this.aeronDirectoryName = aeronDirectoryName;
            return this;
        }

        /**
         * Get the top level Aeron directory used for communication between the Aeron client and Media Driver.
         *
         * @return The top level Aeron directory.
         */
        public String aeronDirectoryName()
        {
            return aeronDirectoryName;
        }

        /**
         * {@link Aeron} client for communicating with the local Media Driver.
         * <p>
         * This client will be closed when the {@link Archive#close()} or {@link #close()} methods are called if
         * {@link #ownsAeronClient()} is true.
         *
         * @param aeron client for communicating with the local Media Driver.
         * @return this for a fluent API.
         * @see Aeron#connect()
         */
        public Context aeron(final Aeron aeron)
        {
            this.aeron = aeron;
            return this;
        }

        /**
         * {@link Aeron} client for communicating with the local Media Driver.
         * <p>
         * If not provided then a default will be established during {@link #conclude()} by calling
         * {@link Aeron#connect()}.
         *
         * @return client for communicating with the local Media Driver.
         */
        public Aeron aeron()
        {
            return aeron;
        }

        /**
         * Does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         *
         * @param ownsAeronClient does this context own the {@link #aeron()} client.
         * @return this for a fluent API.
         */
        public Context ownsAeronClient(final boolean ownsAeronClient)
        {
            this.ownsAeronClient = ownsAeronClient;
            return this;
        }

        /**
         * Does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         *
         * @return does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         */
        public boolean ownsAeronClient()
        {
            return ownsAeronClient;
        }

        /**
         * The {@link Catalog} describing the contents of the Archive.
         *
         * @param catalog {@link Catalog} describing the contents of the Archive.
         * @return this for a fluent API.
         */
        Context catalog(final Catalog catalog)
        {
            this.catalog = catalog;
            return this;
        }

        /**
         * The {@link Catalog} describing the contents of the Archive.
         *
         * @return the {@link Catalog} describing the contents of the Archive.
         */
        Catalog catalog()
        {
            return catalog;
        }

        /**
         * The {@link ArchiveMarkFile} for the Archive.
         *
         * @param archiveMarkFile {@link ArchiveMarkFile} for the Archive.
         * @return this for a fluent API.
         */
        public Context archiveMarkFile(final ArchiveMarkFile archiveMarkFile)
        {
            this.markFile = archiveMarkFile;
            return this;
        }

        /**
         * The {@link ArchiveMarkFile} for the Archive.
         *
         * @return {@link ArchiveMarkFile} for the Archive.
         */
        public ArchiveMarkFile archiveMarkFile()
        {
            return markFile;
        }

        /**
         * Maximum number of catalog entries for the Archive.
         * <p>
         * <b>Note: </b> This method has no effect.
         *
         * @param maxCatalogEntries for the archive.
         * @return this for a fluent API.
         * @see #catalogCapacity(long)
         * @deprecated This method was deprecated in favor of {@link #catalogCapacity(long)} which works with bytes
         * rather than number of entries.
         */
        @Deprecated
        public Context maxCatalogEntries(final long maxCatalogEntries)
        {
            return this;
        }

        /**
         * Maximum number of catalog entries for the Archive.
         * <p>
         * <b>Note: </b> This method is not used.
         *
         * @return maximum number of catalog entries for the Archive.
         * @see #catalogCapacity()
         * @deprecated This method was deprecated in favor of {@link #catalogCapacity()} which returns capacity of
         * the {@link Catalog} in bytes rather than in number of entries.
         */
        @Deprecated
        @Config
        public long maxCatalogEntries()
        {
            return -1;
        }

        /**
         * Capacity in bytes of the {@link Catalog}.
         *
         * @param catalogCapacity in bytes.
         * @return this for a fluent API.
         * @see Configuration#CATALOG_CAPACITY_PROP_NAME
         */
        public Context catalogCapacity(final long catalogCapacity)
        {
            this.catalogCapacity = catalogCapacity;
            return this;
        }

        /**
         * Capacity in bytes of the {@link Catalog}.
         *
         * @return capacity in bytes of the {@link Catalog}.
         * @see Configuration#CATALOG_CAPACITY_PROP_NAME
         */
        @Config
        public long catalogCapacity()
        {
            return catalogCapacity;
        }

        /**
         * Get the {@link AuthenticatorSupplier} that should be used for the Archive.
         *
         * @return the {@link AuthenticatorSupplier} to be used for the Archive.
         */
        @Config
        public AuthenticatorSupplier authenticatorSupplier()
        {
            return authenticatorSupplier;
        }

        /**
         * Set the {@link AuthenticatorSupplier} that will be used for the Archive.
         *
         * @param authenticatorSupplier {@link AuthenticatorSupplier} to use for the Archive.
         * @return this for a fluent API.
         */
        public Context authenticatorSupplier(final AuthenticatorSupplier authenticatorSupplier)
        {
            this.authenticatorSupplier = authenticatorSupplier;
            return this;
        }

        /**
         * Get the {@link AuthorisationServiceSupplier} that should be used for the Archive.
         *
         * @return the {@link AuthorisationServiceSupplier} to be used for the Archive.
         */
        @Config
        public AuthorisationServiceSupplier authorisationServiceSupplier()
        {
            return authorisationServiceSupplier;
        }

        /**
         * <p>Set the {@link AuthorisationServiceSupplier} that will be used for the Archive.</p>
         * <p>When using an authorisation service for the ConsensusModule, then the following values for protocolId,
         * actionId, and type should be considered.</p>
         *
         * <table>
         *     <caption>Parameters for authorisation service queries from the Archive</caption>
         *     <thead>
         *         <tr><td>Description</td><td>protocolId</td><td>actionId</td><td>type(s)</td></tr>
         *     </thead>
         *     <tbody>
         *         <tr>
         *             <td>Start the recording of a stream</td>
         *             <td>{@link io.aeron.archive.codecs.MessageHeaderDecoder#SCHEMA_ID}</td>
         *             <td>{@link io.aeron.archive.codecs.StartRecordingRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Start the recording of a stream (version 2)</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.StartRecordingRequest2Decoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Stop the recording of a stream</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.StopRecordingRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Replay a stream from the archive</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.ReplayRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Stop a replay from the archive</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.StopReplayRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>List the recordings from the archive</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.ListRecordingsRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>List the recordings from the archive for a specific URI</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.ListRecordingsForUriRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>List a specific recording from the archive</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.ListRecordingRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Extend a recording</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.ExtendRecordingRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Extend a recording (version 2)</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.ExtendRecordingRequest2Decoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Gets the position of a recording</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.RecordingPositionRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Truncate a recording</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.TruncateRecordingRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Stop a recording by subscription</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.StopRecordingSubscriptionRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Extend a recording</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.ExtendRecordingRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Get the stop position for a recording</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.StopPositionRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Find the last recording for a given stream, session and channel fragment</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.FindLastMatchingRecordingRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>List subscriptions being used for recordings</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.ListRecordingSubscriptionsRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Stop all replays</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.StopAllReplaysRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Start replicating a recording</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.ReplicateRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Start replicating a recording (version 2)</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.ReplicateRequest2Decoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Stop replication a recording</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.StopReplicationRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Get the start position of a recording</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.StartRecordingRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Detach segment files from a recording</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.DetachSegmentsRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Delete detached segments</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.DeleteDetachedSegmentsRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Attach new segments for a recording</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.AttachSegmentsRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Migrate segments from one recording to another</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.MigrateSegmentsRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Keep alive the archive connection</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.KeepAliveRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Replicate a recording with a tag</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.TaggedReplicateRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Stop recording by recording id</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.StopRecordingByIdentityRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Purge a recording by recording id</td>
         *             <td></td>
         *             <td>{@link io.aeron.archive.codecs.PurgeRecordingRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *     </tbody>
         * </table>
         *
         * @param authorisationServiceSupplier {@link AuthorisationServiceSupplier} to use for the Archive.
         * @return this for a fluent API.
         */
        public Context authorisationServiceSupplier(final AuthorisationServiceSupplier authorisationServiceSupplier)
        {
            this.authorisationServiceSupplier = authorisationServiceSupplier;
            return this;
        }

        /**
         * Get the secure random algorithm should be used by various Aeron components.
         *
         * @return the secure random algorithm to be used.
         * @see #secureRandomAlgorithm(String)
         */
        public String secureRandomAlgorithm()
        {
            return secureRandomAlgorithm;
        }

        /**
         * Define which secure random algorithm should be used by various Aeron components. This string will be passed
         * to {@link java.security.SecureRandom#getInstance(String)}, with one exception. The special case of
         * <code>strong</code> (case-insensitive) will use {@link SecureRandom#getInstanceStrong()}
         *
         * @param algorithm the algorithm to be used or <code>strong</code>
         * @return this for the fluent API.
         * @see CommonContext#getSecureRandomAlgorithm()
         * @see CommonContext#SECURE_RANDOM_ALGORITHM_PROP_NAME
         * @see CommonContext#SECURE_RANDOM_ALGORITHM_DEFAULT
         */
        public Context secureRandomAlgorithm(final String algorithm)
        {
            this.secureRandomAlgorithm = algorithm;
            return this;
        }

        /**
         * Get the configured instance of SecureRandom using {@link SecureRandom#getInstanceStrong()} if
         * <code>strong</code> is specified.
         *
         * @return instance of SecureRandom
         * @throws AeronException if there is a problem resolving the algorithm
         */
        public SecureRandom secureRandom()
        {
            try
            {
                if ("strong".equalsIgnoreCase(secureRandomAlgorithm))
                {
                    return SecureRandom.getInstanceStrong();
                }
                else
                {
                    return SecureRandom.getInstance(secureRandomAlgorithm);
                }
            }
            catch (final NoSuchAlgorithmException ex)
            {
                throw new AeronException(
                    "unable to create SecureRandom for algorithm=" + secureRandomAlgorithm, ex, ERROR);
            }
        }

        CountDownLatch abortLatch()
        {
            return abortLatch;
        }

        void concludeRecordChecksum()
        {
            if (null == recordChecksum)
            {
                final String checksumClass = Configuration.recordChecksum(properties);
                if (!Strings.isEmpty(checksumClass))
                {
                    recordChecksum = Checksums.newInstance(checksumClass);
                }
            }
        }

        void concludeReplayChecksum()
        {
            if (null == replayChecksum)
            {
                final String checksumClass = Configuration.replayChecksum(properties);
                if (!Strings.isEmpty(checksumClass))
                {
                    replayChecksum = Checksums.newInstance(checksumClass);
                }
            }
        }

        Context dataBuffer(final UnsafeBuffer dataBuffer)
        {
            this.dataBuffer = dataBuffer;
            return this;
        }

        UnsafeBuffer dataBuffer()
        {
            if (null == dataBuffer)
            {
                dataBuffer = new UnsafeBuffer(allocateDirectAligned(fileIoMaxLength, CACHE_LINE_LENGTH));
            }

            return dataBuffer;
        }

        Context replayBuffer(final UnsafeBuffer replayBuffer)
        {
            this.replayBuffer = replayBuffer;
            return this;
        }

        UnsafeBuffer replayBuffer()
        {
            if (DEDICATED != threadingMode)
            {
                return dataBuffer();
            }

            if (null == replayBuffer)
            {
                replayBuffer = new UnsafeBuffer(allocateDirectAligned(fileIoMaxLength, CACHE_LINE_LENGTH));
            }

            return replayBuffer;
        }

        Context recordChecksumBuffer(final UnsafeBuffer recordChecksumBuffer)
        {
            this.recordChecksumBuffer = recordChecksumBuffer;
            return this;
        }

        UnsafeBuffer recordChecksumBuffer()
        {
            if (null == recordChecksum)
            {
                return null;
            }

            if (DEDICATED != threadingMode)
            {
                return dataBuffer();
            }

            if (null == recordChecksumBuffer)
            {
                recordChecksumBuffer = new UnsafeBuffer(allocateDirectAligned(fileIoMaxLength, CACHE_LINE_LENGTH));
            }

            return recordChecksumBuffer;
        }

        /**
         * Close the context and free applicable resources.
         * <p>
         * If {@link #ownsAeronClient()} is true then the {@link #aeron()} client will be closed.
         */
        public void close()
        {
            CloseHelper.close(countedErrorHandler, catalog);
            CloseHelper.close(countedErrorHandler, archiveDirChannel);

            if (ownsAeronClient)
            {
                CloseHelper.close(aeron);
            }
            else
            {
                CloseHelper.close(countedErrorHandler, controlSessionsCounter);
                CloseHelper.close(countedErrorHandler, recordingSessionCounter);
                CloseHelper.close(countedErrorHandler, replaySessionCounter);
                CloseHelper.close(countedErrorHandler, totalWriteBytesCounter);
                CloseHelper.close(countedErrorHandler, totalWriteTimeCounter);
                CloseHelper.close(countedErrorHandler, maxWriteTimeCounter);
                CloseHelper.close(countedErrorHandler, totalReadBytesCounter);
                CloseHelper.close(countedErrorHandler, totalReadTimeCounter);
                CloseHelper.close(countedErrorHandler, maxReadTimeCounter);
                closeDutyCycleCounters(conductorDutyCycleTracker);
                closeDutyCycleCounters(recorderDutyCycleTracker);
                closeDutyCycleCounters(replayerDutyCycleTracker);
                CloseHelper.close(errorCounter);
            }

            if (errorHandler instanceof AutoCloseable)
            {
                CloseHelper.close((AutoCloseable)errorHandler);
            }
            CloseHelper.close(markFile);
        }

        private void closeDutyCycleCounters(final DutyCycleTracker dutyCycleTracker)
        {
            if (dutyCycleTracker instanceof DutyCycleStallTracker)
            {
                final DutyCycleStallTracker dutyCycleStallTracker = (DutyCycleStallTracker)dutyCycleTracker;
                CloseHelper.close(countedErrorHandler, dutyCycleStallTracker.maxCycleTime());
                CloseHelper.close(countedErrorHandler, dutyCycleStallTracker.cycleTimeThresholdExceededCount());
            }
        }

        private void concludeArchiveId()
        {
            if (NULL_VALUE == archiveId)
            {
                if (null != aeron)
                {
                    archiveId = aeron.clientId();
                }
                else
                {
                    archiveId = CommonContext.nextCorrelationId(
                        new File(aeronDirectoryName), epochClock, new CommonContext().driverTimeoutMs());
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return "Archive.Context" +
                "\n{" +
                "\n    isConcluded=" + isConcluded() +
                "\n    deleteArchiveOnStart=" + deleteArchiveOnStart +
                "\n    ownsAeronClient=" + ownsAeronClient +
                "\n    aeronDirectoryName='" + aeronDirectoryName + '\'' +
                "\n    aeron=" + aeron +
                "\n    archiveDir=" + archiveDir +
                "\n    archiveDirectoryName='" + archiveDirectoryName + '\'' +
                "\n    archiveDirChannel=" + archiveDirChannel +
                "\n    archiveFileStore=" + archiveFileStore +
                "\n    archiveId=" + archiveId +
                "\n    catalog=" + catalog +
                "\n    markFile=" + markFile +
                "\n    archiveClientContext=" + archiveClientContext +
                "\n    mediaDriverAgentInvoker=" + mediaDriverAgentInvoker +
                "\n    controlChannel='" + controlChannel + '\'' +
                "\n    controlStreamId=" + controlStreamId +
                "\n    localControlChannel='" + localControlChannel + '\'' +
                "\n    localControlStreamId=" + localControlStreamId +
                "\n    controlTermBufferSparse=" + controlTermBufferSparse +
                "\n    controlTermBufferLength=" + controlTermBufferLength +
                "\n    controlMtuLength=" + controlMtuLength +
                "\n    recordingEventsChannel='" + recordingEventsChannel + '\'' +
                "\n    recordingEventsStreamId=" + recordingEventsStreamId +
                "\n    recordingEventsEnabled=" + recordingEventsEnabled +
                "\n    replicationChannel='" + replicationChannel + '\'' +
                "\n    connectTimeoutNs=" + connectTimeoutNs +
                "\n    sessionLivenessCheckIntervalNs=" + sessionLivenessCheckIntervalNs +
                "\n    replayLingerTimeoutNs=" + replayLingerTimeoutNs +
                "\n    maxCatalogEntries=" + -1 +
                "\n    catalogCapacity=" + catalogCapacity +
                "\n    lowStorageSpaceThreshold=" + lowStorageSpaceThreshold +
                "\n    segmentFileLength=" + segmentFileLength +
                "\n    fileSyncLevel=" + fileSyncLevel +
                "\n    catalogFileSyncLevel=" + catalogFileSyncLevel +
                "\n    maxConcurrentRecordings=" + maxConcurrentRecordings +
                "\n    maxConcurrentReplays=" + maxConcurrentReplays +
                "\n    fileIoMaxLength=" + fileIoMaxLength +
                "\n    threadingMode=" + threadingMode +
                "\n    threadFactory=" + threadFactory +
                "\n    abortLatch=" + abortLatch +
                "\n    idleStrategySupplier=" + idleStrategySupplier +
                "\n    replayerIdleStrategySupplier=" + replayerIdleStrategySupplier +
                "\n    recorderIdleStrategySupplier=" + recorderIdleStrategySupplier +
                "\n    epochClock=" + epochClock +
                "\n    authenticatorSupplier=" + authenticatorSupplier +
                "\n    controlSessionsCounter=" + controlSessionsCounter +
                "\n    recordingSessionCounter=" + recordingSessionCounter +
                "\n    replaySessionCounter=" + replaySessionCounter +
                "\n    errorBufferLength=" + errorBufferLength +
                "\n    errorHandler=" + errorHandler +
                "\n    errorCounter=" + errorCounter +
                "\n    countedErrorHandler=" + countedErrorHandler +
                "\n    recordChecksum=" + recordChecksum +
                "\n    replayChecksum=" + replayChecksum +
                "\n    dataBuffer=" + dataBuffer +
                "\n    replayBuffer=" + replayBuffer +
                "\n    recordChecksumBuffer=" + recordChecksumBuffer +
                "\n    conductorCycleThresholdNs=" + conductorCycleThresholdNs +
                "\n    recorderCycleThresholdNs=" + recorderCycleThresholdNs +
                "\n    replayerCycleThresholdNs=" + replayerCycleThresholdNs +
                "\n    conductorDutyCycleTracker=" + conductorDutyCycleTracker +
                "\n    recorderDutyCycleTracker=" + recorderDutyCycleTracker +
                "\n    replayerDutyCycleTracker=" + replayerDutyCycleTracker +
                "\n    totalWriteBytesCounter=" + totalWriteBytesCounter +
                "\n    totalWriteTimeCounter=" + totalWriteTimeCounter +
                "\n    maxWriteTimeCounter=" + maxWriteTimeCounter +
                "\n    totalReadBytesCounter=" + totalReadBytesCounter +
                "\n    totalReadTimeCounter=" + totalReadTimeCounter +
                "\n    maxReadTimeCounter=" + maxReadTimeCounter +
                "\n}";
        }
    }

    /**
     * The filename to be used for a segment file based on recording id and position the segment begins.
     *
     * @param recordingId         to identify the recorded stream.
     * @param segmentBasePosition at which the segment file begins.
     * @return the filename to be used for a segment file based on recording id and position the segment begins.
     */
    static String segmentFileName(final long recordingId, final long segmentBasePosition)
    {
        return recordingId + "-" + segmentBasePosition + Configuration.RECORDING_SEGMENT_SUFFIX;
    }

    /**
     * Get the {@link FileChannel} for the parent directory for the recordings and catalog, so it can be sync'ed
     * to storage when new files are created.
     *
     * @param directory     which will store the files created by the archive.
     * @param fileSyncLevel to be applied for file updates, {@link Archive.Configuration#FILE_SYNC_LEVEL_PROP_NAME}.
     * @return the {@link FileChannel} for the parent directory for the recordings and catalog if fileSyncLevel
     * greater than zero otherwise null.
     */
    static FileChannel channelForDirectorySync(final File directory, final int fileSyncLevel)
    {
        if (fileSyncLevel > 0)
        {
            try
            {
                return FileChannel.open(directory.toPath());
            }
            catch (final IOException ignore)
            {
            }
        }

        return null;
    }
}
