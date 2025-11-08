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
import io.aeron.AeronCounters;
import io.aeron.CommonContext;
import io.aeron.RethrowingErrorHandler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.AppVersionValidator;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.mark.ClusterComponentType;
import io.aeron.cluster.codecs.mark.MarkFileHeaderEncoder;
import io.aeron.config.Config;
import io.aeron.config.DefaultType;
import io.aeron.driver.DutyCycleTracker;
import io.aeron.driver.status.DutyCycleStallTracker;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.version.Versioned;
import org.agrona.CloseHelper;
import org.agrona.DelegatingErrorHandler;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.MarkFile;
import org.agrona.SemanticVersion;
import org.agrona.Strings;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.StatusIndicator;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.aeron.ChannelUri.addAliasIfAbsent;
import static io.aeron.CommonContext.driverFilePageSize;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.LIVENESS_TIMEOUT_MS;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.SERVICE_NAME_PROP_NAME;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.agrona.SystemUtil.getDurationInNanos;
import static org.agrona.SystemUtil.getSizeAsInt;
import static org.agrona.SystemUtil.loadPropertiesFiles;

/**
 * Container for a service in the cluster managed by the Consensus Module. This is where business logic resides and
 * loaded via {@link ClusteredServiceContainer.Configuration#SERVICE_CLASS_NAME_PROP_NAME} or
 * {@link ClusteredServiceContainer.Context#clusteredService(ClusteredService)}.
 */
@Versioned
public final class ClusteredServiceContainer implements AutoCloseable
{
    /**
     * Launch the clustered service container and await a shutdown signal.
     *
     * @param args command line argument which is a list for properties files as URLs or filenames.
     */
    @SuppressWarnings("try")
    public static void main(final String[] args)
    {
        loadPropertiesFiles(args);

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
            ClusteredServiceContainer ignore = launch(new Context().terminationHook(barrier::signalAll)))
        {
            barrier.await();

            System.out.println("Shutdown ClusteredServiceContainer...");
        }
    }

    private final Context ctx;
    private final AgentRunner serviceAgentRunner;

    private ClusteredServiceContainer(final Context ctx)
    {
        this.ctx = ctx;

        try
        {
            ctx.conclude();
        }
        catch (final Exception ex)
        {
            final ClusterMarkFile markFile = ctx.markFile;
            if (null != markFile)
            {
                markFile.signalFailedStart();
            }

            ctx.close();
            throw ex;
        }

        final ClusteredServiceAgent agent = new ClusteredServiceAgent(ctx);
        serviceAgentRunner = new AgentRunner(ctx.idleStrategy(), ctx.errorHandler(), ctx.errorCounter(), agent);
    }

    /**
     * Launch an ClusteredServiceContainer using a default configuration.
     *
     * @return a new instance of a ClusteredServiceContainer.
     */
    public static ClusteredServiceContainer launch()
    {
        return launch(new Context());
    }

    /**
     * Launch a ClusteredServiceContainer by providing a configuration context.
     *
     * @param ctx for the configuration parameters.
     * @return a new instance of a ClusteredServiceContainer.
     */
    public static ClusteredServiceContainer launch(final Context ctx)
    {
        final ClusteredServiceContainer clusteredServiceContainer = new ClusteredServiceContainer(ctx);
        AgentRunner.startOnThread(clusteredServiceContainer.serviceAgentRunner, ctx.threadFactory());

        return clusteredServiceContainer;
    }

    /**
     * Get the {@link Context} that is used by this {@link ClusteredServiceContainer}.
     *
     * @return the {@link Context} that is used by this {@link ClusteredServiceContainer}.
     */
    public Context context()
    {
        return ctx;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.close(serviceAgentRunner);
    }

    /**
     * Configuration options for the consensus module and service container within a cluster.
     */
    @Config(existsInC = false)
    public static final class Configuration
    {
        /**
         * Type of snapshot for this service.
         */
        public static final long SNAPSHOT_TYPE_ID = 2;

        /**
         * Update interval for cluster mark file in nanoseconds.
         */
        public static final long MARK_FILE_UPDATE_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);

        /**
         * Timeout in milliseconds to detect liveness.
         */
        public static final long LIVENESS_TIMEOUT_MS = 10 * TimeUnit.NANOSECONDS.toMillis(MARK_FILE_UPDATE_INTERVAL_NS);

        /**
         * Property name for the identity of the cluster instance.
         */
        @Config
        public static final String CLUSTER_ID_PROP_NAME = "aeron.cluster.id";

        /**
         * Default identity for a clustered instance.
         */
        @Config
        public static final int CLUSTER_ID_DEFAULT = 0;

        /**
         * Identity for a clustered service. Services should be numbered from 0 and be contiguous.
         */
        @Config
        public static final String SERVICE_ID_PROP_NAME = "aeron.cluster.service.id";

        /**
         * Default identity for a clustered service.
         */
        @Config
        public static final int SERVICE_ID_DEFAULT = 0;

        /**
         * Name for a clustered service to be the role of the {@link Agent}.
         */
        @Config
        public static final String SERVICE_NAME_PROP_NAME = "aeron.cluster.service.name";

        /**
         * Name for a clustered service to be the role of the {@link Agent}.
         */
        @Config
        public static final String SERVICE_NAME_DEFAULT = "clustered-service";

        /**
         * Class name for dynamically loading a {@link ClusteredService}. This is used if
         * {@link Context#clusteredService()} is not set.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String SERVICE_CLASS_NAME_PROP_NAME = "aeron.cluster.service.class.name";

        /**
         * Channel to be used for log or snapshot replay on startup.
         */
        @Config
        public static final String REPLAY_CHANNEL_PROP_NAME = "aeron.cluster.replay.channel";

        /**
         * Default channel to be used for log or snapshot replay on startup.
         */
        @Config
        public static final String REPLAY_CHANNEL_DEFAULT = CommonContext.IPC_CHANNEL;

        /**
         * Stream id within a channel for the clustered log or snapshot replay.
         */
        @Config
        public static final String REPLAY_STREAM_ID_PROP_NAME = "aeron.cluster.replay.stream.id";

        /**
         * Default stream id for the log or snapshot replay within a channel.
         */
        @Config
        public static final int REPLAY_STREAM_ID_DEFAULT = 103;

        /**
         * Channel for control communications between the local consensus module and services.
         */
        @Config
        public static final String CONTROL_CHANNEL_PROP_NAME = "aeron.cluster.control.channel";

        /**
         * Default channel for communications between the local consensus module and services. This should be IPC.
         */
        @Config
        public static final String CONTROL_CHANNEL_DEFAULT = "aeron:ipc?term-length=128k";

        /**
         * Stream id within the control channel for communications from the consensus module to the services.
         */
        @Config
        public static final String SERVICE_STREAM_ID_PROP_NAME = "aeron.cluster.service.stream.id";

        /**
         * Default stream id within the control channel for communications from the consensus module.
         */
        @Config
        public static final int SERVICE_STREAM_ID_DEFAULT = 104;

        /**
         * Stream id within the control channel for communications from the services to the consensus module.
         */
        @Config
        public static final String CONSENSUS_MODULE_STREAM_ID_PROP_NAME = "aeron.cluster.consensus.module.stream.id";

        /**
         * Default stream id within a channel for communications from the services to the consensus module.
         */
        @Config
        public static final int CONSENSUS_MODULE_STREAM_ID_DEFAULT = 105;

        /**
         * Channel to be used for archiving snapshots.
         */
        @Config
        public static final String SNAPSHOT_CHANNEL_PROP_NAME = "aeron.cluster.snapshot.channel";

        /**
         * Default channel to be used for archiving snapshots.
         */
        @Config
        public static final String SNAPSHOT_CHANNEL_DEFAULT = "aeron:ipc?alias=snapshot";

        /**
         * Stream id within a channel for archiving snapshots.
         */
        @Config
        public static final String SNAPSHOT_STREAM_ID_PROP_NAME = "aeron.cluster.snapshot.stream.id";

        /**
         * Default stream id for the archived snapshots within a channel.
         */
        @Config
        public static final int SNAPSHOT_STREAM_ID_DEFAULT = 106;

        /**
         * Directory to use for the aeron cluster.
         */
        @Config
        public static final String CLUSTER_DIR_PROP_NAME = "aeron.cluster.dir";

        /**
         * Default directory to use for the aeron cluster.
         */
        @Config
        public static final String CLUSTER_DIR_DEFAULT = "aeron-cluster";

        /**
         * Directory to use for the aeron cluster services, will default to
         * {@link io.aeron.cluster.ConsensusModule.Context#clusterDir()} if not specified.
         */
        @Config(defaultType = DefaultType.STRING)
        public static final String CLUSTER_SERVICES_DIR_PROP_NAME = "aeron.cluster.services.dir";

        /**
         * Directory to use for the Cluster component's mark file.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String MARK_FILE_DIR_PROP_NAME = "aeron.cluster.mark.file.dir";

        /**
         * Length in bytes of the error buffer for the cluster container.
         */
        @Config(id = "SERVICE_ERROR_BUFFER_LENGTH")
        public static final String ERROR_BUFFER_LENGTH_PROP_NAME = "aeron.cluster.service.error.buffer.length";

        /**
         * Default length in bytes of the error buffer for the cluster container.
         */
        @Config(id = "SERVICE_ERROR_BUFFER_LENGTH")
        public static final int ERROR_BUFFER_LENGTH_DEFAULT = 1024 * 1024;

        /**
         * Is this a responding service to client requests property.
         */
        @Config
        public static final String RESPONDER_SERVICE_PROP_NAME = "aeron.cluster.service.responder";

        /**
         * Default to true that this a responding service to client requests.
         */
        @Config
        public static final boolean RESPONDER_SERVICE_DEFAULT = true;

        /**
         * Fragment limit to use when polling the log.
         */
        @Config
        public static final String LOG_FRAGMENT_LIMIT_PROP_NAME = "aeron.cluster.log.fragment.limit";

        /**
         * Default fragment limit for polling log.
         */
        @Config
        public static final int LOG_FRAGMENT_LIMIT_DEFAULT = 50;

        /**
         * Delegating {@link ErrorHandler} which will be first in the chain before delegating to the
         * {@link Context#errorHandler()}.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String DELEGATING_ERROR_HANDLER_PROP_NAME =
            "aeron.cluster.service.delegating.error.handler";

        /**
         * Property name for threshold value for the container work cycle threshold to track
         * for being exceeded.
         */
        @Config(id = "SERVICE_CYCLE_THRESHOLD")
        public static final String CYCLE_THRESHOLD_PROP_NAME = "aeron.cluster.service.cycle.threshold";

        /**
         * Default threshold value for the container work cycle threshold to track for being exceeded.
         */
        @Config(
            id = "SERVICE_CYCLE_THRESHOLD",
            defaultType = DefaultType.LONG,
            defaultLong = 1_000_000L)
        public static final long CYCLE_THRESHOLD_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(1);

        /**
         * Property name for threshold value, which is used for tracking snapshot duration breaches.
         *
         * @since 1.44.0
         */
        @Config
        public static final String SNAPSHOT_DURATION_THRESHOLD_PROP_NAME = "aeron.cluster.service.snapshot.threshold";

        /**
         * Default threshold value, which is used for tracking snapshot duration breaches.
         *
         * @since 1.44.0
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 1000L * 1000 * 1000)
        public static final long SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(1000);

        /**
         * Counter type id for the cluster node role.
         */
        public static final int CLUSTER_NODE_ROLE_TYPE_ID = AeronCounters.CLUSTER_NODE_ROLE_TYPE_ID;

        /**
         * Counter type id of the commit position.
         */
        public static final int COMMIT_POSITION_TYPE_ID = AeronCounters.CLUSTER_COMMIT_POSITION_TYPE_ID;

        /**
         * Counter type id for the clustered service error count.
         */
        public static final int CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID =
            AeronCounters.CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID;

        /**
         * The value {@link #CLUSTER_ID_DEFAULT} or system property {@link #CLUSTER_ID_PROP_NAME} if set.
         *
         * @return {@link #CLUSTER_ID_DEFAULT} or system property {@link #CLUSTER_ID_PROP_NAME} if set.
         */
        public static int clusterId()
        {
            return Integer.getInteger(CLUSTER_ID_PROP_NAME, CLUSTER_ID_DEFAULT);
        }

        /**
         * The value {@link #SERVICE_ID_DEFAULT} or system property {@link #SERVICE_ID_PROP_NAME} if set.
         *
         * @return {@link #SERVICE_ID_DEFAULT} or system property {@link #SERVICE_ID_PROP_NAME} if set.
         */
        public static int serviceId()
        {
            return Integer.getInteger(SERVICE_ID_PROP_NAME, SERVICE_ID_DEFAULT);
        }

        /**
         * The value {@link #SERVICE_NAME_DEFAULT} or system property {@link #SERVICE_NAME_PROP_NAME} if set.
         *
         * @return {@link #SERVICE_NAME_DEFAULT} or system property {@link #SERVICE_NAME_PROP_NAME} if set.
         */
        public static String serviceName()
        {
            return System.getProperty(SERVICE_NAME_PROP_NAME, SERVICE_NAME_DEFAULT);
        }

        /**
         * The value {@link #REPLAY_CHANNEL_DEFAULT} or system property {@link #REPLAY_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #REPLAY_CHANNEL_DEFAULT} or system property {@link #REPLAY_CHANNEL_PROP_NAME} if set.
         */
        public static String replayChannel()
        {
            return System.getProperty(REPLAY_CHANNEL_PROP_NAME, REPLAY_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #REPLAY_STREAM_ID_DEFAULT} or system property {@link #REPLAY_STREAM_ID_PROP_NAME}
         * if set.
         *
         * @return {@link #REPLAY_STREAM_ID_DEFAULT} or system property {@link #REPLAY_STREAM_ID_PROP_NAME}
         * if set.
         */
        public static int replayStreamId()
        {
            return Integer.getInteger(REPLAY_STREAM_ID_PROP_NAME, REPLAY_STREAM_ID_DEFAULT);
        }

        /**
         * The value {@link #CONTROL_CHANNEL_DEFAULT} or system property
         * {@link #CONTROL_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #CONTROL_CHANNEL_DEFAULT} or system property
         * {@link #CONTROL_CHANNEL_PROP_NAME} if set.
         */
        public static String controlChannel()
        {
            return System.getProperty(CONTROL_CHANNEL_PROP_NAME, CONTROL_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #CONSENSUS_MODULE_STREAM_ID_DEFAULT} or system property
         * {@link #CONSENSUS_MODULE_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #CONSENSUS_MODULE_STREAM_ID_DEFAULT} or system property
         * {@link #CONSENSUS_MODULE_STREAM_ID_PROP_NAME} if set.
         */
        public static int consensusModuleStreamId()
        {
            return Integer.getInteger(CONSENSUS_MODULE_STREAM_ID_PROP_NAME, CONSENSUS_MODULE_STREAM_ID_DEFAULT);
        }

        /**
         * The value {@link #SERVICE_STREAM_ID_DEFAULT} or system property
         * {@link #SERVICE_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #SERVICE_STREAM_ID_DEFAULT} or system property
         * {@link #SERVICE_STREAM_ID_PROP_NAME} if set.
         */
        public static int serviceStreamId()
        {
            return Integer.getInteger(SERVICE_STREAM_ID_PROP_NAME, SERVICE_STREAM_ID_DEFAULT);
        }

        /**
         * The value {@link #SNAPSHOT_CHANNEL_DEFAULT} or system property {@link #SNAPSHOT_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #SNAPSHOT_CHANNEL_DEFAULT} or system property {@link #SNAPSHOT_CHANNEL_PROP_NAME} if set.
         */
        public static String snapshotChannel()
        {
            return System.getProperty(SNAPSHOT_CHANNEL_PROP_NAME, SNAPSHOT_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #SNAPSHOT_STREAM_ID_DEFAULT} or system property {@link #SNAPSHOT_STREAM_ID_PROP_NAME}
         * if set.
         *
         * @return {@link #SNAPSHOT_STREAM_ID_DEFAULT} or system property {@link #SNAPSHOT_STREAM_ID_PROP_NAME} if set.
         */
        public static int snapshotStreamId()
        {
            return Integer.getInteger(SNAPSHOT_STREAM_ID_PROP_NAME, SNAPSHOT_STREAM_ID_DEFAULT);
        }

        /**
         * Default {@link IdleStrategy} to be employed for cluster agents.
         */
        @Config(id = "CLUSTER_IDLE_STRATEGY")
        public static final String DEFAULT_IDLE_STRATEGY = "org.agrona.concurrent.BackoffIdleStrategy";

        /**
         * {@link IdleStrategy} to be employed for cluster agents.
         */
        @Config
        public static final String CLUSTER_IDLE_STRATEGY_PROP_NAME = "aeron.cluster.idle.strategy";

        /**
         * Property to configure if this node should take standby snapshots. The default for this property is
         * <code>false</code>.
         */
        @Config(defaultType = DefaultType.BOOLEAN, defaultBoolean = false)
        public static final String STANDBY_SNAPSHOT_ENABLED_PROP_NAME = "aeron.cluster.standby.snapshot.enabled";

        /**
         * Create a supplier of {@link IdleStrategy}s that will use the system property.
         *
         * @param controllableStatus if a {@link org.agrona.concurrent.ControllableIdleStrategy} is required.
         * @return the new idle strategy
         */
        public static Supplier<IdleStrategy> idleStrategySupplier(final StatusIndicator controllableStatus)
        {
            return () ->
            {
                final String name = System.getProperty(CLUSTER_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);
                return io.aeron.driver.Configuration.agentIdleStrategy(name, controllableStatus);
            };
        }

        /**
         * The value {@link #CLUSTER_DIR_DEFAULT} or system property {@link #CLUSTER_DIR_PROP_NAME} if set.
         *
         * @return {@link #CLUSTER_DIR_DEFAULT} or system property {@link #CLUSTER_DIR_PROP_NAME} if set.
         */
        public static String clusterDirName()
        {
            return System.getProperty(CLUSTER_DIR_PROP_NAME, CLUSTER_DIR_DEFAULT);
        }

        /**
         * The value of system property {@link #CLUSTER_DIR_PROP_NAME} if set or null.
         *
         * @return {@link #CLUSTER_DIR_PROP_NAME} if set or null.
         */
        public static String clusterServicesDirName()
        {
            return System.getProperty(CLUSTER_SERVICES_DIR_PROP_NAME);
        }

        /**
         * Size in bytes of the error buffer in the mark file.
         *
         * @return length of error buffer in bytes.
         * @see #ERROR_BUFFER_LENGTH_PROP_NAME
         */
        public static int errorBufferLength()
        {
            return getSizeAsInt(ERROR_BUFFER_LENGTH_PROP_NAME, ERROR_BUFFER_LENGTH_DEFAULT);
        }

        /**
         * The value {@link #RESPONDER_SERVICE_DEFAULT} or system property {@link #RESPONDER_SERVICE_PROP_NAME} if set.
         *
         * @return {@link #RESPONDER_SERVICE_DEFAULT} or system property {@link #RESPONDER_SERVICE_PROP_NAME} if set.
         */
        public static boolean isRespondingService()
        {
            final String property = System.getProperty(RESPONDER_SERVICE_PROP_NAME);
            if (null == property)
            {
                return RESPONDER_SERVICE_DEFAULT;
            }

            return "true".equals(property);
        }

        /**
         * The value {@link #LOG_FRAGMENT_LIMIT_DEFAULT} or system property
         * {@link #LOG_FRAGMENT_LIMIT_PROP_NAME} if set.
         *
         * @return {@link #LOG_FRAGMENT_LIMIT_DEFAULT} or system property
         * {@link #LOG_FRAGMENT_LIMIT_PROP_NAME} if set.
         */
        public static int logFragmentLimit()
        {
            return Integer.getInteger(LOG_FRAGMENT_LIMIT_PROP_NAME, LOG_FRAGMENT_LIMIT_DEFAULT);
        }

        /**
         * Get threshold value for the container work cycle threshold to track for being exceeded.
         *
         * @return threshold value in nanoseconds.
         */
        public static long cycleThresholdNs()
        {
            return getDurationInNanos(CYCLE_THRESHOLD_PROP_NAME, CYCLE_THRESHOLD_DEFAULT_NS);
        }

        /**
         * Get threshold value, which is used for monitoring snapshot duration breaches of its predefined
         * threshold.
         *
         * @return threshold value in nanoseconds.
         */
        public static long snapshotDurationThresholdNs()
        {
            return getDurationInNanos(SNAPSHOT_DURATION_THRESHOLD_PROP_NAME, SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS);
        }

        /**
         * Get the configuration value to determine if this node should take standby snapshots be enabled.
         *
         * @return configuration value for standby snapshots being enabled.
         */
        public static boolean standbySnapshotEnabled()
        {
            return Boolean.getBoolean(STANDBY_SNAPSHOT_ENABLED_PROP_NAME);
        }

        /**
         * Create a new {@link ClusteredService} based on the configured {@link #SERVICE_CLASS_NAME_PROP_NAME}.
         *
         * @return a new {@link ClusteredService} based on the configured {@link #SERVICE_CLASS_NAME_PROP_NAME}.
         */
        public static ClusteredService newClusteredService()
        {
            final String className = System.getProperty(Configuration.SERVICE_CLASS_NAME_PROP_NAME);
            if (null == className)
            {
                throw new ClusterException("either a instance or class name for the service must be provided");
            }

            try
            {
                return (ClusteredService)Class.forName(className).getConstructor().newInstance();
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
                return null;
            }
        }

        /**
         * Create a new {@link DelegatingErrorHandler} defined by {@link #DELEGATING_ERROR_HANDLER_PROP_NAME}.
         *
         * @return a new {@link DelegatingErrorHandler} defined by {@link #DELEGATING_ERROR_HANDLER_PROP_NAME} or
         * null if property not set.
         */
        public static DelegatingErrorHandler newDelegatingErrorHandler()
        {
            final String className = System.getProperty(Configuration.DELEGATING_ERROR_HANDLER_PROP_NAME);
            if (null != className)
            {
                try
                {
                    return (DelegatingErrorHandler)Class.forName(className).getConstructor().newInstance();
                }
                catch (final Exception ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }
            }

            return null;
        }

        /**
         * Get the alternative directory to be used for storing the Cluster component's mark file.
         *
         * @return the directory to be used for storing the archive mark file.
         */
        public static String markFileDir()
        {
            return System.getProperty(MARK_FILE_DIR_PROP_NAME);
        }
    }

    /**
     * The context will be owned by {@link ClusteredServiceAgent} after a successful
     * {@link ClusteredServiceContainer#launch(Context)} and closed via {@link ClusteredServiceContainer#close()}.
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

        private volatile boolean isConcluded;
        private int appVersion = SemanticVersion.compose(0, 0, 1);
        private int clusterId = Configuration.clusterId();
        private int serviceId = Configuration.serviceId();
        private String serviceName = System.getProperty(SERVICE_NAME_PROP_NAME);
        private String replayChannel = Configuration.replayChannel();
        private int replayStreamId = Configuration.replayStreamId();
        private String controlChannel = Configuration.controlChannel();
        private int consensusModuleStreamId = Configuration.consensusModuleStreamId();
        private int serviceStreamId = Configuration.serviceStreamId();
        private String snapshotChannel = Configuration.snapshotChannel();
        private int snapshotStreamId = Configuration.snapshotStreamId();
        private int errorBufferLength = Configuration.errorBufferLength();
        private boolean isRespondingService = Configuration.isRespondingService();
        private int logFragmentLimit = Configuration.logFragmentLimit();
        private long cycleThresholdNs = Configuration.cycleThresholdNs();
        private long snapshotDurationThresholdNs = Configuration.snapshotDurationThresholdNs();
        private boolean standbySnapshotEnabled = Configuration.standbySnapshotEnabled();

        private CountDownLatch abortLatch;
        private ThreadFactory threadFactory;
        private Supplier<IdleStrategy> idleStrategySupplier;
        private EpochClock epochClock;
        private NanoClock nanoClock;
        private DistinctErrorLog errorLog;
        private ErrorHandler errorHandler;
        private DelegatingErrorHandler delegatingErrorHandler;
        private AtomicCounter errorCounter;
        private CountedErrorHandler countedErrorHandler;
        private AeronArchive.Context archiveContext;
        private String clusterDirectoryName = Configuration.clusterDirName();
        private File clusterDir;
        private File markFileDir;
        private String aeronDirectoryName = CommonContext.getAeronDirectoryName();
        private Aeron aeron;
        private DutyCycleTracker dutyCycleTracker;
        private SnapshotDurationTracker snapshotDurationTracker;
        private AppVersionValidator appVersionValidator;
        private boolean ownsAeronClient;

        private ClusteredService clusteredService;
        private Runnable terminationHook;
        private ClusterMarkFile markFile;

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
         * Conclude configuration by setting up defaults when specifics are not provided.
         */
        @SuppressWarnings("MethodLength")
        public void conclude()
        {
            if ((boolean)IS_CONCLUDED_VH.getAndSet(this, true))
            {
                throw new ConcurrentConcludeException();
            }

            if (serviceId < 0 || serviceId > 127)
            {
                throw new ConfigurationException("service id outside allowed range (0-127): " + serviceId);
            }

            if (null == threadFactory)
            {
                threadFactory = Thread::new;
            }

            if (null == idleStrategySupplier)
            {
                idleStrategySupplier = Configuration.idleStrategySupplier(null);
            }

            if (null == appVersionValidator)
            {
                appVersionValidator = AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR;
            }

            if (null == epochClock)
            {
                epochClock = SystemEpochClock.INSTANCE;
            }

            if (null == nanoClock)
            {
                nanoClock = SystemNanoClock.INSTANCE;
            }

            if (null == clusterDir)
            {
                clusterDir = new File(clusterDirectoryName);
            }

            if (null == markFileDir)
            {
                final String dir = Configuration.markFileDir();
                markFileDir = Strings.isEmpty(dir) ? clusterDir : new File(dir);
            }

            try
            {
                clusterDir = clusterDir.getCanonicalFile();
                clusterDirectoryName = clusterDir.getAbsolutePath();
                markFileDir = markFileDir.getCanonicalFile();
            }
            catch (final IOException e)
            {
                throw new UncheckedIOException(e);
            }

            IoUtil.ensureDirectoryExists(clusterDir, "cluster");
            IoUtil.ensureDirectoryExists(markFileDir, "mark file");

            if (null == markFile)
            {
                final int filePageSize = null != aeron ? aeron.context().filePageSize() :
                    driverFilePageSize(new File(aeronDirectoryName), epochClock, new CommonContext().driverTimeoutMs());
                markFile = new ClusterMarkFile(
                    new File(markFileDir, ClusterMarkFile.markFilenameForService(serviceId)),
                    ClusterComponentType.CONTAINER,
                    errorBufferLength,
                    epochClock,
                    LIVENESS_TIMEOUT_MS,
                    filePageSize);
            }

            MarkFile.ensureMarkFileLink(
                clusterDir,
                new File(markFile.parentDirectory(), ClusterMarkFile.markFilenameForService(serviceId)),
                ClusterMarkFile.linkFilenameForService(serviceId));

            if (null == errorLog)
            {
                errorLog = new DistinctErrorLog(markFile.errorBuffer(), epochClock, US_ASCII);
            }

            errorHandler = CommonContext.setupErrorHandler(this.errorHandler, errorLog);

            if (null == delegatingErrorHandler)
            {
                delegatingErrorHandler = Configuration.newDelegatingErrorHandler();
                if (null != delegatingErrorHandler)
                {
                    delegatingErrorHandler.next(errorHandler);
                    errorHandler = delegatingErrorHandler;
                }
            }
            else
            {
                delegatingErrorHandler.next(errorHandler);
                errorHandler = delegatingErrorHandler;
            }

            if (Strings.isEmpty(serviceName))
            {
                serviceName = "clustered-service-" + clusterId + "-" + serviceId;
            }

            if (null == aeron)
            {
                aeron = Aeron.connect(
                    new Aeron.Context()
                        .aeronDirectoryName(aeronDirectoryName)
                        .errorHandler(errorHandler)
                        .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE)
                        .awaitingIdleStrategy(YieldingIdleStrategy.INSTANCE)
                        .epochClock(epochClock)
                        .clientName(serviceName));

                ownsAeronClient = true;
            }

            if (!(aeron.context().subscriberErrorHandler() instanceof RethrowingErrorHandler))
            {
                throw new ClusterException("Aeron client must use a RethrowingErrorHandler");
            }

            final ExpandableArrayBuffer tempBuffer = new ExpandableArrayBuffer();
            if (null == errorCounter)
            {
                errorCounter = ClusterCounters.allocateServiceErrorCounter(aeron, tempBuffer, clusterId, serviceId);
            }

            if (null == countedErrorHandler)
            {
                countedErrorHandler = new CountedErrorHandler(errorHandler, errorCounter);
                if (ownsAeronClient)
                {
                    aeron.context().errorHandler(countedErrorHandler);
                }
            }

            if (null == dutyCycleTracker)
            {
                dutyCycleTracker = new DutyCycleStallTracker(
                    ClusterCounters.allocateServiceCounter(
                        aeron,
                        tempBuffer,
                        "Cluster container max cycle time in ns",
                        AeronCounters.CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID,
                        clusterId,
                        serviceId),
                    ClusterCounters.allocateServiceCounter(
                        aeron,
                        tempBuffer,
                        "Cluster container work cycle time exceeded count: threshold=" + cycleThresholdNs + "ns",
                        AeronCounters.CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID,
                        clusterId,
                        serviceId),
                    cycleThresholdNs);
            }

            if (null == snapshotDurationTracker)
            {
                snapshotDurationTracker = new SnapshotDurationTracker(
                    ClusterCounters.allocateServiceCounter(
                        aeron,
                        tempBuffer,
                        "Clustered service max snapshot duration in ns",
                        AeronCounters.CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID,
                        clusterId,
                        serviceId
                    ),
                    ClusterCounters.allocateServiceCounter(
                        aeron,
                        tempBuffer,
                        "Clustered service max snapshot duration exceeded count: threshold=" +
                            snapshotDurationThresholdNs,
                        AeronCounters.CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID,
                        clusterId,
                        serviceId
                    ),
                    snapshotDurationThresholdNs);
            }

            if (null == archiveContext)
            {
                archiveContext = new AeronArchive.Context()
                    .controlRequestChannel(AeronArchive.Configuration.localControlChannel())
                    .controlResponseChannel(AeronArchive.Configuration.localControlChannel())
                    .controlRequestStreamId(AeronArchive.Configuration.localControlStreamId())
                    .controlResponseStreamId(
                        clusterId * 100 + 100 + AeronArchive.Configuration.controlResponseStreamId() + (serviceId + 1));
            }

            if (!archiveContext.controlRequestChannel().startsWith(CommonContext.IPC_CHANNEL))
            {
                throw new ClusterException("local archive control must be IPC");
            }

            if (!archiveContext.controlResponseChannel().startsWith(CommonContext.IPC_CHANNEL))
            {
                throw new ClusterException("local archive control must be IPC");
            }

            archiveContext
                .aeron(aeron)
                .ownsAeronClient(false)
                .lock(NoOpLock.INSTANCE)
                .errorHandler(countedErrorHandler)
                .controlRequestChannel(addAliasIfAbsent(
                archiveContext.controlRequestChannel(),
                "sc-" + serviceId + "-archive-ctrl-req-cluster-" + clusterId))
                .controlResponseChannel(addAliasIfAbsent(
                archiveContext.controlResponseChannel(),
                "sc-" + serviceId + "-archive-ctrl-resp-cluster-" + clusterId))
                .clientName(serviceName);

            if (null == terminationHook)
            {
                terminationHook = () -> {};
            }

            if (null == clusteredService)
            {
                clusteredService = Configuration.newClusteredService();
            }

            abortLatch = new CountDownLatch(!aeron.context().useConductorAgentInvoker() ? 1 : 0);
            concludeMarkFile();

            if (CommonContext.shouldPrintConfigurationOnStart())
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
         * User assigned application version which appended to the log as the appVersion in new leadership events.
         * <p>
         * This can be validated using {@link org.agrona.SemanticVersion} to ensure only application nodes of the same
         * major version communicate with each other.
         *
         * @param appVersion for user application.
         * @return this for a fluent API.
         */
        public Context appVersion(final int appVersion)
        {
            this.appVersion = appVersion;
            return this;
        }

        /**
         * User assigned application version which appended to the log as the appVersion in new leadership events.
         * <p>
         * This can be validated using {@link org.agrona.SemanticVersion} to ensure only application nodes of the same
         * major version communicate with each other.
         *
         * @return appVersion for user application.
         */
        public int appVersion()
        {
            return appVersion;
        }

        /**
         * User assigned application version validator implementation used to check version compatibility.
         * <p>
         * The default validator uses {@link org.agrona.SemanticVersion} semantics.
         *
         * @param appVersionValidator for user application.
         * @return this for fluent API.
         */
        public Context appVersionValidator(final AppVersionValidator appVersionValidator)
        {
            this.appVersionValidator = appVersionValidator;
            return this;
        }

        /**
         * User assigned application version validator implementation used to check version compatibility.
         * <p>
         * The default is to use {@link org.agrona.SemanticVersion} major version for checking compatibility.
         *
         * @return AppVersionValidator in use.
         */
        public AppVersionValidator appVersionValidator()
        {
            return appVersionValidator;
        }

        /**
         * Set the id for this cluster instance. This must match with the Consensus Module.
         *
         * @param clusterId for this clustered instance.
         * @return this for a fluent API
         * @see Configuration#CLUSTER_ID_PROP_NAME
         */
        public Context clusterId(final int clusterId)
        {
            this.clusterId = clusterId;
            return this;
        }

        /**
         * Get the id for this cluster instance. This must match with the Consensus Module.
         *
         * @return the id for this cluster instance.
         * @see Configuration#CLUSTER_ID_PROP_NAME
         */
        @Config
        public int clusterId()
        {
            return clusterId;
        }

        /**
         * Set the id for this clustered service. Services should be numbered from 0 and be contiguous.
         *
         * @param serviceId for this clustered service.
         * @return this for a fluent API
         * @see Configuration#SERVICE_ID_PROP_NAME
         */
        public Context serviceId(final int serviceId)
        {
            this.serviceId = serviceId;
            return this;
        }

        /**
         * Get the id for this clustered service. Services should be numbered from 0 and be contiguous.
         *
         * @return the id for this clustered service.
         * @see Configuration#SERVICE_ID_PROP_NAME
         */
        @Config
        public int serviceId()
        {
            return serviceId;
        }

        /**
         * Set the name for a clustered service to be the {@link Agent#roleName()} for the {@link Agent}.
         *
         * @param serviceName for a clustered service to be the role for the {@link Agent}.
         * @return this for a fluent API.
         * @see Configuration#SERVICE_NAME_PROP_NAME
         */
        public Context serviceName(final String serviceName)
        {
            this.serviceName = serviceName;
            return this;
        }

        /**
         * Get the name for a clustered service to be the {@link Agent#roleName()} for the {@link Agent}.
         *
         * @return the name for a clustered service to be the role of the {@link Agent}.
         * @see Configuration#SERVICE_NAME_PROP_NAME
         */
        @Config
        public String serviceName()
        {
            return serviceName;
        }

        /**
         * Set the channel parameter for the cluster log and snapshot replay channel.
         *
         * @param channel parameter for the cluster log replay channel.
         * @return this for a fluent API.
         * @see Configuration#REPLAY_CHANNEL_PROP_NAME
         */
        public Context replayChannel(final String channel)
        {
            replayChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for the cluster log and snapshot replay channel.
         *
         * @return the channel parameter for the cluster replay channel.
         * @see Configuration#REPLAY_CHANNEL_PROP_NAME
         */
        @Config
        public String replayChannel()
        {
            return replayChannel;
        }

        /**
         * Set the stream id for the cluster log and snapshot replay channel.
         *
         * @param streamId for the cluster log replay channel.
         * @return this for a fluent API
         * @see Configuration#REPLAY_STREAM_ID_PROP_NAME
         */
        public Context replayStreamId(final int streamId)
        {
            replayStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for the cluster log and snapshot replay channel.
         *
         * @return the stream id for the cluster log replay channel.
         * @see Configuration#REPLAY_STREAM_ID_PROP_NAME
         */
        @Config
        public int replayStreamId()
        {
            return replayStreamId;
        }

        /**
         * Set the channel parameter for bidirectional communications between the consensus module and services.
         *
         * @param channel parameter for sending messages to the Consensus Module.
         * @return this for a fluent API.
         * @see Configuration#CONTROL_CHANNEL_PROP_NAME
         */
        public Context controlChannel(final String channel)
        {
            controlChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for bidirectional communications between the consensus module and services.
         *
         * @return the channel parameter for sending messages to the Consensus Module.
         * @see Configuration#CONTROL_CHANNEL_PROP_NAME
         */
        @Config
        public String controlChannel()
        {
            return controlChannel;
        }

        /**
         * Set the stream id for communications from the consensus module and to the services.
         *
         * @param streamId for communications from the consensus module and to the services.
         * @return this for a fluent API
         * @see Configuration#SERVICE_STREAM_ID_PROP_NAME
         */
        public Context serviceStreamId(final int streamId)
        {
            serviceStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for communications from the consensus module and to the services.
         *
         * @return the stream id for communications from the consensus module and to the services.
         * @see Configuration#SERVICE_STREAM_ID_PROP_NAME
         */
        @Config
        public int serviceStreamId()
        {
            return serviceStreamId;
        }

        /**
         * Set the stream id for communications from the services to the consensus module.
         *
         * @param streamId for communications from the services to the consensus module.
         * @return this for a fluent API
         * @see Configuration#CONSENSUS_MODULE_STREAM_ID_PROP_NAME
         */
        public Context consensusModuleStreamId(final int streamId)
        {
            consensusModuleStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for communications from the services to the consensus module.
         *
         * @return the stream id for communications from the services to the consensus module.
         * @see Configuration#CONSENSUS_MODULE_STREAM_ID_PROP_NAME
         */
        @Config
        public int consensusModuleStreamId()
        {
            return consensusModuleStreamId;
        }

        /**
         * Set the channel parameter for snapshot recordings.
         *
         * @param channel parameter for snapshot recordings
         * @return this for a fluent API.
         * @see Configuration#SNAPSHOT_CHANNEL_PROP_NAME
         */
        public Context snapshotChannel(final String channel)
        {
            snapshotChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for snapshot recordings.
         *
         * @return the channel parameter for snapshot recordings.
         * @see Configuration#SNAPSHOT_CHANNEL_PROP_NAME
         */
        @Config
        public String snapshotChannel()
        {
            return snapshotChannel;
        }

        /**
         * Set the stream id for snapshot recordings.
         *
         * @param streamId for snapshot recordings.
         * @return this for a fluent API
         * @see Configuration#SNAPSHOT_STREAM_ID_PROP_NAME
         */
        public Context snapshotStreamId(final int streamId)
        {
            snapshotStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for snapshot recordings.
         *
         * @return the stream id for snapshot recordings.
         * @see Configuration#SNAPSHOT_STREAM_ID_PROP_NAME
         */
        @Config
        public int snapshotStreamId()
        {
            return snapshotStreamId;
        }

        /**
         * Set if this a service that responds to client requests.
         *
         * @param isRespondingService true if this service responds to client requests, otherwise false.
         * @return this for a fluent API.
         * @see Configuration#RESPONDER_SERVICE_PROP_NAME
         */
        public Context isRespondingService(final boolean isRespondingService)
        {
            this.isRespondingService = isRespondingService;
            return this;
        }

        /**
         * Set the fragment limit to be used when polling the log {@link Subscription}.
         *
         * @param logFragmentLimit for this clustered service.
         * @return this for a fluent API
         * @see Configuration#LOG_FRAGMENT_LIMIT_DEFAULT
         */
        public Context logFragmentLimit(final int logFragmentLimit)
        {
            this.logFragmentLimit = logFragmentLimit;
            return this;
        }

        /**
         * Get the fragment limit to be used when polling the log {@link Subscription}.
         *
         * @return the fragment limit to be used when polling the log {@link Subscription}.
         * @see Configuration#LOG_FRAGMENT_LIMIT_PROP_NAME
         */
        @Config
        public int logFragmentLimit()
        {
            return logFragmentLimit;
        }

        /**
         * Is this a service that responds to client requests?
         *
         * @return true if this service responds to client requests, otherwise false.
         * @see Configuration#RESPONDER_SERVICE_PROP_NAME
         */
        @Config(id = "RESPONDER_SERVICE")
        public boolean isRespondingService()
        {
            return isRespondingService;
        }

        /**
         * Get the thread factory used for creating threads.
         *
         * @return thread factory used for creating threads.
         */
        public ThreadFactory threadFactory()
        {
            return threadFactory;
        }

        /**
         * Set the thread factory used for creating threads.
         *
         * @param threadFactory used for creating threads
         * @return this for a fluent API.
         */
        public Context threadFactory(final ThreadFactory threadFactory)
        {
            this.threadFactory = threadFactory;
            return this;
        }

        /**
         * Provides an {@link IdleStrategy} supplier for the idle strategy for the agent duty cycle.
         *
         * @param idleStrategySupplier supplier for the idle strategy for the agent duty cycle.
         * @return this for a fluent API.
         */
        public Context idleStrategySupplier(final Supplier<IdleStrategy> idleStrategySupplier)
        {
            this.idleStrategySupplier = idleStrategySupplier;
            return this;
        }

        /**
         * Get a new {@link IdleStrategy} based on configured supplier.
         *
         * @return a new {@link IdleStrategy} based on configured supplier.
         */
        @Config(id = "CLUSTER_IDLE_STRATEGY")
        public IdleStrategy idleStrategy()
        {
            return idleStrategySupplier.get();
        }

        /**
         * Set the {@link EpochClock} to be used for tracking wall clock time when interacting with the container.
         *
         * @param clock {@link EpochClock} to be used for tracking wall clock time when interacting with the container.
         * @return this for a fluent API.
         */
        public Context epochClock(final EpochClock clock)
        {
            this.epochClock = clock;
            return this;
        }

        /**
         * Get the {@link EpochClock} to used for tracking wall clock time within the container.
         *
         * @return the {@link EpochClock} to used for tracking wall clock time within the container.
         */
        public EpochClock epochClock()
        {
            return epochClock;
        }

        /**
         * Get the {@link ErrorHandler} to be used by the {@link ClusteredServiceContainer}.
         *
         * @return the {@link ErrorHandler} to be used by the {@link ClusteredServiceContainer}.
         */
        public ErrorHandler errorHandler()
        {
            return errorHandler;
        }

        /**
         * Set the {@link ErrorHandler} to be used by the {@link ClusteredServiceContainer}.
         *
         * @param errorHandler the error handler to be used by the {@link ClusteredServiceContainer}.
         * @return this for a fluent API
         */
        public Context errorHandler(final ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        /**
         * Get the {@link DelegatingErrorHandler} to be used by the {@link ClusteredServiceContainer} which will
         * delegate to {@link #errorHandler()} as next in the chain.
         *
         * @return the {@link DelegatingErrorHandler} to be used by the {@link ClusteredServiceContainer}.
         * @see Configuration#DELEGATING_ERROR_HANDLER_PROP_NAME
         */
        @Config
        public DelegatingErrorHandler delegatingErrorHandler()
        {
            return delegatingErrorHandler;
        }

        /**
         * Set the {@link DelegatingErrorHandler} to be used by the {@link ClusteredServiceContainer} which will
         * delegate to {@link #errorHandler()} as next in the chain.
         *
         * @param delegatingErrorHandler the error handler to be used by the {@link ClusteredServiceContainer}.
         * @return this for a fluent API
         * @see Configuration#DELEGATING_ERROR_HANDLER_PROP_NAME
         */
        public Context delegatingErrorHandler(final DelegatingErrorHandler delegatingErrorHandler)
        {
            this.delegatingErrorHandler = delegatingErrorHandler;
            return this;
        }

        /**
         * Get the error counter that will record the number of errors the container has observed.
         *
         * @return the error counter that will record the number of errors the container has observed.
         */
        public AtomicCounter errorCounter()
        {
            return errorCounter;
        }

        /**
         * Set the error counter that will record the number of errors the cluster node has observed.
         *
         * @param errorCounter the error counter that will record the number of errors the cluster node has observed.
         * @return this for a fluent API.
         */
        public Context errorCounter(final AtomicCounter errorCounter)
        {
            this.errorCounter = errorCounter;
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
         * An {@link Aeron} client for the container.
         *
         * @return {@link Aeron} client for the container
         */
        public Aeron aeron()
        {
            return aeron;
        }

        /**
         * Provide an {@link Aeron} client for the container
         * <p>
         * If not provided then one will be created.
         *
         * @param aeron client for the container
         * @return this for a fluent API.
         */
        public Context aeron(final Aeron aeron)
        {
            this.aeron = aeron;
            return this;
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
         * The service this container holds.
         *
         * @return service this container holds.
         */
        @Config(id = "SERVICE_CLASS_NAME")
        public ClusteredService clusteredService()
        {
            return clusteredService;
        }

        /**
         * Set the service this container is to hold.
         *
         * @param clusteredService this container is to hold.
         * @return this for fluent API.
         */
        public Context clusteredService(final ClusteredService clusteredService)
        {
            this.clusteredService = clusteredService;
            return this;
        }

        /**
         * Set the context that should be used for communicating with the local Archive.
         *
         * @param archiveContext that should be used for communicating with the local Archive.
         * @return this for a fluent API.
         */
        public Context archiveContext(final AeronArchive.Context archiveContext)
        {
            this.archiveContext = archiveContext;
            return this;
        }

        /**
         * Get the context that should be used for communicating with the local Archive.
         *
         * @return the context that should be used for communicating with the local Archive.
         */
        public AeronArchive.Context archiveContext()
        {
            return archiveContext;
        }

        /**
         * Set the directory name to use for the consensus module directory.
         *
         * @param clusterDirectoryName to use.
         * @return this for a fluent API.
         * @see Configuration#CLUSTER_DIR_PROP_NAME
         */
        public Context clusterDirectoryName(final String clusterDirectoryName)
        {
            this.clusterDirectoryName = clusterDirectoryName;
            return this;
        }

        /**
         * The directory name to use for the cluster directory.
         *
         * @return directory name for the cluster directory.
         * @see Configuration#CLUSTER_DIR_PROP_NAME
         */
        @Config(id = "CLUSTER_DIR")
        public String clusterDirectoryName()
        {
            return clusterDirectoryName;
        }

        /**
         * Set the directory to use for the cluster directory.
         *
         * @param clusterDir to use.
         * @return this for a fluent API.
         * @see ClusteredServiceContainer.Configuration#CLUSTER_DIR_PROP_NAME
         */
        public Context clusterDir(final File clusterDir)
        {
            this.clusterDir = clusterDir;
            return this;
        }

        /**
         * The directory used for the cluster directory.
         *
         * @return directory for the cluster directory.
         * @see ClusteredServiceContainer.Configuration#CLUSTER_DIR_PROP_NAME
         */
        public File clusterDir()
        {
            return clusterDir;
        }

        /**
         * Get the directory in which the ClusteredServiceContainer will store mark file (i.e. {@code
         * cluster-mark-service-0.dat}). It defaults to {@link #clusterDir()} if it is not set explicitly via the {@link
         * ClusteredServiceContainer.Configuration#MARK_FILE_DIR_PROP_NAME}.
         *
         * @return the directory in which the ClusteredServiceContainer will store mark file (i.e.
         * {@code cluster-mark-service-0.dat}).
         * @see ClusteredServiceContainer.Configuration#MARK_FILE_DIR_PROP_NAME
         * @see #clusterDir()
         */
        @Config
        public File markFileDir()
        {
            return markFileDir;
        }

        /**
         * Set the directory in which the ClusteredServiceContainer will store mark file (i.e. {@code
         * cluster-mark-service-0.dat}).
         *
         * @param markFileDir the directory in which the ClusteredServiceContainer will store mark file (i.e. {@code
         *                    cluster-mark-service-0.dat}).
         * @return this for a fluent API.
         */
        public ClusteredServiceContainer.Context markFileDir(final File markFileDir)
        {
            this.markFileDir = markFileDir;
            return this;
        }

        /**
         * Set the {@link Runnable} that is called when container is instructed to terminate.
         *
         * @param terminationHook that can be used to terminate a service container.
         * @return this for a fluent API.
         */
        public Context terminationHook(final Runnable terminationHook)
        {
            this.terminationHook = terminationHook;
            return this;
        }

        /**
         * Get the {@link Runnable} that is called when container is instructed to terminate.
         *
         * @return the {@link Runnable} that can be used to terminate a service container.
         */
        public Runnable terminationHook()
        {
            return terminationHook;
        }

        /**
         * Set the {@link ClusterMarkFile} in use.
         *
         * @param markFile to use.
         * @return this for a fluent API.
         */
        public Context clusterMarkFile(final ClusterMarkFile markFile)
        {
            this.markFile = markFile;
            return this;
        }

        /**
         * The {@link ClusterMarkFile} in use.
         *
         * @return {@link ClusterMarkFile} in use.
         */
        public ClusterMarkFile clusterMarkFile()
        {
            return markFile;
        }

        /**
         * Set the error buffer length in bytes to use.
         *
         * @param errorBufferLength in bytes to use.
         * @return this for a fluent API.
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
         */
        @Config(id = "SERVICE_ERROR_BUFFER_LENGTH")
        public int errorBufferLength()
        {
            return errorBufferLength;
        }

        /**
         * Set the {@link DistinctErrorLog} in use.
         *
         * @param errorLog to use.
         * @return this for a fluent API.
         */
        public Context errorLog(final DistinctErrorLog errorLog)
        {
            this.errorLog = errorLog;
            return this;
        }

        /**
         * The {@link DistinctErrorLog} in use.
         *
         * @return {@link DistinctErrorLog} in use.
         */
        public DistinctErrorLog errorLog()
        {
            return errorLog;
        }

        /**
         * The {@link NanoClock} as a source of time in nanoseconds for measuring duration.
         *
         * @return the {@link NanoClock} as a source of time in nanoseconds for measuring duration.
         */
        public NanoClock nanoClock()
        {
            return nanoClock;
        }

        /**
         * The {@link NanoClock} as a source of time in nanoseconds for measuring duration.
         *
         * @param clock to be used.
         * @return this for a fluent API.
         */
        public Context nanoClock(final NanoClock clock)
        {
            nanoClock = clock;
            return this;
        }

        /**
         * Set a threshold for the container work cycle time which when exceed it will increment the
         * counter.
         *
         * @param thresholdNs value in nanoseconds
         * @return this for fluent API.
         * @see Configuration#CYCLE_THRESHOLD_PROP_NAME
         * @see Configuration#CYCLE_THRESHOLD_DEFAULT_NS
         */
        public Context cycleThresholdNs(final long thresholdNs)
        {
            this.cycleThresholdNs = thresholdNs;
            return this;
        }

        /**
         * Threshold for the container work cycle time which when exceed it will increment the
         * counter.
         *
         * @return threshold to track for the container work cycle time.
         */
        @Config(id = "SERVICE_CYCLE_THRESHOLD")
        public long cycleThresholdNs()
        {
            return cycleThresholdNs;
        }

        /**
         * Set a duty cycle tracker to be used for tracking the duty cycle time of the container.
         *
         * @param dutyCycleTracker to use for tracking.
         * @return this for fluent API.
         */
        public Context dutyCycleTracker(final DutyCycleTracker dutyCycleTracker)
        {
            this.dutyCycleTracker = dutyCycleTracker;
            return this;
        }

        /**
         * The duty cycle tracker used to track the container duty cycle.
         *
         * @return the duty cycle tracker.
         */
        public DutyCycleTracker dutyCycleTracker()
        {
            return dutyCycleTracker;
        }

        /**
         * Set a threshold for snapshot duration which when exceeded will result in a counter increment.
         *
         * @param thresholdNs value in nanoseconds.
         * @return this for fluent API.
         * @see Configuration#SNAPSHOT_DURATION_THRESHOLD_PROP_NAME
         * @see Configuration#SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS
         * @since 1.44.0
         */
        public Context snapshotDurationThresholdNs(final long thresholdNs)
        {
            this.snapshotDurationThresholdNs = thresholdNs;
            return this;
        }

        /**
         * Threshold for snapshot duration which when exceeded will result in a counter increment.
         *
         * @return threshold value in nanoseconds.
         * @since 1.44.0
         */
        @Config
        public long snapshotDurationThresholdNs()
        {
            return snapshotDurationThresholdNs;
        }

        /**
         * Set snapshot duration tracker used for monitoring snapshot duration.
         *
         * @param snapshotDurationTracker snapshot duration tracker.
         * @return this for fluent API.
         * @since 1.44.0
         */
        public Context snapshotDurationTracker(final SnapshotDurationTracker snapshotDurationTracker)
        {
            this.snapshotDurationTracker = snapshotDurationTracker;
            return this;
        }

        /**
         * Get snapshot duration tracker used for monitoring snapshot duration.
         *
         * @return snapshot duration tracker.
         * @since 1.44.0
         */
        public SnapshotDurationTracker snapshotDurationTracker()
        {
            return snapshotDurationTracker;
        }

        /**
         * Delete the cluster container directory.
         */
        public void deleteDirectory()
        {
            if (null != clusterDir)
            {
                IoUtil.delete(clusterDir, false);
            }
        }

        /**
         * Indicates if this node should take standby snapshots.
         *
         * @return <code>true</code> if this should take standby snapshots, <code>false</code> otherwise.
         * @see ClusteredServiceContainer.Configuration#STANDBY_SNAPSHOT_ENABLED_PROP_NAME
         * @see ClusteredServiceContainer.Configuration#standbySnapshotEnabled()
         */
        @Config
        public boolean standbySnapshotEnabled()
        {
            return standbySnapshotEnabled;
        }

        /**
         * Indicates if this node should take standby snapshots.
         *
         * @param standbySnapshotEnabled if this node should take standby snapshots.
         * @return this for a fluent API.
         * @see ClusteredServiceContainer.Configuration#STANDBY_SNAPSHOT_ENABLED_PROP_NAME
         * @see ClusteredServiceContainer.Configuration#standbySnapshotEnabled()
         */
        public ClusteredServiceContainer.Context standbySnapshotEnabled(final boolean standbySnapshotEnabled)
        {
            this.standbySnapshotEnabled = standbySnapshotEnabled;
            return this;
        }

        /**
         * Close the context and free applicable resources.
         * <p>
         * If {@link #ownsAeronClient()} is true then the {@link #aeron()} client will be closed.
         */
        public void close()
        {
            final ErrorHandler errorHandler = countedErrorHandler();
            if (ownsAeronClient)
            {
                CloseHelper.close(errorHandler, aeron);
            }

            CloseHelper.close(markFile);
        }

        CountDownLatch abortLatch()
        {
            return abortLatch;
        }

        private void concludeMarkFile()
        {
            ClusterMarkFile.checkHeaderLength(
                aeron.context().aeronDirectoryName(), controlChannel(), null, serviceName, null);

            final MarkFileHeaderEncoder encoder = markFile.encoder();

            encoder
                .archiveStreamId(archiveContext.controlRequestStreamId())
                .serviceStreamId(serviceStreamId)
                .consensusModuleStreamId(consensusModuleStreamId)
                .ingressStreamId(Aeron.NULL_VALUE)
                .memberId(Aeron.NULL_VALUE)
                .serviceId(serviceId)
                .clusterId(clusterId)
                .aeronDirectory(aeron.context().aeronDirectoryName())
                .controlChannel(controlChannel)
                .ingressChannel(null)
                .serviceName(serviceName)
                .authenticator(null);

            markFile.signalReady(epochClock.time());
        }

        /**
         * {@inheritDoc}
         */
        public String toString()
        {
            return "ClusteredServiceContainer.Context" +
                "\n{" +
                "\n    isConcluded=" + isConcluded() +
                "\n    ownsAeronClient=" + ownsAeronClient +
                "\n    aeronDirectoryName='" + aeronDirectoryName + '\'' +
                "\n    aeron=" + aeron +
                "\n    archiveContext=" + archiveContext +
                "\n    clusterDirectoryName='" + clusterDirectoryName + '\'' +
                "\n    clusterDir=" + clusterDir +
                "\n    appVersion=" + appVersion +
                "\n    clusterId=" + clusterId +
                "\n    serviceId=" + serviceId +
                "\n    serviceName='" + serviceName + '\'' +
                "\n    replayChannel='" + replayChannel + '\'' +
                "\n    replayStreamId=" + replayStreamId +
                "\n    controlChannel='" + controlChannel + '\'' +
                "\n    consensusModuleStreamId=" + consensusModuleStreamId +
                "\n    serviceStreamId=" + serviceStreamId +
                "\n    snapshotChannel='" + snapshotChannel + '\'' +
                "\n    snapshotStreamId=" + snapshotStreamId +
                "\n    errorBufferLength=" + errorBufferLength +
                "\n    isRespondingService=" + isRespondingService +
                "\n    logFragmentLimit=" + logFragmentLimit +
                "\n    abortLatch=" + abortLatch +
                "\n    threadFactory=" + threadFactory +
                "\n    idleStrategySupplier=" + idleStrategySupplier +
                "\n    epochClock=" + epochClock +
                "\n    errorLog=" + errorLog +
                "\n    errorHandler=" + errorHandler +
                "\n    delegatingErrorHandler=" + delegatingErrorHandler +
                "\n    errorCounter=" + errorCounter +
                "\n    countedErrorHandler=" + countedErrorHandler +
                "\n    clusteredService=" + clusteredService +
                "\n    terminationHook=" + terminationHook +
                "\n    cycleThresholdNs=" + cycleThresholdNs +
                "\n    dutyCyleTracker=" + dutyCycleTracker +
                "\n    snapshotDurationThresholdNs=" + snapshotDurationThresholdNs +
                "\n    snapshotDurationTracker=" + snapshotDurationTracker +
                "\n    markFile=" + markFile +
                "\n}";
        }
    }
}
