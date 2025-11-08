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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.Counter;
import io.aeron.RethrowingErrorHandler;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.mark.ClusterComponentType;
import io.aeron.cluster.service.ClusterCounters;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.config.Config;
import io.aeron.config.DefaultType;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.security.CredentialsSupplier;
import io.aeron.security.NullCredentialsSupplier;
import io.aeron.version.Versioned;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.MarkFile;
import org.agrona.Strings;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.aeron.AeronCounters.CLUSTER_BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID;
import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.CommonContext.driverFilePageSize;
import static io.aeron.cluster.ConsensusModule.Configuration.SERVICE_ID;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.LIVENESS_TIMEOUT_MS;
import static java.lang.System.getProperty;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.agrona.SystemUtil.getDurationInNanos;

/**
 * Backup component which can run remote from a cluster which polls for snapshots and replicates the log.
 */
@Versioned
public final class ClusterBackup implements AutoCloseable
{
    /**
     * The type id of the {@link Counter} used for the backup state.
     */
    public static final int BACKUP_STATE_TYPE_ID = AeronCounters.CLUSTER_BACKUP_STATE_TYPE_ID;

    /**
     * The type id of the {@link Counter} used for the live log position counter.
     */
    public static final int LIVE_LOG_POSITION_TYPE_ID = AeronCounters.CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID;

    /**
     * The type id of the {@link Counter} used for the next query deadline counter.
     */
    public static final int QUERY_DEADLINE_TYPE_ID = AeronCounters.CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID;

    /**
     * The type id of the {@link Counter} used for keeping track of the number of errors that have occurred.
     */
    public static final int CLUSTER_BACKUP_ERROR_COUNT_TYPE_ID = AeronCounters.CLUSTER_BACKUP_ERROR_COUNT_TYPE_ID;

    /**
     * State of the cluster backup state machine.
     */
    public enum State
    {
        /**
         * Query leader for current status for backup.
         */
        BACKUP_QUERY(0),

        /**
         * Retrieve a copy of the latest snapshot from the leader.
         */
        SNAPSHOT_RETRIEVE(1),

        /**
         * Setup recording for live log.
         */
        LIVE_LOG_RECORD(2),

        /**
         * Replay the current live log since snapshot and join it.
         */
        LIVE_LOG_REPLAY(3),

        /**
         * Update the local {@link RecordingLog} for recovery.
         */
        UPDATE_RECORDING_LOG(4),

        /**
         * Back up live log and track progress until next query deadline is reached.
         */
        BACKING_UP(5),

        /**
         * On error or progress stall the backup is reset and started over again.
         */
        RESET_BACKUP(6),

        /**
         * The backup is complete and closed.
         */
        CLOSED(7);

        static final State[] STATES = values();

        private final int code;

        State(final int code)
        {
            if (code != ordinal())
            {
                throw new IllegalArgumentException(name() + " - code must equal ordinal value: code=" + code);
            }

            this.code = code;
        }

        /**
         * Code which represents the {@link State} as an int.
         *
         * @return code which represents the {@link State} as an int.
         */
        public int code()
        {
            return code;
        }

        /**
         * Get the {@link State} encoded in an {@link AtomicCounter}.
         *
         * @param counter to get the current state for.
         * @return the state for the {@link ClusterBackup}.
         * @throws ClusterException if the counter is not one of the valid values.
         */
        public static State get(final AtomicCounter counter)
        {
            if (counter.isClosed())
            {
                return CLOSED;
            }

            return get(counter.get());
        }

        /**
         * Get the {@link State} with matching {@link #code()}.
         *
         * @param code to lookup.
         * @return the {@link State} matching {@link #code()}.
         */
        public static State get(final long code)
        {
            if (code < 0 || code > (STATES.length - 1))
            {
                throw new ClusterException("invalid state counter code: " + code);
            }

            return STATES[(int)code];
        }
    }

    /**
     * Defines the type of node that this will receive log data from.
     */
    public enum SourceType
    {
        /**
         * Receive from any node in the cluster.
         */
        ANY,
        /**
         * Only receive data from the leader node.
         */
        LEADER,
        /**
         * Receive data from any node that is not the leader.
         */
        FOLLOWER
    }

    private final ClusterBackup.Context ctx;
    private final AgentInvoker agentInvoker;
    private final AgentRunner agentRunner;

    private ClusterBackup(final ClusterBackup.Context ctx)
    {
        try
        {
            ctx.conclude();
            this.ctx = ctx;

            final ClusterBackupAgent agent = new ClusterBackupAgent(ctx);

            if (ctx.useAgentInvoker())
            {
                agentRunner = null;
                agentInvoker = new AgentInvoker(ctx.errorHandler(), ctx.errorCounter(), agent);
            }
            else
            {
                agentRunner = new AgentRunner(ctx.idleStrategy(), ctx.errorHandler(), ctx.errorCounter(), agent);
                agentInvoker = null;
            }
        }
        catch (final ConcurrentConcludeException ex)
        {
            throw ex;
        }
        catch (final Exception ex)
        {
            CloseHelper.quietClose(ctx::close);
            throw ex;
        }
    }

    /**
     * Launch an {@link ClusterBackup} using a default configuration.
     *
     * @return a new instance of an {@link ClusterBackup}.
     */
    public static ClusterBackup launch()
    {
        return launch(new ClusterBackup.Context());
    }

    /**
     * Launch an {@link ClusterBackup} by providing a configuration context.
     *
     * @param ctx for the configuration parameters.
     * @return a new instance of an {@link ClusterBackup}.
     */
    public static ClusterBackup launch(final ClusterBackup.Context ctx)
    {
        final ClusterBackup clusterBackup = new ClusterBackup(ctx);
        if (null != clusterBackup.agentRunner)
        {
            AgentRunner.startOnThread(clusterBackup.agentRunner, ctx.threadFactory());
        }
        else
        {
            clusterBackup.agentInvoker.start();
        }

        return clusterBackup;
    }

    /**
     * Get the {@link ClusterBackup.Context} that is used by this {@link ClusterBackup}.
     *
     * @return the {@link ClusterBackup.Context} that is used by this {@link ClusterBackup}.
     */
    public ClusterBackup.Context context()
    {
        return ctx;
    }

    /**
     * Get the {@link AgentInvoker} for the cluster backup.
     *
     * @return the {@link AgentInvoker} for the cluster backup.
     */
    public AgentInvoker conductorAgentInvoker()
    {
        return agentInvoker;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        final CountedErrorHandler countedErrorHandler = ctx.countedErrorHandler();
        CloseHelper.close(countedErrorHandler, agentRunner);
        CloseHelper.close(countedErrorHandler, agentInvoker);
    }

    /**
     * Configuration options for {@link ClusterBackup} with defaults and constants for system properties lookup.
     */
    @Config(existsInC = false)
    public static class Configuration
    {
        /**
         * Channel template used for catchup and replication of log and snapshots.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String CLUSTER_BACKUP_CATCHUP_ENDPOINT_PROP_NAME = "aeron.cluster.backup.catchup.endpoint";

        /**
         * Channel template used for catchup and replication of log and snapshots.
         */
        @Config
        public static final String CLUSTER_BACKUP_CATCHUP_CHANNEL_PROP_NAME = "aeron.cluster.backup.catchup.channel";

        /**
         * Default channel template used for catchup and replication of log and snapshots.
         */
        @Config
        public static final String CLUSTER_BACKUP_CATCHUP_CHANNEL_DEFAULT =
            "aeron:udp?alias=backup|cc=cubic|so-sndbuf=512k|so-rcvbuf=512k|rcv-wnd=512k";

        /**
         * Interval at which a cluster backup will send backup queries.
         */
        @Config
        public static final String CLUSTER_BACKUP_INTERVAL_PROP_NAME = "aeron.cluster.backup.interval";

        /**
         * Default interval at which a cluster backup will send backup queries.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 60L * 60 * 1000 * 1000 * 1000)
        public static final long CLUSTER_BACKUP_INTERVAL_DEFAULT_NS = TimeUnit.HOURS.toNanos(1);

        /**
         * Timeout within which a cluster backup will expect a response from a backup query.
         */
        @Config
        public static final String CLUSTER_BACKUP_RESPONSE_TIMEOUT_PROP_NAME = "aeron.cluster.backup.response.timeout";

        /**
         * Default timeout within which a cluster backup will expect a response from a backup query.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 5L * 1000 * 1000 * 1000)
        public static final long CLUSTER_BACKUP_RESPONSE_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

        /**
         * Timeout within which a cluster backup will expect progress.
         */
        @Config
        public static final String CLUSTER_BACKUP_PROGRESS_TIMEOUT_PROP_NAME = "aeron.cluster.backup.progress.timeout";

        /**
         * Interval at which the cluster backup is re-initialised after an exception has been thrown.
         */
        @Config
        public static final String CLUSTER_BACKUP_COOL_DOWN_INTERVAL_PROP_NAME =
            "aeron.cluster.backup.cool.down.interval";

        /**
         * Default interval at which the cluster back is re-initialised after an exception has been thrown.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 30L * 1000 * 1000 * 1000)
        public static final long CLUSTER_BACKUP_COOL_DOWN_INTERVAL_DEFAULT_NS = TimeUnit.SECONDS.toNanos(30);

        /**
         * Default timeout within which a cluster backup will expect progress.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 10L * 1000 * 1000 * 1000)
        public static final long CLUSTER_BACKUP_PROGRESS_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(10);

        /**
         * The source type used for the cluster backup. Should match on of the {@link SourceType} enum values.
         */
        @Config
        public static final String CLUSTER_BACKUP_SOURCE_TYPE_PROP_NAME = "aeron.cluster.backup.source.type";

        /**
         * Default source type to receive log traffic from.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "ANY")
        public static final String CLUSTER_BACKUP_SOURCE_TYPE_DEFAULT = SourceType.ANY.name();

        /**
         * The value of system property {@link #CLUSTER_BACKUP_CATCHUP_ENDPOINT_PROP_NAME} if set, otherwise it will
         * try to derive the catchup endpoint from {@link ConsensusModule.Configuration#clusterMembers()} and
         * {@link ConsensusModule.Configuration#clusterMemberId()}. Failing that null will be returned.
         *
         * @return system property {@link #CLUSTER_BACKUP_CATCHUP_ENDPOINT_PROP_NAME}, the derived value, or null.
         */
        public static String catchupEndpoint()
        {
            String configuredCatchupEndpoint = System.getProperty(CLUSTER_BACKUP_CATCHUP_ENDPOINT_PROP_NAME);

            if (null == configuredCatchupEndpoint && null != ConsensusModule.Configuration.clusterMembers())
            {
                final ClusterMember member = ClusterMember.determineMember(
                    ClusterMember.parse(ConsensusModule.Configuration.clusterMembers()),
                    ConsensusModule.Configuration.clusterMemberId(),
                    ConsensusModule.Configuration.memberEndpoints());

                configuredCatchupEndpoint = member.catchupEndpoint();
            }

            return configuredCatchupEndpoint;
        }

        /**
         * The value {@link #CLUSTER_BACKUP_CATCHUP_CHANNEL_DEFAULT} or system property
         * {@link #CLUSTER_BACKUP_CATCHUP_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #CLUSTER_BACKUP_CATCHUP_CHANNEL_DEFAULT} or system property
         * {@link #CLUSTER_BACKUP_CATCHUP_CHANNEL_PROP_NAME} if set.
         */
        public static String catchupChannel()
        {
            return System.getProperty(CLUSTER_BACKUP_CATCHUP_CHANNEL_PROP_NAME, CLUSTER_BACKUP_CATCHUP_CHANNEL_DEFAULT);
        }

        /**
         * The value of system property {@link ConsensusModule.Configuration#consensusChannel()} if set. If that channel
         * does not have an endpoint set, then this will try to derive one using
         * {@link ConsensusModule.Configuration#clusterMembers()} and
         * {@link ConsensusModule.Configuration#clusterMemberId()}.
         *
         * @return system property {@link #CLUSTER_BACKUP_CATCHUP_CHANNEL_PROP_NAME}, the derived value, or null.
         */
        public static String consensusChannel()
        {
            String consensusChannel = ConsensusModule.Configuration.consensusChannel();

            if (null != consensusChannel && null != ConsensusModule.Configuration.clusterMembers())
            {
                final ChannelUri consensusUri = ChannelUri.parse(consensusChannel);
                if (!consensusUri.containsKey(ENDPOINT_PARAM_NAME))
                {
                    final ClusterMember member = ClusterMember.determineMember(
                        ClusterMember.parse(ConsensusModule.Configuration.clusterMembers()),
                        ConsensusModule.Configuration.clusterMemberId(),
                        ConsensusModule.Configuration.memberEndpoints());

                    consensusUri.put(ENDPOINT_PARAM_NAME, member.consensusEndpoint());
                    consensusChannel = consensusUri.toString();
                }
            }

            return consensusChannel;
        }

        /**
         * Interval at which a cluster backup will send backup queries.
         *
         * @return Interval at which a cluster backup will send backup queries.
         * @see #CLUSTER_BACKUP_INTERVAL_PROP_NAME
         */
        public static long clusterBackupIntervalNs()
        {
            return getDurationInNanos(CLUSTER_BACKUP_INTERVAL_PROP_NAME, CLUSTER_BACKUP_INTERVAL_DEFAULT_NS);
        }

        /**
         * Timeout within which a cluster backup will expect a response from a backup query.
         *
         * @return timeout within which a cluster backup will expect a response from a backup query.
         * @see #CLUSTER_BACKUP_RESPONSE_TIMEOUT_PROP_NAME
         */
        public static long clusterBackupResponseTimeoutNs()
        {
            return getDurationInNanos(
                CLUSTER_BACKUP_RESPONSE_TIMEOUT_PROP_NAME, CLUSTER_BACKUP_RESPONSE_TIMEOUT_DEFAULT_NS);
        }

        /**
         * Timeout within which a cluster backup will expect progress.
         *
         * @return timeout within which a cluster backup will expect progress.
         * @see #CLUSTER_BACKUP_PROGRESS_TIMEOUT_PROP_NAME
         */
        public static long clusterBackupProgressTimeoutNs()
        {
            return getDurationInNanos(
                CLUSTER_BACKUP_PROGRESS_TIMEOUT_PROP_NAME, CLUSTER_BACKUP_PROGRESS_TIMEOUT_DEFAULT_NS);
        }

        /**
         * Interval at which the cluster backup is re-initialised after an exception has been thrown.
         *
         * @return interval at which the cluster backup is re-initialised after an exception has been thrown.
         * @see #CLUSTER_BACKUP_COOL_DOWN_INTERVAL_PROP_NAME
         */
        public static long clusterBackupCoolDownIntervalNs()
        {
            return getDurationInNanos(
                CLUSTER_BACKUP_COOL_DOWN_INTERVAL_PROP_NAME, CLUSTER_BACKUP_COOL_DOWN_INTERVAL_DEFAULT_NS);
        }

        /**
         * Returns the string representation of the {@link SourceType} that this backup instance will use depending on
         * the value of the {@link #CLUSTER_BACKUP_CATCHUP_CHANNEL_PROP_NAME} system property if set or
         * {@link #CLUSTER_BACKUP_SOURCE_TYPE_DEFAULT} if not.
         *
         * @return the configured source type.
         */
        public static String clusterBackupSourceType()
        {
            return System.getProperty(CLUSTER_BACKUP_SOURCE_TYPE_PROP_NAME, CLUSTER_BACKUP_SOURCE_TYPE_DEFAULT);
        }

        /**
         * Determines what position to start from when backing up the log from the cluster.
         */
        public enum ReplayStart
        {
            /**
             * Start from the earliest available position in the cluster log. May not be 0 if the cluster log was
             * truncated at some point.
             */
            BEGINNING,
            /**
             * Start backing up from the log position of the most recent snapshot in the log.
             */
            LATEST_SNAPSHOT
        }

        /**
         * Default value for the initial cluster replay start.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "BEGINNING")
        public static final ReplayStart CLUSTER_INITIAL_REPLAY_START_DEFAULT = ReplayStart.BEGINNING;

        /**
         * Property name for setting the cluster replay start.
         */
        @Config
        public static final String CLUSTER_INITIAL_REPLAY_START_PROP_NAME = "cluster.backup.initial.replay.start";

        /**
         * Get the initial value for the cluster relay start.
         *
         * @return enum to determine where to start replaying the log from.
         * @see #CLUSTER_INITIAL_REPLAY_START_PROP_NAME
         */
        public static ReplayStart clusterInitialReplayStart()
        {
            final String propertyValue = getProperty(CLUSTER_INITIAL_REPLAY_START_PROP_NAME);
            if (null == propertyValue)
            {
                return CLUSTER_INITIAL_REPLAY_START_DEFAULT;
            }

            return ReplayStart.valueOf(propertyValue);
        }
    }

    /**
     * Context for overriding default configuration for {@link ClusterBackup}.
     */
    public static class Context implements Cloneable
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
        private boolean ownsAeronClient = false;
        private String aeronDirectoryName = CommonContext.getAeronDirectoryName();
        private Aeron aeron;

        private int clusterId = ClusteredServiceContainer.Configuration.clusterId();
        private String consensusChannel = Configuration.consensusChannel();
        private int consensusStreamId = ConsensusModule.Configuration.consensusStreamId();
        private int consensusModuleSnapshotStreamId = ConsensusModule.Configuration.snapshotStreamId();
        private int serviceSnapshotStreamId = ClusteredServiceContainer.Configuration.snapshotStreamId();
        private int logStreamId = ConsensusModule.Configuration.logStreamId();
        private String catchupEndpoint = Configuration.catchupEndpoint();
        private String catchupChannel = Configuration.catchupChannel();

        private long clusterBackupIntervalNs = Configuration.clusterBackupIntervalNs();
        private long clusterBackupResponseTimeoutNs = Configuration.clusterBackupResponseTimeoutNs();
        private long clusterBackupProgressTimeoutNs = Configuration.clusterBackupProgressTimeoutNs();
        private long clusterBackupCoolDownIntervalNs = Configuration.clusterBackupCoolDownIntervalNs();
        private int errorBufferLength = ConsensusModule.Configuration.errorBufferLength();

        private boolean deleteDirOnStart = false;
        private boolean useAgentInvoker = false;
        private String clusterDirectoryName = ClusteredServiceContainer.Configuration.clusterDirName();
        private File clusterDir;
        private File markFileDir;
        private ClusterMarkFile markFile;
        private String clusterConsensusEndpoints = ConsensusModule.Configuration.clusterConsensusEndpoints();
        private ThreadFactory threadFactory;
        private EpochClock epochClock;
        private Supplier<IdleStrategy> idleStrategySupplier;

        private DistinctErrorLog errorLog;
        private ErrorHandler errorHandler;
        private AtomicCounter errorCounter;
        private CountedErrorHandler countedErrorHandler;
        private Counter stateCounter;
        private Counter liveLogPositionCounter;
        private Counter nextQueryDeadlineMsCounter;
        private Counter snapshotRetrieveCounter;

        private AeronArchive.Context archiveContext;
        private AeronArchive.Context clusterArchiveContext;
        private Runnable terminationHook;
        private ClusterBackupEventsListener eventsListener;
        private CredentialsSupplier credentialsSupplier;
        private String sourceType = Configuration.clusterBackupSourceType();
        private long replicationProgressTimeoutNs = ConsensusModule.Configuration.replicationProgressTimeoutNs();
        private long replicationProgressIntervalNs = ConsensusModule.Configuration.replicationProgressIntervalNs();
        private Configuration.ReplayStart initialReplayStart = Configuration.clusterInitialReplayStart();

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
            final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

            if ((boolean)IS_CONCLUDED_VH.getAndSet(this, true))
            {
                throw new ConcurrentConcludeException();
            }

            if (null == clusterDir)
            {
                clusterDir = new File(clusterDirectoryName);
            }
            else
            {
                clusterDirectoryName = clusterDir.getPath();
            }

            if (deleteDirOnStart)
            {
                IoUtil.delete(clusterDir, false);
            }

            if (null == catchupEndpoint)
            {
                throw new ClusterException("ClusterBackup.Context.catchupEndpoint must be set");
            }

            if (!clusterDir.exists() && !clusterDir.mkdirs())
            {
                throw new ClusterException("failed to create cluster dir: " + clusterDir.getAbsolutePath());
            }

            if (null == markFileDir)
            {
                final String dir = ClusteredServiceContainer.Configuration.markFileDir();
                markFileDir = Strings.isEmpty(dir) ? clusterDir : new File(dir);
            }

            if (!markFileDir.exists() && !markFileDir.mkdirs())
            {
                throw new ArchiveException("failed to create mark file dir: " + markFileDir.getAbsolutePath());
            }

            if (null == epochClock)
            {
                epochClock = SystemEpochClock.INSTANCE;
            }

            if (Aeron.NULL_VALUE == replicationProgressIntervalNs)
            {
                replicationProgressIntervalNs = Math.max(replicationProgressTimeoutNs / 10, 1);
            }

            if (null == markFile)
            {
                final int filePageSize = null != aeron ? aeron.context().filePageSize() :
                    driverFilePageSize(new File(aeronDirectoryName), epochClock, new CommonContext().driverTimeoutMs());
                markFile = new ClusterMarkFile(
                    new File(markFileDir, ClusterMarkFile.FILENAME),
                    ClusterComponentType.BACKUP,
                    errorBufferLength,
                    epochClock,
                    LIVENESS_TIMEOUT_MS,
                    filePageSize);
            }

            MarkFile.ensureMarkFileLink(
                clusterDir,
                new File(markFile.parentDirectory(), ClusterMarkFile.FILENAME),
                ClusterMarkFile.LINK_FILENAME);

            if (null == errorLog)
            {
                errorLog = new DistinctErrorLog(markFile.errorBuffer(), epochClock, US_ASCII);
            }

            errorHandler = CommonContext.setupErrorHandler(errorHandler, errorLog);

            final String clientName = "cluster-backup clusterId=" + clusterId;
            if (null == aeron)
            {
                ownsAeronClient = true;

                aeron = Aeron.connect(
                    new Aeron.Context()
                        .aeronDirectoryName(aeronDirectoryName)
                        .errorHandler(errorHandler)
                        .epochClock(epochClock)
                        .useConductorAgentInvoker(true)
                        .awaitingIdleStrategy(YieldingIdleStrategy.INSTANCE)
                        .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE)
                        .clientLock(NoOpLock.INSTANCE)
                        .clientName(clientName));

                if (null == errorCounter)
                {
                    errorCounter = ClusterCounters.allocateVersioned(
                        aeron,
                        buffer,
                        "ClusterBackup Errors",
                        CLUSTER_BACKUP_ERROR_COUNT_TYPE_ID,
                        clusterId,
                        ClusterBackupVersion.VERSION,
                        ClusterBackupVersion.GIT_SHA);
                }
            }

            if (!(aeron.context().subscriberErrorHandler() instanceof RethrowingErrorHandler))
            {
                throw new ClusterException("Aeron client must use a RethrowingErrorHandler");
            }

            if (!aeron.context().useConductorAgentInvoker())
            {
                throw new ClusterException("Aeron client must use conductor agent invoker");
            }

            if (null == errorCounter)
            {
                throw new ClusterException("error counter must be supplied if aeron client is");
            }

            if (null == countedErrorHandler)
            {
                countedErrorHandler = new CountedErrorHandler(errorHandler, errorCounter);
                if (ownsAeronClient)
                {
                    aeron.context().errorHandler(countedErrorHandler);
                }
            }

            if (null == stateCounter)
            {
                stateCounter = ClusterCounters.allocate(
                    aeron, buffer, "ClusterBackup State", BACKUP_STATE_TYPE_ID, clusterId);
            }

            if (null == liveLogPositionCounter)
            {
                liveLogPositionCounter = ClusterCounters.allocate(
                    aeron, buffer, "ClusterBackup live log position", LIVE_LOG_POSITION_TYPE_ID, clusterId);
            }

            if (null == nextQueryDeadlineMsCounter)
            {
                nextQueryDeadlineMsCounter = ClusterCounters.allocate(
                    aeron, buffer, "ClusterBackup next query deadline in ms", QUERY_DEADLINE_TYPE_ID, clusterId);
            }

            if (null == snapshotRetrieveCounter)
            {
                snapshotRetrieveCounter = ClusterCounters.allocate(
                    aeron,
                    buffer,
                    "ClusterBackup snapshots retrieved",
                    CLUSTER_BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID,
                    clusterId);
            }

            if (null == threadFactory)
            {
                threadFactory = Thread::new;
            }

            if (null == idleStrategySupplier)
            {
                idleStrategySupplier = ClusteredServiceContainer.Configuration.idleStrategySupplier(null);
            }

            if (null == archiveContext)
            {
                archiveContext = new AeronArchive.Context()
                    .controlRequestChannel(AeronArchive.Configuration.localControlChannel())
                    .controlResponseChannel(AeronArchive.Configuration.localControlChannel())
                    .controlRequestStreamId(AeronArchive.Configuration.localControlStreamId());
            }

            archiveContext
                .aeron(aeron)
                .errorHandler(errorHandler)
                .ownsAeronClient(false)
                .lock(NoOpLock.INSTANCE)
                .clientName(clientName);

            if (!archiveContext.controlRequestChannel().startsWith(CommonContext.IPC_CHANNEL))
            {
                throw new ClusterException("local archive control must be IPC");
            }

            if (!archiveContext.controlResponseChannel().startsWith(CommonContext.IPC_CHANNEL))
            {
                throw new ClusterException("local archive control must be IPC");
            }

            if (null == clusterArchiveContext)
            {
                clusterArchiveContext = new AeronArchive.Context();
            }

            clusterArchiveContext
                .aeron(aeron)
                .ownsAeronClient(false)
                .lock(NoOpLock.INSTANCE)
                .clientName(clientName);

            if (null == terminationHook)
            {
                terminationHook = () -> {};
            }

            if (null == credentialsSupplier)
            {
                credentialsSupplier = new NullCredentialsSupplier();
            }

            try
            {
                SourceType.valueOf(sourceType);
            }
            catch (final IllegalArgumentException ex)
            {
                throw new ConfigurationException(
                    "ClusterBackup.Context.sourceType=" + sourceType + " is not valid. Must be one of: " +
                    Arrays.toString(SourceType.values()));
            }

            concludeMarkFile();
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
         * {@link Aeron} client for communicating with the local Media Driver.
         * <p>
         * This client will be closed when the {@link ClusterBackup#close()} or {@link #close()} methods are called
         * if {@link #ownsAeronClient()} is true.
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
         * Should the consensus module attempt to immediately delete {@link #clusterDir()} on startup.
         *
         * @param deleteDirOnStart Attempt deletion.
         * @return this for a fluent API.
         */
        public Context deleteDirOnStart(final boolean deleteDirOnStart)
        {
            this.deleteDirOnStart = deleteDirOnStart;
            return this;
        }

        /**
         * Will the consensus module attempt to immediately delete {@link #clusterDir()} on startup.
         *
         * @return true when directory will be deleted, otherwise false.
         */
        public boolean deleteDirOnStart()
        {
            return deleteDirOnStart;
        }

        /**
         * Set the id for this cluster instance.
         *
         * @param clusterId for this clustered instance.
         * @return this for a fluent API
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CLUSTER_ID_PROP_NAME
         */
        public Context clusterId(final int clusterId)
        {
            this.clusterId = clusterId;
            return this;
        }

        /**
         * Get the id for this cluster instance.
         *
         * @return the id for this cluster instance.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CLUSTER_ID_PROP_NAME
         */
        public int clusterId()
        {
            return clusterId;
        }

        /**
         * Set the directory name to use for the cluster directory.
         *
         * @param clusterDirectoryName to use.
         * @return this for a fluent API.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CLUSTER_DIR_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CLUSTER_DIR_PROP_NAME
         */
        public String clusterDirectoryName()
        {
            return clusterDirectoryName;
        }

        /**
         * Set the directory to use for the cluster directory.
         *
         * @param clusterDir to use.
         * @return this for a fluent API.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CLUSTER_DIR_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CLUSTER_DIR_PROP_NAME
         */
        public File clusterDir()
        {
            return clusterDir;
        }

        /**
         * Get the directory in which the ClusterBackup will store mark file (i.e. {@code cluster-mark.dat}). It
         * defaults to {@link #clusterDir()} if it is not set explicitly via the
         * {@link io.aeron.cluster.service.ClusteredServiceContainer.Configuration#MARK_FILE_DIR_PROP_NAME}.
         *
         * @return the directory in which the ClusterBackup will store mark file (i.e. {@code cluster-mark.dat}).
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#MARK_FILE_DIR_PROP_NAME
         * @see #clusterDir()
         */
        public File markFileDir()
        {
            return markFileDir;
        }

        /**
         * Set the directory in which the ClusterBackup will store mark file (i.e. {@code cluster-mark.dat}).
         *
         * @param markFileDir the directory in which the ClusterBackup will store mark file (i.e. {@code
         *                    cluster-mark.dat}).
         * @return this for a fluent API.
         */
        public ClusterBackup.Context markFileDir(final File markFileDir)
        {
            this.markFileDir = markFileDir;
            return this;
        }

        /**
         * Set the {@link io.aeron.archive.client.AeronArchive.Context} used for communicating with the local Archive.
         *
         * @param archiveContext used for communicating with the local Archive.
         * @return this for a fluent API.
         */
        public Context archiveContext(final AeronArchive.Context archiveContext)
        {
            this.archiveContext = archiveContext;
            return this;
        }

        /**
         * Get the {@link io.aeron.archive.client.AeronArchive.Context} used for communicating with the local Archive.
         *
         * @return the {@link io.aeron.archive.client.AeronArchive.Context} used for communicating with the local
         * Archive.
         */
        public AeronArchive.Context archiveContext()
        {
            return archiveContext;
        }

        /**
         * Set the {@link io.aeron.archive.client.AeronArchive.Context} used for communicating with the remote Archive
         * in the cluster being backed up.
         *
         * @param archiveContext used for communicating with the remote Archive in the cluster being backed up.
         * @return this for a fluent API.
         */
        public Context clusterArchiveContext(final AeronArchive.Context archiveContext)
        {
            this.clusterArchiveContext = archiveContext;
            return this;
        }

        /**
         * Get the {@link io.aeron.archive.client.AeronArchive.Context} used for communicating with the remote Archive
         * in the cluster being backed up.
         *
         * @return the {@link io.aeron.archive.client.AeronArchive.Context} used for communicating the remote Archive
         * in the cluster being backed up.
         */
        public AeronArchive.Context clusterArchiveContext()
        {
            return clusterArchiveContext;
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
        public ClusterBackup.Context idleStrategySupplier(final Supplier<IdleStrategy> idleStrategySupplier)
        {
            this.idleStrategySupplier = idleStrategySupplier;
            return this;
        }

        /**
         * Get a new {@link IdleStrategy} based on configured supplier.
         *
         * @return a new {@link IdleStrategy} based on configured supplier.
         */
        public IdleStrategy idleStrategy()
        {
            return idleStrategySupplier.get();
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
         * Get the {@link ErrorHandler} to be used by the Consensus Module.
         *
         * @return the {@link ErrorHandler} to be used by the Consensus Module.
         */
        public ErrorHandler errorHandler()
        {
            return errorHandler;
        }

        /**
         * Set the {@link ErrorHandler} to be used by the Cluster Backup.
         *
         * @param errorHandler the error handler to be used by the Cluster Backup.
         * @return this for a fluent API
         */
        public Context errorHandler(final ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
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
         * Set the channel parameter for the consensus communication channel.
         *
         * @param channel parameter for the consensus communication channel.
         * @return this for a fluent API.
         * @see ConsensusModule.Configuration#CONSENSUS_CHANNEL_PROP_NAME
         */
        public Context consensusChannel(final String channel)
        {
            consensusChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for the consensus communication channel.
         *
         * @return the channel parameter for the consensus communication channel.
         * @see ConsensusModule.Configuration#CONSENSUS_CHANNEL_PROP_NAME
         */
        public String consensusChannel()
        {
            return consensusChannel;
        }

        /**
         * Set the stream id for the consensus channel.
         *
         * @param streamId for the consensus channel.
         * @return this for a fluent API
         * @see ConsensusModule.Configuration#CONSENSUS_STREAM_ID_PROP_NAME
         */
        public Context consensusStreamId(final int streamId)
        {
            consensusStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for the consensus channel.
         *
         * @return the stream id for the consensus channel.
         * @see ConsensusModule.Configuration#CONSENSUS_STREAM_ID_PROP_NAME
         */
        public int consensusStreamId()
        {
            return consensusStreamId;
        }

        /**
         * Set the stream id for the consensus module snapshot replay.
         *
         * @param streamId for the consensus module snapshot replay channel.
         * @return this for a fluent API
         * @see io.aeron.cluster.ConsensusModule.Context#snapshotStreamId()
         */
        public Context consensusModuleSnapshotStreamId(final int streamId)
        {
            consensusModuleSnapshotStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for the consensus module snapshot replay channel.
         *
         * @return the stream id for the consensus module snapshot replay channel.
         * @see io.aeron.cluster.ConsensusModule.Context#snapshotStreamId()
         */
        public int consensusModuleSnapshotStreamId()
        {
            return consensusModuleSnapshotStreamId;
        }

        /**
         * Set the stream id for the clustered service snapshot replay.
         *
         * @param streamId for the clustered service snapshot replay channel.
         * @return this for a fluent API
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Context#snapshotStreamId()
         */
        public Context serviceSnapshotStreamId(final int streamId)
        {
            serviceSnapshotStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for the clustered service snapshot replay channel.
         *
         * @return the stream id for the clustered service snapshot replay channel.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Context#snapshotStreamId()
         */
        public int serviceSnapshotStreamId()
        {
            return serviceSnapshotStreamId;
        }

        /**
         * Set the stream id for the cluster log channel.
         *
         * @param streamId for the cluster log channel.
         * @return this for a fluent API
         * @see ConsensusModule.Configuration#LOG_STREAM_ID_PROP_NAME
         */
        public Context logStreamId(final int streamId)
        {
            logStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for the cluster log channel.
         *
         * @return the stream id for the cluster log channel.
         * @see ConsensusModule.Configuration#LOG_STREAM_ID_PROP_NAME
         */
        @Config
        public int logStreamId()
        {
            return logStreamId;
        }

        /**
         * Set the endpoint that will be subscribed to in order to receive logs and snapshots.
         *
         * @param catchupEndpoint to use for the log retrieval.
         * @return catchup endpoint to use for the log retrieval.
         * @see Configuration#catchupEndpoint()
         */
        public Context catchupEndpoint(final String catchupEndpoint)
        {
            this.catchupEndpoint = catchupEndpoint;
            return this;
        }

        /**
         * Get the catchup endpoint to use for log retrieval.
         *
         * @return catchup endpoint to use for the log retrieval.
         * @see Configuration#catchupEndpoint()
         */
        @Config(id = "CLUSTER_BACKUP_CATCHUP_ENDPOINT")
        public String catchupEndpoint()
        {
            return catchupEndpoint;
        }

        /**
         * Set the catchup channel template to use for log and snapshot retrieval.
         *
         * @param catchupChannel to use for the log and snapshot retrieval.
         * @return catchup endpoint to use for the log and snapshot retrieval.
         * @see Configuration#CLUSTER_BACKUP_CATCHUP_CHANNEL_PROP_NAME
         */
        public Context catchupChannel(final String catchupChannel)
        {
            this.catchupChannel = catchupChannel;
            return this;
        }

        /**
         * Get the catchup channel template to use for log and snapshot retrieval.
         *
         * @return catchup endpoint to use for the log and snapshot retrieval.
         * @see Configuration#CLUSTER_BACKUP_CATCHUP_CHANNEL_PROP_NAME
         */
        @Config(id = "CLUSTER_BACKUP_CATCHUP_CHANNEL")
        public String catchupChannel()
        {
            return catchupChannel;
        }

        /**
         * Interval at which a cluster backup will send backup queries.
         *
         * @param clusterBackupIntervalNs between add cluster members and snapshot recording queries.
         * @return this for a fluent API.
         * @see Configuration#CLUSTER_BACKUP_INTERVAL_PROP_NAME
         * @see Configuration#CLUSTER_BACKUP_INTERVAL_DEFAULT_NS
         */
        public Context clusterBackupIntervalNs(final long clusterBackupIntervalNs)
        {
            this.clusterBackupIntervalNs = clusterBackupIntervalNs;
            return this;
        }

        /**
         * Interval at which a cluster backup will send backup queries.
         *
         * @return the interval at which a cluster backup will send backup queries.
         * @see Configuration#CLUSTER_BACKUP_INTERVAL_PROP_NAME
         * @see Configuration#CLUSTER_BACKUP_INTERVAL_DEFAULT_NS
         */
        @Config
        public long clusterBackupIntervalNs()
        {
            return clusterBackupIntervalNs;
        }

        /**
         * Timeout within which a cluster backup will expect a response from a backup query.
         *
         * @param clusterBackupResponseTimeoutNs within which a cluster backup will expect a response.
         * @return this for a fluent API.
         * @see Configuration#CLUSTER_BACKUP_RESPONSE_TIMEOUT_PROP_NAME
         * @see Configuration#CLUSTER_BACKUP_RESPONSE_TIMEOUT_DEFAULT_NS
         */
        public Context clusterBackupResponseTimeoutNs(final long clusterBackupResponseTimeoutNs)
        {
            this.clusterBackupResponseTimeoutNs = clusterBackupResponseTimeoutNs;
            return this;
        }

        /**
         * Timeout within which a cluster backup will expect a response from a backup query.
         *
         * @return timeout within which a cluster backup will expect a response from a backup query.
         * @see Configuration#CLUSTER_BACKUP_RESPONSE_TIMEOUT_PROP_NAME
         * @see Configuration#CLUSTER_BACKUP_RESPONSE_TIMEOUT_DEFAULT_NS
         */
        @Config
        public long clusterBackupResponseTimeoutNs()
        {
            return clusterBackupResponseTimeoutNs;
        }

        /**
         * Timeout within which a cluster backup will expect progress.
         *
         * @param clusterBackupProgressTimeoutNs within which a cluster backup will expect a response.
         * @return this for a fluent API.
         * @see Configuration#CLUSTER_BACKUP_PROGRESS_TIMEOUT_PROP_NAME
         * @see Configuration#CLUSTER_BACKUP_PROGRESS_TIMEOUT_DEFAULT_NS
         */
        public Context clusterBackupProgressTimeoutNs(final long clusterBackupProgressTimeoutNs)
        {
            this.clusterBackupProgressTimeoutNs = clusterBackupProgressTimeoutNs;
            return this;
        }

        /**
         * Timeout within which a cluster backup will expect progress.
         *
         * @return timeout within which a cluster backup will expect progress.
         * @see Configuration#CLUSTER_BACKUP_PROGRESS_TIMEOUT_PROP_NAME
         * @see Configuration#CLUSTER_BACKUP_PROGRESS_TIMEOUT_DEFAULT_NS
         */
        @Config
        public long clusterBackupProgressTimeoutNs()
        {
            return clusterBackupProgressTimeoutNs;
        }

        /**
         * Interval at which the cluster backup is re-initialised after an exception has been thrown.
         *
         * @param clusterBackupCoolDownIntervalNs time before the cluster backup is re-initialised.
         * @return this for a fluent API.
         * @see Configuration#CLUSTER_BACKUP_COOL_DOWN_INTERVAL_PROP_NAME
         * @see Configuration#CLUSTER_BACKUP_COOL_DOWN_INTERVAL_DEFAULT_NS
         */
        public Context clusterBackupCoolDownIntervalNs(final long clusterBackupCoolDownIntervalNs)
        {
            this.clusterBackupCoolDownIntervalNs = clusterBackupCoolDownIntervalNs;
            return this;
        }

        /**
         * Interval at which the cluster backup is re-initialised after an exception has been thrown.
         *
         * @return interval at which the cluster backup is re-initialised after an exception has been thrown.
         * @see Configuration#CLUSTER_BACKUP_COOL_DOWN_INTERVAL_PROP_NAME
         * @see Configuration#CLUSTER_BACKUP_COOL_DOWN_INTERVAL_DEFAULT_NS
         */
        @Config
        public long clusterBackupCoolDownIntervalNs()
        {
            return clusterBackupCoolDownIntervalNs;
        }

        /**
         * String representing the cluster members consensus endpoints.
         * <p>
         * {@code "endpoint,endpoint,endpoint"}
         *
         * @param endpoints which are to be contacted for joining the cluster.
         * @return this for a fluent API.
         */
        public Context clusterConsensusEndpoints(final String endpoints)
        {
            this.clusterConsensusEndpoints = endpoints;
            return this;
        }

        /**
         * The endpoints representing cluster members of the cluster to attempt to a backup from.
         *
         * @return members of the cluster to attempt to request a backup from.
         */
        public String clusterConsensusEndpoints()
        {
            return clusterConsensusEndpoints;
        }

        /**
         * Set the {@link Runnable} that is called when the {@link ClusterBackup} processes a termination action.
         *
         * @param terminationHook that can be used to terminate.
         * @return this for a fluent API.
         */
        public Context terminationHook(final Runnable terminationHook)
        {
            this.terminationHook = terminationHook;
            return this;
        }

        /**
         * Get the {@link Runnable} that is called when the {@link ClusterBackup} processes a termination action.
         *
         * @return the {@link Runnable} that can be used to terminate.
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
         * Get the counter for the current state of the cluster backup.
         *
         * @return the counter for the current state of the cluster backup.
         * @see ClusterBackup.State
         */
        public Counter stateCounter()
        {
            return stateCounter;
        }

        /**
         * Set the counter for the current state of the cluster backup.
         *
         * @param stateCounter the counter for the current state of the cluster backup.
         * @return this for a fluent API.
         * @see ClusterBackup.State
         */
        public Context stateCounter(final Counter stateCounter)
        {
            this.stateCounter = stateCounter;
            return this;
        }

        /**
         * Get the counter for the current position of the live log.
         *
         * @return the counter for the current position of the live log.
         */
        public Counter liveLogPositionCounter()
        {
            return liveLogPositionCounter;
        }

        /**
         * Set the counter for the current position of the live log.
         *
         * @param liveLogPositionCounter the counter for the current position of the live log.
         * @return this for a fluent API.
         */
        public Context liveLogPositionCounter(final Counter liveLogPositionCounter)
        {
            this.liveLogPositionCounter = liveLogPositionCounter;
            return this;
        }

        /**
         * Set the counter for the number of snapshots retrieved by the backup from the cluster.
         *
         * @param snapshotRetrieveCounter the counter to use for snapshots retrieved.
         * @return this for a fluent API.
         */
        public Context snapshotRetrieveCounter(final Counter snapshotRetrieveCounter)
        {
            this.snapshotRetrieveCounter = snapshotRetrieveCounter;
            return this;
        }

        /**
         * Get the counter for the number of snapshots retrieved by the backup from the cluster.
         *
         * @return counter for the number of snapshots retrieved by the backup from the cluster.
         */
        public Counter snapshotRetrieveCounter()
        {
            return snapshotRetrieveCounter;
        }

        /**
         * Get the counter for the next query deadline ms.
         *
         * @return the counter for the next query deadline ms.
         */
        public Counter nextQueryDeadlineMsCounter()
        {
            return nextQueryDeadlineMsCounter;
        }

        /**
         * Set the counter for the next query deadline ms.
         *
         * @param nextQueryDeadlineMsCounter the counter for the next query deadline ms.
         * @return this for a fluent API.
         */
        public Context nextQueryDeadlineMsCounter(final Counter nextQueryDeadlineMsCounter)
        {
            this.nextQueryDeadlineMsCounter = nextQueryDeadlineMsCounter;
            return this;
        }

        /**
         * Get the {@link ClusterBackupEventsListener} in use for the backup agent.
         *
         * @return {@link ClusterBackupEventsListener} in use for the backup agent.
         * @see ClusterBackupEventsListener
         */
        public ClusterBackupEventsListener eventsListener()
        {
            return eventsListener;
        }

        /**
         * Set the {@link ClusterBackupEventsListener} to use for the backup agent.
         *
         * @param eventsListener to use for the backup agent.
         * @return this for a fluent API.
         * @see ClusterBackupEventsListener
         */
        public Context eventsListener(final ClusterBackupEventsListener eventsListener)
        {
            this.eventsListener = eventsListener;
            return this;
        }

        /**
         * Should an {@link AgentInvoker} be used for running the {@link ClusterBackup} rather than run it on
         * a thread with a {@link AgentRunner}.
         *
         * @param useAgentInvoker use {@link AgentInvoker} be used for running the {@link ClusterBackup}?
         * @return this for a fluent API.
         */
        public Context useAgentInvoker(final boolean useAgentInvoker)
        {
            this.useAgentInvoker = useAgentInvoker;
            return this;
        }

        /**
         * Should an {@link AgentInvoker} be used for running the {@link ClusterBackup} rather than run it on
         * a thread with a {@link AgentRunner}.
         *
         * @return true if the {@link ClusterBackup} will be run with an {@link AgentInvoker} otherwise false.
         */
        public boolean useAgentInvoker()
        {
            return useAgentInvoker;
        }

        /**
         * Set the {@link CredentialsSupplier} to be used for authentication with the cluster.
         *
         * @param credentialsSupplier to be used for authentication with the cluster.
         * @return this for fluent API.
         */
        public Context credentialsSupplier(final CredentialsSupplier credentialsSupplier)
        {
            this.credentialsSupplier = credentialsSupplier;
            return this;
        }

        /**
         * Get the {@link CredentialsSupplier} to be used for authentication with the cluster.
         *
         * @return the {@link CredentialsSupplier} to be used for authentication with the cluster.
         */
        public CredentialsSupplier credentialsSupplier()
        {
            return this.credentialsSupplier;
        }

        /**
         * Set the {@link SourceType} to be used for this backup instance.
         *
         * @param sourceType type of sources to receive log traffic from.
         * @return this for a fluent API
         */
        public Context sourceType(final SourceType sourceType)
        {
            this.sourceType = sourceType.name();
            return this;
        }

        /**
         * Get the currently configured source type.
         *
         * @return source type for this backup instance.
         * @throws IllegalArgumentException if the configured source type is not one of {@link SourceType}
         */
        @Config(id = "CLUSTER_BACKUP_SOURCE_TYPE")
        public SourceType sourceType()
        {
            return SourceType.valueOf(sourceType);
        }

        /**
         * Timeout when no progress it detected when performing an archive replication (typically for snapshots).
         *
         * @param timeoutNs timeout in nanoseconds
         * @return this for a fluent API.
         * @see ConsensusModule.Configuration#replicationProgressTimeoutNs()
         */
        public Context replicationProgressTimeoutNs(final long timeoutNs)
        {
            this.replicationProgressTimeoutNs = timeoutNs;
            return this;
        }

        /**
         * Timeout when no progress it detected when performing an archive replication (typically for snapshots).
         *
         * @return timeout in nanoseconds.
         * @see ConsensusModule.Configuration#replicationProgressTimeoutNs()
         */
        @Config(id = "CLUSTER_REPLICATION_PROGRESS_TIMEOUT")
        public long replicationProgressTimeoutNs()
        {
            return replicationProgressTimeoutNs;
        }

        /**
         * Interval between checks for replication progress.  Defaults to {@link Context#replicationProgressTimeoutNs()}
         * divided by <code>10</code>.
         *
         * @param intervalNs timeout in nanoseconds
         * @return this for a fluent API.
         * @see ConsensusModule.Configuration#replicationProgressIntervalNs()
         */
        public Context replicationProgressIntervalNs(final long intervalNs)
        {
            this.replicationProgressIntervalNs = intervalNs;
            return this;
        }

        /**
         * Interval between checks for replication progress.
         *
         * @return timeout in nanoseconds.
         * @see ConsensusModule.Configuration#replicationProgressIntervalNs()
         */
        @Config(id = "CLUSTER_REPLICATION_PROGRESS_INTERVAL")
        public long replicationProgressIntervalNs()
        {
            return replicationProgressIntervalNs;
        }

        /**
         * Where to start the replay from the cluster used for the backup. Doesn't set a specific position, but instead
         * it will use an enum value to derive appropriate place to start. This only applies to the first replay used
         * for this backup.
         *
         * @param replayStart determines the position to be used to start the backup replay from.
         * @return this for a fluent API.
         * @see Configuration.ReplayStart
         * @see Configuration#CLUSTER_INITIAL_REPLAY_START_DEFAULT
         */
        public Context initialReplayStart(final Configuration.ReplayStart replayStart)
        {
            this.initialReplayStart = replayStart;
            return this;
        }

        /**
         * Get the place for the cluster replay to start from when no local copy of the log exists.
         *
         * @return the cluster replay start.
         * @see Configuration.ReplayStart
         * @see Configuration#CLUSTER_INITIAL_REPLAY_START_DEFAULT
         */
        @Config(id = "CLUSTER_INITIAL_REPLAY_START")
        public Configuration.ReplayStart initialReplayStart()
        {
            return this.initialReplayStart;
        }

        /**
         * Delete the cluster directory.
         */
        public void deleteDirectory()
        {
            if (null != clusterDir)
            {
                IoUtil.delete(clusterDir, false);
            }
        }

        /**
         * Close the context and free applicable resources.
         * <p>
         * If {@link #ownsAeronClient()} is true then the {@link #aeron()} client will be closed.
         */
        public void close()
        {
            if (ownsAeronClient)
            {
                CloseHelper.close(countedErrorHandler, aeron);
            }
            else if (!aeron.isClosed())
            {
                CloseHelper.close(countedErrorHandler, stateCounter);
                CloseHelper.close(countedErrorHandler, liveLogPositionCounter);
            }

            CloseHelper.close(markFile);
        }

        /**
         * {@inheritDoc}
         */
        public String toString()
        {
            return "ClusterBackup.Context" +
                "\n{" +
                "\n    isConcluded=" + isConcluded() +
                "\n    ownsAeronClient=" + ownsAeronClient +
                "\n    aeronDirectoryName='" + aeronDirectoryName + '\'' +
                "\n    aeron=" + aeron +
                "\n    clusterId=" + clusterId +
                "\n    consensusChannel='" + consensusChannel + '\'' +
                "\n    consensusStreamId=" + consensusStreamId +
                "\n    consensusModuleSnapshotStreamId=" + consensusModuleSnapshotStreamId +
                "\n    serviceSnapshotStreamId=" + serviceSnapshotStreamId +
                "\n    logStreamId=" + logStreamId +
                "\n    catchupEndpoint='" + catchupEndpoint + '\'' +
                "\n    catchupChannel='" + catchupChannel + '\'' +
                "\n    clusterBackupIntervalNs=" + clusterBackupIntervalNs +
                "\n    clusterBackupResponseTimeoutNs=" + clusterBackupResponseTimeoutNs +
                "\n    clusterBackupProgressTimeoutNs=" + clusterBackupProgressTimeoutNs +
                "\n    clusterBackupCoolDownIntervalNs=" + clusterBackupCoolDownIntervalNs +
                "\n    errorBufferLength=" + errorBufferLength +
                "\n    deleteDirOnStart=" + deleteDirOnStart +
                "\n    useAgentInvoker=" + useAgentInvoker +
                "\n    clusterDirectoryName='" + clusterDirectoryName + '\'' +
                "\n    clusterDir=" + clusterDir +
                "\n    markFile=" + markFile +
                "\n    clusterConsensusEndpoints='" + clusterConsensusEndpoints + '\'' +
                "\n    threadFactory=" + threadFactory +
                "\n    epochClock=" + epochClock +
                "\n    idleStrategySupplier=" + idleStrategySupplier +
                "\n    errorLog=" + errorLog +
                "\n    errorHandler=" + errorHandler +
                "\n    errorCounter=" + errorCounter +
                "\n    countedErrorHandler=" + countedErrorHandler +
                "\n    stateCounter=" + stateCounter +
                "\n    liveLogPositionCounter=" + liveLogPositionCounter +
                "\n    nextQueryDeadlineMsCounter=" + nextQueryDeadlineMsCounter +
                "\n    archiveContext=" + archiveContext +
                "\n    clusterArchiveContext=" + clusterArchiveContext +
                "\n    terminationHook=" + terminationHook +
                "\n    eventsListener=" + eventsListener +
                "\n}";
        }

        private void concludeMarkFile()
        {
            ClusterMarkFile.checkHeaderLength(
                aeron.context().aeronDirectoryName(), null, null, null, null);

            markFile.encoder()
                .archiveStreamId(archiveContext.controlRequestStreamId())
                .serviceStreamId(ClusteredServiceContainer.Configuration.serviceStreamId())
                .consensusModuleStreamId(ClusteredServiceContainer.Configuration.consensusModuleStreamId())
                .ingressStreamId(AeronCluster.Configuration.ingressStreamId())
                .memberId(-1)
                .serviceId(SERVICE_ID)
                .clusterId(ClusteredServiceContainer.Configuration.clusterId())
                .aeronDirectory(aeron.context().aeronDirectoryName())
                .controlChannel(null)
                .ingressChannel(null)
                .serviceName(null)
                .authenticator(null);

            markFile.signalReady(epochClock.time());
        }
    }
}
