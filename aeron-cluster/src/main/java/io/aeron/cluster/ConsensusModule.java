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
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.AdminRequestDecoder;
import io.aeron.cluster.codecs.AdminRequestType;
import io.aeron.cluster.codecs.BackupQueryDecoder;
import io.aeron.cluster.codecs.HeartbeatRequestDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.StandbySnapshotDecoder;
import io.aeron.cluster.codecs.mark.ClusterComponentType;
import io.aeron.cluster.service.ClusterClock;
import io.aeron.cluster.service.ClusterCounters;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.cluster.service.SnapshotDurationTracker;
import io.aeron.config.Config;
import io.aeron.config.DefaultType;
import io.aeron.driver.DutyCycleTracker;
import io.aeron.driver.NameResolver;
import io.aeron.driver.status.DutyCycleStallTracker;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.security.Authenticator;
import io.aeron.security.AuthenticatorSupplier;
import io.aeron.security.AuthorisationService;
import io.aeron.security.AuthorisationServiceSupplier;
import io.aeron.security.DefaultAuthenticatorSupplier;
import io.aeron.version.Versioned;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.MarkFile;
import org.agrona.SemanticVersion;
import org.agrona.Strings;
import org.agrona.SystemUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import static io.aeron.AeronCounters.CLUSTER_ELECTION_COUNT_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_STANDBY_SNAPSHOT_COUNTER_TYPE_ID;
import static io.aeron.AeronCounters.NODE_CONTROL_TOGGLE_TYPE_ID;
import static io.aeron.AeronCounters.validateCounterTypeId;
import static io.aeron.ChannelUri.addAliasIfAbsent;
import static io.aeron.ChannelUri.parse;
import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.CommonContext.INITIAL_TERM_ID_PARAM_NAME;
import static io.aeron.CommonContext.TERM_ID_PARAM_NAME;
import static io.aeron.CommonContext.TERM_OFFSET_PARAM_NAME;
import static io.aeron.CommonContext.UDP_CHANNEL;
import static io.aeron.CommonContext.driverFilePageSize;
import static io.aeron.CommonContext.fallbackLogger;
import static io.aeron.cluster.ConsensusModule.Configuration.CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.CLUSTER_CLOCK_PROP_NAME;
import static io.aeron.cluster.ConsensusModule.Configuration.CLUSTER_NODE_ROLE_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.COMMIT_POSITION_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.CONSENSUS_MODULE_STATE_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.CONTROL_TOGGLE_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.ELECTION_STATE_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.SERVICE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.SNAPSHOT_COUNTER_TYPE_ID;
import static java.lang.Boolean.parseBoolean;
import static org.agrona.BitUtil.findNextPositivePowerOfTwo;
import static org.agrona.SystemUtil.getDurationInNanos;
import static org.agrona.SystemUtil.getSizeAsInt;
import static org.agrona.SystemUtil.loadPropertiesFiles;

/**
 * Component which resides on each node and is responsible for coordinating consensus within a cluster in concert
 * with the lifecycle of clustered services.
 */
@Versioned
public final class ConsensusModule implements AutoCloseable
{
    /**
     * Default set of flags when taking a snapshot.
     */
    public static final int CLUSTER_ACTION_FLAGS_DEFAULT = 0;

    /**
     * Flag for a snapshot taken on a standby node.
     */
    public static final int CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT = 1;

    /**
     * Possible states for the {@link ConsensusModule}.
     * These will be reflected in the {@link Context#moduleStateCounter()} counter.
     */
    public enum State
    {
        /**
         * Starting up and recovering state.
         */
        INIT(0),

        /**
         * Active state with ingress and expired timers appended to the log.
         */
        ACTIVE(1),

        /**
         * Suspended processing of ingress and expired timers.
         */
        SUSPENDED(2),

        /**
         * In the process of taking a snapshot.
         */
        SNAPSHOT(3),

        /**
         * Quitting the cluster and shutting down as soon as services ack without taking a snapshot.
         */
        QUITTING(4),

        /**
         * In the process of terminating the node.
         */
        TERMINATING(5),

        /**
         * Terminal state.
         */
        CLOSED(6);

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
         * Code to be stored in an {@link AtomicCounter} for the enum value.
         *
         * @return code to be stored in an {@link AtomicCounter} for the enum value.
         */
        public final int code()
        {
            return code;
        }

        /**
         * Get the {@link State} encoded in an {@link AtomicCounter}.
         *
         * @param counter to get the current state for.
         * @return the state for the {@link ConsensusModule}.
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
         * Get the {@link State} corresponding to a particular code.
         *
         * @param code representing a {@link State}.
         * @return the {@link State} corresponding to the provided code.
         * @throws ClusterException if the code does not correspond to a valid State.
         */
        public static State get(final long code)
        {
            if (code < 0 || code > (STATES.length - 1))
            {
                throw new ClusterException("invalid state counter code: " + code);
            }

            return STATES[(int)code];
        }

        /**
         * Get the current state of the {@link ConsensusModule}.
         *
         * @param counters  to search within.
         * @param clusterId to which the allocated counter belongs.
         * @return the state of the ConsensusModule or null if not found.
         */
        public static State find(final CountersReader counters, final int clusterId)
        {
            final int counterId = ClusterCounters.find(counters, CONSENSUS_MODULE_STATE_TYPE_ID, clusterId);
            if (Aeron.NULL_VALUE != counterId)
            {
                return State.get(counters.getCounterValue(counterId));
            }

            return null;
        }
    }

    /**
     * Launch an {@link ConsensusModule} with that communicates with an out of process {@link io.aeron.archive.Archive}
     * and {@link io.aeron.driver.MediaDriver} then awaits shutdown signal.
     *
     * @param args command line argument which is a list for properties files as URLs or filenames.
     */
    @SuppressWarnings("try")
    public static void main(final String[] args)
    {
        loadPropertiesFiles(args);

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
            ConsensusModule ignored = launch(new Context().terminationHook(barrier::signalAll)))
        {
            barrier.await();
            System.out.println("Shutdown ConsensusModule...");
        }
    }

    private final Context ctx;
    private final ConsensusModuleAgent conductor;
    private final AgentRunner conductorRunner;
    private final AgentInvoker conductorInvoker;

    ConsensusModule(final Context ctx)
    {
        try
        {
            ctx.conclude();
            this.ctx = ctx;

            conductor = new ConsensusModuleAgent(ctx);

            if (ctx.useAgentInvoker())
            {
                conductorInvoker = new AgentInvoker(ctx.errorHandler(), ctx.errorCounter(), conductor);
                conductorRunner = null;
            }
            else
            {
                conductorRunner = new AgentRunner(
                    ctx.idleStrategy(), ctx.errorHandler(), ctx.errorCounter(), conductor);
                conductorInvoker = null;
            }
        }
        catch (final ConcurrentConcludeException ex)
        {
            throw ex;
        }
        catch (final Exception ex)
        {
            if (null != ctx.clusterMarkFile())
            {
                ctx.clusterMarkFile().signalFailedStart();
                ctx.clusterMarkFile().force();
            }

            CloseHelper.quietClose(ctx::close);
            throw ex;
        }
    }

    /**
     * Launch an {@link ConsensusModule} using a default configuration.
     *
     * @return a new instance of an {@link ConsensusModule}.
     */
    public static ConsensusModule launch()
    {
        return launch(new Context());
    }

    /**
     * Launch an {@link ConsensusModule} by providing a configuration context.
     *
     * @param ctx for the configuration parameters.
     * @return a new instance of an {@link ConsensusModule}.
     */
    public static ConsensusModule launch(final Context ctx)
    {
        final ConsensusModule consensusModule = new ConsensusModule(ctx);

        if (null != consensusModule.conductorRunner)
        {
            AgentRunner.startOnThread(consensusModule.conductorRunner, ctx.threadFactory());
        }
        else
        {
            consensusModule.conductorInvoker.start();
        }

        return consensusModule;
    }

    /**
     * Get the {@link ConsensusModule.Context} that is used by this {@link ConsensusModule}.
     *
     * @return the {@link ConsensusModule.Context} that is used by this {@link ConsensusModule}.
     */
    public Context context()
    {
        return ctx;
    }

    /**
     * Get the {@link AgentInvoker} for the consensus module.
     *
     * @return the {@link AgentInvoker} for the consensus module.
     */
    public AgentInvoker conductorAgentInvoker()
    {
        return conductorInvoker;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.closeAll(conductorRunner, conductorInvoker);
    }

    /**
     * Configuration options for cluster.
     */
    @Config(existsInC = false)
    public static final class Configuration
    {
        /**
         * Major version of the network protocol from consensus module to consensus module. If these don't match then
         * consensus modules are not compatible.
         */
        public static final int PROTOCOL_MAJOR_VERSION = 1;

        /**
         * Minor version of the network protocol from consensus module to consensus module. If these don't match then
         * some features may not be available.
         */
        public static final int PROTOCOL_MINOR_VERSION = 0;

        /**
         * Patch version of the network protocol from consensus module to consensus module. If these don't match then
         * bug fixes may not have been applied.
         */
        public static final int PROTOCOL_PATCH_VERSION = 0;

        /**
         * Combined semantic version for the client to consensus module protocol.
         *
         * @see SemanticVersion
         */
        public static final int PROTOCOL_SEMANTIC_VERSION = SemanticVersion.compose(
            PROTOCOL_MAJOR_VERSION, PROTOCOL_MINOR_VERSION, PROTOCOL_PATCH_VERSION);

        /**
         * Type of snapshot for this component.
         */
        public static final long SNAPSHOT_TYPE_ID = 1;

        /**
         * Property name for the limit for fragments to be consumed on each poll of ingress.
         */
        @Config
        public static final String CLUSTER_INGRESS_FRAGMENT_LIMIT_PROP_NAME = "aeron.cluster.ingress.fragment.limit";

        /**
         * Default for the limit for fragments to be consumed on each poll of ingress.
         */
        @Config
        public static final int CLUSTER_INGRESS_FRAGMENT_LIMIT_DEFAULT = 50;

        /**
         * Property name for whether IPC ingress is allowed or not.
         */
        @Config
        public static final String CLUSTER_INGRESS_IPC_ALLOWED_PROP_NAME = "aeron.cluster.ingress.ipc.allowed";

        /**
         * Default for whether IPC ingress is allowed or not.
         */
        @Config
        public static final String CLUSTER_INGRESS_IPC_ALLOWED_DEFAULT = "false";

        /**
         * Service ID to identify a snapshot in the {@link RecordingLog} for the consensus module.
         */
        public static final int SERVICE_ID = Aeron.NULL_VALUE;

        /**
         * Property name for the identity of the cluster member.
         */
        @Config
        public static final String CLUSTER_MEMBER_ID_PROP_NAME = "aeron.cluster.member.id";

        /**
         * Default property for the cluster member identity.
         */
        @Config
        public static final int CLUSTER_MEMBER_ID_DEFAULT = 0;

        /**
         * Property name for the identity of the appointed leader. This is when automated leader elections are
         * not employed.
         * <p>
         * This feature is for testing and not recommended for production usage.
         */
        @Config
        public static final String APPOINTED_LEADER_ID_PROP_NAME = "aeron.cluster.appointed.leader.id";

        /**
         * Default property for the appointed cluster leader id. A value of {@link Aeron#NULL_VALUE} means no leader
         * has been appointed and thus an automated leader election should occur.
         */
        @Config
        public static final int APPOINTED_LEADER_ID_DEFAULT = Aeron.NULL_VALUE;

        /**
         * Property name for the comma separated list of cluster member endpoints.
         * <p>
         * <code>
         * 0,ingress:port,consensus:port,log:port,catchup:port,archive:port| \
         * 1,ingress:port,consensus:port,log:port,catchup:port,archive:port| ...
         * </code>
         * <p>
         * The ingress endpoints will be used as the endpoint substituted into the
         * {@link io.aeron.cluster.client.AeronCluster.Configuration#INGRESS_CHANNEL_PROP_NAME} if the endpoint
         * is not provided when unicast.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String CLUSTER_MEMBERS_PROP_NAME = "aeron.cluster.members";

        /**
         * Property name for the comma separated list of cluster consensus endpoints used for cluster
         * backup and cluster standby nodes.
         */
        @Config
        public static final String CLUSTER_CONSENSUS_ENDPOINTS_PROP_NAME = "aeron.cluster.consensus.endpoints";

        /**
         * Default property for the list of cluster consensus endpoints.
         */
        @Config
        public static final String CLUSTER_CONSENSUS_ENDPOINTS_DEFAULT = "";

        /**
         * Channel for the clustered log.
         */
        @Config
        public static final String LOG_CHANNEL_PROP_NAME = "aeron.cluster.log.channel";

        /**
         * Channel for the clustered log. This channel can exist for a potentially long time given cluster operation
         * so attention should be given to configuration such as term-length and mtu.
         */
        @Config
        public static final String LOG_CHANNEL_DEFAULT = "aeron:udp?term-length=64m";

        /**
         * Property name for the comma separated list of member endpoints.
         * <p>
         * <code>
         * ingress:port,consensus:port,log:port,catchup:port,archive:port
         * </code>
         *
         * @see #CLUSTER_MEMBERS_PROP_NAME
         */
        @Config
        public static final String MEMBER_ENDPOINTS_PROP_NAME = "aeron.cluster.member.endpoints";

        /**
         * Default property for member endpoints.
         */
        @Config
        public static final String MEMBER_ENDPOINTS_DEFAULT = "";

        /**
         * Stream id within a channel for the clustered log.
         */
        @Config
        public static final String LOG_STREAM_ID_PROP_NAME = "aeron.cluster.log.stream.id";

        /**
         * Stream id within a channel for the clustered log.
         */
        @Config
        public static final int LOG_STREAM_ID_DEFAULT = 100;

        /**
         * Channel to be used for archiving snapshots.
         */
        public static final String SNAPSHOT_CHANNEL_DEFAULT = "aeron:ipc?alias=snapshot";

        /**
         * Stream id for the archived snapshots within a channel.
         */
        public static final int SNAPSHOT_STREAM_ID_DEFAULT = 107;

        /**
         * Message detail to be sent when max concurrent session limit is reached.
         */
        public static final String SESSION_LIMIT_MSG = "concurrent session limit";

        /**
         * Message detail to be sent when a session is rejected due to authentication.
         */
        public static final String SESSION_REJECTED_MSG = "session failed authentication";

        /**
         * Message detail to be sent when a session has an invalid client version.
         */
        public static final String SESSION_INVALID_VERSION_MSG = "invalid client version";

        /**
         * Channel to be used communicating cluster consensus to each other. This can be used for default
         * configuration with the endpoints replaced with those provided by {@link #CLUSTER_MEMBERS_PROP_NAME}.
         */
        @Config
        public static final String CONSENSUS_CHANNEL_PROP_NAME = "aeron.cluster.consensus.channel";

        /**
         * Channel to be used for communicating cluster consensus to each other. This can be used for default
         * configuration with the endpoints replaced with those provided by {@link #CLUSTER_MEMBERS_PROP_NAME}.
         */
        @Config
        public static final String CONSENSUS_CHANNEL_DEFAULT = "aeron:udp?term-length=64k";

        /**
         * Stream id within a channel for communicating consensus messages.
         */
        @Config
        public static final String CONSENSUS_STREAM_ID_PROP_NAME = "aeron.cluster.consensus.stream.id";

        /**
         * Stream id for the communicating consensus messages.
         */
        @Config
        public static final int CONSENSUS_STREAM_ID_DEFAULT = 108;

        /**
         * Channel to be used for replicating logs and snapshots from other archives to the local one.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String REPLICATION_CHANNEL_PROP_NAME = "aeron.cluster.replication.channel";

        /**
         * Channel template used for replaying logs to a follower using the {@link ClusterMember#catchupEndpoint()}.
         */
        @Config
        public static final String FOLLOWER_CATCHUP_CHANNEL_PROP_NAME = "aeron.cluster.follower.catchup.channel";

        /**
         * Default channel template used for replaying logs to a follower using the
         * {@link ClusterMember#catchupEndpoint()}.
         */
        @Config
        public static final String FOLLOWER_CATCHUP_CHANNEL_DEFAULT = UDP_CHANNEL;

        /**
         * Channel used to build the control request channel for the leader Archive.
         */
        @Config
        public static final String LEADER_ARCHIVE_CONTROL_CHANNEL_PROP_NAME =
            "aeron.cluster.leader.archive.control.channel";

        /**
         * Default channel used to build the control request channel for the leader Archive.
         */
        @Config
        public static final String LEADER_ARCHIVE_CONTROL_CHANNEL_DEFAULT = "aeron:udp?term-length=64k";

        /**
         * Counter type id for the consensus module state.
         */
        public static final int CONSENSUS_MODULE_STATE_TYPE_ID = AeronCounters.CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID;

        /**
         * Counter type id for the cluster node role.
         */
        public static final int CLUSTER_NODE_ROLE_TYPE_ID =
            ClusteredServiceContainer.Configuration.CLUSTER_NODE_ROLE_TYPE_ID;

        /**
         * Counter type id for the control toggle.
         */
        public static final int CONTROL_TOGGLE_TYPE_ID = ClusterControl.CONTROL_TOGGLE_TYPE_ID;

        /**
         * Counter type id of a commit position.
         */
        public static final int COMMIT_POSITION_TYPE_ID =
            ClusteredServiceContainer.Configuration.COMMIT_POSITION_TYPE_ID;

        /**
         * Type id of a recovery state counter.
         */
        public static final int RECOVERY_STATE_TYPE_ID = AeronCounters.CLUSTER_RECOVERY_STATE_TYPE_ID;

        /**
         * Counter type id for count of snapshots taken.
         */
        public static final int SNAPSHOT_COUNTER_TYPE_ID = AeronCounters.CLUSTER_SNAPSHOT_COUNTER_TYPE_ID;

        /**
         * Type id for election state counter.
         */
        public static final int ELECTION_STATE_TYPE_ID = AeronCounters.CLUSTER_ELECTION_STATE_TYPE_ID;

        /**
         * Counter type id for the consensus module error count.
         */
        public static final int CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID =
            AeronCounters.CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID;

        /**
         * Counter type id for the number of cluster clients which have been timed out.
         */
        public static final int CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID =
            AeronCounters.CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID;

        /**
         * Counter type id for the number of invalid requests which the cluster has received.
         */
        public static final int CLUSTER_INVALID_REQUEST_COUNT_TYPE_ID =
            AeronCounters.CLUSTER_INVALID_REQUEST_COUNT_TYPE_ID;

        /**
         * The number of services in this cluster instance.
         *
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SERVICE_ID_PROP_NAME
         */
        @Config
        public static final String SERVICE_COUNT_PROP_NAME = "aeron.cluster.service.count";

        /**
         * The number of services in this cluster instance.
         */
        @Config
        public static final int SERVICE_COUNT_DEFAULT = 1;

        /**
         * Maximum number of cluster sessions that can be active concurrently.
         */
        @Config
        public static final String MAX_CONCURRENT_SESSIONS_PROP_NAME = "aeron.cluster.max.sessions";

        /**
         * Maximum number of cluster sessions that can be active concurrently.
         */
        @Config
        public static final int MAX_CONCURRENT_SESSIONS_DEFAULT = 10;

        /**
         * Timeout for a session if no activity is observed.
         */
        @Config
        public static final String SESSION_TIMEOUT_PROP_NAME = "aeron.cluster.session.timeout";

        /**
         * Timeout for a session if no activity is observed.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 10L * 1000 * 1000 * 1000)
        public static final long SESSION_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(10);

        /**
         * Timeout for a leader if no heartbeat is received by another member.
         */
        @Config
        public static final String LEADER_HEARTBEAT_TIMEOUT_PROP_NAME = "aeron.cluster.leader.heartbeat.timeout";

        /**
         * Timeout for a leader if no heartbeat is received by another member.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 10L * 1000 * 1000 * 1000)
        public static final long LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(10);

        /**
         * Interval at which a leader will send heartbeats if the log is not progressing.
         */
        @Config
        public static final String LEADER_HEARTBEAT_INTERVAL_PROP_NAME = "aeron.cluster.leader.heartbeat.interval";

        /**
         * Interval at which a leader will send heartbeats if the log is not progressing.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 200L * 1000 * 1000)
        public static final long LEADER_HEARTBEAT_INTERVAL_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(200);

        /**
         * Timeout after which an election vote will be attempted after startup while waiting to canvass the status
         * of members if a majority has been heard from.
         */
        @Config
        public static final String STARTUP_CANVASS_TIMEOUT_PROP_NAME = "aeron.cluster.startup.canvass.timeout";

        /**
         * Default timeout after which an election vote will be attempted on startup when waiting to canvass the
         * status of all members before going for a majority if possible.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 60L * 1000 * 1000 * 1000)
        public static final long STARTUP_CANVASS_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(60);

        /**
         * Timeout after which an election fails if the candidate does not get a majority of votes.
         */
        @Config
        public static final String ELECTION_TIMEOUT_PROP_NAME = "aeron.cluster.election.timeout";

        /**
         * Default timeout after which an election fails if the candidate does not get a majority of votes.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 1000L * 1000 * 1000)
        public static final long ELECTION_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(1);

        /**
         * Interval at which a member will send out status updates during election phases.
         */
        @Config
        public static final String ELECTION_STATUS_INTERVAL_PROP_NAME = "aeron.cluster.election.status.interval";

        /**
         * Default interval at which a member will send out status updates during election phases.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 100L * 1000 * 1000)
        public static final long ELECTION_STATUS_INTERVAL_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(100);

        /**
         * Name of the system property for specifying a supplier of {@link Authenticator} for the cluster.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "io.aeron.security.DefaultAuthenticatorSupplier")
        public static final String AUTHENTICATOR_SUPPLIER_PROP_NAME = "aeron.cluster.authenticator.supplier";

        /**
         * Name of the system property for specifying a supplier of {@link AuthorisationService} for the cluster.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME =
            "aeron.cluster.authorisation.service.supplier";

        /**
         * Default {@link AuthorisationServiceSupplier} that returns an {@link AuthorisationService} that
         * allows the commands necessary for Cluster Backup and Aeron Cluster Standby.
         * ({@link AllowBackupAndStandbyAuthorisationService}).
         */
        public static final AuthorisationServiceSupplier DEFAULT_AUTHORISATION_SERVICE_SUPPLIER =
            () -> AllowBackupAndStandbyAuthorisationService.INSTANCE;

        /**
         * Size in bytes of the error buffer for the cluster.
         */
        @Config
        public static final String ERROR_BUFFER_LENGTH_PROP_NAME = "aeron.cluster.error.buffer.length";

        /**
         * Size in bytes of the error buffer for the cluster.
         */
        @Config
        public static final int ERROR_BUFFER_LENGTH_DEFAULT = ClusterMarkFile.ERROR_BUFFER_MIN_LENGTH;

        /**
         * Timeout a leader will wait on getting termination ACKs from followers.
         */
        @Config
        public static final String TERMINATION_TIMEOUT_PROP_NAME = "aeron.cluster.termination.timeout";

        /**
         * Property name for threshold value for the consensus module agent work cycle threshold to track
         * for being exceeded.
         */
        @Config
        public static final String CYCLE_THRESHOLD_PROP_NAME = "aeron.cluster.cycle.threshold";

        /**
         * Default threshold value for the consensus module agent work cycle threshold to track for being exceeded.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 1_000_000L)
        public static final long CYCLE_THRESHOLD_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(1);

        /**
         * Property name for threshold value, which is used for tracking total snapshot duration breaches.
         */
        @Config
        public static final String TOTAL_SNAPSHOT_DURATION_THRESHOLD_PROP_NAME =
            "aeron.cluster.total.snapshot.threshold";

        /**
         * Default threshold value, which is used for tracking total snapshot duration breaches.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 1000L * 1000 * 1000)
        public static final long TOTAL_SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS =
            TimeUnit.MILLISECONDS.toNanos(1000);

        /**
         * Default timeout a leader will wait on getting termination ACKs from followers.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 10L * 1000 * 1000 * 1000)
        public static final long TERMINATION_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(10);

        /**
         * Resolution in nanoseconds for each tick of the timer wheel for scheduling deadlines.
         */
        @Config
        public static final String WHEEL_TICK_RESOLUTION_PROP_NAME = "aeron.cluster.wheel.tick.resolution";

        /**
         * Resolution in nanoseconds for each tick of the timer wheel for scheduling deadlines. Defaults to 8ms.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 4L * 1000 * 1000)
        public static final long WHEEL_TICK_RESOLUTION_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(8);

        /**
         * Number of ticks, or spokes, on the timer wheel. Higher number of ticks reduces potential conflicts
         * traded off against memory usage.
         */
        @Config
        public static final String TICKS_PER_WHEEL_PROP_NAME = "aeron.cluster.ticks.per.wheel";

        /**
         * Number of ticks, or spokes, on the timer wheel. Higher number of ticks reduces potential conflicts
         * traded off against memory usage. Defaults to 128 per wheel.
         */
        @Config
        public static final int TICKS_PER_WHEEL_DEFAULT = 128;

        /**
         * The level at which files should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         */
        @Config
        public static final String FILE_SYNC_LEVEL_PROP_NAME = "aeron.cluster.file.sync.level";

        /**
         * Default file sync level of normal writes.
         */
        @Config
        public static final int FILE_SYNC_LEVEL_DEFAULT = 0;

        /**
         * {@link TimerServiceSupplier} to be used for creating the {@link TimerService} used by consensus module.
         */
        @Config
        public static final String TIMER_SERVICE_SUPPLIER_PROP_NAME = "aeron.cluster.timer.service.supplier";

        /**
         * Name of the {@link TimerServiceSupplier} that creates {@link TimerService} based on the timer wheel
         * implementation.
         */
        public static final String TIMER_SERVICE_SUPPLIER_WHEEL = "io.aeron.cluster.WheelTimerServiceSupplier";

        /**
         * Name of the {@link TimerServiceSupplier} that creates a sequence-preserving {@link TimerService} based
         * on a priority heap implementation.
         */
        public static final String TIMER_SERVICE_SUPPLIER_PRIORITY_HEAP =
            "io.aeron.cluster.PriorityHeapTimerServiceSupplier";

        /**
         * Default {@link TimerServiceSupplier}.
         */
        @Config
        public static final String TIMER_SERVICE_SUPPLIER_DEFAULT = TIMER_SERVICE_SUPPLIER_WHEEL;

        /**
         * Property name for the name returned from {@link Agent#roleName()} for the consensus module.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String CLUSTER_CONSENSUS_MODULE_AGENT_ROLE_NAME_PROP_NAME =
            "aeron.cluster.consensus.module.agent.role.name";

        /**
         * Property name for replication progress timeout.
         *
         * @since 1.41.0
         */
        @Config
        public static final String CLUSTER_REPLICATION_PROGRESS_TIMEOUT_PROP_NAME =
            "aeron.cluster.replication.progress.timeout";

        /**
         * Default timeout for replication progress in nanoseconds.
         *
         * @since 1.41.0
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 10L * 1000 * 1000 * 1000)
        public static final long CLUSTER_REPLICATION_PROGRESS_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(10);

        /**
         * Property name for replication progress interval.
         *
         * @since 1.41.0
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 0)
        public static final String CLUSTER_REPLICATION_PROGRESS_INTERVAL_PROP_NAME =
            "aeron.cluster.replication.progress.interval";

        /**
         * Property name of enabling the acceptance of standby snapshots.
         */
        @Config(defaultType = DefaultType.BOOLEAN, defaultBoolean = false)
        public static final String CLUSTER_ACCEPT_STANDBY_SNAPSHOTS_PROP_NAME =
            "aeron.cluster.accept.standby.snapshots";

        /**
         * Property name of setting {@link ClusterClock}. Should specify a fully qualified class name.
         * Defaults to {@link MillisecondClusterClock}.
         *
         * @since 1.44.0
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "io.aeron.cluster.MillisecondClusterClock")
        public static final String CLUSTER_CLOCK_PROP_NAME = "aeron.cluster.clock";

        /**
         * Property name of setting {@link ConsensusModuleExtension}. Should specify a fully qualified class name.
         *
         * @since 1.45.0
         */
        public static final String CONSENSUS_MODULE_EXTENSION_CLASS_NAME_PROP_NAME =
            "aeron.cluster.consensus.module.extension";

        /**
         * Property name of the setting to control whether the consensus module should bind the control endpoint of
         * the consensus publication to its local consensus address.
         */
        @Config(defaultType = DefaultType.BOOLEAN, defaultBoolean = true)
        public static final String CONSENSUS_MODULE_ENABLE_CONTROL_ON_CONSENSUS_CHANNEL_PROP_NAME =
            "aeron.cluster.consensus.control.enable";

        /**
         * Property name of the setting to control whether the consensus module should bind the control endpoint of
         * the consensus publication to its local log address.
         */
        @Config(defaultType = DefaultType.BOOLEAN, defaultBoolean = true)
        public static final String CONSENSUS_MODULE_ENABLE_CONTROL_ON_LOG_CHANNEL_PROP_NAME =
            "aeron.cluster.log.control.enable";

        /**
         * The value {@link #CLUSTER_INGRESS_FRAGMENT_LIMIT_DEFAULT} or system property
         * {@link #CLUSTER_INGRESS_FRAGMENT_LIMIT_PROP_NAME} if set.
         *
         * @return {@link #CLUSTER_INGRESS_FRAGMENT_LIMIT_DEFAULT} or system property
         * {@link #CLUSTER_INGRESS_FRAGMENT_LIMIT_PROP_NAME} if set.
         */
        public static int ingressFragmentLimit()
        {
            return Integer.getInteger(CLUSTER_INGRESS_FRAGMENT_LIMIT_PROP_NAME, CLUSTER_INGRESS_FRAGMENT_LIMIT_DEFAULT);
        }

        /**
         * The value {@link #CLUSTER_INGRESS_IPC_ALLOWED_DEFAULT} or system property
         * {@link #CLUSTER_INGRESS_IPC_ALLOWED_PROP_NAME} if set.
         *
         * @return {@link #CLUSTER_INGRESS_IPC_ALLOWED_DEFAULT} or system property
         * {@link #CLUSTER_INGRESS_IPC_ALLOWED_PROP_NAME} if set.
         */
        public static boolean isIpcIngressAllowed()
        {
            return "true".equalsIgnoreCase(System.getProperty(
                CLUSTER_INGRESS_IPC_ALLOWED_PROP_NAME, CLUSTER_INGRESS_IPC_ALLOWED_DEFAULT));
        }

        /**
         * The value {@link #CLUSTER_MEMBER_ID_DEFAULT} or system property
         * {@link #CLUSTER_MEMBER_ID_PROP_NAME} if set.
         *
         * @return {@link #CLUSTER_MEMBER_ID_DEFAULT} or system property
         * {@link #CLUSTER_MEMBER_ID_PROP_NAME} if set.
         */
        public static int clusterMemberId()
        {
            return Integer.getInteger(CLUSTER_MEMBER_ID_PROP_NAME, CLUSTER_MEMBER_ID_DEFAULT);
        }

        /**
         * The value {@link #APPOINTED_LEADER_ID_DEFAULT} or system property
         * {@link #APPOINTED_LEADER_ID_PROP_NAME} if set.
         * <p>
         * This feature is for testing and not recommended for production usage.
         *
         * @return {@link #APPOINTED_LEADER_ID_DEFAULT} or system property
         * {@link #APPOINTED_LEADER_ID_PROP_NAME} if set.
         */
        public static int appointedLeaderId()
        {
            return Integer.getInteger(APPOINTED_LEADER_ID_PROP_NAME, APPOINTED_LEADER_ID_DEFAULT);
        }

        /**
         * The value of system property {@link #CLUSTER_MEMBERS_PROP_NAME} if set, null otherwise.
         *
         * @return of system property {@link #CLUSTER_MEMBERS_PROP_NAME} if set.
         */
        public static String clusterMembers()
        {
            return System.getProperty(CLUSTER_MEMBERS_PROP_NAME);
        }

        /**
         * The value {@link #CLUSTER_CONSENSUS_ENDPOINTS_DEFAULT} or system property
         * {@link #CLUSTER_CONSENSUS_ENDPOINTS_PROP_NAME} if set.
         *
         * @return {@link #CLUSTER_CONSENSUS_ENDPOINTS_DEFAULT} or system property
         * {@link #CLUSTER_CONSENSUS_ENDPOINTS_PROP_NAME} it set.
         */
        public static String clusterConsensusEndpoints()
        {
            return System.getProperty(CLUSTER_CONSENSUS_ENDPOINTS_PROP_NAME, CLUSTER_CONSENSUS_ENDPOINTS_DEFAULT);
        }

        /**
         * The value {@link #LOG_CHANNEL_DEFAULT} or system property {@link #LOG_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #LOG_CHANNEL_DEFAULT} or system property {@link #LOG_CHANNEL_PROP_NAME} if set.
         */
        public static String logChannel()
        {
            return System.getProperty(LOG_CHANNEL_PROP_NAME, LOG_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #LOG_STREAM_ID_DEFAULT} or system property {@link #LOG_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #LOG_STREAM_ID_DEFAULT} or system property {@link #LOG_STREAM_ID_PROP_NAME} if set.
         */
        public static int logStreamId()
        {
            return Integer.getInteger(LOG_STREAM_ID_PROP_NAME, LOG_STREAM_ID_DEFAULT);
        }

        /**
         * The value {@link #MEMBER_ENDPOINTS_DEFAULT} or system property {@link #MEMBER_ENDPOINTS_PROP_NAME} if set.
         *
         * @return {@link #MEMBER_ENDPOINTS_DEFAULT} or system property {@link #MEMBER_ENDPOINTS_PROP_NAME} if set.
         */
        public static String memberEndpoints()
        {
            return System.getProperty(MEMBER_ENDPOINTS_PROP_NAME, MEMBER_ENDPOINTS_DEFAULT);
        }

        /**
         * The value {@link #SNAPSHOT_CHANNEL_DEFAULT} or system property
         * {@link io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SNAPSHOT_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #SNAPSHOT_CHANNEL_DEFAULT} or system property
         * {@link io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SNAPSHOT_CHANNEL_PROP_NAME} if set.
         */
        public static String snapshotChannel()
        {
            return System.getProperty(
                ClusteredServiceContainer.Configuration.SNAPSHOT_CHANNEL_PROP_NAME, SNAPSHOT_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #SNAPSHOT_STREAM_ID_DEFAULT} or system property
         * {@link io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SNAPSHOT_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #SNAPSHOT_STREAM_ID_DEFAULT} or system property
         * {@link io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SNAPSHOT_STREAM_ID_PROP_NAME} if set.
         */
        public static int snapshotStreamId()
        {
            return Integer.getInteger(
                ClusteredServiceContainer.Configuration.SNAPSHOT_STREAM_ID_PROP_NAME, SNAPSHOT_STREAM_ID_DEFAULT);
        }

        /**
         * The value {@link #SERVICE_COUNT_DEFAULT} or system property
         * {@link #SERVICE_COUNT_PROP_NAME} if set.
         *
         * @return {@link #SERVICE_COUNT_DEFAULT} or system property
         * {@link #SERVICE_COUNT_PROP_NAME} if set.
         */
        public static int serviceCount()
        {
            return Integer.getInteger(SERVICE_COUNT_PROP_NAME, SERVICE_COUNT_DEFAULT);
        }

        /**
         * The value {@link #MAX_CONCURRENT_SESSIONS_DEFAULT} or system property
         * {@link #MAX_CONCURRENT_SESSIONS_PROP_NAME} if set.
         *
         * @return {@link #MAX_CONCURRENT_SESSIONS_DEFAULT} or system property
         * {@link #MAX_CONCURRENT_SESSIONS_PROP_NAME} if set.
         */
        public static int maxConcurrentSessions()
        {
            return Integer.getInteger(MAX_CONCURRENT_SESSIONS_PROP_NAME, MAX_CONCURRENT_SESSIONS_DEFAULT);
        }

        /**
         * Timeout for a session if no activity is observed.
         *
         * @return timeout in nanoseconds to wait for activity
         * @see #SESSION_TIMEOUT_PROP_NAME
         */
        public static long sessionTimeoutNs()
        {
            return getDurationInNanos(SESSION_TIMEOUT_PROP_NAME, SESSION_TIMEOUT_DEFAULT_NS);
        }

        /**
         * Timeout for a leader if no heartbeat is received by another member.
         *
         * @return timeout in nanoseconds to wait for heartbeat from a leader.
         * @see #LEADER_HEARTBEAT_TIMEOUT_PROP_NAME
         */
        public static long leaderHeartbeatTimeoutNs()
        {
            return getDurationInNanos(LEADER_HEARTBEAT_TIMEOUT_PROP_NAME, LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS);
        }

        /**
         * Interval at which a leader will send a heartbeat if the log is not progressing.
         *
         * @return timeout in nanoseconds to for leader heartbeats when no log being appended.
         * @see #LEADER_HEARTBEAT_INTERVAL_PROP_NAME
         */
        public static long leaderHeartbeatIntervalNs()
        {
            return getDurationInNanos(LEADER_HEARTBEAT_INTERVAL_PROP_NAME, LEADER_HEARTBEAT_INTERVAL_DEFAULT_NS);
        }

        /**
         * Timeout waiting to canvass the status of cluster members before voting if a majority have been heard from.
         *
         * @return timeout in nanoseconds to wait for the status of other cluster members before voting.
         * @see #STARTUP_CANVASS_TIMEOUT_PROP_NAME
         */
        public static long startupCanvassTimeoutNs()
        {
            return getDurationInNanos(STARTUP_CANVASS_TIMEOUT_PROP_NAME, STARTUP_CANVASS_TIMEOUT_DEFAULT_NS);
        }

        /**
         * Timeout waiting for votes to become leader in an election.
         *
         * @return timeout in nanoseconds to wait for votes to become leader in an election.
         * @see #ELECTION_TIMEOUT_PROP_NAME
         */
        public static long electionTimeoutNs()
        {
            return getDurationInNanos(ELECTION_TIMEOUT_PROP_NAME, ELECTION_TIMEOUT_DEFAULT_NS);
        }

        /**
         * Interval at which a member will send out status messages during the election phases.
         *
         * @return interval at which a member will send out status messages during the election phases.
         * @see #ELECTION_STATUS_INTERVAL_PROP_NAME
         */
        public static long electionStatusIntervalNs()
        {
            return getDurationInNanos(ELECTION_STATUS_INTERVAL_PROP_NAME, ELECTION_STATUS_INTERVAL_DEFAULT_NS);
        }

        /**
         * Timeout waiting for follower termination by leader.
         *
         * @return timeout in nanoseconds to wait followers to terminate.
         * @see #TERMINATION_TIMEOUT_PROP_NAME
         */
        public static long terminationTimeoutNs()
        {
            return getDurationInNanos(TERMINATION_TIMEOUT_PROP_NAME, TERMINATION_TIMEOUT_DEFAULT_NS);
        }

        /**
         * Get threshold value for the consensus module agent work cycle threshold to track for being exceeded.
         *
         * @return threshold value in nanoseconds.
         */
        public static long cycleThresholdNs()
        {
            return getDurationInNanos(CYCLE_THRESHOLD_PROP_NAME, CYCLE_THRESHOLD_DEFAULT_NS);
        }

        /**
         * Get threshold value, which is used for monitoring total snapshot duration breaches of its predefined
         * threshold.
         *
         * @return threshold value in nanoseconds.
         */
        public static long totalSnapshotDurationThresholdNs()
        {
            return getDurationInNanos(
                TOTAL_SNAPSHOT_DURATION_THRESHOLD_PROP_NAME, TOTAL_SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS);
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
         * The value {@link DefaultAuthenticatorSupplier#INSTANCE} or system property
         * {@link #AUTHENTICATOR_SUPPLIER_PROP_NAME} if set.
         *
         * @return {@link DefaultAuthenticatorSupplier#INSTANCE} or system property
         * {@link #AUTHENTICATOR_SUPPLIER_PROP_NAME} if set.
         */
        public static AuthenticatorSupplier authenticatorSupplier()
        {
            final String supplierClassName = System.getProperty(AUTHENTICATOR_SUPPLIER_PROP_NAME);
            if (Strings.isEmpty(supplierClassName))
            {
                return DefaultAuthenticatorSupplier.INSTANCE;
            }

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
            final String supplierClassName = System.getProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME);
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
                fallbackLogger().println("Warning: Cluster authorisation service set to allow all requests");
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
         * The value {@link #CONSENSUS_CHANNEL_DEFAULT} or system property
         * {@link #CONSENSUS_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #CONSENSUS_CHANNEL_DEFAULT} or system property
         * {@link #CONSENSUS_CHANNEL_PROP_NAME} if set.
         */
        public static String consensusChannel()
        {
            return System.getProperty(CONSENSUS_CHANNEL_PROP_NAME, CONSENSUS_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #CONSENSUS_STREAM_ID_DEFAULT} or system property
         * {@link #CONSENSUS_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #CONSENSUS_STREAM_ID_DEFAULT} or system property
         * {@link #CONSENSUS_STREAM_ID_PROP_NAME} if set.
         */
        public static int consensusStreamId()
        {
            return Integer.getInteger(CONSENSUS_STREAM_ID_PROP_NAME, CONSENSUS_STREAM_ID_DEFAULT);
        }

        /**
         * The system property for {@link #REPLICATION_CHANNEL_PROP_NAME} if set or null.
         *
         * @return system property {@link #REPLICATION_CHANNEL_PROP_NAME} if set or null.
         */
        public static String replicationChannel()
        {
            return System.getProperty(REPLICATION_CHANNEL_PROP_NAME);
        }

        /**
         * The value {@link #FOLLOWER_CATCHUP_CHANNEL_DEFAULT} or system property
         * {@link #FOLLOWER_CATCHUP_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #FOLLOWER_CATCHUP_CHANNEL_DEFAULT} or system property
         * {@link #FOLLOWER_CATCHUP_CHANNEL_PROP_NAME} if set.
         */
        public static String followerCatchupChannel()
        {
            return System.getProperty(FOLLOWER_CATCHUP_CHANNEL_PROP_NAME, FOLLOWER_CATCHUP_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #LEADER_ARCHIVE_CONTROL_CHANNEL_DEFAULT} or system property
         * {@link #LEADER_ARCHIVE_CONTROL_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #LEADER_ARCHIVE_CONTROL_CHANNEL_DEFAULT} or system property
         * {@link #LEADER_ARCHIVE_CONTROL_CHANNEL_PROP_NAME} if set.
         */
        public static String leaderArchiveControlChannel()
        {
            return System.getProperty(LEADER_ARCHIVE_CONTROL_CHANNEL_PROP_NAME, LEADER_ARCHIVE_CONTROL_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #WHEEL_TICK_RESOLUTION_DEFAULT_NS} or system property
         * {@link #WHEEL_TICK_RESOLUTION_PROP_NAME} if set.
         *
         * @return {@link #WHEEL_TICK_RESOLUTION_DEFAULT_NS} or system property
         * {@link #WHEEL_TICK_RESOLUTION_PROP_NAME} if set.
         */
        public static long wheelTickResolutionNs()
        {
            return getDurationInNanos(WHEEL_TICK_RESOLUTION_PROP_NAME, WHEEL_TICK_RESOLUTION_DEFAULT_NS);
        }

        /**
         * The value {@link #TICKS_PER_WHEEL_DEFAULT} or system property
         * {@link #CLUSTER_MEMBER_ID_PROP_NAME} if set.
         *
         * @return {@link #TICKS_PER_WHEEL_DEFAULT} or system property
         * {@link #TICKS_PER_WHEEL_PROP_NAME} if set.
         */
        public static int ticksPerWheel()
        {
            return Integer.getInteger(TICKS_PER_WHEEL_PROP_NAME, TICKS_PER_WHEEL_DEFAULT);
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
         */
        public static int fileSyncLevel()
        {
            return Integer.getInteger(FILE_SYNC_LEVEL_PROP_NAME, FILE_SYNC_LEVEL_DEFAULT);
        }

        /**
         * The name of the {@link TimerServiceSupplier} to use for supplying the {@link TimerService}.
         *
         * @return {@link #TIMER_SERVICE_SUPPLIER_DEFAULT} or system property.
         * {@link #TIMER_SERVICE_SUPPLIER_PROP_NAME} if set.
         */
        public static String timerServiceSupplier()
        {
            return System.getProperty(TIMER_SERVICE_SUPPLIER_PROP_NAME, TIMER_SERVICE_SUPPLIER_DEFAULT);
        }

        /**
         * The name to be used for the {@link Agent#roleName()} for the consensus module agent.
         *
         * @return name to be used for the {@link Agent#roleName()} for the consensus module agent.
         * @see #CLUSTER_CONSENSUS_MODULE_AGENT_ROLE_NAME_PROP_NAME
         */
        public static String agentRoleName()
        {
            return System.getProperty(CLUSTER_CONSENSUS_MODULE_AGENT_ROLE_NAME_PROP_NAME);
        }

        /**
         * The amount of time to wait to time out an archive replication when progress has stalled.
         *
         * @return system property {@link #CLUSTER_REPLICATION_PROGRESS_TIMEOUT_PROP_NAME} or
         * {@link #CLUSTER_REPLICATION_PROGRESS_TIMEOUT_DEFAULT_NS}.
         * @since 1.41.0
         */
        public static long replicationProgressTimeoutNs()
        {
            return SystemUtil.getDurationInNanos(
                CLUSTER_REPLICATION_PROGRESS_TIMEOUT_PROP_NAME, CLUSTER_REPLICATION_PROGRESS_TIMEOUT_DEFAULT_NS);
        }


        /**
         * Interval between checks for progress on an archive replication.
         *
         * @return system property {@link #CLUSTER_REPLICATION_PROGRESS_INTERVAL_PROP_NAME} or {@link Aeron#NULL_VALUE}
         * if not set.
         * @since 1.41.0
         */
        public static long replicationProgressIntervalNs()
        {
            return SystemUtil.getDurationInNanos(CLUSTER_REPLICATION_PROGRESS_INTERVAL_PROP_NAME, Aeron.NULL_VALUE);
        }

        /**
         * If this node should accept snapshots from standby nodes.
         *
         * @return value from property {@link #CLUSTER_ACCEPT_STANDBY_SNAPSHOTS_PROP_NAME} or false if not set.
         */
        public static boolean acceptStandbySnapshots()
        {
            return Boolean.getBoolean(CLUSTER_ACCEPT_STANDBY_SNAPSHOTS_PROP_NAME);
        }

        /**
         * Create a new {@link ConsensusModuleExtension} based on the configured
         * {@link #CONSENSUS_MODULE_EXTENSION_CLASS_NAME_PROP_NAME}.
         *
         * @return a new {@link ConsensusModuleExtension} based on the configured
         * {@link #CONSENSUS_MODULE_EXTENSION_CLASS_NAME_PROP_NAME}.
         */
        public static ConsensusModuleExtension newConsensusModuleExtension()
        {
            final String className = System.getProperty(Configuration.CONSENSUS_MODULE_EXTENSION_CLASS_NAME_PROP_NAME);
            if (null != className)
            {
                try
                {
                    return (ConsensusModuleExtension)Class.forName(className).getConstructor().newInstance();
                }
                catch (final Exception ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }
            }

            return null;
        }

        /**
         * Determine the ConsensusModule should bind the control endpoint of the consensus publication.
         *
         * @return <code>true</code> if the ConsensusModule should bind the control endpoint of the consensus
         * publication.
         */
        public static boolean enableControlOnConsensusChannel()
        {
            return parseBoolean(System.getProperty(
                CONSENSUS_MODULE_ENABLE_CONTROL_ON_CONSENSUS_CHANNEL_PROP_NAME, "true"));
        }

        /**
         * Determine the ConsensusModule should bind the control endpoint of the log publication.
         *
         * @return <code>true</code> if the ConsensusModule should bind the log endpoint of the consensus
         * publication.
         */
        public static boolean enableControlOnLogChannel()
        {
            return parseBoolean(System.getProperty(CONSENSUS_MODULE_ENABLE_CONTROL_ON_LOG_CHANNEL_PROP_NAME, "true"));
        }
    }

    /**
     * Programmable overrides for configuring the {@link ConsensusModule} in a cluster.
     * <p>
     * The context will be owned by {@link ConsensusModuleAgent} after a successful
     * {@link ConsensusModule#launch(Context)} and closed via {@link ConsensusModule#close()}.
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
        private boolean ownsAeronClient = false;
        private String aeronDirectoryName = CommonContext.getAeronDirectoryName();
        private Aeron aeron;

        private boolean deleteDirOnStart = false;
        private String clusterDirectoryName = ClusteredServiceContainer.Configuration.clusterDirName();
        private String clusterServicesDirectoryName = ClusteredServiceContainer.Configuration.clusterServicesDirName();
        private File clusterDir;
        private File markFileDir;
        private RecordingLog recordingLog;
        private ClusterMarkFile markFile;
        private NodeStateFile nodeStateFile;
        private int fileSyncLevel = Archive.Configuration.fileSyncLevel();

        private int appVersion = SemanticVersion.compose(0, 0, 1);
        private int clusterId = ClusteredServiceContainer.Configuration.clusterId();
        private int clusterMemberId = Configuration.clusterMemberId();
        private int appointedLeaderId = Configuration.appointedLeaderId();
        private String clusterMembers = Configuration.clusterMembers();
        private String ingressChannel = AeronCluster.Configuration.ingressChannel();
        private int ingressStreamId = AeronCluster.Configuration.ingressStreamId();
        private boolean isIpcIngressAllowed = Configuration.isIpcIngressAllowed();
        private int ingressFragmentLimit = Configuration.ingressFragmentLimit();
        private String egressChannel = AeronCluster.Configuration.egressChannel();
        private String logChannel = Configuration.logChannel();
        private int logStreamId = Configuration.logStreamId();
        private String memberEndpoints = Configuration.memberEndpoints();
        private String replayChannel = ClusteredServiceContainer.Configuration.replayChannel();
        private int replayStreamId = ClusteredServiceContainer.Configuration.replayStreamId();
        private String controlChannel = ClusteredServiceContainer.Configuration.controlChannel();
        private int consensusModuleStreamId = ClusteredServiceContainer.Configuration.consensusModuleStreamId();
        private int serviceStreamId = ClusteredServiceContainer.Configuration.serviceStreamId();
        private String snapshotChannel = Configuration.snapshotChannel();
        private int snapshotStreamId = Configuration.snapshotStreamId();
        private String consensusChannel = Configuration.consensusChannel();
        private int consensusStreamId = Configuration.consensusStreamId();
        private String replicationChannel = Configuration.replicationChannel();
        private String followerCatchupChannel = Configuration.followerCatchupChannel();
        private String leaderArchiveControlChannel = Configuration.leaderArchiveControlChannel();
        private int logFragmentLimit = ClusteredServiceContainer.Configuration.logFragmentLimit();

        private int serviceCount = Configuration.serviceCount();
        private int errorBufferLength = Configuration.errorBufferLength();
        private int maxConcurrentSessions = Configuration.maxConcurrentSessions();
        private int ticksPerWheel = Configuration.ticksPerWheel();
        private long wheelTickResolutionNs = Configuration.wheelTickResolutionNs();
        private long sessionTimeoutNs = Configuration.sessionTimeoutNs();
        private long leaderHeartbeatTimeoutNs = Configuration.leaderHeartbeatTimeoutNs();
        private long leaderHeartbeatIntervalNs = Configuration.leaderHeartbeatIntervalNs();
        private long startupCanvassTimeoutNs = Configuration.startupCanvassTimeoutNs();
        private long electionTimeoutNs = Configuration.electionTimeoutNs();
        private long electionStatusIntervalNs = Configuration.electionStatusIntervalNs();
        private long terminationTimeoutNs = Configuration.terminationTimeoutNs();
        private long cycleThresholdNs = Configuration.cycleThresholdNs();
        private long totalSnapshotDurationThresholdNs = Configuration.totalSnapshotDurationThresholdNs();

        private String agentRoleName = Configuration.agentRoleName();
        private ThreadFactory threadFactory;
        private Supplier<IdleStrategy> idleStrategySupplier;
        private ClusterClock clusterClock;
        private EpochClock epochClock;
        private Random random;
        private TimerServiceSupplier timerServiceSupplier;
        private Function<Context, LongConsumer> clusterTimeConsumerSupplier;
        private ConsensusModuleExtension consensusModuleExtension;
        private DistinctErrorLog errorLog;
        private ErrorHandler errorHandler;
        private AtomicCounter errorCounter;
        private CountedErrorHandler countedErrorHandler;

        private Counter moduleStateCounter;
        private Counter electionStateCounter;
        private Counter clusterNodeRoleCounter;
        private Counter commitPosition;
        private Counter clusterControlToggle;
        private Counter nodeControlToggle;
        private Counter snapshotCounter;
        private Counter timedOutClientCounter;
        private Counter standbySnapshotCounter;
        private Counter electionCounter;
        private Counter leadershipTermId;
        private Runnable terminationHook;

        private AeronArchive.Context archiveContext;
        private AuthenticatorSupplier authenticatorSupplier;
        private AuthorisationServiceSupplier authorisationServiceSupplier;
        private LogPublisher logPublisher;
        private EgressPublisher egressPublisher;
        private DutyCycleTracker dutyCycleTracker;
        private SnapshotDurationTracker totalSnapshotDurationTracker;
        private AppVersionValidator appVersionValidator;
        private boolean isLogMdc;
        private boolean useAgentInvoker = false;
        private ConsensusModuleStateExport bootstrapState = null;
        private boolean acceptStandbySnapshots = Configuration.acceptStandbySnapshots();
        private boolean enableControlOnConsensusChannel = Configuration.enableControlOnConsensusChannel();
        private boolean enableControlOnLogChannel = Configuration.enableControlOnLogChannel();

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

            validateLogChannel();

            if (null == clusterDir)
            {
                clusterDir = new File(clusterDirectoryName);
            }

            if (null == markFileDir)
            {
                final String dir = ClusteredServiceContainer.Configuration.markFileDir();
                markFileDir = Strings.isEmpty(dir) ? clusterDir : new File(dir);
            }

            try
            {
                clusterDir = clusterDir.getCanonicalFile();
                clusterDirectoryName = clusterDir.getAbsolutePath();
                markFileDir = markFileDir.getCanonicalFile();

                if (Strings.isEmpty(clusterServicesDirectoryName))
                {
                    clusterServicesDirectoryName = clusterDirectoryName;
                }
                else
                {
                    clusterServicesDirectoryName = new File(clusterServicesDirectoryName).getCanonicalPath();
                }
            }
            catch (final IOException ex)
            {
                throw new UncheckedIOException(ex);
            }

            if (null == clusterMembers)
            {
                throw new ClusterException("ConsensusModule.Context.clusterMembers must be set");
            }

            if (deleteDirOnStart)
            {
                IoUtil.delete(clusterDir, false);
            }

            IoUtil.ensureDirectoryExists(clusterDir, "cluster");
            IoUtil.ensureDirectoryExists(markFileDir, "mark file");

            if (startupCanvassTimeoutNs / leaderHeartbeatTimeoutNs < 2)
            {
                throw new ClusterException(
                    "startupCanvassTimeoutNs=" + startupCanvassTimeoutNs +
                    " must be a multiple of leaderHeartbeatTimeoutNs=" + leaderHeartbeatTimeoutNs);
            }

            if (null == clusterClock)
            {
                final String clockClassName = System.getProperty(
                    CLUSTER_CLOCK_PROP_NAME, MillisecondClusterClock.class.getName());
                try
                {
                    clusterClock = (ClusterClock)Class.forName(clockClassName).getConstructor().newInstance();
                }
                catch (final Exception e)
                {
                    throw new ClusterException("failed to instantiate ClusterClock " + clockClassName, e);
                }
            }

            if (null == epochClock)
            {
                epochClock = SystemEpochClock.INSTANCE;
            }

            if (null == appVersionValidator)
            {
                appVersionValidator = AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR;
            }

            if (null == clusterTimeConsumerSupplier)
            {
                clusterTimeConsumerSupplier = (ctx) -> (timestamp) -> {};
            }

            if (null == markFile)
            {
                final int filePageSize = null != aeron ? aeron.context().filePageSize() :
                    driverFilePageSize(new File(aeronDirectoryName), epochClock, new CommonContext().driverTimeoutMs());
                markFile = new ClusterMarkFile(
                    new File(markFileDir, ClusterMarkFile.FILENAME),
                    ClusterComponentType.CONSENSUS_MODULE,
                    errorBufferLength,
                    epochClock,
                    ClusteredServiceContainer.Configuration.LIVENESS_TIMEOUT_MS,
                    filePageSize);
            }

            MarkFile.ensureMarkFileLink(
                clusterDir,
                new File(markFile.parentDirectory(), ClusterMarkFile.FILENAME),
                ClusterMarkFile.LINK_FILENAME);

            if (null == nodeStateFile)
            {
                try
                {
                    nodeStateFile = new NodeStateFile(clusterDir, true, fileSyncLevel());
                }
                catch (final IOException ex)
                {
                    throw new ClusterException("unable to create node-state file", ex);
                }
            }

            if (Aeron.NULL_VALUE == nodeStateFile.candidateTerm().candidateTermId() &&
                Aeron.NULL_VALUE != markFile.candidateTermId())
            {
                nodeStateFile.updateCandidateTermId(markFile.candidateTermId(), Aeron.NULL_VALUE, epochClock.time());
            }

            if (null == errorLog)
            {
                errorLog = new DistinctErrorLog(markFile.errorBuffer(), epochClock, StandardCharsets.US_ASCII);
            }

            errorHandler = CommonContext.setupErrorHandler(errorHandler, errorLog);

            if (null == recordingLog)
            {
                recordingLog = new RecordingLog(clusterDir, true);
            }

            if (Strings.isEmpty(agentRoleName))
            {
                agentRoleName = "consensus-module-" + clusterId + "-" + clusterMemberId;
            }

            final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

            if (null == aeron)
            {
                ownsAeronClient = true;

                aeron = Aeron.connect(
                    new Aeron.Context()
                        .aeronDirectoryName(aeronDirectoryName)
                        .errorHandler(errorHandler)
                        .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE)
                        .epochClock(epochClock)
                        .useConductorAgentInvoker(true)
                        .awaitingIdleStrategy(YieldingIdleStrategy.INSTANCE)
                        .clientLock(NoOpLock.INSTANCE)
                        .clientName(agentRoleName));

                if (null == errorCounter)
                {
                    errorCounter = ClusterCounters.allocateVersioned(
                        aeron,
                        buffer,
                        "Cluster Errors",
                        CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID,
                        clusterId,
                        ConsensusModuleVersion.VERSION,
                        ConsensusModuleVersion.GIT_SHA);
                }
            }

            if (null == ingressChannel)
            {
                throw new ClusterException("ingressChannel must be specified");
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

            if (null == moduleStateCounter)
            {
                final CountersReader counters = aeron.countersReader();
                if (Aeron.NULL_VALUE != ClusterCounters.find(counters, CONSENSUS_MODULE_STATE_TYPE_ID, clusterId))
                {
                    throw new ClusterException("existing consensus module detected for clusterId=" + clusterId);
                }

                moduleStateCounter = ClusterCounters.allocate(
                    aeron, buffer, "Consensus Module state", CONSENSUS_MODULE_STATE_TYPE_ID, clusterId);
            }
            validateCounterTypeId(aeron, moduleStateCounter, CONSENSUS_MODULE_STATE_TYPE_ID);


            if (null == electionStateCounter)
            {
                electionStateCounter = ClusterCounters.allocate(
                    aeron, buffer, "Cluster election state", ELECTION_STATE_TYPE_ID, clusterId);
            }
            validateCounterTypeId(aeron, electionStateCounter, ELECTION_STATE_TYPE_ID);

            if (null == electionCounter)
            {
                electionCounter = ClusterCounters.allocate(
                    aeron, buffer, "Cluster election count", CLUSTER_ELECTION_COUNT_TYPE_ID, clusterId);
            }
            validateCounterTypeId(aeron, electionCounter, CLUSTER_ELECTION_COUNT_TYPE_ID);

            if (null == leadershipTermId)
            {
                leadershipTermId = ClusterCounters.allocate(
                    aeron, buffer, "Cluster leadership term id", CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID, clusterId);
            }
            validateCounterTypeId(aeron, leadershipTermId, CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

            if (null == clusterNodeRoleCounter)
            {
                clusterNodeRoleCounter = ClusterCounters.allocate(
                    aeron, buffer, "Cluster node role", CLUSTER_NODE_ROLE_TYPE_ID, clusterId);
            }
            validateCounterTypeId(aeron, clusterNodeRoleCounter, CLUSTER_NODE_ROLE_TYPE_ID);

            if (null == commitPosition)
            {
                commitPosition = ClusterCounters.allocate(
                    aeron, buffer, "Cluster commit-pos:", COMMIT_POSITION_TYPE_ID, clusterId);
            }
            validateCounterTypeId(aeron, commitPosition, COMMIT_POSITION_TYPE_ID);

            if (null == clusterControlToggle)
            {
                clusterControlToggle = ClusterCounters.allocate(
                    aeron, buffer, "Cluster control toggle", CONTROL_TOGGLE_TYPE_ID, clusterId);
            }
            validateCounterTypeId(aeron, clusterControlToggle, CONTROL_TOGGLE_TYPE_ID);

            if (null == nodeControlToggle)
            {
                nodeControlToggle = ClusterCounters.allocate(
                    aeron, buffer, "Node control toggle", NODE_CONTROL_TOGGLE_TYPE_ID, clusterId);
            }
            validateCounterTypeId(aeron, nodeControlToggle, NODE_CONTROL_TOGGLE_TYPE_ID);

            if (null == snapshotCounter)
            {
                snapshotCounter = ClusterCounters.allocate(
                    aeron, buffer, "Cluster snapshot count", SNAPSHOT_COUNTER_TYPE_ID, clusterId);
            }
            validateCounterTypeId(aeron, snapshotCounter, SNAPSHOT_COUNTER_TYPE_ID);

            if (null == timedOutClientCounter)
            {
                timedOutClientCounter = ClusterCounters.allocate(
                    aeron, buffer, "Cluster timed out client count", CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID, clusterId);
            }
            validateCounterTypeId(aeron, timedOutClientCounter, CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID);

            // TODO: Disable with configuration... (Mike)
            if (acceptStandbySnapshots)
            {
                if (null == standbySnapshotCounter)
                {
                    standbySnapshotCounter = ClusterCounters.allocate(
                        aeron,
                        buffer,
                        "Cluster standby snapshots received",
                        CLUSTER_STANDBY_SNAPSHOT_COUNTER_TYPE_ID,
                        clusterId);
                }
                validateCounterTypeId(aeron, standbySnapshotCounter, CLUSTER_STANDBY_SNAPSHOT_COUNTER_TYPE_ID);
            }

            if (null == dutyCycleTracker)
            {
                dutyCycleTracker = new DutyCycleStallTracker(
                    ClusterCounters.allocate(
                        aeron, buffer, "Cluster max cycle time in ns",
                        AeronCounters.CLUSTER_MAX_CYCLE_TIME_TYPE_ID, clusterId),
                    ClusterCounters.allocate(
                        aeron, buffer, "Cluster work cycle time exceeded count: threshold=" + cycleThresholdNs + "ns",
                        AeronCounters.CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID, clusterId),
                    cycleThresholdNs);
            }

            if (null == totalSnapshotDurationTracker)
            {
                totalSnapshotDurationTracker = new SnapshotDurationTracker(
                    ClusterCounters.allocate(
                        aeron,
                        buffer,
                        "Total max snapshot duration in ns",
                        AeronCounters.CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID,
                        clusterId),
                    ClusterCounters.allocate(
                        aeron,
                        buffer,
                        "Total max snapshot duration exceeded count: threshold=" +
                            totalSnapshotDurationThresholdNs + "ns",
                        AeronCounters.CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID,
                        clusterId),
                    totalSnapshotDurationThresholdNs);
            }

            if (null == threadFactory)
            {
                threadFactory = Thread::new;
            }

            if (null == idleStrategySupplier)
            {
                idleStrategySupplier = ClusteredServiceContainer.Configuration.idleStrategySupplier(null);
            }

            if (null == timerServiceSupplier)
            {
                timerServiceSupplier = getTimerServiceSupplierFromSystemProperty();
            }

            if (null == archiveContext)
            {
                archiveContext = new AeronArchive.Context()
                    .controlRequestChannel(AeronArchive.Configuration.localControlChannel())
                    .controlResponseChannel(AeronArchive.Configuration.localControlChannel())
                    .controlRequestStreamId(AeronArchive.Configuration.localControlStreamId())
                    .controlResponseStreamId(
                        clusterId * 100 + 100 + AeronArchive.Configuration.controlResponseStreamId());
            }

            if (!archiveContext.controlRequestChannel().startsWith(CommonContext.IPC_CHANNEL))
            {
                throw new ClusterException("local archive control must be IPC");
            }

            if (!archiveContext.controlResponseChannel().startsWith(CommonContext.IPC_CHANNEL))
            {
                throw new ClusterException("local archive control must be IPC");
            }

            if (null == replicationChannel)
            {
                throw new ClusterException("replicationChannel must be set");
            }

            archiveContext
                .aeron(aeron)
                .errorHandler(countedErrorHandler)
                .ownsAeronClient(false)
                .lock(NoOpLock.INSTANCE)
                .controlRequestChannel(addAliasIfAbsent(
                archiveContext.controlRequestChannel(),
                "cm-archive-ctrl-req-cluster-" + clusterId + "-member-" + clusterMemberId))
                .controlResponseChannel(addAliasIfAbsent(
                archiveContext.controlResponseChannel(),
                "cm-archive-ctrl-resp-cluster-" + clusterId + "-member-" + clusterMemberId))
                .clientName(agentRoleName);

            if (null == terminationHook)
            {
                terminationHook = () -> {};
            }

            if (null == authenticatorSupplier)
            {
                authenticatorSupplier = Configuration.authenticatorSupplier();
            }

            if (null == authorisationServiceSupplier)
            {
                authorisationServiceSupplier = Configuration.authorisationServiceSupplier();
            }

            if (null == random)
            {
                random = new Random();
            }

            if (null == logPublisher)
            {
                logPublisher = new LogPublisher(logChannel());
            }

            if (null == egressPublisher)
            {
                egressPublisher = new EgressPublisher(leaderHeartbeatTimeoutNs);
            }

            final ChannelUri channelUri = parse(logChannel());
            isLogMdc = channelUri.isUdp() && null == channelUri.get(ENDPOINT_PARAM_NAME);

            if (null == consensusModuleExtension)
            {
                consensusModuleExtension = Configuration.newConsensusModuleExtension();
            }

            if (null != consensusModuleExtension && 0 != serviceCount)
            {
                throw new ClusterException("Service count must be zero when a ConsensusModuleExtension installed");
            }

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
         * Set the directory name to use for the consensus module directory.
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
         * The directory name to use for the consensus module directory.
         *
         * @return directory name for the consensus module directory.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CLUSTER_DIR_PROP_NAME
         */
        public String clusterDirectoryName()
        {
            return clusterDirectoryName;
        }

        /**
         * Set the directory used by clustered service containers. By default, the cluster services will share a
         * directory with the consensus module. However, sometimes it will be useful for these values to be different.
         * Setting this value allows that location to be tracked in the ClusterMarkFile to allow for the ClusterTool to
         * resolve their location when using action like <code>describe</code> or <code>errors</code>. There is an
         * expectation that all clustered service containers will use the same directory.
         *
         * @param clusterServicesDirectoryName to use.
         * @return this for a fluent API.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CLUSTER_SERVICES_DIR_PROP_NAME
         */
        public Context clusterServicesDirectoryName(final String clusterServicesDirectoryName)
        {
            this.clusterServicesDirectoryName = clusterServicesDirectoryName;
            return this;
        }

        /**
         * The directory used by the clustered service containers.
         *
         * @return directory used by the clustered service containers.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CLUSTER_SERVICES_DIR_PROP_NAME
         * @see #clusterServicesDirectoryName(String)
         */
        @Config(id = "CLUSTER_SERVICES_DIR")
        public String clusterServicesDirectoryName()
        {
            return clusterServicesDirectoryName;
        }

        /**
         * Set the directory to use for the consensus module directory.
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
         * The directory used for the consensus module directory.
         *
         * @return directory for the consensus module directory.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CLUSTER_DIR_PROP_NAME
         */
        public File clusterDir()
        {
            return clusterDir;
        }

        /**
         * Get the directory in which the ConsensusModule will store mark file (i.e. {@code cluster-mark.dat}). It
         * defaults to {@link #clusterDir()} if it is not set explicitly via the
         * {@link io.aeron.cluster.service.ClusteredServiceContainer.Configuration#MARK_FILE_DIR_PROP_NAME}.
         *
         * @return the directory in which the ConsensusModule will store mark file (i.e. {@code cluster-mark.dat}).
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#MARK_FILE_DIR_PROP_NAME
         * @see #clusterDir()
         */
        public File markFileDir()
        {
            return markFileDir;
        }

        /**
         * Set the directory in which the ConsensusModule will store mark file (i.e. {@code cluster-mark.dat}).
         *
         * @param markFileDir the directory in which the Archive will store mark file (i.e. {@code cluster-mark.dat}).
         * @return this for a fluent API.
         */
        public ConsensusModule.Context markFileDir(final File markFileDir)
        {
            this.markFileDir = markFileDir;
            return this;
        }

        /**
         * Set the {@link RecordingLog} for the log terms and snapshots.
         *
         * @param recordingLog to use.
         * @return this for a fluent API.
         */
        public Context recordingLog(final RecordingLog recordingLog)
        {
            this.recordingLog = recordingLog;
            return this;
        }

        /**
         * The {@link RecordingLog} for the log terms and snapshots.
         *
         * @return {@link RecordingLog} for the  log terms and snapshots.
         */
        public RecordingLog recordingLog()
        {
            return recordingLog;
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
         * Get level at which files should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         *
         * @return the level to be applied for file write.
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
         * @see Configuration#FILE_SYNC_LEVEL_PROP_NAME
         */
        public Context fileSyncLevel(final int syncLevel)
        {
            this.fileSyncLevel = syncLevel;
            return this;
        }

        /**
         * Set the id for this cluster instance. This must match with the service containers.
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
         * Get the id for this cluster instance. This must match with the service containers.
         *
         * @return the id for this cluster instance.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CLUSTER_ID_PROP_NAME
         */
        public int clusterId()
        {
            return clusterId;
        }

        /**
         * This cluster member identity.
         *
         * @param clusterMemberId for this member.
         * @return this for a fluent API.
         * @see Configuration#CLUSTER_MEMBER_ID_PROP_NAME
         */
        public Context clusterMemberId(final int clusterMemberId)
        {
            this.clusterMemberId = clusterMemberId;
            return this;
        }

        /**
         * This cluster member identity.
         *
         * @return this cluster member identity.
         * @see Configuration#CLUSTER_MEMBER_ID_PROP_NAME
         */
        @Config
        public int clusterMemberId()
        {
            return clusterMemberId;
        }

        /**
         * The cluster member id of the appointed cluster leader.
         * <p>
         * -1 means no leader has been appointed and an automated leader election should occur.
         * <p>
         * This feature is for testing and not recommended for production usage.
         *
         * @param appointedLeaderId for the cluster.
         * @return this for a fluent API.
         * @see Configuration#APPOINTED_LEADER_ID_PROP_NAME
         */
        public Context appointedLeaderId(final int appointedLeaderId)
        {
            this.appointedLeaderId = appointedLeaderId;
            return this;
        }

        /**
         * The cluster member id of the appointed cluster leader.
         * <p>
         * -1 means no leader has been appointed and an automated leader election should occur.
         *
         * @return cluster member id of the appointed cluster leader.
         * @see Configuration#APPOINTED_LEADER_ID_PROP_NAME
         */
        @Config
        public int appointedLeaderId()
        {
            return appointedLeaderId;
        }

        /**
         * String representing the cluster members.
         * <p>
         * <code>
         * 0,ingress:port,consensus:port,log:port,catchup:port,archive:port| \
         * 1,ingress:port,consensus:port,log:port,catchup:port,archive:port| ...
         * </code>
         * <p>
         * The ingress endpoints will be used as the endpoint substituted into the {@link #ingressChannel()}
         * if the endpoint is not provided unless it is multicast.
         *
         * @param clusterMembers which are all candidates to be leader.
         * @return this for a fluent API.
         * @see Configuration#CLUSTER_MEMBERS_PROP_NAME
         */
        public Context clusterMembers(final String clusterMembers)
        {
            this.clusterMembers = clusterMembers;
            return this;
        }

        /**
         * The endpoints representing members of the cluster which are all candidates to be leader.
         * <p>
         * The ingress endpoints will be used as the endpoint in {@link #ingressChannel()} if the endpoint is
         * not provided in that when it is not multicast.
         *
         * @return members of the cluster which are all candidates to be leader.
         * @see Configuration#CLUSTER_MEMBERS_PROP_NAME
         */
        @Config
        public String clusterMembers()
        {
            return clusterMembers;
        }

        /**
         * Set the channel parameter for the ingress channel.
         *
         * @param channel parameter for the ingress channel.
         * @return this for a fluent API.
         * @see io.aeron.cluster.client.AeronCluster.Configuration#INGRESS_CHANNEL_PROP_NAME
         */
        public Context ingressChannel(final String channel)
        {
            ingressChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for the ingress channel.
         *
         * @return the channel parameter for the ingress channel.
         * @see io.aeron.cluster.client.AeronCluster.Configuration#INGRESS_CHANNEL_PROP_NAME
         */
        public String ingressChannel()
        {
            return ingressChannel;
        }

        /**
         * Set the stream id for the ingress channel.
         *
         * @param streamId for the ingress channel.
         * @return this for a fluent API
         * @see io.aeron.cluster.client.AeronCluster.Configuration#INGRESS_STREAM_ID_PROP_NAME
         */
        public Context ingressStreamId(final int streamId)
        {
            ingressStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for the ingress channel.
         *
         * @return the stream id for the ingress channel.
         * @see io.aeron.cluster.client.AeronCluster.Configuration#INGRESS_STREAM_ID_PROP_NAME
         */
        public int ingressStreamId()
        {
            return ingressStreamId;
        }

        /**
         * Set limit for fragments to be consumed on each poll of ingress.
         *
         * @param ingressFragmentLimit for the ingress channel.
         * @return this for a fluent API
         * @see Configuration#CLUSTER_INGRESS_FRAGMENT_LIMIT_PROP_NAME
         */
        public Context ingressFragmentLimit(final int ingressFragmentLimit)
        {
            this.ingressFragmentLimit = ingressFragmentLimit;
            return this;
        }

        /**
         * The limit for fragments to be consumed on each poll of ingress.
         *
         * @return the limit for fragments to be consumed on each poll of ingress.
         * @see Configuration#CLUSTER_INGRESS_FRAGMENT_LIMIT_PROP_NAME
         */
        @Config(id = "CLUSTER_INGRESS_FRAGMENT_LIMIT")
        public int ingressFragmentLimit()
        {
            return ingressFragmentLimit;
        }

        /**
         * Set the template channel that is used to refine the response channels for a Cluster, i.e. egress channel,
         * backup query response channel, heartbeat response channel etc. Defaults to {@code null} in which case the
         * provided response channel will be used. The main use-case is the ability to define the `interface` parameter
         * to steer the response traffic via a specific network interface.
         *
         * <p><em><strong>Note:</strong> each URI parameter that is defined on this channel template will
         * <strong>override</strong> the corresponding URI parameter in the provided response channel!</em>
         *
         * @param channel the channel template for response channels.
         * @return this for a fluent API.
         * @see io.aeron.cluster.client.AeronCluster.Configuration#EGRESS_CHANNEL_PROP_NAME
         */
        public Context egressChannel(final String channel)
        {
            egressChannel = channel;
            return this;
        }

        /**
         * Get the template channel that is used to refine the response channels for a Cluster, i.e. egress channel,
         * backup query response channel, heartbeat response channel etc. Defaults to {@code null} in which case the
         * provided response channel will be used. The main use-case is the ability to define the `interface` parameter
         * to steer the response traffic via a specific network interface.
         *
         * <p><em><strong>Note:</strong> each URI parameter that is defined on this channel template will
         * <strong>override</strong> the corresponding URI parameter in the provided response channel!</em>
         *
         * @return the channel template for response channels.
         * @see io.aeron.cluster.client.AeronCluster.Configuration#EGRESS_CHANNEL_PROP_NAME
         */
        public String egressChannel()
        {
            return egressChannel;
        }

        /**
         * Set whether IPC ingress is allowed or not.
         *
         * @param isIpcIngressAllowed or not.
         * @return this for a fluent API
         * @see Configuration#CLUSTER_INGRESS_IPC_ALLOWED_PROP_NAME
         */
        public Context isIpcIngressAllowed(final boolean isIpcIngressAllowed)
        {
            this.isIpcIngressAllowed = isIpcIngressAllowed;
            return this;
        }

        /**
         * Get whether IPC ingress is allowed or not.
         *
         * @return whether IPC ingress is allowed or not.
         * @see Configuration#CLUSTER_INGRESS_IPC_ALLOWED_PROP_NAME
         */
        @Config(id = "CLUSTER_INGRESS_IPC_ALLOWED")
        public boolean isIpcIngressAllowed()
        {
            return isIpcIngressAllowed;
        }

        /**
         * Set the channel parameter for the cluster log channel.
         *
         * @param channel parameter for the cluster log channel.
         * @return this for a fluent API.
         * @see Configuration#LOG_CHANNEL_PROP_NAME
         */
        public Context logChannel(final String channel)
        {
            logChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for the cluster log channel.
         *
         * @return the channel parameter for the cluster channel.
         * @see Configuration#LOG_CHANNEL_PROP_NAME
         */
        @Config
        public String logChannel()
        {
            return logChannel;
        }

        /**
         * Set the stream id for the cluster log channel.
         *
         * @param streamId for the cluster log channel.
         * @return this for a fluent API
         * @see Configuration#LOG_STREAM_ID_PROP_NAME
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
         * @see Configuration#LOG_STREAM_ID_PROP_NAME
         */
        public int logStreamId()
        {
            return logStreamId;
        }

        /**
         * Set the endpoints for this cluster node.
         *
         * @param endpoints for the cluster node.
         * @return this for a fluent API.
         * @see Configuration#MEMBER_ENDPOINTS_PROP_NAME
         */
        public Context memberEndpoints(final String endpoints)
        {
            memberEndpoints = endpoints;
            return this;
        }

        /**
         * Get the endpoints for this cluster node.
         *
         * @return the endpoints for the cluster node.
         * @see Configuration#MEMBER_ENDPOINTS_PROP_NAME
         */
        @Config
        public String memberEndpoints()
        {
            return memberEndpoints;
        }

        /**
         * Set the channel parameter for the cluster log and snapshot replay channel.
         *
         * @param channel parameter for the cluster log replay channel.
         * @return this for a fluent API.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#REPLAY_CHANNEL_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#REPLAY_CHANNEL_PROP_NAME
         */
        public String replayChannel()
        {
            return replayChannel;
        }

        /**
         * Set the stream id for the cluster log and snapshot replay channel.
         *
         * @param streamId for the cluster log replay channel.
         * @return this for a fluent API
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#REPLAY_STREAM_ID_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#REPLAY_STREAM_ID_PROP_NAME
         */
        public int replayStreamId()
        {
            return replayStreamId;
        }

        /**
         * Set the channel parameter for bidirectional communications between the consensus module and services.
         *
         * @param channel parameter for bidirectional communications between the consensus module and services.
         * @return this for a fluent API.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CONTROL_CHANNEL_PROP_NAME
         */
        public Context controlChannel(final String channel)
        {
            controlChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for bidirectional communications between the consensus module and services.
         *
         * @return the channel parameter for bidirectional communications between the consensus module and services.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CONTROL_CHANNEL_PROP_NAME
         */
        public String controlChannel()
        {
            return controlChannel;
        }

        /**
         * Set the stream id for communications from the consensus module and to the services.
         *
         * @param streamId for communications from the consensus module and to the services.
         * @return this for a fluent API
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SERVICE_STREAM_ID_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SERVICE_STREAM_ID_PROP_NAME
         */
        public int serviceStreamId()
        {
            return serviceStreamId;
        }

        /**
         * Set the stream id for communications from the services to the consensus module.
         *
         * @param streamId for communications from the services to the consensus module.
         * @return this for a fluent API
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CONSENSUS_MODULE_STREAM_ID_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CONSENSUS_MODULE_STREAM_ID_PROP_NAME
         */
        public int consensusModuleStreamId()
        {
            return consensusModuleStreamId;
        }

        /**
         * Set the channel parameter for snapshot recordings.
         *
         * @param channel parameter for snapshot recordings
         * @return this for a fluent API.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SNAPSHOT_CHANNEL_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SNAPSHOT_CHANNEL_PROP_NAME
         */
        public String snapshotChannel()
        {
            return snapshotChannel;
        }

        /**
         * Set the stream id for snapshot recordings.
         *
         * @param streamId for snapshot recordings.
         * @return this for a fluent API
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SNAPSHOT_STREAM_ID_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SNAPSHOT_STREAM_ID_PROP_NAME
         */
        public int snapshotStreamId()
        {
            return snapshotStreamId;
        }

        /**
         * Set the channel parameter for the consensus communication channel.
         *
         * @param channel parameter for the consensus communication channel.
         * @return this for a fluent API.
         * @see Configuration#CONSENSUS_CHANNEL_PROP_NAME
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
         * @see Configuration#CONSENSUS_CHANNEL_PROP_NAME
         */
        @Config
        public String consensusChannel()
        {
            return consensusChannel;
        }

        /**
         * Set the stream id for the consensus channel.
         *
         * @param streamId for the consensus channel.
         * @return this for a fluent API
         * @see Configuration#CONSENSUS_STREAM_ID_PROP_NAME
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
         * @see Configuration#CONSENSUS_STREAM_ID_PROP_NAME
         */
        @Config
        public int consensusStreamId()
        {
            return consensusStreamId;
        }

        /**
         * Set the channel parameter for the replication communication channel. This is channel that the local
         * will send to src archives to replay their recordings back to. It should contain an endpoint that other
         * members in the cluster will be able to reach. Using port 0 for the endpoint is valid for this channel to
         * simplify port allocation.
         *
         * @param channel replication communication channel to be used by the consensus module.
         * @return this for a fluent API
         * @see Configuration#REPLICATION_CHANNEL_PROP_NAME
         */
        public Context replicationChannel(final String channel)
        {
            replicationChannel = channel;
            return this;
        }

        /**
         * Get the replication channel for logs and snapshots.
         *
         * @return channel to receive replication responses from other node's archives when using log and snapshot
         * replication  to catch up.
         * @see Configuration#REPLICATION_CHANNEL_PROP_NAME
         */
        @Config
        public String replicationChannel()
        {
            return replicationChannel;
        }

        /**
         * Set a channel template used for replaying logs to a follower using the
         * {@link ClusterMember#catchupEndpoint()}.
         *
         * @param channel to do a catch replay to a follower.
         * @return this for a fluent API
         * @see Configuration#FOLLOWER_CATCHUP_CHANNEL_PROP_NAME
         */
        public Context followerCatchupChannel(final String channel)
        {
            this.followerCatchupChannel = channel;
            return this;
        }

        /**
         * Gets the channel template used for replaying logs to a follower using the
         * {@link ClusterMember#catchupEndpoint()}.
         *
         * @return channel used for replaying older data during a catchup phase.
         * @see Configuration#FOLLOWER_CATCHUP_CHANNEL_PROP_NAME
         */
        @Config
        public String followerCatchupChannel()
        {
            return followerCatchupChannel;
        }

        /**
         * Set a channel template used to build the control request channel for the leader Archive using the
         * {@link ClusterMember#archiveEndpoint()}.
         *
         * @param channel for the Archive control requests.
         * @return this for a fluent API
         * @see Configuration#LEADER_ARCHIVE_CONTROL_CHANNEL_PROP_NAME
         */
        public Context leaderArchiveControlChannel(final String channel)
        {
            this.leaderArchiveControlChannel = channel;
            return this;
        }

        /**
         * Gets the channel template used to build the control request channel for the leader Archive using the
         * {@link ClusterMember#archiveEndpoint()}.
         *
         * @return channel used for replaying older data during a catchup phase.
         * @see Configuration#LEADER_ARCHIVE_CONTROL_CHANNEL_PROP_NAME
         */
        @Config
        public String leaderArchiveControlChannel()
        {
            return leaderArchiveControlChannel;
        }

        /**
         * Set the fragment limit to be used when polling the log {@link Subscription}.
         *
         * @param logFragmentLimit for this clustered service.
         * @return this for a fluent API
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#LOG_FRAGMENT_LIMIT_DEFAULT
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#LOG_FRAGMENT_LIMIT_PROP_NAME
         */
        public int logFragmentLimit()
        {
            return logFragmentLimit;
        }

        /**
         * Resolution in nanoseconds for each tick of the timer wheel for scheduling deadlines.
         *
         * @param wheelTickResolutionNs the resolution in nanoseconds of each tick on the timer wheel.
         * @return this for a fluent API
         * @see Configuration#WHEEL_TICK_RESOLUTION_PROP_NAME
         */
        public Context wheelTickResolutionNs(final long wheelTickResolutionNs)
        {
            this.wheelTickResolutionNs = wheelTickResolutionNs;
            return this;
        }

        /**
         * Resolution in nanoseconds for each tick of the timer wheel for scheduling deadlines.
         *
         * @return the resolution in nanoseconds for each tick on the timer wheel.
         * @see Configuration#WHEEL_TICK_RESOLUTION_PROP_NAME
         */
        @Config
        public long wheelTickResolutionNs()
        {
            return wheelTickResolutionNs;
        }

        /**
         * Number of ticks, or spokes, on the timer wheel. Higher number of ticks reduces potential conflicts
         * traded off against memory usage.
         *
         * @param ticksPerWheel the number of ticks on the timer wheel.
         * @return this for a fluent API
         * @see Configuration#TICKS_PER_WHEEL_PROP_NAME
         */
        public Context ticksPerWheel(final int ticksPerWheel)
        {
            this.ticksPerWheel = ticksPerWheel;
            return this;
        }

        /**
         * Number of ticks, or spokes, on the timer wheel. Higher number of ticks reduces potential conflicts
         * traded off against memory usage.
         *
         * @return the number of ticks on the timer wheel.
         * @see Configuration#TICKS_PER_WHEEL_PROP_NAME
         */
        @Config
        public int ticksPerWheel()
        {
            return ticksPerWheel;
        }

        /**
         * Set the number of clustered services in this cluster instance.
         *
         * @param serviceCount the number of clustered services in this cluster instance.
         * @return this for a fluent API
         * @see Configuration#SERVICE_COUNT_PROP_NAME
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SERVICE_ID_PROP_NAME
         */
        public Context serviceCount(final int serviceCount)
        {
            this.serviceCount = serviceCount;
            return this;
        }

        /**
         * Get the number of clustered services in this cluster instance.
         *
         * @return the number of clustered services in this cluster instance.
         * @see Configuration#SERVICE_COUNT_PROP_NAME
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SERVICE_ID_PROP_NAME
         */
        @Config
        public int serviceCount()
        {
            return serviceCount;
        }

        /**
         * Set the limit for the maximum number of concurrent cluster sessions.
         *
         * @param maxSessions after which new sessions will be rejected.
         * @return this for a fluent API
         * @see Configuration#MAX_CONCURRENT_SESSIONS_PROP_NAME
         */
        public Context maxConcurrentSessions(final int maxSessions)
        {
            this.maxConcurrentSessions = maxSessions;
            return this;
        }

        /**
         * Get the limit for the maximum number of concurrent cluster sessions.
         *
         * @return the limit for the maximum number of concurrent cluster sessions.
         * @see Configuration#MAX_CONCURRENT_SESSIONS_PROP_NAME
         */
        @Config
        public int maxConcurrentSessions()
        {
            return maxConcurrentSessions;
        }

        /**
         * Timeout for a session if no activity is observed.
         *
         * @param sessionTimeoutNs to wait for activity on a session.
         * @return this for a fluent API.
         * @see Configuration#SESSION_TIMEOUT_PROP_NAME
         */
        public Context sessionTimeoutNs(final long sessionTimeoutNs)
        {
            this.sessionTimeoutNs = sessionTimeoutNs;
            return this;
        }

        /**
         * Timeout for a session if no activity is observed.
         *
         * @return the timeout for a session if no activity is observed.
         * @see Configuration#SESSION_TIMEOUT_PROP_NAME
         */
        @Config
        public long sessionTimeoutNs()
        {
            return CommonContext.checkDebugTimeout(sessionTimeoutNs, TimeUnit.NANOSECONDS);
        }

        /**
         * Timeout for a leader if no heartbeat is received by another member.
         *
         * @param heartbeatTimeoutNs to wait for heartbeat from a leader.
         * @return this for a fluent API.
         * @see Configuration#LEADER_HEARTBEAT_TIMEOUT_PROP_NAME
         */
        public Context leaderHeartbeatTimeoutNs(final long heartbeatTimeoutNs)
        {
            this.leaderHeartbeatTimeoutNs = heartbeatTimeoutNs;
            return this;
        }

        /**
         * Timeout for a leader if no heartbeat is received by another member.
         *
         * @return the timeout for a leader if no heartbeat is received by another member.
         * @see Configuration#LEADER_HEARTBEAT_TIMEOUT_PROP_NAME
         */
        @Config
        public long leaderHeartbeatTimeoutNs()
        {
            return leaderHeartbeatTimeoutNs;
        }

        /**
         * Interval at which a leader will send heartbeats if the log is not progressing.
         *
         * @param heartbeatIntervalNs between leader heartbeats.
         * @return this for a fluent API.
         * @see Configuration#LEADER_HEARTBEAT_INTERVAL_PROP_NAME
         */
        @Config
        public Context leaderHeartbeatIntervalNs(final long heartbeatIntervalNs)
        {
            this.leaderHeartbeatIntervalNs = heartbeatIntervalNs;
            return this;
        }

        /**
         * Interval at which a leader will send heartbeats if the log is not progressing.
         *
         * @return the interval at which a leader will send heartbeats if the log is not progressing.
         * @see Configuration#LEADER_HEARTBEAT_INTERVAL_PROP_NAME
         */
        public long leaderHeartbeatIntervalNs()
        {
            return leaderHeartbeatIntervalNs;
        }

        /**
         * Timeout to wait for hearing the status of all cluster members on startup after recovery before commencing
         * an election if a majority of members has been heard from.
         *
         * @param timeoutNs to wait on startup after recovery before commencing an election.
         * @return this for a fluent API.
         * @see Configuration#STARTUP_CANVASS_TIMEOUT_PROP_NAME
         */
        public Context startupCanvassTimeoutNs(final long timeoutNs)
        {
            this.startupCanvassTimeoutNs = timeoutNs;
            return this;
        }

        /**
         * Timeout to wait for hearing the status of all cluster members on startup after recovery before commencing
         * an election if a majority of members has been heard from.
         *
         * @return the timeout to wait on startup after recovery before commencing an election.
         * @see Configuration#STARTUP_CANVASS_TIMEOUT_PROP_NAME
         */
        @Config
        public long startupCanvassTimeoutNs()
        {
            return startupCanvassTimeoutNs;
        }

        /**
         * Timeout to wait for votes in an election before declaring the election void and starting over.
         *
         * @param timeoutNs to wait for votes in an elections.
         * @return this for a fluent API.
         * @see Configuration#ELECTION_TIMEOUT_PROP_NAME
         */
        public Context electionTimeoutNs(final long timeoutNs)
        {
            this.electionTimeoutNs = timeoutNs;
            return this;
        }

        /**
         * Timeout to wait for votes in an election before declaring the election void and starting over.
         *
         * @return the timeout to wait for votes in an elections.
         * @see Configuration#ELECTION_TIMEOUT_PROP_NAME
         */
        @Config
        public long electionTimeoutNs()
        {
            return electionTimeoutNs;
        }

        /**
         * Interval at which a member will send out status messages during the election phases.
         *
         * @param electionStatusIntervalNs between status message updates.
         * @return this for a fluent API.
         * @see Configuration#ELECTION_STATUS_INTERVAL_PROP_NAME
         * @see Configuration#ELECTION_STATUS_INTERVAL_DEFAULT_NS
         */
        public Context electionStatusIntervalNs(final long electionStatusIntervalNs)
        {
            this.electionStatusIntervalNs = electionStatusIntervalNs;
            return this;
        }

        /**
         * Interval at which a member will send out status messages during the election phases.
         *
         * @return the interval at which a member will send out status messages during the election phases.
         * @see Configuration#ELECTION_STATUS_INTERVAL_PROP_NAME
         * @see Configuration#ELECTION_STATUS_INTERVAL_DEFAULT_NS
         */
        @Config
        public long electionStatusIntervalNs()
        {
            return electionStatusIntervalNs;
        }

        /**
         * Timeout to wait for follower termination by leader.
         *
         * @param terminationTimeoutNs to wait for follower termination.
         * @return this for a fluent API.
         * @see Configuration#TERMINATION_TIMEOUT_PROP_NAME
         * @see Configuration#TERMINATION_TIMEOUT_DEFAULT_NS
         */
        public Context terminationTimeoutNs(final long terminationTimeoutNs)
        {
            this.terminationTimeoutNs = terminationTimeoutNs;
            return this;
        }

        /**
         * Timeout to wait for follower termination by leader.
         *
         * @return timeout to wait for follower termination by leader.
         * @see Configuration#TERMINATION_TIMEOUT_PROP_NAME
         * @see Configuration#TERMINATION_TIMEOUT_DEFAULT_NS
         */
        @Config
        public long terminationTimeoutNs()
        {
            return terminationTimeoutNs;
        }

        /**
         * Set a threshold for the consensus module agent work cycle time which when exceed it will increment the
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
         * Threshold for the consensus module agent work cycle time which when exceed it will increment the
         * counter.
         *
         * @return threshold to track for the consensus module agent work cycle time.
         */
        @Config
        public long cycleThresholdNs()
        {
            return cycleThresholdNs;
        }

        /**
         * Set a duty cycle tracker to be used for tracking the duty cycle time of the consensus module agent.
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
         * The duty cycle tracker used to track the consensus module agent duty cycle.
         *
         * @return the duty cycle tracker.
         */
        public DutyCycleTracker dutyCycleTracker()
        {
            return dutyCycleTracker;
        }

        /**
         * Set a threshold for total snapshot duration which when exceeded will result in a counter increment.
         *
         * @param thresholdNs value in nanoseconds
         * @return this for fluent API.
         * @see ConsensusModule.Configuration#TOTAL_SNAPSHOT_DURATION_THRESHOLD_PROP_NAME
         * @see ConsensusModule.Configuration#TOTAL_SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS
         */
        public Context totalSnapshotDurationThresholdNs(final long thresholdNs)
        {
            this.totalSnapshotDurationThresholdNs = thresholdNs;
            return this;
        }

        /**
         * Threshold for total snapshot duration which when exceeded it will increment the counter.
         *
         * @return threshold value in nanoseconds.
         */
        @Config
        public long totalSnapshotDurationThresholdNs()
        {
            return totalSnapshotDurationThresholdNs;
        }

        /**
         * Set snapshot duration tracker used for monitoring total snapshot duration.
         *
         * @param snapshotDurationTracker snapshot duration tracker.
         * @return this for fluent API.
         */
        public Context totalSnapshotDurationTracker(final SnapshotDurationTracker snapshotDurationTracker)
        {
            this.totalSnapshotDurationTracker = snapshotDurationTracker;
            return this;
        }

        /**
         * Get snapshot duration tracker used for monitoring total snapshot duration.
         *
         * @return snapshot duration tracker
         */
        public SnapshotDurationTracker totalSnapshotDurationTracker()
        {
            return totalSnapshotDurationTracker;
        }

        /**
         * Get the {@link Agent#roleName()} to be used for the consensus module agent. If {@code null} then one will
         * be generated.
         *
         * @return the {@link Agent#roleName()} to be used for the consensus module agent.
         */
        @Config(id = "CLUSTER_CONSENSUS_MODULE_AGENT_ROLE_NAME")
        public String agentRoleName()
        {
            return agentRoleName;
        }

        /**
         * Set the {@link Agent#roleName()} to be used for the consensus module agent.
         *
         * @param agentRoleName to be used for the consensus module agent.
         * @return this for a fluent API.
         */
        public Context agentRoleName(final String agentRoleName)
        {
            this.agentRoleName = agentRoleName;
            return this;
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
        public IdleStrategy idleStrategy()
        {
            return idleStrategySupplier.get();
        }

        /**
         * Set the {@link ClusterClock} to be used for timestamping messages and timers.
         *
         * @param clock {@link ClusterClock} to be used for timestamping messages and timers.
         * @return this for a fluent API.
         */
        public Context clusterClock(final ClusterClock clock)
        {
            this.clusterClock = clock;
            return this;
        }

        /**
         * Get the {@link ClusterClock} to used for timestamping messages and timers.
         *
         * @return the {@link ClusterClock} to used for timestamping messages and timers.
         */
        @Config
        public ClusterClock clusterClock()
        {
            return clusterClock;
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
         * Set the supplier of a consumer of timestamps which can be used for testing time progress in a cluster. The
         * timestamp passed to the consumer is the timestamp of the last completed {@link Agent#doWork()} cycle by the
         * consensus module {@link Agent}. The supplier will be called after the context is concluded and can be
         * referenced during the construction of the consumer.
         *
         * @param clusterTimeConsumerSupplier to which the latest timestamp will be passed.
         * @return this for a fluent API
         */
        public Context clusterTimeConsumerSupplier(final Function<Context, LongConsumer> clusterTimeConsumerSupplier)
        {
            this.clusterTimeConsumerSupplier = clusterTimeConsumerSupplier;
            return this;
        }

        /**
         * Get the supplier of a consumer of timestamps which can be used for testing time progress in a cluster. The
         * timestamp passed to the consumer is the timestamp of the last completed {@link Agent#doWork()} cycle by the
         * consensus module {@link Agent}.
         *
         * @return the consumer of timestamps for completed work cycles.
         */
        public Function<Context, LongConsumer> clusterTimeConsumerSupplier()
        {
            return clusterTimeConsumerSupplier;
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
         * Set the {@link ErrorHandler} to be used by the Consensus Module.
         *
         * @param errorHandler the error handler to be used by the Consensus Module.
         * @return this for a fluent API
         */
        public Context errorHandler(final ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        /**
         * The {@link #errorHandler()} that will increment {@link #errorCounter()} by default.
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
         * Get the counter for the current state of the consensus module.
         *
         * @return the counter for the current state of the consensus module.
         * @see ConsensusModule.State
         */
        public Counter moduleStateCounter()
        {
            return moduleStateCounter;
        }

        /**
         * Set the counter for the current state of the consensus module.
         *
         * @param moduleState the counter for the current state of the consensus module.
         * @return this for a fluent API.
         * @see ConsensusModule.State
         */
        public Context moduleStateCounter(final Counter moduleState)
        {
            this.moduleStateCounter = moduleState;
            return this;
        }

        /**
         * Get the counter for the current state of an election.
         *
         * @return the counter for the current state of an election.
         * @see ElectionState
         */
        public Counter electionStateCounter()
        {
            return electionStateCounter;
        }

        /**
         * Set the counter for the current state of an election.
         *
         * @param electionStateCounter for the current state of an election.
         * @return this for a fluent API.
         * @see ElectionState
         */
        public Context electionStateCounter(final Counter electionStateCounter)
        {
            this.electionStateCounter = electionStateCounter;
            return this;
        }

        /**
         * Get the counter for the commit position the cluster has reached for consensus.
         *
         * @return the counter for the commit position the cluster has reached for consensus.
         */
        public Counter commitPositionCounter()
        {
            return commitPosition;
        }

        /**
         * Set the counter for the commit position the cluster has reached for consensus.
         *
         * @param commitPosition counter for the commit position the cluster has reached for consensus.
         * @return this for a fluent API.
         */
        public Context commitPositionCounter(final Counter commitPosition)
        {
            this.commitPosition = commitPosition;
            return this;
        }

        /**
         * Get the counter for representing the current {@link io.aeron.cluster.service.Cluster.Role} of the
         * consensus module node.
         *
         * @return the counter for representing the current {@link io.aeron.cluster.service.Cluster.Role} of the
         * cluster node.
         * @see io.aeron.cluster.service.Cluster.Role
         */
        public Counter clusterNodeRoleCounter()
        {
            return clusterNodeRoleCounter;
        }

        /**
         * Set the counter for representing the current {@link io.aeron.cluster.service.Cluster.Role} of the
         * cluster node.
         *
         * @param nodeRole the counter for representing the current {@link io.aeron.cluster.service.Cluster.Role}
         *                 of the cluster node.
         * @return this for a fluent API.
         * @see io.aeron.cluster.service.Cluster.Role
         */
        public Context clusterNodeRoleCounter(final Counter nodeRole)
        {
            this.clusterNodeRoleCounter = nodeRole;
            return this;
        }

        /**
         * Get the counter for the control toggle for triggering actions for the cluster.
         *
         * @return the counter for triggering cluster node actions.
         * @see ClusterControl
         */
        public Counter controlToggleCounter()
        {
            return clusterControlToggle;
        }

        /**
         * Set the counter for the control toggle for triggering actions for the cluster.
         *
         * @param controlToggle the counter for triggering cluster node actions.
         * @return this for a fluent API.
         * @see ClusterControl
         */
        public Context controlToggleCounter(final Counter controlToggle)
        {
            this.clusterControlToggle = controlToggle;
            return this;
        }

        /**
         * Get the counter for the control toggle for triggering actions on the cluster node.
         *
         * @return the counter for triggering cluster node actions.
         * @see ClusterControl
         */
        public Counter nodeControlToggleCounter()
        {
            return nodeControlToggle;
        }

        /**
         * Set the counter for the control toggle for triggering actions on the cluster node.
         *
         * @param nodeControlToggle the counter for triggering cluster node actions.
         * @return this for a fluent API.
         * @see ClusterControl
         */
        public Context nodeControlToggleCounter(final Counter nodeControlToggle)
        {
            this.nodeControlToggle = nodeControlToggle;
            return this;
        }

        /**
         * Get the counter for the count of snapshots taken.
         *
         * @return the counter for the count of snapshots taken.
         */
        public Counter snapshotCounter()
        {
            return snapshotCounter;
        }

        /**
         * Set the counter for the count of snapshots taken.
         *
         * @param snapshotCounter the count of snapshots taken.
         * @return this for a fluent API.
         */
        public Context snapshotCounter(final Counter snapshotCounter)
        {
            this.snapshotCounter = snapshotCounter;
            return this;
        }

        /**
         * Get the counter for the count of clients that have been timed out and disconnected.
         *
         * @return the counter for the count of clients that have been timed out and disconnected.
         */
        public Counter timedOutClientCounter()
        {
            return timedOutClientCounter;
        }

        /**
         * Set the counter for the count of clients that have been timed out and disconnected.
         *
         * @param timedOutClientCounter the count of clients that have been timed out and disconnected.
         * @return this for a fluent API.
         */
        public Context timedOutClientCounter(final Counter timedOutClientCounter)
        {
            this.timedOutClientCounter = timedOutClientCounter;
            return this;
        }

        /**
         * {@link Aeron} client for communicating with the local Media Driver.
         * <p>
         * This client will be closed when the {@link ConsensusModule#close()} or {@link #close()} methods are called
         * if {@link #ownsAeronClient()} is true.
         * <p>
         * The method is mostly here in order to test with a custom Aeron instance. The recommended approach is to set
         * the aeronDirectoryName and allow the ConsensusModule to construct the Aeron client. The supplied Aeron
         * client must be set to {@code Aeron.Context.useConductorInvoker(true)} to work correctly with the
         * ConsensusModule.
         *
         * @param aeron client for communicating with the local Media Driver.
         * @return this for a fluent API.
         * @see io.aeron.Aeron#connect()
         * @see io.aeron.Aeron.Context#useConductorAgentInvoker(boolean)
         * @see #aeronDirectoryName(String)
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
         * Set the {@link io.aeron.archive.client.AeronArchive.Context} that should be used for communicating with the
         * local Archive.
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
         * Get the {@link io.aeron.archive.client.AeronArchive.Context} that should be used for communicating with
         * the local Archive.
         *
         * @return the {@link io.aeron.archive.client.AeronArchive.Context} that should be used for communicating
         * with the local Archive.
         */
        public AeronArchive.Context archiveContext()
        {
            return archiveContext;
        }

        /**
         * Get the {@link AuthenticatorSupplier} that should be used for the consensus module.
         *
         * @return the {@link AuthenticatorSupplier} to be used for the consensus module.
         */
        @Config
        public AuthenticatorSupplier authenticatorSupplier()
        {
            return authenticatorSupplier;
        }

        /**
         * Set the {@link AuthenticatorSupplier} that will be used for the consensus module.
         *
         * @param authenticatorSupplier {@link AuthenticatorSupplier} to use for the consensus module.
         * @return this for a fluent API.
         */
        public Context authenticatorSupplier(final AuthenticatorSupplier authenticatorSupplier)
        {
            this.authenticatorSupplier = authenticatorSupplier;
            return this;
        }

        /**
         * Get the {@link AuthorisationServiceSupplier} that should be used for the consensus module.
         *
         * @return the {@link AuthorisationServiceSupplier} to be used for the consensus module.
         */
        @Config
        public AuthorisationServiceSupplier authorisationServiceSupplier()
        {
            return authorisationServiceSupplier;
        }

        /**
         * <p>Set the {@link AuthorisationServiceSupplier} that will be used for the consensus module.</p>
         *
         * <p>When using an authorisation service for the ConsensusModule, then the following values for protocolId,
         * actionId, and type should be considered.</p>
         * <table>
         *     <caption>Parameters for authorisation service queries from the Consensus Module</caption>
         *     <thead>
         *         <tr><td>Description</td><td>protocolId</td><td>actionId</td><td>type(s)</td></tr>
         *     </thead>
         *     <tbody>
         *         <tr>
         *             <td>Admin requests made through the client API</td>
         *             <td>{@link MessageHeaderDecoder#SCHEMA_ID}</td>
         *             <td>{@link AdminRequestDecoder#TEMPLATE_ID}</td>
         *             <td>{@link AdminRequestType#SNAPSHOT}</td>
         *         </tr>
         *         <tr>
         *             <td>Backup queries from Cluster Backup &amp; Standby</td>
         *             <td></td>
         *             <td>{@link BackupQueryDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Heartbeat requests from Cluster Standby</td>
         *             <td></td>
         *             <td>{@link HeartbeatRequestDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *         <tr>
         *             <td>Standby snapshot notifications from Cluster Standby</td>
         *             <td></td>
         *             <td>{@link StandbySnapshotDecoder#TEMPLATE_ID}</td>
         *             <td><code>(null)</code></td>
         *         </tr>
         *     </tbody>
         * </table>
         *
         * @param authorisationServiceSupplier {@link AuthorisationServiceSupplier} to use for the consensus module.
         * @return this for a fluent API.
         */
        public Context authorisationServiceSupplier(final AuthorisationServiceSupplier authorisationServiceSupplier)
        {
            this.authorisationServiceSupplier = authorisationServiceSupplier;
            return this;
        }

        /**
         * Should an {@link AgentInvoker} be used for running the {@link ConsensusModule} rather than run it on
         * a thread with a {@link AgentRunner}.
         *
         * @param useAgentInvoker use {@link AgentInvoker} be used for running the {@link ConsensusModule}?
         * @return this for a fluent API.
         */
        public ConsensusModule.Context useAgentInvoker(final boolean useAgentInvoker)
        {
            this.useAgentInvoker = useAgentInvoker;
            return this;
        }

        /**
         * Should an {@link AgentInvoker} be used for running the {@link ConsensusModule} rather than run it on
         * a thread with a {@link AgentRunner}.
         *
         * @return true if the {@link ConsensusModule} will be run with an {@link AgentInvoker} otherwise false.
         */
        public boolean useAgentInvoker()
        {
            return useAgentInvoker;
        }

        /**
         * Set the {@link Runnable} that is called when the {@link ConsensusModule} processes a termination action.
         *
         * @param terminationHook that can be used to terminate a consensus module.
         * @return this for a fluent API.
         */
        public Context terminationHook(final Runnable terminationHook)
        {
            this.terminationHook = terminationHook;
            return this;
        }

        /**
         * Get the {@link Runnable} that is called when the {@link ConsensusModule} processes a termination action.
         *
         * @return the {@link Runnable} that can be used to terminate a consensus module.
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

        Context nodeStateFile(final NodeStateFile nodeStateFile)
        {
            this.nodeStateFile = nodeStateFile;
            return this;
        }

        /**
         * The {@link NodeStateFile} that is used by the consensus module.
         *
         * @return node state file.
         */
        public NodeStateFile nodeStateFile()
        {
            return nodeStateFile;
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
         * The source of random values for timeouts used in elections.
         *
         * @param random source of random values for timeouts used in elections.
         * @return this for a fluent API.
         */
        public Context random(final Random random)
        {
            this.random = random;
            return this;
        }

        /**
         * The source of random values for timeouts used in elections.
         *
         * @return source of random values for timeouts used in elections.
         */
        public Random random()
        {
            return random;
        }

        /**
         * Provides an {@link TimerService} supplier for the idle strategy for the agent duty cycle.
         *
         * @param timerServiceSupplier supplier for the idle strategy for the agent duty cycle.
         * @return this for a fluent API.
         */
        public Context timerServiceSupplier(final TimerServiceSupplier timerServiceSupplier)
        {
            this.timerServiceSupplier = timerServiceSupplier;
            return this;
        }

        /**
         * Supplier of the {@link TimerService} instances.
         *
         * @return supplier of dynamically created {@link TimerService} instances.
         */
        @Config
        public TimerServiceSupplier timerServiceSupplier()
        {
            return timerServiceSupplier;
        }

        /**
         * Register a ConsensusModuleExtension to extend the behaviour of the
         * consensus module instead of using ClusteredServices.
         *
         * @param consensusModuleExtension supplier for consensus module extension
         * @return this for a fluent API.
         */
        public Context consensusModuleExtension(final ConsensusModuleExtension consensusModuleExtension)
        {
            this.consensusModuleExtension = consensusModuleExtension;
            return this;
        }

        /**
         * Registered consensus module extension.
         *
         * @return Registered consensus module extension or null.
         */
        public ConsensusModuleExtension consensusModuleExtension()
        {
            return consensusModuleExtension;
        }

        /**
         * Deprecated for removal.
         *
         * @return {@code null}.
         */
        @Deprecated
        public NameResolver nameResolver()
        {
            return null;
        }

        /**
         * Deprecated for removal.
         *
         * @param ignore as deprecated.
         * @return this for fluent API.
         */
        @Deprecated
        public Context nameResolver(final NameResolver ignore)
        {
            return this;
        }

        /**
         * Indicate whether this node should accept snapshots from standby nodes.
         *
         * @return <code>true</code> if this node should accept snapshots from standby nodes, <code>false</code>
         * otherwise.
         * @see Configuration#CLUSTER_ACCEPT_STANDBY_SNAPSHOTS_PROP_NAME
         * @see Configuration#acceptStandbySnapshots()
         */
        @Config(id = "CLUSTER_ACCEPT_STANDBY_SNAPSHOTS")
        public boolean acceptStandbySnapshots()
        {
            return acceptStandbySnapshots;
        }

        /**
         * Indicate whether this node should accept snapshots from standby nodes.
         *
         * @param acceptStandbySnapshots <code>true</code> if this node should accept snapshots from standby nodes,
         *                               <code>false</code> otherwise.
         * @return this for a fluent API.
         * @see Configuration#CLUSTER_ACCEPT_STANDBY_SNAPSHOTS_PROP_NAME
         * @see Configuration#acceptStandbySnapshots()
         */
        public ConsensusModule.Context acceptStandbySnapshots(final boolean acceptStandbySnapshots)
        {
            this.acceptStandbySnapshots = acceptStandbySnapshots;
            return this;
        }

        /**
         * Get the counter used to track standby snapshots accepted by this node.
         *
         * @return the counter for standby snapshots.
         */
        public Counter standbySnapshotCounter()
        {
            return standbySnapshotCounter;
        }

        /**
         * Set the counter used to track standby snapshots accepted by this node.
         *
         * @param standbySnapshotCounter the counter for standby snapshots.
         * @return this for a fluentAPI.
         */
        public Context standbySnapshotCounter(final Counter standbySnapshotCounter)
        {
            this.standbySnapshotCounter = standbySnapshotCounter;
            return this;
        }

        /**
         * Get the counter used to track the number of elections on this node.
         *
         * @return the counter for elections.
         * @since 1.44.0
         */
        public Counter electionCounter()
        {
            return electionCounter;
        }

        /**
         * Set the counter used to track the number of elections on this node.
         *
         * @param electionCounter the counter for elections.
         * @return this for a fluentAPI.
         * @since 1.44.0
         */
        public Context electionCounter(final Counter electionCounter)
        {
            this.electionCounter = electionCounter;
            return this;
        }

        /**
         * Get the counter used to track the leadership term id.
         *
         * @return the counter containing the leadership term id.
         * @since 1.44.0
         */
        public Counter leadershipTermIdCounter()
        {
            return leadershipTermId;
        }

        /**
         * Set the counter used to track the number of elections on this node.
         *
         * @param leadershipTermId the counter for elections.
         * @return this for a fluentAPI.
         * @since 1.44.0
         */
        public Context leadershipTermIdCounter(final Counter leadershipTermId)
        {
            this.leadershipTermId = leadershipTermId;
            return this;
        }

        /**
         * If the ConsensusModule should set the control endpoint for the Consensus publication.
         *
         * @return <code>true</code> if the control endpoint should be set, <code>false</code> otherwise.
         */
        public boolean enableControlOnConsensusChannel()
        {
            return enableControlOnConsensusChannel;
        }

        /**
         * If the ConsensusModule should bind the control endpoint for the Consensus publication.
         *
         * @param enableControlOnConsensusChannel <code>true</code> if the control endpoint should be set,
         *                                        <code>false</code> otherwise.
         * @return this for fluent API.
         */
        public Context enableControlOnConsensusChannel(final boolean enableControlOnConsensusChannel)
        {
            this.enableControlOnConsensusChannel = enableControlOnConsensusChannel;
            return this;
        }

        /**
         * If the ConsensusModule should set the control endpoint for the Consensus publication.
         *
         * @return <code>true</code> if the control endpoint should be bound, <code>false</code> otherwise.
         */
        public boolean enableControlOnLogControl()
        {
            return enableControlOnLogChannel;
        }

        /**
         * If the LogModule should set the control endpoint for the Log publication.
         *
         * @param setControlOnLogChannel <code>true</code> if the control endpoint should be set, <code>false</code>
         *                               otherwise.
         * @return this for fluent API.
         */
        public Context enableControlOnLogControl(final boolean setControlOnLogChannel)
        {
            this.enableControlOnLogChannel = setControlOnLogChannel;
            return this;
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
            CloseHelper.close(countedErrorHandler, recordingLog);
            CloseHelper.close(countedErrorHandler, nodeStateFile);
            CloseHelper.close(countedErrorHandler, markFile);
            if (errorHandler instanceof AutoCloseable handler)
            {
                CloseHelper.quietClose(handler); // Ignore error to ensure the rest will be closed
            }

            if (ownsAeronClient)
            {
                CloseHelper.close(aeron);
            }
            else if (!aeron.isClosed())
            {
                CloseHelper.closeAll(
                    timedOutClientCounter,
                    clusterControlToggle,
                    snapshotCounter,
                    moduleStateCounter,
                    electionStateCounter,
                    clusterNodeRoleCounter,
                    commitPosition);
            }
        }

        Context logPublisher(final LogPublisher logPublisher)
        {
            this.logPublisher = logPublisher;
            return this;
        }

        LogPublisher logPublisher()
        {
            return logPublisher;
        }

        Context egressPublisher(final EgressPublisher egressPublisher)
        {
            this.egressPublisher = egressPublisher;
            return this;
        }

        EgressPublisher egressPublisher()
        {
            return egressPublisher;
        }

        boolean isLogMdc()
        {
            return isLogMdc;
        }

        /**
         * Start up the consensus module using pre-supplied state skipping the recovery process. Internal use only.
         *
         * @param bootstrapState to initialize the consensus module.
         * @return this for a fluent API.
         */
        ConsensusModule.Context bootstrapState(final ConsensusModuleStateExport bootstrapState)
        {
            this.bootstrapState = bootstrapState;
            return this;
        }

        ConsensusModuleStateExport bootstrapState()
        {
            return bootstrapState;
        }

        private void concludeMarkFile()
        {
            final String aeronDirectory = aeron.context().aeronDirectoryName();
            final String authenticatorClassName = authenticatorSupplier.getClass().getName();
            ClusterMarkFile.checkHeaderLength(
                aeronDirectory, controlChannel(), ingressChannel, null, authenticatorClassName);

            markFile.encoder()
                .archiveStreamId(archiveContext.controlRequestStreamId())
                .serviceStreamId(serviceStreamId)
                .consensusModuleStreamId(consensusModuleStreamId)
                .ingressStreamId(ingressStreamId)
                .memberId(clusterMemberId)
                .serviceId(SERVICE_ID)
                .clusterId(clusterId)
                .aeronDirectory(aeronDirectory)
                .controlChannel(controlChannel)
                .ingressChannel(ingressChannel)
                .serviceName(null)
                .authenticator(authenticatorClassName)
                .servicesClusterDir(clusterServicesDirectoryName);

            markFile.updateActivityTimestamp(epochClock.time());
            markFile.signalReady();
            markFile.force();
        }

        private TimerServiceSupplier getTimerServiceSupplierFromSystemProperty()
        {
            final String timeServiceClassName = Configuration.timerServiceSupplier();
            if (WheelTimerServiceSupplier.class.getName().equals(timeServiceClassName))
            {
                return new WheelTimerServiceSupplier(
                    clusterClock.timeUnit(),
                    0,
                    findNextPositivePowerOfTwo(
                        clusterClock.timeUnit().convert(wheelTickResolutionNs, TimeUnit.NANOSECONDS)),
                    ticksPerWheel);
            }
            else if (PriorityHeapTimerServiceSupplier.class.getName().equals(timeServiceClassName))
            {
                return new PriorityHeapTimerServiceSupplier();
            }

            throw new ClusterException("invalid TimerServiceSupplier: " + timeServiceClassName);
        }

        private void validateLogChannel()
        {
            final ChannelUri logChannelUri = parse(logChannel);
            if (logChannelUri.containsKey(INITIAL_TERM_ID_PARAM_NAME))
            {
                throw new ConfigurationException("logChannel must not contain: " + INITIAL_TERM_ID_PARAM_NAME);
            }
            if (logChannelUri.containsKey(TERM_ID_PARAM_NAME))
            {
                throw new ConfigurationException("logChannel must not contain: " + TERM_ID_PARAM_NAME);
            }
            if (logChannelUri.containsKey(TERM_OFFSET_PARAM_NAME))
            {
                throw new ConfigurationException("logChannel must not contain: " + TERM_OFFSET_PARAM_NAME);
            }
        }

        /**
         * {@inheritDoc}
         */
        public String toString()
        {
            return "ConsensusModule.Context" +
                "\n{" +
                "\n    isConcluded=" + isConcluded() +
                "\n    ownsAeronClient=" + ownsAeronClient +
                "\n    aeronDirectoryName='" + aeronDirectoryName + '\'' +
                "\n    aeron=" + aeron +
                "\n    deleteDirOnStart=" + deleteDirOnStart +
                "\n    clusterDirectoryName='" + clusterDirectoryName + '\'' +
                "\n    clusterDir=" + clusterDir +
                "\n    recordingLog=" + recordingLog +
                "\n    markFile=" + markFile +
                "\n    fileSyncLevel=" + fileSyncLevel +
                "\n    appVersion=" + appVersion +
                "\n    clusterId=" + clusterId +
                "\n    clusterMemberId=" + clusterMemberId +
                "\n    appointedLeaderId=" + appointedLeaderId +
                "\n    clusterMembers='" + clusterMembers + '\'' +
                "\n    ingressChannel='" + ingressChannel + '\'' +
                "\n    ingressStreamId=" + ingressStreamId +
                "\n    ingressFragmentLimit=" + ingressFragmentLimit +
                "\n    logChannel='" + logChannel + '\'' +
                "\n    logStreamId=" + logStreamId +
                "\n    memberEndpoints='" + memberEndpoints + '\'' +
                "\n    replayChannel='" + replayChannel + '\'' +
                "\n    replayStreamId=" + replayStreamId +
                "\n    controlChannel='" + controlChannel + '\'' +
                "\n    consensusModuleStreamId=" + consensusModuleStreamId +
                "\n    serviceStreamId=" + serviceStreamId +
                "\n    snapshotChannel='" + snapshotChannel + '\'' +
                "\n    snapshotStreamId=" + snapshotStreamId +
                "\n    consensusChannel='" + consensusChannel + '\'' +
                "\n    consensusStreamId=" + consensusStreamId +
                "\n    replicationChannel='" + replicationChannel + '\'' +
                "\n    logFragmentLimit=" + logFragmentLimit +
                "\n    serviceCount=" + serviceCount +
                "\n    errorBufferLength=" + errorBufferLength +
                "\n    maxConcurrentSessions=" + maxConcurrentSessions +
                "\n    ticksPerWheel=" + ticksPerWheel +
                "\n    wheelTickResolutionNs=" + wheelTickResolutionNs +
                "\n    timerServiceSupplier=" + timerServiceSupplier +
                "\n    sessionTimeoutNs=" + sessionTimeoutNs +
                "\n    leaderHeartbeatTimeoutNs=" + leaderHeartbeatTimeoutNs +
                "\n    leaderHeartbeatIntervalNs=" + leaderHeartbeatIntervalNs +
                "\n    startupCanvassTimeoutNs=" + startupCanvassTimeoutNs +
                "\n    electionTimeoutNs=" + electionTimeoutNs +
                "\n    electionStatusIntervalNs=" + electionStatusIntervalNs +
                "\n    terminationTimeoutNs=" + terminationTimeoutNs +
                "\n    threadFactory=" + threadFactory +
                "\n    idleStrategySupplier=" + idleStrategySupplier +
                "\n    clusterClock=" + clusterClock +
                "\n    epochClock=" + epochClock +
                "\n    random=" + random +
                "\n    errorLog=" + errorLog +
                "\n    errorHandler=" + errorHandler +
                "\n    errorCounter=" + errorCounter +
                "\n    countedErrorHandler=" + countedErrorHandler +
                "\n    moduleStateCounter=" + moduleStateCounter +
                "\n    electionStateCounter=" + electionStateCounter +
                "\n    clusterNodeRoleCounter=" + clusterNodeRoleCounter +
                "\n    commitPosition=" + commitPosition +
                "\n    controlToggle=" + clusterControlToggle +
                "\n    nodeControlToggle=" + nodeControlToggle +
                "\n    snapshotCounter=" + snapshotCounter +
                "\n    timedOutClientCounter=" + timedOutClientCounter +
                "\n    standbySnapshotCounter=" + standbySnapshotCounter +
                "\n    electionCounter=" + electionCounter +
                "\n    leadershipTermId=" + leadershipTermId +
                "\n    terminationHook=" + terminationHook +
                "\n    archiveContext=" + archiveContext +
                "\n    authenticatorSupplier=" + authenticatorSupplier +
                "\n    authorisationServiceSupplier=" + authorisationServiceSupplier +
                "\n    logPublisher=" + logPublisher +
                "\n    egressPublisher=" + egressPublisher +
                "\n    isLogMdc=" + isLogMdc +
                "\n    useAgentInvoker=" + useAgentInvoker +
                "\n    cycleThresholdNs=" + cycleThresholdNs +
                "\n    dutyCycleTracker=" + dutyCycleTracker +
                "\n    totalSnapshotDurationThresholdNs=" + totalSnapshotDurationThresholdNs +
                "\n    totalSnapshotDurationTracker=" + totalSnapshotDurationTracker +
                "\n    acceptStandbySnapshots=" + acceptStandbySnapshots +
                "\n    bootstrapState=" + bootstrapState +
                "\n}";
        }
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "ConsensusModule{" +
            "conductor=" + conductor +
            '}';
    }
}
