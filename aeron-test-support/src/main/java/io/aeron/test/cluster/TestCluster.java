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
package io.aeron.test.cluster;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.Counter;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingSignalConsumer;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterBackupEventsListener;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.ClusterMember;
import io.aeron.cluster.ClusterMembership;
import io.aeron.cluster.ClusterTool;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.ElectionState;
import io.aeron.cluster.NodeControl;
import io.aeron.cluster.RecordingLog;
import io.aeron.cluster.TimerServiceSupplier;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.client.ControlledEgressListener;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.NewLeadershipTermEventDecoder;
import io.aeron.cluster.service.Cluster;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.RegistrationException;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.samples.archive.RecordingDescriptor;
import io.aeron.samples.archive.RecordingDescriptorCollector;
import io.aeron.security.AuthenticatorSupplier;
import io.aeron.security.AuthorisationServiceSupplier;
import io.aeron.security.CredentialsSupplier;
import io.aeron.security.DefaultAuthenticatorSupplier;
import io.aeron.security.NullCredentialsSupplier;
import io.aeron.test.DataCollector;
import io.aeron.test.Tests;
import io.aeron.test.driver.DriverOutputConsumer;
import io.aeron.test.driver.JavaTestMediaDriver;
import io.aeron.test.driver.RedirectingNameResolver;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.MutableBoolean;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.service.Cluster.Role.FOLLOWER;
import static io.aeron.test.cluster.ClusterTests.*;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public final class TestCluster implements AutoCloseable
{
    static final int SEGMENT_FILE_LENGTH = 16 * 1024 * 1024;
    static final long CATALOG_CAPACITY = 128 * 1024;

    static final String LOG_CHANNEL = "aeron:udp?term-length=512k|alias=raft";
    static final String ARCHIVE_LOCAL_CONTROL_CHANNEL = "aeron:ipc";
    static final String EGRESS_CHANNEL = "aeron:udp?term-length=128k|endpoint=localhost:0|alias=egress";
    static final String INGRESS_CHANNEL = "aeron:udp?term-length=128k|alias=ingress";
    static final long LEADER_HEARTBEAT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);
    static final long STARTUP_CANVASS_TIMEOUT_NS = LEADER_HEARTBEAT_TIMEOUT_NS * 2;
    public static final String CLUSTER_BASE_DIR_PROP_NAME = "aeron.test.system.cluster.base.dir";

    public static final String DEFAULT_NODE_MAPPINGS =
        "node0,localhost,localhost|" +
        "node1,localhost,localhost|" +
        "node2,localhost,localhost|";

    private final DataCollector dataCollector = new DataCollector();
    private final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
    private final DefaultEgressListener defaultEgressListener = new DefaultEgressListener();
    private EgressListener egressListener = defaultEgressListener;
    private ControlledEgressListener controlledEgressListener;

    private final TestNode[] nodes;
    private final String staticClusterMembers;
    private final String staticClusterMemberEndpoints;
    private final String[] clusterMembersEndpoints;
    private final String[] senderWildcardPortRanges;
    private final String[] receiverWildcardPortRanges;
    private final String clusterConsensusEndpoints;
    private final int staticMemberCount;
    private final int appointedLeaderId;
    private final int backupNodeIndex;
    private final IntFunction<TestNode.TestService[]> serviceSupplier;
    private final boolean useResponseChannels;

    private String logChannel;
    private String ingressChannel;
    private String egressChannel;
    private AuthorisationServiceSupplier authorisationServiceSupplier;
    private AuthenticatorSupplier authenticationSupplier;
    private TimerServiceSupplier timerServiceSupplier;
    private TestMediaDriver clientMediaDriver;
    private AeronCluster client;
    private TestBackupNode backupNode;
    private int archiveSegmentFileLength;
    private IntHashSet byHostInvalidInitialResolutions;
    private IntHashSet byMemberInvalidInitialResolutions;
    private boolean acceptStandbySnapshots;
    private File markFileBaseDir;
    private String clusterBaseDir;
    private ClusterBackup.Configuration.ReplayStart replayStart;

    private TestCluster(
        final int staticMemberCount,
        final int appointedLeaderId,
        final IntHashSet byHostInvalidInitialResolutions,
        final IntFunction<TestNode.TestService[]> serviceSupplier,
        final boolean useResponseChannels)
    {
        this.serviceSupplier = requireNonNull(serviceSupplier);
        if ((staticMemberCount + 1) >= 10)
        {
            throw new IllegalArgumentException("max members exceeded: max=9 count=" + staticMemberCount);
        }

        this.nodes = new TestNode[staticMemberCount + 1];
        this.backupNodeIndex = staticMemberCount;
        this.useResponseChannels = useResponseChannels;
        this.staticClusterMembers = clusterMembers(0, staticMemberCount, this.useResponseChannels);
        this.staticClusterMemberEndpoints = ingressEndpoints(0, staticMemberCount);
        this.clusterMembersEndpoints = clusterMembersEndpoints(0, staticMemberCount);
        this.senderWildcardPortRanges = senderWildcardPortRanges(0, staticMemberCount);
        this.receiverWildcardPortRanges = receiverWildcardPortRanges(0, staticMemberCount);
        this.clusterConsensusEndpoints = clusterConsensusEndpoints(0, 0, staticMemberCount);
        this.staticMemberCount = staticMemberCount;
        this.appointedLeaderId = appointedLeaderId;
        this.byHostInvalidInitialResolutions = byHostInvalidInitialResolutions;

    }

    public static void awaitElectionClosed(final TestNode follower)
    {
        awaitElectionState(follower, ElectionState.CLOSED);
    }

    public static void awaitElectionState(final TestNode node, final ElectionState electionState)
    {
        final Supplier<String> msg = () -> "index=" + node.index() + " role=" + node.role() + " electionState=" +
            node.electionState() + " expected=" + electionState;
        while (node.electionState() != electionState)
        {
            await(10, msg);
        }
    }

    private static void await(final int delayMs)
    {
        Tests.sleep(delayMs);
        ClusterTests.failOnClusterError();
    }

    private static void await(final int delayMs, final Supplier<String> message)
    {
        Tests.sleep(delayMs, message);
        ClusterTests.failOnClusterError();
    }

    public static ClusterMembership awaitMembershipSize(final TestNode node, final int size)
    {
        while (true)
        {
            final ClusterMembership clusterMembership = node.clusterMembership();
            if (clusterMembership.activeMembers.size() == size)
            {
                return clusterMembership;
            }
            await(10);
        }
    }

    public static void awaitActiveMember(final TestNode node)
    {
        while (true)
        {
            final ClusterMembership clusterMembership = node.clusterMembership();
            if (clusterMembership.activeMembers.stream().anyMatch((cm) -> cm.id() == node.index()))
            {
                return;
            }
            await(10);
        }
    }

    public void close()
    {
        final boolean isInterrupted = Thread.interrupted();
        try
        {
            CloseHelper.closeAll(
                client,
                clientMediaDriver,
                null != backupNode ? () -> backupNode.close() : null,
                () -> CloseHelper.closeAll(Stream.of(nodes).filter(Objects::nonNull).collect(toList())));
        }
        finally
        {
            if (isInterrupted)
            {
                Thread.currentThread().interrupt();
            }
        }

        ClusterTests.failOnClusterError();
    }

    public TestNode startStaticNode(final int index, final boolean cleanStart)
    {
        return startStaticNode(index, cleanStart, serviceSupplier);
    }

    public TestNode startStaticNode(
        final int index, final boolean cleanStart, final IntFunction<TestNode.TestService[]> serviceSupplier)
    {
        final String baseDirName = clusterBaseDir + "-" + index;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + index + "-driver";
        final File markFileDir = null != markFileBaseDir ? new File(markFileBaseDir, "mark-" + index) : null;
        final TestNode.Context context = new TestNode.Context(serviceSupplier.apply(index), nodeNameMappings());

        context.aeronArchiveContext
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel(archiveControlRequestChannel(index))
            .controlResponseChannel(archiveControlResponseChannel(index))
            .controlResponseStreamId(3000 + index)
            .aeronDirectoryName(aeronDirName);

        context.mediaDriverContext
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .senderWildcardPortRange(senderWildcardPortRanges[index])
            .receiverWildcardPortRange(receiverWildcardPortRanges[index])
            .dirDeleteOnShutdown(false)
            .dirDeleteOnStart(true)
            .enableExperimentalFeatures(useResponseChannels);

        context.archiveContext
            .archiveId(index)
            .catalogCapacity(CATALOG_CAPACITY)
            .archiveDir(new File(baseDirName, "archive"))
            .controlChannel(context.aeronArchiveContext.controlRequestChannel())
            .controlStreamId(context.aeronArchiveContext.controlRequestStreamId())
            .localControlChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL + "?alias=archiveId-" + index)
            .localControlStreamId(context.aeronArchiveContext.controlRequestStreamId())
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(cleanStart)
            .segmentFileLength(archiveSegmentFileLength)
            .replicationChannel(archiveReplicationChannel(index));

        context.consensusModuleContext
            .clusterMemberId(index)
            .clusterMembers(staticClusterMembers)
            .startupCanvassTimeoutNs(STARTUP_CANVASS_TIMEOUT_NS)
            .leaderHeartbeatTimeoutNs(LEADER_HEARTBEAT_TIMEOUT_NS)
            .appointedLeaderId(appointedLeaderId)
            .clusterDir(new File(baseDirName, "consensus-module"))
            .ingressChannel(ingressChannel)
            .logChannel(logChannel)
            .replicationChannel(clusterReplicationChannel(0, index))
            .archiveContext(context.aeronArchiveContext.clone()
                .controlRequestChannel(context.archiveContext.localControlChannel())
                .controlRequestStreamId(context.archiveContext.localControlStreamId())
                .controlResponseChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL))
            .sessionTimeoutNs(TimeUnit.SECONDS.toNanos(10))
            .totalSnapshotDurationThresholdNs(TimeUnit.MILLISECONDS.toNanos(100))
            .authenticatorSupplier(authenticationSupplier)
            .authorisationServiceSupplier(authorisationServiceSupplier)
            .timerServiceSupplier(timerServiceSupplier)
            .acceptStandbySnapshots(acceptStandbySnapshots)
            .markFileDir(markFileDir)
            .deleteDirOnStart(cleanStart);

        nodes[index] = new TestNode(context, dataCollector);

        return nodes[index];
    }

    public TestBackupNode startClusterBackupNode(final boolean cleanStart)
    {
        return startClusterBackupNode(cleanStart, new NullCredentialsSupplier());
    }

    public TestBackupNode startClusterBackupNode(final boolean cleanStart, final ClusterBackup.SourceType sourceType)
    {
        return startClusterBackupNode(cleanStart, new NullCredentialsSupplier(), sourceType);
    }

    public TestBackupNode startClusterBackupNode(
        final boolean cleanStart,
        final CredentialsSupplier credentialsSupplier)
    {
        return startClusterBackupNode(cleanStart, credentialsSupplier, ClusterBackup.SourceType.FOLLOWER);
    }

    public TestBackupNode startClusterBackupNode(
        final boolean cleanStart,
        final CredentialsSupplier credentialsSupplier,
        final ClusterBackup.SourceType sourceType)
    {
        return startClusterBackupNode(cleanStart, credentialsSupplier, sourceType, 0);
    }

    public TestBackupNode startClusterBackupNode(
        final boolean cleanStart,
        final CredentialsSupplier credentialsSupplier,
        final ClusterBackup.SourceType sourceType,
        final int catchupEndpointPort)
    {
        final int index = staticMemberCount;
        final String baseDirName = clusterBaseDir + "-" + index;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + index + "-driver";
        final File markFileDir = null != markFileBaseDir ? new File(markFileBaseDir, "mark-" + index) : null;
        final TestBackupNode.Context context = new TestBackupNode.Context();

        context.mediaDriverContext
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .errorHandler(errorHandler(index))
            .senderWildcardPortRange(senderWildcardPortRanges[index])
            .receiverWildcardPortRange(receiverWildcardPortRanges[index])
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(false)
            .nameResolver(new RedirectingNameResolver(nodeNameMappings(index)))
            .enableExperimentalFeatures(useResponseChannels);

        context.archiveContext
            .archiveId(index)
            .catalogCapacity(CATALOG_CAPACITY)
            .aeronDirectoryName(aeronDirName)
            .archiveDir(new File(baseDirName, "archive"))
            .controlChannel(archiveControlRequestChannel(index) + "|alias=backup-control")
            .controlStreamId(-2734238)
            .localControlChannel("aeron:ipc?alias=backup-local-control")
            .localControlStreamId(8080808 + index)
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(cleanStart)
            .segmentFileLength(archiveSegmentFileLength)
            .replicationChannel(archiveReplicationChannel(index));

        final ChannelUri consensusChannelUri = ChannelUri.parse(context.clusterBackupContext.consensusChannel());
        final String backupStatusEndpoint = clusterBackupStatusEndpoint(0, index);
        consensusChannelUri.put(CommonContext.ENDPOINT_PARAM_NAME, backupStatusEndpoint);

        context.clusterBackupContext
            .clusterConsensusEndpoints(clusterConsensusEndpoints)
            .consensusChannel(consensusChannelUri.toString())
            .clusterBackupCoolDownIntervalNs(TimeUnit.SECONDS.toNanos(1))
            .catchupEndpoint(hostname(index) + ":" + catchupEndpointPort)
            .archiveContext(new AeronArchive.Context()
            .aeronDirectoryName(aeronDirName)
            .controlRequestChannel(context.archiveContext.localControlChannel())
            .controlRequestStreamId(context.archiveContext.localControlStreamId())
            .controlResponseChannel("aeron:ipc?alias=backup-archive-local-resp")
            .controlResponseStreamId(9090909 + index))
            .clusterArchiveContext(new AeronArchive.Context()
            .aeronDirectoryName(aeronDirName)
            .controlRequestChannel(context.archiveContext.controlChannel())
            .controlResponseChannel(archiveControlResponseChannel(index)))
            .clusterDir(new File(baseDirName, "cluster-backup"))
            .credentialsSupplier(credentialsSupplier)
            .sourceType(sourceType)
            .deleteDirOnStart(cleanStart)
            .markFileDir(markFileDir)
            .initialReplayStart(replayStart)
            .eventsListener(new BackupListener());

        backupNode = new TestBackupNode(index, context, dataCollector);

        return backupNode;
    }

    public TestNode startStaticNodeFromBackup()
    {
        return startStaticNodeFromBackup(serviceSupplier);
    }

    public TestNode startStaticNodeFromBackup(final IntFunction<TestNode.TestService[]> serviceSupplier)
    {
        final String baseDirName = clusterBaseDir + "-" + backupNodeIndex;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + backupNodeIndex + "-driver";
        final File markFileDir = null != markFileBaseDir ? new File(markFileBaseDir, "mark-" + backupNodeIndex) : null;
        final TestNode.Context context = new TestNode.Context(
            serviceSupplier.apply(backupNodeIndex),
            nodeNameMappings());

        if (null == backupNode || !backupNode.isClosed())
        {
            throw new IllegalStateException("backup node must be closed before starting from backup");
        }

        context.aeronArchiveContext
            .controlRequestChannel(archiveControlRequestChannel(backupNodeIndex))
            .controlResponseChannel(archiveControlResponseChannel(backupNodeIndex))
            .aeronDirectoryName(aeronDirName);

        context.mediaDriverContext
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .senderWildcardPortRange(senderWildcardPortRanges[backupNodeIndex])
            .receiverWildcardPortRange(receiverWildcardPortRanges[backupNodeIndex])
            .dirDeleteOnStart(true)
            .enableExperimentalFeatures(useResponseChannels);

        context.archiveContext
            .catalogCapacity(CATALOG_CAPACITY)
            .archiveDir(new File(baseDirName, "archive"))
            .controlChannel(context.aeronArchiveContext.controlRequestChannel())
            .localControlChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(false)
            .replicationChannel(archiveReplicationChannel(backupNodeIndex));

        context.consensusModuleContext
            .clusterMemberId(backupNodeIndex)
            .clusterMembers(singleNodeClusterMember(0, backupNodeIndex))
            .appointedLeaderId(backupNodeIndex)
            .clusterDir(new File(baseDirName, "cluster-backup"))
            .ingressChannel(ingressChannel)
            .logChannel(logChannel)
            .replicationChannel(clusterReplicationChannel(0, backupNodeIndex))
            .archiveContext(context.aeronArchiveContext.clone()
                .controlRequestChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
                .controlResponseChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL))
            .sessionTimeoutNs(TimeUnit.SECONDS.toNanos(10))
            .authorisationServiceSupplier(authorisationServiceSupplier)
            .timerServiceSupplier(timerServiceSupplier)
            .acceptStandbySnapshots(acceptStandbySnapshots)
            .markFileDir(markFileDir)
            .deleteDirOnStart(false);

        backupNode = null;
        nodes[backupNodeIndex] = new TestNode(context, dataCollector);

        return nodes[backupNodeIndex];
    }

    public void stopNode(final TestNode testNode)
    {
        testNode.close();
    }

    public void stopAllNodes()
    {
        CloseHelper.close(backupNode);
        CloseHelper.closeAll(nodes);
    }

    public void stopClient()
    {
        CloseHelper.closeAll(client, clientMediaDriver);
    }

    public void restartAllNodes(final boolean cleanStart)
    {
        for (int i = 0; i < staticMemberCount; i++)
        {
            startStaticNode(i, cleanStart);
        }
    }

    public void shouldErrorOnClientClose(final boolean shouldErrorOnClose)
    {
        defaultEgressListener.shouldErrorOnClientClose = shouldErrorOnClose;
    }

    public void logChannel(final String logChannel)
    {
        this.logChannel = logChannel;
    }

    public void ingressChannel(final String ingressChannel)
    {
        this.ingressChannel = ingressChannel;
    }

    public void egressChannel(final String egressChannel)
    {
        this.egressChannel = egressChannel;
    }

    public void egressListener(final EgressListener egressListener)
    {
        this.egressListener = egressListener;
    }

    public void controlledEgressListener(final ControlledEgressListener controlledEgressListener)
    {
        this.controlledEgressListener = controlledEgressListener;
    }

    public void authorisationServiceSupplier(final AuthorisationServiceSupplier authorisationServiceSupplier)
    {
        this.authorisationServiceSupplier = authorisationServiceSupplier;
    }

    public void timerServiceSupplier(final TimerServiceSupplier timerServiceSupplier)
    {
        this.timerServiceSupplier = timerServiceSupplier;
    }

    public void authenticationSupplier(final AuthenticatorSupplier authenticationSupplier)
    {
        this.authenticationSupplier = authenticationSupplier;
    }

    private void segmentFileLength(final int archiveSegmentFileLength)
    {
        this.archiveSegmentFileLength = archiveSegmentFileLength;
    }

    public AeronCluster client()
    {
        return client;
    }

    public ExpandableArrayBuffer msgBuffer()
    {
        return msgBuffer;
    }

    public AeronCluster reconnectClient()
    {
        if (null == client)
        {
            throw new IllegalStateException("Aeron client not previously connected");
        }

        return connectClient();
    }

    public AeronCluster connectClient()
    {
        return connectClient(new AeronCluster.Context().ingressChannel(ingressChannel).egressChannel(egressChannel));
    }

    public AeronCluster connectClient(final CredentialsSupplier credentialsSupplier)
    {
        return connectClient(new AeronCluster.Context()
            .credentialsSupplier(credentialsSupplier)
            .ingressChannel(ingressChannel)
            .egressChannel(egressChannel));
    }

    public AeronCluster connectClient(final AeronCluster.Context clientCtx)
    {
        final String aeronDirName = CommonContext.getAeronDirectoryName();

        if (null == clientMediaDriver)
        {
            dataCollector.add(Paths.get(aeronDirName));

            final MediaDriver.Context ctx = new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(false)
                .aeronDirectoryName(aeronDirName)
                .nameResolver(new RedirectingNameResolver(nodeNameMappings()))
                .senderWildcardPortRange("20700 20709")
                .receiverWildcardPortRange("20710 20719")
                .enableExperimentalFeatures(useResponseChannels);

            clientMediaDriver = TestMediaDriver.launch(ctx, clientDriverOutputConsumer(dataCollector));
        }

        clientCtx
            .aeronDirectoryName(aeronDirName)
            .isIngressExclusive(true)
            .egressListener(egressListener)
            .controlledEgressListener(controlledEgressListener)
            .ingressEndpoints(staticClusterMemberEndpoints);

        try
        {
            CloseHelper.close(client);
            client = AeronCluster.connect(clientCtx.clone());
        }
        catch (final TimeoutException ex)
        {
            System.out.println("Warning: " + ex);

            CloseHelper.close(client);
            client = AeronCluster.connect(clientCtx);
        }

        return client;
    }

    public AeronCluster asyncConnectClient()
    {
        final AeronCluster.Context clientCtx = new AeronCluster.Context()
            .ingressChannel(ingressChannel)
            .egressChannel(egressChannel);

        final String aeronDirName = CommonContext.getAeronDirectoryName();

        if (null == clientMediaDriver)
        {
            dataCollector.add(Paths.get(aeronDirName));

            final MediaDriver.Context ctx = new MediaDriver.Context()
                .threadingMode(ThreadingMode.INVOKER)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(false)
                .aeronDirectoryName(aeronDirName)
                .nameResolver(new RedirectingNameResolver(nodeNameMappings()))
                .enableExperimentalFeatures(useResponseChannels);

            clientMediaDriver = TestMediaDriver.launch(ctx, clientDriverOutputConsumer(dataCollector));
        }

        final Aeron aeron = Aeron.connect(new Aeron.Context()
            .useConductorAgentInvoker(true)
            .aeronDirectoryName(aeronDirName));

        clientCtx
            .aeron(aeron)
            .ownsAeronClient(true)
            .isIngressExclusive(true)
            .egressListener(egressListener)
            .controlledEgressListener(controlledEgressListener)
            .ingressEndpoints(staticClusterMemberEndpoints);

        final AgentInvoker conductorAgentInvoker = aeron.conductorAgentInvoker();
        try
        {
            CloseHelper.close(client);
            final AeronCluster.AsyncConnect asyncConnect = AeronCluster.asyncConnect(clientCtx.clone());
            while (null == (client = asyncConnect.poll()))
            {
                invokeSharedAgentInvoker();
                if (null != conductorAgentInvoker)
                {
                    conductorAgentInvoker.invoke();
                }

                Tests.yield();
            }
        }
        catch (final TimeoutException ex)
        {
            System.out.println("Warning: " + ex);

            CloseHelper.close(client);
            final AeronCluster.AsyncConnect asyncConnect = AeronCluster.asyncConnect(clientCtx.clone());
            while (null == (client = asyncConnect.poll()))
            {
                invokeSharedAgentInvoker();
                if (null != conductorAgentInvoker)
                {
                    conductorAgentInvoker.invoke();
                }

                Tests.yield();
            }
        }

        return client;
    }

    public AeronCluster connectIpcClient(final AeronCluster.Context clientCtx, final String aeronDirName)
    {
        clientCtx
            .aeronDirectoryName(aeronDirName)
            .isIngressExclusive(true)
            .ingressChannel("aeron:ipc")
            .egressChannel("aeron:ipc")
            .egressListener(egressListener)
            .controlledEgressListener(controlledEgressListener)
            .ingressEndpoints(null);

        try
        {
            CloseHelper.close(client);
            client = AeronCluster.connect(clientCtx.clone());
        }
        catch (final TimeoutException ex)
        {
            CloseHelper.close(client);
            client = AeronCluster.connect(clientCtx);
        }

        return client;
    }

    public void closeClient()
    {
        CloseHelper.close(client);
    }

    public void sendMessages(final int messageCount)
    {
        for (int i = 0; i < messageCount; i++)
        {
            msgBuffer.putInt(0, i);
            try
            {
                pollUntilMessageSent(BitUtil.SIZE_OF_INT);
            }
            catch (final Exception ex)
            {
                final String msg = "failed to send message " + i + " of " + messageCount + " cause=" + ex.getMessage();
                throw new ClusterException(msg, ex);
            }
        }
    }

    public void sendLargeMessages(final int messageCount)
    {
        final int messageLength = msgBuffer.putStringWithoutLengthAscii(0, LARGE_MSG);

        for (int i = 0; i < messageCount; i++)
        {
            try
            {
                pollUntilMessageSent(messageLength);
            }
            catch (final Exception ex)
            {
                final String msg = "failed to send message " + i + " of " + messageCount + " cause=" + ex.getMessage();
                throw new ClusterException(msg, ex);
            }
        }
    }

    public void sendMessageToSlowDownService(final int index, final long durationNs)
    {
        final String pause = PAUSE + "|" + index + "|" + durationNs;
        final int messageLength = msgBuffer.putStringWithoutLengthAscii(0, pause);

        try
        {
            pollUntilMessageSent(messageLength);
        }
        catch (final Exception ex)
        {
            final String msg = "failed to send message cause=" + ex.getMessage();
            throw new ClusterException(msg, ex);
        }
    }

    public void sendUnexpectedMessages(final int messageCount)
    {
        final int messageLength = msgBuffer.putStringWithoutLengthAscii(0, ClusterTests.UNEXPECTED_MSG);

        for (int i = 0; i < messageCount; i++)
        {
            try
            {
                pollUntilMessageSent(messageLength);
            }
            catch (final Exception ex)
            {
                final String msg = "failed to send message " + i + " of " + messageCount + " cause=" + ex.getMessage();
                throw new ClusterException(msg, ex);
            }
        }
    }

    public void sendErrorGeneratingMessages(final int messageCount)
    {
        final int messageLength = msgBuffer.putStringWithoutLengthAscii(0, ClusterTests.ERROR_MSG);

        for (int i = 0; i < messageCount; i++)
        {
            try
            {
                pollUntilMessageSent(messageLength);
            }
            catch (final Exception ex)
            {
                final String msg = "failed to send message " + i + " of " + messageCount + " cause=" + ex.getMessage();
                throw new ClusterException(msg, ex);
            }
        }
    }

    public void sendTerminateMessage()
    {
        final int messageLength = msgBuffer.putStringWithoutLengthAscii(0, ClusterTests.TERMINATE_MSG);

        try
        {
            pollUntilMessageSent(messageLength);
        }
        catch (final Exception ex)
        {
            throw new ClusterException("failed to send message cause=" + ex.getMessage(), ex);
        }
    }

    public void sendAndAwaitMessages(final int messageCount)
    {
        sendAndAwaitMessages(messageCount, messageCount);
    }

    public void sendAndAwaitMessages(final int sendCount, final int awaitCount)
    {
        sendMessages(sendCount);
        awaitResponseMessageCount(awaitCount);
        awaitServicesMessageCount(awaitCount);
    }

    public void pollUntilMessageSent(final int messageLength)
    {
        while (true)
        {
            requireNonNull(client, "Client is not connected").pollEgress();

            final long position = client.offer(msgBuffer, 0, messageLength);
            if (position > 0)
            {
                return;
            }

            if (Publication.ADMIN_ACTION == position)
            {
                continue;
            }

            if (Publication.MAX_POSITION_EXCEEDED == position)
            {
                throw new ClusterException("max position exceeded");
            }

            await(1);
        }
    }

    public void awaitResponseMessageCount(final int messageCount)
    {
        clientKeepAlive.init();
        final Supplier<String> msg =
            () -> "expected=" + messageCount + " responseCount=" + defaultEgressListener.responseCount();

        while (defaultEgressListener.responseCount() < messageCount)
        {
            if (0 == pollClient())
            {
                Tests.yieldingIdle(msg);
            }

            try
            {
                clientKeepAlive.run();
            }
            catch (final ClusterException ex)
            {
                final String message = "count=" + defaultEgressListener.responseCount() + " awaiting=" + messageCount +
                    " cause=" + ex.getMessage();
                throw new RuntimeException(message, ex);
            }
        }
    }

    public void awaitNewLeadershipEvent(final int count)
    {
        while (defaultEgressListener.newLeaderEvent() < count || !client.ingressPublication().isConnected())
        {
            await(1);
            pollClient();
        }
    }

    public void awaitLossOfLeadership(final TestNode.TestService leaderService)
    {
        if (null != client)
        {
            clientKeepAlive.init();
        }

        while (leaderService.roleChangedTo() != FOLLOWER)
        {
            Tests.sleep(100);
            if (null != client)
            {
                clientKeepAlive.run();
            }
        }
    }

    private int pollClient()
    {
        invokeSharedAgentInvoker();

        final AgentInvoker conductorAgentInvoker = client.context().aeron().conductorAgentInvoker();
        if (null != conductorAgentInvoker)
        {
            conductorAgentInvoker.invoke();
        }

        return client.pollEgress();
    }

    private void invokeSharedAgentInvoker()
    {
        if (clientMediaDriver instanceof JavaTestMediaDriver &&
            ThreadingMode.INVOKER == clientMediaDriver.context().threadingMode())
        {
            clientMediaDriver.sharedAgentInvoker().invoke();
        }
    }

    public void awaitCommitPosition(final TestNode node, final long logPosition)
    {
        while (node.commitPosition() < logPosition)
        {
            Tests.yield();
        }
    }

    public void awaitActiveSessionCount(final TestNode node, final int count)
    {
        final Supplier<String> message =
            () -> "node " + node + " fail to reach active session count, expected=" + count + ", current=" +
            node.service().activeSessionCount();

        while (node.service().activeSessionCount() != count)
        {
            await(1, message);
        }
    }

    public void awaitActiveSessionCount(final int count)
    {
        for (final TestNode node : nodes)
        {
            if (null != node && !node.isClosed())
            {
                awaitActiveSessionCount(node, count);
            }
        }
    }

    public TestNode findLeader(final int skipIndex)
    {
        for (int i = 0; i < nodes.length; i++)
        {
            final TestNode node = nodes[i];
            if (i == skipIndex || null == node || node.isClosed())
            {
                continue;
            }

            if (node.isLeader() && ElectionState.CLOSED == node.electionState())
            {
                return node;
            }
        }

        return null;
    }

    public TestNode findLeader()
    {
        return findLeader(NULL_VALUE);
    }

    public TestNode awaitLeader(final int skipIndex)
    {
        final Supplier<String> message = () -> Arrays.stream(nodes)
            .map((node) -> null != node ? node.index() + " " + node.role() + " " + node.electionState() : "null")
            .collect(Collectors.joining(", "));

        TestNode leaderNode;
        while (null == (leaderNode = findLeader(skipIndex)))
        {
            await(10, message);
        }

        return leaderNode;
    }

    public TestNode awaitLeader()
    {
        return awaitLeader(NULL_VALUE);
    }

    public TestNode awaitLeaderAndClosedElection(final int nodeId)
    {
        final TestNode leader = awaitLeader(nodeId);

        for (final TestNode node : nodes)
        {
            if (null != node)
            {
                awaitElectionClosed(node);
            }
        }

        return leader;
    }

    public TestNode awaitLeaderAndClosedElection()
    {
        return awaitLeaderAndClosedElection(NULL_VALUE);
    }

    public List<TestNode> followers()
    {
        return followers(0);
    }

    public ArrayList<TestNode> followers(final int expectedMinimumFollowerCount)
    {
        final ArrayList<TestNode> followers = new ArrayList<>();
        final EnumMap<Cluster.Role, ArrayList<TestNode>> nonFollowers = new EnumMap<>(Cluster.Role.class);

        for (final TestNode node : nodes)
        {
            if (null != node && !node.isClosed())
            {
                while (ElectionState.CLOSED != node.electionState())
                {
                    Tests.yield();
                }

                final Cluster.Role role = node.role();
                if (role == Cluster.Role.FOLLOWER)
                {
                    followers.add(node);
                }
                else
                {
                    nonFollowers.computeIfAbsent(role, (r) -> new ArrayList<>()).add(node);
                }
            }
        }

        if (followers.size() < expectedMinimumFollowerCount)
        {
            throw new RuntimeException(
                "expectedMinimumFollowerCount=" + expectedMinimumFollowerCount +
                " < followers.size=" + followers.size() +
                " nonFollowers=" + nonFollowers);
        }

        return followers;
    }

    public void awaitBackupState(final ClusterBackup.State targetState)
    {
        if (null == backupNode)
        {
            throw new IllegalStateException("no backup node present");
        }

        final Supplier<String> message =
            () -> "expectedState=" + targetState + " actualState=" + backupNode.backupState();
        while (true)
        {
            final ClusterBackup.State state = backupNode.backupState();
            if (targetState == state)
            {
                break;
            }

            if (ClusterBackup.State.CLOSED == state)
            {
                throw new IllegalStateException("backup is closed");
            }

            Tests.sleep(10, message);
        }
    }

    public void awaitBackupLiveLogPosition(final long position)
    {
        if (null == backupNode)
        {
            throw new IllegalStateException("no backup node present");
        }

        while (true)
        {
            final long livePosition = backupNode.liveLogPosition();
            if (livePosition >= position)
            {
                break;
            }

            if (NULL_POSITION == livePosition)
            {
                throw new ClusterException("backup live log position is closed");
            }

            Tests.sleep(10, "awaiting position=%d livePosition=%d", position, livePosition);
        }
    }

    public void awaitBackupSnapshotRetrievedCount(final long snapshotCount)
    {
        if (null == backupNode)
        {
            throw new IllegalStateException("no backup node present");
        }

        @SuppressWarnings("indentation")
        final Supplier<String> msg =
            () -> "Snapshot retrieve count not found expected=" + snapshotCount +
                " actual=" + backupNode.snapshotRetrieveCount();

        while (backupNode.snapshotRetrieveCount() < snapshotCount)
        {
            Tests.yieldingIdle(msg);
        }
    }

    public TestNode node(final int index)
    {
        return nodes[index];
    }

    public void takeSnapshot(final TestNode leaderNode)
    {
        final AtomicCounter controlToggle = getClusterControlToggle(leaderNode);
        assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));
    }

    public void takeStandbySnapshot(final TestNode leaderNode)
    {
        final AtomicCounter controlToggle = getClusterControlToggle(leaderNode);
        assertTrue(ClusterControl.ToggleState.STANDBY_SNAPSHOT.toggle(controlToggle));
    }

    public void shutdownCluster(final TestNode leaderNode)
    {
        final AtomicCounter controlToggle = getClusterControlToggle(leaderNode);
        assertTrue(ClusterControl.ToggleState.SHUTDOWN.toggle(controlToggle));
    }

    public void abortCluster(final TestNode leaderNode)
    {
        final AtomicCounter controlToggle = getClusterControlToggle(leaderNode);
        assertTrue(ClusterControl.ToggleState.ABORT.toggle(controlToggle));
    }

    public void suspendCluster(final TestNode leaderNode)
    {
        final AtomicCounter controlToggle = getClusterControlToggle(leaderNode);
        assertTrue(ClusterControl.ToggleState.SUSPEND.toggle(controlToggle));
    }

    public void awaitSnapshotCount(final long value)
    {
        for (final TestNode node : nodes)
        {
            if (null != node && !node.isClosed())
            {
                awaitSnapshotCount(node, value);
            }
        }
    }

    public void awaitSnapshotCount(final TestNode node, final long value)
    {
        clientKeepAlive.init();
        awaitCounter(node, value, node.consensusModule().context().snapshotCounter(), clientKeepAlive);
    }

    public long getSnapshotCount(final TestNode node)
    {
        final Counter snapshotCounter = node.consensusModule().context().snapshotCounter();

        if (snapshotCounter.isClosed())
        {
            throw new IllegalStateException("counter is unexpectedly closed");
        }

        return snapshotCounter.get();
    }

    public void awaitStandbySnapshotCount(final long value)
    {
        for (final TestNode node : nodes)
        {
            if (null != node && !node.isClosed())
            {
                awaitStandbySnapshotCount(node, value);
            }
        }
    }

    public void awaitStandbySnapshotCount(final TestNode node, final long value)
    {
        clientKeepAlive.init();
        awaitCounter(node,
            value,
            requireNonNull(
            node.consensusModule().context().standbySnapshotCounter(), "node not configured for standby snapshots"),
            clientKeepAlive);
    }

    public long getStandbySnapshotCount(final TestNode node)
    {
        final Counter snapshotCounter = requireNonNull(
            node.consensusModule().context().standbySnapshotCounter(), "node not configured for standby snapshots");

        if (snapshotCounter.isClosed())
        {
            throw new IllegalStateException("counter is unexpectedly closed");
        }

        return snapshotCounter.get();
    }

    private static void awaitCounter(
        final TestNode node, final long value, final Counter snapshotCounter, final Runnable keepAlive)
    {
        final Supplier<String> msg = () -> "node=" + node.index() +
            " role=" + node.role() +
            " expected=" + value +
            " snapshotCount=" + snapshotCounter.get();

        while (true)
        {
            if (snapshotCounter.isClosed())
            {
                throw new IllegalStateException("counter is unexpectedly closed");
            }

            if (snapshotCounter.get() >= value)
            {
                break;
            }

            Tests.yieldingIdle(msg);

            keepAlive.run();
        }
    }

    public long logPosition()
    {
        final TestNode leader = findLeader();
        return leader.consensusModule().context().commitPositionCounter().get();
    }

    public void awaitNodeTermination(final TestNode node)
    {
        final Supplier<String> msg =
            () -> "Failed to see node=" + node.index() + " terminate, " +
            "hasMemberTerminated=" + node.hasMemberTerminated() +
            ", hasServiceTerminated=" + node.hasServiceTerminated();

        while (!node.hasMemberTerminated() || !node.hasServiceTerminated())
        {
            Tests.yieldingIdle(msg);
        }
    }

    public void awaitNodeTerminations()
    {
        for (final TestNode node : nodes)
        {
            if (null != node)
            {
                awaitNodeTermination(node);
            }
        }
    }

    public void awaitServicesMessageCount(final int messageCount)
    {
        for (final TestNode node : nodes)
        {
            if (null != node && !node.isClosed())
            {
                awaitServiceMessageCount(node, messageCount);
            }
        }
    }

    public void awaitTimerEventCount(final int expectedTimerEventsCount)
    {
        for (final TestNode node : nodes)
        {
            if (null != node && !node.isClosed())
            {
                awaitTimerEventCount(node, expectedTimerEventsCount);
            }
        }
    }

    public void terminationsExpected(final boolean isExpected)
    {
        for (final TestNode node : nodes)
        {
            if (null != node)
            {
                node.isTerminationExpected(isExpected);
            }
        }
    }

    public void awaitServiceMessageCount(final TestNode node, final int messageCount)
    {
        final TestNode.TestService[] services = node.services();
        awaitServiceMessageCount(node, services, messageCount);
    }

    public void awaitServiceMessagePredicate(final TestNode node, final IntPredicate countPredicate)
    {
        final TestNode.TestService service = node.service();
        awaitServiceMessagePredicate(node, service, countPredicate);
    }

    private final KeepAlive clientKeepAlive = new KeepAlive();

    public void awaitServiceMessageCount(
        final TestNode node, final TestNode.TestService service, final int messageCount)
    {
        clientKeepAlive.init();
        service.awaitServiceMessageCount(messageCount, clientKeepAlive, node);
    }

    public void awaitServiceMessageCount(
        final TestNode node, final TestNode.TestService[] services, final int messageCount)
    {
        clientKeepAlive.init();
        for (final TestNode.TestService service : services)
        {
            service.awaitServiceMessageCount(messageCount, clientKeepAlive, node);
        }
    }

    public void awaitServiceMessagePredicate(
        final TestNode node, final TestNode.TestService service, final IntPredicate countPredicate)
    {
        service.awaitServiceMessagePredicate(countPredicate, clientKeepAlive, node);
    }

    public void awaitLiveAndSnapshotMessageCount(
        final TestNode node,
        final TestNode.TestService service,
        final IntPredicate liveCountPredicate,
        final IntPredicate snapshotCountPredicate)
    {
        clientKeepAlive.init();
        service.awaitLiveAndSnapshotMessageCount(liveCountPredicate, snapshotCountPredicate, clientKeepAlive, node);
    }

    public void awaitLiveAndSnapshotMessageCount(
        final TestNode node, final IntPredicate liveCountPredicate, final IntPredicate snapshotCountPredicate)
    {
        final TestNode.TestService service = node.service();
        awaitLiveAndSnapshotMessageCount(node, service, liveCountPredicate, snapshotCountPredicate);
    }

    public void awaitTimerEventCount(final TestNode node, final int expectedTimerEventsCount)
    {
        final TestNode.TestService service = node.service();
        awaitTimerEventCount(node, service, expectedTimerEventsCount);
    }

    public void awaitTimerEventCount(
        final TestNode node, final TestNode.TestService service, final int expectedTimerEventsCount)
    {
        clientKeepAlive.init();
        long count;

        while ((count = service.timerCount()) < expectedTimerEventsCount)
        {
            Thread.yield();
            if (Thread.interrupted())
            {
                throw new TimeoutException("await timer events: count=" + count + " awaiting=" +
                    expectedTimerEventsCount + " node=" + node);
            }

            if (service.hasReceivedUnexpectedMessage())
            {
                fail("service received unexpected message");
            }

            clientKeepAlive.run();
        }
    }

    public void awaitNodeState(final TestNode node, final Predicate<TestNode> predicate)
    {
        clientKeepAlive.init();

        while (!predicate.test(node))
        {
            Thread.yield();
            if (Thread.interrupted())
            {
                throw new TimeoutException("timeout while awaiting node state");
            }

            clientKeepAlive.run();
        }
    }

    public void awaitSnapshotsLoaded()
    {
        for (final TestNode node : nodes)
        {
            if (null != node)
            {
                awaitSnapshotLoadedForService(node);
            }
        }
    }

    public void awaitSnapshotLoadedForService(final TestNode node)
    {
        while (!node.allSnapshotsLoaded())
        {
            Tests.yield();
        }
    }

    public void awaitNeutralControlToggle(final TestNode leaderNode)
    {
        final AtomicCounter controlToggle = getClusterControlToggle(leaderNode);
        while (controlToggle.get() != ClusterControl.ToggleState.NEUTRAL.code())
        {
            Tests.yield();
        }
    }

    public AtomicCounter getClusterControlToggle(final TestNode leaderNode)
    {
        final CountersReader counters = leaderNode.countersReader();
        final int clusterId = leaderNode.consensusModule().context().clusterId();
        final AtomicCounter controlToggle = ClusterControl.findControlToggle(counters, clusterId);
        assertNotNull(controlToggle);

        return controlToggle;
    }

    public void awaitNeutralNodeControlToggle()
    {
        for (final TestNode node : nodes)
        {
            if (null != node)
            {
                awaitNeutralNodeControlToggle(node);
            }
        }
    }

    public void awaitNeutralNodeControlToggle(final TestNode node)
    {
        final AtomicCounter controlToggle = getNodeControlToggle(node);
        while (controlToggle.get() != NodeControl.ToggleState.NEUTRAL.code())
        {
            Tests.yield();
        }
    }

    public AtomicCounter getNodeControlToggle(final TestNode node)
    {
        final CountersReader counters = node.countersReader();
        final int clusterId = node.consensusModule().context().clusterId();
        final AtomicCounter controlToggle = NodeControl.findControlToggle(counters, clusterId);
        assertNotNull(controlToggle);

        return controlToggle;
    }

    public static String clusterMembers(final int clusterId, final int memberCount)
    {
        return clusterMembers(clusterId, 0, memberCount, false);
    }

    public static String clusterMembers(final int clusterId, final int memberCount, final boolean useResponseChannels)
    {
        return clusterMembers(clusterId, 0, memberCount, useResponseChannels);
    }

    public static String clusterMembers(
        final int clusterId, final int initialMemberId, final int memberCount, final boolean useResponseChannels)
    {
        final StringBuilder builder = new StringBuilder();

        for (int i = initialMemberId; i < (initialMemberId + memberCount); i++)
        {
            builder
                .append(i).append(',')
                .append(hostname(i)).append(":2").append(clusterId).append("11").append(i).append(',')
                .append(hostname(i)).append(":2").append(clusterId).append("22").append(i).append(',')
                .append(hostname(i)).append(":2").append(clusterId).append("33").append(i).append(',')
                .append(hostname(i)).append(":0,")
                .append(hostname(i)).append(":801").append(i);

            if (useResponseChannels)
            {
                builder.append(',').append(hostname(i)).append(":2").append(clusterId).append("44").append(i);
            }

            builder.append('|');
        }

        builder.setLength(builder.length() - 1);

        return builder.toString();
    }

    public static String singleNodeClusterMember(final int clusterId, final int i)
    {
        final String hostname = hostname(i);

        return i + "," +
            hostname + ":2" + clusterId + "11" + i + ',' +
            hostname + ":2" + clusterId + "22" + i + ',' +
            hostname + ":2" + clusterId + "33" + i + ',' +
            hostname + ":0," +
            hostname + ":801" + i;
    }

    public static String ingressEndpoints(final int clusterId, final int memberCount)
    {
        return ingressEndpoints(clusterId, 0, memberCount);
    }

    public static String ingressEndpoints(final int clusterId, final int initialMemberId, final int memberCount)
    {
        final StringBuilder builder = new StringBuilder();

        for (int i = 0; i < memberCount; i++)
        {
            final int memberId = initialMemberId + i;
            builder.append(memberId)
                .append('=')
                .append(hostname(memberId))
                .append(":2")
                .append(clusterId)
                .append("11")
                .append(memberId)
                .append(',');
        }

        builder.setLength(builder.length() - 1);

        return builder.toString();
    }

    public void purgeLogToLastSnapshot()
    {
        for (final TestNode testNode : nodes)
        {
            purgeLogToLastSnapshot(testNode);
        }
    }

    public void purgeLogToLastSnapshot(final TestNode node)
    {
        if (null == node || node.isClosed())
        {
            return;
        }

        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(10);
        final RecordingLog recordingLog = new RecordingLog(node.consensusModule().context().clusterDir(), false);
        final RecordingLog.Entry latestSnapshot = requireNonNull(
            recordingLog.getLatestSnapshot(ConsensusModule.Configuration.SERVICE_ID));
        final long recordingId = recordingLog.findLastTermRecordingId();
        if (RecordingPos.NULL_RECORDING_ID == recordingId)
        {
            throw new RuntimeException("Unable to find log recording");
        }

        try (Aeron aeron = Aeron.connect(
            new Aeron.Context().aeronDirectoryName(node.mediaDriver().aeronDirectoryName())))
        {
            final MutableBoolean segmentsDeleted = new MutableBoolean(false);
            final RecordingSignalConsumer deleteSignalConsumer =
                (controlSessionId, correlationId, recordingId1, subscriptionId, position, signal) ->
                {
                    if (RecordingSignal.DELETE == signal)
                    {
                        segmentsDeleted.set(true);
                    }
                };

            final AeronArchive.Context aeronArchiveCtx = node
                .consensusModule()
                .context()
                .archiveContext()
                .clone()
                .recordingSignalConsumer(deleteSignalConsumer)
                .aeron(aeron)
                .ownsAeronClient(false);

            try (AeronArchive aeronArchive = AeronArchive.connect(aeronArchiveCtx))
            {
                aeronArchive.listRecording(recordingId, collector.reset());
                final RecordingDescriptor recordingDescriptor = collector.descriptors().get(0);

                final long newStartPosition = AeronArchive.segmentFileBasePosition(
                    recordingDescriptor.startPosition(),
                    latestSnapshot.logPosition,
                    recordingDescriptor.termBufferLength(),
                    recordingDescriptor.segmentFileLength());
                final long segmentsDeleteCount = aeronArchive.purgeSegments(recordingId, newStartPosition);

                while (0 < segmentsDeleteCount && !segmentsDeleted.get())
                {
                    aeronArchive.pollForRecordingSignals();
                }
            }
        }
    }

    static String[] clusterMembersEndpoints(final int clusterId, final int maxMemberCount)
    {
        final String[] clusterMembersEndpoints = new String[maxMemberCount];

        for (int i = 0; i < maxMemberCount; i++)
        {
            clusterMembersEndpoints[i] =
                hostname(i) + ":2" + clusterId + "11" + i + ',' +
                hostname(i) + ":2" + clusterId + "22" + i + ',' +
                hostname(i) + ":2" + clusterId + "33" + i + ',' +
                hostname(i) + ":2" + clusterId + "44" + i + ',' +
                hostname(i) + ":801" + i;
        }

        return clusterMembersEndpoints;
    }

    static String clusterConsensusEndpoints(final int clusterId, final int beginIndex, final int endIndex)
    {
        final StringBuilder builder = new StringBuilder();

        for (int i = beginIndex; i < endIndex; i++)
        {
            builder.append(hostname(i)).append(":2").append(clusterId).append("22").append(i).append(',');
        }

        builder.setLength(builder.length() - 1);

        return builder.toString();
    }

    static String[] senderWildcardPortRanges(final int clusterId, final int maxMemberCount)
    {
        final String[] ranges = new String[maxMemberCount + 1];

        for (int i = 0; i <= maxMemberCount; i++)
        {
            ranges[i] = "2" + clusterId + "5" + i + "0 " + "2" + clusterId + "5" + i + "9";
        }

        return ranges;
    }

    static String[] receiverWildcardPortRanges(final int clusterId, final int maxMemberCount)
    {
        final String[] ranges = new String[maxMemberCount + 1];

        for (int i = 0; i <= maxMemberCount; i++)
        {
            ranges[i] = "2" + clusterId + "6" + i + "0 " + "2" + clusterId + "6" + i + "9";
        }

        return ranges;
    }

    static String hostname(final int memberId)
    {
        return memberId < 3 ? "node" + memberId : "localhost";
    }

    static String clusterBackupStatusEndpoint(final int clusterId, final int maxMemberCount)
    {
        return hostname(maxMemberCount) + ":2" + clusterId + "22" + maxMemberCount;
    }

    static String archiveControlRequestChannel(final int memberId)
    {
        return "aeron:udp?endpoint=" + archiveControlRequestEndpoint(memberId);
    }

    static String archiveControlRequestEndpoint(final int memberId)
    {
        return hostname(memberId) + ":801" + memberId;
    }

    static String archiveControlResponseChannel(final int memberId)
    {
        return "aeron:udp?endpoint=" + archiveControlResponseEndpoint(memberId);
    }

    static String archiveControlResponseEndpoint(final int memberId)
    {
        return hostname(memberId) + ":0";
    }

    static String archiveReplicationChannel(final int memberId)
    {
        return "aeron:udp?endpoint=" + archiveReplicationEndpoint(memberId);
    }

    static String archiveReplicationEndpoint(final int memberId)
    {
        return hostname(memberId) + ":802" + memberId;
    }

    static String clusterReplicationChannel(final int clusterId, final int memberId)
    {
        return "aeron:udp?endpoint=" + clusterReplicationEndpoint(clusterId, memberId) + "|linger=5000000000";
    }

    static String clusterReplicationEndpoint(final int clusterId, final int memberId)
    {
        return hostname(memberId) + ":2" + clusterId + "55" + memberId;
    }

    public void invalidateLatestSnapshot()
    {
        for (final TestNode node : nodes)
        {
            if (null != node)
            {
                try (RecordingLog recordingLog = new RecordingLog(node.consensusModule().context().clusterDir(), false))
                {
                    assertTrue(recordingLog.invalidateLatestSnapshot());
                }
            }
        }
    }

    public void disableNameResolution(final String hostname)
    {
        toggleNameResolution(hostname, RedirectingNameResolver.DISABLE_RESOLUTION);
    }

    public void enableNameResolution(final String hostname)
    {
        toggleNameResolution(hostname, RedirectingNameResolver.USE_INITIAL_RESOLUTION_HOST);
    }

    public void restoreNameResolution(final int nodeId)
    {
        byHostInvalidInitialResolutions.remove(nodeId);
        toggleNameResolution(hostname(nodeId), RedirectingNameResolver.USE_RE_RESOLUTION_HOST);
    }

    public void restoreByMemberNameResolution(final int memberId)
    {
        byMemberInvalidInitialResolutions.remove(memberId);
        final TestMediaDriver mediaDriver = null != backupNode && backupNode.index() == memberId ?
            backupNode.mediaDriver() : node(memberId).mediaDriver();
        final CountersReader counters = mediaDriver.counters();

        for (int i = 0; i < 3; i++)
        {
            RedirectingNameResolver.updateNameResolutionStatus(
                counters, hostname(i), RedirectingNameResolver.USE_RE_RESOLUTION_HOST);
        }
    }

    private void toggleNameResolution(final String hostname, final int disableValue)
    {
        for (final TestNode node : nodes)
        {
            if (null != node)
            {
                final CountersReader counters = node.mediaDriver().counters();
                RedirectingNameResolver.updateNameResolutionStatus(counters, hostname, disableValue);
            }
        }
    }

    private String nodeNameMappings()
    {
        return nodeNameMappings(byHostInvalidInitialResolutions);
    }

    private String nodeNameMappings(final int memberId)
    {
        return nodeNameMappings(byHostInvalidInitialResolutions, byMemberInvalidInitialResolutions, memberId);
    }

    private static String nodeNameMappings(final IntHashSet invalidInitialResolutions)
    {
        return
            "node0," + (invalidInitialResolutions.contains(0) ? "bad.invalid" : "localhost") + ",localhost|" +
            "node1," + (invalidInitialResolutions.contains(1) ? "bad.invalid" : "localhost") + ",localhost|" +
            "node2," + (invalidInitialResolutions.contains(2) ? "bad.invalid" : "localhost") + ",localhost|";
    }

    private static String nodeNameMappings(
        final IntHashSet byHostInvalidInitialResolutions,
        final IntHashSet byMemberInvalidInitialResolutions,
        final int memberId)
    {
        final boolean memberInvalid = byMemberInvalidInitialResolutions.contains(memberId);

        final String host0 = memberInvalid || byHostInvalidInitialResolutions.contains(0) ? "bad.invalid" : "localhost";
        final String host1 = memberInvalid || byHostInvalidInitialResolutions.contains(1) ? "bad.invalid" : "localhost";
        final String host2 = memberInvalid || byHostInvalidInitialResolutions.contains(2) ? "bad.invalid" : "localhost";

        return
            "node0," + host0 + ",localhost|" +
            "node1," + host1 + ",localhost|" +
            "node2," + host2 + ",localhost|";
    }

    public DataCollector dataCollector()
    {
        return dataCollector;
    }

    public void assertRecordingLogsEqual()
    {
        List<RecordingLog.Entry> firstEntries = null;
        for (final TestNode node : nodes)
        {
            if (null == node)
            {
                continue;
            }

            try (RecordingLog recordingLog = new RecordingLog(node.consensusModule().context().clusterDir(), false))
            {
                if (null == firstEntries)
                {
                    firstEntries = recordingLog.entries();
                }
                else
                {
                    final List<RecordingLog.Entry> entries = recordingLog.entries();
                    assertEquals(
                        firstEntries.size(),
                        entries.size(),
                        "length mismatch: \n[0]" + firstEntries + " != " + "\n[" + node.index() + "] " + entries);

                    for (int i = 0; i < firstEntries.size(); i++)
                    {
                        final RecordingLog.Entry a = firstEntries.get(i);
                        final RecordingLog.Entry b = entries.get(i);

                        final ReflectionEquals matcher = new ReflectionEquals(a, "timestamp");
                        assertTrue(matcher.matches(b), "Mismatch (" + i + "): " + a + " != " + b);
                    }
                }
            }
        }
    }

    public void validateRecordingLogWithReplay(final int nodeId)
    {
        final TestNode node = node(nodeId);
        final ConsensusModule.Context consensusModuleCtx = node.consensusModule().context();
        final AeronArchive.Context clone = consensusModuleCtx.archiveContext().clone();
        try (
            AeronArchive aeronArchive = AeronArchive.connect(clone);
            RecordingLog recordingLog = new RecordingLog(consensusModuleCtx.clusterDir(), false))
        {
            final RecordingLog.Entry lastTerm = recordingLog.findLastTerm();
            assertNotNull(lastTerm);
            final long recordingId = lastTerm.recordingId;

            final long recordingPosition = aeronArchive.getRecordingPosition(recordingId);
            final Subscription replay = aeronArchive.replay(
                recordingId,
                0,
                recordingPosition,
                "aeron:udp?endpoint=localhost:6666",
                100001);

            final MutableLong position = new MutableLong();
            final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
            final NewLeadershipTermEventDecoder newLeadershipTermEventDecoder = new NewLeadershipTermEventDecoder();
            final FragmentHandler fragmentHandler =
                (buffer, offset, length, header) ->
                {
                    messageHeaderDecoder.wrap(buffer, offset);

                    if (NewLeadershipTermEventDecoder.TEMPLATE_ID == messageHeaderDecoder.templateId())
                    {
                        newLeadershipTermEventDecoder.wrapAndApplyHeader(buffer, offset, messageHeaderDecoder);

                        final RecordingLog.Entry termEntry = recordingLog.findTermEntry(
                            newLeadershipTermEventDecoder.leadershipTermId());

                        assertNotNull(termEntry);
                        assertEquals(
                            newLeadershipTermEventDecoder.termBaseLogPosition(), termEntry.termBaseLogPosition);

                        if (0 < newLeadershipTermEventDecoder.leadershipTermId())
                        {
                            final RecordingLog.Entry previousTermEntry = recordingLog.findTermEntry(
                                newLeadershipTermEventDecoder.leadershipTermId() - 1);
                            assertNotNull(previousTermEntry);
                            assertEquals(
                                newLeadershipTermEventDecoder.termBaseLogPosition(),
                                previousTermEntry.logPosition,
                                previousTermEntry.toString());
                        }
                    }

                    position.set(header.position());
                };

            while (position.get() < recordingPosition)
            {
                if (0 == replay.poll(fragmentHandler, 10))
                {
                    Tests.yield();
                }
            }
        }
    }

    public void seedRecordingsFromLatestSnapshot()
    {
        for (final TestNode node : nodes)
        {
            if (null != node)
            {
                ClusterTool.seedRecordingLogFromSnapshot(node.consensusModule().context().clusterDir());
            }
        }
    }

    public static Builder aCluster()
    {
        return new Builder();
    }

    public void awaitBackupNodeErrors()
    {
        final TestBackupNode testBackupNode = requireNonNull(backupNode);
        while (0 == testBackupNode.clusterBackupErrorCount())
        {
            Tests.sleep(1, "No errors observed on backup node");
        }
    }

    private long countAllServiceErrors(final TestNode node)
    {
        final TestNode.TestService[] services = node.services();

        long errorCount = 0;
        for (final TestNode.TestService service : services)
        {
            errorCount += service.cluster().context().errorCounter().get();
        }

        return errorCount;
    }

    public void awaitServiceErrors(final TestNode node, final long count)
    {
        while (count < countAllServiceErrors(node))
        {
            Tests.sleep(1, "Errors count=" + count + " not seen on node=" + node.index());
        }
    }

    public void awaitServiceErrors(final long count)
    {
        for (final TestNode node : nodes)
        {
            if (null != node)
            {
                awaitServiceErrors(node, count);
            }
        }
    }

    public void failNextSnapshot(final boolean failNextSnapshot)
    {
        for (final TestNode node : nodes)
        {
            if (null != node)
            {
                node.service().failNextSnapshot(failNextSnapshot);
            }
        }
    }

    public static final class Builder
    {
        private int nodeCount = 3;
        private int appointedLeaderId = NULL_VALUE;
        private String logChannel = LOG_CHANNEL;
        private String ingressChannel = INGRESS_CHANNEL;
        private String egressChannel = EGRESS_CHANNEL;
        private AuthorisationServiceSupplier authorisationServiceSupplier;
        private AuthenticatorSupplier authenticationSupplier = new DefaultAuthenticatorSupplier();
        private TimerServiceSupplier timerServiceSupplier;
        private IntFunction<TestNode.TestService[]> serviceSupplier =
            (i) -> new TestNode.TestService[]{ new TestNode.TestService().index(i) };
        private final IntHashSet byHostInvalidInitialResolutions = new IntHashSet();
        private final IntHashSet byMemberInvalidInitialResolutions = new IntHashSet();
        private int archiveSegmentFileLength = TestCluster.SEGMENT_FILE_LENGTH;
        private boolean acceptStandbySnapshots = false;
        private ClusterBackup.Configuration.ReplayStart replayStart = ClusterBackup.Configuration.ReplayStart.BEGINNING;
        private File markFileBaseDir = null;
        private String clusterBaseDir = System.getProperty(
            CLUSTER_BASE_DIR_PROP_NAME, CommonContext.getAeronDirectoryName());
        private boolean useResponseChannels = false;

        public Builder withStaticNodes(final int nodeCount)
        {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder withAppointedLeader(final int appointedLeaderId)
        {
            this.appointedLeaderId = appointedLeaderId;
            return this;
        }

        public Builder withInvalidNameResolution(final int nodeId)
        {
            if (2 < nodeId)
            {
                throw new IllegalArgumentException("Only nodes 0 to 2 have name mappings that can be invalidated");
            }

            byHostInvalidInitialResolutions.add(nodeId);
            return this;
        }

        public Builder withMemberSpecificInvalidNameResolution(final int memberId)
        {
            byMemberInvalidInitialResolutions.add(memberId);
            return this;
        }

        public Builder withLogChannel(final String logChannel)
        {
            this.logChannel = logChannel;
            return this;
        }

        public Builder withIngressChannel(final String ingressChannel)
        {
            this.ingressChannel = ingressChannel;
            return this;
        }

        public Builder withEgressChannel(final String egressChannel)
        {
            this.egressChannel = egressChannel;
            return this;
        }

        public Builder withServiceSupplier(final IntFunction<TestNode.TestService[]> serviceSupplier)
        {
            this.serviceSupplier = serviceSupplier;
            return this;
        }

        public Builder withAuthorisationServiceSupplier(final AuthorisationServiceSupplier authorisationServiceSupplier)
        {
            this.authorisationServiceSupplier = authorisationServiceSupplier;
            return this;
        }

        public Builder withAuthenticationSupplier(final AuthenticatorSupplier authenticatorSupplier)
        {
            this.authenticationSupplier = authenticatorSupplier;
            return this;
        }

        public Builder withTimerServiceSupplier(final TimerServiceSupplier timerServiceSupplier)
        {
            this.timerServiceSupplier = timerServiceSupplier;
            return this;
        }

        public Builder withStandbySnapshots(final boolean acceptStandbySnapshots)
        {
            this.acceptStandbySnapshots = acceptStandbySnapshots;
            return this;
        }

        public Builder withSegmentFileLength(final int archiveSegmentFileLength)
        {
            this.archiveSegmentFileLength = archiveSegmentFileLength;
            return this;
        }

        public Builder withClusterBaseDir(final String clusterBaseDir)
        {
            this.clusterBaseDir = clusterBaseDir;
            return this;
        }

        public Builder markFileBaseDir(final File markFileBaseDir)
        {
            this.markFileBaseDir = markFileBaseDir;
            return this;
        }

        public Builder replayStart(final ClusterBackup.Configuration.ReplayStart replayStart)
        {
            this.replayStart = replayStart;
            return this;
        }

        public Builder useResponseChannels(final boolean enabled)
        {
            this.useResponseChannels = enabled;
            return this;
        }

        public TestCluster start()
        {
            return start(nodeCount);
        }

        public TestCluster start(final int toStart)
        {
            if (toStart > nodeCount)
            {
                throw new IllegalStateException(
                    "Unable to start " + toStart + " nodes, only " + nodeCount + " available");
            }

            final TestCluster testCluster = new TestCluster(
                nodeCount,
                appointedLeaderId,
                byHostInvalidInitialResolutions,
                serviceSupplier,
                useResponseChannels);
            testCluster.logChannel(logChannel);
            testCluster.ingressChannel(ingressChannel);
            testCluster.egressChannel(egressChannel);
            testCluster.authenticationSupplier(authenticationSupplier);
            testCluster.authorisationServiceSupplier(authorisationServiceSupplier);
            testCluster.timerServiceSupplier(timerServiceSupplier);
            testCluster.segmentFileLength(archiveSegmentFileLength);
            testCluster.invalidInitialResolutions(byHostInvalidInitialResolutions, byMemberInvalidInitialResolutions);
            testCluster.acceptStandbySnapshots(acceptStandbySnapshots);
            testCluster.markFileBaseDir(markFileBaseDir);
            testCluster.clusterBaseDir(clusterBaseDir);
            testCluster.replyStart(replayStart);

            try
            {
                for (int i = 0; i < toStart; i++)
                {
                    try
                    {
                        testCluster.startStaticNode(i, true);
                    }
                    catch (final RegistrationException e)
                    {
                        if (!byHostInvalidInitialResolutions.contains(i))
                        {
                            throw e;
                        }
                    }
                }
            }
            catch (final Exception e)
            {
                CloseHelper.close(testCluster);
                throw e;
            }

            return testCluster;
        }
    }

    private void replyStart(final ClusterBackup.Configuration.ReplayStart replayStart)
    {
        this.replayStart = replayStart;
    }

    private void clusterBaseDir(final String clusterBaseDir)
    {
        this.clusterBaseDir = clusterBaseDir;
    }

    private void markFileBaseDir(final File markFileBaseDir)
    {
        this.markFileBaseDir = markFileBaseDir;
    }

    private void invalidInitialResolutions(
        final IntHashSet byHostInvalidInitialResolutions,
        final IntHashSet byMemberInvalidInitialResolutions)
    {
        this.byHostInvalidInitialResolutions = byHostInvalidInitialResolutions;
        this.byMemberInvalidInitialResolutions = byMemberInvalidInitialResolutions;
    }

    private void acceptStandbySnapshots(final boolean acceptStandbySnapshots)
    {
        this.acceptStandbySnapshots = acceptStandbySnapshots;
    }

    public static DriverOutputConsumer clientDriverOutputConsumer(final DataCollector dataCollector)
    {
        if (TestMediaDriver.shouldRunJavaMediaDriver())
        {
            return null;
        }

        return new DriverOutputConsumer()
        {
            public void outputFiles(final String aeronDirectoryName, final File stdoutFile, final File stderrFile)
            {
                dataCollector.add(stdoutFile.toPath());
                dataCollector.add(stderrFile.toPath());
            }
        };
    }

    public static final CredentialsSupplier SIMPLE_CREDENTIALS_SUPPLIER = new CredentialsSupplier()
    {
        public byte[] encodedCredentials()
        {
            return "admin:admin".getBytes(StandardCharsets.US_ASCII);
        }

        public byte[] onChallenge(final byte[] encodedChallenge)
        {
            return ArrayUtil.EMPTY_BYTE_ARRAY;
        }
    };

    public static final CredentialsSupplier CHALLENGE_RESPONSE_CREDENTIALS_SUPPLIER = new CredentialsSupplier()
    {
        public byte[] encodedCredentials()
        {
            return "admin:adminC".getBytes(StandardCharsets.US_ASCII);
        }

        public byte[] onChallenge(final byte[] encodedChallenge)
        {
            return "admin:CSadmin".getBytes(StandardCharsets.US_ASCII);
        }
    };

    public static final CredentialsSupplier INVALID_SIMPLE_CREDENTIALS_SUPPLIER = new CredentialsSupplier()
    {
        public byte[] encodedCredentials()
        {
            return "admin:invalid".getBytes(StandardCharsets.US_ASCII);
        }

        public byte[] onChallenge(final byte[] encodedChallenge)
        {
            return ArrayUtil.EMPTY_BYTE_ARRAY;
        }
    };

    public static final CredentialsSupplier INVALID_CHALLENGE_RESPONSE_CREDENTIALS_SUPPLIER = new CredentialsSupplier()
    {
        public byte[] encodedCredentials()
        {
            return "admin:adminC".getBytes(StandardCharsets.US_ASCII);
        }

        public byte[] onChallenge(final byte[] encodedChallenge)
        {
            return "admin:invalid".getBytes(StandardCharsets.US_ASCII);
        }
    };

    private final class KeepAlive implements Runnable
    {
        private long keepAliveDeadlineMs;
        private EpochClock epochClock;

        private void init()
        {
            this.epochClock = requireNonNull(client, "client is not connected")
                .context().aeron().context().epochClock();
            final long nowMs = epochClock.time();
            keepAliveDeadlineMs = nowMs + TimeUnit.SECONDS.toMillis(1);
        }

        public void run()
        {
            final long nowMs = requireNonNull(epochClock, "did you call init() first?").time();
            if (nowMs > keepAliveDeadlineMs)
            {
                client.sendKeepAlive();
                keepAliveDeadlineMs = nowMs + TimeUnit.SECONDS.toMillis(1);
            }
            pollClient();
        }
    }

    static class DefaultEgressListener implements EgressListener
    {
        private final MutableLong responseCount = new MutableLong();
        private final MutableInteger newLeaderEvent = new MutableInteger();
        private boolean shouldErrorOnClientClose = true;

        public void onMessage(
            final long clusterSessionId,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            responseCount.increment();
        }

        public void onSessionEvent(
            final long correlationId,
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final EventCode code,
            final String detail)
        {
            if (EventCode.ERROR == code)
            {
                throw new ClusterException(detail);
            }
            else if (EventCode.CLOSED == code && shouldErrorOnClientClose)
            {
                final String msg = "[" + System.nanoTime() / 1_000_000_000.0 + "] session closed due to " + detail;
                throw new ClusterException(msg);
            }
        }

        public void onNewLeader(
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final String ingressEndpoints)
        {
            newLeaderEvent.increment();
        }

        long responseCount()
        {
            return responseCount.get();
        }

        int newLeaderEvent()
        {
            return newLeaderEvent.get();
        }
    }

    private static final class BackupListener implements ClusterBackupEventsListener
    {
        public void onBackupQuery()
        {
        }

        public void onPossibleFailure(final Exception ex)
        {
        }

        public void onBackupResponse(
            final ClusterMember[] clusterMembers,
            final ClusterMember logSourceMember,
            final List<RecordingLog.Snapshot> snapshotsToRetrieve)
        {
            for (final ClusterMember clusterMember : clusterMembers)
            {
                if (clusterMember.isLeader())
                {
                    return;
                }
            }

            throw new RuntimeException("No member has isLeader flag set");
        }

        public void onUpdatedRecordingLog(
            final RecordingLog recordingLog, final List<RecordingLog.Snapshot> snapshotsRetrieved)
        {
        }

        public void onLiveLogProgress(final long recordingId, final long recordingPosCounterId, final long logPosition)
        {
        }
    }
}
