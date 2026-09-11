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
import io.aeron.ChannelUri;
import io.aeron.ConcurrentPublication;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.UnavailableImageHandler;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.cluster.codecs.AppendPositionDecoder;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.codecs.LeadershipConfirmAckDecoder;
import io.aeron.cluster.codecs.CompactLeadershipConfirmAckDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.ClusterTerminationException;
import io.aeron.driver.DutyCycleTracker;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.security.AuthorisationService;
import io.aeron.security.DefaultAuthenticatorSupplier;
import io.aeron.status.ReadableCounter;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestClusterClock;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

import static io.aeron.AeronCounters.CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_CONTROL_TOGGLE_TYPE_ID;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.ClusterControl.ToggleState.NEUTRAL;
import static io.aeron.cluster.ClusterControl.ToggleState.RESUME;
import static io.aeron.cluster.ClusterControl.ToggleState.STANDBY_SNAPSHOT;
import static io.aeron.cluster.ClusterControl.ToggleState.SUSPEND;
import static io.aeron.cluster.ConsensusModule.CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT;
import static io.aeron.cluster.ConsensusModule.Configuration.SESSION_LIMIT_MSG;
import static io.aeron.cluster.ConsensusModuleAgent.SLOW_TICK_INTERVAL_NS;
import static io.aeron.cluster.client.AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION;
import static java.lang.Boolean.TRUE;
import static org.agrona.concurrent.status.CountersReader.COUNTER_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConsensusModuleAgentTest
{
    private static final long SLOW_TICK_INTERVAL_MS = TimeUnit.NANOSECONDS.toMillis(SLOW_TICK_INTERVAL_NS);
    private static final String RESPONSE_CHANNEL_ONE = "aeron:udp?endpoint=localhost:11111";
    private static final String RESPONSE_CHANNEL_TWO = "aeron:udp?endpoint=localhost:22222";
    private static final int SCHEMA_ID = 17;
    private static final int UPDATE_INTERVAL_MS = 19;

    private final EgressPublisher mockEgressPublisher = mock(EgressPublisher.class);
    private final LogPublisher mockLogPublisher = mock(LogPublisher.class);
    private final Aeron mockAeron = mock(Aeron.class);
    private final ConcurrentPublication mockResponsePublication = mock(ConcurrentPublication.class);
    private final ExclusivePublication mockExclusivePublication = mock(ExclusivePublication.class);
    private final Counter mockTimedOutClientCounter = mock(Counter.class);
    private final LongConsumer mockTimeConsumer = mock(LongConsumer.class);
    private final Image mockImage = mock(Image.class);
    private final Header header = new Header(0, 0, mockImage);
    private final CountersManager countersManager = Tests.newCountersManager(2 * COUNTER_LENGTH);

    private final ConsensusModule.Context ctx = TestContexts.localhostConsensusModule()
        .errorHandler(Tests::onError)
        .errorCounter(mock(AtomicCounter.class))
        .countedErrorHandler(mock(CountedErrorHandler.class))
        .moduleStateCounter(mock(Counter.class))
        .commitPositionCounter(mock(Counter.class))
        .controlToggleCounter(mock(Counter.class))
        .nodeControlToggleCounter(mock(Counter.class))
        .clusterNodeRoleCounter(mock(Counter.class))
        .electionCounter(mock(Counter.class))
        .leadershipTermIdCounter(mock(Counter.class))
        .timedOutClientCounter(mockTimedOutClientCounter)
        .clusterTimeConsumerSupplier((ctx) -> mockTimeConsumer)
        .idleStrategySupplier(NoOpIdleStrategy::new)
        .timerServiceSupplier((clusterClock, timerHandler) -> mock(TimerService.class))
        .aeron(mockAeron)
        .clusterMemberId(0)
        .authenticatorSupplier(new DefaultAuthenticatorSupplier())
        .authorisationServiceSupplier(() -> AuthorisationService.DENY_ALL)
        .clusterMarkFile(mock(ClusterMarkFile.class))
        .archiveContext(new AeronArchive.Context())
        .logPublisher(mockLogPublisher)
        .egressPublisher(mockEgressPublisher)
        .dutyCycleTracker(new DutyCycleTracker());

    private Counter newCounter(final String name, final int typeId)
    {
        final AtomicCounter atomicCounter = countersManager.newCounter(name, typeId);
        return new Counter(countersManager, atomicCounter.id());
    }

    @BeforeEach
    void before()
    {
        when(mockAeron.conductorAgentInvoker()).thenReturn(mock(AgentInvoker.class));
        when(mockEgressPublisher.sendEvent(any(), anyLong(), anyInt(), any(), any())).thenReturn(TRUE);
        when(mockLogPublisher.appendSessionClose(anyInt(), any(), anyLong(), anyLong(), any())).thenReturn(TRUE);
        when(mockLogPublisher.appendSessionOpen(any(), anyLong(), anyLong())).thenReturn(128L);
        when(mockLogPublisher.appendClusterAction(anyLong(), anyLong(), any(ClusterAction.class), anyInt()))
            .thenReturn(TRUE);
        when(mockAeron.addPublication(anyString(), anyInt())).thenReturn(mockResponsePublication);
        when(mockAeron.getPublication(anyLong())).thenReturn(mockResponsePublication);
        when(mockAeron.addExclusivePublication(anyString(), anyInt())).thenReturn(mockExclusivePublication);
        when(mockAeron.addSubscription(anyString(), anyInt())).thenReturn(mock(Subscription.class));
        when(mockAeron.addSubscription(anyString(), anyInt(), eq(null), any(UnavailableImageHandler.class)))
            .thenReturn(mock(Subscription.class));
        when(mockResponsePublication.isConnected()).thenReturn(TRUE);
        when(mockResponsePublication.availableWindow()).thenReturn(Long.MAX_VALUE);
    }

    @Test
    void shouldUseAssignedRoleName()
    {
        final String expectedRoleName = "test-role-name";
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        ctx.agentRoleName(expectedRoleName)
            .epochClock(clock.asEpochClock())
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        assertEquals(expectedRoleName, agent.roleName());
    }

    @Test
    void shouldLimitActiveSessions()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        ctx.maxConcurrentSessions(1)
            .epochClock(clock.asEpochClock())
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        final long correlationIdOne = 1L;
        agent.state(ConsensusModule.State.ACTIVE, "");
        agent.role(Cluster.Role.LEADER);
        Tests.setField(agent, "appendPosition", mock(ReadableCounter.class));
        agent.onSessionConnect(
            correlationIdOne, 2, PROTOCOL_SEMANTIC_VERSION, RESPONSE_CHANNEL_ONE, new byte[0], "", header);

        clock.update(UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);
        agent.doWork();
        verify(mockTimeConsumer).accept(clock.time());

        verify(mockLogPublisher).appendSessionOpen(any(ClusterSession.class), anyLong(), anyLong());

        final long correlationIdTwo = 2L;
        agent.onSessionConnect(
            correlationIdTwo, 3, PROTOCOL_SEMANTIC_VERSION, RESPONSE_CHANNEL_TWO, new byte[0], "", header);
        clock.update(clock.time() + 10L, TimeUnit.MILLISECONDS);
        agent.doWork();
        verify(mockTimeConsumer).accept(clock.time());

        verify(mockEgressPublisher).sendEvent(
            any(ClusterSession.class), anyLong(), anyInt(), eq(EventCode.ERROR), eq(SESSION_LIMIT_MSG));
    }

    @Test
    void shouldCloseInactiveSession()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        final long startMs = SLOW_TICK_INTERVAL_MS;
        clock.update(startMs, TimeUnit.MILLISECONDS);

        ctx.epochClock(clock.asEpochClock())
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        final long correlationId = 1L;
        agent.state(ConsensusModule.State.ACTIVE, "");
        agent.role(Cluster.Role.LEADER);
        Tests.setField(agent, "appendPosition", mock(ReadableCounter.class));
        agent.onSessionConnect(
            correlationId, 2, PROTOCOL_SEMANTIC_VERSION, RESPONSE_CHANNEL_ONE, new byte[0], "", header);

        agent.doWork();

        verify(mockLogPublisher).appendSessionOpen(any(ClusterSession.class), anyLong(), eq(startMs));
        verify(mockTimeConsumer).accept(clock.time());

        final long timeMs = startMs + TimeUnit.NANOSECONDS.toMillis(ConsensusModule.Configuration.sessionTimeoutNs());
        clock.update(timeMs, TimeUnit.MILLISECONDS);
        agent.clusterMember().timeOfLastAppendPositionNs(clock.nanoTime());

        agent.doWork();

        final long timeoutMs = timeMs + SLOW_TICK_INTERVAL_MS;
        clock.update(timeoutMs, TimeUnit.MILLISECONDS);
        agent.doWork();

        verify(mockTimeConsumer).accept(clock.time());
        verify(mockTimedOutClientCounter).incrementRelease();
        verify(mockLogPublisher).appendSessionClose(
            anyInt(), any(ClusterSession.class), anyLong(), eq(timeoutMs), eq(clock.timeUnit()));
        verify(mockEgressPublisher).sendEvent(
            any(ClusterSession.class), anyLong(), anyInt(), eq(EventCode.CLOSED), eq(CloseReason.TIMEOUT.name()));
    }

    @Test
    void shouldCloseTerminatedSession()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        final long startMs = SLOW_TICK_INTERVAL_MS;
        clock.update(startMs, TimeUnit.MILLISECONDS);

        ctx.epochClock(clock.asEpochClock())
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        final long correlationId = 1L;
        agent.state(ConsensusModule.State.ACTIVE, "");
        agent.role(Cluster.Role.LEADER);
        Tests.setField(agent, "appendPosition", mock(ReadableCounter.class));
        agent.onSessionConnect(
            correlationId, 2, PROTOCOL_SEMANTIC_VERSION, RESPONSE_CHANNEL_ONE, new byte[0], "", header);

        agent.doWork();

        final ArgumentCaptor<ClusterSession> sessionCaptor = ArgumentCaptor.forClass(ClusterSession.class);

        verify(mockLogPublisher).appendSessionOpen(sessionCaptor.capture(), anyLong(), eq(startMs));

        final long timeMs = startMs + SLOW_TICK_INTERVAL_MS;
        clock.update(timeMs, TimeUnit.MILLISECONDS);
        agent.doWork();

        agent.onServiceCloseSession(sessionCaptor.getValue().id());

        verify(mockLogPublisher).appendSessionClose(
            anyInt(), any(ClusterSession.class), anyLong(), eq(timeMs), eq(clock.timeUnit()));
        verify(mockEgressPublisher).sendEvent(
            any(ClusterSession.class),
            anyLong(),
            anyInt(),
            eq(EventCode.CLOSED),
            eq(CloseReason.SERVICE_ACTION.name()));
    }

    @Test
    void shouldSuspendThenResume()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        final Counter stateCounter = newCounter("state counter", CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID);
        final Counter controlToggle = newCounter("control toggle", CLUSTER_CONTROL_TOGGLE_TYPE_ID);

        controlToggle.set(NEUTRAL.code());

        ctx.moduleStateCounter(stateCounter)
            .controlToggleCounter(controlToggle)
            .epochClock(clock.asEpochClock())
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        Tests.setField(agent, "appendPosition", mock(ReadableCounter.class));

        assertEquals(ConsensusModule.State.INIT.code(), stateCounter.get());

        agent.state(ConsensusModule.State.ACTIVE, "");
        agent.role(Cluster.Role.LEADER);
        assertEquals(ConsensusModule.State.ACTIVE.code(), stateCounter.get());

        SUSPEND.toggle(controlToggle);
        clock.update(SLOW_TICK_INTERVAL_MS, TimeUnit.MILLISECONDS);
        agent.doWork();

        assertEquals(ConsensusModule.State.SUSPENDED.code(), stateCounter.get());
        assertEquals(SUSPEND.code(), controlToggle.get());

        RESUME.toggle(controlToggle);
        clock.update(SLOW_TICK_INTERVAL_MS * 2, TimeUnit.MILLISECONDS);
        agent.doWork();

        assertEquals(ConsensusModule.State.ACTIVE.code(), stateCounter.get());
        assertEquals(NEUTRAL.code(), controlToggle.get());

        final InOrder inOrder = Mockito.inOrder(mockLogPublisher);
        inOrder.verify(mockLogPublisher).appendClusterAction(anyLong(), anyLong(), eq(ClusterAction.SUSPEND), anyInt());
        inOrder.verify(mockLogPublisher).appendClusterAction(anyLong(), anyLong(), eq(ClusterAction.RESUME), anyInt());
    }

    @Test
    void shouldThrowClusterTerminationExceptionUponShutdown()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        final CountedErrorHandler countedErrorHandler = mock(CountedErrorHandler.class);
        final MutableLong stateValue = new MutableLong();
        final Counter mockState = mock(Counter.class);

        when(mockState.get()).thenAnswer((invocation) -> stateValue.value);
        doAnswer(
            (invocation) ->
            {
                stateValue.value = invocation.getArgument(0);
                return null;
            })
            .when(mockState).set(anyLong());

        ctx.countedErrorHandler(countedErrorHandler)
            .moduleStateCounter(mockState)
            .epochClock(clock.asEpochClock())
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        agent.state(ConsensusModule.State.QUITTING, "");

        assertThrows(ClusterTerminationException.class,
            () -> agent.onServiceAck(1024, 100, 0, 55, 0));
    }

    @ParameterizedTest
    @CsvSource(value = {
        "false, null, aeron:udp?endpoint=acme:2040, aeron:udp?endpoint=acme:2040",
        "true, aeron:udp?endpoint=host:port, aeron:ipc, aeron:ipc",
        "false, null, aeron:ipc, aeron:ipc",
        "false, aeron:udp?endpoint=host:port, aeron:ipc, aeron:udp?endpoint=host:port",
        "false, aeron:udp?endpoint=host1:5050|interface=eth0|mtu=1440, " +
            "aeron:udp?endpoint=localhost:8080|mtu=8k|alias=test, " +
            "aeron:udp?endpoint=host1:5050|interface=eth0|mtu=1440|alias=test",
        "false, aeron:udp?endpoint=node0:21300|eos=false, aeron:udp?mtu=8000|interface=if1|eos=true|ttl=100, " +
            "aeron:udp?endpoint=node0:21300|eos=false|mtu=8000|interface=if1|ttl=100"
    }, nullValues = "null")
    void responseChannelIsBuiltBasedOnTheEgressChannel(
        final boolean isIpcIngressAllowed,
        final String egressChannel,
        final String responseChannel,
        final String expectedResponseChannel)
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        ctx.epochClock(clock.asEpochClock())
            .clusterClock(clock)
            .egressChannel(egressChannel)
            .isIpcIngressAllowed(isIpcIngressAllowed);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        agent.state(ConsensusModule.State.ACTIVE, "");
        agent.role(Cluster.Role.LEADER);

        final long correlationId = 1L;
        final int responseStreamId = 42;
        agent.onSessionConnect(
            correlationId, responseStreamId, PROTOCOL_SEMANTIC_VERSION, responseChannel, new byte[0], "", header);

        final ArgumentCaptor<String> channelCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockAeron).asyncAddPublication(channelCaptor.capture(), eq(responseStreamId));

        assertEquals(ChannelUri.parse(expectedResponseChannel), ChannelUri.parse(channelCaptor.getValue()));
    }

    @Test
    void shouldPublishLogMessageButNotSnapshotOnStandbySnapshot()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        final Counter stateCounter = newCounter("state counter", CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID);
        final Counter controlToggle = newCounter("control toggle", CLUSTER_CONTROL_TOGGLE_TYPE_ID);

        controlToggle.set(NEUTRAL.code());

        ctx.moduleStateCounter(stateCounter)
            .controlToggleCounter(controlToggle)
            .epochClock(clock.asEpochClock())
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        Tests.setField(agent, "appendPosition", mock(ReadableCounter.class));

        assertEquals(ConsensusModule.State.INIT.code(), stateCounter.get());

        agent.state(ConsensusModule.State.ACTIVE, "");
        agent.role(Cluster.Role.LEADER);
        assertEquals(ConsensusModule.State.ACTIVE.code(), stateCounter.get());

        assertTrue(STANDBY_SNAPSHOT.toggle(controlToggle));
        clock.update(SLOW_TICK_INTERVAL_MS, TimeUnit.MILLISECONDS);
        agent.doWork();

        assertEquals(ConsensusModule.State.ACTIVE.code(), stateCounter.get());
        assertEquals(NEUTRAL.code(), stateCounter.get());

        final InOrder inOrder = Mockito.inOrder(mockLogPublisher);
        inOrder.verify(mockLogPublisher).appendClusterAction(
            anyLong(), anyLong(), eq(ClusterAction.SNAPSHOT), eq(CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT));

        agent.onReplayClusterAction(-1, 2048, 0, ClusterAction.SNAPSHOT, CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT);
        assertEquals(ConsensusModule.State.ACTIVE.code(), stateCounter.get());
    }

    @Test
    void onNewLeadershipTermShouldUpdateTimeOfLastLeaderMessageReceived()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.NANOSECONDS);
        ctx.clusterClock(clock)
            .epochClock(clock.asEpochClock())
            .appVersionValidator(AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR);

        final int leadershipTermId = 2;
        final ConsensusModuleAgent consensusModuleAgent = new ConsensusModuleAgent(ctx);
        consensusModuleAgent.leadershipTermId(leadershipTermId);
        assertEquals(0, consensusModuleAgent.timeOfLastLeaderUpdateNs());

        clock.increment(12345);

        consensusModuleAgent.onNewLeadershipTerm(
            3,
            4,
            1024,
            2048,
            leadershipTermId,
            100,
            600,
            4096,
            8,
            200,
            0,
            42,
            777,
            false);

        assertEquals(12345, consensusModuleAgent.timeOfLastLeaderUpdateNs());
    }

    @Test
    void onCommitPositionShouldUpdateTimeOfLastLeaderMessageReceived()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.NANOSECONDS);
        ctx.clusterClock(clock)
            .epochClock(clock.asEpochClock());

        final int leadershipTermId = 42;
        final ConsensusModuleAgent consensusModuleAgent = new ConsensusModuleAgent(ctx);
        consensusModuleAgent.leadershipTermId(leadershipTermId);
        assertEquals(0, consensusModuleAgent.timeOfLastLeaderUpdateNs());

        clock.increment(444);

        consensusModuleAgent.onCommitPosition(
            leadershipTermId, 555, 0, LegacyConfirmation.NULL_COUNTER);

        assertEquals(444, consensusModuleAgent.timeOfLastLeaderUpdateNs());
    }

    @Test
    void shouldDelegateHandlingToRegisteredExtension()
    {
        final ConsensusModuleExtension consensusModuleExtension = mock(ConsensusModuleExtension.class, "used adapter");
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        ctx.epochClock(clock.asEpochClock())
            .clusterClock(clock)
            .consensusModuleExtension(consensusModuleExtension);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        agent.onExtensionMessage(0, 1, SCHEMA_ID, 0, null, 0, 0, null);

        verify(consensusModuleExtension)
            .onIngressExtensionMessage(0, 1, SCHEMA_ID, 0, null, 0, 0, null);
    }

    @Test
    void shouldThrowExceptionOnUnknownSchemaAndNoAdapter()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        final CountedErrorHandler mockErrorHandler = mock(CountedErrorHandler.class);
        ctx.countedErrorHandler(mockErrorHandler)
            .epochClock(clock.asEpochClock())
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        agent.onExtensionMessage(0, 0, SCHEMA_ID, 0, null, 0, 0, null);
        verify(mockErrorHandler).onError(any(ClusterEvent.class));
    }

    @Test
    void shouldHandlePaddingMessageAtEndOfTerm()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        final Counter stateCounter = newCounter("state counter", CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID);
        final Counter controlToggle = newCounter("control toggle", CLUSTER_CONTROL_TOGGLE_TYPE_ID);

        controlToggle.set(NEUTRAL.code());

        ctx.moduleStateCounter(stateCounter)
            .controlToggleCounter(controlToggle)
            .epochClock(clock.asEpochClock())
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        Tests.setField(agent, "appendPosition", mock(ReadableCounter.class));

        assertEquals(ConsensusModule.State.INIT.code(), stateCounter.get());

        agent.state(ConsensusModule.State.ACTIVE, "");
        agent.role(Cluster.Role.LEADER);
        assertEquals(ConsensusModule.State.ACTIVE.code(), stateCounter.get());

        final LogAdapter mockLogAdapter = mock(LogAdapter.class);

        agent.replayLogPoll(mockLogAdapter, 65536);
    }

    @Test
    void notifiedCommitPositionShouldNotGoBackwardsUponReceivingCommitPosition()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        final Counter stateCounter = newCounter("state counter", CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID);
        final Counter controlToggle = newCounter("control toggle", CLUSTER_CONTROL_TOGGLE_TYPE_ID);

        controlToggle.set(NEUTRAL.code());

        ctx.moduleStateCounter(stateCounter)
            .controlToggleCounter(controlToggle)
            .epochClock(clock.asEpochClock())
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        assertEquals(ConsensusModule.State.INIT.code(), stateCounter.get());

        agent.state(ConsensusModule.State.ACTIVE, "");
        agent.role(Cluster.Role.FOLLOWER);
        final long leadershipTermId = 42;
        agent.leadershipTermId(leadershipTermId);
        final ClusterMember leader = new ClusterMember(19, "", "", "", "", "", "");
        Tests.setField(agent, "leaderMember", leader);
        assertSame(leader, Tests.getField(agent, "leaderMember"));

        assertEquals(0, agent.notifiedCommitPosition());

        clock.increment(1);
        agent.onCommitPosition(leadershipTermId, 100, leader.id(), LegacyConfirmation.NULL_COUNTER);
        assertEquals(100, agent.notifiedCommitPosition());
        assertEquals(clock.timeNanos(), agent.timeOfLastLogUpdateNs());

        clock.increment(1);
        agent.onCommitPosition(leadershipTermId, 200, leader.id(), LegacyConfirmation.NULL_COUNTER);
        assertEquals(200, agent.notifiedCommitPosition());
        assertEquals(clock.timeNanos(), agent.timeOfLastLogUpdateNs());

        clock.increment(1);
        agent.onCommitPosition(leadershipTermId, 50, leader.id(), LegacyConfirmation.NULL_COUNTER);
        assertEquals(200, agent.notifiedCommitPosition());
        assertEquals(clock.timeNanos(), agent.timeOfLastLogUpdateNs());

        clock.increment(1);
        agent.onCommitPosition(leadershipTermId, -1, leader.id(), LegacyConfirmation.NULL_COUNTER);
        assertEquals(200, agent.notifiedCommitPosition());
        assertEquals(clock.timeNanos(), agent.timeOfLastLogUpdateNs());

        final long lastUpdateNs = clock.timeNanos();
        clock.increment(1);
        agent.onCommitPosition(leadershipTermId - 1, 5000, leader.id(), LegacyConfirmation.NULL_COUNTER);
        assertEquals(200, agent.notifiedCommitPosition());
        assertEquals(lastUpdateNs, agent.timeOfLastLogUpdateNs());

        clock.increment(5);
        agent.onCommitPosition(leadershipTermId, 700, -100, LegacyConfirmation.NULL_COUNTER);
        assertEquals(200, agent.notifiedCommitPosition());
        assertEquals(lastUpdateNs, agent.timeOfLastLogUpdateNs());

        clock.increment(3);
        agent.role(Cluster.Role.CANDIDATE);
        agent.onCommitPosition(leadershipTermId, 555, leader.id(), LegacyConfirmation.NULL_COUNTER);
        assertEquals(200, agent.notifiedCommitPosition());
        assertEquals(lastUpdateNs, agent.timeOfLastLogUpdateNs());

        clock.increment(2);
        agent.role(Cluster.Role.LEADER);
        agent.onCommitPosition(leadershipTermId, 999, leader.id(), LegacyConfirmation.NULL_COUNTER);
        assertEquals(200, agent.notifiedCommitPosition());
        assertEquals(lastUpdateNs, agent.timeOfLastLogUpdateNs());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldSharePublicationCreditBetweenAppendPositionAndConfirmationAck(final boolean legacy)
    {
        final FollowerPositionFixture follower = new FollowerPositionFixture(legacy);
        for (int cycle = 1; cycle <= 32; cycle++)
        {
            follower.recordedPosition = cycle * 64L;
            follower.requestConfirmation(cycle);

            // Continuous writes and reads compete for one frame of publication credit per duty cycle.
            assertEquals(1, follower.update(1));
            assertEquals(cycle, follower.messages.size());
            if (1 == (cycle & 1))
            {
                follower.assertAppendPosition(cycle - 1, follower.recordedPosition);
            }
            else
            {
                follower.assertConfirmationAck(cycle - 1, cycle);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRetryPendingConfirmationWhileRecordingContinues(final boolean legacy)
    {
        final FollowerPositionFixture follower = new FollowerPositionFixture(legacy);
        follower.recordedPosition = 64;
        follower.requestConfirmation(7);
        assertEquals(1, follower.update(1));
        follower.assertAppendPosition(0, 64);

        follower.recordedPosition = 128;
        assertEquals(1, follower.update(1));
        follower.assertConfirmationAck(1, 7);

        follower.recordedPosition = 192;
        assertEquals(1, follower.update(1));
        follower.assertAppendPosition(2, 192);
        assertEquals(0, follower.update(1));
        assertEquals(3, follower.messages.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRetainLatestConfirmationAcrossBackpressure(final boolean legacy)
    {
        final FollowerPositionFixture follower = new FollowerPositionFixture(legacy);
        follower.recordedPosition = 64;
        follower.requestConfirmation(7);
        assertEquals(0, follower.update(0));

        follower.recordedPosition = 128;
        follower.requestConfirmation(8);
        assertEquals(0, follower.update(0));
        assertTrue(follower.messages.isEmpty());

        assertEquals(2, follower.update(2));
        follower.assertConfirmationAck(0, 8);
        follower.assertAppendPosition(1, 128);
        assertEquals(0, follower.update(2));
        assertEquals(2, follower.messages.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldSendAppendPositionAndConfirmationInSameCycleWithoutBackpressure(final boolean legacy)
    {
        final FollowerPositionFixture follower = new FollowerPositionFixture(legacy);
        for (int round = 1; round <= 3; round++)
        {
            follower.recordedPosition = round * 64L;
            follower.requestConfirmation(round);
            assertEquals(2, follower.update(2));
            follower.assertAppendPosition((round - 1) * 2, follower.recordedPosition);
            follower.assertConfirmationAck((round - 1) * 2 + 1, round);
        }
        assertEquals(6, follower.messages.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldContinueAppendHeartbeatsWithoutConfirmationTraffic(final boolean legacy)
    {
        final FollowerPositionFixture follower = new FollowerPositionFixture(legacy);
        follower.recordedPosition = 64;
        assertEquals(1, follower.update(1));
        follower.assertAppendPosition(0, 64);
        assertEquals(0, follower.update(1));

        follower.nowNs += ctx.leaderHeartbeatIntervalNs();
        assertEquals(1, follower.update(1));
        follower.assertAppendPosition(1, 64);
        assertEquals(2, follower.messages.size());
    }

    @Test
    void shouldConfirmOnlyAfterTheRequestedRoundIsBroadcastAndEchoed()
    {
        final LeaderConfirmationFixture leader = new LeaderConfirmationFixture();
        final FollowerPositionFixture follower = new FollowerPositionFixture(1, 0);
        final long token = leader.agent.triggerQuorumConfirmation();
        assertFalse(leader.agent.isLeadershipConfirmedSince(token));

        leader.broadcastTo(follower);
        assertFalse(leader.agent.isLeadershipConfirmedSince(token));
        assertEquals(2, follower.update(2));
        assertFalse(leader.agent.isLeadershipConfirmedSince(token));
        follower.deliverLastAckTo(leader);
        assertTrue(leader.agent.isLeadershipConfirmedSince(token));

        final long nextToken = leader.agent.triggerQuorumConfirmation();
        follower.deliverLastAckTo(leader);
        assertFalse(leader.agent.isLeadershipConfirmedSince(nextToken));
        leader.broadcastTo(follower);
        assertEquals(1, follower.update(1));
        follower.deliverLastAckTo(leader);
        assertTrue(leader.agent.isLeadershipConfirmedSince(nextToken));
    }

    @Test
    void shouldCoalesceRequestsWithoutAdvancingOnOrdinaryCommitTraffic()
    {
        final LeaderConfirmationFixture leader = new LeaderConfirmationFixture();
        final FollowerPositionFixture follower = new FollowerPositionFixture(1, 0);
        final long first = leader.agent.triggerQuorumConfirmation();
        assertEquals(first, leader.agent.triggerQuorumConfirmation());
        leader.broadcastTo(follower);
        final long round = leader.lastBroadcastRound;
        assertEquals(2, follower.update(2));
        follower.deliverLastAckTo(leader);
        assertTrue(leader.agent.isLeadershipConfirmedSince(first));

        leader.broadcastTo(follower);
        assertEquals(round, leader.lastBroadcastRound);
        assertEquals(0, follower.update(2));

        final long next = leader.agent.triggerQuorumConfirmation();
        leader.broadcastTo(follower);
        assertEquals(round + 1, leader.lastBroadcastRound);
        assertTrue(leader.agent.isLeadershipConfirmedSince(first));
        assertFalse(leader.agent.isLeadershipConfirmedSince(next));
    }

    @Test
    void shouldIgnoreAcknowledgementsFromIneligibleSendersAndStates()
    {
        final LeaderConfirmationFixture leader = new LeaderConfirmationFixture();
        final FollowerPositionFixture follower = new FollowerPositionFixture(1, 0);
        final long token = leader.agent.triggerQuorumConfirmation();
        leader.broadcastTo(follower);
        final long round = leader.lastBroadcastRound;
        leader.agent.onCompactLeadershipConfirmAck(42, 99, round, leader.followerImages[1]);
        leader.agent.onCompactLeadershipConfirmAck(41, 1, round, leader.followerImages[1]);
        leader.agent.onCompactLeadershipConfirmAck(42, 1, LegacyConfirmation.NULL_COUNTER, leader.followerImages[1]);
        assertFalse(leader.agent.isLeadershipConfirmedSince(token));
        assertFalse(leader.members[1].compactConfirmation.confirmed(42, token));

        Tests.setField(leader.agent, "election", mock(Election.class));
        leader.agent.onCompactLeadershipConfirmAck(42, 1, round, leader.followerImages[1]);
        assertEquals(Aeron.NULL_VALUE, leader.agent.triggerQuorumConfirmation());
        assertFalse(leader.agent.isLeadershipConfirmedSince(token));
        Tests.setField(leader.agent, "election", null);
        for (final Cluster.Role role : new Cluster.Role[]{ Cluster.Role.FOLLOWER, Cluster.Role.CANDIDATE })
        {
            leader.agent.role(role);
            leader.agent.onCompactLeadershipConfirmAck(42, 1, round, leader.followerImages[1]);
            assertEquals(Aeron.NULL_VALUE, leader.agent.triggerQuorumConfirmation());
            assertFalse(leader.agent.isLeadershipConfirmedSince(token));
        }
        assertFalse(leader.members[1].compactConfirmation.confirmed(42, token));

        leader.agent.role(Cluster.Role.LEADER);
        assertEquals(2, follower.update(2));
        follower.deliverLastAckTo(leader);
        assertTrue(leader.agent.isLeadershipConfirmedSince(token));
        assertFalse(leader.agent.isLeadershipConfirmedSince(Aeron.NULL_VALUE));
        assertFalse(leader.agent.isLeadershipConfirmedSince(
            Long.MAX_VALUE));
    }

    @Test
    void shouldNotWalkBackAnAcknowledgedRound()
    {
        final LeaderConfirmationFixture leader = new LeaderConfirmationFixture();
        final FollowerPositionFixture follower = new FollowerPositionFixture(1, 0);
        final long first = leader.agent.triggerQuorumConfirmation();
        leader.broadcastTo(follower);
        final long firstRound = leader.lastBroadcastRound;
        assertEquals(2, follower.update(2));
        follower.deliverLastAckTo(leader);

        final long next = leader.agent.triggerQuorumConfirmation();
        leader.broadcastTo(follower);
        assertEquals(1, follower.update(1));
        follower.deliverLastAckTo(leader);
        leader.agent.onCompactLeadershipConfirmAck(42, 1, firstRound, leader.followerImages[1]);
        assertTrue(leader.members[1].compactConfirmation.confirmed(42, next));
        assertTrue(leader.agent.isLeadershipConfirmedSince(first));
        assertTrue(leader.agent.isLeadershipConfirmedSince(next));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldNotEchoDuplicateOlderOrAbsentRounds(final boolean legacy)
    {
        final FollowerPositionFixture follower = new FollowerPositionFixture(legacy);
        follower.requestConfirmation(7);
        assertEquals(2, follower.update(2));
        follower.assertConfirmationAck(1, 7);
        for (final int round : new int[]{ 7, 6, LegacyConfirmation.NULL_COUNTER })
        {
            follower.requestConfirmation(round);
            assertEquals(0, follower.update(2));
        }
        follower.requestConfirmation(8);
        assertEquals(1, follower.update(1));
        follower.assertConfirmationAck(2, 8);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldDiscardBackpressuredFollowerEchoWhenAnElectionCompletes(final boolean legacy)
    {
        final FollowerPositionFixture follower = new FollowerPositionFixture(legacy);
        follower.requestConfirmation(7);
        assertEquals(0, follower.update(0));
        follower.beginTerm(43);
        assertEquals(1, follower.update(2));
        follower.assertAppendPosition(0, 0);
        assertEquals(0, follower.update(2));
        follower.requestConfirmation(1);
        assertEquals(1, follower.update(1));
        follower.assertConfirmationAck(1, 1);
    }

    @Test
    void shouldSuppressElectionBroadcastCountersAndDiscardEarlierTermTokens()
    {
        final LeaderConfirmationFixture leader = new LeaderConfirmationFixture();
        final FollowerPositionFixture follower = new FollowerPositionFixture(1, 0);
        final long oldToken = leader.agent.triggerQuorumConfirmation();
        leader.broadcastTo(follower);
        assertEquals(2, follower.update(2));
        follower.deliverLastAckTo(leader);
        assertTrue(leader.agent.isLeadershipConfirmedSince(oldToken));
        leader.agent.triggerQuorumConfirmation();

        Tests.setField(leader.agent, "election", mock(Election.class));
        leader.agent.publishCommitPosition(1000, 43);
        assertEquals(LegacyConfirmation.NULL_COUNTER, leader.lastBroadcastRound);
        assertFalse(leader.agent.isLeadershipConfirmedSince(oldToken));
        assertEquals(Aeron.NULL_VALUE, leader.agent.triggerQuorumConfirmation());

        leader.beginTerm(43);
        follower.beginTerm(43);
        final long freshToken = leader.agent.triggerQuorumConfirmation();
        leader.broadcastTo(follower);
        follower.update(2);
        follower.deliverLastAckTo(leader);
        assertTrue(leader.agent.isLeadershipConfirmedSince(freshToken));
        assertFalse(leader.agent.isLeadershipConfirmedSince(oldToken));
    }

    private static ClusterMember[] setActiveMembers(final ConsensusModuleAgent agent, final int... ids)
    {
        final ClusterMember[] members = new ClusterMember[ids.length];
        for (int i = 0; i < ids.length; i++)
        {
            members[i] = new ClusterMember(ids[i], "", "", "", "", "", "");
        }

        final org.agrona.collections.Int2ObjectHashMap<ClusterMember> map =
            new org.agrona.collections.Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(members, map);
        Tests.setField(agent, "activeMembers", members);
        Tests.setField(agent, "clusterMemberByIdMap", map);

        return members;
    }

    @Test
    void notifiedCommitPositionShouldNotGoBackwardsUponElectionCompletion()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        final Counter stateCounter = newCounter("state counter", CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID);
        final Counter controlToggle = newCounter("control toggle", CLUSTER_CONTROL_TOGGLE_TYPE_ID);

        controlToggle.set(NEUTRAL.code());

        ctx.moduleStateCounter(stateCounter)
            .controlToggleCounter(controlToggle)
            .epochClock(clock.asEpochClock())
            .clusterClock(clock)
            .recordingLog(mock(RecordingLog.class))
            .ingressChannel("aeron:udp");

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        assertEquals(ConsensusModule.State.INIT.code(), stateCounter.get());

        agent.state(ConsensusModule.State.ACTIVE, "");
        agent.role(Cluster.Role.FOLLOWER);
        final Election election = mock(Election.class);
        final long logPosition = 200L;
        when(election.logPosition()).thenReturn(logPosition);
        when(election.leader()).thenReturn(mock(ClusterMember.class));
        Tests.setField(agent, "election", election);
        assertSame(election, Tests.getField(agent, "election"));

        assertEquals(0, agent.notifiedCommitPosition());

        agent.electionComplete(555);
        assertEquals(logPosition, agent.notifiedCommitPosition());
        verify(election).logPosition();

        reset(election);
        when(election.logPosition()).thenReturn(50L);
        when(election.leader()).thenReturn(mock(ClusterMember.class));
        Tests.setField(agent, "election", election);

        agent.electionComplete(777);
        assertEquals(logPosition, agent.notifiedCommitPosition());
        verify(election).logPosition();
    }

    @Test
    @SuppressWarnings("MethodLength")
    void notifiedCommitPositionShouldNotGoBackwardsUponReceivingNewLeadershipTerm()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        final Counter stateCounter = newCounter("state counter", CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID);
        final Counter controlToggle = newCounter("control toggle", CLUSTER_CONTROL_TOGGLE_TYPE_ID);

        controlToggle.set(NEUTRAL.code());

        final VersionValidator appVersionValidator = mock(VersionValidator.class);
        when(appVersionValidator.isVersionCompatible(anyInt(), anyInt())).thenReturn(true);
        ctx.moduleStateCounter(stateCounter)
            .controlToggleCounter(controlToggle)
            .epochClock(clock.asEpochClock())
            .clusterClock(clock)
            .appVersionValidator(appVersionValidator);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        assertEquals(ConsensusModule.State.INIT.code(), stateCounter.get());

        agent.state(ConsensusModule.State.ACTIVE, "");
        agent.role(Cluster.Role.FOLLOWER);
        final long leadershipTermId = 42;
        agent.leadershipTermId(leadershipTermId);
        final ClusterMember leader = new ClusterMember(19, "", "", "", "", "", "");
        Tests.setField(agent, "leaderMember", leader);
        assertSame(leader, Tests.getField(agent, "leaderMember"));

        assertEquals(0, agent.notifiedCommitPosition());

        clock.increment(1);
        agent.onNewLeadershipTerm(
            0,
            1,
            NULL_POSITION,
            NULL_POSITION,
            leadershipTermId,
            0,
            1024 * 1024,
            100,
            8,
            clock.nanoTime(),
            leader.id(),
            16,
            1,
            false);
        assertEquals(100, agent.notifiedCommitPosition());
        assertEquals(clock.timeNanos(), agent.timeOfLastLogUpdateNs());

        clock.increment(1);
        agent.onNewLeadershipTerm(
            0,
            1,
            NULL_POSITION,
            NULL_POSITION,
            leadershipTermId,
            0,
            1024 * 1024,
            3000,
            8,
            clock.nanoTime(),
            leader.id(),
            16,
            1,
            false);
        assertEquals(3000, agent.notifiedCommitPosition());
        assertEquals(clock.timeNanos(), agent.timeOfLastLogUpdateNs());

        clock.increment(1);
        agent.onNewLeadershipTerm(
            0,
            1,
            NULL_POSITION,
            NULL_POSITION,
            leadershipTermId,
            0,
            1024 * 1024,
            50,
            8,
            clock.nanoTime(),
            leader.id(),
            16,
            1,
            false);
        assertEquals(3000, agent.notifiedCommitPosition());
        assertEquals(clock.timeNanos(), agent.timeOfLastLogUpdateNs());

        clock.increment(1);
        agent.onNewLeadershipTerm(
            0,
            1,
            NULL_POSITION,
            NULL_POSITION,
            leadershipTermId,
            0,
            1024 * 1024,
            NULL_POSITION,
            8,
            clock.nanoTime(),
            leader.id(),
            16,
            1,
            false);
        assertEquals(3000, agent.notifiedCommitPosition());
        assertEquals(clock.timeNanos(), agent.timeOfLastLogUpdateNs());

        final long timeOfLastUpdateNs = clock.timeNanos();

        // wrong leadershipTermId
        clock.increment(1);
        agent.onNewLeadershipTerm(
            0,
            1,
            NULL_POSITION,
            NULL_POSITION,
            leadershipTermId - 1,
            0,
            1024 * 1024,
            5000,
            8,
            clock.nanoTime(),
            leader.id(),
            16,
            1,
            false);
        assertEquals(3000, agent.notifiedCommitPosition());
        assertEquals(timeOfLastUpdateNs, agent.timeOfLastLogUpdateNs());

        // wrong leaderId
        clock.increment(1);
        agent.onNewLeadershipTerm(
            0,
            1,
            NULL_POSITION,
            NULL_POSITION,
            leadershipTermId,
            0,
            1024 * 1024,
            7000,
            8,
            clock.nanoTime(),
            999999,
            16,
            1,
            false);
        assertEquals(3000, agent.notifiedCommitPosition());
        assertEquals(timeOfLastUpdateNs, agent.timeOfLastLogUpdateNs());

        // wrong role
        for (final Cluster.Role role : Cluster.Role.values())
        {
            if (Cluster.Role.FOLLOWER != role)
            {
                agent.role(role);
                clock.increment(1);
                agent.onNewLeadershipTerm(
                    0,
                    1,
                    NULL_POSITION,
                    NULL_POSITION,
                    leadershipTermId,
                    0,
                    1024 * 1024,
                    10000,
                    8,
                    clock.nanoTime(),
                    leader.id(),
                    16,
                    1,
                    false);
                assertEquals(3000, agent.notifiedCommitPosition());
                assertEquals(timeOfLastUpdateNs, agent.timeOfLastLogUpdateNs());
            }
        }
    }

    @Test
    void shouldTerminateOnArchiveStorageError()
    {
        final Subscription subscription = mock(Subscription.class);
        final ArchiveException sourceException =
            new ArchiveException("out of disc", ArchiveException.STORAGE_SPACE);
        when(subscription.poll(any(), anyInt())).thenThrow(
            sourceException);
        when(mockAeron.addSubscription(anyString(), anyInt())).thenReturn(subscription);
        final Runnable terminationHook = mock(Runnable.class);
        final ExtendedTerminationHook extendedTerminationHook = mock(ExtendedTerminationHook.class);
        ctx.terminationHook(terminationHook)
            .extendedTerminationHook(extendedTerminationHook)
            .clusterClock(new TestClusterClock());

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        final ClusterTerminationException exception =
            assertThrowsExactly(ClusterTerminationException.class, agent::doWork);
        assertFalse(exception.isExpected());
        assertEquals("unexpected termination", exception.getMessage());

        final InOrder inOrder =
            inOrder(ctx.countedErrorHandler(), subscription, terminationHook, extendedTerminationHook);
        inOrder.verify(subscription).poll(any(), anyInt());
        inOrder.verify(ctx.countedErrorHandler()).onError(sourceException);
        inOrder.verify(extendedTerminationHook).run(exception);
        inOrder.verify(terminationHook).run();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldTerminateIfLocalArchiveConnectionIsLost()
    {
        final Subscription subscription = mock(Subscription.class);
        final NumberFormatException sourceException = new NumberFormatException("xyz");
        when(subscription.poll(any(), anyInt())).thenThrow(
            sourceException);
        when(mockAeron.addSubscription(anyString(), anyInt())).thenReturn(subscription);
        final Runnable terminationHook = mock(Runnable.class);
        final ExtendedTerminationHook extendedTerminationHook = mock(ExtendedTerminationHook.class);
        ctx.terminationHook(terminationHook)
            .extendedTerminationHook(extendedTerminationHook)
            .clusterClock(new TestClusterClock());

        final AeronArchive aeronArchive = mock(AeronArchive.class);
        when(aeronArchive.state()).thenReturn(AeronArchive.State.CLOSED);
        final Election election = mock(Election.class);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        Tests.setField(agent, "archive", aeronArchive);
        Tests.setField(agent, "slowTickDeadlineNs", Long.MAX_VALUE);
        Tests.setField(agent, "election", election);

        final ClusterTerminationException exception =
            assertThrowsExactly(ClusterTerminationException.class, agent::doWork);
        assertFalse(exception.isExpected());
        assertEquals("unexpected termination", exception.getMessage());

        final InOrder inOrder =
            inOrder(ctx.countedErrorHandler(), subscription, terminationHook, extendedTerminationHook, election);
        inOrder.verify(subscription).poll(any(), anyInt());
        inOrder.verify(ctx.countedErrorHandler()).onError(sourceException);
        inOrder.verify(extendedTerminationHook).run(exception);
        inOrder.verify(terminationHook).run();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldPropagateAgentTerminationExceptionAndRunTerminationHooks()
    {
        final Subscription subscription = mock(Subscription.class);
        final AgentTerminationException sourceException = new AgentTerminationException("test");
        when(subscription.poll(any(), anyInt())).thenThrow(
            sourceException);
        when(mockAeron.addSubscription(anyString(), anyInt())).thenReturn(subscription);
        final Runnable terminationHook = mock(Runnable.class);
        final ExtendedTerminationHook extendedTerminationHook = mock(ExtendedTerminationHook.class);
        ctx.terminationHook(terminationHook)
            .extendedTerminationHook(extendedTerminationHook)
            .clusterClock(new TestClusterClock());

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        final AgentTerminationException exception =
            assertThrowsExactly(AgentTerminationException.class, agent::doWork);
        assertSame(sourceException, exception);

        final InOrder inOrder =
            inOrder(ctx.countedErrorHandler(), subscription, terminationHook, extendedTerminationHook);
        inOrder.verify(subscription).poll(any(), anyInt());
        inOrder.verify(extendedTerminationHook).run(sourceException);
        inOrder.verify(terminationHook).run();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldDelegateNonTerminalErrorHandlingToElectionIfSet()
    {
        final Subscription subscription = mock(Subscription.class);
        final NumberFormatException sourceException = new NumberFormatException("xyz");
        when(subscription.poll(any(), anyInt())).thenThrow(sourceException);
        when(mockAeron.addSubscription(anyString(), anyInt())).thenReturn(subscription);
        ctx.clusterClock(new TestClusterClock());

        final Election election = mock(Election.class);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        Tests.setField(agent, "election", election);
        Tests.setField(agent, "slowTickDeadlineNs", Long.MAX_VALUE);

        agent.doWork();

        final InOrder inOrder = inOrder(ctx.countedErrorHandler(), subscription, election);
        inOrder.verify(subscription).poll(any(), anyInt());
        inOrder.verify(election).handleError(anyLong(), eq(sourceException));
        inOrder.verifyNoMoreInteractions();
    }

    private static void completeElection(
        final ConsensusModuleAgent agent, final long term, final ClusterMember leader, final long nowNs)
    {
        final Election election = mock(Election.class);
        when(election.leadershipTermId()).thenReturn(term);
        when(election.leader()).thenReturn(leader);
        Tests.setField(agent, "election", election);
        agent.electionComplete(nowNs);
    }

    private final class LeaderConfirmationFixture
    {
        private final ConsensusModuleAgent agent;
        private final ClusterMember[] members;
        private long lastBroadcastRound;
        private final Image[] followerImages = { mock(Image.class), mock(Image.class), mock(Image.class) };
        private long position;
        private long term;

        private LeaderConfirmationFixture()
        {
            final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
            final ConsensusModule.Context context = ctx.clone()
                .clusterMemberId(0).epochClock(clock.asEpochClock()).clusterClock(clock)
                .recordingLog(mock(RecordingLog.class)).ingressChannel("aeron:udp");
            agent = new ConsensusModuleAgent(context);
            agent.state(ConsensusModule.State.ACTIVE, "");
            agent.role(Cluster.Role.LEADER);
            members = setActiveMembers(agent, 0, 1, 2);
            final ConsensusPublisher publisher = mock(ConsensusPublisher.class);
            when(publisher.consensusConnection(any(), anyInt())).thenReturn(true);
            doAnswer(invocation ->
            {
                lastBroadcastRound = invocation.getArgument(3);
                return true;
            }).when(publisher).compactCommitPosition(any(), anyLong(), anyLong(), anyLong());
            doAnswer(invocation ->
            {
                lastBroadcastRound = invocation.<Integer>getArgument(4);
                return null;
            }).when(publisher).commitPosition(any(), anyLong(), anyLong(), anyInt(), anyInt());
            for (int i = 1; i < members.length; i++)
            {
                members[i].publication(mock(ExclusivePublication.class));
                agent.onConsensusConnection(i, followerImages[i]);
            }
            Tests.setField(agent, "consensusPublisher", publisher);
            beginTerm(42);
        }

        private void beginTerm(final long term)
        {
            this.term = term;
            completeElection(agent, term, members[0], position);
        }

        private void broadcastTo(final FollowerPositionFixture follower)
        {
            position += 64;
            assertEquals(1, agent.updateLeaderPosition(position, position, position));
            follower.agent.onCompactCommitPosition(term, position, lastBroadcastRound, follower.leaderImage);
        }
    }

    private final class FollowerPositionFixture
    {
        private static final long LEADERSHIP_TERM_ID = 42;
        private final ConsensusModuleAgent agent;
        private final ClusterMember leader;
        private final int memberId;
        private final boolean legacy;
        private final Image leaderImage = mock(Image.class);
        private long term = LEADERSHIP_TERM_ID;
        private final List<UnsafeBuffer> messages = new ArrayList<>();
        private int frameCredit;
        private long recordedPosition;
        private long nowNs;

        private FollowerPositionFixture(final boolean legacy)
        {
            this(0, 1, legacy);
        }

        private FollowerPositionFixture(final int memberId, final int leaderId)
        {
            this(memberId, leaderId, false);
        }

        private FollowerPositionFixture(final int memberId, final int leaderId, final boolean legacy)
        {
            this.memberId = memberId;
            this.legacy = legacy;
            final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
            final ConsensusModule.Context context = ctx.clone()
                .clusterMemberId(memberId)
                .clusterMembers(
                    memberId + ",localhost:20000,localhost:20001,localhost:20002,localhost:0,localhost:8010")
                .epochClock(clock.asEpochClock()).clusterClock(clock)
                .recordingLog(mock(RecordingLog.class)).ingressChannel("aeron:udp");
            agent = new ConsensusModuleAgent(context);
            agent.state(ConsensusModule.State.ACTIVE, "");
            agent.role(Cluster.Role.FOLLOWER);
            agent.leadershipTermId(LEADERSHIP_TERM_ID);
            leader = setActiveMembers(agent, 0, 1, 2)[leaderId];
            agent.onConsensusConnection(leaderId, leaderImage);

            final ExclusivePublication publication = mock(ExclusivePublication.class);
            when(publication.tryClaim(anyInt(), any(BufferClaim.class))).thenAnswer(invocation ->
            {
                if (0 == frameCredit)
                {
                    return Publication.BACK_PRESSURED;
                }

                --frameCredit;
                final int length = invocation.getArgument(0);
                final UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);
                final BufferClaim claim = invocation.getArgument(1);
                claim.wrap(buffer, 0, DataHeaderFlyweight.HEADER_LENGTH + length);
                messages.add(buffer);
                return messages.size() * 64L;
            });
            leader.publication(publication);
            // Credit tests start after connection setup; CompactConsensusTest exercises the announcements.
            leader.compactConfirmation.announced(publication);
            Tests.setField(agent, "leaderMember", leader);
            Tests.setField(agent, "consensusPublisher", new ConsensusPublisher());
            final ReadableCounter appendPosition = mock(ReadableCounter.class);
            when(appendPosition.get()).thenAnswer(invocation -> recordedPosition);
            Tests.setField(agent, "appendPosition", appendPosition);
            beginTerm(LEADERSHIP_TERM_ID);
        }

        private void requestConfirmation(final int counter)
        {
            if (legacy)
            {
                agent.onCommitPosition(term, recordedPosition, leader.id(), counter);
            }
            else
            {
                agent.onCompactCommitPosition(term, recordedPosition, counter, leaderImage);
            }
        }

        private int update(final int frameCredit)
        {
            this.frameCredit = frameCredit;
            return agent.updateFollowerPosition(++nowNs);
        }

        private void beginTerm(final long term)
        {
            this.term = term;
            completeElection(agent, term, leader, ++nowNs);
        }

        private void deliverLastAckTo(final LeaderConfirmationFixture target)
        {
            final CompactLeadershipConfirmAckDecoder ack =
                new CompactLeadershipConfirmAckDecoder().wrapAndApplyHeader(
                    messages.get(messages.size() - 1), DataHeaderFlyweight.HEADER_LENGTH, new MessageHeaderDecoder());
            target.agent.onCompactLeadershipConfirmAck(
                ack.leadershipTermId(), ack.followerMemberId(), ack.confirmationCounter(),
                target.followerImages[memberId]);
        }

        private void assertAppendPosition(final int index, final long position)
        {
            final UnsafeBuffer buffer = messages.get(index);
            final MessageHeaderDecoder header =
                new MessageHeaderDecoder().wrap(buffer, DataHeaderFlyweight.HEADER_LENGTH);
            assertEquals(AppendPositionDecoder.TEMPLATE_ID, header.templateId());
            final AppendPositionDecoder decoder =
                new AppendPositionDecoder().wrapAndApplyHeader(buffer, DataHeaderFlyweight.HEADER_LENGTH, header);
            assertEquals(term, decoder.leadershipTermId());
            assertEquals(memberId, decoder.followerMemberId());
            assertEquals(position, decoder.logPosition());
        }

        private void assertConfirmationAck(final int index, final int counter)
        {
            final UnsafeBuffer buffer = messages.get(index);
            final MessageHeaderDecoder header =
                new MessageHeaderDecoder().wrap(buffer, DataHeaderFlyweight.HEADER_LENGTH);
            if (legacy)
            {
                assertEquals(LeadershipConfirmAckDecoder.TEMPLATE_ID, header.templateId());
                final LeadershipConfirmAckDecoder decoder = new LeadershipConfirmAckDecoder()
                    .wrapAndApplyHeader(buffer, DataHeaderFlyweight.HEADER_LENGTH, header);
                assertEquals(term, decoder.leadershipTermId());
                assertEquals(memberId, decoder.followerMemberId());
                assertEquals(counter, decoder.confirmationCounter());
            }
            else
            {
                assertEquals(CompactLeadershipConfirmAckDecoder.TEMPLATE_ID, header.templateId());
                final CompactLeadershipConfirmAckDecoder decoder = new CompactLeadershipConfirmAckDecoder()
                    .wrapAndApplyHeader(buffer, DataHeaderFlyweight.HEADER_LENGTH, header);
                assertEquals(term, decoder.leadershipTermId());
                assertEquals(memberId, decoder.followerMemberId());
                assertEquals(counter, decoder.confirmationCounter());
            }
        }
    }
}
