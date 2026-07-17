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
import io.aeron.Subscription;
import io.aeron.UnavailableImageHandler;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.ClusterTerminationException;
import io.aeron.driver.DutyCycleTracker;
import io.aeron.logbuffer.Header;
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
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

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
import static org.mockito.Mockito.never;
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

    // Mirrors the leadership-confirmation token packing in ConsensusModuleAgent (term in the high 32 bits,
    // counter in the low 32 bits).
    private static long confirmationToken(final long leadershipTermId, final int confirmationCounter)
    {
        return (leadershipTermId << 32) | (confirmationCounter & 0xFFFF_FFFFL);
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
            leadershipTermId, 555, 0, ConsensusModuleAgent.NULL_CONFIRMATION_COUNTER);

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
        agent.onCommitPosition(leadershipTermId, 100, leader.id(), ConsensusModuleAgent.NULL_CONFIRMATION_COUNTER);
        assertEquals(100, agent.notifiedCommitPosition());
        assertEquals(clock.timeNanos(), agent.timeOfLastLogUpdateNs());

        clock.increment(1);
        agent.onCommitPosition(leadershipTermId, 200, leader.id(), ConsensusModuleAgent.NULL_CONFIRMATION_COUNTER);
        assertEquals(200, agent.notifiedCommitPosition());
        assertEquals(clock.timeNanos(), agent.timeOfLastLogUpdateNs());

        clock.increment(1);
        agent.onCommitPosition(leadershipTermId, 50, leader.id(), ConsensusModuleAgent.NULL_CONFIRMATION_COUNTER);
        assertEquals(200, agent.notifiedCommitPosition());
        assertEquals(clock.timeNanos(), agent.timeOfLastLogUpdateNs());

        clock.increment(1);
        agent.onCommitPosition(leadershipTermId, -1, leader.id(), ConsensusModuleAgent.NULL_CONFIRMATION_COUNTER);
        assertEquals(200, agent.notifiedCommitPosition());
        assertEquals(clock.timeNanos(), agent.timeOfLastLogUpdateNs());

        final long lastUpdateNs = clock.timeNanos();
        clock.increment(1);
        agent.onCommitPosition(leadershipTermId - 1, 5000, leader.id(), ConsensusModuleAgent.NULL_CONFIRMATION_COUNTER);
        assertEquals(200, agent.notifiedCommitPosition());
        assertEquals(lastUpdateNs, agent.timeOfLastLogUpdateNs());

        clock.increment(5);
        agent.onCommitPosition(leadershipTermId, 700, -100, ConsensusModuleAgent.NULL_CONFIRMATION_COUNTER);
        assertEquals(200, agent.notifiedCommitPosition());
        assertEquals(lastUpdateNs, agent.timeOfLastLogUpdateNs());

        clock.increment(3);
        agent.role(Cluster.Role.CANDIDATE);
        agent.onCommitPosition(leadershipTermId, 555, leader.id(), ConsensusModuleAgent.NULL_CONFIRMATION_COUNTER);
        assertEquals(200, agent.notifiedCommitPosition());
        assertEquals(lastUpdateNs, agent.timeOfLastLogUpdateNs());

        clock.increment(2);
        agent.role(Cluster.Role.LEADER);
        agent.onCommitPosition(leadershipTermId, 999, leader.id(), ConsensusModuleAgent.NULL_CONFIRMATION_COUNTER);
        assertEquals(200, agent.notifiedCommitPosition());
        assertEquals(lastUpdateNs, agent.timeOfLastLogUpdateNs());
    }

    @Test
    void followerEchoesAdvancedConfirmationCounterOncePerRound() throws Exception
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        ctx.epochClock(clock.asEpochClock()).clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        agent.state(ConsensusModule.State.ACTIVE, "");
        agent.role(Cluster.Role.FOLLOWER);
        final long leadershipTermId = 42;
        agent.leadershipTermId(leadershipTermId);
        final ClusterMember leader = new ClusterMember(1, "", "", "", "", "", "");
        Tests.setField(agent, "leaderMember", leader);
        Tests.setField(agent, "appendPosition", mock(ReadableCounter.class));

        final ConsensusPublisher mockPublisher = mock(ConsensusPublisher.class);
        when(mockPublisher.leadershipConfirmAck(any(), anyLong(), anyInt(), anyInt())).thenReturn(TRUE);
        Tests.setField(agent, "consensusPublisher", mockPublisher);

        final java.lang.reflect.Method sendFollowerPosition =
            ConsensusModuleAgent.class.getDeclaredMethod("updateFollowerPosition", long.class);
        sendFollowerPosition.setAccessible(true);

        // A commit-position carrying an advanced counter from the current leader is echoed once, tagged with the
        // follower's own current term and member id.
        agent.onCommitPosition(leadershipTermId, 100, leader.id(), 7);
        sendFollowerPosition.invoke(agent, clock.timeNanos());
        verify(mockPublisher).leadershipConfirmAck(any(), eq(leadershipTermId), eq(0), eq(7));

        // The same counter arriving again (e.g. on plain commit traffic) must not be echoed again -> no amplification.
        reset(mockPublisher);
        when(mockPublisher.leadershipConfirmAck(any(), anyLong(), anyInt(), anyInt())).thenReturn(TRUE);
        agent.onCommitPosition(leadershipTermId, 200, leader.id(), 7);
        sendFollowerPosition.invoke(agent, clock.timeNanos());
        verify(mockPublisher, never()).leadershipConfirmAck(any(), anyLong(), anyInt(), anyInt());

        // A pre-v17 leader (null counter) is never echoed.
        agent.onCommitPosition(leadershipTermId, 300, leader.id(), ConsensusModuleAgent.NULL_CONFIRMATION_COUNTER);
        sendFollowerPosition.invoke(agent, clock.timeNanos());
        verify(mockPublisher, never()).leadershipConfirmAck(any(), anyLong(), anyInt(), anyInt());
    }

    @Test
    void leaderConfirmsLeadershipOnlyWhenAQuorumEchoesBeyondTheToken()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        ctx.epochClock(clock.asEpochClock()).clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        agent.state(ConsensusModule.State.ACTIVE, "");
        agent.role(Cluster.Role.LEADER);
        final long leadershipTermId = 42;
        agent.leadershipTermId(leadershipTermId);

        final ClusterMember[] members = new ClusterMember[]
        {
            new ClusterMember(0, "", "", "", "", "", ""), // self (ctx.clusterMemberId == 0)
            new ClusterMember(1, "", "", "", "", "", ""),
            new ClusterMember(2, "", "", "", "", "", "")
        };
        final org.agrona.collections.Int2ObjectHashMap<ClusterMember> map =
            new org.agrona.collections.Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(members, map);
        Tests.setField(agent, "activeMembers", members);
        Tests.setField(agent, "clusterMemberByIdMap", map);

        // Only self so far -> below quorum of 2.
        assertFalse(agent.isLeadershipConfirmedSince(confirmationToken(leadershipTermId, 4)));

        // Follower 1 echoes counter 5 in the current term -> self + follower 1 = quorum.
        agent.onLeadershipConfirmAck(leadershipTermId, 1, 5);
        assertTrue(agent.isLeadershipConfirmedSince(confirmationToken(leadershipTermId, 4)));
        // ...but a token at or beyond what was echoed is not yet confirmed (in-flight/older acks cannot fake it).
        assertFalse(agent.isLeadershipConfirmedSince(confirmationToken(leadershipTermId, 5)));

        // An echo from a prior term must not be counted.
        agent.onLeadershipConfirmAck(leadershipTermId - 1, 2, 1000);
        assertFalse(agent.isLeadershipConfirmedSince(confirmationToken(leadershipTermId, 5)));

        // Follower 2 echoes beyond the token in the current term -> confirmed again.
        agent.onLeadershipConfirmAck(leadershipTermId, 2, 6);
        assertTrue(agent.isLeadershipConfirmedSince(confirmationToken(leadershipTermId, 5)));
    }

    @Test
    void confirmationIsUnavailableWhileAnElectionIsInProgress()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        ctx.epochClock(clock.asEpochClock()).clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        agent.state(ConsensusModule.State.ACTIVE, "");
        agent.role(Cluster.Role.LEADER);
        final long leadershipTermId = 42;
        agent.leadershipTermId(leadershipTermId);

        final ClusterMember[] members = new ClusterMember[]
        {
            new ClusterMember(0, "", "", "", "", "", ""),
            new ClusterMember(1, "", "", "", "", "", "")
        };
        final org.agrona.collections.Int2ObjectHashMap<ClusterMember> map =
            new org.agrona.collections.Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(members, map);
        Tests.setField(agent, "activeMembers", members);
        Tests.setField(agent, "clusterMemberByIdMap", map);

        final ConsensusPublisher mockPublisher = mock(ConsensusPublisher.class);
        Tests.setField(agent, "consensusPublisher", mockPublisher);
        // A stale counter from a prior leadership that must not leak into the new term.
        Tests.setField(agent, "confirmationCounter", 500);

        // During an election: no token is handed out, and the stale counter is replaced by the absent sentinel
        // on the wire so a follower cannot echo it and confirm before a post-election round.
        Tests.setField(agent, "election", mock(Election.class));
        assertEquals(Aeron.NULL_VALUE, agent.triggerQuorumConfirmation());
        agent.publishCommitPosition(1000L, leadershipTermId);
        verify(mockPublisher).commitPosition(
            any(), eq(leadershipTermId), eq(1000L), eq(0), eq(ConsensusModuleAgent.NULL_CONFIRMATION_COUNTER));

        // Once the election completes, the real counter is both handed out and broadcast.
        reset(mockPublisher);
        Tests.setField(agent, "election", null);
        // The token packs the term with the counter; the wire counter is still the raw int (unaffected).
        assertEquals(confirmationToken(leadershipTermId, 500), agent.triggerQuorumConfirmation());
        agent.publishCommitPosition(1000L, leadershipTermId);
        verify(mockPublisher).commitPosition(any(), eq(leadershipTermId), eq(1000L), eq(0), eq(500));
    }

    @Test
    void isLeadershipConfirmedSinceRejectsReservedSentinelTokens()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        ctx.epochClock(clock.asEpochClock()).clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        agent.state(ConsensusModule.State.ACTIVE, "");
        agent.role(Cluster.Role.LEADER);
        final long leadershipTermId = 42;
        agent.leadershipTermId(leadershipTermId);

        final ClusterMember[] members = new ClusterMember[]
        {
            new ClusterMember(0, "", "", "", "", "", ""),
            new ClusterMember(1, "", "", "", "", "", "")
        };
        final org.agrona.collections.Int2ObjectHashMap<ClusterMember> map =
            new org.agrona.collections.Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(members, map);
        Tests.setField(agent, "activeMembers", members);
        Tests.setField(agent, "clusterMemberByIdMap", map);

        // A follower has echoed a high counter, so a real token confirms...
        agent.onLeadershipConfirmAck(leadershipTermId, 1, 1000);
        assertTrue(agent.isLeadershipConfirmedSince(confirmationToken(leadershipTermId, 5)));

        // ...but the reserved sentinels must never confirm, even though every echo compares "beyond" them.
        assertFalse(agent.isLeadershipConfirmedSince(Aeron.NULL_VALUE));
        assertFalse(agent.isLeadershipConfirmedSince(ConsensusModuleAgent.NULL_CONFIRMATION_COUNTER));
    }

    @Test
    void tokenFromAnEarlierTermDoesNotConfirmAfterReElection()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        ctx.epochClock(clock.asEpochClock()).clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        agent.state(ConsensusModule.State.ACTIVE, "");
        agent.role(Cluster.Role.LEADER);
        final long termA = 42;
        agent.leadershipTermId(termA);

        final ClusterMember[] members = new ClusterMember[]
        {
            new ClusterMember(0, "", "", "", "", "", ""),
            new ClusterMember(1, "", "", "", "", "", "")
        };
        final org.agrona.collections.Int2ObjectHashMap<ClusterMember> map =
            new org.agrona.collections.Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(members, map);
        Tests.setField(agent, "activeMembers", members);
        Tests.setField(agent, "clusterMemberByIdMap", map);

        // Capture a token in term A; a quorum confirms it in term A. (confirmationCounter is 0 after
        // electionComplete; set it here since this test does not run a full election.)
        Tests.setField(agent, "confirmationCounter", 0);
        final long tokenTermA = agent.triggerQuorumConfirmation();
        agent.onLeadershipConfirmAck(termA, 1, 1000);
        assertTrue(agent.isLeadershipConfirmedSince(tokenTermA));

        // This node is deposed and re-elected (a later term); a quorum confirms in the new term.
        final long termB = termA + 2;
        agent.leadershipTermId(termB);
        agent.onLeadershipConfirmAck(termB, 1, 1000);

        // The stale term-A token must NOT confirm in term B, even though a quorum has echoed in term B.
        assertFalse(agent.isLeadershipConfirmedSince(tokenTermA));
        // A fresh term-B token does confirm.
        assertTrue(agent.isLeadershipConfirmedSince(confirmationToken(termB, 0)));
    }

    @Test
    void confirmationTokenUsesUnsignedTermBits()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        ctx.epochClock(clock.asEpochClock()).clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        agent.state(ConsensusModule.State.ACTIVE, "");
        agent.role(Cluster.Role.LEADER);
        // A term whose low 32 bits have the sign bit set: extraction must be unsigned (>>> not >>).
        final long leadershipTermId = 0x1_8000_0000L;
        agent.leadershipTermId(leadershipTermId);

        final ClusterMember[] members = new ClusterMember[]
        {
            new ClusterMember(0, "", "", "", "", "", ""),
            new ClusterMember(1, "", "", "", "", "", "")
        };
        final org.agrona.collections.Int2ObjectHashMap<ClusterMember> map =
            new org.agrona.collections.Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(members, map);
        Tests.setField(agent, "activeMembers", members);
        Tests.setField(agent, "clusterMemberByIdMap", map);

        final long token = confirmationToken(leadershipTermId, 5);
        agent.onLeadershipConfirmAck(leadershipTermId, 1, 6);
        assertTrue(agent.isLeadershipConfirmedSince(token));
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
        ctx.terminationHook(terminationHook).clusterClock(new TestClusterClock());

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        final ClusterTerminationException exception =
            assertThrowsExactly(ClusterTerminationException.class, agent::doWork);
        assertFalse(exception.isExpected());
        assertEquals("unexpected termination", exception.getMessage());

        final InOrder inOrder = inOrder(ctx.countedErrorHandler(), subscription, terminationHook);
        inOrder.verify(subscription).poll(any(), anyInt());
        inOrder.verify(ctx.countedErrorHandler()).onError(sourceException);
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
        ctx.terminationHook(terminationHook).clusterClock(new TestClusterClock());

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

        final InOrder inOrder = inOrder(ctx.countedErrorHandler(), subscription, terminationHook, election);
        inOrder.verify(subscription).poll(any(), anyInt());
        inOrder.verify(ctx.countedErrorHandler()).onError(sourceException);
        inOrder.verify(terminationHook).run();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldPropagateAgentTerminationExceptionAndRunTerminationHook()
    {
        final Subscription subscription = mock(Subscription.class);
        final AgentTerminationException sourceException = new AgentTerminationException("test");
        when(subscription.poll(any(), anyInt())).thenThrow(
            sourceException);
        when(mockAeron.addSubscription(anyString(), anyInt())).thenReturn(subscription);
        final Runnable terminationHook = mock(Runnable.class);
        ctx.terminationHook(terminationHook).clusterClock(new TestClusterClock());

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        final AgentTerminationException exception =
            assertThrowsExactly(AgentTerminationException.class, agent::doWork);
        assertSame(sourceException, exception);

        final InOrder inOrder = inOrder(ctx.countedErrorHandler(), subscription, terminationHook);
        inOrder.verify(subscription).poll(any(), anyInt());
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
}
