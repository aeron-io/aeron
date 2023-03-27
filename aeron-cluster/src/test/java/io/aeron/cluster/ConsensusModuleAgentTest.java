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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ConcurrentPublication;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.UnavailableImageHandler;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.ClusterTerminationException;
import io.aeron.driver.DutyCycleTracker;
import io.aeron.security.AuthorisationService;
import io.aeron.security.DefaultAuthenticatorSupplier;
import io.aeron.status.ReadableCounter;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestClusterClock;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.AgentInvoker;
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
import static io.aeron.cluster.ClusterControl.ToggleState.*;
import static io.aeron.cluster.ConsensusModule.Configuration.SESSION_LIMIT_MSG;
import static io.aeron.cluster.ConsensusModule.CLUSTER_ACTION_FLAGS_BACKGROUND_SNAPSHOT;
import static io.aeron.cluster.ConsensusModuleAgent.SLOW_TICK_INTERVAL_NS;
import static io.aeron.cluster.client.AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION;
import static java.lang.Boolean.TRUE;
import static org.agrona.concurrent.status.CountersReader.COUNTER_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class ConsensusModuleAgentTest
{
    private static final long SLOW_TICK_INTERVAL_MS = TimeUnit.NANOSECONDS.toMillis(SLOW_TICK_INTERVAL_NS);
    private static final String RESPONSE_CHANNEL_ONE = "aeron:udp?endpoint=localhost:11111";
    private static final String RESPONSE_CHANNEL_TWO = "aeron:udp?endpoint=localhost:22222";

    private final EgressPublisher mockEgressPublisher = mock(EgressPublisher.class);
    private final LogPublisher mockLogPublisher = mock(LogPublisher.class);
    private final Aeron mockAeron = mock(Aeron.class);
    private final ConcurrentPublication mockResponsePublication = mock(ConcurrentPublication.class);
    private final ExclusivePublication mockExclusivePublication = mock(ExclusivePublication.class);
    private final Counter mockTimedOutClientCounter = mock(Counter.class);
    private final LongConsumer mockTimeConsumer = mock(LongConsumer.class);
    private final CountersManager countersManager = Tests.newCountersMananger(2 * COUNTER_LENGTH);
    private long registrationId = 20;

    private final ConsensusModule.Context ctx = TestContexts.localhostConsensusModule()
        .errorHandler(Tests::onError)
        .errorCounter(mock(AtomicCounter.class))
        .countedErrorHandler(mock(CountedErrorHandler.class))
        .moduleStateCounter(mock(Counter.class))
        .commitPositionCounter(mock(Counter.class))
        .controlToggleCounter(mock(Counter.class))
        .clusterNodeRoleCounter(mock(Counter.class))
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
        return new Counter(countersManager, ++registrationId, atomicCounter.id());
    }

    @BeforeEach
    public void before()
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
    }

    @Test
    public void shouldGenerateRoleNameWhenNotSet()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        ctx.epochClock(clock).clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        assertEquals("consensus-module_0_0", agent.roleName());
    }

    @Test
    public void shouldUseAssignedRoleName()
    {
        final String expectedRoleName = "test-role-name";
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        ctx.agentRoleName(expectedRoleName)
            .epochClock(clock)
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        assertEquals(expectedRoleName, agent.roleName());
    }

    @Test
    public void shouldLimitActiveSessions()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        ctx.maxConcurrentSessions(1)
            .epochClock(clock)
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        final long correlationIdOne = 1L;
        agent.state(ConsensusModule.State.ACTIVE);
        agent.role(Cluster.Role.LEADER);
        Tests.setField(agent, "appendPosition", mock(ReadableCounter.class));
        agent.onSessionConnect(correlationIdOne, 2, PROTOCOL_SEMANTIC_VERSION, RESPONSE_CHANNEL_ONE, new byte[0]);

        clock.update(17, TimeUnit.MILLISECONDS);
        agent.doWork();
        verify(mockTimeConsumer).accept(clock.time());

        verify(mockLogPublisher).appendSessionOpen(any(ClusterSession.class), anyLong(), anyLong());

        final long correlationIdTwo = 2L;
        agent.onSessionConnect(correlationIdTwo, 3, PROTOCOL_SEMANTIC_VERSION, RESPONSE_CHANNEL_TWO, new byte[0]);
        clock.update(clock.time() + 10L, TimeUnit.MILLISECONDS);
        agent.doWork();
        verify(mockTimeConsumer).accept(clock.time());

        verify(mockEgressPublisher).sendEvent(
            any(ClusterSession.class), anyLong(), anyInt(), eq(EventCode.ERROR), eq(SESSION_LIMIT_MSG));
    }

    @Test
    public void shouldCloseInactiveSession()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        final long startMs = SLOW_TICK_INTERVAL_MS;
        clock.update(startMs, TimeUnit.MILLISECONDS);

        ctx.epochClock(clock)
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        final long correlationId = 1L;
        agent.state(ConsensusModule.State.ACTIVE);
        agent.role(Cluster.Role.LEADER);
        Tests.setField(agent, "appendPosition", mock(ReadableCounter.class));
        agent.onSessionConnect(correlationId, 2, PROTOCOL_SEMANTIC_VERSION, RESPONSE_CHANNEL_ONE, new byte[0]);

        agent.doWork();

        verify(mockLogPublisher).appendSessionOpen(any(ClusterSession.class), anyLong(), eq(startMs));
        verify(mockTimeConsumer).accept(clock.time());

        final long timeMs = startMs + TimeUnit.NANOSECONDS.toMillis(ConsensusModule.Configuration.sessionTimeoutNs());
        clock.update(timeMs, TimeUnit.MILLISECONDS);
        agent.doWork();

        final long timeoutMs = timeMs + SLOW_TICK_INTERVAL_MS;
        clock.update(timeoutMs, TimeUnit.MILLISECONDS);
        agent.doWork();

        verify(mockTimeConsumer).accept(clock.time());
        verify(mockTimedOutClientCounter).incrementOrdered();
        verify(mockLogPublisher).appendSessionClose(
            anyInt(), any(ClusterSession.class), anyLong(), eq(timeoutMs), eq(clock.timeUnit()));
        verify(mockEgressPublisher).sendEvent(
            any(ClusterSession.class), anyLong(), anyInt(), eq(EventCode.CLOSED), eq(CloseReason.TIMEOUT.name()));
    }

    @Test
    public void shouldCloseTerminatedSession()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        final long startMs = SLOW_TICK_INTERVAL_MS;
        clock.update(startMs, TimeUnit.MILLISECONDS);

        ctx.epochClock(clock)
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        final long correlationId = 1L;
        agent.state(ConsensusModule.State.ACTIVE);
        agent.role(Cluster.Role.LEADER);
        Tests.setField(agent, "appendPosition", mock(ReadableCounter.class));
        agent.onSessionConnect(correlationId, 2, PROTOCOL_SEMANTIC_VERSION, RESPONSE_CHANNEL_ONE, new byte[0]);

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
    public void shouldSuspendThenResume()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        final Counter stateCounter = newCounter("state counter", CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID);
        final Counter controlToggle = newCounter("control toggle", CLUSTER_CONTROL_TOGGLE_TYPE_ID);

        controlToggle.set(NEUTRAL.code());

        ctx.moduleStateCounter(stateCounter);
        ctx.controlToggleCounter(controlToggle);
        ctx.epochClock(clock).clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        Tests.setField(agent, "appendPosition", mock(ReadableCounter.class));

        assertEquals(ConsensusModule.State.INIT.code(), stateCounter.get());

        agent.state(ConsensusModule.State.ACTIVE);
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
    public void shouldThrowClusterTerminationExceptionUponShutdown()
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
            .epochClock(clock)
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        agent.state(ConsensusModule.State.QUITTING);

        assertThrows(ClusterTerminationException.class,
            () -> agent.onServiceAck(1024, 100, 0, 55, 0));
    }

    @ParameterizedTest
    @CsvSource(value = {
        "null, aeron:udp?endpoint=acme:2040, aeron:udp?endpoint=acme:2040",
        ", aeron:ipc, aeron:ipc",
        "aeron:udp?endpoint=host1:5050|interface=eth0|mtu=1440, aeron:udp?endpoint=localhost:8080|mtu=8000, " +
            "aeron:udp?endpoint=localhost:8080|interface=eth0|mtu=1440",
        "aeron:udp?endpoint=node0:21300|eos=false, aeron:udp?mtu=8000|interface=if1|eos=true|ttl=100, " +
            "aeron:udp?endpoint=node0:21300|eos=false"
    }, nullValues = "null")
    void responseChannelIsBuiltBasedOnTheEgressChannel(
        final String egressChannel, final String responseChannel, final String expectedResponseChannel)
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        ctx.epochClock(clock).clusterClock(clock);
        ctx.egressChannel(egressChannel);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        agent.state(ConsensusModule.State.ACTIVE);
        agent.role(Cluster.Role.LEADER);

        final long correlationId = 1L;
        final int responseStreamId = 42;
        agent.onSessionConnect(
            correlationId, responseStreamId, PROTOCOL_SEMANTIC_VERSION, responseChannel, new byte[0]);

        final ArgumentCaptor<String> channelCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockAeron).asyncAddPublication(channelCaptor.capture(), eq(responseStreamId));

        assertEquals(ChannelUri.parse(expectedResponseChannel), ChannelUri.parse(channelCaptor.getValue()));
    }

    @Test
    public void shouldPublishLogMessageButNotSnapshotOnBackgroundSnapshot()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        final Counter stateCounter = newCounter("state counter", CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID);
        final Counter controlToggle = newCounter("control toggle", CLUSTER_CONTROL_TOGGLE_TYPE_ID);

        controlToggle.set(NEUTRAL.code());

        ctx.moduleStateCounter(stateCounter);
        ctx.controlToggleCounter(controlToggle);
        ctx.epochClock(clock).clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        Tests.setField(agent, "appendPosition", mock(ReadableCounter.class));

        assertEquals(ConsensusModule.State.INIT.code(), stateCounter.get());

        agent.state(ConsensusModule.State.ACTIVE);
        agent.role(Cluster.Role.LEADER);
        assertEquals(ConsensusModule.State.ACTIVE.code(), stateCounter.get());

        assertTrue(BACKGROUND_SNAPSHOT.toggle(controlToggle));
        clock.update(SLOW_TICK_INTERVAL_MS, TimeUnit.MILLISECONDS);
        agent.doWork();

        assertEquals(ConsensusModule.State.ACTIVE.code(), stateCounter.get());
        assertEquals(NEUTRAL.code(), stateCounter.get());

        final InOrder inOrder = Mockito.inOrder(mockLogPublisher);
        inOrder.verify(mockLogPublisher).appendClusterAction(
            anyLong(), anyLong(), eq(ClusterAction.SNAPSHOT), eq(CLUSTER_ACTION_FLAGS_BACKGROUND_SNAPSHOT));

        agent.onReplayClusterAction(-1, ClusterAction.SNAPSHOT, CLUSTER_ACTION_FLAGS_BACKGROUND_SNAPSHOT);
        assertEquals(ConsensusModule.State.ACTIVE.code(), stateCounter.get());
    }

    @Test
    void onNewLeadershipTermShouldUpdateTimeOfLastLeaderMessageReceived()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.NANOSECONDS);
        ctx.clusterClock(clock)
            .epochClock(clock)
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
            8,
            200,
            0,
            42,
            777,
            false);

        assertEquals(12345, consensusModuleAgent.timeOfLastLeaderUpdateNs());
    }

    @Test
    void onCommmitPositionShouldUpdateTimeOfLastLeaderMessageReceived()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.NANOSECONDS);
        ctx.clusterClock(clock)
            .epochClock(clock);

        final int leadershipTermId = 42;
        final ConsensusModuleAgent consensusModuleAgent = new ConsensusModuleAgent(ctx);
        consensusModuleAgent.leadershipTermId(leadershipTermId);
        assertEquals(0, consensusModuleAgent.timeOfLastLeaderUpdateNs());

        clock.increment(444);

        consensusModuleAgent.onCommitPosition(leadershipTermId, 555, 0);

        assertEquals(444, consensusModuleAgent.timeOfLastLeaderUpdateNs());
    }
}
