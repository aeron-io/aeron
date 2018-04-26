/*
 * Copyright 2014-2018 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.Counter;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.status.ReadableCounter;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import static io.aeron.cluster.ClusterControl.ToggleState.*;
import static java.lang.Boolean.TRUE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class SequencerAgentTest
{
    private static final String RESPONSE_CHANNEL_ONE = "responseChannelOne";
    private static final String RESPONSE_CHANNEL_TWO = "responseChannelTwo";

    private final EgressPublisher mockEgressPublisher = mock(EgressPublisher.class);
    private final LogPublisher mockLogPublisher = mock(LogPublisher.class);
    private final Aeron mockAeron = mock(Aeron.class);
    private final ConcurrentPublication mockResponsePublication = mock(ConcurrentPublication.class);

    private final ConsensusModule.Context ctx = new ConsensusModule.Context()
        .errorHandler(Throwable::printStackTrace)
        .errorCounter(mock(AtomicCounter.class))
        .moduleStateCounter(mock(Counter.class))
        .controlToggleCounter(mock(Counter.class))
        .clusterNodeCounter(mock(Counter.class))
        .idleStrategySupplier(NoOpIdleStrategy::new)
        .aeron(mockAeron)
        .clusterMemberId(0)
        .serviceHeartbeatCounters(mock(Counter.class))
        .epochClock(new SystemEpochClock())
        .authenticatorSupplier(new DefaultAuthenticatorSupplier())
        .clusterMarkFile(mock(ClusterMarkFile.class))
        .archiveContext(new AeronArchive.Context());

    @Before
    public void before()
    {
        when(mockAeron.conductorAgentInvoker()).thenReturn(mock(AgentInvoker.class));
        when(mockEgressPublisher.sendEvent(any(), any(), any())).thenReturn(TRUE);
        when(mockLogPublisher.appendConnectedSession(any(), anyLong())).thenReturn(128L);
        when(mockLogPublisher.appendClusterAction(any(), anyLong(), anyLong(), anyLong())).thenReturn(TRUE);
        when(mockAeron.addPublication(anyString(), anyInt())).thenReturn(mockResponsePublication);
        when(mockAeron.addSubscription(anyString(), anyInt())).thenReturn(mock(Subscription.class));
        when(mockResponsePublication.isConnected()).thenReturn(TRUE);
    }

    @Test
    public void shouldLimitActiveSessions()
    {
        final CachedEpochClock clock = new CachedEpochClock();
        ctx.maxConcurrentSessions(1);
        ctx.epochClock(clock);

        final SequencerAgent agent = newSequencerAgent();

        final long correlationIdOne = 1L;
        agent.state(ConsensusModule.State.ACTIVE);
        agent.role(Cluster.Role.LEADER);
        agent.commitPositionCounter(mock(Counter.class));
        agent.logRecordingPositionCounter(mock(ReadableCounter.class));
        agent.onSessionConnect(correlationIdOne, 2, RESPONSE_CHANNEL_ONE, new byte[0]);

        clock.update(1);
        agent.doWork();

        verify(mockLogPublisher).appendConnectedSession(any(ClusterSession.class), anyLong());

        final long correlationIdTwo = 2L;
        agent.onSessionConnect(correlationIdTwo, 3, RESPONSE_CHANNEL_TWO, new byte[0]);
        clock.update(2);
        agent.doWork();

        verify(mockEgressPublisher).sendEvent(
            any(ClusterSession.class), eq(EventCode.ERROR), eq(ConsensusModule.Configuration.SESSION_LIMIT_MSG));
    }

    @Test
    public void shouldCloseInactiveSession()
    {
        final CachedEpochClock clock = new CachedEpochClock();
        final long startMs = 7L;
        clock.update(startMs);

        ctx.epochClock(clock);

        final SequencerAgent agent = newSequencerAgent();

        final long correlationId = 1L;
        agent.state(ConsensusModule.State.ACTIVE);
        agent.role(Cluster.Role.LEADER);
        agent.commitPositionCounter(mock(Counter.class));
        agent.logRecordingPositionCounter(mock(ReadableCounter.class));
        agent.onSessionConnect(correlationId, 2, RESPONSE_CHANNEL_ONE, new byte[0]);

        agent.doWork();

        verify(mockLogPublisher).appendConnectedSession(any(ClusterSession.class), eq(startMs));

        final long timeMs = startMs + TimeUnit.NANOSECONDS.toMillis(ConsensusModule.Configuration.sessionTimeoutNs());
        clock.update(timeMs);
        agent.doWork();

        final long timeoutMs = timeMs + 1L;
        clock.update(timeoutMs);
        agent.doWork();

        verify(mockLogPublisher).appendClosedSession(any(ClusterSession.class), eq(timeoutMs));
        verify(mockEgressPublisher).sendEvent(
            any(ClusterSession.class), eq(EventCode.ERROR), eq(ConsensusModule.Configuration.SESSION_TIMEOUT_MSG));
    }

    @Test
    public void shouldSuspendThenResume()
    {
        final CachedEpochClock clock = new CachedEpochClock();

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

        final MutableLong controlValue = new MutableLong(NEUTRAL.code());
        final Counter mockControlToggle = mock(Counter.class);
        when(mockControlToggle.get()).thenAnswer((invocation) -> controlValue.value);
        doAnswer(
            (invocation) ->
            {
                controlValue.value = invocation.getArgument(0);
                return null;
            })
            .when(mockControlToggle).set(anyLong());

        ctx.moduleStateCounter(mockState);
        ctx.controlToggleCounter(mockControlToggle);
        ctx.epochClock(clock);

        final SequencerAgent agent = newSequencerAgent();
        agent.commitPositionCounter(mock(Counter.class));
        agent.logRecordingPositionCounter(mock(ReadableCounter.class));

        assertThat((int)stateValue.get(), is(ConsensusModule.State.INIT.code()));

        agent.state(ConsensusModule.State.ACTIVE);
        agent.role(Cluster.Role.LEADER);
        assertThat((int)stateValue.get(), is(ConsensusModule.State.ACTIVE.code()));

        controlValue.value = SUSPEND.code();
        clock.update(1);
        agent.doWork();

        assertThat((int)stateValue.get(), is(ConsensusModule.State.SUSPENDED.code()));
        assertThat((int)controlValue.get(), is(NEUTRAL.code()));

        controlValue.value = RESUME.code();
        clock.update(2);
        agent.doWork();

        assertThat((int)stateValue.get(), is(ConsensusModule.State.ACTIVE.code()));
        assertThat((int)controlValue.get(), is(NEUTRAL.code()));

        final InOrder inOrder = Mockito.inOrder(mockLogPublisher);
        inOrder.verify(mockLogPublisher).appendClusterAction(
            eq(ClusterAction.SUSPEND), anyLong(), anyLong(), anyLong());
        inOrder.verify(mockLogPublisher).appendClusterAction(
            eq(ClusterAction.RESUME), anyLong(), anyLong(), anyLong());
    }

    private SequencerAgent newSequencerAgent()
    {
        return new SequencerAgent(ctx, mockEgressPublisher, mockLogPublisher);
    }
}