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
import io.aeron.CommonContext;
import io.aeron.ConcurrentPublication;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.UnavailableCounterHandler;
import io.aeron.cluster.ExtendedTerminationHook;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.driver.DutyCycleTracker;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.test.CountersAnswer;
import io.aeron.test.Tests;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.errors.ErrorLogReader;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.AeronCounters.CLUSTER_COMMIT_POSITION_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_RECOVERY_STATE_TYPE_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ClusteredServiceAgentTest
{
    @Test
    void shouldClaimAndWriteToBufferWhenFollower()
    {
        final Aeron aeron = mock(Aeron.class);
        final Publication publication = mock(Publication.class);

        final ClusteredServiceContainer.Context ctx = new ClusteredServiceContainer.Context()
            .aeron(aeron)
            .idleStrategySupplier(() -> YieldingIdleStrategy.INSTANCE);
        final ClusteredServiceAgent clusteredServiceAgent = new ClusteredServiceAgent(ctx);

        final BufferClaim bufferClaim = new BufferClaim();
        final int length = 64;
        final DirectBuffer msg = new UnsafeBuffer(new byte[length]);

        final long position = clusteredServiceAgent.tryClaim(0, publication, length, bufferClaim);
        assertEquals(ClientSession.MOCKED_OFFER, position);

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        buffer.putBytes(bufferClaim.offset() + AeronCluster.SESSION_HEADER_LENGTH, msg, 0, length);
        bufferClaim.commit();
    }

    @Test
    void shouldAbortClusteredServiceIfCommitPositionCounterIsClosed()
    {
        final UnsafeBuffer claimBuffer = new UnsafeBuffer(new byte[64 * 1024]);
        final Aeron aeron = mock(Aeron.class);
        final ConcurrentPublication publication = mock(ConcurrentPublication.class);
        final Subscription subscription = mock(Subscription.class);
        when(aeron.addPublication(any(), anyInt())).thenReturn(publication);
        when(aeron.addSubscription(any(), anyInt())).thenReturn(subscription);
        when(publication.tryClaim(anyInt(), any())).thenAnswer(
            (invocation) ->
            {
                final BufferClaim claim = invocation.getArgument(1, BufferClaim.class);
                claim.wrap(claimBuffer, 0, claimBuffer.capacity());
                return invocation.getArgument(0, Integer.class).longValue();
            });
        final ArgumentCaptor<UnavailableCounterHandler> captor =
            ArgumentCaptor.forClass(UnavailableCounterHandler.class);
        final ClusterMarkFile markFile = mock(ClusterMarkFile.class);
        final CountersManager countersManager = Tests.newCountersManager(64 * 1024);

        when(aeron.addCounter(anyInt(), any(), anyInt(), anyInt(), any(), anyInt(), anyInt()))
            .then(CountersAnswer.mapTo(countersManager));

        RecoveryState.allocate(aeron, NULL_VALUE, 0, 0, 0, 0);

        final int commitPositionCounterId = countersManager.allocate("commit-pos", CLUSTER_COMMIT_POSITION_TYPE_ID);
        final int recoveryStateCounterId = countersManager.allocate("recovery-state", CLUSTER_RECOVERY_STATE_TYPE_ID);
        countersManager.setCounterValue(recoveryStateCounterId, NULL_VALUE);

        when(aeron.countersReader()).thenReturn(countersManager);
        when(aeron.isClosed()).thenReturn(false);

        final CachedNanoClock nanoClock = new CachedNanoClock();
        final ClusteredServiceContainer.Context ctx = new ClusteredServiceContainer.Context()
            .aeron(aeron)
            .nanoClock(nanoClock)
            .epochClock(new CachedEpochClock())
            .clusterMarkFile(markFile)
            .clusteredService(mock(ClusteredService.class))
            .dutyCycleTracker(new DutyCycleTracker())
            .idleStrategySupplier(() -> YieldingIdleStrategy.INSTANCE)
            .errorLog(mock(DistinctErrorLog.class))
            .terminationHook(() -> {})
            .extendedTerminationHook(t -> {});
        final ClusteredServiceAgent clusteredServiceAgent = new ClusteredServiceAgent(ctx);

        clusteredServiceAgent.onStart();

        verify(aeron).addUnavailableCounterHandler(captor.capture());

        clusteredServiceAgent.doWork();

        captor.getValue().onUnavailableCounter(countersManager, 0, commitPositionCounterId);

        nanoClock.advance(TimeUnit.MILLISECONDS.toNanos(2));
        assertThrowsExactly(ClusterTerminationException.class, clusteredServiceAgent::doWork);
    }

    @Test
    void shouldLogErrorInsteadOfThrowingIfSessionIsNotFoundOnClose()
    {
        final Aeron aeron = mock(Aeron.class);
        final DistinctErrorLog distinctErrorLog = new DistinctErrorLog(
            new UnsafeBuffer(ByteBuffer.allocateDirect(16384)), new SystemEpochClock());
        final CountersManager countersManager = Tests.newCountersManager(16 * 1024);
        final AtomicCounter errorCounter = countersManager.newCounter("test");
        final long originalErrorCount = errorCounter.get();

        final ErrorHandler errorHandler = CommonContext.setupErrorHandler(null, distinctErrorLog);
        final CountedErrorHandler countedErrorHandler = new CountedErrorHandler(errorHandler, errorCounter);
        final ClusteredServiceContainer.Context ctx = new ClusteredServiceContainer.Context()
            .aeron(aeron)
            .idleStrategySupplier(() -> YieldingIdleStrategy.INSTANCE)
            .countedErrorHandler(countedErrorHandler);
        final ClusteredServiceAgent clusteredServiceAgent = new ClusteredServiceAgent(ctx);

        clusteredServiceAgent.onSessionClose(99, 999, 9999, 99999, CloseReason.CLIENT_ACTION);

        assertEquals(originalErrorCount + 1, errorCounter.get());
        assertTrue(ErrorLogReader.hasErrors(distinctErrorLog.buffer()));
    }

    @Test
    void shouldRunTerminationHooksWhenTerminating()
    {
        final Aeron aeron = mock(Aeron.class);
        when(aeron.isClosed()).thenReturn(true);

        final Runnable terminationHook = mock(Runnable.class);
        final ExtendedTerminationHook extendedTerminationHook = mock(ExtendedTerminationHook.class);

        final ClusteredServiceContainer.Context ctx = new ClusteredServiceContainer.Context()
            .aeron(aeron)
            .idleStrategySupplier(() -> YieldingIdleStrategy.INSTANCE)
            .nanoClock(SystemNanoClock.INSTANCE)
            .dutyCycleTracker(new DutyCycleTracker())
            .terminationHook(terminationHook)
            .extendedTerminationHook(extendedTerminationHook);

        final ClusteredServiceAgent clusteredServiceAgent = new ClusteredServiceAgent(ctx);

        final AgentTerminationException terminationException =
            assertThrows(AgentTerminationException.class, clusteredServiceAgent::doWork);

        final InOrder inOrder = inOrder(terminationHook, extendedTerminationHook);
        inOrder.verify(extendedTerminationHook).run(terminationException);
        inOrder.verify(terminationHook).run();
    }
}
