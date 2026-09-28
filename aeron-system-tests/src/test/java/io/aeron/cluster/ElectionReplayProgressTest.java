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

import io.aeron.cluster.service.ClientSession;
import io.aeron.logbuffer.Header;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import net.bytebuddy.asm.Advice;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static io.aeron.cluster.ElectionState.CANVASS;
import static io.aeron.cluster.ElectionState.FOLLOWER_REPLAY;
import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A restarted follower replays a real committed prefix while a recorded tail remains uncommitted. The returning
 * leader then wins a newer ballot, requiring this follower's recorded-position report to commit that tail.
 */
@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class ElectionReplayProgressTest
{
    static final int BASELINE = 1;
    static final int TAIL = 2;
    static final int RECOVERY = 4;
    static volatile boolean holdCommit;
    static volatile boolean dropReports;
    static volatile boolean partialReplayDone;
    static volatile boolean replayTimedOut;
    static volatile long oldTerm;
    static volatile long prefix;
    static volatile long tailPosition;
    static volatile ReplaySample replaySample;
    static final ThreadLocal<Boolean> REPLAY_REPORT = new ThreadLocal<>();
    private static final Class<?>[] INSTRUMENTED_CLASSES =
        { ConsensusModuleAgent.class, Election.class, LogPublisher.class, ConsensusPublisher.class };
    private static ClusterInstrumentor[] instrumentors;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();
    private final AtomicInteger acknowledged = new AtomicInteger();

    @BeforeAll
    static void beforeAll()
    {
        assertEquals(4, INSTRUMENTED_CLASSES.length);
        instrumentors = new ClusterInstrumentor[]
        {
            new ClusterInstrumentor(HoldCommitWork.class, "ConsensusModuleAgent", "updateLeaderPosition"),
            new ClusterInstrumentor(DropReports.class, "ConsensusModuleAgent", "onAppendPosition"),
            new ClusterInstrumentor(DropOldAnnouncement.class, "ConsensusModuleAgent", "onNewLeadershipTerm"),
            new ClusterInstrumentor(ObserveState.class, "Election", "state"),
            new ClusterInstrumentor(ObserveReplay.class, "Election", "followerReplay"),
            new ClusterInstrumentor(ObserveReport.class, "ConsensusPublisher", "appendPosition"),
            new ClusterInstrumentor(ObserveTail.class, "LogPublisher", "appendMessage")
        };
    }

    @AfterAll
    static void afterAll()
    {
        holdCommit = false;
        dropReports = false;
        partialReplayDone = false;
        for (final ClusterInstrumentor instrumentor : instrumentors)
        {
            instrumentor.reset();
        }
    }

    @Test
    @InterruptAfter(90)
    void shouldReportRecordedTailAfterRealPartialReplay()
    {
        runScenario(false);
    }

    @Test
    @InterruptAfter(90)
    void shouldLeaveReplayWaitWhenLeaderDisappears()
    {
        runScenario(true);
    }

    @SuppressWarnings("MethodLength")
    private void runScenario(final boolean loseLeader)
    {
        holdCommit = false;
        dropReports = false;
        partialReplayDone = false;
        replayTimedOut = false;
        tailPosition = 0;
        replaySample = null;
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withAppointedLeader(0)
            .withLeaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(3))
            .withStartupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(6))
            .withSessionTimeoutNs(TimeUnit.SECONDS.toNanos(60))
            .withElectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(200))
            .withElectionStatusIntervalNs(TimeUnit.MILLISECONDS.toNanos(10))
            .withServiceSupplier(index -> new TestNode.TestService[]{ new TrackingService().index(index) })
            .start();
        systemTestWatcher.cluster(cluster);
        systemTestWatcher.ignoreErrorsMatching(s -> s.contains("unexpected vote request") ||
            s.contains("unexpected new leadership term") ||
            s.contains("timeout awaiting commit position during replay") || s.contains("heartbeat timeout"));
        cluster.egressListener((sessionId, timestamp, buffer, offset, length, header) ->
            acknowledged.getAndUpdate(value -> value | buffer.getInt(offset)));
        try
        {
            final TestNode leader = cluster.awaitLeader();
            assertEquals(0, leader.memberId());
            cluster.connectClient();
            send(cluster, BASELINE);
            await(cluster, "baseline applied", () -> hasAcknowledged(BASELINE) && applied(cluster.node(1), BASELINE));
            oldTerm = leader.consensusModule().context().recordingLog().findLastTerm().leadershipTermId;

            // Pause the old leader's commit-processing work, allowing real archive recording and consensus
            // reception to continue. Its actual unchanged committed prefix is advertised to the restarting voter.
            // This finite scheduling pause supplies no invented log position, notification, or election field.
            holdCommit = true;
            prefix = leader.commitPosition();
            cluster.stopNode(cluster.node(2));
            send(cluster, TAIL);
            await(cluster, "uncommitted tail recorded on both members", () -> tailPosition > prefix &&
                leader.appendPosition() >= tailPosition && cluster.node(1).appendPosition() >= tailPosition);
            assertFalse(hasAcknowledged(TAIL));
            cluster.stopNode(cluster.node(1));
            cluster.startStaticNode(1, false);
            await(cluster, "partial archive replay applied by service", () ->
                partialReplayDone && applied(cluster.node(1), BASELINE));
            assertFalse(applied(cluster.node(1), TAIL));

            // Drop subsequent announcements from the old term after partial replay, retaining its nonzero
            // commit notification across CANVASS. The appointed leader returns and wins a genuine newer ballot.
            cluster.stopNode(leader);
            cluster.closeClient();
            holdCommit = false;
            dropReports = loseLeader;
            cluster.startStaticNode(0, false);
            await(null, "new term replay waiting on retained commit notification", () -> null != replaySample);
            final ReplaySample sample = replaySample;
            System.out.println("partial-replay evidence: " + sample + " loseLeader=" + loseLeader);
            assertEquals(prefix, sample.appliedPosition);
            assertEquals(prefix, sample.notifiedPosition);
            assertTrue(sample.recordedPosition >= tailPosition && sample.acceptedTerm > oldTerm);
            assertTrue(sample.reportSent && sample.nextState == FOLLOWER_REPLAY,
                "PARTIAL_REPLAY_REPORT: follower abandoned replay without publishing its recorded tail");

            if (loseLeader)
            {
                replayTimedOut = false;
                cluster.stopNode(cluster.node(0));
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(7);
                while (!replayTimedOut && System.nanoTime() < deadline)
                {
                    Tests.yield();
                }
                System.out.println("replay-timeout evidence: timedOut=" + replayTimedOut +
                    " state=" + cluster.node(1).electionState());
                assertTrue(replayTimedOut,
                    "REPLAY_COMMIT_TIMEOUT: follower stayed in replay wait after its leader disappeared");
                await(null, "canvass after replay timeout", () -> cluster.node(1).electionState() == CANVASS);
                dropReports = false;
                cluster.startStaticNode(0, false);
            }

            cluster.awaitLeader();
            cluster.connectClient();
            send(cluster, RECOVERY);
            await(cluster, "client service restored", () -> hasAcknowledged(RECOVERY) &&
                applied(cluster.node(0), RECOVERY) && applied(cluster.node(1), RECOVERY));
            assertTrue(applied(cluster.node(1), TAIL));
        }
        finally
        {
            holdCommit = false;
            dropReports = false;
            partialReplayDone = false;
        }
    }

    private static void send(final TestCluster cluster, final int identity)
    {
        cluster.msgBuffer().putInt(0, identity);
        cluster.pollUntilMessageSent(Integer.BYTES);
    }

    private boolean hasAcknowledged(final int identity)
    {
        return (acknowledged.get() & identity) != 0;
    }

    private static boolean applied(final TestNode node, final int identity)
    {
        return (((TrackingService)node.service()).identities & identity) != 0;
    }

    private static void await(final TestCluster cluster, final String description, final BooleanSupplier condition)
    {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
        while (!condition.getAsBoolean())
        {
            if (null != cluster)
            {
                cluster.client().pollEgress();
                cluster.client().sendKeepAlive();
            }
            Tests.yield();
            assertTrue(System.nanoTime() < deadline, "fixture timed out: " + description +
                " prefix=" + prefix + " tail=" + tailPosition + " partial=" + partialReplayDone);
        }
    }

    public static class TrackingService extends TestNode.TestService
    {
        volatile int identities;

        public void onSessionMessage(
            final ClientSession session, final long timestamp, final DirectBuffer buffer,
            final int offset, final int length, final Header header)
        {
            identities |= buffer.getInt(offset);
            super.onSessionMessage(session, timestamp, buffer, offset, length, header);
        }
    }

    record ReplaySample(
        long acceptedTerm, long appliedPosition, long notifiedPosition, long recordedPosition,
        boolean reportSent, ElectionState nextState)
    {
    }

    public static class HoldCommitWork
    {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        static boolean enter(@Advice.FieldValue("memberId") final int memberId)
        {
            return holdCommit && memberId == 0;
        }
    }

    public static class DropReports
    {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        static boolean enter(@Advice.FieldValue("memberId") final int memberId)
        {
            return dropReports && memberId == 0;
        }
    }

    public static class DropOldAnnouncement
    {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        static boolean enter(
            @Advice.FieldValue("memberId") final int memberId,
            @Advice.Argument(4) final long term)
        {
            return partialReplayDone && memberId == 1 && term <= oldTerm;
        }
    }

    public static class ObserveState
    {
        @Advice.OnMethodEnter
        static void enter(
            @Advice.Argument(0) final ElectionState nextState,
            @Advice.Argument(2) final String reason,
            @Advice.FieldValue("state") final ElectionState state,
            @Advice.FieldValue("logPosition") final long logPosition,
            @Advice.FieldValue("appendPosition") final long appendPosition,
            @Advice.This final Election election)
        {
            if (election.thisMemberId() == 1)
            {
                if (nextState == CANVASS && state == FOLLOWER_REPLAY &&
                    logPosition == prefix && appendPosition > logPosition)
                {
                    partialReplayDone = true;
                }
                if (reason.contains("timeout awaiting commit position during replay"))
                {
                    replayTimedOut = true;
                }
            }
        }
    }

    public static class ObserveReplay
    {
        @Advice.OnMethodEnter
        static ReplaySample enter(
            @Advice.FieldValue("logPosition") final long logPosition,
            @Advice.FieldValue("appendPosition") final long appendPosition,
            @Advice.FieldValue("notifiedCommitPosition") final long notified,
            @Advice.This final Election election)
        {
            if (election.thisMemberId() == 1 && election.leadershipTermId() > oldTerm &&
                logPosition == prefix && notified == prefix && appendPosition > logPosition)
            {
                REPLAY_REPORT.set(false);
                return new ReplaySample(election.leadershipTermId(), logPosition, notified, appendPosition,
                    false, FOLLOWER_REPLAY);
            }
            return null;
        }

        @Advice.OnMethodExit(onThrowable = Throwable.class)
        static void exit(
            @Advice.Enter final ReplaySample entry,
            @Advice.FieldValue("state") final ElectionState state)
        {
            if (null != entry && null == replaySample &&
                (Boolean.TRUE.equals(REPLAY_REPORT.get()) || state == CANVASS))
            {
                replaySample = new ReplaySample(entry.acceptedTerm(), entry.appliedPosition(),
                    entry.notifiedPosition(), entry.recordedPosition(),
                    Boolean.TRUE.equals(REPLAY_REPORT.get()), state);
            }
            REPLAY_REPORT.remove();
        }
    }

    public static class ObserveReport
    {
        @Advice.OnMethodExit
        static void exit(@Advice.Return final boolean sent)
        {
            if (sent && null != REPLAY_REPORT.get())
            {
                REPLAY_REPORT.set(true);
            }
        }
    }

    public static class ObserveTail
    {
        @Advice.OnMethodExit
        static void exit(
            @Advice.Argument(3) final DirectBuffer buffer,
            @Advice.Argument(4) final int offset,
            @Advice.Return final long position)
        {
            if (position > 0 && buffer.getInt(offset) == TAIL)
            {
                tailPosition = position;
            }
        }
    }
}
