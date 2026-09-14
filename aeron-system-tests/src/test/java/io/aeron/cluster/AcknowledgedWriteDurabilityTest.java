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

import io.aeron.Counter;
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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.cluster.ElectionState.CANVASS;
import static io.aeron.cluster.ElectionState.CLOSED;
import static io.aeron.cluster.ElectionState.INIT;
import static io.aeron.cluster.ElectionState.LEADER_LOG_REPLICATION;
import static io.aeron.test.cluster.TestCluster.aCluster;
import static io.aeron.test.cluster.TestCluster.awaitElectionClosed;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A member that has voted in a newer leadership term must not resume following the leader of an older term. If it
 * does, the old leader regains a quorum and acknowledges writes which the newer term then truncates.
 * <p>
 * Schedule: the two followers restart and elect a newer term that the old leader never hears about, the winner is
 * held before it publishes its term, and the old leader then answers the other voter's canvass with its old term.
 */
@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class AcknowledgedWriteDurabilityTest
{
    private static final int MEMBER_COUNT = 3;
    private static final int BASELINE_WRITE = 1;
    private static final int TARGET_WRITE = 2;
    private static final int RECOVERY_WRITE = 4;
    private static final int TWO_NODE_WRITE = 8;
    private static final int STALE_TERMS_WITHOUT_QUORUM = 100;
    private static final long LEADER_HEARTBEAT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    static final int PHASE_IDLE = 0;
    /** Old-term leadership messages are dropped everywhere and vote requests are dropped at the old leader. */
    static final int PHASE_ISOLATE_OLD_LEADER = 1;
    /** Old-term leadership messages flow again; vote requests are still dropped at the old leader. */
    static final int PHASE_STALE_LEADER_VISIBLE = 2;
    static final int PHASE_HEALED = 3;

    static volatile int phase = PHASE_IDLE;
    static volatile int oldLeaderId = NULL_VALUE;
    static volatile long oldTerm = NULL_VALUE;
    static volatile boolean releaseNewLeader;
    static final AtomicInteger NEW_LEADER_ID = new AtomicInteger(NULL_VALUE);
    static final AtomicInteger STALE_LEADERSHIP_TERMS = new AtomicInteger();
    static final Set<Integer> VOTERS_IN_NEWER_TERM = ConcurrentHashMap.newKeySet();
    static final Set<Integer> STALE_LEADERSHIP_TERM_RECEIVERS = ConcurrentHashMap.newKeySet();
    static final Set<Integer> LEADERSHIP_TERM_REGRESSIONS = ConcurrentHashMap.newKeySet();
    static final Map<Integer, Long> ACCEPTED_LEADERSHIP_TERMS = new ConcurrentHashMap<>();
    static final Map<Integer, Long> LEADER_TERMS = new ConcurrentHashMap<>();
    static final Set<Integer> TRUNCATED = ConcurrentHashMap.newKeySet();
    static final Set<Integer> TRUNCATED_BELOW_COMMIT_POSITION = ConcurrentHashMap.newKeySet();

    // ClusterInstrumentor retransforms loaded classes; advice on a class that is not yet loaded was observed not to
    // apply. Election is loaded by the advice signatures below, the agent only by this reference.
    private static final Class<?>[] INSTRUMENTED_CLASSES = { ConsensusModuleAgent.class, Election.class };
    private static ClusterInstrumentor[] instrumentors;

    @BeforeAll
    static void beforeAll()
    {
        assertEquals(2, INSTRUMENTED_CLASSES.length);
        instrumentors = new ClusterInstrumentor[]
        {
            new ClusterInstrumentor(DropOldTermLeadership.class, "ConsensusModuleAgent", "onNewLeadershipTerm"),
            new ClusterInstrumentor(DropVoteRequestsAtOldLeader.class, "ConsensusModuleAgent", "onRequestVote"),
            new ClusterInstrumentor(ObserveTruncation.class, "ConsensusModuleAgent", "truncateLogEntry"),
            new ClusterInstrumentor(ObserveVote.class, "Election", "placeVote"),
            new ClusterInstrumentor(ObserveLeadershipTerm.class, "Election", "onNewLeadershipTerm"),
            new ClusterInstrumentor(HoldNewLeader.class, "Election", "state"),
        };
    }

    @AfterAll
    static void afterAll()
    {
        for (final ClusterInstrumentor instrumentor : instrumentors)
        {
            instrumentor.reset();
        }
    }

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private final AtomicInteger acknowledgedWrites = new AtomicInteger();

    @Test
    @InterruptAfter(120)
    void shouldRetainEveryAcknowledgedWriteAfterVoterHearsOldLeaderAgain()
    {
        final TestCluster cluster = startCluster();
        try
        {
            final TestNode oldLeader = awaitBaselineWrite(cluster);
            final int otherVoterId = electNewerTermBehindOldLeader(cluster, oldLeader);
            awaitOldLeaderHeardByCanvassingVoter(cluster, otherVoterId);
            final boolean targetAcknowledged = sendTargetWriteToOldLeader(cluster);
            cluster.closeClient();

            phase = PHASE_HEALED;
            releaseNewLeader = true;
            awaitNewTermReachesEveryNode(cluster);

            recoverFromRecordings(cluster);
            assertRecoveredState(cluster, targetAcknowledged);
        }
        finally
        {
            releaseNewLeader = true;
            phase = PHASE_IDLE;
        }
    }

    /**
     * Liveness counterpart: the winner of the newer term dies before publishing it. The old leader and the other
     * voter are a quorum and must elect a newer term and serve clients again without the dead member.
     */
    @Test
    @InterruptAfter(120)
    void shouldElectNewerTermWhenItsWinnerDiesBeforePublishingIt()
    {
        final TestCluster cluster = startCluster();
        try
        {
            final TestNode oldLeader = awaitBaselineWrite(cluster);
            final int otherVoterId = electNewerTermBehindOldLeader(cluster, oldLeader);
            awaitOldLeaderHeardByCanvassingVoter(cluster, otherVoterId);

            phase = PHASE_HEALED;
            final int deadMemberId = NEW_LEADER_ID.get();
            cluster.stopNode(cluster.node(deadMemberId));
            releaseNewLeader = true;

            // A write appended to the stale leader can survive replay or be discarded by the next election.
            // Its fate is not asserted.
            sendWrite(cluster, TARGET_WRITE);
            final long deadlineNs = System.nanoTime() + 5 * LEADER_HEARTBEAT_TIMEOUT_NS;
            while (!hasNewerTermLeader(cluster, deadMemberId) && System.nanoTime() < deadlineNs)
            {
                keepClientAlive(cluster);
                Tests.yield();
            }
            assertTrue(
                hasNewerTermLeader(cluster, deadMemberId),
                "two remaining members never elected a newer term after its winner died: " + diagnostics(cluster));
            sendWrite(cluster, TWO_NODE_WRITE);
            awaitAcknowledgement(cluster, TWO_NODE_WRITE);
            cluster.closeClient();

            cluster.startStaticNode(deadMemberId, false);
            awaitAllElectionsClosed(cluster);
            cluster.connectClient();
            sendWrite(cluster, RECOVERY_WRITE);
            awaitAcknowledgement(cluster, RECOVERY_WRITE);
            awaitApplied(cluster, RECOVERY_WRITE);

            assertAll(
                () -> assertEquals(
                    Collections.emptySet(),
                    LEADERSHIP_TERM_REGRESSIONS,
                    "members that followed a leadership term below their candidate term"),
                () -> assertEquals(
                    Collections.emptySet(),
                    TRUNCATED_BELOW_COMMIT_POSITION,
                    "members that truncated their log below their commit position"),
                () -> assertConsistentServices(cluster));
        }
        finally
        {
            releaseNewLeader = true;
            phase = PHASE_IDLE;
        }
    }

    private TestCluster startCluster()
    {
        resetFaults();
        final TestCluster cluster = aCluster()
            .withStaticNodes(MEMBER_COUNT)
            .withLeaderHeartbeatTimeoutNs(LEADER_HEARTBEAT_TIMEOUT_NS)
            .withStartupCanvassTimeoutNs(2 * LEADER_HEARTBEAT_TIMEOUT_NS)
            .withElectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(200))
            .withElectionStatusIntervalNs(TimeUnit.MILLISECONDS.toNanos(10))
            .withServiceSupplier(index -> new TestNode.TestService[]{ new WriteTrackingService().index(index) })
            .start();
        systemTestWatcher.cluster(cluster);
        systemTestWatcher.ignoreErrorsMatching(s -> s.contains("Truncating Cluster Log") ||
            s.contains("less than logPosition") || s.contains("quorum position went backwards") ||
            s.contains("unexpected vote request") || s.contains("unexpected new leadership term"));
        cluster.egressListener((clusterSessionId, timestamp, buffer, offset, length, header) ->
            acknowledgedWrites.getAndUpdate(writes -> writes | buffer.getInt(offset)));

        return cluster;
    }

    // Returns the settled leader, having recorded its member id and term for the fault advice.
    private TestNode awaitBaselineWrite(final TestCluster cluster)
    {
        cluster.awaitLeader();
        cluster.connectClient();
        sendWrite(cluster, BASELINE_WRITE);
        awaitAcknowledgement(cluster, BASELINE_WRITE);
        cluster.awaitServicesMessageCount(1);

        // A member that lost a split startup ballot re-nominates at once, so settle on a quiet period first.
        awaitStableLeadership(cluster);
        final TestNode oldLeader = cluster.awaitLeader();
        oldLeaderId = oldLeader.memberId();
        Tests.await(() ->
        {
            keepClientAlive(cluster);
            return oldLeaderId == cluster.client().leaderMemberId();
        });
        oldTerm = LEADER_TERMS.get(oldLeaderId);

        return oldLeader;
    }

    // Both followers restart from their recordings and elect a newer term that the old leader never hears about.
    // The winner is held before it publishes its term. Returns the member that voted for the new leader.
    private static int electNewerTermBehindOldLeader(final TestCluster cluster, final TestNode oldLeader)
    {
        phase = PHASE_ISOLATE_OLD_LEADER;
        // One follower at a time, so the old leader always sees an active quorum and never steps down.
        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            if (i != oldLeader.memberId())
            {
                cluster.stopNode(cluster.node(i));
                final TestNode restarted = cluster.startStaticNode(i, false);
                awaitWithDiagnostics(cluster, "restarted follower canvassing", () ->
                    CANVASS == restarted.electionState());
            }
        }
        awaitWithDiagnostics(cluster, "newer-term leader held", () -> NULL_VALUE != NEW_LEADER_ID.get());

        final int otherVoterId = otherMemberId(oldLeaderId, NEW_LEADER_ID.get());
        assertTrue(VOTERS_IN_NEWER_TERM.contains(otherVoterId), "voters in the newer term: " + VOTERS_IN_NEWER_TERM);

        return otherVoterId;
    }

    // The old leader answers the voter's canvass with its old term while the voter canvasses with a higher candidate
    // term. A correct voter ignores the old term, leaving the old leader without a quorum.
    private static void awaitOldLeaderHeardByCanvassingVoter(final TestCluster cluster, final int voterId)
    {
        phase = PHASE_STALE_LEADER_VISIBLE;
        final TestNode voter = cluster.node(voterId);
        awaitWithDiagnostics(cluster, "old leader heard by voter", 5, () ->
            STALE_LEADERSHIP_TERM_RECEIVERS.contains(voterId) &&
            (CANVASS == voter.electionState() || LEADERSHIP_TERM_REGRESSIONS.contains(voterId)));
    }

    // Send a write to the old leader and poll for its acknowledgement until it arrives, or until the voter has
    // ignored enough further old-term messages to show the old leader will not regain a quorum.
    private boolean sendTargetWriteToOldLeader(final TestCluster cluster)
    {
        sendWrite(cluster, TARGET_WRITE);
        final int staleTermsAtSend = STALE_LEADERSHIP_TERMS.get();
        final long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!isAcknowledged(TARGET_WRITE) && System.nanoTime() < deadlineNs)
        {
            if (LEADERSHIP_TERM_REGRESSIONS.isEmpty() &&
                STALE_LEADERSHIP_TERMS.get() - staleTermsAtSend >= STALE_TERMS_WITHOUT_QUORUM)
            {
                break;
            }
            keepClientAlive(cluster);
            Tests.yield();
        }

        return isAcknowledged(TARGET_WRITE);
    }

    // Once the held leader publishes its term, every other member either adopts it or truncates its log to the new
    // term's base position. A member that truncated below its commit position cannot rejoin the live log, so the
    // cluster is not awaited any further: the recordings are already in their final shape.
    private static void awaitNewTermReachesEveryNode(final TestCluster cluster)
    {
        awaitWithDiagnostics(cluster, "new term reaches every node", () ->
        {
            for (int i = 0; i < MEMBER_COUNT; i++)
            {
                if (i != NEW_LEADER_ID.get() && !TRUNCATED.contains(i) &&
                    ACCEPTED_LEADERSHIP_TERMS.getOrDefault(i, oldTerm) <= oldTerm)
                {
                    return false;
                }
            }
            return true;
        });
    }

    private void recoverFromRecordings(final TestCluster cluster)
    {
        cluster.stopAllNodes();
        cluster.restartAllNodes(false);
        cluster.awaitLeader();
        awaitAllElectionsClosed(cluster);
        cluster.connectClient();
        sendWrite(cluster, RECOVERY_WRITE);
        awaitAcknowledgement(cluster, RECOVERY_WRITE);
        awaitApplied(cluster, RECOVERY_WRITE);
    }

    private static void assertRecoveredState(final TestCluster cluster, final boolean targetAcknowledged)
    {
        assertAll(
            () -> assertEquals(
                Collections.emptySet(),
                LEADERSHIP_TERM_REGRESSIONS,
                "members that followed a leadership term below their candidate term"),
            () -> assertEquals(
                Collections.emptySet(),
                TRUNCATED_BELOW_COMMIT_POSITION,
                "members that truncated their log below their commit position"),
            () ->
            {
                for (int i = 0; i < MEMBER_COUNT; i++)
                {
                    final TestNode node = cluster.node(i);
                    assertTrue(hasApplied(node, BASELINE_WRITE), "baseline write missing on node " + i);
                    if (targetAcknowledged)
                    {
                        assertTrue(
                            hasApplied(node, TARGET_WRITE),
                            "write acknowledged to the client is missing after recovery on node " + i +
                            ", members that truncated below their commit position: " +
                            TRUNCATED_BELOW_COMMIT_POSITION);
                    }
                }
            });
    }

    private static void assertConsistentServices(final TestCluster cluster)
    {
        final int expected = ((WriteTrackingService)cluster.node(0).service()).writes;
        for (int i = 1; i < MEMBER_COUNT; i++)
        {
            assertEquals(expected, ((WriteTrackingService)cluster.node(i).service()).writes,
                "service state on node " + i + " differs from node 0");
        }
    }

    // The remaining members have closed an election for a term newer than the one the dead winner interrupted.
    private static boolean hasNewerTermLeader(final TestCluster cluster, final int deadMemberId)
    {
        boolean newerTerm = false;
        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            if (i != deadMemberId)
            {
                if (CLOSED != cluster.node(i).electionState())
                {
                    return false;
                }
                newerTerm |= LEADER_TERMS.getOrDefault(i, oldTerm) > oldTerm;
            }
        }
        return newerTerm;
    }

    private static void resetFaults()
    {
        phase = PHASE_IDLE;
        oldLeaderId = NULL_VALUE;
        oldTerm = NULL_VALUE;
        releaseNewLeader = false;
        NEW_LEADER_ID.set(NULL_VALUE);
        STALE_LEADERSHIP_TERMS.set(0);
        VOTERS_IN_NEWER_TERM.clear();
        STALE_LEADERSHIP_TERM_RECEIVERS.clear();
        LEADERSHIP_TERM_REGRESSIONS.clear();
        ACCEPTED_LEADERSHIP_TERMS.clear();
        LEADER_TERMS.clear();
        TRUNCATED.clear();
        TRUNCATED_BELOW_COMMIT_POSITION.clear();
    }

    private static int otherMemberId(final int memberIdA, final int memberIdB)
    {
        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            if (i != memberIdA && i != memberIdB)
            {
                return i;
            }
        }
        throw new IllegalStateException("memberIdA=" + memberIdA + " memberIdB=" + memberIdB);
    }

    private static void awaitStableLeadership(final TestCluster cluster)
    {
        final long[] electionCounts = new long[MEMBER_COUNT];
        while (true)
        {
            awaitAllElectionsClosed(cluster);
            for (int i = 0; i < MEMBER_COUNT; i++)
            {
                electionCounts[i] = cluster.node(i).electionCount();
            }

            final long quietUntilNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            boolean stable = true;
            while (stable && System.nanoTime() < quietUntilNs)
            {
                keepClientAlive(cluster);
                Tests.yield();
                for (int i = 0; i < MEMBER_COUNT; i++)
                {
                    final TestNode node = cluster.node(i);
                    stable &= electionCounts[i] == node.electionCount() && CLOSED == node.electionState();
                }
            }
            if (stable)
            {
                return;
            }
        }
    }

    private static void awaitAllElectionsClosed(final TestCluster cluster)
    {
        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            awaitElectionClosed(cluster.node(i));
        }
    }

    private static void awaitWithDiagnostics(
        final TestCluster cluster, final String what, final BooleanSupplier condition)
    {
        awaitWithDiagnostics(cluster, what, 30, condition);
    }

    private static void awaitWithDiagnostics(
        final TestCluster cluster, final String what, final int timeoutSeconds, final BooleanSupplier condition)
    {
        final long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (!condition.getAsBoolean())
        {
            keepClientAlive(cluster);
            Tests.yield();
            if (System.nanoTime() >= deadlineNs)
            {
                throw new IllegalStateException("timed out awaiting " + what + ": " + diagnostics(cluster));
            }
        }
    }

    private static String diagnostics(final TestCluster cluster)
    {
        final StringBuilder sb = new StringBuilder()
            .append("oldLeader=").append(oldLeaderId).append(" oldTerm=").append(oldTerm)
            .append(" newLeader=").append(NEW_LEADER_ID.get())
            .append(" voters=").append(VOTERS_IN_NEWER_TERM)
            .append(" staleTerms=").append(STALE_LEADERSHIP_TERMS.get())
            .append(" staleReceivers=").append(STALE_LEADERSHIP_TERM_RECEIVERS)
            .append(" regressions=").append(LEADERSHIP_TERM_REGRESSIONS)
            .append(" accepted=").append(ACCEPTED_LEADERSHIP_TERMS)
            .append(" leaderTerms=").append(LEADER_TERMS)
            .append(" truncated=").append(TRUNCATED)
            .append(" truncatedBelowCommit=").append(TRUNCATED_BELOW_COMMIT_POSITION);
        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            final TestNode node = cluster.node(i);
            sb.append(" | node").append(i);
            if (null == node || node.isClosed())
            {
                sb.append(" closed");
                continue;
            }
            sb.append(' ').append(node.electionState()).append(' ').append(node.role())
                .append(" commit=").append(node.commitPosition()).append(" elections=").append(node.electionCount());
        }
        return sb.toString();
    }

    private static void sendWrite(final TestCluster cluster, final int write)
    {
        cluster.msgBuffer().putInt(0, write);
        cluster.pollUntilMessageSent(Integer.BYTES);
    }

    private static void keepClientAlive(final TestCluster cluster)
    {
        cluster.client().pollEgress();
        cluster.client().sendKeepAlive();
    }

    private boolean isAcknowledged(final int write)
    {
        return (acknowledgedWrites.get() & write) != 0;
    }

    private void awaitAcknowledgement(final TestCluster cluster, final int write)
    {
        Tests.await(() ->
        {
            keepClientAlive(cluster);
            return isAcknowledged(write);
        });
    }

    private static void awaitApplied(final TestCluster cluster, final int write)
    {
        Tests.await(() ->
        {
            keepClientAlive(cluster);
            for (int i = 0; i < MEMBER_COUNT; i++)
            {
                if (!hasApplied(cluster.node(i), write))
                {
                    return false;
                }
            }
            return true;
        });
    }

    private static boolean hasApplied(final TestNode node, final int write)
    {
        return (((WriteTrackingService)node.service()).writes & write) != 0;
    }

    public static class WriteTrackingService extends TestNode.TestService
    {
        volatile int writes;

        public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            writes |= buffer.getInt(offset);
            super.onSessionMessage(session, timestamp, buffer, offset, length, header);
        }
    }

    public static class DropOldTermLeadership
    {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        static boolean skip(@Advice.Argument(4) final long leadershipTermId)
        {
            return PHASE_ISOLATE_OLD_LEADER == phase && leadershipTermId == oldTerm;
        }
    }

    public static class DropVoteRequestsAtOldLeader
    {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        static boolean skip(@Advice.FieldValue("memberId") final int memberId)
        {
            final int phase = AcknowledgedWriteDurabilityTest.phase;
            return (PHASE_ISOLATE_OLD_LEADER == phase || PHASE_STALE_LEADER_VISIBLE == phase) &&
                memberId == oldLeaderId;
        }
    }

    public static class ObserveVote
    {
        @Advice.OnMethodEnter
        static void placeVote(
            @Advice.Argument(0) final long candidateTermId,
            @Advice.Argument(2) final boolean vote,
            @Advice.This final Election election)
        {
            if (PHASE_IDLE != phase && vote && candidateTermId > oldTerm)
            {
                VOTERS_IN_NEWER_TERM.add(election.thisMemberId());
            }
        }
    }

    public static class ObserveLeadershipTerm
    {
        @Advice.OnMethodEnter
        static long candidateTermIdBefore(@Advice.FieldValue("candidateTermId") final long candidateTermId)
        {
            return candidateTermId;
        }

        @Advice.OnMethodExit(onThrowable = Throwable.class)
        static void onNewLeadershipTerm(
            @Advice.Argument(4) final long leadershipTermId,
            @Advice.Argument(10) final int leaderMemberId,
            @Advice.Enter final long candidateTermIdBefore,
            @Advice.FieldValue("state") final ElectionState state,
            @Advice.FieldValue("leadershipTermId") final long electionLeadershipTermId,
            @Advice.FieldValue("leaderMember") final ClusterMember leaderMember,
            @Advice.This final Election election)
        {
            if (PHASE_IDLE != phase && INIT != state && leadershipTermId < candidateTermIdBefore)
            {
                STALE_LEADERSHIP_TERMS.incrementAndGet();
                STALE_LEADERSHIP_TERM_RECEIVERS.add(election.thisMemberId());
            }
            if (PHASE_IDLE != phase && electionLeadershipTermId == leadershipTermId &&
                null != leaderMember && leaderMember.id() == leaderMemberId)
            {
                // no lambdas or method references: this code is inlined into Election.
                final Long accepted = ACCEPTED_LEADERSHIP_TERMS.get(election.thisMemberId());
                if (null == accepted || accepted < leadershipTermId)
                {
                    ACCEPTED_LEADERSHIP_TERMS.put(election.thisMemberId(), leadershipTermId);
                }
                if (leadershipTermId < candidateTermIdBefore)
                {
                    LEADERSHIP_TERM_REGRESSIONS.add(election.thisMemberId());
                }
            }
        }
    }

    /**
     * Records the term of every member that wins an election and, during the isolation phase, holds the winner on
     * the transition into {@code LEADER_LOG_REPLICATION}: before it has a log publication or the leader role, so it
     * cannot answer a canvass with its new term. Holding the consensus thread beyond the Aeron client's inter-service
     * timeout (the driver's client liveness timeout, 10s by default) terminates the node, so every wait that
     * overlaps the hold is bounded well below that.
     */
    public static class HoldNewLeader
    {
        @Advice.OnMethodEnter
        static void state(
            @Advice.Argument(0) final ElectionState newState,
            @Advice.FieldValue("state") final ElectionState state,
            @Advice.FieldValue("leadershipTermId") final long leadershipTermId,
            @Advice.This final Election election)
        {
            if (LEADER_LOG_REPLICATION == newState && LEADER_LOG_REPLICATION != state)
            {
                LEADER_TERMS.put(election.thisMemberId(), leadershipTermId);
                if (PHASE_ISOLATE_OLD_LEADER == phase &&
                    election.thisMemberId() != oldLeaderId &&
                    leadershipTermId > oldTerm)
                {
                    NEW_LEADER_ID.compareAndSet(NULL_VALUE, election.thisMemberId());
                    while (!releaseNewLeader && !Thread.currentThread().isInterrupted())
                    {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                }
            }
        }
    }

    public static class ObserveTruncation
    {
        @Advice.OnMethodExit
        static void truncateLogEntry(
            @Advice.Argument(1) final long logPosition,
            @Advice.FieldValue("commitPosition") final Counter commitPosition,
            @Advice.FieldValue("memberId") final int memberId)
        {
            TRUNCATED.add(memberId);
            if (logPosition < commitPosition.getPlain())
            {
                TRUNCATED_BELOW_COMMIT_POSITION.add(memberId);
            }
        }
    }
}
