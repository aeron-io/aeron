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

import io.aeron.Subscription;
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.cluster.ElectionState.CANVASS;
import static io.aeron.cluster.ElectionState.CLOSED;
import static io.aeron.cluster.ElectionState.FOLLOWER_CATCHUP;
import static io.aeron.cluster.ElectionState.FOLLOWER_READY;
import static io.aeron.cluster.ElectionState.LEADER_LOG_REPLICATION;
import static io.aeron.test.cluster.TestCluster.aCluster;
import static io.aeron.test.cluster.TestCluster.awaitElectionClosed;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A real old-term recording can have a larger position than a new-term client write without containing that write.
 * Stop both followers to create the old tail, elect the shorter pair, then let the old member canvass while the
 * new voter is stopped. A transport-only subscriber keeps the publication connected without recording or voting.
 * The catch-up case also gates term-event publication to exercise recording metadata before replay.
 */
@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class StalePositionQuorumTest
{
    static final int BASELINE = 1;
    static final int OLD_TAIL = 2;
    static final int NEW_TERM_MARKER = 4;
    static final int TARGET = 8;
    static final int RECOVERY = 16;
    static volatile int oldId = NULL_VALUE;
    static volatile int newId = NULL_VALUE;
    static volatile long oldTerm = NULL_VALUE;
    static volatile long tailPosition = NULL_VALUE;
    static volatile long targetPosition = NULL_VALUE;
    static volatile long stalePosition = NULL_VALUE;
    static volatile boolean isolateOld;
    static volatile LogPublisher lastPublisher;
    static volatile boolean holdTermEvent;
    static volatile boolean dropCatchupCommit;
    static final Map<Integer, Long> METADATA_TERMS = new ConcurrentHashMap<>();
    static final AtomicInteger STALE_REPORTS = new AtomicInteger();
    static final AtomicInteger QUORUM_POLLS = new AtomicInteger();
    static final Map<Integer, Long> LEADER_TERMS = new ConcurrentHashMap<>();

    private static final Class<?>[] INSTRUMENTED_CLASSES =
        { ConsensusModuleAgent.class, Election.class, LogPublisher.class };
    private static ClusterInstrumentor[] instrumentors;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();
    private final AtomicInteger acknowledged = new AtomicInteger();

    @BeforeAll
    static void beforeAll()
    {
        assertTrue(INSTRUMENTED_CLASSES.length == 3);
        instrumentors = new ClusterInstrumentor[]
        {
            new ClusterInstrumentor(DropAtOldMember.class, "ConsensusModuleAgent", "onCanvassPosition"),
            new ClusterInstrumentor(DropAtOldMember.class, "ConsensusModuleAgent", "onRequestVote"),
            new ClusterInstrumentor(DropAtOldMember.class, "ConsensusModuleAgent", "onVote"),
            new ClusterInstrumentor(DropAtOldMember.class, "ConsensusModuleAgent", "onNewLeadershipTerm"),
            new ClusterInstrumentor(DropAtOldMember.class, "ConsensusModuleAgent", "onCommitPosition"),
            new ClusterInstrumentor(DropAtOldMember.class, "ConsensusModuleAgent", "onAppendPosition"),
            new ClusterInstrumentor(ObserveTerm.class, "Election", "state"),
            new ClusterInstrumentor(ObserveMessage.class, "LogPublisher", "appendMessage"),
            new ClusterInstrumentor(ObserveStalePosition.class, "ConsensusModuleAgent", "updateMemberLogPosition"),
            new ClusterInstrumentor(
                ObserveQuorumPoll.class, "ConsensusModuleAgent", "quorumPositionBoundedByLeaderLog"),
            new ClusterInstrumentor(
                GateTermEvent.class, "ConsensusModuleAgent", "appendNewLeadershipTermEvent"),
            new ClusterInstrumentor(DropCatchupCommit.class, "ConsensusModuleAgent", "onCommitPosition")
        };
    }

    @AfterAll
    static void afterAll()
    {
        isolateOld = false;
        holdTermEvent = false;
        dropCatchupCommit = false;
        for (final ClusterInstrumentor instrumentor : instrumentors)
        {
            instrumentor.reset();
        }
    }

    @Test
    @InterruptAfter(120)
    @SuppressWarnings("MethodLength")
    void shouldNotAcknowledgeNewWriteUsingAnOldTermTailAndShouldRetainItAfterFailover()
    {
        resetFaults();
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withLeaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(10))
            .withStartupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(20))
            .withElectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(200))
            .withElectionStatusIntervalNs(TimeUnit.MILLISECONDS.toNanos(10))
            .withServiceSupplier(index -> new TestNode.TestService[]{ new WriteTrackingService().index(index) })
            .start();
        systemTestWatcher.cluster(cluster);
        systemTestWatcher.ignoreErrorsMatching(s -> s.contains("Truncating Cluster Log") ||
            s.contains("unexpected vote request") || s.contains("unexpected new leadership term") ||
            s.contains("quorum position went backwards"));
        cluster.egressListener((sessionId, timestamp, buffer, offset, length, header) ->
            acknowledged.getAndUpdate(value -> value | buffer.getInt(offset)));

        try
        {
            cluster.awaitLeader();
            cluster.connectClient();
            send(cluster, BASELINE, Integer.BYTES);
            await(cluster, "baseline acknowledgement", () -> hasAcknowledged(BASELINE));
            cluster.awaitServicesMessageCount(1);
            for (int i = 0; i < 3; i++)
            {
                awaitElectionClosed(cluster.node(i));
            }
            awaitStableLeadership(cluster);
            final TestNode originalLeader = cluster.awaitLeader();
            oldId = originalLeader.memberId();
            oldTerm = LEADER_TERMS.get(oldId);
            final Subscription oldTransportSink = keepTransportConnected(cluster, originalLeader);
            final int first = (oldId + 1) % 3;
            final int second = (oldId + 2) % 3;
            cluster.stopNode(cluster.node(first));
            cluster.stopNode(cluster.node(second));

            // The active-quorum timeout has not expired yet. Record a large, genuinely uncommitted old tail.
            send(cluster, OLD_TAIL, 8 * 1024);
            await(cluster, "old tail recorded", () -> tailPosition > 0 &&
                originalLeader.appendPosition() >= tailPosition);
            assertFalse(hasAcknowledged(OLD_TAIL), "the old tail must have no replica and no acknowledgement");
            cluster.stopNode(originalLeader);
            oldTransportSink.close();
            cluster.closeClient();

            cluster.startStaticNode(first, false);
            cluster.startStaticNode(second, false);
            final TestNode newLeader = cluster.awaitLeader();
            newId = newLeader.memberId();
            final int voterId = newId == first ? second : first;
            final TestNode voter = cluster.node(voterId);
            awaitElectionClosed(newLeader);
            awaitElectionClosed(voter);
            cluster.connectClient();
            send(cluster, NEW_TERM_MARKER, Integer.BYTES);
            await(cluster, "new term committed and applied", () -> hasAcknowledged(NEW_TERM_MARKER) &&
                hasApplied(voter, NEW_TERM_MARKER));
            final long voterPosition = voter.appendPosition();
            final Subscription transportSink = keepTransportConnected(cluster, newLeader);
            assertTrue(LEADER_TERMS.get(newId) > oldTerm);
            assertTrue(tailPosition > voterPosition + 512, "old tail must cover the later, unrelated target position");

            cluster.stopNode(voter);
            isolateOld = true;
            final TestNode oldMember = cluster.startStaticNode(oldId, false);
            await(cluster, "authentic stale canvass at new leader", () ->
                oldMember.electionState() == CANVASS && stalePosition == tailPosition && STALE_REPORTS.get() > 0);
            final long electionCount = newLeader.electionCount();
            send(cluster, TARGET, Integer.BYTES);
            await(cluster, "target recorded above voter position", () -> targetPosition > voterPosition &&
                newLeader.appendPosition() >= targetPosition && stalePosition >= targetPosition);

            // Observe active quorum calculations and poll actual egress throughout the window. The old member
            // stays in CANVASS and the other replica is stopped, so neither can record the new-term target.
            final long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (!hasAcknowledged(TARGET) && System.nanoTime() < deadlineNs)
            {
                pollClient(cluster);
                Tests.yield();
            }
            final boolean prematurelyAcknowledged = hasAcknowledged(TARGET);
            assertTrue(QUORUM_POLLS.get() > 0, "the actual leader quorum calculation must execute in the window");
            assertTrue(newLeader.electionCount() == electionCount && newLeader.electionState() == CLOSED);
            assertTrue(oldMember.electionState() == CANVASS && !hasApplied(oldMember, TARGET));
            assertTrue(stalePosition >= targetPosition && voterPosition < targetPosition);

            if (!prematurelyAcknowledged)
            {
                // A positive run must also demonstrate a real ACK after a current-term replica returns.
                cluster.startStaticNode(voterId, false);
                await(cluster, "target acknowledged with real replica", () -> hasAcknowledged(TARGET));
                await(cluster, "target applied at real replica", () -> hasApplied(cluster.node(voterId), TARGET));
            }

            // Remove the only possible source of an unsafe target before healing the other two members.
            cluster.stopNode(newLeader);
            transportSink.close();
            cluster.closeClient();
            isolateOld = false;
            if (cluster.node(voterId).isClosed())
            {
                cluster.startStaticNode(voterId, false);
            }
            cluster.awaitLeader();
            awaitElectionClosed(cluster.node(oldId));
            awaitElectionClosed(cluster.node(voterId));
            cluster.connectClient();
            send(cluster, RECOVERY, Integer.BYTES);
            await(cluster, "surviving recordings recovered", () -> hasAcknowledged(RECOVERY) &&
                hasApplied(cluster.node(oldId), RECOVERY) && hasApplied(cluster.node(voterId), RECOVERY));

            System.out.println("stale-position evidence: oldTerm=" + oldTerm + " tail=" + tailPosition +
                " voter=" + voterPosition + " target=" + targetPosition +
                " prematureAck=" + prematurelyAcknowledged + " recoveredTarget=" +
                hasApplied(cluster.node(voterId), TARGET));
            assertAll(
                () -> assertFalse(prematurelyAcknowledged,
                    "target acknowledged before any current-term replica recorded it"),
                () -> assertTrue(hasApplied(cluster.node(oldId), TARGET),
                    "acknowledged target missing from old member after recovery"),
                () -> assertTrue(hasApplied(cluster.node(voterId), TARGET),
                    "acknowledged target missing from voter after recovery"));
        }
        finally
        {
            isolateOld = false;
        }
    }

    private static void resetFaults()
    {
        oldId = NULL_VALUE;
        newId = NULL_VALUE;
        oldTerm = NULL_VALUE;
        tailPosition = NULL_VALUE;
        targetPosition = NULL_VALUE;
        stalePosition = NULL_VALUE;
        isolateOld = false;
        holdTermEvent = false;
        dropCatchupCommit = false;
        STALE_REPORTS.set(0);
        QUORUM_POLLS.set(0);
        LEADER_TERMS.clear();
        METADATA_TERMS.clear();
    }

    @Test
    @InterruptAfter(120)
    @SuppressWarnings("MethodLength")
    void shouldRetainAcknowledgedCatchupWriteAcrossMetadataLagAndFailover()
    {
        resetFaults();
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withLeaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(5))
            .withStartupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(10))
            .withSessionTimeoutNs(TimeUnit.SECONDS.toNanos(60))
            .withElectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(200))
            .withElectionStatusIntervalNs(TimeUnit.MILLISECONDS.toNanos(10))
            .withServiceSupplier(index -> new TestNode.TestService[]{ new WriteTrackingService().index(index) })
            .start();
        systemTestWatcher.cluster(cluster);
        systemTestWatcher.ignoreErrorsMatching(s -> s.contains("Truncating Cluster Log") ||
            s.contains("unexpected vote request") || s.contains("unexpected new leadership term") ||
            s.contains("quorum position went backwards") || s.contains("unexpected image close during catchup"));
        cluster.egressListener((sessionId, timestamp, buffer, offset, length, header) ->
            acknowledged.getAndUpdate(value -> value | buffer.getInt(offset)));

        try
        {
            cluster.awaitLeader();
            cluster.connectClient();
            send(cluster, BASELINE, Integer.BYTES);
            await(cluster, "baseline acknowledgement", () -> hasAcknowledged(BASELINE));
            cluster.awaitServicesMessageCount(1);
            awaitStableLeadership(cluster);
            final TestNode originalLeader = cluster.awaitLeader();
            oldId = originalLeader.memberId();
            oldTerm = LEADER_TERMS.get(oldId);
            holdTermEvent = true;
            cluster.stopNode(originalLeader);

            // Simulated publication backpressure holds the new term event, while the live follower joins
            // and advances recording metadata. No bytes, reported positions, or term fields are fabricated.
            await(cluster, "new term event held", () -> newId != NULL_VALUE);
            final TestNode leader = cluster.node(newId);
            final int voterId = 3 - oldId - newId;
            final TestNode voter = cluster.node(voterId);
            final long newTerm = LEADER_TERMS.get(newId);
            await(cluster, "live follower metadata advanced", () ->
                METADATA_TERMS.getOrDefault(voterId, oldTerm) == newTerm && voter.electionState() == CLOSED);
            final long voterPosition = voter.appendPosition();
            final Subscription sink = keepTransportConnected(cluster, leader);
            cluster.stopNode(voter);
            holdTermEvent = false;
            await(cluster, "new leader ready", () -> leader.electionState() == CLOSED &&
                cluster.client().leaderMemberId() == newId);
            send(cluster, TARGET, Integer.BYTES);
            await(cluster, "uncommitted target recorded", () -> targetPosition > voterPosition &&
                leader.appendPosition() >= targetPosition);
            assertFalse(hasAcknowledged(TARGET));

            // The late member must take archive catch-up, not live join. Drop only commit messages, allowing
            // real recording and accepted-term position reports while replay remains at the old prefix.
            dropCatchupCommit = true;
            final TestNode late = cluster.startStaticNode(oldId, false);
            await(cluster, "catchup acknowledges target before service replay", () -> hasAcknowledged(TARGET));
            assertTrue(late.electionState() == FOLLOWER_CATCHUP);
            assertTrue(late.appendPosition() >= targetPosition);
            final long catchupMetadataTerm = METADATA_TERMS.get(oldId);
            assertFalse(hasApplied(late, TARGET));

            cluster.stopNode(leader);
            sink.close();
            cluster.closeClient();
            cluster.startStaticNode(voterId, false);
            cluster.awaitLeader();
            dropCatchupCommit = false;
            awaitElectionClosed(cluster.node(oldId));
            awaitElectionClosed(cluster.node(voterId));
            cluster.connectClient();
            send(cluster, RECOVERY, Integer.BYTES);
            await(cluster, "recovery write applied", () -> hasAcknowledged(RECOVERY) &&
                hasApplied(cluster.node(oldId), RECOVERY) && hasApplied(cluster.node(voterId), RECOVERY));
            System.out.println("catchup-metadata evidence: oldTerm=" + oldTerm + " acceptedTerm=" + newTerm +
                " catchupMetadataTerm=" + catchupMetadataTerm +
                " voter=" + voterPosition + " target=" + targetPosition + " acknowledged=true recoveredTarget=" +
                hasApplied(cluster.node(voterId), TARGET));
            assertAll(
                () -> assertTrue(catchupMetadataTerm == newTerm, "catchup report preceded recording metadata"),
                () -> assertTrue(hasApplied(cluster.node(oldId), TARGET),
                    "acknowledged catchup write lost at late member"),
                () -> assertTrue(hasApplied(cluster.node(voterId), TARGET),
                    "acknowledged catchup write lost at voter"));
        }
        finally
        {
            holdTermEvent = false;
            dropCatchupCommit = false;
        }
    }

    private static void send(final TestCluster cluster, final int value, final int length)
    {
        cluster.msgBuffer().setMemory(0, length, (byte)0);
        cluster.msgBuffer().putInt(0, value);
        cluster.pollUntilMessageSent(length);
    }

    private static void awaitStableLeadership(final TestCluster cluster)
    {
        final long[] counts = new long[3];
        final long[] quietSince = { System.nanoTime() };
        await(cluster, "stable baseline leadership", () ->
        {
            for (int i = 0; i < 3; i++)
            {
                final TestNode node = cluster.node(i);
                if (node.electionState() != CLOSED || node.electionCount() != counts[i])
                {
                    quietSince[0] = System.nanoTime();
                    counts[i] = node.electionCount();
                }
            }
            return System.nanoTime() - quietSince[0] >= TimeUnit.SECONDS.toNanos(2);
        });
    }

    // Keep UDP transport connected without supplying an archive recording or consensus position report.
    // This represents a live transport whose recording consumer has stopped making progress.
    private static Subscription keepTransportConnected(final TestCluster cluster, final TestNode leader)
    {
        final Subscription sink = cluster.client().context().aeron().addSubscription(
            "aeron:udp?endpoint=localhost:0", leader.consensusModule().context().logStreamId());
        await(cluster, "transport sink bound", () -> null != sink.resolvedEndpoint());
        lastPublisher.publication().addDestination("aeron:udp?endpoint=" + sink.resolvedEndpoint());
        await(cluster, "transport sink connected", sink::isConnected);
        return sink;
    }

    private static void pollClient(final TestCluster cluster)
    {
        cluster.client().pollEgress();
        cluster.client().sendKeepAlive();
    }

    private static void await(final TestCluster cluster, final String description, final BooleanSupplier condition)
    {
        final long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!condition.getAsBoolean())
        {
            pollClient(cluster);
            Tests.yield();
            if (System.nanoTime() >= deadlineNs)
            {
                throw new IllegalStateException("timed out: " + description + " old=" + oldId + " new=" + newId +
                    " tail=" + tailPosition + " target=" + targetPosition + " stale=" + stalePosition);
            }
        }
    }

    private boolean hasAcknowledged(final int value)
    {
        return (acknowledged.get() & value) != 0;
    }

    private static boolean hasApplied(final TestNode node, final int value)
    {
        return (((WriteTrackingService)node.service()).writes & value) != 0;
    }

    public static class WriteTrackingService extends TestNode.TestService
    {
        volatile int writes;

        public void onSessionMessage(
            final ClientSession session, final long timestamp, final DirectBuffer buffer,
            final int offset, final int length, final Header header)
        {
            writes |= buffer.getInt(offset);
            super.onSessionMessage(session, timestamp, buffer, offset, length, header);
        }
    }

    public static class DropAtOldMember
    {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        static boolean skip(@Advice.FieldValue("memberId") final int memberId)
        {
            return isolateOld && memberId == oldId;
        }
    }

    public static class ObserveTerm
    {
        @Advice.OnMethodEnter
        static void state(
            @Advice.Argument(0) final ElectionState state,
            @Advice.FieldValue("leadershipTermId") final long term,
            @Advice.FieldValue("logLeadershipTermId") final long metadataTerm,
            @Advice.This final Election election)
        {
            if (state == LEADER_LOG_REPLICATION)
            {
                LEADER_TERMS.put(election.thisMemberId(), term);
            }
            if (state == FOLLOWER_READY || state == FOLLOWER_CATCHUP)
            {
                METADATA_TERMS.put(election.thisMemberId(), metadataTerm);
            }
        }
    }

    public static class GateTermEvent
    {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        static boolean skip(
            @Advice.FieldValue("memberId") final int memberId,
            @Advice.FieldValue("logPublisher") final LogPublisher publisher)
        {
            if (holdTermEvent && memberId != oldId)
            {
                lastPublisher = publisher;
                newId = memberId;
                return true;
            }
            return false;
        }
    }

    public static class DropCatchupCommit
    {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        static boolean skip(@Advice.FieldValue("memberId") final int memberId)
        {
            return dropCatchupCommit && memberId == oldId;
        }
    }

    public static class ObserveMessage
    {
        @Advice.OnMethodExit
        static void append(
            @Advice.Argument(3) final DirectBuffer buffer,
            @Advice.Argument(4) final int offset,
            @Advice.This final LogPublisher publisher,
            @Advice.Return final long position)
        {
            if (position > 0)
            {
                lastPublisher = publisher;
                if (buffer.getInt(offset) == OLD_TAIL)
                {
                    tailPosition = position;
                }
                else if (buffer.getInt(offset) == TARGET)
                {
                    targetPosition = position;
                }
            }
        }
    }

    public static class ObserveStalePosition
    {
        @Advice.OnMethodExit
        static void update(
            @Advice.FieldValue("memberId") final int memberId,
            @Advice.Argument(0) final ClusterMember member,
            @Advice.Argument(1) final long term,
            @Advice.Argument(2) final long position)
        {
            if (isolateOld && memberId == newId && member.id() == oldId && term == oldTerm)
            {
                stalePosition = position;
                STALE_REPORTS.incrementAndGet();
            }
        }
    }

    public static class ObserveQuorumPoll
    {
        @Advice.OnMethodExit
        static void poll(
            @Advice.FieldValue("memberId") final int memberId,
            @Advice.Argument(1) final long position)
        {
            if (isolateOld && memberId == newId && targetPosition > 0 && position >= targetPosition &&
                stalePosition >= targetPosition)
            {
                QUORUM_POLLS.incrementAndGet();
            }
        }
    }
}
