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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
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
    static volatile boolean holdTermEvent;
    static volatile boolean dropCatchupCommit;
    static final Map<Integer, Long> METADATA_TERMS = new ConcurrentHashMap<>();
    static final AtomicInteger STALE_REPORTS = new AtomicInteger();
    static final AtomicInteger QUORUM_POLLS = new AtomicInteger();
    static final Map<Integer, Long> LEADER_TERMS = new ConcurrentHashMap<>();
    static final ThreadLocal<long[]> CATCHUP_TERMS = new ThreadLocal<>();
    static volatile CatchupReport catchupReport;
    static volatile boolean nominationScenario;
    static volatile boolean nominationReset;
    static volatile boolean probeNomination;
    static volatile int nominationMember = NULL_VALUE;
    static volatile long nominationLogTerm;
    static volatile long nominationAcceptedTerm;
    static volatile NominationSample nominationSample;
    static final Map<Integer, Long> NOMINATION_PEERS = new ConcurrentHashMap<>();
    static final AtomicReference<DestinationCommand> DESTINATION_COMMAND = new AtomicReference<>();

    private static final Class<?>[] INSTRUMENTED_CLASSES =
        { ConsensusModuleAgent.class, Election.class, LogPublisher.class,
            ConsensusPublisher.class, ClusterMember.class };
    private static ClusterInstrumentor[] instrumentors;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();
    private final AtomicInteger acknowledged = new AtomicInteger();

    @BeforeAll
    static void beforeAll()
    {
        assertTrue(INSTRUMENTED_CLASSES.length == 5);
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
            new ClusterInstrumentor(DropCatchupCommit.class, "ConsensusModuleAgent", "onCommitPosition"),
            new ClusterInstrumentor(ObserveCatchupTerms.class, "ConsensusModuleAgent", "catchupPoll"),
            new ClusterInstrumentor(ObserveCatchupReport.class, "ConsensusPublisher", "appendPosition"),
            new ClusterInstrumentor(HoldNewLeaderReplay.class, "Election", "leaderReplay"),
            new ClusterInstrumentor(HoldNominationCanvass.class, "Election", "canvass"),
            new ClusterInstrumentor(DropNominationCommit.class, "ConsensusModuleAgent", "onCommitPosition"),
            new ClusterInstrumentor(DropNominationAnnouncement.class, "ConsensusModuleAgent", "onNewLeadershipTerm"),
            new ClusterInstrumentor(DropNominationVoteRequest.class, "ConsensusModuleAgent", "onRequestVote"),
            new ClusterInstrumentor(ObserveNominationPeer.class, "ConsensusModuleAgent", "updateMemberLogPosition"),
            new ClusterInstrumentor(ObserveCandidate.class, "ClusterMember", "isUnanimousCandidate"),
            new ClusterInstrumentor(AddTransportDestination.class, "ConsensusModuleAgent", "doWork")
        };
    }

    @AfterAll
    static void afterAll()
    {
        isolateOld = false;
        holdTermEvent = false;
        dropCatchupCommit = false;
        nominationScenario = false;
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
        catchupReport = null;
        nominationScenario = false;
        nominationReset = false;
        probeNomination = false;
        nominationMember = NULL_VALUE;
        nominationSample = null;
        NOMINATION_PEERS.clear();
        DESTINATION_COMMAND.set(null);
    }

    @Test
    @InterruptAfter(120)
    @SuppressWarnings("MethodLength")
    void shouldAssessNominationUsingRecordedTermAfterUnreplayedAcceptance()
    {
        resetFaults();
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withLeaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(5))
            .withStartupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(10))
            .withElectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(200))
            .withElectionStatusIntervalNs(TimeUnit.MILLISECONDS.toNanos(10))
            .withServiceSupplier(index -> new TestNode.TestService[]{ new WriteTrackingService().index(index) })
            .start();
        systemTestWatcher.cluster(cluster);
        systemTestWatcher.ignoreErrorsMatching(s -> s.contains("unexpected vote request") ||
            s.contains("unexpected new leadership term") || s.contains("Truncating Cluster Log") ||
            s.contains("timeout awaiting commit position during replay") || s.contains("potential new election"));
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
            final TestNode oldLeader = cluster.awaitLeader();
            oldId = oldLeader.memberId();
            oldTerm = LEADER_TERMS.get(oldId);
            final int first = (oldId + 1) % 3;
            final int second = (oldId + 2) % 3;
            final Subscription sink = keepTransportConnected(cluster, oldLeader);
            cluster.stopNode(cluster.node(first));
            cluster.stopNode(cluster.node(second));
            send(cluster, OLD_TAIL, 8 * 1024);
            await(cluster, "long old-term recording", () -> tailPosition > 0 &&
                oldLeader.appendPosition() >= tailPosition);
            assertFalse(hasAcknowledged(OLD_TAIL));
            cluster.stopNode(oldLeader);
            sink.close();
            cluster.closeClient();

            // The shorter pair elects a new term. Pause its leader before replay and drop commit delivery,
            // so its voter accepts that term but times out before replaying or persisting it as log metadata.
            nominationScenario = true;
            cluster.startStaticNode(first, false);
            cluster.startStaticNode(second, false);
            awaitWithoutClient("accepted term returned to canvass without replay", () -> nominationReset);
            assertTrue(nominationAcceptedTerm > nominationLogTerm);
            cluster.stopNode(cluster.node(newId));
            cluster.startStaticNode(newId, false);
            isolateOld = true;
            cluster.startStaticNode(oldId, false);
            awaitWithoutClient("real canvasses from both peers", () ->
                NOMINATION_PEERS.getOrDefault(oldId, 0L) == tailPosition && NOMINATION_PEERS.containsKey(newId));
            probeNomination = true;
            awaitWithoutClient("actual unanimous-candidate decision", () -> null != nominationSample);
            final NominationSample sample = nominationSample;
            System.out.println("nomination evidence: " + sample);
            assertTrue(sample.acceptedTerm > sample.logTerm && sample.longerPeerPosition > sample.position);
            assertFalse(sample.unanimous,
                "NOMINATION_LOG_TERM: accepted but unreplayed term made a shorter log appear unanimously eligible");
            nominationScenario = false;
            isolateOld = false;
            cluster.awaitLeader();
            cluster.connectClient();
            send(cluster, RECOVERY, Integer.BYTES);
            await(cluster, "client service after nomination probe", () -> hasAcknowledged(RECOVERY));
        }
        finally
        {
            nominationScenario = false;
            isolateOld = false;
        }
    }

    private static void awaitWithoutClient(final String description, final BooleanSupplier condition)
    {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!condition.getAsBoolean())
        {
            Tests.yield();
            assertTrue(System.nanoTime() < deadline, "fixture timed out: " + description +
                " old=" + oldId + " new=" + newId + " voter=" + nominationMember +
                " peers=" + NOMINATION_PEERS);
        }
    }

    @Test
    @InterruptAfter(120)
    void shouldRetainAcknowledgedCatchupWriteAcrossMetadataLagAndFailover()
    {
        runCatchupScenario(false);
    }

    @Test
    @InterruptAfter(120)
    void shouldReportAcceptedTermDuringCatchupBeforeReplayingTermEvent()
    {
        runCatchupScenario(true);
    }

    @SuppressWarnings("MethodLength")
    private void runCatchupScenario(final boolean checkReportOnly)
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
            if (checkReportOnly)
            {
                await(cluster, "actual catchup report ahead of replay", () -> null != catchupReport);
                final CatchupReport report = catchupReport;
                System.out.println("catchup-report evidence: " + report + " target=" + targetPosition);
                assertTrue(report.position >= targetPosition && report.acceptedTerm > report.agentTerm);
                assertEquals(report.acceptedTerm, report.reportedTerm,
                    "CATCHUP_REPORT_TERM: recorded target reported in old replay term");
                await(cluster, "target acknowledged from catchup recording", () -> hasAcknowledged(TARGET));
                assertFalse(hasApplied(late, TARGET));
                dropCatchupCommit = false;
                await(cluster, "catchup target applied after commit delivery", () -> hasApplied(late, TARGET));
                send(cluster, RECOVERY, Integer.BYTES);
                await(cluster, "continued client service", () -> hasAcknowledged(RECOVERY) &&
                    hasApplied(late, RECOVERY));
                sink.close();
                return;
            }
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
        // The agent owns an invoker-mode Aeron client with a NoOpLock. Queue this command on that agent's
        // thread instead of concurrently servicing its ClientConductor from the JUnit thread.
        final DestinationCommand command = new DestinationCommand(leader.memberId(),
            "aeron:udp?endpoint=" + sink.resolvedEndpoint(), new CompletableFuture<>());
        assertTrue(DESTINATION_COMMAND.compareAndSet(null, command));
        await(cluster, "transport destination submitted", () -> command.completion.isDone());
        command.completion.join();
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
            if (nominationScenario && state == CANVASS && election.thisMemberId() == nominationMember &&
                term > metadataTerm)
            {
                nominationLogTerm = metadataTerm;
                nominationAcceptedTerm = term;
                nominationReset = true;
            }
        }
    }

    public static class GateTermEvent
    {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        static boolean skip(
            @Advice.FieldValue("memberId") final int memberId)
        {
            if (holdTermEvent && memberId != oldId)
            {
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
            @Advice.Return final long position)
        {
            if (position > 0)
            {
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

    record CatchupReport(long acceptedTerm, long agentTerm, long reportedTerm, long position)
    {
    }

    public static class ObserveCatchupTerms
    {
        @Advice.OnMethodEnter
        static void enter(
            @Advice.FieldValue("memberId") final int memberId,
            @Advice.FieldValue("leadershipTermId") final long agentTerm,
            @Advice.FieldValue("election") final Election election)
        {
            if (dropCatchupCommit && memberId == oldId)
            {
                CATCHUP_TERMS.set(new long[]{ election.leadershipTermId(), agentTerm });
            }
        }

        @Advice.OnMethodExit(onThrowable = Throwable.class)
        static void exit()
        {
            CATCHUP_TERMS.remove();
        }
    }

    public static class ObserveCatchupReport
    {
        @Advice.OnMethodExit
        static void exit(
            @Advice.Argument(1) final long reportedTerm,
            @Advice.Argument(2) final long position,
            @Advice.Return final boolean sent)
        {
            final long[] terms = CATCHUP_TERMS.get();
            if (sent && null != terms && terms[0] > terms[1] && position >= targetPosition &&
                targetPosition > 0 && null == catchupReport)
            {
                catchupReport = new CatchupReport(terms[0], terms[1], reportedTerm, position);
            }
        }
    }

    record NominationSample(
        long logTerm, long acceptedTerm, long selfTerm, long position, long longerPeerPosition, boolean unanimous)
    {
    }

    public static class HoldNewLeaderReplay
    {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        static boolean enter(@Advice.This final Election election)
        {
            if (nominationScenario && !nominationReset && election.thisMemberId() != oldId)
            {
                newId = election.thisMemberId();
                nominationMember = 3 - oldId - newId;
                return true;
            }
            return nominationScenario && election.thisMemberId() == newId;
        }
    }

    public static class HoldNominationCanvass
    {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        static boolean enter(@Advice.This final Election election)
        {
            return nominationScenario && nominationReset && !probeNomination &&
                election.thisMemberId() == nominationMember;
        }
    }

    public static class DropNominationCommit
    {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        static boolean enter()
        {
            return nominationScenario;
        }
    }

    public static class DropNominationAnnouncement
    {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        static boolean enter()
        {
            return nominationScenario && newId != NULL_VALUE;
        }
    }

    public static class DropNominationVoteRequest
    {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        static boolean enter()
        {
            return nominationScenario && nominationReset;
        }
    }

    public static class ObserveNominationPeer
    {
        @Advice.OnMethodExit
        static void exit(
            @Advice.FieldValue("memberId") final int memberId,
            @Advice.Argument(0) final ClusterMember member)
        {
            if (nominationScenario && nominationReset && memberId == nominationMember)
            {
                NOMINATION_PEERS.put(member.id(), member.logPosition());
            }
        }
    }

    public static class ObserveCandidate
    {
        @Advice.OnMethodExit
        static void exit(
            @Advice.Argument(0) final ClusterMember[] members,
            @Advice.Argument(1) final ClusterMember candidate,
            @Advice.Return final boolean unanimous)
        {
            if (nominationScenario && probeNomination && candidate.id() == nominationMember &&
                null == nominationSample)
            {
                long longerPeerPosition = 0;
                for (final ClusterMember member : members)
                {
                    if (member.id() == oldId)
                    {
                        longerPeerPosition = member.logPosition();
                    }
                }
                nominationSample = new NominationSample(nominationLogTerm, nominationAcceptedTerm,
                    candidate.leadershipTermId(), candidate.logPosition(), longerPeerPosition, unanimous);
            }
        }
    }

    record DestinationCommand(int memberId, String channel, CompletableFuture<Void> completion)
    {
    }

    public static class AddTransportDestination
    {
        @Advice.OnMethodEnter
        static void enter(
            @Advice.FieldValue("memberId") final int memberId,
            @Advice.FieldValue("logPublisher") final LogPublisher publisher)
        {
            final DestinationCommand command = DESTINATION_COMMAND.get();
            if (null != command && command.memberId() == memberId &&
                DESTINATION_COMMAND.compareAndSet(command, null))
            {
                try
                {
                    publisher.publication().asyncAddDestination(command.channel());
                    command.completion().complete(null);
                }
                catch (final Exception ex)
                {
                    command.completion().completeExceptionally(ex);
                }
            }
        }
    }
}
