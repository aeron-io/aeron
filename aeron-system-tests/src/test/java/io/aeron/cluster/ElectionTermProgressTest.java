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

import io.aeron.cluster.client.ClusterEvent;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import net.bytebuddy.asm.Advice;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static io.aeron.Aeron.NULL_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises election term accounting with real retained recordings, archive replay, and quorum reports.
 * Probes observe the actual calculation on the consensus thread; they never supply a position or term.
 */
@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class ElectionTermProgressTest
{
    static volatile boolean observe;
    static final ThreadLocal<String> PATH = new ThreadLocal<>();
    static final Map<String, QuorumSample> SAMPLES = new ConcurrentHashMap<>();
    static volatile ElectionState heldState;
    static volatile int heldLeaderId = NULL_VALUE;
    static volatile long baselineTerm;
    static volatile VoteResult voteResult;
    private static final Class<?>[] INSTRUMENTED_CLASSES = { ConsensusModuleAgent.class, Election.class };
    private static ClusterInstrumentor[] instrumentors;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @BeforeAll
    static void beforeAll()
    {
        assertEquals(2, INSTRUMENTED_CLASSES.length);
        instrumentors = new ClusterInstrumentor[]
        {
            new ClusterInstrumentor(ObservePath.class, "Election", "leaderLogReplication"),
            new ClusterInstrumentor(ObservePath.class, "Election", "leaderReplay"),
            new ClusterInstrumentor(ObservePath.class, "Election", "onCanvassPosition"),
            new ClusterInstrumentor(ObserveQuorum.class, "ConsensusModuleAgent", "quorumPositionBoundedByLeaderLog"),
            new ClusterInstrumentor(HoldLeaderWork.class, "Election", "leaderLogReplication"),
            new ClusterInstrumentor(HoldLeaderWork.class, "Election", "leaderReplay"),
            new ClusterInstrumentor(HoldLeaderWork.class, "Election", "leaderInit"),
            new ClusterInstrumentor(HoldLeaderWork.class, "Election", "leaderReady"),
            new ClusterInstrumentor(DropLeadershipWhileHeld.class, "ConsensusModuleAgent", "onNewLeadershipTerm"),
            new ClusterInstrumentor(DropLeadershipWhileHeld.class, "ConsensusModuleAgent", "onCommitPosition"),
            new ClusterInstrumentor(ObserveVoteRequest.class, "Election", "onRequestVote")
        };
    }

    @AfterAll
    static void afterAll()
    {
        observe = false;
        heldState = null;
        for (final ClusterInstrumentor instrumentor : instrumentors)
        {
            instrumentor.reset();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 3 })
    @InterruptAfter(90)
    void shouldCountRecordedQuorumBeforeJoiningLeaderLog(final int memberCount)
    {
        observe = false;
        SAMPLES.clear();
        final TestCluster cluster = aCluster()
            .withStaticNodes(memberCount)
            .withLeaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(5))
            .withStartupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(10))
            .withElectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(200))
            .withElectionStatusIntervalNs(TimeUnit.MILLISECONDS.toNanos(10))
            .start();
        systemTestWatcher.cluster(cluster);
        systemTestWatcher.ignoreErrorsMatching(s -> s.contains("unexpected vote request") ||
            s.contains("unexpected new leadership term"));
        try
        {
            cluster.awaitLeader();
            cluster.connectClient();
            cluster.sendAndAwaitMessages(1);
            cluster.awaitServicesMessageCount(1);
            cluster.closeClient();
            cluster.stopAllNodes();

            // A nonempty prefix is essential: an incorrectly computed zero quorum can still elect an empty log.
            // In the three-member case only a quorum returns, so the leader must count its own recording.
            observe = true;
            cluster.startStaticNode(0, false);
            if (memberCount == 3)
            {
                cluster.startStaticNode(1, false);
            }
            await("replication quorum observation", () -> SAMPLES.containsKey("leaderLogReplication"));
            assertQuorum("leaderLogReplication");
            await("replay quorum observation", () -> SAMPLES.containsKey("leaderReplay"));
            assertQuorum("leaderReplay");
            cluster.awaitLeader();
            cluster.connectClient();
            cluster.sendAndAwaitMessages(1, 2);
        }
        finally
        {
            observe = false;
        }
    }

    private static void assertQuorum(final String path)
    {
        final QuorumSample sample = SAMPLES.get(path);
        System.out.println("election-quorum evidence: path=" + path + " " + sample);
        assertEquals(sample.acceptedTerm, sample.selfTerm, "EARLY_SELF_TERM: leader excluded its own recording");
        assertEquals(sample.appendPosition, sample.quorumPosition,
            "ELECTION_QUORUM_TERM: recorded current-term quorum was excluded before leader log join");
    }

    @Test
    @InterruptAfter(90)
    void shouldUseElectionQuorumWhenAnsweringOldCanvass()
    {
        observe = false;
        heldState = null;
        heldLeaderId = NULL_VALUE;
        SAMPLES.clear();
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withLeaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(5))
            .withStartupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(10))
            .withElectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(200))
            .withElectionStatusIntervalNs(TimeUnit.MILLISECONDS.toNanos(10))
            .start();
        systemTestWatcher.cluster(cluster);
        systemTestWatcher.ignoreErrorsMatching(s -> s.contains("unexpected vote request") ||
            s.contains("unexpected new leadership term"));
        try
        {
            final TestNode leader = cluster.awaitLeader();
            cluster.connectClient();
            cluster.sendAndAwaitMessages(1);
            baselineTerm = leader.consensusModule().context().recordingLog().findLastTerm().leadershipTermId;
            cluster.closeClient();
            cluster.stopAllNodes();
            observe = true;
            heldState = ElectionState.LEADER_REPLAY;
            cluster.startStaticNode(0, false);
            cluster.startStaticNode(1, false);
            await("leader paused before replay", () -> heldLeaderId != NULL_VALUE);
            cluster.startStaticNode(2, false);
            await("canvass response quorum observation", () -> SAMPLES.containsKey("onCanvassPosition"));
            assertQuorum("onCanvassPosition");
            heldState = null;
            cluster.awaitLeader();
            cluster.connectClient();
            cluster.sendAndAwaitMessages(1, 2);
        }
        finally
        {
            heldState = null;
            observe = false;
        }
    }

    @ParameterizedTest
    @EnumSource(value = ElectionState.class,
        names = { "LEADER_LOG_REPLICATION", "LEADER_REPLAY", "LEADER_INIT", "LEADER_READY" })
    @InterruptAfter(90)
    void shouldRestartUnfinishedLeaderOnRealNewerBallot(final ElectionState state)
    {
        observe = false;
        heldState = null;
        heldLeaderId = NULL_VALUE;
        voteResult = null;
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withLeaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(2))
            .withStartupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(4))
            .withElectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(200))
            .withElectionStatusIntervalNs(TimeUnit.MILLISECONDS.toNanos(10))
            .start();
        systemTestWatcher.cluster(cluster);
        systemTestWatcher.ignoreErrorsMatching(s -> s.contains("unexpected vote request") ||
            s.contains("unexpected new leadership term") || s.contains("failed to join live log") ||
            s.contains("no catchup progress") || s.contains("new leader detected") ||
            s.contains("heartbeat timeout") || s.contains("potential new election"));
        try
        {
            final TestNode oldLeader = cluster.awaitLeader();
            cluster.connectClient();
            cluster.sendAndAwaitMessages(1);
            baselineTerm = oldLeader.consensusModule().context().recordingLog().findLastTerm().leadershipTermId;
            cluster.closeClient();
            heldState = state;
            cluster.stopNode(oldLeader);
            await("unfinished leader reaches " + state, () -> heldLeaderId != NULL_VALUE);
            // The two other members can now form a real newer ballot while this elected leader is paused.
            cluster.startStaticNode(oldLeader.index(), false);
            await("newer vote request at unfinished leader", () -> null != voteResult);
            final VoteResult result = voteResult;
            System.out.println("unfinished-leader evidence: " + result);
            assertEquals(state, result.state);
            assertTrue(result.requestedTerm > result.acceptedTerm);
            assertTrue(result.restarted,
                "UNFINISHED_LEADER_BALLOT: elected leader did not restart on a real newer vote request");
            heldState = null;
            cluster.awaitLeader();
            cluster.connectClient();
            cluster.sendAndAwaitMessages(1, 2);
        }
        finally
        {
            heldState = null;
        }
    }

    private static void await(final String description, final BooleanSupplier condition)
    {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean())
        {
            Tests.yield();
            assertTrue(System.nanoTime() < deadline, "fixture timed out: " + description + " samples=" + SAMPLES);
        }
    }

    record QuorumSample(
        int memberId, long acceptedTerm, long agentTerm, long selfTerm,
        long appendPosition, long quorumPosition, int supportingMembers)
    {
    }

    record VoteResult(ElectionState state, long acceptedTerm, long requestedTerm, boolean restarted)
    {
    }

    public static class HoldLeaderWork
    {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        static boolean enter(
            @Advice.FieldValue("state") final ElectionState state,
            @Advice.FieldValue("leadershipTermId") final long term,
            @Advice.This final Election election)
        {
            if (null != heldState && state == heldState && term > baselineTerm &&
                (heldLeaderId == NULL_VALUE || election.thisMemberId() == heldLeaderId))
            {
                heldLeaderId = election.thisMemberId();
                return true;
            }
            return false;
        }
    }

    public static class DropLeadershipWhileHeld
    {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        static boolean enter()
        {
            // Delay leadership/commit delivery while leaving real canvass, request-vote, vote, and append traffic.
            return null != heldState && heldLeaderId != NULL_VALUE;
        }
    }

    public static class ObserveVoteRequest
    {
        @Advice.OnMethodEnter
        static VoteResult enter(
            @Advice.Argument(2) final long requestedTerm,
            @Advice.FieldValue("state") final ElectionState state,
            @Advice.FieldValue("leadershipTermId") final long term,
            @Advice.This final Election election)
        {
            return null != heldState && state == heldState && election.thisMemberId() == heldLeaderId &&
                requestedTerm > term ? new VoteResult(state, term, requestedTerm, false) : null;
        }

        @Advice.OnMethodExit(onThrowable = Throwable.class)
        static void exit(@Advice.Enter final VoteResult entry, @Advice.Thrown final Throwable error)
        {
            if (null != entry && null == voteResult)
            {
                voteResult = new VoteResult(entry.state(), entry.acceptedTerm(), entry.requestedTerm(),
                    error instanceof ClusterEvent && error.getMessage().contains("unexpected vote request"));
            }
        }
    }

    public static class ObservePath
    {
        @Advice.OnMethodEnter
        static void enter(@Advice.Origin("#m") final String method)
        {
            PATH.set(method);
        }

        @Advice.OnMethodExit(onThrowable = Throwable.class)
        static void exit()
        {
            PATH.remove();
        }
    }

    public static class ObserveQuorum
    {
        @Advice.OnMethodExit
        static void exit(
            @Advice.FieldValue("memberId") final int memberId,
            @Advice.FieldValue("leadershipTermId") final long agentTerm,
            @Advice.FieldValue("election") final Election election,
            @Advice.FieldValue("activeMembers") final ClusterMember[] members,
            @Advice.FieldValue("leaderHeartbeatTimeoutNs") final long timeoutNs,
            @Advice.Argument(1) final long appendPosition,
            @Advice.Argument(2) final long nowNs,
            @Advice.Return final long quorumPosition)
        {
            final String path = PATH.get();
            if (observe && null != path && null != election && appendPosition > 0 &&
                election.leadershipTermId() > agentTerm)
            {
                final long acceptedTerm = election.leadershipTermId();
                long selfTerm = -1;
                int supportingMembers = 0;
                for (final ClusterMember member : members)
                {
                    if (member.id() == memberId)
                    {
                        selfTerm = member.leadershipTermId();
                    }
                    if (member.logPosition() >= appendPosition &&
                        member.timeOfLastAppendPositionNs() + timeoutNs > nowNs &&
                        (member.id() == memberId || member.leadershipTermId() == acceptedTerm))
                    {
                        supportingMembers++;
                    }
                }
                if (supportingMembers >= ClusterMember.quorumThreshold(members.length))
                {
                    SAMPLES.putIfAbsent(path, new QuorumSample(memberId, acceptedTerm, agentTerm, selfTerm,
                        appendPosition, quorumPosition, supportingMembers));
                }
            }
        }
    }
}
