/*
 * Copyright 2014-2026 Real Logic Limited.
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

import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import net.bytebuddy.asm.Advice;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.test.cluster.TestCluster.aCluster;
import static io.aeron.test.cluster.TestCluster.awaitElectionState;

@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class OverlappingElectionsClusterTest
{
    private static ClusterInstrumentor leaderInstrumentor;
    private static ClusterInstrumentor candidateInstrumentor;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @BeforeAll
    static void beforeAll()
    {
        leaderInstrumentor = new ClusterInstrumentor(TermZeroLeader.class, "Election", "leaderLogReplication");
        candidateInstrumentor = new ClusterInstrumentor(TermOneCandidate.class, "Election", "candidateBallot");
    }

    @AfterAll
    static void afterAll()
    {
        candidateInstrumentor.reset();
        leaderInstrumentor.reset();
    }

    public static class TermZeroLeader
    {
        static final int NODE_ID = 0;
        static volatile boolean isStalling = true;

        @Advice.OnMethodEnter
        static void leaderLogReplication(@Advice.FieldValue("leadershipTermId") final long leadershipTermId)
        {
            while (isStalling && 0 == leadershipTermId && !Thread.currentThread().isInterrupted())
            {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
        }
    }

    public static class TermOneCandidate
    {
        static volatile boolean isStalling = true;
        static volatile boolean hasStalled = false;

        @Advice.OnMethodEnter
        static void candidateBallot(
            @Advice.FieldValue("candidateTermId") final long candidateTermId,
            @Advice.FieldValue("clusterMembers") final ClusterMember[] clusterMembers)
        {
            while (isStalling &&
                1 == candidateTermId &&
                ClusterMember.isQuorumLeader(clusterMembers, candidateTermId) &&
                !Thread.currentThread().isInterrupted())
            {
                hasStalled = true;
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
        }
    }

    @Test
    @InterruptAfter(30)
    void shouldNotLoseAcknowledgedMessagesDuringOverlappingElections()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).withAppointedLeader(TermZeroLeader.NODE_ID).start();
        systemTestWatcher.cluster(cluster);

        // Node 0 wins term 0 but has not sent NewLeadershipTerm.
        awaitElectionState(cluster.node(TermZeroLeader.NODE_ID), ElectionState.LEADER_LOG_REPLICATION);

        // Clear the appointed leader so the followers can run for term 1 while node 0 is stalled.
        for (int i = 0; i < cluster.memberCount(); i++)
        {
            cluster.node(i).consensusModule().context().appointedLeaderId(NULL_VALUE);
        }

        // The term 1 candidate is in CANDIDATE_BALLOT with a quorum of votes.
        Tests.await(() -> TermOneCandidate.hasStalled);

        // Allow the term 0 leader to announce its leadership.
        TermZeroLeader.isStalling = false;

        // The voter's FOLLOWER_BALLOT times out to CANVASS.
        // The voter and the term 0 leader must elect a leader while the term 1 candidate remains stalled.
        cluster.awaitLeaderWithoutElectionTerminationCheck(NULL_VALUE);

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        // The candidate resumes
        TermOneCandidate.isStalling = false;
        cluster.awaitServicesMessageCount(messageCount);
        cluster.awaitLeader();
        cluster.sendAndAwaitMessages(messageCount, messageCount * 2);
    }
}
