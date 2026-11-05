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

import io.aeron.cluster.client.ClusterException;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import net.bytebuddy.asm.Advice;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static io.aeron.test.cluster.TestCluster.awaitElectionClosed;

@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class FailedFirstElectionClusterTest
{
    private static ClusterInstrumentor clusterInstrumentor;

    @BeforeAll
    static void beforeAll()
    {
        clusterInstrumentor = new ClusterInstrumentor(FailFirstElectionIntercept.class, "Election", "onRequestVote");
    }

    @AfterAll
    static void afterAll()
    {
        clusterInstrumentor.reset();
    }

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    public static class FailFirstElectionIntercept
    {
        @Advice.OnMethodEnter
        static void onRequestVote(
            final long logLeadershipTermId,
            final long logPosition,
            final long candidateTermId,
            final int candidateId,
            @Advice.This final Object thisObject)
        {
            final Election election = (Election)thisObject;
            if (candidateId != election.thisMemberId() && candidateTermId == 0)
            {
                throw new ClusterException(
                    "Forced failure: memberId=" + election.thisMemberId() + ", candidateTermId=" + candidateTermId);
            }
        }
    }

    @Test
    @InterruptAfter(120)
    void shouldRecoverWhenFollowerIsMultipleTermsBehindFromEmptyLog()
    {
        final int numNodes = 3;
        final int messageCount = 10;
        final int numTerms = 3;

        final TestCluster cluster = aCluster().withStaticNodes(numNodes).start(2);
        cluster.awaitLeader();
        cluster.connectClient();

        systemTestWatcher.cluster(cluster);
        systemTestWatcher.ignoreErrorsMatching((s) -> s.contains("Forced failure"));

        int totalMessages = 0;

        for (int i = 0; i < numTerms; i++)
        {
            final TestNode oldLeader = cluster.awaitLeader();

            cluster.sendMessages(messageCount);
            totalMessages += messageCount;
            cluster.awaitResponseMessageCount(totalMessages);

            cluster.stopNode(oldLeader);
            cluster.startStaticNode(oldLeader.index(), false);
            cluster.awaitLeader(oldLeader.index());
        }

        awaitElectionClosed(cluster.startStaticNode(2, true));

        cluster.sendMessages(messageCount);
        totalMessages += messageCount;

        cluster.awaitResponseMessageCount(totalMessages);
        cluster.awaitServicesMessageCount(totalMessages);

        cluster.assertRecordingLogsEqual();
    }
}
