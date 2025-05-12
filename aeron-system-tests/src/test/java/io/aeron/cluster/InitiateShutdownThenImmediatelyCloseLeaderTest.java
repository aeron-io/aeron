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

import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.service.Cluster;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class InitiateShutdownThenImmediatelyCloseLeaderTest
{
    private MethodCallBlocker methodCallBlocker;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @BeforeEach
    void setUp()
    {
        methodCallBlocker = new MethodCallBlocker();
    }

    @AfterEach
    void tearDown()
    {
        methodCallBlocker.removeInstrumentation();
    }

    @Test
    @InterruptAfter(20)
    @Disabled("Intermittent")
    void shouldNotSendTerminationAckToNewLeaderWhenOldLeaderInitiatedShutdownAndImmediatelyClosed()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);
        systemTestWatcher.ignoreErrorsMatching(error -> error.contains("termination ack not sent"));

        final TestNode firstLeader = cluster.awaitLeader();
        assertEquals(Cluster.Role.LEADER, firstLeader.role());

        final int aIndex = (firstLeader.index() + 1) % 3;
        final int bIndex = (firstLeader.index() + 2) % 3;
        final TestNode aNode = cluster.node(aIndex);
        final TestNode bNode = cluster.node(bIndex);

        aNode.isTerminationExpected(true);
        bNode.isTerminationExpected(true);

        final MethodCallBlocker.Controller aOnTerminationPosition = methodCallBlocker.getControllerFor(
            "io.aeron.cluster.ConsensusModuleAgent",
            "onTerminationPosition",
            "consensus-module-0-" + aIndex
        );

        final MethodCallBlocker.Controller aAckBlocker = methodCallBlocker.getControllerFor(
            "io.aeron.cluster.service.ConsensusModuleProxy",
            "ack",
            "clustered-service-0-" + aIndex + "-0"
        );

        final MethodCallBlocker.Controller bPollArchiveEventsBlocker = methodCallBlocker.getControllerFor(
            "io.aeron.cluster.ConsensusModuleAgent",
            "consensusWork",
            "consensus-module-0-" + bIndex
        );

        aOnTerminationPosition.blockNextEntry();
        bPollArchiveEventsBlocker.blockNextEntry();
        bPollArchiveEventsBlocker.awaitBlocked();

        cluster.shutdownCluster(firstLeader);

        aOnTerminationPosition.awaitBlocked();

        firstLeader.gracefulClose();

        final long logRecordingId = 0;
        final long archiveId = bNode.archive().context().archiveId();
        final CountersReader counters = bNode.mediaDriver().counters();

        Tests.await(() ->
            RecordingPos.findCounterIdByRecording(counters, logRecordingId, archiveId) == RecordingPos.NULL_RECORDING_ID
        );

        aAckBlocker.blockNextEntry();
        bPollArchiveEventsBlocker.release();
        aOnTerminationPosition.release();

        Tests.await(() -> aNode.role() == Cluster.Role.LEADER || bNode.role() == Cluster.Role.LEADER);
        final TestNode secondLeader = aNode.role() == Cluster.Role.LEADER ? aNode : bNode;

        aAckBlocker.release();

        if (secondLeader.index() == aIndex)
        {
            cluster.awaitNodeTermination(bNode);
        }
        else if (secondLeader.index() == bIndex)
        {
            cluster.awaitNodeTermination(aNode);
        }
        secondLeader.close();
    }
}
