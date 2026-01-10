/*
 * Copyright 2014-2024 Real Logic Limited.
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
import io.aeron.test.IpTables;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;

import static io.aeron.test.cluster.TestCluster.aCluster;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
@EnabledOnOs(OS.LINUX)
public class ClusterUncommittedStateTest
{
    private static final List<String> HOSTNAMES = List.of("127.2.0.0", "127.2.1.0", "127.2.2.0");
    private static final String CHAIN_NAME = "CLUSTER-TEST";

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();
    private TestCluster cluster;

    @BeforeEach
    void setUp()
    {
        IpTables.setupChain(CHAIN_NAME);
    }

    @AfterEach
    void tearDown()
    {
        IpTables.tearDownChain(CHAIN_NAME);
    }

    @Test
    @SlowTest
    @InterruptAfter(20)
    void shouldRollbackUncommittedSuspendControlToggle()
    {
        cluster = aCluster()
            .withStaticNodes(HOSTNAMES.size())
            .withCustomAddresses(HOSTNAMES)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();
        final List<String> leaderHostname = List.of(HOSTNAMES.get(firstLeader.memberId()));
        final List<String> followerHostnames = new ArrayList<>(HOSTNAMES);
        followerHostnames.remove(firstLeader.memberId());

        IpTables.makeSymmetricNetworkPartition(CHAIN_NAME, leaderHostname, followerHostnames);

        cluster.suspendCluster(firstLeader);
        Tests.await(() -> ConsensusModule.State.SUSPENDED == firstLeader.moduleState());

        Tests.await(() -> null != cluster.findLeader(firstLeader.memberId()));

        IpTables.flushChain(CHAIN_NAME);
        Tests.await(() -> ConsensusModule.State.ACTIVE == firstLeader.moduleState());
    }

    @Test
    @SlowTest
    @InterruptAfter(20)
    void shouldRollbackUncommittedResumeControlToggle()
    {
        cluster = aCluster()
            .withStaticNodes(HOSTNAMES.size())
            .withCustomAddresses(HOSTNAMES)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();
        final List<String> leaderHostname = List.of(HOSTNAMES.get(firstLeader.memberId()));
        final List<String> followerHostnames = new ArrayList<>(HOSTNAMES);
        followerHostnames.remove(firstLeader.memberId());

        cluster.suspendCluster(firstLeader);
        Tests.await(() ->
        {
            boolean allNodesSuspended = true;
            for (int i = 0; i < cluster.memberCount(); ++i)
            {
                if (ConsensusModule.State.SUSPENDED != cluster.node(i).moduleState())
                {
                    allNodesSuspended = false;
                }
            }
            return allNodesSuspended;
        });

        IpTables.makeSymmetricNetworkPartition(CHAIN_NAME, leaderHostname, followerHostnames);

        cluster.resumeCluster(firstLeader);
        Tests.await(() -> ConsensusModule.State.ACTIVE == firstLeader.moduleState());

        Tests.await(() -> null != cluster.findLeader(firstLeader.memberId()));

        IpTables.flushChain(CHAIN_NAME);
        Tests.await(() -> ConsensusModule.State.SUSPENDED == firstLeader.moduleState());
    }

    @Test
    @SlowTest
    @InterruptAfter(20)
    void shouldRollbackUncommittedSnapshotToggle()
    {
        cluster = aCluster()
            .withStaticNodes(HOSTNAMES.size())
            .withCustomAddresses(HOSTNAMES)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();
        final List<String> leaderHostname = List.of(HOSTNAMES.get(firstLeader.memberId()));
        final List<String> followerHostnames = new ArrayList<>(HOSTNAMES);
        followerHostnames.remove(firstLeader.memberId());

        cluster.suspendCluster(firstLeader);
        Tests.await(() ->
        {
            boolean allNodesSuspended = true;
            for (int i = 0; i < cluster.memberCount(); ++i)
            {
                if (ConsensusModule.State.SUSPENDED != cluster.node(i).moduleState())
                {
                    allNodesSuspended = false;
                }
            }
            return allNodesSuspended;
        });

        IpTables.makeSymmetricNetworkPartition(CHAIN_NAME, leaderHostname, followerHostnames);

        cluster.resumeCluster(firstLeader);
        Tests.await(() -> ConsensusModule.State.ACTIVE == firstLeader.moduleState());

        cluster.suspendCluster(firstLeader);
        Tests.await(() -> ConsensusModule.State.SUSPENDED == firstLeader.moduleState());

        cluster.resumeCluster(firstLeader);
        Tests.await(() -> ConsensusModule.State.ACTIVE == firstLeader.moduleState());

        cluster.takeSnapshot(firstLeader);
        Tests.await(() -> ConsensusModule.State.SNAPSHOT == firstLeader.moduleState());

        Tests.await(() -> null != cluster.findLeader(firstLeader.memberId()));

        IpTables.flushChain(CHAIN_NAME);
        Tests.await(() -> ConsensusModule.State.SUSPENDED == firstLeader.moduleState());
    }
}
