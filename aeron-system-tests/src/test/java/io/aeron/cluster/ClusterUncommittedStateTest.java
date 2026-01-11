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

import io.aeron.cluster.codecs.NewLeadershipTermEventEncoder;
import io.aeron.cluster.codecs.SessionOpenEventEncoder;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.IpTables;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.CloseHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;

import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
    void shouldNextCommittedSessionIdReflectOnlyCommittedSessions()
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

        CloseHelper.close(cluster.asyncConnectClient());
        CloseHelper.close(cluster.asyncConnectClient());
        CloseHelper.close(cluster.asyncConnectClient());
        final long estimatedLogFixedLengthSize = (NewLeadershipTermEventEncoder.BLOCK_LENGTH + HEADER_LENGTH) +
            (3 * (SessionOpenEventEncoder.BLOCK_LENGTH + HEADER_LENGTH));
        Tests.await(() -> firstLeader.appendPosition() > estimatedLogFixedLengthSize);

        TestNode newLeader;
        do
        {
            newLeader = cluster.findLeader(firstLeader.memberId());
            Tests.yield();
        }
        while (null == newLeader);

        cluster.takeSnapshot(newLeader);
        Tests.await(() ->
        {
            boolean allSnapshotsTaken = true;
            for (int i = 0; i < cluster.memberCount(); ++i)
            {
                if (firstLeader.memberId() != i && 1 != cluster.getSnapshotCount(cluster.node(i)))
                {
                    allSnapshotsTaken = false;
                }
            }
            return allSnapshotsTaken;
        });

        IpTables.flushChain(CHAIN_NAME);
        Tests.await(() -> 1 == cluster.getSnapshotCount(firstLeader));

        for (int i = 0; i < cluster.memberCount(); ++i)
        {
            assertEquals(1, ClusterTest.readSnapshot(cluster.node(i)));
        }
    }
}