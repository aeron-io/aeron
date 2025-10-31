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

import io.aeron.cluster.service.Cluster;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class AppointedLeaderTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private static final int LEADER_ID = 1;

    private String aeronDirectory;

    @BeforeEach
    void setup(@TempDir final Path tempDir)
    {
        aeronDirectory = tempDir.toString();
    }

    @Test
    @InterruptAfter(20)
    void shouldConnectAndSendKeepAlive()
    {
        final TestCluster cluster = aCluster()
            .withAeronBaseDir(aeronDirectory)
            .withStaticNodes(3)
            .withAppointedLeader(LEADER_ID).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        assertEquals(LEADER_ID, leader.index());
        assertEquals(Cluster.Role.LEADER, leader.role());

        cluster.connectClient();
        assertTrue(cluster.client().sendKeepAlive());
    }

    @Test
    @InterruptAfter(20)
    void shouldEchoMessagesViaService()
    {
        final TestCluster cluster = aCluster()
            .withAeronBaseDir(aeronDirectory)
            .withStaticNodes(3).withAppointedLeader(LEADER_ID).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        assertEquals(LEADER_ID, leader.index());
        assertEquals(Cluster.Role.LEADER, leader.role());

        cluster.connectClient();
        cluster.sendAndAwaitMessages(10);
    }
}
