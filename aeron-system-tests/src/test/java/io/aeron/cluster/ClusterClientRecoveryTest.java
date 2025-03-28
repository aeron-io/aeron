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

import io.aeron.cluster.client.AeronCluster;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static io.aeron.test.cluster.TestCluster.awaitElectionClosed;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SlowTest
@ExtendWith({EventLogExtension.class, InterruptingTestCallback.class })
class ClusterClientRecoveryTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(30)
    @Disabled
    void shouldCloseClusterClientAfterClusterShutdown()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode follower = cluster.followers().get(0);
        awaitElectionClosed(follower);

        final AeronCluster client = cluster.connectClient();

        assertEquals(false, client.isClosed());
        assertEquals(false, client.ingressPublication().isClosed());
        assertEquals(true, client.sendKeepAlive());

        cluster.node(0).close();
        cluster.node(1).close();
        cluster.node(2).close();

        final IdleStrategy idleStrategy = new SleepingIdleStrategy();
        idleStrategy.reset();

        while (true)
        {
            client.sendKeepAlive();
            client.pollEgress();

            final boolean clientClosed = client.isClosed();
            final boolean ingressPublicationClosed = client.ingressPublication().isClosed();

            if (clientClosed && ingressPublicationClosed)
            {
                break;
            }

            Tests.idle(idleStrategy, "AeronCluster#isClosed = %s, AeronCluster#ingressPublication#isClosed = %s",
                clientClosed, ingressPublicationClosed);
        }
    }
}
