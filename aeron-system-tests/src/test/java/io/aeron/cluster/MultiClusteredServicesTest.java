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

import io.aeron.cluster.service.ClientSession;
import io.aeron.logbuffer.Header;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.atomic.AtomicLong;

import static io.aeron.test.cluster.TestCluster.aCluster;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class MultiClusteredServicesTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @BeforeEach
    void setUp()
    {
    }

    static final class Service extends TestNode.TestService
    {
        final AtomicLong count = new AtomicLong(0);

        @Override
        public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            count.incrementAndGet();
        }
    }

    @Test
    @InterruptAfter(20)
    void shouldSupportMultipleServicesPerNode()
    {
        final Service serviceA = new Service();
        final Service serviceB = new Service();
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withServiceSupplier((i) -> new TestNode.TestService[]{ serviceA, serviceB })
            .start(3);
        systemTestWatcher.cluster(cluster);

        cluster.connectClient();
        cluster.sendMessages(3);

        Tests.awaitValue(serviceA.count, 3);
        Tests.awaitValue(serviceB.count, 3);
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 2 })
    @InterruptAfter(40)
    void shouldContinueFromLogIfSnapshotThrowsException(final int failedServiceCount)
    {
        final TestNode.TestService[][] clusterTestServices =
            {
                { new TestNode.TestService(), new TestNode.TestService() },
                { new TestNode.TestService(), new TestNode.TestService() },
                { new TestNode.TestService(), new TestNode.TestService() }
            };

        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withServiceSupplier((i) -> clusterTestServices[i])
            .start(3);
        systemTestWatcher.cluster(cluster);

        final TestNode leader0 = cluster.awaitLeader();

        final int messageCount = 3;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitServicesMessageCount(messageCount);

        for (final TestNode.TestService[] testServices : clusterTestServices)
        {
            for (int i = 0; i < failedServiceCount; i++)
            {
                testServices[i].failNextSnapshot(true);
            }
        }

        cluster.takeSnapshot(leader0);
        cluster.sendMessages(messageCount);
        cluster.awaitServicesMessageCount(messageCount * 2);

        cluster.awaitSnapshotCount(0);
        cluster.awaitServiceErrors(failedServiceCount);

        Tests.sleep(1_000);

        cluster.stopAllNodes();
        cluster.restartAllNodes(false);
        cluster.awaitLeader();
        cluster.reconnectClient();

        cluster.sendMessages(messageCount);
        cluster.awaitServicesMessageCount(messageCount * 3);
    }
}
