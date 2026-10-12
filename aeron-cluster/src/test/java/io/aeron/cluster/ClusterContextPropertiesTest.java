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

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static io.aeron.CommonContext.AERON_DIR_PROP_NAME;
import static io.aeron.cluster.ConsensusModule.Configuration.CLUSTER_MEMBERS_PROP_NAME;
import static io.aeron.cluster.ConsensusModule.Configuration.CLUSTER_MEMBER_ID_PROP_NAME;
import static io.aeron.cluster.ConsensusModule.Configuration.SERVICE_COUNT_PROP_NAME;
import static io.aeron.cluster.ConsensusModule.Configuration.SESSION_TIMEOUT_PROP_NAME;
import static io.aeron.cluster.client.AeronCluster.Configuration.INGRESS_CHANNEL_PROP_NAME;
import static io.aeron.cluster.client.AeronCluster.Configuration.MESSAGE_TIMEOUT_PROP_NAME;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.CLUSTER_DIR_PROP_NAME;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.CLUSTER_ID_PROP_NAME;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.SERVICE_NAME_PROP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class ClusterContextPropertiesTest
{
    @AfterEach
    void tearDown()
    {
        System.clearProperty(CLUSTER_MEMBER_ID_PROP_NAME);
        System.clearProperty(CLUSTER_ID_PROP_NAME);
    }

    @Test
    void suppliedPropertiesConfigureTheConsensusModuleContext()
    {
        final Properties properties = new Properties();
        properties.setProperty(AERON_DIR_PROP_NAME, "/tmp/aeron-a");
        properties.setProperty(CLUSTER_MEMBER_ID_PROP_NAME, "3");
        properties.setProperty(CLUSTER_MEMBERS_PROP_NAME, "0,localhost:20000,localhost:20001," +
            "localhost:20002,localhost:20003,localhost:8010");
        properties.setProperty(SERVICE_COUNT_PROP_NAME, "5");
        properties.setProperty(SESSION_TIMEOUT_PROP_NAME, "9s");

        final ConsensusModule.Context context = new ConsensusModule.Context(properties);

        assertSame(properties, context.properties());
        assertEquals("/tmp/aeron-a", context.aeronDirectoryName());
        assertEquals(3, context.clusterMemberId());
        assertEquals(5, context.serviceCount());
        assertEquals(TimeUnit.SECONDS.toNanos(9), context.sessionTimeoutNs());
    }

    @Test
    void twoConsensusModuleContextsFromDifferentPropertiesAreIndependent()
    {
        final Properties a = new Properties();
        a.setProperty(CLUSTER_MEMBER_ID_PROP_NAME, "0");
        a.setProperty(CLUSTER_DIR_PROP_NAME, "/tmp/cluster-0");

        final Properties b = new Properties();
        b.setProperty(CLUSTER_MEMBER_ID_PROP_NAME, "1");
        b.setProperty(CLUSTER_DIR_PROP_NAME, "/tmp/cluster-1");

        final ConsensusModule.Context contextA = new ConsensusModule.Context(a);
        final ConsensusModule.Context contextB = new ConsensusModule.Context(b);

        assertEquals(0, contextA.clusterMemberId());
        assertEquals(1, contextB.clusterMemberId());
        assertEquals("/tmp/cluster-0", contextA.clusterDirectoryName());
        assertEquals("/tmp/cluster-1", contextB.clusterDirectoryName());
        assertNotEquals(contextA.clusterDirectoryName(), contextB.clusterDirectoryName());
    }

    @Test
    void suppliedPropertiesShadowSystemPropertiesAndAreNotWrittenBackToThem()
    {
        System.setProperty(CLUSTER_MEMBER_ID_PROP_NAME, "1");

        final Properties properties = new Properties();
        properties.setProperty(CLUSTER_MEMBER_ID_PROP_NAME, "2");

        assertEquals(2, new ConsensusModule.Context(properties).clusterMemberId());
        assertEquals(1, new ConsensusModule.Context().clusterMemberId());
        assertEquals("1", System.getProperty(CLUSTER_MEMBER_ID_PROP_NAME));
    }

    @Test
    void suppliedPropertiesConfigureTheClusteredServiceContainerContext()
    {
        final Properties properties = new Properties();
        properties.setProperty(CLUSTER_ID_PROP_NAME, "8");
        properties.setProperty(SERVICE_NAME_PROP_NAME, "service-a");
        properties.setProperty(CLUSTER_DIR_PROP_NAME, "/tmp/cluster-a");

        final ClusteredServiceContainer.Context context = new ClusteredServiceContainer.Context(properties);

        assertSame(properties, context.properties());
        assertEquals(8, context.clusterId());
        assertEquals("service-a", context.serviceName());
        assertEquals("/tmp/cluster-a", context.clusterDirectoryName());
    }

    @Test
    void suppliedPropertiesConfigureTheClusterBackupContext()
    {
        final Properties properties = new Properties();
        properties.setProperty(AERON_DIR_PROP_NAME, "/tmp/aeron-backup");
        properties.setProperty(CLUSTER_DIR_PROP_NAME, "/tmp/cluster-backup");

        final ClusterBackup.Context context = new ClusterBackup.Context(properties);

        assertSame(properties, context.properties());
        assertEquals("/tmp/cluster-backup", context.clusterDirectoryName());
    }

    @Test
    void suppliedPropertiesConfigureTheAeronClusterClientContext()
    {
        final Properties properties = new Properties();
        properties.setProperty(AERON_DIR_PROP_NAME, "/tmp/aeron-a");
        properties.setProperty(INGRESS_CHANNEL_PROP_NAME, "aeron:udp?term-length=64k");
        properties.setProperty(MESSAGE_TIMEOUT_PROP_NAME, "4s");

        final AeronCluster.Context context = new AeronCluster.Context(properties);

        assertSame(properties, context.properties());
        assertEquals("/tmp/aeron-a", context.aeronDirectoryName());
        assertEquals("aeron:udp?term-length=64k", context.ingressChannel());
        assertEquals(TimeUnit.SECONDS.toNanos(4), context.messageTimeoutNs());
    }
}
