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

import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.TopologyTest;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static io.aeron.test.cluster.TestCluster.aCluster;

@TopologyTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
@EnabledOnOs(OS.LINUX)
class ClusterNetworkPartitionTest
{
    private static final List<String> HOSTNAMES = Arrays.asList("127.1.0.0", "127.1.1.0", "127.1.2.0");
    private static final String CHAIN_NAME = "CLUSTER-TEST";

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private TestCluster cluster = null;

    private File baseDir;

    @BeforeEach
    void setUp()
    {
        createChain(CHAIN_NAME);
        flushChain(CHAIN_NAME);
        addToInput(CHAIN_NAME);
    }

    @AfterEach
    void tearDown()
    {
        flushChain(CHAIN_NAME);
        removeFromInput(CHAIN_NAME);
        deleteChain(CHAIN_NAME);
    }

    @Test
    @InterruptAfter(30)
    void shouldStartClusterThenElectNewLeaderAfterPartition()
    {
        cluster = aCluster()
            .withStaticNodes(3)
            .withCustomAddresses(HOSTNAMES)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();

        cluster.connectClient();
        cluster.sendAndAwaitMessages(1);

        makeNetworkPartition(HOSTNAMES, firstLeader.index());

        cluster.sendMessages(20);

        cluster.awaitLeader(firstLeader.index());

        final TestNode node = cluster.node(firstLeader.index());
        cluster.awaitNodeState(node, (n) -> n.electionState() == ElectionState.CANVASS);

        flushChain(CHAIN_NAME);

        cluster.awaitNodeState(node, (n) -> n.electionState() == ElectionState.CLOSED);

        cluster.sendMessages(10);
        cluster.awaitServicesMessageCount(11);
    }

    private void deleteChain(final String chainName)
    {
        runIpTablesCmd(false, "sudo", "iptables", "-X", chainName);
    }

    private void removeFromInput(final String chainName)
    {
        boolean isSuccess;
        do
        {
            isSuccess = runIpTablesCmd(true, "sudo", "iptables", "-D", "INPUT", "-j", chainName);
        }
        while (isSuccess);
    }

    private void addToInput(final String chainName)
    {
        runIpTablesCmd(true, "sudo", "iptables", "-A", "INPUT", "-j", chainName);
    }

    private void makeNetworkPartition(final List<String> hostnames, final int toIsolateIndex)
    {
        final String toIsolateHostname = hostnames.get(toIsolateIndex);
        for (int i = 0; i < hostnames.size(); i++)
        {
            if (i != toIsolateIndex)
            {
                final String otherHostname = hostnames.get(i);
                runIpTablesCmd(
                    false, "sudo", "iptables", "-A", CHAIN_NAME,
                    "-d", toIsolateHostname,
                    "-s", otherHostname,
                    "-j", "DROP");
                runIpTablesCmd(
                    false, "sudo", "iptables", "-A", CHAIN_NAME,
                    "-d", otherHostname,
                    "-s", toIsolateHostname,
                    "-j", "DROP");
            }
        }

        runIpTablesCmd(false, "sudo", "iptables", "-A", CHAIN_NAME, "-j", "RETURN");
    }

    private void createChain(final String chainName)
    {
        runIpTablesCmd(true, "sudo", "-n", "iptables", "-N", chainName);
    }

    private void flushChain(final String chainName)
    {
        runIpTablesCmd(true, "sudo", "-n", "iptables", "-F", chainName);
    }

    private static boolean runIpTablesCmd(final boolean ignoreError, final String... command)
    {
        try
        {
            final Process start = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();

            final ByteArrayOutputStream commandOutput = new ByteArrayOutputStream();
            try (InputStream inputStream = start.getInputStream())
            {
                final byte[] block = new byte[4096];
                while (start.isAlive())
                {
                    final int read = inputStream.read(block);
                    if (0 < read)
                    {
                        commandOutput.write(block, 0, read);
                    }
                    Tests.yield();
                }
            }

            final boolean isSuccess = 0 == start.exitValue();
            if (!isSuccess && !ignoreError)
            {
                final String commandMsg = commandOutput.toString(StandardCharsets.UTF_8);
                throw new RuntimeException("Command: '" + String.join(" ", command) + "' failed - " + commandMsg);
            }

            return isSuccess;
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
