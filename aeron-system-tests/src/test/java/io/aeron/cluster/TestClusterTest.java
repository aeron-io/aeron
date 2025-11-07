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
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class TestClusterTest
{

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private String aeronDirectory;

    @BeforeEach
    void setup(@TempDir final Path tempDir)
    {
        aeronDirectory = tempDir.toString();
    }

    @Test
    @InterruptAfter(20)
    void testCustomAeronDirectory()
    {
        final AeronCluster.Context clientCtx = new AeronCluster.Context();

        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withAeronBaseDir(aeronDirectory)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        assertNotNull(cluster.connectClient());

        final Set<String> seen = new HashSet<>();
        for (int i = 0; i < cluster.memberCount(); i++)
        {
            final String dir = cluster.node(i).mediaDriver().context().aeronDirectoryName();
            assertTrue(seen.add(dir), "Duplicate Aeron dir: " + dir);
            assertFalse(dir.startsWith("/dev/shm"), "Aeron dir under /dev/shm: " + dir);
        }

        final String dir = clientCtx.aeronDirectoryName();
        assertTrue(seen.add(dir), "Duplicate Aeron dir: " + dir);
        assertFalse(dir.startsWith("/dev/shm"), "Aeron dir under /dev/shm: " + dir);

    }

}
