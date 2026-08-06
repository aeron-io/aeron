/*
 * Copyright 2014-2026 Real Logic Limited.
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

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.cluster.ClusterTests;
import io.aeron.test.cluster.StubClusteredService;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.driver.TestMediaDriver;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

import static io.aeron.CommonContext.THREAD_NAMING_CLASSIC;
import static io.aeron.CommonContext.THREAD_NAMING_NEW;
import static io.aeron.CommonContext.THREAD_NAMING_PROP_NAME;
import static io.aeron.cluster.ClusterBackup.AERON_CLUSTER_BACKUP_THREAD_NAME;
import static io.aeron.cluster.ClusterBackup.AERON_CLUSTER_BACKUP_THREAD_NAME_CLASSIC;
import static io.aeron.cluster.ConsensusModule.AERON_CLUSTER_CONSENSUS_THREAD_NAME;
import static io.aeron.test.TestPropertiesUtil.backupAndOverrideSystemProperties;
import static io.aeron.test.TestPropertiesUtil.restoreSystemProperties;
import static io.aeron.test.cluster.TestCluster.aCluster;
import static java.lang.management.ManagementFactory.getThreadMXBean;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class ClusterThreadNamingTest
{
    private static Stream<Arguments> threadNamingModes()
    {
        // NOTE: The following cases do not cover the clustered service defaults.
        return Stream.of(
            Arguments.of(
                THREAD_NAMING_CLASSIC, null, null,
                new String[]{
                    AERON_CLUSTER_BACKUP_THREAD_NAME_CLASSIC,
                    "consensus-module-0-0"
                }),
            Arguments.of(
                THREAD_NAMING_NEW, null, null,
                new String[]{
                    AERON_CLUSTER_BACKUP_THREAD_NAME,
                    AERON_CLUSTER_CONSENSUS_THREAD_NAME
                }),
            Arguments.of(
                THREAD_NAMING_CLASSIC, "consensus-override", "clustered-service-override",
                new String[]{
                    AERON_CLUSTER_BACKUP_THREAD_NAME_CLASSIC,
                    "consensus-override",
                    "clustered-service-override" }),
            Arguments.of(
                THREAD_NAMING_NEW, "consensus-override", "clustered-service-override",
                new String[]{
                    AERON_CLUSTER_BACKUP_THREAD_NAME,
                    "consensus-override",
                    "clustered-service-override" }));
    }

    @ParameterizedTest
    @MethodSource("threadNamingModes")
    @InterruptAfter(5)
    void shouldUseCorrectThreadNamesByNamingMode(
        final String threadNamingMode,
        final String consensusAgentRoleNameOverride,
        final String clusterServiceNameOverride,
        final String[] expectedThreadNames)
    {
        final Properties testProperties = new Properties();
        testProperties.setProperty(THREAD_NAMING_PROP_NAME, threadNamingMode);
        final Properties backup = backupAndOverrideSystemProperties(new Properties(), testProperties);
        try
        {
            try (
                TestCluster cluster = aCluster()
                    .withStaticNodes(3)
                    .withConsensusModuleAgentRoleName(consensusAgentRoleNameOverride)
                    .withClusterServiceName(clusterServiceNameOverride)
                    .start())
            {
                cluster.awaitLeader();
                cluster.startClusterBackupNode(true);
                awaitThreads(expectedThreadNames);
            }

        }
        finally
        {
            restoreSystemProperties(backup);
        }
    }

    private static Stream<Arguments> clusteredServiceDefaultThreadNamingModes()
    {
        return Stream.of(
            Arguments.of(THREAD_NAMING_CLASSIC, "clustered-service-0-0"),
            Arguments.of(THREAD_NAMING_NEW, "aeron-cl-cs-0"));
    }

    @ParameterizedTest
    @MethodSource("clusteredServiceDefaultThreadNamingModes")
    @InterruptAfter(5)
    @SuppressWarnings("try")
    void shouldUseCorrectDefaultClusteredServiceThreadNamesPerMode(
        final String threadNamingMode, final String expectedName, @TempDir final Path tmpDir)
    {
        final Properties testProperties = new Properties();
        testProperties.setProperty(THREAD_NAMING_PROP_NAME, threadNamingMode);
        final Properties backup = backupAndOverrideSystemProperties(new Properties(), testProperties);
        try
        {
            final String aeronDirectoryName = tmpDir.resolve("aeron").toString();
            final ClusteredService clusteredService = new StubClusteredService();
            final ClusteredServiceContainer.Context context = new ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDirectoryName)
                .clusterDir(tmpDir.resolve("cluster").toFile())
                .clusteredService(clusteredService)
                .errorHandler(ClusterTests.errorHandler(0))
                .clusterId(0)
                .serviceId(0);

            try (TestMediaDriver mediaDriver = TestMediaDriver.launch(
                    new MediaDriver.Context()
                        .aeronDirectoryName(aeronDirectoryName)
                        .threadingMode(ThreadingMode.SHARED)
                        .termBufferSparseFile(true)
                        .dirDeleteOnStart(true), null);
                Archive archive = Archive.launch(
                    TestContexts.localhostArchive()
                        .aeronDirectoryName(aeronDirectoryName)
                        .archiveDir(tmpDir.resolve("archive").toFile())
                        .threadingMode(ArchiveThreadingMode.SHARED)
                        .deleteArchiveOnStart(true));
                ConsensusModule consensusModule = ConsensusModule.launch(
                    TestContexts.localhostConsensusModule()
                        .aeronDirectoryName(aeronDirectoryName)
                        .clusterDir(tmpDir.resolve("cluster").toFile())
                        .errorHandler(ClusterTests.errorHandler(0))
                        .terminationHook(ClusterTests.NOOP_TERMINATION_HOOK)
                        .logChannel("aeron:ipc")
                        .replicationChannel("aeron:udp?endpoint=localhost:0")
                        .ingressChannel("aeron:udp")
                        .clusterId(0)
                        .clusterMemberId(0)
                        .deleteDirOnStart(true));
                ClusteredServiceContainer container = ClusteredServiceContainer.launch(context))
            {
                Tests.await(() ->
                    ElectionState.CLOSED == ElectionState.get(consensusModule.context().electionStateCounter()));
                awaitThreads(expectedName);
            }
        }
        finally
        {
            restoreSystemProperties(backup);
        }
    }

    private static void awaitThreads(final String... expectedThreadNames)
    {
        final ThreadMXBean threadBean = getThreadMXBean();
        final String[] sortedExpected = expectedThreadNames.clone();
        Arrays.sort(sortedExpected);

        Tests.await(
            () ->
            {
                final long[] threadIds = threadBean.getAllThreadIds();
                final ThreadInfo[] threadInfos = threadBean.getThreadInfo(threadIds, 0);
                final String[] actual = Arrays.stream(threadInfos)
                    .filter(Objects::nonNull)
                    .map(ThreadInfo::getThreadName)
                    .filter(name -> Arrays.binarySearch(sortedExpected, name) >= 0)
                    .distinct()
                    .sorted()
                    .toArray(String[]::new);
                return Arrays.equals(sortedExpected, actual);
            });
    }
}
