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
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.cluster.ClusterTests;
import io.aeron.test.cluster.StubClusteredService;
import io.aeron.test.driver.TestMediaDriver;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.aeron.CommonContext.THREAD_NAMING_CLASSIC;
import static io.aeron.CommonContext.THREAD_NAMING_NEW;
import static io.aeron.CommonContext.THREAD_NAMING_PROP_NAME;
import static io.aeron.cluster.ClusterBackup.AERON_CLUSTER_BACKUP_THREAD_NAME;
import static io.aeron.cluster.ClusterBackup.AERON_CLUSTER_BACKUP_THREAD_NAME_CLASSIC;
import static io.aeron.cluster.ConsensusModule.AERON_CLUSTER_CONSENSUS_THREAD_NAME;
import static io.aeron.test.TestContexts.LOCALHOST_SINGLE_HOST_CLUSTER_MEMBERS;
import static java.lang.management.ManagementFactory.getThreadMXBean;

public class ClusterThreadNamingTest
{

    private static Stream<Arguments> clusterBackupThreadNamingModes()
    {
        return Stream.of(
            Arguments.of(THREAD_NAMING_CLASSIC, AERON_CLUSTER_BACKUP_THREAD_NAME_CLASSIC),
            Arguments.of(THREAD_NAMING_NEW, AERON_CLUSTER_BACKUP_THREAD_NAME));
    }

    @ParameterizedTest
    @MethodSource("clusterBackupThreadNamingModes")
    @SuppressWarnings("try")
    void shouldUseCorrectClusterBackupThreadName(
        final String threadNamingMode, final String expectedName, @TempDir final Path tmpDir)
    {
        System.setProperty(THREAD_NAMING_PROP_NAME, threadNamingMode);
        try
        {
            final String aeronDirectoryName = tmpDir.resolve("aeron").toString();
            try (TestMediaDriver mediaDriver = TestMediaDriver.launch(new MediaDriver.Context()
                    .aeronDirectoryName(aeronDirectoryName)
                    .threadingMode(ThreadingMode.SHARED)
                    .termBufferSparseFile(true)
                    .dirDeleteOnStart(true), null);
                Archive archive = Archive.launch(TestContexts.localhostArchive()
                    .aeronDirectoryName(aeronDirectoryName)
                    .archiveDir(tmpDir.resolve("archive").toFile())
                    .threadingMode(ArchiveThreadingMode.SHARED)
                    .deleteArchiveOnStart(true)))
            {

                try (ClusterBackup clusterBackup = ClusterBackup.launch(new ClusterBackup.Context()
                    .aeronDirectoryName(aeronDirectoryName)
                    .clusterDir(tmpDir.resolve("cluster").toFile())
                    .errorHandler(ClusterTests.errorHandler(0))
                    .consensusChannel("aeron:udp?endpoint=localhost:0")
                    .clusterConsensusEndpoints("localhost:20001")
                    .catchupEndpoint("localhost:0")
                    .deleteDirOnStart(true)))
                {
                    awaitThread(expectedName);
                }
            }
        }
        finally
        {
            System.clearProperty(THREAD_NAMING_PROP_NAME);
        }
    }

    private static Stream<Arguments> consensusModuleThreadNamingModes()
    {
        return Stream.of(
            Arguments.of(THREAD_NAMING_CLASSIC, null, "consensus-module-0-0"),
            Arguments.of(THREAD_NAMING_NEW, null, AERON_CLUSTER_CONSENSUS_THREAD_NAME),
            Arguments.of(THREAD_NAMING_CLASSIC, "consensus-override", "consensus-override"),
            Arguments.of(THREAD_NAMING_NEW, "consensus-override", "consensus-override"));
    }

    @ParameterizedTest
    @MethodSource("consensusModuleThreadNamingModes")
    @SuppressWarnings("try")
    void shouldUseCorrectConsensusModuleThreadName(
        final String threadNamingMode,
        final String agentRoleNameOverride,
        final String expectedName,
        @TempDir final Path tmpDir)
    {
        System.setProperty(THREAD_NAMING_PROP_NAME, threadNamingMode);
        try
        {
            final String aeronDirectoryName = tmpDir.resolve("aeron").toString();
            final ConsensusModule.Context context = TestContexts.localhostConsensusModule()
                .aeronDirectoryName(aeronDirectoryName)
                .clusterDir(tmpDir.resolve("cluster").toFile())
                .errorHandler(ClusterTests.errorHandler(0))
                .terminationHook(ClusterTests.NOOP_TERMINATION_HOOK)
                .logChannel("aeron:ipc")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .ingressChannel("aeron:udp")
                .clusterId(0)
                .clusterMemberId(0)
                .deleteDirOnStart(true);
            if (null != agentRoleNameOverride)
            {
                context.agentRoleName(agentRoleNameOverride);
            }

            try (TestMediaDriver mediaDriver = TestMediaDriver.launch(new MediaDriver.Context()
                    .aeronDirectoryName(aeronDirectoryName)
                    .threadingMode(ThreadingMode.SHARED)
                    .termBufferSparseFile(true)
                    .dirDeleteOnStart(true), null);
                Archive archive = Archive.launch(TestContexts.localhostArchive()
                    .aeronDirectoryName(aeronDirectoryName)
                    .archiveDir(tmpDir.resolve("archive").toFile())
                    .threadingMode(ArchiveThreadingMode.SHARED)
                    .deleteArchiveOnStart(true));
                ConsensusModule consensusModule = ConsensusModule.launch(context))
            {

                final ClusteredService clusteredService = new StubClusteredService();
                try (ClusteredServiceContainer container = ClusteredServiceContainer.launch(
                    new ClusteredServiceContainer.Context()
                        .aeronDirectoryName(aeronDirectoryName)
                        .clusterDir(tmpDir.resolve("cluster").toFile())
                        .clusteredService(clusteredService)
                        .errorHandler(ClusterTests.errorHandler(0))
                        .clusterId(0)
                        .serviceId(0)))
                {
                    awaitThread(expectedName);
                }
            }
        }
        finally
        {
            System.clearProperty(THREAD_NAMING_PROP_NAME);
        }
    }

    private static Stream<Arguments> clusteredServiceThreadNamingModes()
    {
        return Stream.of(
            Arguments.of(THREAD_NAMING_CLASSIC, null, "clustered-service-0-0"),
            Arguments.of(THREAD_NAMING_NEW, null, "aeron-cl-cs-0"),
            Arguments.of(THREAD_NAMING_CLASSIC, "clustered-service-override", "clustered-service-override"),
            Arguments.of(THREAD_NAMING_NEW, "clustered-service-override", "clustered-service-override"));
    }

    @ParameterizedTest
    @MethodSource("clusteredServiceThreadNamingModes")
    @SuppressWarnings("try")
    void shouldUseCorrectClusteredServiceThreadName(
        final String threadNamingMode,
        final String serviceNameOverride,
        final String expectedName,
        @TempDir final Path tmpDir)
    {
        System.setProperty(THREAD_NAMING_PROP_NAME, threadNamingMode);
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
            if (null != serviceNameOverride)
            {
                context.serviceName(serviceNameOverride);
            }

            try (TestMediaDriver mediaDriver = TestMediaDriver.launch(new MediaDriver.Context()
                    .aeronDirectoryName(aeronDirectoryName)
                    .threadingMode(ThreadingMode.SHARED)
                    .termBufferSparseFile(true)
                    .dirDeleteOnStart(true), null);
                Archive archive = Archive.launch(TestContexts.localhostArchive()
                    .aeronDirectoryName(aeronDirectoryName)
                    .archiveDir(tmpDir.resolve("archive").toFile())
                    .threadingMode(ArchiveThreadingMode.SHARED)
                    .deleteArchiveOnStart(true));
                ConsensusModule consensusModule = ConsensusModule.launch(new ConsensusModule.Context()
                    .aeronDirectoryName(aeronDirectoryName)
                    .clusterDir(tmpDir.resolve("cluster").toFile())
                    .errorHandler(ClusterTests.errorHandler(0))
                    .terminationHook(ClusterTests.NOOP_TERMINATION_HOOK)
                    .logChannel("aeron:ipc")
                    .replicationChannel("aeron:udp?endpoint=localhost:0")
                    .ingressChannel("aeron:udp")
                    .clusterMembers(LOCALHOST_SINGLE_HOST_CLUSTER_MEMBERS)
                    .clusterId(0)
                    .clusterMemberId(0)
                    .deleteDirOnStart(true)))
            {

                try (ClusteredServiceContainer container = ClusteredServiceContainer.launch(context))
                {
                    awaitThread(expectedName);
                }
            }
        }
        finally
        {
            System.clearProperty(THREAD_NAMING_PROP_NAME);
        }
    }

    private static void awaitThread(final String expectedName)
    {
        final ThreadMXBean threadBean = getThreadMXBean();

        Tests.await(
            () ->
            {
                final long[] threadIds = threadBean.getAllThreadIds();
                final ThreadInfo[] threadInfos = threadBean.getThreadInfo(threadIds, 0);
                return Arrays.stream(threadInfos)
                    .filter(Objects::nonNull)
                    .map(ThreadInfo::getThreadName)
                    .anyMatch(expectedName::equals);
            },
            TimeUnit.SECONDS.toNanos(1));
    }
}
