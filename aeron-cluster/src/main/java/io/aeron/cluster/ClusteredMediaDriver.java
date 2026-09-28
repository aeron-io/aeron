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

import io.aeron.archive.Archive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.topology.AffinityRegistry;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.SystemUtil;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.status.AtomicCounter;

import static org.agrona.SystemUtil.loadPropertiesFiles;

/**
 * Clustered media driver which is an aggregate of a {@link MediaDriver}, {@link Archive},
 * and a {@link ConsensusModule}.
 */
public class ClusteredMediaDriver implements AutoCloseable
{
    private final MediaDriver driver;
    private final Archive archive;
    private final ConsensusModule consensusModule;

    ClusteredMediaDriver(final MediaDriver driver, final Archive archive, final ConsensusModule consensusModule)
    {
        this.driver = driver;
        this.archive = archive;
        this.consensusModule = consensusModule;
    }

    /**
     * Launch the clustered media driver aggregate and await a shutdown signal.
     *
     * @param args command line argument which is a list for properties files as URLs or filenames.
     */
    @SuppressWarnings("try")
    public static void main(final String[] args)
    {
        loadPropertiesFiles(args);

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
            ClusteredMediaDriver ignore = launch(
                new MediaDriver.Context().terminationHook(barrier::signalAll),
                new Archive.Context(),
                new ConsensusModule.Context().terminationHook(barrier::signalAll)))
        {
            barrier.await();
            System.out.println("Shutdown ClusteredMediaDriver...");
        }
    }

    /**
     * Launch a new {@link ClusteredMediaDriver} with default contexts.
     *
     * @return a new {@link ClusteredMediaDriver} with default contexts.
     */
    public static ClusteredMediaDriver launch()
    {
        return launch(new MediaDriver.Context(), new Archive.Context(), new ConsensusModule.Context());
    }

    /**
     * Launch a new {@link ClusteredMediaDriver} with provided contexts.
     * <p>
     * Unless an {@link AffinityRegistry} has been supplied to any of the contexts, a single registry is shared by
     * all components so their pinned threads are remapped onto distinct CPUs of the effective cpuset. Topology
     * validation runs once if any component enables it, and warnings are fatal if any component that enables
     * validation treats them as errors.
     *
     * @param driverCtx          for configuring the {@link MediaDriver}.
     * @param archiveCtx         for configuring the {@link Archive}.
     * @param consensusModuleCtx for the configuration of the {@link ConsensusModule}.
     * @return a new {@link ClusteredMediaDriver} with the provided contexts.
     */
    public static ClusteredMediaDriver launch(
        final MediaDriver.Context driverCtx,
        final Archive.Context archiveCtx,
        final ConsensusModule.Context consensusModuleCtx)
    {
        MediaDriver driver = null;
        Archive archive = null;
        ConsensusModule consensusModule = null;

        try
        {
            shareAffinityRegistry(driverCtx, archiveCtx, consensusModuleCtx);
            driver = MediaDriver.launch(driverCtx);

            final int errorCounterId = SystemCounterDescriptor.ERRORS.id();
            final AtomicCounter errorCounter = null != archiveCtx.errorCounter() ?
                archiveCtx.errorCounter() : new AtomicCounter(driverCtx.countersValuesBuffer(), errorCounterId);
            final ErrorHandler errorHandler = null != archiveCtx.errorHandler() ?
                archiveCtx.errorHandler() : driverCtx.errorHandler();

            archive = Archive.launch(archiveCtx
                .mediaDriverAgentInvoker(driver.sharedAgentInvoker())
                .aeronDirectoryName(driver.aeronDirectoryName())
                .errorHandler(errorHandler)
                .errorCounter(errorCounter));

            consensusModule = ConsensusModule.launch(consensusModuleCtx
                .aeronDirectoryName(driverCtx.aeronDirectoryName()));

            return new ClusteredMediaDriver(driver, archive, consensusModule);
        }
        catch (final Exception ex)
        {
            CloseHelper.quietCloseAll(consensusModule, archive, driver);
            throw ex;
        }
    }

    private static void shareAffinityRegistry(
        final MediaDriver.Context driverCtx,
        final Archive.Context archiveCtx,
        final ConsensusModule.Context consensusModuleCtx)
    {
        if (null != driverCtx.affinityRegistry() ||
            null != archiveCtx.affinityRegistry() ||
            null != consensusModuleCtx.affinityRegistry())
        {
            return;
        }

        final AffinityRegistry registry = AffinityRegistry.newDefault();
        driverCtx.registerThreadAffinities(registry);
        archiveCtx.registerThreadAffinities(registry);
        consensusModuleCtx.registerThreadAffinities(registry);

        final boolean validateTopology = driverCtx.driverCpusetAffinity() ||
            archiveCtx.archiveCpusetAffinity() ||
            consensusModuleCtx.clusterCpusetAffinity();
        final boolean warningsAsErrors =
            (driverCtx.driverCpusetAffinity() && driverCtx.driverCpusetWarningsAsErrors()) ||
            (archiveCtx.archiveCpusetAffinity() && archiveCtx.archiveCpusetWarningsAsErrors()) ||
            (consensusModuleCtx.clusterCpusetAffinity() && consensusModuleCtx.clusterCpusetWarningsAsErrors());
        registry.conclude(SystemUtil.isLinux() && validateTopology, warningsAsErrors);

        driverCtx.affinityRegistry(registry);
        archiveCtx.affinityRegistry(registry);
        consensusModuleCtx.affinityRegistry(registry);
    }

    /**
     * Get the {@link MediaDriver} used in the aggregate.
     *
     * @return the {@link MediaDriver} used in the aggregate.
     */
    public MediaDriver mediaDriver()
    {
        return driver;
    }

    /**
     * Get the {@link Archive} used in the aggregate.
     *
     * @return the {@link Archive} used in the aggregate.
     */
    public Archive archive()
    {
        return archive;
    }

    /**
     * Get the {@link ConsensusModule} used in the aggregate.
     *
     * @return the {@link ConsensusModule} used in the aggregate.
     */
    public ConsensusModule consensusModule()
    {
        return consensusModule;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        CloseHelper.closeAll(consensusModule, archive, driver);
    }
}
