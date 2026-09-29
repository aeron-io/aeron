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
package io.aeron.archive;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.status.SystemCounterDescriptor;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.status.AtomicCounter;

import static org.agrona.SystemUtil.loadPropertiesFiles;

/**
 * Archiving media driver which is an aggregate of a {@link MediaDriver} and an {@link Archive}.
 */
public class ArchivingMediaDriver implements AutoCloseable
{
    private final MediaDriver driver;
    private final Archive archive;

    ArchivingMediaDriver(final MediaDriver driver, final Archive archive)
    {
        this.driver = driver;
        this.archive = archive;
    }

    /**
     * Launch an {@link Archive} with an embedded {@link MediaDriver} and await a shutdown signal.
     *
     * @param args command line argument which is a list for properties files as URLs or filenames.
     */
    @SuppressWarnings("try")
    public static void main(final String[] args)
    {
        loadPropertiesFiles(args);

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
            ArchivingMediaDriver ignore = launch(
                new MediaDriver.Context().terminationHook(barrier::signalAll),
                new Archive.Context()))
        {
            barrier.await();
            System.out.println("Shutdown Archive...");
        }
    }

    /**
     * Launch a new {@link ArchivingMediaDriver} with defaults for {@link io.aeron.driver.MediaDriver.Context} and
     * {@link io.aeron.archive.Archive.Context}.
     *
     * @return a new {@link ArchivingMediaDriver} with default contexts.
     */
    public static ArchivingMediaDriver launch()
    {
        return launch(new MediaDriver.Context(), new Archive.Context());
    }

    /**
     * Launch a new {@link ArchivingMediaDriver} with provided contexts.
     *
     * @param driverCtx  for configuring the {@link MediaDriver}.
     * @param archiveCtx for configuring the {@link Archive}.
     * @return a new {@link ArchivingMediaDriver} with the provided contexts.
     */
    public static ArchivingMediaDriver launch(final MediaDriver.Context driverCtx, final Archive.Context archiveCtx)
    {
        MediaDriver driver = null;
        Archive archive = null;
        try
        {
            driver = MediaDriver.launch(driverCtx);

            final int errorCounterId = SystemCounterDescriptor.ERRORS.id();
            final AtomicCounter errorCounter = null != archiveCtx.errorCounter() ?
                archiveCtx.errorCounter() : new AtomicCounter(driverCtx.countersValuesBuffer(), errorCounterId);
            final ErrorHandler errorHandler = null != archiveCtx.errorHandler() ?
                archiveCtx.errorHandler() : driverCtx.errorHandler();

            archive = Archive.launch(archiveCtx
                .mediaDriverAgentInvoker(driver.sharedAgentInvoker())
                .aeronDirectoryName(driverCtx.aeronDirectoryName())
                .errorHandler(errorHandler)
                .errorCounter(errorCounter));

            return new ArchivingMediaDriver(driver, archive);
        }
        catch (final Exception ex)
        {
            CloseHelper.quietCloseAll(archive, driver);
            throw ex;
        }
    }

    /**
     * Get the launched {@link Archive}.
     *
     * @return the launched {@link Archive}.
     */
    public Archive archive()
    {
        return archive;
    }

    /**
     * Get the launched {@link MediaDriver}.
     *
     * @return the launched {@link MediaDriver}.
     */
    public MediaDriver mediaDriver()
    {
        return driver;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        CloseHelper.closeAll(archive, driver);
    }
}
