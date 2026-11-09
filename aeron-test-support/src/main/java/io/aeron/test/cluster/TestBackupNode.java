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
package io.aeron.test.cluster;

import io.aeron.Counter;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterTool;
import io.aeron.cluster.RecordingLog;
import io.aeron.driver.MediaDriver;
import io.aeron.test.DataCollector;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.EpochClock;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;

public class TestBackupNode implements AutoCloseable
{
    private final TestMediaDriver mediaDriver;
    private final Archive archive;
    private final ClusterBackup clusterBackup;
    private final int index;
    private final Context context;
    private boolean isClosed = false;

    TestBackupNode(final int index, final Context context, final DataCollector dataCollector)
    {
        this.index = index;
        this.context = context;
        try
        {
            mediaDriver = TestMediaDriver.launch(
                context.mediaDriverContext, TestCluster.clientDriverOutputConsumer(dataCollector));

            final String aeronDirectoryName = mediaDriver.context().aeronDirectoryName();
            archive = Archive.launch(context.archiveContext.aeronDirectoryName(aeronDirectoryName));

            clusterBackup = ClusterBackup.launch(context.clusterBackupContext.aeronDirectoryName(aeronDirectoryName));

            dataCollector.add(clusterBackup.context().clusterDir().toPath());
            dataCollector.add(archive.context().archiveDir().toPath());
            dataCollector.add(mediaDriver.context().aeronDirectory().toPath());
        }
        catch (final RuntimeException ex)
        {
            try
            {
                close();
            }
            catch (final Exception ex2)
            {
                ex.addSuppressed(ex2);
            }
            throw ex;
        }
    }

    @Override
    public void close()
    {
        if (!isClosed)
        {
            isClosed = true;
            CloseHelper.closeAll(clusterBackup, archive, mediaDriver);
        }
    }

    boolean isClosed()
    {
        return isClosed;
    }

    ClusterBackup.State backupState()
    {
        return ClusterBackup.State.get(context.clusterBackupContext.stateCounter());
    }

    long liveLogPosition()
    {
        final Counter counter = context.clusterBackupContext.liveLogPositionCounter();
        if (counter.isClosed())
        {
            return NULL_POSITION;
        }

        return counter.get();
    }

    public long snapshotRetrieveCount()
    {
        return context.clusterBackupContext.snapshotRetrieveCounter().get();
    }

    public EpochClock epochClock()
    {
        return context.clusterBackupContext.epochClock();
    }

    public long nextBackupQueryDeadlineMs()
    {
        return ClusterTool.nextBackupQueryDeadlineMs(context.clusterBackupContext.clusterDir());
    }

    public boolean nextBackupQueryDeadlineMs(final long delayMs)
    {
        final long nowMs = epochClock().time();

        return ClusterTool.nextBackupQueryDeadlineMs(context.clusterBackupContext.clusterDir(), nowMs + delayMs);
    }

    public AtomicBuffer clusterBackupErrorLog()
    {
        return clusterBackup.context().clusterMarkFile().errorBuffer();
    }

    long clusterBackupErrorCount()
    {
        return clusterBackup.context().errorCounter().get();
    }

    int index()
    {
        return index;
    }

    TestMediaDriver mediaDriver()
    {
        return mediaDriver;
    }

    public long recordingLogStartPosition()
    {
        try (RecordingLog recordingLog = new RecordingLog(context.clusterBackupContext.clusterDir(), false))
        {
            final long recordingId = Objects.requireNonNull(recordingLog.findLastTerm()).recordingId;

            final AeronArchive.Context backupArchiveContext = context.clusterBackupContext.archiveContext();
            try (AeronArchive aeronArchive = AeronArchive.connect(new AeronArchive.Context()
                .aeronDirectoryName(backupArchiveContext.aeronDirectoryName())
                .controlRequestChannel(backupArchiveContext.controlRequestChannel())
                .controlRequestStreamId(backupArchiveContext.controlRequestStreamId())
                .controlResponseChannel(backupArchiveContext.controlResponseChannel())
                .controlResponseStreamId(ThreadLocalRandom.current().nextInt())))
            {
                return aeronArchive.getStartPosition(recordingId);
            }
        }
    }

    static class Context
    {
        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context();
        final Archive.Context archiveContext = new Archive.Context();
        final ClusterBackup.Context clusterBackupContext = new ClusterBackup.Context();
    }
}
