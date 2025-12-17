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

import io.aeron.ChannelUri;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.service.Cluster;
import org.agrona.CloseHelper;
import org.agrona.concurrent.CountedErrorHandler;

final class LogReplay
{
    private final AeronArchive archive;
    private final long startPosition;
    private final long stopPosition;
    private final long replaySessionId;
    private final int logSessionId;
    private final ConsensusModuleAgent consensusModuleAgent;
    private final ConsensusModule.Context ctx;
    private final LogAdapter logAdapter;
    private final Subscription logSubscription;

    LogReplay(
        final AeronArchive archive,
        final long recordingId,
        final long startPosition,
        final long stopPosition,
        final LogAdapter logAdapter,
        final ConsensusModule.Context ctx)
    {
        this.archive = archive;
        this.startPosition = startPosition;
        this.stopPosition = stopPosition;
        this.logAdapter = logAdapter;
        this.consensusModuleAgent = logAdapter.consensusModuleAgent();
        this.ctx = ctx;

        final String channel = ctx.replayChannel();
        final int streamId = ctx.replayStreamId();
        final long length = stopPosition - startPosition;
        replaySessionId = archive.startReplay(recordingId, startPosition, length, channel, streamId);
        logSessionId = (int)replaySessionId;
        logSubscription = ctx.aeron().addSubscription(ChannelUri.addSessionId(channel, logSessionId), streamId);
    }

    void close()
    {
        final CountedErrorHandler errorHandler = ctx.countedErrorHandler();
        try
        {
            archive.stopReplay(replaySessionId);
        }
        catch (final RuntimeException ex)
        {
            errorHandler.onError(ex);
        }
        logAdapter.disconnect(errorHandler);
        CloseHelper.close(errorHandler, logSubscription);
    }

    int doWork()
    {
        int workCount = 0;

        if (null == logAdapter.image())
        {
            final Image image = logSubscription.imageBySessionId(logSessionId);
            if (null != image)
            {
                if (image.joinPosition() != startPosition)
                {
                    throw new ClusterException(
                        "joinPosition=" + image.joinPosition() + " expected startPosition=" + startPosition,
                        ClusterException.Category.WARN);
                }

                logAdapter.image(image);
                consensusModuleAgent.awaitServicesReady(
                    logSubscription.channel(),
                    logSubscription.streamId(),
                    logSessionId,
                    startPosition,
                    stopPosition,
                    true,
                    Cluster.Role.FOLLOWER);

                workCount += 1;
            }
        }
        else
        {
            workCount += consensusModuleAgent.replayLogPoll(logAdapter, stopPosition);
        }

        return workCount;
    }

    boolean isDone()
    {
        return logAdapter.position() >= stopPosition &&
            consensusModuleAgent.state() != ConsensusModule.State.SNAPSHOT;
    }

    long position()
    {
        return logAdapter.position();
    }

    public String toString()
    {
        return "LogReplay{" +
            "startPosition=" + startPosition +
            ", stopPosition=" + stopPosition +
            ", replaySessionId=" + replaySessionId +
            ", logSessionId=" + logSessionId +
            ", logSubscription=" + logSubscription +
            ", position=" + position() +
            '}';
    }
}
