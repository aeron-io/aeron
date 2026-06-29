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

import io.aeron.Aeron;
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
    @SuppressWarnings("JavadocVariable")
    private enum State
    {
        REBUILD_LOG_ADAPTER_INIT, REBUILD_LOG_ADAPTER, REPLAY_INIT, REPLAY
    }

    private final AeronArchive archive;
    private final long recordingId;
    private final long logAdapterRebuildPosition;
    private final long startPosition;
    private final long stopPosition;
    private final ConsensusModuleAgent consensusModuleAgent;
    private final ConsensusModule.Context ctx;
    private final LogAdapter logAdapter;

    private State state;
    private long replaySessionId;
    private int logSessionId;
    private Subscription logSubscription;

    LogReplay(
        final AeronArchive archive,
        final long recordingId,
        final long logAdapterRebuildPosition,
        final long startPosition,
        final long stopPosition,
        final LogAdapter logAdapter,
        final ConsensusModule.Context ctx)
    {
        this.archive = archive;
        this.recordingId = recordingId;
        this.logAdapterRebuildPosition = logAdapterRebuildPosition;
        this.startPosition = startPosition;
        this.stopPosition = stopPosition;

        this.consensusModuleAgent = logAdapter.consensusModuleAgent();
        this.ctx = ctx;
        this.logAdapter = logAdapter;

        state(Aeron.NULL_VALUE != logAdapterRebuildPosition ? State.REBUILD_LOG_ADAPTER_INIT : State.REPLAY_INIT);
    }

    void close()
    {
        final CountedErrorHandler errorHandler = ctx.countedErrorHandler();
        try
        {
            if (Aeron.NULL_VALUE != replaySessionId)
            {
                archive.stopReplay(replaySessionId);
            }
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
        return switch (state)
        {
            case REBUILD_LOG_ADAPTER_INIT -> rebuildLogAdapterInit();
            case REBUILD_LOG_ADAPTER -> rebuildLogAdapter();
            case REPLAY_INIT -> replayInit();
            case REPLAY -> replay();
        };
    }

    private int rebuildLogAdapterInit()
    {
        final String channel = ctx.replayChannel();
        final int streamId = ctx.replayStreamId();
        final long length = startPosition - logAdapterRebuildPosition;
        replaySessionId = archive.startReplay(recordingId, logAdapterRebuildPosition, length, channel, streamId);
        logSessionId = (int)replaySessionId;
        logSubscription = ctx.aeron().addSubscription(ChannelUri.addSessionId(channel, logSessionId), streamId);
        state(State.REBUILD_LOG_ADAPTER);
        return 1;
    }

    private int rebuildLogAdapter()
    {
        int workCount = 0;

        if (null == logAdapter.image())
        {
            final Image image = logSubscription.imageBySessionId(logSessionId);
            if (null != image)
            {
                if (image.joinPosition() != logAdapterRebuildPosition)
                {
                    throw new ClusterException(
                        "joinPosition=" + image.joinPosition() +
                            " expected logAdapterRebuildPosition=" + logAdapterRebuildPosition,
                        ClusterException.Category.WARN);
                }

                logAdapter.image(image);
                workCount += 1;
            }
        }
        else if (startPosition == logAdapter.position() && logAdapter.image().isEndOfStream())
        {
            replaySessionId = Aeron.NULL_VALUE;
            logSessionId = Aeron.NULL_VALUE;
            logAdapter.disconnect(ctx.countedErrorHandler());
            logSubscription = null;
            state(State.REPLAY_INIT);
            workCount += 1;
        }
        else
        {
            workCount += consensusModuleAgent.replayLogPoll(logAdapter, startPosition);
        }

        return workCount;
    }

    private int replayInit()
    {
        final String channel = ctx.replayChannel();
        final int streamId = ctx.replayStreamId();
        final long length = stopPosition - startPosition;
        replaySessionId = archive.startReplay(recordingId, startPosition, length, channel, streamId);
        logSessionId = (int)replaySessionId;
        logSubscription = ctx.aeron().addSubscription(ChannelUri.addSessionId(channel, logSessionId), streamId);
        state(State.REPLAY);
        return 1;
    }

    private int replay()
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

    private void state(final State state)
    {
        this.state = state;
    }

    boolean isDone()
    {
        return state == State.REPLAY &&
            logAdapter.position() >= stopPosition &&
            consensusModuleAgent.state() != ConsensusModule.State.SNAPSHOT;
    }

    long position()
    {
        return logAdapter.position();
    }

    public String toString()
    {
        return "LogReplay{" +
            ", recordingId=" + recordingId +
            ", logAdapterRebuildPosition=" + logAdapterRebuildPosition +
            ", startPosition=" + startPosition +
            ", stopPosition=" + stopPosition +
            ", state=" + state +
            ", replaySessionId=" + replaySessionId +
            ", logSessionId=" + logSessionId +
            ", logSubscription=" + logSubscription +
            ", position=" + position() +
            '}';
    }
}
