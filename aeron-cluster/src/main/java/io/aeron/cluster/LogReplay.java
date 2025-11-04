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
    @SuppressWarnings("JavadocVariable")
    private enum State
    {
        RECOVER_FRAGMENTS_INIT, RECOVER_FRAGMENTS, REPLAY_INIT, REPLAY
    }

    private final long fragmentsBeforeStartPosition;
    private final long startPosition;
    private final long stopPosition;
    private final AeronArchive archive;
    private final ConsensusModuleAgent consensusModuleAgent;
    private final ConsensusModule.Context ctx;
    private final LogAdapter logAdapter;
    private final long recordingId;

    private State state;
    private Subscription logSubscription;
    private long replaySessionId;
    private int logSessionId;

    LogReplay(
        final AeronArchive archive,
        final long recordingId,
        final long fragmentsBeforeLogPosition,
        final long startPosition,
        final long stopPosition,
        final LogAdapter logAdapter,
        final ConsensusModule.Context ctx)
    {
        this.archive = archive;
        this.recordingId = recordingId;
        this.fragmentsBeforeStartPosition = fragmentsBeforeLogPosition;
        this.startPosition = startPosition;
        this.stopPosition = stopPosition;
        this.logAdapter = logAdapter;
        this.consensusModuleAgent = logAdapter.consensusModuleAgent();
        this.ctx = ctx;

        if (Aeron.NULL_VALUE != fragmentsBeforeLogPosition)
        {
            state = State.RECOVER_FRAGMENTS_INIT;
        }
        else
        {
            state = State.REPLAY_INIT;
        }
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

        switch (state)
        {
            case RECOVER_FRAGMENTS_INIT ->
            {
                final long length = startPosition - fragmentsBeforeStartPosition;
                replaySessionId = archive.startReplay(
                    recordingId, fragmentsBeforeStartPosition, length, ctx.replayChannel(), ctx.replayStreamId());
                logSessionId = (int)replaySessionId;

                final String channel = ChannelUri.addSessionId(ctx.replayChannel(), logSessionId);
                logSubscription = ctx.aeron().addSubscription(channel, ctx.replayStreamId());

                state = State.RECOVER_FRAGMENTS;
                workCount += 1;
            }

            case RECOVER_FRAGMENTS ->
            {
                if (null == logAdapter.image())
                {
                    final Image image = logSubscription.imageBySessionId(logSessionId);
                    if (null != image)
                    {
                        if (image.joinPosition() != fragmentsBeforeStartPosition)
                        {
                            throw new ClusterException(
                                "joinPosition=" + image.joinPosition() +
                                    " expected startPosition=" + fragmentsBeforeStartPosition,
                                ClusterException.Category.WARN);
                        }

                        logAdapter.image(image);
                        workCount += 1;
                    }
                }
                else if (logAdapter.image().isEndOfStream() && logAdapter.position() == startPosition)
                {
                    logAdapter.image(null);
                    CloseHelper.close(ctx.countedErrorHandler(), logSubscription);
                    state = State.REPLAY_INIT;
                    workCount += 1;
                }
                else
                {
                    workCount += consensusModuleAgent.replayLogPoll(logAdapter, startPosition);
                }
            }

            case REPLAY_INIT ->
            {
                final long length = stopPosition - startPosition;
                replaySessionId = archive.startReplay(
                    recordingId, startPosition, length, ctx.replayChannel(), ctx.replayStreamId());
                logSessionId = (int)replaySessionId;

                final String channel = ChannelUri.addSessionId(ctx.replayChannel(), logSessionId);
                logSubscription = ctx.aeron().addSubscription(channel, ctx.replayStreamId());

                state = State.REPLAY;
                workCount += 1;
            }

            case REPLAY ->
            {
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
            }
        }

        return workCount;
    }

    boolean isDone()
    {
        return logAdapter.position() >= stopPosition &&
            consensusModuleAgent.state() != ConsensusModule.State.SNAPSHOT;
    }

    boolean hasProcessedAllFragmentsBeforeLogPosition()
    {
        return State.REPLAY_INIT == state || State.REPLAY == state;
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
