/*
 * Copyright 2014-2025 Real Logic Limited.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.aeron.archive;

import io.aeron.driver.DutyCycleTracker;
import org.agrona.CloseHelper;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.AtomicCounter;

import java.util.concurrent.CountDownLatch;

import static io.aeron.CommonContext.threadName;
import static io.aeron.archive.Archive.AERON_ARCHIVE_CONDUCTOR_THREAD_NAME;
import static io.aeron.archive.Archive.AERON_ARCHIVE_CONDUCTOR_THREAD_NAME_CLASSIC;

final class DedicatedModeArchiveConductor extends ArchiveConductor
{
    private static final int COMMAND_LIMIT = 10;

    private final ManyToOneConcurrentLinkedQueue<Session> closeQueue;
    private AgentRunner recorderAgentRunner;
    private AgentRunner replayerAgentRunner;

    DedicatedModeArchiveConductor(final Archive.Context ctx)
    {
        super(ctx, threadName(AERON_ARCHIVE_CONDUCTOR_THREAD_NAME, AERON_ARCHIVE_CONDUCTOR_THREAD_NAME_CLASSIC));
        closeQueue = new ManyToOneConcurrentLinkedQueue<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onStart()
    {
        super.onStart();

        recorderAgentRunner = new AgentRunner(ctx.recorderIdleStrategy(), errorHandler, ctx.errorCounter(), recorder);
        replayerAgentRunner = new AgentRunner(ctx.replayerIdleStrategy(), errorHandler, ctx.errorCounter(), replayer);

        AgentRunner.startOnThread(recorderAgentRunner, ctx.recorderThreadFactory());
        AgentRunner.startOnThread(replayerAgentRunner, ctx.replayerThreadFactory());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int doWork()
    {
        final int workCount = processCloseQueue();
        return workCount + super.doWork();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void closeSessionWorkers()
    {
        CloseHelper.close(errorHandler, recorderAgentRunner);
        CloseHelper.close(errorHandler, replayerAgentRunner);

        while (processCloseQueue() > 0 || !closeQueue.isEmpty())
        {
            Thread.yield();
            if (Thread.currentThread().isInterrupted())
            {
                break;
            }
        }
    }

    Recorder newRecorder()
    {
        return new DedicatedModeRecorder(
            errorHandler,
            ctx.errorCounter(),
            closeQueue,
            ctx.abortLatch(),
            ctx.recorderDutyCycleTracker(),
            ctx);
    }

    Replayer newReplayer()
    {
        return new DedicatedModeReplayer(
            errorHandler,
            ctx.errorCounter(),
            closeQueue,
            ctx.abortLatch(),
            ctx.replayerDutyCycleTracker(),
            ctx);
    }

    private int processCloseQueue()
    {
        int i;
        Session session;
        for (i = 0; i < COMMAND_LIMIT && (session = closeQueue.poll()) != null; i++)
        {
            if (session instanceof RecordingSession)
            {
                closeRecordingSession((RecordingSession)session);
            }
            else if (session instanceof ReplaySession)
            {
                closeReplaySession((ReplaySession)session);
            }
            else
            {
                closeSession(session);
            }
        }

        return i;
    }

    static class DedicatedModeRecorder extends Recorder
    {
        private final ManyToOneConcurrentLinkedQueue<RecordingSession> sessionsQueue;
        private final ManyToOneConcurrentLinkedQueue<Session> closeQueue;
        private final AtomicCounter errorCounter;
        private final CountDownLatch abortLatch;
        private final DutyCycleTracker dutyCycleTracker;
        private final NanoClock nanoClock;
        private volatile boolean isAbort;

        DedicatedModeRecorder(
            final CountedErrorHandler errorHandler,
            final AtomicCounter errorCounter,
            final ManyToOneConcurrentLinkedQueue<Session> closeQueue,
            final CountDownLatch abortLatch,
            final DutyCycleTracker dutyCycleTracker,
            final Archive.Context context)
        {
            super(errorHandler, context);

            this.closeQueue = closeQueue;
            this.errorCounter = errorCounter;
            this.sessionsQueue = new ManyToOneConcurrentLinkedQueue<>();
            this.abortLatch = abortLatch;
            this.dutyCycleTracker = dutyCycleTracker;
            this.nanoClock = context.nanoClock();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void abort()
        {
            isAbort = true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onStart()
        {
            super.onStart();

            dutyCycleTracker.update(nanoClock.nanoTime());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int doWork()
        {
            if (isAbort)
            {
                throw new AgentTerminationException();
            }

            dutyCycleTracker.measureAndUpdate(nanoClock.nanoTime());

            return drainSessionsQueue() + super.doWork();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void preSessionsClose()
        {
            drainSessionsQueue();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void addSession(final RecordingSession session)
        {
            send(session);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void closeSession(final RecordingSession session)
        {
            while (!closeQueue.offer(session))
            {
                if (!errorCounter.isClosed())
                {
                    errorCounter.increment();
                }

                Thread.yield();
                if (Thread.currentThread().isInterrupted())
                {
                    break;
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void postSessionsClose()
        {
            if (isAbort)
            {
                abortLatch.countDown();
            }
        }

        private int drainSessionsQueue()
        {
            int workCount = 0;
            RecordingSession session;

            while (null != (session = sessionsQueue.poll()))
            {
                workCount += 1;
                super.addSession(session);
            }

            return workCount;
        }

        private void send(final RecordingSession session)
        {
            while (!sessionsQueue.offer(session))
            {
                if (!errorCounter.isClosed())
                {
                    errorCounter.increment();
                }

                Thread.yield();
                if (Thread.currentThread().isInterrupted())
                {
                    break;
                }
            }
        }
    }

    static class DedicatedModeReplayer extends Replayer
    {
        private final ManyToOneConcurrentLinkedQueue<ReplaySession> sessionsQueue;
        private final ManyToOneConcurrentLinkedQueue<Session> closeQueue;
        private final AtomicCounter errorCounter;
        private final CountDownLatch abortLatch;
        private final DutyCycleTracker dutyCycleTracker;
        private final NanoClock nanoClock;
        private volatile boolean isAbort;

        DedicatedModeReplayer(
            final CountedErrorHandler errorHandler,
            final AtomicCounter errorCounter,
            final ManyToOneConcurrentLinkedQueue<Session> closeQueue,
            final CountDownLatch abortLatch,
            final DutyCycleTracker dutyCycleTracker,
            final Archive.Context context)
        {
            super(errorHandler, context);

            this.closeQueue = closeQueue;
            this.errorCounter = errorCounter;
            this.sessionsQueue = new ManyToOneConcurrentLinkedQueue<>();
            this.abortLatch = abortLatch;
            this.dutyCycleTracker = dutyCycleTracker;
            this.nanoClock = context.nanoClock();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void abort()
        {
            isAbort = true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void addSession(final ReplaySession session)
        {
            send(session);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onStart()
        {
            super.onStart();
            dutyCycleTracker.update(nanoClock.nanoTime());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int doWork()
        {
            if (isAbort)
            {
                throw new AgentTerminationException();
            }

            dutyCycleTracker.measureAndUpdate(nanoClock.nanoTime());

            return drainSessionQueue() + super.doWork();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void preSessionsClose()
        {
            drainSessionQueue();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void closeSession(final ReplaySession session)
        {
            while (!closeQueue.offer(session))
            {
                if (!errorCounter.isClosed())
                {
                    errorCounter.increment();
                }

                Thread.yield();
                if (Thread.currentThread().isInterrupted())
                {
                    break;
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void postSessionsClose()
        {
            if (isAbort)
            {
                abortLatch.countDown();
            }
        }

        private int drainSessionQueue()
        {
            int workCount = 0;
            ReplaySession session;

            while (null != (session = sessionsQueue.poll()))
            {
                workCount += 1;
                super.addSession(session);
            }

            return workCount;
        }

        private void send(final ReplaySession session)
        {
            while (!sessionsQueue.offer(session))
            {
                if (!errorCounter.isClosed())
                {
                    errorCounter.increment();
                }

                Thread.yield();
                if (Thread.currentThread().isInterrupted())
                {
                    break;
                }
            }
        }
    }
}
