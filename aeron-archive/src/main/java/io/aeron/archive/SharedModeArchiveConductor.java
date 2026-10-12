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

import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.CountedErrorHandler;

import static io.aeron.CommonContext.threadName;
import static io.aeron.archive.Archive.AERON_ARCHIVE_SHARED_THREAD_NAME;
import static io.aeron.archive.Archive.AERON_ARCHIVE_SHARED_THREAD_NAME_CLASSIC;

final class SharedModeArchiveConductor extends ArchiveConductor
{
    private AgentInvoker replayerAgentInvoker;
    private AgentInvoker recorderAgentInvoker;

    SharedModeArchiveConductor(final Archive.Context ctx)
    {
        super(ctx, threadName(
            ctx.properties(), AERON_ARCHIVE_SHARED_THREAD_NAME, AERON_ARCHIVE_SHARED_THREAD_NAME_CLASSIC));
    }

    public void onStart()
    {
        super.onStart();

        replayerAgentInvoker = new AgentInvoker(errorHandler, ctx.errorCounter(), replayer);
        recorderAgentInvoker = new AgentInvoker(errorHandler, ctx.errorCounter(), recorder);

        replayerAgentInvoker.start();
        recorderAgentInvoker.start();
    }

    Recorder newRecorder()
    {
        return new SharedModeRecorder(errorHandler, ctx);
    }

    Replayer newReplayer()
    {
        return new SharedModeReplayer(errorHandler, ctx);
    }

    public int doWork()
    {
        return super.doWork() +
            replayerAgentInvoker.invoke() +
            invokeAeronInvoker() +
            invokeDriverConductor() +
            recorderAgentInvoker.invoke() +
            invokeAeronInvoker() +
            invokeDriverConductor();
    }

    protected void closeSessionWorkers()
    {
        CloseHelper.close(ctx.countedErrorHandler(), recorderAgentInvoker);
        CloseHelper.close(ctx.countedErrorHandler(), replayerAgentInvoker);
    }

    class SharedModeRecorder extends Recorder
    {
        SharedModeRecorder(final CountedErrorHandler errorHandler, final Archive.Context context)
        {
            super(errorHandler, context);
        }

        protected void closeSession(final RecordingSession session)
        {
            closeRecordingSession(session);
        }
    }

    class SharedModeReplayer extends Replayer
    {
        SharedModeReplayer(final CountedErrorHandler errorHandler, final Archive.Context context)
        {
            super(errorHandler, context);
        }

        protected void closeSession(final ReplaySession session)
        {
            closeReplaySession(session);
        }
    }
}
