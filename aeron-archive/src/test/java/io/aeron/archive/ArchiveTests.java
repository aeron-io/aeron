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

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.ControlResponseAdapter;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.test.Tests;
import org.agrona.IoUtil;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableBoolean;
import org.agrona.collections.MutableLong;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

class ArchiveTests
{
    private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    static File makeTestDirectory()
    {
        final File archiveDir = new File(SystemUtil.tmpDirName(), "archive-test");
        if (archiveDir.exists())
        {
            System.err.println("warning - archive directory exists, deleting: " + archiveDir.getAbsolutePath());
            IoUtil.delete(archiveDir, false);
        }

        if (!archiveDir.mkdirs())
        {
            throw new IllegalStateException("failed to make archive test directory: " + archiveDir.getAbsolutePath());
        }

        return archiveDir;
    }

    static void awaitConnectResponse(
        final Subscription controlResponse, final long expectedCorrelationId, final LongConsumer receiveSessionId)
    {
        final MutableBoolean hasResponse = new MutableBoolean();
        final ControlResponseAdapter controlResponseAdapter = new ControlResponseAdapter(
            new FailControlResponseListener()
            {
                @Override
                public void onResponse(
                    final long controlSessionId,
                    final long correlationId,
                    final long relevantId,
                    final ControlResponseCode code,
                    final String errorMessage)
                {
                    if (correlationId == expectedCorrelationId)
                    {
                        if (ControlResponseCode.OK != code)
                        {
                            throw new IllegalStateException("expected=" + ControlResponseCode.OK + " actual=" + code);
                        }

                        receiveSessionId.accept(controlSessionId);
                        hasResponse.set(true);
                    }
                }
            },
            controlResponse,
            1
        );

        Tests.executeUntil(
            hasResponse::get,
            (j) ->
            {
                if (0 == controlResponseAdapter.poll())
                {
                    Thread.yield();
                }
            },
            Integer.MAX_VALUE,
            TIMEOUT_NS);
    }

    static long awaitOk(final Subscription controlResponse, final long expectedCorrelationId)
    {
        final MutableBoolean hasResponse = new MutableBoolean();
        final MutableLong relevantIdCapture = new MutableLong(Aeron.NULL_VALUE);
        final ControlResponseAdapter controlResponseAdapter = new ControlResponseAdapter(
            new FailControlResponseListener()
            {
                @Override
                public void onResponse(
                    final long controlSessionId,
                    final long correlationId,
                    final long relevantId,
                    final ControlResponseCode code,
                    final String errorMessage)
                {
                    if (correlationId == expectedCorrelationId)
                    {
                        if (ControlResponseCode.OK != code)
                        {
                            System.out.println(errorMessage);
                            throw new IllegalStateException("expected=" + ControlResponseCode.OK + " actual=" + code);
                        }

                        relevantIdCapture.set(relevantId);
                        hasResponse.set(true);
                    }
                }
            },
            controlResponse,
            1
        );

        Tests.executeUntil(
            hasResponse::get,
            (j) ->
            {
                if (0 == controlResponseAdapter.poll())
                {
                    Thread.yield();
                }
            },
            Integer.MAX_VALUE,
            TIMEOUT_NS);

        return relevantIdCapture.get();
    }

    static long awaitResponse(final Subscription controlResponse, final long expectedCorrelationId)
    {
        final MutableBoolean hasResponse = new MutableBoolean();
        final MutableLong relevantIdCapture = new MutableLong(Aeron.NULL_VALUE);
        final ControlResponseAdapter controlResponseAdapter = new ControlResponseAdapter(
            new FailControlResponseListener()
            {
                @Override
                public void onResponse(
                    final long controlSessionId,
                    final long correlationId,
                    final long relevantId,
                    final ControlResponseCode code,
                    final String errorMessage)
                {
                    if (correlationId == expectedCorrelationId)
                    {
                        relevantIdCapture.set(relevantId);
                        hasResponse.set(true);
                    }
                }
            },
            controlResponse,
            1
        );

        Tests.executeUntil(
            hasResponse::get,
            (j) ->
            {
                if (0 == controlResponseAdapter.poll())
                {
                    Thread.yield();
                }
            },
            Integer.MAX_VALUE,
            TIMEOUT_NS);

        return relevantIdCapture.get();
    }
}
