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
package io.aeron;

import io.aeron.driver.reports.LossReportReader;
import io.aeron.driver.reports.LossReportUtil;
import io.aeron.test.Tests;
import org.agrona.BufferUtil;
import org.agrona.concurrent.*;
import org.agrona.concurrent.errors.ErrorConsumer;
import org.agrona.concurrent.errors.ErrorLogReader;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SystemTests
{
    static void verifyLossOccurredForStream(final String aeronDirectoryName, final int streamId)
        throws IOException
    {
        final File lossReportFile = LossReportUtil.file(aeronDirectoryName);
        assertTrue(lossReportFile.exists());

        MappedByteBuffer mappedByteBuffer = null;

        try (RandomAccessFile file = new RandomAccessFile(lossReportFile, "r");
            FileChannel channel = file.getChannel())
        {
            mappedByteBuffer = channel.map(READ_ONLY, 0, channel.size());
            final AtomicBuffer buffer = new UnsafeBuffer(mappedByteBuffer);

            final LossReportReader.EntryConsumer lossEntryConsumer = mock(LossReportReader.EntryConsumer.class);
            LossReportReader.read(buffer, lossEntryConsumer);

            verify(lossEntryConsumer).accept(
                longThat((l) -> l > 0),
                longThat((l) -> l > 0),
                anyLong(),
                anyLong(),
                anyInt(),
                eq(streamId),
                any(),
                any());
        }
        finally
        {
            BufferUtil.free(mappedByteBuffer);
        }
    }

    static void waitForErrorToOccur(
        final String aeronDirectoryName,
        final Matcher<String> matcher,
        final IdleStrategy retryIdle) throws IOException
    {
        final File cncFile = CommonContext.newCncFile(aeronDirectoryName);
        assertTrue(cncFile.exists());

        MappedByteBuffer cncByteBuffer = null;

        try (
            RandomAccessFile file = new RandomAccessFile(cncFile, "r");
            FileChannel channel = file.getChannel())
        {
            cncByteBuffer = channel.map(READ_ONLY, 0, channel.size());
            final AtomicBuffer errorLogBuffer = CommonContext.errorLogBuffer(cncByteBuffer);

            final MatcherErrorConsumer errorConsumer = new MatcherErrorConsumer(matcher);

            ErrorLogReader.read(errorLogBuffer, errorConsumer);
            if (errorConsumer.hasMatched())
            {
                return;
            }

            if (null == retryIdle)
            {
                fail(errorConsumer.toString());
            }

            while (!errorConsumer.hasMatched())
            {
                errorConsumer.reset();
                ErrorLogReader.read(errorLogBuffer, errorConsumer);

                Tests.idle(retryIdle, errorConsumer::toString);
            }
        }
        finally
        {
            BufferUtil.free(cncByteBuffer);
        }
    }

    static final class MatcherErrorConsumer implements ErrorConsumer
    {
        private final Matcher<String> matcher;
        private final List<String> encodedExceptions = new ArrayList<>();
        private boolean hasMatched = false;

        MatcherErrorConsumer(final Matcher<String> matcher)
        {
            this.matcher = matcher;
        }

        @Override
        public void accept(
            final int observationCount,
            final long firstObservationTimestamp,
            final long lastObservationTimestamp,
            final String encodedException)
        {
            hasMatched = matcher.matches(encodedException);
            encodedExceptions.add(encodedException);
        }

        void reset()
        {
            encodedExceptions.clear();
        }

        boolean hasMatched()
        {
            return hasMatched;
        }

        @Override
        public String toString()
        {
            final StringDescription description = new StringDescription();
            final String lineSeparator = System.lineSeparator();

            description.appendText("Unable to match: ");
            matcher.describeTo(description);
            description.appendText(", against the following errors:");
            encodedExceptions.forEach(
                (encodedException) ->
                {
                    description.appendText(lineSeparator).appendText("  ");
                    description.appendText(encodedException);
                });
            description.appendText(lineSeparator);

            return description.toString();
        }
    }
}
