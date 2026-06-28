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
package io.aeron.driver.reports;

import org.agrona.BitUtil;
import org.agrona.concurrent.AtomicBuffer;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import static io.aeron.driver.reports.LossReport.*;
import static org.agrona.BitUtil.SIZE_OF_INT;

/**
 * Reader that provides the function to read entries from a {@link LossReport}.
 */
public final class LossReportReader
{
    /**
     * CSV style header for using with {@link #defaultEntryConsumer(PrintStream)}.
     */
    public static final String LOSS_REPORT_CSV_HEADER =
        "#OBSERVATION_COUNT,TOTAL_BYTES_LOST,FIRST_OBSERVATION,LAST_OBSERVATION,SESSION_ID,STREAM_ID,CHANNEL,SOURCE";

    /**
     * Consumer function to be implemented by caller of the read method.
     */
    @FunctionalInterface
    public interface EntryConsumer
    {
        /**
         * Accept an entry from the loss report, so it can be consumed.
         *
         * @param observationCount          for the stream instance.
         * @param totalBytesLost            for the stream instance.
         * @param firstObservationTimestamp on the stream instance.
         * @param lastObservationTimestamp  on the stream instance.
         * @param sessionId                 identifying the stream.
         * @param streamId                  identifying the stream.
         * @param channel                   to which the stream belongs.
         * @param source                    of the stream.
         */
        void accept(
            long observationCount,
            long totalBytesLost,
            long firstObservationTimestamp,
            long lastObservationTimestamp,
            int sessionId,
            int streamId,
            String channel,
            String source);
    }

    private LossReportReader()
    {
    }

    /**
     * Create a default {@link EntryConsumer} which outputs to a provided {@link PrintStream}.
     *
     * @param out to write entries to.
     * @return a new {@link EntryConsumer} which outputs to a provided {@link PrintStream}.
     */
    public static EntryConsumer defaultEntryConsumer(final PrintStream out)
    {
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

        return
            (observationCount,
            totalBytesLost,
            firstObservationTimestamp,
            lastObservationTimestamp,
            sessionId,
            streamId,
            channel,
            source) ->
            {
                out.format(
                    "%d,%d,%s,%s,%d,%d,%s,%s%n",
                    observationCount,
                    totalBytesLost,
                    dateFormat.format(new Date(firstObservationTimestamp)),
                    dateFormat.format(new Date(lastObservationTimestamp)),
                    sessionId,
                    streamId,
                    channel,
                    source);
            };
    }

    /**
     * Read a {@link LossReport} contained in the buffer. This can be done concurrently.
     *
     * @param buffer        containing the loss report.
     * @param entryConsumer to be called to accept each entry in the report.
     * @return the number of entries read.
     */
    public static int read(final AtomicBuffer buffer, final EntryConsumer entryConsumer)
    {
        final int capacity = buffer.capacity();

        int recordsRead = 0;
        int offset = 0;

        while (offset < capacity)
        {
            final long observationCount = buffer.getLongVolatile(offset + OBSERVATION_COUNT_OFFSET);
            if (observationCount <= 0)
            {
                break;
            }

            ++recordsRead;

            final String channel = buffer.getStringAscii(offset + CHANNEL_OFFSET);
            final String source = buffer.getStringAscii(
                offset + CHANNEL_OFFSET + BitUtil.align(SIZE_OF_INT + channel.length(), SIZE_OF_INT));

            entryConsumer.accept(
                observationCount,
                buffer.getLongVolatile(offset + TOTAL_BYTES_LOST_OFFSET),
                buffer.getLong(offset + FIRST_OBSERVATION_OFFSET),
                buffer.getLongVolatile(offset + LAST_OBSERVATION_OFFSET),
                buffer.getInt(offset + SESSION_ID_OFFSET),
                buffer.getInt(offset + STREAM_ID_OFFSET),
                channel,
                source);

            final int recordLength =
                CHANNEL_OFFSET +
                BitUtil.align(SIZE_OF_INT + channel.length(), SIZE_OF_INT) +
                SIZE_OF_INT + source.length();
            offset += BitUtil.align(recordLength, ENTRY_ALIGNMENT);
        }

        return recordsRead;
    }
}
