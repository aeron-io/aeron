/*
 * Copyright 2014-2026 Real Logic Limited.
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
package io.aeron.logging;

import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.SystemNanoClock;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static io.aeron.logging.PrintLoggerEventCallback.NEW_LINE;
import static io.aeron.logging.PrintLoggerEventCallback.endsWithNewLine;
import static java.nio.channels.FileChannel.open;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZonedDateTime.ofInstant;
import static java.time.ZoneId.systemDefault;
import static java.time.format.DateTimeFormatter.ofPattern;

final class RollingFileEventWriter implements LoggerEventWriter
{
    static final DateTimeFormatter DATE_TIME_FORMATTER = ofPattern("uuuu-MM-dd HH:mm:ss.SSSZ");
    private final StringBuilder tsBuilder = new StringBuilder();
    private final String filename;
    private final Path logFilePath;
    private final long maxFileLength;
    private final NanoClock nanoClock;
    private final EpochClock epochClock;
    private int nextFileIndex = 1;
    private FileChannel fileChannel;

    RollingFileEventWriter(final String filename, final long maxFileLength)
    {
        this(filename, maxFileLength, SystemNanoClock.INSTANCE, SystemEpochClock.INSTANCE);
    }

    RollingFileEventWriter(
        final String filename,
        final long maxFileLength,
        final NanoClock nanoClock,
        final EpochClock epochClock)
    {
        this.filename = filename;
        this.logFilePath = Path.of(filename);
        this.maxFileLength = maxFileLength;
        this.nanoClock = nanoClock;
        this.epochClock = epochClock;
        try
        {
            this.fileChannel = open(this.logFilePath, CREATE, WRITE, APPEND);
            this.writeLogFileStartMessage(nanoClock.nanoTime(), epochClock.time(), systemDefault());
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    void writeLogFileStartMessage(final long timestampNs, final long timestampMs, final ZoneId zone) throws IOException
    {
        tsBuilder.setLength(0);
        LogUtil.appendTimestamp(tsBuilder, timestampNs);
        tsBuilder.append("log started ")
            .append(DATE_TIME_FORMATTER.format(ofInstant(ofEpochMilli(timestampMs), zone)))
            .append(NEW_LINE);
        fileChannel.write(ByteBuffer.wrap(tsBuilder.toString().getBytes()));
    }

    public void write(final StringBuilder sb) throws IOException
    {
        if (!endsWithNewLine(sb))
        {
            sb.append(NEW_LINE);
        }
        fileChannel.write(ByteBuffer.wrap(sb.toString().getBytes()));
        if (fileChannel.position() >= maxFileLength)
        {
            fileChannel.close();
            Path rolledFilePath;
            do
            {
                rolledFilePath = Path.of(filename + "." + nextFileIndex);
                nextFileIndex++;
            }
            while (Files.exists(rolledFilePath));
            // move the current log file to rolled file
            Files.move(logFilePath, rolledFilePath);
            // Re-open the log file (that was previously moved)
            fileChannel = open(logFilePath, CREATE_NEW, WRITE, APPEND);
            writeLogFileStartMessage(nanoClock.nanoTime(), epochClock.time(), systemDefault());
        }
    }
}
