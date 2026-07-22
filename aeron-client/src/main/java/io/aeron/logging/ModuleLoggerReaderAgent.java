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
package io.aeron.logging;

import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static io.aeron.logging.CommonEventDissector.dissectLogStartMessage;
import static io.aeron.logging.EventConfiguration.EVENT_READER_FRAME_LIMIT;
import static io.aeron.logging.EventConfiguration.MAX_EVENT_LENGTH;
import static java.lang.System.lineSeparator;
import static java.nio.channels.FileChannel.open;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.time.ZoneId.systemDefault;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.agrona.SystemUtil.parseSize;

/**
 * Simple reader of the {@link ManyToOneRingBuffer} held by {@link EventConfiguration#eventReader} that appends to
 * {@link System#out} by default or to file if {@link #LOG_FILENAME_PROP_NAME} System property is set.
 */
public final class ModuleLoggerReaderAgent implements Agent
{
    /**
     * Event Buffer length system property name. If not set then output will default to {@link System#out}.
     */
    public static final String LOG_FILENAME_PROP_NAME = "aeron.event.log.filename";
    /**
     * Event Buffer log file max length system property. If not set then {@link Long#MAX_VALUE} will be used.
     */
    static final String LOG_FILE_MAX_LENGTH = "aeron.event.log.file.max.length";

    /**
     * Length of the buffer used for writing rendered messages to the log file.
     */
    public static final int BUFFER_LENGTH = (MAX_EVENT_LENGTH + lineSeparator().length()) * 2;

    private final ManyToOneRingBuffer ringBuffer = EventConfiguration.eventReader().ringBuffer();
    private final StringBuilder builder = new StringBuilder(MAX_EVENT_LENGTH);
    private final MessageHandler messageHandler = this::onMessage;
    private final ByteBuffer byteBuffer;
    private final Int2ObjectHashMap<ModuleLogger> loggers = new Int2ObjectHashMap<>();
    private final PrintStream out;
    private final NanoClock nanoClock;
    private final EpochClock epochClock;
    private final long maxFileLength;
    private final String filename;
    private final Path logFilePath;
    private int nextFileIndex = 1;

    private FileChannel fileChannel;

    ModuleLoggerReaderAgent(final Properties configOptions, final List<ModuleLogger> loggers)
    {
        this(
            configOptions,
            System.out,
            SystemNanoClock.INSTANCE,
            SystemEpochClock.INSTANCE,
            loggers);
    }

    ModuleLoggerReaderAgent(
        final Properties configOptions,
        final PrintStream out,
        final NanoClock nanoClock,
        final EpochClock epochClock,
        final List<ModuleLogger> loggers)
    {
        filename = configOptions.getProperty(LOG_FILENAME_PROP_NAME);
        logFilePath = null != filename ? Path.of(filename) : null;
        maxFileLength = getMaxFileLength(configOptions);

        this.nanoClock = Objects.requireNonNull(nanoClock);
        this.epochClock = Objects.requireNonNull(epochClock);
        for (final ModuleLogger componentLogger : loggers)
        {
            this.loggers.put(componentLogger.typeCode(), componentLogger);
        }

        if (null != logFilePath)
        {
            this.out = null;
            try
            {
                fileChannel = open(logFilePath, CREATE, APPEND, WRITE);
            }
            catch (final IOException ex)
            {
                throw new UncheckedIOException(ex);
            }

            byteBuffer = allocateDirectAligned(BUFFER_LENGTH, CACHE_LINE_LENGTH);
        }
        else
        {
            fileChannel = null;
            byteBuffer = null;
            this.out = Objects.requireNonNull(out);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void onStart()
    {
        appendFileHeader(nanoClock.nanoTime(), epochClock.time());

        if (null == fileChannel)
        {
            out.print(builder);
        }
        else
        {
            appendEvent(builder, byteBuffer);
            write(byteBuffer);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void onClose()
    {
        CloseHelper.close(fileChannel);
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return "event-log-reader";
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        final int eventsRead = ringBuffer.read(messageHandler, EVENT_READER_FRAME_LIMIT);
        if (null != byteBuffer && byteBuffer.position() > 0)
        {
            write(byteBuffer);
        }

        return eventsRead;
    }

    private void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
    {
        final int eventCodeTypeId = msgTypeId >> 16;
        final int eventCodeId = msgTypeId & 0xFFFF;

        builder.setLength(0);

        decodeLogEvent(buffer, index, eventCodeTypeId, eventCodeId, loggers, builder);

        if (null == fileChannel)
        {
            out.print(builder);
        }
        else
        {
            appendEvent(builder, byteBuffer);
        }
    }

    static void decodeLogEvent(
        final MutableDirectBuffer buffer,
        final int index,
        final int eventCodeTypeId,
        final int eventCodeId,
        final Int2ObjectHashMap<ModuleLogger> loggers,
        final StringBuilder builder)
    {
        final ModuleLogger componentLogger = loggers.get(eventCodeTypeId);
        if (null != componentLogger)
        {
            componentLogger.decode(buffer, index, eventCodeId, builder);
        }
        else
        {
            builder.append("Unknown EventCodeType: ").append(eventCodeTypeId);
        }

        builder.append(lineSeparator());
    }

    private void appendEvent(final StringBuilder builder, final ByteBuffer buffer)
    {
        final int length = builder.length();

        if (buffer.position() + length > buffer.capacity())
        {
            write(buffer);
        }

        final int position = buffer.position();

        for (int i = 0, p = position; i < length; i++, p++)
        {
            buffer.put(p, (byte)builder.charAt(i));
        }

        buffer.position(position + length);
    }

    private void write(final ByteBuffer buffer)
    {
        try
        {
            buffer.flip();

            do
            {
                fileChannel.write(buffer);
            }
            while (buffer.remaining() > 0);
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        finally
        {
            buffer.clear();
        }

        try
        {
            checkForFileRolling(logFilePath, filename, maxFileLength);
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private static Map<String, String> asConfigOptions(final String filename)
    {
        return null != filename ? Map.of(LOG_FILENAME_PROP_NAME, filename) : Map.of();
    }

    private long getMaxFileLength(final Properties configOptions)
    {
        final String maxFileLengthStr = configOptions.getProperty(LOG_FILE_MAX_LENGTH);
        try
        {
            return null != maxFileLengthStr ? parseSize(LOG_FILE_MAX_LENGTH, maxFileLengthStr) : Long.MAX_VALUE;
        }
        catch (final NumberFormatException ex)
        {
            System.err.println("Disabling log rotation, invalid '" + LOG_FILE_MAX_LENGTH + "' - " + ex.getMessage());
            return Long.MAX_VALUE;
        }
    }

    private void checkForFileRolling(
        final Path logFilePath,
        final String filename,
        final long maxFileLength) throws IOException
    {
        if (fileChannel.position() < maxFileLength)
        {
            return;
        }

        fileChannel.close();

        Path rolledFilePath;
        do
        {
            rolledFilePath = Path.of(filename + "." + nextFileIndex);
            nextFileIndex++;
        }
        while (Files.exists(rolledFilePath));

        Files.move(logFilePath, rolledFilePath);

        fileChannel = open(logFilePath, CREATE_NEW, APPEND, WRITE);

        appendFileHeader(nanoClock.nanoTime(), epochClock.time());
        appendEvent(builder, byteBuffer);
        write(byteBuffer);
    }

    private void appendFileHeader(final long startTimeNs, final long startTimeMs)
    {
        builder.setLength(0);
        dissectLogStartMessage(startTimeNs, startTimeMs, systemDefault(), builder);

        builder.append(", enabled loggers: {");

        final EventCodeType[] eventCodeTypes = EventCodeType.values();
        final IntHashSet visited = new IntHashSet(loggers.size());

        for (final EventCodeType type : eventCodeTypes)
        {
            visited.add(type.getTypeCode());
            final ModuleLogger logger = loggers.get(type.getTypeCode());
            if (null != logger)
            {
                builder.append(type).append(": ").append(logger.version()).append(", ");
            }
        }

        loggers.forEachInt((type, logger) ->
        {
            if (!visited.contains(type))
            {
                builder.append(logger.getClass().getName()).append(": ").append(logger.version()).append(", ");
            }
        });

        if (2 < builder.length() &&
            ',' == builder.charAt(builder.length() - 2) &&
            ' ' == builder.charAt(builder.length() - 1))
        {
            builder.setLength(builder.length() - 2);
        }

        builder.append('}').append(lineSeparator());
    }
}
