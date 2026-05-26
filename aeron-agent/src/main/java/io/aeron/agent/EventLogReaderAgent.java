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
package io.aeron.agent;

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
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.aeron.agent.CommonEventDissector.dissectLogStartMessage;
import static io.aeron.agent.EventConfiguration.EVENT_READER_FRAME_LIMIT;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
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
 * Simple reader of {@link EventConfiguration#EVENT_RING_BUFFER} that appends to {@link System#out} by default
 * or to file if {@link #LOG_FILENAME_PROP_NAME} System property is set.
 */
public final class EventLogReaderAgent implements Agent
{
    /**
     * Event Buffer length system property name. If not set then output will default to {@link System#out}.
     */
    public static final String LOG_FILENAME_PROP_NAME = ConfigOption.LOG_FILENAME;

    /**
     * Length of the buffer used for writing rendered messages to the log file.
     */
    public static final int BUFFER_LENGTH = (MAX_EVENT_LENGTH + lineSeparator().length()) * 2;

    private final ManyToOneRingBuffer ringBuffer = EventConfiguration.EVENT_RING_BUFFER;
    private final StringBuilder builder = new StringBuilder(MAX_EVENT_LENGTH);
    private final MessageHandler messageHandler = this::onMessage;
    private final ByteBuffer byteBuffer;
    private final Int2ObjectHashMap<ComponentLogger> loggers = new Int2ObjectHashMap<>();
    private final PrintStream out;
    private final NanoClock nanoClock;
    private final EpochClock epochClock;
    private final long maxFileLength;
    private final String filename;
    private int nextFileIndex = 1;

    private FileChannel fileChannel;

    EventLogReaderAgent(final String filename, final List<ComponentLogger> loggers)
    {
        this(filename, System.out, SystemNanoClock.INSTANCE, SystemEpochClock.INSTANCE, loggers);
    }

    EventLogReaderAgent(final Map<String, String> configOptions, final List<ComponentLogger> loggers)
    {
        this(
            configOptions,
            System.out,
            SystemNanoClock.INSTANCE,
            SystemEpochClock.INSTANCE, loggers);
    }

    EventLogReaderAgent(
        final String filename,
        final PrintStream out,
        final NanoClock nanoClock,
        final EpochClock epochClock,
        final List<ComponentLogger> loggers)
    {
        this(asConfigOptions(filename), out, nanoClock, epochClock, loggers);
    }

    EventLogReaderAgent(
        final Map<String, String> configOptions,
        final PrintStream out,
        final NanoClock nanoClock,
        final EpochClock epochClock,
        final List<ComponentLogger> loggers)
    {
        filename = configOptions.get(LOG_FILENAME_PROP_NAME);
        maxFileLength = getMaxFileLength(configOptions);

        this.nanoClock = Objects.requireNonNull(nanoClock);
        this.epochClock = Objects.requireNonNull(epochClock);
        for (final ComponentLogger componentLogger : loggers)
        {
            this.loggers.put(componentLogger.typeCode(), componentLogger);
        }

        if (null != filename)
        {
            this.out = null;
            try
            {
                fileChannel = open(Paths.get(filename), CREATE, APPEND, WRITE);
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
        final Int2ObjectHashMap<ComponentLogger> loggers,
        final StringBuilder builder)
    {
        final ComponentLogger componentLogger = loggers.get(eventCodeTypeId);
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
            checkForFileRolling(filename, maxFileLength);
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

    private long getMaxFileLength(final Map<String, String> configOptions)
    {
        final String maxFileLengthStr = configOptions.get(ConfigOption.LOG_FILE_MAX_LENGTH);
        try
        {
            return null != maxFileLengthStr ?
                parseSize(ConfigOption.LOG_FILE_MAX_LENGTH, maxFileLengthStr) : Long.MAX_VALUE;
        }
        catch (final NumberFormatException ex)
        {
            System.err.println(ex.getMessage() + "  Disabling log rotation.");
            return Long.MAX_VALUE;
        }
    }

    private void checkForFileRolling(final String filename, final long maxFileLength) throws IOException
    {
        if (fileChannel.position() < maxFileLength)
        {
            return;
        }

        fileChannel.close();
        final Path file = Path.of(filename);

        Path rolledFile;
        do
        {
            rolledFile = Path.of(filename + "." + nextFileIndex);
            nextFileIndex++;
        }
        while (Files.exists(rolledFile));

        Files.move(file, rolledFile);

        fileChannel = open(Paths.get(filename), CREATE_NEW, APPEND, WRITE);

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
            final ComponentLogger logger = loggers.get(type.getTypeCode());
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
