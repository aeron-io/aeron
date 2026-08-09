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

import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.SystemNanoClock;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.time.ZoneId;
import java.util.ServiceLoader;

import static io.aeron.CommonContext.EVENT_LOG_FILENAME_PROP_NAME;
import static io.aeron.CommonContext.EVENT_LOG_FILE_MAX_LENGTH;
import static io.aeron.logging.CborUtils.ENUM_TAG;
import static io.aeron.logging.CborUtils.IPV4_TAG;
import static io.aeron.logging.CborUtils.IPV6_TAG;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZonedDateTime.ofInstant;
import static org.agrona.PrintBufferUtil.appendPrettyHexDump;
import static org.agrona.PrintBufferUtil.byteToHexStringPadded;
import static org.agrona.SystemUtil.parseSize;

/**
 * Main implementation of the Aeron Logger.
 */
public class PrintLoggerEventCallback implements LoggerEventCallback
{
    /**
     * Cross-platform new line.
     */
    public static final String NEW_LINE = String.format("%n");

    // [53609.381133403] CLUSTER: ELECTION_STATE_CHANGE [122/122]:
    // memberId=2 CANDIDATE_BALLOT -> LEADER_LOG_REPLICATION leaderId=2 candidateTermId=0 leadershipTermId=0
    // logPosition=0 logLeadershipTermId=-1 appendPosition=0 catchupPosition=-1 reason="unanimous leader"
    private final LoggerEventWriter writer;
    private final StringBuilder sb = new StringBuilder();
    private final Int2ObjectHashMap<BinaryRenderer> binaryRenderers = new Int2ObjectHashMap<>();
    private int msgTypeId;

    /**
     * Default Constructor.
     */
    public PrintLoggerEventCallback()
    {
        this(System.getProperty(EVENT_LOG_FILENAME_PROP_NAME), retrieveMaxFileLength());
    }

    PrintLoggerEventCallback(final PrintStream out)
    {
        this(out, SystemNanoClock.INSTANCE, SystemEpochClock.INSTANCE);
    }

    PrintLoggerEventCallback(final PrintStream out, final NanoClock nanoClock, final EpochClock epochClock)
    {
        this.writer = new StreamEventWriter(out, nanoClock, epochClock);
        this.loadBinaryRenderers();
    }

    PrintLoggerEventCallback(final String filename, final long maxFileLength)
    {
        this(filename, maxFileLength, SystemNanoClock.INSTANCE, SystemEpochClock.INSTANCE);
    }

    PrintLoggerEventCallback(
        final String filename,
        final long maxFileLength,
        final NanoClock nanoClock,
        final EpochClock epochClock)
    {
        this.writer = null != filename ?
            new RollingFileEventWriter(filename, maxFileLength, nanoClock, epochClock) :
            new StreamEventWriter(System.out, nanoClock, epochClock);
        this.loadBinaryRenderers();
    }

    static void appendLogStartMessage(
        final StringBuilder sb,
        final long timestampNs,
        final long timestampMs,
        final ZoneId zone) throws IOException
    {
        sb.setLength(0);
        LogUtil.appendTimestamp(sb, timestampNs);
        sb.append("log started ")
            .append(RollingFileEventWriter.DATE_TIME_FORMATTER.format(ofInstant(ofEpochMilli(timestampMs), zone)))
            .append(NEW_LINE);
    }

    private void loadBinaryRenderers()
    {
        for (final BinaryRenderer binaryRenderer : ServiceLoader.load(BinaryRenderer.class))
        {
            for (final int msgTypeId : binaryRenderer.supportingMsgTypeIds())
            {
                binaryRenderers.put(msgTypeId, binaryRenderer);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onHeader(
        final int eventType,
        final int eventCode,
        final CharSequence eventCodeName,
        final long timestamp)
    {
        msgTypeId = (eventType << 16) | eventCode;
        final EventCodeType eventCodeType = EventCodeType.get(eventType);
        sb.delete(0, sb.length());

        LogUtil.appendTimestamp(sb, timestamp);
        sb.append(eventCodeType.name()).append(": ");
        sb.append(eventCodeName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onValue(final CharSequence name, final long tag, final CharSequence value)
    {
        sb.append(' ').append(name).append('=');
        if (ENUM_TAG == tag)
        {
            sb.append(value);
        }
        else
        {
            sb.append("\"").append(value).append("\"");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onValue(final CharSequence name, final long tag, final long value)
    {
        sb.append(' ').append(name).append('=').append(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onValue(final CharSequence name, final long tag, final boolean value)
    {
        sb.append(' ').append(name).append('=').append(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onValue(final CharSequence name, final long tag, final DirectBuffer value)
    {
        sb.append(' ').append(name).append('=');
        if (IPV4_TAG == tag && 4 == value.capacity())
        {
            sb.append(0xFF & value.getByte(0)).append('.');
            sb.append(0xFF & value.getByte(1)).append('.');
            sb.append(0xFF & value.getByte(2)).append('.');
            sb.append(0xFF & value.getByte(3));
        }
        else if (IPV6_TAG == tag && 16 == value.capacity())
        {
            appendIpV6Address(sb, value);
        }
        else if (binaryRenderers.containsKey(msgTypeId))
        {
            final BinaryRenderer renderer = binaryRenderers.get(msgTypeId);
            renderer.append(sb, msgTypeId, value, 0, value.capacity());
        }
        else
        {
            sb.append(NEW_LINE);
            appendPrettyHexDump(sb, value);
            sb.append(NEW_LINE);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onFooter(final boolean truncated)
    {
        if (truncated)
        {
            sb.append("truncated");
        }
        try
        {
            writer.write(sb);
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException(e);
        }
        sb.delete(0, sb.length());
    }

    private static long retrieveMaxFileLength()
    {
        final String maxFileLengthStr = System.getProperty(EVENT_LOG_FILE_MAX_LENGTH);
        try
        {
            return null != maxFileLengthStr ? parseSize(EVENT_LOG_FILE_MAX_LENGTH, maxFileLengthStr) : Long.MAX_VALUE;
        }
        catch (final NumberFormatException ex)
        {
            System.err.println(
                "Disabling log rotation, invalid '" + EVENT_LOG_FILE_MAX_LENGTH + "' - " + ex.getMessage());
            return Long.MAX_VALUE;
        }
    }

    private static int ipV6Group(final DirectBuffer buffer, final int index)
    {
        final int byteOffset = (index * 2);
        return ((buffer.getByte(byteOffset) << 8) & 0xFF00) | (buffer.getByte(byteOffset + 1) & 0xFF);
    }

    private static void appendIpV6Address(final StringBuilder builder, final DirectBuffer buffer)
    {
        int bestStart = -1;
        int bestLength = 0;
        int runStart = -1;
        int runLength = 0;

        for (int i = 0; i < 8; i++)
        {
            if (0 == ipV6Group(buffer, i))
            {
                if (-1 == runStart)
                {
                    runStart = i;
                }
                runLength++;
            }
            else
            {
                if (runLength > bestLength)
                {
                    bestStart = runStart;
                    bestLength = runLength;
                }
                runStart = -1;
                runLength = 0;
            }
        }

        if (runLength > bestLength)
        {
            bestStart = runStart;
            bestLength = runLength;
        }

        if (bestLength < 2)
        {
            bestStart = -1;
        }

        builder.append('[');
        for (int i = 0; i < 8;)
        {
            if (i == bestStart)
            {
                builder.append("::");
                i += bestLength;
                continue;
            }

            builder.append(byteToHexStringPadded(0xFF & buffer.getByte(i * 2)));
            builder.append(byteToHexStringPadded(0xFF & buffer.getByte((i * 2) + 1)));

            i++;

            if (i < 8 && i != bestStart)
            {
                builder.append(':');
            }
        }
        builder.append(']');
    }

    static boolean endsWithNewLine(final StringBuilder sb)
    {
        if (sb.length() < NEW_LINE.length())
        {
            return false;
        }

        for (int i = NEW_LINE.length(); --i != -1;)
        {
            if (NEW_LINE.charAt(i) != sb.charAt((sb.length() - NEW_LINE.length() + i)))
            {
                return false;
            }
        }

        return true;
    }
}
