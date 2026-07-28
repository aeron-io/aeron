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

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.aeron.logging.CborUtils.ENUM_TAG;
import static io.aeron.logging.CborUtils.IPV4_TAG;
import static io.aeron.logging.CborUtils.IPV6_TAG;
import static java.nio.channels.FileChannel.open;
import static java.nio.file.StandardOpenOption.*;
import static java.nio.file.StandardOpenOption.CREATE;
import static org.agrona.PrintBufferUtil.appendPrettyHexDump;
import static org.agrona.PrintBufferUtil.byteToHexStringPadded;

class PrintLoggerEventCallback implements LoggerEventCallback
{
    // [53609.381133403] CLUSTER: ELECTION_STATE_CHANGE [122/122]:
    // memberId=2 CANDIDATE_BALLOT -> LEADER_LOG_REPLICATION leaderId=2 candidateTermId=0 leadershipTermId=0
    // logPosition=0 logLeadershipTermId=-1 appendPosition=0 catchupPosition=-1 reason="unanimous leader"
    private final PrintStream out;
    private final Path logFilePath;
    private final StringBuilder sb = new StringBuilder();
    private final String filename;
    private final long maxFileLength;
    private int nextFileIndex = 1;
    private FileChannel mainFileChannel;
    private final String newLine = String.format("%n");
    PrintLoggerEventCallback()
    {
        // TODO: Set things up based on properties // See ModuleLoggerReaderAgent.java
        this("", 0);
    }

    PrintLoggerEventCallback(final PrintStream out)
    {
        this.out = out;
        this.filename = null;
        this.logFilePath = null;
        this.maxFileLength = 0;
    }

    PrintLoggerEventCallback(final String filename, final long maxFileLength)
    {
        this.filename = filename;
        this.logFilePath = null != filename ? Path.of(filename) : null;
        this.maxFileLength = maxFileLength;
        if (null == this.filename)
        {
            this.out = System.out;
        }
        else
        {
            this.out = null;
            try
            {
                this.mainFileChannel = open(this.logFilePath, CREATE, WRITE, APPEND);
            }
            catch (final IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }
    }

    public void onHeader(
        final int eventType,
        final int eventCode,
        final CharSequence eventCodeName,
        final long timestamp)
    {
        final EventCodeType eventCodeType = EventCodeType.get(eventType);

        sb.delete(0, sb.length());

        LogUtil.appendTimestamp(sb, timestamp);
        sb.append(eventCodeType.name()).append(": ");
        sb.append(eventCodeName);
    }

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

    public void onValue(final CharSequence name, final long tag, final long value)
    {
        sb.append(' ').append(name).append('=').append(value);
    }

    public void onValue(final CharSequence name, final long tag, final boolean value)
    {
        sb.append(' ').append(name).append('=').append(value);
    }

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
        else
        {
            sb.append(newLine);
            appendPrettyHexDump(sb, value);
            sb.append(newLine);
        }
    }

    public void onFooter(final boolean truncated)
    {
        if (truncated)
        {
            sb.append("truncated");
        }
        if (null == mainFileChannel)
        {
            if (endsWithNewLine(sb))
            {
                out.print(sb);
            }
            else
            {
                out.println(sb);
            }
        }
        else
        {
            try
            {
                mainFileChannel.write(ByteBuffer.wrap(sb.toString().getBytes()));
                if (mainFileChannel.position() >= this.maxFileLength)
                {
                    mainFileChannel.close();
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
                    mainFileChannel = open(logFilePath, CREATE_NEW, WRITE, APPEND);
                }
            }
            catch (final IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }
        sb.delete(0, sb.length());
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

    private boolean endsWithNewLine(final StringBuilder sb)
    {
        if (sb.length() < newLine.length())
        {
            return false;
        }

        for (int i = newLine.length(); --i != -1;)
        {
            if (newLine.charAt(i) != sb.charAt((sb.length() - newLine.length() + i)))
            {
                return false;
            }
        }

        return true;
    }
}
