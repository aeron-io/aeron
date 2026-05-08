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
package io.aeron.samples;

import io.aeron.LogBuffers;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.PrintStream;
import java.util.Date;

import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.lang.Math.min;

/**
 * Command line utility for inspecting a log buffer to see what terms and messages it contains.
 */
public class LogInspector
{
    /**
     * Data format property name for fragments which can be ASCII or HEX.
     */
    public static final String AERON_LOG_DATA_FORMAT_PROP_NAME = "aeron.log.inspector.data.format";

    /**
     * Default data format for fragments derived from the system property, or HEX if not set.
     */
    public static final String AERON_LOG_DATA_FORMAT_DEFAULT = System.getProperty(
        AERON_LOG_DATA_FORMAT_PROP_NAME, "hex").toLowerCase();

    /**
     * Property name for if the default header should be skipped for output.
     */
    public static final String AERON_LOG_SKIP_DEFAULT_HEADER_PROP_NAME = "aeron.log.inspector.skipDefaultHeader";

    /**
     * Default for whether the default header should be skipped, derived from the system property.
     */
    public static final boolean AERON_LOG_SKIP_DEFAULT_HEADER_DEFAULT =
        "true".equals(System.getProperty(AERON_LOG_SKIP_DEFAULT_HEADER_PROP_NAME));

    /**
     * Property name for if zeros should be skipped in the output to reduce noise.
     */
    public static final String AERON_LOG_SCAN_OVER_ZEROES_PROP_NAME = "aeron.log.inspector.scanOverZeroes";

    /**
     * Default for whether zeros should be skipped in the output, derived from the system property.
     */
    public static final boolean AERON_LOG_SCAN_OVER_ZEROES_DEFAULT =
        "true".equals(System.getProperty(AERON_LOG_SCAN_OVER_ZEROES_PROP_NAME));

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    public static void main(final String[] args)
    {
        final PrintStream out = System.out;
        if (args.length < 1)
        {
            out.println("Usage: LogInspector <logFileName> [dump limit in bytes per message]");
            return;
        }

        final String logFileName = args[0];
        final int messageDumpLimit = args.length >= 2 ? Integer.parseInt(args[1]) : Integer.MAX_VALUE;

        run(logFileName, messageDumpLimit,
            AERON_LOG_DATA_FORMAT_DEFAULT,
            AERON_LOG_SKIP_DEFAULT_HEADER_DEFAULT,
            AERON_LOG_SCAN_OVER_ZEROES_DEFAULT);
    }

    /**
     * Run the log inspection, printing output to {@link System#out}.
     *
     * @param logFileName       path to the log file to inspect.
     * @param messageDumpLimit  maximum number of bytes to dump from each message body.
     * @param format            output format for message body bytes: {@code "hex"} or {@code "ascii"}.
     * @param skipDefaultHeader if {@code true}, the default frame header is omitted from the output.
     * @param scanOverZeroes    if {@code true}, zero-length frames are skipped rather than stopping the scan.
     */
    public static void run(
        final String logFileName,
        final int messageDumpLimit,
        final String format,
        final boolean skipDefaultHeader,
        final boolean scanOverZeroes)
    {
        run(logFileName, messageDumpLimit, format, skipDefaultHeader, scanOverZeroes, System.out);
    }

    /**
     * Run the log inspection, printing output to the provided {@link PrintStream}.
     *
     * @param logFileName       path to the log file to inspect.
     * @param messageDumpLimit  maximum number of bytes to dump from each message body.
     * @param format            output format for message body bytes: {@code "hex"} or {@code "ascii"}.
     * @param skipDefaultHeader if {@code true}, the default frame header is omitted from the output.
     * @param scanOverZeroes    if {@code true}, zero-length frames are skipped rather than stopping the scan.
     * @param out               destination for all inspection output.
     */
    @SuppressWarnings("methodLength")
    public static void run(
        final String logFileName,
        final int messageDumpLimit,
        final String format,
        final boolean skipDefaultHeader,
        final boolean scanOverZeroes,
        final PrintStream out)
    {
        try (LogBuffers logBuffers = new LogBuffers(logFileName))
        {
            out.println("======================================================================");
            out.println(new Date() + " Inspection dump for " + logFileName);
            out.println("======================================================================");

            final DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight();
            final UnsafeBuffer[] termBuffers = logBuffers.duplicateTermBuffers();
            final int termLength = logBuffers.termLength();
            final UnsafeBuffer metaDataBuffer = logBuffers.metaDataBuffer();
            final int initialTermId = initialTermId(metaDataBuffer);

            out.println("   Is connected: " + isConnected(metaDataBuffer));
            out.println("Initial term id: " + initialTermId);
            out.println("     Term count: " + activeTermCount(metaDataBuffer));
            out.println("   Active index: " + indexByTermCount(activeTermCount(metaDataBuffer)));
            out.println("    Term length: " + termLength);
            out.println("     MTU length: " + mtuLength(metaDataBuffer));
            out.println("      Page size: " + pageSize(metaDataBuffer));
            out.println("   EOS position: " + endOfStreamPosition(metaDataBuffer));
            out.println();

            if (!skipDefaultHeader)
            {
                dataHeaderFlyweight.wrap(defaultFrameHeader(metaDataBuffer));
                out.println("default " + dataHeaderFlyweight);
            }
            out.println();

            for (int i = 0; i < PARTITION_COUNT; i++)
            {
                final long rawTail = rawTailVolatile(metaDataBuffer, i);
                final long termOffset = rawTail & 0xFFFF_FFFFL;
                final int termId = termId(rawTail);
                final int offset = (int)Math.min(termOffset, termLength);
                final int positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);
                final long position = computePosition(termId, offset, positionBitsToShift, initialTermId);

                out.println("Index " + i + " Term Meta Data" +
                    " termOffset=" + termOffset +
                    " termId=" + termId +
                    " rawTail=" + rawTail +
                    " position=" + position);
            }

            for (int i = 0; i < PARTITION_COUNT; i++)
            {
                out.println();
                out.println("======================================================================");
                out.println("Index " + i + " Term Data");
                out.println();

                final UnsafeBuffer termBuffer = termBuffers[i];
                int offset = 0;
                do
                {
                    dataHeaderFlyweight.wrap(termBuffer, offset, termLength - offset);
                    out.println(offset + ": " + dataHeaderFlyweight);

                    final int frameLength = dataHeaderFlyweight.frameLength();
                    if (frameLength < DataHeaderFlyweight.HEADER_LENGTH)
                    {
                        if (0 == frameLength && scanOverZeroes)
                        {
                            offset += FrameDescriptor.FRAME_ALIGNMENT;
                            continue;
                        }

                        try
                        {
                            final int limit = min(termLength - (offset + HEADER_LENGTH), messageDumpLimit);
                            out.println(formatBytes(termBuffer, offset + HEADER_LENGTH, limit, format));
                        }
                        catch (final Exception ex)
                        {
                            System.err.println("frameLength=" + frameLength + " offset=" + offset);
                            ex.printStackTrace();
                        }

                        break;
                    }

                    final int limit = min(frameLength - HEADER_LENGTH, messageDumpLimit);
                    out.println(formatBytes(termBuffer, offset + HEADER_LENGTH, limit, format));

                    offset += BitUtil.align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
                }
                while (offset < termLength);
            }
        }
    }

    /**
     * Format bytes in a buffer using the default data format from {@link #AERON_LOG_DATA_FORMAT_DEFAULT}.
     *
     * @param buffer containing the bytes to be formatted.
     * @param offset in the buffer at which the bytes begin.
     * @param length of the bytes in the buffer.
     * @return a char array of the formatted bytes.
     */
    public static char[] formatBytes(final DirectBuffer buffer, final int offset, final int length)
    {
        return formatBytes(buffer, offset, length, AERON_LOG_DATA_FORMAT_DEFAULT);
    }

    /**
     * Format bytes in a buffer using the specified data format.
     *
     * @param buffer containing the bytes to be formatted.
     * @param offset in the buffer at which the bytes begin.
     * @param length of the bytes in the buffer.
     * @param format {@code "ascii"} for ASCII output, or any other value for HEX.
     * @return a char array of the formatted bytes.
     */
    public static char[] formatBytes(
        final DirectBuffer buffer, final int offset, final int length, final String format)
    {
        switch (format)
        {
            case "us-ascii":
            case "us_ascii":
            case "ascii":
                return bytesToAscii(buffer, offset, length);

            default:
                return bytesToHex(buffer, offset, length);
        }
    }

    private static char[] bytesToAscii(final DirectBuffer buffer, final int offset, final int length)
    {
        final char[] chars = new char[length];

        for (int i = 0; i < length; i++)
        {
            byte b = buffer.getByte(offset + i);

            if (b < 0)
            {
                b = 0;
            }

            chars[i] = (char)b;
        }

        return chars;
    }

    /**
     * Format bytes to HEX for printing.
     *
     * @param buffer containing the bytes.
     * @param offset in the buffer at which the bytes begin.
     * @param length of the bytes in the buffer.
     * @return a char array of the formatted bytes in HEX.
     */
    public static char[] bytesToHex(final DirectBuffer buffer, final int offset, final int length)
    {
        final char[] chars = new char[length * 2];

        for (int i = 0; i < length; i++)
        {
            final int b = buffer.getByte(offset + i) & 0xFF;

            chars[i * 2] = HEX_ARRAY[b >>> 4];
            chars[i * 2 + 1] = HEX_ARRAY[b & 0x0F];
        }

        return chars;
    }
}
