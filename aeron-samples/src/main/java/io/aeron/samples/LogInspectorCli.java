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

import java.io.PrintStream;

/**
 * Command line wrapper for {@link LogInspector} that provides a proper option-parsing interface.
 * <p>
 * Parsed options are passed directly to {@link LogInspector#run} rather than via system properties,
 * so the two tools remain independently usable.
 * <p>
 * Example usage:
 * <pre>
 * java io.aeron.samples.LogInspectorCli -f ascii -z true /path/to/log 128
 * </pre>
 */
public class LogInspectorCli
{
    /**
     * Holds the result of a successful argument parse.
     *
     * @param format            output format for message body bytes: {@code "hex"} or {@code "ascii"}.
     * @param skipDefaultHeader if {@code true}, the default frame header is omitted from the output.
     * @param scanOverZeroes    if {@code true}, zero-length frames are skipped rather than stopping the scan.
     * @param logFileName       path to the log file to inspect.
     * @param messageDumpLimit  maximum number of bytes to dump from each message body.
     */
    record ParsedArgs(
        String format,
        boolean skipDefaultHeader,
        boolean scanOverZeroes,
        String logFileName,
        int messageDumpLimit)
    {
    }

    /**
     * Parse command line arguments.
     *
     * @param args to parse.
     * @return a {@link ParsedArgs} on success, or {@code null} if {@code -h} was requested.
     * @throws IllegalArgumentException if the arguments are invalid.
     */
    static ParsedArgs parseArgs(final String[] args)
    {
        String format = "hex";
        boolean skipDefaultHeader = false;
        boolean scanOverZeroes = false;

        int i = 0;
        while (i < args.length && args[i].startsWith("-"))
        {
            final String flag = args[i++];

            switch (flag)
            {
                case "-h":
                    return null;

                case "-f":
                    if (i >= args.length)
                    {
                        throw new IllegalArgumentException("option -f requires a value");
                    }
                    format = args[i++];
                    if (!"hex".equals(format) && !"ascii".equals(format))
                    {
                        throw new IllegalArgumentException(
                            "invalid value for -f: '" + format + "' (expected hex or ascii)");
                    }
                    break;

                case "-d":
                    if (i >= args.length)
                    {
                        throw new IllegalArgumentException("option -d requires a value");
                    }
                    final String dVal = args[i++];
                    if (!"true".equals(dVal) && !"false".equals(dVal))
                    {
                        throw new IllegalArgumentException(
                            "invalid value for -d: '" + dVal + "' (expected true or false)");
                    }
                    skipDefaultHeader = Boolean.parseBoolean(dVal);
                    break;

                case "-z":
                    if (i >= args.length)
                    {
                        throw new IllegalArgumentException("option -z requires a value");
                    }
                    final String zVal = args[i++];
                    if (!"true".equals(zVal) && !"false".equals(zVal))
                    {
                        throw new IllegalArgumentException(
                            "invalid value for -z: '" + zVal + "' (expected true or false)");
                    }
                    scanOverZeroes = Boolean.parseBoolean(zVal);
                    break;

                default:
                    throw new IllegalArgumentException("unknown option: '" + flag + "'");
            }
        }

        final int positionalCount = args.length - i;
        if (positionalCount < 1 || positionalCount > 2)
        {
            throw new IllegalArgumentException(
                "expected 1 or 2 positional arguments, got " + positionalCount);
        }

        final String logFileName = args[i];
        final int messageDumpLimit = positionalCount == 2 ? Integer.parseInt(args[i + 1]) : Integer.MAX_VALUE;

        return new ParsedArgs(format, skipDefaultHeader, scanOverZeroes, logFileName, messageDumpLimit);
    }

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    public static void main(final String[] args)
    {
        try
        {
            final ParsedArgs parsed = parseArgs(args);
            if (null == parsed)
            {
                printUsage(System.out);
                return;
            }
            LogInspector.run(
                parsed.logFileName,
                parsed.messageDumpLimit,
                parsed.format,
                parsed.skipDefaultHeader,
                parsed.scanOverZeroes);
        }
        catch (final IllegalArgumentException ex)
        {
            exitWithError(ex.getMessage());
        }
    }

    private static void printUsage(final PrintStream out)
    {
        out.format(
            "Usage: LogInspectorCli " +
            "[-h] [-d true|false] [-f hex|ascii] [-z true|false] <filename> [number_of_bytes]%n" +
            "  -h: Shows usage (this message).%n" +
            "  -d: Skip the default header in the output. Valid values: true or false. Defaults to false.%n" +
            "  -f: Output format for the body of each message. Valid values: hex or ascii. Defaults to hex.%n" +
            "      ascii is useful when message contents are known to be strings.%n" +
            "  -z: Skip zeros in the file. Valid values: true or false. Defaults to false.%n" +
            "      Useful for scanning a log that joined late or experienced loss.%n" +
            "  filename: path to the log file to be inspected.%n" +
            "  number_of_bytes: limits the number of bytes dumped from each message body.%n");
        out.flush();
    }

    private static void exitWithError(final String message)
    {
        System.err.println("error: " + message);
        printUsage(System.err);
        System.exit(1);
    }
}
