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

import io.aeron.CncFileDescriptor;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.AsciiEncoding;
import org.agrona.DirectBuffer;
import org.agrona.SystemUtil;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.status.CountersReader;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static io.aeron.driver.status.PerImageIndicator.PER_IMAGE_TYPE_ID;
import static io.aeron.driver.status.PublisherLimit.PUBLISHER_LIMIT_TYPE_ID;
import static io.aeron.driver.status.PublisherPos.PUBLISHER_POS_TYPE_ID;
import static io.aeron.driver.status.ReceiveChannelStatus.RECEIVE_CHANNEL_STATUS_TYPE_ID;
import static io.aeron.driver.status.ReceiverPos.RECEIVER_POS_TYPE_ID;
import static io.aeron.driver.status.SendChannelStatus.SEND_CHANNEL_STATUS_TYPE_ID;
import static io.aeron.driver.status.SenderLimit.SENDER_LIMIT_TYPE_ID;
import static io.aeron.driver.status.StreamCounter.CHANNEL_OFFSET;
import static io.aeron.driver.status.StreamCounter.REGISTRATION_ID_OFFSET;
import static io.aeron.driver.status.StreamCounter.SESSION_ID_OFFSET;
import static io.aeron.driver.status.StreamCounter.STREAM_ID_OFFSET;
import static io.aeron.driver.status.SystemCounterDescriptor.SYSTEM_COUNTER_TYPE_ID;

/**
 * Tool for printing out Aeron counters. A command-and-control (CnC) file is maintained by media driver
 * in shared memory. This application reads the CnC file and prints the counters. Layout of the CnC file is
 * described in {@link CncFileDescriptor}.
 * <p>
 * This tool accepts filters on the command line, e.g. for connections only see example below:
 * <p>
 * <code>
 * java -cp aeron-samples/build/libs/samples.jar io.aeron.samples.AeronStat type=[1-9] identity=12345
 * </code>
 */
public class AeronStat
{
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("HH:mm:ss");
    private static final String ANSI_CLS = "\u001b[2J";
    private static final String ANSI_HOME = "\u001b[H";

    /**
     * The delay in seconds between each update.
     */
    private static final String DELAY = "delay";

    /**
     * Whether to watch for updates or run once.
     */
    private static final String WATCH = "watch";

    /**
     * Types of the counters.
     * <ul>
     * <li>0: System Counters</li>
     * <li>1 - 5, 9, 10, 11: Stream Positions and Indicators</li>
     * <li>6 - 7: Channel Endpoint Status</li>
     * </ul>
     */
    private static final String COUNTER_TYPE_ID = "type";

    /**
     * The identity of each counter that can either be the system counter id or registration id for positions.
     */
    private static final String COUNTER_IDENTITY = "identity";

    /**
     * Session id filter to be used for position counters.
     */
    private static final String COUNTER_SESSION_ID = "session";

    /**
     * Stream id filter to be used for position counters.
     */
    private static final String COUNTER_STREAM_ID = "stream";

    /**
     * Channel filter to be used for position counters.
     */
    private static final String COUNTER_CHANNEL = "channel";

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     * @throws IOException          if an error occurs writing to the console.
     * @throws InterruptedException if the thread sleep delay is interrupted.
     */
    public static void main(final String[] args) throws IOException, InterruptedException
    {
        long delayMs = 1000L;
        boolean watch = true;
        Pattern typeFilter = null;
        Pattern identityFilter = null;
        Pattern sessionFilter = null;
        Pattern streamFilter = null;
        Pattern channelFilter = null;

        if (0 != args.length)
        {
            checkForHelp(args);

            for (final String arg : args)
            {
                final int equalsIndex = arg.indexOf('=');
                if (-1 == equalsIndex)
                {
                    System.out.println("Arguments must be in name=pattern format: Invalid '" + arg + "'");
                    return;
                }

                final String argName = arg.substring(0, equalsIndex);
                final String argValue = arg.substring(equalsIndex + 1);

                switch (argName)
                {
                    case WATCH:
                        watch = Boolean.parseBoolean(argValue);
                        break;

                    case DELAY:
                        delayMs = Long.parseLong(argValue) * 1000L;
                        break;

                    case COUNTER_TYPE_ID:
                        typeFilter = Pattern.compile(argValue);
                        break;

                    case COUNTER_IDENTITY:
                        identityFilter = Pattern.compile(argValue);
                        break;

                    case COUNTER_SESSION_ID:
                        sessionFilter = Pattern.compile(argValue);
                        break;

                    case COUNTER_STREAM_ID:
                        streamFilter = Pattern.compile(argValue);
                        break;

                    case COUNTER_CHANNEL:
                        channelFilter = Pattern.compile(argValue);
                        break;

                    default:
                        System.out.println("Unrecognised argument: '" + arg + "'");
                        return;
                }
            }
        }

        final CncFileReader cncFileReader = CncFileReader.map();

        final CounterFilter counterFilter = new CounterFilter(
            typeFilter, identityFilter, sessionFilter, streamFilter, channelFilter);

        if (watch)
        {
            workLoop(delayMs, () -> printOutput(cncFileReader, counterFilter));
        }
        else
        {
            printOutput(cncFileReader, counterFilter);
        }
    }

    @SuppressWarnings("try")
    private static void workLoop(final long delayMs, final Runnable outputPrinter)
        throws IOException, InterruptedException
    {
        final AtomicBoolean running = new AtomicBoolean(true);
        try (ShutdownSignalBarrier ignore = new ShutdownSignalBarrier(() -> running.set(false)))
        {
            do
            {
                clearScreen();
                outputPrinter.run();
                Thread.sleep(delayMs);
            }
            while (running.get());
        }
    }

    private static void printOutput(final CncFileReader cncFileReader, final CounterFilter counterFilter)
    {
        System.out.print(DATE_FORMAT.format(new Date()));
        System.out.println(
            " - Aeron Stat (CnC v" + cncFileReader.semanticVersion() + ")" +
            ", pid " + cncFileReader.driverPid() +
            ", heartbeat age " + cncFileReader.driverHeartbeatAgeMs() + "ms");
        System.out.println("======================================================================");

        final CountersReader counters = cncFileReader.countersReader();
        final int maxIdWidth = AsciiEncoding.digitCount(counters.maxCounterId());
        final String formatString = "%" + maxIdWidth + "d: %,26d - %s%n";

        counters.forEach(
            (counterId, typeId, keyBuffer, label) ->
            {
                if (counterFilter.filter(typeId, keyBuffer))
                {
                    final long value = counters.getCounterValue(counterId);
                    System.out.format(formatString, counterId, value, label);
                }
            }
        );

        System.out.println("--");
    }

    private static void checkForHelp(final String[] args)
    {
        for (final String arg : args)
        {
            if ("-?".equals(arg) || "-h".equals(arg) || "-help".equals(arg))
            {
                System.out.format(
                    "Usage: [-Daeron.dir=<directory containing CnC file>] AeronStat%n" +
                    "\t[delay=<seconds between updates>]%n" +
                    "\t[watch=<true|false>]%n" +
                    "filter by optional regex patterns:%n" +
                    "\t[type=<pattern>]%n" +
                    "\t[identity=<pattern>]%n" +
                    "\t[session=<pattern>]%n" +
                    "\t[stream=<pattern>]%n" +
                    "\t[channel=<pattern>]%n");

                System.exit(0);
            }
        }
    }

    private static void clearScreen() throws IOException, InterruptedException
    {
        if (SystemUtil.isWindows())
        {
            new ProcessBuilder("C:\\Windows\\System32\\cmd.exe", "/c", "cls").inheritIO().start().waitFor();
        }
        else
        {
            System.out.print(ANSI_CLS + ANSI_HOME);
        }
    }

    static class CounterFilter
    {
        private final Pattern typeFilter;
        private final Pattern identityFilter;
        private final Pattern sessionFilter;
        private final Pattern streamFilter;
        private final Pattern channelFilter;

        CounterFilter(
            final Pattern typeFilter,
            final Pattern identityFilter,
            final Pattern sessionFilter,
            final Pattern streamFilter,
            final Pattern channelFilter)
        {
            this.typeFilter = typeFilter;
            this.identityFilter = identityFilter;
            this.sessionFilter = sessionFilter;
            this.streamFilter = streamFilter;
            this.channelFilter = channelFilter;
        }

        private static boolean match(final Pattern pattern, final Supplier<String> supplier)
        {
            return null == pattern || pattern.matcher(supplier.get()).find();
        }

        boolean filter(final int typeId, final DirectBuffer keyBuffer)
        {
            if (!match(typeFilter, () -> Integer.toString(typeId)))
            {
                return false;
            }

            if (SYSTEM_COUNTER_TYPE_ID == typeId && !match(identityFilter, () -> Integer.toString(keyBuffer.getInt(0))))
            {
                return false;
            }
            else if ((typeId >= PUBLISHER_LIMIT_TYPE_ID && typeId <= RECEIVER_POS_TYPE_ID) ||
                typeId == SENDER_LIMIT_TYPE_ID || typeId == PER_IMAGE_TYPE_ID || typeId == PUBLISHER_POS_TYPE_ID)
            {
                return
                    match(identityFilter, () -> Long.toString(keyBuffer.getLong(REGISTRATION_ID_OFFSET))) &&
                    match(sessionFilter, () -> Integer.toString(keyBuffer.getInt(SESSION_ID_OFFSET))) &&
                    match(streamFilter, () -> Integer.toString(keyBuffer.getInt(STREAM_ID_OFFSET))) &&
                    match(channelFilter, () -> keyBuffer.getStringAscii(CHANNEL_OFFSET));
            }
            else if (typeId >= SEND_CHANNEL_STATUS_TYPE_ID && typeId <= RECEIVE_CHANNEL_STATUS_TYPE_ID)
            {
                return match(channelFilter, () -> keyBuffer.getStringAscii(ChannelEndpointStatus.CHANNEL_OFFSET));
            }

            return true;
        }
    }
}
