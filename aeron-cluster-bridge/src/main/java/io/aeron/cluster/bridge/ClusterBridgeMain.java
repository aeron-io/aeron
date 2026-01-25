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
package io.aeron.cluster.bridge;

import org.agrona.concurrent.ShutdownSignalBarrier;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unified entry point for the Cluster Bridge Service.
 * <p>
 * <b>Design Rationale (SOLID - Single Responsibility):</b>
 * This class has one responsibility: parsing arguments and dispatching to the
 * appropriate operational mode. The actual logic is delegated to specialized
 * components ({@link BridgeSender}, {@link BridgeReceiver}).
 * <p>
 * <b>Design Rationale (OOP - Command Pattern):</b>
 * Different operational modes are encapsulated as separate methods, making
 * the code easy to extend with new modes.
 * <p>
 * <b>Supported Modes:</b>
 * <table border="1">
 *   <tr><th>Mode</th><th>Description</th></tr>
 *   <tr><td>sender</td><td>Run as bridge sender (one direction)</td></tr>
 *   <tr><td>receiver</td><td>Run as bridge receiver (one direction)</td></tr>
 *   <tr><td>ME</td><td>Run as Matching Engine node (bidirectional)</td></tr>
 *   <tr><td>RMS</td><td>Run as Risk Management node (bidirectional)</td></tr>
 * </table>
 * <p>
 * <b>Usage Examples:</b>
 * <pre>
 * # Run as sender
 * java ClusterBridgeMain --mode=sender
 *
 * # Run as ME node (sends to RMS, receives from RMS)
 * java ClusterBridgeMain --mode=ME --message-count=100
 * </pre>
 * <p>
 * <b>Assumptions:</b>
 * <ul>
 *   <li>System properties take precedence over command-line arguments</li>
 *   <li>Archive directory is shared between sender/receiver on same machine</li>
 *   <li>Default mode is SENDER if not specified</li>
 * </ul>
 */
public final class ClusterBridgeMain
{
    /**
     * Operational modes for the bridge service.
     */
    public enum Mode
    {
        /** Run as sender only (single direction) */
        SENDER,

        /** Run as receiver only (single direction) */
        RECEIVER,

        /** Run as Matching Engine node: sends ME→RMS, receives RMS→ME */
        ME,

        /** Run as Risk Management node: sends RMS→ME, receives ME→RMS */
        RMS
    }

    private ClusterBridgeMain()
    {
        // Static utility class
    }

    /**
     * Main entry point.
     *
     * @param args command-line arguments
     * @throws Exception if startup fails
     */
    public static void main(final String[] args) throws Exception
    {
        final Mode mode = parseMode(args);
        final int messageCount = parseMessageCount(args);

        System.out.println("[ClusterBridge] =====================================");
        System.out.println("[ClusterBridge] Cluster Bridge Service");
        System.out.println("[ClusterBridge] Mode: " + mode);
        System.out.println("[ClusterBridge] Message count: " + messageCount);
        System.out.println("[ClusterBridge] =====================================");

        switch (mode)
        {
            case SENDER:
                runSender();
                break;

            case RECEIVER:
                runReceiver();
                break;

            case ME:
                runBidirectional(BridgeConfiguration.Direction.ME_TO_RMS,
                    BridgeConfiguration.Direction.RMS_TO_ME, messageCount);
                break;

            case RMS:
                runBidirectional(BridgeConfiguration.Direction.RMS_TO_ME,
                    BridgeConfiguration.Direction.ME_TO_RMS, messageCount);
                break;

            default:
                printUsage();
                System.exit(1);
        }
    }

    /**
     * Parse the mode from command-line arguments.
     */
    private static Mode parseMode(final String[] args)
    {
        for (final String arg : args)
        {
            if (arg.startsWith("--mode="))
            {
                final String modeStr = arg.substring(7).toUpperCase();
                try
                {
                    return Mode.valueOf(modeStr);
                }
                catch (final IllegalArgumentException e)
                {
                    System.err.println("Unknown mode: " + modeStr);
                    printUsage();
                    System.exit(1);
                }
            }
        }
        return Mode.SENDER;
    }

    /**
     * Parse the message count from command-line arguments.
     */
    private static int parseMessageCount(final String[] args)
    {
        for (final String arg : args)
        {
            if (arg.startsWith("--message-count="))
            {
                return Integer.parseInt(arg.substring(16));
            }
        }
        return BridgeConfiguration.messageCount();
    }

    /**
     * Run as standalone sender.
     */
    private static void runSender() throws Exception
    {
        BridgeSender.main(new String[0]);
    }

    /**
     * Run as standalone receiver.
     */
    private static void runReceiver() throws Exception
    {
        BridgeReceiver.main(new String[0]);
    }

    /**
     * Run in bidirectional mode.
     * <p>
     * <b>Note:</b> This is a simplified demonstration. In production:
     * <ul>
     *   <li>Sender and receiver would use different archive directories</li>
     *   <li>Each would run in a separate process/container</li>
     *   <li>Proper channel configuration for multicast would be used</li>
     * </ul>
     */
    private static void runBidirectional(
        final BridgeConfiguration.Direction sendDirection,
        final BridgeConfiguration.Direction receiveDirection,
        final int messageCount) throws Exception
    {
        System.out.println("[ClusterBridge] Starting bidirectional mode");
        System.out.println("[ClusterBridge] Send direction: " + sendDirection);
        System.out.println("[ClusterBridge] Receive direction: " + receiveDirection);

        final AtomicBoolean running = new AtomicBoolean(true);

        // Configure sender direction
        System.setProperty(BridgeConfiguration.DIRECTION_PROP, sendDirection.name());
        System.setProperty(BridgeConfiguration.STREAM_ID_PROP,
            String.valueOf(sendDirection.defaultStreamId()));

        @SuppressWarnings("try")
        final ShutdownSignalBarrier ignored = new ShutdownSignalBarrier(() -> running.set(false));
        try (BridgeSender sender = new BridgeSender(sendDirection))
        {
            sender.start();

            // Start receiver in background thread
            final Thread receiverThread = new Thread(() ->
            {
                System.setProperty(BridgeConfiguration.DIRECTION_PROP, receiveDirection.name());
                System.setProperty(BridgeConfiguration.STREAM_ID_PROP,
                    String.valueOf(receiveDirection.defaultStreamId()));

                try (BridgeReceiver receiver = new BridgeReceiver(receiveDirection))
                {
                    receiver.start();

                    while (running.get() && receiver.isRunning())
                    {
                        receiver.poll(10);
                        Thread.sleep(1);
                    }
                }
                catch (final Exception e)
                {
                    e.printStackTrace();
                }
            }, "bridge-receiver-" + receiveDirection);
            receiverThread.setDaemon(true);
            receiverThread.start();

            // Send messages
            for (int i = 0; i < messageCount && running.get() && sender.isRunning(); i++)
            {
                sender.publishHeartbeat();
                Thread.sleep(BridgeConfiguration.messageIntervalMs());
            }

            sender.awaitArchiveCatchup();
            System.out.println("[ClusterBridge] Sender complete");

            // Allow receiver to finish
            Thread.sleep(1000);
            running.set(false);
            receiverThread.join(5000);

            System.out.println("[ClusterBridge] Bidirectional mode complete");
        }
    }

    /**
     * Print usage information.
     */
    private static void printUsage()
    {
        System.out.println();
        System.out.println("Cluster Bridge Service - Inter-cluster messaging with replay support");
        System.out.println();
        System.out.println("Usage: ClusterBridgeMain --mode=<MODE> [options]");
        System.out.println();
        System.out.println("Modes:");
        System.out.println("  sender   - Run as bridge sender (single direction)");
        System.out.println("  receiver - Run as bridge receiver (single direction)");
        System.out.println("  ME       - Run as Matching Engine node (bidirectional)");
        System.out.println("  RMS      - Run as Risk Management node (bidirectional)");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --message-count=N  Number of messages to send (default: 100)");
        System.out.println();
        System.out.println("System Properties (override with -D):");
        System.out.println("  bridge.direction     - ME_TO_RMS or RMS_TO_ME");
        System.out.println("  bridge.stream.id     - Aeron stream ID");
        System.out.println("  bridge.channel       - Aeron channel URI");
        System.out.println("  bridge.archive.dir   - Archive storage directory");
        System.out.println("  bridge.checkpoint.dir - Checkpoint storage directory");
        System.out.println("  bridge.message.interval.ms - Interval between messages");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java ClusterBridgeMain --mode=sender");
        System.out.println("  java ClusterBridgeMain --mode=ME --message-count=1000");
        System.out.println("  java -Dbridge.direction=RMS_TO_ME ClusterBridgeMain --mode=sender");
    }
}
