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

import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import org.agrona.BufferUtil;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Main entry point for the inter-cluster bridge service.
 * <p>
 * Launches an embedded {@link ArchivingMediaDriver} (Media Driver + Archive),
 * configures archive recording for both directions, and runs senders and
 * receivers for bidirectional ME &lt;-&gt; RMS communication.
 * <p>
 * <b>Usage:</b> {@code ClusterBridge --mode sender|receiver|bridge}
 * <p>
 * <b>Design (SRP):</b> This class is the composition root — it wires together
 * all components but contains no business logic itself.
 * <p>
 * <b>Assumption:</b> A single process runs the bridge for both directions.
 * In production, you might split sender/receiver across processes using
 * {@code --mode sender} or {@code --mode receiver}.
 */
public final class ClusterBridge
{
    /**
     * Main method. Accepts {@code --mode sender|receiver|bridge}.
     *
     * @param args command-line arguments.
     * @throws Exception if the bridge fails to start.
     */
    @SuppressWarnings("try")
    public static void main(final String[] args) throws Exception
    {
        String mode = "bridge";
        for (int i = 0; i < args.length - 1; i++)
        {
            if ("--mode".equals(args[i]))
            {
                mode = args[i + 1];
            }
        }

        System.out.println("ClusterBridge starting in mode: " + mode);

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .spiesSimulateConnection(true);

        // Archive.Context: controlChannel and replicationChannel are required.
        // Assumption: port 8010 is the Aeron Archive default control port.
        final Archive.Context archiveCtx = new Archive.Context()
            .controlChannel(BridgeConfiguration.ARCHIVE_CONTROL_CHANNEL)
            .replicationChannel(BridgeConfiguration.ARCHIVE_REPLICATION_CHANNEL)
            .deleteArchiveOnStart(true);

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
            ArchivingMediaDriver mediaDriver = ArchivingMediaDriver.launch(
                driverCtx.terminationHook(barrier::signalAll), archiveCtx))
        {
            final AeronArchive.Context archiveClientCtx = new AeronArchive.Context()
                .aeronDirectoryName(driverCtx.aeronDirectoryName());

            try (AeronArchive archive = AeronArchive.connect(archiveClientCtx))
            {
                runMode(mode, archive, barrier);
            }
        }

        System.out.println("ClusterBridge shutdown complete.");
    }

    @SuppressWarnings("try")
    private static void runMode(
        final String mode,
        final AeronArchive archive,
        final ShutdownSignalBarrier barrier) throws IOException
    {
        final Aeron aeron = archive.context().aeron();
        final Path checkpointDir = Paths.get(BridgeConfiguration.CHECKPOINT_DIR);

        // Start recording both directions. SourceLocation.LOCAL means the
        // recording happens at the publication source (co-located archive).
        archive.startRecording(
            BridgeConfiguration.ME_TO_RMS_CHANNEL,
            BridgeConfiguration.ME_TO_RMS_STREAM_ID,
            SourceLocation.LOCAL);

        archive.startRecording(
            BridgeConfiguration.RMS_TO_ME_CHANNEL,
            BridgeConfiguration.RMS_TO_ME_STREAM_ID,
            SourceLocation.LOCAL);

        System.out.println("Archive recording started for both directions.");

        switch (mode)
        {
            case "sender":
                runSender(aeron);
                break;

            case "receiver":
                runReceiver(archive, checkpointDir);
                break;

            case "bridge":
            default:
                runBridge(aeron, archive, checkpointDir);
                break;
        }
    }

    private static void runSender(final Aeron aeron)
    {
        try (BridgeSender meToRms = new BridgeSender(
                aeron,
                BridgeConfiguration.ME_TO_RMS_CHANNEL,
                BridgeConfiguration.ME_TO_RMS_STREAM_ID,
                BridgeConfiguration.DIRECTION_ME_TO_RMS);
            BridgeSender rmsToMe = new BridgeSender(
                aeron,
                BridgeConfiguration.RMS_TO_ME_CHANNEL,
                BridgeConfiguration.RMS_TO_ME_STREAM_ID,
                BridgeConfiguration.DIRECTION_RMS_TO_ME))
        {
            sendDemoMessages(meToRms, "ME->RMS");
            sendDemoMessages(rmsToMe, "RMS->ME");
        }
    }

    private static void runReceiver(
        final AeronArchive archive,
        final Path checkpointDir)
    {
        try (BridgeReceiver meToRmsRx = new BridgeReceiver(
                archive,
                BridgeConfiguration.ME_TO_RMS_CHANNEL,
                BridgeConfiguration.ME_TO_RMS_STREAM_ID,
                BridgeConfiguration.DIRECTION_ME_TO_RMS,
                checkpointDir,
                (seq, payload) -> System.out.println("[ME->RMS] seq=" + seq));
            BridgeReceiver rmsToMeRx = new BridgeReceiver(
                archive,
                BridgeConfiguration.RMS_TO_ME_CHANNEL,
                BridgeConfiguration.RMS_TO_ME_STREAM_ID,
                BridgeConfiguration.DIRECTION_RMS_TO_ME,
                checkpointDir,
                (seq, payload) -> System.out.println("[RMS->ME] seq=" + seq)))
        {
            meToRmsRx.initLiveOnly();
            rmsToMeRx.initLiveOnly();

            while (true)
            {
                final int work = meToRmsRx.poll() + rmsToMeRx.poll();
                if (work == 0)
                {
                    Thread.yield();
                }
            }
        }
    }

    @SuppressWarnings("try")
    private static void runBridge(
        final Aeron aeron,
        final AeronArchive archive,
        final Path checkpointDir) throws IOException
    {
        try (BridgeSender meToRms = new BridgeSender(
                aeron,
                BridgeConfiguration.ME_TO_RMS_CHANNEL,
                BridgeConfiguration.ME_TO_RMS_STREAM_ID,
                BridgeConfiguration.DIRECTION_ME_TO_RMS);
            BridgeSender rmsToMe = new BridgeSender(
                aeron,
                BridgeConfiguration.RMS_TO_ME_CHANNEL,
                BridgeConfiguration.RMS_TO_ME_STREAM_ID,
                BridgeConfiguration.DIRECTION_RMS_TO_ME);
            BridgeReceiver meToRmsRx = new BridgeReceiver(
                archive,
                BridgeConfiguration.ME_TO_RMS_CHANNEL,
                BridgeConfiguration.ME_TO_RMS_STREAM_ID,
                BridgeConfiguration.DIRECTION_ME_TO_RMS,
                checkpointDir,
                (seq, payload) -> System.out.println("[ME->RMS RX] seq=" + seq));
            BridgeReceiver rmsToMeRx = new BridgeReceiver(
                archive,
                BridgeConfiguration.RMS_TO_ME_CHANNEL,
                BridgeConfiguration.RMS_TO_ME_STREAM_ID,
                BridgeConfiguration.DIRECTION_RMS_TO_ME,
                checkpointDir,
                (seq, payload) -> System.out.println("[RMS->ME RX] seq=" + seq)))
        {
            meToRmsRx.initLiveOnly();
            rmsToMeRx.initLiveOnly();

            sendDemoMessages(meToRms, "ME->RMS");
            sendDemoMessages(rmsToMe, "RMS->ME");

            // Poll receivers for a bounded time to drain all sent messages
            final long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline)
            {
                meToRmsRx.poll();
                rmsToMeRx.poll();
                Thread.yield();
            }

            System.out.println("[ME->RMS RX] received=" + meToRmsRx.receivedCount());
            System.out.println("[RMS->ME RX] received=" + rmsToMeRx.receivedCount());
        }
    }

    private static void sendDemoMessages(final BridgeSender sender, final String label)
    {
        final UnsafeBuffer payloadBuf = new UnsafeBuffer(BufferUtil.allocateDirectAligned(256, 64));
        final int count = BridgeConfiguration.MESSAGE_COUNT;

        while (!sender.isConnected())
        {
            Thread.yield();
        }

        for (int i = 0; i < count; i++)
        {
            final String msg = label + " msg " + (i + 1);
            final byte[] msgBytes = msg.getBytes();
            payloadBuf.putBytes(0, msgBytes);

            long seq = -1;
            while (seq < 0)
            {
                seq = sender.send(payloadBuf, 0, msgBytes.length);
                if (seq < 0)
                {
                    Thread.yield();
                }
            }
        }

        System.out.println("[" + label + "] Sent " + count + " messages.");
    }

    private ClusterBridge()
    {
    }
}
