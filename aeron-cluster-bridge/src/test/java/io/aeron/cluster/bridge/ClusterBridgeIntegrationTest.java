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
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for the inter-cluster bridge service.
 * <p>
 * Verifies:
 * <ul>
 *   <li>Bidirectional messaging (ME-to-RMS and RMS-to-ME)</li>
 *   <li>No duplicate messages</li>
 *   <li>Correct ordering (monotonically increasing sequences)</li>
 *   <li>Replay catch-up after simulated receiver restart</li>
 * </ul>
 */
class ClusterBridgeIntegrationTest
{
    @TempDir
    Path tempDir;

    private ArchivingMediaDriver mediaDriver;
    private Aeron aeron;
    private AeronArchive archive;

    @BeforeEach
    void setUp()
    {
        final String aeronDir = tempDir.resolve("aeron").toString();

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED)
            .aeronDirectoryName(aeronDir);

        // Archive.Context requires controlChannel (UDP) and replicationChannel.
        // We use localhost ports that don't conflict with other tests.
        final Archive.Context archiveCtx = new Archive.Context()
            .controlChannel("aeron:udp?endpoint=localhost:18010")
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .deleteArchiveOnStart(true)
            .archiveDir(tempDir.resolve("archive").toFile())
            .threadingMode(ArchiveThreadingMode.SHARED)
            .aeronDirectoryName(aeronDir);

        mediaDriver = ArchivingMediaDriver.launch(driverCtx, archiveCtx);

        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));

        // Connect to Archive via IPC (local control), not via the network control channel.
        archive = AeronArchive.connect(new AeronArchive.Context()
            .controlRequestChannel(mediaDriver.archive().context().localControlChannel())
            .controlRequestStreamId(mediaDriver.archive().context().localControlStreamId())
            .controlResponseChannel(mediaDriver.archive().context().localControlChannel())
            .aeron(aeron));
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(archive, aeron, mediaDriver);
    }

    @Test
    void shouldSendAndReceiveMeToRms() throws Exception
    {
        final int messageCount = 50;
        final Path checkpointDir = tempDir.resolve("ckpt1");
        java.nio.file.Files.createDirectories(checkpointDir);

        archive.startRecording(
            BridgeConfiguration.ME_TO_RMS_CHANNEL,
            BridgeConfiguration.ME_TO_RMS_STREAM_ID,
            SourceLocation.LOCAL);

        final List<Long> receivedSequences = Collections.synchronizedList(new ArrayList<>());

        try (BridgeSender sender = new BridgeSender(
                aeron,
                BridgeConfiguration.ME_TO_RMS_CHANNEL,
                BridgeConfiguration.ME_TO_RMS_STREAM_ID,
                BridgeConfiguration.DIRECTION_ME_TO_RMS);
            BridgeReceiver receiver = new BridgeReceiver(
                archive,
                BridgeConfiguration.ME_TO_RMS_CHANNEL,
                BridgeConfiguration.ME_TO_RMS_STREAM_ID,
                BridgeConfiguration.DIRECTION_ME_TO_RMS,
                checkpointDir,
                (seq, payload) -> receivedSequences.add(seq)))
        {
            receiver.initLiveOnly();
            awaitConnected(sender);
            sendMessages(sender, messageCount, "test");
            drainReceiver(receiver, receivedSequences, messageCount);
        }

        assertEquals(messageCount, receivedSequences.size(), "Expected all messages received");
        assertOrderedAndNoDuplicates(receivedSequences);
    }

    @Test
    void shouldSendAndReceiveRmsToMe() throws Exception
    {
        final int messageCount = 50;
        final Path checkpointDir = tempDir.resolve("ckpt2");
        java.nio.file.Files.createDirectories(checkpointDir);

        archive.startRecording(
            BridgeConfiguration.RMS_TO_ME_CHANNEL,
            BridgeConfiguration.RMS_TO_ME_STREAM_ID,
            SourceLocation.LOCAL);

        final List<Long> receivedSequences = Collections.synchronizedList(new ArrayList<>());

        try (BridgeSender sender = new BridgeSender(
                aeron,
                BridgeConfiguration.RMS_TO_ME_CHANNEL,
                BridgeConfiguration.RMS_TO_ME_STREAM_ID,
                BridgeConfiguration.DIRECTION_RMS_TO_ME);
            BridgeReceiver receiver = new BridgeReceiver(
                archive,
                BridgeConfiguration.RMS_TO_ME_CHANNEL,
                BridgeConfiguration.RMS_TO_ME_STREAM_ID,
                BridgeConfiguration.DIRECTION_RMS_TO_ME,
                checkpointDir,
                (seq, payload) -> receivedSequences.add(seq)))
        {
            receiver.initLiveOnly();
            awaitConnected(sender);
            sendMessages(sender, messageCount, "rms");
            drainReceiver(receiver, receivedSequences, messageCount);
        }

        assertEquals(messageCount, receivedSequences.size(), "Expected all RMS->ME messages");
        assertOrderedAndNoDuplicates(receivedSequences);
    }

    @Test
    void shouldCatchUpAfterRestart() throws Exception
    {
        final int firstBatch = 25;
        final int secondBatch = 25;
        final Path checkpointDir = tempDir.resolve("ckpt3");
        java.nio.file.Files.createDirectories(checkpointDir);

        archive.startRecording(
            BridgeConfiguration.ME_TO_RMS_CHANNEL,
            BridgeConfiguration.ME_TO_RMS_STREAM_ID,
            SourceLocation.LOCAL);

        final List<Long> firstBatchSeqs = Collections.synchronizedList(new ArrayList<>());

        try (BridgeSender sender = new BridgeSender(
                aeron,
                BridgeConfiguration.ME_TO_RMS_CHANNEL,
                BridgeConfiguration.ME_TO_RMS_STREAM_ID,
                BridgeConfiguration.DIRECTION_ME_TO_RMS))
        {
            // Phase 1: receiver processes firstBatch, then is stopped (simulating crash)
            try (BridgeReceiver receiver = new BridgeReceiver(
                    archive,
                    BridgeConfiguration.ME_TO_RMS_CHANNEL,
                    BridgeConfiguration.ME_TO_RMS_STREAM_ID,
                    BridgeConfiguration.DIRECTION_ME_TO_RMS,
                    checkpointDir,
                    (seq, data) -> firstBatchSeqs.add(seq)))
            {
                receiver.initLiveOnly();
                awaitConnected(sender);
                sendMessages(sender, firstBatch, "batch1");
                drainReceiver(receiver, firstBatchSeqs, firstBatch);
            }

            assertEquals(firstBatch, firstBatchSeqs.size(), "First batch fully received");

            // Phase 2: send more messages while receiver is down
            sendMessages(sender, secondBatch, "batch2");

            // Wait for archive to finish recording
            Thread.sleep(500);

            // Phase 3: restart receiver — it should catch up from archive
            final List<Long> replaySeqs = Collections.synchronizedList(new ArrayList<>());

            // Need a separate AeronArchive connection for the replay receiver
            // (different controlResponseStreamId to avoid conflicts)
            try (AeronArchive replayArchive = AeronArchive.connect(new AeronArchive.Context()
                    .controlRequestChannel(mediaDriver.archive().context().localControlChannel())
                    .controlRequestStreamId(mediaDriver.archive().context().localControlStreamId())
                    .controlResponseChannel(mediaDriver.archive().context().localControlChannel())
                    .controlResponseStreamId(AeronArchive.Configuration.controlResponseStreamId() + 10)
                    .aeron(aeron)))
            {
                try (BridgeReceiver replayReceiver = new BridgeReceiver(
                        replayArchive,
                        BridgeConfiguration.ME_TO_RMS_CHANNEL,
                        BridgeConfiguration.ME_TO_RMS_STREAM_ID,
                        BridgeConfiguration.DIRECTION_ME_TO_RMS,
                        checkpointDir,
                        (seq, data) -> replaySeqs.add(seq)))
                {
                    replayReceiver.init();
                    drainReceiver(replayReceiver, replaySeqs, secondBatch);
                }
            }

            assertTrue(replaySeqs.size() >= secondBatch,
                "Replay should catch up at least " + secondBatch + " messages, got: " + replaySeqs.size());

            for (final long seq : replaySeqs)
            {
                assertTrue(seq > firstBatch,
                    "Replay should only contain second batch seqs, got: " + seq);
            }

            assertOrderedAndNoDuplicates(replaySeqs);
        }
    }

    // ---- Helpers ----

    private static void awaitConnected(final BridgeSender sender)
    {
        final long deadline = System.currentTimeMillis() + 5000;
        while (!sender.isConnected() && System.currentTimeMillis() < deadline)
        {
            Thread.yield();
        }
        assertTrue(sender.isConnected(), "Sender should be connected");
    }

    private static void sendMessages(
        final BridgeSender sender, final int count, final String prefix)
    {
        final UnsafeBuffer payload = new UnsafeBuffer(
            BufferUtil.allocateDirectAligned(64, 64));

        for (int i = 0; i < count; i++)
        {
            final String msg = prefix + "-" + (i + 1);
            payload.putBytes(0, msg.getBytes());
            long seq = -1;
            while (seq < 0)
            {
                seq = sender.send(payload, 0, msg.getBytes().length);
                if (seq < 0)
                {
                    Thread.yield();
                }
            }
        }
    }

    private static void drainReceiver(
        final BridgeReceiver receiver, final List<Long> sequences, final int expectedCount)
    {
        final long deadline = System.currentTimeMillis() + 10000;
        while (sequences.size() < expectedCount && System.currentTimeMillis() < deadline)
        {
            receiver.poll();
            Thread.yield();
        }
    }

    private static void assertOrderedAndNoDuplicates(final List<Long> sequences)
    {
        long prev = 0;
        for (final long seq : sequences)
        {
            assertTrue(seq > prev,
                "Expected strictly increasing sequence: prev=" + prev + ", got=" + seq);
            prev = seq;
        }
    }
}
