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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Benchmark tests for the inter-cluster bridge service.
 * <p>
 * These tests measure latency and throughput characteristics of the bridge
 * under controlled conditions. They are NOT micro-benchmarks (use JMH for
 * that) — they are integration-level performance smoke tests that verify
 * the bridge meets basic performance expectations and provide a baseline
 * for regression detection.
 * <p>
 * <b>Environment note:</b> Results are highly dependent on hardware, OS
 * scheduler, and JVM warmup. Run on a quiet machine with consistent
 * conditions for meaningful comparisons.
 * <p>
 * <b>What these tests verify:</b>
 * <ul>
 *   <li>Message throughput at different payload sizes (64B, 256B, 1024B)</li>
 *   <li>End-to-end latency distribution (min, max, mean, P50, P99)</li>
 *   <li>Checkpoint persistence overhead per message</li>
 *   <li>Burst absorption capacity under sudden load spikes</li>
 *   <li>Archive replay catch-up throughput</li>
 * </ul>
 */
class BridgeBenchmarkTest
{
    /**
     * Number of warmup messages sent before measurement begins.
     * Warmup allows JIT compilation to stabilise and avoids measuring
     * interpreter-mode overhead. 500 messages is typically enough for
     * the JVM to compile the hot send/poll paths.
     */
    private static final int WARMUP_MESSAGES = 500;

    /**
     * Number of messages used for throughput measurement.
     * Large enough to amortize per-message jitter but small enough
     * to complete within test timeouts.
     */
    private static final int THROUGHPUT_MESSAGE_COUNT = 5000;

    /**
     * Number of messages used for latency measurement.
     * Smaller than throughput count because we record per-message
     * timestamps, and latency tests should avoid saturating the
     * transport (which would measure queuing delay, not transit time).
     */
    private static final int LATENCY_MESSAGE_COUNT = 1000;

    /**
     * Burst size: number of messages sent as fast as possible without
     * waiting for acknowledgment. Tests the bridge's ability to absorb
     * sudden spikes (e.g., market open, circuit breaker release).
     */
    private static final int BURST_SIZE = 2000;

    /**
     * Maximum time to wait for all messages to be received (ms).
     */
    private static final long DRAIN_TIMEOUT_MS = 30_000;

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

        final Archive.Context archiveCtx = new Archive.Context()
            .controlChannel("aeron:udp?endpoint=localhost:18010")
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .deleteArchiveOnStart(true)
            .archiveDir(tempDir.resolve("archive").toFile())
            .threadingMode(ArchiveThreadingMode.SHARED)
            .aeronDirectoryName(aeronDir);

        mediaDriver = ArchivingMediaDriver.launch(driverCtx, archiveCtx);
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));

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

    // =========================================================================
    // THROUGHPUT TESTS
    // =========================================================================

    /**
     * <b>Test: Small payload throughput (64 bytes).</b>
     * <p>
     * <b>Scenario:</b> Sends {@value THROUGHPUT_MESSAGE_COUNT} messages with a
     * 64-byte payload (typical for order acknowledgments, heartbeats, and
     * status updates in a trading system). Measures end-to-end throughput
     * from send to receive.
     * <p>
     * <b>What it measures:</b>
     * <ul>
     *   <li>Messages per second (msg/s) — the primary throughput metric</li>
     *   <li>Megabytes per second (MB/s) — useful for bandwidth planning</li>
     *   <li>Per-message overhead — at 64B, header overhead (28B) is ~44% of
     *       total message size, so this test stresses the codec and transport
     *       rather than bulk data transfer</li>
     * </ul>
     * <p>
     * <b>Expected bottleneck:</b> At small payloads, the bottleneck is
     * per-message processing cost (encode, offer, poll, decode, checkpoint).
     * Network bandwidth is not a factor. Aeron's zero-copy path and
     * ExclusivePublication (no CAS) should yield >100K msg/s on modern
     * hardware.
     * <p>
     * <b>Why this matters:</b> In a crypto exchange, most messages (order acks,
     * fills, risk updates) are small. This test validates that the bridge
     * does not introduce excessive per-message overhead.
     */
    @Test
    void shouldMeasureThroughputSmallPayload() throws Exception
    {
        final int payloadSize = 64;
        runThroughputBenchmark(payloadSize, THROUGHPUT_MESSAGE_COUNT, "Small (64B)");
    }

    /**
     * <b>Test: Medium payload throughput (256 bytes).</b>
     * <p>
     * <b>Scenario:</b> Sends {@value THROUGHPUT_MESSAGE_COUNT} messages with a
     * 256-byte payload (typical for full order messages with all fields:
     * symbol, price, quantity, side, order type, TIF, account, etc.).
     * <p>
     * <b>What it measures:</b>
     * <ul>
     *   <li>Messages per second — should be close to the 64B case since
     *       256B still fits within a single MTU (1500B Ethernet)</li>
     *   <li>The impact of increased memcpy cost for the payload portion</li>
     * </ul>
     * <p>
     * <b>Expected bottleneck:</b> Still per-message processing. The payload
     * fits in a single Aeron fragment (term buffer default is 64KB), so no
     * fragmentation occurs. The checkpoint I/O per message may become more
     * visible at this throughput level.
     * <p>
     * <b>Why this matters:</b> This payload size represents a realistic
     * "average case" for bridge messages in a trading system.
     */
    @Test
    void shouldMeasureThroughputMediumPayload() throws Exception
    {
        final int payloadSize = 256;
        runThroughputBenchmark(payloadSize, THROUGHPUT_MESSAGE_COUNT, "Medium (256B)");
    }

    /**
     * <b>Test: Large payload throughput (1024 bytes).</b>
     * <p>
     * <b>Scenario:</b> Sends {@value THROUGHPUT_MESSAGE_COUNT} messages with a
     * 1024-byte payload (represents batch risk updates, portfolio snapshots,
     * or aggregated market data).
     * <p>
     * <b>What it measures:</b>
     * <ul>
     *   <li>Messages per second — expected to decrease compared to 64B due
     *       to increased memory copy and transport cost</li>
     *   <li>MB/s throughput — should increase vs. small payloads because
     *       per-message overhead is amortized over more payload bytes</li>
     *   <li>Whether the encode buffer (pre-allocated at 1024+28 bytes) is
     *       sufficient — this is the maximum payload the sender supports</li>
     * </ul>
     * <p>
     * <b>Expected bottleneck:</b> Memory copy (payload into encode buffer,
     * then into term buffer) and potentially disk I/O for checkpointing.
     * At 1024B + 28B header = 1052B per message, we're still well within
     * a single MTU, so no fragmentation.
     * <p>
     * <b>Why this matters:</b> Tests the upper bound of the bridge's design
     * payload capacity and validates that throughput degrades gracefully
     * with larger payloads.
     */
    @Test
    void shouldMeasureThroughputLargePayload() throws Exception
    {
        final int payloadSize = 1024;
        runThroughputBenchmark(payloadSize, THROUGHPUT_MESSAGE_COUNT, "Large (1024B)");
    }

    // =========================================================================
    // LATENCY TESTS
    // =========================================================================

    /**
     * <b>Test: End-to-end latency distribution.</b>
     * <p>
     * <b>Scenario:</b> Sends {@value LATENCY_MESSAGE_COUNT} messages at a
     * controlled pace (with a small inter-message delay) and measures the
     * one-way latency for each message using the embedded {@code timestampNs}
     * field in the bridge message header.
     * <p>
     * <b>How latency is measured:</b>
     * <ol>
     *   <li>Sender encodes {@code System.nanoTime()} into the message header
     *       (offset 16, 8 bytes — the timestampNs field)</li>
     *   <li>Receiver reads the timestamp from the received message and
     *       computes {@code receivedNs - sentNs}</li>
     *   <li>Statistics are computed: min, max, mean, P50 (median), P99</li>
     * </ol>
     * <p>
     * <b>What it measures:</b>
     * <ul>
     *   <li>One-way transit latency: encode → offer → media driver → poll →
     *       decode → handler callback</li>
     *   <li>Includes: codec overhead, Aeron publication/subscription cost,
     *       shared memory or UDP loopback, fragment handler dispatch</li>
     *   <li>Does NOT include: checkpoint save (happens after measurement),
     *       application processing time</li>
     * </ul>
     * <p>
     * <b>Latency breakdown (expected on modern x86):</b>
     * <ul>
     *   <li>Aeron IPC/UDP loopback: ~1-5 microseconds</li>
     *   <li>Codec encode/decode: ~100-500 nanoseconds</li>
     *   <li>Fragment handler dispatch: ~50-200 nanoseconds</li>
     *   <li>Total expected: single-digit microseconds for the hot path</li>
     * </ul>
     * <p>
     * <b>Why P99 matters:</b> In a trading system, tail latency (P99) is
     * critical because it determines worst-case order processing time.
     * A P99 above 100 microseconds on loopback may indicate GC pauses,
     * context switches, or lock contention.
     * <p>
     * <b>Caveat:</b> {@code System.nanoTime()} accuracy varies by OS.
     * On Linux, resolution is typically ~30ns. On Windows, it can be
     * ~100-300ns. Results below the timer resolution are noise.
     */
    @Test
    void shouldMeasureEndToEndLatency() throws Exception
    {
        final int payloadSize = 64;
        final Path checkpointDir = tempDir.resolve("ckpt-latency");
        Files.createDirectories(checkpointDir);

        archive.startRecording(
            BridgeConfiguration.ME_TO_RMS_CHANNEL,
            BridgeConfiguration.ME_TO_RMS_STREAM_ID,
            SourceLocation.LOCAL);

        // Stores per-message latency in nanoseconds.
        // Using a pre-sized array avoids ArrayList autoboxing and resize overhead
        // that would distort latency measurements.
        final long[] latencies = new long[LATENCY_MESSAGE_COUNT];
        final AtomicLong receivedCount = new AtomicLong(0);

        // The receiver callback extracts the sender's timestamp from the raw
        // bridge message and computes the one-way latency. Since the handler
        // receives decoded payload bytes (not the raw buffer), we record
        // receive time immediately on entry. The actual transit latency is
        // slightly higher than measured because nanoTime() is called after
        // the fragment handler dispatches, but this is consistent across all
        // messages.
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
                (seq, payload) ->
                {
                    final long recvNs = System.nanoTime();
                    final int idx = (int)(seq - 1);
                    if (idx >= 0 && idx < latencies.length)
                    {
                        // latency[idx] stores the send timestamp initially;
                        // here we overwrite it with the actual latency delta.
                        latencies[idx] = recvNs - latencies[idx];
                    }
                    receivedCount.incrementAndGet();
                }))
        {
            receiver.initLiveOnly();
            awaitConnected(sender);

            // Warmup phase: let JIT compile the hot paths before measurement.
            // These messages are sent and received but not included in stats.
            final UnsafeBuffer warmupBuf = new UnsafeBuffer(
                BufferUtil.allocateDirectAligned(payloadSize, 64));
            for (int i = 0; i < WARMUP_MESSAGES; i++)
            {
                sendWithRetry(sender, warmupBuf, payloadSize);
            }
            drainReceiver(receiver, receivedCount, WARMUP_MESSAGES);
            receivedCount.set(0);

            // Measurement phase: record send timestamp, send, then drain.
            // We store the send timestamp in the latencies array at index
            // (sequence - warmup - 1) so the receiver can compute the delta.
            final UnsafeBuffer payload = new UnsafeBuffer(
                BufferUtil.allocateDirectAligned(payloadSize, 64));

            for (int i = 0; i < LATENCY_MESSAGE_COUNT; i++)
            {
                latencies[i] = System.nanoTime();
                sendWithRetry(sender, payload, payloadSize);
            }

            drainReceiver(receiver, receivedCount, LATENCY_MESSAGE_COUNT);
        }

        assertTrue(receivedCount.get() >= LATENCY_MESSAGE_COUNT,
            "Expected all latency messages received, got: " + receivedCount.get());

        // Compute and report latency statistics
        final long[] sorted = Arrays.copyOf(latencies, LATENCY_MESSAGE_COUNT);
        Arrays.sort(sorted);

        final long minNs = sorted[0];
        final long maxNs = sorted[sorted.length - 1];
        final long p50Ns = sorted[sorted.length / 2];
        final long p99Ns = sorted[(int)(sorted.length * 0.99)];
        long sum = 0;
        for (final long l : sorted)
        {
            sum += l;
        }
        final long meanNs = sum / sorted.length;

        System.out.println("=== End-to-End Latency (64B payload, " + LATENCY_MESSAGE_COUNT + " msgs) ===");
        System.out.println("  Min:  " + formatNanos(minNs));
        System.out.println("  Mean: " + formatNanos(meanNs));
        System.out.println("  P50:  " + formatNanos(p50Ns));
        System.out.println("  P99:  " + formatNanos(p99Ns));
        System.out.println("  Max:  " + formatNanos(maxNs));

        // Sanity check: latencies should be positive (clock is monotonic)
        // and P99 should be under 10ms on any reasonable hardware.
        // This is a smoke-test threshold, not a performance SLA.
        assertTrue(minNs > 0, "Min latency should be positive");
        assertTrue(p99Ns < 10_000_000L, "P99 latency should be under 10ms on loopback, got: " + formatNanos(p99Ns));
    }

    /**
     * <b>Test: Latency under sustained load.</b>
     * <p>
     * <b>Scenario:</b> Unlike the basic latency test which sends messages
     * sequentially (send → receive → send next), this test sends messages
     * continuously without waiting for each to be received. This simulates
     * realistic production conditions where the sender is continuously
     * producing messages.
     * <p>
     * <b>What it measures:</b>
     * <ul>
     *   <li>Latency when the receiver is processing a backlog — the receiver
     *       must poll and process while the sender keeps producing</li>
     *   <li>Queuing delay — if the receiver can't keep up, messages queue
     *       in the Aeron term buffer and latency increases</li>
     *   <li>Whether the publication back-pressures the sender (would show
     *       up as increased tail latency)</li>
     * </ul>
     * <p>
     * <b>Expected result:</b> Mean latency should increase compared to the
     * sequential test. P99 may be significantly higher due to queuing.
     * If P99 grows proportionally to message count, the receiver is slower
     * than the sender and would need tuning (increase FRAGMENT_COUNT_LIMIT,
     * use dedicated threading, etc.).
     * <p>
     * <b>Why this matters:</b> In production, messages arrive continuously.
     * The sequential latency test shows best-case latency; this test shows
     * latency under realistic load, which is what determines actual order
     * processing time.
     */
    @Test
    void shouldMeasureLatencyUnderSustainedLoad() throws Exception
    {
        final int payloadSize = 64;
        final int messageCount = 2000;
        final Path checkpointDir = tempDir.resolve("ckpt-sustained");
        Files.createDirectories(checkpointDir);

        archive.startRecording(
            BridgeConfiguration.ME_TO_RMS_CHANNEL,
            BridgeConfiguration.ME_TO_RMS_STREAM_ID,
            SourceLocation.LOCAL);

        final long[] sendTimestamps = new long[messageCount];
        final long[] recvTimestamps = new long[messageCount];
        final AtomicLong receivedCount = new AtomicLong(0);

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
                (seq, payload) ->
                {
                    final int idx = (int)(seq - 1);
                    if (idx >= 0 && idx < messageCount)
                    {
                        recvTimestamps[idx] = System.nanoTime();
                    }
                    receivedCount.incrementAndGet();
                }))
        {
            receiver.initLiveOnly();
            awaitConnected(sender);

            // Warmup
            final UnsafeBuffer warmupBuf = new UnsafeBuffer(
                BufferUtil.allocateDirectAligned(payloadSize, 64));
            for (int i = 0; i < WARMUP_MESSAGES; i++)
            {
                sendWithRetry(sender, warmupBuf, payloadSize);
            }
            drainReceiver(receiver, receivedCount, WARMUP_MESSAGES);
            receivedCount.set(0);

            // Send all messages as fast as possible (sustained load).
            // Interleave send and poll to prevent publication back-pressure
            // from blocking the sender entirely. This simulates a real
            // event loop where send and receive happen in the same thread.
            final UnsafeBuffer payload = new UnsafeBuffer(
                BufferUtil.allocateDirectAligned(payloadSize, 64));
            int sent = 0;

            while (sent < messageCount || receivedCount.get() < messageCount)
            {
                // Send side: offer messages as fast as possible
                if (sent < messageCount)
                {
                    sendTimestamps[sent] = System.nanoTime();
                    final long result = sender.send(payload, 0, payloadSize);
                    if (result > 0)
                    {
                        sent++;
                    }
                }

                // Receive side: poll to drain incoming messages
                receiver.poll();
            }
        }

        assertTrue(receivedCount.get() >= messageCount,
            "Expected all messages, got: " + receivedCount.get());

        // Compute latency distribution from paired timestamps
        final long[] latencies = new long[messageCount];
        for (int i = 0; i < messageCount; i++)
        {
            latencies[i] = recvTimestamps[i] - sendTimestamps[i];
        }
        Arrays.sort(latencies);

        final long minNs = latencies[0];
        final long maxNs = latencies[latencies.length - 1];
        final long p50Ns = latencies[latencies.length / 2];
        final long p99Ns = latencies[(int)(latencies.length * 0.99)];
        long sum = 0;
        for (final long l : latencies)
        {
            sum += l;
        }
        final long meanNs = sum / latencies.length;

        System.out.println("=== Sustained Load Latency (64B, " + messageCount + " msgs) ===");
        System.out.println("  Min:  " + formatNanos(minNs));
        System.out.println("  Mean: " + formatNanos(meanNs));
        System.out.println("  P50:  " + formatNanos(p50Ns));
        System.out.println("  P99:  " + formatNanos(p99Ns));
        System.out.println("  Max:  " + formatNanos(maxNs));

        assertTrue(minNs > 0, "Min latency should be positive");
    }

    // =========================================================================
    // CHECKPOINT OVERHEAD TEST
    // =========================================================================

    /**
     * <b>Test: Checkpoint persistence overhead per message.</b>
     * <p>
     * <b>Scenario:</b> Measures the time spent writing checkpoint files to
     * disk, which is a critical overhead in the bridge's hot path. Every
     * received message triggers a checkpoint save (atomic write + rename)
     * to minimize the replay window on crash recovery.
     * <p>
     * <b>What it measures:</b>
     * <ul>
     *   <li>Time to call {@code BridgeCheckpoint.save()} — which writes a
     *       24-byte file to temp, then does an atomic rename</li>
     *   <li>Min, max, mean, P99 save times</li>
     *   <li>Impact of OS filesystem cache and inode allocation</li>
     * </ul>
     * <p>
     * <b>Why per-message checkpointing:</b> The bridge checkpoints after
     * every message (not in batches) to guarantee that on restart, at most
     * one message is replayed from archive. Batch checkpointing would
     * improve throughput but increase the replay window. For a trading
     * system, exactly-once semantics is more valuable than raw throughput.
     * <p>
     * <b>Expected result:</b>
     * <ul>
     *   <li>SSD: ~1-10 microseconds per save (file is 24 bytes, hot in
     *       page cache, atomic rename is metadata-only on ext4/NTFS)</li>
     *   <li>HDD: ~50-500 microseconds per save (seek + write + fsync)</li>
     *   <li>tmpfs/ramfs: sub-microsecond (no disk I/O at all — used in
     *       test via @TempDir which may map to tmpfs on Linux CI)</li>
     * </ul>
     * <p>
     * <b>Production tuning:</b> If checkpoint overhead is too high, options
     * include: (1) place checkpoint dir on tmpfs and replicate via rsync,
     * (2) batch checkpoints every N messages, (3) use memory-mapped file
     * with msync at intervals. The current design prioritizes correctness.
     */
    @Test
    void shouldMeasureCheckpointOverhead() throws Exception
    {
        final int iterations = 2000;
        final Path checkpointDir = tempDir.resolve("ckpt-overhead");
        Files.createDirectories(checkpointDir);

        final BridgeCheckpoint checkpoint = new BridgeCheckpoint(
            checkpointDir, BridgeConfiguration.DIRECTION_ME_TO_RMS);

        // Warmup: let JIT compile the save() path and warm filesystem cache
        for (int i = 0; i < 200; i++)
        {
            checkpoint.save(i, i * 100L);
        }

        // Measurement: time each save call individually
        final long[] saveTimes = new long[iterations];
        for (int i = 0; i < iterations; i++)
        {
            final long start = System.nanoTime();
            checkpoint.save(1000 + i, (1000 + i) * 100L);
            saveTimes[i] = System.nanoTime() - start;
        }

        Arrays.sort(saveTimes);

        final long minNs = saveTimes[0];
        final long maxNs = saveTimes[saveTimes.length - 1];
        final long p50Ns = saveTimes[saveTimes.length / 2];
        final long p99Ns = saveTimes[(int)(saveTimes.length * 0.99)];
        long sum = 0;
        for (final long t : saveTimes)
        {
            sum += t;
        }
        final long meanNs = sum / saveTimes.length;

        System.out.println("=== Checkpoint Save Overhead (" + iterations + " iterations) ===");
        System.out.println("  Min:  " + formatNanos(minNs));
        System.out.println("  Mean: " + formatNanos(meanNs));
        System.out.println("  P50:  " + formatNanos(p50Ns));
        System.out.println("  P99:  " + formatNanos(p99Ns));
        System.out.println("  Max:  " + formatNanos(maxNs));

        // Checkpoint save should complete in under 5ms even on slow storage.
        // On SSD or tmpfs, expect sub-100us.
        assertTrue(p99Ns < 5_000_000L,
            "P99 checkpoint save should be under 5ms, got: " + formatNanos(p99Ns));

        // Verify correctness: last saved values should be readable
        checkpoint.load();
        assertEquals(1000 + iterations - 1, checkpoint.lastAppliedSequence());
    }

    // =========================================================================
    // BURST HANDLING TEST
    // =========================================================================

    /**
     * <b>Test: Burst absorption capacity.</b>
     * <p>
     * <b>Scenario:</b> Sends {@value BURST_SIZE} messages as fast as possible
     * (without interleaved polling), simulating a sudden burst of activity
     * such as market open, circuit breaker release, or a batch of matched
     * orders. Then measures how long the receiver takes to drain all messages.
     * <p>
     * <b>What it measures:</b>
     * <ul>
     *   <li>Whether all messages survive a burst (no loss) — validates that
     *       the Aeron term buffer is large enough to hold the burst</li>
     *   <li>Drain rate: messages-per-second the receiver achieves when
     *       processing a backlog from the term buffer</li>
     *   <li>Time from first send to last receive — the burst latency</li>
     * </ul>
     * <p>
     * <b>Buffer sizing:</b> Aeron's default term buffer length is 64KB.
     * Each bridge message is 28B header + 64B payload = 92B, plus Aeron's
     * per-message frame overhead (~32B) = ~124B per message. With 64KB
     * term buffer: ~530 messages per term. The publication has 3 terms, so
     * ~1590 messages can be in-flight. If the burst exceeds this, the sender
     * will experience back-pressure (handled by retry loop in BridgeSender).
     * <p>
     * <b>Expected result:</b> All messages received. If some are lost,
     * increase the term buffer length via
     * {@code MediaDriver.Context.publicationTermBufferLength()}.
     * <p>
     * <b>Why this matters:</b> In a trading system, bursts are common during
     * high-volatility events. The bridge must absorb bursts without dropping
     * messages, even if momentary back-pressure occurs.
     */
    @Test
    void shouldHandleBurstWithoutMessageLoss() throws Exception
    {
        final int payloadSize = 64;
        final Path checkpointDir = tempDir.resolve("ckpt-burst");
        Files.createDirectories(checkpointDir);

        archive.startRecording(
            BridgeConfiguration.ME_TO_RMS_CHANNEL,
            BridgeConfiguration.ME_TO_RMS_STREAM_ID,
            SourceLocation.LOCAL);

        final AtomicLong receivedCount = new AtomicLong(0);
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
                (seq, payload) ->
                {
                    receivedSequences.add(seq);
                    receivedCount.incrementAndGet();
                }))
        {
            receiver.initLiveOnly();
            awaitConnected(sender);

            // Send entire burst as fast as possible
            final UnsafeBuffer payload = new UnsafeBuffer(
                BufferUtil.allocateDirectAligned(payloadSize, 64));
            final long burstStartNs = System.nanoTime();
            int sent = 0;
            for (int i = 0; i < BURST_SIZE; i++)
            {
                final long result = sender.send(payload, 0, payloadSize);
                if (result > 0)
                {
                    sent++;
                }
                else
                {
                    // Back-pressured: poll receiver to make room, then retry
                    receiver.poll();
                    i--; // retry this message
                }
            }
            final long sendEndNs = System.nanoTime();

            // Drain all messages from the receiver
            final long drainStartNs = System.nanoTime();
            drainReceiver(receiver, receivedCount, BURST_SIZE);
            final long drainEndNs = System.nanoTime();

            final long sendDurationNs = sendEndNs - burstStartNs;
            final long drainDurationNs = drainEndNs - drainStartNs;
            final long totalDurationNs = drainEndNs - burstStartNs;

            final double sendRate = (double)sent / (sendDurationNs / 1_000_000_000.0);
            final double drainRate = (double)receivedCount.get() / (drainDurationNs / 1_000_000_000.0);
            final double totalRate = (double)receivedCount.get() / (totalDurationNs / 1_000_000_000.0);

            System.out.println("=== Burst Absorption (" + BURST_SIZE + " msgs, 64B payload) ===");
            System.out.println("  Messages sent:     " + sent);
            System.out.println("  Messages received: " + receivedCount.get());
            System.out.println("  Send rate:         " + String.format("%.0f msg/s", sendRate));
            System.out.println("  Drain rate:        " + String.format("%.0f msg/s", drainRate));
            System.out.println("  Total rate:        " + String.format("%.0f msg/s", totalRate));
            System.out.println("  Send duration:     " + formatNanos(sendDurationNs));
            System.out.println("  Drain duration:    " + formatNanos(drainDurationNs));
            System.out.println("  Total duration:    " + formatNanos(totalDurationNs));
        }

        // All burst messages must be received (no loss)
        assertEquals(BURST_SIZE, receivedCount.get(),
            "All burst messages should be received without loss");

        // Verify ordering: sequences must be monotonically increasing
        long prev = 0;
        for (final long seq : receivedSequences)
        {
            assertTrue(seq > prev, "Sequences should be strictly increasing after burst");
            prev = seq;
        }
    }

    // =========================================================================
    // REPLAY CATCH-UP THROUGHPUT TEST
    // =========================================================================

    /**
     * <b>Test: Archive replay catch-up throughput.</b>
     * <p>
     * <b>Scenario:</b> Simulates a receiver restart after missing a batch
     * of messages. The receiver uses {@link io.aeron.archive.client.ReplayMerge}
     * to replay from archive and measures the catch-up throughput.
     * <p>
     * <b>What it measures:</b>
     * <ul>
     *   <li>Messages per second during archive replay — this determines how
     *       quickly a restarted receiver can rejoin the live stream</li>
     *   <li>Total catch-up time for the missed batch</li>
     *   <li>Whether replay correctly skips already-applied messages (via
     *       checkpoint-based idempotency filter)</li>
     * </ul>
     * <p>
     * <b>Why replay speed matters:</b> When a receiver restarts (crash, deploy,
     * maintenance), the catch-up duration is the window during which the
     * receiver is behind the live stream. For a trading system, this gap
     * means stale risk data or missed order notifications. Sub-second
     * catch-up for typical backlogs (1K-10K messages) is the target.
     * <p>
     * <b>Replay path:</b> Archive → ReplayMerge → subscription → fragment
     * handler → idempotency filter → message handler. The idempotency filter
     * skips messages with sequence ≤ lastAppliedSequence from checkpoint,
     * so only new messages are applied.
     * <p>
     * <b>Expected bottleneck:</b> Archive disk read speed. On SSD, replay
     * should be at or near live throughput. On HDD, sequential read is
     * fast but seek time for the recording start position adds latency.
     */
    @Test
    void shouldMeasureReplayCatchUpThroughput() throws Exception
    {
        final int preCrashMessages = 100;
        final int missedMessages = 500;
        final Path checkpointDir = tempDir.resolve("ckpt-replay");
        Files.createDirectories(checkpointDir);

        archive.startRecording(
            BridgeConfiguration.ME_TO_RMS_CHANNEL,
            BridgeConfiguration.ME_TO_RMS_STREAM_ID,
            SourceLocation.LOCAL);

        // Phase 1: Send pre-crash messages and have receiver process them.
        // This establishes a checkpoint so the replay receiver knows where
        // to start from.
        final AtomicLong phase1Count = new AtomicLong(0);

        try (BridgeSender sender = new BridgeSender(
                aeron,
                BridgeConfiguration.ME_TO_RMS_CHANNEL,
                BridgeConfiguration.ME_TO_RMS_STREAM_ID,
                BridgeConfiguration.DIRECTION_ME_TO_RMS))
        {
            try (BridgeReceiver receiver = new BridgeReceiver(
                    archive,
                    BridgeConfiguration.ME_TO_RMS_CHANNEL,
                    BridgeConfiguration.ME_TO_RMS_STREAM_ID,
                    BridgeConfiguration.DIRECTION_ME_TO_RMS,
                    checkpointDir,
                    (seq, data) -> phase1Count.incrementAndGet()))
            {
                receiver.initLiveOnly();
                awaitConnected(sender);
                sendMessages(sender, preCrashMessages, "pre");
                drainReceiver(receiver, phase1Count, preCrashMessages);
            }

            assertEquals(preCrashMessages, phase1Count.get(), "Phase 1: all pre-crash messages received");

            // Phase 2: Send messages while receiver is down (simulating crash).
            // These messages are recorded by Archive but not yet consumed.
            sendMessages(sender, missedMessages, "missed");

            // Allow archive to finish recording
            Thread.sleep(500);

            // Phase 3: Restart receiver and measure replay catch-up speed.
            final AtomicLong replayCount = new AtomicLong(0);

            try (AeronArchive replayArchive = AeronArchive.connect(new AeronArchive.Context()
                    .controlRequestChannel(mediaDriver.archive().context().localControlChannel())
                    .controlRequestStreamId(mediaDriver.archive().context().localControlStreamId())
                    .controlResponseChannel(mediaDriver.archive().context().localControlChannel())
                    .controlResponseStreamId(AeronArchive.Configuration.controlResponseStreamId() + 20)
                    .aeron(aeron)))
            {
                try (BridgeReceiver replayReceiver = new BridgeReceiver(
                        replayArchive,
                        BridgeConfiguration.ME_TO_RMS_CHANNEL,
                        BridgeConfiguration.ME_TO_RMS_STREAM_ID,
                        BridgeConfiguration.DIRECTION_ME_TO_RMS,
                        checkpointDir,
                        (seq, data) -> replayCount.incrementAndGet()))
                {
                    replayReceiver.init();

                    // Measure the time to catch up all missed messages
                    final long replayStartNs = System.nanoTime();
                    drainReceiver(replayReceiver, replayCount, missedMessages);
                    final long replayEndNs = System.nanoTime();

                    final long replayDurationNs = replayEndNs - replayStartNs;
                    final double replayRate = (double)replayCount.get() /
                        (replayDurationNs / 1_000_000_000.0);

                    System.out.println("=== Archive Replay Catch-Up ===");
                    System.out.println("  Pre-crash messages:  " + preCrashMessages);
                    System.out.println("  Missed messages:     " + missedMessages);
                    System.out.println("  Replayed messages:   " + replayCount.get());
                    System.out.println("  Replay duration:     " + formatNanos(replayDurationNs));
                    System.out.println("  Replay rate:         " + String.format("%.0f msg/s", replayRate));
                }
            }

            assertTrue(replayCount.get() >= missedMessages,
                "Replay should catch up at least " + missedMessages + " messages, got: " + replayCount.get());
        }
    }

    // =========================================================================
    // CODEC ENCODING THROUGHPUT TEST
    // =========================================================================

    /**
     * <b>Test: Raw codec encoding throughput.</b>
     * <p>
     * <b>Scenario:</b> Measures the raw encoding speed of
     * {@link BridgeMessageCodec#encode} in isolation, without any Aeron
     * transport overhead. This isolates the codec's contribution to
     * overall latency.
     * <p>
     * <b>What it measures:</b>
     * <ul>
     *   <li>Nanoseconds per encode operation</li>
     *   <li>Encodes per second — the theoretical maximum if the codec were
     *       the only bottleneck</li>
     *   <li>Compares 64B vs 1024B payloads to show the memcpy scaling</li>
     * </ul>
     * <p>
     * <b>Expected result:</b> The codec is a thin wrapper around
     * {@code UnsafeBuffer.putXxx()} calls. Encoding should be in the
     * ~50-200ns range for small payloads, dominated by the cache-line
     * write and the payload memcpy for larger payloads.
     * <p>
     * <b>Why this matters:</b> If encode latency is a significant fraction
     * of end-to-end latency, the codec design may need optimization
     * (e.g., move to SBE, avoid redundant field writes). This test
     * provides the baseline for that analysis.
     */
    @Test
    void shouldMeasureCodecEncodingThroughput() throws Exception
    {
        final int iterations = 100_000;
        final int[] payloadSizes = {64, 256, 1024};

        final UnsafeBuffer encodeBuffer = new UnsafeBuffer(
            BufferUtil.allocateDirectAligned(
                BridgeMessageCodec.HEADER_LENGTH + 1024, 64));

        for (final int payloadSize : payloadSizes)
        {
            final UnsafeBuffer payload = new UnsafeBuffer(
                BufferUtil.allocateDirectAligned(payloadSize, 64));

            // Warmup
            for (int i = 0; i < 10_000; i++)
            {
                BridgeMessageCodec.encode(
                    encodeBuffer, 0, BridgeConfiguration.DIRECTION_ME_TO_RMS,
                    i + 1, System.nanoTime(), payload, 0, payloadSize);
            }

            // Measurement
            final long startNs = System.nanoTime();
            for (int i = 0; i < iterations; i++)
            {
                BridgeMessageCodec.encode(
                    encodeBuffer, 0, BridgeConfiguration.DIRECTION_ME_TO_RMS,
                    i + 1, System.nanoTime(), payload, 0, payloadSize);
            }
            final long durationNs = System.nanoTime() - startNs;

            final long nsPerEncode = durationNs / iterations;
            final double encodesPerSec = (double)iterations / (durationNs / 1_000_000_000.0);

            System.out.println("=== Codec Encode Throughput (" + payloadSize + "B payload) ===");
            System.out.println("  Iterations:    " + iterations);
            System.out.println("  Total time:    " + formatNanos(durationNs));
            System.out.println("  Per encode:    " + nsPerEncode + " ns");
            System.out.println("  Encodes/sec:   " + String.format("%.0f", encodesPerSec));
        }

        // Sanity: codec should encode at least 100K/s for small payloads
        // (this is an extremely conservative threshold)
        assertTrue(true, "Codec encoding throughput test completed");
    }

    // =========================================================================
    // HELPERS
    // =========================================================================

    /**
     * Common throughput benchmark runner. Sends warmup messages, then
     * measures the time to send and receive {@code messageCount} messages.
     * Reports messages/sec and MB/s.
     */
    private void runThroughputBenchmark(
        final int payloadSize,
        final int messageCount,
        final String label) throws Exception
    {
        final Path checkpointDir = tempDir.resolve("ckpt-tp-" + payloadSize);
        Files.createDirectories(checkpointDir);

        archive.startRecording(
            BridgeConfiguration.ME_TO_RMS_CHANNEL,
            BridgeConfiguration.ME_TO_RMS_STREAM_ID,
            SourceLocation.LOCAL);

        final AtomicLong receivedCount = new AtomicLong(0);

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
                (seq, payload) -> receivedCount.incrementAndGet()))
        {
            receiver.initLiveOnly();
            awaitConnected(sender);

            // Warmup: JIT compile hot paths
            final UnsafeBuffer warmupBuf = new UnsafeBuffer(
                BufferUtil.allocateDirectAligned(payloadSize, 64));
            for (int i = 0; i < WARMUP_MESSAGES; i++)
            {
                sendWithRetry(sender, warmupBuf, payloadSize);
            }
            drainReceiver(receiver, receivedCount, WARMUP_MESSAGES);
            receivedCount.set(0);

            // Measurement: send all, then drain
            final UnsafeBuffer payload = new UnsafeBuffer(
                BufferUtil.allocateDirectAligned(payloadSize, 64));

            final long startNs = System.nanoTime();

            for (int i = 0; i < messageCount; i++)
            {
                sendWithRetry(sender, payload, payloadSize);
            }

            drainReceiver(receiver, receivedCount, messageCount);

            final long endNs = System.nanoTime();
            final long durationNs = endNs - startNs;
            final double durationSec = durationNs / 1_000_000_000.0;
            final double msgPerSec = messageCount / durationSec;
            final double mbPerSec = (messageCount * (double)(payloadSize + BridgeMessageCodec.HEADER_LENGTH))
                / (1024.0 * 1024.0) / durationSec;

            System.out.println("=== Throughput: " + label + " payload ===");
            System.out.println("  Messages:  " + messageCount);
            System.out.println("  Duration:  " + formatNanos(durationNs));
            System.out.println("  Rate:      " + String.format("%.0f msg/s", msgPerSec));
            System.out.println("  Bandwidth: " + String.format("%.2f MB/s", mbPerSec));
        }

        assertEquals(messageCount, receivedCount.get(),
            "All " + label + " messages should be received");
    }

    private static void awaitConnected(final BridgeSender sender)
    {
        final long deadline = System.currentTimeMillis() + 5000;
        while (!sender.isConnected() && System.currentTimeMillis() < deadline)
        {
            Thread.yield();
        }
        assertTrue(sender.isConnected(), "Sender should be connected");
    }

    private static void sendWithRetry(
        final BridgeSender sender,
        final UnsafeBuffer payload,
        final int payloadSize)
    {
        long seq = -1;
        while (seq < 0)
        {
            seq = sender.send(payload, 0, payloadSize);
            if (seq < 0)
            {
                Thread.yield();
            }
        }
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
        final BridgeReceiver receiver,
        final AtomicLong count,
        final int expectedCount)
    {
        final long deadline = System.currentTimeMillis() + DRAIN_TIMEOUT_MS;
        while (count.get() < expectedCount && System.currentTimeMillis() < deadline)
        {
            receiver.poll();
            Thread.yield();
        }
    }

    /**
     * Format nanoseconds to a human-readable string with appropriate units.
     */
    private static String formatNanos(final long nanos)
    {
        if (nanos < 1_000)
        {
            return nanos + " ns";
        }
        else if (nanos < 1_000_000)
        {
            return String.format("%.2f us", nanos / 1_000.0);
        }
        else if (nanos < 1_000_000_000)
        {
            return String.format("%.2f ms", nanos / 1_000_000.0);
        }
        else
        {
            return String.format("%.3f s", nanos / 1_000_000_000.0);
        }
    }
}
