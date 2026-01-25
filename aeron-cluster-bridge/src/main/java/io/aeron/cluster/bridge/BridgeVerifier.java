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

import org.agrona.DirectBuffer;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Deterministic verifier for the Bridge Service.
 * <p>
 * <b>Design Rationale (Testing - Automated Verification):</b>
 * This verifier provides automated, deterministic validation of the bridge's
 * core guarantees:
 * <ul>
 *   <li>No duplicates: Each sequence number appears exactly once</li>
 *   <li>Ordering: Sequences are received in increasing order</li>
 *   <li>Completeness: All sent messages are received</li>
 * </ul>
 * <p>
 * <b>Design Rationale (SOLID - Single Responsibility):</b>
 * This class is dedicated to verification logic. It does not handle actual
 * message processing - that's delegated to BridgeReceiver.
 * <p>
 * <b>Usage:</b>
 * <pre>
 * ./gradlew :aeron-cluster-bridge:runBridgeVerifier
 * </pre>
 * <p>
 * <b>Verification Steps:</b>
 * <ol>
 *   <li>Clean up any existing checkpoints/archives</li>
 *   <li>Start sender and receiver in separate threads</li>
 *   <li>Send N messages</li>
 *   <li>Verify receiver got exactly N messages</li>
 *   <li>Verify no duplicates (using HashSet)</li>
 *   <li>Verify ordering (lastSeq &lt; currentSeq)</li>
 *   <li>Report results</li>
 * </ol>
 * <p>
 * <b>Assumptions:</b>
 * <ul>
 *   <li>Single sender, single receiver</li>
 *   <li>Clean start (no existing state)</li>
 *   <li>Local execution (same machine)</li>
 * </ul>
 */
public final class BridgeVerifier
{
    private final BridgeConfiguration.Direction direction;
    private final int messageCount;
    private final AtomicBoolean running = new AtomicBoolean(true);

    // Verification state
    private final LongHashSet receivedSequences = new LongHashSet();
    private final AtomicLong lastReceivedSequence = new AtomicLong(0);
    private final AtomicLong duplicateCount = new AtomicLong(0);
    private final AtomicLong outOfOrderCount = new AtomicLong(0);
    private final AtomicLong receiveCount = new AtomicLong(0);

    /**
     * Create a verifier for the specified configuration.
     *
     * @param direction    the bridge direction to verify
     * @param messageCount the number of messages to send/verify
     */
    public BridgeVerifier(final BridgeConfiguration.Direction direction, final int messageCount)
    {
        this.direction = direction;
        this.messageCount = messageCount;
    }

    /**
     * Run the verification test.
     *
     * @return true if all verifications pass
     */
    public boolean verify() throws Exception
    {
        System.out.println("[Verifier] =====================================");
        System.out.println("[Verifier] Bridge Verification Test");
        System.out.println("[Verifier] Direction: " + direction);
        System.out.println("[Verifier] Message count: " + messageCount);
        System.out.println("[Verifier] =====================================");

        // Clean up existing state
        cleanupState();

        // Configure direction
        System.setProperty(BridgeConfiguration.DIRECTION_PROP, direction.name());
        System.setProperty(BridgeConfiguration.STREAM_ID_PROP,
            String.valueOf(direction.defaultStreamId()));
        System.setProperty(BridgeConfiguration.MESSAGE_INTERVAL_MS_PROP, "1");

        final CountDownLatch senderComplete = new CountDownLatch(1);
        final CountDownLatch receiverReady = new CountDownLatch(1);

        // Start receiver first
        final Thread receiverThread = new Thread(() ->
        {
            try (BridgeReceiver receiver = new BridgeReceiver(direction))
            {
                receiver.messageHandler(this::onMessage);
                receiver.start();

                receiverReady.countDown();

                final long deadline = System.currentTimeMillis() + 60_000; // 60 second timeout

                while (running.get() && System.currentTimeMillis() < deadline)
                {
                    receiver.poll(10);

                    // Exit when we've received all messages
                    if (receiveCount.get() >= messageCount)
                    {
                        break;
                    }

                    Thread.sleep(1);
                }
            }
            catch (final Exception e)
            {
                e.printStackTrace();
            }
        }, "verifier-receiver");
        receiverThread.start();

        // Wait for receiver to be ready
        if (!receiverReady.await(10, TimeUnit.SECONDS))
        {
            System.err.println("[Verifier] Receiver failed to start");
            running.set(false);
            return false;
        }

        // Start sender
        final Thread senderThread = new Thread(() ->
        {
            try (BridgeSender sender = new BridgeSender(direction))
            {
                sender.start();

                // Wait a bit for receiver to connect
                Thread.sleep(500);

                for (int i = 0; i < messageCount && running.get(); i++)
                {
                    sender.publishHeartbeat();

                    // Minimal delay
                    if (i % 100 == 0)
                    {
                        Thread.sleep(1);
                    }
                }

                sender.awaitArchiveCatchup();
                senderComplete.countDown();
            }
            catch (final Exception e)
            {
                e.printStackTrace();
            }
        }, "verifier-sender");
        senderThread.start();

        // Wait for sender to complete
        if (!senderComplete.await(60, TimeUnit.SECONDS))
        {
            System.err.println("[Verifier] Sender did not complete in time");
        }

        // Give receiver time to process remaining messages
        Thread.sleep(2000);

        // Stop receiver
        running.set(false);
        receiverThread.join(5000);
        senderThread.join(5000);

        // Report results
        return reportResults();
    }

    /**
     * Message handler for verification.
     * <p>
     * <b>Thread Safety:</b> Called from receiver thread only.
     */
    private void onMessage(
        final long sequence,
        final long timestamp,
        final int msgType,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        // Check for duplicates
        if (receivedSequences.contains(sequence))
        {
            duplicateCount.incrementAndGet();
            System.err.println("[Verifier] DUPLICATE detected: seq=" + sequence);
            return;
        }

        // Check ordering
        final long lastSeq = lastReceivedSequence.get();
        if (sequence <= lastSeq)
        {
            outOfOrderCount.incrementAndGet();
            System.err.println("[Verifier] OUT OF ORDER: last=" + lastSeq + ", current=" + sequence);
        }

        // Record sequence
        receivedSequences.add(sequence);
        lastReceivedSequence.set(sequence);
        receiveCount.incrementAndGet();

        // Progress indicator
        if (receiveCount.get() % 100 == 0)
        {
            System.out.println("[Verifier] Progress: " + receiveCount.get() + "/" + messageCount);
        }
    }

    /**
     * Report verification results.
     *
     * @return true if all checks pass
     */
    private boolean reportResults()
    {
        System.out.println();
        System.out.println("[Verifier] =====================================");
        System.out.println("[Verifier] VERIFICATION RESULTS");
        System.out.println("[Verifier] =====================================");
        System.out.println("[Verifier] Messages expected: " + messageCount);
        System.out.println("[Verifier] Messages received: " + receiveCount.get());
        System.out.println("[Verifier] Duplicates:        " + duplicateCount.get());
        System.out.println("[Verifier] Out-of-order:      " + outOfOrderCount.get());

        final long missing = messageCount - receiveCount.get();
        System.out.println("[Verifier] Missing:           " + missing);

        final boolean pass =
            receiveCount.get() == messageCount &&
            duplicateCount.get() == 0 &&
            outOfOrderCount.get() == 0;

        System.out.println("[Verifier] =====================================");
        if (pass)
        {
            System.out.println("[Verifier] RESULT: PASS");
        }
        else
        {
            System.out.println("[Verifier] RESULT: FAIL");
            if (receiveCount.get() != messageCount)
            {
                System.out.println("[Verifier] - Message count mismatch");
            }
            if (duplicateCount.get() > 0)
            {
                System.out.println("[Verifier] - Duplicates detected");
            }
            if (outOfOrderCount.get() > 0)
            {
                System.out.println("[Verifier] - Out-of-order messages detected");
            }
        }
        System.out.println("[Verifier] =====================================");

        return pass;
    }

    /**
     * Clean up any existing state files.
     */
    private void cleanupState()
    {
        System.out.println("[Verifier] Cleaning up existing state...");

        // Delete checkpoint
        BridgeCheckpoint.delete(BridgeConfiguration.checkpointDir(), direction);

        System.out.println("[Verifier] State cleanup complete");
    }

    /**
     * Main entry point for the verifier.
     *
     * @param args command line arguments
     * @throws Exception if verification fails
     */
    public static void main(final String[] args) throws Exception
    {
        final BridgeConfiguration.Direction direction = BridgeConfiguration.direction();
        final int messageCount = BridgeConfiguration.messageCount();

        final BridgeVerifier verifier = new BridgeVerifier(direction, messageCount);
        final boolean success = verifier.verify();

        System.exit(success ? 0 : 1);
    }
}
