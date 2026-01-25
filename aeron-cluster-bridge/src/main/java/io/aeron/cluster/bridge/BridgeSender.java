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
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.locks.LockSupport;

/**
 * Bridge sender that publishes sequenced messages on a UDP channel.
 * <p>
 * <b>Design (SRP):</b> Sole responsibility is encoding and offering messages
 * via an {@link ExclusivePublication}. Sequence numbering is managed here
 * (monotonically increasing, 1-based).
 * <p>
 * <b>Design choice:</b> Uses {@link ExclusivePublication} (not
 * {@link io.aeron.ConcurrentPublication}) because the bridge has a single
 * sender per direction. This avoids a CAS on every {@code offer()} call.
 * <p>
 * <b>Failure handling:</b>
 * <ul>
 *   <li><b>Exponential backoff:</b> On back-pressure, retries with increasing
 *       delay from {@link BridgeConfiguration#INITIAL_BACKOFF_NS} up to
 *       {@link BridgeConfiguration#MAX_BACKOFF_NS}.</li>
 *   <li><b>Circuit breaker:</b> After
 *       {@link BridgeConfiguration#CIRCUIT_BREAKER_THRESHOLD} consecutive
 *       failures, the sender enters an "open" state and rejects sends
 *       immediately for a cooldown period, avoiding wasted CPU on a broken
 *       publication.</li>
 * </ul>
 * <p>
 * <b>Assumption:</b> The caller is single-threaded per sender instance.
 * ExclusivePublication is not thread-safe.
 */
public final class BridgeSender implements AutoCloseable
{
    private final ExclusivePublication publication;
    private final int direction;
    private final UnsafeBuffer encodeBuffer;
    private long nextSequence = 1;
    private int consecutiveFailures;
    private long circuitOpenUntilNs;

    /**
     * Create a bridge sender.
     *
     * @param aeron     the Aeron client instance.
     * @param channel   the UDP channel to publish on.
     * @param streamId  the stream ID.
     * @param direction the bridge direction constant.
     */
    public BridgeSender(
        final Aeron aeron,
        final String channel,
        final int streamId,
        final int direction)
    {
        this.publication = aeron.addExclusivePublication(channel, streamId);
        this.direction = direction;
        // Pre-allocate encode buffer: header (28 bytes) + max payload (1024 bytes).
        // Assumption: payloads do not exceed 1024 bytes for bridge messages.
        this.encodeBuffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(
            BridgeMessageCodec.HEADER_LENGTH + 1024, 64));
    }

    /**
     * Send a payload as a sequenced bridge message.
     * <p>
     * Uses exponential backoff on back-pressure and a circuit breaker
     * for sustained failures. Returns -1 if the send fails.
     *
     * @param payload       the payload buffer.
     * @param payloadOffset offset within the payload buffer.
     * @param payloadLength length of the payload.
     * @return the sequence number assigned, or -1 if the send failed.
     */
    public long send(
        final DirectBuffer payload,
        final int payloadOffset,
        final int payloadLength)
    {
        // Circuit breaker: if open, reject immediately until cooldown expires
        if (consecutiveFailures >= BridgeConfiguration.CIRCUIT_BREAKER_THRESHOLD)
        {
            if (System.nanoTime() < circuitOpenUntilNs)
            {
                return -1; // Circuit open — fast-fail
            }
            // Cooldown expired — half-open: allow one attempt through
        }

        final long sequence = nextSequence;
        final long timestampNs = System.nanoTime();

        final int encodedLength = BridgeMessageCodec.encode(
            encodeBuffer, 0, direction, sequence, timestampNs,
            payload, payloadOffset, payloadLength);

        int retries = 0;
        long backoffNs = BridgeConfiguration.INITIAL_BACKOFF_NS;

        while (true)
        {
            final long result = publication.offer(encodeBuffer, 0, encodedLength);

            if (result > 0)
            {
                nextSequence++;
                consecutiveFailures = 0; // Reset circuit breaker on success
                return sequence;
            }
            else if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION)
            {
                retries++;
                if (retries > BridgeConfiguration.MAX_BACK_PRESSURE_RETRIES)
                {
                    onSendFailure();
                    return -1;
                }

                // Exponential backoff: park with increasing delay, capped at MAX_BACKOFF_NS
                LockSupport.parkNanos(backoffNs);
                backoffNs = Math.min(backoffNs * 2, BridgeConfiguration.MAX_BACKOFF_NS);
            }
            else
            {
                // NOT_CONNECTED, CLOSED, MAX_POSITION_EXCEEDED — non-retriable
                onSendFailure();
                return -1;
            }
        }
    }

    /**
     * Check if the circuit breaker is currently open (rejecting sends).
     *
     * @return true if the circuit breaker is open.
     */
    public boolean isCircuitOpen()
    {
        return consecutiveFailures >= BridgeConfiguration.CIRCUIT_BREAKER_THRESHOLD &&
            System.nanoTime() < circuitOpenUntilNs;
    }

    /**
     * Reset the circuit breaker, allowing sends to proceed immediately.
     */
    public void resetCircuitBreaker()
    {
        consecutiveFailures = 0;
        circuitOpenUntilNs = 0;
    }

    private void onSendFailure()
    {
        consecutiveFailures++;
        if (consecutiveFailures >= BridgeConfiguration.CIRCUIT_BREAKER_THRESHOLD)
        {
            circuitOpenUntilNs = System.nanoTime() + BridgeConfiguration.CIRCUIT_BREAKER_COOLDOWN_NS;
        }
    }

    /**
     * Check if the publication is connected to at least one subscriber.
     *
     * @return true if connected.
     */
    public boolean isConnected()
    {
        return publication.isConnected();
    }

    /**
     * Get the current session ID of the publication.
     *
     * @return the session ID.
     */
    public int sessionId()
    {
        return publication.sessionId();
    }

    /**
     * Get the next sequence number that will be assigned.
     *
     * @return the next sequence.
     */
    public long nextSequence()
    {
        return nextSequence;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        publication.close();
    }
}
