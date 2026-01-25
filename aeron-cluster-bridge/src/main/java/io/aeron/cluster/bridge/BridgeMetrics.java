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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Lock-free metrics counters for the inter-cluster bridge.
 * <p>
 * Provides production-grade observability with zero-allocation counters
 * suitable for high-frequency trading paths. All counters use
 * {@link AtomicLong} for thread-safe, lock-free updates that can be
 * safely read from monitoring threads without disturbing the hot path.
 * <p>
 * <b>Design (SRP):</b> Sole responsibility is accumulating and exposing
 * operational counters. No formatting, no I/O — consumers decide how to
 * export (JMX, Prometheus, logging, etc.).
 * <p>
 * <b>Design (OCP):</b> New metrics can be added without modifying existing
 * consumers — they simply ignore fields they don't read.
 * <p>
 * <b>Thread safety:</b> All counters are safe for concurrent increment
 * (sender/receiver threads) and read (monitoring thread). Counters are
 * monotonically increasing — they never decrease or reset, following the
 * Prometheus counter convention.
 * <p>
 * <b>Usage:</b> Create one instance per bridge direction. Pass to sender
 * and receiver constructors. Poll from a monitoring thread or expose via
 * JMX/HTTP.
 * <pre>
 * BridgeMetrics metrics = new BridgeMetrics("me-to-rms");
 * // ... pass to sender/receiver ...
 * // From monitoring thread:
 * long sent = metrics.messagesSent();
 * long rate = metrics.messagesSent() - previousSent; // delta for rate
 * </pre>
 */
public final class BridgeMetrics
{
    private final String name;
    private final AtomicLong messagesSent = new AtomicLong(0);
    private final AtomicLong messagesReceived = new AtomicLong(0);
    private final AtomicLong messagesSkipped = new AtomicLong(0);
    private final AtomicLong sendFailures = new AtomicLong(0);
    private final AtomicLong backPressureEvents = new AtomicLong(0);
    private final AtomicLong circuitBreakerTrips = new AtomicLong(0);
    private final AtomicLong checkpointSaves = new AtomicLong(0);
    private final AtomicLong checkpointErrors = new AtomicLong(0);
    private final AtomicLong bytesPublished = new AtomicLong(0);
    private final AtomicLong bytesReceived = new AtomicLong(0);
    private final AtomicLong invalidMessages = new AtomicLong(0);
    private final AtomicLong lastSendTimestampNs = new AtomicLong(0);
    private final AtomicLong lastReceiveTimestampNs = new AtomicLong(0);
    private final AtomicLong maxSendLatencyNs = new AtomicLong(0);
    private final AtomicLong totalSendLatencyNs = new AtomicLong(0);

    /**
     * Create a named metrics instance for one bridge direction.
     *
     * @param name descriptive name (e.g. "me-to-rms" or "rms-to-me").
     */
    public BridgeMetrics(final String name)
    {
        this.name = name;
    }

    // ---- Increment methods (called from hot path) ----

    /**
     * Record a successfully sent message.
     *
     * @param encodedLength total bytes published (header + payload).
     */
    public void recordMessageSent(final int encodedLength)
    {
        messagesSent.incrementAndGet();
        bytesPublished.addAndGet(encodedLength);
        lastSendTimestampNs.set(System.nanoTime());
    }

    /**
     * Record a send latency sample (time from encode start to offer success).
     *
     * @param latencyNs latency in nanoseconds.
     */
    public void recordSendLatency(final long latencyNs)
    {
        totalSendLatencyNs.addAndGet(latencyNs);
        long currentMax;
        do
        {
            currentMax = maxSendLatencyNs.get();
            if (latencyNs <= currentMax)
            {
                return;
            }
        }
        while (!maxSendLatencyNs.compareAndSet(currentMax, latencyNs));
    }

    /**
     * Record a successfully received and applied message.
     *
     * @param payloadLength payload bytes received.
     */
    public void recordMessageReceived(final int payloadLength)
    {
        messagesReceived.incrementAndGet();
        bytesReceived.addAndGet(payloadLength + BridgeMessageCodec.HEADER_LENGTH);
        lastReceiveTimestampNs.set(System.nanoTime());
    }

    /**
     * Record a skipped message (duplicate from replay).
     */
    public void recordMessageSkipped()
    {
        messagesSkipped.incrementAndGet();
    }

    /**
     * Record a send failure (non-retriable or max retries exceeded).
     */
    public void recordSendFailure()
    {
        sendFailures.incrementAndGet();
    }

    /**
     * Record a back-pressure event (publication returned BACK_PRESSURED).
     */
    public void recordBackPressure()
    {
        backPressureEvents.incrementAndGet();
    }

    /**
     * Record a circuit breaker trip (threshold exceeded).
     */
    public void recordCircuitBreakerTrip()
    {
        circuitBreakerTrips.incrementAndGet();
    }

    /**
     * Record a successful checkpoint save.
     */
    public void recordCheckpointSave()
    {
        checkpointSaves.incrementAndGet();
    }

    /**
     * Record a checkpoint save error.
     */
    public void recordCheckpointError()
    {
        checkpointErrors.incrementAndGet();
    }

    /**
     * Record an invalid message (bad magic, bad version, bad length).
     */
    public void recordInvalidMessage()
    {
        invalidMessages.incrementAndGet();
    }

    // ---- Read methods (called from monitoring thread) ----

    /**
     * Get the metrics instance name.
     *
     * @return the name.
     */
    public String name()
    {
        return name;
    }

    /**
     * Total messages successfully sent.
     *
     * @return sent count.
     */
    public long messagesSent()
    {
        return messagesSent.get();
    }

    /**
     * Total messages successfully received and applied.
     *
     * @return received count.
     */
    public long messagesReceived()
    {
        return messagesReceived.get();
    }

    /**
     * Total messages skipped (duplicates from replay).
     *
     * @return skipped count.
     */
    public long messagesSkipped()
    {
        return messagesSkipped.get();
    }

    /**
     * Total send failures.
     *
     * @return failure count.
     */
    public long sendFailures()
    {
        return sendFailures.get();
    }

    /**
     * Total back-pressure events encountered during send.
     *
     * @return back-pressure count.
     */
    public long backPressureEvents()
    {
        return backPressureEvents.get();
    }

    /**
     * Total circuit breaker trips.
     *
     * @return trip count.
     */
    public long circuitBreakerTrips()
    {
        return circuitBreakerTrips.get();
    }

    /**
     * Total successful checkpoint saves.
     *
     * @return save count.
     */
    public long checkpointSaves()
    {
        return checkpointSaves.get();
    }

    /**
     * Total checkpoint save errors.
     *
     * @return error count.
     */
    public long checkpointErrors()
    {
        return checkpointErrors.get();
    }

    /**
     * Total bytes published (header + payload).
     *
     * @return bytes published.
     */
    public long bytesPublished()
    {
        return bytesPublished.get();
    }

    /**
     * Total bytes received (header + payload).
     *
     * @return bytes received.
     */
    public long bytesReceived()
    {
        return bytesReceived.get();
    }

    /**
     * Total invalid messages (bad magic/version/length).
     *
     * @return invalid count.
     */
    public long invalidMessages()
    {
        return invalidMessages.get();
    }

    /**
     * Timestamp (nanoTime) of the last successful send.
     *
     * @return last send timestamp in nanoseconds, or 0 if never sent.
     */
    public long lastSendTimestampNs()
    {
        return lastSendTimestampNs.get();
    }

    /**
     * Timestamp (nanoTime) of the last successful receive.
     *
     * @return last receive timestamp in nanoseconds, or 0 if never received.
     */
    public long lastReceiveTimestampNs()
    {
        return lastReceiveTimestampNs.get();
    }

    /**
     * Maximum observed send latency in nanoseconds.
     *
     * @return max send latency ns.
     */
    public long maxSendLatencyNs()
    {
        return maxSendLatencyNs.get();
    }

    /**
     * Mean send latency in nanoseconds (total / sent).
     * Returns 0 if no messages have been sent.
     *
     * @return mean send latency ns.
     */
    public long meanSendLatencyNs()
    {
        final long sent = messagesSent.get();
        return sent > 0 ? totalSendLatencyNs.get() / sent : 0;
    }

    /**
     * Generate a formatted snapshot string for logging or diagnostics.
     * <p>
     * This is intended for human consumption (logs, dashboards).
     * For machine consumption, use the individual accessor methods.
     *
     * @return formatted metrics snapshot.
     */
    public String snapshot()
    {
        return "BridgeMetrics[" + name + "]{" +
            "sent=" + messagesSent.get() +
            ", received=" + messagesReceived.get() +
            ", skipped=" + messagesSkipped.get() +
            ", sendFailures=" + sendFailures.get() +
            ", backPressure=" + backPressureEvents.get() +
            ", circuitTrips=" + circuitBreakerTrips.get() +
            ", checkpointSaves=" + checkpointSaves.get() +
            ", checkpointErrors=" + checkpointErrors.get() +
            ", bytesOut=" + bytesPublished.get() +
            ", bytesIn=" + bytesReceived.get() +
            ", invalid=" + invalidMessages.get() +
            ", maxSendLatencyNs=" + maxSendLatencyNs.get() +
            ", meanSendLatencyNs=" + meanSendLatencyNs() +
            '}';
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return snapshot();
    }
}
