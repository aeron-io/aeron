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
package io.aeron.cluster.bridge.monitoring;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Comprehensive metrics collection for Bridge components.
 * <p>
 * <b>Design Rationale:</b>
 * Uses LongAdder for high-contention counters (better than AtomicLong
 * under heavy concurrent updates) and AtomicLong for gauges.
 * <p>
 * <b>Metric Categories:</b>
 * <ul>
 *   <li>Throughput: messages sent/received per second</li>
 *   <li>Latency: processing time percentiles</li>
 *   <li>Errors: failure counts by type</li>
 *   <li>Health: component status indicators</li>
 * </ul>
 */
public final class BridgeMetrics
{
    private final String componentName;

    // Throughput counters (use LongAdder for high-throughput scenarios)
    private final LongAdder messagesSent = new LongAdder();
    private final LongAdder messagesReceived = new LongAdder();
    private final LongAdder bytesSent = new LongAdder();
    private final LongAdder bytesReceived = new LongAdder();

    // Error counters
    private final LongAdder backPressureEvents = new LongAdder();
    private final LongAdder connectionLostEvents = new LongAdder();
    private final LongAdder duplicatesDiscarded = new LongAdder();
    private final LongAdder sequenceGaps = new LongAdder();
    private final LongAdder checksumFailures = new LongAdder();
    private final LongAdder publicationErrors = new LongAdder();

    // Gauges (current values)
    private final AtomicLong lastSequenceSent = new AtomicLong(0);
    private final AtomicLong lastSequenceReceived = new AtomicLong(0);
    private final AtomicLong currentPosition = new AtomicLong(0);
    private final AtomicLong archivePosition = new AtomicLong(0);
    private final AtomicLong checkpointPosition = new AtomicLong(0);
    private final AtomicLong queueDepth = new AtomicLong(0);

    // Latency tracking (simple histogram buckets in nanoseconds)
    private final LongAdder latencyUnder1us = new LongAdder();
    private final LongAdder latency1to10us = new LongAdder();
    private final LongAdder latency10to100us = new LongAdder();
    private final LongAdder latency100usto1ms = new LongAdder();
    private final LongAdder latencyOver1ms = new LongAdder();

    // Timing
    private final AtomicLong lastMessageTimestampNs = new AtomicLong(0);
    private final AtomicLong startTimeMs = new AtomicLong(System.currentTimeMillis());

    // Status
    private volatile boolean connected = false;
    private volatile boolean healthy = true;
    private volatile String lastError = "";

    public BridgeMetrics(final String componentName)
    {
        this.componentName = componentName;
    }

    // =========================================================================
    // Recording methods (called from hot path - must be fast)
    // =========================================================================

    public void recordMessageSent(final int bytes)
    {
        messagesSent.increment();
        bytesSent.add(bytes);
        lastMessageTimestampNs.set(System.nanoTime());
    }

    public void recordMessageReceived(final int bytes)
    {
        messagesReceived.increment();
        bytesReceived.add(bytes);
        lastMessageTimestampNs.set(System.nanoTime());
    }

    public void recordLatencyNanos(final long latencyNs)
    {
        if (latencyNs < 1_000)
        {
            latencyUnder1us.increment();
        }
        else if (latencyNs < 10_000)
        {
            latency1to10us.increment();
        }
        else if (latencyNs < 100_000)
        {
            latency10to100us.increment();
        }
        else if (latencyNs < 1_000_000)
        {
            latency100usto1ms.increment();
        }
        else
        {
            latencyOver1ms.increment();
        }
    }

    public void recordBackPressure()
    {
        backPressureEvents.increment();
    }

    public void recordConnectionLost()
    {
        connectionLostEvents.increment();
        connected = false;
    }

    public void recordConnectionEstablished()
    {
        connected = true;
    }

    public void recordDuplicateDiscarded()
    {
        duplicatesDiscarded.increment();
    }

    public void recordSequenceGap(final long expected, final long received)
    {
        sequenceGaps.increment();
    }

    public void recordChecksumFailure()
    {
        checksumFailures.increment();
        healthy = false;
    }

    public void recordPublicationError(final String error)
    {
        publicationErrors.increment();
        lastError = error;
    }

    public void updateSequenceSent(final long sequence)
    {
        lastSequenceSent.set(sequence);
    }

    public void updateSequenceReceived(final long sequence)
    {
        lastSequenceReceived.set(sequence);
    }

    public void updatePosition(final long position)
    {
        currentPosition.set(position);
    }

    public void updateArchivePosition(final long position)
    {
        archivePosition.set(position);
    }

    public void updateCheckpointPosition(final long position)
    {
        checkpointPosition.set(position);
    }

    public void updateQueueDepth(final long depth)
    {
        queueDepth.set(depth);
    }

    // =========================================================================
    // Getters for metric export
    // =========================================================================

    public String componentName()
    {
        return componentName;
    }

    public long messagesSent()
    {
        return messagesSent.sum();
    }

    public long messagesReceived()
    {
        return messagesReceived.sum();
    }

    public long bytesSent()
    {
        return bytesSent.sum();
    }

    public long bytesReceived()
    {
        return bytesReceived.sum();
    }

    public long backPressureEvents()
    {
        return backPressureEvents.sum();
    }

    public long connectionLostEvents()
    {
        return connectionLostEvents.sum();
    }

    public long duplicatesDiscarded()
    {
        return duplicatesDiscarded.sum();
    }

    public long sequenceGaps()
    {
        return sequenceGaps.sum();
    }

    public long checksumFailures()
    {
        return checksumFailures.sum();
    }

    public long publicationErrors()
    {
        return publicationErrors.sum();
    }

    public long lastSequenceSent()
    {
        return lastSequenceSent.get();
    }

    public long lastSequenceReceived()
    {
        return lastSequenceReceived.get();
    }

    public long currentPosition()
    {
        return currentPosition.get();
    }

    public long archivePosition()
    {
        return archivePosition.get();
    }

    public long checkpointPosition()
    {
        return checkpointPosition.get();
    }

    public long queueDepth()
    {
        return queueDepth.get();
    }

    public boolean isConnected()
    {
        return connected;
    }

    public boolean isHealthy()
    {
        return healthy;
    }

    public String lastError()
    {
        return lastError;
    }

    // =========================================================================
    // Derived metrics
    // =========================================================================

    /**
     * Calculate messages per second over lifetime.
     */
    public double messagesPerSecond()
    {
        final long elapsed = System.currentTimeMillis() - startTimeMs.get();
        if (elapsed <= 0)
        {
            return 0;
        }
        return (messagesSent.sum() + messagesReceived.sum()) * 1000.0 / elapsed;
    }

    /**
     * Calculate bytes per second over lifetime.
     */
    public double bytesPerSecond()
    {
        final long elapsed = System.currentTimeMillis() - startTimeMs.get();
        if (elapsed <= 0)
        {
            return 0;
        }
        return (bytesSent.sum() + bytesReceived.sum()) * 1000.0 / elapsed;
    }

    /**
     * Calculate archive lag (current position - checkpoint position).
     */
    public long archiveLag()
    {
        return currentPosition.get() - checkpointPosition.get();
    }

    /**
     * Get latency distribution as percentages.
     */
    public LatencyDistribution latencyDistribution()
    {
        final long total = latencyUnder1us.sum() + latency1to10us.sum() +
            latency10to100us.sum() + latency100usto1ms.sum() + latencyOver1ms.sum();

        if (total == 0)
        {
            return new LatencyDistribution(0, 0, 0, 0, 0);
        }

        return new LatencyDistribution(
            100.0 * latencyUnder1us.sum() / total,
            100.0 * latency1to10us.sum() / total,
            100.0 * latency10to100us.sum() / total,
            100.0 * latency100usto1ms.sum() / total,
            100.0 * latencyOver1ms.sum() / total
        );
    }

    /**
     * Reset all counters (for testing or periodic resets).
     */
    public void reset()
    {
        messagesSent.reset();
        messagesReceived.reset();
        bytesSent.reset();
        bytesReceived.reset();
        backPressureEvents.reset();
        connectionLostEvents.reset();
        duplicatesDiscarded.reset();
        sequenceGaps.reset();
        checksumFailures.reset();
        publicationErrors.reset();
        latencyUnder1us.reset();
        latency1to10us.reset();
        latency10to100us.reset();
        latency100usto1ms.reset();
        latencyOver1ms.reset();
        startTimeMs.set(System.currentTimeMillis());
        lastError = "";
        healthy = true;
    }

    /**
     * Export metrics in Prometheus format.
     */
    public String toPrometheusFormat()
    {
        final StringBuilder sb = new StringBuilder();
        final String prefix = "bridge_" + componentName.toLowerCase().replace('-', '_');

        // Counters
        appendMetric(sb, prefix + "_messages_sent_total", messagesSent.sum(), "counter");
        appendMetric(sb, prefix + "_messages_received_total", messagesReceived.sum(), "counter");
        appendMetric(sb, prefix + "_bytes_sent_total", bytesSent.sum(), "counter");
        appendMetric(sb, prefix + "_bytes_received_total", bytesReceived.sum(), "counter");
        appendMetric(sb, prefix + "_backpressure_events_total", backPressureEvents.sum(), "counter");
        appendMetric(sb, prefix + "_connection_lost_total", connectionLostEvents.sum(), "counter");
        appendMetric(sb, prefix + "_duplicates_discarded_total", duplicatesDiscarded.sum(), "counter");
        appendMetric(sb, prefix + "_sequence_gaps_total", sequenceGaps.sum(), "counter");
        appendMetric(sb, prefix + "_errors_total", publicationErrors.sum(), "counter");

        // Gauges
        appendMetric(sb, prefix + "_last_sequence_sent", lastSequenceSent.get(), "gauge");
        appendMetric(sb, prefix + "_last_sequence_received", lastSequenceReceived.get(), "gauge");
        appendMetric(sb, prefix + "_position_bytes", currentPosition.get(), "gauge");
        appendMetric(sb, prefix + "_archive_position_bytes", archivePosition.get(), "gauge");
        appendMetric(sb, prefix + "_checkpoint_position_bytes", checkpointPosition.get(), "gauge");
        appendMetric(sb, prefix + "_connected", connected ? 1 : 0, "gauge");
        appendMetric(sb, prefix + "_healthy", healthy ? 1 : 0, "gauge");

        // Latency histogram buckets
        appendMetric(sb, prefix + "_latency_bucket{le=\"1us\"}", latencyUnder1us.sum(), "counter");
        appendMetric(sb, prefix + "_latency_bucket{le=\"10us\"}", latency1to10us.sum(), "counter");
        appendMetric(sb, prefix + "_latency_bucket{le=\"100us\"}", latency10to100us.sum(), "counter");
        appendMetric(sb, prefix + "_latency_bucket{le=\"1ms\"}", latency100usto1ms.sum(), "counter");
        appendMetric(sb, prefix + "_latency_bucket{le=\"+Inf\"}", latencyOver1ms.sum(), "counter");

        return sb.toString();
    }

    private void appendMetric(final StringBuilder sb, final String name, final long value, final String type)
    {
        sb.append("# TYPE ").append(name).append(" ").append(type).append("\n");
        sb.append(name).append(" ").append(value).append("\n");
    }

    @Override
    public String toString()
    {
        return "BridgeMetrics{" +
            "component='" + componentName + '\'' +
            ", sent=" + messagesSent.sum() +
            ", received=" + messagesReceived.sum() +
            ", backpressure=" + backPressureEvents.sum() +
            ", duplicates=" + duplicatesDiscarded.sum() +
            ", gaps=" + sequenceGaps.sum() +
            ", connected=" + connected +
            ", healthy=" + healthy +
            '}';
    }

    /**
     * Latency distribution percentages.
     */
    public static final class LatencyDistribution
    {
        public final double under1us;
        public final double from1to10us;
        public final double from10to100us;
        public final double from100usto1ms;
        public final double over1ms;

        LatencyDistribution(
            final double under1us,
            final double from1to10us,
            final double from10to100us,
            final double from100usto1ms,
            final double over1ms)
        {
            this.under1us = under1us;
            this.from1to10us = from1to10us;
            this.from10to100us = from10to100us;
            this.from100usto1ms = from100usto1ms;
            this.over1ms = over1ms;
        }

        @Override
        public String toString()
        {
            return String.format("<1µs: %.1f%%, 1-10µs: %.1f%%, 10-100µs: %.1f%%, 100µs-1ms: %.1f%%, >1ms: %.1f%%",
                under1us, from1to10us, from10to100us, from100usto1ms, over1ms);
        }
    }
}
