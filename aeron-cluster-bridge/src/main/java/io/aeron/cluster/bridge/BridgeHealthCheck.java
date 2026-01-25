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

/**
 * Health check for the inter-cluster bridge, suitable for Kubernetes
 * liveness and readiness probes, load balancer health checks, or
 * operational dashboards.
 * <p>
 * <b>Liveness vs Readiness:</b>
 * <ul>
 *   <li><b>Liveness ({@link #isAlive()}):</b> Can the bridge process still
 *       function? Checks that the underlying Aeron client is not closed
 *       and that the sender/receiver objects exist. A failed liveness check
 *       means the process should be restarted.</li>
 *   <li><b>Readiness ({@link #isReady()}):</b> Is the bridge ready to accept
 *       traffic? Checks that both sender is connected and receiver is
 *       subscribed. A failed readiness check means the bridge should not
 *       receive traffic but may recover on its own.</li>
 * </ul>
 * <p>
 * <b>Staleness detection:</b> The {@link #isHealthy()} check also verifies
 * that messages were sent or received within a configurable staleness
 * threshold. If no activity occurs for longer than the threshold, the
 * bridge is considered unhealthy. This catches "zombie" states where the
 * process is running but not processing messages (e.g., disconnected
 * publication, deadlocked thread).
 * <p>
 * <b>Design (SRP):</b> This class only evaluates health — it does not
 * take corrective action. Recovery is the responsibility of the
 * orchestrator (Kubernetes, systemd, supervisor).
 * <p>
 * <b>Thread safety:</b> All methods are safe to call from any thread.
 * Reads are non-blocking and rely on volatile semantics from
 * {@link BridgeMetrics} atomics.
 * <p>
 * <b>Integration example (Kubernetes):</b>
 * <pre>
 * // In your HTTP health endpoint:
 * if (healthCheck.isAlive()) return 200;
 * else return 503;
 * </pre>
 *
 * <b>Integration example (log-based monitoring):</b>
 * <pre>
 * // Periodic health log (e.g., every 10 seconds):
 * logger.info(healthCheck.statusReport());
 * </pre>
 */
public final class BridgeHealthCheck
{
    /**
     * Default staleness threshold: 30 seconds.
     * If no messages are sent or received within this window, the bridge
     * is considered stale. Override via constructor for environments with
     * different traffic patterns.
     */
    public static final long DEFAULT_STALENESS_THRESHOLD_NS = 30_000_000_000L;

    private final BridgeSender sender;
    private final BridgeReceiver receiver;
    private final BridgeMetrics senderMetrics;
    private final BridgeMetrics receiverMetrics;
    private final long stalenessThresholdNs;

    /**
     * Create a health check with the default staleness threshold.
     *
     * @param sender          the bridge sender (may be null if receive-only).
     * @param receiver        the bridge receiver (may be null if send-only).
     * @param senderMetrics   metrics for the sender direction (may be null).
     * @param receiverMetrics metrics for the receiver direction (may be null).
     */
    public BridgeHealthCheck(
        final BridgeSender sender,
        final BridgeReceiver receiver,
        final BridgeMetrics senderMetrics,
        final BridgeMetrics receiverMetrics)
    {
        this(sender, receiver, senderMetrics, receiverMetrics, DEFAULT_STALENESS_THRESHOLD_NS);
    }

    /**
     * Create a health check with a custom staleness threshold.
     *
     * @param sender               the bridge sender (may be null).
     * @param receiver             the bridge receiver (may be null).
     * @param senderMetrics        metrics for the sender (may be null).
     * @param receiverMetrics      metrics for the receiver (may be null).
     * @param stalenessThresholdNs staleness threshold in nanoseconds.
     */
    public BridgeHealthCheck(
        final BridgeSender sender,
        final BridgeReceiver receiver,
        final BridgeMetrics senderMetrics,
        final BridgeMetrics receiverMetrics,
        final long stalenessThresholdNs)
    {
        this.sender = sender;
        this.receiver = receiver;
        this.senderMetrics = senderMetrics;
        this.receiverMetrics = receiverMetrics;
        this.stalenessThresholdNs = stalenessThresholdNs;
    }

    /**
     * Liveness probe: is the bridge process still functional?
     * <p>
     * Checks:
     * <ul>
     *   <li>Sender exists and publication is not closed</li>
     *   <li>Receiver exists and is still running (not shut down)</li>
     * </ul>
     * <p>
     * A failed liveness probe indicates the process is broken and
     * should be restarted by the orchestrator.
     *
     * @return true if the bridge is alive.
     */
    public boolean isAlive()
    {
        final boolean senderAlive = sender == null || sender.isConnected();
        final boolean receiverAlive = receiver == null || receiver.isRunning();
        return senderAlive && receiverAlive;
    }

    /**
     * Readiness probe: is the bridge ready to process traffic?
     * <p>
     * Checks:
     * <ul>
     *   <li>Sender is connected to at least one subscriber</li>
     *   <li>Receiver subscription is connected (has at least one image)</li>
     *   <li>Receiver has merged with the live stream (replay-merge complete)</li>
     * </ul>
     * <p>
     * A failed readiness probe means the bridge is alive but cannot
     * yet process traffic (e.g., still catching up from archive).
     * Traffic should be held until ready.
     *
     * @return true if the bridge is ready for traffic.
     */
    public boolean isReady()
    {
        final boolean senderReady = sender == null || sender.isConnected();
        final boolean receiverReady = receiver == null ||
            (receiver.isConnected() && receiver.isMerged());
        return senderReady && receiverReady;
    }

    /**
     * Full health check: alive + ready + not stale.
     * <p>
     * Combines liveness, readiness, and staleness checks into a single
     * boolean. Use this for comprehensive monitoring where you need a
     * single "is everything OK?" signal.
     * <p>
     * Staleness is only checked if messages have been previously sent
     * or received (fresh starts are not considered stale).
     *
     * @return true if the bridge is fully healthy.
     */
    public boolean isHealthy()
    {
        return isAlive() && isReady() && !isStale();
    }

    /**
     * Check if the bridge is stale (no recent activity).
     * <p>
     * A bridge is considered stale if:
     * <ul>
     *   <li>At least one message has been sent or received previously
     *       (not a fresh start)</li>
     *   <li>The time since the last send or receive exceeds the
     *       staleness threshold</li>
     * </ul>
     * <p>
     * This catches "zombie" processes that are technically alive but not
     * processing messages (disconnected, deadlocked, etc.).
     *
     * @return true if the bridge is stale.
     */
    public boolean isStale()
    {
        final long now = System.nanoTime();
        boolean hasActivity = false;

        if (senderMetrics != null && senderMetrics.messagesSent() > 0)
        {
            hasActivity = true;
            if ((now - senderMetrics.lastSendTimestampNs()) > stalenessThresholdNs)
            {
                return true;
            }
        }

        if (receiverMetrics != null && receiverMetrics.messagesReceived() > 0)
        {
            hasActivity = true;
            if ((now - receiverMetrics.lastReceiveTimestampNs()) > stalenessThresholdNs)
            {
                return true;
            }
        }

        // If no activity has ever occurred, not stale (fresh start)
        return false;
    }

    /**
     * Check if the sender's circuit breaker is currently open.
     * <p>
     * An open circuit breaker means the sender is experiencing sustained
     * failures and is rejecting sends immediately. This is a critical
     * operational alert — investigate the publication or network.
     *
     * @return true if the circuit breaker is open, false if no sender.
     */
    public boolean isCircuitBreakerOpen()
    {
        return sender != null && sender.isCircuitOpen();
    }

    /**
     * Generate a human-readable status report for logging or dashboards.
     * <p>
     * Includes all health dimensions (alive, ready, stale, circuit breaker)
     * and key counters from metrics.
     *
     * @return formatted status report string.
     */
    public String statusReport()
    {
        final StringBuilder sb = new StringBuilder(512);
        sb.append("BridgeHealthCheck{");
        sb.append("alive=").append(isAlive());
        sb.append(", ready=").append(isReady());
        sb.append(", healthy=").append(isHealthy());
        sb.append(", stale=").append(isStale());
        sb.append(", circuitBreakerOpen=").append(isCircuitBreakerOpen());

        if (sender != null)
        {
            sb.append(", sender.connected=").append(sender.isConnected());
        }

        if (receiver != null)
        {
            sb.append(", receiver.running=").append(receiver.isRunning());
            sb.append(", receiver.merged=").append(receiver.isMerged());
            sb.append(", receiver.connected=").append(receiver.isConnected());
        }

        if (senderMetrics != null)
        {
            sb.append(", sender.sent=").append(senderMetrics.messagesSent());
            sb.append(", sender.failures=").append(senderMetrics.sendFailures());
            sb.append(", sender.backPressure=").append(senderMetrics.backPressureEvents());
        }

        if (receiverMetrics != null)
        {
            sb.append(", receiver.received=").append(receiverMetrics.messagesReceived());
            sb.append(", receiver.skipped=").append(receiverMetrics.messagesSkipped());
            sb.append(", receiver.invalid=").append(receiverMetrics.invalidMessages());
        }

        sb.append('}');
        return sb.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return statusReport();
    }
}
