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

import java.time.Instant;

/**
 * Immutable point-in-time status snapshot of the entire bridge.
 * <p>
 * Captures the operational state of both directions (ME-to-RMS and
 * RMS-to-ME) in a single consistent object. Intended for:
 * <ul>
 *   <li>Periodic status logging (e.g., every 10 seconds)</li>
 *   <li>HTTP/JMX status endpoints</li>
 *   <li>Operational dashboards</li>
 *   <li>Alerting systems (Datadog, PagerDuty, Grafana)</li>
 * </ul>
 * <p>
 * <b>Design (SRP):</b> This is a read-only data carrier — no business
 * logic, no I/O. It aggregates data from {@link BridgeHealthCheck} and
 * {@link BridgeMetrics} into a single snapshot.
 * <p>
 * <b>Immutability:</b> Once created via {@link #capture}, the snapshot
 * is immutable and safe to pass across threads, serialize to JSON, or
 * cache without defensive copies.
 * <p>
 * <b>Usage:</b>
 * <pre>
 * BridgeStatus status = BridgeStatus.capture(healthCheck, meToRmsMetrics, rmsToMeMetrics);
 * logger.info(status.toLogString());
 * // or serialize to JSON for HTTP endpoint
 * </pre>
 */
public final class BridgeStatus
{
    private final Instant capturedAt;
    private final boolean alive;
    private final boolean ready;
    private final boolean healthy;
    private final boolean stale;
    private final boolean circuitBreakerOpen;

    // ME-to-RMS direction metrics
    private final long meToRmsSent;
    private final long meToRmsReceived;
    private final long meToRmsSkipped;
    private final long meToRmsSendFailures;
    private final long meToRmsBackPressure;
    private final long meToRmsBytesOut;
    private final long meToRmsBytesIn;
    private final long meToRmsInvalid;

    // RMS-to-ME direction metrics
    private final long rmsToMeSent;
    private final long rmsToMeReceived;
    private final long rmsToMeSkipped;
    private final long rmsToMeSendFailures;
    private final long rmsToMeBackPressure;
    private final long rmsToMeBytesOut;
    private final long rmsToMeBytesIn;
    private final long rmsToMeInvalid;

    // Bridge version
    private final String protocolVersion;

    private BridgeStatus(
        final Instant capturedAt,
        final boolean alive,
        final boolean ready,
        final boolean healthy,
        final boolean stale,
        final boolean circuitBreakerOpen,
        final long meToRmsSent,
        final long meToRmsReceived,
        final long meToRmsSkipped,
        final long meToRmsSendFailures,
        final long meToRmsBackPressure,
        final long meToRmsBytesOut,
        final long meToRmsBytesIn,
        final long meToRmsInvalid,
        final long rmsToMeSent,
        final long rmsToMeReceived,
        final long rmsToMeSkipped,
        final long rmsToMeSendFailures,
        final long rmsToMeBackPressure,
        final long rmsToMeBytesOut,
        final long rmsToMeBytesIn,
        final long rmsToMeInvalid,
        final String protocolVersion)
    {
        this.capturedAt = capturedAt;
        this.alive = alive;
        this.ready = ready;
        this.healthy = healthy;
        this.stale = stale;
        this.circuitBreakerOpen = circuitBreakerOpen;
        this.meToRmsSent = meToRmsSent;
        this.meToRmsReceived = meToRmsReceived;
        this.meToRmsSkipped = meToRmsSkipped;
        this.meToRmsSendFailures = meToRmsSendFailures;
        this.meToRmsBackPressure = meToRmsBackPressure;
        this.meToRmsBytesOut = meToRmsBytesOut;
        this.meToRmsBytesIn = meToRmsBytesIn;
        this.meToRmsInvalid = meToRmsInvalid;
        this.rmsToMeSent = rmsToMeSent;
        this.rmsToMeReceived = rmsToMeReceived;
        this.rmsToMeSkipped = rmsToMeSkipped;
        this.rmsToMeSendFailures = rmsToMeSendFailures;
        this.rmsToMeBackPressure = rmsToMeBackPressure;
        this.rmsToMeBytesOut = rmsToMeBytesOut;
        this.rmsToMeBytesIn = rmsToMeBytesIn;
        this.rmsToMeInvalid = rmsToMeInvalid;
        this.protocolVersion = protocolVersion;
    }

    /**
     * Capture a point-in-time snapshot of the bridge status.
     * <p>
     * Reads all counters and health checks at the time of call.
     * The returned snapshot is immutable.
     *
     * @param healthCheck    the bridge health check.
     * @param meToRmsMetrics metrics for the ME-to-RMS direction (may be null).
     * @param rmsToMeMetrics metrics for the RMS-to-ME direction (may be null).
     * @return an immutable status snapshot.
     */
    public static BridgeStatus capture(
        final BridgeHealthCheck healthCheck,
        final BridgeMetrics meToRmsMetrics,
        final BridgeMetrics rmsToMeMetrics)
    {
        return new BridgeStatus(
            Instant.now(),
            healthCheck.isAlive(),
            healthCheck.isReady(),
            healthCheck.isHealthy(),
            healthCheck.isStale(),
            healthCheck.isCircuitBreakerOpen(),
            meToRmsMetrics != null ? meToRmsMetrics.messagesSent() : 0,
            meToRmsMetrics != null ? meToRmsMetrics.messagesReceived() : 0,
            meToRmsMetrics != null ? meToRmsMetrics.messagesSkipped() : 0,
            meToRmsMetrics != null ? meToRmsMetrics.sendFailures() : 0,
            meToRmsMetrics != null ? meToRmsMetrics.backPressureEvents() : 0,
            meToRmsMetrics != null ? meToRmsMetrics.bytesPublished() : 0,
            meToRmsMetrics != null ? meToRmsMetrics.bytesReceived() : 0,
            meToRmsMetrics != null ? meToRmsMetrics.invalidMessages() : 0,
            rmsToMeMetrics != null ? rmsToMeMetrics.messagesSent() : 0,
            rmsToMeMetrics != null ? rmsToMeMetrics.messagesReceived() : 0,
            rmsToMeMetrics != null ? rmsToMeMetrics.messagesSkipped() : 0,
            rmsToMeMetrics != null ? rmsToMeMetrics.sendFailures() : 0,
            rmsToMeMetrics != null ? rmsToMeMetrics.backPressureEvents() : 0,
            rmsToMeMetrics != null ? rmsToMeMetrics.bytesPublished() : 0,
            rmsToMeMetrics != null ? rmsToMeMetrics.bytesReceived() : 0,
            rmsToMeMetrics != null ? rmsToMeMetrics.invalidMessages() : 0,
            "BRDG-v" + BridgeMessageCodec.VERSION);
    }

    // ---- Health state accessors ----

    /**
     * @return the instant this snapshot was captured.
     */
    public Instant capturedAt()
    {
        return capturedAt;
    }

    /**
     * @return true if the bridge is alive (liveness probe).
     */
    public boolean isAlive()
    {
        return alive;
    }

    /**
     * @return true if the bridge is ready (readiness probe).
     */
    public boolean isReady()
    {
        return ready;
    }

    /**
     * @return true if the bridge is fully healthy.
     */
    public boolean isHealthy()
    {
        return healthy;
    }

    /**
     * @return true if the bridge is stale (no recent activity).
     */
    public boolean isStale()
    {
        return stale;
    }

    /**
     * @return true if the circuit breaker is open.
     */
    public boolean isCircuitBreakerOpen()
    {
        return circuitBreakerOpen;
    }

    /**
     * @return the protocol version string.
     */
    public String protocolVersion()
    {
        return protocolVersion;
    }

    // ---- ME-to-RMS metrics ----

    /**
     * @return ME-to-RMS messages sent.
     */
    public long meToRmsSent()
    {
        return meToRmsSent;
    }

    /**
     * @return ME-to-RMS messages received.
     */
    public long meToRmsReceived()
    {
        return meToRmsReceived;
    }

    /**
     * @return ME-to-RMS messages skipped (duplicates).
     */
    public long meToRmsSkipped()
    {
        return meToRmsSkipped;
    }

    /**
     * @return ME-to-RMS send failures.
     */
    public long meToRmsSendFailures()
    {
        return meToRmsSendFailures;
    }

    /**
     * @return ME-to-RMS back-pressure events.
     */
    public long meToRmsBackPressure()
    {
        return meToRmsBackPressure;
    }

    /**
     * @return ME-to-RMS bytes published.
     */
    public long meToRmsBytesOut()
    {
        return meToRmsBytesOut;
    }

    /**
     * @return ME-to-RMS bytes received.
     */
    public long meToRmsBytesIn()
    {
        return meToRmsBytesIn;
    }

    /**
     * @return ME-to-RMS invalid messages.
     */
    public long meToRmsInvalid()
    {
        return meToRmsInvalid;
    }

    // ---- RMS-to-ME metrics ----

    /**
     * @return RMS-to-ME messages sent.
     */
    public long rmsToMeSent()
    {
        return rmsToMeSent;
    }

    /**
     * @return RMS-to-ME messages received.
     */
    public long rmsToMeReceived()
    {
        return rmsToMeReceived;
    }

    /**
     * @return RMS-to-ME messages skipped.
     */
    public long rmsToMeSkipped()
    {
        return rmsToMeSkipped;
    }

    /**
     * @return RMS-to-ME send failures.
     */
    public long rmsToMeSendFailures()
    {
        return rmsToMeSendFailures;
    }

    /**
     * @return RMS-to-ME back-pressure events.
     */
    public long rmsToMeBackPressure()
    {
        return rmsToMeBackPressure;
    }

    /**
     * @return RMS-to-ME bytes published.
     */
    public long rmsToMeBytesOut()
    {
        return rmsToMeBytesOut;
    }

    /**
     * @return RMS-to-ME bytes received.
     */
    public long rmsToMeBytesIn()
    {
        return rmsToMeBytesIn;
    }

    /**
     * @return RMS-to-ME invalid messages.
     */
    public long rmsToMeInvalid()
    {
        return rmsToMeInvalid;
    }

    /**
     * Generate a structured log line suitable for log aggregation systems
     * (Splunk, ELK, CloudWatch Logs).
     * <p>
     * Uses key=value format for easy parsing by log analysis tools.
     *
     * @return formatted status log line.
     */
    public String toLogString()
    {
        return "bridge.status" +
            " time=" + capturedAt +
            " version=" + protocolVersion +
            " alive=" + alive +
            " ready=" + ready +
            " healthy=" + healthy +
            " stale=" + stale +
            " circuit_open=" + circuitBreakerOpen +
            " me_rms.sent=" + meToRmsSent +
            " me_rms.recv=" + meToRmsReceived +
            " me_rms.skip=" + meToRmsSkipped +
            " me_rms.fail=" + meToRmsSendFailures +
            " me_rms.bp=" + meToRmsBackPressure +
            " me_rms.bytes_out=" + meToRmsBytesOut +
            " me_rms.bytes_in=" + meToRmsBytesIn +
            " me_rms.invalid=" + meToRmsInvalid +
            " rms_me.sent=" + rmsToMeSent +
            " rms_me.recv=" + rmsToMeReceived +
            " rms_me.skip=" + rmsToMeSkipped +
            " rms_me.fail=" + rmsToMeSendFailures +
            " rms_me.bp=" + rmsToMeBackPressure +
            " rms_me.bytes_out=" + rmsToMeBytesOut +
            " rms_me.bytes_in=" + rmsToMeBytesIn +
            " rms_me.invalid=" + rmsToMeInvalid;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return toLogString();
    }
}
