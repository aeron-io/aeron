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
 * Configuration constants for the inter-cluster bridge service.
 * <p>
 * All channel and stream ID defaults can be overridden via system properties,
 * following the Aeron convention of {@code aeron.bridge.*} namespacing.
 * <p>
 * <b>Assumption:</b> Stream IDs 1001 and 1002 are reserved for bridge traffic
 * and must not collide with any cluster-internal stream IDs. Ports 20121 and
 * 20122 are chosen to avoid conflict with the Aeron samples default (20121 is
 * reused intentionally for the ME direction).
 */
public final class BridgeConfiguration
{
    // ---- Direction constants (SRP: identifies message flow without coupling to transport) ----

    /**
     * Direction constant: Matching Engine to Risk Management System.
     */
    public static final int DIRECTION_ME_TO_RMS = 0;

    /**
     * Direction constant: Risk Management System to Matching Engine.
     */
    public static final int DIRECTION_RMS_TO_ME = 1;

    // ---- System property keys ----

    /**
     * System property for the ME-to-RMS live channel.
     */
    public static final String ME_TO_RMS_CHANNEL_PROP = "aeron.bridge.me.to.rms.channel";

    /**
     * System property for the RMS-to-ME live channel.
     */
    public static final String RMS_TO_ME_CHANNEL_PROP = "aeron.bridge.rms.to.me.channel";

    /**
     * System property for the ME-to-RMS stream ID.
     */
    public static final String ME_TO_RMS_STREAM_ID_PROP = "aeron.bridge.me.to.rms.stream.id";

    /**
     * System property for the RMS-to-ME stream ID.
     */
    public static final String RMS_TO_ME_STREAM_ID_PROP = "aeron.bridge.rms.to.me.stream.id";

    /**
     * System property for the checkpoint directory.
     */
    public static final String CHECKPOINT_DIR_PROP = "aeron.bridge.checkpoint.dir";

    /**
     * System property for the message count in demo mode.
     */
    public static final String MESSAGE_COUNT_PROP = "aeron.bridge.message.count";

    // ---- Resolved defaults ----

    /**
     * Default ME-to-RMS live channel (UDP unicast for local demo).
     */
    public static final String ME_TO_RMS_CHANNEL =
        System.getProperty(ME_TO_RMS_CHANNEL_PROP, "aeron:udp?endpoint=localhost:20121");

    /**
     * Default RMS-to-ME live channel (UDP unicast for local demo).
     */
    public static final String RMS_TO_ME_CHANNEL =
        System.getProperty(RMS_TO_ME_CHANNEL_PROP, "aeron:udp?endpoint=localhost:20122");

    /**
     * Default ME-to-RMS stream ID.
     */
    public static final int ME_TO_RMS_STREAM_ID = Integer.getInteger(ME_TO_RMS_STREAM_ID_PROP, 1001);

    /**
     * Default RMS-to-ME stream ID.
     */
    public static final int RMS_TO_ME_STREAM_ID = Integer.getInteger(RMS_TO_ME_STREAM_ID_PROP, 1002);

    /**
     * Default checkpoint directory.
     */
    public static final String CHECKPOINT_DIR = System.getProperty(CHECKPOINT_DIR_PROP, ".");

    /**
     * Default message count for demo/test.
     */
    public static final int MESSAGE_COUNT = Integer.getInteger(MESSAGE_COUNT_PROP, 100);

    // ---- Tuning constants ----

    /**
     * Fragment count limit for polling subscriptions.
     * Keeps per-poll latency bounded (OCP: change this value without
     * modifying receiver logic).
     */
    public static final int FRAGMENT_COUNT_LIMIT = 10;

    /**
     * Maximum back-pressure retry attempts before returning failure.
     */
    public static final int MAX_BACK_PRESSURE_RETRIES = 3;

    // ---- Retry / Backoff / Circuit Breaker constants ----

    /**
     * Initial backoff duration in nanoseconds for exponential retry.
     * Default: 100 microseconds.
     */
    public static final long INITIAL_BACKOFF_NS = 100_000L;

    /**
     * Maximum backoff duration in nanoseconds for exponential retry.
     * Default: 10 milliseconds. Caps the exponential growth.
     */
    public static final long MAX_BACKOFF_NS = 10_000_000L;

    /**
     * Number of consecutive send failures that opens the circuit breaker.
     * When open, {@code send()} returns -1 immediately until the cooldown expires.
     * Default: 50 consecutive failures.
     */
    public static final int CIRCUIT_BREAKER_THRESHOLD = 50;

    /**
     * Cooldown period in nanoseconds while the circuit breaker is open.
     * After this duration, the circuit breaker transitions to half-open and
     * allows a single send attempt. Default: 1 second.
     */
    public static final long CIRCUIT_BREAKER_COOLDOWN_NS = 1_000_000_000L;

    // ---- ReplayMerge channel templates ----

    /**
     * Multi-destination subscription channel for {@link io.aeron.archive.client.ReplayMerge}.
     * Must use {@code control-mode=manual} so destinations can be added/removed dynamically.
     */
    public static final String MDS_CHANNEL = "aeron:udp?control-mode=manual";

    /**
     * Replay destination with ephemeral port. The OS assigns an available port;
     * {@link io.aeron.archive.client.ReplayMerge} resolves it before starting replay.
     */
    public static final String REPLAY_DESTINATION = "aeron:udp?endpoint=localhost:0";

    /**
     * Replay channel template for the Archive to use when sending replay data.
     */
    public static final String REPLAY_CHANNEL = "aeron:udp?endpoint=localhost:0";

    // ---- Archive control channel (required for Archive.Context) ----

    /**
     * UDP control channel for the Aeron Archive control protocol.
     * <b>Assumption:</b> Port 8010 is the standard Aeron Archive control port.
     */
    public static final String ARCHIVE_CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:8010";

    /**
     * Replication channel for the Archive.
     */
    public static final String ARCHIVE_REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";

    private BridgeConfiguration()
    {
    }
}
