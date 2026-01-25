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
 * Configuration constants and utilities for the inter-cluster Bridge Service.
 * <p>
 * <b>Design Rationale (SOLID - Single Responsibility):</b>
 * This class has one responsibility: providing configuration values. It centralizes
 * all configuration in one place, making it easy to modify defaults and understand
 * the system's configurable parameters.
 * <p>
 * <b>Design Rationale (OOP - Encapsulation):</b>
 * Configuration values are accessed through static methods rather than public fields,
 * allowing for future changes to how values are resolved (e.g., adding validation,
 * caching, or alternative configuration sources).
 * <p>
 * <b>Assumptions:</b>
 * <ul>
 *   <li>System properties take precedence over default values</li>
 *   <li>Stream IDs 2001/2002 avoid conflicts with default Aeron samples (1001-1003)</li>
 *   <li>Channel endpoints use localhost for single-machine testing</li>
 *   <li>For production, channels should use multicast addresses (239.x.x.x)</li>
 * </ul>
 * <p>
 * Supports bidirectional communication between:
 * <ul>
 *   <li>Cluster A: Matching Engine (ME)</li>
 *   <li>Cluster B: Risk Management System (RMS)</li>
 * </ul>
 */
public final class BridgeConfiguration
{
    /**
     * Enumeration of message flow directions.
     * <p>
     * <b>Design Rationale (OOP - Encapsulation):</b>
     * Direction encapsulates both the semantic meaning and the technical details
     * (stream ID) in a single type-safe enum, preventing mismatched configurations.
     */
    public enum Direction
    {
        /** Messages flowing from Matching Engine to Risk Management System */
        ME_TO_RMS(1, 2001),

        /** Messages flowing from Risk Management System to Matching Engine */
        RMS_TO_ME(2, 2002);

        private final int code;
        private final int defaultStreamId;

        Direction(final int code, final int defaultStreamId)
        {
            this.code = code;
            this.defaultStreamId = defaultStreamId;
        }

        /**
         * Get the numeric code for this direction (used in checkpoint files).
         *
         * @return the direction code
         */
        public int code()
        {
            return code;
        }

        /**
         * Get the default stream ID for this direction.
         *
         * @return the default stream ID
         */
        public int defaultStreamId()
        {
            return defaultStreamId;
        }

        /**
         * Resolve a direction from its numeric code.
         *
         * @param code the direction code
         * @return the corresponding Direction enum value
         * @throws IllegalArgumentException if code is unknown
         */
        public static Direction fromCode(final int code)
        {
            for (final Direction d : values())
            {
                if (d.code == code)
                {
                    return d;
                }
            }
            throw new IllegalArgumentException("Unknown direction code: " + code);
        }
    }

    // ========================================================================
    // System Property Names
    // ========================================================================

    /** System property for message flow direction */
    public static final String DIRECTION_PROP = "bridge.direction";

    /** System property for Aeron stream ID */
    public static final String STREAM_ID_PROP = "bridge.stream.id";

    /** System property for Aeron channel URI */
    public static final String CHANNEL_PROP = "bridge.channel";

    /** System property for archive storage directory */
    public static final String ARCHIVE_DIR_PROP = "bridge.archive.dir";

    /** System property for checkpoint storage directory */
    public static final String CHECKPOINT_DIR_PROP = "bridge.checkpoint.dir";

    /** System property for number of messages to send */
    public static final String MESSAGE_COUNT_PROP = "bridge.message.count";

    /** System property for interval between messages in milliseconds */
    public static final String MESSAGE_INTERVAL_MS_PROP = "bridge.message.interval.ms";

    /** System property to enable/disable embedded media driver */
    public static final String EMBEDDED_DRIVER_PROP = "bridge.embedded.driver";

    /** System property to enable/disable replay-merge for recovery */
    public static final String REPLAY_MERGE_PROP = "bridge.replay.merge";

    /** System property for replay-merge timeout in milliseconds */
    public static final String MERGE_TIMEOUT_MS_PROP = "bridge.merge.timeout.ms";

    // ========================================================================
    // Default Values
    // ========================================================================

    /** Default direction: ME to RMS */
    public static final Direction DEFAULT_DIRECTION = Direction.ME_TO_RMS;

    /**
     * Default channel for ME to RMS traffic.
     * <p>
     * <b>Assumption:</b> Uses localhost for single-machine demo.
     * Production should use multicast: aeron:udp?endpoint=239.1.1.1:40456|interface=0.0.0.0
     */
    public static final String DEFAULT_CHANNEL_ME_TO_RMS = "aeron:udp?endpoint=localhost:40456";

    /**
     * Default channel for RMS to ME traffic.
     */
    public static final String DEFAULT_CHANNEL_RMS_TO_ME = "aeron:udp?endpoint=localhost:40457";

    /** Default archive directory relative to working directory */
    public static final String DEFAULT_ARCHIVE_DIR = "./build/aeron-archive";

    /** Default checkpoint directory relative to working directory */
    public static final String DEFAULT_CHECKPOINT_DIR = "./build/checkpoints";

    /** Default number of messages to send in demos */
    public static final int DEFAULT_MESSAGE_COUNT = 100;

    /** Default interval between messages in milliseconds */
    public static final int DEFAULT_MESSAGE_INTERVAL_MS = 10;

    /** Default: use embedded media driver for simplicity */
    public static final boolean DEFAULT_EMBEDDED_DRIVER = true;

    /** Default: enable replay-merge for deterministic recovery */
    public static final boolean DEFAULT_REPLAY_MERGE = true;

    /** Default timeout for replay-merge operation in milliseconds */
    public static final long DEFAULT_MERGE_TIMEOUT_MS = 10_000;

    // ========================================================================
    // Archive Control Channels (fixed values for local archive)
    // ========================================================================

    /** Archive control request channel */
    public static final String ARCHIVE_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8010";

    /** Archive control response channel */
    public static final String ARCHIVE_CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:8020";

    /** Archive control request stream ID */
    public static final int ARCHIVE_CONTROL_REQUEST_STREAM_ID = 10001;

    /** Archive control response stream ID */
    public static final int ARCHIVE_CONTROL_RESPONSE_STREAM_ID = 10002;

    /** Archive recording events stream ID */
    public static final int ARCHIVE_RECORDING_EVENTS_STREAM_ID = 10003;

    // ========================================================================
    // Message Type Constants
    // ========================================================================

    /** New order message type */
    public static final int MSG_TYPE_ORDER_NEW = 1;

    /** Order cancellation message type */
    public static final int MSG_TYPE_ORDER_CANCEL = 2;

    /** Order modification message type */
    public static final int MSG_TYPE_ORDER_MODIFY = 3;

    /** Risk check request message type */
    public static final int MSG_TYPE_RISK_CHECK_REQUEST = 10;

    /** Risk check response message type */
    public static final int MSG_TYPE_RISK_CHECK_RESPONSE = 11;

    /** Position update message type */
    public static final int MSG_TYPE_POSITION_UPDATE = 20;

    /** Heartbeat message type (used for testing and connectivity) */
    public static final int MSG_TYPE_HEARTBEAT = 99;

    // ========================================================================
    // Message Layout Constants
    // ========================================================================

    /**
     * Maximum message size including header and payload.
     * <p>
     * <b>Assumption:</b> 1024 bytes is sufficient for most trading messages.
     * Larger messages should be fragmented at the application level.
     */
    public static final int MAX_MESSAGE_SIZE = 1024;

    /** Size of the message header in bytes */
    public static final int HEADER_SIZE = 24;

    private BridgeConfiguration()
    {
        // Utility class - prevent instantiation
    }

    // ========================================================================
    // Configuration Accessor Methods
    // ========================================================================

    /**
     * Get the configured message flow direction.
     *
     * @return the direction from system property or default
     */
    public static Direction direction()
    {
        final String dirStr = System.getProperty(DIRECTION_PROP);
        if (null == dirStr || dirStr.isEmpty())
        {
            return DEFAULT_DIRECTION;
        }
        return Direction.valueOf(dirStr);
    }

    /**
     * Get the configured Aeron stream ID.
     *
     * @return the stream ID from system property or direction default
     */
    public static int streamId()
    {
        return Integer.getInteger(STREAM_ID_PROP, direction().defaultStreamId());
    }

    /**
     * Get the configured Aeron channel URI.
     *
     * @return the channel URI from system property or direction default
     */
    public static String channel()
    {
        final String channel = System.getProperty(CHANNEL_PROP);
        if (null != channel && !channel.isEmpty())
        {
            return channel;
        }
        return direction() == Direction.ME_TO_RMS ? DEFAULT_CHANNEL_ME_TO_RMS : DEFAULT_CHANNEL_RMS_TO_ME;
    }

    /**
     * Get the archive storage directory path.
     *
     * @return the archive directory path
     */
    public static String archiveDir()
    {
        return System.getProperty(ARCHIVE_DIR_PROP, DEFAULT_ARCHIVE_DIR);
    }

    /**
     * Get the checkpoint storage directory path.
     *
     * @return the checkpoint directory path
     */
    public static String checkpointDir()
    {
        return System.getProperty(CHECKPOINT_DIR_PROP, DEFAULT_CHECKPOINT_DIR);
    }

    /**
     * Get the number of messages to send in demo mode.
     *
     * @return the message count
     */
    public static int messageCount()
    {
        return Integer.getInteger(MESSAGE_COUNT_PROP, DEFAULT_MESSAGE_COUNT);
    }

    /**
     * Get the interval between messages in milliseconds.
     *
     * @return the message interval
     */
    public static int messageIntervalMs()
    {
        return Integer.getInteger(MESSAGE_INTERVAL_MS_PROP, DEFAULT_MESSAGE_INTERVAL_MS);
    }

    /**
     * Check if embedded media driver is enabled.
     * <p>
     * <b>Design Note:</b> Embedded driver simplifies deployment but may not be
     * suitable for production where a shared media driver is preferred.
     *
     * @return true if embedded driver should be used
     */
    public static boolean embeddedDriver()
    {
        return Boolean.parseBoolean(System.getProperty(EMBEDDED_DRIVER_PROP, String.valueOf(DEFAULT_EMBEDDED_DRIVER)));
    }

    /**
     * Check if replay-merge is enabled for recovery.
     * <p>
     * <b>Design Note:</b> ReplayMerge provides seamless transition from archive
     * replay to live stream. Disable only for debugging purposes.
     *
     * @return true if replay-merge should be used
     */
    public static boolean replayMerge()
    {
        return Boolean.parseBoolean(System.getProperty(REPLAY_MERGE_PROP, String.valueOf(DEFAULT_REPLAY_MERGE)));
    }

    /**
     * Get the replay-merge timeout in milliseconds.
     *
     * @return the merge timeout
     */
    public static long mergeTimeoutMs()
    {
        return Long.getLong(MERGE_TIMEOUT_MS_PROP, DEFAULT_MERGE_TIMEOUT_MS);
    }
}
