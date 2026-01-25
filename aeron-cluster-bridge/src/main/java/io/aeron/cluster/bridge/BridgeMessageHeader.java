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
import org.agrona.MutableDirectBuffer;

/**
 * Zero-allocation codec for bridge message headers.
 * <p>
 * <b>Design Rationale (Performance - Zero-Allocation):</b>
 * This class uses static methods operating directly on buffers rather than
 * creating intermediate objects. This is critical for low-latency paths where
 * garbage collection pauses must be minimized.
 * <p>
 * <b>Design Rationale (SOLID - Single Responsibility):</b>
 * This class handles only message header encoding/decoding. Payload handling
 * is left to the application layer.
 * <p>
 * <b>Design Rationale (SOLID - Open/Closed):</b>
 * The header format is versioned through the layout. New fields can be added
 * at the end without breaking existing readers (forward compatibility).
 * <p>
 * Message layout (24 bytes total):
 * <pre>
 * ┌──────────────────────────────────────────────────────────────────┐
 * │                     Bridge Message Header                         │
 * ├──────────┬──────────┬──────────┬──────────┬──────────────────────┤
 * │ Offset   │ Size     │ Type     │ Field    │ Description          │
 * ├──────────┼──────────┼──────────┼──────────┼──────────────────────┤
 * │ 0        │ 8 bytes  │ long     │ sequence │ Monotonic sequence # │
 * │ 8        │ 8 bytes  │ long     │ timestamp│ Epoch nanoseconds    │
 * │ 16       │ 4 bytes  │ int      │ msgType  │ Message type ID      │
 * │ 20       │ 4 bytes  │ int      │ length   │ Payload length       │
 * │ 24       │ N bytes  │ byte[]   │ payload  │ Application data     │
 * └──────────┴──────────┴──────────┴──────────┴──────────────────────┘
 * </pre>
 * <p>
 * <b>Assumptions:</b>
 * <ul>
 *   <li>Sequence numbers are monotonically increasing per direction</li>
 *   <li>Timestamps use System.nanoTime() for ordering (not wall-clock time)</li>
 *   <li>Message types are defined in {@link BridgeConfiguration}</li>
 *   <li>Payload length does not include header size</li>
 * </ul>
 */
public final class BridgeMessageHeader
{
    /** Byte offset of the sequence number field */
    public static final int OFFSET_SEQUENCE = 0;

    /** Byte offset of the timestamp field */
    public static final int OFFSET_TIMESTAMP = 8;

    /** Byte offset of the message type field */
    public static final int OFFSET_MSG_TYPE = 16;

    /** Byte offset of the payload length field */
    public static final int OFFSET_LENGTH = 20;

    /** Byte offset where payload begins */
    public static final int OFFSET_PAYLOAD = 24;

    /** Total size of the header in bytes */
    public static final int HEADER_SIZE = 24;

    private BridgeMessageHeader()
    {
        // Static utility class - prevent instantiation
    }

    /**
     * Encode the message header into the buffer.
     * <p>
     * <b>Performance Note:</b> This method performs 4 primitive writes with no
     * object allocations. Expected latency: &lt; 100 nanoseconds.
     *
     * @param buffer    the target buffer (must have at least HEADER_SIZE bytes available)
     * @param offset    the starting offset in the buffer
     * @param sequence  the message sequence number (must be positive, monotonically increasing)
     * @param timestamp the timestamp in nanoseconds (typically from System.nanoTime())
     * @param msgType   the message type (see BridgeConfiguration.MSG_TYPE_* constants)
     * @param length    the payload length in bytes (0 for header-only messages)
     */
    public static void encode(
        final MutableDirectBuffer buffer,
        final int offset,
        final long sequence,
        final long timestamp,
        final int msgType,
        final int length)
    {
        buffer.putLong(offset + OFFSET_SEQUENCE, sequence);
        buffer.putLong(offset + OFFSET_TIMESTAMP, timestamp);
        buffer.putInt(offset + OFFSET_MSG_TYPE, msgType);
        buffer.putInt(offset + OFFSET_LENGTH, length);
    }

    /**
     * Get the sequence number from the buffer.
     * <p>
     * <b>Design Note:</b> Sequence numbers are used for:
     * <ul>
     *   <li>Ordering verification</li>
     *   <li>Duplicate detection</li>
     *   <li>Gap detection during recovery</li>
     * </ul>
     *
     * @param buffer the source buffer
     * @param offset the starting offset in the buffer
     * @return the sequence number
     */
    public static long sequence(final DirectBuffer buffer, final int offset)
    {
        return buffer.getLong(offset + OFFSET_SEQUENCE);
    }

    /**
     * Get the timestamp from the buffer.
     * <p>
     * <b>Assumption:</b> Timestamps are relative (nanoTime), not absolute wall-clock.
     * They're useful for latency measurement, not event timing across systems.
     *
     * @param buffer the source buffer
     * @param offset the starting offset in the buffer
     * @return the timestamp in nanoseconds
     */
    public static long timestamp(final DirectBuffer buffer, final int offset)
    {
        return buffer.getLong(offset + OFFSET_TIMESTAMP);
    }

    /**
     * Get the message type from the buffer.
     *
     * @param buffer the source buffer
     * @param offset the starting offset in the buffer
     * @return the message type identifier
     */
    public static int msgType(final DirectBuffer buffer, final int offset)
    {
        return buffer.getInt(offset + OFFSET_MSG_TYPE);
    }

    /**
     * Get the payload length from the buffer.
     * <p>
     * <b>Note:</b> This is the payload length only, not including the header.
     *
     * @param buffer the source buffer
     * @param offset the starting offset in the buffer
     * @return the payload length in bytes
     */
    public static int payloadLength(final DirectBuffer buffer, final int offset)
    {
        return buffer.getInt(offset + OFFSET_LENGTH);
    }

    /**
     * Get the total message length (header + payload).
     *
     * @param buffer the source buffer
     * @param offset the starting offset in the buffer
     * @return the total message length in bytes
     */
    public static int totalLength(final DirectBuffer buffer, final int offset)
    {
        return HEADER_SIZE + payloadLength(buffer, offset);
    }

    /**
     * Format the header as a human-readable string for logging/debugging.
     * <p>
     * <b>Warning:</b> This method allocates a String. Do not use in hot paths.
     *
     * @param buffer the source buffer
     * @param offset the starting offset in the buffer
     * @return formatted string representation
     */
    public static String format(final DirectBuffer buffer, final int offset)
    {
        return String.format(
            "seq=%d, ts=%d, type=%d, len=%d",
            sequence(buffer, offset),
            timestamp(buffer, offset),
            msgType(buffer, offset),
            payloadLength(buffer, offset));
    }
}
