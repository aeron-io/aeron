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
 * Zero-allocation binary codec for bridge messages.
 * <p>
 * <b>Design choice (SRP):</b> This class is purely a stateless codec — it
 * encodes and decodes fields from a buffer without owning any state. This
 * makes it safe to call from any thread and trivially testable.
 * <p>
 * <b>Design choice (no SBE):</b> The 28-byte header is simple enough that
 * a hand-rolled codec using {@link org.agrona.concurrent.UnsafeBuffer} is
 * sufficient and avoids adding an SBE code-generation step. If the protocol
 * evolves beyond a few more fields, migrating to SBE is recommended.
 * <p>
 * <b>Wire format:</b>
 * <pre>
 * Offset  Size  Field
 * ------  ----  -----
 * 0       4     magic (0x42524447 = "BRDG")
 * 4       1     version (1)
 * 5       1     direction (0 = ME_TO_RMS, 1 = RMS_TO_ME)
 * 6       2     reserved (alignment padding)
 * 8       8     sequence (1-based, monotonically increasing per direction)
 * 16      8     timestampNs (System.nanoTime() at send time)
 * 24      4     payloadLength
 * 28      N     payload bytes
 * </pre>
 * <p>
 * <b>Assumption:</b> All multi-byte fields use native byte order (little-endian
 * on x86). Both sides of the bridge are assumed to run on the same architecture.
 */
public final class BridgeMessageCodec
{
    /**
     * Magic bytes identifying a bridge message: "BRDG" = 0x42524447.
     */
    public static final int MAGIC = 0x4252_4447;

    /**
     * Current protocol version.
     */
    public static final byte VERSION = 1;

    /**
     * Offset of the magic field.
     */
    public static final int MAGIC_OFFSET = 0;

    /**
     * Offset of the version field.
     */
    public static final int VERSION_OFFSET = 4;

    /**
     * Offset of the direction field.
     */
    public static final int DIRECTION_OFFSET = 5;

    /**
     * Offset of the sequence field.
     */
    public static final int SEQUENCE_OFFSET = 8;

    /**
     * Offset of the timestamp field.
     */
    public static final int TIMESTAMP_OFFSET = 16;

    /**
     * Offset of the payload length field.
     */
    public static final int PAYLOAD_LENGTH_OFFSET = 24;

    /**
     * Total header size in bytes.
     */
    public static final int HEADER_LENGTH = 28;

    private BridgeMessageCodec()
    {
    }

    /**
     * Encode a bridge message into the given buffer at the specified offset.
     * <p>
     * <b>Hot-path note:</b> No allocations. All writes go directly to the
     * provided buffer.
     *
     * @param buffer        mutable buffer to write into.
     * @param offset        offset within the buffer.
     * @param direction     message direction (0=ME_TO_RMS, 1=RMS_TO_ME).
     * @param sequence      monotonically increasing sequence number.
     * @param timestampNs   send timestamp in nanoseconds.
     * @param payload       payload data buffer.
     * @param payloadOffset offset within the payload buffer.
     * @param payloadLength length of the payload in bytes.
     * @return total encoded length (header + payload).
     */
    public static int encode(
        final MutableDirectBuffer buffer,
        final int offset,
        final int direction,
        final long sequence,
        final long timestampNs,
        final DirectBuffer payload,
        final int payloadOffset,
        final int payloadLength)
    {
        buffer.putInt(offset + MAGIC_OFFSET, MAGIC);
        buffer.putByte(offset + VERSION_OFFSET, VERSION);
        buffer.putByte(offset + DIRECTION_OFFSET, (byte)direction);
        buffer.putShort(offset + VERSION_OFFSET + 2, (short)0); // reserved padding
        buffer.putLong(offset + SEQUENCE_OFFSET, sequence);
        buffer.putLong(offset + TIMESTAMP_OFFSET, timestampNs);
        buffer.putInt(offset + PAYLOAD_LENGTH_OFFSET, payloadLength);

        if (payloadLength > 0)
        {
            buffer.putBytes(offset + HEADER_LENGTH, payload, payloadOffset, payloadLength);
        }

        return HEADER_LENGTH + payloadLength;
    }

    /**
     * Read the magic field from a message.
     *
     * @param buffer buffer containing the message.
     * @param offset offset of the message start.
     * @return the magic value.
     */
    public static int magic(final DirectBuffer buffer, final int offset)
    {
        return buffer.getInt(offset + MAGIC_OFFSET);
    }

    /**
     * Read the direction field from a message.
     *
     * @param buffer buffer containing the message.
     * @param offset offset of the message start.
     * @return the direction byte as unsigned int.
     */
    public static int direction(final DirectBuffer buffer, final int offset)
    {
        return buffer.getByte(offset + DIRECTION_OFFSET) & 0xFF;
    }

    /**
     * Read the sequence field from a message.
     *
     * @param buffer buffer containing the message.
     * @param offset offset of the message start.
     * @return the sequence number.
     */
    public static long sequence(final DirectBuffer buffer, final int offset)
    {
        return buffer.getLong(offset + SEQUENCE_OFFSET);
    }

    /**
     * Read the payload length field from a message.
     *
     * @param buffer buffer containing the message.
     * @param offset offset of the message start.
     * @return the payload length in bytes.
     */
    public static int payloadLength(final DirectBuffer buffer, final int offset)
    {
        return buffer.getInt(offset + PAYLOAD_LENGTH_OFFSET);
    }

    /**
     * Compute the offset where the payload begins.
     *
     * @param messageOffset offset of the message start.
     * @return offset of the payload.
     */
    public static int payloadOffset(final int messageOffset)
    {
        return messageOffset + HEADER_LENGTH;
    }

    /**
     * Validate that a buffer at the given offset contains a valid bridge message header.
     *
     * @param buffer buffer containing the message.
     * @param offset offset of the message start.
     * @param length total available length at the offset.
     * @return true if the header is valid.
     */
    public static boolean isValid(final DirectBuffer buffer, final int offset, final int length)
    {
        return length >= HEADER_LENGTH &&
            buffer.getInt(offset + MAGIC_OFFSET) == MAGIC &&
            buffer.getByte(offset + VERSION_OFFSET) == VERSION;
    }
}
