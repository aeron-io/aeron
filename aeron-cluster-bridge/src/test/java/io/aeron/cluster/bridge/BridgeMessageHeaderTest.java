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

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link BridgeMessageHeader}.
 * <p>
 * <b>Test Strategy:</b>
 * Tests are organized by functionality:
 * <ul>
 *   <li>Encoding: Verifies header fields are written correctly</li>
 *   <li>Decoding: Verifies header fields are read correctly</li>
 *   <li>Symmetry: Verifies encode/decode round-trip</li>
 *   <li>Edge cases: Boundary values, zero, max</li>
 * </ul>
 */
@DisplayName("BridgeMessageHeader Unit Tests")
class BridgeMessageHeaderTest
{
    private UnsafeBuffer buffer;

    @BeforeEach
    void setUp()
    {
        buffer = new UnsafeBuffer(new byte[BridgeMessageHeader.HEADER_SIZE + 100]);
    }

    @Nested
    @DisplayName("Encoding Tests")
    class EncodingTests
    {
        @Test
        @DisplayName("should encode all header fields at offset 0")
        void shouldEncodeAllFieldsAtOffsetZero()
        {
            final long sequence = 12345L;
            final long timestamp = 9876543210L;
            final int msgType = BridgeConfiguration.MSG_TYPE_HEARTBEAT;
            final int length = 256;

            BridgeMessageHeader.encode(buffer, 0, sequence, timestamp, msgType, length);

            assertEquals(sequence, buffer.getLong(BridgeMessageHeader.OFFSET_SEQUENCE));
            assertEquals(timestamp, buffer.getLong(BridgeMessageHeader.OFFSET_TIMESTAMP));
            assertEquals(msgType, buffer.getInt(BridgeMessageHeader.OFFSET_MSG_TYPE));
            assertEquals(length, buffer.getInt(BridgeMessageHeader.OFFSET_LENGTH));
        }

        @Test
        @DisplayName("should encode at non-zero offset")
        void shouldEncodeAtNonZeroOffset()
        {
            final int offset = 16;
            final long sequence = 999L;

            BridgeMessageHeader.encode(buffer, offset, sequence, 0L, 0, 0);

            assertEquals(sequence, buffer.getLong(offset + BridgeMessageHeader.OFFSET_SEQUENCE));
        }

        @Test
        @DisplayName("should encode zero values")
        void shouldEncodeZeroValues()
        {
            BridgeMessageHeader.encode(buffer, 0, 0L, 0L, 0, 0);

            assertEquals(0L, buffer.getLong(BridgeMessageHeader.OFFSET_SEQUENCE));
            assertEquals(0L, buffer.getLong(BridgeMessageHeader.OFFSET_TIMESTAMP));
            assertEquals(0, buffer.getInt(BridgeMessageHeader.OFFSET_MSG_TYPE));
            assertEquals(0, buffer.getInt(BridgeMessageHeader.OFFSET_LENGTH));
        }

        @Test
        @DisplayName("should encode max values")
        void shouldEncodeMaxValues()
        {
            BridgeMessageHeader.encode(buffer, 0, Long.MAX_VALUE, Long.MAX_VALUE,
                Integer.MAX_VALUE, Integer.MAX_VALUE);

            assertEquals(Long.MAX_VALUE, BridgeMessageHeader.sequence(buffer, 0));
            assertEquals(Long.MAX_VALUE, BridgeMessageHeader.timestamp(buffer, 0));
            assertEquals(Integer.MAX_VALUE, BridgeMessageHeader.msgType(buffer, 0));
            assertEquals(Integer.MAX_VALUE, BridgeMessageHeader.payloadLength(buffer, 0));
        }
    }

    @Nested
    @DisplayName("Decoding Tests")
    class DecodingTests
    {
        @Test
        @DisplayName("should decode sequence number")
        void shouldDecodeSequence()
        {
            final long expected = 42L;
            BridgeMessageHeader.encode(buffer, 0, expected, 0L, 0, 0);

            assertEquals(expected, BridgeMessageHeader.sequence(buffer, 0));
        }

        @Test
        @DisplayName("should decode timestamp")
        void shouldDecodeTimestamp()
        {
            final long expected = System.nanoTime();
            BridgeMessageHeader.encode(buffer, 0, 0L, expected, 0, 0);

            assertEquals(expected, BridgeMessageHeader.timestamp(buffer, 0));
        }

        @Test
        @DisplayName("should decode message type")
        void shouldDecodeMsgType()
        {
            final int expected = BridgeConfiguration.MSG_TYPE_ORDER_NEW;
            BridgeMessageHeader.encode(buffer, 0, 0L, 0L, expected, 0);

            assertEquals(expected, BridgeMessageHeader.msgType(buffer, 0));
        }

        @Test
        @DisplayName("should decode payload length")
        void shouldDecodePayloadLength()
        {
            final int expected = 512;
            BridgeMessageHeader.encode(buffer, 0, 0L, 0L, 0, expected);

            assertEquals(expected, BridgeMessageHeader.payloadLength(buffer, 0));
        }

        @Test
        @DisplayName("should calculate total length correctly")
        void shouldCalculateTotalLength()
        {
            final int payloadLen = 100;
            BridgeMessageHeader.encode(buffer, 0, 0L, 0L, 0, payloadLen);

            assertEquals(BridgeMessageHeader.HEADER_SIZE + payloadLen,
                BridgeMessageHeader.totalLength(buffer, 0));
        }
    }

    @Nested
    @DisplayName("Round-Trip Tests")
    class RoundTripTests
    {
        @Test
        @DisplayName("should round-trip all fields correctly")
        void shouldRoundTripAllFields()
        {
            final long sequence = 123456789L;
            final long timestamp = 987654321L;
            final int msgType = BridgeConfiguration.MSG_TYPE_RISK_CHECK_REQUEST;
            final int length = 1024;

            BridgeMessageHeader.encode(buffer, 0, sequence, timestamp, msgType, length);

            assertEquals(sequence, BridgeMessageHeader.sequence(buffer, 0));
            assertEquals(timestamp, BridgeMessageHeader.timestamp(buffer, 0));
            assertEquals(msgType, BridgeMessageHeader.msgType(buffer, 0));
            assertEquals(length, BridgeMessageHeader.payloadLength(buffer, 0));
        }

        @Test
        @DisplayName("should round-trip with multiple messages")
        void shouldRoundTripMultipleMessages()
        {
            // Simulate multiple messages in buffer
            final int[] offsets = {0, 50, 100};
            final long[] sequences = {1L, 2L, 3L};

            for (int i = 0; i < offsets.length; i++)
            {
                BridgeMessageHeader.encode(buffer, offsets[i], sequences[i], 0L, 0, 0);
            }

            for (int i = 0; i < offsets.length; i++)
            {
                assertEquals(sequences[i], BridgeMessageHeader.sequence(buffer, offsets[i]));
            }
        }
    }

    @Nested
    @DisplayName("Format Tests")
    class FormatTests
    {
        @Test
        @DisplayName("should format header for logging")
        void shouldFormatHeader()
        {
            BridgeMessageHeader.encode(buffer, 0, 1L, 2L, 3, 4);

            final String formatted = BridgeMessageHeader.format(buffer, 0);

            assertTrue(formatted.contains("seq=1"));
            assertTrue(formatted.contains("ts=2"));
            assertTrue(formatted.contains("type=3"));
            assertTrue(formatted.contains("len=4"));
        }
    }

    @Nested
    @DisplayName("Constants Tests")
    class ConstantsTests
    {
        @Test
        @DisplayName("header size should be 24 bytes")
        void headerSizeShouldBe24Bytes()
        {
            assertEquals(24, BridgeMessageHeader.HEADER_SIZE);
        }

        @Test
        @DisplayName("offsets should be sequential")
        void offsetsShouldBeSequential()
        {
            assertEquals(0, BridgeMessageHeader.OFFSET_SEQUENCE);
            assertEquals(8, BridgeMessageHeader.OFFSET_TIMESTAMP);
            assertEquals(16, BridgeMessageHeader.OFFSET_MSG_TYPE);
            assertEquals(20, BridgeMessageHeader.OFFSET_LENGTH);
            assertEquals(24, BridgeMessageHeader.OFFSET_PAYLOAD);
        }
    }
}
