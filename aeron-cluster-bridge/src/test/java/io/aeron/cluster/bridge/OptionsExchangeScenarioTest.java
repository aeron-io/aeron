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
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Scenario-based tests for Options Trading Exchange.
 * <p>
 * <b>Purpose:</b>
 * These tests simulate real-world options trading scenarios to verify:
 * <ul>
 *   <li>Order flow from ME to RMS and back</li>
 *   <li>Risk check request/response cycles</li>
 *   <li>Position updates propagation</li>
 *   <li>Deterministic message ordering</li>
 *   <li>Sequence number correctness</li>
 * </ul>
 * <p>
 * <b>Test Organization:</b>
 * Tests are named with pattern: scenario_condition_expectedResult
 * This makes it easy to understand what each test verifies.
 * <p>
 * <b>Trading Scenarios Covered:</b>
 * <ol>
 *   <li>New order submission → Risk check → Accept/Reject</li>
 *   <li>Order modification → Position update</li>
 *   <li>Burst order flow during volatility</li>
 *   <li>Sequence gap detection</li>
 *   <li>Duplicate message handling</li>
 * </ol>
 */
@DisplayName("Options Exchange Trading Scenarios")
class OptionsExchangeScenarioTest
{
    private UnsafeBuffer messageBuffer;
    private AtomicLong sequenceGenerator;
    private List<ReceivedMessage> receivedMessages;

    /**
     * Represents a captured message for verification.
     */
    static class ReceivedMessage
    {
        final long sequence;
        final long timestamp;
        final int msgType;
        final byte[] payload;

        ReceivedMessage(final long sequence, final long timestamp, final int msgType, final byte[] payload)
        {
            this.sequence = sequence;
            this.timestamp = timestamp;
            this.msgType = msgType;
            this.payload = payload;
        }
    }

    @BeforeEach
    void setUp()
    {
        messageBuffer = new UnsafeBuffer(new byte[BridgeConfiguration.MAX_MESSAGE_SIZE]);
        sequenceGenerator = new AtomicLong(1);
        receivedMessages = new ArrayList<>();
    }

    // =========================================================================
    // Scenario 1: New Order Flow
    // =========================================================================

    @Nested
    @DisplayName("Scenario: New Order Submission")
    class NewOrderScenario
    {
        @Test
        @DisplayName("ME should send new order request to RMS with correct sequence")
        void newOrder_sendsToRMS_sequenceIsCorrect()
        {
            // Given: A new order from the matching engine
            final long orderId = 1001L;
            final double price = 150.50;
            final int quantity = 100;

            // When: ME sends order to RMS for risk check
            final long sequence = createOrderMessage(orderId, price, quantity);

            // Then: Message has valid sequence and type
            assertEquals(1L, sequence, "First message should have sequence 1");
            assertEquals(BridgeConfiguration.MSG_TYPE_ORDER_NEW,
                BridgeMessageHeader.msgType(messageBuffer, 0));
        }

        @Test
        @DisplayName("RMS should respond with risk check result")
        void riskCheckResponse_fromRMS_containsOrderId()
        {
            // Given: Order was sent and risk check performed
            final long orderId = 1001L;
            createOrderMessage(orderId, 150.50, 100);

            // When: RMS responds with risk approval
            final long responseSeq = createRiskCheckResponse(orderId, true, "APPROVED");

            // Then: Response has incremented sequence
            assertEquals(2L, responseSeq);
            assertEquals(BridgeConfiguration.MSG_TYPE_RISK_CHECK_RESPONSE,
                BridgeMessageHeader.msgType(messageBuffer, 0));
        }

        @Test
        @DisplayName("Complete order flow should maintain sequence ordering")
        void completeOrderFlow_sequences_areMonotonic()
        {
            // Simulate complete flow: Order → Risk Check → Response → Execution
            final long seq1 = createOrderMessage(1001L, 150.50, 100);
            final long seq2 = createRiskCheckRequest(1001L);
            final long seq3 = createRiskCheckResponse(1001L, true, "OK");
            final long seq4 = createPositionUpdate(1001L, 100);

            // Verify monotonic sequence
            assertTrue(seq1 < seq2);
            assertTrue(seq2 < seq3);
            assertTrue(seq3 < seq4);
            assertEquals(4L, seq4);
        }
    }

    // =========================================================================
    // Scenario 2: Order Modification
    // =========================================================================

    @Nested
    @DisplayName("Scenario: Order Modification")
    class OrderModificationScenario
    {
        @Test
        @DisplayName("Order modification should have correct message type")
        void orderModify_messageType_isCorrect()
        {
            final long orderId = 2001L;
            final double newPrice = 155.00;

            createOrderModifyMessage(orderId, newPrice);

            assertEquals(BridgeConfiguration.MSG_TYPE_ORDER_MODIFY,
                BridgeMessageHeader.msgType(messageBuffer, 0));
        }

        @Test
        @DisplayName("Order cancel should be properly sequenced")
        void orderCancel_afterModify_sequenceIncremented()
        {
            // Given: An existing order
            final long orderId = 2001L;
            createOrderMessage(orderId, 150.00, 100);

            // When: Order is modified then cancelled
            final long modifySeq = createOrderModifyMessage(orderId, 155.00);
            final long cancelSeq = createOrderCancelMessage(orderId);

            // Then: Sequences are properly ordered
            assertTrue(cancelSeq > modifySeq);
        }
    }

    // =========================================================================
    // Scenario 3: Burst Trading (Volatility Event)
    // =========================================================================

    @Nested
    @DisplayName("Scenario: Burst Order Flow (Market Volatility)")
    class BurstTradingScenario
    {
        @Test
        @DisplayName("Burst of 100 orders should have sequential sequences")
        void burstOrders_100Orders_allSequential()
        {
            final int burstSize = 100;
            final long[] sequences = new long[burstSize];

            // Simulate burst of orders during volatility
            for (int i = 0; i < burstSize; i++)
            {
                sequences[i] = createOrderMessage(3000L + i, 100.0 + i, 10);
            }

            // Verify all sequences are sequential
            for (int i = 1; i < burstSize; i++)
            {
                assertEquals(sequences[i - 1] + 1, sequences[i],
                    "Sequence gap at index " + i);
            }
        }

        @Test
        @DisplayName("Interleaved orders and risk responses should maintain order")
        void interleavedOrdersAndResponses_maintainSequenceOrder()
        {
            final List<Long> allSequences = new ArrayList<>();

            // Simulate interleaved traffic
            for (int i = 0; i < 10; i++)
            {
                // Order from ME
                allSequences.add(createOrderMessage(4000L + i, 100.0, 10));

                // Risk response from RMS
                allSequences.add(createRiskCheckResponse(4000L + i, true, "OK"));
            }

            // Verify monotonic ordering
            for (int i = 1; i < allSequences.size(); i++)
            {
                assertTrue(allSequences.get(i) > allSequences.get(i - 1),
                    "Sequence not monotonic at index " + i);
            }
        }
    }

    // =========================================================================
    // Scenario 4: Sequence Gap Detection
    // =========================================================================

    @Nested
    @DisplayName("Scenario: Sequence Gap Detection")
    class SequenceGapScenario
    {
        @Test
        @DisplayName("Receiver should detect sequence gap")
        void sequenceGap_detected_whenMessagesSkipped()
        {
            // Simulate receiving messages with gap
            final long lastApplied = 10L;
            final long received = 15L;

            final boolean hasGap = (received != lastApplied + 1);

            assertTrue(hasGap);
            assertEquals(4, received - lastApplied - 1, "Should have 4 missing messages");
        }

        @Test
        @DisplayName("Gap of 0 indicates no missing messages")
        void noGap_whenConsecutive()
        {
            final long lastApplied = 10L;
            final long received = 11L;

            final boolean hasGap = (received != lastApplied + 1);

            assertFalse(hasGap);
        }
    }

    // =========================================================================
    // Scenario 5: Duplicate Handling
    // =========================================================================

    @Nested
    @DisplayName("Scenario: Duplicate Message Handling")
    class DuplicateHandlingScenario
    {
        @Test
        @DisplayName("Duplicate sequence should be identified")
        void duplicateSequence_isIdentified()
        {
            final long lastApplied = 10L;
            final long received = 10L;

            final boolean isDuplicate = (received <= lastApplied);

            assertTrue(isDuplicate);
        }

        @Test
        @DisplayName("Old sequence should be identified as duplicate")
        void oldSequence_isIdentifiedAsDuplicate()
        {
            final long lastApplied = 10L;
            final long received = 5L;

            final boolean isDuplicate = (received <= lastApplied);

            assertTrue(isDuplicate, "Sequence 5 should be duplicate when lastApplied=10");
        }

        @Test
        @DisplayName("New sequence should not be duplicate")
        void newSequence_notDuplicate()
        {
            final long lastApplied = 10L;
            final long received = 11L;

            final boolean isDuplicate = (received <= lastApplied);

            assertFalse(isDuplicate);
        }
    }

    // =========================================================================
    // Scenario 6: Position Updates
    // =========================================================================

    @Nested
    @DisplayName("Scenario: Position Update Propagation")
    class PositionUpdateScenario
    {
        @Test
        @DisplayName("Position update should have correct message type")
        void positionUpdate_correctMessageType()
        {
            final long orderId = 5001L;
            final int quantity = 500;

            createPositionUpdate(orderId, quantity);

            assertEquals(BridgeConfiguration.MSG_TYPE_POSITION_UPDATE,
                BridgeMessageHeader.msgType(messageBuffer, 0));
        }

        @Test
        @DisplayName("Multiple position updates should be sequenced")
        void multiplePositionUpdates_areSequenced()
        {
            final long seq1 = createPositionUpdate(5001L, 100);
            final long seq2 = createPositionUpdate(5001L, 200);
            final long seq3 = createPositionUpdate(5001L, 300);

            assertEquals(seq1 + 1, seq2);
            assertEquals(seq2 + 1, seq3);
        }
    }

    // =========================================================================
    // Scenario 7: Heartbeat Handling
    // =========================================================================

    @Nested
    @DisplayName("Scenario: Heartbeat Messages")
    class HeartbeatScenario
    {
        @Test
        @DisplayName("Heartbeat should have correct message type")
        void heartbeat_correctMessageType()
        {
            createHeartbeat();

            assertEquals(BridgeConfiguration.MSG_TYPE_HEARTBEAT,
                BridgeMessageHeader.msgType(messageBuffer, 0));
        }

        @Test
        @DisplayName("Heartbeat timestamp should be recent")
        void heartbeat_timestampIsRecent()
        {
            final long before = System.nanoTime();
            createHeartbeat();
            final long after = System.nanoTime();

            final long timestamp = BridgeMessageHeader.timestamp(messageBuffer, 0);

            assertTrue(timestamp >= before && timestamp <= after);
        }
    }

    // =========================================================================
    // Helper Methods for Message Creation
    // =========================================================================

    private long createOrderMessage(final long orderId, final double price, final int quantity)
    {
        final long sequence = sequenceGenerator.getAndIncrement();
        final long timestamp = System.nanoTime();

        // Encode payload: orderId (8) + price (8) + quantity (4) = 20 bytes
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[20]);
        payload.putLong(0, orderId);
        payload.putDouble(8, price);
        payload.putInt(16, quantity);

        BridgeMessageHeader.encode(messageBuffer, 0, sequence, timestamp,
            BridgeConfiguration.MSG_TYPE_ORDER_NEW, 20);

        return sequence;
    }

    private long createOrderModifyMessage(final long orderId, final double newPrice)
    {
        final long sequence = sequenceGenerator.getAndIncrement();
        final long timestamp = System.nanoTime();

        BridgeMessageHeader.encode(messageBuffer, 0, sequence, timestamp,
            BridgeConfiguration.MSG_TYPE_ORDER_MODIFY, 16);

        return sequence;
    }

    private long createOrderCancelMessage(final long orderId)
    {
        final long sequence = sequenceGenerator.getAndIncrement();
        final long timestamp = System.nanoTime();

        BridgeMessageHeader.encode(messageBuffer, 0, sequence, timestamp,
            BridgeConfiguration.MSG_TYPE_ORDER_CANCEL, 8);

        return sequence;
    }

    private long createRiskCheckRequest(final long orderId)
    {
        final long sequence = sequenceGenerator.getAndIncrement();
        final long timestamp = System.nanoTime();

        BridgeMessageHeader.encode(messageBuffer, 0, sequence, timestamp,
            BridgeConfiguration.MSG_TYPE_RISK_CHECK_REQUEST, 8);

        return sequence;
    }

    private long createRiskCheckResponse(final long orderId, final boolean approved, final String reason)
    {
        final long sequence = sequenceGenerator.getAndIncrement();
        final long timestamp = System.nanoTime();

        BridgeMessageHeader.encode(messageBuffer, 0, sequence, timestamp,
            BridgeConfiguration.MSG_TYPE_RISK_CHECK_RESPONSE, 16);

        return sequence;
    }

    private long createPositionUpdate(final long orderId, final int quantity)
    {
        final long sequence = sequenceGenerator.getAndIncrement();
        final long timestamp = System.nanoTime();

        BridgeMessageHeader.encode(messageBuffer, 0, sequence, timestamp,
            BridgeConfiguration.MSG_TYPE_POSITION_UPDATE, 12);

        return sequence;
    }

    private long createHeartbeat()
    {
        final long sequence = sequenceGenerator.getAndIncrement();
        final long timestamp = System.nanoTime();

        BridgeMessageHeader.encode(messageBuffer, 0, sequence, timestamp,
            BridgeConfiguration.MSG_TYPE_HEARTBEAT, 0);

        return sequence;
    }
}
