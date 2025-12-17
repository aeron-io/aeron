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

#include "aeron_driver_conductor_test.h"

#define COUNTER_LABEL "counter label"
#define COUNTER_TYPE_ID (102)
#define COUNTER_KEY_LENGTH (sizeof(int64_t) + 3)

using testing::_;

class DriverConductorCounterTest : public DriverConductorTest, public testing::Test
{
public:

protected:

    std::string m_label = COUNTER_LABEL;
    uint8_t m_key[COUNTER_KEY_LENGTH] = { 0 };
    size_t m_key_length = COUNTER_KEY_LENGTH;
};

TEST_F(DriverConductorCounterTest, shouldBeAbleToAddSingleCounter)
{
    int64_t client_id = 66;
    int64_t reg_id = nextCorrelationId();

    memcpy(m_key, &reg_id, sizeof(reg_id));
    ASSERT_EQ(addCounter(client_id, reg_id, COUNTER_TYPE_ID, m_key, m_key_length, m_label), 0);
    doWork();

    int32_t client_counter_id;
    int32_t counter_id;
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_COUNTER_READY, _, _))
        .With(IsCounterUpdate(client_id, _))
        .WillOnce(CaptureCounterId(&client_counter_id));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_COUNTER_READY, _, _))
        .With(IsCounterUpdate(reg_id, _))
        .WillOnce(CaptureCounterId(&counter_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);

    EXPECT_CALL(m_mockCallbacks, onCounter(_, _, _, _, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, onCounter(_, COUNTER_TYPE_ID, _, _, _, _)).
        With(IsIdCounter(reg_id, m_label));
    EXPECT_CALL(m_mockCallbacks, onCounter(_, AERON_COUNTER_CLIENT_HEARTBEAT_TIMESTAMP_TYPE_ID, _, _, _, _)).
        With(IsIdCounter(client_id, std::string("client-heartbeat: id=66")));
    readCounters(mock_counter_handler);
}

TEST_F(DriverConductorCounterTest, shouldRemoveSingleCounter)
{
    int64_t client_id = nextCorrelationId();
    int64_t reg_id = nextCorrelationId();

    ASSERT_EQ(addCounter(client_id, reg_id, COUNTER_TYPE_ID, m_key, m_key_length, m_label), 0);
    doWork();

    int32_t counter_id;
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_COUNTER_READY, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_COUNTER_READY, _, _))
        .With(IsCounterUpdate(reg_id, _))
        .WillOnce(CaptureCounterId(&counter_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);

    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    int64_t remove_correlation_id = nextCorrelationId();
    ASSERT_EQ(removeCounter(client_id, remove_correlation_id, reg_id), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_UNAVAILABLE_COUNTER, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
        .With(IsOperationSuccess(remove_correlation_id));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_UNAVAILABLE_COUNTER, _, _))
        .With(IsCounterUpdate(reg_id, counter_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorCounterTest, shouldRemoveCounterOnClientTimeout)
{
    int64_t client_id = nextCorrelationId();
    int64_t reg_id = nextCorrelationId();

    ASSERT_EQ(addCounter(client_id, reg_id, COUNTER_TYPE_ID, m_key, m_key_length, m_label), 0);
    doWork();

    int32_t counter_id;
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_COUNTER_READY, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_COUNTER_READY, _, _))
        .With(IsCounterUpdate(reg_id, _))
        .WillOnce(CaptureCounterId(&counter_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);

    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    doWorkForNs((int64_t)(m_context.m_context->client_liveness_timeout_ns * 2));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);

    EXPECT_CALL(m_mockCallbacks, onCounter(_, _, _, _, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, onCounter(counter_id, _, _, _, _, _)).Times(0);
    readCounters(mock_counter_handler);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_CLIENT_TIMEOUT, _, _))
        .With(IsTimeout(client_id));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_UNAVAILABLE_COUNTER, _, _))
        .With(IsCounterUpdate(reg_id, counter_id));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_UNAVAILABLE_COUNTER, _, _))
        .With(IsCounterUpdate(client_id, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorCounterTest, shouldRemoveMultipleCountersOnClientTimeout)
{
    int64_t client_id = nextCorrelationId();
    int64_t reg_id1 = nextCorrelationId();
    int64_t reg_id2 = nextCorrelationId();
    int total_client_plus_custom_counters = 3;

    ASSERT_EQ(addCounter(client_id, reg_id1, COUNTER_TYPE_ID, m_key, m_key_length, m_label), 0);
    ASSERT_EQ(addCounter(client_id, reg_id2, COUNTER_TYPE_ID, m_key, m_key_length, m_label), 0);
    doWork();
    doWork();

    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    doWorkForNs((int64_t)(m_context.m_context->client_liveness_timeout_ns * 2));

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_CLIENT_TIMEOUT, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_UNAVAILABLE_COUNTER, _, _))
        .Times(total_client_plus_custom_counters);

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorCounterTest, shouldRemoveMultipleCountersOnClientClose)
{
    test_set_nano_time(INT64_C(100) * 1000 * 1000 * 1000);

    int64_t client_id = nextCorrelationId();
    int64_t reg_id1 = nextCorrelationId();
    int64_t reg_id2 = nextCorrelationId();
    int total_client_plus_custom_counters = 3;

    ASSERT_EQ(addCounter(client_id, reg_id1, COUNTER_TYPE_ID, m_key, m_key_length, m_label), 0);
    ASSERT_EQ(addCounter(client_id, reg_id2, COUNTER_TYPE_ID, m_key, m_key_length, m_label), 0);
    doWork();

    closeClient(client_id);
    doWork();
    doWorkForNs((int64_t)(m_context.m_context->timer_interval_ns));

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_COUNTER_READY, _, _))
        .Times(total_client_plus_custom_counters);
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_UNAVAILABLE_COUNTER, _, _))
        .Times(total_client_plus_custom_counters);

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorCounterTest, shouldNotRemoveCounterOnClientKeepalive)
{
    int64_t client_id = nextCorrelationId();
    int64_t reg_id = nextCorrelationId();

    ASSERT_EQ(addCounter(client_id, reg_id, COUNTER_TYPE_ID, m_key, m_key_length, m_label), 0);
    doWork();

    int32_t counter_id;
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_COUNTER_READY, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_COUNTER_READY, _, _))
        .With(IsCounterUpdate(reg_id, _))
        .WillOnce(CaptureCounterId(&counter_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);

    int64_t timeout = (int64_t)m_context.m_context->client_liveness_timeout_ns * 2;

    doWorkForNs(
        timeout,
        100,
        [&]()
        {
            clientKeepalive(client_id);
        });

    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 1u);

    EXPECT_CALL(m_mockCallbacks, onCounter(_, _, _, _, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, onCounter(counter_id, _, _, _, _, _)).Times(1);
    readCounters(mock_counter_handler);
}

TEST_F(DriverConductorCounterTest, shouldIncrementCounterOnConductorThresholdExceeded)
{
    volatile int64_t *maxCycleTimeCounter = aeron_counters_manager_addr(
        &m_conductor.m_conductor.counters_manager, AERON_SYSTEM_COUNTER_CONDUCTOR_MAX_CYCLE_TIME);
    volatile int64_t *thresholdExceededCounter = aeron_counters_manager_addr(
        &m_conductor.m_conductor.counters_manager, AERON_SYSTEM_COUNTER_CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED);

    nano_time = 0;
    doWork();
    nano_time = INT64_C(750) * 1000 * 1000;
    doWork();
    nano_time = INT64_C(1750) * 1000 * 1000;
    doWork();
    nano_time = INT64_C(2250) * 1000 * 1000;
    doWork();
    nano_time = INT64_C(2850) * 1000 * 1000;
    doWork();
    nano_time = INT64_C(3451) * 1000 * 1000;
    doWork();

    int64_t maxCycleTime;
    AERON_GET_ACQUIRE(maxCycleTime, *maxCycleTimeCounter);

    ASSERT_EQ(1000 * 1000 * 1000, maxCycleTime);

    int64_t thresholdExceeded;
    AERON_GET_ACQUIRE(thresholdExceeded, *thresholdExceededCounter);

    ASSERT_EQ(3, thresholdExceeded);
}
