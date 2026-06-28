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

using testing::_;
using testing::HasSubstr;

class DriverConductorConfigTest : public DriverConductorTest, public testing::Test
{
};

TEST_F(DriverConductorConfigTest, shouldRejectResponsePublicationDestination)
{
    addPublication(1234, 1234, "aeron:udp?control=localhost:4000|control-mode=manual", 1234, false);
    doWorkUntilDone();
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_COUNTER_READY, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    addDestination(1234, 1234, 1234, "aeron:udp?endpoint=localhost:8001|control-mode=response");
    doWorkUntilDone();
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _))
    .WillOnce(
        [&](std::int32_t msgTypeId, uint8_t *buffer, size_t length)
        {
            const aeron_error_response_t *msg = reinterpret_cast<aeron_error_response_t *>(buffer);
            char *msg_buf = (char *)malloc(msg->error_message_length + 1);

            memset(msg_buf, 0, msg->error_message_length + 1);
            memcpy(msg_buf, buffer + sizeof(aeron_error_response_t), msg->error_message_length);

            EXPECT_THAT(msg_buf, HasSubstr(AERON_UDP_CHANNEL_CONTROL_MODE_KEY));
            EXPECT_THAT(msg_buf, HasSubstr(AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE_VALUE));

            free(msg_buf);
        });
    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    addDestination(1234, 1234, 1234, "aeron:udp?endpoint=localhost:8001|response-correlation-id=2345");
    doWorkUntilDone();
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _))
        .WillOnce(
            [&](std::int32_t msgTypeId, uint8_t *buffer, size_t length)
            {
                const aeron_error_response_t *msg = reinterpret_cast<aeron_error_response_t *>(buffer);
                char *msg_buf = (char *)malloc(msg->error_message_length + 1);

                memset(msg_buf, 0, msg->error_message_length + 1);
                memcpy(msg_buf, buffer + sizeof(aeron_error_response_t), msg->error_message_length);

                EXPECT_THAT(msg_buf, HasSubstr(AERON_URI_RESPONSE_CORRELATION_ID_KEY));

                free(msg_buf);
            });
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorConfigTest, shouldRejectResponseSubscriptionDestination)
{
    addNetworkSubscription(1234, 1234, "aeron:udp?control-mode=manual", 1234);
    doWorkUntilDone();
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_COUNTER_READY, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    addReceiveDestination(1234, 1234, 1234, "aeron:udp?endpoint=localhost:8001|control-mode=response");
    doWorkUntilDone();
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _))
        .WillOnce(
            [&](std::int32_t msgTypeId, uint8_t *buffer, size_t length)
            {
                const aeron_error_response_t *msg = reinterpret_cast<aeron_error_response_t *>(buffer);
                char *msg_buf = (char *)malloc(msg->error_message_length + 1);

                memset(msg_buf, 0, msg->error_message_length + 1);
                memcpy(msg_buf, buffer + sizeof(aeron_error_response_t), msg->error_message_length);

                EXPECT_THAT(msg_buf, HasSubstr(AERON_UDP_CHANNEL_CONTROL_MODE_KEY));
                EXPECT_THAT(msg_buf, HasSubstr(AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE_VALUE));

                free(msg_buf);
            });
    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    addReceiveDestination(1234, 1234, 1234, "aeron:udp?endpoint=localhost:8001|response-correlation-id=2345");
    doWorkUntilDone();
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _))
        .WillOnce(
            [&](std::int32_t msgTypeId, uint8_t *buffer, size_t length)
            {
                const aeron_error_response_t *msg = reinterpret_cast<aeron_error_response_t *>(buffer);
                char *msg_buf = (char *)malloc(msg->error_message_length + 1);

                memset(msg_buf, 0, msg->error_message_length + 1);
                memcpy(msg_buf, buffer + sizeof(aeron_error_response_t), msg->error_message_length);

                EXPECT_THAT(msg_buf, HasSubstr(AERON_URI_RESPONSE_CORRELATION_ID_KEY));

                free(msg_buf);
            });
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}
