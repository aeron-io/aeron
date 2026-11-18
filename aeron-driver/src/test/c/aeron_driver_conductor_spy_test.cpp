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

class DriverConductorSpyTest : public DriverConductorTest, public testing::Test
{
protected:
    inline size_t subscriberCount(aeron_network_publication_t *publication)
    {
        return publication->conductor_fields.subscribable.length;
    }
};

TEST_F(DriverConductorSpyTest, shouldBeAbleToAddSingleSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1, -1), 0);

    doWorkUntilDone();

    int32_t client_counter_id;
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_COUNTER_READY, _, _))
        .With(IsCounterUpdate(client_id, _))
        .WillOnce(CaptureCounterId(&client_counter_id));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);

    EXPECT_EQ(aeron_driver_conductor_num_spy_subscriptions(&m_conductor.m_conductor), 1u);

    EXPECT_CALL(m_mockCallbacks, onCounter(_, _, _, _, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, onCounter(_, AERON_COUNTER_CLIENT_HEARTBEAT_TIMESTAMP_TYPE_ID, _, _, _, _)).
        With(IsIdCounter(client_id, std::string("client-heartbeat: id=0")));
    EXPECT_EQ(0, readCounters(mock_counter_handler)) << aeron_errmsg();
}

TEST_F(DriverConductorSpyTest, shouldBeAbleToAddAndRemoveSingleSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1, -1), 0);
    doWorkUntilDone();
    EXPECT_EQ(aeron_driver_conductor_num_spy_subscriptions(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id, sub_id), 0);
    doWorkUntilDone();

    EXPECT_EQ(aeron_driver_conductor_num_spy_subscriptions(&m_conductor.m_conductor), 0u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
        .With(IsOperationSuccess(remove_correlation_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorSpyTest, shouldBeAbleToAddMultipleSubscriptionsWithSameChannelSameStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();
    int64_t sub_id_3 = nextCorrelationId();
    int64_t sub_id_4 = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addSpySubscription(client_id, sub_id_2, CHANNEL_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addSpySubscription(client_id, sub_id_3, CHANNEL_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addSpySubscription(client_id, sub_id_4, CHANNEL_1, STREAM_ID_1, -1), 0);

    doWorkUntilDone();

    ASSERT_EQ(aeron_driver_conductor_num_spy_subscriptions(&m_conductor.m_conductor), 4u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _)).Times(4);
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorSpyTest, shouldBeAbleToAddMultipleSubscriptionsWithDifferentChannelSameStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();
    int64_t sub_id_3 = nextCorrelationId();
    int64_t sub_id_4 = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addSpySubscription(client_id, sub_id_2, CHANNEL_2, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addSpySubscription(client_id, sub_id_3, CHANNEL_3, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addSpySubscription(client_id, sub_id_4, CHANNEL_4, STREAM_ID_1, -1), 0);

    doWorkUntilDone();

    ASSERT_EQ(aeron_driver_conductor_num_spy_subscriptions(&m_conductor.m_conductor), 4u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _)).Times(4);
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorSpyTest, shouldBeAbleToAddSingleSubscriptionThenAddSinglePublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addPublication(client_id, pub_id, CHANNEL_1, STREAM_ID_1, false), 0);
    doWorkUntilDone();

    aeron_network_publication_t *publication = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id);
    EXPECT_EQ(subscriberCount(publication), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_active_spy_subscriptions(&m_conductor.m_conductor, CHANNEL_1, STREAM_ID_1), 1u);

    int32_t session_id = 0;
    std::string log_file_name;
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    {
        testing::InSequence s;

        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
            .With(IsSubscriptionReady(sub_id));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    auto *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);
                    session_id = response->session_id;
                    log_file_name
                        .append((char *)buffer + sizeof(aeron_publication_buffers_ready_t), (size_t)response->log_file_length);
                });
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    auto *response = reinterpret_cast<aeron_image_buffers_ready_t *>(buffer);
                    EXPECT_THAT(response, IsImageBuffersReady(sub_id, STREAM_ID_1, session_id, log_file_name, std::string(AERON_IPC_CHANNEL)));
                });
    }
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorSpyTest, shouldBeAbleToAddSingleSubscriptionWithTagThenAddSinglePublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id, CHANNEL_TAG_1001, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addPublication(client_id, pub_id, CHANNEL_1_WITH_TAG_1001, STREAM_ID_1, false), 0);
    doWorkUntilDone();

    aeron_network_publication_t *publication = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id);
    EXPECT_EQ(subscriberCount(publication), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_active_spy_subscriptions(
        &m_conductor.m_conductor, CHANNEL_TAG_1001, STREAM_ID_1), 1u);

    int32_t session_id = 0;
    std::string log_file_name;

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    {
        testing::InSequence s;

        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
            .With(IsSubscriptionReady(sub_id));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    auto *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);
                    session_id = response->session_id;
                    log_file_name.append(
                        (char *)buffer + sizeof(aeron_publication_buffers_ready_t), (size_t)response->log_file_length);
                });
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    auto *response = reinterpret_cast<aeron_image_buffers_ready_t *>(buffer);
                    EXPECT_THAT(response,
                        IsImageBuffersReady(sub_id, STREAM_ID_1, session_id, log_file_name, std::string(AERON_IPC_CHANNEL)));
                });
    }
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorSpyTest, shouldBeAbleToAddSinglePublicationThenAddSingleSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addPublication(client_id, pub_id, CHANNEL_1, STREAM_ID_1, false), 0);
    ASSERT_EQ(addSpySubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1, -1), 0);
    doWorkUntilDone();

    aeron_network_publication_t *publication = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id);
    EXPECT_EQ(subscriberCount(publication), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_active_spy_subscriptions(&m_conductor.m_conductor, CHANNEL_1, STREAM_ID_1), 1u);

    int32_t session_id = 0;
    std::string log_file_name;
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    {
        testing::InSequence s;

        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    auto *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);
                    session_id = response->session_id;
                    log_file_name.append(
                        (char *)buffer + sizeof(aeron_publication_buffers_ready_t), (size_t)response->log_file_length);
                });
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
            .With(IsSubscriptionReady(sub_id));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    auto *response = reinterpret_cast<aeron_image_buffers_ready_t *>(buffer);
                    EXPECT_THAT(response,
                        IsImageBuffersReady(sub_id, STREAM_ID_1, session_id, log_file_name, std::string(AERON_IPC_CHANNEL)));
                });
    }
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorSpyTest, shouldBeAbleToAddSinglePublicationThenAddSingleSubscriptionWithTag)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addPublication(client_id, pub_id, CHANNEL_1_WITH_TAG_1001, STREAM_ID_1, false), 0);
    ASSERT_EQ(addSpySubscription(client_id, sub_id, CHANNEL_TAG_1001, STREAM_ID_1, -1), 0);
    doWorkUntilDone();

    aeron_network_publication_t *publication = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id);
    EXPECT_EQ(subscriberCount(publication), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_active_spy_subscriptions(
        &m_conductor.m_conductor, CHANNEL_TAG_1001, STREAM_ID_1), 1u);

    int32_t session_id = 0;
    std::string log_file_name;
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    {
        testing::InSequence s;

        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    auto *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);
                    session_id = response->session_id;
                    log_file_name
                        .append((char *)buffer + sizeof(aeron_publication_buffers_ready_t), (size_t)response->log_file_length);
                });
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
            .With(IsSubscriptionReady(sub_id));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    auto *response = reinterpret_cast<aeron_image_buffers_ready_t *>(buffer);
                    EXPECT_THAT(response,
                        IsImageBuffersReady(sub_id, STREAM_ID_1, session_id, log_file_name, std::string(AERON_IPC_CHANNEL)));
                });
    }
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorSpyTest, shouldBeAbleToAddMultipleSubscriptionWithSameStreamIdThenAddSinglePublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addSpySubscription(client_id, sub_id_2, CHANNEL_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addPublication(client_id, pub_id, CHANNEL_1, STREAM_ID_1, false), 0);
    doWorkUntilDone();

    aeron_network_publication_t *publication = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id);
    EXPECT_EQ(subscriberCount(publication), 2u);
    EXPECT_EQ(aeron_driver_conductor_num_active_spy_subscriptions(&m_conductor.m_conductor, CHANNEL_1, STREAM_ID_1), 2u);

    int32_t session_id = 0;
    std::string log_file_name;
    testing::Sequence s1, s2;

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id_1))
        .InSequence(s1);
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id_2))
        .InSequence(s2);
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .InSequence(s1, s2)
        .WillOnce(
            [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
            {
                auto *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);
                session_id = response->session_id;
                log_file_name
                    .append((char *)buffer + sizeof(aeron_publication_buffers_ready_t), (size_t)response->log_file_length);
            });
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
        .Times(2)
        .InSequence(s1, s2)
        .WillRepeatedly(
            [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
            {
                auto *response = reinterpret_cast<aeron_image_buffers_ready_t *>(buffer);
                EXPECT_THAT(
                    response, testing::AnyOf(
                        IsImageBuffersReady(sub_id_1, STREAM_ID_1, session_id, log_file_name, std::string(AERON_IPC_CHANNEL)),
                        IsImageBuffersReady(sub_id_2, STREAM_ID_1, session_id, log_file_name, std::string(AERON_IPC_CHANNEL))
                ));
            });
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorSpyTest, shouldBeAbleToAddSingleSubscriptionThenAddMultipleExclusivePublicationsWithSameStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addPublication(client_id, pub_id_1, CHANNEL_1, STREAM_ID_1, true), 0);
    ASSERT_EQ(addPublication(client_id, pub_id_2, CHANNEL_1, STREAM_ID_1, true), 0);
    doWorkUntilDone();

    aeron_network_publication_t *publication_1 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_1);
    EXPECT_EQ(subscriberCount(publication_1), 1u);
    aeron_network_publication_t *publication_2 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_2);
    EXPECT_EQ(subscriberCount(publication_2), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_active_spy_subscriptions(&m_conductor.m_conductor, CHANNEL_1, STREAM_ID_1), 2u);

    int32_t session_id_1 = 0;
    int32_t session_id_2 = 0;
    std::string log_file_name_1;
    std::string log_file_name_2;
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    {
        testing::InSequence s;

        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
            .With(IsSubscriptionReady(sub_id));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    auto *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);
                    session_id_1 = response->session_id;
                    log_file_name_1.append(
                        (char *)buffer + sizeof(aeron_publication_buffers_ready_t), (size_t)response->log_file_length);
                });
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    auto *response = reinterpret_cast<aeron_image_buffers_ready_t *>(buffer);
                    EXPECT_THAT(response,
                        IsImageBuffersReady(sub_id, STREAM_ID_1, session_id_1, log_file_name_1, std::string(AERON_IPC_CHANNEL)));
                });
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    auto *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);
                    session_id_2 = response->session_id;
                    log_file_name_2.append(
                        (char *)buffer + sizeof(aeron_publication_buffers_ready_t), (size_t)response->log_file_length);
                });
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    auto *response = reinterpret_cast<aeron_image_buffers_ready_t *>(buffer);
                    EXPECT_THAT(response,
                        IsImageBuffersReady(sub_id, STREAM_ID_1, session_id_2, log_file_name_2, std::string(AERON_IPC_CHANNEL)));
                });
    }
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorSpyTest, shouldNotLinkSubscriptionOnAddPublicationAfterFirstAddPublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addPublication(client_id, pub_id_1, CHANNEL_1, STREAM_ID_1, false), 0);
    ASSERT_EQ(addPublication(client_id, pub_id_2, CHANNEL_1, STREAM_ID_1, false), 0);
    doWorkUntilDone();

    aeron_network_publication_t *publication = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_1);
    EXPECT_EQ(subscriberCount(publication), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_active_spy_subscriptions(&m_conductor.m_conductor, CHANNEL_1, STREAM_ID_1), 1u);

    int32_t session_id = 0;
    std::string log_file_name;
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    {
        testing::InSequence s;

        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
            .With(IsSubscriptionReady(sub_id));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
            .With(IsPublicationReady(pub_id_1, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    auto *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);
                    session_id = response->session_id;
                    log_file_name.append(
                        (char *)buffer + sizeof(aeron_publication_buffers_ready_t), (size_t)response->log_file_length);
                });
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    auto *response = reinterpret_cast<aeron_image_buffers_ready_t *>(buffer);
                    EXPECT_THAT(response,
                        IsImageBuffersReady(sub_id, STREAM_ID_1, session_id, log_file_name, std::string(AERON_IPC_CHANNEL)));
                });
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
            .With(IsPublicationReady(pub_id_2, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    auto *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);
                    session_id = response->session_id;
                    std::string response_log_file_name(
                        (char *)buffer + sizeof(aeron_publication_buffers_ready_t), (size_t)response->log_file_length);
                    EXPECT_EQ(pub_id_2, response->correlation_id);
                    EXPECT_EQ(pub_id_1, response->registration_id);
                    EXPECT_EQ(log_file_name, response_log_file_name);
                });
    }
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorSpyTest, shouldBeAbleToTimeoutSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1, false), 0);
    doWorkUntilDone();;
    EXPECT_EQ(aeron_driver_conductor_num_spy_subscriptions(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    doWorkForNs(
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_spy_subscriptions(&m_conductor.m_conductor), 0u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_CLIENT_TIMEOUT, _, _))
        .With(IsTimeout(client_id));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorSpyTest, shouldBeAbleToNotTimeoutSubscriptionOnKeepalive)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1, false), 0);
    doWorkUntilDone();
    EXPECT_EQ(aeron_driver_conductor_num_spy_subscriptions(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    int64_t timeout =
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2);

    doWorkForNs(
        timeout,
        100,
        [&]()
        {
            clientKeepalive(client_id);
        });

    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_spy_subscriptions(&m_conductor.m_conductor), 1u);
}

TEST_F(DriverConductorSpyTest, shouldAddNetworkPublicationThenSingleSpyWithSameSessionId)
{
    const int64_t client_id = nextCorrelationId();
    const int64_t pub_id = nextCorrelationId();
    const int64_t sub_id = nextCorrelationId();
    const int32_t ipc_session_id = -4097;
    std::string pub_channel =
        std::string(CHANNEL_1) + "|" +
        std::string(AERON_URI_SESSION_ID_KEY) + "=" + std::to_string(ipc_session_id);
    std::string sub_channel =
        std::string(AERON_SPY_PREFIX) + std::string(CHANNEL_1) + "|" +
        std::string(AERON_URI_SESSION_ID_KEY) + "=" + std::to_string(ipc_session_id);

    ASSERT_EQ(addPublication(client_id, pub_id, pub_channel, STREAM_ID_1, false), 0);
    ASSERT_EQ(addSubscription(client_id, sub_id, sub_channel, STREAM_ID_1, false), 0);
    doWorkUntilDone();

    aeron_network_publication_t *publication = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id);
    EXPECT_EQ(subscriberCount(publication), 1u);

    int32_t session_id = 0;
    std::string log_file_name;
    testing::Sequence sequence;

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .InSequence(sequence)
        .WillOnce(CapturePublicationReady(&session_id, &log_file_name));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id))
        .InSequence(sequence);
    readAllBroadcastsFromConductor(mock_broadcast_handler, 3);  // Limit the poll to capture the publication ready.

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
        .With(IsAvailableImage(_, sub_id, STREAM_ID_1, session_id, log_file_name.c_str(), AERON_IPC_CHANNEL));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorSpyTest, shouldAddSingleSpyThenNetworkPublicationWithSameSessionId)
{
    const int64_t client_id = nextCorrelationId();
    const int64_t pub_id = nextCorrelationId();
    const int64_t sub_id = nextCorrelationId();
    const int32_t ipc_session_id = -4097;
    std::string pub_channel =
        std::string(CHANNEL_1) + "|" + std::string(AERON_URI_SESSION_ID_KEY) + "=" + std::to_string(ipc_session_id);
    std::string sub_channel =
        std::string(AERON_SPY_PREFIX) + std::string(CHANNEL_1) + "|" +
            std::string(AERON_URI_SESSION_ID_KEY) + "=" + std::to_string(ipc_session_id);

    ASSERT_EQ(addSubscription(client_id, sub_id, sub_channel, STREAM_ID_1, false), 0);
    ASSERT_EQ(addPublication(client_id, pub_id, pub_channel, STREAM_ID_1, false), 0);
    doWorkUntilDone();

    aeron_network_publication_t *publication = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id);
    EXPECT_EQ(subscriberCount(publication), 1u);

    int32_t session_id = 0;
    std::string log_file_name;
    testing::Sequence sequence;

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id))
        .InSequence(sequence);
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .InSequence(sequence)
        .WillOnce(CapturePublicationReady(&session_id, &log_file_name));
    readAllBroadcastsFromConductor(mock_broadcast_handler, 3);  // Limit the poll to capture the publication ready.

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
        .With(IsAvailableImage(_, sub_id, STREAM_ID_1, session_id, log_file_name.c_str(), AERON_IPC_CHANNEL));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorSpyTest, shouldNotAddNetworkPublicationThenSingleSpyWithDifferentSessionId)
{
    const int64_t client_id = nextCorrelationId();
    const int64_t pub_id = nextCorrelationId();
    const int64_t sub_id = nextCorrelationId();
    const int32_t pub_session_id = -4097;
    const int32_t sub_session_id = -4098;
    std::string pub_channel =
        std::string(CHANNEL_1) + "|" +
        std::string(AERON_URI_SESSION_ID_KEY) + "=" + std::to_string(pub_session_id);
    std::string sub_channel =
        std::string(AERON_SPY_PREFIX) + std::string(CHANNEL_1) + "|" +
        std::string(AERON_URI_SESSION_ID_KEY) + "=" + std::to_string(sub_session_id);

    ASSERT_EQ(addPublication(client_id, pub_id, pub_channel, STREAM_ID_1, false), 0);
    ASSERT_EQ(addSubscription(client_id, sub_id, sub_channel, STREAM_ID_1, false), 0);
    doWorkUntilDone();

    aeron_network_publication_t *publication = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id);
    EXPECT_EQ(subscriberCount(publication), 0u);

    int32_t session_id = 0;
    std::string log_file_name;
    testing::Sequence sequence;

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .InSequence(sequence)
        .WillOnce(CapturePublicationReady(&session_id, &log_file_name));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id))
        .InSequence(sequence);
    readAllBroadcastsFromConductor(mock_broadcast_handler, 3);  // Limit the poll to capture the publication ready.

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
        .With(IsAvailableImage(_, sub_id, STREAM_ID_1, pub_session_id, log_file_name.c_str(), AERON_IPC_CHANNEL))
        .Times(0);
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorSpyTest, shouldNotAddSingleSpyThenNetworkPublicationWithDifferentSessionId)
{
    const int64_t client_id = nextCorrelationId();
    const int64_t pub_id = nextCorrelationId();
    const int64_t sub_id = nextCorrelationId();
    const int32_t pub_session_id = -4097;
    const int32_t sub_session_id = -4098;
    std::string pub_channel =
        std::string(CHANNEL_1) + "|" +
        std::string(AERON_URI_SESSION_ID_KEY) + "=" + std::to_string(pub_session_id);
    std::string sub_channel =
        std::string(AERON_SPY_PREFIX) + std::string(CHANNEL_1) + "|" +
        std::string(AERON_URI_SESSION_ID_KEY) + "=" + std::to_string(sub_session_id);

    ASSERT_EQ(addSubscription(client_id, sub_id, sub_channel, STREAM_ID_1, false), 0);
    ASSERT_EQ(addPublication(client_id, pub_id, pub_channel, STREAM_ID_1, false), 0);
    doWorkUntilDone();

    aeron_network_publication_t *publication = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id);
    EXPECT_EQ(subscriberCount(publication), 0u);

    int32_t session_id = 0;
    std::string log_file_name;
    testing::Sequence sequence;

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id))
        .InSequence(sequence);
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .InSequence(sequence)
        .WillOnce(CapturePublicationReady(&session_id, &log_file_name));
    readAllBroadcastsFromConductor(mock_broadcast_handler, 3);  // Limit the poll to capture the publication ready.

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
        .With(IsAvailableImage(_, sub_id, STREAM_ID_1, pub_session_id, log_file_name.c_str(), AERON_IPC_CHANNEL))
        .Times(0);
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}
