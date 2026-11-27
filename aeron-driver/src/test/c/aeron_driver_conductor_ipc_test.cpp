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

class DriverConductorIpcTest : public DriverConductorTest, public testing::Test
{
protected:
    inline size_t subscriberCount(aeron_ipc_publication_t *publication)
    {
        return publication->conductor_fields.subscribable.length;
    }
};

// TODO: Parameterise
TEST_F(DriverConductorIpcTest, shouldBeAbleToAddSingleIpcSubscriptionThenAddSingleIpcPublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addIpcSubscription(client_id, sub_id, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addIpcPublication(client_id, pub_id, STREAM_ID_1, false), 0);
    doWorkUntilDone();

    aeron_ipc_publication_t *publication = aeron_driver_conductor_find_ipc_publication(
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

// TODO: Parameterise
TEST_F(DriverConductorIpcTest, shouldBeAbleToAddSingleIpcPublicationThenAddSingleIpcSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addIpcPublication(client_id, pub_id, STREAM_ID_1, false), 0);
    ASSERT_EQ(addIpcSubscription(client_id, sub_id, STREAM_ID_1, -1), 0);
    doWorkUntilDone();

    aeron_ipc_publication_t *publication = aeron_driver_conductor_find_ipc_publication(
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

// TODO: Parameterise
TEST_F(DriverConductorIpcTest, shouldBeAbleToAddMultipleIpcSubscriptionWithSameStreamIdThenAddSingleIpcPublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addIpcSubscription(client_id, sub_id_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addIpcSubscription(client_id, sub_id_2, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addIpcPublication(client_id, pub_id, STREAM_ID_1, false), 0);
    doWorkUntilDone();

    aeron_ipc_publication_t *publication = aeron_driver_conductor_find_ipc_publication(
        &m_conductor.m_conductor, pub_id);
    EXPECT_EQ(subscriberCount(publication), 2u);

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
        .WillOnce(CapturePublicationReady(&session_id, &log_file_name));

    readAllBroadcastsFromConductor(mock_broadcast_handler, 4);  // Limit the poll to capture the publication ready.

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
        .With(IsAvailableImage(_, sub_id_1, STREAM_ID_1, session_id, log_file_name.c_str(), AERON_IPC_CHANNEL));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
        .With(IsAvailableImage(_, sub_id_2, STREAM_ID_1, session_id, log_file_name.c_str(), AERON_IPC_CHANNEL));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

// TODO: Parameterise
TEST_F(DriverConductorIpcTest, shouldAddSingleIpcSubscriptionThenAddMultipleExclusiveIpcPublicationsWithSameStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addIpcSubscription(client_id, sub_id, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addIpcPublication(client_id, pub_id_1, STREAM_ID_1, true), 0);
    ASSERT_EQ(addIpcPublication(client_id, pub_id_2, STREAM_ID_1, true), 0);
    doWorkUntilDone();

    aeron_ipc_publication_t *publication_1 = aeron_driver_conductor_find_ipc_publication(
        &m_conductor.m_conductor, pub_id_1);
    EXPECT_EQ(subscriberCount(publication_1), 1u);
    aeron_ipc_publication_t *publication_2 = aeron_driver_conductor_find_ipc_publication(
        &m_conductor.m_conductor, pub_id_2);
    EXPECT_EQ(subscriberCount(publication_2), 1u);

    int32_t session_id_1 = 0;
    int32_t session_id_2 = 0;
    std::string log_file_name_1;
    std::string log_file_name_2;

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY, _, _))
        .WillOnce(CapturePublicationReady(&session_id_1, &log_file_name_1));
    readAllBroadcastsFromConductor(mock_broadcast_handler, 3);  // Limit the poll to capture the publication ready.

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
        .With(IsAvailableImage(_, sub_id, STREAM_ID_1, session_id_1, log_file_name_1.c_str(), AERON_IPC_CHANNEL));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY, _, _))
        .WillOnce(CapturePublicationReady(&session_id_2, &log_file_name_2));
    readAllBroadcastsFromConductor(mock_broadcast_handler, 2);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
        .With(IsAvailableImage(_, sub_id, STREAM_ID_1, session_id_2, log_file_name_2.c_str(), AERON_IPC_CHANNEL));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

// TODO: Parameterise
TEST_F(DriverConductorIpcTest, shouldNotLinkSubscriptionOnAddPublicationAfterFirstAddPublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addIpcSubscription(client_id, sub_id, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addIpcPublication(client_id, pub_id_1, STREAM_ID_1, false), 0);
    ASSERT_EQ(addIpcPublication(client_id, pub_id_2, STREAM_ID_1, false), 0);
    doWorkUntilDone();

    aeron_ipc_publication_t *publication = aeron_driver_conductor_find_ipc_publication(
        &m_conductor.m_conductor, pub_id_1);
    EXPECT_EQ(subscriberCount(publication), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_active_ipc_subscriptions(&m_conductor.m_conductor, STREAM_ID_1), 1u);

    int32_t session_id = 0;
    std::string log_file_name;
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .WillOnce(CapturePublicationReady(&session_id, &log_file_name));
    readAllBroadcastsFromConductor(mock_broadcast_handler, 3);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
        .With(IsAvailableImage(_, sub_id, STREAM_ID_1, session_id, log_file_name.c_str(), AERON_IPC_CHANNEL));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .With(IsPubReadyWithFile(pub_id_1, pub_id_2, STREAM_ID_1, session_id, log_file_name));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

// TODO: Paramterise
TEST_F(DriverConductorIpcTest, shouldBeAbleToTimeoutMultipleIpcSubscriptions)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id1 = nextCorrelationId();
    int64_t sub_id2 = nextCorrelationId();
    int64_t sub_id3 = nextCorrelationId();

    ASSERT_EQ(addIpcSubscription(client_id, sub_id1, STREAM_ID_1, false), 0);
    ASSERT_EQ(addIpcSubscription(client_id, sub_id2, STREAM_ID_2, false), 0);
    ASSERT_EQ(addIpcSubscription(client_id, sub_id3, STREAM_ID_3, false), 0);

    doWorkUntilDone();

    EXPECT_EQ(aeron_driver_conductor_num_ipc_subscriptions(&m_conductor.m_conductor), 3u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    uint64_t durationNs =
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2);
    doWorkForNs(static_cast<int64_t>(durationNs));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_ipc_subscriptions(&m_conductor.m_conductor), 0u);
}

// TODO: Parameterise
TEST_F(DriverConductorIpcTest, shouldBeAbleToTimeoutIpcPublicationWithActiveIpcSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addIpcPublication(client_id, pub_id, STREAM_ID_1, false), 0);
    ASSERT_EQ(addIpcSubscription(client_id, sub_id, STREAM_ID_1, false), 0);
    doWorkUntilDone();

    ASSERT_EQ(removePublication(client_id, remove_correlation_id, pub_id), 0);
    doWorkUntilDone();

    readAllBroadcastsFromConductor(null_broadcast_handler);

    auto timeout = static_cast<int64_t>(m_context.m_context->publication_linger_timeout_ns * 2);

    doWorkForNs(
        timeout,
        100,
        [&]()
        {
            clientKeepalive(client_id);
        });

    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_ipc_publications(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_active_ipc_subscriptions(&m_conductor.m_conductor, STREAM_ID_1), 0u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_UNAVAILABLE_IMAGE, _, _))
        .With(IsUnavailableImage(STREAM_ID_1, pub_id, sub_id, AERON_IPC_CHANNEL));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

// TODO: Parameterise
TEST_F(DriverConductorIpcTest, shouldAddIpcPublicationThenSubscriptionWithSessionId)
{
    const int64_t client_id = nextCorrelationId();
    const int64_t pub_id = nextCorrelationId();
    const int64_t sub_id = nextCorrelationId();
    const int32_t ipc_session_id = -4097;
    std::string channel =
        std::string(AERON_IPC_CHANNEL) + "?" + std::string(AERON_URI_SESSION_ID_KEY) + "=" + std::to_string(ipc_session_id);

    ASSERT_EQ(addPublication(client_id, pub_id, channel, STREAM_ID_1, false), 0);
    ASSERT_EQ(addSubscription(client_id, sub_id, channel, STREAM_ID_1, false), 0);
    doWorkUntilDone();

    aeron_ipc_publication_t *publication = aeron_driver_conductor_find_ipc_publication(
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

// TODO: Parameterise
TEST_F(DriverConductorIpcTest, shouldAddIpcSubscriptionThenPublicationWithSessionId)
{
    const int64_t client_id = nextCorrelationId();
    const int64_t pub_id = nextCorrelationId();
    const int64_t sub_id = nextCorrelationId();
    const int32_t ipc_session_id = -4097;
    std::string channel =
        std::string(AERON_IPC_CHANNEL) + "?" + std::string(AERON_URI_SESSION_ID_KEY) + "=" + std::to_string(ipc_session_id);

    ASSERT_EQ(addSubscription(client_id, sub_id, channel, STREAM_ID_1, false), 0);
    ASSERT_EQ(addPublication(client_id, pub_id, channel, STREAM_ID_1, false), 0);
    doWorkUntilDone();

    aeron_ipc_publication_t *publication = aeron_driver_conductor_find_ipc_publication(
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

// TODO: Parameterise
TEST_F(DriverConductorIpcTest, shouldNotAddIpcPublicationThenSubscriptionWithDifferentSessionId)
{
    const int64_t client_id = nextCorrelationId();
    const int64_t pub_id = nextCorrelationId();
    const int64_t sub_id = nextCorrelationId();
    const int32_t pub_session_id = -4097;
    const int32_t sub_session_id = -4098;
    std::string pub_channel =
        std::string(AERON_IPC_CHANNEL) + "?" + std::string(AERON_URI_SESSION_ID_KEY) + "=" + std::to_string(pub_session_id);
    std::string sub_channel =
        std::string(AERON_IPC_CHANNEL) + "?" + std::string(AERON_URI_SESSION_ID_KEY) + "=" + std::to_string(sub_session_id);

    ASSERT_EQ(addPublication(client_id, pub_id, pub_channel, STREAM_ID_1, false), 0);
    ASSERT_EQ(addSubscription(client_id, sub_id, sub_channel, STREAM_ID_1, false), 0);
    doWorkUntilDone();

    aeron_ipc_publication_t *publication = aeron_driver_conductor_find_ipc_publication(
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

// TODO: Parameterise
TEST_F(DriverConductorIpcTest, shouldNotAddIpcSubscriptionThenPublicationWithDifferentSessionId)
{
    const int64_t client_id = nextCorrelationId();
    const int64_t pub_id = nextCorrelationId();
    const int64_t sub_id = nextCorrelationId();
    const int32_t pub_session_id = -4097;
    const int32_t sub_session_id = -4098;
    std::string pub_channel =
        std::string(AERON_IPC_CHANNEL) + "?" + std::string(AERON_URI_SESSION_ID_KEY) + "=" + std::to_string(pub_session_id);
    std::string sub_channel =
        std::string(AERON_IPC_CHANNEL) + "?" + std::string(AERON_URI_SESSION_ID_KEY) + "=" + std::to_string(sub_session_id);

    ASSERT_EQ(addSubscription(client_id, sub_id, sub_channel, STREAM_ID_1, false), 0);
    ASSERT_EQ(addPublication(client_id, pub_id, pub_channel, STREAM_ID_1, false), 0);
    doWorkUntilDone();

    aeron_ipc_publication_t *publication = aeron_driver_conductor_find_ipc_publication(
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

TEST_F(DriverConductorIpcTest, shouldUseSessionIdFromTaggedIpcPublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addPublication(
        client_id, pub_id_1, AERON_IPC_CHANNEL "?tags=1001,1002|term-length=131072|mtu=8192", STREAM_ID_1, false), 0);
    ASSERT_EQ(addPublication(client_id, pub_id_2, AERON_IPC_CHANNEL "?session-id=tag:1002", STREAM_ID_2, false), 0);
    doWorkUntilDone();
    EXPECT_EQ(aeron_driver_conductor_num_ipc_publications(&m_conductor.m_conductor), 2u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    aeron_ipc_publication_t *pub_1 = aeron_driver_conductor_find_ipc_publication(&m_conductor.m_conductor, pub_id_1);
    aeron_ipc_publication_t *pub_2 = aeron_driver_conductor_find_ipc_publication(&m_conductor.m_conductor, pub_id_2);
    ASSERT_NE(nullptr, pub_1);
    ASSERT_NE(nullptr, pub_2);
    EXPECT_EQ(pub_1->session_id, pub_2->session_id);
    EXPECT_EQ(131072, pub_2->log_meta_data->term_length);
    EXPECT_EQ(8192, pub_2->log_meta_data->mtu_length);
}

TEST_F(DriverConductorIpcTest, shouldErrorWithUnknownIpcSessionIdTag)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addPublication(client_id, pub_id, AERON_IPC_CHANNEL "?session-id=tag:1002", STREAM_ID_1, false), 0);
    doWorkUntilDone();
    EXPECT_EQ(aeron_driver_conductor_num_ipc_publications(&m_conductor.m_conductor), 0u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(pub_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorIpcTest, shouldNotResolveIpcSessionIdTagAgainstNetworkPublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t network_pub_id = nextCorrelationId();
    int64_t ipc_pub_id = nextCorrelationId();

    ASSERT_EQ(addPublication(client_id, network_pub_id, CHANNEL_1 "|tags=1001,1002", STREAM_ID_1, false), 0);
    doWorkUntilDone();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(addPublication(client_id, ipc_pub_id, AERON_IPC_CHANNEL "?session-id=tag:1002", STREAM_ID_1, false), 0);
    doWorkUntilDone();
    EXPECT_EQ(aeron_driver_conductor_num_ipc_publications(&m_conductor.m_conductor), 0u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(ipc_pub_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorIpcTest, shouldNotResolveNetworkSessionIdTagAgainstIpcPublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t ipc_pub_id = nextCorrelationId();
    int64_t network_pub_id = nextCorrelationId();

    ASSERT_EQ(addPublication(client_id, ipc_pub_id, AERON_IPC_CHANNEL "?tags=1001,1002", STREAM_ID_1, false), 0);
    doWorkUntilDone();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(addPublication(client_id, network_pub_id, CHANNEL_1 "|session-id=tag:1002", STREAM_ID_1, false), 0);
    doWorkUntilDone();
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 0u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(network_pub_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}
