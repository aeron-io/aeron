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

namespace
{
// State shared with eol_uaf_remove_publication_cleanup (the cleanup callback has no user pointer).
aeron_driver_native_resource_agent_t *g_eol_uaf_agent = nullptr;
bool g_eol_uaf_drain_during_cleanup = false;

// Installed as context->log.remove_publication_cleanup, which the managed-resource sweep invokes
// from aeron_ipc_publication_entry_delete BEFORE aeron_ipc_publication_close. Draining the native
// resource agent here runs any free that free_func has already scheduled for this publication.
// With the buggy ordering (free_func before delete_func) the publication is freed at this point and
// the following close reads freed memory -> AddressSanitizer heap-use-after-free. With the correct
// ordering (delete_func before free_func) nothing is queued yet, so this is a no-op and close reads
// a live publication.
void eol_uaf_remove_publication_cleanup(int32_t /*session_id*/, int32_t /*stream_id*/, size_t /*channel_length*/, const char * /*channel*/)
{
    if (g_eol_uaf_drain_during_cleanup && nullptr != g_eol_uaf_agent)
    {
        aeron_driver_native_resource_agent_do_work(g_eol_uaf_agent);
    }
}
}

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

// Regression test for the managed-resource end-of-life ordering: the conductor must close an
// end-of-life resource before scheduling its (async) free. The native resource agent frees the
// publication on its own thread, so if the free is scheduled before the close, the agent can free
// the struct while aeron_ipc_publication_close is still reading it.
//
// This single-threaded test forces that interleaving deterministically: a cleanup callback drains
// the native resource agent from inside aeron_ipc_publication_entry_delete (before the close). With
// the buggy ordering the publication is freed there and the close reads freed memory, which the
// sanitiser build reports as a heap-use-after-free. With the correct ordering the free has not been
// scheduled yet, the drain is a no-op, and the close is safe.
TEST_F(DriverConductorIpcTest, DISABLED_shouldCloseEndOfLifeIpcPublicationBeforeSchedulingAsyncFree)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addIpcPublication(client_id, pub_id, STREAM_ID_1, false), 0);
    ASSERT_EQ(addIpcSubscription(client_id, sub_id, STREAM_ID_1, false), 0);
    doWorkUntilDone();
    ASSERT_EQ(aeron_driver_conductor_num_ipc_publications(&m_conductor.m_conductor), 1u);

    ASSERT_EQ(removePublication(client_id, remove_correlation_id, pub_id), 0);
    doWorkUntilDone();

    readAllBroadcastsFromConductor(null_broadcast_handler);

    g_eol_uaf_agent = &m_conductor.m_native_resource_agent;
    g_eol_uaf_drain_during_cleanup = true;
    m_context.m_context->log.remove_publication_cleanup = eol_uaf_remove_publication_cleanup;

    auto timeout = static_cast<int64_t>(m_context.m_context->publication_linger_timeout_ns * 2);
    doWorkForNs(
        timeout,
        100,
        [&]()
        {
            clientKeepalive(client_id);
        });

    g_eol_uaf_drain_during_cleanup = false;
    g_eol_uaf_agent = nullptr;

    EXPECT_EQ(aeron_driver_conductor_num_ipc_publications(&m_conductor.m_conductor), 0u);
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
