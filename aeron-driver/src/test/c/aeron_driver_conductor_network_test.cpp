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

#include <cinttypes>
#include "aeron_driver_conductor_test.h"

using testing::_;
using testing::Eq;
using testing::Ne;
using testing::Args;
using testing::Mock;
using testing::AnyNumber;

class DriverConductorNetworkTest : public DriverConductorTest, public testing::Test
{
protected:
    aeron_publication_image_t *aeron_driver_conductor_find_publication_image(
        aeron_driver_conductor_t *conductor, aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
    {
        for (size_t i = 0, length = conductor->publication_images.length; i < length; i++)
        {
            aeron_publication_image_t *image = conductor->publication_images.array[i].image;

            if (endpoint == image->endpoint && stream_id == image->stream_id)
            {
                return image;
            }
        }

        return nullptr;
    }

    inline size_t aeron_publication_image_subscriber_count(aeron_publication_image_t *image)
    {
        return image->conductor_fields.subscribable.length;
    }
};

TEST_F(DriverConductorNetworkTest, shouldBeAbleToAddMultipleNetworkSubscriptionsWithDifferentChannelSameStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();
    int64_t sub_id_3 = nextCorrelationId();
    int64_t sub_id_4 = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, CHANNEL_2, STREAM_ID_1), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_3, CHANNEL_3, STREAM_ID_1), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_4, CHANNEL_4, STREAM_ID_1), 0);

    doWorkUntilDone();

    aeron_receive_channel_endpoint_t *endpoint_1 = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);
    aeron_receive_channel_endpoint_t *endpoint_2 = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_2);
    aeron_receive_channel_endpoint_t *endpoint_3 = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_3);
    aeron_receive_channel_endpoint_t *endpoint_4 = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_4);

    ASSERT_NE(endpoint_1, (aeron_receive_channel_endpoint_t *)nullptr);
    ASSERT_NE(endpoint_2, (aeron_receive_channel_endpoint_t *)nullptr);
    ASSERT_NE(endpoint_3, (aeron_receive_channel_endpoint_t *)nullptr);
    ASSERT_NE(endpoint_4, (aeron_receive_channel_endpoint_t *)nullptr);
    ASSERT_EQ(aeron_driver_conductor_num_receive_channel_endpoints(&m_conductor.m_conductor), 4u);
    ASSERT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 4u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id_1));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id_2));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id_3));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id_4));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldKeepSubscriptionMediaEndpointUponRemovalOfAllButOneSubscriber)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();
    int64_t sub_id_3 = nextCorrelationId();
    int64_t sub_id_4 = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, CHANNEL_1, STREAM_ID_2), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_3, CHANNEL_1, STREAM_ID_3), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_4, CHANNEL_1, STREAM_ID_4), 0);

    doWorkUntilDone();

    int64_t remove_correlation_id_1 = nextCorrelationId();
    int64_t remove_correlation_id_2 = nextCorrelationId();
    int64_t remove_correlation_id_3 = nextCorrelationId();

    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id_1, sub_id_1), 0);
    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id_2, sub_id_2), 0);
    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id_3, sub_id_3), 0);

    doWorkUntilDone();

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    ASSERT_NE(endpoint, (aeron_receive_channel_endpoint_t *)nullptr);
    ASSERT_EQ(aeron_driver_conductor_num_receive_channel_endpoints(&m_conductor.m_conductor), 1u);
    ASSERT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 1u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _)).Times(4);
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _)).Times(3);

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldCreatePublicationImageForActiveNetworkSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
    doWorkUntilDone();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    createPublicationImage(endpoint, STREAM_ID_1, 1000);

    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 1u);

    aeron_publication_image_t *image = aeron_driver_conductor_find_publication_image(
        &m_conductor.m_conductor, endpoint, STREAM_ID_1);

    EXPECT_NE(image, (aeron_publication_image_t *)nullptr);
    EXPECT_EQ(aeron_publication_image_subscriber_count(image), 1u);

    int64_t image_registration_id = aeron_publication_image_registration_id(image);
    const char *log_file_name = aeron_publication_image_log_file_name(image);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
        .With(IsAvailableImage(image_registration_id, sub_id, STREAM_ID_1, SESSION_ID, log_file_name, SOURCE_IDENTITY));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldNotCreatePublicationImageForNonActiveNetworkSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
    doWorkUntilDone();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    createPublicationImage(endpoint, STREAM_ID_2, 1000);

    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 0u);

    readAllBroadcastsFromConductor(null_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldRemoveSubscriptionFromImageWhenRemoveSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
    doWorkUntilDone();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    createPublicationImage(endpoint, STREAM_ID_1, 1000);

    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 1u);

    aeron_publication_image_t *image =
        aeron_driver_conductor_find_publication_image(&m_conductor.m_conductor, endpoint, STREAM_ID_1);

    EXPECT_NE(image, (aeron_publication_image_t *)nullptr);
    EXPECT_EQ(aeron_publication_image_subscriber_count(image), 1u);

    int64_t remove_correlation_id = nextCorrelationId();
    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id, sub_id), 0);
    doWork();

    EXPECT_EQ(aeron_publication_image_subscriber_count(image), 0u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
        .With(IsOperationSuccess(remove_correlation_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldTimeoutImageAndSendUnavailableImageWhenNoActivity)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
    doWorkUntilDone();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    createPublicationImage(endpoint, STREAM_ID_1, 1000);

    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 1u);

    aeron_publication_image_t *image = aeron_driver_conductor_find_publication_image(
        &m_conductor.m_conductor, endpoint, STREAM_ID_1);

    EXPECT_NE(image, (aeron_publication_image_t *)nullptr);
    EXPECT_EQ(aeron_publication_image_subscriber_count(image), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    int64_t image_correlation_id = image->conductor_fields.managed_resource.registration_id;

    uint64_t timeoutNs = m_context.m_context->image_liveness_timeout_ns +
        (m_context.m_context->client_liveness_timeout_ns * 2) + (m_context.m_context->timer_interval_ns * 3);

    doWorkForNs(
        static_cast<int64_t>(timeoutNs),
        100,
        [&]()
        {
            clientKeepalive(client_id);
        });

    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 0u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_UNAVAILABLE_IMAGE, _, _))
        .With(IsUnavailableImage(STREAM_ID_1, image_correlation_id, sub_id, CHANNEL_1));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldRemoveSubscriptionAfterImageTimeout)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
    doWorkUntilDone();

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    createPublicationImage(endpoint, STREAM_ID_1, 1000);

    uint64_t timeoutNs = m_context.m_context->image_liveness_timeout_ns +
        (m_context.m_context->client_liveness_timeout_ns * 2) + (m_context.m_context->timer_interval_ns * 3);

    doWorkForNs(
        static_cast<int64_t>(timeoutNs),
        100,
        [&]()
        {
            clientKeepalive(client_id);
        });

    readAllBroadcastsFromConductor(null_broadcast_handler);
    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_active_network_subscriptions(&m_conductor.m_conductor, CHANNEL_1, STREAM_ID_1), 0u);
    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id, sub_id), 0);

    doWorkUntilDone();
    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 0u);
}

TEST_F(DriverConductorNetworkTest, shouldRetryFreeOperationsAfterSubscrptionIsClosed)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
    doWorkUntilDone();

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    createPublicationImage(endpoint, STREAM_ID_1, 1000);

    uint64_t timeoutNs = m_context.m_context->image_liveness_timeout_ns +
        (m_context.m_context->client_liveness_timeout_ns * 2) + (m_context.m_context->timer_interval_ns * 3);

    free_map_raw_log = false;
    int64_t *free_fails_counter = aeron_system_counter_addr(
        &m_conductor.m_conductor.system_counters, AERON_SYSTEM_COUNTER_FREE_FAILS);
    EXPECT_EQ(aeron_counter_get_plain(free_fails_counter), 0);

    doWorkForNs(
        static_cast<int64_t>(timeoutNs),
        100,
        [&]()
        {
            clientKeepalive(client_id);
        });

    readAllBroadcastsFromConductor(null_broadcast_handler);
    const int64_t free_fails = aeron_counter_get_plain(free_fails_counter);
    EXPECT_GT(free_fails, 0);
    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_active_network_subscriptions(&m_conductor.m_conductor, CHANNEL_1, STREAM_ID_1), 0u);
    free_map_raw_log = true;

    doWorkUntilDone();

    readAllBroadcastsFromConductor(null_broadcast_handler);
    EXPECT_EQ(aeron_counter_get_plain(free_fails_counter), free_fails);
    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_active_network_subscriptions(&m_conductor.m_conductor, CHANNEL_1, STREAM_ID_1), 0u);
    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id, sub_id), 0);

    doWorkUntilDone();
    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 0u);
}

TEST_F(DriverConductorNetworkTest, shouldSendAvailableImageForMultipleSubscriptions)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, CHANNEL_1, STREAM_ID_1), 0);
    doWorkUntilDone();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    createPublicationImage(endpoint, STREAM_ID_1, 1000);

    aeron_publication_image_t *image = aeron_driver_conductor_find_publication_image(
        &m_conductor.m_conductor, endpoint, STREAM_ID_1);

    EXPECT_NE(image, (aeron_publication_image_t *)nullptr);
    EXPECT_EQ(aeron_publication_image_subscriber_count(image), 2u);

    int64_t image_registration_id = aeron_publication_image_registration_id(image);
    const char *log_file_name = aeron_publication_image_log_file_name(image);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
        .With(IsAvailableImage(image_registration_id, sub_id_1, STREAM_ID_1, SESSION_ID, log_file_name, SOURCE_IDENTITY));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
        .With(IsAvailableImage(image_registration_id, sub_id_2, STREAM_ID_1, SESSION_ID, log_file_name, SOURCE_IDENTITY));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldSendAvailableImageForSecondSubscriptionAfterCreatingImage)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1), 0);
    doWorkUntilDone();

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    createPublicationImage(endpoint, STREAM_ID_1, 1000);

    aeron_publication_image_t *image = aeron_driver_conductor_find_publication_image(
        &m_conductor.m_conductor, endpoint, STREAM_ID_1);

    EXPECT_NE(image, (aeron_publication_image_t *)nullptr);

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, CHANNEL_1, STREAM_ID_1), 0);
    doWorkUntilDone();

    int64_t image_registration_id = aeron_publication_image_registration_id(image);
    const char *log_file_name = aeron_publication_image_log_file_name(image);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    {
        testing::InSequence sequenced;

        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
            .With(IsSubscriptionReady(sub_id_1));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
            .With(IsAvailableImage(image_registration_id, sub_id_1, STREAM_ID_1, SESSION_ID, log_file_name, SOURCE_IDENTITY));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
            .With(IsSubscriptionReady(sub_id_2));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
            .With(IsAvailableImage(image_registration_id, sub_id_2, STREAM_ID_1, SESSION_ID, log_file_name, SOURCE_IDENTITY));
    }
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldTimeoutImageAndSendUnavailableImageWhenNoActivityForMultipleSubscriptions)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1), 0);
    doWorkUntilDone();

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    createPublicationImage(endpoint, STREAM_ID_1, 1000);

    aeron_publication_image_t *image = aeron_driver_conductor_find_publication_image(
        &m_conductor.m_conductor, endpoint, STREAM_ID_1);

    EXPECT_NE(image, (aeron_publication_image_t *)nullptr);

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, CHANNEL_1, STREAM_ID_1), 0);
    doWorkUntilDone();

    int64_t image_correlation_id = aeron_publication_image_registration_id(image);
    uint64_t timeoutNs = m_context.m_context->image_liveness_timeout_ns +
        (m_context.m_context->client_liveness_timeout_ns * 2);

    doWorkForNs(
        static_cast<int64_t>(timeoutNs),
        100,
        [&]()
        {
            clientKeepalive(client_id);
        });

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    {
        testing::InSequence sequenced;

        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
            .With(IsSubscriptionReady(sub_id_1));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
            .With(IsSubscriptionReady(sub_id_2));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_UNAVAILABLE_IMAGE, _, _))
            .With(IsUnavailableImage(STREAM_ID_1, image_correlation_id, sub_id_1, CHANNEL_1));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_UNAVAILABLE_IMAGE, _, _))
            .With(IsUnavailableImage(STREAM_ID_1, image_correlation_id, sub_id_2, CHANNEL_1));
    }
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldUseExistingChannelEndpointOnAddPublicationWithSameTagIdAndSameStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addPublication(client_id, pub_id_1, CHANNEL_1 "|tags=1001", STREAM_ID_1, false), 0);
    ASSERT_EQ(addPublication(client_id, pub_id_2, "aeron:udp?tags=1001", STREAM_ID_1, false), 0);
    doWorkUntilDone();
    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    uint64_t timeoutNs =
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2);
    doWorkForNs(static_cast<int64_t>(timeoutNs));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 0u);
}

TEST_F(DriverConductorNetworkTest, shouldNotLogRetryableResourceTemporarilyUnavailableToDistinctErrorLog)
{
    const int64_t correlation_id = nextCorrelationId();
    const size_t observations_before =
        aeron_distinct_error_log_num_observations(&m_conductor.m_conductor.error_log);

    // RESOURCE_TEMPORARILY_UNAVAILABLE is a transient, retryable condition reported to the client (for example
    // adding a publication while a send channel endpoint is still closing). It must not be recorded in the
    // distinct error log, matching the Java driver which only sends it to the client.
    aeron_driver_conductor_on_error(
        &m_conductor.m_conductor,
        -AERON_ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE,
        "send_channel_endpoint found in CLOSING state, please retry",
        correlation_id);

    EXPECT_EQ(observations_before,
        aeron_distinct_error_log_num_observations(&m_conductor.m_conductor.error_log));

    // A genuine (non-retryable) error must still be recorded in the distinct error log.
    aeron_driver_conductor_on_error(
        &m_conductor.m_conductor,
        -AERON_ERROR_CODE_INVALID_CHANNEL,
        "invalid channel",
        correlation_id);

    EXPECT_EQ(observations_before + 1,
        aeron_distinct_error_log_num_observations(&m_conductor.m_conductor.error_log));
}

TEST_F(DriverConductorNetworkTest, shouldNotEvictRecreatedSendChannelEndpointWhenPriorEndpointDeferredDeleteRuns)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();

    // Create the initial send channel endpoint for the channel.
    ASSERT_EQ(addPublication(client_id, pub_id_1, CHANNEL_1, STREAM_ID_1, false), 0);
    doWorkUntilDone();
    ASSERT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 1u);

    aeron_send_channel_endpoint_t *endpoint_a = aeron_driver_conductor_find_send_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);
    ASSERT_NE(endpoint_a, nullptr);

    char canonical[1024];
    size_t canonical_length = endpoint_a->conductor_fields.udp_channel->canonical_length;
    ASSERT_LT(canonical_length, sizeof(canonical));
    memcpy(canonical, endpoint_a->conductor_fields.udp_channel->canonical_form, canonical_length);

    aeron_str_to_ptr_hash_map_t *endpoint_map =
        &m_conductor.m_conductor.send_channel_endpoint_by_channel_map;

    // Begin teardown of the endpoint by removing its only publication.
    int64_t remove_id = nextCorrelationId();
    ASSERT_EQ(removePublication(client_id, remove_id, pub_id_1), 0);
    doWork();

    // Advance past the publication linger (one timer cycle per iteration) until the publication is freed,
    // which drops the endpoint reference count to zero and transitions it to CLOSING.
    int guard = 0;
    while (0u != aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor) && guard++ < 1000)
    {
        test_increment_nano_time((int64_t)m_context.m_context->publication_linger_timeout_ns + 1);
        clientKeepalive(client_id);
        doWork();
    }

    // Complete the conductor->sender->conductor release round-trip with plain doWork() calls. doWork() does
    // not advance the test clock, so the managed-resource sweep does not run: this leaves the endpoint removed
    // from the lookup map (by on_release_resource) but still present in the endpoint array awaiting its
    // deferred delete.
    guard = 0;
    while (nullptr != aeron_str_to_ptr_hash_map_get(endpoint_map, canonical, canonical_length) &&
        guard++ < 1000)
    {
        doWork();
    }

    // Window reached: prior endpoint is out of the lookup map but still in the array.
    ASSERT_EQ(nullptr, aeron_str_to_ptr_hash_map_get(endpoint_map, canonical, canonical_length));
    ASSERT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 1u);

    // Recreate an endpoint for the same channel. It is mapped under the same canonical form as the prior,
    // not-yet-deleted endpoint.
    int64_t pub_id_2 = nextCorrelationId();
    ASSERT_EQ(addPublication(client_id, pub_id_2, CHANNEL_1, STREAM_ID_1, false), 0);
    doWorkUntilDone();

    void *mapped_b = aeron_str_to_ptr_hash_map_get(endpoint_map, canonical, canonical_length);
    ASSERT_NE(nullptr, mapped_b);
    ASSERT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 2u);

    // Fire managed-resource sweeps until the prior end-of-life endpoint is deferred-deleted. The deferred
    // delete must not remove the map entry that now belongs to the recreated successor.
    int sweep_guard = 0;
    while (aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor) > 1u &&
        sweep_guard++ < 1000)
    {
        test_increment_nano_time((int64_t)m_context.m_context->timer_interval_ns + 1);
        clientKeepalive(client_id);
        doWork();
    }
    ASSERT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 1u);

    // Regression: a stale-handle deferred delete used to remove the map entry by canonical form
    // unconditionally, evicting the live successor and leaving the lookup map and endpoint array inconsistent.
    EXPECT_EQ(mapped_b, aeron_str_to_ptr_hash_map_get(endpoint_map, canonical, canonical_length));

    // A subsequent publication on the same channel must reuse the mapped endpoint rather than create a
    // duplicate (which, against a real media driver, collides on the bind with EADDRINUSE).
    int64_t pub_id_3 = nextCorrelationId();
    ASSERT_EQ(addPublication(client_id, pub_id_3, CHANNEL_1, STREAM_ID_1, false), 0);
    doWorkUntilDone();
    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 1u);
}

TEST_F(DriverConductorNetworkTest, shouldUseExistingChannelEndpointOnAddPublicationWithSameTagIdDifferentStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addPublication(client_id, pub_id_1, CHANNEL_1 "|tags=1001", STREAM_ID_1, false), 0);
    ASSERT_EQ(addPublication(client_id, pub_id_2, "aeron:udp?tags=1001", STREAM_ID_2, false), 0);
    doWorkUntilDone();
    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 2u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    uint64_t timeoutNs =
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2);
    doWorkForNs(static_cast<int64_t>(timeoutNs));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 0u);
}

TEST_F(DriverConductorNetworkTest, shouldUseExistingChannelEndpointOnAddSubscriptionWithSameTagId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1 "|tags=1001", STREAM_ID_1), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, "aeron:udp?tags=1001", STREAM_ID_1), 0);
    doWorkUntilDone();
    EXPECT_EQ(aeron_driver_conductor_num_receive_channel_endpoints(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 2u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    uint64_t timeoutNs =
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2);
    doWorkForNs(static_cast<int64_t>(timeoutNs));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_receive_channel_endpoints(&m_conductor.m_conductor), 0u);
}

TEST_F(DriverConductorNetworkTest, shouldUseExistingPublicationOnAddPublicationWithSameSessionTagIdAndSameStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addPublication(client_id, pub_id_1, CHANNEL_1 "|tags=1001,1002", STREAM_ID_1, false), 0);
    ASSERT_EQ(addPublication(client_id, pub_id_2, CHANNEL_2 "|session-id=tag:1002", STREAM_ID_1, false), 0);
    doWorkUntilDone();
    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 2u);
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 2u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    aeron_network_publication_t *pub_1 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_1);
    aeron_network_publication_t *pub_2 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_2);

    EXPECT_EQ(pub_1->session_id, pub_2->session_id);

    uint64_t timeoutNs =
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2);
    doWorkForNs(static_cast<int64_t>(timeoutNs));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 0u);
}

TEST_F(DriverConductorNetworkTest, shouldErrorWithUnknownSessionIdTag)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();

    ASSERT_EQ(addPublication(client_id, pub_id_1, CHANNEL_2 "|session-id=tag:1002", STREAM_ID_1, false), 0);
    doWorkUntilDone();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldErrorWithInvalidSessionIdTag)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();

    ASSERT_EQ(addPublication(client_id, pub_id_1, CHANNEL_1 "|tags=1001,1002", STREAM_ID_1, false), 0);
    doWorkUntilDone();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(addPublication(client_id, pub_id_1, CHANNEL_2 "|session-id=tag:1002a", STREAM_ID_1, false), 0);
    doWorkUntilDone();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToAddAndRemoveDestinationToManualMdcPublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();
    int64_t add_destination_id = nextCorrelationId();
    int64_t remove_destination_id = nextCorrelationId();

    ASSERT_EQ(addPublication(client_id, pub_id, CHANNEL_MDC_MANUAL, STREAM_ID_1, false), 0);
    doWorkUntilDone();
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(addDestination(client_id, add_destination_id, pub_id, CHANNEL_1), 0);
    doWorkUntilDone();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
        .With(IsOperationSuccess(add_destination_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);

    ASSERT_EQ(removeDestination(client_id, remove_destination_id, pub_id, CHANNEL_1), 0);
    doWorkUntilDone();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
        .With(IsOperationSuccess(remove_destination_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldFailToAddSubscriptionWithDifferentReliabilityParameter)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
    doWorkUntilDone();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1_UNRELIABLE, STREAM_ID_1), 0);
    doWorkUntilDone();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldAllowDifferentReliabilityParameterWithSpecificSession)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1_UNRELIABLE, STREAM_ID_1), 0);
    doWorkUntilDone();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1_WITH_SESSION_ID_1, STREAM_ID_1), 0);
    doWorkUntilDone();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldAddMdsWithSingleUnicastSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_MDC_MANUAL, STREAM_ID_1), 0);
    doWorkUntilDone();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);

    int64_t dest_correlation_id = nextCorrelationId();

    ASSERT_EQ(addReceiveDestination(client_id, dest_correlation_id, sub_id, CHANNEL_1), 0);
    doWorkUntilDone();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldFailToAddMdsWithSingleUnicastSubscriptionWithInvalidRegistrationId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t invalid_sub_id = sub_id + 1000000;

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_MDC_MANUAL, STREAM_ID_1), 0);
    doWorkUntilDone();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);

    int64_t dest_correlation_id = nextCorrelationId();

    ASSERT_EQ(addReceiveDestination(client_id, dest_correlation_id, invalid_sub_id, CHANNEL_1), 0);
    doWorkUntilDone();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldFailToAddMdsWithSingleUnicastSubscriptionWithInvalidNonManualControlEndpoint)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_2, STREAM_ID_1), 0);
    doWorkUntilDone();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);

    int64_t dest_correlation_id = nextCorrelationId();

    ASSERT_EQ(addReceiveDestination(client_id, dest_correlation_id, sub_id, CHANNEL_1), 0);
    doWorkUntilDone();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldAddAndRemoveMdsWithSingleUnicastSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_MDC_MANUAL, STREAM_ID_1), 0);
    doWorkUntilDone();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    int64_t dest_correlation_id = nextCorrelationId();

    ASSERT_EQ(addReceiveDestination(client_id, dest_correlation_id, sub_id, CHANNEL_1), 0);
    doWorkUntilDone();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(removeReceiveDestination(client_id, remove_correlation_id, sub_id, CHANNEL_1), 0);
    doWorkUntilDone();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldFailToRemoveMdsWithSingleUnicastSubscriptionWithInvalidSusbcriptionId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t invalid_sub_id = sub_id + 1000000;

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_MDC_MANUAL, STREAM_ID_1), 0);
    doWorkUntilDone();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    int64_t dest_correlation_id = nextCorrelationId();

    ASSERT_EQ(addReceiveDestination(client_id, dest_correlation_id, sub_id, CHANNEL_1), 0);
    doWorkUntilDone();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(removeReceiveDestination(client_id, remove_correlation_id, invalid_sub_id, CHANNEL_1), 0);
    doWorkUntilDone();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldFailToAddPublicationWithAtsEnabled)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addPublication(client_id, pub_id, CHANNEL_1 "|ats=true", STREAM_ID_1, false), 0);
    doWorkUntilDone();
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldFailToAddSubscriptionWithAtsEnabled)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1 "|ats=true", STREAM_ID_1), 0);
    doWorkUntilDone();
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
    EXPECT_EQ(1, readAllBroadcastsFromConductor(mock_broadcast_handler));
}

TEST_F(DriverConductorNetworkTest, shouldSkipControlAddressReResolutionIfEndpointIsNotActive)
{
    aeron_receive_channel_endpoint_t endpoint;
    endpoint.conductor_fields.status = AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_CLOSING;
    m_conductor.m_conductor.context->receiver_proxy = nullptr;

    aeron_command_re_resolve_t cmd;
    cmd.endpoint_name = "#@#%@&^%#@$*%*@$ this won't resolve #%$*&$@%^";
    cmd.endpoint = &endpoint;
    cmd.destination = nullptr;
    memset(&cmd.existing_addr, 0, sizeof(cmd.existing_addr));

    auto driverErrorCounter =
        aeron_counters_manager_addr(&m_context.m_counters_manager, AERON_SYSTEM_COUNTER_ID_ERRORS);
    EXPECT_EQ(0, aeron_counter_get_acquire(driverErrorCounter));

    aeron_driver_conductor_on_re_resolve_control(&m_conductor.m_conductor, &cmd);

    doWork();
    doWork();

    // error counter zero means that the command was not executed
    EXPECT_EQ(0, aeron_counter_get_acquire(driverErrorCounter));
}

TEST_F(DriverConductorNetworkTest, shouldNotNotifyControlAddressReResolutionResultsIfEndpointIsNotActiveWhenTaskCompletes)
{
    aeron_receive_channel_endpoint_t endpoint;
    endpoint.conductor_fields.status = AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_ACTIVE;
    m_conductor.m_conductor.context->receiver_proxy = nullptr;

    aeron_command_re_resolve_t cmd;
    cmd.endpoint_name = "127.0.0.1:10101";
    cmd.endpoint = &endpoint;
    cmd.destination = nullptr;
    memset(&cmd.existing_addr, 0, sizeof(cmd.existing_addr));

    auto driverErrorCounter =
        aeron_counters_manager_addr(&m_context.m_counters_manager, AERON_SYSTEM_COUNTER_ID_ERRORS);
    EXPECT_EQ(0, aeron_counter_get_acquire(driverErrorCounter));

    aeron_driver_conductor_on_re_resolve_control(&m_conductor.m_conductor, &cmd);
    endpoint.conductor_fields.status = AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_CLOSED;

    doWork();
    doWork();

    EXPECT_EQ(0, aeron_counter_get_acquire(driverErrorCounter));
}

TEST_F(DriverConductorNetworkTest, shouldReportControlAddressReResolutionErrorEvenIfEndpointIsNotActiveWhenTaskCompletes)
{
    aeron_receive_channel_endpoint_t endpoint;
    endpoint.conductor_fields.status = AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_ACTIVE;
    m_conductor.m_conductor.context->receiver_proxy = nullptr;

    aeron_command_re_resolve_t cmd;
    cmd.endpoint_name = "*#&(%#%&";
    cmd.endpoint = &endpoint;
    cmd.destination = nullptr;
    memset(&cmd.existing_addr, 0, sizeof(cmd.existing_addr));

    auto driverErrorCounter =
        aeron_counters_manager_addr(&m_context.m_counters_manager, AERON_SYSTEM_COUNTER_ID_ERRORS);
    EXPECT_EQ(0, aeron_counter_get_acquire(driverErrorCounter));

    aeron_driver_conductor_on_re_resolve_control(&m_conductor.m_conductor, &cmd);
    endpoint.conductor_fields.status = AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_CLOSING;

    doWork();
    doWork();

    EXPECT_EQ(1, aeron_counter_get_acquire(driverErrorCounter));
    EXPECT_EQ(0, aeron_errcode());
}

TEST_F(DriverConductorNetworkTest, shouldSkipEndpointAddressReResolutionIfEndpointIsNotActive)
{
    aeron_send_channel_endpoint_t endpoint;
    endpoint.conductor_fields.status = AERON_SEND_CHANNEL_ENDPOINT_STATUS_CLOSED;
    m_conductor.m_conductor.context->sender_proxy = nullptr;

    aeron_command_re_resolve_t cmd;
    cmd.endpoint_name = "#@#%@&^%#@$*%*@$ this won't resolve #%$*&$@%^";
    cmd.endpoint = &endpoint;
    cmd.destination = nullptr;
    memset(&cmd.existing_addr, 0, sizeof(cmd.existing_addr));

    auto driverErrorCounter =
        aeron_counters_manager_addr(&m_context.m_counters_manager, AERON_SYSTEM_COUNTER_ID_ERRORS);
    EXPECT_EQ(0, aeron_counter_get_acquire(driverErrorCounter));

    aeron_driver_conductor_on_re_resolve_endpoint(&m_conductor.m_conductor, &cmd);

    doWork();
    doWork();

    // error counter zero means that the command was not executed
    EXPECT_EQ(0, aeron_counter_get_acquire(driverErrorCounter));
}

TEST_F(DriverConductorNetworkTest, shouldNotNotifyEndpointAddressReResolutionResultsIfEndpointIsNotActiveWhenTaskCompletes)
{
    aeron_send_channel_endpoint_t endpoint;
    endpoint.conductor_fields.status = AERON_SEND_CHANNEL_ENDPOINT_STATUS_ACTIVE;
    m_conductor.m_conductor.context->sender_proxy = nullptr;

    aeron_command_re_resolve_t cmd;
    cmd.endpoint_name = "127.0.0.1:10101";
    cmd.endpoint = &endpoint;
    cmd.destination = nullptr;
    memset(&cmd.existing_addr, 0, sizeof(cmd.existing_addr));

    auto driverErrorCounter =
        aeron_counters_manager_addr(&m_context.m_counters_manager, AERON_SYSTEM_COUNTER_ID_ERRORS);
    EXPECT_EQ(0, aeron_counter_get_acquire(driverErrorCounter));

    aeron_driver_conductor_on_re_resolve_endpoint(&m_conductor.m_conductor, &cmd);
    endpoint.conductor_fields.status = AERON_SEND_CHANNEL_ENDPOINT_STATUS_CLOSING;

    doWork();
    doWork();

    EXPECT_EQ(0, aeron_counter_get_acquire(driverErrorCounter));
}

TEST_F(DriverConductorNetworkTest, shouldReportEndpointAddressReResolutionErrorEvenIfEndpointIsNotActiveWhenTaskCompletes)
{
    aeron_send_channel_endpoint_t endpoint;
    endpoint.conductor_fields.status = AERON_SEND_CHANNEL_ENDPOINT_STATUS_ACTIVE;
    m_conductor.m_conductor.context->sender_proxy = nullptr;

    aeron_command_re_resolve_t cmd;
    cmd.endpoint_name = "*#&(%#%&";
    cmd.endpoint = &endpoint;
    cmd.destination = nullptr;
    memset(&cmd.existing_addr, 0, sizeof(cmd.existing_addr));

    auto driverErrorCounter =
        aeron_counters_manager_addr(&m_context.m_counters_manager, AERON_SYSTEM_COUNTER_ID_ERRORS);
    EXPECT_EQ(0, aeron_counter_get_acquire(driverErrorCounter));

    aeron_driver_conductor_on_re_resolve_endpoint(&m_conductor.m_conductor, &cmd);
    endpoint.conductor_fields.status = AERON_SEND_CHANNEL_ENDPOINT_STATUS_CLOSED;

    doWork();
    doWork();

    EXPECT_EQ(1, aeron_counter_get_acquire(driverErrorCounter));
    EXPECT_EQ(0, aeron_errcode());
}
