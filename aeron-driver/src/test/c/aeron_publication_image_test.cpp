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

#include <vector>
#include "aeron_receiver_test.h"

extern "C"
{
#include "aeron_publication_image.h"
#include "aeron_data_packet_dispatcher.h"
#include "aeron_driver_receiver.h"
}

#define CAPACITY (32 * 1024)
#define TERM_BUFFER_SIZE (64 * 1024)
#define MTU (4096)

typedef std::array<std::uint8_t, CAPACITY> buffer_t;
typedef std::array<std::uint8_t, 4 * CAPACITY> buffer_4x_t;

static bool always_measure_rtt(void *state, int64_t now_ns)
{
    return true;
}

class PublicationImageTest : public ReceiverTestBase, public testing::Test
{
public:
    void SetUp() final
    {
        DoSetUp();
    }

    void TearDown() final
    {
        DoTearDown();
    }
};

class ZeroDelayFeedbackGeneratorTest : public ReceiverTestBase, public testing::TestWithParam<bool>
{
public:
    void SetUp() final
    {
        DoSetUp();
    }

    void TearDown() final
    {
        DoTearDown();
    }
};

INSTANTIATE_TEST_SUITE_P(
    ZeroDelayFeedbackGeneratorTests,
    ZeroDelayFeedbackGeneratorTest,
    testing::Values(true, false)
);

TEST_P(ZeroDelayFeedbackGeneratorTest, shouldConfiguredZeroDelayFeedbackGeneratorIfSubscriptionIsUnreliable)
{
    bool treat_as_multicast = GetParam();

    const char *uri = "aeron:udp?endpoint=localhost:9090|nak-delay=5ms";
    aeron_receive_channel_endpoint_t *endpoint = createEndpoint(uri);
    int32_t stream_id = 777;
    int32_t session_id = 42;
    int64_t registration_id = 0;

    aeron_udp_channel_t *channel;
    aeron_receive_destination_t *dest;

    ASSERT_EQ(0, aeron_udp_channel_parse(strlen(uri), uri, &m_resolver, &channel, false));

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest,
        channel,
        channel,
        m_context,
        &m_counters_manager,
        registration_id,
        endpoint->channel_status.counter_id));

    aeron_publication_image_t *image = createImage(endpoint, dest, stream_id, session_id, 111, false, treat_as_multicast);
    ASSERT_NE(nullptr, image) << aeron_errmsg();

    auto delay_generator_state = image->loss_detector.feedback_delay_state;
    EXPECT_EQ(0, delay_generator_state->static_delay.delay_ns);
    EXPECT_EQ(0, delay_generator_state->static_delay.retry_ns);
    EXPECT_NE(&m_context->multicast_delay_feedback_generator, delay_generator_state);
    EXPECT_NE(&m_context->unicast_delay_feedback_generator, delay_generator_state);

    aeron_publication_image_remove_destination(image, endpoint->conductor_fields.udp_channel);
    endpoint->transport_bindings->poller_remove_func(&m_receiver.poller, &dest->transport);
    endpoint->transport_bindings->close_func(&dest->transport);
    aeron_receive_destination_delete(dest, &m_counters_manager);
}

TEST_F(PublicationImageTest, shouldAddAndRemoveDestination)
{
    const char *uri_1 = "aeron:udp?endpoint=localhost:9090";
    const char *uri_2 = "aeron:udp?endpoint=localhost:9091";
    const char *uri_3 = "aeron:udp?endpoint=localhost:9093";
    aeron_receive_channel_endpoint_t *endpoint = createMdsEndpoint();
    int64_t registration_id = 0;
    int32_t stream_id = 1001;
    int32_t session_id = 1000001;
    aeron_receive_destination_t *destination = nullptr;

    aeron_udp_channel_t *channel_1 = createChannel(uri_1);
    aeron_receive_destination_t *dest_1;

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_1,
        channel_1,
        channel_1,
        m_context,
        &m_counters_manager,
        registration_id,
        endpoint->channel_status.counter_id));
    ASSERT_EQ(1, aeron_receive_channel_endpoint_add_destination(endpoint, dest_1));

    aeron_publication_image_t *image = createImage(endpoint, dest_1, stream_id, session_id);

    aeron_udp_channel_t *channel_2 = createChannel(uri_2);
    aeron_receive_destination_t *dest_2;

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_2,
        channel_2,
        channel_2,
        m_context,
        &m_counters_manager,
        registration_id,
        endpoint->channel_status.counter_id));
    ASSERT_EQ(2, aeron_receive_channel_endpoint_add_destination(endpoint, dest_2));

    ASSERT_EQ(2, aeron_publication_image_add_destination(image, dest_2));

    aeron_udp_channel_t *remove_channel_1 = createChannel(uri_1, &m_channels_for_tear_down);

    ASSERT_EQ(1, aeron_receive_channel_endpoint_remove_destination(endpoint, remove_channel_1, &destination));
    endpoint->transport_bindings->poller_remove_func(&m_receiver.poller, &dest_1->transport);
    endpoint->transport_bindings->close_func(&dest_1->transport);

    ASSERT_EQ(1u, endpoint->destinations.length);
    ASSERT_EQ(1, aeron_publication_image_remove_destination(image, remove_channel_1));
    ASSERT_EQ(1u, image->connections.length);
    ASSERT_EQ(dest_1, destination);
    aeron_receive_destination_delete(dest_1, &m_counters_manager);

    aeron_udp_channel_t *channel_not_added = createChannel(uri_3, &m_channels_for_tear_down);

    destination = nullptr;
    ASSERT_EQ(0, aeron_receive_channel_endpoint_remove_destination(endpoint, channel_not_added, &destination));
    ASSERT_EQ(1u, endpoint->destinations.length);
    ASSERT_EQ(0, aeron_publication_image_remove_destination(image, channel_not_added));
    ASSERT_EQ(1u, image->connections.length);
    ASSERT_EQ((aeron_receive_destination_t *) nullptr, destination);

    aeron_udp_channel_t *remove_channel_2 = createChannel(uri_2, &m_channels_for_tear_down);

    ASSERT_EQ(1, aeron_receive_channel_endpoint_remove_destination(endpoint, remove_channel_2, &destination));
    endpoint->transport_bindings->poller_remove_func(&m_receiver.poller, &dest_2->transport);
    endpoint->transport_bindings->close_func(&dest_2->transport);

    ASSERT_EQ(0u, endpoint->destinations.length);
    ASSERT_EQ(1, aeron_publication_image_remove_destination(image, remove_channel_2));
    ASSERT_EQ(0u, image->connections.length);
    ASSERT_EQ(dest_2, destination);
    aeron_receive_destination_delete(dest_2, &m_counters_manager);
}

TEST_F(PublicationImageTest, shouldSendControlMessagesToAllDestinations)
{
    struct sockaddr_storage addr = {}; // Don't really care what value this is.
    uint8_t data[128];
    auto *message = reinterpret_cast<aeron_data_header_t *>(data);
    const char *uri_1 = "aeron:udp?endpoint=localhost:9090";
    const char *uri_2 = "aeron:udp?endpoint=localhost:9091";
    aeron_receive_channel_endpoint_t *endpoint = createMdsEndpoint();
    int32_t stream_id = 1001;
    int32_t session_id = 1000001;
    int64_t registration_id = 0;

    aeron_udp_channel_t *channel_1;
    aeron_receive_destination_t *dest_1;
    aeron_udp_channel_t *channel_2;
    aeron_receive_destination_t *dest_2;

    aeron_udp_channel_parse(strlen(uri_1), uri_1, &m_resolver, &channel_1, false);
    aeron_udp_channel_parse(strlen(uri_2), uri_2, &m_resolver, &channel_2, false);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_1,
        channel_1,
        channel_1,
        m_context,
        &m_counters_manager,
        registration_id,
        endpoint->channel_status.counter_id));
    ASSERT_EQ(1, aeron_receive_channel_endpoint_add_destination(endpoint, dest_1));

    aeron_publication_image_t *image = createImage(endpoint, dest_1, stream_id, session_id);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_2,
        channel_2,
        channel_2,
        m_context,
        &m_counters_manager,
        registration_id,
        endpoint->channel_status.counter_id));
    ASSERT_EQ(2, aeron_receive_channel_endpoint_add_destination(endpoint, dest_2));

    ASSERT_EQ(2, aeron_publication_image_add_destination(image, dest_2));

    ASSERT_EQ(AERON_PUBLICATION_IMAGE_STATE_ACTIVE, image->conductor_fields.state);
    image->congestion_control->should_measure_rtt = always_measure_rtt;

    auto *bindings_state_dest1 = static_cast<aeron_test_udp_bindings_state_t *>(dest_1->transport.bindings_clientd);
    auto *bindings_state_dest2 = static_cast<aeron_test_udp_bindings_state_t *>(dest_2->transport.bindings_clientd);

    aeron_publication_image_schedule_status_message(image, 0, TERM_BUFFER_SIZE);
    aeron_publication_image_send_pending_status_message(image, 1000000000);
    ASSERT_EQ(1, bindings_state_dest1->sm_count);
    ASSERT_EQ(0, bindings_state_dest2->sm_count);

    aeron_publication_image_on_gap_detected(image, 0, 0, 1);
    aeron_publication_image_send_pending_loss(image);
    ASSERT_EQ(1, bindings_state_dest1->nak_count);

    aeron_publication_image_initiate_rttm(image, 1000000000);
    ASSERT_EQ(1, bindings_state_dest1->rttm_count);

    message->stream_id = stream_id;
    message->session_id = session_id;
    message->frame_header.frame_length = 64;
    message->term_id = 0;
    message->term_offset = 0;

    aeron_publication_image_insert_packet(image, dest_2, 0, 0, data, 64, &addr, nullptr);

    aeron_publication_image_schedule_status_message(image, 1, TERM_BUFFER_SIZE);
    aeron_publication_image_send_pending_status_message(image, 2000000000);
    ASSERT_EQ(2, bindings_state_dest1->sm_count);
    ASSERT_EQ(1, bindings_state_dest2->sm_count);
    ASSERT_EQ(3, aeron_counter_get_plain(image->status_messages_sent_counter));

    aeron_publication_image_on_gap_detected(image, 0, 0, 1);
    aeron_publication_image_send_pending_loss(image);
    ASSERT_EQ(2, bindings_state_dest1->nak_count);
    ASSERT_EQ(1, bindings_state_dest2->nak_count);
    ASSERT_EQ(3, aeron_counter_get_plain(image->nak_messages_sent_counter));

    aeron_publication_image_initiate_rttm(image, 2000000000);
    ASSERT_EQ(2, bindings_state_dest1->rttm_count);
}

TEST_F(PublicationImageTest, shouldRemovePendingSetupOnRemoveMdsDestinationWithControlEndpoint)
{
    aeron_receive_channel_endpoint_t *endpoint = createMdsEndpoint();
    aeron_udp_channel_t *channel = createChannel("aeron:udp?control=localhost:9092");

    aeron_receive_destination_t *destination = nullptr;
    ASSERT_EQ(0, aeron_receive_destination_create(
        &destination, channel, channel, m_context, &m_counters_manager, 0, endpoint->channel_status.counter_id));

    aeron_command_add_rcv_destination_t add_cmd = {};
    add_cmd.endpoint = endpoint;
    add_cmd.destination = destination;
    aeron_driver_receiver_on_add_destination(&m_receiver, &add_cmd);
    ASSERT_EQ(1U, m_receiver.pending_setups.length);

    aeron_command_remove_rcv_destination_t remove_cmd = {};
    remove_cmd.endpoint = endpoint;
    remove_cmd.channel = channel;
    aeron_driver_receiver_on_remove_destination(&m_receiver, &remove_cmd);
    ASSERT_EQ(0U, m_receiver.pending_setups.length);
}

TEST_F(PublicationImageTest, shouldHandleEosAcrossDestinations)
{
    struct sockaddr_storage addr = {}; // Don't really care what value this is.
    uint8_t data[128];
    memset(data, 0, sizeof(data));

    auto *heartbeat = reinterpret_cast<aeron_data_header_t *>(data);
    const char *uri_1 = "aeron:udp?endpoint=localhost:9090";
    const char *uri_2 = "aeron:udp?endpoint=localhost:9091";
    aeron_receive_channel_endpoint_t *endpoint = createMdsEndpoint();
    int32_t stream_id = 1001;
    int32_t session_id = 1000001;
    int64_t registration_id = 0;

    aeron_udp_channel_t *channel_1;
    aeron_receive_destination_t *dest_1;
    aeron_udp_channel_t *channel_2;
    aeron_receive_destination_t *dest_2;

    aeron_udp_channel_parse(strlen(uri_1), uri_1, &m_resolver, &channel_1, false);
    aeron_udp_channel_parse(strlen(uri_2), uri_2, &m_resolver, &channel_2, false);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_1,
        channel_1,
        channel_1,
        m_context,
        &m_counters_manager,
        registration_id,
        endpoint->channel_status.counter_id));
    ASSERT_EQ(1, aeron_receive_channel_endpoint_add_destination(endpoint, dest_1));

    aeron_publication_image_t *image = createImage(endpoint, dest_1, stream_id, session_id);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_2,
        channel_2,
        channel_2,
        m_context,
        &m_counters_manager,
        registration_id,
        endpoint->channel_status.counter_id));
    ASSERT_EQ(2, aeron_receive_channel_endpoint_add_destination(endpoint, dest_2));

    ASSERT_EQ(2, aeron_publication_image_add_destination(image, dest_2));

    ASSERT_EQ(AERON_PUBLICATION_IMAGE_STATE_ACTIVE, image->conductor_fields.state);
    image->congestion_control->should_measure_rtt = always_measure_rtt;

    heartbeat->stream_id = stream_id;
    heartbeat->session_id = session_id;
    heartbeat->frame_header.frame_length = 0;
    heartbeat->term_id = 0;
    heartbeat->term_offset = 0;
    heartbeat->frame_header.flags |= AERON_DATA_HEADER_EOS_FLAG;

    bool is_eos = true;
    AERON_GET_ACQUIRE(is_eos, image->is_end_of_stream);
    ASSERT_EQ(false, is_eos);

    aeron_publication_image_insert_packet(image, dest_2, 0, 0, data, AERON_DATA_HEADER_LENGTH, &addr, nullptr);

    AERON_GET_ACQUIRE(is_eos, image->is_end_of_stream);
    ASSERT_EQ(false, is_eos);

    aeron_publication_image_insert_packet(image, dest_1, 0, 0, data, AERON_DATA_HEADER_LENGTH, &addr, nullptr);

    AERON_GET_ACQUIRE(is_eos, image->is_end_of_stream);
    ASSERT_EQ(true, is_eos);
}

TEST_F(PublicationImageTest, shouldNotSendControlMessagesToAllDestinationThatHaveNotBeenActive)
{
    struct sockaddr_storage addr = {}; // Don't really care what value this is.
    uint8_t data[128];
    auto *message = reinterpret_cast<aeron_data_header_t *>(data);
    const char *uri_1 = "aeron:udp?endpoint=localhost:9090";
    const char *uri_2 = "aeron:udp?endpoint=localhost:9091";
    aeron_receive_channel_endpoint_t *endpoint = createMdsEndpoint();
    int32_t stream_id = 1001;
    int32_t session_id = 1000001;
    int64_t registration_id = 0;

    int64_t t0_ns = 1000 * 1000 * 1000;
    int64_t t1_ns = t0_ns + (2 * AERON_RECEIVE_DESTINATION_TIMEOUT_NS);

    aeron_udp_channel_t *channel_1;
    aeron_receive_destination_t *dest_1;
    aeron_udp_channel_t *channel_2;
    aeron_receive_destination_t *dest_2;

    aeron_udp_channel_parse(strlen(uri_1), uri_1, &m_resolver, &channel_1, false);
    aeron_udp_channel_parse(strlen(uri_2), uri_2, &m_resolver, &channel_2, false);

    aeron_clock_update_cached_nano_time(m_context->receiver_cached_clock, t0_ns);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_1,
        channel_1,
        channel_1,
        m_context,
        &m_counters_manager,
        registration_id,
        endpoint->channel_status.counter_id));
    ASSERT_EQ(1, aeron_receive_channel_endpoint_add_destination(endpoint, dest_1));

    aeron_publication_image_t *image = createImage(endpoint, dest_1, stream_id, session_id);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_2,
        channel_2,
        channel_2,
        m_context,
        &m_counters_manager,
        registration_id,
        endpoint->channel_status.counter_id));
    ASSERT_EQ(2, aeron_receive_channel_endpoint_add_destination(endpoint, dest_2));

    ASSERT_EQ(2, aeron_publication_image_add_destination(image, dest_2));

    ASSERT_EQ(AERON_PUBLICATION_IMAGE_STATE_ACTIVE, image->conductor_fields.state);
    image->congestion_control->should_measure_rtt = always_measure_rtt;

    auto *bindings_state_dest1 = static_cast<aeron_test_udp_bindings_state_t *>(dest_1->transport.bindings_clientd);

    size_t message_length = 64;

    message->stream_id = stream_id;
    message->session_id = session_id;
    message->frame_header.frame_length = (int32_t) message_length;
    message->term_id = 0;
    message->term_offset = 0;

    aeron_publication_image_insert_packet(image, dest_1, 0, 0, data, message_length, &addr, nullptr);
    aeron_publication_image_insert_packet(image, dest_2, 0, 0, data, message_length, &addr, nullptr);

    aeron_clock_update_cached_nano_time(m_context->receiver_cached_clock, t1_ns);

    auto next_offset = (int32_t) message_length;
    message->term_offset = next_offset;

    aeron_publication_image_insert_packet(image, dest_2, 0, next_offset, data, message_length, &addr, nullptr);

    aeron_publication_image_schedule_status_message(image, 1, TERM_BUFFER_SIZE);
    aeron_publication_image_send_pending_status_message(image, t1_ns);
    EXPECT_EQ(0, bindings_state_dest1->sm_count);

    aeron_publication_image_on_gap_detected(image, 0, 0, 1);
    aeron_publication_image_send_pending_loss(image);
    EXPECT_EQ(0, bindings_state_dest1->nak_count);

    aeron_publication_image_initiate_rttm(image, t1_ns);
    EXPECT_EQ(0, bindings_state_dest1->rttm_count);
}

TEST_F(PublicationImageTest, shouldTrackActiveTransportAccountBasedOnFrames)
{
    struct sockaddr_storage addr = {}; // Don't really care what value this is.
    uint8_t data[128];
    auto *message = reinterpret_cast<aeron_data_header_t *>(data);
    const char *uri_1 = "aeron:udp?endpoint=localhost:9090";
    const char *uri_2 = "aeron:udp?endpoint=localhost:9091";
    aeron_receive_channel_endpoint_t *endpoint = createMdsEndpoint();
    int32_t stream_id = 1001;
    int32_t session_id = 1000001;
    int64_t registration_id = 0;

    int64_t t0_ns = static_cast<int64_t>(2 * m_context->image_liveness_timeout_ns);

    aeron_udp_channel_t *channel_1;
    aeron_receive_destination_t *dest_1;
    aeron_udp_channel_t *channel_2;
    aeron_receive_destination_t *dest_2;

    aeron_udp_channel_parse(strlen(uri_1), uri_1, &m_resolver, &channel_1, false);
    aeron_udp_channel_parse(strlen(uri_2), uri_2, &m_resolver, &channel_2, false);

    aeron_clock_update_cached_nano_time(m_context->receiver_cached_clock, t0_ns);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_1,
        channel_1,
        channel_1,
        m_context,
        &m_counters_manager,
        registration_id,
        endpoint->channel_status.counter_id));
    ASSERT_EQ(1, aeron_receive_channel_endpoint_add_destination(endpoint, dest_1));

    aeron_publication_image_t *image = createImage(endpoint, dest_1, stream_id, session_id);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_2,
        channel_2,
        channel_2,
        m_context,
        &m_counters_manager,
        registration_id,
        endpoint->channel_status.counter_id));
    ASSERT_EQ(2, aeron_receive_channel_endpoint_add_destination(endpoint, dest_2));

    ASSERT_EQ(2, aeron_publication_image_add_destination(image, dest_2));

    ASSERT_EQ(AERON_PUBLICATION_IMAGE_STATE_ACTIVE, image->conductor_fields.state);
    image->congestion_control->should_measure_rtt = always_measure_rtt;

    auto *test_bindings_state = static_cast<aeron_test_udp_bindings_state_t *>(dest_1->transport.bindings_clientd);

    aeron_publication_image_schedule_status_message(image, 0, TERM_BUFFER_SIZE);
    aeron_publication_image_send_pending_status_message(image, t0_ns);
    ASSERT_EQ(1, test_bindings_state->sm_count);

    ASSERT_EQ(0, image->log_meta_data->active_transport_count);

    message->stream_id = stream_id;
    message->session_id = session_id;
    message->frame_header.frame_length = 64;
    message->term_id = 0;
    message->term_offset = 0;

    aeron_publication_image_insert_packet(image, dest_2, 0, 0, data, 64, &addr, nullptr);
    aeron_publication_image_schedule_status_message(image, 0, TERM_BUFFER_SIZE);
    aeron_publication_image_send_pending_status_message(image, t0_ns);

    ASSERT_EQ(1, image->log_meta_data->active_transport_count);

    aeron_publication_image_insert_packet(image, dest_1, 0, 0, data, 64, &addr, nullptr);
    aeron_publication_image_schedule_status_message(image, 0, TERM_BUFFER_SIZE);
    aeron_publication_image_send_pending_status_message(image, t0_ns);

    ASSERT_EQ(2, image->log_meta_data->active_transport_count);
}

TEST_F(PublicationImageTest, shouldTrackUnderRunningTransportsWithLastSmAndReceiverWindowLength)
{
    struct sockaddr_storage addr = {}; // Don't really care what value this is.
    uint8_t data[128];
    auto *message = reinterpret_cast<aeron_data_header_t *>(data);
    const char *uri_1 = "aeron:udp?endpoint=localhost:9090";
    const char *uri_2 = "aeron:udp?endpoint=localhost:9091";
    aeron_receive_channel_endpoint_t *endpoint = createMdsEndpoint();
    int32_t stream_id = 1001;
    int32_t session_id = 1000001;
    int64_t registration_id = 0;
    size_t message_length = 64;

    int64_t t0_ns = 10 * AERON_RECEIVE_DESTINATION_TIMEOUT_NS;
    int64_t t1_ns = t0_ns + AERON_RECEIVE_DESTINATION_TIMEOUT_NS;

    aeron_udp_channel_t *channel_1;
    aeron_receive_destination_t *dest_1;
    aeron_udp_channel_t *channel_2;
    aeron_receive_destination_t *dest_2;

    aeron_udp_channel_parse(strlen(uri_1), uri_1, &m_resolver, &channel_1, false);
    aeron_udp_channel_parse(strlen(uri_2), uri_2, &m_resolver, &channel_2, false);

    aeron_clock_update_cached_nano_time(m_context->receiver_cached_clock, t0_ns);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_1,
        channel_1,
        channel_1,
        m_context,
        &m_counters_manager,
        registration_id,
        endpoint->channel_status.counter_id));
    ASSERT_EQ(1, aeron_receive_channel_endpoint_add_destination(endpoint, dest_1));

    aeron_publication_image_t *image = createImage(endpoint, dest_1, stream_id, session_id);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_2,
        channel_2,
        channel_2,
        m_context,
        &m_counters_manager,
        registration_id,
        endpoint->channel_status.counter_id));
    ASSERT_EQ(2, aeron_receive_channel_endpoint_add_destination(endpoint, dest_2));

    ASSERT_EQ(2, aeron_publication_image_add_destination(image, dest_2));

    ASSERT_EQ(AERON_PUBLICATION_IMAGE_STATE_ACTIVE, image->conductor_fields.state);
    image->congestion_control->should_measure_rtt = always_measure_rtt;

    auto *bindings_state_dest1 = static_cast<aeron_test_udp_bindings_state_t *>(dest_1->transport.bindings_clientd);
    auto *bindings_state_dest2 = static_cast<aeron_test_udp_bindings_state_t *>(dest_2->transport.bindings_clientd);

    aeron_publication_image_schedule_status_message(image, 0, TERM_BUFFER_SIZE);
    aeron_publication_image_send_pending_status_message(image, t0_ns);
    ASSERT_EQ(1, bindings_state_dest1->sm_count);

    aeron_clock_update_cached_nano_time(m_context->receiver_cached_clock, t1_ns);

    message->stream_id = stream_id;
    message->session_id = session_id;
    message->frame_header.frame_length = (int32_t) message_length;
    message->term_id = 0;
    message->term_offset = 0;

    aeron_publication_image_insert_packet(image, dest_2, 0, 0, data, message_length, &addr, nullptr);

    aeron_publication_image_schedule_status_message(image, message_length, TERM_BUFFER_SIZE);
    aeron_publication_image_send_pending_status_message(image, t1_ns);

    ASSERT_EQ(1, bindings_state_dest1->sm_count);

    aeron_publication_image_insert_packet(image, dest_1, 0, 0, data, message_length, &addr, nullptr);

    aeron_publication_image_schedule_status_message(image, message_length, TERM_BUFFER_SIZE);
    aeron_publication_image_send_pending_status_message(image, t1_ns);

    ASSERT_EQ(2, bindings_state_dest1->sm_count);
    ASSERT_EQ(2, bindings_state_dest2->sm_count);
}

TEST_F(PublicationImageTest, shouldReportUniqueLoss)
{
    const char *uri = "aeron:udp?endpoint=localhost:9090";
    aeron_receive_channel_endpoint_t *endpoint = createEndpoint(uri);
    int32_t stream_id = 777;
    int32_t session_id = 42;
    int64_t registration_id = 0;

    aeron_udp_channel_t *channel;
    aeron_receive_destination_t *dest;

    ASSERT_EQ(0, aeron_udp_channel_parse(strlen(uri), uri, &m_resolver, &channel, false));

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest,
        channel,
        channel,
        m_context,
        &m_counters_manager,
        registration_id,
        endpoint->channel_status.counter_id));

    aeron_publication_image_t *image = createImage(endpoint, dest, stream_id, session_id);
    ASSERT_NE(nullptr, image) << aeron_errmsg();

    const int32_t term_id = 111;
    const int32_t offset = 128;
    const size_t length = 192;

    // initial loss report
    aeron_publication_image_on_gap_detected(image, term_id, offset, length);
    EXPECT_EQ(1, image->begin_loss_change);
    EXPECT_EQ(term_id, image->loss_term_id);
    EXPECT_EQ(offset, image->loss_term_offset);
    EXPECT_EQ(length, image->loss_length);
    EXPECT_EQ(1, image->end_loss_change);
    EXPECT_EQ(1, aeron_loss_reporter_read(
        m_loss_reporter_buffer.data(),
        m_loss_reporter_buffer.size(),
        [](
            void *clientd,
            int64_t observation_count,
            int64_t total_bytes_lost,
            int64_t first_observation_timestamp,
            int64_t last_observation_timestamp,
            int32_t session_id,
            int32_t stream_id,
            const char *channel,
            int32_t channel_length,
            const char *source,
            int32_t source_length)
        {
            EXPECT_EQ(1, observation_count);
            EXPECT_EQ(192, total_bytes_lost);
            EXPECT_EQ(first_observation_timestamp, last_observation_timestamp);
            EXPECT_EQ(42, session_id);
            EXPECT_EQ(777, stream_id);
        },
        nullptr));

    // same loss => no reporting
    aeron_publication_image_on_gap_detected(image, term_id, offset, length);
    EXPECT_EQ(2, image->begin_loss_change);
    EXPECT_EQ(term_id, image->loss_term_id);
    EXPECT_EQ(offset, image->loss_term_offset);
    EXPECT_EQ(length, image->loss_length);
    EXPECT_EQ(2, image->end_loss_change);
    EXPECT_EQ(1, aeron_loss_reporter_read(
        m_loss_reporter_buffer.data(),
        m_loss_reporter_buffer.size(),
        [](
            void *clientd,
            int64_t observation_count,
            int64_t total_bytes_lost,
            int64_t first_observation_timestamp,
            int64_t last_observation_timestamp,
            int32_t session_id,
            int32_t stream_id,
            const char *channel,
            int32_t channel_length,
            const char *source,
            int32_t source_length)
        {
            EXPECT_EQ(1, observation_count);
            EXPECT_EQ(192, total_bytes_lost);
            EXPECT_EQ(first_observation_timestamp, last_observation_timestamp);
            EXPECT_EQ(42, session_id);
            EXPECT_EQ(777, stream_id);
        },
        nullptr));

    // less loss => no reporting
    aeron_publication_image_on_gap_detected(image, term_id, offset, 32);
    EXPECT_EQ(3, image->begin_loss_change);
    EXPECT_EQ(term_id, image->loss_term_id);
    EXPECT_EQ(offset, image->loss_term_offset);
    EXPECT_EQ(32, image->loss_length);
    EXPECT_EQ(3, image->end_loss_change);
    EXPECT_EQ(1, aeron_loss_reporter_read(
        m_loss_reporter_buffer.data(),
        m_loss_reporter_buffer.size(),
        [](
            void *clientd,
            int64_t observation_count,
            int64_t total_bytes_lost,
            int64_t first_observation_timestamp,
            int64_t last_observation_timestamp,
            int32_t session_id,
            int32_t stream_id,
            const char *channel,
            int32_t channel_length,
            const char *source,
            int32_t source_length)
        {
            EXPECT_EQ(1, observation_count);
            EXPECT_EQ(192, total_bytes_lost);
            EXPECT_EQ(first_observation_timestamp, last_observation_timestamp);
            EXPECT_EQ(42, session_id);
            EXPECT_EQ(777, stream_id);
        },
        nullptr));

    // larger loss => report
    aeron_publication_image_on_gap_detected(image, term_id, offset, 1500);
    EXPECT_EQ(4, image->begin_loss_change);
    EXPECT_EQ(term_id, image->loss_term_id);
    EXPECT_EQ(offset, image->loss_term_offset);
    EXPECT_EQ(1500, image->loss_length);
    EXPECT_EQ(4, image->end_loss_change);
    EXPECT_EQ(1, aeron_loss_reporter_read(
        m_loss_reporter_buffer.data(),
        m_loss_reporter_buffer.size(),
        [](
            void *clientd,
            int64_t observation_count,
            int64_t total_bytes_lost,
            int64_t first_observation_timestamp,
            int64_t last_observation_timestamp,
            int32_t session_id,
            int32_t stream_id,
            const char *channel,
            int32_t channel_length,
            const char *source,
            int32_t source_length)
        {
            EXPECT_EQ(2, observation_count);
            EXPECT_EQ(1500, total_bytes_lost);
            EXPECT_LE(first_observation_timestamp, last_observation_timestamp);
            EXPECT_EQ(42, session_id);
            EXPECT_EQ(777, stream_id);
        },
        nullptr));

    // overlapping loss => report
    aeron_publication_image_on_gap_detected(image, term_id, offset + 996, 700);
    EXPECT_EQ(5, image->begin_loss_change);
    EXPECT_EQ(term_id, image->loss_term_id);
    EXPECT_EQ(offset + 996, image->loss_term_offset);
    EXPECT_EQ(700, image->loss_length);
    EXPECT_EQ(5, image->end_loss_change);
    EXPECT_EQ(1, aeron_loss_reporter_read(
        m_loss_reporter_buffer.data(),
        m_loss_reporter_buffer.size(),
        [](
            void *clientd,
            int64_t observation_count,
            int64_t total_bytes_lost,
            int64_t first_observation_timestamp,
            int64_t last_observation_timestamp,
            int32_t session_id,
            int32_t stream_id,
            const char *channel,
            int32_t channel_length,
            const char *source,
            int32_t source_length)
        {
            EXPECT_EQ(3, observation_count);
            EXPECT_EQ(1696, total_bytes_lost);
            EXPECT_LE(first_observation_timestamp, last_observation_timestamp);
            EXPECT_EQ(42, session_id);
            EXPECT_EQ(777, stream_id);
        },
        nullptr));

    // non-overlapping loss => report
    aeron_publication_image_on_gap_detected(image, term_id, offset + 4096, 128);
    EXPECT_EQ(6, image->begin_loss_change);
    EXPECT_EQ(term_id, image->loss_term_id);
    EXPECT_EQ(offset + 4096, image->loss_term_offset);
    EXPECT_EQ(128, image->loss_length);
    EXPECT_EQ(6, image->end_loss_change);
    EXPECT_EQ(1, aeron_loss_reporter_read(
        m_loss_reporter_buffer.data(),
        m_loss_reporter_buffer.size(),
        [](
            void *clientd,
            int64_t observation_count,
            int64_t total_bytes_lost,
            int64_t first_observation_timestamp,
            int64_t last_observation_timestamp,
            int32_t session_id,
            int32_t stream_id,
            const char *channel,
            int32_t channel_length,
            const char *source,
            int32_t source_length)
        {
            EXPECT_EQ(4, observation_count);
            EXPECT_EQ(1824, total_bytes_lost);
            EXPECT_LE(first_observation_timestamp, last_observation_timestamp);
            EXPECT_EQ(42, session_id);
            EXPECT_EQ(777, stream_id);
        },
        nullptr));

    // loss in another term => report
    aeron_publication_image_on_gap_detected(image, term_id + 3, 0, 400);
    EXPECT_EQ(7, image->begin_loss_change);
    EXPECT_EQ(term_id + 3, image->loss_term_id);
    EXPECT_EQ(0, image->loss_term_offset);
    EXPECT_EQ(400, image->loss_length);
    EXPECT_EQ(7, image->end_loss_change);
    EXPECT_EQ(1, aeron_loss_reporter_read(
        m_loss_reporter_buffer.data(),
        m_loss_reporter_buffer.size(),
        [](
            void *clientd,
            int64_t observation_count,
            int64_t total_bytes_lost,
            int64_t first_observation_timestamp,
            int64_t last_observation_timestamp,
            int32_t session_id,
            int32_t stream_id,
            const char *channel,
            int32_t channel_length,
            const char *source,
            int32_t source_length)
        {
            EXPECT_EQ(5, observation_count);
            EXPECT_EQ(2224, total_bytes_lost);
            EXPECT_LE(first_observation_timestamp, last_observation_timestamp);
            EXPECT_EQ(42, session_id);
            EXPECT_EQ(777, stream_id);
        },
        nullptr));

    // same loss => no report
    aeron_publication_image_on_gap_detected(image, term_id + 3, 0, 400);
    EXPECT_EQ(8, image->begin_loss_change);
    EXPECT_EQ(term_id + 3, image->loss_term_id);
    EXPECT_EQ(0, image->loss_term_offset);
    EXPECT_EQ(400, image->loss_length);
    EXPECT_EQ(8, image->end_loss_change);
    EXPECT_EQ(1, aeron_loss_reporter_read(
        m_loss_reporter_buffer.data(),
        m_loss_reporter_buffer.size(),
        [](
            void *clientd,
            int64_t observation_count,
            int64_t total_bytes_lost,
            int64_t first_observation_timestamp,
            int64_t last_observation_timestamp,
            int32_t session_id,
            int32_t stream_id,
            const char *channel,
            int32_t channel_length,
            const char *source,
            int32_t source_length)
        {
            EXPECT_EQ(5, observation_count);
            EXPECT_EQ(2224, total_bytes_lost);
            EXPECT_LE(first_observation_timestamp, last_observation_timestamp);
            EXPECT_EQ(42, session_id);
            EXPECT_EQ(777, stream_id);
        },
        nullptr));

    aeron_publication_image_remove_destination(image, channel);
    endpoint->transport_bindings->poller_remove_func(&m_receiver.poller, &dest->transport);
    endpoint->transport_bindings->close_func(&dest->transport);
    aeron_receive_destination_delete(dest, &m_counters_manager);
}

class TermOffsetValidationTest : public ReceiverTestBase, public testing::TestWithParam<int32_t>
{
public:
    void SetUp() final
    {
        DoSetUp();
    }

    void TearDown() final
    {
        DoTearDown();
    }
};

INSTANTIATE_TEST_SUITE_P(
    TermOffsetValidationTests,
    TermOffsetValidationTest,
    testing::Values(-100, -32, 64 * 1024, 2500)
);

TEST_P(TermOffsetValidationTest, shouldRejectPacketIfFirstFrameHasWrongTermOffset)
{
    sockaddr_storage addr;
    uint8_t data[128];
    memset(&data, 0, sizeof(data));
    aeron_publication_image_t image;
    image.initial_term_id = 0;
    int64_t invalid_packets = 0;
    image.invalid_packets_counter = &invalid_packets;
    image.invalidation_reason = nullptr;
    image.term_length_mask = (64 * 1024) - 1;
    aeron_receive_destination_t destination;

    int32_t term_id = 1;
    int32_t term_offset = GetParam();

    auto *frame = reinterpret_cast<aeron_data_header_t *>(&data);
    frame->frame_header.frame_length = 64;
    frame->frame_header.type = AERON_HDR_TYPE_DATA;
    frame->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame->frame_header.flags = 0;
    frame->term_offset = term_offset;

    EXPECT_EQ(0, aeron_publication_image_insert_packet(
        &image, &destination, term_id, term_offset, data, 64, &addr, nullptr));
    EXPECT_EQ(1, invalid_packets);
}

TEST_F(PublicationImageTest, shouldRejectPacketIfItContainsTrailingBytes)
{
    sockaddr_storage addr;
    uint8_t data[128];
    memset(&data, 0, sizeof(data));
    aeron_publication_image_t image;
    image.initial_term_id = 0;
    int64_t invalid_packets = 0;
    image.invalid_packets_counter = &invalid_packets;
    image.invalidation_reason = nullptr;
    image.term_length_mask = (64 * 1024) - 1;
    aeron_receive_destination_t destination;

    int32_t term_id = 1;
    int32_t term_offset = 0;

    auto *frame = reinterpret_cast<aeron_data_header_t *>(&data);
    frame->frame_header.frame_length = 64;
    frame->frame_header.type = AERON_HDR_TYPE_DATA;
    frame->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame->frame_header.flags = 0;
    frame->term_offset = term_offset;

    EXPECT_EQ(0, aeron_publication_image_insert_packet(
        &image, &destination, term_id, term_offset, data, 128, &addr, nullptr));
    EXPECT_EQ(1, invalid_packets);
}

class FrameTypeValidationTest : public ReceiverTestBase, public testing::TestWithParam<int16_t>
{
public:
    void SetUp() final
    {
        DoSetUp();
    }

    void TearDown() final
    {
        DoTearDown();
    }
};

INSTANTIATE_TEST_SUITE_P(
    FrameTypeValidationTests,
    FrameTypeValidationTest,
    testing::Values(
        -500,
        AERON_HDR_TYPE_NAK,
        AERON_HDR_TYPE_SM,
        AERON_HDR_TYPE_ERR,
        AERON_HDR_TYPE_SETUP,
        AERON_HDR_TYPE_RTTM,
        AERON_HDR_TYPE_RES,
        AERON_HDR_TYPE_ATS_DATA,
        AERON_HDR_TYPE_ATS_SETUP,
        AERON_HDR_TYPE_ATS_SM,
        AERON_HDR_TYPE_RSP_SETUP,
        AERON_HDR_TYPE_EXT,
        INT16_MAX,
        INT16_MIN)
);

TEST_P(FrameTypeValidationTest, shouldRejectPacketWithFramesHavingWrongFrameType)
{
    sockaddr_storage addr;
    uint8_t data[128];
    memset(&data, 0, sizeof(data));
    aeron_publication_image_t image;
    image.initial_term_id = 0;
    int64_t invalid_packets = 0;
    image.invalid_packets_counter = &invalid_packets;
    image.invalidation_reason = nullptr;
    image.term_length_mask = (64 * 1024) - 1;
    aeron_receive_destination_t destination;

    int32_t term_id = 1;
    int32_t term_offset = 128;

    auto *frame1 = reinterpret_cast<aeron_data_header_t *>(&data[0]);
    frame1->frame_header.frame_length = 64;
    frame1->frame_header.type = AERON_HDR_TYPE_DATA;
    frame1->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame1->frame_header.flags = 0;
    frame1->term_offset = term_offset;

    auto *frame2 = reinterpret_cast<aeron_data_header_t *>(&data[frame1->frame_header.frame_length]);
    frame2->frame_header.frame_length = 64;
    frame2->frame_header.type = GetParam();
    frame2->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame2->frame_header.flags = 0;
    frame2->term_offset = term_offset + frame1->frame_header.frame_length;

    EXPECT_EQ(0, aeron_publication_image_insert_packet(
        &image, &destination, term_id, term_offset, data, 128, &addr, nullptr));
    EXPECT_EQ(1, invalid_packets);
}

TEST_F(PublicationImageTest, shouldRejectPacketIfFrameOffsetIsIncorrect)
{
    sockaddr_storage addr;
    uint8_t data[128];
    memset(&data, 0, sizeof(data));
    aeron_publication_image_t image;
    image.initial_term_id = 0;
    int64_t invalid_packets = 0;
    image.invalid_packets_counter = &invalid_packets;
    image.invalidation_reason = nullptr;
    image.term_length_mask = (64 * 1024) - 1;
    aeron_receive_destination_t destination;

    int32_t term_id = 1;
    int32_t term_offset = 128;

    auto *frame1 = reinterpret_cast<aeron_data_header_t *>(&data[0]);
    frame1->frame_header.frame_length = 64;
    frame1->frame_header.type = AERON_HDR_TYPE_DATA;
    frame1->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame1->frame_header.flags = 0;
    frame1->term_offset = term_offset;

    auto *frame2 = reinterpret_cast<aeron_data_header_t *>(&data[frame1->frame_header.frame_length]);
    frame2->frame_header.frame_length = 64;
    frame2->frame_header.type = AERON_HDR_TYPE_DATA;
    frame2->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame2->frame_header.flags = 0;
    frame2->term_offset = term_offset + 19;

    EXPECT_EQ(0, aeron_publication_image_insert_packet(
        &image, &destination, term_id, term_offset, data, 128, &addr, nullptr));
    EXPECT_EQ(1, invalid_packets);
}

class FrameLengthValidationTest : public ReceiverTestBase, public testing::TestWithParam<int32_t>
{
public:
    void SetUp() final
    {
        DoSetUp();
    }

    void TearDown() final
    {
        DoTearDown();
    }
};

INSTANTIATE_TEST_SUITE_P(
    FrameLengthValidationTests,
    FrameLengthValidationTest,
    testing::Values(-32, 0, 1, 2, 3, 4, 8, 16, 20, 24, 29, 31));

TEST_P(FrameLengthValidationTest, shouldRejectPacketIfFrameLengthIsLessThan32Bytes)
{
    sockaddr_storage addr;
    uint8_t data[128];
    memset(&data, 0, sizeof(data));
    aeron_publication_image_t image;
    image.initial_term_id = 0;
    int64_t invalid_packets = 0;
    image.invalid_packets_counter = &invalid_packets;
    image.invalidation_reason = nullptr;
    image.term_length_mask = (64 * 1024) - 1;
    aeron_receive_destination_t destination;

    int32_t term_id = 1;
    int32_t term_offset = 128;

    auto *frame1 = reinterpret_cast<aeron_data_header_t *>(&data[0]);
    frame1->frame_header.frame_length = 64;
    frame1->frame_header.type = AERON_HDR_TYPE_DATA;
    frame1->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame1->frame_header.flags = 0;
    frame1->term_offset = term_offset;

    auto *frame2 = reinterpret_cast<aeron_data_header_t *>(&data[frame1->frame_header.frame_length]);
    frame2->frame_header.frame_length = GetParam();
    frame2->frame_header.type = AERON_HDR_TYPE_DATA;
    frame2->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame2->frame_header.flags = 0;
    frame2->term_offset = term_offset + frame1->frame_header.frame_length;

    EXPECT_EQ(0, aeron_publication_image_insert_packet(
        &image, &destination, term_id, term_offset, data, 96, &addr, nullptr));
    EXPECT_EQ(1, invalid_packets);
}

TEST_F(PublicationImageTest, shouldRejectPacketIfFrameDoesNotFitIntoThePacket)
{
    sockaddr_storage addr;
    uint8_t data[128];
    memset(&data, 0, sizeof(data));
    aeron_publication_image_t image;
    image.initial_term_id = 0;
    int64_t invalid_packets = 0;
    image.invalid_packets_counter = &invalid_packets;
    image.invalidation_reason = nullptr;
    image.term_length_mask = (64 * 1024) - 1;
    aeron_receive_destination_t destination;

    int32_t term_id = 1;
    int32_t term_offset = 128;

    auto *frame1 = reinterpret_cast<aeron_data_header_t *>(&data[0]);
    frame1->frame_header.frame_length = 64;
    frame1->frame_header.type = AERON_HDR_TYPE_DATA;
    frame1->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame1->frame_header.flags = 0;
    frame1->term_offset = term_offset;

    auto *frame2 = reinterpret_cast<aeron_data_header_t *>(&data[frame1->frame_header.frame_length]);
    frame2->frame_header.frame_length = 2048;
    frame2->frame_header.type = AERON_HDR_TYPE_DATA;
    frame2->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame2->frame_header.flags = 0;
    frame2->term_offset = term_offset + frame1->frame_header.frame_length;

    EXPECT_EQ(0, aeron_publication_image_insert_packet(
        &image, &destination, term_id, term_offset, data, 128, &addr, nullptr));
    EXPECT_EQ(1, invalid_packets);
}

TEST_F(PublicationImageTest, shouldRejectPacketIfFrameLengthPlussOffsetExceedsTermLengthBoundary)
{
    sockaddr_storage addr;
    uint8_t data[128];
    memset(&data, 0, sizeof(data));
    aeron_publication_image_t image;
    image.initial_term_id = 0;
    int64_t invalid_packets = 0;
    image.invalid_packets_counter = &invalid_packets;
    image.invalidation_reason = nullptr;
    image.term_length_mask = (64 * 1024) - 1;
    aeron_receive_destination_t destination;

    int32_t term_id = 1;
    int32_t term_offset = image.term_length_mask + 1 - 96;

    auto *frame1 = reinterpret_cast<aeron_data_header_t *>(&data[0]);
    frame1->frame_header.frame_length = 64;
    frame1->frame_header.type = AERON_HDR_TYPE_DATA;
    frame1->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame1->frame_header.flags = 0;
    frame1->term_offset = term_offset;

    auto *frame2 = reinterpret_cast<aeron_data_header_t *>(&data[frame1->frame_header.frame_length]);
    frame2->frame_header.frame_length = 64;
    frame2->frame_header.type = AERON_HDR_TYPE_DATA;
    frame2->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame2->frame_header.flags = 0;
    frame2->term_offset = term_offset + frame1->frame_header.frame_length;

    EXPECT_EQ(0, aeron_publication_image_insert_packet(
        &image, &destination, term_id, term_offset, data, 128, &addr, nullptr));
    EXPECT_EQ(1, invalid_packets);
}

TEST_F(PublicationImageTest, shouldAllowTrailingPaddingFrameToExceedPacketLength)
{
    sockaddr_storage addr;
    uint8_t data[128];
    memset(&data, 0, sizeof(data));
    int64_t invalid_packets = 0;
    int64_t flow_control_over_runs = 0;
    aeron_publication_image_t image;
    image.initial_term_id = 0;
    image.invalid_packets_counter = &invalid_packets;
    image.flow_control_over_runs_counter= &flow_control_over_runs;
    image.invalidation_reason = nullptr;
    int32_t term_length = 64 * 1024;
    image.term_length_mask = term_length - 1;
    image.position_bits_to_shift = aeron_number_of_trailing_zeroes(term_length);
    image.last_sm_position = 0;
    image.last_overrun_threshold = 0;
    aeron_clock_cache_t clock;
    clock.cached_nano_time = 1;
    image.cached_clock = &clock;
    aeron_receive_destination_t destination;

    int32_t term_id = 1;
    int32_t term_offset = 0;

    auto *frame1 = reinterpret_cast<aeron_data_header_t *>(&data[0]);
    frame1->frame_header.frame_length = 64;
    frame1->frame_header.type = AERON_HDR_TYPE_DATA;
    frame1->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame1->frame_header.flags = 0;
    frame1->term_offset = term_offset;

    auto *frame2 = reinterpret_cast<aeron_data_header_t *>(&data[frame1->frame_header.frame_length]);
    frame2->frame_header.frame_length = 4096;
    frame2->frame_header.type = AERON_HDR_TYPE_PAD;
    frame2->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame2->frame_header.flags = 0;
    frame2->term_offset = term_offset + frame1->frame_header.frame_length;

    EXPECT_EQ(128, aeron_publication_image_insert_packet(
        &image, &destination, term_id, term_offset, data, 128, &addr, nullptr));
    EXPECT_EQ(0, invalid_packets);
    EXPECT_EQ(1, flow_control_over_runs);
}

TEST_F(PublicationImageTest, shouldAllowHeartBeats)
{
    sockaddr_storage addr;
    uint8_t data[128];
    memset(&data, 0, sizeof(data));
    int64_t invalid_packets = 0;
    int64_t flow_control_over_runs = 0;
    int64_t heartbeats_received = 0;
    int64_t rcv_hwm_position = 0;
    aeron_publication_image_t image;
    image.initial_term_id = 0;
    image.invalid_packets_counter = &invalid_packets;
    image.flow_control_over_runs_counter= &flow_control_over_runs;
    image.heartbeats_received_counter= &heartbeats_received;
    image.invalidation_reason = nullptr;
    int32_t term_length = 64 * 1024;
    image.term_length_mask = term_length - 1;
    image.position_bits_to_shift = aeron_number_of_trailing_zeroes(term_length);
    image.last_sm_position = 0;
    image.last_overrun_threshold = 10 * term_length;
    aeron_clock_cache_t clock;
    clock.cached_nano_time = 123;
    image.cached_clock = &clock;
    image.connections.length = 0;
    image.connections.capacity = 0;
    image.connections.array = nullptr;
    image.is_end_of_stream = false;
    image.rcv_hwm_position.value_addr = &rcv_hwm_position;
    aeron_receive_destination_t destination;
    destination.has_control_addr = false;

    int32_t term_id = 5;
    int32_t term_offset = 8192;

    auto *frame1 = reinterpret_cast<aeron_data_header_t *>(&data[0]);
    frame1->frame_header.frame_length = 0;
    frame1->frame_header.type = AERON_HDR_TYPE_DATA;
    frame1->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame1->frame_header.flags = 0;
    frame1->term_offset = term_offset;

    EXPECT_EQ(AERON_DATA_HEADER_LENGTH, aeron_publication_image_insert_packet(
        &image, &destination, term_id, term_offset, data, AERON_DATA_HEADER_LENGTH, &addr, nullptr));
    EXPECT_EQ(0, invalid_packets);
    EXPECT_EQ(0, flow_control_over_runs);
    EXPECT_EQ(1, heartbeats_received);
    EXPECT_EQ(clock.cached_nano_time, image.time_of_last_packet_ns);
    EXPECT_EQ(
        aeron_logbuffer_compute_position(term_id, term_offset, image.position_bits_to_shift, image.initial_term_id),
        rcv_hwm_position);

    aeron_free(image.connections.array);
}

TEST_F(PublicationImageTest, shouldAssignPacketTimestamps)
{
    sockaddr_storage addr;
    timespec media_receive_timestamp = {};
    media_receive_timestamp.tv_sec = 23648234;
    media_receive_timestamp.tv_nsec = 987;
    int64_t media_timestamp =
        (int64_t)1000000000 * (int64_t)media_receive_timestamp.tv_sec + media_receive_timestamp.tv_nsec;
    uint8_t data[288];
    memset(&data, 0, sizeof(data));
    int64_t invalid_packets = 0;
    int64_t flow_control_over_runs = 0;
    aeron_publication_image_t image;
    image.initial_term_id = 0;
    image.invalid_packets_counter = &invalid_packets;
    image.flow_control_over_runs_counter= &flow_control_over_runs;
    image.invalidation_reason = nullptr;
    int32_t term_length = 64 * 1024;
    image.term_length_mask = term_length - 1;
    image.position_bits_to_shift = aeron_number_of_trailing_zeroes(term_length);
    image.last_sm_position = 0;
    image.last_overrun_threshold = 0;
    aeron_clock_cache_t clock;
    clock.cached_nano_time = 1;
    image.cached_clock = &clock;
    aeron_receive_destination_t destination;
    destination.transport.timestamp_flags = AERON_UDP_CHANNEL_TRANSPORT_MEDIA_RCV_TIMESTAMP | AERON_UDP_CHANNEL_TRANSPORT_CHANNEL_RCV_TIMESTAMP;
    aeron_receive_channel_endpoint_t endpoint;
    aeron_udp_channel_t udp_channel;
    udp_channel.channel_rcv_timestamp_offset = 0;
    udp_channel.media_rcv_timestamp_offset = AERON_UDP_CHANNEL_RESERVED_VALUE_OFFSET;
    endpoint.conductor_fields.udp_channel = &udp_channel;
    image.endpoint = &endpoint;

    int32_t term_id = 1;
    int32_t term_offset = 1024;

    auto *frame1 = reinterpret_cast<aeron_data_header_t *>(&data[0]);
    frame1->frame_header.frame_length = 64;
    frame1->frame_header.type = AERON_HDR_TYPE_DATA;
    frame1->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame1->frame_header.flags = AERON_DATA_HEADER_UNFRAGMENTED;
    frame1->term_offset = term_offset;

    auto *frame2 = reinterpret_cast<aeron_data_header_t *>(&data[64]);
    frame2->frame_header.frame_length = 64;
    frame2->frame_header.type = AERON_HDR_TYPE_DATA;
    frame2->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame2->frame_header.flags = AERON_DATA_HEADER_BEGIN_FLAG;
    frame2->term_offset = term_offset + 64;

    auto *frame3 = reinterpret_cast<aeron_data_header_t *>(&data[128]);
    frame3->frame_header.frame_length = 64;
    frame3->frame_header.type = AERON_HDR_TYPE_DATA;
    frame3->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame3->frame_header.flags = 0;
    frame3->term_offset = term_offset + 128;

    auto *frame4 = reinterpret_cast<aeron_data_header_t *>(&data[192]);
    frame4->frame_header.frame_length = 64;
    frame4->frame_header.type = AERON_HDR_TYPE_DATA;
    frame4->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame4->frame_header.flags = AERON_DATA_HEADER_END_FLAG;
    frame4->term_offset = term_offset + 192;

    auto *frame5 = reinterpret_cast<aeron_data_header_t *>(&data[256]);
    frame5->frame_header.frame_length = 1024;
    frame5->frame_header.type = AERON_HDR_TYPE_PAD;
    frame5->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame5->frame_header.flags = AERON_DATA_HEADER_UNFRAGMENTED;
    frame5->term_offset = term_offset + 256;

    EXPECT_EQ(288, aeron_publication_image_insert_packet(
        &image, &destination, term_id, term_offset, data, 288, &addr, &media_receive_timestamp));
    EXPECT_EQ(0, invalid_packets);
    EXPECT_EQ(1, flow_control_over_runs);
    EXPECT_EQ(media_timestamp, frame1->reserved_value);
    auto rcv_timestamp = reinterpret_cast<int64_t*>(&data[32]);
    EXPECT_NE(0, *rcv_timestamp);
    EXPECT_EQ(media_timestamp, frame2->reserved_value);
    EXPECT_EQ(*rcv_timestamp, *reinterpret_cast<int64_t*>(&data[96]));
    EXPECT_EQ(0, frame3->reserved_value);
    EXPECT_EQ(0, *reinterpret_cast<int64_t*>(&data[160]));
    EXPECT_EQ(0, frame4->reserved_value);
    EXPECT_EQ(0, *reinterpret_cast<int64_t*>(&data[224]));
    EXPECT_EQ(0, frame5->reserved_value);
}

TEST_F(PublicationImageTest, shouldNotAssignRcvTimestampIfNotEnabled)
{
    sockaddr_storage addr;
    timespec media_receive_timestamp = {};
    media_receive_timestamp.tv_sec = 23648234;
    media_receive_timestamp.tv_nsec = 987;
    int64_t media_timestamp =
        (int64_t)1000000000 * (int64_t)media_receive_timestamp.tv_sec + media_receive_timestamp.tv_nsec;
    uint8_t data[288];
    memset(&data, 0, sizeof(data));
    int64_t invalid_packets = 0;
    int64_t flow_control_over_runs = 0;
    aeron_publication_image_t image;
    image.initial_term_id = 0;
    image.invalid_packets_counter = &invalid_packets;
    image.flow_control_over_runs_counter= &flow_control_over_runs;
    image.invalidation_reason = nullptr;
    int32_t term_length = 64 * 1024;
    image.term_length_mask = term_length - 1;
    image.position_bits_to_shift = aeron_number_of_trailing_zeroes(term_length);
    image.last_sm_position = 0;
    image.last_overrun_threshold = 0;
    aeron_clock_cache_t clock;
    clock.cached_nano_time = 1;
    image.cached_clock = &clock;
    aeron_receive_destination_t destination;
    destination.transport.timestamp_flags = AERON_UDP_CHANNEL_TRANSPORT_MEDIA_RCV_TIMESTAMP;
    aeron_receive_channel_endpoint_t endpoint;
    aeron_udp_channel_t udp_channel;
    udp_channel.channel_rcv_timestamp_offset = 0;
    udp_channel.media_rcv_timestamp_offset = AERON_UDP_CHANNEL_RESERVED_VALUE_OFFSET;
    endpoint.conductor_fields.udp_channel = &udp_channel;
    image.endpoint = &endpoint;

    int32_t term_id = 1;
    int32_t term_offset = 1024;

    auto *frame1 = reinterpret_cast<aeron_data_header_t *>(&data[0]);
    frame1->frame_header.frame_length = 64;
    frame1->frame_header.type = AERON_HDR_TYPE_DATA;
    frame1->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame1->frame_header.flags = AERON_DATA_HEADER_UNFRAGMENTED;
    frame1->term_offset = term_offset;

    auto *frame2 = reinterpret_cast<aeron_data_header_t *>(&data[64]);
    frame2->frame_header.frame_length = 64;
    frame2->frame_header.type = AERON_HDR_TYPE_DATA;
    frame2->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame2->frame_header.flags = AERON_DATA_HEADER_BEGIN_FLAG;
    frame2->term_offset = term_offset + 64;

    auto *frame3 = reinterpret_cast<aeron_data_header_t *>(&data[128]);
    frame3->frame_header.frame_length = 64;
    frame3->frame_header.type = AERON_HDR_TYPE_DATA;
    frame3->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame3->frame_header.flags = 0;
    frame3->term_offset = term_offset + 128;

    auto *frame4 = reinterpret_cast<aeron_data_header_t *>(&data[192]);
    frame4->frame_header.frame_length = 64;
    frame4->frame_header.type = AERON_HDR_TYPE_DATA;
    frame4->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame4->frame_header.flags = AERON_DATA_HEADER_END_FLAG;
    frame4->term_offset = term_offset + 192;

    auto *frame5 = reinterpret_cast<aeron_data_header_t *>(&data[256]);
    frame5->frame_header.frame_length = 1024;
    frame5->frame_header.type = AERON_HDR_TYPE_PAD;
    frame5->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame5->frame_header.flags = AERON_DATA_HEADER_UNFRAGMENTED;
    frame5->term_offset = term_offset + 256;

    EXPECT_EQ(288, aeron_publication_image_insert_packet(
        &image, &destination, term_id, term_offset, data, 288, &addr, &media_receive_timestamp));
    EXPECT_EQ(0, invalid_packets);
    EXPECT_EQ(1, flow_control_over_runs);
    EXPECT_EQ(media_timestamp, frame1->reserved_value);
    EXPECT_EQ(0, *reinterpret_cast<int64_t*>(&data[32]));
    EXPECT_EQ(media_timestamp, frame2->reserved_value);
    EXPECT_EQ(0, *reinterpret_cast<int64_t*>(&data[96]));
    EXPECT_EQ(0, frame3->reserved_value);
    EXPECT_EQ(0, *reinterpret_cast<int64_t*>(&data[160]));
    EXPECT_EQ(0, frame4->reserved_value);
    EXPECT_EQ(0, *reinterpret_cast<int64_t*>(&data[224]));
    EXPECT_EQ(0, frame5->reserved_value);
}

TEST_F(PublicationImageTest, shouldNotAssignMediaTimestampIfNotEnabled)
{
    sockaddr_storage addr;
    timespec media_receive_timestamp = {};
    media_receive_timestamp.tv_sec = 23648234;
    media_receive_timestamp.tv_nsec = 987;
    uint8_t data[288];
    memset(&data, 0, sizeof(data));
    int64_t invalid_packets = 0;
    int64_t flow_control_over_runs = 0;
    aeron_publication_image_t image;
    image.initial_term_id = 0;
    image.invalid_packets_counter = &invalid_packets;
    image.flow_control_over_runs_counter= &flow_control_over_runs;
    image.invalidation_reason = nullptr;
    int32_t term_length = 64 * 1024;
    image.term_length_mask = term_length - 1;
    image.position_bits_to_shift = aeron_number_of_trailing_zeroes(term_length);
    image.last_sm_position = 0;
    image.last_overrun_threshold = 0;
    aeron_clock_cache_t clock;
    clock.cached_nano_time = 1;
    image.cached_clock = &clock;
    aeron_receive_destination_t destination;
    destination.transport.timestamp_flags = AERON_UDP_CHANNEL_TRANSPORT_CHANNEL_RCV_TIMESTAMP;
    aeron_receive_channel_endpoint_t endpoint;
    aeron_udp_channel_t udp_channel;
    udp_channel.channel_rcv_timestamp_offset = 0;
    udp_channel.media_rcv_timestamp_offset = AERON_UDP_CHANNEL_RESERVED_VALUE_OFFSET;
    endpoint.conductor_fields.udp_channel = &udp_channel;
    image.endpoint = &endpoint;

    int32_t term_id = 1;
    int32_t term_offset = 1024;

    auto *frame1 = reinterpret_cast<aeron_data_header_t *>(&data[0]);
    frame1->frame_header.frame_length = 64;
    frame1->frame_header.type = AERON_HDR_TYPE_DATA;
    frame1->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame1->frame_header.flags = AERON_DATA_HEADER_UNFRAGMENTED;
    frame1->term_offset = term_offset;

    auto *frame2 = reinterpret_cast<aeron_data_header_t *>(&data[64]);
    frame2->frame_header.frame_length = 64;
    frame2->frame_header.type = AERON_HDR_TYPE_DATA;
    frame2->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame2->frame_header.flags = AERON_DATA_HEADER_BEGIN_FLAG;
    frame2->term_offset = term_offset + 64;

    auto *frame3 = reinterpret_cast<aeron_data_header_t *>(&data[128]);
    frame3->frame_header.frame_length = 64;
    frame3->frame_header.type = AERON_HDR_TYPE_DATA;
    frame3->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame3->frame_header.flags = 0;
    frame3->term_offset = term_offset + 128;

    auto *frame4 = reinterpret_cast<aeron_data_header_t *>(&data[192]);
    frame4->frame_header.frame_length = 64;
    frame4->frame_header.type = AERON_HDR_TYPE_DATA;
    frame4->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame4->frame_header.flags = AERON_DATA_HEADER_END_FLAG;
    frame4->term_offset = term_offset + 192;

    auto *frame5 = reinterpret_cast<aeron_data_header_t *>(&data[256]);
    frame5->frame_header.frame_length = 1024;
    frame5->frame_header.type = AERON_HDR_TYPE_PAD;
    frame5->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame5->frame_header.flags = AERON_DATA_HEADER_UNFRAGMENTED;
    frame5->term_offset = term_offset + 256;

    EXPECT_EQ(288, aeron_publication_image_insert_packet(
        &image, &destination, term_id, term_offset, data, 288, &addr, &media_receive_timestamp));
    EXPECT_EQ(0, invalid_packets);
    EXPECT_EQ(1, flow_control_over_runs);
    EXPECT_EQ(0, frame1->reserved_value);
    auto rcv_timestamp = reinterpret_cast<int64_t*>(&data[32]);
    EXPECT_NE(0, *rcv_timestamp);
    EXPECT_EQ(0, frame2->reserved_value);
    EXPECT_EQ(*rcv_timestamp, *reinterpret_cast<int64_t*>(&data[96]));
    EXPECT_EQ(0, frame3->reserved_value);
    EXPECT_EQ(0, *reinterpret_cast<int64_t*>(&data[160]));
    EXPECT_EQ(0, frame4->reserved_value);
    EXPECT_EQ(0, *reinterpret_cast<int64_t*>(&data[224]));
    EXPECT_EQ(0, frame5->reserved_value);
}

TEST_F(PublicationImageTest, shouldNotSetTimestampsIfEndpointIsNull)
{
    sockaddr_storage addr;
    timespec media_receive_timestamp = {};
    media_receive_timestamp.tv_sec = 23648234;
    media_receive_timestamp.tv_nsec = 987;
    uint8_t data[288];
    memset(&data, 0, sizeof(data));
    int64_t invalid_packets = 0;
    int64_t flow_control_over_runs = 0;
    aeron_publication_image_t image;
    image.initial_term_id = 0;
    image.invalid_packets_counter = &invalid_packets;
    image.flow_control_over_runs_counter= &flow_control_over_runs;
    image.invalidation_reason = nullptr;
    int32_t term_length = 64 * 1024;
    image.term_length_mask = term_length - 1;
    image.position_bits_to_shift = aeron_number_of_trailing_zeroes(term_length);
    image.last_sm_position = 0;
    image.last_overrun_threshold = 0;
    aeron_clock_cache_t clock;
    clock.cached_nano_time = 1;
    image.cached_clock = &clock;
    aeron_receive_destination_t destination;
    destination.transport.timestamp_flags = AERON_UDP_CHANNEL_TRANSPORT_MEDIA_RCV_TIMESTAMP | AERON_UDP_CHANNEL_TRANSPORT_CHANNEL_RCV_TIMESTAMP;
    image.endpoint = nullptr;

    int32_t term_id = 1;
    int32_t term_offset = 1024;

    auto *frame1 = reinterpret_cast<aeron_data_header_t *>(&data[0]);
    frame1->frame_header.frame_length = 64;
    frame1->frame_header.type = AERON_HDR_TYPE_DATA;
    frame1->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame1->frame_header.flags = AERON_DATA_HEADER_UNFRAGMENTED;
    frame1->term_offset = term_offset;

    auto *frame2 = reinterpret_cast<aeron_data_header_t *>(&data[64]);
    frame2->frame_header.frame_length = 64;
    frame2->frame_header.type = AERON_HDR_TYPE_DATA;
    frame2->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame2->frame_header.flags = AERON_DATA_HEADER_BEGIN_FLAG;
    frame2->term_offset = term_offset + 64;

    auto *frame3 = reinterpret_cast<aeron_data_header_t *>(&data[128]);
    frame3->frame_header.frame_length = 64;
    frame3->frame_header.type = AERON_HDR_TYPE_DATA;
    frame3->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame3->frame_header.flags = 0;
    frame3->term_offset = term_offset + 128;

    auto *frame4 = reinterpret_cast<aeron_data_header_t *>(&data[192]);
    frame4->frame_header.frame_length = 64;
    frame4->frame_header.type = AERON_HDR_TYPE_DATA;
    frame4->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame4->frame_header.flags = AERON_DATA_HEADER_END_FLAG;
    frame4->term_offset = term_offset + 192;

    auto *frame5 = reinterpret_cast<aeron_data_header_t *>(&data[256]);
    frame5->frame_header.frame_length = 1024;
    frame5->frame_header.type = AERON_HDR_TYPE_PAD;
    frame5->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame5->frame_header.flags = AERON_DATA_HEADER_UNFRAGMENTED;
    frame5->term_offset = term_offset + 256;

    EXPECT_EQ(288, aeron_publication_image_insert_packet(
        &image, &destination, term_id, term_offset, data, 288, &addr, &media_receive_timestamp));
    EXPECT_EQ(0, invalid_packets);
    EXPECT_EQ(1, flow_control_over_runs);
    EXPECT_EQ(0, frame1->reserved_value);
    EXPECT_EQ(0, *reinterpret_cast<int64_t*>(&data[32]));
    EXPECT_EQ(0, frame2->reserved_value);
    EXPECT_EQ(0, *reinterpret_cast<int64_t*>(&data[96]));
    EXPECT_EQ(0, frame3->reserved_value);
    EXPECT_EQ(0, *reinterpret_cast<int64_t*>(&data[160]));
    EXPECT_EQ(0, frame4->reserved_value);
    EXPECT_EQ(0, *reinterpret_cast<int64_t*>(&data[224]));
    EXPECT_EQ(0, frame5->reserved_value);
}

TEST_F(PublicationImageTest, shouldNotSetTimestampsIfTimestampOffsetDoNotFitTheFrameLength)
{
    sockaddr_storage addr;
    timespec media_receive_timestamp = {};
    media_receive_timestamp.tv_sec = 5000;
    media_receive_timestamp.tv_nsec = 123;
    int64_t media_timestamp =
        (int64_t)1000000000 * (int64_t)media_receive_timestamp.tv_sec + media_receive_timestamp.tv_nsec;
    uint8_t data[256];
    memset(&data, 0, sizeof(data));
    int64_t invalid_packets = 0;
    int64_t flow_control_over_runs = 0;
    aeron_publication_image_t image;
    image.initial_term_id = 0;
    image.invalid_packets_counter = &invalid_packets;
    image.flow_control_over_runs_counter= &flow_control_over_runs;
    image.invalidation_reason = nullptr;
    int32_t term_length = 64 * 1024;
    image.term_length_mask = term_length - 1;
    image.position_bits_to_shift = aeron_number_of_trailing_zeroes(term_length);
    image.last_sm_position = 0;
    image.last_overrun_threshold = 0;
    aeron_clock_cache_t clock;
    clock.cached_nano_time = 1;
    image.cached_clock = &clock;
    aeron_receive_destination_t destination;
    destination.transport.timestamp_flags = AERON_UDP_CHANNEL_TRANSPORT_MEDIA_RCV_TIMESTAMP | AERON_UDP_CHANNEL_TRANSPORT_CHANNEL_RCV_TIMESTAMP;
    aeron_receive_channel_endpoint_t endpoint;
    aeron_udp_channel_t udp_channel;
    udp_channel.channel_rcv_timestamp_offset = 40;
    udp_channel.media_rcv_timestamp_offset = 80;
    endpoint.conductor_fields.udp_channel = &udp_channel;
    image.endpoint = &endpoint;

    int32_t term_id = 1;
    int32_t term_offset = 1024;

    auto *frame1 = reinterpret_cast<aeron_data_header_t *>(&data[0]);
    frame1->frame_header.frame_length = 128;
    frame1->frame_header.type = AERON_HDR_TYPE_DATA;
    frame1->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame1->frame_header.flags = AERON_DATA_HEADER_UNFRAGMENTED;
    frame1->term_offset = term_offset;

    auto *frame2 = reinterpret_cast<aeron_data_header_t *>(&data[128]);
    frame2->frame_header.frame_length = 64;
    frame2->frame_header.type = AERON_HDR_TYPE_DATA;
    frame2->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame2->frame_header.flags = AERON_DATA_HEADER_UNFRAGMENTED;
    frame2->term_offset = term_offset + 128;

    auto *frame3 = reinterpret_cast<aeron_data_header_t *>(&data[192]);
    frame3->frame_header.frame_length = 64;
    frame3->frame_header.type = AERON_HDR_TYPE_PAD;
    frame3->frame_header.version = AERON_FRAME_HEADER_VERSION;
    frame3->frame_header.flags = AERON_DATA_HEADER_UNFRAGMENTED;
    frame3->term_offset = term_offset + 192;
    frame3->session_id = 42;
    frame3->stream_id = -500;
    frame3->term_id = 12;
    frame3->reserved_value = 777;

    EXPECT_EQ(256, aeron_publication_image_insert_packet(
        &image, &destination, term_id, term_offset, data, 256, &addr, &media_receive_timestamp));
    EXPECT_EQ(0, invalid_packets);
    EXPECT_EQ(1, flow_control_over_runs);
    EXPECT_NE(0, *reinterpret_cast<int64_t*>(&data[72]));
    EXPECT_EQ(media_timestamp, *reinterpret_cast<int64_t*>(&data[112]));
    EXPECT_EQ(term_offset + 192, frame3->term_offset);
    EXPECT_EQ(42, frame3->session_id);
    EXPECT_EQ(-500, frame3->stream_id);
    EXPECT_EQ(12, frame3->term_id);
    EXPECT_EQ(777, frame3->reserved_value);
    EXPECT_EQ(0, *reinterpret_cast<int64_t*>(&data[224]));
    EXPECT_EQ(0, *reinterpret_cast<int64_t*>(&data[240]));
}
