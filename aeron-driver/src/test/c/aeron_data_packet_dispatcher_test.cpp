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

#include <array>
#include <gtest/gtest.h>
#include "aeron_receiver_test.h"

extern "C"
{
#include "util/aeron_dlopen.h"
#include "aeron_publication_image.h"
#include "aeron_data_packet_dispatcher.h"
#include "aeron_driver_receiver.h"

int aeron_driver_ensure_dir_is_recreated(aeron_driver_context_t *context);
}

#define CAPACITY (32 * 1024)
#define TERM_BUFFER_SIZE (64 * 1024)
#define MTU (4096)
#define STREAM_SESSION_LIMIT (5)

typedef std::array<std::uint8_t, CAPACITY> buffer_t;
typedef std::array<std::uint8_t, 4 * CAPACITY> buffer_4x_t;

void *get_on_publication_image_fptr()
{
    return aeron_dlsym(RTLD_DEFAULT, "aeron_driver_conductor_on_create_publication_image");
}

class DataPacketDispatcherTest : public ReceiverTestBase
{
protected:
    void SetUp() override
    {
        ReceiverTestBase::SetUp();
        aeron_driver_context_set_stream_session_limit(m_context, STREAM_SESSION_LIMIT);

        m_receive_endpoint = createEndpoint("aeron:udp?endpoint=localhost:9090");
        ASSERT_NE(nullptr, m_receive_endpoint) << aeron_errmsg();
        m_destination = m_receive_endpoint->destinations.array[0].destination;
        m_dispatcher = &m_receive_endpoint->dispatcher;
        m_test_bindings_state =
            static_cast<aeron_test_udp_bindings_state_t *>(m_destination->transport.bindings_clientd);
    }

    aeron_publication_image_t *createImage(int32_t stream_id, int32_t session_id, int64_t correlation_id = 0)
    {
        return ReceiverTestBase::createImage(m_receive_endpoint, m_destination, stream_id, session_id, correlation_id);
    }

    bool isEmpty(aeron_mpsc_rb_t *ring_buffer)
    {
        return aeron_mpsc_rb_consumer_position(ring_buffer) == aeron_mpsc_rb_producer_position(ring_buffer);
    }

    aeron_receive_channel_endpoint_t *m_receive_endpoint = nullptr;
    aeron_receive_destination_t *m_destination = nullptr;
    aeron_data_packet_dispatcher_t *m_dispatcher = nullptr;
    aeron_test_udp_bindings_state_t *m_test_bindings_state = nullptr;
};

TEST_F(DataPacketDispatcherTest, shouldInsertDataInputSubscribedPublicationImage)
{
    AERON_DECL_ALIGNED(buffer_t data_buffer, 16);

    int32_t session_id = 123123;
    int32_t stream_id = 434523;

    aeron_publication_image_t *image = createImage(stream_id, session_id);
    ASSERT_NE(nullptr, image) << aeron_errmsg();

    aeron_data_header_t *data_header = dataPacket(data_buffer, stream_id, session_id);
    size_t len = sizeof(aeron_data_header_t) + 8;

    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription(m_dispatcher, stream_id));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_publication_image(m_dispatcher, image));

    int bytes_written = aeron_data_packet_dispatcher_on_data(
        m_dispatcher,
        m_receive_endpoint,
        m_destination,
        data_header,
        data_buffer.data(),
        len,
        &m_receive_endpoint->conductor_fields.udp_channel->local_data);

    ASSERT_EQ((int)len, bytes_written);
    ASSERT_EQ((int64_t)len, *image->rcv_hwm_position.value_addr);
    ASSERT_EQ(AERON_PUBLICATION_IMAGE_STATE_ACTIVE, image->conductor_fields.state);
}

TEST_F(DataPacketDispatcherTest, shouldNotInsertDataInputWithNoSubscription)
{
    AERON_DECL_ALIGNED(buffer_t data_buffer, 16);

    int32_t session_id = 123123;
    int32_t stream_id = 434523;

    aeron_publication_image_t *image = createImage(stream_id, session_id);
    aeron_data_header_t *data_header = dataPacket(data_buffer, stream_id, session_id);
    size_t len = sizeof(aeron_data_header_t) + 8;

    int32_t other_stream_id = stream_id + 1;

    // Subscribe to a difference id
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription(m_dispatcher, other_stream_id));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_publication_image(m_dispatcher, image));

    int64_t position_before_data = *image->rcv_hwm_position.value_addr;
    int64_t expected_position_after_data = position_before_data;

    int bytes_written = aeron_data_packet_dispatcher_on_data(
        m_dispatcher,
        m_receive_endpoint,
        m_destination,
        data_header,
        data_buffer.data(),
        len,
        &m_receive_endpoint->conductor_fields.udp_channel->local_data);

    ASSERT_EQ(0, bytes_written);
    ASSERT_EQ(expected_position_after_data, *image->rcv_hwm_position.value_addr);
}

TEST_F(DataPacketDispatcherTest, shouldElicitSetupMessageForSubscriptionWithoutImage)
{
    AERON_DECL_ALIGNED(buffer_t data_buffer, 16);

    int32_t session_id = 123123;
    int32_t stream_id = 434523;

    aeron_data_header_t *data_header = dataPacket(data_buffer, stream_id, session_id);
    size_t len = sizeof(aeron_data_header_t) + 8;

    // No publication added...
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription(m_dispatcher, stream_id));

    ASSERT_EQ(0, aeron_data_packet_dispatcher_on_data(
        m_dispatcher,
        m_receive_endpoint,
        m_destination,
        data_header,
        data_buffer.data(),
        len,
        &m_receive_endpoint->conductor_fields.udp_channel->local_data));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_on_data(
        m_dispatcher,
        m_receive_endpoint,
        m_destination,
        data_header,
        data_buffer.data(),
        len,
        &m_receive_endpoint->conductor_fields.udp_channel->local_data));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_on_data(
        m_dispatcher,
        m_receive_endpoint,
        m_destination,
        data_header,
        data_buffer.data(),
        len,
        &m_receive_endpoint->conductor_fields.udp_channel->local_data));

    ASSERT_EQ(1, m_test_bindings_state->sm_count);

    aeron_data_packet_dispatcher_remove_pending_setup(m_dispatcher, session_id, stream_id);

    ASSERT_EQ(0, aeron_data_packet_dispatcher_on_data(
        m_dispatcher,
        m_receive_endpoint,
        m_destination,
        data_header,
        data_buffer.data(),
        len,
        &m_receive_endpoint->conductor_fields.udp_channel->local_data));

    ASSERT_EQ(2, m_test_bindings_state->sm_count);
}

TEST_F(DataPacketDispatcherTest, shouldRequestCreateImageUponReceivingSetupOnceForTheSameStreamSessionId)
{
    AERON_DECL_ALIGNED(buffer_t data_buffer, 16);

    int32_t session_id = 123123;
    int32_t stream_id = 434523;

    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription(m_dispatcher, stream_id));

    aeron_setup_header_t *setup_header = setupPacket(data_buffer, stream_id, session_id);

    aeron_data_packet_dispatcher_on_setup(
        m_dispatcher,
        m_receive_endpoint,
        nullptr,
        setup_header,
        data_buffer.data(),
        sizeof(*setup_header),
        &m_receive_endpoint->conductor_fields.udp_channel->local_data);
    aeron_data_packet_dispatcher_on_setup(
        m_dispatcher,
        m_receive_endpoint,
        nullptr,
        setup_header,
        data_buffer.data(),
        sizeof(*setup_header),
        &m_receive_endpoint->conductor_fields.udp_channel->local_data);
    aeron_data_packet_dispatcher_on_setup(
        m_dispatcher,
        m_receive_endpoint,
        nullptr,
        setup_header,
        data_buffer.data(),
        sizeof(*setup_header),
        &m_receive_endpoint->conductor_fields.udp_channel->local_data);

    ASSERT_EQ(UINT64_C(1), aeron_mpsc_rb_read(
        m_conductor_proxy.command_queue,
        verify_conductor_cmd_function,
        get_on_publication_image_fptr(),
        3));
}

TEST_F(DataPacketDispatcherTest, shouldRequestCreateImageUponReceivingSetupMultipleTimeForDifferentSessionIds)
{
    AERON_DECL_ALIGNED(buffer_t data_buffer, 16);

    int32_t stream_id = 434523;

    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription(m_dispatcher, stream_id));

    aeron_setup_header_t *setup_header_1 = setupPacket(data_buffer, stream_id, 1001);
    aeron_data_packet_dispatcher_on_setup(
        m_dispatcher,
        m_receive_endpoint,
        nullptr,
        setup_header_1,
        data_buffer.data(),
        sizeof(*setup_header_1),
        &m_receive_endpoint->conductor_fields.udp_channel->local_data);

    aeron_setup_header_t *setup_header_2 = setupPacket(data_buffer, stream_id, 1002);
    aeron_data_packet_dispatcher_on_setup(
        m_dispatcher,
        m_receive_endpoint,
        nullptr,
        setup_header_2,
        data_buffer.data(),
        sizeof(*setup_header_1),
        &m_receive_endpoint->conductor_fields.udp_channel->local_data);

    aeron_setup_header_t *setup_header_3 = setupPacket(data_buffer, stream_id, 1003);
    aeron_data_packet_dispatcher_on_setup(
        m_dispatcher,
        m_receive_endpoint,
        nullptr,
        setup_header_3,
        data_buffer.data(),
        sizeof(*setup_header_1),
        &m_receive_endpoint->conductor_fields.udp_channel->local_data);

    ASSERT_EQ(UINT64_C(3), aeron_mpsc_rb_read(
        m_conductor_proxy.command_queue,
        verify_conductor_cmd_function,
        get_on_publication_image_fptr(),
        3));
}

TEST_F(DataPacketDispatcherTest, DISABLED_shouldSetImageInactiveOnRemoveSubscription)
{
    int32_t session_id = 123123;
    int32_t stream_id = 434523;

    aeron_publication_image_t *image = createImage(stream_id, session_id);
    ASSERT_NE(nullptr, image) << aeron_errmsg();

    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription(m_dispatcher, stream_id));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_publication_image(m_dispatcher, image));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_remove_subscription(m_dispatcher, stream_id));

    ASSERT_EQ(AERON_PUBLICATION_IMAGE_STATE_DRAINING, image->conductor_fields.state);
}

TEST_F(DataPacketDispatcherTest, DISABLED_shouldSetImageInactiveOnRemoveImage)
{
    int32_t session_id = 123123;
    int32_t stream_id = 434523;

    aeron_publication_image_t *image = createImage(stream_id, session_id);
    ASSERT_NE(nullptr, image) << aeron_errmsg();

    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription(m_dispatcher, stream_id));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_publication_image(m_dispatcher, image));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_remove_publication_image(m_dispatcher, image));

    ASSERT_EQ(AERON_PUBLICATION_IMAGE_STATE_DRAINING, image->conductor_fields.state);
}

TEST_F(DataPacketDispatcherTest, shouldIgnoreDataAndSetupAfterImageRemoved)
{
    AERON_DECL_ALIGNED(buffer_t data_buffer, 16);

    int32_t session_id = 123123;
    int32_t stream_id = 434523;

    aeron_publication_image_t *image = createImage(stream_id, session_id);
    ASSERT_NE(nullptr, image) << aeron_errmsg();

    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription(m_dispatcher, stream_id));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_publication_image(m_dispatcher, image));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_remove_publication_image(m_dispatcher, image));


    aeron_data_header_t *data_header = dataPacket(data_buffer, stream_id, session_id);
    size_t len = sizeof(aeron_data_header_t) + 8;
    aeron_setup_header_t *setup_header = setupPacket(data_buffer, stream_id, session_id);

    aeron_data_packet_dispatcher_on_data(
        m_dispatcher,
        m_receive_endpoint,
        m_destination,
        data_header,
        data_buffer.data(),
        len,
        &m_receive_endpoint->conductor_fields.udp_channel->local_data);
    ASSERT_TRUE(isEmpty(m_conductor_proxy.command_queue));
    aeron_data_packet_dispatcher_on_setup(
        m_dispatcher,
        m_receive_endpoint,
        nullptr,
        setup_header,
        data_buffer.data(),
        sizeof(*setup_header),
        &m_receive_endpoint->conductor_fields.udp_channel->local_data);

    ASSERT_EQ(0, m_test_bindings_state->msg_count + m_test_bindings_state->mmsg_count);
    ASSERT_TRUE(isEmpty(m_conductor_proxy.command_queue));
}

TEST_F(DataPacketDispatcherTest, shouldNotIgnoreDataAndSetupAfterImageRemovedAndCoolDownRemoved)
{
    AERON_DECL_ALIGNED(buffer_t data_buffer, 16);

    int32_t session_id = 123123;
    int32_t stream_id = 434523;

    aeron_publication_image_t *image = createImage(stream_id, session_id);
    ASSERT_NE(nullptr, image) << aeron_errmsg();

    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription(m_dispatcher, stream_id));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_publication_image(m_dispatcher, image));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_remove_publication_image(m_dispatcher, image));
    aeron_data_packet_dispatcher_remove_cool_down(m_dispatcher, session_id, stream_id);

    aeron_data_header_t *data_header = dataPacket(data_buffer, stream_id, session_id);
    size_t len = sizeof(aeron_data_header_t) + 8;
    aeron_setup_header_t *setup_header = setupPacket(data_buffer, stream_id, session_id);

    aeron_data_packet_dispatcher_on_data(
        m_dispatcher,
        m_receive_endpoint,
        m_destination,
        data_header,
        data_buffer.data(),
        len,
        &m_receive_endpoint->conductor_fields.udp_channel->local_data);
    aeron_data_packet_dispatcher_on_setup(
        m_dispatcher,
        m_receive_endpoint,
        m_destination,
        setup_header,
        data_buffer.data(),
        sizeof(*setup_header),
        &m_receive_endpoint->conductor_fields.udp_channel->local_data);

    EXPECT_EQ(1, m_test_bindings_state->sm_count);
    ASSERT_EQ(UINT64_C(1), aeron_mpsc_rb_read(
        m_conductor_proxy.command_queue,
        verify_conductor_cmd_function,
        get_on_publication_image_fptr(),
        1));
}

TEST_F(DataPacketDispatcherTest, shouldNotRemoveNewPublicationImageFromOldRemovePublicationImageAfterRemoveSubscription)
{
    AERON_DECL_ALIGNED(buffer_t data_buffer, 16);

    int32_t session_id = 123123;
    int32_t stream_id = 434523;

    aeron_publication_image_t *image1 = createImage(stream_id, session_id, 0);
    aeron_publication_image_t *image2 = createImage(stream_id, session_id, 1);

    aeron_data_header_t *data_header = dataPacket(data_buffer, stream_id, session_id);
    size_t len = sizeof(aeron_data_header_t) + 8;

    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription(m_dispatcher, stream_id));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_publication_image(m_dispatcher, image1));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_remove_subscription(m_dispatcher, stream_id));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription(m_dispatcher, stream_id));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_publication_image(m_dispatcher, image2));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_remove_publication_image(m_dispatcher, image1));

    int bytes_written = aeron_data_packet_dispatcher_on_data(
        m_dispatcher,
        m_receive_endpoint,
        m_destination,
        data_header,
        data_buffer.data(),
        len,
        &m_receive_endpoint->conductor_fields.udp_channel->local_data);

    ASSERT_EQ((int)len, bytes_written);
    ASSERT_EQ((int64_t)len, *image2->rcv_hwm_position.value_addr);
}

TEST_F(DataPacketDispatcherTest, shouldAddSessionSpecificSubscriptionAndIgnoreOtherSession)
{
    AERON_DECL_ALIGNED(buffer_t data_buffer, 16);

    int32_t session_id1 = 123123;
    int32_t session_id2 = session_id1 + 1;
    int32_t stream_id = 434523;

    aeron_setup_header_t *setup_session1 = setupPacket(data_buffer, stream_id, session_id1);

    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription_by_session(m_dispatcher, stream_id, session_id1));

    aeron_data_packet_dispatcher_on_setup(
        m_dispatcher,
        m_receive_endpoint,
        nullptr,
        setup_session1,
        data_buffer.data(),
        sizeof(*setup_session1),
        &m_receive_endpoint->conductor_fields.udp_channel->local_data);

    ASSERT_EQ((size_t)1, aeron_mpsc_rb_read(
        m_conductor_proxy.command_queue,
        verify_conductor_cmd_function,
        get_on_publication_image_fptr(),
        1));

    ASSERT_TRUE(isEmpty(m_conductor_proxy.command_queue));

    aeron_setup_header_t *setup_session2_ignored = setupPacket(data_buffer, stream_id, session_id2);
    aeron_data_packet_dispatcher_on_setup(
        m_dispatcher,
        m_receive_endpoint,
        nullptr,
        setup_session2_ignored,
        data_buffer.data(),
        sizeof(*setup_session2_ignored),
        &m_receive_endpoint->conductor_fields.udp_channel->local_data);

    ASSERT_TRUE(isEmpty(m_conductor_proxy.command_queue));
}

TEST_F(DataPacketDispatcherTest, shouldRemoveSessionSpecificSubscriptionAndStillReceiveIntoImage)
{
    AERON_DECL_ALIGNED(buffer_t data_buffer, 16);

    int32_t session_id = 123123;
    int32_t stream_id = 434523;

    aeron_publication_image_t *image = createImage(stream_id, session_id);
    ASSERT_NE(nullptr, image) << aeron_errmsg();

    aeron_data_header_t *data_header = dataPacket(data_buffer, stream_id, session_id);
    size_t len = sizeof(aeron_data_header_t) + 8;

    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription(m_dispatcher, stream_id));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_publication_image(m_dispatcher, image));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription_by_session(m_dispatcher, stream_id, session_id));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_remove_subscription_by_session(m_dispatcher, stream_id, session_id));

    int bytes_written = aeron_data_packet_dispatcher_on_data(
        m_dispatcher,
        m_receive_endpoint,
        m_destination,
        data_header,
        data_buffer.data(),
        len,
        &m_receive_endpoint->conductor_fields.udp_channel->local_data);

    ASSERT_EQ((int)len, bytes_written);
    ASSERT_EQ((int64_t)len, *image->rcv_hwm_position.value_addr);
    ASSERT_EQ(AERON_PUBLICATION_IMAGE_STATE_ACTIVE, image->conductor_fields.state);
}

TEST_F(DataPacketDispatcherTest, shouldNotRemoveStreamInterestOnRemovalOfSessionSpecificSubscriptionIfNoImage)
{
    AERON_DECL_ALIGNED(buffer_t data_buffer, 16);

    int32_t session_id_1 = 123123;
    int32_t session_id_2 = 123124;
    int32_t stream_id = 434523;

    aeron_publication_image_t *image = createImage(stream_id, session_id_2);
    ASSERT_NE(nullptr, image) << aeron_errmsg();

    aeron_data_header_t *data_header = dataPacket(data_buffer, stream_id, session_id_2);
    size_t len = sizeof(aeron_data_header_t) + 8;

    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription_by_session(m_dispatcher, stream_id, session_id_1));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription_by_session(m_dispatcher, stream_id, session_id_2));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_remove_subscription_by_session(m_dispatcher, stream_id, session_id_1));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_publication_image(m_dispatcher, image));

    int bytes_written = aeron_data_packet_dispatcher_on_data(
        m_dispatcher,
        m_receive_endpoint,
        m_destination,
        data_header,
        data_buffer.data(),
        len,
        &m_receive_endpoint->conductor_fields.udp_channel->local_data);

    ASSERT_EQ((int)len, bytes_written);
    ASSERT_EQ((int64_t)len, *image->rcv_hwm_position.value_addr);
    ASSERT_EQ(AERON_PUBLICATION_IMAGE_STATE_ACTIVE, image->conductor_fields.state);
}

TEST_F(DataPacketDispatcherTest, shouldPreventNewSessionsOnceStreamSessionLimitIsExceeded)
{
    AERON_DECL_ALIGNED(buffer_t data_buffer, 16);

    int32_t streamId = 434523;
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription(m_dispatcher, streamId));

    for (int i = 0; i < STREAM_SESSION_LIMIT; i++)
    {
        int32_t sessionId = 1000 + i;

        aeron_setup_header_t *setup_header = setupPacket(data_buffer, streamId, sessionId);

        ASSERT_EQ(0, aeron_data_packet_dispatcher_on_setup(
            m_dispatcher,
            m_receive_endpoint,
            nullptr,
            setup_header,
            data_buffer.data(),
            sizeof(*setup_header),
            &m_receive_endpoint->conductor_fields.udp_channel->local_data));

        aeron_publication_image_t *image = createImage(streamId, sessionId, 2000 + i);
        ASSERT_NE(nullptr, image) << aeron_errmsg();

        ASSERT_EQ(0, aeron_data_packet_dispatcher_add_publication_image(m_dispatcher, image));
    }

    aeron_setup_header_t *setup_header = setupPacket(data_buffer, streamId, 10000, 20000);

    ASSERT_EQ(-1, aeron_data_packet_dispatcher_on_setup(
        m_dispatcher,
        m_receive_endpoint,
        nullptr,
        setup_header,
        data_buffer.data(),
        sizeof(*setup_header),
        &m_receive_endpoint->conductor_fields.udp_channel->local_data));
}
